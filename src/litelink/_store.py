"""A local table store beside the log, replicated the way a log is (§16).

A store is one SQLite database of declared tables. It is the source of truth:
every read and write is local, and nothing in this module touches the network.
Getting it off the machine is the WAL replication a log already has (§3a): the
store writes a litestream config for its database, the deployment runs the
sidecar on it, `flush` asks that sidecar to ship now through its control
socket, and `restore` pulls the database back on a machine that has none. The
config, the socket, the flush and the restore are `_replication`'s, shared with
the log rather than copied, so the two cannot drift apart.

The store never starts litestream. A library that supervised the sidecar would
own its lifecycle and could become a second replicator of one database, which
is the corruption litestream warns about; see `_replication`.

The consumer's writes and its position in what it consumes commit together:
`transaction()` is one SQLite transaction, and `advance` writes the reserved
`litelink_offset` inside it (S1). That is the whole of the pattern; nothing
here knows what is being consumed.
"""

from __future__ import annotations

import base64
import json
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

import pyarrow as pa

from litelink._buffer import _column_ddl
from litelink._layout import Layout, is_remote, validate_published
from litelink._replication import (
    destination,
    flush,
    litestream_config,
    restore_buffer,
)
from litelink._s3 import S3Options
from litelink._types import column_type

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence
    from os import PathLike

OFFSET = "litelink_offset"
RESERVED_PREFIXES = ("litelink_", "_litelink")

# The row key of a single-row table: a rowid alias pinned to 0 by a CHECK, so a
# second row is refused by SQLite rather than by a Python test.
_SINGLE = "_litelink_row"

# One statement each, run inside the opening transaction: `executescript`
# would COMMIT before running them, outside it.
_SYSTEM = (
    "CREATE TABLE IF NOT EXISTS _litelink_meta "
    "(k TEXT PRIMARY KEY, v TEXT NOT NULL) STRICT",
    "CREATE TABLE IF NOT EXISTS _litelink_offsets ("
    "subscription TEXT PRIMARY KEY, litelink_offset INTEGER NOT NULL) STRICT",
)

# Statements a consumer's SQL may not run. A transaction statement would end
# the store's transaction under it, committing the consumer's rows without
# their offset or the offset without the rows, which is the one thing S1
# forbids. DDL goes through the declaration, so the recorded schema stays the
# one copy of what the tables are. ATTACH would reach a database the replica
# does not hold.
_DENIED = {
    sqlite3.SQLITE_TRANSACTION,
    sqlite3.SQLITE_SAVEPOINT,
    sqlite3.SQLITE_ATTACH,
    sqlite3.SQLITE_DETACH,
    sqlite3.SQLITE_PRAGMA,
    sqlite3.SQLITE_CREATE_TABLE,
    sqlite3.SQLITE_CREATE_INDEX,
    sqlite3.SQLITE_CREATE_TRIGGER,
    sqlite3.SQLITE_CREATE_VIEW,
    sqlite3.SQLITE_CREATE_TEMP_TABLE,
    sqlite3.SQLITE_CREATE_TEMP_INDEX,
    sqlite3.SQLITE_CREATE_TEMP_TRIGGER,
    sqlite3.SQLITE_CREATE_TEMP_VIEW,
    sqlite3.SQLITE_CREATE_VTABLE,
    sqlite3.SQLITE_DROP_TABLE,
    sqlite3.SQLITE_DROP_INDEX,
    sqlite3.SQLITE_DROP_TRIGGER,
    sqlite3.SQLITE_DROP_VIEW,
    sqlite3.SQLITE_DROP_TEMP_TABLE,
    sqlite3.SQLITE_DROP_TEMP_INDEX,
    sqlite3.SQLITE_DROP_TEMP_TRIGGER,
    sqlite3.SQLITE_DROP_TEMP_VIEW,
    sqlite3.SQLITE_DROP_VTABLE,
    sqlite3.SQLITE_ALTER_TABLE,
}
_WRITES = {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE}


@dataclass(frozen=True, slots=True)
class StoreTable:
    """One declared table: its columns, and how rows are identified.

    `key` declares a primary key. `single_row` is the one-row checkpoint: the
    table holds at most one row, so `INSERT ... ON CONFLICT ("_litelink_row")
    DO UPDATE` replaces it. A table with neither is a plain heap.
    """

    schema: pa.Schema
    key: tuple[str, ...] = ()
    single_row: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "key", tuple(self.key))


@dataclass(frozen=True, slots=True)
class StoreLayout:
    """Where one store's files live. Derives paths and does no I/O.

    Satisfies `_replication.Replicated`, which is what lets the store use the
    log's config, socket and flush unchanged.
    """

    root: Path
    name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "root", Path(self.root).resolve())
        if not self.name or "/" in self.name or self.name in {".", ".."}:
            msg = f"a store's name is one path segment, not {self.name!r}"
            raise ValueError(msg)

    @property
    def directory(self) -> Path:
        return self.root / self.name

    @property
    def database(self) -> Path:
        return Layout(self.root, self.name).store_db

    @property
    def wal(self) -> Path:
        return self.directory / "store.db-wal"

    @property
    def databases(self) -> tuple[Path, ...]:
        """What a WAL-shipping sidecar has to replicate: the one database."""
        return (self.database,)

    @property
    def replication_config(self) -> Path:
        return self.directory / "litestream.yml"

    @property
    def log_buffer(self) -> Path:
        """A log's buffer, whose presence says this directory is a log's.

        A store in a log's directory would share its `litestream.yml` and its
        control socket, both derived from the directory, so the second one to
        write its config would silently stop the first replicating.
        """
        return Layout(self.root, self.name).buffer_db

    def refuse_a_log(self) -> None:
        """Refuse a log's directory, before anything is written to it."""
        if self.log_buffer.exists():
            msg = (
                f"{self.directory} holds a log; a store needs a directory of "
                f"its own, because the replication config and the sidecar's "
                f"control socket are both derived from the directory"
            )
            raise FileExistsError(msg)


# -- declarations -------------------------------------------------------------


def _quote(name: str) -> str:
    return f'"{name}"'


def _names(names: Iterable[str]) -> str:
    return ", ".join(_quote(n) for n in names)


def _reserved(name: str) -> bool:
    return name.lower().startswith(RESERVED_PREFIXES)


def _identifier(name: str, what: str) -> None:
    if not name or '"' in name or "\x00" in name:
        msg = f"{what} {name!r} is not a usable SQLite identifier"
        raise ValueError(msg)

    if _reserved(name):
        msg = f"{what} {name!r}: names beginning {RESERVED_PREFIXES} are reserved"
        raise ValueError(msg)


def _check_table(name: str, table: StoreTable) -> None:
    _identifier(name, "table")
    seen: set[str] = set()
    for f in table.schema:
        _identifier(f.name, f"column {name}.")
        # SQLite identifiers are case-insensitive, so `Id` and `id` collide in
        # the DDL even though Arrow keeps them apart.
        if f.name.lower() in seen:
            msg = f"table {name!r} declares {f.name!r} twice (case-insensitively)"
            raise ValueError(msg)

        seen.add(f.name.lower())
        try:
            kind = column_type(f.type)
        except TypeError as exc:
            msg = f"column {name}.{f.name}: {exc}"
            raise TypeError(msg) from None

        if kind.nested is not None:
            # A nested value is JSON text in SQLite, which no CHECK can see
            # into, and a transaction hands the consumer SQL: nothing would
            # stand between an UPDATE and a value the declared type refuses.
            msg = (
                f"column {name}.{f.name}: a store holds scalar columns only; "
                f"{f.type} would be JSON that no constraint can check"
            )
            raise TypeError(msg)

    if table.key and table.single_row:
        msg = f"table {name!r}: a single-row table has no key of its own"
        raise ValueError(msg)

    for column in table.key:
        if column not in table.schema.names:
            msg = f"table {name!r}: key column {column!r} is not declared"
            raise ValueError(msg)

        if table.schema.field(column).nullable:
            msg = f"table {name!r}: key column {column!r} must be non-nullable"
            raise ValueError(msg)


def _create_ddl(name: str, table: StoreTable) -> str:
    # The log's own column DDL (I17): every column `ANY` with a `typeof` and
    # range CHECK, so the consumer's raw SQL cannot store a value of the wrong
    # type any more than an append can.
    columns = [_column_ddl(f.name, f) for f in table.schema]
    if table.single_row:
        columns.insert(
            0,
            f'"{_SINGLE}" INTEGER PRIMARY KEY NOT NULL CHECK ("{_SINGLE}" = 0)',
        )

    if table.key:
        columns.append(f"PRIMARY KEY ({_names(table.key)})")

    return f'CREATE TABLE "{name}" (\n  ' + ",\n  ".join(columns) + "\n) STRICT"


def _encode_table(table: StoreTable) -> dict[str, object]:
    return {
        "schema": base64.b64encode(table.schema.serialize().to_pybytes()).decode(),
        "key": list(table.key),
        "single_row": table.single_row,
    }


def _decode_table(value: Mapping[str, object]) -> StoreTable:
    schema = pa.ipc.read_schema(pa.py_buffer(base64.b64decode(str(value["schema"]))))
    key = value["key"]
    assert isinstance(key, list)
    return StoreTable(
        schema=schema, key=tuple(key), single_row=bool(value["single_row"])
    )


def schema_changes(
    recorded: Mapping[str, StoreTable], declared: Mapping[str, StoreTable]
) -> list[str]:
    """The DDL taking `recorded` to `declared`, or ValueError naming what isn't
    additive.

    Additive means a new table, or a new NULLABLE column on an existing one.
    Everything else would change what an existing row means — a dropped or
    retyped column, a tightened nullability, a new key — and is refused. A new
    non-nullable column is refused too: the rows already there have no value
    for it, and a default invented here would be a value nobody wrote.
    """
    statements: list[str] = []
    problems: list[str] = []
    for name in sorted(set(recorded) - set(declared)):
        problems.append(f"table {name!r} is recorded but not declared (dropped)")

    for name, table in declared.items():
        _check_table(name, table)
        old = recorded.get(name)
        if old is None:
            statements.append(_create_ddl(name, table))
            continue

        if old.key != table.key or old.single_row != table.single_row:
            problems.append(f"table {name!r} changes its key")

        for f in old.schema:
            if f.name not in table.schema.names:
                problems.append(f"column {name}.{f.name} is dropped or renamed")
                continue

            new = table.schema.field(f.name)
            if not new.type.equals(f.type):
                problems.append(
                    f"column {name}.{f.name} changes type {f.type} -> {new.type}"
                )

            if new.nullable != f.nullable:
                problems.append(f"column {name}.{f.name} changes nullability")

        for f in table.schema:
            if f.name in old.schema.names:
                continue

            if not f.nullable:
                problems.append(
                    f"new column {name}.{f.name} must be nullable: existing rows "
                    f"have no value for it"
                )
                continue

            statements.append(
                f'ALTER TABLE "{name}" ADD COLUMN {_column_ddl(f.name, f)}'
            )

    if problems:
        msg = "a store's schema changes are additive only: " + "; ".join(problems)
        raise ValueError(msg)

    return statements


# -- the transaction ----------------------------------------------------------


class StoreTransaction:
    """The consumer's side of one store transaction.

    Everything done through it commits together, with the offsets it advanced,
    or not at all. Usable only inside the `with` that made it.
    """

    def __init__(self, con: sqlite3.Connection) -> None:
        self._con = con
        self._open = True

    def _live(self) -> sqlite3.Connection:
        if not self._open:
            msg = "this transaction has ended"
            raise RuntimeError(msg)

        if not self._con.in_transaction:
            # SQLite ended it: an `OR ROLLBACK` conflict, or an error it rolls
            # back on. A write now would autocommit on its own — an offset
            # without the rows it describes (S1).
            msg = (
                "SQLite rolled this transaction back (a conflict resolved by "
                "ROLLBACK, or an error); nothing in it committed"
            )
            raise RuntimeError(msg)

        return self._con

    def execute(
        self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] = ()
    ) -> sqlite3.Cursor:
        """Run one statement of the consumer's own SQL inside the transaction.

        Reads and writes of the declared tables only. Ending the transaction,
        changing the schema, PRAGMAs, ATTACH and any write to a store's own
        `_litelink_` tables are refused by an authorizer, because each of them
        would break the commit this transaction is.
        """
        con = self._live()
        con.set_authorizer(_authorize)
        try:
            return con.execute(sql, parameters)
        finally:
            con.set_authorizer(None)

    def advance(self, subscription: str, offset: int) -> None:
        """Record that everything through `offset` of `subscription` is done.

        In this transaction, so it commits with the work it describes (S1).
        Strictly forward: an offset at or below the recorded one is refused, so
        a replayed message fails loudly instead of being applied twice (S2).
        """
        con = self._live()
        if type(offset) is not int or offset < 0:
            msg = f"offset must be a non-negative int, not {offset!r}"
            raise ValueError(msg)

        current = _offset(con, subscription)
        if current is not None and offset <= current:
            msg = (
                f"subscription {subscription!r} is already at {current}; "
                f"{offset} does not advance it"
            )
            raise ValueError(msg)

        con.execute(
            f"INSERT INTO _litelink_offsets (subscription, {OFFSET}) VALUES (?, ?) "
            f"ON CONFLICT (subscription) DO UPDATE SET {OFFSET} = excluded.{OFFSET}",
            (subscription, offset),
        )

    def offset(self, subscription: str) -> int | None:
        """Where `subscription` stands, including this transaction's advance."""
        return _offset(self._live(), subscription)


def _authorize(action: int, arg1: str | None, *_: object) -> int:
    if action in _DENIED:
        return sqlite3.SQLITE_DENY

    if action in _WRITES and arg1 is not None and _reserved(arg1):
        return sqlite3.SQLITE_DENY

    return sqlite3.SQLITE_OK


def _read_only(action: int, *_: object) -> int:
    if action in _DENIED or action in _WRITES:
        return sqlite3.SQLITE_DENY

    return sqlite3.SQLITE_OK


def _offset(con: sqlite3.Connection, subscription: str) -> int | None:
    row = con.execute(
        f"SELECT {OFFSET} FROM _litelink_offsets WHERE subscription = ?",
        (subscription,),
    ).fetchone()
    return None if row is None else int(row[0])


def _meta(con: sqlite3.Connection) -> dict[str, str]:
    return dict(con.execute("SELECT k, v FROM _litelink_meta").fetchall())


def _set_meta(con: sqlite3.Connection, pairs: Mapping[str, str]) -> None:
    con.executemany(
        "INSERT INTO _litelink_meta (k, v) VALUES (?, ?) "
        "ON CONFLICT (k) DO UPDATE SET v = excluded.v",
        list(pairs.items()),
    )


def _connect(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    # WAL is what the sidecar ships; a rollback-journal database has no WAL
    # for it to read.
    con.execute("PRAGMA journal_mode=WAL")
    # FULL: a commit is durable when it returns, which is what lets a consumer
    # acknowledge the message whose offset it just advanced (S1).
    con.execute("PRAGMA synchronous=FULL")
    con.execute("PRAGMA busy_timeout=5000")
    return con


def _check_published(published: str) -> None:
    validate_published(published)
    if not is_remote(published):
        # A store's `published` exists only to name where its replica goes,
        # and a local directory is not off the machine.
        msg = (
            f"a store replicates to object storage (s3://), not {published!r}: "
            f"the replica exists to get the database off this machine"
        )
        raise ValueError(msg)


def _decode_tables(meta: Mapping[str, str]) -> dict[str, StoreTable]:
    raw = json.loads(meta.get("tables", "{}"))
    return {name: _decode_table(value) for name, value in raw.items()}


def _decode_retention(meta: Mapping[str, str]) -> timedelta | None:
    seconds = meta.get("retention", "")
    return timedelta(seconds=float(seconds)) if seconds else None


# -- the handle ---------------------------------------------------------------


class Store:
    """One open table store. Build it with `Store.open` or `Store.restore`.

    Threads within a process share one handle: every call takes one lock, and
    a transaction holds it from `BEGIN` to `COMMIT`. A second process opening
    the same store is SQLite's to arbitrate, through its write lock.
    """

    def __init__(
        self, layout: StoreLayout, con: sqlite3.Connection, s3: S3Options
    ) -> None:
        self._layout = layout
        self._con: sqlite3.Connection | None = con
        self._s3 = s3
        self._lock = threading.Lock()
        self._tx_thread: int | None = None

    # -- construction ------------------------------------------------------

    @classmethod
    def open(
        cls,
        root: PathLike[str] | str,
        name: str,
        *,
        tables: Mapping[str, StoreTable],
        published: str | None = None,
        s3_options: S3Options | None = None,
        retention: timedelta | None = None,
    ) -> Self:
        """Open the store at `<root>/<name>`, creating it if it does not exist.

        A local database always wins, and `open` never restores one: a
        replica is pulled only by `restore`, on a machine that has none.

        `tables` is the whole declaration. Changes against the recorded one
        are applied here, in the opening transaction, if they are additive;
        anything else is refused with every difference named. `published` and
        `retention` are recorded on first use, and an open naming a different
        `published` is refused: it is where this store's replica already is.
        """
        layout = StoreLayout(Path(root), name)
        if published is not None:
            _check_published(published)

        if retention is not None and retention <= timedelta(0):
            msg = f"retention must be positive, not {retention}"
            raise ValueError(msg)

        layout.refuse_a_log()
        layout.directory.mkdir(parents=True, exist_ok=True)
        con = _connect(layout.database)
        try:
            cls._declare(con, tables, published, retention)
        except BaseException:
            con.close()
            raise

        return cls(layout, con, s3_options or S3Options())

    @staticmethod
    def _declare(
        con: sqlite3.Connection,
        tables: Mapping[str, StoreTable],
        published: str | None,
        retention: timedelta | None,
    ) -> None:
        """Apply the declaration under the write lock, re-reading every premise
        after taking it: another process may have opened the store first."""
        con.execute("BEGIN IMMEDIATE")
        try:
            for statement in _SYSTEM:
                con.execute(statement)

            meta = _meta(con)
            recorded = meta.get("published", "")
            if published is not None and recorded and recorded != published:
                msg = (
                    f"this store replicates to {recorded!r}; it cannot be opened "
                    f"with published={published!r}"
                )
                raise ValueError(msg)

            for statement in schema_changes(_decode_tables(meta), tables):
                con.execute(statement)

            pairs = {
                "tables": json.dumps(
                    {n: _encode_table(t) for n, t in sorted(tables.items())}
                ),
            }
            if published is not None:
                pairs["published"] = published

            if retention is not None:
                pairs["retention"] = repr(retention.total_seconds())

            _set_meta(con, pairs)
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")

            raise

    @classmethod
    def restore(
        cls,
        root: PathLike[str] | str,
        name: str,
        *,
        published: str,
        tables: Mapping[str, StoreTable],
        s3_options: S3Options | None = None,
        retention: timedelta | None = None,
        binary: str | None = None,
    ) -> Self:
        """Pull the store's database from its replica onto a machine with none.

        The log's restore path: write the config `replication_config_for`
        gives, then `restore_buffer` into the database's own path. It refuses
        a machine that already has the database, because a local database is
        the source of truth and a restore over it would discard commits the
        replica never received. It refuses a stray `-wal` too: SQLite would
        replay it into the restored file, mixing two histories.

        No replica is an error naming where it looked, not an empty store:
        a caller that wanted an empty one would have called `open`.
        """
        layout = StoreLayout(Path(root), name)
        _check_published(published)
        # First: a restore writes and may remove `litestream.yml`, and in a
        # log's directory that file is the log's.
        layout.refuse_a_log()
        for path in (layout.database, layout.wal):
            if path.exists():
                msg = (
                    f"{path} exists; a local store is the source of truth and is "
                    f"never restored over. Open it, or remove the directory first"
                )
                raise FileExistsError(msg)

        options = s3_options or S3Options()
        layout.directory.mkdir(parents=True, exist_ok=True)
        config = layout.replication_config
        config.write_text(litestream_config(layout, published, options, retention))
        restore_buffer(config, layout.database, options, binary)
        if not layout.database.exists():
            config.unlink(missing_ok=True)
            msg = (
                f"no replica of store {name!r} at "
                f"{destination(published, name)}/{layout.database.name}"
            )
            raise FileNotFoundError(msg)

        store = cls.open(
            root,
            name,
            tables=tables,
            published=published,
            s3_options=options,
            retention=retention,
        )
        store.write_replication_config()
        return store

    # -- identity ----------------------------------------------------------

    @property
    def root(self) -> Path:
        return self._layout.root

    @property
    def name(self) -> str:
        return self._layout.name

    @property
    def path(self) -> Path:
        """The database file."""
        return self._layout.database

    @property
    def tables(self) -> dict[str, StoreTable]:
        """The declaration as recorded, which is what the database holds."""
        return _decode_tables(self._read_meta())

    @property
    def published(self) -> str | None:
        """Where the replica goes, or None for a store that is not replicated."""
        return self._read_meta().get("published") or None

    @property
    def retention(self) -> timedelta | None:
        return _decode_retention(self._read_meta())

    @property
    def databases(self) -> tuple[Path, ...]:
        """What the sidecar has to replicate: the one database."""
        return self._layout.databases

    # -- replication -------------------------------------------------------

    @staticmethod
    def replication_config_for(
        root: PathLike[str] | str,
        name: str,
        published: str,
        s3_options: S3Options | None = None,
        retention: timedelta | None = None,
    ) -> str:
        """The config for a store that may not exist here yet, as
        `WriteHandle.replication_config_for` gives one for a log."""
        _check_published(published)
        return litestream_config(
            StoreLayout(Path(root), name),
            published,
            s3_options or S3Options(),
            retention,
        )

    def replication_config(self) -> str:
        """A litestream config for this store, from `_replication` (§3a).

        The same function a log's comes from, so the socket, the destination
        (`<published>/<name>/_wal`), the per-database snapshot window that
        prunes the replica, and the endpoint lines are the log's.
        """
        meta = self._read_meta()
        published = meta.get("published")
        if not published:
            msg = f"store {self.name!r} has no published location to replicate to"
            raise ValueError(msg)

        return litestream_config(
            self._layout, published, self._s3, _decode_retention(meta)
        )

    def write_replication_config(self) -> Path:
        """Write that config beside the database, and return where."""
        destination = self._layout.replication_config
        destination.write_text(self.replication_config())
        return destination

    def flush(self, binary: str | None = None, timeout: int = 60) -> None:
        """Ship the database to its replica now; return once the replica has it.

        The log's flush: `litestream sync -wait` through the running sidecar's
        control socket. It raises when no sidecar answers and starts nothing,
        since it cannot tell "no sidecar" from "a sidecar started from a config
        without the socket", and guessing wrong is two replicators.

        Ships what is committed. Inside a transaction, that excludes the
        transaction itself, so it is refused there.
        """
        self._refuse_inside_transaction("flush")
        if not self.published:
            msg = f"store {self.name!r} has no published location to flush to"
            raise ValueError(msg)

        flush(self._layout, self._layout.database, binary, timeout)

    # -- reads -------------------------------------------------------------

    def offset(self, subscription: str) -> int | None:
        """Where `subscription` stands, as last committed."""
        with self._locked() as con:
            return _offset(con, subscription)

    def query(
        self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] = ()
    ) -> list[tuple[Any, ...]]:
        """Run a read-only statement against the committed state."""
        with self._locked() as con:
            con.set_authorizer(_read_only)
            try:
                return con.execute(sql, parameters).fetchall()
            finally:
                con.set_authorizer(None)

    # -- writes ------------------------------------------------------------

    @contextmanager
    def transaction(self) -> Iterator[StoreTransaction]:
        """One SQLite transaction: the consumer's writes and its offsets.

        Commits when the block exits normally, rolls back when it raises, and
        is durable when it returns (`synchronous=FULL`). Not reentrant: a
        nested one is refused rather than allowed to end the one it is in.
        """
        with self._locked() as con:
            con.execute("BEGIN IMMEDIATE")
            self._tx_thread = threading.get_ident()
            tx = StoreTransaction(con)
            try:
                yield tx
                # SQLite can end a transaction by itself — an `OR ROLLBACK`
                # conflict, or an error it rolls back on — and the consumer may
                # have caught the exception. Committing now would commit
                # nothing, or whatever autocommitted since: refuse instead (S1).
                tx._live()  # noqa: SLF001
                con.execute("COMMIT")
            except BaseException:
                if con.in_transaction:
                    con.execute("ROLLBACK")

                raise
            finally:
                tx._open = False  # noqa: SLF001
                self._tx_thread = None

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        # The lock is held for the whole transaction, so closing from inside
        # one on its own thread would wait for itself for ever.
        self._refuse_inside_transaction("close")
        with self._lock:
            if self._con is not None:
                self._con.close()
                self._con = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # -- internals ---------------------------------------------------------

    def _refuse_inside_transaction(self, what: str) -> None:
        if self._tx_thread == threading.get_ident():
            msg = (
                f"{what} cannot run inside a store transaction on the same "
                f"thread; use the transaction's own methods"
            )
            raise RuntimeError(msg)

    @contextmanager
    def _locked(self) -> Iterator[sqlite3.Connection]:
        self._refuse_inside_transaction("this call")
        with self._lock:
            if self._con is None:
                msg = f"store {self.name!r} is closed"
                raise RuntimeError(msg)

            yield self._con

    def _read_meta(self) -> dict[str, str]:
        with self._locked() as con:
            return _meta(con)
