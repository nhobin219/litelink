"""A caller's statements in the buffer's own write transaction.

`with log.transaction() as tx:` opens one write transaction on `buffer.db`, and
everything given `tx` runs inside it: `append`, `extend` and `ingest` with
`tx=tx`, and the caller's own SQL through `tx.execute`. It commits when the
block ends and rolls back if the block raises, so rows and whatever the caller
writes beside them commit together or not at all.

**This is the only thing litelink offers a caller for that.** It keeps no keyed
state, no snapshot of the caller's tables, and nothing in the published table
about them: a caller that wants a value committed with its rows keeps it in a
table of its own, named under its own prefix (`docs/SPEC.md` §3c), and litelink
guarantees that none of its maintenance touches that table (I18).

**Shaped to outlive SQLite.** `Transaction` is a protocol with no SQLite in it:
statements go in as text and parameters, results come back as plain tuples, and
no cursor, connection or row id escapes. `dialect` names the SQL the backend
speaks, so a library can write each statement for the backend it is handed.
A Postgres backend for the buffer would implement the same protocol over a
Postgres transaction, and a caller's code would not change shape.

**Two guards make the shared connection safe to hand out.**

- `WriteLock`. The transaction holds the buffer's write lock for its whole
  block, so other threads wait instead of joining it. The thread holding the
  block may re-enter the lock only through a call it gave `tx`: any other call
  on the write connection from inside the block would either BEGIN inside it
  (an error whose ROLLBACK ends the caller's transaction, after which the
  caller's statements autocommit one by one) or silently join it.
- The authorizer. A caller's statement may read any table, but may not write
  one of litelink's, create a table under one of litelink's names, end the
  transaction or a savepoint of it, or change a connection setting. SQLite refuses it before
  the statement runs.
"""

from __future__ import annotations

import contextlib
import sqlite3
import threading
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence

OPEN_TRANSACTION = (
    "this thread has a litelink transaction open on this log; inside the "
    "block, only append, extend and ingest given tx=, and tx's own methods, "
    "may use it"
)

# PRAGMAs that only read the schema. Everything else a PRAGMA can do is a
# setting of the connection litelink's durability rests on (`synchronous`,
# `journal_mode`, `foreign_keys`) or a value in a namespace every library in
# the file shares (`user_version`, `application_id`).
_READ_PRAGMAS = frozenset(
    {
        "foreign_key_list",
        "index_info",
        "index_list",
        "index_xinfo",
        "table_info",
        "table_xinfo",
    }
)

# The authorizer's arguments name the table in different positions per action.
# Creating a view or a temporary object under one of litelink's names is
# refused with the writes: the temporary schema is searched first, so a
# `temp.meta` would take every unqualified statement litelink runs on this
# connection.
_WRITE_ARG1 = frozenset(
    {
        sqlite3.SQLITE_INSERT,
        sqlite3.SQLITE_UPDATE,
        sqlite3.SQLITE_DELETE,
        sqlite3.SQLITE_DROP_TABLE,
        sqlite3.SQLITE_CREATE_TABLE,
        sqlite3.SQLITE_CREATE_TEMP_TABLE,
        sqlite3.SQLITE_DROP_TEMP_TABLE,
        sqlite3.SQLITE_CREATE_VIEW,
        sqlite3.SQLITE_CREATE_TEMP_VIEW,
        sqlite3.SQLITE_CREATE_VTABLE,
    }
)
_WRITE_ARG2 = frozenset(
    {
        sqlite3.SQLITE_ALTER_TABLE,
        sqlite3.SQLITE_CREATE_INDEX,
        sqlite3.SQLITE_CREATE_TEMP_INDEX,
        sqlite3.SQLITE_CREATE_TRIGGER,
        sqlite3.SQLITE_CREATE_TEMP_TRIGGER,
        sqlite3.SQLITE_DROP_INDEX,
        sqlite3.SQLITE_DROP_TEMP_INDEX,
        sqlite3.SQLITE_DROP_TRIGGER,
        sqlite3.SQLITE_DROP_TEMP_TRIGGER,
    }
)


class Transaction(Protocol):
    """A log's write transaction, as `log.transaction()` yields it.

    Valid inside its `with` block, on the thread that opened it.
    """

    @property
    def dialect(self) -> str:
        """The SQL this transaction runs: `"sqlite"`, with `?` and `:name`
        parameters."""
        ...

    def execute(
        self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] = ()
    ) -> list[tuple[Any, ...]]:
        """Run one statement in the transaction. Returns the rows it produced,
        empty for a statement that produces none."""
        ...

    def executemany(
        self,
        sql: str,
        parameters: Iterable[Sequence[Any] | Mapping[str, Any]],
    ) -> None:
        """Run one statement once per parameter set, in the transaction."""
        ...


class WriteLock:
    """The buffer's write-connection mutex, aware of a caller's transaction.

    Reentrant, as the `RLock` it wraps: a method holding it calls others that
    take it. While a caller's transaction is open (`hold`) its thread owns the
    lock, so every other thread waits at `__enter__`; the owning thread gets
    through only inside `admit`, which the calls given `tx` enter. Any other
    entry from that thread raises instead of reaching the connection.
    """

    __slots__ = ("_admitted", "_held", "_lock")

    def __init__(self) -> None:
        self._lock = threading.RLock()
        # Both touched only by the thread that owns `_lock`, so neither needs
        # a lock of its own: another thread reads them only after acquiring it,
        # by which time `hold` has reset them.
        self._held = False
        self._admitted = 0

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        """`RLock.acquire`, refusing the open transaction's thread outside
        `admit`."""
        if not self._lock.acquire(blocking, timeout):
            return False

        # `_held` with the lock acquired means this thread holds the open
        # transaction: any other thread would still be waiting above.
        if self._held and not self._admitted:
            self._lock.release()
            raise RuntimeError(OPEN_TRANSACTION)

        return True

    def release(self) -> None:
        self._lock.release()

    def __enter__(self) -> None:
        self.acquire()

    def __exit__(self, *_: object) -> None:
        self._lock.release()

    @contextlib.contextmanager
    def hold(self) -> Iterator[None]:
        """Own the lock for a caller's transaction, refusing a second one on
        this thread."""
        with self:
            self._held = True
            try:
                yield
            finally:
                self._held = False
                self._admitted = 0

    @contextlib.contextmanager
    def admit(self) -> Iterator[None]:
        """Let the transaction's own thread re-enter, for a call given `tx`."""
        self._admitted += 1
        try:
            yield
        finally:
            self._admitted -= 1


class SqliteTransaction:
    """`Transaction` over the buffer's write connection.

    Built by `Buffer.transaction` inside the BEGIN it commits, and handed the
    connection and the test for litelink's names rather than the buffer: it
    runs statements and nothing else.
    """

    dialect = "sqlite"

    def __init__(
        self, connection: sqlite3.Connection, owned: Callable[[str], bool]
    ) -> None:
        self._con = connection
        self._owned = owned
        self._thread = threading.get_ident()
        self._open = True
        # The reason the authorizer last refused an action, for the error.
        self._denied: str | None = None

    def execute(
        self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] = ()
    ) -> list[tuple[Any, ...]]:
        with self._guarded():
            return self._con.execute(sql, parameters).fetchall()

    def executemany(
        self,
        sql: str,
        parameters: Iterable[Sequence[Any] | Mapping[str, Any]],
    ) -> None:
        with self._guarded():
            self._con.executemany(sql, parameters)

    def check(self) -> None:
        """Refuse use after the block, from another thread, or once SQLite has
        ended the transaction under it.

        That last happens: SQLite rolls a transaction back by itself on some
        errors (a full disk, an I/O error), and every statement after that
        would autocommit alone. So this runs before and after every statement.
        """
        if not self._open:
            msg = "this litelink transaction has ended; open a new one"
            raise RuntimeError(msg)

        if threading.get_ident() != self._thread:
            msg = "a litelink transaction is usable only on the thread that opened it"
            raise RuntimeError(msg)

        if not self._con.in_transaction:
            self._open = False
            msg = (
                "SQLite rolled this litelink transaction back after an error; "
                "nothing in it was committed"
            )
            raise RuntimeError(msg)

    def close(self) -> None:
        """Mark the transaction ended, once its COMMIT or ROLLBACK has run."""
        self._open = False

    @contextlib.contextmanager
    def _guarded(self) -> Iterator[None]:
        """One caller statement, under the authorizer.

        Installed per statement rather than for the transaction, so litelink's
        own statements inside it (an `extend` given `tx`) run unrestricted.
        Setting an authorizer expires every prepared statement, which is what
        makes this sound: a statement prepared earlier without it, and cached
        under the same text, is prepared again and checked.
        """
        self.check()
        self._denied = None
        self._con.set_authorizer(self._authorize)
        try:
            yield
        except sqlite3.DatabaseError as exc:
            # SQLite says only "not authorized". Which rule refused it is the
            # part a caller can act on.
            if self._denied is None:
                raise

            raise PermissionError(self._denied) from exc
        finally:
            self._con.set_authorizer(None)
            self.check()

    def _authorize(
        self,
        action: int,
        arg1: str | None,
        arg2: str | None,
        _database: str | None,
        _trigger: str | None,
    ) -> int:
        """SQLite's authorizer callback: deny what would reach litelink's own
        state, allow everything else."""
        denied = self._denial(action, arg1, arg2)
        if denied is None:
            return sqlite3.SQLITE_OK

        self._denied = denied

        return sqlite3.SQLITE_DENY

    def _denial(self, action: int, arg1: str | None, arg2: str | None) -> str | None:
        """Why a caller's statement may not take this action, or None."""
        if action == sqlite3.SQLITE_SAVEPOINT:
            # A savepoint around an `extend` can roll its rows back while the
            # block goes on to commit, and the next append takes the same
            # offsets: the caller would hold offsets that name other rows.
            return (
                f"{arg1} {arg2}: savepoints would let rows given `tx` roll back "
                "inside a block that commits; raise to roll the block back"
            )

        if action == sqlite3.SQLITE_TRANSACTION:
            return (
                f"{arg1}: the transaction is the `with` block's to begin and "
                "end; raise inside the block to roll it back"
            )

        if (
            action == sqlite3.SQLITE_PRAGMA
            and (arg1 or "").lower() not in _READ_PRAGMAS
        ):
            return (
                f"PRAGMA {arg1}: a caller's statement may read the schema, not "
                "change the connection or the file's shared values"
            )

        if action in (sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH):
            return "ATTACH and DETACH are not allowed in a litelink transaction"

        table = arg1 if action in _WRITE_ARG1 else None
        table = arg2 if action in _WRITE_ARG2 else table
        if table is not None and self._owned(table):
            return (
                f"{table!r} is litelink's: a caller writes only tables it "
                "creates, named with its own library's prefix"
            )

        return None
