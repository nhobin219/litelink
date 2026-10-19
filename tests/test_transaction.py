"""A caller's statements in the buffer's write transaction (§3c, I18).

`with log.transaction() as tx` is the whole surface: rows from `append`,
`extend` and `ingest` given `tx`, and the caller's own SQL through
`tx.execute`, commit together. Everything else here is what keeps the shared
connection safe to hand out, and the promise that litelink's maintenance never
touches what the caller keeps beside it.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import threading
from dataclasses import replace
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

import litelink
from litelink import LogConfig, RetiredError, WriteHandle
from litelink._buffer import OWNED_TABLES
from litelink._layout import Layout
from litelink._replication import litestream_binary
from tests.test_log import SCHEMA, open_log, rows

if TYPE_CHECKING:
    from pathlib import Path

    from litelink._s3 import S3Options

CREATE_SEEN = "CREATE TABLE IF NOT EXISTS mylib_seen (id TEXT PRIMARY KEY, through INT)"


def caller_rows(root: Path) -> list[tuple[object, ...]]:
    """`mylib_seen` as a fresh connection reads it: what was committed."""
    con = sqlite3.connect(Layout(root, "s").buffer_db)
    try:
        exists = con.execute(
            "SELECT 1 FROM sqlite_master WHERE name = 'mylib_seen'"
        ).fetchone()

        return (
            con.execute("SELECT * FROM mylib_seen ORDER BY id").fetchall()
            if exists
            else []
        )
    finally:
        con.close()


def offsets(log: WriteHandle) -> list[int]:
    table = log.scan(columns=["litelink_offset"]).read_all()

    return sorted(table.column("litelink_offset").to_pylist())


# -- committing together --------------------------------------------------


def test_rows_and_caller_statements_commit_together(tmp_path: Path) -> None:
    """Both are visible to another connection once the block ends, and not
    before.

    Falsify by committing in `joined` (a COMMIT after each extend): the rows
    are visible to the other connection inside the block.
    """
    with open_log(tmp_path) as log:
        with log.transaction() as tx:
            written = log.extend(rows(3), tx=tx)
            tx.execute(CREATE_SEEN)
            tx.execute("INSERT INTO mylib_seen VALUES (?, ?)", ("a", written[-1]))

            other = litelink.open(tmp_path, "s", read_only=True)
            assert other.scan().read_all().num_rows == 0
            other.close()

        assert written == [1, 2, 3]
        assert offsets(log) == [1, 2, 3]

    assert caller_rows(tmp_path) == [("a", 3)]


@pytest.mark.parametrize("call", ["append", "extend", "ingest"])
def test_every_write_path_takes_the_transaction(tmp_path: Path, call: str) -> None:
    """`append`, `extend` and `ingest` alike: their rows roll back with the
    block's.

    Falsify by dropping `tx` on any one path (calling the buffer without it):
    the call is refused as a second use of the open transaction.
    """
    batch = rows(2)
    with open_log(tmp_path) as log:
        with pytest.raises(KeyError), log.transaction() as tx:
            tx.execute(CREATE_SEEN)
            if call == "append":
                log.append(batch[0], tx=tx)
            elif call == "extend":
                log.extend(batch, tx=tx)
            else:
                assert log.ingest(pa.Table.from_pylist(batch, SCHEMA), tx=tx) == (1, 3)

            raise KeyError

        assert offsets(log) == []

        with log.transaction() as tx:
            got = getattr(log, call)(
                batch[0]
                if call == "append"
                else (
                    pa.Table.from_pylist(batch, SCHEMA) if call == "ingest" else batch
                ),
                tx=tx,
            )

        assert got == {"append": 1, "extend": [1, 2], "ingest": (1, 3)}[call]
        assert offsets(log) == ([1] if call == "append" else [1, 2])

    assert caller_rows(tmp_path) == []


def test_ingest_in_a_transaction_refuses_publish_and_flush(tmp_path: Path) -> None:
    """Loaded through the buffer, the rows publish with maintenance; asking
    the load to push its own output names something that does not exist."""
    with open_log(tmp_path) as log, log.transaction() as tx:
        table = pa.Table.from_pylist(rows(1), SCHEMA)
        for given in ({"publish": True}, {"publish": False}, {"flush": True}):
            with pytest.raises(ValueError, match="do not apply"):
                log.ingest(table, tx=tx, **given)


def test_a_refused_batch_undoes_only_itself(tmp_path: Path) -> None:
    """A row naming an unknown column refuses its batch, leaves the
    transaction open with everything before it, and consumes no offset.

    Falsify by replacing the SAVEPOINT in `_insert` with nothing: the refused
    batch's first row stays in the transaction and commits.
    """
    with open_log(tmp_path) as log:
        with log.transaction() as tx:
            log.extend(rows(2), tx=tx)
            tx.execute(CREATE_SEEN)
            tx.execute("INSERT INTO mylib_seen VALUES ('a', 2)")
            with pytest.raises(ValueError, match="nope"):
                log.extend([*rows(1), {"nope": 1}], tx=tx)

            assert log.extend(rows(1), tx=tx) == [3]

        assert offsets(log) == [1, 2, 3]

    assert caller_rows(tmp_path) == [("a", 2)]


@pytest.mark.parametrize("call", ["append", "extend", "ingest"])
def test_an_append_from_inside_another_appends_rows_is_refused(
    tmp_path: Path, call: str
) -> None:
    """A generator of rows that appends again would cut the open group under
    the outer call, leaving two open groups, rows served twice and a seal
    that fails for ever. Refused, and the outer batch with it.

    Falsify by removing the `_inserting` test from `Buffer.append`: the inner
    call succeeds and the outer one returns offsets around it.
    """
    config = LogConfig(target_seal_rows=3)
    with open_log(tmp_path, config) as log:
        with log.transaction() as tx:

            def batch() -> object:
                yield from rows(2)
                inner = rows(4, start=2)
                if call == "append":
                    log.append(inner[0], tx=tx)
                elif call == "extend":
                    log.extend(inner, tx=tx)
                else:
                    log.ingest(pa.Table.from_pylist(inner, SCHEMA), tx=tx)

                yield from rows(2, start=6)

            with pytest.raises(RuntimeError, match="inside the rows"):
                log.extend(batch(), tx=tx)  # ty: ignore[invalid-argument-type]

            assert log.extend(rows(4), tx=tx) == [1, 2, 3, 4]

        log.seal(flush=True)
        assert offsets(log) == [1, 2, 3, 4]


def test_sqlite_ending_the_transaction_is_never_autocommitted_past(
    tmp_path: Path,
) -> None:
    """SQLite rolls a transaction back by itself on some errors (a full disk);
    every statement after that would autocommit alone. Simulated by ending it
    under the block.

    Falsify by removing `in_transaction` from `SqliteTransaction.check`: the
    INSERT after the rollback commits on its own.
    """
    with open_log(tmp_path) as log:
        with pytest.raises(RuntimeError, match="rolled"), log.transaction() as tx:
            tx.execute(CREATE_SEEN)
            log._buffer._con.execute("ROLLBACK")  # noqa: SLF001
            tx.execute(CREATE_SEEN)

        with pytest.raises(RuntimeError, match="rolled"), log.transaction() as tx:
            log.extend(rows(1), tx=tx)
            log._buffer._con.execute("ROLLBACK")  # noqa: SLF001
            log.extend(rows(1), tx=tx)

        assert offsets(log) == []

    assert caller_rows(tmp_path) == []


# -- who may use it -------------------------------------------------------


def test_a_call_without_tx_inside_the_block_is_refused_and_harmless(
    tmp_path: Path,
) -> None:
    """The block's own thread calling the handle WITHOUT `tx` would BEGIN
    inside the open transaction, fail, and roll it back on the way out,
    leaving the caller's next statements to autocommit one by one.

    Falsify by removing the `_held` test from `WriteLock.__enter__`: the
    append fails with "cannot start a transaction within a transaction" and
    its ROLLBACK takes the caller's rows with it.
    """
    with open_log(tmp_path) as log:
        with log.transaction() as tx:
            log.extend(rows(2), tx=tx)
            for refused in (
                lambda: log.append(rows(1)[0]),
                lambda: log.seal(flush=True),
                lambda: log.end_offset(),
            ):
                with pytest.raises(RuntimeError, match="transaction open"):
                    refused()

            with pytest.raises(RuntimeError, match="transaction open"):
                with log.transaction():
                    pass

            tx.execute(CREATE_SEEN)
            tx.execute("INSERT INTO mylib_seen VALUES ('a', 2)")

        assert offsets(log) == [1, 2]
        # And the handle is whole again once the block ends.
        assert log.append(rows(1)[0]) == 3

    assert caller_rows(tmp_path) == [("a", 2)]


def test_a_transaction_is_refused_outside_its_block_log_and_thread(
    tmp_path: Path,
) -> None:
    """Another log's, one whose block has ended, and one used from another
    thread are all refused.

    Falsify by dropping the identity test from `Buffer.joined`: the other
    log's transaction is accepted and the rows go in on this log's connection
    outside any block of its own. Or by dropping the thread test from
    `SqliteTransaction.check`: the other thread's statement runs.
    """
    with open_log(tmp_path / "a") as log, open_log(tmp_path / "b") as other:
        # Both open, so "is a block open on this log" cannot stand in for
        # "is this its block".
        with other.transaction() as theirs, log.transaction():
            with pytest.raises(ValueError, match="not an open transaction"):
                log.extend(rows(1), tx=theirs)

        with pytest.raises(ValueError, match="not an open transaction"):
            other.extend(rows(1), tx=theirs)

        with pytest.raises(RuntimeError, match="ended"):
            theirs.execute("SELECT 1")

        with log.transaction() as tx:
            caught: list[BaseException] = []

            def elsewhere() -> None:
                try:
                    tx.execute("SELECT 1")
                except RuntimeError as exc:
                    caught.append(exc)

            thread = threading.Thread(target=elsewhere)
            thread.start()
            thread.join()

        assert len(caught) == 1
        assert "thread" in str(caught[0])
        assert offsets(log) == offsets(other) == []


def test_another_thread_waits_for_the_commit_rather_than_joining(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Another thread's append, attempted while the block is open, lands
    after it ends, in its own transaction: the block rolling back does not
    take it along.

    The interleaving is forced: the block does not roll back until the other
    thread is at the lock. Falsify by taking the lock in `transaction` only
    around the BEGIN: the other append joins the block and is rolled back
    with it, or fails beginning inside it.
    """
    with open_log(tmp_path) as log:
        lock = log._buffer._lock  # noqa: SLF001
        arrived = threading.Event()
        real = type(lock).__enter__
        done: list[object] = []

        def entering(self: object) -> None:
            if threading.current_thread() is not threading.main_thread():
                arrived.set()

            real(self)  # ty: ignore[invalid-argument-type]

        def append() -> None:
            done.append(log.extend(rows(1)))

        monkeypatch.setattr(type(lock), "__enter__", entering)
        with pytest.raises(KeyError), log.transaction() as tx:
            log.extend(rows(5), tx=tx)
            thread = threading.Thread(target=append)
            thread.start()
            arrived.wait()
            raise KeyError

        thread.join()
        monkeypatch.undo()

        assert done == [[1]]
        assert offsets(log) == [1]


def test_a_retired_log_refuses_rows_in_a_transaction_not_the_callers_tables(
    tmp_path: Path,
) -> None:
    """Retirement ends the log's rows, in a block as outside one. The
    caller's tables stay readable and writable, so a caller can carry its
    state from a retired log to the next.

    Falsify by dropping the `refuse_retired` trigger from `Buffer._create`:
    the extend inside the block lands on the retired log.
    """
    where = f"file://{tmp_path / 'published'}"
    with litelink.new(tmp_path, "s", schema=SCHEMA, published=where) as log:
        with log.transaction() as tx:
            log.extend(rows(4), tx=tx)
            tx.execute(CREATE_SEEN)
            tx.execute("INSERT INTO mylib_seen VALUES ('a', 4)")

        log.retire()
        with log.transaction() as tx:
            assert tx.execute("SELECT * FROM mylib_seen") == [("a", 4)]
            with pytest.raises(RetiredError):
                log.extend(rows(1), tx=tx)

            tx.execute("INSERT INTO mylib_seen VALUES ('b', 4)")

        assert offsets(log) == [1, 2, 3, 4]

    assert caller_rows(tmp_path) == [("a", 4), ("b", 4)]


# -- what a caller's statement may do ---------------------------------------


REFUSED = [
    ("DELETE FROM buffer", "litelink's"),
    ("UPDATE meta SET v = 'x'", "litelink's"),
    ("INSERT INTO claim (owner) VALUES ('me')", "litelink's"),
    ("UPDATE sqlite_sequence SET seq = 0", "litelink's"),
    ("DROP TABLE extent", "litelink's"),
    ("ALTER TABLE buffer ADD COLUMN mylib_x INT", "litelink's"),
    ("CREATE INDEX mylib_i ON buffer (key)", "litelink's"),
    ("CREATE TRIGGER mylib_t AFTER INSERT ON buffer BEGIN SELECT 1; END", "litelink's"),
    ("CREATE TABLE lease (x)", "litelink's"),
    ("CREATE TABLE litelink_next (x)", "litelink's"),
    ("CREATE TEMP TABLE meta (k, v)", "litelink's"),
    ("CREATE TEMP VIEW Extent AS SELECT 1", "litelink's"),
    ("COMMIT", "block's"),
    ("ROLLBACK", "block's"),
    ("BEGIN", "block's"),
    ("SAVEPOINT mine", "savepoints"),
    ("RELEASE litelink_append", "savepoints"),
    ("ROLLBACK TO litelink_append", "savepoints"),
    ("PRAGMA synchronous = OFF", "PRAGMA"),
    ("PRAGMA user_version = 3", "PRAGMA"),
    ("PRAGMA journal_mode", "PRAGMA"),
]


def test_a_caller_statement_cannot_reach_litelinks_state(tmp_path: Path) -> None:
    """Each refused before it runs, and the transaction carries on to commit.

    Falsify by returning SQLITE_OK from `_authorize` for everything: the
    first statement empties the buffer, and later ones end the transaction.
    """
    with open_log(tmp_path) as log:
        log.extend(rows(2))
        with log.transaction() as tx:
            log.extend(rows(1), tx=tx)
            for statement, why in REFUSED:
                with pytest.raises(PermissionError, match=why):
                    tx.execute(statement)

            tx.execute(CREATE_SEEN)
            tx.execute("INSERT INTO mylib_seen VALUES ('a', 3)")

        assert offsets(log) == [1, 2, 3]
        assert log.append(rows(1)[0]) == 4

    assert caller_rows(tmp_path) == [("a", 3)]


def test_a_caller_trigger_cannot_write_litelinks_tables_either(
    tmp_path: Path,
) -> None:
    """A trigger on the caller's own table is checked when the statement that
    fires it is."""
    with open_log(tmp_path) as log, log.transaction() as tx:
        tx.execute(CREATE_SEEN)
        tx.execute(
            "CREATE TRIGGER mylib_wipe AFTER INSERT ON mylib_seen"
            " BEGIN DELETE FROM buffer; END"
        )
        with pytest.raises(PermissionError, match="'buffer' is litelink's"):
            tx.execute("INSERT INTO mylib_seen VALUES ('a', 1)")


def test_what_a_caller_may_do(tmp_path: Path) -> None:
    """Read litelink's tables, introspect, and create and write its own
    objects."""
    with open_log(tmp_path) as log:
        with log.transaction() as tx:
            log.extend(rows(2), tx=tx)
            assert tx.dialect == "sqlite"
            assert tx.execute('SELECT max("litelink_offset") FROM buffer') == [(2,)]
            assert tx.execute("PRAGMA table_info(buffer)")
            tx.execute(
                "CREATE TABLE mylib_auto (n INTEGER PRIMARY KEY AUTOINCREMENT, v)"
            )
            tx.execute("CREATE INDEX mylib_auto_v ON mylib_auto (v)")
            tx.executemany("INSERT INTO mylib_auto (v) VALUES (?)", [(1,), (2,)])
            tx.execute("DELETE FROM mylib_auto")
            tx.executemany("INSERT INTO mylib_auto (v) VALUES (:v)", [{"v": 9}])
            assert tx.execute("SELECT v FROM mylib_auto") == [(9,)]

        # The caller's AUTOINCREMENT shares `sqlite_sequence` with the buffer,
        # through SQLite's own bookkeeping, and leaves the offsets alone.
        assert log.append(rows(1)[0]) == 3


def test_owned_tables_names_every_table_a_buffer_has(tmp_path: Path) -> None:
    """`OWNED_TABLES` is the authorizer's and I18's list: a table `_create`
    adds without it would be writable by a caller's statement.

    Falsify by removing any name but `lease` from `OWNED_TABLES`.
    """
    with open_log(tmp_path) as log:
        tables = {
            str(row[0])
            for row in sqlite3.connect(log.databases[0]).execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }

    assert OWNED_TABLES - tables == {"lease"}
    assert tables <= OWNED_TABLES


# -- maintenance never touches a caller's tables (I18) ---------------------


def caller_state(path: Path) -> tuple[list[tuple[object, ...]], ...]:
    """Every `mylib_` object's definition, and every row of its tables."""
    con = sqlite3.connect(path)
    try:
        objects = con.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master"
            " WHERE name LIKE 'mylib~_%' ESCAPE '~' ORDER BY name"
        ).fetchall()
        contents = [
            con.execute(f"SELECT * FROM {name} ORDER BY 1").fetchall()
            for kind, name, _, _ in objects
            if kind in ("table", "view")
        ]

        return (objects, *contents)
    finally:
        con.close()


def record_tables(log: WriteHandle) -> list[tuple[str, str | None]]:
    """Every name litelink's statements reach from now on, with the schema it
    is in, on each of the buffer's connections, read from SQLite's
    authorizer."""
    seen: list[tuple[str, str | None]] = []

    def authorize(
        _action: int,
        arg1: str | None,
        arg2: str | None,
        database: str | None,
        _trigger: str | None,
    ) -> int:
        seen.extend((name, database) for name in (arg1, arg2) if name)

        return sqlite3.SQLITE_OK

    buffer = log._buffer  # noqa: SLF001
    connections = (buffer._con, buffer._reader, buffer._sealer)  # noqa: SLF001
    for con in {id(c): c for c in connections}.values():
        con.set_authorizer(authorize)

    return seen


def test_maintenance_never_touches_a_callers_tables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """I18. A caller's table, index, trigger and view in `buffer.db` come
    through every maintenance pass exactly as committed: seal, compact,
    publish, evict, reclaim (a VACUUM included), sweep, recover, truncate, a
    reopen, which runs `_create` and recovery again, and retire. No statement litelink
    prepares on any of the buffer's connections names one of them, except
    the VACUUM, which rewrites the whole file.

    Each pass is checked to have done its work, so a pass that silently did
    nothing cannot pass this: the eviction empties the buffer and staging,
    the VACUUM runs, the truncation drops four files and the compaction
    merges.

    Falsify by adding `DELETE FROM mylib_seen` to `Buffer.evict_rows`, or
    `DROP INDEX IF EXISTS mylib_seen_through` to `Buffer._create`: the state
    differs, and the first is also seen by the recorder.
    """
    # Small files, nothing retained past a publish, and a VACUUM whatever the
    # file's size: every pass has something to do on a few hundred rows.
    monkeypatch.setattr("litelink._buffer._VACUUM_FLOOR_BYTES", 0)
    config = replace(
        LogConfig(),
        target_seal_size=1 << 30,
        target_compact_size=2000,
        compact_min_files=2,
        staging_retention=None,
        staging_max_bytes=1,
    )
    where = f"file://{tmp_path / 'published'}"
    root = tmp_path / "log"
    with litelink.new(root, "s", schema=SCHEMA, config=config, published=where) as log:
        database = log.databases[0]
        with log.transaction() as tx:
            last = log.extend(rows(40), tx=tx)[-1]
            assert last == 40
            tx.execute(CREATE_SEEN)
            tx.execute("CREATE INDEX mylib_seen_through ON mylib_seen (through)")
            tx.execute("CREATE TABLE mylib_audit (id TEXT)")
            tx.execute(
                "CREATE TRIGGER mylib_seen_audit AFTER INSERT ON mylib_seen"
                " BEGIN INSERT INTO mylib_audit VALUES (new.id); END"
            )
            tx.execute(
                "CREATE VIEW mylib_latest AS SELECT max(through) FROM mylib_seen"
            )
            tx.execute("INSERT INTO mylib_seen VALUES ('a', ?)", (last,))

        before = caller_state(database)
        seen = record_tables(log)

        def fill(first: int) -> None:
            """Ten sealed files of 40 rows from offset `first`: small enough
            that compaction merges them, too big for it to before a publish."""
            for start in range(first, first + 400, 40):
                log.extend(rows(40, start=start))
                log.seal(flush=True)

        statements: list[str] = []
        log._buffer._con.set_trace_callback(statements.append)  # noqa: SLF001
        fill(40)
        log.publish(flush=True)
        log.evict()
        assert log.buffered_rows() == 0
        log.reclaim()
        assert "VACUUM" in statements
        log.sweep()
        log.recover()
        assert log.truncate(below=200) == 161

        fill(440)
        before_merge = log.staging_files()
        log.compact(flush=True)
        assert log.staging_files() < before_merge
        log.publish(flush=True)
        log.advance(flush=True)
        assert log.published_through() == 840
        assert min(offsets(log)) == 161
        assert log.staging_files() == 0

    # The VACUUM copies every table into a schema SQLite names `vacuum_db`,
    # or `vacuum_<random>` in newer releases, and swaps the
    # file: the one statement that reaches a caller's table, by rewriting the
    # whole file with its contents unchanged (I18). It is also the proof that
    # the recorder sees a caller's names when something does reach them.
    reached = {name for name, database in seen if name.lower().startswith("mylib")}
    assert reached >= {"mylib_seen", "mylib_audit"}
    assert not [
        (name, database)
        for name, database in seen
        if name.lower().startswith("mylib")
        and not (database or "").startswith("vacuum_")
    ]

    # A reopen runs `_create` and recovery again; `retire` empties the buffer
    # and writes its closing range.
    with litelink.open(root, "s") as log:
        seen = record_tables(log)
        log.extend(rows(1))
        log.retire()
        assert log._buffer.retired() is not None  # noqa: SLF001

    assert seen
    assert not [name for name, _ in seen if name.lower().startswith("mylib")]
    assert caller_state(database) == before


def test_caller_tables_are_lost_on_a_restore_from_the_published_table(
    tmp_path: Path,
) -> None:
    """The published table holds the log's rows and nothing of the caller's,
    so a restore from it alone brings back a buffer with no caller tables.
    A caller that needs them back keeps them where a WAL replica of
    `buffer.db` can restore them, or elsewhere (§3c)."""
    where = f"file://{tmp_path / 'published'}"
    with litelink.new(tmp_path / "a", "s", schema=SCHEMA, published=where) as log:
        with log.transaction() as tx:
            log.extend(rows(10), tx=tx)
            tx.execute(CREATE_SEEN)
            tx.execute("INSERT INTO mylib_seen VALUES ('a', 10)")

        log.seal(flush=True)
        log.publish(flush=True)

    with litelink.restore(tmp_path / "b", "s", published=where) as revived:
        assert offsets(revived) == list(range(1, 11))

    assert caller_rows(tmp_path / "a") == [("a", 10)]
    assert caller_rows(tmp_path / "b") == []


@pytest.mark.s3
def test_caller_tables_come_back_from_a_wal_replica(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The replica ships `buffer.db` whole, so a restore from it brings the
    caller's tables back with the rows they were committed with.

    Falsify by deleting `mylib_` objects in `Buffer.strip_local_state`: the
    restored buffer has the rows and not the table.
    """
    binary = litestream_binary()
    if not os.access(binary, os.X_OK):
        pytest.skip("litestream is not provisioned; run `just litestream`")

    where = f"s3://{bucket}/caller"
    config = replace(LogConfig(), wal_replication=True)
    primary = tmp_path / "primary"
    with litelink.new(
        primary, "s", schema=SCHEMA, config=config, published=where, s3_options=s3
    ) as log:
        with log.transaction() as tx:
            log.extend(rows(10), tx=tx)
            tx.execute(CREATE_SEEN)
            tx.execute("INSERT INTO mylib_seen VALUES ('a', 10)")

        replication = log.write_replication_config()

    environment = dict(os.environ)
    resolved = s3.resolved()
    environment["LITESTREAM_ACCESS_KEY_ID"] = resolved.access_key or ""
    environment["LITESTREAM_SECRET_ACCESS_KEY"] = resolved.secret_key or ""
    subprocess.run(  # noqa: S603
        [str(binary), "replicate", "-config", str(replication), "-exec", "sleep 3"],
        check=True,
        env=environment,
        capture_output=True,
        timeout=120,
    )

    second = tmp_path / "second"
    with litelink.restore(
        second, "s", published=where, s3_options=s3, binary=str(binary)
    ) as revived:
        assert offsets(revived) == list(range(1, 11))

    assert caller_rows(second) == [("a", 10)]
