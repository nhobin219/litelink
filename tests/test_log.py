"""The core capture loop: append, seal, read, recover."""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import litelink
from litelink._buffer import Buffer
from litelink.log import OFFSET, Log, LogConfig

SCHEMA = pa.schema(
    [
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("key", pa.string()),
        pa.field("payload", pa.string()),
    ]
)


def open_log(root: Path, config: LogConfig | None = None) -> Log:
    """Create the log, or reopen it — the shape is only stated once."""
    if (root / "catalog.db").exists():
        log = Log.open(root, "s")
        if config is not None:
            log.set_config(config)

        return log

    return Log.new(root, "s", schema=SCHEMA, sort_by=("event_ts", "key"), config=config)


def _today() -> date:
    return datetime.now(UTC).date()


def open_log_readonly(root: Path) -> litelink.LogReader:
    return litelink.reader(root, "s")


def rows(n: int, *, start: int = 0) -> list[dict[str, object]]:
    return [
        {"event_ts": 1000 + i, "key": f"k{i % 3}", "payload": f'{{"seq":{i}}}'}
        for i in range(start, start + n)
    ]


def read_all(log: Log | litelink.LogReader) -> list[tuple[object, ...]]:
    return [tuple(r.values()) for r in log.scan().read_all().to_pylist()]


def test_append_returns_monotonic_offsets(tmp_path: Path) -> None:
    with open_log(tmp_path) as log:
        assert log.append(rows(1)[0]) == 1
        assert log.extend(rows(3)) == [2, 3, 4]
        assert log.end_offset() == 5


def test_caller_supplied_offset_is_rejected(tmp_path: Path) -> None:
    """I11: the library owns `offset` and never accepts one."""
    with open_log(tmp_path) as log:
        with pytest.raises(ValueError, match="I11"):
            log.append({"litelink_offset": 7, "event_ts": 1, "key": "k", "payload": ""})


def test_offset_in_schema_is_rejected(tmp_path: Path) -> None:
    """I11 again, one layer earlier."""
    with pytest.raises(ValueError, match="I11"):
        Log.new(
            tmp_path,
            "s",
            schema=pa.schema([pa.field("litelink_offset", pa.int64())]),
            sort_by=(),
        )


def test_read_before_any_seal_sees_the_buffer(tmp_path: Path) -> None:
    with open_log(tmp_path) as log:
        log.extend(rows(5))
        assert len(read_all(log)) == 5


def test_seal_then_read_returns_every_row_once(tmp_path: Path) -> None:
    """The union must not double-count across the boundary (I3)."""
    with open_log(tmp_path) as log:
        log.extend(rows(10))
        assert log.seal() == 11
        log.extend(rows(5, start=10))

        offsets = [r[0] for r in read_all(log)]
        assert offsets == list(range(1, 16))


def test_seal_empties_the_buffer_but_not_the_log(tmp_path: Path) -> None:
    with open_log(tmp_path) as log:
        log.extend(rows(4))
        log.seal()
        assert log._buffer.extent() is None
        assert len(read_all(log)) == 4


def test_offsets_never_reused_after_the_buffer_empties(tmp_path: Path) -> None:
    """I9. This is the assertion that fails with a bare INTEGER PRIMARY KEY."""
    with open_log(tmp_path) as log:
        log.extend(rows(3))
        log.seal()
        assert log._buffer.extent() is None
        assert log.extend(rows(1, start=3)) == [4]
        assert log.table_extent() == (1, 3)


def test_sealed_file_is_sorted_by_sort_by(tmp_path: Path) -> None:
    """§4: the sort order is applied at write time, not merely declared."""
    with open_log(tmp_path) as log:
        log.extend(
            [{"event_ts": ts, "key": "k", "payload": ""} for ts in (300, 100, 200)]
        )
        log.seal()

        scanned = log.scan(columns=["event_ts"]).read_all()["event_ts"].to_pylist()
        assert scanned == [300, 100, 200], "scan orders by offset, not by sort_by"

    written = next(tmp_path.rglob("*.parquet"))
    assert pq.read_table(written)["event_ts"].to_pylist() == [100, 200, 300]


def test_reopen_sees_committed_data(tmp_path: Path) -> None:
    with open_log(tmp_path) as log:
        log.extend(rows(6))
        log.seal()
        log.extend(rows(2, start=6))

    with open_log(tmp_path) as reopened:
        assert len(read_all(reopened)) == 8
        assert reopened.end_offset() == 9


def test_recovery_completes_an_interrupted_seal(tmp_path: Path) -> None:
    """Crash between the Iceberg commit and the buffer delete (§4, §11).

    The rows are in the table AND still in the buffer. A read in that window
    must return each exactly once, and reopening must drop the stale rows.
    """
    with open_log(tmp_path) as log:
        log.extend(rows(4))
        extent = log._buffer.extent()
        assert extent is not None
        end = extent[1] + 1
        rel_path = log._layout.seal_path(1, end, "tok")
        log._buffer.claim_seal(1, end, rel_path)
        log._write_and_commit(1, end, rel_path)
        # deliberately NOT finish_seal: this is the crash window
        assert log._buffer.extent() is not None
        assert len(read_all(log)) == 4

    with open_log(tmp_path) as recovered:
        assert recovered._buffer.pending_seal() is None
        assert recovered._buffer.extent() is None
        assert len(read_all(recovered)) == 4


def test_recovery_redoes_a_seal_that_never_committed(tmp_path: Path) -> None:
    """Crash between claiming the range and committing the file (I2).

    The retry must reuse the claimed path rather than compute a new one.
    """
    with open_log(tmp_path) as log:
        log.extend(rows(3))
        log._buffer.claim_seal(1, 4, log._layout.seal_path(1, 4, "tok"))

    with open_log(tmp_path) as recovered:
        assert recovered._buffer.pending_seal() is None
        assert recovered.table_extent() == (1, 3)
        assert len(read_all(recovered)) == 3
        assert len(list(tmp_path.rglob("*.parquet"))) == 1, "no orphaned file"


def test_target_size_queues_a_cut_and_seal_due_writes_it(tmp_path: Path) -> None:
    """§4's size trigger, split across the two roles that own its halves.

    Crossing `target_seal_size` is the appender's business — it records the cut in
    the transaction that crosses it. Writing the file is the maintainer's, and
    `seal_due` is the call it makes. Neither half guesses what the other did.
    """
    with open_log(tmp_path, LogConfig(target_seal_size=512)) as log:
        log.extend(rows(40))

        assert log._buffer.pending_group() is not None, "the cut was not recorded"
        assert log.table_extent() is None, "an append wrote a file"

        assert log.seal_due() is not None, "seal_due found nothing queued"
        assert log.table_extent() is not None, "should have sealed on size"
        assert len(read_all(log)) == 40


def test_a_synchronous_seal_is_available(tmp_path: Path) -> None:
    """Appending never seals. `seal()` is how a caller makes the table move."""
    with open_log(tmp_path, LogConfig(target_seal_size=512)) as log:
        log.extend(rows(40))

        assert log.table_extent() is None, "an append sealed on its own"

        log.seal()

        assert log.table_extent() is not None, "seal() did not move the table"
        assert len(read_all(log)) == 40


def test_rows_stay_readable_across_a_seal(tmp_path: Path) -> None:
    """Nothing about correctness depends on when the seal lands.

    Between the Iceberg commit and the buffer delete a row is in both tiers, and
    §7's boundary excludes it from one of them — which is why a seal can happen
    whenever a maintainer gets to it without a reader noticing.
    """
    with open_log(tmp_path, LogConfig(target_seal_size=512)) as log:
        log.extend(rows(60))

        for _ in range(10):
            assert len(read_all(log)) == 60, "a row went missing before the seal"

        log.seal_due()

        for _ in range(10):
            assert len(read_all(log)) == 60, "a row went missing after the seal"


def test_a_seal_holds_the_buffer_lock_only_to_claim_and_clean_up(
    tmp_path: Path,
) -> None:
    """What the lock split actually guarantees (§13.6).

    Deliberately measures lock-hold time rather than append latency. The append
    stall is dominated by the GIL — a seal is CPU-bound in pure Python, so the
    sealing thread starves the appending one whether or not it holds a lock —
    and a test that asserted on end-to-end latency would be asserting something
    this change cannot deliver on its own.
    """
    import threading
    import time

    # The wrapper times from BEFORE acquire, so it counts waiting as holding.
    # Nothing else seals in this process, so there is nothing to wait behind.
    config = LogConfig(target_seal_size=1 << 30)
    with open_log(tmp_path, config) as log:
        log.extend(rows(400))

        held: list[float] = []
        real_lock = log._lock

        class Timed:
            def __enter__(self) -> None:
                self._at = time.perf_counter()
                real_lock.acquire()

            def __exit__(self, *_: object) -> None:
                real_lock.release()
                held.append((time.perf_counter() - self._at) * 1000)

        log._lock = cast("threading.RLock", Timed())
        started = time.perf_counter()
        log.seal()
        total = (time.perf_counter() - started) * 1000
        log._lock = real_lock

    locked = sum(held)

    assert total > 5.0, "seal was too fast to say anything about"
    assert locked < total / 2, (
        f"lock held {locked:.1f} ms of a {total:.1f} ms seal — step 2 is not lock-free"
    )


def test_only_one_seal_runs_at_a_time(tmp_path: Path) -> None:
    """`sealing` holds one row by design (§2), and two seals would overlap."""
    with open_log(tmp_path, LogConfig(target_seal_size=1 << 30)) as log:
        log.extend(rows(50))
        held = log._lease("seal")
        assert held.acquire(), "could not simulate a seal in flight"

        # Recording the cut is unconditional; writing the file is not.
        assert log.seal() == 51
        assert log.table_files() == 0, "sealed while another seal was in flight"

        held.release()
        assert log.seal() == 51
        assert log.table_files() == 1


def test_close_waits_for_an_in_flight_seal(tmp_path: Path) -> None:
    """A seal holds a claim in `sealing`; letting it finish beats replaying it."""
    log = open_log(tmp_path, LogConfig(target_seal_size=512))
    log.extend(rows(60))
    log.close()

    with open_log(tmp_path) as reopened:
        assert reopened._buffer.pending_seal() is None, "left a claim behind"
        assert len(read_all(reopened)) == 60


def test_local_retention_zero_without_an_archive_is_rejected(tmp_path: Path) -> None:
    """§8: it means 'evict on upload', and there is nothing to upload to."""
    with pytest.raises(ValueError, match="archive"):
        open_log(tmp_path, LogConfig(local_retention=timedelta(0)))


def test_readonly_sees_a_writer_s_committed_rows(tmp_path: Path) -> None:
    """A second view of a live log, as the tail script uses (§1: one writer)."""
    with open_log(tmp_path) as writer:
        writer.extend(rows(3))

        with open_log_readonly(tmp_path) as reader:
            assert len(read_all(reader)) == 3

            writer.extend(rows(2, start=3))
            assert len(read_all(reader)) == 5, "reads are not pinned to open time"

            writer.seal()
            assert len(read_all(reader)) == 5, "still exactly once across the seal"


def test_a_reader_has_no_mutation_to_refuse(tmp_path: Path) -> None:
    """Absent, not raising — which is the whole point of dropping the flag.

    `Log.open(read_only=True)` returned a `Log` whose thirteen write methods
    existed and raised "this Log was opened readonly". The interface described
    the writer rather than the thing. `reader()` returns a `LogReader`, which
    has no write surface at all, so there is nothing to gate and no
    `_writable()` call anyone can forget to add.

    Falsify by giving `LogReader` any one of these as a delegation.
    """
    with open_log(tmp_path) as writer:
        writer.extend(rows(2))

    with open_log_readonly(tmp_path) as reader:
        for absent in (
            "append",
            "extend",
            "seal",
            "await_seal",
            "maintain",
            "compact",
            "evict",
            "expire",
            "sync",
            "hydrate",
            "rewrite_archive",
            "set_config",
            "set_sort_by",
            "set_archive",
            "add_column",
            "recover",
        ):
            assert not hasattr(reader, absent), f"a reader exposes {absent}"

        # And it still answers everything a reader should.
        assert reader.end_offset() == 3
        assert len(read_all(reader)) == 2


def test_readonly_will_not_create_a_log(tmp_path: Path) -> None:
    """Opening a log that does not exist must fail rather than quietly make one."""
    with pytest.raises(FileNotFoundError, match="no litelink log at"):
        litelink.reader(tmp_path / "nothing-here", "s")


def test_maintenance_runs_on_a_background_thread(tmp_path: Path) -> None:
    """The shape examples/adsb/capture.py uses (§1: one process, threads within it).

    Python's sqlite3 defaults to check_same_thread=True, so without the
    connection flag and the lock, every background maintain() raises and the
    log grows forever while looking healthy.
    """
    import threading

    config = LogConfig(target_seal_size=2048, compact_min_files=2)
    failures: list[BaseException] = []
    stop = threading.Event()

    with open_log(tmp_path, config) as log:

        def maintain_until_stopped() -> None:
            while not stop.wait(0.01):
                try:
                    log.maintain()
                except BaseException as exc:  # noqa: BLE001 - recorded, not swallowed
                    failures.append(exc)
                    return

        worker = threading.Thread(target=maintain_until_stopped, daemon=True)
        worker.start()
        try:
            for batch in range(40):
                log.extend(rows(25, start=batch * 25))
        finally:
            stop.set()
            worker.join(timeout=10)

        assert failures == [], (
            f"maintenance failed on the worker thread: {failures[0]!r}"
        )
        assert len(read_all(log)) == 1000, (
            "concurrent maintenance lost or duplicated rows"
        )


def test_an_interrupt_inside_commit_is_not_masked(tmp_path: Path) -> None:
    """Cleanup must not raise over the exception that caused it.

    An interrupt landing inside COMMIT leaves no transaction to roll back, and
    a bare ROLLBACK in the handler then raises OperationalError with the real
    cause buried underneath — which is how a Ctrl-C during `just demo-capture`
    became a crash instead of a clean stop.
    """

    class InterruptingCursor:
        """Commits for real, then raises as an interrupt would."""

        def __init__(self, cursor: sqlite3.Cursor) -> None:
            self._cursor = cursor

        # `Any` for the parameters: this only forwards, and sqlite3's own
        # accepted type is a union of protocols not worth restating in a proxy.
        def execute(self, sql: str, parameters: Any = ()) -> sqlite3.Cursor:
            result = self._cursor.execute(sql, parameters)
            if sql == "COMMIT":
                raise KeyboardInterrupt

            return result

        def __getattr__(self, name: str) -> object:
            return getattr(self._cursor, name)

    class InterruptingConnection:
        def __init__(self, connection: sqlite3.Connection) -> None:
            self._connection = connection

        def cursor(self) -> InterruptingCursor:
            return InterruptingCursor(self._connection.cursor())

        def __getattr__(self, name: str) -> object:
            return getattr(self._connection, name)

    with open_log(tmp_path) as log:
        real = log._buffer._con
        log._buffer._con = cast("sqlite3.Connection", InterruptingConnection(real))
        try:
            with pytest.raises(KeyboardInterrupt):
                log.append(rows(1)[0])
        finally:
            log._buffer._con = real

        # The COMMIT did land, so the row is durable despite the raise.
        assert len(read_all(log)) == 1


def test_set_config_reaches_the_thing_that_makes_the_cut(tmp_path: Path) -> None:
    """§12 says policy can change under a running log. All of it, not most.

    `target_seal_size` decides where the appender cuts, and the appender is the
    buffer — so a config update that stopped at `Log` left the log sizing
    files to whatever it was opened with, silently and for its whole life.
    """
    with open_log(tmp_path, LogConfig(target_seal_size=1 << 30)) as log:
        log.extend(rows(40))

        assert log._buffer.pending_group() is None, "nothing should have crossed"

        log.set_config(LogConfig(target_seal_size=512))
        log.extend(rows(40, start=40))

        assert log._buffer.pending_group() is not None, (
            "the new target_seal_size never reached the buffer"
        )


def test_a_log_needs_no_sort_by(tmp_path: Path) -> None:
    """Offset order is the default, and it is what the rows are already in.

    Not a fallback: the buffer returns rows ordered by offset, so leaving
    `sort_by` unset means no sort runs at seal time at all. It is the cheapest
    option as well as the safest one.
    """
    with Log.new(tmp_path, "s", schema=SCHEMA) as log:
        log.extend(rows(50))
        log.seal()

        assert log.sort_by == ()
        assert log.table_files() == 1

        stored = log.scan().read_all()
        offsets = stored.column(OFFSET).to_pylist()
        assert offsets == list(range(1, 51)), (
            "a sealed file must hold its rows in offset order, so replay from "
            "an offset is a sequential read rather than a sort"
        )


def test_an_unsorted_log_reopens_unsorted(tmp_path: Path) -> None:
    """`open` recovers the shape from the table, and "no sort order" is a
    shape — not a missing value to be guessed at."""
    with Log.new(tmp_path, "s", schema=SCHEMA) as log:
        log.extend(rows(4))

    with Log.open(tmp_path, "s") as reopened:
        assert reopened.sort_by == ()


def test_a_sort_key_correlated_with_the_offset_keeps_files_prunable(
    tmp_path: Path,
) -> None:
    """Why the guidance is about CORRELATION rather than about sorting.

    Files hold contiguous offset ranges however they are sorted internally. A
    sort column that tracks arrival stays contiguous with them, so each file's
    min/max on it covers a narrow slice and a predicate prunes whole files. A
    scattered one gives every file nearly the whole domain, and there is
    nothing left to prune with.
    """
    with Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)) as log:
        for batch in range(4):
            log.extend(rows(20, start=batch * 20))
            log.seal()

        files = log._table.data_files()
        assert len(files) == 4

        stored = [pq.read_table(f.path).column("event_ts").to_pylist() for f in files]
        spans = [(min(c), max(c)) for c in stored]
        assert all(spans[i][1] < spans[i + 1][0] for i in range(len(spans) - 1)), (
            "correlated keys leave each file a disjoint slice, so files prune"
        )


def test_append_refuses_a_column_this_log_does_not_have(tmp_path: Path) -> None:
    """Nothing below catches this, and the checks that exist fire too late.

    `_insert` builds each row as `tuple(row.get(c) for c in columns)` — it
    enumerates the SCHEMA's columns, never the row's keys — so an unknown key
    is dropped before any SQL exists. Neither SQLite nor pyarrow ever sees it,
    and `append` returns an offset for a row it silently truncated.
    """
    with open_log(tmp_path) as log:
        with pytest.raises(ValueError, match="does not have: \\['region'\\]"):
            log.append({"event_ts": 1, "key": "a", "payload": "p", "region": "eu"})

        # And the message names the declared set, because "unknown column" on
        # its own does not tell you whether you misspelled or misremembered.
        with pytest.raises(ValueError, match="Declared:"):
            log.append({"event_ts": 1, "key": "a", "payload": "p", "region": "eu"})

        assert log.end_offset() == 1, "a refused row must not consume an offset"


def test_append_refuses_a_typo_that_shadows_a_declared_column(
    tmp_path: Path,
) -> None:
    """The row a length check cannot catch, and the reason there is not one.

    A first design guarded the subset test with `len(row) != width`, on the
    reasoning that a matching width means every declared column is present.
    It does not: it means as many keys are missing as are unknown. One typo —
    `ky` for `key` — has the right width, skips the test, stores NULL in the
    column it shadowed, and if that column is non-nullable every scan and every
    seal then fails for ever while appends keep succeeding.
    """
    with open_log(tmp_path) as log:
        with pytest.raises(ValueError, match="does not have: \\['ky'\\]"):
            log.append({"event_ts": 1, "ky": "a", "payload": "p"})

        assert log.end_offset() == 1


def test_the_unknown_column_fast_path_cannot_be_fooled(tmp_path: Path) -> None:
    """The width test is skipped only when the row proves it is complete.

    `_insert` avoids the set test when every declared column came back
    non-None AND the row has exactly that many keys — which together mean the
    row holds the declared columns and nothing else. Each case below attacks
    one half of that pairing:

    - a full-width row with a real value in every column PLUS an extra key has
      the wrong width, so the fast path does not apply;
    - a row of exactly the right width that hides an unknown key behind an
      absent one (`ky` for `key`) has a None among its values, which is what
      sends it to the full test. This is the shape that broke the original
      `len(row) != width and not declared(row)` predicate.

    Falsify by dropping the `None in values` half: the second row is accepted
    and `key` is silently NULL.
    """
    with open_log(tmp_path) as log:
        # Every column supplied and non-None, plus one extra: width differs.
        with pytest.raises(ValueError, match="does not have: \\['region'\\]"):
            log.append({"event_ts": 1, "key": "a", "payload": "p", "region": "eu"})

        # Exactly the declared width, but one key is a typo for another.
        with pytest.raises(ValueError, match="does not have: \\['ky'\\]"):
            log.append({"event_ts": 1, "ky": "a", "payload": "p"})

        # And the fast path itself still accepts the row it exists for.
        log.append({"event_ts": 1, "key": "a", "payload": "p"})

        assert log.scan().read_all().num_rows == 1


def test_append_still_accepts_a_row_missing_a_nullable_column(
    tmp_path: Path,
) -> None:
    """The refusal is about UNKNOWN columns, not absent ones.

    A missing nullable column is a legal row and stores NULL — the shape a
    subset test permits and a width test would not.
    """
    with open_log(tmp_path) as log:
        log.append({"event_ts": 1, "key": "a"})

        rows = log.scan().read_all().to_pylist()

        assert rows[0]["payload"] is None


def test_append_refuses_a_row_omitting_a_non_nullable_column(
    tmp_path: Path,
) -> None:
    """The unknown-column wedge from the other side (I17).

    Naming a column the log lacks and failing to supply one it requires both
    end as a NULL nothing below catches. `row.get` yields None for an absent
    key, SQLite stores it, and the scan cast is where it finally raises — long
    after `append` handed back an offset.
    """
    with open_log(tmp_path) as log:
        with pytest.raises(ValueError, match="non-nullable columns NULL"):
            log.append({"key": "a", "payload": "p"})

        # Named, and named as absent rather than as None: the two have the same
        # consequence but not the same fix.
        with pytest.raises(ValueError, match="Absent from the row: \\['event_ts'\\]"):
            log.append({"key": "a", "payload": "p"})

        assert log.end_offset() == 1, "a refused row must not consume an offset"


def test_append_refuses_a_non_nullable_column_supplied_as_none(
    tmp_path: Path,
) -> None:
    """Explicitly None is the same NULL as absent, so it is the same refusal.

    Worth its own test because the obvious implementation — checking the row's
    KEYS against the required set — passes this row: `event_ts` is present. It
    is the VALUE that wedges the log, so the check has to read values.
    """
    with open_log(tmp_path) as log:
        with pytest.raises(ValueError, match="Supplied as None: \\['event_ts'\\]"):
            log.append({"event_ts": None, "key": "a", "payload": "p"})

        assert log.end_offset() == 1


def test_the_refusal_protects_rows_written_before_the_bad_one(
    tmp_path: Path,
) -> None:
    """Why this is data loss and not just a bad row.

    The failing cast is a property of the FILE, not of the row, so one NULL in
    a non-nullable column takes every row in the log down with it — including
    the ones acknowledged long before. Meanwhile `append` keeps succeeding, so
    a writer sees a healthy log while every reader sees nothing.

    Falsify by removing the `_required` scan from `_insert`: this scan then
    raises `Casting field 'event_ts' with null values to non-nullable` and the
    100 good rows above become unreadable.
    """
    with open_log(tmp_path) as log:
        log.extend(rows(100))

        with pytest.raises(ValueError, match="non-nullable columns NULL"):
            log.append({"key": "late", "payload": "p"})

        assert log.scan().read_all().num_rows == 100, (
            "the rows acknowledged before the bad one must still read"
        )


def test_a_typo_on_a_non_nullable_column_names_the_unknown_key(
    tmp_path: Path,
) -> None:
    """One typo trips both halves of I17, and the useful half wins.

    `event_tz` is undeclared AND leaves the non-nullable `event_ts` absent.
    The caller mistyped one key, so naming that key is what shortens the
    search; "event_ts is missing" describes the consequence, not the cause.
    """
    with open_log(tmp_path) as log:
        with pytest.raises(ValueError, match="does not have: \\['event_tz'\\]"):
            log.append({"event_tz": 1, "key": "a", "payload": "p"})

        assert log.end_offset() == 1


TYPED = pa.schema(
    [
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("key", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("flag", pa.bool_()),
    ]
)


def test_append_refuses_a_value_that_would_wedge_every_scan(
    tmp_path: Path,
) -> None:
    """SQLite has affinities, not types, so it stores whatever it is given.

    The declared schema is not consulted again until the value is read back —
    by which point `append` has returned an offset. A value Arrow cannot parse
    then makes EVERY scan raise, including scans of rows written before it,
    while appends keep succeeding.

    Falsify by removing the type gate from `_insert`: the append succeeds and
    the assert on the surviving rows fails with `ArrowInvalid`.
    """
    log = Log.new(tmp_path, "s", schema=TYPED)
    with log:
        log.extend([{"event_ts": i, "key": "k"} for i in range(50)])

        for row in (
            {"event_ts": "not-a-number", "key": "k"},
            {"event_ts": 1, "price": "expensive"},
            {"event_ts": 1, "key": b"\xff\xfe"},
        ):
            with pytest.raises(ValueError, match="wrong type"):
                log.append(row)

        assert log.scan().read_all().num_rows == 50, (
            "the rows acknowledged before the bad one must still read"
        )


def test_append_refuses_a_value_that_would_be_silently_rewritten(
    tmp_path: Path,
) -> None:
    """The quiet half, and the worse one.

    These parse. They just do not survive: `1.5` into an int64 reads back as
    `1` and `12345` into a string as `'12345'`. No error is raised anywhere,
    so what comes out is not what went in.

    Caught by the DDL now rather than by Python — `STRICT` refuses the REAL,
    and a string column is declared `ANY` so a `typeof` CHECK can see that
    `12345` is an integer before SQLite would have stringified it.
    """
    log = Log.new(tmp_path, "s", schema=TYPED)
    with log:
        for row in (
            {"event_ts": 1.5, "key": "k"},
            {"event_ts": 1, "key": 12345},
            {"event_ts": 1, "flag": 7},
        ):
            with pytest.raises(ValueError, match="wrong type"):
                log.append(row)

        assert log.end_offset() == 1, "a refused row must not consume an offset"

        # `True` into an int64 is the one leniency, and it is deliberate.
        # Python's sqlite3 driver converts a bool to 1 before SQLite sees the
        # value, so no constraint can distinguish it from a plain int — and
        # re-adding a Python check for this alone would cost the whole per-row
        # gate the DDL just replaced. It is lossless: `True == 1` in Python,
        # and Arrow would store 1 either way.
        log.append({"event_ts": True, "key": "k"})

        assert log.scan().read_all().column("event_ts").to_pylist() == [1]


def test_append_refuses_a_string_that_sqlite_would_convert(
    tmp_path: Path,
) -> None:
    """The reason every column is declared ANY.

    A STRICT column of a declared type does not refuse a wrong value, it
    CONVERTS one — an INTEGER column given `'77'` stores 77 and `'007'` stores
    7, a REAL column given `'1e999'` stores `inf`. The conversion happens
    before any CHECK could see it, so a constraint on a typed column would be
    asked about a value that had already been changed.

    `'007'` is the one that shows why "lossless" is not the test: it survives
    as 7, and no round trip gets the original back.

    Falsify by declaring the columns with their affinities instead of ANY:
    every row below is accepted and silently rewritten.
    """
    log = Log.new(tmp_path, "s", schema=TYPED)
    with log:
        for row in (
            {"event_ts": "77"},
            {"event_ts": "007"},
            {"event_ts": " 77 "},
            {"event_ts": 1, "price": "1e999"},
            {"event_ts": 1, "flag": "0"},
        ):
            with pytest.raises(ValueError, match="wrong type"):
                log.append(row)

        assert log.end_offset() == 1


def test_a_column_added_later_is_validated_like_any_other(
    tmp_path: Path,
) -> None:
    """`ALTER TABLE ADD COLUMN` must carry the same DDL as `CREATE TABLE`.

    It did not, and `_create` is `CREATE TABLE IF NOT EXISTS`, so the gap
    survived every reopen: a column added by `add_column` was unvalidated for
    the life of the log. An int32 given 2**40 was stored, `append` returned an
    offset, and then every scan AND every seal raised for ever while appends
    kept succeeding — the buffer could never drain.

    Falsify by building the ALTER from the affinity alone: all three rows
    below are accepted, and the seal at the end raises `ArrowInvalid`.
    """
    log = Log.new(tmp_path, "s", schema=TYPED)
    with log:
        log.extend([{"event_ts": i} for i in range(5)])
        log.add_column("late", pa.int32())

        for row in (
            {"event_ts": 9, "late": 2**40},
            {"event_ts": 9, "late": "5"},
            {"event_ts": 9, "late": 1.5},
        ):
            with pytest.raises(ValueError, match="wrong type|cannot hold"):
                log.append(row)

        log.append({"event_ts": 9, "late": 7})
        log.seal()

        assert log.scan().read_all().num_rows == 6


def test_append_refuses_an_integer_a_float_column_cannot_hold(
    tmp_path: Path,
) -> None:
    """An int is a legal float only while the conversion is lossless.

    The buffer stores values with no conversion, so an out-of-range integer
    stays an INTEGER in SQLite and `pa.array(..., type=float64)` then refuses
    to build the column AT ALL. One such value made every scan and every seal
    raise for ever — including for rows written before it — while `append`
    kept returning offsets, so the buffer could never drain.

    The bound is what the type represents exactly: 2**53 for float64, 2**24
    for float32. `-(2**63)` is here because testing the integer with `abs`
    raised SQLite's `OperationalError` out of the CHECK — there is no positive
    counterpart for the most-negative int64 — instead of a refusal.

    Falsify by allowing `typeof IN ('integer','real')` without the bound: all
    four rows are accepted and the scan at the end raises `ArrowInvalid`.
    """
    log = Log.new(tmp_path, "s", schema=RANGED_FLOAT)
    with log:
        log.extend([{"k": i, "f64": 1.5} for i in range(5)])

        for row in (
            {"k": 9, "f64": 2**53 + 1},
            {"k": 9, "f32": 2**24 + 1},
            {"k": 9, "f32": -(2**63)},
            {"k": 9, "f64": -(2**63)},
        ):
            with pytest.raises(ValueError, match="cannot hold the integer"):
                log.append(row)

        assert log.scan().read_all().num_rows == 5


def test_a_float_column_still_takes_an_integer_it_can_hold(
    tmp_path: Path,
) -> None:
    """`{"price": 5}` is too natural to refuse, and the exact bounds are legal."""
    log = Log.new(tmp_path, "s", schema=RANGED_FLOAT)
    with log:
        log.append({"k": 1, "f64": 5})
        log.append({"k": 2, "f64": 2**53})
        log.append({"k": 3, "f64": -(2**53)})
        log.append({"k": 4, "f32": 2**24})
        log.seal()

        got = log.scan().read_all().column("f64").to_pylist()

        assert got[:3] == [5.0, float(2**53), float(-(2**53))]


def test_the_type_refusal_names_the_column_the_value_and_the_declared_type(
    tmp_path: Path,
) -> None:
    """ "Wrong type" alone does not say which of six columns, or what it wanted."""
    log = Log.new(tmp_path, "s", schema=TYPED)
    with (
        log,
        pytest.raises(ValueError, match=r"event_ts=1\.5 \(float, declared int64\)"),
    ):
        log.append({"event_ts": 1.5, "key": "k"})


def test_append_accepts_values_the_exact_check_alone_would_refuse(
    tmp_path: Path,
) -> None:
    """The fast gate is `type(v) in carriers`; legality is a broader question.

    A `StrEnum` member IS a string and must be stored as one. An `int` in a
    float column is lossless and too natural to refuse. Both miss the exact
    check and are then allowed by `accepts` — which is why a miss is not a
    refusal.
    """
    import enum

    class Region(enum.StrEnum):
        EU = "eu-west-1"

    log = Log.new(tmp_path, "s", schema=TYPED)
    with log:
        log.append({"event_ts": 1, "key": Region.EU, "price": 5})

        table = log.scan().read_all()

        assert table.column("key").to_pylist() == ["eu-west-1"]
        assert table.column("price").to_pylist() == [5.0]


RANGED_FLOAT = pa.schema(
    [
        pa.field("k", pa.int64(), nullable=False),
        pa.field("f64", pa.float64()),
        pa.field("f32", pa.float32()),
    ]
)

RANGED = pa.schema(
    [
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("n32", pa.int32()),
        pa.field("f32", pa.float32()),
    ]
)


def test_append_refuses_a_value_the_column_cannot_hold(tmp_path: Path) -> None:
    """Right type, wrong magnitude — which the type gate cannot see.

    `2**40` IS an int and `1e300` IS a float, so both pass an exact-type check
    and then fail differently: the int32 is stored unchanged and makes every
    scan raise `Integer value ... not in range`, while the float32 reads back
    as `inf` with no error raised anywhere.
    """
    log = Log.new(tmp_path, "s", schema=RANGED)
    with log:
        log.extend([{"event_ts": i} for i in range(20)])

        for row in (
            {"event_ts": 1, "n32": 2**40},
            {"event_ts": 1, "n32": -(2**40)},
            {"event_ts": 1, "f32": 1e300},
        ):
            with pytest.raises(ValueError, match="cannot hold"):
                log.append(row)

        assert log.scan().read_all().num_rows == 20


def test_append_accepts_the_exact_bounds_and_an_explicit_infinity(
    tmp_path: Path,
) -> None:
    """The check is a range, not a smaller one, and `inf` is a real float32.

    A float32 represents infinity exactly, so passing one is a statement rather
    than an overflow. What is refused is a FINITE value that would silently
    become infinite. Falsify by dropping the `_INFINITE` clause: the explicit
    infinities below are then refused.
    """
    log = Log.new(tmp_path, "s", schema=RANGED)
    with log:
        log.append({"event_ts": 1, "n32": 2**31 - 1})
        log.append({"event_ts": 2, "n32": -(2**31)})
        log.append({"event_ts": 3, "f32": float("inf")})
        log.append({"event_ts": 4, "f32": float("-inf")})
        # A column that CAN overflow but was not supplied is not a range fault.
        log.append({"event_ts": 5})

        table = log.scan().read_all()

        assert table.column("n32").to_pylist()[:2] == [2**31 - 1, -(2**31)]
        assert table.column("f32").to_pylist()[2:4] == [float("inf"), float("-inf")]


def test_extend_refuses_the_batch_without_writing_any_of_it(
    tmp_path: Path,
) -> None:
    """One bad row rejects the batch, and the batch is one transaction.

    `_insert` runs inside `BEGIN IMMEDIATE`, so raising part way rolls the
    whole thing back. That matters more than it looks: a partial batch would
    hand back offsets for rows the caller never learns were stored.
    """
    with open_log(tmp_path) as log:
        good = [{"event_ts": i, "key": f"k{i}", "payload": "p"} for i in range(50)]
        bad = [*good, {"event_ts": 50, "key": "k50", "payload": "p", "region": "eu"}]

        with pytest.raises(ValueError, match="does not have"):
            log.extend(bad)

        assert log.end_offset() == 1, "the batch was partly written"
        assert log.scan().read_all().num_rows == 0


def test_a_row_that_is_wrong_twice_names_the_forbidden_column_first(
    tmp_path: Path,
) -> None:
    """I11 before I17, when a row breaks both.

    `litelink_offset` is in the declared set, so the subset test passes it
    through either way and the ORDER of the two checks is what decides which
    error the caller reads. Supplying an offset is the specifically forbidden
    thing; "unknown column" would send them looking at the wrong key.
    """
    with open_log(tmp_path) as log:
        with pytest.raises(ValueError, match="I11"):
            log.append(
                {
                    "event_ts": 1,
                    "key": "a",
                    "payload": "p",
                    "litelink_offset": 9,
                    "region": "eu",
                }
            )


def test_start_offset_reserves_the_range_below_it(tmp_path: Path) -> None:
    """`[1, N-1]` is left unassigned for ever (§13.4).

    The reserve exists so a cutover can point live capture at litelink today
    and backfill the history later, into offsets live capture will never take.
    """
    log = Log.new(tmp_path, "s", schema=SCHEMA, start_offset=1000)
    with log:
        assert log.append({"event_ts": 1, "key": "a", "payload": "p"}) == 1000
        assert log.extend(rows(3)) == [1001, 1002, 1003]
        log.seal()

        assert log._table.extent() == (1000, 1003)
        assert log.scan().read_all().num_rows == 4


def test_start_offset_is_recorded_durably(tmp_path: Path) -> None:
    """And only when there IS a reserve.

    A backfill has to tell this reserve from a `Log.restore` fence, and nothing
    else can: both are empty ranges below the log's offsets, and `Log` says
    "the skipped range leaves no trace once the sequence has moved". Absent
    must therefore mean "no reserve" rather than "a reserve of nothing" — a log
    created at 1 and later restored has a gap below its offsets too, and that
    gap is a fence that must never be filled.
    """
    log = Log.new(tmp_path / "seeded", "s", schema=SCHEMA, start_offset=1000)
    with log:
        assert log._buffer.get_meta("start_offset") == "1000"

    with Log.open(tmp_path / "seeded", "s") as reopened:
        assert reopened._buffer.get_meta("start_offset") == "1000"
        assert reopened.end_offset() == 1000

    plain = Log.new(tmp_path / "plain", "s", schema=SCHEMA)
    with plain:
        assert plain._buffer.get_meta("start_offset") is None
        assert plain.append({"event_ts": 1}) == 1


def test_start_offset_below_one_is_refused(tmp_path: Path) -> None:
    """`1` is the current behaviour, not an error; below it is meaningless."""
    for bad in (0, -1):
        with pytest.raises(ValueError, match="at least 1"):
            Log.new(tmp_path / str(bad), "s", schema=SCHEMA, start_offset=bad)

    with Log.new(tmp_path / "one", "s", schema=SCHEMA, start_offset=1) as log:
        assert log.append({"event_ts": 1}) == 1


def test_the_tail_cache_serves_a_seeded_log_before_its_first_seal(
    tmp_path: Path,
) -> None:
    """Assert HITS, not rows — the rows are right either way.

    `_tail_lo` is `first_offset - 1`, which on a seeded log is far above the
    boundary `Reader.query` passes while the local table has no extent (0). A
    guard gating on `_tail_lo <= floor` therefore missed on every read and
    re-converted the whole buffer per query — 4.2 ms/read against 42 at the
    default 8 MiB first-seal window.

    Three broken variants all return correct rows and all pass the rest of this
    suite: the unguarded original (0 hits of 20), one that pins `_tail_from` at
    the buffer's floor instead of only raising it (about half), and one that
    drops the lower bound entirely. So this asserts the mechanism.
    """
    log = Log.new(tmp_path, "s", schema=SCHEMA, start_offset=1_000_000)
    with log:
        log.extend(rows(500))

        hits = 0
        original = Buffer._reusable

        def counting(self: Buffer, floor: int) -> pa.Table | None:
            nonlocal hits
            got = original(self, floor)
            hits += got is not None

            return got

        Buffer._reusable = counting  # type: ignore[method-assign]
        try:
            for _ in range(5):
                log.scan().read_all()
        finally:
            Buffer._reusable = original  # type: ignore[method-assign]

        # The first read builds it; every later one must reuse it.
        assert hits == 4, f"tail cache missed on a seeded log: {hits} of 4"


def test_the_tail_cache_prunes_what_the_boundary_excludes(tmp_path: Path) -> None:
    """The slice must still drop rows at or below `floor`.

    Asked of `rows_above` directly, because going through `scan()` cannot see
    it: a seal deletes the rows it covered, so the buffer holds nothing below
    the boundary and the slice has nothing to prune. The bug this guards
    against — returning the whole cache regardless of `floor` — is invisible
    there and returns duplicate rows here.

    Falsify by slicing at `self._tail_lo` instead of `max(floor, _tail_lo)`:
    the second call returns 40 rows instead of 20.
    """
    with open_log(tmp_path) as log:
        log.extend(rows(40))
        buffer = log._buffer

        assert buffer.rows_above(0).num_rows == 40
        assert buffer.rows_above(20).num_rows == 20
        assert buffer.rows_above(39).num_rows == 1
        assert buffer.rows_above(40).num_rows == 0


def test_the_tail_cache_refuses_a_boundary_below_what_it_holds(
    tmp_path: Path,
) -> None:
    """The lower bound is a correctness guard, not an optimisation.

    A cache built for a high boundary is missing every row below it. Serving it
    to a lower boundary returns fewer rows than exist — silently, since the
    result looks like a perfectly ordinary answer.

    Falsify by dropping the lower bound (`floor <= _tail_hi` alone): the second
    call returns the 10 cached rows instead of all 40.
    """
    with open_log(tmp_path) as log:
        log.extend(rows(40))
        buffer = log._buffer

        assert buffer.rows_above(30).num_rows == 10
        assert buffer.rows_above(0).num_rows == 40
