"""The core capture loop: append, seal, read, recover."""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

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


def open_log_readonly(root: Path) -> Log:
    return Log.open(root, "s", read_only=True)


def rows(n: int, *, start: int = 0) -> list[dict[str, object]]:
    return [
        {"event_ts": 1000 + i, "key": f"k{i % 3}", "payload": f'{{"seq":{i}}}'}
        for i in range(start, start + n)
    ]


def read_all(log: Log) -> list[tuple[object, ...]]:
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
            log.append(
                {"litelink_offset": 7, "event_ts": 1, "key": "k", "payload": b""}
            )


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
            [{"event_ts": ts, "key": "k", "payload": b""} for ts in (300, 100, 200)]
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


def test_readonly_refuses_every_mutation(tmp_path: Path) -> None:
    with open_log(tmp_path) as writer:
        writer.extend(rows(2))

    with open_log_readonly(tmp_path) as reader:
        for mutate in (
            lambda: reader.append(rows(1)[0]),
            lambda: reader.extend(rows(1)),
            reader.seal,
            reader.maintain,
        ):
            with pytest.raises(RuntimeError, match="readonly"):
                mutate()


def test_readonly_will_not_create_a_log(tmp_path: Path) -> None:
    """Opening a log that does not exist must fail rather than quietly make one."""
    with pytest.raises(FileNotFoundError, match="no litelink log at"):
        Log.open(tmp_path / "nothing-here", "s", read_only=True)


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

        assert log._sort_by == ()
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
        assert reopened._sort_by == ()


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
