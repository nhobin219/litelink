"""The seal queue: where the cut is made, and by whom (SPEC §4).

`target_size` is the library's one promise about file size, and a running byte
counter cannot keep it. A counter says a threshold was crossed, never where —
so a sealer that polls one cuts wherever the buffer has reached by the time it
looks, and the file it writes measures how far behind the sealer was rather
than what was asked for. These tests are about the cut being decided by the
appender, in the transaction that crosses it.
"""

from __future__ import annotations

import threading
import time
from datetime import timedelta
from typing import TYPE_CHECKING

import pyarrow as pa

from litelink import Log, LogConfig

if TYPE_CHECKING:
    from pathlib import Path

SCHEMA = pa.schema(
    [
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("key", pa.string()),
        pa.field("payload", pa.string()),
    ]
)

# Wide enough that a row is a meaningful fraction of the target below, so an
# overshoot of even a few rows would show up rather than hiding in the noise.
PAYLOAD = "p" * 400
TARGET = 32 * 1024


def open_log(root: Path, config: LogConfig | None = None) -> Log:
    return Log.new(root, "s", schema=SCHEMA, sort_by=("event_ts",), config=config)


def rows(n: int, *, start: int = 0) -> list[dict[str, object]]:
    return [
        {"event_ts": 1000 + i, "key": f"k{i % 3}", "payload": PAYLOAD}
        for i in range(start, start + n)
    ]


def groups(log: Log) -> list[tuple[int, int | None, int | None, int]]:
    """Every queue row: `(group_id, start, end, bytes)`, oldest first."""
    return [
        (int(g), s, e, int(b))
        for g, s, e, b in log._buffer._con.execute(
            "SELECT group_id, start_offset, end_offset, bytes FROM seal_group"
            " ORDER BY group_id"
        ).fetchall()
    ]


def quiet(**kwargs: object) -> LogConfig:
    """A config whose sealer will not act behind the test's back.

    NOT `seal_mode="inline"` — that seals on every append, retiring a group
    before a test can look at it. A background sealer with a poll it will
    never reach leaves the queue intact and still exercises the real path.
    `close()` wakes it immediately, so nothing waits an hour.
    """
    return LogConfig(
        target_size=TARGET,
        seal_poll=timedelta(hours=1),
        snapshot_retention=timedelta(days=1),
        **kwargs,  # ty: ignore[invalid-argument-type]
    )


def test_a_closed_group_is_never_smaller_than_the_target(tmp_path: Path) -> None:
    """The cut is at the first row that crosses, so the group holds at least it."""
    with open_log(tmp_path, quiet()) as log:
        log.extend(rows(400))
        closed = [g for g in groups(log) if g[2] is not None]

        assert closed, "nothing crossed the target"
        for _, _, _, size in closed:
            assert size >= TARGET, f"cut early at {size} bytes, target {TARGET}"


def test_a_closed_group_overshoots_by_at_most_one_row(tmp_path: Path) -> None:
    """Exactly at the crossing row, not at the end of the batch.

    Cutting on the batch boundary would make file size depend on how the caller
    chose to batch — which §1 says carries no meaning of its own. Appended here
    in one 400-row batch precisely so a batch-boundary cut would fail this.
    """
    with open_log(tmp_path, quiet()) as log:
        log.extend(rows(400))
        one_row = len(PAYLOAD) + 32

        for _, _, end, size in groups(log):
            if end is None:
                continue

            assert size < TARGET + one_row, (
                f"group ran to {size} bytes, more than one row past {TARGET}"
            )


def test_a_batch_that_crosses_many_times_cuts_many_times(tmp_path: Path) -> None:
    """One transaction, several files. The batch is not the unit of anything."""
    with open_log(tmp_path, quiet()) as log:
        log.extend(rows(500))

        closed = [g for g in groups(log) if g[2] is not None]

        assert len(closed) > 3, f"one batch produced {len(closed)} cuts"


def test_groups_tile_the_offset_space_without_gap_or_overlap(tmp_path: Path) -> None:
    """Each file picks up exactly where the last left off (I3 depends on it)."""
    with open_log(tmp_path, quiet()) as log:
        log.extend(rows(400))

        boundary = 1
        for _, start, end, _ in groups(log):
            if end is None:
                continue

            assert start == boundary, f"group starts at {start}, expected {boundary}"
            boundary = end


def test_a_sealer_that_falls_behind_still_writes_sized_files(tmp_path: Path) -> None:
    """The whole point of queueing the cut rather than the trigger.

    Nothing seals while 20 groups' worth of rows arrive. A sealer reading a
    byte counter would then cut once, at whatever the buffer had reached, and
    emit a single file twenty times the target.
    """
    with open_log(tmp_path, quiet()) as log:
        log.extend(rows(2000))
        queued = [g for g in groups(log) if g[2] is not None]

        assert len(queued) > 5, "not enough backlog to be a test"

        # One call drains the whole backlog: it cuts what is still open, then
        # seals everything up to that cut.
        assert log.seal() is not None
        assert log.seal() is None, "left work behind"
        assert log.table_files() == len(queued) + 1, (
            f"{len(queued)} queued plus the open group, {log.table_files()} files"
        )

        sizes = [f.size for f in log._table.data_files()]

        assert max(sizes) < TARGET * 2, (
            f"largest file {max(sizes)} bytes — the backlog was merged into one"
        )


def test_an_explicit_seal_cuts_its_own_rows_whatever_else_is_running(
    tmp_path: Path,
) -> None:
    """The cut must not depend on how far behind a sealer happens to be.

    `seal` used to cut only when the queue was empty, so a call made while a
    background sealer still had a group queued left the caller's rows uncut and
    sealed an older group instead — sometimes returning None having sealed
    nothing. Eight appends and eight seals then produced SEVEN files, one of
    them holding two appends' worth of rows. Same calls, different data
    layout, decided by a race.

    Run with a live background sealer on a short poll, which is what made it
    reproduce.
    """
    config = LogConfig(
        target_size=1 << 30,
        seal_poll=timedelta(milliseconds=1),
        snapshot_retention=timedelta(days=1),
    )
    with open_log(tmp_path, config) as log:
        for i in range(8):
            log.extend(rows(20, start=i * 20))
            log.seal()

        assert log.await_seal(timeout=30), "a queued cut was never written"
        assert log.table_files() == 8, "eight appends and eight seals, eight files"
        assert log.table_rows() == 160


def test_an_empty_group_is_never_closed(tmp_path: Path) -> None:
    """`seal()` on an untouched log has nothing to write, not an empty file."""
    with open_log(tmp_path, quiet()) as log:
        assert log.seal() is None
        assert log._table.data_files() == []


def test_max_age_seals_a_stream_that_never_fills_a_group(tmp_path: Path) -> None:
    """§4's other trigger, which was dead config until the queue existed.

    Without it a quiet stream never reaches Iceberg at all: it sits in SQLite
    indefinitely, because the only trigger was a byte threshold it never met.
    """
    config = quiet(max_age=timedelta(0))
    with open_log(tmp_path, config) as log:
        log.extend(rows(3))

        assert log._table.data_files() == [], "nothing should have crossed the target"

        closed = log._buffer.close_open_group(int(time.time()))

        assert closed, "an aged group was not closed"
        assert log.seal() == 4
        assert log.table_rows() == 3


def test_max_age_leaves_a_young_group_alone(tmp_path: Path) -> None:
    """Or every poll would emit a stub file, which is §6's whole complaint."""
    with open_log(tmp_path, quiet()) as log:
        log.extend(rows(3))

        assert not log._buffer.close_open_group(int(time.time()) - 3600)
        assert log.seal() is not None, "an explicit seal still cuts"


def test_the_age_clock_starts_with_the_first_row_not_the_group(tmp_path: Path) -> None:
    """An idle group would otherwise seal a one-row file the moment it filled."""
    with open_log(tmp_path, quiet()) as log:
        log.extend(rows(1))
        log.seal()

        # A fresh, empty group now exists and is about to sit idle.
        time.sleep(1.1)
        opened_before = log._buffer._con.execute(
            "SELECT opened_at FROM seal_group WHERE end_offset IS NULL"
        ).fetchone()[0]

        assert opened_before is None, "an empty group carries no age"

        log.extend(rows(1, start=50))
        opened_after = log._buffer._con.execute(
            "SELECT opened_at FROM seal_group WHERE end_offset IS NULL"
        ).fetchone()[0]

        assert opened_after >= int(time.time()) - 1, "clock started before the row"


def test_a_reopened_log_adopts_the_rows_it_finds(tmp_path: Path) -> None:
    """The seeding scan, which is the only SUM() left and runs once per open."""
    root = tmp_path
    log = open_log(root, quiet())
    log.extend(rows(10))
    log.close()

    reopened = Log.open(root, "s")
    try:
        open_group = [g for g in groups(reopened) if g[2] is None]

        assert len(open_group) == 1, f"expected one open group, got {open_group}"
        assert open_group[0][1] == 1, "did not adopt the buffered rows"
        assert open_group[0][3] > 0, "adopted the rows but not their size"
    finally:
        reopened.close()


def test_reading_while_writing_does_not_corrupt_the_buffer(tmp_path: Path) -> None:
    """Regression: DuckDB must not open the buffer database itself.

    Attaching it put the file under two independently linked SQLite libraries
    in one process. POSIX advisory locks are per process and per inode, and
    each library keeps its own table of open descriptors to compensate — so one
    closing a handle dropped the other's locks and the two stopped being
    serialised. This corrupted the database on the FIRST concurrent scan:
    "database disk image is malformed", with a torn -shm mmap raising SIGBUS.
    """
    config = LogConfig(
        target_size=TARGET,
        seal_poll=timedelta(milliseconds=10),
        snapshot_retention=timedelta(days=1),
    )
    with open_log(tmp_path, config) as log:
        stop = threading.Event()
        failures: list[str] = []

        def write() -> None:
            i = 0
            while not stop.is_set():
                try:
                    log.extend(rows(50, start=i))
                    i += 50
                except Exception as exc:  # noqa: BLE001 - reported, not raised
                    failures.append(f"write: {exc}")
                    return

        writer = threading.Thread(target=write)
        writer.start()
        try:
            for _ in range(25):
                log.scan().read_all()
        except Exception as exc:  # noqa: BLE001 - the failure this guards
            failures.append(f"scan: {exc}")
        finally:
            stop.set()
            writer.join(30)

        assert not failures, failures

        check = log._buffer._con.execute("PRAGMA integrity_check").fetchall()

        assert check == [("ok",)], f"buffer database damaged: {check}"
