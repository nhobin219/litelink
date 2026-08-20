"""The seal queue: where the cut is made, and by whom (SPEC §4).

`target_size` is the library's one promise about file size, and a running byte
counter cannot keep it. A counter says a threshold was crossed, never where —
so a sealer that polls one cuts wherever the buffer has reached by the time it
looks, and the file it writes measures how far behind the sealer was rather
than what was asked for. These tests are about the cut being decided by the
appender, in the transaction that crosses it.
"""

from __future__ import annotations

import shutil
import threading
from datetime import UTC, date, datetime, timedelta
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


def _today() -> date:
    return datetime.now(UTC).date()


def groups(log: Log) -> list[tuple[int, int | None, int | None, int]]:
    """Every queue row: `(group_id, start, end, bytes)`, oldest first.

    Unnamed extents only. A row keeps its identity after it seals — it gains
    the file's name and stops being queue — so the queue is the extents that
    have no file yet.
    """
    return [
        (int(g), s, e, int(b))
        for g, s, e, b in log._buffer._con.execute(
            "SELECT group_id, start_offset, end_offset, bytes FROM extent"
            " WHERE rel_path IS NULL ORDER BY group_id"
        ).fetchall()
    ]


def quiet(**kwargs: object) -> LogConfig:
    """The ordinary config. Nothing seals unless a test asks it to.

    There is no sealer to quieten any more: appending records cuts and
    `seal`/`seal_due` are the only things that write files, so the queue stays
    exactly as the appends left it.
    """
    return LogConfig(
        target_size=TARGET,
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

    Run against a maintainer draining the same queue — a thread here, a process
    in a deployment; the lease cannot tell the difference — because competition
    is what made it reproduce.
    """
    config = LogConfig(target_size=1 << 30, snapshot_retention=timedelta(days=1))
    with open_log(tmp_path, config) as log:
        stop = threading.Event()

        def maintain() -> None:
            while not stop.wait(0.001):
                log.seal_due()

        draining = threading.Thread(target=maintain)
        draining.start()
        try:
            for i in range(8):
                log.extend(rows(20, start=i * 20))
                log.seal()
        finally:
            stop.set()
            draining.join(30)

        assert log.await_seal(timeout=30), "a queued cut was never written"
        assert log.table_files() == 8, "eight appends and eight seals, eight files"
        assert log.table_rows() == 160


def test_a_seal_that_died_after_its_commit_does_not_wedge_the_queue(
    tmp_path: Path,
) -> None:
    """Replay has to be idempotent on the ORDINARY path, not just at open.

    A seal commits its file and then retires its group. Dying between those
    two leaves the file in the table and the group at the head of the queue.
    Replaying it blindly re-registers a file the table already holds, pyiceberg
    refuses with "already referenced by table", `finish_seal` never runs — and
    every later seal fails on the same group. Sealing wedges permanently and
    the buffer grows without bound.

    `recover()` handles this at open, but only if it can take the lease; a
    holder that died inside its TTL means it cannot, and nothing retries.
    """
    # A target nothing crosses, so the only cut is the explicit one below and
    # the group covers every row.
    config = LogConfig(target_size=1 << 30, snapshot_retention=timedelta(days=1))
    with open_log(tmp_path, config) as log:
        log.extend(rows(100))

        # Everything a seal does, stopping short of retiring the group.
        log._buffer.close_open_group()
        group = log._buffer.pending_group()

        assert group is not None
        start, end = group
        path = log._layout.seal_path(start, end, "tok")
        log._buffer.claim_seal(start, end, path)
        log._write_and_commit(end, path)

        assert log.table_rows() == 100, "the commit did not land"
        assert log._buffer.pending_group() == group, "the group was retired early"

        # The next sealer must finish it rather than redo it.
        assert log.seal_due() == end
        assert log._buffer.pending_group() is None, "the queue never drained"
        assert log.table_files() == 1, "the file was written twice"
        assert log.table_rows() == 100


def test_an_empty_group_is_never_closed(tmp_path: Path) -> None:
    """`seal()` on an untouched log has nothing to write, not an empty file."""
    with open_log(tmp_path, quiet()) as log:
        assert log.seal() is None
        assert log._table.data_files() == []


def test_a_quiet_stream_keeps_its_rows_in_the_buffer(tmp_path: Path) -> None:
    """There is no timer, and that is the point (§3a).

    A `max_age` seal emitted a small file every interval for ever on a quiet
    stream — the layout §6 exists to repair — and made one knob serve as both a
    file-size and an RPO policy, so shrinking it to lose less on a crash
    produced worse files. Freshness in the cloud belongs to WAL replication.

    So a stream that never fills a group never writes a file, and its rows stay
    where they are: durable at commit, readable through the union, and waiting.
    """
    with open_log(tmp_path, quiet()) as log:
        log.extend(rows(3))

        assert log.seal_due() is None, "sealed without reaching target_size"
        assert log._table.data_files() == [], "wrote an undersized file"
        assert log.buffered_rows() == 3, "the rows went somewhere else"
        assert len(log.scan().read_all()) == 3, "buffered rows must still read"


def test_only_an_explicit_seal_cuts_short(tmp_path: Path) -> None:
    """The one way this library writes an undersized file, and it takes a call."""
    with open_log(tmp_path, quiet()) as log:
        log.extend(rows(3))

        assert log.seal_due() is None, "something cut without being asked"
        assert log.seal() is not None, "an explicit seal must still cut"
        assert log.table_files() == 1


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


def test_the_read_cache_is_bounded_by_the_unsealed_tail(tmp_path: Path) -> None:
    """It mirrors what is unsealed, and lets go of what is not.

    Reads convert the buffer's tail to Arrow incrementally and keep it, so the
    thing to prove is that it shrinks: an Arrow slice is zero-copy, and a cache
    that only ever grew would hold every row the log had ever seen in memory.
    Measured over 24 append/seal/read cycles, the tail returns to zero rows and
    `pa.total_allocated_bytes()` to zero after each seal.
    """
    with open_log(tmp_path, quiet()) as log:
        for _ in range(4):
            log.extend(rows(200))
            log.scan().read_all()

            assert log._buffer._tail is not None
            assert log._buffer._tail.num_rows > 0, "nothing was cached to release"

            log.seal()
            log.scan().read_all()

            assert log._buffer._tail is not None
            assert log._buffer._tail.num_rows == 0, (
                f"{log._buffer._tail.num_rows} sealed rows still cached"
            )

        log.extend(rows(200))
        log.scan().read_all()

    assert log._buffer._tail is None, "close left the cache holding rows"


def test_the_read_cache_never_hides_rows_a_seal_raced_past(tmp_path: Path) -> None:
    """The boundary and the first buffered row are not the same number.

    A reader resolves the tier boundary, then reads the buffer above it. A seal
    landing between those two steps deletes the rows in between, so the cache
    gets built from a boundary lower than its own first row. Recording the
    boundary as though it were the row before the first made the slice
    arithmetic count from a row that no longer existed — and an over-long Arrow
    slice comes back EMPTY rather than raising, so the miscount was returned as
    "nothing buffered" and every row above the boundary vanished from queries.
    """
    with open_log(tmp_path, quiet()) as log:
        buffer = log._buffer
        buffer.append(rows(300))
        # A seal committed and dropped offsets 1..200. Claimed first, because
        # `finish_seal` only clears the claim it is given.
        buffer.claim_seal(1, 201, "sealed")
        buffer.finish_seal(201, "sealed")

        # A reader whose boundary was still 100 when it looked.
        assert buffer.rows_above(100).num_rows == 100

        # The next query, with the boundary caught up.
        assert buffer.rows_above(200).num_rows == buffer._rows("> ?", (200,)).num_rows
        assert buffer.rows_above(200).num_rows == 100, "the cache hid buffered rows"


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


def test_a_retried_seal_takes_a_new_name_and_queues_the_old(tmp_path: Path) -> None:
    """Two owners must never write one path, and neither may go untracked.

    A seal's name was once derived from its range alone, so a writer stalled
    past its lease and the owner that took over both wrote the same file —
    `pq.write_table` truncates on open, so it became a blend of two writers
    with one of them committing it. Recovery reads the name back from
    `sealing` rather than recomputing it, so determinism bought nothing.

    Unique names alone would trade a torn file for one this database cannot
    name. The abandoned attempt is queued for deletion BEFORE the claim is
    replaced, which is what keeps every file on disk reachable from SQLite.
    """
    config = LogConfig(target_size=1 << 30, snapshot_retention=timedelta(days=1))
    with open_log(tmp_path, config) as log:
        log.extend(rows(50))
        log._buffer.close_open_group()
        group = log._buffer.pending_group()

        assert group is not None
        start, end = group

        # An attempt that claimed a name and never committed.
        abandoned = log._layout.seal_path(start, end, "stalled")
        log._buffer.claim_seal(start, end, abandoned)
        log._layout.absolute(abandoned).parent.mkdir(parents=True, exist_ok=True)
        log._layout.absolute(abandoned).write_bytes(b"half a parquet file")

        assert log.seal_due() == end

        claimed = [f.path for f in log._table.data_files()]

        assert len(claimed) == 1
        assert not claimed[0].endswith("stalled.parquet"), (
            "the retry reused the stalled attempt's name"
        )
        assert abandoned in log._buffer.queued_deletions(), (
            "the abandoned file is on disk and reachable from nothing"
        )


def test_a_second_commit_for_a_sealed_range_is_declined(tmp_path: Path) -> None:
    """The window between the lease check and the commit, closed.

    A fence cannot be atomic with an Iceberg commit — the compare-and-swap
    knows nothing of our lease — so a writer that loses the lease in those
    milliseconds could still register. With per-attempt names its commit no
    longer collides, so it succeeded, and the range landed in the table twice.

    Iceberg was already serialising the two: one CAS moves the pointer and the
    other raises. What defeated it was our own retry, which reloaded and tried
    again. The commit now declines a range the table already covers, so the
    loser's retry does nothing — and a writer arriving afterwards never
    attempts at all.
    """
    config = LogConfig(target_size=1 << 30, snapshot_retention=timedelta(days=1))
    with open_log(tmp_path, config) as log:
        log.extend(rows(60))
        end = log.seal()

        assert end is not None
        assert log.table_files() == 1
        assert log.table_rows() == 60

        # A lapsed writer, waking with its own file already written, commits.
        second = log._layout.seal_path(1, end, "lapsed")
        dest = log._layout.absolute(second)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(log._table.data_files()[0].path, dest)

        log._table.register(str(dest), sealed_through=end)

        assert log.table_files() == 1, "a second file for the same range landed"
        assert log.table_rows() == 60, "rows were duplicated"
