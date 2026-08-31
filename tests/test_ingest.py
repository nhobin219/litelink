"""Bulk ingest: an Arrow entry point that writes past the SQLite buffer (§13.4)."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import litelink
import litelink.log
from litelink._claim import EVERYTHING, new_owner
from litelink._layout import Layout
from litelink._s3 import S3Options
from litelink.log import OFFSET, LogConfig, WriteHandle

if TYPE_CHECKING:
    from pathlib import Path

SCHEMA = pa.schema(
    [
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("key", pa.string()),
        pa.field("payload", pa.string()),
    ]
)


def open_log(root: Path, config: LogConfig | None = None) -> WriteHandle:
    if Layout(root, "s").buffer_db.exists():
        log = litelink.open(root, "s")
        if config is not None:
            log.set_config(config)

        return log

    return litelink.new(
        root, "s", schema=SCHEMA, sort_by=("event_ts", "key"), config=config
    )


def rows(n: int, *, start: int = 0) -> list[dict[str, object]]:
    return [
        {"event_ts": 1000 + i, "key": f"k{i % 3}", "payload": f'{{"seq":{i}}}'}
        for i in range(start, start + n)
    ]


# -- the reserve (§13.4, stage 2a) ---------------------------------------------


def test_a_reserve_makes_the_next_append_skip_the_range(tmp_path: Path) -> None:
    """I9 asked of the one path that issues offsets without writing rows."""
    with open_log(tmp_path) as log:
        log.extend(rows(3))

        assert log._buffer.reserve(1000) == (4, 1003)
        assert log.end_offset() == 1004
        assert log.append(rows(1)[0]) == 1004


def test_a_reserve_on_an_empty_buffer_starts_at_one(tmp_path: Path) -> None:
    """The sequence row does not exist until the first insert, so a log whose
    whole load arrives through ingest never sees one."""
    with open_log(tmp_path) as log:
        assert log._buffer.reserve(500) == (1, 500)
        assert log.end_offset() == 501


def test_sequential_reserves_are_adjacent(tmp_path: Path) -> None:
    """What makes ingest's per-file ranges contiguous without checking."""
    with open_log(tmp_path) as log:
        first = log._buffer.reserve(10)
        second = log._buffer.reserve(10)
        third = log._buffer.reserve(1)

    assert first == (1, 10)
    assert second == (11, 20)
    assert third == (21, 21)


@pytest.mark.parametrize("count", [0, -1])
def test_reserving_nothing_is_refused(tmp_path: Path, count: int) -> None:
    with open_log(tmp_path) as log, pytest.raises(ValueError, match="at least one"):
        log._buffer.reserve(count)


def test_a_reserve_leaves_the_other_sequences_alone(tmp_path: Path) -> None:
    """`extent.group_id` and `claim.id` are AUTOINCREMENT too, so the UPDATE
    has to be keyed on the buffer's row."""
    with open_log(tmp_path) as log:
        log.extend(rows(3))
        before = dict(
            log._buffer._con.execute("SELECT name, seq FROM sqlite_sequence").fetchall()
        )

        log._buffer.reserve(1000)

        after = dict(
            log._buffer._con.execute("SELECT name, seq FROM sqlite_sequence").fetchall()
        )

    assert before["buffer"] == 3
    assert after["buffer"] == 1003
    assert {k: v for k, v in after.items() if k != "buffer"} == {
        k: v for k, v in before.items() if k != "buffer"
    }


def test_a_reserve_never_lands_on_a_buffered_row(tmp_path: Path) -> None:
    """The floor is `max(seq, max(offset))`. A sequence somehow left below the
    rows present must not hand back offsets those rows already hold."""
    with open_log(tmp_path) as log:
        log.extend(rows(50))
        log._buffer._con.execute("UPDATE sqlite_sequence SET seq = 10")

        lo, hi = log._buffer.reserve(5)

        assert (lo, hi) == (51, 55)
        assert log.append(rows(1)[0]) == 56


# -- ingest: the shape it produces (§13.4, stages 2b and 2c) --------------------


def table(n: int, *, start: int = 0) -> pa.Table:
    return pa.Table.from_pylist(rows(n, start=start), schema=SCHEMA)


def test_ingest_writes_rows_that_read_back_once(tmp_path: Path) -> None:
    with open_log(tmp_path) as log:
        assert log.ingest(table(2000)) == (1, 2000)

        assert log._table.extent() == (1, 2000)
        assert log.scan().read_all().num_rows == 2000
        # Nothing went through the buffer, and the sequence still moved.
        assert log._buffer.extent() is None
        assert log.append(rows(1)[0]) == 2001
        # No claim outstanding, or recovery would queue a live file.
        assert log._buffer.pending_outputs() == []
        assert log._buffer.due_deletions(2**62) == []


def test_ingest_returns_none_for_a_source_with_no_rows(tmp_path: Path) -> None:
    with open_log(tmp_path) as log:
        assert log.ingest(table(0)) is None
        assert log.end_offset() == 1
        assert log._table.extent() is None


def test_ingest_accepts_a_record_batch_reader(tmp_path: Path) -> None:
    """A Table is a reader that ends after one pull, so there is no branch."""
    with open_log(tmp_path) as log:
        assert log.ingest(table(500).to_reader(max_chunksize=64)) == (1, 500)
        assert log.scan().read_all().num_rows == 500


def test_an_ingested_file_is_sorted_within_itself(tmp_path: Path) -> None:
    """§4's sort, applied per file. Offsets are materialised in input order and
    then permuted by the sort, so the range stays dense while the rows move."""
    descending = pa.Table.from_pylist(rows(300)[::-1], schema=SCHEMA)
    with open_log(tmp_path) as log:
        log.ingest(descending)
        (data_file,) = log._table.data_files()

    written = pq.read_table(data_file.path)
    keys = written.column("event_ts").to_pylist()
    assert keys == sorted(keys)
    offsets = written.column(OFFSET).to_pylist()
    assert offsets != sorted(offsets)
    assert sorted(offsets) == list(range(1, 301))


def test_a_large_source_is_split_at_the_compaction_target(tmp_path: Path) -> None:
    config = LogConfig(target_seal_size=4096, target_compact_size=8192)
    with open_log(tmp_path, config) as log:
        log.ingest(table(4000))
        files = sorted(log._table.data_files(), key=lambda f: f.lo)
        held = log._buffer.file_bytes()

    assert len(files) > 3
    # Contiguous and non-overlapping, with nothing computing adjacency.
    assert files[0].lo == 1
    assert files[-1].hi == 4000
    for earlier, later in zip(files, files[1:], strict=False):
        assert later.lo == earlier.hi + 1

    # Every file but the remainder is at the budget, so `runs()` gives each a
    # run of its own and compaction never selects it.
    at_target = [size for path, size in held.items() if "ingested" in path]
    assert sum(1 for size in at_target if size >= 8192) >= len(files) - 1


def test_ingested_files_are_born_past_compaction(tmp_path: Path) -> None:
    config = LogConfig(target_seal_size=4096, target_compact_size=8192)
    with open_log(tmp_path, config) as log:
        log.ingest(table(4000))
        before = {f.path for f in log._table.data_files()}

        log.maintain()

        assert {f.path for f in log._table.data_files()} == before


def test_ingest_lands_above_a_frontier_a_seal_left(tmp_path: Path) -> None:
    with open_log(tmp_path) as log:
        log.extend(rows(5))
        log.seal()
        log.await_seal()

        assert log.ingest(table(100, start=5)) == (6, 105)
        assert log.scan().read_all().num_rows == 105
        assert log.append(rows(1)[0]) == 106


# -- ingest: what it refuses ---------------------------------------------------


def test_ingest_is_refused_while_rows_await_a_seal(tmp_path: Path) -> None:
    """A row left below the reservation lands in a file spanning it, and
    transiently sits in no leg of the read at all."""
    with open_log(tmp_path) as log:
        log.extend(rows(40))

        with pytest.raises(RuntimeError, match="no seal has been asked to cut"):
            log.ingest(table(100))

        assert log.end_offset() == 41
        assert log._table.extent() is None


def test_ingest_is_refused_while_the_seal_queue_holds_a_group(tmp_path: Path) -> None:
    """The one-read version passes here and loses the rows. `seal()` cuts and
    returns even when it sealed nothing, so a writer whose drain was blocked is
    left with a fresh EMPTY open group over a queued one."""
    with open_log(tmp_path) as log:
        log.extend(rows(40))
        log._buffer.close_open_group()

        assert log._buffer.open_group_started() is False
        with pytest.raises(RuntimeError, match="seal queue still holds 1-41"):
            log.ingest(table(100))

        assert log.end_offset() == 41


def test_ingest_is_refused_while_a_seal_is_in_flight(tmp_path: Path) -> None:
    with open_log(tmp_path) as log:
        log._buffer.claim_seal(1, 41, "s/data/1-41-abcdef01.parquet")

        with pytest.raises(RuntimeError, match="in flight"):
            log.ingest(table(100))

        assert log.end_offset() == 1


def test_ingest_is_refused_under_wal_replication(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A bulk range never enters the buffer, so with replication on nothing
    off-box holds it between the load and the first sync (§3a)."""
    with litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        archive=f"s3://{bucket}/prefix",
        s3=s3,
        config=LogConfig(wal_replication=True),
    ) as log:
        with pytest.raises(RuntimeError, match="wal_replication"):
            log.ingest(table(100))

        assert log.end_offset() == 1


def test_ingest_is_refused_while_another_owner_holds_the_log(tmp_path: Path) -> None:
    with open_log(tmp_path) as log:
        held = log._buffer.claim("maintain", 0, EVERYTHING, new_owner())
        assert held.acquire()

        with pytest.raises(RuntimeError, match="another owner"):
            log.ingest(table(100))

        assert log.end_offset() == 1


def test_a_source_carrying_the_offset_column_is_refused(tmp_path: Path) -> None:
    with open_log(tmp_path) as log:
        carrying = table(10).add_column(
            0,
            pa.field(OFFSET, pa.int64(), nullable=False),
            pa.array(range(1, 11), pa.int64()),
        )

        with pytest.raises(ValueError, match="I11"):
            log.ingest(carrying)

        assert log.end_offset() == 1


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (pa.table({"event_ts": pa.array([1], pa.int64())}), "missing"),
        (
            pa.table(
                {
                    "event_ts": pa.array([1], pa.int64()),
                    "key": ["k"],
                    "payload": ["p"],
                    "extra": ["x"],
                }
            ),
            "unknown",
        ),
        (
            pa.table(
                {
                    "event_ts": pa.array([1], pa.int64()),
                    "key": pa.array([[1, 2]], pa.list_(pa.int64())),
                    "payload": ["p"],
                }
            ),
            "cannot be cast",
        ),
    ],
)
def test_a_foreign_source_is_refused_before_anything_is_reserved(
    tmp_path: Path, source: pa.Table, expected: str
) -> None:
    """A rejection AFTER a reservation is a permanent hole in the offsets."""
    with open_log(tmp_path) as log:
        with pytest.raises(ValueError, match=expected):
            log.ingest(source)

        assert log.end_offset() == 1


def test_a_value_the_schema_cannot_hold_costs_no_offsets(tmp_path: Path) -> None:
    """The schema check settles types; a VALUE that will not cast is caught by
    the cast itself, which happens before the chunk's reserve. Both refuse; the
    property that matters is that neither spends offsets."""
    unparseable = pa.table(
        {"event_ts": ["not a number"], "key": ["k"], "payload": ["p"]}
    )
    with open_log(tmp_path) as log:
        with pytest.raises(ValueError, match="not a number"):
            log.ingest(unparseable)

        assert log.end_offset() == 1
        assert log._table.extent() is None


# -- ingest: what a failure leaves behind (I2) ---------------------------------


def test_a_load_that_dies_mid_write_leaves_nothing_unnameable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every path is in SQLite before its bytes are, so a crash leaves files
    this database can still name — the one category §12 refuses to have."""
    config = LogConfig(target_seal_size=4096, target_compact_size=8192)
    written: list[object] = []
    real = litelink.log.fsync

    def failing(path: Any) -> None:
        written.append(path)
        if len(written) == 3:
            msg = "the disk went away"
            raise OSError(msg)

        real(path)

    with open_log(tmp_path, config) as log:
        monkeypatch.setattr(litelink.log, "fsync", failing)

        with pytest.raises(OSError, match="the disk went away"):
            log.ingest(table(4000))

        monkeypatch.undo()
        # Nothing landed: the batch commit had not run.
        assert log._table.extent() is None
        # The two complete files were queued by the ingest itself...
        queued = set(log._buffer.due_deletions(2**62))
        assert len(queued) == 2
        # ...and the third, claimed before its bytes existed, is still named by
        # `compacting` for recovery to resolve.
        outstanding = {path for _, _, path in log._buffer.pending_outputs()}
        assert len(outstanding) == 1
        assert not outstanding & queued

    with open_log(tmp_path) as reopened:
        assert reopened._buffer.pending_outputs() == []
        assert outstanding <= set(reopened._buffer.due_deletions(2**62))


def test_a_declined_register_raises_rather_than_returning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`_write_and_commit` queues a declined seal and returns normally, which is
    right there and exactly wrong here: nothing else holds these rows. The
    silent version acknowledged 3000 rows and served 50."""
    with open_log(tmp_path) as log:
        monkeypatch.setattr(log._table, "register", lambda *a, **k: False)

        with pytest.raises(RuntimeError, match="declined a bulk range"):
            log.ingest(table(100))

        monkeypatch.undo()
        assert log._table.extent() is None
        assert len(log._buffer.due_deletions(2**62)) == 1
        assert log._buffer.pending_outputs() == []


def test_a_lost_reservation_leaves_a_gap_the_log_reads_across(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """§6 needs files non-overlapping and adjacent in offset order, not free of
    integer gaps. A failed load costs its range and nothing else."""
    with open_log(tmp_path) as log:
        monkeypatch.setattr(log._table, "register", lambda *a, **k: False)
        with pytest.raises(RuntimeError, match="declined a bulk range"):
            log.ingest(table(100))

        monkeypatch.undo()
        assert log.ingest(table(50)) == (101, 150)
        log.extend(rows(3))
        log.seal()
        log.await_seal()

        assert log.scan().read_all().num_rows == 53
        assert log._table.extent() == (101, 153)
        log.maintain()
        assert log.scan().read_all().num_rows == 53


def test_files_are_registered_several_per_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A commit costs far more than the write it publishes — 4.1 s against
    648 ms against S3 — so writes and commits decouple."""
    config = LogConfig(target_seal_size=4096, target_compact_size=8192)
    monkeypatch.setattr(litelink.log, "_INGEST_BATCH", 3)
    commits: list[int] = []

    with open_log(tmp_path, config) as log:
        register = log._table.register

        def counted(paths: list[str], *args: Any, **kwargs: Any) -> bool:
            commits.append(len(paths))

            return register(paths, *args, **kwargs)

        monkeypatch.setattr(log._table, "register", counted)
        log.ingest(table(4000))
        monkeypatch.undo()

        files = log._table.data_files()

    assert len(files) > 3
    assert max(commits) == 3, commits
    assert sum(commits) == len(files)
    assert len(commits) < len(files)


def test_an_ingested_range_survives_the_whole_archive_cycle(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The reservation is upward, so every tier boundary keeps holding: `sync`
    pushes it, `evict` clamps to it, and a merged read serves it once."""
    with litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts", "key"),
        config=LogConfig(
            target_seal_size=4096,
            target_compact_size=8192,
            compact_min_files=2,
            local_retention=timedelta(seconds=0),
            snapshot_retention=timedelta(seconds=0),
        ),
        archive=f"s3://{bucket}/prefix",
        s3=s3,
    ) as log:
        assert log.ingest(table(3000)) == (1, 3000)
        log.extend(rows(400, start=3000))
        log.seal()
        log.await_seal()

        log.sync()
        assert log._archive.require().extent() is not None
        log.maintain()

        assert log.scan(include_archive=True).read_all().num_rows == 3400
        assert log.append(rows(1)[0]) == 3401
