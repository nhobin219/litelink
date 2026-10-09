"""Bulk ingest: an Arrow entry point that writes past the SQLite buffer (§13.4)."""

from __future__ import annotations

import re
from contextlib import contextmanager
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import litelink
import litelink._handle
import litelink._table
from litelink import OFFSET, LogConfig, WriteHandle
from litelink._claim import EVERYTHING, new_owner
from litelink._layout import Layout
from litelink._s3 import S3Options

if TYPE_CHECKING:
    from collections.abc import Iterator
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

        assert log._buffer.reserve(1000) == (4, 1004)
        assert log.end_offset() == 1004
        assert log.append(rows(1)[0]) == 1004


def test_a_reserve_on_an_empty_buffer_starts_at_one(tmp_path: Path) -> None:
    """The sequence row does not exist until the first insert, so a log whose
    whole load arrives through ingest never sees one."""
    with open_log(tmp_path) as log:
        assert log._buffer.reserve(500) == (1, 501)
        assert log.end_offset() == 501


def test_sequential_reserves_are_adjacent(tmp_path: Path) -> None:
    """What makes ingest's per-file ranges contiguous without checking."""
    with open_log(tmp_path) as log:
        first = log._buffer.reserve(10)
        second = log._buffer.reserve(10)
        third = log._buffer.reserve(1)

    assert first == (1, 11)
    assert second == (11, 21), "adjacent: each starts where the last ended"
    assert third == (21, 22)


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

        start, end = log._buffer.reserve(5)

        assert (start, end) == (51, 56)
        assert log.append(rows(1)[0]) == 56


# -- ingest: the shape it produces (§13.4, stages 2b and 2c) --------------------


def buffered(log: WriteHandle) -> int:
    """Rows the buffer actually holds — not `buffered_rows`, which reports the
    unsealed tail and so reads 0 for rows a seal deliberately retained."""
    return int(log._buffer._con.execute("SELECT count(*) FROM buffer").fetchone()[0])


def table(n: int, *, start: int = 0) -> pa.Table:
    return pa.Table.from_pylist(rows(n, start=start), schema=SCHEMA)


def test_ingest_writes_rows_that_read_back_once(tmp_path: Path) -> None:
    with open_log(tmp_path) as log:
        assert log.ingest(table(2000)) == (1, 2001)

        assert log._table.span() == (1, 2001)
        assert log.scan().read_all().num_rows == 2000
        # Nothing went through the buffer, and the sequence still moved.
        assert log._buffer.span() is None
        assert log.append(rows(1)[0]) == 2001
        # No claim outstanding, or recovery would queue a live file.
        assert log._buffer.pending_outputs() == []
        assert log._buffer.due_deletions(2**62) == []


def test_ingest_returns_none_for_a_source_with_no_rows(tmp_path: Path) -> None:
    with open_log(tmp_path) as log:
        assert log.ingest(table(0)) is None
        assert log.end_offset() == 1
        assert log._table.span() is None


def test_ingest_accepts_a_record_batch_reader(tmp_path: Path) -> None:
    """A Table is a reader that ends after one pull, so there is no branch."""
    with open_log(tmp_path) as log:
        assert log.ingest(table(500).to_reader(max_chunksize=64)) == (1, 501)
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
    config = LogConfig(
        target_seal_size=4096,
        target_compact_size=8192,
        target_row_group_size=4096,
    )
    with open_log(tmp_path, config) as log:
        log.ingest(table(4000))
        files = sorted(log._table.data_files(), key=lambda f: f.start)
        held = log._buffer.file_bytes()

    assert len(files) > 3
    # Contiguous and non-overlapping, with nothing computing adjacency.
    assert files[0].start == 1
    assert (files[-1].end - 1) == 4000
    for earlier, later in zip(files, files[1:], strict=False):
        assert later.start == earlier.end

    # Every file but the remainder is at the budget, so `runs()` gives each a
    # run of its own and compaction never selects it.
    at_target = [size for path, size in held.items() if "ingested" in path]
    assert sum(1 for size in at_target if size >= 8192) >= len(files) - 1


def test_ingested_files_are_born_past_compaction(tmp_path: Path) -> None:
    config = LogConfig(
        target_seal_size=4096,
        target_compact_size=8192,
        target_row_group_size=4096,
    )
    with open_log(tmp_path, config) as log:
        log.ingest(table(4000))
        before = {f.path for f in log._table.data_files()}

        log.advance()

        assert {f.path for f in log._table.data_files()} == before


def test_ingest_lands_above_a_frontier_a_seal_left(tmp_path: Path) -> None:
    with open_log(tmp_path) as log:
        log.extend(rows(5))
        log.seal(flush=True)
        log.await_seal()

        assert log.ingest(table(100, start=5)) == (6, 106)
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
        assert log._table.span() is None


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
        log._buffer.claim_seal(1, 41, "s/data/sealed/1-41-abcdef01.parquet")

        with pytest.raises(RuntimeError, match="in flight"):
            log.ingest(table(100))

        assert log.end_offset() == 1


def test_ingest_runs_under_wal_replication_and_says_what_it_does_not_cover(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """It used to refuse this, and refusing was strictly worse.

    WAL shipping genuinely cannot carry a bulk range — those rows never enter
    the buffer. But `evict("buffer")` reads the same flag, so the prescribed
    workaround of turning replication off to load would drop the buffer's copy
    of everything already captured: to protect rows that cannot be replicated,
    it stripped the off-box copy from rows that were.

    The load now pushes its own output, so the scope statement is narrower than
    it was: WAL still cannot carry a bulk range, but the published table has it before
    `ingest` returns. Note the knock-on asserted below — the push is a PREFIX,
    so it takes the captured rows as well, and `evict("buffer")` then drops
    what the published table holds. The rows move from buffer to bucket; they are never
    in neither.
    """
    config = LogConfig(
        target_seal_size=4096,
        target_compact_size=8192,
        target_row_group_size=4096,
        staging_snapshot_retention=timedelta(seconds=0),
        published_snapshot_retention=timedelta(seconds=0),
        wal_replication=True,
    )
    with litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        published=f"s3://{bucket}/prefix",
        s3_options=s3,
    ) as log:
        log.extend(rows(300))
        log.seal(flush=True)
        log.await_seal()
        # A seal RETAINS its rows here: with replication on the buffer is the
        # off-box copy until the published table has the range (§3a).
        retained = buffered(log)
        assert retained == 300

        lo, hi = log.ingest(table(500, start=300)) or (0, 0)

        assert (lo, hi) == (301, 801), "[start, end)"
        assert log.scan().read_all().num_rows == 800
        # The captured rows still have an off-box copy — the load did not strip
        # it, which is the whole of what refusing got wrong. They have MOVED,
        # though: the load's own publish pushed them, and `evict("buffer")` then
        # dropped what the published table had taken. Buffer or bucket, never neither.
        assert log.published_through() >= retained
        # And the loaded range is second-copied too, which is the point of
        # publishing from inside `ingest`: nothing WAL ships holds these rows, so
        # without this they would be on local disk alone.
        assert log.published_through() >= hi - 1
        # Still not in the buffer, which is the honest scope claim about WAL.
        assert buffered(log) < hi - lo

        log.publish()

        assert log.published_through() >= 300


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
        assert log._table.span() is None


def test_merging_a_loads_tail_reads_it_a_row_group_at_a_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A load's tail is an input like a seal, and up to `target_compact_size`
    on disk. Read whole it is that times the compression ratio in Arrow, in one
    allocation; read a row group at a time, it is bounded like every other
    input."""
    config = LogConfig(
        target_seal_size=4096,
        target_compact_size=64 * 1024,
        target_row_group_size=8192,
        compact_min_files=2,
    )
    with open_log(tmp_path, config) as log:
        log.ingest(table(6000), publish=False)
        tail = log._table.data_files()[-1]
        assert tail.size < config.compact_size, "the load must leave a tail"
        assert pq.ParquetFile(tail.path).metadata.num_row_groups > 2

        log.extend(rows(200, start=6000))
        log.seal(flush=True)

        read: list[int] = []
        scan_range = litelink._table.LogTable.scan_range

        def spy(self: Any, start: int, end: int) -> pa.Table:
            got = scan_range(self, start, end)
            read.append(got.nbytes)

            return got

        monkeypatch.setattr(litelink._table.LogTable, "scan_range", spy)
        log.compact(flush=True)
        monkeypatch.undo()

        assert read, "the tail must have been merged"
        assert max(read) <= 2 * config.target_row_group_size, (
            f"an input was read whole: {max(read)} bytes"
        )
        assert log.scan().read_all().num_rows == 6200


# -- ingest: what a failure leaves behind (I2) ---------------------------------


def test_a_load_that_dies_mid_write_leaves_nothing_unnameable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every path is in SQLite before its bytes are, so a crash leaves files
    this database can still name — the one category §12 refuses to have."""
    config = LogConfig(
        target_seal_size=4096,
        target_compact_size=8192,
        target_row_group_size=4096,
    )
    written: list[object] = []
    real = litelink._handle.stream_parquet

    @contextmanager
    def failing(path: Any, schema: Any, compression: Any) -> Iterator[Any]:
        written.append(path)
        # The bytes land and the durable write then fails, so the third file is
        # on disk under a name only `compacting` holds. That is the state I2
        # exists for, and a raise BEFORE the write would not produce it.
        with real(path, schema, compression) as write:
            yield write

        if len(written) == 3:
            msg = "the disk went away"
            raise OSError(msg)

    with open_log(tmp_path, config) as log:
        monkeypatch.setattr(litelink._handle, "stream_parquet", failing)

        with pytest.raises(OSError, match="the disk went away"):
            log.ingest(table(4000))

        monkeypatch.undo()
        # Nothing landed: the batch commit had not run.
        assert log._table.span() is None
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
        assert log._table.span() is None
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
        assert log.ingest(table(50)) == (101, 151)
        log.extend(rows(3))
        log.seal(flush=True)
        log.await_seal()

        assert log.scan().read_all().num_rows == 53
        assert log._table.span() == (101, 154)
        log.advance()
        assert log.scan().read_all().num_rows == 53


def test_files_are_registered_several_per_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A commit costs far more than the write it publishes — 4.1 s against
    648 ms against S3 — so writes and commits decouple."""
    config = LogConfig(
        target_seal_size=4096,
        target_compact_size=8192,
        target_row_group_size=4096,
    )
    monkeypatch.setattr(litelink._handle, "_INGEST_BATCH", 3)
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


def test_an_ingested_range_survives_the_whole_published_table_cycle(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The reservation is upward, so every tier boundary keeps holding: `publish`
    pushes it, `evict` clamps to it, and a merged read serves it once."""
    with litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts", "key"),
        config=LogConfig(
            target_seal_size=4096,
            target_compact_size=8192,
            target_row_group_size=4096,
            compact_min_files=2,
            staging_retention=timedelta(seconds=0),
            staging_snapshot_retention=timedelta(seconds=0),
            published_snapshot_retention=timedelta(seconds=0),
        ),
        published=f"s3://{bucket}/prefix",
        s3_options=s3,
    ) as log:
        assert log.ingest(table(3000)) == (1, 3001)
        log.extend(rows(400, start=3000))
        log.seal(flush=True)
        log.await_seal()

        log.publish()
        assert log._published.require().span() is not None
        log.advance()

        assert log.scan().read_all().num_rows == 3400
        assert log.append(rows(1)[0]) == 3401


@pytest.mark.parametrize("replicated", [True, False])
def test_a_load_flushes_its_tail_by_default_only_when_the_wal_is_replicated(
    tmp_path: Path, bucket: str, s3: S3Options, replicated: bool
) -> None:
    """A loaded range gets the same durability as an appended one.

    With `wal_replication`, an appended row is off-box from its commit, and a
    load's rows never enter the buffer the replica ships — so the published
    table is their only off-box copy, and the default flushes the short last
    file that `stable_prefix` would hold back. On a quiet stream that tail
    otherwise stays on one disk indefinitely: 113,399 rows on the deployment
    that found it.

    Without it, an appended trailing run stays local until it fills, and so
    does a load's: the default does not flush, and the tail merges with what
    is sealed after it instead of becoming an undersized published file.

    Falsify by always flushing (the unreplicated case pushes the tail), or by
    never flushing (the replicated case leaves it behind).
    """
    with litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=LogConfig(
            target_seal_size=4096,
            target_compact_size=8192,
            target_row_group_size=4096,
            compact_min_files=2,
            staging_snapshot_retention=timedelta(seconds=0),
            published_snapshot_retention=timedelta(seconds=0),
            wal_replication=replicated,
        ),
        published=f"s3://{bucket}/prefix",
        s3_options=s3,
    ) as log:
        _, hi = log.ingest(table(3000)) or (0, 0)

        if replicated:
            assert log.published_through() >= hi - 1, (
                "a replicated log's loaded range is not second-copied"
            )
        else:
            assert 0 < log.published_through() < hi - 1, (
                "an unreplicated log flushed its load's short tail"
            )

        # Either default can be overridden, and `publish=False` skips the push.
        _, hi2 = log.ingest(table(500, start=hi), flush=True) or (0, 0)
        assert log.published_through() >= hi2 - 1

        _, hi3 = log.ingest(table(500, start=hi2), publish=False) or (0, 0)
        assert log.published_through() < hi3 - 1


# -- the codec (§12) -----------------------------------------------------------


def codecs_of(log: WriteHandle) -> set[str]:
    """The compression every data file the log holds was written with."""
    return {
        pq.ParquetFile(f.path).metadata.row_group(0).column(0).compression
        for f in log._table.data_files()
    }


@pytest.mark.parametrize(
    ("setting", "written"),
    [("zstd", "ZSTD"), ("snappy", "SNAPPY"), ("none", "UNCOMPRESSED")],
)
def test_every_write_path_uses_the_configured_codec(
    tmp_path: Path, setting: str, written: str
) -> None:
    """A seal, a compaction and a bulk ingest, in one log. The codec had no
    home at all before this — every site took pyarrow's Snappy default — so
    what this pins is that a write path cannot quietly have its own answer."""
    config = LogConfig(
        target_seal_size=4096,
        target_compact_size=8192,
        target_row_group_size=4096,
        compact_min_files=2,
        compression=setting,
    )
    with open_log(tmp_path, config) as log:
        log.extend(rows(400))
        log.seal(flush=True)
        log.await_seal()
        log.ingest(table(2000, start=400))
        log.advance()

        assert codecs_of(log) == {written}
        assert log.scan().read_all().num_rows == 2400


def test_zstd_is_the_default(tmp_path: Path) -> None:
    with open_log(tmp_path) as log:
        assert log.config.compression == "zstd"
        log.ingest(table(100))

        assert codecs_of(log) == {"ZSTD"}


def test_a_codec_this_build_cannot_write_is_refused_at_config_time(
    tmp_path: Path,
) -> None:
    """Not at the first write, which is a seal in a maintainer minutes later,
    with the rows already acknowledged and every retry failing the same way."""
    with pytest.raises(ValueError, match="compression must be one of"):
        litelink.new(
            tmp_path, "s", schema=SCHEMA, config=LogConfig(compression="brotli")
        )


def test_a_log_reads_across_files_written_with_different_codecs(
    tmp_path: Path,
) -> None:
    """Which is what makes changing the setting safe on a live log: Parquet
    records the codec per column chunk, so nothing has to be rewritten."""
    config = LogConfig(target_seal_size=4096, compression="snappy")
    with open_log(tmp_path, config) as log:
        log.ingest(table(500))
        log.set_config(
            LogConfig(target_seal_size=4096, compression="zstd"),
        )
        log.ingest(table(500, start=500))

        assert codecs_of(log) == {"SNAPPY", "ZSTD"}
        assert log.scan().read_all().num_rows == 1000
        assert (
            log.sql("SELECT count(*) c FROM log").read_all().column("c")[0].as_py()
            == 1000
        )


def test_a_load_pushes_the_undersized_seals_beneath_it_too(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The cost of publishing from inside `ingest`, pinned so it is not a surprise.

    `stable_prefix` holds back a trailing run, and a deployment may rely on that
    to keep undersized files out of its published table. A load cannot honour that rule
    and still second-copy itself: `_push` takes a PREFIX, because the watermark
    it records has to stay contiguous for eviction to trust it (I4). So an
    unsettled SEAL below the load is pushed as well.

    Measured while trying the narrow version — extending the push only through
    bulk-loaded files never advances past a seal sitting at index 0, and
    `published_through()` stays 0 with the load unpublished.

    The trade is deliberate, and cheap because of WHEN a load happens: `ingest`
    claims the whole log, so it is a backfill-time operation with live capture
    typically stopped, and the trailing run is the load's own. Against that, not
    pushing leaves rows that never entered the buffer on a single disk.
    `publish=False` opts out for a caller who would rather sequence it themselves.

    Falsify by reverting `flush` to the ordinary `stable_prefix` gate:
    `published_through()` drops below the load's `hi`.
    """
    config = LogConfig(
        target_seal_size=4096,
        target_compact_size=1024 * 1024,
        compact_min_files=2,
        staging_snapshot_retention=timedelta(seconds=0),
        published_snapshot_retention=timedelta(seconds=0),
        wal_replication=True,
    )
    with litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        published=f"s3://{bucket}/prefix",
        s3_options=s3,
    ) as log:
        # Far below the compact target, so it lands in the trailing run.
        log.extend(rows(200))
        log.seal(flush=True)
        log.await_seal()
        log.publish()

        assert log.published_through() == 0, (
            "an ordinary publish pushed an undersized seal"
        )

        _, hi = log.ingest(table(300, start=200)) or (0, 0)

        # The load is second-copied, and the seal beneath it went with it.
        assert log.published_through() >= hi - 1

        # And opting out leaves the next load where it was.
        _, hi2 = log.ingest(table(50, start=hi), publish=False) or (0, 0)

        assert log.published_through() < hi2 - 1


NON_FINITE_LOADS = [
    ("float32 NaN", pa.float32(), float("nan"), "x", None),
    ("float64 inf", pa.float64(), float("inf"), "x", None),
    ("float64 -inf", pa.float64(), float("-inf"), "x", None),
    (
        "struct field",
        pa.struct([pa.field("d", pa.float64())]),
        {"d": float("nan")},
        "x",
        "x.d",
    ),
    ("list item", pa.list_(pa.float32()), [1.0, float("inf")], "x", "x[]"),
    (
        "map value in a struct",
        pa.struct([pa.field("m", pa.map_(pa.string(), pa.float64()))]),
        {"m": [("k", float("-inf"))]},
        "x",
        "x.m[]",
    ),
]


@pytest.mark.parametrize("as_reader", [False, True], ids=["table", "reader"])
@pytest.mark.parametrize(
    ("label", "type_", "value", "column", "path"),
    NON_FINITE_LOADS,
    ids=[case[0] for case in NON_FINITE_LOADS],
)
def test_a_bulk_load_holding_a_non_finite_float_costs_no_offsets(
    tmp_path: Path,
    label: str,
    type_: pa.DataType,
    value: object,
    column: str,
    path: str | None,
    as_reader: bool,
) -> None:
    """`ingest` meets neither the CHECK nor the encoder, so it asks (#87).

    Per chunk and before the reservation, so a refused load leaves no hole,
    naming the column and — for a nested one — where inside it.

    Falsify by removing the `_refuse_non_finite` call from `_ingest_chunks`:
    each load is accepted.
    """
    schema = pa.schema([pa.field("k", pa.int64()), pa.field("x", type_)])
    source = pa.Table.from_pylist(
        [{"k": 1, "x": None}, {"k": 2, "x": value}], schema=schema
    )
    load = source.to_reader() if as_reader else source
    where = "" if path is None else f" at {re.escape(path)}"

    with litelink.new(tmp_path, "s", schema=schema, sort_by=("k",)) as log:
        with pytest.raises(
            ValueError,
            match=rf"column '{column}' cannot hold .*{where}: a log holds only finite floats",
        ):
            log.ingest(load)

        assert log.end_offset() == 1
        assert log._table.span() is None


def test_a_nan_under_a_null_struct_is_not_a_stored_value(tmp_path: Path) -> None:
    """A struct child's slot under a NULL parent is not part of the row.

    Checked through `StructArray.flatten()`, which folds the parent's validity
    in; reading the raw child would refuse a legitimate load.
    """
    type_ = pa.struct([pa.field("d", pa.float64())])
    schema = pa.schema([pa.field("k", pa.int64()), pa.field("x", type_)])
    hidden = pa.StructArray.from_arrays(
        [pa.array([float("nan")])], names=["d"], mask=pa.array([True])
    )
    source = pa.Table.from_arrays([pa.array([1], pa.int64()), hidden], schema=schema)

    with litelink.new(tmp_path, "s", schema=schema, sort_by=("k",)) as log:
        assert log.ingest(source) == (1, 2)
        assert log.scan().read_all()["x"].to_pylist() == [None]
