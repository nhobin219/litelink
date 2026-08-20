"""Local storage reclamation: compact, evict, expire (SPEC §6, §8, §12)."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from pyiceberg.catalog.sql import SqlCatalog

from litelink.log import Log, LogConfig
from tests.test_log import SCHEMA, _today, open_log, read_all, rows


def seal_files(log: Log, count: int, per_file: int = 4) -> None:
    """Produce `count` sealed files, each holding `per_file` rows."""
    for i in range(count):
        log.extend(rows(per_file, start=i * per_file))
        log.seal()


def test_compaction_merges_adjacent_small_files(tmp_path: Path) -> None:
    with open_log(
        tmp_path, LogConfig(compact_below=1 << 30, compact_min_files=2)
    ) as log:
        seal_files(log, 4)
        assert len(log._table.data_files()) == 4

        log.maintain()

        assert len(log._table.data_files()) == 1
        assert len(read_all(log)) == 16
        assert log.table_extent() == (1, 16)


def test_compaction_needs_compact_min_files(tmp_path: Path) -> None:
    """Below the threshold the pass must leave the files alone."""
    with open_log(
        tmp_path, LogConfig(compact_below=1 << 30, compact_min_files=5)
    ) as log:
        seal_files(log, 4)
        log.maintain()

        assert len(log._table.data_files()) == 4


def test_compaction_skips_files_over_compact_below(tmp_path: Path) -> None:
    with open_log(tmp_path, LogConfig(compact_below=1, compact_min_files=2)) as log:
        seal_files(log, 3)
        log.maintain()

        assert len(log._table.data_files()) == 3


def test_compaction_output_is_re_sorted(tmp_path: Path) -> None:
    """§6 step 2: re-sorted, not merely concatenated."""
    import pyarrow.parquet as pq

    config = LogConfig(compact_below=1 << 30, compact_min_files=2)
    with Log.new(
        tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",), config=config
    ) as log:
        for ts in (500, 100, 400, 200):
            log.append({"event_ts": ts, "key": "k", "payload": b""})
            log.seal()

        log.maintain()

        merged = log._table.data_files()
        assert len(merged) == 1
        written = pq.read_table(merged[0].path)["event_ts"].to_pylist()
        assert written == [100, 200, 400, 500]


def test_compaction_preserves_every_row(tmp_path: Path) -> None:
    config = LogConfig(compact_below=1 << 30, compact_min_files=2)
    with open_log(tmp_path, config) as log:
        seal_files(log, 5, per_file=3)
        before = read_all(log)

        log.maintain()

        assert read_all(log) == before


def test_eviction_drops_files_past_local_retention(tmp_path: Path) -> None:
    """§8. With no archive this is deletion of the only copy, by design."""
    config = LogConfig(
        compact_min_files=99,  # isolate eviction from compaction
        local_retention=timedelta(microseconds=1),
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        log.extend(rows(2, start=12))
        assert len(log._table.data_files()) == 3

        log.maintain()

        assert log._table.data_files() == []
        # The buffer is untouched: retention governs the table, and buffer rows
        # are removed at seal and nowhere else (§8).
        assert len(read_all(log)) == 2


def test_no_eviction_without_local_retention(tmp_path: Path) -> None:
    with open_log(tmp_path, LogConfig(compact_min_files=99)) as log:
        seal_files(log, 3)
        log.maintain()

        assert len(log._table.data_files()) == 3
        assert len(read_all(log)) == 12


def test_expiry_drops_old_snapshots(tmp_path: Path) -> None:
    config = LogConfig(
        compact_min_files=99, snapshot_retention=timedelta(microseconds=1)
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        assert log._table.snapshot_count() == 3

        log.maintain()

        # The current snapshot is never expired, whatever its age.
        assert log._table.snapshot_count() == 1
        assert len(read_all(log)) == 12


def test_maintain_refuses_when_an_archive_is_configured(tmp_path: Path) -> None:
    """I4 needs sync's registration watermark, and sync does not exist yet."""
    log = Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        archive="s3://bucket/prefix",
    )
    with pytest.raises(NotImplementedError, match="sync"):
        log.maintain()

    log.close()


def test_reads_stay_correct_across_a_compaction(tmp_path: Path) -> None:
    """I8: once readable, a row stays readable."""
    config = LogConfig(compact_below=1 << 30, compact_min_files=2)
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        log.extend(rows(4, start=12))
        before = read_all(log)

        log.maintain()

        assert read_all(log) == before
        assert [r[0] for r in read_all(log)] == list(range(1, 17))


def test_eviction_alone_does_not_free_disk(tmp_path: Path) -> None:
    """§12: local disk holds local_retention + snapshot_retention of data.

    Eviction removes a file from the current snapshot; the bytes stay on disk,
    referenced by the previous snapshot, until expiry deletes them. Conflating
    the two is how a disk-space calculation comes out a whole retention window
    short.
    """
    config = LogConfig(
        compact_min_files=99,
        local_retention=timedelta(microseconds=1),
        snapshot_retention=timedelta(days=365),  # nothing may expire
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        on_disk = {p.name for p in tmp_path.rglob("*.parquet")}
        assert len(on_disk) == 3

        log.maintain()

        assert log._table.data_files() == [], "evicted from the table"
        assert {p.name for p in tmp_path.rglob("*.parquet")} == on_disk, (
            "still on disk, held by the pre-eviction snapshot"
        )

    with open_log(
        tmp_path,
        LogConfig(compact_min_files=99, snapshot_retention=timedelta(microseconds=1)),
    ) as log:
        log.maintain()

        assert list(tmp_path.rglob("*.parquet")) == [], "expiry is what deletes bytes"


def test_sweep_spares_an_in_flight_seal(tmp_path: Path) -> None:
    """The one unreferenced file that must survive.

    A seal writes its Parquet before committing it, so between those two steps
    the file is on disk and no snapshot names it. `sealing` is what tells the
    sweep it is not an orphan — without that guard, a maintain() interleaved
    with a seal deletes the file the very next step is about to register.
    """
    config = LogConfig(
        compact_min_files=99, snapshot_retention=timedelta(microseconds=1)
    )
    with open_log(tmp_path, config) as log:
        log.extend(rows(3))
        rel_path = log._layout.seal_path(1, 4, _today())
        log._buffer.claim_seal(1, 4, rel_path)

        written = tmp_path / rel_path
        written.parent.mkdir(parents=True, exist_ok=True)
        written.write_bytes(b"not really parquet, but on disk and uncommitted")

        log.maintain()

        assert written.exists(), "sweep deleted a file `sealing` had claimed"


def test_recovery_removes_a_crashed_compaction_by_name(tmp_path: Path) -> None:
    """§11: no snapshot was committed, so the output is dead — and it is named.

    The point is that recovery unlinks one known path. Nothing scans a
    directory to discover that this file was garbage.
    """
    config = LogConfig(compact_min_files=99)
    with open_log(tmp_path, config) as log:
        seal_files(log, 1)
        rel_path = log._layout.compaction_path(1, 4, "deadbeef")
        log._buffer.claim_compaction(1, 4, rel_path)
        half_written = tmp_path / rel_path
        half_written.parent.mkdir(parents=True, exist_ok=True)
        half_written.write_bytes(b"a compaction that never committed")

    with open_log(tmp_path, config) as recovered:
        assert not half_written.exists()
        assert recovered._buffer.pending_compaction() is None
        # The inputs were never superseded, so the table is already correct.
        assert len(read_all(recovered)) == 4


def tracked_paths(log: Log) -> set[Path]:
    """Every file the log can account for without listing a directory.

    Referenced by a live snapshot, claimed by an in-flight seal or compaction,
    or queued for deletion. If a file on disk is in none of these, nothing
    short of a filesystem walk could ever find it again.
    """
    tracked = {Path(path) for path in log._table.referenced_paths()}
    for pending in (log._buffer.pending_seal(), log._buffer.pending_compaction()):
        if pending is not None:
            tracked.add(log.root / pending[2])

    tracked |= {log.root / p for p in log._buffer.queued_deletions()}

    return tracked


def assert_nothing_untracked(log: Log) -> set[Path]:
    on_disk = set(log.root.rglob("*.parquet"))
    untracked = on_disk - tracked_paths(log)
    assert untracked == set(), f"{len(untracked)} file(s) findable only by scanning"

    return on_disk


def test_no_data_file_is_untracked_through_a_full_lifecycle(tmp_path: Path) -> None:
    """The property that lets reclamation be a keyed read instead of a walk.

    Every Parquet file this library creates must be findable from SQLite or
    from a live snapshot at all times — including mid-seal, mid-compaction, and
    while superseded files await their grace period. The test is allowed to
    walk the filesystem; the library is not.
    """
    config = LogConfig(
        compact_below=1 << 30,
        compact_min_files=2,
        local_retention=timedelta(days=365),
        snapshot_retention=timedelta(days=365),
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 4)
        assert_nothing_untracked(log)

        log.maintain()  # compacts; sources are superseded but not yet deletable
        on_disk = assert_nothing_untracked(log)
        assert len(on_disk) == 5, "4 sources awaiting deletion, plus the merge"
        assert len(log._buffer.queued_deletions()) == 4

        # Mid-seal: written, not yet committed, claimed by `sealing`.
        log.extend(rows(3, start=16))
        rel_path = log._layout.seal_path(17, 20, _today())
        log._buffer.claim_seal(17, 20, rel_path)
        log._write_and_commit(20, rel_path)
        assert_nothing_untracked(log)


def test_queued_files_are_deleted_once_the_grace_period_passes(tmp_path: Path) -> None:
    config = LogConfig(compact_below=1 << 30, compact_min_files=2)
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        log.maintain()
        assert len(log._buffer.queued_deletions()) == 3
        assert len(list(tmp_path.rglob("*.parquet"))) == 4

    # Reopen with a grace period short enough that the queue is due. The
    # deadline is evaluated against the CURRENT setting, not one frozen at
    # enqueue time, so lowering it takes effect on what is already queued.
    impatient = LogConfig(
        compact_below=1 << 30,
        compact_min_files=2,
        snapshot_retention=timedelta(microseconds=1),
    )
    with open_log(tmp_path, impatient) as log:
        log.maintain()

        assert log._buffer.queued_deletions() == []
        assert len(list(tmp_path.rglob("*.parquet"))) == 1
        assert len(read_all(log)) == 12


def test_a_referenced_file_is_never_deleted_by_the_drain(tmp_path: Path) -> None:
    """Belt and braces: the queue is a hint, a live reference is a veto."""
    config = LogConfig(
        compact_min_files=99, snapshot_retention=timedelta(microseconds=1)
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 1)
        live = log._table.data_files()[0]
        # Queue a file that is still referenced, which the grace period alone
        # would happily let through.
        log._maintenance._enqueue([live.path])

        log.maintain()

        assert Path(live.path).exists()
        assert len(read_all(log)) == 4


def test_iceberg_metadata_does_not_grow_without_bound(tmp_path: Path) -> None:
    """Iceberg's own bookkeeping leaks in two ways, neither self-correcting.

    metadata.json is written per commit and kept forever unless the table
    properties say otherwise. Manifest and manifest-list avro accumulate two
    per commit and survive expire_snapshots — verified against pyiceberg
    0.11.1. On a stream sealing every five minutes that is ~576 avro files a
    day that nothing would ever remove.
    """
    config = LogConfig(
        compact_min_files=99, snapshot_retention=timedelta(microseconds=1)
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 8)
        avro_before = len(list(tmp_path.rglob("*.avro")))
        assert avro_before >= 8, "one manifest list per commit, at least"

        log.maintain()
        log.maintain()  # the second pass retires what the first only queued

        metadata = list(tmp_path.rglob("*.metadata.json"))
        assert len(metadata) <= 11, f"{len(metadata)} metadata files for 8 commits"
        assert len(list(tmp_path.rglob("*.avro"))) < avro_before, (
            "expired snapshots' manifests were never reclaimed"
        )
        assert len(read_all(log)) == 32, "the data is still readable"


def test_metadata_properties_are_applied_to_an_existing_table(tmp_path: Path) -> None:
    """A table created before these properties existed must pick them up."""
    open_log(tmp_path).close()

    # Reach past litelink to strip the property, standing in for a table created
    # before these defaults existed.
    catalog = SqlCatalog(
        "local",
        uri=f"sqlite:///{tmp_path / 'catalog.db'}",
        warehouse=f"file://{tmp_path}",
    )
    table = catalog.load_table("litelink.s")
    with table.transaction() as transaction:
        transaction.remove_properties("write.metadata.delete-after-commit.enabled")

    assert (
        "write.metadata.delete-after-commit.enabled"
        not in catalog.load_table("litelink.s").properties
    )

    with open_log(tmp_path) as reopened:
        assert (
            reopened._table.properties["write.metadata.delete-after-commit.enabled"]
            == "true"
        )


def test_counts_from_the_manifest_list_match_the_files(tmp_path: Path) -> None:
    """The summaries are a shortcut, so they have to agree with the long way.

    `file_count` and `record_count` read the manifest LIST, which summarises
    each manifest — one file, rather than opening every manifest to walk its
    entries. The risk is the arithmetic: live files are ADDED plus EXISTING, and
    compaction and eviction both leave DELETED tombstones behind that must not
    be counted.
    """
    config = LogConfig(
        target_size=1 << 40,
        compact_below=1 << 30,
        compact_min_files=2,
        local_retention=timedelta(microseconds=1),
    )

    with open_log(tmp_path, config) as log:
        table = log._table

        def agrees(stage: str) -> None:
            table._counts_at = table._extent_at = None
            files = table.data_files()

            assert table.file_count() == len(files), stage
            assert table.record_count() == sum(f.rows for f in files), stage

        for i in range(4):
            log.extend(rows(50, start=i * 50))
            log.seal()

        agrees("after seals")

        log._maintenance.compact()
        agrees("after compaction")

        log.extend(rows(50, start=200))
        log.seal()
        agrees("seal after compaction")

        log._maintenance.evict()
        agrees("after eviction")

        assert table.file_count() == 0, "eviction removed everything"


def test_manifests_are_merged_rather_than_accumulated(tmp_path: Path) -> None:
    """One manifest per seal is what made the boundary read expensive.

    Each commit writes its own manifest, so N data files became N manifest avro
    files and deriving the boundary meant opening every one. Measured at 60
    files: 60 manifests and a 45 ms read, against 1 manifest and 2.3 ms merged.
    """
    config = LogConfig(compact_min_files=99)
    with open_log(tmp_path, config) as log:
        for i in range(8):
            log.extend(rows(20, start=i * 20))
            log.seal()

        table = log._table._table
        snapshot = table.current_snapshot()
        assert snapshot is not None
        manifests = snapshot.manifests(table.io)

        assert log.table_files() == 8, "eight seals, eight data files"
        assert len(manifests) < 8, (
            f"{len(manifests)} manifests for 8 files — not merging"
        )


def test_a_rewrite_never_writes_over_the_file_it_is_reading(tmp_path: Path) -> None:
    """A compaction's source is the file it replaces. A seal's is the buffer.

    That difference is why a seal may overwrite its path on retry and a
    compaction may not. With a deterministic `{lo}-{hi}` name, re-compacting a
    range that had already been compacted wrote to the path it was reading:
    `set_sort_by(rewrite=True)` after any compaction truncated the live,
    table-referenced file, and a crash mid-write destroyed the only copy of
    those rows. Two owners racing the role hit the same collision.
    """
    config = LogConfig(compact_below=1 << 30, compact_min_files=2)
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        log.maintain()

        before = [f.path for f in log._table.data_files()]

        assert len(before) == 1, "expected one compacted file to rewrite"

        log.set_sort_by(("key", "event_ts"), rewrite=True)
        after = [f.path for f in log._table.data_files()]

        assert len(after) == 1
        assert after[0] != before[0], "the rewrite reused the live file's path"
        assert len(read_all(log)) == 12
