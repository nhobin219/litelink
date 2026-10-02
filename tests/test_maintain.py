"""Local storage reclamation: compact, evict, expire (SPEC §6, §8, §12)."""

from __future__ import annotations

import os
import random
import threading
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pyiceberg.catalog.sql import SqlCatalog

import litelink
import litelink._maintenance as maintenance
from litelink._claim import EVERYTHING, Claim, new_owner
from litelink._config import COMPACT_MULTIPLE
from litelink._handle import LogConfig, WriteHandle, validate
from litelink._layout import Layout
from litelink._maintenance import _covered, runs, stable_prefix
from litelink._table import DataFile
from tests.test_log import SCHEMA, open_log, read_all, rows


def seal_files(log: WriteHandle, count: int, per_file: int = 4) -> None:
    """Produce `count` sealed files, each holding `per_file` rows."""
    for i in range(count):
        log.extend(rows(per_file, start=i * per_file))
        log.seal(flush=True)


def test_compaction_merges_adjacent_small_files(tmp_path: Path) -> None:
    with open_log(
        tmp_path, LogConfig(target_seal_size=1 << 30, compact_min_files=2)
    ) as log:
        seal_files(log, 4)
        assert len(log._table.data_files()) == 4

        log.advance()

        assert len(log._table.data_files()) == 1
        assert len(read_all(log)) == 16
        assert log.staging_extent() == (1, 17)


def test_compaction_needs_compact_min_files(tmp_path: Path) -> None:
    """Below the threshold the pass must leave the files alone."""
    with open_log(
        tmp_path, LogConfig(target_seal_size=1 << 30, compact_min_files=5)
    ) as log:
        seal_files(log, 4)
        log.advance()

        assert len(log._table.data_files()) == 4


def test_compaction_leaves_full_files_alone(tmp_path: Path) -> None:
    """In normal operation compaction is a no-op.

    Every file here came from a cut the appender made at `target_seal_size`, so each
    already holds what a file should. Merging any two would produce one holding
    twice that. The rule that decides this reads what the files hold in memory,
    not their size on disk — these compress to a fraction of the target, and
    judged that way every one of them looks starved.
    """
    config = LogConfig(
        target_seal_size=2048,
        # Equal, which is what makes this test about "already full" rather than
        # about conversion. The default is eight times the seal size, and under
        # that these files WOULD merge — correctly, into one published-shaped
        # file. See `test_compaction_converts_sealed_files_into_larger_ones`.
        target_compact_size=2048,
        compact_min_files=2,
    )
    with open_log(tmp_path, config) as log:
        log.extend(rows(200))
        log.seal(flush=True)
        before = len(log._table.data_files())
        assert before >= 3, "the target must be crossed several times"

        log.advance()

        assert len(log._table.data_files()) == before


def test_compaction_output_is_re_sorted(tmp_path: Path) -> None:
    """§6 step 2: re-sorted, not merely concatenated."""
    import pyarrow.parquet as pq

    config = LogConfig(target_seal_size=1 << 30, compact_min_files=2)
    with litelink.new(
        tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",), config=config
    ) as log:
        for ts in (500, 100, 400, 200):
            log.append({"event_ts": ts, "key": "k", "payload": ""})
            log.seal(flush=True)

        log.advance()

        merged = log._table.data_files()
        assert len(merged) == 1
        written = pq.read_table(merged[0].path)["event_ts"].to_pylist()
        assert written == [100, 200, 400, 500]


def test_compaction_preserves_every_row(tmp_path: Path) -> None:
    config = LogConfig(target_seal_size=1 << 30, compact_min_files=2)
    with open_log(tmp_path, config) as log:
        seal_files(log, 5, per_file=3)
        before = read_all(log)

        log.advance()

        assert read_all(log) == before


def test_eviction_drops_files_past_staging_retention(tmp_path: Path) -> None:
    """§8. Eviction drops what the published table holds, and the rows stay
    readable from there — a local-only log publishes to a local directory (#98)."""
    config = LogConfig(
        compact_min_files=99,  # isolate eviction from compaction
        staging_retention=timedelta(microseconds=1),
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        log.extend(rows(2, start=12))
        assert len(log._table.data_files()) == 3

        log.publish(flush=True)
        log.advance()

        assert log._table.data_files() == []
        # The buffer is untouched: retention governs the table, and buffer rows
        # are removed at seal and nowhere else (§8).
        assert log.buffered_rows() == 2
        assert len(read_all(log)) == 14, "every row, the evicted ones published"


def test_eviction_never_drops_what_is_not_published(tmp_path: Path) -> None:
    """I4 holds for every log (#98): a local-only log's retention used to
    delete the only copy, and now waits for `publish` like any other.

    Falsify by skipping the published table clamp in `Maintenance.evict`: every sealed
    file leaves, and twelve rows with it.
    """
    config = LogConfig(
        compact_min_files=99, staging_retention=timedelta(microseconds=1)
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        log.advance()

        assert len(log._table.data_files()) == 3, "nothing is published yet"
        assert len(read_all(log)) == 12


def test_no_eviction_without_staging_retention(tmp_path: Path) -> None:
    with open_log(tmp_path, LogConfig(compact_min_files=99)) as log:
        seal_files(log, 3)
        log.advance()

        assert len(log._table.data_files()) == 3
        assert len(read_all(log)) == 12


def test_expiry_drops_old_snapshots(tmp_path: Path) -> None:
    config = LogConfig(
        compact_min_files=99, staging_snapshot_retention=timedelta(microseconds=1)
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        assert log._table.snapshot_count() == 3

        log.advance()

        # The current snapshot is never expired, whatever its age.
        assert log._table.snapshot_count() == 1
        assert len(read_all(log)) == 12


def test_eviction_waits_for_the_published_table_to_hold_the_file(
    tmp_path: Path,
) -> None:
    """I4, and the only line of `advance` that is correctness (§5, §8).

    With a published table configured the local copy stops being the only one once the
    published table holds that FILE — a row `publish` writes when the copy exists, naming
    the bucket it went to (§4a). Not a watermark: one summarised the same facts
    and was the only boundary in the log that could move backwards.
    """
    config = LogConfig(staging_retention=timedelta(0))
    log = litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        published="s3://bucket/prefix",
    )
    try:
        seal_files(log, 3)
        before = log.staging_files()

        assert before == 3

        # Nothing published yet: retention says evict everything, I4 says none.
        # `evict`, the routine under test: `advance` would also publish, to a
        # bucket this test never stands up.
        log.evict()

        assert log.staging_files() == before, "evicted with an empty published table"

        # The published table now holds the first file, and only that one.
        first = min(log._table.data_files(), key=lambda f: f.start)
        log._buffer.record_file(
            f"s3://bucket/prefix/data/{first.start}.parquet", first.start, first.end, 1
        )
        log.evict()

        assert log.staging_files() == before - 1, "did not evict what was published"
    finally:
        log.close()


def test_reads_stay_correct_across_a_compaction(tmp_path: Path) -> None:
    """I8: once readable, a row stays readable."""
    config = LogConfig(target_seal_size=1 << 30, compact_min_files=2)
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        log.extend(rows(4, start=12))
        before = read_all(log)

        log.advance()

        assert read_all(log) == before
        assert [r[0] for r in read_all(log)] == list(range(1, 17))


def test_eviction_alone_does_not_free_disk(tmp_path: Path) -> None:
    """§12: local disk holds staging_retention + staging_snapshot_retention of data.

    Eviction removes a file from the current snapshot; the bytes stay on disk,
    referenced by the previous snapshot, until expiry deletes them. Conflating
    the two is how a disk-space calculation comes out a whole retention window
    short.
    """
    config = LogConfig(
        compact_min_files=99,
        staging_retention=timedelta(microseconds=1),
        staging_snapshot_retention=timedelta(days=365),  # nothing may expire
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        local = tmp_path / "s" / "data"
        on_disk = {p.name for p in local.rglob("*.parquet")}
        assert len(on_disk) == 3

        log.publish(flush=True)
        log.advance()

        assert log._table.data_files() == [], "evicted from the table"
        assert {p.name for p in local.rglob("*.parquet")} == on_disk, (
            "still on disk, held by the pre-eviction snapshot"
        )

    with open_log(
        tmp_path,
        LogConfig(
            compact_min_files=99, staging_snapshot_retention=timedelta(microseconds=1)
        ),
    ) as log:
        log.advance()

        assert list((tmp_path / "s" / "data").rglob("*.parquet")) == [], (
            "expiry is what deletes bytes"
        )


def test_sweep_spares_an_in_flight_seal(tmp_path: Path) -> None:
    """The one unreferenced file that must survive.

    A seal writes its Parquet before committing it, so between those two steps
    the file is on disk and no snapshot names it. `sealing` is what tells the
    sweep it is not an orphan — without that guard, an advance() interleaved
    with a seal deletes the file the very next step is about to register.
    """
    config = LogConfig(
        compact_min_files=99, staging_snapshot_retention=timedelta(microseconds=1)
    )
    with open_log(tmp_path, config) as log:
        log.extend(rows(3))
        rel_path = log._layout.seal_path(1, 4, "tok")
        log._buffer.claim_seal(1, 4, rel_path)

        written = tmp_path / rel_path
        written.parent.mkdir(parents=True, exist_ok=True)
        written.write_bytes(b"not really parquet, but on disk and uncommitted")

        log.advance()

        assert written.exists(), "sweep deleted a file `sealing` had claimed"


def test_recovery_queues_a_crashed_compaction_by_name(tmp_path: Path) -> None:
    """§11: no snapshot was committed, so the output is dead — and it is named.

    The point is that recovery resolves one known path. Nothing scans a
    directory to discover that this file was garbage.

    QUEUED rather than unlinked, because the owner being recovered from may not
    be dead. A maintainer stalled past its lease can wake between the check and
    the removal and commit the very file about to be deleted, taking the whole
    range with it. The queue's drain re-reads what the table references at
    unlink time, so the last word belongs to a check made when the file is
    actually removed — the same route an abandoned seal has always taken.
    """
    config = LogConfig(compact_min_files=99, staging_snapshot_retention=timedelta(0))
    with open_log(tmp_path, config) as log:
        seal_files(log, 1)
        rel_path = log._layout.compaction_path(1, 4, "deadbeef")
        log._buffer.claim_compaction(1, 4, rel_path)
        half_written = tmp_path / rel_path
        half_written.parent.mkdir(parents=True, exist_ok=True)
        half_written.write_bytes(b"a compaction that never committed")

    with open_log(tmp_path, config) as recovered:
        assert rel_path in recovered._buffer.queued_deletions()
        assert recovered._buffer.pending_compaction() is None

        recovered._maintenance.drain()

        assert not half_written.exists()
        # The inputs were never superseded, so the table is already correct.
        assert len(read_all(recovered)) == 4


def test_recovery_never_removes_a_file_the_table_adopted(tmp_path: Path) -> None:
    """The reason recovery queues instead of unlinking.

    An owner recovered from may still be alive — stalled past its lease — and
    can commit its output between the check and the removal. Here the file IS
    referenced, standing in for that commit having landed, and the drain must
    refuse it. Unlinking on the strength of an earlier read would take the
    whole range: the sources it superseded were queued before the commit and
    drain away behind it.
    """
    config = LogConfig(compact_min_files=99, staging_snapshot_retention=timedelta(0))
    with open_log(tmp_path, config) as log:
        seal_files(log, 1)
        live = log._table.data_files()[0]
        # Claimed as though a crashed compaction had produced it, while the
        # table in fact references it.
        log._buffer.claim_compaction(
            live.start, live.end, log._layout.relative(live.path)
        )

    with open_log(tmp_path, config) as recovered:
        recovered._maintenance.drain()

        assert Path(live.path).exists(), "a referenced file must survive recovery"
        assert len(read_all(recovered)) == 4


def tracked_paths(log: WriteHandle) -> set[Path]:
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


def assert_nothing_untracked(log: WriteHandle) -> set[Path]:
    on_disk = set(log.root.rglob("data/**/*.parquet"))
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
        target_seal_size=1 << 30,
        compact_min_files=2,
        staging_retention=timedelta(days=365),
        staging_snapshot_retention=timedelta(days=365),
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 4)
        assert_nothing_untracked(log)

        log.advance()  # compacts; sources are superseded but not yet deletable
        on_disk = assert_nothing_untracked(log)
        assert len(on_disk) == 5, "4 sources awaiting deletion, plus the merge"
        assert len(log._buffer.queued_deletions()) == 4

        # Mid-seal: written, not yet committed, claimed by `sealing`.
        log.extend(rows(3, start=16))
        rel_path = log._layout.seal_path(17, 20, "tok")
        log._buffer.claim_seal(17, 20, rel_path)
        log._write_and_commit(17, 20, rel_path)
        assert_nothing_untracked(log)


def test_manifests_a_commit_merged_away_are_reclaimed(tmp_path: Path) -> None:
    """#111: with manifest merging on, each seal writes an `-m0` manifest and
    folds it into an `-m1` before committing. Only the `-m1` is in a snapshot,
    so a reclaim that walks snapshots never found the `-m0`. On a long-running
    box that was 99% of them, and metadata grew with every seal.

    After expiry, the only unreferenced manifest left is the one the current
    snapshot's own commit merged away, which goes when that snapshot expires.

    Falsify by having `expire` enqueue `metadata_paths` instead of
    `expiring_paths`: one orphan per seal stays on disk.
    """
    config = LogConfig(compact_min_files=99, staging_snapshot_retention=timedelta(0))
    with open_log(tmp_path, config) as log:
        seal_files(log, 8)
        log._maintenance.expire()

        manifests = set(log.root.rglob("metadata/*-m*.avro"))
        referenced = {Path(path) for path in log._table.referenced_paths()}
        unreferenced = manifests - referenced
        assert len(unreferenced) <= 1, sorted(p.name for p in unreferenced)
        assert len(read_all(log)) == 32


def test_queued_files_are_deleted_once_the_grace_period_passes(tmp_path: Path) -> None:
    config = LogConfig(target_seal_size=1 << 30, compact_min_files=2)
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        log.advance()
        assert len(log._buffer.queued_deletions()) == 3
        assert len(list(tmp_path.rglob("data/**/*.parquet"))) == 4

    # Reopen with a grace period short enough that the queue is due. The
    # deadline is evaluated against the CURRENT setting, not one frozen at
    # enqueue time, so lowering it takes effect on what is already queued.
    impatient = LogConfig(
        target_seal_size=1 << 30,
        compact_min_files=2,
        staging_snapshot_retention=timedelta(microseconds=1),
    )
    with open_log(tmp_path, impatient) as log:
        log.advance()

        assert log._buffer.queued_deletions() == []
        assert len(list(tmp_path.rglob("data/**/*.parquet"))) == 1
        assert len(read_all(log)) == 12


def test_a_referenced_file_is_never_deleted_by_the_drain(tmp_path: Path) -> None:
    """Belt and braces: the queue is a hint, a live reference is a veto."""
    config = LogConfig(
        compact_min_files=99, staging_snapshot_retention=timedelta(microseconds=1)
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 1)
        live = log._table.data_files()[0]
        # Queue a file that is still referenced, which the grace period alone
        # would happily let through.
        log._maintenance._enqueue([live.path])

        log.advance()

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
        compact_min_files=99, staging_snapshot_retention=timedelta(microseconds=1)
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 8)
        # The staging table's, since `advance` now publishes too.
        staging = metadata_dir(log)
        avro_before = len(list(staging.glob("*.avro")))
        assert avro_before >= 8, "one manifest list per commit, at least"

        log.advance()
        log.advance()  # the second pass retires what the first only queued

        metadata = list(staging.glob("*.metadata.json"))
        assert len(metadata) <= 11, f"{len(metadata)} metadata files for 8 commits"
        assert len(list(staging.glob("*.avro"))) < avro_before, (
            "expired snapshots' manifests were never reclaimed"
        )
        assert len(read_all(log)) == 32, "the data is still readable"


def test_metadata_properties_are_applied_to_an_existing_table(tmp_path: Path) -> None:
    """A table created before these properties existed must pick them up."""
    open_log(tmp_path).close()

    # Reach past litelink to strip the property, standing in for a table created
    # before these defaults existed.
    layout = Layout(tmp_path, "s")
    catalog = SqlCatalog(
        "staging", uri=layout.catalog_uri, warehouse=layout.warehouse_uri
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
        target_seal_size=1 << 40,
        compact_min_files=2,
        staging_retention=timedelta(microseconds=1),
    )

    with open_log(tmp_path, config) as log:
        table = log._table

        def agrees(stage: str) -> None:
            table._counts_at = table._span_at = None
            files = table.data_files()

            assert table.file_count() == len(files), stage
            assert table.record_count() == sum(f.rows for f in files), stage

        for i in range(4):
            log.extend(rows(50, start=i * 50))
            log.seal(flush=True)

        agrees("after seals")

        log._maintenance.compact()
        agrees("after compaction")

        log.extend(rows(50, start=200))
        log.seal(flush=True)
        agrees("seal after compaction")

        log.publish(flush=True)
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
            log.seal(flush=True)

        table = log._table._table
        snapshot = table.current_snapshot()
        assert snapshot is not None
        manifests = snapshot.manifests(table.io)

        assert log.staging_files() == 8, "eight seals, eight data files"
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
    config = LogConfig(target_seal_size=1 << 30, compact_min_files=2)
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        log.advance()

        before = [f.path for f in log._table.data_files()]

        assert len(before) == 1, "expected one compacted file to rewrite"

        log.set_sort_by(("key", "event_ts"), rewrite=True)
        after = [f.path for f in log._table.data_files()]

        assert len(after) == 1
        assert after[0] != before[0], "the rewrite reused the live file's path"
        assert len(read_all(log)) == 12


def sized(*sizes: int) -> tuple[list[DataFile], dict[str, int]]:
    """Files holding the given uncompressed sizes, adjacent and in order.

    Sizes come as a separate mapping because that is how the real ones do: a
    file's size on disk is what compression made of it, and what it holds in
    memory is carried in the buffer beside it.
    """
    files, memory, offset = [], {}, 1
    for size in sizes:
        path = f"{offset}.parquet"
        # A deliberately misleading on-disk size: every rule under test must
        # read `memory`, and any that reaches for `size` gets a wrong answer.
        files.append(DataFile(path=path, size=1, rows=1, start=offset, end=offset + 1))
        memory[path] = size
        offset += 1

    return files, memory


def test_a_run_closes_before_it_exceeds_the_budget() -> None:
    """The output cap. Without it, a hundred files just under the line merge
    into one file a hundred times the target."""
    files, memory = sized(30, 30, 30, 30, 30)
    grouped = runs(files, 100, memory)

    assert [len(run) for run in grouped] == [3, 2]
    assert all(sum(memory[f.path] for f in run) <= 100 for run in grouped)


def test_a_file_over_the_budget_forms_its_own_run() -> None:
    """It has no room for a neighbour, so it must not drag one in."""
    files, memory = sized(10, 500, 10)
    grouped = runs(files, 100, memory)

    assert [[memory[f.path] for f in run] for run in grouped] == [[10], [500], [10]]


def test_an_unmeasured_file_counts_as_full() -> None:
    """Unknown is not zero.

    Treating an unrecorded size as small is what pulls an already-correct file
    into a rewrite; the cost of leaving it alone is only a merge that did not
    happen.
    """
    files, memory = sized(10, 10, 10)
    del memory[files[1].path]

    assert [len(run) for run in runs(files, 100, memory)] == [1, 1, 1]


def test_the_trailing_run_is_never_settled() -> None:
    """It is under budget, so a file that has not been written yet can still
    join it — pushing it now would published table something compaction will replace.

    Two files short of `min_files`, so compaction leaves them alone today; it
    is room in the budget, not the merge, that makes them unsettled.
    """
    files, memory = sized(60, 60, 20)

    assert stable_prefix(files, 100, 3, memory) == 1


def test_a_full_trailing_run_is_settled() -> None:
    """Nothing more fits, so nothing can change it."""
    files, memory = sized(60, 60, 100)

    assert stable_prefix(files, 100, 2, memory) == 3


def test_nothing_before_a_mergeable_run_is_settled() -> None:
    """Compaction is about to rewrite it, and the watermark is a prefix, so the
    files ahead of it cannot be published past it either."""
    files, memory = sized(200, 200, 10, 10, 10)

    assert stable_prefix(files, 100, 2, memory) == 2


def test_a_stranded_small_file_is_still_settled() -> None:
    """The regression that made a single explicit seal block the published table
    forever. A small file between larger neighbours can never be merged — no
    run containing it fits the budget — so waiting for it to grow waits
    forever, and the watermark never advances past it again."""
    files, memory = sized(98, 5, 98, 200)

    assert stable_prefix(files, 100, 2, memory) == 4


def test_sizing_does_not_depend_on_how_well_the_data_compressed() -> None:
    """What the on-disk rule got wrong.

    These files each hold a full target's worth of rows and compressed to an
    eighth of it. Judged by their size on disk they all look starved, and
    compaction merged eight at a time into a file holding eight times the
    memory the target allows — while `publish`, asking whether a file had reached
    half the target, found none and left the published table empty. Judged by what they
    hold, each is already full: nothing to merge, everything archivable.
    """
    target = 64 * 1024
    files, memory = sized(*([target] * 24))

    assert runs(files, target, memory) == [[f] for f in files], "each already full"
    assert stable_prefix(files, target, 2, memory) == 24


def test_eviction_outlives_the_snapshot_that_added_the_file(tmp_path: Path) -> None:
    """`staging_retention` must not depend on `staging_snapshot_retention`.

    A file's age came from the snapshot that added it, and expiry deletes that
    snapshot — after which the file was in no age map at all, `evict` could not
    classify it as stale, and it stayed on local disk for ever.

    The two settings are sized by unrelated things: §6 says `staging_snapshot_retention`
    must exceed the longest SCAN, §8 says `staging_retention` must exceed the
    longest hot LOOKBACK. So the ordinary configuration has expiry running in
    minutes and retention in days — and every file lost its age long before it
    was old enough to evict. Retention silently did nothing.
    """
    config = LogConfig(
        target_seal_size=1 << 30,
        compact_min_files=2,
        staging_retention=timedelta(microseconds=1),
        staging_snapshot_retention=timedelta(0),
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 4)
        files = log._table.data_files()
        assert len(files) == 4
        log.publish(flush=True)

        # A commit AFTER the last seal, which is what makes every remaining
        # file's adding snapshot expirable. Iceberg always keeps the current
        # snapshot, so without this the newest file stays dateable and drags
        # the rest out with it — which is why the fault only appears once a log
        # has been running a while, and never in a test that just seals.
        # Eviction itself is the commit that does it in practice.
        log._table.evict_below(files[0].end)
        log._maintenance.expire()

        remaining = log._table.data_files()
        assert remaining, "the fixture must leave files behind to evict"
        assert not set(log._table.snapshot_ages()) & {f.path for f in remaining}, (
            "the fixture must leave every remaining file undateable"
        )

        log._maintenance.evict()

        assert log._table.data_files() == [], (
            "a file whose adding snapshot has expired must still be evictable"
        )


def test_staging_rows_keeps_recent_data_a_time_window_would_drop(
    tmp_path: Path,
) -> None:
    """The case a window alone cannot express.

    An hour of a quiet stream is a handful of rows. A retention window sized
    for a busy stream then evicts almost everything the moment it goes quiet,
    and the next hot read — the thing `staging_retention` exists to serve — goes
    to the network for data written minutes ago.
    """
    config = LogConfig(
        target_seal_size=1 << 30,
        compact_min_files=2,
        staging_retention=timedelta(microseconds=1),
        staging_rows=8,
        staging_snapshot_retention=timedelta(0),
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 4)  # 4 rows each, all older than the window
        log._maintenance.evict()

        kept = log._table.data_files()
        assert sum(f.rows for f in kept) >= 8, (
            "the row floor must hold data the window would have dropped"
        )
        assert (kept[-1].end - 1) == 16, "the newest rows are the ones kept"


def test_the_two_retention_limits_keep_whichever_holds_more(tmp_path: Path) -> None:
    """Floors, not ceilings.

    Both say what must stay readable without a network round trip, so the
    binding one is whichever retains more — the opposite of how the seal
    combines its limits, where they are ceilings and the tighter wins.
    """
    config = LogConfig(
        target_seal_size=1 << 30,
        compact_min_files=2,
        # Retains everything: nothing is an hour old.
        staging_retention=timedelta(hours=1),
        # Retains almost nothing on its own.
        staging_rows=1,
        staging_snapshot_retention=timedelta(0),
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 4)
        log._maintenance.evict()

        assert len(log._table.data_files()) == 4, (
            "the window retains everything, so the row floor must not evict"
        )


def test_a_row_floor_alone_is_a_retention_policy(tmp_path: Path) -> None:
    """`staging_retention=None` used to mean "never evict", full stop. With a row
    floor set it means "no limit from TIME", and the floor still applies."""
    config = LogConfig(
        target_seal_size=1 << 30,
        compact_min_files=2,
        staging_retention=None,
        staging_rows=4,
        staging_snapshot_retention=timedelta(0),
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 4)
        log.publish(flush=True)
        log._maintenance.evict()

        kept = log._table.data_files()
        assert sum(f.rows for f in kept) == 4
        assert (kept[-1].end - 1) == 16


def test_compaction_converts_sealed_files_into_larger_ones(tmp_path: Path) -> None:
    """The job the split gives it.

    With one size knob a compacted file held exactly what a sealed file held,
    so compaction could repair an undersized file and never produce a large
    one — which is why it was a no-op in normal operation. Separating the two
    makes it a conversion stage: seal at the size a hot read wants to scan,
    compact at the size object storage wants to receive.
    """
    config = LogConfig(
        target_seal_size=4096,
        target_compact_size=4 * 4096,
        compact_min_files=2,
        staging_snapshot_retention=timedelta(0),
    )
    with open_log(tmp_path, config) as log:
        log.extend(rows(1200))
        log.seal()
        sealed = log._table.data_files()
        held = log._maintenance.memory()
        assert len(sealed) >= 4, "several full seals to convert"
        assert all(held[f.path] <= 4096 * 1.5 for f in sealed), "seal-sized"

        log.advance()

        compacted = log._table.data_files()
        after = log._maintenance.memory()
        assert len(compacted) < len(sealed), "compaction must merge"
        assert max(after[f.path] for f in compacted) > 4096, (
            "a compacted file must hold more than a sealed one"
        )
        assert all(after[f.path] <= 4 * 4096 for f in compacted), (
            "and no more than the compaction target"
        )
        assert log.scan().read_all().num_rows == 1200


def test_only_compacted_files_are_eligible_for_the_published_table(
    tmp_path: Path,
) -> None:
    """Eligibility falls out of the existing rule, with nothing added.

    `publish` pushes what compaction has finished with. Raise the compaction
    target above the seal size and a freshly sealed file is a merge candidate
    by definition — so it is not settled, and not published, until it has been
    converted. No separate eligibility flag, and no way for the two to disagree
    about which files are still in play.
    """
    config = LogConfig(
        target_seal_size=4096,
        target_compact_size=8 * 4096,
        compact_min_files=2,
        staging_snapshot_retention=timedelta(0),
    )
    with open_log(tmp_path, config) as log:
        log.extend(rows(200))
        log.seal()
        sealed = log._table.data_files()
        memory = log._maintenance.memory()

        settled = stable_prefix(
            sealed,
            config.compact_size,
            config.compact_min_files,
            memory,
            config.compact_rows,
        )

        assert settled == 0, (
            "sealed files are merge candidates, so none may be published yet"
        )


def test_the_compaction_target_defaults_to_a_multiple_of_the_seal(
    tmp_path: Path,
) -> None:
    """Conversion is on by default, including with no published table.

    File count is a measured read cost here rather than a reputation: reading
    the offset boundary from manifest statistics measured 1.0 ms over one file
    and 44 ms over 64. A local-only log gets that benefit too, which is why the
    default is a multiple rather than "same as the seal, convert nothing".
    """
    config = LogConfig(target_seal_size=4096, compact_min_files=2)

    assert config.compact_size == 4096 * COMPACT_MULTIPLE

    with open_log(
        tmp_path, replace(config, staging_snapshot_retention=timedelta(0))
    ) as log:
        log.extend(rows(1200))
        log.seal()
        before = len(log._table.data_files())
        assert before >= 4

        log.advance()

        assert len(log._table.data_files()) < before, (
            "the conversion must run without a published table configured"
        )
        assert log.scan().read_all().num_rows == 1200


def test_row_ceilings_scale_with_the_conversion_too() -> None:
    """Setting only the seal's row limit must not cap conversion at one seal.

    A ceiling that did not scale would make `compact_rows` equal
    `target_seal_rows`, so every sealed file would already be at it and nothing
    would ever merge — the conversion silently off for anyone who set a row
    limit.
    """
    assert LogConfig(target_seal_size=4096, target_seal_rows=100).compact_rows == (
        100 * COMPACT_MULTIPLE
    )
    assert LogConfig(target_seal_size=4096).compact_rows is None


def test_a_compaction_target_under_the_seal_size_is_refused() -> None:
    """It would ask compaction to shrink a file it just merged, for ever."""
    config = LogConfig(target_seal_size=8192, target_compact_size=4096)
    with pytest.raises(ValueError, match="target_compact_size"):
        validate(SCHEMA, (), config, None)


def test_the_passes_can_be_run_separately(tmp_path: Path) -> None:
    """`advance` is one call for all three; the parts are callable alone.

    Their costs differ by orders of magnitude now that conversion reads and
    rewrites whole files while eviction and expiry are metadata commits, so a
    deployment may want them on different schedules. Running them separately
    has to reach the same state as running them together.
    """
    config = LogConfig(
        target_seal_size=4096,
        target_compact_size=8 * 4096,
        compact_min_files=2,
        staging_retention=timedelta(microseconds=1),
        staging_snapshot_retention=timedelta(0),
    )
    with open_log(tmp_path, config) as log:
        log.extend(rows(1200))
        log.seal()
        before = len(log._table.data_files())
        assert before >= 4

        log.compact()
        converted = len(log._table.data_files())
        assert converted < before, "compaction must run on its own"

        log.publish(flush=True)
        log.evict()
        log.reclaim()

        assert log._table.data_files() == [], "eviction must run on its own"
        # Every row still reads: the evicted ones from the published table,
        # the unsealed tail from the buffer, which eviction never touches.
        assert log.scan().read_all().num_rows == 1200
        assert log.buffered_rows() > 0, "the tail never reached the seal target"


def test_a_pass_defers_to_a_claim_over_the_range_it_wanted(tmp_path: Path) -> None:
    """Exclusion is by range now, not by role (§4a).

    What stops two maintainers compacting the same run to the same
    deterministic path — a torn file rather than a conflict Iceberg could
    resolve — is that the second finds the range claimed. It skips rather than
    raising: another owner working there is ordinary, not an error, and the
    work is still there next pass.
    """
    with open_log(
        tmp_path, LogConfig(target_seal_size=4096, compact_min_files=2)
    ) as log:
        seal_files(log, 3)
        before = len(log._table.data_files())

        assert before >= 2

        held = log._lease("maintain")

        assert held.acquire()

        try:
            log.compact()

            assert len(log._table.data_files()) == before, (
                "compacted a range another owner had claimed"
            )
        finally:
            held.release()

        log.compact()

        assert len(log._table.data_files()) < before, "did not compact once free"


def test_two_owners_compact_disjoint_ranges_at_once(tmp_path: Path) -> None:
    """The point of claiming a range instead of a role (§4a).

    Two operations on disjoint offsets commute, so nothing needs to serialise
    them. Under one lease per role the second waited on the first for the whole
    of its work — reading and rewriting Parquet, none of which touches anything
    the other reads.
    """
    with open_log(
        tmp_path, LogConfig(target_seal_size=4096, compact_min_files=2)
    ) as log:
        seal_files(log, 4)
        files = sorted(log._table.data_files(), key=lambda f: f.start)

        assert len(files) >= 4

        # One owner is working on the bottom of the log.
        low = log._buffer.claim("compact", files[0].start, files[1].end, "other-owner")

        assert low.acquire()

        try:
            # A second owner claims the top, and is not refused.
            high = log._buffer.claim("compact", files[2].start, files[-1].end, "mine")

            assert high.acquire(), "disjoint ranges must not exclude each other"

            high.release()

            # Overlapping, and it is.
            clash = log._buffer.claim("compact", files[1].start, files[2].end, "mine")

            assert not clash.acquire(), "overlapping ranges must exclude"
        finally:
            low.release()


def test_eviction_only_ever_removes_whole_files(tmp_path: Path) -> None:
    """A row floor lands mid-file; the boundary must not.

    `evict_below` filters by ROW, so a boundary inside a file makes pyiceberg
    rewrite it copy-on-write — and the replacement lands at a path this library
    never learns, which breaks the rule every reclamation rests on: a file's
    path is in SQLite before the file exists (I2). The superseded original is
    left out of the deletion queue too, so once expiry drops the snapshots
    naming it, nothing can name it again. `drain` is a keyed read and this
    design refuses directory scans, so it is unreclaimable for good.

    The age limit is already file-aligned — it is some file's `hi` — and so is
    the published table clamp. Only the row floor is arbitrary, which is why it arrived
    with `staging_rows`.
    """
    config = LogConfig(
        target_seal_size=1 << 30,
        target_compact_size=1 << 30,
        compact_min_files=2,
        staging_retention=timedelta(microseconds=1),
        # Deliberately not a multiple of the 4 rows each sealed file holds, so
        # the raw boundary falls inside one.
        staging_rows=6,
        staging_snapshot_retention=timedelta(0),
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 5)
        before = {f.path for f in log._table.data_files()}
        assert len(before) == 5

        log._maintenance.evict()

        after = log._table.data_files()
        assert {f.path for f in after} <= before, (
            "eviction must not introduce a file, which is what a copy-on-write "
            "rewrite of a straddling file would do"
        )
        # Whole files only: every survivor keeps the exact range it was sealed
        # with, and the row floor is honoured by keeping MORE than asked.
        assert sum(f.rows for f in after) >= 6
        assert all(f.rows == 4 for f in after), "a file was split by the boundary"


def test_compaction_will_not_merge_a_file_the_published_table_holds(
    tmp_path: Path,
) -> None:
    """A merge spanning the published table's extent is a duplicate that cannot be undone.

    Its inputs would include files already pushed, so the merged file covers a
    range partially overlapping one the published table holds — and `register` declines
    only a range that is ENTIRELY covered, so the partial one is admitted and
    the same offsets sit in two published files for ever.

    Compaction therefore skips a file the published table holds, asked per file (§4a).
    That is also what keeps the two tiers' ranges aligned: a file the published table
    holds is never rewritten locally, so the ranges stay comparable at all.
    """
    config = LogConfig(
        target_seal_size=4096,
        target_compact_size=8 * 4096,
        compact_min_files=2,
        staging_snapshot_retention=timedelta(0),
    )
    log = litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        published="s3://bucket/prefix",
    )
    try:
        log.extend(rows(1200))
        log.seal()
        files = sorted(log._table.data_files(), key=lambda f: f.start)

        assert len(files) >= 4

        # The published table holds the first two.
        for data_file in files[:2]:
            log._buffer.record_file(
                f"s3://bucket/prefix/data/{data_file.start}.parquet",
                data_file.start,
                data_file.end,
                1,
            )

        boundary = files[1].end - 1
        log.compact()

        merged = log._table.data_files()

        assert all(f.start > boundary or (f.end - 1) <= boundary for f in merged), (
            "no file may span the published table's extent, or the published table gets it twice"
        )
        assert log.scan().read_all().num_rows == 1200
    finally:
        log.close()


def test_rewriting_the_published_table_does_not_strand_staging_eviction(
    tmp_path: Path,
) -> None:
    """The two tiers cut the same rows independently, and I4 must not care.

    `compact("published")` re-cuts the published table to different boundaries — that is its
    whole job. Asking whether a local file's range EQUALS a published one then
    failed for every local file, permanently: eviction clamped to zero and
    stopped, and compaction stopped treating published files as the published table's
    business and merged across its extent. Neither heals, because nothing ever
    re-cuts the published table back.
    """
    log = litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        published="s3://bucket/prefix",
    )
    with log:
        seal_files(log, 3, per_file=4)
        files = sorted(log._table.data_files(), key=lambda f: f.start)

        assert len(files) == 3

        # The published table holds every row of all three, cut its own way: two files
        # whose boundaries line up with none of the local ones.
        start, end = files[0].start, files[-1].end
        middle = files[1].start + 1
        log._buffer.record_file("s3://bucket/prefix/data/a.parquet", start, middle, 1)
        log._buffer.record_file("s3://bucket/prefix/data/b.parquet", middle, end, 1)

        assert (
            log._maintenance.published_prefix(
                files, log._published.uri, include_intents=False
            )
            == end
        ), "the published table holds every row; how it cut them is not I4's business"


def test_a_gap_in_the_published_table_stops_the_walk(tmp_path: Path) -> None:
    """Coverage must join adjacent files without inventing rows between them."""
    log = litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        published="s3://bucket/prefix",
    )
    with log:
        seal_files(log, 3, per_file=4)
        files = sorted(log._table.data_files(), key=lambda f: f.start)

        # The first file, then a hole, then the third.
        log._buffer.record_file(
            "s3://bucket/prefix/data/a.parquet", files[0].start, files[0].end, 1
        )
        log._buffer.record_file(
            "s3://bucket/prefix/data/c.parquet", files[2].start, files[2].end, 1
        )

        assert (
            log._maintenance.published_prefix(
                files, log._published.uri, include_intents=False
            )
            == files[0].end
        ), "a range the published table does not hold must stop the walk"


def test_a_merge_will_not_resurrect_rows_evicted_since_it_chose_its_run(
    tmp_path: Path,
) -> None:
    """A claim taken after the premise was read isolates nothing on its own.

    Compaction lists the files once and claims per run, so eviction can claim
    that range, commit its removal and release it in between. The sources are
    still on disk under I6's grace, so the merge reads them happily and
    `_commit` retries the swap onto the fresh table — committing evicted rows
    back into the log, with a fresh `named_at` that shields them for another
    whole retention period.
    """
    config = LogConfig(target_seal_size=1 << 30, compact_min_files=2)
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        run = sorted(log._table.data_files(), key=lambda f: f.start)
        rows_before = len(read_all(log))

        assert len(run) == 3

        # Eviction happened after this run was chosen and before the merge
        # takes its claim.
        log._table.evict_below(run[0].end)
        remaining = len(read_all(log))

        assert remaining < rows_before, "the setup must actually evict"

        log._maintenance._rewrite_run(log._table, run, None)

        assert len(read_all(log)) == remaining, (
            "the merge put back rows eviction had removed"
        )


def test_the_coverage_walk_agrees_with_the_offsets_it_stands_for() -> None:
    """`_covered` is an optimisation of a set membership test; prove it is one.

    I4 asks whether the published table holds a local file's rows. The honest way to
    answer is to build the set of offsets the published table holds and test the file's
    against it, which is unaffordable; the walk is what makes it affordable, so
    it has to give the same answer for every shape — overlapping published
    ranges, duplicates, gaps, ranges reaching in from below.
    """
    random.seed(20260822)
    for _ in range(4000):
        ranges = []
        for _ in range(random.randint(0, 4)):
            start = random.randint(0, 12)
            ranges.append((start, start + random.randint(0, 6)))

        lo = random.randint(0, 12)
        hi = lo + random.randint(0, 6)

        held: set[int] = set()
        for start, end in ranges:
            held |= set(range(start, end))

        assert _covered(sorted(ranges), lo, hi) == (set(range(lo, hi)) <= held), (
            f"disagreed on {sorted(ranges)} covering [{lo}, {hi})"
        )


def test_eviction_will_not_commit_after_its_claim_has_lapsed(tmp_path: Path) -> None:
    """The merge path asks this before its commit; eviction did not.

    A claim expires 30 s after it is taken, and nothing between the acquire and
    the commit consulted it again. Lapsed, a compaction may claim a run below
    the boundary and pass its own premise check truthfully — the sources ARE
    still live — and then this commit removes them while the merge, whose claim
    is valid throughout, commits them back.
    """
    config = LogConfig(staging_rows=1, target_seal_size=1 << 30)
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        log.publish(flush=True)
        before = log.staging_files()

        assert before == 3

        # The eviction claim lapses and another owner takes the range, which is
        # what "no longer ours" actually means: an expired claim nobody wants
        # may still be renewed, by design.
        original = Claim.acquire
        rival: list[Claim] = []

        def lapsing(self: Claim) -> bool:
            if not original(self):
                return False

            if self.kind != "evict":
                return True

            with self.lock:
                self.connection.execute(
                    "UPDATE claim SET expires_at = 1 WHERE id = ?", (self.row_id,)
                )

            taker = Claim(
                self.connection,
                self.lock,
                "compact",
                self.start,
                (self.end - 1),
                new_owner(),
            )
            assert original(taker)
            rival.append(taker)

            return True

        Claim.acquire = lapsing
        try:
            with pytest.raises(RuntimeError, match="lost the"):
                log.evict()

        finally:
            Claim.acquire = original
            for taker in rival:
                taker.release()

        assert log.staging_files() == before, "committed without holding the claim"


def test_an_outer_renew_does_not_switch_off_the_run_claim(tmp_path: Path) -> None:
    """`renew or claim.renew` read naturally and was wrong.

    A rewrite run under the whole-log lease — `compact("published")`,
    `rewrite_sorted` — passes that lease's `renew` down. Taking it in place of
    the run claim's stopped the run claim from being renewed at all, and the
    pre-commit check then consulted the outer claim instead. A merge over the
    TTL lost its exclusion with no stall required.

    Falsify by making `_both` return `theirs` when it is given: no run claim
    is renewed.
    """
    config = LogConfig(target_seal_size=1 << 30, compact_min_files=2)
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        run = sorted(log._table.data_files(), key=lambda f: f.start)
        renewed: list[int] = []
        original = Claim.renew

        def counting(self: Claim) -> bool:
            renewed.append(self.row_id or 0)

            return original(self)

        Claim.renew = counting
        try:
            log._maintenance._rewrite_run(log._table, run, lambda: True)

        finally:
            Claim.renew = original

        assert renewed, "the run claim was never renewed while a merge ran"


def test_drain_needs_no_claim_and_still_keeps_what_is_referenced(
    tmp_path: Path,
) -> None:
    """`drain` takes no claim (#118): a queued name can never become
    referenced again, so the veto plus the grace period are the whole of its
    safety. It runs while another owner holds the entire log — and a due entry
    that a live snapshot still references is kept, not unlinked.

    Falsify by dropping the reference veto in `drain`: the live file is
    unlinked and the log can no longer read its rows.
    """
    config = LogConfig(
        target_seal_size=1 << 30,
        compact_min_files=2,
        staging_snapshot_retention=timedelta(0),
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        log.compact()
        live = log._table.data_files()[0]
        # Queued with a stamp long past, while the current snapshot names it:
        # what a compaction's recovery leaves when the commit had in fact
        # landed.
        log._buffer.enqueue_deletions([log._maintenance._key(live.path)], 0)
        superseded = [
            p
            for p in log._buffer.due_deletions(2**62)
            if p != log._maintenance._key(live.path)
        ]
        assert superseded, "expected superseded files awaiting deletion"

        other = Claim(
            log._buffer._con, log._buffer._lock, "maintain", 0, EVERYTHING, new_owner()
        )
        assert other.acquire()
        try:
            # Expires the snapshot that still named them, then drains.
            log.reclaim("staging")
        finally:
            other.release()

        due = set(log._buffer.due_deletions(2**62))
        assert not set(superseded) & due, "drain waited on another owner's claim"
        assert Path(live.path).exists(), "a referenced file was unlinked"
        assert log._maintenance._key(live.path) in due, "and it stays queued"
        assert len(read_all(log)) == 12


def test_eviction_reads_the_policy_the_log_records(tmp_path: Path) -> None:
    """`set_config` writes durable state; a maintainer has to hear about it.

    The same reasoning the published location already earned, applied to the
    settings beside it. Eviction is where it shows: it decides deletions from
    `staging_retention` and `staging_rows`, so a process holding the copy it read
    at open goes on deleting the only copy of rows the durable policy now says
    to keep — and §8 reads as an obligation, not a hint.
    """
    with open_log(tmp_path, LogConfig(staging_rows=1, target_seal_size=1 << 30)) as log:
        seal_files(log, 3)
        before = log.staging_files()

        assert before == 3

        # Another process raises the floor to cover everything.
        log._buffer.set_meta("config", LogConfig(staging_rows=10_000).to_json())
        log.evict()

        assert log.staging_files() == before, (
            "evicted against the policy this process happened to read at open"
        )


def test_a_refreshed_policy_reaches_compaction_publish_and_the_buffer(
    tmp_path: Path,
) -> None:
    """One owner, or compaction and publish can disagree about what is in play.

    `WriteHandle` used to keep its own copy of the policy beside `Maintenance`'s, kept
    in step by `set_config` writing both. Refreshing only one of them left
    compaction reading the new policy while `publish` read the old — and `runs`
    exists precisely so those two cannot disagree, because a file `publish`
    settles under one grouping and compaction merges under another leaves the
    published table holding rows rewritten underneath it. The buffer's seal target is
    the third copy, and a stale one sizes every file the log writes.
    """
    with open_log(tmp_path, LogConfig(staging_rows=1, target_seal_size=4096)) as log:
        seal_files(log, 2)
        raised = LogConfig(
            staging_rows=10_000, target_seal_size=1 << 20, target_compact_size=1 << 23
        )
        log._buffer.set_meta("config", raised.to_json())

        log.evict()

        assert log.config.target_compact_size == raised.target_compact_size, (
            "publish reads the policy through WriteHandle; it must be the refreshed one"
        )
        assert log._maintenance.config.staging_rows == raised.staging_rows
        assert log._buffer.config().target_seal_size == raised.target_seal_size, (
            "the buffer sizes every file the log writes; it reads the same row"
        )


def test_maintenance_survives_the_policy_changing_underneath_it(
    tmp_path: Path,
) -> None:
    """The hazard removing the cached copy introduces, and why it is tolerable.

    A value that was stable for a whole pass is now read live, so it can change
    between two reads inside one decision — `stable_prefix` alone reads three
    fields. What keeps that safe is that the policy is a POLICY: it decides how
    big to cut and when to merge, never which rows go where. A torn read makes
    a badly-sized file, not a wrong one.

    The one place it could have been an invariant is `runs`, which compaction
    and `publish` share so they cannot disagree about what is in play — and per
    segment I4 closes that: a file the published table holds is never merged again, so
    a disagreement costs an undersized published file, which `_push` already
    documents as tolerated.
    """
    config = LogConfig(target_seal_size=4096, compact_min_files=2, staging_rows=200)
    with open_log(tmp_path, config) as log:
        stop = threading.Event()
        churned = 0
        failures: list[str] = []

        def flip() -> None:
            nonlocal churned
            sizes = (8192, 16384, 32768)
            while not stop.is_set():
                churned += 1
                try:
                    log.set_config(
                        replace(
                            config,
                            target_compact_size=sizes[churned % 3],
                            compact_min_files=2 + (churned % 3),
                            # The OPTIONAL fields too, which the first version
                            # of this test missed: it churned only ints, and a
                            # torn read of two ints is merely an odd size. A
                            # field seen as an int by the guard and as None by
                            # the arithmetic after it is `int - None`.
                            staging_rows=None if churned % 2 else 200,
                            staging_retention=None
                            if churned % 3
                            else timedelta(seconds=30),
                        )
                    )
                except RuntimeError:
                    pass
                except Exception as exc:  # noqa: BLE001
                    failures.append(f"{type(exc).__name__}: {exc}")

        thread = threading.Thread(target=flip, daemon=True)
        thread.start()
        try:
            written = 0
            for _ in range(12):
                log.extend(rows(120, start=written))
                written += 120
                log.seal(flush=True)
                try:
                    log.advance()
                except RuntimeError:
                    pass
        finally:
            stop.set()
            thread.join(timeout=5)

        assert not failures, failures[:3]
        assert churned > 1, "the policy never actually changed"

        offsets = [r[0] for r in read_all(log)]

        assert len(set(offsets)) == len(offsets), (
            "duplicate rows under a churning policy"
        )
        assert log.scan().read_all().num_rows == len(offsets)


def test_a_decision_reads_the_policy_once(tmp_path: Path) -> None:
    """The invariant itself, rather than a crash staged to prove it.

    Each `self.config` is now an independent read of the durable row, so two
    of them inside one decision can disagree — and here they did arithmetic on
    each other: `staging_rows` seen as an int by the guard and as None by the
    subtraction after it is `int - None`, a TypeError out of `advance()`. The
    shipped maintainer catches RuntimeError and CommitFailedException, so that
    stopped maintenance entirely.

    Counting the reads tests that directly. Staging the crash instead means
    engineering an exact ordering, which is a test of the harness rather than
    of the code — the first attempt at this passed against the broken version
    because the values happened to line up harmlessly.
    """
    config = LogConfig(
        target_seal_size=1 << 30, staging_rows=200, staging_retention=None
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        original = log._buffer.config
        reads = 0

        def counting() -> LogConfig:
            nonlocal reads
            reads += 1

            return original()

        log._buffer.config = counting  # ty: ignore[invalid-assignment]
        try:
            boundary = log._maintenance._retention_boundary()

        finally:
            log._buffer.config = original  # ty: ignore[invalid-assignment]

        assert isinstance(boundary, int)
        assert reads == 1, (
            f"read the policy {reads} times in one decision; two reads can "
            "disagree, and these two do arithmetic on each other"
        )


def test_the_published_prefix_is_always_a_file_boundary(tmp_path: Path) -> None:
    """What `_push`'s arithmetic rests on.

    It splits `pending` at `published_prefix` and counts the part below as
    settled, which is only a prefix count if the split lands on a file edge —
    otherwise `pending[:settled]` names a different set than the one the split
    described, and the watermark is written from the wrong file.

    Files are ordered by offset and the walk stops at the first file the
    published table does not fully hold, so the answer is either 0 or some file's `end`,
    and everything below it is a prefix. Asserted over random coverage
    rather than argued.
    """
    random.seed(20260823)
    log = litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        published="s3://bucket/prefix",
    )
    with log:
        seal_files(log, 5, per_file=4)
        files = sorted(log._table.data_files(), key=lambda f: f.start)

        assert len(files) == 5

        for trial in range(40):
            with log._buffer._lock:
                log._buffer._con.execute(
                    "DELETE FROM extent WHERE rel_path LIKE 's3://%'"
                )
                log._buffer._con.commit()

            # A random subset of the files gets a published copy.
            for index, data_file in enumerate(files):
                if random.random() < 0.6:
                    log._buffer.record_file(
                        f"s3://bucket/prefix/data/{trial}-{index}.parquet",
                        data_file.start,
                        data_file.end,
                        1,
                    )

            frozen = log._maintenance.published_prefix(
                files, "s3://bucket/prefix", include_intents=False
            )
            below = [f for f in files if f.start < frozen]
            above = [f for f in files if f.start >= frozen]

            assert frozen == 0 or frozen in {f.end for f in files}, (
                f"{frozen} is not a file boundary"
            )
            assert files[: len(below)] == below, "the split is not a prefix"
            assert files[len(below) :] == above, "the remainder is not a suffix"


def test_coverage_is_read_in_one_statement(tmp_path: Path) -> None:
    """The union is one query, so it is one snapshot.

    Read as two statements, `extent` and `extent_intent` are two separate WAL
    reads — and a confirm committing between them moves a range out of the
    first after the second was taken, so it appears in NEITHER. Compaction's
    read is safe only when it overstates coverage; that understates it, which
    is the straddle this record exists to prevent, reopened by the shape of the
    query rather than by the design.

    Asserted by counting the statements, because the race itself is
    microseconds wide and a test that tried to land inside it would be a test
    of the harness.
    """
    with open_log(tmp_path, LogConfig(target_seal_size=4096)) as log:
        seal_files(log, 2)
        log._buffer.intend_file("s3://bucket/prefix/data/a.parquet", 1, 5, 99)

        statements: list[str] = []

        def watching(sql: str) -> None:
            if "extent" in sql and "SELECT" in sql:
                statements.append(sql)

        # SQLite's own hook: `Connection.execute` is read-only and cannot be
        # wrapped.
        log._buffer._con.set_trace_callback(watching)
        try:
            log._buffer.published_ranges("s3://bucket/prefix", 0, include_intents=True)

        finally:
            log._buffer._con.set_trace_callback(None)

        assert len(statements) == 1, (
            f"coverage read in {len(statements)} statements; two snapshots can "
            "disagree and a range can fall out of both"
        )
        assert "extent_intent" in statements[0], "the intents must be in that statement"


def test_the_deletion_grace_starts_at_the_commit(tmp_path: Path) -> None:
    """A reader cannot hold a file the commit has not yet superseded.

    Superseded files are queued BEFORE the commit, because a crash in between
    would lose the only record of their paths. But the grace period is about
    readers still holding them (I6), so it has to be measured from when they
    actually left the table — stamped at the queueing, a merge slower than
    `staging_snapshot_retention` burns the whole grace before it commits, and the
    originals are due the instant they stop being referenced.

    Worse for an attempt that aborts and is retried later: it commits against
    a stamp spent whenever the first attempt began.
    """
    config = LogConfig(target_seal_size=1 << 30, compact_min_files=2)
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        sources = [f.path for f in log._table.data_files()]

        assert len(sources) == 3

        # Queued as a slow merge would leave them: long before the commit.
        stale = int(datetime.now(UTC).timestamp()) - 86_400
        log._buffer.enqueue_deletions(
            [log._maintenance._key(p) for p in sources], stale
        )

        assert log._buffer.due_deletions(stale + 1), "the setup must look overdue"

        log.compact()

        # After the merge that superseded them, the clock reads from now.
        overdue = log._buffer.due_deletions(stale + 1)

        assert not overdue, (
            f"still due against a stamp from before the commit: {overdue}"
        )


def test_eviction_restamps_what_it_drops(tmp_path: Path) -> None:
    """Eviction is a supersession commit, and re-dates what it drops to it.

    A path already queued with an old stamp — once reachable with no failure
    at all through `hydrate` re-registering a queued name, removed in #118 —
    would otherwise keep that stamp through the `INSERT OR IGNORE`, leave the
    table already overdue, and be drained out from under a reader streaming
    it. The stamp is what the reader's grace rests on, so it is asserted
    directly.
    """
    config = LogConfig(staging_rows=1, target_seal_size=1 << 30)
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        log.publish(flush=True)
        dropped = [log._maintenance._key(f.path) for f in log._table.data_files()]

        assert len(dropped) == 3

        # Already queued with a day-old stamp.
        stale = int(datetime.now(UTC).timestamp()) - 86_400
        log._buffer.enqueue_deletions(dropped, stale)

        assert log._buffer.due_deletions(stale + 1), "the setup must look overdue"

        log.evict()

        # Only what actually LEFT the table. Eviction keeps the newest file,
        # and one it did not supersede is rightly still carrying the stamp
        # invented above.
        still = {log._maintenance._key(f.path) for f in log._table.data_files()}
        removed = [p for p in dropped if p not in still]

        assert removed, "eviction dropped nothing, so this asserts nothing"

        overdue = [p for p in log._buffer.due_deletions(stale + 1) if p in removed]

        assert not overdue, f"evicted files still due against a spent stamp: {overdue}"


def test_expiry_restamps_the_metadata_it_supersedes(tmp_path: Path) -> None:
    """The restamp has to speak the queue's key language.

    `metadata_paths` returns absolute paths and the queue is keyed
    root-relative, so passing them raw makes the UPDATE match nothing —
    silently, because that is what SQL does. Expiry's own trailing `drain()`
    then unlinks every just-superseded manifest in the same call with no grace,
    out from under a reader that planned its scan through them.

    Tested at the mapping rather than by staging the route, because the route
    needs a manifest shared between snapshots and this log merges manifests on
    every commit — the natural case is rare, while the mistake is not.
    """
    with open_log(tmp_path, LogConfig(target_seal_size=1 << 30)) as log:
        maintenance = log._maintenance
        absolute = str(log._layout.absolute("s/metadata/shared-m0.avro"))
        stale = int(datetime.now(UTC).timestamp()) - 86_400

        maintenance._enqueue([absolute])
        with log._buffer._lock:
            log._buffer._con.execute(
                "UPDATE pending_delete SET superseded_at = ?", (stale,)
            )
            log._buffer._con.commit()

        assert log._buffer.due_deletions(stale + 1), "the setup must look overdue"

        # Exactly what `expire` does after its commit.
        log._buffer.restamp_deletions(
            (maintenance._key(p) for p in [absolute]),
            int(datetime.now(UTC).timestamp()),
        )

        assert not log._buffer.due_deletions(stale + 1), (
            "the restamp matched nothing: it must key paths the way the "
            "enqueue beside it does"
        )


def test_staging_rows_counts_rows_rather_than_differencing_offsets(
    tmp_path: Path,
) -> None:
    """§8's floor has to survive a hole in the offset space.

    It used to be `next_offset() - 1 - staging_rows`, which reads naturally and
    assumes offsets are dense. A rollback's occasional gap makes that retain
    slightly MORE than asked — the safe direction. A reservation does the
    opposite: a restore skips 2**20 offsets to keep I9, so the subtraction puts
    the boundary far above every local file and the first `advance()` evicts
    the entire staging window.

    Simulated here by seeding the sequence forward, which is what a restore
    does. The rows already sealed must stay.
    """
    config = replace(
        LogConfig(), target_seal_size=2048, compact_min_files=2, staging_rows=1_000_000
    )
    with open_log(tmp_path, config=config) as log:
        log.extend(rows(400))
        log.seal()
        before = log.staging_rows()

        assert before > 0, "nothing sealed, so there is nothing to evict"

        # ROWS, not files: `advance` also compacts, so a file count falls for
        # a reason that has nothing to do with this.
        #
        # The hole. `Buffer.seed_offsets` is what a restore uses; here it
        # stands in for one, moving `next_offset` far above every sealed row.
        log._buffer.seed_offsets(log._buffer.next_offset() + (1 << 20))  # noqa: SLF001
        log.advance()

        assert log.staging_rows() == before, (
            "the reserved hole was read as a million rows and evicted the window"
        )


# -- the stranded-metadata sweep (#113) -------------------------------------


def metadata_dir(log: WriteHandle) -> Path:
    return Path(log._table.metadata_location.removeprefix("file://")).parent


def strand(log: WriteHandle, name: str, *, age: timedelta) -> Path:
    """A file in the staging table's `metadata/` that no commit recorded, as a
    commit that lost its compare-and-swap or crashed before it leaves one."""
    path = metadata_dir(log) / name
    path.write_bytes(b"stranded")
    written = (datetime.now(UTC) - age).timestamp()
    os.utime(path, (written, written))

    return path


@pytest.fixture
def sweep_now(monkeypatch: pytest.MonkeyPatch) -> None:
    """No age floor and no listing interval, so a test sees each pass act."""
    monkeypatch.setattr(maintenance, "SWEEP_MIN_AGE", timedelta(0))
    monkeypatch.setattr(maintenance, "SWEEP_INTERVAL", timedelta(0))


def test_the_sweep_deletes_stranded_metadata_and_nothing_live(
    tmp_path: Path, sweep_now: None
) -> None:
    """A stranded manifest, manifest list and `metadata.json` go; every file a
    live snapshot or the table's metadata log names stays, and the log reads.

    Falsify by dropping `live_metadata()` from the live set: the previous
    `metadata.json` versions the table keeps are deleted, and the assertion on
    them fails.
    """
    config = LogConfig(compact_min_files=99, staging_snapshot_retention=timedelta(0))
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        before = set(metadata_dir(log).iterdir())
        stranded = {
            strand(log, f"{uuid.uuid4()}-m0.avro", age=timedelta(hours=2)),
            strand(log, f"snap-1-1-{uuid.uuid4()}.avro", age=timedelta(hours=2)),
            strand(log, f"00099-{uuid.uuid4()}.metadata.json", age=timedelta(hours=2)),
        }

        log.advance()

        assert not any(p.exists() for p in stranded)
        live = {Path(p) for p in log._table.referenced_paths()} | {
            Path(p) for p in log._table.live_metadata()
        }
        assert before & live <= set(metadata_dir(log).iterdir()), (
            "a file the table still names was deleted"
        )
        assert len(before & live) > 3, "the test must hold live files to keep"
        assert len(read_all(log)) == 12


def test_the_sweep_leaves_young_files_for_a_commit_in_flight(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A commit writes its files before the swap that makes them live, so a
    young unreferenced file may be one about to be committed. The age floor
    holds even at a zero retention.

    Falsify by setting `SWEEP_MIN_AGE` to zero: the young file is deleted.
    """
    monkeypatch.setattr(maintenance, "SWEEP_INTERVAL", timedelta(0))
    config = LogConfig(compact_min_files=99, staging_snapshot_retention=timedelta(0))
    with open_log(tmp_path, config) as log:
        seal_files(log, 1)
        young = strand(log, f"{uuid.uuid4()}-m0.avro", age=timedelta(minutes=5))
        old = strand(log, f"{uuid.uuid4()}-m0.avro", age=timedelta(hours=2))

        log.advance()

        assert young.exists()
        assert not old.exists()


def test_the_sweep_works_a_backlog_down_in_batches(
    tmp_path: Path, sweep_now: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A backlog is listed once and deleted `SWEEP_BATCH` a pass, so no pass
    stalls on it; a file stranded meanwhile waits for the next listing.

    Falsify by deleting the whole list in one pass: the first pass leaves
    nothing, and the count after it fails.
    """
    monkeypatch.setattr(maintenance, "SWEEP_BATCH", 2)
    config = LogConfig(compact_min_files=99, staging_snapshot_retention=timedelta(0))
    with open_log(tmp_path, config) as log:
        seal_files(log, 1)
        backlog = [
            strand(log, f"{uuid.uuid4()}-m0.avro", age=timedelta(hours=2))
            for _ in range(5)
        ]

        log.advance()
        assert sum(p.exists() for p in backlog) == 3

        monkeypatch.setattr(maintenance, "SWEEP_INTERVAL", timedelta(hours=4))
        late = strand(log, f"{uuid.uuid4()}-m0.avro", age=timedelta(hours=2))
        log.advance()
        log.advance()

        assert not any(p.exists() for p in backlog)
        assert late.exists(), "listed again only once the interval passes"


def test_the_sweep_refuses_a_listing_that_names_files_differently(
    tmp_path: Path, sweep_now: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the listing does not contain the current metadata as the table names
    it, every comparison is meaningless and the sweep would delete the table.
    It deletes nothing instead.

    Falsify by removing the anchor check: the listing below names the live
    files as `file://` URIs, so none match `live` and every one is deleted.
    """
    # A day's retention, so expiry and drain delete nothing and every file
    # that goes would have been the sweep's doing.
    config = LogConfig(
        compact_min_files=99, staging_snapshot_retention=timedelta(days=1)
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 2)
        table = log._table
        real = table.metadata_files
        monkeypatch.setattr(
            table, "metadata_files", lambda: [(f"file://{p}", t) for p, t in real()]
        )
        before = set(metadata_dir(log).iterdir())
        old = datetime.now(UTC) - timedelta(days=2)
        for path in before:
            os.utime(path, (old.timestamp(), old.timestamp()))

        log.advance()

        assert before <= set(metadata_dir(log).iterdir())
        assert len(read_all(log)) == 8


def test_a_failing_sweep_never_fails_the_pass(
    tmp_path: Path,
    sweep_now: None,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Housekeeping: a failure is logged and the next pass lists again.

    Falsify by letting the exception out of `_sweep`: `advance` raises.
    """
    config = LogConfig(compact_min_files=99, staging_snapshot_retention=timedelta(0))
    with open_log(tmp_path, config) as log:
        seal_files(log, 1)
        stranded = strand(log, f"{uuid.uuid4()}-m0.avro", age=timedelta(hours=2))
        table = log._table
        real = table.remove

        def refuse(path: str) -> None:
            raise PermissionError(path)

        monkeypatch.setattr(table, "remove", refuse)
        log.advance()

        assert stranded.exists()
        assert "retried on the next pass" in caplog.text

        monkeypatch.setattr(table, "remove", real)
        log.advance()

        assert not stranded.exists()


def test_an_expiry_with_nothing_to_expire_commits_nothing(tmp_path: Path) -> None:
    """pyiceberg commits an empty removal — a new `metadata.json` and a catalog
    swap — when nothing is old enough. On the published table, every `publish`
    would pay a remote commit to learn nothing changed.

    Falsify by dropping the guard in `expire_snapshots_older_than`: the
    metadata location moves.
    """
    config = LogConfig(
        compact_min_files=99, staging_snapshot_retention=timedelta(days=1)
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 2)
        before = log._table.metadata_location

        log._maintenance.expire()

        assert log._table.metadata_location == before
