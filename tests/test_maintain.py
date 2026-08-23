"""Local storage reclamation: compact, evict, expire (SPEC §6, §8, §12)."""

from __future__ import annotations

import random
import threading
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
from pyiceberg.catalog.sql import SqlCatalog

from litelink._claim import EVERYTHING, Claim, new_owner
from litelink._maintenance import _covered, runs, stable_prefix
from litelink._table import DataFile
from litelink.log import COMPACT_MULTIPLE, Log, LogConfig, validate
from tests.test_log import SCHEMA, open_log, read_all, rows


def seal_files(log: Log, count: int, per_file: int = 4) -> None:
    """Produce `count` sealed files, each holding `per_file` rows."""
    for i in range(count):
        log.extend(rows(per_file, start=i * per_file))
        log.seal()


def test_compaction_merges_adjacent_small_files(tmp_path: Path) -> None:
    with open_log(
        tmp_path, LogConfig(target_seal_size=1 << 30, compact_min_files=2)
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
        tmp_path, LogConfig(target_seal_size=1 << 30, compact_min_files=5)
    ) as log:
        seal_files(log, 4)
        log.maintain()

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
        # that these files WOULD merge — correctly, into one archive-shaped
        # file. See `test_compaction_converts_sealed_files_into_larger_ones`.
        target_compact_size=2048,
        compact_min_files=2,
    )
    with open_log(tmp_path, config) as log:
        log.extend(rows(200))
        log.seal()
        before = len(log._table.data_files())
        assert before >= 3, "the target must be crossed several times"

        log.maintain()

        assert len(log._table.data_files()) == before


def test_compaction_output_is_re_sorted(tmp_path: Path) -> None:
    """§6 step 2: re-sorted, not merely concatenated."""
    import pyarrow.parquet as pq

    config = LogConfig(target_seal_size=1 << 30, compact_min_files=2)
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
    config = LogConfig(target_seal_size=1 << 30, compact_min_files=2)
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


def test_eviction_waits_for_the_archive_to_hold_the_file(tmp_path: Path) -> None:
    """I4, and the only line of `maintain` that is correctness (§5, §8).

    With an archive configured the local copy stops being the only one once the
    archive holds that FILE — a row `sync` writes when the copy exists, naming
    the bucket it went to (§4a). Not a watermark: one summarised the same facts
    and was the only boundary in the log that could move backwards.
    """
    config = LogConfig(local_retention=timedelta(0))
    log = Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        archive="s3://bucket/prefix",
    )
    try:
        seal_files(log, 3)
        before = log.table_files()

        assert before == 3

        # Nothing archived yet: retention says evict everything, I4 says none.
        log.maintain()

        assert log.table_files() == before, "evicted with an empty archive"

        # The archive now holds the first file, and only that one.
        first = min(log._table.data_files(), key=lambda f: f.lo)
        log._buffer.record_file(
            f"s3://bucket/prefix/data/{first.lo}.parquet", first.lo, first.hi + 1, 1
        )
        log.maintain()

        assert log.table_files() == before - 1, "did not evict what was archived"
    finally:
        log.close()


def test_reads_stay_correct_across_a_compaction(tmp_path: Path) -> None:
    """I8: once readable, a row stays readable."""
    config = LogConfig(target_seal_size=1 << 30, compact_min_files=2)
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
        rel_path = log._layout.seal_path(1, 4, "tok")
        log._buffer.claim_seal(1, 4, rel_path)

        written = tmp_path / rel_path
        written.parent.mkdir(parents=True, exist_ok=True)
        written.write_bytes(b"not really parquet, but on disk and uncommitted")

        log.maintain()

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
    config = LogConfig(compact_min_files=99, snapshot_retention=timedelta(0))
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
    config = LogConfig(compact_min_files=99, snapshot_retention=timedelta(0))
    with open_log(tmp_path, config) as log:
        seal_files(log, 1)
        live = log._table.data_files()[0]
        # Claimed as though a crashed compaction had produced it, while the
        # table in fact references it.
        log._buffer.claim_compaction(live.lo, live.hi, log._layout.relative(live.path))

    with open_log(tmp_path, config) as recovered:
        recovered._maintenance.drain()

        assert Path(live.path).exists(), "a referenced file must survive recovery"
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
        target_seal_size=1 << 30,
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
        rel_path = log._layout.seal_path(17, 20, "tok")
        log._buffer.claim_seal(17, 20, rel_path)
        log._write_and_commit(20, rel_path)
        assert_nothing_untracked(log)


def test_queued_files_are_deleted_once_the_grace_period_passes(tmp_path: Path) -> None:
    config = LogConfig(target_seal_size=1 << 30, compact_min_files=2)
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        log.maintain()
        assert len(log._buffer.queued_deletions()) == 3
        assert len(list(tmp_path.rglob("*.parquet"))) == 4

    # Reopen with a grace period short enough that the queue is due. The
    # deadline is evaluated against the CURRENT setting, not one frozen at
    # enqueue time, so lowering it takes effect on what is already queued.
    impatient = LogConfig(
        target_seal_size=1 << 30,
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
        target_seal_size=1 << 40,
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
    config = LogConfig(target_seal_size=1 << 30, compact_min_files=2)
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
        files.append(DataFile(path=path, size=1, rows=1, lo=offset, hi=offset))
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
    join it — pushing it now would archive something compaction will replace.

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
    files ahead of it cannot be archived past it either."""
    files, memory = sized(200, 200, 10, 10, 10)

    assert stable_prefix(files, 100, 2, memory) == 2


def test_a_stranded_small_file_is_still_settled() -> None:
    """The regression that made a single explicit seal block the archive
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
    memory the target allows — while `sync`, asking whether a file had reached
    half the target, found none and left the archive empty. Judged by what they
    hold, each is already full: nothing to merge, everything archivable.
    """
    target = 64 * 1024
    files, memory = sized(*([target] * 24))

    assert runs(files, target, memory) == [[f] for f in files], "each already full"
    assert stable_prefix(files, target, 2, memory) == 24


def test_eviction_outlives_the_snapshot_that_added_the_file(tmp_path: Path) -> None:
    """`local_retention` must not depend on `snapshot_retention`.

    A file's age came from the snapshot that added it, and expiry deletes that
    snapshot — after which the file was in no age map at all, `evict` could not
    classify it as stale, and it stayed on local disk for ever.

    The two settings are sized by unrelated things: §6 says `snapshot_retention`
    must exceed the longest SCAN, §8 says `local_retention` must exceed the
    longest hot LOOKBACK. So the ordinary configuration has expiry running in
    minutes and retention in days — and every file lost its age long before it
    was old enough to evict. Retention silently did nothing.
    """
    config = LogConfig(
        target_seal_size=1 << 30,
        compact_min_files=2,
        local_retention=timedelta(microseconds=1),
        snapshot_retention=timedelta(0),
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 4)
        files = log._table.data_files()
        assert len(files) == 4

        # A commit AFTER the last seal, which is what makes every remaining
        # file's adding snapshot expirable. Iceberg always keeps the current
        # snapshot, so without this the newest file stays dateable and drags
        # the rest out with it — which is why the fault only appears once a log
        # has been running a while, and never in a test that just seals.
        # Eviction itself is the commit that does it in practice.
        log._table.evict_through(files[0].hi)
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


def test_local_rows_keeps_recent_data_a_time_window_would_drop(
    tmp_path: Path,
) -> None:
    """The case a window alone cannot express.

    An hour of a quiet stream is a handful of rows. A retention window sized
    for a busy stream then evicts almost everything the moment it goes quiet,
    and the next hot read — the thing `local_retention` exists to serve — goes
    to the network for data written minutes ago.
    """
    config = LogConfig(
        target_seal_size=1 << 30,
        compact_min_files=2,
        local_retention=timedelta(microseconds=1),
        local_rows=8,
        snapshot_retention=timedelta(0),
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 4)  # 4 rows each, all older than the window
        log._maintenance.evict()

        kept = log._table.data_files()
        assert sum(f.rows for f in kept) >= 8, (
            "the row floor must hold data the window would have dropped"
        )
        assert kept[-1].hi == 16, "the newest rows are the ones kept"


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
        local_retention=timedelta(hours=1),
        # Retains almost nothing on its own.
        local_rows=1,
        snapshot_retention=timedelta(0),
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 4)
        log._maintenance.evict()

        assert len(log._table.data_files()) == 4, (
            "the window retains everything, so the row floor must not evict"
        )


def test_a_row_floor_alone_is_a_retention_policy(tmp_path: Path) -> None:
    """`local_retention=None` used to mean "never evict", full stop. With a row
    floor set it means "no limit from TIME", and the floor still applies."""
    config = LogConfig(
        target_seal_size=1 << 30,
        compact_min_files=2,
        local_retention=None,
        local_rows=4,
        snapshot_retention=timedelta(0),
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 4)
        log._maintenance.evict()

        kept = log._table.data_files()
        assert sum(f.rows for f in kept) == 4
        assert kept[-1].hi == 16


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
        snapshot_retention=timedelta(0),
    )
    with open_log(tmp_path, config) as log:
        log.extend(rows(1200))
        log.seal_due()
        sealed = log._table.data_files()
        held = log._maintenance.memory()
        assert len(sealed) >= 4, "several full seals to convert"
        assert all(held[f.path] <= 4096 * 1.5 for f in sealed), "seal-sized"

        log.maintain()

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


def test_only_compacted_files_are_eligible_for_the_archive(tmp_path: Path) -> None:
    """Eligibility falls out of the existing rule, with nothing added.

    `sync` pushes what compaction has finished with. Raise the compaction
    target above the seal size and a freshly sealed file is a merge candidate
    by definition — so it is not settled, and not archived, until it has been
    converted. No separate eligibility flag, and no way for the two to disagree
    about which files are still in play.
    """
    config = LogConfig(
        target_seal_size=4096,
        target_compact_size=8 * 4096,
        compact_min_files=2,
        snapshot_retention=timedelta(0),
    )
    with open_log(tmp_path, config) as log:
        log.extend(rows(200))
        log.seal_due()
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
            "sealed files are merge candidates, so none may be archived yet"
        )


def test_the_compaction_target_defaults_to_a_multiple_of_the_seal(
    tmp_path: Path,
) -> None:
    """Conversion is on by default, including with no archive.

    File count is a measured read cost here rather than a reputation: reading
    the offset boundary from manifest statistics measured 1.0 ms over one file
    and 44 ms over 64. A local-only log gets that benefit too, which is why the
    default is a multiple rather than "same as the seal, convert nothing".
    """
    config = LogConfig(target_seal_size=4096, compact_min_files=2)

    assert config.compact_size == 4096 * COMPACT_MULTIPLE

    with open_log(tmp_path, replace(config, snapshot_retention=timedelta(0))) as log:
        log.extend(rows(1200))
        log.seal_due()
        before = len(log._table.data_files())
        assert before >= 4

        log.maintain()

        assert len(log._table.data_files()) < before, (
            "the conversion must run without an archive configured"
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
    """`maintain` is one call for all three; the parts are callable alone.

    Their costs differ by orders of magnitude now that conversion reads and
    rewrites whole files while eviction and expiry are metadata commits, so a
    deployment may want them on different schedules. Running them separately
    has to reach the same state as running them together.
    """
    config = LogConfig(
        target_seal_size=4096,
        target_compact_size=8 * 4096,
        compact_min_files=2,
        local_retention=timedelta(microseconds=1),
        snapshot_retention=timedelta(0),
    )
    with open_log(tmp_path, config) as log:
        log.extend(rows(1200))
        log.seal_due()
        before = len(log._table.data_files())
        assert before >= 4

        log.compact()
        converted = len(log._table.data_files())
        assert converted < before, "compaction must run on its own"

        log.evict()
        log.expire()

        assert log._table.data_files() == [], "eviction must run on its own"
        # What is left is the unsealed tail, which never reached the seal
        # target and is therefore still in the buffer. Eviction removes FILES;
        # rows that are not in one are not its business.
        assert log.scan().read_all().num_rows == log.buffered_rows()


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
        files = sorted(log._table.data_files(), key=lambda f: f.lo)

        assert len(files) >= 4

        # One owner is working on the bottom of the log.
        low = log._buffer.claim("compact", files[0].lo, files[1].hi, "other-owner")

        assert low.acquire()

        try:
            # A second owner claims the top, and is not refused.
            high = log._buffer.claim("compact", files[2].lo, files[-1].hi, "mine")

            assert high.acquire(), "disjoint ranges must not exclude each other"

            high.release()

            # Overlapping, and it is.
            clash = log._buffer.claim("compact", files[1].lo, files[2].hi, "mine")

            assert not clash.acquire(), "overlapping ranges must exclude"
        finally:
            low.release()


def test_eviction_only_ever_removes_whole_files(tmp_path: Path) -> None:
    """A row floor lands mid-file; the boundary must not.

    `evict_through` filters by ROW, so a boundary inside a file makes pyiceberg
    rewrite it copy-on-write — and the replacement lands at a path this library
    never learns, which breaks the rule every reclamation rests on: a file's
    path is in SQLite before the file exists (I2). The superseded original is
    left out of the deletion queue too, so once expiry drops the snapshots
    naming it, nothing can name it again. `drain` is a keyed read and this
    design refuses directory scans, so it is unreclaimable for good.

    The age limit is already file-aligned — it is some file's `hi` — and so is
    the archive clamp. Only the row floor is arbitrary, which is why it arrived
    with `local_rows`.
    """
    config = LogConfig(
        target_seal_size=1 << 30,
        target_compact_size=1 << 30,
        compact_min_files=2,
        local_retention=timedelta(microseconds=1),
        # Deliberately not a multiple of the 4 rows each sealed file holds, so
        # the raw boundary falls inside one.
        local_rows=6,
        snapshot_retention=timedelta(0),
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


def test_compaction_will_not_merge_a_file_the_archive_holds(tmp_path: Path) -> None:
    """A merge spanning the archive's extent is a duplicate that cannot be undone.

    Its inputs would include files already pushed, so the merged file covers a
    range partially overlapping one the archive holds — and `register` declines
    only a range that is ENTIRELY covered, so the partial one is admitted and
    the same offsets sit in two archive files for ever.

    Compaction therefore skips a file the archive holds, asked per file (§4a).
    That is also what keeps the two tiers' ranges aligned: a file the archive
    holds is never rewritten locally, so the ranges stay comparable at all.
    """
    config = LogConfig(
        target_seal_size=4096,
        target_compact_size=8 * 4096,
        compact_min_files=2,
        snapshot_retention=timedelta(0),
    )
    log = Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        archive="s3://bucket/prefix",
    )
    try:
        log.extend(rows(1200))
        log.seal_due()
        files = sorted(log._table.data_files(), key=lambda f: f.lo)

        assert len(files) >= 4

        # The archive holds the first two.
        for data_file in files[:2]:
            log._buffer.record_file(
                f"s3://bucket/prefix/data/{data_file.lo}.parquet",
                data_file.lo,
                data_file.hi + 1,
                1,
            )

        boundary = files[1].hi
        log.compact()

        merged = log._table.data_files()

        assert all(f.lo > boundary or f.hi <= boundary for f in merged), (
            "no file may span the archive's extent, or the archive gets it twice"
        )
        assert log.scan().read_all().num_rows == 1200
    finally:
        log.close()


def test_repointing_does_not_move_any_boundary_backwards(tmp_path: Path) -> None:
    """A re-point changes where the NEXT file goes, and nothing else (§4a).

    The frontier this replaces had to be reset, because it named ranges of the
    archive being left — and that reset was the only backwards boundary move in
    the log, which is what made every reader that had cached the old position
    wrong at once. Per segment there is nothing to reset: files already pushed
    keep naming the bucket that holds them.
    """
    log = Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        archive="s3://bucket/prefix",
    )
    with log:
        seal_files(log, 2)
        first = min(log._table.data_files(), key=lambda f: f.lo)
        log._buffer.record_file(
            f"s3://bucket/prefix/data/{first.lo}.parquet", first.lo, first.hi + 1, 1
        )
        local = log._table.data_files()

        assert log._maintenance.archived_prefix(local, log._archive.uri) == first.hi

        log.set_archive("s3://bucket/elsewhere")

        assert log._maintenance.archived_prefix(local, log._archive.uri) == 0, (
            "the new archive holds nothing, and says so without any reset"
        )

        log.set_archive("s3://bucket/prefix")

        assert log._maintenance.archived_prefix(local, log._archive.uri) == first.hi, (
            "pointing back finds the copies still recorded where they are"
        )


def test_rewriting_the_archive_does_not_strand_local_eviction(tmp_path: Path) -> None:
    """The two tiers cut the same rows independently, and I4 must not care.

    `rewrite_archive` re-cuts the archive to different boundaries — that is its
    whole job. Asking whether a local file's range EQUALS an archived one then
    failed for every local file, permanently: eviction clamped to zero and
    stopped, and compaction stopped treating archived files as the archive's
    business and merged across its extent. Neither heals, because nothing ever
    re-cuts the archive back.
    """
    log = Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        archive="s3://bucket/prefix",
    )
    with log:
        seal_files(log, 3, per_file=4)
        files = sorted(log._table.data_files(), key=lambda f: f.lo)

        assert len(files) == 3

        # The archive holds every row of all three, cut its own way: two files
        # whose boundaries line up with none of the local ones.
        lo, hi = files[0].lo, files[-1].hi
        middle = files[1].lo + 1
        log._buffer.record_file("s3://bucket/prefix/data/a.parquet", lo, middle, 1)
        log._buffer.record_file("s3://bucket/prefix/data/b.parquet", middle, hi + 1, 1)

        assert log._maintenance.archived_prefix(files, log._archive.uri) == hi, (
            "the archive holds every row; how it cut them is not I4's business"
        )


def test_a_gap_in_the_archive_stops_the_walk(tmp_path: Path) -> None:
    """Coverage must join adjacent files without inventing rows between them."""
    log = Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        archive="s3://bucket/prefix",
    )
    with log:
        seal_files(log, 3, per_file=4)
        files = sorted(log._table.data_files(), key=lambda f: f.lo)

        # The first file, then a hole, then the third.
        log._buffer.record_file(
            "s3://bucket/prefix/data/a.parquet", files[0].lo, files[0].hi + 1, 1
        )
        log._buffer.record_file(
            "s3://bucket/prefix/data/c.parquet", files[2].lo, files[2].hi + 1, 1
        )

        assert (
            log._maintenance.archived_prefix(files, log._archive.uri) == files[0].hi
        ), "a range the archive does not hold must stop the walk"


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
        run = sorted(log._table.data_files(), key=lambda f: f.lo)
        rows_before = len(read_all(log))

        assert len(run) == 3

        # Eviction happened after this run was chosen and before the merge
        # takes its claim.
        log._table.evict_through(run[0].hi)
        remaining = len(read_all(log))

        assert remaining < rows_before, "the setup must actually evict"

        log._maintenance._rewrite_run(log._table, run, None)

        assert len(read_all(log)) == remaining, (
            "the merge put back rows eviction had removed"
        )


def test_the_coverage_walk_agrees_with_the_offsets_it_stands_for() -> None:
    """`_covered` is an optimisation of a set membership test; prove it is one.

    I4 asks whether the archive holds a local file's rows. The honest way to
    answer is to build the set of offsets the archive holds and test the file's
    against it, which is unaffordable; the walk is what makes it affordable, so
    it has to give the same answer for every shape — overlapping archived
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
    config = LogConfig(local_rows=1, target_seal_size=1 << 30)
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        before = log.table_files()

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
                self.connection, self.lock, "compact", self.lo, self.hi, new_owner()
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

        assert log.table_files() == before, "committed without holding the claim"


def test_a_caller_heartbeat_does_not_switch_off_the_run_claim(tmp_path: Path) -> None:
    """`heartbeat or claim.renew` read naturally and was wrong.

    Any caller passing a heartbeat — which is what the role-lease era asked
    for, so it is a habit carried forward — stopped the run claim from being
    renewed at all, and the pre-commit check then consulted a stranger's
    callback instead of the claim. A merge over the TTL lost its exclusion with
    no stall required.
    """
    config = LogConfig(target_seal_size=1 << 30, compact_min_files=2)
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        renewed: list[int] = []
        original = Claim.renew

        def counting(self: Claim) -> bool:
            renewed.append(self.row_id or 0)

            return original(self)

        Claim.renew = counting
        try:
            log.compact(heartbeat=lambda: True)

        finally:
            Claim.renew = original

        assert renewed, "the run claim was never renewed while a merge ran"


def test_drain_will_not_unlink_while_another_owner_holds_the_log(
    tmp_path: Path,
) -> None:
    """The unlink is not metadata, so it declares like everything else (§4a).

    Expiry is safe claimless — a metadata commit that CAS orders, idempotent.
    The deletion that follows it is not. Consulting the table without declaring
    anything leaves the window every other pass here was built to close:
    `hydrate` re-registers a file under the very name the queue still holds,
    deliberately reusing the archived key, and can commit that between the veto
    being read and the file being unlinked. The local table then references a
    file that is not there.
    """
    config = LogConfig(
        target_seal_size=1 << 30,
        compact_min_files=2,
        snapshot_retention=timedelta(0),
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        log.compact()

        queued = log._buffer.due_deletions(2**62)

        assert queued, "expected superseded files awaiting deletion"

        other = Claim(
            log._buffer._con, log._buffer._lock, "maintain", 0, EVERYTHING, new_owner()
        )

        assert other.acquire()

        try:
            log.expire()

            # Expiry queues MORE as it goes, so what matters is that the
            # entries already due are still due: nothing was unlinked.
            assert set(queued) <= set(log._buffer.due_deletions(2**62)), (
                "unlinked while another owner held the log"
            )
        finally:
            other.release()

        log.expire()

        assert not set(queued) & set(log._buffer.due_deletions(2**62)), (
            "never drained once the log was free"
        )


def test_drain_stops_if_it_loses_the_log_mid_sweep(tmp_path: Path) -> None:
    """The unlink is this pass's commit, so the claim is asked again at it.

    Everything slow in a drain sits between the veto being read and the
    deletions — opening the archive, walking its manifests, one remote round
    trip per queued object. Past the TTL another owner may lawfully take the
    whole log, `hydrate` a file under the very name still queued here, and
    release; a drain holding a dead claim would then unlink it against a stale
    veto and leave the local table pointing at a file that is not there.
    """
    config = LogConfig(
        target_seal_size=1 << 30,
        compact_min_files=2,
        snapshot_retention=timedelta(0),
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        log.compact()
        queued = log._buffer.due_deletions(2**62)

        assert queued, "expected superseded files awaiting deletion"

        # The claim is taken and then lost, the way a slow remote leg loses it.
        original = Claim.acquire
        stolen: list[Claim] = []

        def losing(self: Claim) -> bool:
            if not original(self):
                return False

            if self.kind != "drain":
                return True

            with self.lock:
                self.connection.execute(
                    "UPDATE claim SET expires_at = 1 WHERE id = ?", (self.row_id,)
                )

            rival = Claim(
                self.connection, self.lock, "maintain", 0, EVERYTHING, new_owner()
            )
            assert original(rival)
            stolen.append(rival)

            return True

        Claim.acquire = losing
        try:
            with pytest.raises(RuntimeError, match="lost the"):
                log.expire()

        finally:
            Claim.acquire = original
            for rival in stolen:
                rival.release()

        assert set(queued) <= set(log._buffer.due_deletions(2**62)), (
            "deleted while another owner held the log"
        )


def test_eviction_reads_the_archive_under_its_own_claim(tmp_path: Path) -> None:
    """Everything that decides a deletion is read under the claim, or it is a
    statement about the past.

    `sync` learned this for itself — under the claim, not before it — and
    eviction acts on the same fact. The window is not narrow: `set_archive` is
    documented as something the shipped writer calls on every restart, and it
    takes the whole log, which is free precisely while eviction holds nothing.
    Attaching an archive between the read and the acquire left eviction
    deleting the only copy of every aged row the new archive was configured to
    receive — and sync can never push them afterwards, because they have left
    the table.
    """
    config = LogConfig(local_rows=1, target_seal_size=1 << 30)
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        before = log.table_files()

        assert before == 3
        assert not log._archive.configured()

        # The archive is attached between eviction's read and its claim.
        original = Claim.acquire

        def attaching(self: Claim) -> bool:
            if self.kind == "evict":
                log._buffer.set_meta("archive", "s3://bucket/prefix")

            return original(self)

        Claim.acquire = attaching
        try:
            log.evict()

        finally:
            Claim.acquire = original

        assert log.table_files() == before, (
            "deleted the only copy of rows an archive had just been configured for"
        )


def test_eviction_reads_the_policy_the_log_records(tmp_path: Path) -> None:
    """`set_config` writes durable state; a maintainer has to hear about it.

    The same reasoning the archive location already earned, applied to the
    settings beside it. Eviction is where it shows: it decides deletions from
    `local_retention` and `local_rows`, so a process holding the copy it read
    at open goes on deleting the only copy of rows the durable policy now says
    to keep — and §8 reads as an obligation, not a hint.
    """
    with open_log(tmp_path, LogConfig(local_rows=1, target_seal_size=1 << 30)) as log:
        seal_files(log, 3)
        before = log.table_files()

        assert before == 3

        # Another process raises the floor to cover everything.
        log._buffer.set_meta("config", LogConfig(local_rows=10_000).to_json())
        log.evict()

        assert log.table_files() == before, (
            "evicted against the policy this process happened to read at open"
        )


def test_a_refreshed_policy_reaches_compaction_sync_and_the_buffer(
    tmp_path: Path,
) -> None:
    """One owner, or compaction and sync can disagree about what is in play.

    `Log` used to keep its own copy of the policy beside `Maintenance`'s, kept
    in step by `set_config` writing both. Refreshing only one of them left
    compaction reading the new policy while `sync` read the old — and `runs`
    exists precisely so those two cannot disagree, because a file `sync`
    settles under one grouping and compaction merges under another leaves the
    archive holding rows rewritten underneath it. The buffer's seal target is
    the third copy, and a stale one sizes every file the log writes.
    """
    with open_log(tmp_path, LogConfig(local_rows=1, target_seal_size=4096)) as log:
        seal_files(log, 2)
        raised = LogConfig(
            local_rows=10_000, target_seal_size=1 << 20, target_compact_size=1 << 23
        )
        log._buffer.set_meta("config", raised.to_json())

        log.evict()

        assert log.config.target_compact_size == raised.target_compact_size, (
            "sync reads the policy through Log; it must be the refreshed one"
        )
        assert log._maintenance.config.local_rows == raised.local_rows
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
    and `sync` share so they cannot disagree about what is in play — and per
    segment I4 closes that: a file the archive holds is never merged again, so
    a disagreement costs an undersized archive file, which `_push` already
    documents as tolerated.
    """
    config = LogConfig(target_seal_size=4096, compact_min_files=2, local_rows=200)
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
                            local_rows=None if churned % 2 else 200,
                            local_retention=None
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
                log.seal()
                try:
                    log.maintain()
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
    each other: `local_rows` seen as an int by the guard and as None by the
    subtraction after it is `int - None`, a TypeError out of `maintain()`. The
    shipped maintainer catches RuntimeError and CommitFailedException, so that
    stopped maintenance entirely.

    Counting the reads tests that directly. Staging the crash instead means
    engineering an exact ordering, which is a test of the harness rather than
    of the code — the first attempt at this passed against the broken version
    because the values happened to line up harmlessly.
    """
    config = LogConfig(target_seal_size=1 << 30, local_rows=200, local_retention=None)
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


def test_the_archived_prefix_is_always_a_file_boundary(tmp_path: Path) -> None:
    """What `_push`'s arithmetic rests on.

    It splits `pending` at `archived_prefix` and counts the part below as
    settled, which is only a prefix count if the split lands on a file edge —
    otherwise `pending[:settled]` names a different set than the one the split
    described, and the watermark is written from the wrong file.

    Files are ordered by offset and the walk stops at the first file the
    archive does not fully hold, so the answer is either 0 or some file's `hi`,
    and everything at or below it is a prefix. Asserted over random coverage
    rather than argued.
    """
    random.seed(20260823)
    log = Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        archive="s3://bucket/prefix",
    )
    with log:
        seal_files(log, 5, per_file=4)
        files = sorted(log._table.data_files(), key=lambda f: f.lo)

        assert len(files) == 5

        for trial in range(40):
            with log._buffer._lock:
                log._buffer._con.execute(
                    "DELETE FROM extent WHERE rel_path LIKE 's3://%'"
                )
                log._buffer._con.commit()

            # A random subset of the files gets an archive copy.
            for index, data_file in enumerate(files):
                if random.random() < 0.6:
                    log._buffer.record_file(
                        f"s3://bucket/prefix/data/{trial}-{index}.parquet",
                        data_file.lo,
                        data_file.hi + 1,
                        1,
                    )

            frozen = log._maintenance.archived_prefix(files, "s3://bucket/prefix")
            below = [f for f in files if f.lo <= frozen]
            above = [f for f in files if f.lo > frozen]

            assert frozen == 0 or frozen in {f.hi for f in files}, (
                f"{frozen} is not a file boundary"
            )
            assert files[: len(below)] == below, "the split is not a prefix"
            assert files[len(below) :] == above, "the remainder is not a suffix"
