"""Local storage reclamation: compact, evict, expire (SPEC §6, §8, §12)."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
from pyiceberg.catalog.sql import SqlCatalog

from litelink._maintenance import runs, stable_prefix
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


def test_eviction_waits_for_the_archive_watermark(tmp_path: Path) -> None:
    """I4, and the only line of `maintain` that is correctness (§5, §8).

    With an archive configured the local copy stops being the only one only
    once sync says so. `maintain` used to refuse outright rather than risk it;
    now it clamps the eviction boundary to what sync has recorded, so a sync
    arbitrarily far behind delays eviction instead of losing data.

    No object store needed: the watermark is a `meta` row, and what is under
    test is that eviction reads it.
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

        # Sync has reached the first file only.
        first = min(f.hi for f in log._table.data_files())
        log._buffer.set_meta("archive_through", str(first))
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
