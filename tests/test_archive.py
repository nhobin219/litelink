"""The remote tier, against a real S3-compatible endpoint (§5).

Skipped unless one is reachable — `just rustfs` brings one up locally, and the
same tests pass against AWS by pointing `AWS_ENDPOINT_URL` elsewhere. Nothing
here is mocked: the point is the parts a fake cannot exercise, which is most of
them. pyiceberg writing a catalog over object storage, `add_files` registering
by S3 URI, DuckDB reading that table back through httpfs, and the union of
three tiers that only means anything when one of them is genuinely remote.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pyarrow as pa
import pytest

from litelink import Log, LogConfig
from litelink._s3 import S3Options
from litelink.log import OFFSET
from tests.conftest import filesystem

pytestmark = pytest.mark.s3

SCHEMA = pa.schema(
    [
        pa.field("event_ts", pa.int64()),
        pa.field("key", pa.string()),
        pa.field("payload", pa.string()),
    ]
)

# Enough that a 64 KiB target seals many files rather than one, so the merged
# read has real boundaries to get wrong.
ROWS = 4000


def rows(count: int) -> list[dict[str, object]]:
    return [
        {"event_ts": i, "key": f"k{i % 7}", "payload": "x" * 96} for i in range(count)
    ]


def archived_log(root: Path, bucket: str, s3: S3Options, **overrides: object) -> Log:
    settings: dict[str, object] = {
        "target_size": 64 * 1024,
        "compact_min_files": 2,
        "snapshot_retention": timedelta(seconds=0),
    }
    settings.update(overrides)
    config = LogConfig(**settings)  # ty: ignore[invalid-argument-type]

    return Log.new(
        root,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        archive=f"s3://{bucket}/prefix",
        s3=s3,
    )


def test_sync_pushes_sealed_files_and_records_the_watermark(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """§5 steps 1-3: upload, register, record — and the watermark is what I4
    later reads to decide what may be evicted."""
    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        sealed = log._table.data_files()
        assert sealed, "the fixture must produce sealed files to push"

        log.sync()

        remote = log._archive.require()
        assert remote.extent() == (1, sealed[-1].hi), "the archive must cover them"
        assert int(log._buffer.get_meta("archive_through") or 0) == sealed[-1].hi


def test_a_read_spans_archive_local_and_buffer(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The union that is the whole point: rows evicted from local disk are
    still readable, exactly once, alongside rows that never left.

    `local_retention=0` evicts everything the archive holds as soon as it holds
    it, so by the end the only local rows are the unsealed tail — and a merged
    read must still return the full stream with no gap at either seam.
    """
    with archived_log(tmp_path, bucket, s3, local_retention=timedelta(0)) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        before = log.table_files()
        log.sync()
        log.maintain()

        assert log.table_files() < before, "eviction must have removed local files"
        local = log.scan().read_all()
        assert local.num_rows < ROWS, "the local tier must no longer hold everything"

        merged = log.sql("SELECT * FROM log", include_archive=True).read_all()
        offsets = merged.column(OFFSET).to_pylist()
        assert sorted(offsets) == list(range(1, ROWS + 1)), (
            "every offset exactly once, no duplicate across tiers and no gap"
        )


def test_a_hot_read_never_touches_the_archive(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """I5, asserted rather than assumed.

    The default is local disk only, so a log whose archive is unreachable must
    still serve `scan()`. Pointing the credentials at a dead endpoint after the
    push is what makes that falsifiable: if the read path opened the archive
    regardless of the flag, this would raise instead of returning rows.
    """
    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()

        log._archive._s3 = S3Options(
            endpoint="http://127.0.0.1:1",
            access_key="nobody",
            secret_key="nothing",
            region="us-east-1",
        )
        log._archive._handle = None

        assert log.scan().read_all().num_rows == ROWS

        with pytest.raises(Exception, match=r".+"):
            log.sql("SELECT * FROM log", include_archive=True).read_all()


def test_only_settled_files_reach_the_archive(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The rule that keeps the archive well-sized by construction.

    An explicit `seal()` cuts wherever the buffer happens to be, so it can emit
    a file holding far less than the target. Pushed, it would sit in object
    storage as an undersized file nothing local can merge away — the archive
    would need a repair pass to fix a sizing decision made here. So a trailing
    small file stays behind, and the watermark stops short of it.

    "Small" is measured in what the file HOLDS, not what it cost to store. The
    payload here compresses about eight to one, so every file looks starved
    beside a target stated in uncompressed bytes, and a rule reading sizes off
    disk pushed nothing at all.
    """
    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.extend(rows(4))
        log.seal()

        files = log._table.data_files()
        held = log._maintenance.memory()
        assert held[files[-1].path] < log.config.target_size, (
            "the tail must hold less than a full target to test this"
        )

        log.sync()

        assert int(log._buffer.get_meta("archive_through") or 0) == files[-2].hi
        assert log._archive.require().extent() == (1, files[-2].hi)


def test_hydrate_brings_evicted_files_back_to_local_disk(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """§8: raising `local_retention` is an operation, not a config change.

    Everything is evicted first, so the local table holds only the unsealed
    tail and a plain `scan()` cannot see the rest. After hydrating, the same
    read — no `include_archive`, no network — returns the whole stream, which
    is the point: the data is back on local disk, not merely reachable.
    """
    with archived_log(tmp_path, bucket, s3, local_retention=timedelta(0)) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()
        log.maintain()

        evicted = log.scan().read_all().num_rows
        assert evicted < ROWS, "the fixture must evict something to test this"

        log.hydrate(since=timedelta(hours=1))

        assert log.scan().read_all().num_rows == ROWS
        restored = log.sql("SELECT * FROM log").read_all().column(OFFSET).to_pylist()
        assert sorted(restored) == list(range(1, ROWS + 1)), (
            "every offset exactly once, with no overlap against what stayed local"
        )


def test_hydrate_is_idempotent(tmp_path: Path, bucket: str, s3: S3Options) -> None:
    """Run twice and nothing doubles.

    Files land under the name they have in the archive, so the second pass
    rewrites the same paths, and the range filter refuses anything the local
    table already holds. Without that filter the second run would register the
    archive's copy of a range alongside the copy it just restored, and every
    row in it would be read twice.
    """
    with archived_log(tmp_path, bucket, s3, local_retention=timedelta(0)) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()
        log.maintain()

        log.hydrate(since=timedelta(hours=1))
        once = log.table_files()
        log.hydrate(since=timedelta(hours=1))

        assert log.table_files() == once
        assert log.scan().read_all().num_rows == ROWS


def test_hydrate_ignores_files_older_than_the_window(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A zero window restores nothing, which is what makes the window real."""
    with archived_log(tmp_path, bucket, s3, local_retention=timedelta(0)) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()
        log.maintain()
        evicted = log.table_files()

        log.hydrate(since=timedelta(0))

        assert log.table_files() == evicted


def test_rewrite_archive_merges_files_left_undersized(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The repair, on the layout that needs repairing.

    Every file here is pushed under a small target and is then undersized
    against a larger one — which is what lowering and raising `target_size`
    does to an archive, since the archive is immutable history and a size
    change applies only to what has not been written yet. The rewrite merges
    them and the data survives exactly.
    """
    with archived_log(tmp_path, bucket, s3, target_size=8 * 1024) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()

        remote = log._archive.require()
        before = len(remote.data_files())
        assert before >= 4, "the fixture must archive several files to merge"

        log.set_config(replace(log.config, target_size=1024 * 1024))
        log.rewrite_archive()

        remote.reload()
        after = remote.data_files()
        assert len(after) < before, "the rewrite must reduce the file count"
        assert [f.lo for f in after] == sorted(f.lo for f in after)
        assert after[0].lo == 1, "the range must still start where it did"
        assert after[-1].hi == max(f.hi for f in remote.data_files())

        restored = log.sql("SELECT * FROM log", include_archive=True).read_all()
        assert sorted(restored.column(OFFSET).to_pylist()) == list(range(1, ROWS + 1))


def test_rewrite_archive_defers_deleting_what_it_superseded(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """I6 reaches across the network.

    A reader that resolved the archive before the rewrite is still reading
    those objects, so they go through the same queue and the same grace period
    a local compaction's sources do. Deleting them at commit time would break
    a scan already in flight — and object storage has no equivalent of a POSIX
    unlink that leaves an open handle working.
    """
    with archived_log(
        tmp_path,
        bucket,
        s3,
        target_size=8 * 1024,
        snapshot_retention=timedelta(hours=1),
    ) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()
        superseded = {f.path for f in log._archive.require().data_files()}

        log.set_config(replace(log.config, target_size=1024 * 1024))
        log.rewrite_archive()

        queued = set(log._buffer.queued_deletions())
        assert superseded & queued, "the sources must be queued, not deleted"

        fs = filesystem(s3)
        for path in superseded & queued:
            assert fs.exists(path.removeprefix("s3://")), (
                "a queued file must still exist until its grace period passes"
            )

        log.set_config(replace(log.config, snapshot_retention=timedelta(0)))
        log.maintain()

        for path in superseded & queued:
            assert not fs.exists(path.removeprefix("s3://")), (
                "the drain must remove remote files once they come due"
            )
