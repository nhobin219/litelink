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
from litelink._layout import Layout
from litelink._s3 import S3Options
from litelink._table import LogTable, _recorded_location
from litelink.log import OFFSET, table_schema
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


# Coprime with any row count used here, so the sort column is a PERMUTATION of
# arrival order rather than agreeing with it. Files are clustered by `sort_by`,
# and a test whose sort column only ever increases cannot tell that apart from
# offset order — which is the one thing the archive rewrite depends on.
STRIDE = 7919


def scrambled(count: int) -> list[dict[str, object]]:
    return [
        {"event_ts": (i * STRIDE) % count, "key": f"k{i % 7}", "payload": "x" * 96}
        for i in range(count)
    ]


def archived_log(root: Path, bucket: str, s3: S3Options, **overrides: object) -> Log:
    settings: dict[str, object] = {
        "target_seal_size": 64 * 1024,
        # Conversion off, so these tests are about what reaches the archive and
        # not about what makes a file eligible. By default compaction converts
        # sealed files into ones eight times larger and `sync` waits for that,
        # which is correct and has its own test —
        # `test_only_compacted_files_are_eligible_for_the_archive`. Leaving it
        # on here would mean every archive test first had to produce eight
        # seals' worth of rows to observe anything.
        "target_compact_size": 64 * 1024,
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
        assert held[files[-1].path] < log.config.target_seal_size, (
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
    against a larger one — which is what lowering and raising `target_seal_size`
    does to an archive, since the archive is immutable history and a size
    change applies only to what has not been written yet. The rewrite merges
    them and the data survives exactly.
    """
    with archived_log(
        tmp_path,
        bucket,
        s3,
        target_seal_size=8 * 1024,
        target_compact_size=8 * 1024,
        local_retention=timedelta(0),
    ) as log:
        log.extend(scrambled(ROWS))
        log.seal_due()
        log.sync()
        # Evicted, so the read at the end is served BY the archive. Without
        # this the local table still holds every row, the archive leg of the
        # union is bounded above by the local extent and contributes nothing,
        # and the assertions below pass without reading a rewritten file at
        # all — which is exactly what they did before this line.
        log.maintain()
        assert log.scan().read_all().num_rows < ROWS, "the read must need S3"

        remote = log._archive.require()
        before = len(remote.data_files())
        assert before >= 4, "the fixture must archive several files to merge"

        log.set_config(
            replace(
                log.config,
                target_seal_size=1024 * 1024,
                target_compact_size=1024 * 1024,
            )
        )
        log.rewrite_archive()

        remote.reload()
        after = remote.data_files()
        assert len(after) < before, "the rewrite must reduce the file count"
        assert [f.lo for f in after] == sorted(f.lo for f in after)
        assert after[0].lo == 1, "the range must still start where it did"
        assert after[-1].hi == max(f.hi for f in remote.data_files())

        restored = log.sql("SELECT * FROM log", include_archive=True).read_all()
        assert sorted(restored.column(OFFSET).to_pylist()) == list(range(1, ROWS + 1))

        # Every row still carries the offset it was written with. The rewrite
        # re-ingests through a buffer, so offsets are REASSIGNED by the counter
        # rather than copied — exact only while the rows arrive in the order
        # their offsets already have. They do not by default: a file is
        # clustered by `sort_by`, and a scan hands them back that way, which is
        # why `scrambled` puts the sort column deliberately out of step with
        # arrival order. Get it wrong and nothing raises: the offsets are still
        # 1..N with no gap, every row still holds its own data, and only the
        # pairing between them is destroyed.
        paired = restored.sort_by([(OFFSET, "ascending")])
        assert paired.column("event_ts").to_pylist() == [
            (i * STRIDE) % ROWS for i in range(ROWS)
        ], "a row was handed an offset belonging to another row"


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
        target_seal_size=8 * 1024,
        target_compact_size=8 * 1024,
        snapshot_retention=timedelta(hours=1),
    ) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()
        superseded = {f.path for f in log._archive.require().data_files()}

        log.set_config(
            replace(
                log.config,
                target_seal_size=1024 * 1024,
                target_compact_size=1024 * 1024,
            )
        )
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


def test_an_interrupted_hydrate_can_be_finished(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The hole that no later run could fill.

    Restoring upward makes the first file the new lowest local range, so a
    failure before the next one leaves the gap ABOVE what was restored. The
    next run takes its floor from that new lower bound, finds the gap is no
    longer below it, and skips it for ever — and `_union` bounds the archive
    leg by the local floor, so those offsets are then served by neither tier.

    Restoring downward, an interruption is only a range that starts higher than
    intended, and the next run continues from there.
    """
    with archived_log(tmp_path, bucket, s3, local_retention=timedelta(0)) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()
        log.maintain()
        assert log.scan().read_all().num_rows < ROWS

        archive = log._archive.require()
        real_fetch = archive.fetch
        calls = 0

        def fail_after_one(path: str, destination: Path) -> None:
            nonlocal calls
            calls += 1
            if calls > 1:
                msg = "network died mid-hydrate"
                raise OSError(msg)

            real_fetch(path, destination)

        archive.fetch = fail_after_one  # ty: ignore[invalid-assignment]
        with pytest.raises(OSError, match="mid-hydrate"):
            log.hydrate(since=timedelta(hours=1))

        archive.fetch = real_fetch  # ty: ignore[invalid-assignment]
        partial = log.sql("SELECT * FROM log").read_all().column(OFFSET).to_pylist()
        assert partial, "the first file must have been restored"
        assert sorted(partial) == list(range(min(partial), max(partial) + 1)), (
            "an interrupted hydrate must leave a contiguous local range, not a hole"
        )

        log.hydrate(since=timedelta(hours=1))

        restored = log.sql("SELECT * FROM log").read_all().column(OFFSET).to_pylist()
        assert sorted(restored) == list(range(1, ROWS + 1)), (
            "the second run must finish what the first started"
        )


def test_repointing_an_archive_reaches_the_new_one(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Re-pointing must actually re-point, and must not carry a watermark.

    The archive's catalog is a LOCAL SQLite file keyed by table id, so an entry
    made for the old prefix is found again unless it is dropped — and the
    handle then reads, and `sync` writes, into the bucket the log claims to
    have left. Worse, `_push` reconciles the watermark up from that old
    archive's extent, undoing the reset that exists to stop eviction deleting
    the only copy of rows the new archive has never been sent.
    """
    first, second = f"s3://{bucket}/one", f"s3://{bucket}/two"
    fs = filesystem(s3)
    with archived_log(tmp_path, bucket, s3) as log:
        log.set_archive(first)
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()
        assert log.archived_through() > 0
        in_first = len(fs.find(first.removeprefix("s3://")))
        assert in_first > 0

        log.set_archive(second)

        assert log.archived_through() == 0, (
            "a bucket that has been sent nothing has earned no watermark"
        )

        log.sync()

        assert second.removeprefix("s3://") in log._archive.require().metadata_location
        assert len(fs.find(second.removeprefix("s3://"))) > 0, (
            "sync must reach the NEW archive"
        )
        assert len(fs.find(first.removeprefix("s3://"))) == in_first, (
            "detaching an archive is not deleting it"
        )


def test_detaching_and_reattaching_keeps_the_archive(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Coming back to the same archive must find what is in it.

    Dropping the catalog entry the moment a log is re-pointed looked like the
    fix for reaching the wrong archive, and broke this instead: pointing back
    at an archive that still held data built a fresh empty table over it, and
    rows already evicted locally were reachable from nowhere. The entry is
    checked against the prefix when the archive is opened, so leaving and
    returning to the same one changes nothing.
    """
    # Not exactly zero: that means "evict on upload" and validation refuses to
    # detach an archive it presupposes. A microsecond evicts just as promptly
    # and leaves detaching legal, which is what this is about.
    with archived_log(
        tmp_path, bucket, s3, local_retention=timedelta(microseconds=1)
    ) as log:
        where = log.archive
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()
        log.maintain()
        assert log.scan().read_all().num_rows < ROWS, "rows must be archive-only"
        watermark = log.archived_through()

        log.set_archive(None)
        log.set_archive(where)
        log.sync()

        assert log.archived_through() == watermark, (
            "the same archive still holds the same range"
        )
        merged = log.sql("SELECT * FROM log", include_archive=True).read_all()
        assert sorted(merged.column(OFFSET).to_pylist()) == list(range(1, ROWS + 1)), (
            "every row must still be readable after a detach and reattach"
        )


def test_a_sibling_prefix_is_not_mistaken_for_this_one(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """`s3://b/one` is a string prefix of `s3://b/one-more`.

    The archive's catalog entry is checked against the prefix asked for, and a
    bare `startswith` accepts a sibling's entry as this log's — so a log
    re-pointed from `one-more` to `one` would keep reading and writing into
    `one-more`, which is a neighbour it was explicitly pointed away from.
    """
    sibling, target = f"s3://{bucket}/one-more", f"s3://{bucket}/one"
    fs = filesystem(s3)
    with archived_log(tmp_path, bucket, s3) as log:
        log.set_archive(sibling)
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()
        assert len(fs.find(sibling.removeprefix("s3://"))) > 0

        log.set_archive(target)
        log.sync()

        where = log._archive.require().metadata_location
        assert where.startswith(f"{target}/"), (
            f"a sibling prefix was mistaken for this one: {where}"
        )


def test_a_transient_failure_does_not_replace_the_archive(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """An archive that cannot be READ is not an archive that is not there.

    Opening it used to catch everything and rebuild, so a 503, a timeout or an
    expired token was taken for "no table" — and the repair dropped the only
    pointer to a live archive and wrote an empty one over it, while the
    watermark went on telling eviction those rows were safe elsewhere.

    Whether the entry belongs to this prefix is answered from the local catalog
    row, offline, so a genuine mismatch is still detected without reading the
    bucket at all. Only a failed read of OUR OWN metadata reaches here, and
    that is an error.
    """
    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()
        where = log.archive
        assert where is not None
        recorded = _recorded_location(log._layout)
        assert recorded is not None and recorded.startswith(f"{where}/")

    fs = filesystem(s3)
    before = len(fs.find(where.removeprefix("s3://")))
    assert before > 0

    unreadable = replace(s3, access_key="wrong", secret_key="wrong")
    with pytest.raises(Exception, match=r".+"):
        LogTable.open_archive(
            Layout(tmp_path, "s"), where, unreadable, table_schema(SCHEMA)
        )

    assert len(fs.find(where.removeprefix("s3://"))) == before, (
        "an unreadable archive must not be replaced"
    )
    assert _recorded_location(Layout(tmp_path, "s")) == recorded, (
        "and its catalog entry must survive"
    )


def test_an_unreadable_catalog_schema_falls_back_rather_than_rebuilds(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ "Cannot tell" must not be answered as "there is nothing there".

    The entry is located by reading pyiceberg's own catalog table directly,
    which is fast and offline and depends on a schema this library does not
    own. If that read fails — a future pyiceberg lays the rows out differently
    — answering None sends the open down the create path against an entry that
    still exists, and every open of that log then fails on a unique
    constraint. Not data loss, but a log nobody can open, from a query that was
    only ever an optimisation. It falls back to asking pyiceberg instead:
    slower, and wrong in no direction.

    The failure is injected rather than simulated by damaging the catalog,
    because damaging it breaks pyiceberg too — which then rebuilds it empty and
    makes the archive genuinely absent, testing something else entirely.
    """
    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()
        where = log.archive
        assert where is not None
        expected = _recorded_location(log._layout)
        assert expected is not None

    def unanswerable(_: Layout) -> str | None:
        msg = "cannot read the archive catalog's own table"
        raise LookupError(msg)

    monkeypatch.setattr("litelink._table._recorded_location", unanswerable)
    table = LogTable.open_archive(
        Layout(tmp_path, "s"), where, s3, table_schema(SCHEMA)
    )

    assert table.metadata_location == expected, (
        "the existing archive must be found, not replaced"
    )
