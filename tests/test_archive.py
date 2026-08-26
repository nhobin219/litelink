"""The remote tier, against a real S3-compatible endpoint (§5).

Skipped unless one is reachable — `just rustfs` brings one up locally, and the
same tests pass against AWS by pointing `AWS_ENDPOINT_URL` elsewhere. Nothing
here is mocked: the point is the parts a fake cannot exercise, which is most of
them. pyiceberg writing a catalog over object storage, `add_files` registering
by S3 URI, DuckDB reading that table back through httpfs, and the union of
three tiers that only means anything when one of them is genuinely remote.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest

from litelink import Log, LogConfig
from litelink._buffer import Buffer
from litelink._layout import NAMESPACE, Layout
from litelink._maintenance import Maintenance
from litelink._read import load_extension, secret_sql
from litelink._s3 import S3Options
from litelink._table import (
    VERSION_HINT,
    LogTable,
    _recorded_location,
    forget_archive_entry,
)
from litelink.log import OFFSET, RESTORE_RESERVE, table_schema
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


def test_a_read_never_repairs_the_archive_catalog(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Dropping and recreating a catalog entry is a write, and reads do not
    hold the lease that makes it safe.

    Two processes cold-opening after a re-point would both find the mismatch;
    the second's drop can land after the first has already created, uploaded
    and committed, taking the live entry with it and leaving an empty table
    over pushed files. So only a caller holding the maintenance lease may
    repair, and a reader that finds a mismatch says so.
    """
    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()
        first = log.archive
        assert first is not None

    # The entry now names `first`, while the log is pointed at `second`.
    # Written straight to `meta` rather than through `set_archive`, so nothing
    # has had the chance to repair the entry before the read sees it.
    second = f"s3://{bucket}/elsewhere"
    with Log.open(tmp_path, "s", s3=s3) as writer:
        writer._buffer.set_meta("archive", second)

    with Log.open(tmp_path, "s", read_only=True, s3=s3) as reader:
        with pytest.raises(ValueError, match="not under"):
            reader._archive.table()

    assert _recorded_location(Layout(tmp_path, "s")) is not None, (
        "a read must not have dropped the entry"
    )


def test_a_read_before_the_first_sync_simply_has_no_archive_leg(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Absent is not wrong. A log configured with an archive nothing has pushed
    to yet reads without that leg, and creates nothing by reading."""
    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(200))
        log.seal_due()

        merged = log.sql("SELECT * FROM log", include_archive=True).read_all()

        assert merged.num_rows == 200
        assert _recorded_location(log._layout) is None, (
            "reading must not create the archive table"
        )


def test_a_repoint_leaves_reads_working(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Re-pointing is routine, so it must not break reading until a maintainer
    happens to run.

    The catalog entry still names the previous archive until something replaces
    it, and only a lease holder may — so `set_archive` does it, under the
    lease, rather than leaving every cross-tier read raising in the meantime.
    The check at open stays as the repair for what this misses: another owner
    holding the lease, or a crash between the two durable writes.
    """
    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()

        log.set_archive(f"s3://{bucket}/moved")

    with Log.open(tmp_path, "s", read_only=True, s3=s3) as reader:
        merged = reader.sql("SELECT * FROM log", include_archive=True).read_all()

        assert merged.num_rows == ROWS, "a re-pointed log must still be readable"


def test_repointing_cannot_interleave_with_a_sync(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A sync that has already pushed finishes by writing the watermark.

    Land a re-point between those two moments and the log points at the new,
    empty archive while carrying a watermark earned by the old one — eviction
    believes it and deletes the only local copy of rows the new archive has
    never been sent. Nothing lowers a watermark, so no later sync undoes it.

    Re-reading the location at the top of `sync` narrows that window; only the
    lease closes it. Held here by a stand-in for the maintainer.
    """
    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(200))
        log.seal_due()

        # Short, so the bounded wait does not make this test wait it out.
        log._settings_wait = 0.2  # ty: ignore[unresolved-attribute]
        held = log._lease("maintain")
        assert held.acquire()
        try:
            with pytest.raises(RuntimeError, match="has held a claim"):
                log.set_archive(f"s3://{bucket}/elsewhere")
        finally:
            held.release()

        # And the refusal changed nothing: the archive is still the old one.
        assert log.archive is not None
        assert log.archive.endswith("/prefix")

        log.set_archive(f"s3://{bucket}/elsewhere")
        assert log.archive.endswith("/elsewhere")


def test_a_read_against_a_never_synced_archive_writes_nothing(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """ "Creates nothing" has to mean locally too.

    Constructing the catalog creates its tables in `archive.db`, and
    registering the namespace adds a row — writes, from a path that promised to
    make none. So absence is decided from the catalog file before one is built,
    and a reader against an archive nothing has pushed to touches nothing.
    """
    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(200))
        log.seal_due()
        assert not log._layout.archive_db.exists()

        merged = log.sql("SELECT * FROM log", include_archive=True).read_all()

        assert merged.num_rows == 200
        assert not log._layout.archive_db.exists(), (
            "a read must not create the archive catalog"
        )


def test_a_failed_repoint_puts_the_old_catalog_entry_back(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A half-done move that leaves NEITHER archive is worse than not moving.

    The repair drops the entry and creates a table at the new prefix, and
    creating can fail — configuring an archive deliberately does not require
    the bucket to exist yet. A drop with no create destroys the only record of
    where the previous archive's metadata is, and
    `previous_metadata_location` dies with the row, so rolling back would build
    an empty table over data nothing could then reach.
    """
    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()
        original = _recorded_location(log._layout)
        assert original is not None

    # A prefix in a bucket that does not exist, so the create must fail.
    missing = "s3://litelink-no-such-bucket-3f9a2/elsewhere"
    with pytest.raises(Exception, match=r".+"):
        LogTable.open_archive(
            Layout(tmp_path, "s"), missing, s3, table_schema(SCHEMA), repair=True
        )

    assert _recorded_location(Layout(tmp_path, "s")) == original, (
        "a failed repair must leave the previous archive reachable"
    )


def test_drain_never_deletes_from_an_archive_the_log_has_left(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A queued remote path names the archive it was superseded in.

    The reference veto asks the archive the log is pointed at NOW, so after a
    re-point those entries would be checked against a new archive that
    references nothing, and deleted from the old bucket — where they may still
    be live, and may be the only copy of rows already evicted locally.
    """
    old = f"s3://{bucket}/retired"
    fs = filesystem(s3)
    with archived_log(tmp_path, bucket, s3, snapshot_retention=timedelta(0)) as log:
        log.set_archive(old)
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()
        objects = fs.find(old.removeprefix("s3://"))
        assert objects

        # Queue one of the old archive's live objects, as an interrupted
        # rewrite would have, then leave for a different archive.
        stranded = f"s3://{objects[0]}"
        log._buffer.enqueue_deletions([stranded], 0)
        log.set_archive(f"s3://{bucket}/current")

        log._maintenance.drain()

        assert fs.exists(objects[0]), (
            "drain must not delete from the archive the log has left"
        )
        assert stranded in log._buffer.queued_deletions(), (
            "and it stays queued for whoever owns that archive"
        )


def test_a_trailing_slash_does_not_wedge_the_remote_queue(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The configured URI may carry a trailing slash; queued paths never do.

    Every remote path is built from the warehouse with its slashes stripped, so
    comparing against the URI verbatim classifies this log's OWN objects as
    another archive's — and the guard that exists to protect a retired bucket
    instead stops the queue draining at all, for ever.
    """
    with archived_log(tmp_path, bucket, s3, snapshot_retention=timedelta(0)) as log:
        log.set_archive(f"s3://{bucket}/slashed/")
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()

        fs = filesystem(s3)
        objects = fs.find(f"{bucket}/slashed")
        assert objects
        doomed = f"s3://{objects[0]}"
        log._buffer.enqueue_deletions([doomed], 0)

        log._maintenance.drain()

        assert doomed not in log._buffer.queued_deletions(), (
            "the log's own archived object must be drainable"
        )


def test_a_register_whose_rows_never_landed_is_recovered_from_the_manifest(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The crash window I4-per-segment has, and how it closes (§4a).

    The row naming a file's archive copy is written AFTER the register, so a
    crash between the two leaves the archive holding a range nothing local
    records. Compaction decides from those rows, so it would merge that file
    into one spanning the archive's extent, and the next push would register a
    partially overlapping range — which `register` admits, because it declines
    only a range entirely covered.

    Nothing is promised beforehand to cover it. The archive's own manifest is
    the truth, and the next push reads it anyway, so recovery is a backfill.
    """
    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()
        local = log._table.data_files()
        settled = log._maintenance.archived_prefix(
            local, log._archive.uri, include_intents=False
        )

        assert settled > 0

        # The register landed; the rows recording it did not.
        with log._buffer._lock:
            log._buffer._con.execute("DELETE FROM extent WHERE rel_path LIKE 's3://%'")
            log._buffer._con.commit()

        assert (
            log._maintenance.archived_prefix(
                local, log._archive.uri, include_intents=False
            )
            == 0
        ), "the setup must actually reproduce the crash"

        log.sync()

        assert (
            log._maintenance.archived_prefix(
                log._table.data_files(), log._archive.uri, include_intents=False
            )
            == settled
        ), "the archive's manifest says what it holds; recover from it"
        assert log.scan(include_archive=True).read_all().num_rows == ROWS


def test_the_backfill_sees_copies_another_process_pushed(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The manifest is read as it IS, not as this handle last saw it.

    A pyiceberg handle is a frozen snapshot and `Archive` caches it for the
    life of the process, so a maintainer that synced, released the lease and
    took it back would otherwise recover against an archive that has since
    grown — and go on treating another maintainer's pushes as unarchived.
    """
    with archived_log(tmp_path, bucket, s3) as writer:
        writer.extend(rows(ROWS))
        writer.seal_due()
        writer.sync()

        with Log.open(tmp_path, "s", s3=s3) as other:
            # `other` caches its archive handle here, at today's extent.
            assert other.archive_files() > 0

            writer.extend(rows(ROWS))
            writer.seal_due()
            writer.sync()
            grown = writer._maintenance.archived_prefix(
                writer._table.data_files(), writer._archive.uri, include_intents=False
            )

            with other._buffer._lock:
                other._buffer._con.execute(
                    "DELETE FROM extent WHERE rel_path LIKE 's3://%'"
                )
                other._buffer._con.commit()

            other.sync()

            assert (
                other._maintenance.archived_prefix(
                    other._table.data_files(), other._archive.uri, include_intents=False
                )
                == grown
            ), "recovered against a stale manifest"


def test_repointing_to_the_same_archive_spelled_differently_keeps_the_watermark(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A trailing slash is not a move.

    Every path builder strips one, so `s3://b/p` and `s3://b/p/` name the same
    objects everywhere except the comparison that decides whether the archive
    changed. Read verbatim, re-stating the current archive with a slash on the
    end reads as a move and resets both watermarks — against a bucket that
    genuinely holds the data, which is I4's whole premise for eviction.
    """
    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()
        settled = log.archived_through()
        assert settled > 0

        log.set_archive(f"s3://{bucket}/prefix/")

        assert log.archived_through() == settled, (
            "the same archive, spelled with a trailing slash, is the same archive"
        )
        assert log.scan().read_all().num_rows == ROWS


def test_a_repoint_is_all_or_nothing(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Where the archive is and what it holds are only true together.

    The two watermarks describe the PREVIOUS archive, so a crash between them
    and the location leaves a log whose parts disagree — and both orderings
    have cost a defect. Watermark last leaves the new archive carrying the old
    one's promise, which eviction believes. Watermark first leaves the OLD
    archive with a frontier of zero, and compaction, unlike eviction, does not
    wait for a sync before merging across a boundary the archive already holds.

    So the failure is injected rather than reasoned about: any single write may
    fail, and what survives must still be coherent.
    """
    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()
        settled = log.archived_through()
        assert settled > 0

        calls = 0
        original = log._buffer.set_meta

        def flaky(key: str, value: str) -> None:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("crash between the writes")

            original(key, value)

        log._buffer.set_meta = flaky  # ty: ignore[invalid-assignment]
        try:
            log.set_archive(f"s3://{bucket}/elsewhere")
        except RuntimeError:
            pass

        finally:
            log._buffer.set_meta = original  # ty: ignore[invalid-assignment]

        recorded = log._buffer.get_meta("archive") or None
        if recorded == f"s3://{bucket}/prefix":
            assert log.archived_through() == settled, (
                "still the old archive, so its watermark must still be true"
            )

        else:
            assert log.archived_through() == 0, (
                "a new archive must not inherit the old one's promise"
            )


def test_set_meta_if_writes_only_while_the_guard_still_holds(tmp_path: Path) -> None:
    """The guard and the write it protects are one transaction, or no guard.

    `sync` re-reads which archive it is pushing to before recording a
    watermark. Read and written separately, that check only reports where the
    archive was — a `set_archive` landing between leaves the log pointed at the
    NEW archive holding the OLD one's extent, which eviction believes (I4) and
    nothing ever lowers.

    What this pins is the contract, not the atomicity: the window the
    transaction closes is between two adjacent statements, and a test that
    tried to land inside it would be a race that usually loses. Atomicity here
    is by construction — one `BEGIN IMMEDIATE`, per SPEC §4a.
    """
    log = Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",))
    with log:
        log._buffer.set_meta("archive", "s3://a/p")

        assert not log._buffer.set_meta_if("archive", "s3://b/p", {"w": "9"}), (
            "a guard that no longer holds must decline"
        )
        assert log._buffer.get_meta("w") is None

        assert log._buffer.set_meta_if("archive", "s3://a/p", {"w": "9"})
        assert log._buffer.get_meta("w") == "9"


def test_a_repoint_during_a_push_forfeits_the_watermark(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A watermark earned by one archive is never recorded against another.

    The push outlives its lease — a register alone measured 4.1 s against S3,
    and retries compound it — so the `set_archive` that races it holds the
    lease lawfully. What must not happen is the log ending up pointed at the
    new archive while `archived_through` describes the old one: eviction acts
    on that number (I4) and deletes local files the new archive was never sent.
    """
    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()
        settled = log.archived_through()
        assert settled > 0

        log.extend(rows(ROWS))
        log.seal_due()

        # The re-point lands while this push is still in S3.
        archive = log._archive.require()
        original = archive.register

        def racing(*args: object, **kwargs: object) -> bool:
            outcome = original(*args, **kwargs)  # ty: ignore[invalid-argument-type]
            log._buffer.set_meta("archive", f"s3://{bucket}/elsewhere")
            return outcome

        archive.register = racing  # ty: ignore[invalid-assignment]
        try:
            with pytest.raises(RuntimeError, match="re-pointed"):
                log.sync()

        finally:
            archive.register = original  # ty: ignore[invalid-assignment]

        assert log._maintenance.archived_through() == settled, (
            "an archive the log has never pushed to must not inherit a watermark"
        )


def test_eviction_learns_about_an_archive_attached_by_another_process(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """I4 is owed by the log, not by a process's memory of it.

    Attaching an archive to a log a maintainer already has open is supported
    (§13.0). `sync` is the only thing that refreshes this process's `Archive`,
    and a maintainer that believes the log is local-only never syncs — so it
    would go on deleting the only copy of every row that ages past
    `local_retention`, for as long as it ran, while the durable configuration
    promised the archive held them.

    The detach direction heals on its own, because a push to an archive that is
    gone fails. This direction has nothing that fails.
    """
    config = LogConfig(
        target_seal_size=32 * 1024,
        target_compact_size=32 * 1024,
        compact_min_files=2,
        local_rows=1,
        snapshot_retention=timedelta(seconds=0),
    )
    writer = Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",), config=config)
    with writer:
        writer.extend(rows(ROWS))
        writer.seal_due()

        # The maintainer opened while the log was local-only.
        with Log.open(tmp_path, "s", s3=s3) as maintainer:
            assert not maintainer._archive.configured()

            writer.set_archive(f"s3://{bucket}/prefix")

            maintainer.evict()

            assert maintainer.scan().read_all().num_rows == ROWS, (
                "nothing may be deleted for an archive that holds nothing"
            )


def test_a_commit_retry_will_not_follow_the_catalog_to_another_archive(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The Iceberg commit is the one durable write with no watermark fence.

    `_commit` reloads and retries when the branch moves under it, and the
    catalog row is keyed by table id rather than by identity — so a re-point
    racing a slow register makes the reload re-bind the operation to the NEW
    archive, and the retry commits paths that live in the old bucket.
    """
    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()

        archive = log._archive.require()
        moved = Layout(tmp_path, "s").warehouse_uri
        archive._warehouse = f"s3://{bucket}/somewhere-else"

        with pytest.raises(RuntimeError, match="moved out of"):
            archive._verify_identity()

        assert moved


def test_restating_the_archive_from_a_stale_process_keeps_the_watermark(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """ "Is this a move?" is a question about the log, not about this process.

    Nothing refreshes a process's memory of where the archive is except a sync,
    so in the two-process deployment `set_archive` is documented for, a caller
    can hold a stale one. Asked of that memory, both answers are wrong — here,
    re-asserting the archive the log already has reads as a move and zeroes the
    watermarks of a bucket that genuinely holds the data, which drops the
    compaction frontier to 0 over a live archive.
    """
    with archived_log(tmp_path, bucket, s3) as writer:
        writer.extend(rows(ROWS))
        writer.seal_due()
        writer.sync()
        settled = writer.archived_through()
        assert settled > 0

        with Log.open(tmp_path, "s", s3=s3) as other:
            # There is no per-process memory to go stale any more, so the
            # case this once modelled cannot arise: re-asserting the archive
            # the log already has reads the same durable value either way.
            other.set_archive(f"s3://{bucket}/prefix")

            assert other.archived_through() == settled, (
                "re-asserting the archive the log already has is not a move"
            )


def test_a_fence_cannot_be_satisfied_by_the_repoint_it_guards_against(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Both sides of the comparison must not move together.

    `Archive` is shared by the log, the reader and the maintainer exactly so a
    re-point reaches all three — which means a `set_archive` on another thread
    updates the value a fence is about to compare against as well as the one it
    compares. Read live, the fence passes, and the watermark this push earned
    is recorded against an archive that never received it.
    """
    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()
        settled = log.archived_through()
        assert settled > 0

        log.extend(rows(ROWS))
        log.seal_due()

        # A full in-process re-point, landing while the push is in S3: it moves
        # the durable location AND this shared object's memory of it.
        archive = log._archive.require()
        original = archive.register

        def racing(*args: object, **kwargs: object) -> bool:
            outcome = original(*args, **kwargs)  # ty: ignore[invalid-argument-type]
            log._buffer.set_meta("archive", f"s3://{bucket}/elsewhere")
            return outcome

        archive.register = racing  # ty: ignore[invalid-assignment]
        try:
            with pytest.raises(RuntimeError, match="re-pointed"):
                log.sync()

        finally:
            archive.register = original  # ty: ignore[invalid-assignment]

        assert log._maintenance.archived_through() == settled, (
            "an archive the log has never pushed to must not inherit a watermark"
        )


def test_the_archive_refuses_a_range_that_starts_inside_its_extent(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The last line of defence, and the only one not reasoned around.

    `_covers` declines a range entirely covered, which makes a replayed push
    harmless. A range that starts inside the extent and ends beyond it is a
    different thing: those offsets land in two files at once, in the immutable
    tier, with nothing able to repair it. Everything upstream is arranged so a
    merge never straddles the archive's extent, and every gap found in that
    arrangement has been a fresh piece of reasoning — this check holds however
    the reasoning turns out.
    """
    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.sync()
        archive = log._archive.require()
        archive.reload()
        covered = archive.extent()

        assert covered is not None

        # A file whose range begins inside what the archive already holds.
        with pytest.raises(ValueError, match="two files at once"):
            archive.register(["s3://nowhere/straddle.parquet"], lo=covered[1])

        # And one that ENGULFS it — starting below the extent and running past
        # it. This is the worse shape, not an excused one: it puts every
        # archived offset in two files rather than some of them.
        with pytest.raises(ValueError, match="two files at once"):
            archive.register(["s3://nowhere/engulf.parquet"], lo=max(covered[0] - 5, 0))

        # And one that begins cleanly above it is not refused by this check.
        archive._refuse_straddle(covered[1] + 1)


def test_the_log_keeps_working_after_the_archive_is_re_cut(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """`rewrite_archive` while local files still overlap what it re-cuts.

    The two tiers then hold the same rows at boundaries neither shares, which
    is the state every later decision has to survive: I4 asking whether the
    archive holds a local file's rows, compaction deciding what is already the
    archive's business, and the archive refusing a range that starts inside its
    extent. The existing rewrite test evicts everything first, so none of that
    is exercised there.
    """
    config = replace(
        LogConfig(),
        target_seal_size=16 * 1024,
        target_compact_size=32 * 1024,
        compact_min_files=2,
        local_rows=2000,
        snapshot_retention=timedelta(seconds=0),
    )
    log = Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        archive=f"s3://{bucket}/prefix",
        s3=s3,
    )
    with log:
        total = 0
        for _ in range(4):
            log.extend(rows(3000))
            total += 3000
            log.seal_due()
            log.maintain()
            log.sync()

        assert log.archive_files() > 2, "expected several undersized archive files"
        assert log._table.data_files(), "local files must still overlap the archive"

        # The documented reason it exists: a raised target leaves history sized
        # for the old one.
        log.set_config(replace(config, target_compact_size=256 * 1024))
        log.rewrite_archive()

        assert log.scan(include_archive=True).read_all().num_rows == total

        # The superseded rows still sit in `extent`: `drain` removes them only
        # after the grace period, and until it does they still match the local
        # cuts. The state that matters is the one AFTER that, so take it.
        current = {(f.lo, f.hi + 1) for f in log._archive.require().data_files()}
        with log._buffer._lock:
            for lo, hi in log._buffer.archived_ranges(
                log._archive.uri or "", 0, include_intents=False
            ):
                if (lo, hi) not in current:
                    log._buffer._con.execute(
                        "DELETE FROM extent WHERE start_offset = ? AND end_offset = ? "
                        "AND rel_path LIKE 's3://%'",
                        (lo, hi),
                    )

            log._buffer._con.commit()

        # And the log has to keep going past a re-cut archive.
        log.extend(rows(3000))
        total += 3000
        log.seal_due()
        log.maintain()
        log.sync()

        table = log.scan(include_archive=True).read_all()
        offsets = (
            log.scan(columns=["litelink_offset"], include_archive=True)
            .read_all()
            .column(0)
            .to_pylist()
        )

        assert table.num_rows == total, "sync stalled or lost rows after the re-cut"
        assert len(set(offsets)) == len(offsets), "duplicate offsets after the re-cut"

        # And eviction must still be making progress. This is what the re-cut
        # actually broke: reads keep answering correctly whether or not I4 can
        # still recognise the archive's copies, so a row count says nothing.
        # `local_rows` is what says it — asked for 2,000 and the archive holds
        # every one of these, a local table still carrying all 15,000 means
        # eviction has stopped and `local_retention` is silently void.
        local = log.scan().read_all().num_rows

        assert local < total // 2, (
            f"eviction stalled after the re-cut: {local} of {total} rows still local"
        )


def test_a_stale_handle_cannot_repair_the_archive_it_was_pointed_away_from(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Opening with `repair` is the most dangerous thing a handle does.

    It lets `open_archive` drop a catalog entry naming another prefix and
    create a fresh table at this one. The maintenance claim is what entitles a
    caller to that; the durable location is what tells it WHICH archive to
    repair, and every repairing caller except `sync` inherited the privilege
    without the premise. A handle that remembers the archive the log has left
    then destroys the live archive's catalog entry, and the next pass "repairs"
    again by creating an empty table over its data.
    """
    with archived_log(tmp_path, bucket, s3) as writer:
        writer.extend(rows(ROWS))
        writer.seal_due()
        writer.sync()

        with Log.open(tmp_path, "s", s3=s3) as stale:
            # `stale` remembers the first archive and never opens its handle.
            assert stale._archive.uri == f"s3://{bucket}/prefix"

            writer.set_archive(f"s3://{bucket}/second")
            writer.extend(rows(ROWS))
            writer.seal_due()
            writer.sync()
            readable = writer.scan(include_archive=True).read_all().num_rows

            # The documented ad-hoc operation, run from the stale handle.
            stale.rewrite_archive()

            assert writer.scan(include_archive=True).read_all().num_rows == readable, (
                "a stale handle repaired the wrong archive and lost history"
            )
            assert stale._archive.uri == f"s3://{bucket}/second", (
                "the repairing open must adopt the location the log records"
            )


def test_a_rewrite_re_cuts_to_the_compact_row_target(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The scratch takes BOTH targets from compaction, not one from each.

    It cuts at whichever ceiling comes first, so carrying the live seal ROW cap
    made a rewrite cut its outputs at the seal's row limit while the archive
    holds files sized to the compact one — many times more files than it
    started with, each still undersized by bytes, so the next
    `rewrite_archive` flags the same tail again and it never converges. The
    operation exists to merge undersized archived files; that inverted it.
    """
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        target_seal_rows=40,
        target_compact_size=16 * 1024,
        target_compact_rows=80,
        compact_min_files=2,
        snapshot_retention=timedelta(seconds=0),
    )
    log = Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        archive=f"s3://{bucket}/prefix",
        s3=s3,
    )
    with log:
        for _ in range(6):
            log.extend(rows(400))
            log.seal_due()
            log.maintain()
            log.sync()

        before = log.archive_files()

        assert before > 1, "expected several archived files to re-cut"

        # A raised target makes the history undersized, which is the reason
        # this operation exists.
        log.set_config(replace(config, target_compact_size=1 << 20))
        log.rewrite_archive()

        after = log.archive_files()

        assert after <= before, (
            f"the rewrite fragmented the archive: {before} files became {after}"
        )
        assert log.scan(include_archive=True).read_all().num_rows == 2400


def test_compaction_while_detached_does_not_wedge_a_reattach(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Four legitimate operations, no warning at any step, and a dead log.

    Detach, raise the compaction target so history is undersized again,
    maintain, re-attach. While detached, compaction had no archive to ask
    about, so it merged across ranges the archive still holds — and nothing
    re-cuts a LOCAL straddler: `rewrite_archive` works the other side. On
    re-attach, eviction pins below the straddler for ever and every push is
    refused by `_refuse_straddle`, which the shipped maintainer does not catch.

    Detaching does not make the archive's copies stop existing, so compaction
    asks whether ANY archive holds a file, not whether the configured one
    does. It costs nothing: only compacted files are ever pushed, so a file
    with an archive copy is already at the target.
    """
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        target_compact_size=16 * 1024,
        compact_min_files=2,
        snapshot_retention=timedelta(seconds=0),
    )
    archive = f"s3://{bucket}/prefix"
    log = Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        archive=archive,
        s3=s3,
    )
    with log:
        for _ in range(4):
            log.extend(rows(400))
            log.seal_due()
            log.maintain()
            log.sync()

        archived = log._maintenance.archived_prefix(
            log._table.data_files(), log._archive.uri, include_intents=False
        )

        assert archived > 0, "expected the archive to hold a prefix"

        log.set_archive(None)
        log.set_config(replace(config, target_compact_size=1 << 20))
        log.extend(rows(400))
        log.seal_due()
        log.maintain()

        assert all(
            f.lo > archived or f.hi <= archived for f in log._table.data_files()
        ), "merged across a range the archive holds while detached"

        log.set_archive(archive)
        log.maintain()
        log.sync()

        assert log.scan(include_archive=True).read_all().num_rows == 2000


def test_expiring_the_archive_will_not_repair_it_without_a_claim(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A repairing open is a claim holder's privilege, and expiry had neither.

    Expiry is exempt from claims because it is a metadata commit CAS orders.
    That is true of the snapshot expiry and not of the repairing open beside
    it, which DROPS a catalog entry naming another prefix and creates a table
    in its place. Two of those at once collide on the first attempt, because
    pyiceberg writes the metadata object before inserting the catalog row — and
    the loser raises a bare `Exception` the shipped maintainer does not catch.

    Rounds nine and ten fixed which archive a repairing open targets; this is
    the other half, who is entitled to open one.
    """
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        target_compact_size=16 * 1024,
        compact_min_files=2,
        snapshot_retention=timedelta(seconds=0),
    )
    log = Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        archive=f"s3://{bucket}/prefix",
        s3=s3,
    )
    with log:
        for _ in range(4):
            log.extend(rows(400))
            log.seal_due()
            log.maintain()
            log.sync()

        # A rewrite is the one thing that queues a REMOTE deletion, which is
        # the signal `_expire_archive` acts on.
        log.set_config(replace(config, target_compact_size=1 << 20))
        log.rewrite_archive()

        assert any("://" in p for p in log._buffer.queued_deletions()), (
            "expected a remote entry to make expiry reach the archive"
        )

        opened: list[bool] = []
        original = log._archive.table

        def watching(*, repair: bool = False) -> object:
            opened.append(repair)

            return original(repair=repair)

        held = log._lease("maintain")

        assert held.acquire()

        log._archive.table = watching  # ty: ignore[invalid-assignment]
        try:
            log.expire()

        finally:
            log._archive.table = original  # ty: ignore[invalid-assignment]
            held.release()

        assert not any(opened), (
            "opened the archive with repair while another owner held the log"
        )


def test_the_published_hint_names_the_metadata_the_commit_produced(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The hint has to name the metadata the table is actually at.

    Asserted against the pointer directly, which is why this sits beside the
    re-attach test rather than inside it: a round trip through `set_archive`
    recovers a hint that is one version stale often enough to pass, because the
    missing snapshot's rows may still be in the local tier.

    It does NOT pin publish-after-reload. That was the intent, and falsifying
    it showed the ordering is not observable: pyiceberg updates the handle in
    place when a commit lands, so publishing before `_commit`'s reload writes
    the same hint. The claim the code makes has been narrowed to match.
    """
    where = f"s3://{bucket}/hinted"
    with Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        config=replace(LogConfig(), target_seal_size=8 * 1024, compact_min_files=2),
        archive=where,
        s3=s3,
    ) as log:
        log.extend(rows(400))
        log.seal_due()
        log.maintain()
        log.sync()

        archive = log._archive.require()  # noqa: SLF001
        archive.reload()
        current = str(archive.metadata_location)

    fs = filesystem(s3)
    published = fs.cat(f"{bucket}/hinted/{NAMESPACE}/s/metadata/{VERSION_HINT}")

    assert current.endswith(f"/{published.decode().strip()}.metadata.json"), (
        f"hint {published!r} does not name {current!r}"
    )


def test_the_archive_reads_as_a_directory_with_no_catalog_at_all(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """What the hint buys beyond re-attach: an archive nothing local can read.

    litelink resolves the archive through `archive.db` and hands DuckDB a
    metadata path (§7). This is the other reader — an engine pointed at the
    prefix, with no catalog, no local root, and nothing but the bucket.

    **`version_name_format` is required, and that is the documented cost.**
    DuckDB's default is the Hadoop `v%s%s.metadata.json`; pyiceberg names its
    metadata `00003-<uuid>.metadata.json`, so the hint holds that stem and the
    format has to stop prepending a `v`. Writing a second copy under the
    Hadoop name would remove the parameter and add an object per commit that
    nothing collects — see `VERSION_HINT`.
    """
    duckdb = pytest.importorskip("duckdb")
    where = f"s3://{bucket}/standalone"
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        target_compact_size=16 * 1024,
        compact_min_files=2,
        local_rows=200,
    )
    with Log.new(
        tmp_path, "s", schema=SCHEMA, config=config, archive=where, s3=s3
    ) as log:
        for _ in range(4):
            log.extend(rows(400))
            log.seal_due()
            log.maintain()
            log.sync()

        archived = log.archived_through()

    assert archived > 0, "nothing reached the archive to read back"

    connection = duckdb.connect()
    load_extension(connection, "iceberg", remote=False)
    load_extension(connection, "httpfs", remote=True)
    connection.execute(secret_sql(s3))
    directory = f"{where}/{NAMESPACE}/s"
    rows_read = connection.execute(
        f"SELECT count(*) FROM iceberg_scan('{directory}',"
        " version_name_format = '%s%s.metadata.json')"
    ).fetchone()

    assert rows_read is not None
    assert rows_read[0] == archived


def test_pointing_back_at_an_archive_restores_everything_it_held(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A re-point costs reach, not data — and pointing back gets it back.

    Both halves matter and they are different claims. While pointed elsewhere,
    rows evicted into the old archive are out of reach: the read path resolves
    exactly one archive, so `scan(include_archive=True)` returns fewer rows
    than were written, silently. That has not changed.

    What has is the way back. This test asserted the opposite until the archive
    began publishing `version-hint.text` beside its metadata — before that, the
    local catalog row was the only thing naming the archive's current metadata,
    and re-pointing drops it, so returning built an EMPTY table over objects
    still sitting in the bucket. Now the bucket says where its own metadata is,
    and `open_archive` registers from that instead of creating.
    """
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        target_compact_size=16 * 1024,
        compact_min_files=2,
        local_rows=200,
        snapshot_retention=timedelta(seconds=0),
    )
    first = f"s3://{bucket}/first"
    log = Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        archive=first,
        s3=s3,
    )
    with log:
        written = 0
        for _ in range(4):
            log.extend(rows(400))
            written += 400
            log.seal_due()
            log.maintain()
            log.sync()

        assert log.scan(include_archive=True).read_all().num_rows == written

        log.set_archive(f"s3://{bucket}/second")
        moved = log.scan(include_archive=True).read_all().num_rows

        assert moved < written, "expected the evicted history to be out of reach"

        # ALL of them, not merely more than `moved`. Adopting the archive has
        # to hand back the extent it actually holds; a partial recovery would
        # mean registering a metadata JSON older than the last commit, which is
        # the failure mode a remembered-at-open pointer would have had and this
        # one must not.
        log.set_archive(first)

        assert log.scan(include_archive=True).read_all().num_rows == written

        # And it is genuinely the old table, not a new one that happens to
        # read: an empty table created over the objects would show no files at
        # all while the union still answered from the local tier.
        assert log.archive_files() > 0


def test_a_fresh_prefix_after_a_target_raise_does_not_stall(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Compaction and `sync` must exclude the same files, or they deadlock.

    `stable_prefix` holds a file back when compaction might still merge it, and
    compaction refuses to merge anything some archive already holds. Give
    compaction that second input without giving it to `sync` and the two stop
    agreeing: after a re-point to a FRESH prefix the floor is 0, so files the
    old archive covers are back in `pending`, group into a mergeable run under
    the raised target, and are held back for ever against a merge that will
    never happen. Nothing is ever pushed, the watermark never moves, eviction
    pins on it, and no error surfaces anywhere.

    The re-attach test next to this one hides it, because re-attaching the SAME
    archive leaves its own extent as the floor, which keeps those files out of
    `pending` entirely.
    """
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        target_compact_size=16 * 1024,
        compact_min_files=2,
        snapshot_retention=timedelta(seconds=0),
    )
    log = Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        archive=f"s3://{bucket}/first",
        s3=s3,
    )
    with log:
        for _ in range(4):
            log.extend(rows(400))
            log.seal_due()
            log.maintain()
            log.sync()

        log.set_config(replace(config, target_compact_size=1 << 20))
        log.set_archive(f"s3://{bucket}/second")

        for _ in range(4):
            log.extend(rows(400))
            log.seal_due()
            log.maintain()
            log.sync()

        assert log.archive_files() > 0, (
            "nothing was ever pushed to the new archive: sync and compaction "
            "disagree about which files are still in play"
        )
        assert log.archived_through() > 0, "the watermark never moved"


def _crash_before_recording(log: Log) -> None:
    """Sync, dying between the register and the rows recording it."""
    original = Buffer.record_file

    def dying(*args: object, **kwargs: object) -> None:
        msg = "crash between the register and the record"
        raise RuntimeError(msg)

    Buffer.record_file = dying
    try:
        with pytest.raises(RuntimeError, match="crash between"):
            log.sync()

    finally:
        Buffer.record_file = original


def test_a_register_without_its_rows_cannot_wedge_the_log(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The window two polarities exist to close.

    A register lands and the rows recording it do not. Compaction decides what
    it may merge from those rows, so a compaction-target change before the next
    sync regroups the pushed-but-unrecorded files and commits a LOCAL file
    straddling the archive's extent — after which every push is refused for
    ever and nothing re-cuts a local straddler.

    The intent is written before the register, and compaction reads intents
    while eviction does not: overstated coverage is compaction's safe
    direction, understated is eviction's, and one record cannot be both.
    """
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        target_compact_size=16 * 1024,
        compact_min_files=2,
        snapshot_retention=timedelta(seconds=0),
    )
    log = Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        archive=f"s3://{bucket}/prefix",
        s3=s3,
    )
    with log:
        for _ in range(3):
            log.extend(rows(400))
            log.seal_due()
            log.maintain()

        _crash_before_recording(log)

        archive = log._archive.require()
        archive.reload()
        extent = archive.extent()
        intents = log._buffer.intents(log._archive.uri or "")

        assert extent is not None
        assert intents, "the crash must leave the intents behind"

        local = log._table.data_files()

        assert log._maintenance.archived_prefix(local, None, include_intents=True) > 0
        assert (
            log._maintenance.archived_prefix(
                local, log._archive.uri, include_intents=False
            )
            == 0
        ), "eviction must not see an intended copy as a landed one"

        # The ingredient that turns the crash into a permanent stall.
        log.set_config(replace(config, target_compact_size=1 << 20))
        log.maintain()

        assert all(
            f.lo > extent[1] or f.hi <= extent[1] for f in log._table.data_files()
        ), "merged across the archive's extent"

        log.sync()

        assert not log._buffer.intents(log._archive.uri or ""), "intents not reconciled"
        assert log.scan(include_archive=True).read_all().num_rows == 1200


def test_eviction_never_acts_on_an_intended_copy(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The understating half of the split, on its own.

    An intent says a copy is coming, not that it is there. Eviction deletes the
    only local copy on the strength of what it reads, so reading an intent as
    coverage is the loss this whole record exists to prevent — and it is the
    direction a `confirmed` column would have handed an older build for free.
    """
    config = replace(LogConfig(), local_rows=50, target_seal_size=1 << 30)
    log = Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        archive=f"s3://{bucket}/prefix",
        s3=s3,
    )
    with log:
        # Several files, so the row floor lands on an edge below the head —
        # with one file there is no boundary to snap to and eviction returns
        # early whatever it believes about coverage.
        for _ in range(3):
            log.extend(rows(100))
            log.seal()

        before = len(log._table.data_files())

        assert before == 3

        for data_file in log._table.data_files():
            log._buffer.intend_file(
                f"s3://{bucket}/prefix/data/{data_file.lo}.parquet",
                data_file.lo,
                data_file.hi + 1,
                1,
            )

        log.evict()

        assert len(log._table.data_files()) == before, (
            "evicted the only copy on the strength of an intended one"
        )


def test_two_owners_intending_one_path_do_not_collide(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """`intend_file` is an upsert, and a bare insert kills the wrong process.

    A holder that stalled past its TTL and resumed can intend a path the owner
    that took over is also intending. On a bare insert the primary key raises,
    and neither the maintainer nor anything else catches it — so the takeover
    race would end the LAWFUL holder's pass rather than the stale one's.

    Nothing else in this suite drives two live intents onto one path: every
    other scenario intends a path reconciliation has already cleared.
    """
    with archived_log(tmp_path, bucket, s3) as log:
        path = f"s3://{bucket}/prefix/data/contested.parquet"

        log._buffer.intend_file(path, 1, 101, 4096)
        # The other owner, intending the same path with its own view of it.
        log._buffer.intend_file(path, 1, 101, 8192)

        intents = log._buffer.intents(f"s3://{bucket}/prefix")

        assert len(intents) == 1, "one path, one intent"
        assert intents[0] == (path, 1, 101, 8192), "the later intent must win"


def test_a_healed_row_carries_the_measured_bytes(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The test the plan mandated, which the first build shipped without.

    The `bytes` column on an intent exists for exactly one reader: rule 1,
    healing a crash with the only measurement that survives it. Without this
    assertion the whole suite passes against a reconciliation that records the
    compact target everywhere — which is what the first build did, because rule
    2 re-fired for the paths rule 1 had just confirmed and its conflict clause
    overwrote them.

    What that costs is not cosmetic: a rewrite's deliberately undersized tail
    recorded as full is never flagged by `_badly_sized` again, so
    `rewrite_archive` stops converging it, and nothing re-measures an archived
    file.
    """
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        target_compact_size=64 * 1024,
        compact_min_files=2,
        snapshot_retention=timedelta(seconds=0),
    )
    log = Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        archive=f"s3://{bucket}/prefix",
        s3=s3,
    )
    with log:
        for _ in range(3):
            log.extend(rows(400))
            log.seal_due()
            log.maintain()

        _crash_before_recording(log)

        intended = {
            path: size
            for path, _, _, size in log._buffer.intents(log._archive.uri or "")
        }

        assert intended, "the crash must leave intents behind"
        assert all(size != config.compact_size for size in intended.values()), (
            "the fixture must not coincide with the default it is testing for"
        )

        log.sync()

        healed = {
            path: size
            for path, size in log._buffer.file_bytes().items()
            if path in intended
        }

        assert healed, "the intents were never confirmed"
        assert healed == intended, (
            "a healed row must carry the bytes its intent measured, not the "
            f"compact default: {healed} != {intended}"
        )


def test_a_rewrite_that_lost_its_claim_does_not_commit(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The check between writing and committing, which `_recut` went without.

    A rewrite that stalls past the TTL lets recovery take its claim and queue
    every one of its outputs; `drain` snapshots its reference veto once per
    pass while those objects are still unreferenced; and then the stalled
    commit lands. Drain deletes the objects the manifest now names, the
    superseded originals become unreferenced and go too, and the range exists
    in no object at all — every guard behaving exactly as written.

    Renewing before the commit makes that unreachable: recovery's acquire
    deleted the claim's row, so the renew finds nothing and the rewrite aborts
    while the originals are still live.
    """
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        target_compact_size=16 * 1024,
        compact_min_files=2,
        snapshot_retention=timedelta(seconds=0),
    )
    log = Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        archive=f"s3://{bucket}/prefix",
        s3=s3,
    )
    with log:
        for _ in range(4):
            log.extend(rows(400))
            log.seal_due()
            log.maintain()
            log.sync()

        before = log.archive_files()
        readable = log.scan(include_archive=True).read_all().num_rows

        assert before > 1

        log.set_config(replace(config, target_compact_size=1 << 20))

        # The claim survives every upload and is gone by the commit. The scratch
        # teardown is the marker: the next checkpoint after it is the one
        # guarding `replace_range`, and before this fix there was none.
        # `_discard_scratch` runs TWICE — once clearing any leftover before the
        # rewrite starts, once tearing down at the end — so the flag flips on
        # the second. Flipping on the first aborted the rewrite before it
        # uploaded anything, which passed whether or not the guard existed.
        state = {"calls": 0}
        discard = Maintenance._discard_scratch

        def after_teardown(self: Maintenance) -> None:
            discard(self)
            state["calls"] += 1

        Maintenance._discard_scratch = after_teardown
        try:
            with pytest.raises(RuntimeError, match="lost the claim"):
                log._maintenance.rewrite_archive(heartbeat=lambda: state["calls"] < 2)

        finally:
            Maintenance._discard_scratch = discard

        assert log.archive_files() == before, "committed without holding the claim"
        assert log.scan(include_archive=True).read_all().num_rows == readable


def test_a_rewrite_restamps_the_files_it_supersedes(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The archive half of the grace fix, where the loss was demonstrated.

    `rewrite_archive` queues the files it is replacing when it STARTS, and they
    stop being referenced only when `replace_range` commits. Left at the
    queueing, a rewrite slower than `snapshot_retention` — and re-cutting an
    archive is the slowest thing here — spends the whole grace before it
    commits, so drain takes the originals out from under any reader that
    resolved the pre-rewrite snapshot.

    An end-to-end reader test does not reproduce it: a scan resolved after the
    rewrite reads the new files and never asks about the old ones. What the
    reader depends on is the stamp, so the stamp is what this asserts.
    """
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        target_compact_size=16 * 1024,
        compact_min_files=2,
        snapshot_retention=timedelta(seconds=0),
    )
    log = Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        archive=f"s3://{bucket}/prefix",
        s3=s3,
    )
    with log:
        for _ in range(4):
            log.extend(rows(400))
            log.seal_due()
            log.maintain()
            log.sync()

        assert log.archive_files() > 1

        # A rewrite that began a day ago, as a slow one effectively has.
        stale = int(datetime.now(UTC).timestamp()) - 86_400
        superseded = [f.path for f in log._archive.require().data_files()]
        log._buffer.enqueue_deletions(superseded, stale)

        assert log._buffer.due_deletions(stale + 1), "the setup must look overdue"

        log.set_config(replace(config, target_compact_size=1 << 20))
        log.rewrite_archive()

        overdue = [p for p in log._buffer.due_deletions(stale + 1) if p in superseded]

        assert not overdue, (
            "superseded archive files are still due against a stamp from "
            f"before the commit that superseded them: {overdue}"
        )


def test_replication_holds_sealed_rows_until_the_archive_has_them(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """§3a's middle hole, closed. I4 one tier up.

    A seal moves rows from SQLite into a Parquet file that no sidecar
    replicates, so with WAL shipping on, dropping them at seal removes the only
    off-box copy of a range the archive does not hold yet. The machine dying in
    that window loses them from the MIDDLE of the offset space: below the seal
    frontier so the buffer no longer has them, above the archive frontier so
    the bucket does not either.

    So they stay until sync has pushed the range, and only then go.
    """
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        target_compact_size=16 * 1024,
        compact_min_files=2,
        wal_replication=True,
    )
    with Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        config=config,
        archive=f"s3://{bucket}/held",
        s3=s3,
    ) as log:
        log.extend(rows(1200))
        log.seal_due()

        sealed = log.table_extent()

        assert sealed is not None, "nothing sealed, so the case is not set up"
        # The buffer's FLOOR is the measure, not its size: `count_above(0)`
        # counts the unsealed tail too, which is in the buffer either way.
        buffered = log._buffer.extent()  # noqa: SLF001

        assert buffered is not None
        assert buffered[0] <= sealed[1], (
            "the seal dropped rows the archive does not have yet"
        )
        # And a read is unaffected, which is what makes holding them affordable:
        # the buffer leg is bounded by the local table's committed extent.
        assert log.scan().read_all().num_rows == 1200

        log.maintain()
        log.sync()
        archived = log.archived_through()

        assert archived > 0, "nothing reached the archive"
        # Released only up to the ARCHIVE's frontier, never the seal's.
        released = log._buffer.extent()  # noqa: SLF001

        assert released is not None
        assert released[0] > archived, "rows the archive holds were never released"
        assert log.scan(include_archive=True).read_all().num_rows == 1200


def test_without_replication_a_seal_still_drops_its_rows(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """An archive alone is not the trigger.

    Without a sidecar the buffer and the Parquet share a disk and die together,
    so holding buys nothing and costs SQLite growth on every seal. The gate is
    `wal_replication`, and this is the half that proves an archive by itself
    does not flip it.
    """
    config = replace(LogConfig(), target_seal_size=8 * 1024, compact_min_files=2)
    with Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        config=config,
        archive=f"s3://{bucket}/unheld",
        s3=s3,
    ) as log:
        log.extend(rows(1200))
        log.seal_due()

        sealed = log.table_extent()

        assert sealed is not None
        buffered = log._buffer.extent()  # noqa: SLF001
        # Either the buffer is empty, or what is in it is strictly the unsealed
        # tail — never a row the seal already wrote to Parquet.
        assert buffered is None or buffered[0] > sealed[1], (
            "rows were held with no sidecar to replicate them"
        )


def test_a_held_seal_does_not_widen_the_next_file(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The defect deferring the delete introduces, if the seal is not bounded.

    The seal's read used to be unbounded below. It was correct only because the
    delete made a floor: after `finish_seal`, the buffer's minimum WAS the next
    group's start. Hold the rows and that stops being true, so an unbounded
    read sweeps every earlier row into the next file.

    Nothing catches it at seal time — the local `register` passes no `lo`, so
    `_refuse_straddle` returns early. It surfaces later and elsewhere: manifest
    ranges stop being non-overlapping, the local leg is an unfiltered
    `iceberg_scan` so the overlap is returned twice, and the next sync refuses
    the straddle for ever.

    So this asserts the FILES, not the row count — a total can be right while
    the ranges overlap.
    """
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        target_compact_size=1 << 30,  # never merge, so the seal cuts stand
        compact_min_files=2,
        wal_replication=True,
    )
    with Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        config=config,
        archive=f"s3://{bucket}/widen",
        s3=s3,
    ) as log:
        for _ in range(3):
            log.extend(rows(600))
            log.seal_due()

        files = sorted(log._table.data_files(), key=lambda f: f.lo)  # noqa: SLF001

        assert len(files) > 2, "not enough seals to have a second one to widen"
        # Contiguous and non-overlapping (§4, §6). A widened file starts at the
        # log's floor instead of its own group's, so every later file overlaps
        # every earlier one.
        for earlier, later in zip(files, files[1:], strict=False):
            assert later.lo == earlier.hi + 1, (
                f"{later.lo} does not follow {earlier.hi}: ranges overlap"
            )

        # And the read agrees, which is what the overlap would break.
        assert log.scan().read_all().num_rows == 1800
        assert log.scan().read_all().column(OFFSET).to_pylist() == list(range(1, 1801))


def test_the_archive_declares_the_same_sort_order_as_the_log(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """§4: the order is declared as table metadata, on BOTH tiers.

    `open_archive` never declared one, so an archive holding clustered data
    said nothing about it — a table lying about itself to any reader that is
    not this library, and the reason `sort_by` was unanswerable from the
    archive alone.
    """
    config = replace(LogConfig(), target_seal_size=8 * 1024, compact_min_files=2)
    with Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        archive=f"s3://{bucket}/sorted",
        s3=s3,
    ) as log:
        log.extend(scrambled(600))
        log.seal_due()
        log.maintain()
        log.sync()

        archive = log._archive.require()  # noqa: SLF001

        assert archive.sort_by() == ("event_ts",), (
            "the archive holds clustered data and declares no order"
        )
        # And it still holds the rows, so declaring the order did not disturb
        # the create/publish sequence around it.
        assert log.archived_through() > 0


def test_attaching_an_archive_that_is_ahead_of_the_log_is_refused(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The obvious failover attempt, which wedges the log silently.

    `Log.new` on a second box then `set_archive` at the old prefix: `sync`
    computes its floor from the archive's extent, every local file sits below
    it, so nothing is ever pushed. The watermark is still written, eviction's
    I4 clamp finds no `extent` rows and pins at zero, and local disk grows
    without bound while `sync()` returns success having uploaded nothing.
    """
    where = f"s3://{bucket}/ahead"
    config = replace(LogConfig(), target_seal_size=8 * 1024, compact_min_files=2)
    # A populated archive: this is the log that legitimately owns it.
    with Log.new(
        tmp_path / "first", "s", schema=SCHEMA, config=config, archive=where, s3=s3
    ) as owner:
        owner.extend(rows(1200))
        owner.seal_due()
        owner.maintain()
        owner.sync()

        assert owner.archived_through() > 0

    # A fresh log elsewhere, appending from offset 1, pointed at that archive.
    with Log.new(
        tmp_path / "second", "s", schema=SCHEMA, config=config, s3=s3
    ) as fresh:
        fresh.extend(rows(10))

        with pytest.raises(ValueError, match="another log's history"):
            fresh.set_archive(where)

        assert fresh.archive is None, "the log was re-pointed despite the refusal"


def test_a_prefix_that_holds_nothing_yet_is_still_attachable(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The guard must not fail closed.

    `_repoint` deliberately tolerates an archive that does not exist yet —
    configuring one is a statement of intent, not a claim that the bucket is
    there — and `set_archive` runs on every writer restart. A check that
    raised on an unreadable prefix would turn a routine restart into a coin
    toss against object storage.
    """
    with Log.new(tmp_path, "s", schema=SCHEMA, s3=s3) as log:
        log.extend(rows(10))
        log.set_archive(f"s3://{bucket}/never-written-to")

        assert log.archive == f"s3://{bucket}/never-written-to"


def test_a_hint_naming_unreadable_metadata_does_not_block_attaching(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The guard reads the network in TWO steps, and either can fail.

    An unreachable endpoint is already absorbed — `_published_location`
    swallows and answers None — so this exercises the other half: a hint that
    IS readable, naming metadata that is not. The guard has to treat that as
    "cannot tell" rather than "refuse", because `set_archive` runs on every
    writer restart and configuring an archive is a statement of intent. A real
    problem there surfaces loudly at the first `sync`, which is where it can
    be acted on.
    """
    prefix = f"s3://{bucket}/corrupt"
    fs = filesystem(s3)
    hint = f"{bucket}/corrupt/{NAMESPACE}/s/metadata/{VERSION_HINT}"
    fs.pipe(hint, b"00042-does-not-exist")

    with Log.new(tmp_path, "s", schema=SCHEMA, s3=s3) as log:
        log.extend(rows(10))
        log.set_archive(prefix)

        assert log.archive == prefix


@pytest.mark.slow
def test_a_log_is_recovered_onto_another_machine(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """§3a failover, end to end, against the real sidecar.

    The procedure this replaces — restore the databases and open — does not
    work: `catalog.db` records ABSOLUTE paths to local Iceberg metadata that no
    sidecar ships, so a restored catalog names files on a machine that is gone.
    That failure is asserted first, so this test says what it fixes.
    """
    binary = Path(__file__).resolve().parent.parent / ".bin" / "litestream"
    if not os.access(binary, os.X_OK):
        pytest.skip("litestream is not provisioned — run `just litestream`")

    where = f"s3://{bucket}/failover"
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        target_compact_size=16 * 1024,
        compact_min_files=2,
        wal_replication=True,
    )
    primary = tmp_path / "primary"
    with Log.new(
        primary,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        archive=where,
        s3=s3,
    ) as log:
        log.extend(rows(1200))
        log.seal_due()
        log.maintain()
        log.sync()
        # More on top, sealed but never synced: the band that used to be lost.
        log.extend(rows(400))
        log.seal_due()
        archived = log.archived_through()
        replicated = log.end_offset() - 1
        served = log.scan(include_archive=True).read_all().num_rows

        assert archived < replicated, "nothing left unsynced, so the band is untested"

        replication = log.write_replication_config()

    # Ship it. `-config` names all three databases; only the buffer is restored.
    environment = dict(os.environ)
    resolved = s3.resolved()
    if resolved.access_key and resolved.secret_key:
        environment["LITESTREAM_ACCESS_KEY_ID"] = resolved.access_key
        environment["LITESTREAM_SECRET_ACCESS_KEY"] = resolved.secret_key

    subprocess.run(  # noqa: S603
        [str(binary), "replicate", "-config", str(replication), "-exec", "sleep 3"],
        check=True,
        env=environment,
        capture_output=True,
        timeout=120,
    )

    # AFTER the sidecar stops: rows the primary served and never shipped. This
    # is hole B, and it is what makes the offset reserve necessary rather than
    # decorative — without these the replica's frontier equals the primary's
    # and no reuse is possible to detect.
    with Log.open(primary, "s", s3=s3) as log:
        log.extend(rows(50))
        written = log.end_offset() - 1

    assert written > replicated, "the unreplicated tail did not happen"

    # The machine dies.
    second = tmp_path / "second"

    with Log.restore(second, "s", archive=where, s3=s3, binary=str(binary)) as revived:
        report = revived.recovery()

        assert report is not None
        # Every row is readable again — including the sealed-but-unsynced band,
        # which survives because a seal keeps its rows until the archive has
        # them when wal_replication is on.
        assert revived.scan(include_archive=True).read_all().num_rows == served
        # The shape came from `meta`, not from the catalog that was not restored.
        assert revived._sort_by == ("event_ts",)  # noqa: SLF001
        assert revived.config.target_seal_size == 8 * 1024
        # And it resumes ABOVE everything the primary ever assigned, so no
        # offset the dead machine served is handed to different data.
        resumed = revived.append({"event_ts": 1, "key": "k", "payload": "p"})

        assert resumed > written, f"reissued offset {resumed}, primary served {written}"
        assert report.skipped[1] - report.skipped[0] + 1 == RESTORE_RESERVE

        # And it goes on working. `maintain` is where a stale local `extent`
        # row would surface: compaction reads those rows to decide what to
        # merge, and they name Parquet that is on the machine that died.
        revived.seal_due()
        revived.maintain()
        revived.sync()

        assert revived.scan(include_archive=True).read_all().num_rows == served + 1

        # And this database describes a filesystem that exists. Every local
        # path it names is a file that is here — the dead machine's are gone.
        for path in revived._buffer.file_bytes():  # noqa: SLF001
            if "://" not in path:
                assert (second / path).exists(), (
                    f"names a file that is not here: {path}"
                )


def test_a_stale_archive_catalog_reads_short_until_it_is_dropped(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The measured 261-vs-1061 case, and the one line that fixes it.

    `open_archive` consults `version-hint.text` only when the catalog has NO
    row for the table. With a stale row present it calls `load_table` on
    whatever that names — and old metadata JSONs survive in the bucket until
    expiry, so the load SUCCEEDS and reports the archive as it was several
    syncs ago. Silent, and in the losing direction.

    Worse than under-reading: the next sync commits onto that lineage and
    `publish_pointer` republishes the hint over the fork, destroying the
    pointer a later recovery depends on.

    Both halves are asserted here — the hazard, so it stays documented, and
    that `forget_archive_entry` removes it. `Log.restore` calls that before
    opening, so an operator who restored all three databases by hand gets the
    same protection as one who did not.
    """
    where = f"s3://{bucket}/stale"
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        target_compact_size=16 * 1024,
        compact_min_files=2,
    )
    root = tmp_path / "log"
    layout = Layout(root, "s")
    with Log.new(root, "s", schema=SCHEMA, config=config, archive=where, s3=s3) as log:
        log.extend(rows(600))
        log.seal_due()
        log.maintain()
        log.sync()
        early = log.archive_files()

    # A replica of `archive.db` taken here — the state a WAL restore would
    # bring back — and then the archive grows past it.
    stale = tmp_path / "stale-archive.db"
    shutil.copyfile(layout.archive_db, stale)

    with Log.open(root, "s", s3=s3) as log:
        for _ in range(3):
            log.extend(rows(600))
            log.seal_due()
            log.maintain()
            log.sync()

        current = log.archive_files()

    assert current > early, "the archive did not grow, so staleness is untestable"

    # The hazard: the old catalog wins over the bucket's own pointer.
    shutil.copyfile(stale, layout.archive_db)
    with Log.open(root, "s", s3=s3) as log:
        assert log.archive_files() == early, (
            "expected the stale catalog to be believed; the case has changed"
        )

    # And the fix, which is what `restore` does before it opens anything.
    assert forget_archive_entry(layout), "there was no entry to drop"

    with Log.open(root, "s", s3=s3) as log:
        # A reader may not adopt — that is a write to `archive.db` — so it
        # still sees nothing until a repairing caller runs.
        assert log.archive_files() == 0
        log.sync()

        assert log.archive_files() == current, "adoption did not recover the archive"


def test_forgetting_an_archive_entry_that_is_not_there_is_a_no_op(
    tmp_path: Path,
) -> None:
    """A fresh restore has no `archive.db` at all, which is the ordinary case.

    Constructing a `SqlCatalog` to drop one row would create that catalog's own
    tables as a side effect — a write, from a path whose whole purpose is to
    leave less behind.
    """
    layout = Layout(tmp_path, "s")

    assert forget_archive_entry(layout) is False
    assert not layout.archive_db.exists(), "it created the database it was checking"


def test_a_restore_over_an_interrupted_seal_does_not_duplicate_rows(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A restore lands on whatever the replica caught, including a live seal.

    `sealing` is populated for the whole duration of every seal — the Parquet
    write and the Iceberg commit — so on a busy log a replica reflects one for
    a real fraction of wall time.

    Keeping that claim through a restore looked protective: `_recover_seal`
    finds the rebuilt table empty and rewrites the interrupted file. It also
    duplicates it. The closed-but-unsealed `extent` row is dropped as local, so
    `finish_seal`'s naming UPDATE matches nothing and reports success anyway,
    while the fresh open group still spans the range — and with the rows held
    rather than discarded, the next cut writes them again.

    Asserted on DISTINCT offsets, because the totals are what hid it: the row
    count is simply higher, and every file looks plausible.
    """
    where = f"s3://{bucket}/interrupted"
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        compact_min_files=2,
        wal_replication=True,
    )
    primary = tmp_path / "primary"
    with Log.new(
        primary, "s", schema=SCHEMA, config=config, archive=where, s3=s3
    ) as log:
        log.extend(rows(800))
        log.seal_due()
        log.maintain()
        log.sync()
        # A seal claimed and never finished, exactly as a crash leaves one.
        log.extend(rows(400))
        group = log._buffer.pending_group()  # noqa: SLF001

        assert group is not None, "nothing queued, so there is no seal to interrupt"

        start, end = group
        log._buffer.claim_seal(  # noqa: SLF001
            start, end, log._layout.seal_path(start, end, "tok")
        )
        written = log.end_offset() - 1

    # A restore of that database, by hand — the state a replica would carry.
    second = tmp_path / "second"
    (second / "s").mkdir(parents=True)
    source = sqlite3.connect(Layout(primary, "s").buffer_db)
    copy = sqlite3.connect(Layout(second, "s").buffer_db)
    source.backup(copy)
    source.close()
    copy.close()

    buffer = Buffer.open(Layout(second, "s").buffer_db, SCHEMA)
    try:
        buffer.strip_local_state(1 << 20)
    finally:
        buffer.close()

    LogTable.create(Layout(second, "s"), table_schema(SCHEMA), ())
    with Log.open(second, "s", s3=s3) as revived:
        revived._archive.table(repair=True)  # noqa: SLF001
        # `seal()`, not `seal_due()`. The recovered group is OPEN — `_seed_group`
        # builds it, and the appender never cut it — so `seal_due` drains
        # nothing and the overlap never materialises. Closing it is what the
        # next real seal on that box would do.
        revived.seal()
        revived.maintain()

        offsets = revived.scan(include_archive=True).read_all().column(OFFSET)

        assert len(offsets) == len(set(offsets.to_pylist())), (
            "the interrupted seal was replayed on top of the recovered group"
        )
        assert len(offsets) <= written


def test_recovering_a_committed_seal_keeps_the_rows_replication_still_owes(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The crash window §3a exists for, on the recovery path rather than the
    ordinary one.

    `_recover_seal` has two exits. The one that finds the file already
    committed retired the group with the default `discard=True`, so it deleted
    rows the archive had not taken — the only off-box copy — while the sibling
    exit eighteen lines below passed the flag correctly.
    """
    where = f"s3://{bucket}/recovered"
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        compact_min_files=2,
        wal_replication=True,
    )
    with Log.new(
        tmp_path, "s", schema=SCHEMA, config=config, archive=where, s3=s3
    ) as log:
        log.extend(rows(600))
        group = log._buffer.pending_group()  # noqa: SLF001

        assert group is not None

        start, end = group
        rel_path = log._layout.seal_path(start, end, "tok")
        log._buffer.claim_seal(start, end, rel_path)  # noqa: SLF001
        # Committed, not retired: the crash lands between the two.
        log._write_and_commit(start, end, rel_path)

        held = log._buffer.count_above(0)  # noqa: SLF001

        assert held > 0

        log.recover()

        assert log._buffer.count_above(0) == held, (  # noqa: SLF001
            "recovery deleted rows the archive has not been sent"
        )


def test_attaching_another_logs_archive_is_refused_at_both_entry_points(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A populated archive this log never pushed to belongs to another log.

    Offsets cannot tell them apart — two logs of the same name both start at 1,
    so a foreign archive whose ranges sit BELOW this log's next offset passes
    the "is it ahead of us" check. What tells them apart is that a log which
    pushed to an archive keeps its `extent` rows naming that prefix across a
    detach (§4a).

    Refused at the door rather than contained afterwards, and two attempts to
    contain it are why. `_push` raises the watermark to the archive's extent on
    every pass, and its backfill writes `extent` rows for the archive's ENTIRE
    manifest within one sync — so by the time anything downstream looks, the
    foreign archive's ranges ARE this log's records. Measured: a bound derived
    from either moved with the contamination.

    Both entry points, because either can be the one that points the log —
    `Log.new(archive=...)` is exactly what an operator reaches for when failing
    over by hand.
    """
    foreign = f"s3://{bucket}/foreign"
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        compact_min_files=2,
        wal_replication=True,
    )
    # Someone else's log, which fills that prefix.
    with Log.new(
        tmp_path / "owner", "s", schema=SCHEMA, config=config, archive=foreign, s3=s3
    ) as owner:
        owner.extend(rows(1200))
        owner.seal_due()
        owner.maintain()
        owner.sync()

        assert owner.archived_through() > 0, "the foreign archive holds nothing"

    # Creating a log against it — the failover-by-hand shape.
    with pytest.raises(ValueError, match="no record of pushing"):
        Log.new(
            tmp_path / "mine", "s", schema=SCHEMA, config=config, archive=foreign, s3=s3
        )

    assert not (tmp_path / "mine" / "s" / "buffer.db").exists(), (
        "the refusal left a half-built log behind"
    )

    # And pointing an existing one at it. Created local-only, so without
    # `wal_replication` — `validate` refuses that pair, and the archive is
    # what this is about to try to attach.
    local_only = replace(config, wal_replication=False)
    with Log.new(
        tmp_path / "other", "s", schema=SCHEMA, config=local_only, s3=s3
    ) as other:
        other.extend(rows(2000))
        other.seal_due()

        with pytest.raises(ValueError, match="no record of pushing"):
            other.set_archive(foreign)

        assert other.archive is None, "the log was pointed despite the refusal"


def test_a_restore_from_a_replica_the_archive_has_outrun(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A replica is a snapshot from BEFORE the primary's last sync.

    That is ordinary replication lag, not a crash window: the bucket routinely
    holds ranges the replicated `extent` rows do not mention. Seeded from the
    replica alone, the open group starts below the archive's frontier, and the
    first seal here writes a file reaching into the archive's extent.

    Which wedges the log for good — `_refuse_straddle` raises on every push,
    `archived_prefix` returns 0 for the straddler so eviction pins at zero, and
    local disk grows without bound. Nothing re-cuts a local straddler, and this
    is the operation you run when the archive is the only surviving copy.

    Asserted by DOING the work a revived box does — seal, maintain, sync,
    repeatedly — rather than by inspecting the group, because the group looks
    entirely reasonable right up until the push.
    """
    where = f"s3://{bucket}/outrun"
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        compact_min_files=2,
        wal_replication=True,
    )
    primary = tmp_path / "primary"
    with Log.new(
        primary, "s", schema=SCHEMA, config=config, archive=where, s3=s3
    ) as log:
        log.extend(rows(800))
        log.seal_due()
        log.maintain()
        log.sync()

        # THE SNAPSHOT, taken here — before the sync below. This is the lag.
        second = tmp_path / "second"
        (second / "s").mkdir(parents=True)
        source = sqlite3.connect(Layout(primary, "s").buffer_db)
        copy = sqlite3.connect(Layout(second, "s").buffer_db)
        source.backup(copy)
        source.close()
        copy.close()

        # The primary carries on: more rows, sealed and PUSHED. The archive is
        # now ahead of everything the snapshot knows about.
        log.extend(rows(800))
        log.seal_due()
        log.maintain()
        log.sync()
        ahead = log.archived_through()

    stale = Buffer.peek_meta(Layout(second, "s").buffer_db, "archive_through")

    assert stale is not None and int(stale) < ahead, (
        "the archive did not outrun the snapshot, so the case is not set up"
    )

    # Restored from that snapshot, then worked the way a revived box is.
    with Log.restore(second, "s", archive=where, s3=s3) as revived:
        # Checked BEFORE any work: the first seal recycles the open group, so
        # a stale one is invisible a moment later. Releasing the archived rows
        # empties this buffer — every row in the snapshot is below the frontier
        # the archive reached — so a group still naming the replica's start
        # would be one with no rows behind it.
        group = revived._buffer._con.execute(  # noqa: SLF001
            "SELECT start_offset FROM extent"
            " WHERE end_offset IS NULL AND rel_path IS NULL"
        ).fetchone()

        assert group is not None, "the log came back with no open group at all"
        assert (group[0] is None) == (revived.buffered_rows() == 0), (
            f"open group starts at {group[0]} with "
            f"{revived.buffered_rows()} rows buffered"
        )

        for _ in range(3):
            revived.extend(rows(200))
            revived.seal_due()
            revived.maintain()
            revived.sync()

        assert revived.archived_through() > ahead, (
            "sync never got past the archive's frontier: the log is wedged"
        )
        # And eviction is not pinned at zero by a straddling local file.
        assert revived.scan(include_archive=True).read_all().num_rows > 0


def test_an_interrupted_restore_cannot_reissue_the_primarys_offsets(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`Log.restore` has two durable writes; the order between them decides.

    `LogTable.create` is what makes a root openable, and the offset reserve is
    what makes its offsets safe. With the table first, an interruption between
    them left a root that `restore` refuses to retry and `Log.open` cheerfully
    accepts — reporting `recovery() is None`, sequence still at the replica's
    frontier, handing out offsets the dead primary had already served.

    Reversed, no interruption can produce that: before the reserve there is no
    table, so the root cannot open at all, and `restore` resumes it.
    """
    where = f"s3://{bucket}/interrupted-restore"
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        compact_min_files=2,
        wal_replication=True,
    )
    primary = tmp_path / "primary"
    with Log.new(
        primary, "s", schema=SCHEMA, config=config, archive=where, s3=s3
    ) as log:
        log.extend(rows(600))
        log.seal_due()
        log.maintain()
        log.sync()
        log.extend(rows(300))
        served = log.end_offset() - 1

    second = tmp_path / "second"
    (second / "s").mkdir(parents=True)
    source = sqlite3.connect(Layout(primary, "s").buffer_db)
    copy = sqlite3.connect(Layout(second, "s").buffer_db)
    source.backup(copy)
    source.close()
    copy.close()

    # THE INTERRUPTION, between the two durable writes. A full disk, SIGKILL,
    # SQLITE_BUSY — anything raising where the reserve happens.
    def die(self: Buffer, reserve: int) -> tuple[int, int]:
        msg = "interrupted"
        raise RuntimeError(msg)

    monkeypatch.setattr(Buffer, "strip_local_state", die)

    with pytest.raises(RuntimeError, match="interrupted"):
        Log.restore(second, "s", archive=where, s3=s3)

    monkeypatch.undo()

    # The reserve never ran, so the sequence is still the replica's. With the
    # table created FIRST this root would open, report `recovery() is None`,
    # and hand out offsets the primary already served.
    with pytest.raises(FileNotFoundError):
        Log.open(second, "s", s3=s3)

    # And the half state is resumable rather than a dead end.
    with Log.restore(second, "s", archive=where, s3=s3) as revived:
        resumed = revived.append({"event_ts": 1, "key": "k", "payload": "p"})

        assert resumed > served, (
            f"reissued offset {resumed}; the primary served through {served}"
        )
