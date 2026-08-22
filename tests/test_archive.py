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
    second = f"s3://{bucket}/elsewhere"
    with Log.open(tmp_path, "s", read_only=True, s3=s3) as reader:
        reader._archive.set_uri(second)

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
        settled = log._maintenance.archived_prefix(local)

        assert settled > 0

        # The register landed; the rows recording it did not.
        with log._buffer._lock:
            log._buffer._con.execute("DELETE FROM extent WHERE rel_path LIKE 's3://%'")
            log._buffer._con.commit()

        assert log._maintenance.archived_prefix(local) == 0, (
            "the setup must actually reproduce the crash"
        )

        log.sync()

        assert log._maintenance.archived_prefix(log._table.data_files()) == settled, (
            "the archive's manifest says what it holds; recover from it"
        )
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
            grown = writer._maintenance.archived_prefix(writer._table.data_files())

            with other._buffer._lock:
                other._buffer._con.execute(
                    "DELETE FROM extent WHERE rel_path LIKE 's3://%'"
                )
                other._buffer._con.commit()

            other.sync()

            assert (
                other._maintenance.archived_prefix(other._table.data_files()) == grown
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
            # This process's memory goes stale the way a long-running one does.
            other._archive.set_uri(None)

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
            log._archive.set_uri(f"s3://{bucket}/elsewhere")
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
            for lo, hi in log._buffer.archived_ranges(log._archive.uri or "", 0):
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
