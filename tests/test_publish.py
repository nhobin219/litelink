"""The remote tier, against a real S3-compatible endpoint (§5).

Skipped unless one is reachable — `just rustfs` brings one up locally, and the
same tests pass against AWS by pointing `AWS_ENDPOINT_URL` elsewhere. Nothing
here is mocked: the point is the parts a fake cannot exercise, which is most of
them. pyiceberg writing a catalog over object storage, `add_files` registering
by S3 URI, DuckDB reading that table back through httpfs, and the union of
three tiers that only means anything when one of them is genuinely remote.
"""

from __future__ import annotations

import inspect
import os
import shutil
import sqlite3
import subprocess
import uuid
from collections.abc import Iterable
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

import litelink
from litelink import LogConfig, WriteHandle
from litelink._buffer import Buffer
from litelink._handle import (
    OFFSET,
    PUBLISHED_RESTORE_RESERVE,
    RESTORE_RESERVE,
    LogHandle,
    table_schema,
)
from litelink._layout import Layout
from litelink._published import PUBLISHED_KEY, Published
from litelink._read import secret_sql
from litelink._s3 import S3Options
from litelink._table import (
    VERSION_HINT,
    LogTable,
    _recorded_location,
    forget_published_entry,
)
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
# offset order — which is the one thing the published rewrite depends on.
STRIDE = 7919


def scrambled(count: int) -> list[dict[str, object]]:
    return [
        {"event_ts": (i * STRIDE) % count, "key": f"k{i % 7}", "payload": "x" * 96}
        for i in range(count)
    ]


def published_log(
    root: Path, bucket: str, s3: S3Options, **overrides: object
) -> WriteHandle:
    settings: dict[str, object] = {
        "target_seal_size": 64 * 1024,
        # Conversion off, so these tests are about what reaches the published table and
        # not about what makes a file eligible. By default compaction converts
        # sealed files into ones eight times larger and `publish` waits for that,
        # which is correct and has its own test —
        # `test_only_compacted_files_are_eligible_for_the_published_table`. Leaving it
        # on here would mean every published table test first had to produce eight
        # seals' worth of rows to observe anything.
        "target_compact_size": 64 * 1024,
        "compact_min_files": 2,
        "staging_snapshot_retention": timedelta(seconds=0),
        "published_snapshot_retention": timedelta(seconds=0),
    }
    settings.update(overrides)
    config = LogConfig(**settings)  # ty: ignore[invalid-argument-type]

    return litelink.new(
        root,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        published=f"s3://{bucket}/prefix",
        s3_options=s3,
    )


def test_publish_pushes_sealed_files_and_records_the_watermark(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """§5 steps 1-3: upload, register, record — and the watermark is what I4
    later reads to decide what may be evicted."""
    with published_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal()
        sealed = log._table.data_files()
        assert sealed, "the fixture must produce sealed files to push"

        log.publish()

        remote = log._published.require()
        assert remote.span() == (1, sealed[-1].end), (
            "the published table must cover them"
        )
        assert int(log._buffer.get_meta("published_through") or 0) == (
            sealed[-1].end - 1
        )


def test_a_read_spans_published_staging_and_buffer(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The union that is the whole point: rows evicted from local disk are
    still readable, exactly once, alongside rows that never left.

    `staging_retention=0` evicts everything the published table holds as soon as it holds
    it, so by the end the only local rows are the unsealed tail — and a merged
    read must still return the full stream with no gap at either seam.
    """
    with published_log(tmp_path, bucket, s3, staging_retention=timedelta(0)) as log:
        log.extend(rows(ROWS))
        log.seal()
        before = log.staging_files()
        log.publish()
        log.advance()

        assert log.staging_files() < before, "eviction must have removed local files"
        assert log.staging_extent() is None, (
            "the fixture must evict the staging tier dry"
        )

        # `scan()` reaches the published table of its own accord (#90): nothing below
        # the staging table is on local disk, and the query asks for all of it.
        # This once asserted a SHORT read here, which was the defect rather
        # than the design — measured then, 476 of 1,500 rows with no error.
        merged = log.sql("SELECT * FROM log").read_all()
        offsets = merged.column(OFFSET).to_pylist()
        assert sorted(offsets) == list(range(1, ROWS + 1)), (
            "every offset exactly once, no duplicate across tiers and no gap"
        )


def test_a_hot_read_never_touches_the_published_table(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """I5, asserted rather than assumed, with the published table genuinely unreachable.

    Part of the log is evicted, so the published table holds rows local disk does not.
    Then the credentials point at a dead endpoint: a read bounded inside the
    staging window must still be served, because the published table's tier row says
    nothing below the staging table matches it — and a read that reaches into
    the evicted history must FAIL rather than come back short.

    Falsify by returning `(True, True)` from `Reader._tiers`: the bounded read
    raises. Or `(True, False)`: the unbounded one returns the local rows alone.
    """
    with published_log(
        tmp_path, bucket, s3, staging_retention=timedelta(0), staging_rows=1000
    ) as log:
        log.extend(rows(ROWS))
        log.seal(flush=True)
        log.publish(flush=True)
        log.advance()
        extent = log.staging_extent()
        assert extent is not None
        assert 1 < extent[0], "the fixture must evict part of the log"

        log._published._s3 = S3Options(
            endpoint="http://127.0.0.1:1",
            access_key="nobody",
            secret_key="nothing",
            region="us-east-1",
        )
        log._published._handle = None

        hot = log.scan(start_offset=extent[0]).read_all()
        assert hot.num_rows == ROWS - extent[0] + 1

        with pytest.raises(Exception, match=r".+"):
            log.scan().read_all()


def test_only_settled_files_reach_the_published_table(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The rule that keeps the published table well-sized by construction.

    An explicit `seal()` cuts wherever the buffer happens to be, so it can emit
    a file holding far less than the target. Pushed, it would sit in object
    storage as an undersized file nothing local can merge away — the published table
    would need a repair pass to fix a sizing decision made here. So a trailing
    small file stays behind, and the watermark stops short of it.

    "Small" is measured in what the file HOLDS, not what it cost to store. The
    payload here compresses about eight to one, so every file looks starved
    beside a target stated in uncompressed bytes, and a rule reading sizes off
    disk pushed nothing at all.
    """
    with published_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal()
        log.extend(rows(4))
        log.seal(flush=True)

        files = log._table.data_files()
        held = log._maintenance.memory()
        assert held[files[-1].path] < log.config.target_seal_size, (
            "the tail must hold less than a full target to test this"
        )

        log.publish()

        assert int(log._buffer.get_meta("published_through") or 0) == (
            files[-2].end - 1
        )
        assert log._published.require().span() == (1, files[-2].end)


def test_a_transient_failure_does_not_replace_the_published_table(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A published table that cannot be READ is not a published table that is not there.

    Opening it used to catch everything and rebuild, so a 503, a timeout or an
    expired token was taken for "no table" — and the repair dropped the only
    pointer to a live published table and wrote an empty one over it, while the
    watermark went on telling eviction those rows were safe elsewhere.

    Whether the entry belongs to this prefix is answered from the local catalog
    row, offline, so a genuine mismatch is still detected without reading the
    bucket at all. Only a failed read of OUR OWN metadata reaches here, and
    that is an error.
    """
    with published_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal()
        log.publish()
        where = log.published
        assert where is not None
        recorded = _recorded_location(log._layout)
        assert recorded is not None and recorded.startswith(f"{where}/")

    fs = filesystem(s3)
    before = len(fs.find(where.removeprefix("s3://")))
    assert before > 0

    unreadable = replace(s3, access_key="wrong", secret_key="wrong")
    with pytest.raises(Exception, match=r".+"):
        LogTable.open_published(
            Layout(tmp_path, "s"), where, unreadable, table_schema(SCHEMA)
        )

    assert len(fs.find(where.removeprefix("s3://"))) == before, (
        "an unreadable published table must not be replaced"
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
    makes the published table genuinely absent, testing something else entirely.
    """
    with published_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal()
        log.publish()
        where = log.published
        assert where is not None
        expected = _recorded_location(log._layout)
        assert expected is not None

    def unanswerable(_: Layout) -> str | None:
        msg = "cannot read the published catalog's own table"
        raise LookupError(msg)

    monkeypatch.setattr("litelink._table._recorded_location", unanswerable)
    table = LogTable.open_published(
        Layout(tmp_path, "s"), where, s3, table_schema(SCHEMA)
    )

    assert table.metadata_location == expected, (
        "the existing published table must be found, not replaced"
    )


def test_a_read_never_repairs_the_published_catalog(
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
    with published_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal()
        log.publish()
        first = log.published
        assert first is not None

    # The entry now names `first`, while the log is pointed at `second`.
    # Written straight to `meta` — the state a pre-0.7 re-point that crashed
    # half done leaves — so nothing has had the chance to repair the entry
    # before the read sees it.
    second = f"s3://{bucket}/elsewhere"
    with litelink.open(tmp_path, "s", s3_options=s3) as writer:
        writer._buffer.set_meta("published", second)

    with litelink.open(tmp_path, "s", read_only=True, s3_options=s3) as reader:
        with pytest.raises(ValueError, match="not under"):
            reader._published.table()

    assert _recorded_location(Layout(tmp_path, "s")) is not None, (
        "a read must not have dropped the entry"
    )


def test_a_read_before_the_first_publish_simply_has_no_published_leg(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Absent is not wrong. A log configured with a published table nothing has pushed
    to yet reads without that leg, and creates nothing by reading."""
    with published_log(tmp_path, bucket, s3) as log:
        log.extend(rows(200))
        log.seal()

        merged = log.sql("SELECT * FROM log").read_all()

        assert merged.num_rows == 200
        assert _recorded_location(log._layout) is None, (
            "reading must not create the published table"
        )


def test_a_read_against_a_never_published_published_writes_nothing(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """ "Creates nothing" has to mean locally too.

    Constructing the catalog creates its tables in `published.db`, and
    registering the namespace adds a row — writes, from a path that promised to
    make none. So absence is decided from the catalog file before one is built,
    and a reader against a published table nothing has pushed to touches nothing.
    """
    with published_log(tmp_path, bucket, s3) as log:
        log.extend(rows(200))
        log.seal()
        assert not log._layout.published_db.exists()

        merged = log.sql("SELECT * FROM log").read_all()

        assert merged.num_rows == 200
        assert not log._layout.published_db.exists(), (
            "a read must not create the published catalog"
        )


def test_a_failed_repoint_puts_the_old_catalog_entry_back(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A half-done move that leaves NEITHER published table is worse than not moving.

    The repair drops the entry and creates a table at the new prefix, and
    creating can fail — configuring a published table deliberately does not require
    the bucket to exist yet. A drop with no create destroys the only record of
    where the previous published table's metadata is, and
    `previous_metadata_location` dies with the row, so rolling back would build
    an empty table over data nothing could then reach.
    """
    with published_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal()
        log.publish()
        original = _recorded_location(log._layout)
        assert original is not None

    # A prefix in a bucket that does not exist, so the create must fail.
    missing = "s3://litelink-no-such-bucket-3f9a2/elsewhere"
    with pytest.raises(Exception, match=r".+"):
        LogTable.open_published(
            Layout(tmp_path, "s"), missing, s3, table_schema(SCHEMA), repair=True
        )

    assert _recorded_location(Layout(tmp_path, "s")) == original, (
        "a failed repair must leave the previous published table reachable"
    )


def test_drain_never_deletes_outside_the_logs_own_published_table(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A queued remote path outside the log's prefix is left alone.

    The published location is fixed at creation now (#118), but a log written
    before could re-point, and its queue can still name objects of the table
    it left — checked against this log's table, they look unreferenced, while
    they may be live, and the only copy of rows evicted elsewhere.

    Falsify by dropping the prefix guard in `_drain_published`: the other
    table's live object is deleted.
    """
    fs = filesystem(s3)
    elsewhere = litelink.new(
        tmp_path / "elsewhere",
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        published=f"s3://{bucket}/retired",
        s3_options=s3,
    )
    with elsewhere:
        elsewhere.extend(rows(ROWS))
        elsewhere.seal(flush=True)
        elsewhere.publish(flush=True)

    objects = fs.find(f"{bucket}/retired")
    assert objects

    with published_log(tmp_path / "log", bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal(flush=True)
        log.publish(flush=True)

        # Another table's live object, queued as a pre-0.7 re-point leaves it.
        stranded = f"s3://{objects[0]}"
        log._buffer.enqueue_deletions([stranded], 0)
        log._maintenance.drain_published()

        assert fs.exists(objects[0]), (
            "drain must not delete outside the log's own published table"
        )
        assert stranded in log._buffer.queued_deletions(), (
            "and it stays queued for whoever owns that published table"
        )


def test_a_trailing_slash_does_not_wedge_the_remote_queue(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The configured URI may carry a trailing slash; queued paths never do.

    Every remote path is built from the warehouse with its slashes stripped, so
    comparing against the URI verbatim classifies this log's OWN objects as
    another published table's — and the guard that exists to protect a retired bucket
    instead stops the queue draining at all, for ever.
    """
    with published_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal()
        log.publish()

        fs = filesystem(s3)
        objects = fs.find(f"{bucket}/prefix")
        assert objects

        # An object the published table does NOT reference, chosen deliberately. This
        # used to take `objects[0]`, which passed on listing order rather than
        # on the guard under test: the first key happened to be an unreferenced
        # metadata object, so when the layout changed and a live data file
        # sorted first, the drain refused for the referenced-file veto — a
        # correct refusal, with nothing to do with slashes.
        remote = log._published.table(repair=True)
        assert remote is not None
        referenced = remote.referenced_paths()
        doomed = next(
            f"s3://{obj}" for obj in objects if f"s3://{obj}" not in referenced
        )
        log._buffer.enqueue_deletions([doomed], 0)

        # The slash goes into `meta` DIRECTLY, because every public way in
        # normalises it away: `new` and `open` both `rstrip("/")`
        # before storing. Written through any of them this test cannot fail —
        # verified by deleting the guard's own `rstrip` and watching it still
        # pass — so it proved nothing about the guard it names.
        #
        # A durable value with a trailing slash is still reachable: it is what
        # a log written by a version that normalised somewhere else carries,
        # and `drain` reads `meta`, not the argument someone once passed.
        log._buffer.set_meta(PUBLISHED_KEY, f"s3://{bucket}/prefix/")
        assert log._published.uri == f"s3://{bucket}/prefix/"

        log._maintenance._drain_published(remote, None)

        assert doomed not in log._buffer.queued_deletions(), (
            "the log's own published object must be drainable"
        )


def test_a_register_whose_rows_never_landed_is_recovered_from_the_manifest(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The crash window I4-per-segment has, and how it closes (§4a).

    The row naming a file's published copy is written AFTER the register, so a
    crash between the two leaves the published table holding a range nothing local
    records. Compaction decides from those rows, so it would merge that file
    into one spanning the published table's extent, and the next push would register a
    partially overlapping range — which `register` admits, because it declines
    only a range entirely covered.

    Nothing is promised beforehand to cover it. The published table's own manifest is
    the truth, and the next push reads it anyway, so recovery is a backfill.
    """
    with published_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal()
        log.publish()
        local = log._table.data_files()
        settled = log._maintenance.published_prefix(local, include_intents=False)

        assert settled > 0

        # The register landed; the rows recording it did not.
        with log._buffer._lock:
            log._buffer._con.execute("DELETE FROM meta WHERE k = 'published_through'")
            log._buffer._con.commit()

        assert log._maintenance.published_prefix(local, include_intents=False) == 0, (
            "the setup must actually reproduce the crash"
        )

        log.publish()

        assert (
            log._maintenance.published_prefix(
                log._table.data_files(), include_intents=False
            )
            == settled
        ), "the published table's manifest says what it holds; recover from it"
        assert log.scan().read_all().num_rows == ROWS


def test_the_backfill_sees_copies_another_process_pushed(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The manifest is read as it IS, not as this handle last saw it.

    A pyiceberg handle is a frozen snapshot and `Published` caches it for the
    life of the process, so a maintainer that published, released the lease and
    took it back would otherwise recover against a published table that has since
    grown — and go on treating another maintainer's pushes as unpublished.
    """
    with published_log(tmp_path, bucket, s3) as writer:
        writer.extend(rows(ROWS))
        writer.seal()
        writer.publish()

        with litelink.open(tmp_path, "s", s3_options=s3) as other:
            # `other` caches its published handle here, at today's extent.
            assert other.published_files() > 0

            writer.extend(rows(ROWS))
            writer.seal()
            writer.publish()
            grown = writer._maintenance.published_prefix(
                writer._table.data_files(), include_intents=False
            )

            with other._buffer._lock:
                other._buffer._con.execute(
                    "DELETE FROM meta WHERE k = 'published_through'"
                )
                other._buffer._con.commit()

            other.publish()

            assert (
                other._maintenance.published_prefix(
                    other._table.data_files(),
                    include_intents=False,
                )
                == grown
            ), "recovered against a stale manifest"


def test_a_commit_retry_will_not_follow_the_catalog_to_another_published_table(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The Iceberg commit is the one durable write with no watermark fence.

    `_commit` reloads and retries when the branch moves under it, and the
    catalog row is keyed by table id rather than by identity — so a re-point
    racing a slow register makes the reload re-bind the operation to the NEW
    published table, and the retry commits paths that live in the old bucket.
    """
    with published_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal()
        log.publish()

        published = log._published.require()
        moved = Layout(tmp_path, "s").warehouse_uri
        published._warehouse = f"s3://{bucket}/somewhere-else"

        with pytest.raises(RuntimeError, match="moved out of"):
            published._verify_identity()

        assert moved


def test_the_published_table_refuses_a_range_that_starts_inside_its_extent(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The last line of defence, and the only one not reasoned around.

    `_covers` declines a range entirely covered, which makes a replayed push
    harmless. A range that starts inside the extent and ends beyond it is a
    different thing: those offsets land in two files at once, in the immutable
    tier, with nothing able to repair it. Everything upstream is arranged so a
    merge never straddles the published table's extent, and every gap found in that
    arrangement has been a fresh piece of reasoning — this check holds however
    the reasoning turns out.
    """
    with published_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal()
        log.publish()
        published = log._published.require()
        published.reload()
        covered = published.span()

        assert covered is not None

        # A file whose range begins inside what the published table already
        # holds: at its last offset.
        with pytest.raises(ValueError, match="two files at once"):
            published.register(["s3://nowhere/straddle.parquet"], start=covered[1] - 1)

        # And one that ENGULFS it — starting below the extent and running past
        # it. This is the worse shape, not an excused one: it puts every
        # published offset in two files rather than some of them.
        with pytest.raises(ValueError, match="two files at once"):
            published.register(
                ["s3://nowhere/engulf.parquet"], start=max(covered[0] - 5, 0)
            )

        # And one that begins cleanly above it is not refused by this check.
        published._refuse_straddle(covered[1])


def test_the_published_hint_names_the_metadata_the_commit_produced(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The hint has to name the metadata the table is actually at.

    Asserted against the pointer directly: a round trip through the hint
    recovers one that is one version stale often enough to pass, because the
    missing snapshot's rows may still be in the staging tier.

    It does NOT pin publish-after-reload. That was the intent, and falsifying
    it showed the ordering is not observable: pyiceberg updates the handle in
    place when a commit lands, so publishing before `_commit`'s reload writes
    the same hint. The claim the code makes has been narrowed to match.
    """
    where = f"s3://{bucket}/hinted"
    with litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        config=replace(LogConfig(), target_seal_size=8 * 1024, compact_min_files=2),
        published=where,
        s3_options=s3,
    ) as log:
        log.extend(rows(400))
        log.seal()
        log.advance()
        log.publish()

        published = log._published.require()  # noqa: SLF001
        published.reload()
        current = str(published.metadata_location)

    fs = filesystem(s3)
    hint = fs.cat(f"{bucket}/hinted/s/metadata/{VERSION_HINT}")

    assert current.endswith(f"/{hint.decode().strip()}.metadata.json"), (
        f"hint {hint!r} does not name {current!r}"
    )


def test_the_published_table_reads_as_a_directory_with_no_catalog_at_all(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """What the hint buys beyond re-attach: a published table nothing local can read.

    litelink resolves the published table through `published.db` and hands DuckDB a
    metadata path (§7). This is the other reader — an engine pointed at the
    prefix, with no catalog, no local root, and nothing but the bucket.

    **`version_name_format` is required, and that is the documented cost.**
    DuckDB's default is the Hadoop `v%s%s.metadata.json`; pyiceberg names its
    metadata `00003-<uuid>.metadata.json`, so the hint holds that stem and the
    format has to stop prepending a `v`. Writing a second copy under the
    Hadoop name would remove the parameter and add an object per commit that
    nothing collects — see `VERSION_HINT`.
    """
    where = f"s3://{bucket}/standalone"
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        target_compact_size=16 * 1024,
        compact_min_files=2,
        staging_rows=200,
    )
    with litelink.new(
        tmp_path, "s", schema=SCHEMA, config=config, published=where, s3_options=s3
    ) as log:
        for _ in range(4):
            log.extend(rows(400))
            log.seal()
            log.advance()
            log.publish()

        published = log.published_through()

    assert published > 0, "nothing reached the published table to read back"

    # The documented way another machine provisions DuckDB for this (#108).
    connection = litelink.duckdb_connection(s3_options=s3)
    # `{prefix}/{name}`, which is the table location itself now. It used to be
    # `{prefix}/litelink/{name}` — pyiceberg's `<warehouse>/<namespace>/<table>`
    # default — while the data files sat at `{prefix}/{name}/data`, so an engine
    # pointed at the table read metadata from one directory describing files in
    # another.
    directory = f"{where}/s"
    rows_read = connection.execute(
        f"SELECT count(*) FROM iceberg_scan('{directory}',"
        " version_name_format = '%s%s.metadata.json')"
    ).fetchone()

    assert rows_read is not None
    assert rows_read[0] == published


def _crash_before_recording(log: WriteHandle) -> None:
    """Publish, dying between the register and the confirm that records it —
    the call naming the copies that landed. Reconciliation's own confirm,
    before anything is uploaded, names none and goes through."""
    original = Buffer.confirm_published

    def dying(self: Buffer, through: int, landed: Iterable[str] = ()) -> None:
        paths = list(landed)
        if paths:
            msg = "crash between the register and the record"
            raise RuntimeError(msg)

        original(self, through, paths)

    Buffer.confirm_published = dying
    try:
        with pytest.raises(RuntimeError, match="crash between"):
            log.publish()

    finally:
        Buffer.confirm_published = original


def test_a_register_without_its_rows_cannot_wedge_the_log(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The window two polarities exist to close.

    A register lands and the rows recording it do not. Compaction decides what
    it may merge from those rows, so a compaction-target change before the next
    publish regroups the pushed-but-unrecorded files and commits a LOCAL file
    straddling the published table's extent — after which every push is refused for
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
        staging_snapshot_retention=timedelta(seconds=0),
        published_snapshot_retention=timedelta(seconds=0),
    )
    log = litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        published=f"s3://{bucket}/prefix",
        s3_options=s3,
    )
    with log:
        for _ in range(3):
            log.extend(rows(400))
            log.seal()
            log.compact()

        _crash_before_recording(log)

        published = log._published.require()
        published.reload()
        extent = published.span()
        intents = log._buffer.intents(log._published.uri or "")

        assert extent is not None
        assert intents, "the crash must leave the intents behind"

        local = log._table.data_files()

        assert log._maintenance.published_prefix(local, include_intents=True) > 0
        assert log._maintenance.published_prefix(local, include_intents=False) == 0, (
            "eviction must not see an intended copy as a landed one"
        )

        # The ingredient that turns the crash into a permanent stall.
        log.set_config(replace(config, target_compact_size=1 << 20))
        log.compact()

        assert all(
            f.start >= extent[1] or f.end <= extent[1] for f in log._table.data_files()
        ), "merged across the published table's extent"

        log.publish()

        assert not log._buffer.intents(log._published.uri or ""), (
            "intents not reconciled"
        )
        assert log.scan().read_all().num_rows == 1200


def test_eviction_never_acts_on_an_intended_copy(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The understating half of the split, on its own.

    An intent says a copy is coming, not that it is there. Eviction deletes the
    only local copy on the strength of what it reads, so reading an intent as
    coverage is the loss this whole record exists to prevent — and it is the
    direction a `confirmed` column would have handed an older build for free.
    """
    config = replace(LogConfig(), staging_rows=50, target_seal_size=1 << 30)
    log = litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        published=f"s3://{bucket}/prefix",
        s3_options=s3,
    )
    with log:
        # Several files, so the row floor lands on an edge below the head —
        # with one file there is no boundary to snap to and eviction returns
        # early whatever it believes about coverage.
        for _ in range(3):
            log.extend(rows(100))
            log.seal(flush=True)

        before = len(log._table.data_files())

        assert before == 3

        for data_file in log._table.data_files():
            log._buffer.intend_file(
                f"s3://{bucket}/prefix/data/{data_file.start}.parquet",
                data_file.start,
                data_file.end,
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
    with published_log(tmp_path, bucket, s3) as log:
        path = f"s3://{bucket}/prefix/data/contested.parquet"

        log._buffer.intend_file(path, 1, 101, 4096)
        # The other owner, intending the same path with its own view of it.
        log._buffer.intend_file(path, 1, 101, 8192)

        intents = log._buffer.intents(f"s3://{bucket}/prefix")

        assert len(intents) == 1, "one path, one intent"
        assert intents[0] == (path, 1, 101, 8192), "the later intent must win"


def test_a_healed_crash_raises_the_watermark_and_retires_the_intents(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """After a crash between the register and the confirm, the next publish
    reconciles against the published table's manifest: the watermark reaches
    the span's end and every intent the manifest holds is retired, together.

    Falsify by dropping reconciliation's `confirm_published`: the watermark
    stays where the crash left it and the intents stay behind.
    """
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        target_compact_size=16 * 1024,
        compact_min_files=2,
        staging_snapshot_retention=timedelta(seconds=0),
        published_snapshot_retention=timedelta(seconds=0),
    )
    log = litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        published=f"s3://{bucket}/prefix",
        s3_options=s3,
    )
    with log:
        for _ in range(3):
            log.extend(rows(400))
            log.seal()
            log.compact()

        _crash_before_recording(log)

        published = log._published.require()
        published.reload()
        span = published.span()

        assert span is not None
        assert log._buffer.intents(log._published.uri or ""), "the crash left intents"
        assert log.published_through() < span[1] - 1, "the crash left the watermark"

        log.publish()

        assert log.published_through() >= span[1] - 1
        assert not log._buffer.intents(log._published.uri or ""), (
            "intents the manifest holds were not retired"
        )


def test_replication_holds_sealed_rows_until_the_published_table_has_them(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """§3a's middle hole, closed. I4 one tier up.

    A seal moves rows from SQLite into a Parquet file that no sidecar
    replicates, so with WAL shipping on, dropping them at seal removes the only
    off-box copy of a range the published table does not hold yet. The machine dying in
    that window loses them from the MIDDLE of the offset space: below the seal
    frontier so the buffer no longer has them, above the published frontier so
    the bucket does not either.

    So they stay until publish has pushed the range, and only then go.
    """
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        target_compact_size=16 * 1024,
        compact_min_files=2,
        wal_replication=True,
    )
    with litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        config=config,
        published=f"s3://{bucket}/held",
        s3_options=s3,
    ) as log:
        log.extend(rows(1200))
        log.seal()

        sealed = log.staging_extent()

        assert sealed is not None, "nothing sealed, so the case is not set up"
        # The buffer's FLOOR is the measure, not its size: `count_from(0)`
        # counts the unsealed tail too, which is in the buffer either way.
        buffered = log._buffer.span()  # noqa: SLF001

        assert buffered is not None
        assert buffered[0] < sealed[1], (
            "the seal dropped rows the published table does not have yet"
        )
        # And a read is unaffected, which is what makes holding them affordable:
        # the buffer leg is bounded by the staging table's committed extent.
        assert log.scan().read_all().num_rows == 1200

        log.advance()
        log.publish()
        published = log.published_through()

        assert published > 0, "nothing reached the published table"
        # Released only up to the PUBLISHED table's frontier, never the seal's.
        released = log._buffer.span()  # noqa: SLF001

        assert released is not None
        assert released[0] > published, (
            "rows the published table holds were never released"
        )
        assert log.scan().read_all().num_rows == 1200


def test_without_replication_eviction_does_not_wait_for_the_published_table(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A published table alone is not the trigger.

    Without a sidecar the buffer and the Parquet share a disk and die together,
    so holding buys nothing and costs SQLite growth. The gate is
    `wal_replication`, and this is the half that proves a published table by
    itself does not flip it: `evict("buffer")` drops what a seal committed to
    staging, with nothing published yet (#122).

    Falsify by having `evict_buffer` use the published table's coverage
    whenever one is remote: nothing is published, so nothing is dropped.
    """
    config = replace(LogConfig(), target_seal_size=8 * 1024, compact_min_files=2)
    with litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        config=config,
        published=f"s3://{bucket}/unheld",
        s3_options=s3,
    ) as log:
        log.extend(rows(1200))
        log.seal()
        log.evict("buffer")

        sealed = log.staging_extent()

        assert sealed is not None
        buffered = log._buffer.span()  # noqa: SLF001
        # Either the buffer is empty, or what is in it is strictly the unsealed
        # tail — never a row the seal already wrote to Parquet.
        assert buffered is None or buffered[0] >= sealed[1], (
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
    `iceberg_scan` so the overlap is returned twice, and the next publish refuses
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
    with litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        config=config,
        published=f"s3://{bucket}/widen",
        s3_options=s3,
    ) as log:
        for _ in range(3):
            log.extend(rows(600))
            log.seal()

        files = sorted(log._table.data_files(), key=lambda f: f.start)  # noqa: SLF001

        assert len(files) > 2, "not enough seals to have a second one to widen"
        # Contiguous and non-overlapping (§4, §6). A widened file starts at the
        # log's floor instead of its own group's, so every later file overlaps
        # every earlier one.
        for earlier, later in zip(files, files[1:], strict=False):
            assert later.start == earlier.end, (
                f"{later.start} does not follow {(earlier.end - 1)}: ranges overlap"
            )

        # And the read agrees, which is what the overlap would break.
        assert log.scan().read_all().num_rows == 1800
        assert log.scan().read_all().column(OFFSET).to_pylist() == list(range(1, 1801))


def test_the_published_table_declares_the_same_sort_order_as_the_log(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """§4: the order is declared as table metadata, on BOTH tiers.

    `open_published` never declared one, so a published table holding clustered data
    said nothing about it — a table lying about itself to any reader that is
    not this library, and the reason `sort_by` was unanswerable from the
    published table alone.
    """
    config = replace(LogConfig(), target_seal_size=8 * 1024, compact_min_files=2)
    with litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        published=f"s3://{bucket}/sorted",
        s3_options=s3,
    ) as log:
        log.extend(scrambled(600))
        log.seal()
        log.advance()
        log.publish()

        published = log._published.require()  # noqa: SLF001

        assert published.sort_by() == ("event_ts",), (
            "the published table holds clustered data and declares no order"
        )
        # And it still holds the rows, so declaring the order did not disturb
        # the create/publish sequence around it.
        assert log.published_through() > 0


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
    with litelink.new(
        primary,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        published=where,
        s3_options=s3,
    ) as log:
        log.extend(rows(1200))
        log.seal()
        log.advance()
        log.publish()
        # More on top, sealed but never published: the band that used to be lost.
        log.extend(rows(400))
        log.seal()
        published = log.published_through()
        replicated = log.end_offset() - 1
        served = log.scan().read_all().num_rows

        assert published < replicated, "nothing left unsynced, so the band is untested"

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
    with litelink.open(primary, "s", s3_options=s3) as log:
        log.extend(rows(50))
        written = log.end_offset() - 1

    assert written > replicated, "the unreplicated tail did not happen"

    # The machine dies.
    second = tmp_path / "second"

    with litelink.restore(
        second, "s", published=where, s3_options=s3, binary=str(binary)
    ) as revived:
        report = revived.recovery()

        assert report is not None
        # Every row is readable again — including the sealed-but-unsynced band,
        # which survives because a seal keeps its rows until the published table has
        # them when wal_replication is on.
        assert revived.scan().read_all().num_rows == served
        # The shape came from `meta`, not from the catalog that was not restored.
        assert revived.sort_by == ("event_ts",)  # noqa: SLF001
        assert revived.config.target_seal_size == 8 * 1024
        # And it resumes ABOVE everything the primary ever assigned, so no
        # offset the dead machine served is handed to different data.
        resumed = revived.append({"event_ts": 1, "key": "k", "payload": "p"})

        assert resumed > written, f"reissued offset {resumed}, primary served {written}"
        assert report.skipped[1] - report.skipped[0] == RESTORE_RESERVE

        # And it goes on working. `advance` is where a stale local `extent`
        # row would surface: compaction reads those rows to decide what to
        # merge, and they name Parquet that is on the machine that died.
        revived.seal()
        revived.advance()
        revived.publish()

        assert revived.scan().read_all().num_rows == served + 1

        # And this database describes a filesystem that exists. Every local
        # path it names is a file that is here — the dead machine's are gone.
        for path in revived._buffer.file_bytes():  # noqa: SLF001
            if "://" not in path:
                assert (second / path).exists(), (
                    f"names a file that is not here: {path}"
                )


def test_a_stale_published_catalog_reads_short_until_it_is_dropped(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The measured 261-vs-1061 case, and the one line that fixes it.

    `open_published` consults `version-hint.text` only when the catalog has NO
    row for the table. With a stale row present it calls `load_table` on
    whatever that names — and old metadata JSONs survive in the bucket until
    expiry, so the load SUCCEEDS and reports the published table as it was several
    publishes ago. Silent, and in the losing direction.

    Worse than under-reading: the next publish commits onto that lineage and
    `publish_pointer` republishes the hint over the fork, destroying the
    pointer a later recovery depends on.

    Both halves are asserted here — the hazard, so it stays documented, and
    that `forget_published_entry` removes it. `WriteHandle.restore` calls that before
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
    with litelink.new(
        root, "s", schema=SCHEMA, config=config, published=where, s3_options=s3
    ) as log:
        log.extend(rows(600))
        log.seal()
        log.advance()
        log.publish()
        early = log.published_files()

    # A replica of `published.db` taken here — the state a WAL restore would
    # bring back — and then the published table grows past it.
    stale = tmp_path / "stale-archive.db"
    shutil.copyfile(layout.published_db, stale)

    with litelink.open(root, "s", s3_options=s3) as log:
        for _ in range(3):
            log.extend(rows(600))
            log.seal()
            log.advance()
            log.publish()

        current = log.published_files()

    assert current > early, (
        "the published table did not grow, so staleness is untestable"
    )

    # The hazard: the old catalog wins over the bucket's own pointer.
    shutil.copyfile(stale, layout.published_db)
    with litelink.open(root, "s", s3_options=s3) as log:
        assert log.published_files() == early, (
            "expected the stale catalog to be believed; the case has changed"
        )

    # And the fix, which is what `restore` does before it opens anything.
    assert forget_published_entry(layout), "there was no entry to drop"

    with litelink.open(root, "s", s3_options=s3) as log:
        # A reader may not adopt — that is a write to `published.db` — so it
        # still sees nothing until a repairing caller runs.
        assert log.published_files() == 0
        log.publish()

        assert log.published_files() == current, (
            "adoption did not recover the published table"
        )


def test_forgetting_a_published_entry_that_is_not_there_is_a_no_op(
    tmp_path: Path,
) -> None:
    """A fresh restore has no `published.db` at all, which is the ordinary case.

    Constructing a `SqlCatalog` to drop one row would create that catalog's own
    tables as a side effect — a write, from a path whose whole purpose is to
    leave less behind.
    """
    layout = Layout(tmp_path, "s")

    assert forget_published_entry(layout) is False
    assert not layout.published_db.exists(), "it created the database it was checking"


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
    with litelink.new(
        primary, "s", schema=SCHEMA, config=config, published=where, s3_options=s3
    ) as log:
        log.extend(rows(800))
        log.seal()
        log.advance()
        log.publish()
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
    with litelink.open(second, "s", s3_options=s3) as revived:
        revived._published.table(repair=True)  # noqa: SLF001
        # `seal()`, not `seal()`. The recovered group is OPEN — `_seed_group`
        # builds it, and the appender never cut it — so `seal` drains
        # nothing and the overlap never materialises. Closing it is what the
        # next real seal on that box would do.
        revived.seal(flush=True)
        revived.advance()

        offsets = revived.scan().read_all().column(OFFSET)

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
    rows the published table had not taken — the only off-box copy — while the sibling
    exit eighteen lines below passed the flag correctly. A seal no longer
    deletes rows on any exit (#122); this keeps both honest.
    """
    where = f"s3://{bucket}/recovered"
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        compact_min_files=2,
        wal_replication=True,
    )
    with litelink.new(
        tmp_path, "s", schema=SCHEMA, config=config, published=where, s3_options=s3
    ) as log:
        log.extend(rows(600))
        group = log._buffer.pending_group()  # noqa: SLF001

        assert group is not None

        start, end = group
        rel_path = log._layout.seal_path(start, end, "tok")
        log._buffer.claim_seal(start, end, rel_path)  # noqa: SLF001
        # Committed, not retired: the crash lands between the two.
        log._write_and_commit(start, end, rel_path)

        held = log._buffer.count_from(1)  # noqa: SLF001

        assert held > 0

        log.recover()

        assert log._buffer.count_from(1) == held, (  # noqa: SLF001
            "recovery deleted rows the published table has not been sent"
        )


def test_creating_a_log_on_another_logs_published_table_is_refused(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A populated published table this log never pushed to belongs to another log.

    Offsets cannot tell them apart — two logs of the same name both start at 1,
    so a foreign published table whose ranges sit BELOW this log's next offset passes
    the "is it ahead of us" check. What tells them apart is that a log which
    pushed to a published table keeps its `extent` rows naming that prefix across a
    detach (§4a).

    Refused at the door rather than contained afterwards, and two attempts to
    contain it are why. `_push` raises the watermark to the published table's extent on
    every pass, and its backfill writes `extent` rows for the published table's ENTIRE
    manifest within one publish — so by the time anything downstream looks, the
    foreign published table's ranges ARE this log's records. Measured: a bound derived
    from either moved with the contamination.

    `litelink.new(published=...)` is the only way to point a log now (#118), and it
    is exactly what an operator reaches for when failing over by hand.
    """
    foreign = f"s3://{bucket}/foreign"
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        compact_min_files=2,
        wal_replication=True,
    )
    # Someone else's log, which fills that prefix.
    with litelink.new(
        tmp_path / "owner",
        "s",
        schema=SCHEMA,
        config=config,
        published=foreign,
        s3_options=s3,
    ) as owner:
        owner.extend(rows(1200))
        owner.seal()
        owner.advance()
        owner.publish()

        assert owner.published_through() > 0, (
            "the foreign published table holds nothing"
        )

    # Creating a log against it — the failover-by-hand shape.
    with pytest.raises(ValueError, match="no record of pushing"):
        litelink.new(
            tmp_path / "mine",
            "s",
            schema=SCHEMA,
            config=config,
            published=foreign,
            s3_options=s3,
        )

    assert not (tmp_path / "mine" / "s" / "buffer.db").exists(), (
        "the refusal left a half-built log behind"
    )


def test_a_restore_from_a_replica_the_published_table_has_outrun(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A replica is a snapshot from BEFORE the primary's last publish.

    That is ordinary replication lag, not a crash window: the bucket routinely
    holds ranges the replicated `extent` rows do not mention. Seeded from the
    replica alone, the open group starts below the published table's frontier, and the
    first seal here writes a file reaching into the published table's extent.

    Which wedges the log for good — `_refuse_straddle` raises on every push,
    `published_prefix` returns 0 for the straddler so eviction pins at zero, and
    local disk grows without bound. Nothing re-cuts a local straddler, and this
    is the operation you run when the published table is the only surviving copy.

    Asserted by DOING the work a revived box does — seal, maintain, publish,
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
    with litelink.new(
        primary, "s", schema=SCHEMA, config=config, published=where, s3_options=s3
    ) as log:
        log.extend(rows(800))
        log.seal()
        log.advance()
        log.publish()

        # THE SNAPSHOT, taken here — before the publish below. This is the lag.
        second = tmp_path / "second"
        (second / "s").mkdir(parents=True)
        source = sqlite3.connect(Layout(primary, "s").buffer_db)
        copy = sqlite3.connect(Layout(second, "s").buffer_db)
        source.backup(copy)
        source.close()
        copy.close()

        # The primary carries on: more rows, sealed and PUSHED. The published table is
        # now ahead of everything the snapshot knows about.
        log.extend(rows(800))
        log.seal()
        log.advance()
        log.publish()
        ahead = log.published_through()

    stale = Buffer.peek_meta(Layout(second, "s").buffer_db, "published_through")

    assert stale is not None and int(stale) < ahead, (
        "the published table did not outrun the snapshot, so the case is not set up"
    )

    # Restored from that snapshot, then worked the way a revived box is.
    with litelink.restore(second, "s", published=where, s3_options=s3) as revived:
        # Checked BEFORE any work: the first seal recycles the open group, so
        # a stale one is invisible a moment later. Releasing the published rows
        # empties this buffer — every row in the snapshot is below the frontier
        # the published table reached — so a group still naming the replica's start
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
            revived.seal()
            revived.advance()
            revived.publish()

        assert revived.published_through() > ahead, (
            "publish never got past the published table's frontier: the log is wedged"
        )
        # And eviction is not pinned at zero by a straddling local file.
        assert revived.scan().read_all().num_rows > 0


def test_a_restore_fence_clears_the_published_table_and_not_just_the_replica(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """`RESTORE_RESERVE` is measured from the REPLICA's sequence, and the
    replica's sequence is not the highest offset anyone issued.

    `strip_local_state` runs before `restore` knows where the published table is, so it
    fences above what the snapshot carried. The reconcile below it exists
    precisely because that is not the whole truth — the bucket routinely holds
    ranges the replicated `extent` rows have never heard of — and the fence was
    never told. Left alone the restored log issues offsets the published table already
    holds: I9 broken, `publish` reporting success while pushing nothing for ever
    because the file sits below the published table's floor, and the union truncating
    the published leg at the colliding local extent, so published rows are served
    by no leg at all.

    The report already knew, and said so in a way nothing read: `skipped` is
    built from a `highest` that includes this frontier, so it came back as an
    INVERTED range. That is what an invariant looks like when it is computed
    and then not checked, and it is asserted here as one.

    The sequence is advanced with `reserve` rather than by writing a million
    rows, because the hazard is the DISTANCE between the replica's sequence and
    the published table's frontier and nothing about how it was travelled. Bulk ingest
    makes that distance cheap; it does not create it.
    """
    where = f"s3://{bucket}/fence"
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        # Equal to the seal target, so the load emits several files AT the
        # budget rather than one under it — `stable_prefix` holds back a
        # trailing run with room in it, and a single undersized file would
        # leave the published table exactly where the snapshot left it.
        target_compact_size=8 * 1024,
        compact_min_files=2,
        wal_replication=True,
    )
    primary = tmp_path / "primary"
    with litelink.new(
        primary, "s", schema=SCHEMA, config=config, published=where, s3_options=s3
    ) as log:
        log.extend(rows(300))
        log.seal(flush=True)
        log.await_seal()
        log.advance()

        # THE SNAPSHOT. The replica stalls here, at a low sequence.
        second = tmp_path / "second"
        (second / "s").mkdir(parents=True)
        source = sqlite3.connect(Layout(primary, "s").buffer_db)
        copy = sqlite3.connect(Layout(second, "s").buffer_db)
        source.backup(copy)
        source.close()
        copy.close()
        stalled_at = log.end_offset()

        # The primary carries on past the fence's whole width and PUSHES, so
        # the published table holds offsets a `RESTORE_RESERVE` above the snapshot
        # cannot reach.
        log._buffer.reserve(2 * RESTORE_RESERVE)  # noqa: SLF001
        log.ingest(
            pa.Table.from_pylist(rows(400), schema=SCHEMA).select(
                [f.name for f in SCHEMA]
            )
        )
        log.publish()
        published = log._published.require().span()  # noqa: SLF001

        assert published is not None, "the published table holds nothing to outrun with"
        ahead = published[1] - 1

    assert ahead > stalled_at + RESTORE_RESERVE, (
        "the published table did not outrun the snapshot by more than the fence, "
        "so the case is not set up"
    )

    with litelink.restore(second, "s", published=where, s3_options=s3) as revived:
        report = revived.recovery()

        assert report is not None, "a restored log must carry its report"
        assert revived.end_offset() > ahead, (
            f"resumed at {revived.end_offset()} while the published table holds "
            f"through {ahead}: the next append reuses a published offset"
        )
        # A WHOLE fence above the frontier, not one offset past it. The
        # published table is a lower bound on what the primary issued — rows it
        # acknowledged and never got to publish are above it, and their offsets
        # must not be reissued either. That is the same reason the reserve
        # exists over the replica's own sequence.
        assert report.resumed_at > ahead + RESTORE_RESERVE
        # The inverted range was the symptom, and it is an invariant now.
        assert report.skipped[0] <= report.skipped[1], (
            f"skipped range is inverted: {report.skipped}"
        )

        issued = revived.extend(rows(5))

        assert min(issued) > ahead, f"reissued published offsets: {issued}"


def test_an_interrupted_restore_cannot_reissue_the_primarys_offsets(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`WriteHandle.restore` has two durable writes; the order between them decides.

    `LogTable.create` is what makes a root openable, and the offset reserve is
    what makes its offsets safe. With the table first, an interruption between
    them left a root that `restore` refuses to retry and `WriteHandle.open` cheerfully
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
    with litelink.new(
        primary, "s", schema=SCHEMA, config=config, published=where, s3_options=s3
    ) as log:
        log.extend(rows(600))
        log.seal()
        log.advance()
        log.publish()
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
        litelink.restore(second, "s", published=where, s3_options=s3)

    monkeypatch.undo()

    # The reserve never ran, so the sequence is still the replica's. With the
    # table created FIRST this root would open, report `recovery() is None`,
    # and hand out offsets the primary already served.
    with pytest.raises(FileNotFoundError):
        litelink.open(second, "s", s3_options=s3)

    # And the half state is resumable rather than a dead end.
    with litelink.restore(second, "s", published=where, s3_options=s3) as revived:
        resumed = revived.append({"event_ts": 1, "key": "k", "payload": "p"})

        assert resumed > served, (
            f"reissued offset {resumed}; the primary served through {served}"
        )


def test_a_failed_restore_never_leaves_an_openable_root(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`LogTable.create` is the commit point, and must be the LAST write.

    That table is what makes a root openable. Wherever it sits, an interruption
    AFTER it leaves a root `restore` refuses to retry — both databases exist —
    and `WriteHandle.open` cheerfully accepts, reporting `recovery() is None`. An
    earlier ordering put it second and claimed no such state existed; it had
    one, one write later, and the measured consequence was worse than the
    offset reuse that ordering was fixing: the open group still at the
    replica's stale frontier, the first seal straddling the published table's extent,
    and 712 published offsets vanishing from every full `scan()`
    with no error anywhere.

    So the property, rather than any one window: if `restore` raises, nothing
    can open the root. Asserted for each step that can fail — and a bad minute
    in object storage is enough to fail the adoption, no crash required.
    """
    where = f"s3://{bucket}/failed"
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        compact_min_files=2,
        wal_replication=True,
    )
    primary = tmp_path / "primary"
    with litelink.new(
        primary, "s", schema=SCHEMA, config=config, published=where, s3_options=s3
    ) as log:
        log.extend(rows(600))
        log.seal()
        log.advance()
        log.publish()

    def die(*args: object, **kwargs: object) -> object:
        msg = "a bad minute in object storage"
        raise RuntimeError(msg)

    # Every step that can fail, INCLUDING the sort-order commit inside
    # `LogTable.create`. That method is two commits — the catalog row makes the
    # root openable, the declaration follows — so a failure in the second used
    # to leave a root refusing to retry while `WriteHandle.open` accepted it, telling
    # the operator to delete a log whose data was intact.
    for attempt, (target, method) in enumerate(
        [
            (Buffer, "strip_local_state"),
            (Published, "table"),
            (Buffer, "reseed_group"),
            (LogTable, "set_sort_order"),
        ]
    ):
        root = tmp_path / f"try{attempt}"
        (root / "s").mkdir(parents=True)
        source = sqlite3.connect(Layout(primary, "s").buffer_db)
        copy = sqlite3.connect(Layout(root, "s").buffer_db)
        source.backup(copy)
        source.close()
        copy.close()

        with monkeypatch.context() as patched:
            patched.setattr(target, method, die)

            with pytest.raises(RuntimeError, match="bad minute"):
                litelink.restore(root, "s", published=where, s3_options=s3)

        # The root is not a log. Whatever failed, nothing here can be opened
        # and handed offsets, because the table that would make it openable is
        # written only once everything else has landed.
        assert not LogTable.exists_for(Layout(root, "s")), (
            f"{method} failed and still left an openable root"
        )
        with pytest.raises(FileNotFoundError):
            litelink.open(root, "s", s3_options=s3)

        # And it is resumable rather than a dead end.
        with litelink.restore(root, "s", published=where, s3_options=s3) as revived:
            assert revived.scan().read_all().num_rows > 0


def test_a_refused_restore_does_not_drop_a_live_logs_catalog_row(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """`restore`'s rollback must never undo a row it did not create.

    `LogTable.create` is two commits, so a failure in the second drops the
    catalog row to leave the root unopenable — which is right when this call
    made the row, and destructive when it did not. `create` raises
    `TableAlreadyExistsError` on a row that pre-exists, and the blanket
    `except` then dropped a LIVE log's only pointer to its local files.

    Reachable with `buffer.db` gone and the table present, which the guard at
    the top cannot catch: it keys on the buffer. Removing the buffer is a
    plausible answer to that guard's own "Remove it, or restore into another
    root".
    """
    binary = Path(__file__).resolve().parent.parent / ".bin" / "litestream"
    if not os.access(binary, os.X_OK):
        pytest.skip("litestream is not provisioned — run `just litestream`")

    where = f"s3://{bucket}/live-row"
    config = replace(
        LogConfig(),
        target_seal_size=8 * 1024,
        compact_min_files=2,
        wal_replication=True,
    )
    with litelink.new(
        tmp_path, "s", schema=SCHEMA, config=config, published=where, s3_options=s3
    ) as log:
        log.extend(rows(1200))
        log.seal()
        log.advance()
        log.publish()
        readable = log.scan().read_all().num_rows
        replication = log.write_replication_config()

    # A replica has to exist, or `restore` fails at the download and never
    # reaches the create this is about.
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

    # The buffer removed by hand; the table and its files stay. That is a
    # plausible answer to the guard's own "Remove it, or restore into another
    # root", and it walks straight past the guard, which keys on the buffer.
    Layout(tmp_path, "s").buffer_db.unlink()

    with pytest.raises(Exception, match="already exists"):
        litelink.restore(
            tmp_path, "s", published=where, s3_options=s3, binary=str(binary)
        )

    # The row survives, so the local files are still referenced and the log
    # still reads. Before this, `WriteHandle.open` answered "use new() to create one".
    assert LogTable.exists_for(Layout(tmp_path, "s"))
    with litelink.open(tmp_path, "s", read_only=True, s3_options=s3) as reopened:
        assert reopened.scan().read_all().num_rows == readable


def test_every_empty_published_table_spelling_means_the_local_default(
    tmp_path: Path,
) -> None:
    """`published=""` must land where `None` does.

    Normalising `"" -> None` once happened after the guards, so an empty
    string slipped past them — 7,828 acknowledged rows lost. The plausible
    route is not a literal but `os.environ.get("PUBLISHED", "")`.
    """
    for number, spelling in enumerate(("", "/", "///")):
        root = tmp_path / str(number)
        with litelink.new(root, "s", schema=SCHEMA, published=spelling) as log:
            assert log.published == Layout(root, "s").default_published


def test_creating_a_log_with_an_empty_published_table_publishes_locally(
    tmp_path: Path,
) -> None:
    """`litelink.new(published="")` is `published=None`: the local default."""
    with litelink.new(tmp_path, "s", schema=SCHEMA, published="") as log:
        assert log.published == Layout(tmp_path, "s").default_published


@pytest.mark.slow
def test_a_writer_reports_where_its_next_append_lands_not_what_it_can_serve(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A writer's `end_offset` is the sequence; a reader's is the tiers.

    `LogHandle.end_offset` answers "past the last row I can SERVE", which is
    what a follower needs and what a handle assembled from two tiers can
    honestly claim. A writer is asked where its next row will land, and only
    `sqlite_sequence` knows that — it is the thing that assigns it.

    The two coincide on a healthy log and diverge exactly where the staging
    table is empty while the published table holds rows. `restore` is that state by
    construction: the fence burns `RESTORE_RESERVE` offsets, so the sequence
    sits 2**20 above the published table's frontier while the rebuilt table is empty.

    Inheriting the reader's answer reported the PUBLISHED table's frontier there. A
    caller reading `end_offset()` as "what comes next" — which is what its
    docstring and §4's half-open seal ranges promise — would land inside the
    fence that I9 exists to hold.

    Falsify by deleting `WriteHandle.end_offset`: the writer inherits the
    reader's version and this fails by about `RESTORE_RESERVE`.
    """
    where = f"s3://{bucket}/prefix"
    second = tmp_path / "second"
    with published_log(tmp_path / "first", bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal()
        log.advance()
        log.publish()

        # Stand the second box up from a copy of the buffer, the way the
        # failover tests beside this one do — `restore` needs a replica and
        # this test is about the arithmetic, not about litestream.
        (second / "s").mkdir(parents=True)
        source = sqlite3.connect(Layout(tmp_path / "first", "s").buffer_db)
        copy = sqlite3.connect(Layout(second, "s").buffer_db)
        source.backup(copy)
        source.close()
        copy.close()

    with litelink.restore(second, "s", published=where, s3_options=s3) as revived:
        assert revived.staging_extent() is None, (
            "a restore rebuilds the staging table empty — that is the state where "
            "the two questions diverge"
        )

        sequence = revived._buffer.next_offset()  # noqa: SLF001
        assert sequence > ROWS + RESTORE_RESERVE - 1, "the fence must be in place"

        assert revived.end_offset() == sequence, (
            f"a writer reported {revived.end_offset()} while its next append "
            f"lands at {sequence} — inside the fence"
        )

        # A READER on the same log answers the other question, correctly —
        # checked BEFORE the append below, which would close the gap by
        # putting a row at the sequence.
        # Restored from a replica, so the staging table is empty and the rows
        # are in the published table — a view that reads it is the only one that can
        # answer "what can I serve".
        with litelink.open(second, "s", read_only=True, s3_options=s3) as view:
            served = view.scan().read_all().column(OFFSET).to_pylist()
            assert view.end_offset() == max(served) + 1
            assert view.end_offset() < revived.end_offset(), (
                "the reader reports what it can serve; the writer, where it "
                "will write — and across a fence they must differ"
            )
            assert revived.end_offset() - view.end_offset() >= RESTORE_RESERVE

        # The promise the writer makes, checked directly.
        assert revived.append(rows(1)[0]) == revived.end_offset() - 1


@pytest.mark.slow
def test_buffered_rows_sees_another_process_seal(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The §7 boundary has to be re-read, or the two tiers double-count.

    A seal LEAVES its rows in the buffer when `wal_replication` is on — the
    buffer is the off-box copy until the published table has the range (§3a) — so
    `buffered_rows` cannot ask the buffer how many rows it holds. It counts
    above the staging table's frontier instead.

    That frontier moves when ANOTHER process seals, and `LogTable.extent`
    resolves nothing: it compares against this handle's in-memory
    `metadata_location`. Read directly, it pins the boundary to whatever
    snapshot was last loaded, and every row the other process has sealed still
    counts as unsealed — so `staging_rows() + buffered_rows()` counts that band
    twice, against I3's guarantee that each row crosses the boundary exactly
    once.

    This is the documented two-role topology: RUNTIME.md has the writer append
    while the maintainer seals.

    Falsify by reading `self._table.span()` without the reload
    in `buffered_rows`: the reader reports 20 buffered and 20 in the table for
    a log holding 20.
    """
    where = f"s3://{bucket}/prefix"
    config = replace(
        LogConfig(),
        target_seal_size=1,
        wal_replication=True,
        staging_snapshot_retention=timedelta(seconds=0),
        published_snapshot_retention=timedelta(seconds=0),
    )
    with litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        published=where,
        s3_options=s3,
    ) as writer:
        writer.extend(rows(20))

        with litelink.open(tmp_path, "s", read_only=True, s3_options=s3) as reader:
            # Warm the reader's view BEFORE the seal, so its cached pointer is
            # the stale one.
            assert reader.buffered_rows() == 20
            assert reader.staging_rows() == 0

            writer.seal()

            # `buffered_rows` FIRST, before anything else reloads. That order
            # is the whole test: `staging_rows()` resolves the catalog, so
            # calling it first repairs the stale pointer and hides this.
            # `examples/adsb/tail.py` only reads the right number because it
            # happens to evaluate `staging_rows()` earlier in the same tuple.
            buffered = reader.buffered_rows()
            assert buffered == 0, (
                f"the reader counted {buffered} rows as unsealed after another "
                f"process sealed them"
            )

            assert reader.staging_rows() == 20, "the fixture must seal"
            assert reader.staging_rows() + reader.buffered_rows() == 20, (
                "the tiers must not double-count across the §7 boundary"
            )

        # And the writer sees the maintainer's seal too, in the order that
        # hides the bug: `buffered_rows` alone, with nothing reloading first.
        assert writer.buffered_rows() == 0


@pytest.mark.slow
def test_a_handle_that_read_an_empty_published_table_still_sees_it_fill(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Handle lifetime, which is where the last two criticals both lived.

    `Published.table` returns its cached handle while the URI matches, and
    `LogTable.extent` short-circuits on an unchanged `metadata_location`. So a
    handle that first touches the published table while the table EXISTS BUT IS EMPTY
    pins the answer "no extent" — and `_published_required` then derives that the
    published table is not load-bearing, for the rest of that handle's life.

    The empty-but-existing published table is ordinary, not a corner: `publish` creates
    the table on a maintenance tick before anything is sealed. One early call
    is enough to pin
    it — a `scan`, an `end_offset`, even a bare metadata poll of the kind
    `examples/adsb/tail.py` makes on its first tick.

    Measured before the fix, on the documented two-role topology: after the
    maintainer sealed, published and evicted 20 rows, a reader that had scanned
    once beforehand returned **0 of 20, indefinitely**, while `coverage()` on
    the same handle reported it gap-free. `coverage` resolves, so touching it
    repaired the pointer by accident; a scan-only loop never healed.

    Every other test opens a fresh handle after the eviction, which is why
    nine review rounds did not reach this.

    Falsify by reading `self._published.table(repair=False).span()` in
    `_published_required` instead of `self._published_span()`.
    """
    where = f"s3://{bucket}/prefix"
    config = replace(
        LogConfig(),
        target_seal_size=64 * 1024,
        target_compact_size=64 * 1024,
        compact_min_files=2,
        staging_retention=timedelta(0),
        staging_snapshot_retention=timedelta(seconds=0),
        published_snapshot_retention=timedelta(seconds=0),
    )
    with litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        published=where,
        s3_options=s3,
    ) as writer:
        # A maintenance tick BEFORE anything is sealed: this creates the
        # published table, empty. That is the state that used to poison a handle.
        writer.publish()

        with litelink.open(tmp_path, "s", read_only=True, s3_options=s3) as reader:
            assert reader.scan().read_all().num_rows == 0
            assert reader.end_offset() >= 1

            # Now the log fills, is published, and is evicted dry — the rows
            # exist only in the published table.
            writer.extend(rows(ROWS))
            writer.seal()
            writer.publish()
            writer.advance()
            assert writer.staging_extent() is None, "the fixture must evict dry"

            # The SAME handle, which read the published table while it was empty.
            served = reader.scan().read_all().column(OFFSET).to_pylist()
            assert sorted(served) == list(range(1, ROWS + 1)), (
                f"a handle that read the published table while it was empty served "
                f"{len(served)} of {ROWS} rows afterwards"
            )

            # And the writer's own, through a view that reads the published table.
            assert writer.scan().read_all().num_rows == ROWS


@pytest.mark.slow
def test_an_evicted_log_still_serves_every_row(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A default read must not go short because eviction emptied the table.

    Eviction moves files out of the staging table once the published table holds them
    (I4). A log evicted dry therefore has its rows in exactly one place, and a
    read that skips the published table returns the unsealed buffer alone — measured
    before this was fixed, 476 of 1,500 rows, with no error at all.

    With nothing local, every published file is below the staging table, so any
    read that could match one reads the published table.

    Falsify by returning `(True, False)` from `Reader._tiers`: the row count
    drops to the buffer's share with no error at all.
    """
    with published_log(tmp_path, bucket, s3, staging_retention=timedelta(0)) as log:
        log.extend(rows(ROWS))
        log.seal()
        log.publish()
        log.advance()

        assert log.staging_extent() is None, "the fixture must evict the tier dry"
        assert log.published_through() > 0

        served = log.scan().read_all().column(OFFSET).to_pylist()
        assert sorted(served) == list(range(1, ROWS + 1)), (
            f"an evicted log served {len(served)} of {ROWS} rows"
        )

        # A reader on the same root agrees, opened the same way.
        with litelink.open(tmp_path, "s", read_only=True, s3_options=s3) as view:
            assert view.scan().read_all().num_rows == ROWS


def test_the_handle_surface_is_exactly_what_the_docs_print() -> None:
    """Every handle is on the primary, and the surface is pinned to API.md.

    `snapshot` and `RemoteReadHandle` are gone (#90): reading a log from another
    machine is any Iceberg engine over its published table, not a litelink handle. The
    remaining hierarchy only ADDS, and each class's members are exactly the
    table `docs/API.md` prints — a member added without updating it makes the
    documented surface a lie, which this catches.
    """
    assert not hasattr(litelink, "snapshot"), "remote reads were removed (#90)"
    assert not hasattr(litelink, "RemoteReadHandle"), "remote reads were removed (#90)"
    assert not hasattr(litelink, "follow")

    # Every handle inherits ONE read path, so drift is impossible rather than
    # merely detectable.
    for shared in ("scan", "sql", "coverage"):
        assert getattr(WriteHandle, shared) is getattr(LogHandle, shared), (
            f"{shared} is reimplemented rather than inherited"
        )

    # The one deliberate override: a writer is asked where its next append
    # lands, from `sqlite_sequence`, which only a writer can know. Inheriting
    # the reader's "past the last row I can serve" made a restored log report
    # the published table's frontier while its next append took an offset
    # RESTORE_RESERVE higher, inside the fence I9 exists to hold.
    assert WriteHandle.end_offset is not LogHandle.end_offset
    assert litelink.LocalReadHandle.end_offset is LogHandle.end_offset, (
        "a local reader answers the reader's question"
    )

    # Every subclass ADDS; none refuses.
    assert issubclass(WriteHandle, litelink.LocalReadHandle)
    assert issubclass(litelink.LocalReadHandle, LogHandle)

    # The write surface is not merely refused, it is absent.
    for absent in ("append", "extend", "seal", "publish", "advance", "set_config"):
        assert not hasattr(LogHandle, absent), f"LogHandle exposes {absent}"

    assert {n for n in vars(LogHandle) if not n.startswith("_")} == {
        # read
        "scan",
        "sql",
        # observe
        "coverage",
        "end_offset",
        "buffered_rows",
        "staging_rows",
        "staging_files",
        "staging_extent",
        "published_through",
        "published_files",
        "column_statistics",
        # identity
        "root",
        "name",
        "config",
        "sort_by",
        "schema",
        "published",
        # lifecycle
        "close",
    }

    assert {n for n in vars(litelink.LocalReadHandle) if not n.startswith("_")} == {
        "databases",
        "replication_config",
        "write_replication_config",
    }, "the local handle adds the replication surface and nothing else"

    # It builds from the read collaborators alone. Passing a `WriteHandle` in
    # was an earlier shape, and a handle that holds a writer asks it questions
    # it answers for a writer.
    taken = set(inspect.signature(LogHandle.__init__).parameters) - {"self"}
    assert taken == {
        "layout",
        "table",
        "buffer",
        "published",
        "reader",
    }


def _clone_buffer(source: Path, target: Path) -> None:
    """Copy a buffer the way an operator recovering one has to.

    Through the backup API rather than `shutil.copy`: the buffer is in WAL
    mode, so the `.db` file alone can be missing every recent write — a plain
    copy of a busy log produced a file whose `meta` table did not exist yet.
    """
    src = sqlite3.connect(source)
    dst = sqlite3.connect(target)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()


def test_restore_refuses_a_buffer_bound_to_another_published_table(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """`published=` says where the replica came from; `meta` says which log it is.

    They agree on the ordinary path by construction, so the divergence only
    appears when the buffer arrives some other way — and hand-placing one is
    the documented recovery for a log whose WAL was never replicated. Left
    unchecked the mismatch is silent: the handle comes back attached to the
    buffer's published table, reporting ITS frontier, while the rows under the published table
    the caller actually named are invisible.

    Both harms are asserted, because the second is the one an operator would
    never think to look for: adoption runs `table(repair=True)`, which with no
    published hint takes the CREATE branch and writes a fresh `metadata.json`
    and `version-hint.text` into the OTHER bucket. A restore that refuses must
    leave that published table untouched.
    """
    held = f"s3://{bucket}/held"
    other = f"s3://{bucket}/other"
    config = replace(LogConfig(), target_seal_size=8 * 1024, compact_min_files=2)

    # The published table the caller means, with rows actually in it.
    primary = tmp_path / "primary"
    with litelink.new(
        primary, "s", schema=SCHEMA, config=config, published=held, s3_options=s3
    ) as log:
        log.extend(rows(800))
        log.seal()
        log.advance()
        log.publish()
        seeded = log.published_through()

    assert seeded > 0, (
        "the published table was never published, so the case is not set up"
    )

    # A different log, bound to a different published table, whose buffer is the donor.
    donor = tmp_path / "donor"
    litelink.new(
        donor, "s", schema=SCHEMA, config=config, published=other, s3_options=s3
    ).close()

    revived = tmp_path / "revived"
    (revived / "s").mkdir(parents=True)
    _clone_buffer(Layout(donor, "s").buffer_db, Layout(revived, "s").buffer_db)

    with pytest.raises(ValueError, match="records published=") as caught:
        litelink.restore(revived, "s", published=held, s3_options=s3)

    # Both prefixes named: which one it found, and which one was asked for.
    assert other in str(caught.value)
    assert held in str(caught.value)

    # And the published table it would have misbound to is untouched -- no lineage
    # published into a bucket this call never had any business writing.
    fs = filesystem(s3)
    assert not fs.exists(f"{bucket}/other/s/metadata/version-hint.text"), (
        "a refused restore still published a table into the buffer's published table"
    )


def test_restore_accepts_the_same_published_table_written_with_a_trailing_slash(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """`s3://b/p/` and `s3://b/p` name one published table, and must not read as two.

    The conflict check compares the argument against what `meta` recorded, and
    a prefix is an ordinary string a caller types — so without normalising the
    trailing slash the guard would refuse the very restore it exists to
    protect, and the operator's escape from it is to guess at punctuation.
    """
    held = f"s3://{bucket}/slash"
    config = replace(LogConfig(), target_seal_size=8 * 1024, compact_min_files=2)

    primary = tmp_path / "primary"
    with litelink.new(
        primary, "s", schema=SCHEMA, config=config, published=held, s3_options=s3
    ) as log:
        log.extend(rows(800))
        log.seal()
        log.advance()
        log.publish()
        seeded = log.published_through()

    assert seeded > 0, (
        "the published table was never published, so the case is not set up"
    )

    revived = tmp_path / "revived"
    (revived / "s").mkdir(parents=True)
    _clone_buffer(Layout(primary, "s").buffer_db, Layout(revived, "s").buffer_db)

    # The SAME published table, one trailing slash different. This must attach.
    with litelink.restore(
        revived, "s", published=held + "/", s3_options=s3
    ) as revived_log:
        assert revived_log.published_through() == seeded
        assert revived_log.scan().read_all().num_rows > 0


def test_restore_refuses_a_buffer_from_a_local_only_log(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A buffer from a log that published locally cannot be attached to S3.

    It fails without this guard too — but at the very END, from
    `write_replication_config`, which is after `LogTable.create`. That is the
    commit point, so the caller gets an exception over a root that is now
    openable, holds a local-only log, and that `restore` will not retry because
    both databases exist. Asserted by checking the root is still RETRYABLE:
    no table was created, which is the difference the guard buys.
    """
    donor = tmp_path / "donor"
    litelink.new(donor, "s", schema=SCHEMA, published=None).close()

    revived = tmp_path / "revived"
    (revived / "s").mkdir(parents=True)
    _clone_buffer(Layout(donor, "s").buffer_db, Layout(revived, "s").buffer_db)

    with pytest.raises(ValueError, match="records published='file://") as caught:
        litelink.restore(
            revived, "s", published=f"s3://{bucket}/nowhere", s3_options=s3
        )

    assert f"s3://{bucket}/nowhere" in str(caught.value)
    # The commit point was never reached, so a corrected call can still run.
    assert not LogTable.exists_for(Layout(revived, "s")), (
        "restore built the log before refusing; the root is no longer retryable"
    )


def test_an_otel_log_reads_back_exactly_from_the_published_table(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Binary and nested columns through publish, the published leg, and plain DuckDB.

    Read on the primary with the local copy evicted, so every row comes back
    through `iceberg_scan` over the published table — where a fixed width or a nested
    type would come back reshaped if anything on the way lost it. And the published table is read by engines with no litelink at
    all, which is where the map has to be addressable as a map (#79).
    """
    from tests.test_types import OTEL, otel_row

    where = f"s3://{bucket}/otel"
    rows = [otel_row(n) for n in range(1, 41)]
    config = LogConfig(
        target_seal_rows=10,
        compact_min_files=2,
        staging_retention=timedelta(0),
    )
    with litelink.new(
        tmp_path,
        "s",
        schema=OTEL,
        sort_by=("ts",),
        config=config,
        published=where,
        s3_options=s3,
    ) as log:
        log.extend(rows)
        log.seal(flush=True)
        log.advance()
        log.publish(flush=True)
        assert log.published_through() == 40
        log.advance()  # evicts the local copy, so the read below is the published leg

    expected = pa.Table.from_pylist(rows, schema=OTEL).to_pylist()
    with litelink.open(tmp_path, "s", read_only=True, s3_options=s3) as view:
        assert view.staging_rows() == 0, "the local copy must be evicted"
        assert view.schema == OTEL
        assert view.scan().read_all().drop([OFFSET]).to_pylist() == expected

    con = duckdb.connect()
    con.execute(secret_sql(s3))
    counted = con.execute(
        f"SELECT count(*), max(attributes['attempt'].i) FROM iceberg_scan('{where}/s',"
        " version_name_format = '%s%s.metadata.json')"
    ).fetchall()
    assert counted == [(40, 40)]


def test_the_statistics_tiers_partition_the_log(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Each row in exactly one tier, and the three together are the whole log.

    `"published"` is what eviction moved below the staging table, not the
    published table's copy of the staging window, which `"staging"` already counts; the
    buffer is what no file holds yet. Their counts add up to `tier=None` and to
    what a scan returns.

    With `wal_replication`, a seal keeps its rows in the buffer until the
    published table has them, so the last batch here is in both the staging table and
    the buffer — and must be counted once.

    Falsify by rolling up every published file for `"published"` (dropping the
    `below_staging` split): its count is the whole published table. Or by counting the
    buffer without the ceiling: the held batch is counted twice. Either way the
    three tiers add up to more than the log.
    """
    tail = 37
    held = 23
    with published_log(
        tmp_path,
        bucket,
        s3,
        staging_retention=timedelta(0),
        staging_rows=1000,
        wal_replication=True,
    ) as log:
        log.extend(rows(ROWS))
        log.seal(flush=True)
        log.publish(flush=True)
        log.advance()
        log.extend(
            {"event_ts": ROWS + i, "key": "h", "payload": "y"} for i in range(held)
        )
        log.seal(flush=True)
        assert log._buffer.span() is not None, "the seal must keep its rows"  # noqa: SLF001
        log.extend(
            {"event_ts": ROWS + held + i, "key": "t", "payload": "y"}
            for i in range(tail)
        )

        local = log.column_statistics(tier="staging")
        published = log.column_statistics(tier="published")
        buffered = log.column_statistics(tier="buffer")
        whole = log.column_statistics()
        extent = log.staging_extent()
        assert extent is not None

        assert local.record_count is not None
        assert 0 < local.record_count < ROWS, "the fixture must evict part of it"
        assert (local[OFFSET].min, local[OFFSET].max + 1) == extent

        assert published.tier == "published"
        assert (published[OFFSET].min, published[OFFSET].max) == (1, extent[0] - 1)
        assert published["event_ts"].max == extent[0] - 2

        assert buffered.record_count == tail
        assert buffered[OFFSET].min == ROWS + held + 1

        everything = log.scan().read_all()
        assert published.record_count is not None
        assert buffered.record_count is not None
        assert (
            local.record_count + published.record_count + buffered.record_count
            == whole.record_count
            == everything.num_rows
            == ROWS + held + tail
        )
        last = ROWS + held + tail
        assert (whole[OFFSET].min, whole[OFFSET].max) == (1, last)
        assert (whole["event_ts"].min, whole["event_ts"].max) == (0, last - 1)
        assert whole["key"].null_count == 0


def test_a_seal_that_keeps_its_rows_does_not_count_them_twice(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """With `wal_replication` a seal leaves its rows in the buffer, so the
    buffer and the files both hold them; the whole log counts each once.
    """
    with published_log(tmp_path, bucket, s3, wal_replication=True) as log:
        log.extend(rows(500))
        log.seal(flush=True)

        assert log._buffer.count_from(1) == 500, "the fixture must keep sealed rows"
        assert log.buffered_rows() == 0
        assert log.staging_rows() == 500

        whole = log.column_statistics()
        assert whole.record_count == 500
        assert whole["event_ts"].null_count == 0
        assert (whole[OFFSET].min, whole[OFFSET].max) == (1, 500)


def test_maintain_expires_the_published_table_and_drains_what_that_frees(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Every `advance` publishes and then expires published snapshots past
    `published_snapshot_retention`, and the manifest lists and manifests only
    they referenced are deleted once due (#113). A table that was only ever
    published used to keep every one of them.

    Falsify by dropping `expire_published` from `advance`: four snapshots
    survive, and so do their manifest lists.
    """
    with published_log(tmp_path, bucket, s3) as log:
        for _ in range(4):
            log.extend(rows(ROWS))
            log.seal()
            log.advance()

        published = log._published.require()
        published.reload()
        assert len(list(published._table.snapshots())) == 1

        listed = {path for path, _ in published.metadata_files()}
        lists = {p for p in listed if p.rpartition("/")[2].startswith("snap-")}
        assert len(lists) == 1, "an expired snapshot's manifest list must be deleted"
        assert lists <= published.anchors(), "and the one left is the current one"
        assert log.scan().read_all().num_rows == 4 * ROWS


def test_the_sweep_deletes_stranded_metadata_from_object_storage(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The sweep on S3: a listing names objects as the published table names
    them, so a stranded manifest is deleted and nothing live is.

    Falsify by building the listed names without the `s3://` scheme: the
    anchor check refuses the listing and the stranded object survives.
    """
    import litelink._maintenance as maintenance

    monkeypatch.setattr(maintenance, "SWEEP_MIN_AGE", timedelta(0))
    monkeypatch.setattr(maintenance, "SWEEP_INTERVAL", timedelta(0))
    fs = filesystem(s3)
    with published_log(tmp_path, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal()
        log.publish()

        published = log._published.require()
        directory = published.metadata_location.rpartition("/")[0]
        stranded = f"{directory}/{uuid.uuid4()}-m0.avro"
        fs.pipe(stranded.removeprefix("s3://"), b"stranded")
        live = published.referenced_paths() | published.live_metadata()

        log.sweep()

        assert not fs.exists(stranded.removeprefix("s3://"))
        published.reload()
        for path in published.referenced_paths():
            assert fs.exists(path.removeprefix("s3://")), path

        assert live, "the test must hold live objects to keep"
        assert log.scan().read_all().num_rows == ROWS


def test_a_disk_cached_connection_shares_published_reads_by_key(
    tmp_path: Path, bucket: str, s3: S3Options, isolated_read_cache: Path
) -> None:
    """A reader on another machine caching the published table (#118):
    a `duckdb_connection(disk_cache=True, cache_key=…)` caches the published
    table's blocks on disk, and a later connection with the same key — another
    process, in production — reuses them rather than fetching from S3.

    Falsify by not calling `install_read_cache` from `duckdb_connection`: no
    directory is filled.
    """
    root = tmp_path / "log"
    with published_log(
        root, bucket, s3, staging_retention=timedelta(0), staging_rows=0
    ) as log:
        log.extend(rows(ROWS))
        log.advance(flush=True)
        metadata = log._published.require().metadata_location  # noqa: SLF001

    def cached(key: str) -> int:
        return sum(
            len(files)
            for _, _, files in os.walk(isolated_read_cache / "litelink" / key)
        )

    def read(key: str) -> None:
        connection = litelink.duckdb_connection(
            s3_options=s3, disk_cache=True, memory_cache=False, cache_key=key
        )
        try:
            (count,) = connection.execute(
                f"SELECT count(*) FROM iceberg_scan('{metadata}')"
            ).fetchone() or (0,)
            assert count == ROWS
        finally:
            connection.close()

    # An empty key's directory fills: the reads went through the cache.
    read("stream-1")
    filled = cached("stream-1")
    assert filled > 0, "a published read cached nothing"

    # The same key again adds nothing: every block was already there.
    read("stream-1")
    assert cached("stream-1") == filled, "the second reader fetched blocks again"

    # The control: another key fills its own.
    read("stream-2")
    assert cached("stream-2") == filled


def test_a_disk_cached_reader_sees_new_publishes_through_current_metadata(
    tmp_path: Path, bucket: str, s3: S3Options, isolated_read_cache: Path
) -> None:
    """A disk-cached reader that resolves the table with `current_metadata`
    reads each new publish, on the same connection and on a new one (#141).

    `cache_httpfs` caches `version-hint.text` like any file, so a scan of the
    table directory on such a connection stays on its first snapshot, or with
    the hint excluded from the disk cache, fails DuckDB's ETag check through
    the cached file handle. `current_metadata` reads the hint outside DuckDB,
    and the metadata path it returns is immutable, so the cache is right for it.

    Falsify by resolving the hint through the connection instead
    (`iceberg_scan('<location>', version_name_format=…)`): the second read
    raises the ETag error, or without the exclusion returns the first count.
    """
    root = tmp_path / "log"
    with published_log(root, bucket, s3) as log:
        log.extend(rows(250))
        log.advance(flush=True)
        published = log._published.require()  # noqa: SLF001
        location = published.metadata_location.rpartition("/metadata/")[0]

        def count(connection: duckdb.DuckDBPyConnection) -> int:
            metadata = litelink.current_metadata(location, s3_options=s3)
            (found,) = connection.execute(
                f"SELECT count(*) FROM iceberg_scan('{metadata}')"
            ).fetchone() or (0,)
            return int(found)

        connection = litelink.duckdb_connection(
            s3_options=s3, disk_cache=True, cache_key="reader"
        )
        assert count(connection) == 250
        assert litelink.current_metadata(location, s3_options=s3) == (
            published.metadata_location
        )

        log.extend(rows(50))
        log.advance(flush=True)

        assert count(connection) == 300, "the same connection read a stale snapshot"
        fresh = litelink.duckdb_connection(
            s3_options=s3, disk_cache=True, cache_key="reader"
        )
        assert count(fresh) == 300, "a new connection read a stale snapshot"


def test_advance_cycles_and_reloads_reuse_one_file_io(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every load and commit of a table reuses the process's FileIO for it (#137).

    pyiceberg builds a FileIO per `load_table` and per commit, each with its
    own `S3FileSystem` and connection pool, and each in a reference cycle that
    only the cycle collector frees. On Python 3.14 a reader reloading every few
    milliseconds held 1,013 open S3 connections. Counted here rather than
    measured in sockets, so the answer does not depend on when a collector runs.

    Across full `advance()` cycles in which every step has work — seal,
    compact, publish, eviction (`staging_rows`), both expiries and drains
    (zero retention) — with a read between them, and across many bare reloads
    of both tables.

    Falsify by dropping either `_Catalog` override: every reload and commit
    builds a new FileIO, two per load without either.
    """
    from pyiceberg.io.pyarrow import PyArrowFileIO

    with litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=LogConfig(
            target_seal_size=4096,
            target_compact_size=16 * 1024,
            compact_min_files=2,
            staging_rows=100,
            staging_snapshot_retention=timedelta(seconds=0),
            published_snapshot_retention=timedelta(seconds=0),
        ),
        published=f"s3://{bucket}/prefix",
        s3_options=s3,
    ) as log:
        # Warm: the first cycle builds each table's FileIO once.
        log.extend(rows(200))
        log.advance(flush=True)
        published = log._published.table()
        assert published is not None

        built: list[object] = []
        real_init = PyArrowFileIO.__init__

        def counting(self: PyArrowFileIO, *args: object, **kwargs: object) -> None:
            built.append(self)
            real_init(self, *args, **kwargs)  # ty: ignore[invalid-argument-type]

        monkeypatch.setattr(PyArrowFileIO, "__init__", counting)

        snapshots = published.snapshot_count()
        for cycle in range(1, 6):
            log.extend(rows(300))
            log.advance(flush=True)
            assert log.scan(columns=[OFFSET]).read_all().num_rows == 200 + 300 * cycle

        for _ in range(50):
            published.reload()
            log._table.reload()

        # Every step had work: everything published, staging evicted to its
        # floor, and the published table expired down from its pushes.
        assert log.published_through() >= log.end_offset() - 1
        assert log.staging_rows() < log.end_offset() - 1
        assert published.snapshot_count() <= snapshots
        assert built == [], (
            f"{len(built)} FileIOs built across advance cycles and reloads"
        )


# -- restore without a WAL replica (#144) --------------------------------------


def offset_list(table: pa.Table) -> list[int]:
    return [int(offset) for offset in table.column(OFFSET).to_pylist()]


def _restore_settings() -> LogConfig:
    """`published_log`'s settings, with eviction on so the cycles after a
    restore exercise it."""
    return LogConfig(
        target_seal_size=64 * 1024,
        target_compact_size=64 * 1024,
        compact_min_files=2,
        staging_snapshot_retention=timedelta(seconds=0),
        published_snapshot_retention=timedelta(seconds=0),
        staging_retention=timedelta(0),
        staging_rows=0,
    )


def test_restore_without_a_replica_rebuilds_the_log_from_its_published_table(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A log that never replicated its WAL, its machine gone, comes back from
    its published table at its own name and keeps working.

    It continues at the published end plus the restore fence, holds the same
    schema and `sort_by`, reads every published row, and then runs real seal,
    compact, publish, evict and reclaim cycles — rows the dead machine had not
    published are gone, as there was no other copy of them.

    Falsify by restoring the `FileNotFoundError` for an absent replica: this
    raises instead of rebuilding.
    """
    first = tmp_path / "first"
    with published_log(first, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.advance(flush=True)
        published_end = log.published_through() + 1

        assert published_end == log.end_offset(), "the setup publishes everything"

        # Acknowledged and never published: lost with the machine.
        log.extend(rows(50))

    shutil.rmtree(first)
    where = f"s3://{bucket}/prefix"
    with litelink.restore(
        tmp_path / "second", "s", published=where, s3_options=s3
    ) as revived:
        report = revived.recovery()

        assert report is not None
        resumed = published_end + PUBLISHED_RESTORE_RESERVE

        assert revived.end_offset() == resumed
        assert report.skipped == (published_end, resumed)
        assert revived.schema == SCHEMA
        assert revived.sort_by == ("event_ts",)
        assert revived.scan().read_all().num_rows == ROWS

        revived.set_config(_restore_settings())
        for _ in range(3):
            revived.extend(rows(300))
            revived.advance(flush=True)

        assert revived.published_through() == revived.end_offset() - 1
        assert revived.staging_rows() == 0, "eviction did not run after the restore"
        offsets = offset_list(revived.scan(columns=[OFFSET]).read_all())

        assert len(offsets) == ROWS + 900
        assert len(set(offsets)) == len(offsets), "a row was read twice"
        assert max(offsets) == revived.end_offset() - 1


def test_restore_without_a_replica_starts_above_what_the_log_issued(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The published end trails what the log issued by everything it had not
    published yet, so a rebuild starts above the `litelink.issued_through` the
    last publish recorded, not only above the end — or it hands those offsets,
    already returned by `append` and seen by readers, to different rows.

    Falsify by starting at the published end alone: the restored log resumes
    below the recorded issue.
    """
    first = tmp_path / "first"
    with published_log(first, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.seal()
        # Without `flush` the trailing run stays local, so the log has issued
        # past what the published table holds when this push records it.
        log.publish()
        published_end = log.published_through() + 1
        issued = log.end_offset() - 1

        assert issued >= published_end, "the setup must issue past the push"

    shutil.rmtree(first)
    with litelink.restore(
        tmp_path / "second", "s", published=f"s3://{bucket}/prefix", s3_options=s3
    ) as revived:
        assert revived.end_offset() == issued + 1 + PUBLISHED_RESTORE_RESERVE


def test_a_restore_reserve_overrides_the_fence(tmp_path: Path) -> None:
    """`published_reserve` replaces the rebuild's default fence, measured from
    the same point; and a reserve that is not a non-negative integer is
    refused before anything is created."""
    where = f"file://{tmp_path / 'published'}"
    first = tmp_path / "first"
    with litelink.new(
        first,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=_restore_settings(),
        published=where,
    ) as log:
        log.extend(rows(ROWS))
        log.advance(flush=True)
        issued = log.end_offset() - 1

    shutil.rmtree(first)
    for name in ("wal_reserve", "published_reserve"):
        for bad in (-1, 2.5, True):
            with pytest.raises(ValueError, match=name):
                litelink.restore(tmp_path / "bad", "s", published=where, **{name: bad})  # ty: ignore[invalid-argument-type]

            assert not (tmp_path / "bad").exists()

    with litelink.restore(
        tmp_path / "second", "s", published=where, published_reserve=1000
    ) as revived:
        assert revived.end_offset() == issued + 1 + 1000


def test_restore_without_a_replica_derives_the_shape_from_an_older_table(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A published table only an older version wrote carries no shape
    properties, and the schema and `sort_by` come from its Iceberg schema and
    declared sort order instead.

    Falsify by reading only the properties: the restore finds no shape.
    """
    from litelink._table import SCHEMA_PROPERTY, SORT_PROPERTY

    first = tmp_path / "first"
    with published_log(first, bucket, s3) as log:
        log.extend(rows(ROWS))
        log.advance(flush=True)
        table = log._published.require()._table  # noqa: SLF001

        assert SCHEMA_PROPERTY in table.properties, "publish stamps the shape"

        with table.transaction() as transaction:
            transaction.remove_properties(SCHEMA_PROPERTY, SORT_PROPERTY)

    shutil.rmtree(first)
    with litelink.restore(
        tmp_path / "second", "s", published=f"s3://{bucket}/prefix", s3_options=s3
    ) as revived:
        assert revived.schema == SCHEMA
        assert revived.sort_by == ("event_ts",)
        assert revived.scan().read_all().num_rows == ROWS


def test_restore_without_a_replica_or_a_published_table_refuses(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """No replica and no published rows: nothing to restore, and nothing left
    behind on disk."""
    target = tmp_path / "second"
    with pytest.raises(FileNotFoundError, match="no published rows"):
        litelink.restore(target, "s", published=f"s3://{bucket}/nothing", s3_options=s3)

    assert not (target / "s" / "buffer.db").exists()
    assert not (target / "s" / "catalog.db").exists()


def test_restore_rebuilds_from_a_local_published_table(tmp_path: Path) -> None:
    """A local published table has no replica beside it, so restore rebuilds
    from the table directly — a published directory on a volume that outlived
    the log's own."""
    where = f"file://{tmp_path / 'published'}"
    first = tmp_path / "first"
    with litelink.new(
        first,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=_restore_settings(),
        published=where,
    ) as log:
        log.extend(rows(ROWS))
        log.advance(flush=True)
        published_end = log.published_through() + 1

    shutil.rmtree(first)
    with litelink.restore(tmp_path / "second", "s", published=where) as revived:
        assert revived.end_offset() == published_end + PUBLISHED_RESTORE_RESERVE
        assert revived.scan().read_all().num_rows == ROWS
        revived.extend(rows(100))
        revived.advance(flush=True)

        assert revived.scan().read_all().num_rows == ROWS + 100


def test_an_open_group_reseeds_above_what_is_published_not_only_staging(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """With the WAL replicated, the buffer keeps rows until the published table
    has them, and eviction can empty staging while it still does. An open group
    rebuilt then must start above the published end, or it adopts rows already
    sealed and published and seals them a second time — a file the seal's
    register declines, since the published table holds the range, but a whole
    rewrite of the band for nothing.

    Falsify by seeding from staging's `extent` alone (`_seed_group`): the
    reopened log's seal finds the published rows to write.
    """
    with published_log(
        tmp_path,
        bucket,
        s3,
        wal_replication=True,
        staging_retention=timedelta(0),
        staging_rows=0,
    ) as log:
        log.extend(rows(ROWS))
        log.seal(flush=True)
        log.publish(flush=True)
        log.evict("staging")
        log.reclaim("staging")

        assert log.staging_rows() == 0, "the setup must evict staging to nothing"
        assert log.buffered_rows() > 0, "and keep the published rows buffered"

        # What a sealer dying with the open group closed leaves behind.
        with log._buffer._lock:  # noqa: SLF001
            log._buffer._con.execute(  # noqa: SLF001
                "DELETE FROM extent WHERE end_offset IS NULL AND rel_path IS NULL"
            )
            log._buffer._con.commit()  # noqa: SLF001

    with litelink.open(tmp_path, "s", s3_options=s3) as reopened:
        # Everything is sealed and published, so there is nothing to seal. A
        # group that adopted the buffered rows writes them out again; the
        # seal's register then declines the file, since the published table
        # holds the range, so it lands nowhere — which is why this asserts on
        # the seal and not on what is read.
        assert reopened.seal(flush=True) is None, "a published row was sealed again"
        assert reopened.scan().read_all().num_rows == ROWS
