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
import uuid
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from litelink import Log, LogConfig
from litelink._s3 import S3Options
from litelink.log import OFFSET

if TYPE_CHECKING:
    from collections.abc import Iterator

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


def options() -> S3Options:
    """Explicit for rustfs, environment for anything else.

    `just rustfs` is the default because it needs no credentials to exist
    anywhere; exporting `AWS_ENDPOINT_URL` runs the same tests against another
    endpoint, and unsetting it runs them against AWS.
    """
    if os.environ.get("AWS_ENDPOINT_URL"):
        return S3Options().resolved()

    return S3Options(
        endpoint="http://127.0.0.1:9000",
        access_key="litelink",
        secret_key="litelink-secret",
        region="us-east-1",
    ).resolved()


def filesystem(s3: S3Options):  # noqa: ANN201  — s3fs is an optional import
    s3fs = pytest.importorskip("s3fs", reason="the `s3` extra is not installed")

    return s3fs.S3FileSystem(
        key=s3.access_key,
        secret=s3.secret_key,
        client_kwargs={"endpoint_url": s3.endpoint, "region_name": s3.region},
    )


@pytest.fixture(scope="session")
def s3() -> S3Options:
    """The endpoint, or a skip. Reachability is checked once, by listing.

    A connection error means no endpoint is running and the tier is untestable
    here; anything else is a real failure and must not be swallowed into a
    skip, or a broken archive would look like an absent one.
    """
    resolved = options()
    fs = filesystem(resolved)
    try:
        fs.ls("/")
    except OSError as exc:
        pytest.skip(f"no S3 endpoint at {resolved.endpoint}: {exc}")

    return resolved


@pytest.fixture
def bucket(s3: S3Options) -> Iterator[str]:
    """A fresh bucket per test, removed afterwards.

    Per test rather than shared: these assert on object counts and on what a
    catalog holds, and a bucket carrying another test's files makes both
    meaningless.
    """
    fs = filesystem(s3)
    name = f"litelink-test-{uuid.uuid4().hex[:12]}"
    fs.mkdir(name)
    try:
        yield name
    finally:
        fs.rm(name, recursive=True)


def archived_log(root: Path, bucket: str, s3: S3Options, **overrides: object) -> Log:
    config = LogConfig(
        target_size=64 * 1024,
        compact_min_files=2,
        snapshot_retention=timedelta(seconds=0),
        **overrides,  # ty: ignore[invalid-argument-type]
    )

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
