"""The three roles, as three real processes, against real object storage.

Everything else in the suite runs the roles in one process, where a Python lock
can paper over an ordering the design says the leases must handle. This runs
them the way §1 says to deploy them — a writer appending, a maintainer sealing
and compacting and syncing, a reader querying across all three tiers — as
separate OS processes with nothing shared but the log directory, SQLite, and a
bucket.

What it is really testing is that the coordination is durable rather than
in-memory. Separate processes share no GIL, no lock and no cache: they agree
only through the `lease` table, the seal queue and the Iceberg commits. If any
of that were actually relying on being in one process, this is where it shows.
"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

    from litelink._s3 import S3Options

pytestmark = pytest.mark.s3

ROWS = 6000
BATCH = 50

# Small enough that the writer crosses it many times, so the maintainer has
# real work and the reader sees the layout change underneath it.
TARGET = 32 * 1024

PRELUDE = """
import time
import pyarrow as pa
from datetime import timedelta
from litelink import Log, LogConfig
from litelink._s3 import S3Options

SCHEMA = pa.schema([
    pa.field("event_ts", pa.int64()),
    pa.field("key", pa.string()),
    pa.field("payload", pa.string()),
])
S3 = S3Options(endpoint={endpoint!r}, access_key={access_key!r},
               secret_key={secret_key!r}, region={region!r})
ROOT = {root!r}
ARCHIVE = {archive!r}
ROWS = {rows}
BATCH = {batch}
TARGET = {target}
"""


def script(body: str, root: Path, archive: str, s3: S3Options) -> str:
    return PRELUDE.format(
        endpoint=s3.endpoint,
        access_key=s3.access_key,
        secret_key=s3.secret_key,
        region=s3.region,
        root=str(root),
        archive=archive,
        rows=ROWS,
        batch=BATCH,
        target=TARGET,
    ) + textwrap.dedent(body)


def spawn(source: str) -> subprocess.Popen[str]:
    return subprocess.Popen(  # noqa: S603
        [sys.executable, "-c", source],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def finish(process: subprocess.Popen[str], role: str) -> str:
    out, err = process.communicate(timeout=180)
    assert process.returncode == 0, f"{role} failed:\n{err}"

    return out


WRITER = """
    config = LogConfig(
        target_seal_size=TARGET,
        compact_min_files=2,
        local_retention=timedelta(seconds=0),
        snapshot_retention=timedelta(seconds=0),
    )
    log = Log.new(ROOT, "s", schema=SCHEMA, sort_by=("event_ts",),
                  config=config, archive=ARCHIVE, s3=S3)
    print("ready", flush=True)
    for start in range(0, ROWS, BATCH):
        log.extend([
            {"event_ts": i, "key": f"k{i % 7}", "payload": "p" * 64}
            for i in range(start, start + BATCH)
        ])
        time.sleep(0.002)
    log.close()
    print(f"wrote {ROWS}", flush=True)
"""

# Seals, compacts, evicts and pushes — nothing else. It never appends, and it
# holds the seal and maintain leases the writer therefore never takes.
MAINTAINER = """
    log = Log.open(ROOT, "s", s3=S3)
    passes = 0
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        log.seal_due()
        try:
            log.maintain()
            log.sync()
        except RuntimeError:
            pass
        passes += 1
        if log.table_rows() >= ROWS and not log._buffer.rows_above(0).num_rows:
            # One more of each before leaving. Eviction reads the archive
            # watermark, so it can only remove what the PREVIOUS sync pushed —
            # stopping the moment everything is sealed would leave the last
            # files local and the tier untested.
            log.maintain()
            log.sync()
            log.maintain()
            break
        time.sleep(0.05)
    log.close()
    print(f"passes {passes}", flush=True)
"""

# Reads while the other two work, and checks the one invariant that has to hold
# at every instant: whatever is visible is a contiguous run of offsets from 1,
# each exactly once. A seal, a compaction, an eviction or a sync landing
# mid-query would show up here as a gap or a repeat.
READER = """
    log = Log.open(ROOT, "s", read_only=True, s3=S3)
    samples, high = 0, 0
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        offsets = log.sql(
            "SELECT * FROM log", include_archive=True
        ).read_all().column("litelink_offset").to_pylist()
        if offsets:
            assert len(set(offsets)) == len(offsets), "duplicate offsets"
            assert sorted(offsets) == list(range(1, max(offsets) + 1)), "gap"
            assert max(offsets) >= high, "the visible stream went backwards"
            high = max(offsets)
            samples += 1
        if high >= ROWS:
            break
        time.sleep(0.05)
    log.close()
    print(f"samples {samples} high {high}", flush=True)
"""


def test_writer_maintainer_and_reader_run_as_separate_processes(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """All three at once, sharing only the log directory and the bucket."""
    root = tmp_path / "log"
    archive = f"s3://{bucket}/prefix"

    writer = spawn(script(WRITER, root, archive, s3))
    # The maintainer and the reader both `open()`, which needs the log to
    # exist. Waiting for the writer's first line is the handshake — a retry
    # loop would test the retry loop rather than the roles.
    assert writer.stdout is not None
    assert writer.stdout.readline().strip() == "ready"

    maintainer = spawn(script(MAINTAINER, root, archive, s3))
    reader = spawn(script(READER, root, archive, s3))

    written = finish(writer, "writer")
    maintained = finish(maintainer, "maintainer")
    observed = finish(reader, "reader")

    assert f"wrote {ROWS}" in written
    assert "passes" in maintained
    assert f"high {ROWS}" in observed, (
        f"the reader never caught up to the writer: {observed.strip()}"
    )


def test_the_archive_actually_took_part(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The same run, then asserted from outside: rows really did leave local
    disk and really are being served from object storage.

    Without this the three-process test above would pass just as well with the
    archive doing nothing — every row would simply still be local.
    """
    from litelink import Log

    root = tmp_path / "log"
    archive = f"s3://{bucket}/prefix"

    writer = spawn(script(WRITER, root, archive, s3))
    assert writer.stdout is not None
    assert writer.stdout.readline().strip() == "ready"
    maintainer = spawn(script(MAINTAINER, root, archive, s3))
    finish(writer, "writer")
    finish(maintainer, "maintainer")

    with Log.open(root, "s", s3=s3) as log:
        watermark = int(log._buffer.get_meta("archive_through") or 0)
        assert watermark > 0, "nothing was ever archived"

        local = log.scan().read_all().num_rows
        assert local < ROWS, "eviction never removed anything from local disk"

        merged = log.sql("SELECT * FROM log", include_archive=True).read_all()
        offsets = merged.column("litelink_offset").to_pylist()
        assert sorted(offsets) == list(range(1, ROWS + 1)), (
            "the union of the tiers must be the whole stream, exactly once"
        )
        assert json.dumps({"archived_through": watermark, "local": local})
