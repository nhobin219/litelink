"""A log a real older release wrote, opened by this one.

The tier statistics (#90) are new state an older log does not have. The
backfill tests elsewhere forge that by deleting the rows; this one doesn't
forge anything. It writes, syncs and evicts a log with litelink 0.5.1 from
PyPI — through 0.5.1's API, `archive=` and `sync()` — in an isolated
environment, then opens it here.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import textwrap
from typing import TYPE_CHECKING

import duckdb
import pytest

import litelink
from litelink import OFFSET
from litelink._read import Reader

if TYPE_CHECKING:
    from pathlib import Path

    from litelink._s3 import S3Options

pytestmark = [pytest.mark.s3, pytest.mark.release]

RELEASE = "0.5.1"
ROWS = 4000

WRITE = textwrap.dedent(
    """
    import json, os
    from datetime import timedelta
    import pyarrow as pa
    import litelink

    options = json.loads(os.environ["LITELINK_TEST_S3"])
    schema = pa.schema([("event_ts", pa.int64()), ("key", pa.string())])
    config = litelink.LogConfig(
        target_seal_size=64 * 1024,
        target_compact_size=64 * 1024,
        compact_min_files=2,
        local_retention=timedelta(0),
        local_rows=1000,
    )
    with litelink.new(
        os.environ["LITELINK_TEST_ROOT"], "s", schema=schema, sort_by=("event_ts",),
        config=config, archive=os.environ["LITELINK_TEST_ARCHIVE"],
        s3=litelink.S3Options(**options),
    ) as log:
        rows = int(os.environ["LITELINK_TEST_ROWS"])
        log.extend({"event_ts": i, "key": "k" * 64} for i in range(rows))
        log.seal()
        log.sync(push_unsettled=True)
        log.maintain()
        print(json.dumps({"version": litelink.__version__,
                          "extent": log.table_extent()}))
    """
)


def written_by_release(tmp_path: Path, bucket: str, s3: S3Options) -> dict:
    """Run WRITE under the released litelink; return what it reported."""
    if shutil.which("uv") is None:
        pytest.skip("uv is not on PATH")

    resolved = s3.resolved()
    env = {
        **os.environ,
        "LITELINK_TEST_ROOT": str(tmp_path),
        "LITELINK_TEST_ARCHIVE": f"s3://{bucket}/prefix",
        "LITELINK_TEST_ROWS": str(ROWS),
        "LITELINK_TEST_S3": json.dumps(
            {
                "endpoint": resolved.endpoint,
                "access_key": resolved.access_key,
                "secret_key": resolved.secret_key,
                "region": resolved.region,
            }
        ),
    }
    # Out of this checkout: `uv run` would otherwise resolve the project and
    # import this source tree rather than the release.
    done = subprocess.run(  # noqa: S603
        [
            "uv",
            "run",
            "--no-project",
            "--isolated",
            "--with",
            f"litelink=={RELEASE}",
            "python",
            "-c",
            WRITE,
        ],  # fmt: skip
        env=env,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    if done.returncode != 0 and "Failed to fetch" in done.stderr:
        pytest.skip(f"could not install litelink {RELEASE}: no network")

    assert done.returncode == 0, done.stderr
    report = json.loads(done.stdout.strip().splitlines()[-1])
    assert report["version"] == RELEASE, "the release, not this checkout, wrote it"

    return report


def test_a_log_a_released_version_wrote_reads_back_and_backfills(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Opened here, a 0.5.1 log gets its published tier row from the published
    table's own manifests, then serves every row, and a hot read stops reaching
    the published table.

    Falsify by removing `_backfill_manifest` from `litelink.open`: the tier
    row is never stored, and the hot read reaches the published table.
    """
    report = written_by_release(tmp_path, bucket, s3)
    low = report["extent"][0]
    assert 1 < low <= ROWS, "the release must have evicted part of the log"

    with litelink.open(tmp_path, "s", s3_options=s3) as log:
        coverage = log.coverage()
        assert coverage.published == (1, low)
        offsets = log.scan(columns=[OFFSET]).read_all().column(0).to_pylist()
        assert sorted(offsets) == list(range(1, ROWS + 1))

        reached: list[object] = []
        original = Reader._prepare_remote  # noqa: SLF001

        def counted(reader: Reader, cursor: duckdb.DuckDBPyConnection):  # noqa: ANN202
            reached.append(cursor)
            return original(reader, cursor)

        monkeypatch.setattr(Reader, "_prepare_remote", counted)
        hot = log.scan(start_offset=low).read_all()

        assert hot.num_rows == ROWS - low + 1
        assert reached == [], "backfilled, the hot read stays local"
