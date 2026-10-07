"""Restore brings the published table's tail back into staging (#166).

A restore rebuilds staging empty. The files compaction was still working on —
seals a flushed publish pushed early, an in-progress file uploaded unfinished —
come back as recompaction candidates, so the restored log merges them and
swaps the result in, as the machine that wrote them would have.
"""

from __future__ import annotations

import shutil
import sqlite3
from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

import litelink
from litelink import LogConfig, WriteHandle
from litelink._layout import Layout, is_compacted
from litelink._table import LogTable
from tests.test_log import SCHEMA, rows

if TYPE_CHECKING:
    from pathlib import Path

    from litelink._s3 import S3Options
    from litelink._table import DataFile

PER_SEAL = 4


def seal(log: WriteHandle, index: int) -> None:
    log.extend(rows(PER_SEAL, start=index * PER_SEAL))
    log.seal(flush=True)


def published_files(log: WriteHandle) -> list[DataFile]:
    published = log._published.require()
    published.reload()

    return published.data_files()


def flushing_log(
    root: Path, where: str, s3: S3Options
) -> tuple[WriteHandle, LogConfig]:
    """A log that has flushed a few seals early, under a target of three seals."""
    config = LogConfig(
        target_seal_size=1 << 30,
        target_row_group_rows=PER_SEAL,
        compact_min_files=2,
        staging_snapshot_retention=timedelta(0),
        published_snapshot_retention=timedelta(0),
    )
    log = litelink.new(
        root,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
        published=where,
        s3_options=s3,
    )
    seal(log, 0)
    config = replace(config, target_compact_size=log._table.data_files()[0].size * 3)
    log.set_config(config)
    for index in range(1, 3):
        seal(log, index)
        log.advance(flush=True)

    return log, config


def work_until_swapped(log: WriteHandle, early: set[str], start: int) -> int:
    """Seal and flush until no early copy is left in the published table; the
    next seal's index."""
    index = start
    while early & {f.path for f in published_files(log)}:
        assert index < start + 60, "the restored tail was never swapped out"
        seal(log, index)
        log.advance(flush=True)
        index += 1

    return index


def test_a_restore_from_the_published_table_recompacts_its_tail(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    where = f"s3://{bucket}/tail"
    first = tmp_path / "first"
    log, config = flushing_log(first, where, s3)
    early = {f.path for f in published_files(log) if not is_compacted(f.path)}
    assert early, "the case needs early seals in the published table"
    log.close()
    shutil.rmtree(first)

    with litelink.restore(
        tmp_path / "second", "s", published=where, s3_options=s3, config=config
    ) as restored:
        staging = restored._table.data_files()
        assert staging, "the tail came back"
        published = restored._published.require()
        uris = {published.uri(restored._layout.relative(f.path)) for f in staging}
        assert uris <= {f.path for f in published_files(restored)}, (
            "under the paths the published table knows them by"
        )
        assert restored.published_through() == staging[-1].end - 1

        index = work_until_swapped(restored, early, 3)
        # The three seals published before the restore, and every one since.
        assert restored.scan().read_all().num_rows == index * PER_SEAL


def test_a_restore_from_a_replica_recompacts_its_tail(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A hand-placed `buffer.db` is the replica path, which trims the buffer
    below the published frontier — so no row is both buffered and restored."""
    where = f"s3://{bucket}/replica-tail"
    first = tmp_path / "first"
    log, config = flushing_log(first, where, s3)
    early = {f.path for f in published_files(log) if not is_compacted(f.path)}
    assert early

    second = tmp_path / "second"
    (second / "s").mkdir(parents=True)
    source = sqlite3.connect(Layout(first, "s").buffer_db)
    copy = sqlite3.connect(Layout(second, "s").buffer_db)
    source.backup(copy)
    source.close()
    copy.close()
    log.close()

    with litelink.restore(second, "s", published=where, s3_options=s3) as restored:
        restored.set_config(config)
        assert restored._table.data_files(), "the tail came back"
        offsets = restored.scan().read_all().column("litelink_offset").to_pylist()
        assert len(offsets) == len(set(offsets)), "no row twice"

        work_until_swapped(restored, early, 3)
        offsets = restored.scan().read_all().column("litelink_offset").to_pylist()
        assert len(offsets) == len(set(offsets)), "no row twice"


def test_eviction_keeps_a_restored_tail_until_it_is_swapped(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Restored candidates are published already, so I4 alone would let even
    an eager retention drop them — and they would stay small for good."""
    where = f"s3://{bucket}/held"
    first = tmp_path / "first"
    log, config = flushing_log(first, where, s3)
    log.close()
    shutil.rmtree(first)

    with litelink.restore(
        tmp_path / "second",
        "s",
        published=where,
        s3_options=s3,
        config=replace(config, staging_retention=timedelta(0)),
    ) as restored:
        tail = restored._table.span()
        assert tail is not None
        restored.advance()

        # Merged into an in-progress file by now, perhaps, but local either way.
        local = restored._table.span()
        assert local is not None and local[0] <= tail[0] and tail[1] <= local[1], (
            "the restored tail stays local until it is swapped"
        )


def test_a_tail_that_cannot_be_downloaded_does_not_fail_the_restore(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The log exists and works by the time the tail is fetched, and `restore`
    refuses to run over a log that exists — so a failed download leaves the
    tail published at its size, not a restore that cannot be retried."""
    where = f"s3://{bucket}/unreachable"
    first = tmp_path / "first"
    log, config = flushing_log(first, where, s3)
    log.close()
    shutil.rmtree(first)

    def refused(self: LogTable, uri: str, destination: Path) -> None:
        destination.write_bytes(b"partial")
        msg = "the bucket refused the download"
        raise OSError(msg)

    monkeypatch.setattr(LogTable, "get", refused)
    with litelink.restore(
        tmp_path / "second", "s", published=where, s3_options=s3, config=config
    ) as restored:
        monkeypatch.undo()
        assert restored._table.data_files() == [], "nothing half-registered"
        assert restored.scan().read_all().num_rows == 3 * PER_SEAL
        seal(restored, 3)
        restored.advance(flush=True)
        assert restored.scan().read_all().num_rows == 4 * PER_SEAL
