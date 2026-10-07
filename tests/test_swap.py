"""Publishing early, growing one in-progress file, swapping it in finished.

A flushed publish pushes seals compaction is not finished with. They stay
recompaction candidates (#160): compaction rewrites them into the log's one
in-progress file, a step at a time, until that file reaches the target (#162),
and `publish` then replaces their published copies with it in one commit over
the same rows. Every log has a published table (#98), a local directory by
default, so none of this needs object storage.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import pytest

from litelink import LogConfig, WriteHandle
from litelink._layout import is_compacted
from litelink._table import LogTable
from tests.test_log import open_log, read_all, rows

if TYPE_CHECKING:
    from pathlib import Path

    from litelink._table import DataFile

PER_SEAL = 4
# Enough seals for the in-progress file to reach a target of a few seals:
# merged, small seals shed most of their per-file overhead, so it grows slowly.
PASSES = 60


def seal(log: WriteHandle, index: int) -> None:
    """The `index`th seal of `PER_SEAL` rows: offsets from `index * PER_SEAL + 1`."""
    log.extend(rows(PER_SEAL, start=index * PER_SEAL))
    log.seal(flush=True)


def published_files(log: WriteHandle) -> list[DataFile]:
    published = log._published.require()
    published.reload()

    return published.data_files()


def sized_log(tmp_path: Path, **overrides: Any) -> WriteHandle:
    """A log whose compaction target is three seals on disk, with a row group
    per seal so a file can be cut after any of them."""
    config = LogConfig(
        target_seal_size=1 << 30,
        target_row_group_rows=PER_SEAL,
        compact_min_files=2,
        staging_snapshot_retention=timedelta(0),
        published_snapshot_retention=timedelta(0),
        **overrides,
    )
    log = open_log(tmp_path, config)
    seal(log, 0)
    size = log._table.data_files()[0].size
    log.set_config(replace(config, target_compact_size=size * 3))

    return log


def finished(log: WriteHandle, files: list[DataFile]) -> list[DataFile]:
    return [
        f for f in files if is_compacted(f.path) and f.size >= log.config.compact_size
    ]


def test_a_flushing_log_publishes_every_pass_and_swaps_in_finished_files(
    tmp_path: Path,
) -> None:
    """The whole lifecycle under `advance(flush=True)`: every pass publishes
    what it sealed; the in-progress file absorbs those seals locally; once it is
    finished, it replaces their early copies — and an unfinished one is never
    uploaded."""
    with sized_log(tmp_path) as log:
        for index in range(1, PASSES):
            seal(log, index)
            log.advance(flush=True)

            assert log.published_through() == (index + 1) * PER_SEAL, (
                "a flushed pass publishes everything it sealed"
            )
            files = published_files(log)
            assert not [
                f
                for f in files
                if is_compacted(f.path) and f.size < log.config.compact_size
            ], "an in-progress file is never uploaded"
            assert all(a.end <= b.start for a, b in zip(files, files[1:], strict=False))

        swapped = finished(log, published_files(log))
        assert swapped, "finished files replaced their early copies"
        assert swapped[0].start == 1
        assert len(read_all(log)) == PASSES * PER_SEAL, "every row, exactly once"


def test_without_flush_a_finished_file_is_published_as_it_is(
    tmp_path: Path,
) -> None:
    """Nothing is published early, so nothing is owed a swap: the in-progress
    file is pushed once, when it is finished."""
    with sized_log(tmp_path) as log:
        for index in range(1, PASSES):
            seal(log, index)
            log.advance()

        files = published_files(log)
        assert files, "finished files were published"
        assert all(f.size >= log.config.compact_size for f in files)
        assert all(is_compacted(f.path) for f in files)
        assert log._buffer.awaiting_swaps() == set()
        assert len(read_all(log)) == PASSES * PER_SEAL


def test_eviction_keeps_the_in_progress_region_until_its_swap(
    tmp_path: Path,
) -> None:
    """Dropped before it is merged, an early copy would stay in the published
    table at seal size for good; dropped before its swap, a finished file would
    never replace them. So on every pass, each early copy still published has
    its rows local, and a swapped file goes like any published file."""
    with sized_log(tmp_path, staging_retention=timedelta(0)) as log:
        for index in range(1, PASSES):
            seal(log, index)
            log.advance(flush=True)

            local = log._table.data_files()
            for early in published_files(log):
                if is_compacted(early.path):
                    continue

                assert any(
                    f.start <= early.start and early.end <= f.end for f in local
                ), f"early copy [{early.start}, {early.end}) has nothing to replace it"

        log.advance(flush=True)
        assert not finished(log, log._table.data_files()), "a swapped file is evicted"
        assert finished(log, published_files(log))
        assert len(read_all(log)) == PASSES * PER_SEAL


def test_a_flushed_advance_never_folds_unpublished_rows_into_the_file(
    tmp_path: Path,
) -> None:
    """Published before compacting: the in-progress file only absorbs seals the
    published table already has, so it never straddles the floor and a flush
    is never kept from publishing a row."""
    with sized_log(tmp_path) as log:
        for index in range(1, PASSES):
            seal(log, index)
            log.advance(flush=True)
            floor = log.published_through() + 1
            assert all(
                f.end <= floor or f.start >= floor for f in log._table.data_files()
            )


def test_a_swap_interrupted_before_its_commit_is_retried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Uploaded, never committed: the finished file is still owed, so the next
    pass uploads and commits it."""
    real = LogTable.replace_range

    def dying(self: LogTable, *args: Any, **kwargs: Any) -> None:
        if self._is_published:
            msg = "the process died before the commit"
            raise RuntimeError(msg)

        real(self, *args, **kwargs)

    with sized_log(tmp_path) as log:
        monkeypatch.setattr(LogTable, "replace_range", dying)
        died = None
        for index in range(1, PASSES):
            seal(log, index)
            try:
                log.advance(flush=True)
            except RuntimeError:
                died = index
                break

        assert died is not None, "a finished file was owed a swap"
        monkeypatch.undo()
        assert log._buffer.intents(log._published.uri), "intended, not committed"
        assert not finished(log, published_files(log)), "the early copies serve"
        assert len(read_all(log)) == (died + 1) * PER_SEAL

        log.advance(flush=True)

        assert finished(log, published_files(log))
        assert log._buffer.intents(log._published.uri) == [], "intent resolved"
        assert len(read_all(log)) == (died + 1) * PER_SEAL
