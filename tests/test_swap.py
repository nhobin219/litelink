"""Publishing early, then swapping the merged file in (#160).

A flushed publish pushes files compaction is not finished with. They stay
recompaction candidates: compaction merges them like any other, and `publish`
replaces their published copies with the merged file in one commit over the
same rows — extending the published table when the merged file reaches past it.
Every log has a published table (#98), a local directory by default, so none of
this needs object storage.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from litelink import LogConfig, WriteHandle
from litelink._layout import is_compacted
from litelink._table import LogTable
from tests.test_log import open_log, read_all, rows

if TYPE_CHECKING:
    from litelink._table import DataFile

PER_SEAL = 4


def seal(log: WriteHandle, index: int) -> None:
    """The `index`th seal of `PER_SEAL` rows: offsets from `index * PER_SEAL + 1`."""
    log.extend(rows(PER_SEAL, start=index * PER_SEAL))
    log.seal(flush=True)


def published_files(log: WriteHandle) -> list[DataFile]:
    published = log._published.require()
    published.reload()

    return published.data_files()


def sized_log(tmp_path: Path, **overrides: Any) -> WriteHandle:
    """A log whose compaction target holds three seals on disk, so the fourth
    seal closes a run of three."""
    config = LogConfig(
        target_seal_size=1 << 30,
        compact_min_files=2,
        staging_snapshot_retention=timedelta(0),
        published_snapshot_retention=timedelta(0),
        **overrides,
    )
    log = open_log(tmp_path, config)
    seal(log, 0)
    size = log._table.data_files()[0].size
    log.set_config(replace(config, target_compact_size=int(size * 3.5)))

    return log


def test_a_flushed_publish_is_swapped_for_the_merged_file(tmp_path: Path) -> None:
    """The straddling case: the merged file holds two published seals and one
    not yet pushed, so one commit replaces the two and extends past them."""
    with sized_log(tmp_path) as log:
        seal(log, 1)
        log.publish(flush=True)
        early = {f.path for f in published_files(log)}
        assert len(early) == 2
        assert log.published_through() == 2 * PER_SEAL

        seal(log, 2)
        seal(log, 3)  # closes the run of three
        log.advance()

        files = published_files(log)
        assert is_compacted(files[0].path), "the merged file replaced the seals"
        assert (files[0].start, files[0].end) == (1, 3 * PER_SEAL + 1)
        assert not early & {f.path for f in files}, "no early copy is left"
        assert log.published_through() == 3 * PER_SEAL, "and it extended past them"
        # Queued for deletion before the commit; with no snapshot retention,
        # this pass's own drain has already removed them.
        assert not any(Path(p.removeprefix("file://")).exists() for p in early), (
            "the replaced copies are deleted, not leaked"
        )
        assert log._buffer.awaiting_swaps() == set()
        assert len(read_all(log)) == 4 * PER_SEAL, "every row, exactly once"


def test_a_pure_swap_leaves_the_watermark_where_it_was(tmp_path: Path) -> None:
    """Every input published early: the swap replaces them and covers no new
    row, so the watermark does not move."""
    with sized_log(tmp_path) as log:
        for index in range(1, 4):
            seal(log, index)

        log.publish(flush=True)
        through = log.published_through()
        assert through == 4 * PER_SEAL

        log.advance()

        files = published_files(log)
        assert is_compacted(files[0].path)
        assert (files[0].start, files[0].end) == (1, 3 * PER_SEAL + 1)
        assert log.published_through() == through
        assert [f.start for f in files] == [1, 3 * PER_SEAL + 1], "seamless"
        assert len(read_all(log)) == 4 * PER_SEAL


def test_eviction_holds_a_recompaction_candidate(tmp_path: Path) -> None:
    """Dropped before it is merged, an early copy would stay in the published
    table at seal size for good. Once swapped, the merged file goes like any
    published file."""
    with sized_log(tmp_path, staging_retention=timedelta(0)) as log:
        seal(log, 1)
        log.publish(flush=True)
        log.advance()

        assert len(log._table.data_files()) == 2, "candidates stay local"

        seal(log, 2)
        seal(log, 3)
        log.advance()

        staging = log._table.data_files()
        assert [(f.start, f.end) for f in staging] == [(3 * PER_SEAL + 1, 17)], (
            "the swapped file is evicted; only the unpublished seal is left"
        )
        assert len(read_all(log)) == 4 * PER_SEAL


def test_a_swap_interrupted_before_its_commit_is_retried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Uploaded, never committed: the merged file is still owed, so the next
    pass uploads and commits it."""
    with sized_log(tmp_path) as log:
        seal(log, 1)
        log.publish(flush=True)
        seal(log, 2)
        seal(log, 3)

        real = LogTable.replace_range

        def dying(self: LogTable, *args: Any, **kwargs: Any) -> None:
            if self._is_published:
                msg = "the process died before the commit"
                raise RuntimeError(msg)

            real(self, *args, **kwargs)

        monkeypatch.setattr(LogTable, "replace_range", dying)
        with pytest.raises(RuntimeError, match="died before the commit"):
            log.advance()

        monkeypatch.undo()
        assert len(log._buffer.awaiting_swaps()) == 1, "still owed"
        assert len(log._buffer.intents(log._published.uri)) == 1, "intended"
        assert len(published_files(log)) == 2, "the early copies still serve"
        assert len(read_all(log)) == 4 * PER_SEAL

        log.advance()

        assert is_compacted(published_files(log)[0].path)
        assert log._buffer.awaiting_swaps() == set()
        assert log._buffer.intents(log._published.uri) == [], "intent resolved"
        assert len(read_all(log)) == 4 * PER_SEAL


def test_a_flushed_advance_does_not_merge_the_trailing_run(tmp_path: Path) -> None:
    """Merging it would make one small, final file of it on every flush; the
    seals go as they are and stay candidates."""
    with sized_log(tmp_path) as log:
        seal(log, 1)
        log.advance(flush=True)

        assert len(log._table.data_files()) == 2
        assert not any(is_compacted(f.path) for f in log._table.data_files())
        assert log.published_through() == 2 * PER_SEAL
