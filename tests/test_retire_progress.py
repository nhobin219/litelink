"""Retirement makes progress even when retained buffer rows are already published."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from litelink import WriteHandle
from tests.test_log import rows
from tests.test_published import local_log, offsets

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize("tail", [0, 1])
def test_buffered_rows_excludes_published_rows_after_staging_eviction(
    tmp_path: Path, tail: int
) -> None:
    """Count above both tiers, converting the inclusive watermark to an end.

    Falsify by counting from staging alone, or by omitting the watermark's +1:
    retained published rows count as unsealed.
    """
    with local_log(tmp_path, staging_retention=timedelta(0)) as log:
        log.extend(rows(4))
        assert log.buffered_rows() == 4
        log.seal(flush=True)
        assert log.buffered_rows() == 0
        log.evict("buffer")
        log.publish(flush=True)
        log.evict("staging")
        assert log.staging_extent() is None
        assert log.published_through() == 4
        assert log._buffer.span() == (1, 5)  # noqa: SLF001

        log.extend(rows(tail))
        assert log.buffered_rows() == tail
        assert offsets(log) == list(range(1, 5 + tail))


def test_retire_does_not_reseal_published_rows_without_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The split eviction/publish ordering must not spin in the seal loop.

    Empty staging still cannot authorize buffer deletion (§8), so retirement
    refuses cleanup and keeps the rows. Falsify by counting from staging only:
    the bounded seal hook observes the otherwise endless second iteration.
    """
    with local_log(tmp_path, staging_retention=timedelta(0)) as log:
        log.extend(rows(4))
        log.seal(flush=True)
        log.evict("buffer")
        log.publish(flush=True)
        log.evict("staging")
        assert log.staging_extent() is None
        assert log._buffer.span() == (1, 5)  # noqa: SLF001
        real_seal = log.seal
        calls = 0

        def seal(self: WriteHandle, *, flush: bool = False) -> int | None:
            nonlocal calls
            calls += 1
            assert calls == 1, "retire repeated a seal that could not make progress"
            return real_seal(flush=flush)

        monkeypatch.setattr(WriteHandle, "seal", seal)
        with pytest.raises(RuntimeError, match="retire\\(\\) could not empty"):
            log.retire()

        assert calls == 0, "published rows are not an unsealed tail"
        marker = log._buffer.retired()  # noqa: SLF001
        assert marker is not None and marker["state"] == "retiring"
        assert log._buffer.span() == (1, 5)  # noqa: SLF001
        assert offsets(log) == [1, 2, 3, 4]


def test_retire_raises_when_a_seal_makes_no_progress(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stalled seal fails promptly, preserves rows, and permits a retry.

    Falsify by dropping the progress check: the second call trips the bounded
    hook instead of the retirement error. No timeouts or sleeps are needed.
    """
    with local_log(tmp_path) as log:
        log.extend(rows(4))
        calls = 0

        def seal(self: WriteHandle, *, flush: bool = False) -> int | None:
            nonlocal calls
            calls += 1
            assert calls == 1, "retire repeated a seal that could not make progress"
            return None

        with monkeypatch.context() as patch:
            patch.setattr(WriteHandle, "seal", seal)
            with pytest.raises(RuntimeError, match="retire\\(\\).*seal.*progress"):
                log.retire()

        assert calls == 1
        marker = log._buffer.retired()  # noqa: SLF001
        assert marker is not None and marker["state"] == "retiring"
        assert log._buffer.span() == (1, 5)  # noqa: SLF001
        assert log.buffered_rows() == 4
        assert offsets(log) == [1, 2, 3, 4]

        log.retire()
        marker = log._buffer.retired()  # noqa: SLF001
        assert marker is not None and marker["state"] == "retired"
        assert log._buffer.span() is None  # noqa: SLF001
        assert offsets(log) == [1, 2, 3, 4]
