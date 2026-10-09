"""Truncating a log: its history below an offset dropped from every tier (#80).

Whole files only, never past what the published table holds, and through the
same cycle as every other delete: a commit that queues the files, expiry of
the snapshots still naming them, then the drain once their grace has passed.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from litelink import LogConfig, WriteHandle
from litelink._handle import MAINTAIN_ROLE
from tests.test_log import open_log, rows

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from litelink._table import DataFile

PER_SEAL = 4
SEALS = 5


def offsets(log: WriteHandle) -> list[int]:
    table = log.scan(columns=["litelink_offset"]).read_all()

    return sorted(table.column("litelink_offset").to_pylist())


def published_files(log: WriteHandle) -> list[DataFile]:
    published = log._published.require()
    published.reload()

    return published.data_files()


def staging_files(log: WriteHandle) -> list[DataFile]:
    log._table.reload()

    return log._table.data_files()


def published_log(tmp_path: Path, config: LogConfig | None = None) -> WriteHandle:
    """Five files of four rows, offsets 1-20, each published: files start at
    1, 5, 9, 13 and 17 in both tables."""
    log = open_log(tmp_path, config or LogConfig(target_seal_size=1 << 30))
    for index in range(SEALS):
        log.extend(rows(PER_SEAL, start=index * PER_SEAL))
        log.seal(flush=True)

    log.publish(flush=True)

    return log


def test_truncate_drops_whole_files_below_and_reports_the_floor(
    tmp_path: Path,
) -> None:
    """Offset 11 lies inside the file starting at 9, which stays whole: the
    floor is 9, in both tables. The offset counter does not move.

    Falsify by dropping the snap to a file boundary: the floor reads 11 and
    pyiceberg rewrites the straddling file. Or by keeping the buffer's rows: a
    seal leaves them for `evict("buffer")`, so rows 1-8 stay there.
    """
    with published_log(tmp_path) as log:
        assert log._buffer.span() == (1, 21)
        floor = log.truncate(below=11)

        assert floor == 9
        assert log._buffer.span() == (9, 21)
        assert offsets(log) == list(range(9, 21))
        assert min(f.start for f in staging_files(log)) == 9
        assert min(f.start for f in published_files(log)) == 9
        assert log.coverage().staging == (9, 21)
        assert log.end_offset() == 21
        assert log.append(rows(1)[0]) == 21


def test_truncate_narrows_the_published_tier_row(tmp_path: Path) -> None:
    """With staging evicted, the published table is the log, and its tier row
    is what `coverage` and read routing consult. It narrows to the floor.

    Falsify by skipping `_record_published_row`: coverage still starts at 1.
    """
    with published_log(tmp_path) as log:
        log._maintenance.evict(everything=True)
        assert log.coverage().published == (1, 21)

        assert log.truncate(below=11) == 9

        assert log.coverage().published == (9, 21)
        assert offsets(log) == list(range(9, 21))


def test_truncate_never_passes_what_the_published_table_holds(
    tmp_path: Path,
) -> None:
    """Rows only this machine has are not history. Asked for everything, it
    stops at the watermark, and the next publish carries on from there.

    Falsify by dropping the clamp: the unpublished files go, and offsets 21-28
    are lost.
    """
    with published_log(tmp_path) as log:
        log.extend(rows(PER_SEAL * 2, start=SEALS * PER_SEAL))
        log.seal(flush=True)
        log.extend(rows(2, start=SEALS * PER_SEAL + PER_SEAL * 2))

        assert log.truncate(below=1_000) == 21
        assert offsets(log) == list(range(21, 31))

        log.publish(flush=True)
        assert min(f.start for f in published_files(log)) == 21
        assert log.truncate(below=1_000) == 29


def test_truncated_files_wait_out_the_grace_then_are_deleted(
    tmp_path: Path,
) -> None:
    """No direct deletes: the files are queued and stay on disk through the
    grace, and `reclaim` deletes them once it has passed.

    Falsify by unlinking in `truncate`: the files are gone at the first check.
    """
    with published_log(tmp_path) as log:
        staged = [f.path for f in staging_files(log) if f.end <= 9]
        pushed = [f.path for f in published_files(log) if f.end <= 9]
        assert len(staged) == len(pushed) == 2

        log.truncate(below=9)

        queued = log._buffer.due_deletions(1 << 62)
        for path in staged:
            assert log._maintenance._key(path) in queued

        for path in pushed:
            assert path in queued

        # Within the grace (an hour by default), reclaim deletes none of them.
        log.reclaim()
        assert all(_exists(p) for p in staged + pushed)

        log.set_config(
            replace(
                log.config,
                staging_snapshot_retention=timedelta(0),
                published_snapshot_retention=timedelta(0),
            )
        )
        log.reclaim()
        assert not any(_exists(p) for p in staged + pushed)
        assert offsets(log) == list(range(9, 21))


def test_truncated_rows_do_not_come_back(tmp_path: Path) -> None:
    """Passes after a truncate (seal, publish, compact, evict) work above the
    floor and never put the dropped rows back.

    Falsify by truncating only the published table: the next publish finds
    the staging files below its floor and pushes them again.
    """
    config = LogConfig(target_seal_size=1 << 30, compact_min_files=2)
    with published_log(tmp_path, config) as log:
        assert log.truncate(below=9) == 9

        for index in range(SEALS, SEALS + 3):
            log.extend(rows(PER_SEAL, start=index * PER_SEAL))
            log.advance(flush=True)

        assert offsets(log) == list(range(9, 33))
        assert min(f.start for f in published_files(log)) == 9
        assert min(f.start for f in staging_files(log)) >= 9


def test_truncate_refuses_while_another_owner_holds_a_claim(
    tmp_path: Path,
) -> None:
    """The whole-log claim: nothing runs beside it, and a held claim refuses
    rather than waits."""
    with published_log(tmp_path) as log:
        held = log._lease(MAINTAIN_ROLE, 0, 1)
        assert held.acquire()
        try:
            with pytest.raises(RuntimeError, match="claim"):
                log.truncate(below=9)
        finally:
            held.release()

        assert offsets(log) == list(range(1, 21))
        assert log.truncate(below=9) == 9


def test_a_lapsed_claim_never_narrows_over_an_eviction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The exact narrowing comes after the published commit and its manifest
    walk, which on object storage can outlast the claim's TTL. Lapsed, a
    maintainer's eviction can widen the tier row first, and narrowing over it
    leaves the rows it moved in no tier a read consults. The claim is checked
    after the walk, so the truncate refuses instead, and calling it again
    finishes.

    Falsify by dropping that `checkpoint` from `_record_published_row`: rows
    5-8 are missing from the scan.
    """
    import litelink
    import litelink._handle as handle

    config = LogConfig(
        target_seal_size=1 << 30,
        target_compact_size=1,
        staging_retention=timedelta(0),
    )
    with published_log(tmp_path, config) as log:
        log.evict("buffer")
        maintainer = litelink.open(tmp_path, "s")
        real = handle.checkpoint
        stalled: list[bool] = []

        def stall(renew: Callable[[], bool]) -> None:
            if not stalled:
                stalled.append(True)
                # The walk outran the TTL, and a maintainer got in.
                log._buffer._con.execute("UPDATE claim SET expires_at = 0")
                log._buffer._con.commit()
                maintainer.evict("staging", end_offset=9)

            real(renew)

        monkeypatch.setattr(handle, "checkpoint", stall)
        with pytest.raises(RuntimeError, match="lost the claim"):
            log.truncate(below=5)

        monkeypatch.setattr(handle, "checkpoint", real)
        assert offsets(log) == list(range(5, 21))
        assert log.truncate(below=5) == 5
        assert offsets(log) == list(range(5, 21))
        maintainer.close()


def test_truncate_refuses_a_negative_offset(tmp_path: Path) -> None:
    with published_log(tmp_path) as log, pytest.raises(ValueError, match="offset"):
        log.truncate(below=-1)


def _exists(path: str) -> bool:
    from pathlib import Path

    return Path(path.removeprefix("file://")).exists()
