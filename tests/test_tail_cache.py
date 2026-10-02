"""The buffer's read cache: `Buffer.rows_from` converts the unsealed tail to
Arrow incrementally, and has to answer exactly what an uncached read would.

The cache holds the rows `[_tail_start, _tail_end)` and is complete from
`_tail_complete_from`. Its arithmetic rests on two properties of the buffer —
offsets arrive only above the last one, and leave only as a prefix — and its
history is of silent miscounts: rows hidden from every later query, with no
error anywhere. So these assert answers, not mechanisms, against the uncached
read, over sequences no hand-written case would think of.
"""

from __future__ import annotations

import random
import threading
import time
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

import litelink
from litelink import OFFSET
from litelink._buffer import Buffer
from tests.test_seal_group import open_log, quiet, rows

if TYPE_CHECKING:
    from pathlib import Path

    from litelink import WriteHandle


def offsets(table: pa.Table) -> list[int]:
    return table.column(OFFSET).to_pylist()


def uncached(buffer: Buffer, start: int | None) -> list[int]:
    """What `rows_from(start)` must return: the same query, with no cache."""
    return offsets(buffer._rows(">= ?", (0 if start is None else start,)))  # noqa: SLF001


@pytest.mark.parametrize("seed", range(8))
def test_the_cache_answers_exactly_what_an_uncached_read_would(
    tmp_path: Path, seed: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Appends, prefix deletions, reserved holes, and reads at boundaries that
    rise, fall, land inside the cache, below it, above it, and on its edges.

    One rule from the buffer's contract is kept: rows leave only after a tier
    holds them, so a read never asks below a prefix already deleted — its
    boundary is the staging table's end, which covers what was sealed, and the
    union cuts the buffer leg there again. Below that, rows a deletion took can
    still be in the cache, by design.

    Each read is compared with the uncached query, and the cache must actually
    be reused — a cache that always rebuilt would pass the comparison and
    measure nothing.

    Falsify by any one of: fetching `> _tail_end` instead of `>=` (a row lost
    per refresh); slicing one row too many in `_reusable`; dropping the
    `_tail_complete_from <= start` bound (a high cache served to a low
    boundary); or recording `_tail_start = start` instead of the first row's
    offset. Each fails some seed.
    """
    rng = random.Random(seed)
    hits = 0
    original = Buffer._reusable  # noqa: SLF001

    def counting(self: Buffer, start: int) -> pa.Table | None:
        nonlocal hits
        got = original(self, start)
        hits += got is not None

        return got

    monkeypatch.setattr(Buffer, "_reusable", counting)

    reads = 0
    released = 0
    with open_log(tmp_path, quiet()) as log:
        buffer = log._buffer  # noqa: SLF001
        for step in range(300):
            roll = rng.random()
            if roll < 0.35:
                buffer.append(rows(rng.randint(1, 20)))
            elif roll < 0.45:
                # Rows leave only as a prefix — what a seal does.
                lowest = buffer.lowest_offset()
                if lowest is not None:
                    cut = lowest + rng.randint(0, 15)
                    buffer.evict_rows(0, cut)
                    released = max(released, cut)
            elif roll < 0.5:
                # A hole no buffered row will ever fill, as `ingest` leaves.
                buffer.reserve(rng.randint(1, 5))
            else:
                top = buffer.next_offset() + 3
                if released == 0 and rng.random() < 0.1:
                    start: int | None = None
                elif rng.random() < 0.6:
                    # Mostly near the front, where a real boundary sits.
                    start = released + rng.randint(0, 10)
                else:
                    start = rng.randint(released, max(released, top))

                got = offsets(buffer.rows_from(start))
                assert got == uncached(buffer, start), (
                    f"seed {seed}, step {step}: rows_from({start}) disagreed"
                )
                reads += 1

    # Boundaries here fall as often as they rise, and a fall rebuilds, so this
    # only proves the cache takes part; the rising-boundary tests pin the rate.
    assert hits > reads // 6, f"the cache was reused {hits} times in {reads} reads"


def test_a_read_at_every_boundary_of_one_cache(tmp_path: Path) -> None:
    """The edges, exhaustively: one cache of `[1, 41)`, then every `start`
    from 0 to 45 in both directions, each against the uncached read.

    Falsify by testing `start < _tail_end` instead of `start <= _tail_end`
    in `_reusable`: a boundary at the cache's end rebuilds, and the hit count
    for the rising pass comes up short.
    """
    with open_log(tmp_path, quiet()) as log:
        buffer = log._buffer  # noqa: SLF001
        buffer.append(rows(40))

        for start in [*range(46), *reversed(range(46))]:
            assert offsets(buffer.rows_from(start)) == uncached(buffer, start), start

        # Rising from the bottom, every read but the first is a slice of the
        # first — including `start == 41`, the end, which is an empty answer
        # the cache can give without asking SQLite.
        buffer._drop_tail()  # noqa: SLF001
        hits = 0
        original = Buffer._reusable  # noqa: SLF001
        for start in range(42):
            if original(buffer, start) is not None:
                hits += 1

            buffer.rows_from(start)

        assert hits == 41, f"{hits} of 41 rising reads reused the cache"


def test_reads_racing_seals_see_every_row_exactly_once(tmp_path: Path) -> None:
    """Through the public API and across handles, as a deployment runs it: a
    writer appends and seals while a reader scans in another thread, on its
    own handle and its own cache.

    Each scan must return each offset once, with no gap, and must include every
    row acknowledged before it started. A miscount in the cache's slice shows
    up as a hole above the boundary or a row in both legs.
    """
    done = threading.Event()
    acknowledged = 0
    failures: list[str] = []

    with open_log(tmp_path, quiet()) as writer:
        writer.extend(rows(10))
        writer.seal(flush=True)

        def read() -> None:
            with litelink.open(tmp_path, "s", read_only=True) as reader:
                while not done.is_set():
                    floor = acknowledged
                    got = sorted(offsets(reader.scan(columns=[OFFSET]).read_all()))
                    if got != list(range(1, len(got) + 1)):
                        failures.append(f"not exactly once and gapless: {len(got)}")
                    elif len(got) < floor:
                        failures.append(f"{len(got)} rows, {floor} acknowledged")

        thread = threading.Thread(target=read)
        thread.start()
        try:
            deadline = time.monotonic() + 3
            batch = 0
            while time.monotonic() < deadline:
                writer.extend(rows(7, start=acknowledged))
                acknowledged = writer.end_offset() - 1
                batch += 1
                if batch % 5 == 0:
                    writer.seal(flush=True)
        finally:
            done.set()
            thread.join()

    assert not failures, failures[:5]


def test_a_log_written_before_the_cache_changed_reads_the_same(
    tmp_path: Path,
) -> None:
    """The cache lives in a process's memory, never on disk, so a log needs no
    migration for it: whatever a buffer holds, the first read builds the cache
    from the rows. Asserted on a log with a sealed prefix, a reserved hole and
    a buffered tail, reopened by a fresh handle.
    """
    log: WriteHandle
    with open_log(tmp_path, quiet()) as log:
        log.extend(rows(30))
        log.seal(flush=True)
        log._buffer.reserve(5)  # noqa: SLF001
        log.extend(rows(12, start=30))
        expected = offsets(log.scan(columns=[OFFSET]).read_all())

    with litelink.open(tmp_path, "s") as reopened:
        assert reopened._buffer._tail is None, "a new handle starts with no cache"  # noqa: SLF001
        assert offsets(reopened.scan(columns=[OFFSET]).read_all()) == expected
        assert offsets(reopened.scan(columns=[OFFSET]).read_all()) == expected
