"""Bulk ingest: an Arrow entry point that writes past the SQLite buffer (§13.4)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

import litelink
from litelink._layout import Layout
from litelink.log import LogConfig, WriteHandle

if TYPE_CHECKING:
    from pathlib import Path

SCHEMA = pa.schema(
    [
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("key", pa.string()),
        pa.field("payload", pa.string()),
    ]
)


def open_log(root: Path, config: LogConfig | None = None) -> WriteHandle:
    if Layout(root, "s").buffer_db.exists():
        log = litelink.open(root, "s")
        if config is not None:
            log.set_config(config)

        return log

    return litelink.new(
        root, "s", schema=SCHEMA, sort_by=("event_ts", "key"), config=config
    )


def rows(n: int, *, start: int = 0) -> list[dict[str, object]]:
    return [
        {"event_ts": 1000 + i, "key": f"k{i % 3}", "payload": f'{{"seq":{i}}}'}
        for i in range(start, start + n)
    ]


# -- the reserve (§13.4, stage 2a) ---------------------------------------------


def test_a_reserve_makes_the_next_append_skip_the_range(tmp_path: Path) -> None:
    """I9 asked of the one path that issues offsets without writing rows."""
    with open_log(tmp_path) as log:
        log.extend(rows(3))

        assert log._buffer.reserve(1000) == (4, 1003)
        assert log.end_offset() == 1004
        assert log.append(rows(1)[0]) == 1004


def test_a_reserve_on_an_empty_buffer_starts_at_one(tmp_path: Path) -> None:
    """The sequence row does not exist until the first insert, so a log whose
    whole load arrives through ingest never sees one."""
    with open_log(tmp_path) as log:
        assert log._buffer.reserve(500) == (1, 500)
        assert log.end_offset() == 501


def test_sequential_reserves_are_adjacent(tmp_path: Path) -> None:
    """What makes ingest's per-file ranges contiguous without checking."""
    with open_log(tmp_path) as log:
        first = log._buffer.reserve(10)
        second = log._buffer.reserve(10)
        third = log._buffer.reserve(1)

    assert first == (1, 10)
    assert second == (11, 20)
    assert third == (21, 21)


@pytest.mark.parametrize("count", [0, -1])
def test_reserving_nothing_is_refused(tmp_path: Path, count: int) -> None:
    with open_log(tmp_path) as log, pytest.raises(ValueError, match="at least one"):
        log._buffer.reserve(count)


def test_a_reserve_leaves_the_other_sequences_alone(tmp_path: Path) -> None:
    """`extent.group_id` and `claim.id` are AUTOINCREMENT too, so the UPDATE
    has to be keyed on the buffer's row."""
    with open_log(tmp_path) as log:
        log.extend(rows(3))
        before = dict(
            log._buffer._con.execute("SELECT name, seq FROM sqlite_sequence").fetchall()
        )

        log._buffer.reserve(1000)

        after = dict(
            log._buffer._con.execute("SELECT name, seq FROM sqlite_sequence").fetchall()
        )

    assert before["buffer"] == 3
    assert after["buffer"] == 1003
    assert {k: v for k, v in after.items() if k != "buffer"} == {
        k: v for k, v in before.items() if k != "buffer"
    }


def test_a_reserve_never_lands_on_a_buffered_row(tmp_path: Path) -> None:
    """The floor is `max(seq, max(offset))`. A sequence somehow left below the
    rows present must not hand back offsets those rows already hold."""
    with open_log(tmp_path) as log:
        log.extend(rows(50))
        log._buffer._con.execute("UPDATE sqlite_sequence SET seq = 10")

        lo, hi = log._buffer.reserve(5)

        assert (lo, hi) == (51, 55)
        assert log.append(rows(1)[0]) == 56
