"""The core capture loop: append, seal, read, recover."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from litelink.log import Log, LogConfig

if TYPE_CHECKING:
    from pathlib import Path

SCHEMA = pa.schema(
    [
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("key", pa.string()),
        pa.field("payload", pa.large_binary()),
    ]
)


def open_log(root: Path, config: LogConfig | None = None) -> Log:
    return Log(root, "s", schema=SCHEMA, sort_by=("event_ts", "key"), config=config)


def rows(n: int, *, start: int = 0) -> list[dict[str, object]]:
    return [
        {"event_ts": 1000 + i, "key": f"k{i % 3}", "payload": b"x" * 16}
        for i in range(start, start + n)
    ]


def read_all(log: Log) -> list[tuple[object, ...]]:
    return [tuple(r.values()) for r in log.scan().read_all().to_pylist()]


def test_append_returns_monotonic_offsets(tmp_path: Path) -> None:
    with open_log(tmp_path) as log:
        assert log.append(rows(1)[0]) == 1
        assert log.extend(rows(3)) == [2, 3, 4]
        assert log.end_offset() == 5


def test_caller_supplied_offset_is_rejected(tmp_path: Path) -> None:
    """I11: the library owns `offset` and never accepts one."""
    with open_log(tmp_path) as log:
        with pytest.raises(ValueError, match="I11"):
            log.append({"offset": 7, "event_ts": 1, "key": "k", "payload": b""})


def test_offset_in_schema_is_rejected(tmp_path: Path) -> None:
    """I11 again, one layer earlier."""
    with pytest.raises(ValueError, match="I11"):
        Log(
            tmp_path,
            "s",
            schema=pa.schema([pa.field("offset", pa.int64())]),
            sort_by=(),
        )


def test_read_before_any_seal_sees_the_buffer(tmp_path: Path) -> None:
    with open_log(tmp_path) as log:
        log.extend(rows(5))
        assert len(read_all(log)) == 5


def test_seal_then_read_returns_every_row_once(tmp_path: Path) -> None:
    """The union must not double-count across the boundary (I3)."""
    with open_log(tmp_path) as log:
        log.extend(rows(10))
        assert log.seal() == 11
        log.extend(rows(5, start=10))

        offsets = [r[0] for r in read_all(log)]
        assert offsets == list(range(1, 16))


def test_seal_empties_the_buffer_but_not_the_log(tmp_path: Path) -> None:
    with open_log(tmp_path) as log:
        log.extend(rows(4))
        log.seal()
        assert log._buffer.extent() is None
        assert len(read_all(log)) == 4


def test_offsets_never_reused_after_the_buffer_empties(tmp_path: Path) -> None:
    """I9. This is the assertion that fails with a bare INTEGER PRIMARY KEY."""
    with open_log(tmp_path) as log:
        log.extend(rows(3))
        log.seal()
        assert log._buffer.extent() is None
        assert log.extend(rows(1, start=3)) == [4]
        assert log.table_extent() == (1, 3)


def test_sealed_file_is_sorted_by_sort_by(tmp_path: Path) -> None:
    """§4: the sort order is applied at write time, not merely declared."""
    with open_log(tmp_path) as log:
        log.extend(
            [{"event_ts": ts, "key": "k", "payload": b""} for ts in (300, 100, 200)]
        )
        log.seal()

        scanned = log.scan(columns=["event_ts"]).read_all()["event_ts"].to_pylist()
        assert scanned == [300, 100, 200], "scan orders by offset, not by sort_by"

    written = next(tmp_path.rglob("*.parquet"))
    assert pq.read_table(written)["event_ts"].to_pylist() == [100, 200, 300]


def test_reopen_sees_committed_data(tmp_path: Path) -> None:
    with open_log(tmp_path) as log:
        log.extend(rows(6))
        log.seal()
        log.extend(rows(2, start=6))

    with open_log(tmp_path) as reopened:
        assert len(read_all(reopened)) == 8
        assert reopened.end_offset() == 9


def test_recovery_completes_an_interrupted_seal(tmp_path: Path) -> None:
    """Crash between the Iceberg commit and the buffer delete (§4, §11).

    The rows are in the table AND still in the buffer. A read in that window
    must return each exactly once, and reopening must drop the stale rows.
    """
    with open_log(tmp_path) as log:
        log.extend(rows(4))
        extent = log._buffer.extent()
        assert extent is not None
        end = extent[1] + 1
        rel_path = log._seal_path(1, end)
        log._buffer.claim_seal(1, end, rel_path)
        log._write_and_commit(end, rel_path)
        # deliberately NOT finish_seal: this is the crash window
        assert log._buffer.extent() is not None
        assert len(read_all(log)) == 4

    with open_log(tmp_path) as recovered:
        assert recovered._buffer.pending_seal() is None
        assert recovered._buffer.extent() is None
        assert len(read_all(recovered)) == 4


def test_recovery_redoes_a_seal_that_never_committed(tmp_path: Path) -> None:
    """Crash between claiming the range and committing the file (I2).

    The retry must reuse the claimed path rather than compute a new one.
    """
    with open_log(tmp_path) as log:
        log.extend(rows(3))
        log._buffer.claim_seal(1, 4, log._seal_path(1, 4))

    with open_log(tmp_path) as recovered:
        assert recovered._buffer.pending_seal() is None
        assert recovered.table_extent() == (1, 3)
        assert len(read_all(recovered)) == 3
        assert len(list(tmp_path.rglob("*.parquet"))) == 1, "no orphaned file"


def test_target_size_triggers_a_seal(tmp_path: Path) -> None:
    with open_log(tmp_path, LogConfig(target_size=512)) as log:
        log.extend(rows(40))
        assert log.table_extent() is not None, "should have sealed on size"
        assert len(read_all(log)) == 40


def test_local_retention_zero_without_an_archive_is_rejected(tmp_path: Path) -> None:
    """§8: it means 'evict on upload', and there is nothing to upload to."""
    with pytest.raises(ValueError, match="archive"):
        open_log(tmp_path, LogConfig(local_retention=timedelta(0)))


def test_capture_works_with_no_network(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """I5 and §14's first bullet: the central claim.

    Blocks Python-level socket creation, then drives the whole loop. Note the
    limitation: DuckDB and pyiceberg-core reach the network from C++, which
    never passes through `socket.socket`, so this catches a Python-level
    regression (a pyiceberg HTTP call, an S3 client) and not a C++ one. The
    airtight version of this test needs a network namespace, and belongs in a
    suite that can ask for one.
    """
    import socket

    def refuse(*args: object, **kwargs: object) -> None:
        msg = "network access during a hot-path operation"
        raise OSError(msg)

    monkeypatch.setattr(socket, "socket", refuse)

    with open_log(tmp_path, LogConfig(target_size=256)) as log:
        log.extend(rows(20))
        log.seal()
        log.extend(rows(20, start=20))
        assert len(read_all(log)) == 40
        assert log.end_offset() == 41
