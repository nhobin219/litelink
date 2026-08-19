"""Local storage reclamation: compact, evict, expire (SPEC §6, §8, §12)."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from litelink.log import Log, LogConfig
from tests.test_log import SCHEMA, open_log, read_all, rows

if TYPE_CHECKING:
    from pathlib import Path


def seal_files(log: Log, count: int, per_file: int = 4) -> None:
    """Produce `count` sealed files, each holding `per_file` rows."""
    for i in range(count):
        log.extend(rows(per_file, start=i * per_file))
        log.seal()


def test_compaction_merges_adjacent_small_files(tmp_path: Path) -> None:
    with open_log(
        tmp_path, LogConfig(compact_below=1 << 30, compact_min_files=2)
    ) as log:
        seal_files(log, 4)
        assert len(log._data_files()) == 4

        log.maintain()

        assert len(log._data_files()) == 1
        assert len(read_all(log)) == 16
        assert log.table_extent() == (1, 16)


def test_compaction_needs_compact_min_files(tmp_path: Path) -> None:
    """Below the threshold the pass must leave the files alone."""
    with open_log(
        tmp_path, LogConfig(compact_below=1 << 30, compact_min_files=5)
    ) as log:
        seal_files(log, 4)
        log.maintain()

        assert len(log._data_files()) == 4


def test_compaction_skips_files_over_compact_below(tmp_path: Path) -> None:
    with open_log(tmp_path, LogConfig(compact_below=1, compact_min_files=2)) as log:
        seal_files(log, 3)
        log.maintain()

        assert len(log._data_files()) == 3


def test_compaction_output_is_re_sorted(tmp_path: Path) -> None:
    """§6 step 2: re-sorted, not merely concatenated."""
    import pyarrow.parquet as pq

    config = LogConfig(compact_below=1 << 30, compact_min_files=2)
    with Log(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",), config=config) as log:
        for ts in (500, 100, 400, 200):
            log.append({"event_ts": ts, "key": "k", "payload": b""})
            log.seal()

        log.maintain()

        merged = log._data_files()
        assert len(merged) == 1
        written = pq.read_table(merged[0].path)["event_ts"].to_pylist()
        assert written == [100, 200, 400, 500]


def test_compaction_preserves_every_row(tmp_path: Path) -> None:
    config = LogConfig(compact_below=1 << 30, compact_min_files=2)
    with open_log(tmp_path, config) as log:
        seal_files(log, 5, per_file=3)
        before = read_all(log)

        log.maintain()

        assert read_all(log) == before


def test_eviction_drops_files_past_local_retention(tmp_path: Path) -> None:
    """§8. With no archive this is deletion of the only copy, by design."""
    config = LogConfig(
        compact_min_files=99,  # isolate eviction from compaction
        local_retention=timedelta(microseconds=1),
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        log.extend(rows(2, start=12))
        assert len(log._data_files()) == 3

        log.maintain()

        assert log._data_files() == []
        # The buffer is untouched: retention governs the table, and buffer rows
        # are removed at seal and nowhere else (§8).
        assert len(read_all(log)) == 2


def test_no_eviction_without_local_retention(tmp_path: Path) -> None:
    with open_log(tmp_path, LogConfig(compact_min_files=99)) as log:
        seal_files(log, 3)
        log.maintain()

        assert len(log._data_files()) == 3
        assert len(read_all(log)) == 12


def test_expiry_drops_old_snapshots(tmp_path: Path) -> None:
    config = LogConfig(
        compact_min_files=99, snapshot_retention=timedelta(microseconds=1)
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        assert log._table.inspect.snapshots().num_rows == 3

        log.maintain()

        # The current snapshot is never expired, whatever its age.
        assert log._table.inspect.snapshots().num_rows == 1
        assert len(read_all(log)) == 12


def test_maintain_refuses_when_an_archive_is_configured(tmp_path: Path) -> None:
    """I4 needs sync's registration watermark, and sync does not exist yet."""
    log = Log(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        archive="s3://bucket/prefix",
    )
    with pytest.raises(NotImplementedError, match="sync"):
        log.maintain()

    log.close()


def test_reads_stay_correct_across_a_compaction(tmp_path: Path) -> None:
    """I8: once readable, a row stays readable."""
    config = LogConfig(compact_below=1 << 30, compact_min_files=2)
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        log.extend(rows(4, start=12))
        before = read_all(log)

        log.maintain()

        assert read_all(log) == before
        assert [r[0] for r in read_all(log)] == list(range(1, 17))


def test_eviction_alone_does_not_free_disk(tmp_path: Path) -> None:
    """§12: local disk holds local_retention + snapshot_retention of data.

    Eviction removes a file from the current snapshot; the bytes stay on disk,
    referenced by the previous snapshot, until expiry deletes them. Conflating
    the two is how a disk-space calculation comes out a whole retention window
    short.
    """
    config = LogConfig(
        compact_min_files=99,
        local_retention=timedelta(microseconds=1),
        snapshot_retention=timedelta(days=365),  # nothing may expire
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 3)
        on_disk = {p.name for p in tmp_path.rglob("*.parquet")}
        assert len(on_disk) == 3

        log.maintain()

        assert log._data_files() == [], "evicted from the table"
        assert {p.name for p in tmp_path.rglob("*.parquet")} == on_disk, (
            "still on disk, held by the pre-eviction snapshot"
        )

    with open_log(
        tmp_path,
        LogConfig(compact_min_files=99, snapshot_retention=timedelta(microseconds=1)),
    ) as log:
        log.maintain()

        assert list(tmp_path.rglob("*.parquet")) == [], "expiry is what deletes bytes"


def test_sweep_spares_an_in_flight_seal(tmp_path: Path) -> None:
    """The one unreferenced file that must survive.

    A seal writes its Parquet before committing it, so between those two steps
    the file is on disk and no snapshot names it. `sealing` is what tells the
    sweep it is not an orphan — without that guard, a maintain() interleaved
    with a seal deletes the file the very next step is about to register.
    """
    config = LogConfig(
        compact_min_files=99, snapshot_retention=timedelta(microseconds=1)
    )
    with open_log(tmp_path, config) as log:
        log.extend(rows(3))
        rel_path = log._seal_path(1, 4)
        log._buffer.claim_seal(1, 4, rel_path)

        written = tmp_path / rel_path
        written.parent.mkdir(parents=True, exist_ok=True)
        written.write_bytes(b"not really parquet, but on disk and uncommitted")

        log.maintain()

        assert written.exists(), "sweep deleted a file `sealing` had claimed"


def test_sweep_removes_a_crashed_compaction_orphan(tmp_path: Path) -> None:
    """§11: no snapshot was committed, so the file is unreferenced and swept."""
    config = LogConfig(
        compact_min_files=99, snapshot_retention=timedelta(microseconds=1)
    )
    with open_log(tmp_path, config) as log:
        seal_files(log, 1)
        orphan = tmp_path / "s" / "data" / "orphan.parquet"
        orphan.write_bytes(b"leftover from a compaction that never committed")

        log.maintain()

        assert not orphan.exists()
        assert len(read_all(log)) == 4, "the live file is untouched"
