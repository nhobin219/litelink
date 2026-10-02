"""`column_statistics`: a tier's per-column rollup from its manifests (#85)."""

from __future__ import annotations

import random
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pytest
from pyiceberg.conversions import to_bytes
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField

import litelink
from litelink import LogConfig
from litelink._statistics import rollup, whole_log

if TYPE_CHECKING:
    from pathlib import Path

SCHEMA = pa.schema(
    [
        pa.field("k", pa.int64(), nullable=False),
        pa.field("i32", pa.int32()),
        pa.field("f32", pa.float32()),
        pa.field("f64", pa.float64()),
        pa.field("b", pa.bool_()),
        pa.field("s", pa.string()),
        pa.field("allnull", pa.int64()),
    ]
)
BOUNDED = ("litelink_offset", "k", "i32", "f32", "f64", "b")


def _value(rng: random.Random, type_: pa.DataType) -> object:
    if rng.random() < 0.3:
        return None

    if pa.types.is_int32(type_):
        return rng.randint(-(2**31), 2**31 - 1)

    if pa.types.is_floating(type_):
        return rng.choice([rng.uniform(-1e6, 1e6), 0.0, -0.0])

    if pa.types.is_boolean(type_):
        return rng.random() < 0.5

    return f"s{rng.randint(0, 99)}"


def _random_log(root: Path, seed: int) -> litelink.WriteHandle:
    """Several sealed files, then a bulk load.

    The rules a log can reach are reached: a file where a column is all NULL
    (the whole batch leaves `i32` empty), and a file written by `ingest`. A
    file that predates a column — possible only in a log an older release
    widened — is covered at the file level below.
    """
    rng = random.Random(seed)
    config = LogConfig(target_seal_rows=rng.randint(3, 12))
    log = litelink.new(root, "s", schema=SCHEMA, sort_by=("k",), config=config)
    key = 0
    for batch in range(rng.randint(3, 6)):
        rows = []
        for _ in range(rng.randint(1, 20)):
            key += 1
            row: dict[str, object] = {"k": key}
            for field in SCHEMA:
                if field.name not in ("k", "allnull"):
                    row[field.name] = _value(rng, field.type)

            if batch == 1:
                row["i32"] = None

            rows.append(row)

        log.extend(rows)
        log.seal(flush=True)

    loaded = [
        {"k": key + 1, "f64": rng.uniform(-1e6, 1e6), "f32": None},
        {"k": key + 2, "f64": rng.uniform(-1e6, 1e6), "f32": 2.5},
    ]
    log.ingest(pa.Table.from_pylist(loaded, schema=SCHEMA))

    return log


@pytest.mark.parametrize("seed", range(6))
def test_the_rollup_agrees_with_the_data(tmp_path: Path, seed: int) -> None:
    """Equal to what reading the data gives, wherever it states a value.

    A stated bound or count is exact. None is the only permitted answer other
    than the truth — and a column present in every file, with a bound in each,
    must not get it.
    """
    with _random_log(tmp_path, seed) as log:
        assert log.buffered_rows() == 0
        data = log.scan().read_all()
        stats = log.column_statistics(tier="staging")

        assert stats.tier == "staging"
        assert stats.record_count == data.num_rows
        assert stats.file_count == log.staging_files()
        assert set(stats) == {"litelink_offset", *SCHEMA.names}

        for name in stats:
            values = data[name].to_pylist()
            column = stats[name]
            if column.null_count is not None:
                assert column.null_count == values.count(None), name

            if column.value_count is not None:
                assert column.value_count == len(values), name

            present = [v for v in values if v is not None]
            if column.min is not None:
                assert name in BOUNDED
                assert (column.min, column.max) == (min(present), max(present)), name

        # Not vacuous: None where it is owed, and only there.
        offsets = data["litelink_offset"].to_pylist()
        assert (stats["litelink_offset"].min, stats["litelink_offset"].max) == (
            min(offsets),
            max(offsets),
        )
        assert stats["k"].min == 1
        assert stats["i32"].min is not None, "an all-NULL file is proven, not unknown"
        assert stats["allnull"].null_count == data.num_rows
        assert stats["allnull"].min is None
        assert stats["s"].min is None, "string bounds are truncated, so none"
        assert stats["f64"].nan_count == 0, "no write path admits NaN (#87)"
        assert stats["f32"].nan_count == 0
        assert stats["i32"].nan_count is None, "only floats have one"

        # The whole log: the same files plus rows no seal has written yet.
        rng = random.Random(seed)
        tail_key = max(data["k"].to_pylist())
        log.extend(
            [
                {"k": tail_key + n, "i32": rng.randint(-5, 5)}
                for n in range(1, rng.randint(2, 9))
            ]
        )
        whole = log.column_statistics()
        assert whole.tier is None
        _assert_agrees(whole, log.scan().read_all())


def _assert_agrees(stats: litelink.TierStatistics, data: pa.Table) -> None:
    """Every stated figure equals the data's, and None is the only other answer."""
    assert stats.record_count == data.num_rows
    for name in stats:
        values = data[name].to_pylist()
        column = stats[name]
        if column.null_count is not None:
            assert column.null_count == values.count(None), name

        if column.value_count is not None:
            assert column.value_count == len(values), name

        present = [v for v in values if v is not None]
        if column.min is not None:
            assert (column.min, column.max) == (min(present), max(present)), name

    offsets = data["litelink_offset"].to_pylist()
    assert (stats["litelink_offset"].min, stats["litelink_offset"].max) == (
        min(offsets),
        max(offsets),
    )


def _file(record_count: int, **metrics: Any) -> DataFile:
    return DataFile.from_args(
        content=DataFileContent.DATA,
        file_path="x.parquet",
        file_format=FileFormat.PARQUET,
        record_count=record_count,
        file_size_in_bytes=1,
        **metrics,
    )


_LONG = Schema(NestedField(1, "n", LongType(), required=False))


def _bounded(record_count: int, lo: int, hi: int, nulls: int = 0) -> DataFile:
    return _file(
        record_count,
        value_counts={1: record_count},
        null_value_counts={1: nulls},
        lower_bounds={1: to_bytes(LongType(), lo)},
        upper_bounds={1: to_bytes(LongType(), hi)},
    )


def test_a_file_with_values_but_no_bound_makes_the_bounds_unknown() -> None:
    """The rule a data comparison cannot see, so asked of `rollup` directly.

    Two rows, one NULL, no bound: the other row's value is not covered by the
    other file's bounds, so reporting them would be narrower than the truth.

    Falsify by skipping the `self.unknown = True` in `_Column.add`.
    """
    unbounded = _file(2, value_counts={1: 2}, null_value_counts={1: 1})
    stats = rollup("staging", _LONG, [_bounded(3, 5, 9), unbounded])

    assert (stats["n"].min, stats["n"].max) == (None, None)
    assert stats["n"].null_count == 1


def test_an_all_null_file_adds_nothing_to_the_bounds() -> None:
    """Proven by its null count, so the other files' bounds stand.

    Falsify by treating every file without a bound as unknown.
    """
    all_null = _file(4, value_counts={1: 4}, null_value_counts={1: 4})
    stats = rollup("staging", _LONG, [_bounded(3, 5, 9), all_null, _bounded(2, -1, 6)])

    assert (stats["n"].min, stats["n"].max) == (-1, 9)
    assert stats["n"].null_count == 4
    assert stats.record_count == 9
    assert stats.file_count == 3


def test_a_file_missing_a_count_makes_that_sum_unknown() -> None:
    """A column added after a file was written has no counts there at all."""
    predates = _file(5)
    stats = rollup("staging", _LONG, [_bounded(3, 5, 9), predates])

    assert stats["n"].null_count is None
    assert stats["n"].value_count is None
    assert stats["n"].min is None


def test_an_empty_tier_counts_zero() -> None:
    stats = rollup("published", _LONG, [])

    assert (stats.record_count, stats.file_count) == (0, 0)
    assert stats["n"].null_count == 0
    assert stats["n"].min is None


def test_the_tier_must_be_named_and_the_published_table_must_exist(
    tmp_path: Path,
) -> None:
    with litelink.new(tmp_path, "s", schema=SCHEMA) as log:
        with pytest.raises(ValueError, match="no published table"):
            log.column_statistics(tier="published")

        with pytest.raises(
            ValueError, match="'staging', 'published', 'buffer' or None"
        ):
            log.column_statistics(tier="nearline")  # ty: ignore[invalid-argument-type]


def test_buffered_rows_are_their_own_tier(tmp_path: Path) -> None:
    """Buffered rows are in `"buffer"`, not in `"staging"`, until a seal writes
    them — and the three tiers add up to the whole log. The case where a seal
    KEEPS its rows in the buffer needs a published table; see
    `test_the_statistics_tiers_partition_the_log`.
    """
    with litelink.new(tmp_path, "s", schema=SCHEMA) as log:
        log.extend({"k": i} for i in range(10))
        log.seal(flush=True)
        log.extend({"k": i} for i in range(10, 13))

        local = log.column_statistics(tier="staging")
        buffered = log.column_statistics(tier="buffer")
        whole = log.column_statistics()

        assert (local.record_count, local["k"].max) == (10, 9)
        assert buffered.tier == "buffer"
        assert (buffered.record_count, buffered["k"].min) == (3, 10)
        assert local.record_count is not None
        assert buffered.record_count is not None
        assert whole.record_count == local.record_count + buffered.record_count


_WITH_OFFSET = Schema(
    NestedField(1, "litelink_offset", LongType(), required=True),
    NestedField(2, "n", LongType(), required=False),
)


def _span(lo: int, hi: int, n_lo: int, n_hi: int) -> DataFile:
    rows = hi - lo + 1
    return _file(
        rows,
        value_counts={1: rows, 2: rows},
        null_value_counts={1: 0, 2: 0},
        lower_bounds={1: to_bytes(LongType(), lo), 2: to_bytes(LongType(), n_lo)},
        upper_bounds={1: to_bytes(LongType(), hi), 2: to_bytes(LongType(), n_hi)},
    )


_EMPTY = pa.table(
    {"litelink_offset": pa.array([], pa.int64()), "n": pa.array([], pa.int64())}
)
_NAMES = ("litelink_offset", "n")


def test_the_whole_log_takes_each_file_once() -> None:
    """A published file local still holds is not counted twice.

    Falsify by adding every published file: the record count doubles for the
    overlapping range.
    """
    local = [_span(10, 19, 100, 200)]
    published = [_span(1, 9, 50, 60), _span(10, 19, 100, 200)]
    stats = whole_log(_NAMES, (_WITH_OFFSET, local), (_WITH_OFFSET, published), _EMPTY)

    assert stats.record_count == 19
    assert stats.file_count == 2
    assert (stats["n"].min, stats["n"].max) == (50, 200)
    assert stats["n"].null_count == 0


def test_a_straddling_published_file_keeps_its_bounds_and_loses_the_counts() -> None:
    """Rows on both sides of the local boundary cannot be counted once.

    Only `compact("published")` re-cutting the published table makes one. Its bounds are
    over rows the log holds, so they stand; every count is None, never doubled.
    """
    local = [_span(10, 19, 100, 200)]
    published = [_span(1, 14, 1, 5)]
    stats = whole_log(_NAMES, (_WITH_OFFSET, local), (_WITH_OFFSET, published), _EMPTY)

    assert stats.record_count is None
    assert stats["n"].null_count is None
    assert (stats["n"].min, stats["n"].max) == (1, 200)
    assert (stats["litelink_offset"].min, stats["litelink_offset"].max) == (1, 19)


def test_buffered_rows_a_file_already_holds_are_not_counted_again() -> None:
    """With `wal_replication` a seal keeps its rows in the buffer.

    Falsify by counting the whole buffer: 5 becomes 8.
    """
    local = [_span(1, 3, 7, 7)]
    buffered = pa.table(
        {
            "litelink_offset": pa.array([1, 2, 3, 4, 5], pa.int64()),
            "n": pa.array([7, 7, 7, None, 9], pa.int64()),
        }
    )
    stats = whole_log(_NAMES, (_WITH_OFFSET, local), None, buffered)

    assert stats.record_count == 5
    assert stats["n"].null_count == 1
    assert (stats["n"].min, stats["n"].max) == (7, 9)
