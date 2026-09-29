"""Per-column statistics for a log, from its manifests (#85).

Rolled up from the `lower_bounds`, `upper_bounds` and count maps Iceberg keeps
per data file, so no data file is opened — the same walk `LogTable` makes for
`litelink_offset`, over every column. A consumer prunes on the result, so the
rule throughout is that missing information comes out as None, never as a
narrower bound or a smaller count.

**Computed when asked, from what Iceberg already stores.** pyiceberg writes
these metrics into the manifests at every commit, so nothing new is written
and nothing has to be kept in step with seals, compaction, eviction or sync —
a stored rollup would be a second home for a fact the manifests already hold.

What pyiceberg records for litelink's files, measured, decides most of it:

- **No NaN counts, ever.** Files are registered with `add_files`, whose
  metrics come from Parquet footers, and Parquet keeps no NaN count. NaN is
  still excluded from the bounds — a file holding `[NaN, 1.0]` reports
  `1.0..1.0` — so `nan_count` is None and a float's bounds say nothing about
  NaN. Only `ingest` can put one in a top-level float; `append` refuses it.
- **No usable bounds for strings or bytes.** They are truncated to 16, the
  string upper bound incremented past any real value (`zzz…{`), and a binary
  value of 0xff bytes loses its upper bound altogether. Bounds are reported for
  numeric and bool columns only.
- **No metrics for a nested column itself**, only for its leaves, and never
  bounds. Its statistics are None.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, cast

import pyarrow as pa
from pyiceberg.conversions import from_bytes
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    PrimitiveType,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from pyiceberg.manifest import DataFile
    from pyiceberg.schema import Schema
    from pyiceberg.types import IcebergType

Tier = Literal["local", "archive"]

OFFSET = "litelink_offset"

# Types whose Iceberg bounds are the values themselves, not a truncated prefix.
_BOUNDED = (IntegerType, LongType, FloatType, DoubleType, BooleanType)
_FLOATING = (FloatType, DoubleType)


@dataclass(frozen=True, slots=True)
class ColumnStatistics:
    """One column across everything the statistics cover.

    `min`/`max` are the column's own Python type, and None when any file with
    rows in it has no bound — unless its null count proves the column is all
    NULL there — or when the type keeps no exact bounds (strings, bytes,
    nested). They exclude NaN, which Iceberg's bounds never include.

    The counts are sums, and None when any file lacks the count: a column added
    after a file was written has none in that file. As Iceberg defines it,
    `value_count` includes NULLs. `nan_count` is None for anything but a float,
    and for floats in a data file, since nothing records it.
    """

    # `Any` rather than `object`: the type is the column's, so a caller must be
    # able to compare it without a cast — pruning is what this is for.
    min: Any
    max: Any
    null_count: int | None
    value_count: int | None
    nan_count: int | None


@dataclass(frozen=True)
class TierStatistics(Mapping[str, ColumnStatistics]):
    """Every column, by name, `litelink_offset` included.

    `tier` is what was asked for — `"local"`, `"archive"`, or None for the
    whole log. `record_count` is None only when a whole-log rollup could not
    count without double counting; see `whole_log`.
    """

    tier: Tier | None
    record_count: int | None
    file_count: int
    columns: Mapping[str, ColumnStatistics]

    def __getitem__(self, name: str) -> ColumnStatistics:
        return self.columns[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self.columns)

    def __len__(self) -> int:
        return len(self.columns)


class _Column:
    """The fold for one column, as files arrive."""

    def __init__(self, field_id: int, field_type: IcebergType) -> None:
        self.field_id = field_id
        self.field_type = field_type
        self.bounded = isinstance(field_type, _BOUNDED)
        self.floating = isinstance(field_type, _FLOATING)
        # `Any`: decoded bounds of one primitive type, comparable with each
        # other, which is all the fold asks of them.
        self.low: Any = None
        self.high: Any = None
        # Bounds, once any file with rows in this column has none, are unknown
        # rather than whatever the other files say.
        self.unknown = not self.bounded
        self.nulls: int | None = 0
        self.values: int | None = 0
        self.nans: int | None = 0 if self.floating else None

    def add(self, data_file: DataFile) -> None:
        field_id = self.field_id
        nulls = (data_file.null_value_counts or {}).get(field_id)
        values = (data_file.value_counts or {}).get(field_id)
        self.nulls = None if self.nulls is None or nulls is None else self.nulls + nulls
        self.values = (
            None if self.values is None or values is None else self.values + values
        )
        if self.floating:
            nans = (data_file.nan_value_counts or {}).get(field_id)
            self.nans = None if self.nans is None or nans is None else self.nans + nans

        if self.unknown or data_file.record_count == 0:
            return

        low = (data_file.lower_bounds or {}).get(field_id)
        high = (data_file.upper_bounds or {}).get(field_id)
        if low is None or high is None:
            # Nothing to fold only if the counts PROVE there is nothing: every
            # row NULL. A float whose only values are NaN has no bound and is
            # not all NULL, and a column added after this file was written has
            # no counts here at all — both leave the bounds unknown.
            if nulls is None or nulls != data_file.record_count:
                self.unknown = True

            return

        # A bounded type is primitive; `_BOUNDED` holds nothing else.
        field_type = cast("PrimitiveType", self.field_type)
        low_value: Any = from_bytes(field_type, low)
        high_value: Any = from_bytes(field_type, high)
        self.low = low_value if self.low is None else min(self.low, low_value)
        self.high = high_value if self.high is None else max(self.high, high_value)

    def result(self) -> ColumnStatistics:
        unknown = self.unknown
        return ColumnStatistics(
            min=None if unknown else self.low,
            max=None if unknown else self.high,
            null_count=self.nulls,
            value_count=self.values,
            nan_count=self.nans,
        )


_NOTHING = ColumnStatistics(None, None, None, None, None)


def rollup(
    tier: Tier | None, schema: Schema, data_files: Iterable[DataFile]
) -> TierStatistics:
    """Fold data files into per-column statistics."""
    columns = {
        field.name: _Column(field.field_id, field.field_type) for field in schema.fields
    }
    # A nested column keeps no metrics of its own, only its leaves do.
    nested = {
        name
        for name, column in columns.items()
        if not isinstance(column.field_type, PrimitiveType)
    }
    record_count = 0
    file_count = 0
    for data_file in data_files:
        record_count += data_file.record_count
        file_count += 1
        for name, column in columns.items():
            if name not in nested:
                column.add(data_file)

    return TierStatistics(
        tier=tier,
        record_count=record_count,
        file_count=file_count,
        columns={
            name: _NOTHING if name in nested else column.result()
            for name, column in columns.items()
        },
    )


def _offsets(schema: Schema, data_file: DataFile) -> tuple[int, int]:
    """The offset range one file covers. Every litelink file has these bounds."""
    field = schema.find_field(OFFSET)
    return (
        from_bytes(field.field_type, data_file.lower_bounds[field.field_id]),
        from_bytes(field.field_type, data_file.upper_bounds[field.field_id]),
    )


def whole_log(
    names: Sequence[str],
    local: tuple[Schema, list[DataFile]],
    archive: tuple[Schema, list[DataFile]] | None,
    buffered: pa.Table,
) -> TierStatistics:
    """The entire log: local files, the archive beyond them, and the buffer.

    The tiers overlap by design (I3) — the archive keeps what local still
    holds, and with `wal_replication` a seal keeps its rows in the buffer — so
    this takes each row from one place, as a read does:

    - every **local** file;
    - every **archive** file outside the local offset range, which is what
      eviction dropped locally;
    - every **buffered** row above all of those files, counted from the rows
      themselves, exactly.

    One case cannot be split: an archive file straddling the local range, which
    only `rewrite_archive` re-cutting the archive can produce. Its rows are
    partly local too, so its bounds still hold — a bound over rows the log
    holds twice is a bound over rows it holds — but no count can be taken
    without double counting, and every count, `record_count` included, comes
    out None rather than wrong.
    """
    local_schema, local_files = local
    parts = [rollup(None, local_schema, local_files)]
    covered = [_offsets(local_schema, data_file) for data_file in local_files]
    span = (
        (min(lo for lo, _ in covered), max(hi for _, hi in covered))
        if covered
        else None
    )

    straddles = False
    if archive is not None:
        archive_schema, archive_files = archive
        beyond = []
        for data_file in archive_files:
            lo, hi = _offsets(archive_schema, data_file)
            if span is None or hi < span[0] or lo > span[1]:
                beyond.append(data_file)
                covered.append((lo, hi))
            elif lo < span[0] or hi > span[1]:
                beyond.append(data_file)
                covered.append((lo, hi))
                straddles = True

        parts.append(rollup(None, archive_schema, beyond))

    # Above every file, rather than above the local table alone: with the
    # local table evicted dry the archive is the boundary, and a seal that
    # keeps its rows leaves them here too.
    ceiling = max((hi for _, hi in covered), default=0)
    offsets = buffered.column(OFFSET).to_pylist()
    tail = buffered.slice(sum(1 for offset in offsets if offset <= ceiling))
    parts.append(_from_rows(tail))

    merged = _merge(names, parts)
    if not straddles:
        return merged

    return TierStatistics(
        tier=None,
        record_count=None,
        file_count=merged.file_count,
        columns={
            name: ColumnStatistics(column.min, column.max, None, None, None)
            for name, column in merged.columns.items()
        },
    )


def _from_rows(rows: pa.Table) -> TierStatistics:
    """Exact statistics for buffered rows, which no file holds yet.

    Bounds only for the types a file would carry them for, so a column's
    statistics mean the same whichever tier its rows are in.
    """
    columns: dict[str, ColumnStatistics] = {}
    bounded = [
        field.name
        for field in rows.schema
        if pa.types.is_integer(field.type)
        or pa.types.is_floating(field.type)
        or pa.types.is_boolean(field.type)
    ]
    extremes = (
        rows.group_by([]).aggregate(
            [(name, how) for name in bounded for how in ("min", "max")]
        )
        if rows.num_rows and bounded
        else None
    )
    for field in rows.schema:
        name = field.name
        low = high = None
        if extremes is not None and name in bounded:
            low = extremes[f"{name}_min"][0].as_py()
            high = extremes[f"{name}_max"][0].as_py()

        columns[name] = ColumnStatistics(
            min=low,
            max=high,
            null_count=rows.column(name).null_count,
            value_count=rows.num_rows,
            # `append` refuses a top-level NaN — SQLite would store it as NULL —
            # so a buffered float holds none, exactly.
            nan_count=0 if pa.types.is_floating(field.type) else None,
        )

    return TierStatistics(
        tier=None, record_count=rows.num_rows, file_count=0, columns=columns
    )


def _merge(names: Sequence[str], parts: Sequence[TierStatistics]) -> TierStatistics:
    """Combine disjoint parts, by the same rule `_Column.add` folds files by."""
    counts = [part.record_count for part in parts]
    record_count = (
        None
        if any(count is None for count in counts)
        else sum(count for count in counts if count is not None)
    )
    columns: dict[str, ColumnStatistics] = {}
    for name in names:
        low: Any = None
        high: Any = None
        unknown = False
        nulls: int | None = 0
        values: int | None = 0
        nans: int | None = 0
        for part in parts:
            rows = part.record_count
            if rows == 0:
                continue

            column = part.columns.get(name)
            if column is None:
                # A part with rows that never had the column: nothing known.
                column = _NOTHING

            nulls = (
                None
                if nulls is None or column.null_count is None
                else nulls + column.null_count
            )
            values = (
                None
                if values is None or column.value_count is None
                else values + column.value_count
            )
            nans = (
                None
                if nans is None or column.nan_count is None
                else nans + column.nan_count
            )
            if column.min is None:
                if column.null_count is None or column.null_count != rows:
                    unknown = True

                continue

            low = column.min if low is None else min(low, column.min)
            high = column.max if high is None else max(high, column.max)

        columns[name] = ColumnStatistics(
            min=None if unknown else low,
            max=None if unknown else high,
            null_count=nulls,
            value_count=values,
            nan_count=nans,
        )

    return TierStatistics(
        tier=None,
        record_count=record_count,
        file_count=sum(part.file_count for part in parts),
        columns=columns,
    )
