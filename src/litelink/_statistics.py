"""Per-column statistics for a log, from its manifests (#85).

Rolled up from the `lower_bounds`, `upper_bounds` and count maps Iceberg keeps
per data file, so no data file is opened — the same walk `LogTable` makes for
`litelink_offset`, over every column. A consumer prunes on the result, so the
rule throughout is that missing information comes out as None, never as a
narrower bound or a smaller count.

**Computed when asked, from what Iceberg already stores.** pyiceberg writes
these metrics into the manifests at every commit, so nothing new is written
and nothing has to be kept in step with seals, compaction, eviction or publish —
a stored rollup would be a second home for a fact the manifests already hold.

The one exception is deliberate and lives elsewhere: `buffer.db` keeps each
PUBLISHED file's bounds (`_prune`), because deciding whether a query needs the
published table must not cost the network round trip it exists to avoid. This module
still reads the manifests, so it answers from what the published table says rather
than from that copy.

What pyiceberg records for litelink's files, measured, decides most of it:

- **No NaN counts in the manifests.** Files are registered with `add_files`,
  whose metrics come from Parquet footers, and Parquet keeps none. It does not
  matter: every write path refuses NaN and ±inf (#87), so a float column's
  `nan_count` is 0 by construction and its bounds are over every value.
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

Tier = Literal["staging", "published", "buffer"]

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
    nested).

    The counts are sums, and None when any file lacks the count: a column added
    after a file was written has none in that file. As Iceberg defines it,
    `value_count` includes NULLs. `nan_count` is 0 for a float — no write path
    admits NaN (#87) — and None for anything else.
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

    `tier` is what was asked for — `"staging"`, `"published"`, or None for the
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
        # 0 by construction, not read from the manifests, which never hold a
        # NaN count: every write path refuses NaN (#87).
        self.nans: int | None = 0 if self.floating else None

    def add(self, data_file: DataFile) -> None:
        field_id = self.field_id
        nulls = (data_file.null_value_counts or {}).get(field_id)
        values = (data_file.value_counts or {}).get(field_id)
        self.nulls = None if self.nulls is None or nulls is None else self.nulls + nulls
        self.values = (
            None if self.values is None or values is None else self.values + values
        )
        if self.unknown or data_file.record_count == 0:
            return

        low = (data_file.lower_bounds or {}).get(field_id)
        high = (data_file.upper_bounds or {}).get(field_id)
        if low is None or high is None:
            # Nothing to fold only if the counts PROVE there is nothing: every
            # row NULL. A column added after this file was written has no
            # counts here at all, which leaves the bounds unknown.
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


def staging_span(schema: Schema, files: Sequence[DataFile]) -> tuple[int, int] | None:
    """The offset range the local files cover, or None when there are none."""
    covered = [_offsets(schema, data_file) for data_file in files]
    if not covered:
        return None

    return min(lo for lo, _ in covered), max(hi for _, hi in covered)


def below_staging(
    span: tuple[int, int] | None, schema: Schema, files: Sequence[DataFile]
) -> tuple[list[DataFile], bool]:
    """The published files holding rows outside the local range, and whether any
    of them straddles it.

    What eviction moved out of the staging table, and so what a read's published
    leg covers. A file that straddles the range — only `rewrite_published` can
    cut one — is included whole: its bounds overstate, but its rows cannot be
    split from the local copies without opening it.
    """
    beyond = []
    straddles = False
    for data_file in files:
        lo, hi = _offsets(schema, data_file)
        if span is None or hi < span[0] or lo > span[1]:
            beyond.append(data_file)
        elif lo < span[0] or hi > span[1]:
            beyond.append(data_file)
            straddles = True

    return beyond, straddles


def above(buffered: pa.Table, ceiling: int) -> pa.Table:
    """The buffered rows above every file: the ones no tier's file holds yet."""
    offsets = buffered.column(OFFSET).to_pylist()

    return buffered.slice(sum(1 for offset in offsets if offset <= ceiling))


def uncounted(statistics: TierStatistics) -> TierStatistics:
    """`statistics` with every count unknown, bounds kept.

    For a part that includes a straddling file, whose rows are partly local
    too: a bound over rows held twice is still a bound, but no count can be
    taken without double counting.
    """
    return TierStatistics(
        tier=statistics.tier,
        record_count=None,
        file_count=statistics.file_count,
        columns={
            name: ColumnStatistics(column.min, column.max, None, None, None)
            for name, column in statistics.columns.items()
        },
    )


def whole_log(
    names: Sequence[str],
    local: tuple[Schema, list[DataFile]],
    published: tuple[Schema, list[DataFile]] | None,
    buffered: pa.Table,
) -> TierStatistics:
    """The entire log: local files, the published table beyond them, and the buffer.

    The tiers overlap by design (I3) — the published table keeps what local still
    holds, and with `wal_replication` a seal keeps its rows in the buffer — so
    this takes each row from one place, as a read does, and the three parts
    are exactly what `column_statistics` reports for `"staging"`, `"published"`
    and `"buffer"`:

    - every **local** file;
    - every **published table** file outside the local offset range, which is what
      eviction dropped locally;
    - every **buffered** row above all of those files, counted from the rows
      themselves, exactly.

    One case cannot be split: a published file straddling the local range, which
    only `rewrite_published` re-cutting the published table can produce. Its rows are
    partly local too, so its bounds still hold — a bound over rows the log
    holds twice is a bound over rows it holds — but no count can be taken
    without double counting, and every count, `record_count` included, comes
    out None rather than wrong.
    """
    local_schema, local_files = local
    parts = [rollup(None, local_schema, local_files)]
    span = staging_span(local_schema, local_files)
    ceiling = 0 if span is None else span[1]

    straddles = False
    if published is not None:
        published_schema, published_files = published
        beyond, straddles = below_staging(span, published_schema, published_files)
        parts.append(rollup(None, published_schema, beyond))
        ceiling = max([ceiling, *(_offsets(published_schema, f)[1] for f in beyond)])

    # Above every file, rather than above the staging table alone: with the
    # staging table evicted dry the published table is the boundary, and a seal that
    # keeps its rows leaves them here too.
    parts.append(_from_rows(above(buffered, ceiling)))

    merged = _merge(names, parts)

    return uncounted(merged) if straddles else merged


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
            # No write path admits NaN (#87).
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
