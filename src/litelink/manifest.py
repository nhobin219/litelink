"""Statistics manifests: one row of per-column bounds per unit, and pruning on them.

A manifest is a Parquet table with one row per unit a reader might skip — a
tier of one log in litelink (`staging`, `published`), a sealed log of a stream in
streamcast — and one struct column per prunable column:

    tier       start_offset  end_offset  record_count  price                     side
    staging    3001          4001        1000          {min, max, null_count, …} {min, max, …}
    published  1             3001        3000          {min, max, null_count, …} {min, max, …}

Named for Iceberg's own: an Iceberg manifest is per-file statistics for
skipping files, and this is the same thing one level up. **Wide**, one struct
per column, because a long `(unit, column, min, max)` layout cannot hold typed
bounds for columns of different types.

**Pruning is SOUND in one direction only.** Including a unit that holds no
match is a wasted scan; excluding one that holds a match is a wrong answer
with no symptom. So every rule here fails towards include, and the tests check
every exclusion against DuckDB over generated data — NULLs, NaN, infinity,
all-null columns and units missing a column.

**`litelink_offset` is not a statistic.** It is the log's own sequence, dense
and monotonic across every unit — a log's tiers, a stream's logs — so each
unit's `[start_offset, end_offset)` says exactly where it sits, and a term on
the offset column is judged against that range rather than a struct. That is
also what lets a unit with no statistics at all — the buffer, a stream's live
log — still be skipped by offset (streamcast#32).

This began as streamcast's per-log manifest (streamcast#27) and moved here so
both use one implementation: `key` names the unit column, `"tier"` for litelink
and `"log"` for a stream.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Final, NamedTuple

import pyarrow as pa

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from litelink._statistics import TierStatistics

PRUNABLE: Final = frozenset(
    {pa.int32(), pa.int64(), pa.float32(), pa.float64(), pa.bool_()}
)
"""The column types a bound may prune on. Anything else cannot decide.

Strings are out on purpose: Iceberg truncates their bounds (16 characters by
default, the upper one incremented), and the comparison would have to match
DuckDB's byte order exactly. Binary and nested columns have no useful order.
"""

OPERATORS: Final = frozenset({"==", "<", "<=", ">", ">=", "in"})
"""What a term may use. Anything else cannot decide, and so includes."""

OFFSET: Final = "litelink_offset"
"""The column pruned by each unit's `[start_offset, end_offset)`, never a struct."""

Term = tuple[str, str, object]
"""`(column, operator, value)` — one conjunct of the predicate being pruned for.

`value` is a list for `in`. Terms are ANDed: a unit survives only if every term
might match it. A caller with an OR, or an operator not listed, passes fewer
terms — dropping a conjunct can only include more, never fewer.
"""


class Row(NamedTuple):
    """One unit's row: its name, offsets, declared columns and statistics.

    `end_offset` is exclusive, as it is on every extent in litelink, and None
    for a unit still growing — the buffer, a stream's live log — which is then
    never skipped for a term above it. `schema`
    is what decides each struct column's type, so a column with no statistics
    in this unit still gets a typed NULL rather than none at all.
    """

    name: str
    start_offset: int
    end_offset: int | None
    schema: pa.Schema
    statistics: TierStatistics


def columns(schemas: Iterable[pa.Schema]) -> dict[str, pa.DataType]:
    """The prunable columns across `schemas`, with their types.

    Every name keeps one type across the units (a log's schema is fixed, and a
    stream fixes a column's type for its life), so a union keyed by name is
    well defined. First occurrence fixes the order. `litelink_offset` is left
    out: its range is each unit's `start_offset`/`end_offset`.
    """
    found: dict[str, pa.DataType] = {}
    for schema in schemas:
        for field in schema:
            if field.type in PRUNABLE and field.name != OFFSET:
                found.setdefault(field.name, field.type)

    return found


def _struct(kind: pa.DataType) -> pa.DataType:
    fields = [
        pa.field("min", kind),
        pa.field("max", kind),
        pa.field("null_count", pa.int64()),
        pa.field("value_count", pa.int64()),
    ]
    if pa.types.is_floating(kind):
        fields.append(pa.field("nan_count", pa.int64()))

    return pa.struct(fields)


def build(rows: Sequence[Row], *, key: str = "tier") -> pa.Table:
    """The manifest for `rows`: one row per unit, one struct per column.

    A column the unit does not have, or has no statistics for, is a NULL
    struct — which is "no statistics", and never prunes.
    """
    kinds = columns(row.schema for row in rows)
    schema = pa.schema(
        [
            pa.field(key, pa.string(), nullable=False),
            pa.field("start_offset", pa.int64(), nullable=False),
            pa.field("end_offset", pa.int64()),
            pa.field("record_count", pa.int64()),
            *(pa.field(name, _struct(kind)) for name, kind in kinds.items()),
        ]
    )

    table = []
    for row in rows:
        statistics = row.statistics
        record: dict[str, object] = {
            key: row.name,
            "start_offset": row.start_offset,
            "end_offset": row.end_offset,
            "record_count": statistics.record_count,
        }
        for name, kind in kinds.items():
            found = statistics.columns.get(name)
            if found is None:
                record[name] = None
                continue

            value = {
                "min": found.min,
                "max": found.max,
                "null_count": found.null_count,
                "value_count": found.value_count,
            }
            if pa.types.is_floating(kind):
                value["nan_count"] = found.nan_count

            record[name] = value

        table.append(record)

    return pa.Table.from_pylist(table, schema=schema)


def extend(previous: pa.Table | None, row: Row, *, key: str = "tier") -> pa.Table:
    """`previous` with `row`, replacing any row it already had for that unit.

    Replacing rather than appending, because a write that is retried after a
    crash would otherwise leave two rows for one unit saying two things.

    The new row's columns are unioned with the old ones; a column one side
    lacks is NULL there — no statistics, which never prunes.
    """
    added = build([row], key=key)
    if previous is None:
        return added

    return pa.concat_tables(
        [without(previous, row.name, key=key), added], promote_options="default"
    )


def without(manifest: pa.Table, name: str, *, key: str = "tier") -> pa.Table:
    """`manifest` with no row for `name` — which reads as "no statistics"."""
    return manifest.filter(
        pa.array([unit != name for unit in manifest[key].to_pylist()], pa.bool_())
    )


# -- pruning -------------------------------------------------------------------


def prune(
    manifest: pa.Table | None,
    names: Sequence[str],
    terms: Sequence[Term],
    *,
    key: str = "tier",
) -> list[str]:
    """The units in `names` that might hold a row matching every term.

    `names` is the READER's list, and it is the authority: a manifest row for
    any other unit is ignored, and a listed unit with no row is included. A
    manifest newer than whatever the reader resolved its units from can
    therefore add nothing. A unit whose `record_count` is 0 is excluded for
    any terms at all — including none.

    **Evaluated in Python rather than as a PyArrow expression**, and that is a
    soundness decision rather than a style one. A filter drops rows where the
    expression is NULL, and NULL here means "no statistics" — so the natural
    default of a vectorised filter would be to EXCLUDE a unit it knows nothing
    about. There are a handful of rows, so there is nothing to vectorise.
    """
    if manifest is None:
        return list(names)

    kinds = {
        field.name: field.type.field("min").type
        for field in manifest.schema
        if pa.types.is_struct(field.type)
    }
    rows = {row[key]: row for row in manifest.to_pylist()}

    return [
        name
        for name in names
        if (row := rows.get(name)) is None
        or (
            # A unit known to hold no rows holds no match, whatever the terms.
            # Unknown (None) is not zero.
            row.get("record_count") != 0
            and all(
                _offset_may_match(row, term)
                if term[0] == OFFSET
                else _may_match(row, term, kinds)
                for term in terms
            )
        )
    ]


def _may_match(
    row: Mapping[str, object], term: Term, kinds: Mapping[str, pa.DataType]
) -> bool:
    """Whether a unit with these statistics MIGHT hold a row matching `term`.

    True whenever it cannot decide. The comparisons are the rows DuckDB would
    return: a NULL matches no comparison, so bounds over the non-null values
    are enough — except for NaN, below.
    """
    column, operator, value = term
    kind = kinds.get(column)
    stats = row.get(column)
    if kind is None or operator not in OPERATORS or not isinstance(stats, dict):
        return True

    low, high = stats.get("min"), stats.get("max")
    if low is None or high is None:
        # An all-null column has no bounds, and a file that recorded none
        # makes the rollup's bound unknown. Neither says anything about rows.
        return True

    if pa.types.is_floating(kind):
        # **A defence, not a case.** litelink refuses NaN on every write path
        # (#87), so its statistics report 0 here and this never fires. It stays
        # because a NaN count that is non-zero or unknown — which is what
        # pyiceberg records by itself — really would make the bounds unsafe to
        # prune on: they exclude NaN, and DuckDB sorts NaN above every float.
        # Measured on 1.5.5, a file holding [10.0, NaN] returns the NaN for
        # `x > 5` and not for `x > 50`, through `iceberg_scan`, `read_parquet`
        # and litelink's `sql` alike.
        nans = stats.get("nan_count")
        if nans is None or nans > 0:
            return True

    values = value if operator == "in" else [value]
    if not isinstance(values, (list, tuple)):
        return True

    try:
        return any(_compare(operator, low, high, v) for v in values)
    except TypeError:
        # A value the column's type does not compare with — a string against
        # an integer column. Not this function's to reject: it cannot decide.
        return True


def _offset_may_match(row: Mapping[str, object], term: Term) -> bool:
    """Whether a unit's `[start_offset, end_offset)` can hold an offset `term` matches.

    Exact, since offsets are dense: the unit holds every offset in its range
    and none outside it. An empty range holds nothing. A None end is a unit
    still growing, bounded below only.
    """
    _, operator, value = term
    start, end = row.get("start_offset"), row.get("end_offset")
    if operator not in OPERATORS or not isinstance(start, int):
        return True

    if end is None:
        high: object = math.inf
    elif isinstance(end, int):
        if end <= start:
            return False

        high = end - 1
    else:
        return True

    values = value if operator == "in" else [value]
    if not isinstance(values, (list, tuple)):
        return True

    try:
        return any(_compare(operator, start, high, v) for v in values)
    except TypeError:
        return True


def _compare(operator: str, low: object, high: object, value: object) -> bool:
    """Whether some value in `[low, high]` satisfies `operator value`."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return True

    if operator in ("==", "in"):
        return low <= value <= high  # ty: ignore[unsupported-operator]

    if operator == "<":
        return low < value  # ty: ignore[unsupported-operator]

    if operator == "<=":
        return low <= value  # ty: ignore[unsupported-operator]

    if operator == ">":
        return high > value  # ty: ignore[unsupported-operator]

    return high >= value  # ty: ignore[unsupported-operator]


__all__ = [
    "OFFSET",
    "OPERATORS",
    "PRUNABLE",
    "Row",
    "Term",
    "build",
    "columns",
    "extend",
    "prune",
    "without",
]
