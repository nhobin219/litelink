"""The column types a log can carry, and the mappings they need.

One table rather than three. A column has to survive four hops — SQLite
storage, the Parquet write, Iceberg's type system, and the DuckDB cast on the
buffer leg of a read — and until this existed the SQLite affinities and the
DuckDB casts were separate maps that had to agree by hand. They did not: a
`uint32` column created a log fine, accepted an append, and then failed on the
first read with `KeyError: 'uint32'`, having already made the data durable.

The set is deliberately conservative. Iceberg narrows silently where it cannot
represent a type — `int8` and `int16` become `int32`, and `uint32`/`uint64`
become *signed* `int32`/`int64`, which loses the top half of the range — so
rather than pass those through, they are refused with the reason.

`binary` is absent for a different reason, and a temporary one: the read path
pushes its boundary predicate into SQLite, which is 14x faster and is what
keeps cleanup from costing query latency, and the mechanism that allows it
cannot carry blob bytes. §15 is where binary payloads belong anyway — they
bypass the buffer rather than travelling through it, which is precisely the
constraint met here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple

import pyarrow as pa

if TYPE_CHECKING:
    from collections.abc import Callable


class ColumnType(NamedTuple):
    """How one Arrow type is carried through each layer."""

    sqlite: str
    """SQLite column affinity. Advisory, but it keeps values round-tripping as
    the type the Iceberg schema will demand at seal."""

    duckdb: str
    """The DuckDB type the buffer leg is cast to. SQLite's per-value typing
    comes through the scanner loosely, so the union needs this explicit (§7)."""

    variable_length: bool
    """Whether a value's size depends on the value.

    The seal trigger works in bytes, so the buffer has to measure itself. Fixed
    types count as a constant; only these need asking SQLite. Recorded here
    rather than inferred from the affinity, so that "is this variable-length"
    is not a second opinion about types held somewhere else."""


# Matched in order, because Arrow's predicates overlap.
_SUPPORTED: tuple[tuple[Callable[[pa.DataType], bool], ColumnType], ...] = (
    (
        pa.types.is_boolean,
        ColumnType("INTEGER", "BOOLEAN", variable_length=False),
    ),
    (
        pa.types.is_int32,
        ColumnType("INTEGER", "INTEGER", variable_length=False),
    ),
    (
        pa.types.is_int64,
        ColumnType("INTEGER", "BIGINT", variable_length=False),
    ),
    (
        pa.types.is_float32,
        ColumnType("REAL", "FLOAT", variable_length=False),
    ),
    (
        pa.types.is_float64,
        ColumnType("REAL", "DOUBLE", variable_length=False),
    ),
    (
        pa.types.is_string,
        ColumnType("TEXT", "VARCHAR", variable_length=True),
    ),
    (
        pa.types.is_large_string,
        ColumnType("TEXT", "VARCHAR", variable_length=True),
    ),
)

# Types worth refusing with a reason rather than a lookup failure.
_REASONS: tuple[tuple[Callable[[pa.DataType], bool], str], ...] = (
    (
        pa.types.is_unsigned_integer,
        "Iceberg has no unsigned types, so this becomes signed and loses the "
        "top half of its range. Declare int64.",
    ),
    (
        lambda t: pa.types.is_int8(t) or pa.types.is_int16(t),
        "Iceberg widens this to int32 without saying so. Declare int32.",
    ),
    (
        lambda t: pa.types.is_binary(t) or pa.types.is_large_binary(t),
        "not supported yet. The buffer leg of a read pushes its boundary into "
        "SQLite through `sqlite_query`, which cannot carry blob bytes — it "
        "decodes them as UTF-8 and fails, with or without a CAST. Encode as "
        "text for now; SPEC §15 is where binary payloads are designed to live, "
        "and they bypass the buffer entirely there.",
    ),
    (
        pa.types.is_temporal,
        "not yet supported: the buffer stores these as integers and the "
        "round trip through SQLite is untested. Store epoch integers.",
    ),
    (
        lambda t: pa.types.is_nested(t) or pa.types.is_decimal(t),
        "not yet supported: SQLite has no column affinity for it.",
    ),
)


def column_type(type_: pa.DataType) -> ColumnType:
    """How to carry `type_`, or a TypeError explaining why it cannot be."""
    for predicate, mapping in _SUPPORTED:
        if predicate(type_):
            return mapping

    for predicate, reason in _REASONS:
        if predicate(type_):
            msg = f"unsupported column type {type_}: {reason}"
            raise TypeError(msg)

    msg = f"unsupported column type {type_}"
    raise TypeError(msg)


def validate_schema(schema: pa.Schema) -> None:
    """Refuse a schema at creation rather than at the first read.

    This is the whole reason the table exists: the alternative is a log that
    accepts writes and then cannot serve them.
    """
    for field in schema:
        try:
            column_type(field.type)
        except TypeError as exc:
            msg = f"column {field.name!r}: {exc}"
            raise TypeError(msg) from None
