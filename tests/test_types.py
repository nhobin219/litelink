"""Which column types a log will carry, and how it refuses the rest.

The failure this prevents: a `uint32` column used to create a log fine, accept
an append, and then fail on the first read with `KeyError: 'uint32'` — data
durable and unreadable, because the SQLite affinity map and the DuckDB cast map
were separate and disagreed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

import litelink
from litelink._types import column_type, validate_schema

if TYPE_CHECKING:
    from pathlib import Path

CARRIED = [
    pa.int32(),
    pa.int64(),
    pa.float32(),
    pa.float64(),
    pa.bool_(),
    pa.string(),
    pa.large_string(),
]


@pytest.mark.parametrize("type_", CARRIED, ids=str)
def test_carried_types_map_through_every_layer(type_: pa.DataType) -> None:
    mapping = column_type(type_)

    assert mapping.sqlite in {"INTEGER", "REAL", "TEXT", "BLOB"}
    assert mapping.duckdb


# A real value per type. Passing None for every column looks like coverage and
# is not: a null never exercises the conversion from SQLite's storage class to
# the Arrow type the seal writes, which is where booleans were broken.
SAMPLE: dict[str, object] = {
    "int32": 7,
    "int64": 7,
    "float": 1.5,
    "double": 1.5,
    "bool": True,
    "string": "x",
    "large_string": "unicode ☃, and a quote'''s worth of trouble",
}


@pytest.mark.parametrize("type_", CARRIED, ids=str)
def test_carried_types_survive_a_round_trip(tmp_path: Path, type_: pa.DataType) -> None:
    """Create, append, seal, read — with a real value AND a null."""
    schema = pa.schema([pa.field("event_ts", pa.int64()), pa.field("c", type_)])
    root = tmp_path / str(type_).replace("[", "_").replace("]", "")
    sample = SAMPLE[str(type_)]

    with litelink.new(root, "s", schema=schema, sort_by=("event_ts",)) as log:
        log.extend([{"event_ts": 1, "c": sample}, {"event_ts": 2, "c": None}])
        assert log.scan().read_all()["c"].to_pylist() == [sample, None], "from buffer"

        log.seal()

        assert log.scan().read_all()["c"].to_pylist() == [sample, None], "from table"


@pytest.mark.parametrize(
    ("type_", "reason"),
    [
        (pa.uint32(), "unsigned"),
        (pa.uint64(), "unsigned"),
        (pa.int8(), "widens"),
        (pa.int16(), "widens"),
        (pa.binary(), "not supported yet"),
        (pa.large_binary(), "not supported yet"),
        (pa.timestamp("us"), "not yet supported"),
        (pa.decimal128(10, 2), "not yet supported"),
        (pa.list_(pa.int64()), "not yet supported"),
    ],
    ids=str,
)
def test_refused_types_say_why(type_: pa.DataType, reason: str) -> None:
    with pytest.raises(TypeError, match=reason):
        column_type(type_)


def test_a_log_refuses_an_uncarryable_column_at_creation(tmp_path: Path) -> None:
    """The whole point: fail here, not after the data is durable."""
    schema = pa.schema(
        [pa.field("event_ts", pa.int64()), pa.field("counter", pa.uint32())]
    )

    with pytest.raises(TypeError, match="'counter'.*unsigned"):
        litelink.new(tmp_path, "s", schema=schema, sort_by=("event_ts",))

    assert not (tmp_path / "s" / "buffer.db").exists(), "nothing left behind"


def test_validate_schema_names_the_offending_column() -> None:
    schema = pa.schema([pa.field("ok", pa.int64()), pa.field("bad", pa.uint64())])

    with pytest.raises(TypeError, match="column 'bad'"):
        validate_schema(schema)


def test_declared_types_come_back_as_declared(tmp_path: Path) -> None:
    """Arrow is the interchange type at every edge, and the declaration wins.

    Iceberg has one string type and one binary type, and DuckDB returns the
    32-bit-offset Arrow forms for both — so without casting at the edges, a
    column declared `large_binary` comes back `binary`. The declared schema is
    stored and cast to instead, which is why this holds for the wide forms and
    not only the narrow ones.
    """
    schema = pa.schema(
        [
            pa.field("event_ts", pa.int64()),
            pa.field("key", pa.large_string()),
            pa.field("payload", pa.string()),
        ]
    )

    with litelink.new(tmp_path, "s", schema=schema, sort_by=("event_ts",)) as log:
        log.append({"event_ts": 1, "key": "a", "payload": "p"})

        from_buffer = log.scan().read_all().schema
        assert from_buffer.field("key").type == pa.large_string()
        assert from_buffer.field("payload").type == pa.string()

        log.seal()

        from_table = log.scan().read_all().schema
        assert from_table.field("key").type == pa.large_string()
        assert from_table.field("payload").type == pa.string()

    with litelink.open(tmp_path, "s") as reopened:
        reopened_schema = reopened.scan().read_all().schema
        assert reopened_schema.field("key").type == pa.large_string()
        assert reopened_schema.field("payload").type == pa.string()


def test_a_log_with_no_stored_schema_refuses_to_open(tmp_path: Path) -> None:
    """No fallback to the table's own view.

    Under I16 a schema change records its intent before acting and is replayed
    on recovery, so a log missing its Arrow schema has not been interrupted —
    it is damaged. Guessing from the table would serve reads under a schema the
    data does not have.
    """
    schema = pa.schema([pa.field("event_ts", pa.int64()), pa.field("key", pa.string())])
    litelink.new(tmp_path, "s", schema=schema, sort_by=("event_ts",)).close()

    log = litelink.open(tmp_path, "s")
    log._buffer._con.execute("DELETE FROM meta WHERE k = 'arrow_schema'")
    log.close()

    with pytest.raises(ValueError, match="no stored Arrow schema"):
        litelink.open(tmp_path, "s")


def test_a_schema_disagreeing_with_the_table_refuses_to_open(tmp_path: Path) -> None:
    """The two records disagreeing means something wrote outside litelink."""
    schema = pa.schema([pa.field("event_ts", pa.int64()), pa.field("key", pa.string())])
    litelink.new(tmp_path, "s", schema=schema, sort_by=("event_ts",)).close()

    stale = pa.schema([pa.field("event_ts", pa.int64()), pa.field("gone", pa.string())])
    log = litelink.open(tmp_path, "s")
    log._buffer.set_meta("arrow_schema", stale.serialize().to_pybytes().hex())
    log.close()

    with pytest.raises(ValueError, match="disagrees with the Iceberg table"):
        litelink.open(tmp_path, "s")


# The values a fixture reaches for by default are the ones that cannot fail.
# `7` for an int64 exercises nothing about int64.
EXTREMES: list[tuple[str, pa.DataType, object]] = [
    ("int64 max", pa.int64(), 2**63 - 1),
    ("int64 min", pa.int64(), -(2**63)),
    ("int32 max", pa.int32(), 2**31 - 1),
    ("int32 min", pa.int32(), -(2**31)),
    ("float64 denormal", pa.float64(), 5e-324),
    ("float64 inf", pa.float64(), float("inf")),
    ("float64 -inf", pa.float64(), float("-inf")),
    ("float32 max", pa.float32(), 3.4028234663852886e38),
    ("empty string", pa.string(), ""),
    ("nul byte in string", pa.string(), "a\x00b"),
    ("quotes and newline", pa.string(), 'it\'s\n"quoted"\\'),
    ("astral plane", pa.string(), "🛩️ ☃ Ω"),
    ("100 KB string", pa.string(), "x" * 100_000),
    ("bool false", pa.bool_(), False),
]


@pytest.mark.parametrize(
    ("label", "type_", "value"), EXTREMES, ids=[c[0] for c in EXTREMES]
)
def test_extreme_values_survive_the_round_trip(
    tmp_path: Path, label: str, type_: pa.DataType, value: object
) -> None:
    """Each supported type at the edges of what it can hold.

    Every one of these crosses SQLite's storage classes, a Parquet write and
    Iceberg's type system, and any of those could quietly reshape a value —
    quoting through the SQL the read path builds, a nul byte through a TEXT
    column, an int64 at the limit of a REAL-affinity mistake.
    """
    schema = pa.schema([pa.field("event_ts", pa.int64()), pa.field("c", type_)])

    with litelink.new(tmp_path, "s", schema=schema, sort_by=("event_ts",)) as log:
        log.append({"event_ts": 1, "c": value})

        assert log.scan().read_all()["c"].to_pylist() == [value], "from the buffer"

        log.seal()

        assert log.scan().read_all()["c"].to_pylist() == [value], "from the table"


def test_nan_is_refused_rather_than_silently_nulled(tmp_path: Path) -> None:
    """SQLite has no NaN — it stores one as NULL.

    Verified directly: `INSERT` a NaN into a REAL column and `typeof` reports
    null. So a float column would accept a NaN and return a null, with nothing
    raised anywhere. Infinity is unaffected and round-trips, which is what makes
    the NaN case easy to miss.

    Refused for the same reason `_types` refuses whole types it cannot carry:
    changing a value silently is worse than declining it.
    """
    schema = pa.schema([pa.field("event_ts", pa.int64()), pa.field("c", pa.float64())])

    with litelink.new(tmp_path, "s", schema=schema, sort_by=("event_ts",)) as log:
        with pytest.raises(ValueError, match="NaN"):
            log.append({"event_ts": 1, "c": float("nan")})

        # Nothing was written, and the log still works.
        log.append({"event_ts": 1, "c": float("inf")})
        log.append({"event_ts": 2, "c": None})

        assert log.scan().read_all()["c"].to_pylist() == [float("inf"), None]
