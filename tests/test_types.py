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

from litelink import Log
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
    pa.binary(),
    pa.large_binary(),
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
    "large_string": "x",
    "binary": b"x",
    "large_binary": b"x",
}


@pytest.mark.parametrize("type_", CARRIED, ids=str)
def test_carried_types_survive_a_round_trip(tmp_path: Path, type_: pa.DataType) -> None:
    """Create, append, seal, read — with a real value AND a null."""
    schema = pa.schema([pa.field("event_ts", pa.int64()), pa.field("c", type_)])
    root = tmp_path / str(type_).replace("[", "_").replace("]", "")
    sample = SAMPLE[str(type_)]

    with Log.new(root, "s", schema=schema, sort_by=("event_ts",)) as log:
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
        Log.new(tmp_path, "s", schema=schema, sort_by=("event_ts",))

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
            pa.field("payload", pa.large_binary()),
        ]
    )

    with Log.new(tmp_path, "s", schema=schema, sort_by=("event_ts",)) as log:
        log.append({"event_ts": 1, "key": "a", "payload": b"p"})

        from_buffer = log.scan().read_all().schema
        assert from_buffer.field("key").type == pa.large_string()
        assert from_buffer.field("payload").type == pa.large_binary()

        log.seal()

        from_table = log.scan().read_all().schema
        assert from_table.field("key").type == pa.large_string()
        assert from_table.field("payload").type == pa.large_binary()

    with Log.open(tmp_path, "s") as reopened:
        reopened_schema = reopened.scan().read_all().schema
        assert reopened_schema.field("key").type == pa.large_string()
        assert reopened_schema.field("payload").type == pa.large_binary()


def test_a_log_with_no_stored_schema_refuses_to_open(tmp_path: Path) -> None:
    """No fallback to the table's own view.

    Under I16 a schema change records its intent before acting and is replayed
    on recovery, so a log missing its Arrow schema has not been interrupted —
    it is damaged. Guessing from the table would serve reads under a schema the
    data does not have.
    """
    schema = pa.schema([pa.field("event_ts", pa.int64()), pa.field("key", pa.string())])
    Log.new(tmp_path, "s", schema=schema, sort_by=("event_ts",)).close()

    log = Log.open(tmp_path, "s")
    log._buffer._con.execute("DELETE FROM meta WHERE k = 'arrow_schema'")
    log.close()

    with pytest.raises(ValueError, match="no stored Arrow schema"):
        Log.open(tmp_path, "s")


def test_a_schema_disagreeing_with_the_table_refuses_to_open(tmp_path: Path) -> None:
    """The two records disagreeing means something wrote outside litelink."""
    schema = pa.schema([pa.field("event_ts", pa.int64()), pa.field("key", pa.string())])
    Log.new(tmp_path, "s", schema=schema, sort_by=("event_ts",)).close()

    stale = pa.schema([pa.field("event_ts", pa.int64()), pa.field("gone", pa.string())])
    log = Log.open(tmp_path, "s")
    log._buffer.set_meta("arrow_schema", stale.serialize().to_pybytes().hex())
    log.close()

    with pytest.raises(ValueError, match="disagrees with the Iceberg table"):
        Log.open(tmp_path, "s")
