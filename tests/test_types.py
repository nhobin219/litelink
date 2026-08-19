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


@pytest.mark.parametrize("type_", CARRIED, ids=str)
def test_carried_types_survive_a_round_trip(tmp_path: Path, type_: pa.DataType) -> None:
    """Create, append, seal, read — the path the KeyError used to hide in."""
    schema = pa.schema([pa.field("event_ts", pa.int64()), pa.field("c", type_)])
    root = tmp_path / str(type_).replace("[", "_").replace("]", "")

    with Log.new(root, "s", schema=schema, sort_by=("event_ts",)) as log:
        log.append({"event_ts": 1, "c": None})
        assert log.scan().read_all().num_rows == 1, "readable from the buffer"
        log.seal()
        assert log.scan().read_all().num_rows == 1, "readable from the table"


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


def test_the_schema_a_log_carries_is_the_one_the_table_reports(tmp_path: Path) -> None:
    """Arrow `string` is stored as Iceberg string and reads back `large_string`.

    The log adopts the stored type immediately, so a fresh log and a reopened
    one agree — otherwise the DuckDB cast differs between them.
    """
    schema = pa.schema([pa.field("event_ts", pa.int64()), pa.field("key", pa.string())])

    with Log.new(tmp_path, "s", schema=schema, sort_by=("event_ts",)) as created:
        created_schema = created._schema

    with Log.open(tmp_path, "s") as reopened:
        assert created_schema == reopened._schema
        assert created_schema.field("key").type == pa.large_string()


def test_arrow_schemas_get_their_field_ids_assigned(tmp_path: Path) -> None:
    """Why Arrow is the default: the numbering is not the caller's job.

    Stating the Iceberg schema directly would mean a hand-written id per
    column, and pyiceberg accepts duplicates silently — which breaks §9's add,
    drop and rename, since those resolve by id. Arrow hands that bookkeeping to
    pyiceberg, which assigns unique ids by construction. That is why there is
    one schema type and it is this one.
    """
    schema = pa.schema([pa.field("event_ts", pa.int64()), pa.field("key", pa.string())])

    with Log.new(tmp_path, "s", schema=schema, sort_by=("event_ts",)) as log:
        ids = [f.field_id for f in log._table._table.schema().fields]

        assert len(set(ids)) == len(ids), "assigned ids must be unique"
        assert log._table.offset_field_id() in ids
