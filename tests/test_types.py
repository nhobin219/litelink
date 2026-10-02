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
from litelink import LogConfig
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
    pa.binary(16),
    pa.struct([pa.field("a", pa.int64()), pa.field("b", pa.string())]),
    pa.map_(pa.string(), pa.int64()),
    pa.list_(pa.string()),
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
    "binary": b"\x00\xffbytes",
    "fixed_size_binary[16]": bytes(range(16)),
    "struct<a: int64, b: string>": {"a": 7, "b": "x"},
    "map<string, int64>": {"k": 1, "j": 2},
    "list<item: string>": ["a", "b"],
}


def _as_read(values: list[object], type_: pa.DataType) -> list[object]:
    """What a read hands back for `values` appended to a `type_` column.

    Not `values` itself: a map is appended as a dict and read back as a list of
    pairs. Building the expectation through Arrow at the declared type is
    right for every column rather than a special case for one.
    """
    return pa.array(values, type=type_).to_pylist()


@pytest.mark.parametrize("type_", CARRIED, ids=str)
def test_carried_types_survive_a_round_trip(tmp_path: Path, type_: pa.DataType) -> None:
    """Create, append, seal, read — with a real value AND a null."""
    schema = pa.schema([pa.field("event_ts", pa.int64()), pa.field("c", type_)])
    root = tmp_path / str(type_).replace("[", "_").replace("]", "")
    sample = SAMPLE[str(type_)]

    with litelink.new(root, "s", schema=schema, sort_by=("event_ts",)) as log:
        log.extend([{"event_ts": 1, "c": sample}, {"event_ts": 2, "c": None}])
        expected = _as_read([sample, None], type_)
        assert log.scan().read_all()["c"].to_pylist() == expected, "from buffer"

        log.seal(flush=True)

        assert log.scan().read_all()["c"].to_pylist() == expected, "from table"


@pytest.mark.parametrize(
    ("type_", "reason"),
    [
        (pa.uint32(), "unsigned"),
        (pa.uint64(), "unsigned"),
        (pa.int8(), "widens"),
        (pa.int16(), "widens"),
        (pa.large_binary(), "declare binary"),
        (pa.large_list(pa.int64()), "declare list"),
        (pa.dense_union([pa.field("a", pa.int64())]), "no union type"),
        (pa.struct([]), "at least one field"),
        (pa.map_(pa.float64(), pa.string()), "map keys must be"),
        (pa.list_(pa.uint32()), "unsigned"),
        (
            pa.struct([pa.field("a", pa.int64()), pa.field("a", pa.string())]),
            "duplicate",
        ),
        (pa.timestamp("us"), "Represent time as a column type that is"),
        (pa.decimal128(10, 2), "not yet supported"),
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

        log.seal(flush=True)

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
    ("float32 max", pa.float32(), 3.4028234663852886e38),
    ("empty string", pa.string(), ""),
    ("nul byte in string", pa.string(), "a\x00b"),
    ("quotes and newline", pa.string(), 'it\'s\n"quoted"\\'),
    ("astral plane", pa.string(), "🛩️ ☃ Ω"),
    ("100 KB string", pa.string(), "x" * 100_000),
    ("bool false", pa.bool_(), False),
    ("empty binary", pa.binary(), b""),
    ("every byte value", pa.binary(), bytes(range(256))),
    ("fixed all 0xff", pa.binary(8), b"\xff" * 8),
    ("empty map", pa.map_(pa.string(), pa.string()), {}),
    ("empty list", pa.list_(pa.int64()), []),
    ("list of nulls", pa.list_(pa.int64()), [None, None]),
    ("struct of nulls", pa.struct([pa.field("a", pa.int64())]), {"a": None}),
    ("int64 limits nested", pa.list_(pa.int64()), [2**63 - 1, -(2**63)]),
    (
        "largest finite doubles nested",
        pa.list_(pa.float64()),
        [1.7976931348623157e308, -5e-324],
    ),
    ("int keys", pa.map_(pa.int32(), pa.binary()), {-(2**31): b"", 7: b"\x00"}),
    ("quotes in a key", pa.map_(pa.string(), pa.string()), {'a"b\\': "\n"}),
    (
        "three levels",
        pa.struct([pa.field("m", pa.map_(pa.string(), pa.list_(pa.binary(4))))]),
        {"m": {"k": [b"abcd", None]}},
    ),
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
        expected = _as_read([value], type_)

        assert log.scan().read_all()["c"].to_pylist() == expected, "from the buffer"

        log.seal(flush=True)

        assert log.scan().read_all()["c"].to_pylist() == expected, "from the table"


@pytest.mark.parametrize("nullable", [True, False], ids=["nullable", "required"])
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")], ids=str)
@pytest.mark.parametrize("type_", [pa.float32(), pa.float64()], ids=str)
def test_a_non_finite_float_is_refused_naming_its_column(
    tmp_path: Path, type_: pa.DataType, value: float, nullable: bool
) -> None:
    """A log holds only finite floats (#87), and says so naming the column.

    Three different mechanisms reach the one refusal. ±inf fails the float
    CHECK. NaN never reaches a CHECK — SQLite stores it as NULL — so in a
    nullable column the post-insert test catches it, and in a required one it
    fails NOT NULL and `_explain` recognises it, where it used to escape as a
    bare `NOT NULL constraint failed`.

    Falsify by restoring `OR abs(x) = 9e999` to the float32 CHECK (its inf
    cases pass), or by removing the non-finite test from `_explain` (the
    required NaN cases raise SQLite's message instead).
    """
    schema = pa.schema(
        [pa.field("event_ts", pa.int64()), pa.field("c", type_, nullable=nullable)]
    )

    with litelink.new(tmp_path, "s", schema=schema, sort_by=("event_ts",)) as log:
        with pytest.raises(
            ValueError,
            match=r"column 'c' cannot hold (nan|inf|-inf): a log holds only finite floats",
        ):
            log.extend([{"event_ts": 1, "c": 1.0}, {"event_ts": 2, "c": value}])

        assert log.end_offset() == 1, "the whole batch rolled back"

        log.append({"event_ts": 3, "c": 2.5})
        log.seal(flush=True)
        assert log.scan().read_all()["c"].to_pylist() == [2.5]


# -- binary and nested columns, for OpenTelemetry logs (#79) -------------------

ANY_VALUE = pa.struct(
    [
        pa.field("s", pa.string()),
        pa.field("b", pa.bool_()),
        pa.field("i", pa.int64()),
        pa.field("d", pa.float64()),
        pa.field("x", pa.binary()),
    ]
)
"""OTel's AnyValue, one nullable field per variant: Iceberg has no union."""

OTEL = pa.schema(
    [
        pa.field("ts", pa.int64(), nullable=False),
        pa.field("severity", pa.int32()),
        pa.field("body", ANY_VALUE),
        pa.field("attributes", pa.map_(pa.string(), ANY_VALUE)),
        pa.field("trace_id", pa.binary(16)),
        pa.field("span_id", pa.binary(8)),
        pa.field("tags", pa.list_(pa.string())),
    ]
)


def otel_row(n: int) -> dict[str, object]:
    return {
        "ts": n,
        "severity": 9,
        "body": {"s": f"message {n}"},
        "attributes": {
            "service.name": {"s": "api"},
            "attempt": {"i": n},
            "raw": {"x": bytes([n % 256, 0, 255])},
            "ratio": {"d": n / 3},
        },
        "trace_id": n.to_bytes(16, "big"),
        "span_id": n.to_bytes(8, "big"),
        "tags": ["a", f"t{n}"],
    }


def _read_back(log: litelink.LogHandle) -> list[dict[str, object]]:
    return log.scan().read_all().drop(["litelink_offset"]).to_pylist()


def _expected(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return pa.Table.from_pylist(rows, schema=OTEL).to_pylist()


def test_an_otel_log_round_trips_through_every_local_path(tmp_path: Path) -> None:
    """The shape #79 exists for, through buffer, seal, compaction and reopen.

    Exact at every step, and the nested parts stay queryable as what they are:
    a struct field and a map entry in a predicate, a trace id compared as bytes.
    """
    config = LogConfig(target_seal_rows=10, compact_min_files=2)
    rows = [otel_row(n) for n in range(1, 26)]

    with litelink.new(
        tmp_path, "s", schema=OTEL, sort_by=("ts",), config=config
    ) as log:
        log.extend(rows[:5])
        assert _read_back(log) == _expected(rows[:5]), "from the buffer"

        log.seal(flush=True)
        log.extend(rows[5:])
        assert _read_back(log) == _expected(rows), "across table and buffer"

        assert log.scan(where="body.s = 'message 7'").read_all().num_rows == 1
        at = log.sql("SELECT attributes['attempt'].i AS n FROM log WHERE ts = 20")
        assert at.read_all().to_pylist() == [{"n": 20}]
        wanted = (3).to_bytes(16, "big").hex()
        by_id = log.sql(f"SELECT ts FROM log WHERE trace_id = from_hex('{wanted}')")
        assert by_id.read_all().to_pylist() == [{"ts": 3}]

        log.seal(flush=True)
        log.advance()
        assert log.staging_files() == 1, "compaction merged nested files"
        assert _read_back(log) == _expected(rows), "after compaction"

    with litelink.open(tmp_path, "s") as reopened:
        assert _read_back(reopened) == _expected(rows), "after reopen"


REFUSED_NESTED: list[tuple[str, dict[str, object], str]] = [
    ("unknown struct key", {"body": {"s": "x", "zz": 1}}, "does not have: \\['zz'\\]"),
    ("wrong leaf type", {"attributes": {"k": {"x": "text"}}}, r"\['k'\]\.x: 'text'"),
    ("int64 overflow in a map", {"attributes": {"k": {"i": 2**64}}}, r"\['k'\]\.i"),
    ("inexact int into double", {"body": {"d": 2**60}}, "exactly only up to"),
    ("bool into int", {"body": {"i": True}}, "not a valid int64"),
    ("duplicate map key", {"attributes": [("a", {}), ("a", {})]}, "duplicate map key"),
    ("None map key", {"attributes": {None: {"s": "x"}}}, "cannot be None"),
    ("not a pair", {"attributes": [("a",)]}, "not a \\(key, value\\) pair"),
    ("str where a list goes", {"tags": "ab"}, "not a valid list"),
    ("wrong list item", {"tags": ["a", 3]}, r"\[1\]: 3"),
    ("scalar where a struct goes", {"body": "text"}, "not a valid struct"),
    ("short trace id", {"trace_id": b"x" * 15}, "fixed_size_binary\\[16\\]"),
    ("str where bytes go", {"span_id": "0102030405060708"}, "fixed_size_binary\\[8\\]"),
    ("NaN in a struct", {"body": {"d": float("nan")}}, "at d: nan is not finite"),
    (
        "inf in a map value",
        {"attributes": {"k": {"d": float("inf")}}},
        r"\['k'\]\.d: inf",
    ),
    (
        "-inf in a map value",
        {"attributes": {"k": {"d": float("-inf")}}},
        "-inf is not finite",
    ),
]


@pytest.mark.parametrize(
    ("label", "change", "match"),
    REFUSED_NESTED,
    ids=[label for label, _, _ in REFUSED_NESTED],
)
def test_a_nested_or_binary_value_the_column_cannot_hold_is_refused(
    tmp_path: Path, label: str, change: dict[str, object], match: str
) -> None:
    """Refused at append, naming the column and the path inside it.

    SQLite cannot see inside a nested value and Arrow would accept several of
    these — it drops an unknown struct key without a word — so without the
    check each is either a row acknowledged and silently changed, or one that
    fails the seal for ever.

    Falsify by making `Nested.encode` skip `self._encode`: every nested row
    here is then accepted.
    """
    with litelink.new(tmp_path, "s", schema=OTEL, sort_by=("ts",)) as log:
        with pytest.raises(ValueError, match=match):
            log.extend([otel_row(1), {**otel_row(2), **change}])

        assert log.end_offset() == 1, "the whole batch rolled back"

        log.append(otel_row(3))
        log.seal(flush=True)
        assert _read_back(log) == _expected([otel_row(3)])


def test_a_nested_value_counts_its_stored_size_toward_the_seal(tmp_path: Path) -> None:
    """The seal cut is by bytes, so a nested value must count as what it is.

    Measured from the stored JSON. Counting the Python object — a dict, a fixed
    8 bytes — would let a buffer of large attribute maps grow far past
    `target_seal_size` before the appender cut a file.

    Falsify by measuring the raw row again in `_row_bytes`: nothing is queued.
    """
    schema = pa.schema(
        [pa.field("ts", pa.int64()), pa.field("m", pa.map_(pa.string(), pa.string()))]
    )
    config = LogConfig(target_seal_size=4096)

    with litelink.new(tmp_path, "s", schema=schema, config=config) as log:
        log.extend([{"ts": i, "m": {"k": "x" * 1000}} for i in range(8)])

        assert log.seal() is not None, "8 KB of maps crossed a 4 KB target"


def test_sort_by_cannot_name_a_nested_column(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="sort_by cannot name"):
        litelink.new(tmp_path, "s", schema=OTEL, sort_by=("attributes",))
