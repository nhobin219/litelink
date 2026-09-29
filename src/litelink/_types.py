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

`binary` and `fixed_size_binary(n)` are carried as SQLite blobs, for values the
size of a trace id — NOT payloads. Their bytes go through the buffer, the WAL
replica and every hot read like any other value; frames and point clouds are
§15's blob fields, which bypass the buffer and are not built yet (SPEC §15).

`struct`, `map` and `list` are carried as JSON text, because SQLite has no
nested type and so no CHECK can see inside one. Their values are checked in
Python instead, against the declared type, before the insert — see `Nested`.
"""

from __future__ import annotations

import base64
import functools
import json
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, NamedTuple

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

    carriers: frozenset[type]
    """The EXACT Python types that may carry a value into this column.

    `NoneType` is in every set: a NULL is a question about nullability, which
    `Shape.required` answers with its own message, not about the carrier.

    Exact types rather than an isinstance relation, because the check that uses
    this is `type(v) in carriers` — one C-level set lookup per value, on the
    write path. `bool` is deliberately absent from the integer sets: `True` is
    an `int` to `isinstance` and would otherwise be stored as `1` in an int64
    column with nothing said. `accepts` is what forgives a subclass."""

    accepts: Callable[[object], bool]
    """The definitive check, consulted only when `carriers` misses.

    A `str` subclass — a `StrEnum` member is the common one — is a legitimate
    value that `type(v) in carriers` rejects. This is the second opinion that
    lets it through, and it costs nothing in the ordinary case because a
    correct value never reaches it."""

    bounds: tuple[float, float] | None
    """The range a value must fall in, or None if the type cannot overflow.

    Only `int32` and `float32` have one, which is what keeps this affordable:
    a schema of int64s, float64s and strings has no bounded column at all and
    the check is one truthiness test per row.

    The two failure modes differ and both are silent at append. An int32 given
    2**40 is stored by SQLite unchanged and then wedges EVERY scan
    (`Integer value ... not in range`). A float32 given 1e300 is worse — it
    reads back as `inf`, with no error anywhere.

    `int64` is deliberately absent. SQLite refuses an out-of-range integer
    itself, at the insert, inside the transaction and with no offset consumed;
    adding bounds for it would make every ordinary schema pay the loop."""

    exact_int: int | None
    """The largest integer this float type represents EXACTLY, or None.

    Only floats have one. An integer is a legal value for a float column —
    `{"price": 5}` is too natural to refuse — but only while the conversion is
    lossless, which is the same rule that refuses `1.5` into an int64.

    It is not merely about precision. The buffer stores values with no
    conversion, so an out-of-range integer stays an INTEGER in SQLite and
    `pa.array(..., type=float64)` then refuses to build the column at all:
    every scan and every seal raises for ever, while appends keep succeeding
    and the buffer never drains. 2**53 for float64, 2**24 for float32."""

    variable_length: bool
    """Whether a value's size depends on the value.

    The seal trigger works in bytes, so the buffer has to measure itself. Fixed
    types count as a constant; only these need asking SQLite. Recorded here
    rather than inferred from the affinity, so that "is this variable-length"
    is not a second opinion about types held somewhere else."""

    width: int | None = None
    """The exact byte length of a `fixed_size_binary` value, or None."""

    nested: Nested | None = None
    """How a `struct`, `map` or `list` value is checked, stored and read back,
    or None for a column SQLite can hold directly."""


def _is_int(value: object) -> bool:
    # `bool` excluded: it is an `int` subclass, so `True` would otherwise be a
    # legal int64 and read back as `1`.
    return isinstance(value, int) and not isinstance(value, bool)


def _is_float(value: object) -> bool:
    # An `int` is a lossless float for every value that matters here, and
    # `{"price": 5}` is too natural to refuse.
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_str(value: object) -> bool:
    return isinstance(value, str)


def _is_bool(value: object) -> bool:
    return isinstance(value, bool)


def _is_bytes(value: object) -> bool:
    return isinstance(value, bytes | bytearray)


_NONE = type(None)
_INT = frozenset({int, _NONE})
_FLOAT = frozenset({float, int, _NONE})
_STR = frozenset({str, _NONE})
_BOOL = frozenset({bool, _NONE})
_BYTES = frozenset({bytes, bytearray, _NONE})

_INT32 = (-(2**31), 2**31 - 1)
# The largest finite float32. A float64 above it becomes `inf` on the way in.
_FLOAT32 = (-3.4028235e38, 3.4028235e38)

# Matched in order, because Arrow's predicates overlap.
_SUPPORTED: tuple[tuple[Callable[[pa.DataType], bool], ColumnType], ...] = (
    (
        pa.types.is_boolean,
        ColumnType(
            "INTEGER", "BOOLEAN", _BOOL, _is_bool, None, None, variable_length=False
        ),
    ),
    (
        pa.types.is_int32,
        ColumnType(
            "INTEGER", "INTEGER", _INT, _is_int, _INT32, None, variable_length=False
        ),
    ),
    (
        pa.types.is_int64,
        ColumnType(
            "INTEGER", "BIGINT", _INT, _is_int, None, None, variable_length=False
        ),
    ),
    (
        pa.types.is_float32,
        ColumnType(
            "REAL", "FLOAT", _FLOAT, _is_float, _FLOAT32, 2**24, variable_length=False
        ),
    ),
    (
        pa.types.is_float64,
        ColumnType(
            "REAL", "DOUBLE", _FLOAT, _is_float, None, 2**53, variable_length=False
        ),
    ),
    (
        pa.types.is_string,
        ColumnType("TEXT", "VARCHAR", _STR, _is_str, None, None, variable_length=True),
    ),
    (
        pa.types.is_large_string,
        ColumnType("TEXT", "VARCHAR", _STR, _is_str, None, None, variable_length=True),
    ),
    (
        pa.types.is_binary,
        ColumnType("BLOB", "BLOB", _BYTES, _is_bytes, None, None, variable_length=True),
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
        pa.types.is_large_binary,
        "declare binary: Iceberg has one binary type, and a buffered value is "
        "far below the 2 GiB that large_binary exists to exceed.",
    ),
    (
        lambda t: (
            pa.types.is_large_list(t)
            or pa.types.is_list_view(t)
            or pa.types.is_large_list_view(t)
            or pa.types.is_fixed_size_list(t)
        ),
        "declare list: Iceberg has one list type.",
    ),
    (
        pa.types.is_union,
        "Iceberg has no union type. Model the variants as a struct with one "
        "nullable field per variant — OTel's AnyValue is the usual case.",
    ),
    (
        pa.types.is_temporal,
        "not supported. Represent time as a column type that is — an int64 "
        "epoch is the usual choice, a string works too. See SPEC §13.8.",
    ),
    (
        pa.types.is_decimal,
        "not yet supported: SQLite has no column affinity for it.",
    ),
)


@functools.cache
def column_type(type_: pa.DataType) -> ColumnType:
    """How to carry `type_`, or a TypeError explaining why it cannot be.

    Cached: a nested type builds its codec here, and this is asked per column
    by every `Shape` and every read.
    """
    for predicate, mapping in _SUPPORTED:
        if predicate(type_):
            return mapping

    if pa.types.is_fixed_size_binary(type_):
        width = type_.byte_width
        return ColumnType(
            "BLOB",
            "BLOB",
            _BYTES,
            lambda v: isinstance(v, bytes | bytearray) and len(v) == width,
            None,
            None,
            variable_length=False,
            width=width,
        )

    if pa.types.is_struct(type_) or pa.types.is_map(type_) or pa.types.is_list(type_):
        nested = Nested(type_)
        return ColumnType(
            "TEXT",
            nested.duckdb,
            _STR,
            _is_str,
            None,
            None,
            variable_length=True,
            nested=nested,
        )

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


# -- nested columns -----------------------------------------------------------

_INT64 = (-(2**63), 2**63 - 1)
_JSON = json.JSONEncoder(separators=(",", ":"), ensure_ascii=False, allow_nan=True)
_MAP_KEYS = (pa.types.is_string, pa.types.is_large_string, pa.types.is_integer)


class NestedValueError(ValueError):
    """A value inside a nested column that its declared type cannot hold.

    The path to the value is assembled while the error propagates, one segment
    per level, rather than passed down: formatting `['service.name'].x` for
    every field of every row, only to discard it, measured as most of what a
    nested encode cost.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason
        self.path = ""

    def within(self, segment: str) -> None:
        self.path = segment + self.path

    def __str__(self) -> str:
        return f"at {self.path.removeprefix('.') or 'the top level'}: {self.reason}"


class Nested:
    """A `struct`, `map` or `list` column: checked in Python, stored as JSON.

    **Why Python, when every other rule is a SQLite CHECK.** SQLite has no
    nested type, so the stored value is text and no CHECK can see into it. And
    Arrow is not a check either, measured: `pa.array` silently DROPS a struct
    key the type does not declare, and accepts None for a non-nullable child —
    the first is a truncated row `append` acknowledged, the second a file the
    seal cannot write. So this walks the declared type and refuses exactly what
    the scalar columns refuse: a wrong type, an out-of-range number, an
    inexact integer into a float, an unknown struct field, a null where the
    field is not nullable.

    **Why JSON.** It is compact, SQLite and every engine can read it, and it
    round-trips through Python exactly: floats print shortest-repr, integers
    are unbounded, NaN and infinities survive. Bytes cannot be JSON, so they
    are base64; a map is a list of `[key, value]` pairs, so the declared order
    survives. The decode walk exists only for those two, and a subtree that
    holds neither is handed to Arrow as `json.loads` returns it.

    Stored as the declared value, never re-encoded — an unknown struct key is
    refused rather than dropped, so what JSON holds is what was appended.
    """

    def __init__(self, type_: pa.DataType) -> None:
        self.type = type_
        self._encode = _encoder(type_, "")
        self._decode = _decoder(type_)
        self.duckdb = _duckdb(type_)

    def encode(self, value: object) -> str:
        """Check `value` against the type and return what the buffer stores."""
        return _JSON.encode(self._encode(value))

    def decode(self, text: str) -> object:
        """What the buffer stored, as the value `pa.array` builds the type from."""
        value = json.loads(text)
        return value if self._decode is None else self._decode(value)


def _refuse(value: object, expected: str) -> NestedValueError:
    return NestedValueError(
        f"{value!r} ({type(value).__name__}) is not a valid {expected}"
    )


def _encoder(type_: pa.DataType, where: str) -> Callable[[object], object]:
    """The check-and-encode function for `type_`, built once per column.

    `where` names the field for a TypeError at build time only; a refused
    VALUE carries its own path, see `NestedValueError`.
    """
    if pa.types.is_boolean(type_):
        return _encode_bool

    if pa.types.is_int32(type_):
        return _encode_int(_INT32, "int32")

    if pa.types.is_int64(type_):
        return _encode_int(_INT64, "int64")

    if pa.types.is_float32(type_):
        return _encode_float(2**24, _FLOAT32[1], "float")

    if pa.types.is_float64(type_):
        return _encode_float(2**53, None, "double")

    if pa.types.is_string(type_) or pa.types.is_large_string(type_):
        return _encode_str

    if pa.types.is_binary(type_):
        return _encode_bytes(None)

    if pa.types.is_fixed_size_binary(type_):
        return _encode_bytes(type_.byte_width)

    if pa.types.is_struct(type_):
        return _encode_struct(type_, where)

    if pa.types.is_map(type_):
        return _encode_map(type_, where)

    if pa.types.is_list(type_):
        item = type_.value_field
        return _encode_list(_encoder(item.type, f"{where}[]"), item.nullable)

    # Not carried inside a nested column either; `column_type` has the reason.
    try:
        column_type(type_)
    except TypeError as exc:
        msg = f"field {where}: {exc}"
        raise TypeError(msg) from None

    msg = f"field {where}: unsupported type {type_} inside a nested column"
    raise TypeError(msg)


def _encode_bool(value: object) -> object:
    if not isinstance(value, bool):
        raise _refuse(value, "bool")

    return value


def _encode_int(bounds: tuple[int, int], name: str) -> Callable[[object], object]:
    lo, hi = bounds

    def encode(value: object) -> object:
        if not isinstance(value, int) or isinstance(value, bool):
            raise _refuse(value, name)

        number = int(value)
        if not lo <= number <= hi:
            msg = f"{number} is outside {name}'s range"
            raise NestedValueError(msg)

        return number

    return encode


def _encode_float(
    exact: int, limit: float | None, name: str
) -> Callable[[object], object]:
    def encode(value: object) -> object:
        if isinstance(value, float):
            # NaN and the infinities are legal here, unlike a top-level float
            # column: JSON text carries them exactly, where SQLite stores a
            # NaN as NULL. What is refused is a FINITE value that the declared
            # width would turn into an infinity.
            if limit is not None and abs(value) > limit and abs(value) != float("inf"):
                msg = f"{value!r} overflows {name}"
                raise NestedValueError(msg)

            return value

        if isinstance(value, int) and not isinstance(value, bool):
            number = int(value)
            if not -exact <= number <= exact:
                msg = (
                    f"{name} holds every integer exactly only up to {exact}; "
                    "pass it as a float to store it as one"
                )
                raise NestedValueError(msg)

            return float(number)

        raise _refuse(value, name)

    return encode


def _encode_str(value: object) -> object:
    if not isinstance(value, str):
        raise _refuse(value, "string")

    return value


def _encode_bytes(width: int | None) -> Callable[[object], object]:
    expected = "binary" if width is None else f"fixed_size_binary[{width}]"

    def encode(value: object) -> object:
        if not isinstance(value, bytes | bytearray):
            raise _refuse(value, expected)

        if width is not None and len(value) != width:
            msg = f"{len(value)} bytes, but {expected} holds exactly {width}"
            raise NestedValueError(msg)

        return base64.b64encode(value).decode("ascii")

    return encode


def _encode_struct(type_: pa.StructType, where: str) -> Callable[[object], object]:
    fields = [type_.field(i) for i in range(type_.num_fields)]
    label = where or "the top level"
    if not fields:
        msg = f"field {label}: a struct needs at least one field"
        raise TypeError(msg)

    names = [field.name for field in fields]
    if len(set(names)) != len(names):
        msg = f"field {label}: duplicate struct field names {names}"
        raise TypeError(msg)

    declared = frozenset(names)
    parts = [
        (
            field.name,
            field.nullable,
            _encoder(field.type, f"{where}.{field.name}" if where else field.name),
        )
        for field in fields
    ]

    def encode(value: object) -> object:
        if not isinstance(value, Mapping):
            raise _refuse(value, "struct (a mapping of field names)")

        if not declared.issuperset(value):
            # What `pa.array` would do instead is drop them, silently.
            unknown = [key for key in value if key not in declared]
            msg = f"names fields the struct does not have: {unknown}. Declared: {names}"
            raise NestedValueError(msg)

        out: dict[str, object] = {}
        for name, nullable, encode_field in parts:
            item = value.get(name)
            if item is None:
                if not nullable:
                    error = NestedValueError("None, but the field is not nullable")
                    error.within(f".{name}")
                    raise error

                out[name] = None
                continue

            try:
                out[name] = encode_field(item)
            except NestedValueError as exc:
                exc.within(f".{name}")
                raise

        return out

    return encode


def _encode_map(type_: pa.MapType, where: str) -> Callable[[object], object]:
    key_type = type_.key_type
    if not any(predicate(key_type) for predicate in _MAP_KEYS):
        msg = (
            f"field {where or 'at the top level'}: map keys must be a string or "
            f"an integer, not {key_type}"
        )
        raise TypeError(msg)

    encode_key = _encoder(key_type, f"{where}[key]")
    item = type_.item_field
    encode_item = _encoder(item.type, f"{where}[]")
    nullable = item.nullable

    def entry(key: object, item_value: object) -> list[object]:
        try:
            if key is None:
                msg = "a map key cannot be None"
                raise NestedValueError(msg)

            stored_key = encode_key(key)
            if item_value is None:
                if not nullable:
                    msg = "None, but map values are not nullable"
                    raise NestedValueError(msg)

                return [stored_key, None]

            return [stored_key, encode_item(item_value)]
        except NestedValueError as exc:
            exc.within(f"[{key!r}]")
            raise

    def encode(value: object) -> object:
        if isinstance(value, Mapping):
            # A mapping's keys are already unique.
            return [entry(key, item_value) for key, item_value in value.items()]

        if not isinstance(value, list | tuple):
            raise _refuse(value, "map (a mapping, or a sequence of pairs)")

        out: list[list[object]] = []
        seen: set[object] = set()
        for pair in value:
            if not isinstance(pair, list | tuple) or len(pair) != 2:
                msg = f"{pair!r} is not a (key, value) pair"
                raise NestedValueError(msg)

            stored = entry(pair[0], pair[1])
            if stored[0] in seen:
                error = NestedValueError("duplicate map key")
                error.within(f"[{pair[0]!r}]")
                raise error

            seen.add(stored[0])
            out.append(stored)

        return out

    return encode


def _encode_list(
    encode_item: Callable[[object], object], nullable: bool
) -> Callable[[object], object]:
    def encode(value: object) -> object:
        # Not any iterable: a str, a bytes or a dict is iterable and is almost
        # never meant as a list, and each would be split without complaint.
        if not isinstance(value, list | tuple):
            raise _refuse(value, "list (a list or tuple)")

        out: list[object] = []
        for index, item in enumerate(value):
            try:
                if item is None:
                    if not nullable:
                        msg = "None, but list items are not nullable"
                        raise NestedValueError(msg)

                    out.append(None)
                else:
                    out.append(encode_item(item))
            except NestedValueError as exc:
                exc.within(f"[{index}]")
                raise

        return out

    return encode


def _decoder(type_: pa.DataType) -> Callable[[Any], object] | None:
    """Undo what JSON could not hold, or None where `json.loads` already did."""
    if pa.types.is_binary(type_) or pa.types.is_fixed_size_binary(type_):
        return base64.b64decode

    if pa.types.is_struct(type_):
        parts = [
            (type_.field(i).name, _decoder(type_.field(i).type))
            for i in range(type_.num_fields)
        ]
        if all(decode is None for _, decode in parts):
            return None

        needing = [(name, decode) for name, decode in parts if decode is not None]

        def decode_struct(value: dict[str, Any]) -> object:
            # In place: `json.loads` built this dict for this call alone.
            for name, decode in needing:
                item = value[name]
                if item is not None:
                    value[name] = decode(item)

            return value

        return decode_struct

    if pa.types.is_map(type_):
        # Always, because Arrow builds a map from (key, value) TUPLES and JSON
        # returns each pair as a list, which it refuses outright.
        decode_item = _decoder(type_.item_type)

        if decode_item is None:
            return lambda value: list(map(tuple, value))

        item_decoder: Callable[[Any], object] = decode_item

        def decode_map(value: list[list[Any]]) -> object:
            return [
                (key, item if item is None else item_decoder(item))
                for key, item in value
            ]

        return decode_map

    if pa.types.is_list(type_):
        decode_element = _decoder(type_.value_type)
        if decode_element is None:
            return None

        element: Callable[[Any], object] = decode_element

        def decode_list(value: list[Any]) -> object:
            return [item if item is None else element(item) for item in value]

        return decode_list

    return None


def _duckdb(type_: pa.DataType) -> str:
    """The DuckDB spelling of `type_`, for the buffer leg's cast (§7)."""
    if pa.types.is_struct(type_):
        fields = ", ".join(
            '"{}" {}'.format(
                type_.field(i).name.replace('"', '""'), _duckdb(type_.field(i).type)
            )
            for i in range(type_.num_fields)
        )
        return f"STRUCT({fields})"

    if pa.types.is_map(type_):
        return f"MAP({_duckdb(type_.key_type)}, {_duckdb(type_.item_type)})"

    if pa.types.is_list(type_):
        return f"{_duckdb(type_.value_type)}[]"

    return column_type(type_).duckdb
