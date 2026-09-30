"""Which archive files a query could need, decided from bounds kept locally (#90).

Every query reads the buffer and the local table. The archive is read only when
some archive file could hold a row the query matches, and that is decided here
without touching the network: `sync` records each file's per-column bounds in
`buffer.db` before it registers the file, so the question is a comparison
against rows already on disk.

**The decision is DuckDB's, not this module's.** A predicate `price > 78000.5`
becomes `hi_price > 78000.5`, evaluated by DuckDB over a relation of bounds
typed exactly as the log's columns — so the comparison binds with the same
coercions the real predicate does. Evaluating the constant here in Python and
comparing it would not: DuckDB compares an integer column to `5.5` as a
DOUBLE, where casting `5.5` to the column's type rounds it to 6, and a file
whose maximum is exactly 6 would then read as unable to match `price > 5.5`.

**Anything not understood reads the archive.** The rule throughout is the one
`_statistics` states for its consumers: missing information must widen the
answer, never narrow it. A predicate this does not translate is dropped from
the condition (so it constrains nothing), a query whose shape it does not
recognise yields no condition at all, and a bound that is unknown compares as
NULL and is coalesced to "could match".
"""

from __future__ import annotations

import json
import threading
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, NamedTuple, cast

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.conversions import from_bytes

from litelink._statistics import _BOUNDED

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping

    from pyiceberg.manifest import DataFile
    from pyiceberg.schema import Schema
    from pyiceberg.types import PrimitiveType

# The relation the bounds are registered under, per query cursor.
BOUNDS_REL = "__litelink_archive_bounds"

# A column's comparison, with the column on the LEFT, as the bound it tests.
# `col > K` can hold for a row of a file exactly when the file's maximum is
# above K; `col < K`, when its minimum is below it.
_COMPARE = {
    "COMPARE_GREATERTHAN": ("hi", ">"),
    "COMPARE_GREATERTHANOREQUALTO": ("hi", ">="),
    "COMPARE_LESSTHAN": ("lo", "<"),
    "COMPARE_LESSTHANOREQUALTO": ("lo", "<="),
}

# A query's condition depends only on its text and the columns it can name, and
# a polling reader sends the same text over and over — so it is parsed once.
_CACHED_CONDITIONS = 256
_CONDITIONS: OrderedDict[
    tuple[str, tuple[tuple[str, str], ...], frozenset[str]], Condition | None
] = OrderedDict()
_CACHE_LOCK = threading.Lock()

# The same comparison with its operands swapped: `K < col` is `col > K`.
_MIRROR = {
    "COMPARE_GREATERTHAN": "COMPARE_LESSTHAN",
    "COMPARE_GREATERTHANOREQUALTO": "COMPARE_LESSTHANOREQUALTO",
    "COMPARE_LESSTHAN": "COMPARE_GREATERTHAN",
    "COMPARE_LESSTHANOREQUALTO": "COMPARE_GREATERTHANOREQUALTO",
    "COMPARE_EQUAL": "COMPARE_EQUAL",
}


def file_bounds(schema: Schema, data_file: DataFile) -> dict[str, list[Any]]:
    """`{column: [lo, hi]}` for the columns whose bounds this file records.

    Only the types whose Iceberg bounds are the values themselves — integers,
    floats and booleans (`_statistics._BOUNDED`). A column with no recorded
    bound is left out, which reads as "unknown" and so as "could match":
    strings are truncated, a nested column keeps none, and a column added after
    the file was written has none in it.
    """
    lowers = data_file.lower_bounds or {}
    uppers = data_file.upper_bounds or {}
    bounds: dict[str, list[Any]] = {}
    for field in schema.fields:
        if not isinstance(field.field_type, _BOUNDED):
            continue

        low = lowers.get(field.field_id)
        high = uppers.get(field.field_id)
        if low is None or high is None:
            continue

        field_type = cast("PrimitiveType", field.field_type)
        bounds[field.name] = [from_bytes(field_type, low), from_bytes(field_type, high)]

    return bounds


def encode(bounds: Mapping[str, list[Any]]) -> str:
    """The stored form. JSON round-trips every bounded type exactly — Python
    writes a float as its shortest repr, which reads back as the same double."""
    return json.dumps(bounds, sort_keys=True)


class Atom(NamedTuple):
    """One comparison of a stored bound with a constant: `hi_3 > (5)`.

    `exact` is the constant as an int when both sides are integers — a literal
    with no cast, against an integer column — which is the one case where
    evaluating it outside DuckDB cannot disagree with DuckDB: integer
    comparison has no coercion to get wrong.
    """

    bound: str
    operator: str
    constant: str
    exact: int | None

    def sql(self) -> str:
        return f'"{self.bound}" {self.operator} ({self.constant})'


# A conjunct: any one of its alternatives, each a conjunction of atoms. `x = K`
# is one alternative of two atoms; `x IN (a, b)` is two alternatives.
Term = tuple[tuple[Atom, ...], ...]

_PYARROW = {
    "<": pc.less,
    "<=": pc.less_equal,
    ">": pc.greater,
    ">=": pc.greater_equal,
}


class Condition(NamedTuple):
    """The AND of its terms, each true for a file whose bounds say it could
    hold a matching row. An unknown bound makes an atom NULL, and a term
    whose value is NULL counts as true: missing information widens."""

    terms: tuple[Term, ...]

    def also(self, *terms: Term) -> Condition:
        return Condition(self.terms + terms)

    @property
    def exact(self) -> bool:
        return all(
            atom.exact is not None
            for term in self.terms
            for alternative in term
            for atom in alternative
        )

    def sql(self) -> str:
        """Over `BOUNDS_REL`. Each term coalesced on its own: NULL AND false
        is false, so one unknown column must not rule out a file another
        column cannot."""
        return " AND ".join(
            "coalesce(("
            + " OR ".join(
                "(" + " AND ".join(atom.sql() for atom in alternative) + ")"
                for alternative in term
            )
            + "), true)"
            for term in self.terms
        )

    def holds_for_any(self, bounds: pa.Table) -> bool:
        """Whether any file could match, in pyarrow. Only when `exact`.

        Kleene logic throughout, so NULL propagates exactly as it does in the
        SQL form, and a term left NULL is then read as true. Measured at about
        0.02 ms against 0.8 ms for the same test as a DuckDB statement, which
        on a hot read was a fifth of the whole query.
        """
        rows = bounds.num_rows
        result = pa.array([True] * rows, pa.bool_())
        for term in self.terms:
            either = pa.array([False] * rows, pa.bool_())
            for alternative in term:
                every = pa.array([True] * rows, pa.bool_())
                for atom in alternative:
                    compare = _PYARROW[atom.operator]
                    column = bounds.column(atom.bound).cast(pa.int64())
                    every = pc.and_kleene(
                        every, compare(column, pa.scalar(atom.exact, pa.int64()))
                    )

                either = pc.or_kleene(either, every)

            result = pc.and_(result, pc.fill_null(either, fill_value=True))

        return bool(pc.any(result).as_py())


def condition(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    columns: Mapping[str, str],
    integers: frozenset[str] = frozenset(),
) -> Condition | None:
    """What a file's bounds must satisfy for `query` to match a row of it.

    `columns` maps each bounded column's name, lowercased, to the suffix of
    its bound columns in `BOUNDS_REL` — `lo_<n>`/`hi_<n>` — and `integers`
    names the suffixes whose columns are integers. None when nothing in the
    query narrows anything, which the caller reads as "every file could
    match".

    **Narrowed only for one shape: a single SELECT over `log` alone.** No
    joins, CTEs, set operations or subqueries, because the pruned relation is
    the `log` view itself — every reference to it sees the same rows, so a
    second reference with a different predicate, or none, would see rows
    missing that it needed. Only the top-level WHERE is read, and only its
    AND-ed conjuncts: an OR, a NOT, or a comparison against anything other
    than a constant drops out and constrains nothing.

    Select-list aliases cannot capture a WHERE column: DuckDB binds a name in
    WHERE to the table's column before an alias, which was checked rather
    than assumed.
    """
    key = (query, tuple(sorted(columns.items())), integers)
    with _CACHE_LOCK:
        if key in _CONDITIONS:
            _CONDITIONS.move_to_end(key)
            return _CONDITIONS[key]

    narrowed = _condition(connection, query, columns, integers)
    with _CACHE_LOCK:
        _CONDITIONS[key] = narrowed
        while len(_CONDITIONS) > _CACHED_CONDITIONS:
            _CONDITIONS.popitem(last=False)

    return narrowed


def _condition(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    columns: Mapping[str, str],
    integers: frozenset[str],
) -> Condition | None:
    """`condition`, uncached."""
    try:
        row = connection.execute(
            f"SELECT json_serialize_sql({_quoted(query)})"
        ).fetchone()
    except duckdb.Error:
        return None

    if row is None:
        return None

    parsed: dict[str, Any] = json.loads(str(row[0]))

    if parsed.get("error") or len(parsed.get("statements", ())) != 1:
        return None

    node = parsed["statements"][0]["node"]
    if node.get("type") != "SELECT_NODE" or node.get("cte_map", {}).get("map"):
        return None

    source = node.get("from_table") or {}
    if (
        source.get("type") != "BASE_TABLE"
        or str(source.get("table_name", "")).lower() != "log"
        or source.get("schema_name")
        or source.get("catalog_name")
    ):
        return None

    if any(_is_subquery(item) for item in _walk(node)):
        return None

    where = node.get("where_clause")
    if where is None:
        return None

    qualifiers = {"log"}
    if source.get("alias"):
        qualifiers.add(str(source["alias"]).lower())

    terms = tuple(
        term
        for conjunct in _conjuncts(where)
        if (term := _translate(connection, conjunct, columns, integers, qualifiers))
        is not None
    )

    return Condition(terms) if terms else None


def _quoted(text: str) -> str:
    """A SQL string literal. Inlined rather than bound as a parameter: binding
    a Python value costs DuckDB a round of module lookups per call, which was
    most of what this decision cost a hot read."""
    return "'" + text.replace("'", "''") + "'"


def _walk(item: object) -> Iterator[dict[str, Any]]:
    if isinstance(item, dict):
        yield item
        for value in item.values():
            yield from _walk(value)
    elif isinstance(item, list):
        for value in item:
            yield from _walk(value)


def _is_subquery(item: Mapping[str, Any]) -> bool:
    return item.get("class") == "SUBQUERY" or item.get("type") == "SUBQUERY"


def _conjuncts(expression: dict[str, Any]) -> Iterator[dict[str, Any]]:
    if (
        expression.get("class") == "CONJUNCTION"
        and expression.get("type") == "CONJUNCTION_AND"
    ):
        for child in expression.get("children", ()):
            yield from _conjuncts(child)
    else:
        yield expression


def _translate(
    connection: duckdb.DuckDBPyConnection,
    expression: dict[str, Any],
    columns: Mapping[str, str],
    integers: frozenset[str],
    qualifiers: set[str],
) -> Term | None:
    """One conjunct as a test on bounds, or None if it cannot narrow anything."""

    def operand(node: dict[str, Any], column: str) -> tuple[str, int | None] | None:
        constant = _constant(connection, node)
        if constant is None:
            return None

        return constant, (_exact(node) if column in integers else None)

    kind = expression.get("type")
    if expression.get("class") == "COMPARISON" and kind in _MIRROR:
        column = _column(expression["left"], columns, qualifiers)
        other = expression["right"]
        if column is None:
            column = _column(expression["right"], columns, qualifiers)
            other = expression["left"]
            kind = _MIRROR[kind]

        value = None if column is None else operand(other, column)
        if column is None or value is None:
            return None

        return (_test(column, kind, *value),)

    if expression.get("class") == "BETWEEN" and kind == "COMPARE_BETWEEN":
        column = _column(expression["input"], columns, qualifiers)
        if column is None:
            return None

        lower = operand(expression["lower"], column)
        upper = operand(expression["upper"], column)
        if lower is None or upper is None:
            return None

        return (
            (
                Atom(f"hi_{column}", ">=", *lower),
                Atom(f"lo_{column}", "<=", *upper),
            ),
        )

    if expression.get("class") == "OPERATOR" and kind == "COMPARE_IN":
        children = expression.get("children", [])
        column = _column(children[0], columns, qualifiers) if children else None
        if column is None:
            return None

        values = [operand(child, column) for child in children[1:]]
        known = [value for value in values if value is not None]
        if not known or len(known) != len(values):
            return None

        return tuple(_test(column, "COMPARE_EQUAL", *value) for value in known)

    return None


def _test(column: str, kind: str, constant: str, exact: int | None) -> tuple[Atom, ...]:
    """One alternative: the atoms `column <kind> constant` needs of a file."""
    if kind == "COMPARE_EQUAL":
        return (
            Atom(f"lo_{column}", "<=", constant, exact),
            Atom(f"hi_{column}", ">=", constant, exact),
        )

    bound, operator = _COMPARE[kind]

    return (Atom(f"{bound}_{column}", operator, constant, exact),)


# The integer literal types DuckDB's parser produces, before any binding.
_INTEGER_LITERALS = frozenset(
    {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UBIGINT", "UHUGEINT"}
)


def _exact(node: Mapping[str, Any]) -> int | None:
    """A bare integer literal's value, when it fits in an int64; else None.

    Bare: a CAST can change what the literal means before it is compared,
    so only DuckDB may evaluate one.
    """
    if node.get("class") != "CONSTANT":
        return None

    value = node.get("value") or {}
    number = value.get("value")
    if (
        value.get("is_null")
        or (value.get("type") or {}).get("id") not in _INTEGER_LITERALS
        or not isinstance(number, int)
        or isinstance(number, bool)
        or not -(2**63) <= number < 2**63
    ):
        return None

    return number


def _column(
    expression: Mapping[str, Any], columns: Mapping[str, str], qualifiers: set[str]
) -> str | None:
    """The bound-column suffix a column reference names, or None."""
    if expression.get("class") != "COLUMN_REF":
        return None

    names = [str(name).lower() for name in expression.get("column_names", ())]
    if len(names) == 2 and names[0] in qualifiers:
        names = names[1:]

    if len(names) != 1:
        return None

    return columns.get(names[0])


def _constant(
    connection: duckdb.DuckDBPyConnection, expression: dict[str, Any]
) -> str | None:
    """A constant expression's SQL, regenerated by DuckDB's own serialiser.

    A literal, or casts of one — nothing that reads a column, calls a
    function, or names a parameter. `now()` is excluded deliberately: this is
    evaluated in a different statement from the query, a moment earlier, and
    `ts < now() - INTERVAL 1 HOUR` then admits fewer rows here than there.
    """
    probe = expression
    while probe.get("class") == "CAST":
        probe = probe["child"]

    if probe.get("class") != "CONSTANT":
        return None

    template = {
        "error": False,
        "statements": [
            {
                "node": {
                    "type": "SELECT_NODE",
                    "modifiers": [],
                    "cte_map": {"map": []},
                    "select_list": [expression],
                    "from_table": {
                        "type": "EMPTY",
                        "alias": "",
                        "sample": None,
                        "query_location": 18446744073709551615,
                    },
                    "where_clause": None,
                    "group_expressions": [],
                    "group_sets": [],
                    "aggregate_handling": "STANDARD_HANDLING",
                    "having": None,
                    "sample": None,
                    "qualify": None,
                },
                "named_param_map": [],
            }
        ],
    }
    try:
        row = connection.execute(
            f"SELECT json_deserialize_sql({_quoted(json.dumps(template))})"
        ).fetchone()
    except duckdb.Error:
        return None

    text = None if row is None else str(row[0])
    if text is None or not text.startswith("SELECT "):
        return None

    return text.removeprefix("SELECT ")


def _bounded(field_type: pa.DataType) -> bool:
    """The Arrow side of `_statistics._BOUNDED`: the types a file keeps exact
    bounds for. Unsigned and narrow integers never reach a log (`_types`)."""
    return (
        pa.types.is_int32(field_type)
        or pa.types.is_int64(field_type)
        or pa.types.is_floating(field_type)
        or pa.types.is_boolean(field_type)
    )


def stats_columns(schema: pa.Schema) -> dict[str, str]:
    """Each bounded column's lowercased name, to its suffix in `BOUNDS_REL`.

    By index rather than by name, so a column called anything at all — a
    quote, a space — needs no escaping inside the condition. A name that
    collides with another once lowercased is left out: DuckDB resolves names
    case-insensitively, and a reference could mean either.
    """
    lowered = [name.lower() for name in schema.names]

    return {
        low: str(index)
        for index, (low, field) in enumerate(zip(lowered, schema, strict=True))
        if lowered.count(low) == 1 and _bounded(field.type)
    }


def integer_columns(schema: pa.Schema) -> frozenset[str]:
    """The `BOUNDS_REL` suffixes of the integer columns among `stats_columns`."""
    return frozenset(
        str(index)
        for index, field in enumerate(schema)
        if pa.types.is_int32(field.type) or pa.types.is_int64(field.type)
    )


def relation(schema: pa.Schema, stored: Iterable[str]) -> pa.Table:
    """The stored bounds as a table DuckDB can test a condition against.

    Each bound column is typed exactly as the log column it bounds, which is
    what makes `hi_<n> > K` bind the way `column > K` does. A file with no
    bound for a column gets NULL there, and the condition reads NULL as
    "could match".
    """
    decoded = [json.loads(bounds) for bounds in stored]
    columns: dict[str, pa.Array] = {}
    for index, field in enumerate(schema):
        if not _bounded(field.type):
            continue

        pairs = [bounds.get(field.name) or (None, None) for bounds in decoded]
        columns[f"lo_{index}"] = pa.array([p[0] for p in pairs], type=field.type)
        columns[f"hi_{index}"] = pa.array([p[1] for p in pairs], type=field.type)

    if not columns:
        # Nothing bounded, not even the offset — unreachable for a log, whose
        # offset is always int64. Still a relation with one row per file.
        columns["files"] = pa.array([0] * len(decoded), type=pa.int8())

    return pa.table(columns)
