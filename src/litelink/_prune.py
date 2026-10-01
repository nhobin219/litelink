"""A query's WHERE as manifest terms, so a read can skip whole tiers (#90).

`litelink.manifest.prune` decides from `(column, operator, value)` terms; this
is where a SQL query becomes them. DuckDB parses the query
(`json_serialize_sql`), and each AND-ed comparison between a column and a
constant becomes one term.

**Anything not understood reads every tier.** A conjunct this does not
translate is dropped — dropping one can only include more — and a query whose
shape it does not recognise yields no terms at all.

**Values are what DuckDB compares against, not what the literal says.** The
pruner compares in Python, so each constant is converted the way DuckDB binds
it against that column's type, measured rather than assumed:

- a FLOAT column compares to `0.1` as the FLOAT 0.1, so a float32 value is
  cast to FLOAT here too — `f > 0.1` is false for a stored 0.1;
- a DOUBLE column casts the literal to DOUBLE, so a decimal becomes the double
  DuckDB would make of it;
- an integer column compares to a decimal exactly (`2**53 + 1 = 2**53.0` is
  false), which Python's `int`-against-`Decimal` comparison also does — so
  integers and decimals are kept as they are, and a DOUBLE literal, which
  DuckDB would compare in floating point, is dropped.
"""

from __future__ import annotations

import json
import threading
from collections import OrderedDict
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import duckdb
import pyarrow as pa

from litelink.manifest import PRUNABLE, Term

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

# A comparison with its operands swapped: `K < col` is `col > K`.
_MIRROR = {
    "COMPARE_GREATERTHAN": "COMPARE_LESSTHAN",
    "COMPARE_GREATERTHANOREQUALTO": "COMPARE_LESSTHANOREQUALTO",
    "COMPARE_LESSTHAN": "COMPARE_GREATERTHAN",
    "COMPARE_LESSTHANOREQUALTO": "COMPARE_GREATERTHANOREQUALTO",
    "COMPARE_EQUAL": "COMPARE_EQUAL",
}

_OPERATOR = {
    "COMPARE_GREATERTHAN": ">",
    "COMPARE_GREATERTHANOREQUALTO": ">=",
    "COMPARE_LESSTHAN": "<",
    "COMPARE_LESSTHANOREQUALTO": "<=",
    "COMPARE_EQUAL": "==",
}

# A query's terms depend only on its text and the columns it can name, and a
# polling reader sends the same text over and over — so it is parsed once.
_CACHED = 256
_TERMS: OrderedDict[tuple[str, tuple[tuple[str, str], ...]], tuple[Term, ...]] = (
    OrderedDict()
)
_CACHE_LOCK = threading.Lock()


def terms(
    connection: duckdb.DuckDBPyConnection, query: str, schema: pa.Schema
) -> tuple[Term, ...]:
    """The terms `query`'s WHERE implies, over `schema`'s prunable columns.

    Empty when nothing narrows anything, which `prune` reads as "every tier
    could match".

    **Narrowed only for one shape: a single SELECT over `log` alone.** No
    joins, CTEs, set operations or subqueries, because a skipped tier is
    skipped for the `log` view itself — every reference to it sees the same
    rows, so a second reference with a different predicate, or none, would
    miss rows it needed. Only the top-level WHERE is read, and only its AND-ed
    conjuncts: an OR, a NOT, or a comparison against anything other than a
    constant drops out and constrains nothing.

    Select-list aliases cannot capture a WHERE column: DuckDB binds a name in
    WHERE to the table's column before an alias, which was checked rather
    than assumed.
    """
    # A name that collides with another once lowercased is left out: DuckDB
    # resolves names case-insensitively, and a reference could mean either.
    lowered = [name.lower() for name in schema.names]
    kinds = {
        field.name.lower(): (field.name, str(field.type))
        for field in schema
        if field.type in PRUNABLE and lowered.count(field.name.lower()) == 1
    }

    key = (query, tuple(sorted((low, kind[1]) for low, kind in kinds.items())))
    with _CACHE_LOCK:
        if key in _TERMS:
            _TERMS.move_to_end(key)
            return _TERMS[key]

    found = _terms(connection, query, schema, kinds)
    with _CACHE_LOCK:
        _TERMS[key] = found
        while len(_TERMS) > _CACHED:
            _TERMS.popitem(last=False)

    return found


def _terms(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    schema: pa.Schema,
    kinds: Mapping[str, tuple[str, str]],
) -> tuple[Term, ...]:
    """`terms`, uncached."""
    try:
        row = connection.execute(
            f"SELECT json_serialize_sql({_quoted(query)})"
        ).fetchone()
    except duckdb.Error:
        return ()

    if row is None:
        return ()

    parsed: dict[str, Any] = json.loads(str(row[0]))
    if parsed.get("error") or len(parsed.get("statements", ())) != 1:
        return ()

    node = parsed["statements"][0]["node"]
    if node.get("type") != "SELECT_NODE" or node.get("cte_map", {}).get("map"):
        return ()

    source = node.get("from_table") or {}
    if (
        source.get("type") != "BASE_TABLE"
        or str(source.get("table_name", "")).lower() != "log"
        or source.get("schema_name")
        or source.get("catalog_name")
    ):
        return ()

    if any(_is_subquery(item) for item in _walk(node)):
        return ()

    where = node.get("where_clause")
    if where is None:
        return ()

    qualifiers = {"log"}
    if source.get("alias"):
        qualifiers.add(str(source["alias"]).lower())

    types = {field.name: field.type for field in schema}
    found: list[Term] = []
    for conjunct in _conjuncts(where):
        found.extend(_translate(connection, conjunct, kinds, types, qualifiers))

    return tuple(found)


def _translate(
    connection: duckdb.DuckDBPyConnection,
    expression: dict[str, Any],
    kinds: Mapping[str, tuple[str, str]],
    types: Mapping[str, pa.DataType],
    qualifiers: set[str],
) -> list[Term]:
    """One conjunct's terms. Empty if it cannot narrow anything."""

    def value(node: dict[str, Any], column: str) -> tuple[bool, object]:
        return _value(connection, node, types[column])

    kind = expression.get("type")
    if expression.get("class") == "COMPARISON" and kind in _MIRROR:
        column = _column(expression["left"], kinds, qualifiers)
        other = expression["right"]
        if column is None:
            column = _column(expression["right"], kinds, qualifiers)
            other = expression["left"]
            kind = _MIRROR[kind]

        if column is None:
            return []

        known, constant = value(other, column)

        return [(column, _OPERATOR[kind], constant)] if known else []

    if expression.get("class") == "BETWEEN" and kind == "COMPARE_BETWEEN":
        column = _column(expression["input"], kinds, qualifiers)
        if column is None:
            return []

        found: list[Term] = []
        low_known, low = value(expression["lower"], column)
        high_known, high = value(expression["upper"], column)
        if low_known:
            found.append((column, ">=", low))

        if high_known:
            found.append((column, "<=", high))

        return found

    if expression.get("class") == "OPERATOR" and kind == "COMPARE_IN":
        children = expression.get("children", [])
        column = _column(children[0], kinds, qualifiers) if children else None
        if column is None:
            return []

        values = [value(child, column) for child in children[1:]]
        if not values or not all(known for known, _ in values):
            return []

        return [(column, "in", [v for _, v in values])]

    return []


def _value(
    connection: duckdb.DuckDBPyConnection,
    expression: dict[str, Any],
    column_type: pa.DataType,
) -> tuple[bool, object]:
    """`(known, value)`: the constant as DuckDB compares it to `column_type`.

    See the module docstring for the rule per type. Unknown when the constant
    is not a literal (or casts of one), when DuckDB cannot evaluate it, or when
    the comparison would not be exact in Python.
    """
    sql = _constant(connection, expression)
    if sql is None:
        return False, None

    if pa.types.is_float32(column_type):
        sql = f"CAST(({sql}) AS FLOAT)"
    elif pa.types.is_float64(column_type):
        sql = f"CAST(({sql}) AS DOUBLE)"

    try:
        row = connection.execute(f"SELECT {sql}").fetchone()
    except duckdb.Error:
        return False, None

    if row is None:
        return False, None

    result = row[0]
    if pa.types.is_floating(column_type):
        return isinstance(result, float), result

    if pa.types.is_boolean(column_type):
        return isinstance(result, bool), result

    # Integers: exact values only, which Python compares as DuckDB does.
    exact = isinstance(result, (int, Decimal)) and not isinstance(result, bool)

    return exact, result


def _quoted(text: str) -> str:
    """A SQL string literal. Inlined rather than bound as a parameter: binding
    a Python value costs DuckDB a round of module lookups per call."""
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


def _column(
    expression: Mapping[str, Any],
    kinds: Mapping[str, tuple[str, str]],
    qualifiers: set[str],
) -> str | None:
    """The column a reference names, as the schema spells it, or None."""
    if expression.get("class") != "COLUMN_REF":
        return None

    names = [str(name).lower() for name in expression.get("column_names", ())]
    if len(names) == 2 and names[0] in qualifiers:
        names = names[1:]

    if len(names) != 1:
        return None

    found = kinds.get(names[0])

    return None if found is None else found[0]


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
