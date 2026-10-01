"""Manifest pruning: skipping a unit must never skip a match.

Ported from streamcast (streamcast#27), which now uses this implementation. The
one property that matters is checked against DuckDB, the engine a reader
queries with: **every unit `prune` excludes holds no row matching the
predicate.** Including a unit that holds nothing is a wasted scan and passes;
excluding one that holds a match is a wrong answer with no symptom and fails.
The generator covers what a fixed table of cases forgets — NULLs, all-null
columns, and units that lack a column entirely.

**It generates NaN and infinity on purpose.** litelink refuses both (#87), so no
real log holds one; the pruner's NaN rule is a defence, and a defence nothing
exercises is one nobody would notice breaking.
"""

from __future__ import annotations

import math

import duckdb
import pyarrow as pa
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from litelink import manifest
from litelink._statistics import ColumnStatistics, TierStatistics
from litelink.manifest import Row, build, prune

POOL: dict[str, pa.DataType] = {
    "i32": pa.int32(),
    "i64": pa.int64(),
    "f32": pa.float32(),
    "f64": pa.float64(),
    "flag": pa.bool_(),
}


# -- statistics the way `column_statistics` reports them -----------------------


def statistics(table: pa.Table) -> TierStatistics:
    """Min and max over the non-null, non-NaN values, as Iceberg's bounds are.

    Plain Python rather than `pyarrow.compute`, so the stand-in does not share
    pyarrow's own ideas about NaN with the code it checks.
    """
    found = {}
    for name in table.column_names:
        cells = table[name].to_pylist()
        floating = pa.types.is_floating(table[name].type)
        nans = sum(1 for c in cells if isinstance(c, float) and math.isnan(c))
        real = [
            c
            for c in cells
            if c is not None and not (isinstance(c, float) and math.isnan(c))
        ]
        found[name] = ColumnStatistics(
            min=min(real) if real else None,
            max=max(real) if real else None,
            null_count=sum(1 for c in cells if c is None),
            value_count=len(cells),
            nan_count=nans if floating else None,
        )

    return TierStatistics(
        tier=None, record_count=table.num_rows, file_count=1, columns=found
    )


def row(name: str, start: int, table: pa.Table) -> Row:
    return Row(name, start, start + table.num_rows, table.schema, statistics(table))


# -- what DuckDB says ------------------------------------------------------------


def literal(value: object) -> str:
    if value is None:
        return "NULL"

    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"

    if isinstance(value, float):
        if math.isnan(value):
            return "'nan'::DOUBLE"

        if math.isinf(value):
            return "'inf'::DOUBLE" if value > 0 else "'-inf'::DOUBLE"

        return f"{value!r}::DOUBLE"

    return str(value)


def matching(table: pa.Table, terms: list[manifest.Term]) -> int:
    """How many rows of `table` DuckDB says match every term.

    A column the unit lacks is NULL, which is what `UNION ALL BY NAME` makes of
    it — and a NULL matches no comparison.
    """
    clauses = []
    for column, operator, value in terms:
        reference = f'"{column}"' if column in table.column_names else "NULL"
        if operator == "in":
            options = ", ".join(literal(v) for v in value)  # ty: ignore[not-iterable]
            clauses.append(f"{reference} IN ({options})")
        else:
            sql = "<>" if operator == "!=" else operator.replace("==", "=")
            clauses.append(f"{reference} {sql} {literal(value)}")

    # **A native table, which reads every row.** DuckDB compares NaN above
    # every float in any row it reads, but a scan that skips on statistics may
    # never read a NaN row, because those statistics leave NaN out. The native
    # answer is the most inclusive, so it is the one a sound pruner has to
    # agree with.
    connection = duckdb.connect()
    connection.register("arrow_log", table)
    connection.execute("CREATE TABLE log AS SELECT * FROM arrow_log")
    where = " AND ".join(clauses) or "TRUE"
    return connection.sql(f"SELECT count(*) FROM log WHERE {where}").fetchone()[0]  # ty: ignore[not-subscriptable]


# -- generation ------------------------------------------------------------------


def values_for(kind: pa.DataType) -> st.SearchStrategy[object]:
    if kind == pa.bool_():
        return st.booleans()

    if kind == pa.int32():
        return st.integers(-(2**31), 2**31 - 1)

    if kind == pa.int64():
        return st.integers(-(2**63), 2**63 - 1)

    if kind == pa.float32():
        return st.floats(width=32, allow_nan=True, allow_infinity=True)

    return st.floats(allow_nan=True, allow_infinity=True)


def small_values_for(kind: pa.DataType, centre: int = 0) -> st.SearchStrategy[object]:
    """Values that land near each other, so bounds and predicates overlap.

    Tight on purpose: a pruner only gets the chance to be wrong when it
    excludes something, and wide random values almost never let it.
    """
    if kind == pa.bool_():
        return st.booleans()

    if pa.types.is_integer(kind):
        return st.integers(centre - 1, centre + 1)

    return st.one_of(
        st.integers(centre - 1, centre + 1).map(float),
        st.integers(centre - 1, centre + 1).map(float),
        st.sampled_from([math.nan, math.inf, -math.inf, -0.0]),
    )


@st.composite
def column(draw: st.DrawFn, kind: pa.DataType, rows: int, centre: int) -> pa.Array:
    shape = draw(st.sampled_from(["mostly", "mostly", "mostly", "all_null", "wide"]))
    if shape == "all_null":
        return pa.array([None] * rows, type=kind)

    pick = values_for(kind) if shape == "wide" else small_values_for(kind, centre)
    # NULLs present but rare, so the bounds come from real values.
    cell = st.one_of(pick, pick, pick, pick, st.none())
    return pa.array(draw(st.lists(cell, min_size=rows, max_size=rows)), type=kind)


@st.composite
def scenarios(draw: st.DrawFn) -> tuple[list[pa.Table], list[manifest.Term]]:
    """Units and a predicate drawn together, so terms mostly name real columns."""
    tables = []
    centres = []
    for _ in range(draw(st.integers(1, 4))):
        names = draw(st.lists(st.sampled_from(list(POOL)), min_size=1, unique=True))
        rows = draw(st.integers(0, 8))
        # Each unit in its own narrow band, as tiers are — different offsets,
        # different hours — so bounds differ and predicates exclude.
        centre = draw(st.integers(-6, 6))
        centres.append(centre)
        tables.append(pa.table({n: draw(column(POOL[n], rows, centre)) for n in names}))

    present = sorted({n for t in tables for n in t.column_names})
    predicate: list[manifest.Term] = []
    for _ in range(draw(st.integers(1, 3))):
        name = draw(st.sampled_from([*present, *present, *present, "absent"]))
        kind = POOL.get(name, pa.int64())
        operator = draw(st.sampled_from([*sorted(manifest.OPERATORS), "!="]))

        # A value inside some unit's band is where a bound compared the wrong
        # way excludes a match; one just past the edge of a band is where a
        # max that ignores NaN does. Each `in` value draws its own place, so a
        # list can straddle a unit's bounds.
        def near() -> int:
            centre = draw(st.sampled_from(centres))
            return draw(
                st.sampled_from([centre, centre, centre - 2, centre + 2])
                if draw(st.integers(0, 3))
                else st.integers(-6, 6)
            )

        def value_for(kind: pa.DataType = kind) -> object:
            if draw(st.integers(0, 6)) == 0:
                return None

            return draw(small_values_for(kind, near()))

        value: object = (
            [value_for() for _ in range(draw(st.integers(1, 3)))]
            if operator == "in"
            else value_for()
        )
        predicate.append((name, operator, value))

    return tables, predicate


# -- the property ----------------------------------------------------------------


@settings(
    max_examples=600,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
@given(scenario=scenarios())
def test_an_excluded_unit_never_holds_a_match(scenario):
    tables, predicate = scenario
    rows = []
    start = 1
    for index, table in enumerate(tables):
        rows.append(row(f"unit{index}", start, table))
        start += max(table.num_rows, 1)

    kept = set(prune(build(rows), [r.name for r in rows], predicate))

    for unit, table in zip(rows, tables, strict=True):
        if unit.name not in kept:
            assert matching(table, predicate) == 0, (
                f"{unit.name} was pruned but holds a match for {predicate}"
            )


# -- the rules, one at a time -----------------------------------------------------


def one(table: pa.Table, name: str = "unit0") -> Row:
    return row(name, 1, table)


class TestItPrunes:
    def test_bounds_that_cannot_match_exclude_the_unit(self):
        cheap = pa.table({"price": pa.array([1.0, 5.0, 9.0])})
        dear = pa.table({"price": pa.array([100.0, 150.0])})
        table = build([one(cheap, "cheap"), one(dear, "dear")])

        assert prune(table, ["cheap", "dear"], [("price", ">", 50.0)]) == ["dear"]
        assert prune(table, ["cheap", "dear"], [("price", "<", 50.0)]) == ["cheap"]
        assert prune(table, ["cheap", "dear"], [("price", "==", 5.0)]) == ["cheap"]
        assert prune(table, ["cheap", "dear"], [("price", "in", [7.0, 120.0])]) == [
            "cheap",
            "dear",
        ]

    def test_terms_are_anded(self):
        table = pa.table({"a": pa.array([1, 2]), "b": pa.array([10, 20])})

        assert (
            prune(build([one(table)]), ["unit0"], [("a", "==", 1), ("b", "==", 99)])
            == []
        )

    def test_an_empty_unit_is_excluded_whatever_the_terms(self):
        """A unit known to hold no rows holds no match — the one rule this has
        that streamcast's did not, for a tier emptied by eviction."""
        empty = pa.table({"x": pa.array([], type=pa.int64())})
        full = pa.table({"x": pa.array([1])})
        table = build([one(empty, "empty"), one(full, "full")])

        assert prune(table, ["empty", "full"], []) == ["full"]
        assert prune(table, ["empty", "full"], [("x", "==", 1)]) == ["full"]


class TestItIncludesWhatItCannotDecide:
    def test_a_float_column_with_a_nan_is_never_pruned(self):
        """Iceberg's max excludes NaN; DuckDB says NaN > 5."""
        table = pa.table({"x": pa.array([1.0, math.nan])})

        assert matching(table, [("x", ">", 5.0)]) == 1
        assert prune(build([one(table)]), ["unit0"], [("x", ">", 5.0)]) == ["unit0"]

    def test_an_unknown_nan_count_is_a_maybe(self):
        """What every pyiceberg-written file reports: `nan_value_count` None."""
        stats = TierStatistics(
            tier=None,
            record_count=2,
            file_count=1,
            columns={"x": ColumnStatistics(1.0, 2.0, 0, 2, nan_count=None)},
        )
        table = pa.table({"x": pa.array([1.0, 2.0])})
        manifest_ = build([Row("unit0", 1, 3, table.schema, stats)])

        assert prune(manifest_, ["unit0"], [("x", ">", 50.0)]) == ["unit0"]

    def test_an_all_null_column_has_no_bounds_to_prune_on(self):
        table = pa.table({"x": pa.array([None, None], type=pa.int64())})

        assert prune(build([one(table)]), ["unit0"], [("x", "==", 3)]) == ["unit0"]

    def test_a_listed_unit_with_no_row_is_included(self):
        table = build([one(pa.table({"x": pa.array([1])}), "known")])

        assert prune(table, ["known", "unknown"], [("x", "==", 99)]) == ["unknown"]

    def test_a_unit_lacking_the_column_is_included(self):
        with_x = pa.table({"x": pa.array([1])})
        without = pa.table({"y": pa.array([1])})
        table = build([one(with_x, "with"), one(without, "without")])

        assert prune(table, ["with", "without"], [("x", "==", 99)]) == ["without"]

    def test_an_unknown_column_operator_or_value_includes(self):
        table = build([one(pa.table({"x": pa.array([1, 2])}))])

        assert prune(table, ["unit0"], [("nope", "==", 99)]) == ["unit0"]
        assert prune(table, ["unit0"], [("x", "!=", 1)]) == ["unit0"]
        assert prune(table, ["unit0"], [("x", "==", None)]) == ["unit0"]
        assert prune(table, ["unit0"], [("x", "==", "a string")]) == ["unit0"]
        assert prune(table, ["unit0"], [("x", "in", 3)]) == ["unit0"]

    def test_no_manifest_includes_every_unit(self):
        assert prune(None, ["a", "b"], [("x", "==", 1)]) == ["a", "b"]


class TestTheReadersListIsTheAuthority:
    def test_a_row_for_a_unit_the_reader_does_not_list_is_ignored(self):
        table = build(
            [
                one(pa.table({"x": pa.array([1])}), "a"),
                one(pa.table({"x": pa.array([1])}), "b"),
            ]
        )

        assert prune(table, ["a"], []) == ["a"]

    def test_the_order_is_the_readers(self):
        table = build(
            [
                one(pa.table({"x": pa.array([1])}), "a"),
                one(pa.table({"x": pa.array([1])}), "b"),
            ]
        )

        assert prune(table, ["b", "a"], []) == ["b", "a"]


class TestTheTable:
    def test_only_prunable_columns_get_statistics(self):
        table = build([one(pa.table({"x": pa.array([1]), "tag": pa.array(["a"])}))])

        assert "x" in table.column_names
        assert "tag" not in table.column_names, "string bounds are truncated"

    def test_the_key_column_is_the_callers(self):
        """`tier` for a log's tiers, `log` for a stream's sealed logs."""
        unit = one(pa.table({"x": pa.array([1])}), "trades")
        table = build([unit], key="log")

        assert table.column_names[0] == "log"
        assert prune(table, ["trades"], [("x", "==", 9)], key="log") == []

    def test_extend_replaces_a_units_row(self):
        first = one(pa.table({"x": pa.array([1])}), "a")
        again = one(pa.table({"x": pa.array([7])}), "a")
        table = manifest.extend(manifest.extend(None, first), again)

        assert table["tier"].to_pylist() == ["a"]
        assert prune(table, ["a"], [("x", "==", 1)]) == []


class TestTheOffsetIsTheUnitsRange:
    """`litelink_offset` is judged against `[start_offset, end_offset)`."""

    def unit(self, name: str, start: int, end: int | None) -> Row:
        empty = TierStatistics(tier=None, record_count=None, file_count=0, columns={})
        return Row(name, start, end, pa.schema([pa.field("x", pa.int64())]), empty)

    def test_a_unit_is_skipped_by_its_range_alone(self):
        """No statistics at all, and still skipped — the buffer, a live log.

        Falsify by routing offset terms through `_may_match`: with no struct
        for the offset column, every unit is kept.
        """
        table = build([self.unit("old", 1, 101), self.unit("new", 101, 201)])

        assert prune(table, ["old", "new"], [("litelink_offset", "<", 101)]) == ["old"]
        assert prune(table, ["old", "new"], [("litelink_offset", ">=", 101)]) == ["new"]
        assert prune(table, ["old", "new"], [("litelink_offset", "==", 100)]) == ["old"]
        assert prune(table, ["old", "new"], [("litelink_offset", "in", [5, 150])]) == [
            "old",
            "new",
        ]

    def test_the_end_is_exclusive(self):
        table = build([self.unit("u", 1, 101)])

        assert prune(table, ["u"], [("litelink_offset", ">=", 101)]) == []
        assert prune(table, ["u"], [("litelink_offset", ">=", 100)]) == ["u"]

    def test_a_unit_with_no_end_is_never_skipped_from_above(self):
        """None is a unit still growing — the buffer, a live log."""
        table = build([self.unit("buffer", 500, None)])

        assert prune(table, ["buffer"], [("litelink_offset", ">", 10**12)]) == [
            "buffer"
        ]
        assert prune(table, ["buffer"], [("litelink_offset", "<", 500)]) == []

    def test_an_empty_range_holds_nothing(self):
        table = build([self.unit("u", 7, 7)])

        assert prune(table, ["u"], [("litelink_offset", "<", 100)]) == []

    def test_the_offset_is_not_a_statistics_column(self):
        schema = pa.schema(
            [pa.field("litelink_offset", pa.int64()), pa.field("x", pa.int64())]
        )
        table = build([Row("u", 1, 3, schema, statistics(pa.table({"x": [1, 2]})))])

        assert "litelink_offset" not in table.column_names
