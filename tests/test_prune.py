"""Which tiers a query reads, decided from the log's tier manifest (#90).

Two halves. Turning a query into manifest terms — DuckDB's parse, and each
constant converted the way DuckDB compares it — needs no object storage and is
checked directly. The rest runs against a real archive, because the claims are
about when the network is and is not touched, and about the rows that come
back either way.
"""

from __future__ import annotations

import sqlite3
from datetime import timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import litelink
from litelink._layout import Layout
from litelink._prune import terms
from litelink._read import Reader
from litelink._table import LogTable
from litelink._tiers import ARCHIVE, LOCAL, TierManifest
from litelink.log import OFFSET
from litelink.manifest import build, prune
from tests.test_archive import ROWS, archived_log, rows
from tests.test_manifest import row

if TYPE_CHECKING:
    from pathlib import Path

    from litelink import WriteHandle
    from litelink._s3 import S3Options

SCHEMA = pa.schema(
    [
        pa.field(OFFSET, pa.int64()),
        pa.field("side", pa.int32()),
        pa.field("price", pa.float64()),
        pa.field("ratio", pa.float32()),
        pa.field("flag", pa.bool_()),
        pa.field("sym", pa.string()),
    ]
)


def of(where: str, query: str = "SELECT * FROM log WHERE {}") -> tuple:
    return terms(duckdb.connect(), query.format(where), SCHEMA)


def test_comparisons_become_terms() -> None:
    assert of("side > 1 AND price <= 2.5") == (("side", ">", 1), ("price", "<=", 2.5))
    assert of("5 < side") == (("side", ">", 5),)
    assert of("side BETWEEN 1 AND 3") == (("side", ">=", 1), ("side", "<=", 3))
    assert of("side IN (1, 3)") == (("side", "in", [1, 3]),)
    assert of('"litelink_offset" >= 10') == ((OFFSET, ">=", 10),)
    assert of("l.side = 2", "SELECT * FROM log l WHERE {}") == (("side", "==", 2),)
    assert of("flag = true") == (("flag", "==", True),)


def test_an_integer_against_a_decimal_is_kept_exact() -> None:
    assert of("side > 5.5") == (("side", ">", Decimal("5.5")),)


@pytest.mark.parametrize(
    "where",
    [
        "side > 1 OR price > 2",  # an OR is not a conjunction of tests
        "sym = 'x'",  # strings keep no usable bound
        "side > 1.5::DOUBLE",  # a DOUBLE: DuckDB compares in floating point
        "side > '3'",  # nor a string an integer column casts
        "price > epoch(now())",  # evaluated a moment before the query
        "side > price",  # not a constant
    ],
)
def test_what_cannot_be_decided_exactly_is_dropped(where: str) -> None:
    assert of(where) == ()


@pytest.mark.parametrize(
    "query",
    [
        # A second reference to `log` would see the tiers skipped for the first.
        "SELECT * FROM log WHERE price > (SELECT max(price) FROM log)",
        "SELECT * FROM log a JOIN log b USING (side) WHERE a.price > 100",
        "SELECT (SELECT count(*) FROM log) FROM log WHERE price > 100",
        "WITH t AS (SELECT * FROM log) SELECT * FROM t WHERE price > 100",
        "SELECT * FROM log WHERE price > 100 UNION ALL SELECT * FROM log",
        "SELECT * FROM log; SELECT * FROM log WHERE price > 100",
        "not sql at all",
    ],
)
def test_only_a_plain_select_over_log_is_narrowed(query: str) -> None:
    """Every other shape reads every tier — correct, and only slower."""
    assert terms(duckdb.connect(), query, SCHEMA) == ()


@pytest.mark.parametrize(
    ("column", "where", "stored"),
    [
        # FLOAT against 0.1 compares as FLOAT: a stored 0.1f is not > 0.1.
        ("ratio", "ratio > 0.1", 0.1),
        ("ratio", "ratio <= 0.1", 0.1),
        # DOUBLE against a decimal compares as DOUBLE.
        ("price", "price <= 0.1", 0.1),
        ("price", "price > 0.1", 0.1),
        # An integer against a decimal compares exactly.
        ("side", "side > 5.5", 6),
        ("side", "side < 5.5", 6),
        ("side", "side = 2", 2),
    ],
)
def test_a_value_is_what_duckdb_compares_against(
    column: str, where: str, stored: object
) -> None:
    """The pruner compares in Python, so it must agree with DuckDB exactly.

    One stored value, as both its min and max: the unit is kept exactly when
    DuckDB returns the row.

    Falsify by dropping the CAST to FLOAT in `_prune._value`: `ratio <= 0.1`
    prunes a unit DuckDB returns a row from.
    """
    table = pa.table({column: pa.array([stored], type=SCHEMA.field(column).type)})
    connection = duckdb.connect()
    connection.register("arrow_log", table)
    connection.execute("CREATE TABLE log AS SELECT * FROM arrow_log")
    returned = connection.execute(f"SELECT count(*) FROM log WHERE {where}").fetchone()
    assert returned is not None

    found = terms(duckdb.connect(), f"SELECT * FROM log WHERE {where}", SCHEMA)
    assert found, "the case must narrow"
    kept = prune(build([row("unit", 1, table)]), ["unit"], found)

    assert (kept == ["unit"]) == bool(returned[0]), (found, returned)


def test_the_manifest_is_a_parquet_file_beside_the_log(tmp_path: Path) -> None:
    """`<name>.manifest.parquet`, readable by anything that reads Parquet."""
    schema = pa.schema([pa.field("x", pa.int64())])
    with litelink.new(tmp_path, "trades", schema=schema) as log:
        log.extend({"x": i} for i in range(10))
        log.seal()

    table = pq.read_table(tmp_path / "trades" / "trades.manifest.parquet")
    found = {r["tier"]: r for r in table.to_pylist()}

    assert found[LOCAL]["x"]["min"] == 0
    assert found[LOCAL]["x"]["max"] == 9
    assert found[ARCHIVE]["record_count"] == 0, "no archive: known empty"


# -- against a real archive ---------------------------------------------------


def evicted(tmp_path: Path, bucket: str, s3: S3Options) -> WriteHandle:
    """A log whose archive holds every row and whose local table the last ~1000.

    `event_ts` is the row's position, so `event_ts = offset - 1` and a
    predicate on either lands in a known tier.
    """
    log = archived_log(
        tmp_path, bucket, s3, local_retention=timedelta(0), local_rows=1000
    )
    log.extend(rows(ROWS))
    log.seal()
    log.sync(push_unsettled=True)
    log.maintain()
    extent = log.table_extent()
    assert extent is not None, "the fixture must keep part of the log local"
    assert 1 < extent[0] < ROWS, "the fixture must evict part of the log"

    return log


class Remote:
    """Counts archive resolutions, or refuses them."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, *, refuse: bool) -> None:
        self.calls = 0
        original = Reader._prepare_remote  # noqa: SLF001

        def counted(reader: Reader, cursor: duckdb.DuckDBPyConnection):  # noqa: ANN202
            self.calls += 1
            if refuse:
                msg = "this read must not touch the archive"
                raise AssertionError(msg)

            return original(reader, cursor)

        monkeypatch.setattr(Reader, "_prepare_remote", counted)


def rows_of(tmp_path: Path) -> dict[str, dict]:
    table = TierManifest(Layout(tmp_path, "s")).load()
    assert table is not None

    return {r["tier"]: r for r in table.to_pylist()}


@pytest.mark.s3
def test_a_read_inside_the_local_window_never_touches_the_archive(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """I5 under per-query selection: bounded above what eviction took, local.

    The archive holds every row, so each of these COULD be answered from it —
    what keeps them local is the archive's tier row, which describes only what
    eviction moved there. Refused rather than counted, so a regression fails
    at the read that reached out.

    Falsify by returning `(True, True)` from `Reader._tiers`: every read here
    raises.
    """
    with evicted(tmp_path, bucket, s3) as log:
        extent = log.table_extent()
        assert extent is not None
        low = extent[0]
        Remote(monkeypatch, refuse=True)

        assert log.scan(start_offset=low).read_all().num_rows == ROWS - low + 1
        # The sort key, which is what a caller actually bounds on (§7).
        assert (
            log.scan(where=f"event_ts >= {low - 1}").read_all().num_rows
            == ROWS - low + 1
        )
        assert log.sql(
            f'SELECT count(*) FROM log WHERE "litelink_offset" > {ROWS - 5}'
        ).read_all().column(0).to_pylist() == [5]
        # Nothing matches anywhere: the archive cannot, so it is not asked.
        assert log.scan(where="event_ts > 1000000").read_all().num_rows == 0


@pytest.mark.s3
def test_a_read_reaching_below_the_local_window_reads_the_archive(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The other half: history is read without the caller naming a tier.

    Under 0.4.0 these returned the local rows alone unless the handle had been
    built with `include_archive=True`.
    """
    with evicted(tmp_path, bucket, s3) as log:
        remote = Remote(monkeypatch, refuse=False)

        assert log.scan().read_all().num_rows == ROWS
        assert log.scan(where="event_ts < 10").read_all().num_rows == 10
        assert remote.calls == 2


@pytest.mark.s3
def test_a_pruned_read_returns_what_reading_every_tier_returns(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pruning may only ever skip a tier that contributes nothing.

    Each query run twice — as decided, and with every tier forced in — and the
    answers compared. The queries straddle the local boundary, sit wholly on
    either side of it, and include the shapes that must not be narrowed at
    all: a subquery and a self-join over `log` would see the pruned rows too.

    Falsify by swapping `low` and `high` in `manifest._compare`'s `<` and `>`
    rules: reads bounded on one side of the local window skip the tier on the
    other and come back short. Or by dropping the subquery check in
    `_prune._terms`: the scalar subquery counts only the local rows.
    """
    with evicted(tmp_path, bucket, s3) as log:
        extent = log.table_extent()
        assert extent is not None
        edge = extent[0] - 1  # `event_ts` of the first local row
        queries = [
            "SELECT count(*) FROM log",
            f"SELECT count(*) FROM log WHERE event_ts < {edge}",
            f"SELECT count(*) FROM log WHERE event_ts <= {edge}",
            f"SELECT count(*) FROM log WHERE event_ts >= {edge}",
            f"SELECT count(*) FROM log WHERE event_ts BETWEEN {edge - 5} AND {edge + 5}",
            f"SELECT count(*) FROM log WHERE event_ts IN (3, {edge}, {ROWS - 1})",
            "SELECT count(*) FROM log WHERE key = 'k3' AND event_ts < 100",
            f"SELECT count(*) FROM log WHERE event_ts < 5 OR event_ts > {ROWS - 5}",
            'SELECT count(*) FROM log WHERE "litelink_offset" = 1',
            f"SELECT count(*) FROM log WHERE event_ts > 5.5 AND event_ts < {edge}.5",
            # The outer WHERE narrows to the local window; the subquery must
            # still count the whole log.
            (
                "SELECT (SELECT count(*) FROM log) FROM log"
                f" WHERE event_ts > {edge} LIMIT 1"
            ),
            (
                "SELECT count(*) FROM log a JOIN log b ON a.event_ts = b.event_ts"
                f" WHERE a.event_ts > {edge}"
            ),
        ]
        decided = [log.sql(q).read_all().column(0).to_pylist() for q in queries]

        monkeypatch.setattr(Reader, "_tiers", lambda *_: (True, True))
        forced = [log.sql(q).read_all().column(0).to_pylist() for q in queries]

    assert decided == forced
    assert decided[0] == [ROWS], "the fixture must serve the whole log"


@pytest.mark.s3
def test_the_tier_rows_describe_what_each_tier_holds(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """After sync and eviction: the local row covers the local table, and the
    archive row covers what eviction moved below it — not the whole archive.

    Falsify by removing the `widen(ARCHIVE, …)` in `Maintenance.evict`: the
    archive row never grows past what the first sync computed.
    """
    with evicted(tmp_path, bucket, s3) as log:
        extent = log.table_extent()
        assert extent is not None
        found = rows_of(tmp_path)

        archive = found[ARCHIVE]
        assert archive[OFFSET]["min"] == 1
        assert archive[OFFSET]["max"] >= extent[0] - 1, "covers every evicted row"
        assert archive["event_ts"]["max"] < ROWS - 1, "not the archive's local copies"

        local = found[LOCAL]
        assert local[OFFSET]["min"] <= extent[0]
        assert local[OFFSET]["max"] >= extent[1]


@pytest.mark.s3
def test_every_register_widens_the_local_row_before_it_commits(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The ordering the local row's soundness rests on.

    At the moment each register commits, the local row must already cover the
    file — a reader that resolves the table just after would otherwise skip
    the local tier for rows only it holds.

    Falsify by moving the `before_add` call in `LogTable.register` after
    `self._commit(add)`.
    """
    covered: list[int] = []
    original = LogTable._commit  # noqa: SLF001

    def commit(table: LogTable, operation):  # noqa: ANN001, ANN202
        if table.before_add is not None:
            covered.append(rows_of(tmp_path)[LOCAL][OFFSET]["max"] or 0)

        return original(table, operation)

    with archived_log(tmp_path, bucket, s3) as log:
        monkeypatch.setattr(LogTable, "_commit", commit)
        for step in range(3):
            log.extend(rows(ROWS // 4))
            log.seal()
            committed = log.table_extent()
            assert committed is not None
            assert covered, "no local commit was observed"
            assert covered[-1] >= committed[1], (
                f"seal {step} committed ahead of its row"
            )


@pytest.mark.s3
def test_a_log_without_a_manifest_reads_every_tier_until_backfilled(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A log written before the manifest existed: correct at once, local after
    the writer's next `open`.

    Forged by deleting the file. A reader cannot trust what is not there, so
    it reads the archive for every query; the writer's `open` backfills from
    the manifests, and the same hot read is local again.

    Falsify by removing `_backfill_manifest` from `litelink.open`: the last
    read reaches the archive.
    """
    with evicted(tmp_path, bucket, s3) as log:
        extent = log.table_extent()
        assert extent is not None
        low = extent[0]

    manifest = TierManifest(Layout(tmp_path, "s"))
    manifest.path.unlink()

    remote = Remote(monkeypatch, refuse=False)
    with litelink.open(tmp_path, "s", read_only=True, s3=s3) as reader:
        assert reader.scan(start_offset=low).read_all().num_rows == ROWS - low + 1
        assert remote.calls == 1, "no manifest must read the archive"

    with litelink.open(tmp_path, "s", s3=s3) as writer:
        assert manifest.has(LOCAL)
        assert manifest.has(ARCHIVE)
        assert writer.scan(start_offset=low).read_all().num_rows == ROWS - low + 1
        assert remote.calls == 1, "backfilled, the hot read is local again"


@pytest.mark.s3
def test_repointing_forgets_the_archive_row(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A row describes one archive; a move must not carry it to another.

    Pointed at a fresh prefix, the row is recomputed there: nothing below the
    local table. Pointed back, the original's again. Pointed at one that
    cannot be read, the row is gone — and reads include that archive.

    Falsify by removing the `drop(ARCHIVE)` in `_repoint`: the unreadable
    prefix keeps the old archive's row.
    """
    with evicted(tmp_path, bucket, s3) as log:
        original = log.archive
        assert original is not None

        before = rows_of(tmp_path)[ARCHIVE]
        assert before["record_count"] > 0

        log.set_archive(f"s3://{bucket}/elsewhere")
        assert rows_of(tmp_path)[ARCHIVE]["record_count"] == 0

        log.set_archive(original)
        assert rows_of(tmp_path)[ARCHIVE][OFFSET] == before[OFFSET]
        assert log.scan().read_all().num_rows == ROWS

        log.set_archive(f"s3://{bucket}-nonexistent/prefix")
        assert ARCHIVE not in rows_of(tmp_path)


@pytest.mark.s3
def test_a_restore_computes_both_rows_from_what_it_rebuilt(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """After a failover the local table is empty and the archive is all of
    history, so the archive row must cover every archived offset.

    Falsify by removing `_backfill_manifest` from `restore`: the rows are
    dropped and nothing computes them.
    """
    where = f"s3://{bucket}/prefix"
    primary = tmp_path / "primary"
    with archived_log(primary, bucket, s3) as log:
        log.extend(rows(ROWS // 2))
        log.seal()
        log.sync(push_unsettled=True)

        second = tmp_path / "second"
        (second / "s").mkdir(parents=True)
        source = sqlite3.connect(Layout(primary, "s").buffer_db)
        copy = sqlite3.connect(Layout(second, "s").buffer_db)
        source.backup(copy)
        source.close()
        copy.close()

        log.extend(
            {"event_ts": ROWS + i, "key": "late", "payload": "z"} for i in range(50)
        )
        log.seal()
        log.sync(push_unsettled=True)
        archived = log.archived_through()

    with litelink.restore(second, "s", archive=where, s3=s3) as revived:
        found = rows_of(second)
        assert found[ARCHIVE][OFFSET]["max"] == archived
        assert found[LOCAL]["record_count"] == 0
        late = revived.scan(where=f"event_ts >= {ROWS}").read_all()
        assert late.num_rows == 50
