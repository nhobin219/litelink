"""Which tiers a query reads, decided from the tier statistics (#90).

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
import pytest

import litelink
from litelink._layout import Layout
from litelink._prune import terms
from litelink._read import Reader
from litelink._tiers import ArchiveTier, Stored
from litelink.log import OFFSET, LogHandle
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


def test_a_log_without_an_archive_knows_the_archive_holds_nothing(
    tmp_path: Path,
) -> None:
    """Known empty from birth, so no query ever includes a leg it cannot read."""
    schema = pa.schema([pa.field("x", pa.int64())])
    with litelink.new(tmp_path, "trades", schema=schema) as log:
        stored = log._tiers.load()  # noqa: SLF001

    assert stored is not None
    assert stored.statistics.record_count == 0
    assert stored.offsets[1] <= stored.offsets[0], "an empty range"


def test_local_statistics_are_of_the_snapshot_a_read_resolved(
    tmp_path: Path,
) -> None:
    """The local row comes from the snapshot being scanned, or not at all.

    A read holding an older pointer gets None — "read the local tier" —
    rather than a newer snapshot's rollup, which could leave out rows the
    older snapshot still serves.

    Falsify by dropping the pointer check in `LogTable.statistics_at`: the
    stale pointer gets the newer rollup.
    """
    schema = pa.schema([pa.field("x", pa.int64())])
    with litelink.new(tmp_path, "trades", schema=schema) as log:
        log.extend({"x": i} for i in range(10))
        log.seal()
        table = log._table  # noqa: SLF001
        table.reload()
        first = table.metadata_location
        before = table.statistics_at(first)
        assert before is not None
        assert before["x"].max == 9

        log.extend({"x": i} for i in range(10, 20))
        log.seal()
        table.reload()

        assert table.statistics_at(first) is None
        after = table.statistics_at(table.metadata_location)
        assert after is not None
        assert after["x"].max == 19


def buffered_log(tmp_path: Path) -> WriteHandle:
    """Offsets 1–100 sealed into the local table, 101–150 still buffered."""
    schema = pa.schema([pa.field("x", pa.int64())])
    log = litelink.new(tmp_path, "s", schema=schema)
    log.extend({"x": i} for i in range(100))
    log.seal()
    log.extend({"x": i} for i in range(100, 150))

    return log


def test_a_read_below_the_buffer_does_not_read_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The buffer is pruned by offset alone: a scan that ends below its lowest
    offset never converts its rows, and still returns every row it should.

    Falsify by returning True from `Reader._buffer_could_match`: the tail is
    read for the scan that cannot use it.
    """
    from litelink._buffer import Buffer

    read: list[int | None] = []
    original = Buffer.rows_above

    def counted(buffer: Buffer, boundary: int | None) -> pa.Table:
        read.append(boundary)
        return original(buffer, boundary)

    with buffered_log(tmp_path) as log:
        monkeypatch.setattr(Buffer, "rows_above", counted)

        below = log.scan(end_offset=51).read_all()
        assert below.column(OFFSET).to_pylist() == list(range(1, 51))
        assert read == [], "the buffer holds nothing below 101"

        into = log.scan(start_offset=120).read_all()
        assert into.column(OFFSET).to_pylist() == list(range(120, 151))
        assert len(read) == 1

        # A term on another column says nothing about offsets: read.
        log.sql("SELECT count(*) FROM log WHERE x < 10").read_all()
        assert len(read) == 2


def test_an_unfiltered_read_skips_the_local_rollup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With no terms nothing but an empty tier can be skipped, and a local table
    with an extent is not empty — so the rollup is not worth computing.

    Falsify by dropping the `and found` from `Reader._tiers`: the unfiltered
    scan computes the rollup too.
    """
    from litelink._table import LogTable

    asked: list[str] = []
    original = LogTable.statistics_at

    def counted(table: LogTable, location: str):  # noqa: ANN202
        asked.append(location)
        return original(table, location)

    with buffered_log(tmp_path) as log:
        # No stored row, so a read that needs the rollup has to compute it.
        with sqlite3.connect(Layout(tmp_path, "s").buffer_db) as forged:
            forged.execute("DELETE FROM tier_statistics WHERE tier = 'local'")

        monkeypatch.setattr(LogTable, "statistics_at", counted)

        assert log.scan().read_all().num_rows == 150
        assert asked == []

        assert log.scan(where="x >= 140").read_all().num_rows == 10
        assert len(asked) == 1


def test_the_committer_stores_the_local_rollup_for_every_other_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """After a seal the local row is in `buffer.db`, stamped with the version it
    describes, and a reader in another handle uses it rather than rolling the
    version up again.

    Falsify by removing the `after_commit` call from `LogTable._commit`: the
    reader has no stored row and rolls the version up itself.
    """
    import litelink._table as table_module

    with buffered_log(tmp_path) as log:
        table = log._table  # noqa: SLF001
        table.reload()
        _, found = log._buffer.tiers()  # noqa: SLF001
        stored = found.get("local")
        assert stored is not None
        assert stored[2] == table.metadata_location, "stamped with its version"
        assert stored[0] == (1, 101)

        with litelink.open(tmp_path, "s", read_only=True) as reader:

            def refuse(*_: object) -> None:
                msg = "the stored rollup should have been used"
                raise AssertionError(msg)

            monkeypatch.setattr(table_module, "rollup", refuse)
            assert reader.scan(where="x >= 140").read_all().num_rows == 10


def test_a_stored_rollup_for_another_version_is_not_used(tmp_path: Path) -> None:
    """The stamp is the guard: a row for a version other than the one a read
    resolved is ignored, and the reader rolls its own version up.

    Falsify by dropping the version comparison in `Reader._tiers`: the forged
    row, claiming the local tier holds nothing matching, skips it and the
    read comes back short.
    """
    from litelink._tiers import empty, encode

    with buffered_log(tmp_path) as log:
        schema = log._buffer.shape().table  # noqa: SLF001
        with sqlite3.connect(Layout(tmp_path, "s").buffer_db) as forged:
            forged.execute(
                "UPDATE tier_statistics SET statistics = ?, version = ?"
                " WHERE tier = 'local'",
                (encode(schema, empty(schema)), "99999-other.metadata.json"),
            )

        assert log.scan(where="x < 10").read_all().num_rows == 10


def test_the_stored_local_rollup_only_moves_forward(tmp_path: Path) -> None:
    """Two commits' stores can land in either order; the older one arriving last
    does not replace the newer.

    Falsify by removing the sequence comparison from
    `Buffer.store_local_statistics`: the older version overwrites the newer.
    """
    with buffered_log(tmp_path) as log:
        buffer = log._buffer  # noqa: SLF001
        newer = "/x/metadata/00012-aaa.metadata.json"
        older = "/x/metadata/00011-bbb.metadata.json"

        assert buffer.store_local_statistics(newer, (1, 5), "{}")
        assert not buffer.store_local_statistics(older, (1, 3), "{}")
        assert buffer.tiers()[1]["local"][2] == newer


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


def archive_row(log: LogHandle) -> Stored | None:
    """The archive's stored tier row — its range and statistics — or None."""
    return ArchiveTier(log._buffer).load()  # noqa: SLF001


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
def test_the_archive_row_describes_what_eviction_moved_below_the_local_table(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Every evicted offset, and not the archive's copy of the local window.

    Falsify by removing the `widen` in `Maintenance.evict`: the archive row
    stays as the first sync computed it, before anything was evicted.
    """
    with evicted(tmp_path, bucket, s3) as log:
        extent = log.table_extent()
        assert extent is not None
        found = archive_row(log)
        assert found is not None

        assert found.offsets[0] == 1
        assert found.offsets[1] >= extent[0], "covers every evicted row"
        assert OFFSET not in found.statistics.columns, "the range is stored apart"
        assert found.statistics["event_ts"].max < ROWS - 1, "not the local copies"


@pytest.mark.s3
def test_a_log_without_an_archive_row_reads_the_archive_until_backfilled(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A log written before the row existed: correct at once, local after the
    writer's next `open`.

    Forged by deleting the row. A reader cannot trust what is not there, so it
    reads the archive for every query; the writer's `open` backfills from the
    archive's manifests, and the same hot read is local again.

    Falsify by removing `_backfill_manifest` from `litelink.open`: the last
    read reaches the archive.
    """
    with evicted(tmp_path, bucket, s3) as log:
        extent = log.table_extent()
        assert extent is not None
        low = extent[0]

    with sqlite3.connect(Layout(tmp_path, "s").buffer_db) as forged:
        forged.execute("DELETE FROM tier_offsets")
        forged.execute("DELETE FROM tier_statistics")

    remote = Remote(monkeypatch, refuse=False)
    with litelink.open(tmp_path, "s", read_only=True, s3=s3) as reader:
        assert reader.scan(start_offset=low).read_all().num_rows == ROWS - low + 1
        assert remote.calls == 1, "no archive row must read the archive"

    with litelink.open(tmp_path, "s", s3=s3) as writer:
        assert writer._tiers.has()  # noqa: SLF001
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

        before = archive_row(log)
        assert before is not None and before.statistics.record_count

        log.set_archive(f"s3://{bucket}/elsewhere")
        fresh = archive_row(log)
        assert fresh is not None and fresh.statistics.record_count == 0

        log.set_archive(original)
        again = archive_row(log)
        assert again is not None and again.offsets == before.offsets
        assert log.scan().read_all().num_rows == ROWS

        log.set_archive(f"s3://{bucket}-nonexistent/prefix")
        assert archive_row(log) is None


@pytest.mark.s3
def test_a_restore_computes_the_archive_row_from_the_archive(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """After a failover the local table is empty and the archive is all of
    history, so the archive row must cover every archived offset — taken from
    the archive, not from the replica's copy, which lags it.

    Falsify by removing `_backfill_manifest` from `restore`: the row is
    dropped and nothing computes it.
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
        found = archive_row(revived)
        assert found is not None
        assert found.offsets[1] == archived + 1
        late = revived.scan(where=f"event_ts >= {ROWS}").read_all()
        assert late.num_rows == 50


@pytest.mark.s3
def test_eviction_widens_the_archive_row_before_it_commits(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The ordering that keeps the archive row from ever lagging the local
    table: at the moment eviction commits, the row already covers what it is
    evicting — so a read resolving the new, higher floor finds it covered.

    Falsify by moving the `widen` in `Maintenance.evict` after
    `evict_through`: at the commit the row still stops below the new floor.
    """
    from litelink._table import LogTable

    seen: list[tuple[int, int | None]] = []
    original = LogTable.evict_through

    def checked(table: LogTable, boundary: int) -> None:
        stored = ArchiveTier(log._buffer).load()  # noqa: SLF001
        seen.append((boundary, None if stored is None else stored.offsets[1]))
        original(table, boundary)

    log = archived_log(
        tmp_path, bucket, s3, local_retention=timedelta(0), local_rows=1000
    )
    with log:
        log.extend(rows(ROWS))
        log.seal()
        log.sync(push_unsettled=True)
        monkeypatch.setattr(LogTable, "evict_through", checked)
        log.maintain()

    assert seen, "the fixture must evict"
    for boundary, covered_until in seen:
        assert covered_until is not None and covered_until > boundary


# -- coverage -------------------------------------------------------------------


def coverage_log(tmp_path: Path, bucket: str, s3: S3Options) -> WriteHandle:
    """Archive below, local table in the middle, a buffered tail on top."""
    log = evicted(tmp_path, bucket, s3)
    log.extend({"event_ts": ROWS + i, "key": "t", "payload": "y"} for i in range(7))

    return log


def covered(coverage: litelink.Coverage) -> list[int]:
    return [
        offset
        for span in (coverage.archive, coverage.local, coverage.buffer)
        if span is not None
        for offset in range(span[0], span[1] + 1)
    ]


@pytest.mark.s3
def test_coverage_partitions_the_log_without_the_network(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Three ranges, each offset in exactly one, and the archive never asked.

    Falsify by reading the archive's own extent for `archive` (its whole
    range, overlapping the local copy): offsets repeat across tiers, and the
    refused archive raises.
    """
    from litelink._archive import Archive

    with coverage_log(tmp_path, bucket, s3) as log:
        extent = log.table_extent()
        assert extent is not None

        def refuse(*_: object) -> None:
            msg = "coverage must not open the archive"
            raise AssertionError(msg)

        monkeypatch.setattr(Archive, "table", refuse)
        coverage = log.coverage()

        assert coverage.archive == (1, extent[0] - 1)
        assert coverage.local == extent
        assert coverage.buffer == (ROWS + 1, ROWS + 7)
        assert covered(coverage) == list(range(1, ROWS + 8)), "each offset once"


@pytest.mark.s3
def test_coverage_reads_the_archive_only_when_no_row_is_stored(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A log not yet backfilled gets the same answer, from the manifests.

    Falsify by returning None for `archive` when there is no stored row: the
    archive's range goes missing, and with it where the log starts.
    """
    with coverage_log(tmp_path, bucket, s3) as log:
        expected = log.coverage()
        with sqlite3.connect(Layout(tmp_path, "s").buffer_db) as forged:
            forged.execute("DELETE FROM tier_offsets WHERE tier = 'archive'")
            forged.execute("DELETE FROM tier_statistics WHERE tier = 'archive'")

        assert log.coverage() == expected


@pytest.mark.s3
def test_coverage_without_the_archive_never_opens_it(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`archive=False` answers a log with no stored archive row from local disk.

    That is the one case the full answer reads the archive's manifests, so a
    replay held to the local floor would otherwise pay — or fail on — S3.

    Falsify by ignoring `archive` in the fallback: the refused archive raises.
    """
    from litelink._archive import Archive

    with coverage_log(tmp_path, bucket, s3) as log:
        expected = log.coverage()
        with sqlite3.connect(Layout(tmp_path, "s").buffer_db) as forged:
            forged.execute("DELETE FROM tier_offsets WHERE tier = 'archive'")
            forged.execute("DELETE FROM tier_statistics WHERE tier = 'archive'")

        def refuse(*_: object) -> None:
            msg = "coverage(archive=False) must not open the archive"
            raise AssertionError(msg)

        monkeypatch.setattr(Archive, "table", refuse)
        coverage = log.coverage(archive=False)

        assert coverage.archive is None, "not asked"
        assert (coverage.local, coverage.buffer) == (expected.local, expected.buffer)


def test_a_stored_local_range_for_another_version_is_not_used(
    tmp_path: Path,
) -> None:
    """The local range is the stamped one only for the version the table is at.

    Falsify by dropping the version comparison in `coverage`: the forged row's
    range is reported.
    """
    with buffered_log(tmp_path) as log:
        with sqlite3.connect(Layout(tmp_path, "s").buffer_db) as forged:
            forged.execute(
                "UPDATE tier_offsets SET start_offset = 500, end_offset = 600"
                " WHERE tier = 'local'"
            )
            forged.execute(
                "UPDATE tier_statistics SET version = '99999-other.metadata.json'"
                " WHERE tier = 'local'"
            )

        coverage = log.coverage()

        assert coverage.local == (1, 100)
        assert coverage.buffer == (101, 150)
        assert coverage.archive is None, "no archive configured"


@pytest.mark.s3
def test_coverage_leaves_rows_a_seal_kept_to_the_local_table(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """With `wal_replication` a seal keeps its rows in the buffer until `sync`;
    they are reported once, as local, and the buffer only above.

    Falsify by reporting the buffer's own extent without clipping it: the
    buffer starts at 1 and every sealed offset is counted twice.
    """
    with archived_log(tmp_path, bucket, s3, wal_replication=True) as log:
        log.extend(rows(100))
        log.seal()
        log.extend({"event_ts": 100 + i, "key": "t", "payload": "y"} for i in range(5))
        assert log.buffered_rows() == 5
        coverage = log.coverage()

        assert coverage.local == (1, 100)
        assert coverage.buffer == (101, 105)
        assert covered(coverage) == list(range(1, 106)), "each offset once"


@pytest.mark.s3
def test_a_read_without_the_archive_stops_at_the_local_floor(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`archive=False` serves the local table and the buffer, and only them —
    from the floor `coverage(archive=False)` reports, with no network.

    The reads here reach below the local table, so without the flag each one
    reads the archive. Falsify by ignoring `archive` in `Reader.query`: the
    refused archive raises.
    """
    with coverage_log(tmp_path, bucket, s3) as log:
        coverage = log.coverage(archive=False)
        assert coverage.local is not None
        assert coverage.buffer is not None
        floor = coverage.local[0]
        assert floor > 1, "the fixture must evict part of the log"
        Remote(monkeypatch, refuse=True)

        scanned = log.scan(archive=False).read_all()[OFFSET].to_pylist()
        assert scanned == list(range(floor, coverage.buffer[1] + 1))

        below = log.scan(end_offset=floor, archive=False).read_all()
        assert below.num_rows == 0, "only the archive holds these"

        counted = log.sql("SELECT count(*) AS n FROM log", archive=False)
        assert counted.read_all()["n"][0].as_py() == len(scanned)
