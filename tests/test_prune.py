"""Which tiers a query reads, decided from bounds kept locally (#90).

Two halves. The condition itself — DuckDB's parse of a query turned into a test
on per-file bounds — needs no object storage and is checked directly. The rest
runs against a real archive, because the claims are about when the network is
and is not touched, and about the rows that come back either way.
"""

from __future__ import annotations

import sqlite3
from datetime import timedelta
from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa
import pytest

import litelink
from litelink._buffer import ARCHIVE_BOUNDS_FOR, Buffer
from litelink._layout import Layout
from litelink._prune import (
    BOUNDS_REL,
    condition,
    integer_columns,
    relation,
    stats_columns,
)
from litelink._read import Reader
from litelink.log import OFFSET
from tests.test_archive import ROWS, archived_log, rows

if TYPE_CHECKING:
    from pathlib import Path

    from litelink import WriteHandle
    from litelink._s3 import S3Options

BOUNDED = pa.schema(
    [
        pa.field(OFFSET, pa.int64()),
        pa.field("side", pa.int32()),
        pa.field("price", pa.float64()),
        pa.field("sym", pa.string()),
    ]
)


def could_match(query: str, *stored: str) -> list[bool]:
    """Per stored file, whether `query` could match a row of it."""
    connection = duckdb.connect()
    narrowed = condition(
        connection, query, stats_columns(BOUNDED), integer_columns(BOUNDED)
    )
    if narrowed is None:
        return [True] * len(stored)

    connection.register(BOUNDS_REL, relation(BOUNDED, stored))

    return [
        bool(v)
        for (v,) in connection.execute(
            f"SELECT {narrowed.sql()} FROM {BOUNDS_REL}"
        ).fetchall()
    ]


def test_a_constant_binds_the_way_the_query_binds_it() -> None:
    """`side > 5.5` against an int column whose maximum is 6 could match.

    DuckDB compares an integer column to 5.5 as a DOUBLE. Casting the constant
    to the column's type instead rounds it to 6, and `6 > 6` is false — a file
    holding `side = 6` would then be skipped by a query that matches it.

    Falsify by rendering the constant as `CAST((K) AS <column type>)` in
    `_prune._test`: the first file reads as unable to match.
    """
    files = ('{"side": [0, 6]}', '{"side": [0, 5]}')

    assert could_match("SELECT * FROM log WHERE side > 5.5", *files) == [True, False]
    assert could_match("SELECT * FROM log WHERE 5.5 < side", *files) == [True, False]
    assert could_match("SELECT * FROM log WHERE side >= 6", *files) == [True, False]
    assert could_match("SELECT * FROM log WHERE side = 6", *files) == [True, False]


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("SELECT * FROM log WHERE price > 9.3", [False, True]),
        ("SELECT * FROM log WHERE price < 1.5", [False, True]),
        ("SELECT * FROM log WHERE price <= 1.5", [True, True]),
        ("SELECT * FROM log WHERE price BETWEEN 9.3 AND 10", [False, True]),
        ("SELECT * FROM log WHERE price BETWEEN 2 AND 3", [True, True]),
        ("SELECT * FROM log WHERE price IN (0.5, 12)", [False, True]),
        ("SELECT * FROM log l WHERE l.price > 9.3 AND side = 1", [False, True]),
        ('SELECT * FROM log WHERE "litelink_offset" >= 11', [False, True]),
        # Neither narrows: the string column keeps no bound, and an OR is not
        # a conjunction of tests.
        ("SELECT * FROM log WHERE sym = 'x'", [True, True]),
        ("SELECT * FROM log WHERE price > 9.3 OR side = 0", [True, True]),
    ],
)
def test_each_file_is_tested_on_its_own_bounds(
    query: str, expected: list[bool]
) -> None:
    """The first file is fully bounded; the second knows only its offsets.

    A column with no stored bound is NULL in the relation, and reads as "could
    match" — the second file must never be ruled out by a column it has no
    bound for.
    """
    files = (
        '{"litelink_offset": [1, 10], "price": [1.5, 9.25], "side": [0, 1]}',
        '{"litelink_offset": [11, 20]}',
    )

    assert could_match(query, *files) == expected


@pytest.mark.parametrize(
    "query",
    [
        # A second reference to `log` would see the rows pruned for the first.
        "SELECT * FROM log WHERE price > (SELECT max(price) FROM log)",
        "SELECT * FROM log a JOIN log b USING (side) WHERE a.price > 100",
        "SELECT (SELECT count(*) FROM log) FROM log WHERE price > 100",
        "WITH t AS (SELECT * FROM log) SELECT * FROM t WHERE price > 100",
        "SELECT * FROM log WHERE price > 100 UNION ALL SELECT * FROM log",
        # Evaluated a moment before the query it decides for.
        "SELECT * FROM log WHERE price > epoch(now())",
        "SELECT * FROM log; SELECT * FROM log WHERE price > 100",
        "not sql at all",
    ],
)
def test_only_a_plain_select_over_log_is_narrowed(query: str) -> None:
    """Every other shape reads every tier — correct, and only slower."""
    assert condition(duckdb.connect(), query, stats_columns(BOUNDED)) is None


@pytest.mark.parametrize(
    "where",
    [
        "side > 1",
        "side >= 1",
        "side < 1",
        "1 < side",
        "side = 1",
        "side BETWEEN 1 AND 3",
        "side IN (0, 7)",
        "side > -1 AND litelink_offset < 15",
        "litelink_offset = 20",
        "side > 9999999999",
    ],
)
def test_the_integer_shortcut_agrees_with_duckdb(where: str) -> None:
    """Integer comparisons are decided in pyarrow, everything else in DuckDB.

    The shortcut exists for speed and is only sound if it cannot disagree, so
    each file is decided both ways — one file at a time, so a wrong answer on
    any file shows. The files cover a match, a miss, and bounds that are
    unknown.

    Falsify by filling a NULL term with False in `holds_for_any`: a file with
    no bound for the column is then ruled out, where DuckDB keeps it.
    """
    stored = (
        '{"litelink_offset": [1, 10], "side": [0, 1]}',
        '{"litelink_offset": [11, 20], "side": [5, 9]}',
        '{"litelink_offset": [21, 30]}',
        '{"side": [2, 2]}',
    )
    connection = duckdb.connect()
    narrowed = condition(
        connection,
        f"SELECT * FROM log WHERE {where}",
        stats_columns(BOUNDED),
        integer_columns(BOUNDED),
    )
    assert narrowed is not None
    assert narrowed.exact, "every constant here is an integer literal"

    for one in stored:
        bounds = relation(BOUNDED, [one])
        connection.register(BOUNDS_REL, bounds)
        row = connection.execute(
            f"SELECT {narrowed.sql()} FROM {BOUNDS_REL}"
        ).fetchone()
        assert row is not None
        assert narrowed.holds_for_any(bounds) == bool(row[0]), one


def test_a_cast_or_a_fraction_is_left_to_duckdb() -> None:
    """Only a bare integer literal against an integer column is exact."""
    columns = stats_columns(BOUNDED)
    integers = integer_columns(BOUNDED)
    connection = duckdb.connect()

    def exact(where: str) -> bool:
        narrowed = condition(
            connection, f"SELECT * FROM log WHERE {where}", columns, integers
        )
        assert narrowed is not None
        return narrowed.exact

    assert exact("side > 5")
    assert not exact("side > 5.5")
    assert not exact("side > CAST(5 AS DOUBLE)")
    assert not exact("price > 5"), "an integer against a float column"


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


@pytest.mark.s3
def test_a_read_inside_the_local_window_never_touches_the_archive(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """I5 under per-query selection: bounded above what eviction took, local.

    The archive holds every row, so each of these COULD be answered from it —
    what keeps them local is the stored bounds saying no archived file below
    the local table matches. Refused rather than counted, so a regression
    fails at the read that reached out.

    Falsify by returning True from `Reader._archive_could_match`: every read
    here raises.
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

    Each query run twice — as decided, and with the archive forced in — and
    the answers compared. The queries straddle the local boundary, sit wholly
    on either side of it, and include the shapes that must not be narrowed at
    all: a subquery and a self-join over `log` would see the pruned rows too.

    Falsify by swapping `lo` and `hi` in `_prune._COMPARE`: the reads bounded
    below the local window skip the archive and come back short. Or by
    dropping the subquery check in `_prune.condition`: the scalar subquery
    counts only the local rows.
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

        monkeypatch.setattr(Reader, "_archive_could_match", lambda *_: True)
        forced = [log.sql(q).read_all().column(0).to_pylist() for q in queries]

    assert decided == forced
    assert decided[0] == [ROWS], "the fixture must serve the whole log"


@pytest.mark.s3
def test_every_file_the_archive_holds_has_stored_bounds(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """After any number of pushes, the stored rows are the archive's files.

    Staged before each register, so a file the archive holds without a row —
    the state that loses rows from a pruned read — cannot arise from a crash
    between the two. Checked against the archive's own manifests.

    Falsify by deleting the `stage_archive_bounds` call in `_push`: the files
    of every push after the first are missing.
    """
    with archived_log(tmp_path, bucket, s3) as log:
        for _ in range(3):
            log.extend(rows(ROWS // 4))
            log.seal()
            log.sync(push_unsettled=True)

        archive = log._archive.require()  # noqa: SLF001
        archive.reload()
        held = {f.file_path for f in archive.live_files()[1]}
        stored = {
            path
            for (path,) in log._buffer._con.execute(  # noqa: SLF001
                "SELECT rel_path FROM archive_bounds"
            )
        }
        complete_for = log._buffer.archive_bounds_version()[0]  # noqa: SLF001

        assert len(held) > 3, "the fixture must push several files"
        assert held <= stored
        assert complete_for == log.archive


@pytest.mark.s3
def test_a_log_without_stored_bounds_reads_the_archive_until_backfilled(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A log written before bounds existed: correct at once, local after `open`.

    Forged by deleting what an older build never wrote. A reader cannot trust
    what is not there, so it reads the archive for every query; the writer's
    next `open` backfills from the manifests, and the same hot read is local
    again.

    Falsify by removing `_backfill_archive_bounds` from `WriteHandle.open`:
    the last read reaches the archive.
    """
    with evicted(tmp_path, bucket, s3) as log:
        extent = log.table_extent()
        assert extent is not None
        low = extent[0]

    buffer_db = Layout(tmp_path, "s").buffer_db
    with sqlite3.connect(buffer_db) as forged:
        forged.execute("DELETE FROM archive_bounds")
        forged.execute("DELETE FROM meta WHERE k = ?", (ARCHIVE_BOUNDS_FOR,))

    remote = Remote(monkeypatch, refuse=False)
    with litelink.open(tmp_path, "s", read_only=True, s3=s3) as reader:
        assert reader.scan(start_offset=low).read_all().num_rows == ROWS - low + 1
        assert remote.calls == 1, "unknown bounds must read the archive"

    with litelink.open(tmp_path, "s", s3=s3) as writer:
        assert Buffer.peek_meta(buffer_db, ARCHIVE_BOUNDS_FOR) == writer.archive
        assert writer.scan(start_offset=low).read_all().num_rows == ROWS - low + 1
        assert remote.calls == 1, "backfilled, the hot read is local again"


@pytest.mark.s3
def test_repointing_forgets_the_bounds_and_records_the_new_archives(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Bounds describe one archive; a move must not carry them to another.

    Pointed away and back, the rows each time are the files of the archive
    the log now names — none at the fresh prefix, and the original's again on
    return. Pointed at one that cannot be read, they are complete for nothing.

    Falsify by dropping `ARCHIVE_BOUNDS_FOR` from `_repoint`'s reset: the
    unreadable prefix is trusted with the old archive's files.
    """
    with evicted(tmp_path, bucket, s3) as log:
        original = log.archive
        assert original is not None

        def stored() -> tuple[str | None, int]:
            complete_for = log._buffer.archive_bounds_version()[0]  # noqa: SLF001
            (count,) = log._buffer._con.execute(  # noqa: SLF001
                "SELECT count(*) FROM archive_bounds"
            ).fetchone()
            return complete_for, count

        before = stored()
        assert before[0] == original
        assert before[1] == log.archive_files() > 0

        log.set_archive(f"s3://{bucket}/elsewhere")
        assert stored() == (f"s3://{bucket}/elsewhere", 0)

        log.set_archive(original)
        assert stored() == before
        assert log.scan().read_all().num_rows == ROWS

        # Where the new archive cannot be read, nothing replaces the old
        # rows — so the move itself has to be what stops them being trusted.
        log.set_archive(f"s3://{bucket}-nonexistent/prefix")
        assert stored()[0] is None


@pytest.mark.s3
def test_a_restore_takes_its_bounds_from_the_archive_not_the_replica(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The replica lags the archive, and its bounds lag with it.

    The restored log's local table is empty, so every archive file sits below
    it — a file pushed after the replica shipped, missing from its bounds,
    would be skipped by any read those bounds rule out, and its rows are then
    in no tier the read looks at.

    Falsify by removing the reset before `_backfill_archive_bounds` in
    `restore`: the replica's bounds are kept, and the read of the late rows
    comes back empty.
    """
    where = f"s3://{bucket}/prefix"
    primary = tmp_path / "primary"
    with archived_log(primary, bucket, s3) as log:
        log.extend(rows(ROWS // 2))
        log.seal()
        log.sync(push_unsettled=True)

        # The replica, as of now.
        second = tmp_path / "second"
        (second / "s").mkdir(parents=True)
        source = sqlite3.connect(Layout(primary, "s").buffer_db)
        copy = sqlite3.connect(Layout(second, "s").buffer_db)
        source.backup(copy)
        source.close()
        copy.close()

        # Pushed after it shipped.
        log.extend(
            {"event_ts": ROWS + i, "key": "late", "payload": "z"} for i in range(50)
        )
        log.seal()
        log.sync(push_unsettled=True)
        held = log.archive_files()

    with litelink.restore(second, "s", archive=where, s3=s3) as revived:
        (stored,) = revived._buffer._con.execute(  # noqa: SLF001
            "SELECT count(*) FROM archive_bounds"
        ).fetchone()
        assert stored == held
        late = revived.scan(where=f"event_ts >= {ROWS}").read_all()
        assert late.num_rows == 50
