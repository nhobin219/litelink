"""A log with no remote published table publishes to a local one (#98).

Every log has a published table now: on S3 when one is given, otherwise a local
directory under the log's own. The pipeline is the same either way — seal,
compact, publish, evict — so none of this needs S3 or the network.
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import duckdb
import pytest

import litelink
from litelink import OFFSET, LogConfig, RetiredError, WriteHandle
from litelink._layout import Layout
from litelink._read import Reader
from litelink._table import VERSION_HINT
from tests.test_publish import ROWS, SCHEMA, rows


def local_log(root: Path, *, at: str | None = None, **overrides: object) -> WriteHandle:
    """A log given no published table, sized like `published_log` so it seals many files."""
    settings: dict[str, object] = {
        "target_seal_size": 64 * 1024,
        "target_compact_size": 64 * 1024,
        "compact_min_files": 2,
        "staging_snapshot_retention": timedelta(seconds=0),
        "published_snapshot_retention": timedelta(seconds=0),
    }
    settings.update(overrides)
    config = LogConfig(**settings)  # ty: ignore[invalid-argument-type]

    return litelink.new(
        root, "s", schema=SCHEMA, sort_by=("event_ts",), config=config, published=at
    )


def published(root: Path, *, at: str | None = None, **overrides: object) -> WriteHandle:
    """Every row published, most evicted from the staging table, a tail buffered."""
    settings: dict[str, object] = {
        "staging_retention": timedelta(0),
        "staging_rows": 1000,
        **overrides,
    }
    log = local_log(root, at=at, **settings)
    log.extend(rows(ROWS))
    log.seal()
    log.publish(push_unsettled=True)
    log.maintain()
    log.extend({"event_ts": ROWS + i, "key": "t", "payload": "y"} for i in range(7))

    return log


def offsets(log: litelink.LogHandle) -> list[int]:
    return sorted(log.scan(columns=[OFFSET]).read_all().column(0).to_pylist())


def test_a_log_given_no_published_table_publishes_under_its_own_directory(
    tmp_path: Path,
) -> None:
    """The default is `<root>/<name>/published`, recorded in the log.

    Falsify by leaving `meta` empty in `litelink.new`: `log.published` still
    reads the default, but the stored row this asserts is missing.
    """
    default = Layout(tmp_path, "s").default_published
    assert default == f"file://{tmp_path / 's' / 'published'}"

    with local_log(tmp_path) as log:
        assert log.published == default
        assert log._buffer.get_meta("published") == default  # noqa: SLF001


def test_a_local_only_log_evicts_what_it_published_and_reads_it_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Eviction drops only what the published table holds, and a full read gets
    the rest from there — without httpfs or credentials.

    Falsify by loading httpfs for every published table in `Reader._prepare_remote`:
    the refused load raises.
    """
    import litelink._read as read

    original = read.load_extension

    def refuse(con: duckdb.DuckDBPyConnection, name: str, **kwargs: bool) -> None:
        if name == "httpfs":
            msg = "a local published table must not load httpfs"
            raise AssertionError(msg)

        original(con, name, **kwargs)

    monkeypatch.setattr(read, "load_extension", refuse)

    with published(tmp_path) as log:
        extent = log.staging_extent()
        assert extent is not None
        assert extent[0] > 1, "the fixture must evict part of the log"
        assert offsets(log) == list(range(1, ROWS + 8))

        coverage = log.coverage()
        assert coverage.published == (1, extent[0])
        assert log.column_statistics(tier="published").record_count == extent[0] - 1


def test_any_engine_reads_the_local_published_table(tmp_path: Path) -> None:
    """It is plain Iceberg, published through `version-hint.text` like the S3
    one, so DuckDB reads it with no catalog and no litelink."""
    with published(tmp_path) as log:
        table = tmp_path / "s" / "published" / "s"
        assert (table / "metadata" / VERSION_HINT).exists()
        through = log.published_through()

    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg")
    count = con.execute(
        f"SELECT count(*), max(litelink_offset) FROM iceberg_scan('{table}', "
        "version_name_format = '%s%s.metadata.json')"
    ).fetchone()
    assert count == (through, through)


def test_retiring_a_local_only_log_keeps_every_row(tmp_path: Path) -> None:
    """`retire()` used to refuse a local-only log, since emptying its staging
    table deleted the only copy. Now the published table holds them.

    Falsify by skipping the `publish` step in `retire`: eviction finds the tail
    unpublished and the log is refused as not yet retired.
    """
    with published(tmp_path) as log:
        total = log.end_offset() - 1
        log.retire()

        assert log.staging_rows() == 0
        with pytest.raises(RetiredError):
            log.append({"event_ts": 1, "key": "k", "payload": "p"})

    with litelink.open(tmp_path, "s", read_only=True) as reader:
        assert offsets(reader) == list(range(1, total + 1))


def test_rewrite_published_works_on_a_local_published_table(tmp_path: Path) -> None:
    """It used to refuse a local-only log.

    A re-cut keeps every row, and the files it supersedes are queued as
    published objects — by URI — so they are released through the published
    table's own expiry rather than unlinked as if they were local files.

    Falsify by naming the published table's files as plain paths in `LogTable._name`:
    the superseded files are queued as local ones.
    """
    with published(tmp_path, staging_retention=timedelta(0), staging_rows=0) as log:
        expected = offsets(log)
        # A raised target is one of the things `rewrite_published` exists for.
        log.set_config(replace(log.config, target_compact_size=4 * 64 * 1024))
        log.rewrite_published()

        superseded = [
            key
            for key in log._buffer.queued_deletions()  # noqa: SLF001
            if "/published/" in key
        ]
        assert superseded, "the re-cut must supersede published files"
        assert all(key.startswith("file:///") for key in superseded)

        time.sleep(1.1)
        # `publish`, which drains the published table's queue (#113).
        log.publish()
        assert offsets(log) == expected
        assert not any(
            Path(key.removeprefix("file://")).exists() for key in superseded
        ), "released once the published table expired the snapshots naming them"


def test_replication_restore_and_hydrate_need_a_remote_published_table(
    tmp_path: Path,
) -> None:
    """The WAL replica gets unsealed rows off this machine, and a local
    published table is on it; `hydrate` would copy files from this disk to this disk.

    Falsify by removing the `remote()` check from `hydrate`: it runs.
    """
    with pytest.raises(ValueError, match="remote published table"):
        local_log(tmp_path, wal_replication=True)

    with local_log(tmp_path) as log:
        with pytest.raises(ValueError, match="remote"):
            log.replication_config()

        with pytest.raises(ValueError, match="remote published table"):
            log.hydrate(timedelta(days=1))

    with pytest.raises(ValueError, match="remote published table"):
        litelink.restore(tmp_path / "elsewhere", "s", published=f"file://{tmp_path}/x")


def test_an_explicit_local_published_table_is_used(tmp_path: Path) -> None:
    """`file:///directory` names a local published table anywhere, the way `s3://`
    names a remote one; relative paths are refused."""
    where = f"file://{tmp_path / 'shared'}"
    with litelink.new(tmp_path / "root", "s", schema=SCHEMA, published=where) as log:
        log.extend(rows(10))
        log.seal()
        log.publish(push_unsettled=True)
        assert log.published == where
        assert (tmp_path / "shared" / "s" / "metadata" / VERSION_HINT).exists()

    with pytest.raises(ValueError, match="absolute"):
        litelink.new(tmp_path / "other", "s", schema=SCHEMA, published="file://shared")


def test_a_log_from_before_published_tables_gets_the_default(tmp_path: Path) -> None:
    """A local-only log written before #98 records no published table. A writer's open
    records the default, and the first `publish` publishes everything it holds.

    Falsify by removing the default from `litelink.open`'s writer path: the
    publish fences compare an empty row with the default and refuse the push.
    """
    with local_log(tmp_path) as log:
        log.extend(rows(500))
        log.seal()

    with sqlite3.connect(Layout(tmp_path, "s").buffer_db) as old:
        old.execute("DELETE FROM meta WHERE k = 'published'")

    with litelink.open(tmp_path, "s", read_only=True) as reader:
        assert reader.published == Layout(tmp_path, "s").default_published

    with litelink.open(tmp_path, "s") as log:
        log.publish(push_unsettled=True)
        assert log.published_through() == 500


def test_set_published_none_points_back_at_the_local_default(tmp_path: Path) -> None:
    """There is no detached state: None re-points to the local default, and
    I4 still holds across the move — nothing the new published table lacks is evicted.

    Falsify by mapping None to "" in `set_published`: `log.published` reads the
    default, but the stored row is empty and the next publish's fence refuses it.
    """
    with local_log(tmp_path, staging_retention=timedelta(0), staging_rows=0) as log:
        log.extend(rows(500))
        log.seal()
        log.set_published(f"file://{tmp_path / 'away'}")
        log.publish(push_unsettled=True)
        log.set_published(None)

        assert log._buffer.get_meta("published") == log.published  # noqa: SLF001
        log.maintain()
        assert log.staging_rows() == 500, (
            "the default published table holds none of it yet"
        )

        log.publish(push_unsettled=True)
        log.maintain()
        assert log.staging_rows() == 0
        assert offsets(log) == list(range(1, 501))


def test_the_reader_never_loads_httpfs_for_a_local_published_table(
    tmp_path: Path,
) -> None:
    with published(tmp_path) as log:
        log.scan().read_all()
        assert not log._reader._remote_ready  # noqa: SLF001
        assert isinstance(log._reader, Reader)  # noqa: SLF001


def test_a_move_the_new_published_table_cannot_take_is_refused_and_not_recorded(
    tmp_path: Path,
) -> None:
    """A move opens the new table before it records anything, so one that
    cannot reach it fails the call and leaves the log where it was.

    Falsify by making the move's `adopt` best effort in `set_published`: the
    call succeeds and the log records a published table nothing can open.
    """
    blocker = tmp_path / "blocker"
    blocker.write_text("a file, so nothing can be created beneath it")

    with local_log(tmp_path) as log:
        log.extend(rows(100))
        log.seal()
        before = log.published

        with pytest.raises(OSError):  # noqa: PT011
            log.set_published(f"file://{blocker}/published table")

        assert log.published == before, "a move that failed was recorded"
        log.publish(push_unsettled=True)
        assert log.published_through() == 100


def test_restating_the_published_table_takes_no_claim_and_opens_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A writer that declares its published table on every restart is told it already
    has it — without waiting on maintenance or reaching the published table.

    Falsify by removing the early return in `set_published`: the refused claim
    raises.
    """
    from litelink._handle import WriteHandle as Handle
    from litelink._table import LogTable

    def refuse(*_: object, **__: object) -> None:
        msg = "a restatement must not claim or open anything"
        raise AssertionError(msg)

    with local_log(tmp_path) as log:
        where = log.published
        monkeypatch.setattr(Handle, "_claim_settings", refuse)
        monkeypatch.setattr(LogTable, "open_published", refuse)

        log.set_published(where)
        log.set_published(None)
        log.set_published(where + "/")

        assert log.published == where


def stored_names(root: Path) -> dict[str, set[str]]:
    """Every name a log has stored: meta keys, tier rows, catalog names, files."""
    layout = Layout(root, "s")
    with sqlite3.connect(layout.buffer_db) as con:
        meta = {str(r[0]) for r in con.execute("SELECT k FROM meta")}
        tiers = {str(r[0]) for r in con.execute("SELECT tier FROM tier_offsets")}
        indexes = {
            str(r[0])
            for r in con.execute("SELECT name FROM sqlite_master WHERE type='index'")
        }

    catalogs: set[str] = set()
    for database in layout.directory.glob("*.db"):
        with sqlite3.connect(database) as con:
            try:
                catalogs |= {
                    f"{database.name}:{r[0]}"
                    for r in con.execute(
                        "SELECT DISTINCT catalog_name FROM iceberg_tables"
                    )
                }
            except sqlite3.OperationalError:
                continue

    files = {p.name for p in layout.directory.glob("*.db")}

    return {
        "meta": meta,
        "tiers": tiers,
        "indexes": indexes,
        "catalogs": catalogs,
        "files": files,
    }


def test_a_new_log_stores_only_the_new_names(tmp_path: Path) -> None:
    """#98's names on disk, not only in the API: `published` and
    `published_through` in `meta`, `staging`/`published` tier rows,
    `published.db`, and catalogs named `staging` and `published`.

    Falsify by setting `PUBLISHED_KEY` back to "archive": the location is
    stored under the old key.
    """
    with published(tmp_path):
        pass

    names = stored_names(tmp_path)
    assert {"published", "published_through"} <= names["meta"]
    assert not {"archive", "archive_through"} & names["meta"]
    assert names["tiers"] >= {"staging", "published"}
    assert not {"local", "archive"} & names["tiers"]
    assert "extent_published" in names["indexes"]
    assert "extent_archived" not in names["indexes"]
    assert names["catalogs"] == {"catalog.db:staging", "published.db:published"}
    assert "archive.db" not in names["files"]


def test_a_log_with_the_old_names_opens_and_moves_to_the_new_ones(
    tmp_path: Path,
) -> None:
    """A log written before #98 stores `archive`, `archive_through`, tier rows
    `local`/`archive`, `archive.db` and catalogs named `local`/`archive`. It
    opens and reads unchanged; every row it writes takes the new name, and the
    files and catalog names it already has are kept rather than rewritten.

    Falsify by dropping the old-name fallback from `_meta_value`: the reader
    takes the local default for its location and reads an empty table — or
    `adopt_current_names` from `open`: the old keys outlive the writer.
    """
    # Somewhere other than the default, which is also what a missing location
    # reads as — so only the old key can say where this one is.
    with published(tmp_path, at=f"file://{tmp_path / 'elsewhere'}") as log:
        expected = offsets(log)
        coverage = log.coverage()

    layout = Layout(tmp_path, "s")
    with sqlite3.connect(layout.buffer_db) as con:
        con.execute("UPDATE meta SET k = 'archive' WHERE k = 'published'")
        con.execute(
            "UPDATE meta SET k = 'archive_through' WHERE k = 'published_through'"
        )
        for table in ("tier_offsets", "tier_statistics"):
            con.execute(f"UPDATE {table} SET tier = 'local' WHERE tier = 'staging'")  # noqa: S608
            con.execute(f"UPDATE {table} SET tier = 'archive' WHERE tier = 'published'")  # noqa: S608

        con.execute("DROP INDEX extent_published")
        con.execute(
            "CREATE INDEX extent_archived ON extent (start_offset)"
            " WHERE rel_path IS NOT NULL"
        )

    (layout.directory / "published.db").rename(layout.directory / "archive.db")
    for database, old in (("catalog.db", "local"), ("archive.db", "archive")):
        with sqlite3.connect(layout.directory / database) as con:
            con.execute("UPDATE iceberg_tables SET catalog_name = ?", (old,))
            con.execute(
                "UPDATE iceberg_namespace_properties SET catalog_name = ?", (old,)
            )

    with litelink.open(tmp_path, "s", read_only=True) as reader:
        assert offsets(reader) == expected
        assert reader.coverage() == coverage

    with litelink.open(tmp_path, "s") as log:
        assert offsets(log) == expected
        log.extend(rows(5))
        log.seal()
        log.publish(push_unsettled=True)
        log.maintain()
        assert offsets(log) == [*expected, *range(expected[-1] + 1, expected[-1] + 6)]

    names = stored_names(tmp_path)
    assert {"published", "published_through"} <= names["meta"]
    assert not {"archive", "archive_through"} & names["meta"]
    assert not {"local", "archive"} & names["tiers"], "written, so renamed"
    assert names["indexes"] >= {"extent_published"}
    assert "extent_archived" not in names["indexes"]
    assert names["files"] >= {"archive.db"}, "the catalog file is kept"
    assert "published.db" not in names["files"]
    assert names["catalogs"] == {"catalog.db:local", "archive.db:archive"}


def test_publish_sweeps_a_local_published_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The default published table is on local disk and named by `file://`
    URIs, which the sweep's listing must produce too (#113).

    Falsify by listing local files as plain paths: the anchor check refuses
    every listing, and the stranded manifest survives.
    """
    import uuid

    import litelink._maintenance as maintenance

    monkeypatch.setattr(maintenance, "SWEEP_MIN_AGE", timedelta(0))
    monkeypatch.setattr(maintenance, "SWEEP_INTERVAL", timedelta(0))
    with local_log(tmp_path) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.publish()

        table = log._published.require()  # noqa: SLF001
        directory = Path(table.metadata_location.removeprefix("file://")).parent
        stranded = directory / f"{uuid.uuid4()}-m0.avro"
        stranded.write_bytes(b"stranded")

        log.extend(rows(ROWS))
        log.seal_due()
        log.publish()

        assert not stranded.exists()
        assert log.scan().read_all().num_rows == 2 * ROWS


def test_retire_deletes_stranded_metadata_in_both_tables(tmp_path: Path) -> None:
    """A retired log takes no more passes, so `retire` sweeps both tables
    completely before it finishes (#113).

    Falsify by dropping `sweep_everything` from `retire`: both stranded
    manifests survive.
    """
    import os
    import uuid
    from datetime import UTC, datetime

    old = (datetime.now(UTC) - timedelta(hours=2)).timestamp()
    with local_log(tmp_path) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        log.publish()

        stranded = []
        for table in (log._table, log._published.require()):  # noqa: SLF001
            directory = Path(table.metadata_location.removeprefix("file://")).parent
            path = directory / f"{uuid.uuid4()}-m0.avro"
            path.write_bytes(b"stranded")
            os.utime(path, (old, old))
            stranded.append(path)

        log.retire()

        assert not any(path.exists() for path in stranded)

    with litelink.open(tmp_path, "s", read_only=True) as retired:
        assert retired.scan().read_all().num_rows == ROWS


def test_the_published_sweep_runs_after_publish_releases_its_lease(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The sweep needs no claim, and a backlog pass on object storage is tens
    of seconds of deletes; under `publish`'s lease every other process's
    `maintain` and `publish` would be refused for all of it.

    Falsify by running `sweep_published` inside the `try` that holds the
    lease: the lease cannot be taken during the sweep.
    """
    with local_log(tmp_path) as log:
        log.extend(rows(ROWS))
        log.seal_due()
        maintenance = log._maintenance  # noqa: SLF001
        real = maintenance._sweep  # noqa: SLF001
        free: list[bool] = []

        def sweep(*args: object, **kwargs: object) -> None:
            lease = log._lease("maintain")  # noqa: SLF001
            free.append(lease.acquire())
            lease.release()
            real(*args, **kwargs)  # ty: ignore[invalid-argument-type]

        monkeypatch.setattr(maintenance, "_sweep", sweep)
        log.publish()

        assert free == [True]
