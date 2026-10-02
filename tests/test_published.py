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
from litelink._claim import Claim, new_owner
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
    log.seal(flush=True)
    log.publish(flush=True)
    log.advance()
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
        # `expire_published`, which drains the published table's queue (#113).
        log.reclaim("published")
        assert offsets(log) == expected
        assert not any(
            Path(key.removeprefix("file://")).exists() for key in superseded
        ), "released once the published table expired the snapshots naming them"


def test_replication_and_restore_need_a_remote_published_table(
    tmp_path: Path,
) -> None:
    """The WAL replica gets unsealed rows off this machine, and a local
    published table is on it, so replication has nowhere to ship to and a
    restore nothing off-box to restore from.

    Falsify by removing the remote check from `replication_config`: it returns
    a config.
    """
    with pytest.raises(ValueError, match="remote published table"):
        local_log(tmp_path, wal_replication=True)

    with local_log(tmp_path) as log:
        with pytest.raises(ValueError, match="remote"):
            log.replication_config()

    with pytest.raises(ValueError, match="remote published table"):
        litelink.restore(tmp_path / "elsewhere", "s", published=f"file://{tmp_path}/x")


def test_an_explicit_local_published_table_is_used(tmp_path: Path) -> None:
    """`file:///directory` names a local published table anywhere, the way `s3://`
    names a remote one; relative paths are refused."""
    where = f"file://{tmp_path / 'shared'}"
    with litelink.new(tmp_path / "root", "s", schema=SCHEMA, published=where) as log:
        log.extend(rows(10))
        log.seal(flush=True)
        log.publish(flush=True)
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
        log.seal(flush=True)

    with sqlite3.connect(Layout(tmp_path, "s").buffer_db) as old:
        old.execute("DELETE FROM meta WHERE k = 'published'")

    with litelink.open(tmp_path, "s", read_only=True) as reader:
        assert reader.published == Layout(tmp_path, "s").default_published

    with litelink.open(tmp_path, "s") as log:
        log.publish(flush=True)
        assert log.published_through() == 500


def test_set_published_none_points_back_at_the_local_default(tmp_path: Path) -> None:
    """There is no detached state: None re-points to the local default, and
    I4 still holds across the move — nothing the new published table lacks is evicted.

    Falsify by mapping None to "" in `set_published`: `log.published` reads the
    default, but the stored row is empty and the next publish's fence refuses it.
    """
    with local_log(tmp_path, staging_retention=timedelta(0), staging_rows=0) as log:
        log.extend(rows(500))
        log.seal(flush=True)
        log.set_published(f"file://{tmp_path / 'away'}")
        log.publish(flush=True)
        log.set_published(None)

        assert log._buffer.get_meta("published") == log.published  # noqa: SLF001
        log.evict()
        assert log.staging_rows() == 500, (
            "the default published table holds none of it yet"
        )

        log.publish(flush=True)
        log.advance()
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
        log.seal(flush=True)
        before = log.published

        with pytest.raises(OSError):  # noqa: PT011
            log.set_published(f"file://{blocker}/published table")

        assert log.published == before, "a move that failed was recorded"
        log.publish(flush=True)
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
        log.seal(flush=True)
        log.publish(flush=True)
        log.advance()
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
        log.seal()
        log.publish()

        table = log._published.require()  # noqa: SLF001
        directory = Path(table.metadata_location.removeprefix("file://")).parent
        stranded = directory / f"{uuid.uuid4()}-m0.avro"
        stranded.write_bytes(b"stranded")

        log.sweep()

        assert not stranded.exists()
        assert log.scan().read_all().num_rows == ROWS


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
        log.seal()
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


def test_the_published_sweep_runs_with_publishs_lease_released(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The sweep needs no claim, and a backlog pass on object storage is tens
    of seconds of deletes; under `publish`'s lease every other process's
    `advance` and `publish` would be refused for all of it.

    Falsify by running `sweep_published` inside `publish`'s `try`, which
    holds the lease: the lease cannot be taken during the sweep.
    """
    with local_log(tmp_path) as log:
        log.extend(rows(ROWS))
        log.seal()
        maintenance = log._maintenance  # noqa: SLF001
        real = maintenance._sweep  # noqa: SLF001
        free: list[bool] = []

        def sweep(*args: object, **kwargs: object) -> None:
            lease = log._lease("maintain")  # noqa: SLF001
            free.append(lease.acquire())
            lease.release()
            real(*args, **kwargs)  # ty: ignore[invalid-argument-type]

        monkeypatch.setattr(maintenance, "_sweep", sweep)
        log.advance()

        assert free == [True, True], "both sweeps, with the lease free"


def test_maintain_seals_publishes_and_evicts_in_one_pass(tmp_path: Path) -> None:
    """The pipeline runs in lifecycle order: what a pass seals is published, and
    what it publishes is evicted, in the same pass (#117, #119).

    Falsify by moving `publish` after `evict` in `advance`: the staging table
    still holds the files after one pass.
    """
    with local_log(tmp_path, staging_retention=timedelta(0), staging_rows=0) as log:
        log.extend(rows(ROWS))

        log.advance()

        # The trailing run and the unsealed tail stay local until they settle;
        # everything that was published has already left staging.
        through = log.published_through()
        assert through > 0
        assert all(
            f.start > through
            for f in log._table.data_files()  # noqa: SLF001
        ), "a published file survived the pass that published it"
        assert log.scan().read_all().num_rows == ROWS


def test_a_failed_publish_raises_after_local_maintenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """On a machine cut off from its published table, local storage is still
    reclaimed, and the failure is not swallowed.

    Falsify by letting the publish's error escape at once: `evict` never
    runs and the call log ends at `publish`.
    """
    with local_log(tmp_path) as log:
        log.extend(rows(ROWS))
        ran: list[str] = []
        for name in ("evict", "reclaim", "sweep"):
            real = getattr(log, name)

            def record(*args: object, real=real, name=name) -> None:
                ran.append(" ".join([name, *map(str, args)]))
                real(*args)

            monkeypatch.setattr(log, name, record)

        def unreachable(**_: object) -> None:
            ran.append("publish")
            raise OSError("published table unreachable")

        monkeypatch.setattr(log, "publish", unreachable)

        with pytest.raises(OSError, match="unreachable"):
            log.advance()

        assert ran == [
            "publish",
            "evict buffer",
            "evict staging",
            "reclaim staging",
            "sweep staging",
        ], "local steps run, the published ones do not"


def test_evict_reclaim_and_sweep_take_the_table_they_act_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One routine per operation, the table an argument, None for every table
    it acts on. A misspelt table — or one the routine does not act on — is
    refused, not a call that silently did nothing (#119, #122).

    Falsify by treating an unknown table as None: the misspelling acts on all.
    """
    with local_log(tmp_path) as log:
        maintenance = log._maintenance  # noqa: SLF001
        called: list[str] = []
        for name in (
            "evict_buffer",
            "evict",
            "expire",
            "expire_published",
            "sweep_staging",
            "sweep_published",
        ):
            monkeypatch.setattr(
                maintenance, name, lambda *_, name=name, **__: called.append(name)
            )

        buffer = log._buffer  # noqa: SLF001
        monkeypatch.setattr(
            buffer, "reclaim_free_pages", lambda *_: called.append("vacuum")
        )

        log.evict("buffer")
        log.reclaim("staging")
        log.sweep("published")
        assert called == ["evict_buffer", "expire", "sweep_published"]

        called.clear()
        log.evict()
        log.reclaim()
        log.sweep()
        assert called == [
            "evict_buffer",
            "evict",
            "vacuum",
            "expire",
            "expire_published",
            "sweep_staging",
            "sweep_published",
        ]

        for routine, wrong in (
            (log.evict, "published"),
            (log.reclaim, "stagin"),
            (log.sweep, "buffer"),
        ):
            with pytest.raises(ValueError, match="table must be"):
                routine(wrong)  # ty: ignore[invalid-argument-type]


def test_advance_flush_pushes_everything_to_the_published_table(tmp_path: Path) -> None:
    """`flush` means the same at every stage: push everything through now,
    regardless of thresholds. On `advance` it reaches `seal` and `publish`, so
    one pass leaves nothing buffered and nothing unpublished (#119).

    Falsify by not passing `flush` to `publish` in `advance`: the trailing
    run stays local and `published_through` falls short of the last row.
    """
    with local_log(tmp_path) as log:
        log.extend(rows(ROWS))

        log.advance()
        assert log.published_through() < ROWS, "without flush the tail waits"

        log.advance(flush=True)
        assert log.buffered_rows() == 0
        assert log.published_through() == ROWS
        assert log.scan().read_all().num_rows == ROWS


def test_evict_bounds_narrow_what_is_dropped(tmp_path: Path) -> None:
    """`[start_offset, end_offset)` narrows an eviction and never widens it, so
    a caller can chunk a large one (#122). Staging eviction removes a prefix,
    so a `start_offset` above the staging table's first offset is refused
    rather than evicting nothing.

    Falsify by ignoring `end_offset` in `evict_buffer`: the whole sealed range
    goes in the first call.
    """
    with local_log(tmp_path) as log:
        log.extend(rows(ROWS))
        log.seal(flush=True)
        buffer = log._buffer  # noqa: SLF001
        assert buffer.span() == (1, ROWS + 1)

        log.evict("buffer", end_offset=101)
        assert buffer.span() == (101, ROWS + 1)

        log.evict("buffer", start_offset=101, end_offset=201)
        assert buffer.span() == (201, ROWS + 1)

        log.evict("buffer")
        assert buffer.span() is None
        assert log.scan().read_all().num_rows == ROWS

        with pytest.raises(ValueError, match="removes a prefix"):
            log.evict("staging", start_offset=2)

        with pytest.raises(ValueError, match="below start_offset"):
            log.evict("buffer", start_offset=10, end_offset=5)


def test_a_local_log_never_loads_the_read_cache(
    tmp_path: Path, isolated_read_cache: Path
) -> None:
    """The cache is for S3 reads; a local published table is already on disk
    (#118). A read that reaches it loads no `cache_httpfs`, changes no DuckDB
    setting, and creates no cache directory.

    Falsify by installing the cache before the reader's `remote()` check: the
    extension loads and the default directory appears.
    """
    with local_log(tmp_path, staging_retention=timedelta(0), staging_rows=0) as log:
        log.extend(rows(ROWS))
        log.advance(flush=True)
        assert log.staging_files() == 0, "the read must reach the published table"

        assert log.scan().read_all().num_rows == ROWS

        connection = log._reader._connect()  # noqa: SLF001
        extensions = {
            name
            for (name,) in connection.execute(
                "SELECT extension_name FROM duckdb_extensions() WHERE loaded"
            ).fetchall()
        }
        assert "cache_httpfs" not in extensions
        assert "httpfs" not in extensions
        assert not (isolated_read_cache / "litelink").exists()


def _claim(log: WriteHandle, start: int, end: int) -> Claim:
    """A claim held by another owner, as a concurrent seal or compaction holds
    one."""
    other = Claim(
        log._buffer._con,  # noqa: SLF001
        log._buffer._lock,  # noqa: SLF001
        "other",
        start,
        end,
        new_owner(),
    )
    assert other.acquire()

    return other


def test_publish_claims_only_the_range_it_pushes(tmp_path: Path) -> None:
    """Once the published table has its tier row, a publish claims
    `[floor, end of what it pushes)` and nothing else (#118): a claim above
    that — a seal of new rows, a compaction of the trailing run — does not
    refuse it, and one inside it does.

    Falsify by having `_publish_lease` return the whole-log lease: the claim
    above the push refuses it.
    """
    with local_log(tmp_path) as log:
        log.extend(rows(ROWS))
        log.seal(flush=True)
        log.publish(flush=True)  # the first: writes the tier row
        log.extend(rows(ROWS))
        log.seal(flush=True)

        floor = log.published_through() + 1
        above = _claim(log, 10**9, 10**9 + 1)
        try:
            log.publish(flush=True)
        finally:
            above.release()

        assert log.published_through() == 2 * ROWS

        log.extend(rows(ROWS))
        log.seal(flush=True)
        inside = _claim(log, 2 * ROWS + 1, 2 * ROWS + 2)
        try:
            with pytest.raises(RuntimeError, match="another owner"):
                log.publish(flush=True)
        finally:
            inside.release()

        assert floor > 1
        assert log.scan().read_all().num_rows == 3 * ROWS


def test_a_publish_that_must_write_the_tier_row_claims_the_whole_log(
    tmp_path: Path,
) -> None:
    """With no tier row, the push computes it exactly from the published
    table's manifests, and that narrowing write must not race eviction
    widening it — so the publish takes the whole log (#118). A re-point
    leaves exactly this: the table there, its row dropped.

    Falsify by giving a publish without a tier row a range lease: a claim far
    above its files no longer refuses it.
    """
    with local_log(tmp_path) as log:
        log.extend(rows(ROWS))
        log.seal(flush=True)
        log.publish(flush=True)
        log._tiers.drop()  # noqa: SLF001 — as a re-point leaves it
        log.extend(rows(ROWS))
        log.seal(flush=True)

        far = _claim(log, 10**9, 10**9 + 1)
        try:
            with pytest.raises(RuntimeError, match="another owner"):
                log.publish(flush=True)
        finally:
            far.release()

        log.publish(flush=True)
        assert log._tiers.has()  # noqa: SLF001
        assert log.published_through() == 2 * ROWS


def test_two_publishes_exclude_each_other_with_nothing_to_push(
    tmp_path: Path,
) -> None:
    """Both start at the published floor, so their ranges overlap even when
    neither has anything to upload — and that matters: a push forgets intents
    it does not find landed, and another publisher's are files it is
    uploading (#118).

    Falsify by claiming an empty range when nothing is settled: the second
    publish proceeds beside the first.
    """
    with local_log(tmp_path) as log:
        log.extend(rows(ROWS))
        log.seal(flush=True)
        log.publish(flush=True)

        lease, _ = log._publish_lease(flush=False)  # noqa: SLF001
        assert lease.acquire()
        try:
            with pytest.raises(RuntimeError, match="another owner"):
                log.publish()
        finally:
            lease.release()
