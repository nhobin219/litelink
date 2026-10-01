"""A log with no remote archive publishes to a local one (#98).

Every log has an archive now: on S3 when one is given, otherwise a local
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
from litelink import LogConfig, RetiredError, WriteHandle
from litelink._layout import Layout
from litelink._read import Reader
from litelink._table import VERSION_HINT
from litelink.log import OFFSET
from tests.test_archive import ROWS, SCHEMA, rows


def local_log(root: Path, **overrides: object) -> WriteHandle:
    """A log given no archive, sized like `archived_log` so it seals many files."""
    settings: dict[str, object] = {
        "target_seal_size": 64 * 1024,
        "target_compact_size": 64 * 1024,
        "compact_min_files": 2,
        "snapshot_retention": timedelta(seconds=0),
    }
    settings.update(overrides)
    config = LogConfig(**settings)  # ty: ignore[invalid-argument-type]

    return litelink.new(root, "s", schema=SCHEMA, sort_by=("event_ts",), config=config)


def published(root: Path, **overrides: object) -> WriteHandle:
    """Every row published, most evicted from the local table, a tail buffered."""
    settings: dict[str, object] = {
        "local_retention": timedelta(0),
        "local_rows": 1000,
        **overrides,
    }
    log = local_log(root, **settings)
    log.extend(rows(ROWS))
    log.seal()
    log.sync(push_unsettled=True)
    log.maintain()
    log.extend({"event_ts": ROWS + i, "key": "t", "payload": "y"} for i in range(7))

    return log


def offsets(log: litelink.LogHandle) -> list[int]:
    return sorted(log.scan(columns=[OFFSET]).read_all().column(0).to_pylist())


def test_a_log_given_no_archive_publishes_under_its_own_directory(
    tmp_path: Path,
) -> None:
    """The default is `<root>/<name>/published`, recorded in the log.

    Falsify by leaving `meta` empty in `litelink.new`: `log.archive` still
    reads the default, but the stored row this asserts is missing.
    """
    default = Layout(tmp_path, "s").default_archive
    assert default == f"file://{tmp_path / 's' / 'published'}"

    with local_log(tmp_path) as log:
        assert log.archive == default
        assert log._buffer.get_meta("archive") == default  # noqa: SLF001


def test_a_local_only_log_evicts_what_it_published_and_reads_it_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Eviction drops only what the published table holds, and a full read gets
    the rest from there — without httpfs or credentials.

    Falsify by loading httpfs for every archive in `Reader._prepare_remote`:
    the refused load raises.
    """
    import litelink._read as read

    original = read.load_extension

    def refuse(con: duckdb.DuckDBPyConnection, name: str, **kwargs: bool) -> None:
        if name == "httpfs":
            msg = "a local archive must not load httpfs"
            raise AssertionError(msg)

        original(con, name, **kwargs)

    monkeypatch.setattr(read, "load_extension", refuse)

    with published(tmp_path) as log:
        extent = log.table_extent()
        assert extent is not None
        assert extent[0] > 1, "the fixture must evict part of the log"
        assert offsets(log) == list(range(1, ROWS + 8))

        coverage = log.coverage()
        assert coverage.archive == (1, extent[0] - 1)
        assert log.column_statistics(tier="archive").record_count == extent[0] - 1


def test_any_engine_reads_the_local_published_table(tmp_path: Path) -> None:
    """It is plain Iceberg, published through `version-hint.text` like the S3
    one, so DuckDB reads it with no catalog and no litelink."""
    with published(tmp_path) as log:
        table = tmp_path / "s" / "published" / "s"
        assert (table / "metadata" / VERSION_HINT).exists()
        through = log.archived_through()

    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg")
    count = con.execute(
        f"SELECT count(*), max(litelink_offset) FROM iceberg_scan('{table}', "
        "version_name_format = '%s%s.metadata.json')"
    ).fetchone()
    assert count == (through, through)


def test_retiring_a_local_only_log_keeps_every_row(tmp_path: Path) -> None:
    """`retire()` used to refuse a local-only log, since emptying its local
    table deleted the only copy. Now the published table holds them.

    Falsify by skipping the `sync` step in `retire`: eviction finds the tail
    unpublished and the log is refused as not yet retired.
    """
    with published(tmp_path) as log:
        total = log.end_offset() - 1
        log.retire()

        assert log.table_rows() == 0
        with pytest.raises(RetiredError):
            log.append({"event_ts": 1, "key": "k", "payload": "p"})

    with litelink.open(tmp_path, "s", read_only=True) as reader:
        assert offsets(reader) == list(range(1, total + 1))


def test_rewrite_archive_works_on_a_local_archive(tmp_path: Path) -> None:
    """It used to refuse a local-only log.

    A re-cut keeps every row, and the files it supersedes are queued as
    published objects — by URI — so they are released through the published
    table's own expiry rather than unlinked as if they were local files.

    Falsify by naming the archive's files as plain paths in `LogTable._name`:
    the superseded files are queued as local ones.
    """
    with published(tmp_path, local_retention=timedelta(0), local_rows=0) as log:
        expected = offsets(log)
        # A raised target is one of the things `rewrite_archive` exists for.
        log.set_config(replace(log.config, target_compact_size=4 * 64 * 1024))
        log.rewrite_archive()

        superseded = [
            key
            for key in log._buffer.queued_deletions()  # noqa: SLF001
            if "/published/" in key
        ]
        assert superseded, "the re-cut must supersede published files"
        assert all(key.startswith("file:///") for key in superseded)

        time.sleep(1.1)
        log.maintain()
        assert offsets(log) == expected
        assert not any(
            Path(key.removeprefix("file://")).exists() for key in superseded
        ), "released once the published table expired the snapshots naming them"


def test_replication_restore_and_hydrate_need_a_remote_archive(
    tmp_path: Path,
) -> None:
    """The WAL replica gets unsealed rows off this machine, and a local
    archive is on it; `hydrate` would copy files from this disk to this disk.

    Falsify by removing the `remote()` check from `hydrate`: it runs.
    """
    with pytest.raises(ValueError, match="remote archive"):
        local_log(tmp_path, wal_replication=True)

    with local_log(tmp_path) as log:
        with pytest.raises(ValueError, match="remote"):
            log.replication_config()

        with pytest.raises(ValueError, match="remote archive"):
            log.hydrate(timedelta(days=1))

    with pytest.raises(ValueError, match="remote archive"):
        litelink.restore(tmp_path / "elsewhere", "s", archive=f"file://{tmp_path}/x")


def test_an_explicit_local_archive_is_used(tmp_path: Path) -> None:
    """`file:///directory` names a local archive anywhere, the way `s3://`
    names a remote one; relative paths are refused."""
    where = f"file://{tmp_path / 'shared'}"
    with litelink.new(tmp_path / "root", "s", schema=SCHEMA, archive=where) as log:
        log.extend(rows(10))
        log.seal()
        log.sync(push_unsettled=True)
        assert log.archive == where
        assert (tmp_path / "shared" / "s" / "metadata" / VERSION_HINT).exists()

    with pytest.raises(ValueError, match="absolute"):
        litelink.new(tmp_path / "other", "s", schema=SCHEMA, archive="file://shared")


def test_a_log_from_before_published_tables_gets_the_default(tmp_path: Path) -> None:
    """A local-only log written before #98 records no archive. A writer's open
    records the default, and the first `sync` publishes everything it holds.

    Falsify by removing the default from `litelink.open`'s writer path: the
    sync fences compare an empty row with the default and refuse the push.
    """
    with local_log(tmp_path) as log:
        log.extend(rows(500))
        log.seal()

    with sqlite3.connect(Layout(tmp_path, "s").buffer_db) as old:
        old.execute("DELETE FROM meta WHERE k = 'archive'")

    with litelink.open(tmp_path, "s", read_only=True) as reader:
        assert reader.archive == Layout(tmp_path, "s").default_archive

    with litelink.open(tmp_path, "s") as log:
        log.sync(push_unsettled=True)
        assert log.archived_through() == 500


def test_set_archive_none_points_back_at_the_local_default(tmp_path: Path) -> None:
    """There is no detached state: None re-points to the local default, and
    I4 still holds across the move — nothing the new archive lacks is evicted.

    Falsify by mapping None to "" in `set_archive`: `log.archive` reads the
    default, but the stored row is empty and the next sync's fence refuses it.
    """
    with local_log(tmp_path, local_retention=timedelta(0), local_rows=0) as log:
        log.extend(rows(500))
        log.seal()
        log.set_archive(f"file://{tmp_path / 'away'}")
        log.sync(push_unsettled=True)
        log.set_archive(None)

        assert log._buffer.get_meta("archive") == log.archive  # noqa: SLF001
        log.maintain()
        assert log.table_rows() == 500, "the default archive holds none of it yet"

        log.sync(push_unsettled=True)
        log.maintain()
        assert log.table_rows() == 0
        assert offsets(log) == list(range(1, 501))


def test_the_reader_never_loads_httpfs_for_a_local_archive(tmp_path: Path) -> None:
    with published(tmp_path) as log:
        log.scan().read_all()
        assert not log._reader._remote_ready  # noqa: SLF001
        assert isinstance(log._reader, Reader)  # noqa: SLF001


def test_a_move_the_new_archive_cannot_take_is_refused_and_not_recorded(
    tmp_path: Path,
) -> None:
    """A move opens the new table before it records anything, so one that
    cannot reach it fails the call and leaves the log where it was.

    Falsify by making the move's `adopt` best effort in `set_archive`: the
    call succeeds and the log records an archive nothing can open.
    """
    blocker = tmp_path / "blocker"
    blocker.write_text("a file, so nothing can be created beneath it")

    with local_log(tmp_path) as log:
        log.extend(rows(100))
        log.seal()
        before = log.archive

        with pytest.raises(OSError):  # noqa: PT011
            log.set_archive(f"file://{blocker}/archive")

        assert log.archive == before, "a move that failed was recorded"
        log.sync(push_unsettled=True)
        assert log.archived_through() == 100


def test_restating_the_archive_takes_no_claim_and_opens_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A writer that declares its archive on every restart is told it already
    has it — without waiting on maintenance or reaching the archive.

    Falsify by removing the early return in `set_archive`: the refused claim
    raises.
    """
    from litelink._table import LogTable
    from litelink.log import WriteHandle as Handle

    def refuse(*_: object, **__: object) -> None:
        msg = "a restatement must not claim or open anything"
        raise AssertionError(msg)

    with local_log(tmp_path) as log:
        where = log.archive
        monkeypatch.setattr(Handle, "_claim_settings", refuse)
        monkeypatch.setattr(LogTable, "open_archive", refuse)

        log.set_archive(where)
        log.set_archive(None)
        log.set_archive(where + "/")

        assert log.archive == where
