"""The pre-0.2 layout, and moving off it (SPEC §2, `_migrate`).

**The legacy tree is built by litelink, not assembled by hand.** The fixture
below patches the four `Layout` members that moved and then calls the ordinary
constructors, so the tree under test is what the old code actually produced —
catalogs at the root, metadata under `<root>/litelink/<name>`, data already at
`<root>/<name>/data`. Hand-building it would test the migration against this
file's idea of the old layout rather than against the old layout.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from unittest import mock

import duckdb
import pyarrow as pa
import pytest
from pyarrow.fs import FileSelector

import litelink
from litelink import LogConfig
from litelink._layout import NAMESPACE, Layout
from litelink._migrate import (
    _entries,
    _prefix_holds,
    _s3_filesystem,
    _split_catalog,
    build_plan,
    is_legacy,
    migrate,
)
from litelink._read import load_extension, secret_sql
from litelink._s3 import S3Options
from litelink._table import LogTable

if TYPE_CHECKING:
    from collections.abc import Iterator

SCHEMA = pa.schema(
    [
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("key", pa.string()),
    ]
)


def rows(n: int, *, start: int = 0) -> list[dict[str, object]]:
    return [{"event_ts": 1000 + i, "key": f"k{i % 3}"} for i in range(start, start + n)]


@contextmanager
def _legacy_layout() -> Iterator[None]:
    """Make `Layout` describe the pre-0.2 tree, for the duration of a build.

    Scoped to the construction and no further, which matters more than it
    looks: left active it also patches the code under test, so `is_legacy`
    asks the patched `Layout` where the catalog is, finds it, and reports a
    legacy tree as already migrated. Every assertion downstream then passes on
    a migration that never ran.
    """
    patches = [
        mock.patch.object(
            Layout, "catalog_db", property(lambda self: self.root / "catalog.db")
        ),
        mock.patch.object(
            Layout, "archive_db", property(lambda self: self.root / "archive.db")
        ),
        mock.patch.object(
            Layout,
            "table_location",
            property(lambda self: f"file://{self.root / NAMESPACE / self.name}"),
        ),
        mock.patch.object(
            Layout,
            "archive_table_location",
            lambda self, prefix: f"{prefix.rstrip('/')}/{NAMESPACE}/{self.name}",
        ),
        mock.patch.object(
            Layout,
            "replication_config",
            property(lambda self: self.root / "litestream.yml"),
        ),
    ]
    for patch in patches:
        patch.start()

    try:
        yield
    finally:
        for patch in patches:
            patch.stop()


def _build_legacy(root: Path, name: str = "s", count: int = 400) -> None:
    with (
        _legacy_layout(),
        litelink.new(
            root,
            name,
            schema=SCHEMA,
            sort_by=("event_ts",),
            config=LogConfig(target_seal_size=4 * 1024, compact_min_files=1000),
        ) as log,
    ):
        log.extend(rows(count))
        while log.seal() is not None:
            pass


def test_the_fixture_really_builds_the_old_layout(tmp_path: Path) -> None:
    """Everything below is worthless if this is not the pre-0.2 tree."""
    _build_legacy(tmp_path)

    assert (tmp_path / "catalog.db").exists(), "catalogs sat at the root"
    assert (tmp_path / NAMESPACE / "s" / "metadata").is_dir(), (
        "metadata sat under <root>/litelink/<name>"
    )
    assert (tmp_path / "s" / "data").is_dir(), "data was already per-stream"
    assert not (tmp_path / "s" / "catalog.db").exists()
    assert is_legacy(tmp_path, "s")


def test_a_dry_run_changes_nothing(tmp_path: Path) -> None:
    """The default, because this moves the files a log resolves through."""
    _build_legacy(tmp_path)
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))

    plan = build_plan(tmp_path, "s")

    assert plan.needed
    assert sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*")) == before


def test_migration_moves_the_tree_and_keeps_every_row(tmp_path: Path) -> None:
    """The whole point, asserted on what a reader gets back."""
    _build_legacy(tmp_path)
    # Through the legacy layout: the current code REFUSES a legacy log rather
    # than reading it, which is asserted on its own below.
    with _legacy_layout(), litelink.open(tmp_path, "s", read_only=True) as log:
        expected = log.scan().read_all().num_rows
        assert expected == 400

    migrate(tmp_path, "s")

    assert not (tmp_path / "catalog.db").exists(), "the shared catalog is gone"
    assert not (tmp_path / NAMESPACE).exists(), "so is the namespace directory"
    assert (tmp_path / "s" / "catalog.db").exists()
    assert (tmp_path / "s" / "metadata").is_dir()
    assert not is_legacy(tmp_path, "s")

    with litelink.open(tmp_path, "s", read_only=True) as reopened:
        assert reopened.scan().read_all().num_rows == expected


def test_migration_preserves_the_retention_clock(tmp_path: Path) -> None:
    """Snapshot ids and commit times survive, which is why this rewrites.

    Retention derives a file's age from the commit time of the snapshot that
    added it — §2 stamps no ingest column — so a migration that recreated the
    table with `add_files` would stamp every file with its own timestamp and
    silently reset eviction and deletion on both tiers. `snapshot_ages` is the
    exact function that would be lied to, so it is the one asked here.
    """
    _build_legacy(tmp_path)
    with _legacy_layout():
        before = LogTable.load(Layout(tmp_path, "s"), readonly=True).snapshot_ages()

    assert before, "no snapshots to preserve — the fixture sealed nothing"

    migrate(tmp_path, "s")

    after = LogTable.load(Layout(tmp_path, "s"), readonly=True).snapshot_ages()

    assert {Path(p).name for p in after} == {Path(p).name for p in before}
    for path, added in before.items():
        assert after[path] == added, f"{path} lost its commit time"


def test_migration_is_idempotent(tmp_path: Path) -> None:
    """Running it twice is a no-op, not a second half-move."""
    _build_legacy(tmp_path)
    migrate(tmp_path, "s")

    again = migrate(tmp_path, "s")

    assert not again.needed
    with litelink.open(tmp_path, "s", read_only=True) as log:
        assert log.scan().read_all().num_rows == 400


def test_a_shared_catalog_survives_until_every_stream_has_moved(tmp_path: Path) -> None:
    """Two logs, one root — the case the old layout made possible.

    The shared file cannot be deleted while a sibling still resolves through
    it, and the migrated stream must not carry the sibling's row: a catalog
    entry naming a table whose metadata is not under this directory can only
    mislead.
    """
    import sqlite3

    _build_legacy(tmp_path, "trades", count=200)
    _build_legacy(tmp_path, "quotes", count=200)

    plan = migrate(tmp_path, "trades")

    assert (tmp_path / "catalog.db").exists(), "quotes still resolves through it"
    assert any("quotes" in note for note in plan.notes)

    connection = sqlite3.connect(tmp_path / "trades" / "catalog.db")
    names = connection.execute("SELECT table_name FROM iceberg_tables").fetchall()
    connection.close()
    assert names == [("trades",)], "the sibling's row must not come along"

    migrate(tmp_path, "quotes")

    assert not (tmp_path / "catalog.db").exists(), "the last stream takes it with it"
    for name in ("trades", "quotes"):
        with litelink.open(tmp_path, name, read_only=True) as log:
            assert log.scan().read_all().num_rows == 200


def test_the_stale_root_config_is_removed(tmp_path: Path) -> None:
    """It names a replica path that no longer exists.

    Left in place, a sidecar still reading it replicates to the old
    `<prefix>/_wal` — recreating the tree the migration just retired, silently.
    """
    _build_legacy(tmp_path)
    (tmp_path / "litestream.yml").write_text("dbs:\n")

    plan = migrate(tmp_path, "s")

    assert not (tmp_path / "litestream.yml").exists()
    assert any("Regenerate per stream" in note for note in plan.notes)


def test_the_shared_config_survives_until_the_last_stream_migrates(
    tmp_path: Path,
) -> None:
    """It names EVERY stream's buffer, so it is not the first stream's to delete.

    The root `litestream.yml` is the single per-root config the pre-0.2 layout
    forced. Removing it when the first stream migrated left every other stream
    with no replication config anywhere — and nothing said so; they simply
    stopped being replicated at the sidecar's next restart. While a sibling
    remains the file is not even stale: that sibling has not moved, and it
    still names its buffer correctly.
    """
    _build_legacy(tmp_path, "trades", count=200)
    _build_legacy(tmp_path, "quotes", count=200)
    config = tmp_path / "litestream.yml"
    config.write_text("dbs:\n")

    plan = migrate(tmp_path, "trades")

    assert config.exists(), "quotes is still replicated through it"
    assert any("quotes" in note for note in plan.notes)

    migrate(tmp_path, "quotes")

    assert not config.exists(), "the last stream takes it with it"


def test_a_log_already_on_the_new_layout_is_left_alone(tmp_path: Path) -> None:
    """Built through the real `Layout`, so it is already new-layout."""
    with litelink.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)) as log:
        log.extend(rows(10))

    assert not is_legacy(tmp_path, "s")
    assert not build_plan(tmp_path, "s").needed


def test_an_absent_log_is_refused_rather_than_half_migrated(tmp_path: Path) -> None:
    plan = build_plan(tmp_path, "nothing-here")

    assert plan.blockers
    assert not plan.needed


def test_retention_still_evicts_after_a_migration(tmp_path: Path) -> None:
    """The clock is not just preserved in metadata — it still drives eviction.

    `snapshot_ages` feeding the same timestamps back is the mechanism; this is
    the behaviour that mechanism exists for, asked of a migrated log.
    """
    _build_legacy(tmp_path)
    migrate(tmp_path, "s")

    with litelink.open(tmp_path, "s") as log:
        log.set_config(
            LogConfig(local_retention=timedelta(days=3650), compact_min_files=1000)
        )
        log.maintain()

        assert log.scan().read_all().num_rows == 400, (
            "a retention window wider than the log's age must evict nothing"
        )


def test_opening_a_legacy_log_names_the_migration(tmp_path: Path) -> None:
    """Not "use new()", which would create an empty log beside live data."""
    _build_legacy(tmp_path)

    with pytest.raises(FileNotFoundError, match="pre-0.2 layout") as caught:
        litelink.open(tmp_path, "s", read_only=True)

    assert "python -m litelink.migrate" in str(caught.value)


@pytest.mark.s3
def test_the_archive_metadata_moves_and_stays_readable(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The remote half: metadata beside its data, and still resolvable.

    Two readers are asked, because they resolve the archive by different
    routes. litelink goes through `archive.db`, whose row the migration
    rewrites; an outside engine goes through `version-hint.text`, which the
    migration moves without changing a byte of — the hint holds a bare metadata
    stem, and stems do not move.
    """
    where = f"s3://{bucket}/legacy"
    with _legacy_layout():
        with litelink.new(
            tmp_path,
            "s",
            schema=SCHEMA,
            sort_by=("event_ts",),
            # Compaction on and cheap: `sync` only pushes COMPACTED files, so
            # a high `compact_min_files` would leave the archive empty.
            config=LogConfig(
                target_seal_size=1024,
                target_compact_size=2048,
                compact_min_files=2,
                snapshot_retention=timedelta(seconds=0),
            ),
            archive=where,
            s3=s3,
        ) as log:
            log.extend(rows(400))
            while log.seal() is not None:
                pass

            log.maintain()
            log.sync()
            archived = log.archived_through()

        assert archived > 0, "nothing reached the archive to migrate"

    filesystem = _s3_filesystem(s3)
    legacy_objects = _entries(f"{where}/{NAMESPACE}/s/metadata", s3)
    assert legacy_objects, "the fixture put no metadata at the legacy prefix"

    plan = migrate(tmp_path, "s", archive=where, s3=s3)

    assert any(step.action == "archive" for step in plan.steps)
    assert not _entries(f"{where}/{NAMESPACE}/s/metadata", s3), "legacy prefix drained"
    assert _entries(f"{where}/s/metadata", s3), "and the new one is populated"

    # Data objects were never touched, which is the claim that keeps this cheap.
    data = [
        info.path
        for info in filesystem.get_file_info(
            FileSelector(f"{bucket}/legacy/s/data", recursive=True)
        )
        if info.is_file
    ]
    assert data, "data objects must still be where they always were"

    with litelink.open(tmp_path, "s", s3=s3) as reopened:
        assert reopened.archived_through() == archived
        assert reopened.scan(include_archive=True).read_all().num_rows == 400

    connection = duckdb.connect()
    load_extension(connection, "iceberg", remote=False)
    load_extension(connection, "httpfs", remote=True)
    connection.execute(secret_sql(s3))
    counted = connection.execute(
        f"SELECT count(*) FROM iceberg_scan('{where}/s',"
        " version_name_format = '%s%s.metadata.json')"
    ).fetchone()

    assert counted is not None
    assert counted[0] == archived, "an outside engine reads the moved archive"


def test_the_catalog_copy_sees_commits_still_in_the_wal(tmp_path: Path) -> None:
    """These databases run in WAL mode, so the newest row is not in the file.

    `shutil.copyfile` takes `catalog.db` and leaves `catalog.db-wal` behind,
    which on a live log silently rewinds the catalog to its last checkpoint —
    and the row it rewinds is `metadata_location`, the one thing this migration
    exists to move. The copy goes through SQLite's backup API for that reason.
    """
    import sqlite3

    source = tmp_path / "catalog.db"
    setup = sqlite3.connect(source)
    setup.execute(
        "CREATE TABLE iceberg_tables (catalog_name TEXT, table_namespace TEXT,"
        " table_name TEXT, metadata_location TEXT, previous_metadata_location TEXT)"
    )
    setup.execute(
        "INSERT INTO iceberg_tables VALUES ('local', 'litelink', 's', 'old', NULL)"
    )
    setup.commit()
    setup.execute("PRAGMA journal_mode=WAL")
    setup.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    setup.close()

    # A connection that stays open with checkpointing off, so the update below
    # lives in `-wal` and nowhere else — which is the state a running log's
    # catalog is in whenever this migration might be pointed at it.
    live = sqlite3.connect(source)
    live.execute("PRAGMA wal_autocheckpoint=0")
    live.execute("UPDATE iceberg_tables SET metadata_location = 'new'")
    live.commit()

    assert (tmp_path / "catalog.db-wal").exists(), "the update must be in the WAL"

    try:
        _split_catalog(source, tmp_path / "copy.db", "local", "s", None)
    finally:
        live.close()

    copy = sqlite3.connect(tmp_path / "copy.db")
    landed = copy.execute("SELECT metadata_location FROM iceberg_tables").fetchall()
    copy.close()

    assert landed == [("new",)], (
        "the copy rewound to the last checkpoint and lost the current pointer"
    )


@pytest.mark.s3
def test_dropping_the_legacy_wal_is_a_second_pass_on_a_migrated_log(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """`--drop-legacy-wal` runs AFTER the layout has already moved.

    That is the documented sequence — migrate, restart the sidecar so a fresh
    replica lands under `<prefix>/<name>/_wal`, confirm it, only then drop the
    old one — so the second invocation necessarily meets a log that is no
    longer legacy. `migrate` branched on "the plan has steps" rather than on
    "the layout is legacy", so the second pass either did nothing or, once the
    drop itself became a step, ran the whole migration again over a tree that
    had already moved.

    And the drop has to remove the zero-byte `.../` keys object stores leave
    behind. A delete pass filtered on `is_file` classifies every one of them as
    a directory, deletes nothing, and reports success.
    """
    where = f"s3://{bucket}/second-pass"
    _build_legacy(tmp_path)

    filesystem = _s3_filesystem(s3)
    legacy_wal = f"{bucket}/second-pass/_wal"
    # The 0.1 replica shape: keys are ROOT-relative, so the buffer sits under
    # the stream name while the two shared catalogs sit directly under `_wal`.
    for key in ("s/buffer.db", "catalog.db", "archive.db"):
        with filesystem.open_output_stream(f"{legacy_wal}/{key}/0000/x.ltx") as handle:
            handle.write(b"pretend-ltx")

    assert _prefix_holds(f"s3://{legacy_wal}", s3)

    migrate(tmp_path, "s", archive=where, s3=s3)

    assert not is_legacy(tmp_path, "s")
    assert _prefix_holds(f"s3://{legacy_wal}", s3), (
        "the migration must NOT drop the old replica: between it and a fresh "
        "one there is a window where it is the only off-box copy"
    )

    # Refused until a REPLACEMENT exists, which is the same window asked of the
    # flag: with the old replica gone and the sidecar not yet restarted, the
    # log has no off-box copy of its unsealed rows at all.
    premature = migrate(tmp_path, "s", archive=where, s3=s3, drop_legacy_wal=True)

    assert premature.blockers, "nothing may be dropped before a fresh replica"
    assert _prefix_holds(f"s3://{legacy_wal}", s3)

    # Stand in for the sidecar having run against the new config.
    with filesystem.open_output_stream(
        f"{bucket}/second-pass/s/_wal/buffer.db/0000/x.ltx"
    ) as handle:
        handle.write(b"pretend-ltx")

    plan = migrate(tmp_path, "s", archive=where, s3=s3, drop_legacy_wal=True)

    assert not _prefix_holds(f"s3://{legacy_wal}", s3), "second pass drops it"
    assert any("dropped the legacy WAL" in note for note in plan.notes)
    with litelink.open(tmp_path, "s", read_only=True) as log:
        assert log.scan().read_all().num_rows == 400, "and touches nothing else"


@pytest.mark.s3
def test_migrating_one_stream_leaves_a_siblings_archive_intact(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Two streams, one archive prefix — the ordinary 0.1 arrangement.

    Sharing a prefix was normal then: the catalogs and the WAL replica were
    shared per root anyway, and data objects kept their root-relative names —
    namespaced by stream — so nothing collided. That makes `<prefix>/litelink/` a SHARED directory
    holding every stream's metadata.

    A sweep of the namespace after moving one stream therefore deleted every
    other stream's manifests. Their data objects survive with nothing left to
    name them — the offset-to-file mapping lived only in those manifests — and
    the next migration reports "no archive metadata found ... nothing to move
    remotely", rewrites the pointer, prints "verified: N rows before and
    after" from the LOCAL tier alone, and exits 0.
    """
    where = f"s3://{bucket}/shared-prefix"
    settings = LogConfig(
        target_seal_size=1024,
        target_compact_size=2048,
        compact_min_files=2,
        snapshot_retention=timedelta(seconds=0),
    )
    with _legacy_layout():
        for name in ("trades", "quotes"):
            with litelink.new(
                tmp_path,
                name,
                schema=SCHEMA,
                sort_by=("event_ts",),
                config=settings,
                archive=where,
                s3=s3,
            ) as log:
                log.extend(rows(400))
                while log.seal() is not None:
                    pass

                log.maintain()
                log.sync()
                assert log.archived_through() > 0

    sibling = f"{where}/{NAMESPACE}/quotes/metadata"
    before = _entries(sibling, s3)
    assert before, "the fixture archived nothing for the sibling"

    migrate(tmp_path, "trades", archive=where, s3=s3)

    assert _entries(sibling, s3) == before, (
        "migrating one stream must not touch another stream's archive metadata"
    )

    # And the sibling still migrates on its own terms afterwards.
    plan = migrate(tmp_path, "quotes", archive=where, s3=s3)

    assert any(step.action == "archive" for step in plan.steps), (
        "the sibling's archive metadata must still be there to move"
    )
    assert not _entries(sibling, s3), "and is drained once it does move"
    with litelink.open(tmp_path, "quotes", s3=s3) as log:
        assert log.scan(include_archive=True).read_all().num_rows == 400


def test_an_archive_without_credentials_is_refused(tmp_path: Path) -> None:
    """Rather than rewriting a pointer to a location nothing was copied to.

    `migrate` is exported, so a caller can pass `archive=` and omit `s3=`. The
    remote move is guarded on the options; the `archive.db` pointer rewrite was
    guarded only on the prefix being non-empty, so the catalog ended up naming
    `<prefix>/<name>/metadata` while every object still sat under the old one.
    """
    _build_legacy(tmp_path)

    plan = migrate(tmp_path, "s", archive="s3://bucket/prefix")

    assert plan.blockers
    assert is_legacy(tmp_path, "s"), "and nothing was changed"


@pytest.mark.s3
def test_restore_refuses_a_live_pre_0_2_log(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The destructive one, and the reason `exists_for` now raises.

    A pre-0.2 log keeps its catalog at the root, so `<name>/catalog.db` is
    missing and `LogTable.exists_for` used to report the log ABSENT. `restore`
    reads that as "an interrupted restore to resume", skips the guard that
    refuses to overwrite a live log, and runs `strip_local_state`: 2**20
    offsets burned on a log still being written to, every `extent` row deleted,
    `sealing` and `claim` wiped, and a fresh empty catalog written beside the
    real one — while `restore` returns a handle and reports the loss only as a
    `skipped` range.

    A REAL archive, not a bogus URI. With an unreachable one `restore` fails
    early for its own reasons, so the test would pass whether or not the guard
    is there — which is the only thing it exists to check.
    """
    where = f"s3://{bucket}/refuse-restore"
    with _legacy_layout():
        with litelink.new(
            tmp_path,
            "s",
            schema=SCHEMA,
            sort_by=("event_ts",),
            config=LogConfig(
                target_seal_size=1024,
                target_compact_size=2048,
                compact_min_files=2,
                wal_replication=True,
            ),
            archive=where,
            s3=s3,
        ) as log:
            log.extend(rows(400))
            while log.seal() is not None:
                pass

            log.maintain()
            log.sync()
            before, archived = log.end_offset(), log.archived_through()

        assert archived > 0, "the fixture archived nothing, so restore would no-op"

    with pytest.raises((FileExistsError, FileNotFoundError), match="pre-0.2"):
        litelink.restore(tmp_path, "s", archive=where, s3=s3)

    with _legacy_layout(), litelink.open(tmp_path, "s", s3=s3) as intact:
        assert intact.end_offset() == before, "no offsets were burned"
        assert intact.scan().read_all().num_rows == 400, "and no rows were lost"


def test_both_open_paths_name_the_migration(tmp_path: Path) -> None:
    """Read-only and writable `open` must diagnose the same directory alike."""
    _build_legacy(tmp_path)

    for readonly in (True, False):
        with pytest.raises(FileNotFoundError, match="pre-0.2") as caught:
            litelink.open(tmp_path, "s", read_only=readonly)

        assert "python -m litelink.migrate" in str(caught.value)


@pytest.mark.s3
def test_dropping_the_legacy_wal_is_refused_while_a_sibling_needs_it(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """`<prefix>/_wal` is shared per root, like `catalog.db` is.

    It holds `catalog.db`, `archive.db` AND every stream's `buffer.db` replica,
    keyed by the path relative to the ROOT. A stream that has not migrated
    still has its only off-box copy of unsealed rows in there — rows that are
    by definition in no Parquet file and no archive manifest, which is the
    whole reason `wal_replication` exists — so dropping it early is
    unrecoverable loss rather than untidiness.

    Refused loudly rather than skipped: the operator asked for the deletion.
    """
    where = f"s3://{bucket}/shared-wal"
    _build_legacy(tmp_path, "trades", count=200)
    _build_legacy(tmp_path, "quotes", count=200)

    filesystem = _s3_filesystem(s3)
    legacy_wal = f"{bucket}/shared-wal/_wal"
    for key in ("catalog.db", "trades/buffer.db", "quotes/buffer.db"):
        with filesystem.open_output_stream(f"{legacy_wal}/{key}/0000/x.ltx") as handle:
            handle.write(b"pretend-ltx")

    migrate(tmp_path, "trades", archive=where, s3=s3)
    with filesystem.open_output_stream(
        f"{bucket}/shared-wal/trades/_wal/buffer.db/0000/x.ltx"
    ) as handle:
        handle.write(b"pretend-ltx")

    refused = migrate(tmp_path, "trades", archive=where, s3=s3, drop_legacy_wal=True)

    assert refused.blockers, "must refuse while quotes still replicates through it"
    assert any("quotes" in blocker for blocker in refused.blockers)
    assert _prefix_holds(f"s3://{legacy_wal}", s3), "and delete nothing"

    # Once the sibling has moved — and once a fresh replica exists for it —
    # this root's keys under the shared replica are nobody's and may go.
    migrate(tmp_path, "quotes", archive=where, s3=s3)
    with filesystem.open_output_stream(
        f"{bucket}/shared-wal/quotes/_wal/buffer.db/0000/x.ltx"
    ) as handle:
        handle.write(b"pretend-ltx")

    allowed = migrate(tmp_path, "quotes", archive=where, s3=s3, drop_legacy_wal=True)

    assert not allowed.blockers
    assert not _prefix_holds(f"s3://{legacy_wal}/quotes", s3), "its own goes"
    assert not _prefix_holds(f"s3://{legacy_wal}/catalog.db", s3), "shared too"

    # `trades` keeps its buffer replica until `trades` is dropped in turn. The
    # drop deletes the keys it can NAME — this stream's buffer, plus whatever
    # the shared catalog still lists — rather than the prefix, because nothing
    # binds an archive prefix to one root and a wholesale delete would reach
    # another root's replicas. A stream that migrated before the shared catalog
    # was removed is no longer nameable, so it is left for its own pass.
    assert _prefix_holds(f"s3://{legacy_wal}/trades", s3)

    migrate(tmp_path, "trades", archive=where, s3=s3, drop_legacy_wal=True)

    assert not _prefix_holds(f"s3://{legacy_wal}", s3), "one pass per stream"
