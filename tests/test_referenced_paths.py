"""Deletion vetoes keep live history without building file-inspection tables."""

from __future__ import annotations

from collections import Counter
from datetime import timedelta
from pathlib import Path

import pytest
from pyiceberg.manifest import ManifestEntryStatus, ManifestFile
from pyiceberg.table.inspect import InspectTable
from pyiceberg.table.snapshots import Snapshot

import litelink
from litelink import LogConfig
from litelink._table import METADATA_PROPERTIES, LogTable
from tests.test_log import open_log, rows


def inspected_paths(table: LogTable) -> set[str]:
    """The old veto's result, kept as an independent compatibility oracle."""
    return {
        table._name(path)
        for path in table._table.inspect.all_files()["file_path"].to_pylist()
    } | table.metadata_paths(table._table.snapshots())


@pytest.mark.parametrize("tier", ["staging", "published"])
def test_references_match_inspection_through_history_and_expiry(
    tmp_path: Path, tier: str
) -> None:
    """I6: an old snapshot's files survive until that snapshot expires."""
    with open_log(tmp_path, LogConfig(target_seal_size=1 << 30)) as log:
        table = log._table if tier == "staging" else log._published.require()
        assert table.referenced_paths() == inspected_paths(table) == set()
        for batch in range(3):
            log.extend(rows(2, start=batch * 2))
            log.seal(flush=True)
            if tier == "published":
                log.publish(flush=True)

        original = table.file_paths()
        table.evict_below(5)
        historical = original - table.file_paths()
        assert historical
        before = table.referenced_paths()
        assert before == inspected_paths(table)
        assert historical <= before

        current = table.current_snapshot()
        assert current is not None
        table.expire_snapshots(
            [
                snapshot.snapshot_id
                for snapshot in table._table.snapshots()
                if snapshot.snapshot_id != current.snapshot_id
            ]
        )
        deleted = {
            table._name(entry.data_file.file_path)
            for snapshot in table._table.snapshots()
            for manifest in snapshot.manifests(table._table.io)
            for entry in manifest.fetch_manifest_entry(
                table._table.io, discard_deleted=False
            )
            if entry.status == ManifestEntryStatus.DELETED
        }
        assert historical <= deleted
        after = table.referenced_paths()
        assert after == inspected_paths(table)
        assert not historical & after
        assert table.file_paths() <= after


@pytest.mark.parametrize("tier", ["staging", "published"])
def test_reference_walk_reads_each_manifest_once_without_arrow_inspection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tier: str
) -> None:
    """Shared manifests cost one decode; no full-history Arrow allocation."""
    monkeypatch.setitem(METADATA_PROPERTIES, "commit.manifest-merge.enabled", "false")
    with open_log(
        tmp_path,
        LogConfig(
            target_seal_size=1 << 30,
            published_snapshot_retention=timedelta(days=365),
        ),
    ) as log:
        table = log._table if tier == "staging" else log._published.require()
        for batch in range(3):
            log.extend(rows(2, start=batch * 2))
            log.seal(flush=True)
            if tier == "published":
                log.publish(flush=True)

        expected = inspected_paths(table)
        snapshots = table._table.snapshots()
        manifest_paths = [
            manifest.manifest_path
            for snapshot in snapshots
            for manifest in snapshot.manifests(table._table.io)
        ]
        assert len(manifest_paths) > len(set(manifest_paths))
        entries = Counter()
        lists = Counter()
        fetch = ManifestFile.fetch_manifest_entry
        manifests = Snapshot.manifests

        def count_entries(self, io, discard_deleted=True):
            assert io is table._table.io
            entries[self.manifest_path] += 1
            return fetch(self, io, discard_deleted=discard_deleted)

        def count_lists(self, io):
            lists[self.snapshot_id] += 1
            return manifests(self, io)

        def no_inspection(self):
            pytest.fail("reference collection must not materialize Arrow inspection")

        monkeypatch.setattr(ManifestFile, "fetch_manifest_entry", count_entries)
        monkeypatch.setattr(Snapshot, "manifests", count_lists)
        monkeypatch.setattr(InspectTable, "all_files", no_inspection)
        assert table.referenced_paths() == expected
        assert entries == Counter(dict.fromkeys(manifest_paths, 1))
        assert lists == Counter({snapshot.snapshot_id: 1 for snapshot in snapshots})


def test_published_drain_reloads_before_collecting_references(tmp_path: Path) -> None:
    """I6: a handle opened before a publish must veto its newly live files."""
    with open_log(tmp_path, LogConfig(target_seal_size=1 << 30)) as writer:
        with litelink.open(tmp_path, "s") as cleaner:
            stale = cleaner._published.require()
            assert stale.file_paths() == set()
            writer.extend(rows(2))
            writer.seal(flush=True)
            writer.publish(flush=True)
            live = writer._published.require().file_paths()
            assert live
            assert stale.file_paths() == set()
            writer._buffer.enqueue_deletions(live, superseded_at=0)

            cleaner._maintenance.drain_published()

            assert live <= set(cleaner._buffer.queued_deletions())
            assert all(Path(path.removeprefix("file://")).exists() for path in live)
            assert live <= stale.referenced_paths()


def test_manifest_read_failure_prevents_published_deletes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An incomplete reference set must never authorize a delete (I6)."""
    with open_log(tmp_path, LogConfig(target_seal_size=1 << 30)) as log:
        log.extend(rows(2))
        log.seal(flush=True)
        log.publish(flush=True)
        live = log._published.require().file_paths()
        assert live
        log._buffer.enqueue_deletions(live, superseded_at=0)

        def unreadable(self, io, discard_deleted=True):
            raise OSError("manifest read failed")

        monkeypatch.setattr(ManifestFile, "fetch_manifest_entry", unreadable)
        with pytest.raises(OSError, match="manifest read failed"):
            log._maintenance.drain_published()

        assert live <= set(log._buffer.queued_deletions())
        assert all(Path(path.removeprefix("file://")).exists() for path in live)
