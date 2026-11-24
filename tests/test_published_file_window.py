"""Publish retains its active window, while Iceberg holds the history (#228)."""

from __future__ import annotations

import gc
from collections.abc import Iterable, Iterator
from contextlib import ExitStack
from datetime import timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.manifest import ManifestFile
from pyiceberg.table.inspect import InspectTable

import litelink
from litelink import OFFSET, LogConfig, WriteHandle
from litelink._buffer import Buffer
from litelink._handle import table_schema
from litelink._table import DataFile, LogTable

SCHEMA = pa.schema([pa.field("payload", pa.string())])


def new_log(root: Path, name: str = "s") -> WriteHandle:
    return litelink.new(
        root,
        name,
        schema=SCHEMA,
        config=LogConfig(
            target_seal_size=1 << 20,
            target_compact_size=1,
            staging_retention=timedelta(0),
            staging_snapshot_retention=timedelta(0),
            published_snapshot_retention=timedelta(0),
        ),
    )


def seal(log: WriteHandle, count: int = 2) -> None:
    log.extend([{"payload": "x" * 32}] * count)
    log.seal(flush=True)


def published_offsets(log: WriteHandle) -> list[int]:
    return sorted(
        log._published.require()._table.scan().to_arrow().column(OFFSET).to_pylist()
    )


def retained_summaries(log: WriteHandle) -> list[DataFile]:
    """Follow instance fields and containers, without entering module globals.

    All litelink collaborators reachable from the writer count, including the
    reader and maintenance handles; a relocated history cache still fails L1.
    """
    pending: list[object] = [log]
    seen: set[int] = set()
    found = []
    while pending:
        value = pending.pop()
        if id(value) in seen:
            continue

        seen.add(id(value))
        if isinstance(value, DataFile):
            found.append(value)
        elif isinstance(value, dict):
            pending.extend(value.values())
        elif isinstance(value, (list, tuple, set, frozenset)):
            pending.extend(value)
        elif type(value).__module__.startswith("litelink.") and hasattr(
            value, "__dict__"
        ):
            pending.extend(vars(value).values())

    return found


@pytest.mark.parametrize("flush", [True, False])
def test_publish_retains_no_published_history(tmp_path: Path, flush: bool) -> None:
    """L1: no summaries reachable through collaborators, even after rare reads."""
    with ExitStack() as stack:
        logs = [stack.enter_context(new_log(tmp_path, f"s{i}")) for i in range(5)]
        for cycle in range(12):
            for log in logs:
                seal(log)
                log.publish(flush=flush)
                log.evict("buffer")
                log.evict("staging")
                # Exercise the rare whole-list caller too: it must not pin a
                # history-sized list on the published handle between passes.
                assert len(log._published.require().data_files()) == cycle + 1
                assert log._table.data_files() == []
                assert log._buffer.intents(log.published) == []

            gc.collect()
            retained = [file for log in logs for file in retained_summaries(log)]
            assert retained == [], f"cycle {cycle}: retained {len(retained)} summaries"


def crash_after_register(log: WriteHandle, monkeypatch: pytest.MonkeyPatch) -> None:
    real = Buffer.confirm_published

    def crash(self: Buffer, through: int, landed: Iterable[str] = ()) -> None:
        if through > 0:
            raise RuntimeError("lost register acknowledgement")

        real(self, through, landed)

    with monkeypatch.context() as patch:
        patch.setattr(Buffer, "confirm_published", crash)
        with pytest.raises(RuntimeError, match="lost register acknowledgement"):
            log.publish(flush=True)

    assert log.published_through() == 0
    assert log._buffer.intents(log.published)


def test_push_reconciles_by_path_after_register_before_confirm(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """L2: an existing-schema intent survives restart, without a second upload (I4)."""
    with new_log(tmp_path) as log:
        seal(log)
        crash_after_register(log, monkeypatch)
        landed = {path for path, *_ in log._buffer.intents(log.published)}

    with litelink.open(tmp_path, "s") as log:
        confirmed: set[str] = set()
        real = Buffer.confirm_published

        def confirm(self: Buffer, through: int, paths: Iterable[str] = ()) -> None:
            paths = list(paths)
            confirmed.update(paths)
            real(self, through, paths)

        def no_upload(*args: Any, **kwargs: Any) -> None:
            pytest.fail("reconciliation uploaded a file already registered")

        monkeypatch.setattr(Buffer, "confirm_published", confirm)
        monkeypatch.setattr(LogTable, "put", no_upload)
        log.publish(flush=False)
        assert confirmed == landed
        assert log.published_through() == 2
        assert log._buffer.intents(log.published) == []
        assert published_offsets(log) == [1, 2]
        assert log._published.require()._files == []


def test_dead_intent_below_staging_is_retired(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """L3: another path covering the same offsets does not confirm a dead copy."""
    with new_log(tmp_path) as log:
        seal(log)
        log.publish(flush=True)
        log.evict("staging")
        seal(log)
        dead = log._published.require().uri("data/never-registered.parquet")
        log._buffer.intend_file(dead, 1, 3, 64)
        assert log._published.require().paths_among([dead]) == set()
        assert log._table.span() == (3, 5)
        forgotten: list[str] = []
        real = Buffer.forget_intent

        def forget(self: Buffer, path: str) -> None:
            forgotten.append(path)
            real(self, path)

        monkeypatch.setattr(Buffer, "forget_intent", forget)
        log.publish(flush=False)
        assert forgotten == [dead]
        assert log._buffer.intents(log.published) == []
        assert log.published_through() == 4
        assert published_offsets(log) == [1, 2, 3, 4]


def test_manifest_failure_preserves_intents_and_watermark(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A partial walk is an error, never evidence that an intent is absent."""
    with new_log(tmp_path) as log:
        seal(log)
        crash_after_register(log, monkeypatch)
        published = log._published.require()
        published.span()  # Cache the span so the injected failure is in path matching.
        log._buffer.intend_file(published.uri("data/dead.parquet"), 1, 3, 64)
        before = log._buffer.intents(log.published)
        real = ManifestFile.fetch_manifest_entry
        visited: list[str] = []

        def fail_mid_walk(
            self: ManifestFile, *args: Any, **kwargs: Any
        ) -> Iterator[Any]:
            for entry in real(self, *args, **kwargs):
                visited.append(str(entry.data_file.file_path))
                yield entry
                raise OSError("manifest read refused")

        with monkeypatch.context() as patch:
            patch.setattr(ManifestFile, "fetch_manifest_entry", fail_mid_walk)
            with pytest.raises(OSError, match="manifest read refused"):
                log.publish(flush=False)

        assert visited
        assert log.published_through() == 0
        assert log._buffer.intents(log.published) == before
        log.publish(flush=False)
        assert log.published_through() == 2
        assert log._buffer.intents(log.published) == []


def write_merged(log: WriteHandle, name: str, start: int, end: int) -> str:
    path = log._layout.directory / "data" / f"{name}.parquet"
    pq.write_table(
        pa.Table.from_pydict(
            {OFFSET: list(range(start, end)), "payload": ["x" * 32] * (end - start)},
            schema=table_schema(SCHEMA),
        ),
        path,
    )
    return str(path)


def test_swap_matches_only_aligned_files(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """L4: swap one aligned candidate; keep the misaligned originals live (I6)."""
    with new_log(tmp_path) as log:
        for count in (1, 1, 2, 2):
            seal(log, count)

        log.publish(flush=True)
        published = log._published.require()
        originals = published.data_files()
        aligned = write_merged(log, "aligned", 1, 3)
        misaligned = write_merged(log, "misaligned", 3, 6)
        log._table.replace_range(1, 7, [aligned, misaligned])
        rels = [log._layout.relative(path) for path in (aligned, misaligned)]
        log._buffer.record_merge([], [], swap=rels)

        # The helper includes both boundaries only on genuine overlap; every
        # per-candidate replaced list is preserved by the union-window read.
        assert published.files_overlapping(2, 3) == [originals[1]]
        log._table.data_files()  # Populate staging's bounded cache before the sentinel.

        def no_inspection(*args: Any, **kwargs: Any) -> None:
            pytest.fail("swap materialized whole-history file metrics")

        with monkeypatch.context() as patch:
            patch.setattr(InspectTable, "files", no_inspection)
            log.publish(flush=True)

        current = published.data_files()
        assert [(f.start, f.end) for f in current] == [(1, 3), (3, 5), (5, 7)]
        assert current[0].path == published.uri(rels[0])
        assert current[1:] == originals[2:]
        assert "do not line up" in caplog.text
        assert published_offsets(log) == list(range(1, 7))
        # Old snapshots still protect the aligned inputs queued by the swap.
        assert set(log._buffer.due_deletions(2**63 - 1)) == {
            f.path for f in originals[:2]
        }
        referenced = published.referenced_paths()
        assert {f.path for f in originals} <= referenced
        log._maintenance.drain_published(grace=timedelta(0))
        assert all(Path(f.path.removeprefix("file://")).exists() for f in originals)


def test_publish_never_builds_inspect_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """L5: ordinary publishes and empty reconciliation avoid Arrow file metrics."""
    with new_log(tmp_path) as log:
        seal(log)
        log._table.data_files()  # Staging's bounded cache is allowed.

        def forbidden(*args: Any, **kwargs: Any) -> None:
            pytest.fail("publish materialized whole-history file metrics")

        monkeypatch.setattr(InspectTable, "files", forbidden)
        log.publish(flush=False)
        log.publish(flush=False)
        assert log.published_through() == 2
        monkeypatch.setattr(ManifestFile, "fetch_manifest_entry", forbidden)
        assert log._published.require().paths_among([]) == set()
