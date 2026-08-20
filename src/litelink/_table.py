"""The local Iceberg table, and the pyiceberg calls that reach it.

Everything that knows pyiceberg's shape lives here, so the rest of the library
deals in offsets, paths and extents. Several methods exist only because
pyiceberg's own behaviour needed working around — each says which.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.conversions import from_bytes
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.transforms import IdentityTransform

from litelink._predicates import offset_at_or_below, offset_between

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence
    from datetime import datetime

    import pyarrow as pa
    from pyiceberg.table import Table
    from pyiceberg.table.snapshots import Snapshot

    from litelink._layout import Layout

# Iceberg keeps every metadata.json ever written unless told otherwise, which on
# a stream that seals every few minutes is a file per seal, forever. These bound
# it to the current version plus a few for rollback. Manifests are NOT covered —
# see `expire_snapshots`.
# Bounded: a commit that keeps losing is contention worth surfacing, not
# something to retry forever behind a caller's back.
_COMMIT_ATTEMPTS = 5

METADATA_PROPERTIES = {
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "10",
    # Each commit otherwise writes its own manifest, and litelink commits one
    # file per seal — so a table of N data files becomes N manifest avro files,
    # and deriving the tier boundary means opening every one of them. Measured
    # at 60 files: 60 manifests and a 45 ms boundary read, against 1 manifest
    # and 2.3 ms with merging on.
    #
    # It is not a trade against write cost, which is what makes the choice easy.
    # Accumulated manifests slow every later commit too, since each rewrites a
    # manifest list naming all of them: the same 60 seals ran at a 110 ms median
    # unmerged and 65 ms merged.
    #
    # min-count 2 merges on essentially every commit, which is as close as this
    # property gets to the one manifest a stream actually wants. Iceberg's
    # default is 100 — sized for batch jobs, and hours of accumulation for a
    # stream that seals every few minutes.
    "commit.manifest-merge.enabled": "true",
    "commit.manifest.min-count-to-merge": "2",
}


@dataclass(frozen=True, slots=True)
class DataFile:
    """One Iceberg data file, as the maintenance passes need to see it."""

    path: str
    size: int
    rows: int
    lo: int
    hi: int


class LogTable:
    """The local Iceberg table for one log.

    Holds a pyiceberg `Table`, which is a snapshot-in-time view, and reloads it
    whenever the current state matters. §7 is explicit that a cached pointer
    silently serves a stale snapshot.
    """

    def __init__(self, catalog: SqlCatalog, layout: Layout, table: Table) -> None:
        """Take the loaded table. `create` and `load` are what load it.

        Assigning only, so that a caller holding a `Table` from anywhere — a
        fixture, a different catalog — can build one of these around it.
        """
        self._catalog = catalog
        self._layout = layout
        # Guards the handle below and the caches keyed off it — NOT the work
        # done with them. A compaction reads files, writes Parquet and commits;
        # only the commit and the cache reads belong in here. Holding it across
        # the whole pass is what made a read wait 21.5 s behind one.
        #
        # Safe to hold so briefly because a pyiceberg table is an immutable
        # view of one snapshot: a caller that took the handle keeps working
        # against the snapshot it read, and `reload` only changes which
        # snapshot the NEXT caller sees.
        self._lock = threading.RLock()
        self._table = table
        # Snapshot-derived facts, cached against the metadata pointer. See
        # `extent`. The file count rides along because reading the manifests
        # produces it for free, and counting files any other way means
        # materialising every file's metadata.
        # Two caches, both keyed by the metadata pointer, because they come from
        # different depths of the metadata tree. Counts are summarised in the
        # manifest list; per-file bounds are only in the manifest entries, which
        # means opening each manifest.
        self._extent_at: str | None = None
        self._extent: tuple[int, int] | None = None
        self._counts_at: str | None = None
        self._file_count = 0
        self._record_count = 0

    @classmethod
    def create(
        cls, layout: Layout, schema: pa.Schema, sort_by: Sequence[str]
    ) -> LogTable:
        """Create the table and declare its sort order.

        §4 wants the order declared as table metadata AND applied at write
        time. The declaration is what makes `sort_by` recoverable by `open`,
        rather than something the caller has to restate identically forever.
        """
        catalog = cls._catalog_for(layout)
        catalog.create_namespace_if_not_exists(layout.table_id.split(".")[0])
        catalog.create_table(
            layout.table_id, schema=schema, properties=METADATA_PROPERTIES
        )

        table = cls(catalog, layout, catalog.load_table(layout.table_id))
        table.set_sort_order(sort_by)

        return table

    @classmethod
    def load(cls, layout: Layout, *, readonly: bool) -> LogTable:
        """Load an existing table. Raises if there is none."""
        catalog = cls._catalog_for(layout)
        table = cls(catalog, layout, catalog.load_table(layout.table_id))
        if not readonly:
            table.ensure_metadata_properties()

        return table

    @staticmethod
    def _catalog_for(layout: Layout) -> SqlCatalog:
        return SqlCatalog(
            "local", uri=layout.catalog_uri, warehouse=layout.warehouse_uri
        )

    def exists(self) -> bool:
        return True

    # -- shape --------------------------------------------------------------

    def arrow_schema(self) -> pa.Schema:
        """The table's schema as pyarrow, `offset` included.

        Not byte-identical to what was passed to `create`: Iceberg has one
        string type, so a pyarrow `string` comes back as `large_string`. The
        logical schema is the same, and both map to the same SQLite affinity
        and the same DuckDB cast, but an equality assertion against the
        original will fail.
        """
        return schema_to_pyarrow(self._table.schema())

    def sort_by(self) -> tuple[str, ...]:
        """The declared sort order, as column names (§4)."""
        names = {f.field_id: f.name for f in self._table.schema().fields}

        return tuple(names[f.source_id] for f in self._table.sort_order().fields)

    def set_sort_order(self, sort_by: Sequence[str]) -> None:
        """Declare the sort order. Does NOT reorder existing data."""
        if not sort_by:
            return

        self._commit(lambda: self._apply_sort_order(sort_by))

    def _apply_sort_order(self, sort_by: Sequence[str]) -> None:
        with self._table.update_sort_order() as update:
            for column in sort_by:
                update.asc(column, IdentityTransform())

    # -- state ------------------------------------------------------------

    def reload(self) -> None:
        table = self._catalog.load_table(self._layout.table_id)
        with self._lock:
            self._table = table

    @property
    def metadata_location(self) -> str:
        return str(self._table.metadata_location)

    @property
    def properties(self) -> dict[str, str]:
        return dict(self._table.properties)

    def is_empty(self) -> bool:
        return self._table.current_snapshot() is None

    def snapshot_count(self) -> int:
        """How many snapshots the table still carries — expiry's visible effect."""
        return int(self._table.inspect.snapshots().num_rows)

    def offset_field_id(self) -> int:
        return self._table.schema().find_field("litelink_offset").field_id

    # -- reads from statistics --------------------------------------------

    def data_files(self) -> list[DataFile]:
        """Current data files with their offset extents, ordered by offset.

        Extents come from manifest column statistics, so no data file is
        opened — which is what makes the tier boundary cheap enough for §7 to
        derive it on every read.
        """
        if self.is_empty():
            return []

        files = self._table.inspect.files()
        field = self._table.schema().find_field("litelink_offset")
        found = [
            DataFile(
                path=_local(path),
                size=int(size),
                rows=int(rows),
                lo=from_bytes(field.field_type, dict(lower)[field.field_id]),
                hi=from_bytes(field.field_type, dict(upper)[field.field_id]),
            )
            for path, size, rows, lower, upper in zip(
                files["file_path"].to_pylist(),
                files["file_size_in_bytes"].to_pylist(),
                files["record_count"].to_pylist(),
                files["lower_bounds"].to_pylist(),
                files["upper_bounds"].to_pylist(),
                strict=True,
            )
        ]

        return sorted(found, key=lambda f: f.lo)

    def extent(self) -> tuple[int, int] | None:
        """`(lo, hi)` over the current snapshot — §7's tier boundary.

        Cached against `metadata_location`, which is the version pointer: an
        unchanged pointer is the same snapshot, so the extent cannot have moved.
        This does not weaken §7's "resolve per query, never pin" — the resolve
        still happens, at ~0.5 ms, and is what decides whether the cache stands.

        The caching is not an optimisation so much as a correction. §7 describes
        this read as ~0.6 ms and "fixed rather than proportional", but reading
        it from manifests costs time proportional to FILE COUNT — measured at
        1.0 ms for one file and 46 ms for 64. Between commits that work is
        entirely redundant, and on a read-heavy log it was nearly all of the
        per-query overhead.
        """
        with self._lock:
            self._refresh()

            return self._extent

    def file_count(self) -> int:
        """How many data files the current snapshot holds.

        From the manifest list, which summarises each manifest — so this reads
        one file rather than opening every manifest. Measured at 60 files:
        0.65 ms against 42.9 ms for the entry walk.
        """
        with self._lock:
            self._refresh_counts()

            return self._file_count

    def record_count(self) -> int:
        """How many rows the current snapshot holds.

        Also summarised in the manifest list. Counting by scanning instead reads
        the offset column out of every Parquet file — correct, and proportional
        to the data, which is the wrong shape for anything polling.
        """
        with self._lock:
            self._refresh_counts()

            return self._record_count

    def _refresh_counts(self) -> None:
        location = self.metadata_location
        if location == self._counts_at:
            return

        snapshot = self._table.current_snapshot()
        files = records = 0
        if snapshot is not None:
            for manifest in snapshot.manifests(self._table.io):
                # Live entries are ADDED plus EXISTING; DELETED are tombstones
                # a compaction or eviction left behind.
                files += (manifest.added_files_count or 0) + (
                    manifest.existing_files_count or 0
                )
                records += (manifest.added_rows_count or 0) + (
                    manifest.existing_rows_count or 0
                )

        self._file_count, self._record_count = files, records
        self._counts_at = location

    def _refresh(self) -> None:
        location = self.metadata_location
        if location != self._extent_at:
            self._extent = self._read_extent()
            self._extent_at = location

    def _read_extent(self) -> tuple[int, int] | None:
        """Offset bounds straight off the manifest entries.

        Deliberately not `inspect.files()`, which materialises an 18-column
        Arrow table — including `readable_metrics`, which decodes the bounds of
        every column — to answer a question about one. Measured at roughly half
        the cost across file counts, and it still opens no data file.
        """
        snapshot = self._table.current_snapshot()
        if snapshot is None:
            return None

        field = self._table.schema().find_field("litelink_offset")
        lows: list[int] = []
        highs: list[int] = []
        for manifest in snapshot.manifests(self._table.io):
            for entry in manifest.fetch_manifest_entry(
                self._table.io, discard_deleted=True
            ):
                lows.append(
                    from_bytes(
                        field.field_type, entry.data_file.lower_bounds[field.field_id]
                    )
                )
                highs.append(
                    from_bytes(
                        field.field_type, entry.data_file.upper_bounds[field.field_id]
                    )
                )

        return None if not lows else (min(lows), max(highs))

    def file_paths(self) -> set[str]:
        return {f.path for f in self.data_files()}

    def referenced_paths(self) -> set[str]:
        """Every file any live snapshot needs — data and Iceberg's own."""
        return {
            _local(path)
            for path in self._table.inspect.all_files()["file_path"].to_pylist()
        } | self.metadata_paths(self._table.snapshots())

    def metadata_paths(self, snapshots: Iterable[Snapshot]) -> set[str]:
        """Manifest lists and manifests belonging to `snapshots`.

        Iceberg's own bookkeeping, which `expire_snapshots` does not delete —
        verified against pyiceberg 0.11.1: expiring four of five snapshots left
        all ten avro files on disk. Two per commit accumulates fast on a stream
        that seals every few minutes.
        """
        paths: set[str] = set()
        for snapshot in snapshots:
            paths.add(_local(snapshot.manifest_list))
            paths.update(
                _local(manifest.manifest_path)
                for manifest in snapshot.manifests(self._table.io)
            )

        return paths

    def snapshots_older_than(self, cutoff: datetime) -> list[Snapshot]:
        """Snapshots eligible for expiry, excluding the current one."""
        current = self._table.current_snapshot()
        current_id = None if current is None else current.snapshot_id

        return [
            snapshot
            for snapshot in self._table.snapshots()
            if snapshot.timestamp_ms / 1000 < cutoff.timestamp()
            and snapshot.snapshot_id != current_id
        ]

    def snapshot_ages(self) -> dict[str, datetime]:
        """File path -> when the snapshot that added it committed.

        §2 stamps no ingest column, so a file's age is its snapshot's commit
        time and nothing else.
        """
        snapshots = self._table.inspect.snapshots()
        committed = dict(
            zip(
                snapshots["snapshot_id"].to_pylist(),
                snapshots["committed_at"].to_pylist(),
                strict=True,
            )
        )

        entries = self._table.inspect.entries()
        ages: dict[str, datetime] = {}
        for snapshot_id, data_file in zip(
            entries["snapshot_id"].to_pylist(),
            entries["data_file"].to_pylist(),
            strict=True,
        ):
            added = committed.get(snapshot_id)
            if added is not None:
                ages[_local(data_file["file_path"])] = added

        return ages

    # -- commits ------------------------------------------------------------
    #
    # Iceberg commits are optimistic: the commit asserts the branch has not
    # moved, and pyiceberg raises rather than retrying. §11 already says what
    # should happen instead — "the loser refreshes and retries" — so that lives
    # here rather than in each caller.
    #
    # This is what makes a second process possible. A manager process running
    # maintenance beside a capturing writer loses the race whenever a seal
    # commits mid-compaction; without the retry it dies on
    # CommitFailedException, which is exactly what it did before this existed.

    def _commit(self, operation: Callable[[], None]) -> None:
        """Run a commit, refreshing and retrying if the branch moved under it.

        Safe to replay because a failed commit landed nothing: the range a
        compaction rewrites sits below the boundary a concurrent seal appends
        above, so the second attempt selects the same files as the first.
        """
        for attempt in range(_COMMIT_ATTEMPTS):
            try:
                with self._lock:
                    operation()
            except CommitFailedException:
                if attempt == _COMMIT_ATTEMPTS - 1:
                    raise

                self.reload()
            else:
                self.reload()

                return

    def register(self, path: str) -> None:
        """Add an already-written file to the table (§4 step 2).

        `add_files` rather than `append`: pyiceberg's append writes the file
        itself and commits afterwards, so a crash in between orphans a file
        under a name nothing recorded — exactly what I2 exists to prevent.
        """
        self._commit(lambda: self._table.add_files([path]))

    def replace_range(self, lo: int, hi: int, path: str) -> None:
        """Swap `[lo, hi]` for one already-written file, in one snapshot (§6).

        `overwrite()` would do this in a single call, but it writes the output
        itself — putting a path on disk this process only learns about
        afterwards, which is what the deletion queue exists to avoid.
        """

        def swap() -> None:
            with self._table.transaction() as transaction:
                transaction.delete(delete_filter=offset_between(lo, hi))
                transaction.add_files([path])

        self._commit(swap)

    def evict_through(self, boundary: int) -> None:
        """Drop every file at or below `boundary` from the current snapshot (§8)."""
        self._commit(
            lambda: self._table.delete(delete_filter=offset_at_or_below(boundary))
        )

    def expire_snapshots_older_than(self, cutoff: datetime) -> None:
        """Expire snapshot METADATA. Does not delete any file — see §6."""
        self._commit(
            lambda: (
                self._table.maintenance.expire_snapshots().older_than(cutoff).commit()
            )
        )

    def scan_range(self, lo: int, hi: int) -> pa.Table:
        return self._table.scan(row_filter=offset_between(lo, hi)).to_arrow()

    def ensure_metadata_properties(self) -> None:
        """Apply the metadata-retention properties to a table that predates them."""
        missing = {
            key: value
            for key, value in METADATA_PROPERTIES.items()
            if self._table.properties.get(key) != value
        }
        if not missing:
            return

        self._commit(lambda: self._set_properties(missing))

    def _set_properties(self, properties: dict[str, str]) -> None:
        with self._table.transaction() as transaction:
            transaction.set_properties(properties)


def _local(path: object) -> str:
    """Iceberg records `file://` URIs; the filesystem wants plain paths."""
    return str(path).removeprefix("file://")
