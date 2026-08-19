"""The local Iceberg table, and the pyiceberg calls that reach it.

Everything that knows pyiceberg's shape lives here, so the rest of the library
deals in offsets, paths and extents. Several methods exist only because
pyiceberg's own behaviour needed working around — each says which.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.conversions import from_bytes

from litelink._predicates import offset_at_or_below, offset_between

if TYPE_CHECKING:
    from collections.abc import Iterable
    from datetime import datetime

    import pyarrow as pa
    from pyiceberg.table.snapshots import Snapshot

    from litelink._layout import Layout

# Iceberg keeps every metadata.json ever written unless told otherwise, which on
# a stream that seals every few minutes is a file per seal, forever. These bound
# it to the current version plus a few for rollback. Manifests are NOT covered —
# see `expire_snapshots`.
METADATA_PROPERTIES = {
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "10",
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

    def __init__(self, catalog: SqlCatalog, layout: Layout) -> None:
        self._catalog = catalog
        self._layout = layout
        self._table = catalog.load_table(layout.table_id)
        # Extent cache, keyed by the metadata pointer. See `extent`.
        self._extent_at: str | None = None
        self._extent: tuple[int, int] | None = None

    @classmethod
    def open(cls, layout: Layout, schema: pa.Schema, *, readonly: bool) -> LogTable:
        """Load the table, creating it and its namespace unless readonly."""
        catalog = SqlCatalog(
            "local", uri=layout.catalog_uri, warehouse=layout.warehouse_uri
        )
        if not readonly:
            catalog.create_namespace_if_not_exists(layout.table_id.split(".")[0])

        try:
            catalog.load_table(layout.table_id)
        except Exception:
            if readonly:
                raise

            catalog.create_table(
                layout.table_id, schema=schema, properties=METADATA_PROPERTIES
            )

        table = cls(catalog, layout)
        if not readonly:
            table.ensure_metadata_properties()

        return table

    # -- state ------------------------------------------------------------

    def reload(self) -> None:
        self._table = self._catalog.load_table(self._layout.table_id)

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
        return self._table.schema().find_field("offset").field_id

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
        field = self._table.schema().find_field("offset")
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
        location = self.metadata_location
        if location != self._extent_at:
            self._extent = self._read_extent()
            self._extent_at = location

        return self._extent

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

        field = self._table.schema().find_field("offset")
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

    def register(self, path: str) -> None:
        """Add an already-written file to the table (§4 step 2).

        `add_files` rather than `append`: pyiceberg's append writes the file
        itself and commits afterwards, so a crash in between orphans a file
        under a name nothing recorded — exactly what I2 exists to prevent.
        """
        self._table.add_files([path])
        self.reload()

    def replace_range(self, lo: int, hi: int, path: str) -> None:
        """Swap `[lo, hi]` for one already-written file, in one snapshot (§6).

        `overwrite()` would do this in a single call, but it writes the output
        itself — putting a path on disk this process only learns about
        afterwards, which is what the deletion queue exists to avoid.
        """
        with self._table.transaction() as transaction:
            transaction.delete(delete_filter=offset_between(lo, hi))
            transaction.add_files([path])

        self.reload()

    def evict_through(self, boundary: int) -> None:
        """Drop every file at or below `boundary` from the current snapshot (§8)."""
        self._table.delete(delete_filter=offset_at_or_below(boundary))
        self.reload()

    def expire_snapshots_older_than(self, cutoff: datetime) -> None:
        """Expire snapshot METADATA. Does not delete any file — see §6."""
        self._table.maintenance.expire_snapshots().older_than(cutoff).commit()
        self.reload()

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

        with self._table.transaction() as transaction:
            transaction.set_properties(missing)

        self.reload()


def _local(path: object) -> str:
    """Iceberg records `file://` URIs; the filesystem wants plain paths."""
    return str(path).removeprefix("file://")
