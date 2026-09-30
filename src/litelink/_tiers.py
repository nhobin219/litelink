"""The log's tier manifest: `<name>.manifest.parquet`, one row per tier (#90).

The file `litelink.manifest` describes, with `tier` as its key and two rows:

- **`local`** — the local Iceberg table.
- **`archive`** — what the archive holds BELOW the local table: the rows
  eviction dropped. Not the whole archive, whose copies of the local window
  would put its maximum timestamp at "a few minutes ago" and send every hot
  query to the network. The archive leg of a read covers exactly this range —
  offsets under the local table's first — so this is the row that decides it.

The buffer has no row, as a stream's live log has none: it is always read.

**Overstating is the safe direction, and every write is ordered for it.** A
row that claims more than its tier holds costs a read that finds nothing; one
that claims less loses rows from every read that skips the tier. So a row is
WIDENED before the commit that adds rows to its tier, and narrowed only after
the commit that removes them — and only where no concurrent commit can have
widened it in between (see `WriteHandle` and `Maintenance.evict`). A reader
loads the file before and after resolving the tiers, and skips a tier only
when both copies rule it out, so neither ordering can catch it mid-change.

**A missing row means "no statistics", and nothing turns it into a row but an
exact rollup.** Widening a row that is not there would describe only the rows
being added, so it does nothing; the tier is read until something computes the
whole of it.

Rewritten whole under an exclusive lock, and replaced by rename, so a reader
sees one version or the next. Readers take no lock.
"""

from __future__ import annotations

import contextlib
import fcntl
import os
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq

from litelink._statistics import (
    ColumnStatistics,
    TierStatistics,
    _merge,
)
from litelink.manifest import Row, columns, extend, without

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator
    from pathlib import Path

    from litelink._layout import Layout

LOCAL = "local"
ARCHIVE = "archive"
TIERS = (LOCAL, ARCHIVE)

OFFSET = "litelink_offset"

# The key column, as `litelink.manifest` names it for a log's tiers.
KEY = "tier"


class TierManifest:
    """The manifest file of one log: load it, and change one row at a time."""

    def __init__(self, layout: Layout) -> None:
        self._path = layout.directory / f"{layout.name}.manifest.parquet"
        self._lock = layout.directory / f"{layout.name}.manifest.lock"
        # `(stat key, table)` — a reader stats the file per query and parses
        # it only when it changed.
        self._cache: tuple[tuple[int, int, int], pa.Table] | None = None

    @property
    def path(self) -> Path:
        return self._path

    # -- reading ---------------------------------------------------------

    def load(self) -> pa.Table | None:
        """The current manifest, or None if the log has none yet."""
        try:
            stat = self._path.stat()
        except FileNotFoundError:
            return None

        key = (stat.st_ino, stat.st_mtime_ns, stat.st_size)
        cached = self._cache
        if cached is not None and cached[0] == key:
            return cached[1]

        try:
            table = pq.read_table(self._path)
        except FileNotFoundError:
            return None

        self._cache = (key, table)

        return table

    def has(self, tier: str) -> bool:
        manifest = self.load()

        return manifest is not None and tier in manifest[KEY].to_pylist()

    # -- writing ---------------------------------------------------------

    def replace(self, tier: str, schema: pa.Schema, statistics: TierStatistics) -> None:
        """Make `tier`'s row exactly `statistics` — the one write that narrows.

        Only for a caller that knows no commit can have widened the row since
        `statistics` was taken.
        """
        with self._exclusive() as current:
            self._save(extend(current, _row(tier, schema, statistics), key=KEY))

    def widen(self, tier: str, schema: pa.Schema, statistics: TierStatistics) -> None:
        """Add `statistics` to `tier`'s row, before the commit that adds them.

        A missing row stays missing: it is "no statistics", and a row made of
        only the rows being added would claim the tier holds nothing else.
        """
        with self._exclusive() as current:
            if current is None:
                return

            held = _statistics(current, tier)
            if held is None:
                return

            merged = _union(schema, held, statistics)
            self._save(extend(current, _row(tier, schema, merged), key=KEY))

    def evicted(self, boundary: int, removed: int) -> None:
        """After eviction removed every local row at or below `boundary`.

        Narrows the local row by what can be narrowed while seals run
        concurrently: the offset floor rises past `boundary`, since whatever a
        seal adds sits above it, and `removed` rows come off the count. The
        other columns keep their bounds — a seal may already have widened them
        for a file this pass cannot see.
        """
        with self._exclusive() as current:
            held = None if current is None else _statistics(current, LOCAL)
            if current is None or held is None:
                return

            columns_ = dict(held.columns)
            offsets = columns_.get(OFFSET)
            if offsets is not None and offsets.min is not None:
                columns_[OFFSET] = ColumnStatistics(
                    min=max(offsets.min, boundary + 1),
                    max=offsets.max,
                    null_count=offsets.null_count,
                    value_count=offsets.value_count,
                    nan_count=offsets.nan_count,
                )

            count = held.record_count
            narrowed = TierStatistics(
                tier=None,
                record_count=None if count is None else max(count - removed, 0),
                file_count=held.file_count,
                columns=columns_,
            )
            schema = _schema_of(current)
            self._save(extend(current, _row(LOCAL, schema, narrowed), key=KEY))

    def drop(self, tier: str) -> None:
        """Forget `tier`'s row, so reads include the tier until it is rebuilt."""
        with self._exclusive() as current:
            if current is not None:
                self._save(without(current, tier, key=KEY))

    @contextlib.contextmanager
    def _exclusive(self) -> Iterator[pa.Table | None]:
        """The current manifest, read under the lock every writer takes.

        Across processes, because the sealer and the maintainer usually are
        separate ones: each rewrites the whole file, and two interleaved
        read-modify-writes would each drop the other's change.
        """
        self._lock.parent.mkdir(parents=True, exist_ok=True)
        with self._lock.open("a") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            try:
                try:
                    current = pq.read_table(self._path)
                except FileNotFoundError:
                    current = None

                yield current
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def _save(self, manifest: pa.Table) -> None:
        """Replace the file atomically, durable before the rename."""
        staging = self._path.with_name(self._path.name + ".tmp")
        with staging.open("wb") as sink:
            pq.write_table(manifest, sink)
            sink.flush()
            os.fsync(sink.fileno())

        staging.replace(self._path)


def _row(tier: str, schema: pa.Schema, statistics: TierStatistics) -> Row:
    lo, end = offsets(statistics)

    return Row(tier, lo, end, schema, statistics)


def offsets(statistics: TierStatistics) -> tuple[int, int]:
    """`(start, end)` from the offset column's bounds; end exclusive.

    `(0, 0)` when there are none — a tier with no rows.
    """
    column = statistics.columns.get(OFFSET)
    if column is None or column.min is None or column.max is None:
        return 0, 0

    return int(column.min), int(column.max) + 1


def _statistics(manifest: pa.Table, tier: str) -> TierStatistics | None:
    """A row read back as statistics, or None if the tier has no row."""
    for row in manifest.to_pylist():
        if row[KEY] != tier:
            continue

        found: dict[str, ColumnStatistics] = {}
        for field in manifest.schema:
            if not pa.types.is_struct(field.type):
                continue

            value: dict[str, Any] | None = row[field.name]
            if value is None:
                continue

            found[field.name] = ColumnStatistics(
                min=value.get("min"),
                max=value.get("max"),
                null_count=value.get("null_count"),
                value_count=value.get("value_count"),
                nan_count=value.get("nan_count"),
            )

        return TierStatistics(
            tier=None,
            record_count=row["record_count"],
            file_count=0,
            columns=found,
        )

    return None


def _schema_of(manifest: pa.Table) -> pa.Schema:
    """The declared columns a manifest's structs describe, as a schema."""
    return pa.schema(
        [
            pa.field(field.name, field.type.field("min").type)
            for field in manifest.schema
            if pa.types.is_struct(field.type)
        ]
    )


def _union(
    schema: pa.Schema, held: TierStatistics, added: TierStatistics
) -> TierStatistics:
    """Both, by the rule `_statistics` folds files by."""
    return _merge(list(columns([schema])), [held, added])


def empty(schema: pa.Schema) -> TierStatistics:
    """A tier known to hold nothing — which prunes for every query."""
    return TierStatistics(
        tier=None,
        record_count=0,
        file_count=0,
        columns={
            name: ColumnStatistics(
                None, None, 0, 0, 0 if pa.types.is_floating(kind) else None
            )
            for name, kind in columns([schema]).items()
        },
    )


def footer_statistics(paths: Iterable[str], schema: pa.Schema) -> TierStatistics:
    """Statistics for Parquet files not yet in any table, from their footers.

    What `add_files` itself reads to fill the Iceberg manifest, taken before
    the commit so a tier's row can be widened first. Only the prunable
    columns; a row group without a bound for a column that is not all NULL
    makes that column unknown, and unknown never prunes.
    """
    kinds = columns([schema])
    lows: dict[str, Any] = {}
    highs: dict[str, Any] = {}
    unknown: set[str] = set()
    nulls = dict.fromkeys(kinds, 0)
    values = dict.fromkeys(kinds, 0)
    records = 0
    files = 0
    for path in paths:
        files += 1
        metadata = pq.ParquetFile(path).metadata
        records += metadata.num_rows
        indices = {
            metadata.schema.column(i).path: i for i in range(metadata.num_columns)
        }
        for group in range(metadata.num_row_groups):
            row_group = metadata.row_group(group)
            if row_group.num_rows == 0:
                continue

            for name in kinds:
                index = indices.get(name)
                stats = None if index is None else row_group.column(index).statistics
                if stats is None:
                    unknown.add(name)
                    continue

                nulls[name] += stats.null_count or 0
                values[name] += row_group.num_rows
                if stats.has_min_max:
                    low, high = stats.min, stats.max
                    lows[name] = low if name not in lows else min(lows[name], low)
                    highs[name] = high if name not in highs else max(highs[name], high)
                elif stats.null_count != row_group.num_rows:
                    unknown.add(name)

    return TierStatistics(
        tier=None,
        record_count=records,
        file_count=files,
        columns={
            name: ColumnStatistics(
                min=None if name in unknown else lows.get(name),
                max=None if name in unknown else highs.get(name),
                null_count=None if name in unknown else nulls[name],
                value_count=None if name in unknown else values[name],
                # No write path admits NaN (#87).
                nan_count=0 if pa.types.is_floating(kind) else None,
            )
            for name, kind in kinds.items()
        },
    )
