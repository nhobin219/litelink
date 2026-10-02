"""The published tier row: what a read consults to skip the published table (#90).

A read decides per query which tiers it needs, with `litelink.manifest.prune`
over two rows keyed by `tier`:

- **`staging`** — the staging table, rolled up from its manifests. The
  process that commits a new version stores that version's rollup, stamped
  with the version, so every other process reads it rather than rolling it up
  again; a reader uses it only when the stamp is the version it resolved, and
  otherwise rolls the version up itself (`LogTable.statistics_at`, cached in
  memory per version). A late or missing store costs a rollup, never a wrong
  answer.
- **`published`** — what the published table holds BELOW the staging table:
  the rows eviction moved there. Not the whole published table, whose copy of
  the staging window would put its maximum timestamp at "a few minutes ago"
  and send every hot query to the network. The published leg of a read covers
  exactly this range — offsets under the staging table's first — so this is
  the row that decides it.

The published table's row is the one stored, in `buffer.db`, because its
statistics otherwise live in the published table's manifests on S3 and the
decision must not cost the round trip it exists to avoid. Eviction is the last
moment those rows' statistics are on local disk, and it usually runs in another
process than the reader, so the row is written by the writer side and read by
everyone.

**Overstating is the safe direction.** A row that claims more than the
published table holds below the staging table costs a read that finds nothing;
one that claims less loses rows. So eviction WIDENS the row before the commit
that moves rows below the staging table, and the one write that narrows — an
exact rollup from the published table's manifests — runs only under the
whole-log maintenance claim, where eviction cannot run beside it. `publish` never
changes it: it adds copies of rows the staging table still holds.

**A missing row means "no statistics", and nothing turns it into a row but an
exact rollup.** Widening a row that is not there would describe only the rows
being added, so it does nothing; the published table is read until something
computes the whole of it.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, NamedTuple

import pyarrow as pa

from litelink._statistics import ColumnStatistics, TierStatistics, _merge
from litelink.manifest import Entry, columns

if TYPE_CHECKING:
    from litelink._buffer import Buffer

STAGING = "staging"
PUBLISHED = "published"
BUFFER = "buffer"
TIERS = (STAGING, PUBLISHED)

OFFSET = "litelink_offset"

# The key column, as `litelink.manifest` names it for a log's tiers.
KEY = "tier"


class Stored(NamedTuple):
    """A stored tier: where it sits in the log, its columns' statistics, and —
    for the staging tier — the version of the table they were rolled up from."""

    offsets: tuple[int, int]
    statistics: TierStatistics
    version: str | None


class StoredTiers:
    """Every stored tier row, decoded, as a reader consults them per query.

    One SQLite statement per call. The decoded rows are kept against the
    generation counter every write bumps, so a query with no write since the
    last decodes nothing.
    """

    def __init__(self, buffer: Buffer) -> None:
        self._buffer = buffer
        self._cache: tuple[str, dict[str, Stored]] | None = None

    def load(self) -> dict[str, Stored]:
        generation, raw = self._buffer.tiers()
        cached = self._cache
        if cached is not None and generation is not None and cached[0] == generation:
            return cached[1]

        decoded = {
            tier: Stored(offsets, decode(statistics), version)
            for tier, (offsets, statistics, version) in raw.items()
        }
        if generation is not None:
            self._cache = (generation, decoded)

        return decoded


class PublishedTier:
    """The published table's tier row, kept in `buffer.db` (`tier_offsets` and
    `tier_statistics`)."""

    def __init__(self, buffer: Buffer) -> None:
        self._buffer = buffer
        self._stored = StoredTiers(buffer)

    def load(self) -> Stored | None:
        """The row, or None when the log has none yet."""
        return self._stored.load().get(PUBLISHED)

    def has(self) -> bool:
        return self.load() is not None

    def replace(self, schema: pa.Schema, statistics: TierStatistics) -> None:
        """Make the row exactly `statistics` — the one write that narrows.

        Only under the whole-log claim, where eviction cannot widen it between
        `statistics` being taken and this landing. The range is taken from the
        statistics' own offset bounds.
        """
        stored = (offsets(statistics), encode(schema, statistics))
        self._buffer.update_tier(PUBLISHED, lambda *_: stored)

    def widen(self, schema: pa.Schema, statistics: TierStatistics) -> None:
        """Add `statistics` to the row, before the commit that evicts them.

        A missing row stays missing: it is "no statistics", and a row made of
        only the rows being added would claim the published table holds
        nothing else.
        """
        added = offsets(statistics)

        def change(
            current: tuple[int, int] | None, raw: str | None
        ) -> tuple[tuple[int, int], str] | None:
            if current is None or raw is None:
                return None

            merged = _union(schema, decode(raw), statistics)
            if added[1] <= added[0]:
                span = current
            elif current[1] <= current[0]:
                span = added
            else:
                span = (min(current[0], added[0]), max(current[1], added[1]))

            return span, encode(schema, merged)

        self._buffer.update_tier(PUBLISHED, change)

    def drop(self) -> None:
        """Forget the row, so reads include the published table until it is rebuilt."""
        self._buffer.update_tier(PUBLISHED, lambda *_: None)


def encode(schema: pa.Schema, statistics: TierStatistics) -> str:
    """The stored form: the prunable columns' bounds and counts, as JSON.

    JSON round-trips every prunable type exactly — Python writes a float as its
    shortest repr, which reads back as the same double. `litelink_offset` is
    not among them: its range is stored apart, in `tier_offsets`.
    """
    kinds = columns([schema])
    return json.dumps(
        {
            "record_count": statistics.record_count,
            "columns": {
                name: [
                    column.min,
                    column.max,
                    column.null_count,
                    column.value_count,
                    column.nan_count,
                ]
                for name, column in statistics.columns.items()
                if name in kinds
            },
        },
        sort_keys=True,
    )


def decode(raw: str) -> TierStatistics:
    stored: dict[str, Any] = json.loads(raw)
    return TierStatistics(
        tier=None,
        record_count=stored.get("record_count"),
        file_count=0,
        columns={
            name: ColumnStatistics(*values)
            for name, values in stored.get("columns", {}).items()
        },
    )


def entry(
    tier: str,
    span: tuple[int, int | None],
    schema: pa.Schema,
    statistics: TierStatistics,
) -> Entry:
    """A tier as a manifest entry: `span` is its `[start, end)`."""
    return Entry(tier, span[0], span[1], schema, statistics)


# The buffer's statistics: none. It is pruned by its offset range alone.
UNKNOWN = TierStatistics(tier=None, record_count=None, file_count=0, columns={})


def offsets(statistics: TierStatistics) -> tuple[int, int]:
    """`(start, end)` from the offset column's bounds; end exclusive.

    `(0, 0)` when there are none — a tier with no rows.
    """
    column = statistics.columns.get(OFFSET)
    if column is None or column.min is None or column.max is None:
        return 0, 0

    return int(column.min), int(column.max) + 1


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
