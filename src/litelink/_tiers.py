"""The archive's tier row: what a read consults to skip the archive (#90).

A read decides per query which tiers it needs, with `litelink.manifest.prune`
over two rows keyed by `tier`:

- **`local`** — the local Iceberg table, rolled up from the snapshot the read
  resolved (`LogTable.statistics_at`). Its statistics are Iceberg's own, on
  local disk, so nothing is stored for it.
- **`archive`** — what the archive holds BELOW the local table: the rows
  eviction moved there. Not the whole archive, whose copy of the local window
  would put its maximum timestamp at "a few minutes ago" and send every hot
  query to the network. The archive leg of a read covers exactly this range —
  offsets under the local table's first — so this is the row that decides it.

The archive's row is the one stored, in `buffer.db`, because its statistics
otherwise live in the archive's manifests on S3 and the decision must not
cost the round trip it exists to avoid. Eviction is the last moment those rows'
statistics are on local disk, and it usually runs in another process than the
reader, so the row is written by the writer side and read by everyone.

**Overstating is the safe direction.** A row that claims more than the archive
holds below the local table costs a read that finds nothing; one that claims
less loses rows. So eviction WIDENS the row before the commit that moves rows
below the local table, and the one write that narrows — an exact rollup from
the archive's manifests — runs only under the whole-log maintenance claim,
where eviction cannot run beside it. `sync` and `rewrite_archive` never change
it: one adds copies of rows the local table still holds, the other re-cuts
rows the archive already has.

**A missing row means "no statistics", and nothing turns it into a row but an
exact rollup.** Widening a row that is not there would describe only the rows
being added, so it does nothing; the archive is read until something computes
the whole of it.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from litelink._statistics import ColumnStatistics, TierStatistics, _merge
from litelink.manifest import Row, columns

if TYPE_CHECKING:
    from litelink._buffer import Buffer

LOCAL = "local"
ARCHIVE = "archive"
TIERS = (LOCAL, ARCHIVE)

OFFSET = "litelink_offset"

# The key column, as `litelink.manifest` names it for a log's tiers.
KEY = "tier"


class ArchiveTier:
    """The archive's tier row, kept in `buffer.db`."""

    def __init__(self, buffer: Buffer) -> None:
        self._buffer = buffer
        # `(generation, decoded)` — a reader asks per query, and decodes only
        # when a write has moved the generation on.
        self._cache: tuple[str | None, TierStatistics | None] | None = None

    def load(self) -> TierStatistics | None:
        """The row, or None when the log has none yet."""
        generation, raw = self._buffer.archive_tier()
        cached = self._cache
        if cached is not None and cached[0] == generation and generation is not None:
            return cached[1]

        decoded = None if raw is None else decode(raw)
        self._cache = (generation, decoded)

        return decoded

    def has(self) -> bool:
        return self._buffer.archive_tier()[1] is not None

    def replace(self, schema: pa.Schema, statistics: TierStatistics) -> None:
        """Make the row exactly `statistics` — the one write that narrows.

        Only under the whole-log claim, where eviction cannot widen it between
        `statistics` being taken and this landing.
        """
        encoded = encode(schema, statistics)
        self._buffer.update_archive_tier(lambda _: encoded)

    def widen(self, schema: pa.Schema, statistics: TierStatistics) -> None:
        """Add `statistics` to the row, before the commit that evicts them.

        A missing row stays missing: it is "no statistics", and a row made of
        only the rows being added would claim the archive holds nothing else.
        """

        def change(current: str | None) -> str | None:
            if current is None:
                return None

            return encode(schema, _union(schema, decode(current), statistics))

        self._buffer.update_archive_tier(change)

    def drop(self) -> None:
        """Forget the row, so reads include the archive until it is rebuilt."""
        self._buffer.update_archive_tier(lambda _: None)


def encode(schema: pa.Schema, statistics: TierStatistics) -> str:
    """The stored form: the prunable columns' bounds and counts, as JSON.

    JSON round-trips every prunable type exactly — Python writes a float as its
    shortest repr, which reads back as the same double.
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


def row(tier: str, schema: pa.Schema, statistics: TierStatistics) -> Row:
    """`statistics` as a manifest row, offsets taken from its own bounds."""
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
