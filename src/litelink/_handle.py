"""The core append-only stream.

A `WriteHandle` is one stream: one SQLite buffer, one staging table, and one
published table — on S3, or a local directory by default (SPEC §1, #98). Rows
are durable at commit and queryable immediately; `offset` is assigned by the
library and is the only column it owns (§2, I11).

Core library only. The blob-field extension (§15) is deliberately absent —
applications that need a small binary column declare an ordinary `binary`
column in their own schema, which §15.2 already says is the right route for
payloads that fit comfortably in the buffer.

Logs are immutable: a log's schema is fixed when it is created, and changing it
means starting a new log (§9).
"""

from __future__ import annotations

import contextlib
import functools
import json
import random
import threading
import time
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.exceptions import TableAlreadyExistsError

from litelink._buffer import (
    SCHEMA_KEY,
    SORT_KEY,
    START_OFFSET_KEY,
    Buffer,
    RetiredError,
    RowProbe,
    Shape,
)
from litelink._claim import EVERYTHING, Claim, new_owner
from litelink._config import LogConfig
from litelink._fs import write_parquet
from litelink._layout import Layout, is_remote, validate_published
from litelink._maintenance import (
    CONFIG_KEY,
    Maintenance,
    checkpoint,
    stable_prefix,
)
from litelink._published import PUBLISHED_KEY, Published
from litelink._read import Reader, duckdb_connection
from litelink._replication import flush, litestream_config, restore_buffer
from litelink._s3 import S3Options
from litelink._statistics import (
    Tier,
    TierStatistics,
    _from_rows,
    below_staging,
    file_span,
    rollup,
    rows_at_or_after,
    staging_span,
    uncounted,
    whole_log,
)
from litelink._table import (
    RETIRED_PROPERTY,
    LogTable,
    forget_published_entry,
    published_columns,
    published_retired,
    published_span,
)
from litelink._tiers import PUBLISHED as PUBLISHED_TIER
from litelink._tiers import STAGING as STAGING_TIER
from litelink._tiers import UNKNOWN as UNKNOWN_TIER
from litelink._tiers import PublishedTier, StoredTiers, empty
from litelink._tiers import encode as encode_tier
from litelink._types import NON_FINITE, column_type, validate_schema

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence
    from os import PathLike
    from typing import Self

    from litelink._table import DataFile

# What `append` and `extend` take, and the one alias in a public signature.
#
# Defined at RUNTIME rather than under `TYPE_CHECKING`, so it can be exported:
# a caller annotating against those signatures has to name it, and a name that
# exists only to a type checker cannot be imported. `Mapping` comes from
# `collections.abc`, so the runtime import costs a stdlib module already loaded.
Row = Mapping[str, object]

# The one column the library owns (§2). Named rather than spelled inline so
# `validate` has something to check against, and prefixed so it can never
# collide with a column an application declares.
OFFSET = "litelink_offset"

# How often `await_seal` re-asks. Each round attempts a drain, and taking the
# lease is a write, so this is slower than a pure poll would need to be —
# 20 attempts a second while a caller is blocked, and none otherwise.
_AWAIT_POLL = 0.05

# How long a configuration change waits for maintenance to finish before it
# gives up. Long enough to cover an ordinary merge, short enough that a wedged
# log is reported rather than hung.
_SETTINGS_WAIT_S = 10.0

SEAL_ROLE = "seal"
MAINTAIN_ROLE = "maintain"
# Bulk ingest's own role, and NOT `seal`. Reusing the sealer's would put a bulk
# range into `sealing`, where `_recover_seal` reads `rows_between(start, end)`
# from a buffer that never held it, writes an EMPTY Parquet, registers it as
# covering the range and then discards every buffered row below it. Measured:
# 40 acknowledged rows deleted into no file, and an empty file carries no
# column statistics, so `LogTable.span()` raises on every call afterwards.
INGEST_ROLE = "ingest"


# One home, in `_maintenance`, because eviction reads it too.
_CONFIG_KEY = CONFIG_KEY
# One home, in `_published`, because `evict` reads it too.
_PUBLISHED_KEY = PUBLISHED_KEY
# One home, in `_buffer`, because the buffer is what reads it per call — it
# owns `meta`, and `shape()` is where every holder of the schema now gets it.
_SCHEMA_KEY = SCHEMA_KEY
# One home, in `_buffer`, beside the other durable facts about a log's shape.
_START_OFFSET_KEY = START_OFFSET_KEY
# §4's declared clustering, kept here as well as on the table.
#
# Not a duplicate of the Iceberg sort order — a fact the staging CATALOG carries
# and nothing else does. `catalog.db` is replicated but cannot be restored onto
# another machine (it records absolute paths to local metadata no sidecar
# ships), so a failover rebuilds the staging table rather than restoring it, and
# has to be told what order to declare. `open_published` never declared one
# either, so the published table could not answer it.
#
# One home, in `_buffer`, because the seal, compaction and the published table
# all read it too — see `Buffer.sort_by`.
_SORT_KEY = SORT_KEY


# How many offsets a restore skips before resuming (§3a).
#
# `sqlite_sequence` comes back from the replica, so it resumes above everything
# the REPLICA received — not above everything the primary ASSIGNED. Rows
# appended inside the replication lag were returned to callers by `append` and
# never shipped, so resuming at the replica's frontier hands those same
# integers to different data. I9 says offsets are never reused for the life of
# a stream, and §6 needs files adjacent in offset order rather than free of
# integer gaps — so a gap is expressible and a reuse is not.
#
# 2**20 is generous against any plausible replication lag and free against
# int64. It errs large on purpose: a gap is visible to a consumer, and a
# rewind looks like ordinary operation.
RESTORE_RESERVE = 1 << 20


# How many staged files one Iceberg commit takes.
#
# A commit costs far more than the write it publishes — 4.1 s against 648 ms,
# measured against S3 — because it reads each footer and writes a manifest, a
# manifest list, a fresh `metadata.json` and a new catalog pointer, none of
# which gets cheaper for holding one file instead of twenty. At `compact_size`
# and 200-byte rows a 160M-row load is roughly 480 files, which is half an hour
# of commits alone if each registers on its own.
#
# What batching costs is a wider window in which a written file is not yet
# registered. It is not a new KIND of window: every one of these paths is in
# `compacting` before its bytes exist (I2), so recovery finds them either way.
_INGEST_BATCH = 20

# The Parquet codecs `LogConfig.compression` accepts. pyarrow's set, minus the
# ones there is no reason to offer: `brotli` and `lz4` are neither the smallest
# nor the fastest here, and a codec nobody has measured on this shape is a
# setting whose consequences the library cannot describe.
_CODECS = frozenset({"none", "snappy", "gzip", "zstd"})


def _refuse_foreign_schema(incoming: pa.Schema, declared: pa.Schema) -> None:
    """Check a bulk source against the log's schema BEFORE anything is reserved.

    An Arrow batch carries its schema, so one comparison proves the names and
    types of every row in it by construction — the per-row validation the
    mapping path does is not optimised away here, it stops being necessary
    (§13.4). That is the second argument for this endpoint, independent of
    avoiding the row-by-row rewrite, and for a backfill it is the larger one.

    Up front, and only here, because a rejection AFTER a reservation is a
    permanent hole in the offset space. Type compatibility is settled by casting
    an empty table, which costs nothing and answers the same question the real
    cast will.
    """
    if OFFSET in incoming.names:
        msg = f"`{OFFSET}` is assigned by the library and cannot be supplied (I11)"
        raise ValueError(msg)

    missing = [name for name in declared.names if name not in incoming.names]
    unknown = [name for name in incoming.names if name not in declared.names]
    if missing or unknown:
        msg = (
            "this source does not match the log's schema"
            + (f"; missing {missing}" if missing else "")
            + (f"; unknown {unknown}" if unknown else "")
        )
        raise ValueError(msg)

    try:
        incoming.empty_table().select(declared.names).cast(declared)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError) as exc:
        msg = f"this source's types cannot be cast to the log's schema: {exc}"
        raise ValueError(msg) from exc


def _refuse_non_finite(rows: pa.Table) -> None:
    """Refuse a bulk chunk holding NaN or ±inf, anywhere in any float (#87).

    `append` refuses them through its CHECK and its encoder; `ingest` writes
    Arrow straight to Parquet and meets neither, so it is asked here — per
    chunk, vectorised, before the chunk reserves anything. Measured at under
    a nanosecond a value, against roughly 200 ns a row for the load itself.
    """
    for field in rows.schema:
        for path, leaf in _float_leaves(
            field.name, field.type, rows.column(field.name)
        ):
            finite = pc.is_finite(leaf)
            if pc.all(finite).as_py() is False:
                bad = next(
                    value
                    for value, ok in zip(
                        leaf.to_pylist(), finite.to_pylist(), strict=True
                    )
                    if ok is False
                )
                where = "" if path == field.name else f" at {path}"
                msg = f"column {field.name!r} cannot hold {bad!r}{where}: {NON_FINITE}"
                raise ValueError(msg)


def _float_leaves(
    path: str, type_: pa.DataType, column: pa.ChunkedArray | pa.Array
) -> Iterator[tuple[str, pa.ChunkedArray | pa.Array]]:
    """Every float inside `column`, with the path to it, as Arrow arrays.

    Struct children through `flatten()`, which folds the parent's validity in,
    so a value under a null struct is never mistaken for a stored one.
    """
    if pa.types.is_floating(type_):
        yield path, column
    elif pa.types.is_struct(type_):
        for chunk in _chunks_of(column):
            for field, child in zip(type_, chunk.flatten(), strict=True):
                yield from _float_leaves(f"{path}.{field.name}", field.type, child)
    elif pa.types.is_list(type_):
        for chunk in _chunks_of(column):
            yield from _float_leaves(f"{path}[]", type_.value_type, chunk.flatten())
    elif pa.types.is_map(type_):
        for chunk in _chunks_of(column):
            # Keys are strings or integers (`_types`); only the values can be
            # floats.
            yield from _float_leaves(f"{path}[]", type_.item_type, chunk.items)


def _chunks_of(column: pa.ChunkedArray | pa.Array) -> list[pa.Array]:
    return column.chunks if isinstance(column, pa.ChunkedArray) else [column]


def _chunks(
    reader: pa.RecordBatchReader, size: int, row_cap: int | None
) -> Iterator[pa.Table]:
    """One output file's worth of rows at a time, bounded by memory.

    A batch that would overshoot is SLICED rather than taken whole, because the
    source may be one `pa.Table` holding the entire corpus in a single chunk —
    and then "emit a file per batch" is one file for the load, which is the
    split not happening. Slicing an Arrow batch copies nothing.

    The budget is `target_compact_size` in Arrow's in-memory bytes, and the per
    row cost is averaged over the batch it came from rather than summed per
    row: a seal counts exactly because it sees rows one at a time and this does
    not, and paying a per-row measurement to place a file boundary would cost
    more than the boundary is worth. Files land near the target rather than on
    it, which is the tolerance `runs()` already works within — and a file at the
    target forms a run of one there, so landing near it is what keeps these
    files out of compaction rather than merely close to a number.

    A single row larger than the whole budget still becomes a file of one row.
    That is deliberate — the alternative is a loop that cannot advance.
    """
    held: list[pa.RecordBatch] = []
    measured = 0
    counted = 0
    for batch in reader:
        if not batch.num_rows:
            continue

        per_row = max(1, batch.nbytes // batch.num_rows)
        cursor = 0
        while cursor < batch.num_rows:
            take = batch.num_rows - cursor
            if row_cap is not None:
                take = min(take, row_cap - counted)

            take = min(take, max(1, (size - measured) // per_row))
            piece = batch.slice(cursor, take)
            held.append(piece)
            measured += per_row * piece.num_rows
            counted += piece.num_rows
            cursor += take
            if measured >= size or (row_cap is not None and counted >= row_cap):
                yield pa.Table.from_batches(held, schema=reader.schema)
                held, measured, counted = [], 0, 0

    if held:
        yield pa.Table.from_batches(held, schema=reader.schema)


@dataclass(frozen=True)
class _Recovery:
    """What a restore recovered and what it skipped, for the caller to report."""

    recovered: int
    resumed_at: int
    # `[start, end)`, half-open like every range litelink reports.
    skipped: tuple[int, int]


def _foreign_published(published: str) -> ValueError:
    """This log has no record of pushing to a published table that already
    holds data.

    Which makes it another log's. Two logs of the same name both start at
    offset 1, so the ranges cannot tell them apart — what can is that a log
    which pushed to a published table keeps its `extent` rows naming that
    prefix, even after it is pointed elsewhere (§4a). No rows, data present:
    not ours.
    """
    return ValueError(
        f"the published table at {published!r} holds data this log has no record of pushing, "
        f"so it belongs to another log. Attaching it would let its contents be read "
        f"as this log's own, push nothing, and pin eviction — silently. To resume "
        f"that log here use litelink.restore; to start a new one, point at an unused "
        f"prefix"
    )


# The last release that can still move a 0.1 log to the per-stream layout, or
# finish an `add_column` it interrupted. Both were removed in the next (§9).
LAST_MIGRATING_RELEASE = "0.5.1"

# The record an interrupted `add_column` left in `meta`. Read only to refuse.
_LEGACY_INTENT_KEY = "schema_intent"


def legacy_layout(layout: Layout, why: str = "") -> str:
    """The refusal for a log still in the pre-0.2 layout: catalogs at the root.

    This release no longer carries the migration, so the message names the
    last one that does, and the command it runs.
    """
    return (
        f"the log at {layout.root}/{layout.name} uses the pre-0.2 layout, whose "
        f"catalogs sit at the root{'; ' + why if why else ''}. This release cannot "
        f"move it. Install litelink {LAST_MIGRATING_RELEASE} and run:\n"
        f"  python -m litelink.migrate --root {layout.root} --name {layout.name} --apply\n"
        f"then open it with this release"
    )


def _span(start: int, end: int) -> tuple[int, int] | None:
    """`[start, end)`, or None when it holds nothing."""
    return None if end <= start else (start, end)


def _now() -> str:
    """A timestamp a person will read."""
    return datetime.now(UTC).isoformat(timespec="seconds")


def _repointed_mid_push() -> RuntimeError:
    """The log was pointed at another published table while a publish pass
    was pushing.

    A push can outlive its lease — a register alone measured 4.1 s against S3,
    and retries compound it — so the re-point that races it took the lease
    lawfully. Nothing here is corrupt; the watermark this push earned simply
    describes a published table the log has left, and recording it would tell
    eviction (I4) that the new published table holds rows it has never been
    sent.
    """
    return RuntimeError(
        "the published table was re-pointed while this publish was pushing; its watermark "
        "belongs to the previous published table and is not recorded"
    )


def _scan_query(
    declared: Sequence[str],
    *,
    columns: Sequence[str] | None = None,
    where: str | None = None,
    start_offset: int | None = None,
    end_offset: int | None = None,
) -> str:
    """The SQL behind `scan`, built from the declared columns alone.

    A function rather than a method because it touches no handle state: give
    it column names and bounds and it returns a string, which makes it
    testable without a log. That is the whole of the reason now — an earlier
    version of this docstring said the separation existed because two classes
    needed it, which was true while `Follower` and `Log` each had a read path
    and stopped being true when the hierarchy gave them one.

    Taking the column names rather than a schema keeps it independent of who
    stores them.
    """
    projection = ", ".join(f'"{c}"' for c in (columns or (OFFSET, *declared)))
    predicates = [f"({where})"] if where else []
    if start_offset is not None:
        predicates.append(f'"{OFFSET}" >= {int(start_offset)}')

    if end_offset is not None:
        predicates.append(f'"{OFFSET}" < {int(end_offset)}')

    clause = f" WHERE {' AND '.join(predicates)}" if predicates else ""

    return f'SELECT {projection} FROM log{clause} ORDER BY "{OFFSET}"'


@dataclass(frozen=True, slots=True)
class Coverage:
    """Where each tier sits in the log, as half-open `[start, end)` offset
    ranges — the convention of the stored tier offsets and `litelink.manifest`.

    The ranges partition the log, as `column_statistics`' tiers do: `published`
    is what only the published table holds — below the staging table — `staging`
    is the staging table, and `buffer` is the buffered rows above every file.
    None for a tier holding nothing. The lowest offset any of them reports is
    where the log starts, and the highest `end` is the offset after its last
    buffered or published row; the lowest of `staging` and `buffer` is how far
    back a read can go without the published table.
    """

    published: tuple[int, int] | None
    staging: tuple[int, int] | None
    buffer: tuple[int, int] | None


class LogHandle:
    """Everything you can ask a log without writing to it.

    Every handle is on the primary, the host that holds the log's directory:
    it reads the buffer, the staging table and the published table, and sees
    the writer's commits as they land. Reading a log from another machine is
    any Iceberg engine over its published table, not a litelink handle (#90);
    the `follow`/`snapshot` readers that once did it are gone.

    **Which tiers a read touches is decided per query, from what it asks for**
    (#90). The buffer is skipped only when its offsets rule it out, and the
    staging and published tables only when their statistics say they cannot hold
    a matching row: the staging table's from the snapshot the read resolved, the
    published table's from a row in `buffer.db` covering what it holds below the
    staging table, widened by eviction as rows move there (`_tiers`). A read
    bounded inside the staging window stays on local disk (I5), and one that
    reaches back reads history — because the whole log is the right answer to
    it.

    That reverses 0.4.0, deliberately. 0.4.0 fixed the tiers at assembly with
    `include_archive`, so a `scan` would not start touching the network
    because eviction happened to run; the price was that the caller named a
    tier, and a handle without the archive answered short for any query that
    reached below the local window. Now latency follows the predicate rather
    than the handle: the same query reads the published table once eviction
    has moved the rows it asks for there.
    """

    def __init__(
        self,
        *,
        layout: Layout,
        table: LogTable,
        buffer: Buffer,
        published: Published,
        reader: Reader,
    ) -> None:
        self._layout = layout
        self._table = table
        self._buffer = buffer
        self._published = published
        self._reader = reader

    # -- identity ----------------------------------------------------------

    @property
    def root(self) -> Path:
        return self._layout.root

    @property
    def name(self) -> str:
        return self._layout.name

    @property
    def config(self) -> LogConfig:
        """The policy in force, read through to the log on every access."""
        return self._buffer.config()

    @property
    def sort_by(self) -> tuple[str, ...]:
        """The declared clustering (§7), read through like `config`."""
        return self._buffer.sort_by()

    @property
    def schema(self) -> pa.Schema:
        """The caller's columns, as declared at `new`, without the offset (I11).

        `open` takes no schema, so this is the only way to ask a log what shape
        it is. A property, like `config` and `sort_by` beside it — the refactor
        that built this class dropped the decorator, and because attribute
        access still SUCCEEDED it failed silently: callers got a bound method,
        `if handle.schema:` stayed true, and the break surfaced only where the
        value was used as a schema.
        """
        return self._buffer.schema

    @property
    def published(self) -> str:
        """Where the published table is: `s3://…`, or a `file://` directory —
        by default under the log's own, for a log given no remote one (#98).

        Read from `meta` on every access, so a caller cannot disagree with the
        log about it.
        """
        return self._published.uri

    # -- read --------------------------------------------------------------

    def scan(
        self,
        *,
        columns: Sequence[str] | None = None,
        where: str | None = None,
        start_offset: int | None = None,
        end_offset: int | None = None,
        published: bool = True,
    ) -> pa.RecordBatchReader:
        """Read the log as one relation, newest data included.

        Unions the tiers and bounds each by its neighbour's committed offset
        range, resolved from manifest statistics at query time (§7, I3). The
        tiers overlap by design, so the bounds are what make each row appear
        exactly once.

        Which tiers, and therefore whether this touches the network, follows
        from `where` and the offset bounds: the published table is read only
        when what it holds below the staging table could hold a matching row.

        **`start_offset`/`end_offset` are the only bounds that can skip the
        buffer.** It keeps no column statistics, but its offsets are a known
        range — from its lowest up — so a scan ending below that range never
        reads it. A `where` on any other column still reads it. The same holds
        for a `litelink_offset` comparison in `where` or in `sql`.

        `published=False` never reads the published table: the result is what
        the staging table and the buffer hold, and rows only the published table
        has are left out rather than refused. `coverage(published=False)` says
        where that floor is, without the network either.

        Always bound on a LEADING column of `sort_by`. §7 measures a
        non-leading predicate at 119 ms against 13 ms for the same predicate
        with a leading bound.

        Returns a streaming reader rather than a table: a full-window read with
        a 400-byte payload column is 611 ms and proportional to the data, so
        materialising it is the caller's choice to make.
        """
        return self.sql(
            _scan_query(
                self.schema.names,
                columns=columns,
                where=where,
                start_offset=start_offset,
                end_offset=end_offset,
            ),
            published=published,
        )

    def sql(self, query: str, *, published: bool = True) -> pa.RecordBatchReader:
        """Run arbitrary DuckDB SQL against the log, exposed as `log`.

        The escape hatch for what `scan` cannot express. Quote
        `"litelink_offset"` — it is a DuckDB reserved word.

        Reads the published table only when the query could match a row of it;
        see `LogHandle`. Only a single SELECT over `log` with AND-ed comparisons
        in its WHERE is narrowed — any other shape reads every tier, which is
        always correct and only slower.

        `published=False` never reads the published table, as for `scan`.
        """
        try:
            # No lock. `Reader` guards its own connection and `LogTable` its
            # own cache, both briefly.
            return self._reader.query(query, published=published)
        except FileNotFoundError as exc:
            # Narrow: the race where a concurrent `publish` sweeps the metadata
            # JSON this read resolved before it loaded it. pyiceberg raises
            # `FileNotFoundError` there.
            #
            # Narrow in the other direction too, which was tested rather than
            # assumed: an endpoint cut out from under a live reader raises
            # `OSError` (NETWORK_CONNECTION) and bad credentials raise `OSError`
            # (ACCESS_DENIED). Neither is mislabelled "reassemble".
            #
            # It does NOT cover the published table's data files or manifests. A
            # missing data file surfaces as `HTTPException` inside `execute`,
            # and a missing manifest can abort the process outright — DuckDB's
            # iceberg extension calls `std::terminate` — which is pre-existing
            # and reaches an ordinary read of a local published table too.
            # Neither is catchable here.
            #
            # "Can", not "does": that abort was observed on a COLD read. A
            # reader that has already scanned serves from DuckDB's cache and
            # neither aborts nor raises.
            #
            # Three earlier versions of this comment claimed otherwise.
            raise self._swept(exc) from exc

    # -- observe -----------------------------------------------------------

    def end_offset(self) -> int:
        """The offset after the last row this reader can see.

        **Which tier answers depends on whether the published table is
        load-bearing.** With staging files, `sqlite_sequence` is authoritative:
        it is the offset the next append receives, and nothing below it is
        missing, because the staging table holds whatever the buffer has
        sealed. That keeps this a cheap SQLite read on the writer's own log.

        With an empty staging table — a log evicted dry — it is not
        authoritative, and this is where a replica-restored reader once bit.
        `next_offset` reads a sequence its own docstring calls "the highest
        value ever assigned and never lowers", so it keeps counting rows the
        replica no longer carries: a seal taken while `_discard_on_seal()` was
        true deletes its rows and they reach the published table only at the
        next `publish`. In between they are in neither tier.

        Measured on such a reader in that window: it served 864 rows and
        reported 1,501, and a caller using this as a resume cursor skipped 636
        rows permanently once the primary published. Over-reporting loses data;
        under-reporting only re-delivers. So the answer is taken from the
        tiers that actually hold rows, which is `litelink.restore`'s formula.
        """
        if not self._published_required():
            return self._buffer.next_offset()

        buffered = self._buffer.span()

        return max(self._checked_span()[1], 0 if buffered is None else buffered[1])

    def buffered_rows(self) -> int:
        """Rows durable in the buffer but not yet sealed.

        Counted against the staging table's boundary rather than by asking the
        buffer how many rows it holds, because a seal LEAVES its rows there
        when `wal_replication` is on (§3a) — the buffer is the off-box copy
        until the published table has the range, so its row count is not the
        unsealed tail.

        **It reloads first.** `LogTable.span` compares
        against this handle's in-memory `metadata_location` and resolves
        nothing, so reading it directly pins the boundary to whatever snapshot
        was last loaded and every row another process has since sealed still
        counts as unsealed. Measured against a maintainer that had just sealed
        20 rows: a reader reported `buffered=20, table=20` for a log holding
        20, double-counting the whole band across the §7 boundary that I3 says
        each row crosses exactly once.

        A refactor inlined this method's body minus the reload, and it hid
        because every other observation here reloads — `examples/adsb/tail.py`
        only reads the right number because it happens to call `staging_rows()`
        first in the same tuple.
        """
        self._table.reload()
        span = self._table.span()

        return self._buffer.count_from(0 if span is None else span[1])

    def staging_rows(self) -> int:
        """Rows in the staging table, from the manifests."""
        self._table.reload()

        return self._table.record_count()

    def staging_files(self) -> int:
        """Data files in the staging table — what compaction is bringing down."""
        self._table.reload()

        return self._table.file_count()

    def staging_extent(self) -> tuple[int, int] | None:
        """The staging table's offsets as `[start, end)`, from statistics (§7).

        Half-open, like `coverage()` and the stored tier offsets.
        """
        self._table.reload()

        return self._table.span()

    def published_through(self) -> int:
        """Highest offset the published table is known to hold, 0 if none (§5,
        I4).

        The log's own cached watermark, not a network read — the same `meta`
        row eviction consults so it can ask a keyed read instead of a round
        trip. `coverage()` gives the published table's range below the staging
        table.
        """
        recorded = self._buffer.get_meta(Maintenance.PUBLISHED_THROUGH_KEY)

        return 0 if recorded is None else int(recorded)

    def published_files(self) -> int:
        """How many data files the published table holds, or 0 before its first
        publish.

        A network round trip, unlike `staging_files()`.
        """
        published = self._published.table()
        if published is None:
            return 0

        try:
            published.reload()

            return len(published.data_files())
        except FileNotFoundError as exc:
            # The same refusal every other member touching the published table
            # gives. This was the one that escaped the conversion and raised a
            # bare `FileNotFoundError` naming an S3 key — loud either way, but a
            # caller handling "the snapshot was swept" should not have to handle
            # it twice.
            raise self._swept(exc) from exc

    def column_statistics(self, *, tier: Tier | None = None) -> TierStatistics:
        """Per-column min, max and counts, opening no data file.

        The tiers partition the log — each row counted in exactly one — and
        together they are `tier=None`, the whole log:

        - `"staging"`: the staging table's current snapshot.
        - `"published"`: what the published table holds BELOW the staging
          table, the rows eviction moved there. Not the published table's copy
          of the staging window, which `"staging"` already counts. Read from the
          published table's manifests, so it asks the network. A log with
          nothing in staging (retired, or evicted dry) has the whole published
          table here.
        - `"buffer"`: the buffered rows above every file, counted from the
          rows themselves.

        Files are rolled up from their Iceberg manifests; buffered rows, which
        no file holds yet, are counted directly. A consumer prunes on this, so
        missing information is None rather than a narrower bound — see
        `ColumnStatistics` for exactly when, including why a float's bounds say
        nothing about NaN and why strings carry none. A published file that
        straddles the staging range (only `rewrite_published` cuts one) makes
        every count None in `"published"` and in the whole log: its bounds hold,
        but its rows cannot be counted once without opening it.
        """
        if tier is not None and tier not in ("staging", "published", "buffer"):
            msg = f"tier must be 'staging', 'published', 'buffer' or None, not {tier!r}"
            raise ValueError(msg)

        if tier == "staging":
            # Through the cache routing uses, so the same snapshot is rolled
            # up once whichever asks first.
            self._table.reload()
            location = self._table.metadata_location
            found = self._table.statistics_at(location)
            if found is None:
                # The table moved between the reload and the ask: roll up the
                # snapshot it is at now.
                self._table.reload()
                return rollup("staging", *self._table.live_files())

            return found

        # The order a read resolves its legs in, and for the same reason. The
        # buffer FIRST: a seal lands its file and then deletes the rows, so a
        # row it moves afterwards is in the staging snapshot taken next. The
        # published table LAST: eviction follows registration (I4), so a
        # published snapshot taken after the staging one holds everything the
        # staging one has already given up.
        buffered = self._buffer.rows_from(None)
        self._table.reload()
        local_schema, local_files = self._table.live_files()
        span = staging_span(local_schema, local_files)

        if tier == "buffer" and span is not None:
            # With staging files, nothing in the published table reaches above
            # them, so the buffer's part needs no network read.
            ceiling = 0 if span is None else span[1]
            return self._retier(
                "buffer", _from_rows(rows_at_or_after(buffered, ceiling))
            )

        remote = None
        published = self._published.table()
        if published is not None:
            try:
                published.reload()
                remote = published.live_files()
            except FileNotFoundError as exc:
                raise self._swept(exc) from exc

        if tier is None:
            return whole_log(
                (OFFSET, *self.schema.names),
                (local_schema, local_files),
                remote,
                buffered,
            )

        beyond: list = []
        straddles = False
        ceiling = 0 if span is None else span[1]
        if remote is not None:
            beyond, straddles = below_staging(span, remote[0], remote[1])
            ceiling = max([ceiling, *(file_span(remote[0], f)[1] for f in beyond)])

        if tier == "buffer":
            return self._retier(
                "buffer", _from_rows(rows_at_or_after(buffered, ceiling))
            )

        if remote is None:
            msg = f"log {self.name!r} has no published table to take statistics from"
            raise ValueError(msg)

        found = rollup("published", remote[0], beyond)

        return uncounted(found) if straddles else found

    @staticmethod
    def _retier(tier: Tier, statistics: TierStatistics) -> TierStatistics:
        return TierStatistics(
            tier=tier,
            record_count=statistics.record_count,
            file_count=statistics.file_count,
            columns=statistics.columns,
        )

    def coverage(self, *, published: bool = True) -> Coverage:
        """Each tier's offset range, from the offsets the log keeps — no network.

        `litelink_offset` is the log's own sequence, dense and monotonic across
        the tiers, so each tier is fully described by where it starts and ends.
        Those are kept already, for routing (#90): the published table's range
        and the staging table's are stored in `buffer.db`, and the buffer's are
        its own indexed `min` and `max`. So this is one SQLite read and two
        edge seeks, against `column_statistics`' walk of every manifest.

        The staging range is the stored one when it was stamped with the
        version the table is at, and the snapshot's own span otherwise —
        cached per version, so a commit costs one manifest walk per process at
        most. The published table's range is read from its manifests only when
        the log has no stored row yet: a log written before the row existed
        and not yet backfilled, or one just re-pointed. That is the one case
        this touches the network, and it is never answered wrongly instead.

        `published=False` is for a caller that will not read the published
        table — a replay held to the staging floor, `min(staging[0],
        buffer[0])`. Its `published` is None, meaning "not asked" rather than
        "empty", and nothing but local disk is opened, so the fallback above
        cannot reach the network.

        A gap — offsets assigned but in no tier — is not reported. On the
        primary the only one is a `litelink.restore` fence, which `recovery()`
        reports on the handle that made it.
        """
        stored = StoredTiers(self._buffer).load()
        self._table.reload()
        location, extent = self._table.snapshot()

        cached = stored.get(STAGING_TIER)
        if cached is not None and cached.version == location:
            local = _span(*cached.offsets)
        else:
            local = extent

        published_row = stored.get(PUBLISHED_TIER)
        if published_row is not None:
            below = _span(*published_row.offsets)
        elif published:
            below = self._published_below(local)
        else:
            below = None

        held = self._buffer.span()
        ceiling = max((r[1] for r in (below, local) if r is not None), default=0)
        buffer = None if held is None else _span(max(held[0], ceiling), held[1])

        return Coverage(
            published=below if published else None, staging=local, buffer=buffer
        )

    def _published_below(self, local: tuple[int, int] | None) -> tuple[int, int] | None:
        """The published table's `[start, end)` below the staging table, read
        from its manifests.

        The fallback for a log with no stored published row. A file straddling
        the staging table's first offset counts whole, as everywhere else.
        """
        try:
            published = self._published.table()
            if published is None:
                return None

            published.reload()
            schema, files = published.live_files()
        except FileNotFoundError as exc:
            raise self._swept(exc) from exc

        spans = [file_span(schema, f) for f in files]
        below = [s for s in spans if local is None or s[0] < local[0]]
        if not below:
            return None

        return min(start for start, _ in below), max(end for _, end in below)

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        """Release the reader's connection and the buffer's. Nothing else.

        It is one of two overrides in the hierarchy. The other is
        `WriteHandle.end_offset`, which answers a genuinely different question
        than this class's: where the next append lands, rather than past the
        last row this handle can serve.
        """
        self._reader.close()
        self._buffer.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # -- internals ---------------------------------------------------------

    def _published_required(self) -> bool:
        """Whether the published table is the only source of this log's
        published rows.

        True when the staging table holds nothing and the published table holds
        something. Both halves are asked directly, and the ORDER is what keeps
        a hot read local: the table is checked first, from manifest statistics
        already on disk, so a log with staging files answers False without
        touching the network. I5 protects a read that HAS local disk to serve;
        one with none faces a metadata GET or silently wrong results.

        **Do not reintroduce a local proxy for "the published table holds
        rows". This has now been wrong three times, each time in the silent
        direction.**

        - Keying on the empty table alone demanded an archive from a log
          created with one and never synced, refusing a first read that should
          simply have served the buffer.
        - Adding `published_through() > 0` fixed that and broke re-pointing:
          `_repoint` deliberately zeroes that watermark when the location
          moves, and pointing away and back is two moves, so a log returning
          to the published table it just left reads 0 while the bucket still
          holds everything. Measured on an evicted log: `scan()` returned 550 of
          4,000 rows with no error, and `coverage()` called it gap-free.
        - Replacing it with a durable marker `follow` wrote made the local
          eviction case unreachable, which hid a real defect behind what looked
          like a design boundary.

        The published table is the authority on what it holds, and it is asked
        through the RESOLVING read. A first version asked `Published.table()`
        directly, on the theory that the cached handle cost one metadata
        resolve and no more. That was the fourth wrong proxy: `Published.table`
        returns its handle unchanged while the URI matches, and `LogTable.span`
        short-circuits on an unchanged `metadata_location`, so a handle first
        touched while the published table EXISTED BUT WAS EMPTY answered None
        for the rest of its life.

        That state is ordinary — `publish` creates the table on a maintenance
        tick before anything is sealed, and `set_published` creates one at a new
        prefix — and one early call was enough to pin it: a `scan`, an
        `end_offset`, even a bare metadata poll. The handle then served the
        buffer alone with no error while `coverage()`, which does resolve,
        reported the same log gap-free. Measured: 0 of 20 rows, indefinitely, on
        both a reader and a writer.

        So the cost is published metadata GETs on a read of a log with an empty
        staging table — two of them, since the read that follows resolves the
        published table again for its own bounds, and an earlier version of
        this docstring said one. A log with staging files never reaches this
        line, which is what keeps I5 intact for a hot read; a log without them
        has no local data to protect, and the alternative is silently wrong
        results.

        **On a log that DOES have staging files, the reload above costs 0.63 ms
        and the read resolves the table a second time for its own bounds.**
        Measured two ways that agree: `LogTable.reload()` in isolation at
        0.6278 ms, and the p10 of an end-to-end scan with and without this
        check at 0.63 and 0.61 ms, against an A/A noise floor of 0.34 ms.

        It is a FIXED cost, so it is ~10% of the smallest possible scan
        (6.5 ms over 1,710 rows) and ~0.1% of the full-window read §7 measures
        at 611 ms. Collapsing it means deriving this inside `Reader.query`,
        after the resolve that already happens there — which is possible, and
        is not worth doing on the strength of 0.63 ms, because that path's own
        docstring explains that getting it wrong leaves "rows silently missing
        from the answer". Revisit if a workload ever shows it.
        """
        # The staging table first, which is manifest statistics and no network.
        # Reloaded because the dangerous direction is a stale "the table has
        # files" on one another process has just evicted — that would drop the
        # published leg and serve short with no error.
        self._table.reload()
        if self._table.span() is not None:
            return False

        return self._published_span() is not None

    def _published_span(self) -> tuple[int, int] | None:
        """The published table's span, forcing a real re-read, or None if
        unreachable.

        **The re-read is not optional.** Both caches short-circuit —
        `Published.table` on a live handle and `LogTable.span` on an unchanged
        `metadata_location` — so without `reload()` a long-lived reader would
        answer from the pointer it last saw, and `end_offset()` would report
        a published table another process has since committed past.

        The published table carries `previous-versions-max: 10`, so a pointer
        eleven published commits old names deleted metadata. That is `_swept`:
        on the primary it is a race with a concurrent `publish`, and retrying
        reads the current pointer.
        """
        try:
            adopted = self._published.table(repair=False)
            if adopted is None:
                return None

            adopted.reload()

            return adopted.span()
        except FileNotFoundError as exc:
            raise self._swept(exc) from exc

    def _checked_span(self) -> tuple[int, int]:
        """The published table's span, or a refusal — for when it is
        load-bearing.

        Two failures, and neither may be silent. An unreachable published table
        would otherwise fall through to the buffer leg, which on an empty
        staging table is every published row missing with no error; and a
        published table reporting no span has nothing for this to merge.
        """
        extent = self._published_span()
        if extent is None:
            msg = (
                f"{self.root}/{self.name} can no longer reach its published table, and it "
                f"holds no local files — reading on would omit every published row. "
                f"Check that the published table is reachable (credentials, endpoint) and "
                f"retry"
            )
            raise RuntimeError(msg)

        return extent

    def _swept(self, exc: FileNotFoundError) -> RuntimeError:
        return RuntimeError(
            f"the published table moved on while this read was resolving it — another "
            f"process committed past the pointer it held ({exc}). Retry: the next "
            f"read resolves the current pointer"
        )


class LocalReadHandle(LogHandle):
    """A read-only handle to a log on THIS machine, beside its writer.

    Adds the replication surface, and that is the whole of the difference from
    `LogHandle`. Generating a sidecar config is exactly what you do alongside
    a live writer — and reaching for a writer to do it takes both whole-log
    claims and the SQLite write lock, and runs `recover()`, which can finish
    and replace the writer's in-flight seal. `examples/adsb/replicate.py` did
    that for one commit.

    A local handle shares the primary's root and name, so the replica key it
    emits IS the primary's own correct one.

    Sees the writer's commits as they land: `catalog.db` and `published.db`
    live in the stream's directory and both processes read the same rows.
    """

    @property
    def databases(self) -> tuple[Path, ...]:
        """The SQLite files a restore needs (§3a), for a sidecar to replicate.

        Public because deciding to run a sidecar is a deployment choice, but
        knowing WHICH files carry the log's state is not — it is this library's,
        and a sidecar configured by hand against a guess silently omits one.
        """
        return self._layout.databases

    def replication_config(self) -> str:
        """A litestream config for this log (§3a).

        **A read, which is why it belongs here.** Generating it is exactly what
        you do alongside a live writer, and doing that through a writer takes
        both whole-log claims and the SQLite write lock on a log another
        process owns — and runs `recover()`, which can finish and replace that
        process's in-flight seal. `examples/adsb/replicate.py` did that for one
        commit, because this surface was on `WriteHandle` alone and the example
        had to reach for a writer to get at it.

        A local handle shares the primary's root and name, so the key it emits
        IS the primary's own correct one.

        This used to be a runtime refusal on one shared class. Moving it here
        made the guard unnecessary — there is nothing to call.
        """
        published = self.published
        if not self._published.remote():
            msg = (
                f"{self.root}/{self.name} publishes to a local published table "
                f"({published}), so there is nowhere off this machine to ship its "
                f"WAL — replication needs a remote (s3://) one"
            )
            raise ValueError(msg)

        return litestream_config(
            self._layout,
            published,
            self._published.s3,
            self.config.wal_retention,
        )

    def write_replication_config(self) -> Path:
        """Write that config next to the log, and return where.

        Beside the data rather than at a configured path, for the same reason
        every other path is derived: a setting for it would be one more thing
        to keep in step with the log it describes.
        """
        destination = self._layout.replication_config
        destination.write_text(self.replication_config())

        return destination


class WriteHandle(LocalReadHandle):
    """One append-only stream.

    Single writer per log (§1): SQLite's write lock is per file, and one process
    per stream is the intended topology.

    Threads within that process are fine, and scheduling `maintain()` on a
    background thread is the expected shape — every public method takes one
    lock. A second *process* is not: maintenance commits to the catalog and
    writes the buffer database, so it is a writer, and two of those is the case
    §1 excludes.

    Construct through `litelink.new`, `open` or `restore`; `litelink.open(...,
    read_only=True)` gives a second view alongside a live writer. The
    initialiser takes already built collaborators and does no I/O, so a test can
    substitute any of them.
    """

    def __init__(
        self,
        *,
        layout: Layout,
        table: LogTable,
        buffer: Buffer,
        reader: Reader,
        maintenance: Maintenance,
        config: LogConfig,
        published: Published,
    ) -> None:
        super().__init__(
            layout=layout,
            table=table,
            buffer=buffer,
            published=published,
            reader=reader,
        )
        # Set only by `restore`, and read only by `recovery()`. What a failover
        # recovered and what it skipped are facts about one operation, knowable
        # at the moment it runs and not afterwards — the skipped range leaves no
        # trace once the sequence has moved, and the recovered count is
        # indistinguishable from ordinary buffered rows a second later.
        self._restored_from: _Recovery | None = None
        # The tier rows (#90): the published table's, which eviction widens and
        # a rollup under the whole-log claim replaces, and the staging one this
        # handle stores after each commit it makes to the staging table.
        self._tiers = PublishedTier(buffer)
        table.after_commit = self._store_staging_statistics
        self._maintenance = maintenance
        # Sequences the only thing left that needs it: this handle mutating
        # several objects at once, in `set_config`, `set_published` and
        # `set_sort_by`, where a SQLite row and a Python object have to change
        # together.
        #
        # Nothing on the append, seal or read paths takes it. Each collaborator
        # owns its own safety — the buffer serialises its connection,
        # `LogTable` guards its handle and caches, `Reader` guards its DuckDB
        # connection — and the leases decide who may seal or maintain across
        # processes, which no in-memory lock could. A lock on top of those is a
        # second answer to a settled question, and it was not free: one held
        # across a whole maintenance pass made a read wait 21.5 s.
        self._lock = threading.RLock()

        # Who may seal, and who may maintain. Durable rather than in-memory,
        # because the answer has to survive the process asking (§13.6): a
        # boolean says nothing about a second process, and `claim_seal`
        # overwrites rather than refuses, so two sealers would take turns
        # clobbering each other's claim.
        #
        # The same leases decide recovery. §11 has the hazard in both
        # directions — a maintenance process redoing the writer's in-flight
        # seal, a writer deleting a maintenance process's half-written
        # compaction — and ownership is what resolves it.

    # -- construction ------------------------------------------------------

    @classmethod
    def new(
        cls,
        root: PathLike[str] | str,
        name: str,
        *,
        schema: pa.Schema,
        sort_by: Sequence[str] | None = None,
        config: LogConfig | None = None,
        published: str | None = None,
        s3: S3Options | None = None,
        start_offset: int = 1,
    ) -> Self:
        """Create a log. Raises if one already exists at `root/name`.

        This is the only call that takes the log's shape, because the shape is
        fixed at creation and recovered by `open` thereafter.

        **`start_offset` leaves `[1, start_offset)` unassigned for ever**,
        and is the one way to do so. It exists to align a log's offsets with a
        sequence something else already owns, and to reserve room a later
        backfill can fill — see §13's *Bulk ingest*.

        It is creation-only, and there is deliberately no way to re-seed a log
        afterwards. `Buffer.seed_offsets`' guard reads the offsets currently
        BUFFERED, which is empty once a seal has deleted them, so it would not
        refuse a re-seed onto already-issued offsets: the next appends would
        reuse committed offsets, `_union` would hide them behind the table's
        span and the seal's `register` would decline the file and queue it
        for deletion — rows acknowledged and silently gone. A fresh buffer is
        the only state in which seeding is safe, and `new` is the only call
        that has one.

        The value is recorded in `meta` when it is greater than 1, because a
        backfill has to tell that reserve from a `litelink.restore` fence. Both are
        empty ranges below the log's offsets and nothing else distinguishes
        them — see `START_OFFSET_KEY`.

        `schema` is the application's columns. The library adds `offset` and
        owns nothing else — no ingest timestamp, no transaction id (§2).

        Arrow, always. The table underneath is Iceberg and its schema could be
        stated directly, but every Iceberg field needs an explicit `field_id`
        and pyiceberg accepts duplicates without complaint — two fields numbered
        1 construct fine — and every engine reading the published table resolves
        columns by field ID, so a duplicate silently breaks both. That numbering
        is library bookkeeping, the same argument §2 makes for `offset`, so it
        is pyiceberg's job and not the caller's.

        The cost of one schema type is that Arrow does not map onto Iceberg
        exactly: `string` is stored and returned as `large_string`, and types
        Iceberg would narrow silently are refused instead (see `_types`).

        **`sort_by` defaults to offset order, and most logs should leave it
        there.** §7 measures it as a read-shape decision rather than a tuning
        knob: it declares which predicates prune, only a LEADING column prunes,
        and changing it later rewrites every file (see `set_sort_by`).

        The default is not a fallback — it is the order the rows are already
        in, so it costs strictly less than any sort key. No sort runs at seal
        time, and each file's offset range is contiguous and exact, which is
        the tightest file-level statistic the table can carry.

        Set it only for a column highly correlated with the offset, which for a
        capture stream means an arrival timestamp. An UNCORRELATED key is worse
        than it looks, and in two ways that no benchmark of the seal will show.
        Files always hold contiguous offset ranges, so if the sort column is
        scattered across them, every file's min/max on it spans nearly the whole
        domain and file-level pruning does nothing — only row-group skipping
        inside each file survives. And rows within a file end up in a random
        permutation of offset order, so replay from an offset stops being
        sequential and needs a sort after reading. For a log, replay is the
        primary access pattern, which makes that the expensive half.

        `published` is where the log publishes: a warehouse prefix on S3
        (`s3://bucket/prefix`) or a local directory (`file:///directory`).
        None publishes under the log's own directory, and then capture, seal,
        compaction, publishing, retention and reads all work with no network,
        forever (§11).
        """
        # `None` and `()` mean the same thing — offset order — so nothing
        # downstream has to distinguish "unset" from "explicitly unsorted".
        order = tuple(sort_by or ())
        settings = config or LogConfig()
        # Same normalisation as `set_published`: an empty string and None both
        # mean the local default, which `validate` sees as None.
        published = (published or "").rstrip("/") or None
        validate(schema, order, settings, published)
        # The type as well as the range, because this one is written to `meta`
        # and read back by anything that needs the reserve: `2.5` seeds 2 and
        # records "2.5", so the durable record would disagree with the log.
        if not isinstance(start_offset, int) or start_offset < 1:
            msg = f"start_offset must be an integer of at least 1, not {start_offset!r}"
            raise ValueError(msg)

        layout = Layout(Path(root), name)
        if layout.buffer_db.exists():
            msg = f"a log already exists at {layout.root}/{name} — use open()"
            raise FileExistsError(msg)

        # BEFORE anything is created, so a refusal leaves no half-built log —
        # which would then block the retry, since `new` refuses an existing
        # buffer and `restore` does too.
        #
        # A fresh log holds no `extent` rows at all, so any published table
        # that already has data belongs to something else. This is the shape an
        # operator reaches for when failing over by hand: `litelink.new` on the
        # second box, pointed at the old prefix. Silently, that log pushes
        # nothing — its offsets are below the published table's — eviction
        # pins, and the published table's contents read back as its own.
        # `set_published` refuses the same thing; both entry points need it,
        # because either can be the one that points the log.
        if published is not None:
            try:
                covered = published_span(layout, published, s3 or S3Options())
            except Exception:
                # Unreachable or unreadable is "cannot tell", which passes:
                # configuring a published table is a statement of intent, not a
                # claim that the bucket is already there. Measured: an empty
                # prefix and a nonexistent bucket both return None, while bad
                # credentials and a dead endpoint raise — and all four have to
                # pass, because credentials commonly attach to a box AFTER the
                # log is configured.
                #
                # An attempt to narrow this to `except OSError` broke sixteen
                # tests that configure a published table with no live endpoint,
                # which is the pattern this is protecting, not an accident of
                # theirs. The way to catch a typo'd key is `litelink.preflight`,
                # which can also check the things this cannot: the litestream
                # binary and the DuckDB extensions.
                covered = None

            if covered is not None:
                raise _foreign_published(published)

        layout.create()
        table = LogTable.create(layout, table_schema(schema), order)
        buffer = Buffer.open(
            layout.buffer_db,
            schema,
        )
        # Arrow is the interchange type at every edge — SQLite to Parquet,
        # Iceberg to Arrow — so the declared Arrow schema is what those edges
        # cast to. It is kept here because Iceberg cannot represent it: one
        # string type and one binary type, so `large_binary` would come back
        # `binary` and the declaration would be quietly overruled.
        # BEFORE either `meta` write, and that ordering is the whole of its
        # crash safety. `litelink.open` refuses a log with no stored schema or
        # config, so every window between here and `set_meta_all` fails closed.
        # Seeding AFTER them leaves one whose crash yields a log that reopens
        # silently at offset 1 — and `litelink.new` then refuses to retry, because
        # the buffer exists. The reserve would be lost with no error anywhere.
        if start_offset > 1:
            buffer.seed_offsets(start_offset)

        buffer.set_meta(_SCHEMA_KEY, schema.serialize().to_pybytes().hex())
        # One transaction. `validate` has just accepted the policy and the
        # location together, and written separately a crash between them
        # records the policy with no location — which a writer's `open` reads
        # as the local default, so a log created to publish to S3 would publish
        # beside itself instead, and nothing would say so.
        buffer.set_meta_all(
            {
                _CONFIG_KEY: settings.to_json(),
                _SORT_KEY: json.dumps(list(order)),
                # Always recorded (#98): the local default when none is given,
                # so the location the publish fences compare is the stored one.
                _PUBLISHED_KEY: (published or "").rstrip("/")
                or layout.default_published,
                # Recorded only when there IS a reserve. Absent means
                # "started at 1", which a backfill must read as "no reserve"
                # rather than "a reserve of nothing" — a log created at 1 and
                # later restored has a gap below its offsets too, and that gap
                # is a fence.
                **({_START_OFFSET_KEY: str(start_offset)} if start_offset > 1 else {}),
            }
        )

        # Built here and handed to all three, so each is given its published
        # table at construction rather than having one pushed into it
        # afterwards.
        remote = Published(layout, buffer, s3)

        log = cls(
            layout=layout,
            table=table,
            buffer=buffer,
            reader=Reader(
                layout,
                table,
                buffer,
                duckdb_connection,
                published=remote,
            ),
            maintenance=Maintenance(table, buffer, layout, remote),
            config=settings,
            published=remote,
        )

        # A fresh local default holds nothing below the staging table, known at
        # birth. A given one may already hold rows, and the check above may
        # have been unable to reach it, so its row waits for the first publish.
        if published is None:
            table_schema_ = buffer.shape().table
            log._tiers.replace(table_schema_, empty(table_schema_))

        return log

    @classmethod
    def open(
        cls,
        root: PathLike[str] | str,
        name: str,
        *,
        s3: S3Options | None = None,
    ) -> Self:
        """Open an existing log, and recover it.

        Takes none of the shape: the columns come from the Iceberg table, and
        their declared Arrow types, the config, the published table and the
        sort order all from the buffer's `meta` table (§2, §4). The sort order
        used to come from the table's own declaration; it moved because
        `catalog.db` cannot be restored onto another machine, so a failover
        rebuilds that table and has to be told what to declare. Restating any
        of it here would invite a caller to state something the log does not
        agree with, and the log is the one that is right.

        Always recovers, because this always returns a writer. For a second
        view of a log another process is writing, use
        `litelink.open(..., read_only=True)`, which runs no recovery and has no
        mutation to refuse — so it cannot disturb the single writer §1 assumes.
        """
        layout = Layout(Path(root), name)
        # The pre-0.2 tree first, and separately, because it is indistinguishable
        # from an absent log by the test below — its catalog is at the root —
        # and the two want opposite advice. `_assembly._existing` says the same
        # thing on the read-only path; both open paths have to, or the same
        # directory gets two different diagnoses.
        if layout.is_legacy():
            raise FileNotFoundError(legacy_layout(layout))

        # Asked of THIS log's table, not merely of `catalog.db`. The file was
        # once shared by every log under the root, so a root holding one log
        # answered the question for every other name in it — and the caller got
        # pyiceberg's `NoSuchTableError` out of the load below instead of the
        # message here. It is per-stream now, but the row check still earns its
        # keep: the file exists from the moment the catalog is created, before
        # any table is registered in it. "Cannot tell" falls through and lets
        # the load answer, which is slower and correct.
        try:
            present = LogTable.exists_for(layout)
        except LookupError:
            present = True

        if not present:
            msg = f"no litelink log at {layout.root}/{name} — use new() to create one"
            raise FileNotFoundError(msg)

        table = LogTable.load(layout, readonly=False)
        schema = _declared_schema(layout, application_schema(table.arrow_schema()))

        # Read through a throwaway connection, like the schema above it,
        # because the buffer needs `target_seal_size` before it can size the
        # groups it cuts and the value lives in the buffer's own database. A
        # wart: policy is stored inside the thing that consumes it.
        #
        # Required, not defaulted, for the same reason as the schema: new()
        # always writes it, so its absence is a damaged log rather than an
        # older one, and quietly substituting defaults would change how a log
        # seals and what it retains without saying so.
        encoded = Buffer.peek_meta(layout.buffer_db, _CONFIG_KEY)
        if encoded is None:
            msg = f"log at {layout.root}/{name} has no stored config; it is corrupt"
            raise ValueError(msg)

        config = LogConfig.from_json(encoded)
        buffer = Buffer.open(layout.buffer_db, schema)
        # From `meta`, not from the table. Required like the config and for
        # the same reason: `new` always writes it, so its absence is a damaged
        # log, and defaulting to no order would silently de-cluster every file
        # the next compaction rewrites while the table still declared one.
        #
        # Read here only to fail fast with a message naming the log — and to
        # cover an unparseable value, which the bare presence check did not.
        # Every decision that needs the order reads `Buffer.sort_by` itself.
        try:
            buffer.sort_by()
        except ValueError as exc:
            msg = f"log at {layout.root}/{name} has no stored sort order; it is corrupt"
            raise ValueError(msg) from exc

        remote = Published(layout, buffer, s3)
        log = cls(
            layout=layout,
            table=table,
            buffer=buffer,
            reader=Reader(
                layout,
                table,
                buffer,
                duckdb_connection,
                published=remote,
            ),
            maintenance=Maintenance(table, buffer, layout, remote),
            config=config,
            published=remote,
        )
        log.recover()
        log._backfill_manifest()

        return log

    @classmethod
    def replication_config_for(
        cls,
        root: PathLike[str] | str,
        name: str,
        published: str,
        s3: S3Options | None = None,
        retention: timedelta | None = None,
    ) -> str:
        """A litestream config for a log that may not exist here yet (§3a).

        The same file `replication_config` produces, without needing an open
        log to produce it — which is the chicken-and-egg a failover hits. The
        config names the databases to restore, and you need it BEFORE you have
        them. `Layout` is pure path arithmetic, so everything the file says is
        derivable from a root, a name and a published table.
        """
        layout = Layout(Path(root), name)

        return litestream_config(layout, published, s3 or S3Options(), retention)

    @classmethod
    def restore(
        cls,
        root: PathLike[str] | str,
        name: str,
        *,
        published: str,
        s3: S3Options | None = None,
        binary: str | None = None,
    ) -> Self:
        """Recover a log onto a machine that is not the one that wrote it (§3a).

        Point it at the published table, get a working log back, resume
        appending. The procedure this replaces was "restore the databases and
        open it", which does not work: `catalog.db` records ABSOLUTE paths to
        the staging table's Iceberg metadata, and a sidecar ships the `.db`
        files and nothing else — not that metadata, not the Parquet. So a
        restored catalog names files on a machine that is gone, and `open`
        raises `FileNotFoundError`.

        What is recovered, and what is not:

        - **The published table**, in full. It names its own current metadata
          in `version-hint.text`, so it is adopted rather than rebuilt.
        - **The unsealed tail and everything the published table lacks**, from
          `buffer.db`. A seal keeps its rows until the published table has them
          when `wal_replication` is on, so the band between the two frontiers
          comes back too — that is what keeping them makes possible.
        - **The staging table**, rebuilt EMPTY. Its Parquet is on the dead
          machine and its metadata was never replicated.
        - **NOT** rows appended inside the replication lag. They were served to
          callers and never shipped. Their offsets are skipped rather than
          reissued; see below.

        **`published.db` is deliberately not restored**, even though it is
        replicated. `open_published` consults `version-hint.text` only when the
        catalog has no row for the table — with a stale row present it loads
        whatever that names, and old metadata survives in the bucket until
        expiry, so it succeeds. Measured: a stale replica reported one published
        file where the bucket held five, and a union read 261 rows instead of
        1061. Worse, the next publish pass commits onto that lineage and
        republishes the hint over it, destroying the pointer this recovery
        depends on. Stale is worse than absent, and absent is already handled.

        **`catalog.db` is not restored either**, and stays in the replication
        set regardless: same-machine recovery is where its absolute paths still
        resolve, and it is the only record of which Parquet the staging table is
        made of in a design that refuses directory listing.
        """
        # First, because this path does not go through `validate` — it takes no
        # schema and no config — and a malformed prefix would otherwise surface
        # as a YAML parse error from the litestream subprocess, after the root
        # has already been created.
        validate_published(published)
        if not is_remote(published):
            msg = (
                f"restore needs a remote published table (s3://), not {published!r}: it "
                f"recovers a log from the WAL replica beside it, and only a "
                f"remote published table has one"
            )
            raise ValueError(msg)

        layout = Layout(Path(root), name)
        # A buffer with no TABLE for this log is a restore interrupted before
        # its last write, not a log. Refusing it would leave the root in a state
        # neither this nor `litelink.open` accepts, so the only way out would be
        # deleting it by hand. Resumed instead: everything before that write is
        # repeatable, and the reserve simply skips another window.
        #
        # Asked of the table, not of `catalog.db`. That file was once shared by
        # every log under the root, so in a root holding a second log it
        # existed already and a genuinely interrupted restore would never
        # resume. It is per-stream now, but it still exists from the moment the
        # catalog is created, before any table is registered in it.
        #
        # A catalog it cannot READ counts as a log that exists. The resume path
        # reserves offsets, deletes every `extent` row and wipes the claim
        # tables — safe on an interrupted restore, catastrophic on a live log —
        # so "cannot tell" has exactly one safe reading, and it is not the one
        # that proceeds.
        # Before anything, and before `exists_for` — which refuses to answer
        # for this tree rather than reporting it absent. A pre-0.2 log has its
        # catalog at the root, so `<name>/catalog.db` is missing and every
        # test below reads it as "no table here": `resuming` becomes True, the
        # guard that refuses to overwrite a live log is skipped, and the resume
        # path burns RESTORE_RESERVE offsets on a log that is still being
        # written to. Measured on a real one: 300 readable rows down to 240,
        # with eight Parquet files stranded and referenced by nothing.
        if layout.is_legacy():
            raise FileExistsError(legacy_layout(layout, "restore refuses to touch it"))

        try:
            has_table = LogTable.exists_for(layout)
        except LookupError:
            has_table = True

        resuming = layout.buffer_db.exists() and not has_table
        if layout.buffer_db.exists() and not resuming:
            msg = (
                f"a log already exists at {layout.root}/{name}; restore refuses to "
                f"overwrite it. Remove it, or restore into another root"
            )
            raise FileExistsError(msg)

        # One config per STREAM, in the stream's own directory, so the
        # collision this once guarded against is gone by construction: a root
        # holding several logs gives each its own config and its own sidecar.
        # The check below survives for the case that remains — a hand-written
        # config naming a database outside this log, which restoring here would
        # overwrite and silently stop replicating.
        config_path = layout.replication_config
        if config_path.exists():
            # Every `- path:` the file names, which is the database list. It
            # must be a subset of THIS log's, and an earlier version only
            # checked that this log's buffer appeared somewhere in it — so a
            # hand-written per-root config naming several buffers, which §3a
            # tells operators to write, passed and was then overwritten with
            # one naming only the restored log. Every other log under that root
            # stopped replicating at the sidecar's next restart, silently.
            named = {
                line.split("path: ", 1)[1].strip()
                for line in config_path.read_text().splitlines()
                if line.startswith("  - path: ")
            }
            if not named <= {str(path) for path in layout.databases}:
                msg = (
                    f"{config_path} replicates databases outside {layout.root}/{name}; "
                    f"restoring here would overwrite it and stop them. Restore into a "
                    f"root of its own"
                )
                raise FileExistsError(msg)

        options = s3 or S3Options()
        layout.create()
        config_path.write_text(litestream_config(layout, published, options))

        # ONLY the buffer. `restore_buffer` passes `-if-replica-exists`, so an
        # absent replica returns quietly and the check below is what reports
        # it — which is the point, since this is the caller that knows what
        # "no replica" means here. See `restore_buffer` for why that flag does
        # not hide a real failure.
        #
        # Skipped when resuming — the buffer is already here, and litestream
        # would refuse to write over it anyway.
        if not resuming:
            restore_buffer(config_path, layout.buffer_db, options, binary)

        if not layout.buffer_db.exists():
            # Both readings: nothing in the arguments separates a log that
            # never replicated from `name`/`published` naming no log at all.
            msg = (
                f"no replica of {layout.buffer_db.name} under {published} — there is "
                f"nothing to restore. A log with wal_replication off has no off-box "
                f"copy of its unsealed rows and cannot be recovered onto another "
                f"machine, or `name` and `published` do not describe a log that exists"
            )
            raise FileNotFoundError(msg)

        # THE BUFFER IS THE AUTHORITY ON IDENTITY, so a conflicting `published`
        # is a caller bug and has to be loud.
        #
        # `published` names where the litestream replica is pulled FROM; the log
        # attaches to whatever `meta` records, adopted ~60 lines below. On the
        # ordinary path the two agree by construction — the replica under
        # `published` was written by the log that recorded it — which is why
        # this went unnoticed. They diverge only when the buffer arrives some
        # other way, and that is not an exotic case: hand-placing one is the
        # documented recovery for a log whose WAL was never replicated, since
        # `wal_replication=False` leaves no replica for `restore_buffer` to
        # find.
        #
        # Left alone the divergence is SILENT and the handle comes back bound to
        # the buffer's published table. Measured on 0.2.3:
        # `restore(published=A)` over a buffer recording B returned a working
        # handle with `handle.published == B`, `published_through() == 0` and a
        # full `scan()` of 0 rows, while A held 5,000 — and the adoption's
        # `table(repair=True)` then took its CREATE branch and wrote a
        # `metadata.json` and a `version-hint.text` into B, publishing a lineage
        # over a published table the caller never named.
        #
        # Refused rather than honoured, and refused HERE: `published` cannot be
        # made to win without overwriting the one fact that says which log this
        # is, and refusing before `forget_published_entry` below means a
        # conflicting call mutates neither bucket nor catalog.
        #
        # A buffer recording NO published table — one written before #98 by a
        # log that never had an archive — is refused by the same rule, and for
        # a reason worth stating: it would fail anyway, just far too late. The
        # log would be built publishing to its local default, and
        # `write_replication_config` would then raise at the very end because
        # a local published table has nowhere to ship its WAL. That raise lands
        # AFTER `LogTable.create`, which is the commit point — so the caller
        # gets an exception over a root that is now openable and which
        # `restore` itself will not retry because both databases exist.
        # Refusing up here turns that into the same early, retryable failure
        # as the conflict case.
        recorded = Buffer.peek_meta(layout.buffer_db, _PUBLISHED_KEY)
        if (recorded or "").rstrip("/") != published.rstrip("/"):
            found = (
                f"records published={recorded!r}"
                if recorded
                else "records no published table, so it is not from a published log"
            )
            fix = (
                f"Pass published={recorded!r}, or restore a buffer belonging to "
                f"{published!r}"
                if recorded
                else (
                    f"restore cannot attach {published!r} to it — a log's published table is "
                    f"set when it is created, or later with set_published"
                )
            )
            msg = (
                f"{layout.buffer_db.name} {found}, but restore was called with "
                f"published={published!r}. The buffer is the authority on which log "
                f"this is, so the two have to agree. {fix}"
            )
            raise ValueError(msg)

        # A stale entry, if an operator restored all three by hand. Not merely
        # unnecessary — actively destructive: `open_published` consults
        # `version-hint.text` ONLY when the catalog has no row, so a stale row
        # is loaded instead, and old metadata survives in the bucket until
        # expiry so the load succeeds. Measured: a stale replica reported one
        # published file where the bucket held five, and a union read 261 rows
        # instead of 1061. The next publish pass then commits onto that lineage
        # and republishes the hint over it, destroying the pointer this recovery
        # depends on. Dropped so adoption is the only path.
        # Retired logs are not taken over. Both records are asked, because each
        # can be missing where the other is not: the replica's closed buffer
        # ships only when the sidecar does, and a log retired before the
        # published table property existed has only the marker.
        marker = Buffer.peek_retired(layout.buffer_db)
        if marker is not None:
            raise RetiredError.of(marker, name)

        recorded_retirement = published_retired(layout, published, options)
        if recorded_retirement is not None:
            raise RetiredError.of(
                {"state": "retired", "published": published, **recorded_retirement},
                name,
            )

        forget_published_entry(layout)

        # The shape comes from `meta`, which the restored buffer carries: the
        # declared Arrow schema, the policy, and the sort order. Read before
        # the table is built, because the table is built FROM them.
        encoded = Buffer.peek_meta(layout.buffer_db, _CONFIG_KEY)
        raw_schema = Buffer.peek_meta(layout.buffer_db, _SCHEMA_KEY)
        raw_sort = Buffer.peek_meta(layout.buffer_db, _SORT_KEY)
        if encoded is None or raw_schema is None or raw_sort is None:
            msg = (
                f"the restored {layout.buffer_db.name} carries no stored shape; it "
                f"is not a litelink buffer, or it is corrupt"
            )
            raise ValueError(msg)

        schema = pa.ipc.read_schema(pa.py_buffer(bytes.fromhex(raw_schema)))
        sort_by = tuple(json.loads(raw_sort))

        # EVERY OTHER DURABLE WRITE FIRST; `LogTable.create` LAST.
        #
        # That table is what makes this root openable, so wherever it sits, an
        # interruption after it leaves a root `restore` refuses to retry — both
        # databases now exist — and `litelink.open` cheerfully accepts,
        # reporting `recovery() is None`. An earlier version put it second and
        # claimed "this order has no such state"; it had one, one write later.
        # Measured from that window: the open group still at the replica's stale
        # frontier, the first seal writing a file straddling the published
        # table's span, 712 published offsets vanishing from every full `scan()`
        # with no error anywhere, and `publish` raising for ever afterwards.
        #
        # Nothing before it needs it. Adoption and the reconcile are about the
        # PUBLISHED table and the buffer; neither reads the staging table. So it
        # becomes the commit point: everything ahead of it is repeatable, and a
        # root without it cannot be opened by anything.
        buffer = Buffer.open(layout.buffer_db, schema)
        try:
            released, resumed = buffer.strip_local_state(RESTORE_RESERVE)

            # ADOPTED, explicitly. `published.db` was deliberately not restored,
            # so nothing here has a catalog row for the published table — and an
            # ordinary `open` will not create one: adoption is a write to that
            # catalog, and `open_published` reserves it for a repairing caller.
            # Without this the log comes back holding only what the buffer
            # carried, and every read that needs the published table silently
            # leaves its leg out.
            #
            # Built standalone rather than reached through a `WriteHandle`,
            # because there is no staging table yet — which is the whole point
            # of doing it here. It reads the published table's location from the
            # restored `meta`, so it adopts the published table THIS log wrote,
            # not whatever prefix the caller named for the WAL.
            remote = Published(layout, buffer, options)
            where = remote.uri
            if remote.table(repair=True) is None and where is not None:
                # Not best-effort. A restore that cannot reach the published
                # table has recovered the unsealed tail and nothing else, and
                # returning it as a success is the "partial recovery that looks
                # whole" this refuses elsewhere.
                msg = (
                    f"restored the buffer but could not adopt the published table at "
                    f"{where!r}: it holds nothing this log can read. The rows "
                    f"below the published frontier are not recovered"
                )
                raise RuntimeError(msg)

            # RECONCILED against the published table, and this is not optional.
            # A replica is a consistent snapshot from BEFORE the primary's last
            # publish — ordinary replication lag, not a crash window — so the
            # published table is routinely ahead of the `extent` rows the buffer
            # carries. Left alone, `_seed_group` opens the group at the
            # replica's stale frontier while the bucket already holds past it,
            # and the first seal here writes a file reaching into the published
            # table's span.
            #
            # Which wedges the log permanently: `_refuse_straddle` raises on
            # every push, `published_prefix` returns 0 for the straddler so
            # eviction pins at zero, and disk grows without bound. Nothing
            # re-cuts a staging straddler, and this is the operation you run when
            # the published table is the only surviving copy.
            #
            # Releasing what the published table holds and re-seeding is what
            # closes it. Those rows are genuinely safe: the published table has
            # them, which is the same authority `_push` releases on.
            adopted = remote.table()
            # `frontier` is the published span's end: the offset after the last
            # one the bucket holds.
            covered = None if adopted is None else adopted.span()
            frontier = 0 if covered is None else covered[1]
            if covered is not None:
                released -= buffer.release_below(covered[1])
                buffer.reseed_group()

            # **And the fence has to clear the PUBLISHED table, not just the
            # replica.** `strip_local_state` ran fifty lines up, before this
            # method knew where the published table was, so it reserved
            # `RESTORE_RESERVE` above the sequence the REPLICA carried. That is
            # the right floor only while the replica's sequence is the highest
            # offset anyone issued, and the paragraph above is this method
            # admitting it is not: the whole reason for the reconcile is that
            # the bucket routinely holds ranges the replicated `extent` rows
            # have never heard of.
            #
            # Left alone the restored log issues offsets the published table
            # already holds, which is I9 broken in the one direction nothing
            # detects. Reproduced: replica at seq 301, 3M rows loaded and
            # pushed, `published_through` 2,864,714 — and the restore resumed at
            # 1,048,877, inside the published table's range. The reissued rows
            # seal into the rebuilt staging table, `publish` reports success and
            # pushes nothing for ever because the file sits below the published
            # table's floor, and a full `scan()` returns 1,048,881 rows of the
            # 3,000,600 acknowledged: the union truncates the published leg at
            # the colliding staging span, so ~1.95M published rows are served by
            # no leg at all while five offsets durably name two different rows.
            #
            # The report already knew. `skipped` was `(highest + 1, resumed - 1)`
            # over a `highest` that takes this frontier into account, so it came
            # back INVERTED — `(2864715, 1048876)` — which is what an invariant
            # looks like when it is computed and then not checked.
            #
            # Reachable before bulk ingest existed, and cheaper with it: this
            # needs the published table more than `RESTORE_RESERVE` ahead of the
            # replica, which used to take a million rows through the buffer
            # during a sidecar outage and now takes one `reserve` that ships
            # almost no WAL. The hazard is lag plus restore either way, so it
            # is closed here rather than by refusing the load.
            wanted = frontier + RESTORE_RESERVE
            if wanted > resumed:
                resumed = buffer.reserve(wanted - resumed)[1]
        finally:
            buffer.close()

        # REBUILT, not restored. Its Parquet is on the machine that died and
        # its Iceberg metadata was never replicated, so there is nothing to
        # point at — see this method's docstring. Last, so it is the moment
        # this root becomes a log.
        #
        # ALL OR NOTHING, because `create` is two commits: the catalog row that
        # makes the root openable, then the sort order. A failure between them
        # left a root `restore` refuses to retry — telling the operator to
        # delete a log whose data is in fact intact — declaring no sort order,
        # with no replication config and no recovery report. Undoing the row
        # puts the root back to unopenable, which is the state the whole
        # ordering above exists to guarantee.
        try:
            LogTable.create(layout, table_schema(schema), sort_by)
        except TableAlreadyExistsError:
            # Undo NOTHING here. The row was already there, so this call did not
            # make it and dropping it would destroy a live log's only pointer to
            # its local files. Reached with `buffer.db` absent and the row
            # present — which the `FileExistsError` guard above cannot catch,
            # keying as it does on the buffer — and reproduced: a table at
            # offsets 1..1152 with the published table at 504 lost the reference
            # to 505..1152, sealed locally and never published, with
            # `litelink.open` then answering "use new() to create one".
            raise
        except Exception:
            with contextlib.suppress(Exception):
                LogTable.forget(layout)

            raise

        log = cls.open(layout.root, name, s3=options)

        # The published table's row taken afresh. The staging table was rebuilt
        # empty and every published file now sits below it, so nothing a
        # manifest said before the failover describes it. If the published table
        # cannot be read now, the row stays missing and every query reads it
        # until a publish pass can.
        log._tiers.drop()
        log._backfill_manifest()

        # REWRITTEN, now that the policy is back. The config above had to be
        # written before `buffer.db` existed — that is the chicken-and-egg this
        # method is for — so it could not carry `wal_retention`, which lives in
        # the `meta` that was still in the replica. Left as it was, the box
        # that just took over would replicate under litestream's defaults
        # rather than the window the log records, and RUNTIME presents this
        # file as the one an operator then runs the sidecar against.
        log.write_replication_config()

        # DERIVED from the highest offset anything still holds, not from
        # `resumed - RESTORE_RESERVE`. A resumed restore reserves twice, so
        # subtracting one window named only the last of them — measured, a log
        # that skipped 1101..2098252 reported 1049677..2098252.
        #
        # `recovered` is likewise the count AFTER the reconcile, which the
        # subtraction above it performs: rows the published table already held were
        # released two lines later, and counting them as recovered describes a
        # buffer that no longer exists.
        local = log._table.span()  # noqa: SLF001
        buffered = log._buffer.span()  # noqa: SLF001
        # The PUBLISHED table's own frontier, read above, not
        # `published_through` — that reads the replica's `meta`, which is the
        # exact staleness the reconcile ten lines up exists to correct. It bites
        # whenever the release empties the buffer, which is the ordinary "died
        # shortly after a publish pass" shape: measured, 16,100 offsets reported
        # skipped that were present and readable. Nothing is lost by it, but the
        # documented response to a skipped range is to re-fetch from upstream,
        # and doing that would duplicate them.
        # Every one an end, so this is the first offset nothing holds — and
        # never below 1, where offsets start.
        held_end = max(
            1,
            0 if local is None else local[1],
            frontier,
            0 if buffered is None else buffered[1],
        )
        log._restored_from = _Recovery(  # noqa: SLF001
            recovered=released,
            resumed_at=resumed,
            skipped=(held_end, resumed),
        )

        return log

    # -- settings ----------------------------------------------------------

    @property
    def _schema(self) -> pa.Schema:
        """The declared columns, read from the log rather than remembered.

        A `WriteHandle` opened before a schema change would otherwise validate
        against the columns it was constructed with for the rest of its life. The
        buffer owns the durable copy and caches the decode, so this costs one
        keyed `meta` read.
        """
        return self._buffer.shape().schema

    def set_config(self, config: LogConfig) -> None:
        """Replace the operational policy (§12).

        Every knob here governs future work only — when to seal, how long to
        keep, what to compact — so this needs no rewrite. `sort_by` and the
        schema are not in here precisely because they do.
        """
        with self._lock:
            # The same claim `set_published` takes, and for the same reason.
            # `validate` refuses a PAIR — `wal_replication` with no remote
            # published table to replicate to — so the two halves have to be
            # decided together. Reading the other half durably is not enough on
            # its own: read and write as two transactions and the check is only
            # a statement about the past, so the two setters could each pass
            # against a state the other was about to change and assemble the
            # refused pair between them. The pair this first guarded was an
            # evict-on-upload policy with no archive to evict into, and the next
            # maintenance pass executed it faithfully, deleting the only copy of
            # everything sealed. Verified by execution before this claim
            # existed.
            lease = self._claim_settings()

            try:
                validate(
                    self._schema,
                    self._buffer.sort_by(),
                    config,
                    self._buffer.get_meta(_PUBLISHED_KEY) or None,
                )
                # Asked again at the write. The claim makes the read and the
                # write one decision only while it is held, and a stall past
                # the TTL between them is the threat the TTL exists for: the
                # other setter takes the lapsed claim lawfully, validates
                # against the half this one has not written yet, writes its
                # own — and between them they record the pair `validate` just
                # refused. Every data commit already asks this; the setters
                # stopped one line short.
                checkpoint(lease.renew)
                # The only write. There is nothing to fan out: `Maintenance`,
                # the seal target and `config` all read this row rather than
                # keeping copies of it.
                self._buffer.set_meta(_CONFIG_KEY, config.to_json())
            finally:
                lease.release()

    def recovery(self) -> _Recovery | None:
        """What `restore` recovered, or None on a log opened normally.

        Two numbers an operator needs and cannot get later: how many rows came
        back from the replica, and which offsets were skipped to avoid
        reissuing ones the dead machine had already served.
        """
        return self._restored_from

    def set_published(self, published: str | None) -> None:
        """Point the log at another published table, or back at its local
        default with None (§5). There is no detached state (#98).

        Takes the whole-log claim, so it cannot interleave with a publish pass,
        a merge, an eviction or the other setter — `validate` refuses a PAIR, so
        the policy and the location have to be decided together. The shipped
        writer calls this on every restart while a maintainer runs in another
        process, so it waits for maintenance rather than failing on the first
        try; see `_claim_settings`.

        **Re-pointing does not move anything, but it is reversible.** Rows
        already evicted into the old published table stay there, and the read
        path resolves only the published table the log currently names — so
        while pointed elsewhere they are not readable through this log, and a
        full `scan()` returns fewer rows than were written, silently.

        Pointing BACK undoes that. Each published table writes
        `version-hint.text` beside its metadata at every commit, so a prefix
        whose catalog entry this log dropped is registered from what the bucket
        itself says rather than created empty over the top of it. Only a
        repairing caller adopts, which `set_published` is; a read still leaves
        the published leg out until one has run.

        **A published table AHEAD of this log is refused.** One whose span
        reaches the next offset to be assigned is another log's history, and
        attaching it wedges this one silently — nothing is ever pushed,
        eviction pins, and local disk grows without bound. `litelink.restore`
        is the operation for resuming that log here. Pointing back at a
        published table this log has moved past is unaffected, and supported.

        **One writer per published table is otherwise assumed, not checked.**
        The hint records where the metadata was at the last commit THIS log
        made. Another writer touching that published table while this log
        pointed elsewhere would leave the hint behind its true state, and
        adopting it would strand the commits made in between. That is §13's
        published-identity seam; the contract is one writer per log, and
        nothing here enforces it.

        What re-pointing no longer does is disturb what the log already knows.
        There is no watermark to carry across: each pushed file records the
        bucket its copy went to (§4a), so ranges the old published table holds
        go on naming it, eviction keeps asking about the published table that
        is configured now, and compaction keeps refusing to merge across any of
        them.
        """
        # NORMALISED here, not in `_repoint`, so every guard below and the
        # write see the same location. `set_published("")` once detached past
        # guards that saw it as non-None — 7,828 acknowledged rows lost. None
        # and "" now both mean the local default (#98): there is no detached
        # state, so nothing for a guard to miss.
        published = (published or "").rstrip("/") or self._layout.default_published

        # Re-stating where the log already points does nothing: no claim, no
        # write, no network. A writer that declares its published table on
        # every restart must not wait for maintenance, or fail during an
        # outage, to be told what it already knew.
        if published == self._published.location():
            return

        with self._lock:
            lease = self._claim_settings()

            # Every write inside, so a failure — a full disk, a busy database —
            # releases the claim instead of stranding it for its whole TTL.
            try:
                # Read, checked and written UNDER the claim. `validate` refuses
                # a PAIR, and reading the other half durably is not enough on
                # its own: read and write as two transactions with nothing
                # between them, and the check is only a statement about the
                # past — `set_config` could land its half in the gap, so
                # between them the two calls assemble the very pair neither
                # would accept, and the next maintenance pass executes it.
                #
                # `set_config` takes this same claim, which is what makes the
                # two serialise. It is the rule §4a already states for data,
                # applied to the configuration that governs it.
                validate(self._schema, self._buffer.sort_by(), self.config, published)
                self._refuse_published_ahead(published)
                self._refuse_published_behind(published)
                # Asked again under the claim: another process may have made
                # the same move meanwhile, and then there is nothing to do.
                if published == self._published.location():
                    return

                # A MOVE opens — or creates, or adopts through its
                # `version-hint.text` — the table at the new location before
                # anything is written, and fails the call if it cannot. Best
                # effort, it reported success while the catalog still named
                # the old table, and every other process refused the published
                # table until a maintenance pass repaired it.
                self._published.adopt(published)

                # The other half of the same rule; see `set_config`.
                checkpoint(lease.renew)
                self._repoint(published)
            finally:
                lease.release()

    def _refuse_published_behind(self, published: str | None) -> None:
        """Refuse a published table missing a column this log has.

        Logs are immutable now (§9), but a log that used `add_column` under an
        older release can still meet a published table it pointed away from
        before that change: pointing back does no schema work —
        `open_published` only DECLARES a schema when it creates a table, and
        nothing in `src/` re-declares an existing one — so that published table
        stays narrow, and every later push fails permanently:

            ValueError: PyArrow table contains more columns: region.

        `publish` then never advances, eviction's I4 clamp pins on the unpushed
        files, and local disk grows without bound.

        Refused rather than repaired. Widening an existing published table is
        `union_by_name` against a table this log did not create, which belongs
        with `rewrite_published` and wants its own design; doing it quietly from
        a setter would mean `set_published` mutating a shared published table as
        a side effect. Tracked as an issue.
        """
        if published is None:
            return

        # Read from the bucket, not through `self._published`: that object
        # takes its URI from `meta`, which still names the OLD published table.
        try:
            columns = published_columns(self._layout, published, self._published.s3)
        except Exception:
            # A bad minute in object storage is not a schema disagreement.
            # `_refuse_published_ahead` treats it the same way: cannot tell, so
            # pass rather than refuse a published table that may be fine.
            return

        if columns is None:
            return

        declared = set(self._buffer.shape().schema.names)
        missing = sorted(declared - set(columns))
        if missing:
            msg = (
                f"published table at {published} is missing {missing}, which this log has "
                "added since. Attaching it would make every later publish fail "
                "permanently and pin eviction, so the log would grow without "
                "bound. Widen the published table first"
            )
            raise ValueError(msg)

    def _refuse_published_ahead(self, published: str | None) -> None:
        """Refuse a published table whose span reaches past this log's next
        offset.

        That published table belongs to a different log's history, and
        attaching it wedges this one SILENTLY. Traced: `publish` computes
        `floor` from the published table's span, every staging file sits below
        it, so `pending` is empty and nothing is ever pushed. The watermark is
        still written, eviction's I4 clamp finds no `extent` rows and pins at
        zero, and local disk grows without bound while `publish()` returns
        success having uploaded nothing. No error surfaces at any step.

        It is reachable by the obvious failover attempt — `litelink.new` on a
        second box, then `set_published` at the old prefix — which is exactly
        what `litelink.restore` exists to do properly.

        **Its last offset `>= next_offset`, not "overlaps".** Pointing back at a
        published table that holds offsets this log has moved PAST is supported
        and tested; the published table's ranges simply sit below the staging
        ones. Only a published table reaching at or above the next offset to be
        assigned is describing a stream this log is not.

        **Read through `version-hint.text`, never `open_published`.** At this
        point `meta` still names the OLD published table, so `Published.table()`
        opens that one. Going to `open_published` for the new prefix fails both
        ways: with `repair=False` the catalog row names the old published table
        and the boundary check raises on every ordinary re-point; with
        `repair=True` it drops that row as a side effect of what is meant to be
        a read.

        Absent, unreachable, or unreadable all PASS. `_repoint` deliberately
        tolerates a published table that does not exist yet — "configuring one
        is a statement of intent, not a claim that the bucket exists" — and this
        is called on every writer restart, so it must not fail closed on a bad
        minute in object storage.
        """
        if published is None:
            return

        try:
            covered = published_span(self._layout, published, self._published.s3)
        except Exception:
            return

        if covered is None:
            return

        nxt = self._buffer.next_offset()
        # Ahead when its last offset, `covered[1] - 1`, is at or past `nxt`.
        if covered[1] > nxt:
            msg = (
                f"the published table at {published!r} holds offsets up to {covered[1] - 1}, at or "
                f"above this log's next offset ({nxt}) — it is another log's "
                f"history. Attaching it would push nothing and pin eviction, "
                f"silently. To resume that log here, use litelink.restore"
            )
            raise ValueError(msg)

        # And it must be a published table this log has SEEN. A populated
        # prefix that this log holds no `extent` row for is somebody else's,
        # whatever its offsets look like — and offsets are all a comparison has
        # to go on, since two logs of the same name both start at 1.
        #
        # Attaching one cannot be contained downstream, which two attempts
        # tried: the watermark is raised to the published table's span by
        # `confirmed` on every pass, and this log's own `extent` rows are
        # written for the published table's ENTIRE manifest by `_push`'s
        # backfill within one publish pass. Measured — a bound derived from
        # either moved with the contamination. The backfill is right to trust
        # the manifest; what it needs is for the published table to be ours,
        # and §13's identity token is what would prove it. Until then the check
        # belongs at the moment the log is pointed, before anything has
        # laundered anything.
        #
        # Pointing back passes: ranges pushed to a published table go on naming
        # it after the log points elsewhere (§4a), so returning to one finds its
        # own records intact. A fresh prefix passes too — `covered` is None
        # above.
        if not self._buffer.published_records(published, 0):
            raise _foreign_published(published)

    def _repoint(self, published: str | None) -> None:
        """Record the new location, with the maintenance lease held."""
        # Already normalised by `set_published`, which is the only caller and
        # does it before its guards rather than after. Repeated here so this
        # method is correct on its own terms: every path builder strips
        # trailing slashes, so `s3://b/p` and `s3://b/p/` are the same published
        # table everywhere except here — where the difference would read as a
        # move and reset the watermarks of a published table that genuinely
        # holds data.
        normalised = (published or "").rstrip("/") or self._layout.default_published
        # ONE transaction, because the three facts are only true together.
        #
        # Where the published table is, and the two watermarks describing what
        # the PREVIOUS one held: the confirmed one eviction acts on, and the
        # frontier compaction reads to decide which files are already the
        # published table's business. As separate writes a crash lands between
        # them, and BOTH orders have cost a defect — watermark last leaves the
        # new published table carrying the old one's promise, which eviction
        # believes; watermark first leaves the old published table with a
        # frontier of zero, and compaction does not wait for a publish the way
        # eviction does, so it merges across a boundary the published table
        # already holds. There is no ordering that is safe, so there is no
        # ordering.
        # Whether this is a move is decided against the DURABLE location, in
        # the transaction that acts on the decision. This object's memory of
        # where the published table is goes stale the moment another process
        # re-points it, and nothing but a publish pass refreshes it — so a
        # maintainer re-asserting the published table it already has would read
        # its own staleness as a move and zero the watermarks of a bucket that
        # holds the data.
        # The published table's tier row describes the published table being
        # left, so a move forgets it BEFORE the log points anywhere new:
        # forgotten first, a read can only include the published table, never
        # trust the old one's row for the new one. A restatement keeps it — the
        # shipped writer calls this on every restart. Compared against the
        # durable location, which nothing else can move while this holds the
        # claim.
        if self._published.location() != normalised:
            self._tiers.drop()

        self._buffer.set_meta_moved(
            _PUBLISHED_KEY,
            normalised,
            {Maintenance.PUBLISHED_THROUGH_KEY: "0"},
        )

        # Reaches the maintainer and the reader because all three hold this
        # object. `evict` asks it whether I4 is owed anything, and a setting
        # that stopped at `WriteHandle` would leave the maintainer deleting the
        # only copy of rows a published table was just configured to receive.

        # The new published table's tier row, while the claim is held, so reads
        # stop fetching it for every query — from the table `set_published`
        # already adopted, so nothing is repaired here. Best effort: the first
        # publish pass records it otherwise.
        with contextlib.suppress(Exception):
            adopted = self._published.table()
            if adopted is not None and not self._tiers.has():
                self._record_published_row(adopted)

    def set_sort_by(self, sort_by: Sequence[str], *, rewrite: bool) -> None:
        """Change the sort order, re-clustering every file the staging table
        owns.

        §7 calls `sort_by` a read-shape decision rather than a tuning knob:
        it declares which predicates prune, and the clustering that makes them
        prune is baked into each file when it is written. So a new order that
        is only declared would apply to future seals and silently leave every
        existing file clustered the old way — the same predicate fast on recent
        data and slow on older data, with nothing to indicate why.

        `rewrite` must be passed explicitly. It is the honest name for the
        cost: every file this rewrites is read, re-sorted and replaced.

        **The published prefix is not re-clustered, and cannot be.**
        `_rewrite_run` skips any run holding an offset the published table
        holds, because a staging rewrite there would commit a file straddling
        the published table's span and nothing re-cuts a staging straddler. So
        on a published log this changes three declarations and rewrites only
        what publish has not yet taken — and `rewrite_published` is not the
        other half either: it re-ingests from the
        first badly-SIZED file onwards, so a well-sized published prefix is
        never a candidate and keeps its original clustering for ever.

        That is §6's "sealed once and never rewritten" applied to history, not
        an oversight — but this docstring used to say "every existing file",
        which on a published log was a claim the code did not honour and did
        not report. A re-sort is a decision for a log's staging window; the
        published table keeps the clustering it was written with.
        """
        with self._lock:
            requested = tuple(sort_by)
            validate(self._schema, requested, self.config, self._published.uri)
            if requested == self._buffer.sort_by() and not rewrite:
                # Restating the order the log already declares, without asking
                # for the data. Nothing to declare, nothing accepted, so this
                # is the no-op it looks like.
                #
                # It used to return here whatever `rewrite` said, and that left
                # the one crash gap nothing healed: a crash after `meta` and
                # before the rewrite finished leaves the declarations NEW and
                # the files OLD, and the natural retry found the orders equal
                # and returned without doing anything. Reproduced by review.
                # `rewrite=True` on an unchanged order is now the way to finish
                # it — the whole table re-clustered, which is what that flag
                # already means and what an interrupted rewrite needs.
                return

            if not rewrite:
                msg = (
                    "changing sort_by re-clusters every file the staging table "
                    "owns, and leaves the published prefix as it is; "
                    "pass rewrite=True to accept that cost"
                )
                raise ValueError(msg)

            # Under the maintain lease, because a rewrite IS a compaction —
            # same claim record, same deterministic output path, same commit.
            # Without it this reached that path beside a running `maintain()`
            # in another process: two writers to one `compaction_path`, and a
            # single-row `compacting` intent each would clear from under the
            # other, leaving a half-written file nothing could name.
            #
            # Taken with the bounded WAIT the other administrative operations
            # use, not a single attempt. A single attempt loses to any pass
            # already running, which made the documented way to finish an
            # interrupted re-sort — the `rewrite=True` retry above — fail
            # spuriously beside a maintainer that never stops. `set_config`
            # measured one startup in six lost to exactly this.
            lease = self._claim_settings()

            try:
                # `meta` LAST of the durable writes, because it is what
                # every decision reads: a crash before it leaves the log
                # deciding by the OLD key, which is the order every existing
                # file is already in, so the operation is simply not done.
                #
                # What it does NOT leave is the declarations untouched, and
                # this comment used to say otherwise — "the log goes on using
                # the OLD key that the tables still declare", which a review
                # reproduced as false. After the declaration writes and before
                # `meta`, the table declares NEW while `meta` still says OLD.
                # That mismatch is declaration-only, and the natural retry
                # heals it: the check above compares against `meta`, which is
                # unchanged, so a repeated call proceeds and completes all of
                # them.
                #
                # Writing `meta` FIRST is what does not heal. `meta` would say
                # NEW while every file stayed OLD, and the retry would find the
                # orders equal and return — which is the gap the `rewrite=True`
                # branch above now fills.
                self._table.set_sort_order(requested)
                # The published table's declaration too, and BEFORE `meta` like
                # the staging one: `open_published` declares an order only on a
                # table it creates, so a published table that already exists is
                # re-declared here or never. Missing it entirely left a published
                # table created after a re-sort born declaring the old key, with
                # every file pushed into it clustered by the new one.
                self._published.redeclare_sort_order(requested)
                self._buffer.set_meta(_SORT_KEY, json.dumps(list(requested)))
                self._maintenance.rewrite_sorted(
                    heartbeat=lease.renew, owner=lease.owner
                )
            finally:
                lease.release()

    def _claim_settings(self) -> Claim:
        """Take the whole-log claim the configuration operations share.

        Retried, not refused on the first try. `set_config` and `set_published`
        exclude each other because `validate` refuses a PAIR and the two halves
        have to be decided together — but they also collide with ordinary
        maintenance, and the shipped writer calls both on every restart while a
        maintainer runs continuously. Measured before this wait existed: one
        startup in six failed, which turns a routine restart into a coin toss.

        Bounded, so a genuinely long merge still surfaces rather than hanging.
        A configuration change is administrative and rare; waiting a few
        seconds for a compaction to finish is the right trade, and failing
        after that is honest.
        """
        wait = getattr(self, "_settings_wait", _SETTINGS_WAIT_S)
        deadline = time.monotonic() + wait
        while True:
            claim = self._lease(MAINTAIN_ROLE)
            if claim.acquire():
                return claim

            if time.monotonic() >= deadline:
                msg = (
                    "another owner has held a claim over this log for "
                    f"{wait:.0f}s; maintenance may be mid-pass. Retry."
                )
                raise RuntimeError(msg)

            time.sleep(random.uniform(0.01, 0.05))

    def _lease(self, role: str, start: int = 0, end: int = EVERYTHING) -> Claim:
        """A fresh claim on `[start, end)` for this attempt.

        Minted per call rather than held as a field. A field would fix one owner
        for the whole handle, and two threads sharing it would then re-enter
        each other's claim — leaving it excluding nothing inside a process.

        The default range is the whole log, which is what a configuration change
        needs: re-pointing a published table or rewriting its files is not an
        operation on an offset interval, so it excludes every pass rather than
        commuting with any of them.
        """
        return self._buffer.claim(role, start, end, new_owner())

    # -- write ---------------------------------------------------------------

    def end_offset(self) -> int:
        """The offset the next append will receive — an EXCLUSIVE upper bound.

        Half-open, matching the `[start, end)` seal ranges in §4, so this on a
        fresh log is the first offset it will ever assign and never a sentinel.

        **A writer answers a different question than a reader, and inheriting
        the reader's answer was wrong twice.** `LogHandle.end_offset` reports
        the offset after the last row THAT HANDLE CAN SERVE, which is what a
        reader assembled from several tiers can honestly claim. A writer is
        asked where its next row will land, and only `sqlite_sequence` knows
        that — it is the thing that assigns it.

        The two coincide on a healthy log and diverge exactly where the staging
        table is empty while the published table holds rows:

        - **After `restore`.** The fence reserves `RESTORE_RESERVE` offsets, so
          the sequence is 1,048,576 above the published table's frontier and the
          rebuilt staging table is empty. The inherited version reported 801
          where the next append took 1,049,377 — a caller reading it as "what
          comes next" would collide with the fence I9 exists to hold.
        - **After `evict` empties the table.** The inherited version reads
          published metadata, so a writer's cheapest observation became a
          network call that RAISES when the bucket is unreachable. It returned
          3,001 locally before, and could not fail. `docs/API.md` calls
          `published_through()` against `end_offset()` the number to alarm on,
          which made the monitoring call the one that dies in an outage.

        So this reads SQLite and nothing else. It cannot fail and cannot lag.
        """
        return self._buffer.next_offset()

    def append(self, row: Row) -> int:
        """Append one row. Returns the assigned offset.

        Durable when this returns — one SQLite transaction, `synchronous=FULL`
        (§3). There is no in-memory write buffer to flush, and that absence is
        the point: it is the failure the README opens with.

        A caller-supplied `offset` is rejected (I11), and so is a column this
        log does not have: the insert is built from the SCHEMA's columns, so an
        unknown key would otherwise be dropped before any SQL exists and this
        would return an offset for a row it had silently truncated.
        """
        return self.extend([row])[0]

    def extend(self, rows: Iterable[Row]) -> list[int]:
        """Append many rows in ONE transaction. Returns the assigned offsets.

        The batch is the durability unit: one fsync amortised across the batch,
        which is the whole of §3's throughput story. It carries no meaning
        beyond that — see §1 on why there is no transaction id column.

        **One bad row rejects the whole batch**, because the batch is one
        transaction: a row naming a column this log does not have raises before
        anything commits, and no offset is consumed. Partial acceptance would
        be worse than refusal — it would hand back offsets for some rows while
        the caller learns nothing about the rest.
        """

        # No lock. `Buffer` serialises its own connection, which is the only
        # thing two appending threads share — and the append decides nothing
        # beyond the cut it records (see `extent`). It does not measure,
        # compare, signal, or start anything: a maintainer calls `seal_due` and
        # finds the work waiting, exactly as it calls `maintain` and finds
        # files to compact.
        return self._buffer.append(rows)

    # -- bulk ingest ---------------------------------------------------------

    def ingest(
        self,
        source: pa.Table | pa.RecordBatchReader,
        *,
        publish: bool = True,
    ) -> tuple[int, int] | None:
        """Write Arrow straight into the immutable tier (§13.4). Returns the
        offsets it took as `[start, end)` — half-open, like every range
        litelink reports — or None for an empty source.

        The SQLite buffer exists to make a row durable before it is in Parquet
        (§2). A bulk load's source is ALREADY durable — a Parquet corpus, or an
        Arrow table read from one — so every row pushed through the buffer at
        `synchronous=FULL` pays once more for a guarantee it already has.
        Measured here on 400k rows, where fsync is cheap and the gap is
        therefore understated: 182,801 rows/s through the buffer against
        5,103,266 rows/s writing Arrow straight to Parquet. On the deployment
        that wanted this, the same load measured about 32 hours.

        **This path refuses concurrency rather than surviving it**, and that is
        a decision rather than an omission. Everything about a load is sized in
        hours, and a design that stayed correct beside live capture needs a
        range-aware coverage predicate `register` does not have: `_covers`
        answers from the span's end alone, so the FIRST live seal after the
        reserve raises that bound past this range's end and the commit is
        declined — which `_write_and_commit` turns into a queued deletion and a
        normal return. Reproduced with one ordinary 50-row batch and one
        ordinary seal: 3000 rows acknowledged, 50 readable, no error at any
        step. Concurrent bulk ingest is its own problem and its own issue.

        So the whole log is claimed for the whole load, and three reads have to
        pass before it starts. They are not three heuristics: acknowledged rows
        live in exactly one table, `buffer`; exactly one function turns them
        into a file, `_write_and_commit`; it has exactly two callers, whose
        ranges come from `pending_group()` and `pending_seal()`; and the only
        two things that put a range into `pending_group()` — `_cut` and
        `close_open_group` — both require the open group to have taken rows. A
        third caller of `_write_and_commit`, or a second function that turns
        buffer rows into Parquet, needs a fourth read.

        **Concurrent appends are excluded by §1, not by the claim.**
        `Buffer.append` consults no claim, and putting one on the hot path is
        not acceptable. `ingest` is called BY the single writer, in its process,
        and that is part of the contract rather than something checked.

        **`wal_replication` is not refused, and the reason it once was is worth
        recording.** WAL shipping genuinely cannot carry a bulk range: with
        replication on the buffer IS the off-box copy until the published table
        has the range (§3a), and these rows never enter the buffer. Reproduced
        against a zero-lag replica: 920 rows acknowledged, 420 restored,
        `recovery()` reporting plain success.

        That is a true statement about SCOPE, and it was briefly turned into a
        refusal — load with replication off, turn it on afterwards. Which is
        strictly worse, because `_discard_on_seal` reads the same flag: turn
        replication off and the next seal stops retaining its rows, so the
        buffer's copy of everything ALREADY captured is dropped. Measured on a
        replicated log with a published table: 300 sealed rows retained in the
        buffer, `set_config(wal_replication=False)`, one more seal, and all 300
        are gone from it with the published table holding none — 350
        acknowledged rows in local Parquet alone. To load rows that could never
        be replicated, the workaround stripped the off-box copy from rows that
        were.

        It also fixed nothing: the range is uncovered until `publish` either way,
        and `recovery()` reports plain success either way. A refusal that
        relocates an exposure, adds a second one, and leaves the reporting
        defect untouched is not a safety measure. What the caller needs is the
        scope stated, which is the paragraph below.

        **This pushes its own output to the published table**, and
        `publish=False` opts out. That is the fix for the paragraph below, which
        described the old behaviour: a load's rows never enter the buffer, so
        WAL replication cannot carry them and the published table is their only
        second copy — while an ordinary `publish` held the load's short last
        file back behind `stable_prefix`. On a stream that then went quiet the
        run never settled. Measured on the deployment that found it: 113,399
        rows on one disk, with `coverage()` reporting no gap.

        The push runs after the load is durable, so a failure leaves the rows
        loaded and raises saying so — retry the push, never the load, which
        would reserve a fresh range and duplicate it.

        **The published table is still a loaded range's only second copy**,
        which is what the push above is for. An ORDINARY `publish` is not enough
        and that is why this does not call one: `stable_prefix` holds back a
        trailing run still under `target_compact_size`, because a run with room
        in it may yet take files that have not been written — and the last file
        of a load is short unless the load divides evenly. Measured: 3000 rows
        loaded into 11 files, one `publish()`, `published_through()` 2830, the
        last 170 rows in one staging file with `coverage()` reporting no gap.

        That was documented as terminating, since the run settles once roughly
        another `target_compact_size` of rows arrives above it. On a stream that
        goes quiet it does not: 113,399 loaded rows sat on one disk on the
        deployment that found this, across nine streams and ~698,000 rows. The
        window scaled by the arrival rate, and a slow stream has none.

        **Compare `published_through()` against the `end - 1` this returns
        whenever the push did not run** — `publish=False`, or a push that raised
        after the load had landed. Until they meet, the corpus you loaded from
        is the range's second copy, which is the same durability this path's
        whole premise rests on: the source is already durable, which is why the
        buffer is not in the way. Do not enable replication expecting it to
        close that window — nothing it ships contains these rows.

        Files come out sized at `target_compact_size` and sorted by `sort_by`,
        which is what makes them indistinguishable from a compacted file and so
        born past the maintenance lifecycle: `runs()` closes a run when the next
        file would exceed the budget, so a file already at it forms a run of
        one, and `_merge` rewrites only at `compact_min_files`. Sorted WITHIN a
        file, like a seal's output — offsets are materialised in input order and
        then permuted by the sort, so the range stays dense while the rows move.
        "Sorted" and "contiguous" are claims about two different columns.

        **A load that fails leaves a hole, and that is the accepted cost.** The
        offsets of the reservation being written when it failed are gone; §6
        needs files non-overlapping and adjacent in offset order, not free of
        integer gaps, and every pass was measured correct on a gapped log. The
        one price worth stating rather than discovering: compaction will merge
        across the gap, and a merged file spanning one can never be re-cut by
        `rewrite_published`.

        Takes a `pa.Table` or a `pa.RecordBatchReader` and nothing else.
        Parquet-to-Arrow is the caller's — `pq.ParquetFile(...).iter_batches()`
        is a reader, and how they got there is theirs. Memory is bounded at one
        output file either way, because a Table is a reader that ends after one
        pull.

        Returns None for a source with no rows, which is the one answer a range
        cannot express.
        """
        # Asked here as well as by the buffer's trigger: a load writes Parquet
        # straight into the table and never inserts a buffer row.
        if self._buffer.retired() is not None:
            raise self._buffer.retired_error()

        shape = self._buffer.shape()
        reader = source.to_reader() if isinstance(source, pa.Table) else source
        _refuse_foreign_schema(reader.schema, shape.schema)

        lease = self._lease(INGEST_ROLE)
        if not lease.acquire():
            msg = (
                "another owner holds a claim over this log; bulk ingest needs "
                "the whole of it for the whole load"
            )
            raise RuntimeError(msg)

        try:
            self._refuse_unfiled_rows()
            # Once, under the claim. The arithmetic below says `register`
            # cannot decline, and that argument is about the CURRENT table —
            # a handle that has only appended for hours holds the snapshot it
            # opened with.
            self._table.reload()

            loaded = self._ingest_chunks(reader, shape, lease)
        finally:
            lease.release()

        # AFTER the claim is released, so the push takes the maintenance lease
        # in the ordinary way rather than running under a role that excludes
        # different things. Nothing is lost by the gap: a compaction landing in
        # it merges files the push would then take instead, which is the same
        # rows by another name.
        #
        # `push_unsettled`, because the trailing run is precisely what has no
        # second copy — `stable_prefix` holds a load's short last file back for
        # a merge that a quiet stream never earns.
        if loaded is not None and publish:
            try:
                # COMPACT first, and it is not tidiness. The push below takes
                # the whole trailing run, so every undersized seal still sitting
                # below the load goes to the published table with it — and
                # compaction will not merge what the published table holds, so
                # they stay small there for ever. Merging them in staging first
                # collapses that to the one file a run genuinely cannot fill.
                # Measured on five small seals: six undersized objects pushed
                # without this, one with it.
                self.compact()
                self.publish(push_unsettled=True)
            except Exception as exc:
                # The LOAD succeeded and its rows are durable in Parquet; only
                # the second copy is missing. Saying so leading with that is the
                # difference between a caller retrying the load — which would
                # reserve a fresh range and duplicate it — and retrying the push.
                msg = (
                    f"loaded offsets [{loaded[0]}, {loaded[1]}) successfully, but "
                    f"could not push them to the published table: {exc}. The rows are in "
                    f"local Parquet and are NOT yet second-copied; retry with "
                    f"publish(push_unsettled=True) rather than re-running the "
                    f"load, which would reserve a new range"
                )
                raise RuntimeError(msg) from exc

        return loaded

    def _refuse_unfiled_rows(self) -> None:
        """Refuse an ingest while any acknowledged row is still owed a file.

        The reservation goes ABOVE everything the log has issued, so a row left
        below it lands in a file that spans the reservation — and §6 forbids two
        files covering one offset. Transiently it is worse than that: `_union`
        bounds the buffer leg by the table's span, which the bulk file raises
        past the stranded row, so the row is in no leg of the read at all.
        Measured: 501 acknowledged, `scan` returned 500. Durably, the next seal
        cuts from that row upward and commits a file overlapping the bulk range,
        which nothing objects to — `_write_and_commit` passes no `start`, so
        `_refuse_straddle` never fires on the seal path.

        **The obvious one-read version of this is not enough**, and the failure
        is silent. `seal()` cuts and then returns even when it sealed nothing —
        losing the lease is not a failure — so a writer that appends and calls
        `seal()` while a maintainer holds the range is left with a FRESH empty
        open group and its rows sitting in a group already queued. Checking the
        open group alone passes; the rows are below `start`, in no file; the
        maintainer drains its queue, `_covers` declines the file, and
        `finish_seal(discard=True)` deletes them. The resulting table is
        contiguous, non-overlapping and undetectably wrong.

        `pending_seal()` is the same shape one step later, and worse: `recover()`
        runs unguarded at every `open`.
        """
        queued = self._buffer.pending_group()
        if queued is not None:
            msg = (
                f"the seal queue still holds {queued[0]}-{queued[1]}: bulk "
                "ingest reserves offsets above everything this log has issued, "
                "and a file cut from those rows afterwards would span the "
                "reservation. Call seal() and await_seal() first."
            )
            raise RuntimeError(msg)

        flight = self._buffer.pending_seal()
        if flight is not None:
            msg = (
                f"a seal of {flight[0]}-{flight[1]} is in flight: bulk ingest "
                "needs every acknowledged row already in a file. Call "
                "await_seal() first."
            )
            raise RuntimeError(msg)

        if self._buffer.open_group_started():
            msg = (
                "the buffer holds rows no seal has been asked to cut: bulk "
                "ingest needs every acknowledged row already in a file. Call "
                "seal() and await_seal() first."
            )
            raise RuntimeError(msg)

    def _ingest_chunks(
        self, reader: pa.RecordBatchReader, shape: Shape, lease: Claim
    ) -> tuple[int, int] | None:
        """The loop: reserve, materialise, sort, write, register in batches.

        **A reservation per output file, not one for the load.** `reserve(n)`
        needs `n` up front and a `RecordBatchReader` cannot say how many rows it
        has; materialising to find out is bounded by memory and defeats the
        point at 160M rows. Consuming to a file's worth and reserving exactly
        that many keeps memory at one file, needs no branch between a Table and
        a reader, and leaves a stream that dies half way with N complete files
        registered and one reservation lost rather than one enormous one.

        Ranges stay contiguous with nothing computing or checking it: sequential
        reserves are adjacent, because `reserve` reads and advances one counter.

        **`register` cannot decline these, by arithmetic rather than by the
        claim**, and the difference matters to whoever later tries to shorten
        the claim believing they are trading only concurrency. `start` is
        `seq + 1` and AUTOINCREMENT never issues above `seq`, so every file
        already in the table ends at or before `start`, and `_covers` is False
        by construction — file by file, since after file N registers the
        frontier is its `end` and the next reserve starts there. The same
        arithmetic settles `published_through`, which is some file's last
        offset. What the claim actually buys is keeping a maintainer's `evict`
        or `compact` off the range while this runs.
        """
        config = self.config
        order = self._buffer.sort_by()
        offset_field = shape.table.field(0)
        # The load's `[first, last)`.
        first: int | None = None
        last: int | None = None
        staged: list[tuple[str, int, int, int]] = []
        try:
            for chunk in _chunks(reader, config.compact_size, config.compact_rows):
                rows = chunk.select(shape.columns).cast(shape.schema)
                # Before the reservation, which is what makes a refusal free:
                # after it, the chunk's offsets are a permanent hole.
                _refuse_non_finite(rows)
                start, end = self._buffer.reserve(rows.num_rows)
                rows = rows.add_column(
                    0, offset_field, pa.array(range(start, end), type=pa.int64())
                )
                if order:
                    rows = rows.sort_by([(c, "ascending") for c in order])

                rel_path = self._layout.ingest_path(start, end, uuid.uuid4().hex[:8])
                # I2: the path is in SQLite before the bytes are on disk, so a
                # crash before the commit leaves a file recovery can name rather
                # than one only a directory scan could find. `claim_output`
                # rather than `claim_seal` — see `INGEST_ROLE`.
                self._buffer.claim_output(start, end, rel_path)
                dest = self._layout.absolute(rel_path)
                dest.parent.mkdir(parents=True, exist_ok=True)
                write_parquet(rows, dest, config.compression)
                staged.append((rel_path, start, end, rows.nbytes))
                first = start if first is None else first
                last = end
                # `DEFAULT_TTL_MS` is 30 s and this path is sized in hours, so
                # without a renew per file the exclusion evaporates during the
                # first `pq.write_table`.
                checkpoint(lease.renew)
                if len(staged) >= _INGEST_BATCH:
                    self._commit_staged(staged, lease)
                    staged = []

            if staged:
                self._commit_staged(staged, lease)
                staged = []
        except BaseException:
            # Queued BEFORE the claims go, which is the whole of the ordering:
            # a unique name with no queue entry is a file this database can no
            # longer name. `drain` refuses anything the table references, so
            # queueing a file whose commit turns out to have landed is safe.
            self._abandon(staged)

            raise

        return None if first is None or last is None else (first, last)

    def _commit_staged(
        self, staged: list[tuple[str, int, int, int]], lease: Claim
    ) -> None:
        """Register a batch of written files in ONE commit, then record them.

        A decline is raised rather than swallowed. `_write_and_commit` queues a
        declined seal and returns normally, which is right there — another owner
        sealed the same range, so the rows are in a file either way — and is
        exactly wrong here, where nothing else holds these rows. That silent
        return is the shape this path was reviewed for.
        """
        checkpoint(lease.renew)
        added = self._table.register(
            [str(self._layout.absolute(rel_path)) for rel_path, _, _, _ in staged],
            end=staged[-1][2],
            published_through=self._maintenance.published_through(),
            # The last line of defence, which the seal path does not get: a
            # range partially overlapping the table is refused rather than
            # admitted into two files at once. It cannot fire here — see the
            # arithmetic in `_ingest_chunks` — and it costs one span read.
            start=staged[0][1],
        )
        if not added:
            msg = (
                f"the table declined a bulk range starting at {staged[0][1]}, "
                "which cannot happen while this log is quiescent and means "
                "something advanced the frontier during the load. Nothing was "
                "committed and the staged files have been queued for deletion."
            )
            raise RuntimeError(msg)

        for rel_path, start, end, held in staged:
            # What the file holds UNCOMPRESSED, which is the currency
            # `target_compact_size` and every `extent.bytes` are stated in —
            # never its size on disk, which on data that compresses 8:1 would
            # have compaction merge eight already-full files into one.
            #
            # Arrow's own accounting rather than the appender's estimate,
            # which models the same layout and stays at or a little above it
            # (#84). This one is measured and O(1), because the table is here.
            #
            # Recorded AFTER the commit: a crash between the two leaves the
            # size unknown, and unknown reads as full, which is the direction
            # that leaves the file alone.
            self._buffer.record_file(rel_path, start, end, held)
            self._buffer.clear_compaction(rel_path)

    def _abandon(self, staged: list[tuple[str, int, int, int]]) -> None:
        """Queue written-but-unregistered files, then release their claims."""
        if not staged:
            return

        paths = [rel_path for rel_path, _, _, _ in staged]
        self._buffer.enqueue_deletions(paths, int(datetime.now(UTC).timestamp()))
        for rel_path in paths:
            self._buffer.clear_compaction(rel_path)

    # -- seal ---------------------------------------------------------------

    def seal(self) -> int | None:
        """Cut everything buffered into files. Returns the exclusive end offset.

        Deterministic in the only way that matters to the data: the cut lands
        where the caller asked, always, so a given sequence of appends and
        seals produces the same files whatever else is running. None means
        nothing was buffered — never that someone else was busy.

        Whether *this* call writes those files depends on who holds the lease.
        Use `await_seal` when the table itself has to have moved before you
        look at it.

        The lock is held for §4's steps 1 and 3 — claiming the range, and
        deleting the rows it covered — and released for step 2, which is all of
        the cost. Step 2 reads the buffer on its own connection and commits
        through its own table handle, so an append can proceed the whole time it
        runs.

        That division is what §4 already implies. Step 1 fixes `[start, end)`
        before the file exists, so rows arriving during step 2 land above `end`
        and cannot change what it is writing; step 3 is garbage collection
        rather than correctness, because §7's boundary already excludes those
        rows once the commit lands.

        One seal at a time. A second would claim a range overlapping the first,
        and `sealing` holds one row by design (§2).
        """
        # Cut unconditionally, and that is the whole contract. Cutting only
        # when the queue happened to be empty made this method's effect depend
        # on how far behind a sealer was: the rows the caller had just appended
        # went uncut, an older group was sealed instead, and the call could
        # return None having sealed nothing at all. Two appends and two seals
        # could then produce one file, not two.
        #
        # Unlocked, because the two calls need not be atomic together: another
        # sealer cutting between them leaves `last_queued_end` HIGHER, so this
        # drains a superset of its own rows, which is harmless. What must never
        # happen is failing to cut.
        #
        # `seal_due` does NOT come through here — it drains queued groups only
        # — or a quiet stream would emit a stub file every poll, which is the
        # pathology §6 exists to clean up after.
        self._buffer.close_open_group()
        target = self._buffer.last_queued_end()

        if target is None:
            return None

        # Then seal as much of it as this caller is entitled to. Losing the
        # lease is not a failure and not "nothing to do": another sealer holds
        # it and is working through the same queue, so the cut still becomes a
        # file, just not by this call. Blocking until it did would put a
        # caller's `seal()` behind another process's lease TTL, which is a
        # worse bargain than returning — `await_seal` is for callers who need
        # the table to have moved.
        while True:
            group = self._buffer.pending_group()
            if group is None or group[1] > target:
                return target

            if self._seal_queued() is None:
                return target

    def _seal_queued(self) -> int | None:
        """Seal the oldest queued group. None if none is queued.

        Split from `seal` so that draining the queue can never cut a group
        short: everything this writes was sized when its rows arrived.
        """
        # Outside `_lock`, because the lease excludes other OWNERS and the lock
        # sequences this process's own buffer writes — different jobs. Taking
        # it inside put two more fsyncs under the lock an append needs, and at
        # `synchronous=FULL` those are the expensive part of a small seal.
        #
        # One mechanism for both cases: owners are unique per attempt, so the
        # row that refuses a sealer in another process refuses one in another
        # thread on the same terms — and it lapses if this attempt dies
        # mid-seal, so another may finish what `sealing` records. The range
        # comes from the queue, not from whatever the buffer happens to hold
        # now. That is the difference between a file of `target_seal_size` and
        # a file of however much arrived while the sealer was getting here —
        # and it costs one indexed row read instead of the SCAN that asking the
        # buffer for its span used to.
        #
        # Read BEFORE the claim, because the claim is over this range and the
        # queue is what names it. Nothing is decided by the read: a second
        # sealer reading the same group loses the claim and returns.
        group = self._buffer.pending_group()
        if group is None:
            return None

        start, end = group
        # No lock here: the claim is the exclusion, and it already refuses
        # every other owner in this process and any other. A lock would be a
        # second answer to a question already settled.
        lease = self._buffer.claim(SEAL_ROLE, start, end, new_owner())
        if not lease.acquire():
            return None

        # A claim already naming this range means a previous attempt got at
        # least as far as recording it and then died — possibly AFTER its
        # commit landed. Replaying blindly re-registers a file the table
        # already holds, pyiceberg refuses it, `finish_seal` never runs, and
        # the group stays at the head of the queue failing forever. Sealing
        # wedges and the buffer grows without bound.
        #
        # `_recover_seal` is exactly the idempotent version — commit only if
        # the file is absent, retire the group either way — so a replay goes
        # through it. Detected with a keyed read rather than by asking the
        # table, which would walk manifests on every ordinary seal to learn
        # something only a replay needs to know.
        # Re-read the queue head UNDER the claim, because the read above
        # happened before it. A sealer that blocked in `acquire()` — which is a
        # `BEGIN IMMEDIATE` and can wait the whole busy timeout — may wake to
        # find another sealer has since sealed this group and moved on. Its
        # claim then succeeds because the range is free again, and it proceeds
        # on a group that no longer exists.
        #
        # The damage is not the wasted work. `sealing` holds ONE row, so this
        # sealer's `claim_seal` would delete the live sealer's, and that
        # sealer's `finish_seal` then returns False: its Iceberg commit has
        # landed, but its `extent` row is never named and its buffer rows are
        # never dropped. Reads stay correct — the buffer leg is bounded by the
        # table's span — so it is a wasted rewrite rather than loss, and the
        # next attempt re-names the group. Observed under three concurrent
        # sealers.
        #
        # Aborting is what the acquire-failure path above already does when
        # two sealers want the same group, so a caller draining in a loop
        # treats the two identically.
        if self._buffer.pending_group() != group:
            lease.release()

            return None

        claimed = self._buffer.pending_seal()
        if claimed is not None and claimed[1] == end:
            # In the `try` below in spirit, and now in fact: returning from
            # here without releasing left the seal role dead for its whole TTL,
            # so the drain loop above exited with groups still queued and
            # nobody able to take them.
            try:
                self._recover_seal(lease)
            finally:
                lease.release()

            return end

        rel_path = self._layout.seal_path(start, end, uuid.uuid4().hex[:8])
        # I2: the range and its path are fixed BEFORE the file exists, so a
        # retry recomputes nothing and overwrites in place rather than
        # stranding the first attempt under a different name.
        self._buffer.claim_seal(start, end, rel_path)

        try:
            # Renewed either side of the expensive half, and a lost lease is
            # fatal rather than something to push through. Another owner that
            # takes this role replays the SAME range to the SAME path (I2), so
            # continuing would mean two processes writing one file: whichever
            # finished second would truncate the other, and a `finish_seal`
            # from the loser would drop the buffer rows backing a file nobody
            # had completely written.
            #
            # The write itself cannot be checkpointed — it is one blocking call
            # — so it gets a full TTL and no more. A single group is one
            # `target_seal_size` file; a write that outlasts 30 s means the
            # machine is in trouble, and stopping is the right answer then too.
            if not lease.renew():
                msg = "lost the claim on this seal range before writing"
                raise RuntimeError(msg)

            self._write_and_commit(start, end, rel_path, lease)
            self._buffer.finish_seal(end, rel_path, discard=self._discard_on_seal())
        finally:
            lease.release()

        return end

    def await_seal(self, timeout: float | None = None) -> bool:
        """Block until the queue is drained and no seal is in flight.

        A caller that wants to observe the table needs this: nothing about
        correctness does — the rows are durable and readable throughout — but
        `seal` promises the cut, not that the file has been written.

        Asks the two tables rather than an Event, so it is also true across
        processes: a queued group and an in-flight `sealing` claim are both
        durable state, and an Event is neither.

        **Helps rather than only waits.** Each round it tries to drain the
        queue itself, which does nothing while another owner holds the lease —
        and everything once that owner dies and its lease lapses. Purely
        watching would hang until the timeout, or forever without one, over
        work no survivor was going to do.

        This is a `WriteHandle` method and not a `LogHandle` one for that
        reason: a reader could only watch, and a watcher that cannot help is the
        case this exists to avoid. It used to branch on `readonly` to decide,
        which is the flag that no longer exists.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if self._buffer.pending_group() is None and (
                self._buffer.pending_seal() is None
            ):
                return True

            self._seal_queued()

            if deadline is not None and time.monotonic() >= deadline:
                return False

            time.sleep(_AWAIT_POLL)

    def _discard_on_seal(self) -> bool:
        """Whether a seal may drop the rows it just wrote to Parquet (§3a).

        The rule is I4 one tier up: never delete the only off-box copy. What
        counts as another copy is a property of the deployment, so this is the
        one question, asked in one place.

        - **A local published table** — nothing is off-box either way, so
          holding buys nothing. Discard.
        - **A remote published table, no `wal_replication`** — the buffer and
          the Parquet share a disk and die together, so holding buys nothing
          and costs SQLite growth. Discard.
        - **A remote published table and `wal_replication`** — the buffer IS
          the off-box copy until the published table has the range. Hold, and
          let `release_below` drop them once publish has pushed it.

        `validate` refuses `wal_replication` without a remote published table,
        so the last case is just the flag — but both halves are read, because
        the flag alone would be a claim about the published table that this
        does not check.

        Read durably on every seal rather than cached: `set_config` and
        `set_published` both change the answer from another process, and §4a's
        rule is that a decision reads the log rather than its own memory.
        """
        return not (self.config.wal_replication and self._published.remote())

    def _write_and_commit(
        self, start: int, end: int, rel_path: str, lease: Claim | None = None
    ) -> None:
        """Write the Parquet file, fsync it, then commit it to the table.

        I1 in this order: committing first would publish a manifest entry for a
        file that may not survive the crash.

        `start` as well as `end`, because the buffer's floor is no longer the
        group's floor: with WAL replication on, sealed rows stay until the
        published table has them (§3a), so a read bounded only above would sweep
        every earlier row into this file. `Buffer.rows_between` records what
        that costs.
        """
        # The rows come from the buffer, whose schema is read per call — so
        # they may carry a column this handle's TABLE does not know about yet.
        # A sealer never appends, so nothing else in its process would ever
        # have noticed the change: it would write a correct file and then hand
        # it to `add_files`, which refuses a file with a column the table
        # lacks ("PyArrow table contains more columns"). The seal fails for
        # ever while the writer keeps appending.
        #
        # Reloaded only when actually behind — a name comparison against the
        # cached schema, not a catalog read per seal.
        if not set(self._buffer.shape().columns) <= set(
            self._table.arrow_schema().names
        ):
            self._table.reload()

        rows = self._buffer.rows_between(start, end)
        order = self._buffer.sort_by()
        if order:
            # §4: the sort order is declared as table metadata AND applied here.
            # Metadata records intent; it does not sort for you.
            rows = rows.sort_by([(c, "ascending") for c in order])

        dest = self._layout.absolute(rel_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        write_parquet(rows, dest, self.config.compression)

        # Checked immediately before the commit, because this is the moment a
        # lapsed owner does real damage. Its file now has a name of its own, so
        # `register` no longer collides with the owner that took over — it
        # succeeds, and the range lands in the table twice. The shared name
        # used to refuse that by accident; nothing does now except this.
        #
        # A narrow window remains between the check and the commit. It cannot
        # be closed from here — Iceberg's CAS knows nothing of our lease — and
        # it is milliseconds against a 30 s TTL.
        if lease is not None and not lease.renew():
            # The file exists and will never be registered, so it has to stay
            # nameable: queue it before raising, or it is a file on disk that
            # this database cannot find.
            self._buffer.enqueue_deletions(
                [rel_path], int(datetime.now(UTC).timestamp())
            )
            msg = "lost the claim on this seal range before committing"
            raise RuntimeError(msg)

        # `end` passed so the commit can decline if the range is already in
        # the table. The lease check above is the fence; this is what makes a
        # failure of that fence harmless rather than a duplicate.
        if not self._table.register(
            [str(dest)],
            end=end,
            # The published table too. An empty staging table covers nothing,
            # so after a stalled writer's range has been sealed, published and
            # evicted, the staging check alone would let it re-register a file
            # the log has already moved past.
            published_through=self._maintenance.published_through(),
        ):
            # Declined: another owner already sealed this range, so this file
            # is redundant and will never be referenced. Queue it, or it joins
            # the one category this design has no way to find — a file on disk
            # that no SQLite row names. The lease-fence path above does the
            # same; both fences have to leave the disk in a describable state.
            self._buffer.enqueue_deletions(
                [rel_path], int(datetime.now(UTC).timestamp())
            )

    def seal_due(self) -> int | None:
        """Seal everything the policy says is ready. Returns the last end, or
        None.

        The maintainer's frequent call, and the counterpart to `maintain`: both
        are plain methods the caller runs on its own schedule, because the
        library has no business owning a thread or an interval. This one is
        cheap when there is nothing to do — an indexed read of one row — so it
        can be run often; `maintain` reads table metadata and wants to be run
        rarely. That difference is the only reason they are two methods.

        "Due" means cut. `target_seal_size` is the only trigger and it needs
        nothing here: the cut was recorded by the append that crossed it, and
        this writes the file. There is no age branch, so a quiet stream is
        simply one whose rows stay in the buffer — durable, readable, and
        replicated by §3a — until enough of them arrive to fill a file.

        A group whose lease is held elsewhere is left alone, not waited for.
        """

        end = None
        # Peeked before `_seal_queued` takes the lock, because almost every
        # call finds nothing and taking the write lock to discover that would
        # serialise a maintainer against appends on a timer.
        while self._buffer.pending_group() is not None:
            sealed = self._seal_queued()
            if sealed is None:
                # Another owner holds the lease and is draining the same queue.
                break

            end = sealed

        return end

    # -- recovery ----------------------------------------------------------

    def recover(self) -> None:
        """Finish whatever a crash interrupted (§4, §11).

        Idempotent in every direction, which is why it can simply run at open
        rather than being an operator's decision.
        """
        # Each half is replayed only by whoever is entitled to it. Another
        # process may be part way through the very operation this would redo.
        maintain = self._lease(MAINTAIN_ROLE)
        if maintain.acquire():
            try:
                self._recover_compaction()
            finally:
                maintain.release()

        seal = self._lease(SEAL_ROLE)
        if seal.acquire():
            try:
                self._recover_seal(seal)
            finally:
                seal.release()

    def _recover_seal(self, lease: Claim | None = None) -> None:
        """If the commit landed, only the buffer delete is outstanding; if it
        did not, the whole file is rewritten to the same path.

        Reloads first, because "did the commit land" is a question about the
        CURRENT table and this handle may predate it. A writer that has only
        appended for hours holds a snapshot from when it opened; asked with
        that, it would decide a committed file is missing and rewrite the live
        one underneath the readers scanning it.
        """
        pending = self._buffer.pending_seal()
        if pending is None:
            return

        self._table.reload()

        start, end, rel_path = pending
        if str(self._layout.absolute(rel_path)) in self._table.file_paths():
            # `discard` here too, and it was missing. This is the crash window
            # §3a exists for — committed, not yet retired — so defaulting to a
            # delete removed the only off-box copy of a range the published
            # table does not hold, with replication on. Measured: five buffered
            # rows before the crash, zero after the recovery.
            self._buffer.finish_seal(end, rel_path, discard=self._discard_on_seal())

            return

        # Not committed, so it has to be written — but NOT to the name the last
        # attempt chose. That attempt may still be running: a writer stalled
        # past its lease is indistinguishable from one that died, and
        # `pq.write_table` truncates on open, so sharing the name blends two
        # writers into one file and commits it.
        #
        # The abandoned name goes on the deletion queue BEFORE the claim is
        # replaced. That ordering is the whole of it: a unique name with no
        # queue entry is a file this database can no longer name, which is the
        # one thing §12 refuses — worse than the collision it fixes.
        self._buffer.enqueue_deletions([rel_path], int(datetime.now(UTC).timestamp()))
        retry = self._layout.seal_path(start, end, uuid.uuid4().hex[:8])
        self._buffer.claim_seal(start, end, retry)
        self._write_and_commit(start, end, retry, lease)
        self._buffer.finish_seal(end, retry, discard=self._discard_on_seal())

    def _recover_compaction(self) -> None:
        """Resolve a compaction interrupted before its commit (§11).

        Unlike a seal, an interrupted compaction is not redone. Its inputs are
        still live — the transaction that would have superseded them never
        committed — so the table is already correct and the next `maintain()`
        will pick the same run up again. All that is owed is the half-written
        output, and `compacting` names it, so removing it costs one unlink
        rather than a directory scan — or, for a published rewrite, one DELETE
        rather than a paginated LIST over object storage.
        """
        pending = self._buffer.pending_compaction()
        if pending is None:
            return

        # No reload. This used to decide from `file_paths()` and unlink, so it
        # needed the freshest possible view — and still raced. It queues now,
        # and the decision that matters is `drain`'s, which reloads at the
        # moment it removes.

        # EVERY claim, not the first. A published rewrite writes one file per
        # re-cut segment and claims each before it exists (I2), so taking one
        # row and then clearing the table left the rest as objects in a bucket
        # that nothing references and only a paginated LIST could find — the
        # one thing this design refuses to need.
        # QUEUED, not unlinked. The check and the removal are two moments, and
        # the owner this is recovering from may not be dead — a maintainer
        # stalled past its lease can wake between them and commit the very file
        # about to be deleted, taking the whole range with it, because its
        # sources were queued before that commit and drain away behind it.
        #
        # The seal path never had this exposure: an abandoned seal goes through
        # `pending_delete`, whose drain refuses anything the table references.
        # Recovery uses the same route.
        #
        # That veto is read ONCE per drain pass, not per removal — this said
        # otherwise, and the difference is the whole of a defect found on the
        # published side: a commit landing after the veto was read leaves drain
        # deleting objects the manifest now names. What actually makes it safe
        # is that the committer renews its claim immediately before committing,
        # so a claim recovery has taken cannot commit at all.
        claimed = self._buffer.pending_outputs()
        self._maintenance.enqueue_recovered(key for _, _, key in claimed)

        # The scratch database an interrupted rewrite left behind. It is
        # rebuilt from the published table next time, so nothing in it is owed.
        self._layout.rewrite_db.unlink(missing_ok=True)
        # Only the rows just read. A rewrite whose lease lapsed mid-upload can
        # be claiming its next segment while this runs, and clearing the table
        # wholesale takes that claim with it — leaving an object in the bucket
        # named by nothing, which is the state claims exist to prevent.
        for _, _, key in claimed:
            self._buffer.clear_compaction(key)

    # -- maintenance -------------------------------------------------------

    def publish(self, *, push_unsettled: bool = False) -> None:
        """Push to the published table: upload, register, record the watermark
        (§5).

        `push_unsettled=True` pushes EVERYTHING unpublished, including the
        trailing run `stable_prefix` normally holds back for compaction. It is
        not scoped to any subset — `_push` walks a prefix, because the watermark
        it records has to stay contiguous for eviction to trust it (I4), so
        there is no way to push the top of the list without the rest.

        `ingest` passes it, because a load's rows never enter the buffer and so
        have no second copy to wait behind. An operator passes it to close a
        load's tail on a log that has gone quiet, where the run never settles.
        The cost is undersized objects in the published table that compaction
        will not merge afterwards; `ingest` runs a compaction first to keep that
        to what a run genuinely cannot fill.

        Published-facing work only. Lazy, restartable, and arbitrarily far
        behind — no read depends on it. Every log has a published table (#98),
        local by default, so there is always somewhere to push.

        **Only files at or above the compaction threshold are pushed**, and that
        one rule does three jobs. The published table never receives an
        undersized file, so nothing ever has to merge one back out of object
        storage — which would mean paying egress to fix a sizing decision made
        locally. Such a file is also never a compaction input (`compact` only
        builds runs from files BELOW the threshold), so nothing merges across
        what the published table already holds, and no push can duplicate or
        strand a range. And the undersized frontier stays in staging, bounded by
        roughly `compact_min_files` files, until compaction grows it past the
        line.

        There is therefore at most one undersized region in the system and it is
        always the staging one — **as long as nobody passes `push_unsettled`**.
        That flag exists because a bulk load's rows never enter the buffer, so
        the trailing run holding its short last file has no second copy to wait
        behind, and the rule above would strand it on local disk for as long as
        the stream stayed quiet. A load therefore can leave undersized objects
        in the published table — up to `compact_min_files - 1` seals forming a
        run `_merge` will not rewrite, plus the load's own tail — and compaction
        will not merge them afterwards, because it refuses to touch anything the
        published table holds. `rewrite_published` re-cuts them. `ingest`
        compacts before pushing to keep the count to what a run genuinely cannot
        fill.

        **Then the published table's housekeeping**: expire its snapshots older
        than `published_snapshot_retention`, delete the objects that frees once
        their grace has passed, and sweep stranded metadata (#113). Published-
        facing, so here and not in `maintain`, which must keep a partitioned
        machine's local storage in order without the network (§11).

        DEVIATES from §5, which also lists local eviction (step 5). That is
        local storage work and belongs to `maintain`, which runs whether or not
        a publish pass has. Publish's remaining obligation to eviction is the
        registration watermark it records in `meta`, which is what lets
        `maintain` enforce I4.
        """
        # Re-read where the published table IS before pushing to it.
        # `set_published` is a durable change made by whichever process runs
        # it, and every other process cached the old value when it opened — so
        # a maintainer started before a re-point would go on pushing to the
        # retired published table and, worse, reconcile the watermark from ITS
        # span. Eviction trusts that watermark and deletes staging files the new
        # published table has never been sent: the rows survive only in the
        # bucket the re-point was retiring.
        #
        # One keyed read per publish pass, against a change that is rare and
        # durable: `Published` reads the location from `meta` on every access.
        lease = self._lease(MAINTAIN_ROLE)
        if not lease.acquire():
            msg = "another owner holds a claim over this range"
            raise RuntimeError(msg)

        try:
            # UNDER the lease, not before it. Read first, the location can be
            # re-pointed between the read and the acquire — `set_published`
            # takes the same lease, so it is free until this line — and the push
            # then runs against the old published table while the log durably
            # points at the new one. `_push` would reconcile the old published
            # table's span into the watermark with no network call at all, and
            # eviction believes a watermark whatever earned it.
            # PINNED here, and every fence downstream compares against this
            # string rather than re-reading the object. `Published` is shared by
            # the log, the reader and the maintainer precisely so a re-point
            # reaches all three — which means a `set_published` on another
            # thread moves the value a fence was going to compare AGAINST, and
            # both sides of the comparison change together. The fence passes,
            # and the watermark this push earned is recorded against a published
            # table that never received it.
            self._push(lease, self._published.uri, push_unsettled=push_unsettled)
            # The published table's own housekeeping, after the push and under
            # the same lease: expire its snapshots, delete what has come due,
            # sweep stranded metadata (#113). Here rather than in `maintain`,
            # which must keep working with no network (§11).
            self._maintenance.tidy_published(lease)
        finally:
            lease.release()

    def _push(
        self, lease: Claim, pinned: str | None, *, push_unsettled: bool = False
    ) -> None:
        """Upload and register everything above the published table's span.

        **`push_unsettled` pushes the trailing run too**, which `publish`
        otherwise holds back because compaction may still merge it. Holding it
        back is right when the rows have a second copy and wrong when they do
        not: a bulk load's last file is short unless the load divides evenly,
        and `ingest` rows never enter the buffer, so until that file is in the
        published table the only copy is local disk. Measured on the deployment
        that found this: 3,000 rows loaded into 11 files, one ordinary
        `publish()`, `published_through()` 2,830 — the last 170 rows local, with
        `coverage()` reporting no gap. On a stream that then went quiet the run
        never settled, and 113,399 rows sat on one disk.

        Safe in the direction it moves. Compaction refuses to merge anything a
        published table already holds, so a file pushed early is simply never
        merged — the cost is a small object the published table keeps until
        `rewrite_published` re-cuts it, not a duplicate range. The deadlock the
        shared `runs` exclusion guards against is the opposite direction:
        holding back a file compaction will never touch.

        Everything compaction has finished with, which `stable_prefix` decides
        from compaction's own rule rather than from a size of its own. A file
        pushed and then merged in staging would leave the published table
        holding rows that have been rewritten underneath it, so the two must
        agree on which files are still in play, and the only way to guarantee
        that is to ask the same function.

        The published table may still gain a small file: one stranded between
        larger neighbours can never be merged, so holding it back would block
        the watermark forever rather than improve anything. That is a cosmetic
        cost with a deliberate cause, and `rewrite_published` is the tool for
        it.
        """
        # Read under the claim, the same as everything else that decides what
        # this pass does. The grouping `stable_prefix` computes has to match
        # the one compaction computes — `runs` is shared so they cannot
        # disagree — and in the shipped topology they are separate processes,
        # so agreement means both reading the policy the log records rather
        # than the one each happened to open with.

        published = self._published.require()
        self._table.reload()
        # The PUBLISHED table reloaded too, and for a stronger reason than the
        # staging table. A pyiceberg handle is a frozen snapshot view, and
        # `Published` caches it for the life of the process — so a second
        # maintainer that published, released the lease and took it back reads
        # the span as it was before the OTHER maintainer's register. Every other
        # reader of this span already reloads; this is the one place that turns
        # it into a durable watermark, and a stale answer here retires the
        # frontier against a published table that has since grown past it.
        published.reload()

        # The published table's tier row, if nothing has computed one yet: a
        # log given a published table at `new`, one written before the manifest
        # existed, one re-pointed where the published table could not be read.
        # Until then every query reads the published table. A push adds only
        # copies of staging rows, so it never changes this row itself —
        # eviction does.
        if not self._tiers.has():
            self._record_published_row(published)

        # The published span's end: everything below it is in the bucket.
        covered = published.span()
        floor = 0 if covered is None else covered[1]

        # RELEASED HERE, at the top of the pass, from the published table's own
        # span.
        # These are rows a seal held because nothing off-box had them yet
        # (`_discard_on_seal`); the published table now does, so they can go.
        #
        # Not at the tail of this method, and that placement is the whole of
        # its crash-safety. `_push` returns early in three places before its
        # watermark — nothing to upload, a declined register, a re-point — so a
        # crash between `register` and a trailing release leaves the rows held,
        # and the NEXT pass finds nothing above `floor` to push and returns
        # before reaching the release. On a log that has gone quiet, they are
        # held indefinitely. Driven from the frontier instead, it is idempotent
        # and every pass retries it for free.
        if not self._discard_on_seal() and floor:
            # The published table's own span, and that is sound only because
            # `_refuse_published_ahead` has already established the published
            # table is OURS. Two narrower bounds were tried here and neither
            # works: the watermark is raised to `floor` by `confirmed` below on
            # every pass, and this log's own `extent` rows are written for the
            # published table's whole manifest by the backfill within one
            # publish pass. Both are downstream of a contamination that has to
            # be stopped at the point the log is pointed.
            self._buffer.release_below(floor)

        # The watermark reconciled against the published table itself. It is a
        # cache of what the published table holds — kept for the push floor and
        # for display, and no longer for anything that authorises a deletion —
        # so a commit that landed while the `meta` write after it did not would
        # leave it behind for ever: the next pass computes `floor` from the
        # published table, finds nothing left to push, and never revisits it.
        #
        # Compared and written in one transaction, not read and then written.
        # Reconciling against a published table the log has been pointed away
        # from is the loss this path is here to prevent, and a guard that reads
        # first only reports where the published table was.
        # Stored as the last offset held, so one below the span's end.
        confirmed = max(self._maintenance.published_through(), floor - 1)
        self._buffer.set_meta_if(
            _PUBLISHED_KEY, pinned, {Maintenance.PUBLISHED_THROUGH_KEY: str(confirmed)}
        )

        memory = self._maintenance.memory()

        # BACKFILL, and it is what makes I4-per-segment recoverable. The row
        # naming a file's published copy is written after the register, so a
        # crash between the two leaves the published table holding a range that
        # nothing in `buffer.db` records — and compaction, which now decides
        # from those rows, would merge it into a file spanning the published
        # table's span. The next push would register a partial overlap, which
        # `register` admits.
        #
        # The published table's own manifest is the truth, so recover from it
        # rather than promising anything beforehand. Reading it costs nothing
        # extra: `span()` above already walked it.
        # Bounded by the staging window, not by the published table. Every
        # decision the rows feed — what compaction may merge, what eviction may
        # drop — is about files the staging table still holds, so a published
        # file entirely below them changes no answer. Unbounded, this read and
        # this loop grew with the published table and ran on every single
        # publish pass.
        # Bound before the first use. The backfill below sizes an unmeasured
        # published file by the compact target, and `stable_prefix` groups by
        # the same policy — two reads, and nothing that makes them agree.
        config = self.config
        local = self._table.data_files()
        base = min((f.start for f in local), default=0)

        # RECONCILIATION, matched by path in the published table's manifest
        # rather than by offset range. Range matching reads plausibly and is
        # wrong: a rewrite's intents name new objects over a range the stale
        # files being replaced still cover, so a crashed rewrite's dead intents
        # would be confirmed rather than dropped.
        #
        # Bounds differ between the two reads and must. The manifest walk is
        # bounded by the staging window, or it grows with the published table
        # and runs on every publish pass. The intent read is unbounded, because
        # an intent below the window has to be reachable to be dropped.
        held_paths = {f.path: f for f in published.data_files() if f.end > base}
        recorded = {
            path for path, _, _, _ in self._buffer.published_records(pinned or "", base)
        }
        intended = {
            path: (start, end, size)
            for path, start, end, size in self._buffer.intents(pinned or "")
        }

        # ONE rule per path, decided by which of the two tables holds it. An
        # earlier shape ran the rules as separate loops over a `recorded` set
        # snapshotted before either — so rule 2 re-fired for every path rule 1
        # had just confirmed, and `record_file`'s conflict clause overwrote the
        # intent's measured bytes with the default. That made the `bytes`
        # column dead in every reachable path: the rewrite tail this exists to
        # size correctly was durably recorded as full, and nothing re-measures
        # a published file.
        for path, landed in held_paths.items():
            recovered = intended.get(path)
            if recovered is not None:
                # 1. The register landed. Confirm it with the bytes the intent
                #    carried — the only measurement that survives a crash
                #    between a rewrite's commit and its confirm.
                start, end, size = recovered
                self._buffer.record_file(path, start, end, size)
            elif path not in recorded:
                # 2. In the manifest with no row of either kind: the backfill
                #    this rule grew out of.
                self._buffer.record_file(
                    path,
                    landed.start,
                    landed.end,
                    memory.get(path, config.compact_size),
                )

        for path in intended:
            if path not in held_paths:
                # 3. Nothing in the manifest holds that path, so the register
                #    never landed and the intent is dead. Below the staging
                #    window this also drops intents whose register DID land —
                #    the manifest walk is bounded — and their sizes are then
                #    never measured. No reader below the window asks.
                self._buffer.forget_intent(path)

        pending = [f for f in self._table.data_files() if f.end > floor]
        # `stable_prefix` holds a file back when compaction might still merge
        # it, and compaction refuses to merge anything some published table
        # already holds — so the two need the SAME exclusion or they deadlock.
        # They share `runs` for exactly this reason, and giving compaction a
        # second input this could not see was enough to break it: after a
        # re-point to a fresh prefix the floor is 0, so files an old published
        # table covers are back in `pending`, group into a mergeable run under a
        # raised target, and are held back for ever against a merge that will
        # never happen. Nothing is pushed, the watermark never moves, eviction
        # pins on it, and no error surfaces anywhere.
        #
        # A file no merge can touch is settled by definition. Only the part
        # above that line is still compaction's business.
        # Intents included, so this exclusion is literally compaction's. The
        # two share `runs` so they cannot disagree about what is in play, and
        # a second input one of them could not see is what deadlocked them once
        # already.
        frozen = self._maintenance.published_prefix(pending, None, include_intents=True)
        head = [f for f in pending if f.start >= frozen]
        settled = (len(pending) - len(head)) + stable_prefix(
            head,
            config.compact_size,
            config.compact_min_files,
            memory,
            config.compact_rows,
        )
        if push_unsettled:
            # EVERYTHING unpublished, and that is forced rather than chosen.
            # `pending[:settled]` is a PREFIX because the watermark recorded
            # below has to stay contiguous — eviction trusts it for I4 — so
            # there is no way to push a load's tail while leaving an undersized
            # SEAL beneath it unpushed. Attempted and measured: extending only
            # through bulk-loaded files never advances past a seal sitting at
            # index 0, and `published_through()` stays 0 with the load
            # unpublished. Hence the blunt name.
            #
            # So this ships those files. Acceptable because of WHEN a load
            # happens: `ingest` claims the whole log and is a backfill-time
            # operation, so live capture is typically stopped and the trailing
            # run is the load's own. And the alternative is worse by a wide
            # margin — a load's rows never enter the buffer, so not pushing them
            # leaves them on one disk. Compaction will not merge what the
            # published table holds, so any small objects persist until
            # `rewrite_published` re-cuts them.
            settled = len(pending)

        uploaded: list[tuple[DataFile, str]] = []
        for data_file in pending[:settled]:
            checkpoint(lease.renew)
            rel_path = self._layout.relative(data_file.path)
            # The intent BEFORE the upload, which is the seal's I2 argument
            # applied to the published table: the name goes down before the
            # object exists. What it guards is the register that follows — that
            # can land while the row recording it does not, and compaction
            # decides what it may merge from those rows, so without this a
            # compaction-target change before the next publish merges across a
            # range the published table holds and every later push is refused
            # for ever.
            #
            # For EVERY file, not only measured ones: the unmeasured are
            # exactly the ones the confirm below used to skip.
            self._buffer.intend_file(
                published.uri(rel_path),
                data_file.start,
                data_file.end,
                memory.get(data_file.path, config.compact_size),
            )
            published.put(self._layout.absolute(rel_path), rel_path)
            uploaded.append((data_file, rel_path))

        if not uploaded:
            return

        # ONE commit for everything uploaded, because the commit is what costs.
        # Measured against S3: 648 ms to upload a file and 4.1 s to register
        # it, and registering does not get cheaper for holding one file instead
        # of twenty — it reads a footer, writes a manifest, a manifest list and
        # a fresh metadata.json whatever the count. Per file, that made `publish`
        # take 83 s over sixteen files while the sealer sharing its thread
        # waited, and the buffer grew for the whole of it.
        checkpoint(lease.renew)
        last = uploaded[-1][0]
        if (self._buffer.get_meta(_PUBLISHED_KEY) or None) != pinned:
            # The upload already spent longer than a lease TTL against S3 more
            # than once, which is how the log gets re-pointed underneath a push
            # — and registering into the published table it was pointed AWAY
            # from writes the log's rows somewhere nothing will look for them
            # again.
            raise _repointed_mid_push()

        if not published.register(
            [published.uri(rel_path) for _, rel_path in uploaded],
            end=last.end,
            # The low end too, so the published table can refuse a range that
            # starts inside what it already holds. Everything upstream is arranged so
            # that cannot happen; this is the check that holds regardless of
            # whether the arrangement has a gap.
            start=uploaded[0][0].start,
        ):
            return

        for data_file, rel_path in uploaded:
            # The published table's copy holds what the staging one did, and
            # this is the only moment both names are known. Nothing could
            # re-derive it afterwards: the staging entry goes when the staging
            # file is unlinked, and a Parquet footer records what the rows
            # compressed from, not what the appender counted them as.
            #
            # For every file, with the same default the intent used. The guard
            # that used to skip unmeasured ones was a hole: in a takeover the
            # confirm is the only thing that recreates rows a rival's
            # reconciliation dropped, so skipping any file reopens the window
            # for exactly the files the intent was added to protect.
            #
            # Same span, second location.
            self._buffer.record_file(
                published.uri(rel_path),
                data_file.start,
                data_file.end,
                memory.get(data_file.path, config.compact_size),
            )

        # After the register, never before: the watermark is a promise that the
        # published table HAS the range, and I4 lets `maintain` delete the
        # staging copy on the strength of it.
        #
        # And only if it is still a promise about the SAME published table.
        # This push can outlive its lease — a register alone measured 4.1 s and
        # retries compound it — and a re-point that takes the lease meanwhile
        # leaves this about to record a watermark earned by a bucket the log has
        # left.
        #
        # Re-read rather than trusted, and in the same transaction as the
        # write: nothing lowers a watermark afterwards, so recording one earned
        # by a bucket the log has left is not a mistake anything corrects.
        if not self._buffer.set_meta_if(
            # The stored watermark is the last offset held.
            _PUBLISHED_KEY,
            pinned,
            {Maintenance.PUBLISHED_THROUGH_KEY: str(last.end - 1)},
        ):
            raise _repointed_mid_push()

        # Again, now that this push has landed and its `record_file` rows
        # exist. The release at the top of the pass ran before them, so on its
        # own it frees rows one whole publish pass late — safe, since holding is
        # the safe direction, but a busy log would carry a pass's worth it no
        # longer needs.
        #
        # This one is the promptness; that one is the correctness. A crash
        # between the register above and this line leaves rows held, and the
        # next pass frees them from the same rows without needing anything to
        # have been uploaded. Neither placement alone is both.
        #
        if not self._discard_on_seal():
            self._buffer.release_below(last.end)

    def _store_staging_statistics(self) -> None:
        """Store the rollup of the staging table's current version, for everyone.

        Run after each staging commit this process makes. The rollup is 2–3 ms
        (measured at 1–64 files), paid once here rather than once per process
        that queries the new version. Stamped with the version and written
        forward only — see `Buffer.store_staging_statistics`.
        """
        location, extent = self._table.snapshot()
        statistics = self._table.statistics_at(location)
        if statistics is None:
            return

        offsets = (0, 0) if extent is None else extent
        self._buffer.store_staging_statistics(
            location, offsets, encode_tier(self._buffer.shape().table, statistics)
        )

    def _record_published_row(self, published: LogTable) -> None:
        """The published table's tier row, exactly, from its manifests.

        Its files reaching below the staging table — the range the published leg
        of a read covers. A file straddling the staging table's first offset is
        included whole, so its bounds overstate; that is the safe direction.

        Narrows, so only under the whole-log maintenance claim: eviction, the
        one thing that widens this row, cannot run beside it, and nothing
        else moves the staging table's floor down.
        """
        published.reload()
        schema, files = published.live_files()
        self._table.reload()
        span = self._table.span()
        below = [f for f in files if span is None or file_span(schema, f)[0] < span[0]]
        self._tiers.replace(self._buffer.shape().table, rollup(None, schema, below))

    def _backfill_manifest(self) -> None:
        """Compute the published table's tier row if the manifest lacks it,
        best effort.

        At open, so a log written before the manifest existed stops reading the
        published table for every query. Under the whole-log claim, which
        eviction cannot hold beside. If the claim is held elsewhere, the
        holder's next `publish` computes the row; until then the published
        table is simply read.

        It reads the published table's manifests, so a remote one needs the
        network. A published table this process has no catalog entry for is
        not opened at all (`table()` answers None offline), so a log that has
        never published touches no network here.
        """
        if self._tiers.has():
            return

        lease = self._lease(MAINTAIN_ROLE)
        if not lease.acquire():
            return

        try:
            published = self._published.table()
            if published is not None:
                self._record_published_row(published)
        except Exception:  # noqa: BLE001
            # Unreachable is the ordinary case this tolerates — a box whose
            # credentials arrive after the log is opened. A row not written is
            # a tier that is read, which is correct and only slower.
            pass
        finally:
            lease.release()

    def maintain(self) -> None:
        """Reclaim local storage: compact, evict, expire (§6, §8, §12).

        The one call most deployments want, and it takes the lease once for
        all three. `compact`, `evict` and `expire` are callable on their own
        for the case this cannot express — schedules that differ because the
        costs do, now that conversion reads and rewrites files while the other
        two are metadata commits.

        **Eviction is bounded by I4 on every log**: a file that publish has not
        yet registered in the published table is never evicted, however old. So
        on a partitioned-off machine, this compacts and expires but leaves the
        window growing — §11's "local eviction stalls" — and on a log whose
        published table is a local directory, `staging_retention` takes effect
        as `publish` pushes to it. Eviction never deletes data (#98).

        Whether a stalled or partial pass should be reported rather than silent
        is open — §11 treats stalled eviction as an operational condition, and
        returning None says nothing about it.
        """

        # The lease is the exclusion, and it is the only one this needs. There
        # is no lock around the pass any more: a compaction reads every file it
        # merges and writes a new one, and holding a lock that reads also take
        # made one read wait 21.5 s. `LogTable` guards its handle and caches
        # for the moment each is touched, and `_commit` retries a branch that
        # moved underneath it, which is what cross-process safety rests on
        # anyway — a lock could never have provided it.
        #
        # Taken after the refusals above, so a rejected call does not leave
        # a lease behind for its TTL and lock out the process that could
        # have done the work.
        # Each pass claims what it works on; see `_pass`.
        self._maintenance.run(heartbeat=None)

        # Sealing IS maintenance, so a caller running only this in a loop has
        # to get it; `seal_due` is exposed separately only because it is cheap
        # enough to run far more often than the rest of this.
        #
        # AFTER the pass, not before, and the comment here used to say the
        # opposite of the line it sat on. The pass works on files that are
        # already sealed, so a group cut during this call becomes a candidate
        # on the next one — a cycle of latency, against a pass that would
        # otherwise compact a file it had just written.
        self.seal_due()

        # LAST, and only when asked. The pass above is what frees pages —
        # eviction and the release of rows the published table holds both
        # delete — so reclaiming before it would measure a free list that is
        # about to grow. Off unless
        # `vacuum_free_ratio` is set, because this is the one part of a
        # maintenance pass that blocks appends; see `reclaim_buffer`.
        ratio = self.config.vacuum_free_ratio
        if ratio is not None:
            self.reclaim_buffer(ratio)

    def reclaim_buffer(self, min_free_ratio: float = 0.0) -> int:
        """Return `buffer.db`'s dead space to the OS. Bytes reclaimed, or 0.

        SQLite puts pages freed by a DELETE on a free list and never shrinks the
        file, so a buffer that seals and publishes for months keeps every page
        it has ever needed. Locally that is invisible — the free list is reused
        — and it is FAILOVER that pays, because litestream replicates the FILE:
        every `restore` downloads and applies the dead space. Measured on a
        1-day-old capture, 457 MB holding 20,658 live rows with 92% of its pages
        free, restoring in 12.5 s against 0.8 s vacuumed.

        **Manual, because the cost lands on the write path.** `VACUUM` takes an
        exclusive lock and rebuilds the file, so appends stall for as long as
        the LIVE data takes to copy — 0.3 s at 35 MB. Only the deployment knows
        whether its arrival rate can absorb that, and a writer with no off-box
        readers can decline for ever and lose nothing but disk. Set
        `vacuum_free_ratio` to have `maintain` do it on the ordinary pass.

        Call it when appends can tolerate the pause: after a batch, on a quiet
        period, or from the same schedule that runs `maintain`. Cheap to call
        and do nothing — the check is three PRAGMAs — so it is safe in a loop.

        `litelink_offset` is untouched: values keep their gaps and the
        AUTOINCREMENT counter survives, including on a buffer the published
        table has fully drained (I9). See `Buffer.reclaim_free_pages`.
        """
        return self._buffer.reclaim_free_pages(min_free_ratio)

    def compact(self, heartbeat: Callable[[], bool] | None = None) -> None:
        """Convert sealed files into `target_compact_size` ones (§6).

        The heavy half of `maintain`, and the reason the three passes are
        callable separately: it reads and rewrites whole files, while eviction
        and expiry are metadata commits that finish in milliseconds. A
        deployment that wants them on different schedules — convert hourly,
        expire every minute — can have that, and one that does not should call
        `maintain` and get all three.
        """
        self._pass(self._maintenance.compact, heartbeat)

    def evict(self, heartbeat: Callable[[], bool] | None = None) -> None:
        """Drop files past `staging_retention` from the staging table (§8).

        Never past what the published table holds (I4), so a publish that is
        behind delays this rather than losing data.
        """
        self._pass(lambda _: self._maintenance.evict(), heartbeat)

    def expire(self, heartbeat: Callable[[], bool] | None = None) -> None:
        """Expire staging snapshots past `staging_snapshot_retention`, delete
        what has come due, and sweep stranded metadata (§6, §8). The published
        table's are `publish`'s (#113)."""
        self._pass(lambda _: self._maintenance.expire(), heartbeat)

    def _pass(
        self,
        run: Callable[[Callable[[], bool] | None], None],
        heartbeat: Callable[[], bool] | None,
    ) -> None:
        """One maintenance pass under the maintenance lease.

        The same exclusion `maintain` takes, so running the passes separately
        is not a way around it: a second owner is refused whichever entry point
        it came through.
        """
        # No claim here. Each pass claims the range it actually works on —
        # compaction a run, eviction the prefix it removes — so two maintainers
        # exclude each other only where their work overlaps, which is what §4a
        # buys over one lease per role. An entry-point claim would put that
        # back and cover every offset in the log while doing so.
        run(heartbeat)

    def rewrite_published(self) -> None:
        """Merge undersized files already in the published table (§6, ad-hoc).

        An operation, not a policy. Nothing calls it on a schedule and normal
        operation does not need it: `publish` pushes only files compaction has
        finished with, so the published table is well-sized by construction —
        except for what a bulk load pushed with `push_unsettled` to avoid
        stranding rows that have no second copy. It exists for the three things
        that break that on purpose — an explicit `seal()` stranding a small
        file, a change to `target_compact_size`, which applies to the future
        while the published table is immutable history, and that load's
        undersized push.

        Run it when nothing else is maintaining the log: it takes the same
        lease as `maintain` and `publish`, and it rewrites the same files they
        would.
        """
        lease = self._lease(MAINTAIN_ROLE)
        if not lease.acquire():
            msg = "another owner holds a claim over this range"
            raise RuntimeError(msg)

        try:
            self._maintenance.rewrite_published(lease.renew, lease.owner)
        finally:
            lease.release()

    def retire(self) -> None:
        """End this log for good: every row to the published table, nothing
        left local.

        After it, the log takes no rows — from this handle, from a writer that
        opened before it ran, or from anything that `open`s or `restore`s it —
        and each refusal says when it retired and at which offset, so the next
        log can start at the one after. Reads still work: `open(...,
        read_only=True)` reads the published table, and so does any Iceberg
        engine.
        `hydrate` still works too, for a retired log someone replays often.

        Steps, each safe to re-run — a crash leaves the log `retiring`, and
        calling this again finishes it:

        1. give the buffer an end — the offset the next append would have
           taken — and flush the WAL replica. From this commit no append lands
           (the buffer's trigger keys on that end), so nothing can arrive after
           the final push;
        2. seal everything buffered;
        3. `publish(push_unsettled=True)`, which also releases the rows a
           `wal_replication` seal was holding;
        4. evict the whole staging table, and check nothing is left local;
        5. record the retirement on the published table (`litelink.retired`),
           so `restore` refuses whatever a replica says;
        6. narrow the buffer's range to `[end, end)`, empty, and flush the WAL
           replica again. That empty range is what reads as `retired` rather
           than `retiring`, and what lets every read skip the buffer.

        One fact records it, not a separate marker: a buffer with an end is a
        log that takes no more rows. `retired()` derives the state from it, and
        `restore` finds it in the replica.

        **The flush asks the running sidecar; it never starts one.** Two
        litestream processes replicating one database is corruption, so with
        `wal_replication` on and no sidecar answering on its control socket,
        this raises and says to regenerate the config — a config from before
        the socket existed is the usual cause.
        """
        marker = self._buffer.retired()
        if marker is not None and marker.get("state") == "retired":
            return

        # Step 1: the buffer gets an end, which is what refuses every append
        # from here on (its trigger). A no-op when resuming.
        schema = self._buffer.shape().table
        self._buffer.close_buffer(encode_tier(schema, UNKNOWN_TIER))
        self._flush_replica()

        while self.buffered_rows():
            self.seal()
            self.await_seal()

        self.publish(push_unsettled=True)
        self._maintenance.evict(everything=True)

        self._table.reload()
        if self._table.span() is not None or self._buffer.span() is not None:
            msg = (
                f"retire() could not empty {self.root}/{self.name}: local files or "
                "buffered rows remain that the published table does not hold yet. It stays "
                "retiring and takes no rows; call retire() again once publish can "
                "reach the published table"
            )
            raise RuntimeError(msg)

        published = self._published.require()
        published.reload()
        # The property records the last offset held, as `through` does everywhere.
        covered = published.span()
        through = None if covered is None else covered[1] - 1
        claim = self._claim_settings()
        try:
            published.set_properties(
                {RETIRED_PROPERTY: json.dumps({"through": through, "at": _now()})}
            )
        finally:
            claim.release()

        # Last: the buffer's range narrowed to `[end, end)` with a record count
        # of 0 — which is what makes the log read as retired rather than
        # retiring, and lets every read skip the buffer without reading it.
        self._buffer.empty_buffer_range(encode_tier(schema, empty(schema)))
        self._flush_replica()

    def _flush_replica(self) -> None:
        """Ship `buffer.db` to its WAL replica now, when one is being kept."""
        if self.config.wal_replication:
            flush(self._layout)

    def hydrate(self, since: timedelta) -> None:
        """Re-register published files into the staging table (§8).

        Raising `staging_retention` is an operation, not a config change:
        without this, a raised setting applies only to data captured afterwards.

        `since` is measured against when the PUBLISHED table took each file,
        which is the only age still on record — the staging snapshots that once
        dated them went with the eviction, and the library stamps no timestamp
        of its own (§2). So this reads "bring back what was published in the
        last week", not "what was captured then"; for a stream that fell behind,
        those differ.

        Files are copied down and registered under the name they have remotely,
        so hydrating twice writes the same paths rather than accumulating
        copies, and one interrupted halfway is finished by the next run. Only
        ranges strictly below what the staging table already holds are
        considered, which is what keeps files contiguous and non-overlapping
        (§4) — the published table's copy of a range the staging table still has
        would otherwise be added a second time and every row in it read twice.

        A hydrated file is not re-measured: its `extent` row carries the size
        recorded for its published copy, and where none was recorded it counts
        as full, which is what an unknown size means everywhere else — so
        compaction will not merge it, and `publish` will not push it back to
        the published table it just came from. Eviction still applies to it,
        which is the point: this is temporary unless `staging_retention` is
        raised too.

        Needs a remote published table, like `restore` and
        `replication_config`: a local one is on this disk already, so copying
        from it buys nothing.
        """
        if not self._published.remote():
            msg = (
                f"hydrate() needs a remote published table (s3://); this log publishes to "
                f"{self.published}, which is on this disk already — reads below "
                f"the staging table get its rows from there"
            )
            raise ValueError(msg)

        # The maintenance lease, because this writes the staging table and
        # copies files into the data directory — the same two things eviction
        # and compaction do, and for the same reason they must not overlap.
        lease = self._lease(MAINTAIN_ROLE)
        if not lease.acquire():
            msg = "another owner holds a claim over this range"
            raise RuntimeError(msg)

        try:
            self._pull(lease, since)
        finally:
            lease.release()

    def _pull(self, lease: Claim, since: timedelta) -> None:
        """Copy down and register everything published since `since`."""
        published = self._published.require()
        self._table.reload()

        covered = self._table.span()
        # Nothing in staging means nothing to sit below, so everything
        # qualifies.
        floor = covered[0] if covered is not None else None
        cutoff = datetime.now(UTC) - since
        added = published.snapshot_ages()
        held = self._maintenance.memory()

        eligible = [
            data_file
            for data_file in published.data_files()
            if (floor is None or data_file.end <= floor)
            and (stamped := added.get(data_file.path)) is not None
            and stamped.replace(tzinfo=UTC) >= cutoff
        ]

        # DOWNWARD from the staging floor, and only across an unbroken join.
        #
        # Registering upward left a hole no later run could fill. The first
        # file restored becomes the new lowest staging range, so a failure
        # before the next one — a lost lease, a network error, or a `since`
        # window that selected a non-contiguous set because `rewrite_published`
        # re-dated a middle file — leaves the gap ABOVE what was restored. The
        # next run takes its floor from that new lower bound, finds the gap no
        # longer below it, and skips it for ever. `_union` bounds the published
        # leg by the staging floor, so those offsets are then served by neither
        # tier: rows silently missing from every query.
        #
        # Downward, every step keeps the staging range contiguous, so an
        # interruption is just a range that starts higher than intended and the
        # next run continues from there. Stopping at a gap rather than stepping
        # over it is the same rule: what cannot be joined onto cannot be
        # restored without creating one.
        for data_file in sorted(eligible, key=lambda f: f.end, reverse=True):
            if floor is not None and data_file.end != floor:
                break

            checkpoint(lease.renew)
            rel_path = published.key(data_file.path)
            destination = self._layout.absolute(rel_path)
            published.fetch(data_file.path, destination)
            # Asked AGAIN, after the fetch and before the commit. The download
            # is a whole file rather than a stream, so it is the slow leg, and
            # the checkpoint above only says the claim was held before it. Past
            # the TTL, `drain` may lawfully take the whole log and unlink this
            # very name — the queue still holds it, since it is the eviction
            # that motivated the hydrate — and its own per-deletion renewal
            # cannot help, because there `drain` is the legitimate holder and
            # this is the lapsed one. Registering afterwards points the table
            # at a file that is not on disk: every scan over the range raises,
            # and `record_file` stamps it fresh so eviction cannot age it out
            # for a whole `staging_retention`, while a re-run of `hydrate` skips
            # the range because the staging floor now covers it.
            checkpoint(lease.renew)
            # No `end`: that check exists to decline a range the
            # table already covers, and every range here is deliberately below
            # what it covers. The filter above is what prevents an overlap.
            self._table.register([str(destination)])
            # An `extent` row for the staging copy, carrying what the published
            # table's copy holds. Restored data is subject to
            # `staging_retention` like anything else, and eviction dates a file
            # by this record — so a hydrated file without one would sit
            # undateable, treated as newly written, and never leave again.
            # A file the published table never measured counts as FULL,
            # matching what this promises above. Zero was the opposite, and it
            # was dormant only while a watermark kept hydrated files out of
            # every size consumer — removing the watermark is what wakes it: a
            # file recorded at zero bytes is a permanent compaction candidate
            # that merging can never make big enough to stop being one.
            self._buffer.record_file(
                rel_path,
                data_file.start,
                data_file.end,
                held.get(data_file.path, self.config.compact_size),
            )
            floor = data_file.start


def _declared_schema(layout: Layout, from_table: pa.Schema) -> pa.Schema:
    """The Arrow schema as declared, checked against the table's columns.

    Both records must exist and agree. A log's schema is fixed at creation
    (§9), so a log whose two records disagree has been corrupted, or written to
    behind the library's back. Continuing with a guess would serve reads under
    a schema the data does not have.

    One disagreement used to be legitimate: an `add_column` interrupted between
    its Iceberg commits and its SQLite record, which recovery finished. That
    operation is gone, so a log left mid-change by an older release is refused
    with the way to finish it.
    """
    encoded = Buffer.peek_meta(layout.buffer_db, _SCHEMA_KEY)
    if encoded is None:
        msg = (
            f"log at {layout.root}/{layout.name} has no stored Arrow schema; "
            "its buffer database is missing or corrupt"
        )
        raise ValueError(msg)

    if Buffer.peek_meta(layout.buffer_db, _LEGACY_INTENT_KEY):
        msg = (
            f"log at {layout.root}/{layout.name} has an `add_column` that an "
            f"older release started and did not finish. This release has no "
            f"schema changes (logs are immutable, §9), so it cannot finish it: "
            f"open the log once with litelink {LAST_MIGRATING_RELEASE}, which "
            f"completes it, then open it with this one"
        )
        raise ValueError(msg)

    declared = pa.ipc.read_schema(pa.BufferReader(bytes.fromhex(encoded)))
    if declared.names != from_table.names:
        msg = (
            f"stored schema {declared.names} disagrees with the Iceberg table "
            f"{from_table.names} — the log has been modified outside litelink"
        )
        raise ValueError(msg)

    return declared


def validate_row(schema: pa.Schema, row: Row) -> None:
    """Check a row against a schema without appending it (#77).

    Raises exactly what `append(row)` on a log of this schema would raise —
    the same exception and the same message, naming the offending column — and
    returns None for a row it would accept. No log is needed and nothing is
    written, so a caller that only sometimes has a log can hold every row to
    one rule and attach a log later without a change in what is accepted.

    `schema` is the caller's columns, as `new` takes them and `LogHandle.schema`
    returns them. One `new` would refuse is refused here the same way, since
    no row could ever be appended under it.
    """
    _probe(schema).check(row)


@functools.lru_cache(maxsize=64)
def _probe(schema: pa.Schema) -> RowProbe:
    """One probe per schema, kept. Building one — `Shape.of` and a CREATE
    TABLE — measured ~260 us, against ~4 us to check a row on one that exists,
    and a caller validating per message asks the same schema every time."""
    validate(schema, (), LogConfig(), None)

    return RowProbe(schema)


def application_schema(schema: pa.Schema) -> pa.Schema:
    """The caller's columns — the table's schema with `offset` removed."""
    return pa.schema([f for f in schema if f.name != "litelink_offset"])


def table_schema(schema: pa.Schema) -> pa.Schema:
    """The caller's columns with `offset` in front — the table's real schema."""
    return pa.schema([pa.field("litelink_offset", pa.int64(), nullable=False), *schema])


def validate(
    schema: pa.Schema,
    sort_by: Sequence[str],
    config: LogConfig,
    published: str | None,
) -> None:
    """Reject configurations that cannot mean what they say.

    Separate from construction so the rules read as a list rather than as
    guards scattered through a constructor.
    """
    if OFFSET in schema.names:
        msg = f"`{OFFSET}` is owned by the library and must not be in the schema (I11)"
        raise ValueError(msg)

    # Before anything else: a column the library cannot carry end-to-end must
    # fail here, not on the first read after the data is already durable.
    validate_schema(schema)

    # The published table's SHAPE, before any rule that reads its meaning.
    # Callers reach this with the empty string already normalised to None, so a
    # string here is one somebody meant — and a malformed one is not caught
    # anywhere downstream, because every consumer parses it positionally. See
    # `validate_published`.
    if published is not None:
        validate_published(published)

    missing = [c for c in sort_by if c not in schema.names]
    if missing:
        msg = f"sort_by names columns not in the schema: {missing}"
        raise ValueError(msg)

    # A file sorted by a struct, map or list has no order an engine can prune
    # on, and neither Iceberg's sort order nor Arrow's sort accepts one.
    nested = [c for c in sort_by if column_type(schema.field(c).type).nested]
    if nested:
        msg = f"sort_by cannot name a struct, map or list column: {nested}"
        raise ValueError(msg)

    for name in ("staging_snapshot_retention", "published_snapshot_retention"):
        # The same sign slip, one field over. Expiry computes `now - retention`,
        # so a negative one puts the cutoff in the future: every superseded
        # file is unlinked in the pass that supersedes it, and I6's whole
        # promise — the grace must exceed the longest scan — is not merely
        # shortened but inverted. Zero is allowed deliberately; it means "no
        # grace", which tests and demos ask for on purpose.
        retention = getattr(config, name)
        if retention < timedelta(0):
            msg = f"{name} must not be negative: {retention}"
            raise ValueError(msg)

    if config.staging_retention is not None and config.staging_retention < timedelta(0):
        # The same check its twin above has always had, and the reason it
        # matters more here: eviction computes `now - staging_retention`, so a
        # negative one puts the cutoff in the FUTURE and every file in the log
        # is stale, and eviction drops everything the published table holds,
        # from one sign slip.
        msg = f"staging_retention must not be negative: {config.staging_retention}"
        raise ValueError(msg)

    if config.wal_replication and (published is None or not is_remote(published)):
        msg = (
            "wal_replication needs a remote published table (s3://): the WAL replica "
            "exists to get unsealed rows off this machine, and a local "
            "published table is on it"
        )
        raise ValueError(msg)

    if (
        config.vacuum_free_ratio is not None
        and not 0.0 <= config.vacuum_free_ratio <= 1.0
    ):
        msg = (
            f"vacuum_free_ratio is a share of the buffer file, so it must be "
            f"between 0 and 1: {config.vacuum_free_ratio}. Use None to never "
            f"reclaim, which is the default"
        )
        raise ValueError(msg)

    if config.wal_retention is not None and not config.wal_replication:
        msg = (
            "wal_retention needs wal_replication: it is a window written into "
            "the sidecar's config, so with nothing shipping the WAL it is a "
            "setting nothing reads"
        )
        raise ValueError(msg)

    if config.wal_retention is not None and config.wal_retention.total_seconds() <= 0:
        msg = (
            f"wal_retention must be positive: {config.wal_retention}. It is how "
            "far back a restore may go, and at or below zero litestream is "
            "being asked to expire every snapshot as it takes it — which "
            "leaves the replica unable to restore to any point at all. Leave "
            "it None for litestream's own default"
        )
        raise ValueError(msg)

    if config.compact_min_files < 2:
        msg = (
            f"compact_min_files must be at least 2: {config.compact_min_files}. "
            "It is how many files a run needs before compaction will merge it, "
            "and a run always holds at least one — so at one, every run looks "
            "mergeable, nothing is ever settled, and `stable_prefix` returns "
            "zero for ever: publish pushes nothing, the watermark stands still, "
            "eviction pins on it and the staging table grows without bound, while "
            "every pass rewrites every file to no purpose. Merging a run of one "
            "is a no-op rewrite in any case"
        )
        raise ValueError(msg)

    if config.target_seal_rows is not None and config.target_seal_rows < 1:
        msg = (
            f"target_seal_rows must be at least 1: {config.target_seal_rows}. "
            "It is the number of rows a file may hold, and a file has to hold "
            "the row that crossed the limit"
        )
        raise ValueError(msg)

    if config.compact_size < config.target_seal_size:
        msg = (
            f"target_compact_size ({config.compact_size}) must be at least "
            f"target_seal_size ({config.target_seal_size}): compaction converts "
            "sealed files into larger ones, and a smaller target would ask it "
            "to shrink a file it just merged, for ever"
        )
        raise ValueError(msg)

    if (
        config.compact_rows is not None
        and config.target_seal_rows is not None
        and config.compact_rows < config.target_seal_rows
    ):
        msg = (
            f"target_compact_rows ({config.compact_rows}) must be at least "
            f"target_seal_rows ({config.target_seal_rows}), for the same reason"
        )
        raise ValueError(msg)

    if config.staging_rows is not None and config.staging_rows < 0:
        msg = f"staging_rows must not be negative: {config.staging_rows}"
        raise ValueError(msg)

    if config.compression not in _CODECS:
        # Here rather than at the first write, because the first write is a
        # seal — in a maintainer, minutes after the config was accepted, with
        # the rows already acknowledged and the buffer filling behind a seal
        # that now fails every time it is retried. pyarrow raises on the
        # unknown codec and nothing upstream would turn that into a message
        # naming the setting.
        msg = (
            f"compression must be one of {sorted(_CODECS)}, not {config.compression!r}"
        )
        raise ValueError(msg)
