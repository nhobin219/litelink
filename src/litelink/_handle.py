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
import itertools
import json
import logging
import random
import time
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.exceptions import TableAlreadyExistsError

from litelink._buffer import (
    PUBLISHED_THROUGH_KEY,
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
from litelink._fs import stream_parquet, write_parquet
from litelink._layout import Layout, is_remote, validate_published
from litelink._maintenance import (
    CONFIG_KEY,
    Maintenance,
    checkpoint,
    chunks,
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
    ISSUED_PROPERTY,
    RETIRED_PROPERTY,
    SCHEMA_PROPERTY,
    SORT_PROPERTY,
    LogTable,
    forget_published_entry,
    narrow,
    published_issued_through,
    published_retired,
    published_shape,
    published_span,
)
from litelink._tiers import PUBLISHED as PUBLISHED_TIER
from litelink._tiers import STAGING as STAGING_TIER
from litelink._tiers import UNKNOWN as UNKNOWN_TIER
from litelink._tiers import PublishedTier, StoredTiers, empty
from litelink._tiers import encode as encode_tier
from litelink._types import NON_FINITE, column_type, validate_schema

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence
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

_log = logging.getLogger(__name__)

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

# The fence a restore with no WAL replica leaves above what the log is known to
# have issued (#144). `RESTORE_RESERVE` only has to cover replication lag,
# measured from the replica's own sequence. Without a replica the best record
# is the last publish's `litelink.issued_through`, and the dead machine may
# have issued any number of offsets after it — for as long as publishing was
# behind or down. 2^40 is over a trillion: 127 days of unpublished appends at
# 100,000 rows a second. Offsets may have gaps (`start_offset`, a failed load),
# and an int64 holds millions of fences this wide.
PUBLISHED_RESTORE_RESERVE = 1 << 40


# How many staged files one Iceberg commit takes.
#
# A commit costs far more than the write it publishes — 4.1 s against 648 ms,
# measured against S3 — because it reads each footer and writes a manifest, a
# manifest list, a fresh `metadata.json` and a new catalog pointer, none of
# which gets cheaper for holding one file instead of twenty. At the default
# 512 MiB a load is few files and this rarely binds; under a small
# `target_compact_size` a 160M-row load is thousands of files, and committing
# each on its own is hours of commits.
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


@dataclass(frozen=True)
class _Recovery:
    """What a restore recovered and what it skipped, for the caller to report."""

    recovered: int
    resumed_at: int
    # `[start, end)`, half-open like every range litelink reports.
    skipped: tuple[int, int]


def _refuse_other_shape(
    source: str,
    recorded: pa.Schema,
    recorded_sort: tuple[str, ...],
    schema: pa.Schema | None,
    sort_by: Sequence[str] | None,
) -> None:
    """Refuse a caller's `schema` or `sort_by` that differs from the shape
    `source` records exactly. Either may be omitted."""
    problems: list[str] = []
    if schema is not None and not schema.equals(recorded, check_metadata=True):
        problems += _schema_differences(schema, recorded, exact=True)
        problems = problems or ["the field metadata differs"]

    if sort_by is not None and tuple(sort_by) != recorded_sort:
        problems.append(f"sort_by is {tuple(sort_by)!r}, not {recorded_sort!r}")

    if problems:
        msg = f"the shape given does not match the one {source} records: " + "; ".join(
            problems
        )
        raise ValueError(msg)


def _refuse_unlike_iceberg(
    schema: pa.Schema,
    sort_by: tuple[str, ...],
    iceberg: pa.Schema,
    declared_sort: tuple[str, ...],
) -> None:
    """Refuse a caller's shape that disagrees with what an unstamped published
    table's Iceberg metadata does record: its columns, their order, their types
    up to `large_*`, their nullability, and its declared sort order."""
    narrowed = pa.schema([narrow(field) for field in schema])
    problems = _schema_differences(narrowed, iceberg, exact=False)
    if sort_by != declared_sort:
        problems.append(
            f"sort_by is {sort_by!r}, but the table declares {declared_sort!r}"
        )

    if problems:
        msg = "the shape given does not match the published table: " + "; ".join(
            problems
        )
        raise ValueError(msg)


def _schema_differences(
    given: pa.Schema, recorded: pa.Schema, *, exact: bool
) -> list[str]:
    """What differs between two schemas, column by column."""
    problems: list[str] = []
    if given.names != recorded.names:
        missing = [n for n in recorded.names if n not in given.names]
        extra = [n for n in given.names if n not in recorded.names]
        if missing:
            problems.append(f"missing columns {missing}")

        if extra:
            problems.append(f"columns the table lacks {extra}")

        if not missing and not extra:
            problems.append(f"columns in the order {given.names}, not {recorded.names}")

        return problems

    for mine, theirs in zip(given, recorded, strict=True):
        if mine.type != theirs.type:
            problems.append(f"{mine.name!r} is {mine.type}, not {theirs.type}")

        if mine.nullable != theirs.nullable:
            problems.append(
                f"{mine.name!r} is {'nullable' if mine.nullable else 'not nullable'}, "
                f"not {'nullable' if theirs.nullable else 'not nullable'}"
            )

        if exact and mine.metadata != theirs.metadata:
            problems.append(f"{mine.name!r} carries different field metadata")

    return problems


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

    Tiers are not fixed at assembly. That would keep a `scan` off the network
    however far eviction ran, at the price of the caller naming a tier and a
    handle without the published table answering short for any query that
    reached below the local window. Latency follows the predicate rather than
    the handle: the same query reads the published table once eviction
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
        replica no longer carries: without replication, `evict("buffer")` drops
        rows once STAGING holds them, and they reach the published table only
        at the next `publish`. In between they are in neither tier.

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
        straddles the staging range (only a re-cut by an earlier version makes
        one) makes
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
        tick before anything is sealed — and one early call was enough to pin it: a `scan`, an
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

    Threads within that process are fine, and scheduling `advance()` on a
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
        # No handle-wide lock. Each collaborator owns its own safety — the
        # buffer serialises its connection, `LogTable` guards its handle and
        # caches, `Reader` guards its DuckDB connection — and the leases decide
        # who may seal or maintain across processes, which no in-memory lock
        # could. A lock on top of those would be a second answer to a settled
        # question, and not a free one: held across a maintenance pass, it
        # made a read wait 21.5 s.

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
        s3_options: S3Options | None = None,
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
        and it is fixed when the log is created, like the schema: a different
        order is a new log, started where this one ends.

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
        # An empty string and None both mean the local default, which
        # `validate` sees as None.
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
        # A log is pointed at its published table here and nowhere else, so
        # this is the only place that needs the check.
        if published is not None:
            try:
                covered = published_span(layout, published, s3_options or S3Options())
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

        return cls._create(
            layout,
            schema=schema,
            order=order,
            settings=settings,
            published=published,
            s3_options=s3_options,
            first_offset=start_offset,
            # Recorded only when there IS a reserve. Absent means "started at
            # 1", which a backfill must read as "no reserve" rather than "a
            # reserve of nothing" — a log created at 1 and later restored has a
            # gap below its offsets too, and that gap is a fence.
            meta={_START_OFFSET_KEY: str(start_offset)} if start_offset > 1 else {},
        )

    @classmethod
    def _create(
        cls,
        layout: Layout,
        *,
        schema: pa.Schema,
        order: tuple[str, ...],
        settings: LogConfig,
        published: str | None,
        s3_options: S3Options | None,
        first_offset: int,
        meta: Mapping[str, str],
    ) -> Self:
        """Create the log's files, its first appended row taking `first_offset`,
        with `meta` written beside its shape. `new`, and `restore` rebuilding a
        log from its published table, once each has decided what to create."""
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
        if first_offset > 1:
            buffer.seed_offsets(first_offset)

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
                **meta,
            }
        )

        # Built here and handed to all three, so each is given its published
        # table at construction rather than having one pushed into it
        # afterwards.
        remote = Published(layout, buffer, s3_options)

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
        s3_options: S3Options | None = None,
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

        remote = Published(layout, buffer, s3_options)
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
        s3_options: S3Options | None = None,
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

        return litestream_config(
            layout, published, s3_options or S3Options(), retention
        )

    @classmethod
    def restore(
        cls,
        root: PathLike[str] | str,
        name: str,
        *,
        published: str,
        s3_options: S3Options | None = None,
        binary: str | None = None,
        schema: pa.Schema | None = None,
        sort_by: Sequence[str] | None = None,
        config: LogConfig | None = None,
        replica_reserve: int = RESTORE_RESERVE,
        published_reserve: int = PUBLISHED_RESTORE_RESERVE,
    ) -> Self:
        """Recover a log onto a machine that is not the one that wrote it (§3a).

        Point it at the published table, get a working log back, resume
        appending. The procedure this replaces was "restore the databases and
        open it", which does not work: `catalog.db` records ABSOLUTE paths to
        the staging table's Iceberg metadata, and a sidecar ships the `.db`
        files and nothing else — not that metadata, not the Parquet. So a
        restored catalog names files on a machine that is gone, and `open`
        raises `FileNotFoundError`.

        What is recovered, and what is not, when a WAL replica of `buffer.db`
        exists (with none, see "No replica" below):

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

        **No replica** — a log that ran with `wal_replication` off, or whose
        published table is a local directory — and the log is rebuilt from the
        published table alone (#144): at its own name, with every row the
        table holds and none it does not. Rows the dead machine had buffered or
        sealed but not published are gone.

        **The log's shape comes from whatever records it exactly, and the
        caller supplies what nothing does.** A replica's `buffer.db` carries
        the declared schema and `sort_by`, and so does a published table any
        0.10+ publish has stamped (`litelink.arrow_schema`,
        `litelink.sort_by`); a `schema` or `sort_by` passed as well must match
        them exactly. A published table no 0.10+ publish stamped records
        neither exactly — Iceberg has one string type and one binary type and
        keeps no Arrow field metadata — so rebuilding from one REQUIRES both,
        checked against what Iceberg does record: the columns, their order,
        types up to `large_*`, nullability, and the declared sort order.

        **`config`** is the policy the restored log runs under, validated
        against its shape before anything is created. Without it, a replica's
        recorded config is kept, and a rebuild from the published table — which
        carries none — uses `LogConfig()`.

        **The restored log resumes above the freshest record of what the old
        log issued, by that record's reserve**, so no offset a reader saw names
        a different row. There are two records, and each reserve covers what
        can have been issued after its own:

        - **The WAL replica's sequence.** Unseen after it: replication lag, so
          `replica_reserve` (2^20).
        - **The published table's `litelink.issued_through`**, recorded by
          every push — or its end, for a table only an older version published.
          Unseen after it: everything issued since the last publish, for as
          long as publishing was behind or down, so `published_reserve` (2^40).

        The freshest one decides: a healthy replica is ahead of the last
        publish, and a replica whose sidecar stopped shipping is not. With no
        replica, the published record is all there is.

        **A replica is always used when there is one**, for its unpublished
        rows and its settings. A replica left behind when WAL replication was
        turned off is still found, and the log comes back with the settings it
        had then — its offsets are safe, since the published record is fresher
        and decides. Delete the replica (`<published>/<name>/_wal`) when turning
        replication off.

        """
        # First, because this path does not go through `validate` — it takes no
        # schema and no config — and a malformed prefix would otherwise surface
        # as a YAML parse error from the litestream subprocess, after the root
        # has already been created.
        validate_published(published)
        for label, value in (
            ("replica_reserve", replica_reserve),
            ("published_reserve", published_reserve),
        ):
            # `bool` is an int to Python, and never what a caller means here.
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                msg = f"{label} must be a non-negative integer, not {value!r}"
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

        options = s3_options or S3Options()
        if not is_remote(published) and not resuming:
            # A local published table has no WAL replica beside it, so there is
            # nothing to pull: the log is rebuilt from the table.
            return cls._restore_published(
                layout,
                published,
                options,
                published_reserve,
                schema=schema,
                sort_by=sort_by,
                config=config,
            )

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
            # No replica: litestream found none, and said so by exiting cleanly
            # and writing nothing — a bucket it cannot reach or credentials it
            # is refused raise above instead. The published table is then all
            # that can be recovered, and the log is rebuilt from it.
            config_path.unlink(missing_ok=True)
            return cls._restore_published(
                layout,
                published,
                options,
                published_reserve,
                schema=schema,
                sort_by=sort_by,
                config=config,
            )

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
                    f"set when it is created and cannot be changed"
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

        recorded_schema = pa.ipc.read_schema(pa.py_buffer(bytes.fromhex(raw_schema)))
        recorded_sort = tuple(json.loads(raw_sort))
        _refuse_other_shape(
            "the replica", recorded_schema, recorded_sort, schema, sort_by
        )
        schema = recorded_schema
        sort_by = recorded_sort
        if config is not None:
            validate(schema, sort_by, config, published)

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
            released, resumed = buffer.strip_local_state(replica_reserve)
            # The first offset past what the replica records the log issued.
            replica_known = resumed - replica_reserve

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
                released -= buffer.evict_rows(0, covered[1])
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
            # And above what the last publish recorded the log had issued: a
            # replica whose sidecar stopped shipping can trail it by far more
            # than any fence sized for replication lag.
            # Every push records what the log had issued in the commit that
            # registers it, so where the stamp exists it is at least the
            # published end; only a table an older version published has none.
            issued = published_issued_through(layout, published, options)
            published_known = frontier if issued is None else issued + 1
            # The FRESHER record decides, by its own reserve: each covers what
            # can have been issued after it. A replica ahead of the last publish
            # leaves only replication lag unseen; a published record ahead of
            # the replica — its sidecar stopped shipping — leaves everything
            # since the last publish.
            if published_known > replica_known:
                wanted = published_known + published_reserve
            else:
                wanted = replica_known + replica_reserve

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

        log = cls.open(layout.root, name, s3_options=options)
        if config is not None:
            log.set_config(config)

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
        # `resumed` minus the reserve. A resumed restore reserves twice, so
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

    @classmethod
    def _restore_published(
        cls,
        layout: Layout,
        published: str,
        options: S3Options,
        reserve: int = PUBLISHED_RESTORE_RESERVE,
        *,
        schema: pa.Schema | None = None,
        sort_by: Sequence[str] | None = None,
        config: LogConfig | None = None,
    ) -> Self:
        """Rebuild a log from its published table, when there is no WAL
        replica to restore (#144).

        With no replica, the published table is everything that can be
        recovered: rows that were buffered, or sealed into staging but not yet
        published, went with the machine. So the log is rebuilt as if it had
        never had a `buffer.db` — at its own name, with no seam:

        - **its shape** from the published table's stamp, or from the caller
          for a table no 0.10+ publish stamped (see `restore`);
        - **the caller's `config`**, or `LogConfig()`, since a published table
          carries none;
        - **the published table adopted** as the log's own, its watermark at
          the table's end;
        - **the offset counter above everything the old log is known to have
          issued, plus `PUBLISHED_RESTORE_RESERVE`**: offsets the old log issued
          but never published may have reached readers, and must never name
          different rows. Known means the published end, or the
          `litelink.issued_through` the last publish recorded when that is
          higher; the fence covers what was issued after it.
        """
        covered = published_span(layout, published, options)
        shape = published_shape(layout, published, options)
        if covered is None or shape is None:
            msg = (
                f"no replica of {layout.buffer_db.name} under {published}, and no "
                f"published rows there to rebuild the log from — so there is nothing "
                f"to restore. Check `name` and `published`; a log that never published "
                f"is started afresh with `new`"
            )
            raise FileNotFoundError(msg)

        recorded_retirement = published_retired(layout, published, options)
        if recorded_retirement is not None:
            raise RetiredError.of(
                {"state": "retired", "published": published, **recorded_retirement},
                layout.name,
            )

        if shape.schema is not None and shape.sort_by is not None:
            _refuse_other_shape(
                "the published table", shape.schema, shape.sort_by, schema, sort_by
            )
            schema, order = shape.schema, shape.sort_by
        else:
            if schema is None or sort_by is None:
                msg = (
                    f"the published table at {published} was written before litelink "
                    f"recorded a log's shape on it, and Iceberg cannot record it "
                    f"exactly — one string type, one binary type, no Arrow field "
                    f"metadata. Pass the log's `schema` and `sort_by` to restore; they "
                    f"are checked against what the table does record"
                )
                raise ValueError(msg)

            order = tuple(sort_by)
            _refuse_unlike_iceberg(schema, order, shape.iceberg, shape.declared_sort)

        settings = config or LogConfig()
        validate(schema, order, settings, published)
        frontier = covered[1]
        # Every push records what the log had issued in the commit that
        # registers it, so where the stamp exists it is at least the published
        # end. A table only an older version published has none, and the end
        # is all there is.
        issued = published_issued_through(layout, published, options)
        known = frontier if issued is None else issued + 1
        first = known + reserve
        _log.warning(
            "litelink: no WAL replica of %s under %s; rebuilding the log from its "
            "published table, which holds offsets below %d. Rows the old machine "
            "had not published are not recovered.",
            layout.name,
            published,
            frontier,
        )
        log = cls._create(
            layout,
            schema=schema,
            order=order,
            settings=settings,
            published=published,
            s3_options=options,
            first_offset=first,
            meta={PUBLISHED_THROUGH_KEY: str(frontier - 1)},
        )
        # Adopted now, as a WAL restore does, so reads reach the published rows
        # from the first query rather than after the first maintenance pass.
        if log._published.table(repair=True) is None:  # noqa: SLF001
            msg = (
                f"rebuilt the log but could not adopt the published table at "
                f"{published!r}"
            )
            raise RuntimeError(msg)

        log._restored_from = _Recovery(  # noqa: SLF001
            recovered=0,
            resumed_at=first,
            skipped=(frontier, first),
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
        # No claim. `validate` refuses a PAIR — `wal_replication` with no
        # remote published table — and the other half, the published location,
        # is fixed when the log is created, so the check reads a value nothing
        # can change and this is a single row write. Every routine re-reads the
        # policy at the point it decides, so a change takes effect at the next
        # decision, not mid-way through one.
        validate(
            self._schema,
            self._buffer.sort_by(),
            config,
            self._buffer.get_meta(_PUBLISHED_KEY) or None,
        )
        # The only write. There is nothing to fan out: `Maintenance`, the seal
        # target and `config` all read this row rather than keeping copies.
        self._buffer.set_meta(_CONFIG_KEY, config.to_json())

    def recovery(self) -> _Recovery | None:
        """What `restore` recovered, or None on a log opened normally.

        Two numbers an operator needs and cannot get later: how many rows came
        back from the replica, and which offsets were skipped to avoid
        reissuing ones the dead machine had already served.
        """
        return self._restored_from

    def _claim_settings(self) -> Claim:
        """Take the whole-log claim the configuration operations share.

        Retried, not refused on the first try. These operations collide with
        ordinary maintenance, which a maintainer runs continuously in another
        process. Measured before this wait existed: one
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
        # compare, signal, or start anything: a maintainer calls `seal` and
        # finds the work waiting, exactly as it calls `advance` and finds
        # files to compact.
        return self._buffer.append(rows)

    # -- bulk ingest ---------------------------------------------------------

    def ingest(
        self,
        source: pa.Table | pa.RecordBatchReader,
        *,
        publish: bool = True,
        flush: bool | None = None,
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

        **`wal_replication` is not refused.** WAL shipping cannot carry a bulk
        range: with replication on the buffer IS the off-box copy until the
        published table has the range (§3a), and these rows never enter the
        buffer. Reproduced against a zero-lag replica: 920 rows acknowledged,
        420 restored, `recovery()` reporting plain success.

        Refusing — load with replication off, turn it on afterwards — would be
        strictly worse, because `evict("buffer")` reads the same flag: turn
        replication off and the next eviction stops retaining its rows, so the
        buffer's copy of everything ALREADY captured is dropped. Measured on a
        replicated log with a published table: 300 sealed rows retained in the
        buffer, `set_config(wal_replication=False)`, one more seal, and all 300
        are gone from it with the published table holding none. The range is
        uncovered until `publish` either way; what closes it is the push below,
        flushed by default on a replicated log.

        **This pushes its own output to the published table**, and
        `publish=False` opts out. The push runs after the load is durable, so a
        failure leaves the rows loaded and raises saying so — retry the push,
        never the load, which would reserve a fresh range and duplicate it.

        **`flush` decides whether the load's short last file goes too, and by
        default it follows `wal_replication`: a loaded range gets the same
        durability as an appended one.** An ordinary `publish` holds back a
        trailing run still under `target_compact_size`, because a run with room
        in it may yet take files that have not been written — and the last file
        of a load is short unless the load divides evenly.

        - **With `wal_replication`**, an appended row has an off-box copy from
          the moment it commits: the WAL replica, and the buffer keeps it until
          the published table has it (§3a). Loaded rows never enter the buffer,
          so nothing the replica ships contains them, and the published table
          is their only off-box copy. So the default flushes: holding the tail
          back would leave a range that replication promises to cover on one
          disk for as long as the stream stays quiet. Measured on the deployment
          that found it: 113,399 loaded rows on one disk across nine streams,
          with `coverage()` reporting no gap.
        - **Without it**, an appended row's only copy is local until `publish`
          pushes it, and a quiet stream's trailing run stays local the same way.
          So the default does not flush: the tail stays in staging and merges
          with what is sealed after it, rather than becoming an undersized file
          in the immutable table. The source corpus is still a second copy of
          those rows, which is more than an appended row has.

        `flush=True` or `flush=False` overrides the default either way;
        `publish=False` skips the push, and `flush` with it.

        **Compare `published_through()` against the `end - 1` this returns
        whenever the push did not cover the load** — `publish=False`, an
        unflushed tail, or a push that raised after the load had landed. Until
        they meet, the corpus you loaded from is the range's second copy.

        Files come out at `target_compact_size` on disk, written a row group
        at a time and each row group sorted by `sort_by`, which is what makes
        them indistinguishable from a compacted file and so born past the
        maintenance lifecycle: `runs()` closes a run when the next file would
        exceed the budget, so a file already at it forms a run of one, and
        `_merge` rewrites only at `compact_min_files`. Sorted WITHIN a row
        group, as compaction sorts — offsets are materialised in input order
        and then permuted by the sort, so the range stays dense while the rows
        move. "Sorted" and "contiguous" are claims about two different columns.

        **A load that fails leaves a hole, and that is the accepted cost.** The
        offsets of the reservation being written when it failed are gone; §6
        needs files non-overlapping and adjacent in offset order, not free of
        integer gaps, and every pass was measured correct on a gapped log. The
        one price worth stating rather than discovering: compaction will merge
        across the gap, and the published table keeps that file as written.

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
        # `flush` by default only when the log replicates its WAL: then the
        # trailing run is the one part of the log with no off-box copy, since
        # `stable_prefix` holds a load's short last file back for a merge a
        # quiet stream never earns. Read at the call, like every setting.
        if flush is None:
            flush = self.config.wal_replication

        if loaded is not None and publish:
            try:
                # COMPACT first, and it is not tidiness. The push below takes
                # the whole trailing run, so every undersized seal still sitting
                # below the load goes to the published table with it — and
                # compaction will not merge what the published table holds, so
                # they stay small there for ever. Merging them in staging first
                # collapses that to the one file a run genuinely cannot fill.
                # Measured on five small seals: six undersized objects pushed
                # without this, one with it. Flushed with the push, so a tail
                # still filling is merged before it goes too.
                self.compact(flush=flush)
                self.publish(flush=flush)
            except Exception as exc:
                # The LOAD succeeded and its rows are durable in Parquet; only
                # the second copy is missing. Saying so leading with that is the
                # difference between a caller retrying the load — which would
                # reserve a fresh range and duplicate it — and retrying the push.
                msg = (
                    f"loaded offsets [{loaded[0]}, {loaded[1]}) successfully, but "
                    f"could not push them to the published table: {exc}. The rows are in "
                    f"local Parquet and are NOT yet second-copied; retry with "
                    f"publish(flush={flush}) rather than re-running the "
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
        maintainer drains its queue, `_covers` declines the file, and the
        next `evict("buffer")` deletes them. The resulting table is
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

        **A reservation per row group, not one for the load.** `reserve(n)`
        needs `n` up front and a `RecordBatchReader` cannot say how many rows it
        has; materialising to find out is bounded by memory and defeats the
        point at 160M rows. Consuming a row group's worth and reserving exactly
        that many keeps memory at one row group, needs no branch between a
        Table and a reader, and leaves a stream that dies half way with N
        complete files registered and the open file's reservations lost rather
        than one enormous one.

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
        cap = config.target_compact_rows

        def prepared() -> Iterator[tuple[pa.Table, int, int]]:
            """Each row group's worth: shaped, reserved, numbered, sorted."""
            for chunk in chunks(
                reader, reader.schema, config.target_row_group_size, cap
            ):
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

                yield rows, start, end

        # The load's `[first, last)`.
        first: int | None = None
        last: int | None = None
        staged: list[tuple[str, int, int, int]] = []
        groups = prepared()
        try:
            group = next(groups, None)
            while group is not None:
                rows, start, end = group
                rel_path = self._layout.ingest_path(start, uuid.uuid4().hex[:8])
                # I2: the path is in SQLite before the bytes are on disk, so a
                # crash before the commit leaves a file recovery can name rather
                # than one only a directory scan could find. `claim_output`
                # rather than `claim_seal` — see `INGEST_ROLE`. The range is
                # the first row group's, because the file's end is not known
                # until it closes; recovery reads only the path.
                self._buffer.claim_output(start, end, rel_path)
                dest = self._layout.absolute(rel_path)
                dest.parent.mkdir(parents=True, exist_ok=True)
                held = 0
                counted = 0
                with stream_parquet(dest, rows.schema, config.compression) as write:
                    while group is not None:
                        rows, _, end = group
                        size = write(rows)
                        held += rows.nbytes
                        counted += rows.num_rows
                        # `DEFAULT_TTL_MS` is 30 s and this path is sized in
                        # hours, so without a renew per row group the exclusion
                        # evaporates while the first file is still being written.
                        checkpoint(lease.renew)
                        group = next(groups, None)
                        # The file closes at the target on disk, or before the
                        # next row group would carry it past the row ceiling.
                        if size >= config.compact_size or (
                            cap is not None
                            and group is not None
                            and counted + group[0].num_rows > cap
                        ):
                            break

                staged.append((rel_path, start, end, held))
                first = start if first is None else first
                last = end
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
            # What the file holds UNCOMPRESSED, which is the currency every
            # `extent.bytes` is stated in.
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

    def seal(self, *, flush: bool = False) -> int | None:
        """Buffer → staging: write the groups the size trigger has cut. Returns
        the exclusive end offset of the last group written, or None.

        `flush=True` cuts and seals EVERYTHING buffered, however small — the
        same flag `publish` and `advance` take, with the same meaning: push
        everything through this stage now, regardless of thresholds. The cost
        is a file smaller than `target_seal_size`, which compaction merges
        later. Without it, a quiet stream's rows simply stay in the buffer —
        durable, readable, and replicated by §3a — until enough arrive to fill
        a file.

        Cheap when there is nothing due — an indexed read of one row — so it
        can be run far more often than `advance`. A group whose lease is held
        elsewhere is left alone, not waited for; `await_seal` is for a caller
        that needs the table to have moved.
        """
        return self._seal_all() if flush else self._seal_due()

    def _seal_all(self) -> int | None:
        """`seal(flush=True)`: cut everything buffered into files. Returns the
        exclusive end offset.

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
        # `_seal_due` does NOT come through here — it drains queued groups only
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
            self._buffer.finish_seal(end, rel_path)
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

    def _seal_due(self) -> int | None:
        """`seal()`: seal everything the policy says is ready. Returns the last
        end, or None.

        The maintainer's frequent call, and the counterpart to `advance`: both
        are plain methods the caller runs on its own schedule, because the
        library has no business owning a thread or an interval. This one is
        cheap when there is nothing to do — an indexed read of one row — so it
        can be run often; `advance` reads table metadata and wants to be run
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
            self._buffer.finish_seal(end, rel_path)

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
        self._buffer.finish_seal(end, retry)

    def _recover_compaction(self) -> None:
        """Resolve a compaction interrupted before its commit (§11).

        Unlike a seal, an interrupted compaction is not redone. Its inputs are
        still live — the transaction that would have superseded them never
        committed — so the table is already correct and the next `advance()`
        will pick the same run up again. All that is owed is the half-written
        output, and `compacting` names it, so removing it costs one unlink
        rather than a directory scan — or, for a published object an earlier
        version's rewrite claimed, one DELETE rather than a paginated LIST over
        object storage.
        """
        pending = self._buffer.pending_compaction()
        if pending is None:
            return

        # No reload. This used to decide from `file_paths()` and unlink, so it
        # needed the freshest possible view — and still raced. It queues now,
        # and the decision that matters is `drain`'s, which reloads at the
        # moment it removes.

        # EVERY claim, not the first. An interrupted operation can hold several,
        # each claimed before its file existed (I2), so taking one
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

        # Only the rows just read. Another operation can be claiming its next
        # output while this runs, and clearing the table wholesale takes that
        # claim with it — leaving a file named by nothing, which is the state
        # claims exist to prevent.
        for _, _, key in claimed:
            self._buffer.clear_compaction(key)

    # -- maintenance -------------------------------------------------------

    def publish(self, *, flush: bool = False) -> None:
        """Push to the published table: upload, register, record the watermark
        (§5).

        `flush=True` pushes EVERYTHING unpublished, including the
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
        always the staging one — **as long as nobody passes `flush`**.
        That flag exists because a bulk load's rows never enter the buffer, so
        the trailing run holding its short last file has no second copy to wait
        behind, and the rule above would strand it on local disk for as long as
        the stream stayed quiet. A load therefore can leave undersized objects
        in the published table — up to `compact_min_files - 1` seals forming a
        run `_merge` will not rewrite, plus the load's own tail — and compaction
        will not merge them afterwards, because it refuses to touch anything the
        published table holds; they stay as written. `ingest` compacts before
        pushing to keep the count to what a run genuinely cannot
        fill.

        **Publishing only**, a building block. Expiring the published table and
        sweeping it are routines of their own (`expire_published`, `sweep`),
        so an orchestrator can schedule them apart from this (#118);
        `advance` runs all of them.

        DEVIATES from §5, which also lists local eviction (step 5). That is
        local storage work and belongs to `evict`. Publish's remaining obligation to eviction is the
        registration watermark it records in `meta`, which is what lets
        `advance` enforce I4.
        """
        lease, bound = self._publish_lease(flush=flush)
        if not lease.acquire():
            msg = "another owner holds a claim over this range"
            raise RuntimeError(msg)

        try:
            self._push(lease, self._published.uri, flush=flush, bound=bound)
        finally:
            lease.release()

    def _publish_lease(self, *, flush: bool) -> tuple[Claim, int | None]:
        """The claim a publish takes, and the offset its push must stay below
        (None for the whole log) (#118).

        **Only the range it pushes**, `[floor, end)`: from the published
        table's frontier to the end of what this push would upload. Nothing
        else needs to wait for it. A seal lands above the staging table's end,
        eviction below the published floor, and compaction refuses what the
        published table holds — and a compaction over the trailing run, which
        `flush` pushes too, overlaps this range and so is excluded by it.
        The whole-log operations still exclude any publish.

        **Two publishes still exclude each other.** Both start at the same
        floor, so their ranges overlap even when there is nothing to push —
        which matters, because `_push` forgets intents it does not find
        landed, and another publisher's intents are files it is uploading.

        **The whole log, when the push might write the tier row.** With no
        published row yet, `_push` computes it exactly from the published
        table's manifests, and that one narrowing write must not race
        eviction widening it (`_tiers`) — so a first publish, or the first
        after a re-point, claims everything. So does a published table that
        can only be opened by repairing it, which is a whole-log claim
        holder's to do.

        Provisional, and read without a claim: `_push` re-reads everything
        under it and clamps what it uploads to `bound`.
        """
        whole = self._lease(MAINTAIN_ROLE)
        if not self._tiers.has():
            return whole, None

        try:
            published = self._published.table()
        except ValueError:
            # The catalog names another prefix than the log — a re-point by an
            # earlier version, left half done — and only a repairing open,
            # under the whole log, may fix it. `_push` does.
            return whole, None

        if published is None:
            return whole, None

        self._table.reload()
        published.reload()
        covered = published.span()
        floor = 0 if covered is None else covered[1]
        pending = [f for f in self._table.data_files() if f.end > floor]
        settled = self._settled(pending, flush=flush)
        end = pending[settled - 1].end if settled else floor + 1

        return self._lease(MAINTAIN_ROLE, floor, max(end, floor + 1)), end

    def _settled(self, pending: list[DataFile], *, flush: bool) -> int:
        """How many of `pending` — staging files above the published floor, in
        offset order — a push takes: everything compaction is finished with
        (`stable_prefix`), past whatever is already intended or held, or all of
        them with `flush`."""
        if flush:
            return len(pending)

        config = self.config
        frozen = self._maintenance.published_prefix(pending, include_intents=True)
        head = [f for f in pending if f.start >= frozen]

        return (len(pending) - len(head)) + stable_prefix(
            head,
            config.compact_size,
            config.compact_min_files,
            config.target_compact_rows,
        )

    def _push(
        self,
        lease: Claim,
        pinned: str | None,
        *,
        flush: bool = False,
        bound: int | None = None,
    ) -> None:
        """Upload and register everything above the published table's span.

        **`flush` pushes the trailing run too**, which `publish`
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
        merged — the cost is a small object the published table keeps, not a
        duplicate range. The deadlock the
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
        cost with a deliberate cause.
        """
        # Read under the claim, the same as everything else that decides what
        # this pass does. The grouping `stable_prefix` computes has to match
        # the one compaction computes — `runs` is shared so they cannot
        # disagree — and in the shipped topology they are separate processes,
        # so agreement means both reading the policy the log records rather
        # than the one each happened to open with.

        # Opened with `repair` only under the whole-log claim, the one that
        # entitles it (`_publish_lease`).
        published = self._published.require(repair=bound is None)
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

        # The log's own shape, on its published table, so a restore with no WAL
        # replica can rebuild the log exactly (#144). Stamped once: a new table
        # at its first publish, an older one at the first publish after an upgrade.
        if published.properties.get(SCHEMA_PROPERTY) is None:
            raw_schema = self._buffer.get_meta(_SCHEMA_KEY)
            if raw_schema is not None:
                published.set_properties(
                    {
                        SCHEMA_PROPERTY: raw_schema,
                        SORT_PROPERTY: json.dumps(list(self._buffer.sort_by())),
                    }
                )

        # The metadata retention properties, re-applied as a writer's `open`
        # re-applies them to the staging table (#155). They are what keeps the
        # table's `metadata.json` files bounded and its manifests merged, and a
        # table can lose one — changed by hand or by another engine, adopted
        # from elsewhere, or older than a property a release adds. A check
        # that commits only when one is missing or wrong.
        published.ensure_metadata_properties()

        # The published table's tier row, if nothing has computed one yet: a
        # log given a published table at `new`, one written before the manifest
        # existed, one re-pointed where the published table could not be read.
        # Until then every query reads the published table. A push adds only
        # copies of staging rows, so it never changes this row itself —
        # eviction does.
        if not self._tiers.has():
            if bound is not None:
                # The row vanished between `_publish_lease` choosing a range
                # and this claim being taken. Writing it is an exact rollup
                # that must not race eviction's `widen` (`_tiers`), so it needs
                # the whole log — and this push holds only a range. So it
                # pushes NOTHING, and the next publish, finding no row, claims
                # the whole log and writes it.
                #
                # Only something dropping the row concurrently reaches here, and
                # nothing does while a log is live: `restore` drops it, but on a
                # fresh machine before any publish. So this is a guard rather
                # than a path — and a publish that silently did nothing is
                # exactly what an operator wants to hear about, since every row
                # it would have pushed stays local one pass longer.
                _log.warning(
                    "litelink: publish of %s pushed nothing: the published "
                    "table's tier row was dropped while the push was claiming "
                    "its range; the next publish claims the whole log and "
                    "pushes",
                    self._layout.directory,
                )
                return

            self._record_published_row(published)

        # The published span's end: everything below it is in the bucket.
        covered = published.span()
        floor = 0 if covered is None else covered[1]

        # RECONCILIATION against the published table's own manifest, which is
        # the truth, matched by path. The watermark is written after a register
        # lands, so a crash between the two leaves the published table holding
        # a range the watermark does not name yet: raised here to the span's
        # end, it covers that range again, and the intents of copies the
        # manifest holds are retired with it — one transaction, so compaction
        # never sees a moment with neither (`confirm_published`).
        #
        # Matched by PATH rather than by offset range. Range matching reads
        # plausibly and is wrong: an intent can name an object over a range
        # another file already covers, so a crashed push's dead intents would be
        # confirmed rather than dropped.
        #
        # Bounds differ between the two reads and must. The manifest walk is
        # bounded by the staging window, or it grows with the published table
        # and runs on every publish pass. The intent read is unbounded, because
        # an intent below the window has to be reachable to be dropped.
        local = self._table.data_files()
        base = min((f.start for f in local), default=0)
        held_paths = {f.path for f in published.data_files() if f.end > base}
        intended = [path for path, _, _, _ in self._buffer.intents(pinned or "")]
        self._buffer.confirm_published(
            floor - 1, [path for path in intended if path in held_paths]
        )
        for path in intended:
            if path not in held_paths:
                # Nothing in the manifest holds that path, so the register
                # never landed and the intent is dead. Below the staging window
                # this also drops intents whose register DID land — the manifest
                # walk is bounded — and the watermark covers those already.
                self._buffer.forget_intent(path)

        memory = self._maintenance.memory()
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
        #
        # `_settled` is the rule, shared with `_publish_lease` so the range a
        # publish claims and the files it pushes are decided the same way.
        #
        # With `flush`, EVERYTHING unpublished, and that is forced rather than chosen.
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
        # published table holds, so any small objects stay as they are.
        settled = self._settled(pending, flush=flush)
        if bound is not None:
            # Inside the range claimed, and nothing past it: a seal or another
            # publish may have moved things since `_publish_lease` read them.
            # A prefix still, so the watermark stays contiguous.
            settled = sum(
                1
                for _ in itertools.takewhile(
                    lambda f: f.end <= bound, pending[:settled]
                )
            )

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
                memory.get(data_file.path, data_file.size),
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

        if not published.register(
            [published.uri(rel_path) for _, rel_path in uploaded],
            end=last.end,
            # The low end too, so the published table can refuse a range that
            # starts inside what it already holds. Everything upstream is arranged so
            # that cannot happen; this is the check that holds regardless of
            # whether the arrangement has a gap.
            start=uploaded[0][0].start,
            # What the log has issued, recorded with the rows that land, so a
            # restore with no replica knows what not to issue again (#144).
            properties={ISSUED_PROPERTY: str(self._buffer.next_offset() - 1)},
        ):
            return

        # After the register, never before: the watermark is a promise that the
        # published table HAS the range, and eviction acts on it. Raised and the
        # intents retired together (`confirm_published`), so compaction never
        # sees a moment covered by neither. Stored as the last offset held, and
        # never lowered: another publish on a disjoint range (#118) can have
        # recorded a higher one while this push was registering.
        self._buffer.confirm_published(
            last.end - 1, [published.uri(rel_path) for _, rel_path in uploaded]
        )

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

    def advance(self, *, flush: bool = False) -> None:
        """Advance the log's rows: every maintenance routine, in the order rows
        move from the buffer to the published table, each table swept after the
        last step that can change it. The one call most deployments want (§5,
        §6, §8, §12).

        `flush=True` pushes everything through, regardless of thresholds: it
        passes `flush` to `seal`, `compact` and `publish`, so every buffered row
        is sealed, the trailing run merged, and every staging file published in
        this pass — at shutdown, say, to
        get everything off this machine. The cost is undersized files, in
        staging and in the published table, where they stay.

        Data moves first, then cleanup follows behind it:

        1. `seal` — buffer to staging;
        2. `compact` — merges a run once it is closed and has
           `compact_min_files` files;
        3. `publish` — staging to published, the files compaction is done with;
        4. `evict("buffer")` — rows the next durable copy holds;
        5. `evict("staging")` — files the published table holds, in the same
           pass (I4);
        6. `reclaim("buffer")`, only when `vacuum_free_ratio` is set — the one
           step that blocks appends;
        7. `reclaim("staging")` — expire snapshots, delete what only they used;
        8. `sweep("staging")`;
        9. `reclaim("published")`;
        10. `sweep("published")`.

        Each is callable on its own, and that is the point of listing them: an
        orchestrator with schedules that differ because the costs do, or that
        wants the published side in another process, calls the routines and
        not this (#118). Each declares its own exclusion — a claim on the
        offsets it touches, or none — so running them apart is safe.

        **Every log publishes** (#98): locally by default, or to S3. **A
        publish that fails raises, after local maintenance**: on a machine cut
        off from a remote published table, steps 4-8 still run —
        eviction has nothing new to take, which is §11's "local eviction
        stalls", but expiry and the staging sweep keep reclaiming — then the
        published steps are skipped and the publish's error is raised. That
        includes another owner holding the lease: maintenance is meant to run
        in one process, so a second one is a deployment error worth hearing
        about, not contention to wait out.

        **Eviction is bounded by I4**: a file the published table does not
        hold yet is never evicted, however old. Eviction never deletes data.
        """
        # Sealing first, so what this pass seals is compacted and published in
        # this pass rather than the next. Compaction only merges a run that is
        # ready and `publish` only takes what compaction is finished with, so
        # a file sealed a moment ago is never touched before its time.
        self.seal(flush=flush)
        self.compact(flush=flush)

        # Held, not raised, so the local steps still run on a machine that
        # cannot reach a remote published table (§11).
        failure: Exception | None = None
        try:
            self.publish(flush=flush)
        except Exception as exc:  # noqa: BLE001 — re-raised below
            failure = exc

        # Buffer before staging: the buffer's boundary is proved by staging
        # files, which staging eviction is about to remove.
        self.evict("buffer")
        self.evict("staging")

        # Off unless `vacuum_free_ratio` is set, because this is the one step
        # that blocks appends; see `reclaim`.
        ratio = self.config.vacuum_free_ratio
        if ratio is not None:
            self.reclaim("buffer", min_free_ratio=ratio)

        self.reclaim("staging")
        self.sweep("staging")
        if failure is not None:
            raise failure

        self.reclaim("published")
        self.sweep("published")

    def compact(self, *, flush: bool = False) -> None:
        """Merge undersized staging files into `target_compact_size` ones (§6).

        A run is merged once it is closed: the next file would take it past the
        target, or it has reached it. The trailing run waits for the files
        still to come, so each row is compacted once. `flush=True` merges the
        trailing run too, for a log about to publish everything it holds.

        The heavy step of `advance`: it reads and rewrites whole files, while
        eviction and expiry are metadata commits that finish in milliseconds,
        which is why the steps are callable separately. It claims each run it
        merges (§4a) and renews that claim as it works, so it excludes another
        maintainer only where their work overlaps.

        Staging only. The published table is never rewritten: it is the log's
        immutable record, and `publish` pushes only files compaction has
        finished with, so it is well-sized by construction. The exceptions —
        a flushed seal or publish, a bulk load's tail, a raised
        `target_compact_size` — leave a few smaller files, which stay as they
        were written.
        """
        self._maintenance.compact(flush=flush)

    def evict(
        self,
        table: Literal["buffer", "staging"] | None = None,
        *,
        start_offset: int | None = None,
        end_offset: int | None = None,
    ) -> None:
        """Drop data the next durable copy already holds (§8, #122). `table` is
        `"buffer"`, `"staging"`, or None for both, buffer first.

        One rule for both: never drop data until the next durable copy has it.

        - **`"buffer"`**: rows staging holds — or, with `wal_replication`, rows
          the published table holds, since until then the buffer is their
          off-box copy (§3a). A seal never deletes its own rows; this does.
        - **`"staging"`**: files past `staging_retention` / `staging_rows` that
          the published table holds (I4). Never past what it holds, so a
          publish that is behind delays this rather than losing data.

        `[start_offset, end_offset)` — half-open, like `scan` — narrows what is
        eligible; it never widens it. **Chunking is the caller's.** Unbounded,
        a buffer eviction is one `DELETE` under SQLite's write lock and stalls
        appends for its duration; bounding the range is how to keep each
        stall short. Staging eviction always removes a PREFIX of the table, so
        a `start_offset` above its first offset is refused with `ValueError`
        rather than evicting nothing.
        """
        valid = _covers(table, "buffer", ("buffer", "staging"))
        if (
            start_offset is not None
            and end_offset is not None
            and end_offset < start_offset
        ):
            msg = f"end_offset {end_offset} is below start_offset {start_offset}"
            raise ValueError(msg)

        if valid:
            self._maintenance.evict_buffer(start_offset, end_offset)

        if _covers(table, "staging", ("buffer", "staging")):
            if start_offset is not None:
                self._table.reload()
                span = self._table.span()
                if span is not None and start_offset > span[0]:
                    msg = (
                        f"staging eviction removes a prefix of the table, which "
                        f"starts at offset {span[0]}; a start_offset of "
                        f"{start_offset} would leave a gap below it"
                    )
                    raise ValueError(msg)

            self._maintenance.evict(end_offset=end_offset)

    def reclaim(
        self,
        table: Literal["buffer", "staging", "published"] | None = None,
        *,
        min_free_ratio: float = 0.0,
    ) -> None:
        """Turn what eviction and expiry left behind into free disk (§3a, §6,
        #122). `table` is `"buffer"`, `"staging"`, `"published"`, or None for
        all three, in that order.

        - **`"buffer"`**: `VACUUM`, returning `buffer.db`'s dead pages to the
          OS — when the free list is at least `min_free_ratio` of the file.
          SQLite never shrinks the file on its own, and litestream replicates
          the FILE, so it is failover that pays: measured on a 1-day-old
          capture, 457 MB holding 20,658 live rows with 92% of its pages free,
          restoring in 12.5 s against 0.8 s vacuumed. **It blocks appends**
          for as long as the live data takes to copy (0.3 s at 35 MB), which
          is why `advance` runs it only when `vacuum_free_ratio` is set.
          `litelink_offset` and the AUTOINCREMENT counter are untouched (I9).
        - **`"staging"` / `"published"`**: two steps, and **a file is not
          deleted by the call that frees it.**

          1. *Expire*: drop snapshots older than the table's snapshot
             retention. This is metadata only; Iceberg deletes no file. Every
             file that no remaining snapshot uses — and every file a
             compaction or eviction superseded — has its path queued in
             `pending_delete`, stamped with when it stopped being referenced.
          2. *Drain*: delete the queued files whose **grace period** has
             passed — the same snapshot retention, counted from that stamp —
             and that no live snapshot references. The grace is what keeps a
             scan that resolved an older snapshot from losing files under it
             (I6).

          So what this call frees is deleted by a LATER call, one retention
          after it stopped being referenced; a retention of zero deletes it in
          the same call. Expiry takes no claim — a metadata commit the
          catalog's compare-and-swap orders — and the delete takes the claim
          `drain` does. A published table `publish` has not created yet is
          skipped.
        """
        allowed = ("buffer", "staging", "published")
        if _covers(table, "buffer", allowed):
            self._buffer.reclaim_free_pages(min_free_ratio)

        if _covers(table, "staging", allowed):
            self._maintenance.expire()

        if _covers(table, "published", allowed):
            self._maintenance.expire_published()

    def sweep(self, table: Literal["staging", "published"] | None = None) -> None:
        """One pass of the stranded-metadata sweep (§6). `table` is
        `"staging"`, `"published"`, or None for both.

        What a commit that lost its pointer swap, or crashed before it, left
        behind. Lists each table's `metadata/` at the first call in a process
        and every four hours after, and deletes at most 500 files a call.
        Takes no claim — a dead metadata file's name is never reused — so it
        can run anywhere beside anything, a daemon thread included. Never
        raises; a failure is logged and retried on the next call.
        """
        allowed = ("staging", "published")
        if _covers(table, "staging", allowed):
            self._maintenance.sweep_staging()

        if _covers(table, "published", allowed):
            self._maintenance.sweep_published()

    def retire(self) -> None:
        """End this log for good: every row to the published table, nothing
        left local.

        After it, the log takes no rows — from this handle, from a writer that
        opened before it ran, or from anything that `open`s or `restore`s it —
        and each refusal says when it retired and at which offset, so the next
        log can start at the one after. Reads still work: `open(...,
        read_only=True)` reads the published table, and so does any Iceberg
        engine.
        A retired log replayed often from another machine can be read through
        a `duckdb_connection` with `disk_cache=True`, which keeps what is read
        across restarts (#118).

        Steps, each safe to re-run — a crash leaves the log `retiring`, and
        calling this again finishes it:

        1. give the buffer an end — the offset the next append would have
           taken — and flush the WAL replica. From this commit no append lands
           (the buffer's trigger keys on that end), so nothing can arrive after
           the final push;
        2. seal everything buffered;
        3. `publish(flush=True)`, which also releases the rows a
           `wal_replication` seal was holding;
        4. evict the whole staging table, and check nothing is left local;
        5. record the retirement on the published table (`litelink.retired`),
           so `restore` refuses whatever a replica says;
        6. `reclaim` both tables, as `advance` does — expire their snapshots
           past retention, which takes a table that carried thousands down to
           the few its retention keeps (#152) — then delete everything that
           queued at once, without the grace: the log takes no more passes, so
           nothing left queued would ever go (#153). A scan still reading an
           expired snapshot of a log being retired can lose its files;
        7. delete every stranded metadata file in both tables — what a commit
           that lost its pointer swap or crashed left behind (#113). A retired
           log takes no more passes, so the sweep gets no later chance;
        8. narrow the buffer's range to `[end, end)`, empty, and flush the WAL
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
            self.seal(flush=True)
            self.await_seal()

        self.publish(flush=True)
        # Buffer first, while the staging files that prove the published
        # table holds those rows are still there to ask.
        self._maintenance.evict_buffer()
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

        # Expired first, as `advance` does (#152). A log that ran under a
        # version with no published expiry can carry thousands of snapshots,
        # each with its own manifest of every file, and everything that walks
        # them grows with that; expiry takes the table down to the snapshots
        # its retention keeps. Each drains what has come due after.
        self.reclaim("staging")
        self.reclaim("published")
        # And what that expiry queued, now rather than after the grace (#153).
        # The grace lets a scan already reading an expired snapshot finish; a
        # retired log takes no more passes, so whatever it leaves queued is
        # never deleted. Referenced files are still kept: the drains' veto
        # stands.
        self._maintenance.drain(grace=timedelta(0))
        self._maintenance.drain_published(grace=timedelta(0))
        # Every stranded metadata file in both tables, all at once (#113). A
        # retired log takes no more passes, so this is the sweep's last chance
        # — and the manifests merged away before #112 are a backlog of
        # thousands. Before the last step, so a failure leaves the log
        # retiring and calling this again finishes it.
        self._maintenance.sweep_everything()

        # Last: the buffer's range narrowed to `[end, end)` with a record count
        # of 0 — which is what makes the log read as retired rather than
        # retiring, and lets every read skip the buffer without reading it.
        self._buffer.empty_buffer_range(encode_tier(schema, empty(schema)))
        self._flush_replica()

    def _flush_replica(self) -> None:
        """Ship `buffer.db` to its WAL replica now, when one is being kept."""
        if self.config.wal_replication:
            flush(self._layout)


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

    if config.compact_size < 1:
        msg = f"target_compact_size must be at least 1: {config.compact_size}"
        raise ValueError(msg)

    if config.target_row_group_size < 1:
        msg = (
            f"target_row_group_size must be at least 1: {config.target_row_group_size}"
        )
        raise ValueError(msg)

    if (
        config.target_compact_rows is not None
        and config.target_seal_rows is not None
        and config.target_compact_rows < config.target_seal_rows
    ):
        msg = (
            f"target_compact_rows ({config.target_compact_rows}) must be at "
            f"least target_seal_rows ({config.target_seal_rows}): compaction "
            "converts sealed files into larger ones, and under a lower ceiling "
            "every sealed file is a run of its own, so nothing would ever merge"
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


def _covers(table: str | None, which: str, allowed: tuple[str, ...]) -> bool:
    """Whether a routine's `table` argument includes `which`; None is all of
    `allowed`.

    Refuses anything else, so a misspelt table — or one this routine does not
    act on — is an error rather than a call that silently did nothing.
    """
    if table is not None and table not in allowed:
        names = ", ".join(f'"{t}"' for t in allowed)
        msg = f"table must be one of {names} or None, not {table!r}"
        raise ValueError(msg)

    return table is None or table == which
