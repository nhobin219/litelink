"""The core append-only stream.

A `Log` is one stream: one SQLite buffer, one local Iceberg table, and
optionally one archive table (SPEC §1). Rows are durable at commit and
queryable immediately; `offset` is assigned by the library and is the only
column it owns (§2, I11).

Core library only. The blob-field extension (§15) is deliberately absent —
applications that need a small binary column declare an ordinary `binary`
column in their own schema, which §15.2 already says is the right route for
payloads that fit comfortably in the buffer.

`sync`, `hydrate` and schema evolution are not implemented; their signatures
are the design, and they raise.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from litelink._buffer import Buffer
from litelink._fs import fsync
from litelink._layout import Layout
from litelink._maintenance import Maintenance
from litelink._read import Reader
from litelink._table import LogTable
from litelink._types import validate_schema

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence
    from os import PathLike
    from types import TracebackType
    from typing import Self

    Row = Mapping[str, object]

# Keys in the buffer's `meta` table (§2). These hold what the Iceberg table
# cannot: deployment policy rather than data shape.
_CONFIG_KEY = "config"
_ARCHIVE_KEY = "archive"
_SCHEMA_KEY = "arrow_schema"


@dataclass(frozen=True, slots=True)
class LogConfig:
    """SPEC §12.

    The defaults are the spec's worked examples, not measured optima — §7's
    numbers come from a 2 vCPU box and every one of these wants re-measuring on
    target hardware.
    """

    # Seal at min(target_size, max_age). §7: this is a READ-LATENCY knob before
    # it is a file-size knob — the buffer is the entire variable cost of a hot
    # read, so seal small and often and let compaction produce the big files.
    #
    # BYTES, not rows. §13.3 is the deciding argument: a row-count bound can
    # exceed a byte-based memory limit, so it loses the race to the OOM killer
    # in exactly the situation the bound exists to prevent.
    #
    # The 8 MiB default is §7's row guidance restated — its table puts a 20k-row
    # buffer at 8.0 MB at the 400-byte row it measured, and 20k rows is the
    # ceiling it recommends.
    #
    # That equivalence is what does NOT generalise. §7's buffer cost is per ROW
    # (SQLite is row-oriented: 1.0 us/row at 20k, 2.3 us/row at 180k), so a
    # stream of 40-byte rows reaches 8 MiB at 200k rows and a read-latency
    # ceiling meant to hold at 20k is breached tenfold. Bytes bound memory; rows
    # bound read latency; they are different failure modes. A narrow-row stream
    # may eventually need `min(target_size, target_rows, max_age)` —
    # deliberately not added now, on one knob until a real workload demands the
    # second.
    target_size: int = 8 * 1024 * 1024
    max_age: timedelta = timedelta(minutes=5)

    # §8. Must exceed the longest hot-path lookback WITH margin.
    #
    # None keeps everything locally and grows without bound. Zero means "evict
    # on upload" — pure archival capture, hot reads limited to the buffer — and
    # presupposes an archive: with `archive=None` it would delete each file as
    # soon as it sealed, so the pair is rejected at construction rather than
    # honoured.
    local_retention: timedelta | None = None
    # §6/§8. Must exceed the longest scan: expiry deletes files an open scan is
    # still reading (I6).
    snapshot_retention: timedelta = timedelta(hours=1)

    # §6. compact_below defaults to half of target_size when None.
    compact_below: int | None = None
    compact_min_files: int = 4

    # §3a. Continuous SQLite WAL shipping. Off by default; decouples RPO from
    # max_age at the cost of a sidecar.
    wal_replication: bool = False

    def to_json(self) -> str:
        """Serialised for the `meta` table, so `open` recovers the policy.

        Durations as seconds rather than any richer encoding: this is read by
        the next process to open the log, and a float is the one representation
        that cannot drift between library versions.
        """
        return json.dumps(
            {
                "target_size": self.target_size,
                "max_age": self.max_age.total_seconds(),
                "local_retention": (
                    None
                    if self.local_retention is None
                    else self.local_retention.total_seconds()
                ),
                "snapshot_retention": self.snapshot_retention.total_seconds(),
                "compact_below": self.compact_below,
                "compact_min_files": self.compact_min_files,
                "wal_replication": self.wal_replication,
            }
        )

    @classmethod
    def from_json(cls, encoded: str) -> LogConfig:
        raw = json.loads(encoded)
        retention = raw["local_retention"]

        return cls(
            target_size=raw["target_size"],
            max_age=timedelta(seconds=raw["max_age"]),
            local_retention=None if retention is None else timedelta(seconds=retention),
            snapshot_retention=timedelta(seconds=raw["snapshot_retention"]),
            compact_below=raw["compact_below"],
            compact_min_files=raw["compact_min_files"],
            wal_replication=raw["wal_replication"],
        )


class Log:
    """One append-only stream.

    Single writer per log (§1): SQLite's write lock is per file, and one process
    per stream is the intended topology.

    Threads within that process are fine, and scheduling `maintain()` on a
    background thread is the expected shape — every public method takes one
    lock. A second *process* is not: maintenance commits to the catalog and
    writes the buffer database, so it is a writer, and two of those is the case
    §1 excludes.

    Construct through `open` or `open_readonly`. The initialiser takes already
    built collaborators and does no I/O, so a test can substitute any of them.
    """

    def __init__(
        self,
        *,
        layout: Layout,
        table: LogTable,
        buffer: Buffer,
        schema: pa.Schema,
        sort_by: Sequence[str],
        config: LogConfig,
        archive: str | None = None,
        readonly: bool = False,
    ) -> None:
        self.name = layout.name
        self.root = layout.root
        self.config = config
        self.readonly = readonly

        self._layout = layout
        self._table = table
        self._buffer = buffer
        self._schema = schema
        self._sort_by = tuple(sort_by)
        self._archive = archive
        self._table_schema = table_schema(schema)

        self._reader = Reader(layout, table, self._table_schema)
        self._maintenance = Maintenance(table, buffer, layout, config, self._sort_by)
        # One lock over every operation, so a maintenance thread and an append
        # loop can share a Log. Coarse on purpose: a seal and a compaction both
        # span several statements plus an Iceberg commit, and interleaving them
        # would let a compaction supersede a range a seal is still writing into.
        # The cost is that a read blocks behind a running compaction — worth
        # revisiting when compaction gets long enough to notice.
        self._lock = threading.RLock()

    # -- construction ------------------------------------------------------

    @classmethod
    def new(
        cls,
        root: PathLike[str] | str,
        name: str,
        *,
        schema: pa.Schema,
        sort_by: Sequence[str],
        config: LogConfig | None = None,
        archive: str | None = None,
    ) -> Self:
        """Create a log. Raises if one already exists at `root/name`.

        This is the only call that takes the log's shape, because the shape is
        fixed at creation and recovered by `open` thereafter.

        `schema` is the application's columns. The library adds `offset` and
        owns nothing else — no ingest timestamp, no transaction id (§2).

        Arrow, always. The table underneath is Iceberg and its schema could be
        stated directly, but every Iceberg field needs an explicit `field_id`
        and pyiceberg accepts duplicates without complaint — two fields
        numbered 1 construct fine. Field IDs are what §9's add, drop and rename
        resolve by, so a duplicate quietly breaks evolution for both columns.
        That numbering is library bookkeeping, the same argument §2 makes for
        `offset`, so it is pyiceberg's job and not the caller's.

        The cost of one schema type is that Arrow does not map onto Iceberg
        exactly: `string` is stored and returned as `large_string`, and types
        Iceberg would narrow silently are refused instead (see `_types`).

        `sort_by` is required on purpose. §7 measures it as a read-shape
        decision, not a tuning knob: it declares which predicates prune, only a
        LEADING column prunes, and changing it later rewrites every file — see
        `set_sort_by`. A capture workload usually wants `("event_ts", "key")`.

        `archive` is the remote warehouse prefix (e.g. `s3://bucket/prefix`).
        None means local-only: capture, seal, compaction, retention and reads
        all work with no network, forever (§11).
        """
        settings = config or LogConfig()
        validate(schema, sort_by, settings, archive)

        layout = Layout(Path(root), name)
        if layout.buffer_db.exists():
            msg = f"a log already exists at {layout.root}/{name} — use open()"
            raise FileExistsError(msg)

        layout.create()
        table = LogTable.create(layout, table_schema(schema), sort_by)
        buffer = Buffer(layout.buffer_db, schema)
        # Arrow is the interchange type at every edge — SQLite to Parquet,
        # Iceberg to Arrow — so the declared Arrow schema is what those edges
        # cast to. It is kept here because Iceberg cannot represent it: one
        # string type and one binary type, so `large_binary` would come back
        # `binary` and the declaration would be quietly overruled.
        buffer.set_meta(_SCHEMA_KEY, schema.serialize().to_pybytes().hex())
        buffer.set_meta(_CONFIG_KEY, settings.to_json())
        if archive is not None:
            buffer.set_meta(_ARCHIVE_KEY, archive)

        return cls(
            layout=layout,
            table=table,
            buffer=buffer,
            schema=schema,
            sort_by=sort_by,
            config=settings,
            archive=archive,
        )

    @classmethod
    def open(
        cls,
        root: PathLike[str] | str,
        name: str,
        *,
        read_only: bool = False,
    ) -> Self:
        """Open an existing log, and recover it.

        Takes none of the shape: the columns come from the Iceberg table, their
        declared Arrow types and the config and archive from the buffer's
        `meta` table (§2), and the sort order from the table's declared sort
        order (§4). Restating any of it here would invite a caller to state
        something the log does not agree with, and the log is the one that is
        right.

        `read_only=True` opens a second view of a log another process is
        writing. It runs no recovery and refuses every mutation, so it cannot
        disturb the single writer §1 assumes.
        """
        layout = Layout(Path(root), name)
        if not layout.catalog_db.exists():
            msg = f"no litelink log at {layout.root} — use new() to create one"
            raise FileNotFoundError(msg)

        table = LogTable.load(layout, readonly=read_only)
        # Structure from Iceberg, spelling from `meta`. The table is
        # authoritative for which columns exist; the stored Arrow schema only
        # records how their types were declared, and is checked against the
        # table rather than trusted blindly.
        from_table = application_schema(table.arrow_schema())
        buffer = Buffer(layout.buffer_db, from_table, readonly=read_only)
        schema = _declared_schema(buffer, from_table)
        buffer.adopt_schema(schema)

        encoded = buffer.get_meta(_CONFIG_KEY)
        log = cls(
            layout=layout,
            table=table,
            buffer=buffer,
            schema=schema,
            sort_by=table.sort_by(),
            config=LogConfig() if encoded is None else LogConfig.from_json(encoded),
            # `or None`: detaching stores an empty string rather than deleting
            # the row, and an empty archive is no archive. Without this it reads
            # back truthy enough to make maintain() refuse to run.
            archive=buffer.get_meta(_ARCHIVE_KEY) or None,
            readonly=read_only,
        )
        if not read_only:
            log.recover()

        return log

    # -- settings ----------------------------------------------------------

    def set_config(self, config: LogConfig) -> None:
        """Replace the operational policy (§12).

        Every knob here governs future work only — when to seal, how long to
        keep, what to compact — so this needs no rewrite. `sort_by` and the
        schema are not in here precisely because they do.
        """
        with self._lock:
            self._writable()
            validate(self._schema, self._sort_by, config, self._archive)
            self._buffer.set_meta(_CONFIG_KEY, config.to_json())
            self.config = config
            self._maintenance = Maintenance(
                self._table, self._buffer, self._layout, config, self._sort_by
            )

    def set_archive(self, archive: str | None) -> None:
        """Point the log at an archive, or detach it (§5)."""
        with self._lock:
            self._writable()
            validate(self._schema, self._sort_by, self.config, archive)
            self._buffer.set_meta(_ARCHIVE_KEY, archive or "")
            self._archive = archive or None

    def set_sort_by(self, sort_by: Sequence[str], *, rewrite: bool) -> None:
        """Change the sort order, rewriting every existing file.

        §7 calls `sort_by` a read-shape decision rather than a tuning knob:
        it declares which predicates prune, and the clustering that makes them
        prune is baked into each file when it is written. So a new order that
        is only declared would apply to future seals and silently leave every
        existing file clustered the old way — the same predicate fast on recent
        data and slow on older data, with nothing to indicate why.

        `rewrite` must be passed explicitly. It is the honest name for the
        cost: every data file is read, re-sorted and replaced.
        """
        with self._lock:
            self._writable()
            requested = tuple(sort_by)
            validate(self._schema, requested, self.config, self._archive)
            if requested == self._sort_by:
                return

            if not rewrite:
                msg = (
                    "changing sort_by re-clusters every existing file; "
                    "pass rewrite=True to accept that cost"
                )
                raise ValueError(msg)

            self._table.set_sort_order(requested)
            self._sort_by = requested
            self._maintenance = Maintenance(
                self._table, self._buffer, self._layout, self.config, requested
            )
            self._maintenance.rewrite_sorted()

    def _writable(self) -> None:
        if self.readonly:
            msg = "this Log was opened readonly"
            raise RuntimeError(msg)

    # -- write -------------------------------------------------------------

    def append(self, row: Row) -> int:
        """Append one row. Returns the assigned offset.

        Durable when this returns — one SQLite transaction, `synchronous=FULL`
        (§3). There is no in-memory write buffer to flush, and that absence is
        the point: it is the failure the README opens with.

        A caller-supplied `offset` is rejected (I11).
        """
        return self.extend([row])[0]

    def extend(self, rows: Iterable[Row]) -> list[int]:
        """Append many rows in ONE transaction. Returns the assigned offsets.

        The batch is the durability unit: one fsync amortised across the batch,
        which is the whole of §3's throughput story. It carries no meaning
        beyond that — see §1 on why there is no transaction id column.
        """
        with self._lock:
            self._writable()
            offsets = self._buffer.append(rows)
            self._maybe_seal()

            return offsets

    # -- read --------------------------------------------------------------

    def scan(
        self,
        *,
        columns: Sequence[str] | None = None,
        where: str | None = None,
        start_offset: int | None = None,
        end_offset: int | None = None,
        include_archive: bool = False,
    ) -> pa.RecordBatchReader:
        """Read the log as one relation, newest data included.

        Unions the tiers and bounds each by its neighbour's committed offset
        extent, resolved from manifest statistics at query time (§7, I3). The
        tiers overlap by design, so the bounds are what make each row appear
        exactly once.

        `include_archive=False` by default: a hot read is local disk only and
        must stay that way (I5). Opting in is opting into network I/O.

        Always bound on a LEADING column of `sort_by`. §7 measures a non-leading
        predicate at 119 ms against 13 ms for the same predicate with a leading
        bound — the column is in the sort key and still does not prune on its
        own.

        Returns a streaming reader rather than a table: a full-window read with
        a 400-byte payload column is 611 ms and proportional to the data, so
        materialising it is the caller's choice to make.
        """
        projection = ", ".join(f'"{c}"' for c in (columns or self._table_schema.names))
        predicates = [f"({where})"] if where else []
        if start_offset is not None:
            predicates.append(f'"offset" >= {int(start_offset)}')

        if end_offset is not None:
            predicates.append(f'"offset" < {int(end_offset)}')

        clause = f" WHERE {' AND '.join(predicates)}" if predicates else ""

        return self.sql(
            f'SELECT {projection} FROM log{clause} ORDER BY "offset"',
            include_archive=include_archive,
        )

    def sql(self, query: str, *, include_archive: bool = False) -> pa.RecordBatchReader:
        """Run arbitrary DuckDB SQL against the log, exposed as `log`.

        The escape hatch for what `scan` cannot express. Quote `"offset"` — it
        is a DuckDB reserved word.
        """
        if include_archive:
            msg = "archive reads are not implemented"
            raise NotImplementedError(msg)

        with self._lock:
            return self._reader.query(query)

    def end_offset(self) -> int:
        """The offset the next append will receive — an EXCLUSIVE upper bound.

        Half-open, matching the `[start, end)` seal ranges in §4, so
        `end_offset()` on a fresh log is the first offset it will ever assign
        and never a sentinel.

        This is the log's end, NOT §7's tier boundary `hi`, which is the local
        Iceberg table's committed maximum and excludes everything still in the
        buffer. A consumer resuming from here sees every durable row; one
        resuming from `hi` silently skips the unsealed tail.
        """
        with self._lock:
            return self._buffer.next_offset()

    def table_extent(self) -> tuple[int, int] | None:
        """`(lo, hi)` offset extent of the local table, from statistics.

        §7's tier boundary. Read from manifest column statistics — no data file
        is opened, which is what makes it ~0.6 ms rather than a scan.
        """
        with self._lock:
            self._table.reload()

            return self._table.extent()

    # -- seal --------------------------------------------------------------

    def seal(self) -> int | None:
        """Force a seal now; returns the exclusive end offset, or None if empty.

        Normally automatic at `min(target_size, max_age)` (§4). Explicit seals
        are for shutdown and tests.
        """
        with self._lock:
            self._writable()
            extent = self._buffer.extent()
            if extent is None:
                return None

            start, last = extent
            end = last + 1
            rel_path = self._layout.seal_path(start, end, datetime.now(UTC).date())
            # I2: the range and its path are fixed BEFORE the file exists, so a
            # retry recomputes nothing and overwrites in place rather than
            # stranding the first attempt under a different name.
            self._buffer.claim_seal(start, end, rel_path)
            self._write_and_commit(end, rel_path)
            self._buffer.finish_seal(end)

            return end

    def _write_and_commit(self, end: int, rel_path: str) -> None:
        """Write the Parquet file, fsync it, then commit it to the table.

        I1 in this order: committing first would publish a manifest entry for a
        file that may not survive the crash.
        """
        rows = self._buffer.rows_below(end)
        if self._sort_by:
            # §4: the sort order is declared as table metadata AND applied here.
            # Metadata records intent; it does not sort for you.
            rows = rows.sort_by([(c, "ascending") for c in self._sort_by])

        dest = self._layout.absolute(rel_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(rows, dest)
        fsync(dest)

        self._table.register(str(dest))

    def _maybe_seal(self) -> None:
        """Seal when the buffer crosses `target_size`.

        The `max_age` branch is not wired up: it needs a clock the caller does
        not drive, and §4 evaluates both triggers at commit time. Sealing on age
        therefore has to be driven by the application calling `seal()` until
        this grows a timer.

        Deliberately only the O(1) counter. This runs after every append, and
        asking SQLite for min/max to check emptiness first — which is what it
        used to do — put a query on the write path to learn something the
        counter already knew, at 13-62% of the raw SQLite floor.
        """
        if self._buffer.byte_size() >= self.config.target_size:
            self.seal()

    # -- recovery ----------------------------------------------------------

    def recover(self) -> None:
        """Finish whatever a crash interrupted (§4, §11).

        Idempotent in every direction, which is why it can simply run at open
        rather than being an operator's decision.
        """
        self._recover_compaction()
        self._recover_seal()

    def _recover_seal(self) -> None:
        """If the commit landed, only the buffer delete is outstanding; if it
        did not, the whole file is rewritten to the same path."""
        pending = self._buffer.pending_seal()
        if pending is None:
            return

        _, end, rel_path = pending
        if str(self._layout.absolute(rel_path)) not in self._table.file_paths():
            self._write_and_commit(end, rel_path)

        self._buffer.finish_seal(end)

    def _recover_compaction(self) -> None:
        """Resolve a compaction interrupted before its commit (§11).

        Unlike a seal, an interrupted compaction is not redone. Its inputs are
        still live — the transaction that would have superseded them never
        committed — so the table is already correct and the next `maintain()`
        will pick the same run up again. All that is owed is the half-written
        output, and `compacting` names it, so removing it costs one unlink
        rather than a directory scan.
        """
        pending = self._buffer.pending_compaction()
        if pending is None:
            return

        _, _, rel_path = pending
        if str(self._layout.absolute(rel_path)) not in self._table.file_paths():
            self._layout.absolute(rel_path).unlink(missing_ok=True)

        self._buffer.clear_compaction()

    # -- maintenance -------------------------------------------------------

    def sync(self) -> None:
        """Push to the archive: upload, register, replicate compactions (§5).

        Archive-facing work only. Lazy, restartable, and arbitrarily far
        behind — no read depends on it. Raises if no archive is configured; with
        `archive=None` there is nothing this could do.

        DEVIATES from §5, which also lists snapshot expiry (step 4) and local
        eviction (step 5). Both are local storage work and belong to `maintain`;
        leaving them here makes `local_retention` silently inert on a local-only
        log, because every step of §5 is archive work and the whole pass is
        skipped. Sync's remaining obligation to eviction is the registration
        watermark it records in `meta`, which is what lets `maintain` enforce I4.
        """
        raise NotImplementedError

    def maintain(self) -> None:
        """Reclaim local storage: compact, evict, expire (§6, §8, §12).

        Runs with or without an archive — this is the call that makes
        `local_retention` mean something on a local-only log.

        **Eviction is bounded by I4 when an archive is configured**: a file that
        sync has not yet registered is never evicted, however old. So on a
        partitioned-off machine, this compacts and expires but leaves the window
        growing — §11's "local eviction stalls".

        **With no archive, eviction is deletion.** I4 is vacuous because nothing
        is owed to an archive, so `local_retention` becomes an ordinary
        retention policy and data past it is gone for good. That is the contract
        a local-only log with a retention asks for; `None` keeps everything and
        grows without bound.

        Whether a stalled or partial pass should be reported rather than silent
        is open — §11 treats stalled eviction as an operational condition, and
        returning None says nothing about it.
        """
        with self._lock:
            self._writable()
            if self._archive is not None:
                # I4 needs sync's registration watermark to decide what is safe
                # to evict, and sync does not exist yet. Refusing beats evicting
                # a file no archive has — that failure is silent and permanent.
                msg = (
                    "maintain() with an archive configured requires sync(), "
                    "which is not implemented"
                )
                raise NotImplementedError(msg)

            self._maintenance.run()

    def hydrate(self, since: timedelta) -> None:
        """Re-register archived files into the local table (§8).

        Raising `local_retention` is an operation, not a config change: without
        this, a raised setting applies only to data captured afterwards.
        """
        raise NotImplementedError

    # -- schema evolution --------------------------------------------------

    def add_column(self, name: str, type_: pa.DataType) -> None:
        """Add a column. Non-breaking: older files read null (§9)."""
        raise NotImplementedError

    def rename_column(self, old: str, new: str, *, breaking_ok: bool) -> None:
        """Rename a column. Safe for the data, BREAKING for consumers (§9).

        Iceberg resolves by field ID, so no file is rewritten — and no engine's
        SQL is rewritten either, so `SELECT qty` breaks the moment the column
        becomes `quantity`. `breaking_ok` must be passed explicitly: the format
        will not stop you, so the API has to (I10).
        """
        raise NotImplementedError

    def drop_column(self, name: str, *, breaking_ok: bool) -> None:
        """Drop a column. Same contract as `rename_column` (§9, I10).

        Re-adding the name later creates a NEW field ID and cannot collide with
        the retired data.
        """
        raise NotImplementedError

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        """Release the buffer and reader handles. Does not seal.

        Not sealing is deliberate: committed rows are already durable, and an
        implicit seal on close would emit an undersized file every time a
        process restarted.
        """
        self._reader.close()
        self._buffer.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


def _declared_schema(buffer: Buffer, from_table: pa.Schema) -> pa.Schema:
    """The Arrow schema as declared, if `meta` still agrees with the table.

    Falls back to the table's own view when the record is missing (a log
    created before this was stored) or when the columns no longer match (a
    schema change reached Iceberg but not here). The table is the one that
    cannot be wrong about which columns exist, so it wins any disagreement.
    """
    encoded = buffer.get_meta(_SCHEMA_KEY)
    if encoded is None:
        return from_table

    declared = pa.ipc.read_schema(pa.BufferReader(bytes.fromhex(encoded)))

    return declared if declared.names == from_table.names else from_table


def application_schema(schema: pa.Schema) -> pa.Schema:
    """The caller's columns — the table's schema with `offset` removed."""
    return pa.schema([f for f in schema if f.name != "offset"])


def table_schema(schema: pa.Schema) -> pa.Schema:
    """The caller's columns with `offset` in front — the table's real schema."""
    return pa.schema([pa.field("offset", pa.int64(), nullable=False), *schema])


def validate(
    schema: pa.Schema,
    sort_by: Sequence[str],
    config: LogConfig,
    archive: str | None,
) -> None:
    """Reject configurations that cannot mean what they say.

    Separate from construction so the rules read as a list rather than as
    guards scattered through a constructor.
    """
    if "offset" in schema.names:
        msg = "`offset` is owned by the library and must not be in the schema (I11)"
        raise ValueError(msg)

    # Before anything else: a column the library cannot carry end-to-end must
    # fail here, not on the first read after the data is already durable.
    validate_schema(schema)

    missing = [c for c in sort_by if c not in schema.names]
    if missing:
        msg = f"sort_by names columns not in the schema: {missing}"
        raise ValueError(msg)

    if archive is None and config.local_retention == timedelta(0):
        msg = (
            "local_retention=0 means 'evict on upload' and presupposes an "
            "archive; with archive=None it would delete each file as it sealed"
        )
        raise ValueError(msg)
