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
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from litelink._buffer import Buffer
from litelink._fs import fsync
from litelink._layout import Layout
from litelink._lease import Lease, new_owner
from litelink._maintenance import Maintenance
from litelink._read import Reader, duckdb_connection
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
# The one column the library owns (§2). Named rather than spelled inline so a
# schema change has something to check against, and prefixed so it can never
# collide with a column §9 lets an application add.
OFFSET = "litelink_offset"

# The two operations whose ownership must survive the process holding it (§13.6).
# How often `await_seal` re-asks. Each round attempts a drain, and taking the
# lease is a write, so this is slower than a pure poll would need to be —
# 20 attempts a second while a caller is blocked, and none otherwise.
_AWAIT_POLL = 0.05

SEAL_ROLE = "seal"
MAINTAIN_ROLE = "maintain"

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
        reader: Reader,
        maintenance: Maintenance,
        schema: pa.Schema,
        sort_by: tuple[str, ...],
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
        self._sort_by = sort_by
        self._archive = archive
        self._reader = reader
        self._maintenance = maintenance
        # Sequences the only thing left that needs it: Log mutating several
        # objects at once, in `set_config`, `set_archive` and `set_sort_by`,
        # where a SQLite row and a Python object have to change together.
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
        # The last failure from the sealing thread, which has no caller to
        # raise to. Surfaced by `close`, so a process that never checks still
        # finds out.

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
        order = tuple(sort_by)
        settings = config or LogConfig()
        validate(schema, order, settings, archive)

        layout = Layout(Path(root), name)
        if layout.buffer_db.exists():
            msg = f"a log already exists at {layout.root}/{name} — use open()"
            raise FileExistsError(msg)

        layout.create()
        table = LogTable.create(layout, table_schema(schema), order)
        buffer = Buffer.open(layout.buffer_db, schema, target_size=settings.target_size)
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
            reader=Reader(
                layout, table, buffer, table_schema(schema), duckdb_connection
            ),
            maintenance=Maintenance(table, buffer, layout, settings, order),
            schema=schema,
            sort_by=order,
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
        schema = _declared_schema(layout, application_schema(table.arrow_schema()))

        # Read through a throwaway connection, like the schema above it,
        # because the buffer needs `target_size` before it can size the groups
        # it cuts and the value lives in the buffer's own database. A wart:
        # policy is stored inside the thing that consumes it.
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
        buffer = Buffer.open(
            layout.buffer_db,
            schema,
            target_size=config.target_size,
            readonly=read_only,
        )
        sort_by = table.sort_by()
        log = cls(
            layout=layout,
            table=table,
            buffer=buffer,
            reader=Reader(
                layout, table, buffer, table_schema(schema), duckdb_connection
            ),
            maintenance=Maintenance(table, buffer, layout, config, sort_by),
            schema=schema,
            sort_by=sort_by,
            config=config,
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
            self._maintenance.set_config(config)
            # The buffer too, or `target_size` changes everywhere except where
            # the cut is actually made and the log quietly keeps sizing files
            # to the value it was opened with.
            self._buffer.set_target_size(config.target_size)

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

            # Under the maintain lease, because a rewrite IS a compaction —
            # same claim record, same deterministic output path, same commit.
            # Without it this reached that path beside a running `maintain()`
            # in another process: two writers to one `compaction_path`, and a
            # single-row `compacting` intent each would clear from under the
            # other, leaving a half-written file nothing could name.
            lease = self._lease(MAINTAIN_ROLE)
            if not lease.acquire():
                msg = "another owner holds the maintenance lease"
                raise RuntimeError(msg)

            try:
                self._table.set_sort_order(requested)
                self._sort_by = requested
                self._maintenance.set_sort_by(requested)
                self._maintenance.rewrite_sorted(heartbeat=lease.renew)
            finally:
                lease.release()

    def _lease(self, role: str) -> Lease:
        """A fresh claim on `role` for this attempt.

        Minted per call rather than held as a field. A field would fix one owner
        for the whole Log, and two threads sharing it would then re-enter each
        other's lease — leaving the role excluding nothing inside a process.
        """
        return self._buffer.lease(role, new_owner())

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
        self._writable()

        # No lock. `Buffer` serialises its own connection, which is the only
        # thing two appending threads share — and the append decides nothing
        # beyond the cut it records (see `seal_group`). It does not measure,
        # compare, signal, or start anything: a maintainer calls `seal_due` and
        # finds the work waiting, exactly as it calls `maintain` and finds
        # files to compact.
        return self._buffer.append(rows)

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
        projection = ", ".join(
            f'"{c}"' for c in (columns or ("litelink_offset", *self._schema.names))
        )
        predicates = [f"({where})"] if where else []
        if start_offset is not None:
            predicates.append(f'"litelink_offset" >= {int(start_offset)}')

        if end_offset is not None:
            predicates.append(f'"litelink_offset" < {int(end_offset)}')

        clause = f" WHERE {' AND '.join(predicates)}" if predicates else ""

        return self.sql(
            f'SELECT {projection} FROM log{clause} ORDER BY "litelink_offset"',
            include_archive=include_archive,
        )

    def sql(self, query: str, *, include_archive: bool = False) -> pa.RecordBatchReader:
        """Run arbitrary DuckDB SQL against the log, exposed as `log`.

        The escape hatch for what `scan` cannot express. Quote `"litelink_offset"` — it
        is a DuckDB reserved word.
        """
        if include_archive:
            msg = "archive reads are not implemented"
            raise NotImplementedError(msg)

        # No lock here. `Reader` guards its own connection and `LogTable` its
        # own cache, both briefly — whereas this used to be the SAME lock a
        # whole maintenance pass held, so a read waited out a compaction.
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
        return self._buffer.next_offset()

    def buffered_rows(self) -> int:
        """Rows durable in the buffer but not yet sealed.

        The unsealed tail, which is what §7 measures as the variable cost of a
        read — so it is the number to watch when tuning the seal threshold.

        Counted in SQLite against the tier boundary rather than through the
        union: a `count(*)` over the log would make DuckDB evaluate the same
        predicate against the Iceberg leg too, reading the offset column out of
        every Parquet file to establish that none of them qualify.
        """
        extent = self.table_extent()

        # Deliberately unlocked, and therefore approximate: a seal landing
        # between the two reads shifts the boundary under the count. This is a
        # number to watch, not one to derive anything from, and locking it
        # would put a metric on the path of every append.
        return self._buffer.count_above(0 if extent is None else extent[1])

    def table_rows(self) -> int:
        """Rows in the local Iceberg table.

        From the manifests, which track a count per file, so this costs nothing
        beyond the snapshot read the boundary already needs.
        """
        self._table.reload()

        return self._table.record_count()

    def table_files(self) -> int:
        """Data files in the local table — what compaction is bringing down."""
        self._table.reload()

        return self._table.file_count()

    def table_extent(self) -> tuple[int, int] | None:
        """`(lo, hi)` offset extent of the local table, from statistics.

        §7's tier boundary. Read from manifest column statistics — no data file
        is opened, which is what makes it ~0.6 ms rather than a scan.
        """
        self._table.reload()

        return self._table.extent()

    # -- seal --------------------------------------------------------------

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
        self._writable()
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
        self._writable()
        # Outside `_lock`, because the lease excludes other OWNERS and the lock
        # sequences this process's own buffer writes — different jobs. Taking
        # it inside put two more fsyncs under the lock an append needs, and at
        # `synchronous=FULL` those are the expensive part of a small seal.
        #
        # One mechanism for both cases: owners are unique per attempt, so the
        # row that refuses a sealer in another process refuses one in another
        # thread on the same terms — and it lapses if this attempt dies
        # mid-seal, so another may finish what `sealing` records.
        lease = self._lease(SEAL_ROLE)
        if not lease.acquire():
            return None

        # No lock here either: the lease is the exclusion, and it already
        # refuses every other owner in this process and any other. A lock would
        # be a second answer to a question already settled.
        #
        # The range comes from the queue, not from whatever the buffer happens
        # to hold now. That is the difference between a file of `target_size`
        # and a file of however much arrived while the sealer was getting here
        # — and it costs one indexed row read instead of the SCAN that asking
        # the buffer for its extent used to.
        group = self._buffer.pending_group()
        if group is None:
            lease.release()
            return None

        start, end = group
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
            # `target_size` file; a write that outlasts 30 s means the machine
            # is in trouble, and stopping is the right answer then too.
            if not lease.renew():
                msg = "lost the seal lease before writing"
                raise RuntimeError(msg)

            self._write_and_commit(end, rel_path, lease)
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
        work no survivor was going to do. A readonly log has no such option and
        can only wait.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if self._buffer.pending_group() is None and (
                self._buffer.pending_seal() is None
            ):
                return True

            if not self.readonly:
                self._seal_queued()

            if deadline is not None and time.monotonic() >= deadline:
                return False

            time.sleep(_AWAIT_POLL)

    def _write_and_commit(
        self, end: int, rel_path: str, lease: Lease | None = None
    ) -> None:
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
            msg = "lost the seal lease before committing"
            raise RuntimeError(msg)

        # `end` passed so the commit can decline if the range is already in
        # the table. The lease check above is the fence; this is what makes a
        # failure of that fence harmless rather than a duplicate.
        self._table.register(str(dest), sealed_through=end)

    def seal_due(self) -> int | None:
        """Seal everything the policy says is ready. Returns the last end, or None.

        The maintainer's frequent call, and the counterpart to `maintain`: both
        are plain methods the caller runs on its own schedule, because the
        library has no business owning a thread or an interval. This one is
        cheap when there is nothing to do — an indexed read of one row — so it
        can be run often; `maintain` reads table metadata and wants to be run
        rarely. That difference is the only reason they are two methods.

        "Due" is §4's two triggers. `target_size` needs nothing here: the cut
        was recorded by the append that crossed it, and this drains it. Age is
        the other, and it has to be evaluated by whoever is sealing, because a
        stream quiet enough to need it is by definition not appending.

        A group whose lease is held elsewhere is left alone, not waited for.
        """
        self._writable()
        # Before draining, not after: a quiet stream has no closed group at
        # all, and this is the only thing that gives it one.
        self._buffer.close_open_group(
            int((datetime.now(UTC) - self.config.max_age).timestamp())
        )

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

    def _recover_seal(self, lease: Lease | None = None) -> None:
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
        self._write_and_commit(end, retry, lease)
        self._buffer.finish_seal(end, retry)

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

        # Reloaded for the same reason `_recover_seal` is, and the consequence
        # here is worse: this UNLINKS. A handle that predates another process's
        # commit reports the output missing, and removing a file the table now
        # references loses the rows outright — the sources it superseded are
        # already out of the snapshot and queued for deletion.
        self._table.reload()

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
        lease = self._lease(MAINTAIN_ROLE)
        if not lease.acquire():
            msg = "another owner holds the maintenance lease"
            raise RuntimeError(msg)

        try:
            # Renewed between passes, not merely held. A compaction can run for
            # tens of seconds against a 30 s lease, and a second maintainer
            # taking the role mid-pass would compact the same runs to the same
            # deterministic path — a torn file, not a conflict Iceberg can
            # resolve. Losing it is a hard error rather than something to plough
            # on through.
            self._maintenance.run(heartbeat=lease.renew)
        finally:
            lease.release()

        # Sealing IS maintenance — it is the first thing done with what the
        # writer leaves behind. Called here so that a caller running only this
        # in a loop is correct; `seal_due` is exposed separately only because
        # it is cheap enough to run far more often than the rest of this.
        self.seal_due()

    def hydrate(self, since: timedelta) -> None:
        """Re-register archived files into the local table (§8).

        Raising `local_retention` is an operation, not a config change: without
        this, a raised setting applies only to data captured afterwards.
        """
        raise NotImplementedError

    # -- schema evolution --------------------------------------------------
    #
    # Each of these writes two records: the Iceberg schema, and the declared
    # Arrow spelling in `meta`. An Iceberg catalog commit and a SQLite write are
    # separate transactions and cannot be made atomic with each other — the same
    # reason §7 gives for not requiring an atomic handoff between two catalogs —
    # so a crash can land between them.
    #
    # **The change is complete when the Arrow schema lands in SQLite, not when
    # the Iceberg commit does.** That is the same completion boundary every
    # other multi-step operation here uses: a seal completes at its final SQLite
    # transaction (§4 step 3), a compaction at `clear_compaction`, a deletion at
    # `forget_deletion`. Iceberg holds the data; SQLite holds the record of what
    # this library has finished doing.
    #
    # So these follow §4's shape rather than relying on `open`'s fallback:
    #
    #   1. record the intended Arrow schema in SQLite, before anything changes
    #   2. commit the schema update to Iceberg
    #   3. write the Arrow schema to `meta` and clear the intent
    #
    # Recovery replays it. An intent whose Iceberg commit already landed
    # finishes at step 3; one whose commit did not is redone or abandoned, and
    # the columns in the table say which. The fallback in `open` is then what it
    # should be — an upgrade path for logs written before the Arrow schema was
    # stored — rather than the crash handler, which would silently drop the
    # declared spelling of the column just added.

    def add_column(self, name: str, type_: pa.DataType) -> None:
        """Add a column. Non-breaking: older files read null (§9).

        Must reject `litelink_offset` (I11), which `validate` enforces at
        creation and this has to enforce again: a schema change is the other
        way a caller could introduce it, and the prefix makes the collision
        unlikely rather than impossible.
        """
        reject_reserved(name)
        raise NotImplementedError

    def rename_column(self, old: str, new: str, *, breaking_ok: bool) -> None:
        """Rename a column. Safe for the data, BREAKING for consumers (§9).

        Renaming TO `litelink_offset` is refused for the same reason as adding
        it, and renaming it away would retire the column §7 derives every tier
        boundary from.

        Iceberg resolves by field ID, so no file is rewritten — and no engine's
        SQL is rewritten either, so `SELECT qty` breaks the moment the column
        becomes `quantity`. `breaking_ok` must be passed explicitly: the format
        will not stop you, so the API has to (I10).
        """
        reject_reserved(new)
        reject_reserved(old)
        raise NotImplementedError

    def drop_column(self, name: str, *, breaking_ok: bool) -> None:
        """Drop a column. Same contract as `rename_column` (§9, I10).

        Re-adding the name later creates a NEW field ID and cannot collide with
        the retired data.
        """
        reject_reserved(name)
        raise NotImplementedError

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        """Release handles. Does not seal, and has nothing to stop.

        Not sealing is deliberate: committed rows are already durable, and an
        implicit seal on close would emit an undersized file every time a
        process restarted. Groups already queued are left queued — they are
        durable, correctly sized, and whoever opens the log next will find
        them, which is the point of writing the cut down rather than deriving
        it.

        Nothing to stop because nothing was started. Sealing happens when a
        caller asks for it, in the caller's own loop, so there is no thread
        here whose lifetime has to be managed and no error from one to
        re-raise at a place the caller never asked about.
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


def _declared_schema(layout: Layout, from_table: pa.Schema) -> pa.Schema:
    """The Arrow schema as declared, checked against the table's columns.

    Both records must exist and agree. There is no fall back to the table's own
    view: under I16 a schema change records its intent before acting and is
    replayed on recovery, so a log whose two records disagree has not been
    interrupted — it has been corrupted, or written to behind the library's
    back. Continuing with a guess would serve reads under a schema the data
    does not have.
    """
    encoded = Buffer.peek_meta(layout.buffer_db, _SCHEMA_KEY)
    if encoded is None:
        msg = (
            f"log at {layout.root}/{layout.name} has no stored Arrow schema; "
            "its buffer database is missing or corrupt"
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


def application_schema(schema: pa.Schema) -> pa.Schema:
    """The caller's columns — the table's schema with `offset` removed."""
    return pa.schema([f for f in schema if f.name != "litelink_offset"])


def table_schema(schema: pa.Schema) -> pa.Schema:
    """The caller's columns with `offset` in front — the table's real schema."""
    return pa.schema([pa.field("litelink_offset", pa.int64(), nullable=False), *schema])


def reject_reserved(name: str) -> None:
    """Refuse any operation that would touch the library's own column (I11).

    Checked at schema-change time as well as at creation: those are the two
    ways a caller could reach it, and monotonicity and non-reuse cannot be
    enforced on a column an application controls.
    """
    if name == OFFSET:
        msg = f"`{OFFSET}` is owned by the library and cannot be added, renamed or dropped (I11)"
        raise ValueError(msg)


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
    if OFFSET in schema.names:
        msg = f"`{OFFSET}` is owned by the library and must not be in the schema (I11)"
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
