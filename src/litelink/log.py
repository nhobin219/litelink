"""The core append-only stream.

A `Log` is one stream: one SQLite buffer, one local Iceberg table, and
optionally one archive table (SPEC §1). Rows are durable at commit and
queryable immediately; `offset` is assigned by the library and is the only
column it owns (§2, I11).

Core library only. The blob-field extension (§15) is deliberately absent —
applications that need a small binary column declare an ordinary `binary`
column in their own schema, which §15.2 already says is the right route for
payloads that fit comfortably in the buffer.

Schema evolution is not implemented; the signatures are the design, and they
raise.
"""

from __future__ import annotations

import contextlib
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

from litelink._archive import ARCHIVE_KEY, Archive
from litelink._buffer import Buffer
from litelink._claim import EVERYTHING, Claim, new_owner
from litelink._fs import fsync
from litelink._layout import Layout
from litelink._maintenance import (
    CONFIG_KEY,
    Maintenance,
    checkpoint,
    stable_prefix,
)
from litelink._read import Reader, duckdb_connection
from litelink._replication import litestream_config
from litelink._s3 import S3Options
from litelink._table import LogTable
from litelink._types import validate_schema

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence
    from os import PathLike
    from types import TracebackType
    from typing import Self

    from litelink._table import DataFile

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


# One home, in `_maintenance`, because eviction reads it too.
_CONFIG_KEY = CONFIG_KEY
# One home, in `_archive`, because `evict` reads it too.
_ARCHIVE_KEY = ARCHIVE_KEY
_SCHEMA_KEY = "arrow_schema"


# How much larger a compacted file is than a sealed one, when nothing says.
#
# The two are at odds by nature: §7 wants the seal small because the buffer is
# what a hot read scans, and both object storage and Parquet want files large.
# Eight is chosen to be comfortably past the point where per-file overhead
# dominates — measured at 44 ms to read the offset boundary over 64 files
# against 1.0 ms over one — while keeping compaction's peak memory, which is
# one run held as a single Arrow table, to something a maintainer process can
# hold. On the default 8 MiB seal that is 64 MiB.
COMPACT_MULTIPLE = 8


@dataclass(frozen=True, slots=True)
class LogConfig:
    """SPEC §12.

    The defaults are the spec's worked examples, not measured optima — §7's
    numbers come from a 2 vCPU box and every one of these wants re-measuring on
    target hardware.
    """

    # The ONLY seal trigger, and therefore the size of every file this library
    # writes. §7 makes it a READ-LATENCY knob before a file-size one: the
    # buffer is the entire variable cost of a hot read.
    #
    # There is deliberately no `max_age` beside it. A timer sealing a quiet
    # stream emits a small file every interval for ever — the layout §6 exists
    # to repair — and it made the knob do double duty as an RPO policy, so
    # shrinking the window to lose less on a crash produced worse files. §3a
    # names that trade and WAL replication is what breaks it: freshness in the
    # cloud is replication's job, not the seal's. With the timer gone every cut
    # lands exactly here, so no undersized file is ever written.
    #
    # BYTES, not rows. §13.3 is the deciding argument: a row-count bound can
    # exceed a byte-based memory limit, so it loses the race to the OOM killer
    # in exactly the situation the bound exists to prevent.
    #
    # UNCOMPRESSED bytes, in memory — not the size of the file that results.
    # Deliberate, and the one thing to understand before setting it. A file
    # holding 8 MiB of rows lands at 8 MiB on disk if they are incompressible
    # and under 1 MiB if they repeat, so on-disk size is an OUTPUT here, never
    # the target. Sizing by it instead would be sizing by the compression
    # ratio: rows per file would swing with the data, and what a reader pays to
    # hold a file — which is the uncompressed size, whatever the file cost to
    # store — would be unbounded. This way it is bounded by construction, and
    # bounded per file is what lets a scan bound its total: N files open at
    # once cost N times this, which is the number to divide a memory budget by
    # when choosing read parallelism.
    #
    # So expect files smaller than this on disk, and set it larger than an
    # on-disk file target would be. Everything downstream is stated in the same
    # currency — compaction sizes a merge by what its inputs HOLD, carried in
    # the `extent` row the seal measured it into, never by what they compressed
    # to (see `_maintenance.runs`).
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
    # may eventually need `min(target_size, target_rows)` —
    # deliberately not added now, on one knob until a real workload demands the
    # second.
    target_seal_size: int = 8 * 1024 * 1024
    # The second half of §7's argument, and the one `target_size` cannot make.
    # Buffer cost is per ROW — SQLite is row-oriented, 1.0 us/row at 20k and
    # 2.3 us/row at 180k — so a stream of 40-byte rows reaches 8 MiB at 200k
    # rows and breaches a read-latency ceiling meant to hold at 20k, tenfold,
    # while every byte-based check reports the buffer is fine.
    #
    # Bytes bound memory; rows bound read latency. Both are CEILINGS on one
    # file, so the seal cuts at whichever is reached FIRST — the mirror of
    # `local_retention` and `local_rows`, which are floors and take whichever
    # retains more.
    #
    # None means no row limit, which is the right default: a narrow-row stream
    # is the case that needs this and a library cannot guess the row width.
    target_seal_rows: int | None = None

    # §6. How big a file should END UP, which is not the same question as how
    # much may sit in the buffer, and the two pull in opposite directions.
    #
    # §7 wants the seal SMALL: the buffer is what a hot read scans, so its size
    # is read latency. The archive wants files LARGE: measured against S3, a
    # 9 kB file takes 648 ms to upload and almost all of that is the round
    # trip, so halving file size doubles the cost of archiving the same stream.
    # One knob cannot serve both — it did, and compaction could therefore never
    # produce a file bigger than a seal, which is why it was a no-op.
    #
    # Splitting them gives compaction a job: converting sealed chunks into
    # archive-shaped ones. Eligibility follows for free — `sync` only takes
    # files compaction has finished with, so raising this above the seal size
    # means a freshly sealed file is a merge candidate and is not archived
    # until it has been converted.
    #
    # The price is write amplification, and it is bounded rather than ongoing:
    # every row is written twice locally, once at seal and once at compaction,
    # and read once in between. Bounded because a converted file is already at
    # the target, so it is never a candidate again — eight seals become one
    # compacted file, once.
    #
    # None means `COMPACT_MULTIPLE` times the seal size, and the conversion is
    # therefore ON by default even with no archive. A local-only log gets the
    # same benefit at read time: file count is a measured cost here, not a
    # reputation — reading the offset boundary from manifest statistics
    # measured 1.0 ms over one file and 44 ms over 64.
    #
    # It is a MULTIPLE for a reason. Sealed files are uniform, so merging whole
    # files lands exactly on the target when it divides and short when it does
    # not: three 1 MiB files against a 4 MiB target give 3 MiB files for ever,
    # 25% under what was asked for, with nothing to indicate why.
    target_compact_size: int | None = None
    target_compact_rows: int | None = None

    @property
    def compact_size(self) -> int:
        """The file size compaction aims for.

        `COMPACT_MULTIPLE` times the seal size unless set. On the 8 MiB default
        seal that is 64 MiB of rows — under Parquet's usual advice once
        compression is applied, and far above the size at which per-file
        overhead dominates a scan.
        """
        return self.target_compact_size or self.target_seal_size * COMPACT_MULTIPLE

    @property
    def compact_rows(self) -> int | None:
        """The row ceiling compaction respects. Scaled like `compact_size`, so
        setting only the seal's row limit does not silently cap conversion at
        one seal's worth."""
        if self.target_compact_rows is not None:
            return self.target_compact_rows

        if self.target_seal_rows is None:
            return None

        return self.target_seal_rows * COMPACT_MULTIPLE

    # §8. Must exceed the longest hot-path lookback WITH margin.
    #
    # None keeps everything locally and grows without bound. Zero means "evict
    # on upload" — pure archival capture, hot reads limited to the buffer — and
    # presupposes an archive: with `archive=None` it would delete each file as
    # soon as it sealed, so the pair is rejected at construction rather than
    # honoured.
    local_retention: timedelta | None = None
    # §8, the other half of the same policy. A window in time and a count of
    # rows bound different things, and which one binds depends on a rate the
    # library cannot know: an hour of a quiet stream is a handful of rows, and
    # an hour of a busy one is more local disk than the machine has. Set both
    # and eviction keeps whichever retains MORE — they are floors on what must
    # stay readable without touching the network, so the binding one is the one
    # that keeps more.
    #
    # The mirror image of the seal's `min(target_size, target_rows)` in §12,
    # deliberately: there the two are ceilings and the tighter wins, here they
    # are floors and the looser does.
    #
    # Rows, not files, because it is a statement about the data — "the last
    # million entries stay local" survives a change to `target_size`, and "the
    # last ten files" does not.
    #
    # FOLLOW-UP: no third floor in BYTES, and deliberately. These two answer
    # "what can I query without touching the network", which is how queries are
    # written — the last hour, the last million rows. "The last 10 GB" is not a
    # statement any query makes, and rows already stand in for bytes since
    # bytes are roughly rows times width.
    #
    # What is genuinely missing is the opposite: nothing bounds local disk from
    # ABOVE, so with both of these unset it grows without limit. That wants a
    # CAP rather than a floor, and it composes the other way — `max` after the
    # `min` below, because a cap evicts MORE and can therefore violate both
    # floors. It would measure on-disk size (`DataFile.size`), one of the few
    # places where that is the right unit. And it could not be honoured at all
    # while sync is behind, since I4 forbids evicting what the archive lacks —
    # a log breaching its floors to stay under a cap is misconfigured and
    # should say so rather than quietly serving every read from object storage.
    local_rows: int | None = None

    # §3a. Continuous WAL shipping, which is the ONLY thing bounding RPO now
    # that the seal has no timer: a stream that goes quiet holds its last
    # partial file's worth of rows indefinitely.
    #
    # A declaration rather than a supervisor. It is read — `write_replication_
    # config` needs it, and validation refuses it without an archive to
    # replicate to — but litelink never starts the sidecar. That is a separate
    # process reading the WAL, which is exactly why replication does not put
    # the network in the write path, and litestream is explicit that two
    # instances must never replicate one database. Supervising it belongs in
    # deployment code, where it is visible: see `examples/maintainer.py`.
    wal_replication: bool = False
    # §6/§8. Must exceed the longest scan: expiry deletes files an open scan is
    # still reading (I6).
    snapshot_retention: timedelta = timedelta(hours=1)

    # §6. What counts as "big enough to leave alone" is `settled_size` of the
    # target, not its own setting — see `_maintenance.settled_size`.
    compact_min_files: int = 4

    def to_json(self) -> str:
        """Serialised for the `meta` table, so `open` recovers the policy.

        Durations as seconds rather than any richer encoding: this is read by
        the next process to open the log, and a float is the one representation
        that cannot drift between library versions.
        """
        return json.dumps(
            {
                "target_seal_size": self.target_seal_size,
                "target_compact_size": self.target_compact_size,
                "target_seal_rows": self.target_seal_rows,
                "target_compact_rows": self.target_compact_rows,
                "local_retention": (
                    None
                    if self.local_retention is None
                    else self.local_retention.total_seconds()
                ),
                "local_rows": self.local_rows,
                "wal_replication": self.wal_replication,
                "snapshot_retention": self.snapshot_retention.total_seconds(),
                "compact_min_files": self.compact_min_files,
            }
        )

    @classmethod
    def from_json(cls, encoded: str) -> LogConfig:
        """Recover the policy, tolerating a record written by another version.

        Every field falls back to its default when absent, because that is what
        an older record MEANS: the log was written before the setting existed,
        so it was running the default. Reading them positionally instead made
        adding any setting break `open` on every existing log — a config that
        cannot be read is a log that cannot be opened, over a policy value that
        was never load-bearing.

        Unknown keys are ignored for the same reason from the other direction:
        a log touched by a newer version stays openable by an older one, minus
        the setting it does not have.
        """
        raw = json.loads(encoded)
        defaults = cls()
        retention = raw.get("local_retention", defaults.local_retention)
        snapshots = raw.get("snapshot_retention")

        return cls(
            target_seal_size=raw.get("target_seal_size", defaults.target_seal_size),
            target_compact_size=raw.get(
                "target_compact_size", defaults.target_compact_size
            ),
            target_seal_rows=raw.get("target_seal_rows", defaults.target_seal_rows),
            target_compact_rows=raw.get(
                "target_compact_rows", defaults.target_compact_rows
            ),
            local_retention=(
                retention
                if isinstance(retention, timedelta) or retention is None
                else timedelta(seconds=retention)
            ),
            local_rows=raw.get("local_rows", defaults.local_rows),
            wal_replication=raw.get("wal_replication", defaults.wal_replication),
            snapshot_retention=(
                defaults.snapshot_retention
                if snapshots is None
                else timedelta(seconds=snapshots)
            ),
            compact_min_files=raw.get("compact_min_files", defaults.compact_min_files),
        )


def _repointed_mid_push() -> RuntimeError:
    """The log was pointed at another archive while a sync was pushing.

    A push can outlive its lease — a register alone measured 4.1 s against S3,
    and retries compound it — so the re-point that races it took the lease
    lawfully. Nothing here is corrupt; the watermark this push earned simply
    describes an archive the log has left, and recording it would tell eviction
    (I4) that the new archive holds rows it has never been sent.
    """
    return RuntimeError(
        "the archive was re-pointed while this sync was pushing; its watermark "
        "belongs to the previous archive and is not recorded"
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
        archive: Archive,
        readonly: bool = False,
    ) -> None:
        self.name = layout.name
        self.root = layout.root
        self.readonly = readonly

        self._layout = layout
        self._table = table
        self._buffer = buffer
        self._schema = schema
        self._sort_by = sort_by
        # The same object the reader and the maintainer were handed. See
        # `_archive.Archive`: it owns the URI, the credentials and the lazily
        # opened handle, so `set_archive` re-points all three at once instead
        # of fanning out to each. Required rather than defaulted, because a
        # collaborator this constructor builds for itself is one no caller can
        # substitute — the reason `new` and `open` build every other one.
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
        sort_by: Sequence[str] | None = None,
        config: LogConfig | None = None,
        archive: str | None = None,
        s3: S3Options | None = None,
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

        `archive` is the remote warehouse prefix (e.g. `s3://bucket/prefix`).
        None means local-only: capture, seal, compaction, retention and reads
        all work with no network, forever (§11).
        """
        # `None` and `()` mean the same thing — offset order — so nothing
        # downstream has to distinguish "unset" from "explicitly unsorted".
        order = tuple(sort_by or ())
        settings = config or LogConfig()
        validate(schema, order, settings, archive)

        layout = Layout(Path(root), name)
        if layout.buffer_db.exists():
            msg = f"a log already exists at {layout.root}/{name} — use open()"
            raise FileExistsError(msg)

        layout.create()
        table = LogTable.create(layout, table_schema(schema), order)
        buffer = Buffer.open(
            layout.buffer_db,
            schema,
            target_size=settings.target_seal_size,
            target_rows=settings.target_seal_rows,
        )
        # Arrow is the interchange type at every edge — SQLite to Parquet,
        # Iceberg to Arrow — so the declared Arrow schema is what those edges
        # cast to. It is kept here because Iceberg cannot represent it: one
        # string type and one binary type, so `large_binary` would come back
        # `binary` and the declaration would be quietly overruled.
        buffer.set_meta(_SCHEMA_KEY, schema.serialize().to_pybytes().hex())
        # The pair in ONE transaction. `validate` has just accepted these two
        # together, and written separately a crash between them records the
        # policy without the archive it presupposes — evict-on-upload with
        # nothing to evict into, which `open` never re-checks and the first
        # maintenance pass carries out.
        buffer.set_meta_all(
            {
                _CONFIG_KEY: settings.to_json(),
                **({_ARCHIVE_KEY: archive} if archive is not None else {}),
            }
        )

        # Built here and handed to all three, so each is given its archive at
        # construction rather than having one pushed into it afterwards.
        remote = Archive(layout, archive, s3, table_schema(schema))

        return cls(
            layout=layout,
            table=table,
            buffer=buffer,
            reader=Reader(
                layout,
                table,
                buffer,
                table_schema(schema),
                duckdb_connection,
                archive=remote,
            ),
            maintenance=Maintenance(table, buffer, layout, settings, order, remote),
            schema=schema,
            sort_by=order,
            config=settings,
            archive=remote,
        )

    @classmethod
    def open(
        cls,
        root: PathLike[str] | str,
        name: str,
        *,
        read_only: bool = False,
        s3: S3Options | None = None,
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
            target_size=config.target_seal_size,
            target_rows=config.target_seal_rows,
            readonly=read_only,
        )
        sort_by = table.sort_by()
        # `or None`: detaching stores an empty string rather than deleting the
        # row, and an empty archive is no archive. Read once here because both
        # the Log and its maintainer need it.
        archive = buffer.get_meta(_ARCHIVE_KEY) or None
        remote = Archive(layout, archive, s3, table_schema(schema))
        log = cls(
            layout=layout,
            table=table,
            buffer=buffer,
            reader=Reader(
                layout,
                table,
                buffer,
                table_schema(schema),
                duckdb_connection,
                archive=remote,
            ),
            maintenance=Maintenance(table, buffer, layout, config, sort_by, remote),
            schema=schema,
            sort_by=sort_by,
            config=config,
            archive=remote,
            readonly=read_only,
        )
        if not read_only:
            log.recover()

        return log

    # -- settings ----------------------------------------------------------

    @property
    def config(self) -> LogConfig:
        """The policy in force, owned by `Maintenance` (§12).

        A property rather than a field, because two copies of a policy that
        another process can change is a way for compaction and `sync` to
        disagree about which files are still in play.
        """
        return self._maintenance.config

    def set_config(self, config: LogConfig) -> None:
        """Replace the operational policy (§12).

        Every knob here governs future work only — when to seal, how long to
        keep, what to compact — so this needs no rewrite. `sort_by` and the
        schema are not in here precisely because they do.
        """
        with self._lock:
            self._writable()
            # The same claim `set_archive` takes, and for the same reason.
            # `validate` refuses a PAIR — an evict-on-upload policy with no
            # archive to evict into — so the two halves have to be decided
            # together. Reading the other half durably is not enough on its
            # own: read and write as two transactions and the check is only a
            # statement about the past, so the two setters could each pass
            # against a state the other was about to change and assemble the
            # refused pair between them. The next maintenance pass then
            # executes it faithfully and deletes the only copy of everything
            # sealed. Verified by execution before this claim existed.
            lease = self._lease(MAINTAIN_ROLE)
            if not lease.acquire():
                msg = (
                    "another owner holds a claim over this range; the archive "
                    "may be being re-pointed. Retry."
                )
                raise RuntimeError(msg)

            try:
                validate(
                    self._schema,
                    self._sort_by,
                    config,
                    self._buffer.get_meta(_ARCHIVE_KEY) or None,
                )
                self._buffer.set_meta(_CONFIG_KEY, config.to_json())
                # One owner. `Maintenance` holds the policy and fans it out to
                # the buffer's seal target; a second copy here was kept in step
                # by this method alone, which is exactly the arrangement that
                # breaks the moment anything else can change the policy.
                self._maintenance.set_config(config)
            finally:
                lease.release()

    def archived_through(self) -> int:
        """Highest offset the archive holds, 0 if none (§5, I4).

        Eviction never goes above it, so the lag between this and `end_offset`
        is what an operator watches: a stalled sync shows up first as local
        disk that stops being reclaimed.
        """
        return self._maintenance.archived_through()

    @property
    def databases(self) -> tuple[Path, ...]:
        """The SQLite files a restore needs (§3a).

        For a WAL-shipping sidecar to replicate. Public because deciding to run
        one is a deployment choice, but knowing WHICH files carry the log's
        state is not — it is this library's, and a sidecar configured by hand
        against a guess is one that silently omits `archive.db` and leaves the
        objects in S3 with nothing to say what they are.

        litelink does not run the sidecar. It is a separate process reading the
        WAL, which is exactly why it does not put the network in the write path
        (§3a); a library that supervised it would.
        """
        return self._layout.databases

    def archive_files(self) -> int:
        """How many data files the archive holds, or 0 with no archive.

        A network round trip, unlike `table_files()`: it reloads the remote
        table to answer. Paired with `table_files()` it is the two halves of
        where the data is — local disk and object storage — which is otherwise
        only visible as a watermark in offsets.
        """
        archive = self._archive.table()
        if archive is None:
            return 0

        archive.reload()

        return len(archive.data_files())

    def replication_config(self) -> str:
        """A litestream config for this log (§3a).

        Derived, not configured: the file set is `databases`, the destination
        is `_wal` beside the archived data, and the endpoint comes from the
        credentials this log was opened with. Everything a hand-written config
        would have to restate, and each of those a way to get it silently
        wrong.
        """
        if self._archive.uri is None:
            msg = "replication needs an archive; this log is local-only"
            raise ValueError(msg)

        return litestream_config(
            self.databases, self._layout.root, self._archive.uri, self._archive.s3
        )

    def write_replication_config(self) -> Path:
        """Write that config next to the log, and return where.

        Beside the data rather than at a configured path, for the same reason
        every other path is derived: a setting for it would be one more thing
        to keep in step with the log it describes.
        """
        destination = self._layout.root / "litestream.yml"
        destination.write_text(self.replication_config())

        return destination

    @property
    def archive(self) -> str | None:
        """Where the archive is, or None for a local-only log.

        The question a maintenance loop has to answer before calling `sync`,
        and one only the log can: the archive is recovered from `meta` on
        `open`, so a caller that restated it could disagree with the log.
        """
        return self._archive.uri

    def set_archive(self, archive: str | None) -> None:
        """Point the log at an archive, or detach it (§5).

        Takes the maintenance lease, so it cannot interleave with a `sync` —
        and raises if another owner holds it, rather than proceeding.

        A sync that has already read `s3://old`, taken the lease and pushed to
        it finishes by writing that archive's extent as the watermark. Land a
        re-point in between and the log points at the new, empty archive while
        carrying a watermark earned by the old one — eviction believes it and
        deletes the only local copy of rows the new archive has never been
        sent. Nothing lowers a watermark, so no later sync undoes it.

        The shipped writer calls this on every restart, and the maintainer runs
        in another process, so the two are exactly the pair that collide.
        """
        with self._lock:
            self._writable()
            lease = self._lease(MAINTAIN_ROLE)
            if not lease.acquire():
                msg = (
                    "another owner holds a claim over this range; a sync may be "
                    "pushing to the current archive. Retry."
                )
                raise RuntimeError(msg)

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
                self._maintenance.refresh_config()
                validate(self._schema, self._sort_by, self.config, archive)
                self._repoint(archive)
            finally:
                lease.release()

    def _repoint(self, archive: str | None) -> None:
        """Record the new location, with the maintenance lease held."""
        # Normalised before anything compares it. Every path builder strips
        # trailing slashes, so `s3://b/p` and `s3://b/p/` are the same archive
        # everywhere except here — where the difference would read as a move
        # and reset the watermarks of an archive that genuinely holds data.
        normalised = (archive or "").rstrip("/") or None
        # ONE transaction, because the three facts are only true together.
        #
        # Where the archive is, and the two watermarks describing what the
        # PREVIOUS one held: the confirmed one eviction acts on, and the
        # frontier compaction reads to decide which files are already the
        # archive's business. As separate writes a crash lands between them,
        # and BOTH orders have cost a defect — watermark last leaves the new
        # archive carrying the old one's promise, which eviction believes;
        # watermark first leaves the old archive with a frontier of zero, and
        # compaction does not wait for a sync the way eviction does, so it
        # merges across a boundary the archive already holds. There is no
        # ordering that is safe, so there is no ordering.
        # Whether this is a move is decided against the DURABLE location, in
        # the transaction that acts on the decision. This object's memory of
        # where the archive is goes stale the moment another process re-points
        # it, and nothing but a sync refreshes it — so a maintainer re-asserting
        # the archive it already has would read its own staleness as a move and
        # zero the watermarks of a bucket that holds the data.
        self._buffer.set_meta_moved(
            _ARCHIVE_KEY,
            normalised or "",
            {Maintenance.ARCHIVED_KEY: "0"},
        )

        # Reaches the maintainer and the reader because all three hold this
        # object. `evict` asks it whether I4 is owed anything, and a setting
        # that stopped at `Log` would leave the maintainer deleting the only
        # copy of rows an archive was just configured to receive.
        self._archive.set_uri(normalised)

        # Repaired here as well as at open, and the difference is who waits.
        # The catalog entry still names the previous archive until something
        # replaces it, and only a lease holder may — so without this, ordinary
        # re-pointing left every `include_archive` read raising until a
        # maintenance pass happened to run.
        #
        # Best effort: the archive may not be reachable at all, and
        # configuring one is a statement of intent, not a claim that the bucket
        # exists yet. `sync` raises loudly the moment the location is used, and
        # the check at open heals what this misses.
        if self._archive.configured():
            with contextlib.suppress(Exception):
                self._archive.table(repair=True)

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
            validate(self._schema, requested, self.config, self._archive.uri)
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
                msg = "another owner holds a claim over this range"
                raise RuntimeError(msg)

            try:
                self._table.set_sort_order(requested)
                self._sort_by = requested
                self._maintenance.set_sort_by(requested)
                self._maintenance.rewrite_sorted(
                    heartbeat=lease.renew, owner=lease.owner
                )
            finally:
                lease.release()

    def _lease(self, role: str, lo: int = 0, hi: int = EVERYTHING) -> Claim:
        """A fresh claim on `[lo, hi]` for this attempt.

        Minted per call rather than held as a field. A field would fix one owner
        for the whole Log, and two threads sharing it would then re-enter each
        other's claim — leaving it excluding nothing inside a process.

        The default range is the whole log, which is what a configuration change
        needs: re-pointing an archive or rewriting its files is not an operation
        on an offset interval, so it excludes every pass rather than commuting
        with any of them.
        """
        return self._buffer.claim(role, lo, hi, new_owner())

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
        # beyond the cut it records (see `extent`). It does not measure,
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
        # No lock here. `Reader` guards its own connection and `LogTable` its
        # own cache, both briefly — whereas this used to be the SAME lock a
        # whole maintenance pass held, so a read waited out a compaction.
        return self._reader.query(query, include_archive=include_archive)

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
        # The range comes from the queue, not from whatever the buffer happens
        # to hold now. That is the difference between a file of `target_size`
        # and a file of however much arrived while the sealer was getting here
        # — and it costs one indexed row read instead of the SCAN that asking
        # the buffer for its extent used to.
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
        #
        # One mechanism for both cases: owners are unique per attempt, so the
        # row that refuses a sealer in another process refuses one in another
        # thread on the same terms — and it lapses if this attempt dies
        # mid-seal, so another may finish what `sealing` records.
        lease = self._buffer.claim(SEAL_ROLE, start, end - 1, new_owner())
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
                msg = "lost the claim on this seal range before writing"
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
        self, end: int, rel_path: str, lease: Claim | None = None
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
            msg = "lost the claim on this seal range before committing"
            raise RuntimeError(msg)

        # `end` passed so the commit can decline if the range is already in
        # the table. The lease check above is the fence; this is what makes a
        # failure of that fence harmless rather than a duplicate.
        if not self._table.register(
            [str(dest)],
            sealed_through=end,
            # The archive too. An empty local table covers nothing, so after a
            # stalled writer's range has been sealed, archived and evicted, the
            # local check alone would let it re-register a file the log has
            # already moved past.
            archived_through=self._maintenance.archived_through(),
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
        """Seal everything the policy says is ready. Returns the last end, or None.

        The maintainer's frequent call, and the counterpart to `maintain`: both
        are plain methods the caller runs on its own schedule, because the
        library has no business owning a thread or an interval. This one is
        cheap when there is nothing to do — an indexed read of one row — so it
        can be run often; `maintain` reads table metadata and wants to be run
        rarely. That difference is the only reason they are two methods.

        "Due" means cut. `target_size` is the only trigger and it needs nothing
        here: the cut was recorded by the append that crossed it, and this
        writes the file. There is no age branch, so a quiet stream is simply
        one whose rows stay in the buffer — durable, readable, and replicated
        by §3a — until enough of them arrive to fill a file.

        A group whose lease is held elsewhere is left alone, not waited for.
        """
        self._writable()

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
        rather than a directory scan — or, for an archive rewrite, one DELETE
        rather than a paginated LIST over object storage.
        """
        pending = self._buffer.pending_compaction()
        if pending is None:
            return

        # No reload. This used to decide from `file_paths()` and unlink, so it
        # needed the freshest possible view — and still raced. It queues now,
        # and the decision that matters is `drain`'s, which reloads at the
        # moment it removes.

        # EVERY claim, not the first. An archive rewrite writes one file per
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
        # `pending_delete`, whose drain re-reads `referenced_paths` at unlink
        # time and refuses anything the table has since adopted. Recovery now
        # uses the same route, so the last word belongs to a check made when
        # the file is actually removed.
        claimed = self._buffer.pending_outputs()
        self._maintenance.enqueue_recovered(key for _, _, key in claimed)

        # The scratch database an interrupted rewrite left behind. It is
        # rebuilt from the archive next time, so nothing in it is owed.
        self._layout.rewrite_db.unlink(missing_ok=True)
        # Only the rows just read. A rewrite whose lease lapsed mid-upload can
        # be claiming its next segment while this runs, and clearing the table
        # wholesale takes that claim with it — leaving an object in the bucket
        # named by nothing, which is the state claims exist to prevent.
        for _, _, key in claimed:
            self._buffer.clear_compaction(key)

    # -- maintenance -------------------------------------------------------

    def sync(self) -> None:
        """Push to the archive: upload, register, record the watermark (§5).

        Archive-facing work only. Lazy, restartable, and arbitrarily far
        behind — no read depends on it. Raises if no archive is configured;
        with `archive=None` there is nothing this could do.

        **Only files at or above the compaction threshold are pushed**, and
        that one rule does three jobs. The archive never receives an undersized
        file, so nothing ever has to merge one back out of object storage —
        which would mean paying egress to fix a sizing decision made locally.
        Such a file is also never a compaction input (`compact` only builds
        runs from files BELOW the threshold), so nothing merges across what the
        archive already holds, and no push can duplicate or strand a range.
        And the undersized frontier stays local, bounded by roughly
        `compact_min_files` files, until compaction grows it past the line.

        There is therefore at most one undersized region in the system and it
        is always the local one. The archive is well-sized by construction
        rather than by a pass that repairs it afterwards.

        DEVIATES from §5, which also lists snapshot expiry (step 4) and local
        eviction (step 5). Both are local storage work and belong to `maintain`;
        leaving them here makes `local_retention` silently inert on a local-only
        log, because every step of §5 is archive work and the whole pass is
        skipped. Sync's remaining obligation to eviction is the registration
        watermark it records in `meta`, which is what lets `maintain` enforce I4.
        """
        self._writable()
        if not self._archive.configured():
            msg = "sync() needs an archive; this log is local-only"
            raise ValueError(msg)

        # Re-read where the archive IS before pushing to it. `set_archive` is
        # a durable change made by whichever process runs it, and every other
        # process cached the old value when it opened — so a maintainer started
        # before a re-point would go on pushing to the retired archive and,
        # worse, reconcile the watermark from ITS extent. Eviction trusts that
        # watermark and deletes local files the new archive has never been
        # sent: the rows survive only in the bucket the re-point was retiring.
        #
        # One keyed read per sync, against a change that is rare and durable.
        # `set_uri` is a no-op when nothing moved.
        lease = self._lease(MAINTAIN_ROLE)
        if not lease.acquire():
            msg = "another owner holds a claim over this range"
            raise RuntimeError(msg)

        try:
            # UNDER the lease, not before it. Read first, the location can be
            # re-pointed between the read and the acquire — `set_archive` takes
            # the same lease, so it is free until this line — and the push then
            # runs against the old archive while the log durably points at the
            # new one. `_push` would reconcile the old archive's extent into
            # the watermark with no network call at all, and eviction believes
            # a watermark whatever earned it.
            self._archive.set_uri(self._buffer.get_meta(_ARCHIVE_KEY) or None)
            if not self._archive.configured():
                msg = "sync() needs an archive; this log is local-only"
                raise ValueError(msg)

            # PINNED here, and every fence downstream compares against this
            # string rather than re-reading the object. `Archive` is shared by
            # the log, the reader and the maintainer precisely so a re-point
            # reaches all three — which means a `set_archive` on another thread
            # moves the value a fence was going to compare AGAINST, and both
            # sides of the comparison change together. The fence passes, and
            # the watermark this push earned is recorded against an archive
            # that never received it.
            self._push(lease, self._archive.uri)
        finally:
            lease.release()

    def _push(self, lease: Claim, pinned: str | None) -> None:
        """Upload and register everything above the archive's extent.

        Everything compaction has finished with, which `stable_prefix` decides
        from compaction's own rule rather than from a size of its own. A file
        pushed and then merged locally would leave the archive holding rows
        that have been rewritten underneath it, so the two must agree on which
        files are still in play, and the only way to guarantee that is to ask
        the same function.

        The archive may still gain a small file: one stranded between larger
        neighbours can never be merged, so holding it back would block the
        watermark forever rather than improve anything. That is a cosmetic cost
        with a deliberate cause, and `rewrite_archive` is the tool for it.
        """
        # Read under the claim, the same as everything else that decides what
        # this pass does. The grouping `stable_prefix` computes has to match
        # the one compaction computes — `runs` is shared so they cannot
        # disagree — and in the shipped topology they are separate processes,
        # so agreement means both reading the policy the log records rather
        # than the one each happened to open with.
        self._maintenance.refresh_config()

        archive = self._archive.require()
        self._table.reload()
        # The ARCHIVE reloaded too, and for a stronger reason than the local
        # table. A pyiceberg handle is a frozen snapshot view, and `Archive`
        # caches it for the life of the process — so a second maintainer that
        # synced, released the lease and took it back reads the extent as it
        # was before the OTHER maintainer's register. Every other reader of
        # this extent already reloads; this is the one place that turns it into
        # a durable watermark, and a stale answer here retires the frontier
        # against an archive that has since grown past it.
        archive.reload()

        covered = archive.extent()
        floor = 0 if covered is None else covered[1]
        # The watermark reconciled against the archive itself. It is a cache of
        # what the archive holds — kept for the push floor and for display, and
        # no longer for anything that authorises a deletion — so a commit that
        # landed while the `meta` write after it did not would leave it behind
        # for ever: the next pass computes `floor` from the archive, finds
        # nothing left to push, and never revisits it.
        #
        # Compared and written in one transaction, not read and then written.
        # Reconciling against an archive the log has been pointed away from is
        # the loss this path is here to prevent, and a guard that reads first
        # only reports where the archive was.
        confirmed = max(self._maintenance.archived_through(), floor)
        self._buffer.set_meta_if(
            _ARCHIVE_KEY, pinned, {Maintenance.ARCHIVED_KEY: str(confirmed)}
        )

        memory = self._maintenance.memory()

        # BACKFILL, and it is what makes I4-per-segment recoverable. The row
        # naming a file's archive copy is written after the register, so a
        # crash between the two leaves the archive holding a range that nothing
        # local records — and compaction, which now decides from those rows,
        # would merge it into a file spanning the archive's extent. The next
        # push would register a partial overlap, which `register` admits.
        #
        # The archive's own manifest is the truth, so recover from it rather
        # than promising anything beforehand. Reading it costs nothing extra:
        # `extent()` above already walked it.
        # Bounded by the local window, not by the archive. Every decision the
        # rows feed — what compaction may merge, what eviction may drop — is
        # about files the local table still holds, so an archive file entirely
        # below them changes no answer. Unbounded, this read and this loop grew
        # with the archive and ran on every single sync.
        local = self._table.data_files()
        base = min((f.lo for f in local), default=0)
        recorded = set(self._buffer.archived_ranges(pinned or "", base))
        for held in archive.data_files():
            if held.hi >= base and (held.lo, held.hi + 1) not in recorded:
                self._buffer.record_file(
                    held.path,
                    held.lo,
                    held.hi + 1,
                    memory.get(held.path, self.config.compact_size),
                )

        pending = [f for f in self._table.data_files() if f.hi > floor]
        settled = stable_prefix(
            pending,
            self.config.compact_size,
            self.config.compact_min_files,
            memory,
            self.config.compact_rows,
        )

        uploaded: list[tuple[DataFile, str]] = []
        for data_file in pending[:settled]:
            checkpoint(lease.renew)
            rel_path = self._layout.relative(data_file.path)
            archive.put(self._layout.absolute(rel_path), rel_path)
            uploaded.append((data_file, rel_path))

        if not uploaded:
            return

        # ONE commit for everything uploaded, because the commit is what costs.
        # Measured against S3: 648 ms to upload a file and 4.1 s to register
        # it, and registering does not get cheaper for holding one file instead
        # of twenty — it reads a footer, writes a manifest, a manifest list and
        # a fresh metadata.json whatever the count. Per file, that made `sync`
        # take 83 s over sixteen files while the sealer sharing its thread
        # waited, and the buffer grew for the whole of it.
        checkpoint(lease.renew)
        last = uploaded[-1][0]
        # Recorded BEFORE the register, and never read by eviction. It is what
        # lets compaction know, after a crash between the register and its
        # confirming write, that these offsets may already be in the archive
        # and must not be merged into a file that straddles its extent.
        if (self._buffer.get_meta(_ARCHIVE_KEY) or None) != pinned:
            # The upload already spent longer than a lease TTL against S3 more
            # than once, which is how the log gets re-pointed underneath a push
            # — and registering into the archive it was pointed AWAY from
            # writes the log's rows somewhere nothing will look for them again.
            raise _repointed_mid_push()

        if not archive.register(
            [archive.uri(rel_path) for _, rel_path in uploaded],
            sealed_through=last.hi + 1,
            # The low end too, so the archive can refuse a range that starts
            # inside what it already holds. Everything upstream is arranged so
            # that cannot happen; this is the check that holds regardless of
            # whether the arrangement has a gap.
            lo=uploaded[0][0].lo,
        ):
            return

        for data_file, rel_path in uploaded:
            # The archive's copy holds what the local one did, and this is the
            # only moment both names are known. Nothing could re-derive it
            # afterwards: the local entry goes when the local file is unlinked,
            # and a Parquet footer records what the rows compressed from, not
            # what the appender counted them as.
            held = memory.get(data_file.path)
            if held is not None:
                # Same extent, second location. `end_offset` is exclusive, as
                # it is on every other extent — the cut is recorded as the
                # offset AFTER the last row.
                self._buffer.record_file(
                    archive.uri(rel_path), data_file.lo, data_file.hi + 1, held
                )

        # After the register, never before: the watermark is a promise that the
        # archive HAS the range, and I4 lets `maintain` delete the local copy on
        # the strength of it.
        #
        # And only if it is still a promise about the SAME archive. This push
        # can outlive its lease — a register alone measured 4.1 s and retries
        # compound it — and a re-point that takes the lease meanwhile leaves
        # this about to record an extent earned by a bucket the log has left.
        # Re-read rather than trusted, because nothing lowers a watermark
        # afterwards.
        # Re-read rather than trusted, and in the same transaction as the
        # write: nothing lowers a watermark afterwards, so recording one earned
        # by a bucket the log has left is not a mistake anything corrects.
        if not self._buffer.set_meta_if(
            _ARCHIVE_KEY, pinned, {Maintenance.ARCHIVED_KEY: str(last.hi)}
        ):
            raise _repointed_mid_push()

    def maintain(self) -> None:
        """Reclaim local storage: compact, evict, expire (§6, §8, §12).

        The one call most deployments want, and it takes the lease once for
        all three. `compact`, `evict` and `expire` are callable on their own
        for the case this cannot express — schedules that differ because the
        costs do, now that conversion reads and rewrites files while the other
        two are metadata commits.

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

        # Sealing IS maintenance — it is the first thing done with what the
        # writer leaves behind. Called here so that a caller running only this
        # in a loop is correct; `seal_due` is exposed separately only because
        # it is cheap enough to run far more often than the rest of this.
        self.seal_due()

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
        """Drop files past `local_retention` from the local table (§8).

        Never past what the archive holds when one is configured (I4), so a
        sync that is behind delays this rather than losing data.
        """
        self._pass(lambda _: self._maintenance.evict(), heartbeat)

    def expire(self, heartbeat: Callable[[], bool] | None = None) -> None:
        """Expire snapshots past `snapshot_retention`, then delete what has
        come due (§6, §8)."""
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
        self._writable()
        # No claim here. Each pass claims the range it actually works on —
        # compaction a run, eviction the prefix it removes — so two maintainers
        # exclude each other only where their work overlaps, which is what §4a
        # buys over one lease per role. An entry-point claim would put that
        # back and cover every offset in the log while doing so.
        run(heartbeat)

    def rewrite_archive(self) -> None:
        """Merge undersized files already in the archive (§6, ad-hoc).

        An operation, not a policy. Nothing calls it on a schedule and normal
        operation does not need it: `sync` pushes only files compaction has
        finished with, so the archive is well-sized by construction. It exists
        for the two things that break that on purpose — an explicit `seal()`
        stranding a small file, and a change to `target_size`, which applies to
        the future while the archive is immutable history.

        Run it when nothing else is maintaining the log: it takes the same
        lease as `maintain` and `sync`, and it rewrites the same files they
        would.
        """
        self._writable()
        if not self._archive.configured():
            msg = "rewrite_archive() needs an archive; this log is local-only"
            raise ValueError(msg)

        lease = self._lease(MAINTAIN_ROLE)
        if not lease.acquire():
            msg = "another owner holds a claim over this range"
            raise RuntimeError(msg)

        try:
            self._maintenance.rewrite_archive(lease.renew, lease.owner)
        finally:
            lease.release()

    def hydrate(self, since: timedelta) -> None:
        """Re-register archived files into the local table (§8).

        Raising `local_retention` is an operation, not a config change: without
        this, a raised setting applies only to data captured afterwards.

        `since` is measured against when the ARCHIVE took each file, which is
        the only age still on record — the local snapshots that once dated them
        went with the eviction, and the library stamps no timestamp of its own
        (§2). So this reads "bring back what was archived in the last week",
        not "what was captured then"; for a stream that fell behind, those
        differ.

        Files are copied down and registered under the name they have
        remotely, so hydrating twice writes the same paths rather than
        accumulating copies, and one interrupted halfway is finished by the
        next run. Only ranges strictly below what the local table already holds
        are considered, which is what keeps files contiguous and
        non-overlapping (§4) — the archive's copy of a range the local table
        still has would otherwise be added a second time and every row in it
        read twice.

        A hydrated file is not measured: nothing local counted its rows, and
        its extent carries no size. That is deliberate and it is what
        an unknown size means everywhere else — the file counts as full, so
        compaction will not merge it and `sync` will not push it back to the
        archive it just came from. Eviction still applies to it, which is the
        point: this is temporary unless `local_retention` is raised too.
        """
        self._writable()
        if not self._archive.configured():
            msg = "hydrate() needs an archive; this log is local-only"
            raise ValueError(msg)

        # The maintenance lease, because this writes the local table and copies
        # files into the data directory — the same two things eviction and
        # compaction do, and for the same reason they must not overlap.
        lease = self._lease(MAINTAIN_ROLE)
        if not lease.acquire():
            msg = "another owner holds a claim over this range"
            raise RuntimeError(msg)

        try:
            self._pull(lease, since)
        finally:
            lease.release()

    def _pull(self, lease: Claim, since: timedelta) -> None:
        """Copy down and register everything archived since `since`."""
        archive = self._archive.require()
        self._table.reload()

        covered = self._table.extent()
        # Nothing local means nothing to sit below, so everything qualifies.
        floor = covered[0] if covered is not None else None
        cutoff = datetime.now(UTC) - since
        added = archive.snapshot_ages()
        held = self._maintenance.memory()

        eligible = [
            data_file
            for data_file in archive.data_files()
            if (floor is None or data_file.hi < floor)
            and (stamped := added.get(data_file.path)) is not None
            and stamped.replace(tzinfo=UTC) >= cutoff
        ]

        # DOWNWARD from the local floor, and only across an unbroken join.
        #
        # Registering upward left a hole no later run could fill. The first
        # file restored becomes the new lowest local range, so a failure before
        # the next one — a lost lease, a network error, or a `since` window
        # that selected a non-contiguous set because `rewrite_archive` re-dated
        # a middle file — leaves the gap ABOVE what was restored. The next run
        # takes its floor from that new lower bound, finds the gap no longer
        # below it, and skips it for ever. `_union` bounds the archive leg by
        # the local floor, so those offsets are then served by neither tier:
        # rows silently missing from every query.
        #
        # Downward, every step keeps the local range contiguous, so an
        # interruption is just a range that starts higher than intended and the
        # next run continues from there. Stopping at a gap rather than stepping
        # over it is the same rule: what cannot be joined onto cannot be
        # restored without creating one.
        for data_file in sorted(eligible, key=lambda f: f.hi, reverse=True):
            if floor is not None and data_file.hi != floor - 1:
                break

            checkpoint(lease.renew)
            rel_path = archive.key(data_file.path)
            destination = self._layout.absolute(rel_path)
            archive.fetch(data_file.path, destination)
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
            # for a whole `local_retention`, while a re-run of `hydrate` skips
            # the range because the local floor now covers it.
            checkpoint(lease.renew)
            # No `sealed_through`: that check exists to decline a range the
            # table already covers, and every range here is deliberately below
            # what it covers. The filter above is what prevents an overlap.
            self._table.register([str(destination)])
            # An extent for the local copy, carrying what the archive's copy
            # holds. Restored data is subject to `local_retention` like
            # anything else, and eviction dates a file by this record — so a
            # hydrated file without one would sit undateable, treated as newly
            # written, and never leave again.
            # A file the archive never measured counts as FULL, matching what
            # this promises above. Zero was the opposite, and it was dormant
            # only while a watermark kept hydrated files out of every size
            # consumer — removing the watermark is what wakes it: a file
            # recorded at zero bytes is a permanent compaction candidate that
            # merging can never make big enough to stop being one.
            self._buffer.record_file(
                rel_path,
                data_file.lo,
                data_file.hi + 1,
                held.get(data_file.path, self.config.compact_size),
            )
            floor = data_file.lo

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

    if config.snapshot_retention < timedelta(0):
        # The same sign slip, one field over. Expiry computes
        # `now - snapshot_retention`, so a negative one puts the cutoff in the
        # future: every superseded file is unlinked in the pass that supersedes
        # it, and I6's whole promise — the grace must exceed the longest scan —
        # is not merely shortened but inverted. Zero is allowed deliberately;
        # it means "no grace", which tests and demos ask for on purpose.
        msg = f"snapshot_retention must not be negative: {config.snapshot_retention}"
        raise ValueError(msg)

    if config.local_retention is not None and config.local_retention < timedelta(0):
        # The same check its twin above has always had, and the reason it
        # matters more here: eviction computes `now - local_retention`, so a
        # negative one puts the cutoff in the FUTURE and every file in the log
        # is stale. On a local-only log that is silent deletion of the only
        # copy of everything, at every negative value, from one sign slip.
        msg = f"local_retention must not be negative: {config.local_retention}"
        raise ValueError(msg)

    # Both floors, and how eviction actually combines them. It takes the LOWER
    # boundary — the policy that retains MORE wins (§12) — so a config is only
    # "evict on upload" when EVERY floor it states is one. Stating one such
    # floor beside a generous one is safe, and the previous version of this
    # rule refused it with a message that was false for that pair.
    floors = [
        config.local_retention is not None and config.local_retention <= timedelta(0),
        config.local_rows is not None and config.local_rows == 0,
    ]
    stated = [
        config.local_retention is not None,
        config.local_rows is not None,
    ]
    if (
        archive is None
        and any(stated)
        and all(drops for drops, given in zip(floors, stated, strict=True) if given)
    ):
        msg = (
            "local_retention=0 and local_rows=0 both mean 'evict on upload' "
            "and presuppose an archive; with archive=None this config would "
            "delete each file as it sealed"
        )
        raise ValueError(msg)

    if config.wal_replication and archive is None:
        msg = (
            "wal_replication needs an archive: WAL segments go beside the "
            "archived data, and a local-only log has nowhere to ship them"
        )
        raise ValueError(msg)

    if config.compact_min_files < 2:
        msg = (
            f"compact_min_files must be at least 2: {config.compact_min_files}. "
            "It is how many files a run needs before compaction will merge it, "
            "and a run always holds at least one — so at one, every run looks "
            "mergeable, nothing is ever settled, and `stable_prefix` returns "
            "zero for ever: sync pushes nothing, the watermark stands still, "
            "eviction pins on it and the local table grows without bound, while "
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

    if config.local_rows is not None and config.local_rows < 0:
        msg = f"local_rows must not be negative: {config.local_rows}"
        raise ValueError(msg)
