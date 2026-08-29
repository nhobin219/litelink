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
import random
import tempfile
import threading
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.exceptions import TableAlreadyExistsError

from litelink._archive import ARCHIVE_KEY, Archive
from litelink._buffer import (
    INTENT_KEY,
    SCHEMA_KEY,
    SORT_KEY,
    START_OFFSET_KEY,
    Buffer,
)
from litelink._claim import EVERYTHING, Claim, new_owner
from litelink._config import LogConfig
from litelink._fs import fsync
from litelink._layout import Layout
from litelink._maintenance import (
    CONFIG_KEY,
    Maintenance,
    checkpoint,
    stable_prefix,
)
from litelink._read import Reader, duckdb_connection
from litelink._replication import litestream_config, restore_buffer
from litelink._s3 import S3Options
from litelink._table import (
    LogTable,
    archive_columns,
    archive_extent,
    forget_archive_entry,
)
from litelink._types import column_type, validate_schema

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence
    from os import PathLike
    from types import TracebackType
    from typing import Self

    from litelink._table import DataFile

# What `append` and `extend` take, and the one alias in a public signature.
#
# Defined at RUNTIME rather than under `TYPE_CHECKING`, so it can be exported:
# a caller annotating against those signatures has to name it, and a name that
# exists only to a type checker cannot be imported. `Mapping` comes from
# `collections.abc`, so the runtime import costs a stdlib module already loaded.
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

# How long a configuration change waits for maintenance to finish before it
# gives up. Long enough to cover an ordinary merge, short enough that a wedged
# log is reported rather than hung.
_SETTINGS_WAIT_S = 10.0

SEAL_ROLE = "seal"
MAINTAIN_ROLE = "maintain"


# One home, in `_maintenance`, because eviction reads it too.
_CONFIG_KEY = CONFIG_KEY
# One home, in `_archive`, because `evict` reads it too.
_ARCHIVE_KEY = ARCHIVE_KEY
# One home, in `_buffer`, because the buffer is what reads it per call — it
# owns `meta`, and `shape()` is where every holder of the schema now gets it.
_SCHEMA_KEY = SCHEMA_KEY
# One home, in `_buffer`, beside the other durable facts about a log's shape.
_START_OFFSET_KEY = START_OFFSET_KEY
# §4's declared clustering, kept here as well as on the table.
#
# Not a duplicate of the Iceberg sort order — a fact the local CATALOG carries
# and nothing else does. `catalog.db` is replicated but cannot be restored onto
# another machine (it records absolute paths to local metadata no sidecar
# ships), so a failover rebuilds the local table rather than restoring it, and
# has to be told what order to declare. `open_archive` never declared one
# either, so the archive could not answer it.
#
# One home, in `_buffer`, because the seal, compaction and the archive all read
# it too — see `Buffer.sort_by`.
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


@dataclass(frozen=True)
class _Recovery:
    """What a restore recovered and what it skipped, for the caller to report."""

    recovered: int
    resumed_at: int
    skipped: tuple[int, int]


def _foreign_archive(archive: str) -> ValueError:
    """This log has no record of pushing to an archive that already holds data.

    Which makes it another log's. Two logs of the same name both start at
    offset 1, so the ranges cannot tell them apart — what can is that a log
    which pushed to an archive keeps its `extent` rows naming that prefix, even
    across a detach (§4a). No rows, data present: not ours.
    """
    return ValueError(
        f"the archive at {archive!r} holds data this log has no record of pushing, "
        f"so it belongs to another log. Attaching it would let its contents be read "
        f"as this log's own, push nothing, and pin eviction — silently. To resume "
        f"that log here use Log.restore; to start a new one, point at an unused "
        f"prefix"
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


def scan_query(
    declared: Sequence[str],
    *,
    columns: Sequence[str] | None = None,
    where: str | None = None,
    start_offset: int | None = None,
    end_offset: int | None = None,
) -> str:
    """The SQL behind `scan`, built from the declared columns alone.

    A free function because two different readers ask for it and only one of
    them is a `Log`. When `Follower` wrapped a whole `Log` it got this by
    calling `Log.scan`, which meant its read went out through `Log.sql` — and
    that is exactly how it lost the swept-snapshot translation, since a wrapper
    can intercept the call it makes but not the dispatch inside it.

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


class Log:
    """One append-only stream.

    Single writer per log (§1): SQLite's write lock is per file, and one process
    per stream is the intended topology.

    Threads within that process are fine, and scheduling `maintain()` on a
    background thread is the expected shape — every public method takes one
    lock. A second *process* is not: maintenance commits to the catalog and
    writes the buffer database, so it is a writer, and two of those is the case
    §1 excludes.

    Construct through `litelink.new`, `open` or `restore`; `litelink.reader`
    gives a second view alongside a live writer. The initialiser takes already
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
        config: LogConfig,
        archive: Archive,
    ) -> None:
        self.name = layout.name
        self.root = layout.root

        self._layout = layout
        self._table = table
        self._buffer = buffer
        # The same object the reader and the maintainer were handed. See
        # `_archive.Archive`: it owns the credentials and the lazily opened
        # handle, and reads the URI from `meta` on every access rather than
        # holding a copy — so `set_archive` moves what all three see by
        # changing one row, instead of fanning out to each. Required rather than defaulted, because a
        # collaborator this constructor builds for itself is one no caller can
        # substitute — the reason `new` and `open` build every other one.
        self._archive = archive
        # Set only by `restore`, and read only by `recovery()`. What a failover
        # recovered and what it skipped are facts about one operation, knowable
        # at the moment it runs and not afterwards — the skipped range leaves no
        # trace once the sequence has moved, and the recovered count is
        # indistinguishable from ordinary buffered rows a second later.
        self._restored_from: _Recovery | None = None
        self._reader = reader
        # `Log = writer state + LogReader`. The reader is built from the same
        # collaborators, not given a second set, so there is one answer to
        # every read question rather than two that can drift.
        self._view = LogReader(
            layout=layout,
            table=table,
            buffer=buffer,
            archive=archive,
            reader=reader,
        )
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
        start_offset: int = 1,
    ) -> Self:
        """Create a log. Raises if one already exists at `root/name`.

        This is the only call that takes the log's shape, because the shape is
        fixed at creation and recovered by `open` thereafter.

        **`start_offset` leaves `[1, start_offset - 1]` unassigned for ever**,
        and is the one way to do so. It exists to align a log's offsets with a
        sequence something else already owns, and to reserve room a later
        backfill can fill — see §13's *Bulk ingest*.

        It is creation-only, and there is deliberately no way to re-seed a log
        afterwards. `Buffer.seed_offsets`' guard reads the offsets currently
        BUFFERED, which is empty once a seal has deleted them, so it would not
        refuse a re-seed onto already-issued offsets: the next appends would
        reuse committed offsets, `_union` would hide them behind the table's
        extent and the seal's `register` would decline the file and queue it
        for deletion — rows acknowledged and silently gone. A fresh buffer is
        the only state in which seeding is safe, and `new` is the only call
        that has one.

        The value is recorded in `meta` when it is greater than 1, because a
        backfill has to tell that reserve from a `Log.restore` fence. Both are
        empty ranges below the log's offsets and nothing else distinguishes
        them — see `START_OFFSET_KEY`.

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
        # Same normalisation as `set_archive`, and for the same reason: an
        # empty string is not None to `validate`, so every rule that
        # presupposes an archive was skipped while `configured()` came back
        # False — `Log.new(archive="", config=LogConfig(local_retention=0))`
        # was accepted, which is exactly the pair `validate` exists to refuse.
        archive = (archive or "").rstrip("/") or None
        validate(schema, order, settings, archive)
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
        # A fresh log holds no `extent` rows at all, so any archive that
        # already has data belongs to something else. This is the shape an
        # operator reaches for when failing over by hand: `Log.new` on the
        # second box, pointed at the old prefix. Silently, that log pushes
        # nothing — its offsets are below the archive's — eviction pins, and
        # the archive's contents read back as its own. `set_archive` refuses
        # the same thing; both entry points need it, because either can be the
        # one that points the log.
        if archive is not None:
            try:
                covered = archive_extent(layout, archive, s3 or S3Options())
            except Exception:
                # Unreachable or unreadable is "cannot tell", which passes:
                # configuring an archive is a statement of intent, not a claim
                # that the bucket is already there.
                covered = None

            if covered is not None:
                raise _foreign_archive(archive)

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
        # crash safety. `Log.open` refuses a log with no stored schema or
        # config, so every window between here and `set_meta_all` fails closed.
        # Seeding AFTER them leaves one whose crash yields a log that reopens
        # silently at offset 1 — and `Log.new` then refuses to retry, because
        # the buffer exists. The reserve would be lost with no error anywhere.
        if start_offset > 1:
            buffer.seed_offsets(start_offset)

        buffer.set_meta(_SCHEMA_KEY, schema.serialize().to_pybytes().hex())
        # The pair in ONE transaction. `validate` has just accepted these two
        # together, and written separately a crash between them records the
        # policy without the archive it presupposes — evict-on-upload with
        # nothing to evict into, which `open` never re-checks and the first
        # maintenance pass carries out.
        buffer.set_meta_all(
            {
                _CONFIG_KEY: settings.to_json(),
                _SORT_KEY: json.dumps(list(order)),
                **({_ARCHIVE_KEY: archive} if archive is not None else {}),
                # Recorded only when there IS a reserve. Absent means
                # "started at 1", which a backfill must read as "no reserve"
                # rather than "a reserve of nothing" — a log created at 1 and
                # later restored has a gap below its offsets too, and that gap
                # is a fence.
                **({_START_OFFSET_KEY: str(start_offset)} if start_offset > 1 else {}),
            }
        )

        # Built here and handed to all three, so each is given its archive at
        # construction rather than having one pushed into it afterwards.
        remote = Archive(layout, buffer, s3)

        return cls(
            layout=layout,
            table=table,
            buffer=buffer,
            reader=Reader(
                layout,
                table,
                buffer,
                duckdb_connection,
                archive=remote,
            ),
            maintenance=Maintenance(table, buffer, layout, remote),
            config=settings,
            archive=remote,
        )

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
        their declared Arrow types, the config, the archive and the sort order
        all from the buffer's `meta` table (§2, §4). The sort order used to
        come from the table's own declaration; it moved because `catalog.db`
        cannot be restored onto another machine, so a failover rebuilds that
        table and has to be told what to declare. Restating any of it here would invite a caller to state
        something the log does not agree with, and the log is the one that is
        right.

        Always recovers, because this always returns a writer. For a second
        view of a log another process is writing, use `litelink.reader`, which
        runs no recovery and has no mutation to refuse — so it cannot disturb
        the single writer §1 assumes.
        """
        layout = Layout(Path(root), name)
        # Asked of THIS log's table, not of `catalog.db`. That file is shared
        # by every log under the root (§2), so a root holding one log answered
        # the question for every other name in it — and the caller got
        # pyiceberg's `NoSuchTableError` out of the load below instead of the
        # message here. "Cannot tell" falls through and lets the load answer,
        # which is slower and correct.
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

        remote = Archive(layout, buffer, s3)
        log = cls(
            layout=layout,
            table=table,
            buffer=buffer,
            reader=Reader(
                layout,
                table,
                buffer,
                duckdb_connection,
                archive=remote,
            ),
            maintenance=Maintenance(table, buffer, layout, remote),
            config=config,
            archive=remote,
        )
        log.recover()

        return log

    @classmethod
    def replication_config_for(
        cls,
        root: PathLike[str] | str,
        name: str,
        archive: str,
        s3: S3Options | None = None,
        retention: timedelta | None = None,
    ) -> str:
        """A litestream config for a log that may not exist here yet (§3a).

        The same file `replication_config` produces, without needing an open
        log to produce it — which is the chicken-and-egg a failover hits. The
        config names the databases to restore, and you need it BEFORE you have
        them. `Layout` is pure path arithmetic, so everything the file says is
        derivable from a root, a name and an archive.
        """
        layout = Layout(Path(root), name)

        return litestream_config(
            layout.databases, layout.root, archive, s3 or S3Options(), retention
        )

    @classmethod
    def restore(
        cls,
        root: PathLike[str] | str,
        name: str,
        *,
        archive: str,
        s3: S3Options | None = None,
        binary: str | None = None,
    ) -> Self:
        """Recover a log onto a machine that is not the one that wrote it (§3a).

        Point it at the archive, get a working log back, resume appending. The
        procedure this replaces was "restore the databases and open it", which
        does not work: `catalog.db` records ABSOLUTE paths to the local Iceberg
        metadata, and a sidecar ships the `.db` files and nothing else — not
        that metadata, not the Parquet. So a restored catalog names files on a
        machine that is gone, and `open` raises `FileNotFoundError`.

        What is recovered, and what is not:

        - **The archive**, in full. It names its own current metadata in
          `version-hint.text`, so it is adopted rather than rebuilt.
        - **The unsealed tail and everything the archive lacks**, from
          `buffer.db`. A seal keeps its rows until the archive has them when
          `wal_replication` is on, so the band between the two frontiers comes
          back too — that is what change makes possible.
        - **The local table**, rebuilt EMPTY. Its Parquet is on the dead
          machine and its metadata was never replicated.
        - **NOT** rows appended inside the replication lag. They were served to
          callers and never shipped. Their offsets are skipped rather than
          reissued; see below.

        **`archive.db` is deliberately not restored**, even though it is
        replicated. `open_archive` consults `version-hint.text` only when the
        catalog has no row for the table — with a stale row present it loads
        whatever that names, and old metadata survives in the bucket until
        expiry, so it succeeds. Measured: a stale replica reported one archive
        file where the bucket held five, and a union read 261 rows instead of
        1061. Worse, the next sync commits onto that lineage and republishes
        the hint over it, destroying the pointer this recovery depends on.
        Stale is worse than absent, and absent is already handled.

        **`catalog.db` is not restored either**, and stays in the replication
        set regardless: same-machine recovery is where its absolute paths still
        resolve, and it is the only record of which Parquet the local table is
        made of in a design that refuses directory listing.
        """
        layout = Layout(Path(root), name)
        # A buffer with no TABLE for this log is a restore interrupted before
        # its last write, not a log. Refusing it would leave the root in a
        # state neither this nor `Log.open` accepts, so the only way out would
        # be deleting it by hand. Resumed instead: everything before that write
        # is repeatable, and the reserve simply skips another window.
        #
        # Asked of the table, not of `catalog.db`. That file is shared by every
        # log under the root (§2), so in a root holding a second log it exists
        # already and a genuinely interrupted restore would never resume.
        #
        # A catalog it cannot READ counts as a log that exists. The resume path
        # reserves offsets, deletes every `extent` row and wipes the claim
        # tables — safe on an interrupted restore, catastrophic on a live log —
        # so "cannot tell" has exactly one safe reading, and it is not the one
        # that proceeds.
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

        # One config per ROOT, and `litestream_config` describes the log it was
        # asked about — so writing one here over a root that already replicates
        # another log would leave that log unreplicated at the sidecar's next
        # restart, silently. Refused rather than merged: §3a's advice is one
        # log per root until a per-root generator exists, and this is that
        # advice enforced.
        config_path = layout.root / "litestream.yml"
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
        layout.root.mkdir(parents=True, exist_ok=True)
        config_path.write_text(
            litestream_config(layout.databases, layout.root, archive, options)
        )

        # ONLY the buffer, and with no `-if-replica-exists`: that flag exits 0
        # when there is no backup, which would make the check below unable to
        # tell a restored database from an absent one.
        #
        # Skipped when resuming — the buffer is already here, and litestream
        # would refuse to write over it anyway.
        if not resuming:
            restore_buffer(config_path, layout.buffer_db, options, binary)

        if not layout.buffer_db.exists():
            msg = (
                f"no replica of {layout.buffer_db.name} under {archive} — there is "
                f"nothing to restore. A log with wal_replication off has no off-box "
                f"copy of its unsealed rows, and cannot be recovered onto another "
                f"machine"
            )
            raise FileNotFoundError(msg)

        # A stale entry, if an operator restored all three by hand. Not merely
        # unnecessary — actively destructive: `open_archive` consults
        # `version-hint.text` ONLY when the catalog has no row, so a stale row
        # is loaded instead, and old metadata survives in the bucket until
        # expiry so the load succeeds. Measured: a stale replica reported one
        # archive file where the bucket held five, and a union read 261 rows
        # instead of 1061. The next sync then commits onto that lineage and
        # republishes the hint over it, destroying the pointer this recovery
        # depends on. Dropped so adoption is the only path.
        forget_archive_entry(layout)

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
        # databases now exist — and `Log.open` cheerfully accepts, reporting
        # `recovery() is None`. An earlier version put it second and claimed
        # "this order has no such state"; it had one, one write later. Measured
        # from that window: the open group still at the replica's stale
        # frontier, the first seal writing a file straddling the archive's
        # extent, 712 archived offsets vanishing from every
        # `scan(include_archive=True)` with no error anywhere, and `sync`
        # raising for ever afterwards.
        #
        # Nothing before it needs it. Adoption and the reconcile are about the
        # ARCHIVE and the buffer; neither reads the local table. So it becomes
        # the commit point: everything ahead of it is repeatable, and a root
        # without it cannot be opened by anything.
        buffer = Buffer.open(layout.buffer_db, schema)
        try:
            released, resumed = buffer.strip_local_state(RESTORE_RESERVE)

            # ADOPTED, explicitly. `archive.db` was deliberately not restored,
            # so nothing here has a catalog row for the archive — and an
            # ordinary `open` will not create one: adoption is a write to that
            # catalog, and `open_archive` reserves it for a repairing caller.
            # Without this the log comes back holding only what the buffer
            # carried, and every `include_archive` read silently leaves the
            # archive leg out.
            #
            # Built standalone rather than reached through a `Log`, because
            # there is no local table yet — which is the whole point of doing
            # it here. It reads the archive's location from the restored
            # `meta`, so it adopts the archive THIS log wrote, not whatever
            # prefix the caller named for the WAL.
            remote = Archive(layout, buffer, options)
            where = remote.uri
            if remote.table(repair=True) is None and where is not None:
                # Not best-effort. A restore that cannot reach the archive has
                # recovered the unsealed tail and nothing else, and returning
                # it as a success is the "partial recovery that looks whole"
                # this refuses elsewhere.
                msg = (
                    f"restored the buffer but could not adopt the archive at "
                    f"{where!r}: it holds nothing this log can read. The rows "
                    f"below the archive frontier are not recovered"
                )
                raise RuntimeError(msg)

            # RECONCILED against the archive, and this is not optional. A
            # replica is a consistent snapshot from BEFORE the primary's last
            # sync — ordinary replication lag, not a crash window — so the
            # archive is routinely ahead of the `extent` rows the buffer
            # carries. Left alone, `_seed_group` opens the group at the
            # replica's stale frontier while the bucket already holds past it,
            # and the first seal here writes a file reaching into the archive's
            # extent.
            #
            # Which wedges the log permanently: `_refuse_straddle` raises on
            # every push, `archived_prefix` returns 0 for the straddler so
            # eviction pins at zero, and disk grows without bound. Nothing
            # re-cuts a local straddler, and this is the operation you run when
            # the archive is the only surviving copy.
            #
            # Releasing what the archive holds and re-seeding is what closes
            # it. Those rows are genuinely safe: the archive has them, which is
            # the same authority `_push` releases on.
            adopted = remote.table()
            covered = None if adopted is None else adopted.extent()
            frontier = 0 if covered is None else covered[1]
            if covered is not None:
                released -= buffer.release_archived(covered[1])
                buffer.reseed_group()
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
            # Undo NOTHING here. The row was already there, so this call did
            # not make it and dropping it would destroy a live log's only
            # pointer to its local files. Reached with `buffer.db` absent and
            # the row present — which the `FileExistsError` guard above cannot
            # catch, keying as it does on the buffer — and reproduced: a table
            # at offsets 1..1152 with the archive at 504 lost the reference to
            # 505..1152, sealed locally and never archived, with `Log.open`
            # then answering "use new() to create one".
            raise
        except Exception:
            with contextlib.suppress(Exception):
                LogTable.forget(layout)

            raise

        log = cls.open(layout.root, name, s3=options)

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
        # subtraction above it performs: rows the archive already held were
        # released two lines later, and counting them as recovered describes a
        # buffer that no longer exists.
        local = log.table_extent()
        buffered = log._buffer.extent()  # noqa: SLF001
        # The ARCHIVE's own frontier, read above, not `archived_through` — that
        # reads the replica's `meta`, which is the exact staleness the reconcile
        # ten lines up exists to correct. It bites whenever the release empties
        # the buffer, which is the ordinary "died shortly after a sync" shape:
        # measured, 16,100 offsets reported skipped that were present and
        # readable. Nothing is lost by it, but the documented response to a
        # skipped range is to re-fetch from upstream, and doing that would
        # duplicate them.
        highest = max(
            0 if local is None else local[1],
            frontier,
            0 if buffered is None else buffered[1],
        )
        log._restored_from = _Recovery(  # noqa: SLF001
            recovered=released,
            resumed_at=resumed,
            skipped=(highest + 1, resumed - 1),
        )

        return log

    # -- settings ----------------------------------------------------------

    @property
    def _schema(self) -> pa.Schema:
        """The declared columns, read from the log rather than remembered.

        A `Log` opened before a schema change would otherwise validate against
        the columns it was constructed with for the rest of its life. The
        buffer owns the durable copy and caches the decode, so this costs one
        keyed `meta` read.
        """
        return self._buffer.shape().schema

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
            lease = self._claim_settings()

            try:
                validate(
                    self._schema,
                    self._buffer.sort_by(),
                    config,
                    self._buffer.get_meta(_ARCHIVE_KEY) or None,
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
                # the seal target and `Log.config` all read this row rather
                # than keeping copies of it.
                self._buffer.set_meta(_CONFIG_KEY, config.to_json())
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
        """The SQLite files a restore needs (§3a). See `LogReader.databases`."""
        return self._view.databases

    def archive_files(self) -> int:
        """How many data files the archive holds, or 0 with no archive."""
        return self._view.archive_files()

    def coverage(self) -> Coverage:
        """What this log can serve, and where it cannot. See `LogReader.coverage`."""
        return self._view.coverage()

    def replication_config(self) -> str:
        """A litestream config for this log (§3a). See `LogReader`."""
        return self._view.replication_config()

    def write_replication_config(self) -> Path:
        """Write that config next to the log, and return where."""
        return self._view.write_replication_config()

    def recovery(self) -> _Recovery | None:
        """What `restore` recovered, or None on a log opened normally.

        Two numbers an operator needs and cannot get later: how many rows came
        back from the replica, and which offsets were skipped to avoid
        reissuing ones the dead machine had already served.
        """
        return self._restored_from

    @property
    def archive(self) -> str | None:
        """Where the archive is, or None for a local-only log.

        The question a maintenance loop has to answer before calling `sync`.
        """
        return self._view.archive

    def set_archive(self, archive: str | None) -> None:
        """Point the log at an archive, or detach it (§5).

        Takes the whole-log claim, so it cannot interleave with a sync, a
        merge, an eviction or the other setter — `validate` refuses a PAIR, so
        the policy and the location have to be decided together. The shipped
        writer calls this on every restart while a maintainer runs in another
        process, so it waits for maintenance rather than failing on the first
        try; see `_claim_settings`.

        **Re-pointing does not move anything, but it is reversible.** Rows
        already evicted into the old archive stay there, and the read path
        resolves only the archive the log currently names — so while pointed
        elsewhere they are not readable through this log, and
        `scan(include_archive=True)` returns fewer rows than were written,
        silently.

        Pointing BACK undoes that. Each archive publishes `version-hint.text`
        beside its metadata at every commit, so a prefix whose catalog entry
        this log dropped is registered from what the bucket itself says rather
        than created empty over the top of it. Only a repairing caller adopts,
        which `set_archive` is; a plain `include_archive` read still leaves the
        leg out until one has run.

        **An archive AHEAD of this log is refused.** One whose extent reaches
        at or above the next offset to be assigned is another log's history,
        and attaching it wedges this one silently — nothing is ever pushed,
        eviction pins, and local disk grows without bound. `Log.restore` is
        the operation for resuming that log here. Re-attaching to an archive
        this log has moved past is unaffected, and supported.

        **One writer per archive is otherwise assumed, not checked.** The hint
        records where the metadata was at the last commit THIS log made.
        Another writer touching that archive while it was detached would leave
        the hint behind its true state, and adopting it would strand the
        commits made in between. That is §13's archive-identity seam; the
        contract is one writer per log, and nothing here enforces it.

        What re-pointing no longer does is disturb what the log already knows.
        There is no watermark to carry across: each pushed file records the
        bucket its copy went to (§4a), so ranges the old archive holds go on
        naming it, eviction keeps asking about the archive that is configured
        now, and compaction keeps refusing to merge across any of them.
        """
        # NORMALISED here, not in `_repoint`. Every guard below tests
        # `archive is None`, and `_repoint` used to be the only place that
        # turned an empty string into None — so `set_archive("")` was non-None
        # to `validate`, to `_refuse_archive_ahead` and to
        # `_refuse_lossy_detach`, and None to the write. It detached with none
        # of the three applying. Measured: 7,828 acknowledged rows lost, the
        # same magnitude as the case those guards exist for.
        archive = (archive or "").rstrip("/") or None

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
                validate(self._schema, self._buffer.sort_by(), self.config, archive)
                self._refuse_archive_ahead(archive)
                self._refuse_archive_behind(archive)
                self._refuse_lossy_detach(archive)
                # The other half of the same rule; see `set_config`.
                checkpoint(lease.renew)
                self._repoint(archive)
            finally:
                lease.release()

    def _refuse_lossy_detach(self, archive: str | None) -> None:
        """Refuse a detach that could expose unarchived files to eviction.

        I4 — never delete a local file the archive lacks — is enforced by a
        clamp in `evict` that runs only while an archive is CONFIGURED. So
        detaching does not merely stop using the archive: it retires the clamp,
        for every process at once, and the next maintenance pass treats the
        files still waiting to be pushed as ordinary retention candidates. A
        maintainer already looping elsewhere does it without the operator
        calling anything.

        Measured: sync 4,550 rows behind, one detach, one pass, 4,025
        acknowledged offsets unreadable. Nothing recovers them — `hydrate`
        restores only what the archive holds, and `sync` cannot push files that
        have left the table.

        §8's "with no archive, `local_retention` is a deletion policy over the
        only copy" is the contract a local-only log asked for. A log that HAD
        an archive did not ask for it, and was getting it as a side effect.

        **A blunt refusal, deliberately.** The precise question is "is there an
        unarchived file that retention would reach", and answering it is not
        the hard part — keeping the clamp alive across a detach is, since the
        per-file `extent` rows outlive `_repoint` and could carry it. That is a
        change to eviction, and it is tracked separately. Until then this
        refuses the whole shape rather than pretending to a precision it does
        not have: with no floors set, eviction does nothing and a detach is
        safe; with one set, it may not be, and the library says so instead of
        guessing.
        """
        if archive is not None or not self._archive.configured():
            return

        config = self.config
        if config.local_retention is None and config.local_rows is None:
            return

        msg = (
            "refusing to detach: this log has a retention floor set, and "
            "detaching retires the I4 clamp that keeps eviction off files the "
            "archive has not taken yet — so the next maintenance pass, in this "
            "process or any other, could delete them. Nothing recovers them.\n"
            "Clear local_retention and local_rows with set_config to say you "
            "accept that, then detach."
        )
        raise ValueError(msg)

    def _refuse_archive_behind(self, archive: str | None) -> None:
        """Refuse an archive missing a column this log has since added.

        Attaching does no schema work — `open_archive` only DECLARES a schema
        when it creates a table, and nothing in `src/` re-declares an existing
        one. So an archive detached before an `add_column` and re-attached
        after it stays narrow, and every later push fails permanently:

            ValueError: PyArrow table contains more columns: region.

        `sync` then never advances, eviction's I4 clamp pins on the unpushed
        files, and local disk grows without bound — the exact consequence
        `_apply_add_column` orders its steps to prevent, arrived at through a
        door that skips step 4 entirely.

        Refused rather than repaired. Widening an existing archive is
        `union_by_name` against a table this log did not create, which belongs
        with `rewrite_archive` and wants its own design; doing it quietly from
        a setter would mean `set_archive` mutating a shared archive as a side
        effect. Tracked as an issue.
        """
        if archive is None:
            return

        # Read from the bucket, not through `self._archive`: that object
        # takes its URI from `meta`, which still names the OLD archive — or
        # None, on the detach-then-attach path this exists to catch.
        try:
            columns = archive_columns(self._layout, archive, self._archive.s3)
        except Exception:
            # A bad minute in object storage is not a schema disagreement.
            # `_refuse_archive_ahead` treats it the same way: cannot tell, so
            # pass rather than refuse an archive that may be fine.
            return

        if columns is None:
            return

        declared = set(self._buffer.shape().schema.names)
        missing = sorted(declared - set(columns))
        if missing:
            msg = (
                f"archive at {archive} is missing {missing}, which this log has "
                "added since. Attaching it would make every later sync fail "
                "permanently and pin eviction, so the log would grow without "
                "bound. Widen the archive first"
            )
            raise ValueError(msg)

    def _refuse_archive_ahead(self, archive: str | None) -> None:
        """Refuse an archive whose extent is above this log's next offset.

        That archive belongs to a different log's history, and attaching it
        wedges this one SILENTLY. Traced: `sync` computes `floor` from the
        archive's extent, every local file sits below it, so `pending` is empty
        and nothing is ever pushed. The watermark is still written, eviction's
        I4 clamp finds no `extent` rows and pins at zero, and local disk grows
        without bound while `sync()` returns success having uploaded nothing.
        No error surfaces at any step.

        It is reachable by the obvious failover attempt — `Log.new` on a second
        box, then `set_archive` at the old prefix — which is exactly what
        `Log.restore` exists to do properly.

        **`>= next_offset`, not "overlaps".** Re-attaching to an archive that
        holds offsets this log has moved PAST is supported and tested; the
        archive's ranges simply sit below the local ones. Only an archive
        reaching at or above the next offset to be assigned is describing a
        stream this log is not.

        **Read through `version-hint.text`, never `open_archive`.** At this
        point `meta` still names the OLD archive, so `Archive.table()` opens
        that one. Going to `open_archive` for the new prefix fails both ways:
        with `repair=False` the catalog row names the old archive and the
        boundary check raises on every ordinary re-point; with `repair=True` it
        drops that row as a side effect of what is meant to be a read.

        Absent, unreachable, or unreadable all PASS. `_repoint` deliberately
        tolerates an archive that does not exist yet — "configuring one is a
        statement of intent, not a claim that the bucket exists" — and this is
        called on every writer restart, so it must not fail closed on a bad
        minute in object storage.
        """
        if archive is None:
            return

        try:
            covered = archive_extent(self._layout, archive, self._archive.s3)
        except Exception:
            return

        if covered is None:
            return

        nxt = self._buffer.next_offset()
        if covered[1] >= nxt:
            msg = (
                f"the archive at {archive!r} holds offsets up to {covered[1]}, at or "
                f"above this log's next offset ({nxt}) — it is another log's "
                f"history. Attaching it would push nothing and pin eviction, "
                f"silently. To resume that log here, use Log.restore"
            )
            raise ValueError(msg)

        # And it must be an archive this log has SEEN. A populated prefix that
        # this log holds no `extent` row for is somebody else's, whatever its
        # offsets look like — and offsets are all a comparison has to go on,
        # since two logs of the same name both start at 1.
        #
        # Attaching one cannot be contained downstream, which two attempts
        # tried: the watermark is raised to the archive's extent by `confirmed`
        # on every pass, and this log's own `extent` rows are written for the
        # archive's ENTIRE manifest by `_push`'s backfill within one sync.
        # Measured — a bound derived from either moved with the contamination.
        # The backfill is right to trust the manifest; what it needs is for the
        # archive to be ours, and §13's identity token is what would prove it.
        # Until then the check belongs at the moment the log is pointed, before
        # anything has laundered anything.
        #
        # Re-attach passes: ranges pushed to an archive go on naming it after a
        # detach (§4a), so returning to one finds its own records intact. A
        # fresh prefix passes too — `covered` is None above.
        if not self._buffer.archive_records(archive, 0):
            raise _foreign_archive(archive)

    def _repoint(self, archive: str | None) -> None:
        """Record the new location, with the maintenance lease held."""
        # Already normalised by `set_archive`, which is the only caller and
        # does it before its guards rather than after. Repeated here so this
        # method is correct on its own terms: every path builder strips
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

    @property
    def schema(self) -> pa.Schema:
        """The caller's columns, as declared at `new`.

        `open` takes no schema, so this is the only way to ask a log what shape
        it is. `litelink_offset` is stripped: this is the schema the caller
        wrote and the one `append` accepts, while the TABLE's schema carries
        the library's column in front of it (I11).
        """
        return self._schema

    @property
    def sort_by(self) -> tuple[str, ...]:
        """The within-file sort order, read from the log on every access (§7).

        §7 tells a reader to bound every scan on a LEADING column of this —
        measured at 13 ms against 119 ms for the same predicate without one —
        which is advice no caller can follow without being able to ask.

        Read through to `meta` like `config` and `archive`, and for the reason
        they are: `set_sort_by` in one process must not leave another holding
        the key it opened with. An earlier draft of this property returned a
        field and claimed parity with those two in its own docstring while
        being the copy they exist to avoid.
        """
        return self._buffer.sort_by()

    def set_sort_by(self, sort_by: Sequence[str], *, rewrite: bool) -> None:
        """Change the sort order, re-clustering every file the local table owns.

        §7 calls `sort_by` a read-shape decision rather than a tuning knob:
        it declares which predicates prune, and the clustering that makes them
        prune is baked into each file when it is written. So a new order that
        is only declared would apply to future seals and silently leave every
        existing file clustered the old way — the same predicate fast on recent
        data and slow on older data, with nothing to indicate why.

        `rewrite` must be passed explicitly. It is the honest name for the
        cost: every file this rewrites is read, re-sorted and replaced.

        **The archived prefix is not re-clustered, and cannot be.** `_rewrite_run`
        skips any run reaching at or below the archive watermark, because a
        local rewrite there would commit a file straddling the archive's extent
        and nothing re-cuts a local straddler. So on a synced log this changes
        three declarations and rewrites only what sync has not yet taken — and
        `rewrite_archive` is not the other half either: it re-ingests from the
        first badly-SIZED file onwards, so a well-sized archived prefix is
        never a candidate and keeps its original clustering for ever.

        That is §6's "sealed once and never rewritten" applied to history, not
        an oversight — but this docstring used to say "every existing file",
        which on an archived log was a claim the code did not honour and did
        not report. A re-sort is a decision for a log's local window; the
        archive keeps the clustering it was written with.
        """
        with self._lock:
            requested = tuple(sort_by)
            validate(self._schema, requested, self.config, self._archive.uri)
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
                    "changing sort_by re-clusters every file the local table "
                    "owns, and leaves the archived prefix as it is; "
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
                # The archive's declaration too, and BEFORE `meta` like the
                # local one: `open_archive` declares an order only on a table
                # it creates, so an archive that already exists is re-declared
                # here or never. Missing it entirely left an archive created
                # after a re-sort born declaring the old key, with every file
                # pushed into it clustered by the new one.
                self._archive.redeclare_sort_order(requested)
                self._buffer.set_meta(_SORT_KEY, json.dumps(list(requested)))
                self._maintenance.rewrite_sorted(
                    heartbeat=lease.renew, owner=lease.owner
                )
            finally:
                lease.release()

    def _claim_settings(self) -> Claim:
        """Take the whole-log claim the configuration operations share.

        Retried, not refused on the first try. `set_config` and `set_archive`
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

    # -- read --------------------------------------------------------------

    def scan(
        self,
        *,
        columns: Sequence[str] | None = None,
        where: str | None = None,
        start_offset: int | None = None,
        end_offset: int | None = None,
        include_archive: bool | None = None,
    ) -> pa.RecordBatchReader:
        """Read the log as one relation, newest data included. See `LogReader.scan`."""
        return self._view.scan(
            columns=columns,
            where=where,
            start_offset=start_offset,
            end_offset=end_offset,
            include_archive=include_archive,
        )

    def sql(
        self, query: str, *, include_archive: bool | None = None
    ) -> pa.RecordBatchReader:
        """Arbitrary DuckDB SQL against the log, exposed as `log`."""
        return self._view.sql(query, include_archive=include_archive)

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
        """Rows durable in the buffer but not yet sealed."""
        return self._view.buffered_rows()

    def table_rows(self) -> int:
        """Rows in the local Iceberg table, from the manifests."""
        return self._view.table_rows()

    def table_files(self) -> int:
        """Data files in the local table — what compaction is bringing down."""
        return self._view.table_files()

    def table_extent(self) -> tuple[int, int] | None:
        """`(lo, hi)` offset extent of the local table, from statistics (§7)."""
        return self._view.table_extent()

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
        # buffer for its extent used to.
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
        # table extent — so it is a wasted rewrite rather than loss, and the
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

        This is a `Log` method and not a `LogReader` one for that reason: a
        reader could only watch, and a watcher that cannot help is the case
        this exists to avoid. It used to branch on `readonly` to decide, which
        is the flag that no longer exists.
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

        - **No archive** — nothing is off-box either way, and holding would
          hold for ever, because nothing would ever release it. Discard.
        - **Archive, no `wal_replication`** — the buffer and the Parquet share
          a disk and die together, so holding buys nothing and costs SQLite
          growth. Discard.
        - **Archive and `wal_replication`** — the buffer IS the off-box copy
          until the archive has the range. Hold, and let `release_archived`
          drop them once sync has pushed it.

        `validate` refuses `wal_replication` without an archive, so the last
        case is just the flag — but both halves are read, because the flag
        alone would be a claim about the archive that this does not check.

        Read durably on every seal rather than cached: `set_config` and
        `set_archive` both change the answer from another process, and §4a's
        rule is that a decision reads the log rather than its own memory.
        """
        return not (self.config.wal_replication and self._archive.configured())

    def _write_and_commit(
        self, start: int, end: int, rel_path: str, lease: Claim | None = None
    ) -> None:
        """Write the Parquet file, fsync it, then commit it to the table.

        I1 in this order: committing first would publish a manifest entry for a
        file that may not survive the crash.

        `start` as well as `end`, because the buffer's floor is no longer the
        group's floor: with WAL replication on, sealed rows stay until the
        archive has them (§3a), so a read bounded only above would sweep every
        earlier row into this file. `Buffer.rows_between` records what that
        costs.
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

        # Last, and under the settings claim rather than either of those: an
        # unfinished schema change is not a seal and not a compaction, and it
        # must not interleave with the setters that share this range.
        self._recover_schema_change()

    def _recover_schema_change(self) -> None:
        """Replay an interrupted `add_column`, or leave it standing.

        Every step is settled by ASKING the thing itself rather than by a
        record of what was done. A stamp cannot be made atomic with the commit
        it describes — it only moves the unreconciled gap from before the
        commit to after it — and a stamp can be false: `Log.restore` rebuilds
        the local table from `meta`, so a replica captured after step 5 would
        arrive carrying a stamp for a step that never landed on THIS machine.

        **If the archive is unreachable the intent is left in place and this
        returns.** The log opens, appends, seals and reads; the change simply
        has not finished. That is safe because step 7 has not run, so the
        declared schema is unchanged and no value of the new column can be
        stored — the INSERT column list derives from `arrow_schema`. Failing
        the open instead would mean an `add_column` interrupted by an outage
        makes the log unopenable for writing until the bucket returns, which
        breaks §11's promise that a log works with no network.
        """
        pending = self._buffer.get_meta(INTENT_KEY)
        if not pending:
            return

        recorded = json.loads(pending)
        name = recorded["add"]
        type_ = (
            pa.ipc.read_schema(pa.py_buffer(bytes.fromhex(recorded["type"])))
            .field(0)
            .type
        )

        try:
            # Inside the try, because taking the claim can itself fail. Unlike
            # the other two halves of `recover()`, which use a non-blocking
            # `acquire()`, this one waits and then RAISES if a maintainer is
            # holding the whole-log range — and `recover()` is unguarded in
            # `open`, so that would fail the open for every writer while
            # `litelink.reader` kept working. That is the failure this method
            # exists to avoid; leaving the acquisition outside meant it could
            # still happen, just for a different reason.
            lease = self._claim_settings()
        except Exception:
            return

        try:
            # Re-read under the claim: another process may have finished it
            # between the check above and here.
            if not self._buffer.get_meta(INTENT_KEY):
                return

            declared = self._buffer.shape().schema
            if name in declared.names:
                # Step 7 landed; only the intent is outstanding.
                self._buffer.set_meta(INTENT_KEY, "")

                return

            self._apply_add_column(name, type_, declared, lease)
        except Exception:
            # Deliberate, and broad on purpose: recovery DEFERS, it never
            # fails the open. §11 promises a log works with no network, and an
            # `add_column` interrupted by an outage must not make the log
            # unopenable for writing until the bucket returns.
            #
            # A genuine conflict — `union_by_name` refusing a real type change
            # — defers here too rather than raising. That is not silent: the
            # intent stands, so the change never completes and the next
            # `add_column` refuses while NAMING the outstanding column. A
            # visible stall beats a log no writer can open.
            return
        finally:
            lease.release()

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
            # delete removed the only off-box copy of a range the archive does
            # not hold, with replication on. Measured: five buffered rows
            # before the crash, zero after the recovery.
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
        # `pending_delete`, whose drain refuses anything the table references.
        # Recovery uses the same route.
        #
        # That veto is read ONCE per drain pass, not per removal — this said
        # otherwise, and the difference is the whole of a defect found on the
        # archive side: a commit landing after the veto was read leaves drain
        # deleting objects the manifest now names. What actually makes it safe
        # is that the committer renews its claim immediately before committing,
        # so a claim recovery has taken cannot commit at all.
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

        # RELEASED HERE, at the top of the pass, from the archive's own extent.
        # These are rows a seal held because nothing off-box had them yet
        # (`_discard_on_seal`); the archive now does, so they can go.
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
            # The archive's own extent, and that is sound only because
            # `_refuse_archive_ahead` has already established the archive is
            # OURS. Two narrower bounds were tried here and neither works: the
            # watermark is raised to `floor` by `confirmed` below on every
            # pass, and this log's own `extent` rows are written for the
            # archive's whole manifest by the backfill within one sync. Both
            # are downstream of a contamination that has to be stopped at the
            # point the log is pointed.
            self._buffer.release_archived(floor)

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
        # Bound before the first use. The backfill below sizes an unmeasured
        # archive file by the compact target, and `stable_prefix` groups by the
        # same policy — two reads, and nothing that makes them agree.
        config = self.config
        local = self._table.data_files()
        base = min((f.lo for f in local), default=0)

        # RECONCILIATION, matched by path in the archive's manifest rather than
        # by offset range. Range matching reads plausibly and is wrong: a
        # rewrite's intents name new objects over a range the stale files being
        # replaced still cover, so a crashed rewrite's dead intents would be
        # confirmed rather than dropped.
        #
        # Bounds differ between the two reads and must. The manifest walk is
        # bounded by the local window, or it grows with the archive and runs on
        # every sync. The intent read is unbounded, because an intent below the
        # window has to be reachable to be dropped.
        held_paths = {f.path: f for f in archive.data_files() if f.hi >= base}
        recorded = {
            path for path, _, _, _ in self._buffer.archive_records(pinned or "", base)
        }
        intended = {
            path: (lo, hi, size)
            for path, lo, hi, size in self._buffer.intents(pinned or "")
        }

        # ONE rule per path, decided by which of the two tables holds it. An
        # earlier shape ran the rules as separate loops over a `recorded` set
        # snapshotted before either — so rule 2 re-fired for every path rule 1
        # had just confirmed, and `record_file`'s conflict clause overwrote the
        # intent's measured bytes with the default. That made the `bytes`
        # column dead in every reachable path: the rewrite tail this exists to
        # size correctly was durably recorded as full, and nothing re-measures
        # an archived file.
        for path, landed in held_paths.items():
            recovered = intended.get(path)
            if recovered is not None:
                # 1. The register landed. Confirm it with the bytes the intent
                #    carried — the only measurement that survives a crash
                #    between a rewrite's commit and its confirm.
                lo, hi, size = recovered
                self._buffer.record_file(path, lo, hi, size)
            elif path not in recorded:
                # 2. In the manifest with no row of either kind: the backfill
                #    this rule grew out of.
                self._buffer.record_file(
                    path,
                    landed.lo,
                    landed.hi + 1,
                    memory.get(path, config.compact_size),
                )

        for path in intended:
            if path not in held_paths:
                # 3. Nothing in the manifest holds that path, so the register
                #    never landed and the intent is dead. Below the local
                #    window this also drops intents whose register DID land —
                #    the manifest walk is bounded — and their sizes are then
                #    never measured. No reader below the window asks.
                self._buffer.forget_intent(path)

        pending = [f for f in self._table.data_files() if f.hi > floor]
        # `stable_prefix` holds a file back when compaction might still merge
        # it, and compaction refuses to merge anything some archive already
        # holds — so the two need the SAME exclusion or they deadlock. They
        # share `runs` for exactly this reason, and giving compaction a second
        # input this could not see was enough to break it: after a re-point to
        # a fresh prefix the floor is 0, so files an old archive covers are
        # back in `pending`, group into a mergeable run under a raised target,
        # and are held back for ever against a merge that will never happen.
        # Nothing is pushed, the watermark never moves, eviction pins on it,
        # and no error surfaces anywhere.
        #
        # A file no merge can touch is settled by definition. Only the part
        # above that line is still compaction's business.
        # Intents included, so this exclusion is literally compaction's. The
        # two share `runs` so they cannot disagree about what is in play, and
        # a second input one of them could not see is what deadlocked them once
        # already.
        frozen = self._maintenance.archived_prefix(pending, None, include_intents=True)
        head = [f for f in pending if f.lo > frozen]
        settled = (len(pending) - len(head)) + stable_prefix(
            head,
            config.compact_size,
            config.compact_min_files,
            memory,
            config.compact_rows,
        )

        uploaded: list[tuple[DataFile, str]] = []
        for data_file in pending[:settled]:
            checkpoint(lease.renew)
            rel_path = self._layout.relative(data_file.path)
            # The intent BEFORE the upload, which is the seal's I2 argument
            # applied to the archive: the name goes down before the object
            # exists. What it guards is the register that follows — that can
            # land while the row recording it does not, and compaction decides
            # what it may merge from those rows, so without this a
            # compaction-target change before the next sync merges across a
            # range the archive holds and every later push is refused for ever.
            #
            # For EVERY file, not only measured ones: the unmeasured are
            # exactly the ones the confirm below used to skip.
            self._buffer.intend_file(
                archive.uri(rel_path),
                data_file.lo,
                data_file.hi + 1,
                memory.get(data_file.path, config.compact_size),
            )
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
            #
            # For every file, with the same default the intent used. The guard
            # that used to skip unmeasured ones was a hole: in a takeover the
            # confirm is the only thing that recreates rows a rival's
            # reconciliation dropped, so skipping any file reopens the window
            # for exactly the files the intent was added to protect.
            #
            # Same extent, second location. `end_offset` is exclusive, as it is
            # on every other extent — the cut is the offset AFTER the last row.
            self._buffer.record_file(
                archive.uri(rel_path),
                data_file.lo,
                data_file.hi + 1,
                memory.get(data_file.path, config.compact_size),
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

        # Again, now that this push has landed and its `record_file` rows
        # exist. The release at the top of the pass ran before them, so on its
        # own it frees rows one whole sync late — safe, since holding is the
        # safe direction, but a busy log would carry a sync's worth it no
        # longer needs.
        #
        # This one is the promptness; that one is the correctness. A crash
        # between the register above and this line leaves rows held, and the
        # next pass frees them from the same rows without needing anything to
        # have been uploaded. Neither placement alone is both.
        #
        if not self._discard_on_seal():
            self._buffer.release_archived(last.hi)

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

        **And DETACHING is the operation that makes a log local-only**, which
        is the part nothing used to say. A log that had an archive never asked
        for that contract, so `set_archive(None)` now refuses while a retention
        floor is set rather than silently converting files awaiting upload into
        ordinary retention candidates. See `_refuse_lossy_detach` and issue #21.

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
        stranding a small file, and a change to `target_compact_size`, which
        applies to the future while the archive is immutable history.

        Run it when nothing else is maintaining the log: it takes the same
        lease as `maintain` and `sync`, and it rewrites the same files they
        would.
        """
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
        # Refuses exactly what `validate_schema` refuses at creation, so a
        # column cannot be added that could never have been declared.
        column_type(type_)

        # Nullability is not checked because it cannot be expressed: `type_` is
        # a DataType, which carries none, and `pa.field(name, type_)` below is
        # nullable. That is the right constraint rather than a gap — rows that
        # predate the column have no value for it, so a required column would
        # make every existing file invalid, and Iceberg refuses it too.

        with self._lock:
            # The claim `set_config` and `set_archive` share. Claims exclude by
            # RANGE, not by kind, and this one is `[0, EVERYTHING]` — so seal,
            # compact, sync, evict, `rewrite_archive`, `hydrate` and
            # `set_sort_by` are all excluded for the whole change.
            #
            # What it does NOT exclude is `Log.extend`, which takes no claim at
            # all, nor `Reader.query`. That is safe only because every holder of
            # the schema now reads it from `meta` per call: a writer appending
            # during the change sees the old columns or the new ones, never a
            # torn pair.
            lease = self._claim_settings()
            try:
                declared = self._buffer.shape().schema
                self._refuse_duplicate_column(name, declared)

                # Step 3: the intent, before anything it describes (I16).
                # Nothing else is recorded — every step below is settled by
                # asking the thing itself, because a stamp can be FALSE.
                # `Log.restore` rebuilds the local table from `meta`, so a
                # replica captured mid-change would carry a stamp saying step 5
                # had landed onto a machine where it had not.
                self._buffer.set_meta(INTENT_KEY, _intent(name, type_))
                checkpoint(lease.renew)
                self._apply_add_column(name, type_, declared, lease)
            finally:
                lease.release()

    def _refuse_duplicate_column(self, name: str, declared: pa.Schema) -> None:
        """One change at a time, and the refusal says which one is outstanding.

        The intent clause is not belt-and-braces. Recovery defers step 4 when
        the archive is unreachable, so an unfinished change can stand for as
        long as the outage lasts. Allow a SECOND change on top of it and step 7
        writes `declared + the new one`, dropping the pending column from the
        declaration while `union_by_name` keeps it in both Iceberg tables — and
        clears the intent, which is what makes it fatal. `_declared_schema` then
        refuses to open the log for every process, reader and writer, on a log
        with every row intact.
        """
        pending = self._buffer.get_meta(INTENT_KEY)
        if pending:
            outstanding = json.loads(pending)["add"]
            msg = (
                f"cannot add {name!r}: the change adding {outstanding!r} has not "
                "finished. Reopen the log to complete it — if its archive is "
                "unreachable the change waits for the archive rather than "
                "failing the open"
            )
            raise ValueError(msg)

        if name in declared.names:
            msg = f"column {name!r} already exists"
            raise ValueError(msg)

    def _apply_add_column(
        self, name: str, type_: pa.DataType, declared: pa.Schema, lease: Claim
    ) -> None:
        """Steps 4-7, in the one order that cannot strand data.

        Step 4 before step 6 is ordered, not merely sequenced. Step 6 is what
        makes a value of the new column storable at all; step 4 is what makes
        the archive able to ACCEPT a file carrying one. `add_files` treats an
        optional field missing from a file as compatible but refuses a file
        with a column the table lacks — so an archive left behind while the
        buffer moves ahead means every later push fails permanently, I4 pins
        eviction, and local disk grows without bound.

        If step 4 cannot complete the change stops there, with nothing local
        changed and nothing to undo.
        """
        widened = pa.schema([*declared, pa.field(name, type_)])
        table = table_schema(widened)

        # Step 4: the ARCHIVE first — §9, "the local table is a window and can
        # be rebuilt; the archive cannot". A full S3 metadata commit against a
        # 30 s TTL, so the lease is renewed immediately before it.
        try:
            remote = self._archive.table(repair=False)
        except Exception as exc:
            # Reaching the archive is a NETWORK question, and every way it can
            # fail means the same thing here: step 4 cannot run. `table()`
            # returns None for an absent archive but raises for an unreachable
            # one, so both shapes have to be converted.
            msg = f"cannot add {name!r}: the archive is unreachable ({exc})"
            raise _ArchiveDeferred(msg) from exc

        if remote is None and self._archive.configured():
            # Configured but not reachable. `table()` returns None for both
            # "no archive" and "cannot open it", and treating them alike here
            # would skip step 4 and run steps 5-7 anyway — leaving the archive
            # behind a buffer that can now store the new column. Every later
            # push would fail permanently.
            msg = (
                f"cannot add {name!r}: the archive is configured but unreachable, "
                "and the archive must be widened first (§9). The change is "
                "recorded and completes on a later open"
            )
            raise _ArchiveDeferred(msg)

        if remote is not None:
            checkpoint(lease.renew)
            remote.add_column(table)

        # Step 5: the local table, same call, same reason.
        checkpoint(lease.renew)
        self._table.add_column(table)
        self._table.reload()

        # Step 6: the buffer DDL. Without it the INSERT column list would name
        # a column SQLite does not have.
        self._buffer.add_table_column(name, type_)

        # Step 7: the declaration, and only then the intent cleared — §9, a
        # schema change is complete when SQLite says so, not when Iceberg does.
        # One transaction, so no reader sees the new schema without the intent
        # already gone.
        self._buffer.set_meta_all(
            {
                _SCHEMA_KEY: widened.serialize().to_pybytes().hex(),
                INTENT_KEY: "",
            }
        )

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
        # One disagreement is legitimate, and only one: a schema change that
        # got its Iceberg commits in and was interrupted before step 7 leaves
        # the table holding exactly one column the declaration does not, with
        # the intent that names it still standing. Recovery finishes it a few
        # lines after this returns.
        #
        # Narrow deliberately — an outstanding intent excuses THAT column and
        # nothing else. Accepting any superset would let a genuinely corrupt
        # table through, which is what this check exists to catch.
        pending = Buffer.peek_meta(layout.buffer_db, INTENT_KEY)
        expected = (*declared.names, json.loads(pending)["add"]) if pending else ()
        if tuple(from_table.names) != expected:
            msg = (
                f"stored schema {declared.names} disagrees with the Iceberg table "
                f"{from_table.names} — the log has been modified outside litelink"
            )
            raise ValueError(msg)

    return declared


class _ArchiveDeferred(RuntimeError):
    """Step 4 could not run because the archive is not reachable.

    Its own type because recovery treats it as "leave the intent standing and
    open anyway", while `add_column` lets it out to the caller — the same
    condition, but a failed call and a deferred replay are different answers.
    """


@dataclass(frozen=True, slots=True)
class Coverage:
    """What a reader holds, and any offsets in neither tier.

    Reported rather than enforced. `gap` is a range the reader cannot serve;
    whether that is lost data or a never-issued reserve is a question only the
    caller can answer — see `LogReader.coverage`. `archive` is None when the
    log has no archive configured.
    """

    archive: tuple[int, int] | None
    buffered: tuple[int, int] | None
    gap: tuple[int, int] | None
    wal_replication: bool


class LogReader:
    """Everything you can ask a log without writing to it.

    One type for both read-only cases, because they differ in STATE and not in
    kind. A reader on the writer's box shares the log's directory and sees its
    local Iceberg table fill; a reader assembled by `follow` has a restored
    replica of the buffer and a local table that is empty and stays empty.
    Nothing else about them differs, and an earlier design that made them two
    classes — a `Log` with a `readonly` flag guarding thirteen write methods,
    and a separate `Follower` — had to keep two answers to every question.

    **What the follower used to encode as a type is derived here.** The
    archive is load-bearing exactly when the local table holds nothing and the
    archive is known to hold something: then it is the only place the archived
    rows can come from, so reads must include it and a missing archive must
    refuse rather than quietly serve the buffer alone. That is true of a
    followed log, whose table is empty by construction, and equally true of a
    local log that has evicted everything — a case the old split got wrong,
    measured at 476 of 1,500 rows returned with no error. See
    `_archive_required` for why the watermark is part of the test.

    **One thing is NOT derived, and it is a real cost of unifying.**
    `include_archive=False` was absent from `Follower` and is merely refused
    here, because the parameter has to exist for the local case. Asking for it
    where the archive is load-bearing raises instead of being unrepresentable.
    """

    def __init__(
        self,
        *,
        layout: Layout,
        table: LogTable,
        buffer: Buffer,
        archive: Archive,
        reader: Reader,
        owned: tempfile.TemporaryDirectory[str] | None = None,
    ) -> None:
        self._layout = layout
        self._table = table
        self._buffer = buffer
        self._archive = archive
        self._reader = reader
        # Set only by `follow`, which assembles into a scratch directory it
        # must remove. A reader on an existing log owns none of its root.
        self._owned_root = owned

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

    def schema(self) -> pa.Schema:
        """The caller's columns, without `litelink_offset` (I11)."""
        return self._buffer.schema

    @property
    def archive(self) -> str | None:
        """Where the archive is, or None for a local-only log.

        Read from `meta` on every access, so a caller cannot disagree with the
        log about it.
        """
        return self._archive.uri

    @property
    def databases(self) -> tuple[Path, ...]:
        """The SQLite files a restore needs (§3a), for a sidecar to replicate."""
        self._refuse_if_followed("databases")

        return self._layout.databases

    def replication_config(self) -> str:
        """A litestream config for this log (§3a).

        **A read, which is why it belongs here.** Generating it is exactly what
        you do alongside a live writer, and doing that through a writer takes
        both whole-log claims and the SQLite write lock on a log another
        process owns — and runs `recover()`, which can finish and replace that
        process's in-flight seal. `examples/adsb/replicate.py` did that for one
        commit, because this surface was on `Log` alone and the example had to
        reach for a writer to get at it.

        Refused on a FOLLOWED log, which is the reason it was kept off readers
        in the first place: `litestream_config` keys each replica on the path
        relative to the root, so a follower with the same log name emits a key
        identical to the primary's, and a sidecar run there ships the
        follower's stripped scratch copy over the primary's only off-box record
        of its unsealed rows. A local reader shares the primary's root and
        name, so the key it emits IS the primary's own correct one.
        """
        self._refuse_if_followed("replication_config")
        archive = self.archive
        if archive is None:
            msg = (
                f"{self.root}/{self.name} has no archive, so there is nowhere to "
                f"ship its WAL — replication needs one"
            )
            raise ValueError(msg)

        return litestream_config(
            self._layout.databases,
            self._layout.root,
            archive,
            self._archive.s3,
            self.config.wal_retention,
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

    def _refuse_if_followed(self, what: str) -> None:
        """A followed log must not describe replication. See `replication_config`.

        Keyed on owning the root, which only `follow` does — it assembles into
        a scratch directory it removes on close. A reader on an existing log
        never owns one.
        """
        if self._owned_root is not None:
            msg = (
                f"this is a followed log, assembled into {self.root}. Its "
                f"{what} would name the PRIMARY's replica key, and a sidecar run "
                f"here would ship this scratch copy over the primary's only "
                f"off-box record of its unsealed rows"
            )
            raise ValueError(msg)

    # -- read --------------------------------------------------------------

    def scan(
        self,
        *,
        columns: Sequence[str] | None = None,
        where: str | None = None,
        start_offset: int | None = None,
        end_offset: int | None = None,
        include_archive: bool | None = None,
    ) -> pa.RecordBatchReader:
        """Read the log as one relation, newest data included.

        Unions the tiers and bounds each by its neighbour's committed offset
        extent, resolved from manifest statistics at query time (§7, I3). The
        tiers overlap by design, so the bounds are what make each row appear
        exactly once.

        `include_archive` defaults to whether the archive is load-bearing —
        False for an ordinary local read, which keeps a hot read on local disk
        (I5), and True when the local table holds nothing and the archive is
        the only source of the archived rows.

        Always bound on a LEADING column of `sort_by`. §7 measures a
        non-leading predicate at 119 ms against 13 ms for the same predicate
        with a leading bound.

        Returns a streaming reader rather than a table: a full-window read with
        a 400-byte payload column is 611 ms and proportional to the data, so
        materialising it is the caller's choice to make.
        """
        return self.sql(
            scan_query(
                self.schema().names,
                columns=columns,
                where=where,
                start_offset=start_offset,
                end_offset=end_offset,
            ),
            include_archive=include_archive,
        )

    def sql(
        self, query: str, *, include_archive: bool | None = None
    ) -> pa.RecordBatchReader:
        """Run arbitrary DuckDB SQL against the log, exposed as `log`.

        The escape hatch for what `scan` cannot express. Quote
        `"litelink_offset"` — it is a DuckDB reserved word.
        """
        required = self._archive_required()
        if include_archive is None:
            include_archive = required
        elif not include_archive and required:
            msg = (
                f"{self.root}/{self.name} holds no local files, so the archive is "
                f"the only source of its archived rows — reading without it would "
                f"return the buffer alone and silently omit them"
            )
            raise ValueError(msg)

        if required:
            # Refuses rather than serving short. Not called otherwise: a log
            # with local files does not need the archive to be correct, and a
            # read must not start costing a metadata GET because one exists.
            self._checked_extent()

        try:
            # No lock. `Reader` guards its own connection and `LogTable` its
            # own cache, both briefly.
            return self._reader.query(query, include_archive=include_archive)
        except FileNotFoundError as exc:
            # Narrow: the race where a primary sweeps the metadata JSON between
            # `_checked_extent`'s reload and `_prepare_remote`'s own — two
            # separate GETs. pyiceberg raises `FileNotFoundError` there.
            #
            # Narrow in the other direction too, which was tested rather than
            # assumed: an endpoint cut out from under a live reader raises
            # `OSError` (NETWORK_CONNECTION) and bad credentials raise `OSError`
            # (ACCESS_DENIED). Neither is mislabelled "reassemble".
            #
            # It does NOT cover the archive's data files or manifests. A
            # missing data file surfaces as `HTTPException` inside `execute`,
            # and a missing manifest can abort the process outright — DuckDB's
            # iceberg extension calls `std::terminate` — which is pre-existing
            # and reaches an ordinary local read of an archive too. Neither is
            # catchable here.
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

        **Which tier answers depends on whether the archive is load-bearing.**
        With local files, `sqlite_sequence` is authoritative: it is the offset
        the next append receives, and nothing below it is missing, because the
        local table holds whatever the buffer has sealed. That keeps this a
        cheap SQLite read on the writer's own log.

        With an empty local table it is not authoritative, and this is where a
        followed log bit. `next_offset` reads a sequence its own docstring
        calls "the highest value ever assigned and never lowers", so it keeps
        counting rows the replica no longer carries: a seal taken while
        `_discard_on_seal()` was true deletes its rows and they reach the
        archive only at the next `sync`. In between they are in neither tier.

        Measured on a followed log in that window: it served 864 rows and
        reported 1,501, and a caller using this as a resume cursor skipped 636
        rows permanently once the primary synced. Over-reporting loses data;
        under-reporting only re-delivers. So the answer is taken from the
        tiers that actually hold rows, which is `Log.restore`'s formula.
        """
        if not self._archive_required():
            return self._buffer.next_offset()

        buffered = self._buffer.extent()

        return (
            max(self._checked_extent()[1], 0 if buffered is None else buffered[1]) + 1
        )

    def buffered_rows(self) -> int:
        """Rows durable in the buffer but not yet sealed."""
        extent = self._table.extent()

        return self._buffer.count_above(0 if extent is None else extent[1])

    def table_rows(self) -> int:
        """Rows in the local Iceberg table, from the manifests."""
        self._table.reload()

        return self._table.record_count()

    def table_files(self) -> int:
        """Data files in the local table — what compaction is bringing down."""
        self._table.reload()

        return self._table.file_count()

    def table_extent(self) -> tuple[int, int] | None:
        """`(lo, hi)` offset extent of the local table, from statistics (§7)."""
        self._table.reload()

        return self._table.extent()

    def archived_through(self) -> int:
        """Highest offset the archive is known to hold, 0 if none (§5, I4).

        The log's own cached watermark, not a network read — the same `meta`
        row eviction consults so it can ask a keyed read instead of a round
        trip. On a followed log that row came from the replica, so it describes
        what the PRIMARY had pushed as of the snapshot; `coverage()` is the one
        that asks the bucket.
        """
        recorded = self._buffer.get_meta(Maintenance.ARCHIVED_KEY)

        return 0 if recorded is None else int(recorded)

    def archive_files(self) -> int:
        """How many data files the archive holds, or 0 with no archive.

        A network round trip, unlike `table_files()`.
        """
        archive = self._archive.table()
        if archive is None:
            return 0

        archive.reload()

        return len(archive.data_files())

    def coverage(self) -> Coverage:
        """What this reader can serve, and where it cannot.

        A reader assembled from a replica cannot ask the primary anything, so
        it reports rather than adjudicates — the failure to avoid is SILENCE,
        not incompleteness.

        **A gap is not necessarily loss.** The gap sits above the archive's
        frontier and below the next thing this knows of: the buffer's first
        offset when it holds rows, and the sequence's end when it does not. A
        range there is either a band the buffer lost, since rows sealed while
        `wal_replication` was off are discarded at seal and gone, or a
        `Log.restore` fence, which burns 2**20 offsets in exactly that
        position. Nothing distinguishes them locally: the fence "leaves no
        trace once the sequence has moved". A caller who knows whether their
        log has failed over can read a gap that this cannot.

        A `start_offset` reserve is NOT among them. It lies BELOW the archive's
        low end, which nothing here compares against, so it never surfaces.

        **An EMPTY buffer is the case that must not report `gap=None`**, and an
        earlier version did: the comparison was against `buffered[0]`, so with
        no buffered rows there was no lower bound and the whole band above the
        archive went unexamined — a reader serving 864 rows of 1,500 reporting
        itself gap-free. The sequence closes it, because it counts every offset
        ever assigned and never lowers.

        What this does NOT model is a second, disjoint band above the buffer's
        own top: `gap` is one range, and it reports the one adjoining the
        archive.
        """
        archive = self._archive_extent()
        buffered = self._buffer.extent()
        gap: tuple[int, int] | None = None
        if archive is not None:
            self._table.reload()
            # ALL THREE TIERS. This arrived for a followed log, whose local
            # table is empty by construction, so "archive frontier to the
            # buffer's first offset" described everything it could serve. A
            # local log has a third tier in between, and leaving it out
            # reported every offset sealed-but-not-yet-synced as unservable —
            # measured, a healthy log with `archive=(1, 432)` and a local table
            # of `(1, 10000)` reported `gap=(433, 10000)` while serving all
            # 9,568 of those rows. Sync lagging the table is the design rather
            # than an error, so that was wrong on nearly every archived log.
            starts = [
                extent[0]
                for extent in (self._table.extent(), buffered)
                if extent is not None
            ]
            above = min(starts) if starts else self._buffer.next_offset()
            if above > archive[1] + 1:
                gap = (archive[1] + 1, above - 1)

        return Coverage(
            archive=archive,
            buffered=buffered,
            gap=gap,
            wal_replication=self.config.wal_replication,
        )

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        """Release handles, then remove the scratch root if this made one."""
        try:
            self._reader.close()
            self._buffer.close()
        finally:
            owned, self._owned_root = self._owned_root, None
            if owned is not None:
                owned.cleanup()

    def __enter__(self) -> LogReader:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # -- internals ---------------------------------------------------------

    def _archive_required(self) -> bool:
        """Whether the archive is the only source of this log's archived rows.

        True when the local table holds nothing and the archive holds
        something. Both halves are asked directly, and the ORDER is what keeps
        a hot read local: the table is checked first, from manifest statistics
        already on disk, so a log with local files answers False without
        touching the network. I5 protects a read that HAS local disk to serve;
        one with none faces a metadata GET or silently wrong results.

        **Do not reintroduce a local proxy for "the archive holds rows". This
        has now been wrong three times, each time in the silent direction.**

        - Keying on the empty table alone demanded an archive from a log
          created with one and never synced, refusing a first read that should
          simply have served the buffer.
        - Adding `archived_through() > 0` fixed that and broke re-pointing:
          `_repoint` deliberately zeroes that watermark when the location
          moves, and a detach-and-reattach is two moves, so a log returning to
          the archive it just left reads 0 while the bucket still holds
          everything. Measured on an evicted log: `scan()` returned 550 of
          4,000 rows with no error, and `coverage()` called it gap-free.
        - Replacing it with a durable marker `follow` wrote made the local
          eviction case unreachable, which hid a real defect behind what looked
          like a design boundary.

        The archive is the authority on what the archive holds. `table()` uses
        the cached handle rather than forcing a reload, so the cost is one
        metadata resolve the first time a reader with an empty table asks — and
        `_archive_extent` still forces its own re-read where staleness is the
        thing being detected.
        """
        self._table.reload()
        if self._table.extent() is not None:
            return False

        if not self._archive.configured():
            return False

        adopted = self._archive.table(repair=False)

        return adopted is not None and adopted.extent() is not None

    def _archive_extent(self) -> tuple[int, int] | None:
        """The archive's extent, forcing a real re-read, or None if unreachable.

        **The re-read is not optional.** On a followed log the adopted metadata
        is pinned, and both caches short-circuit: `Archive.table` on a live
        handle and `LogTable.extent` on an unchanged `metadata_location`. So
        without `reload()` this is an in-memory replay that never touches the
        network — and `coverage()` would go on reporting a healthy, gap-free
        reader against a snapshot the primary had already swept, which is the
        honesty guarantee lying.

        The archive carries `previous-versions-max: 10` — ten previous versions
        beside the current one — so the ELEVENTH further archive commit deletes
        a pinned object, with no time component at all. Archive commits are
        what count: `sync`, `rewrite_archive`, archive expiry. A local
        `maintain()` moves nothing in the bucket.

        On a log whose own writer owns the archive this cannot bite: `sync`
        moves the `archive.db` row, so the re-read finds the current pointer.
        """
        try:
            adopted = self._archive.table(repair=False)
            if adopted is None:
                return None

            adopted.reload()

            return adopted.extent()
        except FileNotFoundError as exc:
            raise self._swept(exc) from exc

    def _checked_extent(self) -> tuple[int, int]:
        """The archive's extent, or a refusal — for when it is load-bearing.

        Two failures, and neither may be silent. An unreachable archive would
        otherwise fall through to the buffer leg, which on an empty local table
        is every archived row missing with no error; and an archive reporting
        no extent has nothing for this to merge.
        """
        extent = self._archive_extent()
        if extent is None:
            msg = (
                f"{self.root}/{self.name} can no longer reach its archive, and it "
                f"holds no local files — reading on would omit every archived row. "
                f"Reassemble with `follow`"
            )
            raise RuntimeError(msg)

        return extent

    def _swept(self, exc: FileNotFoundError) -> RuntimeError:
        return RuntimeError(
            f"this reader's archive snapshot has been swept — the writer has "
            f"committed past it ({exc}). A followed log is a snapshot, not a "
            f"subscription: reassemble with `follow`"
        )


def _intent(name: str, type_: pa.DataType) -> str:
    """The record a schema change writes before it acts (I16).

    The Arrow type travels with the name because recovery has to compare it:
    `union_by_name` is idempotent in a way that also swallows a NARROWING —
    given an existing `n: long` and a passed `n: int32` it calls `promote()`,
    succeeds, and changes nothing. Without the type here the replay could not
    tell that apart from the change it meant to finish.
    """
    return json.dumps(
        {
            "add": name,
            "type": pa.schema([pa.field(name, type_)]).serialize().to_pybytes().hex(),
        }
    )


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
