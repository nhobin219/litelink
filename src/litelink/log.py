"""The core append-only stream.

A `Log` is one stream: one SQLite buffer, one local Iceberg table, and
optionally one archive table (SPEC §1). Rows are durable at commit and
queryable immediately; `offset` is assigned by the library and is the only
column it owns (§2, I11).

Core library only. The blob-field extension (§15) is deliberately absent —
applications that need a small binary column declare an ordinary `binary`
column in their own schema, which §15.2 already says is the right route for
payloads that fit comfortably in the buffer.

Nothing here is implemented. The signatures and the contracts in the
docstrings are the spec's API surface made concrete — read them as the design,
not as documentation of working code.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.conversions import from_bytes

from litelink._buffer import Buffer
from litelink._predicates import offset_at_or_below, offset_between

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence
    from os import PathLike
    from types import TracebackType
    from typing import Self

    Row = Mapping[str, object]

_NAMESPACE = "litelink"


@dataclass(frozen=True, slots=True)
class _DataFile:
    """One Iceberg data file, as the maintenance passes need to see it."""

    path: str
    size: int
    rows: int
    lo: int
    hi: int


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
    # may eventually need `min(target_size, target_rows, max_age)` — deliberately
    # not added now, on one knob until a real workload demands the second.
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


class Log:
    """One append-only stream.

    Single writer per log (§1): SQLite's write lock is per file, and one
    process per stream is the intended topology.

    Opening runs recovery — §4's `sealing` replay, which is idempotent — so a
    crashed process is repaired by the next open rather than by an operator.
    """

    def __init__(
        self,
        root: PathLike[str] | str,
        name: str,
        *,
        schema: pa.Schema,
        sort_by: Sequence[str],
        config: LogConfig | None = None,
        archive: str | None = None,
    ) -> None:
        """Open (or create) the log rooted at `root`.

        `schema` is the application's columns. The library adds `offset` and
        owns nothing else — no ingest timestamp, no transaction id (§2).

        `sort_by` is required on purpose. §7 measures it as a read-shape
        decision, not a tuning knob: it declares which predicates prune, only a
        LEADING column prunes, and changing it later means rewriting the data.
        A capture workload usually wants `("event_ts", "key")`.

        `archive` is the remote warehouse prefix (e.g. `s3://bucket/prefix`).
        None means local-only: capture, seal, compaction, retention and reads
        all work with no network, forever (§11). On a local-only log `sync` is
        an error and `maintain` is the whole storage story — see both.
        """
        if "offset" in schema.names:
            msg = "`offset` is owned by the library and must not be in the schema (I11)"
            raise ValueError(msg)

        missing = [c for c in sort_by if c not in schema.names]
        if missing:
            msg = f"sort_by names columns not in the schema: {missing}"
            raise ValueError(msg)

        self.name = name
        self.root = Path(root)
        self.config = config or LogConfig()
        self._archive = archive
        self._sort_by = tuple(sort_by)
        self._schema = schema

        if self._archive is None and self.config.local_retention == timedelta(0):
            msg = (
                "local_retention=0 means 'evict on upload' and presupposes an "
                "archive; with archive=None it would delete each file as it sealed"
            )
            raise ValueError(msg)

        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / name).mkdir(parents=True, exist_ok=True)

        # `offset` leads the table schema; everything after it is the caller's
        # and is treated as opaque (§2).
        self._table_schema = pa.schema(
            [pa.field("offset", pa.int64(), nullable=False), *schema]
        )
        self._catalog = SqlCatalog(
            "local",
            uri=f"sqlite:///{self.root / 'catalog.db'}",
            warehouse=f"file://{self.root}",
        )
        self._catalog.create_namespace_if_not_exists(_NAMESPACE)
        try:
            self._table = self._catalog.load_table(f"{_NAMESPACE}.{name}")
        except Exception:
            self._table = self._catalog.create_table(
                f"{_NAMESPACE}.{name}", schema=self._table_schema
            )

        self._buffer = Buffer(self.root / name / "buffer.db", schema)
        self._duck: duckdb.DuckDBPyConnection | None = None
        self._recover()

    # -- write ------------------------------------------------------------

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
        offsets = self._buffer.append(rows)
        self._maybe_seal()

        return offsets

    # -- read -------------------------------------------------------------

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

        Always bound on a LEADING column of `sort_by`. §7 measures a
        non-leading predicate at 119 ms against 13 ms for the same predicate
        with a leading bound — the column is in the sort key and still does not
        prune on its own.

        Returns a streaming reader rather than a table: a full-window read with
        a 400-byte payload column is 611 ms and proportional to the data, so
        materialising it is the caller's choice to make.
        """
        projection = ", ".join(f'"{c}"' for c in (columns or self._table_schema.names))
        predicates = [where] if where else []
        if start_offset is not None:
            predicates.append(f'"offset" >= {int(start_offset)}')

        if end_offset is not None:
            predicates.append(f'"offset" < {int(end_offset)}')

        return self.sql(
            f"SELECT {projection} FROM log"
            + (
                f" WHERE {' AND '.join(f'({p})' for p in predicates)}"
                if predicates
                else ""
            )
            + ' ORDER BY "offset"',
            include_archive=include_archive,
        )

    def sql(self, query: str, *, include_archive: bool = False) -> pa.RecordBatchReader:
        """Run arbitrary DuckDB SQL against the log, exposed as `log`.

        The escape hatch for what `scan` cannot express. The relation is built
        per call and cannot be held across calls: every commit writes a new
        metadata JSON, so a cached pointer silently serves a stale snapshot
        (§7). Quote `"offset"` — it is a DuckDB reserved word.
        """
        if include_archive:
            msg = "archive reads are not implemented"
            raise NotImplementedError(msg)

        connection = self._duckdb()
        # Rebuilt per call, never cached. Every commit writes a new metadata
        # JSON, so a view holding yesterday's pointer serves a stale snapshot —
        # and the boundary below must come from the SAME metadata the scan
        # reads, or a seal landing between the two double-counts the gap (I3).
        connection.execute(f"CREATE OR REPLACE TEMP VIEW log AS {self._union_sql()}")

        return connection.execute(query).to_arrow_reader()

    def _union_sql(self) -> str:
        """The §7 hot read: local table, plus the buffer above its extent."""
        columns = tuple(self._table_schema.names)
        # Aliased back to the bare column name: without it the buffer-only leg
        # (nothing sealed yet) exposes columns called `CAST(b."offset" AS
        # BIGINT)`, and only the presence of an Iceberg leg to name them first
        # hides it.
        buffer_side = ", ".join(
            f'b."{c}"::{_DUCKDB_TYPES[str(self._table_schema.field(c).type)]} AS "{c}"'
            for c in columns
        )
        # Cast the buffer side explicitly rather than letting UNION ALL
        # reconcile: SQLite's per-value typing comes through the scanner
        # loosely, and a column that holds integers in every row can still
        # surprise the union (§7).
        buffer_leg = f"SELECT {buffer_side} FROM buf.buffer b"

        extent = self.table_extent()
        if extent is None:
            # Nothing sealed yet, so there is no boundary to derive and no
            # table to union — every row is still in the buffer.
            return buffer_leg

        projection = ", ".join(f'"{c}"' for c in columns)

        return (
            f"SELECT {projection} FROM iceberg_scan('{self._table.metadata_location}')"
            f' UNION ALL {buffer_leg} WHERE b."offset" > {extent[1]}'
        )

    def _duckdb(self) -> duckdb.DuckDBPyConnection:
        if self._duck is None:
            connection = duckdb.connect()
            # Provisioned, not autoinstalled — see scripts/install_duckdb_extensions.py
            # and §7 on why the first read must not be a network read.
            connection.execute("LOAD iceberg")
            connection.execute("LOAD sqlite")
            connection.execute(
                f"ATTACH '{self.root / self.name / 'buffer.db'}' AS buf "
                "(TYPE sqlite, READ_ONLY)"
            )
            self._duck = connection

        return self._duck

    def end_offset(self) -> int:
        """The offset the next append will receive — an EXCLUSIVE upper bound.

        Half-open, matching the `[start, end)` seal ranges in §4, so
        `end_offset()` on a fresh log is the first offset it will ever assign
        and never a sentinel.

        A method rather than a property because it is not free: it resolves the
        catalog and reads the buffer's maximum (~0.6 ms in §7's measurements).

        This is the log's end, NOT §7's tier boundary `hi`, which is the local
        Iceberg table's committed maximum and excludes everything still in the
        buffer. A consumer resuming from here sees every durable row; one
        resuming from `hi` silently skips the unsealed tail.

        Cheaper than the design assumed: AUTOINCREMENT's `sqlite_sequence` is
        the highest offset ever assigned and survives the buffer emptying, so
        no catalog resolve is needed at all.
        """
        return self._buffer.next_offset()

    # -- maintenance ------------------------------------------------------

    def seal(self) -> int | None:
        """Force a seal now; returns the exclusive end offset, or None if empty.

        Normally automatic at `min(target_size, max_age)` (§4). Explicit seals
        are for shutdown and tests.
        """
        extent = self._buffer.extent()
        if extent is None:
            return None

        start, last = extent
        end = last + 1
        rel_path = self._seal_path(start, end)
        # I2: the range and its path are fixed BEFORE the file exists, so a
        # retry recomputes nothing and overwrites in place rather than
        # stranding the first attempt under a different name.
        self._buffer.claim_seal(start, end, rel_path)
        self._write_and_commit(end, rel_path)
        self._buffer.finish_seal(end)

        return end

    def _seal_path(self, start: int, end: int) -> str:
        day = datetime.now(UTC).date().isoformat()

        return f"{self.name}/data/{day}/{start}-{end}.parquet"

    def _write_and_commit(self, end: int, rel_path: str) -> None:
        """Write the Parquet file, fsync it, then commit it to the table.

        I1 in this order: committing first would publish a manifest entry for
        a file that may not survive the crash.
        """
        rows = self._buffer.rows_below(end)
        if self._sort_by:
            # §4: the sort order is declared as table metadata AND applied here.
            # Metadata records intent; it does not sort for you.
            rows = rows.sort_by([(c, "ascending") for c in self._sort_by])

        dest = self.root / rel_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(rows, dest)
        _fsync(dest)

        self._table.add_files([str(dest)])
        self._reload()

    def _maybe_seal(self) -> None:
        """Seal when the buffer crosses `target_size`.

        The `max_age` branch is not wired up: it needs a clock the caller does
        not drive, and §4 evaluates both triggers at commit time. Sealing on
        age therefore has to be driven by the application calling `seal()`
        until this grows a timer.
        """
        extent = self._buffer.extent()
        if extent is None:
            return

        if self._buffer.byte_size() >= self.config.target_size:
            self.seal()

    def _recover(self) -> None:
        """Finish an interrupted seal (§4).

        Idempotent in both directions: if the commit landed, only the buffer
        delete is outstanding; if it did not, the whole file is rewritten to
        the same path.
        """
        self._recover_compaction()

        pending = self._buffer.pending_seal()
        if pending is None:
            return

        _, end, rel_path = pending
        if str(self.root / rel_path) in self._file_paths():
            self._buffer.finish_seal(end)
            return

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
        if str(self.root / rel_path) not in self._file_paths():
            (self.root / rel_path).unlink(missing_ok=True)

        self._buffer.clear_compaction()

    def _file_paths(self) -> set[str]:
        snapshot = self._table.current_snapshot()
        if snapshot is None:
            return set()

        paths = self._table.inspect.files()["file_path"].to_pylist()

        return {p.removeprefix("file://") for p in paths}

    def table_extent(self) -> tuple[int, int] | None:
        """`(lo, hi)` offset extent of the local table, from statistics.

        §7's tier boundary. Read from manifest column statistics — no data file
        is opened, which is what makes it ~0.6 ms rather than a scan.
        """
        snapshot = self._table.current_snapshot()
        if snapshot is None:
            return None

        files = self._table.inspect.files()
        if files.num_rows == 0:
            return None

        field = self._table.schema().find_field("offset")
        lows = [
            from_bytes(field.field_type, dict(b)[field.field_id])
            for b in files["lower_bounds"].to_pylist()
        ]
        highs = [
            from_bytes(field.field_type, dict(b)[field.field_id])
            for b in files["upper_bounds"].to_pylist()
        ]

        return (min(lows), max(highs))

    def sync(self) -> None:
        """Push to the archive: upload, register, replicate compactions (§5).

        Archive-facing work only. Lazy, restartable, and arbitrarily far
        behind — no read depends on it. Raises if no archive is configured;
        with `archive=None` there is nothing this could do.

        DEVIATES from §5, which also lists snapshot expiry (step 4) and local
        eviction (step 5). Both are local storage work and belong to
        `maintain`; leaving them here makes `local_retention` silently inert on
        a local-only log, because every step of §5 is archive work and the
        whole pass is skipped. Sync's remaining obligation to eviction is the
        registration watermark it records in `meta`, which is what lets
        `maintain` enforce I4.
        """
        raise NotImplementedError

    def maintain(self) -> None:
        """Reclaim local storage: compact, evict, expire (§6, §8, §12).

        Runs with or without an archive — this is the call that makes
        `local_retention` mean something on a local-only log.

        The three go together and in this order. Compaction alone INCREASES
        storage, since superseded files stay referenced until their snapshots
        expire (§12); eviction drops files from the current snapshot but frees
        no disk on its own; expiry last is what actually deletes bytes, and it
        holds `snapshot_retention` back so a running scan does not lose files
        underneath it (I6).

        **Eviction is bounded by I4 when an archive is configured**: a file
        that sync has not yet registered is never evicted, however old. So on a
        partitioned-off machine, this compacts and expires but leaves the
        window growing — §11's "local eviction stalls".

        **With no archive, eviction is deletion.** I4 is vacuous because
        nothing is owed to an archive, so `local_retention` becomes an ordinary
        retention policy and data past it is gone for good. That is the
        contract a local-only log with a retention asks for; `None` keeps
        everything and grows without bound.

        Whether a stalled or partial pass should be reported rather than
        silent is open — §11 treats stalled eviction as an operational
        condition, and returning None says nothing about it.
        """
        if self._archive is not None:
            # I4 needs sync's registration watermark to decide what is safe to
            # evict, and sync does not exist yet. Refusing beats evicting a file
            # no archive has — that failure is silent and permanent.
            msg = "maintain() with an archive configured requires sync(), which is not implemented"
            raise NotImplementedError(msg)

        self._compact()
        self._evict()
        self._expire()

    def _data_files(self) -> list[_DataFile]:
        """Current data files with their offset extents, ordered by offset."""
        if self._table.current_snapshot() is None:
            return []

        files = self._table.inspect.files()
        field = self._table.schema().find_field("offset")
        out = [
            _DataFile(
                path=str(path).removeprefix("file://"),
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

        return sorted(out, key=lambda f: f.lo)

    def _compact(self) -> None:
        """Merge runs of undersized adjacent files (§6).

        Required, not opportunistic: the `max_age` seal branch guarantees a
        quiet stream emits a small file every interval indefinitely.

        The table is unpartitioned, so the compaction unit is a contiguous
        offset range — which is safe precisely because sealed files already
        cover contiguous, non-overlapping ranges, so the range filter selects
        exactly the sources and nothing else.
        """
        threshold = self.config.compact_below or self.config.target_size // 2

        run: list[_DataFile] = []
        for data_file in [*self._data_files(), None]:
            # Adjacency is in offset order, so a large file between two small
            # ones ends the run — merging across it would pull an already-sized
            # file through the rewrite for nothing.
            if data_file is not None and data_file.size < threshold:
                run.append(data_file)
                continue

            if len(run) >= self.config.compact_min_files:
                self._compact_run(run)

            run = []

    def _compact_run(self, run: list[_DataFile]) -> None:
        lo, hi = run[0].lo, run[-1].hi
        rel_path = self._compaction_path(lo, hi)
        # Claimed before the file exists, exactly as a seal claims its path
        # (I2). A compaction that dies between the write and the commit is then
        # recoverable by name, instead of being a file nobody can identify
        # without listing the directory.
        self._buffer.claim_compaction(lo, hi, rel_path)
        self._compact_claimed(run, rel_path)
        self._buffer.clear_compaction()

    def _compaction_path(self, lo: int, hi: int) -> str:
        return f"{self.name}/data/compacted/{lo}-{hi}.parquet"

    def _compact_claimed(self, run: list[_DataFile], rel_path: str) -> None:
        lo, hi = run[0].lo, run[-1].hi
        offset_range = offset_between(lo, hi)
        merged = self._table.scan(row_filter=offset_range).to_arrow()
        if self._sort_by:
            # Re-sorted, not merely concatenated: concatenation would leave the
            # row groups carrying each source file's range, which is the
            # statistic the sort exists to tighten.
            merged = merged.sort_by([(c, "ascending") for c in self._sort_by])

        # §6 step 3. Row count and the offset extent are checked exactly; both
        # are what the overwrite's safety argument rests on.
        #
        # Per-column min/max is NOT checked, and cannot be by equality: Iceberg
        # truncates string and binary bounds, so a source bound is a prefix
        # rather than a value and would compare unequal to a correct merge.
        expected = sum(f.rows for f in run)
        if merged.num_rows != expected:
            msg = f"compaction would lose rows: {merged.num_rows} != {expected}"
            raise RuntimeError(msg)

        # Python's min/max over the materialised column, not pyarrow.compute:
        # pc's kernels are generated from a runtime registry, so no static
        # checker can see them. §6 step 2 already holds the whole merge in
        # memory, so one int64 column as a list is a rounding error on that.
        offsets = merged["offset"].to_pylist()
        if min(offsets) != lo or max(offsets) != hi:
            msg = "compaction changed the offset extent"
            raise RuntimeError(msg)

        dest = self.root / rel_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(merged, dest)
        _fsync(dest)

        # One snapshot, so readers never observe a gap or a double count (§6) —
        # and the file is one we named, rather than one pyiceberg placed for us.
        # `overwrite()` would do the same job in a single call, but it writes
        # the output itself, which puts a path on disk that this process only
        # learns about afterwards. That is the whole thing being avoided.
        with self._table.transaction() as transaction:
            transaction.delete(delete_filter=offset_range)
            transaction.add_files([str(dest)])

        self._reload()
        # Superseded, not yet deletable: a scan that started before this commit
        # is still reading them (I6).
        self._enqueue(f.path for f in run)

    def _evict(self) -> None:
        """Drop files older than `local_retention` from the local table (§8).

        Age comes from the snapshot that added the file, not from any data
        column — the library stamps no timestamp (§2).

        With no archive this is deletion of the only copy. That is the contract
        a local-only log with a retention asks for; see §8.
        """
        retention = self.config.local_retention
        if retention is None:
            return

        cutoff = datetime.now(UTC) - retention
        committed = {
            int(snapshot_id): committed_at
            for snapshot_id, committed_at in zip(
                self._table.inspect.snapshots()["snapshot_id"].to_pylist(),
                self._table.inspect.snapshots()["committed_at"].to_pylist(),
                strict=True,
            )
        }

        entries = self._table.inspect.entries()
        expired_paths = {
            str(data_file["file_path"]).removeprefix("file://")
            for snapshot_id, data_file in zip(
                entries["snapshot_id"].to_pylist(),
                entries["data_file"].to_pylist(),
                strict=True,
            )
            if (added := committed.get(int(snapshot_id))) is not None
            and added.replace(tzinfo=UTC) < cutoff
        }
        if not expired_paths:
            return

        stale = [f for f in self._data_files() if f.path in expired_paths]
        if not stale:
            return

        # Files cover contiguous non-overlapping ranges, so evicting a prefix is
        # a single upper bound. Anything newer is untouched.
        boundary = max(f.hi for f in stale)
        self._table.delete(delete_filter=offset_at_or_below(boundary))
        self._reload()
        self._enqueue(f.path for f in stale)

    def _enqueue(self, paths: Iterable[str]) -> None:
        """Queue superseded files, deletable once no live snapshot can hold them."""
        self._buffer.enqueue_deletions(
            (str(Path(p).relative_to(self.root)) for p in paths),
            int(datetime.now(UTC).timestamp()),
        )

    def _expire(self) -> None:
        """Expire snapshots past `snapshot_retention` (§6, §8).

        This is what actually deletes bytes — eviction only removed the file
        from the current snapshot. Held back by `snapshot_retention` so a
        running scan does not lose files underneath it (I6).
        """
        cutoff = datetime.now(UTC) - self.config.snapshot_retention
        self._table.maintenance.expire_snapshots().older_than(cutoff).commit()
        self._reload()
        self._drain_deletions()

    def _drain_deletions(self) -> None:
        """Delete files whose grace period has passed (§6, §8).

        A keyed read of `pending_delete`, not a directory walk. Every file this
        library creates has its path written to SQLite before it is written to
        disk — seals through `sealing`, compactions through `compacting` — so
        there is no category of file that could only be found by looking. That
        matters more the moment this points at object storage, where the walk
        is a paginated LIST that costs money and can lag reality.

        **pyiceberg's expire_snapshots is metadata-only.** Verified against
        0.11.1: expiring three snapshots leaves `inspect.all_files()` empty and
        all three Parquet files on disk. So the deletion is ours to do; the
        queue is what makes it cheap.
        """
        # Read against the CURRENT snapshot_retention, so lowering it takes
        # effect on files already queued.
        cutoff = datetime.now(UTC) - self.config.snapshot_retention
        due = self._buffer.due_deletions(int(cutoff.timestamp()))
        if not due:
            return

        referenced = {
            str(path).removeprefix("file://")
            for path in self._table.inspect.all_files()["file_path"].to_pylist()
        }

        for rel_path in due:
            path = self.root / rel_path
            if str(path) in referenced:
                # A compaction can re-register a path the queue still holds.
                # Deleting a referenced file is unrecoverable, so the check is
                # worth its cost even though the grace period should preclude it.
                continue

            # Unlink first, forget second. A crash between them leaves a row
            # whose unlink is already a no-op; the reverse leaks the file with
            # nothing left pointing at it.
            path.unlink(missing_ok=True)
            self._buffer.forget_deletion(rel_path)

    def _reload(self) -> None:
        self._table = self._catalog.load_table(f"{_NAMESPACE}.{self.name}")

    def hydrate(self, since: timedelta) -> None:
        """Re-register archived files into the local table (§8).

        Raising `local_retention` is an operation, not a config change: without
        this, a raised setting applies only to data captured afterwards.
        """
        raise NotImplementedError

    # -- schema evolution -------------------------------------------------

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

    # -- lifecycle --------------------------------------------------------

    def close(self) -> None:
        """Release the buffer and catalog handles. Does not seal.

        Not sealing is deliberate: committed rows are already durable, and an
        implicit seal on close would emit an undersized file every time a
        process restarted.
        """
        if self._duck is not None:
            self._duck.close()
            self._duck = None

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


# Arrow type -> the DuckDB type the buffer leg is cast to.
_DUCKDB_TYPES = {
    "int64": "BIGINT",
    "int32": "INTEGER",
    "int16": "SMALLINT",
    "int8": "TINYINT",
    "double": "DOUBLE",
    "float": "FLOAT",
    "bool": "BOOLEAN",
    "string": "VARCHAR",
    "large_string": "VARCHAR",
    "binary": "BLOB",
    "large_binary": "BLOB",
}


def _fsync(path: Path) -> None:
    """Fsync a file AND the directory entry that reaches it (I1).

    On most filesystems the file contents can be durable while the name is
    not, so syncing only the file leaves a manifest entry pointing at a path
    that may not exist after a crash.
    """
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)

    dir_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)
