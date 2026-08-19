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

from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence
    from os import PathLike
    from types import TracebackType
    from typing import Self

    import pyarrow as pa

    Row = Mapping[str, object]


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
        raise NotImplementedError

    # -- write ------------------------------------------------------------

    def append(self, row: Row) -> int:
        """Append one row. Returns the assigned offset.

        Durable when this returns — one SQLite transaction, `synchronous=FULL`
        (§3). There is no in-memory write buffer to flush, and that absence is
        the point: it is the failure the README opens with.

        A caller-supplied `offset` is rejected (I11).
        """
        raise NotImplementedError

    def extend(self, rows: Iterable[Row]) -> list[int]:
        """Append many rows in ONE transaction. Returns the assigned offsets.

        The batch is the durability unit: one fsync amortised across the batch,
        which is the whole of §3's throughput story. It carries no meaning
        beyond that — see §1 on why there is no transaction id column.
        """
        raise NotImplementedError

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
        raise NotImplementedError

    def sql(self, query: str, *, include_archive: bool = False) -> pa.RecordBatchReader:
        """Run arbitrary DuckDB SQL against the log, exposed as `log`.

        The escape hatch for what `scan` cannot express. The relation is built
        per call and cannot be held across calls: every commit writes a new
        metadata JSON, so a cached pointer silently serves a stale snapshot
        (§7). Quote `"offset"` — it is a DuckDB reserved word.
        """
        raise NotImplementedError

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
        """
        raise NotImplementedError

    # -- maintenance ------------------------------------------------------

    def seal(self) -> int | None:
        """Force a seal now; returns the exclusive end offset, or None if empty.

        Normally automatic at `min(target_size, max_age)` (§4). Explicit seals
        are for shutdown and tests.
        """
        raise NotImplementedError

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
        raise NotImplementedError

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
        """Release the buffer and catalog handles. Does not seal."""
        raise NotImplementedError

    def __enter__(self) -> Self:
        raise NotImplementedError

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        raise NotImplementedError
