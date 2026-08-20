"""Local storage reclamation: compact, evict, expire, drain (SPEC §6, §8, §12).

Separate from the write path because it shares nothing with it but the tables it
reads, and separate from `Log` because it is the half of the library with no
opinion about appends.

The four run in order and none is useful alone. Compaction alone INCREASES
storage, since superseded files stay referenced until their snapshots expire.
Eviction alone frees no disk, since it removes a file from the current snapshot
while the previous one still references it. Expiry deletes no files at all —
pyiceberg's is metadata-only. Draining is what actually unlinks, and it waits
`snapshot_retention` so a running scan does not lose files underneath it (I6).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pyarrow.parquet as pq

from litelink._fs import fsync

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    import pyarrow as pa

    from litelink._buffer import Buffer
    from litelink._layout import Layout
    from litelink._table import DataFile, LogTable
    from litelink.log import LogConfig


def _checkpoint(heartbeat: Callable[[], bool] | None) -> None:
    """Renew the caller's claim, or refuse to carry on without it.

    Losing the role mid-pass is not something to push through: another owner
    is already redoing this work, and two of them writing the same table is
    what the lease exists to prevent.
    """
    if heartbeat is not None and not heartbeat():
        msg = "lost the maintenance lease mid-pass"
        raise RuntimeError(msg)


class Maintenance:
    """The reclamation passes for one log."""

    def __init__(
        self,
        table: LogTable,
        buffer: Buffer,
        layout: Layout,
        config: LogConfig,
        sort_by: tuple[str, ...],
    ) -> None:
        self._table = table
        self._buffer = buffer
        self._layout = layout
        self._config = config
        self._sort_by = sort_by

    def set_config(self, config: LogConfig) -> None:
        """Adopt new policy in place, rather than being rebuilt around it."""
        self._config = config

    def set_sort_by(self, sort_by: tuple[str, ...]) -> None:
        self._sort_by = sort_by

    def run(self, heartbeat: Callable[[], bool] | None = None) -> None:
        """The three passes, with a checkpoint between them.

        `heartbeat` renews the caller's claim and reports whether it still
        holds it. A pass is long — a compaction of 540 files measured 20 s
        against a 30 s lease — so without one, a second maintainer can take the
        role mid-pass and start compacting the same runs. Both would write the
        same deterministic output path, which is a torn file rather than a
        conflict Iceberg could resolve.

        Between phases rather than inside them: it bounds the exposure to a
        single phase without threading a callback through every loop, and a
        phase that runs long enough to matter is a reason to raise the TTL, not
        to check more often.
        """
        self.compact(heartbeat)
        _checkpoint(heartbeat)
        self.evict()
        _checkpoint(heartbeat)
        self.expire()
        _checkpoint(heartbeat)

    # -- compaction ---------------------------------------------------------

    def compact(self, heartbeat: Callable[[], bool] | None = None) -> None:
        """Merge runs of undersized adjacent files (§6).

        Required, not opportunistic: the `max_age` seal branch guarantees a
        quiet stream emits a small file every interval indefinitely.

        The table is unpartitioned, so the compaction unit is a contiguous
        offset range — safe precisely because sealed files already cover
        contiguous, non-overlapping ranges, so the range filter selects exactly
        the sources and nothing else.
        """
        # Reloaded first, like every other pass. A handle predating another
        # owner's eviction still lists the files it removed — they are unlinked
        # only after the grace period, so they are readable — and merging them
        # re-adds the rows. `_commit` makes that land: its first attempt fails
        # against the moved branch, then it reloads and retries the swap on the
        # FRESH table, committing evicted data back into the log.
        self._table.reload()

        threshold = self._config.compact_below or self._config.target_size // 2

        run: list[DataFile] = []
        for data_file in [*self._table.data_files(), None]:
            # Adjacency is in offset order, so a large file between two small
            # ones ends the run — merging across it would pull an already-sized
            # file through the rewrite for nothing.
            if data_file is not None and data_file.size < threshold:
                run.append(data_file)
                continue

            if len(run) >= self._config.compact_min_files:
                self._compact_run(run, heartbeat)

            run = []

    def _compact_run(
        self, run: list[DataFile], heartbeat: Callable[[], bool] | None = None
    ) -> None:
        lo, hi = run[0].lo, run[-1].hi
        # Unique per attempt. See `compaction_path`: a fixed name made a
        # rewrite of a previous compaction write over the file it was reading.
        rel_path = self._layout.compaction_path(lo, hi, uuid.uuid4().hex[:8])
        # Claimed before the file exists, exactly as a seal claims its path
        # (I2). A compaction that dies between the write and the commit is then
        # recoverable by name, instead of being a file nobody can identify
        # without listing the directory.
        self._buffer.claim_compaction(lo, hi, rel_path)
        self._write_merge(run, rel_path, heartbeat)
        self._buffer.clear_compaction()

    def _write_merge(
        self,
        run: list[DataFile],
        rel_path: str,
        heartbeat: Callable[[], bool] | None = None,
    ) -> None:
        lo, hi = run[0].lo, run[-1].hi
        merged = self._table.scan_range(lo, hi)
        if self._sort_by:
            # Re-sorted, not merely concatenated: concatenation would leave the
            # row groups carrying each source file's range, which is the
            # statistic the sort exists to tighten.
            merged = merged.sort_by([(c, "ascending") for c in self._sort_by])

        _verify(merged, run, lo, hi)

        dest = self._layout.absolute(rel_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(merged, dest)
        fsync(dest)

        # Checked between writing and committing, because those are the two
        # halves a lapsed lease separates. A run outlasting the TTL lets
        # another owner recover — unlinking the output this claimed — and the
        # commit would then land anyway, leaving the table pointing at a file
        # that no longer exists while the sources it superseded drain away.
        _checkpoint(heartbeat)

        # Queued BEFORE the commit that supersedes them, not after. A crash in
        # between used to lose the only record of these paths — recovery clears
        # the compaction claim without re-deriving its sources, so nothing
        # could name them again. Queueing first is safe because `drain` refuses
        # to delete anything the table still references, so an entry made for a
        # commit that never lands simply never comes due.
        #
        # Superseded, not yet deletable either way: a scan that started before
        # this commit is still reading them (I6).
        self._enqueue(f.path for f in run)
        self._table.replace_range(lo, hi, str(dest))

    def rewrite_sorted(self, heartbeat: Callable[[], bool] | None = None) -> None:
        """Re-cluster every data file under the current sort order (§7).

        File boundaries are preserved rather than merged: a rewrite is already
        the expensive operation, and folding compaction into it would change
        the file layout at the same time as the clustering, leaving no way to
        attribute a later regression to either.

        Each file goes through the same claim-write-replace path a compaction
        uses, so a crash mid-rewrite leaves one named file to remove and a
        table still holding the original.
        """
        # Between files, for the same reason `run` checkpoints between phases:
        # this rewrites the WHOLE table, which outlasts a 30 s lease long
        # before it outlasts a user's patience. Per file rather than per pass,
        # because a pass here has no phases to sit between.
        for data_file in self._table.data_files():
            self._compact_run([data_file])
            _checkpoint(heartbeat)

    # -- eviction -----------------------------------------------------------

    def evict(self) -> None:
        """Drop files older than `local_retention` from the local table (§8).

        Age comes from the snapshot that added the file, not from any data
        column — the library stamps no timestamp (§2).

        With no archive this is deletion of the only copy. That is the contract
        a local-only log with a retention asks for; see §8.
        """
        retention = self._config.local_retention
        if retention is None:
            return

        # Same reason as `compact`: this decides what to drop from the ages a
        # handle reports, and a stale one reports a table that has moved.
        self._table.reload()

        cutoff = datetime.now(UTC) - retention
        expired = {
            path
            for path, added in self._table.snapshot_ages().items()
            if added.replace(tzinfo=UTC) < cutoff
        }
        stale = [f for f in self._table.data_files() if f.path in expired]
        if not stale:
            return

        # Files cover contiguous non-overlapping ranges, so evicting a prefix is
        # a single upper bound. Anything newer is untouched.
        boundary = max(f.hi for f in stale)
        # Everything the boundary REMOVES, not just what looked old enough to
        # trigger it. A compaction output has a fresh snapshot age, so it never
        # appears in `stale` — but its offsets can sit below a stale file's
        # `hi`, so the boundary drops it too. Queueing only `stale` left it
        # removed from the table and named by nothing.
        self._enqueue(f.path for f in self._table.data_files() if f.hi <= boundary)
        self._table.evict_through(boundary)

    # -- expiry and the deletion queue --------------------------------------

    def expire(self) -> None:
        """Expire snapshots past `snapshot_retention`, then reclaim (§6, §8)."""
        cutoff = datetime.now(UTC) - self._config.snapshot_retention

        # Collect the doomed snapshots' manifest lists and manifests BEFORE
        # expiring them. Afterwards their names exist nowhere: the metadata that
        # referenced them is gone, and the only remaining way to find the files
        # would be to list the directory — the thing this design refuses to do.
        doomed = self._table.metadata_paths(self._table.snapshots_older_than(cutoff))

        # Queued BEFORE the expiry, like every other supersession here. After
        # it, these names exist nowhere — the metadata that referenced them is
        # gone — so a crash between the two left files only a directory scan
        # could find, which is the one thing this design refuses.
        #
        # Unfiltered, therefore. The filter used to run after expiry, when
        # `referenced_paths` had stopped counting the doomed snapshots; asked
        # beforehand it counts them all and would queue nothing. `drain`'s veto
        # does the same job later and does it repeatedly: a manifest shared with
        # a live snapshot is skipped every pass until that snapshot expires too,
        # and then retired. The cost is queue rows that wait, against files that
        # could not be found at all.
        self._enqueue(doomed)
        self._table.expire_snapshots_older_than(cutoff)
        self.drain()

    def drain(self) -> None:
        """Delete files whose grace period has passed.

        A keyed read of `pending_delete`, not a directory walk. Every file this
        library creates has its path written to SQLite before it is written to
        disk — seals through `sealing`, compactions through `compacting` — so
        there is no category of file that could only be found by looking. That
        matters more the moment this points at object storage, where the walk is
        a paginated LIST that costs money and can lag reality.
        """
        # Read against the CURRENT snapshot_retention, so lowering it takes
        # effect on files already queued.
        cutoff = datetime.now(UTC) - self._config.snapshot_retention
        due = self._buffer.due_deletions(int(cutoff.timestamp()))
        if not due:
            return

        # Reloaded first. This veto is the last thing standing between the
        # deletion queue and an unrecoverable mistake, and asked of a handle
        # that predates another process's commit it reports a live file as
        # unreferenced. Every other cost in this pass dwarfs a catalog resolve.
        self._table.reload()
        referenced = self._table.referenced_paths()
        for rel_path in due:
            path = self._layout.absolute(rel_path)
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

    def _enqueue(self, paths: Iterable[str]) -> None:
        self._buffer.enqueue_deletions(
            (self._layout.relative(p) for p in paths),
            int(datetime.now(UTC).timestamp()),
        )


def _verify(merged: pa.Table, run: list[DataFile], lo: int, hi: int) -> None:
    """§6 step 3, as far as it can be taken.

    Row count and the offset extent are checked exactly; both are what the
    overwrite's safety argument rests on. Per-column min/max is NOT checked and
    cannot be by equality: Iceberg truncates string and binary bounds, so a
    source bound is a prefix rather than a value and would compare unequal to a
    correct merge.
    """
    expected = sum(f.rows for f in run)
    if merged.num_rows != expected:
        msg = f"compaction would lose rows: {merged.num_rows} != {expected}"
        raise RuntimeError(msg)

    # Python's min/max over the materialised column, not pyarrow.compute: pc's
    # kernels are generated from a runtime registry, so no static checker can
    # see them. §6 step 2 already holds the whole merge in memory.
    offsets = merged["litelink_offset"].to_pylist()
    if min(offsets) != lo or max(offsets) != hi:
        msg = "compaction changed the offset extent"
        raise RuntimeError(msg)
