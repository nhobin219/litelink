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

from litelink._archive import Archive
from litelink._fs import fsync

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    import pyarrow as pa

    from litelink._buffer import Buffer
    from litelink._layout import Layout
    from litelink._table import DataFile, LogTable
    from litelink.log import LogConfig


def checkpoint(heartbeat: Callable[[], bool] | None) -> None:
    """Renew the caller's claim, or refuse to carry on without it.

    Losing the role mid-pass is not something to push through: another owner
    is already redoing this work, and two of them writing the same table is
    what the lease exists to prevent.
    """
    if heartbeat is not None and not heartbeat():
        msg = "lost the maintenance lease mid-pass"
        raise RuntimeError(msg)


def settled_size(target_size: int) -> int:
    """The size at which a file is done: big enough to archive, not worth
    merging. Half the target.

    Not a knob, and deliberately not a second one. It was `compact_below`, and
    a separate setting for it could be — was — set above `target_size`, which
    silently wedges the log: compaction caps its output AT the target, so above
    it every file compaction produces is still a merge candidate, and none is
    ever large enough for `sync` to push. Compact forever, archive never.

    The fraction is not arbitrary either. `target_size` is measured on the
    BUFFER, in uncompressed SQLite bytes, while this is measured on the sealed
    Parquet FILE — so the same data is smaller here by whatever compression
    achieved, and a settled file simply never reaches `target_size` on disk.
    Anything at half the target is close enough that rewriting it would move
    most of a file to gain a little of one.

    One definition rather than two call sites computing their own, because
    compaction consumes files below this line and `sync` pushes files above it,
    and those two rules are only complementary while the line is the same. Let
    them drift and a file can be both a merge candidate and archived, which is
    the one thing §5 and §6 must never both believe.
    """
    return target_size // 2


class Maintenance:
    """The reclamation passes for one log."""

    def __init__(
        self,
        table: LogTable,
        buffer: Buffer,
        layout: Layout,
        config: LogConfig,
        sort_by: tuple[str, ...],
        archive: Archive | None = None,
    ) -> None:
        self._table = table
        self._buffer = buffer
        self._layout = layout
        self._config = config
        self._sort_by = sort_by
        self._archive = archive if archive is not None else Archive(layout)

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
        checkpoint(heartbeat)
        self.evict()
        checkpoint(heartbeat)
        self.expire()
        checkpoint(heartbeat)

    # -- compaction ---------------------------------------------------------

    ARCHIVED_KEY = "archive_through"

    def archived_through(self) -> int:
        """Highest offset the archive is known to hold, 0 if none (§5, I4).

        One number rather than a set, because data files cover contiguous
        non-overlapping ranges and sync pushes them in offset order: what the
        archive has is always a prefix. It is the watermark I4 is stated in
        terms of — "never evict a file the archive still lacks" — and the
        record sync leaves for `evict` to read.
        """
        recorded = self._buffer.get_meta(self.ARCHIVED_KEY)

        return 0 if recorded is None else int(recorded)

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

        # Never merge what the archive already has. Marking compaction output
        # archivable is not enough on its own: an output can still fall under
        # `settled_size` and be merged again, and a merge spanning the archive
        # watermark either duplicates the rows already pushed or strands the
        # ones above them. Skipping archived files makes that unreachable —
        # they are simply never inputs.
        archived = self.archived_through()

        threshold = settled_size(self._config.target_size)
        # An upper bound as well as a lower one. Selecting every adjacent file
        # under the threshold and merging the lot puts no ceiling on the
        # result: a hundred files just under it become one file a hundred times
        # `target_size`. That is the same defect as an undersized file with the
        # sign flipped — `target_size` is a statement about how big a file
        # should be, and a compaction that ignores it produces exactly the
        # layout §6 exists to correct.
        #
        # So a run closes when the next file would take it past the target, and
        # the pass emits several correctly sized files instead of one enormous
        # one. Seal output and compaction output then converge on the same size
        # rather than diverging with every pass.
        budget = self._config.target_size

        run: list[DataFile] = []
        size = 0
        for data_file in self._table.data_files():
            if data_file.hi <= archived or data_file.size >= threshold:
                # Final: already archived, or already the size it should be.
                # Ends any run rather than joining it.
                self._merge(run, heartbeat)
                run, size = [], 0
                continue

            if run and size + data_file.size > budget:
                # Adjacency is in offset order, so closing here and starting a
                # new run leaves both contiguous.
                self._merge(run, heartbeat)
                run, size = [], 0

            run.append(data_file)
            size += data_file.size

        self._merge(run, heartbeat)

    def _merge(self, run: list[DataFile], heartbeat: Callable[[], bool] | None) -> None:
        """Compact a run, if there is enough of it to be worth a rewrite."""
        if len(run) >= self._config.compact_min_files:
            self._compact_run(run, heartbeat)

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
        checkpoint(heartbeat)

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
            checkpoint(heartbeat)

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
        # I4, and the only line in this pass that is correctness rather than
        # housekeeping: a file the archive still lacks must not leave the local
        # table, because with an archive configured the local copy is not the
        # only one only once sync says so. Clamped rather than skipped, so a
        # sync that is arbitrarily far behind delays eviction instead of
        # stopping it — §5's "lazy, restartable" applies here too.
        #
        # With no archive there is no watermark and none is owed: §8 says
        # `local_retention` is then a deletion policy over the only copy, which
        # is the contract the operator asked for.
        if self._archive.configured():
            boundary = min(boundary, self.archived_through())
            if boundary <= 0:
                return

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
