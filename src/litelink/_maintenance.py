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
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow.parquet as pq

from litelink._archive import Archive
from litelink._fs import fsync

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

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


def runs(
    files: Sequence[DataFile], budget: int, memory: Mapping[str, int]
) -> list[list[DataFile]]:
    """Adjacent files grouped into merge candidates, each within `budget`.

    The one definition of what compaction considers a run, because two
    collaborators act on it and they must not disagree: `compact` merges these
    groups, and `sync` refuses to archive a file that appears in one, since a
    file pushed and then merged locally leaves the archive holding rows that
    have been rewritten underneath it.

    Sizes come from `memory` — what each file holds uncompressed, as the
    appender counted it — and the budget is `target_size`, which is stated in
    those same units. That correspondence is the point. Sizing this by the
    files' size on disk is what the code here used to do, and on data that
    compresses 8:1 it merged eight files that were each already full, into one
    holding eight times the memory the target allows.

    A file whose size was never recorded counts as full. Unknown is not zero:
    treating it as small is what would pull an already-correct file into a
    rewrite, and the cost of leaving it alone is nothing but a merge that did
    not happen.

    The budget caps the OUTPUT. Merging every adjacent small file without one
    puts no ceiling on the result — a hundred files just under the line become
    one file a hundred times the target, which is the same defect as an
    undersized file with the sign flipped. A run therefore closes when the next
    file would take it past the budget, and the pass emits several correctly
    sized files instead of one enormous one.
    """
    grouped: list[list[DataFile]] = []
    run: list[DataFile] = []
    held = 0
    for data_file in files:
        size = memory.get(data_file.path, budget)
        # A file already at the budget on its own closes the previous run and
        # forms one of its own, which then closes on the next file. No special
        # case needed: it simply never has room for a neighbour.
        if run and held + size > budget:
            grouped.append(run)
            run, held = [], 0

        run.append(data_file)
        held += size

    if run:
        grouped.append(run)

    return grouped


def stable_prefix(
    files: Sequence[DataFile],
    budget: int,
    min_files: int,
    memory: Mapping[str, int],
) -> int:
    """How many leading files compaction will never touch again.

    What `sync` needs to know, asked directly. It used to ask a proxy question
    — "is this file at least half the target?" — and measure it on disk, which
    fails outright on compressible data: a 64 KiB buffer of repetitive rows
    seals to under 8 KiB, so no file ever reached half of 64 KiB, `sync` pushed
    nothing, and the archive stayed empty with nothing to indicate why.
    Compaction's own rule has no such blind spot, and it is the rule that
    actually matters, since the only reason to hold a file back is that
    compaction might rewrite it.

    Two things disqualify a file. It sits in a run compaction would merge right
    now; or it sits in the trailing run, which is under budget and so still has
    room for files that have not been written yet. Everything before the first
    such file is settled: no run containing it can also contain anything new,
    because the files between them already fill the budget.

    A small file in the MIDDLE is therefore pushed, not held. It is under the
    target and always will be — its neighbours are too big to merge with, so
    compaction will not touch it and waiting achieves nothing. Holding it was
    the old behaviour and it meant a single explicit `seal()` blocked the
    archive permanently: everything after it is newer, so the watermark never
    advanced again and I4 then pinned local disk too. Not "later" — never.
    """
    settled = 0
    for run in runs(files, budget, memory):
        if len(run) >= min_files:
            break

        held = sum(memory.get(f.path, budget) for f in run)
        if run[-1] is files[-1] and held < budget:
            break

        settled += len(run)

    return settled


def _is_remote(path: str) -> bool:
    """Whether a queued deletion names an object rather than a local file.

    The queue holds root-relative names for local files and full URIs for
    remote ones, and a URI is the only thing that can carry a scheme — a
    relative path never contains "://". One queue for both, because the grace
    period, the reference veto and the ordering that makes them safe are
    identical either side of the network.
    """
    return "://" in path


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

        archived = self.archived_through()

        # Archived files are never inputs. Marking compaction output archivable
        # is not enough on its own: a merge spanning the archive watermark
        # either duplicates the rows already pushed or strands the ones above
        # them. Skipping them makes that unreachable. They are a prefix — the
        # watermark is contiguous — so dropping them cannot break adjacency.
        pending = [f for f in self._table.data_files() if f.hi > archived]

        for run in runs(pending, self._config.target_size, self.memory()):
            self._merge(run, heartbeat)

    def memory(self) -> dict[str, int]:
        """What each data file holds uncompressed, keyed by the path a
        `DataFile` carries.

        Covers both tiers, because both are measured the same way and the
        archive's entries are the local ones carried across the push. A local
        file is recorded root-relative and named absolutely by the table; a
        remote one is recorded and named by the same URI.
        """
        return {
            key if _is_remote(key) else str(self._layout.absolute(key)): size
            for key, size in self._buffer.file_bytes().items()
        }

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
        # After the commit: until it lands the sources are still the live
        # files, and moving their sizes onto an output that never became real
        # would leave every one of them unmeasured.
        self._buffer.record_merge(
            rel_path, (self._layout.relative(f.path) for f in run)
        )

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

    def rewrite_archive(self, heartbeat: Callable[[], bool] | None = None) -> None:
        """Merge undersized files already in the archive (§6, ad-hoc).

        Not part of `maintain`, and not expected to be needed. The archive is
        well-sized by construction: `sync` pushes only what compaction has
        finished with, so nothing undersized reaches it in normal operation.
        Two things break that, both deliberate acts. An explicit `seal()` can
        strand a small file between larger ones, where compaction can never
        merge it and `sync` pushes it rather than blocking the watermark for
        ever. And lowering `target_size` leaves everything written under the
        old one looking oversized, while raising it leaves everything
        undersized — the archive is immutable history, so a size change applies
        to the future and this is what applies it to the past.

        Sizes are the same ones the seal measured. `sync` carries each entry
        over to the archive's name for the file when it pushes it, so nothing
        here re-derives a size from the file — which could not be done anyway,
        since a Parquet footer records what it held before compression and not
        what the appender counted. An archived file with no entry counts as
        full, exactly as a local one does.

        Runs of one are skipped, and `compact_min_files` does not apply. It is
        a throughput heuristic for a pass that runs continuously — worth
        batching there, pointless in an operation somebody invoked because the
        layout is already wrong.

        The rewritten file is committed before its sources are deletable: they
        go through the same queue and the same grace period as a local
        compaction's, so a reader mid-scan keeps the files it resolved (I6).
        """
        archive = self._archive.table()
        if archive is None:
            msg = "rewrite_archive() needs an archive; this log is local-only"
            raise ValueError(msg)

        archive.reload()
        files = archive.data_files()
        held = self.memory()
        for run in runs(files, self._config.target_size, held):
            if len(run) < 2:
                continue

            checkpoint(heartbeat)
            self._rewrite_remote(archive, run)

    def _rewrite_remote(self, archive: LogTable, run: list[DataFile]) -> None:
        """Replace one run of archived files with a single merged one."""
        lo, hi = run[0].lo, run[-1].hi
        merged = archive.scan_range(lo, hi)
        if self._sort_by:
            merged = merged.sort_by([(c, "ascending") for c in self._sort_by])

        _verify(merged, run, lo, hi)

        rel_path = self._layout.compaction_path(lo, hi, uuid.uuid4().hex[:8])
        # Staged locally and uploaded, because Parquet is written to a file and
        # the alternative is holding a second copy of the run in memory. Under
        # a name of its own rather than the data directory's, so a crash cannot
        # leave something that looks like a local data file: nothing references
        # this path, and the `finally` removes it either way.
        staged = self._layout.root / f"{Path(rel_path).name}.rewrite"
        try:
            pq.write_table(merged, staged)
            fsync(staged)
            archive.put(staged, rel_path)
        finally:
            staged.unlink(missing_ok=True)

        # Queued before the commit that supersedes them, exactly as a local
        # compaction queues its sources: after it their names are still in the
        # old snapshot, but nothing this process holds would re-derive them.
        self._enqueue(data_file.path for data_file in run)
        archive.replace_range(lo, hi, archive.uri(rel_path))
        self._buffer.record_merge(
            archive.uri(rel_path), (data_file.path for data_file in run)
        )

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
        self._expire_archive(cutoff)
        self.drain()

    def _expire_archive(self, cutoff: datetime) -> None:
        """The same expiry on the archive, when a rewrite has left work there.

        Only then. `sync` adds files and never supersedes one, so an archive
        that has only ever been synced has nothing an old snapshot is keeping
        alive, and expiring it every pass would spend a remote catalog commit
        to discover that. `rewrite_archive` is the one thing that supersedes an
        archived file, and it is also the only thing that puts a remote entry
        in the deletion queue — so a queue with one in it is the exact signal
        that the archive has garbage to release.

        Without this the queue never drains: `drain` refuses to delete a file
        any snapshot still references, and until the snapshot that named it
        expires, one always does.
        """
        if not self._archive.configured():
            return

        if not any(_is_remote(p) for p in self._buffer.queued_deletions()):
            return

        archive = self._archive.table()
        if archive is None:
            return

        self._enqueue(archive.metadata_paths(archive.snapshots_older_than(cutoff)))
        archive.expire_snapshots_older_than(cutoff)

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
        # Only if the queue holds something remote, so an ordinary drain on a
        # local-only log still opens nothing. `rewrite_archive` is what puts
        # remote entries here, and it is an operation somebody ran on purpose.
        remote = self._archive.table() if any(_is_remote(p) for p in due) else None
        remote_referenced = set() if remote is None else remote.referenced_paths()

        for rel_path in due:
            if _is_remote(rel_path):
                if remote is None or rel_path in remote_referenced:
                    continue

                remote.remove(rel_path)
                self._buffer.forget_deletion(rel_path)
                continue

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
        """Queue files for deletion once their grace period passes.

        Local files are stored root-relative, so a log directory stays movable;
        remote ones keep the full URI, which is already absolute and has no
        root to be relative to. `_is_remote` is what tells `drain` them apart.
        """
        self._buffer.enqueue_deletions(
            (p if _is_remote(p) else self._layout.relative(p) for p in paths),
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
