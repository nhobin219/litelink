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
from litelink._buffer import _NO_ROW_LIMIT, OFFSET, Buffer
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
    files: Sequence[DataFile],
    budget: int,
    memory: Mapping[str, int],
    rows: int | None = None,
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

    `rows` is `target_rows`, and it has to be here for the same reason. A seal
    that cut on the row limit produced a file holding fewer bytes than
    `target_size`, which by bytes alone looks starved — so compaction would
    merge exactly the files the row cap just created, straight back past it.
    Whichever ceiling the seal respected, this respects too.
    """
    limit = rows or _NO_ROW_LIMIT
    grouped: list[list[DataFile]] = []
    run: list[DataFile] = []
    held = 0
    counted = 0
    for data_file in files:
        size = memory.get(data_file.path, budget)
        # A file already at either ceiling on its own closes the previous run
        # and forms one of its own, which then closes on the next file. No
        # special case needed: it simply never has room for a neighbour.
        if run and (held + size > budget or counted + data_file.rows > limit):
            grouped.append(run)
            run, held, counted = [], 0, 0

        run.append(data_file)
        held += size
        counted += data_file.rows

    if run:
        grouped.append(run)

    return grouped


def stable_prefix(
    files: Sequence[DataFile],
    budget: int,
    min_files: int,
    memory: Mapping[str, int],
    rows: int | None = None,
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
    limit = rows or _NO_ROW_LIMIT
    settled = 0
    for run in runs(files, budget, memory, rows):
        if len(run) >= min_files:
            break

        held = sum(memory.get(f.path, budget) for f in run)
        counted = sum(f.rows for f in run)
        # Room under BOTH ceilings is what makes the trailing run growable. At
        # either one it is finished, and a file that cannot grow is settled.
        if run[-1] is files[-1] and held < budget and counted < limit:
            break

        settled += len(run)

    return settled


def is_remote(path: str) -> bool:
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
        archive: Archive,
    ) -> None:
        self._table = table
        self._buffer = buffer
        self._layout = layout
        self._config = config
        self._sort_by = sort_by
        self._archive = archive
        # Read once per eviction pass rather than once per file. Cleared at the
        # top of `evict`, so a pass never decides from what a previous one saw.
        self._age_cache: dict[str, int] | None = None

    def set_config(self, config: LogConfig) -> None:
        """Adopt new policy in place, rather than being rebuilt around it."""
        self._config = config

    def set_sort_by(self, sort_by: tuple[str, ...]) -> None:
        self._sort_by = sort_by

    def run(self, heartbeat: Callable[[], bool] | None = None) -> None:
        """The local passes, with a checkpoint between them.

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

        A prefix, always: files cover contiguous non-overlapping ranges (§4)
        and `sync` pushes them in order, so one integer describes it.

        Cached in `meta` rather than read from the archive, so eviction can ask
        a keyed read instead of a network round trip to find out what it may
        drop.
        """
        recorded = self._buffer.get_meta(self.ARCHIVED_KEY)

        return 0 if recorded is None else int(recorded)

    def compact(self, heartbeat: Callable[[], bool] | None = None) -> None:
        """Merge runs of undersized adjacent files (§6).

        A no-op in normal operation. The cut is exact and there is no timer to
        cut early, so every file a seal writes already holds what it should.
        This is for the deliberate exceptions: an explicit `seal()`, which cuts
        short by definition, and a change to `target_size`, which leaves
        existing files sized for the old value.

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

        for run in runs(
            pending,
            self._config.compact_size,
            self.memory(),
            self._config.compact_rows,
        ):
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
            key if is_remote(key) else str(self._layout.absolute(key)): size
            for key, size in self._buffer.file_bytes().items()
        }

    def _merge(self, run: list[DataFile], heartbeat: Callable[[], bool] | None) -> None:
        """Compact a run, if there is enough of it to be worth a rewrite."""
        if len(run) >= self._config.compact_min_files:
            self._rewrite_run(self._table, run, heartbeat)

    def _rewrite_run(
        self,
        table: LogTable,
        run: list[DataFile],
        heartbeat: Callable[[], bool] | None = None,
        *,
        upload: bool = False,
    ) -> None:
        """Replace one run of adjacent files with a single merged one.

        Both tiers, one path. A local compaction and an archive rewrite differ
        only in which table they commit to and whether the output is uploaded
        afterwards; everything that makes either safe — the claim before the
        file exists, re-sorting, verification, queueing the sources before the
        commit that supersedes them, carrying their measured sizes onto the
        output — is identical, and was identical when it was written twice.
        """
        lo, hi = run[0].lo, run[-1].hi
        # Unique per attempt. See `compaction_path`: a fixed name made a
        # rewrite of a previous compaction write over the file it was reading.
        rel_path = self._layout.compaction_path(lo, hi, uuid.uuid4().hex[:8])
        target = table.uri(rel_path) if upload else str(self._layout.absolute(rel_path))
        # Claimed before the file exists, exactly as a seal claims its path
        # (I2). One that dies between the write and the commit is then
        # recoverable by name, instead of being a file nobody can identify
        # without listing — which for the archive would be a paginated LIST
        # over object storage, the thing this design refuses. Claimed as the
        # TARGET, so recovery knows which tier to remove it from.
        self._buffer.claim_compaction(lo, hi, self._key(target))
        self._write_merge(table, run, rel_path, target, heartbeat, upload=upload)
        # Only this one. Another operation's claim — a rewrite that crashed
        # before recovery ran — is not ours to retire.
        self._buffer.clear_compaction(self._key(target))

    def _write_merge(
        self,
        table: LogTable,
        run: list[DataFile],
        rel_path: str,
        target: str,
        heartbeat: Callable[[], bool] | None = None,
        *,
        upload: bool = False,
    ) -> None:
        lo, hi = run[0].lo, run[-1].hi
        merged = table.scan_range(lo, hi)
        if self._sort_by:
            # Re-sorted, not merely concatenated: concatenation would leave the
            # row groups carrying each source file's range, which is the
            # statistic the sort exists to tighten.
            merged = merged.sort_by([(c, "ascending") for c in self._sort_by])

        _verify(merged, run, lo, hi)

        # Written locally either way, because Parquet is written to a file and
        # the alternative is holding a second copy of the run in memory. For
        # the archive it is a staging copy under the name it will have
        # remotely, uploaded and then removed.
        dest = self._layout.absolute(rel_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(merged, dest)
        fsync(dest)
        if upload:
            table.put(dest, rel_path)
            dest.unlink(missing_ok=True)

        # Checked between writing and committing, because those are the two
        # halves a lapsed lease separates. A run outlasting the TTL lets
        # another owner recover — removing the output this claimed — and the
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
        table.replace_range(lo, hi, [target])
        # After the commit: until it lands the sources are still the live
        # files, and moving their sizes onto an output that never became real
        # would leave every one of them unmeasured.
        self._buffer.record_merge(self._key(target), (self._key(f.path) for f in run))

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
            self._rewrite_run(self._table, [data_file])
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
        keep_rows = self._config.local_rows
        if retention is None and keep_rows is None:
            return

        # Same reason as `compact`: this decides what to drop from the ages a
        # handle reports, and a stale one reports a table that has moved.
        self._table.reload()
        self._age_cache = None

        # Files cover contiguous non-overlapping ranges, so evicting a prefix
        # is a single upper bound. Anything newer is untouched — and each
        # policy is one such bound, so honouring both is taking the lower.
        # They are floors on what stays readable locally, so the one that
        # retains MORE wins, which is the opposite of how the seal combines its
        # two limits (§12).
        limits: list[int] = []
        if retention is not None:
            cutoff = datetime.now(UTC) - retention
            stale = [f for f in self._table.data_files() if self._written(f) < cutoff]
            # Nothing old enough is a limit of zero, not an absent one: it
            # means this policy would keep everything.
            limits.append(max((f.hi for f in stale), default=0))

        if keep_rows is not None:
            # Offsets are contiguous, so the newest `keep_rows` of them start
            # here. AUTOINCREMENT can leave a gap where a transaction rolled
            # back, which makes this retain slightly more than asked rather
            # than less — the safe direction for a floor.
            limits.append(self._buffer.next_offset() - 1 - keep_rows)

        boundary = min(limits)
        # Snapped DOWN to a file boundary. The age limit is already one — it is
        # some file's `hi` — and so is the archive clamp, but the row floor is
        # arbitrary and lands mid-file on most passes. A mid-file boundary is
        # not a smaller eviction: `evict_through` filters by row, so pyiceberg
        # rewrites the straddling file copy-on-write, and the replacement is
        # written at a path this library never learns. That breaks the rule the
        # whole deletion design rests on — every file's path is in SQLite
        # before the file exists (I2) — and leaves the superseded original out
        # of the queue, so once expiry drops the snapshots naming it, nothing
        # can name it again. `drain` is a keyed read and this design refuses
        # directory scans, so the file is unreclaimable for good.
        files = self._table.data_files()
        boundary = max((f.hi for f in files if f.hi <= boundary), default=0)
        if boundary <= 0:
            return

        # I4: a file the archive still lacks must not leave the local table,
        # because with an archive configured the local copy stops being the
        # only one only once sync says so. Clamped rather than skipped, so a
        # sync that is arbitrarily far behind delays eviction instead of
        # stopping it — §5's "lazy, restartable" applies here too.
        #
        # With no archive there is nothing owed: §8 says `local_retention` is
        # then a deletion policy over the only copy, which is the contract the
        # operator asked for.
        if self._archive.configured():
            boundary = min(boundary, self.archived_through())
            if boundary <= 0:
                return

        # Everything the boundary REMOVES, not just what looked old enough to
        # trigger it. A compaction output has a fresh snapshot age, so it never
        # appears in `stale` — but its offsets can sit below a stale file's
        # `hi`, so the boundary drops it too. Queueing only `stale` left it
        # removed from the table and named by nothing.
        self._enqueue(f.path for f in files if f.hi <= boundary)
        self._table.evict_through(boundary)

    def rewrite_archive(self, heartbeat: Callable[[], bool] | None = None) -> None:
        """Re-cut undersized archived files to `target_size` (§6, ad-hoc).

        Not part of `maintain`, and not expected to be needed. The archive is
        well-sized by construction: `sync` pushes only what compaction has
        finished with, so nothing undersized reaches it in normal operation.
        Two deliberate acts break that. An explicit `seal()` can strand a small
        file between larger ones, where compaction can never merge it and
        `sync` pushes it rather than blocking the watermark for ever. And
        changing `target_size` leaves history sized for the old value, since
        the archive is immutable and a size change applies to the future.

        **It re-ingests rather than merging.** The rows from the first
        badly-sized file onwards are appended to a scratch `Buffer` and sealed
        back out — the same append path, the same `_cut`, the same byte
        accounting, the same extent rows. That is not a saving of code so much
        as of ways to be wrong: a merge can only combine whole files, so it
        lands near the target and leaves the remainder undersized, while the
        appender cuts on the row that crosses and hits it exactly. Sizing the
        archive by a second rule that approximates the first is how this came
        to compare compressed bytes against a memory bound in the first place.

        The scratch buffer stays small. Sealing deletes the rows it took, so it
        holds roughly one file at a time however long the range is, and it is
        removed at the end either way.

        It is also opened without durability, because everything in it is
        derived from the archive and the archive is still there until the final
        commit. Rows arrive one source file per transaction rather than one per
        row, for the same reason.

        One commit swaps the whole range. Committing each new file as it is
        written would have each commit delete a sub-range of a file the next
        one still needs to read.
        """
        archive = self._archive.table()
        if archive is None:
            msg = "rewrite_archive() needs an archive; this log is local-only"
            raise ValueError(msg)

        archive.reload()
        stale = self._badly_sized(archive)
        if len(stale) < 2:
            # One file, or none. A single undersized file at the end of the
            # archive is where an undersized file is allowed to be, and
            # rewriting it alone would produce the same file again.
            return

        # Queued BEFORE the commit that supersedes them, exactly as a local
        # compaction queues its sources, and safe for the same reason: `drain`
        # refuses to delete anything the table still references, so an entry
        # made for a commit that never lands simply never comes due.
        self._enqueue(data_file.path for data_file in stale)
        self._recut(archive, stale, heartbeat)

    def _badly_sized(self, archive: LogTable) -> list[DataFile]:
        """The archived files from the first one under `target_size` on.

        Everything before it already holds a full target and re-cutting it
        would rewrite bytes to reproduce them. Everything after has to move
        regardless of its own size, because the shortfall ahead of it shifts
        every boundary behind it.

        A file whose size was never recorded counts as full, so an archive
        whose local extents were lost is left alone rather than rewritten on a
        guess about what it holds.
        """
        held = self.memory()
        target = self._config.compact_size
        files = archive.data_files()
        for index, data_file in enumerate(files):
            if held.get(data_file.path, target) < target:
                return files[index:]

        return []

    def _recut(
        self,
        archive: LogTable,
        stale: list[DataFile],
        heartbeat: Callable[[], bool] | None = None,
    ) -> None:
        """Append `stale` back through a buffer and seal it out again."""
        lo, hi = stale[0].lo, stale[-1].hi
        # Not durable, deliberately. Every row in here came from the archive
        # and is still in the archive until the single commit at the end, so a
        # crash costs a re-run rather than data — and this does one transaction
        # per source file plus a few per sealed one, every one of which would
        # otherwise fsync for a guarantee nothing here depends on.
        # Removed BEFORE opening, not only after. SQLite's AUTOINCREMENT
        # assigns `max(largest existing rowid, seq) + 1`, so seeding the
        # counter DOWN is silently ignored when rows already sit above it —
        # verified: with rows at 100-105 and the sequence set to 99, the next
        # insert takes 106. A rewrite killed before its first cut leaves rows
        # in this database and no claim to recover by, so the next run would
        # re-append every row at shifted offsets, hold each one twice, pass the
        # row-count guard (which counts only what this run read), and commit
        # files whose offsets carry the wrong rows. Durable archive corruption
        # with every check green. It is derived state; starting from nothing is
        # always correct.
        self._discard_scratch()
        scratch = Buffer.open(
            self._layout.rewrite_db,
            self._buffer.schema,
            target_size=self._config.compact_size,
            durable=False,
        )
        written: list[str] = []
        expected = 0
        try:
            # The rows keep the offsets they already have (§4), so the counter
            # resumes at the start of the range rather than at 1. Reassigned
            # rather than supplied, which is what keeps I11 true of the rewrite
            # as well: nothing hands an offset to `append`.
            scratch.seed_offsets(lo)
            expected = 0
            for data_file in stale:
                checkpoint(heartbeat)
                rows = archive.scan_range(data_file.lo, data_file.hi)
                # Sorted by offset and then stripped of it. The counter is what
                # reassigns them, so the rows have to arrive in the order their
                # offsets already have — files are clustered by `sort_by`, not
                # by offset, so what comes back is in neither order by default.
                # Getting this wrong would not raise anywhere: every row would
                # keep its data and be handed somebody else's offset.
                rows = rows.sort_by([(OFFSET, "ascending")])
                expected += rows.num_rows
                scratch.append(rows.drop_columns([OFFSET]).to_pylist())
                written += self._seal_scratch(scratch, archive, heartbeat)

            # The tail, which by definition did not reach the target. Cutting
            # it short is what `seal()` does, and one undersized file at the
            # end is where one is allowed to be.
            scratch.close_open_group()
            written += self._seal_scratch(scratch, archive, heartbeat)
        finally:
            scratch.close()
            self._discard_scratch()

        # Before the swap, not after. The commit is the point of no return:
        # it deletes the range these rows came from, so a rewrite that lost
        # any of them must fail while the originals are still the live files.
        if expected != hi - lo + 1:
            msg = f"archive rewrite read {expected} rows for offsets {lo}-{hi}"
            raise RuntimeError(msg)

        archive.replace_range(lo, hi, [archive.uri(path) for path in written])
        # Each of this rewrite's own claims, now that the commit names every
        # object they described. Clearing the table wholesale would also retire
        # claims left by an operation that crashed and has not been recovered.
        for path in written:
            self._buffer.clear_compaction(archive.uri(path))

    def _discard_scratch(self) -> None:
        """Remove the rewrite scratch database and its sidecars."""
        for suffix in ("", "-wal", "-shm"):
            self._layout.rewrite_db.with_name(
                self._layout.rewrite_db.name + suffix
            ).unlink(missing_ok=True)

    def _seal_scratch(
        self,
        scratch: Buffer,
        archive: LogTable,
        heartbeat: Callable[[], bool] | None = None,
    ) -> list[str]:
        """Write out every extent the scratch buffer has cut, and return their
        names. The seal's own loop: take the queued range, claim the path
        before the file exists, write, and retire the extent."""
        written: list[str] = []
        while True:
            queued = scratch.pending_group()
            if queued is None:
                return written

            start, end = queued
            rel_path = self._layout.compaction_path(
                start, end - 1, uuid.uuid4().hex[:8]
            )
            # Both claims. `sealing` in the scratch buffer makes the extent
            # recoverable there; `compacting` in the real one is what an
            # interrupted rewrite is found by, since the scratch database is
            # deleted on the way out.
            scratch.claim_seal(start, end, rel_path)
            self._buffer.claim_output(start, end - 1, archive.uri(rel_path))

            rows = scratch.rows_below(end)
            if self._sort_by:
                rows = rows.sort_by([(c, "ascending") for c in self._sort_by])

            dest = self._layout.absolute(rel_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(rows, dest)
            fsync(dest)
            archive.put(dest, rel_path)
            dest.unlink(missing_ok=True)
            checkpoint(heartbeat)

            # The bytes the scratch buffer counted for exactly these rows,
            # which is the same number the appender would have recorded had
            # they been cut this way the first time.
            self._buffer.record_file(
                archive.uri(rel_path), start, end, scratch.group_bytes(end)
            )
            scratch.finish_seal(end, rel_path)
            written.append(rel_path)

    def _written(self, data_file: DataFile) -> datetime:
        """When a file was written, for `local_retention` to measure against.

        The log's own record first. Iceberg's is the fallback and cannot be the
        primary: it dates a file by the snapshot that added it, and `expire`
        deletes that snapshot, after which the file has no age there at all.

        A file neither knows is treated as newly written, so it is never
        evicted on an age nobody recorded. That is the safe direction — the
        cost is disk, and the alternative is deleting data because its age was
        unknown. It applies to files this database never saw, which after
        `hydrate` records what it restores is only files from a version that
        did not keep them.
        """
        named = self._ages().get(self._key(data_file.path))
        if named is not None:
            return datetime.fromtimestamp(named, UTC)

        added = self._table.snapshot_ages().get(data_file.path)

        return datetime.now(UTC) if added is None else added.replace(tzinfo=UTC)

    def _ages(self) -> dict[str, int]:
        """Read once per pass, not once per file."""
        if self._age_cache is None:
            self._age_cache = self._buffer.file_ages()

        return self._age_cache

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

        if not any(is_remote(p) for p in self._buffer.queued_deletions()):
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
        remote = self._archive.table() if any(is_remote(p) for p in due) else None
        remote_referenced = set() if remote is None else remote.referenced_paths()

        for rel_path in due:
            if is_remote(rel_path):
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

    def _key(self, path: str) -> str:
        """How a file is named in SQLite, whichever tier it is in.

        Local files root-relative, so a log directory stays movable; remote
        ones by the full URI, which is already absolute and has no root to be
        relative to. One rule, used by the deletion queue, the extent table and
        the compaction claim alike, so `_is_remote` can tell them apart again
        wherever one of those is read back.
        """
        return path if is_remote(path) else self._layout.relative(path)

    def enqueue_recovered(self, keys: Iterable[str]) -> None:
        """Queue an abandoned operation's outputs for deletion.

        Already keyed the way the queue wants them — a claim records the same
        name `_key` would produce — so this is `_enqueue` without the
        translation, and it exists to make that explicit rather than have
        recovery look like it is enqueueing table paths.

        The grace period applies here as it does everywhere: `drain` refuses to
        remove anything the table still references, which is what makes it safe
        to queue a file whose owner may turn out to be alive.
        """
        self._buffer.enqueue_deletions(keys, int(datetime.now(UTC).timestamp()))

    def _enqueue(self, paths: Iterable[str]) -> None:
        """Queue files for deletion once their grace period passes."""
        self._buffer.enqueue_deletions(
            (self._key(p) for p in paths),
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
