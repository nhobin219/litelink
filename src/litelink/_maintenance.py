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
from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pyarrow.parquet as pq

from litelink._archive import Archive
from litelink._buffer import _NO_ROW_LIMIT, OFFSET, Buffer
from litelink._claim import EVERYTHING, Claim, new_owner
from litelink._fs import fsync

# Where the log records its settings. Beside `ARCHIVE_KEY` in spirit: not
# `Log`'s private business, because eviction decides deletions from it.
CONFIG_KEY = "config"

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

    import pyarrow as pa

    from litelink._buffer import Buffer
    from litelink._layout import Layout
    from litelink._table import DataFile, LogTable
    from litelink.log import LogConfig


def checkpoint(heartbeat: Callable[[], bool] | None) -> None:
    """Renew the caller's claim, or refuse to carry on without it.

    Losing the range mid-pass is not something to push through: another owner
    may already be redoing this work, and two of them writing the same files is
    what the claim exists to prevent. A claim ends by being TAKEN, not by
    expiring — an uncontested holder renews fine — so a failed renew means
    somebody else owns these offsets now.
    """
    if heartbeat is not None and not heartbeat():
        msg = "lost the claim on this range mid-pass"
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


def _both(
    ours: Callable[[], bool], theirs: Callable[[], bool] | None
) -> Callable[[], bool]:
    """Renew our own claim AND report the caller's heartbeat.

    `heartbeat or claim.renew` read naturally and was wrong: any caller passing
    a heartbeat — which is what the role-lease era asked for, so it is a habit
    people carry forward — silently stopped the run claim from being renewed at
    all, and the pre-commit check then consulted a stranger's callback instead
    of the claim. A merge over the TTL would lose its exclusion with no stall
    required, and commit rows eviction had removed in the meantime.

    Ours is renewed first and unconditionally, so a falsy caller heartbeat
    cannot short-circuit it.
    """

    def beat() -> bool:
        held = ours()

        return held if theirs is None else held and theirs()

    return beat


def _covered(ranges: Sequence[tuple[int, int]], lo: int, hi: int) -> bool:
    """Whether `[lo, hi)` sits entirely inside `ranges`, which are sorted.

    A walk rather than a set membership test, because the archive's cuts need
    not line up with anyone else's. Adjacent archived files join — offsets are
    contiguous, so `[1, 151)` and `[151, 301)` together hold `[101, 201)` even
    though neither holds it alone — and a gap ends the answer.
    """
    if hi <= lo:
        # An empty range is held by anything, vacuously. Unreachable from I4,
        # where a file always holds at least one row, but a predicate that
        # answers "no" to a question with no rows in it is a trap for the next
        # caller.
        return True

    reach = lo
    for start, end in ranges:
        if start > reach:
            return False

        if end > reach:
            reach = end
            if reach >= hi:
                return True

    return reach >= hi


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
        sort_by: tuple[str, ...],
        archive: Archive,
    ) -> None:
        self._table = table
        self._buffer = buffer
        self._layout = layout
        self._sort_by = sort_by
        self._archive = archive
        # Read once per eviction pass rather than once per file. Cleared at the
        # top of `evict`, so a pass never decides from what a previous one saw.
        self._age_cache: dict[str, int] | None = None

    @property
    def config(self) -> LogConfig:
        """The policy in force, read from the log on every access.

        No copy is kept here. A second copy is what every one of this seam's
        defects came down to: a decision read the copy while the durable row
        said otherwise, and correctness then depended on a refresh call sitting
        at each decision — twelve of them, and always one short somewhere.
        """
        return self._buffer.config()

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

    def archived_prefix(self, files: Sequence[DataFile], prefix: str | None) -> int:
        """The highest `hi` whose whole prefix the archive holds (§4a).

        I4 asked of segments. A file is the archive's business if the archive
        holds THAT FILE'S ROWS, which `sync` wrote down when it pushed it. The
        walk stops at the first file not fully held, so the answer stays a
        prefix — which is what eviction needs, since it removes one.

        **Coverage, not equality.** The two tiers cut the same rows into files
        independently, and asking whether a local range EQUALS an archived one
        was wrong the moment they could differ. `rewrite_archive` re-cuts the
        archive to different boundaries by design — that is its entire job —
        and every local file then matched nothing, for ever: eviction clamped
        to zero and stopped, and compaction stopped seeing archived files as
        the archive's business and merged across its extent. Neither heals,
        because nothing ever re-cuts the archive back.

        Exact rather than conservative in both directions, and that is the
        point. A watermark had to be raised before a register to cover the
        crash between the two, so it named ranges the archive might not hold,
        and it had to be reset when the log was re-pointed, so it went
        backwards past ranges the archive did hold. Neither is expressible
        here: the row is written when the copy exists, and it names the bucket
        it went to.
        """
        ordered = sorted(files, key=lambda f: f.lo)
        if not ordered:
            return 0

        covered = self._buffer.archived_ranges(prefix, ordered[0].lo)
        reached = 0
        for data_file in ordered:
            # `record_file` stores the end offset exclusive, as every extent
            # does — the cut is the offset AFTER the last row.
            if not _covered(covered, data_file.lo, data_file.hi + 1):
                break

            reached = data_file.hi

        return reached

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

        # Archived files are never inputs. A merge spanning the archive's
        # extent either duplicates the rows already pushed or strands the ones
        # above them, and skipping them makes that unreachable. They are a
        # prefix, so dropping them cannot break adjacency.
        #
        # Asked per file (§4a). It also keeps the two tiers' ranges aligned:
        # a file the archive holds is never rewritten locally, so the local
        # range and the archived range stay the same range, which is what lets
        # `archived_prefix` match them at all.
        local = self._table.data_files()
        # Asked of ANY archive, and asked even when none is configured. A
        # merge across a range some archive holds makes a local file whose
        # boundaries line up with nothing there — and nothing re-cuts a LOCAL
        # straddler, so re-attaching that archive stalls the log for good:
        # eviction pins below the straddler and every push is refused. Four
        # legitimate operations reach it — detach, raise the target, maintain,
        # re-attach — with no warning at any step.
        #
        # Skipping them is not free, and the earlier claim that it was — "a
        # file with an archive copy is already at the target" — is false the
        # moment the target is RAISED after the copy was made, which is the
        # scenario this exists for. What it costs is that such a file stays at
        # the size it was archived at; `rewrite_archive` is the tool for that.
        # What it buys is that no merge can ever straddle a range an archive
        # holds. `_push` applies the same exclusion, or the two deadlock.
        archived = self.archived_prefix(local, None)
        pending = [f for f in local if f.hi > archived]

        # One read, so the two limits describe the same policy.
        config = self.config
        for run in runs(
            pending,
            config.compact_size,
            self.memory(),
            config.compact_rows,
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
        if len(run) >= self.config.compact_min_files:
            self._rewrite_run(self._table, run, heartbeat)

    def _rewrite_run(
        self,
        table: LogTable,
        run: list[DataFile],
        heartbeat: Callable[[], bool] | None = None,
        *,
        upload: bool = False,
        owner: str | None = None,
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
        # The range claimed before a byte is written, and the two are one
        # question: may this merge run, and is the record of it live work or a
        # dead process's leavings. A claim answers both (§4a) — and the check
        # and the insert are one transaction, so eviction cannot have decided
        # to drop this range while this decided to rewrite it.
        # The OPERATION's owner, not a fresh one. A rewrite driven by a config
        # change runs inside that change's whole-log claim, and minting an
        # owner here would make this merge a rival to the operation it is part
        # of — refused by its own claim, silently doing nothing.
        claim = self._buffer.claim(
            "compact", lo, hi, owner or new_owner(), self._key(target)
        )
        if not claim.acquire():
            return

        # The PREMISE re-read, now that the range is held. The file list came
        # from before the claim, and a claim taken after the read isolates
        # nothing on its own: eviction can have claimed this range, committed
        # its removal and released it in between — which is a millisecond
        # unless this thread stalls, and a stall past the TTL is precisely the
        # threat the TTL exists for. The merge would then read the sources from
        # a pre-eviction snapshot, still on disk under I6's grace, and
        # `_commit` would retry the swap onto the fresh table and put every
        # evicted row back.
        table.reload()
        current = table.data_files()
        live = {f.path for f in current}
        if not all(f.path in live for f in run):
            claim.release()

            return

        # The archive premise too, not only the inputs' liveness. The run was
        # grouped at pass start against the watermark as it was then, and a
        # sync that ran since — under a policy whose grouping settles a partial
        # prefix of this run — can have pushed part of it. Merging what is left
        # commits a LOCAL file straddling the archive's extent, and nothing
        # re-cuts a local straddler: `rewrite_archive` works the other side.
        #
        # The archive read DURABLY here, not from this object's memory. A
        # compaction pass holds no pass-level claim — only per-run ones — so a
        # `set_archive` is free between two runs of one pass, and the shipped
        # writer calls it on every restart. Answered from pass-start memory,
        # this guard would report "no archive" for the rest of a pass that now
        # has one, and skip itself entirely.
        # From then on every push is refused by `_refuse_straddle`, the
        # watermark never advances again, and eviction pins on it.
        if table is self._table:
            archived = self.archived_prefix(current, None)
            if any(f.lo <= archived for f in run):
                claim.release()

                return

        self._buffer.claim_compaction(lo, hi, self._key(target))
        try:
            # The claim renews itself while the merge runs. A rewrite over a
            # large run outlasts the TTL, and letting it lapse would invite
            # another owner onto the same range mid-write.
            self._write_merge(
                table,
                run,
                rel_path,
                target,
                _both(claim.renew, heartbeat),
                upload=upload,
            )
        finally:
            claim.release()

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

    def rewrite_sorted(
        self,
        heartbeat: Callable[[], bool] | None = None,
        owner: str | None = None,
    ) -> None:
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
            self._rewrite_run(self._table, [data_file], owner=owner)
            checkpoint(heartbeat)

    # -- eviction -----------------------------------------------------------

    def _retention_boundary(self) -> int:
        """The highest offset the retention policies would drop, before I4.

        Files cover contiguous non-overlapping ranges, so evicting a prefix is
        a single upper bound. Anything newer is untouched — and each policy is
        one such bound, so honouring both is taking the lower. They are floors
        on what stays readable locally, so the one that retains MORE wins,
        which is the opposite of how the seal combines its two limits (§12).

        Its own method because eviction computes it twice: once to decide
        whether there is work and what range to claim, and again under that
        claim, where the answer is the one acted on.
        """
        # ONE read, held for the whole decision. Each `self.config` is an
        # independent read of the durable row now, so two of them inside one
        # decision can disagree — and here they did arithmetic on each other:
        # `local_rows` seen as an int by the test and as None by the
        # subtraction is `int - None`, a TypeError out of `maintain()`. The
        # shipped maintainer catches RuntimeError and CommitFailedException, so
        # that killed the process and stopped maintenance entirely.
        #
        # The fix is not a lock: it is that a decision reads the policy once.
        # Fresh per decision, coherent within it.
        config = self.config
        limits: list[int] = []
        retention = config.local_retention
        if retention is not None:
            cutoff = datetime.now(UTC) - retention
            stale = [f for f in self._table.data_files() if self._written(f) < cutoff]
            # Nothing old enough is a limit of zero, not an absent one: it
            # means this policy would keep everything.
            limits.append(max((f.hi for f in stale), default=0))

        if config.local_rows is not None:
            # Offsets are contiguous, so the newest `local_rows` of them start
            # here. AUTOINCREMENT can leave a gap where a transaction rolled
            # back, which makes this retain slightly more than asked rather
            # than less — the safe direction for a floor.
            limits.append(self._buffer.next_offset() - 1 - config.local_rows)

        return min(limits) if limits else 0

    def evict(self) -> None:
        """Drop files older than `local_retention` from the local table (§8).

        Age comes from the snapshot that added the file, not from any data
        column — the library stamps no timestamp (§2).

        With no archive this is deletion of the only copy. That is the contract
        a local-only log with a retention asks for; see §8.
        """
        # The POLICY re-read first, because it decides everything below. Read
        # again under the claim as well: this one only decides whether there is
        # work, and the one that decides the deletion has to be the guarded one.
        config = self.config
        if config.local_retention is None and config.local_rows is None:
            return

        # Same reason as `compact`: this decides what to drop from the ages a
        # handle reports, and a stale one reports a table that has moved.
        self._table.reload()
        self._age_cache = None

        # Provisional: enough to decide whether there is work at all, and what
        # range to claim. The answer that gets acted on is recomputed below,
        # under the claim.
        boundary = self._retention_boundary()
        files = self._table.data_files()
        boundary = max((f.hi for f in files if f.hi <= boundary), default=0)
        if boundary <= 0:
            return

        # The prefix claimed before anything that decides a deletion is read
        # under it, and eviction declares rather than merely consulting (§4a).
        # An operation that only reads has decided and said nothing durable, so
        # a merge claiming a range that straddles this boundary between the
        # read and the commit puts back exactly what this is about to drop.
        # Both sides declaring in one transaction makes the ordering total.
        #
        # Claimed on the UNCLAMPED boundary, which only ever falls from here —
        # so this covers a superset of what is removed. Claiming the clamped
        # range would mean reading the archive's premise outside the claim,
        # which is the defect below.
        removal = self._buffer.claim("evict", 0, boundary, new_owner())
        if not removal.acquire():
            return

        # Everything that decides a deletion, re-read UNDER the claim, because
        # everything read before it is a statement about the past.
        #
        # `sync` learned this for itself — "UNDER the lease, not before it" —
        # and eviction acts on the same facts without having learned it. The
        # window is not narrow: `set_archive` is documented as something the
        # shipped writer calls on every restart, and it takes the whole log,
        # which is free precisely while this holds nothing. Attaching an
        # archive between the read and the acquire left this deleting the only
        # copy of every aged row the new archive was configured to receive, and
        # sync can never push them afterwards because they have left the table.
        # Re-pointing left it evicting on a clamp earned by the OLD archive,
        # whose rows the read path no longer scans.
        self._table.reload()
        self._age_cache = None
        files = self._table.data_files()
        boundary = min(boundary, self._retention_boundary())

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
            boundary = min(boundary, self.archived_prefix(files, self._archive.uri))

        # Snapped DOWN to a file boundary, against the list as it is now. The
        # age limit is already one — some file's `hi` — and so is the archive
        # clamp, but the row floor is arbitrary and lands mid-file on most
        # passes. A mid-file boundary is not a smaller eviction:
        # `evict_through` filters by row, so pyiceberg rewrites the straddling
        # file copy-on-write at a path this library never learns. That breaks
        # the rule the whole deletion design rests on — every file's path is in
        # SQLite before the file exists (I2) — and leaves the superseded
        # original out of the queue, so once expiry drops the snapshots naming
        # it, nothing can name it again.
        boundary = max((f.hi for f in files if f.hi <= boundary), default=0)
        if boundary <= 0:
            removal.release()

            return

        # Everything the boundary REMOVES, not just what looked old enough to
        # trigger it. A compaction output has a fresh snapshot age, so it never
        # appears in `stale` — but its offsets can sit below a stale file's
        # `hi`, so the boundary drops it too. Queueing only `stale` left it
        # removed from the table and named by nothing.
        try:
            # Still ours? The merge path asks this immediately before its
            # commit and eviction did not, which left the asymmetry that
            # matters: this claim expires 30 s after it was taken, and a stall
            # past the TTL is the threat the TTL exists for. Lapsed, a
            # compaction may claim a run below this boundary and pass its own
            # premise check truthfully, because the sources ARE still live —
            # and then this commit removes them and the merge, whose claim is
            # valid throughout, commits them back with a fresh `named_at` that
            # shields them for another whole retention period.
            #
            # In the other order it is worse: the merge lands first, this
            # commit's CAS retries onto the fresh table, and the delete removes
            # the merged output, which is in nobody's deletion queue. Once
            # expiry drops the snapshots naming it, nothing can name it again.
            checkpoint(removal.renew)
            self._enqueue(f.path for f in files if f.hi <= boundary)
            self._table.evict_through(boundary)
        finally:
            removal.release()

    def rewrite_archive(
        self,
        heartbeat: Callable[[], bool] | None = None,
        owner: str | None = None,
    ) -> None:
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
        # `repair=True`: this holds the maintenance lease, which is what makes
        # replacing an entry that names another prefix safe. Opening with
        # `repair=False` here meant `maintain` and `rewrite_archive` failed
        # after a re-point with an error telling the operator that a
        # maintenance pass would fix it — which they are.
        archive = self._archive.table(repair=True)
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
        self._recut(archive, stale, heartbeat, owner)

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
        target = self.config.compact_size
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
        owner: str | None = None,
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
            durable=False,
        )
        # The scratch reads its cut size from its OWN `meta`, like every
        # buffer, so it needs a policy written into it. It is a fresh database
        # with no config row, and without this it would silently size its cuts
        # by the library defaults rather than by the target this rewrite is
        # re-cutting to — which is the entire point of the operation.
        # BOTH targets mapped, not only the size. The scratch cuts at whichever
        # ceiling comes first, so carrying the live seal ROW cap made a rewrite
        # cut its outputs at the seal's row limit while the archive holds files
        # sized to the compact one — eight times more files than it started
        # with, each still undersized by bytes, so the next `rewrite_archive`
        # flags the same tail again and the operation never converges. It is
        # meant to merge undersized archived files; that inverted it.
        config = self.config
        scratch.set_meta(
            CONFIG_KEY,
            replace(
                config,
                target_seal_size=config.compact_size,
                target_seal_rows=config.compact_rows,
            ).to_json(),
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
        cutoff = datetime.now(UTC) - self.config.snapshot_retention

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

        # CLAIMED, because what follows opens the archive with `repair` on.
        # Expiry is exempt from claims on the grounds that it is a metadata
        # commit CAS orders — true of the snapshot expiry, and not true of a
        # repairing open, which DROPS a catalog entry naming another prefix and
        # creates a table in its place. That privilege belongs to a claim
        # holder: two of them at once collide on the first attempt, because
        # pyiceberg writes the metadata object before inserting the catalog
        # row, and the loser raises a bare `Exception` the shipped maintainer
        # does not catch. Worse, a claimless drop can land after a claim holder
        # has already created and registered, taking the live entry with it.
        #
        # Rounds nine and ten fixed WHICH archive a repairing open targets.
        # This is the other half — who is entitled to repair one — and this
        # call site inherited expiry's exemption without it applying.
        sweep = self._buffer.claim("expire-archive", 0, EVERYTHING, new_owner())
        if not sweep.acquire():
            return

        try:
            self._expire_archive_claimed(cutoff, sweep)
        finally:
            sweep.release()

    def _expire_archive_claimed(self, cutoff: datetime, sweep: Claim) -> None:
        """The archive half of expiry, with the claim held. See `_expire_archive`."""
        archive = self._archive.table(repair=True)
        if archive is None:
            return

        checkpoint(sweep.renew)

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
        cutoff = datetime.now(UTC) - self.config.snapshot_retention
        due = self._buffer.due_deletions(int(cutoff.timestamp()))
        if not due:
            return

        # Claimed, because this UNLINKS. §4a calls expiry safe to run
        # claimless on the grounds that it is a metadata commit ordered by CAS,
        # and that is true of the expiry; it is not true of the deletion that
        # follows it. Consulting the table without declaring anything leaves
        # the window every other pass here was made to close: `hydrate`
        # re-registers a file under the very name the queue still holds — it
        # reuses the archived key deliberately — and it can commit that
        # between this veto being read and the file being unlinked. The local
        # table then references a file that is not there, and every scan over
        # that range raises until eviction ages the entry out.
        #
        # The whole log, since the queue names files from anywhere in it. A
        # refusal costs nothing: the entries stay due and the next pass takes
        # them.
        sweep = self._buffer.claim("drain", 0, EVERYTHING, new_owner())
        if not sweep.acquire():
            return

        try:
            # Reloaded first. This veto is the last thing standing between the
            # deletion queue and an unrecoverable mistake, and asked of a handle
            # that predates another process's commit it reports a live file as
            # unreferenced. Every other cost in this pass dwarfs a catalog resolve.
            self._table.reload()
            referenced = self._table.referenced_paths()
            # Only if the queue holds something remote, so an ordinary drain on a
            # local-only log still opens nothing. `rewrite_archive` is what puts
            # remote entries here, and it is an operation somebody ran on purpose.
            remote = (
                self._archive.table(repair=True)
                if any(is_remote(p) for p in due)
                else None
            )
            remote_referenced = set() if remote is None else remote.referenced_paths()

            for rel_path in due:
                if is_remote(rel_path):
                    if remote is None or rel_path in remote_referenced:
                        continue

                    # Only objects belonging to the archive this log is pointed at.
                    # A queued remote path names the archive it was superseded in,
                    # and the veto above asks the CURRENT one — so after a
                    # re-point, entries left by a rewrite on the old archive would
                    # be checked against a new archive that references nothing and
                    # deleted from the old bucket, where they may still be live and
                    # may be the only copy of rows already evicted locally.
                    #
                    # Left queued rather than forgotten: they are somebody's to
                    # resolve, and the log that owns that archive is the one that
                    # can say whether they are dead. A stranded queue row is a
                    # bounded cost; deleting live data in a bucket this log no
                    # longer understands is not.
                    #
                    # Normalised, because the configured URI may carry a trailing
                    # slash while every queued path is built from it stripped. The
                    # mismatch would classify this log's OWN objects as another
                    # archive's and wedge the remote queue permanently.
                    if not rel_path.startswith(
                        f"{(self._archive.uri or '').rstrip('/')}/"
                    ):
                        continue

                    checkpoint(sweep.renew)
                    remote.remove(rel_path)
                    self._buffer.forget_deletion(rel_path)
                    continue

                path = self._layout.absolute(rel_path)
                if str(path) in referenced:
                    # A compaction can re-register a path the queue still holds.
                    # Deleting a referenced file is unrecoverable, so the check is
                    # worth its cost even though the grace period should preclude it.
                    continue

                # Still ours, asked before EVERY deletion rather than once at
                # the top. The unlink is this pass's commit, and §4a's rule
                # applies to it like any other: holding a claim is asked again
                # at the commit. It matters here because everything slow in
                # this pass sits between the veto being read and the deletions
                # — opening the archive, walking its manifests, and one remote
                # round trip per queued object, measured at ~650 ms each. Past
                # the TTL, a `hydrate` may lawfully take the whole log, register
                # a file under the very name still queued here, and release;
                # this would then unlink it against a stale veto and leave the
                # local table pointing at a file that is not there.
                #
                # Entries left behind cost nothing: they stay due.
                checkpoint(sweep.renew)
                # Unlink first, forget second. A crash between them leaves a row
                # whose unlink is already a no-op; the reverse leaks the file with
                # nothing left pointing at it.
                path.unlink(missing_ok=True)
                self._buffer.forget_deletion(rel_path)

        finally:
            sweep.release()

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
