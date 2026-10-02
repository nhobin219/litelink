"""Local storage reclamation: compact, evict, expire, drain (SPEC §6, §8, §12).

Separate from the write path because it shares nothing with it but the tables it
reads, and separate from `WriteHandle` because it is the half of the library
with no opinion about appends.

The four run in order and none is useful alone. Compaction alone INCREASES
storage, since superseded files stay referenced until their snapshots expire.
Eviction alone frees no disk, since it removes a file from the current snapshot
while the previous one still references it. Expiry deletes no files at all —
pyiceberg's is metadata-only. Draining is what actually unlinks, and it waits
`staging_snapshot_retention` so a running scan does not lose files underneath it
(I6).

The published table's expiry, drain and sweep live here too, but `publish` runs
them (`tidy_published`): `maintain` never needs the network.
"""

from __future__ import annotations

import contextlib
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from litelink._buffer import _NO_ROW_LIMIT, OFFSET, Buffer
from litelink._claim import EVERYTHING, Claim, new_owner
from litelink._fs import write_parquet
from litelink._published import Published
from litelink._statistics import rollup
from litelink._tiers import PublishedTier

_log = logging.getLogger(__name__)

# The stranded-metadata sweep (#113, `Maintenance._sweep`). How often a table's
# `metadata/` is listed: often enough that a lost commit race does not linger,
# rarely enough that the LIST is noise. A process lists at its first pass, so a
# restart is the other trigger.
SWEEP_INTERVAL = timedelta(hours=4)
# Deletions per pass. One S3 delete is a round trip, ~50 ms, so a backlog of
# thousands would otherwise hold one pass for minutes.
SWEEP_BATCH = 500
# The floor under the age a file must reach, whatever a table's retention says.
# A commit writes its files before it swaps the pointer that makes them live,
# and a retention of zero — which tests and demos set — must not let the sweep
# take a commit's manifests out from under it.
SWEEP_MIN_AGE = timedelta(hours=1)
# Concurrent deletes when `retire` sweeps everything at once.
SWEEP_THREADS = 32


@dataclass
class _SweepState:
    """One table's sweep, between passes. In memory only (`_sweep`)."""

    pending: list[str] = field(default_factory=list)
    # `time.monotonic()` at which to list again; 0 lists at the first pass.
    next_listing: float = 0.0
    deleted: int = 0


# Where the log records its settings. Beside `PUBLISHED_KEY` in spirit: not
# `WriteHandle`'s private business, because eviction decides deletions from it.
CONFIG_KEY = "config"

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

    import pyarrow as pa

    from litelink._buffer import Buffer
    from litelink._config import LogConfig
    from litelink._layout import Layout
    from litelink._table import DataFile, LogTable


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
    groups, and `publish` refuses to push a file that appears in one, since a
    file pushed and then merged in staging leaves the published table holding
    rows that have been rewritten underneath it.

    Sizes come from `memory` — what each file holds uncompressed, as the
    appender counted it — and the budget is `target_compact_size`, which is
    stated in those same units. That correspondence is the point. Sizing this
    by the files' size on disk is what the code here used to do, and on data
    that compresses 8:1 it merged eight files that were each already full, into
    one holding eight times the memory the target allows.

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

    `rows` is `target_compact_rows`, and it has to be here for the same reason.
    A seal that cut on the row limit produced a file holding fewer bytes than
    `target_compact_size`, which by bytes alone looks starved — so compaction
    would merge exactly the files the row cap just created, straight back past
    it. Whichever ceiling the seal respected, this respects too.
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

    What `publish` needs to know, asked directly. It used to ask a proxy
    question — "is this file at least half the target?" — and measure it on
    disk, which fails outright on compressible data: a 64 KiB buffer of
    repetitive rows seals to under 8 KiB, so no file ever reached half of 64
    KiB, `publish` pushed nothing, and the published table stayed empty with
    nothing to indicate why. Compaction's own rule has no such blind spot, and
    it is the rule that actually matters, since the only reason to hold a file
    back is that compaction might rewrite it.

    Two things disqualify a file. It sits in a run compaction would merge right
    now; or it sits in the trailing run, which is under budget and so still has
    room for files that have not been written yet. Everything before the first
    such file is settled: no run containing it can also contain anything new,
    because the files between them already fill the budget.

    A small file in the MIDDLE is therefore pushed, not held. It is under the
    target and always will be — its neighbours are too big to merge with, so
    compaction will not touch it and waiting achieves nothing. Holding it was
    the old behaviour and it meant a single explicit `seal()` blocked the
    published table permanently: everything after it is newer, so the watermark
    never advanced again and I4 then pinned local disk too. Not "later" — never.
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


def _covered(ranges: Sequence[tuple[int, int]], start: int, end: int) -> bool:
    """Whether `[start, end)` sits entirely inside `ranges`, which are sorted.

    A walk rather than a set membership test, because the published table's cuts
    need not line up with anyone else's. Adjacent published files join — offsets
    are contiguous, so `[1, 151)` and `[151, 301)` together hold `[101, 201)`
    even though neither holds it alone — and a gap ends the answer.
    """
    if end <= start:
        # An empty range is held by anything, vacuously. Unreachable from I4,
        # where a file always holds at least one row, but a predicate that
        # answers "no" to a question with no rows in it is a trap for the next
        # caller.
        return True

    reach = start
    for held_start, held_end in ranges:
        if held_start > reach:
            return False

        if held_end > reach:
            reach = held_end
            if reach >= end:
                return True

    return reach >= end


def is_remote(path: str) -> bool:
    """Whether a queued deletion names a published file rather than a staging
    one.

    The queue holds root-relative names for staging files and full URIs for
    published ones — `file://` too, when the published table is a local
    directory (#98) — and a URI is the only thing that can carry a scheme: a
    relative path never contains "://". One queue for both, because the grace
    period, the reference veto and the ordering that makes them safe are
    identical either side of the network.
    """
    return "://" in path


def _undersized_from(
    run: Sequence[DataFile], held: Mapping[str, int], target: int
) -> list[DataFile]:
    """The tail of `run` from its first under-target file, or nothing.

    §6's rule inside one dense segment: everything before the first short file
    already holds a full target and re-cutting it would rewrite bytes to
    reproduce them; everything after has to move regardless of its own size,
    because the shortfall ahead of it shifts every boundary behind it.

    A file whose size was never recorded counts as full, so a published table
    whose `extent` rows were lost is left alone rather than rewritten on a
    guess.
    """
    for index, data_file in enumerate(run):
        if held.get(data_file.path, target) < target:
            return list(run[index:])

    return []


class Maintenance:
    """The reclamation passes for one log."""

    def __init__(
        self,
        table: LogTable,
        buffer: Buffer,
        layout: Layout,
        published: Published,
    ) -> None:
        self._table = table
        self._buffer = buffer
        self._layout = layout
        self._published = published
        self._tiers = PublishedTier(buffer)
        # Read once per eviction pass rather than once per file. Cleared at the
        # top of `evict`, so a pass never decides from what a previous one saw.
        self._age_cache: dict[str, int] | None = None
        self._sweeps: dict[str, _SweepState] = {}

    @property
    def config(self) -> LogConfig:
        """The policy in force, read from the log on every access.

        No copy is kept here. A second copy is what every one of this seam's
        defects came down to: a decision read the copy while the durable row
        said otherwise, and correctness then depended on a refresh call sitting
        at each decision — twelve of them, and always one short somewhere.
        """
        return self._buffer.config()

    @property
    def sort_by(self) -> tuple[str, ...]:
        """The declared clustering, read from the log on every access.

        No copy is kept here, for the reason `config` keeps none: this pass
        rewrites files, and a rewrite that clusters by a key the table no
        longer declares is a table lying about itself. `set_sort_by` used to
        push the new value in here; a maintainer in another process never got
        that call.
        """
        return self._buffer.sort_by()

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

    PUBLISHED_THROUGH_KEY = "published_through"

    def published_through(self) -> int:
        """Highest offset the published table is known to hold, 0 if none (§5,
        I4).

        A prefix, always: files cover contiguous non-overlapping ranges (§4)
        and `publish` pushes them in order, so one integer describes it.

        Cached in `meta` rather than read from the published table, so eviction
        can ask a keyed read instead of a network round trip to find out what it
        may drop.
        """
        recorded = self._buffer.get_meta(self.PUBLISHED_THROUGH_KEY)

        return 0 if recorded is None else int(recorded)

    def published_prefix(
        self, files: Sequence[DataFile], prefix: str | None, *, include_intents: bool
    ) -> int:
        """The `end` of the longest prefix of `files` the published table holds
        (§4a), or 0 when it holds none of it.

        I4 asked of segments. A file is the published table's business if the
        published table holds THAT FILE'S ROWS, which `publish` wrote down when
        it pushed it. The walk stops at the first file not fully held, so the
        answer stays a prefix — which is what eviction needs, since it removes
        one.

        **Coverage, not equality.** The two tiers cut the same rows into files
        independently, and asking whether a staging range EQUALS a published one
        was wrong the moment they could differ. `rewrite_published` re-cuts the
        published table to different boundaries by design — that is its entire
        job — and every staging file then matched nothing, for ever: eviction
        clamped to zero and stopped, and compaction stopped seeing published
        files as the published table's business and merged across its span.
        Neither heals, because nothing ever re-cuts the published table back.

        Exact rather than conservative in both directions, and that is the
        point. A watermark had to be raised before a register to cover the crash
        between the two, so it named ranges the published table might not hold,
        and it had to be reset when the log was re-pointed, so it went backwards
        past ranges the published table did hold. Neither is expressible here:
        the row is written when the copy exists, and it names the bucket it went
        to.
        """
        ordered = sorted(files, key=lambda f: f.start)
        if not ordered:
            return 0

        covered = self._buffer.published_ranges(
            prefix, ordered[0].start, include_intents=include_intents
        )
        reached = 0
        for data_file in ordered:
            if not _covered(covered, data_file.start, data_file.end):
                break

            reached = data_file.end

        return reached

    def compact(self, heartbeat: Callable[[], bool] | None = None) -> None:
        """Merge runs of undersized adjacent files (§6).

        Real work on the happy path. Not repair — the cut is exact and there is
        no timer to cut early, so every file a seal writes already holds what
        it should — but conversion: `target_compact_size` defaults to eight
        times `target_seal_size`, so eight sealed files become one, before they
        are published. It also picks up the deliberate exceptions: an explicit
        `seal()`, which cuts short by definition, and a change to
        `target_compact_size`, which leaves existing files sized for the old
        value. A no-op only where the two targets are set equal.

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

        # Published files are never inputs. A merge spanning the published
        # table's span either duplicates the rows already pushed or strands the
        # ones above them, and skipping them makes that unreachable. They are a
        # prefix, so dropping them cannot break adjacency.
        #
        # Asked per file (§4a). It also keeps the two tiers' ranges aligned:
        # a file the published table holds is never rewritten in staging, so
        # the staging range and the published range stay the same range, which
        # is what lets `published_prefix` match them at all.
        local = self._table.data_files()
        # Asked of ANY published table, not only the one the log points at
        # now. A merge across a range some published table holds makes a
        # staging file whose boundaries line up with nothing there — and
        # nothing re-cuts a STAGING straddler, so pointing back at that
        # published table stalls the log for good: eviction pins below the
        # straddler and every push is refused. Four legitimate operations reach
        # it — point away, raise the target, maintain, point back — with no
        # warning at any step.
        #
        # Skipping them is not free, and the earlier claim that it was — "a
        # file with a published copy is already at the target" — is false the
        # moment the target is RAISED after the copy was made, which is the
        # scenario this exists for. What it costs is that such a file stays at
        # the size it was published at; `rewrite_published` is the tool for
        # that. What it buys is that no merge can ever straddle a range a
        # published table holds. `_push` applies the same exclusion, or the two
        # deadlock.
        published = self.published_prefix(local, None, include_intents=True)
        pending = [f for f in local if f.end > published]

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
        published table's entries are the staging ones carried across the push.
        A staging file is recorded root-relative and named absolutely by the
        table; a published one is recorded and named by the same URI.
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

        Both tiers, one path. A staging compaction and a published rewrite
        differ only in which table they commit to and whether the output is
        uploaded afterwards; everything that makes either safe — the claim
        before the file exists, re-sorting, verification, queueing the sources
        before the commit that supersedes them, carrying their measured sizes
        onto the output — is identical, and was identical when it was written
        twice.
        """
        start, end = run[0].start, run[-1].end
        # Unique per attempt. See `compaction_path`: a fixed name made a
        # rewrite of a previous compaction write over the file it was reading.
        rel_path = self._layout.compaction_path(start, end, uuid.uuid4().hex[:8])
        target = table.uri(rel_path) if upload else str(self._layout.absolute(rel_path))
        # Claimed before the file exists, exactly as a seal claims its path
        # (I2). One that dies between the write and the commit is then
        # recoverable by name, instead of being a file nobody can identify
        # without listing — which for a remote published table would be a
        # paginated LIST over object storage, the thing this design refuses.
        # Claimed as the TARGET, so recovery knows which tier to remove it from.
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
            "compact", start, end, owner or new_owner(), self._key(target)
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

        # The published premise too, not only the inputs' liveness. The run was
        # grouped at pass start against the watermark as it was then, and a
        # publish pass that ran since — under a policy whose grouping settles a
        # partial prefix of this run — can have pushed part of it. Merging what
        # is left commits a STAGING file straddling the published table's span,
        # and nothing re-cuts a staging straddler: `rewrite_published` works the
        # other side.
        #
        # The published ranges read DURABLY here, not from this object's
        # memory. A compaction pass holds no pass-level claim — only per-run
        # ones — so a `set_published` is free between two runs of one pass, and
        # the shipped writer calls it on every restart. Answered from pass-start
        # memory, this guard once reported "no archive" for the rest of a pass
        # that had since been given one, and skipped itself entirely.
        # From then on every push is refused by `_refuse_straddle`, the
        # watermark never advances again, and eviction pins on it.
        if table is self._table:
            published = self.published_prefix(current, None, include_intents=True)
            if any(f.start < published for f in run):
                claim.release()

                return

        self._buffer.claim_compaction(start, end, self._key(target))
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
        start, end = run[0].start, run[-1].end
        merged = table.scan_range(start, end)
        order = self.sort_by
        if order:
            # Re-sorted, not merely concatenated: concatenation would leave the
            # row groups carrying each source file's range, which is the
            # statistic the sort exists to tighten.
            merged = merged.sort_by([(c, "ascending") for c in order])

        _verify(merged, run, start, end)

        # Written locally either way, because Parquet is written to a file and
        # the alternative is holding a second copy of the run in memory. For
        # the published table it is a scratch copy under the name it will have
        # there, uploaded and then removed.
        dest = self._layout.absolute(rel_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        write_parquet(merged, dest, self.config.compression)
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
        table.replace_range(start, end, [target])
        # Re-dated to the commit, for the reason `restamp_deletions` gives: a
        # merge that failed between the queueing and this line and was retried
        # later would otherwise supersede these files against a stamp already
        # spent.
        self._buffer.restamp_deletions(
            (self._key(f.path) for f in run), int(datetime.now(UTC).timestamp())
        )
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
        """The `end` below which the retention policies would drop, before I4.

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
        # `staging_rows` seen as an int by the test and as None by the
        # subtraction is `int - None`, a TypeError out of `maintain()`. The
        # shipped maintainer catches RuntimeError and CommitFailedException, so
        # that killed the process and stopped maintenance entirely.
        #
        # The fix is not a lock: it is that a decision reads the policy once.
        # Fresh per decision, coherent within it.
        config = self.config
        limits: list[int] = []
        retention = config.staging_retention
        if retention is not None:
            cutoff = datetime.now(UTC) - retention
            stale = [f for f in self._table.data_files() if self._written(f) < cutoff]
            # Nothing old enough is a limit of zero, not an absent one: it
            # means this policy would keep everything.
            limits.append(max((f.end for f in stale), default=0))

        if config.staging_rows is not None:
            # COUNTED, not subtracted from the frontier. `next_offset() - 1 -
            # staging_rows` reads naturally and assumes the offset space is
            # dense — true of a rollback's occasional gap, and false the moment
            # anything reserves a range. A restore skips 2**20 offsets to keep
            # I9 (§3a), so the subtraction would put the boundary 2**20 above
            # every staging file, and the first `maintain()` after a failover
            # would evict the whole staging window, clamped only by I4. The
            # comment here used to say the arithmetic errs toward retaining
            # MORE, which is the safe direction for a floor; across a large
            # hole it errs the other way.
            #
            # Iceberg records a row count per file, so counting back from the
            # newest costs nothing beyond the manifest read `data_files`
            # already did.
            kept = 0
            for data_file in sorted(
                self._table.data_files(), key=lambda f: f.end, reverse=True
            ):
                if kept >= config.staging_rows:
                    # This file lies entirely outside the window, so everything
                    # up to its end may go.
                    limits.append(data_file.end)
                    break

                kept += data_file.rows
            else:
                # Every staging file is inside the window: keep all of them.
                limits.append(0)

        return min(limits) if limits else 0

    def evict(self, *, everything: bool = False) -> None:
        """Drop files older than `staging_retention` from the staging table (§8).

        `everything` drops every file the published table holds, whatever the
        policy — what `retire()` ends with. I4 still clamps it: a file the
        published table has not registered stays.

        Age comes from `extent.named_at` — the log's own record of when the
        file was named — falling back to the Iceberg snapshot that added it
        when there is no row. Not from any data column: the library stamps no
        timestamp on rows (§2). Snapshot-only was the earlier version and the
        bug that silently stopped reclaiming anything, because expiry retires
        the snapshot the age was being read from.

        Never deletion: every log has a published table (#98), and I4 keeps a
        file until it holds it.
        """
        # The POLICY re-read first, because it decides everything below. Read
        # again under the claim as well: this one only decides whether there is
        # work, and the one that decides the deletion has to be the guarded one.
        config = self.config
        if (
            not everything
            and config.staging_retention is None
            and config.staging_rows is None
        ):
            return

        # Same reason as `compact`: this decides what to drop from the ages a
        # handle reports, and a stale one reports a table that has moved.
        self._table.reload()
        self._age_cache = None

        # Provisional: enough to decide whether there is work at all, and what
        # range to claim. The answer that gets acted on is recomputed below,
        # under the claim.
        files = self._table.data_files()
        boundary = (
            max((f.end for f in files), default=0)
            if everything
            else self._retention_boundary()
        )
        boundary = max((f.end for f in files if f.end <= boundary), default=0)
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
        # range would mean reading the published table's premise outside the
        # claim, which is the defect below.
        removal = self._buffer.claim("evict", 0, boundary, new_owner())
        if not removal.acquire():
            return

        # Everything that decides a deletion, re-read UNDER the claim, because
        # everything read before it is a statement about the past.
        #
        # `publish` learned this for itself — "UNDER the lease, not before it" —
        # and eviction acts on the same facts without having learned it. The
        # window is not narrow: `set_published` is documented as something the
        # shipped writer calls on every restart, and it takes the whole log,
        # which is free precisely while this holds nothing. Attaching a
        # published table between the read and the acquire left this deleting
        # the only copy of every aged row the new published table was
        # configured to receive, and publish can never push them afterwards
        # because they have left the table. Re-pointing left it evicting on a
        # clamp earned by the OLD published table, whose rows the read path no
        # longer scans.
        self._table.reload()
        self._age_cache = None
        files = self._table.data_files()
        if not everything:
            boundary = min(boundary, self._retention_boundary())

        # I4: a file the published table still lacks must not leave the
        # staging table, because the staging copy stops being the only one
        # only once publish says so. Clamped rather than skipped, so a publish
        # that is arbitrarily far behind delays eviction instead of stopping
        # it — §5's "lazy, restartable" applies here too.
        boundary = min(
            boundary,
            # I4 asks whether the published table HAS it, so intents are
            # excluded: deleting the only staging copy on the strength of an
            # intended one is the loss this whole record exists to prevent.
            self.published_prefix(files, self._published.uri, include_intents=False),
        )

        # Snapped DOWN to a file boundary, against the list as it is now. The
        # age limit is already one — some file's `end` — and so is the published
        # table clamp, but the row floor is arbitrary and lands mid-file on most
        # passes. A mid-file boundary is not a smaller eviction: `evict_below`
        # filters by row, so pyiceberg rewrites the straddling file
        # copy-on-write at a path this library never learns. That breaks the
        # rule the whole deletion design rests on — every file's path is in
        # SQLite before the file exists (I2) — and leaves the superseded
        # original out of the queue, so once expiry drops the snapshots naming
        # it, nothing can name it again.
        boundary = max((f.end for f in files if f.end <= boundary), default=0)
        if boundary <= 0:
            removal.release()

            return

        # Everything the boundary REMOVES, not just what looked old enough to
        # trigger it. A compaction output has a fresh snapshot age, so it never
        # appears in `stale` — but its offsets can sit below a stale file's
        # `end`, so the boundary drops it too. Queueing only `stale` left it
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
            dropped = [f.path for f in files if f.end <= boundary]
            # The published table's tier row describes what it holds below the
            # staging table, and these rows are about to be exactly that — so it
            # grows BEFORE they leave, or a read between the two would skip the
            # published table for rows no staging file still has.
            schema, live = self._table.live_files()
            leaving = set(dropped)
            self._tiers.widen(
                self._buffer.shape().table,
                rollup(
                    None,
                    schema,
                    [
                        f
                        for f in live
                        if str(f.file_path).removeprefix("file://") in leaving
                    ],
                ),
            )

            self._enqueue(dropped)
            self._table.evict_below(boundary)
            # Re-dated to the commit, like every other supersession. Eviction
            # needs it without any failure at all: `hydrate` re-registers a
            # file under the very path the queue still holds, drain's veto then
            # preserves that entry rather than draining it, and the re-enqueue
            # when the hydrated file is evicted again is an INSERT OR IGNORE
            # that keeps the FIRST eviction's stamp — from `staging_retention`
            # ago. Measured: files unlinked 0.078 s after leaving the table
            # against a three-second grace.
            self._buffer.restamp_deletions(
                (self._key(p) for p in dropped), int(datetime.now(UTC).timestamp())
            )
        finally:
            removal.release()

    def rewrite_published(
        self,
        heartbeat: Callable[[], bool] | None = None,
        owner: str | None = None,
    ) -> None:
        """Re-cut undersized published files to `target_compact_size` (§6,
        ad-hoc).

        Not part of `maintain`, and not expected to be needed. The published
        table is well-sized by construction: `publish` pushes only what
        compaction has finished with, so nothing undersized reaches it in normal
        operation. Two deliberate acts break that. An explicit `seal()` can
        strand a small file between larger ones, where compaction can never
        merge it and `publish` pushes it rather than blocking the watermark for
        ever. And changing `target_compact_size` leaves history sized for the
        old value, since the published table is immutable and a size change
        applies to the future.

        **It re-ingests rather than merging.** The rows from the first
        badly-sized file onwards are appended to a scratch `Buffer` and sealed
        back out — the same append path, the same `_cut`, the same byte
        accounting, the same extent rows. That is not a saving of code so much
        as of ways to be wrong: a merge can only combine whole files, so it
        lands near the target and leaves the remainder undersized, while the
        appender cuts on the row that crosses and hits it exactly. Sizing the
        published table by a second rule that approximates the first is how this
        came to compare compressed bytes against a memory bound in the first
        place.

        The scratch buffer stays small. Sealing deletes the rows it took, so it
        holds roughly one file at a time however long the range is, and it is
        removed at the end either way.

        It is also opened without durability, because everything in it is
        derived from the published table and the published table is still there
        until the final commit. Rows arrive one source file per transaction
        rather than one per row, for the same reason.

        One commit swaps the whole range. Committing each new file as it is
        written would have each commit delete a sub-range of a file the next
        one still needs to read.
        """
        # `repair=True`: this holds the maintenance lease, which is what makes
        # replacing an entry that names another prefix safe. Opening with
        # `repair=False` here meant `maintain` and `rewrite_published` failed
        # after a re-point with an error telling the operator that a
        # maintenance pass would fix it — which they are.
        published = self._published.table(repair=True)
        if published is None:
            # Nothing published yet, so nothing to re-cut.
            return

        published.reload()
        stale = self._badly_sized(published)
        if len(stale) < 2:
            # One file, or none. A single undersized file at the end of the
            # published table is where an undersized file is allowed to be, and
            # rewriting it alone would produce the same file again.
            return

        # Queued BEFORE the commit that supersedes them, exactly as a staging
        # compaction queues its sources, and safe for the same reason: `drain`
        # refuses to delete anything the table still references, so an entry
        # made for a commit that never lands simply never comes due.
        self._enqueue(data_file.path for data_file in stale)
        self._recut(published, stale, heartbeat, owner)

    def _badly_sized(self, published: LogTable) -> list[DataFile]:
        """The published files from the first one under `target_compact_size` on.

        Everything before it already holds a full target and re-cutting it
        would rewrite bytes to reproduce them. Everything after has to move
        regardless of its own size, because the shortfall ahead of it shifts
        every boundary behind it.

        A file whose size was never recorded counts as full, so a published
        table whose `extent` rows were lost is left alone rather than rewritten
        on a guess about what it holds.

        **A file spanning a gap in the offset space is excluded outright**, and
        so is everything after it. `_recut` re-appends rows through a scratch
        buffer seeded at the range's start, so it RENUMBERS them densely — over
        a gap that hands every row above it an offset belonging to different
        data. `_recut` asserts against that, so before this exclusion a single
        gapped file made every later `rewrite_published` raise: after a failover
        reserves 2**20 offsets (§3a) the first sealed file spans the hole, and
        being the published table's first file it was in every candidate run for
        the life of the log. One un-rewritable file is the honest cost of the
        reserve; an unusable repair tool is not.
        """
        held = self.memory()
        target = self.config.compact_size

        # By dense SEGMENT, and an earlier version got this wrong in the
        # ordering that actually occurs. It tested density before size per file
        # and returned `files[index:]` from the first undersized one — which
        # still CONTAINS any gapped file after it. That is the normal shape: the
        # published table's tail file before a failover is undersized, and the
        # reserve's gapped file lands after it. Measured, `rewrite_published`
        # went on raising for the life of every restored log.
        #
        # A gap bounds a segment at both ends: a file with one inside it, and a
        # file that does not continue the previous file's range.
        #
        # A segment yielding a SINGLE file is skipped rather than returned.
        # `rewrite_published` declines a run of one — merging one file is a
        # no-op rewrite — so returning it stopped the walk and left every later
        # segment unreachable. That made the tool a permanent no-op on exactly
        # the shape it was fixed for: an undersized published table tail, then the
        # reserve's gapped file, then everything a failed-over log goes on to
        # write. The tail alone was the candidate, and nothing above the gap
        # was ever re-cut however much accumulated there.
        segment: list[DataFile] = []
        for data_file in published.data_files():
            dense = data_file.rows == data_file.end - data_file.start
            if dense and (not segment or data_file.start == segment[-1].end):
                segment.append(data_file)
                continue

            candidate = _undersized_from(segment, held, target)
            if len(candidate) > 1:
                return candidate

            # A gapped file cannot start a segment either — nothing may re-cut
            # across it — so only a dense one carries over.
            segment = [data_file] if dense else []

        candidate = _undersized_from(segment, held, target)

        return candidate if len(candidate) > 1 else []

    def _recut(
        self,
        published: LogTable,
        stale: list[DataFile],
        heartbeat: Callable[[], bool] | None = None,
        owner: str | None = None,
    ) -> None:
        """Append `stale` back through a buffer and seal it out again."""
        start, end = stale[0].start, stale[-1].end
        # Not durable, deliberately. Every row in here came from the published
        # table and is still in the published table until the single commit at
        # the end, so a crash costs a re-run rather than data — and this does
        # one transaction per source file plus a few per sealed one, every one
        # of which would otherwise fsync for a guarantee nothing here depends
        # on.
        # Removed BEFORE opening, not only after. SQLite's AUTOINCREMENT
        # assigns `max(largest existing rowid, seq) + 1`, so seeding the
        # counter DOWN is silently ignored when rows already sit above it —
        # verified: with rows at 100-105 and the sequence set to 99, the next
        # insert takes 106. A rewrite killed before its first cut leaves rows
        # in this database and no claim to recover by, so the next run would
        # re-append every row at shifted offsets, hold each one twice, pass the
        # row-count guard (which counts only what this run read), and commit
        # files whose offsets carry the wrong rows. Durable published table
        # corruption with every check green. It is derived state; starting from
        # nothing is always correct.
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
        # cut its outputs at the seal's row limit while the published table
        # holds files sized to the compact one — eight times more files than it
        # started with, each still undersized by bytes, so the next
        # `rewrite_published` flags the same tail again and the operation never
        # converges. It is meant to merge undersized published files; that
        # inverted it.
        config = self.config
        scratch.set_meta(
            CONFIG_KEY,
            replace(
                config,
                target_seal_size=config.compact_size,
                target_seal_rows=config.compact_rows,
            ).to_json(),
        )
        written: list[tuple[str, int, int, int]] = []
        expected = 0
        try:
            # The rows keep the offsets they already have (§4), so the counter
            # resumes at the start of the range rather than at 1. Reassigned
            # rather than supplied, which is what keeps I11 true of the rewrite
            # as well: nothing hands an offset to `append`.
            scratch.seed_offsets(start)
            expected = 0
            for data_file in stale:
                checkpoint(heartbeat)
                rows = published.scan_range(data_file.start, data_file.end)
                # Sorted by offset and then stripped of it. The counter is what
                # reassigns them, so the rows have to arrive in the order their
                # offsets already have — files are clustered by `sort_by`, not
                # by offset, so what comes back is in neither order by default.
                # Getting this wrong would not raise anywhere: every row would
                # keep its data and be handed somebody else's offset.
                rows = rows.sort_by([(OFFSET, "ascending")])
                expected += rows.num_rows
                scratch.append(rows.drop_columns([OFFSET]).to_pylist())
                written += self._seal_scratch(scratch, published, heartbeat)

            # The tail, which by definition did not reach the target. Cutting
            # it short is what `seal()` does, and one undersized file at the
            # end is where one is allowed to be.
            scratch.close_open_group()
            written += self._seal_scratch(scratch, published, heartbeat)
        finally:
            scratch.close()
            self._discard_scratch()

        # Before the swap, not after. The commit is the point of no return:
        # it deletes the range these rows came from, so a rewrite that lost
        # any of them must fail while the originals are still the live files.
        # Against the RANGE WIDTH, deliberately, and this is one of the few
        # places that inference is right rather than a bug. `_recut` re-appends
        # through `seed_offsets(start)`, so rows are RENUMBERED sequentially from
        # `start` — which reproduces their original offsets only if the range is
        # dense. Comparing against `sum(f.rows)` instead would let a gapped
        # range pass and commit every row above the gap under an offset
        # belonging to different data, which is the corruption I9 exists to
        # prevent and which nothing downstream could detect.
        #
        # So this stays a denseness assertion. What changed is that a gapped
        # range no longer REACHES it: `_badly_sized` excludes such files, so
        # a reserved hole (§3a) leaves one file un-rewritable rather than
        # raising on every rewrite the log ever attempts afterwards.
        if expected != end - start:
            msg = (
                f"published rewrite read {expected} rows for offsets [{start}, {end}), "
                f"which is not a dense range"
            )
            raise RuntimeError(msg)

        # Checked between writing and committing, the same as `_write_merge`
        # and for the same reason — those are the two halves a lapsed claim
        # separates. This one went without, and the published side is where it
        # costs most: a rewrite that stalls past the TTL lets recovery take the
        # claim and queue every one of its outputs, `drain` snapshots its
        # reference veto once per pass while they are still unreferenced, and
        # then this commit lands. Drain deletes the objects the manifest now
        # names, the superseded originals become unreferenced and are deleted
        # in turn, and the range exists in no object at all — with every guard
        # behaving exactly as written.
        #
        # Renewing here makes that unreachable rather than unlikely: recovery's
        # acquire deleted this claim's row, so the renew finds nothing and the
        # rewrite aborts while the originals are still live.
        checkpoint(heartbeat)
        published.replace_range(
            start, end, [published.uri(p) for p, _, _, _ in written]
        )
        # The grace period starts HERE, not when they were queued. A reader
        # cannot hold a file the commit has not yet superseded, and a rewrite
        # slower than the published snapshot retention would otherwise have
        # burnt the whole of it before this line — leaving the originals due the instant they
        # stopped being referenced.
        self._buffer.restamp_deletions(
            (self._key(f.path) for f in stale), int(datetime.now(UTC).timestamp())
        )
        # Now they are the published table's, so the intents become records.
        for path, start, end, held in written:
            self._buffer.record_file(published.uri(path), start, end, held)

        # Each of this rewrite's own claims, now that the commit names every
        # object they described. Clearing the table wholesale would also retire
        # claims left by an operation that crashed and has not been recovered.
        for path, _, _, _ in written:
            self._buffer.clear_compaction(published.uri(path))

    def _discard_scratch(self) -> None:
        """Remove the rewrite scratch database and its sidecars."""
        for suffix in ("", "-wal", "-shm"):
            self._layout.rewrite_db.with_name(
                self._layout.rewrite_db.name + suffix
            ).unlink(missing_ok=True)

    def _seal_scratch(
        self,
        scratch: Buffer,
        published: LogTable,
        heartbeat: Callable[[], bool] | None = None,
    ) -> list[tuple[str, int, int, int]]:
        """Write out every extent the scratch buffer has cut, and return their
        names. The seal's own loop: take the queued range, claim the path
        before the file exists, write, and retire the extent."""
        written: list[tuple[str, int, int, int]] = []
        while True:
            queued = scratch.pending_group()
            if queued is None:
                return written

            start, end = queued
            rel_path = self._layout.compaction_path(start, end, uuid.uuid4().hex[:8])
            # Both claims. `sealing` in the scratch buffer makes the extent
            # recoverable there; `compacting` in the real one is what an
            # interrupted rewrite is found by, since the scratch database is
            # deleted on the way out.
            scratch.claim_seal(start, end, rel_path)
            self._buffer.claim_output(start, end, published.uri(rel_path))

            # Bounded at BOTH ends, though this loop's own `finish_seal`
            # would leave the floor correct anyway. That is the point: relying
            # on it is how the log's seal was wrong, and one rule is cheaper to
            # hold than a rule plus a reason it happens not to matter here.
            rows = scratch.rows_between(start, end)
            order = self.sort_by
            if order:
                rows = rows.sort_by([(c, "ascending") for c in order])

            dest = self._layout.absolute(rel_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            write_parquet(rows, dest, self.config.compression)
            # The bytes the scratch buffer counted for exactly these rows,
            # which is the same number the appender would have recorded had
            # they been cut this way the first time — and the scratch is torn
            # down before the commit, so this is the last moment they exist.
            held = scratch.group_bytes(end)
            # INTENDED before the object is written, and only recorded once
            # `replace_range` has committed. Until then these files are not the
            # published table's, so a row saying they are would let eviction
            # drop the staging copies of rows the published table does not yet
            # hold. The intent says the opposite thing to the opposite reader:
            # compaction must not merge across them, because it is about to.
            self._buffer.intend_file(published.uri(rel_path), start, end, held)
            published.put(dest, rel_path)
            dest.unlink(missing_ok=True)
            checkpoint(heartbeat)

            scratch.finish_seal(end, rel_path)
            # Carried in memory, not read back from the intent: a rival publish
            # pass that took over a lapsed claim may drop these rows while this
            # rewrite is still running, and a confirm that depended on them
            # would silently record the default size instead — which for the
            # deliberately undersized tail means `_badly_sized` treats it as
            # full for ever.
            written.append((rel_path, start, end, held))

    def _written(self, data_file: DataFile) -> datetime:
        """When a file was written, for `staging_retention` to measure against.

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
        """Expire each table's snapshots past its retention, reclaim what has
        come due, then sweep stranded metadata (§6, §8, #113)."""
        cutoff = datetime.now(UTC) - self.config.staging_snapshot_retention

        # Collect the doomed snapshots' manifest lists and manifests (and the
        # manifests their commits merged away, #111) BEFORE
        # expiring them. Afterwards their names exist nowhere: the metadata that
        # referenced them is gone, and the only remaining way to find the files
        # would be to list the directory, which `sweep` does only as a
        # backstop for files no commit ever recorded.
        doomed = self._table.expiring_paths(self._table.snapshots_older_than(cutoff))

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
        doomed = list(doomed)
        self._enqueue(doomed)
        self._table.expire_snapshots_older_than(cutoff)
        # The same correction, for the same reason. Narrower here — a manifest
        # is read at query bind rather than throughout a streaming scan — but
        # an expiry that failed between the queue and the commit and ran again
        # a pass later would otherwise retire these the instant they became
        # unreferenced.
        # Through `_key`, like the enqueue above it. `expiring_paths` returns
        # absolute paths and the queue is keyed root-relative, so passing them
        # raw made this an UPDATE matching nothing — silently, because that is
        # what SQL does. Every other restamp site happens to be key-invariant
        # (an `s3://` URI) or already mapped, which is why only this one hid.
        self._buffer.restamp_deletions(
            (self._key(p) for p in doomed), int(datetime.now(UTC).timestamp())
        )
        self.drain()
        self._sweep("staging", self._table, self.config.staging_snapshot_retention)

    def tidy_published(self, claim: Claim) -> None:
        """The published table's half of expiry, drain and sweep (§5, #113).

        Called by `publish` at the end of a pass, under the lease it already
        holds, rather than by `maintain`. Two reasons, both about what
        `maintain` must not do:

        - **No network.** `maintain` keeps a partitioned machine's local
          storage in order (§11). Expiring an S3 published table every pass
          would make each one fail where the network is down, and `publish`
          already needs the network.
        - **No published table conjured.** Opening it with `repair` creates
          one where none exists. `publish` creates it anyway; `maintain`
          has no business to.

        Every publish leaves a snapshot behind, so this is also exactly when
        there is something to expire. It used to run only once
        `rewrite_published` had queued a remote deletion, on the reasoning
        that `publish` never supersedes a file. True of data files and not of
        Iceberg's own: a table that was only ever published kept every
        snapshot, manifest list and manifest it had ever had (#113). A pass
        with nothing old enough commits nothing.

        The claim is what entitles the repairing open. Expiry alone could go
        claimless, as a metadata commit CAS orders; a repairing open DROPS a
        catalog entry naming another prefix and creates a table in its place,
        and two at once collide.
        """
        published = self._published.table(repair=True)
        if published is None:
            return

        checkpoint(claim.renew)
        cutoff = datetime.now(UTC) - self.config.published_snapshot_retention
        retiring = list(
            published.expiring_paths(published.snapshots_older_than(cutoff))
        )
        self._enqueue(retiring)
        published.expire_snapshots_older_than(cutoff)
        self._buffer.restamp_deletions(retiring, int(datetime.now(UTC).timestamp()))

        checkpoint(claim.renew)
        self._drain_published(published, claim.renew)
        self._sweep(
            "published",
            published,
            self.config.published_snapshot_retention,
            claim.renew,
        )

    def drain(self) -> None:
        """Delete staging files whose grace period has passed.

        A keyed read of `pending_delete`, not a directory walk. Every file this
        library creates has its path written to SQLite before it is written to
        disk — seals through `sealing`, compactions through `compacting` — so
        there is no category of file that could only be found by looking.

        Staging entries only. The published table's are drained by `publish`,
        through `tidy_published`, against its own retention.
        """
        # Read against the CURRENT retention, so lowering it takes effect on
        # files already queued.
        cutoff = datetime.now(UTC) - self.config.staging_snapshot_retention
        due = [
            p
            for p in self._buffer.due_deletions(int(cutoff.timestamp()))
            if not is_remote(p)
        ]
        if not due:
            return

        # Claimed, because this UNLINKS. §4a calls expiry safe to run
        # claimless on the grounds that it is a metadata commit ordered by CAS,
        # and that is true of the expiry; it is not true of the deletion that
        # follows it. Consulting the table without declaring anything leaves
        # the window every other pass here was made to close: `hydrate`
        # re-registers a file under the very name the queue still holds — it
        # reuses the published key deliberately — and it can commit that
        # between this veto being read and the file being unlinked. The staging
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
            # unreferenced. Every other cost in this pass dwarfs a catalog
            # resolve.
            self._table.reload()
            referenced = self._table.referenced_paths()

            for rel_path in due:
                path = self._layout.absolute(rel_path)
                if str(path) in referenced:
                    # A compaction can re-register a path the queue still holds.
                    # Deleting a referenced file is unrecoverable, so the check
                    # is worth its cost even though the grace period should
                    # preclude it.
                    continue

                # Still ours, asked before EVERY deletion rather than once at
                # the top. The unlink is this pass's commit, and §4a's rule
                # applies to it like any other: holding a claim is asked again
                # at the commit. Past the TTL, a `hydrate` may lawfully take the
                # whole log, register a file under the very name still queued
                # here, and release; this would then unlink it against a stale
                # veto and leave the staging table pointing at a file that is
                # not there.
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

    def _drain_published(
        self, published: LogTable, renew: Callable[[], bool] | None
    ) -> None:
        """Delete the published table's queued objects whose grace has passed.

        Under `publish`'s lease, which is the claim this needs: nothing else
        can register into the published table while it is held.
        """
        cutoff = datetime.now(UTC) - self.config.published_snapshot_retention
        due = [
            p
            for p in self._buffer.due_deletions(int(cutoff.timestamp()))
            if is_remote(p)
        ]
        if not due:
            return

        # Reloaded first, for the reason `drain` gives.
        published.reload()
        referenced = published.referenced_paths()
        # Normalised, because the configured URI may carry a trailing slash
        # while every queued path is built from it stripped. The mismatch would
        # classify this log's OWN objects as another published table's and
        # wedge the queue permanently.
        ours = f"{(self._published.uri or '').rstrip('/')}/"

        for uri in due:
            if uri in referenced:
                continue

            # Only objects belonging to the published table this log is pointed
            # at. A queued path names the published table it was superseded in,
            # and the veto above asks the CURRENT one — so after a re-point,
            # entries left by a rewrite on the old published table would be
            # checked against a new published table that references nothing and
            # deleted from the old bucket, where they may still be live and may
            # be the only copy of rows already evicted from staging.
            #
            # Left queued rather than forgotten: they are somebody's to
            # resolve, and the log that owns that published table is the one
            # that can say whether they are dead. A stranded queue row is a
            # bounded cost; deleting live data in a bucket this log no longer
            # understands is not.
            if not uri.startswith(ours):
                continue

            # Before every deletion, for the reason `drain` gives: one remote
            # round trip each, measured at ~650 ms, adds up past a TTL.
            checkpoint(renew)
            with contextlib.suppress(FileNotFoundError):
                published.remove(uri)

            self._buffer.forget_deletion(uri)

    # -- the stranded-metadata sweep (#113) ---------------------------------

    def _sweep(
        self,
        name: str,
        table: LogTable,
        retention: timedelta,
        renew: Callable[[], bool] | None = None,
    ) -> None:
        """Delete a batch of `table`'s stranded metadata, listing when due.

        A commit writes its manifests, manifest list and `metadata.json` before
        it swaps the catalog pointer. One that loses the swap, or crashes
        before it, leaves them behind under names nothing recorded — pyiceberg,
        unlike Java Iceberg, does not delete a losing attempt's files. So does
        every merge before #112. This is the one place the library LISTS a
        directory, as a backstop for those, and on a healthy table it finds
        nothing.

        Listed at the first pass after the process starts, then every
        `SWEEP_INTERVAL`; the list is held in memory and worked down
        `SWEEP_BATCH` files a pass, so a backlog never stalls one (S3 deletes
        one object per round trip). Nothing is stored: a restart lists again,
        and finds what is left.

        **No claim, because a dead metadata file cannot come back to life.**
        Every name carries a fresh UUID and is never reused, a snapshot only
        inherits manifests from the current one, and a `metadata.json` only
        builds on the current one. What the listing finds unreferenced and old
        stays dead whatever runs beside this.

        Never raises. It is housekeeping, and a pass that fails here has
        already done the work it exists for; the failure is logged and the
        next pass lists afresh.
        """
        # Per table, not per role: after `set_published` a backlog listed on
        # the old published table must not be worked through the new one.
        state = self._sweeps.setdefault(
            f"{name}:{table.metadata_location.rpartition('/')[0]}", _SweepState()
        )
        try:
            if not state.pending and time.monotonic() >= state.next_listing:
                state.next_listing = time.monotonic() + SWEEP_INTERVAL.total_seconds()
                state.pending = self._stranded(table, max(retention, SWEEP_MIN_AGE))
                if state.pending:
                    _log.info(
                        "litelink: %d stranded metadata file(s) to delete from the %s table",
                        len(state.pending),
                        name,
                    )

            batch = state.pending[:SWEEP_BATCH]
            for path in batch:
                checkpoint(renew)
                with contextlib.suppress(FileNotFoundError):
                    table.remove(path)

                state.pending.remove(path)
                state.deleted += 1

            if batch and not state.pending:
                _log.info(
                    "litelink: deleted %d stranded metadata file(s) from the %s table",
                    state.deleted,
                    name,
                )
                state.deleted = 0
        except Exception:
            _log.warning(
                "litelink: sweeping stranded metadata from the %s table failed; "
                "it is retried on the next pass",
                name,
                exc_info=True,
            )
            # Listed afresh next pass. Anything deleted before the failure is
            # not listed again, and anything not is.
            state.pending = []
            state.next_listing = 0.0

    def sweep_everything(self) -> dict[str, int]:
        """Delete every stranded metadata file in both tables now, not
        `SWEEP_BATCH` a pass. Deleted counts, by table.

        For `retire`: a retired log takes no more passes, so whatever the sweep
        has not reached by then it never will. The same rules as `_sweep`, and
        no claim, for the same reason. Raises rather than logs, so a failure
        leaves the log retiring and the next `retire()` tries again.

        **Deleted in parallel.** One S3 delete is a round trip, ~50 ms, so the
        backlog #111 found — thousands of manifests per log — would hold
        `retire` for minutes one at a time. `SWEEP_THREADS` at once brings 5,500
        to seconds, through the FileIO every other access uses, without a
        batch-delete client and the dependency it would bring.
        """
        config = self.config
        tables: list[tuple[str, LogTable, timedelta]] = [
            ("staging", self._table, config.staging_snapshot_retention)
        ]
        published = self._published.table()
        if published is not None:
            tables.append(("published", published, config.published_snapshot_retention))

        deleted: dict[str, int] = {}
        for name, table, retention in tables:
            doomed = self._stranded(table, max(retention, SWEEP_MIN_AGE))

            def remove(path: str, table: LogTable = table) -> None:
                with contextlib.suppress(FileNotFoundError):
                    table.remove(path)

            with ThreadPoolExecutor(max_workers=SWEEP_THREADS) as pool:
                # `list` so the first failure raises here, not silently.
                list(pool.map(remove, doomed))

            deleted[name] = len(doomed)

        return deleted

    def _stranded(self, table: LogTable, min_age: timedelta) -> list[str]:
        """The metadata files in `table`'s directory nothing will ever read.

        Unreferenced by every live snapshot, not a `metadata.json` the table
        still names, not already queued (`drain` owns those, with their grace),
        and older than `min_age`.

        The order is what makes it safe. The directory is listed FIRST and
        what is live read AFTER, so a commit landing in between counts as
        live. The age rule covers a commit still in flight, whose files exist
        before the swap that makes them referenced, and the manifests of a
        snapshot expired a moment ago that a reader may still hold.
        """
        # Refuse on any sign the listing names files differently from the
        # table. The current `metadata.json` and manifest list are certainly
        # live and certainly on disk; if the listing does not contain them in
        # the form `live` uses, every comparison below is meaningless and the
        # sweep would delete the table out from under itself.
        #
        # Taken BEFORE the listing, so a commit landing during it cannot look
        # like a mismatch: what was current a moment before the listing is
        # still on disk when it runs (the previous `metadata.json` versions
        # are kept, and a manifest list outlives its snapshot by a retention).
        table.reload()
        anchors = table.anchors()
        listed = table.metadata_files()
        table.reload()
        live = table.referenced_paths() | table.live_metadata()

        if not anchors <= {path for path, _ in listed}:
            if listed:
                _log.warning(
                    "litelink: not sweeping %s: its listing does not name the "
                    "table's current metadata as the table does",
                    table.metadata_location,
                )

            return []

        queued = set(self._buffer.queued_deletions())
        cutoff = datetime.now(UTC) - min_age

        return [
            path
            for path, written in listed
            if written < cutoff and path not in live and self._key(path) not in queued
        ]

    def _key(self, path: str) -> str:
        """How a file is named in SQLite, whichever tier it is in.

        Staging files root-relative, so a log directory stays movable; published
        ones by the full URI, which is already absolute and has no root to be
        relative to. One rule, used by the deletion queue, the extent table and
        the compaction claim alike, so `is_remote` can tell them apart again
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


def _verify(merged: pa.Table, run: list[DataFile], start: int, end: int) -> None:
    """§6 step 3, as far as it can be taken.

    Row count and the offset range are checked exactly; both are what the
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
    if min(offsets) != start or max(offsets) != end - 1:
        msg = "compaction changed the offset extent"
        raise RuntimeError(msg)
