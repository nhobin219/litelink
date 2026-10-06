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

The published table has the same routines — `expire_published`,
`drain_published`, `sweep_published` — each callable on its own, so an
orchestrator can run them on their own schedule or process (#118).
"""

from __future__ import annotations

import contextlib
import itertools
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pyarrow as pa

from litelink._buffer import _NO_ROW_LIMIT, PUBLISHED_THROUGH_KEY, Buffer
from litelink._claim import new_owner
from litelink._fs import stream_parquet
from litelink._layout import is_compacted
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
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence

    from litelink._buffer import Buffer
    from litelink._config import LogConfig
    from litelink._layout import Layout
    from litelink._table import DataFile, LogTable


def checkpoint(renew: Callable[[], bool] | None) -> None:
    """Renew the caller's claim, or refuse to carry on without it.

    Losing the range mid-pass is not something to push through: another owner
    may already be redoing this work, and two of them writing the same files is
    what the claim exists to prevent. A claim ends by being TAKEN, not by
    expiring — an uncontested holder renews fine — so a failed renew means
    somebody else owns these offsets now.
    """
    if renew is not None and not renew():
        msg = "lost the claim on this range mid-pass"
        raise RuntimeError(msg)


def chunks(
    batches: Iterable[pa.RecordBatch],
    schema: pa.Schema,
    size: int,
    row_cap: int | None = None,
) -> Iterator[pa.Table]:
    """Rows a row group at a time: `size` of Arrow's in-memory bytes, or
    `row_cap` rows, whichever comes first.

    A batch that would overshoot is SLICED rather than taken whole, because the
    source may be one `pa.Table` holding an entire corpus in a single chunk —
    and then "a group per batch" is one group for the load, which is the split
    not happening. Slicing an Arrow batch copies nothing.

    The per row cost is averaged over the batch it came from rather than summed
    per row: a seal counts exactly because it sees rows one at a time and this
    does not, and paying a per-row measurement to place a row-group boundary
    would cost more than the boundary is worth.

    A single row larger than the whole budget still becomes a group of one row.
    That is deliberate — the alternative is a loop that cannot advance.
    """
    held: list[pa.RecordBatch] = []
    measured = 0
    counted = 0
    for batch in batches:
        if not batch.num_rows:
            continue

        per_row = max(1, batch.nbytes // batch.num_rows)
        cursor = 0
        while cursor < batch.num_rows:
            take = batch.num_rows - cursor
            if row_cap is not None:
                take = min(take, row_cap - counted)

            take = min(take, max(1, (size - measured) // per_row))
            piece = batch.slice(cursor, take)
            held.append(piece)
            measured += per_row * piece.num_rows
            counted += piece.num_rows
            cursor += take
            if measured >= size or (row_cap is not None and counted >= row_cap):
                yield pa.Table.from_batches(held, schema=schema)
                held, measured, counted = [], 0, 0

    if held:
        yield pa.Table.from_batches(held, schema=schema)


def row_groups(
    inputs: Iterable[Iterable[pa.RecordBatch]], schema: pa.Schema, size: int
) -> Iterator[pa.Table]:
    """A merge's rows, a row group at a time, broken only between inputs.

    Whole inputs are gathered until the next would take the group past `size`
    Arrow bytes. Breaking between them is what keeps each row group's offsets
    disjoint from its neighbours': an input is sorted by `sort_by`, so cutting
    one in two leaves both halves spanning its whole offset range. An input
    larger than `size` on its own is the exception, and is cut by `chunks` —
    its pieces overlap each other, and nothing else.
    """
    held: list[pa.Table] = []
    measured = 0
    for source in inputs:
        pieces = chunks(source, schema, size)
        piece = next(pieces, None)
        if piece is None:
            continue

        following = next(pieces, None)
        if following is None:
            # The whole input fits in one row group.
            if held and measured + piece.nbytes > size:
                yield pa.concat_tables(held)
                held, measured = [], 0

            held.append(piece)
            measured += piece.nbytes
            continue

        if held:
            yield pa.concat_tables(held)
            held, measured = [], 0

        yield piece
        yield following
        yield from pieces

    if held:
        yield pa.concat_tables(held)


def runs(
    files: Sequence[DataFile],
    budget: int,
    rows: int | None = None,
) -> list[list[DataFile]]:
    """Adjacent files grouped into merge candidates, each within `budget`.

    The one definition of what compaction considers a run, because two
    collaborators act on it and they must not disagree: `compact` merges these
    groups, and `publish` refuses to push a file that appears in one, since a
    file pushed and then merged in staging leaves the published table holding
    rows that have been rewritten underneath it.

    Sizes are each file's size ON DISK, from its manifest entry, and the budget
    is `target_compact_size`, stated in the same units. A merge's output lands
    under the sum of its inputs — larger row groups compress better — so the
    budget is what a run's inputs fill, and the file comes out somewhat
    smaller.

    The budget caps the OUTPUT. Merging every adjacent small file without one
    puts no ceiling on the result — a hundred files just under the line become
    one file a hundred times the target, which is the same defect as an
    undersized file with the sign flipped. A run therefore closes when the next
    file would take it past the budget, and the pass emits several correctly
    sized files instead of one enormous one.

    `rows` is `target_compact_rows`, a ceiling a log may set as well; whichever
    binds first closes the run.

    The trailing run may still be open — see `is_open`.
    """
    limit = rows or _NO_ROW_LIMIT
    grouped: list[list[DataFile]] = []
    run: list[DataFile] = []
    held = 0
    counted = 0
    for data_file in files:
        # A merge's output counts as full whatever its size. A merge compresses
        # better than its inputs did — larger row groups — so its output lands
        # under the budget its inputs filled, by about a third on compressible
        # data (#158); counted by size, it would join the next run and be
        # rewritten again, and the file still filling would be rewritten on
        # every pass. Counted full, every row is compacted exactly once.
        size = budget if is_compacted(data_file.path) else data_file.size
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


def is_open(
    run: Sequence[DataFile], files: Sequence[DataFile], budget: int, rows: int | None
) -> bool:
    """Whether `run` can still grow: it is the trailing run, and under both
    ceilings, so files not yet written may join it.

    Compaction waits for an open run, and `publish` holds it back: merged now,
    it would be merged again — with everything sealed after it — on every pass
    until it filled.
    """
    limit = rows or _NO_ROW_LIMIT
    held = sum(budget if is_compacted(f.path) else f.size for f in run)
    counted = sum(f.rows for f in run)

    return run[-1] is files[-1] and held < budget and counted < limit


def stable_prefix(
    files: Sequence[DataFile],
    budget: int,
    min_files: int,
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
    settled = 0
    for run in runs(files, budget, rows):
        if len(run) >= min_files:
            break

        # Room under BOTH ceilings is what makes the trailing run growable. At
        # either one it is finished, and a file that cannot grow is settled.
        if is_open(run, files, budget, rows):
            break

        settled += len(run)

    return settled


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

        No copy is kept here, for the reason `config` keeps none: there is one
        copy of a fact, in the log.
        """
        return self._buffer.sort_by()

    # -- compaction ---------------------------------------------------------

    PUBLISHED_THROUGH_KEY = PUBLISHED_THROUGH_KEY

    def published_through(self) -> int:
        """Highest offset the published table is known to hold, 0 if none (§5,
        I4).

        A prefix, always: files cover contiguous non-overlapping ranges (§4)
        and `publish` pushes them in order, so one integer describes it.

        Kept in `meta` rather than read from the published table, so eviction
        asks a keyed read instead of a network round trip to find out what it
        may drop — and keeps working on a machine cut off from object storage.
        """
        recorded = self._buffer.get_meta(self.PUBLISHED_THROUGH_KEY)

        return 0 if recorded is None else int(recorded)

    def published_prefix(
        self, files: Sequence[DataFile], *, include_intents: bool
    ) -> int:
        """The `end` of the longest prefix of `files` the published table holds
        (§4a), or 0 when it holds none of it.

        A file is the published table's business if the published table holds
        THAT FILE'S ROWS. The walk stops at the first file not fully held, so
        the answer stays a prefix — which is what eviction needs, since it
        removes one.

        **Coverage, not equality.** The two tiers cut the same rows into files
        independently, so a staging file is held when its range lies inside
        what the published table covers, whatever the published files' own
        boundaries.

        What the published table covers is `Buffer.published_ranges`: the
        watermark `publish` raises only after a register lands, plus — for
        compaction's question, `include_intents` — the copies a push has
        intended and not yet confirmed.
        """
        ordered = sorted(files, key=lambda f: f.start)
        if not ordered:
            return 0

        covered = self._buffer.published_ranges(
            ordered[0].start, include_intents=include_intents
        )
        reached = 0
        for data_file in ordered:
            if not _covered(covered, data_file.start, data_file.end):
                break

            reached = data_file.end

        return reached

    def compact(self, *, flush: bool = False) -> None:
        """Merge runs of undersized adjacent files (§6). `flush` merges the
        trailing run even while it can still grow (see `is_open`).

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
        # Skipping them is not free: a file with a published copy is NOT
        # necessarily at the target, once the target is RAISED after the copy
        # was made. What it costs is that such a file stays at the size it was
        # published at, as the published copy does. What it buys is that no merge can ever straddle a range a
        # published table holds. `_push` applies the same exclusion, or the two
        # deadlock.
        published = self.published_prefix(local, include_intents=True)
        pending = [f for f in local if f.end > published]

        # One read, so the two limits describe the same policy.
        config = self.config
        budget, rows = config.compact_size, config.target_compact_rows
        for run in runs(pending, budget, rows):
            if flush or not is_open(run, pending, budget, rows):
                self._merge(run)

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

    def _merge(self, run: list[DataFile]) -> None:
        """Compact a run, if there is enough of it to be worth a rewrite."""
        if len(run) >= self.config.compact_min_files:
            self._rewrite_run(self._table, run)

    def _rewrite_run(
        self,
        table: LogTable,
        run: list[DataFile],
    ) -> None:
        """Replace one run of adjacent staging files with a single merged one:
        claimed before the file exists, re-sorted, verified, its sources
        queued before the commit that supersedes them, and their measured
        sizes carried onto the output."""
        start, end = run[0].start, run[-1].end
        # Unique per attempt. See `compaction_path`: a fixed name made a
        # rewrite of a previous compaction write over the file it was reading.
        rel_path = self._layout.compaction_path(start, end, uuid.uuid4().hex[:8])
        target = str(self._layout.absolute(rel_path))
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
        claim = self._buffer.claim(
            "compact", start, end, new_owner(), self._key(target)
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
        # and nothing re-cuts a straddler.
        #
        # The published ranges read DURABLY here, not from this object's
        # memory. A compaction pass holds no pass-level claim — only per-run
        # ones — so what another process recorded between two runs of one pass
        # has to be read, not remembered. A guard answering from pass-start
        # memory can miss coverage recorded since, and a merge that straddles
        # what the published table holds makes every push refuse in
        # `_refuse_straddle`: the watermark never advances again, and eviction
        # pins on it.
        published = self.published_prefix(current, include_intents=True)
        if any(f.start < published for f in run):
            claim.release()

            return

        self._buffer.claim_compaction(start, end, self._key(target))
        try:
            # The claim renews itself while the merge runs. A rewrite over a
            # large run outlasts the TTL, and letting it lapse would invite
            # another owner onto the same range mid-write.
            self._write_merge(table, run, rel_path, target, claim.renew)
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
        renew: Callable[[], bool],
    ) -> None:
        """Write the run's rows to one file, streamed (§6 steps 2-3).

        The inputs are read one at a time, in offset order, through the table
        so each comes out in the current schema. Every `target_row_group_size`
        of them is sorted by `sort_by` and written as one row group — so memory
        is a row group's worth whatever the file's size, and an offset range
        stays inside one or two row groups (see `DEFAULT_ROW_GROUP_SIZE`).
        """
        start, end = run[0].start, run[-1].end
        order = self.sort_by
        config = self.config
        dest = self._layout.absolute(rel_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tally = _Tally()
        # Each input read whole, through the table so it comes out in the
        # current schema, and released before the next is read. Not streamed
        # within an input: pyiceberg's batch reader casts each filtered batch
        # to large types, and pyarrow up to 25.0.1 aborts the process casting a
        # filtered map column (apache/arrow#51029, fixed for 26.0.0). Inputs
        # are seals, or files under the target, so one is small beside a
        # 512 MiB output.
        first = table.scan_range(run[0].start, run[0].end)
        schema = first.schema
        inputs = itertools.chain(
            [first.to_batches()],
            (table.scan_range(f.start, f.end).to_batches() for f in run[1:]),
        )
        del first  # held by `inputs` only until it moves past it
        with stream_parquet(dest, schema, config.compression) as write:
            for group in row_groups(inputs, schema, config.target_row_group_size):
                if order:
                    group = group.sort_by([(c, "ascending") for c in order])

                tally.add(group)
                write(group)
                # Per row group, not per file: a merge of a 512 MB file runs
                # for minutes, far past the claim's TTL.
                checkpoint(renew)

        tally.verify(run, start, end)

        # Checked between writing and committing, because those are the two
        # halves a lapsed lease separates. A run outlasting the TTL lets
        # another owner recover — removing the output this claimed — and the
        # commit would then land anyway, leaving the table pointing at a file
        # that no longer exists while the sources it superseded drain away.
        checkpoint(renew)

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
        # subtraction is `int - None`, a TypeError out of `advance()`. The
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
            # every staging file, and the first `advance()` after a failover
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

    def evict_buffer(
        self, start_offset: int | None = None, end_offset: int | None = None
    ) -> int:
        """Drop buffer rows the next durable copy already holds (#122). Returns
        how many.

        The rule is eviction's, one tier up: never drop data until the next
        durable copy has it.

        - **Without `wal_replication`**, that copy is staging: every row below
          the staging table's committed end is in a file there. The buffer and
          the Parquet share a disk, so holding them longer buys nothing.
        - **With it**, the buffer is the off-box copy until the published table
          has the range (§3a), so the copy that counts is the published one —
          read from the log's own record of what landed there
          (`published_prefix`, without intents), the same authority staging
          eviction acts on for I4. Local, so this never needs the network.

        Rows below the staging table's first file were evicted from it, which
        I4 allowed only once the published table held them, so the boundary is
        the end of the longest prefix of staging files the copy holds. Run
        before `evict("staging")` — as `advance` does — so the files that prove
        it are still there.

        `[start_offset, end_offset)` narrows what is eligible; it never widens
        it. Unbounded, this is one `DELETE` under SQLite's write lock and
        stalls appends for its duration — the bounds are how a caller chunks it.
        """
        self._table.reload()
        files = self._table.data_files()
        if self.config.wal_replication and self._published.remote():
            boundary = self.published_prefix(files, include_intents=False)
        else:
            boundary = max((f.end for f in files), default=0)

        if end_offset is not None:
            boundary = min(boundary, end_offset)

        start = 0 if start_offset is None else start_offset
        if boundary <= start:
            return 0

        return self._buffer.evict_rows(start, boundary)

    def evict(self, *, everything: bool = False, end_offset: int | None = None) -> None:
        """Drop files older than `staging_retention` from the staging table (§8).

        `everything` drops every file the published table holds, whatever the
        policy — what `retire()` ends with. I4 still clamps it: a file the
        published table has not registered stays. `end_offset` caps it too, so
        a caller can evict in bounded steps; eviction always removes a prefix.

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
        # Only ever lowers it, and the claimed boundary below starts from here.
        if end_offset is not None:
            boundary = min(boundary, end_offset)

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
        # and eviction acts on the same facts: what the published table holds,
        # and the retention policy, both of which another process can change
        # between a read here and the claim. Deciding from the earlier read
        # would delete on facts the claim no longer backs, and a file that has
        # left the staging table can never be pushed afterwards.
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
            self.published_prefix(files, include_intents=False),
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
            # Re-dated to the commit, like every other supersession: the paths
            # were queued before it, and the grace is about readers holding
            # them, which starts when the commit lands.
            self._buffer.restamp_deletions(
                (self._key(p) for p in dropped), int(datetime.now(UTC).timestamp())
            )
        finally:
            removal.release()

    def _written(self, data_file: DataFile) -> datetime:
        """When a file was written, for `staging_retention` to measure against.

        The log's own record first. Iceberg's is the fallback and cannot be the
        primary: it dates a file by the snapshot that added it, and `expire`
        deletes that snapshot, after which the file has no age there at all.

        A file neither knows is treated as newly written, so it is never
        evicted on an age nobody recorded. That is the safe direction — the
        cost is disk, and the alternative is deleting data because its age was
        unknown. It applies to files this database never saw: files from a
        version that did not keep these records.
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

    def expire_published(self) -> None:
        """Expire the published table's snapshots past
        `published_snapshot_retention`, then delete what has come due (§6, #113).

        The published half of `expire`, and a routine of its own rather than a
        step inside `publish`, so an orchestrator can run it on its own
        schedule or in its own process (#118). `advance` runs it after
        `publish`.

        **No claim for the expiry.** It is a metadata commit the catalog's
        compare-and-swap orders, like staging's. What used to need one was
        opening the published table with `repair`, which can drop a catalog
        entry and create a table in its place; this opens without it, so a
        table not created yet — or not where the log now points — is skipped,
        and `publish` is what creates and repairs it. The drain that follows
        takes no claim, as `drain` takes none.

        Every publish leaves a snapshot, a manifest list and a manifest behind,
        so a published table that is never expired keeps all of them. A pass
        with nothing old enough commits nothing.
        """
        published = self._published.table()
        if published is None:
            return

        cutoff = datetime.now(UTC) - self.config.published_snapshot_retention
        retiring = list(
            published.expiring_paths(published.snapshots_older_than(cutoff))
        )
        self._enqueue(retiring)
        published.expire_snapshots_older_than(cutoff)
        self._buffer.restamp_deletions(retiring, int(datetime.now(UTC).timestamp()))
        self.drain_published()

    def drain_published(self, grace: timedelta | None = None) -> None:
        """Delete the published table's queued objects whose grace has passed.

        No claim, for the reason `drain` gives. A re-point mid-drain cannot
        widen what this deletes: the table is the one opened here, every
        object is checked against ITS references, and one outside the
        log's current prefix is left queued (`_drain_published`).
        """
        published = self._published.table()
        if published is None:
            return

        self._drain_published(published, None, grace=grace)

    def sweep_staging(self) -> None:
        """One pass of the staging table's stranded-metadata sweep (`_sweep`).

        Called with no claim held, by design: it needs none, and a backlog
        pass on object storage is tens of seconds of deletes. Holding a lease
        through it would refuse every other process's maintenance meanwhile.
        """
        self._sweep("staging", self._table, self.config.staging_snapshot_retention)

    def sweep_published(self) -> None:
        """One pass of the published table's sweep (`sweep_staging` says why
        it holds nothing). Opens without `repair`, which is `publish`'s."""
        published = self._published.table()
        if published is not None:
            self._sweep(
                "published", published, self.config.published_snapshot_retention
            )

    def drain(self, grace: timedelta | None = None) -> None:
        """Delete staging files whose grace period has passed — the table's
        snapshot retention, or `grace` when given (`retire` passes zero).

        A keyed read of `pending_delete`, not a directory walk. Every file this
        library creates has its path written to SQLite before it is written to
        disk — seals through `sealing`, compactions through `compacting` — so
        there is no category of file that could only be found by looking.

        Staging entries only. The published table's are `drain_published`'s,
        against its own retention.
        """
        # Read against the CURRENT retention, so lowering it takes effect on
        # files already queued.
        wait = self.config.staging_snapshot_retention if grace is None else grace
        cutoff = datetime.now(UTC) - wait
        due = [
            p
            for p in self._buffer.due_deletions(int(cutoff.timestamp()))
            if not is_remote(p)
        ]
        if not due:
            return

        # No claim. What a claim would guard is a queued name becoming
        # referenced again between the veto below and the unlink, and nothing
        # can do that. Every path that adds a file to a table adds one with a
        # fresh per-attempt token — a seal, a compaction, an ingest
        # (`Layout.*_path`). Seal recovery commits the
        # claimed name only if it never landed, and otherwise queues it and
        # writes a fresh one; compaction recovery registers nothing. `publish`
        # registers copies of staging files above the published span, and
        # `register` declines a range already covered, so a range a rewrite
        # superseded is never pushed again.
        #
        # So an entry that is due and unreferenced stays unreferenced. Two
        # drains overlapping only unlink the same file twice, which
        # `missing_ok` makes a no-op, and forget the same row twice.
        #
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
                # Still in a live snapshot: the grace period has passed but
                # something references it — a compaction's recovery queues
                # outputs whose commit may in fact have landed. Deleting a
                # referenced file is unrecoverable; it stays queued.
                continue

            # Unlink first, forget second. A crash between them leaves a row
            # whose unlink is already a no-op; the reverse leaks the file with
            # nothing left pointing at it.
            path.unlink(missing_ok=True)
            self._buffer.forget_deletion(rel_path)

    def _drain_published(
        self,
        published: LogTable,
        renew: Callable[[], bool] | None,
        *,
        grace: timedelta | None = None,
    ) -> None:
        """Delete the published table's queued objects whose grace has passed —
        `published_snapshot_retention`, or `grace` when given.

        `renew` is the caller's claim, checked before each delete, when there
        is one; `drain_published` holds none (see `drain`).

        **With no claim to renew, the deletes run in parallel**, as the sweep's
        do: one object-store delete is a round trip, and `retire` drains every
        file its expiry just queued — thousands, on a table that carried a
        snapshot per publish (#153).
        """
        wait = self.config.published_snapshot_retention if grace is None else grace
        cutoff = datetime.now(UTC) - wait
        due = [
            p
            for p in self._buffer.due_deletions(int(cutoff.timestamp()))
            if is_remote(p)
        ]
        if not due:
            return

        # Normalised, because the configured URI may carry a trailing slash
        # while every queued path is built from it stripped. The mismatch would
        # classify this log's OWN objects as another published table's and
        # wedge the queue permanently.
        ours = f"{(self._published.uri or '').rstrip('/')}/"

        # Reloaded first, for the reason `drain` gives — and then refused
        # unless what it reloaded is the table under `ours`. The veto below
        # asks THIS handle what is referenced, and the prefix guard asks `meta`
        # which table is the log's; the two must be the same table, or every
        # object of one looks unreferenced by the other — and live objects
        # get deleted. A reload follows the catalog row, which a re-point by an
        # earlier version, crashed half done, leaves naming another prefix than
        # `meta` does. The queue is left as it is until a publish's repairing
        # open puts the two back in agreement.
        published.reload()
        if not str(published.metadata_location).startswith(ours):
            _log.warning(
                "litelink: not draining the published table: its catalog names "
                "%s, which is not under %s; a publish repairs this",
                published.metadata_location,
                ours,
            )
            return

        referenced = published.referenced_paths()

        deletable: list[str] = []
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

            deletable.append(uri)

        def remove(uri: str) -> None:
            with contextlib.suppress(FileNotFoundError):
                published.remove(uri)

        if renew is None:
            with ThreadPoolExecutor(max_workers=SWEEP_THREADS) as pool:
                # `list` so the first failure raises here, not silently.
                list(pool.map(remove, deletable))

            for uri in deletable:
                self._buffer.forget_deletion(uri)

            return

        for uri in deletable:
            # Before every deletion, for the reason `drain` gives: one remote
            # round trip each, measured at ~650 ms, adds up past a TTL.
            checkpoint(renew)
            remove(uri)
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
        # Per table, not per role: a backlog listed on one table must never be
        # worked through another's handle.
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
        live = table.referenced_metadata()

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


@dataclass
class _Tally:
    """§6 step 3, as far as it can be taken, kept as the rows stream past.

    Row count and the offset range are checked exactly; both are what the
    overwrite's safety argument rests on. Per-column min/max is NOT checked and
    cannot be by equality: Iceberg truncates string and binary bounds, so a
    source bound is a prefix rather than a value and would compare unequal to a
    correct merge.
    """

    rows: int = 0
    low: int | None = None
    high: int | None = None

    def add(self, group: pa.Table) -> None:
        # Python's min/max over the materialised column, not pyarrow.compute:
        # pc's kernels are generated from a runtime registry, so no static
        # checker can see them.
        offsets = group["litelink_offset"].to_pylist()
        if not offsets:
            return

        self.rows += len(offsets)
        low, high = min(offsets), max(offsets)
        self.low = low if self.low is None else min(self.low, low)
        self.high = high if self.high is None else max(self.high, high)

    def verify(self, run: list[DataFile], start: int, end: int) -> None:
        expected = sum(f.rows for f in run)
        if self.rows != expected:
            msg = f"compaction would lose rows: {self.rows} != {expected}"
            raise RuntimeError(msg)

        if self.low != start or self.high != end - 1:
            msg = "compaction changed the offset extent"
            raise RuntimeError(msg)
