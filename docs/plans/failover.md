# Failover: resuming a log on a second box

Spin litelink up on a new machine, point it at the archive, resume writing.

## What broke, and how it was found

`Log.open` on a WAL restore **fails outright**. Measured: create a log, seal
eight files, restore only the SQLite databases litestream ships, remove the
original root, open.

```
FAILED: FileNotFoundError
  .../waltest/orig/litelink/s/metadata/00009-....metadata.json
```

`catalog.db` stores **absolute** paths to the local Iceberg metadata, and
litestream replicates the three `.db` files and nothing else — not that
metadata, not the Parquet. So a restored catalog points into the dead box's
filesystem.

An earlier draft blamed this on RUNTIME's restore being measured where "none of
them ever sealed" (RUNTIME:625). **That explanation was wrong.** `Log.new`
calls `LogTable.create` unconditionally (`log.py:558`), which writes
`00000-<uuid>.metadata.json` before a single row is appended — so there is no
such thing as a litelink log with no local Iceberg metadata, and a never-sealed
log fails identically. Measured, both:

```
never sealed: FileNotFoundError .../litelink/a/metadata/00000-....metadata.json
sealed:       FileNotFoundError .../litelink/b/metadata/00004-....metadata.json
```

Whatever RUNTIME:624-626 measured, it was not `Log.open` on a WAL restore into
a fresh root. The correction belongs in RUNTIME, and `examples/adsb/replicate.py`
restates the same claim.

## What litestream already covers

More than an earlier draft assumed, which is why the Parquet-metadata-snapshot
proposal is gone. litestream replicates whole database **files**, snapshotting
them periodically and shipping incremental segments on top — so there is no
replay from the beginning, and rows written at `Log.new` survive any retention
window. This is why `wal_retention` derives `interval` as half of `retention`:
so the window can never contain zero snapshots.

`buffer.db` therefore carries almost everything: `meta` (schema, config,
archive URI), the offset sequence, every `extent` row, `extent_intent`,
`pending_delete` including its remote entries, and the unsealed rows.

## What it does not cover

**1. `sort_by`.** The one durable fact living only in `catalog.db`.
`LogTable.create` declares it (`_table.py:301`), `Log.open` recovers it as
`table.sort_by()` (`log.py:654`), and it is in no `meta` row. Nor is it on the
archive: `open_archive` never declares a sort order on either branch
(`_table.py:454-514`). A restored log silently stops clustering.

**2. The local tier.** Parquet and its Iceberg metadata are not SQLite, so
litestream never sees them. Inherent, not a gap to close.

**3. Two holes in the offset space**, which are different problems with
different fixes:

```
  [0 .......... archived] safe, in the archive
                [archived ...... sealed] hole A — local Parquet only
                                  [sealed ... replicated] safe, in buffer.db
                                            [replicated ... assigned] hole B
```

- **Hole A** — a seal DELETES the rows it wrote from `buffer.db`
  (`_buffer.py:1002`), so offsets between the archive frontier and the seal
  frontier exist only in unreplicated Parquet. **Closed by change 1 below.**
- **Hole B** — rows appended inside the replication lag were served to callers
  but never shipped. Inherently lost. **Their offsets must not be reissued** —
  change 3.

SPEC states `RPO = max(WAL replication lag, Parquet upload lag)`. Hole A is the
second term and is closeable; hole B is the first term and is not.

## The changes

### 1. Delete buffer rows when they are safe off-box, not at seal

The rule is I4 one tier up: **do not delete the only off-box copy.** What
counts as another copy depends on configuration, so the gate is one condition.

| config | delete on |
|---|---|
| local-only | **seal** — nothing is off-box either way |
| archive, no `wal_replication` | **seal** — buffer and Parquet share a disk and die together, so holding buys nothing |
| archive + `wal_replication` | **sync** — the buffer IS the off-box copy until the archive has the range |

Since `wal_replication` already requires an archive (`log.py:2375`), this reads
as: *delete on seal, unless `wal_replication` is on, in which case delete on
sync.* Local-only keeps today's behaviour exactly — and must, or nothing would
ever release the rows.

**Reads are unaffected.** `Buffer.rows_above(boundary)` bounds the buffer leg by
the local table's committed extent, so held rows never reach DuckDB. §7's
"buffer size is read latency" is about rows ABOVE the boundary. WAL volume drops
slightly: the delete is deferred, not duplicated.

**The seal's INPUT is affected, and this is the load-bearing part.**
`Buffer.rows_below(end)` has no floor — `self._rows("< ?", (end,))`
(`_buffer.py:723`). It is correct today only because the delete bounds it: after
`finish_seal` removes everything below the cut, the buffer's minimum already
equals the next group's start. Defer the delete and seal #2 writes every row
from the archive frontier to `end`, overlapping file #1.

That breaks four things at once, none of them at seal time: manifest ranges stop
being non-overlapping (§4/§6), the local leg is an unfiltered `iceberg_scan` so
reads return the overlap twice, `_refuse_straddle` stalls the next push for ever,
and compaction's `_verify` row-count check fails. `_write_and_commit`'s local
`register` passes no `lo`, so `_refuse_straddle` returns early and nothing
catches it until the next sync.

**So this change requires `Buffer.rows_between(start, end)`**, with `start` from
`pending_group()` — which already carries it, unused, at both call sites.

**The seal TRIGGER is unaffected.** The cut is made by the appender in the
transaction that crosses `target_seal_size` and recorded in the `extent` table,
so what is still sitting in `buffer` does not enter the decision. Verified:
`_read_group` reads the open `extent` row only, and `close_open_group` is
guarded by `start_offset IS NOT NULL`.

**The delete is driven from `archived_through()`, not from the tail of
`_push`.** `_push` has three early returns before the watermark
(`if not uploaded`, a declined `register`, and `_repointed_mid_push`), so a
delete at its tail is skipped by a crash between the register and the delete —
and the next pass finds `pending` empty, returns early, and never reaches it.
On a log that has gone quiet the rows are then held indefinitely. Driving it
from the archive's own frontier at the START of the pass makes it idempotent,
which is what makes the crash window harmless.

**Costs, both real.** `buffer.db` grows with sync lag, and the unsynced band is
stored twice locally. A stalled sync grows it without bound — the same shape as
§11's "local eviction stalls", and it should be reported the same way.

**To verify at build time:** `rows_above`'s incremental Arrow cache assumes rows
"arrive only above the last one, and leave" from the bottom at seal. Deferring
the delete means they leave later and in bigger chunks.

### 2. `sort_by` into `meta`

Written at `Log.new` beside `arrow_schema` and `config`, updated by
`set_sort_by`, read from `meta` by `Log.open`.

**Not archive table properties**, which an earlier draft proposed for the whole
metadata set. Properties would be a second durable home for a fact `buffer.db`
already carries — §4a's "one copy of a fact", which nine review rounds were
spent enforcing.

**No migration.** Pre-release: there are no existing logs, so `meta` carries
`sort_by` from `Log.new` onward and `Log.open` reads it with no fallback. A
missing row is corruption, handled the way a missing `config` row already is.

**Unset must be expressible.** `set_sort_order` early-returns on an empty order
(`_table.py:539`), so `set_sort_by((), rewrite=True)` re-clusters the data and
never clears the declaration. Today `Log.open` reads the declaration and
silently reverts; after this move the two records disagree for ever. Fix
`set_sort_order` to clear the order on empty.

**Also declare the sort order on the archive table.** `open_archive` should call
`set_sort_order` on its create path: the archive is the same table's data later,
and an Iceberg table that does not declare its clustering is lying about itself.
A correctness fix in its own right, and pre-release there are no archives it
arrives too late for.

### 3. `Log.restore(root, name, *, archive, s3=None)`

1. **Refuse an existing log at `root/name`** — matching `Log.new`'s own probe
   (`log.py:553`) — and **refuse when `root/litestream.yml` already names a
   different buffer.** There is one config per root (`log.py:808`) and
   `litestream_config` describes the log it was asked about, so restoring a
   second log into a live root would overwrite that file with a config naming
   only the new buffer, and the first log would stop being replicated at the
   sidecar's next restart — silently. `_replication.py:22` already says "put
   each log in its own root" until a per-root generator exists; this refusal is
   that advice enforced rather than restated.
2. Write `litestream.yml` from `Layout(root, name)` + `archive` — pure path
   arithmetic, no log needed, which is the chicken-and-egg that makes this hard
   today. `replication_config_for(root, name, archive, ...)` is the classmethod;
   `replication_config()` delegates to it.
3. **Restore `buffer.db` only.** No `-if-replica-exists` on it — that flag exits
   0 when no backup is found (verified against the pinned binary), so it would
   make step 4's refusal undetectable.
4. **Refuse if there was no replica.** A partial recovery that looks successful
   is the worse failure.
5. **Do NOT restore `archive.db`**, and delete any row for this table id if one
   is present. Measured: a stale replica reports 1 archive file where the bucket
   holds 5, and the union reads 261 rows instead of 1061 — 800 archived rows
   silently unreadable. `open_archive` consults `version-hint.text` only when
   the catalog has NO row (`_table.py:454`); with a row present it calls
   `load_table` on whatever the stale row names, and old metadata JSONs survive
   in the bucket until expiry, so it succeeds. The next sync then commits onto
   the stale lineage and `publish_pointer` overwrites the hint with the fork,
   **destroying the recovery pointer.** Stale is strictly worse than absent:
   absent is already handled (`_archive.py:144`).
6. **Do NOT restore `catalog.db`** — see the failure at the top. It stays in
   `Layout.databases` and keeps being replicated, because same-box recovery with
   the data directory intact is exactly where its absolute paths are still
   valid, and it is the only record of local Parquet in a design that refuses
   directory listing. Replication set and restore procedure are different
   questions; an earlier draft conflated them.
7. **Rebuild the local Iceberg table, empty**, from `meta`'s schema and
   `sort_by`.
8. **Reconcile the restored buffer.** Drop `extent` rows naming local files —
   they name Parquet that does not exist here, and applied, compaction would
   merge them and the read path would scan them. Archived rows (`is_remote`)
   stay: that is the coverage I4 acts on. `pending_delete` splits the same way,
   and the remote half is **required**: `rewrite_archive` is "the only thing
   that puts a remote entry in the deletion queue" (`_maintenance.py:1156`) and
   the design refuses LIST, so dropping them leaks archive objects nothing can
   ever find again. Drop `claim` rows — a dead box's claims carry future expiry
   and would make the new box wait out a TTL for owners that do not exist.
   Leave `sealing` and `compacting`: `_recover_seal` finds the rebuilt table
   empty and re-writes the interrupted file from surviving buffer rows, which
   RECOVERS data.

   **Also drop the OPEN `extent` row** (`end_offset IS NULL AND rel_path IS
   NULL`) and re-run `_seed_group`. Without this the recovered band is orphaned:
   its `extent` rows were just dropped as local, and `_seed_group` early-returns
   whenever an open group exists (`_buffer.py:410`) — which a restored buffer
   always has, because `_cut` inserts one after every cut. The surviving open
   row starts at the primary's UNSEALED floor, above the band, so the band ends
   up in no leg of the read and is lost at the first seal after recovery. This
   is the case `_seed_group` documents itself for.
9. **Adopt the archive** via `version-hint.text`.
10. **Seal the restored buffer empty, THEN reserve the offset gap**, then
    report. The order is required, not tidy — see below.

### The offset gap

`sqlite_sequence` resumes above what the **replica received**, not what was
**assigned**. Measured: the primary served `[11..15]` after the last shipped
frame; a restored box handed those same five offsets to different rows.

So requiring the WAL shrinks hole B to the replication lag — it does not remove
reuse, and an earlier draft claiming "offsets are exact, nothing is reused" was
wrong.

**Restore therefore skips `RESTORE_RESERVE = 1 << 20` offsets** via
`Buffer.seed_offsets`, and reports the skipped range.

Reserved rather than reused, for three reasons:

1. **`append()` returns the offset to the caller.** Those integers were handed
   out, not merely readable — a writer may have recorded "offset 12" in another
   system. That is a synchronous, certain harm path.
2. **The design already blesses gaps and forbids reuse.** §6 needs files
   "non-overlapping and adjacent in offset order, not free of integer gaps",
   §13.4 blesses reservation explicitly, and I9 says offsets are "never reused"
   (SPEC:1285). Reuse trades a stated invariant for a display convenience.
3. **A gap fails visibly, reuse fails silently.** A consumer at offset 15 seeing
   the next row at 1048591 can detect that; a rewind looks like normal
   operation.

2²⁰ is generous against any plausible second of appends and free against int64.

**§13.4's precondition comes with it, and an earlier draft dropped it.** SPEC
is explicit: "A reservation is a hole in the offset space. A seal spanning it
writes a file whose `litelink_offset` statistics cover `[lo, hi]` while
containing none of it… **Sealing the buffer empty before reserving closes it**"
(SPEC:1484). Under change 1 the restored buffer is guaranteed non-empty — it
holds the recovered band — so the next seal would span the hole. Sealing it
empty first writes the band into a real file, gives it a real `extent` row, and
leaves every later group dense.

**`local_rows` needs fixing regardless of ordering.** It computes its floor as
`next_offset() - 1 - config.local_rows` (`_maintenance.py:665`) — differencing
offsets, which assumes density. After a 2²⁰ reservation the boundary jumps by
2²⁰ and every local file satisfies `f.hi <= boundary`, so the first `maintain()`
after a restore evicts the ENTIRE local window, clamped only by I4. The comment
there reasons about small rollback gaps and says this errs toward "retaining
more, the safe direction for a floor" — with a large hole it errs the other way.
It has to count rows from manifest statistics instead, which is also what the
setting says it does.

`seed_offsets` currently refuses a buffer holding ANY row (`_buffer.py:691`),
guarding a hazard that is real only downward — SQLite assigns
`max(max(rowid), seq) + 1`, so a raise is safe. Narrow the guard to `first - 1 <
max(rowid)` rather than adding a second sequence writer that bypasses it.

The report names two ranges: the rows **lost** and the offsets **skipped**.

Lost is `[archived_through() + 1, extent()[0])`, falling back to `next_offset()`
only when `extent()` is None. An earlier draft used `next_offset()` outright —
which is the CEILING, not a floor, so it reported the whole sealed-but-unsynced
band as lost. Under change 1 that band is recovered, so on a replicated log the
lost range is usually empty, and reporting a sync-lag window of phantom losses
would be the wrong number to pin a test on.

### 4. `set_archive` refuses an archive that is ahead of this log

Found while tracing failover, reachable today, and silent. `Log.new` on a second
box followed by `set_archive(existing_prefix)` adopts an archive whose extent is
far above the local log's offsets. Traced: `floor` exceeds every local `f.hi`,
so `pending` at `log.py:1789` is empty; `confirmed` still writes the archive's
frontier into `meta` (`log.py:1706`); `archived_prefix` finds no `extent` rows
and returns 0, so eviction is pinned. Disk grows without bound and `sync()`
returns success having uploaded nothing.

The guard is **`archive.extent()[1] >= next_offset`**, spelled exactly that way.
"Overlaps the local log" is the wrong wording and would refuse
`test_pointing_back_at_an_archive_restores_everything_it_held` — re-attaching to
an archive holding offsets 1–1600 on a log whose next offset is far above is a
supported operation this must not break.

`ArchiveAbsent` and an unreachable archive **pass**: `_repoint` deliberately
tolerates an archive that does not exist yet (`log.py:928`, "configuring one is
a statement of intent"), and `set_archive` is called on every writer restart, so
this must not add a mandatory round trip that fails closed.

**Read the new prefix's extent through `_published_location`, not
`open_archive`.** At guard time `meta` still names the OLD archive, and `Archive`
holds no copy of the location — so `table()` opens the old one. Going to
`open_archive` directly for the new prefix breaks both ways: with `repair=False`
the catalog row for this table id names the old archive, so the boundary check
raises `ValueError` on every ordinary re-point; with `repair=True` it
`drop_table`s the old entry as a side effect of a read-only check.
`_published_location` is a single GET of `version-hint.text` and touches
`archive.db` not at all. No hint, or any I/O failure, is a pass.

## Tests

Each falsified — break the code, prove the test bites.

1. The opening failure, pinned: seal, restore, remove the original root, open.
2. With `wal_replication`, buffer rows survive a seal and go at sync. Without
   it, they go at seal.
3. A restore recovers the sealed-but-unsynced band (hole A closed).
4. `sort_by` survives a restore, and `set_sort_by(())` clears both records —
   `meta` and the table's declaration — rather than leaving them disagreeing.
5. Restoring with a stale `archive.db` present does not truncate the archive —
   the measured 261-vs-1061 case.
6. Archived `extent` rows and remote `pending_delete` rows survive; local ones
   and `claim` rows do not.
7. Offsets after a restore are strictly above every offset the primary served,
   including ones it served after the last replicated frame.
8. `restore` refuses an existing log, refuses when there was no replica, and
   refuses a root whose `litestream.yml` names another buffer.
9. The reported lost and skipped ranges match.
10. `set_archive` refuses an archive ahead of the log, still permits re-attach,
    and still permits a prefix that does not exist yet.
11. A seal after a deferred delete writes only its own group's rows. Without
    this, test 3 passes for the WRONG reason — the unbounded `rows_below` sweeps
    the orphaned band into the file by accident.
12. `local_rows` retains what it says after a restore, rather than evicting the
    whole local window across the reserved hole.

## Open

**Split-brain is not addressed.** Nothing detects a primary that is not actually
dead; two boxes resuming one archive means two writers, and if both replicate,
two litestream instances on one replica path. `Log.restore` warns; it cannot
check. The fix is §13's archive-identity token — a value in the archive's table
properties, where §13 already puts it — which closes this and change 4 by the
same mechanism. Its own ticket.

## Not in this PR

- The archive identity token (§13), and split-brain detection.
- Local-only WAL backup. `wal_replication` requires an archive because segments
  ship to `{archive}/_wal`, so a local-only log has no off-box story at all. A
  WAL destination independent of the archive prefix is a real product shape and
  a separate feature.
- Schema evolution (§9).
