# Plan: restore two polarities to the archive extent record

**Status: reviewed, revised, ready to build.** The first draft was attacked before any code
was written; the review found one self-answer wrong (Q3) and one detail that would have made
a race *worse than today* (Q5). Both are corrected below, and the corrections are the reason
this document exists.

## The window this closes

`sync` registers a batch into the archive, then writes one `extent` row per pushed
file recording where its copy went (`log.py:1766`). A crash between the register and
those rows leaves the archive holding a range nothing local records.

Compaction decides what it may merge from those rows. So if a compaction-target
change lands before the next `sync` backfills them from the archive's manifest,
compaction regroups the pushed-but-unrecorded files with unpushed neighbours and
commits a **local file straddling the archive's extent**. Every later `sync` is then
refused by `_refuse_straddle`, permanently: watermark frozen, eviction pinned below
the straddler, and the shipped sync role dies because it catches only `RuntimeError`
and `CommitFailedException`. Nothing re-cuts a local straddler.

Verified by execution in review round 13 (scenario S6), with a control (S7) showing
the config change is the necessary ingredient.

## Why the obvious fix is wrong

"Write the rows before the register" alone is not safe, because the rows have two
readers whose safe directions are **opposite**:

| reader | asks | safe when coverage is |
|---|---|---|
| compaction | may I merge across this range? | **overstated** — refusing to merge costs nothing |
| eviction (I4) | may I delete the only local copy? | **understated** — deleting early loses data |

One record cannot be both. The pre-segment design knew this: it kept `archive_pending`
(written before the register, read only by compaction) beside `archive_through`
(written after, read by eviction). The per-segment refactor collapsed them into one
row and lost the distinction — which is exactly this window.

## The change

### 1. Schema

`extent` gains one column:

```sql
ALTER TABLE extent ADD COLUMN confirmed INTEGER NOT NULL DEFAULT 1
```

`DEFAULT 1` is the migration: every existing row was written *after* its register (or is a
local file), so every existing row is confirmed. No backfill pass needed.

There is no migration precedent in this buffer, so the `ALTER` goes in setup behind an
idempotent `pragma table_info` check. A read-only open cannot `ALTER`, and must not need to:
no read-only path names the new column. A log an older build crashed mid-push has no rows at
all, so the migration marks nothing and the next backfill heals it — no worse than today, and
the one case only a local re-cut tool could do better on.

`confirmed = 0` means "a copy was *intended* here"; `1` means "a copy *is* here".

### 2. Writes

| site | today | proposed |
|---|---|---|
| `log.py:1766` sync, after register | `record_file(...)` | **moves before the register** as `confirmed=0`, for EVERY uploaded file; the existing post-register call stays exactly where it is and becomes the confirm |
| `log.py:1687` sync backfill from manifest | `record_file(...)` | `confirmed=1` — the manifest is proof |
| `log.py:2030` hydrate | `record_file(...)` | `confirmed=1` — a local path, not an archive claim |
| `_maintenance.py:1002` rewrite scratch | `record_file(...)` before `replace_range` | `confirmed=0`, confirmed after `replace_range` commits |

`record_file` gains `confirmed: bool = True`, so only the two intent sites change.

Two things about the confirm that the first draft got wrong:

- **It is the existing upsert, not a bare `UPDATE`.** A takeover can delete this push's
  intents while its register is in flight (rule 3), and an `UPDATE` would then match nothing
  — register landed, no rows, which is precisely the window being closed. The upsert
  recreates them, which is what today's code already does and must keep doing.
- **It runs on register success regardless of the identity fence.** If the log was
  re-pointed mid-push the register still landed in the pinned archive, so the copy is real
  and recording it is what makes a later re-point *back* coherent. The fence gates the
  watermark, as it does today, and eviction ignores foreign rows anyway.

The intent is written for **every** uploaded file. The post-register loop skips files with no
measured size (`held is not None`); an intent must not, or the window stays open for exactly
those files. Unknown sizes take `compact_size`, as the backfill already does.

### 3. Reads

- `Buffer.archived_ranges(prefix, floor, *, confirmed_only: bool)`.
- **compaction** → `confirmed_only=False` (sees intents; overstates; safe).
- **eviction** → `confirmed_only=True` (sees only landed copies; understates; safe).
- `archived_through()` → **no change.** It reads `meta`, not `extent`, and is written after
  the register and reconciled from the manifest, so it is already confirmed-polarity by
  construction. The first draft said "confirmed only", which is either a no-op or an
  unplanned change to the seal-replay guard.
- `memory()` / `file_bytes()` → **no change.** A size for a file that may not exist is
  harmless; sizes are policy inputs, not invariants.
- `file_ages()` → **no change**, and it belongs in this list: the first draft claimed a
  complete reader inventory without having one. Eviction dates files by local root-relative
  key, so URI-keyed rows are never looked up.
- The append path's `max(end_offset)` and `last_queued_end` → **no change**; both filter to
  `rel_path IS NULL`, which no archive row satisfies.

**Ordering requirement.** Reconciliation runs under the push claim *before* `frozen` and
`settled` are computed, so no unconfirmed row under the pinned archive survives into the
decision. The three sites that must pass `confirmed_only=False` are `_maintenance.py` compact
(pass level), `_maintenance.py` the per-run recheck, and `log.py` `_push`'s `frozen` — the
same rule in three places, which is why they take the same call.

### 4. Reconciliation, in `_push` under the claim

The existing backfill generalises to three rules, applied only to rows whose URI is
under the **pinned** archive:

Matching is by **path membership in the manifest**, not by range coverage. That is not a
detail: the first draft matched by range and was wrong, because a rewrite's intents name new
objects over a range the *stale files being replaced* still cover. Range matching would have
confirmed dead intents naming objects `drain` was about to delete. Path matching gets it
right in both directions — a crash after a register leaves paths present, a crash before
`replace_range` leaves them absent.

1. path in the manifest, row unconfirmed → **confirm it** (the upsert of rule 2).
2. path in the manifest, no row → **insert confirmed** (today's backfill).
3. path absent, row unconfirmed **and older than the claim TTL** → **delete it**. The
   register never landed. The TTL clause matters: without it, a sync that took over a lapsed
   claim would delete the intents of a register still in flight.

Rows under a **different** archive's URI are left alone, confirmed or not. We cannot check
them without opening that archive.

The cost is not zero, and the first draft said it was. Compaction over-blocks those ranges
for ever, occasionally for a copy that does not exist. What makes it acceptable is that
eviction does not stall behind them: `_push` counts foreign coverage in `frozen`, so those
files are settled by definition and pushed again to the current archive, after which
confirmed rows under the new URI unpin eviction. Re-pointing back resolves them properly.

## What I want attacked

1. **Is `confirmed=0` + delete-on-absence actually safe against a re-point mid-push?**
   Rows are written for the pinned archive; the fence already aborts the push if the
   log is re-pointed. But the rows are already durable by then. They would be
   unconfirmed and belong to an archive we may have left — permanently unreconcilable.
   Cost is over-blocking compaction on those ranges. Acceptable, or a leak?
2. **Does moving the write before the register change `_refuse_straddle`'s inputs?**
   `lo=uploaded[0][0].lo` is computed from `uploaded`, not from rows. I believe not.
3. **`rewrite_archive`'s scratch rows.** They are written per sealed output before one
   `replace_range` commits them all. If that commit fails, all of them are dead intents
   under the *current* archive, so rule 3 deletes them at the next sync. But `_recut`
   also enqueues the superseded objects — does an unconfirmed row interact with
   `drain`'s veto or `_expire_archive`?
4. **Does eviction reading confirmed-only reintroduce the round-12 stall?** `_push`
   settles what compaction cannot merge, computed from `archived_prefix(pending, None)`
   — which under this change sees intents too. Is `sync`'s exclusion still exactly
   compaction's? They must stay the same rule.
5. **Two syncs racing.** They serialise on the whole-log claim, so I believe intents
   cannot interleave — but a lapsed claim plus a takeover?
6. **Is `DEFAULT 1` right for a log written by an older build mid-crash?** Such a log
   may have exactly the unrecorded range this closes. Migration marks nothing, which
   is today's behaviour — no worse, but worth stating.

## Test plan

- The takeover race: a lapsed claim, a rival reconcile deleting live intents, the register
  landing anyway — the confirm must recreate the rows.
- A recut crash before `replace_range`: the dead intents must be deleted, not confirmed.
- A declined register (`_covers`): its intents must not survive as confirmed rows.
- A fence failure mid-push: the register landed, so the rows must still be confirmed.
- The migration default on a log written by an older build.
- Reproduce round 13's S6 end to end (crash between register and rows, raise the
  target, maintain, sync) and assert no straddler and no stall. Must fail without
  the change.
- S7 control: same crash, no config change — still heals.
- Eviction must not act on an unconfirmed row: record an intent, assert the boundary
  does not move.
- Compaction must respect an unconfirmed row: record an intent, assert no merge
  crosses it.
- Reconciliation: all three rules, plus rows under a foreign URI left alone.
- The common path is unchanged: same pushed sets and watermarks as before.

## Risks

- Touches the seam that just converged after three rounds of trading one failure for its
  mirror. The polarities are *added*, not swapped: every existing read keeps its current
  answer unless an intent row exists.
- **Intents are not rare.** They exist for the duration of every register (measured at 4.1 s)
  and for the whole of every `rewrite_archive` (minutes). The first draft said "only inside
  the crash window", which is wrong. Both are benign — compaction skipping in-flight files is
  desirable, and during a recut the stale files' rows keep eviction's answer unchanged — but
  the exposure is continuous, not exceptional.
- **Rule 3's DELETE is the one genuinely new subtraction.** Deleting a real row understates
  coverage, which is compaction's unsafe direction. Every available conservatism is stacked
  on it: pinned prefix only, unconfirmed only, path-absent, older than the TTL, and ordered
  under the claim before anything reads the result.
- One extra SQLite statement per push and one per rewrite output. Negligible next to
  the register.
- `confirmed_only` is a new parameter on a hot-ish path; wrong default = wrong polarity
  silently. Mitigation: **keyword-only, no default**, so every call site states which
  question it is asking.
