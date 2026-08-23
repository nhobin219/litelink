# Plan: restore two polarities to the archive extent record

**Status: two review rounds, redesigned after the second.** The first round found a wrong
self-answer and a detail that would have made a race worse than today. The second found three
blocking defects in the fix — and the third of them, that an OLD build's evictor reads a new
build's intents as landed coverage, could not be gated from the new side at all.

That one changed the design. Intents no longer live in the `extent` table as a flagged
column; they live in **a table of their own**. A reader that does not know the table exists
sees exactly today's behaviour, which is what makes a rolling upgrade safe — and it dissolves
the other two blocking defects with it.

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

A new table, not a new column:

```sql
CREATE TABLE IF NOT EXISTS extent_intent (
  rel_path     TEXT PRIMARY KEY,
  start_offset INTEGER NOT NULL,
  end_offset   INTEGER NOT NULL,
  bytes        INTEGER NOT NULL
)
```

`extent` keeps its exact shape and meaning: **a row there is a copy that exists.** A row in
`extent_intent` is a copy that was *intended*. Confirming is moving the fact from one table
to the other.

**`bytes` is on the intent because RULE 1 needs it**, and only rule 1. The live confirm gets
its sizes from memory (see §2) — an earlier draft of this paragraph said the column existed
because the confirm had nowhere else to get them, which the memory-carried design made false
and which contradicted §2 two pages later.

What needs it is the crash-heal. A rewrite's scratch buffer, the only source of
`group_bytes`, is closed and deleted in a `finally` that runs *before* `replace_range`; so a
crash between the commit and the confirm leaves rule 1 to record those files at the next
sync, and without the column it records every re-cut output at `compact_size` — including the
deliberately undersized tail, which `_badly_sized` then treats as full for ever. `named_at` is not on the
intent: nothing reads it once rule 3 has no TTL clause.

Three defects the second review found in the column design dissolve here, which is why the
design changed rather than the column being patched:

- **Mixed versions.** An older build has no idea `extent_intent` exists, so its eviction
  query is unchanged and reads only landed copies — the safe polarity, by construction rather
  than by a flag it does not know to filter on. A column would have read to it as landed
  coverage, and no gate in the new build can stop an old one; the `lease`-table refusal works
  only because it makes the NEW build refuse.

  That is mixed-version **safety, not protection**: an old compactor beside a new syncer
  reproduces the original window exactly, because "today's behaviour" includes today's bug.
  The window closes only once every process that runs `compact` or `sync` is upgraded. An old
  build's `record_file` also confirms without forgetting, leaving a duplicate intent whose
  range equals the row it duplicates — no extra over-block, swept by rule 1 later.
- **Migration.** `CREATE TABLE IF NOT EXISTS` is the idiom this buffer already uses ten times
  over, and is atomic. The column design needed a `pragma table_info` check plus an `ALTER`,
  which is check-then-act and races two processes opening together — the ordinary shape here,
  a writer and a maintainer starting at once.
- **The upsert.** `record_file`'s conflict action updates only `bytes`, so a confirm written
  as that upsert would have taken the conflict branch and left `confirmed = 0` for ever:
  eviction never advancing past the first synced file, a permanent pin on day one. With two
  tables the confirm is the existing upsert, untouched, plus a delete from the intent table.

### 2. Writes

| site | today | proposed |
|---|---|---|
| `log.py:1766` sync, after register | `record_file(...)` | unchanged, and it becomes the confirm; a new `intend_file(...)` is called for EVERY uploaded file before the register |
| `log.py:1687` sync backfill from manifest | `record_file(...)` | unchanged — the manifest is proof |
| `log.py:2030` hydrate | `record_file(...)` | unchanged — a local path, not an archive claim |
| `_maintenance.py:1002` rewrite scratch | `record_file(...)` before `replace_range` | becomes `intend_file(...)`, moved to before the `archive.put` above it; `record_file` after `replace_range` commits |

`record_file`'s upsert — its columns, its conflict action — does not change; that was the
point, since the column design failed precisely there. The METHOD does change: it grows a
`forget_intent` beside the upsert and therefore a transaction wrapper, where today it is a
single locked statement. `_transaction` is not re-entrant and no caller of `record_file`
holds one, so the wrap is safe.

Two new methods carry the intent side:

```python
def intend_file(self, rel_path: str, start: int, end: int, held: int) -> None
def forget_intent(self, rel_path: str) -> None

# intend_file is an UPSERT:
#   INSERT INTO extent_intent (...) VALUES (...)
#   ON CONFLICT(rel_path) DO UPDATE SET
#     start_offset = excluded.start_offset,
#     end_offset   = excluded.end_offset,
#     bytes        = excluded.bytes
```

and `record_file` calls `forget_intent` for the same path in its own transaction, so
confirming is one atomic move rather than two states a crash can sit between.

`intend_file` **must** be an upsert, and a bare INSERT bites in this design's own centrepiece
race: a zombie holder resuming its upload loop writes an intent for a path the lawful rival
is also intending, and the `PRIMARY KEY` collision raises an `IntegrityError` — which the
shipped role does not catch, so the LAWFUL holder's pass dies. No durable damage; a restart
reconciles. But it is the takeover race killing the wrong process, which is not a thing to
discover in production.

**`intend_file` runs immediately before each upload**, not batched before the register. Both
close the window, but per-upload also gives the archive object the path-before-file property
(I2) that the rest of this design leans on — the same reason a seal writes its path down
first.

**The confirm takes its facts from memory, not from the intent table.** `_recut`'s `written`
list becomes `(rel_path, start, end, bytes)` tuples carried across `replace_range`, and
`_push` already has everything it needs in `uploaded`. The intent table is the *durable*
record, for rule 1 to heal a crash from — it is not the confirm's data source, and a confirm
must never skip a file because the intent is missing. That case is reachable: a rival sync
that lawfully acquired after a lapse can run rule 3 mid-rewrite, when the outputs are not yet
in the manifest, and drop a live rewrite's intents. If the confirm depended on those rows it
would silently degrade to rule 2's `compact_size` default — the exact failure the `bytes`
column was added to prevent, reappearing in a narrower window.

Three things about the confirm:

- **It recreates, it does not merely mark.** A rival sync's reconciliation can delete this
  push's intents while its register is in flight, so the confirm has to be able to write a
  row from nothing. `record_file`'s upsert already is that; a bare `UPDATE` would match
  nothing and leave register-landed-no-rows, which is the window being closed.
- **It runs on register success regardless of the identity fence.** No code motion is needed:
  the record loop already sits *before* the post-register fence, which raises after the rows
  are written. A register that landed is a copy that exists, and recording it is what makes a
  later re-point back coherent.
- **Both the intent AND the confirm run for every uploaded file.** The post-register loop
  skips files with no measured size (`held is not None`), which the second review caught: in
  the takeover race the confirm is the only thing recreating deleted rows, so leaving the
  guard on it reopens today's window for exactly the files the intent was added for. Unknown
  sizes take `compact_size`, as the backfill already does.

### 3. Reads

- `Buffer.archived_ranges(prefix, floor, *, include_intents: bool)` — a `UNION` over
  `extent_intent` when true, and today's query verbatim when false. Keyword-only with no
  default, so no call site can get the polarity by accident.
- **compaction** → `include_intents=True` (overstates; safe). Three call sites, which must
  agree because they are one rule: the compact pass, the per-run recheck, and `_push`'s
  `frozen`.
- **eviction** → `include_intents=False` (understates; safe). Its SQL is then byte-identical
  to today's, which is the point.
- `archived_prefix` takes the flag the same way — **keyword-only, no default** — and is the
  only PRODUCTION caller once
  `_push`'s `recorded` computation is replaced. `tests/test_archive.py` calls
  `archived_ranges` positionally; the keyword-only flag breaks it at build time, which is the
  point of making it keyword-only. Roughly a dozen `archived_prefix` call sites in
  `test_archive.py` and `test_maintain.py` break the same way, and each needs a deliberate
  polarity: assertions about what EVICTION would do take `include_intents=False`, assertions
  about what compaction or `frozen` would do take `True`. Breaking them is the design working
  — every one is a place someone has to choose which question is being asked.
- The intent leg of the union carries the same `"://"` filter as the extent leg. Every intent
  is remote by construction, so it changes nothing — it keeps the two legs reading alike.
- No index on `extent_intent`. It is bounded by in-flight work, not by history.
- `archived_through()` → **no change.** It reads `meta`, not `extent`, and is written after
  the register and reconciled from the manifest, so it is already confirmed-polarity by
  construction. The first draft said "confirmed only", which is either a no-op or an
  unplanned change to the seal-replay guard.
- `memory()` / `file_bytes()` → **no change.** A size for a file that may not exist is
  harmless; sizes are policy inputs, not invariants.
- `file_ages()` → **no change**, and it belongs in this list: the first draft claimed a
  complete reader inventory without having one. Eviction dates files by local root-relative
  key, so URI-keyed rows are never looked up.
- `last_queued_end()` → **no change**; it filters to `rel_path IS NULL`, which no archive row
  satisfies.
- `_seed_group`'s `SELECT max(end_offset) FROM extent` → **no change**, and the second review
  caught that the first inventory got its reason wrong: this one has **no filter** and already
  reads archive rows today. It is safe because a push intent duplicates the range of a live
  local file and a rewrite intent sits under the archive's extent, so neither can raise the
  max — and with a separate table it does not see intents at all.

**Ordering requirement.** Reconciliation runs under the push claim *before* `frozen` and
`settled` are computed, so no unconfirmed row under the pinned archive survives into the
decision. The three sites that must pass `include_intents=True` are `_maintenance.py` compact
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

1. path in the manifest, intent row present → **confirm it**: `record_file` + `forget_intent`.
2. path in the manifest, no row in either table → **insert into `extent`** (today's backfill).
3. path absent from the manifest, intent row present → **drop the intent.** The register
   never landed.

**Rule 3 needs no TTL clause, and the first draft's justification for one was wrong.** It
claimed the clause protects a register still in flight from a rival's reconciliation. It
cannot: a rival can only acquire the claim once it has lapsed, and the claim is renewed at
the very point the intents are written — so by the time any rival reconciles, those intents
are already a full TTL old and the clause deletes them anyway. What actually protects the
race is the confirm recreating the rows, plus the rival writing its own intents over the same
range. Adding a TTL test would buy a marginal case and delay cleanup of genuinely dead
intents by a pass; leaving it out keeps rule 3 a plain statement about the manifest.

The residual window this leaves, named rather than papered over: a rival deletes the live
intents, then crashes *before writing its own*, while the stalled register lands and its
holder dies before the confirm. If nothing else happens, rule 2 heals it at the next sync from the manifest. If a
compaction-target change lands first, compaction regroups those files and commits a straddler
— and that branch is **the original failure, permanently**, not something a later sync
repairs. It needs a lapse, a rival crash inside a sub-second gap, the holder's death, and a
config change, so it is orders of magnitude narrower than the window being closed. It is not
zero, and it is the same shape, which is worth saying rather than implying.

**What rule 3 discards.** Dropping an intent discards the only local record of an
uploaded-but-unregistered object's name. For a PUSH intent that object is reclaimed by the retry overwriting the same deterministic
key. For a REWRITE intent — which is most of what rule 3 sees — that reasoning does not
apply, because rewrite outputs carry per-attempt UUID names; their reclamation rests on the
`compacting` claims, recovery, and drain instead. Both work; the first draft gave the
first reason for both cases. Either way this matches the existing behaviour of a declined
register, which returns without queueing its uploads. Queueing them instead has its own cost:
a raced path whose register does land stays vetoed and queued for ever, and any remote queue
entry makes `_expire_archive` claim and open the archive on every expire pass. **Decision:
bare delete**, consistent with the declined-register path — recorded here so it is a choice
rather than an oversight.

**The API this needs, which the plan previously left unspecified.** Both sides of the match
are path-keyed, and neither exists today: `archived_ranges` returns bare `(lo, hi)` tuples.
Reconciliation needs two reads — intent rows under a prefix returning
`(rel_path, lo, hi, bytes)`, and the archive-side rows of `extent` keyed by path — plus
`forget_intent`.

This **replaces** `_push`'s existing `recorded` computation, which today derefs
`archived_ranges` and matches by range tuple. The writes table above calls the backfill
"unchanged", which is true of its *effect* and not of its code; and the claim elsewhere that
`archived_prefix` is the only caller of `archived_ranges` is only true after this rewrite. Bounds, which differ between the two reads and must:

- the manifest set and the extent-by-path read are bounded by `base = min(local lo)`, as the
  backfill already bounds its walk — otherwise the archive-proportional scan the current code
  explicitly avoids comes back.
- the **intent read is unbounded**. That is what makes "an intent below `base` reads as absent
  and is dropped" possible at all; bounding it would leave those rows unreachable for ever.
  Dropping them is harmless because no local file sits there for any reader to care about,
  and it is a deliberate choice rather than an inherited accident.

Rows under a **different** archive's URI are left alone, confirmed or not. We cannot check
them without opening that archive.

The cost is not zero, and the first draft said it was. Compaction over-blocks those ranges
for ever, occasionally for a copy that does not exist. What makes it acceptable is that
eviction does not stall behind them: `_push` counts foreign coverage in `frozen`, so those
files are settled by definition and pushed again to the current archive, after which
confirmed rows under the new URI unpin eviction. Re-pointing back resolves them properly.

## What three rounds attacked

The six doubts this section used to list were written against the `confirmed`-column design
and are answered or obsolete. What survived the attacks, and what did not:

- **Wrong when written**: reconciliation matching by range rather than path; the confirm as a
  bare `UPDATE`; rule 3's TTL justification; "intents only exist inside the crash window";
  "`archived_through` reads extent rows"; a reader inventory claimed complete twice and
  incomplete both times.
- **Redesigned rather than patched**: intents in a column, which no gate could make safe for
  an old build's evictor.
- **Held under attack**: the polarity split itself; the ordering requirement; `_refuse_straddle`'s
  inputs being untouched; that this closes the original window; that a foreign intent cannot
  stall eviction, because `frozen` settles those files and they are re-pushed.

## Two cases deliberately left open

- **A permanently detached log.** Rule 3 runs only under `sync`, which raises on a local-only
  log, so dead intents there are never swept. Cosmetic permanent over-block, the same class
  as the foreign-URI rows above.
- **`_rewrite_run(upload=True)`.** No caller passes it today. If it is ever activated it needs
  the same intent treatment, or it reopens this shape on the archive side.

## Test plan

- The takeover race: a lapsed claim, a rival reconcile dropping live intents, the register
  landing anyway — the confirm must recreate the rows in `extent`, asserted by reading the
  table rather than by the absence of an error.
- The residual window: rival drops the intents, crashes before writing its own, register
  lands, holder dies before the confirm — rule 2 must heal it at the next sync.
- A mixed-version log: rows written by this build, read by the previous one's eviction query
  — it must see only landed copies, which the separate table gives for free.
- A recut crash before `replace_range`: the dead intents must be deleted, not confirmed.
- **A confirmed row carries the MEASURED bytes, not the default** — specifically a rewrite's
  undersized tail confirmed through rule 1's crash-heal. Without this the whole test list
  passes against a confirm that records `compact_size` everywhere, which is precisely the
  failure the `bytes` column exists to prevent. Falsify it against exactly that.
- A declined register. Two cases, and the first draft of this line asserted the wrong one:
  when a rival registered the SAME deterministic paths, the copies exist and rule 1 *should*
  confirm them — asserting otherwise would fail against correct code. The case worth testing
  is the different-path decline, through `register`'s watermark branch.
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
- **Rule 3's delete is the one genuinely new subtraction.** Dropping a real intent
  understates coverage, which is compaction's unsafe direction. Its conservatisms: pinned
  prefix only, intent table only (never `extent`), path-absent from the manifest, and ordered
  under the claim before anything reads the result. Not, as the first draft claimed, a TTL
  test — see rule 3.
- **The healing rests on deterministic keys, and the intents protect them.** A retry
  re-uploads to the same path only because compaction has not merged the local files
  underneath it — which is exactly what the intents prevent while they exist. The mechanism
  guards its own precondition.
- **`forget_deletion` can delete a live rewrite's confirmed row**, since it deletes by
  `rel_path` and a recovered-but-not-dead rewrite's output carries the same URI. Healed by
  the post-commit confirm, which is one more thing resting on the confirm being able to write
  a row from nothing.
- **`forget_deletion` must NOT also clear `extent_intent`.** Symmetry invites it and it would
  be wrong: rule 3 already sweeps dead intents, and a symmetric delete destroys a live
  recovered-but-not-dead rewrite's intent at precisely the moment the line above needs it to
  survive. Said here because an implementer tidying up would add it without noticing.
- **A crash between `replace_range` and the rewrite's confirm loop** leaves the new files live
  but only intended. Eviction's answer is carried by the superseded files' rows until drain
  removes them, so confirmation depends on a sync running rule 1 within the
  `snapshot_retention` grace; otherwise eviction pins below the rewritten range until one
  does. A stall, not a loss.
- One extra SQLite statement per push and one per rewrite output. Negligible next to
  the register.
- `include_intents` is a new parameter on a hot-ish path; wrong default = wrong polarity
  silently. Mitigation: **keyword-only, no default**, so every call site states which
  question it is asking.
