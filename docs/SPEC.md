# Capture storage

**v1.0** — durable append-only capture into Iceberg tables. Embedded and local-first.

---

## 1. Architecture

```
SQLite buffer          durable on commit. unsealed rows only.
      │                WAL, synchronous=FULL, one db per stream
      │  seal at target_seal_size
      ▼
local Iceberg table    a rolling WINDOW of recent data.
      │                SqlCatalog on SQLite, file:// warehouse
      │  sync: upload data files, register into the archive
      ▼
remote Iceberg table   the full HISTORY. S3 warehouse.
```

**The archive is a superset of the local window, not a disjoint half of it.** Everything
synced is in the archive, including data the local table still holds — that overlap is what
makes losing the machine survivable. The local table is a read accelerator over the recent
range, not a separate shard.

A data file is written locally, uploaded, then registered in the archive; local eviction
later shrinks the window without touching the archive.

**No hot-path read touches the network.** A hot read is the local Iceberg table plus the
SQLite buffer, both on local disk. This holds once the machine is provisioned — DuckDB's
`iceberg` and `avro` extensions are downloaded rather than bundled, and the first read on
a fresh machine fetches them. See §7.

**Everything Iceberg provides is used, not reimplemented** — manifests, per-file column
statistics, schema with field IDs, atomic snapshot commits. The catalog is a SQLite file,
not a service, so this costs no daemon.

### Scope

| Not doing | Why |
|---|---|
| Time travel | Append-only. Snapshots are a commit mechanism here, not a query feature. Point-in-time filtering is `ingest_ts <= as_of` on a column — bitemporal, and strictly more expressive. |
| CDC | No updates, no deletes. A state change is a new row with a new `ingest_ts`. Sync is a watermark. |
| Multi-writer per table | One writer per stream. Multiple machines write separate tables; readers union. |
| A transaction / commit ID column | The library's commit boundary is batching, not meaning — whether 50 rows landed in one transaction or five is an implementation detail. See below. |

---

## 2. Layout

**One SQLite database per stream.** SQLite's write lock is per file, not per table, and one
process per stream is the intended topology.

```sql
CREATE TABLE buffer (
  litelink_offset  INTEGER PRIMARY KEY AUTOINCREMENT,  -- monotonic, never reused. see NOTE
  event_ts    INTEGER NOT NULL,      -- when it happened
  ingest_ts   INTEGER NOT NULL,      -- when we learned it
  key         TEXT,
  payload     BLOB NOT NULL
);

CREATE TABLE sealing (                -- in-flight seal intent; at most one row
  start_offset INTEGER, end_offset INTEGER, rel_path TEXT
);

CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT);
```

That is the entire hand-written catalog. File paths, row counts, byte sizes, per-column
min/max, visibility and schema all live in Iceberg.

**NOTE — `litelink_offset` must never be reused.** Iceberg's sequence numbers are per *snapshot*,
not per row, and do not exist for buffer rows, so they cannot serve as the tier boundary.
The writer assigns `litelink_offset`; Iceberg then computes its min/max as ordinary column
statistics, which is what §7 reads.

A bare `INTEGER PRIMARY KEY` is a rowid alias assigned as `max(rowid)+1` — and buffer rows
are **deleted once something off-box holds them** — at the seal, or at the sync with
`wal_replication` (§3a). Once the table empties, the next insert reuses offsets
already committed to Iceberg, silently destroying monotonicity and corrupting every
boundary read.

**Use `AUTOINCREMENT`.** It is backed by `sqlite_sequence` and never reuses a value, with no
recovery logic to get wrong.

Its reputation for being slow does not apply here. Measured at 50-row batches with
`synchronous=FULL`:

```
AUTOINCREMENT                   12,956 rows/s   77.2 us/row
explicit next_offset in meta    13,484 rows/s   74.2 us/row
in-memory counter, no persist   12,241 rows/s   81.7 us/row
```

The spread is noise — the in-memory variant does strictly the least work and measured
slowest. A ~1 ms fsync per commit swamps the bookkeeping at any batch size in use, so the
choice is a correctness question, not a performance one.

The alternatives are worse for reasons unrelated to speed. An in-memory counter must
recompute `next_offset = max(iceberg_max, buffer_max) + 1` correctly at every startup, and
getting that wrong produces the I9 failure silently. An explicit `meta` counter only earns
its extra moving part if offset ranges must later be pre-allocated across producers, which
one writer per stream does not require.

### Why there is no transaction ID

Grouping rows by the transaction that wrote them looks useful and is not, because the
boundary the library could label is the wrong one.

**The library's commit boundary is batching.** It is the size of whatever `extend()` call
happened to carry — the caller's flow control, not a decision the library makes — and that
grouping carries no information about the data. The boundary that *means* something
belongs to the application, and splits by how the stream arrives:

- **Streamed sources have no grouping at all.** Each message is independent and is committed
  as it arrives. There is nothing to label.
- **Polled sources do have one** — a fetch of 200 entities is a single observation of that
  universe at one instant. But the application knows that; the library only sees 200 appends.
  An application that wants it declares an ordinary column, or leans on the shared
  `ingest_ts` those rows already carry.

That places it on the application side for the same reason as `ingest_ts` (see below), and it
is why the one-column rule survives: the only serious candidate for a second library column
turned out to be application semantics.

**No reader can observe a partial transaction regardless**, so nothing is being given up. A
seal may split a batch across two files, but the boundary read in §7 returns the table's rows
plus every buffer row above `hi` — so the union yields the whole batch either way, before or
after a crash.

**And the truncate argument dissolves with it.** "Revert should land on a transaction
boundary" presumes rows within a transaction are jointly meaningful. In an append-only log of
independent rows, *every* offset is a safe cut point. A cut is only unsafe where the
application defined a group, in which case the application aligns it.

**Table schema.** The library owns exactly **one** column:

```
litelink_offset   int64   required   -- monotonic, library-assigned, never reused
```

Everything else is the application's schema, declared at stream creation and treated as
opaque. Iceberg computes statistics for every column, so all of them prune.

```python
litelink.Log.new(
    root, name,
    schema=pa.schema([...]),          # the application's columns
    sort_by=("event_ts", "key"),      # names from that schema
)
```

**Why `litelink_offset` is library-owned:** it is the tier-boundary mechanism. Sealing selects a
contiguous range of it, compaction filters on it, and the three-way read in §7 derives every
boundary from its extents. Monotonicity and non-reuse (I9) cannot be enforced if the
application supplies it.

**Why the library stamps nothing else — in particular not an ingest timestamp.** Nothing in
the design needs one. Retention is the only time-based operation, and file age comes from
the log's own `extent.named_at` — the moment the file was named — falling back to the
Iceberg snapshot's commit timestamp, and never from a data column. Sorting is configurable.
Statistics are automatic.

More importantly, "ingest time" is ambiguous in a way a library cannot resolve: the moment a
response arrived, the moment `append()` was called, or the moment the transaction committed
— and those differ by up to a batch. Applications with point-in-time semantics have a
specific, tested definition of which one they mean. A library that picks one is silently
wrong for everyone who meant another, and stamping it would relocate a load-bearing
invariant out of the application that specified it.

Applications that want an ingest timestamp declare it as an ordinary column and stamp it
themselves.

**An example schema** for an event-capture workload:

```
event_ts   int64    required   -- when it happened
ingest_ts  int64    required   -- when we learned it; stamped by the application
key        string
payload    binary
<promoted columns, per stream>
```

with `sort_by=("event_ts", "key")`. Point-in-time reads clamp on `ingest_ts`; analytical
predicates use `event_ts`; both prune from Iceberg statistics like any other column.

**Catalogs:**

```python
local  = SqlCatalog("local",  uri=f"sqlite:///{root}/catalog.db", warehouse=f"file://{root}")
remote = SqlCatalog("remote", uri=f"sqlite:///{root}/archive.db", warehouse=s3_prefix)
```

The archive catalog's SQLite file is itself replicated to S3, so other machines can attach.
A REST catalog is a drop-in replacement once more than one machine needs to write.

---

## 3. Write path

Rows append to `buffer`, one transaction per batch. Durable on commit — that is the whole
durability story.

**The batch is whatever one `extend()` call carried**, and `append(row)` is `extend([row])`.
So the batch size is a property of the call site, not a setting: nothing in `LogConfig`
tunes it, and no throughput figure here means anything without it stated.

**`append()` and `extend()` validate per row, and that is the right trade for them.** A
mapping carries no schema of its own, so I17 is enforced one row at a time — names in
Python, types and ranges by the buffer's CHECK constraints. It costs about 400 ns/row,
which is nothing against a per-transaction fsync: these calls exist for true streams and
small batches, where a row costs microseconds to hundreds of microseconds and the check is
under 3% of it. A caller batching thousands of rows per call pays a visible ~10% and wants
the columnar entry point instead, where the schema is checked once for the whole batch —
see §13's *Bulk ingest*.

Reference throughput on network-backed storage at a 2 KB row: 21,850 rows/s at
`synchronous=FULL` (46 µs/row), against a raw `append+fdatasync` floor of 30,464 rows/s.
The batch size behind that pair predates the benchmark harness and was not recorded, which
is the mistake this section now warns about — `just bench` prints the whole curve, one row
per batch size, and is the number to trust on target hardware. On local NVMe the fixed
per-commit cost is a larger fraction.

---

## 3a. Optional: WAL replication for RPO

Without it, unsealed rows exist only on local disk, and nothing bounds how long they stay
there: the seal fires on `target_seal_size` alone, so a stream that goes quiet holds its last
partial file's worth of rows indefinitely. **With the `max_age` timer removed, replication is
the only thing that bounds RPO at all** — it is no longer a way to avoid a trade, it is the
mechanism.

That removal is what makes it clean. A timer bounded RPO by sealing early, which meant the
same knob set the file size and the loss window, and shrinking one wrecked the other. Shipping
the WAL separates them completely: files are sized by `target_seal_size` and RPO falls to the
replication lag, which is a property of a sidecar rather than of the layout.

**Litestream (or equivalent WAL shipping) is the sidecar.** It continuously replicates
SQLite WAL frames to object storage, and litelink never starts it — that is a separate
process reading the WAL, which is exactly why replication does not put the network in the
write path. What the library owns is `Log.databases`: which files carry the log's state and
therefore have to be replicated.

**`wal_replication` is a declaration, not a supervisor.** This paragraph used to say the
opposite — that replication is not configured in `LogConfig`, because a boolean claiming a
sidecar was running would be a setting nothing reads. The flag exists now, and it is read:
`_discard_on_seal` consults it on every seal to decide whether the rows stay in SQLite until
the archive has them, and `validate` refuses it without an archive to ship to and refuses
`wal_retention` without it. What it still does not do is assert that a sidecar is running,
which the library cannot know. So it states an intent the deployment has to honour, and
stating it falsely costs the growth without buying the durability that growth was traded
for: seals hold their rows, nothing ships them, and only `sync` reaching the range releases
them.

`wal_retention` is the other half, and the sidecar enforces it rather than litelink.
`replication_config` emits it as a per-database `snapshot:` block, deriving `interval` as
half the retention so a window can never hold zero snapshots — a restore needs a snapshot at
or before the point it is restoring to.

**All three databases, not just the buffer.** `buffer.db` holds rows no Parquet file has
yet, `catalog.db` says which files the local table is made of, and `archive.db` says the
same for the archive.

That last justification used to be "omit it and the objects in S3 survive with nothing to
say what they are". It is no longer true — the archive publishes `version-hint.text` and can
name its own metadata — and the file is now replicated for the *same-machine* case, where
it saves a round trip, while a failover deliberately does NOT restore it: a stale copy wins
over the bucket's own pointer and reads the archive short. See §3a and `Log.restore`.

**One sidecar per root.** Two of those three live at the root and are shared by every log
under it, so a sidecar per log would run two instances against the same `catalog.db` — what
litestream forbids — and ship them to one replica path. `Log.replication_config` describes
the log it was asked about, so a root holding several logs needs one config naming every
buffer under it, written by hand until this generates it. One log per root avoids the
question.

Optional. Three things to be clear about:

**It covers append→sync, and it used to cover only append→seal.** The difference is a
hole this paragraph once described as a lag. A seal moves rows out of SQLite and into a
Parquet file no sidecar replicates, so deleting them at seal removed the only off-box copy
of a range the archive did not hold yet — and the machine dying in that window lost them
from the MIDDLE of the offset space: below the seal frontier so the buffer no longer had
them, above the archive frontier so the bucket did not either.

**So a seal keeps its rows when `wal_replication` is on**, and `sync` drops them once the
archive holds the range. It is I4 one tier up: never delete the only off-box copy. Reads
are unaffected — the buffer leg is bounded by the local table's committed extent (§7), so
held rows never reach the engine — and the cost is that `buffer.db` grows with sync lag,
which a stalled sync makes unbounded, like a stalled eviction (§11).

The gate is `wal_replication`, not "an archive is configured": with no sidecar the buffer
and the Parquet share a disk and die together, so holding buys nothing. With neither, a
seal discards as it always did, because nothing would ever release the rows.

```
RPO = WAL replication lag        (with wal_replication)
RPO = max(WAL replication lag, Parquet upload lag)   (before this; the hole)
```

**Where a row can be, and which of those places is off the machine.** The
offset space has two holes in it, and they are different problems:

```
  [0 .......... archived]  safe — in the archive
                [archived ...... sealed]  hole A — local Parquet only
                                  [sealed ... replicated]  safe — in buffer.db
                                            [replicated ... assigned]  hole B
```

**Hole A is closed.** It was the band a seal moved out of SQLite into a Parquet
file no sidecar replicates, above what the archive had taken — so it was on the
dead machine and nowhere else. Holding those rows until `sync` pushes the range
removes it.

**Hole B is inherent.** Rows appended inside the replication lag were returned
to callers by `append` and never shipped. Nothing recovers them; what a restore
must not do is hand their offsets to different data, which is why it reserves a
window rather than resuming at the replica's frontier (I9).

**It cannot break writes.** It is a sidecar reading the WAL, not something in the write path.
If it dies, SQLite is unaffected: you lose replication, not data. That is why it does not
violate the no-network-in-the-write-path property.

**A restored buffer holding sealed rows needs no reconciliation.** The read boundary (§7)
comes from the table's committed max offset, so those rows fall outside the buffer's
contribution automatically. That is what makes holding them affordable above, and it is
what made an out-of-date replica safe before it.

It is NOT the same claim as "a restore is correct by construction", which this said until
a measurement disproved it: `catalog.db` records absolute paths to local Iceberg metadata
that no sidecar replicates, so restoring the databases onto another machine and opening
the log fails outright. See §3a's failover notes and `Log.restore`.

## 3b. Reading a log from another machine

`Log.follow` assembles a **read-only view of a log running somewhere else**: the archive
merged with a restored copy of the writer's `buffer.db`, so a reader sees data fresher than
the archive alone — down to the replication lag rather than to the seal cadence. It is what
§3a's replication buys on the read side, and it exists because the alternative readers had
was the archive, which is `sync`-fresh at best.

**It is `restore`'s assembly without the takeover.** `restore` burns `RESTORE_RESERVE`
offsets to fence a machine that may still be writing (I9); a follower appends nothing, so
there is nothing to fence and it reserves none. It opens read-only, which is also what keeps
`recover()` from finishing a seal the primary owns.

**A snapshot, not a subscription.** litestream restores to a point in time, so refreshing
means assembling another one. That is why the root defaults to a temporary directory the
follower owns and removes on close: it is scaffolding for one read session, not a durable
artefact. The archive metadata is pinned at assembly for the same reason — and because
`previous-versions-max: 10` means ten further primary commits delete the metadata object a
follower is holding, **with no time component at all**. A busy primary can sweep a follower
in seconds. Every read therefore re-reads that pointer and refuses with "reassemble" rather
than serving from a snapshot that is gone.

**Two things are unrepresentable rather than refused**, which is why `Follower` wraps a
`Log` instead of extending one:

- **There is no `include_archive=False`.** A follower's local Iceberg table is empty by
  construction — `_assemble_follower` creates it that way — so reading without the archive
  returns the replicated buffer alone: a fraction of the log, silently. As a subclass this
  was a parameter that had to raise; wrapping deletes it.
- **There is no `write_replication_config`.** `litestream_config` keys each replica on the
  path *relative to the root*, so a follower with the same log name produces a key identical
  to the primary's. A sidecar run in a follower's root — which this project's own convention
  says to do — would ship the follower's stripped scratch copy over the primary's only
  off-box record of its unsealed rows. `_restore_replica` keeps the config it needs in a
  temporary directory of its own for the same reason.

**`coverage()` reports, it does not adjudicate.** A follower is assembled from two tiers and
cannot ask the primary anything, so the failure to avoid is silence, not incompleteness. It
returns the archive extent, the buffered extent, the gap between them if any, and whether
the followed log declared `wal_replication`.

**A gap is not necessarily loss**, and nothing local can tell the two apart. An offset range
in neither tier is either a band the buffer lost — rows sealed while `wal_replication` was
off are discarded at seal and gone — or a reserve that was never issued, which `Log.restore`
creates 2**20 of and `start_offset` creates on purpose. From a replica they are
indistinguishable: the reserve "leaves no trace once the sequence has moved" (§13.4). A
caller who knows whether their log has failed over can read a gap that this cannot, so it
reports the gap and lets them.

An earlier design refused to open on a gap. It was wrong for exactly this reason: it refused
every followed log that had ever failed over, on a million offsets that never existed.

**It requires a published archive.** `archive_extent` returns None both for "nothing pushed"
and for "a hint over an empty table", so a log before its first successful sync cannot be
followed. Serving the buffer alone instead would be a reader silently missing every archived
row — the one failure this must not have.

## 4. Seal

Triggered on `min(target_seal_size, target_seal_rows)`, evaluated by the writer at commit
time.
Entirely local. Both are ceilings on one file — bytes bound memory, rows bound the read
latency §7 sizes for — so the cut lands on whichever is reached first, and `target_seal_rows`
defaults to no limit because only the caller knows how wide a row is.

There is no timer. A `max_age` branch was specified here and removed: it emitted a small
file every interval on a quiet stream — the layout §6 exists to repair — and made one knob
serve as both a file-size and an RPO policy, so shrinking it to lose less on a crash
produced worse files. Freshness in the cloud is §3a's job.

```
1. SQLite txn: choose [start, end), write it to `sealing`.
2. Write Parquet locally; commit it to the local Iceberg table.
3. SQLite txn: delete buffer rows < end; clear `sealing`.
```

**Rows are sorted before writing**, by the configured `sort_by` (§12). This is declared as
the table's Iceberg `sort_order` *and* actually applied at write time — the metadata records
intent, it does not sort for you.

**Declared in three places, and each answers a different reader.** The local table's
`sort_order` and the ARCHIVE's say what the data is clustered by, to anything reading either
Iceberg table directly; the archive's went undeclared until failover needed it, which made
an archive holding clustered data silently say nothing about it. `meta` carries it too, and
that is the copy `open` reads — because the local catalog cannot be restored onto another
machine, so a failover rebuilds the local table and has to be told what to declare. An empty
`sort_by` is a value meaning unsorted, and clears all three; the archive's declaration is
best effort, since a re-sort has already rewritten every local file by the time it is
attempted and an unreachable bucket must not fail it.

Sorting only improves **row-group** statistics within a file; file-level statistics are
already tight for `litelink_offset` and `ingest_ts`, because a sealed file covers a contiguous offset
range and therefore a narrow ingest window. So sorting by `ingest_ts` buys nothing.

`event_ts` is the column that needs it. On any stream that backfills — an API returning
records far older than the moment they were fetched — a file's `event_ts` range spans that
whole history, and
without an internal sort every row group's min/max covers it too, so an `event_ts` predicate
prunes nothing below the file. Note that leading with `ingest_ts` would defeat this: it
sorts `event_ts` only within each identical-ingest batch, and values interleave again across
the file.

Default for capture workloads: **`(event_ts, key)`** — `event_ts` primary because the
dominant access pattern is cross-sectional (every key at a timestamp), `key` secondary so a
single-key scan clusters within a timestamp.

The sort costs one in-memory sort per seal, over at most the `target_seal_size` bytes of rows
that seal holds, and does not affect the tier boundaries in §7, which use min/max of
`litelink_offset` and are order-independent.

**Step 1 fixes the range before the file exists**, making the path NAMEABLE:
`{name}/data/{start}-{end}-{token}.parquet`. Chosen after the write instead, a crash
between write and commit lets new rows arrive, and the retry seals a wider range while the
first file is stranded.

The token is per ATTEMPT, not per range, so the path is deliberately NOT deterministic — an
earlier draft made it so, on the reasoning that a retry should overwrite in place. A writer
stalled past its claim is indistinguishable from one that died, and `pq.write_table`
truncates on open, so a shared name blends two writers into one file. Recovery mints a new
token and queues the abandoned name for deletion instead.

**Step 3 is garbage collection, not correctness.** Read consistency comes from the offset
boundary in §7, so the window between steps 2 and 3 is safe in both directions and needs no
`sealed` flag.

**Every commit writes a manifest, so the table must be told to merge them.** A seal is one
commit, so without merging a table of N data files accumulates N manifest avro files — and
§7's boundary, which reads per-file bounds out of the manifest entries, has to open every one
of them. Measured at 60 files: 60 manifests and a 45 ms boundary read, against 1 manifest and
2.3 ms with `commit.manifest-merge.enabled` and a `min-count-to-merge` of 2. Iceberg defaults
the property off with a threshold of 100, which suits batch jobs and means hours of
accumulation for a stream sealing every few minutes.

Merging is not a trade against write cost. Accumulated manifests slow every later commit too,
since each rewrites a manifest list naming all of them: 60 seals ran at a 110 ms median
unmerged against 65 ms merged.

**It does couple the seal to the file count**, because merging rewrites a manifest holding
every live file. That is what compaction already bounds — with compaction running the file
count peaked at 11 and seals held a 82 ms median, while the same workload with compaction
disabled reached 400 files and 426 ms seals. Worth knowing as a feedback loop: compaction
falling behind makes sealing more expensive, not just reads.

**Recovery.** On startup, if `sealing` holds a row: if the local table already contains that
path, run step 3; otherwise redo step 2. Idempotent either way.

Sealing never waits on the network. A machine with no connectivity keeps capturing and
keeps serving reads; it accumulates unregistered files.

---

## 4a. Concurrency between the maintenance passes

Everything after the seal reads the file list, does slow work based on what it read, and
commits. The catalog's compare-and-swap makes each **commit** atomic, but it does not make
that sequence **isolated**: an operation can have its premise invalidated while it works,
and its retry is what makes the stale result land. `compact` documents the case — a handle
predating another owner's eviction still lists the files it removed, merging them re-adds
the rows, and `_commit` reloads and lands exactly that.

A lock over each pass fixes it and is the wrong trade: the slow part is reading and writing
Parquet and uploading, none of which touches the catalog. What the passes actually need is
narrower, and it follows from the data model rather than from a lock.

**Offsets are immutable and files cover contiguous non-overlapping ranges (§4), so two
operations on disjoint ranges commute.** Whether they are disjoint is a comparison of two
integers. The exclusion is interval arithmetic, not mutual exclusion.

### What each pair actually needs

| pair | why it is safe |
|---|---|
| seal ∥ anything | the seal appends above every range the others touch |
| compact ∥ evict | eviction stays below every in-flight merge's `lo` |
| compact ∥ compact | each claims a distinct run and skips runs already claimed |
| compact ∥ sync | a merge must not span into what sync is archiving, and vice versa |
| sync ∥ sync | `register` declines a range the archive already covers |
| evict ∥ expire | both are metadata commits; CAS orders them and both are idempotent |

`compact ∥ sync` is the one that is a correctness matter rather than wasted work.
Compaction skips files at or below the archive watermark, so a merged file cannot span into
the archived range — unless the watermark advances *while* it merges, which makes its
inputs, chosen against the older watermark, include files sync has since archived. Pushing
that merged file adds a range partially overlapping one the archive holds:
`register`'s check declines only a range that is **entirely** covered, so a partial overlap
is admitted and the archive returns duplicate rows.

### The claim, and why it needs an expiry

Range-disjointness answers "may these two run together". It cannot answer the question
recovery has to ask: **is this in-flight record live work, or did that process die?**
Nothing derived from the data answers that; only a deadline does.

So every operation that owns a range writes a claim before its file exists (I2), and the
claim carries an owner and an expiry:

```
claims(id, owner, expires_at, kind, lo, hi, rel_path)
```

One row per **operation** rather than one per **role**. Choosing work means skipping ranges
a live claim covers; recovery means reclaiming an expired one and removing the file it
named. The intent record and the lease become the same row — which is what allows several
passes to run at once without any of them excluding the others by kind.

**Implemented**, with two consequences of one-row-per-operation that one-row-per-role hid.
Nothing overwrites a lapsed claim the way a keyed row did, so it sits there still naming its
owner: `acquire` clears expired overlapping rows and `renew` refuses once expired, or a
stalled holder extends itself back onto a range the log has moved past. And a nested claim
must carry the OPERATION's owner rather than mint one — a rewrite running inside a config
change's whole-log claim was refused by the operation it was part of, and silently did
nothing.

Two ranges are still coarser than the design wants: `sync` and the configuration changes
claim the whole log. For a configuration change that is correct — a re-point is not an
operation on an interval. For `sync` it is conservative: it cannot name the range it will
push until it has read the archive's extent, so narrowing it to `(floor, last]` is the
remaining refinement, and until then `compact ∥ sync` still serialises.

**A claim taken after the premise was read isolates nothing on its own.** Both passes choose
their work from a file list, and the claim comes after: eviction can claim a range, commit
its removal and release it before a merge that already chose those files takes its own
claim. The sources are still on disk under I6's grace, so the merge reads them happily and
`_commit` retries the swap onto the fresh table, putting every evicted row back — with a
fresh `named_at` that shields them for another whole retention period. So each pass re-reads
under its claim and revalidates: a merge checks its inputs are still in the table, and
eviction recomputes its boundary, which otherwise lands mid-file on a merged output and
makes pyiceberg rewrite the straddler at a path nothing records (I2).

**Range-disjointness does not extend to the branch pointer.** Two operations on disjoint
offsets have independent DATA and still swap the same Iceberg branch, so enabling
concurrency raises CAS contention rather than removing it. `_commit` retries with jittered
backoff; exhausting the retries is a legitimate outcome, and a caller looping over the passes
has to treat a lost commit race as "not now" rather than as failure — nothing landed, and the
work is still there next pass.

**Holding a claim is asked again at the commit, not only at the start.** A claim expires
30 s after it is taken and a stall past the TTL is the threat the TTL exists for, so a pass
that re-read its premise under the claim can still lose the claim before it commits — after
which a merge may legitimately take the range, pass its own premise check truthfully, and
commit rows the lapsed eviction is about to remove. Each pass therefore renews immediately
before its commit and refuses to carry on if it no longer holds the range. Note this is not
the same as "expired": an expired claim nobody has taken may still be renewed, and what ends
a claim is the taker deleting its row.

**A caller's heartbeat is combined with the claim's, never substituted for it.** Passing one
was the correct usage in the role-lease era, so it is a habit that survives; `heartbeat or
claim.renew` then silently stopped renewing the claim at all and answered the pre-commit
check with a stranger's callback.

**And the archive refuses a range that starts inside its extent.** Everything upstream is
arranged so a merge never straddles it, and each gap found in that arrangement has been a
fresh piece of reasoning — a crash between a register and the row recording it, then a
compaction-config change before the next sync backfills, is one that survived several
reviews. `_covers` declines only a range ENTIRELY covered, so a straddling one is admitted
and those offsets sit in two files at once, in the immutable tier, with nothing able to
repair it. Refusing costs a stall; admitting costs silence.

**That stall has no remedy today, and this paragraph used to imply one.** Nothing re-cuts a
LOCAL straddler — `rewrite_archive` works the other side — so the log stops advancing its
watermark, eviction pins below the straddling file, and the shipped sync role dies on the
`ValueError` because it catches `RuntimeError` and `CommitFailedException` only. The refusal
is still the right trade against silent permanent duplication in the immutable tier, but
"recoverable" was not true.

Reaching it needs a crash between a register and the `extent` rows recording it, and then a
compaction-target change before the next sync backfills those rows from the archive's
manifest. The window exists because the rows and the manifest are two records of one fact and
only `sync` reconciles them, while compaction decides from the rows alone. What would close
it, in increasing order of work: reconcile the rows against the manifest on the compaction
side too; or record a pushed range BEFORE the register and confirm it after, so the two
readers can take opposite polarities — compaction is safe when coverage is OVERSTATED,
eviction only when it is UNDERSTATED, which is why the pre-segment design kept two records
and read one from each; or ship a tool that re-cuts a local straddler, which would also make
the sentence above true.

The test is `lo <= extent_hi`, with no lower bound. A lower bound was there first and was a
hole rather than a safety condition: it exempted exactly the range that starts BELOW the
extent and runs past it, engulfing the whole thing — every archived offset in two files,
which is the worst version of this rather than an excused one.

**And `drain` claims, because the unlink is not metadata.** Expiry is safe claimless — a
metadata commit CAS orders, idempotent — and the deletion that follows it inherited that
reasoning without earning it. Consulting the table without declaring anything leaves the
window everything else here was built to close: `hydrate` re-registers a file under the very
name the queue still holds, deliberately reusing the archived key, and can commit that
between the veto being read and the file being unlinked. The local table then references a
file that is not there, and every scan over that range raises until eviction ages the entry
out. And it renews before EVERY deletion, not once at the top: the unlink is this pass's
commit, everything slow in a drain sits between the veto being read and the deletions —
opening the archive, walking its manifests, one remote round trip per queued object — and a
claim held for the first of those is not a claim held for the last. `hydrate` renews after
its fetch for the same reason and against the same partner: a whole file downloaded per
iteration, and the name it is restoring is one the deletion queue still holds — drain's own
per-deletion renewal cannot help there, because drain is then the legitimate holder and
hydrate the lapsed one.

**And everything a pass reads to decide a deletion is read under its claim, not before it.**
`sync` learned this for itself and eviction did not, though it acts on the same facts: it
read the archive location, and the policy, before claiming anything. `set_archive` is
documented as something the shipped writer calls on every restart and it takes the whole
log, which is free precisely while eviction holds nothing — so attaching an archive between
the read and the acquire left eviction deleting the only copy of every aged row that archive
was configured to receive, unrecoverably, since sync cannot push what has left the table.
Eviction therefore claims on the UNCLAMPED retention boundary, which only ever falls, and
recomputes everything under the claim. `set_config` gets the same treatment for the same
reason: it writes durable state that no running process would otherwise hear about, and
§8's retention reads as an obligation rather than a hint.

That refresh also forced a correction worth stating on its own: the policy now has ONE
owner. `Log` used to keep a copy beside `Maintenance`'s, with the buffer's seal target as a
third, kept in step by `set_config` writing all three. Two copies is one too many the moment
anything else can change the policy — refreshing one would leave compaction reading the new
grouping while `sync` read the old, and `runs` is shared by exactly those two so that they
cannot disagree about which files are still in play. And the refresh happens in
compaction and in `sync` as well as in eviction, because the shipped topology runs those two
as SEPARATE PROCESSES: refreshing in one place only keeps them in step within a process,
while across processes one restarting after a durable `set_config` and the other not would
leave compaction grouping under a policy `sync` had never heard of, permanently. And
compaction re-reads the archive premise under each RUN claim, not only at pass start: a sync
that ran in between, under a grouping that settles a partial prefix of a run, leaves the
rest of that run merging into a local file straddling the archive's extent — and nothing
re-cuts a local straddler, so every later push is refused and the watermark never moves
again.

**`validate` checks a PAIR, so both halves are read durably.** An evict-on-upload policy
with no archive to evict into is refused in one call, and each setter was checking its own
new half against this process's memory of the other — so two processes could assemble the
refused pair between them, after which the next maintenance pass executes it faithfully and
deletes the only copy of everything sealed. And the rule itself has to match how eviction
combines the floors: it takes the lower boundary, so the policy retaining MORE wins, and a
config is "evict on upload" only when every floor it states is one.

Reading both halves durably is still not enough, and this is the part that took two rounds
to see: read and write as two transactions with nothing between them, and the check is only
a statement about the past. Each setter could pass against a state the other was about to
change, so between them the two calls assembled the very pair neither would accept — and the
next maintenance pass carried it out. **`set_config` and `set_archive` therefore take the
same claim**, which is §4a's own rule about data, applied to the configuration that governs
it: the check and the act share a transaction, or they are not a guard. `Log.new` records
the pair in one `meta` transaction for the same reason, and both setters ask for the claim
again at the write — the read and the write are one decision only while it is held, and a
stall past the TTL between them lets the other setter take the lapsed claim lawfully and
record the other half.

**And a repairing open is a CLAIM HOLDER's privilege.** That is the other half of the same
rule, and it went unenforced at one call site: expiry is exempt from claims because it is a
metadata commit CAS orders — true of the snapshot expiry, and not true of the repairing open
beside it. Two claimless repairs collide on the first attempt, because pyiceberg writes the
metadata object before inserting the catalog row, and the loser raises a bare `Exception` no
maintainer catches; worse, a claimless drop can land after a claim holder has already created
and registered, taking the live entry with it.

**Compaction asks whether ANY archive holds a file, not the configured one.** Detaching does
not make the copies stop existing. Merging across a range some archive holds makes a LOCAL
file whose boundaries line up with nothing there, and nothing re-cuts a local straddler —
`rewrite_archive` works the other side — so re-attaching stalls the log for good: eviction
pins below it and every push is refused. Four legitimate operations reach it, with no warning
at any step: detach, raise the target, maintain, re-attach. Skipping those files costs
nothing, because only compacted files are ever pushed, so one with an archive copy is already
at the target. Eviction still asks about the CONFIGURED archive, because I4 is a promise about
where the copy is.

**And `sync` applies the same exclusion, or the two deadlock.** `stable_prefix` holds a file
back when compaction might still merge it; compaction refuses to merge anything an archive
holds. Those are one rule, and `runs` is shared between them precisely so they cannot
disagree — giving compaction a second input `stable_prefix` could not see was enough to
break it. After a re-point to a fresh prefix the floor is 0, so files the old archive covers
return to `pending`, group into a mergeable run under a raised target, and are held back for
ever against a merge that will never happen: nothing is pushed, the watermark never moves,
eviction pins on it, and nothing raises. A file no merge can touch is settled by definition.

Note what that correction cost the earlier justification: "a file with an archive copy is
already at the target" is false the moment the target is RAISED after the copy was made —
which is the scenario the exclusion exists for. Such a file stays at the size it was
archived at, and `rewrite_archive` is the tool for that.

**Opening the archive with `repair` needs the durable location, not a remembered one.** That
open may drop a catalog entry naming another prefix and create a fresh table at this one;
the claim is what entitles a caller to do it, and the location the log records is what says
WHICH archive to do it to. `sync` re-read the location for exactly this reason, and every
other repairing caller inherited the privilege without the premise — so a handle still
remembering an archive the log had left destroyed the live archive's catalog entry, after
which the next pass "repaired" again by creating an empty table over its data. Measured end
to end: 4,000 rows in, 550 readable, and no error at the point of the damage.

**The rename is an offline upgrade.** A buffer carrying the old `lease` table was last opened
by a build that coordinated through it, and nothing can make that build respect `claim`, so
the two would exclude nothing and put two sealers on one queued group. Opening such a log
refuses rather than running beside one.

### The check and the claim are one transaction

Claiming is not enough on its own, and this is the part that is easy to get wrong. Suppose
eviction reads the live claims, sees none and decides to drop everything at or below 500;
compaction then claims `[400, 600]` and starts merging; eviction commits its removal;
compaction commits its merge and puts rows 400-500 back. Two operations that each checked,
and a window between the checking and the acting.

The fix is not more checking. It is that **the read of conflicting claims and the insert of
one's own claim happen in a single SQLite write transaction.** Those are serialised (§2), so
whichever commits second sees the first, and there is no interval in which both believe
they own the range. The slow work — reading files, writing Parquet, uploading — stays
outside the transaction; only the decision is inside it, and the decision is microseconds.

This is why **eviction claims a range too**, rather than merely consulting the claims of
others. An operation that only reads leaves exactly the window above: it has decided, and
nothing durable says so until its commit lands somewhere else entirely. Both sides
declaring is what makes the ordering total.

So the invariant is: *no operation may begin work on a range until it has durably claimed
that range in a transaction that saw no conflicting claim.* Under it, a merge's inputs are
still live when it commits, and the resurrection above is unreachable rather than
self-correcting.

For the record, when it was reachable it was a policy fault and not a duplication one. A
resurrected range is real rows at their real offsets, and `_union` bounds the archive leg by
the local table's lower extent — so a local table that regains `[1, 100]` bounds the archive
leg to `offset < 1`, and the range is served by exactly one tier either way.

**Three mechanisms, three jobs**, and none substitutes for another:

- **compare-and-swap** in the catalog: the commit is atomic, and a racing commit is detected
  rather than lost.
- **range-disjointness**: two operations never work on the same offsets.
- **owner and expiry on a claim**: in-flight or abandoned — the only question the data
  cannot answer.

### Where a segment lives is a property of the segment, not a watermark

The tiers are four steps, and only three of them are tiers:

```
buffer            rows, uncompacted, serves the newest data      (hot)
sealed files      written out fast, unoptimised, all must be scanned
compacted files   the read-optimised baseline that replaces them
        └── stored locally, or in the archive, or both
```

The fourth step is not a tier. A compacted file in the archive is the **same file in a
second place**, and litelink already records that per file: `sync` calls `record_file` with
the archive's URI, so `extent` holds a row per pushed file naming exactly where its copy
went. `archived_through` is a global summary of facts that are already durable per segment.

That summary is the single most expensive line in this design, because it is **the only
boundary in the system that can move backwards.** Offsets are immutable, seal cuts only
advance, compaction only merges forward — and then a re-point resets the archive watermark
to zero. Every reader that cached the old position is wrong at once, and there is no
ordering of the writes that fixes it, because the problem is not the write ordering; it is
that a per-segment fact was compressed into one mutable number and then had to be
un-compressed by inference.

**So do not compress it.** I4 is asked of a file, not of a watermark:

> A local file may be dropped only if `extent` holds a row for the same offset range whose
> `rel_path` names a copy in the archive this log is configured for.

An equality check on a recorded value, and the consequences fall out:

- **Nothing resets.** A re-point changes where the NEXT file goes. Files already pushed keep
  naming the bucket that holds them, so no boundary moves backwards and no cached position
  becomes wrong.
- **Identity stops being inferred.** "Is this mine?" is a comparison against a URI the log
  wrote down, not an inference from a prefix, a catalog row keyed by table id, or a
  process's memory of its own configuration.
- **Several archives coexist.** Old ranges name the old bucket and new ranges the new one,
  which is half of what makes re-attaching to an archive that already holds data
  expressible. The other half is the archive naming its own current metadata: `SqlCatalog`
  keeps that pointer in the catalog, so the local `archive.db` row was the only thing that
  had it, and a re-point drops that row. Each commit now writes `version-hint.text` beside
  the metadata, and `open_archive` registers from it instead of creating an empty table.
- **The compaction frontier goes.** `archive_pending` exists to stop a merge straddling a
  range the archive may hold; per segment, compaction skips a file that records an archive
  copy and needs no frontier, no crash window between writing it and using it, and no
  reconciliation to retire it.

`archived_through` may remain as a derived `MAX(...)` for the push floor and for display.
What it may not be again is the thing that authorises a deletion.

**Local-only is the same rule with one term absent.** With no archive there is no URI to
record and no row to find, so I4 is vacuous — not unsatisfiable. Eviction is then
`local_retention` and `local_rows` alone, which §8 already says is a deletion policy over
the only copy.

It is tempting to require that a file be compacted before local-only eviction will take it,
by symmetry with the archive case, where only compacted files are ever pushed and therefore
only compacted files are ever eligible. **Resist it.** The reason to hold a file back is
never its compaction state; it is that a merge — `_rewrite_run`, reading a run of files to
write the one that replaces them — holds it as an input right now. That is a claim, and it
is already answered above.

A merge is the pair that matters here because it is the only pass that can put rows *back*:
select `[1, 100]`, have eviction commit their removal, then commit the replacement, and the
range returns. Every other pass holding a file open merely fails when it vanishes — sync's
upload errors and retries, expire is an idempotent metadata commit. Compaction state is a
proxy for this and a bad one, since it is neither necessary (an uncompacted file no merge
has claimed is safe to drop) nor sufficient (a compacted file can be an input to the next
merge up). Requiring "compacted" instead reintroduces the trailing-run holdback
this section rejects below — bounded in bytes, at most one compaction target, but unbounded
in time, so a log that goes idle keeps its last target-sized residue for ever. Whether that
is acceptable depends on whether `local_retention` is a disk heuristic or an obligation to
delete; §8 currently reads as the latter, which would make it a defect rather than a lag.

So eviction, in both configurations, is one rule: **drop what retention no longer wants,
except what a live claim covers, and except — where an archive is configured — what has no
recorded copy in it.**

**Implemented.** `archive_pending` and the frontier are gone; `archived_prefix` walks the
local files and stops at the first without a recorded copy, and both compaction and eviction
ask it. Compaction skipping archived files is not optional here — it is what keeps a local
range and its archived range the same range, so the per-segment test can match them at all.

One window survives the change and closes differently. The row naming a file's archive copy
is written after the register, so a crash between the two leaves the archive holding a range
nothing local records. Nothing is promised beforehand to cover it — that is what made the
watermark inexact in both directions — so the next push backfills from the archive's own
manifest, which it reads anyway.

`archived_through` remains as a derived cache, for the push floor and for display. It no
longer authorises a deletion, which was the whole of the problem.

**Coverage, not equality.** The two tiers cut the same rows into files independently, so
asking whether a local range EQUALS an archived one was wrong the moment they could differ.
`rewrite_archive` re-cuts the archive to different boundaries by design — that is its entire
job — and under an equality test every local file then matched nothing, for ever: eviction
clamped to zero and stopped, and compaction stopped treating archived files as the archive's
business and merged across its extent, which `register` admits as a partial overlap and the
archive keeps as duplicate rows. Neither heals, because nothing re-cuts the archive back.
The question I4 actually asks is whether the archive holds the ROWS, so adjacent archived
files join and a gap ends the answer.

### One copy of a fact, in the log

Nine review rounds of this design found the same defect nine times, each in a place the
previous round had not looked: **a fact with a durable home in `meta` also had a mutable
copy in the process, and a decision read the copy.** A pass reading "no archive" from memory
while the log had one. A repairing open pointed at a bucket the log had left, destroying the
live archive's catalog entry. A fence comparing a value against itself, because a re-point
moved both sides of the comparison together. Two setters each validating against a stale
half of a pair. Compaction and `sync` grouping runs under different policies.

The tell was not the defects but their remedy: twelve `refresh` calls, whose only work was
dragging a copy back into agreement with the log. **A design whose correctness needs N of
those is always one short somewhere, because nothing tells you what N is.**

So the copies are gone. `Archive` stores no location and reads `meta`; `Buffer` owns the one
`LogConfig` and everything that decides from the policy reads it there. All twelve refresh
calls, and the methods behind them, deleted. A stale location or a stale policy is not a bug
guarded against here — it is not a thing that can exist.

What makes it affordable is that the reads are cheap and the expensive parts are cached
**keyed on the durable value**: the parsed config on the raw JSON, the pyiceberg handle on
the URI it was opened for. A key that comes from the log is what stops a cache becoming the
next stale copy — when the log changes, the key changes and the cache retires itself. That
is also, exactly, the archive-handle bug of the ninth round, gone by construction rather
than by a rule.

Measured: a `meta` read is 1.8 us against a 5.4 ms query, and an interleaved A/B of the
append path — the only hot consumer — showed −0.3%, which is noise. Two earlier measurements
of the same change showed 28% and 7% regressions; both were artifacts, one of a cold start
and one of drift between blocks. Interleaving the runs is what settled it.

**A decision reads the policy ONCE.** That is the hazard this trades for, and it is a real
one: each read is now independent, so two of them inside a single decision can disagree. It
bit immediately — `local_rows` seen as an int by the guard and as None by the subtraction
after it is `int - None`, a TypeError out of `maintain()`, which the shipped maintainer does
not catch, so maintenance stopped entirely. The rule is not a lock; it is that every
decision binds the policy to a local first: fresh per decision, coherent within it.

Where a torn read would merely produce an odd file size it is harmless, because the policy
is a POLICY — it decides how big to cut and when to merge, never which rows go where. The
one place it could have been an invariant is `runs`, shared by compaction and `sync` so the
two cannot disagree about what is in play, and per-segment I4 closes that: a file the
archive holds is never merged again.

What this does NOT cover: a pyiceberg table handle is a point-in-time snapshot of REMOTE
state, with no local durable copy to derive from, so `reload()` before deciding remains a
discipline. That is one method and a much smaller surface.

### Rejected: one settled watermark for both

An earlier version of this section had a single `settled_through` that compaction worked
above and eviction at or below. It does not survive, though not for the reason first
recorded here. The original argument was that a quiet stream never settles and so never
evicts, "removing anything at all" — an overstatement twice over: the holdback is the
trailing run, which is bounded by the compaction target, and with an archive configured that
stall is already the behaviour, since a stream that never settles never pushes and I4 pins
eviction regardless. It does not distinguish the design it was rejecting.

The real reason is that the number is wrong in both directions at once. With an archive,
`archived <= settled` always, so eviction at or below `settled_through` would delete files
that settled but were never pushed — an I4 violation, and the binding constraint is
`archived_through` anyway, which is never the larger of the two. Without an archive, nothing
else clamps, so the holdback becomes the only constraint and applies where it has no reason
to: "settled enough to archive" needs the trailing run held back because compaction may
still merge it and pushing a file about to be replaced is waste, while "safe to evict" is a
question about age, not about what may still change. One number cannot mean both, because
the two consumers are asking about different directions in time.

## 5. Sync

Independent, lazy, restartable, arbitrarily far behind. No read depends on it.

```
1. Upload data files not yet in the archive.
2. remote.add_files([...s3 paths...])          -- register; no data movement
3. Replicate compactions (§6) into the archive.
4. Expire snapshots on both tables.
5. Evict files older than local_retention from the LOCAL table only.
```

Step 5 removes files from the local table's current snapshot; the archive is untouched.
**A file must never be evicted locally before step 2 has registered it** — the one ordering
in sync that is correctness, not optimisation (I4).

Sync records how far it has registered in `meta`, as one offset under `archive_through`.

**Steps 4 and 5 do not belong to sync.** Snapshot expiry and local eviction are local
storage work; they are listed here because eviction must respect the registration watermark
step 2 writes. But every other step is archive work, so a log configured with no archive
never runs this pass at all — and would then never expire a snapshot or honour
`local_retention`, leaving the knob silently inert. They are owned by `maintain()` (§12),
which reads that same watermark to enforce I4 and runs with or without an archive.

---

## 6. Compaction

Runs on the happy path, and has real work to do there. Not because seals come out
undersized — every file a seal writes is already the size it was asked to be, since the cut
is exact and there is no timer to cut early — but because the seal size and the file size
are two different targets (§12). `target_compact_size` defaults to eight times
`target_seal_size`, so eight sealed files become one, and that conversion is on **with no
archive configured**: file count is a read cost locally too, measured at 1.0 ms to read the
offset boundary over one file against 44 ms over 64.

It picks up the deliberate exceptions on the way — an explicit `seal()`, which cuts short by
definition, and a change to `target_seal_size`, which leaves history sized for the old value.
It is a no-op only where `target_compact_size` is set equal to the seal size, which is how
the conversion is turned off.

Hand-written, because `rewrite_data_files` is a Spark procedure with no pyiceberg
equivalent.

The table is unpartitioned (§13), so the compaction unit is a **contiguous offset range**. That
works because sealed files already cover contiguous, non-overlapping ranges: pick adjacent
files that together hold less than `target_compact_size`, and their combined range is itself
contiguous.

**Sizing is in uncompressed bytes, never in file size on disk.** `target_compact_size` bounds
what a file HOLDS — the appender's own byte count for the rows that went into it — and that
number is carried per file from the seal that measured it, added up across a merge, and dropped
when the file is unlinked. It cannot be recovered from the file afterwards: on data compressing
8:1 a file holding a full target is an eighth of it on disk, so a rule reading sizes off disk
merges eight already-full files into one holding eight times the memory the target allows —
and, since `sync` refuses anything compaction may still rewrite, archives nothing at all in the
meantime. A file whose size was never recorded counts as full, so an unmeasured file is never
rewritten on a guess.

```
1. Select adjacent files holding < target_compact_size in total, spanning [lo, hi];
   require compact_min_files.
2. Scan them into one Arrow table; re-sort by `sort_by`.
3. Verify row count and per-column min/max against the sources.
4. local.overwrite(table, overwrite_filter=(offset >= lo) & (offset <= hi))  -- one snapshot
5. Upload the compacted file; replicate the same overwrite to the archive.
```

Using the offset range as the filter is what makes this safe without partitions: the
predicate selects exactly the source files and nothing else, because no other file overlaps
that range.

Step 3 is atomic — Iceberg swaps the snapshot pointer — so readers never observe a gap or a
double count. No grace window is needed for *correctness*.

Snapshot expiry still needs one: expiring the pre-compaction snapshot deletes files a
long-running scan may still hold open. Retain snapshots for at least `snapshot_retention`.

**Expiry does not delete the files, and the library must.** Verified against pyiceberg
0.11.1: `maintenance.expire_snapshots()` drops the snapshot metadata and nothing else — after
expiring three snapshots, `inspect.all_files()` is empty and all three Parquet files are still
on disk. So an expiry-only implementation reclaims no space at all, and both retention knobs
become inert as disk controls.

**Reclamation is a queue in SQLite, not a scan of the filesystem.** §11's *"the orphaned
file is unreferenced and swept"* invites a sweep, and a sweep is the wrong mechanism: finding
orphans by listing directories costs a walk proportional to everything retained, and becomes a
paginated LIST against object storage — priced per request, and eventually consistent, so it
can report a file that no longer exists or miss one that does.

The alternative is to make orphans impossible rather than discoverable. Every data file the
library creates has its path written to SQLite *before* it is written to disk:

| table | names | so that |
|---|---|---|
| `sealing` | a seal's output | I2 — the path is recorded before the file exists |
| `compacting` | a compaction's output | a crash mid-write is removable by name |
| `pending_delete` | superseded files | the grace period outlives the commit that ended them |

Compaction therefore writes its own Parquet at a claimed path and commits `delete` +
`add_files` in **one Iceberg transaction**, rather than calling `overwrite()`. Both produce a
single snapshot; only the first leaves the process knowing the filename in advance.

A file is then always in exactly one of four states — referenced by a live snapshot, claimed
by an in-flight seal, claimed by an in-flight compaction, or queued for deletion — and each is
a keyed read. Reclaiming space is draining `pending_delete` for rows superseded longer ago
than `snapshot_retention`, checking each against the live references, unlinking, and only then
forgetting the row: a crash between the unlink and the forget retries a no-op, whereas the
reverse order loses the path with the file still on disk.

Store when a file was superseded, not a precomputed deadline. The grace period is
`snapshot_retention`, and freezing it at enqueue time means a lowered setting never applies to
anything already queued.

**Compaction is local, which is what makes it affordable.** An object-store-native design
downloads sources, merges, and uploads, paying egress on the download. Here each byte
crosses the network at most twice over its life — once as a source, once compacted — and
never inbound.

---

## 7. Read path

### Resolving the table for a reader

pyiceberg owns the catalog; the query engine does not attach to it. Verified against
DuckDB 1.5.5 + pyiceberg 0.11.1:

```
iceberg_scan(metadata_location)   3 rows, OK
iceberg_scan(table_directory)     fails -- "no version-hint could be found"
ATTACH ... (TYPE ICEBERG)         fails -- "AUTHORIZATION_TYPE is 'oauth2'"
```

**DuckDB's Iceberg `ATTACH` assumes a REST catalog** and asks for OAuth2 credentials; it
cannot attach a pyiceberg `SqlCatalog`. Path-based scanning fails for the same reason it
always did — `SqlCatalog` keeps the current metadata pointer in the catalog rather than in a
`version-hint.text` file the way a filesystem catalog would.

**The ARCHIVE is the exception, and deliberately so.** Every archive commit writes that hint
beside its metadata (§5), which is what makes a re-point reversible and lets an engine with
no catalog read the prefix directly. The local table publishes none: its catalog sits in the
same directory as its warehouse, so nothing can be in a position to have one without the
other.

So a read is a two-step handoff:

```python
meta = catalog.load_table("cap.stream").metadata_location   # pyiceberg resolves
duckdb.sql(f"SELECT ... FROM iceberg_scan('{meta}') ...")   # engine reads
```

**Resolve per query, never pin.** Every commit writes a new metadata JSON, so a cached path
silently serves a stale snapshot.

**DuckDB does the reading.** pyiceberg resolves the pointer, DuckDB scans — both legs of the
union then run in one engine. `table.scan().to_arrow()` is not used: its query planning
happens in Python and costs roughly 100 ms per scan, growing with file count (measured: 94 ms
at 20 files, 402 ms at 180). Read throughput is comparable, since both go through C++
Parquet readers, but the planning overhead is paid on every hot-path query.

### The read path's extensions are downloaded, not bundled

Of the DuckDB extensions a read touches, only `parquet` is compiled into the wheel. Verified
against duckdb 1.5.5 on PyPI:

```
parquet          STATICALLY_LINKED
iceberg          REPOSITORY
sqlite_scanner   REPOSITORY
httpfs           REPOSITORY
```

`sqlite_scanner` is no longer provisioned — the buffer leg is not read through DuckDB at
all. See "Two SQLite libraries in one process" below.

A `REPOSITORY` extension is fetched from extensions.duckdb.org the first time a query names
it, and the fetch is silent. **So the first read on a fresh machine is a network read** — at
precisely the point the design claims to be offline. It does not degrade gracefully either:
with autoinstall disabled and nothing cached, `LOAD iceberg` fails with an IO error.

The cache is keyed by DuckDB version and platform (`~/.duckdb/extensions/v1.5.5/linux_amd64`),
so raising the duckdb floor invalidates it and every machine downloads again.

This is a provisioning obligation, not a dependency — no package pins it, so nothing fails at
install time. Install the extensions at build or deploy time, or vendor them for the
air-gapped case. How an *embedding application* discharges it, as opposed to this repo, is
§13.5.

Two details that are easy to get wrong, both verified against duckdb 1.5.5. **The extension
directory is not settable by environment variable** — `DUCKDB_EXTENSION_DIRECTORY` is
silently ignored, and `current_setting('extension_directory')` still reports the default with
it set. `HOME` moves the whole default, and
`duckdb.connect(config={"extension_directory": …})` sets it properly, which means only the
process opening the connection can relocate it. **And `iceberg` depends on `avro`**, which its
init function auto-installs, so a machine with network never notices it missing. A vendored
directory without it fails at `LOAD` asking for an extension nobody mentioned. The blob
workloads in §15 — sensor frames and point clouds — are edge deployments by nature, which is
what makes this load-bearing rather than a note about developer laptops.

### Hot read — local, bounded, offline-capable

```sql
SELECT * FROM <local iceberg table>  WHERE <predicates>
UNION ALL
SELECT * FROM buffer                 WHERE offset > :boundary AND <predicates>
```

**The boundary is derived from the Iceberg table, not from a flag:**
`boundary = max(offset)` over the local table's current snapshot, read from manifest column
statistics.

This is self-consistent at every instant, which is why the seal needs no `sealed` column:

- **before** the Iceberg commit — the boundary is the previous max, so in-flight rows are
  still served from the buffer;
- **after** the commit, before the buffer delete — the boundary has advanced past them, so
  the buffer contribution excludes exactly the rows the table now holds.

Neither window double-counts or drops.

### Cost, measured

1.02M rows (16 files x 64k) in the local table, 400-byte payloads, against a SQLite buffer
of varying size:

```
boundary: resolve catalog + max(off) from statistics      0.6 ms
iceberg leg, 1.02M rows, count                           10.8 ms
iceberg leg, 1h predicate                                12.5 ms
```

```
buffer rows   buffer scan   UNION (1h, all columns)   file size at seal
      1,000        2.0 ms                  394.1 ms             0.4 MB
      5,000        5.0 ms                  393.5 ms             2.0 MB
     20,000       20.5 ms                  421.0 ms             8.0 MB
     60,000      116.7 ms                  574.5 ms            24.0 MB
    180,000      406.6 ms                1,002.3 ms            72.0 MB
```

**The Iceberg side is nearly free; the buffer is the entire variable cost.** Per row, 180k
buffer rows cost roughly 40x what 1M Parquet rows do — SQLite is row-oriented, so there is
no storage-level column pruning and no vectorised read. `ATTACH` and `sqlite_scan` measure
identically (403 vs 408 ms) and a naive trip around DuckDB is slower (`sqlite3.fetchall`
496 ms).

**There IS a cheaper path, and it is the one shipped.** Reading the tail through the
library's own connection and handing DuckDB Arrow measures 25.6 ms against the attached
version's 46.0 ms — because it converts incrementally, re-using what the last scan already
built. It is also the only safe option: attaching the buffer puts it under two
independently linked SQLite libraries in one process, which corrupted the database on the
first concurrent scan. See "Two SQLite libraries in one process".

Below ~20k rows the buffer vanishes into noise and the union floor is the Iceberg leg alone.
Above it the cost goes superlinear: 1.0 us/row at 20k, 1.9 at 60k, 2.3 at 180k.

Projecting only the needed columns roughly halves the buffer leg (216 ms vs 403 ms at 180k)
— the one lever that does not require sealing more often.

**Consequence: the seal threshold is a read-latency knob, not only a file-size knob**, and
it separates cleanly from compaction:

| knob | controls |
|---|---|
| seal threshold (`target_seal_size` / `target_seal_rows`) | how many rows sit in the buffer, hence hot-read latency |
| compaction | how large the files end up, hence scan cost |

So **seal small and often, then compact** — rather than sealing at a large `target_seal_size` to
get large files directly. Both operations are local and cheap, and this is what makes a
small seal threshold affordable. Size the threshold so the buffer stays under ~20k rows: at
50 rows/s that is a ~5 minute seal, holding the buffer near 15k rows and its contribution
under 5% of the read.

### Read performance envelope

**What this is:** a local, in-process, **real-time analytics** store. Ingest is durable at
commit and queryable immediately, so freshness is sub-second *with* durability — which
plain DuckDB-on-Parquet does not provide. "Real-time" means fresh, not point-lookup fast.

**What it is not:** an OLTP or key-value store. Measured against an indexed row store, a
point lookup is ~1,600x slower, and no configuration closes that gap.

1.02M rows in the local table, 1,000 in the buffer, results fully materialised:

```
count(*) / group-by over the whole window          22 - 26 ms
3 scalar columns, whole window (1.02M rows)           131 ms
all columns incl. 400 B payload, last 1h              168 ms
all columns incl. 400 B payload, whole window         611 ms
point query anchored on offset or event_ts             16 ms
point query on k + a time bound                        13 ms
point query on k alone                                119 ms
  catalog resolve                                       2 ms
  buffer contribution at 1k rows                        2 ms
```

**~16 ms is a floor, not a lookup cost** — returning 1 row and 3,001 rows both cost ~16 ms.
That is metadata resolution, file open and row-group decode, and it is largely serial.

**Architecture overhead is ~4 ms** (catalog resolve + buffer), fixed rather than
proportional. Everything else is the cost of reading Parquet, which is what a reader would
pay anyway. That is the performance claim worth making: *the read speed of reading Parquet
directly*.

**Fixed is a property of the implementation, not of the design, and it has to be earned.**
The boundary comes from manifest statistics, and reading those costs time proportional to
*file count*: measured at 1.0 ms over one file and 44 ms over 64, which at the small-file
counts an undersized seal produces is most of a read. Two things bring it back to fixed.

Read the offset bounds off the manifest entries rather than through a full file-metadata
materialisation — pyiceberg's `inspect.files()` builds an eighteen-column Arrow table,
including `readable_metrics`, which decodes the bounds of every column to answer a question
about one. Roughly half the cost, and it still opens no data file.

Then cache the extent against `metadata_location`. That pointer is the table version, so an
unchanged pointer is the same snapshot and the extent cannot have moved; a changed one is
exactly when the manifests must be read again. This does not weaken *"resolve per query,
never pin"* — the resolve still happens, at ~0.5 ms, and is what decides whether the cache
stands. Measured warm: 0.38 ms at one file, 1.05 ms at 64, against 44 ms uncached.

What remains proportional after that is the scan itself opening each file, which is the
cost compaction exists to bound.

**Always bound on a prefix of `sort_by`.** Not merely on *a* sort column — on a leading one.
With `sort_by=(a, b)`, values of `b` are ordered only *within* equal values of `a`, so a
predicate on `b` alone leaves per-file min/max spanning nearly the whole range and nothing
prunes.

Measured with `sort_by=(event_ts, k)`: `k='C42'` alone costs 119 ms, while the same
predicate plus a one-minute `event_ts` bound costs 13 ms. `k` is *in* the sort key and still
does not prune on its own.

The corollary is that `sort_by` is a **read-shape decision, not a tuning knob**: it declares
which predicates will be cheap. A workload dominated by per-key lookups wants `(k, ...)`;
one dominated by cross-sectional reads wants `(event_ts, ...)`. Changing it later requires
rewriting the data.

Note this is **not** fixable by registering more statistics — Iceberg already writes
per-file min/max for every column. Pruning selectivity comes from **clustering**, not from
the presence of statistics, and sort order is the only clustering lever available. Parquet
bloom filters would be the other mechanism, but pyarrow 25 exposes no bloom-filter write
parameters, so pyiceberg cannot emit them.

**Measurement environment.** 2 vCPU (AMD EPYC 9554P under KVM), 7.7 GiB RAM with DuckDB
capped at 6.1 GiB and `threads=2`, virtio-backed storage measuring ~1068 us per fsync. Every
number here is a conservative floor. Scans parallelise, so the full-window figures should
improve close to linearly with cores; the ~16 ms point-query floor is largely serial and
will not move much; and the write throughput in §3 is the most understated, since local NVMe
fsync is 20-50 us against ~1 ms here.

### Full-stream read — all three tiers

The archive overlaps the local window, so the tiers cannot simply be unioned. Bound each by
its neighbour's **actual extent**, read at query time:

```
lo = min(offset) in the local table's current snapshot
hi = max(offset) in the local table's current snapshot

SELECT * FROM <remote iceberg>  WHERE offset <  lo   AND <predicates>
UNION ALL
SELECT * FROM <local iceberg>                        WHERE <predicates>
UNION ALL
SELECT * FROM buffer            WHERE offset >  hi   AND <predicates>
```

Correct at every instant regardless of transient overlap, because `litelink_offset` is monotonic and
the local window is a contiguous range over it. This is the §7 hot-read boundary
generalised, and it is why **no atomic handoff between the two catalogs is required** —
which matters, since two Iceberg commits cannot be made atomic with each other.

Both `lo` and `hi` come from manifest column statistics; neither requires opening a data
file. If the local table is empty (everything evicted), it drops out and the read becomes
archive plus buffer bounded by the archive's max offset.

### Historical read

Query the archive table directly. Ordinary Iceberg — any engine, no custom logic, no
knowledge of the local tier.

### Who reads what

- The **writing machine** uses its local catalog, always.
- **Other machines and engines** attach to the archive catalog. Whether a file happens to sit
  on some machine's disk is not modelled and not published; the archive describes what is in
  S3.

---

## 8. Retention

| knob | governs | too low means |
|---|---|---|
| `local_retention` | how much history the local table keeps | hot reads fall through to the archive |
| `snapshot_retention` | how long expired snapshots survive | long scans hit deleted files |

`local_retention` must exceed the longest hot-path lookback **with margin** — equal leaves
nothing for seal delay.

**`local_retention = 0` is valid**: files are evicted from the local table as soon as they
are registered in the archive, and the local table holds only what has not yet been
uploaded. Hot reads are then limited to the buffer, and anything older goes to the archive
over the network. That is the right setting for pure archival capture — litelink as a
durable staging area into Iceberg — and the wrong one wherever a hot reader looks back
further than the buffer holds. It does not weaken I4: eviction still never precedes registration.

**With no archive, retention is deletion, and that is the intended contract.** I4 forbids
evicting a file the archive still lacks — but nothing is owed to an archive that does not
exist, so the invariant is vacuous and `local_retention` becomes an ordinary retention
policy over the only copy. Data past the window is gone for good.

That is the right shape for a bounded local capture window, and it is worth stating plainly
because I4's rationale is literally *"eviction before registration is data loss."* Here the
loss is the operator's instruction rather than a bug, and the two are distinguishable only
by whether an archive was configured.

**Which makes DETACHING the operation that converts one into the other**, and that was
invisible. I4's clamp is gated on an archive being configured, so `set_archive(None)`
retires it — for every process at once, since the gate reads `meta` — and the next
maintenance pass treats the files still queued for upload as ordinary retention candidates.
A log that HAD an archive never asked for the contract above; it inherited it as a side
effect of a call that reads as "stop using the archive". Measured at 4,025 acknowledged
offsets of 8,000, deleted by a maintainer in another process that the operator never
invoked.

So a detach is refused while `local_retention` or `local_rows` is set. Clearing them first
is how an operator says the loss is intended, which puts the instruction back where this
section says it belongs. The refusal is blunt — it declines cases that are provably safe —
because the precise question is "is there an unarchived file retention would reach", and
answering it properly means keeping the clamp alive across a detach rather than asking
better questions at the setter. `local_retention = None` is the setting that keeps
everything, at unbounded local growth.

`local_retention = 0` presupposes an archive: with none, it would delete each file as it
sealed. Reject the pair at construction rather than honouring it.

Raising it is an operation, not a config change: `hydrate(since=…)` fetches archived files
and re-registers them into the local table. Without it, a raised setting applies only to
data captured afterwards.

Buffer rows are deleted once something off-box holds them — at seal, or at sync with
`wal_replication` (§3a). There is no SQLite retention knob.

---

## 9. Schema evolution

Iceberg assigns each column a permanent field ID and writes it into every Parquet file as
`PARQUET:field_id`. Resolution is by ID, not name, so **add, drop and rename are all safe at
the storage layer**:

- **add** — new ID; older files simply have no value and read null.
- **drop** — the ID is retired; its data stops being projected, which is what dropping means.
  Re-adding the same name later creates a *new* ID and cannot collide with the retired data.
- **rename** — the ID is unchanged, so every existing file follows the new name with no
  rewrite.

**The constraint is the read contract, not the format.** Iceberg resolves by ID inside the
table; it does not rewrite anyone's SQL. `SELECT qty` breaks the moment the column becomes
`quantity`, and the archive exists so external engines can query it directly.

So drops and renames are **supported but breaking for consumers**. Expose them as explicit,
deliberate operations — never as a side effect of editing a schema dict — and treat them as
a versioned change to the stream's public surface. Adds are non-breaking and need no
ceremony.

Apply any schema change to the archive **first**. The local table is a window and can be
rebuilt; the archive cannot.

**A schema change is complete when SQLite says so, not when Iceberg does.** Iceberg cannot
hold the whole declaration: it has one string type and one binary type, so a column declared
as a wide Arrow type comes back narrow, and the declared spelling has to live in the local
database beside the buffer. That makes a schema change two writes — an Iceberg commit and a
SQLite write — which cannot be made atomic with each other, for the same reason §7 gives for
not requiring an atomic handoff between two catalogs.

Use §4's shape, not a best-effort ordering: record the intended schema in SQLite before
anything changes, commit to Iceberg, then write the schema and clear the intent. Recovery
replays it, and the table's columns say which half already landed.

**Probe, never stamp.** The intent records WHAT the change is, never which of its steps have
run. Recording a step does not make it atomic with the commit it describes — it only moves
the unreconciled gap from before that commit to after it — and a stamp can be *false*:
`Log.restore` rebuilds the local table from the declared schema, so a replica captured
mid-change arrives carrying a record of a step that never landed on that machine. Every step
is settled by asking the thing itself: the archive's columns, the local table's, `PRAGMA
table_info(buffer)`, the `meta` row. This requires the Iceberg step to be replayable against
a table where it already landed, which is why it uses `union_by_name` — idempotent — rather
than `add_column`, which raises `name already exists`.

**Recovery defers; it never fails the open.** If the archive cannot be widened, the intent is
left standing and the log opens anyway. It appends, seals and reads; the change simply has
not finished, and it is safe to leave because the declaration is unchanged — so no value of
the new column can be stored, the insert column list being derived from it. Failing the open
instead would mean a change interrupted by an outage makes the log unopenable for writing
until the bucket returns, breaking §11's promise that a log works with no network.

**One change at a time.** A second change while one is outstanding is refused, and the
refusal names the pending column. This is not tidiness: the second change would write
`declared + its own column`, dropping the pending one from the declaration while the Iceberg
tables keep it, and then clear the intent — after which the declared schema and the table
disagree with nothing to explain it, and no process can open the log at all.

This is the same completion boundary every other multi-step operation here uses — a seal
completes at its final SQLite transaction, a compaction when its claim is cleared, a deletion
when its queue row is forgotten. **Iceberg holds the data; SQLite holds the record of what the
library has finished doing.** Treating the Iceberg commit as completion would leave a crash
having added a column whose declared type is gone, with nothing to indicate it.

Nothing is ever lost to a schema mistake: the raw `payload` is stored verbatim, so a column
can be re-promoted under any name at any time.

## 10. Invariants

### SQLite is the coordinator

Iceberg gives an atomic commit **to one table**, and that is worth relying on — §6's
compaction rests on it, swapping the snapshot pointer so readers never observe a gap or a
double count. What it gives across systems is nothing, and every operation here spans
systems: a seal touches the buffer, a file and the table; a compaction touches a file, the
table and the deletion queue; a schema change touches the table and the declared Arrow
schema. There is no commit that covers a pair of those.

So the local database is the coordinator, and the protocol is the same one four times over:

```
1. record the intent in SQLite        -- before anything observable changes
2. do the work, ending in the Iceberg commit
3. record completion in SQLite        -- and clear the intent
```

`sealing` is step 1 for a seal, `compacting` for a compaction, `pending_delete` for a
deletion, and a schema change needs its own. **An operation is complete when step 3 lands,
not when the Iceberg commit does.**

This is not two-phase commit, and it is better suited than 2PC would be: there is no vote and
no participant that can veto, because **the true state is always derivable**. Recovery asks
the table which half already happened — does it contain this path, does it hold this column —
and drives forward or gives up accordingly. Every step is idempotent, so replaying costs a
rewrite at worst.

The rule that follows: **never treat an Iceberg commit as the completion of anything that
also touched local state.** Doing so leaves a crash having half-applied an operation with
nothing recording that it was ever attempted, which is the one situation none of the
recovery paths below can repair.

Each needs a test.

| # | Invariant | Why |
|---|---|---|
| **I1** | The Parquet file is written and fsynced before the Iceberg commit. | The reverse publishes a manifest entry for a file that may not exist. |
| **I2** | The seal range and its path are persisted before the file is written. | No file can exist that this database cannot name. |
| **I3** | Tier boundaries are derived from each neighbour's committed offset extent at read time, never from stored flags or an assumption of disjointness. | The archive overlaps the local window by design. A flag would have to be updated in a different transaction from the Iceberg commit, reintroducing a double-count or drop window. |
| **I4** | A file is never evicted from the local table while a configured archive still lacks it. Vacuous when no archive is configured (§8). | Eviction before registration is data loss. With no archive nothing is owed, and `local_retention` is then a deletion policy the operator asked for — see §8. |
| **I5** | Reads served from within `local_retention` never touch the network or require sync to have run. | The central claim. A read that quietly needs the network reintroduces every problem this shape removes. Conditional because `local_retention = 0` is a valid archival configuration (§8) in which the local window is empty by choice. |
| **I6** | Snapshot expiry retains at least `snapshot_retention`, exceeding the longest scan. | Expiry deletes data files an open scan is still reading. |
| **I7** | Schema changes reach the archive before the local table. | The local table is rebuildable; the archive is not. |
| **I11** | `litelink_offset` is assigned by the library and never accepted from the caller. | Monotonicity and non-reuse are the boundary mechanism; an application-supplied value cannot be enforced. |
| **I17** | An append names only columns the log declares, supplies a value for every non-nullable one, and gives each a value of its declared type, or it is refused. | The insert is built from the SCHEMA's columns, so an unknown key is dropped before any SQL exists and neither SQLite nor pyarrow ever sees it — `append` would return an offset for a row it had truncated. The omission is the same wedge from the other side: a non-nullable column the row leaves out, or supplies as `None`, is stored as NULL, and then **every** scan raises `Casting field … with null values to non-nullable` — including scans of rows written before it — while `append` keeps handing back offsets. Writer sees a healthy log, readers see nothing. A row misspelling a declared column trips both halves at once: it names something undeclared and shadows the real column with NULL. The type clause closes the same two outcomes reached through a value rather than a name: SQLite has affinities, not types, so it stores whatever it is given and the declared schema is not consulted again until the read. A value Arrow cannot parse (`"x"` into an int64) wedges every scan; one it can parse but not preserve (`1.5` into an int64, `12345` into a string, `True` into an int64) is silently rewritten, so what is read back is not what was appended and nothing raises at all. Magnitude is checked with it: `2**40` IS an int and `1e300` IS a float, and they fail the same two ways — the int32 wedges every scan, the float32 reads back as `inf`. **Enforced by the buffer's DDL, not by Python.** Every column is declared `ANY` with a `typeof` CHECK, and that is the whole design: a STRICT column of a declared type does not refuse a wrong value, it CONVERTS one. An INTEGER column given `'77'` stores 77 and `'007'` stores 7; a REAL column given `'1e999'` stores `inf`; a TEXT column given `12345` stores `'12345'`. The conversion happens before any CHECK could see it, so a constraint on a typed column would be asked about a value that had already been changed. `ANY` stores the value exactly as given, which is what lets `typeof` tell the truth about it; STRICT is still declared, because it is what makes `ANY` mean "no conversion". `NOT NULL` carries the nullability half — absent and explicitly-None reach SQLite identically — and the range tests ride in the same CHECK. An integer is a legal value for a FLOAT column — `{"price": 5}` is too natural to refuse — but only within the range where every integer converts exactly (2**53 for float64, 2**24 for float32). Past it the value stays an integer in the buffer, since `ANY` performs no conversion, and Arrow then cannot build the column at all: one such value makes every scan and every seal raise for ever while appends keep succeeding. The bound is a range rather than a per-value test because a SQL CHECK cannot ask whether one particular integer is representable, so some that would convert exactly are refused too. A column added later by `add_column` gets the identical DDL through `ALTER TABLE`, which accepts a CHECK; building it from the affinity alone left every added column unvalidated for the life of the log. Python is left with the one question SQLite cannot be asked, an unknown column: the insert names the schema's columns, so a key the log does not have is dropped before any SQL exists. One leniency is deliberate — `True` into an integer column stores 1, because the driver converts it before SQLite sees it, and it is lossless. |
| **I9** | `litelink_offset` is strictly monotonic for the life of a stream and never reused, including after the buffer empties. | Rowid reuse after a delete silently invalidates every tier boundary in §7. |
| **I10** | Drops and renames go through an explicit versioned operation, never an implicit schema diff. | They are safe for the data and breaking for consumers; the format will not stop you, so the API must. |
| **I8** | Monotonic visibility: once readable, a row stays readable until intentionally retired. | Point-in-time code depends on `t1 < t2 ⇒ read(t1) ⊆ read(t2)`. |
| **I16** | Every operation spanning Iceberg and local state records its intent in SQLite before acting, and is complete only once SQLite records completion. | Iceberg's atomicity stops at one table, so nothing spanning two systems can be committed at once. Without the intent record a crash leaves work half-applied and unattributable; without the completion record the library cannot tell a finished operation from an interrupted one. |

---

## 11. Failure modes

| Failure | Outcome |
|---|---|
| Crash mid-batch | Uncommitted rows lost; committed rows durable. |
| Crash between Parquet write and Iceberg commit | `sealing` row survives; recovery redoes the commit against the same path. |
| Crash between Iceberg commit and buffer delete | The boundary has already advanced, so reads stay correct; recovery drops the stale rows. |
| Network unavailable indefinitely | Capture, seal, compaction and hot reads all continue. Unregistered files accumulate; local eviction stalls (I4). Fails only when local disk fills. |
| Two sync passes race | The Iceberg catalog commit is atomic; the loser refreshes and retries. |
| Local disk fills | Backpressure — §13.3. |
| Machine lost | Exposure is whatever was unregistered. The archive is intact and independently readable. |
| Compaction crashes mid-write | No snapshot was committed; the orphaned file is unreferenced and swept. |
| A second process opens a live log | **Currently unsafe.** Opening runs recovery, and recovery does not know which operations belong to the opener — see below. |

### Recovery ownership, and why a second process is not yet safe

SQLite handles the data locking a second process would need — measured: a capture process
took 20,100 rows beside a maintenance process with no lock contention at all — and the
Iceberg commit races are covered by refreshing and retrying, as this section already
required. What is not covered is recovery.

**Opening a log runs recovery, and recovery claims every interrupted operation, including
another process's.** Verified in both directions:

- a maintenance process opening a live log redoes the writer's in-flight seal, and fails
  re-registering a file the writer is about to register itself
- a writer opening a log deletes a maintenance process's half-written compaction and clears
  its claim, while that process is still writing it

The hazard is symmetric, so suppressing recovery in the second process fixes one direction
and leaves the other. Recovery has to know which operations belong to the opener, and
nothing today records that.

Whatever answers it would also make §1's one writer per stream mechanical rather than
conventional: today two capture processes would both write `buffer` and overwrite each
other's single-row `sealing` claim, and nothing stops them. The options are §13.6, and none
of them is chosen.

---

## 12. Configuration

```
target_seal_rows       max rows per SEAL                  (the other ceiling; the seal cuts at
                                                          whichever is reached FIRST. None =
                                                          no row limit)
target_compact_size    uncompressed bytes per FILE        (what compaction converts sealed
                                                          files INTO. None = 8x the seal)
target_compact_rows    max rows per compacted file        (None = 8x target_seal_rows)
target_seal_size       uncompressed bytes per SEAL        (size it for READ latency and for
                                                          memory -- keep buffer <20k rows;
                                                          files land SMALLER on disk, by
                                                          whatever compression achieved)
local_retention        local table window, by TIME        (> longest hot lookback, with margin; 0 = evict on upload)
local_rows             local table window, by ROWS        (floor: keep at least this many recent rows)
snapshot_retention     snapshot expiry floor              (> longest scan)
compact_min_files      minimum adjacent files to compact  (default 4; below 2 is refused —
                                                          every run would look mergeable)
wal_replication        ship the WAL with a sidecar        (needs an archive; also decides
                                                          whether a seal keeps its rows)
wal_retention          how far back a restore may go      (None = litestream's own default)
```

`sort_by` is NOT in here. Everything above governs future work only, so `set_config` needs
no rewrite; the sort order is a read-shape decision that re-clusters every file the local
table owns, so it is set at `Log.new` and changed by `set_sort_by`. It lives in `meta` beside the
schema, not in `LogConfig`.

`maintain()` runs compaction, eviction **and** expiry together, in that order, and needs no
archive. Each is a no-op or a regression without the others: compaction alone increases
storage, since superseded files stay referenced until their snapshots expire; eviction alone
frees no disk, since it removes a file from the current snapshot while the previous one still
references it; and expiry is what actually deletes bytes, held back by `snapshot_retention`
so a running scan does not lose files underneath it (I6).

The consequence worth planning for is that local disk holds roughly
`local_retention + snapshot_retention` of data, not `local_retention`.

---

## 13. Open questions

0. **The archive's identity is local, and re-pointing has to reconcile it.** Seven
   consecutive review rounds found defects in one seam, each fix adding a guard on top
   of the last. That is a design signal, and it is recorded here rather than patched
   again.

   The shape of the problem: `archive.db` is a LOCAL catalog keyed by table id, naming a
   REMOTE table. Nothing in the entry says which prefix it belongs to, so "is this entry
   mine?" is answered by comparing its metadata location against the configured prefix —
   a string comparison standing in for an identity. Meanwhile `set_archive` changes
   durable state that every other process cached at open, and the watermark it resets is
   the thing eviction deletes on.

   What has accumulated as a result: the entry is validated at open; only a lease holder
   may repair it; `set_archive` takes the maintenance lease; `sync` re-reads the location
   under that lease and re-checks it before writing a watermark; a failed repair restores
   the entry it displaced; `drain` refuses to delete outside the configured prefix. Each
   is correct and each was found the hard way.

   What would replace them: give the archive an IDENTITY the entry carries — a token
   written into the archive's own table properties at creation and recorded beside the
   URI locally, so "is this mine?" is an equality check on a value rather than an
   inference from a path. Prefix comparison then stops being load-bearing and a re-point
   becomes one durable fact to change rather than three that can disagree.

   Re-attaching to an archive that already holds data no longer waits on that: the
   archive publishes `version-hint.text` at every commit and `open_archive` registers
   from it. What the token would add is a CHECK. The hint says where this log left the
   metadata, and adopting it trusts that nothing else wrote the archive in between —
   true under the one-writer-per-log contract, and unverifiable without an identity.

   **The deferral has a measured cost, and this paragraph used to understate it.** It
   claimed the guards above were sufficient for the operations the library supports —
   attach, detach, re-point to a fresh prefix. A later round disproved that. It found
   four more defects in this seam, and unlike their predecessors two of them needed no
   race, no crash and no lease lapse: attaching an archive to a log a maintainer already
   had open let that maintainer go on deleting the only copy of every row past
   `local_retention`, because `evict` asked its own memory whether I4 was owed; and
   re-asserting an archive from a process whose memory had gone stale read as a move and
   zeroed the watermarks of a bucket that held the data. The findings got *less*
   contrived, which is the opposite of what a converging seam does.

   The reason is now legible. The archive's identity lives in four places — the `meta`
   row, each process's `Archive` object, the `archive.db` catalog row, and each captured
   pyiceberg handle — and every guard listed above synchronises one read-write pair. Each
   round finds the next pair nobody has synchronised yet. The four latest fixes (pin the
   URI per push, compare-and-set the re-point against the durable value, refresh `evict`
   from the buffer, refuse a commit whose table left its warehouse) are the same shape
   again, and they are not evidence the next round will be clean.

   The identity token above is what ends it, because it gives every guard one immutable
   value to compare and no second in-memory life. Until it exists, the honest statement
   of the contract is narrower than the API suggests: **re-pointing a live log is
   defended interleaving by interleaving, not by construction.** The regime the current
   mechanism is actually sound in is a re-point with every other process stopped.

0. ~~**Per-operation claims, replacing the maintenance lease.**~~ **Closed: built.**
   The `claim(id, owner, expires_at, kind, lo, hi, rel_path)` table exists, every
   range-owning pass claims before it works with the conflict check and the insert in one
   `BEGIN IMMEDIATE`, and recovery reclaims expired claims rather than a role's. See §4a
   and `_claim.py`.

   `sealing` and `compacting` were NOT subsumed, which the original sketch expected. They
   are intent records — the path written down before the file exists (I2) — and a claim
   answers a different question, so collapsing them would have made one row mean two
   things.

   The correctness item it was also going to fix is fixed by a different mechanism: a merge
   can no longer include files sync has archived, because compaction and `sync` both read
   the per-segment archive records rather than a watermark, and `archived_prefix` excludes
   anything an archive holds.

1. ~~**Partitioning.**~~ **Closed: unpartitioned.** Sealing contiguous offset ranges leaves
   data naturally clustered by ingest time, so `litelink_offset` and `ingest_ts` statistics are tight
   and manifests prune without a partition spec. Partitioning by event date would be
   actively harmful — a seal spanning many event dates emits one file *per partition*,
   recreating the small-file problem sealing exists to prevent, and streams that backfill
   (records arriving far older than their ingest time) would shred every seal.

   The residual weakness is `event_ts` pruning on out-of-order streams, where per-file
   min/max are genuinely wide. If that bites, sort rows by `event_ts` within each file so
   row-group statistics stay tight — do not partition. Iceberg's hidden partitioning means a
   spec can be added later without changing paths or breaking readers, so this stays
   deferrable.
2. **`payload` encoding.** Binary JSON is the simplest default. msgpack or Arrow IPC
   would be smaller. Measure on real payloads first — this is a one-way door once data
   exists, since re-encoding means rewriting the archive.
3. **Local disk backpressure.** The failure that used to be "object storage is down" is now
   "local disk fills." Bound the buffer on **bytes**, not row count — a row-count bound can
   exceed a byte-based memory limit, letting the OOM killer win the race against the policy
   meant to prevent it.
4. **Bulk ingest.** Loading an existing corpus — a backfill, an archive import — through
   `append()` row by row wastes the point of already having Parquet. The wanted path is to
   lock writes, reserve a contiguous offset range, materialise `litelink_offset` into the file, and
   commit. Four things it meets, none blocking, none free.

   **It is a rewrite, not a registration.** I11 forbids a caller-supplied `litelink_offset` and §7
   derives every tier boundary from its extents, so a file lacking the column cannot be
   registered — `add_files` zero-copy is unavailable. §4's sort applies equally, or
   row-group statistics are junk. Budget a full pass over the input, not a PUT.

   **The file is staged, not sealed.** It is raw input to the same local
   normalise-then-upload path as a seal's output, so an oversized one is split before it is
   ever registered — the mirror of a quiet stream's undersized file being merged before upload.
   §6 is merge-only, selecting files that together hold *under* `target_compact_size`, so the
   split is an addition: the same `overwrite` on the same offset-range filter, emitting N files
   instead of one, with step 3's row-count-and-min/max verification unchanged. Bulk ingest is
   what creates the requirement — a seal cannot emit an oversized file, since
   `target_seal_size` already bounds it.

   **A reservation is a hole in the offset space.** A seal spanning it writes a file whose
   `litelink_offset` statistics cover `[lo, hi]` while containing none of it; when the staged file
   commits, the two overlap, which is exactly what §6's *"no other file overlaps that
   range"* forbids. Sealing the buffer empty before reserving closes it — every subsequent
   live row is then above `hi`, so no seal has rows on both sides. This holds only if the
   seal takes `start` from the buffer's own minimum rather than the previous file's `end`.
   The weaker property is also the correct one: §6 needs files non-overlapping and adjacent
   in offset order, not free of integer gaps, and gaps already arise from rolled-back
   batches (§15.3).

   **The orphan sweep does not transfer.** §15.4 sweeps by offset against the §7 boundary,
   which works because every staged blob has a buffer row carrying `{name}_staged`. A bulk
   file has no buffer row and sits *above* the boundary until it commits, so an abandoned
   ingest reads as still-referenced forever. It needs its own table parallel to `sealing`,
   holding `(lo, hi, rel_path)` — not a row in `buffer`, which §7's hot read would union
   into reader output, and not `sealing` itself, which holds one row and would block seals
   for the length of a rewrite. The sweep rule then mirrors §15.3: a staged file with no
   row is an orphan by definition.

   **It amortises I17, which is per-row today and cannot be otherwise.** A row arriving as a
   mapping carries no schema, so every row is checked on its own: its key set against the
   declared columns in Python, its values against the buffer's CHECK constraints in SQLite.
   An Arrow batch carries its schema, so one comparison against the declared one proves the
   names and types of every row in the batch **by construction** — the per-row work is not
   optimised away, it stops being necessary. That is a second argument for this path
   independent of avoiding the row-by-row rewrite, and it is the larger one for a backfill.

   Measured on the write path: validation costs about 400 ns/row of CPU. At `1` and `200`
   rows per `extend()` that is invisible, because each transaction is fsync-bound at ~800 µs
   and ~16 µs per row respectively; at 5,000 rows per batch, where the fsync is amortised
   and a row costs ~4 µs, it is about 10%. So the cost lands exactly on the caller who is
   already batching hard enough to want this endpoint instead.

   **`start_offset` is recorded durably in `meta`, and that is not bookkeeping.** A backfill
   has to tell this reserve from a `Log.restore` fence, and after the fact nothing else can:
   both are empty ranges below the log's offsets, and `Log` says of the failover reserve that
   it *"leaves no trace once the sequence has moved"*. Position does not separate them either — a restore whose replica
   was empty leaves the log high with nothing beneath it, which is a reserve's own shape. So
   the recorded value is the only thing a backfill may bound itself by, and its ABSENCE must
   read as "no reserve" rather than "a reserve of nothing": a log created at offset 1 and
   later restored has a gap below its offsets too, and filling that gap would reissue the
   offsets the fence exists to abandon.

   It is creation-only. `Buffer.seed_offsets`' guard reads the offsets currently BUFFERED,
   which a seal empties — so it cannot refuse a re-seed onto already-issued offsets, and the
   appends that followed would be hidden by the read boundary and their file declined at
   registration. A fresh buffer is the only safe state, and `new` is the only call with one.

   Reserving needs no new counter. Bumping `sqlite_sequence` by N inside the write
   transaction reserves `[old+1, old+N]`, preserves I9, and lands above everything ever
   assigned. §2's note that an explicit `meta` counter *"only earns its extra moving part
   if offset ranges must later be pre-allocated across producers"* is the clause this
   trips; the `sqlite_sequence` bump is the cheaper way to satisfy it.

   **Reserving DOWNWARD is what makes a cutover cheap, and it is the same mechanism.** A log
   created with a non-zero starting offset — `Log.new(..., start_offset=N)` — leaves
   `[1, N-1]` permanently unassigned. Live capture begins immediately at `N`, and the
   historical corpus is bulk-ingested into the reserve afterwards, at whatever pace the
   rewrite takes. The two never contend: the backfill writes strictly below every offset
   live capture will ever hold.

   That turns a cutover from one coordinated operation into two independent ones. Without
   it, adopting litelink for a stream that already has history means either starting at
   offset 1 and blocking live capture until the backfill lands, or accepting that history
   sorts *above* everything captured since. With it, you point the live feed at litelink
   today and backfill next week.

   **It rests on gaps already being legal**, which they are twice over: the reservation
   paragraph above requires it, and rolled-back batches already produce them (§15.3). §6
   needs files non-overlapping and adjacent in offset order, not free of integer gaps.

   **I11 holds the way `Buffer.seed_offsets` already makes it hold.** The caller chooses a
   RANGE; the library still assigns every value inside it. That is the distinction I11 is
   drawing — not "the library picks the number" but "no caller-supplied number can collide
   with, or reuse, one the library has issued". A `start_offset` at creation cannot: nothing
   has been issued yet. `Log.restore` already reserves a gap this way (`RESTORE_RESERVE`),
   for the same reason in a different direction.

   **The API takes `offsets` or `start_offset`, not both.** A backfill that is one contiguous
   run wants the latter; one assembled from several files, or carrying its own ordering,
   wants the former. Either way the library validates before writing anything: every offset
   must fall inside the reserve, and the row count must not exceed it. **A backfill of more
   than `N-1` rows is refused** — there is nowhere to put the overflow that does not collide
   with live capture, and silently placing it above would interleave history with current
   data.

   **The sizing decision is one-way, and that is the sharp edge.** Once live capture has
   taken offsets above `N`, the reserve cannot grow: the space below is bounded by a number
   chosen before the first row. Underestimate and the remaining history has no home.
   Overestimating costs nothing — gaps are free, and §7's tier boundaries are extents rather
   than counts — so the guidance is to pick `N` well above the known row count.

   Two smaller consequences worth stating. A backfill smaller than its reserve leaves a
   permanent gap, which is fine and needs no repair. And the backfilled files cluster
   entirely below the live ones, which keeps offset order and time order correlated.

   **Pruning is not what that buys, and an earlier version of this section said it was.**
   Pruning is per-file; every file covers a contiguous offset range, and each era occupies a
   contiguous offset range, so a file's statistics land inside one era however the log was
   written. History appended AFTER live data prunes just as well — measured, 3 of 6 files
   read either way, and it does not depend on `sort_by` at all. What the reserve buys is
   that a scan with no time predicate returns history first, and that §7's tier boundaries
   put the oldest data in the coldest tier. Once compaction has run a single file straddles
   the era boundary and stops pruning, and it costs the same in both orders — measured, three
   files read either way — so that is not a reason to prefer one.
5. **Extension provisioning for embedders.** §7 makes the extension download a provisioning
   obligation. A repo can discharge it in its bootstrap and its CI; an application that
   `pip install`s the library runs neither. It gets the read path and no extensions, so its
   first read is the network read the design says it is not — and the offline claim is about
   that application, not about this repo.

   **Installing at import time is the option that needs no API, and it is the wrong one.**
   Network I/O inside `import litelink` fires in test suites, in processes that only ever
   write, and in anything that imports the module for an unrelated reason. It charges every
   consumer to fix the one that reads, and it fails in exactly the air-gapped environment it
   was meant to serve.

   **Vendoring the binaries into the wheel** closes it outright and costs the most.
   Extensions are per-platform and per-DuckDB-version, so the project inherits
   platform-specific wheels and a re-vendor on every duckdb bump. That is the right trade
   only if air-gapped installs become the common case rather than the interesting one.

   The likely shape is an explicit call — run at deploy or at startup, doing what
   `scripts/install_duckdb_extensions.py` does — with DuckDB's autoinstall left as the
   documented fallback for callers who have network and do not care. It stays deferrable
   because it is additive and changes nothing about the read path's design. What is not
   deferrable is writing it down, since the alternative is an embedder discovering it from a
   device already in the field.
6. **Coordinating more than one process.** Two facts are established and the design is not.

   **Recovery ownership is unsolved and the hazard is symmetric** (§11). Opening a log runs
   recovery, and recovery claims every interrupted operation including another process's —
   verified in both directions. Everything else a second process needs already works: SQLite
   handles the data locking (measured: 20,100 rows appended beside a maintenance process
   with no contention), and lost Iceberg commits refresh and retry.

   **A seal costs the append that triggers it.** Measured over 600 appends of 25 rows at a
   256 KiB threshold: median append 0.73 ms, p99 2.90 ms, and the 24 appends that sealed
   between 30.83 and 93.21 ms — up to 127x. Whichever caller crosses the threshold pays for
   the sort, the Parquet write, the fsync and the Iceberg commit.

   That second fact is what makes the first worth solving, and it also unsettles who should
   seal at all. §4 assigns it to the writer and step 3 justifies that — the seal deletes
   buffer rows. But only step 3 writes the buffer, and it is explicitly garbage collection
   rather than correctness; the expensive step *reads*, which WAL permits alongside a writer.

   **A claim per operation is built** (§4a; this paragraph described the role-lease era), and the seal is the operation that uses it. `sealing`
   belongs to whoever holds the `seal` role and `compacting` to whoever holds `maintain`, so
   recovery replays only what it owns — which is the hazard above, resolved.

   Nothing configures where the sealer runs, because nothing in the library runs one.
   `seal_due()` drains the queue; `maintain()` compacts, evicts and expires and then
   calls it. Both are plain methods on their caller's schedule, and
   if another owner holds the lease the call is refused and returns rather than
   duplicating the work.

   An earlier design had `seal_mode` ("background" | "inline" | "none") and `seal_poll`,
   with `extend()` starting a daemon thread. That existed only because sealing used to sit
   on the append path — "where does this expensive thing run" was a real question. Once the
   cut moved into the append transaction, sealing became draining, which is what
   `maintain()` already was, and the asymmetry had no defence: there was never a
   `maintain_mode` or a `maintain_poll`. Removing it also removed a library that started
   threads behind its caller, which is how two of §13.6's bugs stayed hidden.

   **An explicit `seal()` records its cut unconditionally.** Cutting only when the queue
   was empty made the method's effect depend on how far behind a sealer was: the caller's
   rows went uncut, an older group was sealed instead, and the call could return None
   having sealed nothing. Eight appends and eight seals produced seven files, one holding
   two appends' worth of rows — the same calls, a different data layout, decided by a
   race. The lease decides who *writes* a file, never where it is cut, so `seal()` now
   reports the cut regardless of who writes it and `await_seal()` is what blocks until
   the table has moved.

   **Two roles, but not necessarily two processes.** `seal` and `maintain` are separately
   leased so they *can* be split, and the shipped shape does not split them: one writer,
   and one storage process holding both. They are the same kind of work — off the hot path,
   committing to the same Iceberg table, neither latency-critical the way an append is — so
   sharing a GIL between them costs nothing that matters, while separating them costs
   something real. `_table_lock` serialises a seal's commit against a maintenance pass
   *within* a process and nothing does across processes, so two storage processes race on
   Iceberg's `write.metadata.delete-after-commit` cleanup and each warns about metadata the
   other already removed. Splitting them is then a deployment decision needing no code
   change, worth making only once compaction delays seals enough to matter — and a delayed
   seal costs latency rather than file size, because the cut was recorded when the rows
   arrived.

   ### An async API, not built

   `fsync` cannot run on an event loop, so an `async` caller reaches this library through
   `asyncio.to_thread` — which is already supported and tested (see the concurrency
   contract in `docs/RUNTIME.md`): a pool hands out a different thread each call, and
   nothing here demands thread affinity.

   What is *not* built is the API that would make that ergonomic. `await log.append(...)`,
   `await log.seal_due()`, and above all `await log.await_seal()` — whose name already
   describes an awaitable and whose current implementation is a sleep-poll loop that an
   event loop would rather own. A capture feed arriving over a websocket is asyncio by
   construction, so the wrapper is worth having.

   Deliberately deferred rather than forgotten. It is a surface decision — sync core with
   an async facade, or async all the way down — and it should be made when the API has
   users to be broken, not stacked onto the change that made the core coherent.

   **A lease statement must be its own transaction.** The buffer connection is shared and
   every write on it takes `Buffer._lock` around an explicit `BEGIN IMMEDIATE`, so a lease
   statement issued *without* that lock lands inside whatever transaction happens to be
   open — an append's — and commits or rolls back with it. A rolled-back append then took
   the lease row with it, leaving its holder believing it held a role the table no longer
   recorded. Observed as two sealers writing the same file, with pyiceberg refusing the
   second: `Cannot add files that are already referenced by table`. `Lease` therefore
   carries the lock as well as the connection, and holding it is what guarantees the
   connection is in autocommit so that one statement is one transaction.

   **The lease is the only exclusion mechanism**, threads included. It works for both
   because an owner is a UUID minted per acquisition rather than per `Log`, so two threads
   calling `seal()` are two owners and the second loses on the same row that would refuse
   another process. An owner fixed per `Log` would be re-entered by every thread sharing it,
   and an owner derived from the pid or thread ident would be worse than useless: both are
   reused after their holder exits, so a new arrival could inherit a dead one's identity and
   re-enter a lease it never took.

   The remaining options, none chosen:

   - **Leave it in-process and seal on a background thread.** Avoids every open item above —
     no leases, no recovery-ownership question, no signalling — because there is still one
     process. But *moving* the seal to a thread wins nothing on its own: it would take the
     same lock for the same duration, relocating the stall from the triggering append to
     every append during the seal.

     It works only if the seal stops holding the write lock for its expensive part, and §4's
     steps divide cleanly for that. Step 1 claims the range and step 3 deletes the sealed
     rows — both brief writes. Step 2 is all of the cost and **reads only**, so it can run on
     a second SQLite connection while appends continue. Measured: a 19,999-row scan on one
     connection took 22.6 ms while 21 appends completed on another at 0.64 ms median, with no
     lock contention.

     So: a second connection, the lock held only for steps 1 and 3, and a guard against two
     seals in flight. **Built, and it is necessary but not sufficient.** Measured with it in
     place: the lock is held 3.1 ms of a 48 ms seal, exactly as intended — and an appending
     thread still stalls, because the lock was never the whole problem.

     **The rest is the GIL.** A seal is CPU-bound in pure Python — 80% of its commit is
     pyiceberg deep-copying `TableMetadata` (§13.7) — so the sealing thread starves the
     appending one whether or not it holds a lock. Demonstrated by moving nothing but
     `sys.setswitchinterval`: at Python's 5 ms default the worst append was 45.2 ms, at 0.1 ms
     it was 6.4 ms, against a no-seal control of 5.7 ms. Contention spreads rather than
     concentrates, so a background seal can show a *worse* p99 than an inline one while
     improving the maximum.

     A library has no business setting a process-wide switch interval, so the lever is the CPU
     cost itself. §13.7 removes the deep copy; running the seal in a separate process would too,
     which is one more thing the multi-process question above is worth to this one. **The
     ordering matters: this option is gated on §13.7, not independent of it.**
   - **A lease per role**, writer and maintainer, each recovering its own intents. Simple to
     state, but the split is coarser than what is actually exclusive, and it forces sealing
     to sit on whichever side owns the buffer.
   - **A lease per resource** — buffer writes, the seal, the compaction — matching the intent
     tables that already exist (I16). Finer and it composes, at the cost of more moving parts
     in the single-process case that remains the default topology.
   - **Move sealing to maintenance entirely**, handing step 3 back to whoever holds the buffer
     write lease.

   **What the built background seal does and does not port.** Its durable half is already
   process-agnostic: `sealing` records the intent before the work and recovery replays it
   without caring which process wrote it (I16). Its runtime half is entirely Python-local, so
   it is thread-portable and not process-portable, and the gap is four named things:

   | | lives in | breaks across processes as |
   |---|---|---|
   | the one-seal-at-a-time guard | a Python bool | both processes seal; `claim_seal` does DELETE-then-INSERT unconditionally, so the second overwrites the first's claim rather than being refused |
   | the wake-up and completion signals | `threading.Event` | no cross-process equivalent |
   | the buffer and table locks | `RLock` | no mutual exclusion; SQLite serialises individual writes but not multi-statement transactions |
   | the buffered-size counter | an in-memory integer | the seal trigger's only input, and it neither exists in another process nor decrements when one seals |

   Each maps onto an option already listed: the guard wants a lease, the signals want a queue
   table or a watermark, and the counter is the sub-question below. Nothing about the durable
   protocol needs revisiting — only who decides to seal, and how they are told.

   Open sub-questions the last option raises, and probably the reason to be careful:

   - **What triggers a seal?** §4 says the writer evaluates it at commit time, and that is
     free because the writer already knows the buffer grew. A maintainer would poll. The
     `max_age` branch would not have cared, being time-based, but it no longer exists.
   - ~~**The size counter is per-process.**~~ **Resolved by `extent`.** The running
     total lives in the open queue row and is written in the same transaction as the rows
     it accounts for, so any process reads it with a keyed read of one row — and there is
     no second, in-memory copy to disagree with it.
   - **Deferring step 3 widens a window that is currently narrow.** It is safe by §7's
     boundary at any width, and it *should* be free: the boundary already excludes sealed
     rows, so a row awaiting deletion is one the read has no reason to touch. Persistence,
     query planning and cleanup are separable concerns and this is where they separate.

     They do not separate today. Measured with 1,000 unsealed rows behind a boundary:
     15.4 ms with 20,000 sealed rows deleted, 29.9 ms with the same rows still present,
     48.5 ms at 60,000 — so the cost tracks what the buffer *holds*, not what the read
     *returns*. DuckDB's sqlite scanner does not turn `litelink_offset > hi` into a rowid
     range; SQLite given the same predicate answers with
     `SEARCH buffer USING INTEGER PRIMARY KEY (rowid>?)` in 1.0 ms against 17.1 ms attached.

     **Fixed by pushing the predicate down**, and then by removing DuckDB from the buffer
     leg entirely. The predicate still goes to SQLite — `SEARCH buffer USING INTEGER
     PRIMARY KEY (rowid>?)` — but through the library's own connection, because
     `sqlite_query('buf', …)` turned out to be unsafe at any speed.

   ### Two SQLite libraries in one process

   `ATTACH '<buffer.db>' (TYPE sqlite)` corrupted the buffer. DuckDB's sqlite extension
   carries its OWN statically linked SQLite, so the file was managed by two independent
   SQLite libraries inside one process. Each keeps private, process-local state: a table of
   open descriptors (to work around POSIX advisory locks being per process and per inode,
   so that closing any descriptor drops all of that process's locks on the file) and the
   coordination for WAL's shared-memory index. Neither is shared between libraries, so the
   reader and the writer stopped being serialised against each other.

   Measured, on the ordinary shape of a scan concurrent with appends:

   | reader | result |
   |---|---|
   | same process, via `sqlite_query` | corrupt on the FIRST scan; `integrity_check` fails afterwards |
   | separate process, via `Log.open(read_only=True)` | 327 scans, clean |
   | same process, strictly sequential append→scan | 300 iterations, clean |

   The symptoms were `database disk image is malformed` and, when the torn mapping was the
   `-shm` index, `SIGBUS`. Cross-process is exactly the case WAL is designed for; two
   libraries inside one process is not, and no attach option, pragma or locking mode
   reconciles them.

   **The buffer leg is therefore read through the connection that already owns the file**
   and handed to DuckDB as Arrow, converted incrementally: rows are immutable once
   committed, arrive only above the last one, and leave only as a prefix at a seal, so a
   query converts its own delta and slices the rest zero-copy. That is also *faster* than
   the attached version, which re-read the whole buffer per query — 25.6 ms against
   46.0 ms with 20,000 sealed and 20,000 buffered rows and 200 rows appended between scans.

     `binary` columns remain unsupported until §15 lands, though no longer for this reason:
     the target workload is tabular JSON off a websocket, where the payload is text, and
     §15's design already has binary payloads **bypass** the buffer rather than travel
     through it.

   ### ~~What `max_age` needs to know, and how little that is~~ (removed)

   **Superseded: there is no `max_age`.** Kept because the reasoning about what the buffer
   may record — and why a library-stamped timestamp is not it — still applies to anything
   time-based that might be proposed later.

   A maintainer cannot evaluate `max_age` without knowing how old the unsealed data is, and
   the buffer records nothing temporal. The obvious move is a library-stamped timestamp
   column, and §2 refuses one at length: *"ingest time" is ambiguous in a way a library
   cannot resolve*, and stamping it relocates a load-bearing invariant out of the
   application. That objection is about a column applications **read**, though — its harm is
   a published meaning nobody agreed on. It does not obviously reach bookkeeping the library
   keeps for itself.

   §15.3 already settled the analogous case in that direction. The staged bit is
   *"deliberately **not** `{name}_size`, and not any column in the published schema"* —
   internal state stays in the buffer, because *"overloading a caller-facing column with
   internal state constrains it"*. A `litelink_ts` in the buffer table only, never in the
   Iceberg schema, sits on the same side of that line.

   **It was not needed at all, and this is now built.** §12 does not say what `max_age` is
   the age *of*, and the cheapest reading needs no per-row data: it bounds how long data may
   sit unsealed, so the quantity is the age of the **oldest unsealed row** — one value,
   written when the buffer goes from empty to non-empty, cleared at seal. O(1), no column
   anywhere, no §2 argument to have.

   That value is `extent.opened_at`, stamped by the **first row** to land in a group and
   null while the group is empty. A sealer closes an aged group on its own poll, which is
   what a quiet stream needs: until this existed `max_age` was dead config — a field that
   was validated, persisted and round-tripped through `open`, and that nothing ever read —
   so a low-rate stream never sealed at all and its rows stayed in SQLite indefinitely.

   Stamping the group's *creation* instead would seal a one-row file the moment an idle
   group finally received a row, which is the pathology §6 exists to clean up after.

   The per-row version buys exactly one thing over that: a **partial** seal, cutting at the
   last row older than `max_age` instead of sealing everything present. Sealing everything is
   not wrong — `max_age` is an upper bound on staleness and sealing early cannot violate it —
   and §4's step 1 already fixes `[start, end)` against the current maximum, so rows arriving
   during the seal simply land above it. So the per-row column buys precision the policy does
   not appear to need, at the cost of the §2 conversation. Worth confirming against a real
   workload before deciding, because it is a one-way door once data exists.

   ### Signalling the maintainer

   SQLite has no notification mechanism, so anything cross-process is polling; the only
   question is what gets polled. A queue table the writer pushes to is the general answer,
   and it is the shape I16 already uses — `sealing`, `compacting` and `pending_delete` are
   all coordination through tables. A watermark is the cheap answer: one `meta` value, read
   with the same query the age check needs anyway, no rows to insert on the write path and
   none to retire.

   The tradeoff is whether the maintainer needs to know *what* happened or only *that
   something is due*. Nothing identified so far needs the former, and adding an insert to the
   append path to deliver it would spend write latency — the thing this whole line of
   thinking is trying to reclaim.
7. **Seal cost grows, mostly with snapshots and secondarily with files.** A seal commits, and
   a commit's cost tracks what the table *metadata* holds — not the rows, and not only the
   files. Measured at one file per seal:

   ```
   snapshots retained     40 files /  40 snapshots     61.6 ms
                         240 files / 240 snapshots    247.6 ms
   snapshots expired      40 files /   1 snapshot      43.2 ms
                         240 files /   1 snapshot      86.3 ms
   ```

   **The larger factor is snapshot accumulation, and expiry arrests it** — which `maintain()`
   already does, bounded by `snapshot_retention`. An earlier version of this entry blamed file
   count alone, measured with `maintain()` never running so that every snapshot survived. That
   made the growth look both steeper and less fixable than it is.

   **The cause is not manifests**, which is the obvious suspect and worth ruling out. They cap
   at `commit.manifest.target-size-bytes` and split rather than growing — verified by lowering
   the target until the split was reachable, after which the largest manifest held at ~61 KB
   against a 64 KB target. `add_files`' duplicate check contributes about 18% (272 ms against
   224 ms at 240 files with it off).

   It is pyiceberg deep-copying the whole `TableMetadata` on every metadata update: 27 copies
   per commit, each descending the full model tree, which is 80% of a commit at 300 files.
   Metadata holds the snapshot list, the schemas and the metadata log —
   `write.metadata.previous-versions-max` already bounds the last, and expiry bounds the
   first. There is no supported way to switch the copying off from outside pyiceberg, so
   keeping the metadata small **is** the mitigation, and both levers for that are already
   config.

   **A file-count component remains**, and it is the honest residual: 43.2 ms to 86.3 ms as
   files went 40 to 240 with snapshots pinned at one. §6 selects files holding *under*
   `target_compact_size`, so a file compaction has already produced at or above that size is
   never revisited — compaction bounds how many *small* files exist and cannot reduce the
   total. Eviction is the only mechanism that removes a large file, and §8 makes
   `local_retention = None` the default, so a local-only capture that keeps its history still
   degrades. Less steeply than this entry first claimed, and for a reason now named.

   Worth measuring before choosing a fix, since the options differ in shape: raising
   `target_compact_size` over time so yesterday's output is tomorrow's input, tiered compaction, or
   the honest possibility that unbounded local retention is not a supported configuration.

   ### Registering files without pyiceberg's commit path

   The cost above is pyiceberg's, not Iceberg's, which makes a fourth option available: write
   the metadata directly. **The library already writes its own Parquet** — at a path claimed in
   `sealing` before the bytes exist (I2), fsynced before the commit (I1) — so pyiceberg is only
   doing the registration. That registration is a manifest entry, a manifest, a manifest list,
   a `metadata.json`, and a pointer swap, and every one of those is a documented file format
   plus a SQLite row update the library already knows how to make atomically (I16).

   The attraction is that appending an entry does not inherently require copying the whole
   table metadata twenty-seven times. The objection is §1's principle that *"everything Iceberg
   provides is used, not reimplemented"*, and the distinction that principle turns on is worth
   stating: using the **format** is the commitment, using the **library** is an implementation
   choice. Files that conform are still Iceberg. The real risk is drift — hand-written metadata
   that is subtly wrong still opens locally and breaks the external readers the archive exists
   for, and it breaks them later, in someone else's engine.

   Two cheaper things should be ruled out first, because both are small and neither risks the
   format:

   - **Commit less often.** A seal must make rows durable, which it does by writing and
     fsyncing Parquet; it does not have to register them in the same breath. Registering every
     Nth seal cuts commit count by N, and the rows stay readable throughout because §7 serves
     anything above the table's extent from the buffer — the same window step 3 already leaves
     open, just wider. What it costs is a larger buffer, which §7 measures as the variable cost
     of a read.
   - **Wait for the upstream fix.** The deep copy is not load-bearing; it is how
     `update_table_metadata` is written today.

   If those are not enough, hand-writing the commit is a bounded piece of work with one hard
   requirement: an external engine must read the result. That is a test before it is a design —
   attach something that is not pyiceberg and assert it sees what litelink says is there.

   **The archive has the same commit cost and it does not matter in the same way.** Its file
   count is unbounded by design — it is the full history — so registering into it rewrites
   manifests against a table that only grows. But that happens in §5, which is lazy,
   restartable and arbitrarily far behind, and no read depends on it. The same work that
   stalls an append when a seal does it is absorbed by a background pass when sync does. That
   is the argument for eviction as the bounding mechanism: it does not remove the cost, it
   moves it to the tier that can wait.

   One coupling survives the move, and it closes a loop worth watching. Eviction may not
   precede registration (I4), so if archive commits slow enough that sync falls behind,
   eviction stalls, the local file count grows, and local seals degrade — the write path
   feeling a cost that was supposed to have been moved off it. The remote table wants the same
   manifest-merge properties as the local one for that reason, and §5's throughput is worth a
   number rather than an assumption.
8. **Iceberg v3.** Tables are written at format-version 2 because pyiceberg will not write
   anything else — `NotImplementedError: Writing V3 is not yet supported`, at 0.11.1 and at
   0.12.0rc1, tracked upstream as apache/iceberg-python#1551. That is the whole of the current
   answer. Nothing about this design prefers v2.

   Three things in v3 would matter here, and the one that looks decisive is not.

   **Variant**, for semi-structured data, is the interesting one and the furthest away —
   pyiceberg has no `VariantType` at any version yet. The target workload is tabular JSON off a
   websocket, where today the choice is to parse every field into a column or keep the frame as
   text. Variant is the third option: store the frame, address into it, let the engine prune.

   **Nanosecond timestamps.** Temporal columns are refused today because their round trip
   through SQLite's storage classes is untested, and `timestamp[ns]` pyiceberg rejected
   outright. The examples carry epoch nanoseconds in an `int64` as a result — honest, and it
   loses the type. v3 has the types and pyiceberg already models them.

   **Default column values**, which would make §9's add-a-column less lossy: an older file
   could read a declared default rather than null.

   **Row lineage does not replace `litelink_offset`**, though it is the obvious candidate. v3
   gives each row a table-level `_row_id`, which answers §2's objection that Iceberg's sequence
   numbers are per *snapshot* rather than per row. It does not answer the other half. A
   `_row_id` is assigned when the row is committed to the table, and the tier boundary needs an
   identifier that exists while the row is still in the buffer — §7 filters the buffer leg on
   an offset the table has not seen. The library keeps owning that column under v3.

   **Deletion vectors** are irrelevant rather than useful: §1 has no updates and no deletes,
   and the only rows that leave do so as whole files leaving a snapshot.

---

## 14. Test plan

Beyond §10:

- **Block all network access; assert writes, seals, compaction and hot reads all succeed.**
  This is I5 and the central claim.
- Kill between Parquet write and Iceberg commit; assert recovery mints a NEW path, queues the abandoned one, and
  leaves no orphan.
- Kill between Iceberg commit and buffer delete; assert a read in that window returns each
  row exactly once (I3).
- With the archive deliberately overlapping the local window, assert a full three-tier read
  returns every row exactly once (I3).
- Evict the local table to empty; assert a full read still returns everything, from archive
  plus buffer alone.
- Seal until the buffer is empty, insert again, and assert the new offsets exceed every
  offset already committed to Iceberg (I9). This fails with a bare `INTEGER PRIMARY KEY`.
- Assert an unregistered file is never evicted locally, even past `local_retention` (I4).
- Expire snapshots during a long scan; assert the scan completes (I6).
- Add a column mid-stream; assert files written before and after union cleanly with nulls.
- Drop a column, re-add the name with a different type; assert both files stay readable and
  the columns do not merge (the retired ID's data is simply not projected).
- Rename a column; assert files written before and after read back under the new name with
  no rewrite, and that the operation is refused unless invoked explicitly (I10).
- Attach an external engine to the archive; assert it sees exactly the expected rows with no
  custom logic.
- Assert written files are sorted by `sort_by`, and that an `event_ts` predicate over a
  backfilling stream reads strictly fewer row groups than the same file written unsorted.
- Assert compaction output is re-sorted, not merely concatenated.
- Benchmark the hot read across buffer sizes; assert the buffer leg stays under a configured
  fraction of total read time at the chosen seal threshold.
- Create a stream whose schema has no timestamp column at all; assert seal, compaction, the
  three-way read and retention all work — proving nothing depends on an ingest column.
- Assert a caller-supplied `litelink_offset` is rejected (I11).
- Create a log with `start_offset=N`; assert the first append lands at N, that `[1, N-1]` is
  never assigned, and that the value survives a reopen. Assert it is ABSENT on a log created
  without one — a backfill must read absence as "no reserve", not as "a reserve of nothing".
- Assert the tail cache serves a seeded log before its first seal, by counting HITS rather
  than rows. Three broken variants return correct rows and pass every other test here: the
  guard keyed on the cache's first offset, one that pins the completeness floor instead of
  only raising it, and one that drops the lower bound. Assert the slice still prunes what the
  boundary excludes, and that a cache built for a high boundary refuses a lower one.
- Assert a row naming an undeclared column is rejected, and that the batch it was in is
  rejected whole with no offset consumed (I17). Include the row that misspells a declared
  column: it has the same width as a correct one, so a length check passes it.
- Assert a value of the wrong type is rejected at APPEND, for both outcomes: one Arrow
  cannot parse (which would wedge every scan) and one it would silently rewrite (`1.5` into
  an int64, `True` into an int64 — the case an `isinstance` check passes, `bool` being an
  `int` subclass). Assert a `str` subclass and an `int` in a float column are still accepted:
  the exact-type gate is a fast path, not the definition of legality.
- Assert a value of the right type but the wrong MAGNITUDE is rejected — `2**40` into an
  int32, `1e300` into a float32 — and that the exact bounds are accepted along with an
  explicit infinity, which a float32 represents exactly and which is a statement rather than
  an overflow.
- Assert a row omitting a non-nullable column is rejected, and so is one supplying it as
  `None` (I17) — while an absent NULLABLE column is still accepted, which is what stops the
  check from being a blanket "every key must be present". Falsify by allowing it and
  scanning: the failure is not scoped to the bad row, so assert the rows written BEFORE it
  become unreadable too.
- Add a column to a log with sealed files; assert old rows read null, new rows carry values,
  and not one file was rewritten. Assert a SEALER that never appends does not drop the new
  column — the process that would never revalidate if the schema were cached per append.
- Interrupt a schema change before its final SQLite write; assert the next open completes it,
  and that a reader opened during the window reads rather than raising. Assert an
  `add_column` whose archive is unreachable leaves the log openable, appendable and readable,
  and that the change completes on a later open (§11).
- Commit twice, then assert a reader that cached `metadata_location` from the first commit
  is detectably stale -- the reason §7 requires resolving it per query.
- Run with `local_retention = 0`; assert seal, upload, archive reads and compaction all work
  and that eviction still never precedes registration (I4).
- Restore a WAL replica taken before a seal; assert the read returns each row exactly once
  despite the restored buffer holding already-sealed rows.
- Property test: for `t1 < t2`, `read(t1) ⊆ read(t2)`, across a seal, a compaction and an
  expiry.

---

# 15. Blob fields

**Extension to capture storage v1.0.** Support for payloads too large to sit comfortably in
the SQLite buffer: sensor frames, point clouds, raw response bodies.

---

## 15.1 The model

**Bytes live beside SQLite while hot, and inside Iceberg once sealed.** The column is the
same shape in both tiers: a plain Iceberg `binary` column holding the bytes themselves.

There is no pointer in the schema. The hot-side staging path is derived from `litelink_offset` and
the field name, so it exists only on the write side and never reaches the table. The archive
is therefore ordinary Iceberg with a binary column, readable by any engine with no
convention to know about and no dereference step.

This is the one design decision worth stating explicitly, because the obvious alternative
looks cheaper and is not. A sidecar object store with a `(path, offset, length)` struct
column avoids inflating the Parquet files, but it puts a reference in the published schema,
makes the archive unreadable without library-specific logic, and creates a class of object
that Iceberg does not manage. Snapshot expiry, orphan cleanup and compaction all ignore
files that no manifest references, so blob lifetime becomes a refcount the library maintains
by hand across crashes. Inlining at seal removes that entire category: the bytes are inside a
data file the manifest already tracks, so they inherit retention, expiry and compaction for
free.

The cost is read amplification on queries that project the blob column, which is bounded and
tunable. See §15.5.

---

## 15.2 API

Declared at stream creation, alongside the application schema:

```python
litelink.Log.new(
    root, name,
    schema=pa.schema([...]),
    sort_by=("event_ts", "key"),
    blob_fields=[litelink.blob_field("payload", hash=True, size=True)],
)
```

`blob_field(name, ...)` declares:

| column | type | purpose |
|---|---|---|
| `{name}` | `binary` | the bytes |
| `{name}_size` | `int64` | optional. prunes as an ordinary statistic without a fetch |
| `{name}_hash` | `binary` | optional. xxh3-128, integrity check without a fetch |

Both siblings are optional and purely for the caller's benefit. Neither carries internal
state — see §15.3.

The siblings are ordinary columns, not metadata. They exist so a reader can filter or verify
without paying to materialize the payload, and they prune from Iceberg statistics like
anything else.

`append()` accepts bytes, a file path, or a file-like object for each blob field. Paths and
file objects are streamed rather than materialized, which matters at 100 MB point clouds.

The declaration is what makes this an extension rather than a convention: the library needs
to know which columns are blob-shaped so the write path routes around SQLite and the seal
knows what to materialize. Applications may of course declare an ordinary `binary` column
themselves for small payloads, and nothing here applies to it.

---

## 15.3 Write path

Amends §3. The buffer row never carries blob bytes.

```
1. BEGIN. INSERT the buffer row (no blob column); take `litelink_offset` from lastrowid.
2. Write bytes to {root}/staging/{offset}.{field}.bin
3. fsync the file AND the directory entry
4. Set {name}_staged = 1 on the row. COMMIT.
```

**Step 3 before step 4 is correctness, not durability hygiene (I12).** A committed row whose
staging file did not survive the crash is a row pointing at nothing, and it is unrecoverable
because the bytes were never anywhere else. Syncing the directory entry matters as much as
the file: on most filesystems the file can be durable while the name that reaches it is not.

The per-blob fsync is affordable precisely because blobs are large. At 2 MB the fixed cost
amortizes to nothing; this would be unacceptable at 2 KB, which is why small payloads should
stay in an ordinary `binary` column and go through the normal buffer path.

### Getting `litelink_offset` before the bytes are written

The staging path derives from `litelink_offset`, so the offset must exist before step 2 — but §2 pins
`INTEGER PRIMARY KEY AUTOINCREMENT`, which assigns at INSERT.

Both hold at once by keeping the transaction open: `INSERT` inside `BEGIN` yields
`lastrowid` immediately, and the row does not become visible until `COMMIT` in step 4. No
separate reservation table and no pre-allocated runs are needed.

**But AUTOINCREMENT reuses an offset after a rollback.** Verified: an `INSERT` that receives
offset *N*, then rolls back, is followed by an `INSERT` that also receives *N*. That is
harmless for I9, which governs *committed* offsets and therefore the tier boundaries — but
it is not harmless here, because step 2 has already put a file on disk at that name.

The failure it would cause: a crash after step 3 leaves `staging/N.payload.bin` with no
committed row. A later row takes offset *N* again with **no** blob, and a seal that inferred
"does this row have a blob?" from file existence would attach the stale bytes to it.

**So blob presence is recorded on the row, never inferred from the filesystem** — by
`{name}_staged`, a bit **in the buffer table only**. It is set in step 4, inside the same
commit that makes the row visible, so it is true exactly when the bytes are durable.

It is deliberately **not** `{name}_size`, and not any column in the published schema:

- The bit is meaningless after seal. Once the bytes are inline in Parquet there is nothing to
  indicate, so it has no business in a table other engines read.
- Overloading a caller-facing column with internal state constrains it. If `size=True` is
  declared, the caller reasonably expects to query it — and now its nulls carry two meanings
  (no blob / not yet staged) that have to be disentangled forever.
- `{name}_size` is optional, so it would have to be silently forced on to serve as the
  sentinel, which is the sort of surprise that shows up as a schema diff nobody asked for.

A staging file whose row has `{name}_staged = 0`, or no row at all, is an orphan by
definition, and the §15.4 sweep removes it.

Staging is local, buffer-scoped, and never uploaded.

---

## 15.4 Seal

Amends §4. One additional step, between choosing the range and writing Parquet:

```
1. SQLite txn: choose [start, end), write it to `sealing`
2. Read buffer rows; for each row with {name}_staged = 1, read staging/{offset}.{field}.bin
3. Write Parquet locally with bytes inline; commit to the local Iceberg table
4. SQLite txn: delete buffer rows < end; clear `sealing`
5. Delete staging files for offsets < end
```

**Step 5 is garbage collection, not correctness**, exactly like step 4. A staging file whose
offset is below the local table's committed max offset is unreferenced by construction,
because the bytes are now in a data file. The sweep is therefore idempotent and can run at
any time, including on startup: delete every staging file whose offset is below the boundary
that §7 already computes.

That derivation is why no orphan window needs a time heuristic. A crash anywhere leaves
staging files that are either still referenced (offset above the boundary, keep) or already
materialized or orphaned (below it, sweep). There is no third state.

**Sorting now moves bytes.** §4 sorts rows by `sort_by` before writing, which with inline
blobs shuffles the payloads rather than a few scalar columns. Sort on a key column and
permute, rather than sorting materialized rows, or the seal cost becomes proportional to
total payload size.

---

## 15.5 Parquet write settings

The defaults are wrong for this shape and the failure is silent, so these are requirements,
not tuning.

**Row groups must be sized by bytes — which the library must compute.** pyarrow exposes
`row_group_size` in **rows** and has no byte-based parameter; the default is unset, falling
back to roughly a million rows. At 10 MB blobs that is a ten-terabyte row group, meaning
every projection of the blob column reads the entire file. Derive a row count from the sizes of the
staged files themselves — which the library always knows, whether or not `{name}_size` was
declared — so a row group holds roughly 10 to 50 blobs. The row group is also the
unit a reader materializes in memory, so it doubles as the per-reader allocation bound.

**Use `large_binary`, or cap row groups well below the limit.** Arrow's `binary` uses 32-bit
offsets, so a column chunk caps at 2 GB. Twenty 100 MB blobs overflow it. Verify how
pyiceberg maps Iceberg `binary` on the way in rather than assuming.

**Set `compression=NONE` on blob columns.** Sensor payloads and media are already compressed;
the codec will spend CPU proving it.

**Raise `target_seal_size`.** The §12 default is a handful of rows once blobs are inline. Size it
so a file still holds a useful number of rows. Note this pulls against §7's finding that the
seal threshold bounds hot-read latency — with blobs, the buffer holds fewer, larger rows, so
the row-count guidance there still governs.

**Confirm the page index is written.** It is what would allow ranged reads below row group
granularity later. Reader support is uneven enough not to design around today, but writing it
costs nothing and keeps the option.

---

## 15.6 Read behaviour

Unchanged in shape. The hot read joins staging by derivation, the sealed read reads the
column directly, and both return the same schema, so callers see one thing.

Degradation is confined to queries that project the blob column:

- **Projections excluding it cost nothing.** Column chunks are contiguous per row group, so a
  query over `event_ts` and `key` never fetches payload bytes. Analytical predicates and the
  tier-boundary reads in §7 are unaffected.
- **Projections including it read whole row groups.** A single-row fetch amplifies to the row
  group size. This is the tunable in §15.5 and the reason to size row groups in blobs rather
  than rows.
- **`{name}_size` and `{name}_hash` are the escape hatch.** Filtering and integrity checking
  work without touching the payload column at all.

A dedicated `read_blob(row)` accessor is worth having so the common single-blob fetch can
push a tight row filter rather than materializing a scan.

### The reads, in full

Both §7 reads with a blob field resolved. Expressible entirely in DuckDB, using `sqlite` to
attach the buffer, `iceberg` to scan the tables, and `read_blob` for staging.

`lo` and `hi` are §7's names: the min and max of `litelink_offset` over the local table's current
snapshot, read from manifest column statistics. The hot read needs only `hi`, which is the
value §7's hot-read prose calls `boundary`; it is the same number and this section uses `hi`
throughout so one value carries one name.

**The column is named `litelink_offset`, not `offset`.** Two reasons, and the second is the
one that forced it. `offset` is a plausible application column — a byte offset, a page
offset, an offset from a reference time — and reserving it taxes every caller. More
decisively, **`offset` is a reserved word in DuckDB**: `SELECT offset FROM t` and
`max(offset)` are both parser errors, so every query the library wrote and every query a
reader wrote against the archive would have to quote it, forever, with the failure being a
syntax error rather than anything that says why. Verified in both directions —
`max(litelink_offset)` parses unquoted, `max(offset)` does not.

The prefix also namespaces it. §9 lets an application add columns freely, and a collision
with the one column the library owns would be a schema change that cannot be made.

Staging resolution is identical in both reads, so it is factored out once:

```sql
INSTALL sqlite; INSTALL iceberg;
ATTACH 'buffer.db' AS buf (TYPE sqlite, READ_ONLY);

CREATE OR REPLACE TEMP VIEW staging AS
  SELECT
    regexp_extract(filename, '(\d+)\.payload\.bin$', 1)::BIGINT AS "offset",
    content AS payload
  FROM read_blob('staging/*.payload.bin');
```

**Hot read** (local table plus buffer):

```sql
SELECT "offset", event_ts, key, payload
FROM iceberg_scan('<local metadata json>')
WHERE <predicates>

UNION ALL

SELECT b."offset", b.event_ts, b.key, s.payload
FROM buf.buffer b
LEFT JOIN staging s USING ("offset")
WHERE b."offset" > $hi AND <predicates>;
```

**Full-stream read** (archive plus local plus buffer). The archive holds no staging tier, so
its blob column is read directly:

```sql
SELECT "offset", event_ts, key, payload
FROM iceberg_scan('<archive metadata json>')
WHERE "offset" < $lo AND <predicates>

UNION ALL

SELECT "offset", event_ts, key, payload
FROM iceberg_scan('<local metadata json>')
WHERE <predicates>

UNION ALL

SELECT b."offset", b.event_ts, b.key, s.payload
FROM buf.buffer b
LEFT JOIN staging s USING ("offset")
WHERE b."offset" > $hi AND <predicates>;
```

The blob field changes nothing about the tier boundaries. Both queries bound each tier by its
neighbour's committed extent exactly as §7 specifies, and the staging join sits entirely
inside the buffer branch.

**Explicit column lists, not `SELECT *`.** The buffer branch sources `payload` from a
different relation than the other branches, so the union cannot be built positionally. This
is the only structural difference from §7's pseudocode.

**The staging join is a glob and a parse, not a per-row lookup.** `read_blob` returns
`filename`, `content`, `size` and `last_modified`, so the directory is globbed once and
joined on the offset parsed out of the name. Table functions cannot take correlated
arguments, so a per-row path could not be passed in even if the pointer were stored; deriving
the path from `litelink_offset` and joining is the only shape available, and it happens to be the
faster one.

`LEFT JOIN` rather than inner: a blob field may be null for some rows, and an inner join
would silently drop them.

**`$lo` and `$hi` are supplied, not computed in SQL.** Expressing either as a scalar subquery
over `iceberg_scan` would read the offset column rather than manifest statistics, which is the
opposite of the §7 design. Resolve them through pyiceberg and pass them as parameters.

**The scan takes a metadata path, not a catalog, and this is deliberate.** DuckDB's `iceberg`
extension attaches REST catalogs only; a pyiceberg `SqlCatalog` on SQLite cannot be attached,
and the path-based `iceberg_scan` is the catalog-free read route. (Verified: `ATTACH … (TYPE
ICEBERG)` against a SQLite catalog fails demanding OAuth2 credentials.) Note this is a
different extension from `ducklake`, whose SQLite catalog support is unrelated.

That constraint happens to coincide with what correctness requires. If DuckDB resolved the
catalog itself, it would select its own snapshot independently of the one `lo` and `hi` were
computed from. A seal committing between the two leaves `hi` stale-low against a newer scan,
so every row in the gap appears in both the Iceberg branch and the buffer branch. Pinning the
metadata file makes the boundary and the scan come from one snapshot by construction, which
is the same argument as deriving boundaries from committed extents rather than flags (I3).

So the library resolves the current metadata location through pyiceberg, reads the extents
from that same metadata, and passes both into the query. Worth testing whether
`iceberg_column_stats()` can supply the extents from manifest statistics against the pinned
path, which would keep the whole read in one DuckDB call without weakening the pin.

Cast the buffer side explicitly rather than relying on the `UNION ALL` to reconcile types.
SQLite's per-value typing comes through the `sqlite` extension loosely, and a column that
holds integers in every row but was declared without affinity can still surprise the union.

With multiple blob fields, each gets its own staging view and its own `LEFT JOIN`, keyed on
the field name in the glob pattern.

---

## 15.7 Retention and orphans

**No new object-storage garbage collection.** Blob bytes are inside Iceberg data files, so
snapshot expiry, compaction and orphan cleanup handle them with no additional machinery. This
is the whole payoff of inlining and it should not be given up casually.

**Staging is the only library-managed storage**, and it is local, offset-named, and swept
against the same boundary the read path already computes. Nothing accumulates in object
storage that Iceberg does not know about.

Compaction (§6) needs no change in principle, since re-sorting an offset range carries the
bytes along. It does change in cost: compaction now reads blob bytes rather than only Parquet
metadata and small columns. Streams with blob fields should therefore get a separate, lower
`target_compact_size`, or the pass will move far more data than the file-count problem
justifies.

---

## 15.8 Amendments to existing sections

| Section | Change |
|---|---|
| §2 Layout | Buffer holds no blob bytes. Adds an internal `{name}_staged` bit per blob field, in the buffer table only — never in the Iceberg schema. |
| §3 Write path | Adds the staging write and the fsync ordering (§15.3). |
| §4 Seal | Adds materialization (step 2), the staging sweep (step 5), and sort-by-key-then-permute. |
| §6 Compaction | Unchanged in logic; needs a separate size bound for blob streams. |
| §7 Read path | Unchanged in shape. Hot reads resolve staging by derivation; `litelink_offset` must be quoted. |
| §8 Retention | Unchanged. Blobs inherit it. |
| §12 Configuration | Adds `blob_row_group_blobs`, `blob_compact_size`; `target_seal_size` raised. |

---

## 15.9 Invariants

Extends §10.

| # | Invariant | Why |
|---|---|---|
| **I12** | The staging file and its directory entry are fsynced before the buffer row commits. | A committed row whose bytes did not survive is unrecoverable. The bytes exist nowhere else. |
| **I13** | A staging file is deleted only when its offset is below the local table's committed max offset. | Above the boundary it is still the only copy. |
| **I14** | Blob presence is read from `{name}_staged` in the buffer, never inferred from a staging file existing, and never from a column in the published schema. | AUTOINCREMENT reuses an offset after a rollback, so a stale staging file can sit at an offset a later row legitimately takes. Keeping the bit internal also stops a caller-facing column carrying two meanings for null. |
| **I15** | Blob bytes are never written to any object storage location that no Iceberg manifest references. | The moment they are, lifetime becomes a hand-maintained refcount and expiry stops working. |

I15 is a design constraint rather than a runtime check, and it is the one to revisit
deliberately if the sidecar approach ever becomes necessary.

---

## 15.10 Failure modes

Extends §11.

| Failure | Outcome |
|---|---|
| Crash between staging write and buffer commit | Orphaned staging file at an offset with no committed row. Swept by the §15.4 rule; never mis-attached, because presence comes from `{name}_staged` (I14). |
| Crash between buffer commit and seal | Staging file survives; the row is durable and readable. This is the case I12 protects. |
| Staging file missing for a row with `{name}_staged = 1` | Unrecoverable data loss for that row. Detectable at seal; fail loudly rather than writing a null. |
| Crash mid-seal after Parquet commit | Staging files below the boundary are now redundant; recovery sweeps them. |
| Local disk fills | Blobs dominate the bound, so §13's byte-based backpressure must count staging, not only buffer rows. |

---

## 15.11 Tests

- Kill between staging write and buffer commit; assert the orphan is swept and no row
  references a missing file.
- **Force an offset reuse**: roll back an insert that staged bytes, then insert a blob-less
  row that takes the same offset; assert the stale bytes are never attached (I14).
- Kill between buffer commit and seal; assert the row reads back with correct bytes.
- Delete a staging file behind a row with `{name}_staged = 1`; assert seal fails loudly
  rather than writing a null or a short value.
- Declare a blob field with `size=False`; assert the published schema contains no
  `{name}_size`, that the staging bit appears nowhere in the Iceberg table, and that seal
  still resolves blobs correctly.
- Assert a projection excluding the blob column reads no blob bytes. Measure, do not assume.
- Assert a single-blob fetch reads at most one row group's worth.
- Write blobs totalling over 2 GB in one row group's worth of rows; assert no offset overflow.
- Assert `{name}_size` and `{name}_hash` prune and verify without materializing the payload.
- Compact a blob-bearing offset range; assert byte-for-byte identity of every payload,
  alongside the existing row count and bounds checks.
- Assert an external engine reads the archive's blob column with no library-specific logic.
- Run a stream with a blob field declared but never populated; assert nulls throughout and no
  staging files.

---

## 15.12 Open

**Container swap.** Iceberg has been working toward a pluggable file-format API, with Lance
discussed as a candidate container. **Both the release version and the timeline here are
unverified — confirm against upstream before relying on them.** If and when it reaches
pyiceberg, the data file container swaps and blob reads gain true random access. The column
is already `binary`, so no schema change is implied and no data has to move; only newly
written files change format. Nothing in this section should be designed around it arriving.

**Small-blob threshold.** Everything here assumes payloads large enough to amortize a
per-blob fsync. Below roughly 1 MB the staging round trip is pure overhead and an ordinary
`binary` column through the normal buffer path is better. Whether the library should route
this automatically by size, or require the application to choose, is unresolved. Automatic
routing is friendlier and introduces a size-dependent write path, which is a durability
behaviour that changes under the caller without warning.

**Ranged reads.** Page-index-driven fetches below row group granularity would cut the fetch
amplification substantially. Blocked on reader support, not on anything here.
