# Capture storage

**v1.0** — durable append-only capture into Iceberg tables. Embedded and local-first.

---

## 1. Architecture

```
SQLite buffer          durable on commit. unsealed rows only.
      │                WAL, synchronous=FULL, one db per stream
      │  seal at target_seal_size
      ▼
staging table          a rolling WINDOW of recent data.
      │                SqlCatalog on SQLite, file:// warehouse
      │  publish: upload data files, register into the published table
      ▼
published table        the full HISTORY. S3, or a local directory.
```

**The published table is a superset of the staging window, not a disjoint half of it.** Everything
published is in the published table, including data the staging table still holds — that overlap is what
makes losing the machine survivable. The staging table is a read accelerator over the recent
range, not a separate shard.

A data file is written locally, uploaded, then registered in the published table; local eviction
later shrinks the window without touching the published table.

**No hot-path read touches the network.** A hot read is the staging table plus the
SQLite buffer, both on local disk. Since v0.1.0 the platform wheels carry DuckDB's `iceberg`,
`avro` and `httpfs` extensions, so an installed package holds unconditionally; a CHECKOUT
still downloads them on first use, which is what `just bootstrap` discharges. See §7.

**Everything Iceberg provides is used, not reimplemented** — manifests, per-file column
statistics, schema with field IDs, atomic snapshot commits. The catalog is a SQLite file,
not a service, so this costs no daemon.

### Scope

| Not doing | Why |
|---|---|
| Time travel | Append-only. Snapshots are a commit mechanism here, not a query feature. Point-in-time filtering is `ingest_ts <= as_of` on a column — bitemporal, and strictly more expressive. |
| CDC | No updates, no deletes. A state change is a new row with a new `ingest_ts`. Publish is a watermark. |
| Multi-writer per table | One writer per stream. Multiple machines write separate tables; readers union. |
| A transaction / commit ID column | The library's commit boundary is batching, not meaning — whether 50 rows landed in one transaction or five is an implementation detail. See below. |

---

## 2. Layout

**One directory per stream, holding everything that stream owns.**

```
<root>/<name>/
    buffer.db            unsealed rows, offsets, claims, queues  (§2, §4a)
    catalog.db           the staging table's Iceberg catalog     (one stream)
    published.db         the published table's Iceberg catalog   (one stream;
                         archive.db on a log from before 0.6)
    litestream.yml       the sidecar's config, if replicating    (§3a)
    data/                sealed Parquet                           (§4)
        compacted/       merged by compaction, and rewrite_published's
                         re-cuts                                 (§6)
        ingested/        written by bulk ingest, never buffered   (§13.4)
    metadata/            Iceberg metadata JSON and Avro
    published/           the published prefix, when not on S3    (§5)
```

and in the published prefix — `s3://…`, or `<root>/<name>/published` by default —
mirroring it:

```
<prefix>/<name>/
    _wal/                LTX segments, one directory per database  (§3a; S3 only)
        buffer.db/  catalog.db/  published.db/
    data/                the same Parquet, same relative path
    metadata/            Iceberg metadata, plus version-hint.text  (§3b)
```

A stream is therefore a subtree that can be copied, replicated, restored or deleted whole,
in either tier: a data file keeps its root-relative name in the published table, so its identity is
its offset range in both tiers and neither has to translate the other's paths.

**Before 0.2 it was not so, and what that cost is worth recording.** `catalog.db` and
`archive.db` sat at `<root>` and were shared by every stream under it, while Iceberg
metadata went to pyiceberg's default `<warehouse>/<namespace>/<table>` — so a table's
metadata lived at `<root>/litelink/<name>/metadata` while its own data files were at
`<root>/<name>/data`, outside the location the table claimed, held together only by the
absolute paths in its manifests. Deleting a table's directory would have left every Parquet
file behind.

The sharing bought nothing: every query against those catalogs is keyed by
`(catalog, namespace, table)` and nothing has ever read across streams. It cost three
things. Replication had to be one sidecar per *root*, because a sidecar per log would have
run two litestream instances against one `catalog.db` — which litestream forbids — so a
root with several streams needed a config written by hand. `follow` (since removed, §3b) had to
drop its `root` parameter, since a caller-supplied directory could collide with a live log's
shared catalogs. And one corrupt catalog took every stream under the root with it.

Contention was *not* among the costs, which is worth stating because it is the first thing
assumed: two streams sealing concurrently against one shared `catalog.db` measured a
57.3 ms median against 66.1 ms for separate roots, over 16 seals. The Iceberg commit is a
tiny transaction; a seal's seconds go to the Parquet and Avro writes, which were always
per-stream.

Logs written before 0.2 keep working only after being moved — `open` detects the old tree
and names the command rather than reporting an absent log. The migration itself,
`python -m litelink.migrate`, shipped through 0.5.1 and is gone from later releases (#93): run
it with 0.5.1. It rewrites metadata pointers in place and preserves snapshot ids and commit
times, because §8 derives a file's age from the snapshot that added it. Data files are
neither moved nor rewritten: they were always at `<root>/<name>/data`.

A root holding several streams moves one stream at a time, and four things stay shared until
the last of them has: `catalog.db`, `archive.db`, the root `litestream.yml` — which names
every stream's buffer — and `<prefix>/_wal`. Each is kept while any stream still resolves
through it, and the old sidecar keeps running, because an unmigrated stream's layout has not
changed. Dropping the old replica is a separate pass per stream, refused while any stream in
the root is outstanding and until something has reached `<prefix>/<name>/_wal`: that replica
holds the only off-box copy of unsealed rows, which are by definition in no Parquet file and
no published manifest.

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

-- and, each in the section that needs it:
--   extent, extent_intent, compacting, pending_delete, claim   (§4a, §6)
--   tier_offsets, tier_statistics                               (§7)
```

That is the entire hand-written catalog. Row counts, per-column min/max and visibility live
in Iceberg; `tier_statistics` keeps rollups of them for routing reads (§7).

**NOTE — `litelink_offset` must never be reused.** Iceberg's sequence numbers are per *snapshot*,
not per row, and do not exist for buffer rows, so they cannot serve as the tier boundary.
The writer assigns `litelink_offset`; Iceberg then computes its min/max as ordinary column
statistics, which is what §7 reads.

A bare `INTEGER PRIMARY KEY` is a rowid alias assigned as `max(rowid)+1` — and buffer rows
are **deleted once something off-box holds them** — at the seal, or by `publish` with
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
plus every buffer row from the table's `end` up — so the union yields the whole batch either way, before or
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
litelink.new(
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
staging   = SqlCatalog("staging",   uri=f"sqlite:///{root}/{name}/catalog.db",   warehouse=f"file://{root}")
published = SqlCatalog("published", uri=f"sqlite:///{root}/{name}/published.db", warehouse=published_prefix)
```

Both tables are created with an **explicit location** — `<root>/<name>` and
`<prefix>/<name>` — rather than the `<warehouse>/<namespace>/<table>` pyiceberg would
otherwise derive. The namespace survives as the table's name inside its catalog; it is no
longer part of any path. That separation is what let the layout move without rewriting a
single catalog row's identity.

A log from before 0.6 keeps the catalog names it stored, `local` and `archive`, and its
`archive.db`: the catalogs are replicated and found by name, so they are read as they are
rather than renamed.

The published catalog's SQLite file is replicated with the others (§3a), but no other machine
attaches it: the published table names its own metadata through `version-hint.text` (§3b).
A REST catalog is a drop-in replacement once more than one machine needs to write.

---

**Offsets are named for what they are, everywhere.** A range is `[start, end)`, with `end`
excluded — in the code (`DataFile.start`/`.end`, `span()`, claims), in every SQLite table
(`start_offset`/`end_offset`), and in the API. A single inclusive offset is a `through`: the
last one held, as in the `published_through` watermark. `lo`/`hi` read as two values a range
contains, which is the inclusive convention litelink left in 0.6, and are not used for
ranges.

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
SQLite WAL frames to object storage, and litelink never starts the CONTINUOUS one — that is a
separate process reading the WAL, which is exactly why replication does not put the network in
the write path. What the library owns is `LocalReadHandle.databases`: which files carry the
log's state and therefore have to be replicated.

**"never starts it" was too broad, and the exception is the recovery path.** `restore` shells
out to the binary itself, on a public code path — a one-shot batch call that runs and exits,
not a supervised daemon. So litestream is a runtime dependency of that path rather than purely
an operator's concern, which is why the platform wheels ship it: leaving it to `PATH` puts the
discovery of a missing binary inside a failover. A `litelink replicate` that daemonised would
be the real crossing, and there is deliberately none.

**`wal_replication` is a declaration, not a supervisor.** This paragraph used to say the
opposite — that replication is not configured in `LogConfig`, because a boolean claiming a
sidecar was running would be a setting nothing reads. The flag exists now, and it is read:
`evict("buffer")` consults it to decide whether rows stay in SQLite until the published table
has them, and `validate` refuses it without a remote (`s3://`) published
table to ship to and refuses `wal_retention` without it. What it still does not do is assert
that a sidecar is running, which the library cannot know. So it states an intent the deployment has to honour, and
stating it falsely costs the growth without buying the durability that growth was traded
for: the buffer holds its rows, nothing ships them, and `evict("buffer")` releases them only
once `publish` has reached the range.

**The sidecar needs a monotonic clock that does not run backwards.** litestream reports a
measured interval through a Prometheus counter, which panics on a negative value, so one
regression of `CLOCK_MONOTONIC` crash-loops it — while logging successful syncs right up to
each panic. On a slow stream that is a durability failure, because the buffer holds the only
copy of unsealed rows. The known trigger is a VM on the `tsc` clocksource; see `docs/API.md`
under Replication, and `python -m litelink` warns on the combination.

`wal_retention` is the other half, and the sidecar enforces it rather than litelink.
`replication_config` emits it as a per-database `snapshot:` block, deriving `interval` as
half the retention so a window can never hold zero snapshots — a restore needs a snapshot at
or before the point it is restoring to.

**All three databases, not just the buffer.** `buffer.db` holds rows no Parquet file has
yet, `catalog.db` says which files the staging table is made of, and `published.db` says the
same for the published table.

That last justification used to be "omit it and the objects in S3 survive with nothing to
say what they are". It is no longer true — the published table publishes `version-hint.text` and can
name its own metadata — and the file is now replicated for the *same-machine* case, where
it saves a round trip, while a failover deliberately does NOT restore it: a stale copy wins
over the bucket's own pointer and reads the published table short. See §3a and `litelink.restore`.

**One sidecar per stream.** All three databases live in the stream's own directory (§2) and
replicate to `<prefix>/<name>/_wal`, so a root holding several streams runs one sidecar per
stream, each with a config `write_replication_config` generates into
`<root>/<name>/litestream.yml`.

This is what the per-stream layout bought. Until 0.2 two of the three sat at the root and
were shared, so a sidecar per log would have run two litestream instances against one
`catalog.db` — which litestream forbids — and shipped them to one replica path. A root with
several logs needed a single config naming every buffer under it, *written by hand*, and the
advice was to keep one log per root to avoid the question.

Optional. Three things to be clear about:

**It covers append→publish, and it used to cover only append→seal.** The difference is a
hole this paragraph once described as a lag. A seal moves rows out of SQLite and into a
Parquet file no sidecar replicates, so deleting them at seal removed the only off-box copy
of a range the published table did not hold yet — and the machine dying in that window lost them
from the MIDDLE of the offset space: below the seal frontier so the buffer no longer had
them, above the published frontier so the bucket did not either.

**So a seal never deletes its rows; `evict("buffer")` does** (#122), and with `wal_replication`
on it waits until the published table holds the range. It is I4 one tier up: never drop data
until the next durable copy has it. Reads are unaffected — the buffer leg is bounded by the
staging table's committed extent (§7), so held rows never reach the engine, and a scan that
ends below that extent skips the buffer entirely — and the cost is that `buffer.db` grows with
publish lag, which a stalled publish makes unbounded, like a stalled eviction (§11).

The gate is `wal_replication`, not "a published table is configured": with no sidecar the buffer
and the Parquet share a disk and die together, so the next durable copy is staging, and
`evict("buffer")` drops rows as soon as a seal has committed them there.

```
RPO = WAL replication lag        (with wal_replication)
RPO = max(WAL replication lag, Parquet upload lag)   (before this; the hole)
```

**Where a row can be, and which of those places is off the machine.** The
offset space has two holes in it, and they are different problems:

```
  [0 .......... published]  safe — in the published table
                [published ...... sealed]  hole A — local Parquet only
                                  [sealed ... replicated]  safe — in buffer.db
                                            [replicated ... assigned]  hole B
```

**Hole A is closed.** It was the band a seal moved out of SQLite into a Parquet
file no sidecar replicates, above what the published table had taken — so it was on the
dead machine and nowhere else. Holding those rows until `publish` pushes the range
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
the log fails outright. See §3a's failover notes and `litelink.restore`.

## 3b. Reading a log from another machine

**litelink reads on the primary.** Every handle is built from a root on the machine that holds
the log — the writer, or a `LocalReadHandle` beside it — and reads the buffer, the staging table
and, when a query could match it, the published table from there (§7). Off that host, the
published table is the interface: an ordinary Iceberg table that publishes `version-hint.text` at every commit, so any engine
pointed at `<published>/<name>` resolves its current metadata with no catalog service and no
litelink install (API.md, "Reading from another machine").

What such a reader cannot see is anything newer than the last `publish`: rows in the buffer or
the staging table are on the primary alone. `litelink_offset` makes polling safe — it is
monotonic and never reused, so a reader keeps the highest one it has seen and asks for what
came after.

There used to be a litelink reader for this, `snapshot` (`follow` before 0.3), which returned a
`RemoteReadHandle`: a published-only view, or with `include_wal=True` a litestream restore of the
writer's `buffer.db` merged with the published table. It was removed (#90). It rebuilt, on every read
host, a restore and an adopted catalog that an Iceberg engine does not need, and it carried a
class of states the primary never meets — a buffer missing rows a seal discarded, pinned
metadata swept by later commits, gaps no replica could explain — each with its own refusal.
Freshness past the last `publish` is the primary's to serve; a WAL replica is for failover (§3a).

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
2. Write Parquet locally; commit it to the staging table.
3. SQLite txn: delete buffer rows < end; clear `sealing`.
```

**Rows are sorted before writing**, by the configured `sort_by` (§12). This is declared as
the table's Iceberg `sort_order` *and* actually applied at write time — the metadata records
intent, it does not sort for you.

**"Sorted" and "contiguous" are claims about two different columns**, and reading them as
one claim is what made §13.4's design look harder than it is. A file is sorted WITHIN itself
by `sort_by`; the set of files is ordered by `litelink_offset`, which nothing sorts because
nothing has to — the offsets are assigned in a contiguous block before the sort permutes the
rows inside it. So a file's offset range stays dense while its rows move, and neither
property constrains the other. That is what lets bulk ingest sort per output file rather
than across a corpus that may not fit in memory.

**Declared in three places, and each answers a different reader.** The staging table's
`sort_order` and the PUBLISHED table's say what the data is clustered by, to anything reading either
Iceberg table directly; the published table's went undeclared until failover needed it, which made
a published table holding clustered data silently say nothing about it. `meta` carries it too, and
that is the copy `open` reads — because the local catalog cannot be restored onto another
machine, so a failover rebuilds the staging table and has to be told what to declare. An empty
`sort_by` is a value meaning unsorted, and clears all three; the published table's declaration is
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

**Recovery.** On startup, if `sealing` holds a row: if the staging table already contains that
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
| compact ∥ evict | eviction stays below every in-flight merge's `start` |
| compact ∥ compact | each claims a distinct run and skips runs already claimed |
| compact ∥ publish | a merge must not span into what publish is publishing, and vice versa |
| publish ∥ publish | both claim from the published floor, so they exclude each other; `register` declines a covered range besides |
| publish ∥ seal, evict | publish claims only `[floor, end of what it pushes)` (#118): seals land above it, eviction below the floor |
| evict ∥ expire | both are metadata commits; CAS orders them and both are idempotent |
| drain ∥ anything | no claim (#118): a queued name can never become referenced again, so the veto and the grace period suffice |

`compact ∥ publish` is the one that is a correctness matter rather than wasted work.
Compaction skips files at or below the published watermark, so a merged file cannot span into
the published range — unless the watermark advances *while* it merges, which makes its
inputs, chosen against the older watermark, include files publish has since published. Pushing
that merged file adds a range partially overlapping one the published table holds:
`register`'s check declines only a range that is **entirely** covered, so a partial overlap
is admitted and the published table returns duplicate rows.

### The claim, and why it needs an expiry

Range-disjointness answers "may these two run together". It cannot answer the question
recovery has to ask: **is this in-flight record live work, or did that process die?**
Nothing derived from the data answers that; only a deadline does.

So every operation that owns a range writes a claim before its file exists (I2), and the
claim carries an owner and an expiry:

```
claims(id, owner, expires_at, kind, start_offset, end_offset, rel_path)
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

Two ranges are still coarser than the design wants: `publish` and the configuration changes
claim the whole log. For a configuration change that is correct — a re-point is not an
operation on an interval. For `publish` it is conservative: it cannot name the range it will
push until it has read the published table's extent, so narrowing it to `(floor, last]` is the
remaining refinement, and until then `compact ∥ publish` still serialises.

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

**An outer claim's renewal is combined with the run's, never substituted for it.** A rewrite
under the whole-log lease (`rewrite_published`, `rewrite_sorted`) passes that lease's `renew`
down; `renew or claim.renew` silently stopped renewing the run claim at all and answered the
pre-commit check with the outer one. Callers no longer pass a callback of their own: each pass
renews its own claim, which is why the public routines dropped `heartbeat`.

**And the published table refuses a range that starts inside its extent.** Everything upstream is
arranged so a merge never straddles it, and each gap found in that arrangement has been a
fresh piece of reasoning — a crash between a register and the row recording it, then a
compaction-config change before the next publish backfills, is one that survived several
reviews. `_covers` declines only a range ENTIRELY covered, so a straddling one is admitted
and those offsets sit in two files at once, in the immutable tier, with nothing able to
repair it. Refusing costs a stall; admitting costs silence.

**That stall has no remedy today, and this paragraph used to imply one.** Nothing re-cuts a
LOCAL straddler — `rewrite_published` works the other side — so the log stops advancing its
watermark, eviction pins below the straddling file, and the shipped publish role dies on the
`ValueError` because it catches `RuntimeError` and `CommitFailedException` only. The refusal
is still the right trade against silent permanent duplication in the immutable tier, but
"recoverable" was not true.

Reaching it needs a crash between a register and the `extent` rows recording it, and then a
compaction-target change before the next publish backfills those rows from the published table's
manifest. The window exists because the rows and the manifest are two records of one fact and
only `publish` reconciles them, while compaction decides from the rows alone. What would close
it, in increasing order of work: reconcile the rows against the manifest on the compaction
side too; or record a pushed range BEFORE the register and confirm it after, so the two
readers can take opposite polarities — compaction is safe when coverage is OVERSTATED,
eviction only when it is UNDERSTATED, which is why the pre-segment design kept two records
and read one from each; or ship a tool that re-cuts a local straddler, which would also make
the sentence above true.

The test is `start < span_end`, with no lower bound. A lower bound was there first and was a
hole rather than a safety condition: it exempted exactly the range that starts BELOW the
extent and runs past it, engulfing the whole thing — every published offset in two files,
which is the worst version of this rather than an excused one.

**`drain` takes no claim, because nothing can make a queued file live again** (#118). What a
claim on the unlink would guard is a queued name becoming referenced between the veto and the
delete. `hydrate` did exactly that, deliberately, and is gone. Everything else that adds a file
to a table adds one with a fresh per-attempt token (a seal, a compaction, an ingest, a published
rewrite). Seal recovery commits a claimed name only if it never landed and otherwise writes a
fresh one, and compaction recovery registers nothing. `publish` registers copies of staging
files above the published span, and `register` declines a covered range, so a range a rewrite
superseded is never pushed again. An entry that is due and unreferenced therefore stays
unreferenced, and two drains overlapping only unlink one file twice.

**`publish` claims only the range it pushes** (#118): `[floor, end)`, from the published
table's frontier to the end of what it uploads, so a seal or eviction in another process is not
refused for the length of an upload. It still excludes compaction over those files (including
the trailing run `flush` takes), another publish (both claim from the same floor), and the
whole-log operations that re-point or re-cut the published table. Two cases take the whole log
instead: a publish that must write the tier row, since that exact rollup must not race
eviction widening it, and a published table only a repairing open can reach.

**And everything a pass reads to decide a deletion is read under its claim, not before it.**
`publish` learned this for itself and eviction did not, though it acts on the same facts: it
read the published location, and the policy, before claiming anything. `set_published` is
documented as something the shipped writer calls on every restart and it takes the whole
log, which is free precisely while eviction holds nothing — so attaching a published table between
the read and the acquire left eviction deleting the only copy of every aged row that published table
was configured to receive, unrecoverably, since publish cannot push what has left the table.
Eviction therefore claims on the UNCLAMPED retention boundary, which only ever falls, and
recomputes everything under the claim. `set_config` gets the same treatment for the same
reason: it writes durable state that no running process would otherwise hear about, and
§8's retention reads as an obligation rather than a hint.

That refresh also forced a correction worth stating on its own: the policy now has ONE
owner. `WriteHandle` used to keep a copy beside `Maintenance`'s, with the buffer's seal target as a
third, kept in step by `set_config` writing all three. Two copies is one too many the moment
anything else can change the policy — refreshing one would leave compaction reading the new
grouping while `publish` read the old, and `runs` is shared by exactly those two so that they
cannot disagree about which files are still in play. And the refresh happens in
compaction and in `publish` as well as in eviction, because the shipped topology runs those two
as SEPARATE PROCESSES: refreshing in one place only keeps them in step within a process,
while across processes one restarting after a durable `set_config` and the other not would
leave compaction grouping under a policy `publish` had never heard of, permanently. And
compaction re-reads the published premise under each RUN claim, not only at pass start: a publish
that ran in between, under a grouping that settles a partial prefix of a run, leaves the
rest of that run merging into a local file straddling the published table's extent — and nothing
re-cuts a local straddler, so every later push is refused and the watermark never moves
again.

**`validate` checks a PAIR, so both halves are read durably.** `wal_replication` with a
local published table is refused in one call — the replica exists to get rows off the machine — and
each setter was checking its own new half against this process's memory of the other, so
two processes could assemble the refused pair between them. (The pair this was found on, an
evict-on-upload policy with no published table, no longer exists: every log has a published table, #98.)

Reading both halves durably is still not enough, and this is the part that took two rounds
to see: read and write as two transactions with nothing between them, and the check is only
a statement about the past. Each setter could pass against a state the other was about to
change, so between them the two calls assembled the very pair neither would accept — and the
next maintenance pass carried it out. **`set_config` and `set_published` therefore take the
same claim**, which is §4a's own rule about data, applied to the configuration that governs
it: the check and the act share a transaction, or they are not a guard. `litelink.new` records
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

**Compaction asks whether ANY published table holds a file, not the configured one.** Detaching does
not make the copies stop existing. Merging across a range some published table holds makes a LOCAL
file whose boundaries line up with nothing there, and nothing re-cuts a local straddler —
`rewrite_published` works the other side — so re-attaching stalls the log for good: eviction
pins below it and every push is refused. Four legitimate operations reach it, with no warning
at any step: detach, raise the target, maintain, re-attach. Skipping those files costs
nothing, because only compacted files are ever pushed, so one with a published copy is already
at the target. Eviction still asks about the CONFIGURED published table, because I4 is a promise about
where the copy is.

**And `publish` applies the same exclusion, or the two deadlock.** `stable_prefix` holds a file
back when compaction might still merge it; compaction refuses to merge anything a published table
holds. Those are one rule, and `runs` is shared between them precisely so they cannot
disagree — giving compaction a second input `stable_prefix` could not see was enough to
break it. After a re-point to a fresh prefix the floor is 0, so files the old published table covers
return to `pending`, group into a mergeable run under a raised target, and are held back for
ever against a merge that will never happen: nothing is pushed, the watermark never moves,
eviction pins on it, and nothing raises. A file no merge can touch is settled by definition.

Note what that correction cost the earlier justification: "a file with a published copy is
already at the target" is false the moment the target is RAISED after the copy was made —
which is the scenario the exclusion exists for. Such a file stays at the size it was
published at, and `rewrite_published` is the tool for that.

**Opening the published table with `repair` needs the durable location, not a remembered one.** That
open may drop a catalog entry naming another prefix and create a fresh table at this one;
the claim is what entitles a caller to do it, and the location the log records is what says
WHICH published table to do it to. `publish` re-read the location for exactly this reason, and every
other repairing caller inherited the privilege without the premise — so a handle still
remembering a published table the log had left destroyed the live published table's catalog entry, after
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
resurrected range is real rows at their real offsets, and `_union` bounds the published leg by
the staging table's lower extent — so a staging table that regains `[1, 100]` bounds the published
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
        └── stored locally, or in the published table, or both
```

The fourth step is not a tier. A compacted file in the published table is the **same file in a
second place**, and litelink already records that per file: `publish` calls `record_file` with
the published table's URI, so `extent` holds a row per pushed file naming exactly where its copy
went. `published_through` is a global summary of facts that are already durable per segment.

That summary is the single most expensive line in this design, because it is **the only
boundary in the system that can move backwards.** Offsets are immutable, seal cuts only
advance, compaction only merges forward — and then a re-point resets the published watermark
to zero. Every reader that cached the old position is wrong at once, and there is no
ordering of the writes that fixes it, because the problem is not the write ordering; it is
that a per-segment fact was compressed into one mutable number and then had to be
un-compressed by inference.

**So do not compress it.** I4 is asked of a file, not of a watermark:

> A local file may be dropped only if `extent` holds a row for the same offset range whose
> `rel_path` names a copy in the published table this log is configured for.

An equality check on a recorded value, and the consequences fall out:

- **Nothing resets.** A re-point changes where the NEXT file goes. Files already pushed keep
  naming the bucket that holds them, so no boundary moves backwards and no cached position
  becomes wrong.
- **Identity stops being inferred.** "Is this mine?" is a comparison against a URI the log
  wrote down, not an inference from a prefix, a catalog row keyed by table id, or a
  process's memory of its own configuration.
- **Several published tables coexist.** Old ranges name the old bucket and new ranges the new one,
  which is half of what makes re-attaching to a published table that already holds data
  expressible. The other half is the published table naming its own current metadata: `SqlCatalog`
  keeps that pointer in the catalog, so the local `published.db` row was the only thing that
  had it, and a re-point drops that row. Each commit now writes `version-hint.text` beside
  the metadata, and `open_published` registers from it instead of creating an empty table.
- **The compaction frontier goes.** `published_pending` exists to stop a merge straddling a
  range the published table may hold; per segment, compaction skips a file that records a published
  copy and needs no frontier, no crash window between writing it and using it, and no
  reconciliation to retire it.

`published_through` may remain as a derived `MAX(...)` for the push floor and for display.
What it may not be again is the thing that authorises a deletion.

**There is no local-only exception.** Every log has a published table (#98) — a local directory when
no remote one is given — so I4 is never vacuous: a local-only log's eviction clamps against
its local published table's records exactly as an S3 log's does.

It is tempting to require that a file be compacted before eviction will take it, since only
compacted files are ever pushed. **Resist it.** The reason to hold a file back is
never its compaction state; it is that a merge — `_rewrite_run`, reading a run of files to
write the one that replaces them — holds it as an input right now. That is a claim, and it
is already answered above.

A merge is the pair that matters here because it is the only pass that can put rows *back*:
select `[1, 100]`, have eviction commit their removal, then commit the replacement, and the
range returns. Every other pass holding a file open merely fails when it vanishes — publish's
upload errors and retries, expire is an idempotent metadata commit. Compaction state is a
proxy for this and a bad one, since it is neither necessary (an uncompacted file no merge
has claimed is safe to drop) nor sufficient (a compacted file can be an input to the next
merge up). Requiring "compacted" instead reintroduces the trailing-run holdback
this section rejects below — bounded in bytes, at most one compaction target, but unbounded
in time, so a log that goes idle keeps its last target-sized residue for ever. Whether that
is acceptable depends on whether `staging_retention` is a disk heuristic or an obligation to
delete; §8 currently reads as the latter, which would make it a defect rather than a lag.

So eviction, in both configurations, is one rule: **drop what retention no longer wants,
except what a live claim covers, and except — where a published table is configured — what has no
recorded copy in it.**

**Implemented.** `published_pending` and the frontier are gone; `published_prefix` walks the
local files and stops at the first without a recorded copy, and both compaction and eviction
ask it. Compaction skipping published files is not optional here — it is what keeps a local
range and its published range the same range, so the per-segment test can match them at all.

One window survives the change and closes differently. The row naming a file's published copy
is written after the register, so a crash between the two leaves the published table holding a range
nothing local records. Nothing is promised beforehand to cover it — that is what made the
watermark inexact in both directions — so the next push backfills from the published table's own
manifest, which it reads anyway.

`published_through` remains as a derived cache, for the push floor and for display. It no
longer authorises a deletion, which was the whole of the problem.

**Coverage, not equality.** The two tiers cut the same rows into files independently, so
asking whether a local range EQUALS a published one was wrong the moment they could differ.
`rewrite_published` re-cuts the published table to different boundaries by design — that is its entire
job — and under an equality test every local file then matched nothing, for ever: eviction
clamped to zero and stopped, and compaction stopped treating published files as the published table's
business and merged across its extent, which `register` admits as a partial overlap and the
published table keeps as duplicate rows. Neither heals, because nothing re-cuts the published table back.
The question I4 actually asks is whether the published table holds the ROWS, so adjacent published
files join and a gap ends the answer.

### One copy of a fact, in the log

Nine review rounds of this design found the same defect nine times, each in a place the
previous round had not looked: **a fact with a durable home in `meta` also had a mutable
copy in the process, and a decision read the copy.** A pass reading "no published table" from memory
while the log had one. A repairing open pointed at a bucket the log had left, destroying the
live published table's catalog entry. A fence comparing a value against itself, because a re-point
moved both sides of the comparison together. Two setters each validating against a stale
half of a pair. Compaction and `publish` grouping runs under different policies.

The tell was not the defects but their remedy: twelve `refresh` calls, whose only work was
dragging a copy back into agreement with the log. **A design whose correctness needs N of
those is always one short somewhere, because nothing tells you what N is.**

So the copies are gone. `Published` stores no location and reads `meta`; `Buffer` owns the one
`LogConfig` and everything that decides from the policy reads it there. All twelve refresh
calls, and the methods behind them, deleted. A stale location or a stale policy is not a bug
guarded against here — it is not a thing that can exist.

What makes it affordable is that the reads are cheap and the expensive parts are cached
**keyed on the durable value**: the parsed config on the raw JSON, the pyiceberg handle on
the URI it was opened for. A key that comes from the log is what stops a cache becoming the
next stale copy — when the log changes, the key changes and the cache retires itself. That
is also, exactly, the published-handle bug of the ninth round, gone by construction rather
than by a rule.

Measured: a `meta` read is 1.8 us against a 5.4 ms query, and an interleaved A/B of the
append path — the only hot consumer — showed −0.3%, which is noise. Two earlier measurements
of the same change showed 28% and 7% regressions; both were artifacts, one of a cold start
and one of drift between blocks. Interleaving the runs is what settled it.

**A decision reads the policy ONCE.** That is the hazard this trades for, and it is a real
one: each read is now independent, so two of them inside a single decision can disagree. It
bit immediately — `staging_rows` seen as an int by the guard and as None by the subtraction
after it is `int - None`, a TypeError out of `advance()`, which the shipped maintainer does
not catch, so maintenance stopped entirely. The rule is not a lock; it is that every
decision binds the policy to a local first: fresh per decision, coherent within it.

Where a torn read would merely produce an odd file size it is harmless, because the policy
is a POLICY — it decides how big to cut and when to merge, never which rows go where. The
one place it could have been an invariant is `runs`, shared by compaction and `publish` so the
two cannot disagree about what is in play, and per-segment I4 closes that: a file the
published table holds is never merged again.

What this does NOT cover: a pyiceberg table handle is a point-in-time snapshot of REMOTE
state, with no local durable copy to derive from, so `reload()` before deciding remains a
discipline. That is one method and a much smaller surface.

### Rejected: one settled watermark for both

An earlier version of this section had a single `settled_through` that compaction worked
above and eviction at or below. It does not survive, though not for the reason first
recorded here. The original argument was that a quiet stream never settles and so never
evicts, "removing anything at all" — an overstatement twice over: the holdback is the
trailing run, which is bounded by the compaction target, and with a published table configured that
stall is already the behaviour, since a stream that never settles never pushes and I4 pins
eviction regardless. It does not distinguish the design it was rejecting.

The real reason is that the number is wrong in both directions at once. With a published table,
`published <= settled` always, so eviction at or below `settled_through` would delete files
that settled but were never pushed — an I4 violation, and the binding constraint is
`published_through` anyway, which is never the larger of the two. Without a published table, nothing
else clamps, so the holdback becomes the only constraint and applies where it has no reason
to: "settled enough to publish" needs the trailing run held back because compaction may
still merge it and pushing a file about to be replaced is waste, while "safe to evict" is a
question about age, not about what may still change. One number cannot mean both, because
the two consumers are asking about different directions in time.

## 5. Publish

Independent, lazy, restartable, arbitrarily far behind. No read depends on it.

```
1. Upload the settled staging files not yet in the published table — those compaction
   has finished with (`stable_prefix`), or every one with flush.
2. published.add_files([...published paths...])  -- register, ONE commit; no data movement
3. Record each file's published copy in `extent`, and the watermark in `meta`.
4. (reclaim("published"), its own routine) Expire the published table's snapshots older than
   published_snapshot_retention, delete what that frees once due; then sweep stranded
   metadata (§6).
```

**Compactions are never replicated.** A file is pushed only once compaction is done with it,
and compaction refuses to merge anything the published table holds (§4a), so the two tables
never need the same overwrite applied twice. Re-cutting what is already published is
`rewrite_published`'s job, run on purpose.

Publish records how far it has registered in `meta`, as one offset under `published_through`
(`archive_through` on a log from before 0.6, which a writer's `open` renames).

**Step 4 is its own routine, `reclaim("published")`, not part of `publish`.** `advance()` runs it
right after `publish` (§12), and an orchestrator can run it on its own schedule. Until #113 the
published table was expired only after `rewrite_published`, on the reasoning that `publish`
never supersedes a file. True of data files and not of Iceberg's own: a table that was only
ever published kept every snapshot, manifest list and manifest it had ever had.

**The staging table's expiry and eviction are not publishing work either.** They are their own
routines, run by `advance()` after `publish` (§12).
Eviction reads what step 3 records to enforce I4: **a file is never evicted locally before step 2 has registered
it** — the one ordering in publishing that is correctness, not optimisation.

---

## 6. Compaction

Runs on the happy path, and has real work to do there. Not because seals come out
undersized — every file a seal writes is already the size it was asked to be, since the cut
is exact and there is no timer to cut early — but because the seal size and the file size
are two different targets (§12). `target_compact_size` defaults to eight times
`target_seal_size`, so eight sealed files become one, and that conversion is on **whether
or not the published table is remote**: file count is a read cost locally too, measured at 1.0 ms to read the
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

**Sizing is in uncompressed bytes, never in file size on disk.** The unit is the Arrow table's
`nbytes` — what a reader pays to hold the rows in memory, since every read hands them back as
Arrow. `target_compact_size` bounds what a file HOLDS in that unit: the appender's estimate
for the rows that went into it, which models the Arrow layout and stays at or a little above
`nbytes` (#84), or `nbytes` itself for a file `ingest` wrote. That number is carried per file
from the seal that measured it, added up across a merge, and dropped when the file is
unlinked. It cannot be recovered from the file afterwards: on data compressing
8:1 a file holding a full target is an eighth of it on disk, so a rule reading sizes off disk
merges eight already-full files into one holding eight times the memory the target allows —
and, since `publish` refuses anything compaction may still rewrite, publishes nothing at all in the
meantime. A file whose size was never recorded counts as full, so an unmeasured file is never
rewritten on a guess.

```
1. Select adjacent files holding < target_compact_size in total, spanning [start, end);
   require compact_min_files.
2. Scan them into one Arrow table; re-sort by `sort_by`.
3. Verify row count and per-column min/max against the sources.
4. staging.overwrite(table, overwrite_filter=(offset >= start) & (offset < end))  -- one snapshot
```

Only the staging table is compacted. The output reaches the published table later, as an
ordinary file `publish` pushes (§5).

Using the offset range as the filter is what makes this safe without partitions: the
predicate selects exactly the source files and nothing else, because no other file overlaps
that range.

Step 4 is atomic — Iceberg swaps the snapshot pointer — so readers never observe a gap or a
double count. No grace window is needed for *correctness*.

Snapshot expiry still needs one: expiring the pre-compaction snapshot deletes files a
long-running scan may still hold open. Each table retains snapshots for its own setting:
`staging_snapshot_retention` (default 15 minutes) for this log's own scans, and
`published_snapshot_retention` (default 1 hour) for readers on other machines, holding a
metadata pointer this process cannot see. Both are bounded. A log's offsets are its
point-in-time reads, so no snapshot is kept for time travel.

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

**One sweep exists anyway, as a backstop for files no commit recorded.** An Iceberg commit
writes its manifests, manifest list and `metadata.json`, then swaps the catalog pointer. One
that loses the swap, or crashes before it, leaves those files under names nothing recorded;
pyiceberg, unlike Java Iceberg, does not delete a losing attempt's files. So `sweep()` — run by
`advance()` after each table's last change — lists `metadata/` at their first pass
in a process and every four hours after, and delete what is unreferenced, not a
`metadata.json` the table still names, not queued, and older than the table's retention (an
hour at least, so a commit in flight keeps its files). The listing comes first and the live
set is read after it, so a commit landing between them counts as live; and the sweep refuses
outright unless the listing names the current metadata exactly as the table does. It needs no
claim, because a metadata file's name carries a fresh UUID and nothing ever references a dead
one again. On a healthy table it finds nothing; it deletes at most 500 files a pass, and
never raises into the pass that runs it. `retire()` runs it once more over both tables, all
at once and in parallel, before the log is marked retired: a retired log takes no more passes.

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
than their table's snapshot retention — staging rows by `advance`, the published table's by
`publish` — checking each against the live references, unlinking, and only then
forgetting the row: a crash between the unlink and the forget retries a no-op, whereas the
reverse order loses the path with the file still on disk.

Store when a file was superseded, not a precomputed deadline. The grace period is the owning
table's snapshot retention, and freezing it at enqueue time means a lowered setting never applies to
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

**The PUBLISHED table is the exception, and deliberately so.** Every published commit writes that hint
beside its metadata (§5), which is what makes a re-point reversible and lets an engine with
no catalog read the prefix directly. The staging table publishes none: its catalog sits in the
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

This is a provisioning obligation for a CHECKOUT, discharged by `just bootstrap` and by CI.
An installed wheel discharges it differently: the platform wheels vendor the extensions, which
is §13.5 and is closed. The obligation remains for anyone building from the sdist or running
on a platform with no published wheel, and `python -m litelink` is what reports it.

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
SELECT * FROM <staging table>        WHERE <predicates>
UNION ALL
SELECT * FROM buffer                 WHERE offset >= :boundary AND <predicates>
```

**The boundary is derived from the Iceberg table, not from a flag:**
`boundary = max(offset) + 1` over the staging table's current snapshot — its `end` — read from
manifest column statistics.

This is self-consistent at every instant, which is why the seal needs no `sealed` column:

- **before** the Iceberg commit — the boundary is the previous end, so in-flight rows are
  still served from the buffer;
- **after** the commit, before the buffer delete — the boundary has advanced past them, so
  the buffer contribution excludes exactly the rows the table now holds.

Neither window double-counts or drops.

### Cost, measured

1.02M rows (16 files x 64k) in the staging table, 400-byte payloads, against a SQLite buffer
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

1.02M rows in the staging table, 1,000 in the buffer, results fully materialised:

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

**Decided per query, from per-tier statistics (#90).** Every query reads the buffer. The
staging leg and the published leg are added only when their tier's statistics could hold a row the
query matches. Neither source is on the network, so I5 holds for any query bounded inside the
staging window:

- **Staging table:** the rollup of the snapshot the read resolved, from the staging table's own Iceberg
  manifests. The process that commits a new version stores its rollup in `buffer.db` after
  the commit, stamped with the version (its metadata file) and written forward only, so every
  other process reads it rather than rolling the same version up. A read uses a stored row
  only when its stamp is the version the read resolved; otherwise it rolls that version up
  itself (`LogTable.statistics_at`, cached per process by version). The stamp makes a late,
  missing or out-of-order store cost a rollup and never a wrong answer. Only the latest
  version is kept: a read misses it only in the milliseconds around a commit.
- **Published table:** a row in `buffer.db` describing what the published table holds **below**
  the staging table. The published leg reads only offsets under the staging table's `start`,
  and the published table's copy of the staging window would put its maximum timestamp at
  "minutes ago" and send every hot query to the network. Stored because its statistics
  otherwise live with the published table — on S3, when it is remote — and eviction, usually
  in another process, is the last moment they are on local disk.

**`litelink_offset` prunes every tier, the buffer included, by range.** It is the log's
sequence, dense and monotonic across the tiers, so each tier's `[start_offset, end_offset)`
says exactly where it sits — the staging table's from its snapshot's span, the published
table's from `tier_offsets`, kept apart from its statistics in `tier_statistics`. The buffer's
starts at its lowest offset and is open above, until `retire()` stores it closed and empty at
the log's end. It is decided before the buffer is read, from that one indexed `min`, which
only rises: rows arrive above it and leave as a prefix, so a query below it now matches
nothing the buffer can later hold. streamcast prunes whole logs the same way (streamcast#32).

The two rows are streamcast's per-log manifest format (streamcast#27) with `tier` as the key,
and the decision is the same function, `litelink.manifest.prune`: `col > K` can match when
`max(col) > K`, `col < K` when `min(col) < K`, `col = K` when K is within both, and anything
it cannot decide — an unknown bound, a column the tier lacks, an operator not on the list —
includes. A tier known to hold no rows is excluded. The query's WHERE becomes those terms
through DuckDB's own parser: only a single SELECT over `log` alone is narrowed, and only its
AND-ed comparisons between a column and a literal. A subquery or join over `log` would see
the pruned relation, so those shapes read every tier. Each literal is converted the way DuckDB
compares it against that column — to FLOAT for a float32 column, to DOUBLE for a float64 one,
kept exact for an integer column — because the pruner compares in Python and must agree with
the engine.

**The published row changes when the local floor moves, not when the published table does.** Eviction
widens it by the rows it moves, from their local manifest statistics, before its commit — so a
read resolving the new, higher floor finds the row already covering what went below it. `publish`
adds only copies of rows the staging table still holds and `rewrite_published` re-cuts rows the
published table has, so neither touches it. The one write that narrows is an exact rollup from the published table's manifests, run only
under the whole-log maintenance claim, which eviction cannot hold beside: at the first `publish`,
on a re-point (which drops the row first), at `restore`, and at `open` for a log written before
the row existed. With no row, the published table is read.

This reverses 0.4.0, which fixed a handle's tiers at assembly (`include_archive`,
`with_archive()`) so that a read would not start touching the network because eviction ran.
That rule bought predictability at the price of the caller naming a tier and a handle
without the published table answering short. Now a query's latency follows its predicates: a bounded
hot query stays local, and an unbounded one reads history because the whole log is the right
answer to it.

The published table overlaps the staging window, so the tiers cannot simply be unioned. Bound each by
its neighbour's **actual extent**, read at query time:

```
start = min(offset) in the staging table's current snapshot
end   = max(offset) + 1, likewise

SELECT * FROM <published table> WHERE offset <  start AND <predicates>
UNION ALL
SELECT * FROM <staging table>                         WHERE <predicates>
UNION ALL
SELECT * FROM buffer            WHERE offset >= end   AND <predicates>
```

Correct at every instant regardless of transient overlap, because `litelink_offset` is monotonic and
the staging window is a contiguous range over it. This is the §7 hot-read boundary
generalised, and it is why **no atomic handoff between the two catalogs is required** —
which matters, since two Iceberg commits cannot be made atomic with each other.

Both `start` and `end` come from manifest column statistics; neither requires opening a data
file. If the staging table is empty (everything evicted), it drops out and the read becomes
published table plus buffer bounded by the published table's `end`. A read with `published=False`
gets the buffer alone: rows only the published table holds are left out, not refused.

### Historical read

Query the published table directly. Ordinary Iceberg — any engine, no custom logic, no
knowledge of the staging tier.

### Who reads what

- The **writing machine** reads through litelink, which resolves both tables from their local
  catalogs and decides per query which tiers to read (§3b).
- **Other machines and engines** read the published table directly, through its
  `version-hint.text`, with no catalog. Whether a file happens to sit on some machine's disk is
  not modelled and not published; the published table describes what is in its prefix.

---

## 8. Retention

| knob | governs | too low means |
|---|---|---|
| `staging_retention` | how much history the staging table keeps | hot reads reach the published table |
| `staging_snapshot_retention` | how long the staging table's expired snapshots survive | long local scans hit deleted files |
| `published_snapshot_retention` | how long the published table's expired snapshots survive | remote readers hit deleted files |

`staging_retention` must exceed the longest hot-path lookback **with margin** — equal leaves
nothing for seal delay.

**`staging_retention = 0` is valid**: files are evicted from the staging table as soon as they
are registered in the published table, and the staging table holds only what has not yet been
uploaded. Hot reads are then limited to the buffer, and anything older goes to the published table
over the network. That is the right setting for pure archival capture — litelink as a
durable staging area into Iceberg — and the wrong one wherever a hot reader looks back
further than the buffer holds. It does not weaken I4: eviction still never precedes registration.

**Retention is never deletion** (#98). It used to be, on a log with no published table: I4 was
vacuous there, so `staging_retention` became a retention policy over the only copy. That made
DETACHING a published table a silent conversion into it — the clamp retired for every process at
once, and a maintainer the operator never invoked deleted 4,025 acknowledged offsets of
8,000 — and it left a local-only log with no output table for compaction and sizing to
shape.

Now every log has a published table: on S3 when one is given, otherwise a local directory under the
log's own. Eviction drops only what it holds, `set_published(None)` re-points to the local
default rather than detaching, and `staging_retention = 0` means "evict on publish" on every
log. The cost moves to the published table: a local one keeps everything until truncation by offset
or age lands, which is a follow-up.

Raising it applies to data captured afterwards. Reading older data often is the reader's
disk cache's job (#118), not the staging table's: it keeps what is read from the published
table, across restarts, without writing to either table.

Buffer rows are deleted by `evict("buffer")` once the next durable copy holds them — staging,
or the published table with `wal_replication` (§3a). There is no SQLite retention knob.

---

## 9. Immutable logs

**A log's schema is fixed when it is created, for life. To change it, start a new log** (#93).
litelink is a storage engine, not a query engine, and an immutable log is the storage-engine
shape: every file in a log has every column, so there is no field-ID bookkeeping, no file
that predates a column, and no operation that spans the published table, the staging table and the
buffer's DDL at once.

Changing a schema is therefore a new name — and so a new published table — starting where the
old log ended:

```python
old.retire()                                  # everything published, nothing local
new = litelink.new(root, "trades-v2", schema=widened, published=prefix,
                   start_offset=old.end_offset())
```

Offsets stay dense across the two logs, and any engine reads `<prefix>/trades` and
`<prefix>/trades-v2` as one sequence. streamcast's `Stream.migrate` does exactly this. Reusing
the old name against the same published table is refused by the foreign-table guard, as it should be: two
logs would put the same offsets in one table.

**What this replaced.** Through 0.5, `add_column` widened a log in place: the archive (as the published table was then called) first,
then the staging table, then the buffer's DDL, with an intent recorded in SQLite before it began
and replayed on recovery, because the Iceberg commits and the SQLite writes cannot be made
atomic. `rename_column` and `drop_column` were specified and never built. All three are gone.
A log a 0.5 release left mid-`add_column` is refused, naming the release that can finish it,
and a log a 0.5 release widened stays readable: the rollup and the reader already treat a
file that predates a column as holding NULLs, with unknown statistics.

## 10. Invariants

### SQLite is the coordinator

Iceberg gives an atomic commit **to one table**, and that is worth relying on — §6's
compaction rests on it, swapping the snapshot pointer so readers never observe a gap or a
double count. What it gives across systems is nothing, and every operation here spans
systems: a seal touches the buffer, a file and the table; a compaction touches a file, the
table and the deletion queue; a retirement touches the buffer, both tables and the
replica. There is no commit that covers a pair of those.

So the local database is the coordinator, and the protocol is the same one four times over:

```
1. record the intent in SQLite        -- before anything observable changes
2. do the work, ending in the Iceberg commit
3. record completion in SQLite        -- and clear the intent
```

`sealing` is step 1 for a seal, `compacting` for a compaction, `pending_delete` for a
deletion, and the buffer's `tier_offsets` row — its end — for a retirement. **An operation
is complete when step 3 lands, not when the Iceberg commit does.**

This is not two-phase commit, and it is better suited than 2PC would be: there is no vote and
no participant that can veto, because **the true state is always derivable**. Recovery asks
the table which half already happened — does it contain this path, does it hold this range —
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
| **I3** | Tier boundaries are derived from each neighbour's committed offset extent at read time, never from stored flags or an assumption of disjointness. | The published table overlaps the staging window by design. A flag would have to be updated in a different transaction from the Iceberg commit, reintroducing a double-count or drop window. |
| **I4** | A file is never evicted from the staging table while the published table still lacks it. Every log has one (#98): on S3, or a local directory. | Eviction before registration is data loss. It used to be vacuous for a log with no published table, which made `staging_retention` a deletion policy over the only copy — and made detaching a published table a silent conversion into one (§8). |
| **I5** | Reads served from within `staging_retention` never touch the network or require publish to have run. | The central claim. A read that quietly needs the network reintroduces every problem this shape removes. Conditional because `staging_retention = 0` is a valid archival configuration (§8) in which the staging window is empty by choice. |
| **I6** | Snapshot expiry retains each table's snapshots for at least its snapshot retention, exceeding the longest scan of that table. | Expiry deletes data files an open scan is still reading. |
| **I7** | *Retired with schema changes (#93).* Schema changes reached the published table before the staging table. | Logs are immutable (§9), so there is no schema change to order. |
| **I11** | `litelink_offset` is assigned by the library and never accepted from the caller. | Monotonicity and non-reuse are the boundary mechanism; an application-supplied value cannot be enforced. |
| **I17** | An append names only columns the log declares, supplies a value for every non-nullable one, and gives each a value of its declared type, or it is refused. | The insert is built from the SCHEMA's columns, so an unknown key is dropped before any SQL exists and neither SQLite nor pyarrow ever sees it — `append` would return an offset for a row it had truncated. The omission is the same wedge from the other side: a non-nullable column the row leaves out, or supplies as `None`, is stored as NULL, and then **every** scan raises `Casting field … with null values to non-nullable` — including scans of rows written before it — while `append` keeps handing back offsets. Writer sees a healthy log, readers see nothing. A row misspelling a declared column trips both halves at once: it names something undeclared and shadows the real column with NULL. The type clause closes the same two outcomes reached through a value rather than a name: SQLite has affinities, not types, so it stores whatever it is given and the declared schema is not consulted again until the read. A value Arrow cannot parse (`"x"` into an int64) wedges every scan; one it can parse but not preserve (`1.5` into an int64, `12345` into a string, `True` into an int64) is silently rewritten, so what is read back is not what was appended and nothing raises at all. Magnitude is checked with it: `2**40` IS an int and `1e300` IS a float, and they fail the same two ways — the int32 wedges every scan, the float32 reads back as `inf`. **Enforced by the buffer's DDL, not by Python.** Every column is declared `ANY` with a `typeof` CHECK, and that is the whole design: a STRICT column of a declared type does not refuse a wrong value, it CONVERTS one. An INTEGER column given `'77'` stores 77 and `'007'` stores 7; a REAL column given `'1e999'` stores `inf`; a TEXT column given `12345` stores `'12345'`. The conversion happens before any CHECK could see it, so a constraint on a typed column would be asked about a value that had already been changed. `ANY` stores the value exactly as given, which is what lets `typeof` tell the truth about it; STRICT is still declared, because it is what makes `ANY` mean "no conversion". `NOT NULL` carries the nullability half — absent and explicitly-None reach SQLite identically — and the range tests ride in the same CHECK. An integer is a legal value for a FLOAT column — `{"price": 5}` is too natural to refuse — but only within the range where every integer converts exactly (2**53 for float64, 2**24 for float32). Past it the value stays an integer in the buffer, since `ANY` performs no conversion, and Arrow then cannot build the column at all: one such value makes every scan and every seal raise for ever while appends keep succeeding. The bound is a range rather than a per-value test because a SQL CHECK cannot ask whether one particular integer is representable, so some that would convert exactly are refused too. Python is left with the one question SQLite cannot be asked, an unknown column: the insert names the schema's columns, so a key the log does not have is dropped before any SQL exists. One leniency is deliberate — `True` into an integer column stores 1, because the driver converts it before SQLite sees it, and it is lossless. A `fixed_size_binary(n)` column adds `length() = n` to its CHECK. **A float must be finite** (#87): the CHECK refuses ±inf, and NaN — which SQLite stores as NULL before any CHECK sees it — is refused in Python, as are non-finite values in a nested column and in `ingest`'s Arrow. NaN because readers disagree about it: Iceberg's file bounds and Parquet's row-group statistics leave it out, so whether DuckDB returns a stored NaN depends on what else shares its file. ±inf because the layer above speaks JSON, which has no infinity. **Nested columns are the one place Python enforces I17.** A `struct`, `map` or `list` value is stored as JSON text, which no CHECK can see into, and Arrow is no check either: it drops a struct key the type does not declare, silently, and accepts `None` for a non-nullable child. So `Nested` walks the declared type before the insert and refuses the same classes — wrong type, out of range, inexact integer into a float, unknown struct field, null where the field is not nullable — naming the path to the value. |
| **I9** | `litelink_offset` is strictly monotonic for the life of a stream and never reused, including after the buffer empties. | Rowid reuse after a delete silently invalidates every tier boundary in §7. |
| **I10** | *Retired with schema changes (#93).* Drops and renames were to go through an explicit versioned operation. | A log's schema is fixed (§9); a changed schema is a new log. |
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
| Two publish passes race | The Iceberg catalog commit is atomic; the loser refreshes and retries. |
| Local disk fills | Backpressure — §13.3. |
| Machine lost | Exposure is whatever was unregistered. An S3 published table is intact and independently readable; a local one is lost with the machine. |
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
target_compact_size    Arrow bytes per FILE               (what compaction converts sealed
                                                          files INTO. None = 8x the seal)
target_compact_rows    max rows per compacted file        (None = 8x target_seal_rows)
target_seal_size       Arrow bytes per SEAL               (size it for READ latency and for
                                                          memory -- keep buffer <20k rows;
                                                          files land SMALLER on disk, by
                                                          whatever compression achieved)
staging_retention      staging window, by TIME            (> longest hot lookback, with margin; 0 = evict on publish)
staging_rows           staging window, by ROWS            (floor: keep at least this many recent rows)
staging_snapshot_retention    staging snapshot expiry floor    (> longest local scan; default 15 min)
published_snapshot_retention  published snapshot expiry floor  (> longest remote scan; default 1 hour)
compact_min_files      minimum adjacent files to compact  (default 4; below 2 is refused —
                                                          every run would look mergeable)
wal_replication        ship the WAL with a sidecar        (needs an s3:// published table; also decides
                                                          whether a seal keeps its rows)
wal_retention          how far back a restore may go      (None = litestream's own default)
```

`sort_by` is NOT in here. Everything above governs future work only, so `set_config` needs
no rewrite; the sort order is a read-shape decision that re-clusters every file the staging
table owns, so it is set at `litelink.new` and changed by `set_sort_by`. It lives in `meta` beside the
schema, not in `LogConfig`.

`advance()` runs the whole pipeline in lifecycle order: seal, compact, publish, reclaim the
buffer, evict, expire, sweep staging, expire the published table, sweep it. Eviction takes only
what `publish` has put in the published table, so it runs after it, in the same pass. Each is
a no-op or a regression without the others: compaction alone increases
storage, since superseded files stay referenced until their snapshots expire; eviction alone
frees no disk, since it removes a file from the current snapshot while the previous one still
references it; and expiry is what actually deletes bytes, held back by
`staging_snapshot_retention` so a running scan does not lose files underneath it (I6).

The consequence worth planning for is that local disk holds roughly
`staging_retention + staging_snapshot_retention` of data, not `staging_retention`.

---

## 13. Open questions

0. **The published table's identity is local, and re-pointing has to reconcile it.** Seven
   consecutive review rounds found defects in one seam, each fix adding a guard on top
   of the last. That is a design signal, and it is recorded here rather than patched
   again.

   The shape of the problem: `published.db` is a LOCAL catalog keyed by table id, naming a
   REMOTE table. Nothing in the entry says which prefix it belongs to, so "is this entry
   mine?" is answered by comparing its metadata location against the configured prefix —
   a string comparison standing in for an identity. Meanwhile `set_published` changes
   durable state that every other process cached at open, and the watermark it resets is
   the thing eviction deletes on.

   What has accumulated as a result: the entry is validated at open; only a lease holder
   may repair it; `set_published` takes the whole-log lease, which overlaps every publish's
   range claim; `publish` re-reads the location under its claim and re-checks it before
   writing a watermark; a failed repair restores
   the entry it displaced; `drain` refuses to delete outside the configured prefix. Each
   is correct and each was found the hard way.

   What would replace them: give the published table an IDENTITY the entry carries — a token
   written into the published table's own table properties at creation and recorded beside the
   URI locally, so "is this mine?" is an equality check on a value rather than an
   inference from a path. Prefix comparison then stops being load-bearing and a re-point
   becomes one durable fact to change rather than three that can disagree.

   Re-attaching to a published table that already holds data no longer waits on that: the
   published table publishes `version-hint.text` at every commit and `open_published` registers
   from it. What the token would add is a CHECK. The hint says where this log left the
   metadata, and adopting it trusts that nothing else wrote the published table in between —
   true under the one-writer-per-log contract, and unverifiable without an identity.

   **The deferral has a measured cost, and this paragraph used to understate it.** It
   claimed the guards above were sufficient for the operations the library supports —
   attach, detach, re-point to a fresh prefix. A later round disproved that. It found
   four more defects in this seam, and unlike their predecessors two of them needed no
   race, no crash and no lease lapse: attaching a published table to a log a maintainer already
   had open let that maintainer go on deleting the only copy of every row past
   `staging_retention`, because `evict` asked its own memory whether I4 was owed; and
   re-asserting a published table from a process whose memory had gone stale read as a move and
   zeroed the watermarks of a bucket that held the data. The findings got *less*
   contrived, which is the opposite of what a converging seam does.

   The reason is now legible. The published table's identity lives in four places — the `meta`
   row, each process's `Published` object, the `published.db` catalog row, and each captured
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
   The `claim(id, owner, expires_at, kind, start_offset, end_offset, rel_path)` table exists, every
   range-owning pass claims before it works with the conflict check and the insert in one
   `BEGIN IMMEDIATE`, and recovery reclaims expired claims rather than a role's. See §4a
   and `_claim.py`.

   `sealing` and `compacting` were NOT subsumed, which the original sketch expected. They
   are intent records — the path written down before the file exists (I2) — and a claim
   answers a different question, so collapsing them would have made one row mean two
   things.

   The correctness item it was also going to fix is fixed by a different mechanism: a merge
   can no longer include files publish has published, because compaction and `publish` both read
   the per-segment published records rather than a watermark, and `published_prefix` excludes
   anything a published table holds.

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
   exists, since re-encoding means rewriting the published table.
3. **Local disk backpressure.** The failure that used to be "object storage is down" is now
   "local disk fills." Bound the buffer on **bytes**, not row count — a row-count bound can
   exceed a byte-based memory limit, letting the OOM killer win the race against the policy
   meant to prevent it.
4. **Bulk ingest.** Loading an existing corpus — a backfill, an import from another store — through
   `append()` row by row wastes the point of already having Parquet. The wanted path is to
   lock writes, reserve a contiguous offset range, materialise `litelink_offset` into the file, and
   commit. Four things it meets, none blocking, none free.

   **Phase 2 is implemented — `WriteHandle.ingest`, taking Arrow.** Three things below
   shipped differently and the differences are recorded where they appear: the exclusivity
   rule is three reads rather than "seal the buffer empty"; the staging table is the
   `compacting` one, which already has the shape this section asks for; and the reservation
   is per output file rather than one for the load. What is NOT implemented is bulk-loading
   history into a `start_offset` reserve under live capture, which is #33's deferred backfill
   and needs a range-aware coverage predicate `register` does not have.

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
   `litelink_offset` statistics cover `[start, end)` while containing none of it; when the staged file
   commits, the two overlap, which is exactly what §6's *"no other file overlaps that
   range"* forbids. The weaker property is also the correct one: §6 needs files
   non-overlapping and adjacent in offset order, not free of integer gaps, and gaps already
   arise from rolled-back batches (§15.3).

   **"Seal the buffer empty before reserving" is not the check, and the one-read version
   loses rows.** `seal()` cuts and then returns even when it sealed nothing — losing the
   lease is not a failure — so a writer that appends and calls `seal()` while a maintainer
   holds the range is left with a FRESH empty open group and its rows in a group already
   queued. Asking only the open group passes; the rows are below `lo`, in no file; the
   maintainer drains its queue, the register is declined, and the next `evict("buffer")`
   deletes them into a table that is contiguous, non-overlapping and undetectably wrong.
   The check is three reads — `pending_group()`, `pending_seal()`, and the open group's
   `start_offset` — and they are complete because of the CALL GRAPH rather than because they
   cover the cases found so far: acknowledged rows live only in `buffer`, only
   `_write_and_commit` turns one into a file, it has exactly two callers, and those callers'
   two range sources plus the open group are these three reads. A third caller, or a second
   function that writes buffer rows to Parquet, needs a fourth read.

   **The orphan sweep does not transfer.** §15.4 sweeps by offset against the §7 boundary,
   which works because every spilled blob has a buffer row carrying `{name}_spilled`. A bulk
   file has no buffer row and sits *above* the boundary until it commits, so an abandoned
   ingest reads as still-referenced forever. It needs a table parallel to `sealing`, holding
   `(start_offset, end_offset, rel_path)` — not a row in `buffer`, which §7's hot read would union into reader
   output, and not `sealing` itself, which holds one row and would block seals for the
   length of a rewrite.

   That table already exists: `compacting` holds exactly `(start_offset, end_offset, rel_path)`, takes several
   rows because a published rewrite claims one object at a time, and its recovery already
   does the right thing for an abandoned ingest — queue every claimed path, and let `drain`
   refuse the ones the table turned out to reference. A second table of the same shape would
   be a second mechanism for one fact. What ingest must NOT reuse is `claim_seal`: on reopen
   `_recover_seal` reads `rows_between(lo, hi + 1)` from a buffer that never held the range,
   writes an EMPTY Parquet, registers it as covering `[lo, hi]`, and discards every buffered
   row below `hi + 1`. Measured: 40 acknowledged rows deleted into no file, and an empty file
   carries no column statistics, so `extent()` then raises on every call.

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
   has to tell this reserve from a `litelink.restore` fence, and after the fact nothing else can:
   both are empty ranges below the log's offsets, and `WriteHandle` says of the failover reserve that
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
   created with a non-zero starting offset — `litelink.new(..., start_offset=N)` — leaves
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
   has been issued yet. `litelink.restore` already reserves a gap this way (`RESTORE_RESERVE`),
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
5. ~~**Extension provisioning for embedders.**~~ **Closed: shipped in the wheel.** §7 made the
   extension download a provisioning obligation. A repo can discharge it in its bootstrap and
   its CI; an application that `pip install`s the library runs neither, so it got the read path
   and no extensions, and its first read was the network read the design says it is not.

   Since v0.1.0 the platform wheels carry `iceberg`, `avro` and `httpfs` built for the DuckDB
   they pin, alongside a checksum-verified litestream, and `python -m litelink` reports what a
   machine is missing. Verified with no network, nothing on PATH and an empty DuckDB home.

   Two things the closing turned up. `avro` is not requested by litelink at all — `iceberg`
   auto-installs it from inside its own init function, over the network — so a bundle without
   it is not offline-capable. And the extensions are keyed by exact DuckDB version AND
   platform, which is why `duckdb` is pinned to a single patch: a wheel is immutable, and the
   day the next patch ships an uncapped range would make every published wheel useless offline
   with no way to amend it.

   The original text below stands as the reasoning that led here.

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
   `seal()` drains the queue; `advance()` calls it first, then runs the rest of the
   pipeline. Both are plain methods on their caller's schedule, and
   if another owner holds the lease the call is refused and returns rather than
   duplicating the work.

   An earlier design had `seal_mode` ("background" | "inline" | "none") and `seal_poll`,
   with `extend()` starting a daemon thread. That existed only because sealing used to sit
   on the append path — "where does this expensive thing run" was a real question. Once the
   cut moved into the append transaction, sealing became draining, which is what
   `advance()` already was, and the asymmetry had no defence: there was never a
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
   `await log.seal()`, and above all `await log.await_seal()` — whose name already
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
   because an owner is a UUID minted per acquisition rather than per `WriteHandle`, so two threads
   calling `seal()` are two owners and the second loses on the same row that would refuse
   another process. An owner fixed per `WriteHandle` would be re-entered by every thread sharing it,
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
   | separate process, via `litelink.open(..., read_only=True)` | 327 scans, clean |
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

     `binary` and `fixed_size_binary(n)` columns are carried since #79: the buffer leg is
     Arrow built in Python, so blob bytes cross it untouched. §15 still owns large
     payloads, which **bypass** the buffer rather than travel through it.

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

   §15.3 already settled the analogous case in that direction. The spilled bit is
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

   **The larger factor is snapshot accumulation, and expiry arrests it** — which `advance()`
   already does, bounded by `staging_snapshot_retention`. An earlier version of this entry blamed file
   count alone, measured with `advance()` never running so that every snapshot survived. That
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
   `staging_retention = None` the default, so a local-only capture that keeps its history still
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
   that is subtly wrong still opens locally and breaks the external readers the published table exists
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

   **The published table has the same commit cost and it does not matter in the same way.** Its file
   count is unbounded by design — it is the full history — so registering into it rewrites
   manifests against a table that only grows. But that happens in §5, which is lazy,
   restartable and arbitrarily far behind, and no read depends on it. The same work that
   stalls an append when a seal does it is absorbed by a background pass when publish does. That
   is the argument for eviction as the bounding mechanism: it does not remove the cost, it
   moves it to the tier that can wait.

   One coupling survives the move, and it closes a loop worth watching. Eviction may not
   precede registration (I4), so if commits to the published table slow enough that publish falls behind,
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

   **Time has no column type, by design.** How to represent it is the application's choice —
   an `int64` epoch is the usual one, a string works too — so the temporal Arrow types are
   refused rather than pending (#79). A native type was measured and rejected: `timestamp[ns]`
   is refused by pyiceberg on v2, and `timestamp[us]` truncates nanoseconds and fails every
   seal past year 9999, because pyiceberg's manifest statistics go through Python's `datetime`.

   **Default column values**, which would have made 0.5's `add_column` less lossy — an older
   file could read a declared default rather than null. Logs are immutable now (§9), so this
   no longer applies.

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
- With the published table deliberately overlapping the staging window, assert a full three-tier read
  returns every row exactly once (I3).
- Evict the staging table to empty; assert a full read still returns everything, from the published
  table plus the buffer alone.
- Seal until the buffer is empty, insert again, and assert the new offsets exceed every
  offset already committed to Iceberg (I9). This fails with a bare `INTEGER PRIMARY KEY`.
- Assert an unregistered file is never evicted locally, even past `staging_retention` (I4).
- Expire snapshots during a long scan; assert the scan completes (I6).
- Attach an external engine to the published table; assert it sees exactly the expected rows with no
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
- Commit twice, then assert a reader that cached `metadata_location` from the first commit
  is detectably stale -- the reason §7 requires resolving it per query.
- Run with `staging_retention = 0`; assert seal, upload, published reads and compaction all work
  and that eviction still never precedes registration (I4).
- Restore a WAL replica taken before a seal; assert the read returns each row exactly once
  despite the restored buffer holding already-sealed rows.
- Property test: for `t1 < t2`, `read(t1) ⊆ read(t2)`, across a seal, a compaction and an
  expiry.

---

# 15. Blob fields

**Extension to capture storage v1.0.** Support for payloads too large to sit comfortably in
the SQLite buffer: sensor frames, point clouds, raw response bodies.

**A blob field is not a `binary` column, and the difference is size.** An ordinary `binary`
or `fixed_size_binary(n)` column is carried today (#79) and travels like any other value:
into SQLite at `synchronous=FULL`, out through the WAL sidecar, into the Arrow conversion
every hot read makes of the buffered tail, and against `target_seal_size`. That is right for
identifiers, hashes and small encoded values — a trace id is 16 bytes. It is wrong for a
payload. A 5 MB point cloud in a `binary` column is fsynced through the buffer, shipped whole
by the replica, materialised by every scan that reaches the buffer, and cuts a file of one or
two rows at the default 8 MiB seal target. A blob field exists to avoid exactly that: its
bytes never enter SQLite (§15.3) and meet the table only at seal.

Blob fields are unbuilt, so a payload that size does not belong in a log yet. Where the line
falls between the two is §15.12's open small-blob threshold — around 1 MB, below which the
spill path costs more than it saves.

---

## 15.1 The model

**Bytes live beside SQLite while hot, and inside Iceberg once sealed.** The column is the
same shape in both tiers: a plain Iceberg `binary` column holding the bytes themselves.

There is no pointer in the schema. The hot-side spill path is derived from `litelink_offset` and
the field name, so it exists only on the write side and never reaches the table. The published table
is therefore ordinary Iceberg with a binary column, readable by any engine with no
convention to know about and no dereference step.

This is the one design decision worth stating explicitly, because the obvious alternative
looks cheaper and is not. A sidecar object store with a `(path, offset, length)` struct
column avoids inflating the Parquet files, but it puts a reference in the published schema,
makes the published table unreadable without library-specific logic, and creates a class of object
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
litelink.new(
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
2. Write bytes to {root}/blobs/{offset}.{field}.bin
3. fsync the file AND the directory entry
4. Set {name}_spilled = 1 on the row. COMMIT.
```

**Step 3 before step 4 is correctness, not durability hygiene (I12).** A committed row whose
blob file did not survive the crash is a row pointing at nothing, and it is unrecoverable
because the bytes were never anywhere else. Syncing the directory entry matters as much as
the file: on most filesystems the file can be durable while the name that reaches it is not.

The per-blob fsync is affordable precisely because blobs are large. At 2 MB the fixed cost
amortizes to nothing; this would be unacceptable at 2 KB, which is why small payloads should
stay in an ordinary `binary` column and go through the normal buffer path.

### Getting `litelink_offset` before the bytes are written

The spill path derives from `litelink_offset`, so the offset must exist before step 2 — but §2 pins
`INTEGER PRIMARY KEY AUTOINCREMENT`, which assigns at INSERT.

Both hold at once by keeping the transaction open: `INSERT` inside `BEGIN` yields
`lastrowid` immediately, and the row does not become visible until `COMMIT` in step 4. No
separate reservation table and no pre-allocated runs are needed.

**But AUTOINCREMENT reuses an offset after a rollback.** Verified: an `INSERT` that receives
offset *N*, then rolls back, is followed by an `INSERT` that also receives *N*. That is
harmless for I9, which governs *committed* offsets and therefore the tier boundaries — but
it is not harmless here, because step 2 has already put a file on disk at that name.

The failure it would cause: a crash after step 3 leaves `blobs/N.payload.bin` with no
committed row. A later row takes offset *N* again with **no** blob, and a seal that inferred
"does this row have a blob?" from file existence would attach the stale bytes to it.

**So blob presence is recorded on the row, never inferred from the filesystem** — by
`{name}_spilled`, a bit **in the buffer table only**. It is set in step 4, inside the same
commit that makes the row visible, so it is true exactly when the bytes are durable.

It is deliberately **not** `{name}_size`, and not any column in the published schema:

- The bit is meaningless after seal. Once the bytes are inline in Parquet there is nothing to
  indicate, so it has no business in a table other engines read.
- Overloading a caller-facing column with internal state constrains it. If `size=True` is
  declared, the caller reasonably expects to query it — and now its nulls carry two meanings
  (no blob / not yet spilled) that have to be disentangled forever.
- `{name}_size` is optional, so it would have to be silently forced on to serve as the
  sentinel, which is the sort of surprise that shows up as a schema diff nobody asked for.

A blob file whose row has `{name}_spilled = 0`, or no row at all, is an orphan by
definition, and the §15.4 sweep removes it.

Spilled blobs are local, buffer-scoped, and never uploaded.

---

## 15.4 Seal

Amends §4. One additional step, between choosing the range and writing Parquet:

```
1. SQLite txn: choose [start, end), write it to `sealing`
2. Read buffer rows; for each row with {name}_spilled = 1, read blobs/{offset}.{field}.bin
3. Write Parquet locally with bytes inline; commit to the staging table
4. SQLite txn: delete buffer rows < end; clear `sealing`
5. Delete blob files for offsets < end
```

**Step 5 is garbage collection, not correctness**, exactly like step 4. A blob file whose
offset is below the staging table's committed max offset is unreferenced by construction,
because the bytes are now in a data file. The sweep is therefore idempotent and can run at
any time, including on startup: delete every blob file whose offset is below the boundary
that §7 already computes.

That derivation is why no orphan window needs a time heuristic. A crash anywhere leaves
blob files that are either still referenced (offset above the boundary, keep) or already
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
spilled files themselves — which the library always knows, whether or not `{name}_size` was
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

Unchanged in shape. The hot read joins the spilled blobs by derivation, the sealed read reads the
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
attach the buffer, `iceberg` to scan the tables, and `read_blob` for the spilled blobs.

`start` and `end` are §7's names: the staging table's span over `litelink_offset` in its
current snapshot, `[start, end)`, read from manifest column statistics. The hot read needs only
`end`, which is the value §7's hot-read prose calls `boundary`; it is the same number and this
section uses `end` throughout so one value carries one name.

**The column is named `litelink_offset`, not `offset`.** Two reasons, and the second is the
one that forced it. `offset` is a plausible application column — a byte offset, a page
offset, an offset from a reference time — and reserving it taxes every caller. More
decisively, **`offset` is a reserved word in DuckDB**: `SELECT offset FROM t` and
`max(offset)` are both parser errors, so every query the library wrote and every query a
reader wrote against the published table would have to quote it, forever, with the failure being a
syntax error rather than anything that says why. Verified in both directions —
`max(litelink_offset)` parses unquoted, `max(offset)` does not.

The prefix also namespaces it. An application names its own columns freely, and a schema
naming the one column the library owns is refused at creation (I11) — a log's columns are
fixed there for life (§9).

Spill resolution is identical in both reads, so it is factored out once:

```sql
INSTALL sqlite; INSTALL iceberg;
ATTACH 'buffer.db' AS buf (TYPE sqlite, READ_ONLY);

CREATE OR REPLACE TEMP VIEW spilled AS
  SELECT
    regexp_extract(filename, '(\d+)\.payload\.bin$', 1)::BIGINT AS "offset",
    content AS payload
  FROM read_blob('blobs/*.payload.bin');
```

**Hot read** (staging table plus buffer):

```sql
SELECT "offset", event_ts, key, payload
FROM iceberg_scan('<staging metadata json>')
WHERE <predicates>

UNION ALL

SELECT b."offset", b.event_ts, b.key, s.payload
FROM buf.buffer b
LEFT JOIN spilled s USING ("offset")
WHERE b."offset" >= $end AND <predicates>;
```

**Full-stream read** (published table plus staging table plus buffer). The published table holds no
spilled blobs, so its blob column is read directly:

```sql
SELECT "offset", event_ts, key, payload
FROM iceberg_scan('<published metadata json>')
WHERE "offset" < $start AND <predicates>

UNION ALL

SELECT "offset", event_ts, key, payload
FROM iceberg_scan('<staging metadata json>')
WHERE <predicates>

UNION ALL

SELECT b."offset", b.event_ts, b.key, s.payload
FROM buf.buffer b
LEFT JOIN spilled s USING ("offset")
WHERE b."offset" >= $end AND <predicates>;
```

The blob field changes nothing about the tier boundaries. Both queries bound each tier by its
neighbour's committed extent exactly as §7 specifies, and the spill join sits entirely
inside the buffer branch.

**Explicit column lists, not `SELECT *`.** The buffer branch sources `payload` from a
different relation than the other branches, so the union cannot be built positionally. This
is the only structural difference from §7's pseudocode.

**The spill join is a glob and a parse, not a per-row lookup.** `read_blob` returns
`filename`, `content`, `size` and `last_modified`, so the directory is globbed once and
joined on the offset parsed out of the name. Table functions cannot take correlated
arguments, so a per-row path could not be passed in even if the pointer were stored; deriving
the path from `litelink_offset` and joining is the only shape available, and it happens to be the
faster one.

`LEFT JOIN` rather than inner: a blob field may be null for some rows, and an inner join
would silently drop them.

**`$start` and `$end` are supplied, not computed in SQL.** Expressing either as a scalar subquery
over `iceberg_scan` would read the offset column rather than manifest statistics, which is the
opposite of the §7 design. Resolve them through pyiceberg and pass them as parameters.

**The scan takes a metadata path, not a catalog, and this is deliberate.** DuckDB's `iceberg`
extension attaches REST catalogs only; a pyiceberg `SqlCatalog` on SQLite cannot be attached,
and the path-based `iceberg_scan` is the catalog-free read route. (Verified: `ATTACH … (TYPE
ICEBERG)` against a SQLite catalog fails demanding OAuth2 credentials.) Note this is a
different extension from `ducklake`, whose SQLite catalog support is unrelated.

That constraint happens to coincide with what correctness requires. If DuckDB resolved the
catalog itself, it would select its own snapshot independently of the one `start` and `end` were
computed from. A seal committing between the two leaves `end` stale-low against a newer scan,
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

With multiple blob fields, each gets its own spill view and its own `LEFT JOIN`, keyed on
the field name in the glob pattern.

---

## 15.7 Retention and orphans

**No new object-storage garbage collection.** Blob bytes are inside Iceberg data files, so
snapshot expiry, compaction and orphan cleanup handle them with no additional machinery. This
is the whole payoff of inlining and it should not be given up casually.

**The spill directory is the only library-managed storage**, and it is local, offset-named, and swept
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
| §2 Layout | Buffer holds no blob bytes. Adds an internal `{name}_spilled` bit per blob field, in the buffer table only — never in the Iceberg schema. |
| §3 Write path | Adds the blob write and the fsync ordering (§15.3). |
| §4 Seal | Adds materialization (step 2), the spill sweep (step 5), and sort-by-key-then-permute. |
| §6 Compaction | Unchanged in logic; needs a separate size bound for blob streams. |
| §7 Read path | Unchanged in shape. Hot reads resolve spilled blobs by derivation; `litelink_offset` must be quoted. |
| §8 Retention | Unchanged. Blobs inherit it. |
| §12 Configuration | Adds `blob_row_group_blobs`, `blob_compact_size`; `target_seal_size` raised. |

---

## 15.9 Invariants

Extends §10.

| # | Invariant | Why |
|---|---|---|
| **I12** | The blob file and its directory entry are fsynced before the buffer row commits. | A committed row whose bytes did not survive is unrecoverable. The bytes exist nowhere else. |
| **I13** | A blob file is deleted only when its offset is below the staging table's committed max offset. | Above the boundary it is still the only copy. |
| **I14** | Blob presence is read from `{name}_spilled` in the buffer, never inferred from a blob file existing, and never from a column in the published schema. | AUTOINCREMENT reuses an offset after a rollback, so a stale blob file can sit at an offset a later row legitimately takes. Keeping the bit internal also stops a caller-facing column carrying two meanings for null. |
| **I15** | Blob bytes are never written to any object storage location that no Iceberg manifest references. | The moment they are, lifetime becomes a hand-maintained refcount and expiry stops working. |

I15 is a design constraint rather than a runtime check, and it is the one to revisit
deliberately if the sidecar approach ever becomes necessary.

---

## 15.10 Failure modes

Extends §11.

| Failure | Outcome |
|---|---|
| Crash between blob write and buffer commit | Orphaned blob file at an offset with no committed row. Swept by the §15.4 rule; never mis-attached, because presence comes from `{name}_spilled` (I14). |
| Crash between buffer commit and seal | Blob file survives; the row is durable and readable. This is the case I12 protects. |
| Blob file missing for a row with `{name}_spilled = 1` | Unrecoverable data loss for that row. Detectable at seal; fail loudly rather than writing a null. |
| Crash mid-seal after Parquet commit | Blob files below the boundary are now redundant; recovery sweeps them. |
| Local disk fills | Blobs dominate the bound, so §13's byte-based backpressure must count spilled blobs, not only buffer rows. |

---

## 15.11 Tests

- Kill between blob write and buffer commit; assert the orphan is swept and no row
  references a missing file.
- **Force an offset reuse**: roll back an insert that spilled bytes, then insert a blob-less
  row that takes the same offset; assert the stale bytes are never attached (I14).
- Kill between buffer commit and seal; assert the row reads back with correct bytes.
- Delete a blob file behind a row with `{name}_spilled = 1`; assert seal fails loudly
  rather than writing a null or a short value.
- Declare a blob field with `size=False`; assert the published schema contains no
  `{name}_size`, that the spilled bit appears nowhere in the Iceberg table, and that seal
  still resolves blobs correctly.
- Assert a projection excluding the blob column reads no blob bytes. Measure, do not assume.
- Assert a single-blob fetch reads at most one row group's worth.
- Write blobs totalling over 2 GB in one row group's worth of rows; assert no offset overflow.
- Assert `{name}_size` and `{name}_hash` prune and verify without materializing the payload.
- Compact a blob-bearing offset range; assert byte-for-byte identity of every payload,
  alongside the existing row count and bounds checks.
- Assert an external engine reads the published table's blob column with no library-specific logic.
- Run a stream with a blob field declared but never populated; assert nulls throughout and no
  blob files.

---

## 15.12 Open

**Container swap.** Iceberg has been working toward a pluggable file-format API, with Lance
discussed as a candidate container. **Both the release version and the timeline here are
unverified — confirm against upstream before relying on them.** If and when it reaches
pyiceberg, the data file container swaps and blob reads gain true random access. The column
is already `binary`, so no schema change is implied and no data has to move; only newly
written files change format. Nothing in this section should be designed around it arriving.

**Small-blob threshold.** Everything here assumes payloads large enough to amortize a
per-blob fsync. Below roughly 1 MB the spill round trip is pure overhead and an ordinary
`binary` column through the normal buffer path is better. Whether the library should route
this automatically by size, or require the application to choose, is unresolved. Automatic
routing is friendlier and introduces a size-dependent write path, which is a durability
behaviour that changes under the caller without warning.

**Ranged reads.** Page-index-driven fetches below row group granularity would cut the fetch
amplification substantially. Blocked on reader support, not on anything here.
