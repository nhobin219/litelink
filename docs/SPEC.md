# Capture storage

**v1.0** — durable append-only capture into Iceberg tables. Local-first, no service.

---

## 1. Architecture

```
SQLite buffer          durable on commit. unsealed rows only.
      │                WAL, synchronous=FULL, one db per stream
      │  seal at min(target_size, max_age)
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
`iceberg` and `sqlite` extensions are downloaded rather than bundled, and the first read on
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
are **deleted at every seal**. Once the table empties, the next insert reuses offsets
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

**The library's commit boundary is batching.** It decides when to flush a batch to SQLite;
that grouping carries no information about the data. The boundary that *means* something
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
litelink.Stream(
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
the Iceberg snapshot's commit timestamp rather than any data column. Sorting is configurable.
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

Reference throughput on network-backed storage at a 2 KB row: 21,850 rows/s at
`synchronous=FULL` (46 µs/row), against a raw `append+fdatasync` floor of 30,464 rows/s.
Re-measure on target hardware; on local NVMe the fixed per-commit cost is a larger fraction.

---

## 3a. Optional: WAL replication for RPO

Without it, unsealed rows exist only on local disk, so the data-loss window on machine
failure is bounded by `max_age` — which means `max_age` is doing double duty as a file-size
policy *and* an RPO policy. Shrinking it to reduce RPO produces small files, which is the
problem sealing exists to solve.

**Litestream (or equivalent WAL shipping) breaks that coupling.** It continuously replicates
SQLite WAL frames to object storage, so `max_age` can stay large for good file sizes while
RPO falls to the replication lag.

Optional, and off by default. Three things to be clear about:

**It covers append→seal only.** Once a seal deletes buffer rows, replication faithfully
carries the delete; sealed-but-unuploaded Parquet is not its concern. So

```
RPO = max(WAL replication lag, Parquet upload lag)
```

Adding it is only worth it if uploads are also prompt — which they cheaply can be, since
they are plain PUTs with no compute.

**It cannot break writes.** It is a sidecar reading the WAL, not something in the write path.
If it dies, SQLite is unaffected: you lose replication, not data. That is why it does not
violate the no-network-in-the-write-path property.

**Restore is correct by construction.** A restored buffer may contain rows already sealed
into the table. No reconciliation is needed, because the read boundary (§7) is derived from
the table's committed max offset, so those rows fall outside the buffer's contribution
automatically.

## 4. Seal

Triggered on `min(target_size, max_age)`, evaluated by the writer at commit time. Entirely
local.

```
1. SQLite txn: choose [start, end), write it to `sealing`.
2. Write Parquet locally; commit it to the local Iceberg table.
3. SQLite txn: delete buffer rows < end; clear `sealing`.
```

**Rows are sorted before writing**, by the configured `sort_by` (§12). This is declared as
the table's Iceberg `sort_order` *and* actually applied at write time — the metadata records
intent, it does not sort for you.

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

The sort costs one in-memory sort of at most `target_size` rows per seal, and does not
affect the tier boundaries in §7, which use min/max of `litelink_offset` and are order-independent.

**Step 1 fixes the range before the file exists**, making the path deterministic:
`{stream}/{date}/{start}-{end}.parquet`. A retry recomputes the identical path and
overwrites rather than orphaning. Chosen after the write instead, a crash between write and
commit lets new rows arrive, and the retry seals a wider range while the first file is
stranded.

**Step 3 is garbage collection, not correctness.** Read consistency comes from the offset
boundary in §7, so the window between steps 2 and 3 is safe in both directions and needs no
`sealed` flag.

**Recovery.** On startup, if `sealing` holds a row: if the local table already contains that
path, run step 3; otherwise redo step 2. Idempotent either way.

Sealing never waits on the network. A machine with no connectivity keeps capturing and
keeps serving reads; it accumulates unregistered files.

---

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

Sync records what it has registered in `meta`, keyed by the local table's snapshot ID.

**Steps 4 and 5 do not belong to sync.** Snapshot expiry and local eviction are local
storage work; they are listed here because eviction must respect the registration watermark
step 2 writes. But every other step is archive work, so a log configured with no archive
never runs this pass at all — and would then never expire a snapshot or honour
`local_retention`, leaving the knob silently inert. They are owned by `maintain()` (§12),
which reads that same watermark to enforce I4 and runs with or without an archive.

---

## 6. Compaction

Required: the `max_age` seal branch guarantees undersized files, so a quiet stream emits a
small file every interval indefinitely.

Hand-written, because `rewrite_data_files` is a Spark procedure with no pyiceberg
equivalent.

The table is unpartitioned (§13), so the compaction unit is a **contiguous offset range**.
That works because sealed files already cover contiguous, non-overlapping ranges: pick
adjacent files under `compact_below`, and their combined range is itself contiguous.

```
1. Select adjacent files under compact_below spanning [lo, hi]; require compact_min_files.
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
| `sealing` | a seal's output | I2 — a retry overwrites in place |
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
cannot attach a pyiceberg `SqlCatalog`. Path-based scanning also fails, because `SqlCatalog`
keeps the current metadata pointer in the catalog rather than in a `version-hint.text` file
the way a filesystem catalog would.

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
identically (403 vs 408 ms) and going around DuckDB is slower (`sqlite3.fetchall` 496 ms),
so there is no cheaper path to the buffer.

Below ~20k rows the buffer vanishes into noise and the union floor is the Iceberg leg alone.
Above it the cost goes superlinear: 1.0 us/row at 20k, 1.9 at 60k, 2.3 at 180k.

Projecting only the needed columns roughly halves the buffer leg (216 ms vs 403 ms at 180k)
— the one lever that does not require sealing more often.

**Consequence: the seal threshold is a read-latency knob, not only a file-size knob**, and
it separates cleanly from compaction:

| knob | controls |
|---|---|
| seal threshold (`target_size` / `max_age`) | how many rows sit in the buffer, hence hot-read latency |
| compaction | how large the files end up, hence scan cost |

So **seal small and often, then compact** — rather than sealing at a large `target_size` to
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
counts a `max_age` seal produces is most of a read. Two things bring it back to fixed.

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
further than `max_age`. It does not weaken I4: eviction still never precedes registration.

**With no archive, retention is deletion, and that is the intended contract.** I4 forbids
evicting a file the archive still lacks — but nothing is owed to an archive that does not
exist, so the invariant is vacuous and `local_retention` becomes an ordinary retention
policy over the only copy. Data past the window is gone for good.

That is the right shape for a bounded local capture window, and it is worth stating plainly
because I4's rationale is literally *"eviction before registration is data loss."* Here the
loss is the operator's instruction rather than a bug, and the two are distinguishable only
by whether an archive was configured. `local_retention = None` is the setting that keeps
everything, at unbounded local growth.

`local_retention = 0` presupposes an archive: with none, it would delete each file as it
sealed. Reject the pair at construction rather than honouring it.

Raising it is an operation, not a config change: `hydrate(since=…)` fetches archived files
and re-registers them into the local table. Without it, a raised setting applies only to
data captured afterwards.

Buffer rows are deleted at seal. There is no SQLite retention knob.

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
| **I2** | The seal range is persisted before the file is written. | Makes the path deterministic, so retries overwrite instead of orphaning. |
| **I3** | Tier boundaries are derived from each neighbour's committed offset extent at read time, never from stored flags or an assumption of disjointness. | The archive overlaps the local window by design. A flag would have to be updated in a different transaction from the Iceberg commit, reintroducing a double-count or drop window. |
| **I4** | A file is never evicted from the local table while a configured archive still lacks it. Vacuous when no archive is configured (§8). | Eviction before registration is data loss. With no archive nothing is owed, and `local_retention` is then a deletion policy the operator asked for — see §8. |
| **I5** | Reads served from within `local_retention` never touch the network or require sync to have run. | The central claim. A read that quietly needs the network reintroduces every problem this shape removes. Conditional because `local_retention = 0` is a valid archival configuration (§8) in which the local window is empty by choice. |
| **I6** | Snapshot expiry retains at least `snapshot_retention`, exceeding the longest scan. | Expiry deletes data files an open scan is still reading. |
| **I7** | Schema changes reach the archive before the local table. | The local table is rebuildable; the archive is not. |
| **I11** | `litelink_offset` is assigned by the library and never accepted from the caller. | Monotonicity and non-reuse are the boundary mechanism; an application-supplied value cannot be enforced. |
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
| Local disk fills | Backpressure — §13.4. |
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
target_size            seal at this buffer size           (size it for READ latency, not
                                                          file size -- keep buffer <20k rows;
                                                          compaction produces the big files)
max_age                seal at this age regardless        (e.g. 5 min)
local_retention        local table window                 (> longest hot lookback, with margin; 0 = evict on upload)
wal_replication        continuous WAL shipping for RPO    (off by default; §3a)
snapshot_retention     snapshot expiry floor              (> longest scan)
compact_below          compact files under this size      (e.g. 0.5 x target_size)
compact_min_files      minimum adjacent files to compact  (e.g. 4)
sort_by                within-file sort order              (capture default: event_ts, key)
```

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
   ever registered — the mirror of a quiet stream's undersized file being merged before
   upload. §6 is merge-only, selecting files *under* `compact_below`, so the split is an
   addition: the same `overwrite` on the same offset-range filter, emitting N files instead
   of one, with step 3's row-count-and-min/max verification unchanged. Bulk ingest is what
   creates the requirement — a seal cannot emit an oversized file, since `target_size`
   already bounds it.

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

   Reserving needs no new counter. Bumping `sqlite_sequence` by N inside the write
   transaction reserves `[old+1, old+N]`, preserves I9, and lands above everything ever
   assigned. §2's note that an explicit `meta` counter *"only earns its extra moving part
   if offset ranges must later be pre-allocated across producers"* is the clause this
   trips; the `sqlite_sequence` bump is the cheaper way to satisfy it.
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

   Options, none chosen:

   - **Leave it in-process and seal on a background thread.** No coordination at all, and it
     takes the spike off the append path, which is most of the prize. The lock already
     exists. Does not address recovery ownership, because there is only one process.
   - **A lease per role**, writer and maintainer, each recovering its own intents. Simple to
     state, but the split is coarser than what is actually exclusive, and it forces sealing
     to sit on whichever side owns the buffer.
   - **A lease per resource** — buffer writes, the seal, the compaction — matching the intent
     tables that already exist (I16). Finer and it composes, at the cost of more moving parts
     in the single-process case that remains the default topology.
   - **Move sealing to maintenance entirely**, handing step 3 back to whoever holds the buffer
     write lease.

   Open sub-questions the last option raises, and probably the reason to be careful:

   - **What triggers a seal?** §4 says the writer evaluates it at commit time, and that is
     free because the writer already knows the buffer grew. A maintainer would poll. The
     `max_age` branch is time-based and would not care, but it is not wired up at all today.
   - **The size counter is per-process.** The seal trigger reads an in-memory byte count the
     appending process maintains; rows removed by another process would not decrement it.
   - **Deferring step 3 widens a window that is currently narrow.** It is safe by §7's
     boundary at any width, but the buffer holds sealed rows for longer, and §7 measures the
     buffer as the entire variable cost of a read.

---

## 14. Test plan

Beyond §10:

- **Block all network access; assert writes, seals, compaction and hot reads all succeed.**
  This is I5 and the central claim.
- Kill between Parquet write and Iceberg commit; assert recovery reuses the same path and
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
litelink.Stream(
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

**Raise `target_size`.** The §12 default is a handful of rows once blobs are inline. Size it
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
`compact_below`, or the pass will move far more data than the file-count problem justifies.

---

## 15.8 Amendments to existing sections

| Section | Change |
|---|---|
| §2 Layout | Buffer holds no blob bytes. Adds an internal `{name}_staged` bit per blob field, in the buffer table only — never in the Iceberg schema. |
| §3 Write path | Adds the staging write and the fsync ordering (§15.3). |
| §4 Seal | Adds materialization (step 2), the staging sweep (step 5), and sort-by-key-then-permute. |
| §6 Compaction | Unchanged in logic; needs a separate `compact_below` for blob streams. |
| §7 Read path | Unchanged in shape. Hot reads resolve staging by derivation; `litelink_offset` must be quoted. |
| §8 Retention | Unchanged. Blobs inherit it. |
| §12 Configuration | Adds `blob_row_group_blobs`, `blob_compact_below`; `target_size` raised. |

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
