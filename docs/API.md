# API

Everything public, on one page. [`SPEC.md`](SPEC.md) says what the system is and
[`RUNTIME.md`](RUNTIME.md) says how it runs; this says what you can call.

```python
import litelink
from litelink import LogConfig, LogHandle, Row, S3Options, WriteHandle, __version__
```

Those are the exports. The handles are the whole object model — there is no session, no client, no
catalog handle to hold. A log is a directory under a root, named at `litelink.new` and opened by
that name for ever after.

**`Row` and `S3Options` are exported because public signatures name them**, and a type a
caller has to name has to be importable. `Row` is `Mapping[str, object]`, what `append` and
`extend` take.

`S3Options` is named by `new`, `open`, `restore` and `replication_config_for`. A frozen dataclass of `endpoint`, `access_key`, `secret_key`
and `region`, every field optional: omit the argument entirely and credentials resolve
through the ordinary AWS chain, which is the intended path on AWS. Construct one only for a
non-AWS endpoint or an explicit key. It is deliberately not part of `LogConfig`, because
`LogConfig` is persisted into the log directory and a secret must not travel with something
that gets copied and attached elsewhere.

## Handles, not logs

The log is the directory and the objects in the bucket. These classes are **handles** to it —
which is why none of them is called `Log`. A class named after the data invites the question
of why a read-only one is a lesser version of it, and `sqlite3` has no `Database` class
either; it has `Connection`.

Every handle can read. Each subclass only **adds**:

```
LogHandle                    identity · read · observe · close        ← annotate this
├── LocalReadHandle          + databases · replication_config · write_replication_config
│   └── WriteHandle          + append · seal · maintain · sync · set_* · add_column
└── RemoteReadHandle         + owns and removes the scratch root it was built in
```

**Nothing inherits a method it has to refuse**, which is the property two earlier shapes kept
failing. A `Follower` subclassing a writable log carried `append`, `seal` and `sync` that only
raised; a `read_only` flag did the same to thirteen methods. A followed log here simply has no
`replication_config`, because `RemoteReadHandle` is a *sibling* of `LocalReadHandle` rather
than a child.

```python
litelink.new(root, name, *, schema, sort_by=None, config=None, archive=None,
             s3=None, start_offset=1)                    -> WriteHandle
litelink.open(root, name, *, s3=None)                    -> WriteHandle
litelink.open(root, name, *, read_only=True, s3=None)    -> LocalReadHandle
litelink.restore(root, name, *, archive, s3=None, binary=None) -> WriteHandle
litelink.follow(name, *, archive, s3=None, binary=None,
                scratch_dir=None)                        -> RemoteReadHandle
```

**`open` is overloaded on the `read_only` literal**, so the type you get is static:

```python
litelink.open(root, name).append(row)                   # checks
litelink.open(root, name, read_only=True).append(row)   # type error, not a runtime one
```

That is how typeshed types the builtin `open()` — `TextIOWrapper` or `BufferedReader`
depending on the mode literal. The difference from the flag this replaced is that read-only
returns a class with **no write methods at all**, rather than one class whose thirteen write
methods raise. A non-literal `read_only` falls back to `LogHandle` and the caller narrows.

### The local/remote boundary is the constructor

| | `open(root, name, read_only=True)` | `follow(name, archive=…)` |
|---|---|---|
| **type** | `LocalReadHandle` | `RemoteReadHandle` |
| **what you pass** | a root **on this machine** | an **archive URI**, and no root |
| **where the data is** | the log's own directory | object storage + a restored replica |
| **the buffer** | the writer's live `buffer.db` | a litestream restore of it |
| **the local Iceberg table** | the writer's, filling as it seals | created **empty**, never filled |
| **freshness** | live — sees commits as they land | a **snapshot**, as of the restore |
| **refreshing** | nothing to do | assemble another one |
| **archive** | optional; a hot read is local disk (I5) | **load-bearing** — see below |
| **replication config** | yes — the key it emits is the primary's own | **absent** — would emit the primary's key |
| **root lifetime** | yours | a scratch dir, removed on `close` |

```python
# On the writer's box: a live second view. No archive argument — the log
# already records where its archive is, and reads come off local disk.
with litelink.open("data", "trades", read_only=True) as r:
    r.scan(where="side = 0")          # local: buffer + local Iceberg table
    r.end_offset()                    # local while the table has files
    r.write_replication_config()      # its replica key IS the primary's

# On any other box: no root, an archive URI, and a WAL sidecar on the writer.
with litelink.follow("trades", archive="s3://bucket/prefix", s3=opts) as r:
    r.coverage()                      # Coverage(archive=(1, 1928), buffered=(1929, 2100), …)
    r.scan(where="side = 0")          # archive + replicated tail, merged
    r.write_replication_config()      # AttributeError — it does not have one
```

**What differs between them is state, not capability.** Whether the archive is load-bearing,
whether a pinned snapshot can be swept, what `end_offset` may trust — all of it is derived
from the tiers by `LogHandle` and written once, so it cannot drift between the two. A followed
log includes the archive automatically and refuses to serve without it because its local table
is empty and its archive holds rows; **a local handle to a fully evicted log meets the same
two conditions and is treated the same way**, correctly, which it was not before.

## The whole surface

Each row is what that class **adds** to the one above it. A test pins every set exactly.

| | |
|---|---|
| **`LogHandle`** — read | `scan` · `sql` |
| **`LogHandle`** — observe | `end_offset` · `buffered_rows` · `table_rows` · `table_files` · `table_extent` · `archived_through` · `archive_files` · `coverage` |
| **`LogHandle`** — identity | `root` · `name` · `config` · `schema` · `sort_by` · `archive` |
| **`LogHandle`** — lifecycle | `close` · context manager |
| **`+ LocalReadHandle`** | `databases` · `replication_config` · `write_replication_config` |
| **`+ RemoteReadHandle`** | owns and removes its scratch root |
| **`+ WriteHandle`** — write | `append` · `extend` · `ingest` |
| **`+ WriteHandle`** — seal | `seal_due` · `seal` · `await_seal` |
| **`+ WriteHandle`** — maintain | `maintain` · `compact` · `evict` · `expire` |
| **`+ WriteHandle`** — archive | `sync` · `hydrate` · `rewrite_archive` |
| **`+ WriteHandle`** — configure | `set_config` · `set_archive` · `set_sort_by` |
| **`+ WriteHandle`** — recover | `recover` · `recovery` |
| **`+ WriteHandle`** — schema | `add_column`; `rename_column`/`drop_column` raise `NotImplementedError` |

`await_seal` is deliberately a `WriteHandle` method: it *helps* drain the queue each round
rather than only watching, and a reader could only watch.

Most deployments use six: `new`/`open`, `extend`, `scan`, `seal_due`, `maintain`, `sync`.

## Lifecycle

```python
litelink.new(root, name, *, schema, sort_by=None, config=None, archive=None, s3=None,
             start_offset=1) -> WriteHandle
litelink.open(root, name, *, s3=None) -> WriteHandle
litelink.open(root, name, *, read_only=True, s3=None) -> LocalReadHandle
litelink.restore(root, name, *, archive, s3=None, binary=None) -> WriteHandle
```

**`new` takes the shape; `open` takes none of it.** Schema, sort order, config and archive
live in the log and come back from it, so nothing at the call site can disagree with what is
on disk. `new` raises `FileExistsError` if a log is already there, and `ValueError` if the
archive it is pointed at holds data this log has no record of pushing — that archive belongs
to another log.

`start_offset` is set here and only here too, and leaves `[1, start_offset - 1]` unassigned
for ever. It aligns a log's offsets with a sequence something else owns, and reserves room a
later backfill can fill (§13). There is deliberately no way to re-seed a log afterwards: the
guard that would refuse it reads the offsets currently BUFFERED, which a seal empties, so it
cannot tell an unused offset from an issued one.

`sort_by` is set here and only here; it is a read-shape decision rather than a knob (§7), and
changing it later rewrites the local table's files — see `set_sort_by` for what that does not
reach. `schema` is your columns — the library prepends
`litelink_offset` itself, and refuses a schema that declares it (I11).

**`open` recovers before it returns**, finishing whatever a crash interrupted. It raises
`FileNotFoundError` for a log that is not there, and `ValueError` for one whose stored config
or sort order is missing — a log that exists but is corrupt.

**`open(..., read_only=True)` opens a second view alongside a live writer.** Any number of processes may hold
one. They take no lease, mutate nothing, and coordinate with nobody. Reading in the *same*
process as the writer is the case to avoid — see RUNTIME.md on two SQLite libraries in one
process.

**`restore` is failover, not a read replica** (§3a). It rebuilds a log on a machine that is
not the one that wrote it: restores `buffer.db` from the WAL replica, rebuilds the local
Iceberg table *empty*, adopts the archive through `version-hint.text`, and reserves an offset
window so nothing the dead machine served is reissued. `binary` names the litestream
executable if it is not on `PATH`. It refuses a root that already holds this log
(`FileExistsError`) or whose `litestream.yml` replicates a different one. Split-brain is not
detected — if the primary is alive you now have two writers on one archive.

```python
log.recover() -> None          # idempotent; `open` already did it
log.recovery() -> _Recovery | None
log.close() -> None
```

`recovery()` returns what a `restore` recovered — `recovered`, `resumed_at`, `skipped` — and
`None` on a log opened normally. Those numbers are available nowhere else afterwards.

`close` releases handles. **It does not seal**, and has nothing to stop, because the library
owns no thread. `WriteHandle` is a context manager, and `with` is the same thing.

## Writing

```python
log.append(row: Row) -> int               # Row = Mapping[str, object]
log.extend(rows: Iterable[Row]) -> list[int]
```

Both return the assigned offsets, and **the rows are durable when the call returns** — one
SQLite transaction at `synchronous=FULL`.

`extend` commits the whole group in one transaction, so it is one fsync for the batch rather
than one per row. **That call size is the write-throughput lever**, and it is a call-site
choice: no `LogConfig` setting tunes it. `append(row)` is `extend([row])`.

An append does no work beyond its own insert. It does not measure the buffer, decide whether
to seal, or delete anything — it records where the next file should be cut, in the same
transaction, and returns.

### Bulk loading: `ingest`

```python
log.ingest(source: pa.Table | pa.RecordBatchReader) -> tuple[int, int] | None
```

Writes Parquet directly and never puts the rows through SQLite. Returns the inclusive offset
range it assigned, or `None` for a source with no rows.

The buffer exists to make a row durable before it is in Parquet, and a bulk load's source is
already durable — so every row pushed through it at `synchronous=FULL` pays a second time for
a guarantee it has. Measured on 400k rows on local disk, where fsync is cheap and the gap is
therefore understated: **182,801 rows/s through the buffer against 5,103,266 rows/s writing
Arrow straight out.**

Parquet-to-Arrow is yours: hand it `pq.ParquetFile(path).iter_batches()` or a `pa.Table`.
Memory is bounded at one output file either way. Files come out sorted by `sort_by` and sized
at `target_compact_size`, so maintenance never has to touch them.

**It refuses concurrency rather than surviving it.** The whole log is claimed for the whole
load, and every acknowledged row must already be in a file — call `seal()` and `await_seal()`
first, or it raises and names what is outstanding. `ingest` is called by the single writer in
its own process; concurrent `append` is excluded by §1, not by a lock.

**It refuses `wal_replication`**, because these rows never enter the buffer and WAL shipping
therefore cannot carry them at all — scope, not timing. Load first, turn replication on when
capture starts.

**The archive is a loaded range's only second copy.** Compare `archived_through()` against the
`hi` this returns; until they meet, the corpus you loaded from is the range's second copy.
`sync` lags the tail on purpose — it holds back a trailing run still under
`target_compact_size` — and the run settles as soon as capture writes past it.

**A load that fails costs its reservation.** The offsets of the file being written are gone,
leaving a gap. Files stay non-overlapping and adjacent in offset order, which is what §6
needs; the one price is that compaction will merge across a gap and such a file can never be
re-cut by `rewrite_archive`.

## Reading

```python
log.scan(*, columns=None, where=None, start_offset=None,
         end_offset=None, include_archive=None) -> pa.RecordBatchReader
log.sql(query, *, include_archive=False) -> pa.RecordBatchReader
```

`scan` unions the tiers and bounds each by its neighbour's committed offset extent, resolved
from manifest statistics at query time (§7, I3). The tiers overlap by design; the bounds are
what make each row appear exactly once.

**`include_archive` defaults to `None`, meaning "decide from the tiers".** That resolves to
False whenever the local table holds files, because a hot read is local disk only and must stay
that way (I5). Opting in is opting into network I/O.

`sql` is the same relation under arbitrary DuckDB SQL, exposed as `log`. Both return a
streaming reader rather than a table: a full-window read with a 400-byte payload column is
611 ms and proportional to the data, so materialising it is the caller's choice.

**Always bound on a leading column of `sort_by`.** §7 measures a non-leading predicate at
119 ms against 13 ms for the same predicate with a leading bound.

Other machines do not use this API at all — see below.

## Reading from another machine

The API above is the *writer's* read: local disk, all three tiers, no network. Every other
machine reads the archive instead, and needs nothing from litelink to do it. The archive is an
ordinary Iceberg table that publishes `version-hint.text` at every commit, so an engine pointed
at the prefix resolves the current metadata itself — no catalog service, no `archive.db`, no
local root, no litelink install.

```sql
INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;
CREATE SECRET (TYPE s3, PROVIDER credential_chain);

SELECT count(*), max(litelink_offset)
FROM iceberg_scan('s3://bucket/prefix/trades',
                  version_name_format = '%s%s.metadata.json');
```

The table sits at `<archive prefix>/<log name>` — its data and metadata together, since
0.2. **`version_name_format` is not
optional**: DuckDB defaults to the Hadoop `v%s%s.metadata.json` while pyiceberg names its
metadata `00003-<uuid>.metadata.json`, so the hint carries that stem and the format has to stop
prepending the `v`. `credential_chain` is the ordinary AWS resolution — profile, instance
metadata, SSO; against another endpoint pass `KEY_ID`, `SECRET`, `ENDPOINT` and
`URL_STYLE 'path'` instead, which is what the library's own reader emits.

**Reading it as it grows.** `sync` is lazy, restartable and arbitrarily far behind, and no read
depends on it, so a reader sees a committed snapshot that lags the writer — never a partial
one. `litelink_offset` is what makes that safe to poll: it is monotonic and never reused, so a
reader keeps the highest one it has seen and asks for what came after.

```sql
-- first pass: whatever is there, and where it ended
SELECT max(litelink_offset) FROM iceberg_scan(...);          -- 1536

-- every pass after: only what the syncs since have published
SELECT * FROM iceberg_scan(...) WHERE litelink_offset > 1536;
```

Resolve the table on each pass rather than caching a metadata path — the hint moves with every
commit, and a pinned pointer serves one stale snapshot for ever. What this reader cannot see is
anything newer than the last `sync()`: rows still in the buffer or the local table are on the
writing box alone, so its freshness lever is the sync interval.

That last sentence used to say "rather than anything at the reader", which `litelink.follow` below
makes false — with a WAL sidecar there *is* a lever at the reader.

`tests/test_archive.py::test_the_archive_reads_as_a_directory_with_no_catalog_at_all` is that
claim as a test — it captures through a live archive and asserts the DuckDB row count equals
the writer's `archived_through()`.

### Fresher than the archive: `litelink.follow`

When the writer runs a WAL sidecar (`wal_replication`, §3a), a reader can do better than the
last `sync()`. `litelink.follow` restores the writer's `buffer.db` from its replica, adopts the
archive beside it, and merges the two — so freshness falls to the replication lag.

```python
litelink.follow(name, *, archive, s3=None, binary=None, scratch_dir=None) -> RemoteReadHandle
```

```python
with litelink.follow("trades", archive="s3://bucket/prefix", s3=opts) as reader:
    reader.coverage()        # what it can serve, and where it cannot
    reader.end_offset()      # compare against the primary's to measure staleness
    reader.scan(where="side = 0")
    reader.sql("SELECT side, count(*) FROM log GROUP BY side")
```

`archive` is where the *WAL replica* lives; the archive prefix itself comes from the replica's
own `meta`. It requires a published archive — a log before its first successful sync cannot be
followed, because serving the buffer alone would silently omit every archived row.

**This returns a `RemoteReadHandle`, a sibling of the `LocalReadHandle` that `open(..., read_only=True)` returns.** It holds the read
collaborators — the replicated buffer, the local table, the archive handle, and the reader
over them — so `append`, `seal`, `sync`, `compact` and `evict` are *absent* rather than
raising: the writer machinery is not built at all.

What makes it behave as a follower is state, not type. Its local table is empty by
construction and its archive holds rows, so the archive is load-bearing: reads include it
automatically, and `include_archive=False` is **refused** rather than answered short. A local
reader in the same state — a fully evicted log — gets the same treatment, correctly.

`replication_config`, `write_replication_config` and `databases` live on
**`LocalReadHandle`**, not on the base: generating a sidecar config is exactly what you do
beside a live writer, and a local handle shares the primary's root and name so the key it
emits is the primary's own correct one. A followed log does not have them at all — it raises
`AttributeError`, because `RemoteReadHandle` is a sibling rather than a child.

That split is the point. `litestream_config` keys each replica on the path relative to the
root, so a follower emitting one would name the **primary's** key, and a sidecar run there
would ship its scratch copy over the primary's only off-box record of its unsealed rows.

**A snapshot, not a subscription.** litestream restores to a point in time, so refreshing means
assembling another follower — exit the block and reopen. The root is always a temporary
directory the follower owns and deletes on close; there is no `root` parameter, because
aiming a follow at a caller's directory could land it on a root that already held a live log
and could leave a stale `archive.db` that wins over the bucket's own hint. (It once also
risked colliding with a live log's shared catalogs; those are per-stream since 0.2, so the
stale-hint hazard is the whole of it now.) `scratch_dir` places the temporary
one somewhere other than `/tmp`, which is often memory-backed and which the restored buffer
can outgrow.

Reads refuse rather than lie once the primary has committed past the pinned snapshot. The
archive keeps `previous-versions-max: 10` — ten previous versions beside the current one — so
the **eleventh further archive commit** is enough, and **there is no time component**. Archive
commits are `sync`, `rewrite_archive` and archive expiry; a local `maintain()` moves nothing in
the bucket. A primary syncing steadily can sweep a follower in seconds. The error says to
reassemble.

```python
Coverage(archive=(1, 1928), buffered=(1929, 2100), gap=None, wal_replication=True)
```

`coverage()` reports; it does not adjudicate. A follower cannot ask the primary anything, so the
failure it avoids is silence. The gap it reports sits above the archive's frontier and below
the next thing the replica knows of — the buffer's first offset when it holds rows, the
sequence's end when it does not, so an empty replica still reports the band it cannot serve.
**A gap there is not necessarily loss**:
it is either rows the buffer discarded at seal with replication off, or a `litelink.restore` fence,
which burns 2**20 offsets in exactly that position — and nothing local tells the two apart. A
caller who knows whether their log has failed over can read a gap that this cannot.

A `start_offset` reserve never appears here: it lies below the archive's low end, which this
does not compare against.

## Sealing

```python
log.seal_due() -> int | None        # drain what the policy queued
log.seal() -> int | None            # cut everything buffered, now
log.await_seal(timeout=None) -> bool
```

**The cut is chosen by the appender**, in the transaction that crosses `target_seal_size` or
`target_seal_rows`, and queued. `seal_due` drains that queue and returns the last exclusive
end offset, or `None` if there was nothing. It is an indexed read of one row when idle, so it
is cheap to call often.

`seal()` cuts short by definition and leaves an undersized file for compaction to merge. It is
for shutdown and for tests, not for a loop.

**Nothing seals unless something calls one of these.** The library owns no thread and no
interval; a writer running alone accumulates in SQLite indefinitely — durable and readable the
whole time, but never reaching Parquet.

## Maintenance

```python
log.maintain() -> None
log.compact(heartbeat=None) -> None
log.evict(heartbeat=None) -> None
log.expire(heartbeat=None) -> None
```

`maintain()` is the one call most deployments want: it takes the maintenance claim once, runs
compaction, eviction and expiry in that order, then calls `seal_due()` at the end. It needs no
archive — this is the call that makes `local_retention` mean anything on a local-only log.

The three are exposed separately because their costs differ by an order of magnitude:
**`compact` reads and rewrites whole files while `evict` and `expire` are metadata commits
that finish in milliseconds**, so a deployment wanting them on different schedules can have
that. `heartbeat` is a `Callable[[], bool]` the pass consults to decide whether to keep going,
which is how a long compaction yields to something more important.

Each is a no-op or a regression without the others: compaction alone increases storage,
eviction alone frees no disk, and expiry is what actually deletes bytes.

## Archive

```python
log.sync() -> None
log.hydrate(since) -> None            # since: timedelta
log.rewrite_archive() -> None
```

`sync` uploads data files, registers them into the archive, replicates compactions, and
records the watermark (§5). It is lazy, restartable and arbitrarily far behind, and **no read
depends on it**. All three raise `ValueError` on a log with no archive, and `RuntimeError`
when another owner holds the claim.

`hydrate(since)` re-registers archived files back into the local table. Raising
`local_retention` is an operation rather than a config change: without this, a raised setting
applies only to data captured afterwards. `since` is measured against when the archive took
each file.

`rewrite_archive` merges undersized files already in the archive. An operation, not a policy —
nothing calls it on a schedule, and normal operation does not need it, because sync pushes
only files compaction has finished with. It exists for the two things that break that on
purpose: an explicit `seal()` stranding a small file, and a change to `target_compact_size`.

## Observing

```python
log.end_offset() -> int                      # EXCLUSIVE upper bound: what the next append gets
log.buffered_rows() -> int                   # durable, not yet sealed
log.table_rows() -> int
log.table_files() -> int                     # what compaction is bringing down
log.table_extent() -> tuple[int, int] | None # (lo, hi) from manifest statistics
log.archived_through() -> int                # highest offset the archive holds, 0 if none
log.archive_files() -> int
```

All local and none of them opens a data file, with one exception: on a **read** handle whose
local table holds nothing while its archive holds rows — a followed log, or a local one
evicted dry — `end_offset()` and `coverage()` read the archive's metadata, because the
buffer's sequence is not authoritative there. `sqlite_sequence` never lowers, so it keeps
counting rows a seal discarded, and taking it at face value made a followed log claim 1,501
while serving 864.

**A `WriteHandle` never does this.** It overrides `end_offset()` to read `sqlite_sequence`
alone, because a writer is asked where its next row will land rather than what it can serve —
so the number an operator alarms on cannot fail during an object-storage outage.

`archived_through()` against `end_offset()` is
the sync lag, which is the number to alarm on: eviction may never precede registration (I4),
so a stalled sync stalls eviction, and the local file count grows until seals feel it.

## Configuration

```python
log.config -> LogConfig
log.set_config(config) -> None
log.archive -> str | None
log.set_archive(archive) -> None              # None detaches
log.schema -> pa.Schema                       # your columns, as declared at new()
log.sort_by -> tuple[str, ...]
log.set_sort_by(sort_by, *, rewrite) -> None
```

**Everything `new` took, the log gives back**, which is what lets `open` take none of it.
`schema` strips `litelink_offset`, so it is the schema you wrote and the one `append` accepts;
`sort_by` is the one §7 tells you to bound every scan on a leading column of, which is advice
no caller can follow without being able to ask.

`sort_by` reads `meta` on every access, like `config` and `archive`. That is not a detail of
the getter: the seal, compaction and the archive's own declaration all read the same one
place, so `set_sort_by` in one process cannot leave a maintainer in another clustering files
by the key it happened to open with.

**There is exactly one copy of the policy, and it is a row in SQLite.** Every decision reads
it from there rather than from memory, so `set_config` in one process is seen by the writer's
next append and the maintainer's next pass, and nothing can hold a stale one.

`set_config` and `set_archive` take the whole-log claim, so they cannot interleave with a
sync, a merge or an eviction. Both wait for maintenance rather than failing on the first try,
because the shipped writer calls `set_archive` on every restart while a maintainer runs
elsewhere.

**`set_archive(None)` is refused while a retention floor is set.** Detaching retires the I4
clamp for every process at once, and eviction would then delete files still queued for upload.

`set_sort_by` re-clusters what the local table owns, so `rewrite` must be passed explicitly
and `rewrite=False` raises `ValueError` naming the cost you have not accepted. It runs under
the maintenance claim, because a rewrite *is* a compaction.

**It does not re-cluster the archived prefix.** A local rewrite there would commit a file
straddling the archive's extent, and nothing re-cuts a local straddler — so on a synced log a
re-sort changes the declarations and rewrites only what `sync` has not yet taken. Archived
data keeps the clustering it was written with, which is §6's "sealed once and never
rewritten" applied to history. `rewrite_archive` is not the other half: it re-ingests from
the first badly-*sized* file onwards, so a well-sized archive is never a candidate.

Passing the order the log already has, with `rewrite=True`, is not a no-op — it is how a
re-sort that died after the `meta` write is finished, since that crash leaves the
declarations correct and the files not.

### LogConfig

```python
target_seal_size      int             = 8 MiB    uncompressed bytes per SEAL
target_seal_rows      int | None      = None     the other ceiling; whichever is hit FIRST
target_compact_size   int | None      = None     bytes per FILE (None = 8x the seal)
target_compact_rows   int | None      = None     rows per compacted file (None = 8x)
local_retention       timedelta|None  = None     local window by TIME (None keeps everything)
local_rows            int | None      = None     local window by ROWS — a floor, not a ceiling
snapshot_retention    timedelta       = 1 hour   how long expired snapshots survive
compact_min_files     int             = 4        minimum adjacent files to merge
wal_replication       bool            = False    needs an archive; also makes a seal KEEP its rows
wal_retention         timedelta|None  = None     how far back a restore may go
```

Frozen dataclass, with `to_json`/`from_json` and two derived properties, `compact_size` and
`compact_rows`. `sort_by` is deliberately not in here — everything above governs future work
only, so `set_config` needs no rewrite.

Sizing is two targets, not one, and §7 and §12 are where that argument lives. Validation is at
construction: `compact_min_files` below 2, a compact size below the seal size, `wal_retention`
without `wal_replication`, and `wal_replication` without an archive are each refused.

## Replication

```python
log.databases -> tuple[Path, ...]
log.replication_config() -> str
log.write_replication_config() -> Path
WriteHandle.replication_config_for(root, name, archive, s3=None, retention=None) -> str
```

litelink does not run the sidecar — it says what the config has to name. `databases` is the
set that carries the log's state: `buffer.db`, `catalog.db`, `archive.db`. Omitting one is
silently wrong, which is why this is generated rather than written by hand.

`replication_config_for` is the classmethod form, for a log that does not exist here yet —
which is the chicken-and-egg a restore has to solve.

**One sidecar per stream.** All three databases live in the stream's own directory and
replicate to `<prefix>/<name>/_wal`, so a root holding several streams runs one sidecar each.
`write_replication_config()` writes the config for the stream its handle is open on, into
`<root>/<name>/litestream.yml` — so a multi-stream root means calling it once per stream, and
each config is complete on its own rather than needing to be merged by hand.

Before 0.2 it was one sidecar per *root*: `catalog.db` and `archive.db` were shared, so a
sidecar per log would have run two litestream instances against one database, and a
multi-stream root needed a config written by hand.

### The sidecar needs a monotonic clock

litestream measures an interval and adds it to a Prometheus counter, and `Counter.Add` panics
on a negative value. So **one backwards tick of `CLOCK_MONOTONIC` kills the process**, and
under `Restart=always` that is a crash loop
([litestream#1488](https://github.com/benbjohnson/litestream/issues/1488)).

That is a durability failure rather than an inconvenience. On a stream that never reaches
`target_seal_size`, the buffer holds the only copy of those rows until it does (§3a) — which
is the case `wal_replication` exists for.

**The symptom lies, so check restarts rather than log lines.** The sidecar logs `replica sync`
and `ltx file uploaded` right up to each panic, so a minute of watching shows healthy
replication. Nothing in litelink looks wrong either, because the *log* is fine; only the
sidecar is dying.

```bash
systemctl --user show <your-litestream-unit> -p NRestarts --value
cat /sys/devices/system/clocksource/clocksource0/current_clocksource
```

The known trigger is a virtual machine using the `tsc` clocksource, where the TSC is not
guaranteed synchronised across vCPUs — a KVM guest booted with `clocksource=tsc` is the
reported case, at 4 regressions in 45 seconds and 13 panics in 15 minutes. Switching to
`kvm-clock` fixed it; make the change persistent, because a sysfs write does not survive
reboot.

`python -m litelink` warns when it sees that combination:

```
WARN  host clock: clocksource tsc on a kvm guest. If CLOCK_MONOTONIC regresses here,
      litestream panics and crash-loops — and it logs successful syncs up to each panic,
      so check restarts, not log lines. A paravirtualised source is available: kvm-clock.
```

It warns rather than fails, and deliberately does **not** sample the clock to decide. Spinning
for a second counting backwards steps was measured on a KVM guest running `tsc` — the affected
configuration — at 105 million samples over 20 seconds with zero regressions, while the
reporter's host showed 4 in 45 seconds. A sampling check would print PASS on hardware that can
still crash-loop, and false confidence is worse than no check. What it reports is the risky
combination, which is a fact rather than a sample.

## Schema evolution

```python
log.add_column(name, type_) -> None
log.rename_column(old, new, *, breaking_ok) -> None
log.drop_column(name, *, breaking_ok) -> None
```

**All three raise `NotImplementedError`.** They are specified in §9 and the signatures are the
contract that will hold when they land: `breaking_ok` is explicit because Iceberg resolves by
field ID, so no file is rewritten and no engine's SQL is rewritten either — `SELECT qty` breaks
the moment the column becomes `quantity`, and the format will not stop you, so the API has to.

All three refuse `litelink_offset` before they raise, which is where I11 is enforced against
the second way a caller could reach it.

## Rules that cut across

**A reader has nothing that writes**, rather than write methods that refuse. `extend`,
`append`, `seal`, `seal_due`, `maintain`, `compact`, `evict`, `expire`, `sync`, `hydrate`,
`rewrite_archive`, `set_config`, `set_archive` and `set_sort_by` are absent from `LogHandle`.
Everything observational and both read paths are there.

This is the difference from the older `Log.open(read_only=True)`, which returned ONE class
whose thirteen write methods existed and raised `RuntimeError("this Log was opened
readonly")`. That is the shape Python's own `open()` has at runtime, where a read-mode file
carries a `.write` that raises `UnsupportedOperation` — but typeshed types the *constructor*
with overloads, and so does this, so the misuse is caught before it runs.

**One writer per log.** SQLite's write lock is per file and one process per stream is the
intended topology; multiple machines write separate logs and readers union.

**The claim decides who does the work, not the caller.** `maintain`, its three passes, `sync`,
`hydrate`, `rewrite_archive` and the two setters all coordinate through rows in SQLite, so a
second caller is refused with `RuntimeError` rather than duplicating the work — and that holds
between threads and between processes on identical terms.

**Nothing runs on a timer.** Size ceilings are enforced synchronously inside the append
transaction; every other knob is a predicate evaluated when the relevant pass runs. Your loop
is the schedule.
