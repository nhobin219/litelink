# API

Everything public, on one page. [`SPEC.md`](SPEC.md) says what the system is and
[`RUNTIME.md`](RUNTIME.md) says how it runs; this says what you can call.

```python
from litelink import Log, LogConfig, Row, S3Options, __version__
```

Those are the exports. `Log` is the whole object model — there is no session, no client, no
catalog handle to hold. A log is a directory under a root, named at `Log.new` and opened by
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

## The whole surface

| | |
|---|---|
| **lifecycle** | `new` · `open` · `restore` · `recover` · `recovery` · `close` |
| **write** | `append` · `extend` |
| **read** | `scan` · `sql` |
| **seal** | `seal_due` · `seal` · `await_seal` |
| **maintain** | `maintain` · `compact` · `evict` · `expire` |
| **archive** | `sync` · `hydrate` · `rewrite_archive` |
| **observe** | `end_offset` · `buffered_rows` · `table_rows` · `table_files` · `table_extent` · `archived_through` · `archive_files` |
| **configure** | `config` · `set_config` · `archive` · `set_archive` · `schema` · `sort_by` · `set_sort_by` |
| **replicate** | `databases` · `replication_config` · `write_replication_config` · `replication_config_for` |
| **schema** | `add_column` · `rename_column` · `drop_column` — all three raise `NotImplementedError` |

Most deployments use six: `new`/`open`, `extend`, `scan`, `seal_due`, `maintain`, `sync`.

## Lifecycle

```python
Log.new(root, name, *, schema, sort_by=None, config=None, archive=None, s3=None) -> Log
Log.open(root, name, *, read_only=False, s3=None) -> Log
Log.restore(root, name, *, archive, s3=None, binary=None) -> Log
```

**`new` takes the shape; `open` takes none of it.** Schema, sort order, config and archive
live in the log and come back from it, so nothing at the call site can disagree with what is
on disk. `new` raises `FileExistsError` if a log is already there, and `ValueError` if the
archive it is pointed at holds data this log has no record of pushing — that archive belongs
to another log.

`sort_by` is set here and only here; it is a read-shape decision rather than a knob (§7), and
changing it later rewrites every file. `schema` is your columns — the library prepends
`litelink_offset` itself, and refuses a schema that declares it (I11).

**`open` recovers before it returns**, finishing whatever a crash interrupted. It raises
`FileNotFoundError` for a log that is not there, and `ValueError` for one whose stored config
or sort order is missing — a log that exists but is corrupt.

**`read_only=True` opens a second view alongside a live writer.** Any number of processes may
hold one. They take no lease, mutate nothing, and coordinate with nobody. Reading in the
*same* process as the writer is the case to avoid — see RUNTIME.md on two SQLite libraries in
one process.

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
owns no thread. `Log` is a context manager, and `with` is the same thing.

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

## Reading

```python
log.scan(*, columns=None, where=None, start_offset=None,
         end_offset=None, include_archive=False) -> pa.RecordBatchReader
log.sql(query, *, include_archive=False) -> pa.RecordBatchReader
```

`scan` unions the tiers and bounds each by its neighbour's committed offset extent, resolved
from manifest statistics at query time (§7, I3). The tiers overlap by design; the bounds are
what make each row appear exactly once.

**`include_archive=False` by default**, because a hot read is local disk only and must stay
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
FROM iceberg_scan('s3://bucket/prefix/litelink/trades',
                  version_name_format = '%s%s.metadata.json');
```

The table sits at `<archive prefix>/litelink/<log name>`. **`version_name_format` is not
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
commit, and a pinned pointer serves one stale snapshot for ever. What a reader cannot see is
anything newer than the last `sync()`: rows still in the buffer or the local table are on the
writing box alone, so the freshness lever is the sync interval rather than anything at the
reader.

`tests/test_archive.py::test_the_archive_reads_as_a_directory_with_no_catalog_at_all` is that
claim as a test — it captures through a live archive and asserts the DuckDB row count equals
the writer's `archived_through()`.

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

All cheap, and none of them opens a data file. `archived_through()` against `end_offset()` is
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

`set_sort_by` re-clusters every existing file, so `rewrite` must be passed explicitly and
`rewrite=False` raises `ValueError` naming the cost you have not accepted. It runs under the
maintenance claim, because a rewrite *is* a compaction.

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
Log.replication_config_for(root, name, archive, s3=None, retention=None) -> str
```

litelink does not run the sidecar — it says what the config has to name. `databases` is the
set that carries the log's state: `buffer.db`, `catalog.db`, `archive.db`. Omitting one is
silently wrong, which is why this is generated rather than written by hand.

`replication_config_for` is the classmethod form, for a log that does not exist here yet —
which is the chicken-and-egg a restore has to solve.

**One sidecar per root**, not per log: two of the three databases are shared by every log under
the root. A root holding several logs needs one config naming every buffer under it, which
this does not generate.

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

**`read_only=True` refuses anything that writes** with `RuntimeError`: `extend`, `append`,
`seal`, `seal_due`, `maintain`, `compact`, `evict`, `expire`, `sync`, `hydrate`,
`rewrite_archive`, `set_config`, `set_archive`, `set_sort_by`. Everything observational and
both read paths stay available.

**One writer per log.** SQLite's write lock is per file and one process per stream is the
intended topology; multiple machines write separate logs and readers union.

**The claim decides who does the work, not the caller.** `maintain`, its three passes, `sync`,
`hydrate`, `rewrite_archive` and the two setters all coordinate through rows in SQLite, so a
second caller is refused with `RuntimeError` rather than duplicating the work — and that holds
between threads and between processes on identical terms.

**Nothing runs on a timer.** Size ceilings are enforced synchronously inside the append
transaction; every other knob is a predicate evaluated when the relevant pass runs. Your loop
is the schedule.
