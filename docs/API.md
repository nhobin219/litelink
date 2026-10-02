# API

Everything public, on one page. [`SPEC.md`](SPEC.md) says what the system is and
[`RUNTIME.md`](RUNTIME.md) says how it runs; this says what you can call.

```python
import litelink
from litelink import LogConfig, LogHandle, Row, S3Options, WriteHandle, __version__
```

Those are the names most code takes; `litelink` also exports `new`, `open`, `restore`,
`validate_row`, `preflight`, `duckdb_connection`, `install_s3_secret`, `LocalReadHandle`,
`Coverage`, `RetiredError`, `ExtensionMissing`, `OFFSET`, the statistics types
(`ColumnStatistics`, `TierStatistics`, `Tier`), the preflight report types (`Check`, `Report`)
and the `manifest` module. The handles are the whole object model — there
is no session, no client, no catalog handle to hold. A log is a directory under a root, named
at `litelink.new` and opened by that name for ever after.

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
    └── WriteHandle          + append · seal · maintain · publish · retire · set_*
```

**Nothing inherits a method it has to refuse**, which is the property two earlier shapes kept
failing. A `Follower` subclassing a writable log carried `append`, `seal` and `publish` that only
raised; a `read_only` flag did the same to thirteen methods.

```python
litelink.new(root, name, *, schema, sort_by=None, config=None, published=None,
             s3=None, start_offset=1)                    -> WriteHandle
litelink.open(root, name, *, s3=None)                    -> WriteHandle
litelink.open(root, name, *, read_only=True, s3=None)    -> LocalReadHandle
litelink.restore(root, name, *, published, s3=None, binary=None) -> WriteHandle
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

### Every handle is on the primary

A handle needs a root **on this machine**: the log's own directory, its live `buffer.db`, and
its staging table filling as it seals. The published table is read from there too, when a scan
asks for it. There is no handle for a log running somewhere else — `snapshot` and its
`RemoteReadHandle` were removed (#90). Another machine reads the published table with any
Iceberg engine; see [Reading from another machine](#reading-from-another-machine).

```python
# On the writer's box: a live second view. No published table argument — the log
# already records where its published table is, and reads come off local disk.
with litelink.open("data", "trades", read_only=True) as r:
    r.scan(start_offset=r.end_offset() - 100)   # recent: local disk only
    r.scan()                          # the whole history, including the published table
    r.coverage()                      # what each tier holds
    r.write_replication_config()      # its replica key IS the primary's (s3:// logs only)
```

On a fully evicted log — its staging table empty and its published table holding rows — any
query that reaches below the buffer reads the published table, since nothing else holds those
rows. `published=False` there returns the buffer alone, which is what it asks for.

## The whole surface

Each row is what that class **adds** to the one above it. A test pins the `LogHandle` and
`LocalReadHandle` sets exactly.

| | |
|---|---|
| **`LogHandle`** — read | `scan` · `sql` |
| **`LogHandle`** — observe | `end_offset` · `buffered_rows` · `staging_rows` · `staging_files` · `staging_extent` · `published_through` · `published_files` · `coverage` · `column_statistics` |
| **`LogHandle`** — identity | `root` · `name` · `config` · `schema` · `sort_by` · `published` |
| **`LogHandle`** — lifecycle | `close` · context manager |
| **`+ LocalReadHandle`** | `databases` · `replication_config` · `write_replication_config` |
| **`+ WriteHandle`** — write | `append` · `extend` · `ingest` |
| **`+ WriteHandle`** — seal | `seal` · `await_seal` |
| **`+ WriteHandle`** — maintain | `advance` · `seal` · `compact` · `publish` · `evict` · `reclaim` · `sweep` |
| **`+ WriteHandle`** — published table | `publish` · `compact("published")` · `retire` |
| **`+ WriteHandle`** — configure | `set_config` · `set_sort_by` |
| **`+ WriteHandle`** — recover | `recover` · `recovery` |

`await_seal` is deliberately a `WriteHandle` method: it *helps* drain the queue each round
rather than only watching, and a reader could only watch.

Most deployments use six: `new`/`open`, `extend`, `scan`, `seal`, `advance`, `publish`.

**Import everything from `litelink`.** The package root and `litelink.manifest` are the
public modules; every other module is private (`_handle`, `_buffer`, …). `litelink.OFFSET` is
the name of the one column litelink owns, `"litelink_offset"`, for code that bounds or reads it
without spelling the string.

**Every offset range litelink reports is half-open, `[start, end)`**: `coverage()`,
`staging_extent()`, what `ingest()` returns, `recovery().skipped`, the tier offsets
`litelink.manifest` prunes on. `end` is the offset after the last one, so a range's length is
`end - start` and an empty one is `start == end`.

**The names carry the convention.** `start`/`end` is a half-open range: an `end` is never an
offset you hold, which is why `end_offset()` is the offset the next append gets. `through` is a
single offset you do hold, the last one: `published_through()`, and the `through` a retirement
records. Nothing is named `lo`/`hi` or `min`/`max` for a range, because those read as two values
the range contains.

## Lifecycle

```python
litelink.new(root, name, *, schema, sort_by=None, config=None, published=None, s3=None,
             start_offset=1) -> WriteHandle
litelink.open(root, name, *, s3=None) -> WriteHandle
litelink.open(root, name, *, read_only=True, s3=None) -> LocalReadHandle
litelink.restore(root, name, *, published, s3=None, binary=None) -> WriteHandle
```

**`new` takes the shape; `open` takes none of it.** Schema, sort order, config and published table
live in the log and come back from it, so nothing at the call site can disagree with what is
on disk. `new` raises `FileExistsError` if a log is already there, and `ValueError` if the
published table it is pointed at holds data this log has no record of pushing — that published table belongs
to another log.

`start_offset` is set here and only here too, and leaves `[1, start_offset)` unassigned
for ever. It aligns a log's offsets with a sequence something else owns, and reserves room a
later backfill can fill (§13). There is deliberately no way to re-seed a log afterwards: the
guard that would refuse it reads the offsets currently BUFFERED, which a seal empties, so it
cannot tell an unused offset from an issued one.

`sort_by` is set here and only here; it is a read-shape decision rather than a knob (§7), and
changing it later rewrites the staging table's files — see `set_sort_by` for what that does not
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
Iceberg table *empty*, adopts the published table through `version-hint.text`, and reserves an offset
window so nothing the dead machine served is reissued. `binary` names the litestream
executable if it is not on `PATH`. It refuses a root that already holds this log
(`FileExistsError`) or whose `litestream.yml` replicates a different one. Split-brain is not
detected — if the primary is alive you now have two writers on one published table.

```python
log.recover() -> None          # idempotent; `open` already did it
log.recovery() -> _Recovery | None
log.close() -> None
```

`recovery()` returns what a `restore` recovered — `recovered`, `resumed_at`, and `skipped` as
`[start, end)` — and `None` on a log opened normally. Those numbers are available nowhere else afterwards.

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

### Checking a row without appending it: `validate_row`

```python
litelink.validate_row(schema: pa.Schema, row: Row) -> None
```

Raises exactly what `append(row)` on a log of that schema would raise — the same exception
and the same message, naming the column — and returns `None` for a row it would accept. It
needs no log and writes nothing, so a caller that only sometimes has a log holds every row to
the same rule either way. Pass the schema `new` took, or `log.schema` for an existing log.

The rules are the buffer's own: the check inserts into a private in-memory copy of the
buffer table and rolls back. Building that copy costs ~260 µs once per schema, after which a
row costs ~4 µs.

### Column types

| Arrow type | append this |
|---|---|
| `int32`, `int64` | `int` |
| `float32`, `float64` | a finite `float`, or an `int` it holds exactly — never NaN or ±inf |
| `bool` | `bool` |
| `string`, `large_string` | `str` |
| `binary`, `fixed_size_binary(n)` | `bytes` — exactly `n` of them for a fixed width. Small values only, see below |
| `struct<…>` | a mapping of declared field names; an absent field is null |
| `map<K, V>` | a mapping, or a sequence of `(key, value)` pairs; keys are strings or integers |
| `list<T>` | a `list` or `tuple` |

**`binary` is for small values — identifiers, hashes, short encoded fields — not payloads.**
Its bytes go through the buffer like any other value: fsynced into SQLite, shipped by the WAL
sidecar, converted by every hot read, and counted against `target_seal_size`, so one large
value makes a file of a row or two. Frames, point clouds and response bodies are what SPEC §15's
blob fields are for, which bypass the buffer; they are specified and not yet built.

**A log holds only finite floats.** NaN and ±inf are refused on every write path — `append`,
`extend`, `ingest` and `validate_row`, top-level and nested, float32 and float64 — naming the
column and the path inside it. NaN because readers disagree about it (Iceberg's and Parquet's
statistics leave it out, so whether a query sees it depends on what shares its file), ±inf
because JSON, the wire above, has none. Represent a missing value as None.

An application that must keep NaN or ±inf can carry them beside the number. One way, not a
rule — the representation is the application's to choose:

```python
schema = pa.schema([
    pa.field("reading", pa.float64()),          # the finite values; prunable
    pa.field("reading_special", pa.string()),   # "nan" | "inf" | "-inf", else None
])

log.append({"reading": 123.456, "reading_special": None})
log.append({"reading": None, "reading_special": "inf"})
```

Two top-level columns rather than a struct, because fields inside a struct carry no file-level
statistics and `reading` would stop pruning; where the float is already nested, as in OTel's
`AnyValue`, a struct field loses nothing. Spelled as Python's `repr` spells them, so
`float(reading_special)` reads one back. litelink does not enforce the pairing — nothing stops
a row from setting both or neither — so that invariant is the application's.

Nested types nest in each other. Anything else is refused at `new` with the reason — unsigned
integers, time types, `large_binary`, `large_list` and unions among them. Iceberg has no union,
so a variant value such as OTel's `AnyValue` is a struct with one nullable field per variant.

A nested value is checked in Python against its declared type, since SQLite cannot see inside
one, and stored as JSON: about 30 µs per row for an OTel body and a four-entry attribute map,
paid only by schemas that have nested columns. Fields inside one carry no file-level
statistics — pyiceberg records none — so an attribute you filter on often belongs in a
top-level column, where it prunes.

### Bulk loading: `ingest`

```python
log.ingest(source: pa.Table | pa.RecordBatchReader, *,
           publish: bool = True) -> tuple[int, int] | None
```

Writes Parquet directly and never puts the rows through SQLite. Returns the offsets it
assigned as `[start, end)`, or `None` for a source with no rows.

**It pushes its own output to the published table**, compacting first, and
`publish=False` opts out. Those rows never enter the buffer, so WAL replication cannot carry them
and the published table is their only second copy — while an ordinary `publish` holds a load's short last
file back behind `stable_prefix` for a merge a quiet stream never earns. If the push fails the
load is still durable and the error says so; retry the push, never the load, which would
reserve a fresh range and duplicate it.

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

**WAL shipping does not carry a loaded range**, because these rows never enter the buffer.
That is a fact about scope, and it is stated rather than enforced: `ingest` runs with
`wal_replication` on, and turning it off to load would be worse, since `evict("buffer")`
reads the same flag and the next eviction would drop the buffer's copy of everything already
captured.

**The published table is a loaded range's only second copy.** When the push did not run, compare
`published_through()` against the `end - 1` this returns; until they meet, the corpus you
loaded from is the range's second copy.
`publish` lags the tail on purpose — it holds back a trailing run still under
`target_compact_size` — and the run settles once roughly another
`target_compact_size` of rows sits above it, whether from capture or from the
next `ingest`.

**A load that fails costs its reservation.** The offsets of the file being written are gone,
leaving a gap. Files stay non-overlapping and adjacent in offset order, which is what §6
needs; the one price is that compaction will merge across a gap and such a file can never be
re-cut by `compact("published")`.

## Reading

```python
log.scan(*, columns=None, where=None, start_offset=None,
         end_offset=None, published=True) -> pa.RecordBatchReader
log.sql(query, *, published=True) -> pa.RecordBatchReader
```

`scan` unions the tiers and bounds each by its neighbour's committed offset extent, resolved
from manifest statistics at query time (§7, I3). The tiers overlap by design; the bounds are
what make each row appear exactly once.

**Which tiers a query reads is decided per query, and the caller need not name one** (#90).
Every query reads the buffer. The staging table and the published table are read only when their
per-column bounds say they could hold a row the query matches:

- **the staging table's** are the rollup of the snapshot the read resolved. The process that
  commits a new version stores its rollup in `buffer.db`, stamped with the version, so other
  processes read it instead of rolling it up (2–3 ms at 1–64 files). A read uses it only
  when the stamp is the version it resolved, and rolls its own version up otherwise;
- **the published table's** describe what it holds **below** the staging table — what eviction moved
  there — and are kept in `buffer.db`, because on S3 they would cost the round trip the
  decision exists to avoid. The whole published table's bounds would include its copy of the staging
  window and send every hot query to the network.

Neither touches the network, so a read bounded inside the staging window stays there (I5).

**`published=False` keeps a read off the published table whatever it asks for.** It reads the staging
table and the buffer only, and rows only the published table holds are left out rather than refused —
for a caller holding replays to the local floor, which `coverage(published=False)` reports
without the network either.
The two rows are in streamcast's manifest format (streamcast#27) with `tier` as the key, and
the decision is `litelink.manifest.prune`, public so streamcast uses the same one.

**`litelink_offset` is each tier's range, not a statistic.** It is the log's own sequence,
dense and monotonic across the tiers, so a term on it is judged against each tier's
`[start_offset, end_offset)`. The published table's range is stored in its own table (`tier_offsets`),
apart from its column statistics. It is also what prunes **the buffer**, which has no
statistics: its range starts at its lowest offset and is open above, so a scan that ends below
the buffer never converts its rows. Without a term on a column the staging tier's rollup is not
computed at all, since only a tier known to be empty could be skipped.

It reads the query's WHERE, and narrows on one shape only: a single `SELECT … FROM log`, no
joins, CTEs, set operations or subqueries, with comparisons (`=`, `<`, `<=`, `>`, `>=`,
`BETWEEN`, `IN`) between a column and a constant, AND-ed together. Integer, float and boolean
columns have bounds; strings, bytes and nested columns do not. Any other part of the WHERE
constrains nothing, and any other shape reads the published table — correct, and only slower. `scan`
builds that shape, with `start_offset`/`end_offset` as offset comparisons.

**This reverses 0.4.0**, which fixed the tiers at assembly with `include_archive` and
`with_archive()`. Both are gone. The trade is stated plainly: latency now follows the
predicate rather than the handle, so the same unbounded query reads the published table once eviction
has moved its rows there. A bounded hot query stays local however much has been evicted.

The published table's row changes when eviction moves rows below the staging table: eviction widens it
before its commit. `publish` and `compact("published")` leave it alone. It is recomputed from the
published table's manifests at the first `publish`, on a re-point, at `restore`, and at `open` for a log
written before it existed. With no row, the published table is read.

`sql` is the same relation under arbitrary DuckDB SQL, exposed as `log`. Both return a
streaming reader rather than a table: a full-window read with a 400-byte payload column is
611 ms and proportional to the data, so materialising it is the caller's choice.

**Always bound on a leading column of `sort_by`.** §7 measures a non-leading predicate at
119 ms against 13 ms for the same predicate with a leading bound.

**Offset bounds are the only ones that can skip the buffer.** `start_offset`/`end_offset` (or
a `litelink_offset` comparison in `where` or `sql`) are judged against the buffer's offset
range, from its lowest buffered offset up, so a scan ending below it never reads the buffered
rows. The buffer keeps no column statistics, so a bound on any other column still reads it;
the staging table and the published table are skipped by either kind.

Other machines do not use this API at all — see below.

## Reading from another machine

The API above is the *writer's* read: local disk, all three tiers, no network. Every other
machine reads the published table instead — on S3, since a local one is on the writer's disk —
and needs nothing from litelink to do it. The published table is an ordinary Iceberg table that publishes `version-hint.text` at every commit, so an engine pointed
at the prefix resolves the current metadata itself — no catalog service, no `published.db`, no
local root, no litelink install.

```sql
INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;
CREATE SECRET (TYPE s3, PROVIDER credential_chain);

SELECT count(*), max(litelink_offset)
FROM iceberg_scan('s3://bucket/prefix/trades',
                  version_name_format = '%s%s.metadata.json');
```

**With litelink installed, `litelink.duckdb_connection` does the provisioning** — from the
extensions litelink's platform wheels bundle, so the first read is not a network fetch (§7):

```python
litelink.duckdb_connection(
    s3: S3Options | None = None,
    *,
    remote: bool = False,
    memory_cache: bool = True,
    disk_cache: bool = False,
    cache_key: str | PathLike | None = None,   # under ~/.cache/litelink; None: "default"
    disk_cache_volume_limit: float = 0.8,
) -> duckdb.DuckDBPyConnection
```

It loads `avro` and `iceberg`; `remote=True` also loads `httpfs` and creates the S3 secret,
from `s3` or, with none given, from the environment and then the AWS credential chain. A
missing extension raises `ExtensionMissing`, naming how to provision it rather than DuckDB's
`INSTALL` advice, and a machine with no credentials at all raises `RuntimeError` naming the
three ways to supply them. Each call builds a new connection, about half a second of
`LOAD iceberg`, so hold on to one.

**A reader on another machine can cache what it reads from S3** (#118). This is what replaced
`hydrate`: a cache holds what is actually read, needs no write to either table, and survives
restarts.

| Parameter | Default | Layer | Lifetime |
| --- | --- | --- | --- |
| `memory_cache` | on | DuckDB's external file cache | the connection's |
| `disk_cache` | **off** | the `cache_httpfs` extension, on disk | across restarts and processes |

- **Off by default, and never used by a log's own handles.** On the host that writes a log, a
  disk cache of its published table would put back on local disk exactly what eviction
  removed. It is for a reader elsewhere: streamcast's `Stream.snapshot`, say, which spans
  several logs and caches by stream.
- **`cache_key` names the directory**, under `$XDG_CACHE_HOME/litelink` (or
  `~/.cache/litelink`): `cache_key="stream-uuid"` is `~/.cache/litelink/stream-uuid`. An
  absolute path is used as given; None is `~/.cache/litelink/default`. The key is the caller's,
  because only the caller knows what deserves its own cache. Every process using the same key
  shares it, which is safe because Iceberg never reuses a file name.
- **The cache belongs to the database the call builds**, shared by its cursors. A caller pooling
  connections pools per key.
- **`disk_cache_volume_limit` bounds it by how full its VOLUME may get**, not by its own size:
  it evicts once the volume is 80% full, counting everything on it. `cache_httpfs` has no byte
  cap of its own, and left alone keeps only 5% free.
- **`memory_cache=False` turns off every RAM layer**, `cache_httpfs`'s own read-through cache
  included.
- **`cache_httpfs` is bundled in the platform wheels.** Elsewhere, `just duckdb-extensions
  --remote` installs it, and without it `disk_cache=True` raises `ExtensionMissing`.

```python
litelink.install_s3_secret(connection, s3: S3Options | None = None) -> None
```

The S3 half on its own, for a connection litelink did not build — one database handing out
`cursor()`s to many readers, which skips the half-second per reader — and for rotated keys.
Secrets belong to the database, so one call covers every cursor. A credential-chain secret is
created with `REFRESH auto`, so an expiring STS token refreshes without calling it again.

The table sits at `<published prefix>/<log name>` — its data and metadata together, since
0.2. **`version_name_format` is not
optional**: DuckDB defaults to the Hadoop `v%s%s.metadata.json` while pyiceberg names its
metadata `00003-<uuid>.metadata.json`, so the hint carries that stem and the format has to stop
prepending the `v`. `credential_chain` is the ordinary AWS resolution — profile, instance
metadata, SSO; against another endpoint pass `KEY_ID`, `SECRET`, `ENDPOINT` and
`URL_STYLE 'path'` instead, which is what the library's own reader emits.

**Reading it as it grows.** `publish` is lazy, restartable and arbitrarily far behind, and no read
depends on it, so a reader sees a committed snapshot that lags the writer — never a partial
one. `litelink_offset` is what makes that safe to poll: it is monotonic and never reused, so a
reader keeps the highest one it has seen and asks for what came after.

```sql
-- first pass: whatever is there, and where it ended
SELECT max(litelink_offset) FROM iceberg_scan(...);          -- 1536

-- every pass after: only what the publishes since have added
SELECT * FROM iceberg_scan(...) WHERE litelink_offset > 1536;
```

Resolve the table on each pass rather than caching a metadata path — the hint moves with every
commit, and a pinned pointer serves one stale snapshot for ever. What this reader cannot see is
anything newer than the last `publish()`: rows still in the buffer or the staging table are on the
writing box alone, so its freshness lever is the publish interval.

`tests/test_publish.py::test_the_published_table_reads_as_a_directory_with_no_catalog_at_all` is that
claim as a test — it captures through a live published table and asserts the DuckDB row count equals
the writer's `published_through()`.


## Sealing

```python
log.seal(*, flush=False) -> int | None   # write what the size trigger cut; flush: everything
log.await_seal(timeout=None) -> bool
```

**The cut is chosen by the appender**, in the transaction that crosses `target_seal_size` or
`target_seal_rows`, and queued. `seal()` drains that queue and returns the last exclusive
end offset, or `None` if there was nothing. It is an indexed read of one row when idle, so it
is cheap to call often.

`seal(flush=True)` cuts everything buffered, however little, and leaves an undersized file for
compaction to merge. `flush` means the same on `seal`, `publish` and `advance`: push
everything through this stage now, regardless of thresholds. It is for shutdown and for
tests, not for a loop.

**Nothing seals unless something calls one of these.** The library owns no thread and no
interval; a writer running alone accumulates in SQLite indefinitely — durable and readable the
whole time, but never reaching Parquet.

## Maintenance

```python
log.advance(*, flush=False) -> None
log.seal(*, flush=False) -> int | None
log.compact(table="staging") -> None             # "published": ad hoc, whole log
log.publish(*, flush=False) -> None
log.evict(table=None, *, start_offset=None, end_offset=None) -> None  # "buffer" | "staging"
log.reclaim(table=None, *, min_free_ratio=0.0) -> None  # "buffer" | "staging" | "published"
log.sweep(table=None) -> None                           # "staging" | "published"
```

`advance()` is the one call most deployments want: the whole pipeline, in the order rows move
from the buffer to the published table, each table swept after the last step that can change it.

1. `seal()`: buffer → staging.
2. `compact()`: merges a run once it has `compact_min_files` files that fit the target.
3. `publish()`: staging → published, only what compaction is finished with.
4. `evict("buffer")`: rows the next durable copy holds — staging, or the published table
   with `wal_replication`.
5. `evict("staging")`: files the published table now holds, in the same pass (I4).
6. `reclaim("buffer")`: `VACUUM`, only when `vacuum_free_ratio` is set.
7. `reclaim("staging")`: expire snapshots past `staging_snapshot_retention`, then delete the
   files whose grace has passed.
8. `sweep("staging")`.
9. `reclaim("published")`: the same against `published_snapshot_retention`.
10. `sweep("published")`.

**`seal` and `publish` only move data; every deletion is `evict`'s.** A seal leaves its rows in
the buffer until `evict("buffer")`; reads see each row once either way, because the read
boundary is the staging table's committed end. **`evict` takes optional half-open
`[start_offset, end_offset)` bounds**, and chunking is the caller's: unbounded, a buffer
eviction is one `DELETE` under SQLite's write lock and stalls appends while it runs. Staging
eviction always removes a prefix, so a `start_offset` above the table's first offset raises.

**`reclaim` on staging or published does not delete a file in the call that frees it.** Iceberg's
expiry is metadata only; what it frees, and what compaction and eviction supersede, is queued
with the time it stopped being referenced, and deleted only once its table's snapshot retention
has passed since then — the grace that lets a scan already reading it finish (I6). A later
`reclaim` deletes it; a retention of zero deletes it in the same call.

**It is meant for a script or a single process, and raises on a second owner.** If another
owner holds the publish lease, or the publish fails for any other reason, steps 4–8 still run
(local storage keeps being reclaimed on a machine cut off from a remote published table), then
the published steps are skipped and the error is raised. Every log publishes (#98), to a local
directory by default, so the pipeline needs the network only when the published table is
remote.

**Each step is also a routine of its own**, for an orchestrator whose schedules differ because
the costs do, or that wants the published side in another process or `sweep()` in a daemon
thread. **`compact` reads and rewrites whole files, while `evict` and `reclaim`'s expiry are
metadata commits that finish in milliseconds, and `publish` is the only step that waits on the
network.** Each takes only the exclusion it needs: a claim on the offsets it touches, or none
(`reclaim`'s expiry and `sweep`). A routine that works on several tables takes the table as an
argument, None meaning every table it acts on, rather than one function per table; a misspelt
table, or one the routine does not act on, raises `ValueError`.

**What each routine excludes**, so an orchestrator can see what may run side by side (SPEC §4a).
A claim excludes only an overlapping claim, whatever its kind:

| Routine | Claims | So it waits for |
| --- | --- | --- |
| `seal()` | the range it seals, above staging's end | nothing but another seal of that range |
| `compact()` | each run it merges | a publish of those files, another compaction of the run |
| `publish()` | `[published floor, end of what it pushes)`; the whole log when it must write the tier row | compaction of those files, another publish, the whole-log operations |
| `evict()` | the prefix it removes | anything overlapping that prefix |
| `reclaim()` | nothing for the expiry or the delete | nothing |
| `sweep()` | nothing | nothing |
| `set_config` | nothing: one `meta` row, read wherever a decision is made | nothing |
| `compact("published")`, `set_sort_by`, `retire` | the whole log | everything above |

The sweep lists a table's `metadata/` at its first pass in a process, then every four hours,
and deletes what a lost or crashed commit left behind, at most 500 files a pass (SPEC §6). It
takes no claim and never raises.

## Published table

```python
log.publish(*, flush: bool = False) -> None
log.compact("published") -> None
```

`publish` uploads the staging files compaction is finished with, registers them into the
published table in one commit, and records the watermark (§5). Compactions are never
replicated: a file is pushed once it is settled, and compaction will not merge what the
published table holds. `flush=True` also pushes the trailing run that
`stable_prefix` holds back for compaction — everything unpublished, not a subset, because the
push walks a prefix and the watermark it records must stay contiguous. Use it to close a bulk
load's tail on a log that has gone quiet; the cost is undersized objects the published table keeps
until `compact("published")` re-cuts them.

`publish` is lazy, restartable and arbitrarily far behind, and **no read
depends on it**. All three raise `RuntimeError` when another owner holds the claim.

**Every log has a published table** (#98). Given `published="s3://bucket/prefix"` it is on S3; given
`published="file:///directory"` it is that directory; given none, it is
`<root>/<name>/published`. The pipeline is the same either way — a local-only log publishes,
evicts and retires exactly like one on S3, and any Iceberg engine reads its table through
`version-hint.text`. Its cost is disk: the local published table keeps everything, until truncation
lands (a follow-up). `wal_replication`, `replication_config()` and `restore` need a remote
one: the WAL replica exists to get rows off the machine, and a local published table is on
this disk already.

**`hydrate`, which copied published files back into the staging table, is gone** (#118). A
reader on another machine caches what it reads instead; see `duckdb_connection`. Raising
`staging_retention` applies to data captured afterwards.

`compact("published")` merges undersized files already in the published table. An operation, not a policy —
nothing calls it on a schedule, and normal operation does not need it, because publish pushes
only files compaction has finished with. It exists for the two things that break that on
purpose: an explicit `seal()` stranding a small file, and a change to `target_compact_size`.

## Observing

```python
log.end_offset() -> int                      # EXCLUSIVE upper bound: what the next append gets
log.buffered_rows() -> int                   # durable, not yet sealed
log.staging_rows() -> int
log.staging_files() -> int                     # what compaction is bringing down
log.staging_extent() -> tuple[int, int] | None # [start, end) from manifest statistics
log.published_through() -> int                # highest offset the published table holds, 0 if none
log.published_files() -> int
log.coverage(*, published=True) -> Coverage    # each tier's [start, end)
```

`coverage()` says where each tier sits in the log: `published` (what only the published table holds,
below the staging table), `staging` and `buffer` (the rows above every file), each a half-open
`[start, end)` or None when the tier holds nothing — the convention of the stored tier offsets
and of `litelink.manifest`, so a range never needs a `+ 1`. They partition the log the way
`column_statistics`' tiers do, so each offset is in exactly one. It reads the offsets the log
already keeps for routing — one SQLite read and the buffer's two edges — so it is local and
cheap. The lowest `start` is where the log starts; the lowest of `staging` and `buffer` is how far
back a read can go without the published table, which is the floor to enforce for a replay that must
not reach it.

The published table's range is read from its manifests only for a log with no stored published row yet
(written before 0.6 and not yet opened by a writer, or just re-pointed). `published=False` skips
that: `published` is None, meaning "not asked", and nothing but local disk is opened — for a
caller that only wants the local floor.

All local and none of them opens a data file, with one exception: on a **read** handle whose
staging table holds nothing while its published table holds rows — a log evicted dry —
`end_offset()` reads the published table's metadata, because the buffer's sequence is
not authoritative there: `sqlite_sequence` never lowers, so it keeps counting rows a seal
moved out of the buffer.

**A `WriteHandle` never does this.** It overrides `end_offset()` to read `sqlite_sequence`
alone, because a writer is asked where its next row will land rather than what it can serve —
so the number an operator alarms on cannot fail during an object-storage outage.

`published_through()` against `end_offset()` is
the publish lag, which is the number to alarm on: eviction may never precede registration (I4),
so a stalled publish stalls eviction, and the local file count grows until seals feel it.

### Per-column statistics: `column_statistics`

```python
log.column_statistics(*, tier=None) -> TierStatistics   # None | "staging" | "published" | "buffer"
stats.record_count, stats.file_count
stats["price"]    # ColumnStatistics(min, max, null_count, value_count, nan_count)
```

Every column's bounds and counts, computed when asked from what Iceberg already keeps in its
manifests, so no data file is opened and nothing extra is written or published.

**The tiers partition the log**, each row counted in exactly one, and together they are
`tier=None`, the default:

| `tier` | What it counts | Reads |
| --- | --- | --- |
| `"staging"` | the staging table's current snapshot | local manifests |
| `"published"` | what the published table holds below the staging table — the rows eviction moved there | the published table's manifests, over the network |
| `"buffer"` | buffered rows above every file, counted from the rows | the buffer (about 10 ms at a full 8 MiB window) |
| `None` | the whole log | all three |

The tiers overlap in storage by design — the published table keeps a copy of the staging window, and a
`wal_replication` seal keeps its rows in the buffer — so each row is counted from one place, as
a read takes it. The one layout whose rows can't be separated is a published file straddling
the local boundary, which only `compact("published")` produces; its bounds still hold, and every
count in `"published"` and `None` comes back `None` rather than doubled.

**`"published"` is 0.5's `"archive"`, narrowed.** That was the whole archive, overlapping the
staging window; this is only what lies below it. A log with nothing in staging — retired, or
evicted dry — still gets the whole published table.

A consumer prunes on this, so missing information is `None`, never a narrower bound:

- **`min`/`max`** are None when any file with rows in the column has no bound, unless its
  null count proves the column is entirely NULL there. That covers a column a 0.5 `add_column`
  added after some files were written.
- **`nan_count` is 0 for every float column.** No write path admits NaN, so a float's bounds
  are over every value it holds and are safe to prune on in both directions.
- **Only numeric and `bool` columns have bounds.** Iceberg truncates string and binary bounds
  to 16 bytes, so they are not values. Nested columns keep no statistics of their own.
- **Counts are sums, and None when any file lacks the count.** `value_count` includes NULLs,
  as Iceberg defines it.

## Retiring a log: `retire()`

```python
log.retire() -> None
```

Ends the log for good: every row goes to the published table, the staging table and buffer are emptied,
and the retirement is recorded twice: the buffer gets an end (its `end_offset`, until now
open), and the published table gets a `litelink.retired` property. Before it finishes, it
deletes every stranded metadata file in both tables (SPEC §6), since a retired log takes no more
maintenance passes; that runs 32 deletes at once, so a backlog of thousands takes seconds.
Afterwards:

| Operation | On a retired log |
| --- | --- |
| `append`, `extend`, `ingest` | `RetiredError`, naming when it retired, its last offset and the `start_offset` for the next log |
| `open(root, name)` | `RetiredError`, the same message |
| `open(root, name, read_only=True)` | allowed; reads come from the published table |
| `restore(...)` | `RetiredError`, from the published table's `litelink.retired` or the replica's closed buffer |
| `column_statistics(tier="published")` | the whole log |

**Appends are refused by SQLite**: a trigger on the buffer refuses every insert once the
buffer has an end, so a writer that opened before `retire()` ran is refused too, at no cost
to the append path. A buffer with an end is the one fact that says the log takes no more
rows; there is no separate marker.

**It is resumable.** The buffer gets its end first — the offset the next append would have
taken, read in the same transaction — so no row can arrive after the final seal and push. A
crash leaves the log `retiring`, taking no rows, and a writer can still open it to call
`retire()` again. The last step narrows the range to empty, which is what reads as `retired`
and lets every read skip the buffer without reading it.

**With `wal_replication`, it flushes the replica** through the running sidecar
(`litestream sync -wait` on its control socket) after each of those two steps, so a restore from the
replica sees the retirement. It never starts a litestream of its own — two processes
replicating one database is corruption — so if the sidecar does not answer, it raises and
says to regenerate the config (see RUNTIME.md).

## Configuration

```python
log.config -> LogConfig
log.set_config(config) -> None
log.published -> str                            # s3://… or file://…, fixed at new()
log.schema -> pa.Schema                       # your columns, as declared at new()
log.sort_by -> tuple[str, ...]
log.set_sort_by(sort_by, *, rewrite) -> None
```

**Everything `new` took, the log gives back**, which is what lets `open` take none of it.
`schema` strips `litelink_offset`, so it is the schema you wrote and the one `append` accepts;
`sort_by` is the one §7 tells you to bound every scan on a leading column of, which is advice
no caller can follow without being able to ask.

`sort_by` reads `meta` on every access, like `config` and `published`. That is not a detail of
the getter: the seal, compaction and the published table's own declaration all read the same one
place, so `set_sort_by` in one process cannot leave a maintainer in another clustering files
by the key it happened to open with.

**There is exactly one copy of the policy, and it is a row in SQLite.** Every decision reads
it from there rather than from memory, so `set_config` in one process is seen by the writer's
next append and the maintainer's next pass, and nothing can hold a stale one.

`set_config` takes no claim. Every setting governs future work only, so a change takes effect
at the next decision that reads it, and a log whose policy changed mid-stream reads exactly
like one that never did.

**The published table is fixed when the log is created**, like the schema. A log does
not move between published tables: to publish somewhere else, `retire()` it and start a new log
where it ended, `new(root, "trades-v2", published=…, start_offset=old.end_offset())`. Offsets
stay dense across the two, and any engine reads both as one sequence.

`set_sort_by` re-clusters what the staging table owns, so `rewrite` must be passed explicitly
and `rewrite=False` raises `ValueError` naming the cost you have not accepted. It runs under
the maintenance claim, because a rewrite *is* a compaction.

**It does not re-cluster the published prefix.** A local rewrite there would commit a file
straddling the published table's extent, and nothing re-cuts a local straddler — so a
re-sort changes the declarations and rewrites only what `publish` has not yet taken. Published
data keeps the clustering it was written with, which is §6's "sealed once and never
rewritten" applied to history. `compact("published")` is not the other half: it re-ingests from
the first badly-*sized* file onwards, so a well-sized published table is never a candidate.

Passing the order the log already has, with `rewrite=True`, is not a no-op — it is how a
re-sort that died after the `meta` write is finished, since that crash leaves the
declarations correct and the files not.

### LogConfig

```python
target_seal_size      int             = 8 MiB    uncompressed (Arrow) bytes per SEAL
target_seal_rows      int | None      = None     the other ceiling; whichever is hit FIRST
target_compact_size   int | None      = None     uncompressed (Arrow) bytes per FILE (None = 8x the seal)
target_compact_rows   int | None      = None     rows per compacted file (None = 8x)
staging_retention     timedelta|None  = None     staging window by TIME (None keeps everything)
staging_rows          int | None      = None     staging window by ROWS — a floor, not a ceiling
staging_snapshot_retention    timedelta = 15 min   how long the staging table's expired snapshots survive
published_snapshot_retention  timedelta = 1 hour   how long the published table's expired snapshots survive
compact_min_files     int             = 4        minimum adjacent files to merge
wal_replication       bool            = False    needs an s3:// published table; also makes a seal KEEP its rows
wal_retention         timedelta|None  = None     how far back a restore may go
vacuum_free_ratio     float | None    = None     reclaim buffer dead space at this free share
compression           str             = "zstd"   Parquet codec: none | snappy | gzip | zstd
```

Frozen dataclass, with `to_json`/`from_json` and two derived properties, `compact_size` and
`compact_rows`. `sort_by` is deliberately not in here — everything above governs future work
only, so `set_config` needs no rewrite.

Sizing is two targets, not one, and §7 and §12 are where that argument lives. Validation is at
construction: `compact_min_files` below 2, a compact size below the seal size, `wal_retention`
without `wal_replication`, `wal_replication` without an s3:// published table, a `vacuum_free_ratio` outside
`[0, 1]`, and a `compression` this build cannot write are each refused.

**`vacuum_free_ratio` is about what a RESTORE pays.** SQLite puts pages freed by a delete on a
free list and never shrinks the file, so a buffer that seals and publishes for months keeps
every page it has ever needed. Locally that is invisible — the free list is reused — but
litestream replicates the FILE, so every `restore` downloads and applies the dead space.
Measured on a 1-day-old capture: 457 MB holding 20,658 live rows with 92% of its pages free, restoring in 12.5 s against 0.8 s for the same content vacuumed.

Set it and `advance` reclaims once the free list reaches that share of the file;
`reclaim("buffer")` does it on demand. **Off by default, because the cost lands on
the write path**: `VACUUM` takes an exclusive lock and stalls appends for as long as the live
data takes to copy — 0.3 s at 35 MB — and only the deployment knows whether its arrival rate
can absorb that. A writer with no WAL replica can leave it None for ever and lose nothing
but disk. 0.5 is the value to reach for: the win scales with what is reclaimed and the cost
with what is kept, so the trade only improves above it.

**`compression` governs every data file** — a seal, a compaction, a published rewrite, a bulk
ingest. It is a setting rather than a constant because the right answer is a property of the
payload: a text or JSON column is what zstd crushes, and §15.5 requires `none` for blob
columns, where a codec spends CPU proving that already-compressed bytes are incompressible.

Measured on a 200k-row JSON payload column, sorted as this library writes it:

| codec | bytes/row | ratio | write | full-scan read |
|---|---|---|---|---|
| `snappy` | 97 | 2.07x | 1.0x | 1.00x |
| `zstd` | 51 | 3.93x | 1.9x | **0.65x** |

It is not a size-for-speed trade, which is why the default changed rather than the docs
gaining advice: zstd reads *faster*, because there is less to read and decompressing it is
cheap. The cost is write CPU, against a write path that is fsync-bound and a published push
that is network-bound.

**Changing it rewrites nothing and is safe on a live log.** Parquet records the codec per
column chunk, so a table holding both reads correctly through `scan` and `sql`, and existing
files are never touched. `compact("published")` is what re-cuts history into the new codec, when
the size is worth the transfer.

## Replication

```python
log.databases -> tuple[Path, ...]
log.replication_config() -> str
log.write_replication_config() -> Path
WriteHandle.replication_config_for(root, name, published, s3=None, retention=None) -> str
```

litelink does not run the sidecar — it says what the config has to name. `databases` is the
set that carries the log's state: `buffer.db`, `catalog.db`, `published.db` (`archive.db` on a
log written before 0.6). Omitting one is silently wrong, which is why this is generated rather
than written by hand. `replication_config()` and `write_replication_config()` raise
`ValueError` on a log whose published table is local: there is nowhere off the machine to ship
the WAL.

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

## A log's schema is fixed

A log has the schema it was created with, for life (SPEC §9, #93). There is no `add_column`,
`rename_column` or `drop_column`. To change a schema, start a new log where the old one ended:

```python
old.retire()
new = litelink.new(root, "trades-v2", schema=widened, published=prefix,
                   start_offset=old.end_offset())
```

Offsets stay dense across the two, so any engine reads `<prefix>/trades` and
`<prefix>/trades-v2` as one sequence. A log a 0.5 release widened with `add_column` stays
readable; one it left mid-change is refused with the release that can finish it (0.5.1).

## Rules that cut across

**A reader has nothing that writes**, rather than write methods that refuse. `extend`,
`append`, `ingest`, `seal`, `await_seal`, `advance`, `compact`, `publish`, `evict`,
`reclaim`, `sweep`, `compact("published")`, `retire`, `set_config` and
`set_sort_by` are absent from `LogHandle`.
Everything observational and both read paths are there.

This is the difference from the older `Log.open(read_only=True)`, which returned ONE class
whose thirteen write methods existed and raised `RuntimeError("this Log was opened
readonly")`. That is the shape Python's own `open()` has at runtime, where a read-mode file
carries a `.write` that raises `UnsupportedOperation` — but typeshed types the *constructor*
with overloads, and so does this, so the misuse is caught before it runs.

**One writer per log.** SQLite's write lock is per file and one process per stream is the
intended topology; multiple machines write separate logs and readers union.

**The claim decides who does the work, not the caller.** `advance`, its routines, `publish`,
`compact("published")`, `retire` and the three setters all coordinate through rows in
SQLite, so a second caller is refused with `RuntimeError` rather than duplicating the work — and that holds
between threads and between processes on identical terms.

**Nothing runs on a timer.** Size ceilings are enforced synchronously inside the append
transaction; every other knob is a predicate evaluated when the relevant pass runs. Your loop
is the schedule.
