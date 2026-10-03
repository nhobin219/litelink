# Changelog

Notable changes per release. The commit history is the detailed record — it is
written to be read, with the reasoning in the body — so this file summarises
rather than restates it.

This project follows [Semantic Versioning](https://semver.org/). Before 1.0 the
minor version carries breaking changes.

## Unreleased

### Changed

- **`ingest()` flushes its short last file by default only when the log
  replicates its WAL** (`flush=None`). A loaded range now gets the same
  durability as an appended one. With `wal_replication`, the published table
  is a loaded row's only off-box copy, so the tail is pushed as before.
  Without it, an appended trailing run stays local until it fills, and now so
  does a load's: it merges with what is sealed after it instead of becoming an
  undersized file in the published table. **On a log without
  `wal_replication`, `ingest()` no longer pushes the tail**; pass
  `flush=True` to keep the old behaviour.

## 0.7.0 — 2026-10-02

> **⚠️ `seal()` CHANGED MEANING — CHECK EVERY CALL.** A bare `seal()` used
> to cut and seal *everything* buffered. It now writes only what the size
> trigger has cut, as `seal_due()` did. **Nothing raises**: a call that
> relied on the old meaning silently stops producing files. Replace every bare
> `seal()` meant as "seal everything now" with **`seal(flush=True)`**, and every
> `seal_due()` with `seal()`.
>
> **⚠️ `seal()` and `publish()` no longer delete buffer rows.** `evict("buffer")`
> does, and `advance()` runs it. A loop that calls `seal()` but never
> `evict("buffer")` or `advance()` now grows `buffer.db` without bound.
>
> **Running the steps in separate processes? Adopt the recommended split.**
> API.md's "Process split" names five maintainer processes beside the writer:
> `seal`, `compact`, `publish`, local cleanup (`evict()`, `reclaim("buffer")`,
> `reclaim("staging")`, `sweep("staging")`) and published cleanup
> (`reclaim("published")`, `sweep("published")`). A split from 0.6 that
> leaves out `evict("buffer")` grows `buffer.db` without bound, one without
> `sweep` keeps the metadata a crashed commit stranded, and one that runs
> published cleanup beside local cleanup lets a slow bucket delay freeing
> local disk. `examples/adsb/maintainer.py` runs each role.

### Changed

- **Breaking: `s3=` is now `s3_options=`** on `new`, `open`, `restore`,
  `replication_config_for`, `preflight`, `install_s3_secret` and
  `duckdb_connection`.
- **Breaking: `duckdb_connection(remote=True)` is gone; `s3_options` is what
  makes a connection read S3.** `duckdb_connection()` reads locally and loads
  nothing S3. `duckdb_connection(s3_options=S3Options())` loads `httpfs` and
  creates the secret from the environment and the AWS credential chain, as
  `remote=True` did; pass explicit options to override. `s3_options` is
  keyword-only. `disk_cache=True` without `s3_options` raises `ValueError`
  rather than doing nothing, since the disk cache wraps httpfs.

- **Breaking: `set_sort_by()` is removed; a log's sort order is fixed when
  the log is created** (#118), like its schema and published table. To use
  another order, `retire()` the log and start a new one where it ended:
  `new(root, name, sort_by=…, start_offset=old.end_offset())`. Nothing
  re-clusters existing files any more: staging turns over within its
  retention and the published table keeps the order it was written with, so
  a re-sort only ever sharpened pruning for a while.

- **Breaking: `rewrite_published()` is removed, and `compact()` takes no
  table** (#118). The published table is the log's immutable record and
  nothing rewrites it. It is well-sized by construction, since `publish`
  pushes only files compaction has finished with; the few smaller files —
  a flushed seal or publish, a bulk load's tail, files published before a
  raised `target_compact_size` — stay as written. `compact()` compacts
  staging, as before. To re-cut a log at another size, backfill it into a
  new log created with that target: `ingest` its rows in offset order.

- **Breaking: `set_published()` is removed; a log's published table is fixed
  when the log is created** (#118), like its schema. To publish somewhere
  else, `retire()` the log and start a new one where it ended:
  `new(root, name, published=…, start_offset=old.end_offset())`. Every guard
  re-pointing needed went with it: the re-point checks in `publish`, the
  whole-log claim the setters took, and adopting a table back from its
  `version-hint.text`. **`set_config()` takes no claim** any more. Logs
  written before keep the catalog repair and `drain_published`'s prefix check,
  in case they carry a half-done re-point; `drain_published` also refuses a
  table whose catalog entry names another prefix than the log does.

- **Breaking: `hydrate()` is removed** (#118). It copied a time window of
  published files back into staging whether anyone read them or not; a
  reader on another machine caches what it actually reads instead (above),
  across restarts, without writing to either table. Rows needed readable with no network at all need `staging_retention`
  raised before they are evicted.

- **Breaking: `evict(table)` and `reclaim(table)`, one set of verbs for every
  table** (#122). `seal` and `publish` only move data; every deletion is
  `evict`'s, and every return of disk is `reclaim`'s.
  - `evict(table=None, *, start_offset=None, end_offset=None)` takes
    `"buffer"` (new: rows the next durable copy holds — staging, or the
    published table with `wal_replication`) and `"staging"` (as before). The
    half-open bounds narrow what is eligible; chunking a large eviction is the
    caller's, since an unbounded buffer eviction is one `DELETE` that stalls
    appends while it runs.
  - `reclaim(table=None, *, min_free_ratio=0.0)` replaces `expire(table)` and
    `reclaim_buffer(min_free_ratio)`: `"buffer"` is `VACUUM`, `"staging"` and
    `"published"` expire snapshots and then delete files whose grace period
    has passed. **`reclaim()` with no argument now also vacuums the buffer**,
    which blocks appends; name the tables to avoid it.
  - `advance()` runs: seal, compact, publish, `evict("buffer")`,
    `evict("staging")`, `reclaim("buffer")` (only with `vacuum_free_ratio`),
    `reclaim("staging")`, `sweep("staging")`, `reclaim("published")`,
    `sweep("published")`.
- **Breaking: `seal(*, flush=False)` replaces `seal()` and `seal_due()`**
  (#119). See the warning above.
- **Breaking: `maintain()` is renamed `advance(*, flush=False)`** (#119). It
  advances the log's rows from the buffer to the published table, which
  "maintain" undersold. `advance(flush=True)` passes `flush` to `seal` and
  `publish`, getting everything off this machine in one pass, e.g. at
  shutdown.
- **Breaking: `publish(push_unsettled=…)` is now `publish(flush=…)`** (#119).
  `flush` means the same on `seal`, `publish` and `advance`: push everything
  through this stage now, regardless of thresholds.
- **Breaking: `LogConfig.snapshot_retention` is split in two** (#113).
  `staging_snapshot_retention` (default **15 minutes**, was 1 hour) and
  `published_snapshot_retention` (default 1 hour). Pass the new names; a stored
  config written by an older version fills both from its `snapshot_retention`.
  A scan of the staging table running longer than 15 minutes now needs the
  staging setting raised.
- **Breaking: `advance()` (formerly `maintain()`) now runs the whole
  pipeline, publish included** (#117, #119, #122). Data moves first, then
  cleanup follows behind it, in the order listed under the `evict` and
  `reclaim` entry above.

  #100 made every log publish, and `advance()` should have published from
  then on. A loop calling `advance()` then `publish()` still works, but the
  second call now finds nothing to do and can be dropped. **`advance()` now
  raises** if the publish fails, including when another owner holds the
  lease: it is meant for one process. The buffer and staging steps still run
  first, so a machine cut off from a remote published table keeps reclaiming
  local storage.
- **The published table is now expired** (#113). `reclaim("published")`
  expires published snapshots older than `published_snapshot_retention`
  and deletes the objects that frees once due. Previously the published table
  was expired only after `rewrite_published()`, so one that was only ever
  published kept every snapshot, manifest list and manifest.
- **Breaking: one routine per operation, the table an argument** (#117,
  #122). `evict`, `reclaim` and `sweep` take the table they act on, None
  meaning every table that routine handles; a misspelt table, or one the
  routine does not act on, raises `ValueError`. `expire()` and
  `expire_published()` are gone: `reclaim("staging")` and `reclaim("published")`
  replace them.
- **Breaking: `heartbeat` is removed** from `compact()`, `evict()` and
  what was `expire()`. Each pass takes and renews its own claim on the range it works
  on (§4a), so a caller's callback had nothing left to do: on `evict()` and
  `expire()` it was already ignored, and on `compact()` it could only abort the
  pass. Drop the argument.

- **`drain` takes no claim, and `publish()` claims only the range it pushes**
  (#118). Nothing can make a queued file live again once `hydrate` is gone,
  so deleting due files needs no exclusion; and a publish no longer refuses a
  seal or eviction in another process for the length of an upload. A publish
  that must write the tier row still takes the whole log. API.md lists what
  each routine excludes.

### Added

- **`duckdb_connection` can cache what it reads from S3** (#118), replacing
  `hydrate` for a reader on another machine: `memory_cache=True` (DuckDB's
  external file cache, for the connection's lifetime), `disk_cache=False` (the
  bundled `cache_httpfs` extension on disk, surviving restarts), `cache_key`
  (the directory under `$XDG_CACHE_HOME/litelink`, e.g. a stream id; absolute
  paths used as given; None is `default`) and `disk_cache_volume_limit=0.8`
  (evicts once the cache's VOLUME is that full, counting everything on it). A
  log's own handles never cache to disk. `cache_httpfs` is bundled in the
  platform wheels and installed by `just duckdb-extensions --remote`.
- **`sweep(table=None)`**, a routine of its own (#117). It takes no claim, so
  an orchestrator can run it on its own schedule, in another process, or in a
  daemon thread.
- **A sweep for stranded Iceberg metadata** (#113). A commit that loses its
  pointer swap, or crashes before it, leaves its manifests, manifest list and
  `metadata.json` behind, and pyiceberg does not delete them. `advance()`
  sweeps each table after its last change. A sweep lists `metadata/` at its
  first pass in a process and every four hours after, and deletes files that
  nothing references and that are older than both an hour and the table's
  retention, at most 500 a pass. It takes no claim, so a slow pass through a
  backlog never holds up another process, and a failure is logged, never
  raised. This also clears the manifests orphaned before #112. `retire()`
  sweeps both tables completely, with 32 deletes in flight, before it marks
  the log retired, since a retired log takes no more passes.

### Fixed

- **`duckdb_connection(memory_cache=False)` turns the memory cache off on a
  local connection too.** It was applied only to a remote one, and DuckDB's
  own default is on for every connection.
- **An expiry with nothing to expire no longer commits.** pyiceberg would
  write a new `metadata.json` and swap the catalog pointer on every pass.
- **Expiry now deletes the manifests a commit merges away** (#111). With
  manifest merging on, each seal wrote an `-m0` manifest and folded it into
  the `-m1` its snapshot lists, so no snapshot ever named the `-m0` and
  nothing deleted it. Metadata grew by one file per seal. Files already
  orphaned this way are reclaimed by the sweep above.

## 0.6.1 — 2026-10-01

### Added

- **`litelink.duckdb_connection(s3=None, *, remote=False)`**: a DuckDB
  connection provisioned to read a published table on another machine
  (#108). It loads `avro` and `iceberg` from the extensions litelink bundles;
  `remote=True` also loads `httpfs` and creates the S3 secret. A missing
  extension raises the now-public `litelink.ExtensionMissing`, and a machine
  with no S3 credentials raises an error naming how to supply them rather than
  DuckDB's "Secret Validation Failure". It replaces reaching into
  `litelink._read` for `duckdb_connection`, `load_extension` and `secret_sql`.
- **`litelink.install_s3_secret(connection, s3=None)`**: the S3 half on its
  own, for a connection litelink did not build (one database handing out
  cursors) and for rotated keys.

### Changed

- **A credential-chain S3 secret is created with `REFRESH auto`**, so a
  long-lived connection keeps working past an STS token's expiry. This applies
  to the log's own reader as well.

## 0.6.0 — 2026-10-01

### Changed — breaking

- **The tiers are renamed for their roles: buffer → staging → published**
  (#98). The local Iceberg table is the staging tier, and the archive is the
  log's published table. Every public name follows, with no aliases:

  | Before | After |
  | --- | --- |
  | `archive=` (`new`, `restore`, `replication_config_for`) | `published=` |
  | `log.archive` · `set_archive()` | `log.published` · `set_published()` |
  | `sync()` · `ingest(sync=)` | `publish()` · `ingest(publish=)` |
  | `archived_through()` · `archive_files()` | `published_through()` · `published_files()` |
  | `rewrite_archive()` | `rewrite_published()` |
  | `table_rows()` · `table_files()` · `table_extent()` | `staging_rows()` · `staging_files()` · `staging_extent()` |
  | `LogConfig(local_retention=, local_rows=)` | `LogConfig(staging_retention=, staging_rows=)` |
  | tiers `"local"` · `"archive"` | `"staging"` · `"published"` |
  | `Coverage.archive` · `Coverage.local` | `Coverage.published` · `Coverage.staging` |
  | `scan/sql/coverage(archive=False)` | `…(published=False)` |

  `hydrate()` and the `litelink.retired` property keep their names.

  **Stored names follow, and old logs still open.** A new log stores
  `published` and `published_through` in `meta`, tier rows `staging` and
  `published`, config keys `staging_retention` and `staging_rows`, and its
  catalog as `published.db` with catalogs named `staging` and `published`.
  Every old name is still read. A writer's `open` moves an old log's `meta`
  keys and tier rows to the new names; its catalog files and catalog names
  are kept (they are replicated, and are found by name), and its config keys
  move the next time the config is written. The entries below use the new
  names.

- **litelink reads on the primary only: `snapshot` and `RemoteReadHandle` are
  removed**, in both modes — archive-only and `include_wal=True` (#90). Every
  handle is built from a root on the machine that holds the log, and still
  reads the buffer, the staging table and, when asked, the published table. Another
  machine reads the published table with any Iceberg engine, as it already could
  without litelink installed: `iceberg_scan('<published prefix>/<name>',
  version_name_format = '%s%s.metadata.json')` in DuckDB. Rows newer than the
  last `publish` are readable on the primary alone. The WAL replica stays, for
  `restore`.
- **litelink decides which tiers a query reads; `include_archive` and
  `with_archive()` are removed** (#90). Every query reads the buffer, and
  reads the staging table and the published table only when their statistics could hold
  a matching row: the staging table's from the snapshot the read resolved, the
  published table's — what it holds below the staging table — from a row kept in
  `buffer.db`. Neither is on the network, so a query bounded inside the staging
  window never touches it, and an unbounded one reads the whole log without
  the caller naming a tier. This reverses 0.4.0's tiers-fixed-at-assembly
  rule: a query's latency now follows its predicates rather than its handle.
  Only a single `SELECT … FROM log` with AND-ed column-to-literal comparisons
  is narrowed; anything else reads every tier.
- **`column_statistics(tier="published")` is the published table below the
  staging table**, not the whole published table: the tiers now partition the
  log, and a new `tier="buffer"` completes them, so `"staging"` +
  `"published"` + `"buffer"` is `tier=None`. For the whole log, use
  `tier=None`. A log with nothing in staging (retired, or evicted dry) still
  gets the whole published table from `"published"`.
- **`litelink.log` is now private (`litelink._handle`).** Everything public is
  exported from `litelink` itself, and `litelink.manifest` stays public. The
  one name callers took from `litelink.log`, `OFFSET`, is now `litelink.OFFSET`:
  `from litelink import OFFSET`.
- **Every offset range litelink reports is half-open, `[start, end)`**, the
  convention its stored tier offsets and `litelink.manifest` already used:
  `coverage()`, `staging_extent()` (was `table_extent()`), and
  `recovery().skipped`. **`ingest()` changes silently**: it keeps its name and
  now returns `(start, end)` with `end` one past the last offset it assigned,
  so a caller treating the second value as the last offset is off by one.
  A single offset (`published_through()`) is still the last one held.
- **Logs are immutable: `add_column`, `rename_column` and `drop_column` are
  removed** (#93). A log keeps the schema it was created with; to change it,
  `retire()` the log and `new()` one at `start_offset=old.end_offset()` under
  a new name. A log a 0.5 release widened stays readable. One a 0.5 release
  left mid-`add_column` is refused, naming 0.5.1 as the release to finish it
  with.
- **`coverage()` reports each tier's offset range: `Coverage(published,
  staging, buffer)`**, each a half-open `[start, end)` or None — the
  convention of the stored tier offsets and `litelink.manifest` — partitioning the log like
  `column_statistics`' tiers. `published` is now what only the published table holds,
  below the staging table, rather than the whole published table; `buffered` is renamed
  `buffer`; `gap` and `wal_replication` are removed. It is read from the
  offsets kept in `buffer.db` for routing, so it no longer opens the published table,
  except for a log with no stored published row yet — and
  `coverage(published=False)` skips even that, for a caller that only needs the
  staging floor, `min(staging[0], buffer[0])`.
- **`scan(published=False)` and `sql(..., published=False)`** read the staging table
  and the buffer only, never the published table, whatever the query asks — what
  `include_archive=False` did, per read rather than per handle. Rows only the
  published table holds are left out, not refused.
- **Every log has a published table; with none given it is a local directory** (#98).
  `litelink.new(published=None)` publishes to `<root>/<name>/published`, and
  `published="file:///directory"` names a local published table anywhere. The pipeline
  is the same as on S3: `publish` copies settled files, eviction drops only what the
  published table holds, and `retire`, `rewrite_published` and tier selection work on
  local-only logs. Any Iceberg engine reads the local
  table through `version-hint.text`.
  - **`staging_retention` and `staging_rows` no longer delete.** On a local-only
    log they used to be a deletion policy over the only copy; now nothing
    leaves the staging table until `publish` has published it, so a local-only
    log that relied on retention to bound disk must call `publish()`, and its
    local published table then grows (truncation is a follow-up).
  - **`set_published(None)` points back at the local default** instead of
    detaching, so the refusal to detach under a retention floor is gone, as
    is the refusal of `staging_retention=0` with no published table.
  - **A move opens its new table before recording it** and raises with
    nothing changed if it cannot. Re-stating the current location is a no-op:
    no claim, no write, no network.
  - **`log.published` is always a string** (`s3://…` or `file://…`).
  - `wal_replication`, `replication_config()`, `restore` and `hydrate` need
    an `s3://` published table; a local one is on this disk already.
  - An existing local-only log records the default at its next writer
    `open`; its first `publish()` publishes everything it holds.
- **The 0.1 → 0.2 migration is removed** (`litelink.migrate`). A log still in
  the pre-0.2 layout is refused with the command to run under litelink 0.5.1,
  the last release that carries it.
- **Regenerate the litestream config** (`write_replication_config()`) and
  restart the sidecar: it now enables the control socket `retire()` flushes
  the replica through.

  Existing logs get the published table's row at the writer's next `open` (or the
  first `publish`, if the published table cannot be read then); until then every query
  reads the published table, which is correct and only slower. **Upgrade every
  process on a log together:** a maintainer still on 0.5 would evict without
  widening the published table's row, and a read on the new version could then skip
  rows eviction had just moved there.
- **`WriteHandle.retire()`** ends a log for good: every row to the published table,
  the staging table and buffer emptied, and the retirement recorded in
  the buffer's range (it gets an end) and on the published table
  (`litelink.retired`). Afterwards
  appends, a writer `open`, `ingest` and `restore` raise `RetiredError`,
  naming the offset the next log should start at; read-only opens and
  `hydrate` still work. Appends are refused by a SQLite trigger, so a writer
  opened before `retire()` is refused too. It is resumable after a crash, and
  with `wal_replication` it flushes the replica through the running sidecar.
- **A scan bounded below the buffer skips it.** `litelink_offset` is judged
  against each tier's `[start_offset, end_offset)`, the buffer's included, so
  a history scan no longer converts the buffered rows to Arrow.
- **`litelink.manifest`**: the statistics manifest and its pruning, public, so
  streamcast uses the same implementation for its sealed logs (`Entry`, one
  unit with its offsets and statistics; `build`, `extend`, `prune`, with the
  key column a parameter). A term on
  `litelink_offset` is judged against each unit's `[start_offset, end_offset)`,
  so a unit with no statistics — a live log, the buffer — still prunes by
  offset; an `end_offset` of None marks a range still growing.

## 0.5.1 — 2026-09-29

### Fixed

- **The seal-size estimate undercounted by up to 3×** for sparse, short-string
  and numeric nested schemas, sealing files well past `target_seal_size` and
  telling compaction they held less than they did (#84). It now models the
  Arrow table a seal builds — a slot for every value, null or not, string and
  list offsets, narrow widths, and nested values as Arrow holds them rather than
  their JSON — and stays between 1.00× and 1.11× of Arrow's `nbytes` across
  the issue's row shapes. The per-row work is about 5× cheaper, since only
  string, binary and float columns are visited.

## 0.5.0 — 2026-09-29

### Changed — breaking

- **A log holds only finite floats.** NaN and ±inf are refused on every write
  path — `append`, `extend`, `ingest` and `validate_row`, top-level and nested,
  float32 and float64 — naming the column and the path inside it (#87). A
  top-level NaN was already refused; ±inf was accepted, and so was NaN nested
  in a struct, list or map or loaded through `ingest`. `column_statistics()`
  now reports `nan_count = 0` for every float column, so float bounds are
  prunable. A NaN in a required column used to raise SQLite's bare `NOT NULL
  constraint failed`; it now says what it is.

### Added

- **`LogHandle.column_statistics(tier=None)`**: every column's min, max, null
  and value counts, with record and file counts, from what the Iceberg
  manifests already hold — no data file opened, nothing new written (#85). The
  default is the whole log, each row counted once across the local table, the
  archive and the buffer; `"local"` or `"archive"` asks for one tier. Missing
  information is None rather than a narrower bound; strings, bytes and nested
  columns carry no bounds, and a float's `nan_count` is 0, since no write path
  admits NaN.
- **`binary`, `fixed_size_binary(n)`, `struct`, `map` and `list` columns**, which
  is what an OpenTelemetry log record needs: trace and span ids, the body and
  attributes as `AnyValue` structs, array values (#79). Nested values are
  checked against the declared type at `append` — an unknown struct field is
  refused where Arrow would drop it — and read back exactly from the buffer,
  the local table, the archive and any engine reading it directly. `binary`
  is for small values such as ids — its bytes go through the buffer — and not
  a substitute for §15's blob fields, which are what large payloads need.
- **`litelink.validate_row(schema, row)`** checks a row without appending it,
  raising exactly what `append` would — same exception, same message — with no
  log required (#77).

### Changed

- The temporal Arrow types are refused as a decision rather than pending work:
  how to represent time is the application's choice, and the refusal suggests
  an `int64` epoch or a string (SPEC §13.8, #79).

## 0.4.1 — 2026-09-23

### Fixed

- **`restore(include_archive=True)` was accepted and ignored.** It built its
  handle with a bare `cls.open(...)`, so the argument went nowhere. It matters
  most exactly there: a restored log's local table is EMPTY by construction,
  which is the one case where a local-only read has nothing to serve — `sql`
  refuses rather than answering short, so the dropped argument surfaced as a
  failed read rather than a quiet one.

## 0.4.0 — 2026-09-22

### Changed — breaking

- **`include_archive` is a property of the handle, not an argument to each
  read.** `open(root, name)` reads local files and the buffer;
  `open(root, name, include_archive=True)` reads the archive too;
  `log.with_archive()` derives a read-only view of an open handle without a
  second SQLite connection or catalog load. `scan()` and `sql()` no longer
  take the parameter.

  It used to default to whether the archive was *load-bearing* — True exactly
  when the local table held nothing and the archive held something — so the
  same call read local files before an eviction pass and object storage after
  it, with nothing at the call site saying so. A read that changes tier
  changes its latency, its failure modes and its cost, and it should not do
  that on a retention schedule.

- **`RemoteReadHandle` takes no `include_archive` at all.** A snapshot is
  assembled from an archive and its local table is empty by construction, so
  `False` would name a handle that can read nothing. The request is now
  unrepresentable rather than refused — which is what `Follower` had before
  the two read-only shapes were unified.

- A handle that cannot reach the archive and finds its local table empty still
  **refuses** rather than serving the buffer alone. The message now names the
  assembly argument instead of a read argument.

**Migrating:** `log.scan(include_archive=True)` becomes
`log.with_archive().scan()`, or open the handle with `include_archive=True`
where every read wants the archive. `include_archive=False` was already the
effective default for a log with local files; drop it.

## 0.3.1 — 2026-09-18

### Changed

- **Assembling an archive-only snapshot costs one fewer round trip.** It fetched
  `version-hint.text` three times and parsed `metadata.json` twice for a single
  assembly, because `archive_shape` and `archive_extent` do the identical first
  two steps and the path called both. Now two fetches and one parse.
  `archive_shape` returns the extent it already had open and the archive-only
  path carries it through. On a local endpoint that is noise; against a bucket
  at 60–75 ms RTT it is a round trip plus a redundant `metadata.json` transfer,
  and on an archive with thousands of files that file is not small.

  The remaining fetch is `open_archive`'s own bucket-first check before
  `repair=True` — the guard that stops a reader taking the CREATE branch and
  publishing a lineage into the bucket the primary would then commit onto. It
  is on the write path too and is deliberately left alone.

### Documentation

- **`data/ingested/` appears in the layout.** Bulk ingest has written there
  since 0.2.2 and neither the README's tree nor SPEC §2 said so, so the only way
  to learn the directory existed was to list the bucket — which is how it was
  found. Both now show it beside `compacted/`, and SPEC gives each its own line
  with the pass that writes it, including that `compacted/` also holds
  `rewrite_archive`'s re-cuts.

- **The README says which read to reach for.** A one-shot query is cheaper
  through a raw `iceberg_scan` than through `snapshot`, which assembles a DuckDB
  connection, a scratch buffer and an adopted catalog before it can answer
  anything. Hold a snapshot open and the order reverses: measured on a
  200k-row archive, a bounded scan is 0.02 s against 0.40 s for a fresh DuckDB
  connection. Both facts are now stated rather than left to be discovered.

- The README's DuckDB example is Python rather than bare SQL, and drops
  `INSTALL`/`LOAD` — DuckDB autoloads `iceberg`, `avro` and `httpfs` when a
  query names them.

## 0.3.0 — 2026-09-03

### Changed

- **`litelink.follow` is renamed to `litelink.snapshot`** (breaking), and the
  old name is REMOVED rather than deprecated. The library is days old and this
  release is its introduction, so an alias would have been compatibility for
  nobody at the price of two names for one thing in the first API anyone reads.

  The name promised a subscription it never provided, and its own docstring had
  to open by saying so — "a snapshot, not a subscription". It also promised
  freshness specifically, which is what made the new flag impossible to add
  under it: `snapshot` promises a point-in-time view without promising which
  point, and both modes satisfy that.

### Added

- **`snapshot` reads the archive alone by default**, skipping the litestream
  restore — which is almost the entire cost of assembling a handle, and which
  is also the only mode that works on an ordinary log: `wal_replication` is
  opt-in and needs a sidecar, so most logs have no replica and the merged read
  fails outright on them. Measured on a log with an archive and no replication:
  the merged read raised in 0.10 s, the archive-only read served 3,870 rows.
  Pass `include_wal=True` for the merged view.
  Measured against S3 at 60–75 ms RTT: a 1.9 MB buffer took 7.2 s, of which
  transfer was ~0.2 s; the rest is one LIST plan plus ~20 serial GETs, and the
  chain grows with the log's *age* rather than its size. The same handle
  archive-only assembles in about a quarter of a second.

  It is a different view, not a faster route to the same one. **Staleness is the
  archive frontier**, not the replication lag — `sync` holds back a trailing run
  under `target_compact_size`, so a quiet stream's frontier lags indefinitely
  rather than by the sync interval. And it **refuses a log whose archive has
  published nothing**, naming `include_wal=True`: that is the ordinary state of
  a slow capture and exactly when the buffer holds everything, so an
  archive-only handle would serve zero rows. The refusal distinguishes a slow
  capture from a mistyped prefix or name by whether a WAL replica exists, so
  the first is told to retry and the second is told what its arguments resolved
  to.

  The schema it reports takes the **column set from the archive's Iceberg
  schema and the types from a data file's Parquet footer**. Iceberg's schema is
  versioned and current; a data file's is whatever was declared when it was
  written, and files of two shapes land in one commit whenever rows were
  sealed-but-unsynced as `add_column` ran. The footer is the only place the
  declared Arrow types survive, since Iceberg has one string type. One caveat:
  `coverage()` on an archive-only handle reports `wal_replication=False`
  whatever the writer's setting, because the archive does not record it.

## 0.2.3 — 2026-09-02

### Fixed

- **A log whose buffer holds all of it can be followed.** `litelink.follow`
  refused any archive with no published metadata pointer, which is exactly a
  slow capture: nothing reaches `target_seal_size`, nothing is pushed, and with
  `wal_replication` a seal RETAINS its rows — so the buffer holds the whole log
  and the WAL carries every row there is. It was the one case where a follower
  is the only way to read a log off-box, and the one case that did not work. On
  the deployment this came from, a stream whose buffer held offsets 1..7,763
  with an archive holding nothing.

  A follower now serves the buffer alone when it can prove the buffer IS the
  log, which takes two independent facts because rows go missing two ways.
  Rows that LEFT — `finish_seal(discard=True)`, `release_archived` — delete a
  prefix, so the buffer's first offset rises above the log's (`start_offset` in
  `meta`, absent meaning 1). Rows that NEVER ENTERED — `ingest` writes straight
  to Parquet — raise nothing and leave a hole inside the buffered range, so
  `ingest` records `ingested_through` at RESERVATION time, before the file
  exists, where no later crash or compaction can lose it.

  Three earlier predicates were tried and broken by review, each on the last
  one's fix: keyed on what was pushed to the prefix, then on the first offset
  alone, then on `extent` rows naming local files. The last failed because
  `extent` is a copy of what the Iceberg manifest owns and the code tolerates
  its absence — a crash between the register and the record writes no row, and
  an ordinary compaction can union a loaded range with held rows either side.
  The marker depends on none of that. `orphaned_local_ranges` survives only as
  a fallback for logs written before the key existed, where it can add refusals
  and never remove one.

  Adoption is skipped rather than attempted in the new path, which keeps the
  guarantee the old refusal really protected: `repair=True` against a prefix
  with no hint takes the CREATE branch, a reader publishing a lineage the
  primary would commit onto.

- **A mistyped archive prefix is refused where it is typed**, instead of
  surfacing as a YAML parse error from the litestream subprocess. Every
  consumer of the prefix parses it positionally, so a malformed one does not
  fail — it means something else. `s3:/bucket/prefix`, with one slash, kept its
  scheme, split at the first slash it found, and produced a bucket named `s3:`,
  which the generated config wrote as `bucket: s3:` — a plain scalar ending in
  a colon. What reached the caller was
  `litestream restore failed: Error: yaml: line 5: mapping values are not
  allowed in this context`, naming a temporary file they never see and a line
  number in it. `new`, `set_archive`, `restore` and `follow` now all raise
  `ValueError` naming the prefix, and the one-slash case suggests the corrected
  string. `open` is unaffected and still opens a log whose stored prefix is
  malformed — it takes no archive argument, and refusing to open the log would
  remove the `set_archive` call that repairs it.

  `python -m litelink <archive-uri>` reports it too, as a failed check rather
  than a traceback out of argv — it is the command an operator reaches for to
  find out what is wrong, and the prefix arrives there from a shell, where a
  missing slash survives every layer that would otherwise catch it. It
  previously answered `ArrowInvalid: Not a valid bucket name: ''`.

- **"There is no replica here" reaches the caller again.** Both `restore` and
  `follow` carried an explanatory
  `FileNotFoundError` for an archive holding no replica of the log — and
  neither could ever raise it. `restore_buffer` ran litestream without
  `-if-replica-exists`, so an absent replica exited non-zero and became
  `litestream restore failed: Error: no matching backup files available` one
  frame below, leaving both messages unreachable for the life of the callers.

  The message now states both readings, because nothing in the arguments
  separates them: the WAL was never replicated, or `name` and `archive` do not
  together name a log that exists. Reproduced against a real bucket by passing
  a log's own name as the last segment of the prefix. Genuine failures are
  unaffected — measured against litestream 0.5.16, a missing bucket still exits
  1 with `NoSuchBucket` and a bad key with `InvalidAccessKeyId`; only absence
  is quiet.

- **`just rustfs` creates its bucket again.** Both recipes still ran
  `uv run --extra s3`, and that extra was deliberately deleted when s3fs moved
  to the dev group — so the endpoint came up and the bucket did not, and the
  ~91 tests in the S3 tier skipped on a fresh checkout.

- **A bulk load now second-copies itself.** `ingest` writes Arrow straight to
  Parquet, so its rows never enter the buffer and WAL replication cannot carry
  them — the archive is their only other copy. But an ordinary `sync` held a
  load's short last file back: `stable_prefix` keeps a trailing run under the
  compaction budget, because a run with room in it may yet take files that have
  not been written. On a stream that then goes quiet the run never settles.
  Measured on a live deployment: **113,399 loaded rows on one disk**, with
  `coverage()` reporting no gap, across nine streams and ~698,000 rows.

  `ingest` now compacts and then pushes with `sync(push_unsettled=True)` when an
  archive is configured; `sync=False` opts out. The push is a PREFIX — the
  watermark it records has to stay contiguous for eviction to trust it (I4) — so
  it takes everything unarchived, undersized seals beneath the load included.
  That is why the compaction runs first: it collapses accumulated small seals so
  only what a run genuinely cannot fill reaches the archive. Measured on five
  small seals, six undersized objects pushed without it against one with it.

  A push that fails leaves the load durable and raises saying so, because
  retrying the LOAD would reserve a fresh range and duplicate it.

### Added

- **`WriteHandle.reclaim_buffer()` and `LogConfig.vacuum_free_ratio`**, for the
  dead space SQLite never returns. Pages freed by a delete go on a free list and
  the file never shrinks, so a buffer that seals and archives for months keeps
  every page it has ever needed. That is invisible locally — the free list is
  reused — and it is the READERS who pay, because litestream replicates the
  file: every `follow` and every `restore` downloads and applies it. Measured on
  a 1-day-old capture, 457 MB holding 20,658 live rows with 92% of its pages
  free, restoring in 12.5 s against 0.8 s for the same content vacuumed.

  **Off by default and manual, because the cost lands on the write path.**
  `VACUUM` takes an exclusive lock and rebuilds the file, so appends stall for as
  long as the live data takes to copy — 0.3 s at 35 MB. Only the deployment knows
  whether its arrival rate can absorb that. Call `reclaim_buffer()` when it can,
  or set `vacuum_free_ratio` to have `maintain` do it once the free list reaches
  that share of the file. A writer with no off-box readers can decline for ever
  and lose nothing but disk.

  The obvious objection — that rewriting the file must cost more in shipped WAL
  than it saves — was measured and is wrong: two sidecars on the same workload
  shipped 3.1 MB without and 0.3 MB with, because litestream ships LTX deltas of
  a smaller database.

  `litelink_offset` is untouched (I9): values keep their gaps, and the
  `AUTOINCREMENT` counter survives a rewrite of a buffer the archive has fully
  drained — which is the case that would otherwise restart at 1 and reissue
  offsets the archive already holds.

## 0.2.2 — 2026-09-01

**0.2.1 was yanked and 0.2.2 is what it should have been.** Its artifacts reached
PyPI from a tag that was cut before two fixes below had landed, and PyPI files
cannot be replaced. 0.2.1 refuses `ingest` while `wal_replication` is on — which
pushes an operator into turning replication off to load, the exact sequence that
makes a replica stale enough to hit the restore defect — and it does not carry
the fix for that defect. Everything else in it is identical to this release.


### Added

- **`WriteHandle.ingest`** — a bulk load path that takes a `pa.Table` or a
  `pa.RecordBatchReader` and writes Parquet without the rows ever entering
  SQLite. The buffer exists to make a row durable before it is in Parquet, and
  a bulk load's source is already durable, so every row through it pays a
  second time for a guarantee it has. Measured on 400k rows on local disk,
  where fsync is cheap and the gap is therefore understated: 182,801 rows/s
  through the buffer against 5,103,266 rows/s straight out.

  Files come out sorted and sized at `target_compact_size`, so maintenance
  never has to touch them. It refuses concurrency rather than surviving it —
  the whole log is claimed for the whole load and every acknowledged row must
  already be in a file. WAL shipping does not carry a loaded range, because
  these rows never enter the buffer — stated rather than enforced, since
  turning replication off to load would drop the buffer's copy of everything
  already captured.
  The archive is a loaded range's only second copy: compare `archived_through()`
  against the `hi` it returns.

  Bulk-loading history into a `start_offset` reserve *under live capture* is
  not this, is still deferred, and needs a range-aware coverage predicate
  `register` does not have.

- **`LogConfig.compression`** — the Parquet codec every data file is written
  with, across seals, compactions, archive rewrites and bulk ingest. A setting
  rather than a constant because the right answer is a property of the payload:
  §15.5 requires `none` for blob columns, where a codec spends CPU proving
  already-compressed bytes are incompressible.


### Fixed

- **A restore no longer reissues offsets the archive already holds.** The
  resume fence was measured `RESTORE_RESERVE` above the sequence the *replica*
  carried, which is the right floor only while the replica's sequence is the
  highest offset anyone issued — and the reconcile beside it exists precisely
  because the bucket routinely holds ranges the replicated rows have never
  heard of. Reproduced: a replica stalled at offset 301 against an archive
  holding through 2,864,714 resumed at 1,048,877, inside the archive's range.
  The reissued rows sealed into the rebuilt table, `sync` reported success
  while pushing nothing for ever, and `scan(include_archive=True)` returned
  1,048,881 rows of 3,000,600 acknowledged. The recovery report's own `skipped`
  range came back inverted, which is now asserted rather than merely computed.

  It needs the archive more than `RESTORE_RESERVE` ahead of the replica, which
  took a million rows through the buffer during a sidecar outage before bulk
  ingest and takes one reservation after it.

### Changed

- **Data files are written with zstd rather than Snappy** (default change). No
  write site specified a codec at all, so every file took pyarrow's Snappy
  default. Measured end to end through `ingest` on a 400k-row JSON payload
  column: 28.5 MB against 15.2 MB, 71 bytes/row against 38. On a real 177M-row
  archive that is 34.8 GB against roughly 15 GB.

  It is not a size-for-speed trade, which is why this is a default change and
  not a note in the docs: zstd measured a full-scan read at 0.65x Snappy's,
  because there is less to read and decompressing it is cheap, and the load
  itself ran no slower. The cost is write CPU, against a write path that is
  fsync-bound and an archive push that is network-bound.

  **Nothing is rewritten and no action is required.** Parquet records the codec
  per column chunk, so a table holding both reads correctly through `scan` and
  `sql`; existing files are untouched and stay readable. `rewrite_archive`
  re-cuts history into the new codec for anyone who wants the space back.

## 0.2.0 — 2026-08-31

### Changed

- **One directory per stream, in both tiers** (breaking). `catalog.db` and
  `archive.db` move from `<root>` into `<root>/<name>`, Iceberg metadata moves
  from `<root>/litelink/<name>/metadata` to `<root>/<name>/metadata`, the WAL
  replica moves from `<prefix>/_wal` to `<prefix>/<name>/_wal`, and
  `litestream.yml` from `<root>` to `<root>/<name>`. Data files do not move —
  they were already at `<root>/<name>/data`.

  The old shape had metadata describing data that lived outside the location
  the table claimed, held together only by absolute paths in its manifests, and
  shared catalogs that bought nothing: every query against them is keyed by
  `(catalog, namespace, table)` and nothing has ever read across streams. What
  the sharing cost was replication — one sidecar per *root*, with a
  multi-stream config that had to be written by hand — and `follow`'s `root`
  parameter, and a blast radius of every stream under the root. It was never
  contention: two streams sealing against one shared catalog measured 57.3 ms
  median against 66.1 ms for separate roots.

- **One sidecar per stream**, following from the above.
  `write_replication_config()` writes a config that is complete on its own for
  the stream its handle is open on — call it once per stream — where a
  multi-stream root previously needed a single config written by hand.

- An external engine reads the archive at `s3://bucket/prefix/<name>` rather
  than `s3://bucket/prefix/litelink/<name>`.

### Added

- **`python -m litelink.migrate`** — move a 0.1 log to the new layout. A dry
  run by default; `--apply` to act, `--archive` to move the archive's metadata
  too. It rewrites metadata pointers and re-encodes manifest lists rather than
  recreating the table, because retention derives a file's age from the
  snapshot that added it and a fresh commit would silently reset the retention
  clock on both tiers. Data is neither moved nor rewritten. It verifies row
  counts before deleting anything. A root holding several streams migrates one
  at a time: `catalog.db`, `archive.db`, `litestream.yml` and `<prefix>/_wal`
  are shared, so each is kept until the last stream has moved.
  `--drop-legacy-wal` is a separate pass, run once per stream, and refuses
  until the root has fully migrated and a fresh replica has landed — the old
  one holds the only off-box copy of unsealed rows.

- `open()` on a log still in the 0.1 layout now says so and names the migration
  command, rather than reporting an absent log — which would invite creating an
  empty one beside data that is still there.

## 0.1.0 — 2026-08-30

First release.

### Added

- **`litelink.follow`** — read a log running on another machine: the archive
  merged with a WAL-replicated copy of the writer's buffer, so a reader sees
  data down to the replication lag rather than to the seal cadence. A snapshot
  rather than a subscription; `coverage()` reports what it can and cannot serve
  rather than adjudicating.
- **`litelink.preflight` / `python -m litelink`** — check that a machine can
  actually run a log: the litestream binary, the DuckDB read path, and whether
  a configured archive is reachable. Non-zero exit when it is not. Run it as
  the user and from the process manager that will own the log — a systemd user
  unit does not inherit a login shell's `PATH`.
- **Platform wheels that carry what the library shells out to**: a
  checksum-verified litestream and the DuckDB `iceberg`, `avro` and `httpfs`
  extensions. A box with no egress, nothing on `PATH` and no extension cache
  reads, writes and restores. About 124 MB per wheel, and CI asserts the cold
  case rather than assuming it.

### Changed

- **The public classes are handles, not logs.** `Log` is now `WriteHandle`, and
  the read-only surface is a `LogHandle` hierarchy — `LocalReadHandle` for a
  reader beside a live writer, `RemoteReadHandle` for a followed log. Every
  subclass only *adds*; nothing inherits a method it has to refuse.
- **`Log.open(read_only=True)` is `litelink.open(root, name, read_only=True)`**,
  overloaded on the literal so the type is static: `.append` on a read-only
  handle is a type error rather than a runtime one.
- Constructors moved to module level — `litelink.new/open/restore/follow` — so
  the returned type is not implied by a receiver that does not match it.
- Provisioning failures name remedies an installed user can act on, instead of
  `just` recipes from a repository they may not have.

### Fixed

- A read of a fully evicted log served only its buffer — measured at 476 of
  1,500 rows, with no error.
- `coverage()` reported every offset sealed-but-not-yet-synced as an unservable
  gap, on essentially every archived log.
- `buffered_rows()` counted rows another process had already sealed, so
  `table_rows() + buffered_rows()` double-counted across the tier boundary.
- A handle that touched an archive once while it was empty answered "the
  archive holds nothing" for the rest of its life, and served the buffer alone.
- `examples/adsb/replicate.py` took both whole-log claims and could finish
  another process's in-flight seal.

### Removed

- The `s3` extra. It declared `pyiceberg[s3fs]` and was never load-bearing:
  nothing imports s3fs, pyiceberg resolves `PyArrowFileIO` first, and arrow
  speaks S3 natively.
- `follow`'s `root` argument, which had no use case and made two latent bugs
  reachable. The root is always a scratch directory; `scratch_dir` chooses
  where it lives.
