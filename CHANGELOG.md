# Changelog

Notable changes per release. The commit history is the detailed record — it is
written to be read, with the reasoning in the body — so this file summarises
rather than restates it.

This project follows [Semantic Versioning](https://semver.org/). Before 1.0 the
minor version carries breaking changes.

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
