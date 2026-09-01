# Changelog

Notable changes per release. The commit history is the detailed record — it is
written to be read, with the reasoning in the body — so this file summarises
rather than restates it.

This project follows [Semantic Versioning](https://semver.org/). Before 1.0 the
minor version carries breaking changes.

## 0.2.1 — 2026-09-01

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
