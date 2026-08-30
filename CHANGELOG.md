# Changelog

Notable changes per release. The commit history is the detailed record — it is
written to be read, with the reasoning in the body — so this file summarises
rather than restates it.

This project follows [Semantic Versioning](https://semver.org/). Before 1.0 the
minor version carries breaking changes.

## Unreleased

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
