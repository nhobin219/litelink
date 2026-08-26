# litelink

Durable append-only capture into Iceberg tables. Local-first, in-process, no service.

**Status: early.** All three tiers work — append, seal, read, compact, evict, expire, and
`sync()` into an Iceberg table on S3, with reads spanning buffer, local table and archive.
Verified against a local S3-compatible store and against AWS. **Schema evolution is not
implemented**, and remains the one specified feature the code does not have. The design is
[`docs/SPEC.md`](docs/SPEC.md), and in places it is still ahead of the code.

---

## What it is

A library for the thing every capture pipeline hand-rolls badly: getting a stream of
observations onto disk durably, into well-sized Parquet, and eventually into object storage
— without a daemon, a broker, or a catalog service.

```
SQLite buffer          durable on commit. unsealed rows only.
      │  seal at target_size
      ▼
local Iceberg table    a rolling window. reads land here.
      │  sync: upload data files, register into the archive
      ▼
remote Iceberg table   full history, on S3.
```

The catalog is a SQLite file, not a service. **No read on the hot path touches the network.**

## Why it exists

The pattern it replaces, measured on a production capture system:

- 125,884 objects / 4.36 GB, **62.5% of them under 16 KiB**
- Parquet files at **2 rows each** on low-rate streams; one stream's files were **58.6%
  metadata**
- a compaction routine written months earlier that nothing ever scheduled
- an in-memory write buffer, so a `SIGKILL` lost everything not yet flushed — in a system
  whose stated first principle was that capture is irreversible

None of that is incompetence. It is what happens by default when durable capture, file
sizing, and tiering are each solved locally and none of them is anyone's job.

## What it is not

An OLTP or key-value store. A point lookup is ~1,600x slower than an indexed row store, and
no configuration closes that gap. It is a **local, in-process, real-time analytics store**:
data is durable at commit and queryable immediately, so freshness is sub-second *with*
durability — but "real-time" means fresh, not point-lookup fast.

Nor is it an unbounded local archive. **Keeping everything on one machine — `archive=None`
with no `local_retention` — degrades as the table grows.** A seal's cost tracks what the
table's metadata holds, and while running `maintain()` bounds the largest part of that, a
residue grows with the file count: compaction merges files *below* half the target size, so one it
has already produced at that size is never revisited, and only eviction removes it. With no
retention set nothing evicts. The seal is on the write path, so the cost lands on appends.

Configure a retention, or an archive to evict into, for anything long-running. Both bound the
file count, which is what bounds the cost. Details and the options for fixing it properly are
in [`docs/SPEC.md`](docs/SPEC.md) §13.7.

## Design in one page

- **Iceberg is used, not reimplemented.** Manifests, per-file column statistics, schema with
  field IDs, and atomic snapshot commits all come from it.
- **The library owns exactly one column**, `litelink_offset` — monotonic, never reused. It is the
  boundary mechanism between tiers. Everything else is the caller's schema.
- **Sync is a watermark, not CDC.** There are no updates or deletes to replicate.
- **Parts are sealed once and never rewritten.** Rewriting a growing partition costs ~144x
  write amplification and buys nothing, because the local WAL already made the row durable.
- **Read boundaries are derived from committed table state**, never from a stored flag — so
  no seal window can double-count or drop.
- **The seal cut is chosen by the appender**, in the transaction that crosses
  `target_size`, and queued. A sealer that falls behind therefore writes several
  correctly-sized files rather than one oversized one.
- **Compaction is required and local**, because a time-based seal trigger guarantees
  undersized files. Local means no egress to read files back.

Read performance is the cost of reading Parquet, plus ~4 ms of fixed overhead.

How the pieces run — writer and maintainer, what crosses between them, and what is safe
to call from which thread or process — is [`docs/RUNTIME.md`](docs/RUNTIME.md).

## Extensions

Not implemented; specified.

**Blob fields** ([`docs/SPEC.md`](docs/SPEC.md) §15) — payloads too large for the buffer
(sensor frames, point clouds, raw response bodies). Bytes stage beside SQLite while hot and
are inlined into Iceberg at seal, so the archive stays ordinary Iceberg with a `binary`
column: no pointers in the published schema, and blob lifetime inherits snapshot expiry and
compaction rather than becoming a hand-maintained refcount.

Until it lands, `binary` columns are refused outright — see `_types`. (They were once
refused because DuckDB's sqlite scanner decoded blob bytes as UTF-8 and failed; that
scanner is gone, so the remaining reason is §15 itself, which has payloads bypass the
buffer rather than travel through it.)

## Open questions

Payload encoding, local-disk backpressure, bulk ingest, and extension provisioning for
embedders. All four in [`docs/SPEC.md`](docs/SPEC.md) §13.

## Using it

```python
import pyarrow as pa
from litelink import Log

# An ADS-B position feed: durable the moment it arrives, queryable a moment later.
schema = pa.schema([
    pa.field("event_ts", pa.int64()),    # when the aircraft transmitted
    pa.field("ingest_ts", pa.int64()),   # stamped by you, never by the library
    pa.field("icao24", pa.string()),
    pa.field("altitude_ft", pa.int64()),
    pa.field("speed_kt", pa.float64()),
])

# new() takes the shape; it is fixed at creation.
log = Log.new("data", "positions", schema=schema, sort_by=("event_ts", "icao24"))
log.append({"event_ts": 1, "ingest_ts": 2, "icao24": "a0f31c",
            "altitude_ft": 37000, "speed_kt": 461.2})   # durable when this returns

# open() takes none of it — schema, sort order, config and archive all come
# from the log itself.
log = Log.open("data", "positions")
log = Log.open("data", "positions", read_only=True)   # alongside a live writer

recent = log.scan(where="event_ts > 1000").read_all()
log.maintain()                                        # compact, evict, expire
```

## Sizing: two targets, not one

A seal wants to be **small** and a file wants to be **large**, and one number cannot serve
both:

| | bounded by | wants to be | because |
|---|---|---|---|
| `target_seal_size` | the buffer | small | the buffer is what a hot read scans, so its size is read latency (§7) |
| `target_compact_size` | a file on disk | large | per-file overhead dominates both scans and uploads |

Compaction is what bridges them, and that is its whole job: converting sealed chunks into
archive-shaped ones. It defaults to **8× the seal size**, so the conversion is on even
without an archive — file count is a measured cost here, not a reputation. Reading the
offset boundary from manifest statistics measured **1.0 ms over one file and 44 ms over
64**, and uploading a 9 kB file to S3 took **648 ms**, nearly all of it round trip rather
than bytes.

The price is bounded write amplification: each row is written twice locally, once at seal
and once at conversion, and never again — a converted file is already at the target, so it
is not a candidate a second time. Set `target_compact_size` equal to the seal size to turn
the conversion off.

Keep it a **multiple** of the seal size. Sealed files are uniform, so merging whole files
lands exactly on the target when it divides and short when it does not — three 1 MiB files
against a 4 MiB target give 3 MiB files for ever.

## Try it

```
just demo-websocket    # capture a live public feed, one process, ~30 seconds
```

No producer, no credentials, no maintainer: Bitstamp publishes BTC/USD trades over an
unauthenticated websocket, and the loop is `log.append(...)` then `log.seal_due()`. It
prints a query over what it captured.

The other end of the range — a synthetic feed you can drive as hard as you like, with one
process per storage role:

```
just demo-capture      # append continuously — the hot path, and nothing else
just demo-maintain     # in another terminal: seal, compact, evict, expire
just demo-tail         # in a third: watch where the rows are
just bench --quick     # write and read throughput on your hardware
```

Local only, no network. To add the archive tier against a local S3-compatible store:

```
just rustfs            # object storage in one container
just demo-archive      # capture, with an archive configured
just demo-maintain     # also pushes to it, and evicts what it has pushed
```

**Against a real AWS bucket**, nothing changes but the environment:

```
cp .env.example .env    # set LITELINK_DEMO_ARCHIVE=s3://your-bucket/prefix
just demo-archive
just demo-maintain
```

Credentials are not in that file unless you put them there. The library reads them from
the environment at the point of use through the ordinary AWS chain, so a profile, instance
metadata or SSO all work untouched — and a log directory, which gets copied and attached
elsewhere, never carries a key with it.

See [`examples/`](examples/).

## Development

```
just bootstrap          # uv sync + git hooks + DuckDB extensions
just check              # lint + format-check + typecheck + tests, same as CI
```

`just bootstrap` provisions the `iceberg` and `avro` DuckDB extensions, which are
downloaded rather than bundled — see [`docs/SPEC.md`](docs/SPEC.md) §7. `just
duckdb-extensions --check` verifies a machine can read offline. The buffer is **not**
read through DuckDB's sqlite scanner: two independently linked SQLite libraries in one
process corrupt the database, which [`docs/RUNTIME.md`](docs/RUNTIME.md) records.

`just --list` has the rest. Tooling is uv + ruff + [ty](https://github.com/astral-sh/ty)
+ pytest; the style gate in `scripts/check_blank_lines.py` requires a blank line after
every compound-statement block.

Commits follow [Conventional Commits](https://www.conventionalcommits.org), enforced by a
`commit-msg` hook (`scripts/check_commit_msg.py`):

```
<type>(<scope>): <lowercase description, no trailing period, subject <= 72 chars>

types   feat fix refactor perf test docs build chore
scopes  the subsystems in docs/SPEC.md — buffer, write, seal, sync, compaction, read,
        retention, schema, blob, catalog, config, replication — plus spec, ci, deps
```

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
