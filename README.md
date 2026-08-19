# litelink

Durable append-only capture into Iceberg tables. Local-first, in-process, no service.

**Status: early.** The local capture loop works — append, seal, read, compact, evict,
expire — against a local Iceberg table. The archive tier does not: `sync()` and archive
reads raise `NotImplementedError`, so today this is durable local capture into Iceberg
rather than the full three-tier design. Schema evolution is likewise unimplemented. The
design is [`docs/SPEC.md`](docs/SPEC.md), and it is ahead of the code.

---

## What it is

A library for the thing every capture pipeline hand-rolls badly: getting a stream of
observations onto disk durably, into well-sized Parquet, and eventually into object storage
— without a daemon, a broker, or a catalog service.

```
SQLite buffer          durable on commit. unsealed rows only.
      │  seal at min(target_size, max_age)
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
- **Compaction is required and local**, because a time-based seal trigger guarantees
  undersized files. Local means no egress to read files back.

Read performance is the cost of reading Parquet, plus ~4 ms of fixed overhead.

## Extensions

Not implemented; specified.

**Blob fields** ([`docs/SPEC.md`](docs/SPEC.md) §15) — payloads too large for the buffer
(sensor frames, point clouds, raw response bodies). Bytes stage beside SQLite while hot and
are inlined into Iceberg at seal, so the archive stays ordinary Iceberg with a `binary`
column: no pointers in the published schema, and blob lifetime inherits snapshot expiry and
compaction rather than becoming a hand-maintained refcount.

Until it lands, `binary` columns are refused outright. The read path pushes its boundary
predicate down into SQLite — which is what stops cleanup costing query latency — and the
mechanism that allows it cannot carry blob bytes. That constraint and the extension point
the same way: §15 has payloads bypass the buffer rather than travel through it.

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

## Try it

```
just demo-capture      # append continuously, maintaining on a background thread
just demo-tail         # in another terminal: watch the log accumulate
just bench --quick     # write and read throughput on your hardware
```

See [`examples/`](examples/).

## Development

```
just bootstrap          # uv sync + git hooks + DuckDB extensions
just check              # lint + format-check + typecheck + tests, same as CI
```

`just bootstrap` provisions the `iceberg` and `sqlite` DuckDB extensions, which are
downloaded rather than bundled — see [`docs/SPEC.md`](docs/SPEC.md) §7. `just
duckdb-extensions --check` verifies a machine can read offline.

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
