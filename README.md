<p align="center">
  <img src="docs/assets/litelink-logo.svg" alt="litelink" width="330">
</p>

[![CI](https://github.com/nhobin219/litelink/actions/workflows/ci.yml/badge.svg)](https://github.com/nhobin219/litelink/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-Apache%20v2-blue)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)](pyproject.toml)
[![Iceberg](https://img.shields.io/badge/Apache%20Iceberg-v2-4B8BBE)](https://iceberg.apache.org/)

# Durable append-only capture into Iceberg tables

**Local-first, in-process, no service.**

## Introduction

litelink is a Python library for the thing every capture pipeline hand-rolls badly: getting a
stream of observations onto disk durably, into well-sized Parquet, and eventually into object
storage — without a daemon, a broker, or a catalog service. `append()` returns once the row is
durable, and a query a moment later sees it.

Rows land in a SQLite buffer under an ordinary transaction, seal into a local Iceberg table
once they reach a target size, and sync from there into an Iceberg table on S3:

```
SQLite buffer          durable on commit. unsealed rows only.
      │  seal at target_size
      ▼
local Iceberg table    a rolling window. reads land here.
      │  sync: upload data files, register into the archive
      ▼
remote Iceberg table   full history, on S3.
```

Reads span all three tiers, and the catalog is a SQLite file rather than a service, so **no
read on the hot path touches the network.**

It exists because doing this by hand goes wrong the same way every time: one production capture
system had accumulated 125,884 objects with 62.5% of them under 16 KiB, Parquet files at 2 rows
each, a compaction routine nothing ever scheduled, and an in-memory write buffer that a
`SIGKILL` emptied. Durable capture, file sizing and tiering are each easy alone and nobody's
job together.

**Status: early.** All three tiers work, and a log survives losing its machine. Read
[what it is not](#what-it-is-not) and [not implemented yet](#not-implemented-yet) before you
commit to it.

## Quick start

```bash
git clone https://github.com/nhobin219/litelink && cd litelink
just bootstrap         # uv sync + git hooks + DuckDB extensions
just demo-websocket    # capture a live public feed, one process, ~30 seconds
```

You need [`uv`](https://docs.astral.sh/uv/) and [`just`](https://github.com/casey/just).
Nothing else: no producer, no credentials, no maintainer, no container. Bitstamp publishes
BTC/USD trades over an unauthenticated websocket, the loop is `log.append(...)` then
`log.seal_due()`, and it prints a query over what it captured.

That demo is [`examples/websocket.py`](examples/websocket.py), and this is its shape:

```python
import pyarrow as pa
from litelink import Log

# A trade feed: durable the moment it arrives, queryable a moment later.
schema = pa.schema([
    pa.field("trade_id", pa.int64()),
    pa.field("event_ts", pa.int64()),    # microseconds, as the exchange sends them
    pa.field("price", pa.float64()),
    pa.field("amount", pa.float64()),
    pa.field("side", pa.int64()),        # 0 buy, 1 sell
])

# new() takes the shape; it is fixed at creation.
log = Log.new("data", "trades", schema=schema, sort_by=("event_ts",))
log.append({"trade_id": 624438572, "event_ts": 1787772776240000,
            "price": 78501.62, "amount": 0.0076, "side": 0})  # durable on return

# open() takes none of it — schema, sort order, config and archive all come
# from the log itself.
log = Log.open("data", "trades")
log = Log.open("data", "trades", read_only=True)   # alongside a live writer

recent = log.scan(where="event_ts > 1787772776000000").read_all()
log.maintain()                                     # compact, evict, expire
```

That is the whole API surface for local capture. Everything below is optional.

## More demos

The other end of the range — a synthetic feed you can drive as hard as you like, with one
process per storage role:

```bash
just demo-capture      # append continuously — the hot path, and nothing else
just demo-maintain     # in another terminal: seal, compact, evict, expire
just demo-tail         # in a third: watch where the rows are
just bench --quick     # write and read throughput on your hardware
```

To add the archive tier against a local S3-compatible store:

```bash
just rustfs            # object storage in one container
just demo-archive      # capture, with an archive configured
just demo-maintain     # also pushes to it, and evicts what it has pushed
```

**Against a real AWS bucket**, nothing changes but the environment:

```bash
cp .env.example .env    # set LITELINK_DEMO_ARCHIVE=s3://your-bucket/prefix
just demo-archive
just demo-maintain
```

Credentials are not in that file unless you put them there. The library reads them from the
environment at the point of use through the ordinary AWS chain, so a profile, instance
metadata or SSO all work untouched — and a log directory, which gets copied and attached
elsewhere, never carries a key with it.

To survive losing the machine, ship the SQLite WAL alongside:

```bash
just litestream        # once: fetch the pinned, checksum-verified sidecar
just demo-replicate    # generates litestream.yml from the log, runs it
```

Then `Log.restore(root, name, archive=...)` rebuilds the log on another box — it writes the
config from the layout alone, restores `buffer.db`, rebuilds the local table, adopts the
archive, and reserves an offset window so nothing the dead machine served is reissued.
Verified against a local S3-compatible store and against AWS. See [`examples/`](examples/).

## How it works

- **Iceberg is used, not reimplemented.** Manifests, per-file column statistics, schema with
  field IDs, and atomic snapshot commits all come from it.
- **The library owns exactly one column**, `litelink_offset` — monotonic, never reused. It is
  the boundary mechanism between tiers. Everything else is the caller's schema.
- **Sync is a watermark, not CDC.** There are no updates or deletes to replicate.
- **Parts are sealed once and never rewritten.** Rewriting a growing partition costs ~144x
  write amplification and buys nothing, because the local WAL already made the row durable.
- **Read boundaries are derived from committed table state**, never from a stored flag — so
  no seal window can double-count or drop.
- **The seal cut is chosen by the appender**, in the transaction that crosses `target_size`,
  and queued. A sealer that falls behind therefore writes several correctly-sized files rather
  than one oversized one.
- **Compaction is local**, and in normal operation a no-op: the seal cuts on
  `target_seal_size` alone, so every file already lands at that size. What it is for is
  converting seals into archive-shaped files when `target_compact_size` is larger. Local means
  no egress to read files back.

Read performance is the cost of reading Parquet, plus ~4 ms of fixed overhead.

How the pieces run — writer and maintainer, what crosses between them, and what is safe to
call from which thread or process — is [`docs/RUNTIME.md`](docs/RUNTIME.md).

### Sizing: two targets, not one

A seal wants to be **small** and a file wants to be **large**, and one number cannot serve
both:

| | bounded by | wants to be | because |
|---|---|---|---|
| `target_seal_size` | the buffer | small | the buffer is what a hot read scans, so its size is read latency (§7) |
| `target_compact_size` | a file on disk | large | per-file overhead dominates both scans and uploads |

Compaction is what bridges them, and that is its whole job: converting sealed chunks into
archive-shaped ones. It defaults to **8× the seal size**, so the conversion is on even without
an archive — file count is a measured cost here, not a reputation. Reading the offset boundary
from manifest statistics measured **1.0 ms over one file and 44 ms over 64**, and uploading a
9 kB file to S3 took **648 ms**, nearly all of it round trip rather than bytes.

The price is bounded write amplification: each row is written twice locally, once at seal and
once at conversion, and never again — a converted file is already at the target, so it is not
a candidate a second time. Set `target_compact_size` equal to the seal size to turn the
conversion off.

Keep it a **multiple** of the seal size. Sealed files are uniform, so merging whole files
lands exactly on the target when it divides and short when it does not — three 1 MiB files
against a 4 MiB target give 3 MiB files for ever.

## What it is not

An OLTP or key-value store. A point lookup is ~1,600x slower than an indexed row store, and no
configuration closes that gap. It is a **local, in-process, real-time analytics store**: data
is durable at commit and queryable immediately, so freshness is sub-second *with* durability —
but "real-time" means fresh, not point-lookup fast.

Nor is it an unbounded local archive. **Keeping everything on one machine — `archive=None`
with no `local_retention` — degrades as the table grows.** A seal's cost tracks what the
table's metadata holds, and while running `maintain()` bounds the largest part of that, a
residue grows with the file count: compaction groups adjacent files whose combined
uncompressed size fits the target, so one it has already produced at that size is never
revisited, and only eviction removes it. With no retention set nothing evicts. The seal is on
the write path, so the cost lands on appends.

Configure a retention, or an archive to evict into, for anything long-running. Both bound the
file count, which is what bounds the cost. Details and the options for fixing it properly are
in [`docs/SPEC.md`](docs/SPEC.md) §13.7.

## Not implemented yet

**Schema evolution** ([`docs/SPEC.md`](docs/SPEC.md) §9) and **blob fields** (§15) are
specified and unbuilt, and are what the code lacks against its own design.

Blob fields are for payloads too large for the buffer (sensor frames, point clouds, raw
response bodies). Bytes stage beside SQLite while hot and are inlined into Iceberg at seal, so
the archive stays ordinary Iceberg with a `binary` column: no pointers in the published
schema, and blob lifetime inherits snapshot expiry and compaction rather than becoming a
hand-maintained refcount. Until it lands, `binary` columns are refused outright — see
`_types`. (They were once refused because DuckDB's sqlite scanner decoded blob bytes as UTF-8
and failed; that scanner is gone, so the remaining reason is §15 itself, which has payloads
bypass the buffer rather than travel through it.)

Still open: payload encoding, local-disk backpressure, bulk ingest, and extension provisioning
for embedders. All four in [`docs/SPEC.md`](docs/SPEC.md) §13.

## Documentation

- [`docs/SPEC.md`](docs/SPEC.md) — the design, and in places still ahead of the code
- [`docs/RUNTIME.md`](docs/RUNTIME.md) — writer and maintainer, threads, processes, what crosses between them
- [`examples/`](examples/) — the websocket capture, and the synthetic feed with one process per role
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — setup, the gates, and what a good PR here looks like
- [`SECURITY.md`](SECURITY.md) — what to report privately, and what is a known limit instead

## Development

```bash
just bootstrap          # uv sync + git hooks + DuckDB extensions
just check              # lint + format-check + typecheck + tests, same as CI
```

`just bootstrap` provisions the `iceberg`, `avro` and `httpfs` DuckDB extensions, which are
downloaded rather than bundled — see [`docs/SPEC.md`](docs/SPEC.md) §7. `just
duckdb-extensions --check` verifies a machine can read offline. The buffer is **not** read
through DuckDB's sqlite scanner: two independently linked SQLite libraries in one process
corrupt the database, which [`docs/RUNTIME.md`](docs/RUNTIME.md) records.

`just --list` has the rest. Tooling is uv + ruff + [ty](https://github.com/astral-sh/ty) +
pytest; the style gate in `scripts/check_blank_lines.py` requires a blank line after every
compound-statement block.

Commits follow [Conventional Commits](https://www.conventionalcommits.org), enforced by a
`commit-msg` hook (`scripts/check_commit_msg.py`):

```
<type>(<scope>): <lowercase description, no trailing period, subject <= 72 chars>

types   feat fix refactor perf test docs build chore
scopes  benchmarks blob buffer catalog ci compaction config deps examples read
        replication retention schema seal spec sync write
```

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
