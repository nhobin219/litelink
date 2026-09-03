<p align="center">
  <img src="docs/assets/litelink-logo.svg" alt="litelink" width="330">
</p>

[![CI](https://github.com/nhobin219/litelink/actions/workflows/ci.yml/badge.svg)](https://github.com/nhobin219/litelink/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-Apache%20v2-blue)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)](pyproject.toml)
[![Iceberg](https://img.shields.io/badge/Apache%20Iceberg-v2-4B8BBE)](https://iceberg.apache.org/)

# Durable append-only capture into Iceberg tables

**Embedded and local-first.**

## Introduction

litelink is a Python library for the thing every capture pipeline hand-rolls badly: getting a
stream of observations onto disk durably, into well-sized Parquet, and eventually into object
storage — without a daemon, a broker, or a catalog service. `append()` returns once the row is
durable, and a query a moment later sees it.

```
SQLite buffer          durable on commit. unsealed rows only.
      │  seal at target_seal_size
      ▼
local Iceberg table    a rolling window. reads land here.
      │  sync: upload data files, register into the archive
      ▼
remote Iceberg table   full history, on S3.
```

Reads span all three tiers and the catalog is a SQLite file rather than a service, so **no
read on the hot path touches the network.** Every other machine reads the archive instead,
with any Iceberg engine and nothing from litelink.

It exists because doing this by hand goes wrong the same way every time: one production
capture system had 125,884 objects, 62.5% of them under 16 KiB, Parquet files at 2 rows each,
a compaction routine nothing ever scheduled, and an in-memory buffer a `SIGKILL` emptied.

**Status: early.** All three tiers work, and a log survives losing its machine. Read
[what it is not](#what-it-is-not) and [not implemented yet](#not-implemented-yet) first.

## Quick start

```bash
pip install litelink        # or: uv add litelink
```

```python
import litelink
import pyarrow as pa

schema = pa.schema([
    pa.field("trade_id", pa.int64()),
    pa.field("event_ts", pa.int64()),    # microseconds, as the exchange sends them
    pa.field("price", pa.float64()),
    pa.field("amount", pa.float64()),
])

# new() takes the shape, fixed at creation. open() takes none of it — schema,
# sort order, config and archive all come from the log itself.
log = litelink.new("data", "trades", schema=schema, sort_by=("event_ts",))

log.append({"trade_id": 624438572, "event_ts": 1787772776240000,
            "price": 78501.62, "amount": 0.0076})     # durable on return

# extend() commits the whole group in ONE transaction — one fsync for the batch,
# not one per row. That call size is the write throughput lever.
log.extend(group_of_rows)

recent = log.scan(where="event_ts > 1787772776000000").read_all()
log.maintain()                                        # compact, evict, expire
```

That is the whole API for local capture. A reader can open the same log alongside a live
writer with `litelink.open("data", "trades", read_only=True)`.

**Nothing else is required** — no producer, no credentials, no maintainer process, no
container. Object storage, WAL replication and cross-machine reads are all opt-in, and each
is one call.

Wheels for Linux and macOS on x86-64 and arm64 carry a checksum-verified litestream and the
DuckDB extensions litelink loads, so a box with no egress still reads, writes and restores.
That costs ~124 MB. Run `python -m litelink` to check a machine before you rely on it; see
[`docs/RUNTIME.md`](docs/RUNTIME.md) for anywhere else.

## Demos

Clone the repo for these; `just bootstrap` sets up the toolchain.

```bash
just demo-websocket    # a live public feed, one process, ~30 seconds
just demo-capture      # a synthetic feed, driven as hard as you like
just demo-maintain     # in another terminal: seal, compact, evict, expire
just rustfs            # object storage in a container, to add the archive tier
just demo-replicate    # ship the SQLite WAL, to survive losing the machine
```

Credentials are never written to the log directory — the library reads them from the
environment through the ordinary AWS chain, so a profile, instance metadata or SSO all work
untouched. `litelink.restore(root, name, archive=...)` rebuilds a log on another box,
reserving an offset window so nothing the dead machine served is reissued.

litelink emits the litestream config; your supervisor runs the binary. Full walkthrough in
[`examples/`](examples/) and [`docs/RUNTIME.md`](docs/RUNTIME.md).

## Reading it from another machine

`litelink.snapshot` is the way in. It resolves the archive's current metadata, handles
credentials, and hands back a read handle:

```python
import litelink

with litelink.snapshot("trades", archive="s3://bucket/prefix") as reader:
    reader.sql("SELECT count(*), max(litelink_offset) FROM log").read_all()
```

`archive` is the prefix the logs sit under and `"trades"` is the log, so this reads
`s3://bucket/prefix/trades/`. Credentials come from the environment; pass
`s3=litelink.S3Options(endpoint=…)` for somewhere that is not AWS. `sql` exposes the log as
`log`; `scan(where=…, columns=…)` is the typed equivalent, and both return a
`pa.RecordBatchReader` rather than a table, so materialising is yours to choose.

By default this reads the **archive alone** — no replica, no litestream, no subprocess — and
assembles in well under a second. The view is as of the archive frontier, which on a quiet
stream can lag indefinitely rather than by the sync interval, because `sync` holds back a
trailing run under `target_compact_size`. `coverage()` reports what it can actually serve.

**Pass `include_wal=True` when you want the freshest read there is.** With the writer running a
WAL sidecar, its `buffer.db` is restored from the replica and merged with the archive, so you
see down to the replication lag rather than to the last `sync`:

```python
with litelink.snapshot("trades", archive="s3://bucket/prefix", include_wal=True) as reader:
    print(reader.coverage())
    # Coverage(archive=(1, 1928), buffered=(1929, 2100), gap=None, wal_replication=True)
```

It needs `wal_replication` on and a sidecar that has shipped; without a replica it raises,
which is why it is not the default.

That restore is the expensive part: measured against a 276k-row log, 22 s to assemble against
1.4 s per scan — and it scales with the buffer FILE's size rather than its row count, so a
writer whose buffer has grown a large free list makes every reader slower (`reclaim_buffer()`
shrinks it). Assemble once and scan many times; re-entering the `with` block per query pays it
every time.

Either way it is a **snapshot, not a subscription** — refreshing means assembling another one —
and it cannot append: a read handle has no write surface at all, rather than one that raises.

### Or any Iceberg engine

The archive is an ordinary Iceberg table that publishes `version-hint.text` at every commit, so
an engine pointed at the prefix resolves the current metadata itself. No catalog service, no
local root, no litelink install:

```python
import duckdb

con = duckdb.connect()
con.execute("CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1');")

table = con.execute("""
    SELECT count(*), max(litelink_offset)
    FROM iceberg_scan('s3://bucket/prefix/trades',
                      version_name_format = '%s%s.metadata.json')
""").arrow().read_all()
```

Point it at the table DIRECTORY — `<archive>/<name>` — not at a metadata JSON.
**`version_name_format` is not optional**: DuckDB defaults to the Hadoop `v%s%s.metadata.json`
while pyiceberg names its metadata `00003-<uuid>.metadata.json`, so the format has to stop
prepending the `v`. `credential_chain` is the ordinary AWS resolution — profile, instance
metadata, SSO; against another endpoint pass `KEY_ID`, `SECRET`, `ENDPOINT` and
`URL_STYLE 'path'` instead. No `INSTALL`/`LOAD` is needed — DuckDB autoloads `iceberg`, `avro`
and `httpfs` when a query names them, and `just bootstrap` provisions them ahead of time so the
first read is not a download.

That is the same question the first snippet asks, and on a default snapshot it returns the same
answer — both read the archive, one through litelink's union and one straight at the table.

**Which to reach for.** A one-shot query in a script is cheaper this way: `snapshot` assembles a
DuckDB connection, a scratch buffer and an adopted catalog before it can answer anything, and
you exit before reusing any of it. Hold a snapshot open and the order reverses — measured on a
200k-row archive, a bounded scan is 0.02 s against 0.40 s for a fresh DuckDB connection, because
the connection and the loaded extensions are already there. Assemble once and scan many times,
or use the query above.

`litelink_offset` is monotonic and never reused, so a reader keeps the highest it has seen and
asks for what came after — which is how you poll the archive as it grows.

Details in [`docs/API.md`](docs/API.md).

## How it works

- **Iceberg is used, not reimplemented.** Manifests, per-file column statistics, schema with
  field IDs, and atomic snapshot commits all come from it.
- **The library owns exactly one column**, `litelink_offset` — monotonic, never reused. It is
  the boundary mechanism between tiers. Everything else is the caller's schema.
- **Parts are sealed once and never rewritten.** Rewriting a growing partition costs ~144x
  write amplification and buys nothing, because the local WAL already made the row durable.
- **Read boundaries come from committed table state**, never from a stored flag — so no seal
  window can double-count or drop.
- **Sizing is two targets, not one.** A seal wants to be small, because the buffer is what a
  hot read scans; a file wants to be large, because per-file overhead dominates scans and
  uploads. Compaction bridges them, on local disk, at 8× the seal size by default.

Read performance is the cost of reading Parquet, plus ~4 ms of fixed overhead. The reasoning
and the measurements are in [`docs/SPEC.md`](docs/SPEC.md); `just bench` reruns them on your
hardware.

### On disk

One directory per stream, holding everything that stream owns — and the archive prefix
mirrors it, so a stream can be copied, replicated or deleted whole in either tier:

```
data/trades/                     s3://bucket/prefix/trades/
    buffer.db                        _wal/
    catalog.db                           buffer.db/
    archive.db                           catalog.db/
    litestream.yml                       archive.db/
    data/                            data/
        *.parquet                        *.parquet
        compacted/*.parquet              compacted/*.parquet
        ingested/*.parquet               ingested/*.parquet
    metadata/                        metadata/
        *.metadata.json                  *.metadata.json
        *.avro                           *.avro
                                         version-hint.text
```

Data files sit under the table's own location, so the path an engine reads
(`s3://bucket/prefix/trades`) is the directory that holds both halves of the table.

Upgrading a log written by 0.1.0: see [Migrating from 0.1](docs/RUNTIME.md#migrating-from-01).

## What it is not

**Not an OLTP or key-value store.** A point lookup is ~1,600x slower than an indexed row
store, and no configuration closes that gap. It is a local, in-process, real-time analytics
store: freshness is sub-second *with* durability, but "real-time" means fresh, not
point-lookup fast.

**Not an unbounded local archive.** A seal's cost tracks what the table's metadata holds, so
a log that never runs `maintain()` and never evicts gets slower on the write path over time.
`maintain()` arrests the larger factor; a retention bounds the rest. Numbers and the
reasoning are in [`docs/SPEC.md`](docs/SPEC.md) §13.7.

## Not implemented yet

**Schema evolution** is half built: `add_column` works, `rename_column` and `drop_column`
raise `NotImplementedError`. **Blob fields** are specified and unbuilt — `binary` columns are
refused outright. Payload encoding, local-disk backpressure and bulk ingest are open. See
[`docs/SPEC.md`](docs/SPEC.md) §9, §15 and §13.

## Documentation

- [`docs/API.md`](docs/API.md) — every public call, on one page
- [`docs/SPEC.md`](docs/SPEC.md) — the design, and in places still ahead of the code
- [`docs/RUNTIME.md`](docs/RUNTIME.md) — writer and maintainer, threads, processes, what crosses between them
- [`examples/`](examples/) — the websocket capture, and the synthetic feed with one process per role
- [`benchmarks/`](benchmarks/) — the harness, including what litelink costs over raw SQLite
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — setup, the gates, and what a good PR here looks like
- [`SECURITY.md`](SECURITY.md) — what to report privately, and what is a known limit instead

## Development

```bash
just bootstrap          # uv sync + git hooks + DuckDB extensions + litestream
just check              # lint + format-check + typecheck + tests, same as CI
just --list             # the rest
```

A checkout downloads the DuckDB extensions and litestream that an installed wheel carries, so
a contributor provisions what a user does not. Tooling is uv + ruff +
[ty](https://github.com/astral-sh/ty) + pytest; commits follow
[Conventional Commits](https://www.conventionalcommits.org), enforced by a hook. See
[`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
