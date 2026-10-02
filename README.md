<p align="center">
  <img src="docs/assets/litelink-logo.svg" alt="litelink" width="330">
</p>

[![CI](https://github.com/nhobin219/litelink/actions/workflows/ci.yml/badge.svg)](https://github.com/nhobin219/litelink/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-Apache%20v2-blue)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)](pyproject.toml)
[![Iceberg](https://img.shields.io/badge/Apache%20Iceberg-v2-4B8BBE)](https://iceberg.apache.org/)

# An embedded Iceberg storage engine for append-only data

litelink takes high-throughput transactional appends and turns them into one well-sized
Iceberg table per log — on S3, or in a local directory. `append()` commits to a SQLite buffer
and returns once the row is durable. Behind it, the library seals rows into sorted Parquet,
compacts small files up to a target size, publishes settled files to that table, and evicts
from local disk what it already holds. Through all of it the log stays one queryable
unit: a read sees every row exactly once, whichever tier holds it, and maintenance runs beside
appends rather than in front of them, so no pass blocks appends for its duration or shows a
reader a half-finished state.

```
append() ──► SQLite buffer          durable on commit
                   │  seal: sorted Parquet at target_seal_size
                   ▼
             staging Iceberg table  compacted to target size, evicted once published
                   │  publish: upload, register
                   ▼
             published Iceberg table  full history, on S3 or in a local directory

scan() / sql() ──► one relation across all three tiers, each row once
```

It runs inside your process, like DuckDB, with no server, daemon or catalog service: what
DuckDB is to query execution, litelink is to the durable write path. The files it writes are
the product. The Parquet a row is sealed into is the Parquet DuckDB, or any other Iceberg
engine, reads, with no export step in between.

|  | DuckDB | litelink |
|---|---|---|
| runs | in your process | in your process |
| without | a server, daemon or cluster | a server, daemon or catalog service |
| owns | the query | the durable write path |
| speaks | SQL over Parquet and Arrow | Iceberg v2, on disk and in object storage |

It's built for the thing every capture pipeline hand-rolls badly: getting a stream of
observations onto disk durably, into well-sized Parquet, and eventually into object storage.
Doing that by hand goes wrong the same way every time: one production capture system had
125,884 objects, 62.5% of them under 16 KiB, Parquet files at 2 rows each, a compaction
routine nothing ever scheduled, and an in-memory buffer a `SIGKILL` emptied.

## The Iceberg table is the product

The usual shape is a write path in one system and an analytical store in another, with a job
copying between them. Here they are one log: rows land in the SQLite buffer, seal into a
staging table, and are published to the log's one output — an Iceberg table on S3, or
in a local directory for a log with no S3 published table. litelink reads across all three, so **no
read on the hot path touches the network**. Everything else reads the published table with
any Iceberg engine, through its `version-hint.text`, with no catalog and no litelink:

```python
import duckdb
import litelink

# Written, durable on return.
log = litelink.open("data", "trades")
log.append({"trade_id": 624438572, "event_ts": 1787772776240000,
            "price": 78501.62, "amount": 0.0076})

# Read on the same box, across whichever tiers could hold a match.
log.sql("SELECT count(*), max(price) FROM log").read_all()

# Read the published table with any Iceberg engine, and litelink not installed at all —
# from S3, or for a local-only log from data/trades/published/trades.
duckdb.sql("""
    SELECT count(*), max(price)
    FROM iceberg_scan('s3://bucket/prefix/trades',
                      version_name_format = '%s%s.metadata.json')
""")
```

The published table holds what `publish` has pushed, which trails the buffer by the publish
interval; rows newer than that are readable through litelink on the writer's machine.

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

## Install

```bash
pip install litelink        # or: uv add litelink
```

**Nothing else is required** — no producer, no credentials, no maintainer process, no
container. Object storage and WAL replication are opt-in, and each is one setting; another
machine reads the published table with no litelink at all.

Wheels for Linux and macOS on x86-64 and arm64 carry a checksum-verified litestream and the
DuckDB extensions litelink loads, so a box with no egress still reads, writes and restores.
That costs ~124 MB. Run `python -m litelink` to check a machine before you rely on it; see
[`docs/RUNTIME.md`](docs/RUNTIME.md) for anywhere else.

## API

```python
litelink.new(root, name, *, schema, sort_by=None, config=None, published=None,
             s3=None, start_offset=1)                            -> WriteHandle
litelink.open(root, name, *, s3=None)                              -> WriteHandle
litelink.open(root, name, *, read_only=True, ...)                  -> LocalReadHandle
litelink.restore(root, name, *, published, s3=None, ...)             -> WriteHandle
litelink.validate_row(schema, row)                                 # raises as append would
litelink.preflight(...)                                            # what python -m litelink runs

# Every handle reads:
    log.scan(*, columns=None, where=None, start_offset=None, end_offset=None, published=True)
    log.sql(query, *, published=True)                 # the log is `log`; both stream Arrow
    log.column_statistics(*, tier=None) · log.coverage(*, published=True)   # tier: staging|published|buffer|None
    log.end_offset() · buffered_rows() · staging_rows() · staging_files() · published_through()
    log.schema · sort_by · config · published

# A WriteHandle also writes:
    log.append(row) -> int                          # durable on return
    log.extend(rows) -> list[int]                   # ONE transaction, one fsync
    log.ingest(table_or_reader)                     # Arrow straight to Parquet
    log.seal() · log.advance()                 # seal; the whole pipeline, publish included
    log.publish(*, flush=False)            # push to the published table
    log.retire()                                    # end the log: all published, none local
    log.set_config(...) · set_published(...) · set_sort_by(..., rewrite=True)
```

The deliberate choices:

- **Handles, not logs.** A read handle has no write methods at all, rather than ones that
  raise, and `open(..., read_only=True)` is typed so misuse is caught before it runs.
- **`new` takes the shape; `open` takes none of it.** Schema, sort order, config and published table
  live in the log, so nothing at the call site can disagree with what is on disk.
- **The library owns no thread.** Nothing seals unless you call `seal()` or `advance()`;
  your loop is the schedule.
- **Which tiers a query reads is decided per query**, from its predicates. A query bounded
  inside the staging window never touches the network, however much has been evicted.

Full reference in [`docs/API.md`](docs/API.md).

## Writing

```python
import litelink
import pyarrow as pa

schema = pa.schema([
    pa.field("trade_id", pa.int64()),
    pa.field("event_ts", pa.int64()),    # microseconds, as the exchange sends them
    pa.field("price", pa.float64()),
    pa.field("amount", pa.float64()),
])

log = litelink.new("data", "trades", schema=schema, sort_by=("event_ts",))

log.append({"trade_id": 624438572, "event_ts": 1787772776240000,
            "price": 78501.62, "amount": 0.0076})     # durable on return
log.extend(group_of_rows)                             # the throughput lever
log.advance()                                        # seal, compact, publish, evict, expire, sweep
```

`extend()` commits the whole group in one transaction, so it is one fsync for the batch
rather than one per row, and that call size is the write-throughput lever. Loading history is
`ingest()`, which writes Arrow straight to Parquet.

## Reading

`sql` exposes the log as `log`; `scan(where=…, columns=…)` is the typed equivalent, and both
return a `pa.RecordBatchReader` rather than a table, so materialising is yours to choose. A
reader can open the same log alongside a live writer with
`litelink.open("data", "trades", read_only=True)`.

**litelink decides which tiers a query reads.** Every query reads the buffer; the staging table
and the published table are read only when they could hold a matching row — for the published
table, a file below the staging table. That is decided from per-column bounds for each tier —
the staging table's from its own Iceberg manifests, the published table's kept in `buffer.db` —
so the decision itself never touches the network:

```python
log.scan(where="event_ts > 1787772000000000")   # recent: local disk only
log.scan(where="event_ts < 1700000000000000")   # history: reads the published table too
log.scan()                                      # the whole log
log.scan(published=False)                       # local disk only, whatever it asks
```

So a query's latency follows its predicates. Bound it on a leading column of `sort_by` and a
recent window stays local; leave it unbounded and it reads every tier, because the whole log
is the right answer. Anything the decision cannot read — an OR, a subquery, a comparison with
something other than a constant — reads the published table rather than risk skipping a
row. The buffer keeps no column statistics, so only an offset bound (`scan(start_offset=…,
end_offset=…)`) can skip it.
`column_statistics(tier=…)` gives every column's bounds and counts without opening a data
file, per tier (`"staging"`, `"published"` below it, `"buffer"`) or for the whole log.

**`retire()` ends a log for good.** It pushes every row to the published table, empties the
staging table and the buffer, and records the retirement by giving the buffer an end and
marking the published table. After that the log opens for reading only, and `append`,
`ingest`, a writer `open` and `restore` all refuse, naming the offset the next log should
start at.

## Reading from another machine

litelink reads on the primary: every handle is on the host that holds the log's root. Off that
host, the published table **is** the interface. It is an ordinary Iceberg table that publishes
`version-hint.text` at every commit, so an engine pointed at the prefix resolves the current
metadata itself. No catalog service, no local root, no litelink install:

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

Point it at the table DIRECTORY — `<published>/<name>` — not at a metadata JSON.
**`version_name_format` is not optional**: DuckDB defaults to the Hadoop `v%s%s.metadata.json`
while pyiceberg names its metadata `00003-<uuid>.metadata.json`, so the format has to stop
prepending the `v`. `credential_chain` is the ordinary AWS resolution — profile, instance
metadata, SSO; against another endpoint pass `KEY_ID`, `SECRET`, `ENDPOINT` and
`URL_STYLE 'path'` instead. No `INSTALL`/`LOAD` is needed — DuckDB autoloads `iceberg`, `avro`
and `httpfs` when a query names them, and `just bootstrap` provisions them ahead of time so the
first read is not a download.

It reads what the published table holds, which on a quiet stream can lag the writer indefinitely
rather than by the publish interval, because `publish` holds back a trailing run under
`target_compact_size`. Rows still in the primary's buffer or staging table are only readable on
the primary.

`litelink_offset` is monotonic and never reused, so a reader keeps the highest it has seen and
asks for what came after — which is how you poll the published table as it grows.

## Demos and recovery

```bash
just demo-websocket    # a live public feed, one process, ~30 seconds
just demo-capture      # a synthetic feed, driven as hard as you like
just demo-maintain     # in another terminal: seal, compact, publish, evict
just rustfs            # object storage in a container, to publish to S3
just demo-replicate    # ship the SQLite WAL, to survive losing the machine
```

Clone the repo for these; `just bootstrap` sets up the toolchain. Credentials are never
written to the log directory — the library reads them from the environment through the
ordinary AWS chain, so a profile, instance metadata or SSO all work untouched.
`litelink.restore(root, name, published="s3://...")` rebuilds a log on another box, reserving an
offset window so nothing the dead machine served is reissued.

litelink emits the litestream config; your supervisor runs the binary. Full walkthrough in
[`examples/`](examples/) and [`docs/RUNTIME.md`](docs/RUNTIME.md).

## On disk

One directory per stream, holding everything that stream owns — and the published prefix
mirrors it, so a stream can be copied, replicated or deleted whole in either tier:

```
data/trades/                     s3://bucket/prefix/trades/
    buffer.db                        _wal/
    catalog.db                           buffer.db/
    published.db                         catalog.db/
    litestream.yml                       published.db/
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
(`s3://bucket/prefix/trades`) is the directory that holds both halves of the table. A log with
no S3 published table keeps the same table on local disk, at `data/trades/published/trades/`,
with no `_wal/`. A log written before 0.6 keeps `archive.db` where a new one has `published.db`.

Upgrading a log written by 0.1.0 takes litelink 0.5.1 first: see
[Migrating from 0.1](docs/RUNTIME.md#migrating-from-01).

## What it is not

- **Not a mutable store.** Rows are only appended, never updated or deleted in place, and a
  log's schema is fixed when it is created. To change the schema, `retire()` the log and start
  a new one where it ended: `new(root, "trades-v2", schema=…, start_offset=old.end_offset())`.
  Offsets stay dense across the two, and any engine reads both as one sequence.

- **Not an unbounded staging table.** A seal's cost tracks what the table's metadata holds, so
  a log that never runs `advance()` and never evicts gets slower on the write path over time.
  `advance()` arrests the larger factor; a retention, with `publish()` running, bounds the
  rest. Numbers and the reasoning are in [`docs/SPEC.md`](docs/SPEC.md) §13.7.

## Not implemented yet

- **Indexes for point lookups.** A lookup by key scans the tiers, pruned only by min/max
  statistics, so finding one row costs a scan rather than a seek — least on `sort_by`'s leading
  column, where the statistics are tight.

- **Blob fields** — large payloads that bypass the buffer — are specified and unbuilt;
  `binary` columns are carried, for ids and other small values rather than payloads
  ([SPEC](docs/SPEC.md) §15).

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
