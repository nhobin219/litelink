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

Reads span all three tiers, and the catalog is a SQLite file rather than a service, so **no
read on the hot path touches the network.** Every other machine reads the archive instead,
with any Iceberg engine and nothing from litelink.

It exists because doing this by hand goes wrong the same way every time: one production
capture system had 125,884 objects, 62.5% of them under 16 KiB, Parquet files at 2 rows each,
a compaction routine nothing ever scheduled, and an in-memory buffer a `SIGKILL` emptied.
Durable capture, file sizing and tiering are each easy alone and nobody's job together.

**Status: early.** All three tiers work, and a log survives losing its machine. Read
[what it is not](#what-it-is-not) and [not implemented yet](#not-implemented-yet) first.

## Install

```bash
pip install litelink        # or: uv add litelink
```

That is the whole of it. The wheel carries what the library shells out to — a
checksum-verified litestream, and the DuckDB `iceberg`, `avro` and `httpfs` extensions built
for the DuckDB it pins — so a machine with no egress, nothing on `PATH` and no DuckDB
extension cache still reads, writes and restores. Verified that way in CI, not assumed.

That costs about 124 MB per wheel, and it buys the failure mode you do not want: a missing
binary discovered during a failover, or an extension that autoinstalls fine on your laptop
and cannot on the box that matters.

```bash
python -m litelink          # PASS/FAIL per requirement, non-zero exit if not ready
```

Platform wheels are published for Linux and macOS on x86-64 and arm64. Anywhere else, pip
builds from the sdist — which produces a working pure-Python wheel with no binaries — and you
supply litestream and the DuckDB extensions yourself; `python -m litelink` tells you which are
missing and how to get them.

## Quick start

```bash
git clone https://github.com/nhobin219/litelink && cd litelink
just bootstrap         # uv sync + git hooks + DuckDB extensions
just demo-websocket    # capture a live public feed, one process, ~30 seconds
```

For working ON litelink you need [`uv`](https://docs.astral.sh/uv/) and
[`just`](https://github.com/casey/just); for using it you need neither. Either way the demo
needs nothing else: no producer, no credentials, no maintainer, no container. That demo is
[`examples/websocket.py`](examples/websocket.py), and this is its shape:

```python
import litelink
import pyarrow as pa

# A trade feed: durable the moment it arrives, queryable a moment later.
schema = pa.schema([
    pa.field("trade_id", pa.int64()),
    pa.field("event_ts", pa.int64()),    # microseconds, as the exchange sends them
    pa.field("price", pa.float64()),
    pa.field("amount", pa.float64()),
    pa.field("side", pa.int64()),        # 0 buy, 1 sell
])

# new() takes the shape, fixed at creation. open() takes none of it — schema,
# sort order, config and archive all come from the log itself.
log = litelink.new("data", "trades", schema=schema, sort_by=("event_ts",))
log.append({"trade_id": 624438572, "event_ts": 1787772776240000,
            "price": 78501.62, "amount": 0.0076, "side": 0})  # durable on return

# extend() commits the whole group in ONE transaction — one fsync for the batch,
# not one per row. That call size is the write throughput lever, and a call-site
# choice: no LogConfig setting tunes it.
log.extend(group_of_rows)                          # append(row) is extend([row])

log = litelink.open("data", "trades")
reader = litelink.open("data", "trades", read_only=True)         # alongside a live writer, no write surface

recent = log.scan(where="event_ts > 1787772776000000").read_all()
log.maintain()                                     # compact, evict, expire
```

That is the whole API surface for local capture. Everything below is optional, and every
public call is in [`docs/API.md`](docs/API.md) — one page, forty-one of them, and most
deployments use six.

## More demos

A synthetic feed you can drive as hard as you like, with one process per storage role:

```bash
just demo-capture      # append continuously — the hot path, and nothing else
just demo-maintain     # in another terminal: seal, compact, evict, expire
just demo-tail         # in a third: watch where the rows are
```

To add the archive tier, against a local S3-compatible store or a real bucket:

```bash
just rustfs            # object storage in one container
just demo-archive      # capture, with an archive configured
just demo-maintain     # also pushes to it, and evicts what it has pushed

cp .env.example .env   # or: set LITELINK_DEMO_ARCHIVE=s3://your-bucket/prefix
```

Credentials are not in that file unless you put them there — the library reads them from the
environment through the ordinary AWS chain, so a profile, instance metadata or SSO all work
untouched, and a log directory never carries a key with it.

To survive losing the machine, ship the SQLite WAL alongside:

```bash
just demo-replicate    # generates litestream.yml from the log, runs it
```

`litelink.restore(root, name, archive=...)` then rebuilds the log on another box, reserving an
offset window so nothing the dead machine served is reissued. Verified against a local
S3-compatible store and against AWS. See [`examples/`](examples/).

### Run the sidecar as its own process

**litelink emits the config; your supervisor runs the binary.** `replication_config()` writes
a `litestream.yml` describing which databases carry the log's state, and systemd, Kubernetes
or anything else runs `litestream replicate -config …` beside the writer. litelink never
starts it and never supervises it, and that is a design commitment rather than an omission:

- **It keeps the network out of the write path.** A sidecar reads the WAL from outside the
  process. If litelink owned the replicator, a stalled upload could push back on `append`,
  and "durable when it returns, with no network in the write path" is the property the whole
  design rests on.
- **The lifecycles are the wrong way round otherwise.** A writer that owns its replicator
  kills it at exactly the moment you need the last frames shipped. A separate process keeps
  draining what is already on disk.
- **§1 allows one writer per log.** Two `Log` handles each spawning a replicator would run
  two litestream instances against one database, which litestream forbids.

litelink *does* run litestream in one place — `restore`, which shells out to it once and waits.
That is a batch call, not a supervised daemon, and it is why the binary ships in the wheel
rather than being left to PATH: the alternative is discovering it is missing during a
failover.

Check the machine before you need it:

```bash
python -m litelink                             # extensions, litestream
python -m litelink s3://bucket/prefix trades   # ...and the archive is reachable
```

Run that as the user and from the process manager that will own the log. A systemd user unit
does not inherit a login shell's PATH, so `which litestream` succeeding in your terminal says
nothing about the unit that will actually perform the restore.

## Reading it from another machine

The demos above are the *writer's* read. Everywhere else reads the archive, which is an
ordinary Iceberg table publishing `version-hint.text` at every commit — so an engine pointed
at the prefix resolves the current metadata itself, with no catalog service, no `archive.db`,
no local root and no litelink install:

```sql
SELECT count(*), max(litelink_offset)
FROM iceberg_scan('s3://bucket/prefix/trades',
                  version_name_format = '%s%s.metadata.json');
```

`litelink_offset` is monotonic and never reused, so a reader keeps the highest one it has seen
and asks for what came after — which is how you follow an archive that `sync` is publishing
into. The extensions, the credential shapes, why `version_name_format` is not optional, and
the polling pattern in full are in [`docs/API.md`](docs/API.md).

That read is only as fresh as the last `sync`. When you have a WAL sidecar running
(`wal_replication`), `litelink.follow` does better: it restores the writer's buffer alongside the
archive and merges them, so the reader sees down to the replication lag instead.

```python
with litelink.follow("trades", archive="s3://bucket/prefix", s3=opts) as reader:
    reader.coverage()   # Coverage(archive=(1, 1928), buffered=(1929, 2100), gap=None, ...)
    reader.scan(where="side = 0", columns=["event_ts", "price"])
```

It never writes anything the primary shares and cannot append — a `LogHandle` has no write
surface at all, rather than one that raises. It is a **snapshot, not a subscription**:
refreshing means assembling another one, and the root it builds is a temporary directory
removed on close. `coverage()` is how it stays honest about what it can and cannot serve.
See [§3b](docs/SPEC.md) for why a gap in that report is not necessarily loss.

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
- **The seal cut is chosen by the appender**, in the transaction that crosses
  `target_seal_size`, and queued. A sealer that falls behind therefore writes several
  correctly-sized files rather than one oversized one.
- **Sizing is two targets, not one.** A seal wants to be small, because the buffer is what a
  hot read scans; a file wants to be large, because per-file overhead dominates scans and
  uploads. Compaction bridges them, on local disk, at 8× the seal size by default.

Read performance is the cost of reading Parquet, plus ~4 ms of fixed overhead. The numbers
behind all of this are [`docs/SPEC.md`](docs/SPEC.md) §7 and §12; how the pieces run is
[`docs/RUNTIME.md`](docs/RUNTIME.md); `just bench` is the same measurement on your hardware.

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
    metadata/                        metadata/
        *.metadata.json                  *.metadata.json
        *.avro                           *.avro
                                         version-hint.text
```

Data files sit under the table's own location, so the path an engine reads
(`s3://bucket/prefix/trades`) is the directory that holds both halves of the table.

> **Upgrading from 0.1.** That release put `catalog.db` and `archive.db` at the root, shared
> by every stream, and Iceberg metadata under `<root>/litelink/<name>/metadata` — outside the
> data it described. `open` detects the old tree and names the fix:
>
> ```bash
> python -m litelink.migrate --root ./data --name trades              # dry run
> python -m litelink.migrate --root ./data --name trades --apply
> ```
>
> Data files are not touched or rewritten; only pointers move. Pass `--archive s3://...` to
> move the archive's metadata too, then restart the sidecar so it replicates to the new
> `<prefix>/<name>/_wal` before dropping the old one with `--drop-legacy-wal`.
>
> A root holding several streams migrates one at a time. `catalog.db`, `archive.db`,
> `litestream.yml` and `<prefix>/_wal` are shared until the last stream has moved — leave the
> old sidecar running until then, since it is still replicating the streams that have not.
> `--drop-legacy-wal` is run once per stream and refuses until every stream in the root has
> migrated and a fresh replica has landed — that old replica holds the only off-box copy of
> unsealed rows, which are in no Parquet file and no archive manifest.

## What it is not

An OLTP or key-value store. A point lookup is ~1,600x slower than an indexed row store, and no
configuration closes that gap. It is a **local, in-process, real-time analytics store**: data
is durable at commit and queryable immediately, so freshness is sub-second *with* durability —
but "real-time" means fresh, not point-lookup fast.

Nor is it an unbounded local archive, though the reason is narrower than an earlier version
of this paragraph claimed. **A seal commits, and a commit's cost tracks what the table's
metadata holds — mostly retained snapshots, and only secondarily files.** Measured at one file
per seal:

```
snapshots retained     40 files /  40 snapshots     61.6 ms
                      240 files / 240 snapshots    247.6 ms
snapshots expired      40 files /   1 snapshot      43.2 ms
                      240 files /   1 snapshot      86.3 ms
```

**`maintain()` already arrests the larger factor**, by expiring snapshots past
`snapshot_retention`. What is left grows with file count — six times the files for twice the
cost — and only eviction removes a file, since compaction stops revisiting one once it reaches
the target size. So a log that runs `maintain()` and never evicts grows slowly; one that runs
neither grows steeply, and the seal is on the write path, so it lands on appends.

Set a retention to bound it. An archive is not what bounds it — a log with an archive and no
retention evicts nothing either — but it is what makes eviction safe rather than lossy, since
I4 stops eviction from passing what the archive holds. Without one, a retention deletes the
only copy, which is a contract a local-only log can ask for deliberately.
[`docs/SPEC.md`](docs/SPEC.md) §13.7.

## Not implemented yet

**Schema evolution** ([`docs/SPEC.md`](docs/SPEC.md) §9) is half built. `add_column` works —
widening the archive, then the local table, then the buffer, so a reader never sees a column
the tables do not have — and `append` validates against the declared schema on every row.
`rename_column` and `drop_column` exist and raise `NotImplementedError`.

**Blob fields** (§15) are specified and unbuilt: `binary` columns are refused outright,
because §15 has large payloads bypass the buffer rather than travel through it.

Still open: payload encoding, local-disk backpressure, and bulk ingest — all three in
[`docs/SPEC.md`](docs/SPEC.md) §13.

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

`just bootstrap` provisions the `iceberg`, `avro` and `httpfs` DuckDB extensions and fetches
litestream. A *checkout* downloads both, unlike an installed wheel, which carries them — so a
contributor has to provision what a user does not. It fetches litestream because eleven tests
skip without it, and a contributor's green run should check what CI's does. Tooling is uv + ruff +
[ty](https://github.com/astral-sh/ty) + pytest. Commits follow
[Conventional Commits](https://www.conventionalcommits.org), enforced by a `commit-msg` hook;
[`CONTRIBUTING.md`](CONTRIBUTING.md) has the types, scopes and style gates.

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
