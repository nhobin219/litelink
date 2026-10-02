# examples

Two demos, at opposite ends of the range.

## Start here

```
just demo-websocket
```

The whole library in one process against a **live public feed** — Bitstamp
publishes BTC/USD trades over an unauthenticated websocket, so there is no
producer to start and no credentials to set. `websocket.py` subscribes, appends
each trade, seals when there is enough to seal, and prints a query over what it
captured. Thirty seconds end to end.

The loop is two calls:

```python
log.append(row(trade))
log.seal()
```

`seal` is an indexed read of one row when there is nothing to do, so calling
it per message costs almost nothing; when a group is queued it writes that one
file and returns. Nothing else seals, so leaving it out means rows accumulate in
SQLite for ever — durable and readable the whole time, but never reaching
Parquet.

**It blocks the event loop, and it is the seal that does it**, not the append.
Measured: an append runs at a 405 us median, a `seal` that actually writes a
file at 43 ms. At this feed's rate that is invisible. At real rates it is the
first thing to fix, and the fix is `adsb/` below.

## `adsb/` — the shape a deployment wants

A synthetic ADS-B position feed, driven as hard as you like, with one process
per storage role. None of it needs object storage, a service, or a network: the
log publishes to a local published table under its own directory.

```
just demo-capture      # terminal 1: append, and nothing else
just demo-maintain     # terminal 2: one process per storage role
just demo-tail         # terminal 3: watch where the rows are
```

`demo-maintain` starts five processes — `seal`, `compact`, `publish`, `clean` and
`clean-published` — and one command stops them all. That is the split litelink
recommends for a deployment ([`docs/API.md`](../docs/API.md#process-split)): a
step gets its own process when it is heavy on CPU or the network.

| Role | Runs |
| --- | --- |
| `seal` | `seal()` |
| `compact` | `compact()` |
| `publish` | `publish()` |
| `clean` | `evict()`, `reclaim("buffer")`, `reclaim("staging")`, `sweep("staging")` |
| `clean-published` | `reclaim("published")`, `sweep("published")` |

A seal is CPU-bound pure Python and so is compaction, only more of it: run
together, sealing waits on compaction through the interpreter and the buffer
grows for as long as it waits. `publish` and the published table's cleanup wait
on the network, and must not hold up anything local. Local cleanup is metadata
commits that finish in milliseconds, in an order — eviction queues what
`reclaim` deletes — so it stays one process. Each prints only when it does
something, so silence is the healthy state.

`maintainer.py --role all` is `advance()`, the single-process shape, and is
right when the costs do not justify five. It is also quieter: processes
committing to one Iceberg table race on pyiceberg's post-commit metadata
cleanup and log a `Failed to delete metadata file` now and then. Measured to be
noise — 817,760 rows read back contiguous across a run that logged it — and
data files are never affected, since those go through litelink's own expiry
queue.

The feed is synthetic on purpose. A demo you can turn up to a hundred thousand
rows a second is the one that shows what the tiers are for; a real feed arrives
at whatever rate it arrives at.

## Publishing to object storage

Everything above is local. To publish sealed files to object storage instead, and evict
from local disk what the bucket holds, add a bucket:

```
just rustfs            # a local S3-compatible store, in one container
just demo-published    # terminal 1: capture, publishing to S3
just demo-maintain     # terminal 2: pushes, and evicts what it has pushed
just demo-tail         # terminal 3: `staging rows` falls as `published rows` rises
```

**Against a real AWS bucket instead**, nothing changes but the environment:

```
cp .env.example .env      # then set LITELINK_DEMO_PUBLISHED=s3://your-bucket/prefix
just demo-published
just demo-maintain
```

`just` loads `.env` automatically. Credentials are NOT in it unless you put them there —
the library reads them from the environment at the point of use through the ordinary AWS
chain, so a profile, instance metadata or SSO all work untouched. That is deliberate:
credentials never enter `LogConfig`, because a log directory gets copied, backed up and
attached elsewhere, and a key inside it travels with all of that.

The reader needs the same environment whenever a query reaches back into the published table. A
query bounded inside the staging window reads local disk only and needs no credentials, which
is what makes a hot read a hot read.

## Continuous RPO

Add `--replicate` to `demo-published` and the maintainer runs litestream alongside itself,
shipping the SQLite WAL to `_wal` beside the published data. Needs an `s3://` published table
to ship to and the binary — `just litestream` fetches a checksum-verified pinned build into
`.bin/`, which both the maintainer and `just demo-replicate` prefer over whatever is on PATH,
because the config format is version-dependent.

That supervision lives in `adsb/maintainer.py`, not in the library: replication is a separate
process reading the WAL, which is exactly why it keeps the network out of the write path,
and litestream is explicit that two instances must never replicate one database. To run it
independently instead, generate the same config and use it directly:

```
uv run python examples/adsb/replicate.py --root litelink-data
.bin/litestream replicate -config litelink-data/positions/litestream.yml
```

The writer appends; the maintainer processes do the rest. Sealing is
maintenance, not part of the hot path — it is the first thing done with what the
writer leaves behind. (A reader is not a role: any number may open the log with
`litelink.open(..., read_only=True)`, holding and mutating nothing.)

`demo-capture` seals nothing at all, and that is the point of running it alone first:
`demo-tail` shows every row in the buffer and none in the staging table. They are durable and
readable the whole time — `scan()` unions the buffer with the table — so nothing is lost
by starting the maintainer late. Start it and the rows move into Parquet at exactly the
cuts recorded while it was not running.

Nothing coordinates the processes but the `claim` table. The writer holds no claim and
never tries; each maintainer pass claims the offset range it works on, and if a process
dies its claims lapse and the next one takes over. Every hand-off is a row in SQLite
rather than an object in Python, and WAL serialises the processes. Reading is safe for
the same reason — but DuckDB must never open the buffer database itself, which
[`docs/RUNTIME.md`](../docs/RUNTIME.md) explains.

The library owns neither the thread nor the interval, so there is no `seal_mode` to set
and nothing starts behind your back.

**Why a process and not the writer's thread.** A seal is CPU-bound pure Python — most of
its commit is pyiceberg copying table metadata — so a sealing thread starves the
appending one through the GIL even while holding no lock: appends measured 45.2 ms behind
an in-process seal. A process does not share the GIL.

The stream is a synthetic ADS-B position feed, generated in-process, parsed into columns
rather than stored as raw frames — which is the point of declaring a schema, since every
field then prunes from Iceberg statistics. `adsb/capture.py` appends and nothing else.
Every append is durable when `extend()` returns, with no buffer to flush, and the only
other thing it does is record where the next file should be cut — see
[`docs/RUNTIME.md`](../docs/RUNTIME.md).

`adsb/tail.py` opens the same log with `litelink.open(..., read_only=True)` while the
writer runs, and prints where the rows are. The column worth watching is the split: rows
move from the buffer into the Iceberg table at each seal, and the total never
double-counts across that boundary because both legs derive from one committed extent
(§7, I3). It counts in DuckDB rather than materialising rows, which is what §7 means
about a query over `litelink_offset` never touching the columns it did not ask for.

The demo keeps its data on purpose — `adsb/tail.py` reads it after the writer stops, and
it is there to poke at — so nothing removes it automatically, and `staging_retention` is
left unset so the window grows without bound. Roughly 25 MB per 30 seconds at the default
rate. A real deployment sets a retention and lets `clean` hold the size.

```
just demo-clean        # delete the captured data when you are done
```

That removes both demo roots — `litelink-data` here and `litelink-ws` from the websocket
capture — plus whatever this log pushed to the published table, so one command covers
every demo in this directory.

Benchmarks live in [`benchmarks/`](../benchmarks/).
