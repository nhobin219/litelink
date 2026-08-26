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
log.seal_due()
```

`seal_due` is an indexed read of one row when there is nothing to do, so calling
it per message costs almost nothing; when a group is queued it writes that one
file and returns. Nothing else seals, so leaving it out means rows accumulate in
SQLite for ever — durable and readable the whole time, but never reaching
Parquet.

**It blocks the event loop, and it is the seal that does it**, not the append.
Measured: an append runs at a 405 us median, a `seal_due` that actually writes a
file at 43 ms. At this feed's rate that is invisible. At real rates it is the
first thing to fix, and the fix is `adsb/` below.

## `adsb/` — the shape a deployment wants

A synthetic ADS-B position feed, driven as hard as you like, with one process
per storage role. None of it needs an archive, a service, or a network.

```
just demo-capture      # terminal 1: append, and nothing else
just demo-maintain     # terminal 2: one process per storage role
just demo-tail         # terminal 3: watch where the rows are
```

`demo-maintain` starts four processes — `seal`, `compact`, `reclaim`, `sync` —
and one command stops them all. They are separate processes rather than one,
because a seal is CPU-bound pure Python and so is compaction, only more of it:
run together, sealing waits on compaction through the interpreter and the buffer
grows for as long as it waits. That is the same argument that makes the writer
its own process, one level down. Each prints only when it does something, so
silence is the healthy state.

`maintainer.py --role all` is the single-process shape, and is right when the
costs do not justify four. It is also quieter: four processes committing to one
Iceberg table race on pyiceberg's post-commit metadata cleanup and log a
`Failed to delete metadata file` now and then. Measured to be noise — 817,760
rows read back contiguous across a run that logged it — but a single process
produces none.

The feed is synthetic on purpose. A demo you can turn up to a hundred thousand
rows a second is the one that shows what the tiers are for; a real feed arrives
at whatever rate it arrives at.

## Adding the archive tier

Everything above is local. To push sealed files to object storage and read across both
tiers, add a bucket:

```
just rustfs            # a local S3-compatible store, in one container
just demo-archive      # terminal 1: capture, with an archive configured
just demo-maintain     # terminal 2: also pushes, and evicts what it has pushed
just demo-tail         # terminal 3: `in table` falls as `archived` rises
```

**Against a real AWS bucket instead**, nothing changes but the environment:

```
cp .env.example .env      # then set LITELINK_DEMO_ARCHIVE=s3://your-bucket/prefix
just demo-archive
just demo-maintain
```

`just` loads `.env` automatically. Credentials are NOT in it unless you put them there —
the library reads them from the environment at the point of use through the ordinary AWS
chain, so a profile, instance metadata or SSO all work untouched. That is deliberate:
credentials never enter `LogConfig`, because a log directory gets copied, backed up and
attached elsewhere, and a key inside it travels with all of that.

The reader needs the same environment, since `include_archive=True` reaches object
storage. Without it, `scan()` still works — it is local disk only, which is what makes a
hot read a hot read.

## Continuous RPO

Add `--replicate` to `demo-archive` and the maintainer runs litestream alongside itself,
shipping the SQLite WAL to `_wal` beside the archived data. Needs the binary on PATH
([install](https://litestream.io/install)) and an archive to ship to.

That supervision lives in `adsb/maintainer.py`, not in the library: replication is a separate
process reading the WAL, which is exactly why it keeps the network out of the write path,
and litestream is explicit that two instances must never replicate one database. To run it
independently instead, generate the same config and use it directly:

```
uv run python examples/adsb/replicate.py --root litelink-data
litestream replicate -config litelink-data/litestream.yml
```

Two roles, because there are two kinds of work: **the hot path, and everything else.**
The writer appends; the maintainer does the rest. Sealing is maintenance, not a third
role — it is the first thing done with what the writer leaves behind. (A reader is not a
role: any number may open the log `read_only`, holding and mutating nothing.)

`demo-capture` seals nothing at all, and that is the point of running it alone first:
`demo-tail` shows every row in the buffer and none in the table. They are durable and
readable the whole time — `scan()` unions the buffer with the table — so nothing is lost
by starting the maintainer late. Start it and the rows move into Parquet at exactly the
cuts recorded while it was not running.

Nothing coordinates that but the `lease` table. The writer holds no lease and never
tries; the maintainer takes both when it starts, and if it dies they lapse and the next
one takes over.

`adsb/maintainer.py` is one loop calling two plain methods at two cadences — `seal_due()`
often, `maintain()` rarely. The library owns neither the thread nor the interval, so
there is no `seal_mode` to set and nothing starts behind your back.

```
just demo-clean        # delete the captured data when you are done
```

Benchmarks live in [`benchmarks/`](../benchmarks/).

Only the maintainer commits to the Iceberg table, which is what keeps the log
free of pyiceberg's `Failed to delete metadata file …` warnings. Two committing processes
race on `write.metadata.delete-after-commit` and the loser complains about a file the
winner already removed. Data files are never affected either way — those go through
`pending_delete`, transactionally.

The demo keeps its data on purpose — `adsb/tail.py` reads it after the writer stops, and it is
there to poke at — so nothing removes it automatically, and `local_retention` is left unset
so the window grows without bound. Roughly 25 MB per 30 seconds at the default rate. A real
deployment sets a retention and lets `maintain()` hold the size; the benchmarks, which have
nothing to inspect afterwards, run in a temp directory and clean up on exit.

The stream is an ADS-B position feed over a websocket, parsed into columns rather than
stored as raw frames — which is the point of declaring a schema, since every field then
prunes from Iceberg statistics.

`adsb/capture.py` appends and nothing else. Every append is durable when `extend()` returns,
with no buffer to flush, and the only other thing it does is record where the next file
should be cut — see [`docs/RUNTIME.md`](../docs/RUNTIME.md).

`adsb/maintainer.py` does everything else, in one process.

**Why a process and not the writer's thread.** A seal is CPU-bound pure Python — most of
its commit is pyiceberg copying table metadata — so a sealing thread starves the
appending one through the GIL even while holding no lock: appends measured 45.2 ms behind
an in-process seal. A process does not share the GIL. This is what the leases are for.

**Why sealing is not its own process.** They are the same kind of work —
off the hot path, writing to the same Iceberg table, neither latency-critical the way an
append is. Sharing a GIL between them costs nothing that matters, and separating them
costs something real: `_table_lock` serialises a seal's commit against a maintenance pass
*within* a process, and nothing does across processes. Run as two, they raced on
Iceberg's delete-after-commit metadata cleanup and each logged warnings about files the
other had already removed.

They keep separate leases, so splitting them later needs no code change — point a second
process at the same log and the `maintain` role moves. Worth doing only if compaction
starts delaying seals enough to matter, and a delayed seal costs latency rather than file
size: the cut was recorded when the rows arrived.

Running both against one log is safe because every hand-off is a row in SQLite rather
than an object in Python, and WAL serialises the processes. Reading is safe for the same
reason — but note that DuckDB must never open the buffer database itself, which
[`docs/RUNTIME.md`](../docs/RUNTIME.md) explains at some cost.

`adsb/tail.py` opens the same log `readonly` while the writer runs, and prints where the rows
are. The column worth watching is the split: rows move from the buffer into the Iceberg
table at each seal, and the total never double-counts across that boundary because both
legs derive from one committed extent (§7, I3). It counts in DuckDB rather than
materialising rows, which is what §7 means about a query over `litelink_offset` never
touching the columns it did not ask for.

None of them needs an archive, a service, or a network.
