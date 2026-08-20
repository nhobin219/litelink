# examples

Three scripts against a real log. None need an archive, a service, or a network.

```
just demo-capture      # terminal 1: append, and nothing else
just demo-maintain     # terminal 2: seal, compact, evict, expire
just demo-tail         # terminal 3: watch where the rows are
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

```
just demo-clean        # delete the captured data when you are done
```

Benchmarks live in [`benchmarks/`](../benchmarks/).

Only the maintainer commits to the Iceberg table, which is what keeps the log
free of pyiceberg's `Failed to delete metadata file …` warnings. Two committing processes
race on `write.metadata.delete-after-commit` and the loser complains about a file the
winner already removed. Data files are never affected either way — those go through
`pending_delete`, transactionally.

The demo keeps its data on purpose — `tail.py` reads it after the writer stops, and it is
there to poke at — so nothing removes it automatically, and `local_retention` is left unset
so the window grows without bound. Roughly 25 MB per 30 seconds at the default rate. A real
deployment sets a retention and lets `maintain()` hold the size; the benchmarks, which have
nothing to inspect afterwards, run in a temp directory and clean up on exit.

The stream is an ADS-B position feed over a websocket, parsed into columns rather than
stored as raw frames — which is the point of declaring a schema, since every field then
prunes from Iceberg statistics.

`capture.py` appends and nothing else. Every append is durable when `extend()` returns,
with no buffer to flush, and the only other thing it does is record where the next file
should be cut — see [`docs/RUNTIME.md`](../docs/RUNTIME.md).

`maintainer.py` does everything else, in one process.

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

`tail.py` opens the same log `readonly` while the writer runs, and prints where the rows
are. The column worth watching is the split: rows move from the buffer into the Iceberg
table at each seal, and the total never double-counts across that boundary because both
legs derive from one committed extent (§7, I3). It counts in DuckDB rather than
materialising rows, which is what §7 means about a query over `litelink_offset` never
touching the columns it did not ask for.

None of them needs an archive, a service, or a network.
