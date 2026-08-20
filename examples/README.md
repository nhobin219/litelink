# examples

Four scripts against a real log, one per role. None need an archive, a service, or a
network.

```
just demo-capture      # terminal 1: append continuously
just demo-seal         # terminal 2: turn the buffer into Parquet
just demo-maintain     # terminal 3: compact, evict, expire
just demo-tail         # terminal 4: watch where the rows are
```

`demo-capture` alone is enough to see something: it seals on a background thread until a
dedicated sealer appears. Start `demo-seal` and that thread begins losing the lease
immediately — no restart, no flag, no coordination between them. Stop it and the writer
takes the role back once the lease lapses. Nothing decides this but the `lease` table.

```
just demo-clean        # delete the captured data when you are done
```

Benchmarks live in [`benchmarks/`](../benchmarks/).

Running the sealer and the maintainer together logs occasional pyiceberg warnings —
`Failed to delete metadata file …`. Both processes commit to the same Iceberg table, and
both honour `write.metadata.delete-after-commit`, so they race to remove the same
superseded metadata JSON and the loser warns about a file that is already gone. Noisy,
not harmful; the data files themselves are never deleted this way (that is
`pending_delete`, and it is transactional).

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

`sealer.py` and `maintainer.py` are the other two roles, in their own processes.

**Why a process and not a thread.** A seal is CPU-bound pure Python — most of its commit
is pyiceberg copying table metadata — so a sealing thread starves the appending one
through the GIL even while holding no lock: appends measured 45.2 ms behind an in-process
seal. A process does not share the GIL. This is what the leases are for.

**Why seal and maintenance are separate from each other.** They hold different leases, so
either can crash or be restarted without stopping the other, and a compaction taking
seconds cannot delay a seal. Persistence, compaction, deletion and query planning are
four concerns; coupling them lets the slowest set the latency of the rest.

Running all three against one log is safe because every hand-off is a row in SQLite
rather than an object in Python, and WAL serialises the processes. Reading is safe for
the same reason — but note that DuckDB must never open the buffer database itself, which
[`docs/RUNTIME.md`](../docs/RUNTIME.md) explains at some cost.

`tail.py` opens the same log `readonly` while the writer runs, and prints where the rows
are. The column worth watching is the split: rows move from the buffer into the Iceberg
table at each seal, and the total never double-counts across that boundary because both
legs derive from one committed extent (§7, I3). It counts in DuckDB rather than
materialising rows, which is what §7 means about a query over `litelink_offset` never
touching the columns it did not ask for.

None of them needs an archive, a service, or a network.
