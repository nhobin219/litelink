# examples

Three scripts against a real log. None of them need an archive, a service, or a network.

```
just demo-capture      # append continuously, maintain on a daemon thread
just demo-tail         # in another terminal: watch it accumulate
```

```
just demo-clean        # delete the captured data when you are done
```

Benchmarks live in [`benchmarks/`](../benchmarks/).

The demo keeps its data on purpose — `tail.py` reads it after the writer stops, and it is
there to poke at — so nothing removes it automatically, and `local_retention` is left unset
so the window grows without bound. Roughly 25 MB per 30 seconds at the default rate. A real
deployment sets a retention and lets `maintain()` hold the size; the benchmarks, which have
nothing to inspect afterwards, run in a temp directory and clean up on exit.

The stream is a websocket trade feed, parsed into columns rather than stored as raw frames —
which is the point of declaring a schema, since every field then prunes from Iceberg
statistics.

`capture.py` is the operational shape of a litelink process: a main thread appending —
durable when `extend()` returns, with no buffer to flush — and a daemon thread calling
`maintain()`. Both share one `Log`, because §1's single-writer rule is about processes.

`tail.py` opens the same log `readonly` while the writer runs, and prints where the rows
are. The column worth watching is the split: rows move from the buffer into the Iceberg
table at each seal, and the total never double-counts across that boundary because both
legs derive from one committed extent (§7, I3). It counts in DuckDB rather than
materialising rows, which is what §7 means about a query over `litelink_offset` never
touching the columns it did not ask for.

Neither script needs an archive, a service, or a network.
