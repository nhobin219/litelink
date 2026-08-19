# examples

Three scripts against a real log. None of them need an archive, a service, or a network.

```
just demo-capture      # append continuously, maintain on a daemon thread
just demo-tail         # in another terminal: watch it accumulate
```

Benchmarks live in [`benchmarks/`](../benchmarks/).

`capture.py` is the operational shape of a litelink process: a main thread appending —
durable when `extend()` returns, with no buffer to flush — and a daemon thread calling
`maintain()`. Both share one `Log`, because §1's single-writer rule is about processes.

`tail.py` opens the same log `readonly` while the writer runs, and prints where the rows
are. The column worth watching is the split: rows move from the buffer into the Iceberg
table at each seal, and the total never double-counts across that boundary because both
legs derive from one committed extent (§7, I3). It counts in DuckDB rather than
materialising rows, which is what §7 means about a query over `offset` never touching the
payload column.

Neither script needs an archive, a service, or a network.
