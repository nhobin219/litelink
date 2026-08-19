# examples

Three scripts against a real log. None of them need an archive, a service, or a network.

```
just demo-capture      # append continuously, maintain on a daemon thread
just demo-tail         # in another terminal: watch it accumulate
just bench --quick     # write and read throughput here, not on the spec's box
```

`capture.py` is the operational shape of a litelink process: a main thread appending —
durable when `extend()` returns, with no buffer to flush — and a daemon thread calling
`maintain()`. Both share one `Log`, because §1's single-writer rule is about processes.

`tail.py` opens the same log `readonly` while the writer runs, and prints where the rows
are. The column worth watching is the split: rows move from the buffer into the Iceberg
table at each seal, and the total never double-counts across that boundary because both
legs derive from one committed extent (§7, I3). It counts in DuckDB rather than
materialising rows, which is what §7 means about a query over `offset` never touching the
payload column.

`benchmark.py` re-measures §3 and §7 locally. Both sections say to: their numbers come from
a 2 vCPU box with ~1 ms fsyncs.

A run on this machine, 400-byte rows:

```
write                          read (20k sealed + 5k buffered)
  batch      rows/s              fixed overhead        2.2 ms
      1       1,271              all columns          17.7 ms
     10       7,229              two columns           8.9 ms
     50      31,567
    200      70,747
```

Write throughput is a statement about fsync, not about work: the same rows cost 55x less
per row at 200 per transaction than at one, because a transaction is one fsync (§3). The
projection gap is §7's one read lever that does not require sealing more often.
