# benchmarks

Internal. These answer "did that change cost us anything", not "how do I use this" —
`examples/` is for the latter.

```
just bench             # write and read throughput here
just bench --quick     # a smaller run
just bench-floor       # what litelink costs on top of raw SQLite
```

SPEC §3 and §7 carry numbers from a 2 vCPU box with ~1 ms fsyncs, and both say to
re-measure on target hardware. `throughput.py` is that measurement.

`vs_sqlite.py` exists because **write throughput is SQLite's write throughput**. The write
path is an `INSERT` at `synchronous=FULL`, so the fsync dominates and raw SQLite is the
floor; the only interesting number is the distance from it. Both sides use the same
columns, the same WAL and synchronous settings, and the same rows per transaction, so what
is left is only what litelink adds.

A run on this machine, 400-byte rows:

```
  batch      raw sqlite        litelink   overhead   us/row
      1         1,552/s         1,498/s       3.7%     23.6
     10         8,403/s         8,979/s      -6.4%     -7.6
     50        37,111/s        34,302/s       8.2%      2.2
    200        92,742/s        76,520/s      21.2%      2.3
   1000       184,831/s       143,192/s      29.1%      1.6
```

Read the last column, not the percentage. Overhead is roughly flat per row while the
fsync's share of each row shrinks as the batch grows, so the percentage climbs while the
real cost does not. Two microseconds against a fsync near a millisecond is not a design
problem — and §7 wants the buffer under ~20k rows anyway, so real capture commits in tens.

Two regressions this has already caught:

- a `min/max` query on every append, to decide whether the buffer was empty — 13-62%
  against the floor for a fact an O(1) counter already had
- constructing the `Log` inside the timed region, which charges Iceberg catalog and table
  creation to write throughput and reports ~100% overhead that is not real
