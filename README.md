# litelink

Durable append-only capture into Iceberg tables. Local-first, in-process, no service.

**Status: specification only.** Nothing is implemented yet. See [`docs/SPEC.md`](docs/SPEC.md).

---

## What it is

A library for the thing every capture pipeline hand-rolls badly: getting a stream of
observations onto disk durably, into well-sized Parquet, and eventually into object storage
— without a daemon, a broker, or a catalog service.

```
SQLite buffer          durable on commit. unsealed rows only.
      │  seal at min(target_size, max_age)
      ▼
local Iceberg table    a rolling window. reads land here.
      │  sync: upload data files, register into the archive
      ▼
remote Iceberg table   full history, on S3.
```

The catalog is a SQLite file, not a service. **No read on the hot path touches the network.**

## Why it exists

The pattern it replaces, measured on a production capture system:

- 125,884 objects / 4.36 GB, **62.5% of them under 16 KiB**
- Parquet files at **2 rows each** on low-rate streams; one stream's files were **58.6%
  metadata**
- a compaction routine written months earlier that nothing ever scheduled
- an in-memory write buffer, so a `SIGKILL` lost everything not yet flushed — in a system
  whose stated first principle was that capture is irreversible

None of that is incompetence. It is what happens by default when durable capture, file
sizing, and tiering are each solved locally and none of them is anyone's job.

## What it is not

An OLTP or key-value store. A point lookup is ~1,600x slower than an indexed row store, and
no configuration closes that gap. It is a **local, in-process, real-time analytics store**:
data is durable at commit and queryable immediately, so freshness is sub-second *with*
durability — but "real-time" means fresh, not point-lookup fast.

## Design in one page

- **Iceberg is used, not reimplemented.** Manifests, per-file column statistics, schema with
  field IDs, and atomic snapshot commits all come from it.
- **The library owns exactly one column**, `offset` — monotonic, never reused. It is the
  boundary mechanism between tiers. Everything else is the caller's schema.
- **Sync is a watermark, not CDC.** There are no updates or deletes to replicate.
- **Parts are sealed once and never rewritten.** Rewriting a growing partition costs ~144x
  write amplification and buys nothing, because the local WAL already made the row durable.
- **Read boundaries are derived from committed table state**, never from a stored flag — so
  no seal window can double-count or drop.
- **Compaction is required and local**, because a time-based seal trigger guarantees
  undersized files. Local means no egress to read files back.

Read performance is the cost of reading Parquet, plus ~4 ms of fixed overhead.

## Extensions

**Blob fields** ([`docs/SPEC.md`](docs/SPEC.md) §15) — payloads too large for the buffer
(sensor frames, point clouds, raw response bodies). Bytes stage beside SQLite while hot and
are inlined into Iceberg at seal, so the archive stays ordinary Iceberg with a `binary`
column: no pointers in the published schema, and blob lifetime inherits snapshot expiry and
compaction rather than becoming a hand-maintained refcount.

## Open questions

Payload encoding, and local-disk backpressure. Both in [`docs/SPEC.md`](docs/SPEC.md) §13.

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
