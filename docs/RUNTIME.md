# Runtime shape

Who does what, in which process, and what crosses between them.

[`SPEC.md`](SPEC.md) says what the system is. This says how it runs: three roles, what
each one touches, and why every hand-off is a row in SQLite rather than an object in
Python. If you are trying to work out whether some step lands on the append that
triggered it, this is the page.

The short version: **an append does no work beyond its own insert.** It does not measure
the buffer, decide whether to seal, or delete anything. It records where the next file
should be cut and returns.

---

## Two roles

| role | what it does | process |
|---|---|---|
| **writer** | `append` / `extend` — the hot path | yours |
| **maintainer** | seal, compact, evict, expire, unlink | its own |

**Sealing is maintenance**, not a third role. It is the first thing the maintainer does
with what the writer leaves behind: turn the buffer into Parquet. Compaction, eviction
and expiry are what it then does with the files that produces.

A reader is not a role in this sense. Any number of processes may open the log
`read_only`; they hold nothing, mutate nothing, and coordinate with nobody.

**Why the maintainer is a separate process.** A seal is CPU-bound pure Python — most of
its commit is pyiceberg copying table metadata — so doing it on a *thread* inside the
writer starves the appending thread through the GIL even while holding no lock. Appends
measured 45.2 ms behind an in-process seal. A separate process does not share the GIL.
That is the whole reason the leases exist.

**Why it is one process and not two.** Sealing and compaction are the same kind of work:
off the hot path, committing to the same Iceberg table, neither latency-critical the way
an append is. Sharing a GIL between them costs nothing that matters, while splitting them
costs something real — `_table_lock` serialises a seal's commit against a maintenance
pass *within* a process, and nothing does across processes, so two maintainer processes
race on Iceberg's delete-after-commit metadata cleanup and each warns about files the
other already removed.

The one role does hold **two leases**, `seal` and `maintain`, because they guard
different recovery records: `sealing` belongs to whoever holds the first and `compacting`
to whoever holds the second, and a process replaying one must not replay the other. That
also means splitting the role across two processes later needs no code change, if a long
compaction ever delays sealing enough to matter. It costs latency, not file size — the
cut was recorded when the rows arrived.

**Both are plain methods, and the caller owns the loop.** `seal_due()` drains the queue
and closes anything past `max_age`; `maintain()` compacts, evicts and expires — and calls
`seal_due()` first, because sealing is the first thing done with what the writer leaves
behind. They are two methods rather than one only because their costs differ by an order
of magnitude: `seal_due()` is an indexed read of one row when idle, so it can be run
often, while `maintain()` reads table metadata and wants to be run rarely.

The library owns no thread and no interval. It used to: `extend()` quietly started a
sealing thread and a `seal_mode` setting chose between "background", "inline" and "none".
None of that survived the queue — once the cut is recorded by the append, sealing is just
draining, which is what `maintain()` already was. A library that spawns threads on your
behalf is also a library whose tests interfere with themselves, which is how two of the
bugs above were found.

Whichever process calls them, **the `lease` table decides** who does the work: a second
caller is refused and returns rather than duplicating it.

---

## End to end

```
 ═══════════════════ WRITER PROCESS (the hot path) ══════════════════════
   extend(rows)
     └─ with _lock:  ONE transaction
          INSERT INTO buffer ................. per row
          running total += row bytes ......... in the same txn
          crossed target_size?  ──► freeze the cut HERE, at this row,
                                    and open the next group
          UPDATE seal_group  ................. once per batch
     ◄─ returns offsets; rows are ALREADY DURABLE (synchronous=FULL)
                              │
                              │ nothing but rows in SQLite.
                              │ no queue object, no event, no signal
                              ▼
 ┌──────────────────── buffer.db  (SQLite, WAL) ────────────────────────┐
 │  buffer │ seal_group │ sealing │ compacting │ pending_delete │ lease │
 │                                                                      │
 │  the coordinator (I16). every hand-off below is a row in here, so    │
 │  it works between THREADS and between PROCESSES on identical terms   │
 └──────────────────────────────────────────────────────────────────────┘
        ▲ lease('seal')                          ▲ lease('maintain')
        │                                        │
 ═══════╧═══════════════ MAINTAINER PROCESS ══════╧══════════════════════
  ─────── seal lease ───────────           ─────── maintain lease ────────
  poll seal_group (one indexed          maintain() in a loop
  row read; no lock taken if
  there is nothing queued)                _table_lock + lease
                                            ├─ compact()
  §4 step 1   with _lock:                   │    merge undersized runs
    lease.acquire() ─► lose? return         │    (intent → `compacting`)
    take (start, end) FROM THE QUEUE        │
    claim_seal(start, end, path)            ├─ evict()   ← local_retention
       └─ path recorded BEFORE the          │    drop files from the table,
          file exists  (I2)                 │    ENQUEUE their paths.
                                            │    never unlinks here
  §4 step 2   NO _lock  ← all the cost      │
    rows_below(end) on a 2nd connection     └─ expire() → drain()
    sort → write Parquet → fsync                 snapshots past
    commit to Iceberg ────────────────┐          snapshot_retention, then
                                      │          unlink files whose grace
  §4 step 3   with _lock:             │          has passed, in the SAME
    DELETE buffer rows < end          │          txn that clears the queue
    DELETE the seal_group row         │
    lease.release()                   ▼
                    ┌─────────────────────────────┐
                    │  local Iceberg table        │
                    │  (Parquet + SQLite catalog) │
                    └─────────────────────────────┘
```

---

## Where rows and files actually go

Two different things get called "eviction". They happen in different roles:

- **Buffer rows** are deleted by the **maintainer**, at step 3 of a seal, once the
  Iceberg commit has landed. The appending call never does this. Step 3 does take the write lock briefly,
  so a concurrent append waits for it — but it is a delete by primary key, not work
  proportional to the seal.
- **Parquet files** are removed from the table by the **maintainer** under
  `local_retention`, and *unlinked* only later by `drain()`, once `snapshot_retention`
  has passed.

A file's path is written to SQLite **before** the file is created (`sealing`,
`compacting`) and again before it is deleted (`pending_delete`). So no file can exist on
disk that this database cannot name, and reclaiming disk is a keyed read rather than a
directory walk. That matters most where a walk is a paginated, billable LIST.

---

## Why the cut is decided by the appender

`target_size` is the library's one promise about file size, and it is kept on the append
path — in the transaction that crosses the threshold, at the exact row that crosses it.

A running byte counter cannot keep that promise. A counter tells a sealer that a
threshold was crossed; it never says **where**. A sealer that polls one cuts wherever the
buffer has got to by the time it looks, so the file it writes measures how far behind the
sealer was rather than what was asked for.

Freezing the cut when the rows arrive inverts that. A sealer that falls behind finds
several groups queued, and **each one is already the right size**. Poll latency then buys
nothing worse than latency: the same file, written later.

It also means the queue is the whole trigger. There is no in-memory byte counter, no
`Event`, and no threshold comparison on the append path — a single indexed row read tells
any process both *that* there is work and *exactly which offsets it covers*.

**An explicit `seal()` cuts unconditionally**, and that is the difference between a
deterministic file layout and a raced one. It used to cut only when the queue happened to
be empty, so a call made while a sealer still had a group queued left the caller's rows
uncut and sealed an older group instead — sometimes returning None having sealed nothing.
Eight appends and eight seals could then produce seven files, one holding two appends'
worth of rows: the same calls, a different layout, decided by timing.

What the lease still decides is who *writes* the file, not where it is cut. So `seal()`
returns the cut it recorded whether or not this caller wrote it, and `await_seal()` is
the call for anyone who needs the table itself to have moved. Blocking inside `seal()`
instead would put a caller behind another process's lease TTL, which is a worse bargain.

`max_age` (§4's other trigger) is the same mechanism from the other side: the open group
records when its **first row** landed, and a sealer closes it once that is old enough.
Stamping the group's creation instead would seal a one-row file the moment an idle group
finally got a row.

---

## What crosses a process boundary

Nothing in Python. Every arrow in the diagram is a SQLite row:

| hand-off | the row |
|---|---|
| "this range should become a file" | `seal_group` with `end_offset` set |
| "I am writing that file, at this path" | `sealing` |
| "I am rewriting these files" | `compacting` |
| "this file may be deleted after its grace" | `pending_delete` |
| "I hold this role until this time" | `lease` |

The one in-process shortcut is a `threading.Event` used to stop the sealer at `close()`.
Nothing about correctness depends on it.

A lease statement runs under the buffer's lock, not just on its connection. Without it
the statement joins whatever transaction another thread has open and is undone by that
transaction's rollback — a claim that can evaporate is not a claim, and two sealers wrote
the same file before this was fixed.

**Ownership follows the lease, including for recovery.** Opening a log replays
interrupted work, and a second opener must not redo an operation another process is still
performing. `sealing` belongs to whoever holds the `seal` role and `compacting` to
whoever holds `maintain`, so each recovers only its own. A holder that exits cleanly
releases; one that is killed leaves a lease that lapses, after which someone else may
finish what it started. Owners are UUIDs minted per acquisition, so two threads sharing a
`Log` are two owners and the row that refuses another process refuses another thread on
identical terms.

---

## Reading

Reads never touch the network and never block the writer. A scan unions two legs in
DuckDB — the Iceberg table, and the buffer's unsealed tail above the table's committed
extent — so a row appears exactly once even though the tiers overlap by design.

**DuckDB does not open the buffer database.** It used to, via
`ATTACH … (TYPE sqlite)`, and that silently corrupted it: DuckDB's sqlite extension
carries its own statically linked SQLite, so the file ended up managed by two independent
SQLite libraries in one process. Each keeps private per-inode state for POSIX locks and
for WAL's shared-memory index, so the two stopped serialising against each other. An
in-process scan concurrent with appends corrupted the database on the *first* scan —
`database disk image is malformed`, and a torn `-shm` mapping raising `SIGBUS`. The same
workload with the reader in a separate process ran clean, which is the tell: cross-process
is the case WAL is designed for; two libraries inside one process is not.

**What that cache costs.** It mirrors the *unsealed* tail and nothing else, so it is
bounded by whatever bounds the buffer — `target_size` plus however far behind the
maintainer is — and it releases: measured over 24 append/seal/read cycles, the cached
tail returns to zero rows and Arrow's allocation to zero after each seal. Run a writer
with no maintainer and it grows, at roughly 1.1x the payload, for the same reason the
SQLite buffer does. That is one more reason a maintainer is not optional.

The buffer leg is therefore read through the connection that already owns the file and
handed to DuckDB as Arrow. It is converted **incrementally** — rows are immutable once
committed, arrive only above the last one, and leave only as a prefix at a seal, so each
query converts its own delta and slices the rest zero-copy. That is also faster than the
attached version was, because the attached version re-read the entire buffer on every
query.

---

## Operating it

```
just demo-capture      # append continuously
just demo-tail         # in another terminal: watch it accumulate
```

A maintainer is not optional. Nothing seals unless something calls `seal_due()` or
`maintain()`, so a writer running alone accumulates in SQLite indefinitely — durable and
readable the whole time, but never reaching Parquet. `examples/maintainer.py` is the
smallest thing that qualifies: one loop, two calls, two intervals.
