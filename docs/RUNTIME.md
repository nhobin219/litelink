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
That is the whole reason the claims exist.

**Why it is one process and not two.** Sealing and compaction are the same kind of work:
off the hot path, committing to the same Iceberg table, neither latency-critical the way
an append is. Sharing a GIL between them costs nothing that matters, while splitting them
costs something real — `_table_lock` serialises a seal's commit against a maintenance
pass *within* a process, and nothing does across processes, so two maintainer processes
race on Iceberg's delete-after-commit metadata cleanup and each warns about files the
other already removed.

The one role does hold **two kinds of claim**, over the seal's queued range and over the
range a pass is working on, because they guard different recovery records: `sealing`
belongs to whoever claimed the first and `compacting` to whoever claimed the second, and a
process replaying one must not replay the other. That
also means splitting the role across two processes later needs no code change, if a long
compaction ever delays sealing enough to matter. It costs latency, not file size — the
cut was recorded when the rows arrived.

**Both are plain methods, and the caller owns the loop.** `seal_due()` drains the queue;
`maintain()` compacts, evicts and expires — and calls
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

Whichever process calls them, **the `claim` table decides** who does the work — by offset
RANGE, not by role (§4a). Two passes on disjoint ranges run at once; one that finds its
range claimed skips it and finds the work still there next pass.

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
          UPDATE extent  ..................... once per batch
     ◄─ returns offsets; rows are ALREADY DURABLE (synchronous=FULL)
                              │
                              │ nothing but rows in SQLite.
                              │ no queue object, no event, no signal
                              ▼
 ┌──────────────────── buffer.db  (SQLite, WAL) ────────────────────────┐
 │ buffer │ extent │ extent_intent │ sealing │ compacting │ claim │
 │ pending_delete                                                       │
 │                                                                      │
 │  the coordinator (I16). every hand-off below is a row in here, so    │
 │  it works between THREADS and between PROCESSES on identical terms   │
 └──────────────────────────────────────────────────────────────────────┘
        ▲ claim(seal range)                       ▲ claim(pass range)
        │                                        │
 ═══════╧═══════════════ MAINTAINER PROCESS ══════╧══════════════════════
  ─────── seal claim ───────────           ─────── pass claim ────────────
  poll extent (one indexed            maintain() in a loop
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
    NAME the extent's row          │
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

There is no second trigger. A `max_age` branch was specified and removed: it emitted a
small file every interval on a quiet stream, and coupled the file size to the RPO so that
improving one wrecked the other. What bounds RPO now is WAL replication, which is a
property of a sidecar rather than of the layout.

---

## What crosses a process boundary

Nothing in Python. Every arrow in the diagram is a SQLite row:

| hand-off | the row |
|---|---|
| "this range should become a file" | `extent`, `end_offset` set, unnamed |
| "this range IS a file, holding this much" | `extent` with `rel_path` set |
| "I am writing that file, at this path" | `sealing` |
| "I am rewriting these files" | `compacting` |
| "this file may be deleted after its grace" | `pending_delete` |
| "I own these offsets until this time" | `claim` |
| "this copy is intended, not yet made" | `extent_intent` |

The one in-process shortcut is a `threading.Event` used to stop the sealer at `close()`.
Nothing about correctness depends on it.

**Concurrency, by axis.** Any number of *processes* may read at once, each with its own
SQLite and DuckDB connections, sharing only files — which is what WAL is for, and what
the corruption fix restored by keeping DuckDB out of the buffer database entirely (327
concurrent scans from a second process, clean). Within one process, each `Log` has its
own `Reader`, so two handles read in parallel; only concurrent `scan()` calls on the
*same* handle serialise, on that reader's lock.

Making those parallel too — a thread-local DuckDB connection per `Log` — was measured and
rejected. On a 2-core box DuckDB already uses both cores for a single query, so 1, 2 and
4 concurrent readers gave 35, 28 and 7 scans/s: more readers, less throughput. It is the
right lever on many-core hardware and the wrong one here. The SQLite side does not have
the option at all, since one writer at a time is a property of the database, not of our
locking — a mutex wait simply becomes a `SQLITE_BUSY` wait of the same length.

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

## File sizing, and where undersized files are allowed to be

`target_size` is the size a file should be, and the seal cut is exact — the appending
transaction closes a group at the row that crosses it. Nothing else produces a file, so in
normal operation **every file in the system is the size it was asked to be**, and the
things that exist to repair sizing have nothing to repair.

**It is measured in uncompressed bytes, in memory — not in the size of the file on disk.**
That is the single most important thing to know before setting it. A file holding 8 MiB of
rows is 8 MiB on disk if they are incompressible and under 1 MiB if they repeat, so on-disk
size is an output here, never the target. Set it larger than an on-disk file target would
be, and expect smaller files than the number suggests.

The reason is that the uncompressed size is the one that bounds anything real. It is what a
reader pays to hold a file whatever the file cost to store, so bounding it per file is what
lets a scan bound its total — N files open at once cost N times this, which is the number
to divide a memory budget by when choosing read parallelism. It is also the only size
knowable at the moment the seal has to decide, since what compression will achieve is not
known until after the write. Sizing by the file instead would be sizing by the compression
ratio: rows per file would swing with the data, and memory per file would be unbounded.

**Everything downstream is stated in the same currency.** A file's uncompressed size is
recorded by the seal that measured it — the appender's own byte count for exactly those
rows — carried in the buffer beside the file, added up across a merge, and dropped when the
file is finally unlinked. Compaction and sync both read it, so neither ever compares a
compressed size to a memory bound. That mistake is not hypothetical: measuring on disk, the
system merged eight already-full files into one holding eight times the target and, because
sync refuses anything compaction may still rewrite, archived nothing at all while doing it.
A file whose size was never recorded counts as full, so an unmeasured file is never
rewritten on a guess.

That holds only because there is no time-based seal. A timer sealing a quiet stream emits
a small file every interval for ever, which is what compaction was built to clean up
after — and it coupled RPO to file size, so shrinking the window to lose less data on a
crash produced worse files. §3a names that trade; WAL replication is what breaks it, and
freshness in the cloud is its job rather than the seal's.

**One undersized file may exist, and only at the frontier.** The buffer's trailing rows
have not reached `target_size` yet; they stay in SQLite, readable, until they do. Anything
already written as a file is full.

**Where compaction still earns its place.** An explicit `seal()` cuts short by definition,
so a caller who wants the table to move now produces a small file. Four adjacent ones make
a run worth merging. It is a no-op the rest of the time — measured at 19.3 ms over 216
files, which is the cost of *asking* (`data_files()` opens every manifest) rather than of
doing.

**Retention has two floors, and the looser one binds.** `local_retention` is a window in
time, `local_rows` a count of recent rows, and which of them actually bounds local disk
depends on a rate the library cannot know — an hour of a quiet stream is a handful of rows,
an hour of a busy one is more disk than the machine has. Both say what must stay readable
without a network round trip, so eviction keeps whichever retains MORE. That is the mirror
of how the seal combines its limits, where they are ceilings and the tighter wins.

A file's age for this purpose is when it was WRITTEN, recorded by the log itself in
`extent`. It is deliberately not the Iceberg snapshot that added it: expiry deletes that
snapshot, and a file dated by one that no longer exists has no age at all — which is how
retention came to silently stop reclaiming anything. The grace period before a file is
actually unlinked is a different clock again, stamped on `pending_delete` when the file
left the table, because that one is about readers still holding it (I6).

"When it left the table" is the COMMIT, not the queueing, and the two are not the same
moment. Files are queued before the commit that supersedes them, since a crash in between
would lose the only record of their paths, so every supersession corrects the stamp
afterwards — a merge, an archive rewrite, an eviction and both expiries. Left at the
queueing, an operation slower than `snapshot_retention` spends the whole grace before it
commits and the files fall due the instant they stop being referenced: measured at a five
second retention, a reader 0.4 s old lost every file its snapshot named and failed
mid-scan.

"When it left the table" is the commit, not the queueing — and the two are not the same
moment. Files are queued BEFORE the commit that supersedes them, since a crash in between
would lose the only record of their paths, so the stamp is corrected at the commit. Left at
the queueing, a rewrite slower than `snapshot_retention` spends the whole grace before it
commits and the originals fall due the instant they stop being referenced: measured at a 5 s
retention, a reader 0.4 s old lost every file its snapshot named and failed mid-scan.

**Sync holds back exactly what compaction might still rewrite**, which it decides by asking
compaction's own rule rather than a size of its own — a file pushed and then merged locally
would leave the archive holding rows that have been rewritten underneath it, so the two
must agree, and the only way to guarantee that is to share the function. Disqualified are
files in a run compaction would merge now, and files in the trailing run, which is under
budget and so still has room for files not yet written.

A small file in the middle is therefore pushed, not held. It can never grow — files are
immutable and its neighbours are too big to merge with — so waiting achieves nothing.
Holding it blocked the archive permanently: everything after it is newer, so the watermark
never advanced, and I4 pinned local disk with it.

So the archive can gain one small file per explicit seal. `rewrite_archive` is the tool
for that, ad-hoc, and the same one that recompacts after a `target_size` change.

**What would change this.** Compaction rewriting everything downstream of an undersized
file would keep the archive perfect — merging `[0.1][8][8]` and splitting at the cap moves
the remainder to the tail, where an undersized file is allowed to be. It is not done
online because the rewrite window is everything unarchived, so the work is largest exactly
when sync is furthest behind, and it charges a full rewrite for a rare deliberate act.

That reasoning depends on `seal()` being exceptional. **If it turns out to be common in
real use, small files will accumulate in the archive faster than anyone runs the offline
tool, and this belongs online after all.** It is a threshold change rather than a redesign:
the mechanism is the same, only the trigger moves.

## The concurrency contract

What is safe, stated rather than inferred. Everything below is a promise; anything not
listed is not one.

**Between processes**

| | |
|---|---|
| one writer process per log | §1. Two processes appending to one log is unsupported and unchecked |
| any number of reader processes | `Log.open(..., read_only=True)`. They claim nothing and mutate nothing |
| maintainers on disjoint offsets | claims exclude by RANGE, so two passes that cannot touch each other's files run at once; one that finds its range claimed skips it |
| open the log *after* forking | a SQLite handle does not survive `fork`, and neither does a DuckDB one |

**Between threads, within a process**

Safe to call on one `Log` from any thread, including different threads on different
calls:

- `append` / `extend`
- `scan` / `sql`
- `seal` / `seal_due` / `maintain`
- `await_seal`, `table_rows`, `table_files`, `table_extent`, `end_offset`

**Not** concurrency-safe — call them when nothing else is using the log:

- `set_config`, `set_archive`, `set_sort_by`
- `close`

The first three mutate a SQLite row and a Python object together, and they are the only
place `Log._lock` still exists. Reconfiguring a log from two threads at once is not a
scenario worth designing for; corrupting one is not a failure worth allowing.

`close` is different, and deliberately unguarded. It closes the connections, and a lock
could not save a caller who is mid-scan on another thread anyway: the seal's read runs on
the second connection precisely so it does NOT wait behind the write lock, so no single
lock covers everything `close` tears down. Guarding it would mean putting that read back
behind the append lock — undoing the split §4 exists for — to defend against a caller
using an object it has asked to be destroyed.

It is safe to leave unguarded because the failure is loud. SQLite raises
`ProgrammingError: Cannot operate on a closed database` and DuckDB
`ConnectionException: Connection already closed`, both immediately. That is the
distinction worth spending effort on: a silent failure earns a lock, a loud one earns a
sentence.

`buffered_rows` is safe but approximate: it reads the tier boundary and the buffer count
without holding anything between them, so a seal landing in the middle shifts one under
the other. It is a number to watch, not one to derive from.

**Threads buy latency, not throughput.** Measured on this design: appending from 1, 2 and
4 threads gave 28.4k, 29.2k and 30.6k rows/s — within noise — while p99 batch latency
went from 3.5–8 ms to 30–36 ms. Appends are fsync-bound and SQLite admits one writer at a
time, so more threads cannot help. Reads are worse: 1, 2 and 4 concurrent readers gave
35, 28 and 7 scans/s, because DuckDB already uses every core for a single query.

So do not reach for threads to go faster. Reach for them to avoid blocking — which is the
case that matters, because an `async` caller has no choice. `fsync` cannot run on an event
loop, so `await log.append(...)` means dispatching to a worker thread, and a pool hands
out a **different thread each call**. That is why the buffer opens its connection with
`check_same_thread=False` and guards it with a lock instead of demanding thread affinity:
affinity would make the library unusable from asyncio, which is where a websocket feed
lives.

**Every write attempt gets its own name.** A seal's path once came from its range alone,
on the reasoning that a retry should overwrite in place and strand nothing. Recovery
never recomputes it — it reads the name back from `sealing` — so determinism bought
nothing, and it cost the thing it appeared to prevent: a writer stalled past its lease
and the owner that took the role over both wrote that one name, and `pq.write_table`
truncates on open, so the file became a blend of two writers with one of them committing
it.

Unique names on their own would trade a torn file for two worse things, and both need
answering.

**One this database cannot name.** So the abandoned attempt is queued in
`pending_delete` *before* its claim is replaced, and a writer whose commit is refused
queues its own file before raising. Every file on disk stays reachable from SQLite, a
stalled writer's output becomes an ordinary tracked orphan, and no reclamation path ever
needs a directory scan. Compaction outputs carry a per-attempt token for the same reason.

**Two copies of the same rows.** The shared name used to make a lapsed writer's commit
fail — pyiceberg refuses a file already referenced — which was an accidental fence, and
unique names remove it: the commit now *succeeds*, and the range is in the table twice.
Silent duplicate rows are worse than the torn file this was meant to fix. So the commit
is fenced explicitly on the lease, immediately before it, and `finish_seal` clears only
the claim it is handed so a lapsed writer cannot wipe its successor's record.

A fence can never be atomic with an Iceberg commit — the compare-and-swap knows nothing
of our lease — so the check leaves a window of milliseconds. **What closes it is not a
tighter fence but Iceberg's own serialisation.** Two racing writers both
compare-and-swap against the same pointer; one moves it and the other raises. That was
already correct, and what defeated it was our retry: reloading and trying again, which
succeeded because per-attempt names no longer collide.

So the commit declines a range the table already covers, re-checked on every attempt
because `_commit` reloads between them. The loser's retry now does nothing, and a writer
arriving after the winner never attempts at all. The lease fence remains as the thing
that stops the work early; this is what makes a failure of that fence harmless rather
than a duplicate.

**Each query gets its own DuckDB cursor**, and that is what makes a returned reader safe
to hold. A reader is lazy — it streams — so the caller drains it after `query` returns.
On one shared connection the next query's `register` and `CREATE OR REPLACE TEMP VIEW`
land underneath a reader still reading from those names: measured, a reader over 200 rows
returned **zero** once another query ran. Not perturbed, destroyed. A cursor is an
independent connection over the same database, with its own registrations and views, and
costs 0.0055 ms.

**Lock order**, for anyone adding one: `Log._lock` → `Reader._lock` →
{`Archive._lock`, `LogTable._lock`, `Buffer._lock`, `Buffer._tail_lock`}. The leaves are never held while
acquiring one another, and nothing below reaches back up, so there is no cycle to
deadlock on. A read takes `Reader._lock` then briefly `LogTable._lock` and
`Buffer._tail_lock`; a seal takes `Buffer._lock` and `LogTable._lock` at different
moments and never together. `Archive._lock` guards the archive URI, its credentials and
the handle they open as one fact, so a re-point cannot race an open in flight and two
threads cannot each pay the round trip; it is held across that open, and never while
calling back into anything above it.

**One extent, four states.** `extent` is the only record of where a range of the stream
lives, and a row keeps its identity through every stage: open while the appender fills it,
closed when the cut is frozen, named when the seal commits the file, and re-pointed at an
S3 URI when `sync` pushes a second copy. `bytes` — what the appender counted those rows as
in memory — is written once, at the cut, and carried by everything downstream: compaction
adds up the runs it merges, `sync` copies the number to the archive's name for the file,
and the archive rewrite sizes its merges from the same column. Nothing re-derives it,
because nothing can: a Parquet footer records what the rows compressed from, not what they
cost to hold, and Iceberg has no per-file field to keep it in — v2's data-file metadata is
a fixed set with nothing user-extensible, and `add_files` cannot attach one.

Sealing therefore names a row rather than deleting one. It was two tables, a queue and a
size map, which is this one split at the moment a file appears — and two tables that could
disagree about the same range.

**The rule the lock actually encodes.** Every statement on the buffer's write connection
takes that lock, reads included. A statement issued while another thread has a
transaction open on the same connection *joins* it: a read sees uncommitted rows that a
rollback then unmakes, and a write commits or rolls back with someone else's work. That
is not a hot-path concern, it is how a lease once evaporated under its holder and how two
sealers came to write the same file. The read-only connection needs no such lock, and
that is the proof the rule is about transactions rather than about threads — nothing ever
opens one on it.

**The seal has two ceilings, and the tighter one binds.** `target_size` bounds the bytes a
file holds, `target_rows` the number of rows, and the cut lands on whichever is reached
first. They are not interchangeable: buffer cost is per ROW, so a stream of narrow rows
reaches a byte target only after far more rows than the read-latency ceiling was sized for,
while every byte-based check reports the buffer is fine. Compaction respects both, or it
would merge exactly the files a row cap just created straight back past it.

Note the direction, which is the opposite of retention's. These are ceilings on one file
and the tighter wins; `local_retention` and `local_rows` are floors on what stays readable
and the looser wins.

**One process per role is the deployable shape.** A seal is CPU-bound pure Python — most
of its commit is pyiceberg copying table metadata — so it starves anything sharing its
interpreter, which is why the writer is its own process (appends measured 45.2 ms behind an
in-process seal). Compaction is the same work and more of it, so the argument repeats:
sealing beside compaction waits on it, and the buffer grows for as long as it waits. A
thread is not enough — it fixes blocking on the network, not contention for the interpreter.

The cost is measured and worth stating: several processes committing to one Iceberg table
race on pyiceberg's post-commit metadata cleanup, and the loser logs `Failed to delete
metadata file` for one the winner already removed. A single-process control over the same
workload logs none. It is noise — 817,760 rows read back contiguous with no gap or
duplicate across a run that logged it — because the commit is protected by the CAS retry
and the metadata this library depends on is deleted through its own expiry queue.

**The passes are callable one at a time**, and worth doing when their costs diverge.
Conversion reads and rewrites whole files; eviction and expiry are metadata commits that
finish in milliseconds; `sync` is the only one that can block on a network. `maintain()`
runs the three local ones and is what most deployments want; `compact()`, `evict()` and
`expire()` exist for the schedules it cannot express.

None of those four takes a claim of its own. Each PASS claims the range it is about to work
on — a merge claims its run, eviction the prefix it removes — so running them separately is
not a way around anything, and two maintainers working different parts of the log do not
wait for each other (§4a).

Measured on the demo against local object storage, one pass: seal 0 ms, compact 147–920 ms,
reclaim 20–400 ms, sync 11–712 ms. A combined number reports an S3 timeout as slow
compaction, which is how an 83 s sync went unnoticed until the buffer had reached 170,540
rows. `seal` reading 0 ms is the healthy case — the loop drains the queue every quarter
second, so anything else means sealing fell behind.

## Sorting, and why the default is not to

`sort_by` is optional and defaults to offset order. That default is not a fallback — it is
the order the buffer returns rows in — so it costs strictly less than any sort key: no sort
runs at seal time, and every file's offset range is contiguous and exact, which is the
tightest file-level statistic the table can carry.

**Set it only for a column highly correlated with the offset.** For a capture stream that
means an arrival timestamp. Files always hold contiguous offset ranges whatever they are
sorted by internally, so a correlated key leaves each file a disjoint slice of that column
and a predicate on it prunes whole files.

An UNCORRELATED key costs twice, and neither cost shows up in a benchmark of the seal:

- **Pruning stops working.** Every file's min/max on a scattered column spans nearly the
  whole domain, so no file can be skipped. Only row-group skipping inside each file
  survives, which is a fraction of the benefit the sort was for.
- **Replay stops being sequential.** Rows inside a file end up in a random permutation of
  offset order, so reading from an offset needs a sort after reading rather than a scan.
  For a log this is the primary access pattern, which makes it the expensive half.

Both are properties of the first seal, not of any later rewrite. `rewrite_archive` and
`compact` re-sort what they rewrite, exactly as a seal does — they neither introduce this
nor repair it.

## Losing the machine

Sealed data is in the archive once `sync` has pushed it. Everything else — rows that have
not sealed yet, and the catalogs that say what the sealed files are — is SQLite on local
disk, and a WAL-shipping sidecar is what gets it off the machine.

**Nothing bounds the loss window without one.** The seal fires on `target_size` alone, so a
stream that goes quiet holds its last partial file's worth of rows indefinitely. The
`max_age` timer used to bound it, at the cost of making one knob set both the file size and
the RPO; removing it made replication the only mechanism rather than one of two.

```
just demo-replicate      # generates litestream.yml from the log, runs the sidecar
```

**Three databases, not one.** `examples/replicate.py` generates the config from
`Log.databases` rather than leaving it to be written by hand, because the set is not
obvious and getting it wrong is silent: `buffer.db` holds rows no Parquet file has yet,
`catalog.db` says which files the local table is made of, and `archive.db` says the same
for the archive — omit it and the objects in S3 survive with nothing able to say what they
are. `archive.db` is created on first use, so a log that has never opened its archive has
none to restore, and a restore procedure has to tolerate that.

**The endpoint goes in the config, the credentials do not.** litestream reads keys from the
environment, so the generated file is safe to commit and copy — the same reason `S3Options`
is not part of `LogConfig`. The endpoint is different: litestream resolves the bucket's
region against real AWS unless the replica names one, so against anything else it fails
with "cannot lookup bucket region" while the credentials it needs sit unused in the
environment.

**The archive says where its own metadata is.** Every archive commit writes
`version-hint.text` beside the metadata JSONs it names, which is what makes a bucket
recoverable without the local root and a re-point reversible — `archive.db`'s catalog row
is otherwise the only pointer to the current metadata, and re-pointing drops it. An engine
with no catalog at all reads the prefix directly:

```sql
SELECT count(*) FROM iceberg_scan('s3://bucket/prefix/litelink/positions',
                                  version_name_format = '%s%s.metadata.json');
```

That parameter is not optional. DuckDB defaults to the Hadoop `v%s%s.metadata.json` while
pyiceberg names its metadata `00003-<uuid>.metadata.json`, so the hint carries that stem
and the format has to stop prepending a `v`.

**A restored buffer holding sealed rows needs no reconciliation.** The read boundary comes
from the table's committed extent (I3), so those rows fall outside the buffer's
contribution automatically.

Measured on this machine: a log at 189,140 appended rows, killed with its directory
discarded, restored from object storage alone — 189,140 rows readable, **none of them ever
sealed**.

**That last clause is the whole caveat, and this used to read as though it were not.**
Restoring the databases onto another machine and opening the log FAILS once anything has
sealed — measured:

```
FileNotFoundError: .../orig/litelink/s/metadata/00009-....metadata.json
```

`catalog.db` records absolute paths to the local Iceberg metadata, and a sidecar ships the
`.db` files and nothing else — not that metadata, not the Parquet. So a restored catalog
points into the dead machine's filesystem. It is not particular to sealed logs either:
`Log.new` writes a metadata JSON before the first append, so a never-sealed log fails the
same way. What the measurement above actually exercised was a restore back onto the SAME
paths.

Failing over to another box therefore rebuilds the local table rather than restoring it —
`Log.restore` — and `catalog.db` stays in the replication set because same-machine
recovery, where those paths still resolve, is exactly where it is the only record of which
Parquet the table is made of.

### Failing over

```python
log = Log.restore("/data", "positions", archive="s3://bucket/prefix")
print(log.recovery())      # what came back, and which offsets were skipped
```

One call. It refuses a root that already holds this log, or whose
`litestream.yml` replicates a different one; writes the config from the layout
alone — which is the chicken-and-egg, since you need the config to name the
databases you are restoring; restores `buffer.db`; rebuilds the local Iceberg
table empty; drops what described the dead machine; adopts the archive through
`version-hint.text`; and reserves an offset gap.

**`archive.db` is not restored, deliberately.** It is replicated and it is
machine-independent — but it is *time*-dependent, and stale is worse than
absent here. `open_archive` reads `version-hint.text` only when the catalog has
no row, so a stale row wins over the bucket's own pointer: measured, one archive
file reported where the bucket held five, and a union reading 261 rows instead
of 1061. The next sync then commits onto that lineage and republishes the hint
over the fork, destroying the pointer the next recovery would need. `restore`
drops any entry it finds before opening, so a hand restore of all three gets the
same protection.

**Offsets resume above everything the primary served, with a gap.**
`sqlite_sequence` comes back from the replica, so it resumes above what the
replica RECEIVED — not above what was ASSIGNED. Rows appended inside the
replication lag were returned to callers by `append` and never shipped, so
resuming at the replica's frontier would hand those integers to different data.
I9 says offsets are never reused; §6 needs files adjacent in offset order rather
than free of gaps. So 2²⁰ offsets are skipped, and `recovery()` reports which.

**What is not recovered:** rows appended inside the replication lag. They are
gone, and no mechanism here returns them — that is the RPO the sidecar's
`sync-interval` sets. Everything below the seal frontier comes back, including
the sealed-but-unsynced band, because a seal keeps its rows until the archive
has them.

**Split-brain is not detected.** If the primary is not actually dead you have
two writers on one archive, and if both replicate, two litestream instances on
one replica path — the thing litestream is explicit about. Nothing here checks
it; §13's archive-identity token is what would.

## Operating it

```
just demo-capture      # append continuously
just demo-tail         # in another terminal: watch it accumulate
```

A maintainer is not optional. Nothing seals unless something calls `seal_due()` or
`maintain()`, so a writer running alone accumulates in SQLite indefinitely — durable and
readable the whole time, but never reaching Parquet. `examples/maintainer.py` is the
smallest thing that qualifies: one loop, two calls, two intervals.
