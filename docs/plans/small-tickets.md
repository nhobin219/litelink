# Small tickets: provisioning, replication, self-describing archive, examples

One PR, seven tickets. They are independent except T3 and T4, which share a
mechanism, and T1/T2, which are both "the demo dead-ends on a fresh machine".

Two of the tickets as filed are not buildable as written, and the reasons are
below rather than in a commit message: **T4** asks for offset-based WAL
retention, which litestream does not have, and **T7** is already implemented.

---

## T1 — `LOAD httpfs` fails with DuckDB's error, not ours

**Symptom.** On a machine that ran `just bootstrap` but never
`just duckdb-extensions --remote`:

```
log.scan(include_archive=True)
  _read.py:162 self._connect().execute("LOAD httpfs")
  _duckdb.IOException: IO Error: Extension ".../httpfs.duckdb_extension" not found.
```

**Cause, and it is two separate things.**

1. `httpfs` is in `REMOTE`, not `READ_PATH`, so `just bootstrap` does not
   install it — deliberately, per §7: "Local-first capture never loads this,
   which is why it is opt-in". But every developer who runs `just demo-archive`
   needs it, and nothing in the demo path says so.
2. An explicit `LOAD` of a known-but-uninstalled extension does not autoinstall
   the way function-triggered autoloading does, so DuckDB's error is what the
   caller sees. **Verified** against duckdb 1.5.5 — with
   `autoinstall_known_extensions` reporting `True` and an empty
   `extension_directory`, `LOAD httpfs` reproduces the filed traceback
   verbatim. So this is not a machine that had autoinstall switched off; it is
   how `LOAD` behaves.

**Fix.** Both halves, neither of them an auto-install.

- `Justfile`: `bootstrap` calls `just duckdb-extensions --remote`. One extra
  download at setup, and the trap is gone for anyone working in this repo.
- `_read.py`: wrap both `LOAD` sites — `duckdb_connection()` for `iceberg`, and
  `_prepare_remote` for `httpfs` — so a missing extension raises litelink's
  error naming `scripts/install_duckdb_extensions.py --remote`, with DuckDB's
  as the cause.

**Rejected: INSTALL at the point of use.** It would fix the traceback and break
§7's claim. The spec's position is that provisioning is an obligation
discharged at build or deploy time and that an unprovisioned machine fails
loudly — `--check` exists to make exactly that assertion. Silently downloading
on the first archive read is what `--check` is written to detect.

The distinction to keep: **`iceberg` must never auto-install** (a hot read is
offline), and `httpfs` is only reached on a read that is already going to the
network. That makes auto-install defensible for `httpfs` alone. It is still not
what this does, because one rule is easier to hold than two, and the developer
trap is closed by `bootstrap` instead.

---

## T2 — `just demo-replicate` dead-ends, and generates a deprecated config

**Symptom as filed.**

```
just demo-replicate
litestream not on PATH — see https://litestream.io/install
```

That is the recipe working as written. The complaint is that it is a dead end:
every other piece of demo infrastructure provisions itself (`just rustfs`
starts a container, `just duckdb-extensions` fetches extensions) and this one
hands you a URL.

**Second half, not filed, found while checking the first.** The generated
config is on litestream's deprecated path:

```go
// litestream v0.5.16 cmd/litestream/main.go:728
Replica  *ReplicaConfig   `yaml:"replica"`
Replicas []*ReplicaConfig `yaml:"replicas"` // Deprecated
```

`litestream_config` emits `replicas:` (a list). v0.5.0's note is "Each database
now supports only a single replica." It still parses, so this is not a live
break — but the demo tells you to install litestream, and what you install
today is 0.5.16.

**Fix.**

- New recipe `just litestream`: download the pinned release tarball for this
  platform into `.bin/`, verify against `checksums.txt`, extract. Idempotent,
  same shape as `duckdb-extensions`. `.bin/` into `.gitignore`.
- `demo-replicate` and `maintainer.py`'s `Sidecar` prefer `.bin/litestream`,
  fall back to PATH, and the "not on PATH" message names `just litestream`.
- `_replication.py`: emit `replica:` (singular). One replica per database is
  what it already writes — the list never had more than one element.

**Version is pinned, not "latest".** A demo that fetches whatever is current
today makes a config-format change somebody else's silent breakage. Pinning
puts it in a diff.

---

## T3 — Publish the archive's metadata pointer to S3 at sync time

**Filed as:** "Version-hint.txt and persisting metadata currently only in
SQLite to s3 at sync time."

**What is only in SQLite.** `archive.db`'s `iceberg_tables` row holds
`metadata_location` — the pointer to the archive table's current
`metadata.json`. That row is the *only* thing that names it. The objects in the
bucket carry no pointer to their own current metadata, because pyiceberg's
`SqlCatalog` keeps it in the catalog rather than in a `version-hint.text` file
the way a filesystem catalog would (§7 records this: `iceberg_scan(directory)`
fails with "no version-hint could be found").

**Three things that costs, all of them already visible:**

1. **A detached archive cannot be re-attached.** `_table.py:360-367` says so in
   a comment: re-pointing drops the row, and "a drop that is not followed by a
   create destroys the only record of where the PREVIOUS archive's metadata
   is." Point away from archive A and back, and `open_archive` calls
   `create_table` — a fresh empty table over data nothing can now reach. §13
   carries this as a known gap.
2. **Nothing else can read the archive.** A bucket of Parquet and metadata JSON
   with no `version-hint.text` needs *this* `archive.db` to be readable at all.
   The archive is not self-describing, which is most of the point of writing
   Iceberg rather than Parquet.
3. **`archive.db` is load-bearing for restore** (T4 depends on this).

**Fix, in two parts.**

*Part A — durable, local.* A `meta` row keyed by archive URI recording that
archive's last known `metadata_location`, written after each successful commit
against it. `open_archive` gains a branch: no catalog row, but a remembered
pointer for this prefix, so `register_table(table_id, remembered)` instead of
`create_table`. **That operation already exists** — `_table.py:392` uses it in
the failure path to put a displaced entry back. Re-attach becomes expressible;
§13's gap closes.

*Part B — durable, remote.* At sync time, after the commit, write
`version-hint.text` under the archive table's `metadata/` prefix, containing
the current metadata version. The archive becomes readable by any Iceberg
engine pointed at the directory, and a total loss of the local root is
recoverable from the bucket alone.

**Three things to settle during the build, none of them design questions:**

- The exact filename and payload DuckDB accepts — `version-hint.text` vs
  `.txt`, version integer vs full path. Verify empirically against duckdb
  1.5.5 before writing the uploader; do not take the Hadoop convention on
  trust.
- **Write it after the commit, never before.** A hint naming metadata that the
  CAS retry then superseded points readers at a snapshot the table moved off.
  Same ordering rule as the intent record.
- **A stale hint is worse than none if it can go backwards.** The commit loop
  retries; the hint write must carry the version that actually won, read back
  from the committed table rather than from the attempt.

**Caveat to document, not to solve.** Part A's pointer records where the
metadata was when the log *left*. If something else wrote that archive while
detached, `register_table` adopts the older state. The documented contract is
one writer per log, so this cannot happen under it — but the contract is
asserted, not checked, and that is the §13 archive-identity seam. The pointer
makes re-attach *possible* and trusts the contract; an identity token would
make it *checkable*. Out of scope here.

**Also out of scope: reading two archives at once.** Restoring means pointing
back. While pointed at B, rows only in A stay unreadable, because `_union`
resolves a single archive per query.

---

## T4 — WAL retention

**Filed as:** "Litestream WAL retention/cleanup (only retain offset > latest
archived offset)".

**This is not expressible, and the reason is worth recording.** litestream
v0.5.16's retention is time-based, everywhere:

```go
type SnapshotConfig struct {          // per-db and global
    Interval  *time.Duration `yaml:"interval"`    // default 24h
    Retention *time.Duration `yaml:"retention"`   // default 24h
}
L0Retention *time.Duration `yaml:"l0-retention"`  // default 5m
```

There is no offset-aware knob, and no `snapshot` subcommand — the v0.5.16 CLI
is `replicate|restore|status|list|info|sync|ltx|wal|...` — so litelink cannot
force a snapshot after a sync to make retention *effectively* offset-driven
either.

**Also worth being clear about what retention does not risk.** A restore always
recovers the *latest* replicated state. Retention bounds how far *back in time*
a restore can go; it never endangers the current one. So "retain only offset >
latest archived offset" is really "keep point-in-time depth no deeper than the
un-archived window" — and that window is a duration.

**Fix.** `LogConfig.wal_retention: float | None` (seconds, None = litestream's
default), emitted as the per-database `snapshot:` block:

```yaml
dbs:
  - path: .../buffer.db
    snapshot:
      interval: 1h
      retention: 6h
    replica: { ... }
```

`validate` refuses `wal_retention` without `wal_replication`, matching the
existing `wal_replication`-without-archive refusal. `interval` is derived as
half the retention so that at least one snapshot always sits inside the window
— retention with a longer interval than itself deletes the chain a restore
needs.

**And the number has to come from evidence.** The un-archived window is
append→seal→compact→sync, which no library can know a priori. `tail.py` gains
the lag it already has the inputs for: `end_offset - archived_through`, and the
wall-clock age of the oldest un-archived row. Set the retention from what that
shows, with margin.

**Depends on T3.** Retention that expires `archive.db`'s history is only safe
once the archive can say what it is without `archive.db`. Land T3 first.

---

## T5 — Inline websocket example

**Filed as:** "Quick example end to end snippet streaming from a websocket
(doesn't need separate processes or background threads, just in-line python
calls to capture and ~perhaps~ seal the segments)."

The point is the shape: no maintainer process, no thread, no lease dance. Just

```python
async for message in ws:
    log.append(decode(message))
    log.seal_due()          # inline; returns None when nothing is due
```

**`examples/websocket.py`**, one file, `--url` optional. With no URL it runs a
tick generator as a local `websockets.serve` coroutine in the *same* event
loop, so the example runs offline and `just check` never touches the network.
`websockets` goes in the `dev` dependency group — not a project dependency, for
the same reason litestream is not one.

**One thing to get right and call out in the file.** `log.append` is a
synchronous SQLite write inside an async loop, so it blocks the loop. At demo
rates that is invisible and at real rates it is the first thing to fix — the
comment says so, and says the fix is a bounded queue drained by
`asyncio.to_thread`, not a second process.

---

## T6 — Headline scan number in the tail demo

**Filed as:** "In the tail demo, fine to have count query timing but we should
also include a headline scan number (time to select all rows of table)."

Today the `read` column times `snapshot()`, which touches no data — that is the
column's whole point ("Nothing here scans data... This is flat"). The ask is
for the number beside it that *does* scan.

**Fix.** A `scan` column: wall-clock to drain
`log.scan(include_archive=...).read_all()`, plus rows/s. Not every tick — a
full scan of a growing log is unbounded and would swamp a 2 s poll — so
`--scan-every` (default 30 s), with the last measured value held in the column
between runs and the cadence printed in the header so nobody reads a stale
number as fresh.

Keeps the contrast the demo is for: a flat metadata read next to a scan whose
cost grows with the log.

---

## T7 — WAL streaming without an archive

**Filed as:** "we shouldn't allow enabling WAL streaming to s3 without archive
enabled."

**Already implemented**, `log.py:2330`:

```python
if config.wal_replication and archive is None:
    msg = ("wal_replication needs an archive: WAL segments go beside the "
           "archived data, and a local-only log has nowhere to ship them")
    raise ValueError(msg)
```

`Log.replication_config` refuses the same case independently, and
`tests/test_construction.py:555` pins the `validate` refusal. **Verified — no
work.** Closed.

---

## Order

T7 is done → T1, T2 (independent, unblock the demo) → T3 → T4 (needs T3) →
T5, T6 (independent).

## Not in this PR

- The archive identity token (§13). T3 trusts the one-writer contract.
- Reading more than one archive per query.
- Schema evolution (§9), still the one specified feature the code lacks.
