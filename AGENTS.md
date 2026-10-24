# litelink

An embedded Iceberg storage engine for append-only data: rows land in a local SQLite
buffer, are sealed to Parquet, and are published to an Iceberg table that any analytical
engine reads. It is **an embedded engine the way DuckDB is an embedded query engine** —
a library you link, not a system you operate. Describe it by category before mechanism,
and compare it with DuckDB and SQLite, not with Kafka or a catalog service.

"Would an embedded engine do this?" rules out daemons, timers and subprocesses the
library owns.

Read [`CONTRIBUTING.md`](CONTRIBUTING.md) for the gates (setup, commit format, style,
tests, releases) and [`docs/SPEC.md`](docs/SPEC.md) before changing behaviour. This file
adds what an agent or a new contributor needs on top of them.

## Commands

```bash
just bootstrap          # uv sync + hooks + DuckDB extensions + .bin/litestream
just check              # lint + format-check + typecheck + tests. What CI runs.
just test-fast          # skips the slow tier, for an inner loop
just test tests/test_seal_group.py -k cut
just rustfs             # a local S3 endpoint, so the replication tier runs
```

A fresh clone or worktree needs `just bootstrap`. Without its `.bin/litestream` a dozen
replication and restore tests skip or fail, and that reads like a regression.

The replication tier runs against a local endpoint (`just rustfs`). A change to
replication, restore or the S3 path is not shown to pass until that tier has run against
one; a skip is not a pass.

## Its consumers are not only Python callers

streamcast stores every log it serves as a litelink table, so a change to the write path,
sealing, retention or the catalog reaches every stream on every broker that uses it. Say in
the pull request which streamcast behaviour a change could move, and test it through
streamcast too when it could.

## What this library must never do

litelink is the durable record of a stream, so a defect is never just a wrong answer. It
is one of four failures, and every change is argued against them:

1. an acknowledged row lost;
2. a row served twice;
3. different rows served for an offset;
4. a file deleted while a live snapshot still references it.

`docs/SPEC.md` §10 names the invariants (I2, I4, I6, I9 and the rest). Cite the one a
change relies on, in the pull request and in the code where it matters.

## Design rules the code already follows

- **One copy of every fact.** Durable state (the config, the published location, the sort
  order) is read through from the buffer's `meta` at each decision, and read once per
  decision so two reads cannot disagree. Never hold it in a field: every defect at the old
  seams came from a stale copy with a refresh call missing somewhere (SPEC §4a).
- **Inject collaborators.** `new`, `open` and `restore` build the `Layout`, `Buffer`,
  `LogTable`, `Published`, `Reader` and `Maintenance` once and pass them in at
  construction; `__init__` does no I/O. A function that needs a capability takes just
  that, such as a claim's `renew` callable rather than the claim. Units then test without a
  whole log, and nothing patches a half-built object after the fact. Protocols, not base
  classes.
- **One responsibility per module.**
  - `Layout` derives paths and does no I/O.
  - `_claim` owns ranges and expiry.
  - `_tiers` owns the tier rows.
  - `_retired` owns the retired-log operations.

  A new concern gets a module of its own rather than growing `_handle`.
- **Decide under the claim.** Take the claim, then re-read every premise; anything read
  before it is a statement about the past. Claim the range in the same transaction as the
  check. Renew (`checkpoint`) before every commit, and after any step that can outlast the
  30 s TTL, such as an S3 commit or a manifest walk.
- **A rare whole-log operation takes the whole-log claim.** A narrower claim needs an
  argument, and that argument needs a review. `truncate`'s `[0, below)` looked safe and
  wasn't, in two ways.
- **Never delete directly.** Every file's path is in SQLite before the file exists (I2).
  Deletion is a commit that drops the file, a queue row in `pending_delete` re-dated to
  that commit, and a drain after the grace that vetoes anything still referenced. The one
  exception is a retired log: no maintainer runs on it, so it deletes at once, and the
  readers' grace there is the caller's to give (streamcast moves its floor before it
  truncates). Say so in the pull request when a change pushes a duty like that up a layer.
- **Names are unique per attempt.** A token goes in every written path, and no path is ever
  reused. Nothing lists a directory except the metadata sweep and `delete`.
- **Fail in the safe direction.** A statistics row may overstate, never understate. A
  failed read of our own metadata is an error, never "absent": creating over a table you
  couldn't read destroys it.
- **Caches key on immutable identity.** Key by the path of an object no write replaces,
  bound by bytes, and scope by the credentials that read it, so one key is never served
  another's bytes.
- **Write no safety argument you haven't verified.** If you can't prove the weaker design
  safe, keep the conservative one, and say in a comment why it is needed, not that the
  alternative "would also be safe". Verify a dependency fact the same way: a feature you
  use must exist at the lowest version `pyproject.toml` allows (a pyarrow option once
  shipped against a floor three releases older than the option).

## Compatibility

- **Before 1.0, prefer the consistent API.** A breaking change is fine when it leaves one
  standard shape instead of two. Say it loudly in the CHANGELOG, ship it in a minor
  release, and add no compatibility shim unless asked. Additions and fixes ship as a
  patch. Under 0.x a minor release is a breaking one, so consumers cap the minor.
- **Anything another version reads changes in two releases**, expand then contract: the
  buffer's schema, the metadata layout, the published table's properties. Readers accept
  old and new while writers keep the old, and writers switch only once every reader has
  upgraded. Measure which versions still read each other's state before claiming it.
- **Say what it means downstream.** A pull request names whether the change is breaking,
  the version it should ship as, and whether consumers' pins must move.

## Tests

- **No network, container or credentials** in the default suite (`CONTRIBUTING.md`); a
  change that breaks that is rejected. Endpoints on loopback, databases in temp files, a
  real tool offline from its cache.
- **Falsify every test you write.** Break the code it covers, see it fail, restore it, see
  it pass, and say in the pull request what you broke. Check that the break applied: a
  patch that matched nothing, or one that hangs instead of failing, proves nothing. When
  you touch an old test, check that it still asserts something.
- **A new test takes seconds.** Measure with `pytest --durations`. One that needs more is
  narrowed to the claim it proves, or goes in the slow tier.
- **Make concurrency deterministic.** Force an interleaving by running the other operation
  from a hook at the point where the first has read something and not yet committed it.
  Never sleeps or timing.
- **Measure what you claim.** A cost or performance claim carries before-and-after numbers
  from the same session on the same machine, and they go in the pull request and the
  CHANGELOG. Object-store costs are measured against `just rustfs`: count real requests,
  with pyarrow's S3 debug log (`pyarrow.fs.initialize_s3(S3LogLevel.Debug)`) or by
  patching `PyArrowFile.open`, with the change on and off in one session.
- **The full suite catches what review misses.** It caught the metadata cache serving one
  credential's reads to another (#185). A failure is read, not retried.

## Review

A change that can lose, duplicate or corrupt rows, deletes or rewrites durable state, or
changes how concurrent operations exclude each other gets an adversarial review before it
merges: the `critical-reviewer` rubric in
[`.claude/agents/critical-reviewer.md`](.claude/agents/critical-reviewer.md), driven as
[`.claude/skills/critical-review/SKILL.md`](.claude/skills/critical-review/SKILL.md)
describes. Give it two or three attacks phrased as "find the state where X breaks", each
tied to a SPEC invariant, and what is already verified, with numbers. Reproduce each
finding before acting on it, and review again after each fix until a pass finds nothing.

It has found a real defect in every sensitive change it reviewed:

- a lapsed claim overwriting an eviction's tier-row widening (#176);
- a narrower claim that wasn't safe (#176);
- a `delete` that couldn't resume after a crash, and a `truncate` that could create an
  empty table (#183).

## Writing

- **Docs and comments describe the code as it is**: the decision and its reasoning, in the
  present tense, including why the obvious alternative was rejected, at the density of the
  file around it. History ("this used to", "before #N") goes in the commit message and the
  CHANGELOG. Add beside existing prose; never shorten or reword a sentence the change does
  not need.
- **Verify every commit.** The `commit-msg` hook can reject one silently when its output is
  piped, so check `git log -1` after each. After an interrupted or rejected edit, check
  `git diff` before the next commit: the write may have landed.

## Releases

`CONTRIBUTING.md` § Releases is the process. A PyPI version can never be corrected, so:

- A tag goes on `main`, on a tree CI passed. Confirm `main` carries the version and the
  dated CHANGELOG before tagging, and never chain a merge and a tag in one command: a
  blocked merge once put a tag on the previous version.
- Moving or deleting a pushed tag is a release decision like making one.

## Tooling that bites

- uv's index cache lags a fresh upload: `uv lock --refresh-package <name>`.
- Stop a process by its PID, never `pkill -f`, which matches your own shell.
