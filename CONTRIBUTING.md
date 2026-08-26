# Contributing

The gates in this repo are strict and mostly automated, so the fastest way to a merged PR is
to know what they check before you write anything.

## Setup

```bash
just bootstrap          # uv sync + git hooks + DuckDB extensions
just check              # lint + format-check + typecheck + tests, exactly what CI runs
```

You need [`uv`](https://docs.astral.sh/uv/) and [`just`](https://github.com/casey/just).
Nothing else — the test suite needs no network, no container, and no credentials, and a
change that breaks that is a change to reject.

`just bootstrap` also installs the `iceberg`, `avro` and `httpfs` DuckDB extensions, which
are downloaded rather than bundled. `just duckdb-extensions --check` verifies a machine can
read with autoinstall off, which is what an offline deployment does.

## Commits

Conventional Commits, enforced by a `commit-msg` hook (`scripts/check_commit_msg.py`). Both
lists are closed — an unknown scope is rejected, so add one to the script in the same commit
if you genuinely need it:

```
<type>(<scope>): <lowercase description, no trailing period, subject <= 72 chars>

types   feat fix refactor perf test docs build chore
scopes  benchmarks blob buffer catalog ci compaction config deps examples read
        replication retention schema seal spec sync write
```

Write the body for someone reading `git log` in a year: what was wrong, why this fix and not
the obvious one, what it cost. The history here is used as documentation and it is expected
to carry reasoning, not a restatement of the diff.

## Style

`ruff` for lint and format at line length 88, [`ty`](https://github.com/astral-sh/ty) for
types, and one house rule the formatter does not know: **a blank line after every
compound-statement block.** `python scripts/check_blank_lines.py --fix` applies it, and
pre-commit runs it for you.

Comments carry the reasoning, not the mechanics. A comment explaining what the next line does
is noise; one explaining why the obvious alternative was rejected is the reason this codebase
is navigable. Match the density of the file you are editing.

## Tests

```bash
just test               # everything
just test-fast          # skips the slow tier, for an inner loop
just test tests/test_seal_group.py -k cut
```

**Falsify every test you write.** Break the code the test covers and confirm the test fails,
then restore it. A test that passes against broken code is worse than no test, because it
reports coverage that is not there.

`tests/test_offline.py` proves the read path works with no network by running it inside a
network namespace. Ubuntu blocks that for unprivileged users, so it *skips* rather than fails
— if you are touching the read path, make sure it actually ran:

```bash
sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0
```

## Performance

The write path is one SQLite transaction at `synchronous=FULL` and the fsync dominates, so
raw SQLite is the floor and the only interesting number is the distance from it.

```bash
just bench              # write and read throughput here
just bench-floor        # what litelink costs on top of raw SQLite
```

Measure before and after in the same session on the same machine — these numbers move with
hardware, and a comparison across two runs on two boxes says nothing. If a change costs
throughput, say so in the commit with the figures rather than leaving it to be discovered.

## Documentation

Three places, and a change usually touches one:

- [`docs/SPEC.md`](docs/SPEC.md) — the design and its reasoning, by section (§). Behaviour
  changes belong here, in the section that claimed the old behaviour.
- [`docs/RUNTIME.md`](docs/RUNTIME.md) — how the pieces run: threads, processes, what crosses
  between them.
- [`README.md`](README.md) — the front door. What it is, how to start, what it is not.

A PR that changes what the library does and leaves the spec describing the old behaviour will
be asked to fix the spec, because a document that lies is worse than a missing one.

## Pull requests

Branch from `main`, keep the PR to one thread of work, and make sure `just check` is green
before pushing — CI runs the same four gates on Python 3.11 and 3.13, and `CI success` is the
required check.

Say what you changed and why in the description. If you found something on the way that you
did not fix, write it down rather than leaving it for the next person to rediscover.
