# litelink development commands
# Install just: uv tool install rust-just

# Default recipe: list available commands
default:
    @just --list

# Create the dev environment. Idempotent — safe to re-run after a pull.
bootstrap:
    uv sync
    uv run pre-commit install --hook-type pre-commit --hook-type commit-msg
    @just duckdb-extensions

# Of what the SPEC §7 read path touches, only `parquet` is compiled into the
# duckdb wheel; `iceberg` and the sqlite scanner are fetched from
# extensions.duckdb.org on first use. Provisioning them here pulls that
# download out of the first hot-path read, where it is supposed to be offline.
#
#   just duckdb-extensions            provision the read path
#   just duckdb-extensions --remote   ...and httpfs, for the archive tier
#   just duckdb-extensions --check    verify, offline-style; installs nothing

# Provision the DuckDB extensions the read path needs
duckdb-extensions *args:
    uv run python scripts/install_duckdb_extensions.py {{args}}

# Repo-wide, matching the pre-commit hook's `pass_filenames: false`: a recipe
# scoped to src/ lets tests/ and scripts/ drift out of compliance while CI
# stays green. (Blank line above the doc comment on purpose — `just --list`
# shows only the last contiguous comment line.)

# Lint
lint:
    uv run ruff check .

# Run ruff formatter + blank line fixer
format path=".":
    uv run ruff format {{path}}
    python scripts/check_blank_lines.py --fix {{path}}

# Check formatting without modifying files
format-check path=".":
    uv run ruff format --check {{path}}
    python scripts/check_blank_lines.py {{path}}

# Static type check
typecheck:
    uv run ty check

# Run the test suite. No infra required — the whole system is local files.
test *args:
    uv run pytest {{args}}

# Everything except the slow tier, for a fast inner loop.
test-fast *args:
    uv run pytest -m "not slow" {{args}}

# All pre-push gates. Mirrors CI, so a green `just check` should mean a green PR.
check: lint format-check typecheck test

# Build the wheel + sdist into dist/
build:
    uv build

# Appends only — nothing seals here. Rows stay buffered until demo-maintain runs.
demo-capture *args:
    uv run python examples/capture.py {{args}}

# Seal, compact, evict and expire — the second role. Run with demo-capture.
demo-maintain *args:
    uv run python examples/maintainer.py {{args}}

# Watch a running capture accumulate. Run alongside `just demo-capture`.
demo-tail *args:
    uv run python examples/tail.py {{args}}

# The demo keeps its data on purpose — tail.py reads it after the writer stops, and
# it is there to poke at — so nothing removes it automatically. The benchmarks do
# clean up: they run in a temp directory.
#
# Delete a demo's captured data.
demo-clean root="litelink-data":
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -e "{{root}}" ]; then
        echo "nothing at {{root}}"
        exit 0
    fi
    echo "removing {{root}} ($(du -sh "{{root}}" | cut -f1))"
    rm -rf "{{root}}"

# Write and read throughput on this machine. --quick for a smaller run.
bench *args:
    uv run python benchmarks/throughput.py {{args}}

# What litelink costs on top of the raw SQLite write it is built on.
bench-floor *args:
    uv run python benchmarks/vs_sqlite.py {{args}}
