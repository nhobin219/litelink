# litelink development commands
# Install just: uv tool install rust-just

# The local S3-compatible endpoint the archive tier is tested and demoed against.
# Matches tests/conftest.py; change both together.
RUSTFS_ENDPOINT := "http://127.0.0.1:9000"
RUSTFS_KEY := "litelink"
RUSTFS_SECRET := "litelink-secret"
RUSTFS_BUCKET := "litelink-demo"

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

# Needs `just rustfs` first, and a maintainer alongside — the writer seals,
# archives and evicts nothing on its own.
#
#   just rustfs         # once
#   just demo-archive   # terminal 1: append, with an archive configured
#   just demo-maintain  # terminal 2: seal, compact, push, evict
#   just demo-tail      # terminal 3: watch `in table` fall as `archived` rises
#
# Capture into a log with the archive tier configured, against local rustfs.
demo-archive *args:
    AWS_ENDPOINT_URL={{RUSTFS_ENDPOINT}} \
    AWS_ACCESS_KEY_ID={{RUSTFS_KEY}} \
    AWS_SECRET_ACCESS_KEY={{RUSTFS_SECRET}} \
    AWS_REGION=us-east-1 \
    uv run python examples/capture.py --archive s3://{{RUSTFS_BUCKET}}/demo {{args}}

# Needs `just rustfs`, a capture running, and the litestream binary on PATH
# (https://litestream.io/install — a single Go binary, not a project dependency:
# it is a sidecar, and a library that pulled it in would be claiming to run it).
#
#   just demo-archive     # terminal 1
#   just demo-replicate   # terminal 2: ship the WAL continuously
#
# Then to prove it, delete the log directory and:
#
#   litestream restore -config litestream.yml -o RESTORED/positions/buffer.db \
#       litelink-data/positions/buffer.db
#
# Continuously replicate the demo log's SQLite state to rustfs (§3a).
demo-replicate root="litelink-data":
    #!/usr/bin/env bash
    set -euo pipefail
    command -v litestream >/dev/null || {
        echo "litestream not on PATH — see https://litestream.io/install" >&2
        exit 1
    }
    export AWS_ENDPOINT_URL={{RUSTFS_ENDPOINT}}
    export AWS_REGION=us-east-1
    # litestream takes credentials from the environment, never from the config
    # file — so the generated YAML is safe to commit and hand around.
    export LITESTREAM_ACCESS_KEY_ID={{RUSTFS_KEY}}
    export LITESTREAM_SECRET_ACCESS_KEY={{RUSTFS_SECRET}}
    uv run python examples/replicate.py --root {{root}} \
        --to s3://{{RUSTFS_BUCKET}}/wal --out {{root}}/litestream.yml
    litestream replicate -config {{root}}/litestream.yml

# Create the demo bucket. Idempotent, and through the same s3fs the library
# uses rather than an AWS CLI nobody is required to have installed.
_rustfs-bucket:
    #!/usr/bin/env bash
    set -euo pipefail
    AWS_ENDPOINT_URL={{RUSTFS_ENDPOINT}} \
    AWS_ACCESS_KEY_ID={{RUSTFS_KEY}} \
    AWS_SECRET_ACCESS_KEY={{RUSTFS_SECRET}} \
    AWS_REGION=us-east-1 \
    uv run --extra s3 python -c "
    import os, s3fs
    fs = s3fs.S3FileSystem(
        key=os.environ['AWS_ACCESS_KEY_ID'],
        secret=os.environ['AWS_SECRET_ACCESS_KEY'],
        client_kwargs={'endpoint_url': os.environ['AWS_ENDPOINT_URL'],
                       'region_name': os.environ['AWS_REGION']},
    )
    if not fs.exists('{{RUSTFS_BUCKET}}'):
        fs.mkdir('{{RUSTFS_BUCKET}}')
    "

# The demo keeps its data on purpose — tail.py reads it after the writer stops, and
# it is there to poke at — so nothing removes it automatically. The benchmarks do
# clean up: they run in a temp directory.
#
# rustfs is an S3-compatible object store in one container — the archive tier
# needs somewhere to push to, and pointing tests and demos at real S3 makes both
# slow, costly and dependent on credentials nobody should need to run `just check`.
# The same code path runs against AWS; only the endpoint differs.
#
#   just rustfs-stop    tears it down, discarding its data
#
# Bring up a local S3-compatible object store for the archive tier. Idempotent.
rustfs:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -n "$(docker ps -q -f name=^litelink-rustfs$)" ]; then
        echo "rustfs already running on {{RUSTFS_ENDPOINT}}"
        exit 0
    fi
    docker rm -f litelink-rustfs >/dev/null 2>&1 || true
    docker run -d --name litelink-rustfs -p 9000:9000 \
        -e RUSTFS_ACCESS_KEY={{RUSTFS_KEY}} \
        -e RUSTFS_SECRET_KEY={{RUSTFS_SECRET}} \
        rustfs/rustfs:latest >/dev/null
    for _ in $(seq 1 40); do
        # Any HTTP answer means it is listening. NOT `curl -f`: an
        # unauthenticated S3 root is a 403, which is a healthy server refusing
        # an anonymous request, and -f treats that as a failure.
        if curl -s -o /dev/null "{{RUSTFS_ENDPOINT}}" 2>/dev/null; then
            just _rustfs-bucket
            echo "rustfs up on {{RUSTFS_ENDPOINT}}"
            echo
            echo "  export AWS_ENDPOINT_URL={{RUSTFS_ENDPOINT}}"
            echo "  export AWS_ACCESS_KEY_ID={{RUSTFS_KEY}}"
            echo "  export AWS_SECRET_ACCESS_KEY={{RUSTFS_SECRET}}"
            echo "  export AWS_REGION=us-east-1"
            echo
            echo "then: just demo-archive"
            exit 0
        fi
        sleep 0.25
    done
    echo "rustfs did not answer on {{RUSTFS_ENDPOINT}}" >&2
    docker logs litelink-rustfs 2>&1 | tail -20 >&2
    exit 1

# Stop rustfs and discard its data. The container is disposable on purpose.
rustfs-stop:
    @docker rm -f litelink-rustfs >/dev/null 2>&1 && echo "rustfs stopped" || echo "not running"

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
