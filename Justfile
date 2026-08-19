# litelink development commands
# Install just: uv tool install rust-just

# Default recipe: list available commands
default:
    @just --list

# Create the dev environment. Idempotent — safe to re-run after a pull.
bootstrap:
    uv sync
    uv run pre-commit install --hook-type pre-commit --hook-type commit-msg

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
