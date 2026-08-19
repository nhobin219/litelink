"""Enforce blank lines after compound statement blocks.

Style rule: add a blank line after every compound statement block
(if/elif/else, for, while, with, try/except/finally) before the
next sibling statement.

NOTE: This script uses only the stdlib so it can run without uv/venv
in pre-commit hooks.

Usage:
    python scripts/check_blank_lines.py [--fix] [paths...]

Without --fix, prints violations and exits non-zero if any found.
With --fix, inserts missing blank lines in-place.
If no paths are given, defaults to the whole repo, matching `just lint` /
`just format-check` and the repo-wide pre-commit hook.
"""

from __future__ import annotations

import ast
import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

# Never checked: build/venv trees that a repo-root walk would otherwise pull in.
# ruff gets these from its own defaults.
EXCLUDED_DIR_NAMES = frozenset(
    {".git", ".venv", "__pycache__", "node_modules", "build", "dist"}
)

# The anchor exclusions are relative to. Derived from this file's own location
# rather than the process cwd or `git rev-parse`: both of those degrade silently
# — a cwd inside a *different* git repo resolves to that repo's root.
REPO_ROOT = Path(__file__).resolve().parents[1]

COMPOUND_TYPES = (
    ast.If,
    ast.For,
    ast.While,
    ast.With,
    ast.Try,
    ast.AsyncFor,
    ast.AsyncWith,
)


def find_violations(source: str, tree: ast.Module) -> list[int]:
    """Return line numbers where a blank line should be inserted.

    Each returned line number is the last line of a compound statement
    that is immediately followed by a sibling statement with no blank
    line in between.
    """
    lines = source.splitlines()
    violations: list[int] = []

    def check_body(body: list[ast.stmt]) -> None:
        for index in range(len(body) - 1):
            statement = body[index]
            next_statement = body[index + 1]

            if not isinstance(statement, COMPOUND_TYPES):
                continue

            end_line = statement.end_lineno
            next_start = next_statement.lineno

            if end_line is None or next_start is None:
                continue

            has_blank = False
            for line_number in range(end_line, next_start - 1):
                line_content = lines[line_number]  # 0-indexed: end_line is 1-indexed
                if line_content.strip() == "":
                    has_blank = True
                    break

            if not has_blank:
                violations.append(end_line)

        for statement in body:
            for attr in ("body", "orelse", "finalbody", "handlers"):
                nested_body = getattr(statement, attr, None)
                if isinstance(nested_body, list) and nested_body:
                    if attr == "handlers":
                        for handler in nested_body:
                            check_body(handler.body)
                    else:
                        check_body(nested_body)

    check_body(tree.body)
    return sorted(set(violations))


def fix_source(source: str, violations: list[int]) -> str:
    """Insert blank lines after the given line numbers."""
    lines = source.splitlines(keepends=True)
    # Process in reverse so insertions don't shift line numbers.
    for line_number in sorted(violations, reverse=True):
        index = (
            line_number  # insert after this 1-indexed line = at this 0-indexed position
        )
        lines.insert(index, "\n")

    return "".join(lines)


def process_file(file_path: Path, fix: bool) -> list[str]:
    """Check (and optionally fix) a single file. Returns violation messages."""
    source = file_path.read_text()

    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []

    violations = find_violations(source, tree)

    if not violations:
        return []

    messages = [
        f"{file_path}:{line_number}: missing blank line after compound statement"
        for line_number in violations
    ]

    if fix:
        fixed = fix_source(source, violations)
        file_path.write_text(fixed)

    return messages


def normalized(path: Path) -> Path:
    """Repo-root-relative form, so every invocation compares the same way."""
    resolved = path.resolve()

    try:
        return resolved.relative_to(REPO_ROOT)
    except ValueError:
        return resolved


def is_excluded_dir(directory: Path) -> bool:
    return bool(EXCLUDED_DIR_NAMES & set(normalized(directory).parts))


def walk_python_files(root: Path) -> list[Path]:
    """Every ``.py`` file under ``root``, pruning excluded trees as it descends.

    ``os.walk`` with in-place ``dirnames`` pruning rather than ``rglob``: rglob
    scandirs every directory before a filter can see the results, so a repo-root
    run would stat the whole of ``.venv`` — hundreds of thousands of entries —
    only to discard them. This gate is on ``just check``'s hot path, and CI
    creates ``.venv`` at the repo root before running it.
    """
    found: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [name for name in dirnames if name not in EXCLUDED_DIR_NAMES]
        current = Path(dirpath)
        if is_excluded_dir(current):
            dirnames[:] = []
            continue

        found.extend(current / name for name in filenames if name.endswith(".py"))

    return sorted(found)


def drop_gitignored(paths: list[Path]) -> list[Path]:
    """Filter out gitignored paths, the way ruff's respect-gitignore does.

    Without this, a repo-root walk picks up local scratch trees, so anyone with
    scratch files would see ``just format-check`` fail on code that is not in the
    repo. One batched ``git check-ignore`` call rather than one per file.

    NUL-delimited both ways: without ``-z`` git renders paths through
    ``core.quotePath``, so a non-ASCII filename comes back escaped, never matches,
    and survives the filter into a ``--fix`` rewrite.
    """
    if not paths:
        return paths

    try:
        result = subprocess.run(
            ["git", "check-ignore", "-z", "--stdin"],
            input="\0".join(str(path) for path in paths),
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        msg = "git is required to skip gitignored paths, and is not on PATH"
        raise RuntimeError(msg) from exc

    # Exit 0 = some paths ignored, 1 = none ignored. Anything else is a real
    # failure (not a git repo) and should not silently widen the set.
    if result.returncode not in (0, 1):
        msg = f"git check-ignore failed: {result.stderr.strip()}"
        raise RuntimeError(msg)

    ignored = {entry for entry in result.stdout.split("\0") if entry}

    return [path for path in paths if str(path) not in ignored]


def collect_paths(args: Sequence[str]) -> list[Path]:
    """Resolve each argument to the ``.py`` files it names.

    A directory argument is walked; gitignored paths are dropped from both cases.
    An argument that names neither is fatal, and says which case it hit: a
    formatting gate that silently collects nothing from a mistyped scope reports
    success on code it never looked at, but reporting "no such path" for an
    existing non-Python file sends the reader hunting a typo that is not there.
    """
    paths: list[Path] = []
    for arg in args:
        path = Path(arg)
        if path.is_file() and path.suffix == ".py":
            paths.append(path)
        elif path.is_dir():
            paths.extend(walk_python_files(path))
        elif path.exists():
            raise SystemExit(f"not a Python file or directory: {arg}")
        else:
            raise SystemExit(f"no such path: {arg}")

    return drop_gitignored(paths)


def main() -> int:
    fix = "--fix" in sys.argv
    args = [arg for arg in sys.argv[1:] if arg != "--fix"]

    if not args:
        args = ["."]

    all_messages: list[str] = []
    for file_path in collect_paths(args):
        all_messages.extend(process_file(file_path, fix))

    for message in all_messages:
        print(message)

    if all_messages and fix:
        print(f"\nFixed {len(all_messages)} violation(s).")

    return 1 if all_messages and not fix else 0


if __name__ == "__main__":
    sys.exit(main())
