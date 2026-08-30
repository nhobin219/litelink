"""Is this machine actually able to run this log?

**The failure this exists to catch is provisioning that looks complete.** Every
tier beyond local disk needs something the Python package cannot carry by
itself, and each one goes missing quietly in its own way:

- litestream is not needed until a `restore` or a `follow`, which is to say it
  is not needed until the worst possible moment to discover it is absent. And
  `which litestream` succeeding in a terminal proves nothing about the systemd
  user unit that will actually run the restore, because user units do not
  inherit a login shell's PATH.
- The DuckDB `httpfs` extension is not compiled into the duckdb wheel and an
  explicit `LOAD` does not fetch it, so an archive read fails on a machine
  where every other check passed. Extensions are built per DuckDB version AND
  platform, so one provisioned for a different duckdb will not load either.
- An archive can be configured against credentials that do not work. `new`
  deliberately allows that — credentials commonly attach to a box after the log
  is configured, and an attempt to refuse it here broke sixteen tests doing
  exactly that legitimately — so nothing on the write path finds out until the
  first `sync`.

None of those is checkable by looking at the code, and all of them are
checkable in about a second. Run this from the process and the user that will
own the log, not from a shell that happens to be logged in.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

from litelink._layout import Layout
from litelink._read import ExtensionMissing, duckdb_connection, load_extension
from litelink._replication import litestream_binary
from litelink._s3 import S3Options
from litelink._table import archive_extent

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass(frozen=True, slots=True)
class Check:
    """One thing that has to be true, and what was found instead."""

    name: str
    ok: bool
    detail: str

    def __str__(self) -> str:
        return f"{'PASS' if self.ok else 'FAIL'}  {self.name}: {self.detail}"


@dataclass(frozen=True, slots=True)
class Report:
    """Every check, and whether the machine is ready.

    Iterable and printable, so a startup probe can `print(report)` and a health
    endpoint can walk `report.checks`.
    """

    checks: tuple[Check, ...]

    @property
    def ok(self) -> bool:
        return all(check.ok for check in self.checks)

    def __iter__(self) -> object:
        return iter(self.checks)

    def __str__(self) -> str:
        lines = [str(check) for check in self.checks]
        lines.append("")
        lines.append("READY" if self.ok else "NOT READY — see the failures above")

        return "\n".join(lines)

    def raise_if_not_ready(self) -> None:
        """For a startup path that should refuse to come up half-provisioned."""
        if self.ok:
            return

        failed = "\n".join(str(c) for c in self.checks if not c.ok)
        msg = f"this machine is not provisioned to run this log:\n{failed}"
        raise RuntimeError(msg)


def _litestream() -> Check:
    """Present, executable, and the right major version.

    Version matters rather than mere presence: v0.5.0 changed the config
    format and `_replication` writes one shape, so an older binary fails at
    restore having passed every presence check.
    """
    resolved = litestream_binary()
    found = resolved if os.sep in resolved else shutil.which(resolved)
    if found is None:
        return Check(
            "litestream",
            ok=False,
            detail=(
                "not found on PATH and not bundled. Needed by restore() and "
                "follow(). Install with `pip install 'litelink[replication]'`"
            ),
        )

    try:
        out = subprocess.run(  # noqa: S603
            [found, "version"],
            capture_output=True,
            check=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return Check("litestream", ok=False, detail=f"{found} would not run: {exc}")

    version = out.stdout.decode(errors="replace").strip() or "unknown"

    return Check("litestream", ok=True, detail=f"{version} at {found}")


def _read_path() -> Check:
    """Build the connection a log builds, the way a log builds it.

    Through `duckdb_connection` rather than a bare `LOAD iceberg`, because the
    two are not the same check and the difference is not academic: `iceberg`
    auto-installs `avro` inside its own init function, so a connection that
    does not load `avro` first fails on a box with no network. An earlier
    version of this check did exactly that and reported a machine NOT READY
    while every real read on it worked.
    """
    try:
        duckdb_connection()
    except ExtensionMissing as exc:
        return Check("duckdb read path", ok=False, detail=str(exc).splitlines()[0])
    except duckdb.Error as exc:
        return Check("duckdb read path", ok=False, detail=str(exc)[:200])

    return Check(
        "duckdb read path",
        ok=True,
        detail=f"iceberg + avro load for duckdb {duckdb.__version__}",
    )


def _extension(name: str, *, required: bool) -> Check:
    """`LOAD name` onto the connection a log would use.

    Onto a real read-path connection rather than a bare one, so an extension
    with its own dependencies is loaded in the same company it will have at
    runtime.
    """
    try:
        load_extension(duckdb_connection(), name, remote=not required)
    except ExtensionMissing as exc:
        return Check(
            f"duckdb `{name}`",
            ok=False,
            detail=str(exc).splitlines()[0],
        )
    except duckdb.Error as exc:
        return Check(f"duckdb `{name}`", ok=False, detail=str(exc)[:160])

    return Check(
        f"duckdb `{name}`", ok=True, detail=f"loads for duckdb {duckdb.__version__}"
    )


def _archive(prefix: str, name: str, s3: S3Options | None) -> Check:
    """Can this machine READ that archive with the credentials it has?

    Through `archive_extent`, which is the same call `new` and `follow` make,
    so this checks what they will actually do rather than something adjacent.
    It reads the published hint from the bucket alone — no `archive.db`, no
    catalog — and it separates the two answers an operator needs told apart:
    `None` for "nothing published there", and a raise for "answered with a
    refusal".

    That distinction is exactly the one `new` cannot act on. An empty prefix
    and a nonexistent bucket both read as no-hint, so `new` has to let bad
    credentials through — credentials commonly attach to a box after the log
    is configured. Here there is a human asking, so a refusal is reportable.
    """
    layout = Layout(Path(tempfile.gettempdir()), name)
    try:
        extent = archive_extent(layout, prefix, (s3 or S3Options()).resolved())
    except Exception as exc:  # noqa: BLE001
        return Check(
            f"archive {prefix}",
            ok=False,
            detail=f"{type(exc).__name__}: {exc}"[:240],
        )

    if extent is None:
        return Check(
            f"archive {prefix}",
            ok=True,
            detail=f"reachable; nothing published for log {name!r} yet",
        )

    return Check(
        f"archive {prefix}", ok=True, detail=f"reachable, holds offsets {extent}"
    )


def preflight(
    *,
    archive: str | None = None,
    name: str = "s",
    s3: S3Options | None = None,
    replication: bool = True,
) -> Report:
    """Check everything this machine needs that the package cannot carry.

    Run it from the process and the user that will own the log — the whole
    point is the gap between a login shell and a service unit.

        python -m litelink                             # local tier only
        python -m litelink s3://bucket/prefix trades   # and the archive

    `replication` skips the litestream check for a deployment that genuinely
    never restores or follows. Everything else is always checked, because a
    log that only writes locally still reads through DuckDB.
    """
    checks: list[Check] = [_read_path()]
    if archive is not None:
        checks.append(_extension("httpfs", required=False))
        checks.append(_archive(archive, name, s3))

    if replication:
        checks.append(_litestream())

    return Report(tuple(checks))


def main(argv: Sequence[str] | None = None) -> int:
    """`python -m litelink [archive-uri]`."""
    import sys

    args = list(sys.argv[1:] if argv is None else argv)
    report = preflight(
        archive=args[0] if args else None,
        name=args[1] if len(args) > 1 else "s",
    )
    print(report)  # noqa: T201

    return 0 if report.ok else 1
