#!/usr/bin/env python3
"""Provision the DuckDB extensions the read path needs.

SPEC §7 reads through `iceberg_scan` plus the `sqlite` scanner. Neither is
compiled into the duckdb wheel — of what the read path touches, only `parquet`
is statically linked — so DuckDB fetches them from extensions.duckdb.org the
first time a query names one. That download is silent, and it happens on the
first read of a fresh machine: the point at which the hot path is supposed to
be offline.

This script pulls that fetch forward to provisioning time, where it can fail
loudly and where a build can cache it.

Usage:
    python scripts/install_duckdb_extensions.py [--remote] [--check]

    --remote  also provision httpfs, for reading the archive tier (§5).
    --check   verify the extensions load with autoinstall DISABLED, which is
              what an offline or air-gapped box actually does. Installs
              nothing; exits non-zero if a fetch would have been required.

The cache is keyed by DuckDB version and platform (`v1.5.5/linux_amd64`), so a
duckdb upgrade invalidates it and every machine re-downloads. Set
DUCKDB_EXTENSION_DIRECTORY to point at a vendored directory instead — that is
the offline install route, and this script honours it either way.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

# The §7 read path: iceberg_scan for the table leg, the sqlite scanner for the
# buffer leg. Both legs run in one engine, so both are needed for any read.
READ_PATH = ("iceberg", "sqlite_scanner")

# The archive tier (§5). Local-first capture never loads this, which is why it
# is opt-in rather than part of the required set.
REMOTE = ("httpfs",)


def extension_directory(con: duckdb.DuckDBPyConnection) -> str:
    """Where this connection resolves extensions, as DuckDB reports it."""
    configured = con.execute("SELECT current_setting('extension_directory')").fetchone()
    if configured and configured[0]:
        return str(configured[0])

    # Unset means the default, ~/.duckdb/extensions/<version>/<platform>. Read
    # it off an installed extension rather than reconstructing the path, so the
    # answer stays correct if DuckDB changes the layout. install_path names the
    # .duckdb_extension file itself, hence the parent.
    row = con.execute(
        "SELECT install_path FROM duckdb_extensions() WHERE installed AND install_path != ''"
    ).fetchone()

    return str(Path(row[0]).parent) if row else "(default, nothing installed yet)"


def install(extensions: tuple[str, ...]) -> int:
    con = duckdb.connect()
    for name in extensions:
        # INSTALL is idempotent and skips the download if the file is present,
        # so re-running costs nothing. LOAD after it is the part that proves the
        # binary is usable rather than merely present on disk.
        con.execute(f"INSTALL {name}")
        con.execute(f"LOAD {name}")
        print(f"  {name:16} ok")

    print(f"\nextension directory: {extension_directory(con)}")

    return 0


def check(extensions: tuple[str, ...]) -> int:
    """Load with autoinstall off — the offline box's behaviour, not this one's.

    Without disabling autoinstall this check passes on any machine with network
    by silently downloading what it was meant to detect the absence of.
    """
    con = duckdb.connect(
        config={
            "autoinstall_known_extensions": False,
            "autoload_known_extensions": False,
        }
    )

    missing: list[str] = []
    for name in extensions:
        try:
            con.execute(f"LOAD {name}")
        except duckdb.IOException:
            missing.append(name)
            print(f"  {name:16} MISSING")
        else:
            print(f"  {name:16} ok")

    if missing:
        sys.stdout.flush()
        print(
            f"\n{len(missing)} extension(s) would be downloaded on first read: "
            f"{', '.join(missing)}\n"
            f"Run `just duckdb-extensions` (or set DUCKDB_EXTENSION_DIRECTORY).",
            file=sys.stderr,
        )
        return 1

    print(f"\nextension directory: {extension_directory(con)}")

    return 0


def main() -> int:
    args = sys.argv[1:]
    unknown = [arg for arg in args if arg not in {"--remote", "--check"}]
    if unknown:
        print(f"unknown argument(s): {', '.join(unknown)}", file=sys.stderr)
        print(__doc__, file=sys.stderr)
        return 2

    extensions = READ_PATH + (REMOTE if "--remote" in args else ())
    verifying = "--check" in args

    print(
        f"duckdb {duckdb.__version__} — {'verifying' if verifying else 'installing'}:"
    )

    return check(extensions) if verifying else install(extensions)


if __name__ == "__main__":
    sys.exit(main())
