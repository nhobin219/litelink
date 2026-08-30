"""Fetch a DuckDB extension into the package, for a platform wheel.

**This one is pinned twice over.** DuckDB extensions are built per extension
version AND per platform — the download path is literally
`/v1.5.5/linux_amd64/httpfs.duckdb_extension.gz` — so a bundled copy is only
usable by the exact DuckDB it was built for. That is why `_read.load_extension`
checks the running version before reaching for the bundle rather than trusting
it, and why the bundle is a fast path rather than the mechanism.

**Both extensions, not just `httpfs`.** An earlier version vendored only
`httpfs`, on a measurement that said `LOAD iceberg` autoinstalls where `httpfs`
raises. That measurement was wrong in the way that matters: autoinstall is a
DOWNLOAD, so it works on an empty extension directory with a network and fails
without one. Verified on a box with no network and no cache — `iceberg` fails
exactly like `httpfs`, and a local-first log cannot read at all.

    python scripts/vendor_duckdb_extension.py --into src/litelink/.bin
    python scripts/vendor_duckdb_extension.py --platform osx_arm64 --into ...
"""

from __future__ import annotations

import argparse
import gzip
import sys
import urllib.error
import urllib.request
from pathlib import Path

# The wheel platforms litelink publishes, mapped to DuckDB's own platform
# string — the one `PRAGMA platform` reports, which is what names the download
# directory. They do not match the Go-style names litestream uses, so the two
# vendoring scripts cannot share a table.
PLATFORMS = {
    "linux-x86_64": "linux_amd64",
    "linux-arm64": "linux_arm64",
    "macos-x86_64": "osx_amd64",
    "macos-arm64": "osx_arm64",
}

# `avro` is not requested by litelink — `iceberg` pulls it in, and does so by
# AUTO-INSTALLING it during its own init function, which needs the network.
# Found the only way it could be: on a box with no network and no cache, where
# `LOAD iceberg` raised
#     Initialization function "iceberg_duckdb_cpp_init" ... threw an exception:
#     "An error occurred while trying to automatically install ... 'avro'"
# So a bundle that carries `iceberg` without `avro` is not offline-capable, and
# the failure appears only once everything else has been provisioned correctly.
EXTENSIONS = ("avro", "iceberg", "httpfs")
BASE = "http://extensions.duckdb.org"

# The DuckDB a wheel's bundled extension serves. Declared rather than taken
# from the build environment, for two reasons: the build runs isolated and
# cannot import duckdb, and the wheel should state which DuckDB it is good for
# instead of inheriting whatever the build host happened to resolve.
#
# `test_packaging` asserts this matches the floor in `pyproject.toml`. A user
# on a different DuckDB is not broken by it — `_read._bundled_extension` checks
# the running version and falls through to machine provisioning on a miss.
DUCKDB_VERSION = "1.5.5"


def extension_url(duckdb_version: str, platform: str, name: str) -> str:
    return f"{BASE}/v{duckdb_version}/{platform}/{name}.duckdb_extension.gz"


def vendor(target: str, duckdb_version: str, into: Path) -> list[Path]:
    """Fetch and decompress every extension. Returns where they landed.

    Laid out as `<duckdb version>/<platform>/httpfs.duckdb_extension`, mirroring
    DuckDB's own extension directory, so the loader can find it by the version
    it is actually running rather than by assuming the build's.
    """
    if target not in PLATFORMS:
        msg = f"unknown platform {target!r}; expected one of {sorted(PLATFORMS)}"
        raise SystemExit(msg)

    platform = PLATFORMS[target]
    landed: list[Path] = []
    for name in EXTENSIONS:
        url = extension_url(duckdb_version, platform, name)
        # An explicit User-Agent: the extension CDN answers 403 to urllib's
        # default while serving curl the same file, which reads as a missing
        # extension rather than a rejected client.
        request = urllib.request.Request(  # noqa: S310
            url, headers={"User-Agent": "litelink-vendor"}
        )
        try:
            with urllib.request.urlopen(request, timeout=300) as response:  # noqa: S310
                payload = gzip.decompress(response.read())
        except urllib.error.HTTPError as exc:
            msg = (
                f"no {name} extension published at {url} ({exc.code}).\n"
                f"DuckDB builds extensions per version and platform, so this "
                f"pair has to exist upstream before it can be vendored."
            )
            raise SystemExit(msg) from exc

        destination = into / duckdb_version / platform / f"{name}.duckdb_extension"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
        landed.append(destination)

    return landed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", default=None, help="default: this machine")
    parser.add_argument("--duckdb-version", default=DUCKDB_VERSION)
    parser.add_argument("--into", type=Path, required=True)
    args = parser.parse_args()

    target = args.platform
    if target is None:
        import duckdb

        reverse = {v: k for k, v in PLATFORMS.items()}
        row = duckdb.connect().execute("PRAGMA platform").fetchone()
        native = None if row is None else str(row[0])
        target = reverse.get(native or "")
        if target is None:
            msg = f"no litelink wheel is published for DuckDB platform {native!r}"
            raise SystemExit(msg)

    for landed in vendor(target, args.duckdb_version, args.into):
        size = landed.stat().st_size / 1e6
        print(  # noqa: T201
            f"vendored {landed.name} for duckdb {args.duckdb_version} "
            f"-> {landed} ({size:.1f} MB)"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
