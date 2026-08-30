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
import hashlib
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
# HTTPS. The same host serves byte-identical content over TLS — verified,
# matching content-length and ETag — and the plain-HTTP form was simply what
# DuckDB's own `.info` files record.
BASE = "https://extensions.duckdb.org"

# The DuckDB a wheel's bundled extension serves. Declared rather than taken
# from the build environment, for two reasons: the build runs isolated and
# cannot import duckdb, and the wheel should state which DuckDB it is good for
# instead of inheriting whatever the build host happened to resolve.
#
# `test_packaging` asserts this matches the floor in `pyproject.toml`. A user
# on a different DuckDB is not broken by it — `_read._bundled_extension` checks
# the running version and falls through to machine provisioning on a miss.
DUCKDB_VERSION = "1.5.5"

# sha256 of each `.gz` as published for DUCKDB_VERSION, recorded here so what
# the wheel carries is a reviewable fact in the repository rather than whatever
# a build runner happened to download.
#
# DuckDB signs extensions and refuses a tampered one at LOAD, so this is not
# the thing standing between a user and arbitrary code — it is what makes a
# corrupt or substituted download fail at BUILD time, on a machine someone is
# watching, instead of becoming a wheel that is broken for everyone who
# installs it. `vendor_litestream.py` verifies for the same reason.
#
# Regenerate with `just duckdb-extension-checksums` after bumping
# DUCKDB_VERSION; a mismatch is a hard failure, never a warning.
CHECKSUMS = {
    "linux_amd64/avro": "b67c7b8f543e1b824167f748e57a62b2d619a03c74c76817ae73340ebc2e9068",
    "linux_amd64/iceberg": "2588cb0046db0ef15f0f18c78434179062c3724832ab00dcb43567be2830edd4",
    "linux_amd64/httpfs": "7cdd52a3135388718884a9b71e3987ba723002121e8e9de399c4ed619d824a05",
    "linux_arm64/avro": "ef6086ea96e6a20b396e0aece88e11fd6a0a32ded0a75134db30ccda6b40983f",
    "linux_arm64/iceberg": "9745d018e3764fd9eeae4c062975fb5f5ed584085d85f297d54b5d738c5ade4b",
    "linux_arm64/httpfs": "0820e0b5b74efaa23608c239df8e744a68943318d530b483a529eace19cb5475",
    "osx_amd64/avro": "3b8ce6fd331bffd66420489e1dcd532b74ba4a5d15d3dbba8500f48b1bc2d507",
    "osx_amd64/iceberg": "3c67e47a78c87d1080627c37bc79299606bd79dd4ada9a931345cadd37fdc3aa",
    "osx_amd64/httpfs": "f445c2692f863bff82609c7061e6e273a4d9fd3b6695e56a6ebc18bd502ed464",
    "osx_arm64/avro": "5531e2418d553b069bf4cc36e6ddefadffd01f179915b1007ce5d778bbbae220",
    "osx_arm64/iceberg": "b9bddab02268434dcdef49f16bbd7d78d3a35beae74cb79c4ecdbb143e106c8a",
    "osx_arm64/httpfs": "758acc0b0c4fbf09506f387ff6f52826b1038b7b6849ded39928d2f992945230",
}


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
                compressed = response.read()
        except urllib.error.HTTPError as exc:
            msg = (
                f"no {name} extension published at {url} ({exc.code}).\n"
                f"DuckDB builds extensions per version and platform, so this "
                f"pair has to exist upstream before it can be vendored."
            )
            raise SystemExit(msg) from exc

        key = f"{platform}/{name}"
        expected = CHECKSUMS.get(key)
        actual = hashlib.sha256(compressed).hexdigest()
        if expected is None:
            msg = (
                f"no recorded checksum for {key} at duckdb {duckdb_version}. "
                f"Regenerate CHECKSUMS after changing DUCKDB_VERSION or the "
                f"platform table — an unrecorded download is one nobody has "
                f"looked at."
            )
            raise SystemExit(msg)

        if actual != expected:
            msg = (
                f"{key} does not match its recorded checksum\n"
                f"  expected {expected}\n"
                f"  actual   {actual}\n"
                f"Refusing to vendor it. This lands in a wheel that everyone "
                f"installs, so a download nobody can account for fails here "
                f"rather than there."
            )
            raise SystemExit(msg)

        destination = into / duckdb_version / platform / f"{name}.duckdb_extension"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(gzip.decompress(compressed))
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
