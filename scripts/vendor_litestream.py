"""Fetch a pinned litestream into the package, for a platform wheel.

The same fetch `just litestream` does, in Python so a build backend can call
it, and able to fetch for a platform that is not the build host — which is what
lets one machine produce every wheel.

**Verified against a digest recorded HERE, not one fetched alongside.** The
binary comes over the network and is then run against a log's databases with
write access to its replica. An earlier version compared the download against
the release's own `checksums.txt` — which detects corruption but not
substitution, because GitHub release assets are mutable and a re-cut asset
comes with regenerated checksums. Pinning the digest in the repository makes
what ships a reviewable fact, the way `vendor_duckdb_extension.py` does.

Run directly to vendor for this machine:

    python scripts/vendor_litestream.py --into src/litelink/.bin

or name a platform:

    python scripts/vendor_litestream.py --platform linux-arm64 --into ...
"""

from __future__ import annotations

import argparse
import hashlib
import platform
import shutil
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path

# Pinned rather than floating. `_replication` writes one config shape and
# v0.5.0 changed the format, so the binary and the code that configures it move
# together or not at all.
VERSION = "0.5.16"
BASE = f"https://github.com/benbjohnson/litestream/releases/download/v{VERSION}"

# sha256 of each release asset, recorded rather than fetched. Regenerate with
# `just litestream-checksums` after bumping VERSION; a mismatch is fatal.
CHECKSUMS = {
    "linux-x86_64": "9e29112380a942e4a62ee07773684396cb8b308dc4d67e130bef41f75e937f0a",
    "linux-arm64": "678022e4103145302598e35d37f8718392d42e153feeb1e2d4a64dd0cd3aaf10",
    "macos-x86_64": "eb554b93c9e2833351b017707e9ba5ac97ffd91d07e8b8b836b3ca7661399c36",
    "macos-arm64": "3e64028ff3522caca7a5ab67244e0373b25f3db68b6e25cac0056bf71c30c337",
}

# The wheel tags litelink publishes, and the release asset each one needs.
# Keys are what `--platform` accepts; the tag is what the wheel is stamped with.
# Asset names taken from the release's own checksums.txt, not guessed: the
# release uses `x86_64`, not the `amd64` its Go build would suggest.
PLATFORMS = {
    "linux-x86_64": ("linux-x86_64", "manylinux_2_17_x86_64.manylinux2014_x86_64"),
    "linux-arm64": ("linux-arm64", "manylinux_2_17_aarch64.manylinux2014_aarch64"),
    "macos-x86_64": ("darwin-x86_64", "macosx_11_0_x86_64"),
    "macos-arm64": ("darwin-arm64", "macosx_11_0_arm64"),
}


def host_platform() -> str:
    """This machine's key in `PLATFORMS`, or a message naming what it is."""
    system = platform.system().lower()
    machine = platform.machine().lower()
    arch = {
        "aarch64": "arm64",
        "arm64": "arm64",
        "x86_64": "x86_64",
        "amd64": "x86_64",
    }.get(machine)
    name = {"linux": "linux", "darwin": "macos"}.get(system)
    if arch is None or name is None:
        msg = f"no litestream wheel is published for {system}/{machine}"
        raise SystemExit(msg)

    return f"{name}-{arch}"


def vendor(target: str, into: Path) -> Path:
    """Fetch, verify, extract. Returns where the binary landed."""
    if target not in PLATFORMS:
        msg = f"unknown platform {target!r}; expected one of {sorted(PLATFORMS)}"
        raise SystemExit(msg)

    suffix, _ = PLATFORMS[target]
    asset = f"litestream-{VERSION}-{suffix}.tar.gz"
    expected = CHECKSUMS.get(target)
    if expected is None:
        msg = (
            f"no recorded checksum for {target}. Regenerate CHECKSUMS after "
            f"changing VERSION or the platform table — an unrecorded download "
            f"is one nobody has looked at."
        )
        raise SystemExit(msg)

    with tempfile.TemporaryDirectory() as scratch:
        archive = Path(scratch) / asset
        with urllib.request.urlopen(f"{BASE}/{asset}", timeout=300) as response:  # noqa: S310
            archive.write_bytes(response.read())

        actual = hashlib.sha256(archive.read_bytes()).hexdigest()
        if actual != expected:
            msg = (
                f"{asset} does not match the published checksum\n"
                f"  expected {expected}\n"
                f"  actual   {actual}\n"
                f"Refusing to vendor it: this binary is run against a log's "
                f"databases with write access to its replica."
            )
            raise SystemExit(msg)

        into.mkdir(parents=True, exist_ok=True)
        with tarfile.open(archive) as tar:
            member = tar.getmember("litestream")
            extracted = tar.extractfile(member)
            if extracted is None:
                msg = f"{asset} has no `litestream` entry"
                raise SystemExit(msg)

            destination = into / "litestream"
            with destination.open("wb") as out:
                shutil.copyfileobj(extracted, out)

    destination.chmod(0o755)

    return destination


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", default=None, help="default: this machine")
    parser.add_argument("--into", type=Path, required=True)
    args = parser.parse_args()

    landed = vendor(args.platform or host_platform(), args.into)
    print(f"vendored litestream {VERSION} -> {landed}")  # noqa: T201

    return 0


if __name__ == "__main__":
    sys.exit(main())
