"""Fetch a pinned litestream into the package, for a platform wheel.

The same fetch `just litestream` does, in Python so a build backend can call
it, and able to fetch for a platform that is not the build host — which is what
lets one machine produce every wheel.

**Verified, not trusted.** The binary comes over the network and is then run
against a log's databases with write access to its replica; a checksum is the
cheapest thing that makes "what the release published" and "what arrived here"
the same question.

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


def _checksum(url: str, asset: str) -> str:
    """The published sha256 for one asset, from the release's checksums file."""
    with urllib.request.urlopen(url, timeout=120) as response:  # noqa: S310
        for line in response.read().decode().splitlines():
            digest, _, name = line.partition("  ")
            if name.strip() == asset:
                return digest.strip()

    msg = f"{asset} is not listed in the release checksums"
    raise SystemExit(msg)


def vendor(target: str, into: Path) -> Path:
    """Fetch, verify, extract. Returns where the binary landed."""
    if target not in PLATFORMS:
        msg = f"unknown platform {target!r}; expected one of {sorted(PLATFORMS)}"
        raise SystemExit(msg)

    suffix, _ = PLATFORMS[target]
    asset = f"litestream-{VERSION}-{suffix}.tar.gz"
    expected = _checksum(f"{BASE}/checksums.txt", asset)

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
