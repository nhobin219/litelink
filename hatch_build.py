"""Build hook: put litestream in the wheel, and tag the wheel for its platform.

**Opt-in, so the ordinary build is unchanged.** With `LITELINK_WHEEL_PLATFORM`
unset — which is every contributor build, every editable install and the sdist
— this does nothing and the result is the pure `py3-none-any` wheel it has
always been. Set it, and the wheel carries a verified litestream and is stamped
for that platform.

    LITELINK_WHEEL_PLATFORM=linux-x86_64 uv build --wheel

The fetch works cross-platform, so one machine produces every wheel; see
`scripts/vendor_litestream.py`, which verifies the release checksum before it
extracts anything.

**Why the binary ships at all.** litelink already runs litestream — `restore`
shells out to it, on a public code path — so it is a runtime dependency and the
"it is a sidecar, not a dependency" framing was only ever true of `replicate`.
Leaving it to PATH means the failure lands at the first restore, which is the
worst moment to discover a missing binary, and PATH is exactly what a systemd
user unit does not inherit from the shell where someone checked.
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path
from typing import Any

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

sys.path.insert(0, str(Path(__file__).parent / "scripts"))

import vendor_duckdb_extension  # noqa: E402
from vendor_litestream import PLATFORMS, vendor  # noqa: E402

ENV = "LITELINK_WHEEL_PLATFORM"


class LitestreamHook(BuildHookInterface):
    """Vendors the binary and forces a platform tag, or stands aside."""

    PLUGIN_NAME = "litelink-litestream"

    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        target = os.environ.get(ENV)
        bundled = Path(self.root) / "src" / "litelink" / ".bin"

        # Never ship a stale binary from a previous platform's build. The
        # directory is build output, not source, and a leftover arm64 binary in
        # an x86_64 wheel would pass every check here and fail on the machine
        # that installed it.
        shutil.rmtree(bundled, ignore_errors=True)

        if target is None:
            return

        if target not in PLATFORMS:
            msg = f"{ENV}={target!r} is not one of {sorted(PLATFORMS)}"
            raise ValueError(msg)

        vendor(target, bundled)

        # And the DuckDB extension, which is pinned twice over: to the
        # platform AND to the exact DuckDB the wheel is built against. The
        # loader checks the running version before using it, so a user who
        # resolves a newer duckdb falls back to machine provisioning rather
        # than loading something that cannot work. The version is declared in
        # the vendor script rather than imported here: the build runs isolated,
        # and the wheel should state which DuckDB it serves.
        vendor_duckdb_extension.vendor(
            target, vendor_duckdb_extension.DUCKDB_VERSION, bundled
        )

        _, tag = PLATFORMS[target]
        build_data["pure_python"] = False
        build_data["tag"] = f"py3-none-{tag}"
        for path in sorted(bundled.rglob("*")):
            if path.is_file():
                build_data["force_include"][str(path)] = str(
                    Path("litelink/.bin") / path.relative_to(bundled)
                )

    def finalize(
        self, version: str, build_data: dict[str, Any], artifact_path: str
    ) -> None:
        """Leave no binary behind in the source tree.

        A build is not supposed to mutate `src/`, and one that leaves a
        platform binary there makes the next build's `pure_python` wheel carry
        it silently.
        """
        shutil.rmtree(Path(self.root) / "src" / "litelink" / ".bin", ignore_errors=True)
