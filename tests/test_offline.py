"""I5, proved rather than approximated (SPEC §10, §14).

§14's first bullet is *"block all network access; assert writes, seals,
compaction and hot reads all succeed"*, and I5 is the claim it tests: a read
served from within `local_retention` never touches the network. That claim is
the reason the design has no daemon, no broker and no catalog service, so it is
worth testing at the level it is made.

Patching `socket.socket` does not reach that level. DuckDB and pyiceberg-core
do their I/O from C++ and Rust, which never passes through Python's socket
module — demonstrated: with `socket.socket` raising, `urllib` is blocked while
DuckDB still downloads an entire extension. So these run the whole loop inside a
network namespace with no interfaces, where the kernel refuses regardless of
which language asks.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import textwrap
from typing import TYPE_CHECKING

import duckdb
import pytest

from litelink._read import ExtensionMissing, load_extension

if TYPE_CHECKING:
    from pathlib import Path

# `unshare -rn` maps the caller to root inside a new user namespace and gives it
# a network namespace holding only a down loopback. No privileges needed, and
# nothing outside the test is affected.
UNSHARE = ("unshare", "--map-root-user", "--net")


def _namespaces_work() -> bool:
    """Whether an unprivileged network namespace can actually be created.

    Probed rather than inferred from the binary existing. GitHub's runners ship
    `unshare` and refuse to use it — Ubuntu restricts unprivileged user
    namespaces through AppArmor, so the call fails with
    `write failed /proc/self/uid_map: Operation not permitted`. Checking
    `which unshare` reported these tests as runnable and then failed all three.
    """
    if sys.platform != "linux" or shutil.which("unshare") is None:
        return False

    probe = subprocess.run(
        [*UNSHARE, "true"], capture_output=True, timeout=30, check=False
    )

    return probe.returncode == 0


pytestmark = pytest.mark.skipif(
    not _namespaces_work(),
    reason=(
        "needs unprivileged Linux network namespaces; on Ubuntu enable with "
        "sysctl -w kernel.apparmor_restrict_unprivileged_userns=0"
    ),
)


def run_offline(code: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [*UNSHARE, sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )


def test_the_namespace_really_blocks_the_network() -> None:
    """Guard the guard.

    If `unshare` silently stopped isolating — a kernel setting, a container
    without user namespaces — every test below would pass while proving
    nothing. This one fails instead.
    """
    result = run_offline("""
        import urllib.request
        try:
            urllib.request.urlopen("https://extensions.duckdb.org", timeout=5)
            print("REACHABLE")
        except Exception:
            print("BLOCKED")
    """)

    assert "BLOCKED" in result.stdout, f"namespace is not isolating: {result.stdout}"


def test_duckdb_cannot_reach_the_network_either(pytestconfig: pytest.Config) -> None:
    """The specific hole the socket-patching version left open.

    With extensions NOT provisioned, DuckDB's autoinstall must fail — which is
    what makes the passing tests below meaningful, since it shows they are
    loading from disk rather than quietly fetching.
    """
    # HOME, not DUCKDB_EXTENSION_DIRECTORY: that variable is silently ignored
    # by duckdb, so setting it would leave this test pointed at the real cache
    # and passing for the wrong reason.
    empty_home = pytestconfig.invocation_params.dir / ".pytest_offline_home"
    result = run_offline(f"""
        import os
        os.environ["HOME"] = {str(empty_home)!r}
        import duckdb
        try:
            duckdb.connect().execute("LOAD iceberg")
            print("LOADED")
        except Exception as exc:
            print("BLOCKED", type(exc).__name__)
    """)

    assert "BLOCKED" in result.stdout, f"DuckDB reached the network: {result.stdout}"


def test_the_whole_loop_runs_with_no_network(tmp_path: Path) -> None:
    """Append, seal, read, compact, expire — all of it, offline.

    Extensions come from the local cache the provisioning step fills, which is
    the other half of the claim: litelink is offline-capable *once provisioned*,
    and §7 is explicit that provisioning is a separate obligation.
    """
    result = run_offline(f"""
        import pyarrow as pa
        from datetime import timedelta
        import litelink
        from litelink import LogConfig

        schema = pa.schema([
            pa.field("event_ts", pa.int64()),
            pa.field("key", pa.string()),
            pa.field("payload", pa.string()),
        ])
        config = LogConfig(
            target_seal_size=4096,
                compact_min_files=2,
            snapshot_retention=timedelta(microseconds=1),
        )
        rows = [
            {{"event_ts": i, "key": f"k{{i % 3}}", "payload": '{{"seq":%d}}' % i}}
            for i in range(400)
        ]

        with litelink.new({str(tmp_path / "offline")!r}, "s", schema=schema,
                     sort_by=("event_ts",), config=config) as log:
            log.extend(rows)
            log.seal()
            log.extend(rows)
            log.seal()
            log.maintain()
            total = log.scan().read_all().num_rows
            bounded = log.scan(where="event_ts < 10").read_all().num_rows
            print("ROWS", total, "BOUNDED", bounded, "END", log.end_offset())

        with litelink.open({str(tmp_path / "offline")!r}, "s") as reopened:
            print("REOPENED", reopened.scan().read_all().num_rows)
    """)

    assert result.returncode == 0, f"offline run failed:\\n{result.stderr}"
    assert "ROWS 800 BOUNDED 20 END 801" in result.stdout, result.stdout
    assert "REOPENED 800" in result.stdout, result.stdout


def test_an_unprovisioned_extension_names_the_command_that_fixes_it(
    tmp_path: Path,
) -> None:
    """The traceback a fresh machine used to get, and what replaced it.

    Reported from a working install: `log.scan(include_archive=True)` raised
    `IOException: Extension ".../httpfs.duckdb_extension" not found`, advising
    `INSTALL httpfs` — a remedy this repo does not use and §7 argues against.

    **`autoinstall_known_extensions` reports True below and the LOAD still
    fails**, which is the part that surprises — so the report is not a machine
    with autoinstall switched off. Measured beside it in a fresh process with
    the same empty directory, `LOAD iceberg` silently DOWNLOADS and succeeds.
    LOAD-time autoinstall is per-extension, and this asserts only the half that
    litelink has to survive.
    """
    empty = tmp_path / "extensions"
    empty.mkdir()
    connection = duckdb.connect(config={"extension_directory": str(empty)})

    assert connection.execute(
        "SELECT current_setting('autoinstall_known_extensions')"
    ).fetchone() == (True,), "the point of this test is that LOAD ignores it"

    with pytest.raises(ExtensionMissing) as caught:
        load_extension(connection, "httpfs", remote=True)

    message = str(caught.value)
    # The flag, not just the script. `httpfs` is behind `--remote`, and an
    # error naming the bare command sends the reader to a run that installs
    # everything except the extension they are missing.
    # Three remedies, for three readers: someone who installed the wheel,
    # someone who will fetch it into their own DuckDB home, and a contributor
    # with a checkout. The message used to name only the last two, which are
    # both useless to a `pip install` consumer — the failure this whole test
    # is about is provisioning that LOOKS complete.
    assert "pip install 'litelink[archive]'" in message
    assert "INSTALL httpfs" in message
    assert "just duckdb-extensions --remote" in message

    # And it says the thing that makes a wrong fix look right: extensions are
    # built per DuckDB version and platform.
    assert "per DuckDB version and platform" in message

    # `remote=True` earns its keep by saying who can ignore this.
    assert "Only the archive tier needs this" in message
    # DuckDB's own error is kept as the cause rather than swallowed: it names
    # the exact path that was searched, which is the only way to tell a missing
    # extension from one installed under a different HOME.
    assert isinstance(caught.value.__cause__, duckdb.IOException)


def test_the_read_path_extension_is_not_offered_the_remote_flag(
    tmp_path: Path,
) -> None:
    """`iceberg` is in the required set, so `--remote` would be wrong advice.

    Two extensions, two provisioning commands, and the error has to pick the
    right one — this is the half of the pair a single hard-coded message would
    get wrong.

    **Autoinstall is disabled here, and it has to be.** Unlike `httpfs`, a
    `LOAD iceberg` against an empty directory downloads the extension and
    succeeds, so an empty directory alone does not reproduce anything. What
    does is the offline machine, which is the case §7 cares about and the one
    `install_duckdb_extensions.py --check` models with these same two settings.
    """
    empty = tmp_path / "extensions"
    empty.mkdir()
    connection = duckdb.connect(
        config={
            "extension_directory": str(empty),
            "autoinstall_known_extensions": False,
            "autoload_known_extensions": False,
        }
    )

    with pytest.raises(ExtensionMissing) as caught:
        load_extension(connection, "iceberg", remote=False)

    assert "--remote" not in str(caught.value)
    assert "just duckdb-extensions" in str(caught.value)
