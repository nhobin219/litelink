"""Smoke coverage for the packaging itself.

Deliberately not vacuous: pytest exits 5 on an empty suite, so a repo with no
tests at all makes the CI test job a no-op that still reports green.
"""

import subprocess
import sys
from pathlib import Path

import pytest

import litelink
from litelink import Log


def test_version_is_a_string() -> None:
    assert isinstance(litelink.__version__, str)
    assert litelink.__version__


def test_py_typed_ships_with_the_package() -> None:
    """Without this marker the package's annotations are invisible downstream."""
    marker = Path(litelink.__file__).parent / "py.typed"

    assert marker.is_file()


@pytest.mark.slow
def test_the_websocket_example_captures_and_queries_in_one_process(
    tmp_path: Path,
) -> None:
    """The smallest shape that is still a durable, queryable log.

    No maintainer, no thread, no second process: `append` then `seal_due`,
    in-line in the event loop. Run rather than imported, because the claim is
    that the SCRIPT works — an import would exercise the functions while
    leaving the wiring, the argument parsing and the local feed untested.

    Offline: with no `--url` it serves its own feed over loopback in the same
    event loop, so this reaches no network.
    """
    pytest.importorskip("websockets", reason="the dev group is not installed")

    root = tmp_path / "ws"
    result = subprocess.run(  # noqa: S603
        [
            sys.executable,
            str(Path(__file__).resolve().parent.parent / "examples" / "websocket.py"),
            "--root",
            str(root),
            "--seconds",
            "2",
            # Its own port, so a developer running the example does not make
            # this fail with "address already in use".
            "--port",
            "8791",
        ],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "appended" in result.stdout
    # It reached Parquet rather than only SQLite, which is the whole point of
    # calling `seal_due` in the loop.
    assert "in 0 file(s)" not in result.stdout, result.stdout
    assert "busiest callsigns" in result.stdout

    # And the log it left behind is a real one, readable by an ordinary open.
    with Log.open(root, "positions", read_only=True) as log:
        assert log.scan().read_all().num_rows > 0
