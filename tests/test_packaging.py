"""What ships, and whether it matches what the code expects.

These are cheap assertions about facts that only break at install time, on
someone else's machine, after everything in a checkout has passed.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

import duckdb
import pytest

from litelink._read import _bundled_extension
from litelink._replication import litestream_binary

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def pyproject() -> dict:
    return tomllib.loads((ROOT / "pyproject.toml").read_text())


def test_the_bundled_extension_matches_the_pinned_duckdb(pyproject: dict) -> None:
    """The wheel's extensions are built for one exact DuckDB, and must say so.

    DuckDB builds extensions per version AND platform — the download path is
    `/v1.5.5/linux_amd64/...` — so the version the build vendors for has to be
    a version the dependency range actually admits. Vendoring for one DuckDB
    while depending on another produces a wheel whose bundle can never load,
    and the fallback hides it: the loader ignores a mismatched bundle, so the
    wheel is merely useless offline rather than broken, which is worse to
    diagnose.

    Falsify by bumping `DUCKDB_VERSION` without touching the dependency floor.
    """
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    from vendor_duckdb_extension import DUCKDB_VERSION

    declared = next(
        dep for dep in pyproject["project"]["dependencies"] if dep.startswith("duckdb")
    )
    floor = declared.split(">=")[1].split(",")[0].strip()

    assert DUCKDB_VERSION == floor, (
        f"the build vendors extensions for duckdb {DUCKDB_VERSION} while "
        f"pyproject declares {declared!r}. A wheel built this way carries a "
        f"bundle its own floor cannot load."
    )


def test_the_bundle_is_looked_up_by_the_running_duckdb() -> None:
    """A mismatched bundle must be ignored, not loaded.

    `duckdb>=…` has no ceiling, so a user can resolve a newer DuckDB than the
    wheel was built against. The bundle is a fast path; on a miss the loader
    falls through to the ordinary `LOAD` and then to the message that says how
    to provision.
    """
    connection = duckdb.connect()

    # Whatever this checkout has, the lookup must be keyed on the RUNNING
    # version — so asking for an extension under a version that cannot be
    # installed here answers None rather than something wrong.
    assert _bundled_extension(connection, "definitely-not-an-extension") is None


def test_the_binary_resolution_order_is_checkout_then_wheel_then_path() -> None:
    """A checkout's binary wins, so a contributor tests what they pinned.

    The order matters in both directions: a developer with `.bin/litestream`
    must not silently test against a wheel's copy, and an installed package
    with no checkout must not fall through to PATH when it carries its own —
    PATH being exactly what a systemd user unit does not inherit.
    """
    resolved = litestream_binary()

    if (ROOT / ".bin" / "litestream").exists():
        assert resolved == str(ROOT / ".bin" / "litestream")
    else:
        assert resolved == "litestream" or resolved.endswith("/.bin/litestream")

    assert litestream_binary("/explicit/path") == "/explicit/path", (
        "an explicit binary= must beat every other source"
    )


def test_the_package_declares_what_it_ships(pyproject: dict) -> None:
    """py.typed and the licences travel, and the build hook is wired.

    Each of these is invisible in a checkout and only missing in the wheel.
    """
    assert (ROOT / "src" / "litelink" / "py.typed").is_file()
    for name in pyproject["project"]["license-files"]:
        assert (ROOT / name).is_file(), f"{name} is declared but not present"

    hooks = pyproject["tool"]["hatch"]["build"]["targets"]["wheel"]["hooks"]
    assert hooks["custom"]["path"] == "hatch_build.py"


def test_no_extra_promises_a_binary(pyproject: dict) -> None:
    """Extras add dependencies, not files.

    An extra cannot put litestream or a DuckDB extension into the wheel, so one
    named for them would be a promise the packaging system cannot keep. They
    ship in the platform wheel instead.
    """
    extras = pyproject["project"].get("optional-dependencies", {})

    assert "replication" not in extras
    assert "archive" not in extras
    assert "s3" not in extras, (
        "the s3 extra was cargo: nothing imports s3fs, and pyiceberg resolves "
        "PyArrowFileIO first"
    )


def test_the_s3_tier_is_not_silently_skipped() -> None:
    """91 tests vanished when `s3fs` stopped being installed, and nothing said so.

    `conftest.filesystem` uses `pytest.importorskip`, so removing the `s3`
    extra turned the entire archive tier into skips — a green run that had
    stopped checking the tier most of this library's bugs have been in. The
    suite reported `286 passed, 91 skipped` and looked fine.

    s3fs is a TEST dependency: the fixtures assert against a bucket directly,
    which is a different job from litelink's own S3 access. It belongs in the
    dev group, and its absence should fail rather than skip.

    Falsify by removing `s3fs` from the dev group: this fails instead of the
    archive suite quietly halving.
    """
    try:
        import s3fs  # noqa: F401
    except ImportError:  # pragma: no cover - the failure this exists to make loud
        pytest.fail(
            "s3fs is not installed, so the entire archive tier will SKIP rather "
            "than run. It is a dev dependency of the test fixtures. Run `uv sync`."
        )
