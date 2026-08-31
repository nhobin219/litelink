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


def _parse(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


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

    # A CEILING, not just a floor. Pinning the vendored version to the floor is
    # the wrong end of the range: an uncapped `duckdb>=1.5.5` lets a resolver
    # install 1.5.6 the day it ships, and the bundle — built for 1.5.5 — is
    # then skipped by design. The wheel is already published and immutable, and
    # the failure is silent, so the package just stops working offline.
    assert "<" in declared, (
        f"{declared!r} has no upper bound. The wheel bundles extensions built "
        f"for duckdb {DUCKDB_VERSION} exactly; without a ceiling the next "
        f"duckdb release makes every published wheel useless offline."
    )

    floor = declared.split(">=")[1].split(",")[0].strip()
    ceiling = declared.split("<")[1].strip().strip('"')

    assert _parse(floor) <= _parse(DUCKDB_VERSION) < _parse(ceiling), (
        f"the build vendors extensions for duckdb {DUCKDB_VERSION}, which is "
        f"outside the declared range {declared!r}"
    )

    # And the ceiling has to be the next PATCH, because that is where the
    # coupling actually is: the bundle is looked up under
    # `.bin/<duckdb.__version__>/`, an exact string, and DuckDB refuses a
    # cross-version extension. A first attempt capped at the next MINOR, which
    # admits 1.5.6 — already on PyPI as `1.5.6.devN` — so every wheel would
    # have gone dead the day it went final, with no way to amend it.
    major, minor, patch = _parse(DUCKDB_VERSION)

    assert _parse(ceiling) == (major, minor, patch + 1), (
        f"{declared!r} admits duckdb versions the bundle cannot serve. The "
        f"extensions are built for {DUCKDB_VERSION} exactly, so the ceiling "
        f"must be {major}.{minor}.{patch + 1}, not {ceiling}."
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


def test_the_required_check_depends_on_every_job() -> None:
    """`ci-success` is the one required check, so a job missing from it is invisible.

    This branch found three defects of the shape "provisioned nothing, said
    nothing, went green", and then found a fourth in the gate meant to catch
    them: `packaging` — the only job that verifies the wheel works on a cold
    box, which is this branch's entire point — was not in `needs`, so it could
    fail red on a PR while the required check reported success.

    The comment above that gate claimed a job added later was covered
    automatically. `needs.*` expands only to the jobs named, and the job right
    below the comment disproved it.

    Falsify by removing any job from `needs`.
    """
    # NOT `importorskip`. This is the guard on the guard, and skipping it if
    # PyYAML goes missing is the exact shape it exists to catch — the same one
    # that let `importorskip("s3fs")` hide 91 tests. An absence here has to be
    # loud.
    try:
        import yaml
    except ImportError:  # pragma: no cover - the failure this makes loud
        pytest.fail(
            "PyYAML is not installed, so the check that the CI gate covers "
            "every job cannot run. It is a dev dependency. Run `uv sync`."
        )

    workflow = yaml.safe_load((ROOT / ".github" / "workflows" / "ci.yml").read_text())

    jobs = set(workflow["jobs"]) - {"ci-success"}
    gate = set(workflow["jobs"]["ci-success"]["needs"])

    assert jobs == gate, (
        f"these jobs are not gated by the required check: {sorted(jobs - gate)}. "
        f"A job outside `needs` can fail while the merge gate passes."
    )

    # And a needed job that never RAN is not a job that passed.
    condition = workflow["jobs"]["ci-success"]["steps"][0]["if"]
    for outcome in ("failure", "cancelled", "skipped"):
        assert outcome in condition, f"the gate ignores a {outcome} job"


def test_release_notes_group_and_carry_the_reason() -> None:
    """The notes exist to preserve WHY, which is what PR-title notes discard.

    `gh --generate-notes` lists pull request titles. This history puts the
    reason in the commit body, so notes that drop the body throw away the part
    worth reading — and a squashed PR's body is the one thing GitHub does not
    show you.

    Falsify by dropping `lead()` from the bullet: the reason disappears and
    this fails.
    """
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    from release_notes import lead, render

    entries = [
        {
            "type": "feat",
            "scope": "read",
            "subject": "follow a log from another machine",
            "body": "The archive merged with a replicated buffer.\n\nDetail after.",
            "commit": "a" * 40,
            "breaking": "",
        },
        {
            "type": "fix",
            "scope": "",
            "subject": "stop double-counting a cross-process seal",
            "body": "",
            "commit": "b" * 40,
            "breaking": "!",
        },
        {
            "type": "wat",
            "scope": "",
            "subject": "an unrecognised type",
            "body": "",
            "commit": "c" * 40,
            "breaking": "",
        },
    ]
    notes = render(entries, "1.2.3", "https://example.test/r")

    assert "## 1.2.3" in notes
    assert "### Breaking" in notes
    assert notes.index("### Breaking") < notes.index("### Added"), (
        "a breaking change has to lead"
    )

    # The reason travels, and only the lead paragraph of it.
    assert "The archive merged with a replicated buffer." in notes
    assert "Detail after." not in notes

    # An unknown type is SHOWN, not dropped. A change that ships unannounced is
    # worse than an untidy heading.
    assert "an unrecognised type" in notes
    assert "### Other" in notes

    # Commits are linkable, since the evidence lives there.
    assert f"https://example.test/r/commit/{'a' * 40}" in notes

    # Trailers are not a reason.
    assert lead("Co-Authored-By: someone <a@b.c>") == ""


def test_release_notes_survive_a_first_release_and_a_long_history() -> None:
    """Both ways the notes broke on the first release they were used for.

    **`last_tag` returned the tag being released.** `git describe --tags` from
    HEAD names the tag AT HEAD on a release run, so the range was
    `v0.1.0..HEAD` — empty — and the step exited 1 with "no commits since
    v0.1.0" *after PyPI had already accepted the upload*. It describes from
    `HEAD^` now, so a first release means the full history.

    **And the full history nearly did not fit.** GitHub rejects a release body
    over 125,000 characters; this repo's first release came to 98,720 with
    reasons included. Rather than fail, it drops the reasons and keeps every
    subject, because an unannounced change is the thing that must not happen.

    Falsify by describing from HEAD, or by removing the limit branch.
    """
    import subprocess
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    from release_notes import last_tag, render

    tags = subprocess.run(  # noqa: S603
        ["git", "tag"], capture_output=True, check=True, text=True, cwd=ROOT
    ).stdout.split()
    head = subprocess.run(  # noqa: S603
        ["git", "tag", "--points-at", "HEAD"],
        capture_output=True,
        check=True,
        text=True,
        cwd=ROOT,
    ).stdout.split()

    if tags:
        assert last_tag() not in head, (
            "last_tag() returned a tag pointing at HEAD, so a release run would "
            "diff the tag against itself and find nothing"
        )

    entries = [
        {
            "type": "fix",
            "scope": "",
            "subject": f"change number {n}",
            "body": "A reason long enough to matter. " * 12,
            "commit": f"{n:040d}",
            "breaking": "",
        }
        for n in range(60)
    ]

    full = render(entries, "9.9.9", "https://x.test")
    capped = render(entries, "9.9.9", "https://x.test", limit=2_000)

    assert len(full) > len(capped)
    assert "Reasons omitted" in capped
    assert capped.count("\n- ") == 60, "every change must still be listed"
    assert "A reason long enough" not in capped


# Every place a reader is told where a log's files are. Kept as one list
# because the failure these guard against is documentation drifting from
# `Layout` — which is not hypothetical: the 0.2 layout change left an
# `iceberg_scan` example in three files pointing at a prefix that no longer
# holds a table, and each one was a command someone would copy and run.
DOCUMENTED = (
    "README.md",
    "docs/SPEC.md",
    "docs/API.md",
    "docs/RUNTIME.md",
    "examples/README.md",
)


def test_no_documented_scan_points_at_the_pre_0_2_archive_path() -> None:
    """`iceberg_scan` examples are copy-pasteable, so they have to be right.

    The archive's table location is `<prefix>/<name>`. It was
    `<prefix>/litelink/<name>` before 0.2 — pyiceberg's
    `<warehouse>/<namespace>/<table>` default — and an engine pointed there now
    finds no table at all.
    """
    import re

    offenders = []
    for name in DOCUMENTED:
        text = (ROOT / name).read_text()
        for match in re.finditer(r"iceberg_scan\('([^']+)'", text):
            if "/litelink/" in match.group(1):
                offenders.append(f"{name}: {match.group(1)}")

    assert not offenders, (
        "these scan examples name the pre-0.2 archive path:\n  "
        + "\n  ".join(offenders)
    )


def test_the_documented_tree_matches_the_layout() -> None:
    """SPEC §2 draws the on-disk tree; `Layout` decides it.

    Asserted against the real object rather than a second copy of the list, so
    moving a file without redrawing the tree fails here instead of in a user's
    directory.
    """
    from litelink._layout import Layout

    layout = Layout(ROOT / "example-root", "trades")
    drawn = (ROOT / "docs" / "SPEC.md").read_text()
    block = drawn.split("## 2. Layout", 1)[1].split("**One SQLite database", 1)[0]

    for path in (
        layout.buffer_db,
        layout.catalog_db,
        layout.archive_db,
        layout.replication_config,
    ):
        assert path.parent == layout.directory, (
            f"{path.name} is outside the stream directory; SPEC §2 says it is inside"
        )
        assert path.name in block, f"SPEC §2's tree does not mention {path.name}"

    assert "data/" in block and "metadata/" in block
    assert f"{layout.name}/data/" in layout.seal_path(1, 2, "tok")
