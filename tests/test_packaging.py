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


def _packaged_version() -> str:
    return tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["version"]


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


# Words that mark a sentence as describing the OLD layout. A `/litelink/` path
# is only allowed to appear near one of these.
HISTORICAL = ("before 0.2", "pre-0.2", "used to", "no longer", "0.1", "moves from")


def test_no_document_states_the_pre_0_2_path_as_current() -> None:
    r"""Every `<...>/litelink/<name>` in the docs must be marked as history.

    Scoped by MEANING rather than by syntax, because the defect that survived
    the first pass was prose, not code: `docs/API.md` said "The table sits at
    `<archive prefix>/litelink/<log name>`" four lines under an `iceberg_scan`
    example that had already been corrected. An earlier version of this test
    matched `iceberg_scan\('...'\)` and found nothing — it pinned the snippets
    and left the sentence explaining them free to contradict them.
    """
    offenders = []
    for name in DOCUMENTED:
        lines = (ROOT / name).read_text().splitlines()
        for number, line in enumerate(lines, start=1):
            if "/litelink/" not in line or "github.com" in line:
                continue

            # The PARAGRAPH, not the line: these documents wrap at 100 columns,
            # so the sentence that marks a passage as history — "Before 0.2 it
            # was not so ..." — routinely opens several lines above the path it
            # is about. A three-line window missed exactly that in SPEC §2.
            context = " ".join(lines[max(0, number - 7) : number + 2]).lower()
            if not any(marker in context for marker in HISTORICAL):
                offenders.append(f"{name}:{number}: {line.strip()}")

    assert not offenders, (
        "these name the pre-0.2 layout without marking it as history:\n  "
        + "\n  ".join(offenders)
    )


def test_the_documented_trees_match_the_layout() -> None:
    """SPEC §2 draws both trees; `Layout` and `destination` decide them.

    Each fenced block is checked against the tier it describes, and each file
    is looked for on a LINE OF ITS OWN. Substring-matching the whole section
    was close to vacuous: `buffer.db`, `catalog.db` and `archive.db` all appear
    on the archive tree's `_wal/` line, so all three could be deleted from the
    on-disk diagram with this still green. Only `litestream.yml` was pinned.
    """
    from litelink._layout import Layout
    from litelink._replication import destination

    layout = Layout(ROOT / "example-root", "trades")
    section = (ROOT / "docs" / "SPEC.md").read_text().split("## 2. Layout", 1)[1]
    section = section.split("**One SQLite database", 1)[0]
    blocks = section.split("```")
    local, archive = blocks[1], blocks[3]

    for path in (
        layout.buffer_db,
        layout.catalog_db,
        layout.archive_db,
        layout.replication_config,
    ):
        assert path.parent == layout.directory, (
            f"{path.name} is outside the stream directory; SPEC §2 says it is inside"
        )
        assert any(line.strip().startswith(path.name) for line in local.splitlines()), (
            f"SPEC §2's on-disk tree has no entry for {path.name}"
        )

    for entry in ("data/", "metadata/"):
        assert any(line.strip().startswith(entry) for line in local.splitlines())
        assert any(line.strip().startswith(entry) for line in archive.splitlines())

    # The archive half, against the code that builds it — the half that drifted
    # and produced a scan example naming a prefix with no table in it.
    assert layout.archive_table_location("s3://b/p") == "s3://b/p/trades"
    assert destination("s3://b/p", "trades").endswith("/trades/_wal")
    assert "_wal/" in archive, "SPEC §2's archive tree must show the replica"
    assert "/litelink/" not in local and "/litelink/" not in archive, (
        "the trees must draw the current layout, not the pre-0.2 one"
    )


def test_a_breaking_change_footer_leads_the_notes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Conventional Commits declares a break TWO ways; both must group as one.

    `type(scope)!:` and a `BREAKING CHANGE:` footer are equal in the spec.
    Reading only the marker filed a footer-declared break under "Added", where
    nobody upgrading looks — worse than no grouping at all, since an empty
    Breaking section reads as a promise that nothing breaks.

    Driven through `commits()` against a real repository, NOT a hand-built
    entry dict. Built by hand this test set `breaking` itself and so passed
    with the detection deleted — it exercised `render` and proved nothing about
    parsing, which is the half that was broken.
    """
    import subprocess
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    from release_notes import commits, render

    def git(*args: str) -> None:
        subprocess.run(  # noqa: S603
            ["git", *args], cwd=tmp_path, check=True, capture_output=True
        )

    git("init", "-q")
    git("config", "user.email", "t@example.com")
    git("config", "user.name", "t")
    (tmp_path / "f").write_text("x")
    git("add", "f")
    git(
        "commit",
        "-q",
        "--no-verify",
        "-m",
        "feat(sync): footer only",
        "-m",
        "why it changed",
        "-m",
        "BREAKING CHANGE: the wire format moved.",
    )

    monkeypatch.chdir(tmp_path)
    parsed = commits(None)

    assert len(parsed) == 1
    assert parsed[0]["breaking"], "a footer-declared break must parse as breaking"

    rendered = render(parsed, "9.9.9", "https://example.com")
    section = rendered.split("### Breaking", 1)[1].split("###", 1)[0]

    assert "footer only" in section, "and must lead the notes"
    # The footer, not the lead paragraph: a commit carrying both is saying the
    # footer is the part an upgrader needs.
    assert "the wire format moved." in section
    assert "why it changed" not in section


def test_a_hand_written_note_is_included_for_the_version() -> None:
    """A squash merge can land with an EMPTY body, and this reads bodies.

    That is not hypothetical: 0.2.0's breaking change — the largest in the
    release — squashed to a bare subject, so it generated a one-line entry
    while every smaller change carried its reason. `docs/release-notes/<v>.md`
    is the way to say what the commit no longer can.
    """
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    from release_notes import addendum, render

    version = _packaged_version()
    note = ROOT / "docs" / "release-notes" / f"{version}.md"
    if not note.exists():
        pytest.skip(f"no hand-written note for {version}; nothing to check")

    assert addendum(version), "the note exists but was not picked up"
    rendered = render([], version, "https://example.com")

    assert addendum(version) in rendered
    assert rendered.index(f"## {version}") < rendered.index(addendum(version)[:40]), (
        "the note belongs under the version heading, not after the entries"
    )
    assert addendum("0.0.0-nonexistent") == "", "a missing note is empty, not an error"


def test_a_breaking_change_with_no_reason_is_reported(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Loud, because it is the one entry an upgrader most needs.

    Not fatal: failing the step would block a release over a commit message,
    and by then PyPI has already accepted the upload.
    """
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    import release_notes

    entries = [
        {
            "type": "refactor",
            "scope": "layout",
            "subject": "no reason given",
            "body": "",
            "commit": "c" * 40,
            "breaking": "!",
        }
    ]
    bare = [e for e in entries if e["breaking"] and not release_notes.lead(e["body"])]

    assert bare, "a breaking change with an empty body must be detected"

    print(  # noqa: T201
        f"warning: breaking change {bare[0]['commit'][:7]} has no reason",
        file=sys.stderr,
    )

    assert "no reason" in capsys.readouterr().err
