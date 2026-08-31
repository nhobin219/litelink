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


def _repo(tmp_path: Path) -> Path:
    """A real git repository, because these functions shell out to git.

    Driven through the actual entry points rather than reimplemented: two of
    the tests below used to build their inputs by hand and so passed with the
    production code deleted.
    """
    import subprocess

    def git(*args: str) -> str:
        return subprocess.run(  # noqa: S603
            ["git", *args], cwd=tmp_path, check=True, capture_output=True, text=True
        ).stdout

    git("init", "-q")
    git("config", "user.email", "t@example.com")
    git("config", "user.name", "t")

    return tmp_path


def _commit(path: Path, subject: str, *body: str) -> None:
    import subprocess

    (path / "f").write_text(subject)
    messages: list[str] = []
    for chunk in (subject, *body):
        messages += ["-m", chunk]

    subprocess.run(  # noqa: S603
        ["git", "add", "f"], cwd=path, check=True, capture_output=True
    )
    subprocess.run(  # noqa: S603
        ["git", "commit", "-q", "--no-verify", *messages],
        cwd=path,
        check=True,
        capture_output=True,
    )


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


def test_notes_over_the_body_limit_drop_reasons_and_keep_every_subject() -> None:
    """GitHub rejects a release body over 125,000 characters.

    This repo's first release came to 98,720 with reasons included, so the
    headroom is real but not large. Over the budget the reasons go and every
    subject stays, because an unannounced change is the thing that must not
    happen — and the note says why they are missing.
    """
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    from release_notes import render

    entries = [
        {
            "type": "fix",
            "scope": "",
            "subject": f"change number {n}",
            "body": "x" * 400,
            "commit": f"{n:040d}",
            "breaking": "",
        }
        for n in range(60)
    ]
    full = render(entries, "9.9.9", "https://x.test", limit=10**9)
    capped = render(entries, "9.9.9", "https://x.test", limit=8_000)

    assert len(full) > len(capped)
    assert "Reasons omitted" in capped
    for n in range(60):
        assert f"change number {n}" in capped, "no change may go unannounced"


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


def test_a_breaking_change_with_no_reason_is_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Loud, because it is the one entry an upgrader most needs.

    Through `main()`. This test used to rebuild the filter and run its own
    `print`, asserting on output it had produced itself — so replacing the
    whole of `main()` with `return 0` left it green. It covered `lead("")` and
    nothing else.

    Not fatal: failing the step would block a release over a commit message,
    and by then PyPI has accepted the upload.
    """
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    import release_notes

    repo = _repo(tmp_path)
    _commit(repo, "refactor(layout)!: no reason given")
    monkeypatch.chdir(repo)

    assert release_notes.main(["--version", "9.9.9"]) == 0

    captured = capsys.readouterr()

    assert "warning" in captured.err
    assert "no reason" in captured.err
    assert "no reason given" in captured.err, "the warning must name the commit"
    assert "### Breaking" in captured.out, "and the entry is still rendered"


def test_a_reason_present_produces_no_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The control. A warning that always fires says nothing."""
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    import release_notes

    repo = _repo(tmp_path)
    _commit(repo, "refactor(layout)!: with a reason", "because of this.")
    monkeypatch.chdir(repo)
    release_notes.main(["--version", "9.9.9"])

    assert "warning" not in capsys.readouterr().err


def test_a_hand_written_note_is_included_under_the_heading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A squash merge can land with an EMPTY body, and this reads bodies.

    Not hypothetical: 0.2.0's breaking change squashed to a bare subject, so
    the largest change in the release rendered as a one-line entry while every
    smaller one carried its reason.

    Against a redirected notes directory, and with entries to be ordered
    against. Keyed to the packaged version it skipped itself whenever no note
    existed — which is every release nobody writes one for — and rendering with
    an empty entry list made the "under the heading, not after the entries"
    assertion unfalsifiable.
    """
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    import release_notes

    notes = tmp_path / "release-notes"
    notes.mkdir()
    (notes / "9.9.9.md").write_text("Hand written — with an em-dash.", encoding="utf-8")
    monkeypatch.setattr(release_notes, "NOTES_DIR", notes)

    assert release_notes.addendum("9.9.9") == "Hand written — with an em-dash."
    assert release_notes.addendum("0.0.0") == "", (
        "a missing note is empty, not an error"
    )

    entry = {
        "type": "feat",
        "scope": "read",
        "subject": "an entry to be ordered against",
        "body": "why.",
        "commit": "a" * 40,
        "breaking": "",
    }
    rendered = release_notes.render([entry], "9.9.9", "https://example.com")

    assert "Hand written" in rendered
    assert rendered.index("## 9.9.9") < rendered.index("Hand written")
    assert rendered.index("Hand written") < rendered.index("an entry to be ordered"), (
        "the note belongs under the heading, above the entries"
    )


def test_the_packaged_version_note_is_picked_up_if_present() -> None:
    """And the real one, without skipping when it is absent."""
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    from release_notes import NOTES_DIR, addendum

    version = _packaged_version()
    note = NOTES_DIR / f"{version}.md"

    assert bool(addendum(version)) == note.exists(), (
        f"a note for {version} exists but was not picked up, or vice versa"
    )


def test_last_tag_ignores_the_tag_being_released(tmp_path: Path, monkeypatch) -> None:
    """The bug that exited 1 AFTER PyPI accepted the upload.

    `git describe --tags` from HEAD names the tag at HEAD on a release run, so
    the range was `v0.1.0..HEAD` — empty. It describes from `HEAD^` now.

    In a repository where HEAD really is tagged. The old test asked the
    CHECKOUT, where `git tag --points-at HEAD` is empty on every ordinary
    commit, so its assertion was `X not in []` — it passed with `HEAD^` changed
    back to `HEAD`, and with `last_tag` replaced by a constant. CI never runs
    on a tagged commit, so it never executed at all.
    """
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    import release_notes

    repo = _repo(tmp_path)
    _commit(repo, "feat(read): first")
    import subprocess

    subprocess.run(  # noqa: S603
        ["git", "tag", "v0.1.0"], cwd=repo, check=True, capture_output=True
    )
    _commit(repo, "feat(read): second")
    subprocess.run(  # noqa: S603
        ["git", "tag", "v0.2.0"], cwd=repo, check=True, capture_output=True
    )
    monkeypatch.chdir(repo)

    assert release_notes.last_tag() == "v0.1.0", (
        "on a release run HEAD carries the tag being published; describing from "
        "HEAD returns it and the commit range comes out empty"
    )
    assert len(release_notes.commits(release_notes.last_tag())) == 1


def test_a_first_release_describes_the_whole_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No previous tag means every commit, not a crash."""
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    import release_notes

    repo = _repo(tmp_path)
    _commit(repo, "feat(read): first")
    _commit(repo, "fix(read): second")
    monkeypatch.chdir(repo)

    assert release_notes.last_tag() is None
    assert len(release_notes.commits(None)) == 2


def test_a_quoted_breaking_change_is_not_a_breaking_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A footer is a footer; prose and code fences that mention it are not.

    Searching the whole body with `re.MULTILINE` matched any line beginning
    with the phrase — inside a fence, or in a sentence explaining the
    convention — and announced ordinary commits as breaking with reasons lifted
    out of the quoted text. The commit that introduced that regex contains the
    phrase in its own body and escaped by one leading backtick.
    """
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    from release_notes import breaking_reason, commits, render

    fenced = "we refuse it.\n\n```\nBREAKING CHANGE: quoted\n```\n\nnot what happened."
    prose = "a footer looks like:\nBREAKING CHANGE: the key moved\nand that is how."

    assert breaking_reason(fenced) is None
    assert breaking_reason(prose) is None

    repo = _repo(tmp_path)
    _commit(repo, "fix(config): refuse a bad value", fenced)
    monkeypatch.chdir(repo)
    rendered = render(commits(None), "9.9.9", "https://example.com")

    assert "### Breaking" not in rendered
    assert "### Fixed" in rendered


def test_a_wrapped_breaking_footer_keeps_all_of_its_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bodies wrap at 88 columns, so a one-line capture truncates every footer.

    It cut mid-clause, dropping the half that tells an upgrader what to DO —
    the worst possible entry to truncate, since it is the only line they read.
    """
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    from release_notes import commits, render

    footer = (
        "BREAKING CHANGE: `follow()` no longer takes a `root` parameter;\n"
        "every stream owns its directory and the caller passes the path."
    )
    repo = _repo(tmp_path)
    _commit(repo, "refactor(read): move the follower", "why it changed", footer)
    monkeypatch.chdir(repo)
    rendered = render(commits(None), "9.9.9", "https://example.com")

    assert "### Breaking" in rendered
    assert "the caller passes the path." in rendered, "the tail must survive"
    assert "why it changed" not in rendered, "the footer wins over the paragraph"


def test_a_footer_whose_value_wraps_to_the_next_line_is_found(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Through real git, because git's own cleanup is what breaks this.

    `--cleanup=whitespace` is the default and strips trailing whitespace from
    every line, so a writer who types `BREAKING CHANGE: ` and wraps the value
    onto the next line has the footer stored as exactly `BREAKING CHANGE:`.
    Requiring a space after the colon dropped the break — silently, and this is
    the ordinary way to write a long one.
    """
    import subprocess
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    from release_notes import commits, render

    repo = _repo(tmp_path)
    _commit(
        repo,
        "refactor(read): move the follower",
        "why it changed",
        "BREAKING CHANGE:\nthe root parameter is gone; pass the stream path.",
    )
    stored = subprocess.run(  # noqa: S603
        ["git", "log", "-1", "--format=%b"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout

    assert "BREAKING CHANGE:\n" in stored, "git must have stripped the trailing space"

    monkeypatch.chdir(repo)
    rendered = render(commits(None), "9.9.9", "https://example.com")

    assert "### Breaking" in rendered
    assert "pass the stream path." in rendered


def test_a_break_declared_outside_a_footer_is_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The loss the strict parse cannot avoid, made loud instead.

    A footer value may span a blank line, but a paragraph that merely quotes
    the phrase is structurally identical — admitting one readmits the other,
    and the fabricating version has already shipped once. So the parse stays
    strict and a break that was meant but not picked up says so, naming the
    commit, rather than vanishing from the notes.
    """
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    import release_notes

    repo = _repo(tmp_path)
    _commit(
        repo,
        "refactor(layout)!: move every log",
        "BREAKING CHANGE: every 0.1 log must be migrated.",
        "Run `python -m litelink.migrate --apply` first.",
    )
    monkeypatch.chdir(repo)
    release_notes.main(["--version", "9.9.9"])
    err = capsys.readouterr().err

    assert "outside a footer" in err
    assert "move every log" in err, "the warning must name the commit"
    assert "docs/release-notes" in err, "and point at the way to fix it"


def test_fence_stripping_is_load_bearing_not_decorative() -> None:
    """The shape where removing fences actually changes the answer.

    A fenced quote alone is already rejected by the block walk — a block
    starting with ``` is not token-headed — so a test using only that passes
    whether or not fences are stripped, and proves nothing about them. This is
    the case that does depend on it: a trailing block that IS token-headed
    (`Note:`) and carries a fenced example under it. Without stripping, the walk
    accepts the block, skips the fence line, and captures the quoted text as a
    real break.
    """
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    from release_notes import breaking_reason

    body = "why\n\nNote: see the example\n```\nBREAKING CHANGE: fabricated\n```"

    assert breaking_reason(body) is None, (
        "a fenced example under a real footer must not become a breaking change"
    )


def test_a_fenced_quote_produces_no_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The control. A warning that fires on every docs commit would be ignored."""
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    import release_notes

    repo = _repo(tmp_path)
    _commit(
        repo,
        "docs(ci): explain how a break is declared",
        "like this:\n\n```\nBREAKING CHANGE: the key moved\n```\n\nthat is all.",
    )
    monkeypatch.chdir(repo)
    release_notes.main(["--version", "9.9.9"])

    assert "outside a footer" not in capsys.readouterr().err


def test_an_unterminated_fence_does_not_swallow_a_footer() -> None:
    """Stripping to end-of-body would take the footer under it with it.

    The asymmetry decides this: a spurious Breaking entry is visible in the
    notes, a lost one is silent.
    """
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    from release_notes import breaking_reason

    body = "why\n\n```\nsome code\n\nBREAKING CHANGE: the real one."

    assert breaking_reason(body) == "the real one."


def test_a_footer_with_no_value_is_breaking_with_no_reason() -> None:
    """Declared and empty is not the same as absent.

    It still leads the notes — the break was declared — and the missing reason
    is what the other warning is for.
    """
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    from release_notes import breaking_reason, lead

    body = "why\n\nBREAKING CHANGE:\n\nCo-Authored-By: x <y>"

    assert breaking_reason(body) == ""
    assert breaking_reason("no footer here") is None
    assert lead(body) == "", "empty, and the bare-reason warning catches it"


def test_an_oversized_hand_written_note_is_still_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The last resort, after dropping reasons has already failed.

    Reasons are trimmable; a hand-written note is not. So the note alone can
    carry the body past the limit that the fallback exists to respect — and the
    failure lands on `gh release create`, AFTER PyPI has accepted the upload,
    which is exactly the ordering that burned this project once already.

    Uncovered until now: deleting the branch left the whole suite green.
    """
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    import release_notes

    notes = tmp_path / "release-notes"
    notes.mkdir()
    (notes / "9.9.9.md").write_text("n" * 200_000, encoding="utf-8")
    monkeypatch.setattr(release_notes, "NOTES_DIR", notes)

    entries = [
        {
            "type": "fix",
            "scope": "",
            "subject": f"change {n}",
            "body": "x" * 400,
            "commit": f"{n:040d}",
            "breaking": "",
        }
        for n in range(40)
    ]
    rendered = release_notes.render(entries, "9.9.9", "https://x.test", limit=20_000)

    assert len(rendered) <= 20_000, "an untrimmable note must not defeat the limit"
    assert rendered.endswith("_Notes truncated._\n")


def test_a_squash_subject_with_an_empty_footer_still_leads_the_notes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The intersection of the two cases this file handles separately.

    A subject GitHub composed for a squash does not parse as Conventional
    Commits, so the break can only come from the footer — and a footer whose
    value wrapped away leaves an EMPTY value, which is falsy but not absent.
    One call site compared truthiness and the other `is not None`, so the
    conforming-subject path filed the break and the squash path dropped it into
    "Other" with no warning at all: `declared_but_unparsed` short-circuits on a
    non-None reason, so nothing said so either.

    The existing empty-footer test only covers the conforming subject, which is
    why the disagreement stayed green.
    """
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    from release_notes import commits, render

    repo = _repo(tmp_path)
    _commit(repo, "One directory per stream, in both tiers (#47)", "BREAKING CHANGE:")
    monkeypatch.chdir(repo)
    parsed = commits(None)

    assert parsed[0]["breaking"], "an empty footer is declared, not absent"

    rendered = render(parsed, "9.9.9", "https://example.com")

    assert "### Breaking" in rendered
    assert "One directory per stream" in rendered.split("### Breaking", 1)[1]
    assert "### Other" not in rendered
