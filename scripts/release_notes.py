"""Release notes from the commit bodies, because that is where the reasons are.

`gh release --generate-notes` lists pull request titles. This history is
written to be read — Conventional Commits with a body explaining *why* — so
generated notes that throw the body away discard the most useful part of it.

    python scripts/release_notes.py                 # since the last tag
    python scripts/release_notes.py --since v0.1.0
    python scripts/release_notes.py --version 0.2.0 > notes.md

**The lead paragraph, not the whole body.** Bodies here run to thirty lines
with measurements and repro output; a release note that inlined them would be
unreadable. The first paragraph is the claim, and the rest is the evidence for
anyone who follows the hash — which is what a commit link is for.

Merge commits are dropped: a squashed PR carries the same subject with none of
the body, so keeping both would print every entry twice.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

# Conventional Commit types, in the order a reader cares about them, with the
# heading each becomes. Types absent from this map are still shown, under
# "Other" — dropping a commit silently is how a change ships unannounced.
SECTIONS = {
    "feat": "Added",
    "fix": "Fixed",
    "perf": "Performance",
    "refactor": "Changed",
    "build": "Packaging",
    "docs": "Documentation",
    "test": "Tests",
}

# `type(scope)!: subject` — `!` marks a break, which leads the notes.
HEADER = re.compile(
    r"^(?P<type>[a-z]+)(?:\((?P<scope>[^)]+)\))?(?P<break>!)?: (?P<subject>.+)$"
)

# The OTHER half of the spec. Conventional Commits declares a break two ways —
# the `!` above, and a `BREAKING CHANGE:` footer — and reading only the first
# files a break under "Added", where nobody upgrading looks. That is worse than
# not grouping at all: the section still renders, so its silence reads as a
# promise that nothing breaks.
#
# A FOOTER, parsed as one, not a regex over the whole body. The spec puts
# footers after a blank line at the end, and searching everything meant any
# line beginning `BREAKING CHANGE:` matched — including one inside a fenced code
# block, or a quoted commit message. Two ordinary commits, a `fix` and a `docs`,
# were announced as breaking with reasons lifted out of a code fence. The
# commit that introduced that regex contains the phrase in its own body and
# escaped only because the line starts with a backtick.
# The separator tolerates a colon at END OF LINE, not only `: `. git's default
# `--cleanup=whitespace` strips trailing whitespace from every line, so a writer
# who puts the value on the next line has their `BREAKING CHANGE: ` stored as
# exactly `BREAKING CHANGE:` — and requiring the space dropped the break
# entirely, silently.
FOOTER_TOKEN = re.compile(
    r"^(?P<token>BREAKING[ -]CHANGE|[A-Za-z][A-Za-z0-9-]*)(?::(?= |$) ?| #)(?P<value>.*)$"
)

SEPARATOR = "\x1e"

# GitHub rejects a release body over 125,000 characters. The first release of
# this repo came to 98,720 with reasons included, so the headroom is real but
# not large, and a release that FAILS because its notes grew is a bad way to
# find out. Over the budget, the reasons are dropped and the subjects kept:
# every change stays announced, which is the part that must not be lost.
BODY_LIMIT = 120_000

# Where `addendum` looks. A module constant rather than an expression inside the
# function, so a test can point it somewhere else instead of depending on
# whichever version happens to be packaged — which is how the test covering it
# turned into a skip.
NOTES_DIR = Path(__file__).resolve().parents[1] / "docs" / "release-notes"


def addendum(version: str | None) -> str:
    """A hand-written note for this version, if one is checked in.

    `docs/release-notes/<version>.md`, included right under the heading. It
    exists because a squash merge can land with an EMPTY body — GitHub composes
    the message and whoever merges can clear it — and this script reads bodies.
    That is how 0.2.0's breaking change, the largest in the release, generated a
    bare line while every smaller entry carried its reason.

    Found by convention rather than passed as a flag, so the release workflow
    needs no change and the file is discoverable by anyone cutting a release.
    """
    if version is None:
        return ""

    note = NOTES_DIR / f"{version}.md"
    # Explicit encoding: these notes carry em-dashes, and the `github-release`
    # job sets up no Python, so it inherits whatever locale the runner has.
    return note.read_text(encoding="utf-8").strip() if note.exists() else ""


def _without_fences(body: str) -> str:
    """The body with fenced code removed, so quoted text cannot pose as a footer."""
    kept, inside = [], False
    for line in body.splitlines():
        if line.lstrip().startswith("```"):
            inside = not inside
            continue

        if not inside:
            kept.append(line)

    if inside:
        # Unterminated: that ``` was probably not a fence at all, and stripping
        # to end-of-body would take any footer under it with it. The asymmetry
        # decides this — a spurious Breaking entry is VISIBLE in the notes, a
        # lost one is silent.
        return body

    return "\n".join(kept)


def breaking_reason(body: str) -> str | None:
    """The `BREAKING CHANGE:` footer's value, or None when there is none.

    **Every trailing footer block, not just the last one.** Footers are one
    block in the spec, but a body here ends with `Co-Authored-By:` and
    `Claude-Session:` trailers, and a writer will naturally leave a blank line
    above them — which would put the break in the second-to-last block and hide
    it from a last-block-only reading.

    **The value spans lines.** `.` does not cross newlines and this repo wraps
    bodies at 88 columns, so a single-line capture truncated essentially every
    real footer mid-clause, dropping the half that says what to DO. Parsing
    terminates at the next footer token, as the spec requires.
    """
    blocks = [b for b in _without_fences(body).split("\n\n") if b.strip()]
    footers: list[str] = []
    for block in reversed(blocks):
        first = block.splitlines()[0]
        if not FOOTER_TOKEN.match(first):
            break

        footers.insert(0, block)

    value: list[str] = []
    capturing = False
    for line in "\n".join(footers).splitlines():
        match = FOOTER_TOKEN.match(line)
        if match is not None:
            if capturing:
                break

            if match["token"].upper().replace("-", " ") == "BREAKING CHANGE":
                capturing = True
                value.append(match["value"])

            continue

        if capturing:
            value.append(line)

    if not capturing:
        return None

    return " ".join(" ".join(value).split())


def declared_but_unparsed(body: str) -> bool:
    """A `BREAKING CHANGE` line that the footer parse would not accept.

    The one shape no structural rule can settle. A footer value may span a
    blank line — the spec terminates it at the next footer token, not at a
    paragraph break — but a paragraph that merely QUOTES the phrase is
    structurally identical to a real multi-paragraph footer. Admitting one
    readmits the other, and the fabricating version has already shipped once.

    So the parse stays strict and the LOSS is made loud instead. A break that
    was meant and not picked up prints a warning naming the commit, and whoever
    is cutting the release moves it into the footer block or writes
    `docs/release-notes/<version>.md`, which is what that file is for.
    """
    if breaking_reason(body) is not None:
        return False

    return any(
        (match := FOOTER_TOKEN.match(line)) is not None
        and match["token"].upper().replace("-", " ") == "BREAKING CHANGE"
        for line in _without_fences(body).splitlines()
    )


def _git(*args: str) -> str:
    return subprocess.run(  # noqa: S603
        ["git", *args], capture_output=True, check=True, text=True
    ).stdout.strip()


def last_tag() -> str | None:
    """The most recent tag BEFORE the commit being described.

    From `HEAD^`, not `HEAD`, and that is the whole point: on a release run
    the tag being published points AT HEAD, so describing from HEAD returns it
    and the range `v0.1.0..HEAD` is empty. That is exactly how this failed on
    the first release it was used for — the notes step exited 1 with "no
    commits since v0.1.0" after PyPI had already accepted the upload.

    None on a first release, or on a root commit, which means the full history
    — the right answer for notes that have no predecessor to diff against.
    """
    try:
        return _git("describe", "--tags", "--abbrev=0", "HEAD^")
    except subprocess.CalledProcessError:
        return None


def commits(since: str | None) -> list[dict[str, str]]:
    """Every non-merge commit in the range, parsed."""
    span = f"{since}..HEAD" if since else "HEAD"
    raw = _git("log", span, "--no-merges", f"--format=%H%n%s%n%b{SEPARATOR}")

    parsed = []
    for entry in raw.split(SEPARATOR):
        lines = entry.strip().splitlines()
        if not lines:
            continue

        commit, subject, body = lines[0], lines[1], "\n".join(lines[2:]).strip()
        match = HEADER.match(subject)
        if match is None:
            # Kept, not dropped. A subject that does not parse is a commit
            # someone still has to know about, and an unannounced change is a
            # worse outcome than an ugly line in the notes.
            # The footer is still read. A subject GitHub composed for a squash
            # does not parse, and that is EXACTLY the case where the body is the
            # only place a break can be declared — the same empty-squash-body
            # situation `addendum` exists for. Filing it under "Other" would put
            # a break where nobody upgrading looks.
            parsed.append(
                {
                    "type": "other",
                    "scope": "",
                    "subject": subject,
                    "body": body,
                    "commit": commit,
                    "breaking": "!" if breaking_reason(body) is not None else "",
                }
            )
            continue

        footer = breaking_reason(body) is not None
        parsed.append(
            {
                "type": match["type"],
                "scope": match["scope"] or "",
                "subject": match["subject"],
                "body": body,
                "commit": commit,
                "breaking": match["break"] or ("!" if footer else ""),
            }
        )

    return parsed


def lead(body: str) -> str:
    """The body's reason: a `BREAKING CHANGE:` footer, else the first paragraph.

    The footer wins when there is one, because a commit that carries both is
    saying the footer is the part an upgrader needs — the lead paragraph
    explains the change, the footer explains what it breaks.
    """
    if not body:
        return ""

    footer = breaking_reason(body)
    if footer:
        return footer

    if footer is not None:
        # Declared with nothing after the colon. Empty, not absent — the entry
        # still leads the notes because the break WAS declared, and the missing
        # reason is what the bare-reason warning is for.
        return ""

    paragraph = body.split("\n\n", 1)[0]
    if paragraph.startswith(("Co-Authored-By:", "Claude-Session:")):
        return ""

    return " ".join(paragraph.split())


def render(
    entries: list[dict[str, str]],
    version: str | None,
    repo: str,
    *,
    limit: int = BODY_LIMIT,
) -> str:
    """Markdown, grouped by type, breaks first — and inside the body limit."""
    full = _render(entries, version, repo, reasons=True)
    if len(full) <= limit:
        return full

    trimmed = _render(entries, version, repo, reasons=False)
    note = (
        f"\n\n_Reasons omitted: the full notes ran to {len(full):,} characters, "
        f"over GitHub's release body limit. Follow any commit link for the why._\n"
    )
    capped = trimmed.rstrip("\n") + note
    if len(capped) <= limit:
        return capped

    # Last resort, after dropping the reasons has already failed. The
    # hand-written note is not trimmable the way reasons are, so on its own it
    # can carry the output past the limit the fallback exists to respect — and
    # the failure lands on `gh release create`, after PyPI has accepted the
    # upload. Ordered after the note so the ordinary over-limit release still
    # explains itself.
    return capped[: limit - 200].rstrip() + "\n\n_Notes truncated._\n"


def _render(
    entries: list[dict[str, str]], version: str | None, repo: str, *, reasons: bool
) -> str:
    out: list[str] = []
    if version:
        out.append(f"## {version}\n")

    note = addendum(version)
    if note:
        out.append(note + "\n")

    breaking = [e for e in entries if e["breaking"]]
    if breaking:
        out.append("### Breaking\n")
        for entry in breaking:
            out.append(_bullet(entry, repo, reasons=reasons))

        out.append("")

    for kind, heading in SECTIONS.items():
        chosen = [e for e in entries if e["type"] == kind and not e["breaking"]]
        if not chosen:
            continue

        out.append(f"### {heading}\n")
        for entry in chosen:
            out.append(_bullet(entry, repo, reasons=reasons))

        out.append("")

    rest = [e for e in entries if e["type"] not in SECTIONS and not e["breaking"]]
    if rest:
        out.append("### Other\n")
        for entry in rest:
            out.append(_bullet(entry, repo, reasons=reasons))

        out.append("")

    return "\n".join(out).strip() + "\n"


def _bullet(entry: dict[str, str], repo: str, *, reasons: bool = True) -> str:
    scope = f"**{entry['scope']}**: " if entry["scope"] else ""
    short = entry["commit"][:7]
    link = f"([`{short}`]({repo}/commit/{entry['commit']}))"
    line = f"- {scope}{entry['subject']} {link}"
    reason = lead(entry["body"]) if reasons else ""

    return f"{line}\n  {reason}" if reason else line


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", default=None, help="default: the last tag")
    parser.add_argument("--version", default=None, help="heading for the notes")
    parser.add_argument(
        "--repo",
        default="https://github.com/nhobin219/litelink",
        help="for commit links",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    since = args.since if args.since is not None else last_tag()
    entries = commits(since)

    # Loud, not fatal. A break with no reason is the one entry an upgrader most
    # needs and the one this script cannot supply — but failing the step would
    # block a release over a commit message, after PyPI has already accepted the
    # upload. See `addendum` for the fix when it happens.
    bare = [e for e in entries if e["breaking"] and not lead(e["body"])]
    for entry in bare:
        print(  # noqa: T201
            f"warning: breaking change {entry['commit'][:7]} "
            f"({entry['subject']}) has no reason in its body",
            file=sys.stderr,
        )

    # And the opposite failure: a break that was MEANT and not picked up. Silent
    # loss is the worse of the two, because the notes then announce no break at
    # all — the exact harm footer support was added to prevent.
    for entry in entries:
        if declared_but_unparsed(entry["body"]):
            print(  # noqa: T201
                f"warning: {entry['commit'][:7]} ({entry['subject']}) mentions "
                f"BREAKING CHANGE outside a footer, so it is NOT in the "
                f"Breaking section. Move it to the footer block, or describe "
                f"it in docs/release-notes/<version>.md",
                file=sys.stderr,
            )

    if not entries:
        print(f"no commits since {since}", file=sys.stderr)  # noqa: T201

        return 1

    print(render(entries, args.version, args.repo.rstrip("/")), end="")  # noqa: T201

    return 0


if __name__ == "__main__":
    sys.exit(main())
