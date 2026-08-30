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

SEPARATOR = "\x1e"

# GitHub rejects a release body over 125,000 characters. The first release of
# this repo came to 98,720 with reasons included, so the headroom is real but
# not large, and a release that FAILS because its notes grew is a bad way to
# find out. Over the budget, the reasons are dropped and the subjects kept:
# every change stays announced, which is the part that must not be lost.
BODY_LIMIT = 120_000


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
            parsed.append(
                {
                    "type": "other",
                    "scope": "",
                    "subject": subject,
                    "body": body,
                    "commit": commit,
                    "breaking": "",
                }
            )
            continue

        parsed.append(
            {
                "type": match["type"],
                "scope": match["scope"] or "",
                "subject": match["subject"],
                "body": body,
                "commit": commit,
                "breaking": match["break"] or "",
            }
        )

    return parsed


def lead(body: str) -> str:
    """The body's first paragraph, unwrapped onto one line."""
    if not body:
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

    return trimmed.rstrip("\n") + note


def _render(
    entries: list[dict[str, str]], version: str | None, repo: str, *, reasons: bool
) -> str:
    out: list[str] = []
    if version:
        out.append(f"## {version}\n")

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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", default=None, help="default: the last tag")
    parser.add_argument("--version", default=None, help="heading for the notes")
    parser.add_argument(
        "--repo",
        default="https://github.com/nhobin219/litelink",
        help="for commit links",
    )
    args = parser.parse_args()

    since = args.since if args.since is not None else last_tag()
    entries = commits(since)
    if not entries:
        print(f"no commits since {since}", file=sys.stderr)  # noqa: T201

        return 1

    print(render(entries, args.version, args.repo.rstrip("/")), end="")  # noqa: T201

    return 0


if __name__ == "__main__":
    sys.exit(main())
