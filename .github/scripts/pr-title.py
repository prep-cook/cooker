#!/usr/bin/env python3
"""Check a pull request title, which becomes the squash commit's subject (CONTRIBUTING.md).

    PR_TITLE='fix(ui): keep Overview tiles whole' pr-title.py
    pr-title.py 'fix(ui): keep Overview tiles whole'

The title is `type(scope): subject`, where type is a change category, scope is one of the
SCOPES below, and subject is an imperative phrase starting in lower case (a first word such as
`OAuth` or `TLS`, with a capital after its first letter, may keep its case) without a full stop.
The whole title fits in MAX_LENGTH characters, leaving room for the ` (#123)` GitHub appends.
"""
import os
import re
import sys

TYPES = {"feat", "fix", "refactor", "test", "style", "chore", "docs", "deps", "ci", "release"}
SCOPES = {
    "cli": "the cooker binary: command line, TUI, discovery (crates/cooker)",
    "server": "HTTP routes, client auth, credential selection, management API (crates/ck-server)",
    "exec": "upstream provider executors (crates/ck-exec)",
    "stream": "shared mmap stream storage and publication (crates/ck-stream)",
    "translate": "request and response translation (crates/ck-translate)",
    "core": "config, credentials, formats, shared helpers (crates/ck-core, crates/ck-wire)",
    "ui": "the dashboard (ui/)",
    "docker": "the Dockerfile and image",
    "ci": "workflows and CI scripts (.github/)",
    "deps": "dependency updates",
    "docs": "README, docs/ and other documentation",
    "bench": "benchmarks, gates and the harness (bench/, harness/)",
    "tools": "repository tools (tools/)",
    "release": "release preparation: version, changelog",
}
MAX_LENGTH = 72
TITLE = re.compile(r"(?P<type>[a-z]+)\((?P<scope>[a-z]+)\): (?P<subject>\S.*)")


def problems(title):
    """Every rule the title breaks, as sentences; empty when it is valid."""
    found = []
    if title != title.strip():
        found.append("Remove the spaces around the title.")
    title = title.strip()
    if len(title) > MAX_LENGTH:
        found.append(f"Shorten the title to {MAX_LENGTH} characters; it has {len(title)}.")
    match = TITLE.fullmatch(title)
    if not match:
        found.append("Start the title with a type and scope, such as `fix(ui): keep Overview tiles whole`.")
        return found
    if match["type"] not in TYPES:
        found.append(f"Unknown type {match['type']}; use one of: {', '.join(sorted(TYPES))}.")
    if match["scope"] not in SCOPES:
        found.append(f"Unknown scope {match['scope']}; use one of: {', '.join(sorted(SCOPES))}.")
    subject = match["subject"]
    word = subject.split()[0]
    if word[0].isupper() and not any(c.isupper() for c in word[1:]):
        found.append(f"Start the subject in lower case: {word[0].lower()}{word[1:]}.")
    if subject.endswith("."):
        found.append("Drop the full stop at the end.")
    if re.match(r"wip\b|fixup!|squash!", subject, re.IGNORECASE):
        found.append("Finish the change before the title says it is done; drop WIP, fixup! or squash!.")
    return found


def main(argv):
    title = argv[1] if len(argv) > 1 else os.environ.get("PR_TITLE", "")
    found = problems(title)
    for problem in found:
        print(f"::error title=Pull request title::{problem}")
    if found:
        print(f"Title: {title!r}. Rules: CONTRIBUTING.md, Commits and pull request titles.")
        return 1
    print(f"Title OK: {title}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
