#!/usr/bin/env python3
"""Lane selection for ci.yml: which jobs a change needs.

    ci-lanes.py                       in the changes job: reads the GitHub event, writes
                                      <lane>=true|false and lanes=<JSON list> to
                                      $GITHUB_OUTPUT and a table to the job summary
    ci-lanes.py --diff <base> <head>  locally: the lanes for the commits between two refs
    ci-lanes.py --paths <path>...     locally: the lanes for a list of changed paths

A push to master, a manual run, the call from release.yml, a pull request labelled
`ci:full`, and any change this script cannot read run every lane. Adding a label does not
start a run; re-run the workflow after adding `ci:full`. A re-run keeps the original event
payload, so the labels are read from the API, falling back to the payload's. Otherwise each changed
path selects the lanes listed by the first rule it matches, and a path no rule matches
selects every lane, so a new top-level file or directory runs everything until a rule
says otherwise. A pull request is read as GitHub's test merge commit against its first
parent, the base branch tip: exactly what merging would change.

Lanes and the jobs they run, each building the Dockerfile stage of the same name:
  dashboard  the dashboard's checks and build
  check      fmt, clippy, the Rust size gate, actionlint, THIRD-PARTY-NOTICES, the harness
             build, script tests, the requirements check
  test       the two test shards and the test counts
  perf       the size and idle gates on the release binary, the image smoke test and size
"""
import json
import os
import re
import subprocess
import sys
import urllib.request

LANES = ("dashboard", "check", "test", "perf")
ALL = frozenset(LANES)
NONE = frozenset()
# Everything that compiles the workspace.
RUST = frozenset({"check", "test", "perf"})

# (pattern for the whole path, lanes); the first match wins.
RULES = (
    # This workflow, its pins, this selector and the Dockerfile that defines every stage: a
    # change here can alter any lane.
    (r"\.github/workflows/ci\.yml|\.github/ci/.*|\.github/scripts/(ci-lanes|test_ci_lanes)\.py"
     r"|Dockerfile|\.dockerignore", ALL),
    (r"\.github/scripts/test-counts\.py", frozenset({"test"})),
    # The other workflows run on their own triggers; here only actionlint reads them.
    (r"\.github/workflows/[^/]+", frozenset({"check"})),
    # The pull request title check and the changelog tool; changes.yml runs them, the check
    # stage their tests.
    (r"\.github/scripts/(pr-title|test_pr_title)\.py|tools/(changelog|test_changelog)\.py",
     frozenset({"check"})),
    # The dependency table that tools/check-crate-deps.py reads; the check stage runs the gate
    # and the tools tests.
    (r"crates/AGENTS\.md", frozenset({"check"})),
    # The agent rule files and the requirements check over them; the requirements job builds
    # the requirements stage, the check and its tests, on every run. AGENTS.md and CLAUDE.md
    # at any depth, except under vendor (vendored source), which keeps the rules below. No
    # lane builds or reads these files otherwise.
    (r"scripts/(check-requirements|test_check_requirements)\.py|REQUIREMENTS\.md|REVIEW\.md"
     r"|(?!vendor/)([^/]+/)*(AGENTS|CLAUDE)\.md", NONE),
    # Repository metadata that no job reads.
    (r"\.github/(ISSUE_TEMPLATE/.*|PULL_REQUEST_TEMPLATE/.*|pull_request_template[^/]*"
     r"|CODEOWNERS|dependabot\.yml|[^/]+\.md)", NONE),
    # Documentation. LICENSE, LICENSE-MIT and NOTICE also go into the image the size gate
    # measures; their compressed bytes are far inside its tolerance, and every push to master
    # measures them again. .gitignore does not filter the Docker build context.
    (r"[^/]+\.md|LICENSE[^/]*|NOTICE|CODEOWNERS|docs/.*|changes/.*|\.coderabbit\.yaml|\.gitignore", NONE),
    # What the dashboard build is made of. Its output is embedded into the binary
    # (crates/ck-server/build.rs), so the tests, the gates and THIRD-PARTY-NOTICES (the npm
    # packages the bundle pulls in) all depend on it.
    (r"ui/(src|public|tools)/.*|ui/(index\.html|package(-lock)?\.json|vite\.config\.ts|tsconfig[^/]*\.json)",
     ALL),
    (r"ui/.*", frozenset({"dashboard"})),
    # Integration tests, examples and benches are test and clippy targets only; nothing
    # under them reaches the release binary (src/ includes fixtures from tests/ only in
    # #[cfg(test)] code), so the size and idle gates are not affected.
    (r"crates/[^/]+/(tests|examples|benches)/.*", frozenset({"check", "test"})),
    # The dependency set, which THIRD-PARTY-NOTICES lists (checked in the check stage).
    (r"Cargo\.(toml|lock)|crates/[^/]+/Cargo\.toml|vendor/.*", RUST),
    (r"about\.toml|tools/third-party-notices\.py|THIRD-PARTY-NOTICES", frozenset({"check"})),
    (r"crates/.*|\.config/.*|\.cargo/.*|clippy\.toml|rustfmt\.toml"
     r"|rust-toolchain(\.toml)?", RUST),
    # harness/rust is built by the check stage only.
    (r"harness/.*", frozenset({"check"})),
    # budgets.txt holds the heap gate's numbers (a test) and the size and idle budgets; the
    # gate scripts and the cache replay script (run by test shard 2) live here too.
    (r"bench/.*", frozenset({"test", "perf"})),
)
_COMPILED = tuple((re.compile(pattern, re.DOTALL), lanes) for pattern, lanes in RULES)


def lanes_for(path):
    lanes = next((lanes for pattern, lanes in _COMPILED if pattern.fullmatch(path)), ALL)
    # The check stage measures every Rust file, wherever it lives (tools/check-rust-size.py).
    return lanes | {"check"} if path.endswith(".rs") else lanes


def classify(paths):
    """The lanes for a list of changed paths; none at all is unexpected and runs all."""
    paths = [path for path in paths if path]
    if not paths:
        return ALL
    selected = set()
    for path in paths:
        selected |= lanes_for(path)
    return frozenset(selected)


def changed_paths(base, head, git=None):
    """Both sides of every change between two commits, or None when git cannot say."""
    git = git or (lambda *args: subprocess.run(
        ("git", *args), check=True, capture_output=True, text=True).stdout)
    try:
        # --no-renames: a rename shows its old and its new path, and needs no blobs.
        out = git("diff", "--no-renames", "--name-only", "-z", base, head)
    except (OSError, subprocess.CalledProcessError):
        return None
    return [path for path in out.split("\0") if path]


def select(event_name, event, git=None):
    """(lanes, reason) for a workflow run."""
    if event_name != "pull_request":
        return ALL, f"{event_name or 'unknown'} event: every lane"
    labels = [label.get("name") for label in (event.get("pull_request") or {}).get("labels") or []]
    if "ci:full" in labels:
        return ALL, "ci:full label: every lane"
    git = git or (lambda *args: subprocess.run(
        ("git", *args), check=True, capture_output=True, text=True).stdout)
    try:
        parents = git("rev-list", "--parents", "-n", "1", "HEAD").split()
    except (OSError, subprocess.CalledProcessError):
        parents = []
    if len(parents) != 3:
        return ALL, "HEAD is not the test merge commit: every lane"
    paths = changed_paths("HEAD^1", "HEAD", git)
    if paths is None:
        return ALL, "git diff failed: every lane"
    if not paths:
        return ALL, "no changed paths: every lane"
    return classify(paths), f"{len(paths)} changed paths"


def current_labels(event, env=None, fetch=None):
    """The pull request's labels now, so a re-run after adding ci:full sees it."""
    env = os.environ if env is None else env
    pull = event.get("pull_request") or {}
    labels = [label.get("name") for label in pull.get("labels") or []]
    number, repo, token = pull.get("number"), env.get("GITHUB_REPOSITORY"), env.get("GH_TOKEN")
    if not (number and repo and token):
        return labels
    url = f"{env.get('GITHUB_API_URL', 'https://api.github.com')}/repos/{repo}/issues/{number}/labels"

    def get(url):
        request = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"})
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.load(response)
    try:
        return sorted(set(labels) | {label["name"] for label in (fetch or get)(url)})
    except (OSError, ValueError, KeyError, TypeError):
        return labels


def write_outputs(lanes, reason):
    ordered = [lane for lane in LANES if lane in lanes]
    with open(os.environ["GITHUB_OUTPUT"], "a") as out:
        for lane in LANES:
            out.write(f"{lane}={'true' if lane in lanes else 'false'}\n")
        out.write(f"lanes={json.dumps(ordered)}\n")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as out:
            out.write(f"### Lanes\n\n{reason}\n\n| Lane | Runs |\n| --- | --- |\n")
            for lane in LANES:
                out.write(f"| {lane} | {'yes' if lane in lanes else 'no'} |\n")
    print(f"{reason}; lanes: {' '.join(ordered) or '(none)'}")


def main(argv):
    if argv[:1] == ["--paths"]:
        print(" ".join(lane for lane in LANES if lane in classify(argv[1:])) or "(none)")
        return 0
    if argv[:1] == ["--diff"] and len(argv) == 3:
        paths = changed_paths(argv[1], argv[2])
        lanes = ALL if paths is None else classify(paths)
        print(" ".join(lane for lane in LANES if lane in lanes) or "(none)")
        return 0
    if argv:
        print(__doc__, file=sys.stderr)
        return 2
    event_name = os.environ.get("GITHUB_EVENT_NAME", "")
    try:
        with open(os.environ["GITHUB_EVENT_PATH"]) as f:
            event = json.load(f)
    except (KeyError, OSError, ValueError):
        event = {}
        if event_name == "pull_request":
            write_outputs(ALL, "event payload unreadable: every lane")
            return 0
    if event_name == "pull_request":
        pull = event.setdefault("pull_request", {})
        pull["labels"] = [{"name": name} for name in current_labels(event)]
    lanes, reason = select(event_name, event)
    write_outputs(lanes, reason)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
