"""Tests for ci-lanes.py; the changes job in ci.yml runs them before it selects lanes.

    python3 -m unittest discover -s .github/scripts -p 'test_ci_lanes.py'
"""
import importlib.util
import pathlib
import subprocess
import unittest

_spec = importlib.util.spec_from_file_location(
    "ci_lanes", pathlib.Path(__file__).with_name("ci-lanes.py"))
ci_lanes = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ci_lanes)

ALL = set(ci_lanes.LANES)
RUST = {"check", "test", "perf"}


def lanes(*paths):
    return set(ci_lanes.classify(list(paths)))


class Paths(unittest.TestCase):
    def test_documentation_runs_nothing(self):
        for path in ("README.md", "CONTRIBUTING.md", "CHANGELOG.md", "AGENTS.md", "SECURITY.md",
                     "SUPPORT.md", "LICENSE", "LICENSE-MIT", "NOTICE", "docs/INSTALL.md",
                     "docs/img/overview-dark.png", "docs/parity-audit/x/y.md",
                     "changes/fix-thing.md", "changes/README.md", ".coderabbit.yaml", ".gitignore",
                     ".github/ISSUE_TEMPLATE/bug.yml", ".github/pull_request_template.md",
                     ".github/PULL_REQUEST_TEMPLATE/zh.md", ".github/CODEOWNERS", "CODEOWNERS",
                     ".github/SECURITY.md", ".github/dependabot.yml"):
            with self.subTest(path=path):
                self.assertEqual(lanes(path), set())

    def test_markdown_inside_shipped_paths_is_not_documentation(self):
        self.assertEqual(lanes("crates/ck-server/README.md"), RUST)
        self.assertEqual(lanes("vendor/README.md"), RUST)
        self.assertEqual(lanes("ui/DESIGN.md"), {"dashboard"})
        self.assertEqual(lanes("bench/results/x.md"), {"test", "perf"})
        self.assertEqual(lanes("harness/README.md"), {"check"})

    def test_rule_files_and_the_requirements_check_run_no_lane(self):
        for path in ("AGENTS.md", "CLAUDE.md", "REQUIREMENTS.md", "REVIEW.md",
                     ".github/AGENTS.md", "crates/ck-server/AGENTS.md",
                     "crates/ck-server/src/CLAUDE.md", "ui/AGENTS.md", "ui/src/AGENTS.md",
                     "scripts/check-requirements.py", "scripts/test_check_requirements.py"):
            with self.subTest(path=path):
                self.assertEqual(lanes(path), set())
        # Vendored source keeps its own rules.
        self.assertEqual(lanes("vendor/gjson/AGENTS.md"), RUST)
        # Only the exact names: a near miss, another script or a nested REQUIREMENTS.md is
        # not covered.
        self.assertEqual(lanes("crates/ck-server/AGENTS.md.bak"), RUST)
        self.assertEqual(lanes("crates/REQUIREMENTS.md"), RUST)
        self.assertEqual(lanes("ui/MY-AGENTS.md"), {"dashboard"})
        self.assertEqual(lanes("scripts/other.py"), ALL)

    def test_crate_dependency_table_runs_the_check_lane(self):
        # tools/check-crate-deps.py reads the table in crates/AGENTS.md.
        self.assertEqual(lanes("crates/AGENTS.md"), {"check"})

    def test_other_workflows_reach_actionlint(self):
        for path in (".github/workflows/release.yml", ".github/workflows/changes.yml"):
            with self.subTest(path=path):
                self.assertEqual(lanes(path), {"check"})

    def test_ci_definition_runs_everything(self):
        for path in (".github/workflows/ci.yml", ".github/ci/pins.env",
                     ".github/scripts/ci-lanes.py", ".github/scripts/test_ci_lanes.py",
                     "Dockerfile", ".dockerignore"):
            with self.subTest(path=path):
                self.assertEqual(lanes(path), ALL)

    def test_ci_helpers(self):
        self.assertEqual(lanes(".github/scripts/test-counts.py"), {"test"})
        for path in (".github/scripts/pr-title.py", ".github/scripts/test_pr_title.py",
                     "tools/changelog.py", "tools/test_changelog.py"):
            with self.subTest(path=path):
                self.assertEqual(lanes(path), {"check"})

    def test_dashboard(self):
        # The binary embeds the dashboard stage's build, so its inputs reach every stage.
        for path in ("ui/src/App.tsx", "ui/package.json", "ui/package-lock.json",
                     "ui/vite.config.ts", "ui/tools/notices-plugin.ts", "ui/index.html",
                     "ui/public/favicon.svg", "ui/tsconfig.json"):
            with self.subTest(path=path):
                self.assertEqual(lanes(path), ALL)
        self.assertEqual(lanes("ui/DESIGN.md", "ui/scripts/measure.mjs"), {"dashboard"})

    def test_rust(self):
        for path in ("crates/ck-server/src/main.rs", "crates/ck-server/build.rs",
                     "crates/ck-server/LICENSE", ".config/nextest.toml", "clippy.toml",
                     "rustfmt.toml", "rust-toolchain.toml", "crates/cooker/Cargo.toml",
                     "Cargo.toml", "Cargo.lock", "vendor/gjson.patch", "vendor/gjson/src/lib.rs"):
            with self.subTest(path=path):
                self.assertEqual(lanes(path), RUST)

    def test_notices(self):
        for path in ("about.toml", "tools/third-party-notices.py", "THIRD-PARTY-NOTICES"):
            with self.subTest(path=path):
                self.assertEqual(lanes(path), {"check"})

    def test_test_only_rust_skips_the_release_gates(self):
        for path in ("crates/ck-server/tests/management.rs",
                     "crates/ck-exec/tests/fixtures/gemini_go.json",
                     "crates/ck-exec/tests/reference/xai/main.go",
                     "crates/ck-server/examples/x.rs", "crates/ck-core/benches/b.rs"):
            with self.subTest(path=path):
                self.assertEqual(lanes(path), {"check", "test"})
        # A source file next to them still runs the gates.
        self.assertEqual(lanes("crates/ck-server/tests/a.rs", "crates/ck-server/src/a.rs"), RUST)

    def test_harness_and_bench(self):
        self.assertEqual(lanes("harness/rust/src/main.rs"), {"check"})
        self.assertEqual(lanes("bench/budgets.txt"), {"test", "perf"})
        self.assertEqual(lanes("bench/gate.sh"), {"test", "perf"})

    def test_rust_anywhere_runs_the_size_gate(self):
        # tools/check-rust-size.py in the check stage measures every Rust file.
        self.assertEqual(lanes("ui/scripts/login-harness/src/main.rs"), {"dashboard", "check"})
        self.assertEqual(lanes("ui/scripts/login-harness/Cargo.toml"), {"dashboard"})
        self.assertEqual(lanes("bench/load/src/main.rs"), {"test", "perf", "check"})

    def test_unknown_paths_run_everything(self):
        for path in ("tools/x.py", "newdir/file", ".github/scripts/new.sh",
                     ".github/other.yml", "crates"):
            with self.subTest(path=path):
                self.assertEqual(lanes(path), ALL)

    def test_union_and_empty(self):
        self.assertEqual(lanes("README.md", "ui/DESIGN.md"), {"dashboard"})
        self.assertEqual(lanes("README.md", "Cargo.lock"), RUST)
        self.assertEqual(lanes(), ALL)
        self.assertEqual(lanes(""), ALL)


class Events(unittest.TestCase):
    MERGE = "m" * 40 + " " + "b" * 40 + " " + "h" * 40 + "\n"

    def fake_git(self, diff, parents=MERGE, fail=False):
        calls = []

        def git(*args):
            calls.append(args)
            if fail:
                raise subprocess.CalledProcessError(1, args)
            return parents if args[0] == "rev-list" else diff
        return git, calls

    def test_non_pull_request_events_run_everything(self):
        for name in ("push", "workflow_dispatch", "workflow_call", "schedule", ""):
            with self.subTest(event=name):
                self.assertEqual(set(ci_lanes.select(name, {})[0]), ALL)

    def test_full_label(self):
        event = {"pull_request": {"labels": [{"name": "ci:full"}]}}
        git, _ = self.fake_git("README.md\0")
        self.assertEqual(set(ci_lanes.select("pull_request", event, git)[0]), ALL)

    def test_labels_come_from_the_api_with_the_payload_as_fallback(self):
        event = {"pull_request": {"number": 7, "labels": [{"name": "rust"}]}}
        env = {"GITHUB_REPOSITORY": "o/r", "GH_TOKEN": "t"}
        urls = []

        def fetch(url):
            urls.append(url)
            return [{"name": "ci:full"}]
        self.assertEqual(ci_lanes.current_labels(event, env, fetch), ["ci:full", "rust"])
        self.assertEqual(urls, ["https://api.github.com/repos/o/r/issues/7/labels"])

        def broken(url):
            raise OSError("offline")
        self.assertEqual(ci_lanes.current_labels(event, env, broken), ["rust"])
        self.assertEqual(ci_lanes.current_labels(event, {}, fetch), ["rust"])

    def test_pull_request_diffs_the_merge_commit_against_its_base(self):
        git, calls = self.fake_git("README.md\0docs/a b\nc.md\0")
        self.assertEqual(set(ci_lanes.select("pull_request", {"pull_request": {}}, git)[0]), set())
        self.assertEqual(calls[-1], ("diff", "--no-renames", "--name-only", "-z", "HEAD^1", "HEAD"))

    def test_failures_run_everything(self):
        pr = {"pull_request": {}}
        git, _ = self.fake_git("", fail=True)
        self.assertEqual(set(ci_lanes.select("pull_request", pr, git)[0]), ALL)
        git, _ = self.fake_git("README.md\0", parents="h" * 40 + " " + "b" * 40 + "\n")
        self.assertEqual(set(ci_lanes.select("pull_request", pr, git)[0]), ALL)
        git, _ = self.fake_git("")
        self.assertEqual(set(ci_lanes.select("pull_request", pr, git)[0]), ALL)


if __name__ == "__main__":
    unittest.main()
