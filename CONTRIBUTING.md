# Contributing

cooker accepts bug reports, fixes and new tests.

## Before you start

Open an issue before starting a large change, so a maintainer can agree on the approach first.

## Building and testing

You need Rust (stable), `cmake`, `clang` and `perl` for BoringSSL.

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

CI runs every check as a stage of the [`Dockerfile`](Dockerfile). With Docker alone, run the same stages locally: `docker buildx build --target check .` runs format, clippy, actionlint, notices, the harness build and the script tests; `--target test` runs the tests with no network; `--target dashboard` and `--target perf` run the size and idle gates.

The test stage uses [cargo-nextest](https://nexte.st), which runs test binaries in parallel (`cargo nextest run --workspace --locked`; its settings are in `.config/nextest.toml`). It then runs the doc tests and the `ck-server` unit tests with the optional relay and profiler: `cargo nextest run -p ck-server --features media-relay,pprof --lib`.

Tests use local mock upstreams only. Do not point a test at a real provider, and do not use a real account or key. Block external network access when you can; CI runs the tests in a network namespace with only a loopback interface. The BoringSSL build and the release link use a lot of memory, so set `CARGO_BUILD_JOBS=2` on a small machine.

Image builds accept `--build-arg CK_COMMIT=<commit-sha>` and `--build-arg CK_BUILD_DATE=<ISO-8601-UTC-date>` for the startup log and management `X-Ck-Commit` and `X-Ck-Build-Date` headers. CI and release builds use the checked-out commit's SHA and committer time in UTC, so rebuilding a commit keeps its build date. Local `cargo build` needs neither variable; without them it reports `none` and `unknown`.

### Performance budgets

CI holds the binary to the numbers in [`bench/budgets.txt`](bench/budgets.txt) and counts events and bytes, never elapsed time:

- Size, on every push to master and on pull requests that can change the binary (see `.github/scripts/ci-lanes.py`): the Linux release binary; its `.text` and `.rodata`; and the number of crates linked in more than one version.
- Idle, on the same changes: [`bench/idle.sh`](bench/idle.sh) with 1 and with 2,000 auth files measures wakeups, CPU ticks, threads and RSS over 30 seconds without requests.
- Heap per request: `crates/ck-server/tests/alloc_budget.rs`, part of `cargo test`, sends one 306 KB and one 1.9 MB request through the Claude route, the same sizes through Claude count_tokens (counted locally) and through the Codex Responses route, and the Claude bodies cloaked and signed through a Claude OAuth login with request capture. It sends them one at a time and eight at once, under a counting allocator.
- Memory soaks ([`bench/soak.sh`](bench/soak.sh)), by hand before a release: the Claude load and the field mix (Claude, Codex and count_tokens requests of 200 KB to 2 MB). The resting RSS must stay flat, and the field mix's peak (`VmHWM`) must stay within its budget.

To run the gates locally, run `docker buildx build --target perf .`. The size lines and the dashboard's `ui/tools/size-budget.json` are ceilings with headroom, so an ordinary pull request does not edit them; the perf and dashboard jobs print each number next to its ceiling. Raise a ceiling or budget only when a change needs it, with a maintainer's approval, and record the reason next to the line.

The dashboard lives in `ui/` (React 19 and Astryx). From that directory, run `npm ci`, `npm run check`, `npm test` and `npm run build`; the build also checks the bundle size budget. See [ui/README.md](ui/README.md).

## Changelog fragments

Do not edit `CHANGELOG.md` in a pull request. Add one file named `changes/<branch-slug>.md`. Its first line is `Added`, `Changed`, `Fixed`, `Removed`, `Security` or `Internal`, followed by `- ` bullets that describe the change users see:

```markdown
Fixed

- The relay stops waiting when the upstream request times out.
```

A pull request that touches shipped code needs a new fragment. The shipped paths are `crates/`, `ui/src/`, `ui/public/`, `ui/index.html`, `Dockerfile`, `vendor/`, `Cargo.toml` and `Cargo.lock`. Tests and documentation do not require one. A maintainer can apply the `no-changelog` label when an entry is unnecessary. The `Changes` workflow checks every fragment. Run `python3 -I tools/changelog.py check` locally to run the same check, and `python3 -I tools/changelog.py render` to preview the Unreleased section. [changes/README.md](changes/README.md) contains the full rules and the release command.

## Pull requests

- Fill in the [pull request template](.github/pull_request_template.md); the first link in the template opens the Chinese version. Name the person who answers for the change. You may use AI to help write the description, but read and verify every line; you answer for it.
- Keep each pull request to one change, with a test that fails without it.
- Sign every commit; the commits are squashed when the pull request merges.
- Request bodies are forwarded byte for byte unless a ported rule rewrites them. Do not re-serialize JSON only to pass it through.
- Never log or forward credentials or client keys to the wrong side.
- Mark a deliberate simplification with a `ponytail:` comment that names its limit and how to lift it.
- CI must pass: the `ci-ok` check requires every lane the change selects (the `dashboard`, `check`, `test` and `perf` stages of the Dockerfile), the changelog fragment check (`fragments`) and the title check (`title`). To run every lane, add the `ci:full` label and re-run the CI workflow.
- A ruleset protects `master`, and nobody can bypass it, administrators included:
  - every change goes through a pull request and is squash-merged;
  - `ci-ok`, `fragments` and `title` must pass. A branch behind `master` can still merge; GitHub refuses only when the two conflict;
  - force pushes, deletion and merge commits are blocked.

## Commits and pull request titles

Pull requests are squash-merged, so the pull request title becomes the commit subject on `master` and GitHub appends the number, as in `fix(ui): keep Overview tiles whole on a phone (#41)`. The description stays on the pull request; write the reason there.

- Write the title as `type(scope): subject`. Types: `feat`, `fix`, `refactor`, `test`, `style`, `chore`, `docs`, `deps`, `ci` and `release`. Scopes: `cli` (crates/cooker), `server`, `exec`, `stream`, `translate`, `core` (ck-core and ck-wire), `ui`, `docker`, `ci`, `docs`, `deps`, `bench`, `tools` and `release`.
- The subject is an English imperative phrase that starts in lower case, such as `retry a reset Claude stream once`, with no full stop. A first word such as `OAuth` keeps its case.
- The whole title fits in 72 characters.
- Commit subjects follow the same grammar. Each commit stands alone; squash fixups before pushing rather than pushing `fixup!` commits.
- The `Changes` workflow checks the title and reruns when you edit it. Run `python3 -I .github/scripts/pr-title.py '<title>'` locally to run the same check. The type and scope lists live in that script.

## Issues

Search existing issues first, then choose a form: bug report or feature request, each in English and in Simplified Chinese. State your version (`cooker --version`) and how you installed it. Remove keys, tokens and account files from logs and configuration before posting. Ask usage questions through [SUPPORT.md](.github/SUPPORT.md) and report vulnerabilities through [SECURITY.md](SECURITY.md), never in a public issue. Everyone taking part follows the [code of conduct](.github/CODE_OF_CONDUCT.md).

By contributing, you agree that your work is released under the [GNU Affero General Public License, version 3 or later](LICENSE).
