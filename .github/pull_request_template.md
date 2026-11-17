<!-- AI may help write this description; unverified content may not be submitted.

State each fact once; do not restate the title, diff or template.
Do not use filler, praise, summaries of your own work or emoji.
Paste only relevant log lines; link or collapse long output.
State what you could not verify.
-->

<sub>中文模板：[提交中文说明](?template=pull_request_template_zh.md)。</sub>

Closes #

## Problem

<!-- The behavior before the change, or the need it meets. -->

## How I fixed it

<!-- The behavior after the change. -->

## Verified

List commands and observed results, including the test that fails without this change. State what you could not verify.

Submitted and answered for by (Human responsible, GitHub username):

---

Check only claims that are true. A false claim closes the pull request.

- [ ] I understand this change and will answer for every line. An AI cannot answer for me.
- [ ] This branch is based on the current `master`.
- [ ] Each commit contains one coherent change with a short English `type(scope): subject` following the same grammar as the pull request title. Fixups are squashed before pushing, not pushed as `fixup!` commits.
- [ ] `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings` and `cargo test --workspace --locked` pass, and the Verified section records any check I could not complete.
- [ ] I ran `npm run check`, `npm test` and `npm run build` in `ui/` when I changed the dashboard, and added every new string to the English, `zh-CN` and `zh-TW` catalogs.
- [ ] No real keys, tokens or account data appear in code, tests, logs or screenshots.
- [ ] I updated the documentation where users see the change.
- [ ] I added a `changes/<branch-slug>.md` fragment when I changed a shipped path (see `changes/README.md`).
- [ ] AI helped write this change.
- [ ] No AI was used for this change.
- [ ] I checked the boxes above without reading them.
- [ ] I am an AI agent, and this content was submitted without human review.
