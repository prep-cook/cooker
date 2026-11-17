# S1 handoff

State as of 2026-11-17. S1 is the rewrite of the request path described in [INTERNAL-STREAM.md](INTERNAL-STREAM.md) and [EXECUTION-CONTRACT.md](EXECUTION-CONTRACT.md). Paths are relative to the repository root. Line numbers are from the head of `feat/s1-execution` at `d29851e1` and drift; search for the symbol when a line does not match.

## 1. Status

S1 moves every provider executor onto one execution contract and moves every request and response through an internal event stream stored in an mmap pool ([INTERNAL-STREAM.md](INTERNAL-STREAM.md)). Clients must see the official API shape; what cannot be converted faithfully is refused, never degraded.

All work is on the integration branch `feat/s1-execution`; each unit is one PR into it. Nothing is in `master`. Merging the branch into `master` and any release need owner approval (root `AGENTS.md`, "Rule priority").

Done: every route family (Chat, Anthropic, Gemini, Responses, compact, images, speech, video, alpha search, Responses WebSocket, Realtime, WebRTC, AI Studio relay) runs on the new path. The last merged PRs are #203 to #209 and #211 (section 6); #210 is being merged. Six cases still reach the old per-provider `execute`; they block the main deletion (D9) and E13 (section 3).

Open:

- #174 (W1, unaligned 4 KiB records): held for XZ's own review. Do not merge before XZ approves.
- F11 (#208), F27 (#209) and F34 (#211) are merged; T1 (#210) is being merged. Check `gh pr list -R prep-cook/cooker --state open`.
- Master sync: `feat/s1-execution` lacks 9 `master` commits (#66 docs rewrite, #68 and #78 lock-descriptor fixes, #71, #72, #75, #76 size ratchet, #129 deps, #131). A PR merging `master` into the S1 branch is being prepared. Then a `feat/s1-execution` to `master` PR goes to an owner. Ruling 2026-11-17: merge into `master` after the remaining units land and CI is green; an owner runs the performance gate afterwards.
- Frontend and management API split (section 3.6): not started, begins after S1 reaches `master`.
- D9 main deletion, E13, DU, RI-MMAP and the performance gate (section 3).
- T1, upstream stream timeouts (ruling 2026-11-15 採 2): #210, being merged. Deadlines sit at `ck_exec::pipeline::start` (`crates/ck-exec/src/pipeline.rs`): 600 s before headers, 300 s idle between upstream chunks, 3,600 s total (`requests.upstream-timeouts`, integers above 0). Before output the attempt fails over without cooling the credential; after output the client gets the protocol error event and one usage record. The old execute arm (Devin, AI Studio, force-mapped aliases) and live sessions are not covered until D9. The numbers are recorded in `REQUIREMENTS.md`, "Numbers", at the owner's request (2026-11-17 「都寫吧要不要他會看」); keeping or dropping them is the next owner's call.
- F-series: every row in the triage is merged, replaced by a media unit (F28, F29 by E7) or listed in section 3.
- Owner answers still needed: R-16 and R-17 (confirm), R-18 and R-19 (section 3).

## 2. Start here

Commands, from [REQUIREMENTS.md](../REQUIREMENTS.md) "Commands" and `crates/AGENTS.md` "Verification". Before opening a PR, on the final tree:

```sh
docker buildx build --target check .
docker buildx build --target test .
docker buildx build --target dashboard .
docker buildx build --target requirements .   # when only rule or doc files changed
docker buildx build --target perf .           # after changes to anything that runs while idle
```

Fast local forms while iterating:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo nextest run --workspace --locked        # or one crate: cargo test -p ck-exec kimi
python3 -I tools/changelog.py check
python3 -I .github/scripts/pr-title.py "$PR_TITLE"
python3 -I tools/check-crate-deps.py
python3 -I tools/third-party-notices.py --check
```

The full suite is slow and memory-heavy because of BoringSSL. On a small machine use `CARGO_BUILD_JOBS=2 nice -n 19`. Tests use loopback mock upstreams only. Run formatting on the tree you push: a format check run on a remote copy leaves your local tree unformatted, and CI then fails on the diff (#201).

CI checks that commonly fail:

- PR title: `type(scope): subject`, English imperative, lower case, at most 72 characters. The allowed scopes are the set in `.github/scripts/pr-title.py` (cli, server, exec, stream, translate, core, ui, docker, ci, docs, deps, bench, tools, release). A title with scope `image` failed on #200; the scope was `docker`.
- Changelog fragment: a PR that touches `crates/`, `ui/src/`, `Dockerfile`, `vendor/`, `Cargo.toml` or `Cargo.lock` adds `changes/<branch-slug>.md` (format in `changes/README.md`). The first line is exactly one type (Added, Changed, Fixed, Removed, Security or Internal); a fragment with two types failed on #206. Do not edit `CHANGELOG.md`. A test-only change in `src/` modules fails the check and needs the `no-changelog` label from a maintainer.
- Deleted test binaries must also leave `.config/nextest.toml`. #199 failed because an override still named the removed `duplex_dispatch` binary.
- Stream pool in tests: the pool lives in `stream.pool_directory`, default `$XDG_RUNTIME_DIR`. Runtimes built with `ck_server::testing::runtime` open a private pool directory (#193). Any other test setup needs `XDG_RUNTIME_DIR` on a tmpfs or its own pool directory. In the Docker `test` stage the store is small: 66 `ck-server` tests answered 503 `stream.storage_exhausted` once every request was stored. Reproduce with `docker buildx build --target test .`, not host `cargo test`.
- Load-sensitive tests: `large_upstream_sections_stay_bounded_in_memory` (`crates/ck-server/src/observability/request_logging.rs:2559`), a `siwc` lock test and an xAI allocation test failed under full-suite load only. Rerun the single test before blaming a change.
- Stacked PRs target their parent branch and must be retargeted after the parent merges. A plain rebase replays the parent's commits; use `git rebase --onto <base> <old parent head>`.
- Rebase conflicts concentrate in `crates/ck-exec/tests/support/mod.rs` and `crates/ck-server/src/observability/usage_record.rs`. Run usage-related units one at a time.

Two PRs that pass CI separately can break the branch together. #179 and #198 were each green, and the branch failed to build after both merged (three interface mismatches, fixed in `fb730ce1` inside #187). After merging two PRs that touch the same interfaces (dispatch, attempt, executors), build and test the branch before merging the next.

Rules that bind every contributor:

- No AI attribution in commits, PRs or comments. Sign commits; the human submitter is the author.
- A release needs an owner plus Vayun. Owners: Zakkaus, xz-dev, Glassyiris, JohnsonRan. No tags, releases or image pushes without that.
- No live-upstream traffic without an owner's approval. Never put secrets in logs, source or client storage.
- The Claude OAuth cloak sends the client's original bytes (member order and `cch` depend on them). The only raw-byte path is G5; see `crates/AGENTS.md` for the bounded-body exception.
- Do not raise a budget in `bench/budgets.txt` without an owner and a reason beside the line.
- Record set-aside findings in [DEFERRED-OPTIONS.md](DEFERRED-OPTIONS.md) (source, reason, trigger) and open decisions in [OPEN-QUESTIONS.md](OPEN-QUESTIONS.md).
- High-risk changes (money, login, security, deletion) need a written plan, a failing test first, ablation of every behaviour hunk and an independent reviewer; see `REVIEW.md`.
- `CONTRIBUTING.md` says a ruleset protects `master` with required checks; `AGENTS.md` "Merging" and OPEN-QUESTIONS row 25 say there are none. The GitHub rules on `master` list pull request, linear history, no force push and no deletion, and no required status checks. Classic branch protection was not queried.

Architecture in one paragraph. A client request enters a codec ingress, which writes canonical events into a request stream. Dispatch picks a candidate; `refence` re-validates the request against that candidate's capability; the executor sends upstream and publishes response events into a response stream (`crates/ck-exec/src/pipeline.rs`); the client egress codec reads that stream. Each attempt has its own identity and stream. Commitment happens at the first deliverable client byte, and failover stops there. Crates: `ck-core` (shared types, config, the old path's `ExecRequest` and `ResponseBody`), `ck-wire`, `ck-stream` (the mmap pool), `ck-translate` (codecs and the old pair registry), `ck-exec` (executors), `ck-server` (routes, dispatch, usage), `cooker` (binary). Stream store limits: 4,096-byte frames after #174 (50,000 before), 256 MiB per stream, 4 GiB total, one hour expiry since last read, 8 MiB metadata per exchange, no heap fallback (a full store answers 503 `stream.storage_exhausted`).

## 3. Handed-over work

### 3.1 D9 main deletion

Merged part: #207 deleted the old-path code that no route reaches (`Executors::fence`, the settings `requests.streaming.bootstrap-retries` and `requests.nonstream-keepalive-interval`, the Responses old-arm rendering, `codex_events::Events::inlined`). Existing configs that still set the two deleted settings are ignored, not rejected.

Not done: `crates/ck-exec/src/translate.rs`, the generic translation in the old Codex client, each provider's old `execute`, the `UsageSink` response methods and `Call::body`. After #203 and #207, no Responses request needs the old arm, but Chat, Anthropic and Gemini requests still reach `rt.executors.execute` (`crates/ck-server/src/inference/dispatch.rs`, near line 1475) in six cases. Each ends in a provider's old `execute`, which calls `translate::request` and `translate::response` and the `UsageSink` response methods.

| # | Case | Where the old arm is chosen | Recommendation |
| --- | --- | --- | --- |
| 1 | Force-mapped model alias, any provider, any non-Responses client | `crates/ck-server/src/inference/dispatch/attempt.rs`, comment at line 276 | Move to the codec path. Small: write `exchange.model` in the Chat, Anthropic and Gemini egress, as the Responses egress already does. |
| 2 | Devin, AI Studio, Gemini Interactions (no codec executor) | `Executors::destination` returns `None`, `crates/ck-exec/src/lib.rs:229` | Keep as one narrow legacy module until their codecs exist (ruling R-12 defers them). |
| 3 | Kimi receiving Claude-format requests, and Kimi count tokens | `crates/ck-exec/src/kimi_exec.rs:44`, `:49`, `:132` (`codec::Error::Destination`) | Move to the codec path. |
| 4 | Anthropic client on a Claude credential doing thinking replay | `contract_path` in `crates/ck-exec/src/claude/execute.rs:192` | Move to the codec path. |
| 5 | Meta with a custom `apply_patch` from a non-Responses client | `crates/ck-exec/src/meta_exec.rs` (`Destination` refusal near line 56) | Refuse with 400. |
| 6 | xAI deferred Chat jobs | `crates/ck-exec/src/xai_deferred.rs`, call at `dispatch.rs:1471` (`Executors::deferred_chat`) | Move to the codec path. |

Case 1 alone keeps every provider's old `execute` reachable, so it comes first. Until all six are moved or refused, `translate.rs`, the old per-provider `execute`, the `UsageSink` response methods and `Call::body` stay, and E13 cannot start. Estimate: one working day for cases 1, 3, 4, 5 and 6 in parallel, then the deletion.

The T1 deadlines (#210) do not cover the old arm either, so cases 1 to 6 run without upstream timeouts until they move. That is one more reason to finish D9.

Decision needed (R-19): confirm the table. Recommended: as written. These moves and the refusal change behaviour, so they need an owner answer first.

The old arm also provides fallbacks that disappear with it: replay-eligible Claude credentials, buffered cloaked requests, Kimi and Meta `apply_patch`. For each fallback you remove, write the test that proves the replacement path.

Acceptance gate: `git grep` finds no old type or pair outside the Phase E files. Do not delete `codex_request::shape` and `xai_request::prepare` while the Codex and xAI WebSocket transports still call them. Whether #199 already removed the old Codex WebSocket path and `websocket_tools.rs` was not verified.

Do not: add a new old-arm fallback to make a case pass; delete a shaping helper that the contract path shares with an old executor before the old `execute` is gone.

### 3.2 E13

Starts after D9. Delete the old `ck-core` types and the rest of the old path: `ExecRequest`, `ExecResponse`, `ResponseBody`, `ExecStream`, `ExecSession`, `UsageSink::response_*`, `run_with_bootstrap_retries`, the media keepalives, `respond::{Writer, stream}`, `images::Raw`, `BINDINGS`, `DOWNLOADS`, `openai_compat_multipart.rs`, `Executors::{images, videos, speech, execute_in_session}`, and the remainder of the `ck-translate` pair registry. Acceptance: `git grep` finds none of them. Also delete the unreachable speech and old-arm residue listed in section 5, item 23.

Decision needed: none beyond R-19. Do not delete a type that a Phase E file still imports; build the branch after each deletion step.

### 3.3 DU: scratch-direct upload

Question: can converted executors send the stored request without first copying the egress lease (`ScratchLease`) into one `Vec`? Result of the investigation: not as a small change. DU is a redesign of the post-encode request pipelines of nine executors. No code was written.

Verified facts:

1. Every converted executor copies the lease into a `Vec` (17 sites, `git grep -n "copy(&" crates/ck-exec/src`), and that `Vec` is the input of whole-body text edits (gjson/sjson ports), not a buffer handed to the HTTP client. No executor sends the lease unedited.

   | Executor | Edits applied to the copy before send |
   | --- | --- |
   | Claude, `claude/execute.rs` | effort snap, then the system, signals and `cache_control` pipeline; the cloak path sends client bytes and stays |
   | Gemini, `gemini_exec.rs` | suffix thinking, model naming, payload rules; count wrapper |
   | Vertex, `vertex_exec.rs` | strip call ids, then Gemini generate |
   | OpenAI-compatible relay, `openai_compat_exec.rs` | model, thinking, payload rules, text-only tool results, max tokens, prompt cache key (hashes the body), `stream_options.include_usage`; compact: sanitize encrypted reasoning |
   | Codex, `codex_exec.rs` | upload checks, thinking, instructions, payload rules; `codex_events.rs` copies replay items |
   | Kimi, `kimi_exec.rs` | request shaping for Responses and Chat |
   | Meta, `meta_exec.rs` | shaping, then usage request accounting |
   | xAI, `xai_exec.rs` | shaping and payload rules; `Events::new` also reads the upload for tool declarations |
   | Antigravity, `antigravity_exec.rs` | legacy `ExecRequest` with two body copies (`body` and `original_body`) |
   | Media, `media_exec.rs`, `codex_alpha.rs` | string conversions and per-provider shaping |

2. gjson and sjson need one contiguous `&[u8]`. A `ScratchLease` is a list of 4 KiB pool frames from sharded free lists (`crates/ck-stream/src/pool.rs`), so a multi-frame lease is not contiguous. Reading it with the current helpers requires the copy.
3. The proxy takes `Bytes` and builds a fresh body per redirect hop (`crates/ck-exec/src/proxy.rs`, around line 885). The HTTP client resends a request refused by an HTTP/2 graceful GOAWAY or REFUSED_STREAM only when the body is a fully buffered `Bytes` body. A streamed lease body loses that transparent retry on every HTTP/2 upstream. The comment near `proxy.rs:877` records that plain bodies were kept for this reason.

Options:

- A, encode-time adjustments. Executors pass their adjustments (model, thinking, stream flags, max-tokens field, prompt cache key, payload rules) to the egress codec, which writes the final body into scratch. Byte identity requires reproducing sjson key placement (in-place replace, append at object end). Whole-body walks (text-only tool results, encrypted-content sanitize, Vertex call ids, Claude `cache_control` and signals) become codec work per protocol. Largest change, cleanest end state.
- B, streaming JSON patch over leases. One chunked path reader (value ranges over lease positions) and one splice writer with sjson-compatible placement. Each pipeline changes from `Vec -> Vec` to "read, then emit patch ops", and the send streams lease chunks with splices. One shared mechanism; every pipeline is still rewritten, and payload rules (wildcards, arrays, raw values, filters) must match sjson byte for byte.
- C, copy-on-write with opportunistic contiguity. Use a zero-copy contiguous view when a lease's frames happen to be consecutive, make pipelines `Cow` until the first edit, and send an unedited lease as owned `Bytes` (reusable, keeps the GOAWAY retry). Smallest change, but no guarantee: a fragmented pool or any edit still copies.

Recommended: B, one executor family per PR, ordered by pipeline size: Meta, Kimi, xAI, Gemini, Vertex, OpenAI-compatible, Codex, Claude, Antigravity (Antigravity first has to leave the old arm). Phase 1 ships the shared pieces with the first caller (Meta): an owned lease chunk type in `ck-stream` that implements `AsRef<[u8]>` so `Bytes::from_owner` is zero-copy, a proxy upload body rebuilt per hop, request capture that reads chunks, and the lease JSON reader and patcher. Each phase keeps the existing request-byte table tests unchanged and adds one row whose body source panics on a whole-body read.

Decisions needed (R-18):

1. Option A, B or C. Recommended B. Only C avoids rewriting the pipelines, and it does not guarantee that no whole-body `Vec` exists.
2. HTTP/2 GOAWAY retry: accept losing the client's transparent resend for streamed uploads, or add an own resend of a refused hop in `proxy::exchange` (the upload handle can be re-created per hop). Recommended: own resend with the same limit as the client library (2 per request), in phase 1.
3. Order against Anthropic stream-error units and Meta/Kimi shaping changes: they edit the same pipelines. Both have merged (#204, #203), so the earlier conflict no longer applies; start from the current branch.

Do not: start DU while the D9 moves touch `claude/execute.rs`, `kimi_exec.rs` or `meta_exec.rs`; run DU as one PR.

### 3.4 W1 (#174) and RI-MMAP

#174 stores request and response data records unaligned in 4,096-byte frames (`FRAME_BYTES` 50,000 to 4,096; chunk limit 36,864 as one constant; a waiting client egress reader triggers an immediate publish). It is open and held: XZ reviews it. Ruling W1-1: the pool sits above the heap in two memory rows. Follow-ups raised on it: store the frame table as contiguous ranges; reconcile the 8,192-byte final-record reservation with 4,096-byte frames (section 5, item 25). Do not merge before XZ approves.

RI-MMAP starts after #174 merges: move the request-stream record index into the mmap pool and change nothing else (ruling 2026-11-16: 「把索引放進 mmap pool 就行，優化合併節點，是否算法有點複雜了」). Do not merge codec nodes. The cost it removes: at 8 concurrent 1.9 MB requests the Claude cloak row held 17,856,940 bytes against a 15,954,548-byte budget (#179).

### 3.5 Performance gate

An owner runs the gate (「效能門禁 这个我之后自己来」). XZ asked for the target values even though the tests may fail (「但是你得把性能测试的目标值给我（但是测试本身可以失败）」). Current pass/fail results were not collected. Targets:

- Storage: no worse than heap on every row at 4,096-byte frames ([INTERNAL-STREAM.md](INTERNAL-STREAM.md) §9.3). Run on tmpfs.
- Codec and pipeline medians within 5 percent of the Go-era baseline (commit `b518060e`); memory 0 percent beyond a calibrated band ([INTERNAL-STREAM.md](INTERNAL-STREAM.md) §9.2, [BENCHMARKS.md](BENCHMARKS.md)).
- Binary: size 52,000,000 bytes, text 35,000,000, rodata 11,000,000, 11 duplicate crates for these rows; the duplicate-crate threshold in `bench/budgets.txt` is 12 after ruling R-14. The prose targets of 30 MB binary and 15 MB RSS are not met.
- Idle: 3 wakeups, 1 tick, 6 threads, RSS 33,500 KB with 1 credential and 85,800 KB with 2,000, 10 percent tolerance. Field soak peak 173,072 KB, 20 percent.
- Heap per request (`crates/ck-server/tests/alloc_budget.rs`): per-route peak and allocation budgets at 306 KB and 1.9 MB bodies, 2 and 3 percent tolerance. The prose target of 3x body is unmet (4x to 8.8x). The `large_requests` test is `#[ignore]`d with a reason in [DEFERRED-OPTIONS.md](DEFERRED-OPTIONS.md): 7 keys exceed the budget, accepted for the prototype (ruling D2-3a-1), no budget was changed.

Known conflicts in the gate definition: [INTERNAL-STREAM.md](INTERNAL-STREAM.md) judges codec CPU and allocation against an absolute per-event budget that is not set, while `bench/codec/gate.py` gates them at 5 percent. The codec gate exits 2 because the new harness has no Interactions and Antigravity codecs. The flow gate cannot pass until the gateway reports internal counters (`--diagnostic` lists the gaps). `release.yml` mentions an image budget but `bench/budgets.txt` has no image line.

Commands: `bench/gate.sh size x86_64-unknown-linux-gnu target/release/cooker`; `bench/gate.sh idle linux target/release/cooker 1` (and `2000`); `bench/gate.sh image cooker:ci`; `bench/stream-gate.sh "$TMPFS" "$OUT/storage"`; `bench/codec-gate.sh "$OLD_CODEC" "$NEW_CODEC" "$OUT/codecs"` after `--calibrate`; `bench/flow-gate.sh "$NEW" "$OUT/flow"`; `bench/container-start.sh cooker:ci "$OUT/container-start"`; by hand before a release `MIX=field bench/soak.sh target/release/cooker 60 "$OUT/soak-field"`.

Do not tune for the gate while D9, E13 and DU are open: the code the gate measures is still changing.

### 3.6 Frontend and management API split (not started)

The plan is `workspace/plans/SPLIT-API-PLAN.md` (v5, 2026-11-13) in the handoff package, not in the repository. No schedule; it starts after S1 reaches `master`.

- P0, done: `ui/dist` is no longer committed; CI builds it.
- P1, contract base: `/admin/v0`, typed responses, and the OpenStack API-SIG envelope, errors and paging. This is also a security PR: the local management API is reachable from any web page (open CORS, and 127.0.0.1 bypasses `allow-remote`).
- P2, area moves: server, TUI and dashboard in one PR each, with no aliases (ruling 2026-11-13 「API 要不直接從 v0 開始計，不要向後兼容」).
- P3: CORS allowlist and key transport.
- P4: move the dashboard to its own repository once P3 has merged and two weeks pass without an `openapi.json` change.

`/v8/management` still appears in 22 files.

## 4. Rulings that constrain the work

Full list: [OPEN-QUESTIONS.md](OPEN-QUESTIONS.md), "Settled". Rows below are the ones most likely to be violated by accident.

| Ruling | Date | Effect |
| --- | --- | --- |
| No backward compatibility (rows 14 and 26). 「没问题合并就好继续速度控制好 并且不考虑向后兼容了」 | 2026-11-13 | Config keys, database schema, routes, response shapes and file paths change when a better design needs it. No fallback, alias or shim; change every caller in the same PR. `crates/AGENTS.md` "Compatibility". |
| Live standard (`S1-LIVE-STANDARD`, A to D): 「全部照建議」 | 2026-11-15 | Realtime, Live and WebRTC store no resume data. Messages are forwarded unchanged in one ordered loop. A client disconnect closes the upstream. 4 KiB frames; ping 30 s, read deadline 60 s, write deadline 10 s, no socket limit per key; every end logs a fixed `cause`. This standard wins over docs where they differ. |
| Effort snap: 「思维强度自动吸附到最近的支持的思维强度档位上」 | 2026-11-15 | A reasoning effort the model does not support snaps to the nearest supported level (ties go lower), never 400. |
| E-Q2 resume: 「这一段参考 https://github.com/xz-dev/openai-api-cache-keeper 禁止自己发明」 | 2026-11-16 | Responses WebSocket reconnect uses `previous_response_id`; a client close cancels the in-flight turn. No invented resume. |
| Q2: 「同意每次都檢查，但是有例外，比如 responses API 這種是在我們本地作為終點」 | 2026-11-16 | Capability is re-checked at every attempt. Responses state ends locally with cooker-issued ids; remote compact support is probed on each switch to an upstream and never cached. |
| Q3: 「我們給下游自己的就好像我們自己是 provider 一樣而非只是水管路由」 | 2026-11-16 | Provider-specific fields live in a per-provider isolated map; cooker behaves as the provider toward clients. |
| Q18b: 「日志不需要持续纠结机密掩盖，这个是一个无底洞」 (R-8) | 2026-11-16 | Stop iterating on log redaction. |
| RI-MMAP and prototype scope: 「先出一个原型」「我可能拿到手还想拆一下模块和数据边界」 | 2026-11-15 | Prototype first; XZ restructures modules and data boundaries and optimizes memory afterwards. |
| R-9 to R-15: 「按照建议就行」 | 2026-11-17 | See the list below. |
| X-1 generic web search: 「事后考虑抽象为通用的 websearch 接口（我记得 OpenAI 有这个接口）」 | 2026-11-16 | Deferred. Hosted search tools such as xAI X Search still reach Responses clients as `function_call` or `custom_tool_call` named `x_*` (#181). Later: map them onto one `web_search` / `web_search_call` interface. |

R-9 to R-15 (2026-11-17, all accepted as recommended):

- R-9: for high-risk main-path units, from the third review round on only money, secrets, hangs, wrong requests reaching an upstream and violations of a ruling block a merge; everything else goes to the deferred list.
- R-10: the production image sets `XDG_RUNTIME_DIR` and the deployment notes require a tmpfs there. The path in the repository is `/run/cooker-runtime` (`Dockerfile`, [BENCHMARKS.md](BENCHMARKS.md)), not `/run/cooker` as first written.
- R-11: AI Studio relay resume is cancelled; no live path resumes.
- R-12: Gemini Interactions, Devin, AI Studio and Meta with a custom `apply_patch` do not go through Responses for now; the skipped credentials are listed as deferred. Meta was ported in #203.
- R-13: a live-session end log records the close code only, never the reason text.
- R-14: the duplicate-crate threshold in `bench/budgets.txt` is 12, not 11 (source: webrtc-rs).
- R-15: a WebRTC call records usage only from its own data channel; a sideband response that belongs to a call records none.

Recommendations not yet confirmed by an owner (the PRs were merged under the assumed answer):

- R-16 (#198, E7c): Codex alpha search with duplicate fields answers 400; an API-key credential without a base URL is not selected for alpha search; native video status `queued` is reported as `pending`. Recommended: accept. Fragment: `changes/feat-s1-e7c-video.md`.
- R-17 (#199, E9a): `store:true` turns over HTTP use the shared egress; the setting `responses.ignore-client-store-false` is absent from `crates/ck-core/src/config/schema.json`; steering, `response.interrupt`, prewarm, WebSocket pings and the §2.3 behaviour were removed (rows in [DEFERRED-OPTIONS.md](DEFERRED-OPTIONS.md)). Recommended: accept.

Other standing rulings: Q7 (Claude `responses/compact` uses Anthropic native compaction, beta `compact-2026-09-04`); Q9 (per-model `default_max_tokens`, then registry `max_completion_tokens`; none means the candidate is skipped, no invented value); Q14 (the Claude cloak sends client bytes); Q17 (xAI `compaction_trigger` goes upstream unchanged, the gateway never fabricates compaction); Antigravity AG-1 to AG-3 (copy the CPA behaviour). The owner also wrote on 2026-11-16 that not everything needs to be finished, since parts may be reworked (「不用全干完，说不定有要返工的」); work stopped at E13 by design.

## 5. Deferred findings

Non-blocking findings set aside during review; open unless a later PR fixed one (not rechecked). The PR column is the PR whose review raised the item; "pre-existing" means the code predates it. Older, owner-recorded rows (non-consuming usage-queue reads; roles and read-only keys; v1 compatibility; hosted dashboard; Chat provenance pointers; Gemini per-modality billing; Antigravity requests the contract cannot serve; unverified compaction edges; URLs split across JSON values in logs) are in [DEFERRED-OPTIONS.md](DEFERRED-OPTIONS.md).

Money and usage first.

| # | Finding | Where | PR |
| --- | --- | --- | --- |
| 1 | Usage arriving after an earlier terminal event is not emitted at `[DONE]` | Antigravity stream end | #185 |
| 2 | Usage is lost when an error event precedes `response.failed` (pre-existing); a `response.failed`-only stream with empty output can emit `response.created` before the producer error, so dispatch may commit instead of failing over | `event.rs:252` | #186 |
| 3 | Usage inside an error event is not published; no documented shape | Anthropic stream errors | #204 |
| 4 | Usage stats switched off mid-attempt leave a deferred xAI Chat job charged without a record | deferred xAI Chat | #197 |
| 5 | Empty speech answer: dispatch treats it as an attempt failure but the usage record settles `failed=false`; video content proxy choice untested | `dispatch/attempt.rs:579`, `:357`, `:403`; `videos.rs:657` | #191 |
| 6 | xAI Chat poll failures drop upstream headers, so `Retry-After` is lost; contract video polling is accepted without `VideoJobs` admission (pre-existing); the job-table mutex is held across the pool publish | `xai_deferred.rs:32-36`; `xai_media.rs:606-654` | #197, #183, #196 |
| 7 | `xai-images` has no billing-dimension rows; the legacy no-usage fallback writes zero counters for ID-only creates and suppressed polls; durable job state across restarts accepted without a ledger (F06-1) | usage records | #172 |
| 8 | Kimi usage: top-level `cached_tokens` unread; `executor_identity` maps only `kimi`; request TTL read before payload rules; shared-prompt rule applied to every Chat upstream and `Usage::choice` takes the last choice; multi-choice `reasoning_tokens` sum untested; no "usage has no bucket" guard in the TTL fill; streamed legacy merge may keep a top-level TTL split; F01 reasoning read only from top-level `output_tokens_details`; with `include_usage: true` and usage only in `choices[0].usage` no aggregate chunk is emitted | `usage_record.rs:574-645, 805-812, 1137-1161, 1215, 2078, 2250-2258`; `usage_snapshot.rs:288-307`; `kimi/chat.rs:40`; `kimi/responses.rs:104`; `kimi_exec.rs:250-263`; `response.rs:373-375` | #188, #189 |
| 9 | After `Tracker::canonical` replaces the detail, apply the Chat request TTL and the Responses applied TTL; the Chat codec writes no `cache_write_by_ttl` | `usage_record.rs` | #189 |

Responses and protocol codecs.

| # | Finding | Where | PR |
| --- | --- | --- | --- |
| 10 | `history::project` refuses calls carrying `namespace` although request egress supports it; `arguments_format: invalid_json` fails projection; `Annotation`, `Grounding`, `UrlContext` and server tool records are dropped (confirm non-replayable); projected-document binding round trip untested | `history.rs:326-328`; `responses/request/write.rs:268-270`; `splice.rs:300-315` | #166 |
| 11 | Producer identity is `auth_index`: moving a credential file or changing `base_url` marks stored reasoning foreign; `prune` is O(n squared); `withhold` can 503 near `max_metadata_bytes` | `attempt.rs:161`; `admit.rs:88`; `attempt.rs:185` | #166 |
| 12 | String `input` becomes message `content:"hi"` instead of an `input_text` array (pre-existing) | `responses/request/write.rs:288-291` | #169 |
| 13 | Responses clients get `server_error`, not the typed code, from the shared envelope; Antigravity compact still returns a zero-send 400 despite OPEN-QUESTIONS row 70 | error envelope; Antigravity compact | #204, #187 |
| 14 | `responses.ignore-client-store-false` is absent from the config schema, so config import and DB load reject it; `docs/CONFIGURATION.md:285` still says keepalive applies to the Responses WebSocket; failover needs `request-retry` of 1 or more; interrupt error wording | `schema.json`; `scheduling/classify.rs:372`; `websocket.rs:414` | #199 |
| 15 | Kimi `CountTokens` `Destination` skips Responses clients; F31: an upstream count answering 200 without a number becomes an opaque 502, and there is no `/backend-api/codex/responses/input_tokens` counterpart of `compact` | `kimi_exec.rs:44`; `attempt.rs:241`; `lib.rs:91-93` | #203, #205 |
| 16 | `Events::inlined` was unpinned by tests (deleted in #207); converted frames end in an extra blank line | `codex_events.rs:167-174` | #203 |
| 17 | Gemini signature bindings fail reconstruction and replay loses interleaving (#180 may have fixed it); Codex tool events drop SSE source fields and unknown members; exponent-form rate-limit durations lose reset time; xAI: `n:1` is refused by the fence although LangChain sends it, the fence message names no field, tool limit 1000 refused in `shape_tools`, `validate_xai` `/n`, `/seed`, `/stop` is dead code, no test for `compaction_trigger` or the `Ingress::xai` gate | `codec/provenance.rs:74,385-403` | #171, #181 |

Media, live sessions and relay.

| # | Finding | Where | PR |
| --- | --- | --- | --- |
| 18 | 32 MiB alpha response cap untested; `content_url` missing versus invalid differs only in message text; `codex_go.json` `images` section unread | `codex_alpha.rs:74`; `codex.rs:690`; `xai_media.rs:411-421` | #198 |
| 19 | Unreachable speech and old-arm residue: `XaiExecutor::speech`, `dispatch.rs:1639`, `speech.rs:136`; delete in D9 or E13. Docs: `docs/STREAM-GOLDEN-CHANGES.md:23` omits `speech_commits_at_its_first_audio_byte`; `speech_go.json` re-serialized with `\uXXXX`; `docs/INTERNAL-STREAM.md` line 17 references shifted | `xai_media.rs:534` | #191 |
| 20 | xAI extend `seconds` refusal, `video.request.value` shape, content `variant`, OpenAI-shaped job fields; Images edit contract unchecked against official docs; uploads still copy whole bodies (DU) | video and images codecs | #161, #175 |
| 21 | W2 must supply the liveness deadline when a read-ahead slot is occupied behind a stalled write, plus a distinct client-disconnect terminal; W6 must reconcile the 8,192-byte final-record reservation with 4,096-byte frames | live sessions | #177 |
| 22 | WebRTC: no write timeout on the call's client path; weak liveness assert; `PassThrough.events()` returns `None` | `session.rs:316-329`; `media_tests.rs:323`; `relay.rs:337-339` | #195 |
| 23 | AI Studio relay: relay keys compared without constant time; a key change between auth and registration leaves the session alive; a stored `oauth.providers.aistudio.ws-auth` fails startup (add the offline `cooker config import` step to the notes); docs omit `auth_token` as a relay-key source; raw browser disconnect text reaches clients when no other credential is available | `net/access.rs:44-52,76`; `aistudio_relay.rs:91`; `runtime/state.rs:431-432`; `config_store.rs:292-294` | #190 |

Anthropic, tests and hygiene.

| # | Finding | Where | PR |
| --- | --- | --- | --- |
| 24 | G5 conflicts with OPEN-QUESTIONS row 66 on a cloak-shaped replay (record in OPEN-QUESTIONS or get a ruling); Antigravity and Devin old arms are not fenced for a block-carrying request; a replayed block with thinking enabled but no `compaction` member still gets the cloak's `context_management`; `usage.iterations` may drop top-level cache counts; `claude.rs` is 2,381 lines with no size ratchet | `usage.rs:115-140` | #160 |
| 25 | F18 model list: 400s bypass the Anthropic envelope; order is display name ascending where the official page says newest first; `lifecycle` query ignored; duplicated setup | `models.rs:59,61-64,67,70-73,347` | #202 |
| 26 | `Executors::fence` was deleted in #207; `refence` is the production check | `ck-exec/src/lib.rs` | #207 |
| 27 | Q-H "invents max tokens" predicate duplicated in `invents_max_tokens` (ck-translate golden) and `guesses_max_tokens` (ck-exec go_exec); `http/errors.rs:88-92` falls back to `api_error` in the Interactions shape; `payload_max_tokens` re-runs payload rules per `capability()` call; F22 test readability and duplicated `model_required()`; test hygiene in `golden.rs:423`, `exec_antigravity.rs:323`; `crates/ck-translate/tests/request_overreach.rs` uses `#[path]` module copies | | #179, #184 |
| 28 | Redaction: an earlier unmatched quote defeats JSON-string tail redaction; a malformed double quote leaks; request-log file names keep path URLs; spool failure drops data; CRLF becomes LF. Pre-existing; ruling R-8 says stop iterating | `ck-wire/src/redact_scan.rs:163-184`; `request_logging.rs:1311`; `request_logging_spool.rs:27-34`; `ck-exec/src/proxy.rs:1350` | #157 |
| 29 | Kimi: `output_config` effort, F43 proof on errors, model canonicalization; centralized proof stripping misses tool-result and reasoning-summary parts; W1 follow-up: frame table as contiguous ranges | | #149, #174 |
| 30 | Required by owner: E8 session-storage lifecycle review if a same-class finding appears; `alloc_budget` `large_requests` ignored (section 3.5) | | #177, #179 |
| 31 | Docs still pointing at deleted code after #207: `docs/parity-audit/status.md:300-301` (rows M2-0035, M2-0036), `docs/INTERNAL-STREAM.md:827-828`; the key `requests.streaming.keepalive-seconds` and the `keepalive` parameter of `respond::stream` and `resume` have no production reader | | #207 |
| 32 | T1 (#210): a deadline value near `i64::MAX` passes validation but panics on `Instant + Duration` (`crates/ck-exec/src/pipeline.rs:93-95`; clamp in `read` or use `checked_add`); `schema.json` was edited by hand, so run `generate_schema.py` once against the pinned Go checkout; the count-tokens local path also sits under the header deadline | `pipeline.rs`; `schema.json` | #210 |
| 33 | F34 (#211): once a Codex WebSocket accepted a turn nothing is resent, and the next turn retries the socket; the official client remembers HTTP for the rest of the session. Follow-up: do the same | Codex WebSocket handshake | #211 |

## 6. Merged in this phase

Merged into `feat/s1-execution`. Earlier units (E0 to E6b, E9b, E9c, D2-1, D2-2, D2-4 to D2-6a, C2, F13, F14, Q18b and the P7 credential and effort fixes) are listed by `gh pr list -R prep-cook/cooker --state merged --limit 200`.

| PR | Purpose |
| --- | --- |
| #170 | Pass and isolate Antigravity Claude signatures |
| #171 | Realtime projector and checker (E10a) |
| #172 | Keep xAI and Codex billed units and xAI media usage (F05, F06) |
| #173 | Fail attempts whose upstream body ends mid-line (P7-4) |
| #175 | Images, speech and video jobs through the executor (E6) |
| #176 | Store Responses turns locally and expand continuations (D2-6b) |
| #177 | Live-session producer and per-message admission (E8) |
| #178 | Complete Responses and Anthropic response shapes |
| #179 | Chat, Anthropic and Gemini served through their codecs (D2-3a) |
| #180 | Restore Gemini signature bindings in stored history |
| #181 | Preserve xAI controls and response content (F12, F24, F26) |
| #182 | Make two load-sensitive tests deterministic |
| #183 | Record usage once per scope and charge video jobs once (EU) |
| #184 | Route reusable prompts to native Responses upstreams (F22) |
| #185 | Keep only reported usage when an Antigravity stream ends (P7-4-1) |
| #186 | Keep usage and classification of failed Responses streams (F07) |
| #187 | Responses and compact served through their codec (D2-3b) |
| #188 | Preserve Chat choice usage without aggregate opt-in (F42) |
| #189 | Bill compaction iterations and Kimi choice usage and TTLs (F01, F08, F09) |
| #190 | Require relay keys to register AI Studio browsers (E12) |
| #191 | Stream speech from its first byte and scope video jobs (E7a) |
| #192 | Realtime sockets as live sessions with usage (E10b) |
| #193 | Each test runtime opens a private stream pool directory |
| #195 | Relay WebRTC calls as live sessions with usage (E11) |
| #196 | Images and deferred image jobs on the contract (E7b) |
| #197 | xAI Chat deferred jobs from the job table (F25) |
| #198 | Video jobs, content and alpha search on the contract (E7c) |
| #199 | Responses WebSocket turns on the exchange path (E9a) |
| #200 | Give the stream pool a runtime directory |
| #202 | Page the Anthropic model list with cursors and limits (F18) |
| #203 | Meta `apply_patch` through the codec path (D2-7') |
| #204 | Type Anthropic stream errors and keep `between_tools` (F15 to F17) |
| #205 | Count Responses input tokens with the destination, upstream counts only (F31) |
| #206 | Serve local Responses GET, cancel, DELETE and background turns (F30) |
| #207 | Delete old-path code that no route reaches (D9 part 1) |
| #208 | Pin one usage record and socket reuse when an xAI WebSocket turn ends on `response.incomplete` (F11, test only) |
| #209 | End xAI and Meta Responses streams with exactly one `data: [DONE]` (F27) |
| #210 | Upstream stream deadlines: before headers, idle, total (T1); being merged |
| #211 | Complete the turn over HTTP when the Codex WebSocket handshake gets 426 (F34) |

Closed without merging: #194 (replaced by #196) and #201 (base fix, included in `fb730ce1` inside #187). Cut or replaced: D2-6k key ring (branch kept), D2-7, EB (replaced by W1), EF browser resume (cancelled, R-11).

## 7. 中文摘要（给 XZ）

状态：S1 的所有路由族已经走新路径，代码都在 `feat/s1-execution` 分支，`master` 没有任何改动；合并到 `master` 和发布都需要 owner 批准。#174（W1）等你自己审，在你通过之前不要合并。F11、F27、F34（#208、#209、#211）已合并，T1（#210，上游流超时）正在合并。`feat/s1-execution` 还缺 9 个 `master` 提交，会先开一个把 `master` 合进来的 PR，然后开 `feat/s1-execution` 到 `master` 的 PR 交给 owner；owner 裁定：剩余单元完成且 CI 通过后合并，性能门禁由 owner 之后自己跑。

交接给你的三件事：

1. D9 主删除（含之后的 E13）。#203 和 #207 之后，Chat、Anthropic、Gemini 请求仍有六种情况进入旧的 `rt.executors.execute`：强制映射的模型别名、Devin／AI Studio／Gemini Interactions、Kimi 的 Claude 格式请求和 count tokens、Claude 的 thinking replay、Meta 在非 Responses 客户端上的自定义 `apply_patch`、xAI 延迟 Chat 任务。在它们迁移或被拒绝之前，`translate.rs`、旧的各 provider `execute`、`UsageSink` 响应方法和 `Call::body` 都不能删。建议：别名、Kimi、Claude replay、xAI 延迟任务迁到 codec 路径（别名最小，只需在 Chat、Anthropic、Gemini 的 egress 写 `exchange.model`）；Devin、AI Studio、Gemini Interactions 保留为一个窄的遗留模块，等它们的 codec 完成；Meta 自定义 `apply_patch` 用 400 拒绝。五项并行约一个工作日，之后再删除。
2. DU（scratch 直接上传）。九个 executor 的发送前处理都对整个请求体做文本编辑，需要连续的 `&[u8]`，而 lease 由多个 4 KiB 帧组成，所以不是去掉一次复制，而是重写这些流水线。HTTP/2 下流式请求体还会失去客户端库对 GOAWAY 的透明重发。建议方案 B（对 lease 做流式 JSON 补丁，每个 executor 族一个 PR，从 Meta 开始），并在 proxy 中自己实现被拒绝跳转的重发（每个请求最多 2 次）。方案 A（编码时调整）改动最大、终态最干净；方案 C（尽量连续、写时复制）最小但不保证零复制。
3. #174 与 RI-MMAP。#174 合并后，只把记录索引移进 mmap pool，不合并 codec 节点。

性能目标（性能门禁由 owner 自己执行，测试本身允许失败）：codec 与 pipeline 中位数在 Go 时代基线的 ±5% 以内；流存储不得比堆内存差，也不能回退；在 tmpfs 上测量，方法见 `docs/BENCHMARKS.md`。二进制、空闲和堆分配的具体数值见第 3.5 节。

并发容量估算（未经测量，仅供规划）：内存上界约为每个在途请求体大小的 2.5 倍。1C1G 约 200 到 250 个大请求，或 800 到 1,000 个小请求；2C4G 约 1,200 个大请求；4C8G 约 2,500 个大请求。需要留意 nofile 限制，并把 stream pool 所在的 tmpfs 计入内存限制。

另有一项未开始的工作：前端与管理 API 拆分（第 3.6 节，计划在交接包的 `workspace/plans/SPLIT-API-PLAN.md`）。P0 已完成，P1 先定 `/admin/v0` 契约并修复本地管理 API 可被任意网页访问的问题，之后依次是区域迁移、CORS 白名单、仪表盘独立仓库；S1 进入 `master` 之后再开始，没有排期。

仍需确认的裁定：R-16、R-17（建议接受）、R-18（DU 的方案选择）、R-19（D9 六种情况的处理）。
