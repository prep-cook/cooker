# cooker dashboard

The management dashboard for cooker. It uses React 19 and the Astryx design system, and calls cooker's management API under `/v8/management` and `/admin/v1`, whose contract is the Rust router in `crates/ck-server/src/management/api.rs`. The Rust binary embeds the build output (`crates/ck-server/build.rs`) and serves it at `/management.html`. The dashboard ships only inside that binary.

Product rules are in [PRODUCT.md](PRODUCT.md); the visual system is in [DESIGN.md](DESIGN.md).

## Commands

Use Node 24, as the Dockerfile does; the minimum is Node 22.12.

```sh
npm ci
npm run dev        # Vite on :5173, proxies /v8 and /healthz to 127.0.0.1:8317 (override with CK_BACKEND=...)
npm run typecheck  # tsc --noEmit, 0 errors expected
npm test           # src/ unit tests, credential-store checks and mock server contract tests
npm run build      # vite build into dist/
npm run check:api  # src/lib/api/routes.ts against the Rust management router
```

`ui/dist` is not committed. The Rust binary embeds it at compile time, so run `npm run build` before `cargo build`. `docker buildx build` builds it in the Dockerfile's `dashboard` stage.

`npm run check:api` reads `fn router` in `crates/ck-server/src/management/api.rs`. It exits 1 when the registry names a route the server lacks, or when a `/v8/management` route has neither a registry entry nor a line with a reason in `tools/api-allowlist.json`. It exits 2 when the router uses a construct it cannot follow.

## Structure

The table describes the React dashboard source. Code rules are in [CONVENTIONS.md](CONVENTIONS.md).

| Path | Role |
| --- | --- |
| `src/main.tsx` | Entry point. When the page starts in Chinese, it loads Astryx's Chinese strings first, then renders `App`. |
| `src/app/` | Providers, sign-in, the shell (`AppShell` and the side nav), the page frame `PageFrame.tsx`, the route table `routes.ts` with hash routing and one lazy chunk per page, the page error boundary, and the theme and language switches. |
| `src/features/<page>/` | One directory per screen: the page, its query hooks in `queries.ts`, and tests. |
| `src/components/` | Parts used by two or more screens, such as the status lamp, the code editor and the chart wrappers. |
| `src/lib/api/` | One module per Management API area and one typed function per route; `routes.ts` is the registry that `check:api` compares with the Rust router. |
| `src/lib/` | The i18n runtime (`i18n.ts`), `notify()`, the session (sign-in, sign-in help, capability probes) and pure domain logic with tests. |
| `src/i18n/` | Message catalogs, one per area: English in `<area>.ts`, shipped with the app; translations in `zh-CN/` and `zh-TW/`, each locale loaded on demand as one chunk through its `index.ts`. |
| `src/themes/layers.css` | The cascade layer order, imported first: reset, Astryx components, the project's StyleX (`app`), theme. |
| `src/themes/site/` | The one theme in three palettes: `siteNeutralTheme.ts` (Astryx neutral colours, the default, built to `site-neutral.*`), `siteTheme.ts` (Rosé Pine Moon, built to `site.*`) and `siteMainTheme.ts` (Rosé Pine, built to `site-main.*`). The last two load on demand. All three are sources that extend `@astryxdesign/theme-neutral` with tokens only (fonts, icons and components are neutral's), and are rebuilt with `astryx theme build`. |

## Honest states

Every read renders loading, empty, error, not available, or data. The dashboard reads the server build from response headers: cooker sends `X-Ck-Version: cooker-<version>`, `X-Ck-Commit` and `X-Ck-Build-Date`.

The dashboard probes other servers once per session for the write actions a screen offers. A route that does not exist answers with gin's empty 404 (or 501 or 405), and the UI cannot distinguish that response from a real route without asking, because `OPTIONS` returns 204 everywhere. Each probe is therefore a request Go rejects with 400 during input validation, before any side effect: an empty JSON body to `POST /credentials`, `POST /credentials/refresh`, `PATCH /credentials/fields`, `DELETE /credentials`, `POST /routing/cooldown/reset`, `POST /requests/api-call` and `POST /oauth/import`; `GET /oauth/auth-url` without a provider; and `GET /observability/usage/queue?count=0`. Each probe was checked against Go 6fecc6e to return 400 with the auth directory and config unchanged. An action whose probe meets an empty 404 renders disabled and is named in one line; a read the server lacks shows "Not available on this server" with the route and status code. `DELETE /observability/logs` cannot be probed safely, so Clear is disabled when the log read is unavailable; otherwise it marks itself off after a first empty 404.

Traffic comes from `recent_requests` in `GET /credentials` and `GET /observability/usage/api-keys` (twenty 10-minute buckets, reported by the server). The live usage view reads `GET /observability/usage/queue`, which removes records for other consumers. It is opt-in, asks first, and keeps events in the tab only.

## Endpoints

Paths that start with `/admin/v1` are under that prefix; all other paths are under `/v8/management`. Every screen also uses `GET /admin/v1/effective/config` and `GET /credentials`. Every configuration write is a `POST /admin/v1/apply`.

| Screen | Endpoints |
| --- | --- |
| Overview | `GET /credentials` every 10 s while visible |
| Accounts | `GET /credentials` every 10 s while visible; `POST /credentials` (upload); `DELETE /credentials`; `GET /credentials/download`; `GET /credentials/models`; `PATCH /credentials/status`; `PATCH /credentials/fields`; `POST /credentials/refresh`; `POST /routing/cooldown/reset`. Quota checks run by button only, one at a time and at most once per account every 2 minutes: `POST /requests/api-call` (Claude, Codex, Kimi usage). Batch actions send one request per account, except one `DELETE /credentials` for all. |
| Connect | `GET /oauth/auth-url`; `GET /oauth/status` every 2 s while waiting; `POST /oauth/callback`; `DELETE /oauth/session`; `POST /oauth/import?provider=vertex` |
| Providers | `GET /observability/usage/api-keys`; `GET /admin/v1/export` when the user reveals a key |
| Client keys | `GET /admin/v1/r/client-key`; `GET /admin/v1/export` when the user reveals a key |
| Models | `GET /routing/model-definitions/<channel>` |
| Payload rules | The shared configuration routes only |
| Usage | `GET /observability/usage/api-keys`; opt-in `GET /observability/usage/queue?count=500` |
| Logs | `GET /observability/logs` (cursor, every 3 s); `DELETE /observability/logs`; `GET /observability/logs/errors`; `GET /observability/logs/errors/<name>`; `GET /observability/logs/requests/<id>` |
| Configuration | The shared configuration routes only |
| System | `GET /server/latest-version` on request |

Each operation in an apply request carries the value (`expect`) or revision (`if_rev`) that the page showed. If the server answers `412`, the value changed after the page showed it; the dashboard writes nothing and reports the change.

## Verification scripts

Use these scripts only with disposable servers and fake credentials; they edit configuration and credential files. The fixture requires management key `orb-dashboard-test-only`, a disabled fake Claude credential with email `operator@example.invalid`, routing strategy `round-robin`, and an empty `requests.payload.default`.

- `node scripts/browser-check-rust.mjs [url] [capture-dir]` drives the dashboard with agent-browser against the Rust binary (or Go). It checks nine screens in both themes, unsupported actions disabled on Rust, a payload rule written and removed, a YAML diff discarded, the 390 px layout, and no key in storage.
- `node scripts/fake-logins.mjs [port]` serves fake provider login endpoints (Claude, Codex, Kimi, Meta token, profile, device-code and key-mint routes) and a Claude usage endpoint that echoes its `Authorization` header. Use fake tokens and `@example.invalid` emails only.
- `scripts/login-harness/` is a test build of cooker whose only change is `Options.login_base`, which points every provider login endpoint at the fake. It is never shipped. Build it with `cp ../../../Cargo.lock . && CARGO_TARGET_DIR=../../../target cargo build` inside that directory.
- `CHROME_PATH=... node scripts/oauth-check.mjs <url> --mode fake` drives Claude and Codex sign-in through a pasted callback URL, and Kimi and Meta through the device code, against the harness. It checks that each new credential appears and that a provider the server lacks gets an honest message. `--mode dead` runs against the shipped binary with a dead `requests.proxy-url` and checks that a pasted callback ends in an honest failure.
- The same script's `--mode stub` uses fake management responses to check relative login URLs, absolute HTTP(S) links, Copy link, guide navigation, cancellation, unsafe-scheme rejection, and a sign-in the server forgot while the tab was hidden (Go and servers before 0.4.2 keep a finished sign-in for a minute). It checks that the status is read as soon as the tab is visible again; that only one new credential name of exactly that built-in provider, between two successful reads, counts as connected; and that a rewritten file, a failed read, or a `kimi` credential for a `kimi-ai` sign-in gives "Result unavailable". It also checks that a status reply delayed past a cancel and a new start leaves the new sign-in alone. Run it against both `/management.html` and a prefixed path such as `/proxy/team/management.html`; it makes no provider calls.
- `node scripts/api-call-check.mjs <base> [<base> ...]` sends the Accounts screen's `POST /requests/api-call` to the fake usage endpoint through each server. It checks `$TOKEN$` substitution and that the quota parser reads the windows, and compares the response shape between servers.
- `CHROME_PATH=... node scripts/quota-check.mjs [dashboard-url] [capture-dir]` drives Overview and Accounts with fake management responses (Playwright, like `oauth-check.mjs`). It checks that failed checks retain live or passive limits and their timestamps, that Claude 403 advice explains setup-token permissions, that a successful retry clears errors, that a second check within 2 minutes sends nothing, that the drawer's Check quota re-enables by itself after its 2 minutes, and that a partial batch delete keeps the failed account selected and names it. It makes no provider calls.
- `CHROME_PATH=... node scripts/first-run-check.mjs <url> mock [capture-dir]` walks first-run setup against `tools/mock/server.mjs --accounts 0` through `vite preview`. It waits for the client key input to retain focus across two consecutive animation frames before typing and submitting with Enter, then checks tool setup and the completed overview in both themes at desktop and phone widths.
- The checks that sign in or send through a real server (`oauth-check.mjs` fake and dead modes, `first-run-check.mjs`, `api-call-check.mjs`) refuse to start unless the server's credential store has a real path, symlinks resolved, strictly inside the system temp dir's real path. The credential store is the `auth_dir` that `GET /admin/v1/credentials` reports (the `<db>.credentials` directory beside its database). `scripts/private-credential-store.mjs` performs the check and also refuses a path that does not resolve. `npm test` covers it with disposable symlinks. Credentials live only in the server's database, so start the server with `--db` or `CK_DB` under a temp dir, and set `HOME` to a temp dir as well; `isolated.sh` isolates the network, not the filesystem. `first-run-check.mjs` alone runs against `tools/mock/server.mjs --accounts 0` and skips the check for it (the mock answers `X-Ck-Version: cooker-mock` and keeps accounts in memory).
- `scripts/verify-backend.mjs` is the API-level verifier for the Go release backend.

## Security

Sign-in sends the management key once, to `POST /v8/management/session`. The server answers with an `HttpOnly; SameSite=Strict` session cookie that every later request carries (`credentials: "same-origin"`, no key header) together with `X-Ck-Dashboard: 1`. The server refuses a cookie request without that header, which an image, form, link or other origin cannot send; `curl` with the cookie no longer works, and command-line clients send the key. No copy of the key stays in memory or storage. The session lasts 7 days, or 30 when "Remember on this device" is ticked; a reload resumes it through `GET /v8/management/session`. Sign-out revokes it on the server and, once the server has confirmed, tells the other tabs over `BroadcastChannel` (`src/lib/keyStore.ts`); those tabs return to the sign-in page without a request of their own. When the server does not confirm, the tab says so and stays signed in. Otherwise the dashboard stores only the theme mode, the palette and the language (`src/app/preferences.ts`), plus the first-run card's flags (`src/lib/firstRun.ts`). Downloaded credential files and configuration previews contain secrets; treat them accordingly. Do not use the dashboard over plain HTTP across a network.

## Fonts

The neutral theme's own fonts: Figtree (body and headings, SIL Open Font License 1.1) and the system monospace stack, as `@astryxdesign/theme-neutral` (MIT) sets them. The theme package bundles no font files, and its `fonts.css` loads Figtree from Google Fonts at run time. `src/themes/site/index.ts` imports the same weights (400 to 700) from `@fontsource/figtree` (OFL 1.1) instead, so the dashboard makes no third-party request. CJK text uses the system fonts.

## Colours

Rosé Pine palette (MIT, https://github.com/rose-pine/palette); the values are unmodified in `src/themes/site/rosePine.ts`.
