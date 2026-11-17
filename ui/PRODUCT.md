# cooker dashboard

The dashboard is the management interface embedded in cooker, a Rust rewrite of CLIProxyAPI. The server binary serves it at `/management.html`, and the dashboard talks only to that server: the Management API (`/v8/management` and `/admin/v1`), plus `GET /v1/models` with a client key when the Use with tools dialog on the Client keys page tests the key. The dashboard is the most visible part of the launch, so it must look authored and remain quick to use.

## Operator and environment

The operator is one developer or a small team. The proxy runs on a laptop, a home server or a small VPS, and coding agents send it requests all day. The dashboard sits in a browser tab next to a terminal and an editor, often late at night. On a phone, the operator uses it to check status or toggle one switch.

The operator opens the dashboard with these questions, in this order: whether traffic is flowing; which credentials are ready, cooling down, disabled or failing; how to connect another account; how to change a setting without breaking anything; and what the logs say.

## Contract

- The data contract is Go CLIProxyAPI's v8 Management API at commit 6fecc6e (v8.0.10). The UI is built to Go's request and response shapes.
- The server may be the Rust binary, which does not implement every management route yet. Like Go, it answers a route it lacks with an empty 404. On servers other than Go, the UI sends one probe per write action, a request Go rejects with 400 before any side effect. It shows actions the server lacks as disabled, with one line naming them. Reads that fail this way show a "not available on this server" state that names the route.

## Non-negotiables

- No invented data. Every read shows loading, empty, error, not available, or data from the server. Telemetry comes from server-side `recent_requests` buckets; the destructive usage queue is opt-in and stays in the tab.
- The management key is sent once, at sign-in, and exchanged for a server session held in an `HttpOnly` cookie, so a reload stays signed in and no script on the page can read the key or the session id. The session lasts 7 days, or 30 when the operator ticks "Remember on this device"; the key stays in the browser's password manager only when the operator saves it there. Sign-out, in this tab or another, revokes the session on the server and cancels any sign-in in progress in any tab. Changing `management.secret-key` or restarting the server ends every session.
- Accessibility: a complete keyboard path, visible focus, WCAG AA contrast in both themes, no status shown by colour alone, and reduced motion respected.
- A small, fast bundle: the gzip ceilings for the initial JavaScript, the initial CSS and the largest lazy chunk are in `tools/size-budget.json`, with the rule in `REQUIREMENTS.md`. They are ceilings with headroom, raised only when a change needs it; `npm run check` and CI fail past them.

## Scope

The dashboard covers Overview, accounts (credentials with their quotas) and connecting accounts (OAuth, device codes, callback paste, Vertex import), provider API keys, client keys, models (aliases, exclusions, catalog), payload rules, configuration (JSON and YAML with a reviewed diff and a stale-write check), logs, usage and system.

Out of scope: accounts or billing, a historical telemetry database, invented Rust-only endpoints, and process metrics the API does not expose.
