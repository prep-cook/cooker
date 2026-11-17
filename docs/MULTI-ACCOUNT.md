# Several accounts

This guide describes routing across your own accounts and API keys on your own machines. Do not pool other people's subscriptions. Check each provider's terms before you start. The proxy must already be running ([GETTING-STARTED.md](GETTING-STARTED.md)).

## Add the accounts

Sign in to each account once, from the dashboard (Connect account) or the command line. While the server runs, the command-line sign-in goes through the server: `cooker --management-base-url http://127.0.0.1:8317 --claude-login` (or `--codex-login`). The command reads the management key from `CK_MANAGEMENT_KEY` or asks for it. With the server stopped, `cooker --db /path/to/cooker.db --claude-login` writes the server's database directly. Every sign-in becomes a credential in SQLite. The proxy does not watch a directory for account files.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/credentials-dark.png">
  <img src="img/credentials-light.png" alt="The Accounts page: two Claude and three Codex accounts with their state, session and weekly quota, and recent requests" width="880">
</picture>

The provider's sign-in page uses the account that your browser is signed in to. To add a second Claude account, sign out of claude.ai first, or open the sign-in link in a private window or another browser profile. `--no-browser` prints the link instead of opening it. The dashboard's Accounts page lists every account and its state. You can disable an account there without deleting it.

## Routing strategies

`routing.strategy` decides which ready account serves the next request.

| Strategy | What it does | Use it when |
|---|---|---|
| `round-robin` | Takes the accounts in turn, one request each. | The accounts are alike and you want even load distribution. |
| `fill-first` | Uses one account until it cools down or reaches a limit, then the next. | You prefer one account and keep another as a fallback. |
| `weighted-round-robin` | Takes the accounts in turn in proportion to each account's `weight`. The strategy leaves out an account with weight 0. | The plans differ in size. |
| `soonest-reset` (default, alias `reset-first`, a cooker addition) | Selects the account whose weekly window resets soonest, until it cools down or reaches a limit, then the next soonest. An account without a known reset gets one request to learn it. | You have several subscriptions and want each one's quota used before its week resets. |

`soonest-reset` is the strategy when `routing.strategy` is unset or empty. It spends the subscription whose weekly window frees up soonest, so quota that would otherwise go unused when the window resets is spent first. Reset times come from the usage headers of each account's last response (`anthropic-ratelimit-unified-*` for Claude, `x-codex-*` for Codex). Among the ready accounts with the highest priority, the strategy orders accounts as follows:

- Accounts that have used up a usage window come last.
- An account without a known reset gets one probe request to learn it, then ranks by what that response reported.
- Accounts with a known reset are ordered earliest reset first. Accounts whose responses report no reset come after them.
- Accounts that rank the same take turns, one request each.

Accounts that never report a reset, such as API keys and providers without usage headers, get one probe request each and then rank the same. After that first request, they take turns as with `round-robin`. When several providers serve the same model, the first provider in the request's provider order is used until none of its accounts is ready, as with `fill-first`. `round-robin` spreads requests across the providers instead.

With session affinity on, a conversation stays on the account that served its first request while that account is ready. The strategy picks the account for new conversations and for a conversation whose account cools down or reaches a limit.

To take the accounts in turn, set the strategy explicitly:

```yaml
routing:
  strategy: round-robin
```

CLIProxyAPI has no `soonest-reset`: it reads that value, and an unset strategy, as `round-robin`.

An account whose rest after a stated reset has ended (at most one hour by default; see [Cooldowns and limits](#cooldowns-and-limits)) still reports its window as used up in its last usage headers. `soonest-reset` therefore sorts that account last and checks it only when the other accounts are used up or cooling.

Change the strategy on the dashboard's Settings page or through `/admin/v1`. The YAML examples here use the YAML shape described in [Configuration](CONFIGURATION.md). Editing an imported YAML file does not change the running server.

Each account can also have a `priority` (default 0). The proxy uses the ready accounts with the highest priority first, then lower-priority accounts once every higher-priority account is cooling down or disabled. Set Priority and Weight in the account's panel on the Accounts page. The corresponding legacy import fields are top-level `"priority"` and `"weight"`. Ongoing edits are persisted in SQLite separately from config revisions.

## Session affinity

Enable affinity on the Settings page. The equivalent YAML is:

```yaml
routing:
  session-affinity: true
```

With affinity on, a conversation stays on the account that served its first request. Providers keep a prompt cache per account, so the next turn of a conversation on the same account reads the long shared beginning from cache. That is cheaper against your limits, and the reply starts sooner. Moving a conversation to another account discards that cache. The proxy recognises a conversation from the session headers that Claude Code, Codex, OpenCode, and similar tools send. When a request has no session header, the proxy matches its messages against the conversations it has seen for the same client key and model. A conversation that grows keeps its account. A branch of an earlier conversation stays on that conversation's account. A conversation whose early history was summarised (compacted) is recognised from the turns it kept. A request that matches no earlier conversation starts a new one, and the proxy matches its next turns against that conversation. Message matching needs a client key (`access.api-keys`) and at least one message that is not a system message. A request without either is identified by its instructions and first user message. A request with no user message is identified by a hash of its first messages.

When `session-affinity` is omitted, both cooker and CLIProxyAPI default to off. Affinity combines with the routing strategy: a bound conversation keeps its account, and new conversations follow the strategy. When the bound account cools down or is disabled, the conversation moves to another account. `session-affinity-ttl` (default `"1h"`) controls how long an idle conversation keeps its account. `session-affinity-subagents` (default `true`) binds child sessions to their parent's account too.

## Cooldowns and limits

When a provider answers 429 (too many requests, or a used-up limit), the proxy rests that account for that model and sends the request to the next ready account. The length of the rest depends on the response:

- If the provider states when to try again (a `Retry-After` header or a stated reset), the account rests for that long, at least 10 seconds but at most one hour at first ([`max-trusted-cooldown`](CONFIGURATION.md#routing)). The next request then checks the account; while that check runs, other requests go elsewhere. If the account is still limited, the next rest doubles, up to the stated reset. The dashboard shows such an account as "Limited until" the stated reset, with the time of the next check.
- Without a stated retry time, the rest starts at one second and doubles on each further 429, up to 30 minutes.
- An error that reports the whole account as out of quota rests the account for every model.
- A rejected token (401 or 403) rests the account for 30 minutes. The dashboard marks it "Sign in again" when the sign-in has expired or was revoked.
- A successful request on the account clears its cooldown for that model.

`routing.retry.request-retry` (default 0) adds rounds over the accounts after every attempt in a round fails. `routing.retry.max-retry-interval` (seconds, default 0) makes the proxy wait for an account to leave cooldown instead of failing immediately. The Reset cooldown action in an account's panel on the Accounts page clears only the proxy's own record. It does not give back any provider limit.

The optional [fail over only on 429 preset](CONFIGURATION.md#optional-preset-fail-over-only-on-429) keeps 429 failover for OAuth accounts. It stops ordinary failover after a failed attempt for the listed non-429 statuses. Credential-preparation failures can still cause another attempt.

## Read the quota view

Press Check quotas on the Accounts page, or Check quota in an account's panel, to ask each account's provider for its usage. For a Claude account, the page shows the rows that Claude Code's own `/usage` shows:

- Current session: the 5-hour window.
- Current week (all models): the weekly limit.
- Current week (Fable only), (Opus only), and similar rows: a separate weekly cap for one model, when the plan has one.
- Extra usage: paid usage beyond the plan, in dollars, when it is turned on.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/quotas-dark.png">
  <img src="img/quotas-light.png" alt="A Claude account's panel on the Accounts page: its current session and weekly limits, then its settings" width="880">
</picture>

Each row shows how much is used and when it resets. Codex accounts show their 5-hour and weekly windows. Between checks, the proxy also reads the limit headers that Codex sends with every response, so the Overview can show Codex limits without a check.

## Per-account proxies and request shaping

Edit these credential metadata fields through the account's dashboard panel or its credential management operations. The JSON below shows such a field:

```json
{
  "proxy_url": "socks5://user:pass@proxy.example.com:1080"
}
```

`proxy_url` sends that account's requests through its own proxy (`http://`, `https://`, `socks5://`, or `socks5h://`); `"direct"` skips every proxy. Without it, the account uses the configured `requests.proxy-url`. When neither is set, most providers follow the `HTTP_PROXY`, `HTTPS_PROXY` and `NO_PROXY` environment variables, while Claude accounts connect directly. For a provider API-key resource, the corresponding field is `proxy-url` on the key's entry.

Claude's default `auto` shaping mode can rewrite system prompts for clients other than Claude Code. Turn it off with credential metadata `"cloak_mode": "never"`, or with the OAuth-scoped setting `oauth.providers.claude.disable-claude-cloak-mode: true`. The rewrite does not make using a Claude subscription in another tool permitted; use an API key or supported cloud provider there.

## Codex over WebSocket

Codex accounts can talk to OpenAI over a WebSocket, the transport the Codex CLI itself uses, instead of one HTTPS request per turn. Enable it per account with the credential metadata field:

```json
{
  "websockets": true
}
```

For a Codex API key, set `websockets: true` on the key's entry under `api-keys.codex`.

## Reach the proxy from other machines

The simplest safe way is [Tailscale](https://tailscale.com/), which puts your machines on a private network:

1. Install Tailscale on the machine running the proxy and on the machines that use it.
2. Through the dashboard or `/admin/v1`, set `server.host` to the proxy machine's Tailscale address (the `100.x.y.z` address from `tailscale ip -4`), so only your tailnet can connect, and restart. Keep `access.api-keys` set.
3. On the other machines, use `http://<machine-name>:8317` (or the `100.x.y.z` address) as the base URL.

If you use `tailscale serve` to add HTTPS, it forwards from `127.0.0.1`, so also set `server.trusted-proxies: [127.0.0.1, "::1"]`. To open the dashboard from another machine, set `management.allow-remote: true`. Never expose the proxy to the internet, with Tailscale Funnel or any other tunnel, unless `access.api-keys` is set: without client keys, anyone who finds the address can use your accounts.

## A worked example: three Claude and two Codex accounts

This example is for one person using their own accounts on their own machines. The Claude Max account takes requests first. Two Claude Pro accounts take over while it cools down. Conversations stay on one account while it is ready, and both Codex accounts use WebSocket. Use Claude subscriptions only in native Anthropic applications, as Anthropic's [authentication rules](https://code.claude.com/docs/en/legal-and-compliance#authentication-and-credential-use) require.

Example in the YAML shape (not the persisted SQLite schema):

```yaml
config-version: 8
server:
  host: "127.0.0.1"
  port: 8317
access:
  api-keys:
    - "your-client-key"
management:
  secret-key: "your-management-key"
routing:
  strategy: fill-first
  session-affinity: true
  session-affinity-ttl: "1h"
  retry:
    request-retry: 1
    max-retry-interval: 30
```

Sign in five times (three Claude accounts, two Codex accounts), then set these fields in each account's panel on the Accounts page. SQLite persists these account edits; do not edit old files in `~/.cooker/auth`.

| Account | Fields | Reason |
|---|---|---|
| Claude Max | `"priority": 10` | Used first. |
| Claude Pro, first | `"priority": 0` | Used when the Max account is cooling down. |
| Claude Pro, second | `"priority": 0` | Used when the Max account is cooling down. |
| Codex, first | `"websockets": true` | WebSocket transport. |
| Codex, second | `"websockets": true` | WebSocket transport. |

With `fill-first`, each provider's top-priority account serves requests until it cools down, then the next ready account takes over. A bound conversation stays on its account while that account is ready. Replace `fill-first` with `soonest-reset` to order accounts by reset time, or with `weighted-round-robin` and `weight` fields to split requests by plan size.

Account state is separate from configuration revisions. Disabling or deleting a credential, or revoking its token, cannot be undone by restoring an old config revision. A full SQLite backup contains sensitive credential state; keep it private and do not assume an old refresh token is still valid. See [safe backup](CONFIGURATION.md#database-safety-and-backup).
