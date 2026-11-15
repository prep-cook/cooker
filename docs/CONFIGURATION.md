# Configuration

> **Design contract:** [State and concurrency](STATE-AND-CONCURRENCY.md) records the single-owner, memory-snapshot, callback, recovery and single-portable-file invariants of the configuration service. Interface names and code layout may evolve; that model must not drift. The operational details below describe the implemented behavior; the limits it does not prove are listed in that document.

SQLite is the sole durable authority for configuration and OAuth credentials. Select the database with `--db PATH`, then `CK_DB`; otherwise it is `cooker.db` beside the `--config` path (whose default is `config.yaml`). Use the same database path for the server, login commands and config commands. While the server runs it owns that database: change it through the dashboard, `/admin/v1` or `--management-base-url`, not through an offline command.

YAML is a first-import format, not a watched runtime file. A first start imports the selected `--config` file, or `./config.yaml` when the flag is absent; subsequent starts do not read it. Only the current layout is accepted: pre-v8 keys such as `port`, `auth-dir` or `claude-api-key` are rejected as unknown settings. Prefer an explicit import before starting the server:

```sh
cooker --db ~/.cooker/cooker.db config validate config.yaml
cooker --db ~/.cooker/cooker.db config import config.yaml
cooker --db ~/.cooker/cooker.db
```

There is no comment-preserving YAML save, file watcher or asynchronous YAML mirror. Change settings through the dashboard or `/admin/v1`; a successful atomic commit publishes the new runtime projection. Startup-only settings report `restart_required`, rather than silently claiming they are live. The settings and YAML examples below describe the YAML import and runtime projection shapes, **not the canonical persisted schema**. The store persists typed resources with stable IDs and revisions. Import preserves OAuth-only provider scope; unsupported or invalid input must be reviewed, not assumed to be silently dropped.

Accepted commands deliver configuration views directly. Credential changes wake a notification-only reconciliation task; idle operation does not hash or inspect the published database for changes. Replacing that file while the service runs is not an online command and does not hot-load its rows.

## Config commands

Without `--management-base-url`, these commands are offline. `config import` takes exclusive ownership, so a running service using the same database refuses it. `config validate`, `config export`, `config backup` and `config listener` only read: with the service running they read its published `--db` file without taking ownership. Copying that file directly is also a complete backup.

```sh
cooker --db cooker.db config import config.yaml       # FILE=- reads stdin
cooker --db cooker.db config validate config.yaml     # validate a candidate
cooker --db cooker.db config validate                 # validate stored config
cooker --db cooker.db config export --output export.json
cooker --db cooker.db config export --secrets --output private-export.json
cooker --db cooker.db config backup private-backup.db
cooker --db cooker.db config listener                 # committed bind settings
```

To import into a running service, explicitly select its management endpoint and key:

```sh
export CK_MANAGEMENT_KEY=...   # or let the command prompt for it
cooker --management-base-url http://127.0.0.1:8317 config import config.yaml
```

Online commands take the management key from `--password`, else from `CK_MANAGEMENT_KEY`, else from a prompt that does not echo when run in a terminal. Prefer the variable or the prompt: a `--password` value is visible to other local users' process listings while the command runs. Without a terminal and without either, the command sends no key and the service refuses it.

This submits the configuration through the existing authenticated API and waits for committed runtime publication. It never opens or writes a local `--db`, even if authentication, TLS, transport or persistence fails. The client uses the current global revision as `If-Match`; a concurrent change returns an error (**412**) instead of silently retrying over it. Use the final endpoint URL: owner mutations under `/admin/v1/` do not follow redirects, including responses without a `Location` header. Existing management access controls and TLS certificate verification still apply.

The TUI's setting and client-key commands acknowledge `/admin/v1/apply` only after a direct HTTP200 response with a numeric committed `revision`. A redirected read, an empty success body or HTTP202 is not a saved setting. Generic reads and non-owner requests retain the existing Go-style redirect behavior.

Among config commands the online context supports **import** only. `config export`, `config validate`, `config backup` and `config listener` stay local: while a service owns the database they read its published file read-only. Online logins, Vertex import and SIWC binding are described under [Online credential commands](#online-credential-commands).

`config import` and file-based `config validate` accept YAML of the current layout or the versioned JSON resource envelope. `config export` emits that same management-compatible JSON envelope (`format: "cooker-config"`, `version: 1`, `revision`, `resources`), not YAML. This preserves OAuth-only scope that canonical YAML would lose. Exports redact secrets by default; `--secrets` includes configuration secrets for a lossless full-config export. Protect it as credentials. New output files are private (0600 on Unix). A configuration export is not a credential backup; use `config backup` for the complete SQLite database, including history and credential secrets.

Installers and health-probe scripts can use `config listener` without parsing YAML or printing secrets. It reads the committed SQLite config and outputs exactly three newline-terminated lines: host (an empty line means wildcard), decimal port (`0` means ephemeral), and TLS enabled (`true` or `false`). It performs no network access or certificate/key-file reads. These are committed startup settings, not proof of the currently bound address after an unapplied restart-only change; a wildcard probe should use loopback, and an ephemeral port needs the actual runtime address.

## Administration API

The resource API uses the existing management authentication and remote-access policy. All paths below are under `/admin/v1`:

Resource kinds are `setting` (a top-level section), `provider` (a provider entry), `client-key` and `router`. Reads include `kind`, `id`, `spec`, ordering/description metadata and revision information. Keep the stable ID when editing: a provider's list position or live `auth_index` is not its identity. Redacted secret fields use `"$secret"`; submitting that marker at the same field preserves the stored secret, rather than replacing it with the marker.

Secret-destination checks cover changes to `base-url`, `proxy-url`, headers or provider type while secrets are retained, including secrets omitted from a PATCH/Set. Explicit plaintext resubmission authorizes the intended change. Error `pointer`/`pointers` values are JSON Pointers relative to the affected resource's `spec`, not the full export envelope.

| Route | Purpose |
|---|---|
| `GET /r/{kind}` | List resources of a kind. |
| `GET /r/{kind}/{id}` | Read a resource and its revision ETag; `?include_secrets=true` reveals that resource's secrets only. |
| `PUT /r/{kind}/{id}` | Create or replace a resource. |
| `PATCH /r/{kind}/{id}` | RFC 7396 merge patch of its spec. |
| `DELETE /r/{kind}/{id}` | Remove a resource. |
| `POST /apply` | Apply several operations atomically as one revision, or none. |
| `GET /effective/config` | Read the compiled runtime projection, including stable `@id` values and live `auth_index` values. |
| `GET /revisions`, `GET /revisions/{rev}` | Read configuration history. |
| `POST /revisions/{rev}/restore` | Restore configuration as a new revision, not credential state. |
| `GET /export`, `POST /import` | Export resources or import a resource envelope or YAML. |
| `GET /credentials` | Read an authorized coherent snapshot of managed original documents and their content hashes. |
| `POST /credentials/batch` | Save managed credential documents atomically, without creating a configuration revision. |

Use the strong quoted `ETag` from a read as `If-Match` when writing. A resource ETag is that resource's revision; effective config, lists and exports carry the global revision ETag. `If-Match: *` requires an existing resource; `If-None-Match: *` on PUT creates only when absent. A failed precondition returns **412**, so reread and reconcile rather than blindly retrying. Weak tags never satisfy the strong comparison and return **412** when no strong tag matches; malformed tags return **400**. Import and history restore require concrete global revision tag(s) in `If-Match`: a missing header or wildcard-only `*` returns **428** because an existence check cannot prevent lost updates. Atomic apply optionally accepts a global precondition, including `*`.

Every configuration write supports `?dry_run=true` and `?message=...`; apply also accepts its dry-run field. A dry run validates the entire candidate and shows redacted changes without persisting or publishing it. Validation failures return **422** with structured `error`, `message`, `resource`, `pointer` and `pointers` fields. When validation identifies a resource field, `resource` names the affected resource and `pointer` locates the field relative to its `spec`; document-wide or parse failures can leave both null. `pointers` is empty for ordinary validation errors; secret-resubmission errors populate it. `secret_destination_changed` means an edit changed where a retained secret would be sent: review the destination and explicitly resubmit plaintext secrets at every JSON Pointer in `pointers` (`pointer` names the first), rather than forwarding the redaction marker to authorize a new destination. Error responses do not expose raw secret values or database/parser internals. Requests are limited to 16 MiB after authentication.

Successful configuration writes return `revision`, `written`, `dry_run`, redacted `changes` and `restart_required`. Non-dry-run writes return the resulting resource ETag, or global revision ETag for apply/delete/import/restore; dry-run responses have no ETag because their candidate is not persisted. `X-Ck-Revision` identifies stored state when available and `X-Ck-Published-Revision` identifies the running projection. History is configuration history only: rolling it back never restores a revoked or deleted OAuth token.

SQLite's resource and global revisions are server-assigned values and cannot be set through a resource spec.

API export is redacted unless explicitly requested with `?include_secrets=true`; a single resource read accepts the same parameter and reveals only that resource, for a client that needs one secret-bearing setting. JSON imports require the export envelope with `format: "cooker-config"`, `version: 1`, an unsigned integer `revision` and `resources`. Use `application/json` for JSON (a charset parameter is accepted), or `application/yaml`, `application/x-yaml`, `text/yaml`, `text/x-yaml` or `text/plain` for legacy YAML; unsupported media types return **415**. A redacted export is for review, not a complete disaster-recovery backup. The old v0/v8 config reads, raw YAML upload and per-setting configuration endpoints are deleted; there is no compatibility alias. Credential, login and other non-config management operations remain separate.

The masking contract covers `/admin/v1` resource/configuration reads, revisions and effective config, credential-list API-key `account` values, and per-key usage. `GET /admin/v1/credentials` is an explicitly authorized plaintext credential-source interface (bytes are base64 encoded, not encrypted); it is no-store and excluded from capture, like protected credential downloads. Management-key access still permits plaintext credential downloads (`/v8/management/credentials/download` and its v0 alias) and usage queue consumption (`/v8/management/observability/usage/queue`, v0 `usage-queue`, and RESP). HTTP management requests send the key as `Authorization: Bearer <key>`; the RESP usage queue authenticates with `AUTH <management key>`, the only non-Bearer form. Queue records contain provider keys in `source` and client keys in `api_key`. Protect access to these interfaces.

### Atomic credential batches

`POST /admin/v1/credentials/batch` uses the same management authentication, remote-access policy and 16 MiB request limit. Submit `{"documents":[{"name":"account.json","data":"<base64 of original file bytes>"}]}`. Names must be distinct managed `.json` basenames, not host paths. The service validates the whole batch using upload validation before changing credential data, commits it once through ordinary owner admission and preserves the original bytes, including whitespace and opaque fields. It returns `{"status":"ok","saved":N}`; it does not create a configuration revision or accept configuration dry-run options.

A document may include `if_match` containing the 64 hexadecimal SHA-256 digits of its current **raw bytes**, or `if_none_match: true` to require absence. These are per-document conditions, not configuration revision ETags; they cannot be combined. A later invalid document returns **400**, and a failed condition returns **412**, with no earlier credential changed. A batch may also contain `deletions: [{"name":"old.json","if_match":"<current raw SHA-256>"}]`; each deletion requires a captured hash. Upsert/delete targets cannot overlap. All targets and conditions are checked before one mixed owner commit; the response adds `deleted: N` when removals were requested. Deletion-only batches may omit `documents`. A missing or changed deletion source returns **412**, not an unconditional removal. Deleted credentials are revoked before acknowledgement, and a standalone copied database preserves the deletion. Store/publication errors never report success. This endpoint is an ordinary authenticated mutation, not a provider-grant preservation capability. The existing multipart credential upload remains separate and can return **207** partial success.

### Online credential commands

While a service owns the database, a terminal sign-in goes through it: a login flag with `--db` alone is an offline write and is refused with `already owned`. Run the login against the service and type the management key when asked, or export it as `CK_MANAGEMENT_KEY` first:

```sh
export CK_MANAGEMENT_KEY=...   # the management.secret-key value
cooker --management-base-url http://127.0.0.1:8317 --claude-login --no-browser
```

With the service stopped, the same login flag with `--db` writes the database directly.

Online built-in logins and Vertex import use the credential batch port:

```sh
export CK_MANAGEMENT_KEY=...
cooker --management-base-url https://proxy.example --vertex-import ./service-account.json
cooker --management-base-url https://proxy.example --codex-device-login --no-browser
cooker --management-base-url https://proxy.example --siwc-bind codex-account.json --no-browser
```

`--codex-login` uses the browser flow instead. These commands do not load the local `--db` or `--config`, even if they identify the running owner's database. Before provider work, an authenticated ordinary dry-run checks owner admission, built-in logins read only the `requests` setting with its secrets (the proxy URL can carry credentials) and the SIWC identifier from `oauth`, never the complete export, and compile through the SQL-free resource importer/compiler, and `GET /admin/v1/credentials` captures original managed bytes, hashes and the owner's managed directory descriptor from memory. The blocking writer hydrates only its needed files in private client scratch, preserving existing opaque metadata for Codex merges; one conditional batch saves the result. SIWC hydrates its captured target **before** file validation, device-ID selection or upstream work, preserving outer Codex tokens and opaque fields while replacing the SIWC partition. Use a managed filename, or an absolute native owner path whose direct parent matches the returned managed directory; unsupported aliases, nested/foreign paths and missing targets are rejected before authorization, without consulting client filesystem state. The caller's accepted destination is not silently replaced with a client-local file. A replacement or deletion since that captured view returns **412** instead of overwriting or resurrecting it.

The provider's save callback itself returns success only after the API acknowledgement, not merely after writing scratch. Redirects, incomplete receipts, TLS/authentication/transport failures and storage errors do not fall back locally. Normal success/failure paths remove private scratch. The inspected Vertex, Codex, SIWC, Kimi/Kimi.ai, xAI, Meta, Devin, Antigravity and Claude writers are enabled; command priority, not the mere presence of a supported flag, decides eligibility. Their synchronous save runs on an owned, awaited blocking worker. Claude hydrates its captured filename namespace before enumerating case-folded legacy aliases; the final declared-path callback submits its new canonical bytes and any captured legacy removal in one source-conditioned batch. Login rendering, online and in the dashboard alike, sees only top-level managed files, so a Claude login does not merge or remove a legacy alias stored in a subdirectory of the credential directory, as Go does; that alias stays a separate credential. Remove the leftover duplicate after signing in. Only a successful renderer's missing declared/captured destination is a deletion; other file-read failures fail the save. Shared private rendering has separate nonblocking operation admission, and subsequent saves advance source conditions only to this client's acknowledged bytes. Overlapping/reentrant saves fail honestly rather than acknowledging another renderer's data. Vertex/Codex/Claude do not fetch configuration secrets they do not use.

## Database safety and backup

Keep the `--db` snapshot in a private persistent directory writable by the service user (0700 on Unix). Containers require a persistent volume for that directory, not just a mount of an old YAML file. Snapshot publication creates a temporary file alongside it, syncs that file, atomically replaces the snapshot and syncs the directory before acknowledging a write. Database files, exports and backups must be private (0600, or equivalent Windows ACLs); they contain provider secrets, client keys and OAuth refresh tokens.

The working checkpoint lives at `<TMPDIR>/cooker/<canonical-db-path-hash>/live.db`. SQLite operations use DELETE journaling and transient connections; the checkpoint is rebuilt from canonical memory. Its directory is private (0700); database images and transient journals are private (0600). Published snapshots do not depend on WAL/SHM files.

Only one service or offline command may own a selected database. Before workspace inspection or SQLite bootstrap, it takes a nonblocking native file lock under `<snapshot-parent>/.cooker-owners/<database-name>.lock`, using the canonical database path. The guard is held through workspace cleanup, so another process cannot split ownership by using a different TMPDIR or racing shutdown. Guard files are generated ownership bookkeeping, **not backup or restore inputs**; workspace cleanup never unlinks them. Do not delete them beneath an active owner or contender.

Offline writes, including `config import` and local logins or Vertex imports, refuse a database another process owns, even when its management API is unreachable. Read-only `config export`, `config validate`, `config backup` and `config listener` are not competing mutations: while another process owns the database they read its published snapshot file read-only, without taking ownership or touching the working checkpoint. They see the latest completed publication, which can trail an in-flight write.

No shared `coordination.db` or other cross-database lock is created; each database's guard is its only cross-process exclusion, so databases sharing a temporary namespace publish independently. A `coordination.db` left there by an older version is unused and may be deleted once no older version runs. PID markers and the old multi-handle registry are no longer ownership inputs and are not created. A settled owner removes its own working directory. Unresolved recovery checkpoints are retained and incomplete cleanup is reported. Startup does not sweep another database's abandoned workspace. Keep the same temporary namespace when intending to resume its surviving work; changing TMPDIR cannot bypass a live owner but can deliberately select a cold start after it stops.

**Copy the `--db` file directly, even while the service runs, for a complete usable backup.** An in-progress copy reads one immutable published snapshot; concurrent writes replace its path rather than modify those bytes. The copy can reflect an older completed state, but not a partially written database. `config backup` remains available and encodes the accepted complete state into a new standalone SQLite database. Keep backups private and stop all server/CLI users before restoring a snapshot. A full backup includes credential state: an old backup can contain revoked tokens, and providers may have invalidated rotated refresh tokens. Configuration-history restore cannot revive those tokens. Logical deletion and SQLite secure-delete settings are not a guarantee of physical erasure from SSDs, snapshots, backups or the underlying disk.

A normal cold start seeds its temporary working database from the published snapshot. If a valid working database survived an abnormal exit, startup resumes it and synchronously republishes its contents before readiness. After exclusive ownership is acquired, missing, empty, uninitialized or corrupt temporary databases fall back to the snapshot; stale PID files do not block that recovery. A supported initialized empty configuration remains valid recovery state. A valid newer unsupported schema is rejected without deleting its checkpoint or replacing the published snapshot, even when its tables are unknown to this build. A transaction committed to the working database but reported failed because publication failed can therefore reappear after resume. The published snapshot is the durable recovery source if OS tmp is lost.

### Restore a backup instead of resuming

**Replacing only the snapshot after an unclean exit can be ignored:** a surviving valid live database takes precedence and republishes over it. To force a cold restore:

1. Stop every server and CLI process using the selected `--db` path.
2. After the owner has stopped, ensure the old working database cannot resume. Either identify its exact `<TMPDIR>/cooker/<canonical-db-path-hash>` directory and remove only that stopped owner's directory; or configure a fresh private, trusted OS temporary directory for the next start. Keep that namespace on later starts; switching back can resume old working state. PID references alone are not proof that ownership is free. Never delete ownership guards or an active workspace.
3. Copy the backup over the selected `--db` file, preserving the service user's ownership and private permissions (0600 on Unix).
4. Start the server with the selected temporary namespace. With no old checkpoint there, it seeds from the restored snapshot. Online imports use its management API; offline commands must wait for the service to stop and use the intended recovery namespace.

## A complete small import example

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
  strategy: round-robin
  session-affinity: true
  retry:
    request-retry: 2
observability:
  logs:
    logging-to-file: true
```

## server

| Setting | What it does |
|---|---|
| `host` | The address to listen on. `"127.0.0.1"` accepts only this computer; `""` listens on every interface. |
| `port` | The port, `8317` by default. |
| `tls.enable`, `tls.cert`, `tls.key` | Serve HTTPS on the same port with this certificate and key file. |
| `max-request-body-bytes` | An explicit request-body limit in bytes; `0` (the default) or a negative value adds no deployment limit. Changes apply to the next request. Anthropic Messages and token counting accept at most 128 MB (128,000,000 bytes), or this setting when smaller; other client routes retain their 64 MiB ceiling. Oversized inference bodies return HTTP 413 in the client's protocol. Management JSON and multipart readers may handle a complete value before the limit. |
| `github-token` | A GitHub token. cooker retains the setting for import/runtime projection but does not send it on release checks; its dashboard is built in. Treat it as a secret; ordinary admin reads and exports are redacted. The older top-level `github-token` import spelling also works; the one under `server` wins. |
| `trusted-proxies` | Addresses of reverse proxies or tunnels whose `X-Forwarded-For` header is trusted. Set it when a tunnel on the same machine forwards requests, for example `[127.0.0.1, "::1"]`. See [running it safely](GETTING-STARTED.md#running-it-safely). |

## access

| Setting | What it does |
|---|---|
| `api-keys` | The client keys your tools send, as `Authorization: Bearer <key>`, `x-api-key`, `x-goog-api-key` or `?key=`. Keep at least one; with an empty list anyone who can reach the server can use it. |

## management

| Setting | What it does |
|---|---|
| `secret-key` | The dashboard and Management API password. A plaintext imported value is stored as its bcrypt hash, not rewritten into YAML. The `CK_MANAGEMENT_PASSWORD` environment variable works too. When neither is set, every management request answers 403 ("management key not set"). Changing or removing it signs out every dashboard session for good: restoring the old value later brings none back. |
| `allow-remote` | `false` (the default) accepts management requests from this computer only. |
| `disable-control-panel` | `true` stops serving the dashboard at `/management.html`; the Management API keeps working. |

## oauth

| Setting | What it does |
|---|---|
| `providers.codex.signin-with-chatgpt-identifier` | The agent name sent when binding Sign in with ChatGPT tokens to a Codex credential (`POST /v8/management/oauth/siwc/bind`), `CLIProxyAPI` when empty. Legacy spelling: `codex.signin-with-chatgpt-identifier`. Go also uses it as the SIWC request User-Agent prefix; cooker does not send SIWC requests yet. |
| `providers.codex.chatgpt-keep-alive` | `false` by default. `true` keeps up to 2 idle connections to `chatgpt.com` per proxy for 90 seconds, so Codex HTTP requests skip the TCP and TLS handshake. Off, every Codex request opens its own connection. A published change applies from the next request, without a restart. |

The private `<canonical-db-path>.credentials` directory is a materialized credential cache and refresh-lock domain, not a supported edit/import source or independent durable authority. Do not edit or share it: credential state loads from SQLite, and cache edits are not imported. Before a provider writer merges existing settings, its affected files are restored from SQLite under the storage transaction; writes, token refreshes and startup can replace cache bytes. The server does not watch files or poll the database. Configuration and credential changes take effect when the owning service commits them, from the dashboard, `/admin/v1` or a `--management-base-url` command; an offline command cannot write a database that a running service owns. The service loads the database once at startup and opens SQLite again only to save a commit. Keep its contents private alongside SQLite. Credential tables are excluded from config export/history/restore; only a full database backup includes them.

Deleting a credential while its token refresh is in flight can return **409** (`credential refresh in progress; retry deletion`); retry after refresh finishes. A conflict leaves the durable credential intact. Delete-all acquires the required credential locks and deletes in one SQLite transaction, not a best-effort series of file removals; missing cache files do not prevent deletion.

## upstream

Provider settings shared by account sign-ins and provider API keys. Settings left under `oauth.providers`, such as `oauth.providers.codex.header-defaults`, apply to sign-ins only.

| Setting | What it does |
|---|---|
| `codex.disable-codex-cloaking`, `codex.stream-bootstrap-buffering`, `codex.stream-bootstrap-timeout`, `codex.orphan-delegation-compatibility`, `codex.model-level-cooling`, `codex.response-steering` | Codex request shaping, stream start-up, cooldown scope and full-duplex WebSocket steering. |
| `claude.model-level-cooling`, `claude.disable-cloaking-model-list`, `claude.disable-claude-cloak-mode`, `claude.header-defaults.*` | Claude cooldown scope, cloaking and the fallback request headers (`user-agent`, `package-version`, `runtime-version`, `os`, `arch`, `timeout`, `timezone`, `stabilize-device-profile`). |
| `claude.tool-search` | `false` by default. `true` turns Codex client `tool_search` on Responses requests to Claude into Anthropic native tool search (`defer_loading` tools and `tool_reference` results) on credentials that support it: Anthropic's own API, or a Claude API key with `native-tool-search: true`. Other credentials and models that predate tool search get the request without `tool_search`. The Codex model catalog then advertises the search tool for a Claude model only when every credential serving it supports native tool search. |
| `xai.inject-x-search` | Adds the `x_search` tool to xAI requests. |

The flat `codex`, `claude`, `claude-code`, `disable-claude-cloak-mode`, `claude-header-defaults` and `xai` keys, and `oauth.providers.claude` and `oauth.providers.xai`, are rejected as unknown settings. OAuth-only settings retain their scope rather than being broadened to API-key credentials. New configuration edits use `/admin/v1` resources, not the removed v8 setting paths.

With `upstream.codex.stream-bootstrap-buffering: true`, HTTP/SSE and non-steering WebSocket attempts hold their initial handshake frames within the existing 48-frame / 1 MiB bounds and optional `stream-bootstrap-timeout`. A retryable disconnect or upstream rejection before those frames are released can move an unpinned request to the next eligible credential through normal selection. Invalid client requests and request-scoped empty-incomplete failures still stop; once output is released, the router does not replay it on another credential. Stateful continuations remain pinned: a lost upstream session needs client replay, not an in-place credential switch.

A retired socket's disconnect notification cannot overtake the next attempt's reset and close its healthy replacement. Local validation and prewarm replies also stop when the upstream socket is lost, even if the downstream writer is blocked.

## api-keys

Provider API keys, as opposed to account sign-ins, go here, grouped by provider: `claude`, `codex`, `gemini`, `vertex`, `xai` and `openai-compatibility` for any service that speaks the OpenAI API. The dashboard's Provider keys page edits them. An OpenAI-compatible service, for example:

```yaml
api-keys:
  openai-compatibility:
    - name: openrouter
      base-url: "https://openrouter.ai/api/v1"
      keys:
        - api-key: "sk-or-..."
      models:
        - name: "moonshotai/kimi-k2"
          alias: "kimi-k2"
```

A `vertex` key with `interactions: true` sends Interactions clients (`/v1beta/interactions`) to Vertex's native Interactions API (`/v1beta1/interactions`) instead of translating them to `generateContent`. A Vertex service-account file opts in the same way with `"interactions": true` or `"native_interactions": true` at the top level. When the credential names a project (a service account's `project_id`, or a file's `project_id` next to its `access_token`), the path is `/v1beta1/projects/<project>/locations/global/interactions`; the credential's `location` is ignored, and the host is the configured `base-url` or `https://aiplatform.googleapis.com`.

Vertex partner and open models (MaaS), named `<publisher>/<model>` such as `deepseek-ai/deepseek-v4.1-flash-maas` or `publishers/<publisher>/models/<model>`, go to Vertex's OpenAI-compatible `endpoints/openapi` route as Chat Completions requests, for every client format and before the Interactions opt-in; the upstream model is `<publisher>/<model>`. Two setups reach the route:

- A `vertex` API-key entry whose `models` list names a partner model, with a third-party relay `base-url`. The request goes to `<base-url>/v1/endpoints/openapi/chat/completions` with `Authorization: Bearer <key>`. A base that already names a version segment (`/v1/`, `/v1beta1/` or a last segment such as `v1`) gets only `/endpoints/openapi` appended; a base that already ends in `/endpoints/openapi` is used as given, one that names `projects/<project>/locations/<location>` gets `/endpoints/openapi` appended, and one containing `/publishers/` is cut there before `/endpoints/openapi` is appended. On Google's own hosts (no `base-url`, or an `aiplatform.googleapis.com` one) the key is not sent and the route answers 400, since those hosts need a service account. A `base-url` that Go's URL parser rejects, such as one with a percent-escaped host like `https://%61iplatform.googleapis.com`, answers 500 without sending a request. So does a `base-url` that Go parses without a host, such as `https:relay.example` or `https:///v1`, or without an `http` or `https` scheme, such as `relay.example`. The same applies to a service account's `base_url`, after its token exchange.
- A Vertex service-account file with `"openapi": true`, which sends the catalog's Gemini models as `google/<model>` (Imagen models excepted; `"openapi": false` turns the route off). It calls `https://<location>-aiplatform.googleapis.com/v1/projects/<project>/locations/<location>/endpoints/openapi/chat/completions` (`global` uses `aiplatform.googleapis.com`) with a fresh access token; a file's `openapi_location` overrides the location. A file `base_url` takes the place of that scheme and host and may carry a path: the version segment and the project route are appended to it under the same rules as above. Any Vertex auth file (not only a service-account one, and whatever its `openapi` setting) lists, besides the Vertex catalog, the models named in its `openapi_models` (a list, or a comma-separated string) or `models` (names, or `{name, alias}` entries), and the targets of the `oauth-model-alias.vertex` aliases. A listed partner model takes the `endpoints/openapi` route unless the file sets `"openapi": false`, in which case it goes to the native `generateContent` route. As for every OAuth alias, an alias without `fork: true` then replaces its target in the model list, so clients select the model by the alias name.

A `models` entry with an `alias` is a per-file model alias in any auth file, like `model_aliases`.

## routing

How the server picks an account for each request. [MULTI-ACCOUNT.md](MULTI-ACCOUNT.md) explains these in detail.

| Setting | What it does |
|---|---|
| `strategy` | `soonest-reset` (alias `reset-first`, the default when unset: select the account whose weekly window resets soonest), `round-robin`, `fill-first` or `weighted-round-robin`. Set `round-robin` to take the accounts in turn as Go does. [Routing strategies](MULTI-ACCOUNT.md#routing-strategies) explains each one. |
| `session-affinity` | `true` keeps a conversation on the account that served its first request. The installer writes `true` for new configs. If omitted, the server default is `false`, as in Go. |
| `session-affinity-ttl` | How long a conversation stays bound to its account, `"1h"` by default. |
| `retry.request-retry` | Extra rounds over the accounts after a failed attempt, `0` by default. |
| `retry.max-retry-credentials` | At most this many accounts tried per round; `0` means all of them. |
| `retry.max-retry-interval` | The longest wait, in seconds, for an account to come out of cooldown before retrying; `0` means do not wait. |
| `cooldown.disable-cooling` | `true` keeps failing accounts in rotation instead of resting them. |
| `cooldown.transient-error-cooldown-seconds` | How long to rest an account after a temporary upstream error; `0` means 60 seconds, a negative value turns it off. |
| `cooldown.save-cooldown-status` | `true` preserves cooldowns in SQLite's separate credential state across restarts. Saves are coalesced through one background writer, with pending writes drained on graceful shutdown. Cooldowns are read from and written to SQLite only; `.cds` files are never read. Cooldowns are not config revisions; config restore is neither a cooldown reset nor restoration of token state. |
| `cooldown.max-trusted-cooldown` | A cooker addition. When an account hits its usage limit, the provider says when it resets, sometimes days ahead, and the account rests until then. Providers often reset early, so cooker trusts the stated time for at most this long (`"1h"` by default; a Go duration such as `"90m"`, or a bare number meaning seconds; at least 10 seconds), then lets the next request through to check. If the account is still limited, the next rest is twice as long, and never longer than the stated reset. `0` trusts the stated reset, as CLIProxyAPI does. An unreadable value is logged and the default is used. See [differences from Go](DIFFERENCES-FROM-GO.md#deliberate-differences). |
| `providers.exclude`, `providers.order` | A cooker addition: which providers a request may use and in what order. See [Provider scope and order](#provider-scope-and-order). |

### Provider scope and order

When several providers serve a model, `routing.providers` limits which may serve it and in what order (cooker only). Without it, `priority` and `strategy` decide, as in CLIProxyAPI.

```yaml
routing:
  strategy: round-robin              # balances inside a tier
  providers:
    exclude: [provider-x]            # never used for a model named without a prefix
    order:                           # tiers, tried top down
      - [provider-a, provider-b]     # first tier: a and b in turn
      - provider-c                   # second tier: only while the first has no usable account
```

- **Selectors**, case-insensitive: an `api-keys` entry `name`, a login type (`kimi`, every login file of it), or one login as `kimi/<file without .json>` or `kimi/<email>`. Unknown or repeated selectors are logged at load.
- **Scope.** `exclude` applies to models named without a prefix (`provider-x/model` still reaches provider-x).
- **Order.** Tiers are tried top down, `strategy` balances inside a tier, and unlisted credentials follow by `priority`. Order only places new conversations: with `session-affinity` a conversation keeps its usable account even when a higher tier recovers, so its cache stays warm.
- **Failover** to the next credential follows the entry's `request-scoped-errors` actions, else the usual handling.

`X-Ck-Routed-Provider` names the serving credential's selector. It is sent only when `routing.providers` sets `exclude` or `order`, or a model router served the request.

### Optional preset: fail over only on 429

This commented preset keeps 429 cooldown and failover for Claude and Codex OAuth accounts. It stops ordinary failover after a failed attempt for the listed non-429 statuses, without adding a cooldown. Credential-preparation failures and an enabled `requests.streaming.bootstrap-retries` can still cause another attempt. Unlisted statuses keep normal handling. The preset applies only to OAuth accounts and leaves plan limits and provider checks unchanged. A credential's own `request_scoped_errors` rules take precedence.

Uncomment the block to use it. `(?s).*` matches any body, including an empty one. Rules need a body matcher; status alone does not match.

```yaml
# oauth:
#   request-scoped-errors:
#     claude: &plan-limits-only
#       - {status: 429, match-regexr: ["(?s).*"], action: continue-and-cooldown}
#       - {status: 400, match-regexr: ["(?s).*"], action: stop}
#       - {status: 401, match-regexr: ["(?s).*"], action: stop}
#       - {status: 402, match-regexr: ["(?s).*"], action: stop}
#       - {status: 403, match-regexr: ["(?s).*"], action: stop}
#       - {status: 404, match-regexr: ["(?s).*"], action: stop}
#       - {status: 408, match-regexr: ["(?s).*"], action: stop}
#       - {status: 500, match-regexr: ["(?s).*"], action: stop}
#       - {status: 502, match-regexr: ["(?s).*"], action: stop}
#       - {status: 503, match-regexr: ["(?s).*"], action: stop}
#       - {status: 504, match-regexr: ["(?s).*"], action: stop}
#     codex: *plan-limits-only
```

## requests

| Setting | What it does |
|---|---|
| `proxy-url` | An outbound proxy for every upstream request, such as `socks5://127.0.0.1:1080` or `http://proxy:3128`. A single account can use its own proxy instead; see [per-account proxies](MULTI-ACCOUNT.md#per-account-proxies-and-request-shaping). |
| `tls-handshake-timeout-seconds` | Seconds a new connection to `api.anthropic.com` (Claude) or `chatgpt.com` (Codex) may take to be ready, so a server that accepts the connection but never answers the TLS handshake fails the attempt and request retry can engage. The default `0` waits indefinitely. It never limits a request once connected. In cooker it also covers the DNS lookup, the TCP connect and a proxy's CONNECT, which Go leaves unbounded. |

Claude accounts ignore `HTTPS_PROXY` and `HTTP_PROXY`. Without an explicit proxy, they connect directly. Set `requests.proxy-url` or the account's `proxy_url` when you need one. Claude API keys pointed at a custom, non-Anthropic base URL use the standard transport, which can inherit environment proxies.

## observability

| Setting | What it does |
|---|---|
| `logs.logging-to-file` | `true` writes the log to `main.log` in a `logs` folder, rotated, instead of standard output. |
| `logs.logs-max-total-size-mb` | The most disk space the log files may use. |
| `logs.request-log` | `true` writes one file per request with client and upstream request/response sections. Recognised sensitive headers and URL fields are masked, but bodies can contain credentials, including Realtime client secrets, as well as private prompts and output. Keep the logs private. |
| `usage.claude-cache-telemetry` | `false` by default. With `usage.usage-statistics-enabled: true` and management enabled, adds content-free final-wire Claude cache observations to the existing usage queue and RESP channel. No tokenizer or maintenance requests run. Hashes rotate on restart; additional records without returned usage have `usage_observed: false`. Large payloads add parsing and hashing work before upstream send. See [Claude cache telemetry](CLAUDE-CACHE-TELEMETRY.md). |
| `pprof.enable` | `false` by default. `true` starts a separate plain-HTTP debug listener, without API keys, management authentication or TLS. Keep it on loopback: its command-line route can expose arguments, and profiling consumes CPU. Changes apply without a restart. |
| `pprof.addr` | The debug listener address, `"127.0.0.1:8316"` by default, also used for an empty or whitespace-only value. Changes move the listener without a restart. A bind failure is logged without stopping the main server; another config publication retries it. |

`/debug/pprof/` lists the routes;
`cmdline` returns NUL-separated arguments and `symbol` accepts program counters in
the raw query or a POST body, separated by `+`. Go runtime profiles return plain-text
`501` answers. CPU `profile?seconds=30` requires a Unix build with
`cargo build --release -p cooker --features ck-server/pprof` and returns a pprof
protobuf. Default builds, published release binaries and Docker images exclude the
profiler and symbol resolver. Their `symbol` route consumes POST bodies but returns
only `num_symbols: 1`, without resolving any names, including shared-library names.
With the feature, an unstripped build gives more symbol names. Do not use another
SIGPROF profiler in the same process. Listener changes allow five seconds for active
requests to finish. On timeout, the error is logged, active requests continue, and
no replacement starts until another config publication. Temporary accept errors
keep the listener open and back off from 5 ms to one second; config changes and
shutdown interrupt the wait. Addresses require numeric ports; unlike Go, `:` and
service-name ports such as `:http` are not accepted.

## models

| Setting | What it does |
|---|---|
| `catalog`, `codex-catalog`, `devin-catalog` | Where the model catalog (`models.json`), the Codex client catalog and the Devin catalog come from: an absolute local path or an `http`/`https` URL. Empty keeps the default. Any other value, such as a relative path, `~/models.json` or a `file://` URL, refuses the config, as in CLIProxyAPI v8.0.35. cooker checks and keeps these settings but does not use them yet; see [DIFFERENCES-FROM-GO.md](DIFFERENCES-FROM-GO.md). |

When a model lists supported reasoning levels, an unsupported standard level snaps to the nearest supported level in this order: `minimal`, `low`, `medium`, `high`, `xhigh`, `max`. Ties choose the lower level. Requests below or above the supported range use its lowest or highest level. This applies within a provider family and across families; `none`, `auto`, budgets and models without a level list keep their existing handling. Unknown level strings are not snapped.

## worker-threads

`worker-threads: 4` at the top level sets how many threads serve requests. Without it, cooker uses the smaller of the CPU count and two, which is plenty for one person: a coding-agent request with a 300 KB prompt costs about 20 to 30 ms of CPU (a small chat request about 1 ms), and most of a request's time is spent waiting on the provider. Each extra thread can keep its own pool of freed memory, so more threads mean a larger resident size. The `TOKIO_WORKER_THREADS` environment variable overrides it.

The setting is read from SQLite before the request runtime starts. Changing it requires a restart; `TOKIO_WORKER_THREADS` overrides it. YAML edits after import have no effect. Remote-store environment settings do not provide an alternate source of thread configuration or runtime configuration.

## realtime

| Setting | What it does |
|---|---|
| `realtime.ping-interval-seconds` | Seconds between the WebSocket pings the gateway sends to a realtime client; default 30. Must be less than `realtime.read-timeout-seconds`. Deploy-only; restart required. |
| `realtime.read-timeout-seconds` | Seconds a realtime client connection may go without a pong before the gateway closes it; default 60. Only a pong extends it. Deploy-only; restart required. |
| `realtime.write-timeout-seconds` | Seconds one write to a realtime client may block before the gateway closes the session; default 10. Deploy-only; restart required. |
| `realtime.max-sockets-per-key` | Concurrent realtime sockets per client key; default 0, no limit. Deploy-only; restart required. |

## model-routers

A model router is a model name, such as `smart`, that picks the model and effort for each request, so the client does not have to choose. It is modelled on OpenRouter's Jev Router: a judge grades how hard each request is, and the router picks the cheapest model whose measured capability meets that difficulty. Scores and prices come from [Artificial Analysis](https://artificialanalysis.ai/), which needs a free API key:

```yaml
artificial-analysis:
  api-key: aa-...            # without one, nothing is fetched
  cache-file: /var/lib/cooker/artificial-analysis.json   # optional: survives restarts

api-keys:
  openai-compatibility:
    - name: opencode-zen     # Jev's provider, connected like any other
      base-url: https://opencode.ai/zen/v1
      prefix: opencode-zen
      keys:
        - api-key: sk-...
      models:
        - name: jev-1.13

model-routers:
  - name: smart              # the model clients send; `smart(high)` hints the effort
    tier: balanced           # cost, balanced (default) or quality
    judge: opencode-zen/jev-1.13
  - name: smart-cheap
    tier: cost
    judge: gemini-3-8-flash  # or any chat model this proxy serves
    models:                  # optional: only these candidates, otherwise every available model
      - claude-sonnet-5-5
      - gpt-6-astra(high)    # a suffix keeps only that effort
      - {model: my-deepseek, aa: "DeepSeek V4 Pro", price: {input: 0.3, output: 1.2}}
    fallback-models: 1       # optional: another model when this one fails (default 0)
    providers:               # optional: like routing.providers, for this router only
      exclude: [provider-c]
      order: [provider-b, [provider-a, kimi]]
```

How a request to `smart` is routed:

- **Candidates.** The `models` list, or every available model, minus those without a usable credential in the router's scope, in a cooldown or with a context window smaller than the request (4 bytes a token, a base64 attachment 1,600 tokens). Each candidate contributes one variant per effort that Artificial Analysis measured and its route offers, matched by display name, then ID, or by `aa`. A variant's cost is Artificial Analysis' cost per benchmark task, scaled by `price` (USD per million tokens, blended 3:1) over the list price when `price` is set.
- **Providers.** A router's `providers` add to `routing.providers` for its requests: both `exclude` lists apply, prefixed models included, and its `order` replaces the global one. The judge follows only `routing.providers`.
- **Model fallback.** With `fallback-models: N`, a request whose model failed before any byte reached the client moves up to N times to the cheapest untried candidate that meets the same requirement (else the strongest left), without a new grade. A request fault or a `stop` rule ends it instead. The conversation moves only once the new model served.
- **Grade.** The `judge`, a model of this proxy named as clients name it, grades the user message (its first 4,000 bytes, with reminder blocks stripped) from 0 to 4 within 10 seconds. A Jev model (upstream name `jev…`) gets a System One `score` question at its provider's `{base-url}/systemone`, trying each ready credential in `routing.providers` order with an equal share of the time left, outside the scheduler and usage. Any other model is a chat judge, asked through the dispatcher for one digit. A router without a judge is skipped; a judge that names a router is never asked. Without a grade (logged as a warning), request signals stand in: 0 for a short prompt without tools, else 1, plus 1 for tools and 1 past about 50,000 tokens. A client effort (the router's suffix, `reasoning_effort`, `reasoning.effort`, `output_config.effort`, Gemini's `thinkingLevel`) adds -1 (`none`, `minimal`, `low`), +1 (`high`) or +2 (`xhigh`, `max`).
- **Pick.** The grade maps linearly onto the index range of the candidates' cost/capability Pareto front, moved by -15% (`cost`), 0 or +20% (`quality`) of that range; the cheapest variant within 1 index point of it serves, since smaller gaps are benchmark noise. The client's suffix is kept when the route has none of its own. Without any rated candidate, the first available listed model serves, else the newest available model.
- **Conversation.** A conversation (the session the server derives, see `session_source` in [DIFFERENCES-FROM-GO.md](DIFFERENCES-FROM-GO.md)) keeps its model for an hour after its last request, so its prompt cache stays warm. Tool-result requests always keep it; a new user message is graded again and moves the conversation only to a stronger model. It also moves when its model becomes unavailable, leaves `models` or is too small. `count_tokens` neither asks the judge nor binds; image, video and speech routes do not route. A Responses client chaining `previous_response_id` should send `prompt_cache_key`, `conversation_id` or a session header, or each turn can land on another model.

The benchmarks are fetched at start, daily, and an hour after a failure, which keeps the last data. `X-Ck-Routed-Model` names the model that served a request and `X-Ck-Routed-Provider` its provider; `GET /v1/models` lists the routers. Not covered yet: the Codex client and Grok Shell catalogs, and upstream WebSocket reuse for Responses WebSocket turns naming a router (they run over HTTP). This setting exists only in cooker ([DIFFERENCES-FROM-GO.md](DIFFERENCES-FROM-GO.md)); Artificial Analysis data requires attribution under its terms.

## Extending

cooker has no plugin interface: the plugin API carried over from the Go gateway is poorly designed, so it was removed rather than kept. Until the maintainers design a new extension mechanism, add a provider, a route or other behaviour by modifying the source, then build and deploy your own image.

## Environment

- `CK_MANAGEMENT_PASSWORD`: the management key, instead of `management.secret-key`.
- `TOKIO_WORKER_THREADS`: the number of request threads, instead of `worker-threads`.
- `RUST_LOG`: overrides the log level, for example `RUST_LOG=debug`.
- A `.env` file in the working directory is loaded at start, as in CLIProxyAPI.
- `CK_DB`: the database path when `--db` is not supplied.
