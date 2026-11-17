#!/usr/bin/env node
// Mock Management and admin config APIs. Zero dependencies, deterministic for a seed.
//
//   node server.mjs [--port 0] [--accounts 1000] [--seed 1] [--key mock] [--release v0.0.0-mock]
//
// --release none answers the release check as the server does for a repository with no release yet.
//
// Response shapes follow crates/ck-server/src/management/*.rs. State lives in memory: write routes
// answer like the real server and change what the next read returns. GET /credentials changes about
// 5% of the accounts per call (request counts, last-used bucket) so the dashboard sees live traffic;
// every other account serialises to identical bytes. Unknown routes answer an empty 404.
// GET /v1/models answers a client key from access.api-keys as the proxy does, for the Use page's test.
import http from "node:http";
import { parseArgs, isDeepStrictEqual } from "node:util";
import { createHash, randomBytes } from "node:crypto";

const { values: opt } = parseArgs({
  options: {
    port: { type: "string", default: "0" },
    host: { type: "string", default: "127.0.0.1" },
    accounts: { type: "string", default: "1000" },
    seed: { type: "string", default: "1" },
    key: { type: "string", default: "mock" },
    churn: { type: "string", default: "0.05" },
    quiet: { type: "boolean", default: false },
    release: { type: "string", default: "v0.0.0-mock" },
  },
});
const N = Math.max(0, Number(opt.accounts));
const CHURN = Number(opt.churn);
const KEY = opt.key;
const VERSION = "cooker-mock";
const EXPOSED_HEADERS = "X-Ck-Trace-Id, X-Ck-Version, X-Ck-Commit, X-Ck-Build-Date, X-SERVER-VERSION, X-SERVER-BUILD-DATE, Location, Retry-After, ETag, X-Request-Id, OpenAI-Request-Id";
// Three days before start, so the dashboard shows a past build in every time zone.
const BUILD_DATE = new Date(Date.now() - 3 * 86_400_000).toISOString();
const BUCKET_MS = 600_000;

// ---------------------------------------------------------------------------------------------
// Deterministic PRNG (mulberry32). All generation and churn draw from it, in request order.
function prng(seed) {
  let a = Number(seed) >>> 0 || 1;
  const next = () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  return next;
}
const rand = prng(opt.seed);
const int = (lo, hi) => lo + Math.floor(rand() * (hi - lo + 1));
const pick = (list) => list[Math.floor(rand() * list.length)];
const hex = (n) => Array.from({ length: n }, () => "0123456789abcdef"[Math.floor(rand() * 16)]).join("");
const chance = (p) => rand() < p;

const startedAt = Date.now();
const iso = (t) => new Date(t).toISOString();

// ---------------------------------------------------------------------------------------------
// Accounts
const PROVIDERS = [
  ["claude", 0.28, "Claude"],
  ["codex", 0.26, "Codex"],
  ["gemini", 0.12, "Gemini"],
  ["kimi", 0.1, "Kimi"],
  ["antigravity", 0.1, "Antigravity"],
  ["xai", 0.07, "xAI"],
  ["devin", 0.04, "Devin"],
  ["vertex", 0.03, "Vertex"],
];
const FIRST = ["ada", "bo", "cyra", "dai", "eli", "fen", "gus", "hana", "ivo", "jun", "kai", "lia", "mo", "nia", "oz", "pia", "quin", "rex", "sol", "tai", "uma", "vik", "wen", "xin", "yu", "zed"];
const LAST = ["stone", "river", "field", "brook", "hill", "wood", "lake", "ridge", "vale", "marsh", "crest", "glen"];
const MODELS = {
  claude: ["claude-sonnet-4-5", "claude-opus-4-1", "claude-haiku-4-5"],
  codex: ["gpt-5-codex", "gpt-5", "gpt-5-mini"],
  gemini: ["gemini-2.5-pro", "gemini-2.5-flash"],
  kimi: ["kimi-k2", "kimi-k2-thinking"],
  antigravity: ["gemini-3-pro-high", "claude-sonnet-4-5-thinking"],
  xai: ["grok-4", "grok-code-fast-1"],
  devin: ["devin-swe-1"],
  vertex: ["gemini-2.5-pro"],
  meta: ["muse-spark"],
  aistudio: ["gemini-2.5-pro", "gemini-2.5-flash"],
};

/** Server-relative bucket clock: bucket 0 starts at the 10-minute boundary before server start. */
const bucketOrigin = Math.floor(startedAt / BUCKET_MS) * BUCKET_MS;
let bucketIndex = 0;
const clock = (t) => new Date(t).toTimeString().slice(0, 5);
const bucketLabel = (back) => {
  const s = bucketOrigin + (bucketIndex - back) * BUCKET_MS;
  return `${clock(s)}-${clock(s + BUCKET_MS)}`;
};

function weighted() {
  let r = rand();
  for (const p of PROVIDERS) if ((r -= p[1]) < 0) return p[0];
  return PROVIDERS[0][0];
}

function makeAccount(i) {
  const provider = weighted();
  const email = `${pick(FIRST)}.${pick(LAST)}${i}@example.invalid`;
  const name = `${provider}-${email}.json`;
  const a = {
    id: name,
    auth_index: hex(16),
    name,
    type: provider,
    provider,
    label: "",
    status: "active",
    status_message: "",
    disabled: false,
    unavailable: false,
    runtime_only: false,
    source: "file",
    size: int(1800, 3600),
    // Not serialised: counts per ten-minute bucket, oldest first.
    _b: Array.from({ length: 20 }, () => [0, 0]),
    success: 0,
    failed: 0,
    quota: { signals: {} },
    email,
    account_type: "oauth",
    account: email,
    created_at: iso(startedAt - int(1, 90) * 86_400_000),
    updated_at: iso(startedAt - int(1, 300) * 3_600_000),
    modtime: iso(startedAt - int(1, 300) * 3_600_000),
    last_refresh: iso(startedAt - int(1, 48) * 3_600_000),
    path: `/var/lib/cooker/auth/${name}`,
    cooldowns: [],
  };
  if (provider === "codex") {
    a.id_token = { chatgpt_account_id: `${hex(8)}-${hex(4)}-${hex(4)}-${hex(4)}-${hex(12)}`, plan_type: pick(["plus", "plus", "pro", "team"]) };
  }
  if (provider === "vertex") a.project_id = `mock-project-${int(1, 40)}`;
  if (chance(0.1)) a.priority = int(1, 10);
  if (chance(0.1)) a.note = pick(["shared", "team seat", "personal", "backup", "do not rotate"]);
  if (chance(0.05)) a.weight = int(1, 5);
  if (chance(0.03)) a.request_retry = int(0, 4);

  // Traffic: about half the accounts served something in the last 200 minutes.
  if (chance(0.55)) {
    const level = int(1, 40);
    for (const b of a._b) {
      if (!chance(0.6)) continue;
      b[0] = int(0, level);
      b[1] = chance(0.12) ? int(1, 3) : 0;
    }
  }
  if (chance(0.15)) a._b[19][0] += int(1, 6);
  const sums = a._b.reduce((s, b) => [s[0] + b[0], s[1] + b[1]], [0, 0]);
  a.success = sums[0] + int(0, 5000);
  a.failed = sums[1] + int(0, 80);

  // State: disabled, cooling, signed out, model-scope cooldown, or ready.
  const r = rand();
  if (r < 0.05) {
    a.disabled = true;
    a.status = "disabled";
    if (chance(0.5)) a.status_message = "disabled via management API";
  } else if (r < 0.1) {
    const at = startedAt + int(1, 6) * 3_600_000;
    const cd = { scope: "credential", reason: pick(["quota", "transient_error"]), retry_at: iso(at), remaining_seconds: Math.round((at - startedAt) / 1000) };
    if (cd.reason === "quota") (cd.backoff_level = int(0, 3)), (cd.http_status = 429);
    else cd.http_status = pick([500, 502, 503]);
    if ((provider === "claude" || provider === "codex") && chance(0.5)) cd.recover_at = iso(at + int(1, 5) * 3_600_000);
    a.cooldowns.push(cd);
    a.status = "error";
    a.unavailable = true;
    a.next_retry_after = cd.retry_at;
  } else if (r < 0.12) {
    a.status = "error";
    a.unavailable = true;
    a.status_message = "unauthorized (refresh token invalid)";
  } else if (r < 0.15) {
    const at = startedAt + int(1, 4) * 3_600_000;
    a.cooldowns.push({
      scope: "model",
      model_key: pick(MODELS[provider] || ["model"]),
      reason: "quota",
      retry_at: iso(at),
      remaining_seconds: Math.round((at - startedAt) / 1000),
      backoff_level: 1,
      http_status: 429,
    });
  }

  // Passive quota windows, as read from response headers.
  if ((provider === "codex" || provider === "claude" || provider === "devin") && !a.disabled && chance(0.75)) {
    const s = {};
    const used = () => (chance(0.12) ? int(86, 99) : int(1, 80));
    if (provider === "codex") {
      for (const [w, minutes] of [["primary", 300], ["secondary", 10080]]) {
        s[`x-codex-${w}-used-percent`] = String(used());
        s[`x-codex-${w}-window-minutes`] = String(minutes);
        s[`x-codex-${w}-reset-after-seconds`] = String(int(600, minutes * 60));
        s[`x-codex-${w}-reset-at`] = String(Math.floor(startedAt / 1000) + int(600, minutes * 60));
      }
    } else if (provider === "claude") {
      for (const [w, span] of [["5h", 18_000], ["7d", 604_800]]) {
        s[`anthropic-ratelimit-unified-${w}-utilization`] = (used() / 100).toFixed(2);
        s[`anthropic-ratelimit-unified-${w}-reset`] = String(Math.floor(startedAt / 1000) + int(600, span));
      }
    } else {
      s.daily_quota_remaining_percent = String(int(5, 100));
      s.daily_quota_reset_at = iso(startedAt + int(1, 20) * 3_600_000);
      s.weekly_quota_remaining_percent = String(int(5, 100));
      s.weekly_quota_reset_at = iso(startedAt + int(1, 6) * 86_400_000);
    }
    a.quota = { observed_at: iso(startedAt - int(1, 30) * 60_000), signals: s };
  }
  return a;
}

const accounts = [];
for (let i = 0; i < N; i++) accounts.push(makeAccount(i));
accounts.sort((x, y) => (x.name.toLowerCase() < y.name.toLowerCase() ? -1 : x.name.toLowerCase() > y.name.toLowerCase() ? 1 : 0));

/** The wire form of one account, cached until the account changes. */
function wire(a) {
  if (a._json) return a._json;
  const { _b, ...out } = a;
  delete out._json;
  out.recent_requests = _b.map(([s, f], i) => ({ time: bucketLabel(19 - i), success: s, failed: f }));
  // The real list is key-sorted (Go map ordering).
  const sorted = {};
  for (const k of Object.keys(out).sort()) sorted[k] = out[k];
  return (a._json = JSON.stringify(sorted));
}

/** Move every bucket window when the server-relative clock crosses a ten-minute boundary. */
function slideBuckets() {
  const idx = Math.floor((Date.now() - bucketOrigin) / BUCKET_MS);
  while (bucketIndex < idx) {
    bucketIndex++;
    for (const a of accounts) {
      a._b.shift();
      a._b.push([0, 0]);
      a._json = null;
    }
  }
}

/** Live traffic: about CHURN of the accounts that can serve receive requests. */
function churn() {
  const live = accounts.filter((a) => !a.disabled && !a.unavailable);
  const k = Math.min(live.length, Math.max(live.length ? 1 : 0, Math.round(accounts.length * CHURN)));
  const seen = new Set();
  while (seen.size < k) {
    const a = live[Math.floor(rand() * live.length)];
    if (seen.has(a)) continue;
    seen.add(a);
    const ok = int(1, 4);
    const bad = chance(0.1) ? 1 : 0;
    a._b[19][0] += ok;
    a._b[19][1] += bad;
    a.success += ok;
    a.failed += bad;
    a._json = null;
  }
}

const find = (name, index) =>
  accounts.find((a) => (name ? a.name === name || a.id === name : true) && (index ? a.auth_index === index : true) && (name || index));

// ---------------------------------------------------------------------------------------------
// Configuration (kebab-case, as GET /admin/v1/effective/config returns it)
const apiKey = () => `sk-${hex(48)}`;
let configVersion = 1;
let config = {
  "config-version": 8,
  server: { host: "", port: 8317, tls: { enable: false } },
  management: { "allow-remote": false, "secret-key": KEY, "disable-auto-update-panel": false },
  access: { "api-keys": [apiKey(), apiKey()] },
  routing: { strategy: "soonest-reset", retry: { "request-retry": 3, "max-retry-interval": 30 }, "session-affinity": true },
  requests: {
    "proxy-url": "",
    payload: {
      default: [{ models: [{ name: "gemini-*", protocol: "gemini" }], params: { "generationConfig.thinkingConfig.thinkingBudget": 32768 } }],
      "default-raw": [],
      override: [{ models: [{ name: "gpt-5*" }], params: { "reasoning.effort": "high" } }],
      "override-raw": [],
      filter: [],
    },
  },
  "api-keys": {
    claude: [{ name: "team", keys: [{ "api-key": `sk-ant-${hex(32)}` }] }],
    codex: [{ name: "relay", "base-url": "https://relay.example.invalid/v1", keys: [{ "api-key": `sk-${hex(32)}` }] }],
    gemini: [{ name: "studio", keys: [{ "api-key": `AIza${hex(35)}` }] }],
    vertex: [],
    "openai-compatibility": [{ name: "openrouter", "base-url": "https://openrouter.example.invalid/api/v1", keys: [{ "api-key": `sk-or-${hex(32)}` }] }],
    interactions: [],
    xai: [],
    meta: [],
  },
  oauth: {
    "model-alias": { claude: [{ name: "claude-sonnet-4-5", alias: "sonnet" }], codex: [] },
    "excluded-models": { claude: ["claude-3-*"], codex: [] },
  },
  client: {},
  multimedia: {},
  observability: { logs: { "logging-to-file": true }, usage: { enabled: true } },
};
const walk = (root, parts) => parts.reduce((v, k) => (v && typeof v === "object" && Object.hasOwn(v, k) ? v[k] : undefined), root);
const emptyLike = (v) => v == null || (typeof v === "object" && Object.keys(v).length === 0);
function withoutAuth(value) {
  if (Array.isArray(value)) return value.map(withoutAuth);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(Object.entries(value).filter(([key]) => !["auth_index", "auth-index"].includes(key)).map(([key, item]) => [key, withoutAuth(item)]));
}
let nextIdentity = 1;
const newIdentity = () => createHash("sha256").update(`${opt.seed}:${nextIdentity++}`).digest("hex").replace(/^(.{8})(.{4}).(.{3}).(.{3})(.{12}).*$/, "$1-$2-4$3-8$4-$5");
function identify(document, before = {}) {
  const providers = new Set();
  const routers = new Set();
  const clients = new Set();
  const assign = (entry, used, base) => {
    const valid = (id) => typeof id === "string" && /^[A-Za-z0-9_.-]+$/.test(id) && !used.has(id);
    let id = [entry["@id"], entry.name?.trim()].find(valid);
    for (let n = 1; !id; n++) if (valid(`${base}-${n}`)) id = `${base}-${n}`;
    entry["@id"] = id;
    used.add(id);
  };
  for (const [family, list] of Object.entries(document["api-keys"] || {})) {
    for (const entry of list || []) {
      assign(entry, providers, family);
      const children = new Set();
      for (const key of entry.keys || []) {
        if (!key["@id"]) key["@id"] = newIdentity();
        if (children.has(key["@id"])) throw new Error("duplicate provider key @id");
        children.add(key["@id"]);
        key.auth_index = createHash("sha256").update(`${entry["@id"]}:${key["api-key"]}`).digest("hex").slice(0, 16);
      }
    }
  }
  for (const entry of document["model-routers"] || []) assign(entry, routers, "router");
  if (document.access?.["api-keys"]) document.access["api-keys"] = document.access["api-keys"].map((key) => {
    const entry = typeof key === "string" ? { key } : key;
    if (!entry["@id"]) entry["@id"] = before.access?.["api-keys"]?.find((stored) => stored.key === entry.key)?.["@id"] || newIdentity();
    assign(entry, clients, "key");
    return entry;
  });
  return document;
}
identify(config);
const secretFields = new Set(["api-key", "key", "secret-key", "github-token", "credential", "username", "password", "token", "access-token", "refresh-token", "client-secret", "authorization", "cookie"]);
function mask(value, field = "", headers = false) {
  let credentialUrl = false;
  if (typeof value === "string") {
    try { const url = new URL(value); credentialUrl = !!(url.username || url.password || url.search); } catch { credentialUrl = false; }
  }
  if ((headers || secretFields.has(field) || credentialUrl) && value != null && value !== "") return "$secret";
  if (Array.isArray(value)) return value.map((item) => mask(item, field, headers));
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, mask(item, key, field === "headers" || field === "header-defaults")]));
}
function shownConfig(document) {
  const shown = mask(document);
  return shown;
}
function resolveHiddenSecrets(value, before) {
  if (value === "$secret") {
    if (before === undefined || before === "$secret") throw new Error("masked value has no stored identity");
    return structuredClone(before);
  }
  if (Array.isArray(value)) {
    const used = new Set();
    return value.map((item) => {
      let stored;
      if (JSON.stringify(item)?.includes('"$secret"')) {
        const candidates = (Array.isArray(before) ? before : []).filter((candidate) => item?.["@id"]
          ? candidate?.["@id"] === item["@id"]
          : isDeepStrictEqual(mask(withoutAuth(candidate)), mask(withoutAuth(item))));
        if (candidates.length !== 1 || used.has(candidates[0])) throw new Error("masked value has no unambiguous stored identity");
        stored = candidates[0];
        used.add(stored);
      }
      return resolveHiddenSecrets(item, stored);
    });
  }
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, resolveHiddenSecrets(item, before?.[key])]));
}
function resources(document) {
  const list = [];
  const add = (kind, id, spec, ord = 0) => list.push({ kind, id, ord, description: "", spec: withoutAuth(spec) });
  for (const [id, value] of Object.entries(document)) {
    if (["config-version", "api-keys", "model-routers"].includes(id)) continue;
    const spec = structuredClone(value);
    if (id === "access") delete spec["api-keys"];
    if (Object.keys(spec).length) add("setting", id, spec);
  }
  for (const [family, groups] of Object.entries(document["api-keys"] || {})) groups.forEach((entry, ord) => {
    const { "@id": id, ...spec } = entry;
    add("provider", id, { ...spec, type: family }, ord);
  });
  (document.access?.["api-keys"] || []).forEach((entry, ord) => add("client-key", entry["@id"], { key: entry.key }, ord));
  (document["model-routers"] || []).forEach(({ "@id": id, ...spec }, ord) => add("router", id, spec, ord));
  return list;
}
const resourceKey = ({ kind, id }) => `${kind}/${id}`;
const resourceRevisions = new Map(resources(config).map((resource) => [resourceKey(resource), { rev: 1, created_rev: 1 }]));
function readResources(document = config) {
  return resources(document).map((resource) => ({ ...resource, ...resourceRevisions.get(resourceKey(resource)) }));
}
const shownResource = (resource) => ({ ...resource, spec: mask(resource.spec) });
function replaceClientKey(document, id, spec, deleting) {
  if (!deleting && (typeof spec?.key !== "string" || !spec.key.trim() || Object.keys(spec).length !== 1)) throw new Error("client key must have a non-empty key");
  document.access ??= {};
  const list = document.access["api-keys"] ??= [];
  const index = list.findIndex((entry) => entry["@id"] === id);
  if (deleting) list.splice(index, 1);
  else if (index < 0) list.push({ "@id": id, key: spec.key });
  else list[index] = { "@id": id, key: spec.key };
}
function replaceResource(document, operation, existing) {
  const { kind, id } = operation;
  const deleting = operation.op === "delete";
  const spec = deleting ? undefined : resolveHiddenSecrets(withoutAuth(operation.op === "patch" ? mergePatch(existing.spec, operation.patch) : operation.spec), existing?.spec);
  if (kind === "client-key") replaceClientKey(document, id, spec, deleting);
  else throw new Error("unsupported resource kind");
}
function mergePatch(value, patch) {
  if (!patch || typeof patch !== "object" || Array.isArray(patch)) return structuredClone(patch);
  const result = value && typeof value === "object" && !Array.isArray(value) ? structuredClone(value) : {};
  for (const [key, item] of Object.entries(patch)) {
    if (item === null) delete result[key];
    else result[key] = mergePatch(result[key], item);
  }
  return result;
}
function parentAtPath(document, parts) {
  let parent = document;
  for (const key of parts) {
    if (!parent || typeof parent !== "object") throw new Error("not a mapping or list");
    if (Array.isArray(parent) && (!/^\d+$/.test(key) || Number(key) >= parent.length)) throw new Error("no list entry");
    if (!Object.hasOwn(parent, key)) Object.defineProperty(parent, key, { value: {}, enumerable: true, writable: true, configurable: true });
    parent = parent[key];
  }
  return parent;
}
function mutatePath(document, parts, value, unset) {
  if (!parts.length) {
    if (unset || !value || typeof value !== "object" || Array.isArray(value)) throw new Error("the whole config must be a mapping");
    return value;
  }
  const parent = parentAtPath(document, parts.slice(0, -1));
  const key = parts.at(-1);
  if (!parent || typeof parent !== "object") throw new Error("not a mapping or list");
  if (Array.isArray(parent)) {
    const index = Number(key);
    if (!/^\d+$/.test(key) || index > parent.length || (unset && index === parent.length)) throw new Error("no list entry");
    if (unset) parent.splice(index, 1);
    else parent[index] = value;
  } else if (unset) delete parent[key];
  else Object.defineProperty(parent, key, { value, enumerable: true, writable: true, configurable: true });
  return document;
}

// ---------------------------------------------------------------------------------------------
// Logs, OAuth sessions, usage
const logs = [];
const LOG_MESSAGES = [
  ["info", "GIN", "200 | 1.2ms | 127.0.0.1 | GET /v8/management/credentials"],
  ["info", "", "request: POST /v1/messages model=claude-sonnet-4-5 auth=%A"],
  ["info", "", "response: 200 OK in 2.3s tokens in=812 out=1204"],
  ["warn", "", "credential %A cooling after upstream 429, retry in 30m"],
  ["error", "", "upstream error 502 from api.anthropic.com, retrying on next credential"],
  ["debug", "", "scheduler picked %A strategy=soonest-reset"],
];
function logLine() {
  const [lvl, , msg] = pick(LOG_MESSAGES);
  const t = new Date().toISOString().replace("T", " ").slice(0, 19);
  const id = lvl === "debug" ? "--------" : hex(8);
  const a = accounts.length ? pick(accounts).auth_index : "none";
  return `[${t}] [${id}] [${lvl.padEnd(5)}] [handler.go:${int(40, 900)}] ${msg.replace("%A", a)}`;
}
for (let i = 0; i < 400; i++) logs.push(logLine());
const errorLogs = [
  { name: `error-v1-messages-2026-11-08T03-12-44-${hex(8)}.log`, size: 4231, modified: Math.floor(startedAt / 1000) - 3600 },
  { name: `error-v1-responses-2026-11-07T21-40-02-${hex(8)}.log`, size: 9120, modified: Math.floor(startedAt / 1000) - 36000 },
];

const sessions = new Map();
/** Live dashboard session ids. */
const dashboardSessions = new Set();

/** Per-provider usage payloads a quota check parses (src/core.ts quotaWindows). */
function usagePayload(url) {
  const used = () => int(3, 95);
  const reset = (h) => iso(Date.now() + h * 3_600_000);
  if (url.includes("anthropic.com"))
    return { five_hour: { utilization: used(), resets_at: reset(3) }, seven_day: { utilization: used(), resets_at: reset(90) } };
  if (url.includes("chatgpt.com"))
    return {
      rate_limit: {
        primary_window: { used_percent: used(), limit_window_seconds: 18000, reset_after_seconds: 7200 },
        secondary_window: { used_percent: used(), limit_window_seconds: 604800, reset_after_seconds: 300000 },
      },
    };
  if (url.includes("kimi"))
    return { usages: { "5h_window": { used: used(), limit: 100, reset_time: reset(2) }, weekly: { used: used(), limit: 100, reset_time: reset(80) } } };
  return {};
}

// ---------------------------------------------------------------------------------------------
// HTTP
const routes = [];
const on = (method, pattern, handler) => routes.push({ method, re: new RegExp(`^${pattern.startsWith("/admin/v1/") ? "" : "/v8/management"}${pattern}$`), handler });
const fail = (status, error, extra) => ({ status, body: { error, ...extra } });
const ok = (body = { status: "ok" }, status = 200) => ({ status, body });
const text = (status, body, type = "text/plain; charset=utf-8", headers = {}) => ({ status, raw: body, type, headers });

function obj(ctx) {
  try {
    const v = JSON.parse(ctx.body.toString("utf8") || "null");
    return v && typeof v === "object" && !Array.isArray(v) ? v : null;
  } catch {
    return null;
  }
}
const str = (v) => (typeof v === "string" ? v.trim() : "");

// Config: each operation observes earlier operations, but failures publish none of them.
on("GET", "/admin/v1/effective/config", () => ok(shownConfig(config)));
on("GET", "/admin/v1/r/([^/]+)", (ctx) => {
  const kind = decodeURIComponent(ctx.m[1]);
  if (!["setting", "provider", "client-key", "router"].includes(kind)) return fail(400, "invalid_kind");
  return { ...ok({ items: readResources().filter((resource) => resource.kind === kind).map(shownResource) }), headers: { ETag: `"${configVersion}"` } };
});
on("GET", "/admin/v1/r/([^/]+)/([^/]+)", (ctx) => {
  const resource = readResources().find((entry) => entry.kind === decodeURIComponent(ctx.m[1]) && entry.id === decodeURIComponent(ctx.m[2]));
  return resource ? { ...ok(shownResource(resource)), headers: { ETag: `"${resource.rev}"` } } : fail(404, "not_found");
});
on("GET", "/admin/v1/export", (ctx) => {
  const include = ctx.q.get("include_secrets");
  if (include !== null && !["true", "false"].includes(include)) return fail(400, "invalid_query");
  const items = resources(config).map((resource) => include === "true" ? resource : shownResource(resource));
  return ok({ format: "cooker-config", version: 1, revision: configVersion, resources: items });
});
function validOperation(operation) {
  if (!operation) return false;
  if (["set", "unset"].includes(operation.op)) return typeof operation.path === "string" && (operation.op !== "set" || Object.hasOwn(operation, "value"));
  return ["put", "patch", "delete"].includes(operation.op) && operation.kind === "client-key" && typeof operation.id === "string" && /^[A-Za-z0-9_.-]+$/.test(operation.id) && (operation.op !== "put" || Object.hasOwn(operation, "spec")) && (operation.op !== "patch" || Object.hasOwn(operation, "patch"));
}
function matchesExpectation(document, parts, operation) {
  if (!Object.hasOwn(operation, "expect")) return true;
  const now = withoutAuth(walk(shownConfig(document), parts));
  const expect = withoutAuth(operation.expect);
  return isDeepStrictEqual(now, expect) || (emptyLike(now) && emptyLike(expect));
}
function applyOperation(document, operation) {
  const before = structuredClone(document);
  if (["set", "unset"].includes(operation.op)) {
    const parts = operation.path.split("/").filter(Boolean);
    if (!matchesExpectation(document, parts, operation)) return { error: fail(412, "precondition_failed") };
    document = mutatePath(document, parts, withoutAuth(operation.value), operation.op === "unset");
    document = resolveHiddenSecrets(document, before);
  } else {
    const existing = readResources(document).find((resource) => resource.kind === operation.kind && resource.id === operation.id);
    if ((operation.if_absent && existing) || (operation.if_rev !== undefined && existing?.rev !== operation.if_rev)) return { error: fail(412, "precondition_failed") };
    if (operation.op !== "put" && !existing) return { error: fail(404, "not_found") };
    replaceResource(document, operation, existing);
  }
  return { document: identify(document, before) };
}
function publishConfig(next) {
  const before = new Map(resources(config).map((resource) => [resourceKey(resource), resource]));
  const after = resources(next);
  const keys = new Set(after.map(resourceKey));
  for (const key of resourceRevisions.keys()) if (!keys.has(key)) resourceRevisions.delete(key);
  for (const resource of after) {
    const key = resourceKey(resource);
    if (!isDeepStrictEqual(before.get(key), resource)) resourceRevisions.set(key, { rev: configVersion + 1, created_rev: resourceRevisions.get(key)?.created_rev ?? configVersion + 1 });
  }
  config = next;
  configVersion++;
}
on("POST", "/admin/v1/apply", (ctx) => {
  const body = obj(ctx);
  if (!body || !Array.isArray(body.ops)) return fail(400, "invalid_body");
  let next = structuredClone(config);
  for (const operation of body.ops) {
    if (!validOperation(operation)) return fail(400, "invalid_body");
    try {
      const result = applyOperation(next, operation);
      if (result.error) return result.error;
      next = result.document;
    } catch (error) {
      return fail(400, "invalid_path", { message: error.message });
    }
  }
  for (const path of ["credentials/concurrency/lifecycle-config-revision", "credentials/concurrency/observation-barrier-revision"]) {
    if (!isDeepStrictEqual(walk(config, path.split("/")), walk(next, path.split("/")))) return fail(400, "read_only_field");
  }
  const written = !isDeepStrictEqual(config, next);
  const restartRequired = ["host", "port", "tls", "trusted-proxies"].some((key) => !isDeepStrictEqual(config.server?.[key], next.server?.[key]));
  if (written && !body.dry_run) publishConfig(next);
  return ok({ revision: configVersion, written, changes: [], restart_required: restartRequired });
});

// Credentials
on("GET", "/credentials", (ctx) => {
  slideBuckets();
  churn();
  const name = str(ctx.q.get("name")),
    index = str(ctx.q.get("auth_index"));
  let list = accounts.filter((a) => (!name || a.name === name || a.id === name) && (!index || a.auth_index === index));
  const page = Number(ctx.q.get("page")),
    size = Number(ctx.q.get("page_size"));
  const observed = `"observed_at":${JSON.stringify(iso(Date.now()))}`;
  if (ctx.q.has("page") || ctx.q.has("page_size")) {
    if ((ctx.q.has("page") && !(page > 0)) || (ctx.q.has("page_size") && !(size > 0))) return fail(400, `${ctx.q.has("page") && !(page > 0) ? "page" : "page_size"} must be a positive integer`);
    const p = page || 1,
      s = size || 50;
    const start = Math.min((p - 1) * s, list.length),
      end = Math.min(start + s, list.length);
    return { status: 200, raw: `{${observed},"files":[${list.slice(start, end).map(wire).join(",")}],"total":${list.length},"page":${p},"page_size":${s},"has_more":${end < list.length}}`, type: "json" };
  }
  return { status: 200, raw: `{${observed},"files":[${list.map(wire).join(",")}]}`, type: "json" };
});
on("POST", "/credentials", (ctx) => {
  const type = ctx.req.headers["content-type"] || "";
  if (!type.startsWith("multipart/form-data")) return fail(400, "invalid multipart form");
  const raw = ctx.body.toString("utf8");
  const parts = [...raw.matchAll(/filename="([^"]+)"\r\n(?:[^\r\n]+\r\n)*\r\n([\s\S]*?)\r\n--/g)];
  if (!parts.length) return fail(400, "no files uploaded");
  const uploaded = [],
    failed = [];
  for (const [, file, content] of parts) {
    if (!file.endsWith(".json")) {
      failed.push({ name: file, error: "file must end with .json" });
      continue;
    }
    let meta;
    try {
      meta = JSON.parse(content);
    } catch {
      failed.push({ name: file, error: "invalid JSON" });
      continue;
    }
    const a = makeAccount(accounts.length);
    a.name = a.id = file;
    a.path = `/var/lib/cooker/auth/${file}`;
    if (typeof meta.type === "string") a.type = a.provider = meta.type;
    if (typeof meta.email === "string") a.email = a.account = meta.email;
    accounts.push(a);
    uploaded.push(file);
  }
  accounts.sort((x, y) => (x.name.toLowerCase() < y.name.toLowerCase() ? -1 : 1));
  return failed.length ? { status: 207, body: { status: "partial", uploaded: uploaded.length, files: uploaded, failed } } : ok({ status: "ok", uploaded: uploaded.length, files: uploaded });
});
on("DELETE", "/credentials", (ctx) => {
  const body = obj(ctx);
  const names = ctx.q.get("name") ? [ctx.q.get("name")] : Array.isArray(body?.names) ? body.names.map(str).filter(Boolean) : [];
  if (!names.length) return fail(400, "invalid request body");
  let deleted = 0;
  for (const n of names) {
    const i = accounts.findIndex((a) => a.name === n);
    if (i < 0) return deleted ? ok({ status: "partial", deleted }) : fail(404, "file not found");
    accounts.splice(i, 1);
    deleted++;
  }
  return ok({ status: "ok", deleted });
});
on("GET", "/credentials/models", (ctx) => {
  const name = ctx.q.get("name");
  if (!name) return fail(400, "name is required");
  const a = find(name);
  return ok({ models: a ? (MODELS[a.provider] || []).map((id) => ({ id, object: "model", created: 0, owned_by: a.provider, type: a.provider })) : [] });
});
on("GET", "/credentials/download", (ctx) => {
  const name = str(ctx.q.get("name"));
  if (!name || /[\\/]/.test(name)) return fail(400, "invalid name");
  if (!name.endsWith(".json")) return fail(400, "name must end with .json");
  const a = find(name);
  if (!a) return fail(404, "file not found");
  const body = JSON.stringify({ type: a.provider, email: a.email, access_token: "mock-access-token", refresh_token: "mock-refresh-token", expired: iso(Date.now() + 3_600_000) });
  return text(200, body, "application/json", { "Content-Disposition": `attachment; filename="${name}"` });
});
on("PATCH", "/credentials/status", (ctx) => {
  const b = obj(ctx);
  if (!b || typeof b.disabled !== "boolean" || !str(b.name)) return fail(400, "invalid request body");
  const a = find(str(b.name), str(b.auth_index));
  if (!a) return fail(404, "auth file not found");
  a.disabled = b.disabled;
  a.status = b.disabled ? "disabled" : "active";
  a.status_message = b.disabled ? "disabled via management API" : "";
  a._json = null;
  return ok({ status: "ok", disabled: a.disabled });
});
on("PATCH", "/credentials/fields", (ctx) => {
  const b = obj(ctx);
  const name = str(b?.name);
  if (!name) return fail(400, "name is required");
  const keys = ["note", "priority", "weight", "request_retry"].filter((k) => k in b);
  if (!keys.length) return fail(400, "no fields to update");
  const a = find(name);
  if (!a) return fail(404, "auth file not found");
  for (const k of keys) {
    if (b[k] === null || b[k] === "") delete a[k];
    else a[k] = b[k];
  }
  a._json = null;
  return ok();
});
on("POST", "/credentials/refresh", (ctx) => {
  const b = obj(ctx);
  if (b?.all === true) return ok({ ok: true, results: accounts.map((a) => ({ id: a.id, success: true })) });
  const name = str(b?.name);
  if (!name) return fail(400, "name or all=true is required");
  const a = find(name, str(b.auth_index));
  if (!a) return fail(404, "auth file not found");
  a.last_refresh = iso(Date.now());
  a._json = null;
  return ok({ ok: true, auth: JSON.parse(wire(a)) });
});
on("POST", "/routing/cooldown/reset", (ctx) => {
  const b = obj(ctx);
  if (!b) return fail(400, "invalid request body");
  const index = str(b.auth_index);
  if (!index) return fail(400, "auth_index is required");
  const a = find("", index);
  if (!a) return fail(404, "auth file not found");
  a.cooldowns = [];
  if (a.status === "error" && !/unauthorized/.test(a.status_message)) (a.status = "active"), (a.unavailable = false), delete a.next_retry_after;
  a._json = null;
  return ok({ status: "ok", auth_index: index });
});
on("GET", "/routing/model-definitions/([^/]+)", (ctx) => {
  const channel = decodeURIComponent(ctx.m[1]).trim().toLowerCase();
  const ids = MODELS[channel];
  if (!ids) return fail(400, "unknown channel", { channel });
  return ok({
    channel,
    models: ids.map((id) => ({ id, object: "model", created: 1_750_000_000, owned_by: channel, type: channel, display_name: id.replace(/-/g, " "), context_length: 200_000, max_completion_tokens: 64_000 })),
  });
});

// Requests
on("POST", "/requests/api-call", (ctx) => {
  const b = obj(ctx);
  if (!b || !str(b.url) || !str(b.method)) return fail(400, "invalid request body");
  const a = find("", str(b.authIndex ?? b.auth_index));
  if (str(b.authIndex ?? b.auth_index) && !a) return fail(404, "auth not found");
  return { status: 200, body: { status_code: 200, header: { "Content-Type": ["application/json"] }, body: JSON.stringify(usagePayload(str(b.url))) } };
});
on("GET", "/server/latest-version", () =>
  opt.release === "none"
    ? fail(502, "invalid_response", { message: "missing release version" })
    : ok({ "latest-version": opt.release }),
);

// Usage
on("GET", "/observability/usage/api-keys", () => {
  const entries = (items) =>
    Object.fromEntries(
      items.map((k) => [
        k,
        { success: 120, failed: 3, recent_requests: Array.from({ length: 20 }, (_, i) => ({ time: bucketLabel(19 - i), success: (i * 7) % 11, failed: i % 9 === 0 ? 1 : 0 })) },
      ]),
    );
  const body = {};
  for (const [family, groups] of Object.entries(config["api-keys"] || {})) {
    for (const g of groups) {
      const provider = family === "openai-compatibility" ? String(g.name || "").toLowerCase() : family;
      body[provider] = { ...(body[provider] || {}), ...entries((g.keys || []).map((k) => k.auth_index)) };
    }
  }
  return ok(body);
});
on("GET", "/observability/usage/queue", (ctx) => {
  const raw = str(ctx.q.get("count"));
  const count = raw ? Number(raw) : 1;
  if (!Number.isInteger(count) || count <= 0) return fail(400, "count must be a positive integer");
  const out = [];
  for (let i = 0; i < Math.min(count, int(0, 3)); i++) {
    const a = accounts.length ? pick(accounts) : null;
    const failed = chance(0.08);
    out.push({
      timestamp: iso(Date.now()),
      request_id: hex(8),
      provider: a?.provider || "claude",
      auth_index: a?.auth_index || "",
      model: pick(MODELS[a?.provider] || ["claude-sonnet-4-5"]),
      latency_ms: int(300, 9000),
      ttft_ms: int(80, 1500),
      failed,
      ...(failed ? { fail: { status_code: 502 } } : {}),
      tokens: { input_tokens: int(200, 4000), output_tokens: int(50, 2000) },
    });
  }
  return ok(out);
});

// OAuth
on("GET", "/oauth/auth-url", (ctx) => {
  const provider = str(ctx.q.get("provider")).toLowerCase();
  if (!provider) return fail(400, "provider is required");
  if (!MODELS[provider]) return fail(400, "unsupported provider", { code: "provider_not_found" });
  const state = hex(16);
  sessions.set(state, { provider, at: Date.now(), cancelled: false });
  return ok({ status: "ok", url: `https://auth.example.invalid/authorize?state=${state}`, state });
});
on("GET", "/oauth/status", (ctx) => {
  const s = sessions.get(str(ctx.q.get("state")));
  if (!s) return ok({ status: "error", error: "unknown or expired state" });
  return ok(s.cancelled ? { status: "error", error: "cancelled" } : { status: "wait" });
});
on("DELETE", "/oauth/session", (ctx) => {
  const s = sessions.get(str(ctx.q.get("state")));
  if (s) s.cancelled = true;
  return ok({ status: "ok", cancelled: !!s });
});
on("POST", "/oauth/callback", () => ok());
on("POST", "/oauth/import", (ctx) => {
  if (str(ctx.q.get("provider")) !== "vertex") return fail(400, "unsupported provider");
  return fail(400, "no file uploaded");
});

// Logs
on("GET", "/observability/logs", (ctx) => {
  if (chance(0.5)) logs.push(logLine());
  if (chance(0.3)) logs.push(logLine());
  const limit = Number(ctx.q.get("limit") || 0);
  if (ctx.q.has("limit") && !(limit > 0)) return fail(400, "invalid limit: must be a positive integer");
  const cursor = str(ctx.q.get("cursor"));
  const at = cursor ? Number(cursor.replace(/^c/, "")) : NaN;
  const reset = !!cursor && !(at >= 0 && at <= logs.length);
  const from = cursor && !reset ? at : Math.max(0, logs.length - (limit || logs.length));
  const lines = logs.slice(from);
  return ok({ lines, "line-count": lines.length, "latest-timestamp": Math.floor(Date.now() / 1000), "next-cursor": `c${logs.length}`, ...(reset ? { "cursor-reset": true } : {}) });
});
on("DELETE", "/observability/logs", () => {
  logs.length = 0;
  return ok({ message: "Logs cleared successfully", removed: 1 });
});
on("GET", "/observability/logs/errors", () => ok({ files: errorLogs }));
on("GET", "/observability/logs/errors/([^/]+)", (ctx) => {
  const name = decodeURIComponent(ctx.m[1]);
  if (!errorLogs.some((f) => f.name === name)) return fail(404, "log file not found");
  return text(200, `=== REQUEST INFO ===\nURL: /v1/messages\nMethod: POST\n\n=== RESPONSE ===\nStatus: 502\n`, "text/plain; charset=utf-8", { "Content-Disposition": `attachment; filename="${name}"` });
});
on("GET", "/observability/logs/requests/([^/]+)", (ctx) => {
  const id = decodeURIComponent(ctx.m[1]).trim();
  if (!id) return fail(400, "missing request ID");
  return text(200, `=== REQUEST INFO ===\nRequest ID: ${id}\nURL: /v1/messages\nMethod: POST\n\n=== RESPONSE ===\nStatus: 200\n`);
});

// ---------------------------------------------------------------------------------------------
// The proxy's model list for a client key; an unknown key gets 401, as from the proxy.
function clientModels(req, send) {
  const given = (req.headers.authorization || "").replace(/^Bearer\s+/i, "") || String(req.headers["x-api-key"] || "");
  const keys = config.access?.["api-keys"] || [];
  const type = { "Content-Type": "application/json; charset=utf-8" };
  if (!given || !keys.some((entry) => entry.key === given)) return send(401, type, '{"error":{"message":"invalid api key","type":"authentication_error"}}'), 401;
  const ids = [...new Set(Object.values(MODELS).flat())];
  send(200, type, JSON.stringify({ object: "list", data: ids.map((id) => ({ id, object: "model", created: 0, owned_by: "cooker" })) }));
  return 200;
}

const PREFIX = "/v8/management";
const server = http.createServer((req, res) => {
  const url = new URL(req.url, "http://mock");
  const send = (status, headers, body) => {
    res.writeHead(status, { "Content-Length": Buffer.byteLength(body ?? ""), ...headers });
    res.end(body);
  };
  const chunks = [];
  req.on("data", (c) => chunks.push(c));
  req.on("end", () => {
    const started = process.hrtime.bigint();
    let status = 404;
    try {
      if (url.pathname === "/healthz") return (status = 200), send(200, { "Content-Type": "application/json" }, '{"status":"ok"}');
      if (url.pathname === "/v1/models" && req.method === "GET") return (status = clientModels(req, send));
      if (![PREFIX, "/admin/v1"].some((prefix) => url.pathname.startsWith(prefix + "/") || url.pathname === prefix)) return send(404, {}, "");
      const exposed = url.pathname.startsWith("/admin/v1/") && req.method !== "OPTIONS" ? [EXPOSED_HEADERS, "X-Ck-Revision, X-Ck-Published-Revision"] : EXPOSED_HEADERS;
      const headers = { "X-Ck-Version": VERSION, "X-Ck-Commit": "0000000", "X-Ck-Build-Date": BUILD_DATE, "Cache-Control": "no-store", "X-Ck-Revision": String(configVersion), "X-Ck-Published-Revision": String(configVersion), "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, POST, PUT, PATCH, DELETE, OPTIONS", "Access-Control-Allow-Headers": "*", "Access-Control-Expose-Headers": exposed };
      if (req.method === "OPTIONS") return (status = 204), send(204, headers, "");
      const auth = req.headers.authorization || "";
      const given = /^bearer /i.test(auth) ? auth.slice(7) : "";
      // The dashboard session, as the server does it (crates/ck-server/src/management/api/session.rs):
      // the key is exchanged for a random id in `cooker_session`; cookie requests need the
      // dashboard header; sign-out revokes the id on the server.
      const id = /(?:^|;\s*)cooker_session=([^;]+)/.exec(req.headers.cookie || "")?.[1];
      const cookie = id !== undefined && dashboardSessions.has(id);
      const dashboard = req.headers["x-ck-dashboard"] === "1";
      if (url.pathname === `${PREFIX}/session`) {
        if (req.method !== "POST" && !dashboard) return (status = 403), send(403, headers, "");
        if (req.method === "GET") return (status = cookie ? 204 : 401), send(status, headers, "");
        if (req.method === "DELETE") {
          if (id !== undefined) dashboardSessions.delete(id);
          return (status = 204), send(204, { ...headers, "Set-Cookie": "cooker_session=; Path=/; Max-Age=0" }, "");
        }
        if (given !== KEY) return (status = 401), send(401, headers, "");
        let body;
        try {
          body = JSON.parse(Buffer.concat(chunks).toString() || "{}");
        } catch {
          return (status = 400), send(400, headers, "");
        }
        const fresh = randomBytes(32).toString("base64url");
        dashboardSessions.add(fresh);
        const age = body.remember ? 30 * 86400 : 7 * 86400;
        const path = typeof body.path === "string" && body.path.startsWith("/") ? body.path : "/";
        return (status = 204), send(204, { ...headers, "Set-Cookie": `cooker_session=${fresh}; Path=${path}; Max-Age=${age}; HttpOnly; SameSite=Strict` }, "");
      }
      if (!given && cookie && !dashboard) return (status = 403), send(403, headers, "");
      if (!given && !cookie) return (status = 401), send(401, { ...headers, "Content-Type": "application/json; charset=utf-8" }, '{"error":"missing management key"}');
      if (given && given !== KEY) return (status = 401), send(401, { ...headers, "Content-Type": "application/json; charset=utf-8" }, '{"error":"invalid management key"}');
      for (const r of routes) {
        if (r.method !== req.method) continue;
        const m = r.re.exec(url.pathname);
        if (!m) continue;
        const out = r.handler({ req, q: url.searchParams, body: Buffer.concat(chunks), m });
        status = out.status;
        if (out.raw !== undefined) {
          const type = out.type === "json" ? "application/json; charset=utf-8" : out.type;
          return send(out.status, { ...headers, "Content-Type": type, ...out.headers }, out.raw);
        }
        return send(out.status, { ...headers, ETag: `"${configVersion}"`, "X-Ck-Revision": String(configVersion), "X-Ck-Published-Revision": String(configVersion), "Content-Type": "application/json; charset=utf-8", ...out.headers }, JSON.stringify(out.body));
      }
      send(404, headers, "");
    } catch (e) {
      status = 500;
      console.error(e);
      send(500, {}, JSON.stringify({ error: "internal_error", message: String(e) }));
    } finally {
      if (!opt.quiet) console.log(`${req.method} ${url.pathname}${url.search} ${status} ${(Number(process.hrtime.bigint() - started) / 1e6).toFixed(1)}ms`);
    }
  });
});
server.listen(Number(opt.port), opt.host, () => {
  const { port } = server.address();
  console.log(`listening http://${opt.host}:${port} accounts=${accounts.length} seed=${opt.seed} key=${KEY}`);
});
for (const sig of ["SIGINT", "SIGTERM"]) process.on(sig, () => server.close(() => process.exit(0)));
