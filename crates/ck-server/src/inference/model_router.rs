//! Model routers (cooker only, see docs/DIFFERENCES-FROM-GO.md): a client names a
//! router from `model-routers` as its model, and the router picks the model and effort
//! that serve the request. A judge model (or, without one, request signals) grades the
//! request's difficulty from 0 to 4; the difficulty maps linearly onto the intelligence
//! index range of the candidates' cost/capability Pareto front, shifted by the router's
//! tier, and the cheapest candidate that meets it serves. Capability and cost come from
//! Artificial Analysis ([`crate::inference::aa`]). A conversation keeps its model while that model
//! stays available and fits the context, so its prompt cache stays warm.

use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, Instant};

use ck_core::config::Config;
use ck_core::exec::Operation;
use ck_core::format::Format;
use ck_translate::codec;
use ck_wire::gostr::GoStr;
use ck_wire::json;
use serde_yaml_ng::Value;

use crate::Runtime;
use crate::http::egress::{Framing, Output, Protocol};
use crate::inference::aa::Catalog;
use crate::inference::dispatch::{Call, Done, Served, Trace};
use crate::scheduling::registry::{self, Spec};

const STICKY_TTL: Duration = Duration::from_secs(3600);
const STICKY_MAX: usize = 65536;
// ponytail: one judge call per new user message, bounded here; a slower judge leaves the
// request to the signal heuristic.
const JUDGE_TIMEOUT: Duration = Duration::from_secs(10);
/// The user turn the judge sees, at most.
const JUDGE_TEXT: usize = 4000;
const TOP_DIFFICULTY: f64 = 4.0;
/// Effort labels a route's suffix accepts (`model(high)`).
const LEVELS: [&str; 6] = ["minimal", "low", "medium", "high", "xhigh", "max"];

/// The difficulty question, as omp-auto-model asks Jev; criterion `i` is score `i`.
const INSTRUCTIONS: &str = "How hard is this coding-agent task for a capable model? Judge the depth of reasoning it needs, not the amount of typing.";
const CRITERIA: [&str; 5] = [
    "trivial: a fixed answer, a lookup, or a mechanical edit with everything specified",
    "routine: a small, well-specified change in familiar code",
    "moderate: multi-step work, or a bug with a reproducible cause; some design choices",
    "hard: debugging without a clear repro, or design where subtle mistakes are likely",
    "frontier: correctness proofs, concurrency or security analysis, research-level problems",
];

/// The runtime's router state: the benchmark catalog and the conversation bindings.
#[derive(Default)]
pub struct State {
    catalog: RwLock<Option<Arc<Catalog>>>,
    sticky: Mutex<HashMap<(String, String), Binding>>,
}

struct Binding {
    pattern: String,
    model: String,
    /// The bound variant's intelligence index: a later turn only moves to a stronger one.
    index: f64,
    expires: Instant,
}

/// A conversation's current route.
struct Bound {
    pattern: String,
    model: String,
    index: f64,
}

impl State {
    pub(crate) fn catalog(&self) -> Option<Arc<Catalog>> {
        self.catalog.read().unwrap_or_else(PoisonError::into_inner).clone()
    }

    pub(crate) fn publish(&self, catalog: Catalog) {
        *self.catalog.write().unwrap_or_else(PoisonError::into_inner) = Some(Arc::new(catalog));
    }

    /// The conversation's route, refreshing the binding.
    fn bound(&self, key: &(String, String), now: Instant) -> Option<Bound> {
        let mut sticky = self.sticky.lock().unwrap_or_else(PoisonError::into_inner);
        let binding = sticky.get_mut(key).filter(|b| b.expires > now)?;
        binding.expires = now + STICKY_TTL;
        Some(Bound {
            pattern: binding.pattern.clone(),
            model: binding.model.clone(),
            index: binding.index,
        })
    }

    fn bind(&self, key: (String, String), pick: &Variant, now: Instant) {
        let mut sticky = self.sticky.lock().unwrap_or_else(PoisonError::into_inner);
        if sticky.len() >= STICKY_MAX {
            sticky.retain(|_, b| b.expires > now);
            // ponytail: a full table of live conversations starts over; LRU if that happens.
            if sticky.len() >= STICKY_MAX {
                sticky.clear();
            }
        }
        sticky.insert(
            key,
            Binding {
                pattern: pick.pattern.clone(),
                model: pick.model.clone(),
                index: pick.index,
                expires: now + STICKY_TTL,
            },
        );
    }
}

#[derive(Clone, Default)]
pub(crate) struct AaSettings {
    pub key: String,
    pub url: String,
    pub cache_file: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Tier {
    Cost,
    Balanced,
    Quality,
}

impl Tier {
    /// Shift of the difficulty-derived requirement, as a share of the front's index
    /// range. Calibrated on 15 labelled tasks against Jev 1.13 (omp-auto-model): these
    /// keep cost < balanced < quality in both capability and spend.
    fn shift(self) -> f64 {
        match self {
            Self::Cost => -0.15,
            Self::Balanced => 0.0,
            Self::Quality => 0.2,
        }
    }
}

struct Entry {
    /// The model as configured, optionally with an effort suffix that pins the effort.
    model: String,
    /// The Artificial Analysis name when it differs from the model's display name.
    aa: String,
    /// The user's blended price per million tokens, when set.
    price: Option<f64>,
}

struct Router {
    name: String,
    tier: Tier,
    /// The model that grades requests, as clients name it (`opencode-zen/jev-1.13`).
    judge: String,
    /// The candidates; empty means every available model.
    models: Vec<Entry>,
    /// The router's provider scope and order over `routing.providers`
    /// ([`crate::scheduling::provider_policy`]); the judge follows only `routing.providers`.
    scope: Arc<crate::scheduling::provider_policy::Providers>,
    /// How many other models a request may move to when its model fails.
    fallback_models: usize,
}

#[derive(Default)]
pub(crate) struct Settings {
    pub aa: AaSettings,
    routers: Vec<Router>,
}

/// The router settings of a config snapshot, parsed once per snapshot.
pub(crate) fn settings(cfg: &Config) -> Arc<Settings> {
    cfg.derived(parse)
}

fn text(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        v => ck_core::config::go_string(v).trim().to_owned(),
    }
}

fn number(v: &Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_i64().map(|n| n as f64))
}

fn parse(cfg: &Config) -> Settings {
    let doc = &cfg.document;
    let aa = &doc["artificial-analysis"];
    let url = text(&aa["url"]);
    let cache_file = text(&aa["cache-file"]);
    let mut routers: Vec<Router> = Vec::new();
    let global = crate::scheduling::provider_policy::Providers::global(cfg);
    for r in doc["model-routers"].as_sequence().into_iter().flatten() {
        let name = text(&r["name"]);
        if name.is_empty() || name == "auto" || routers.iter().any(|o| o.name == name) {
            tracing::warn!("model-routers: skipping an entry without a usable, unique name");
            continue;
        }
        let tier = match text(&r["tier"]).go_lower().as_str() {
            "" | "balanced" => Tier::Balanced,
            "cost" => Tier::Cost,
            "quality" => Tier::Quality,
            other => {
                tracing::warn!("model-routers: {name}: unknown tier {other:?}, using balanced");
                Tier::Balanced
            }
        };
        let models = r["models"]
            .as_sequence()
            .into_iter()
            .flatten()
            .filter_map(|m| {
                let entry = match m {
                    Value::Mapping(_) => Entry {
                        model: text(&m["model"]),
                        aa: text(&m["aa"]),
                        price: match (number(&m["price"]["input"]), number(&m["price"]["output"])) {
                            (Some(input), Some(output)) => Some(crate::inference::aa::blend(input, output)),
                            _ => None,
                        },
                    },
                    m => Entry {
                        model: text(m),
                        aa: String::new(),
                        price: None,
                    },
                };
                if entry.model.is_empty() {
                    tracing::warn!("model-routers: {name}: skipping a model entry without a model");
                    return None;
                }
                Some(entry)
            })
            .collect();
        // A model of this proxy: Jev through its provider, or any chat model.
        let judge = r["judge"].as_str().map(str::trim).unwrap_or_default().to_owned();
        if judge.is_empty() {
            tracing::warn!("model-routers: {name}: skipped, a router needs a judge model");
            continue;
        }
        routers.push(Router {
            judge,
            name,
            tier,
            models,
            scope: Arc::new(global.within(&r["providers"])),
            fallback_models: r["fallback-models"].as_u64().unwrap_or(0) as usize,
        });
    }
    Settings {
        aa: AaSettings {
            key: text(&aa["api-key"]),
            url: if url.is_empty() {
                crate::inference::aa::DEFAULT_URL.to_owned()
            } else {
                url
            },
            cache_file: (!cache_file.is_empty()).then(|| PathBuf::from(cache_file)),
        },
        routers,
    }
}

/// The configured router names, for the model lists.
pub(crate) fn listed(cfg: &Config) -> Vec<Spec> {
    settings(cfg)
        .routers
        .iter()
        .map(|r| Spec {
            id: r.name.clone(),
            object: "model".into(),
            owned_by: "cooker".into(),
            display_name: r.name.clone(),
            ..Default::default()
        })
        .collect()
}

/// One model at one effort, with its measured capability and cost at the user's prices.
#[derive(Debug, Clone, PartialEq)]
struct Variant {
    /// The route sent on: the model ID, with an effort suffix when the measurement has one.
    pattern: String,
    model: String,
    index: f64,
    cost: f64,
}

/// Go `thinking.ParseSuffix` without trimming: `(base, Some(suffix))`.
fn split_suffix(model: &str) -> (&str, Option<&str>) {
    match model.rfind('(') {
        Some(open) if model.ends_with(')') => (&model[..open], Some(&model[open + 1..model.len() - 1])),
        _ => (model, None),
    }
}

/// The measured variants of `spec` its route can send. `entry` is its configured
/// candidate, if any: its suffix keeps only that effort, its `aa` names the measurements.
fn variants(spec: &Spec, entry: Option<&Entry>, catalog: &Catalog) -> Vec<Variant> {
    let pin = entry
        .and_then(|e| split_suffix(&e.model).1)
        .map(|p| p.trim().to_ascii_lowercase());
    let measured = match entry.filter(|e| !e.aa.is_empty()) {
        Some(e) => catalog.variants(&e.aa),
        None => match catalog.variants(&spec.display_name) {
            [] => catalog.variants(&spec.id),
            found => found,
        },
    };
    let id = &spec.id;
    measured
        .iter()
        .filter_map(|m| {
            let level = m.level.as_deref();
            let pattern = match (pin.as_deref(), level) {
                (Some(pin), Some(level)) if level != pin => return None,
                (Some(pin), _) => format!("{id}({pin})"),
                (None, None) => id.clone(),
                (None, Some("off")) => match &spec.thinking {
                    None => id.clone(),
                    Some(t) if t.zero_allowed => format!("{id}(none)"),
                    Some(_) => return None,
                },
                // A level this route does not offer (Claude `max` on a route that tops out
                // at `xhigh`) or a label that is no effort.
                (None, Some(level)) => match &spec.thinking {
                    Some(t)
                        if LEVELS.contains(&level) && (t.levels.is_empty() || t.levels.iter().any(|l| l == level)) =>
                    {
                        format!("{id}({level})")
                    }
                    _ => return None,
                },
            };
            let cost = match entry.and_then(|e| e.price) {
                Some(price) if price > 0.0 && m.blended_price > 0.0 => m.task_cost * price / m.blended_price,
                _ => m.task_cost,
            };
            Some(Variant {
                pattern,
                model: id.clone(),
                index: m.index,
                cost,
            })
        })
        .collect()
}

fn cheaper(a: &&Variant, b: &&Variant) -> std::cmp::Ordering {
    a.cost.total_cmp(&b.cost).then(b.index.total_cmp(&a.index))
}

/// How far below the requirement a variant's index may be and still meet it: index gaps
/// this small are within benchmark noise, and a cheaper variant should not lose to one
/// that costs much more for them.
// ponytail: a fixed point; per-benchmark confidence intervals if Artificial Analysis
// publishes them.
const INDEX_TOLERANCE: f64 = 1.0;

/// The index a variant needs for `difficulty` (0..4) under `tier`, less
/// [`INDEX_TOLERANCE`]. The requirement maps difficulty linearly onto the index range of
/// the cost/capability Pareto front: its top is the best index, its bottom the cheapest
/// variant's (which no variant dominates), so dominated variants never stretch the scale.
fn requirement(pool: &[Variant], difficulty: f64, tier: Tier) -> Option<f64> {
    let low = pool.iter().min_by(cheaper)?.index;
    let high = pool.iter().map(|v| v.index).fold(f64::NEG_INFINITY, f64::max);
    let share = (difficulty / TOP_DIFFICULTY + tier.shift()).clamp(0.0, 1.0);
    Some(low + share * (high - low) - INDEX_TOLERANCE)
}

/// The cheapest variant whose index meets `need`.
fn cheapest(pool: &[Variant], need: f64) -> Option<&Variant> {
    pool.iter().filter(|v| v.index >= need).min_by(cheaper)
}

/// Whether an input item is the user's: it has the `user` role, or it is a Responses
/// `message` that omits its role (developer, system and assistant items name theirs).
fn is_user(item: &json::Res) -> bool {
    let role = item.get("role").str();
    role == "user" || (role.is_empty() && item.get("type").str() == "message")
}

/// The tags around a client's reminder block (Claude Code's date and memory notes).
const REMINDER_OPEN: &str = "<system-reminder>";
const REMINDER_CLOSE: &str = "</system-reminder>";

/// `text` without its complete reminder blocks, trimmed: they are the client's notes to the
/// model, not the user's words. An unclosed tag is kept as written. One pass over `text`.
fn without_reminders(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find(REMINDER_OPEN) {
        let Some(len) = rest[start..].find(REMINDER_CLOSE) else {
            break;
        };
        out.push_str(&rest[..start]);
        rest = &rest[start + len + REMINDER_CLOSE.len()..];
    }
    out.push_str(rest);
    out.trim().to_owned()
}

/// The text of the request's latest user message (Chat Completions, Responses, Messages or
/// Gemini) without reminder blocks, truncated to [`JUDGE_TEXT`] bytes. A turn left without
/// text (a tool result with a reminder appended) yields to the user message before it; text
/// beside a tool result is kept.
fn last_user_text(body: &[u8]) -> String {
    let mut out = String::new();
    for key in ["messages", "input", "contents"] {
        let list = json::get(body, key);
        if !list.exists() {
            continue;
        }
        out = if list.kind == json::Kind::String {
            without_reminders(&list.str())
        } else {
            list.array()
                .into_iter()
                .rev()
                .filter(is_user)
                .find_map(|turn| {
                    let content = turn.get("content");
                    let text = if content.kind == json::Kind::String {
                        without_reminders(&content.str())
                    } else {
                        let parts = if content.exists() { content } else { turn.get("parts") };
                        parts
                            .array()
                            .iter()
                            .map(|p| p.get("text"))
                            .filter(|t| t.kind == json::Kind::String)
                            .map(|t| without_reminders(&t.str()))
                            .filter(|t| !t.is_empty())
                            .collect::<Vec<_>>()
                            .join("\n")
                    };
                    (!text.is_empty()).then_some(text)
                })
                .unwrap_or_default()
        };
        break;
    }
    if out.len() > JUDGE_TEXT {
        let mut end = JUDGE_TEXT;
        while !out.is_char_boundary(end) {
            end -= 1;
        }
        out.truncate(end);
    }
    out
}

/// The client's effort: the router suffix (`smart(high)`), else the body's.
fn effort(suffix: Option<&str>, body: &[u8]) -> String {
    if let Some(s) = suffix.map(str::trim).filter(|s| !s.is_empty()) {
        return s.to_ascii_lowercase();
    }
    [
        "reasoning_effort",
        "reasoning.effort",
        "output_config.effort",
        "generationConfig.thinkingConfig.thinkingLevel",
    ]
    .iter()
    .map(|path| json::get(body, *path))
    .find(|v| v.kind == json::Kind::String)
    .map(|v| v.str().trim().to_ascii_lowercase())
    .unwrap_or_default()
}

/// Difficulty from request signals, only when the judge gives no grade: tools mean agentic
/// work, a long context means more to reason over, a short tool-free prompt is trivial.
fn heuristic(body: &[u8], tokens: usize) -> f64 {
    let tools = json::get(body, "tools.0").exists();
    match (tools, tokens) {
        (false, t) if t < 1_000 => 0.0,
        (tools, t) => 1.0 + f64::from(u8::from(tools)) + f64::from(u8::from(t >= 50_000)),
    }
}

/// Base64 runs at least this long are attachments (images, documents), not text.
const ATTACHMENT_RUN: usize = 4096;
/// What one attachment counts as: about one image at Claude's and GPT's rates.
const ATTACHMENT_TOKENS: usize = 1600;

/// The request's input tokens, estimated in one pass: 4 bytes a token for text, and
/// [`ATTACHMENT_TOKENS`] for each base64 attachment, whose bytes are no measure of its
/// tokens (a 1 MB screenshot is not 250,000 tokens).
// ponytail: a byte heuristic; a tokenizer per candidate if context fits misjudge.
fn estimate_tokens(body: &[u8]) -> usize {
    let (mut attached, mut attachments, mut run) = (0, 0, 0);
    for &b in body {
        if b.is_ascii_alphanumeric() || matches!(b, b'+' | b'/' | b'=') {
            run += 1;
            continue;
        }
        if run >= ATTACHMENT_RUN {
            attached += run;
            attachments += 1;
        }
        run = 0;
    }
    if run >= ATTACHMENT_RUN {
        attached += run;
        attachments += 1;
    }
    (body.len() - attached) / 4 + attachments * ATTACHMENT_TOKENS
}

/// Whether the request ends with a new user message (Chat Completions, Responses,
/// Messages or Gemini), rather than tool results inside an agent loop.
fn fresh_turn(body: &[u8]) -> bool {
    for key in ["messages", "input", "contents"] {
        let list = json::get(body, key);
        if !list.exists() {
            continue;
        }
        if list.kind == json::Kind::String {
            return true;
        }
        let Some(last) = list.array().pop() else {
            return false;
        };
        if !is_user(&last) {
            return false;
        }
        let content = last.get("content");
        if content.kind == json::Kind::String {
            return true;
        }
        let parts = if content.exists() { content } else { last.get("parts") }.array();
        // Claude Code appends text reminders to tool results: any tool result means a loop.
        let tool_result = parts.iter().any(|p| {
            matches!(&*p.get("type").str(), "tool_result" | "function_call_output")
                || p.get("functionResponse").exists()
        });
        return !tool_result && parts.iter().any(|p| p.get("text").kind == json::Kind::String);
    }
    false
}

/// How the client's effort moves the difficulty.
fn effort_shift(effort: &str) -> f64 {
    match effort {
        "none" | "minimal" | "low" => -1.0,
        "high" => 1.0,
        "xhigh" | "max" => 2.0,
        _ => 0.0,
    }
}

/// The judge's grade (0 to 4, fractional for Jev) of the request's last user message.
/// `None` when it has no text or the judge gives no usable answer (logged).
async fn judge(rt: &Arc<Runtime>, call: &Call, model: &str, routers: &[Router]) -> Option<f64> {
    let text = last_user_text(&call.body);
    // A judge that names a router (with or without a suffix) would route its own call.
    if text.trim().is_empty() || routers.iter().any(|r| r.name == split_suffix(model).0.trim()) {
        return None;
    }
    let routes = judgment_routes(rt, model);
    let graded = if routes.is_empty() {
        ask_chat(rt, call, model, &text).await
    } else {
        ask_jev(rt, &routes, &text).await
    };
    match graded {
        Ok(grade) => Some(grade.clamp(0.0, TOP_DIFFICULTY)),
        Err(reason) => {
            tracing::warn!("model router: judge {model} gave no grade ({reason}); using request signals");
            None
        }
    }
}

/// The credentials and upstream model names that serve `model` when it is a Jev judgment
/// model, in `routing.providers` order (then priority); empty for a chat model. Jev
/// answers System One requests at its provider's `{base-url}/systemone` (OpenCode Zen:
/// `https://opencode.ai/zen/v1`).
// ponytail: Jev is told apart by its name; a model flag if other judgment models appear.
fn judgment_routes(rt: &Runtime, model: &str) -> Vec<(Arc<ck_core::credential::Credential>, String)> {
    let registry = rt.registry();
    let providers = registry.providers(model);
    if providers.is_empty() {
        return Vec::new();
    }
    let aliases = crate::scheduling::registry::global_aliases(&rt.config());
    let scope = rt.policy().providers.clone();
    let mut routes: Vec<_> = rt
        .store()
        .snapshot()
        .into_iter()
        .filter_map(|c| {
            let serves = !c.disabled
                && providers.contains(&crate::scheduling::registry::provider_key(&c))
                && registry.client_supports(&c.id, model)
                && !scope.excludes_direct(&c, model)
                && !rt.store().blocked(&c, model)
                && c.attributes.get("base_url").is_some_and(|b| !b.trim().is_empty());
            if !serves {
                return None;
            }
            let upstream = crate::scheduling::registry::execution_models(&aliases, &c, model)
                .0
                .into_iter()
                .next()?;
            upstream.go_lower().starts_with("jev").then_some((c, upstream))
        })
        .collect();
    routes.sort_by_key(|(c, _)| std::cmp::Reverse(scope.rank(c)));
    routes
}

/// Asks Jev through each of `routes` in turn until one grades, within [`JUDGE_TIMEOUT`]
/// in all. Each attempt gets an equal share of what is left, so a credential that hangs
/// cannot use up the next one's turn. Outside the scheduler: a judgment neither binds
/// nor cools a credential.
async fn ask_jev(
    rt: &Runtime,
    routes: &[(Arc<ck_core::credential::Credential>, String)],
    task: &str,
) -> Result<f64, String> {
    let deadline = Instant::now() + JUDGE_TIMEOUT;
    let mut reason = String::new();
    for (i, (credential, upstream)) in routes.iter().enumerate() {
        let share = deadline.saturating_duration_since(Instant::now()) / (routes.len() - i) as u32;
        if share.is_zero() {
            return Err(format!("{reason}, then timed out"));
        }
        reason = match tokio::time::timeout(share, ask_system_one(rt, credential, upstream, task, share)).await {
            Ok(Ok(grade)) => return Ok(grade),
            Ok(Err(reason)) => reason,
            Err(_) => "timed out".to_owned(),
        };
        let name = rt.policy().providers.name(credential);
        tracing::debug!(provider = %name, "model router: judge gave no grade ({reason})");
    }
    Err(reason)
}

/// HTTP clients for System One judgments, one per proxy, kept across requests.
static JUDGE_CLIENTS: std::sync::LazyLock<ck_exec::proxy::GoClients> =
    std::sync::LazyLock::new(|| ck_exec::proxy::GoClients::new(Default::default()));

/// Asks Jev the difficulty question with its provider's credential; its score is the grade.
async fn ask_system_one(
    rt: &Runtime,
    credential: &Arc<ck_core::credential::Credential>,
    model: &str,
    task: &str,
    timeout: Duration,
) -> Result<f64, String> {
    let credential = rt.for_executor(credential);
    let attribute = |key: &str| credential.attributes.get(key).map(|v| v.trim()).unwrap_or_default();
    let url = format!("{}/systemone", attribute("base_url").trim_end_matches('/'));
    let body = serde_json::json!({
        "model": model,
        "state": {"task": task},
        "questions": {"difficulty": {"type": "score", "instructions": INSTRUCTIONS, "criteria": CRITERIA}},
    });
    let mut headers = ck_exec::proxy::GoHeaders::new();
    headers.set("content-type", "application/json");
    headers.set("user-agent", "cooker");
    let key = attribute("api_key");
    if !key.is_empty() {
        headers.set("authorization", format!("Bearer {key}"));
    }
    let body = serde_json::to_vec(&body).map_err(|e| e.to_string())?;
    let client = JUDGE_CLIENTS.get(&ck_exec::proxy::Proxy::effective(&credential, &rt.config()));
    let response = ck_exec::proxy::request(
        &client,
        wreq::Method::POST,
        &url,
        headers,
        Some(body.into()),
        Some(timeout),
    )
    .await
    .map_err(|_| "request failed".to_owned())?;
    if response.status != 200 {
        return Err(format!("HTTP {}", response.status));
    }
    let answer = ck_exec::proxy::read_all(response.body, 1 << 20, false)
        .await
        .map_err(|_| "body read failed".to_owned())?;
    let score = json::get(&answer, "answers.difficulty.score");
    if score.kind != json::Kind::Number {
        return Err("no difficulty score".to_owned());
    }
    Ok(score.float())
}

/// Asks a chat model of this proxy for the grade, through the ordinary dispatcher.
async fn ask_chat(rt: &Arc<Runtime>, call: &Call, model: &str, task: &str) -> Result<f64, String> {
    let mut rubric = format!("{INSTRUCTIONS} Reply with one digit only:");
    for (i, criterion) in CRITERIA.iter().enumerate() {
        rubric.push_str(&format!("\n{i} {criterion}"));
    }
    let body = serde_json::json!({
        "model": model,
        "messages": [{"role": "system", "content": rubric}, {"role": "user", "content": task}],
    });
    let judged = Call {
        entry: Format::OpenAI,
        response: Format::OpenAI,
        operation: Operation::Generate,
        model: model.to_owned(),
        body: serde_json::to_vec(&body).map_err(|e| e.to_string())?.into(),
        stream: false,
        alt: None,
        headers: Default::default(),
        caller: call.caller.clone(),
        forced_provider: None,
        selection_model: None,
        execution_session: None,
        request_path: "/v1/chat/completions".into(),
        peer: None,
        socket: None,
        media: None,
    };
    let trace = Trace::with_request_id(None);
    let output = Output {
        protocol: Protocol::Chat,
        operation: codec::Operation::Generate,
        framing: Framing::Json,
    };
    let answer = async {
        match crate::inference::dispatch::exchange::run(rt, judged, output, &trace).await {
            Ok(Served::Old(Done::Buffered { body, .. })) => Ok(body),
            Ok(Served::Old(Done::Stream { .. })) => Err("streamed answer".to_owned()),
            Ok(Served::Committed(response)) => axum::body::to_bytes(response.into_body(), 1 << 20)
                .await
                .map_err(|_| "body read failed".to_owned()),
            Err(failure) => Err(format!("HTTP {}", failure.status())),
        }
    };
    let body = tokio::time::timeout(JUDGE_TIMEOUT, answer)
        .await
        .map_err(|_| "timed out".to_owned())??;
    let answer = json::get(&body, "choices.0.message.content").str().into_owned();
    grade(&answer)
        .map(f64::from)
        .ok_or_else(|| "no digit 0 to 4".to_owned())
}

/// A chat judge's grade: the first digit of its answer, when it is 0 to 4.
fn grade(answer: &str) -> Option<i32> {
    let digit = answer.chars().find(char::is_ascii_digit)?.to_digit(10)?;
    (digit <= 4).then_some(digit as i32)
}

/// A router's decision for one request.
pub(crate) struct Routed {
    /// The model the request goes to, with an effort suffix.
    pub model: String,
    /// The router's provider scope and order, for the request's credential selections.
    pub scope: Arc<crate::scheduling::provider_policy::Providers>,
    fallback: Fallback,
}

/// What [`fall_back`] needs to pick another model for the same request.
struct Fallback {
    router: String,
    /// Moves left (`fallback-models`).
    left: usize,
    /// The index a replacement should meet; `None` without benchmarks, where the next
    /// listed model serves.
    need: Option<f64>,
    /// Models (IDs) this request already went to.
    tried: Vec<String>,
    /// The conversation's binding key, when the request decides its model.
    key: Option<(String, String)>,
    /// The pick the conversation binds to once it served ([`Routed::commit`]).
    bind: Option<Variant>,
    suffix: Option<String>,
    tokens: usize,
}

/// The decision of a router named by `call.model`; `None` when the call names no router.
/// Boxed: the judge's dispatch re-enters the dispatcher that called this router.
pub(crate) fn resolve<'a>(
    rt: &'a Arc<Runtime>,
    call: &'a Call,
) -> Pin<Box<dyn Future<Output = Option<Routed>> + Send + 'a>> {
    Box::pin(route(rt, call))
}

/// Whether a request that failed this way may move to another model: not when the
/// request itself is at fault (another model fails it too).
pub(crate) fn fails_over(failure: &crate::inference::dispatch::Failure) -> bool {
    use crate::inference::dispatch::Failure;
    match failure {
        Failure::Exec(e) => !crate::scheduling::classify::is_request_invalid(e),
        Failure::Unavailable { .. } | Failure::Cooldown { .. } => true,
        _ => false,
    }
}

/// The pattern sent on, with the client's suffix on a route that sets no effort of its own.
fn finish(pattern: &str, suffix: Option<&str>) -> String {
    match (split_suffix(pattern).1, suffix) {
        (None, Some(s)) => format!("{pattern}({s})"),
        _ => pattern.to_owned(),
    }
}

/// A listed candidate as sent without benchmarks: with its configured effort, if any.
fn unrated(spec: &Spec, entry: Option<&Entry>) -> String {
    match entry.and_then(|e| split_suffix(&e.model).1) {
        Some(pin) => format!("{}({pin})", spec.id),
        None => spec.id.clone(),
    }
}

/// A credential's state for the router's model lists: out of its scope counts as
/// unusable.
fn usable(rt: &Runtime, router: &Router, credential: &str, model: &str) -> crate::scheduling::registry::Suspension {
    match rt.store().get(credential) {
        Some(c) if router.scope.excludes(&c) => registry::Suspension::Other { quota_exceeded: false },
        _ => rt.suspension(credential, model),
    }
}

/// The router's candidates: its listed models (or every available one) that a usable
/// credential in its scope serves and whose context fits `tokens`.
fn candidates<'a>(
    rt: &'a Runtime,
    router: &'a Router,
    registry: &'a crate::scheduling::registry::Registry,
    tokens: usize,
) -> Vec<(&'a Spec, Option<&'a Entry>)> {
    let fits = |s: &Spec| {
        let window = if s.context_length > 0 {
            s.context_length
        } else {
            s.input_token_limit
        };
        window <= 0 || tokens as i64 <= window
    };
    let available: Vec<&Spec> = registry
        .available_with(|c, m| usable(rt, router, c, m))
        .filter(|s| fits(s))
        .collect();
    if router.models.is_empty() {
        return available.iter().map(|s| (*s, None)).collect();
    }
    let find = |model: &str| available.iter().copied().find(|s| s.id.go_eq_fold(model.trim()));
    router
        .models
        .iter()
        .filter_map(|e| find(split_suffix(&e.model).0).map(|s| (s, Some(e))))
        .collect()
}

fn pool(candidates: &[(&Spec, Option<&Entry>)], catalog: Option<Arc<Catalog>>) -> Vec<Variant> {
    match catalog {
        Some(catalog) => candidates
            .iter()
            .flat_map(|(spec, entry)| variants(spec, *entry, &catalog))
            .collect(),
        None => Vec::new(),
    }
}

async fn route(rt: &Arc<Runtime>, call: &Call) -> Option<Routed> {
    let (base, suffix) = split_suffix(&call.model);
    let suffix = suffix.map(str::trim).filter(|s| !s.is_empty());
    let settings = settings(&rt.config());
    let router = settings.routers.iter().find(|r| r.name == base.trim())?;
    // A media route needs a media model, not a chat one.
    if call.media.is_some() {
        return None;
    }
    let tokens = estimate_tokens(&call.body);
    let registry = rt.registry();
    let candidates = candidates(rt, router, &registry, tokens);
    // Counting tokens neither pays for a judge nor decides the conversation's model.
    let counting = call.operation == Operation::CountTokens;
    let now = Instant::now();
    let key = crate::scheduling::session::resolve(
        call.entry,
        &call.headers,
        &call.body,
        call.execution_session.as_deref(),
        &call.caller.principal,
    )
    .id
    .map(|session| (router.name.clone(), session));
    let routed = |pattern: &str, model: &str, need: Option<f64>| Routed {
        model: finish(pattern, suffix),
        scope: router.scope.clone(),
        fallback: Fallback {
            router: router.name.clone(),
            left: router.fallback_models,
            need,
            tried: vec![model.to_owned()],
            key: key.clone().filter(|_| !counting),
            bind: None,
            suffix: suffix.map(str::to_owned),
            tokens,
        },
    };
    // A conversation keeps its model while the router still offers it (a config change can
    // take it away); only a new user message is graded again.
    let bound = key.as_ref().and_then(|k| rt.model_router.bound(k, now)).filter(|b| {
        candidates.iter().any(|(s, entry)| {
            s.id == b.model
                && entry.and_then(|e| split_suffix(&e.model).1).is_none_or(|pin| {
                    split_suffix(&b.pattern)
                        .1
                        .is_some_and(|effort| effort.trim().eq_ignore_ascii_case(pin.trim()))
                })
        })
    });
    if let Some(b) = &bound
        && (counting || !fresh_turn(&call.body))
    {
        tracing::debug!(router = %router.name, model = %b.pattern, "model router: kept (agent loop)");
        return Some(routed(&b.pattern, &b.model, Some(b.index)));
    }
    let pool = pool(&candidates, rt.model_router.catalog());
    if pool.is_empty() {
        if let Some(b) = &bound {
            return Some(routed(&b.pattern, &b.model, None));
        }
        // No benchmark for any candidate: the first listed one, else the newest model.
        let (pattern, model) = match candidates.first() {
            Some((spec, entry @ Some(_))) => (unrated(spec, *entry), spec.id.clone()),
            _ => {
                let (spec, entry) = candidates.iter().rev().max_by_key(|(s, _)| s.created)?;
                (unrated(spec, *entry), spec.id.clone())
            }
        };
        tracing::debug!(router = %router.name, model = %pattern, "model router: no rated candidate");
        return Some(routed(&pattern, &model, None));
    }
    // Token counting never pays for a judgment.
    let judged = if counting {
        None
    } else {
        judge(rt, call, &router.judge, &settings.routers).await
    };
    let signals = judged.unwrap_or_else(|| heuristic(&call.body, tokens));
    let effort = effort(suffix, &call.body);
    let difficulty = (signals + effort_shift(&effort)).clamp(0.0, TOP_DIFFICULTY);
    let need = requirement(&pool, difficulty, router.tier)?;
    let pick = cheapest(&pool, need)?;
    // A conversation only moves up: a weaker pick would throw its prompt cache away.
    if let Some(b) = &bound
        && pick.index <= b.index
    {
        tracing::debug!(
            router = %router.name,
            model = %b.pattern,
            score = ?judged,
            effort = %effort,
            difficulty,
            candidate = %pick.pattern,
            "model router: kept (new message needs no stronger model)"
        );
        return Some(routed(&b.pattern, &b.model, Some(need)));
    }
    tracing::debug!(
        router = %router.name,
        model = %pick.pattern,
        score = ?judged,
        effort = %effort,
        difficulty,
        index = pick.index,
        cost = pick.cost,
        pool = pool.len(),
        from = bound.as_ref().map_or("", |b| b.pattern.as_str()),
        "model router: routed"
    );
    let mut routed = routed(&pick.pattern, &pick.model, Some(need));
    routed.fallback.bind = Some(pick.clone());
    Some(routed)
}

impl Routed {
    /// Whether `fallback-models` lets the request move to another model.
    pub(crate) fn falls_back(&self) -> bool {
        self.fallback.left > 0
    }

    /// Binds the conversation to a newly picked model once that model served: a model
    /// that failed never becomes the conversation's.
    pub(crate) fn commit(&self, rt: &Runtime) {
        if let (Some(key), Some(pick)) = (&self.fallback.key, &self.fallback.bind) {
            rt.model_router.bind(key.clone(), pick, Instant::now());
        }
    }
}

/// Moves a request whose routed model failed to another model: the cheapest untried
/// candidate that meets the same requirement, else the strongest one left; without
/// benchmarks, the next listed one. The conversation moves with it. `None` once
/// `fallback-models` moves are spent or no candidate is left.
pub(crate) fn fall_back(rt: &Runtime, routed: &mut Routed) -> Option<String> {
    let f = &mut routed.fallback;
    f.left = f.left.checked_sub(1)?;
    let settings = settings(&rt.config());
    let router = settings.routers.iter().find(|r| r.name == f.router)?;
    let registry = rt.registry();
    let candidates: Vec<_> = candidates(rt, router, &registry, f.tokens)
        .into_iter()
        .filter(|(s, _)| !f.tried.contains(&s.id))
        .collect();
    let pool = pool(&candidates, rt.model_router.catalog());
    let strongest = || pool.iter().max_by(|a, b| a.index.total_cmp(&b.index));
    let (pattern, model) = match f.need.filter(|_| !pool.is_empty()) {
        Some(need) => {
            let pick = cheapest(&pool, need).or_else(strongest)?;
            f.bind = Some(pick.clone());
            (pick.pattern.clone(), pick.model.clone())
        }
        None => {
            let (spec, entry) = candidates.first()?;
            f.bind = None;
            (unrated(spec, *entry), spec.id.clone())
        }
    };
    tracing::info!(router = %router.name, from = %routed.model, to = %pattern, "model router: fell back");
    f.tried.push(model);
    routed.model = finish(&pattern, f.suffix.as_deref());
    Some(routed.model.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rank(pool: &[Variant], difficulty: f64, tier: Tier) -> Option<&Variant> {
        cheapest(pool, requirement(pool, difficulty, tier)?)
    }

    fn v(model: &str, index: f64, cost: f64) -> Variant {
        Variant {
            pattern: model.into(),
            model: model.into(),
            index,
            cost,
        }
    }

    #[test]
    fn rank_maps_difficulty_onto_the_front() {
        // `dominated` costs more than `mid` and scores lower: it never stretches the scale.
        let pool = [
            v("cheap", 30.0, 0.1),
            v("mid", 50.0, 0.5),
            v("dominated", 20.0, 2.0),
            v("top", 70.0, 3.0),
        ];
        let pick = |d: f64, t| rank(&pool, d, t).unwrap().model.clone();
        assert_eq!(pick(0.0, Tier::Balanced), "cheap");
        assert_eq!(pick(2.0, Tier::Balanced), "mid");
        assert_eq!(pick(3.0, Tier::Balanced), "top");
        assert_eq!(pick(4.0, Tier::Balanced), "top");
        // Jev's fractional score: 0.37 of 4 needs 33.7 on the 30..70 range.
        assert_eq!(pick(0.37, Tier::Balanced), "mid");
        // The requirement is a share of the front's 30..70 range: 0.75 - 0.15 for `cost`
        // at difficulty 3 still needs 54, 0 + 0.2 for `quality` lifts a trivial request to 38.
        assert_eq!(pick(3.0, Tier::Cost), "top");
        assert_eq!(pick(0.0, Tier::Cost), "cheap");
        assert_eq!(pick(0.0, Tier::Quality), "mid");
        assert!(rank(&[], 2.0, Tier::Balanced).is_none());
        // A variant 0.06 short of 57.46 is within the tolerance: it wins on cost over one
        // that scores 1.7 higher for two thirds more.
        let close = [v("low", 34.8, 0.2), v("xhigh", 57.4, 6.0), v("max", 59.1, 10.0)];
        assert_eq!(rank(&close, 3.73, Tier::Balanced).unwrap().model, "xhigh");
        assert_eq!(rank(&close, 4.0, Tier::Balanced).unwrap().model, "max");
    }

    #[test]
    fn tiers_order_capability_for_every_difficulty() {
        let pool: Vec<Variant> = (0..10)
            .map(|i| v(&i.to_string(), 20.0 + 6.0 * f64::from(i), 0.1 * f64::from(i + 1)))
            .collect();
        for d in [0.0, 0.5, 1.0, 2.0, 2.7, 3.0, 4.0] {
            let index = |t| rank(&pool, d, t).unwrap().index;
            assert!(index(Tier::Cost) <= index(Tier::Balanced), "{d}");
            assert!(index(Tier::Balanced) <= index(Tier::Quality), "{d}");
        }
    }

    fn spec(id: &str, thinking: Option<ck_core::registry::ThinkingSupport>) -> Spec {
        Spec {
            id: id.into(),
            display_name: id.into(),
            thinking,
            ..Default::default()
        }
    }

    fn catalog(names: &[(&str, f64, f64)]) -> Catalog {
        let models: Vec<crate::inference::aa::Model> = names
            .iter()
            .map(|(name, index, cost)| {
                serde_json::from_value(serde_json::json!({
                    "name": name,
                    "evaluations": {"artificial_analysis_intelligence_index": index},
                    "artificial_analysis_intelligence_index_cost": {"cost_per_task": {"total_cost": cost}},
                    "pricing": {"price_1m_input_tokens": 2.0, "price_1m_output_tokens": 6.0},
                }))
                .unwrap()
            })
            .collect();
        crate::inference::aa::Catalog::new(0, &models)
    }

    #[test]
    fn variants_follow_the_route_efforts_and_user_prices() {
        let c = catalog(&[
            ("Sol (High)", 60.0, 1.0),
            ("Sol (Max)", 65.0, 2.0),
            ("Sol (Non-reasoning)", 40.0, 0.2),
            ("Sol (Reasoning)", 50.0, 0.5),
        ]);
        let levels = |levels: &[&str], zero| {
            Some(ck_core::registry::ThinkingSupport {
                levels: levels.iter().map(|l| (*l).to_owned()).collect(),
                zero_allowed: zero,
                ..Default::default()
            })
        };
        let patterns = |s: &Spec, e: Option<&Entry>| -> Vec<String> {
            variants(s, e, &c).into_iter().map(|v| v.pattern).collect()
        };
        assert_eq!(
            patterns(&spec("sol", levels(&["low", "high"], true)), None),
            ["sol(high)", "sol(none)"]
        );
        assert_eq!(
            patterns(&spec("sol", levels(&["high", "max"], false)), None),
            ["sol(high)", "sol(max)"]
        );
        assert_eq!(
            patterns(&spec("sol", None), None),
            ["sol"],
            "a route without thinking: only non-reasoning"
        );
        let pinned = Entry {
            model: "SOL(max)".into(),
            aa: String::new(),
            price: Some(1.0),
        };
        let pinned_variants = variants(&spec("sol", levels(&["high", "max"], false)), Some(&pinned), &c);
        assert_eq!(pinned_variants.len(), 1);
        assert_eq!(pinned_variants[0].pattern, "sol(max)");
        // The user's blended price is a quarter of AA's (3:1 blend of 2 and 6 is 3).
        assert!((pinned_variants[0].cost - 2.0 / 3.0).abs() < 1e-9);
        let renamed = Entry {
            model: "my-model".into(),
            aa: "Sol".into(),
            price: None,
        };
        assert_eq!(patterns(&spec("my-model", None), Some(&renamed)), ["my-model"]);
    }
    #[test]
    fn last_user_text_reads_each_format() {
        let chat = br#"{"messages":[{"role":"user","content":"first"},{"role":"assistant","content":"x"},{"role":"user","content":[{"type":"text","text":"a"},{"type":"image"},{"type":"text","text":"b"}]}]}"#;
        assert_eq!(last_user_text(chat), "a\nb");
        assert_eq!(last_user_text(br#"{"input":"do it"}"#), "do it");
        let responses = br#"{"input":[{"role":"user","content":[{"type":"input_text","text":"r"}]},{"type":"function_call_output"}]}"#;
        assert_eq!(last_user_text(responses), "r");
        let gemini = br#"{"contents":[{"role":"user","parts":[{"text":"g"}]}]}"#;
        assert_eq!(last_user_text(gemini), "g");
        // A client's reminder blocks are not the user's words; a mention of the tag is.
        let reminder = "<system-reminder>\nthe date\n</system-reminder>\n";
        let part = |t: &str| serde_json::json!({"type": "text", "text": t});
        let turns = |t: serde_json::Value| serde_json::to_vec(&serde_json::json!({"messages": t})).unwrap();
        let asked = serde_json::json!([{"role": "user", "content": [part(reminder), part("why does it hang?"), part(reminder)]}]);
        assert_eq!(last_user_text(&turns(asked)), "why does it hang?");
        let mixed = format!("{reminder}Explain how <system-reminder> is parsed");
        let mixed = serde_json::json!([{"role": "user", "content": mixed}]);
        assert_eq!(last_user_text(&turns(mixed)), "Explain how <system-reminder> is parsed");
        // Text beside a tool result is kept; a turn left with none yields to the one before.
        let tool = serde_json::json!({"type": "tool_result", "tool_use_id": "t", "content": "found"});
        let beside =
            serde_json::json!([{"role": "user", "content": [tool, part("Thanks, now deploy"), part(reminder)]}]);
        assert_eq!(last_user_text(&turns(beside)), "Thanks, now deploy");
        let looped = serde_json::json!([
            {"role": "user", "content": "fix the race"},
            {"role": "assistant", "content": "ok"},
            {"role": "user", "content": [tool, part(reminder)]}
        ]);
        assert_eq!(last_user_text(&turns(looped)), "fix the race");
        // Responses message may omit role; developer is not user
        let roleless = br#"{"input":[{"type":"message","content":[{"type":"input_text","text":"u"}]},{"role":"developer","content":"d"}]}"#;
        assert_eq!(last_user_text(roleless), "u");
        let long = format!(
            r#"{{"messages":[{{"role":"user","content":"{}"}}]}}"#,
            "é".repeat(JUDGE_TEXT)
        );
        assert!(last_user_text(long.as_bytes()).len() <= JUDGE_TEXT);
    }

    #[test]
    fn bindings_expire_and_refresh() {
        let state = State::default();
        let key = ("smart".to_owned(), "s1".to_owned());
        let t0 = Instant::now();
        let pick = Variant {
            pattern: "m(high)".into(),
            model: "m".into(),
            index: 50.0,
            cost: 1.0,
        };
        state.bind(key.clone(), &pick, t0);
        let bound = state.bound(&key, t0 + STICKY_TTL / 2).unwrap();
        assert_eq!(
            (bound.pattern.as_str(), bound.model.as_str(), bound.index),
            ("m(high)", "m", 50.0)
        );
        // The lookup above refreshed the binding.
        assert!(state.bound(&key, t0 + STICKY_TTL + STICKY_TTL / 4).is_some());
        assert!(state.bound(&key, t0 + STICKY_TTL * 3).is_none());
    }

    #[test]
    fn request_signals() {
        // Only a new user message is a fresh turn; tool results, even with Claude Code's
        // text reminders appended, continue the agent loop.
        assert!(fresh_turn(br#"{"messages":[{"role":"user","content":"hi"}]}"#));
        assert!(fresh_turn(br#"{"input":"hi"}"#));
        assert!(fresh_turn(
            br#"{"input":[{"role":"user","content":[{"type":"input_text","text":"x"}]}]}"#
        ));
        assert!(fresh_turn(br#"{"contents":[{"role":"user","parts":[{"text":"g"}]}]}"#));
        assert!(!fresh_turn(
            br#"{"messages":[{"role":"user","content":"a"},{"role":"tool","content":"r"}]}"#
        ));
        assert!(!fresh_turn(br#"{"messages":[{"role":"user","content":[{"type":"tool_result","content":"r"},{"type":"text","text":"<system-reminder>"}]}]}"#));
        assert!(!fresh_turn(
            br#"{"input":[{"role":"user","content":"a"},{"type":"function_call_output","output":"r"}]}"#
        ));
        assert!(!fresh_turn(
            br#"{"contents":[{"role":"user","parts":[{"functionResponse":{"name":"f"}}]}]}"#
        ));
        assert!(!fresh_turn(br#"{"messages":[]}"#));
        // A 1 MB base64 image counts as one attachment, not 250,000 tokens.
        let image = format!(
            r#"{{"messages":[{{"role":"user","content":[{{"type":"text","text":"look"}},{{"type":"image","source":{{"data":"{}"}}}}]}}]}}"#,
            "A".repeat(1 << 20)
        );
        let tokens = estimate_tokens(image.as_bytes());
        assert!(
            (ATTACHMENT_TOKENS..ATTACHMENT_TOKENS + 100).contains(&tokens),
            "{tokens}"
        );
        assert_eq!(estimate_tokens(&[b'x'; 400]), 100);
        assert_eq!(grade("**3**"), Some(3));
        assert_eq!(grade("7"), None);
        assert_eq!(grade("hard"), None);
        // Without a grade: tools mean agentic work, a long context adds one.
        assert_eq!(heuristic(br#"{"messages":[]}"#, 10), 0.0);
        assert_eq!(heuristic(br#"{"tools":[{}]}"#, 10), 2.0);
        assert_eq!(heuristic(br#"{"tools":[{}]}"#, 60_000), 3.0);
        // The router's suffix wins over the body's effort.
        assert_eq!(effort(Some("High"), br#"{"reasoning_effort":"low"}"#), "high");
        assert_eq!(effort(None, br#"{"output_config":{"effort":"max"}}"#), "max");
    }

    #[test]
    fn settings_parse_routers() {
        let cfg = Config::parse(
            "artificial-analysis:\n  api-key: k\nmodel-routers:\n  - name: smart\n    tier: Quality\n    judge: zen/jev-1.13\n    models:\n      - a\n      - {model: b(high), aa: B, price: {input: 1, output: 5}}\n      - {aa: nameless}\n  - name: auto\n    judge: judge\n  - name: smart\n    judge: judge\n  - name: unjudged\n  - name: mapped\n    judge: {model: jev-1.13}\n  - name: chat\n    judge: flash\n",
        )
        .unwrap();
        let s = settings(&cfg);
        assert_eq!(s.aa.key, "k");
        assert_eq!(s.aa.url, crate::inference::aa::DEFAULT_URL);
        let names: Vec<&str> = s.routers.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, ["smart", "chat"], "a router needs a judge model");
        let smart = &s.routers[0];
        assert_eq!(
            (smart.tier, smart.judge.as_str(), smart.models.len()),
            (Tier::Quality, "zen/jev-1.13", 2)
        );
        assert_eq!(smart.models[1].price, Some(2.0));
        assert!(s.routers[1].models.is_empty());
    }

    /// What a mock System One judge (Jev) received: its `Authorization` and body.
    type Judged = Arc<Mutex<Vec<(Option<String>, String)>>>;

    /// A mock Jev at `<url>/v1/systemone`: a task that says "prove" scores 3.8, "fail"
    /// answers 500, as does any request with the key `bad-key`; the key `slow-key` never
    /// gets an answer; anything else scores 0.
    async fn jev() -> (String, Judged) {
        async fn answer(
            axum::extract::State(seen): axum::extract::State<Judged>,
            headers: axum::http::HeaderMap,
            body: String,
        ) -> axum::response::Response {
            use axum::response::IntoResponse;
            assert_eq!(headers.get("user-agent").unwrap(), "cooker");
            let auth = headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            let rejected = auth.as_deref() == Some("Bearer bad-key");
            let hangs = auth.as_deref() == Some("Bearer slow-key");
            seen.lock().unwrap().push((auth, body.clone()));
            if hangs {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
            let task = json::get(body.as_bytes(), "state.task").str().into_owned();
            if task.contains("fail") || rejected {
                return axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
            let score = if task.contains("prove") { 3.8 } else { 0.0 };
            let reply = serde_json::json!({"model": "jev-1.13", "answers": {"difficulty": {"type": "score", "score": score, "confidence": 0.7}}});
            ([("content-type", "application/json")], reply.to_string()).into_response()
        }
        let seen = Judged::default();
        let app = axum::Router::new()
            .route("/v1/systemone", axum::routing::post(answer))
            .with_state(seen.clone());
        (serve(app).await, seen)
    }

    /// The mock Jev at `url` as an OpenAI-compatible provider `zen` (prefix `zen`), and a
    /// router named `smart` judged by `zen/jev-1.13`; `models` follows the router.
    fn jev_router(url: &str, models: &str) -> String {
        format!(
            "    - name: zen\n      base-url: {url}/v1\n      prefix: zen\n      models:\n        - {{name: jev-1.13}}\n      keys:\n        - api-key: jev-key\nmodel-routers:\n  - name: smart\n    judge: zen/jev-1.13\n{models}"
        )
    }

    /// Bodies the mock OpenAI-compatible upstream received. The judge's upstream model
    /// answers `4`, every other model `ok`.
    async fn upstream(
        axum::extract::State(seen): axum::extract::State<Arc<Mutex<Vec<String>>>>,
        body: String,
    ) -> axum::response::Response {
        use axum::response::IntoResponse;
        let content = if body.contains(r#""model":"up-judge""#) {
            "4"
        } else {
            "ok"
        };
        seen.lock().unwrap().push(body);
        let reply = serde_json::json!({
            "id": "c1", "object": "chat.completion", "created": 1, "model": "up",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": content}, "finish_reason": "stop"}],
        });
        ([("content-type", "application/json")], reply.to_string()).into_response()
    }

    async fn serve(app: axum::Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        url
    }

    /// The config of a proxy whose OpenAI-compatible provider at `upstream_url` serves
    /// `cheap`, `top` and `judge`, with `extra` appended.
    fn config(upstream_url: &str, extra: &str) -> Config {
        Config::parse(&format!(
            "access:\n  api-keys: [client-key]\napi-keys:\n  openai-compatibility:\n    - name: Acme\n      base-url: {upstream_url}/v1\n      models:\n        - {{name: up-cheap, alias: cheap, display-name: Cheap One}}\n        - {{name: up-top, alias: top, display-name: Top One}}\n        - {{name: up-judge, alias: judge}}\n      keys:\n        - api-key: sk-fake-upstream\n{extra}"))
        .unwrap()
    }

    /// A proxy on [`config`]. Returns its URL, the upstream log and URL, and the runtime.
    async fn proxy(extra: &str) -> (String, Arc<Mutex<Vec<String>>>, String, Arc<Runtime>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let upstream_url = serve(axum::Router::new().fallback(upstream).with_state(seen.clone())).await;
        let config = config(&upstream_url, extra);
        let credentials = ck_core::config::credentials::load(&config);
        let executors = ck_exec::Executors {
            claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
            codex: Default::default(),
            devices: Default::default(),
            openai: Default::default(),
            google: Default::default(),
        };
        let rt = Arc::new(crate::testing::runtime(config, credentials, executors));
        (serve(crate::router(rt.clone())).await, seen, upstream_url, rt)
    }

    fn rated() -> Catalog {
        let models: Vec<crate::inference::aa::Model> = [("Cheap One", 30.0, 0.1), ("Top One", 70.0, 3.0)]
            .iter()
            .map(|(name, index, cost)| {
                serde_json::from_value(serde_json::json!({
                    "name": name,
                    "evaluations": {"artificial_analysis_intelligence_index": index},
                    "artificial_analysis_intelligence_index_cost": {"cost_per_task": {"total_cost": cost}},
                }))
                .unwrap()
            })
            .collect();
        Catalog::new(0, &models)
    }

    /// Status, `X-Ck-Routed-Model`, body and `X-Ck-Routed-Provider` of one Chat
    /// Completions request.
    async fn chat(url: &str, body: serde_json::Value) -> (u16, String, String, String) {
        let res = wreq::Client::new()
            .post(format!("{url}/v1/chat/completions"))
            .header("authorization", "Bearer client-key")
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .unwrap();
        let header = |name: &str| {
            res.headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_owned()
        };
        let (routed, provider) = (header("x-ck-routed-model"), header("x-ck-routed-provider"));
        (res.status().as_u16(), routed, res.text().await.unwrap(), provider)
    }

    fn upstream_model(body: &str) -> String {
        json::get(body.as_bytes(), "model").str().into_owned()
    }

    #[tokio::test]
    async fn routes_by_difficulty_keeps_the_conversation_and_asks_the_judge() {
        let (jev_url, judged_by_jev) = jev().await;
        let routers = jev_router(
            &jev_url,
            "  - name: judged\n    judge: judge\n    models: [cheap, top]\n",
        );
        let (url, seen, _, rt) = proxy(&routers).await;
        rt.model_router.publish(rated());
        // Jev grades "hi" trivial: the cheapest rated model serves it.
        let first = serde_json::json!({"model": "smart", "messages": [{"role": "user", "content": "hi"}]});
        let (status, routed, text, _) = chat(&url, first).await;
        assert_eq!((status, routed.as_str()), (200, "cheap"), "{text}");
        assert_eq!(upstream_model(&seen.lock().unwrap()[0]), "up-cheap");
        {
            let asked = judged_by_jev.lock().unwrap();
            let (auth, body) = &asked[0];
            assert_eq!(auth.as_deref(), Some("Bearer jev-key"));
            let body = body.as_bytes();
            assert_eq!(json::get(body, "model").str(), "jev-1.13");
            assert_eq!(json::get(body, "state.task").str(), "hi");
            assert_eq!(json::get(body, "questions.difficulty.type").str(), "score");
            assert_eq!(json::get(body, "questions.difficulty.criteria.#").int(), 5);
        }
        let tools = serde_json::json!([{"type": "function", "function": {"name": "f"}}]);
        let turn = |messages: serde_json::Value| serde_json::json!({"model": "smart", "reasoning_effort": "xhigh", "tools": tools, "messages": messages});
        let hi = serde_json::json!({"role": "user", "content": "hi"});
        let ok = serde_json::json!({"role": "assistant", "content": "ok"});
        // Tool results inside the agent loop keep the model (and its cache) without a judgment.
        let call = serde_json::json!({"role": "assistant", "tool_calls": [{"id": "c", "type": "function", "function": {"name": "f", "arguments": "{}"}}]});
        let looped = turn(serde_json::json!([hi, call, {"role": "tool", "tool_call_id": "c", "content": "r"}]));
        assert_eq!(chat(&url, looped).await.1, "cheap");
        assert_eq!(judged_by_jev.lock().unwrap().len(), 1);
        // A new user message Jev grades hard moves the conversation up ...
        let harder = turn(serde_json::json!([hi, ok, {"role": "user", "content": "now prove it"}]));
        assert_eq!(chat(&url, harder).await.1, "top");
        // ... and a later easy one does not move it back down.
        let easy = serde_json::json!({"model": "smart", "messages": [hi, ok, {"role": "user", "content": "now prove it"}, ok, {"role": "user", "content": "thanks"}]});
        assert_eq!(chat(&url, easy).await.1, "top");
        // A new conversation: Jev's 0 plus the client's maximum effort needs the top model.
        let hard = serde_json::json!({"model": "smart(xhigh)", "messages": [{"role": "user", "content": "other"}], "tools": [{"type": "function", "function": {"name": "f"}}]});
        assert_eq!(
            chat(&url, hard).await.1,
            "top(xhigh)",
            "the client's suffix rides on a route without one"
        );
        // A chat judge grades the turn 4 (frontier): its call goes first, then the top model.
        seen.lock().unwrap().clear();
        let judged = serde_json::json!({"model": "judged", "messages": [{"role": "user", "content": "hello there"}]});
        assert_eq!(chat(&url, judged).await.1, "top");
        let models: Vec<String> = seen.lock().unwrap().iter().map(|b| upstream_model(b)).collect();
        assert_eq!(models, ["up-judge", "up-top"]);
        // Both routers list as models.
        let list = wreq::Client::new()
            .get(format!("{url}/v1/models"))
            .header("authorization", "Bearer client-key")
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            list.contains(r#""id":"smart""#) && list.contains(r#""id":"judged""#),
            "{list}"
        );
    }

    fn routing_call(body: serde_json::Value) -> Call {
        Call {
            entry: Format::OpenAI,
            response: Format::OpenAI,
            operation: Operation::Generate,
            model: "smart".into(),
            body: serde_json::to_vec(&body).unwrap().into(),
            stream: false,
            alt: None,
            headers: Default::default(),
            caller: ck_core::exec::Caller {
                principal: String::new(),
                source: "",
                provider: String::new(),
            },
            forced_provider: None,
            selection_model: None,
            execution_session: Some("router-regression".into()),
            request_path: "/v1/chat/completions".into(),
            peer: None,
            socket: None,
            media: None,
        }
    }

    #[tokio::test]
    async fn unrated_fallback_stays_within_filtered_candidates() {
        let cfg = Config::parse("api-keys:\n  openai-compatibility:\n    - name: local\n      base-url: http://127.0.0.1:1/v1\n      models:\n        - {name: small, max-context-length: 8000}\n        - {name: large, max-context-length: 128000}\n      keys:\n        - api-key: test\nmodel-routers:\n  - name: smart\n    judge: smart\n").unwrap();
        let credentials = ck_core::config::credentials::load(&cfg);
        let rt = Arc::new(crate::testing::runtime(
            cfg.clone(),
            credentials,
            ck_exec::Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
                codex: Default::default(),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
        ));
        let call = routing_call(serde_json::json!({"messages": [{"role": "user", "content": "word ".repeat(8000)}]}));
        assert!(estimate_tokens(&call.body) > 8000);
        assert_eq!(route(&rt, &call).await.unwrap().model, "large");
        let oversized =
            routing_call(serde_json::json!({"messages": [{"role": "user", "content": "word ".repeat(104000)}]}));
        assert!(route(&rt, &oversized).await.is_none());
        let mut cfg = cfg;
        cfg.document["model-routers"][0]["models"] = serde_yaml_ng::from_str("[missing]").unwrap();
        rt.publish_config(cfg);
        assert!(route(&rt, &call).await.is_none());
    }

    #[tokio::test]
    async fn bound_effort_follows_reloaded_candidates() {
        let mut cfg = config(
            "http://127.0.0.1:1",
            "model-routers:\n  - name: smart\n    judge: smart\n    models: [top(high)]\n",
        );
        let credentials = ck_core::config::credentials::load(&cfg);
        let rt = Arc::new(crate::testing::runtime(
            cfg.clone(),
            credentials,
            ck_exec::Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
                codex: Default::default(),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
        ));
        rt.model_router.publish(rated());
        let user = routing_call(serde_json::json!({"messages": [{"role": "user", "content": "hi"}]}));
        let tool =
            routing_call(serde_json::json!({"messages": [{"role": "tool", "tool_call_id": "c", "content": "done"}]}));
        let mut routed = Vec::new();
        for call in [&tool, &user] {
            cfg.document["model-routers"][0]["models"] = serde_yaml_ng::from_str("[top(high)]").unwrap();
            rt.publish_config(cfg.clone());
            let first = route(&rt, &user).await.unwrap();
            assert_eq!(first.model, "top(high)");
            first.commit(&rt);
            cfg.document["model-routers"][0]["models"] = serde_yaml_ng::from_str("[top(low)]").unwrap();
            rt.publish_config(cfg.clone());
            routed.push(route(&rt, call).await.unwrap().model);
        }
        assert_eq!(routed, ["top(low)", "top(low)"]);
    }

    #[tokio::test]
    async fn without_benchmarks_the_first_listed_model_serves() {
        let (url, seen, _, _rt) =
            proxy("model-routers:\n  - name: smart\n    judge: judge\n    models: [missing, top(high), cheap]\n").await;
        let body = serde_json::json!({"model": "smart", "messages": [{"role": "user", "content": "hi"}]});
        let (status, routed, text, _) = chat(&url, body).await;
        assert_eq!((status, routed.as_str()), (200, "top(high)"), "{text}");
        assert_eq!(upstream_model(&seen.lock().unwrap()[0]), "up-top");
    }

    #[tokio::test]
    async fn a_judge_naming_its_router_with_a_suffix_is_not_asked() {
        let (url, seen, _, rt) = proxy("model-routers:\n  - name: smart\n    judge: smart(low)\n").await;
        rt.model_router.publish(rated());
        let body = serde_json::json!({"model": "smart", "messages": [{"role": "user", "content": "hi"}]});
        let answered = tokio::time::timeout(Duration::from_secs(5), chat(&url, body)).await;
        assert_eq!(answered.expect("the router recursed").1, "cheap");
        assert_eq!(seen.lock().unwrap().len(), 1, "only the routed request went upstream");
    }

    #[tokio::test]
    async fn a_failed_judgment_falls_back_to_request_signals() {
        let (jev_url, judged_by_jev) = jev().await;
        let (url, _, _, rt) = proxy(&jev_router(&jev_url, "")).await;
        rt.model_router.publish(rated());
        // Jev answers 500; the short tool-free prompt is then trivial by its signals.
        let body = serde_json::json!({"model": "smart", "messages": [{"role": "user", "content": "please fail"}]});
        let (status, routed, text, _) = chat(&url, body).await;
        assert_eq!((status, routed.as_str()), (200, "cheap"), "{text}");
        assert_eq!(judged_by_jev.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn the_judge_tries_jev_credentials_in_provider_order() {
        let (jev_url, judged_by_jev) = jev().await;
        let zen = |name: &str, key: &str| {
            format!(
                "    - name: {name}\n      base-url: {jev_url}/v1\n      prefix: zen\n      models:\n        - {{name: jev-1.13}}\n      keys:\n        - api-key: {key}\n"
            )
        };
        let (url, _, _, rt) = proxy(&format!(
            "{}{}{}routing:\n  providers:\n    order: [zen-slow, zen-bad]\nmodel-routers:\n  - name: smart\n    judge: zen/jev-1.13\n",
            zen("zen", "jev-key"),
            zen("zen-bad", "bad-key"),
            zen("zen-slow", "slow-key")
        ))
        .await;
        rt.model_router.publish(rated());
        // In order: one credential hangs and gets only its share of the budget, the next
        // answers 500, the last grades the request.
        let started = Instant::now();
        let body = serde_json::json!({"model": "smart", "messages": [{"role": "user", "content": "prove it"}]});
        let (status, routed, text, _) = chat(&url, body).await;
        assert_eq!((status, routed.as_str()), (200, "top"), "{text}");
        assert!(started.elapsed() < JUDGE_TIMEOUT, "{:?}", started.elapsed());
        let keys: Vec<Option<String>> = judged_by_jev.lock().unwrap().iter().map(|(k, _)| k.clone()).collect();
        let keys: Vec<&str> = keys.iter().map(|k| k.as_deref().unwrap_or_default()).collect();
        assert_eq!(keys, ["Bearer slow-key", "Bearer bad-key", "Bearer jev-key"]);
    }

    /// A second OpenAI-compatible provider `Down` at a mock that answers 400 to a request
    /// that says "malformed" and 500 to any other, naming "halt" when the request does;
    /// its rule stops on that 500. It records the upstream models.
    async fn down() -> (String, Arc<Mutex<Vec<String>>>) {
        async fn fail(
            axum::extract::State(seen): axum::extract::State<Arc<Mutex<Vec<String>>>>,
            body: String,
        ) -> axum::response::Response {
            use axum::response::IntoResponse;
            seen.lock().unwrap().push(upstream_model(&body));
            let status = if body.contains("malformed") {
                axum::http::StatusCode::BAD_REQUEST
            } else {
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            };
            let message = if body.contains("halt") { "halt" } else { "down" };
            (status, format!(r#"{{"error":{{"message":"{message}"}}}}"#)).into_response()
        }
        let seen = Arc::new(Mutex::new(Vec::new()));
        let url = serve(axum::Router::new().fallback(fail).with_state(seen.clone())).await;
        let entry = format!(
            "    - name: Down\n      base-url: {url}/v1\n      request-scoped-errors:\n        - {{status: 500, match: [halt], action: stop}}\n      models:\n        - {{name: up-cheap, alias: cheap, display-name: Cheap One}}\n        - {{name: up-top, alias: top, display-name: Top One}}\n      keys:\n        - api-key: sk-fake-down\n"
        );
        (entry, seen)
    }

    #[tokio::test]
    async fn a_router_keeps_to_its_scope_and_falls_back_once() {
        let (entry, down_seen) = down().await;
        let (url, seen, _, rt) = proxy(&format!(
            "{entry}model-routers:\n  - name: picky\n    judge: judge\n    fallback-models: 1\n    models: [cheap, top]\n    providers: {{exclude: [acme]}}\n"
        ))
        .await;
        rt.model_router.publish(rated());
        let ask = |text: &str| serde_json::json!({"model": "picky", "messages": [{"role": "user", "content": text}]});
        // The judge grades 4: `top`, which only `Down` may serve. A request fault stays.
        let (status, routed, text, provider) = chat(&url, ask("malformed")).await;
        assert_eq!(
            (status, routed.as_str(), provider.as_str()),
            (400, "top", "Down"),
            "{text}"
        );
        assert_eq!(*down_seen.lock().unwrap(), ["up-top"]);
        // A `stop` rule ends the request on its model too.
        let (status, routed, text, _) = chat(&url, ask("halt")).await;
        assert_eq!((status, routed.as_str()), (500, "top"), "{text}");
        assert_eq!(*down_seen.lock().unwrap(), ["up-top", "up-top"]);
        // A provider fault moves the request to one other model, never to Acme.
        let (status, routed, text, _) = chat(&url, ask("hello")).await;
        assert_eq!((status, routed.as_str()), (500, "cheap"), "{text}");
        assert_eq!(*down_seen.lock().unwrap(), ["up-top", "up-top", "up-top", "up-cheap"]);
        let acme: Vec<String> = seen.lock().unwrap().iter().map(|b| upstream_model(b)).collect();
        assert_eq!(acme, ["up-judge", "up-judge", "up-judge"], "Acme only judged");
    }

    #[tokio::test]
    async fn a_bound_conversation_follows_a_narrowed_model_list() {
        let (jev_url, _) = jev().await;
        let (url, _, upstream_url, rt) = proxy(&jev_router(&jev_url, "    models: [cheap, top]\n")).await;
        rt.model_router.publish(rated());
        let hi = serde_json::json!({"role": "user", "content": "hi"});
        let first = serde_json::json!({"model": "smart", "messages": [hi]});
        assert_eq!(chat(&url, first).await.1, "cheap");
        // `cheap` leaves the router's list: the next loop turn may no longer use it.
        rt.publish_config(config(&upstream_url, &jev_router(&jev_url, "    models: [top]\n")));
        let call = serde_json::json!({"role": "assistant", "tool_calls": [{"id": "c", "type": "function", "function": {"name": "f", "arguments": "{}"}}]});
        let looped = serde_json::json!({"model": "smart", "messages": [hi, call, {"role": "tool", "tool_call_id": "c", "content": "r"}]});
        assert_eq!(chat(&url, looped).await.1, "top");
    }

    #[tokio::test]
    async fn start_fetches_benchmarks_and_writes_the_cache() {
        let aa = serve(axum::Router::new().fallback(|| async {
            r#"{"data":[{"name":"Top One","evaluations":{"artificial_analysis_intelligence_index":70},"artificial_analysis_intelligence_index_cost":{"cost_per_task":{"total_cost":3}}}]}"#
        }))
        .await;
        let cache = std::env::temp_dir().join(format!("cooker-router-aa-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&cache);
        let (_url, _seen, _, rt) = proxy(&format!(
            "artificial-analysis:\n  api-key: aa-key\n  url: {aa}/models\n  cache-file: {}\n",
            cache.display()
        ))
        .await;
        crate::inference::aa::start(&rt);
        let deadline = Instant::now() + Duration::from_secs(10);
        while rt.model_router.catalog().is_none() {
            assert!(Instant::now() < deadline, "no catalog");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(rt.model_router.catalog().unwrap().variants("Top One")[0].index, 70.0);
        assert!(std::fs::read_to_string(&cache).unwrap().contains("Top One"));
        std::fs::remove_file(cache).unwrap();
    }
}
