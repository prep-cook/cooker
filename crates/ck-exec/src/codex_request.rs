//! Codex request shaping: body rules and upstream headers, applied in Go's order so
//! appended keys land where Go puts them (codex_executor_execute.go, codex_executor_stream.go,
//! codex_executor_request.go, codex_websockets_request.go, helps/codex_input_ids.go,
//! helps/codex_tool_schema.go, openai_responses_signature.go).

use ck_core::gostr::GoStr;
use std::time::Duration;

use ck_core::config::Config;
use ck_core::credential::Credential;
use ck_core::exec::{ExecError, ExecRequest, FailureScope};
use ck_core::format::Format;
use gjson::Kind;
use http::{HeaderMap, HeaderName, HeaderValue};
use sha2::{Digest, Sha256};

use crate::codex_json::{delete, set_bool_if_different, set_raw, set_str};

mod body;
mod headers;
mod input_ids;
mod schema;
#[cfg(test)]
mod tests;

pub(crate) use body::{
    ensure_image_tool, ensure_web_search_for_history, explicit_instructions, normalize_parallel_tool_calls,
    sanitize_reasoning, siwc_body, siwc_request_body,
};
pub(crate) use headers::{custom_headers, http_headers, image_headers, ws_headers};
pub(crate) use input_ids::sanitize_input_ids;
pub(crate) use schema::normalize_tool_schemas;

pub const DEFAULT_BASE_URL: &str = "https://chatgpt.com/backend-api/codex";
pub(crate) const USER_AGENT: &str = "codex-tui/0.154.0 (Mac OS 26.5.2; arm64) iTerm.app/3.6.11 (codex-tui; 0.154.0)";
pub(crate) const ORIGINATOR: &str = "codex-tui";
pub(crate) const WS_BETA: &str = "responses_websockets=2026-02-06";
const ROUTING_HINT: &str = "x-codex-routing-hint";
const LITE_HEADER: &str = "x-openai-internal-codex-responses-lite";

/// Which upstream call a body is shaped for; each has its own Go rule list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Call {
    Stream,
    NonStream,
    Compact,
    Websocket,
}

impl Call {
    /// The upstream protocol: compact goes to the OpenAI Responses endpoint.
    pub fn target(self) -> Format {
        if self == Call::Compact {
            Format::OpenAIResponse
        } else {
            Format::Codex
        }
    }
}

/// Codex settings read from the config snapshot (v8 paths; legacy keys are already moved).
#[derive(Debug, Clone, Default)]
pub(crate) struct Settings {
    pub disable_cloaking: bool,
    pub bootstrap_buffering: bool,
    pub bootstrap_timeout: Option<Duration>,
    pub model_level_cooling: bool,
    pub default_user_agent: String,
    pub default_beta_features: String,
    pub image_generation: bool,
    /// `requests.payload` rules and image-generation mode for this config snapshot.
    pub payload: ck_wire::payload::Rules,
    /// Multi-agent v2 and orphan-delegation settings, both shared by every credential kind.
    pub client: ck_wire::codex_client::Settings,
    /// The `models` of the `codex-api-key` entry the credential resolves to.
    pub key_models: Vec<serde_json::Value>,
    /// `response-steering`: full-duplex WebSocket turns (codex_websockets_duplex.go).
    pub response_steering: bool,
    /// `multimedia.gpt-image-2-base-model`, trimmed: the Images API's tool-call model.
    pub image_base_model: String,
    /// `codexSIWCUserAgent`, read from the whole config (Go uses `e.cfg`, never the API-key view).
    pub siwc_user_agent: String,
}

/// `upstream.codex.response-steering`; the older spellings are already moved there.
pub(crate) fn response_steering(cfg: &Config) -> bool {
    setting(cfg, &["upstream", "codex", "response-steering"])
        .and_then(serde_yaml_ng::Value::as_bool)
        .unwrap_or(false)
}

/// `oauth.providers.codex.chatgpt-keep-alive`, a cooker addition: off unless set to
/// true (YAML 1.1 spellings such as `yes` are normalised by the config schema).
pub(crate) fn chatgpt_keep_alive(cfg: &Config) -> bool {
    setting(cfg, &["oauth", "providers", "codex", "chatgpt-keep-alive"])
        .and_then(serde_yaml_ng::Value::as_bool)
        .unwrap_or(false)
}

fn setting<'a>(cfg: &'a Config, path: &[&str]) -> Option<&'a serde_yaml_ng::Value> {
    path.iter().try_fold(&cfg.document, |v, k| v.get(*k))
}

impl Settings {
    /// The settings `view`'s credential sees: API keys get Go's `ForAPIKey` view, where
    /// the OAuth-only `oauth.providers.codex.*` settings (the header defaults) do not
    /// apply.
    pub fn scoped(cfg: &Config, view: &View<'_>) -> Self {
        let mut settings = if view.api_key {
            Self::from(&cfg.for_api_key())
        } else {
            Self::from(cfg)
        };
        settings.siwc_user_agent = crate::codex_siwc::user_agent(cfg);
        // ponytail: Go's executor-side resolveCodexKeyConfig skips the prefix/proxy
        // tie-break of the shared resolver; they differ only for duplicate key+URL entries.
        settings.key_models = ck_core::config::credentials::resolve_api_key_entry(cfg, "codex", view.credential)
            .map(|entry| entry.models)
            .unwrap_or_default();
        settings
    }

    /// `resolveCodexModelIsCompat`: the model dispatch bound to the attempt, else the
    /// credential's config entry, where the first model whose name or alias equals the
    /// base or requested model decides; none means not compat.
    pub fn is_compat(&self, inbound: &Inbound<'_>) -> bool {
        if let Some(resolved) = inbound.resolved {
            return resolved.is_compat();
        }
        let fold = |a: &str, b: &str| !b.is_empty() && a.go_eq_fold(b);
        let requested = inbound.model.trim();
        let base = base_model(inbound.model);
        let target = base.trim();
        self.key_models
            .iter()
            .find(|m| {
                let field = |k: &str| m.get(k).and_then(serde_json::Value::as_str).unwrap_or_default().trim();
                let (name, alias) = (field("name"), field("alias"));
                fold(name, target) || fold(alias, target) || fold(name, requested) || fold(alias, requested)
            })
            .and_then(|m| m.get("is-compat"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    }

    pub fn from(cfg: &Config) -> Self {
        // Shared settings (`upstream.codex`); the header defaults stay OAuth-only.
        let codex = |k: &str| setting(cfg, &["upstream", "codex", k]);
        let flag = |k: &str| codex(k).and_then(|v| v.as_bool()).unwrap_or(false);
        let text = |path: &[&str]| {
            setting(cfg, path)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .trim()
                .to_owned()
        };
        let image_off = match setting(cfg, &["multimedia", "disable-image-generation"]) {
            None => true,
            Some(v) => v.as_bool() == Some(false) || v.as_str().is_some_and(|s| matches!(s.trim(), "" | "false")),
        };
        Self {
            disable_cloaking: flag("disable-codex-cloaking"),
            bootstrap_buffering: flag("stream-bootstrap-buffering"),
            bootstrap_timeout: codex("stream-bootstrap-timeout")
                .and_then(|v| {
                    v.as_str()
                        .map(str::to_owned)
                        .or_else(|| v.as_i64().map(|n| n.to_string()))
                })
                .and_then(|s| go_duration(&s))
                .filter(|d| !d.is_zero()),
            model_level_cooling: flag("model-level-cooling"),
            default_user_agent: text(&["oauth", "providers", "codex", "header-defaults", "user-agent"]),
            default_beta_features: text(&["oauth", "providers", "codex", "header-defaults", "beta-features"]),
            image_generation: image_off,
            payload: ck_wire::payload::Rules::from_config(cfg),
            client: ck_wire::codex_client::Settings::from_config(cfg),
            key_models: Vec::new(),
            response_steering: response_steering(cfg),
            image_base_model: text(&["multimedia", "gpt-image-2-base-model"]),
            siwc_user_agent: crate::codex_siwc::user_agent(cfg),
        }
    }
}

/// `StreamBootstrapTimeoutDuration`: Go durations, bare seconds, or off words.
/// Anything else is Go's default of 0, meaning no timeout.
fn go_duration(raw: &str) -> Option<Duration> {
    let raw = raw.trim();
    if raw.is_empty()
        || raw == "0"
        || ["none", "unlimited", "disabled", "off", "never"]
            .iter()
            .any(|w| raw.eq_ignore_ascii_case(w))
    {
        return None;
    }
    if let Some(nanos) = ck_core::config::parse_duration(raw).filter(|n| *n >= 0) {
        return Some(Duration::from_nanos(nanos as u64));
    }
    // strconv.Atoi, capped where seconds still fit a time.Duration.
    raw.parse::<i64>()
        .ok()
        .filter(|secs| (0..=i64::MAX / 1_000_000_000).contains(secs))
        .map(|secs| Duration::from_secs(secs as u64))
}

/// The credential fields the Codex executor reads (`codexCreds`, `codexAuthUsesAPIKey`).
pub(crate) struct View<'a> {
    pub credential: &'a Credential,
    pub token: &'a str,
    pub api_key: bool,
    /// `codexUseSIWC`: the credential sends its Sign in with ChatGPT token (7b0319fb).
    pub siwc: bool,
    pub base_url: &'a str,
    /// Effective proxy (`effectiveProxyURL`): the credential's, then `requests.proxy-url`.
    pub proxy: crate::proxy::Proxy,
    /// `$CPA-SESSION-ID`: the request's explicit session, never derived fallbacks.
    pub session: Option<String>,
    /// `oauth.providers.codex.chatgpt-keep-alive` (cooker addition, off by default):
    /// keep idle chatgpt.com connections instead of Go's connection per request. Read
    /// from the config snapshot of each request, so a reload applies to the next one.
    pub chatgpt_keep_alive: bool,
    /// `requests.tls-handshake-timeout-seconds` for chatgpt.com ([`crate::tls::handshake_timeout`]).
    pub handshake: Option<std::time::Duration>,
}

impl<'a> View<'a> {
    /// The view one request uses, with its effective proxy.
    pub fn for_request(credential: &'a Credential, cfg: &Config) -> Self {
        Self {
            proxy: crate::proxy::Proxy::effective(credential, cfg),
            chatgpt_keep_alive: chatgpt_keep_alive(cfg),
            handshake: crate::tls::handshake_timeout(cfg),
            ..Self::new(credential)
        }
    }

    /// Binds the request's explicit session for `$CPA-SESSION-ID` custom headers.
    pub fn with_session(mut self, session: Option<String>) -> Self {
        self.session = session;
        self
    }

    pub fn new(credential: &'a Credential) -> Self {
        let attr = |k: &str| credential.attributes.get(k).map(String::as_str).unwrap_or_default();
        let api_key = attr("api_key");
        let siwc = crate::codex_siwc::enabled(credential);
        // `codexCreds`: a Sign in with ChatGPT credential sends its nested token to the
        // OpenAI API unless `base_url` names another endpoint.
        let (token, base_url) = if siwc {
            let base = attr("base_url");
            (
                crate::codex_siwc::access_token(credential),
                if base.trim().is_empty() {
                    crate::codex_siwc::RESOURCE
                } else {
                    base
                },
            )
        } else {
            let token = if api_key.is_empty() {
                credential.str("access_token").unwrap_or_default()
            } else {
                api_key
            };
            let base = match attr("base_url") {
                "" => DEFAULT_BASE_URL,
                url => url,
            };
            (token, base)
        };
        let kind = attr("auth_kind").trim().to_ascii_lowercase().replace(['_', '-'], "");
        let is_api_key = kind == "apikey" || (kind.is_empty() && !api_key.trim().is_empty());
        Self {
            credential,
            token,
            api_key: is_api_key,
            siwc,
            base_url: base_url.trim_end_matches('/'),
            proxy: crate::proxy::Proxy::Inherit,
            session: None,
            chatgpt_keep_alive: false,
            handshake: None,
        }
    }

    pub fn attr(&self, key: &str) -> &str {
        self.credential
            .attributes
            .get(key)
            .map(String::as_str)
            .unwrap_or_default()
    }

    /// Plan used for the free-plan image tool rule: refreshed metadata wins over the
    /// attribute computed at load time.
    pub(crate) fn plan_type(&self) -> &str {
        self.credential
            .str("plan_type")
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(self.attr("plan_type"))
    }

    /// `isCodexCloakingDisabled`: the attribute (config synthesis copies the matching
    /// `codex-api-key` entry's `disable-codex-cloaking` into it), then the setting.
    pub fn cloaking_disabled(&self, settings: &Settings) -> bool {
        go_bool(self.attr("codex_disable_cloaking")).unwrap_or(settings.disable_cloaking)
    }

    /// `codexWebsocketsEnabled`: attribute first, then metadata.
    pub fn websockets(&self) -> bool {
        match self.attr("websockets").trim() {
            "" => match self.credential.metadata.get("websockets") {
                Some(serde_json::Value::Bool(b)) => *b,
                Some(serde_json::Value::String(s)) => go_bool(s).unwrap_or(false),
                _ => false,
            },
            raw => go_bool(raw).unwrap_or(false),
        }
    }
}

/// Go `strconv.ParseBool`.
fn go_bool(s: &str) -> Option<bool> {
    match s.trim() {
        "1" | "t" | "T" | "true" | "TRUE" | "True" => Some(true),
        "0" | "f" | "F" | "false" | "FALSE" | "False" => Some(false),
        _ => None,
    }
}

/// `thinking.ParseSuffix(model).ModelName`.
pub(crate) fn base_model(model: &str) -> String {
    ck_wire::thinking::parse_suffix(model).model_name
}

pub(crate) fn header<'a>(headers: &'a HeaderMap, name: &str) -> &'a str {
    headers.get(name).and_then(|v| v.to_str().ok()).unwrap_or_default()
}

/// `util.IsCodexResponsesLiteRequest`. The header is decoded as UTF-8 rather than through
/// [`header`], so Go's `TrimSpace` also strips non-ASCII spaces such as U+00A0.
pub(crate) fn is_lite(body: &str, headers: &HeaderMap) -> bool {
    let lite = headers
        .get(LITE_HEADER)
        .and_then(|v| std::str::from_utf8(v.as_bytes()).ok())
        .unwrap_or_default();
    if lite.trim().go_eq_fold("true") {
        return true;
    }
    let v = gjson::get(
        body,
        "client_metadata.ws_request_header_x_openai_internal_codex_responses_lite",
    );
    v.kind() == Kind::True || (v.kind() == Kind::String && v.str().trim().eq_ignore_ascii_case("true"))
}

/// `helps.IsNativeCodexRequest`: Codex/Responses formats on both sides plus the lite marker.
pub(crate) fn is_native(req: &ExecRequest) -> bool {
    Inbound::of(req).native()
}

pub(crate) use crate::codex_shape::Inbound;

/// `translateCodexRequestPairWithUpdateIntent` + `helps.ApplyRequestThinking`: the
/// registered pair (Go's model-rewrite fallback for same-format bodies), then the
/// thinking pipeline for `codex` (`openai-response` for compact). Also returns the
/// translated original request, which payload default rules consult.
fn translate_request(req: &ExecRequest, call: Call, is_compat: bool) -> Result<(String, Vec<u8>), ExecError> {
    let target = call.target();
    let registered = ck_translate::pair(req.source_format, target).is_some();
    if !registered && !matches!(req.source_format, Format::Codex | Format::OpenAIResponse) {
        // ponytail: Go forwards an untranslatable body as-is; refusing locally is clearer.
        return Err(ExecError::local(
            501,
            FailureScope::Request,
            format!(
                "{} -> {} request translation is not registered",
                req.source_format.as_str(),
                target.as_str()
            ),
        ));
    }
    let model = base_model(&req.model);
    let ctx = ck_translate::RequestCtx {
        model: &model,
        stream: matches!(call, Call::Stream | Call::Websocket),
        tool_search: false,
    };
    // `translateCodexRequestPairWithUpdateIntent`: an is-compat model's Claude requests
    // use the compat translator, with no client headers or config (Go passes nil).
    let no_headers = HeaderMap::new();
    let compat = crate::codex_client::Client {
        headers: &no_headers,
        settings: Default::default(),
        target_executor: "",
        is_compat: true,
    };
    let translate = |raw: &[u8]| -> Result<Vec<u8>, ExecError> {
        let mut body = if is_compat && req.source_format == Format::Claude && target == Format::Codex {
            crate::codex_client::translate_request(req.source_format, target, &ctx, raw, &compat)
        } else {
            ck_translate::translate_request(req.source_format, target, &ctx, raw)
        }
        .map_err(|e| ExecError::local(400, FailureScope::Request, e.to_string()))?;
        // The shared legacy Chat translator also serves non-Codex provider policies.
        if req.source_format == Format::OpenAI
            && target == Format::Codex
            && ck_wire::json::get(raw, "parallel_tool_calls").kind == ck_wire::json::Kind::False
        {
            ck_wire::json::set_bool(&mut body, "parallel_tool_calls", false);
        }
        Ok(body)
    };
    // Go translates the original first, and equal payloads once.
    let (original, body) = if req.original_body.is_empty() || req.original_body == req.body {
        let body = translate(&req.body)?;
        (body.clone(), body)
    } else {
        (translate(&req.original_body)?, translate(&req.body)?)
    };
    let caps = req
        .resolved_model
        .as_ref()
        .map(|r| ck_wire::thinking::ModelCaps::from(&r.info));
    let thought = ck_wire::thinking::apply_request_thinking(
        &ck_wire::thinking::RequestThinking {
            payload: Some(&req.body[..]),
            original: Some(&req.original_body[..]),
            model: &req.model,
            from: req.source_format.as_str(),
            to: target.as_str(),
            provider: "codex",
            resolved: caps.as_ref().map(Some),
            has_request_transformer: registered,
            updates_changed: false,
        },
        body.to_vec(),
    )
    .map_err(|e| ExecError::local(e.status(), FailureScope::Request, e.message))?;
    let body = String::from_utf8(thought)
        .map_err(|_| ExecError::local(400, FailureScope::Request, "request body is not valid UTF-8 JSON"))?;
    Ok((body, original))
}

/// Go's Codex payload finalizer: the payload rules on the finished body, after the
/// prompt cache key, the input id sanitizing and the SIWC WebSocket rules (868e137).
pub(crate) struct Finalizer {
    pub(crate) call: Call,
    /// The baseline `default` rules check: the translated original request, or its
    /// `Rules::baseline`.
    pub(crate) original: Vec<u8>,
}

impl Finalizer {
    pub(crate) fn apply(&self, req: &ExecRequest, settings: &Settings, body: String) -> String {
        self.finish(&Inbound::of(req), settings, body)
    }

    pub(crate) fn finish(&self, inbound: &Inbound<'_>, settings: &Settings, body: String) -> String {
        apply_payload(inbound, settings, self.call, body, &self.original)
    }
}

/// The payload rules on a `response.steer` frame: the frame is its own baseline, and
/// its `type` stays `response.steer`.
pub(crate) fn steer_rules(req: &ExecRequest, settings: &Settings, payload: String) -> String {
    let original = settings.payload.baseline("", payload.as_bytes()).unwrap_or_default();
    let out = apply_payload(&Inbound::of(req), settings, Call::Websocket, payload, &original);
    set_str(&out, "type", "response.steer")
}

/// `helps.ApplyPayloadConfigWithRequestForExecutor` for the Codex executors.
fn apply_payload(inbound: &Inbound<'_>, settings: &Settings, call: Call, body: String, original: &[u8]) -> String {
    let requested = if inbound.requested_model.trim().is_empty() {
        inbound.model.trim()
    } else {
        inbound.requested_model.trim()
    };
    let rules = ck_wire::payload::Request {
        target_executor: if call == Call::Websocket {
            "codex-websockets"
        } else {
            "codex"
        },
        model: &base_model(inbound.model),
        requested_model: requested,
        protocol: call.target().as_str(),
        from_protocol: inbound.source.as_str(),
        root: "",
        original,
        request_path: inbound.request_path,
        headers: Some(inbound.headers),
    };
    let out = ck_wire::payload::apply(&settings.payload, &rules, body.into_bytes());
    String::from_utf8(out).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

/// A request body after the Codex rules ([`shape`]).
pub(crate) struct Shaped {
    pub body: String,
    /// The multi-agent v2 optimization renamed the `collaboration` namespace, so upstream
    /// payloads need `ck_wire::codex_client::restore_response`.
    pub optimized: bool,
    /// `HasCodexMultiAgentV2NamespaceConflict` before the optimization: the client already
    /// uses the optimized name, so nothing may be restored.
    pub conflict: bool,
    /// The payload rules, which callers run last.
    pub finalize: Finalizer,
}

/// Applies the Codex body rules for `call` to the old path's translated request.
// ponytail: the Claude-source reasoning replay cache is not applied here yet.
// ponytail: each rule that edits the body returns a new copy while the previous one is
// still alive, so a streamed 1.9 MB /v1/responses request peaks at 16.8 MB of live heap
// (8.8x the body; alloc.codex.* in bench/budgets.txt). At that peak, heaptrack counted
// about four body-sized buffers from `sanitize_reasoning`'s edits, one each from
// `translate_request`, `ensure_image_tool` and the route's
// `rewrite_orphan_delegation_input`, and about three more in string formatting and JSON
// serialization whose callers it did not resolve. Walking the body once and splicing the
// edits into one output buffer (as crates/ck-exec/src/claude/signals.rs does) is the
// way toward the 3x target.
pub(crate) fn shape(req: &ExecRequest, view: &View<'_>, settings: &Settings, call: Call) -> Result<Shaped, ExecError> {
    let inbound = Inbound::of(req);
    let (body, original) = translate_request(req, call, settings.is_compat(&inbound))?;
    Ok(crate::codex_shape::shape_body(
        &inbound, view, settings, call, body, original,
    ))
}

/// `uuid.NewString()`: a random v4 UUID.
pub(crate) fn random_uuid() -> String {
    let mut bytes = [0u8; 16];
    let _ = getrandom::fill(&mut bytes);
    uuid::Builder::from_random_bytes(bytes).into_uuid().to_string()
}

/// UUIDv5(OID, "cli-proxy-api\0codex\0<kind>\0<id>") (`stableProviderSessionUUID`).
pub(crate) fn provider_session_uuid(kind: &str, id: &str) -> Option<String> {
    let id = id.trim();
    (!id.is_empty()).then(|| {
        uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            format!("cli-proxy-api\0codex\0{kind}\0{id}").as_bytes(),
        )
        .to_string()
    })
}

/// `ProviderSessionUUID`: the long-lived execution session (the downstream WebSocket
/// connection) when there is one, else the derived session identity.
pub(crate) fn session_uuid(req: &ExecRequest, ws_session: Option<&str>) -> Option<String> {
    crate::codex_shape::sessions_uuid(
        ws_session.or(req.execution_session.as_deref()),
        req.derived_session.as_deref(),
    )
}

/// `cacheHelper` / `applyCodexPromptCacheHeadersWithContext`: the prompt cache key from
/// the client body (Claude Code agent, Responses or Chat) or the session identity,
/// written into the body.
pub(crate) fn prompt_cache(
    req: &ExecRequest,
    body: String,
    ws_session: Option<&str>,
    websocket: bool,
) -> (String, Option<String>) {
    crate::codex_shape::prompt_cache_for(&Inbound::of(req), body, ws_session, websocket)
}
