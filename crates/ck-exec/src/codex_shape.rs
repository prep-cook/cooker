//! The Codex body rules shared by the old execution path and the executor (`codex_exec`),
//! over an [`Inbound`] view of the request, and the session identities they key on: the
//! prompt cache key and the reasoning replay's session and owner.

use ck_core::exec::ExecRequest;
use ck_core::format::Format;
use ck_wire::json as gj;
use gjson::Kind;
use http::HeaderMap;
use sha2::{Digest, Sha256};

use crate::codex_json::{delete, set_bool_if_different, set_raw, set_str, set_str_if_different};
use crate::codex_replay;
use crate::codex_request::{
    Call, Finalizer, Settings, Shaped, View, base_model, ensure_image_tool, ensure_web_search_for_history,
    explicit_instructions, is_lite, normalize_parallel_tool_calls, normalize_tool_schemas, provider_session_uuid,
    sanitize_reasoning, siwc_body,
};

/// What the Codex body rules read besides the body they shape. The old execution path takes
/// it from an `ExecRequest`; the executor (`codex_exec`) from its attempt and request handle,
/// with its own encoded upload as `payload`, since no executor reads client body bytes.
pub(crate) struct Inbound<'a> {
    /// Upstream model, thinking suffix included.
    pub model: &'a str,
    /// The model the client asked for (payload rules' `requested_model`).
    pub requested_model: &'a str,
    pub source: Format,
    pub response: Format,
    /// The body the client-side checks read (lite marker, explicit instructions, prompt
    /// cache keys): the client body on the old path, the upload on the executor's.
    pub payload: &'a [u8],
    pub headers: &'a HeaderMap,
    pub request_path: &'a str,
    pub resolved: Option<&'a ck_core::exec::ResolvedModel>,
    pub execution_session: Option<&'a str>,
    pub derived_session: Option<&'a str>,
    pub principal: &'a str,
}

impl<'a> Inbound<'a> {
    pub(crate) fn of(req: &'a ExecRequest) -> Self {
        Self {
            model: &req.model,
            requested_model: &req.requested_model,
            source: req.source_format,
            response: req.response_format,
            payload: &req.body,
            headers: &req.headers,
            request_path: &req.request_path,
            resolved: req.resolved_model.as_ref(),
            execution_session: req.execution_session.as_deref(),
            derived_session: req.derived_session.as_deref(),
            principal: &req.caller.principal,
        }
    }

    /// `helps.IsNativeCodexRequest`.
    pub(crate) fn native(&self) -> bool {
        let codexish = |f: Format| matches!(f, Format::Codex | Format::OpenAIResponse);
        codexish(self.source)
            && codexish(self.response)
            && is_lite(&String::from_utf8_lossy(self.payload), self.headers)
    }
}

/// The Codex body rules for `call` on a body already in the call's protocol; `original` is
/// the payload rules' baseline.
pub(crate) fn shape_body(
    inbound: &Inbound<'_>,
    view: &View<'_>,
    settings: &Settings,
    call: Call,
    body: String,
    original: Vec<u8>,
) -> Shaped {
    let model = base_model(inbound.model);
    let model = model.as_str();
    let is_compat = settings.is_compat(inbound);
    let headers = inbound.headers;
    let mut body = call_rules(body, call, model);
    // Go runs this before the instructions default, except on compact: an appended
    // `tools` key lands before or after an appended `instructions` key accordingly.
    if call != Call::Compact {
        body = ensure_web_search_for_history(body, headers);
    }
    if view.siwc {
        // WebSocket turns get theirs after the prompt cache key ([`siwc_request_body`]).
        if call != Call::Websocket {
            let explicit = explicit_instructions(inbound.payload);
            let keep_effort = ck_wire::thinking::has_thinking_suffix(inbound.model);
            body = siwc_body(body, call, explicit, keep_effort);
        }
    } else if !inbound.native() {
        let mut bytes = body.into_bytes();
        ck_wire::codex_client::normalize_codex_instructions(&mut bytes);
        body = String::from_utf8(bytes).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned());
    }
    if call == Call::Compact {
        body = ensure_web_search_for_history(body, headers);
    }
    if call != Call::Compact && settings.image_generation && !view.siwc {
        body = ensure_image_tool(body, model, view, headers);
    }
    body = sanitize_reasoning(body, is_compat);
    body = if call == Call::Websocket {
        if is_lite(&body, headers) {
            set_bool_if_different(body, "parallel_tool_calls", false)
        } else {
            body
        }
    } else {
        normalize_parallel_tool_calls(body, headers)
    };
    let body = normalize_tool_schemas(body);
    let conflict = ck_wire::codex_client::has_namespace_conflict(body.as_bytes());
    let (body, optimized) = crate::codex_client::optimize_for_auth(
        headers,
        body.as_bytes(),
        &settings.client,
        is_compat,
        crate::codex_client::tools_prepared(inbound.request_path),
    );
    let body = String::from_utf8(body).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned());
    Shaped {
        body,
        optimized,
        conflict,
        finalize: Finalizer { call, original },
    }
}

/// The model and the per-call members: what each upstream call accepts.
fn call_rules(mut body: String, call: Call, model: &str) -> String {
    const DROPPED: [&str; 4] = [
        "previous_response_id",
        "generate",
        "prompt_cache_retention",
        "safety_identifier",
    ];
    match call {
        Call::NonStream => {
            body = set_str_if_different(body, "model", model);
            body = set_bool_if_different(body, "stream", true);
            for key in DROPPED.into_iter().chain(["stream_options"]) {
                body = delete(&body, key);
            }
        }
        Call::Stream => {
            for key in DROPPED {
                body = delete(&body, key);
            }
            let delivery = gjson::get(&body, "stream_options.reasoning_summary_delivery");
            let delivery = delivery
                .exists()
                .then(|| (delivery.kind(), delivery.str().to_owned(), delivery.json().to_owned()));
            body = delete(&body, "stream_options");
            if let Some((kind, text, raw)) = delivery {
                // sjson.SetBytes with the decoded value: strings re-quote, scalars stay raw.
                body = if kind == Kind::String {
                    set_str(&body, "stream_options.reasoning_summary_delivery", &text)
                } else {
                    set_raw(&body, "stream_options.reasoning_summary_delivery", &raw)
                };
            }
            body = set_str_if_different(body, "model", model);
        }
        Call::Websocket => {
            body = set_str_if_different(body, "model", model);
            body = set_bool_if_different(body, "stream", true);
            body = delete(&body, "prompt_cache_retention");
            body = delete(&body, "safety_identifier");
        }
        Call::Compact => {
            body = set_str_if_different(body, "model", model);
            body = delete(&body, "stream");
        }
    }
    body
}

/// [`session_uuid`] over the execution and derived sessions.
pub(crate) fn sessions_uuid(execution: Option<&str>, derived: Option<&str>) -> Option<String> {
    match execution.filter(|id| !id.trim().is_empty()) {
        Some(id) => provider_session_uuid("execution-session", id),
        None => derived.and_then(|id| provider_session_uuid("derived-session", id)),
    }
}

/// `helps.ClaudeCodePromptCache`: one deterministic key per Claude Code agent and model.
fn claude_code_prompt_cache(model: &str, payload: &[u8], headers: &HeaderMap) -> Option<String> {
    let model = model.trim();
    let scope = crate::replay::claude_code_session(payload, headers)?;
    (!model.is_empty()).then(|| {
        let identity = format!("cli-proxy-api:codex:claude-code\0{model}\0{scope}");
        uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, identity.as_bytes()).to_string()
    })
}

/// [`prompt_cache`] for `inbound`.
pub(crate) fn prompt_cache_for(
    inbound: &Inbound<'_>,
    body: String,
    ws_session: Option<&str>,
    websocket: bool,
) -> (String, Option<String>) {
    let original = String::from_utf8_lossy(inbound.payload);
    let client_key = gjson::get(&original, "prompt_cache_key");
    let mut id = match inbound.source {
        Format::Claude => {
            let mut model = gjson::get(&body, "model").str().trim().to_owned();
            if model.is_empty() {
                model = base_model(inbound.model);
            }
            claude_code_prompt_cache(&model, inbound.payload, inbound.headers).unwrap_or_default()
        }
        Format::OpenAIResponse if client_key.exists() => client_key.str().to_owned(),
        Format::OpenAI if client_key.exists() && !websocket => client_key.str().trim().to_owned(),
        _ => String::new(),
    };
    if id.is_empty() {
        id = sessions_uuid(ws_session.or(inbound.execution_session), inbound.derived_session).unwrap_or_default();
    }
    let principal = inbound.principal.trim();
    if id.is_empty() && !websocket && inbound.source == Format::OpenAI && !principal.is_empty() {
        id = uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            format!("cli-proxy-api:codex:prompt-cache:{principal}").as_bytes(),
        )
        .to_string();
    }
    if id.is_empty() {
        return (body, None);
    }
    (set_str_if_different(body, "prompt_cache_key", &id), Some(id))
}

/// The reasoning replay session of a request the executor shapes, in Go's order without the
/// client body: the Claude Code session (headers, or the client's `metadata` the upload
/// carries), the execution session, then the upload's and the headers' session ids.
pub(crate) fn replay_session(upload: &[u8], headers: &HeaderMap, execution_session: Option<&str>) -> String {
    crate::replay::claude_code_session(upload, headers)
        .or_else(|| {
            let execution = execution_session.map(str::trim).unwrap_or_default();
            (!execution.is_empty()).then(|| format!("execution:{execution}"))
        })
        .or_else(|| crate::replay::payload_session(upload))
        .or_else(|| crate::replay::header_session(headers))
        .unwrap_or_default()
}

/// Who a replayed turn belongs to: the calling client and the upstream credential, hashed so
/// the cache holds no client key. Turns of one session id never cross either boundary.
pub(crate) fn replay_owner(principal: &str, credential: &str) -> String {
    let digest = Sha256::digest(format!("{}\0{}", principal.trim(), credential.trim()));
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// The replay turn of a completed response's output `items`: its reasoning and tool calls
/// behind a fingerprinted marker; `None` when there is nothing to replay.
pub(crate) fn replay_turn(scope: &codex_replay::Scope, items: impl Iterator<Item = Vec<u8>>) -> Option<Vec<Vec<u8>>> {
    if !scope.valid() {
        return None;
    }
    let (mut replay, mut call_ids, mut assistant) = (Vec::new(), Vec::new(), String::new());
    for raw in items {
        let item = gj::parse(&raw);
        match codex_replay::field(&item, "type").as_str() {
            "reasoning" => replay.push(raw.clone()),
            "function_call" | "custom_tool_call" => {
                let id = codex_replay::field(&item, "call_id");
                if !id.is_empty() {
                    call_ids.push(id);
                }
                replay.push(raw.clone());
            }
            "message" => {
                let fingerprint = codex_replay::assistant_fingerprint(&item);
                if !fingerprint.is_empty() {
                    assistant = fingerprint;
                }
            }
            _ => {}
        }
    }
    if replay.is_empty() {
        return None;
    }
    let mut hasher = Sha256::new();
    hasher.update(scope.request_fingerprint.as_bytes());
    hasher.update(format!("\0assistant\0{assistant}").as_bytes());
    for id in &call_ids {
        hasher.update(format!("\0call\0{id}").as_bytes());
    }
    for item in &replay {
        hasher.update(b"\0item\0");
        hasher.update(item);
    }
    let mut marker = format!(r#"{{"type":"{}"}}"#, codex_replay::TURN_TYPE).into_bytes();
    gj::set_str(&mut marker, "id", codex_replay::hex(&hasher.finalize()));
    if !assistant.is_empty() {
        gj::set_str(&mut marker, "assistant_fingerprint", &assistant);
    }
    if !scope.request_fingerprint.is_empty() {
        gj::set_str(&mut marker, "request_fingerprint", &scope.request_fingerprint);
    }
    for id in &call_ids {
        gj::set_str(&mut marker, "call_ids.-1", id);
    }
    Some(std::iter::once(marker).chain(replay).collect())
}
