//! The xAI request shaping of `prepareResponsesRequestTo` after translation, run on a
//! Responses body: by the old path on its translation of the client body, and by the
//! executor (`xai_exec`) on its own encoded upload. [`Inbound`] is what shaping reads besides
//! that body.

use ck_core::config::Config;
use ck_core::exec::{ExecError, ExecRequest, FailureScope, ResolvedModel};
use ck_core::format::Format;
use ck_wire::gostr::GoStr;
use ck_wire::json as gj;
use ck_wire::thinking::{self, ModelCaps, RequestThinking, parse_suffix};
use http::HeaderMap;
use sha2::{Digest, Sha256};

use crate::openai_compat_payload::{self as compat, set_bool_if_different, set_str_if_different};
use crate::xai::PROVIDER;
use crate::xai_replay as replay;
use crate::xai_request::{self as request, Prepared};
use crate::xai_response::{self as response, text};
use ck_translate::apply_patch_responses as apply_patch;

const COMPOSER_PREFIX: &str = "grok-composer-";

/// The request facts shaping reads besides the body it shapes.
pub(crate) struct Inbound<'a> {
    /// The client's protocol.
    pub source: Format,
    /// The protocol of `payload` and `original`: the client's on the old path, Responses for
    /// the executor's upload.
    pub payload_format: Format,
    /// The upstream model, thinking suffix included.
    pub model: &'a str,
    /// The request body thinking and the session keys read.
    pub payload: &'a [u8],
    /// The body before any executor change (Go `opts.OriginalRequest`).
    pub original: &'a [u8],
    pub headers: &'a HeaderMap,
    pub resolved: Option<&'a ResolvedModel>,
    pub execution_session: Option<&'a str>,
    pub derived_session: Option<&'a str>,
    /// The caller's client key; empty when client auth is off.
    pub principal: &'a str,
    /// The attempt's credential on the executor path, where replay is namespaced by caller
    /// and credential; `None` on the old path.
    pub credential: Option<&'a str>,
    /// A turn of a downstream Responses WebSocket (Go `DownstreamWebsocket(ctx)`).
    pub downstream_websocket: bool,
}

impl<'a> Inbound<'a> {
    /// The old path's facts.
    pub(crate) fn of(req: &'a ExecRequest) -> Self {
        Self {
            source: req.source_format,
            payload_format: req.source_format,
            model: &req.model,
            payload: &req.body,
            original: request::original(req),
            headers: &req.headers,
            resolved: req.resolved_model.as_ref(),
            execution_session: req.execution_session.as_deref(),
            derived_session: req.derived_session.as_deref(),
            principal: &req.caller.principal,
            credential: None,
            downstream_websocket: false,
        }
    }

    /// `xaiExecutionSessionID`.
    pub(crate) fn execution_session_id(&self) -> String {
        if let Some(s) = nonempty(self.execution_session) {
            return s.to_owned();
        }
        let key = text(&gj::get(self.payload, "prompt_cache_key"));
        if !key.is_empty() {
            return key;
        }
        // helps.DerivedSessionUUID("xai", ...).
        match nonempty(self.derived_session) {
            Some(derived) => uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_OID,
                format!("cli-proxy-api\0{PROVIDER}\0derived-session\0{derived}").as_bytes(),
            )
            .to_string(),
            None => String::new(),
        }
    }

    /// `xaiResolveComposerSessionID`: the conversation id Grok composer models need.
    fn composer_session_id(&self, base_model: &str) -> String {
        let session = self.execution_session_id();
        if !session.is_empty() {
            return session;
        }
        if !base_model.trim().go_lower().starts_with(COMPOSER_PREFIX) {
            return String::new();
        }
        if let Some(key) = compat::claude_code_prompt_cache(base_model, self.payload, self.headers) {
            return key;
        }
        uuid::Uuid::new_v4().to_string()
    }

    /// `xaiReasoningReplayIsolateSessionKey`: the old path's replay key of session `key`,
    /// a client-controlled key namespaced by the caller's key; the execution contract's
    /// route keys sessions with `xai_replay_key`.
    pub(crate) fn isolate(&self, key: &str) -> String {
        let (key, principal) = (key.trim(), self.principal.trim());
        if key.is_empty() || key.starts_with("execution:") {
            return key.to_owned();
        }
        if principal.is_empty() {
            return String::new();
        }
        format!("caller:{}:{key}", digest(principal))
    }
}

fn nonempty(s: Option<&str>) -> Option<&str> {
    s.map(str::trim).filter(|s| !s.is_empty())
}

/// The first eight bytes of the SHA-256 of `value`, in hex.
fn digest(value: &str) -> String {
    Sha256::digest(value.as_bytes())[..8]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The shaping of `prepareResponsesRequestTo` after translation: thinking, the upstream
/// model and stream flag, the tool and input rewrites, reasoning replay and the session id.
/// `declarations` is the Responses body whose tools decide the apply_patch winners. The
/// payload rules are not applied: they run last, on the caller's finished body. The result's
/// `original_payload` and `rule_original` stay empty: the old path sets them, and the executor
/// reads its upload instead.
pub(crate) fn shape(
    inbound: &Inbound<'_>,
    cfg: &Config,
    store: &replay::Store,
    stream: bool,
    to: Format,
    body: Vec<u8>,
    declarations: &[u8],
) -> Result<Prepared, ExecError> {
    let base_model = parse_suffix(inbound.model).model_name;
    // cliproxyauth.ResolvedModelInfo, bound by dispatch for configured xai-api-key models.
    let caps = inbound.resolved.map(|r| ModelCaps::from(&r.info));
    let mut body = thinking::apply_request_thinking(&RequestThinking {
        body: &body,
        payload: inbound.payload,
        original: inbound.original,
        model: inbound.model,
        from: inbound.payload_format.as_str(),
        // Go passes the executor identifier as the target format.
        to: PROVIDER,
        provider: PROVIDER,
        resolved: caps.as_ref().map(Some),
        has_request_transformer: false,
        updates_changed: false,
    })
    .map_err(|e| ExecError::local(e.status(), FailureScope::Request, e.message))?;
    set_str_if_different(&mut body, "model", &base_model);
    set_bool_if_different(&mut body, "stream", stream);
    for key in [
        "previous_response_id",
        "prompt_cache_retention",
        "safety_identifier",
        "stream_options",
    ] {
        gj::delete(&mut body, key);
    }
    let settings = ck_wire::codex_client::Settings::from_config(cfg);
    body = ck_wire::codex_client::rewrite_multi_agent_v2_input(inbound.headers, &body, &settings, false);
    let mut apply_patch = apply_patch::State::new(inbound.payload_format, inbound.original, declarations);
    // A plain Go error (translator/common NormalizeApplyPatchResponsesRequest).
    body = apply_patch::normalize_executor_request(&body, Some(inbound.original))
        .map_err(crate::openai_compat::plain_err)?;
    let (body, tools) = request::shape_tools(body, cfg, &mut apply_patch);
    let replay_scope = replay::scope(inbound, &body);
    let mut body = request::shape_input(replay::apply(store, &replay_scope, body), &tools);
    let session_id = inbound.composer_session_id(&base_model);
    if !session_id.is_empty() {
        set_str_if_different(&mut body, "prompt_cache_key", &session_id);
    }
    Ok(Prepared {
        filter_internal_x_search: response::request_has_native_x_search(&body),
        base_model,
        original_payload: Vec::new(),
        body,
        namespace_tools: tools.namespace_tools,
        client_declared_tools: tools.client_declared_tools,
        session_id,
        replay_scope,
        web_search_alias: tools.web_search_alias,
        apply_patch,
        rule_original: Vec::new(),
        rule_protocol: to,
    })
}

/// The `/responses/compact` body of `executeCompactRequest`: no stream flag, tools,
/// sampling or limit members, and no compaction trigger; the client's response id kept.
pub(crate) fn compact_body(mut body: Vec<u8>, previous_response_id: &str) -> Vec<u8> {
    for field in ["stream", "tools"] {
        gj::delete(&mut body, field);
    }
    body = request::normalize_tool_choice_for_tools(body);
    for field in ["max_output_tokens", "temperature", "top_p", "top_k", "stop"] {
        gj::delete(&mut body, field);
    }
    body = crate::xai::remove_input_items(body, "compaction_trigger");
    if !previous_response_id.is_empty() {
        gj::set_str(&mut body, "previous_response_id", previous_response_id);
    }
    body
}
