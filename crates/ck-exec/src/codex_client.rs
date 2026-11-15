//! Request translation for Codex clients (executor/helps/codex_multi_agent_v2.go): the
//! `ck_wire::codex_client` rewrites around `ck_translate`, for every executor that
//! translates a request Codex clients may send. The pure rewrites live in ck-wire;
//! this module also needs the translators, which ck-wire cannot depend on.

use ck_core::config::Config;
use ck_core::format::Format;
use ck_core::gostr::GoStr;
use ck_translate::RequestCtx;
use ck_wire::codex_client::{self as cc, Settings};
use http::HeaderMap;

/// The client side of one translation: its headers, the config the executor reads, the
/// executor's identity and whether the selected API-key model is `is-compat`.
pub(crate) struct Client<'a> {
    pub headers: &'a HeaderMap,
    pub settings: Settings,
    /// Go's `targetExecutor`: Codex executors keep integer tool types; "" for executors
    /// Go calls without one.
    pub target_executor: &'a str,
    pub is_compat: bool,
}

impl<'a> Client<'a> {
    /// The executor's own config, as Go's executors pass `e.cfg`.
    pub fn new(headers: &'a HeaderMap, cfg: &Config, target_executor: &'a str, is_compat: bool) -> Self {
        Self {
            headers,
            settings: Settings::from_config(cfg),
            target_executor,
            is_compat,
        }
    }
}

/// `isCodexTargetExecutor`.
fn codex_target(executor: &str) -> bool {
    matches!(
        executor.trim().go_lower().as_str(),
        "codex" | "codex-websockets" | "codex_websockets"
    )
}

/// The `...WithCompat` translator Go picks for an is-compat model, if the pair has one.
fn compat_translator(from: Format, to: Format) -> Option<ck_translate::RequestFn> {
    Some(match (from, to) {
        (Format::Claude, Format::Codex) => ck_translate::claude_to_codex_with_compat,
        (Format::Claude, Format::Gemini) => ck_translate::claude_to_gemini_with_compat,
        (Format::Claude, Format::Interactions) => ck_translate::claude_to_interactions_with_compat,
        (Format::Claude, Format::OpenAI) => ck_translate::claude_to_openai_with_compat,
        (Format::OpenAI, Format::Claude) => ck_translate::openai_to_claude_with_compat,
        (Format::OpenAIResponse, Format::Claude) => ck_translate::responses_to_claude_with_compat,
        _ => return None,
    })
}

/// `TranslateRequestWithAPIKeyModelCompatibilityForExecutor` (and, without compat,
/// `TranslateRequestWithCodexMultiAgentV2ForExecutor`):
/// 1. Codex clients' integer tool types are normalized for non-Codex executors.
/// 2. OpenAI Responses requests get the orphan-delegation rewrite and, for non-Codex
///    targets, the multi-agent v2 input rewrite (portable messages; compat also strips
///    `author`, `recipient` and passthrough metadata).
/// 3. An is-compat model uses its pair's `...WithCompat` translator between summary
///    extraction and application; anything else goes through
///    `ck_translate::translate_request`.
pub(crate) fn translate_request(
    from: Format,
    to: Format,
    ctx: &RequestCtx<'_>,
    body: &[u8],
    client: &Client<'_>,
) -> Result<Vec<u8>, ck_translate::Error> {
    let mut payload = if codex_target(client.target_executor) {
        body.to_vec()
    } else {
        ck_wire::payload::normalize_codex_tool_integer_types(body, client.headers)
    };
    if from == Format::OpenAIResponse {
        payload = cc::rewrite_orphan_delegation_input(client.headers, &payload, client.settings.orphan_delegation);
        if !matches!(to, Format::Codex | Format::OpenAIResponse) {
            payload = cc::rewrite_multi_agent_v2_input(client.headers, &payload, &client.settings, client.is_compat);
        }
    }
    if from == Format::OpenAIResponse && to == Format::Codex && !codex_target(client.target_executor) {
        non_codex_options(&mut payload);
    }
    let compat = client.is_compat.then(|| compat_translator(from, to)).flatten();
    let Some(translate) = compat else {
        return ck_translate::translate_request(from, to, ctx, &payload);
    };
    use ck_wire::thinking::{apply_summary_config_for_model, extract_translated_summary_config};
    let summary = extract_translated_summary_config(&payload, from.as_str(), to.as_str());
    let translated = translate(ctx, &payload)?;
    Ok(apply_summary_config_for_model(
        &translated,
        to.as_str(),
        ctx.model,
        summary,
    ))
}

/// `TranslateRequestEnvelopeWithCodexMultiAgentV2`: the Responses orphan-delegation and
/// multi-agent v2 rewrites (no integer tool-type normalization), then
/// `ck_translate::translate_request_envelope` with the attempt's model info.
pub(crate) fn translate_request_envelope(
    from: Format,
    to: Format,
    ctx: &RequestCtx<'_>,
    body: &[u8],
    client: &Client<'_>,
    model_info: Option<&ck_core::registry::ModelInfo>,
) -> Result<Vec<u8>, ck_translate::Error> {
    let mut payload = body.to_vec();
    if from == Format::OpenAIResponse {
        payload = cc::rewrite_orphan_delegation_input(client.headers, &payload, client.settings.orphan_delegation);
        if !matches!(to, Format::Codex | Format::OpenAIResponse) {
            payload = cc::rewrite_multi_agent_v2_input(client.headers, &payload, &client.settings, client.is_compat);
        }
    }
    ck_translate::translate_request_envelope(from, to, ctx, &payload, model_info)
}

/// Routes whose handler runs Go's `prepareCodexMultiAgentV2Tools` before dispatch (the
/// Responses HTTP and WebSocket endpoints, not compact). For an eligible client that
/// sets Go's `CodexMultiAgentV2ToolsPreparedContextKey`.
pub(crate) fn tools_prepared(request_path: &str) -> bool {
    matches!(request_path, "/v1/responses" | "/backend-api/codex/responses")
}

/// `OptimizeCodexMultiAgentV2RequestForAuth`: the orphan rewrite, the multi-agent v2
/// optimization (the `collaboration` namespace renamed upstream; `true` means responses
/// need [`cc::restore_response`]), then the compat agent-input rewrite. `settings` must
/// come from the credential's config view (`ForAPIKey` for API keys). Tools the
/// Responses boundary already prepared only lose `message.encrypted` again.
pub(crate) fn optimize_for_auth(
    headers: &HeaderMap,
    body: &[u8],
    settings: &Settings,
    is_compat: bool,
    tools_prepared: bool,
) -> (Vec<u8>, bool) {
    let body = cc::rewrite_orphan_delegation_input(headers, body, settings.orphan_delegation);
    let (mut body, optimized) =
        cc::optimize_request(headers, &body, settings, tools_prepared, cc::served_spawn_agent_models);
    if is_compat {
        body = cc::rewrite_multi_agent_v2_input(headers, &body, settings, true);
    }
    (body, optimized)
}

/// Keep other executors using the Codex translator on their existing option policy.
pub(crate) fn non_codex_options(body: &mut Vec<u8>) {
    use ck_wire::json::{self as gj, Kind};
    let parallel = gj::get(body, "parallel_tool_calls");
    if parallel.exists() && parallel.kind != Kind::True {
        gj::set_bool(body, "parallel_tool_calls", true);
    }
    if gj::get(body, "prompt_cache_options").exists() {
        gj::delete(body, "prompt_cache_options");
    }
    let include = gj::get(body, "include");
    if include.is_array() {
        let values = include.array();
        let has = |name: &str| values.iter().any(|v| v.kind == Kind::String && v.str() == name);
        if has("output.encrypted_content") {
            let encoded = if has("web_search_call.action.sources") {
                r#"["reasoning.encrypted_content","web_search_call.action.sources"]"#
            } else {
                r#"["reasoning.encrypted_content"]"#
            };
            gj::set_raw(body, "include", encoded);
        }
    }
}

#[cfg(test)]
#[path = "codex_client_tests.rs"]
mod tests;
