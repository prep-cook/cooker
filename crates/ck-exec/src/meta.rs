//! Meta Muse executor (internal/runtime/executor/meta_executor*.go).
//!
//! Every client format is translated to Meta Responses and sent, always streaming, to
//! `{base}/responses`. Non-streaming clients get the completed response collected from
//! that SSE stream. A credential holds either a minted API key or only the DCA token
//! (`dca:...`) a key is minted from: by [`MetaExecutor::prepare`] (persisted), or inline
//! for one request as Go's ensureAuth does when preparation did not run.
//!
//! Request translation uses Meta policy; response translation keeps the registered Codex
//! pairs. The apply_patch bridge runs around the Responses wire: a client's
//! custom `apply_patch` tool goes upstream as a JSON function and its calls come back as
//! the custom tool.

use ck_core::gostr::GoStr;
use std::collections::VecDeque;
use std::time::Duration;

use bytes::Bytes;
use ck_core::config::Config;
use ck_core::credential::{Credential, MetadataPatch, Source};
use ck_core::exec::{ExecError, ExecRequest, ExecResponse, ExecStream, FailureScope, Operation, ResponseBody};
use ck_core::format::Format;
use ck_translate::{Pair, RequestCtx, ResponseCtx, StreamTranslator, apply_patch_responses};
use ck_wire::json::{self as gj, Kind};
use ck_wire::thinking::{ModelCaps, RequestThinking, apply_request_thinking, parse_suffix};
use futures_util::StreamExt;
use serde_json::Value;

use crate::codex_response::OutputItems;
use crate::codex_tokens::count_input_tokens;
use crate::kimi_http::{
    DeferredUsage, UsageRule, defer_usage, payload_rules, read_all_strict, refresh_due, report_model, rfc3339_local_now,
};
use crate::meta_auth::{DEFAULT_API_BASE_URL, MetaAuth, MintedKey};
use crate::meta_codex::go_trim_space;
use crate::openai_compat_payload::{ensure_responses_usage_details, sanitize_reasoning_encrypted_content};
use crate::proxy::{GoClients, GoHeaders, MAX_ERROR_BODY, Proxy, default_client, lines, read_all, send};
use crate::tokenizer::Encoding;
use ck_wire::codex_client::normalize_codex_instructions;
#[path = "meta_request.rs"]
mod request;
use request::prepare;
pub(crate) use request::sanitize_web_search_tools;
#[path = "meta_response.rs"]
mod response;

/// Provider string served by this executor.
pub const PROVIDER: &str = "meta";
/// `metaUserAgent`.
pub const USER_AGENT: &str =
    "muse-build/1.3.0 (interactive; macos-aarch64; build ac7280f2aca67769d1455a8847bb502b617d50f6)";
/// `metaNotFoundCooldown`: a 404 without `resets_at` cools the model for five minutes.
const NOT_FOUND_COOLDOWN: Duration = Duration::from_secs(300);
/// Go's bufio.Scanner limit for the upstream stream.
pub(crate) const LINE_LIMIT: usize = 52_428_800;
pub const DISCONNECTED: &str =
    "meta stream error: stream disconnected before response.completed or response.incomplete";

pub struct MetaExecutor {
    pub(crate) clients: GoClients,
    mint_url: Option<String>,
}

impl Default for MetaExecutor {
    fn default() -> Self {
        Self::with_client(default_client())
    }
}

impl MetaExecutor {
    /// Uses a caller-built client (tests point it at local mocks).
    pub fn with_client(client: wreq::Client) -> Self {
        Self {
            clients: GoClients::with_default(client),
            mint_url: None,
        }
    }

    /// Mints against a local mock instead of `META_MINT_URL` / api.meta.ai.
    pub fn with_mint_url(mut self, url: &str) -> Self {
        self.mint_url = Some(url.to_owned());
        self
    }

    /// ShouldPrepareRequestAuth (no usable key but a DCA token), or a due
    /// `refresh_interval`: the SDK refresh lead is nil, so nothing else schedules a mint.
    pub fn needs_prepare(&self, credential: &Credential, cfg: &Config) -> bool {
        self.needs_prepare_at(credential, cfg, chrono::Utc::now())
    }

    pub fn needs_prepare_at(&self, credential: &Credential, _cfg: &Config, now: chrono::DateTime<chrono::Utc>) -> bool {
        self.must_mint(credential)
            || (!is_config_api_key(credential)
                // Go's refresh loop never schedules API-key-kind credentials.
                && !ck_core::registry::dynamic::is_api_key(credential)
                && dca_token(credential).is_some()
                && refresh_due(credential, None, now))
    }

    /// `ShouldPrepareRequestAuth`: requests must wait for a mint because there is no
    /// usable key, only a DCA token.
    pub fn must_mint(&self, credential: &Credential) -> bool {
        !is_config_api_key(credential) && creds(credential).1.is_empty() && dca_token(credential).is_some()
    }

    /// Refresh: re-mints the API key from the DCA token. Also what the runtime should call
    /// after an upstream 401 (Go's tryRefreshAfterUnauthorized covers Meta DCA tokens).
    pub async fn prepare(&self, credential: &Credential, cfg: &Config) -> Result<MetadataPatch, ExecError> {
        self.refresh(credential, cfg).await
    }

    async fn refresh(&self, credential: &Credential, cfg: &Config) -> Result<MetadataPatch, ExecError> {
        let Some(dca) = dca_token(credential) else {
            if !creds(credential).1.is_empty() {
                return Ok(MetadataPatch::default());
            }
            return Err(ExecError::local(
                401,
                FailureScope::Credential,
                "meta executor: missing API key or DCA token",
            ));
        };
        let mut auth = MetaAuth::new(self.clients.get(&Proxy::effective(credential, cfg)));
        if let Some(url) = &self.mint_url {
            auth = auth.with_mint_url(url);
        }
        let minted = auth.mint_api_key(&dca).await.map_err(|e| {
            ExecError::local(
                500,
                FailureScope::Credential,
                format!("meta executor: mint API key failed: {}", e.redacted()),
            )
        })?;
        if minted.api_key.is_empty() {
            return Err(ExecError::local(
                500,
                FailureScope::Credential,
                "meta executor: mint API key returned empty key",
            ));
        }
        Ok(refresh_patch(credential, &dca, &minted, rfc3339_local_now()))
    }

    /// ensureAuth + enrichAuth: a usable key (minting inline from the DCA token when
    /// missing), `base_url`/`api_key` attributes, and Meta's User-Agent as a default
    /// custom header.
    pub(crate) async fn ensure_auth(&self, credential: &Credential, cfg: &Config) -> Result<Credential, ExecError> {
        let mut c = credential.clone();
        if creds(&c).1.is_empty() && dca_token(&c).is_some() {
            let patch = self.refresh(&c, cfg).await?;
            patch.apply(&mut c.metadata);
            // Go's Refresh also writes these attributes on the request's auth.
            for key in ["base_url", "api_key", "access_token"] {
                if let Some(Value::String(v)) = patch.set.get(key) {
                    c.attributes.insert(key.into(), v.clone());
                }
            }
        }
        let (base, token) = creds(&c);
        if token.is_empty() {
            let message = if is_config_api_key(&c) {
                "meta executor: meta-api-key requires a valid API key (DCA tokens require OAuth storage)"
            } else {
                "meta executor: missing API key or access token"
            };
            return Err(ExecError::local(401, FailureScope::Credential, message));
        }
        if c.attributes.get("base_url").is_none_or(|v| v.trim().is_empty()) {
            c.attributes.insert("base_url".into(), base);
        }
        if c.attributes.get("api_key").is_none_or(|v| v.trim().is_empty()) {
            c.attributes.insert("api_key".into(), token);
        }
        c.attributes
            .entry("header:User-Agent".into())
            .or_insert_with(|| USER_AGENT.into());
        Ok(c)
    }

    pub async fn execute(
        &self,
        credential: &Credential,
        req: ExecRequest,
        cfg: &Config,
    ) -> Result<ExecResponse, ExecError> {
        if req.operation == Operation::CountTokens {
            return self.count_tokens(credential, req, cfg).await;
        }
        if req.alt.as_deref() == Some("responses/compact") {
            return Err(ExecError::local(
                501,
                FailureScope::Request,
                "/responses/compact not supported",
            ));
        }
        let enriched = self.ensure_auth(credential, cfg).await?;
        let prepared = prepare(&req, cfg, true)?;
        req.usage.request(Format::Codex, &prepared.body);
        let (base, token) = creds(&enriched);
        if base.trim().is_empty() {
            return Err(ExecError::local(
                401,
                FailureScope::Credential,
                "meta executor: missing provider baseURL",
            ));
        }
        let url = format!("{}/responses", base.strip_suffix('/').unwrap_or(&base));
        let client = self.clients.get(&Proxy::effective(&enriched, cfg));
        let body = Bytes::from(prepared.body);
        // Go: TrackHTTPClient for Execute; ExecuteStream uses TrackHTTPClientRoundTripOnly,
        // so its first body byte only records the first-packet fallback.
        use crate::kimi_http::{capture_chunk, capture_error, capture_metadata, capture_request};
        let capture = req.capture();
        let request_headers = headers(credential, &req.headers, req.session.as_deref(), &token);
        // recordMetaRequest: the enriched credential's account info.
        let (auth_type, auth_value) = ck_core::registry::dynamic::account_info(&enriched);
        capture_request(
            capture,
            &enriched,
            &url,
            &request_headers,
            &body,
            PROVIDER,
            (auth_type, &auth_value),
        );
        req.usage.round_trip_started();
        let mut upstream = send(&client, &url, request_headers, body.clone(), None)
            .await
            .inspect_err(|e| capture_error(capture, e))?;
        capture_metadata(capture, upstream.status, &upstream.headers);
        upstream.body = crate::kimi_http::track_first_byte(upstream.body, &req.usage, req.stream);
        if !(200..300).contains(&upstream.status) {
            let headers = upstream.headers.clone();
            // Go returns a failed error-body read as is, never classified by status.
            let error_body = read_all_strict(upstream.body, MAX_ERROR_BODY)
                .await
                .inspect_err(|e| capture_error(capture, e))?;
            // ponytail: the MAX_ERROR_BODY prefix (proxy.rs ceiling); Go logs the whole body.
            capture_chunk(capture, &error_body);
            // Go observes the response model on every non-stream body, errors included.
            if !req.stream {
                report_model(&req.usage, Format::Codex, &error_body);
            }
            let mut error = upstream_error(upstream.status, &error_body);
            error.headers = Box::new(headers);
            return Err(error);
        }
        response::finish(&req, &body, upstream, prepared.response, prepared.apply_patch).await
    }

    /// CountTokens: O200kBase estimate of the translated Codex body, no upstream call.
    async fn count_tokens(
        &self,
        credential: &Credential,
        req: ExecRequest,
        cfg: &Config,
    ) -> Result<ExecResponse, ExecError> {
        self.ensure_auth(credential, cfg).await?;
        let prepared = prepare(&req, cfg, false)?;
        let count = count_input_tokens(Encoding::O200kBase, &prepared.body).map_err(|e| {
            ExecError::local(
                500,
                FailureScope::Request,
                format!("meta executor: tokenizer init failed: {e}"),
            )
        })?;
        let usage = format!(
            r#"{{"response":{{"usage":{{"input_tokens":{count},"output_tokens":0,"total_tokens":{count}}}}}}}"#
        );
        let payload = ck_translate::translate_token_count(req.response_format, Format::Codex, count, usage.as_bytes());
        Ok(ExecResponse {
            status: 200,
            headers: http::HeaderMap::new(),
            body: ResponseBody::Buffered(Bytes::from(payload)),
        })
    }
}

fn response_ctx<'a>(req: &'a ExecRequest, translated: &'a [u8]) -> ResponseCtx<'a> {
    ResponseCtx {
        model: &req.model,
        original_request: original(req),
        translated_request: translated,
        tool_search: false,
    }
}

/// `opts.OriginalRequest`, falling back to the request payload when empty.
fn original(req: &ExecRequest) -> &Bytes {
    if req.original_body.is_empty() {
        &req.body
    } else {
        &req.original_body
    }
}

/// `SetStringIfDifferent`.
pub(crate) fn set_string_if_different(body: &mut Vec<u8>, path: &str, value: &str) {
    let current = gj::get(body, path);
    if current.kind == Kind::String && *current.bytes() == *value.as_bytes() {
        return;
    }
    gj::set_str(body, path, value);
}

/// `SetBoolIfDifferent`.
fn set_bool_if_different(body: &mut Vec<u8>, path: &str, value: bool) {
    let kind = gj::get(body, path).kind;
    if (value && kind == Kind::True) || (!value && kind == Kind::False) {
        return;
    }
    gj::set_bool(body, path, value);
}

/// The upstream request headers; `client` and `session` are the client's request headers
/// and its bound session, for the credential's custom headers.
pub(crate) fn headers(c: &Credential, client: &http::HeaderMap, session: Option<&str>, token: &str) -> GoHeaders {
    let mut h = GoHeaders::new();
    h.set("Content-Type", "application/json");
    if !token.trim().is_empty() {
        h.set("Authorization", format!("Bearer {token}"));
    }
    h.set("User-Agent", USER_AGENT);
    h.set("X-Client-Id", "tbh:tui");
    // Meta always streams upstream.
    h.set("Accept", "text/event-stream");
    h.set("Cache-Control", "no-cache");
    let session = ck_wire::session::cpa_session_id(session);
    for (name, value) in ck_wire::headers::custom_headers(&c.attributes, client, session.as_deref()) {
        h.set(&name, value);
    }
    if ck_core::registry::dynamic::is_api_key(c) {
        h.set("User-Agent", "cooker");
    }
    h
}

/// `wrapMetaUpstreamError`, with Go's cooldown semantics as scopes: a 429 or 404 cools
/// the model on this credential, a subscription-quota 429 the whole credential.
pub(crate) fn upstream_error(status: u16, body: &[u8]) -> ExecError {
    let mut retry_after = None;
    let scope = match status {
        429 => {
            retry_after = resets_in(body);
            if subscription_quota(body) {
                FailureScope::Credential
            } else {
                FailureScope::Model
            }
        }
        404 => {
            retry_after = Some(resets_in(body).unwrap_or(NOT_FOUND_COOLDOWN));
            FailureScope::Model
        }
        401 | 402 | 403 | 408 | 500.. => FailureScope::Credential,
        _ => FailureScope::Request,
    };
    ExecError {
        status,
        scope,
        body: Bytes::copy_from_slice(body),
        headers: Box::default(),
        retry_after,
        direct: false,
        addon: None,
        upstream: true,
    }
}

/// `parseMetaRetryAfter` for a 429/404 body: time until a future `error.resets_at`.
fn resets_in(body: &[u8]) -> Option<Duration> {
    let resets_at = gj::get(body, "error.resets_at").int();
    if resets_at <= 0 {
        return None;
    }
    let at = std::time::UNIX_EPOCH + Duration::from_secs(resets_at as u64);
    at.duration_since(std::time::SystemTime::now())
        .ok()
        .filter(|d| !d.is_zero())
}

/// `isMetaSubscriptionQuota` (status already 429).
fn subscription_quota(body: &[u8]) -> bool {
    if body.is_empty() {
        return false;
    }
    let message = gj::get(body, "error.message").str().go_lower();
    let code = gj::get(body, "error.code").str().go_lower();
    if message.contains("subscription quota") || message.contains("quota exhausted") {
        return true;
    }
    (code == "rate_limit_exceeded" || code.contains("quota")) && gj::get(body, "error.resets_at").exists()
}

/// `metaStreamEventError`: an `error` or `response.failed` event ends the response with
/// `error.code` as status when it is an HTTP error code, else 502.
pub(crate) fn stream_event_error(event: &[u8]) -> Option<ExecError> {
    let kind = gj::get(event, "type").bytes();
    if &*kind != b"error" && &*kind != b"response.failed" {
        return None;
    }
    let code = gj::get(event, "error.code").int();
    let status = if (400..=599).contains(&code) { code as u16 } else { 502 };
    Some(upstream_error(status, event))
}

/// The Refresh metadata change after a successful mint.
pub(crate) fn refresh_patch(
    credential: &Credential,
    dca: &str,
    minted: &MintedKey,
    now_local: String,
) -> MetadataPatch {
    let mut base = creds(credential).0;
    if !minted.base_url.trim().is_empty() {
        base = minted.base_url.trim().to_owned();
    }
    let mut patch = MetadataPatch::default();
    let mut set = |key: &str, value: Value| {
        patch.set.insert(key.into(), value);
    };
    set("base_url", base.into());
    set("api_key", minted.api_key.clone().into());
    set("access_token", minted.api_key.clone().into());
    set("dca_token", dca.into());
    if !minted.user_email.is_empty() {
        set("email", minted.user_email.clone().into());
    }
    if !minted.user_full_name.is_empty() {
        set("name", minted.user_full_name.clone().into());
    }
    if !minted.subs_tier_name.is_empty() {
        set("subs_tier_name", minted.subs_tier_name.clone().into());
    }
    if !minted.subs_tier_id.is_empty() {
        set("subs_tier_id", minted.subs_tier_id.clone().into());
    }
    set("is_subs_active", minted.is_subs_active.into());
    set("has_payment_method", minted.has_payment_method.into());
    set("type", PROVIDER.into());
    set("last_refresh", now_local.into());
    patch.remove.push("expired".into());
    if minted.subs_tier_name.is_empty() {
        patch.remove.push("subs_tier_name".into());
    }
    if minted.subs_tier_id.is_empty() {
        patch.remove.push("subs_tier_id".into());
    }
    patch
}

fn attribute<'a>(c: &'a Credential, key: &str) -> &'a str {
    c.attributes.get(key).map(|v| v.trim()).unwrap_or_default()
}

fn metadata_str<'a>(c: &'a Credential, key: &str) -> Option<&'a str> {
    c.str(key).map(str::trim).filter(|s| !s.is_empty())
}

/// `metaCreds`: base URL and usable (non-DCA) key, attributes before metadata.
pub(crate) fn creds(c: &Credential) -> (String, String) {
    let mut base = DEFAULT_API_BASE_URL.to_owned();
    let mut token = String::new();
    let usable = |t: &str| !t.is_empty() && !t.starts_with("dca:");
    if !attribute(c, "base_url").is_empty() {
        base = attribute(c, "base_url").to_owned();
    }
    if usable(attribute(c, "api_key")) {
        token = attribute(c, "api_key").to_owned();
    } else if usable(attribute(c, "access_token")) {
        token = attribute(c, "access_token").to_owned();
    }
    if base == DEFAULT_API_BASE_URL
        && let Some(b) = metadata_str(c, "base_url").or_else(|| metadata_str(c, "api_base_url"))
    {
        base = b.to_owned();
    }
    if token.is_empty() {
        if let Some(k) = metadata_str(c, "api_key").filter(|k| usable(k)) {
            token = k.to_owned();
        } else if let Some(t) = metadata_str(c, "access_token").filter(|t| usable(t)) {
            token = t.to_owned();
        }
    }
    (base, token)
}

/// `extractDCAToken`: never for config API keys.
pub(crate) fn dca_token(c: &Credential) -> Option<String> {
    if is_config_api_key(c) {
        return None;
    }
    if !attribute(c, "dca_token").is_empty() {
        return Some(attribute(c, "dca_token").to_owned());
    }
    if attribute(c, "access_token").starts_with("dca:") {
        return Some(attribute(c, "access_token").to_owned());
    }
    if let Some(d) = metadata_str(c, "dca_token") {
        return Some(d.to_owned());
    }
    metadata_str(c, "access_token")
        .filter(|t| t.starts_with("dca:"))
        .map(str::to_owned)
}

/// `IsConfigAPIKeyAuth`: an API-key credential whose source is config.yaml.
pub(crate) fn is_config_api_key(c: &Credential) -> bool {
    let auth_kind = ck_core::registry::dynamic::auth_kind(c);
    if auth_kind != Some("apikey") {
        return false;
    }
    if attribute(c, "runtime_only").go_eq_fold("true") {
        return false;
    }
    match attribute(c, "source_backend").go_lower().as_str() {
        "config" => return true,
        "file" | "filesystem" | "git" | "memory" | "runtime" | "runtime_only" | "objectstore" | "object-store"
        | "postgres" | "postgresql" | "database" | "db" => return false,
        _ => {}
    }
    let source = attribute(c, "source");
    if !source.is_empty() {
        let lower = source.go_lower();
        return lower.starts_with("config:") || lower == "config";
    }
    if !attribute(c, "path").is_empty() {
        return false;
    }
    matches!(c.source, Source::Config { .. })
}

#[cfg(test)]
#[path = "meta_tests.rs"]
mod tests;
