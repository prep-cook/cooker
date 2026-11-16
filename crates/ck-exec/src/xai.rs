//! xAI Grok (internal/runtime/executor/xai_executor*.go): the Responses API reached with
//! an API key at `api.x.ai` or with a Grok OAuth token at the Grok CLI chat proxy, plus
//! Responses compact, token counting and the image and video APIs.
//!
//! Chat always streams upstream (`/responses` with `stream: true`); a non-streaming
//! client gets the translated terminal event. Request shaping lives in xai_request,
//! response rewrites in xai_response and reasoning replay in xai_replay.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use ck_core::config::Config;
use ck_core::credential::{Credential, MetadataPatch};
use ck_core::exec::{ExecError, ExecRequest, ExecResponse, ExecStream, FailureScope, Operation, ResponseBody};
use ck_core::format::Format;
use ck_translate::{ResponseCtx, StreamTranslator};
use ck_wire::gostr::{GoStr, trim_space};
use ck_wire::json::{self as gj, Kind};
use futures_util::StreamExt;
use http::HeaderMap;

use crate::gemini_stream::ClaudeInputTokens;
use crate::openai_compat::status_err;
use crate::openai_compat_http::{self as wire, Clients, GoHeaders};
use crate::openai_compat_payload::{self as compat, ensure_responses_usage_details};
use crate::xai_auth::{self, CLI_CHAT_PROXY_BASE_URL, DEFAULT_API_BASE_URL, metadata_string};
use crate::xai_replay::{self as replay, ReplayScope};
use crate::xai_request::{self as request, Prepared};
use crate::xai_response::{self as response, NamespaceRestorer, OutputItems, XSearchFilter, text};
use ck_translate::apply_patch_responses as apply_patch;

pub const PROVIDER: &str = "xai";

pub(crate) const COMPACT_ALT: &str = "responses/compact";
/// `scanner.Buffer(nil, 52_428_800)`.
pub(crate) const MAX_LINE: usize = 52_428_800;
const DISCONNECTED: &str = "xai stream error: stream disconnected before response.completed or response.incomplete";
/// Keep in sync with the Grok CLI version the chat proxy expects (`xaiClientVersionValue`).
const CLIENT_VERSION: &str = "1.0.44";

/// Go `url.Parse(raw)` succeeds with an `http` or `https` scheme and a host (the video
/// content URL check of the videos handler).
pub fn is_http_url(raw: &str) -> bool {
    crate::xai_url::parse(raw)
        .is_ok_and(|u| matches!(u.scheme.as_str(), "http" | "https") && !(u.hostname.is_empty() && u.port.is_empty()))
}

/// Rewrites upstream URLs (tests point the fixed xAI hosts at a local mock).
type UrlRewrite = Arc<dyn Fn(&str) -> String + Send + Sync>;

#[derive(Default)]
pub struct XaiExecutor {
    pub(crate) clients: Clients,
    pub(crate) replay: Arc<replay::Store>,
    rewrite: Option<UrlRewrite>,
    /// The upstream Responses WebSocket sessions (xai_ws.rs).
    pub(crate) ws: Arc<crate::xai_ws::Ws>,
}

pub(crate) fn apply_patch_error() -> ExecError {
    status_err(502, ck_translate::APPLY_PATCH_UPSTREAM_ERROR)
}

// --- credentials and endpoints (xai_executor_request.go) ---------------------------------

fn attribute<'a>(c: &'a Credential, key: &str) -> &'a str {
    c.attributes.get(key).map_or("", |v| v.trim())
}

/// `xaiCreds`: the bearer token and configured base URL.
pub(crate) fn creds(c: &Credential) -> (String, String) {
    let mut token = attribute(c, "api_key").to_owned();
    let mut base = attribute(c, "base_url").to_owned();
    if token.is_empty() {
        token = metadata_string(c, "access_token");
    }
    if base.is_empty() {
        base = metadata_string(c, "base_url");
    }
    (token, base)
}

/// `strconv.ParseBool`.
pub(crate) fn parse_bool(raw: &str) -> Option<bool> {
    match raw {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

/// `xaiUsingAPI`: the official API path for chat and media. OAuth defaults to the Grok
/// CLI chat proxy.
fn using_api(c: &Credential) -> bool {
    if let Some(value) = parse_bool(attribute(c, "using_api")) {
        return value;
    }
    match c.metadata.get("using_api") {
        Some(serde_json::Value::Bool(b)) => return *b,
        Some(serde_json::Value::String(s)) => {
            if let Some(value) = parse_bool(s.trim()) {
                return value;
            }
        }
        _ => {}
    }
    let kind = attribute(c, "auth_kind");
    if !kind.is_empty() {
        return !kind.go_eq_fold("oauth");
    }
    !metadata_string(c, "auth_kind").go_eq_fold("oauth")
}

fn normalize_base(url: &str) -> &str {
    url.trim().trim_end_matches('/')
}

fn is_default_api(url: &str) -> bool {
    normalize_base(url) == normalize_base(DEFAULT_API_BASE_URL)
}

fn is_cli_chat_proxy(url: &str) -> bool {
    normalize_base(url) == normalize_base(CLI_CHAT_PROXY_BASE_URL)
}

/// `xaiChatBaseURL`: chat and media. Without `using_api` an empty or default base URL
/// becomes the CLI chat proxy; an explicit other base URL is kept.
pub(crate) fn chat_base_url(c: &Credential) -> String {
    let (_, base) = creds(c);
    if using_api(c) {
        return if base.is_empty() {
            DEFAULT_API_BASE_URL.to_owned()
        } else {
            base
        };
    }
    if !base.is_empty() && !is_default_api(&base) {
        return base;
    }
    CLI_CHAT_PROXY_BASE_URL.to_owned()
}

/// `xaiCompactBaseURL`: the chat proxy has no `/responses/compact`.
pub(crate) fn compact_base_url(c: &Credential) -> String {
    let (_, base) = creds(c);
    if base.is_empty() || is_cli_chat_proxy(&base) {
        DEFAULT_API_BASE_URL.to_owned()
    } else {
        base
    }
}

/// `TrackHTTPClient` on a response: the first body byte marks TTFT (the caller marked
/// the round-trip start before sending).
pub(crate) fn track_first_byte(mut upstream: wire::Upstream, usage: &ck_core::exec::UsageSink) -> wire::Upstream {
    if usage.enabled() {
        let usage = usage.clone();
        let mut marked = false;
        upstream.body = upstream
            .body
            .inspect(move |chunk| {
                if !marked && chunk.as_ref().is_ok_and(|b| !b.is_empty()) {
                    marked = true;
                    usage.first_byte();
                }
            })
            .boxed();
    }
    upstream
}

/// Go's deferred `TrackFailure` (or the scanner's `PublishFailure`) when the client goes
/// away mid-request: the canceled context fails the pending HTTP send or body read with
/// `context canceled`, status 499. Dropping a pending future runs none of the executor's
/// error paths, so this publishes that failure when `op` is dropped before it settles,
/// ahead of the server's own end-of-attempt record.
pub(crate) async fn fail_if_canceled<F: std::future::Future>(usage: &ck_core::exec::UsageSink, op: F) -> F::Output {
    struct Pending<'a>(Option<&'a ck_core::exec::UsageSink>);
    impl Drop for Pending<'_> {
        fn drop(&mut self) {
            if let Some(usage) = self.0 {
                // Go's url.Error for a canceled send also names the method and URL; the
                // URL is left out, as in every transport error here.
                usage.publish_failure(499, "context canceled");
            }
        }
    }
    let mut pending = Pending(Some(usage));
    let out = op.await;
    pending.0 = None;
    out
}

/// An error Go returns before creating its usage reporter: the attempt publishes no
/// record (`UsageSink::discard`).
pub(crate) fn before_reporter<T>(
    usage: &ck_core::exec::UsageSink,
    result: Result<T, ExecError>,
) -> Result<T, ExecError> {
    if result.is_err() {
        usage.discard();
    }
    result
}

/// `recordXAIRequest` with the request's own headers. Go logs every xAI request as a POST;
/// this logs the method actually sent, so a video poll shows as a GET
/// (docs/DIFFERENCES-FROM-GO.md).
pub(crate) fn capture_request(
    req: &ExecRequest,
    credential: &Credential,
    method: &str,
    url: &str,
    headers: &GoHeaders,
    body: &[u8],
) -> wire::Capture {
    wire::Capture::request(req, credential, PROVIDER, url, method, headers.pairs(), body)
}

/// The terminal events whose usage Go's HTTP paths observe (`ParseCodexUsage` on
/// completed and incomplete).
pub(crate) const HTTP_USAGE_EVENTS: [&str; 2] = ["response.completed", "response.incomplete"];

/// `strings.TrimSuffix(baseURL, "/") + path`.
pub(crate) fn endpoint(base: &str, path: &str) -> String {
    format!("{}{path}", base.strip_suffix('/').unwrap_or(base))
}

/// Go binds `cfg.ForAPIKey()` for API-key credentials (`executorForAuth`).
pub(crate) fn scoped<'a>(credential: &Credential, cfg: &'a Config) -> Cow<'a, Config> {
    if compat::auth_kind(credential) == "apikey" {
        cfg.for_api_key()
    } else {
        Cow::Borrowed(cfg)
    }
}

/// `applyXAIDefaultHeaders`.
fn default_headers(token: &str, stream: bool, session: &str) -> GoHeaders {
    let mut h = GoHeaders::new();
    h.set("Content-Type", "application/json");
    if !token.trim().is_empty() {
        h.set("Authorization", format!("Bearer {token}"));
    }
    h.set(
        "Accept",
        if stream {
            "text/event-stream"
        } else {
            "application/json"
        },
    );
    h.set("Connection", "Keep-Alive");
    if !session.is_empty() {
        h.set("x-grok-conv-id", session);
    }
    h
}

/// `applyXAICustomHeaders`: `header:*` attributes with the client headers and the
/// canonical session for `$CPA-SESSION-ID`.
fn apply_custom(h: &mut GoHeaders, credential: &Credential, client: &HeaderMap, session: Option<&str>) {
    let session = ck_wire::session::cpa_session_id(session);
    for (name, value) in ck_wire::headers::custom_headers(&credential.attributes, client, session.as_deref()) {
        h.set(&name, value);
    }
}

/// `applyXAIHeaders`.
pub(crate) fn plain_headers(c: &Credential, token: &str, stream: bool, session: &str, req: &ExecRequest) -> GoHeaders {
    request_headers(
        c,
        (token, stream, session),
        (&req.headers, req.session.as_deref()),
        false,
    )
}

/// `applyXAIChatHeaders` when `chat` (the Grok CLI identity when an OAuth credential talks to
/// the chat proxy), else `applyXAIHeaders`. `client` is the client's headers and session.
pub(crate) fn request_headers(
    c: &Credential,
    (token, stream, session): (&str, bool, &str),
    client: (&HeaderMap, Option<&str>),
    chat: bool,
) -> GoHeaders {
    let mut h = default_headers(token, stream, session);
    if chat && !using_api(c) && is_cli_chat_proxy(&chat_base_url(c)) {
        h.set("X-XAI-Token-Auth", "xai-grok-cli");
        h.set("x-grok-client-version", CLIENT_VERSION);
        h.set("User-Agent", format!("xai-grok-workspace/{CLIENT_VERSION}"));
        h.set("x-grok-client-identifier", "grok-shell");
        h.set("x-authenticateresponse", "authenticate-response");
    }
    apply_custom(&mut h, c, client.0, client.1);
    if ck_core::registry::dynamic::is_api_key(c) {
        h.set("User-Agent", "cooker");
    }
    h
}

/// `url.PathEscape`.
pub(crate) fn path_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~' | b'$' | b'&' | b'+' | b':' | b'=' | b'@')
        {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// A non-2xx response: `xaiStatusErr` over the whole body (captured), upstream headers
/// kept. Like Go, a failed body read is returned (and captured) instead.
async fn upstream_error(upstream: wire::Upstream, capture: &wire::Capture) -> ExecError {
    let (status, headers) = (upstream.status, upstream.headers.clone());
    let body = wire::read_all(upstream).await;
    capture.read(&body);
    match body {
        Ok(body) => {
            let mut error = response::status_error(status, &body);
            error.headers = Box::new(headers);
            error
        }
        Err(error) => error,
    }
}

/// `sdktranslator.TranslateNonStream(to, responseFormat)`: identity without a pair. An
/// empty or rejected translation is the apply_patch 502.
fn translate_non_stream(req: &ExecRequest, p: &Prepared, to: Format, data: &[u8]) -> Result<Bytes, ExecError> {
    let ctx = ResponseCtx {
        model: &req.model,
        original_request: &p.original_payload,
        translated_request: &p.body,
        tool_search: false,
    };
    let out = match ck_translate::pair(req.response_format, to) {
        Some(pair) => (pair.non_stream)(&ctx, data).map_err(|_| apply_patch_error())?,
        None => data.to_vec(),
    };
    if out.is_empty() {
        return Err(apply_patch_error());
    }
    Ok(Bytes::from(if req.response_format == Format::OpenAIResponse {
        ensure_responses_usage_details(&out)
    } else {
        out
    }))
}

/// `xaiRemoveInputItemsByType`: the input array is always rebuilt.
pub(crate) fn remove_input_items(mut body: Vec<u8>, item_type: &str) -> Vec<u8> {
    let input = gj::get(&body, "input");
    if !input.is_array() {
        return body;
    }
    let kept: Vec<Vec<u8>> = input
        .array()
        .iter()
        .filter(|item| &*item.get("type").bytes() != item_type.as_bytes())
        .map(|item| item.raw().to_vec())
        .collect();
    let mut raw = b"[".to_vec();
    raw.extend_from_slice(&kept.join(&b","[..]));
    raw.push(b']');
    gj::set_raw(&mut body, "input", raw);
    body
}

impl XaiExecutor {
    /// Uses `client` for credentials without a proxy (tests pass a plain client).
    pub fn with_client(client: wreq::Client) -> Self {
        Self {
            clients: Clients::new(client),
            ..Self::default()
        }
    }

    /// Sends every upstream request to `rewrite(url)` instead of `url`.
    #[cfg(test)]
    pub(crate) fn with_url_rewrite(mut self, rewrite: impl Fn(&str) -> String + Send + Sync + 'static) -> Self {
        self.rewrite = Some(Arc::new(rewrite));
        self
    }

    pub(crate) fn url(&self, url: String) -> String {
        match &self.rewrite {
            Some(rewrite) => rewrite(&url),
            None => url,
        }
    }

    /// Background refresh when a refresh token is close to expiry.
    pub fn needs_prepare(&self, credential: &Credential, cfg: &Config) -> bool {
        self.needs_prepare_at(credential, cfg, chrono::Utc::now())
    }

    pub fn needs_prepare_at(&self, credential: &Credential, _cfg: &Config, now: chrono::DateTime<chrono::Utc>) -> bool {
        xai_auth::needs_refresh(credential, now)
    }

    /// `XAIExecutor.Refresh` through the credential's proxy.
    pub async fn prepare(&self, credential: &Credential, cfg: &Config) -> Result<MetadataPatch, ExecError> {
        let auth = xai_auth::XaiAuth::new(self.clients.for_credential(credential, cfg));
        xai_auth::refresh(&auth, credential).await
    }

    /// A downstream Responses WebSocket turn on the old session path: HTTP generation,
    /// which keeps no upstream state, so a continuation must be replayed. The upstream
    /// socket is [`crate::session::Turns`] (`xai_ws`).
    pub async fn execute_in_session(
        &self,
        credential: &Credential,
        req: ExecRequest,
        cfg: &Config,
        session: &ck_core::exec::ExecSession,
    ) -> Result<ExecResponse, ExecError> {
        if session.continuation {
            // Go's auto executor answers before any reporter exists.
            req.usage.discard();
            return Err(ExecError::replay_required());
        }
        self.execute(credential, req, cfg, true).await
    }

    /// Closes the upstream socket the client connection `id` holds.
    pub fn close_session(&self, id: &str) {
        self.ws.close(id);
    }

    /// Execute / ExecuteStream / CountTokens. `downstream_websocket` marks a turn of a
    /// downstream Responses WebSocket (Go `DownstreamWebsocket(ctx)`).
    pub async fn execute(
        &self,
        credential: &Credential,
        req: ExecRequest,
        cfg: &Config,
        downstream_websocket: bool,
    ) -> Result<ExecResponse, ExecError> {
        let cfg = scoped(credential, cfg);
        let cfg = cfg.as_ref();
        if req.operation == Operation::CountTokens {
            return self.count_tokens(&req, cfg).await;
        }
        if req.alt.as_deref() == Some(COMPACT_ALT) {
            if req.stream {
                // Go answers before creating its usage reporter.
                req.usage.discard();
                return Err(status_err(400, "streaming not supported for /responses/compact"));
            }
            return self.compact(credential, &req, cfg, downstream_websocket).await;
        }
        self.chat(credential, req, cfg, downstream_websocket).await
    }

    async fn chat(
        &self,
        credential: &Credential,
        req: ExecRequest,
        cfg: &Config,
        downstream_websocket: bool,
    ) -> Result<ExecResponse, ExecError> {
        let (token, _) = creds(credential);
        let base = chat_base_url(credential);
        let mut prepared = before_reporter(
            &req.usage,
            request::prepare(&req, cfg, true, Format::Codex, &self.replay, downstream_websocket),
        )?;
        // Execute and ExecuteStream publish only usage they parsed (no EnsurePublished).
        req.usage.usage_required();
        req.usage.request_for(PROVIDER, &prepared.body);
        prepared.finalize_body(cfg, &req);
        let client_headers = (&req.headers, req.session.as_deref());
        let headers = request_headers(credential, (&token, true, &prepared.session_id), client_headers, true);
        let client = self.clients.for_credential(credential, cfg);
        let url = self.url(endpoint(&base, "/responses"));
        let capture = capture_request(&req, credential, "POST", &url, &headers, &prepared.body);
        req.usage.round_trip_started();
        let send = wire::send(&client, &url, headers, Bytes::from(prepared.body.clone()));
        let upstream = fail_if_canceled(&req.usage, send).await;
        capture.sent(&upstream);
        let upstream = track_first_byte(upstream?, &req.usage);
        if !(200..300).contains(&upstream.status) {
            return Err(fail_if_canceled(&req.usage, upstream_error(upstream, &capture)).await);
        }
        let headers = upstream.headers.clone();
        let body = if req.stream {
            let lines = wire::lines(upstream, MAX_LINE);
            ResponseBody::Stream(Pipeline::new(&req, prepared, self.replay.clone(), capture).run(lines))
        } else {
            let data = fail_if_canceled(&req.usage, wire::read_all(upstream)).await;
            capture.read(&data);
            let data = data?;
            ResponseBody::Buffered(buffered(&req, prepared, &self.replay, &data)?)
        };
        Ok(ExecResponse {
            status: 200,
            headers,
            body,
        })
    }

    /// `executeCompactRequest`: the compact call on the official API, replay cleared on
    /// success.
    pub(crate) async fn compact_request(
        &self,
        credential: &Credential,
        req: &ExecRequest,
        cfg: &Config,
        downstream_websocket: bool,
    ) -> Result<(Prepared, Bytes, HeaderMap), ExecError> {
        let (token, _) = creds(credential);
        let base = compact_base_url(credential);
        let mut p = before_reporter(
            &req.usage,
            request::prepare(
                req,
                cfg,
                false,
                Format::OpenAIResponse,
                &self.replay,
                downstream_websocket,
            ),
        )?;
        let previous = text(&gj::get(&req.body, "previous_response_id"));
        let body = crate::xai_shape::compact_body(std::mem::take(&mut p.body), &previous);
        req.usage.request_for(PROVIDER, &body);
        p.body = p.finalize(cfg, req, body);
        let headers = plain_headers(credential, &token, false, &p.session_id, req);
        let client = self.clients.for_credential(credential, cfg);
        let url = self.url(endpoint(&base, "/responses/compact"));
        let capture = capture_request(req, credential, "POST", &url, &headers, &p.body);
        req.usage.round_trip_started();
        let send = wire::send(&client, &url, headers, Bytes::from(p.body.clone()));
        let upstream = fail_if_canceled(&req.usage, send).await;
        capture.sent(&upstream);
        let upstream = track_first_byte(upstream?, &req.usage);
        let status = upstream.status;
        let headers = upstream.headers.clone();
        let data = fail_if_canceled(&req.usage, wire::read_all(upstream)).await;
        capture.read(&data);
        let data = data?;
        if !(200..300).contains(&status) {
            let mut error = response::status_error(status, &data);
            error.headers = Box::new(headers);
            return Err(error);
        }
        // ObserveResponseModel(data), then Publish(ParseOpenAIUsage(data)).
        if req.usage.enabled() {
            req.usage.response_body(Format::OpenAIResponse, &data);
        }
        replay::clear(&self.replay, &p.replay_scope);
        Ok((p, data, headers))
    }

    async fn compact(
        &self,
        credential: &Credential,
        req: &ExecRequest,
        cfg: &Config,
        downstream_websocket: bool,
    ) -> Result<ExecResponse, ExecError> {
        let (mut p, data, headers) = self.compact_request(credential, req, cfg, downstream_websocket).await?;
        let converted = p
            .apply_patch
            .bridge
            .transform_non_stream(&data)
            .map_err(|_| apply_patch_error())?;
        let out = translate_non_stream(req, &p, Format::OpenAIResponse, &converted)?;
        // Publish(ParseOpenAIUsage(data)): the compact body reported by compact_request.
        req.usage.publish();
        Ok(ExecResponse {
            status: 200,
            headers,
            body: ResponseBody::Buffered(out),
        })
    }

    /// CountTokens: O200kBase estimate of the prepared body, no upstream call.
    async fn count_tokens(&self, req: &ExecRequest, cfg: &Config) -> Result<ExecResponse, ExecError> {
        let mut p = request::prepare(req, cfg, false, Format::Codex, &self.replay, false)?;
        p.finalize_body(cfg, req);
        let count = count_input_tokens(&p.body).map_err(|e| {
            ExecError::local(
                500,
                FailureScope::Request,
                format!("xai executor: tokenizer init failed: {e}"),
            )
        })?;
        let usage = format!(
            r#"{{"response":{{"usage":{{"input_tokens":{count},"output_tokens":0,"total_tokens":{count}}}}}}}"#
        );
        let payload = ck_translate::translate_token_count(req.response_format, Format::Codex, count, usage.as_bytes());
        Ok(ExecResponse {
            status: 200,
            headers: HeaderMap::new(),
            body: ResponseBody::Buffered(Bytes::from(payload)),
        })
    }
}
/// The Execute scan over a buffered SSE body: the first completed or incomplete
/// response, with streamed output items restored, translated for the client.
fn buffered(req: &ExecRequest, mut p: Prepared, store: &replay::Store, data: &[u8]) -> Result<Bytes, ExecError> {
    let mut items = OutputItems::default();
    let mut filter = XSearchFilter::new(p.filter_internal_x_search, std::mem::take(&mut p.client_declared_tools));
    let mut restorer = NamespaceRestorer::new(std::mem::take(&mut p.namespace_tools));
    for line in data.split(|b| *b == b'\n') {
        let Some(rest) = line.strip_prefix(b"data:") else {
            continue;
        };
        let event = response::normalize_summary_data(trim_space(rest).to_vec());
        p.apply_patch.remember_dispatcher_event(&event);
        let mut event = restorer.restore(event);
        if !p.web_search_alias.is_empty() {
            event = response::restore_web_search_name(event, &p.web_search_alias);
        }
        let Some(event) = filter.apply(event).filter(|e| !e.is_empty()) else {
            continue;
        };
        let (events, bridge_error) = p.apply_patch.transform(&event);
        if bridge_error.is_some() {
            return Err(apply_patch_error());
        }
        for event in events {
            // ObserveResponseModel on every event; ParseCodexUsage on the terminal one.
            if req.usage.enabled() {
                req.usage
                    .response_line(Format::Codex, &response::usage_line(&event, &HTTP_USAGE_EVENTS));
            }
            let kind = gj::get(&event, "type").bytes().into_owned();
            match kind.as_slice() {
                b"response.output_item.done" => items.collect(&event),
                b"response.completed" | b"response.incomplete" => {
                    let completed = response::normalize_summary_data(items.patch(&event));
                    if kind == b"response.completed" {
                        // Only a completed turn carries replayable terminal state.
                        replay::cache_completed(store, &p.replay_scope, &completed);
                    }
                    let out = translate_non_stream(req, &p, Format::Codex, &completed)?;
                    if response::codex_usage_ok(&event) {
                        req.usage.publish();
                    }
                    return Ok(out);
                }
                _ => {}
            }
        }
    }
    p.apply_patch.finish().map_err(|_| apply_patch_error())?;
    Err(status_err(408, DISCONNECTED))
}

/// Same-format passthrough when no pair is registered (Go's TranslateStream returns
/// the line).
struct Identity;

impl StreamTranslator for Identity {
    fn event(&mut self, event: &[u8]) -> Result<Vec<Bytes>, ck_translate::Error> {
        Ok(vec![Bytes::copy_from_slice(event)])
    }

    fn finish(&mut self) -> Result<Vec<Bytes>, ck_translate::Error> {
        Ok(Vec::new())
    }
}

/// The ExecuteStream goroutine: Go's line state machine (pending `event:` lines, reasoning
/// summary normalization, namespace and alias restoration, the X Search filter), every
/// resulting line translated for the client.
struct Pipeline {
    translator: Box<dyn StreamTranslator>,
    claude: ClaudeInputTokens,
    responses: bool,
    apply_patch: apply_patch::State,
    items: OutputItems,
    filter: XSearchFilter,
    restorer: NamespaceRestorer,
    alias: String,
    store: Arc<replay::Store>,
    scope: ReplayScope,
    pending: Option<Vec<u8>>,
    ready: VecDeque<Result<Bytes, ExecError>>,
    done: bool,
    usage: ck_core::exec::UsageSink,
    /// A completed or incomplete event carried usage (`StreamUsageBuffer.ok`).
    usage_seen: bool,
    capture: wire::Capture,
}

impl Pipeline {
    fn new(req: &ExecRequest, p: Prepared, store: Arc<replay::Store>, capture: wire::Capture) -> Self {
        let translator: Box<dyn StreamTranslator> = match ck_translate::pair(req.response_format, Format::Codex) {
            Some(pair) => (pair.stream)(&ResponseCtx {
                model: &req.model,
                original_request: &p.original_payload,
                translated_request: &p.body,
                tool_search: false,
            }),
            None => Box::new(Identity),
        };
        let original = Bytes::from(p.original_payload.clone());
        Self {
            translator,
            claude: ClaudeInputTokens::new(req.source_format, Format::Codex, req.response_format, original),
            responses: req.response_format == Format::OpenAIResponse,
            apply_patch: p.apply_patch,
            items: OutputItems::default(),
            filter: XSearchFilter::new(p.filter_internal_x_search, p.client_declared_tools),
            restorer: NamespaceRestorer::new(p.namespace_tools),
            alias: p.web_search_alias,
            store,
            scope: p.replay_scope,
            pending: None,
            ready: VecDeque::new(),
            done: false,
            usage: req.usage.clone(),
            usage_seen: false,
            capture,
        }
    }

    /// Ends the stream: a Responses client first gets the frame its Go framer flushes.
    fn terminal(&mut self, error: ExecError) {
        wire::publish_failure(&self.usage, &error);
        let flushed = self.translator.flush_frames();
        self.ready.extend(flushed.into_iter().filter(|f| !f.is_empty()).map(Ok));
        self.ready.push_back(Err(error));
        self.done = true;
    }

    /// `TranslateStreamWithClaudeInputTokens` for one line. `None` after a translator
    /// failure, which has ended the stream.
    fn translate(&mut self, line: &[u8]) -> Option<Vec<Bytes>> {
        let mut out = match self.translator.event(line) {
            Ok(out) => out,
            Err(e) => {
                self.terminal(status_err(502, e.to_string()));
                return None;
            }
        };
        out.retain(|f| !f.is_empty());
        if self.translator.tool_input_failed() {
            return Some(out);
        }
        if self.responses {
            for frame in out.iter_mut() {
                *frame = Bytes::from(ensure_responses_usage_details(frame));
            }
        }
        self.claude.apply(&mut out);
        Some(out)
    }

    /// `emitTranslatedLine`; false ends the stream.
    fn emit(&mut self, line: Vec<u8>) -> bool {
        let (lines, bridge_error) = self.apply_patch.stream(&line);
        let bridge_failed = bridge_error.is_some();
        let mut chunks = vec![];
        for mut line in lines {
            if let Some(rest) = line.strip_prefix(b"data:") {
                let event = trim_space(rest).to_vec();
                let kind = gj::get(&event, "type").bytes().into_owned();
                match kind.as_slice() {
                    b"response.output_item.done" => self.items.collect(&event),
                    b"response.completed" | b"response.incomplete" => {
                        // Reconstructed only after the bridge restored dispatcher children.
                        let event = response::normalize_summary_data(self.items.patch(&event));
                        if &*gj::get(&event, "type").bytes() == b"response.completed" {
                            replay::cache_completed(&self.store, &self.scope, &event);
                        }
                        let content = line.len() - line.iter().rev().take_while(|b| matches!(b, b'\r' | b'\n')).count();
                        let ending = line[content..].to_vec();
                        line = [&b"data: "[..], &event, &ending].concat();
                    }
                    _ => {}
                }
            }
            match self.translate(&line) {
                Some(out) => chunks.extend(out),
                None => return false,
            }
        }
        self.ready.extend(chunks.into_iter().map(Ok));
        // helps.StopApplyPatchStream: the line's frames, then the 502.
        if self.translator.tool_input_failed() || bridge_failed {
            self.terminal(apply_patch_error());
            return false;
        }
        true
    }

    /// One scanned upstream line; false ends the stream.
    fn line(&mut self, line: &[u8]) -> bool {
        if line.starts_with(b"event:") {
            if let Some(pending) = self.pending.take()
                && !self.emit(response::summary_event_line(&pending, ""))
            {
                return false;
            }
            self.pending = Some(line.to_vec());
            return true;
        }
        if let Some(rest) = line.strip_prefix(b"data:") {
            let events = response::normalize_summary_data_events(trim_space(rest).to_vec());
            let had_pending = self.pending.is_some();
            for (i, event) in events.into_iter().enumerate() {
                self.apply_patch.remember_dispatcher_event(&event);
                let mut event = self.restorer.restore(event);
                if !self.alias.is_empty() {
                    event = response::restore_web_search_name(event, &self.alias);
                }
                let Some(event) = self.filter.apply(event).filter(|e| !e.is_empty()) else {
                    if had_pending && i == 0 {
                        self.pending = None;
                    }
                    continue;
                };
                // ObserveResponseModel and, for terminal events, StreamUsageBuffer.Observe.
                let name = gj::get(&event, "type").str().into_owned();
                if self.usage.enabled() {
                    self.usage
                        .response_line(Format::Codex, &response::usage_line(&event, &HTTP_USAGE_EVENTS));
                    if HTTP_USAGE_EVENTS.contains(&name.as_str()) && response::codex_usage_ok(&event) {
                        self.usage_seen = true;
                    }
                }
                if had_pending {
                    let event_line = match self.pending.take() {
                        Some(pending) if i == 0 => response::summary_event_line(&pending, &name),
                        _ => format!("event: {name}").into_bytes(),
                    };
                    if !self.emit(event_line) {
                        return false;
                    }
                }
                if !self.emit([&b"data: "[..], &event].concat()) {
                    return false;
                }
            }
            return true;
        }
        if let Some(pending) = self.pending.take()
            && !self.emit(response::summary_event_line(&pending, ""))
        {
            return false;
        }
        self.emit(line.to_vec())
    }

    /// The end of the scan: a pending event line, the bridge's closing events, then the
    /// scan error or the client's final frames.
    fn end(&mut self, scan_error: Option<ExecError>) {
        if let Some(pending) = self.pending.take()
            && !self.emit(response::summary_event_line(&pending, ""))
        {
            return;
        }
        let (events, finish_error) = self.apply_patch.finish_stream();
        let failed = finish_error.is_some();
        for event in events {
            match self.translate(&event) {
                Some(out) => self.ready.extend(out.into_iter().map(Ok)),
                None => return,
            }
        }
        if failed {
            return self.terminal(apply_patch_error());
        }
        if let Some(error) = scan_error {
            self.capture.error(&error);
            return self.terminal(error);
        }
        self.done = true;
        match self.translator.finish() {
            Ok(out) => self.ready.extend(out.into_iter().filter(|f| !f.is_empty()).map(Ok)),
            Err(e) => {
                let error = status_err(502, e.to_string());
                wire::publish_failure(&self.usage, &error);
                return self.ready.push_back(Err(error));
            }
        }
        // The goroutine's deferred `streamUsage.Publish`; a failure published first wins.
        if self.usage_seen {
            self.usage.publish();
        }
    }

    fn run(self, lines: ExecStream) -> ExecStream {
        futures_util::stream::unfold((lines, self), |(mut lines, mut st)| async move {
            loop {
                if let Some(item) = st.ready.pop_front() {
                    if item.is_err() {
                        st.ready.clear();
                        st.done = true;
                    }
                    return Some((item, (lines, st)));
                }
                if st.done {
                    return None;
                }
                match fail_if_canceled(&st.usage, lines.next()).await {
                    Some(Ok(line)) => {
                        // AppendAPIResponseChunk per scanned line.
                        st.capture.chunk(&line);
                        st.line(&line);
                    }
                    Some(Err(error)) => st.end(Some(error)),
                    None => st.end(None),
                }
            }
        })
        .boxed()
    }
}

// --- token counting (xai_executor_tokens.go) ----------------------------------------------

/// `countXAIInputTokens`: O200kBase over instructions, input, function tools and the
/// text format, joined by newlines.
pub(crate) fn count_input_tokens(body: &[u8]) -> Result<i64, String> {
    let encoder = crate::tokenizer::encoder(crate::tokenizer::Encoding::O200kBase)?;
    if body.is_empty() {
        return Ok(0);
    }
    let mut segments: Vec<String> = vec![];
    let add = |segments: &mut Vec<String>, v: &gj::Res<'_>| {
        let s = String::from_utf8_lossy(trim_space(&v.bytes())).into_owned();
        if !s.is_empty() {
            segments.push(s);
        }
    };
    let add_json = |segments: &mut Vec<String>, v: &gj::Res<'_>| {
        if !v.exists() {
            return;
        }
        if v.kind == Kind::String {
            return add(segments, v);
        }
        let s = String::from_utf8_lossy(trim_space(v.raw())).into_owned();
        if !s.is_empty() {
            segments.push(s);
        }
    };
    let root = gj::parse(body);
    add(&mut segments, &root.get("instructions"));
    let input = root.get("input");
    if input.kind == Kind::String {
        add(&mut segments, &input);
    } else if input.is_array() {
        for item in input.array() {
            match &*item.get("type").bytes() {
                b"message" => {
                    let content = item.get("content");
                    if content.kind == Kind::String {
                        add(&mut segments, &content);
                        continue;
                    }
                    if !content.is_array() {
                        continue;
                    }
                    for part in content.array() {
                        match &*part.get("type").bytes() {
                            b"text" | b"input_text" | b"output_text" => add(&mut segments, &part.get("text")),
                            b"refusal" => add(&mut segments, &part.get("refusal")),
                            b"input_image" => {
                                add(&mut segments, &part.get("image_url"));
                                add(&mut segments, &part.get("file_id"));
                            }
                            b"input_file" => {
                                for key in ["file_data", "file_url", "file_id", "filename"] {
                                    add(&mut segments, &part.get(key));
                                }
                            }
                            b"input_audio" => {
                                add(&mut segments, &part.get("data"));
                                add(&mut segments, &part.get("input_audio.data"));
                            }
                            _ => {}
                        }
                    }
                }
                b"function_call" => {
                    add(&mut segments, &item.get("name"));
                    add_json(&mut segments, &item.get("arguments"));
                }
                b"function_call_output" => add_json(&mut segments, &item.get("output")),
                b"reasoning" => {
                    for part in item.get("summary").array() {
                        add(&mut segments, &part.get("text"));
                    }
                }
                _ => {}
            }
        }
    }
    let tools = root.get("tools");
    for tool in tools.is_array().then(|| tools.array()).into_iter().flatten() {
        if &*tool.get("type").bytes() != b"function" {
            continue;
        }
        add(&mut segments, &tool.get("name"));
        add(&mut segments, &tool.get("description"));
        add_json(&mut segments, &tool.get("parameters"));
    }
    let format = root.get("text.format");
    if format.exists() {
        add(&mut segments, &format.get("name"));
        add_json(&mut segments, &format.get("schema"));
    }
    if segments.is_empty() {
        return Ok(0);
    }
    Ok(encoder.encode_ordinary(&segments.join("\n")).len() as i64)
}

#[cfg(test)]
#[path = "xai_tests.rs"]
mod tests;
