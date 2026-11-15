//! OpenAI-compatible upstreams (internal/runtime/executor/openai_compat_executor.go):
//! configured providers reached with a bearer API key at `base_url`, speaking Chat
//! Completions, Responses compact and the Images API.

use std::collections::VecDeque;
use std::time::SystemTime;

use bytes::Bytes;
use ck_core::config::Config;
use ck_core::credential::Credential;
use ck_core::exec::{ExecError, ExecRequest, ExecResponse, ExecStream, FailureScope, Operation, ResponseBody};
use ck_core::format::Format;
use ck_translate::{RequestCtx, ResponseCtx, StreamTranslator};
use futures_util::StreamExt;

use ck_wire::json as gj;
use ck_wire::thinking::{self, ModelCaps, RequestThinking, parse_suffix};

use crate::gemini_stream::ClaudeInputTokens;
use crate::openai_compat_go as go;
use crate::openai_compat_headers::base_headers;
use crate::openai_compat_http::{self as wire, Clients, GoHeaders};
use crate::openai_compat_multipart as multipart;
use crate::openai_compat_payload as payload;

#[path = "openai_compat_chat.rs"]
mod chat;
#[path = "openai_compat_stream.rs"]
mod stream;
pub(crate) use chat::chat_with;
use stream::frames;

pub const USER_AGENT: &str = "cooker";
const COMPACT_ALT: &str = "responses/compact";
const IMAGES_GENERATIONS: &str = "/images/generations";
const IMAGES_EDITS: &str = "/images/edits";
/// `scanner.Buffer(nil, 52_428_800)`.
pub(crate) const MAX_LINE: usize = 52_428_800;
/// helps.ApplyPatchUpstreamErrorMessage: Go's error for an empty or rejected translation.
const EMPTY_TRANSLATION: &str = ck_translate::APPLY_PATCH_UPSTREAM_ERROR;

/// Providers this executor serves: `openai-compatibility` and `openai-compatible-<name>`
/// (util.OpenAICompatibleProviderKey).
pub fn handles(provider: &str) -> bool {
    provider == "openai-compatibility" || provider.starts_with("openai-compatible-")
}

#[derive(Default)]
pub struct OpenAICompatExecutor {
    clients: Clients,
}

/// Go's plain `statusErr`: never credential-scoped, so a 429 cools only the model
/// (`IsCredentialScoped` is false); other failures by status.
pub(crate) fn scope_for(status: u16) -> FailureScope {
    match status {
        429 => FailureScope::Model,
        401 | 402 | 403 | 408 | 500.. => FailureScope::Credential,
        _ => FailureScope::Request,
    }
}

/// A plain Go error (no status code): no status, scope or retry semantics, which the
/// server models as a transport fault; Go's handler answers 500 with its text.
pub(crate) fn plain_err(message: impl Into<String>) -> ExecError {
    ExecError::local(500, FailureScope::Transport, message)
}

/// `statusErr{code, msg}`: an empty message reads `status N`.
pub(crate) fn status_err(status: u16, message: impl Into<String>) -> ExecError {
    let message = message.into();
    let message = if message.is_empty() {
        format!("status {status}")
    } else {
        message
    };
    ExecError::local(status, scope_for(status), message)
}

/// A chat non-2xx response: `newOpenAICompatStatusError` over the error body, which is
/// captured like Go's `AppendAPIResponseChunk` (read errors ignored, `b, _ :=
/// io.ReadAll`).
async fn upstream_error(upstream: wire::Upstream, capture: &wire::Capture) -> ExecError {
    let (status, headers, body) = wire::read_error_body(upstream).await;
    capture.chunk(&body);
    status_error(status, headers, body, true)
}

/// An images non-2xx response: the whole body is read first and a read error is
/// captured and returned instead, then `newOpenAICompatStatusError`, or the plain
/// `statusErr` the image stream path uses.
async fn images_error(upstream: wire::Upstream, with_retry: bool, capture: &wire::Capture) -> ExecError {
    let (status, headers) = (upstream.status, upstream.headers.clone());
    let body = wire::read_all(upstream).await;
    capture.read(&body);
    match body {
        Ok(body) => status_error(status, headers, body, with_retry),
        Err(error) => error,
    }
}

/// `newOpenAICompatStatusError`: the upstream's status and its body as received, an empty or
/// non-UTF-8 one included; the retry hint is read from the headers and body, never rewritten.
pub(crate) fn status_error(status: u16, headers: http::HeaderMap, body: Bytes, with_retry: bool) -> ExecError {
    let retry_after = with_retry
        .then(|| payload::retry_after(status, &headers, &body, SystemTime::now()))
        .flatten();
    ExecError {
        status,
        scope: scope_for(status),
        body,
        headers: Box::new(headers),
        retry_after,
        direct: false,
        addon: None,
    }
}

pub(crate) fn credentials(credential: &Credential) -> (String, String) {
    let attr = |k: &str| {
        credential
            .attributes
            .get(k)
            .map(|v| v.trim().to_owned())
            .unwrap_or_default()
    };
    (attr("base_url"), attr("api_key"))
}

pub(crate) fn missing_base_url() -> ExecError {
    ExecError::local(401, FailureScope::Credential, "missing provider baseURL")
}

/// `strings.TrimSuffix(baseURL, "/") + path`: one slash only, like Go.
pub(crate) fn endpoint(base_url: &str, path: &str) -> String {
    format!("{}{path}", base_url.strip_suffix('/').unwrap_or(base_url))
}

fn not_registered(what: &str, from: Format, to: Format) -> ExecError {
    ExecError::local(
        501,
        FailureScope::Request,
        format!(
            "{what} translation {} -> {} is not registered",
            from.as_str(),
            to.as_str()
        ),
    )
}

/// `helps.TranslateRequestWithAPIKeyModelCompatibilityAndUpdateIntent` for this executor
/// (no target executor): Codex-client rewrites, then the pair or its compat variant.
// ponytail: the configuration-update intent crate::codex_client reports is not read
// here yet, so thinking always sees `updates_changed: false`, and equal original and
// working payloads are translated twice (Go translates once); the OpenAI-compatible
// port reads it.
fn translate_body(
    req: &ExecRequest,
    cfg: &Config,
    target: Format,
    model: &str,
    stream: bool,
    body: &[u8],
    is_compat: bool,
) -> Result<Vec<u8>, ExecError> {
    let client = crate::codex_client::Client::new(&req.headers, cfg, "", is_compat);
    crate::codex_client::translate_request(
        req.source_format,
        target,
        &RequestCtx {
            model,
            stream,
            tool_search: false,
        },
        body,
        &client,
    )
    .map_err(|e| ExecError::local(400, FailureScope::Request, e.to_string()))
}

/// `opts.OriginalRequest`, else the payload.
fn original_payload(req: &ExecRequest) -> &[u8] {
    if req.original_body.is_empty() {
        &req.body
    } else {
        &req.original_body
    }
}

/// `helps.ApplyPayloadConfigWithRequest` (no target executor) on the translated body;
/// `original` is the translated original request.
fn apply_payload_rules(
    cfg: &Config,
    req: &ExecRequest,
    model: &str,
    target: Format,
    original: &[u8],
    body: Vec<u8>,
) -> Vec<u8> {
    let rules = ck_wire::payload::Rules::from_config(cfg);
    ck_wire::payload::apply(
        &rules,
        &ck_wire::payload::Request {
            target_executor: "",
            model,
            requested_model: route_model(req),
            protocol: target.as_str(),
            from_protocol: req.source_format.as_str(),
            root: "",
            original,
            request_path: &req.request_path,
            headers: Some(&req.headers),
        },
        body,
    )
}

/// `helps.ApplyRequestThinking` with the capabilities bound to this attempt.
fn apply_thinking(
    body: Vec<u8>,
    req: &ExecRequest,
    target: Format,
    provider: &str,
    resolved: Option<&ModelCaps>,
) -> Result<Vec<u8>, ExecError> {
    thinking::apply_request_thinking(
        &RequestThinking {
            payload: Some(&req.body[..]),
            original: Some(&req.original_body[..]),
            model: &req.model,
            from: req.source_format.as_str(),
            to: target.as_str(),
            provider,
            resolved: resolved.map(Some),
            has_request_transformer: ck_translate::pair(req.source_format, target).is_some(),
            updates_changed: false,
        },
        body,
    )
    .map_err(|e| ExecError::local(400, FailureScope::Request, e.message))
}

/// `util.ApplyCustomHeadersFromAttrs` with the client headers and the canonical session.
fn apply_custom(headers: &mut GoHeaders, credential: &Credential, req: &ExecRequest) {
    let session = ck_wire::session::cpa_session_id(req.session.as_deref());
    for (name, value) in ck_wire::headers::custom_headers(&credential.attributes, &req.headers, session.as_deref()) {
        headers.set(&name, value);
    }
}

impl OpenAICompatExecutor {
    /// Uses `client` for credentials without a proxy (tests pass a plain client).
    pub fn with_client(client: wreq::Client) -> Self {
        Self {
            clients: Clients::new(client),
        }
    }

    /// The HTTP client of `credential`'s effective proxy.
    pub(crate) fn client(&self, credential: &Credential, cfg: &Config) -> wreq::Client {
        self.clients.for_credential(credential, cfg)
    }

    pub async fn execute(
        &self,
        credential: &Credential,
        req: ExecRequest,
        cfg: &Config,
    ) -> Result<ExecResponse, ExecError> {
        match req.operation {
            Operation::CountTokens => count_tokens(credential, &req, cfg),
            Operation::Generate => self.chat(credential, req, cfg).await,
        }
    }

    /// Execute (`stream` false) and ExecuteStream (`stream` true) for chat and compact.
    async fn chat(&self, credential: &Credential, req: ExecRequest, cfg: &Config) -> Result<ExecResponse, ExecError> {
        let (base_url, api_key) = credentials(credential);
        let compat = payload::resolve_compat(credential, cfg);
        let client = self.clients.for_credential(credential, cfg);
        chat_with(
            &client,
            credential,
            req,
            cfg,
            (&base_url, &api_key, true),
            compat.as_ref(),
        )
        .await
    }

    /// `executeImages` / `executeImagesStream`: the body (JSON or multipart) goes to
    /// `/images/edits` or `/images/generations` with the model replaced. `request_path` is
    /// the inbound route; `req.stream` selects raw SSE passthrough.
    pub async fn images(
        &self,
        credential: &Credential,
        req: ExecRequest,
        request_path: &str,
        cfg: &Config,
    ) -> Result<ExecResponse, ExecError> {
        let base_model = parse_suffix(&req.model).model_name.trim().to_owned();
        let (base_url, api_key) = credentials(credential);
        if base_url.is_empty() {
            return Err(missing_base_url());
        }
        let client_content_type = req
            .headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .trim()
            .to_owned();
        let (body, content_type) = prepare_images_payload(&req.body, &base_model, &client_content_type, req.stream)?;
        let content_type = if content_type.is_empty() {
            "application/json".to_owned()
        } else {
            content_type
        };
        let (body, content_type) = media_barrier(
            credential,
            &req,
            cfg,
            &base_model,
            &body,
            &content_type,
            &client_content_type,
        )?;
        let path = if request_path.trim().ends_with(IMAGES_EDITS) {
            IMAGES_EDITS
        } else {
            IMAGES_GENERATIONS
        };
        // SetTranslatedReasoningEffort(payload, "openai").
        if req.usage.enabled() {
            req.usage.request(Format::OpenAI, &body);
        }
        let mut headers = base_headers(&api_key, &content_type, true);
        if req.stream {
            headers.set("Accept", "text/event-stream");
            headers.set("Cache-Control", "no-cache");
        }
        apply_custom(&mut headers, credential, &req);
        headers.set("User-Agent", USER_AGENT);
        let client = self.clients.for_credential(credential, cfg);
        let url = endpoint(&base_url, path);
        let capture = wire::Capture::request(
            &req,
            credential,
            &credential.provider,
            &url,
            "POST",
            headers.pairs(),
            &body,
        );
        let upstream = wire::send(&client, &url, headers, body).await;
        capture.sent(&upstream);
        let upstream = upstream?;
        if !(200..300).contains(&upstream.status) {
            return Err(images_error(upstream, !req.stream, &capture).await);
        }
        let headers = upstream.headers.clone();
        if req.stream {
            // Each raw read is logged; a read error is logged and published as a failure
            // before the client gets it.
            let usage = req.usage.clone();
            let stream = capture
                .stream(upstream.body)
                .inspect(move |item| {
                    if let Err(error) = item {
                        wire::publish_failure(&usage, error);
                    }
                })
                .boxed();
            return Ok(ExecResponse {
                status: 200,
                headers,
                body: ResponseBody::Stream(stream),
            });
        }
        let data = wire::read_all(upstream).await;
        capture.read(&data);
        let data = data?;
        if req.usage.enabled() {
            req.usage.response_body(Format::OpenAI, &data);
        }
        Ok(ExecResponse {
            status: 200,
            headers,
            body: ResponseBody::Buffered(data),
        })
    }
}

/// `ApplyMediaPayloadConfig` on a prepared images request (868e137): the payload rules
/// run last, on the body's JSON view, against the client's original request.
fn media_barrier(
    credential: &Credential,
    req: &ExecRequest,
    cfg: &Config,
    model: &str,
    body: &[u8],
    content_type: &str,
    client_content_type: &str,
) -> Result<(Bytes, String), ExecError> {
    let original = if req.original_body.is_empty() {
        &req.body
    } else {
        &req.original_body
    };
    let (body, content_type) =
        crate::payload_media::apply(body, content_type, original, client_content_type, |view, original| {
            crate::payload_barrier::Barrier {
                executor: &credential.provider,
                model,
                protocol: Format::OpenAI.as_str(),
                root: "",
                original,
            }
            .apply(cfg, req, view)
        })
        .map_err(plain_err)?;
    Ok((Bytes::from(body), content_type))
}

/// `prepareOpenAICompatImagesPayload`.
pub(crate) fn prepare_images_payload(
    body: &[u8],
    model: &str,
    content_type: &str,
    stream: bool,
) -> Result<(Bytes, String), ExecError> {
    if go::json_valid(body) {
        let mut body = body.to_vec();
        if !model.is_empty() {
            payload::set_str_if_different(&mut body, "model", model);
        }
        if stream {
            payload::set_bool_if_different(&mut body, "stream", true);
        } else {
            gj::delete(&mut body, "stream");
        }
        return Ok((Bytes::from(body), "application/json".into()));
    }
    let Some(boundary) = multipart::boundary(content_type) else {
        return Ok((Bytes::copy_from_slice(body), content_type.to_owned()));
    };
    // Go returns a plain error here; the handler answers 500.
    let boundary = boundary.map_err(plain_err)?;
    let form =
        multipart::read_form(body, &boundary).map_err(|e| plain_err(format!("read multipart form failed: {e}")))?;
    let (out, content_type) = multipart::rewrite_images_form(&form, model, stream, false);
    Ok((Bytes::from(out), content_type))
}

/// `helps.PayloadRequestedModel`: the client's model, else the execution model.
fn route_model(req: &ExecRequest) -> &str {
    if req.requested_model.trim().is_empty() {
        req.model.trim()
    } else {
        req.requested_model.trim()
    }
}

/// `applyPromptCacheKey`.
fn prompt_cache_key(
    compat: Option<&payload::Compat>,
    provider: &str,
    req: &ExecRequest,
    base_model: &str,
    mut body: Vec<u8>,
) -> Vec<u8> {
    if !compat.is_some_and(|c| c.support_prompt_cache_key) {
        return body;
    }
    let explicit = [&req.body[..], &req.original_body[..], &body[..]]
        .into_iter()
        .map(|source| gj::get(source, "prompt_cache_key").str().trim().to_owned())
        .find(|key| !key.is_empty());
    if let Some(key) = explicit {
        payload::set_str_if_different(&mut body, "prompt_cache_key", &key);
        return body;
    }
    let model = gj::get(&body, "model").str().trim().to_owned();
    let model = if model.is_empty() { base_model.to_owned() } else { model };
    if req.source_format == Format::Claude
        && let Some(key) = payload::claude_code_prompt_cache(&model, &req.body, &req.headers)
    {
        payload::set_str_if_different(&mut body, "prompt_cache_key", &key);
        return body;
    }
    let sessions = (req.execution_session.as_deref(), req.derived_session.as_deref());
    if let Some(key) =
        crate::openai_compat_exec::session_cache_key(provider, &model, req.source_format.as_str(), sessions)
    {
        payload::set_str_if_different(&mut body, "prompt_cache_key", &key);
    }
    body
}

/// `CountTokens`: a local tiktoken estimate of the translated chat request.
pub(crate) fn count_tokens(
    credential: &Credential,
    req: &ExecRequest,
    cfg: &Config,
) -> Result<ExecResponse, ExecError> {
    let base_model = parse_suffix(&req.model).model_name;
    let caps = req.resolved_model.as_ref().map(|r| ModelCaps::from(&r.info));
    let is_compat = req.resolved_model.as_ref().is_some_and(|r| r.is_compat());
    let body = translate_body(req, cfg, Format::OpenAI, &base_model, false, &req.body, is_compat)?;
    // The rule baseline: the translated original, else the translated body before
    // thinking. Go counts with a failed translation's partial body; this refuses.
    let original = if req.original_body.is_empty() {
        body.clone()
    } else {
        translate_body(
            req,
            cfg,
            Format::OpenAI,
            &base_model,
            false,
            &req.original_body,
            is_compat,
        )?
    };
    let body = apply_thinking(body, req, Format::OpenAI, &credential.provider, caps.as_ref())?;
    let body = crate::payload_barrier::Barrier {
        executor: &credential.provider,
        model: &base_model,
        protocol: Format::OpenAI.as_str(),
        root: "",
        original: &original,
    }
    .apply(cfg, req, body);
    let count = payload::count_chat_tokens(&base_model, &body).map_err(|e| {
        ExecError::local(
            500,
            FailureScope::Request,
            format!("openai compat executor: token counting failed: {e}"),
        )
    })?;
    let usage = format!(r#"{{"usage":{{"prompt_tokens":{count},"completion_tokens":0,"total_tokens":{count}}}}}"#);
    let out = ck_translate::translate_token_count(req.response_format, Format::OpenAI, count, usage.as_bytes());
    Ok(ExecResponse {
        status: 200,
        headers: Default::default(),
        body: ResponseBody::Buffered(Bytes::from(out)),
    })
}

/// Same-format passthrough when no pair is registered: one `data:` event per frame.
struct Identity;

impl StreamTranslator for Identity {
    fn event(&mut self, event: &[u8]) -> Result<Vec<Bytes>, ck_translate::Error> {
        Ok(if event.starts_with(b"data: [DONE]") {
            Vec::new()
        } else {
            vec![Bytes::from([event, b"\n\n"].concat())]
        })
    }

    fn finish(&mut self) -> Result<Vec<Bytes>, ck_translate::Error> {
        Ok(Vec::new())
    }
}

fn trim(bytes: &[u8]) -> &[u8] {
    go::trim_space(bytes)
}

#[cfg(test)]
#[path = "openai_compat_tests.rs"]
mod tests;
