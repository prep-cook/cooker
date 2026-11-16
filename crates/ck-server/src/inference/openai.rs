//! OpenAI routes: Chat Completions, legacy Completions (refused), Responses, compact and
//! input_tokens (sdk/api/handlers/openai/openai_handlers.go, openai_responses_handlers.go).

use ck_core::gostr::GoStr;
use std::sync::Arc;

use axum::Extension;
use axum::body::Bytes;
use axum::extract::rejection::BytesRejection;
use axum::extract::{MatchedPath, OriginalUri, State};
use axum::http::HeaderMap;
use axum::response::Response;
use ck_core::exec::{Caller, ExecError, Operation};
use ck_core::format::Format;
use ck_translate::codec;
use serde_json::Value;

use crate::http::egress::{Framing, Output, Protocol};
use crate::http::gojson::{self, Obj};
use crate::http::respond::{self, Writer};
use crate::inference::claude::{alt, peek, read_failed};
use crate::inference::dispatch::{self, Call, Done};
use crate::{Runtime, inference::errors};

struct Request {
    caller: Caller,
    peer: Option<std::net::SocketAddr>,
    query: String,
    headers: HeaderMap,
    path: String,
}

fn call(req: Request, entry: Format, model: String, body: Bytes, stream: bool, alt: Option<String>) -> Call {
    Call {
        entry,
        response: entry,
        operation: Operation::Generate,
        model,
        body,
        stream,
        alt,
        headers: req.headers,
        caller: req.caller,
        forced_provider: None,
        selection_model: None,
        execution_session: None,
        request_path: req.path,
        peer: req.peer,
        socket: None,
        media: None,
    }
}

/// Go `handlers.ReadRequestBody`: `Content-Encoding` is decoded (zstd, applied last
/// first; identity is a no-op) before the JSON is read. A body that fails to decode
/// but is valid JSON is used as sent; otherwise the handler answers 400. A decoded body
/// over `limit` (`server.max-request-body-bytes`) answers 413 instead.
// ponytail: decode errors carry the zstd crate's reason, not klauspost/compress's, and
// the decoded size is bounded by MAX_REQUEST_BYTES (Go reads it unbounded).
pub(crate) fn read_body(headers: &HeaderMap, raw: Bytes, limit: Option<u64>) -> Result<Bytes, Box<Response>> {
    decode_body(headers, raw, limit).map_err(|(status, reason)| {
        Box::new(respond::error_detail(
            status,
            &format!("Invalid request: {reason}"),
            "invalid_request_error",
        ))
    })
}

/// [`read_body`]'s decoding, failing with the status and Go's error text, for handlers
/// that render the failure in their own shape.
pub(crate) fn decode_body(headers: &HeaderMap, raw: Bytes, limit: Option<u64>) -> Result<Bytes, (u16, String)> {
    let encoding = headers
        .get(axum::http::header::CONTENT_ENCODING)
        .map(|v| String::from_utf8_lossy(v.as_bytes()).trim().to_owned())
        .unwrap_or_default();
    if encoding.is_empty() || encoding.eq_ignore_ascii_case("identity") {
        return Ok(raw);
    }
    let decode = || -> Result<Bytes, Option<String>> {
        let mut body = raw.clone();
        for part in encoding.split(',').rev() {
            match part.trim().to_ascii_lowercase().as_str() {
                "" | "identity" => {}
                "zstd" => body = decode_zstd(&body, limit)?,
                other => return Err(Some(format!("unsupported request content encoding: {other}"))),
            }
        }
        Ok(body)
    };
    match decode() {
        Ok(body) => Ok(body),
        // Go returns the limit error before its valid-JSON fallback.
        Err(None) => Err((413, crate::http::body_limit::MESSAGE.to_owned())),
        Err(_) if serde_json::from_slice::<serde::de::IgnoredAny>(&raw).is_ok() => Ok(raw),
        Err(Some(e)) => Err((400, e)),
    }
}

/// `Err(None)` when the decoded body passes `limit`.
fn decode_zstd(raw: &[u8], limit: Option<u64>) -> Result<Bytes, Option<String>> {
    use std::io::Read;
    let fail = |e: std::io::Error| Some(format!("failed to decode zstd request body: {e}"));
    let decoder = zstd::stream::read::Decoder::new(raw)
        .map_err(|e| Some(format!("failed to create zstd request decoder: {e}")))?;
    let cap = limit.unwrap_or(u64::MAX).min(crate::MAX_REQUEST_BYTES as u64);
    let mut out = Vec::new();
    decoder.take(cap + 1).read_to_end(&mut out).map_err(fail)?;
    if limit.is_some_and(|limit| out.len() as u64 > limit) {
        return Err(None);
    }
    if out.len() > crate::MAX_REQUEST_BYTES {
        return Err(fail(std::io::Error::other("decoded body too large")));
    }
    Ok(Bytes::from(out))
}

#[allow(clippy::too_many_arguments)]
pub async fn chat_completions(
    State(rt): State<Arc<Runtime>>,
    Extension(caller): Extension<Caller>,
    peer: dispatch::Peer,
    matched: Option<MatchedPath>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    limit: crate::http::body_limit::Limit,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let body = match body {
        Ok(body) => match read_body(&headers, body, limit.0) {
            Ok(body) => body,
            Err(response) => return *response,
        },
        Err(rejection) => return read_failed(&rejection),
    };
    let fields = peek(&body);
    if ck_wire::json::get(&body, "deferred").kind == ck_wire::json::Kind::True {
        return super::xai_deferred::create(&rt, caller, peer, matched, &uri, headers, body).await;
    }
    let stream = fields.get("stream") == Some(&Value::Bool(true));
    let model = gojson::gjson_string(fields.get("model"));
    let query = uri.query().unwrap_or_default().to_owned();
    let path = dispatch::route_path(matched.as_ref(), &uri);
    let req = Request {
        caller,
        peer: dispatch::peer(peer),
        query,
        headers,
        path,
    };
    let alt = alt(&req.query);
    let output = Output {
        protocol: Protocol::Chat,
        operation: codec::Operation::Generate,
        framing: if stream { Framing::Sse } else { Framing::Json },
    };
    dispatch::exchange::serve(
        &rt,
        call(req, Format::OpenAI, model, body, stream, alt),
        output,
        |result| async move {
            match result {
                Err(failure) => errors::openai(&failure),
                Ok(Done::Buffered { body, .. }) => respond::json(200, "application/json", body),
                Ok(Done::Stream { first, rest, .. }) => {
                    respond::sse(respond::stream(first, rest, ChatSse::default(), None))
                }
            }
        },
    )
    .await
}

/// Q8: S1 has no legacy Completions codec, so the route refuses every request visibly.
pub async fn completions() -> Response {
    errors::not_implemented("The legacy Completions API is not supported. Use /v1/chat/completions.")
}

/// Chat Completions SSE: `data:` frames, an error frame, then `data: [DONE]`.
#[derive(Default)]
struct ChatSse {
    /// A chunk reached the writer (Go's peek loop received one). Go answers a stream
    /// that closes before any with `[DONE]`: `CloseError` lives in `handleStreamResult`.
    delivered: bool,
    /// A written chunk carried a `finish_reason` (Go 97dd9eec `sawFinishReason`).
    finished: bool,
}

/// Go `chunkHasFinishReason`: any choice with a non-null, non-empty `finish_reason`.
fn chunk_has_finish_reason(chunk: &[u8]) -> bool {
    let chunk = chunk.trim_ascii();
    let chunk = chunk.strip_prefix(b"data:").map_or(chunk, <[u8]>::trim_ascii);
    ck_wire::json::get(chunk, "choices")
        .array()
        .iter()
        .any(|choice| !choice.get("finish_reason").bytes().is_empty())
}

impl Writer for ChatSse {
    fn chunk(&mut self, event: Bytes) -> Vec<Bytes> {
        self.delivered = true;
        self.finished |= chunk_has_finish_reason(&event);
        vec![respond::ensure_frame(event)]
    }

    fn error(&mut self, error: &ExecError) -> Vec<Bytes> {
        let status = crate::scheduling::classify::response_status(error);
        let body = errors::openai_stream_body(status, &crate::scheduling::classify::error_text(error));
        vec![Bytes::from(format!("data: {body}\n\n"))]
    }

    fn end(&mut self) -> Vec<Bytes> {
        // Go 97dd9eec: a stream that closes cleanly before any chunk carried a
        // finish_reason was truncated; report it instead of completing with [DONE].
        if self.delivered && !self.finished {
            let body = errors::openai_stream_body(502, "upstream stream closed before any chunk carried finish_reason");
            return vec![Bytes::from(format!("data: {body}\n\n"))];
        }
        vec![Bytes::from_static(b"data: [DONE]\n\n")]
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn responses(
    State(rt): State<Arc<Runtime>>,
    Extension(caller): Extension<Caller>,
    peer: dispatch::Peer,
    matched: Option<MatchedPath>,
    original: OriginalUri,
    headers: HeaderMap,
    limit: crate::http::body_limit::Limit,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let body = match body {
        Ok(body) => match read_body(&headers, body, limit.0) {
            Ok(body) => body,
            Err(response) => return *response,
        },
        Err(rejection) => return read_failed(&rejection),
    };
    // Go `prepareCodexMultiAgentV2Tools` then `prepareCodexOrphanDelegation`.
    let settings = ck_wire::codex_client::Settings::from_config(&rt.config());
    // The spawn_agent model list comes from the Codex client catalog.
    // Cost: one header check, then one atomic load, per request.
    if ck_wire::codex_client::multi_agent_client(&headers, settings.optimize_multi_agent_v2) {
        crate::inference::model_updater::codex_client_catalog_wanted();
    }
    let body = Bytes::from(ck_wire::codex_client::prepare_responses_request(
        &headers, &body, &settings,
    ));
    let fields = peek(&body);
    let stream = fields.get("stream") == Some(&Value::Bool(true));
    let model = gojson::gjson_string(fields.get("model"));
    let codex_client = codex_client(&headers);
    let req = Request {
        caller,
        peer: dispatch::peer(peer),
        query: String::new(),
        headers,
        path: dispatch::route_path(matched.as_ref(), &original.0),
    };
    let output = Output {
        protocol: Protocol::Responses,
        operation: codec::Operation::Generate,
        framing: if stream { Framing::Sse } else { Framing::Json },
    };
    dispatch::exchange::serve(
        &rt,
        call(req, Format::OpenAIResponse, model, body, stream, None),
        output,
        move |result| async move {
            match result {
                Err(failure) if stream => {
                    let mut failure = failure;
                    // Go sanitizes initial streaming errors unless they are direct responses.
                    if failure.direct().is_none() {
                        let status = failure.status();
                        let text = sanitize_error_text(status, &failure.text());
                        failure = dispatch::Failure::Exec(ExecError::local(
                            status,
                            ck_core::exec::FailureScope::Request,
                            text,
                        ));
                    }
                    errors::openai(&failure)
                }
                Err(failure) => errors::openai(&failure),
                Ok(Done::Buffered { body, .. }) => respond::json(200, "application/json", body),
                Ok(Done::Stream { first, mut rest, .. }) => {
                    // Go commits only once a data frame (or a terminal error) is ready.
                    let mut framer = ResponsesSse::new(codex_client);
                    let mut ready = first.map(|f| framer.chunk(f)).unwrap_or_default();
                    while framer.data_frames == 0 && framer.terminal_error.is_none() {
                        let next = futures_util::StreamExt::next(&mut rest).await;
                        if let Some(Ok(event)) = next {
                            ready.extend(framer.chunk(event));
                            continue;
                        }
                        // Go flushes the framer before deciding: a pending data frame
                        // commits the stream, which then ends with the error or the end.
                        ready.extend(framer.flush());
                        if framer.data_frames > 0 {
                            let tail = futures_util::StreamExt::boxed(futures_util::stream::iter(next));
                            let finished = framer.stopped();
                            return respond::sse(respond::resume(ready, finished, tail, framer, None));
                        }
                        match next {
                            Some(Ok(_)) => unreachable!("handled above"),
                            Some(Err(error)) => {
                                let status = crate::scheduling::classify::response_status(&error);
                                let text =
                                    sanitize_error_text(status, &crate::scheduling::classify::error_text(&error));
                                let failure = dispatch::Failure::Exec(ExecError::local(
                                    status,
                                    ck_core::exec::FailureScope::Request,
                                    text,
                                ));
                                return errors::openai(&failure);
                            }
                            None => {
                                let failure = dispatch::Failure::Exec(ExecError::local(
                                    502,
                                    ck_core::exec::FailureScope::Request,
                                    "upstream stream closed before first payload",
                                ));
                                return errors::openai(&failure);
                            }
                        }
                    }
                    let finished = framer.stopped();
                    respond::sse(respond::resume(ready, finished, rest, framer, None))
                }
            }
        },
    )
    .await
}

/// Go `isCodexResponsesClientRequest`.
fn codex_client(headers: &HeaderMap) -> bool {
    let get = |k: &str| {
        headers
            .get(k)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .trim()
            .to_owned()
    };
    let ua = get("user-agent");
    if ua.starts_with("Codex Desktop/")
        || ua.starts_with("codex-tui/")
        || ua == "codex_cli_rs"
        || ua.starts_with("codex_cli_rs/")
        || ua.starts_with("codex_exec/")
    {
        return true;
    }
    let originator = get("originator").go_lower();
    matches!(originator.as_str(), "codex desktop" | "codex-tui" | "codex_cli_rs")
        || ["codex desktop/", "codex-tui/", "codex_cli_rs/"]
            .iter()
            .any(|p| originator.starts_with(p))
}

/// Go `responsesStreamErrorText`: sensitive values redacted, long text truncated, JSON
/// errors reduced to their `error` object.
pub(crate) fn sanitize_error_text(status: u16, text: &str) -> String {
    let trimmed = gojson::trim(text);
    let trimmed = if trimmed.is_empty() {
        gojson::status_text(status)
    } else {
        trimmed
    };
    let Ok(Value::Object(root)) = serde_json::from_str::<Value>(trimmed) else {
        return truncate(&redact(trimmed), 2048);
    };
    let error = root.get("error").filter(|e| e.is_object()).or_else(|| {
        root.get("response")
            .and_then(|r| r.get("error"))
            .filter(|e| e.is_object())
    });
    if let Some(error) = error {
        let mut out = serde_json::Map::new();
        out.insert("error".into(), sanitize_node(error));
        if let Some(seq) = root.get("sequence_number") {
            out.insert("sequence_number".into(), seq.clone());
        }
        return gojson::sorted(&Value::Object(out));
    }
    gojson::sorted(&sanitize_node(&Value::Object(root)))
}

fn sensitive_key(key: &str) -> bool {
    let k = key.trim().go_lower().replace('-', "_");
    if ["tokens", "token_count", "token_limit", "token_usage"]
        .iter()
        .any(|s| k.contains(s))
    {
        return false;
    }
    matches!(
        k.as_str(),
        "authorization"
            | "secret"
            | "password"
            | "passwd"
            | "api_key"
            | "apikey"
            | "token"
            | "access_token"
            | "refresh_token"
            | "id_token"
            | "auth_token"
            | "session_token"
            | "api_token"
            | "client_secret"
            | "client_key"
    ) || ["_secret", "_password", "_api_key", "_token"]
        .iter()
        .any(|s| k.ends_with(s))
}

fn sanitize_node(v: &Value) -> Value {
    match v {
        Value::String(s) => Value::String(truncate(&redact(s), 2048)),
        Value::Object(m) => Value::Object(
            m.iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        if sensitive_key(k) {
                            "[REDACTED]".into()
                        } else {
                            sanitize_node(v)
                        },
                    )
                })
                .collect(),
        ),
        Value::Array(a) => Value::Array(a.iter().map(sanitize_node).collect()),
        other => other.clone(),
    }
}

fn redact(text: &str) -> String {
    static VALUE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(
            r#"(?i)((?:"?(?:api[_-]?key|access[_-]?token|token|authorization|secret)"?)\s*[=:]\s*"?)([^\s"&,;}]+)"#,
        )
        .unwrap()
    });
    static BEARER: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+").unwrap());
    let text = VALUE.replace_all(text, "${1}[REDACTED]");
    BEARER.replace_all(&text, "Bearer [REDACTED]").into_owned()
}

fn truncate(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        text.to_owned()
    } else {
        text.chars().take(limit).collect::<String>() + "…"
    }
}

fn stream_error_class(status: u16) -> (&'static str, &'static str) {
    match status {
        401 => ("invalid_api_key", "invalid_request_error"),
        403 => ("insufficient_quota", "invalid_request_error"),
        429 => ("rate_limit_exceeded", "invalid_request_error"),
        404 => ("model_not_found", "invalid_request_error"),
        408 => ("request_timeout", "server_error"),
        500.. => ("internal_server_error", "server_error"),
        400.. => ("invalid_request_error", "invalid_request_error"),
        _ => ("unknown_error", "invalid_request_error"),
    }
}

/// Go `openAIResponsesStreamErrorDetail`.
fn stream_error_detail(status: u16, text: &str) -> Value {
    let (mut code, kind) = stream_error_class(status);
    let mut message = gojson::trim(text).to_owned();
    if message.is_empty() {
        message = gojson::status_text(status).into();
    }
    let payload = serde_json::from_str::<Value>(gojson::trim(text))
        .ok()
        .filter(Value::is_object);
    let code_owned;
    if let Some(p) = &payload {
        if let Some(e) = p.get("error").filter(|e| e.is_object()) {
            return e.clone();
        }
        if let Some(e) = p.get("response").and_then(|r| r.get("error")).filter(|e| e.is_object()) {
            return e.clone();
        }
        if let Some(m) = p
            .get("message")
            .and_then(Value::as_str)
            .filter(|m| !m.trim().is_empty())
        {
            message = m.trim().to_owned();
        }
        if let Some(c) = p.get("code").filter(|c| !c.is_null()) {
            code_owned = gojson::trim(&gojson::gjson_string(Some(c))).to_owned();
            code = &code_owned;
        }
    }
    let mut detail = serde_json::json!({"type": kind, "code": code, "message": message, "param": null});
    if let Some(p) = &payload {
        if let Some(t) = p
            .get("type")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|t| !t.is_empty() && *t != "error")
        {
            detail["type"] = t.into();
        }
        if let Some(param) = p.get("param") {
            detail["param"] = param.clone();
        }
    }
    detail
}

/// gjson `Get(text, "sequence_number").Int()`, when present.
fn sequence_from(text: &str) -> Option<i64> {
    let seq = ck_wire::json::get(text.as_bytes(), "sequence_number");
    seq.exists().then(|| seq.int())
}

/// Go `BuildOpenAIResponsesStreamErrorChunk` / `...FailedChunk` as an SSE frame.
fn stream_error_frame(codex: bool, status: u16, text: &str, seq: i64, lead: bool) -> Bytes {
    let seq = sequence_from(text).unwrap_or(seq).max(0);
    let detail = gojson::sorted(&stream_error_detail(status, text));
    let lead = if lead { "\n" } else { "" };
    if codex {
        let response = Obj::new().str("status", "failed").raw("error", &detail).finish();
        let chunk = Obj::new()
            .str("type", "response.failed")
            .raw("sequence_number", &seq.to_string())
            .raw("response", &response)
            .finish();
        Bytes::from(format!("{lead}event: response.failed\ndata: {chunk}\n\n"))
    } else {
        let chunk = Obj::new()
            .str("type", "error")
            .raw("error", &detail)
            .raw("sequence_number", &seq.to_string())
            .finish();
        Bytes::from(format!("{lead}event: error\ndata: {chunk}\n\n"))
    }
}

/// Go `responsesSSEFramer`: join executor chunks at the HTTP boundary, after interceptors.
struct ResponsesSse {
    framer: ck_translate::stream::ResponsesFramer,
    codex: bool,
    data_frames: i64,
    last_event: String,
    terminal_event: String,
    terminal_error: Option<(u16, String)>,
    output: std::collections::BTreeMap<i64, String>,
    unindexed: Vec<String>,
}

impl ResponsesSse {
    fn new(codex: bool) -> Self {
        Self {
            framer: Default::default(),
            codex,
            data_frames: 0,
            last_event: String::new(),
            terminal_event: String::new(),
            terminal_error: None,
            output: Default::default(),
            unindexed: Vec::new(),
        }
    }

    fn private(&self, name: &str) -> bool {
        let name = name.trim();
        if name.is_empty() || matches!(name, "response.failed" | "response.error" | "error") {
            return false;
        }
        name.starts_with("responsesapi.")
            || if self.codex {
                name == "codex.rate_limits"
            } else {
                name.starts_with("codex.")
            }
    }

    /// Go `repairFrame`: drops private events, rewrites error payloads, records output
    /// items and restores them into an empty `response.completed` output.
    fn repair(&mut self, frame: Bytes) -> Option<Bytes> {
        use ck_wire::json;
        let event = respond::event_name(&frame);
        if !event.is_empty() && self.private(&event) {
            return None;
        }
        let Some(payload) = respond::data_payload(&frame).filter(|p| !p.is_empty()) else {
            return Some(frame);
        };
        if payload == b"[DONE]" {
            self.data_frames += 1;
            return Some(frame);
        }
        if !json::valid(&payload) {
            return Some(frame);
        }
        let kind = json::get(&payload, "type").str().into_owned();
        if self.private(&event) || self.private(&kind) {
            return None;
        }
        self.data_frames += 1;
        let has_error = ["error", "response.error"].iter().any(|p| {
            let r = json::get(&payload, p);
            r.exists() && r.kind != json::Kind::Null
        }) || (json::get(&payload, "code").exists() && json::get(&payload, "message").exists());
        let error_event = |t: &str| matches!(t, "response.failed" | "response.error" | "error");
        let terminal = |t: &str| {
            matches!(
                t,
                "response.completed"
                    | "response.incomplete"
                    | "response.failed"
                    | "response.done"
                    | "response.error"
                    | "error"
            )
        };
        if error_event(&kind) || has_error {
            if !kind.is_empty() {
                self.last_event = kind.clone();
            }
            return Some(self.error_payload(&payload));
        }
        let event_type = if terminal(&event) || kind.is_empty() {
            event.clone()
        } else {
            kind.clone()
        };
        if !event_type.is_empty() {
            self.last_event = event_type.clone();
        }
        if error_event(&event_type) {
            return Some(self.error_payload(&payload));
        }
        if terminal(&event_type) {
            self.terminal_event = event_type.clone();
        }
        match event_type.as_str() {
            "response.output_item.done" => {
                let item = json::get(&payload, "item");
                if item.is_object() && !item.get("type").str().is_empty() {
                    let raw = String::from_utf8_lossy(item.raw()).into_owned();
                    let index = json::get(&payload, "output_index");
                    if index.exists() {
                        self.output.insert(index.int(), raw);
                    } else {
                        self.unindexed.push(raw);
                    }
                }
            }
            "response.completed" if !self.output.is_empty() || !self.unindexed.is_empty() => {
                let output = json::get(&payload, "response.output");
                if output.exists() && (!output.is_array() || !output.array().is_empty()) {
                    return Some(frame);
                }
                let items: Vec<&str> = self
                    .output
                    .values()
                    .map(String::as_str)
                    .chain(self.unindexed.iter().map(String::as_str))
                    .collect();
                let Ok(repaired) = json::try_set_raw(&payload, "response.output", format!("[{}]", items.join(",")))
                else {
                    return Some(frame);
                };
                if repaired != payload {
                    return Some(frame_with_data(&frame, &repaired));
                }
            }
            _ => {}
        }
        Some(frame)
    }

    /// Go `repairErrorPayload`.
    fn error_payload(&mut self, payload: &[u8]) -> Bytes {
        use ck_wire::json;
        let status = [
            "status",
            "status_code",
            "error.status",
            "error.status_code",
            "response.error.status",
            "response.error.status_code",
        ]
        .iter()
        .map(|p| json::get(payload, p).int())
        .find(|s| (400..=599).contains(s))
        .unwrap_or(502) as u16;
        let text = sanitize_error_text(status, &String::from_utf8_lossy(payload));
        self.terminal_error = Some((status, text.clone()));
        self.terminal_event = if self.codex {
            "response.failed".into()
        } else {
            "error".into()
        };
        let own = json::get(payload, "sequence_number");
        let seq = if own.exists() {
            own.int()
        } else {
            sequence_from(&text).unwrap_or((self.data_frames - 1).max(0))
        };
        stream_error_frame(self.codex, status, &text, seq, false)
    }
}

/// Go `responsesSSEFrameWithData`: the frame's non-data lines, then the payload's lines
/// as `data:` lines.
fn frame_with_data(frame: &[u8], payload: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(frame.len() + payload.len());
    for line in frame.split(|&b| b == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let trimmed = line.trim_ascii();
        if trimmed.is_empty() || trimmed.starts_with(b"data:") {
            continue;
        }
        out.extend_from_slice(line);
        out.push(b'\n');
    }
    for line in payload.split(|&b| b == b'\n') {
        out.extend_from_slice(b"data: ");
        out.extend_from_slice(line);
        out.push(b'\n');
    }
    out.push(b'\n');
    Bytes::from(out)
}

impl ResponsesSse {
    /// Go `responsesSSEFramer.Flush`: the pending frame, repaired, if any.
    fn flush(&mut self) -> Vec<Bytes> {
        self.framer
            .flush()
            .into_iter()
            .filter_map(|frame| self.repair(frame))
            .collect()
    }
}

impl Writer for ResponsesSse {
    fn chunk(&mut self, event: Bytes) -> Vec<Bytes> {
        if !self.terminal_event.is_empty() {
            return Vec::new();
        }
        let event = if event.trim_ascii_start().starts_with(b"{") {
            respond::ensure_frame(event)
        } else {
            event
        };
        self.framer
            .write(&event)
            .into_iter()
            .filter_map(|frame| self.repair(frame))
            .collect()
    }

    fn stopped(&self) -> bool {
        self.terminal_error.is_some()
    }

    fn error(&mut self, error: &ExecError) -> Vec<Bytes> {
        let mut out = self.flush();
        if !self.terminal_event.is_empty() {
            return out;
        }
        let status = crate::scheduling::classify::response_status(error);
        let text = sanitize_error_text(status, &crate::scheduling::classify::error_text(error));
        out.push(stream_error_frame(self.codex, status, &text, self.data_frames, true));
        out
    }

    fn end(&mut self) -> Vec<Bytes> {
        let mut out = self.flush();
        if self.terminal_error.is_some() || !self.terminal_event.is_empty() {
            out.push(Bytes::from_static(b"\n"));
            return out;
        }
        let last = if self.last_event.is_empty() {
            "none"
        } else {
            &self.last_event
        };
        let text = format!("upstream stream closed before a terminal event (last event: {last})");
        out.push(stream_error_frame(self.codex, 502, &text, self.data_frames, true));
        out
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn compact(
    State(rt): State<Arc<Runtime>>,
    Extension(caller): Extension<Caller>,
    peer: dispatch::Peer,
    matched: Option<MatchedPath>,
    original: OriginalUri,
    headers: HeaderMap,
    limit: crate::http::body_limit::Limit,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let body = match body {
        Ok(body) => match read_body(&headers, body, limit.0) {
            Ok(body) => body,
            Err(response) => return *response,
        },
        Err(rejection) => return read_failed(&rejection),
    };
    // Go `prepareCodexOrphanDelegation` (compact skips the multi-agent tool step).
    let settings = ck_wire::codex_client::Settings::from_config(&rt.config());
    let mut body = Bytes::from(ck_wire::codex_client::rewrite_orphan_delegation_input(
        &headers,
        &body,
        settings.orphan_delegation,
    ));
    let fields = peek(&body);
    match fields.get("stream") {
        Some(Value::Bool(true)) => {
            return respond::error_detail(
                400,
                "Streaming not supported for compact responses",
                "invalid_request_error",
            );
        }
        Some(_) => {
            if let Ok(updated) = ck_wire::json::try_delete(&body, "stream") {
                body = Bytes::from(updated);
            }
        }
        None => {}
    }
    let model = gojson::gjson_string(fields.get("model"));
    let req = Request {
        caller,
        peer: dispatch::peer(peer),
        query: String::new(),
        headers,
        path: dispatch::route_path(matched.as_ref(), &original.0),
    };
    let call = call(
        req,
        Format::OpenAIResponse,
        model,
        body,
        false,
        Some("responses/compact".into()),
    );
    let output = Output {
        protocol: Protocol::Responses,
        operation: codec::Operation::Compact,
        framing: Framing::Json,
    };
    dispatch::exchange::serve(&rt, call, output, |result| async move {
        match result {
            Err(failure) => errors::openai(&failure),
            Ok(Done::Buffered { body, .. }) => respond::json(200, "application/json", body),
            Ok(Done::Stream { .. }) => unreachable!("non-stream calls are buffered"),
        }
    })
    .await
}

/// `POST /v1/responses/input_tokens`: the destination's own count of the request, with the
/// stored history a `previous_response_id` names, as one `response.input_tokens` object.
#[allow(clippy::too_many_arguments)]
pub async fn input_tokens(
    State(rt): State<Arc<Runtime>>,
    Extension(caller): Extension<Caller>,
    peer: dispatch::Peer,
    matched: Option<MatchedPath>,
    original: OriginalUri,
    headers: HeaderMap,
    limit: crate::http::body_limit::Limit,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let body = match body {
        Ok(body) => match read_body(&headers, body, limit.0) {
            Ok(body) => body,
            Err(response) => return *response,
        },
        Err(rejection) => return read_failed(&rejection),
    };
    let model = gojson::gjson_string(peek(&body).get("model"));
    let req = Request {
        caller,
        peer: dispatch::peer(peer),
        query: String::new(),
        headers,
        path: dispatch::route_path(matched.as_ref(), &original.0),
    };
    let call = Call {
        operation: Operation::CountTokens,
        ..call(req, Format::OpenAIResponse, model, body, false, None)
    };
    let output = Output {
        protocol: Protocol::Responses,
        operation: codec::Operation::CountTokens,
        framing: Framing::Json,
    };
    dispatch::exchange::serve(&rt, call, output, |result| async move {
        match result {
            Err(failure) => errors::openai(&failure),
            Ok(Done::Buffered { body, .. }) => respond::json(200, "application/json", body),
            Ok(Done::Stream { .. }) => unreachable!("non-stream calls are buffered"),
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn responses_framer_repairs_errors_and_requires_terminal_events() {
        let mut f = ResponsesSse::new(false);
        assert!(
            f.chunk(Bytes::from_static(b"event: codex.rate_limits\ndata: {}\n\n"))
                .is_empty()
        );
        let out = f.chunk(Bytes::from_static(
            b"event: response.created\ndata: {\"type\":\"response.created\"}\n\n",
        ));
        assert_eq!(out.len(), 1);
        let out = f.chunk(Bytes::from_static(
            b"data: {\"type\":\"error\",\"error\":{\"message\":\"bad\",\"code\":\"x\"},\"sequence_number\":4}\n\n",
        ));
        assert_eq!(
            String::from_utf8(out[0].to_vec()).unwrap(),
            "event: error\ndata: {\"type\":\"error\",\"error\":{\"code\":\"x\",\"message\":\"bad\"},\"sequence_number\":4}\n\n"
        );
        assert!(f.stopped());
        let mut g = ResponsesSse::new(true);
        g.chunk(Bytes::from_static(
            b"data: {\"type\":\"response.output_text.delta\"}\n\n",
        ));
        let end = String::from_utf8(g.end()[0].to_vec()).unwrap();
        assert!(end.starts_with("\nevent: response.failed\n"), "{end}");
        assert!(
            end.contains("upstream stream closed before a terminal event (last event: response.output_text.delta)")
        );
        let mut h = ResponsesSse::new(false);
        h.chunk(Bytes::from_static(
            b"data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\"}}\n\n",
        ));
        let done = h.chunk(Bytes::from_static(
            b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"output\":[]}}\n\n",
        ));
        assert_eq!(
            String::from_utf8(done[0].to_vec()).unwrap(),
            "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"output\":[{\"type\":\"message\"}]}}\n\n"
        );
        assert_eq!(h.end(), [Bytes::from_static(b"\n")]);

        // Go splices the recorded raw items with sjson: every other byte is kept.
        let mut k = ResponsesSse::new(false);
        k.chunk(Bytes::from_static(
            b"data: {\"type\":\"response.output_item.done\",\"output_index\":\"1\",\"item\":{\"type\": \"message\", \"id\":\"m\\u00e9\"}}\n\n",
        ));
        k.chunk(Bytes::from_static(
            b"data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"reasoning\"}}\n\n",
        ));
        let done = k.chunk(Bytes::from_static(
            b"event: response.completed\r\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\\u003c1\",\"usage\":{\"x\":1.50}},\"sequence_number\":7}\r\n\r\n",
        ));
        assert_eq!(
            String::from_utf8(done[0].to_vec()).unwrap(),
            "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\\u003c1\",\"usage\":{\"x\":1.50},\"output\":[{\"type\":\"reasoning\"},{\"type\": \"message\", \"id\":\"m\\u00e9\"}]},\"sequence_number\":7}\n\n"
        );
    }

    #[test]
    fn stream_error_sanitizing_redacts_and_reduces() {
        assert_eq!(
            sanitize_error_text(
                401,
                r#"{"error":{"message":"bad Bearer abc.def","api_key":"k"},"other":1}"#
            ),
            r#"{"error":{"api_key":"[REDACTED]","message":"bad Bearer [REDACTED]"}}"#
        );
        assert_eq!(sanitize_error_text(500, "token=abc rest"), "token=[REDACTED] rest");
    }
}
