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
use crate::http::gojson;
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
                // A Responses exchange skips every candidate only the old arm serves
                // (`dispatch::attempt::run`), so an answer is always a committed exchange.
                Ok(_) => unreachable!("a Responses request never takes the old arm"),
            }
        },
    )
    .await
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
            Ok(_) => unreachable!("a Responses request never takes the old arm"),
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
