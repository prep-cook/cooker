//! HTTP-only error envelopes. Streaming errors keep their protocol's event shape.
use axum::{extract::Request, http::HeaderMap, response::Response};
use serde_json::{Value, json};

use crate::inference::errors;

pub(crate) const ANTHROPIC_MAX_REQUEST_BYTES: usize = 128_000_000;

pub(crate) fn anthropic_body_path(path: &str) -> bool {
    matches!(path, "/v1/messages" | "/v1/messages/count_tokens")
}

pub(crate) fn local(request: &Request, status: u16, message: &str) -> Response {
    renderer(request.uri().path(), request.headers())(status, message)
}

pub(crate) fn renderer(path: &str, headers: &HeaderMap) -> fn(u16, &str) -> Response {
    if anthropic_body_path(path) || (path.starts_with("/v1/models") && headers.contains_key("anthropic-version")) {
        anthropic
    } else if path.starts_with("/v1beta/models") || path.starts_with("/v1beta/interactions") {
        gemini
    } else {
        |status, message| super::respond::gin_json(status, errors::openai_body(status, message))
    }
}

pub(crate) fn anthropic(status: u16, text: &str) -> Response {
    anthropic_body(status, errors::claude_body(status, text))
}

pub(crate) fn anthropic_body(status: u16, body: String) -> Response {
    let id = crate::observability::api::current_request_id().unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    // claude_body also serves SSE, where request_id is not part of the error event.
    let mut body = body.into_bytes();
    body.pop();
    body.extend_from_slice(b",\"request_id\":");
    serde_json::to_writer(&mut body, &id).expect("writing JSON into a Vec cannot fail");
    body.push(b'}');
    let mut response = super::respond::json(status, "application/json", body);
    response
        .headers_mut()
        .insert("request-id", id.parse().expect("local request ID is an HTTP header"));
    response
}

pub(crate) fn gemini(status: u16, text: &str) -> Response {
    super::respond::json(status, "application/json", gemini_body(status, text))
}

pub(crate) fn gemini_body(status: u16, text: &str) -> String {
    let parsed = serde_json::from_str::<Value>(text).ok();
    let error = parsed.as_ref().and_then(|v| v.get("error"));
    if let Some(error) = error
        && error.get("code").and_then(Value::as_u64) == Some(u64::from(status))
        && error.get("message").is_some_and(Value::is_string)
        && error.get("status").is_some_and(Value::is_string)
    {
        return json!({"error": error}).to_string();
    }
    let message = error
        .and_then(|v| v.get("message"))
        .and_then(Value::as_str)
        .unwrap_or(text);
    let kind = match status {
        400 | 422 => "INVALID_ARGUMENT",
        401 => "UNAUTHENTICATED",
        402 | 413 | 429 => "RESOURCE_EXHAUSTED",
        403 => "PERMISSION_DENIED",
        404 | 410 => "NOT_FOUND",
        409 => "ABORTED",
        408 | 504 => "DEADLINE_EXCEEDED",
        499 => "CANCELLED",
        501 => "UNIMPLEMENTED",
        502 | 503 | 529 => "UNAVAILABLE",
        _ => "INTERNAL",
    };
    json!({"error": {"code": status, "message": message, "status": kind}}).to_string()
}
