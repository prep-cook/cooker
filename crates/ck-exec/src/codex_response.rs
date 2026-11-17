//! Codex responses: Go's status and terminal-event error classification
//! (codex_executor_terminal.go, codex_websockets_errors.go), per-event SSE processing,
//! bootstrap buffering, and buffered (non-streaming) assembly.

use ck_core::gostr::GoStr;
use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use ck_core::exec::{ExecError, ExecStream, FailureScope};
use futures_util::StreamExt;
use gjson::Kind;
use http::HeaderMap;

use crate::codex_json::{set_raw, set_str};

mod processor;
mod transport;
pub(crate) use processor::{Gate, OutputItems, Processor, SCANNER_MAX_LINE, Step, restore};
pub(crate) use transport::{Bootstrap, bootstrap, processed};

pub(crate) const INCOMPLETE_MESSAGE: &str =
    "stream error: stream disconnected before completion: stream closed before response.completed";
/// `codexBootstrapMaxBufferedFrames` / `codexBootstrapMaxBufferedBytes`.
pub(crate) const BOOTSTRAP_MAX_FRAMES: usize = 48;
pub(crate) const BOOTSTRAP_MAX_BYTES: usize = 1 << 20;

fn lower(v: &gjson::Value<'_>) -> String {
    v.str().trim().to_ascii_lowercase()
}

fn text(body: &str, path: &str) -> String {
    gjson::get(body, path).str().to_owned()
}

/// `isCodexUsageLimitError`.
pub(crate) fn is_usage_limit(body: &str) -> bool {
    ["error.type", "type"].iter().any(|p| {
        gjson::get(body, p)
            .str()
            .trim()
            .eq_ignore_ascii_case("usage_limit_reached")
    })
}

/// `isCodexModelCapacityError`.
pub(crate) fn is_capacity(body: &str) -> bool {
    [text(body, "error.message"), text(body, "message"), body.to_owned()]
        .iter()
        .map(|c| c.trim().to_ascii_lowercase())
        .any(|l| {
            !l.is_empty()
                && (l.contains("model is at capacity")
                    || l.contains("model_at_capacity")
                    || l.contains("model_is_at_capacity")
                    || (l.contains("model") && l.contains("at capacity")))
        })
}

/// `codexStatusErrorClassification`.
pub(crate) fn classification(status: u16, body: &str) -> Option<(&'static str, &'static str)> {
    let mut message = lower(&gjson::get(body, "error.message"));
    if message.is_empty() {
        message = lower(&gjson::get(body, "message"));
    }
    let all = body.trim().to_ascii_lowercase();
    let code = lower(&gjson::get(body, "error.code"));
    let kind = lower(&gjson::get(body, "error.type"));
    let invalid = kind.is_empty() || kind == "invalid_request_error";
    if status == 413
        || code == "context_length_exceeded"
        || code == "context_too_large"
        || (invalid
            && ["context length", "context_length", "maximum context", "too many tokens"]
                .iter()
                .any(|s| message.contains(s)))
    {
        return Some(("context_too_large", "invalid_request_error"));
    }
    if all.contains("invalid signature in thinking block") || all.contains("invalid_encrypted_content") {
        return Some(("thinking_signature_invalid", "invalid_request_error"));
    }
    if code == "previous_response_not_found"
        || all.contains("previous_response_not_found")
        || (all.contains("previous_response_id") && all.contains("not found"))
    {
        return Some(("previous_response_not_found", "invalid_request_error"));
    }
    if status == 401
        || kind == "authentication_error"
        || code == "invalid_api_key"
        || all.contains("invalid or expired token")
        || all.contains("refresh_token_reused")
    {
        return Some(("auth_unavailable", "authentication_error"));
    }
    None
}

/// `classifyCodexStatusError`: rewrites recognised failures into an OpenAI error object.
fn classify(status: u16, body: &str) -> String {
    let Some((code, kind)) = classification(status, body) else {
        return body.to_owned();
    };
    let mut message = text(body, "error.message");
    if message.is_empty() {
        message = text(body, "message");
    }
    if message.is_empty() {
        message = body.trim().to_owned();
    }
    if message.is_empty() {
        message = http::StatusCode::from_u16(status)
            .ok()
            .and_then(|s| s.canonical_reason())
            .unwrap_or_default()
            .to_owned();
    }
    let out = set_str(r#"{"error":{}}"#, "error.message", &message);
    let out = set_str(&out, "error.type", kind);
    set_str(&out, "error.code", code)
}

/// `parseCodexRetryAfter`: only quota exhaustion carries a reset hint.
fn usage_retry_after(status: u16, body: &str, now: SystemTime) -> Option<Duration> {
    if status != 429 || body.is_empty() {
        return None;
    }
    let root = gjson::parse(body);
    for quota in [root.get("error"), gjson::parse(body)] {
        if !quota
            .get("type")
            .str()
            .trim()
            .eq_ignore_ascii_case("usage_limit_reached")
        {
            continue;
        }
        let at = quota.get("resets_at").i64();
        if at > 0 {
            let reset = UNIX_EPOCH + Duration::from_secs(at as u64);
            if let Ok(wait) = reset.duration_since(now)
                && !wait.is_zero()
            {
                return Some(wait);
            }
        }
        let secs = quota.get("resets_in_seconds").i64();
        if secs > 0 {
            return Some(Duration::from_secs(secs as u64));
        }
    }
    None
}

/// `clienterror.IsRequestFault`: the caller's request is wrong; no credential can help.
pub(crate) fn request_fault(status: u16, body: &str) -> bool {
    if status == 402 || status == 429 {
        return false;
    }
    let field = |paths: &[&str]| {
        paths
            .iter()
            .map(|p| gjson::get(body, p).str().trim().to_ascii_lowercase())
            .collect::<Vec<_>>()
    };
    let valid = gjson::valid(body.trim());
    let types = if valid {
        field(&["error.type", "type", "response.error.type", "body.error.type"])
    } else {
        Vec::new()
    };
    let codes = if valid {
        field(&["error.code", "code", "response.error.code", "body.error.code"])
    } else {
        Vec::new()
    };
    if status == 401 && types.iter().any(|t| t == "authentication_error") {
        return false;
    }
    if codes
        .iter()
        .any(|c| c == "model_not_found" || c == "model_not_found_error")
    {
        return false;
    }
    const CODES: [&str; 9] = [
        "cyber_policy",
        "context_length_exceeded",
        "message_too_big",
        "string_above_max_length",
        "invalid_prompt",
        "invalid_value",
        "unsupported_value",
        "invalid_request_error",
        "previous_response_not_found",
    ];
    const TYPES: [&str; 4] = [
        "invalid_request",
        "invalid_request_error",
        "bad_request_error",
        "invalid_prompt",
    ];
    if codes.iter().any(|c| CODES.contains(&c.as_str())) || types.iter().any(|t| TYPES.contains(&t.as_str())) {
        return true;
    }
    let l = body.to_ascii_lowercase();
    if l.contains("item with id")
        && l.contains("not found")
        && l.contains("items are not persisted when `store` is set to false")
    {
        return true;
    }
    matches!(status, 400 | 409 | 413 | 422)
}

/// `newCodexStatusErrWithCooling` as an [`ExecError`]: usage-limit exhaustion is
/// credential-wide unless model-level cooling is on; capacity and usage limits are 429.
pub(crate) fn status_error(status: u16, body: &[u8], headers: HeaderMap, model_level_cooling: bool) -> ExecError {
    let raw = String::from_utf8_lossy(body);
    let code = if is_usage_limit(&raw) || is_capacity(&raw) {
        429
    } else {
        status
    };
    let classified = classify(code, &raw);
    status_error_raw(code, &classified, headers, model_level_cooling)
}

/// A plain `statusErr{code, msg}` with Go's retry hint and credential scope.
pub(crate) fn status_error_raw(code: u16, message: &str, headers: HeaderMap, model_level_cooling: bool) -> ExecError {
    let retry_after = usage_retry_after(code, message, SystemTime::now());
    let scope = if is_usage_limit(message) && !model_level_cooling {
        FailureScope::Credential
    } else if request_fault(code, message) {
        FailureScope::Request
    } else {
        match code {
            429 | 404 => FailureScope::Model,
            _ => FailureScope::Credential,
        }
    };
    let message = if message.is_empty() {
        format!("status {code}")
    } else {
        message.to_owned()
    };
    ExecError {
        status: code,
        scope,
        body: Bytes::from(message),
        headers: Box::new(headers),
        retry_after,
        direct: false,
        addon: None,
        upstream: true,
    }
}

/// Request-scoped stream failures (`codexIncompleteStreamError` and friends).
pub(crate) fn request_scoped(status: u16, message: &str) -> ExecError {
    ExecError::local(status, FailureScope::Request, message)
}

fn error_body(event: &str, path: &str) -> Option<String> {
    let found = gjson::get(event, path);
    if !found.exists() {
        return None;
    }
    let mut body = String::from(r#"{"error":{}}"#);
    if matches!(found.kind(), Kind::Object | Kind::Array) {
        body = set_raw(&body, "error", found.json());
    } else if !found.str().trim().is_empty() {
        body = set_str(&body, "error.message", found.str().trim());
    }
    let missing = |b: &str| gjson::get(b, "error.message").str().trim().is_empty();
    if missing(&body) {
        let m = text(event, "response.error.message");
        if !m.trim().is_empty() {
            body = set_str(&body, "error.message", m.trim());
        }
    }
    for fallback in ["error.code", "error.type"] {
        if missing(&body) {
            let v = text(&body, fallback);
            if !v.trim().is_empty() {
                body = set_str(&body, "error.message", v.trim());
            }
        }
    }
    Some(body)
}

fn top_level_error_body(event: &str) -> Option<String> {
    let get = |p: &str| text(event, p).trim().to_owned();
    let (message, code, kind, param) = (get("message"), get("code"), get("error_type"), get("param"));
    if message.is_empty() && code.is_empty() && kind.is_empty() && param.is_empty() {
        return None;
    }
    let mut body = String::from(r#"{"error":{}}"#);
    for (key, value) in [
        ("error.message", &message),
        ("error.code", &code),
        ("error.type", &kind),
        ("error.param", &param),
    ] {
        if !value.is_empty() {
            body = set_str(&body, key, value);
        }
    }
    if message.is_empty() {
        if !code.is_empty() {
            body = set_str(&body, "error.message", &code);
        } else if !kind.is_empty() {
            body = set_str(&body, "error.message", &kind);
        }
    }
    Some(body)
}

/// `codexTerminalFailureBody`: the error object of an `error` or `response.failed` event.
pub(crate) fn terminal_failure_body(event: &str) -> Option<String> {
    let body = match gjson::get(event, "type").str() {
        "error" => error_body(event, "error").or_else(|| top_level_error_body(event)),
        "response.failed" => error_body(event, "response.error").or_else(|| error_body(event, "error")),
        _ => return None,
    };
    let mut body = body
        .filter(|b| !b.is_empty())
        .unwrap_or_else(|| r#"{"error":{"message":"upstream stream failed without error details"}}"#.into());
    let seq = gjson::get(event, "sequence_number");
    if seq.exists() {
        body = set_raw(&body, "sequence_number", &seq.i64().to_string());
    }
    Some(body)
}

fn context_length(body: &str) -> bool {
    let code = lower(&gjson::get(body, "error.code"));
    let message = lower(&gjson::get(body, "error.message"));
    code == "context_length_exceeded"
        || code == "context_too_large"
        || ["context window", "context length", "too many tokens"]
            .iter()
            .any(|s| message.contains(s))
}

/// `codexTerminalFailureStatus`.
fn terminal_status(body: &str) -> u16 {
    for path in ["error.status_code", "error.status"] {
        let status = gjson::get(body, path).i64();
        if (400..=599).contains(&status) {
            return status as u16;
        }
    }
    let kind = lower(&gjson::get(body, "error.type"));
    let code = lower(&gjson::get(body, "error.code"));
    match () {
        _ if code == "cyber_policy" => 400,
        _ if kind == "not_found_error" || code == "not_found" || code == "model_not_found" => 404,
        _ if kind == "authentication_error" || code == "invalid_api_key" || code == "unauthorized" => 401,
        _ if kind == "permission_error" || code == "forbidden" || code == "permission_denied" => 403,
        _ if kind == "rate_limit_error" || code == "rate_limit_exceeded" => 429,
        _ if kind == "invalid_request_error" || kind == "bad_request_error" => 400,
        _ => 502,
    }
}

/// `codexTerminalFailureErrWithCooling`: an in-stream failure event as an error, with the
/// body it was built from.
pub(crate) fn terminal_failure(event: &str, model_level_cooling: bool) -> Option<(ExecError, String)> {
    let body = terminal_failure_body(event)?;
    let stream_handled = context_length(&body)
        || is_usage_limit(&body)
        || is_capacity(&body)
        || classification(400, &body).is_some_and(|(code, _)| code == "thinking_signature_invalid");
    let status = if stream_handled { 400 } else { terminal_status(&body) };
    Some((
        status_error(status, body.as_bytes(), HeaderMap::new(), model_level_cooling),
        body,
    ))
}

/// `isCodexOverloadBootstrapFailure`: transient capacity rejections another credential may serve.
pub(crate) fn is_overload(body: &str) -> bool {
    if is_capacity(body) {
        return true;
    }
    let kind = lower(&gjson::get(body, "error.type"));
    let code = lower(&gjson::get(body, "error.code"));
    let mut message = lower(&gjson::get(body, "error.message"));
    if message.is_empty() {
        message = lower(&gjson::get(body, "message"));
    }
    kind == "service_unavailable_error"
        || code == "server_is_overloaded"
        || kind == "rate_limit_error"
        || code == "rate_limit_exceeded"
        || ((kind == "server_error" || code == "server_error") && message.contains("you can retry your request"))
}

/// `newCodexBootstrapOverloadErr`: the 503 the upstream refused to put on the wire.
pub(crate) fn bootstrap_overload(body: &str) -> ExecError {
    status_error(503, body.as_bytes(), HeaderMap::new(), false)
}

/// `isCodexBootstrapBufferableEvent`: frames that show the client nothing yet.
pub(crate) fn bufferable(payload: &str) -> bool {
    if payload.trim().is_empty() {
        return true;
    }
    let empty_list = |list: gjson::Value<'_>| {
        list.array().iter().all(|entry| match entry.get("type").str() {
            "output_text" | "summary_text" | "text" | "reasoning_text" => entry.get("text").str().is_empty(),
            "refusal" => entry.get("refusal").str().is_empty(),
            _ => false,
        })
    };
    let empty_part = |payload: &str| {
        let part = gjson::get(payload, "part");
        match part.get("type").str() {
            "output_text" | "summary_text" | "text" | "reasoning_text" => part.get("text").str().is_empty(),
            "refusal" => part.get("refusal").str().is_empty(),
            _ => false,
        }
    };
    match gjson::get(payload, "type").str() {
        "response.created" | "response.in_progress" | "codex.rate_limits" | "codex.response.metadata" | "keepalive" => {
            true
        }
        "response.output_item.added" => {
            let item = gjson::get(payload, "item");
            match item.get("type").str() {
                "message" => empty_list(item.get("content")),
                "reasoning" => {
                    item.get("encrypted_content").str().is_empty()
                        && empty_list(item.get("summary"))
                        && empty_list(item.get("content"))
                }
                "function_call" => item.get("arguments").str().is_empty(),
                "custom_tool_call" => item.get("input").str().is_empty(),
                _ => false,
            }
        }
        "response.content_part.added" | "response.reasoning_summary_part.added" => empty_part(payload),
        _ => false,
    }
}

/// `EnsureResponsesUsageDetails` for one JSON object payload.
pub(crate) fn ensure_usage_details(payload: String) -> String {
    let trimmed = payload.trim();
    if !trimmed.starts_with('{') || gjson::get(trimmed, "object").str() == "response.compaction" {
        return payload;
    }
    let mut out = trimmed.to_owned();
    for path in ["response.usage", "usage"] {
        let usage = gjson::get(&out, path);
        if usage.kind() != Kind::Object {
            continue;
        }
        let (output, input) = (usage.get("output_tokens_details"), usage.get("input_tokens_details"));
        let output = (
            output.exists(),
            output.kind(),
            output.get("reasoning_tokens").exists() && output.get("reasoning_tokens").kind() != Kind::Null,
        );
        let input = (
            input.exists(),
            input.kind(),
            input.get("cached_tokens").exists() && input.get("cached_tokens").kind() != Kind::Null,
        );
        out = match output {
            (false, ..) => set_raw(&out, &format!("{path}.output_tokens_details.reasoning_tokens"), "0"),
            (true, kind, _) if kind != Kind::Object => set_raw(
                &out,
                &format!("{path}.output_tokens_details"),
                r#"{"reasoning_tokens":0}"#,
            ),
            (true, _, false) => set_raw(&out, &format!("{path}.output_tokens_details.reasoning_tokens"), "0"),
            _ => out,
        };
        out = match input {
            (false, ..) => set_raw(&out, &format!("{path}.input_tokens_details.cached_tokens"), "0"),
            (true, kind, _) if kind != Kind::Object => {
                set_raw(&out, &format!("{path}.input_tokens_details"), r#"{"cached_tokens":0}"#)
            }
            (true, _, false) => set_raw(&out, &format!("{path}.input_tokens_details.cached_tokens"), "0"),
            _ => out,
        };
    }
    if out == trimmed { payload } else { out }
}

/// Go `http.StatusText` (where the `http` crate's reason phrases differ).
pub(crate) fn go_status_text(status: u16) -> &'static str {
    match status {
        103 => "Early Hints",
        413 => "Request Entity Too Large",
        414 => "Request URI Too Long",
        416 => "Requested Range Not Satisfiable",
        418 => "I'm a teapot",
        422 => "Unprocessable Entity",
        425 => "Too Early",
        s => http::StatusCode::from_u16(s)
            .ok()
            .and_then(|s| s.canonical_reason())
            .unwrap_or_default(),
    }
}

/// `EnsureResponsesUsageDetails` over one stream chunk: a JSON object, or SSE `data:`
/// lines (Go applies it to every chunk sent to an OpenAI Responses client).
pub(crate) fn ensure_usage_details_chunk(chunk: Bytes) -> Bytes {
    let text = String::from_utf8_lossy(&chunk);
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return chunk;
    }
    if trimmed.starts_with('{') {
        let out = ensure_usage_details(trimmed.to_owned());
        return if out == trimmed { chunk } else { Bytes::from(out) };
    }
    if !text.contains("data:") {
        return chunk;
    }
    let mut modified = false;
    let lines: Vec<String> = text
        .split('\n')
        .map(|line| {
            if !line.trim().starts_with("data:") {
                return line.to_owned();
            }
            let prefix = if line.starts_with("data: ") {
                "data: "
            } else if line.starts_with("data:") {
                "data:"
            } else {
                return line.to_owned();
            };
            let data = line[prefix.len()..].trim();
            if !data.starts_with('{') {
                return line.to_owned();
            }
            let updated = ensure_usage_details(data.to_owned());
            if updated == data {
                return line.to_owned();
            }
            modified = true;
            format!("{prefix}{updated}")
        })
        .collect();
    if modified { Bytes::from(lines.join("\n")) } else { chunk }
}
