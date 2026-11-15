//! Protocol-specific errors before HTTP commitment and within legacy SSE writers.

use axum::http::header;
use axum::response::Response;
use serde_json::Value;

use ck_core::exec::FailureScope;

use crate::http::gojson::{self, Obj};
use crate::inference::dispatch::Failure;

/// Go `WriteErrorResponse`'s text: the error text trimmed, else the status text.
fn text_for(status: u16, text: &str) -> String {
    let v = gojson::trim(text);
    if v.is_empty() {
        gojson::status_text(status).to_owned()
    } else {
        v.to_owned()
    }
}

/// Go `json.Compact` of valid JSON: the white space between tokens dropped, strings
/// untouched (no HTML escaping), so an upstream error fits on one SSE `data:` line.
pub(crate) fn compact_json(json: &str) -> String {
    let mut out = Vec::with_capacity(json.len());
    let (mut in_string, mut escaped) = (false, false);
    for &b in json.as_bytes() {
        if in_string {
            out.push(b);
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
        } else if !matches!(b, b' ' | b'\t' | b'\n' | b'\r') {
            in_string = b == b'"';
            out.push(b);
        }
    }
    // Only ASCII white space was removed, outside strings.
    String::from_utf8(out).expect("compacting valid UTF-8 JSON keeps it UTF-8")
}

/// OpenAI errors always have an object envelope with `message`, `type` and nullable
/// `param` and `code`. Members an upstream or the scheduler adds (a numeric `code`,
/// `model`, `provider`, `reset_seconds`, ...) are kept.
pub fn openai_body(status: u16, text: &str) -> String {
    let status = if status == 0 { 500 } else { status };
    let text = if gojson::trim(text).is_empty() {
        gojson::status_text(status)
    } else {
        text
    };
    let kind = match status {
        401 => "authentication_error",
        403 => "permission_error",
        429 => "rate_limit_error",
        408 | 500.. => "server_error",
        _ => "invalid_request_error",
    };
    let mut detail = match serde_json::from_str::<Value>(text) {
        Ok(Value::Object(mut root)) => match root.remove("error") {
            Some(Value::Object(error)) if error.get("message").is_some_and(Value::is_string) => error,
            _ => serde_json::Map::new(),
        },
        _ => serde_json::Map::new(),
    };
    if detail.is_empty() {
        detail.insert("message".into(), text.into());
    }
    if !detail.get("type").is_some_and(Value::is_string) {
        detail.insert("type".into(), kind.into());
    }
    for name in ["param", "code"] {
        detail.entry(name).or_insert(Value::Null);
    }
    // The four official members first, as OpenAI orders them; the rest follow sorted.
    let mut out = Obj::new();
    for name in ["message", "type", "param", "code"] {
        let value = detail.remove(name).unwrap_or(Value::Null);
        out = out.raw(name, &gojson::sorted(&value));
    }
    for (name, value) in &detail {
        out = out.raw(name, &gojson::sorted(value));
    }
    Obj::new().raw("error", &out.finish()).finish()
}

/// Legacy stream errors keep Go's payload shape; HTTP nullable fields do not apply.
pub(crate) fn openai_stream_body(status: u16, text: &str) -> String {
    let status = if status == 0 { 500 } else { status };
    let text = if gojson::trim(text).is_empty() {
        gojson::status_text(status)
    } else {
        text
    };
    let trimmed = gojson::trim(text);
    if serde_json::from_str::<Value>(trimmed).is_ok() {
        return compact_json(trimmed);
    }
    let (kind, code) = match status {
        401 => ("authentication_error", "invalid_api_key"),
        403 => ("permission_error", "insufficient_quota"),
        429 => ("rate_limit_error", "rate_limit_exceeded"),
        404 => ("invalid_request_error", "model_not_found"),
        408 => ("server_error", "request_timeout"),
        500.. => ("server_error", "internal_server_error"),
        _ => ("invalid_request_error", ""),
    };
    let mut detail = Obj::new().str("message", text).str("type", kind);
    if !code.is_empty() {
        detail = detail.str("code", code);
    }
    Obj::new().raw("error", &detail.finish()).finish()
}

/// Go `claudeErrorTypeFromStatus`.
fn claude_type(status: u16) -> &'static str {
    match status {
        401 => "authentication_error",
        402 => "billing_error",
        403 => "permission_error",
        404 => "not_found_error",
        409 => "conflict_error",
        413 => "request_too_large",
        429 => "rate_limit_error",
        // v8.0.27 (42d484f) adds 408.
        408 | 504 => "timeout_error",
        529 => "overloaded_error",
        500.. => "api_error",
        _ => "invalid_request_error",
    }
}

/// Anthropic SSE errors have no HTTP request identity.
pub fn claude_body(status: u16, text: &str) -> String {
    claude_envelope(status, text, false, false)
}

fn claude_envelope(status: u16, text: &str, normalize_type: bool, redact_message: bool) -> String {
    let mut message = text_for(status, text);
    let thread_not_found = crate::scheduling::classify::is_claude_thread_not_found(status, &message);
    let mut kind = claude_type(status).to_owned();
    let mut code = None;
    if let Ok(Value::Object(payload)) = serde_json::from_str::<Value>(&message) {
        let field = |obj: &serde_json::Map<String, Value>, key: &str| {
            obj.get(key)
                .and_then(Value::as_str)
                .map(gojson::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
        };
        if let Some(Value::Object(err)) = payload.get("error") {
            code = err
                .get("details")
                .and_then(|v| v.get("error_code"))
                .and_then(Value::as_str)
                .and_then(safe_claude_code);
            kind = field(err, "type").unwrap_or(kind);
            if let Some(m) = field(err, "message").or_else(|| field(err, "code")) {
                message = m;
            }
        } else {
            kind = field(&payload, "type").filter(|t| t != "error").unwrap_or(kind);
            if let Some(m) = field(&payload, "message") {
                message = m;
            }
        }
    }
    if redact_message {
        let reason = gojson::status_text(status);
        message = if reason.is_empty() {
            "The request failed."
        } else {
            reason
        }
        .to_owned();
    }
    if normalize_type
        && !matches!(
            kind.as_str(),
            "invalid_request_error"
                | "authentication_error"
                | "billing_error"
                | "permission_error"
                | "not_found_error"
                | "conflict_error"
                | "request_too_large"
                | "rate_limit_error"
                | "api_error"
                | "timeout_error"
                | "overloaded_error"
        )
    {
        kind = claude_type(status).to_owned();
    }
    let mut detail = Obj::new().str("type", &kind).str("message", &message);
    if let Some(code) = code.or(thread_not_found.then_some("thread_not_found")) {
        detail = detail.raw("details", &Obj::new().str("error_code", code).finish());
    }
    Obj::new().str("type", "error").raw("error", &detail.finish()).finish()
}

fn safe_claude_code(code: &str) -> Option<&'static str> {
    match code {
        "compaction_unavailable" => Some("compaction_unavailable"),
        "compaction_block_misplaced" => Some("compaction_block_misplaced"),
        "compaction_signature_invalid" => Some("compaction_signature_invalid"),
        "compaction_content_mismatch" => Some("compaction_content_mismatch"),
        "compaction_nothing_to_summarize" => Some("compaction_nothing_to_summarize"),
        "thread_not_found" => Some("thread_not_found"),
        _ => None,
    }
}

/// Builds the client's envelope even when an executor requested a direct response.
fn write(failure: &Failure, render: impl FnOnce(u16, &str) -> Response) -> Response {
    let status = failure.status();
    let mut response = render(status, &failure.text());
    if let Some(error) = failure.direct() {
        let headers = crate::http::respond::filter_upstream_headers(&error.headers);
        for name in headers.keys() {
            if name == header::CONTENT_TYPE || name == "request-id" {
                continue;
            }
            for value in headers.get_all(name) {
                response.headers_mut().append(name.clone(), value.clone());
            }
        }
    }
    if let Some(seconds) = failure.retry_after() {
        response.headers_mut().insert(header::RETRY_AFTER, seconds.into());
    }
    response
}

pub fn openai(failure: &Failure) -> Response {
    write(failure, |status, text| {
        crate::http::respond::json(status, "application/json", openai_body(status, text))
    })
}

pub fn claude(failure: &Failure) -> Response {
    // Only text this process wrote about its own executors, credentials or scheduling is
    // hidden. Upstream messages and local request-scoped 4xx errors, which name what the
    // client sent, pass through.
    let redact_message = match failure {
        Failure::Exec(error) => !error.upstream && !(error.scope == FailureScope::Request && error.status < 500),
        Failure::UnknownModel(_) | Failure::ImageOnly(_) | Failure::SpeechOnly(_) | Failure::Cooldown { .. } => false,
        Failure::Unavailable { .. } => true,
    };
    write(failure, |status, text| {
        crate::http::errors::anthropic_body(status, claude_envelope(status, text, true, redact_message))
    })
}

pub fn gemini(failure: &Failure) -> Response {
    write(failure, crate::http::errors::gemini)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openai_envelope_preserves_official_fields() {
        let input = r#"{"error":{"message":"invalid parameter","type":"invalid_request_error","param":"model","code":"unknown_parameter"}}"#;
        assert_eq!(
            serde_json::from_str::<Value>(&openai_body(400, input)).unwrap(),
            serde_json::from_str::<Value>(input).unwrap()
        );
        for text in ["plain text", "[]", "null", r#"{"ok":true}"#] {
            let value: Value = serde_json::from_str(&openai_body(400, text)).unwrap();
            assert_eq!(value["error"]["message"], text);
            assert_eq!(value["error"]["type"], "invalid_request_error");
            assert_eq!(value["error"].get("param"), Some(&Value::Null));
            assert_eq!(value["error"].get("code"), Some(&Value::Null));
        }
    }

    #[test]
    fn openai_envelope_keeps_members_the_upstream_or_scheduler_adds() {
        // (input, member, expected)
        let rows = [
            (
                r#"{"error":{"message":"m","code":429}}"#,
                "code",
                serde_json::json!(429),
            ),
            (
                r#"{"error":{"message":"m","type":"t","param":"p","code":"c","status":"RESOURCE_EXHAUSTED"}}"#,
                "status",
                serde_json::json!("RESOURCE_EXHAUSTED"),
            ),
            (
                r#"{"error":{"code":"model_cooldown","model":"x","provider":"claude","reset_seconds":3,"reset_time":"3s","last_upstream_error":"e","message":"m"}}"#,
                "reset_seconds",
                serde_json::json!(3),
            ),
            (
                r#"{"error":{"code":"model_cooldown","model":"x","provider":"claude","reset_seconds":3,"reset_time":"3s","last_upstream_error":"e","message":"m"}}"#,
                "last_upstream_error",
                serde_json::json!("e"),
            ),
        ];
        for (input, member, want) in rows {
            let value: Value = serde_json::from_str(&openai_body(429, input)).unwrap();
            assert_eq!(value["error"][member], want, "{input}");
            assert!(value["error"]["type"].is_string(), "{input}");
            assert!(value["error"].get("param").is_some(), "{input}");
            assert!(value["error"].get("code").is_some(), "{input}");
        }
    }

    #[test]
    fn claude_route_hides_local_executor_text_but_not_upstream_or_client_correctable_text() {
        use ck_core::exec::ExecError;
        let local = ExecError::local(401, FailureScope::Credential, "missing refresh token");
        // An upstream answer need not carry headers (Meta, Antigravity, AI Studio).
        let upstream = ExecError::local(429, FailureScope::Model, "quota exhausted").from_upstream();
        // A refused token refresh keeps the endpoint's status; its scope is the credential's.
        let refresh = ExecError::local(
            400,
            FailureScope::Credential,
            "token refresh failed with status 400: invalid_grant",
        );
        let compact = ExecError::local(
            400,
            FailureScope::Request,
            "This model does not support compaction; choose a model that does.",
        );
        let unavailable = Failure::Unavailable {
            code: "auth_not_found",
            providers: vec!["claude".into()],
            model: "m".into(),
            cause: None,
            retry_after: None,
        };
        // (failure, message the client sees)
        let rows = [
            (Failure::Exec(local), "Unauthorized"),
            (Failure::Exec(refresh), "Bad Request"),
            (Failure::Exec(upstream), "quota exhausted"),
            (
                Failure::Exec(compact),
                "This model does not support compaction; choose a model that does.",
            ),
            (Failure::UnknownModel("nope".into()), "unknown provider for model nope"),
            (unavailable, "Service Unavailable"),
        ];
        for (failure, want) in rows {
            let response = claude(&failure);
            let bytes = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(axum::body::to_bytes(response.into_body(), usize::MAX))
                .unwrap();
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(value["error"]["message"], want, "{value}");
        }
    }

    #[test]
    fn claude_reconstruction_keeps_documented_codes_and_drops_the_rest() {
        // (status, upstream body, type, message, details.error_code)
        let rows = [
            (
                400,
                r#"{"type":"error","error":{"type":"invalid_request_error","message":"content mismatch","details":{"error_code":"compaction_content_mismatch","account":"secret"}},"request_id":"upstream-secret"}"#,
                "invalid_request_error",
                "content mismatch",
                Some("compaction_content_mismatch"),
            ),
            (
                529,
                r#"{"type":"error","error":{"type":"overloaded_error","message":"try later","details":{"error_code":"compaction_unavailable"}}}"#,
                "overloaded_error",
                "try later",
                Some("compaction_unavailable"),
            ),
            (409, "conflict", "conflict_error", "conflict", None),
            (
                429,
                r#"{"error":{"type":"vendor_quota_error","message":"usage limit reached","details":{"error_code":"private_code","account":"secret"}}}"#,
                "rate_limit_error",
                "usage limit reached",
                None,
            ),
        ];
        for (status, body, kind, message, code) in rows {
            let error =
                ck_core::exec::ExecError::local(status, ck_core::exec::FailureScope::Request, body).from_upstream();
            let response = claude(&Failure::Exec(error));
            let bytes = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(axum::body::to_bytes(response.into_body(), usize::MAX))
                .unwrap();
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            let mut want = serde_json::json!({"type": kind, "message": message});
            if let Some(code) = code {
                want["details"] = serde_json::json!({"error_code": code});
            }
            assert_eq!(value["error"], want, "{body}");
            assert!(!value.to_string().contains("secret"), "{body}");
        }
    }

    #[test]
    fn claude_envelope_matches_go_detail_rules() {
        assert_eq!(
            claude_body(
                429,
                r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#
            ),
            r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#
        );
        assert_eq!(
            claude_body(400, r#"{"error":{"code":"bad_thing"}}"#),
            r#"{"type":"error","error":{"type":"invalid_request_error","message":"bad_thing"}}"#
        );
        assert_eq!(
            claude_body(529, r#"{"type":"error","message":"busy"}"#),
            r#"{"type":"error","error":{"type":"overloaded_error","message":"busy"}}"#
        );
        // Go: an empty upstream body becomes `status N` before formatting.
        assert_eq!(
            claude_body(500, "status 500"),
            r#"{"type":"error","error":{"type":"api_error","message":"status 500"}}"#
        );
        assert_eq!(
            claude_body(502, "  "),
            r#"{"type":"error","error":{"type":"api_error","message":"Bad Gateway"}}"#
        );
        assert_eq!(
            claude_body(408, "stream closed"),
            r#"{"type":"error","error":{"type":"timeout_error","message":"stream closed"}}"#
        );
    }

    /// v8.0.27 TestClaudeErrorMarks*MissingThreadForClientReplay: the executor's plain
    /// message and Anthropic's own JSON both get the replay marker; nothing else does.
    #[test]
    fn missing_thread_is_marked_for_client_replay() {
        const THREAD: &str = "No thread state was found for the requested previous_message_id. Replay the full conversation with thread create to start a new Thread.";
        let marked = format!(
            r#"{{"type":"error","error":{{"type":"not_found_error","message":"{THREAD}","details":{{"error_code":"thread_not_found"}}}}}}"#
        );
        assert_eq!(claude_body(404, THREAD), marked);
        let upstream = format!(r#"{{"type":"error","error":{{"type":"not_found_error","message":"{THREAD}"}}}}"#);
        assert_eq!(claude_body(404, &upstream), marked);
        assert_eq!(
            claude_body(404, r#"{"error":{"type":"not_found_error","message":"Not Found"}}"#),
            r#"{"type":"error","error":{"type":"not_found_error","message":"Not Found"}}"#
        );
        assert!(!claude_body(500, THREAD).contains("thread_not_found"));
        // A direct (fast-mode) answer is rewritten on Claude routes only
        // (TestWriteClaudeDirectErrorMarksMissingThreadForClientReplay).
        let direct = |body: &str| {
            let mut error = ck_core::exec::ExecError::local(404, ck_core::exec::FailureScope::Request, body.to_owned());
            error.direct = true;
            Failure::Exec(error)
        };
        let read = |res: Response| {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(axum::body::to_bytes(res.into_body(), usize::MAX))
                .unwrap()
        };
        let response: Value = serde_json::from_slice(&read(claude(&direct(&upstream)))).unwrap();
        assert_eq!(response["error"]["details"]["error_code"], "thread_not_found");
        assert!(response["request_id"].is_string());
        let response: Value = serde_json::from_slice(&read(openai(&direct(&upstream)))).unwrap();
        assert!(response["error"]["message"].is_string());
        assert!(response["error"].get("param").is_some());
        let response: Value = serde_json::from_slice(&read(claude(&direct("not found")))).unwrap();
        assert_eq!(response["error"]["type"], "not_found_error");
    }
}
