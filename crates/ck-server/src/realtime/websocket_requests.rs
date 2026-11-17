//! Request edits and error frames for the Responses WebSocket.
//!
//! Every edit goes through the sjson-compatible editor below, so untouched request bytes
//! reach request ingress exactly as the client sent them.

use ck_wire::json as gj;

/// `sjson.SetRawBytes` through `ck_wire::json`; an edit sjson rejects keeps the input.
pub(crate) fn set_raw(json: &str, path: &str, raw: &str) -> String {
    edited(json, |out| gj::set_raw(out, path, raw))
}

/// `sjson.SetBytes` with a string value.
pub(crate) fn set_str(json: &str, path: &str, value: &str) -> String {
    edited(json, |out| gj::set_str(out, path, value))
}

/// `sjson.DeleteBytes`.
pub(crate) fn delete(json: &str, path: &str) -> String {
    edited(json, |out| gj::delete(out, path))
}

fn edited(json: &str, edit: impl FnOnce(&mut Vec<u8>) -> bool) -> String {
    let mut out = json.as_bytes().to_vec();
    if !edit(&mut out) {
        return json.to_owned();
    }
    String::from_utf8(out).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

/// encoding/json string encoding (`json.Marshal` escapes HTML).
fn go_quote(s: &str, escape_html: bool) -> String {
    let mut out = Vec::with_capacity(s.len() + 2);
    gj::marshal_str(&mut out, s.as_bytes(), escape_html);
    String::from_utf8(out).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

pub(crate) const CREATE: &str = "response.create";
pub(crate) const APPEND: &str = "response.append";

fn text(v: &gjson::Value<'_>) -> String {
    v.str().trim().to_owned()
}

pub(crate) fn field(json: &str, path: &str) -> String {
    text(&gjson::get(json, path))
}

pub(crate) fn status_text(status: u16) -> &'static str {
    match status {
        413 => "Request Entity Too Large",
        414 => "Request URI Too Long",
        416 => "Requested Range Not Satisfiable",
        422 => "Unprocessable Entity",
        418 => "I'm a teapot",
        s => http_status_text(s),
    }
}

fn http_status_text(status: u16) -> &'static str {
    axum::http::StatusCode::from_u16(status)
        .ok()
        .and_then(|s| s.canonical_reason())
        .unwrap_or_default()
}

/// `handlers.BuildErrorResponseBodyWithError` (no terminal-auth classification).
fn error_body(status: u16, text: &str) -> String {
    let trimmed = text.trim();
    if !trimmed.is_empty() && gjson::valid(trimmed) {
        return crate::inference::errors::compact_json(trimmed);
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
    let mut body = format!(r#"{{"error":{{"message":{},"type":"{kind}""#, go_quote(text, true));
    if !code.is_empty() {
        body.push_str(&format!(r#","code":"{code}""#));
    }
    body.push_str("}}");
    body
}

/// `buildResponsesWebsocketErrorPayload`: `{"type":"error","status":S,"error":{...}}`,
/// with the first value of each `addon` header under `headers` (Go sets them in map
/// order, so only a single header has a fixed position).
pub(crate) fn error_payload(status: u16, message: &str, addon: Option<&axum::http::HeaderMap>) -> String {
    let status = if status == 0 { 500 } else { status };
    let text = if message.trim().is_empty() {
        status_text(status).to_owned()
    } else {
        message.to_owned()
    };
    let body = error_body(status, &text);
    let mut payload = set_str("{}", "type", "error");
    payload = set_raw(&payload, "status", &status.to_string());
    if let Some(addon) = addon.filter(|h| !h.is_empty()) {
        let mut headers = String::from("{}");
        for name in addon.keys() {
            let Some(value) = addon.get(name) else { continue };
            // Go's escaping, verbatim (its `\\.` splits a dotted name).
            let key = ck_exec::proxy::canonical_header(name.as_str())
                .replace(r"\\", r"\\\\")
                .replace('.', r"\\.");
            headers = set_str(&headers, &key, &String::from_utf8_lossy(value.as_bytes()));
        }
        payload = set_raw(&payload, "headers", &headers);
    }
    if gjson::valid(&body) {
        let error = gjson::get(&body, "error");
        payload = if error.exists() {
            set_raw(&payload, "error", error.json())
        } else {
            set_raw(&payload, "error", &body)
        };
    }
    if !gjson::get(&payload, "error").exists() {
        payload = set_str(&payload, "error.type", "server_error");
        payload = set_str(&payload, "error.message", &text);
    }
    payload
}

#[cfg(test)]
#[path = "websocket_requests_tests.rs"]
mod tests;
