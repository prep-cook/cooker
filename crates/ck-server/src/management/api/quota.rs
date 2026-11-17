//! `POST /v0/management/quota/fetch` (Go `FetchCredentialQuota`): runs the
//! credential's declarative `quota_probe` (management/api/quota_probe.rs).

use std::sync::Arc;

use super::quota_probe::gojson::{self as pjson, Node};
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use ck_core::config::credentials;
use ck_core::credential::Credential;
use serde_json::{Value, json};

use super::Management;

fn map_json(status: StatusCode, value: &Value) -> Response {
    go_json(status, crate::http::gojson::sorted(value).into_bytes())
}

fn go_json(status: StatusCode, body: Vec<u8>) -> Response {
    (
        status,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        )],
        body,
    )
        .into_response()
}

fn fail(status: StatusCode, message: &str) -> Response {
    map_json(status, &json!({"error": message}))
}

/// Go `credentialQuotaRequest` as gin's `ShouldBindJSON` fills it: exact field names
/// first, then case-insensitive in struct order; `None` when binding fails.
#[derive(Default)]
struct QuotaBody {
    auth_index: [Option<String>; 3],
}

impl QuotaBody {
    /// gin `ShouldBindJSON`: `None` when binding fails.
    fn decode(body: &[u8]) -> Option<Self> {
        let (out, ok) = Self::bind(body);
        ok.then_some(out)
    }

    /// `json.Decoder.Decode` into `credentialQuotaRequest`: the first JSON value only;
    /// exact field names first, then case-insensitive in struct order. A type error
    /// leaves that field as it was and decoding goes on (Go reports it at the end);
    /// a syntax error yields nothing.
    fn bind(body: &[u8]) -> (Self, bool) {
        const FIELDS: [&str; 3] = ["auth_index", "authIndex", "AuthIndex"];
        let mut out = Self::default();
        let fields = match pjson::parse(first_value(body)) {
            Ok(mut node) => match &mut node {
                Node::Object(fields) => std::mem::take(fields),
                Node::Null => return (out, true),
                _ => return (out, false),
            },
            Err(_) => return (out, false),
        };
        let mut ok = true;
        for (key, mut v) in fields {
            let field = FIELDS
                .iter()
                .position(|f| *f == key)
                .or_else(|| FIELDS.iter().position(|f| f.eq_ignore_ascii_case(&key)));
            match (field, &mut v) {
                (Some(i @ 0..=2), Node::String(s)) => out.auth_index[i] = Some(std::mem::take(s)),
                (Some(i @ 0..=2), Node::Null) => out.auth_index[i] = None,
                (Some(_), Node::Null) | (None, _) => {}
                (Some(_), _) => ok = false,
            }
        }
        (out, ok)
    }

    /// Go `resolveAuthIndex`.
    fn auth_index(&self) -> String {
        self.auth_index
            .iter()
            .flatten()
            .map(|s| s.trim())
            .find(|s| !s.is_empty())
            .unwrap_or_default()
            .to_owned()
    }
}

/// The bytes of the first JSON value (`json.Decoder` ignores what follows it).
fn first_value(body: &[u8]) -> &[u8] {
    let start = body.iter().position(|b| !b.is_ascii_whitespace()).unwrap_or(body.len());
    let rest = &body[start..];
    let (mut depth, mut in_string, mut escaped) = (0usize, false, false);
    for (i, &b) in rest.iter().enumerate() {
        if in_string {
            match (escaped, b) {
                (true, _) => escaped = false,
                (false, b'\\') => escaped = true,
                (false, b'"') => {
                    in_string = false;
                    if depth == 0 {
                        return &rest[..=i];
                    }
                }
                _ => {}
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' | b'[' => depth += 1,
            b'}' | b']' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return &rest[..=i];
                }
            }
            b' ' | b'\t' | b'\r' | b'\n' | b',' if depth == 0 => return &rest[..i],
            _ => {}
        }
    }
    rest
}

fn credential(state: &Management, index: &str) -> Option<Arc<Credential>> {
    state
        .rt
        .store()
        .snapshot()
        .into_iter()
        .find(|c| credentials::auth_index(c) == index)
}

/// `POST /v0/management/quota/fetch` (Go `FetchCredentialQuota`).
pub(crate) async fn fetch(
    State(state): State<Arc<Management>>,
    crate::http::body_limit::FirstJson(body): crate::http::body_limit::FirstJson,
) -> Response {
    let Some(body) = QuotaBody::decode(&body) else {
        return fail(StatusCode::BAD_REQUEST, "invalid request body");
    };
    let index = body.auth_index();
    if index.is_empty() {
        return fail(StatusCode::BAD_REQUEST, "auth_index is required");
    }
    let Some(auth) = credential(&state, &index) else {
        return fail(StatusCode::NOT_FOUND, "auth not found");
    };
    // Go: the declarative quota probe, when the metadata declares one.
    if let Some(probe) = auth.metadata.get("quota_probe").and_then(Value::as_object) {
        match super::quota_probe::execute(&state, &auth, probe).await {
            Some(Ok(resp)) => return go_json(StatusCode::OK, pjson::to_vec(&resp)),
            Some(Err(e)) => return fail(StatusCode::BAD_GATEWAY, &format!("quota probe failed: {e}")),
            None => {}
        }
    }
    fail(
        StatusCode::NOT_IMPLEMENTED,
        "no quota provider available for credential",
    )
}
