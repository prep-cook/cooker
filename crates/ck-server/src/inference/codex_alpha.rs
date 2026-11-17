//! Codex Alpha Search (`Server.codexAlphaSearch`, internal/api/server_routes.go):
//! `POST /v1/alpha/search` and `POST /backend-api/codex/alpha/search`, on the execution
//! contract.
//!
//! The route decodes the body with the search codec into the exchange and serves it on Codex
//! credentials the policy `codex_alpha_search_v1` admits (OAuth logins, and API keys with
//! `alpha-search: true`). An upstream refusal fails the attempt over to the next credential
//! before anything reaches the client; the client gets the search result JSON, or an error from
//! the shared renderer.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::rejection::BytesRejection;
use axum::extract::{MatchedPath, OriginalUri, State};
use axum::http::{HeaderMap, HeaderValue};
use axum::response::Response;
use axum::routing::post;
use axum::{Extension, Router};
use ck_core::exec::{Caller, Operation};
use ck_core::format::Format;
use ck_translate::codec;
use ck_translate::search::{Decoder, Phase};

use crate::Runtime;
use crate::http::egress::{Framing, Output, Protocol};
use crate::inference::claude::read_failed;
use crate::inference::dispatch::{self, Call};
use crate::inference::errors;
use crate::inference::images::refused;
use crate::inference::media::Decoded;

pub fn routes() -> Router<Arc<Runtime>> {
    Router::new()
        .route("/v1/alpha/search", post(alpha_search))
        .route("/backend-api/codex/alpha/search", post(alpha_search))
}

/// Go reads at most 16 MiB of the request body (`io.LimitReader`).
const MAX_BODY: usize = 16 << 20;

/// `json.Unmarshal(body, &struct{ ID, Model string })`, trimmed: keys match
/// case-insensitively and the last duplicate wins; a null or non-string value leaves the
/// field as it was; invalid JSON or a non-object sets nothing.
fn routing(body: &[u8]) -> (String, String) {
    let (mut id, mut model) = (String::new(), String::new());
    let text = String::from_utf8_lossy(body);
    if !gjson::valid(&text) {
        return (id, model);
    }
    let root = gjson::parse(&text);
    if root.kind() == gjson::Kind::Object {
        root.each(|key, value| {
            let slot = match key.str() {
                k if k.eq_ignore_ascii_case("id") => &mut id,
                k if k.eq_ignore_ascii_case("model") => &mut model,
                _ => return true,
            };
            if value.kind() == gjson::Kind::String {
                *slot = value.str().to_owned();
            }
            true
        });
    }
    (id.trim().to_owned(), model.trim().to_owned())
}

#[allow(clippy::too_many_arguments)]
async fn alpha_search(
    State(rt): State<Arc<Runtime>>,
    Extension(caller): Extension<Caller>,
    peer: dispatch::Peer,
    matched: Option<MatchedPath>,
    OriginalUri(uri): OriginalUri,
    mut headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let body = match body {
        Ok(body) => body,
        Err(rejection) => return read_failed(&rejection),
    };
    let (id, model) = routing(&body);
    let output = Output {
        protocol: Protocol::Search,
        operation: codec::Operation::AlphaSearch,
        framing: Framing::Json,
    };
    let decode = |writer: &mut ck_stream::Writer, _: &ck_stream::Store| {
        let mut decoder = Decoder::new(writer, Phase::Request, MAX_BODY as u64).map_err(refused)?;
        decoder.advance(writer, &body).map_err(refused)?;
        decoder.finish(writer).map_err(refused)
    };
    let decoded = match Decoded::new(&rt, &caller, output, decode, refused) {
        Ok(decoded) => decoded,
        Err(failure) => return errors::openai(&failure),
    };
    // Go selects with `X-Session-ID` set from the body's `id`, an explicit session.
    if let Ok(value) = HeaderValue::from_str(&id)
        && !id.is_empty()
    {
        headers.insert("x-session-id", value);
    }
    let call = Call {
        entry: Format::OpenAI,
        response: Format::OpenAI,
        operation: Operation::Generate,
        model,
        body: body.clone(),
        stream: false,
        alt: None,
        headers,
        caller,
        forced_provider: Some("codex".into()),
        selection_model: None,
        execution_session: None,
        request_path: dispatch::route_path(matched.as_ref(), &uri),
        peer: dispatch::peer(peer),
        socket: None,
        media: None,
    };
    decoded
        .serve(&rt, call, |result| async move {
            match result {
                Err(failure) => errors::openai(&failure),
                Ok(_) => unreachable!("Alpha Search is served on the execution contract"),
            }
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `encoding/json` into `struct{ ID, Model string }`.
    #[test]
    fn routing_fields_decode_like_go() {
        let r = |s: &str| routing(s.as_bytes());
        let pair = |a: &str, b: &str| (a.to_owned(), b.to_owned());
        assert_eq!(
            r(r#"{"id":"s","id":"t","model":"unregistered","query":"q"}"#),
            pair("t", "unregistered"),
            "duplicates: last wins, other fields kept"
        );
        assert_eq!(r(r#"{"ID":" a ","Model":"m"}"#), pair("a", "m"), "keys fold case");
        assert_eq!(
            r(r#"{"id":1,"model":"m","id":null}"#),
            pair("", "m"),
            "type errors skip only that field"
        );
        assert_eq!(r(r#"{"id":"a","model":"m""#), pair("", ""), "invalid JSON sets nothing");
        assert_eq!(r("[1]"), pair("", ""));
    }
}
