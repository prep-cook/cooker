//! Gemini routes under `/v1beta` (sdk/api/handlers/gemini/gemini_handlers.go).

use std::sync::Arc;

use axum::Extension;
use axum::body::Bytes;
use axum::extract::rejection::BytesRejection;
use axum::extract::{MatchedPath, OriginalUri, Path, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use ck_core::exec::{Caller, ExecError, Operation};
use ck_core::format::Format;
use ck_translate::codec;

use crate::http::egress::{Framing, Output, Protocol};
use crate::http::respond::{self, Writer};
use crate::inference::claude::alt;
use crate::inference::dispatch::{self, Call, Done};
use crate::{Runtime, inference::errors};

#[allow(clippy::too_many_arguments)]
pub async fn action(
    State(rt): State<Arc<Runtime>>,
    Extension(caller): Extension<Caller>,
    peer: dispatch::Peer,
    matched: Option<MatchedPath>,
    OriginalUri(uri): OriginalUri,
    Path(action): Path<String>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let action = action.trim_start_matches('/');
    let parts: Vec<&str> = action.split(':').collect();
    if parts.len() != 2 {
        return crate::http::errors::gemini(404, "Not Found");
    }
    let body = match body {
        Ok(body) => body,
        Err(rejection) => {
            let (status, reason) = crate::inference::claude::read_error(&rejection);
            return crate::http::errors::gemini(status, &reason);
        }
    };
    let alt = alt(uri.query().unwrap_or_default());
    let (operation, stream) = match parts[1] {
        "generateContent" => (Operation::Generate, false),
        "streamGenerateContent" => (Operation::Generate, true),
        "countTokens" => (Operation::CountTokens, false),
        // Go answers any other method with an empty 200.
        _ => return ().into_response(),
    };
    let call = Call {
        entry: Format::Gemini,
        response: Format::Gemini,
        operation,
        model: parts[0].to_owned(),
        body,
        stream,
        alt: alt.clone(),
        headers,
        caller,
        forced_provider: None,
        selection_model: None,
        execution_session: None,
        request_path: dispatch::route_path(matched.as_ref(), &uri),
        peer: dispatch::peer(peer),
        socket: None,
        media: None,
    };
    let framing = match (stream, alt.is_some()) {
        (false, _) => Framing::Json,
        (true, false) => Framing::Sse,
        (true, true) => Framing::JsonArray,
    };
    let output = Output {
        protocol: Protocol::Gemini,
        operation: match operation {
            Operation::CountTokens => codec::Operation::CountTokens,
            _ => codec::Operation::Generate,
        },
        framing,
    };
    dispatch::exchange::serve(&rt, call, output, |result| async move {
        match result {
            Err(failure) => errors::gemini(&failure),
            Ok(Done::Buffered { body, .. }) => respond::json(200, "application/json", body),
            Ok(Done::Stream { first, rest, .. }) => {
                let raw = alt.is_some();
                let body = respond::stream(first, rest, GeminiSse { raw }, None);
                if raw { body.into_response() } else { respond::sse(body) }
            }
        }
    })
    .await
}

/// Gemini streaming: SSE `data:` frames, or raw payloads when `alt` is set.
struct GeminiSse {
    raw: bool,
}

impl Writer for GeminiSse {
    /// Internal execution and interceptors see bare Gemini chunks; only HTTP frames them.
    fn chunk(&mut self, event: Bytes) -> Vec<Bytes> {
        vec![if self.raw { event } else { respond::ensure_frame(event) }]
    }

    fn error(&mut self, error: &ExecError) -> Vec<Bytes> {
        let status = crate::scheduling::classify::response_status(error);
        let body = errors::openai_stream_body(status, &crate::scheduling::classify::error_text(error));
        vec![if self.raw {
            Bytes::from(body)
        } else {
            Bytes::from(format!("event: error\ndata: {body}\n\n"))
        }]
    }

    fn end(&mut self) -> Vec<Bytes> {
        Vec::new()
    }
}

/// Q8: S1 has no Interactions codec, so the route refuses every request visibly, in the
/// Interactions error shape.
pub async fn interactions() -> Response {
    crate::http::errors::interactions(
        501,
        "The Interactions API is not supported. Use generateContent or streamGenerateContent.",
    )
}
