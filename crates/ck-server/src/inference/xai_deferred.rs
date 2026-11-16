//! Deferred xAI Chat Completions (X11): `POST /v1/chat/completions` with `deferred:true`
//! returns xAI's `request_id`, and `GET /v1/chat/deferred-completion/{request_id}` polls it.
//! The Chat handler hands over a literal `deferred:true`; every other body stays with it.
//!
//! <https://docs.x.ai/developers/advanced-api-usage/deferred-chat-completions>

use std::sync::Arc;

use axum::Extension;
use axum::body::Bytes;
use axum::extract::{MatchedPath, OriginalUri, Path, State};
use axum::http::{HeaderMap, Uri};
use axum::response::Response;
use ck_core::exec::{Caller, Operation};
use ck_core::format::Format;
use ck_wire::json as gj;

use super::dispatch::{self, Call, Done, Media, MediaKind};
use super::errors;
use crate::{Runtime, http::respond};

/// A `POST /v1/chat/completions` body whose `deferred` is literally `true`, handed over by
/// the Chat handler once it has read the body.
pub async fn create(
    rt: &Arc<Runtime>,
    caller: Caller,
    peer: dispatch::Peer,
    matched: Option<MatchedPath>,
    uri: &Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let model = gj::get(&body, "model").str().into_owned();
    let call = call(
        caller,
        dispatch::peer(peer),
        headers,
        dispatch::route_path(matched.as_ref(), uri),
        model,
        body,
        None,
    );
    serve(rt, call, false).await
}

/// `GET /v1/chat/deferred-completion/{request_id}`: a poll of the caller's deferred Chat job,
/// pinned to the credential and model the job was created on. A job that is not the caller's
/// is not found here; the executor refuses it before anything is sent.
#[allow(clippy::too_many_arguments)]
pub async fn retrieve(
    State(rt): State<Arc<Runtime>>,
    Extension(caller): Extension<Caller>,
    peer: dispatch::Peer,
    matched: Option<MatchedPath>,
    OriginalUri(uri): OriginalUri,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let id = id.trim().to_owned();
    let Some(binding) = rt.executors.chat_binding(&caller, &id) else {
        return respond::error_detail(
            404,
            "Deferred completion not found or expired.",
            "invalid_request_error",
        );
    };
    let body = Bytes::from(serde_json::json!({"request_id": id}).to_string());
    let call = call(
        caller,
        dispatch::peer(peer),
        headers,
        dispatch::route_path(matched.as_ref(), &uri),
        binding.model,
        body,
        Some(binding.credential),
    );
    serve(&rt, call, true).await
}

#[allow(clippy::too_many_arguments)]
fn call(
    caller: Caller,
    peer: Option<std::net::SocketAddr>,
    headers: HeaderMap,
    path: String,
    model: String,
    body: Bytes,
    pinned: Option<String>,
) -> Call {
    Call {
        entry: Format::OpenAI,
        response: Format::OpenAI,
        operation: Operation::Generate,
        model,
        body,
        stream: false,
        alt: pinned.is_some().then(|| ck_exec::DEFERRED_CHAT_POLL.to_owned()),
        headers,
        caller,
        forced_provider: Some(ck_exec::xai::PROVIDER.to_owned()),
        selection_model: None,
        execution_session: None,
        request_path: path,
        peer,
        turn: None,
        media: Some(Arc::new(Media {
            kind: MediaKind::DeferredChat,
            pinned,
            disallow_free: false,
        })),
    }
}

async fn serve(rt: &Arc<Runtime>, call: Call, poll: bool) -> Response {
    dispatch::serve(rt, call, move |result| async move {
        match result {
            // The executor answers an empty body only for xAI's pending 202.
            Ok(Done::Buffered { body, .. }) => match (poll, body.is_empty()) {
                (true, true) => respond::json(202, "application/json", body),
                _ => respond::json(200, "application/json", body),
            },
            Ok(Done::Stream { .. }) => unreachable!("deferred Chat is buffered"),
            Err(failure) => errors::openai(&failure),
        }
    })
    .await
}
