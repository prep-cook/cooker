//! OpenAI-compatible images under the execution contract (`docs/EXECUTION-CONTRACT.md`): the
//! images codec encodes the Images JSON from this destination's fence, the executor names the
//! upstream model and applies the payload rules last, and the answer, buffered JSON or the
//! `image_generation.*` / `image_edit.*` events the client asked for, streams through the
//! images ingress, which publishes the usage the body reports once.

use ck_core::exec::ExecError;
use ck_translate::codec::{BodyMode, Media};
use ck_translate::images;
use ck_wire::thinking::parse_suffix;

use crate::media_exec::{self, MediaIngress};
use crate::openai_compat_exec::{DESTINATION, RelayIngress, Route, encoding, send};
use crate::types::{Attempt, Request, Streams, Upstream};

/// An image generation or edit on `route`. An edit goes as JSON with its image references, as a
/// generation does.
// ponytail: a relay that takes edits only as multipart needs `images::Destination::OpenAiForm`
// with the upstream model written into the form; no such relay is configured yet.
pub(crate) async fn images(
    attempt: &Attempt<'_>,
    request: &Request<'_>,
    streams: Streams<'_>,
    route: Route,
    media: Media,
) -> Result<Upstream<RelayIngress>, ExecError> {
    let document = request.validated.view().document;
    let stream = media_exec::images_stream(document).map_err(encoding)?;
    let mut body = media_exec::images_upload(request, DESTINATION, images::Destination::OpenAi, streams.request)?;
    let model = parse_suffix(attempt.model).model_name.trim().to_owned();
    if !model.is_empty() {
        crate::openai_compat_payload::set_str_if_different(&mut body, "model", &model);
    }
    let body = rules(attempt, request, &model, body)?;
    let path = if media == Media::ImageEdit {
        "/images/edits"
    } else {
        "/images/generations"
    };
    let upstream = send(&route, attempt, request, path, &model, body, stream).await?;
    let mode = if stream {
        BodyMode::Streaming
    } else {
        BodyMode::Buffered
    };
    // After the send, so an attempt the upstream refused leaves its stream empty.
    let ingress = images::Ingress::new(
        streams.response,
        attempt.identity,
        images::Wire::OpenAi,
        mode,
        media_exec::IMAGES_LIMIT,
    )
    .map_err(encoding)?;
    Ok(Upstream {
        status: upstream.status,
        headers: upstream.headers,
        body: upstream.body,
        ingress: RelayIngress::Media(MediaIngress::Images(Box::new(ingress))),
    })
}

/// The configured payload rules, the last change to the body (Go's media finalizer: protocol
/// `openai`, the encoded upload as the baseline).
fn rules(attempt: &Attempt<'_>, request: &Request<'_>, model: &str, body: Vec<u8>) -> Result<Vec<u8>, ExecError> {
    let requested = media_exec::requested_model(request.validated.view().document).map_err(encoding)?;
    let requested = if requested.is_empty() {
        model
    } else {
        requested.as_str()
    };
    let client = &request.client;
    let original = body.clone();
    Ok(ck_wire::payload::apply(
        &ck_wire::payload::Rules::of(attempt.cfg),
        &ck_wire::payload::Request {
            target_executor: &attempt.credential.provider,
            model,
            requested_model: requested,
            protocol: "openai",
            from_protocol: "openai",
            root: "",
            original: &original,
            request_path: client.route,
            headers: Some(client.headers),
        },
        body,
    ))
}
