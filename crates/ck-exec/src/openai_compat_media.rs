//! OpenAI-compatible images under the execution contract (`docs/EXECUTION-CONTRACT.md`): the
//! images codec encodes the Images JSON or edit form from this destination's fence, the executor
//! names the upstream model and applies the payload rules last, and the answer, buffered JSON or the
//! `image_generation.*` / `image_edit.*` events the client asked for, streams through the
//! images ingress, which publishes the usage the body reports once.

use ck_core::exec::ExecError;
use ck_translate::codec::{BodyMode, Media};
use ck_translate::images;
use ck_wire::thinking::parse_suffix;

use crate::media_exec::{self, MediaIngress};
use crate::openai_compat as compat;
use crate::openai_compat_exec::{DESTINATION, RelayIngress, Route, encoding, send};
use crate::types::{Attempt, Request, Streams, Upstream};

/// The placeholder boundary the fence checks a multipart edit with: the form's shape does not
/// depend on it.
pub(crate) const CHECK_BOUNDARY: &str = "cooker-fence-check-boundary-000000";

/// Where an edit goes: the multipart form the Images API defines, or its JSON body when the form
/// cannot carry the request (an image named by URL or file id rather than sent as data).
pub(crate) fn edit_destination<'a>(
    document: &ck_translate::codec::document::Document,
    boundary: &'a str,
) -> images::Destination<'a> {
    if images::check(document, images::Destination::OpenAiForm(CHECK_BOUNDARY)).is_ok() {
        images::Destination::OpenAiForm(boundary)
    } else {
        images::Destination::OpenAi
    }
}

/// An image generation or edit on `route`: a generation as JSON, an edit as [`edit_destination`]
/// picks, each with the upstream model and the old path's stream member
/// (`prepareOpenAICompatImagesPayload`), then the payload rules.
pub(crate) async fn images(
    attempt: &Attempt<'_>,
    request: &Request<'_>,
    streams: Streams<'_>,
    route: Route,
    media: Media,
) -> Result<Upstream<RelayIngress>, ExecError> {
    let document = request.validated.view().document;
    let stream = media_exec::images_stream(document).map_err(encoding)?;
    let boundary = format!("cooker-{}", uuid::Uuid::new_v4().simple());
    let (target, content_type) = match media {
        Media::ImageEdit => match edit_destination(document, &boundary) {
            form @ images::Destination::OpenAiForm(_) => (form, format!("multipart/form-data; boundary={boundary}")),
            json => (json, "application/json".to_owned()),
        },
        _ => (images::Destination::OpenAi, "application/json".to_owned()),
    };
    let upload = media_exec::images_upload(request, DESTINATION, target, streams.request)?;
    let model = parse_suffix(attempt.model).model_name.trim().to_owned();
    let upload_type = content_type;
    let (body, content_type) = compat::prepare_images_payload(&upload, &model, &upload_type, stream)?;
    let (body, content_type) = rules(
        attempt,
        request,
        &model,
        (&body, &content_type),
        (&upload, &upload_type),
    )?;
    let path = if media == Media::ImageEdit {
        "/images/edits"
    } else {
        "/images/generations"
    };
    let upstream = send(&route, attempt, request, (path, &content_type), &model, body, stream).await?;
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
/// `openai`, the encoded upload as the baseline); a form is shown to the rules as JSON values.
fn rules(
    attempt: &Attempt<'_>,
    request: &Request<'_>,
    model: &str,
    (body, content_type): (&[u8], &str),
    (upload, upload_type): (&[u8], &str),
) -> Result<(Vec<u8>, String), ExecError> {
    let requested = media_exec::requested_model(request.validated.view().document).map_err(encoding)?;
    let requested = if requested.is_empty() {
        model
    } else {
        requested.as_str()
    };
    let client = &request.client;
    crate::payload_media::apply(body, content_type, upload, upload_type, |view, original| {
        ck_wire::payload::apply(
            &ck_wire::payload::Rules::of(attempt.cfg),
            &ck_wire::payload::Request {
                target_executor: &attempt.credential.provider,
                model,
                requested_model: requested,
                protocol: "openai",
                from_protocol: "openai",
                root: "",
                original,
                request_path: client.route,
                headers: Some(client.headers),
            },
            view,
        )
    })
    .map_err(compat::plain_err)
}
