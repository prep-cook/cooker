//! One-shot media under the execution contract (`docs/EXECUTION-CONTRACT.md`): the request
//! egress and response ingress the image, speech and video executors share.
//!
//! An upload is encoded only from the destination's own fence (`ValidatedRequest::for_destination`),
//! so a client-only fence authorizes no send. Each capability runs the codec's `check` for its
//! destination at the fence, so a control the destination cannot express is refused before any
//! attempt sends. The answer streams through the codec's response ingress, which publishes the
//! usage the body reports, scoped and once; the executor reads no body and records no response usage.

use ck_core::exec::ExecError;
use ck_stream::Writer;
use ck_translate::codec::document::{Document, Value};
use ck_translate::codec::{self, Media, Operation, RequestView, ResponseIngress};
use ck_translate::{audio, images, video};

use crate::codex_exec::{copy, encoding};
use crate::types::Request;

/// The limit of one buffered Images body or one SSE event, which hold an image as base64 until
/// its media is published.
pub(crate) const IMAGES_LIMIT: u64 = 96 << 20;

/// The response decoder of a media operation. Boxed: the producer task moves it once, and the
/// variants differ in size.
pub enum MediaIngress {
    Images(Box<images::Ingress>),
    Speech(Box<audio::Ingress>),
    Video(Box<video::Ingress>),
}

impl ResponseIngress for MediaIngress {
    fn flush_deadline(&self) -> Option<std::time::Instant> {
        match self {
            Self::Images(ingress) => ingress.flush_deadline(),
            Self::Speech(ingress) => ingress.flush_deadline(),
            Self::Video(ingress) => ingress.flush_deadline(),
        }
    }

    fn flush(&mut self, owner: &mut Writer) -> codec::Result<()> {
        match self {
            Self::Images(ingress) => ingress.flush(owner),
            Self::Speech(ingress) => ingress.flush(owner),
            Self::Video(ingress) => ingress.flush(owner),
        }
    }

    fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> codec::Result<()> {
        match self {
            Self::Images(ingress) => ingress.advance(owner, bytes),
            Self::Speech(ingress) => ingress.advance(owner, bytes),
            Self::Video(ingress) => ingress.advance(owner, bytes),
        }
    }

    fn finish(&mut self, owner: &mut Writer) -> codec::Result<()> {
        match self {
            Self::Images(ingress) => ingress.finish(owner),
            Self::Speech(ingress) => ingress.finish(owner),
            Self::Video(ingress) => ingress.finish(owner),
        }
    }

    fn fail(&mut self, owner: &mut Writer, status: u16, scope: &'static str) -> codec::Result<()> {
        match self {
            Self::Images(ingress) => ingress.fail(owner, status, scope),
            Self::Speech(ingress) => ingress.fail(owner, status, scope),
            Self::Video(ingress) => ingress.fail(owner, status, scope),
        }
    }
}

/// Whether the document is the request `operation` names: its `x_ck.operation` (images and
/// speech) is the operation's own name.
fn names(doc: &Document, operation: Operation) -> codec::Result<()> {
    let x = doc.required(doc.root(), "x_ck")?;
    if doc.string_is(doc.required(x, "operation")?, operation.name())? {
        Ok(())
    } else {
        Err(codec::Error::Schema)
    }
}

/// The fence check of an image request for `target`.
pub(crate) fn check_images(view: RequestView<'_>, target: images::Destination<'_>) -> codec::Result<()> {
    names(view.document, view.operation)?;
    images::check(view.document, target)
}

/// The fence check of a speech request for `target`.
pub(crate) fn check_speech(view: RequestView<'_>, target: audio::Destination) -> codec::Result<()> {
    names(view.document, view.operation)?;
    audio::check(view.document, target)
}

/// The video action of a media operation; `None` for an image or speech operation.
pub(crate) const fn video_action(media: Media) -> Option<video::Action> {
    match media {
        Media::VideoCreate => Some(video::Action::Create),
        Media::VideoEdit => Some(video::Action::Edit),
        Media::VideoExtend => Some(video::Action::Extend),
        Media::VideoRetrieve => Some(video::Action::Retrieve),
        Media::VideoContent => Some(video::Action::Content),
        Media::ImageGenerate | Media::ImageEdit | Media::Speech => None,
    }
}

/// The destination's fenced view of `request`: a fence closed for another destination, or only
/// the client's, encodes nothing.
fn fenced<'a>(request: &Request<'a>, destination: &str) -> Result<RequestView<'a>, ExecError> {
    request.validated.for_destination(destination).map_err(encoding)
}

/// The image upload for `target`, encoded from `destination`'s fence.
pub(crate) fn images_upload(
    request: &Request<'_>,
    destination: &str,
    target: images::Destination<'_>,
    owner: &mut Writer,
) -> Result<Vec<u8>, ExecError> {
    let view = fenced(request, destination)?;
    copy(&images::encode(owner, view.document, target).map_err(|f| encoding(f.error))?)
}

/// The speech upload for `target`, encoded from `destination`'s fence.
pub(crate) fn speech_upload(
    request: &Request<'_>,
    destination: &str,
    target: audio::Destination,
    owner: &mut Writer,
) -> Result<Vec<u8>, ExecError> {
    let view = fenced(request, destination)?;
    copy(&audio::encode(owner, view.document, target).map_err(|f| encoding(f.error))?)
}

/// The video upload in `dialect`, encoded from `destination`'s fence, and the job a non-create
/// action names. A retrieve without options has no body.
pub(crate) fn video_upload(
    request: &Request<'_>,
    destination: &str,
    dialect: video::Dialect,
    owner: &mut Writer,
) -> Result<(Option<Vec<u8>>, Option<String>), ExecError> {
    let view = fenced(request, destination)?;
    let doc = view.document;
    let lease = video::encode(owner, doc, dialect).map_err(encoding)?;
    let body = lease.as_ref().map(copy).transpose()?;
    let value = doc.required(doc.root(), "value").map_err(encoding)?;
    let job = doc.member(value, "job_id").map_err(encoding)?;
    Ok((body, job.map(|node| text(doc, node)).transpose().map_err(encoding)?))
}

/// Whether the image request asks for an event stream (`stream: true`).
pub(crate) fn images_stream(doc: &Document) -> codec::Result<bool> {
    let x = doc.required(doc.root(), "x_ck")?;
    let controls = doc.required(doc.required(x, "provider_parameters")?, "images")?;
    Ok(match doc.member(controls, "stream")? {
        Some(node) => matches!(doc.node(node)?.value, Value::Bool(true)),
        None => false,
    })
}

/// The request's `model`, trimmed, which payload rules match as the requested model; empty
/// without one.
pub(crate) fn requested_model(doc: &Document) -> codec::Result<String> {
    match doc.member(doc.root(), "model")? {
        Some(node) => Ok(text(doc, node)?.trim().to_owned()),
        None => Ok(String::new()),
    }
}

/// The string at `node`.
pub(crate) fn text(doc: &Document, node: usize) -> codec::Result<String> {
    let mut chars = doc.chars(node)?;
    let mut out = String::new();
    while let Some(c) = chars.next_scalar()? {
        out.push(c);
    }
    Ok(out)
}

/// The `Content-Type` and declared `Content-Length` of an answer, as the speech ingress keeps them.
pub(crate) fn entity(headers: &http::HeaderMap) -> (Option<&str>, Option<u64>) {
    let content_type = headers.get(http::header::CONTENT_TYPE).and_then(|v| v.to_str().ok());
    let length = headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse().ok());
    (content_type, length)
}
