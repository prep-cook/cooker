//! `/v1/images/generations` and `/v1/images/edits`
//! (sdk/api/handlers/openai/openai_images_handlers.go) on the execution contract.
//!
//! The route checks the model family and the prompt as Go's handler does, decodes the request
//! with the images codec into the exchange (a multipart edit through the E1 form ingress), and
//! serves it: the executor of the selected credential encodes it for its upstream (an xAI image
//! request, an OpenAI-compatible JSON body or form, or the Codex image tool), and the client gets
//! the Images response as one JSON body or as generation or edit events, from its first
//! deliverable byte. gpt-image models are routed to whichever provider serves them, with
//! `WithDisallowFreeAuth`.
//!
//! `GET /v1/images/{request_id}` polls a deferred xAI image job (X16) on the credential and model
//! it was created on: 202 with xAI's body while it runs, then the Images response, or xAI's
//! `error` body when the job failed.

use std::sync::Arc;

use axum::Extension;
use axum::body::Bytes;
use axum::extract::rejection::BytesRejection;
use axum::extract::{MatchedPath, OriginalUri, Path, State};
use axum::http::{HeaderMap, header};
use axum::response::{IntoResponse, Response};
use ck_core::exec::{Caller, ExecError, FailureScope, Operation};
use ck_core::format::Format;
use ck_exec::openai_compat_multipart::{self as multipart, Form, MediaType};
use ck_stream::{Store, Writer};
use ck_translate::codec::document::Document;
use ck_translate::codec::event::{Direction, Identity, LocalId};
use ck_translate::codec::multipart as form;
use ck_translate::codec::{self, ClientCapability, Media as MediaOp, RequestIngress};
use ck_translate::images;
use ck_wire::gostr::GoStr;
use ck_wire::json::{self as gj, Kind};

use crate::http::egress::{Framing, Output, Protocol};
use crate::http::respond;
use crate::inference::claude::read_failed;
use crate::inference::dispatch::{self, Call, Failure, Media, MediaKind};
use crate::inference::media::Decoded;
use crate::{Runtime, inference::errors};

const DEFAULT_TOOL_MODEL: &str = "gpt-image-2";
const CODEX_TOOL_MODELS: [&str; 5] = [
    "gpt-image-1.5",
    "gpt-image-2",
    "gpt-image-2.5-flare",
    "gpt-image-2.5-sunburst",
    "gpt-image-2.5",
];
const XAI_DEFAULT_MODEL: &str = "grok-imagine-image";
const XAI_QUALITY_MODEL: &str = "grok-imagine-image-quality";
const XAI_20_MODEL: &str = "grok-imagine-image-2.0";
const GENERATIONS_PATH: &str = "/v1/images/generations";
const EDITS_PATH: &str = "/v1/images/edits";

// --- models -----------------------------------------------------------------------------

/// `imagesModelParts`: `prefix/base` split at the last slash (a trailing slash keeps the
/// whole model as base).
pub(crate) fn model_parts(model: &str) -> (&str, &str) {
    let model = model.trim();
    match model.rfind('/') {
        Some(i) if i < model.len() - 1 => (model[..i].trim(), model[i + 1..].trim()),
        _ => ("", model),
    }
}

fn model_base(model: &str) -> String {
    model_parts(model).1.trim().go_lower()
}

/// `isXAIImagesModel`.
fn is_xai_model(model: &str) -> bool {
    let (prefix, base) = model_parts(model);
    let base = base.trim().go_lower();
    if ![XAI_DEFAULT_MODEL, XAI_QUALITY_MODEL, XAI_20_MODEL].contains(&base.as_str()) {
        return false;
    }
    matches!(prefix.trim().go_lower().as_str(), "" | "xai" | "x-ai" | "grok")
}

/// `isCodexImagesToolModel`.
fn is_codex_tool_model(model: &str) -> bool {
    CODEX_TOOL_MODELS.contains(&model_base(model).as_str())
}

/// `isOpenAICompatImagesModel`: a registered model of type `openai-image`.
fn is_compat_model(model: &str) -> bool {
    let model = model.trim();
    !model.is_empty() && ck_core::registry::lookup_model(model, None).is_some_and(|info| info.kind == "openai-image")
}

pub(crate) fn bad_request(message: &str) -> Response {
    respond::error_detail(400, message, "invalid_request_error")
}

/// `rejectUnsupportedImagesModel`.
fn unsupported(model: &str) -> Option<Response> {
    if is_codex_tool_model(model) || is_xai_model(model) || is_compat_model(model) {
        return None;
    }
    let [a, b, c, d, e] = CODEX_TOOL_MODELS;
    Some(bad_request(&format!(
        "Model {model} is not supported on {GENERATIONS_PATH} or {EDITS_PATH}. Use {a}, {b}, {c}, {d}, {e}, {XAI_DEFAULT_MODEL}, {XAI_QUALITY_MODEL}, {XAI_20_MODEL}, or a configured openai-compatibility image model."
    )))
}

/// `normalizeImagesResponseFormat`.
fn response_format(raw: &str) -> &'static str {
    if raw.trim().go_eq_fold("url") {
        "url"
    } else {
        "b64_json"
    }
}

/// `canonicalXAIImagesModel`.
fn canonical_xai_model(model: &str) -> &'static str {
    match model_base(model).as_str() {
        XAI_QUALITY_MODEL => XAI_QUALITY_MODEL,
        XAI_20_MODEL => XAI_20_MODEL,
        _ => XAI_DEFAULT_MODEL,
    }
}

pub(crate) fn text(body: &[u8], path: &str) -> String {
    gj::get(body, path).str().into_owned()
}

/// `WriteErrorResponse` for a handler-side failure.
pub(crate) fn gateway_error(message: &str) -> Response {
    respond::json(502, "application/json", errors::openai_body(502, message))
}

/// Image generation turned off for every route (`disable-image-generation: true`).
fn disabled(rt: &Runtime) -> bool {
    ck_wire::payload::Rules::of(&rt.config()).image_generation == ck_wire::payload::ImageGeneration::All
}

fn not_found() -> Response {
    axum::http::StatusCode::NOT_FOUND.into_response()
}

// --- execution ----------------------------------------------------------------------------

struct Prepared {
    rt: Arc<Runtime>,
    caller: Caller,
    peer: Option<std::net::SocketAddr>,
    headers: HeaderMap,
    path: String,
}

/// What a client asked of an image request: the routing model, whether it is a gpt-image model
/// routed to its provider, whether the client gets events, and how results are delivered.
struct Ask {
    mode: images::Mode,
    model: String,
    stream: bool,
    delivery: images::Delivery,
}

impl Ask {
    fn new(mode: images::Mode, model: &str, stream: bool, format: &str) -> Self {
        let model = if is_xai_model(model) {
            canonical_xai_model(model).to_owned()
        } else {
            model.trim().to_owned()
        };
        let delivery = if response_format(format) == "url" {
            images::Delivery::Url
        } else {
            images::Delivery::Base64
        };
        Self {
            mode,
            model,
            stream,
            delivery,
        }
    }

    fn output(&self) -> Output {
        let operation = match self.mode {
            images::Mode::Generate => MediaOp::ImageGenerate,
            images::Mode::Edit => MediaOp::ImageEdit,
        };
        Output {
            protocol: Protocol::Images(self.delivery),
            operation: codec::Operation::Media(operation),
            framing: if self.stream { Framing::Sse } else { Framing::Json },
        }
    }
}

/// Decodes the request `decode` writes and serves it on the execution contract.
async fn serve(
    p: Prepared,
    ask: Ask,
    body: Bytes,
    decode: impl FnOnce(&mut Writer, &Store) -> Result<Document, Failure>,
) -> Response {
    let output = ask.output();
    run(p, output, ask.model, ask.stream, None, body, decode).await
}

/// Serves the request `decode` writes as `output`, routed by `model` and, for a job's poll, only
/// on the `pinned` credential.
async fn run(
    p: Prepared,
    output: Output,
    model: String,
    stream: bool,
    pinned: Option<String>,
    body: Bytes,
    decode: impl FnOnce(&mut Writer, &Store) -> Result<Document, Failure>,
) -> Response {
    let decoded = match Decoded::new(&p.rt, &p.caller, output, decode, refused) {
        Ok(decoded) => decoded,
        Err(failure) => return errors::openai(&failure),
    };
    let disallow_free = is_codex_tool_model(&model);
    let call = Call {
        entry: Format::OpenAI,
        response: Format::OpenAI,
        operation: Operation::Generate,
        model,
        body,
        stream,
        alt: None,
        headers: p.headers,
        caller: p.caller,
        forced_provider: None,
        selection_model: None,
        execution_session: None,
        request_path: p.path,
        peer: p.peer,
        turn: None,
        media: Some(Arc::new(Media {
            kind: MediaKind::Images,
            pinned,
            on_selected: None,
            sse: false,
            disallow_free,
        })),
    };
    decoded
        .serve(&p.rt, call, |result| async move {
            match result {
                Err(failure) => errors::openai(&failure),
                // Every image executor (Codex, OpenAI-compatible, xAI) takes the contract arm.
                Ok(_) => unreachable!("image requests are served on the execution contract"),
            }
        })
        .await
}

/// What the images codec refused of a client request, in client wording: malformed input is
/// 400, an oversized body or part 413, and storage follows contract §9.
fn refused(error: codec::Error) -> Failure {
    let error = match error {
        codec::Error::Storage(error) => dispatch::stored(&error),
        codec::Error::Limit | codec::Error::EventTooLarge => {
            ExecError::local(413, FailureScope::Request, format!("Invalid request: {error}"))
        }
        error => ExecError::local(400, FailureScope::Request, format!("Invalid request: {error}")),
    };
    Failure::Exec(error)
}

/// The JSON request decoded by the images codec.
fn json(mode: images::Mode, body: &[u8]) -> impl FnOnce(&mut Writer, &Store) -> Result<Document, Failure> + '_ {
    move |writer, _| {
        let failed = |failure: images::RequestFailure| refused(failure.error);
        let mut decoder = images::Decoder::new(writer, mode, body.len().max(1) as u64).map_err(failed)?;
        decoder.advance(writer, body).map_err(failed)?;
        decoder.finish(writer).map_err(failed)
    }
}

/// The multipart edit read by the E1 form ingress, then by the images codec.
fn multipart_edit<'a>(
    content_type: &'a str,
    body: &'a [u8],
) -> impl FnOnce(&mut Writer, &Store) -> Result<Document, Failure> + 'a {
    move |writer, store| {
        let id = || LocalId::new(&uuid::Uuid::new_v4().simple().to_string()).expect("32 ASCII digits");
        let identity = Identity {
            request_id: id(),
            stream_id: id(),
            attempt: 0,
            direction: Direction::Request,
        };
        let limits = form::Limits {
            body_bytes: body.len() as u64,
            part_bytes: body.len() as u64,
            parts: FORM_PARTS,
        };
        let boundary = form::Boundary::new(writer, content_type).map_err(refused)?;
        let operation = codec::Operation::Media(MediaOp::ImageEdit);
        let mut ingress = form::Ingress::new(writer, store, identity, operation, &boundary, limits).map_err(refused)?;
        ingress.advance(writer, body).map_err(refused)?;
        let ready = ingress.finish(writer, &ClientCapability).map_err(refused)?;
        images::from_form(writer, ready.view()).map_err(|failure| refused(failure.error))
    }
}

/// The most parts an edit form holds: its controls, up to 16 images and a mask.
const FORM_PARTS: u64 = 64;

// --- routes -------------------------------------------------------------------------------

/// The client's JSON request with Go's handler checks: valid JSON, a supported model family and
/// a non-empty prompt, which is trimmed. The model, the event stream flag and the requested
/// response format come back with it.
fn checked_json(body: &[u8]) -> Result<(Vec<u8>, String, bool, String), Box<Response>> {
    if !gj::std_valid(body) {
        return Err(Box::new(bad_request("Invalid request: body must be valid JSON")));
    }
    let mut model = text(body, "model").trim().to_owned();
    if model.is_empty() {
        model = DEFAULT_TOOL_MODEL.into();
    }
    if let Some(rejected) = unsupported(&model) {
        return Err(Box::new(rejected));
    }
    let prompt = text(body, "prompt").trim().to_owned();
    if prompt.is_empty() {
        return Err(Box::new(bad_request("Invalid request: prompt is required")));
    }
    let mut out = body.to_vec();
    gj::set_str(&mut out, "prompt", &prompt);
    let stream = gj::get(body, "stream").bool();
    Ok((out, model, stream, text(body, "response_format")))
}

#[allow(clippy::too_many_arguments)]
pub async fn generations(
    State(rt): State<Arc<Runtime>>,
    Extension(caller): Extension<Caller>,
    peer: dispatch::Peer,
    matched: Option<MatchedPath>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    limit: crate::http::body_limit::Limit,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    if disabled(&rt) {
        return not_found();
    }
    let body = match body.map(|body| crate::inference::openai::read_body(&headers, body, limit.0)) {
        Ok(Ok(body)) => body,
        Ok(Err(response)) => return *response,
        Err(rejection) => return read_failed(&rejection),
    };
    let (request, model, stream, format) = match checked_json(&body) {
        Ok(checked) => checked,
        Err(response) => return *response,
    };
    let p = Prepared {
        rt,
        caller,
        peer: dispatch::peer(peer),
        headers,
        path: dispatch::route_path(matched.as_ref(), &uri),
    };
    let ask = Ask::new(images::Mode::Generate, &model, stream, &format);
    serve(p, ask, body.clone(), json(images::Mode::Generate, &request)).await
}

#[allow(clippy::too_many_arguments)]
pub async fn edits(
    State(rt): State<Arc<Runtime>>,
    Extension(caller): Extension<Caller>,
    peer: dispatch::Peer,
    matched: Option<MatchedPath>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    limit: crate::http::body_limit::Limit,
    read: crate::http::body_limit::Read,
) -> Response {
    if disabled(&rt) {
        return not_found();
    }
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        .unwrap_or_default();
    let lowered = content_type.trim().go_lower();
    let json_body = lowered.starts_with("application/json");
    if !json_body && !lowered.starts_with("multipart/form-data") && !lowered.is_empty() {
        return bad_request(&format!(
            "Invalid request: unsupported Content-Type {}",
            ck_wire::gostr::quote(&lowered)
        ));
    }
    // Go decodes Content-Encoding for JSON edits only (`ReadRequestBody`).
    let body = if json_body {
        match read
            .body
            .map(|body| crate::inference::openai::read_body(&headers, body, limit.0))
        {
            Ok(Ok(body)) => body,
            Ok(Err(response)) => return *response,
            Err(rejection) => return read_failed(&rejection),
        }
    } else {
        use crate::management::api::multipart::{Failed, body};
        match body(read, &content_type) {
            Ok(body) => body,
            Err(Failed::Read(rejection)) => return read_failed(&rejection),
            Err(Failed::Form(status, e)) => {
                return respond::error_detail(status, &format!("Invalid request: {e}"), "invalid_request_error");
            }
        }
    };
    let p = Prepared {
        rt,
        caller,
        peer: dispatch::peer(peer),
        headers,
        path: dispatch::route_path(matched.as_ref(), &uri),
    };
    if json_body {
        let (request, model, stream, format) = match checked_json(&body) {
            Ok(checked) => checked,
            Err(response) => return *response,
        };
        let request = references(request);
        let ask = Ask::new(images::Mode::Edit, &model, stream, &format);
        return serve(p, ask, body.clone(), json(images::Mode::Edit, &request)).await;
    }
    edits_form(p, &content_type, body).await
}

/// `GET /v1/images/{request_id}`: a poll of the caller's deferred xAI image job, pinned to the
/// credential and model the job was created on (E-Q5). A job that is not the caller's is not
/// pinned; the xAI executor refuses it with 404 before anything is sent. The client gets 202 with
/// xAI's body while the job runs, then the Images response, or xAI's `error` body.
#[allow(clippy::too_many_arguments)]
pub async fn retrieve(
    State(rt): State<Arc<Runtime>>,
    Extension(caller): Extension<Caller>,
    peer: dispatch::Peer,
    matched: Option<MatchedPath>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Path(job): Path<String>,
) -> Response {
    if disabled(&rt) {
        return not_found();
    }
    let (pinned, model) = match rt.executors.image_binding(&caller, &job) {
        Some(bound) if is_xai_model(&bound.model) => {
            let model = canonical_xai_model(&bound.model).to_owned();
            (Some(bound.credential), model)
        }
        Some(bound) => (Some(bound.credential), XAI_DEFAULT_MODEL.to_owned()),
        None => (None, XAI_DEFAULT_MODEL.to_owned()),
    };
    let p = Prepared {
        rt,
        caller,
        peer: dispatch::peer(peer),
        headers,
        path: dispatch::route_path(matched.as_ref(), &uri),
    };
    // The job's results come back as xAI stored them: base64 stays base64 and a URL stays a URL.
    let output = Output {
        protocol: Protocol::Images(images::Delivery::Base64),
        operation: codec::Operation::Media(MediaOp::ImageRetrieve),
        framing: Framing::Json,
    };
    let decode = |writer: &mut Writer, _: &Store| images::retrieve(writer, &job).map_err(|f| refused(f.error));
    run(p, output, model, false, pinned, Bytes::new(), decode).await
}

/// A multipart edit: Go's form checks (the boundary, a supported model, a prompt and an image),
/// then the form itself through the E1 ingress.
async fn edits_form(p: Prepared, content_type: &str, body: Bytes) -> Response {
    let form = match multipart_form(content_type, &body) {
        Ok(form) => form,
        Err(e) => return bad_request(&format!("Invalid request: {e}")),
    };
    let mut model = form.value("model").trim().to_owned();
    if model.is_empty() {
        model = DEFAULT_TOOL_MODEL.into();
    }
    if let Some(rejected) = unsupported(&model) {
        return rejected;
    }
    if form.value("prompt").trim().is_empty() {
        return bad_request("Invalid request: prompt is required");
    }
    if form.files("image[]").is_empty() && form.files("image").is_empty() {
        return bad_request("Invalid request: image is required");
    }
    let stream = matches!(form.value("stream").go_lower().trim(), "1" | "true" | "yes" | "on");
    let ask = Ask::new(images::Mode::Edit, &model, stream, &form.value("response_format"));
    let decode = multipart_edit(content_type, &body);
    serve(p, ask, body.clone(), decode).await
}

/// An edit's image references in the images codec's shape: Go's handler also takes a URL string
/// and a Chat-style `{"image_url":{"url":…}}`, which become `{"image_url":…}`. A reference naming
/// a file id stays as sent (X18); `image` and `images` together are refused by the codec rather
/// than merged.
fn references(mut body: Vec<u8>) -> Vec<u8> {
    let one = |value: &gj::Res<'_>| -> Option<Vec<u8>> {
        let url = match value.kind {
            Kind::String => value.str().into_owned(),
            Kind::Json if value.get("image_url").kind == Kind::Json => value.get("image_url.url").str().into_owned(),
            _ => return None,
        };
        let mut out = br#"{"image_url":""}"#.to_vec();
        gj::set_str(&mut out, "image_url", url.trim());
        Some(out)
    };
    let image = gj::get(&body, "image");
    if let Some(fixed) = one(&image) {
        gj::set_raw(&mut body, "image", fixed);
    }
    let list = gj::get(&body, "images");
    if list.is_array() {
        let items: Vec<Option<Vec<u8>>> = list.array().iter().map(one).collect();
        for (i, fixed) in items.into_iter().enumerate() {
            if let Some(fixed) = fixed {
                gj::set_raw(&mut body, &format!("images.{i}"), fixed);
            }
        }
    }
    body
}

/// `http.Request.ParseMultipartForm` as gin's `c.MultipartForm` calls it.
pub(crate) fn multipart_form(content_type: &str, body: &[u8]) -> Result<Form, String> {
    multipart::read_form(body, &multipart_boundary(content_type)?)
}

/// The boundary of a multipart form, or Go's error for a `Content-Type` that
/// `ParseMultipartForm` refuses before it reads the body (`multipartReader`, then the
/// empty-boundary check of the first `NextPart`). The value is kept as sent.
pub(crate) fn multipart_boundary(content_type: &str) -> Result<String, String> {
    const NOT_MULTIPART: &str = "request Content-Type isn't multipart/form-data";
    if content_type.is_empty() {
        return Err(NOT_MULTIPART.into());
    }
    let MediaType::Ok(media, mut params) = multipart::parse_media_type(content_type) else {
        return Err(NOT_MULTIPART.into());
    };
    if media != "multipart/form-data" {
        return Err(NOT_MULTIPART.into());
    }
    match params.remove("boundary") {
        None => Err("no multipart boundary param in Content-Type".into()),
        Some(boundary) if boundary.is_empty() => Err("multipart: boundary is empty".into()),
        Some(boundary) => Ok(boundary),
    }
}
