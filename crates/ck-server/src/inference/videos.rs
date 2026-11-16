//! `/v1/videos*` (xAI-native) and `/openai/v1/videos*` (OpenAI Videos API over xAI)
//! (sdk/api/handlers/openai/openai_videos_handlers.go) on the execution contract.
//!
//! Each route keeps Go's request checks and, for the OpenAI routes, the OpenAI-to-xAI create
//! mapping, then decodes the xAI-shaped request with the video codec into the exchange and serves
//! it: the client gets the job as xAI sent it (native routes) or as an OpenAI video object, once
//! the answer is complete. A job's polls and its content go only to the credential and model it
//! was created on (E-Q5, `VideoJobs`); another caller's, an expired or an unknown job is 404 with
//! nothing sent. The content is the finished file, streamed from its first byte with the media
//! type its host sent, fetched through the job credential's proxy.

use std::sync::Arc;

use axum::Extension;
use axum::body::Bytes;
use axum::extract::rejection::BytesRejection;
use axum::extract::{MatchedPath, OriginalUri, Path, State};
use axum::http::{HeaderMap, header};
use axum::response::Response;
use ck_core::exec::{Caller, Operation};
use ck_core::format::Format;
use ck_stream::{Store, Writer};
use ck_translate::codec::document::Document;
use ck_translate::codec::{self, Media as MediaOp};
use ck_translate::video::{self, Action, Dialect};
use ck_wire::gostr::GoStr;
use ck_wire::json::{self as gj, Kind};

use crate::http::egress::{Framing, Output, Protocol};
use crate::inference::claude::read_failed;
use crate::inference::dispatch::{self, Call, Failure, Media, MediaKind};
use crate::inference::images::{bad_request, model_parts, multipart_form, refused, text};
use crate::inference::media::Decoded;
use crate::{Runtime, http::respond, inference::errors};

const OPENAI_VIDEOS_PATH: &str = "/openai/v1/videos";
const XAI_GENERATIONS_API: &str = "/v1/videos/generations";
const XAI_EDITS_API: &str = "/v1/videos/edits";
const XAI_EXTENSIONS_API: &str = "/v1/videos/extensions";
const SORA_MODEL: &str = "sora-2";
const XAI_MODEL: &str = "grok-imagine-video";
const XAI_15_MODEL: &str = "grok-imagine-video-1.5";
const XAI_15_PREVIEW: &str = "grok-imagine-video-1.5-preview";
const DEFAULT_SECONDS: &str = "4";
const DEFAULT_SIZE: &str = "720x1280";
const DEFAULT_RESOLUTION: &str = "720p";
const MAX_REFERENCES: usize = 7;

// --- models -------------------------------------------------------------------------------

fn base(model: &str) -> String {
    model_parts(model).1.trim().go_lower()
}

/// `isXAIVideosModel`.
fn is_xai(model: &str) -> bool {
    let (prefix, b) = model_parts(model);
    let b = b.trim().go_lower();
    if b != XAI_MODEL && b != XAI_15_MODEL && b != XAI_15_PREVIEW {
        return false;
    }
    matches!(prefix.trim().go_lower().as_str(), "" | "xai" | "x-ai" | "grok")
}

/// `isSoraVideosModel`.
fn is_sora(model: &str) -> bool {
    let b = base(model);
    b == SORA_MODEL || b.starts_with("sora-2-")
}

/// `canonicalXAIVideosModel` (also `responseVideosModel`).
fn canonical(model: &str) -> &'static str {
    if is_sora(model) {
        return XAI_MODEL;
    }
    match base(model).as_str() {
        XAI_15_MODEL | XAI_15_PREVIEW => XAI_15_MODEL,
        _ => XAI_MODEL,
    }
}

/// `routingXAIVideosModel`: the preview alias keeps its own routing name.
fn routing(model: &str) -> &'static str {
    if is_sora(model) {
        return XAI_MODEL;
    }
    match base(model).as_str() {
        XAI_15_MODEL => XAI_15_MODEL,
        XAI_15_PREVIEW => XAI_15_PREVIEW,
        _ => XAI_MODEL,
    }
}

// --- request building ---------------------------------------------------------------------

/// The OpenAI-shaped create request, as `xaiVideoCreateMetadata` remembers it.
struct CreateMeta {
    model: &'static str,
    routing: &'static str,
    prompt: String,
    seconds: String,
    size: String,
    created_at: i64,
}

impl CreateMeta {
    /// The members of the OpenAI video object xAI's answer lacks, kept for the client
    /// (`video::with_originals`); the job's own `progress` and `created_at` win when xAI reports
    /// them.
    fn echo(&self) -> Vec<u8> {
        let mut out = b"{}".to_vec();
        gj::set_str(&mut out, "model", self.model);
        gj::set_str(&mut out, "prompt", &self.prompt);
        gj::set_str(&mut out, "seconds", &self.seconds);
        gj::set_str(&mut out, "size", &self.size);
        gj::set_int(&mut out, "created_at", self.created_at);
        gj::set_raw(&mut out, "progress", "0");
        out
    }
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

/// `strconv.ParseInt(s, 10, 64)`.
fn parse_int(s: &str) -> Option<i64> {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// `normalizeXAIVideosSeconds`: an integer clamped to 1..=15.
fn seconds(raw: &str) -> Result<i64, String> {
    let mut s = raw.trim();
    if s.is_empty() {
        s = DEFAULT_SECONDS;
    }
    let duration = parse_int(s).ok_or("seconds must be an integer")?;
    Ok(duration.clamp(1, 15))
}

/// `xaiVideosSizeOptions`.
fn size_options(raw: &str) -> Result<(String, &'static str), String> {
    let mut size = raw.trim();
    if size.is_empty() {
        size = DEFAULT_SIZE;
    }
    match size {
        "720x1280" | "1024x1792" => Ok((size.to_owned(), "9:16")),
        "1280x720" | "1792x1024" => Ok((size.to_owned(), "16:9")),
        _ => Err("size must be one of 720x1280, 1280x720, 1024x1792, or 1792x1024".into()),
    }
}

/// `xaiVideosAspectRatio`.
fn aspect_ratio(raw: &str) -> &'static str {
    match raw.trim().go_lower().as_str() {
        "1:1" | "square" => "1:1",
        "16:9" | "landscape" => "16:9",
        "9:16" | "portrait" => "9:16",
        "4:3" => "4:3",
        "3:4" => "3:4",
        "3:2" => "3:2",
        "2:3" => "2:3",
        _ => "",
    }
}

/// `xaiVideosResolution`.
fn resolution(raw: &str) -> &'static str {
    match raw.trim().go_lower().as_str() {
        "480p" => "480p",
        "720p" => "720p",
        _ => "",
    }
}

/// `xaiVideosInputImageURL`.
fn input_image(body: &[u8]) -> Result<String, String> {
    let reference = gj::get(body, "input_reference");
    if reference.exists() {
        let url = reference.get("image_url").str().trim().to_owned();
        let file = reference.get("file_id").str().trim().to_owned();
        if !url.is_empty() && !file.is_empty() {
            return Err("input_reference must provide exactly one of image_url or file_id".into());
        }
        if !file.is_empty() {
            return Err(
                "input_reference.file_id is not supported for xAI video generation; use input_reference.image_url"
                    .into(),
            );
        }
        if !url.is_empty() {
            return Ok(url);
        }
    }
    let image = gj::get(body, "image");
    if image.exists() {
        if image.kind == Kind::String {
            return Ok(image.str().trim().to_owned());
        }
        for path in ["url", "image_url.url"] {
            let value = image.get(path).str().trim().to_owned();
            if !value.is_empty() {
                return Ok(value);
            }
        }
    }
    Ok(text(body, "image_url").trim().to_owned())
}

/// `collectXAIVideoReferenceImages`.
fn reference_images(body: &[u8]) -> Vec<String> {
    let mut out = Vec::new();
    let mut push = |value: &str| {
        let value = value.trim();
        if !value.is_empty() {
            out.push(value.to_owned());
        }
    };
    for path in ["reference_images", "reference_image_urls"] {
        let list = gj::get(body, path);
        if !list.is_array() {
            continue;
        }
        for item in list.array() {
            if item.kind == Kind::String {
                push(&item.str());
                continue;
            }
            let url = item.get("url").str().into_owned();
            if !url.is_empty() {
                push(&url);
                continue;
            }
            let url = item.get("image_url.url").str().into_owned();
            if !url.is_empty() {
                push(&url);
            }
        }
    }
    out
}

/// `buildXAIVideosCreateRequest`.
fn create_request(body: &[u8], model: &str) -> Result<(Vec<u8>, CreateMeta), String> {
    let prompt = text(body, "prompt").trim().to_owned();
    if prompt.is_empty() {
        return Err("prompt is required".into());
    }
    let duration = seconds(&text(body, "seconds"))?;
    let (size, mut ratio) = size_options(&text(body, "size"))?;
    let mut res = DEFAULT_RESOLUTION;
    let value = aspect_ratio(&text(body, "aspect_ratio"));
    if !value.is_empty() {
        ratio = value;
    }
    let value = resolution(&text(body, "resolution"));
    if !value.is_empty() {
        res = value;
    }
    let image = input_image(body)?;
    let references = reference_images(body);
    if references.len() > MAX_REFERENCES {
        return Err(format!(
            "reference_images supports at most {MAX_REFERENCES} images on xAI"
        ));
    }
    if !image.is_empty() && !references.is_empty() {
        return Err("image and reference_images cannot be combined on xAI".into());
    }
    let mut req = b"{}".to_vec();
    gj::set_str(&mut req, "model", canonical(model));
    gj::set_str(&mut req, "prompt", &prompt);
    gj::set_raw(&mut req, "duration", duration.to_string());
    gj::set_str(&mut req, "aspect_ratio", ratio);
    gj::set_str(&mut req, "resolution", res);
    if !image.is_empty() {
        gj::set_str(&mut req, "image.url", &image);
    }
    for reference in &references {
        gj::set_str(&mut req, "reference_images.-1.url", reference);
    }
    let meta = CreateMeta {
        model: canonical(model),
        routing: routing(model),
        prompt,
        seconds: duration.to_string(),
        size,
        created_at: now_unix(),
    };
    Ok((req, meta))
}

/// gin `c.ContentType()`: the media type before any space or `;`, trimmed and lowered by
/// the handler.
fn content_type(headers: &HeaderMap) -> String {
    let raw = headers
        .get(header::CONTENT_TYPE)
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        .unwrap_or_default();
    let end = raw.find([' ', ';']).unwrap_or(raw.len());
    raw[..end].trim().go_lower()
}

/// `videosCreateRequestFromForm` over gin's `c.PostForm`: the multipart values or the
/// urlencoded body. A form that fails to parse is Go's `ParseMultipartForm` or
/// `ParseForm` error, which also covers the URL query. `limit` is the request's body
/// limit; without one, Go reads at most 10 MB of a urlencoded body.
fn create_request_from_form(
    headers: &HeaderMap,
    query: Option<&str>,
    body: &[u8],
    limit: Option<u64>,
) -> Result<Vec<u8>, String> {
    use ck_wire::mime::{MediaType, parse_media_type};
    let raw_type = headers
        .get(header::CONTENT_TYPE)
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        .unwrap_or_default();
    let query_error = crate::net::access::query_error(query.unwrap_or_default().as_bytes());
    let form = if content_type(headers) == "multipart/form-data" {
        let form = multipart_form(&raw_type, body)?;
        if let Some(e) = query_error {
            return Err(e);
        }
        Some(form)
    } else {
        // Go `parsePostForm`: [`create`] answers an unusable media type before reading
        // the body; a bad parameter still reads it, so the size error wins over it.
        let error = match parse_media_type(&raw_type) {
            MediaType::Invalid(reason) => return Err(reason.into()),
            MediaType::BadParams(_) => Some("mime: invalid media parameter".to_owned()),
            MediaType::Ok(..) => None,
        };
        const MAX_FORM: usize = 10 << 20;
        // ponytail: Go also caps a limited body at 10 MB when request logging has
        // replaced it before the handler; matching that needs the capture state here.
        if limit.is_none() && body.len() > MAX_FORM {
            return Err("http: POST too large".into());
        }
        if let Some(e) = error.or_else(|| crate::net::access::query_error(body)).or(query_error) {
            return Err(e);
        }
        None
    };
    let encoded = String::from_utf8_lossy(body).into_owned();
    let post_form = |key: &str| -> String {
        match &form {
            Some(form) => form.value(key),
            None => crate::net::access::query_get(&encoded, key)
                .map(|v| String::from_utf8_lossy(&v).into_owned())
                .unwrap_or_default(),
        }
    };
    let first = |keys: &[&str]| {
        keys.iter()
            .map(|k| post_form(k))
            .find(|v| !v.trim().is_empty())
            .unwrap_or_default()
    };
    let mut out = b"{}".to_vec();
    for field in ["model", "prompt", "seconds", "size", "aspect_ratio", "resolution"] {
        let value = post_form(field).trim().to_owned();
        if !value.is_empty() {
            gj::set_str(&mut out, field, value);
        }
    }
    let value = first(&["input_reference[image_url]", "input_reference.image_url", "image_url"]);
    if !value.trim().is_empty() {
        gj::set_str(&mut out, "input_reference.image_url", value.trim());
    }
    let value = first(&["input_reference[file_id]", "input_reference.file_id", "file_id"]);
    if !value.trim().is_empty() {
        gj::set_str(&mut out, "input_reference.file_id", value.trim());
    }
    let refs = post_form("reference_image_urls");
    for reference in refs.trim().split(',') {
        let reference = reference.trim();
        if !reference.is_empty() {
            gj::set_str(&mut out, "reference_image_urls.-1", reference);
        }
    }
    Ok(out)
}

// --- responses ---------------------------------------------------------------------------

/// `buildVideosFailedAPIResponse`.
fn failed_response(model: &str, code: &str, message: &str) -> Vec<u8> {
    let model = if model.trim().is_empty() {
        XAI_MODEL
    } else {
        model.trim()
    };
    let mut out = br#"{"object":"video","status":"failed","progress":0}"#.to_vec();
    gj::set_str(&mut out, "id", format!("video_{}", uuid::Uuid::new_v4().simple()));
    gj::set_str(&mut out, "model", model);
    gj::set_str(&mut out, "error.code", code.trim());
    gj::set_str(&mut out, "error.message", message.trim());
    out
}

/// `writeVideosFailedError` (`c.Data` with `application/json`).
fn failed(status: u16, model: &str, message: &str) -> Response {
    respond::json(
        status,
        "application/json",
        failed_response(model, "invalid_request_error", message),
    )
}

// --- execution ----------------------------------------------------------------------------

struct Request {
    rt: Arc<Runtime>,
    caller: Caller,
    peer: Option<std::net::SocketAddr>,
    headers: HeaderMap,
    path: String,
}

/// What the client gets: the job in `dialect`'s JSON, or the finished file's bytes (`Raw`).
fn output(dialect: Dialect, operation: MediaOp, framing: Framing) -> Output {
    Output {
        protocol: Protocol::Video(dialect),
        operation: codec::Operation::Media(operation),
        framing,
    }
}

/// The xAI-shaped request `body` of `action` on `job` as the video codec decodes it, with the
/// members `echo` names kept for an OpenAI client.
fn decode<'a>(
    action: Action,
    job: Option<&'a str>,
    body: &'a [u8],
    echo: Option<Vec<u8>>,
) -> impl FnOnce(&mut Writer, &Store) -> Result<Document, Failure> + 'a {
    move |writer, _| {
        let phase = video::Phase::Request(action);
        let limit = body.len().max(1) as u64;
        let mut decoder = video::Decoder::new(writer, Dialect::Xai, phase, job, limit).map_err(refused)?;
        if !body.is_empty() {
            decoder.advance(writer, body).map_err(refused)?;
        }
        let operation = decoder.finish(writer).map_err(refused)?.operation;
        match echo {
            Some(echo) => video::with_originals(writer, &operation, Dialect::OpenAi, &echo).map_err(refused),
            None => Ok(operation),
        }
    }
}

/// Serves the request `decode` writes as `output` on the execution contract, routed by `model`
/// and, for a job's poll or content, only on the `pinned` credential.
async fn serve(
    r: Request,
    output: Output,
    model: &str,
    pinned: Option<String>,
    body: Bytes,
    decode: impl FnOnce(&mut Writer, &Store) -> Result<Document, Failure>,
) -> Response {
    let decoded = match Decoded::new(&r.rt, &r.caller, output, decode, refused) {
        Ok(decoded) => decoded,
        Err(failure) => return errors::openai(&failure),
    };
    let call = Call {
        entry: Format::OpenAI,
        response: Format::OpenAI,
        operation: Operation::Generate,
        model: model.trim().to_owned(),
        body,
        stream: false,
        alt: None,
        headers: r.headers,
        caller: r.caller,
        forced_provider: None,
        selection_model: None,
        execution_session: None,
        request_path: r.path,
        peer: r.peer,
        turn: None,
        media: Some(Arc::new(Media {
            kind: MediaKind::Videos,
            pinned,
            disallow_free: false,
        })),
    };
    decoded
        .serve(&r.rt, call, |result| async move {
            match result {
                Err(failure) => errors::openai(&failure),
                Ok(_) => unreachable!("video requests are served on the execution contract"),
            }
        })
        .await
}

/// The credential and routing model a poll of `video` is pinned to: those its job was created on
/// (E-Q5, `VideoJobs`). A job that is not the caller's is not pinned; the executor refuses it
/// with 404 before anything is sent.
fn pinned_and_model(r: &Request, video: &str, fallback: &str) -> (Option<String>, String) {
    match r.rt.executors.video_binding(&r.caller, video) {
        Some(bound) => {
            let model = if bound.model.trim().is_empty() {
                fallback
            } else {
                routing(&bound.model)
            };
            (Some(bound.credential), model.to_owned())
        }
        None => (None, fallback.to_owned()),
    }
}

// --- routes -------------------------------------------------------------------------------

fn request(
    rt: Arc<Runtime>,
    caller: Caller,
    peer: dispatch::Peer,
    matched: Option<MatchedPath>,
    uri: &axum::http::Uri,
    headers: HeaderMap,
) -> Request {
    Request {
        rt,
        caller,
        peer: dispatch::peer(peer),
        headers,
        path: dispatch::route_path(matched.as_ref(), uri),
    }
}

/// `POST /openai/v1/videos` (`VideosCreate`).
#[allow(clippy::too_many_arguments)]
pub async fn create(
    State(rt): State<Arc<Runtime>>,
    Extension(caller): Extension<Caller>,
    peer: dispatch::Peer,
    matched: Option<MatchedPath>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    limit: crate::http::body_limit::Limit,
    client_request: axum::extract::Request,
) -> Response {
    use axum::extract::FromRequest as _;
    use ck_wire::mime::{MediaType, parse_media_type};
    // Go answers these form types without reading the body: `parsePostForm` an unusable
    // urlencoded media type, `ParseMultipartForm` a multipart type or boundary it refuses.
    let raw_type = headers
        .get(header::CONTENT_TYPE)
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        .unwrap_or_default();
    let refused = match content_type(&headers).as_str() {
        "application/x-www-form-urlencoded" => match parse_media_type(&raw_type) {
            MediaType::Invalid(reason) => Some(reason.to_owned()),
            _ => None,
        },
        "multipart/form-data" => crate::inference::images::multipart_boundary(&raw_type).err(),
        _ => None,
    };
    if let Some(reason) = refused {
        return failed(400, XAI_MODEL, &format!("Invalid request: {reason}"));
    }
    use crate::management::api::multipart::Failed;
    let Ok(read) = crate::http::body_limit::Read::from_request(client_request, &()).await;
    let body = match content_type(&headers).as_str() {
        "multipart/form-data" => crate::management::api::multipart::body(read, &raw_type),
        _ => read.body.map_err(Failed::Read),
    };
    let raw = match (content_type(&headers).as_str(), body) {
        (_, Err(Failed::Read(rejection))) => Err(crate::inference::claude::read_error(&rejection)),
        (_, Err(Failed::Form(status, e))) => Err((status, e)),
        ("multipart/form-data" | "application/x-www-form-urlencoded", Ok(body)) => {
            create_request_from_form(&headers, uri.query(), &body, limit.0).map_err(|e| (400, e))
        }
        // Go `readVideosCreateRequest`: `ReadRequestBody`, then the JSON check.
        (_, Ok(body)) => crate::inference::openai::decode_body(&headers, body, limit.0).and_then(|body| {
            if gj::std_valid(&body) {
                Ok(body.to_vec())
            } else {
                Err((400, "body must be valid JSON".to_owned()))
            }
        }),
    };
    let raw = match raw {
        Ok(raw) => raw,
        Err((status, e)) => return failed(status, XAI_MODEL, &format!("Invalid request: {e}")),
    };
    let mut model = text(&raw, "model").trim().to_owned();
    if model.is_empty() {
        model = XAI_MODEL.into();
    }
    if !is_xai(&model) && !is_sora(&model) {
        let path = if uri.path().trim().is_empty() {
            OPENAI_VIDEOS_PATH
        } else {
            uri.path().trim()
        };
        return failed(
            400,
            &model,
            &format!("Model {model} is not supported on {path}. Use {SORA_MODEL}."),
        );
    }
    let (req, meta) = match create_request(&raw, &model) {
        Ok(built) => built,
        Err(e) => return failed(400, canonical(&model), &format!("Invalid request: {e}")),
    };
    let r = request(rt, caller, peer, matched, &uri, headers);
    let output = output(Dialect::OpenAi, MediaOp::VideoCreate, Framing::Json);
    let body = Bytes::from(req);
    let decode = decode(Action::Create, None, &body, Some(meta.echo()));
    serve(r, output, meta.routing, None, body.clone(), decode).await
}

/// `POST /v1/videos`, `/v1/videos/generations`, `/edits`, `/extensions`
/// (`handleXAIVideosNativePost`).
#[allow(clippy::too_many_arguments)]
pub async fn native_post(
    State(rt): State<Arc<Runtime>>,
    Extension(caller): Extension<Caller>,
    peer: dispatch::Peer,
    matched: Option<MatchedPath>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    limit: crate::http::body_limit::Limit,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let body = match body.map(|body| crate::inference::openai::read_body(&headers, body, limit.0)) {
        Ok(Ok(body)) => body,
        Ok(Err(response)) => return *response,
        Err(rejection) => return read_failed(&rejection),
    };
    if !gj::std_valid(&body) {
        return bad_request("Invalid request: body must be valid JSON");
    }
    let mut model = text(&body, "model").trim().to_owned();
    if model.is_empty() {
        model = XAI_MODEL.into();
    }
    if !is_xai(&model) {
        return bad_request(&format!(
            "Model {model} is not supported on {XAI_GENERATIONS_API}, {XAI_EDITS_API}, or {XAI_EXTENSIONS_API}. Use {XAI_MODEL}."
        ));
    }
    let mut raw = body.to_vec();
    gj::set_str(&mut raw, "model", canonical(&model));
    let (action, operation) = match uri.path() {
        XAI_EDITS_API => (Action::Edit, MediaOp::VideoEdit),
        XAI_EXTENSIONS_API => (Action::Extend, MediaOp::VideoExtend),
        _ => (Action::Create, MediaOp::VideoCreate),
    };
    let r = request(rt, caller, peer, matched, &uri, headers);
    let output = output(Dialect::Xai, operation, Framing::Json);
    let body = Bytes::from(raw);
    let decode = decode(action, None, &body, None);
    serve(r, output, routing(&model), None, body.clone(), decode).await
}

/// `GET /v1/videos/{request_id}` (`XAIVideosRetrieve`).
pub async fn native_retrieve(
    State(rt): State<Arc<Runtime>>,
    Extension(caller): Extension<Caller>,
    peer: dispatch::Peer,
    matched: Option<MatchedPath>,
    OriginalUri(uri): OriginalUri,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let id = id.trim().to_owned();
    if id.is_empty() {
        return bad_request("Invalid request: request_id is required");
    }
    let r = request(rt, caller, peer, matched, &uri, headers);
    let (pinned, model) = pinned_and_model(&r, &id, XAI_MODEL);
    let output = output(Dialect::Xai, MediaOp::VideoRetrieve, Framing::Json);
    let decode = decode(Action::Retrieve, Some(&id), b"", None);
    serve(r, output, &model, pinned, Bytes::new(), decode).await
}

/// `GET /openai/v1/videos/{video_id}` (`VideosRetrieve`).
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
    if id.is_empty() {
        return bad_request("Invalid request: video_id is required");
    }
    let r = request(rt, caller, peer, matched, &uri, headers);
    let (pinned, model) = pinned_and_model(&r, &id, XAI_MODEL);
    let output = output(Dialect::OpenAi, MediaOp::VideoRetrieve, Framing::Json);
    // `buildVideosRetrieveAPIResponseFromXAI` names this model when xAI's job names none.
    let mut echo = b"{}".to_vec();
    gj::set_str(&mut echo, "model", canonical(SORA_MODEL));
    let decode = decode(Action::Retrieve, Some(&id), b"", Some(echo));
    serve(r, output, &model, pinned, Bytes::new(), decode).await
}

/// `GET /openai/v1/videos/{video_id}/content` (`VideosContent`): the finished file, which the
/// xAI executor finds by polling the job and downloads from its `video.url`.
#[allow(clippy::too_many_arguments)]
pub async fn content(
    State(rt): State<Arc<Runtime>>,
    Extension(caller): Extension<Caller>,
    peer: dispatch::Peer,
    matched: Option<MatchedPath>,
    OriginalUri(uri): OriginalUri,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let id = id.trim().to_owned();
    if id.is_empty() {
        return bad_request("Invalid request: video_id is required");
    }
    let query = uri.query().unwrap_or_default();
    let mut variant = crate::net::access::query_get(query, "variant")
        .map(|v| String::from_utf8_lossy(&v).trim().to_owned())
        .unwrap_or_default();
    if variant.is_empty() {
        variant = "video".into();
    }
    if variant != "video" {
        return bad_request(&format!(
            "Invalid request: variant {} is not available for xAI video downloads",
            ck_wire::gostr::quote(&variant)
        ));
    }
    let r = request(rt, caller, peer, matched, &uri, headers);
    let (pinned, model) = pinned_and_model(&r, &id, XAI_MODEL);
    let output = output(Dialect::OpenAi, MediaOp::VideoContent, Framing::Raw);
    let decode = decode(Action::Content, Some(&id), b"", None);
    serve(r, output, &model, pinned, Bytes::new(), decode).await
}

#[cfg(test)]
#[path = "videos_tests.rs"]
mod tests;
