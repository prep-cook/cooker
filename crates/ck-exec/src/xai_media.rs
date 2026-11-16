//! xAI images, videos and speech. `XaiExecutor::serve_media` runs them under the execution
//! contract; `images`, `videos` and `speech` are the old routes' entry points until E7 moves
//! the routes onto it.

use bytes::Bytes;
use ck_core::config::Config;
use ck_core::credential::Credential;
use ck_core::exec::{ExecError, ExecRequest, ExecResponse, FailureScope, ResponseBody};
use ck_core::format::Format;
use ck_wire::gostr::GoStr;
use ck_wire::json as gj;
use http::HeaderMap;

use ck_translate::codec::{self, BodyMode, Media, RequestView};
use ck_translate::{audio, images, video};
use ck_wire::thinking::parse_suffix;

use crate::media_exec::{self, MediaIngress};
use crate::openai_compat_http::{self as wire, GoHeaders};
use crate::types::{Attempt, Request, Streams, Upstream};
use crate::xai::{
    PROVIDER, XaiExecutor, capture_request, chat_base_url, compact_base_url, creds, endpoint, fail_if_canceled,
    path_escape, plain_headers, request_headers, scoped, track_first_byte,
};
use crate::xai_events::XaiIngress;
use crate::xai_exec;
use crate::xai_request as request;
use crate::xai_response::{self as response, text};

const IMAGES_GENERATIONS: &str = "/images/generations";
const IMAGES_EDITS: &str = "/images/edits";
const VIDEOS_GENERATIONS: &str = "/videos/generations";
const VIDEOS_EDITS: &str = "/videos/edits";
const VIDEOS_EXTENSIONS: &str = "/videos/extensions";

/// The payload rules on an image or video body (Go's media finalizer: the body's
/// model, else the routed one; protocol `openai`; the client body as the baseline).
fn media_rules(req: &ExecRequest, cfg: &Config, payload: Vec<u8>) -> Vec<u8> {
    let mut model = text(&gj::get(&req.body, "model")).trim().to_owned();
    if model.is_empty() {
        model = req.model.trim().to_owned();
    }
    crate::payload_barrier::Barrier {
        executor: PROVIDER,
        model: &model,
        protocol: "openai",
        root: "",
        original: &req.body,
    }
    .apply(cfg, req, payload)
}

impl XaiExecutor {
    /// `executeImages`: the JSON body, image references in xAI's shape, to
    /// `/images/edits` or `/images/generations` by the inbound route.
    pub async fn images(
        &self,
        credential: &Credential,
        req: ExecRequest,
        request_path: &str,
        cfg: &Config,
    ) -> Result<ExecResponse, ExecError> {
        let cfg = scoped(credential, cfg);
        let path = if request_path.ends_with(IMAGES_EDITS) {
            IMAGES_EDITS
        } else {
            IMAGES_GENERATIONS
        };
        let payload = media_rules(&req, cfg.as_ref(), request::normalize_image_refs(req.body.to_vec()));
        self.media(credential, &req, cfg.as_ref(), wreq::Method::POST, path, payload, false)
            .await
    }

    /// `executeVideos`: generations, edits and extensions by the inbound route; any other
    /// route with a `request_id` polls `GET /videos/{request_id}`.
    pub async fn videos(
        &self,
        credential: &Credential,
        req: ExecRequest,
        request_path: &str,
        cfg: &Config,
    ) -> Result<ExecResponse, ExecError> {
        let cfg = scoped(credential, cfg);
        let payload = media_rules(&req, cfg.as_ref(), request::normalize_image_refs(req.body.to_vec()));
        let route = [VIDEOS_EDITS, VIDEOS_EXTENSIONS, VIDEOS_GENERATIONS]
            .into_iter()
            .find(|p| request_path.ends_with(p));
        let request_id = text(&gj::get(&payload, "request_id"));
        let (method, path) = match route {
            Some(path) => (wreq::Method::POST, path.to_owned()),
            None if !request_id.is_empty() => (wreq::Method::GET, format!("/videos/{}", path_escape(&request_id))),
            None => (wreq::Method::POST, VIDEOS_GENERATIONS.to_owned()),
        };
        self.media(credential, &req, cfg.as_ref(), method, &path, payload, true)
            .await
    }

    /// The shared media request: plain xAI headers on the chat base URL, the whole
    /// response returned as is. `payload` is sent with a POST; a GET poll sends no body,
    /// though Go's capture still logs the payload.
    #[allow(clippy::too_many_arguments)]
    async fn media(
        &self,
        credential: &Credential,
        req: &ExecRequest,
        cfg: &Config,
        method: wreq::Method,
        path: &str,
        payload: Vec<u8>,
        idempotent: bool,
    ) -> Result<ExecResponse, ExecError> {
        let logged = payload.as_slice();
        let body = (method == wreq::Method::POST).then(|| payload.clone());
        let (token, _) = creds(credential);
        let mut headers = plain_headers(credential, &token, false, "", req);
        if idempotent && method == wreq::Method::POST {
            let header = |name: &str| {
                req.headers
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .map(str::trim)
                    .unwrap_or_default()
                    .to_owned()
            };
            // The handler's `Idempotency-Key` metadata, else the client's own header.
            let mut key = header("Idempotency-Key");
            if key.is_empty() {
                key = header("x-idempotency-key");
            }
            if !key.is_empty() {
                headers.set("x-idempotency-key", key);
            }
        }
        let url = self.url(endpoint(&chat_base_url(credential), path));
        let (status, headers, data) = self
            .send_buffered(credential, req, cfg, method, &url, headers, body, logged)
            .await?;
        if !(200..300).contains(&status) {
            let mut error = response::status_error(status, &data);
            error.headers = Box::new(headers);
            return Err(error);
        }
        // Go only observes the response model here (no usage parse): a Codex-format line
        // that is not a terminal event carries exactly that.
        if req.usage.enabled() {
            req.usage.response_line(Format::Codex, &data);
        }
        Ok(ExecResponse {
            status: 200,
            headers,
            body: ResponseBody::Buffered(data),
        })
    }
}

impl XaiExecutor {
    /// One request whose whole response body is read: captured, timed and canceled like
    /// every xAI call. `logged` is what the capture records as the body.
    #[allow(clippy::too_many_arguments)]
    async fn send_buffered(
        &self,
        credential: &Credential,
        req: &ExecRequest,
        cfg: &Config,
        method: wreq::Method,
        url: &str,
        headers: GoHeaders,
        body: Option<Vec<u8>>,
        logged: &[u8],
    ) -> Result<(u16, HeaderMap, Bytes), ExecError> {
        let client = self.clients.for_credential(credential, cfg);
        let route = |_: &url::Url| {
            Ok(crate::proxy::Route {
                client: client.clone(),
                order: None,
            })
        };
        let capture = capture_request(req, credential, method.as_str(), url, &headers, logged);
        req.usage.round_trip_started();
        let send = crate::proxy::send_request(&route, method, url, headers, body.map(Bytes::from), None);
        let upstream = fail_if_canceled(&req.usage, send).await;
        capture.sent(&upstream);
        let upstream = track_first_byte(upstream?, &req.usage);
        let status = upstream.status;
        let headers = upstream.headers.clone();
        let data = fail_if_canceled(&req.usage, wire::read_all(upstream)).await;
        capture.read(&data);
        Ok((status, headers, data?))
    }

    /// `executeSpeech`: the speech handler's JSON to `/tts` on the compact base URL (the
    /// official API unless a custom non-chat-proxy base URL is set; the chat proxy does
    /// not serve `/tts`, `xaiSpeechRequestURL`), with payload rules for the
    /// `openai` protocol. The raw audio comes back with the upstream headers.
    pub async fn speech(
        &self,
        credential: &Credential,
        req: ExecRequest,
        cfg: &Config,
    ) -> Result<ExecResponse, ExecError> {
        let cfg = scoped(credential, cfg);
        let mut model = text(&gj::get(&req.body, "model"));
        if model.is_empty() {
            model = req.model.trim().to_owned();
        }
        let payload = ck_wire::payload::apply(
            &ck_wire::payload::Rules::of(cfg.as_ref()),
            &ck_wire::payload::Request {
                target_executor: PROVIDER,
                model: &model,
                requested_model: request::requested_model(&req),
                protocol: "openai",
                from_protocol: SPEECH_SOURCE,
                root: "",
                original: &req.body,
                request_path: &req.request_path,
                headers: Some(&req.headers),
            },
            req.body.to_vec(),
        );
        let (token, _) = creds(credential);
        let mut headers = plain_headers(credential, &token, false, "", &req);
        // Official TTS answers with raw audio; the media default asks for JSON.
        headers.set("Accept", "*/*");
        let url = self.url(endpoint(&compact_base_url(credential), "/tts"));
        let (status, headers, data) = self
            .send_buffered(
                credential,
                &req,
                cfg.as_ref(),
                wreq::Method::POST,
                &url,
                headers,
                Some(payload.clone()),
                &payload,
            )
            .await?;
        if !(200..300).contains(&status) {
            let mut error = response::status_error(status, &data);
            if error.status == 404 && !speech_model_unavailable(&data) {
                // Go's `xaiSpeechRequestError` (an unknown voice, say): neither rotated nor
                // cooled. A request scope without upstream headers is the explicit marker
                // (classify::explicit_request_scoped); Go's xAI status errors carry none.
                error.scope = FailureScope::Request;
            } else {
                error.headers = Box::new(headers);
            }
            return Err(error);
        }
        // `EnsurePublished`: one record without usage; the audio is never parsed.
        req.usage.publish();
        Ok(ExecResponse {
            status: 200,
            headers,
            body: ResponseBody::Buffered(data),
        })
    }
}

/// Go's `openai-speech` handler type, the speech request's source format.
const SPEECH_SOURCE: &str = "openai-speech";

/// `xaiSpeechModelUnavailablePatterns`: the auth manager's model-support phrases.
const SPEECH_MODEL_UNAVAILABLE: [&str; 12] = [
    "model_not_found",
    "model_not_supported",
    "model is not supported",
    "model is unsupported",
    "model not supported",
    "unsupported model",
    "model is not available",
    "model not available",
    "model is unavailable",
    "model unavailable",
    "not available for your plan",
    "not available for your account",
];

/// `xaiSpeechModelUnavailable`: the error body says the speech model itself is missing,
/// unsupported or unavailable, rather than a request value being wrong.
fn speech_model_unavailable(body: &[u8]) -> bool {
    let names = |text: &str| {
        let lower = text.go_lower();
        SPEECH_MODEL_UNAVAILABLE.iter().any(|p| lower.contains(p))
    };
    if !gj::valid(body) {
        return names(&String::from_utf8_lossy(body));
    }
    [
        "code",
        "error.code",
        "type",
        "error.type",
        "error",
        "error.message",
        "message",
        "detail",
    ]
    .iter()
    .any(|path| names(&gj::get(body, path).str()))
}

/// The limit of one speech body: the audio file itself.
const SPEECH_LIMIT: u64 = 64 << 20;
/// The limit of one video job body; the video itself is a separate download.
const VIDEO_LIMIT: u64 = 1 << 20;

/// Whether xAI serves `media`. A finished video's bytes are fetched from the URL its job names,
/// which the job table holds (E7), not through an xAI endpoint.
pub(crate) const fn admits(media: Media) -> bool {
    !matches!(media, Media::VideoContent)
}

/// The fence check of an xAI media request.
pub(crate) fn check(view: RequestView<'_>, media: Media) -> codec::Result<()> {
    match media {
        Media::ImageGenerate | Media::ImageEdit => media_exec::check_images(view, images::Destination::Xai),
        Media::Speech => media_exec::check_speech(view, audio::Destination::Xai),
        media => match media_exec::video_action(media) {
            Some(action) => video::check_request(view.document, action, video::Dialect::Xai),
            None => Err(codec::Error::Unsupported),
        },
    }
}

impl XaiExecutor {
    /// Images, speech and video jobs under the execution contract: the fenced upload in xAI's
    /// shape with the payload rules last, sent with the plain xAI headers, and the answer
    /// streamed through the media codec's ingress, created only after xAI answered.
    pub(crate) async fn serve_media(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
        media: Media,
    ) -> Result<Upstream<XaiIngress>, ExecError> {
        let cfg = scoped(attempt.credential, attempt.cfg);
        let call = MediaCall {
            attempt,
            request,
            cfg: cfg.as_ref(),
        };
        let (status, headers, body, ingress) = match media {
            Media::ImageGenerate | Media::ImageEdit => {
                let upload = media_exec::images_upload(
                    request,
                    xai_exec::DESTINATION,
                    images::Destination::Xai,
                    streams.request,
                )?;
                let path = if media == Media::ImageEdit {
                    IMAGES_EDITS
                } else {
                    IMAGES_GENERATIONS
                };
                let upstream = self
                    .media_send(
                        &call,
                        wreq::Method::POST,
                        path,
                        Some(call.rules("openai", upload)),
                        false,
                    )
                    .await?;
                let ingress = images::Ingress::new(
                    streams.response,
                    attempt.identity,
                    images::Wire::Xai,
                    BodyMode::Buffered,
                    media_exec::IMAGES_LIMIT,
                )
                .map_err(xai_exec::encoding)?;
                (
                    upstream.status,
                    upstream.headers,
                    upstream.body,
                    MediaIngress::Images(Box::new(ingress)),
                )
            }
            Media::Speech => {
                let upload = media_exec::speech_upload(
                    request,
                    xai_exec::DESTINATION,
                    audio::Destination::Xai,
                    streams.request,
                )?;
                let upstream = self
                    .media_send(
                        &call,
                        wreq::Method::POST,
                        "/tts",
                        Some(call.rules(SPEECH_SOURCE, upload)),
                        true,
                    )
                    .await?;
                let (content_type, length) = media_exec::entity(&upstream.headers);
                let ingress =
                    audio::Ingress::new(streams.response, attempt.identity, content_type, length, SPEECH_LIMIT)
                        .map_err(xai_exec::encoding)?;
                (
                    upstream.status,
                    upstream.headers,
                    upstream.body,
                    MediaIngress::Speech(Box::new(ingress)),
                )
            }
            media => self.video(&call, streams, media).await?,
        };
        Ok(Upstream {
            status,
            headers,
            body,
            ingress: XaiIngress::Media(ingress),
        })
    }

    /// A video job: create, edit and extend post the request; retrieve polls the job it names.
    async fn video(
        &self,
        call: &MediaCall<'_>,
        streams: Streams<'_>,
        media: Media,
    ) -> Result<(u16, HeaderMap, crate::types::Body, MediaIngress), ExecError> {
        let (upload, job) = media_exec::video_upload(
            call.request,
            xai_exec::DESTINATION,
            video::Dialect::Xai,
            streams.request,
        )?;
        let (method, path, phase) = match (media, &job) {
            (Media::VideoCreate, None) => (
                wreq::Method::POST,
                VIDEOS_GENERATIONS.to_owned(),
                video::Phase::Accepted,
            ),
            (Media::VideoEdit, _) => (wreq::Method::POST, VIDEOS_EDITS.to_owned(), video::Phase::Accepted),
            (Media::VideoExtend, _) => (wreq::Method::POST, VIDEOS_EXTENSIONS.to_owned(), video::Phase::Accepted),
            (Media::VideoRetrieve, Some(job)) => (
                wreq::Method::GET,
                format!("/videos/{}", path_escape(job)),
                video::Phase::Job,
            ),
            _ => return Err(xai_exec::encoding(codec::Error::Schema)),
        };
        // A poll sends no body; a POST is the request with the payload rules.
        let body = match upload {
            Some(upload) if method == wreq::Method::POST => Some(call.rules("openai", upload)),
            _ => None,
        };
        let upstream = self.media_send(call, method, &path, body, false).await?;
        let polled = (phase == video::Phase::Job).then_some(job.as_deref()).flatten();
        let ingress = video::Ingress::new(
            streams.response,
            call.attempt.identity,
            video::Dialect::Xai,
            phase,
            polled,
            VIDEO_LIMIT,
        )
        .map_err(xai_exec::encoding)?;
        Ok((
            upstream.status,
            upstream.headers,
            upstream.body,
            MediaIngress::Video(Box::new(ingress)),
        ))
    }

    /// One media request on the chat base URL (speech: the compact base URL, as
    /// `xaiSpeechRequestURL` picks it) with the plain xAI headers. A non-2xx answer is the
    /// attempt's failure as `xaiStatusErr` reads it; an unknown voice (`xaiSpeechRequestError`)
    /// is the request's fault and neither rotates nor cools the credential.
    async fn media_send(
        &self,
        call: &MediaCall<'_>,
        method: wreq::Method,
        path: &str,
        body: Option<Vec<u8>>,
        speech: bool,
    ) -> Result<wire::Upstream, ExecError> {
        let credential = call.attempt.credential;
        let base = if speech {
            compact_base_url(credential)
        } else {
            chat_base_url(credential)
        };
        let url = self.url(endpoint(&base, path));
        crate::xai_url::new_request(&url)?;
        if let Some(error) = crate::xai_url::transport_refusal(&url) {
            return Err(error);
        }
        let headers = call.headers(&method, speech);
        let client = self.clients.for_credential(credential, call.cfg);
        let usage = call.request.client.usage;
        let capture = wire::Capture::on(
            usage.capture().clone(),
            credential,
            PROVIDER,
            &url,
            method.as_str(),
            headers.pairs(),
            body.as_deref().unwrap_or_default(),
        );
        let route = |_: &url::Url| {
            Ok(crate::proxy::Route {
                client: client.clone(),
                order: None,
            })
        };
        usage.round_trip_started();
        let upstream = crate::proxy::send_request(&route, method, &url, headers, body.map(Bytes::from), None).await;
        capture.sent(&upstream);
        let mut upstream = track_first_byte(upstream?, usage);
        if (200..300).contains(&upstream.status) {
            upstream.body = capture.stream(upstream.body);
            return Ok(upstream);
        }
        let (status, headers, data) = wire::read_error_body(upstream).await;
        capture.chunk(&data);
        let mut error = response::status_error(status, &data);
        if speech && error.status == 404 && !speech_model_unavailable(&data) {
            // A request scope without upstream headers is the explicit marker
            // (classify::explicit_request_scoped); Go's xAI status errors carry none.
            error.scope = FailureScope::Request;
        } else {
            error.headers = Box::new(headers);
        }
        Err(error)
    }
}

/// One media attempt's facts the upload rules and headers read.
struct MediaCall<'r> {
    attempt: &'r Attempt<'r>,
    request: &'r Request<'r>,
    /// The attempt's config, as Go binds it for the credential (`cfg.ForAPIKey()`).
    cfg: &'r Config,
}

impl MediaCall<'_> {
    /// The upload with the upstream model, then the configured payload rules, the last change to
    /// the body (Go's media finalizer: protocol `openai`, the encoded upload as the baseline).
    fn rules(&self, from: &str, mut upload: Vec<u8>) -> Vec<u8> {
        let model = parse_suffix(self.attempt.model).model_name;
        let original = upload.clone();
        // The upstream model the route resolved, where the request names one (speech does not).
        if !model.is_empty() && gj::get(&upload, "model").exists() {
            crate::openai_compat_payload::set_str_if_different(&mut upload, "model", &model);
        }
        let requested = media_exec::requested_model(self.request.validated.view().document).unwrap_or_default();
        let requested = if requested.is_empty() {
            model.as_str()
        } else {
            requested.as_str()
        };
        ck_wire::payload::apply(
            &ck_wire::payload::Rules::of(self.cfg),
            &ck_wire::payload::Request {
                target_executor: PROVIDER,
                model: &model,
                requested_model: requested,
                protocol: "openai",
                from_protocol: from,
                root: "",
                original: &original,
                request_path: self.request.client.route,
                headers: Some(self.request.client.headers),
            },
            upload,
        )
    }

    /// `applyXAIHeaders`; speech asks for any type, since TTS answers with raw audio, and a POST
    /// carries the client's idempotency key.
    fn headers(&self, method: &wreq::Method, speech: bool) -> GoHeaders {
        let (token, _) = creds(self.attempt.credential);
        let client = &self.request.client;
        let mut headers = request_headers(
            self.attempt.credential,
            (&token, false, ""),
            (client.headers, client.session),
            false,
        );
        if speech {
            headers.set("Accept", "*/*");
        } else if *method == wreq::Method::POST {
            let header = |name: &str| {
                client
                    .headers
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .map(str::trim)
                    .unwrap_or_default()
            };
            let key = Some(header("Idempotency-Key"))
                .filter(|k| !k.is_empty())
                .unwrap_or_else(|| header("x-idempotency-key"));
            if !key.is_empty() {
                headers.set("x-idempotency-key", key.to_owned());
            }
        }
        headers
    }
}
