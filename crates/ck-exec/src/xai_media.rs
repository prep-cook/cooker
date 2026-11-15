//! xAI images, videos and speech, moved out of `xai.rs` (Phase D, P6; Phase E owns it).

use bytes::Bytes;
use ck_core::config::Config;
use ck_core::credential::Credential;
use ck_core::exec::{ExecError, ExecRequest, ExecResponse, FailureScope, ResponseBody};
use ck_core::format::Format;
use ck_wire::gostr::GoStr;
use ck_wire::json as gj;
use http::HeaderMap;

use crate::openai_compat_http::{self as wire, GoHeaders};
use crate::xai::{
    PROVIDER, XaiExecutor, capture_request, chat_base_url, compact_base_url, creds, endpoint, fail_if_canceled,
    path_escape, plain_headers, scoped, track_first_byte,
};
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
