//! xAI images, videos and speech. `XaiExecutor::serve_media` runs them under the execution
//! contract; `images`, `videos` and `speech` are the old routes' entry points until E7 moves
//! the routes onto it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use ck_core::config::Config;
use ck_core::credential::Credential;
use ck_core::exec::{Caller, ExecError, ExecRequest, ExecResponse, FailureScope, ResponseBody};
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
        self.media(credential, &req, cfg.as_ref(), wreq::Method::POST, path, payload, None)
            .await
    }

    /// `executeVideos`: generations, edits and extensions by the inbound route; any other
    /// route with a `request_id` polls `GET /videos/{request_id}`. A poll of a job this process
    /// neither created nor charged is refused before anything is sent ([`VideoJobs`]).
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
        let job = if method == wreq::Method::GET {
            let Some(admission) = self.jobs.admit(&req.caller, &request_id) else {
                return Err(unknown_job(&request_id));
            };
            VideoJob::Poll(admission)
        } else {
            let binding = JobBinding {
                credential: credential.id.clone(),
                model: req.model.trim().to_owned(),
            };
            VideoJob::Create(self.jobs.reserve(&req.caller, binding).ok_or_else(jobs_full)?)
        };
        self.media(credential, &req, cfg.as_ref(), method, &path, payload, Some(job))
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
        video: Option<VideoJob>,
    ) -> Result<ExecResponse, ExecError> {
        let logged = payload.as_slice();
        let body = (method == wreq::Method::POST).then(|| payload.clone());
        let (token, _) = creds(credential);
        let headers = plain_headers(credential, &token, false, "", req);
        // xAI documents no idempotency key for video creates (https://docs.x.ai/openapi.json,
        // `POST /v1/videos/generations`, `/edits`, `/extensions`: no header parameters), so
        // none is sent: a replayed create must not hand back a job a previous process charged.
        let url = self.url(endpoint(&chat_base_url(credential), path));
        let (status, headers, data) = self
            .send_buffered(credential, req, cfg, method, &url, headers, body, logged)
            .await?;
        if !(200..300).contains(&status) {
            let mut error = response::status_error(status, &data);
            error.headers = Box::new(headers);
            return Err(error);
        }
        // A video is a job: its usage is that of the job, which a poll repeats. A create names
        // it as the server's `video_id` reads it; a poll settles against the job its admission
        // holds, so only the first settlement that finds the job uncharged charges it.
        let admission = match video {
            None => None,
            Some(VideoJob::Poll(admission)) => Some(admission),
            Some(VideoJob::Create(slot)) => {
                let id = text(&gj::get(&data, "request_id"));
                let job = if id.trim().is_empty() {
                    text(&gj::get(&data, "id"))
                } else {
                    id
                };
                slot.created(&job)
            }
        };
        if req.usage.enabled() {
            // An image, or a create naming no job, has nothing a later report could repeat.
            if reports_usage(&data) && admission.as_ref().is_none_or(Admission::settle) {
                // The top-level usage of an image or video body, read as a Responses body's.
                req.usage.response_body(Format::Codex, &data);
            } else {
                // The body without its usage: the model, and a record with no count.
                let mut quiet = data.to_vec();
                gj::delete(&mut quiet, "usage");
                req.usage.response_body(Format::Codex, &quiet);
            }
        }
        Ok(ExecResponse {
            status: 200,
            headers,
            body: ResponseBody::Buffered(data),
        })
    }
}

/// The usage members accounting records from an image or video body: the charge, and every
/// token count the server's Responses usage reader records (xAI images and videos reference).
const BILLED: &[&str] = &[
    "usage.cost_in_usd_ticks",
    "usage.cost_in_nano_usd",
    "usage.prompt_tokens",
    "usage.input_tokens",
    "usage.completion_tokens",
    "usage.output_tokens",
    "usage.total_tokens",
    "usage.prompt_tokens_details.cached_tokens",
    "usage.input_tokens_details.cached_tokens",
    "usage.prompt_tokens_details.cache_write_tokens",
    "usage.prompt_tokens_details.cache_creation_tokens",
    "usage.input_tokens_details.cache_write_tokens",
    "usage.input_tokens_details.cache_creation_tokens",
    "usage.completion_tokens_details.reasoning_tokens",
    "usage.output_tokens_details.reasoning_tokens",
];

/// Whether `body` reports a billed value: an integer, zero included, in one of [`BILLED`]. An
/// absent, null, non-integer or empty usage reports nothing, so it does not spend a job's charge
/// (E-Q13).
fn reports_usage(body: &[u8]) -> bool {
    BILLED.iter().any(|path| {
        let value = gj::get(body, path);
        let raw = std::str::from_utf8(value.raw()).unwrap_or_default();
        value.kind == gj::Kind::Number && (raw.parse::<u64>().is_ok() || raw.parse::<i64>().is_ok())
    })
}

/// How long a job is remembered after its last create or poll (E-Q5).
const JOB_TTL: Duration = Duration::from_secs(3 * 3600);

/// The most jobs and pending creates the table holds (E-Q5). A create beyond it is refused with
/// 503 before anything is sent, so no upstream job exists that cooker could not track.
pub(crate) const JOB_CAPACITY: usize = 4096;

/// What a video request is to its job: a create, which holds a reserved slot until its response
/// names the job, or a poll admitted against it.
pub(crate) enum VideoJob {
    Create(Slot),
    Poll(Admission),
}

type Clock = Arc<dyn Fn() -> Instant + Send + Sync>;

/// The caller a cooker-issued job id belongs to (E-Q5): the client key that matched and the
/// access provider that accepted it. Process memory only; never logged or sent.
fn owner(caller: &Caller) -> String {
    format!("{}\u{0}{}", caller.provider, caller.principal)
}

/// The credential and the model a job was created on, which every poll of it is pinned to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobBinding {
    pub credential: String,
    pub model: String,
}

/// Whether `record` was created or polled within [`JOB_TTL`] of `now`.
fn live(now: Instant, record: &Job) -> bool {
    now.saturating_duration_since(record.at) < JOB_TTL
}

/// One job's record: whether it was charged, when it was created or last polled, how many
/// requests hold an [`Admission`] to it, and where its polls go.
struct Job {
    charged: bool,
    at: Instant,
    in_flight: usize,
    binding: JobBinding,
}

#[derive(Default)]
struct Table {
    /// Keyed by (owner, job id): another caller's id is not found.
    jobs: HashMap<(String, String), Job>,
    /// Creates sent or about to be sent whose response has not named the job yet.
    pending: usize,
}

/// The video jobs this process created (E-Q5, E-Q13). A job id belongs to the caller that
/// created it; any other caller, an expired id or one this process never issued is not found.
/// A job is charged at the first poll that reports usage and never again. A job missing here
/// may already have been charged by an earlier process, so its poll is refused rather than risk
/// a second charge. A record unpolled for [`JOB_TTL`] admits no new poll, but stays while any
/// admitted request holds it, so expiry can only refuse a poll, never erase a charge another
/// admitted poll settles against. The table holds at most `capacity` jobs and pending creates.
// ponytail: process memory, so a job created before a restart cannot be polled through cooker.
// No store on this path outlives the process: the usage queue is in memory, and SQLite holds
// configuration and credentials only. A durable job ledger (F06-1) lifts this; its store awaits
// an owner decision.
#[derive(Clone)]
pub struct VideoJobs {
    table: Arc<Mutex<Table>>,
    clock: Clock,
    capacity: usize,
}

impl Default for VideoJobs {
    fn default() -> Self {
        Self::with_clock(Arc::new(Instant::now), JOB_CAPACITY)
    }
}

impl VideoJobs {
    pub(crate) fn with_clock(clock: Clock, capacity: usize) -> Self {
        Self {
            table: Arc::default(),
            clock,
            capacity,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Table> {
        self.table.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// A slot for a create `caller` is about to send on `binding`, or `None` when the table is
    /// full of live jobs and pending creates.
    pub(crate) fn reserve(&self, caller: &Caller, binding: JobBinding) -> Option<Slot> {
        let now = (self.clock)();
        let mut table = self.lock();
        if table.jobs.len() + table.pending >= self.capacity {
            table.jobs.retain(|_, record| record.in_flight > 0 || live(now, record));
        }
        if table.jobs.len() + table.pending >= self.capacity {
            return None;
        }
        table.pending += 1;
        Some(Slot {
            jobs: self.clone(),
            owner: owner(caller),
            binding: Some(binding),
        })
    }

    /// Where `caller`'s live job `job` is polled, or `None` when it is not theirs, expired or
    /// unknown.
    pub fn binding(&self, caller: &Caller, job: &str) -> Option<JobBinding> {
        let now = (self.clock)();
        let key = (owner(caller), job.trim().to_owned());
        let table = self.lock();
        let record = table.jobs.get(&key)?;
        live(now, record).then(|| record.binding.clone())
    }

    /// A poll's admission to `caller`'s live job, which restarts its expiry; `None` when the job
    /// is not theirs, expired or unknown.
    pub(crate) fn admit(&self, caller: &Caller, job: &str) -> Option<Admission> {
        let job = job.trim();
        if job.is_empty() {
            return None;
        }
        let now = (self.clock)();
        let key = (owner(caller), job.to_owned());
        let mut table = self.lock();
        let record = table.jobs.get_mut(&key).filter(|record| live(now, record))?;
        record.at = now;
        record.in_flight += 1;
        Some(Admission {
            jobs: self.clone(),
            key,
        })
    }
}

#[cfg(test)]
impl VideoJobs {
    /// Records `job` as `caller` created it on no particular credential, as a create's response
    /// does.
    pub(crate) fn created(&self, caller: &Caller, job: &str) -> Option<Admission> {
        let binding = JobBinding {
            credential: String::new(),
            model: String::new(),
        };
        self.reserve(caller, binding)?.created(job)
    }
}

/// A create's reserved place in the table, from before its send until its response names the
/// job. Dropped unused, as when the create fails, it frees the place.
pub(crate) struct Slot {
    jobs: VideoJobs,
    owner: String,
    /// Taken when the slot becomes a job.
    binding: Option<JobBinding>,
}

impl Slot {
    /// Records the job the create's response named in this slot; its admission settles a usage
    /// the create reported. `None` for a response naming no job, which frees the slot.
    pub(crate) fn created(mut self, job: &str) -> Option<Admission> {
        let job = job.trim();
        if job.is_empty() {
            return None;
        }
        let binding = self.binding.take()?;
        let now = (self.jobs.clock)();
        let key = (self.owner.clone(), job.to_owned());
        let mut table = self.jobs.lock();
        table.pending = table.pending.saturating_sub(1);
        // A job id the upstream repeats keeps the charge it already has.
        let record = table.jobs.entry(key.clone()).or_insert(Job {
            charged: false,
            at: now,
            in_flight: 0,
            binding: binding.clone(),
        });
        record.at = now;
        record.binding = binding;
        record.in_flight += 1;
        Some(Admission {
            jobs: self.jobs.clone(),
            key,
        })
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        if self.binding.is_some() {
            let mut table = self.jobs.lock();
            table.pending = table.pending.saturating_sub(1);
        }
    }
}

/// One request's hold on a job's record: the record outlives expiry until every admission to it
/// is dropped.
pub(crate) struct Admission {
    jobs: VideoJobs,
    key: (String, String),
}

impl Admission {
    /// Claims the job's charge: true only for the first settlement that finds the job uncharged,
    /// which marks it charged.
    pub(crate) fn settle(&self) -> bool {
        let mut table = self.jobs.lock();
        // The admission keeps the record; a missing one charges nothing.
        let Some(record) = table.jobs.get_mut(&self.key) else {
            return false;
        };
        if record.charged {
            return false;
        }
        record.charged = true;
        true
    }

    /// Undoes this admission's [`Admission::settle`]: the job is uncharged again.
    pub(crate) fn release(&self) {
        if let Some(record) = self.jobs.lock().jobs.get_mut(&self.key) {
            record.charged = false;
        }
    }
}

impl Drop for Admission {
    fn drop(&mut self) {
        if let Some(record) = self.jobs.lock().jobs.get_mut(&self.key) {
            record.in_flight = record.in_flight.saturating_sub(1);
        }
    }
}

/// The job table as a video response's [`video::Ledger`]: an accepted job takes the create's
/// reserved slot, and a reported usage is the job's charge only when its admission settles it
/// first. Both routes settle against the same [`VideoJobs`], so one job is charged once whichever
/// route polls it.
struct JobLedger {
    /// A create's reserved slot, until the response names the job.
    slot: Option<Slot>,
    /// The poll's admission, or the accepted job's.
    admission: Option<Admission>,
}

impl video::Ledger for JobLedger {
    fn accepted(&mut self, job: &str) {
        self.admission = self.slot.take().and_then(|slot| slot.created(job));
    }

    /// The codec binds a poll's answer to the polled job, so the admission is the job's.
    fn charge(&mut self, _: &str) -> bool {
        self.admission.as_ref().is_some_and(Admission::settle)
    }

    /// Called only after this ledger's own [`video::Ledger::charge`] claimed the job.
    fn release(&mut self, _: &str) {
        if let Some(admission) = &self.admission {
            admission.release();
        }
    }
}

/// The 404 for a poll of a job [`VideoJobs`] does not know for this caller, in the error shape
/// OpenAI and xAI give an unknown ID. It is the request's fault: no other credential is tried.
fn unknown_job(job: &str) -> ExecError {
    let message = format!("Video job '{job}' does not exist for this API key, or has expired. Create the video again.");
    let body = serde_json::json!({"error": {"message": message, "type": "invalid_request_error", "param": null, "code": null}});
    ExecError::local(404, FailureScope::Request, body.to_string())
}

/// The 503 for a create while the job table is full (E-Q5): nothing is sent, so no upstream job
/// exists. It is the request's fault for routing: no other credential is tried or cooled.
fn jobs_full() -> ExecError {
    let message = "Too many video jobs are being tracked. Retry after polling or waiting out existing jobs.";
    let body = serde_json::json!({"error": {"message": message, "type": "server_error", "param": null, "code": "video_jobs_full"}});
    ExecError::local(503, FailureScope::Request, body.to_string())
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
                        Some(call.rules("openai", call.with_model(upload))),
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
        // A poll is admitted against the job table before anything is sent: a job this caller did
        // not create through this process is refused, as on the buffered route. A create reserves
        // its place first, so a full table sends nothing.
        let caller = call.request.client.caller;
        let (admission, slot) = match (media, &job) {
            (Media::VideoRetrieve, Some(job)) => (
                Some(self.jobs.admit(caller, job).ok_or_else(|| unknown_job(job))?),
                None,
            ),
            (Media::VideoCreate | Media::VideoEdit | Media::VideoExtend, _) => {
                let binding = JobBinding {
                    credential: call.attempt.credential.id.clone(),
                    model: media_exec::requested_model(call.request.validated.view().document)
                        .ok()
                        .filter(|model| !model.is_empty())
                        .unwrap_or_else(|| call.attempt.model.trim().to_owned()),
                };
                (None, Some(self.jobs.reserve(caller, binding).ok_or_else(jobs_full)?))
            }
            _ => (None, None),
        };
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
            Some(upload) if method == wreq::Method::POST => Some(call.rules("openai", call.with_model(upload))),
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
            Box::new(JobLedger { slot, admission }),
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
        let headers = call.headers(speech);
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
    /// An image or video upload naming the upstream model the route resolved, whether or not the
    /// client named one. Speech has no model member on xAI's wire.
    fn with_model(&self, mut upload: Vec<u8>) -> Vec<u8> {
        let model = parse_suffix(self.attempt.model).model_name;
        if !model.trim().is_empty() {
            crate::openai_compat_payload::set_str_if_different(&mut upload, "model", model.trim());
        }
        upload
    }

    /// The configured payload rules, the last change to the body (Go's media finalizer: protocol
    /// `openai`, the encoded upload as the baseline).
    fn rules(&self, from: &str, upload: Vec<u8>) -> Vec<u8> {
        let model = parse_suffix(self.attempt.model).model_name;
        let original = upload.clone();
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

    /// `applyXAIHeaders`; speech asks for any type, since TTS answers with raw audio. xAI documents
    /// no idempotency key for video creates (https://docs.x.ai/openapi.json: no header
    /// parameters), so none is sent: a replayed create must not hand back a job a previous
    /// process charged.
    fn headers(&self, speech: bool) -> GoHeaders {
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
        }
        headers
    }
}
