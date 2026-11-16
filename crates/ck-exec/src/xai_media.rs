//! xAI images, videos and speech, which `XaiExecutor::serve_media` runs under the execution
//! contract.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use ck_core::config::Config;
use ck_core::credential::Credential;
use ck_core::exec::{Caller, ExecError, ExecRequest, FailureScope};
use ck_wire::gostr::GoStr;
use ck_wire::json as gj;
use http::HeaderMap;

use ck_translate::codec::document::Document;
use ck_translate::codec::{self, BodyMode, Media, RequestView};
use ck_translate::{audio, images, video};
use ck_wire::thinking::parse_suffix;

use crate::media_exec::{self, MediaIngress};
use crate::openai_compat_http::{self as wire, GoHeaders};
use crate::types::{Attempt, Request, Streams, Upstream};
use crate::xai::{
    PROVIDER, XaiExecutor, capture_request, chat_base_url, compact_base_url, creds, endpoint, fail_if_canceled,
    path_escape, request_headers, scoped, track_first_byte,
};
use crate::xai_events::XaiIngress;
use crate::xai_exec;
use crate::xai_response::{self as response, text};

const IMAGES_GENERATIONS: &str = "/images/generations";
const IMAGES_EDITS: &str = "/images/edits";
const VIDEOS_GENERATIONS: &str = "/videos/generations";
const VIDEOS_EDITS: &str = "/videos/edits";
const VIDEOS_EXTENSIONS: &str = "/videos/extensions";

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
pub(crate) fn reports_usage(body: &[u8]) -> bool {
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

type Clock = Arc<dyn Fn() -> Instant + Send + Sync>;

/// The caller a cooker-issued job id belongs to (E-Q5): the client key that matched and the
/// access provider that accepted it. Process memory only; never logged or sent.
fn owner(caller: &Caller) -> String {
    format!("{}\u{0}{}", caller.provider, caller.principal)
}

/// What a job makes: a poll of one kind never finds a job of the other.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobKind {
    Image,
    Video,
    /// A deferred Chat completion (X11).
    Chat,
}

impl JobKind {
    const fn name(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Video => "video",
            Self::Chat => "chat",
        }
    }
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
    kind: JobKind,
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

/// The video, deferred image and deferred Chat jobs this process created (E-Q5, E-Q13), in
/// one table and one capacity. A job id belongs to the caller that
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

    /// A slot for a create of a `kind` job `caller` is about to send on `binding`, or `None` when
    /// the table is full of live jobs and pending creates.
    pub(crate) fn reserve(&self, caller: &Caller, kind: JobKind, binding: JobBinding) -> Option<Slot> {
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
            kind,
            binding: Some(binding),
        })
    }

    /// Where `caller`'s live `kind` job `job` is polled, or `None` when it is not theirs, of the
    /// other kind, expired or unknown.
    pub fn binding(&self, caller: &Caller, kind: JobKind, job: &str) -> Option<JobBinding> {
        let now = (self.clock)();
        let key = (owner(caller), job.trim().to_owned());
        let table = self.lock();
        let record = table.jobs.get(&key)?;
        (record.kind == kind && live(now, record)).then(|| record.binding.clone())
    }

    /// A poll's admission to `caller`'s live `kind` job, which restarts its expiry; `None` when
    /// the job is not theirs, of the other kind, expired or unknown.
    pub(crate) fn admit(&self, caller: &Caller, kind: JobKind, job: &str) -> Option<Admission> {
        let job = job.trim();
        if job.is_empty() {
            return None;
        }
        let now = (self.clock)();
        let key = (owner(caller), job.to_owned());
        let mut table = self.lock();
        let record = table
            .jobs
            .get_mut(&key)
            .filter(|record| record.kind == kind && live(now, record))?;
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
        self.reserve(caller, JobKind::Video, binding)?.created(job)
    }
}

/// A create's reserved place in the table, from before its send until its response names the
/// job. Dropped unused, as when the create fails, it frees the place.
pub(crate) struct Slot {
    jobs: VideoJobs,
    owner: String,
    kind: JobKind,
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
            kind: self.kind,
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
    /// Claims the job's charge for a report recorded where nothing can fail: true only for the
    /// first settlement that finds the job uncharged, which marks it charged.
    pub(crate) fn settle(&self) -> bool {
        self.settle_with(|| Ok::<_, std::convert::Infallible>(()))
            .unwrap_or_else(|never| match never {})
    }

    /// Charges the job with `publish`: when the job is uncharged, `publish` runs under the table
    /// lock and its success marks the job charged. A concurrent settlement waits for that
    /// outcome, so it charges the job itself when `publish` fails rather than having skipped a
    /// charge nobody makes.
    pub(crate) fn settle_with<E>(&self, publish: impl FnOnce() -> Result<(), E>) -> Result<bool, E> {
        let mut table = self.jobs.lock();
        // The admission keeps the record; a missing one charges nothing.
        let Some(record) = table.jobs.get_mut(&self.key) else {
            return Ok(false);
        };
        if record.charged {
            return Ok(false);
        }
        publish()?;
        record.charged = true;
        Ok(true)
    }
}

impl Drop for Admission {
    fn drop(&mut self) {
        if let Some(record) = self.jobs.lock().jobs.get_mut(&self.key) {
            record.in_flight = record.in_flight.saturating_sub(1);
        }
    }
}

/// The job table as a video or deferred image response's [`video::Ledger`]: an accepted job takes
/// the create's reserved slot, and a reported usage is the job's charge only when its admission
/// settles it first. Both routes settle against the same [`VideoJobs`], so one job is charged once whichever
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
    fn charge(&mut self, _: &str, publish: &mut dyn FnMut() -> codec::Result<()>) -> codec::Result<bool> {
        match &self.admission {
            Some(admission) => admission.settle_with(publish),
            None => Ok(false),
        }
    }
}

/// The 404 for a poll of a `kind` job [`VideoJobs`] does not know for this caller, in the error
/// shape OpenAI and xAI give an unknown ID. It is the request's fault: no other credential is tried.
pub(crate) fn unknown_job(kind: JobKind, job: &str) -> ExecError {
    let message = match kind {
        JobKind::Video => {
            format!("Video job '{job}' does not exist for this API key, or has expired. Create the video again.")
        }
        JobKind::Image => {
            format!("Image job '{job}' does not exist for this API key, or has expired. Create the image again.")
        }
        JobKind::Chat => {
            format!(
                "Deferred completion '{job}' does not exist for this API key, or has expired. Create the completion again."
            )
        }
    };
    let body = serde_json::json!({"error": {"message": message, "type": "invalid_request_error", "param": null, "code": null}});
    ExecError::local(404, FailureScope::Request, body.to_string())
}

/// The 503 for a create while the job table is full (E-Q5): nothing is sent, so no upstream job
/// exists. It is the request's fault for routing: no other credential is tried or cooled.
pub(crate) fn jobs_full(kind: JobKind) -> ExecError {
    let kind = kind.name();
    let message = format!("Too many {kind} jobs are being tracked. Retry after polling or waiting out existing jobs.");
    let code = format!("{kind}_jobs_full");
    let body = serde_json::json!({"error": {"message": message, "type": "server_error", "param": null, "code": code}});
    ExecError::local(503, FailureScope::Request, body.to_string())
}

impl XaiExecutor {
    /// One request whose whole response body is read: captured, timed and canceled like
    /// every xAI call. `logged` is what the capture records as the body; without it the
    /// sent body, or an empty GET, is recorded.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn send_buffered(
        &self,
        credential: &Credential,
        req: &ExecRequest,
        cfg: &Config,
        method: wreq::Method,
        url: &str,
        headers: GoHeaders,
        body: Option<Vec<u8>>,
        logged: Option<&[u8]>,
    ) -> Result<(u16, HeaderMap, Bytes), ExecError> {
        let client = self.clients.for_credential(credential, cfg);
        let route = |_: &url::Url| {
            Ok(crate::proxy::Route {
                client: client.clone(),
                order: None,
            })
        };
        let capture = capture_request(
            req,
            credential,
            method.as_str(),
            url,
            &headers,
            logged.unwrap_or_else(|| body.as_deref().unwrap_or_default()),
        );
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

/// The audio format a speech request asks for: `output_format.codec`, else `response_format`,
/// lower case; empty for neither, which is MP3.
fn speech_format(doc: &Document) -> codec::Result<String> {
    let x = doc.required(doc.root(), "x_ck")?;
    let audio = doc.required(doc.required(x, "provider_parameters")?, "audio")?;
    let format = match doc.member(audio, "output_format")? {
        Some(output) => doc.member(output, "codec")?,
        None => doc.member(audio, "response_format")?,
    };
    Ok(match format {
        Some(node) => media_exec::text(doc, node)?.trim().to_ascii_lowercase(),
        None => String::new(),
    })
}

/// The audio's media type (`speechResponseContentType`): the upstream's unless it names no
/// audio (`speechContentTypeNeedsDefault`), then the requested format's own.
fn speech_type<'a>(upstream: Option<&'a str>, format: &str) -> &'a str {
    let media = upstream.unwrap_or_default().split(';').next().unwrap_or_default();
    match media.trim().to_ascii_lowercase().as_str() {
        "" | "application/json" | "application/octet-stream" | "text/plain" => match format {
            "wav" => "audio/wav",
            "pcm" => "audio/pcm",
            _ => "audio/mpeg",
        },
        _ => upstream.unwrap_or_default(),
    }
}

/// The limit of one speech body: the audio file itself.
const SPEECH_LIMIT: u64 = 64 << 20;
/// The limit of one video job body; the video itself is a separate download.
const VIDEO_LIMIT: u64 = 1 << 20;

/// The finished file's URL a job body names (`xaiVideoContentURLFromPayload`): an absolute http(s)
/// URL with a host. Without one the content request fails as the old route's did, with 502.
fn content_url(polled: &[u8]) -> Result<String, ExecError> {
    let raw = text(&gj::get(polled, "video.url")).trim().to_owned();
    let message = if raw.is_empty() {
        "xAI video response did not include video.url"
    } else if !crate::xai::is_http_url(&raw) {
        "xAI video response included invalid video.url"
    } else {
        return Ok(raw);
    };
    Err(ExecError::local(502, FailureScope::Request, message))
}

/// The fence check of an xAI media request.
pub(crate) fn check(view: RequestView<'_>, media: Media) -> codec::Result<()> {
    match media {
        Media::ImageGenerate | Media::ImageEdit => media_exec::check_images(view, images::Destination::Xai),
        Media::ImageRetrieve => media_exec::check_image_retrieve(view),
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
                // A deferred create reserves its place in the job table before anything is sent,
                // as a video create does, so a full table sends nothing (E-Q5).
                let deferred = media_exec::images_flag(request.validated().view().document, "deferred")
                    .map_err(xai_exec::encoding)?;
                let slot = if deferred {
                    let slot = self.jobs.reserve(request.client.caller, JobKind::Image, call.binding());
                    Some(slot.ok_or_else(|| jobs_full(JobKind::Image))?)
                } else {
                    None
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
                let ingress = match slot {
                    Some(slot) => images::Ingress::deferred(
                        streams.response,
                        attempt.identity,
                        images::Deferred::Accepted,
                        None,
                        media_exec::IMAGES_LIMIT,
                        Box::new(JobLedger {
                            slot: Some(slot),
                            admission: None,
                        }),
                    ),
                    None => images::Ingress::new(
                        streams.response,
                        attempt.identity,
                        images::Wire::Xai,
                        BodyMode::Buffered,
                        media_exec::IMAGES_LIMIT,
                    ),
                }
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
                let format = speech_format(request.validated().view().document).map_err(xai_exec::encoding)?;
                let content_type = speech_type(content_type, &format);
                let ingress = audio::Ingress::new(
                    streams.response,
                    attempt.identity,
                    Some(content_type),
                    length,
                    SPEECH_LIMIT,
                )
                .map_err(xai_exec::encoding)?;
                (
                    upstream.status,
                    upstream.headers,
                    upstream.body,
                    MediaIngress::Speech(Box::new(ingress)),
                )
            }
            Media::ImageRetrieve => self.image_poll(&call, streams).await?,
            Media::VideoContent => self.video_content(&call, streams).await?,
            media => self.video(&call, streams, media).await?,
        };
        Ok(Upstream {
            status,
            headers,
            body,
            ingress: XaiIngress::Media(ingress),
        })
    }

    /// A poll of a deferred image job, admitted against the job table before anything is sent: a
    /// job this caller did not create through this process, or a video job, is refused. xAI
    /// answers 202 while the job runs and 200 with its images or its `error`.
    async fn image_poll(
        &self,
        call: &MediaCall<'_>,
        streams: Streams<'_>,
    ) -> Result<(u16, HeaderMap, crate::types::Body, MediaIngress), ExecError> {
        let job = media_exec::image_job(call.request, xai_exec::DESTINATION)?;
        let admission = self
            .jobs
            .admit(call.request.client.caller, JobKind::Image, &job)
            .ok_or_else(|| unknown_job(JobKind::Image, &job))?;
        let path = format!("/images/{}", path_escape(&job));
        let upstream = self.media_send(call, wreq::Method::GET, &path, None, false).await?;
        let phase = if upstream.status == 202 {
            images::Deferred::Pending
        } else {
            images::Deferred::Polled
        };
        let ingress = images::Ingress::deferred(
            streams.response,
            call.attempt.identity,
            phase,
            Some(&job),
            media_exec::IMAGES_LIMIT,
            Box::new(JobLedger {
                slot: None,
                admission: Some(admission),
            }),
        )
        .map_err(xai_exec::encoding)?;
        Ok((
            upstream.status,
            upstream.headers,
            upstream.body,
            MediaIngress::Images(Box::new(ingress)),
        ))
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
                Some(
                    self.jobs
                        .admit(caller, JobKind::Video, job)
                        .ok_or_else(|| unknown_job(JobKind::Video, job))?,
                ),
                None,
            ),
            (Media::VideoCreate | Media::VideoEdit | Media::VideoExtend, _) => {
                let slot = self.jobs.reserve(caller, JobKind::Video, call.binding());
                (None, Some(slot.ok_or_else(|| jobs_full(JobKind::Video))?))
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
        let mut ingress = video::Ingress::new(
            streams.response,
            call.attempt.identity,
            video::Dialect::Xai,
            phase,
            polled,
            VIDEO_LIMIT,
            Box::new(JobLedger { slot, admission }),
        )
        .map_err(xai_exec::encoding)?;
        let request = call.request.validated.view().document;
        ingress.echo(streams.response, request).map_err(xai_exec::encoding)?;
        Ok((
            upstream.status,
            upstream.headers,
            upstream.body,
            MediaIngress::Video(Box::new(ingress)),
        ))
    }

    /// A finished video's bytes, as the old route fetched them: a poll of the job, admitted against
    /// the job table like any poll, names the file's URL, which is downloaded through this
    /// credential's HTTP client and proxy without its credentials. The poll's usage is the job's
    /// charge when the table finds the job uncharged, as a poll's is.
    async fn video_content(
        &self,
        call: &MediaCall<'_>,
        streams: Streams<'_>,
    ) -> Result<(u16, HeaderMap, crate::types::Body, MediaIngress), ExecError> {
        let (_, job) = media_exec::video_upload(
            call.request,
            xai_exec::DESTINATION,
            video::Dialect::Xai,
            streams.request,
        )?;
        let job = job.ok_or_else(|| xai_exec::encoding(codec::Error::Schema))?;
        let admission = self
            .jobs
            .admit(call.request.client.caller, JobKind::Video, &job)
            .ok_or_else(|| unknown_job(JobKind::Video, &job))?;
        let path = format!("/videos/{}", path_escape(&job));
        let polled = self.media_send(call, wreq::Method::GET, &path, None, false).await?;
        let polled = crate::proxy::read_all(polled.body, VIDEO_LIMIT as usize, true).await?;
        let url = content_url(&polled)?;
        let client = self.clients.for_credential(call.attempt.credential, call.cfg);
        let usage = call.request.client.usage;
        // `HTTPStatusFromErrorOr(err, http.StatusBadGateway)`: a failed download is a 502 unless
        // the error carries its own status.
        let download = match crate::proxy::request(&client, wreq::Method::GET, &url, GoHeaders::new(), None, None).await
        {
            Ok(download) => track_first_byte(download, usage),
            Err(error) if error.scope == FailureScope::Transport => {
                let message = String::from_utf8_lossy(&error.body).into_owned();
                return Err(ExecError::local(502, FailureScope::Request, message));
            }
            Err(error) => return Err(error),
        };
        if !(200..300).contains(&download.status) {
            let status = download.status;
            let body = crate::proxy::read_all(download.body, VIDEO_LIMIT as usize, true)
                .await
                .unwrap_or_default();
            let text = String::from_utf8_lossy(&body).trim().to_owned();
            let message = if text.is_empty() {
                let reason = http::StatusCode::from_u16(status)
                    .ok()
                    .and_then(|s| s.canonical_reason())
                    .unwrap_or("");
                format!("video content download failed: {status} {reason}")
            } else {
                format!("video content download failed: {text}")
            };
            return Err(ExecError::local(status, FailureScope::Request, message));
        }
        let (mime, length) = media_exec::entity(&download.headers);
        let mut ingress = video::content::Download::new(streams.response, call.attempt.identity, &job, mime, length)
            .map_err(xai_exec::encoding)?;
        // A poll body the codec refuses still names the file; it reports no usage.
        let decoded = (|| {
            let phase = video::Phase::Job;
            let mut decoder =
                video::Decoder::new(streams.response, video::Dialect::Xai, phase, Some(&job), VIDEO_LIMIT)?;
            decoder.advance(streams.response, &polled)?;
            decoder.finish(streams.response)
        })();
        if let Ok(decoded) = decoded {
            let mut ledger = JobLedger {
                slot: None,
                admission: Some(admission),
            };
            ingress
                .charge(streams.response, &decoded, video::Dialect::Xai, &mut ledger)
                .map_err(xai_exec::encoding)?;
        }
        Ok((
            download.status,
            download.headers,
            download.body,
            MediaIngress::VideoContent(Box::new(ingress)),
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
        let url = self.url(endpoint(&media_base_url(credential, speech), path));
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

/// The base URL of a media request: the chat base URL, or for speech the compact one
/// (`xaiSpeechRequestURL`), so an OAuth credential on the CLI chat proxy sends speech to the
/// official API.
pub(crate) fn media_base_url(credential: &ck_core::credential::Credential, speech: bool) -> String {
    if speech {
        compact_base_url(credential)
    } else {
        chat_base_url(credential)
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
    /// Where a job this create makes is polled: the attempt's credential, and the model the
    /// client named, else the routed one.
    fn binding(&self) -> JobBinding {
        JobBinding {
            credential: self.attempt.credential.id.clone(),
            model: media_exec::requested_model(self.request.validated().view().document)
                .ok()
                .filter(|model| !model.is_empty())
                .unwrap_or_else(|| self.attempt.model.trim().to_owned()),
        }
    }

    /// An image or video upload naming the upstream model the route resolved, whether or not the
    /// client named one. Speech has no model member on xAI's wire.
    fn with_model(&self, mut upload: Vec<u8>) -> Vec<u8> {
        let model = parse_suffix(self.attempt.model).model_name;
        if !model.trim().is_empty() {
            crate::openai_compat_payload::set_str_if_different(&mut upload, "model", upstream_model(model.trim()));
        }
        upload
    }

    /// The configured payload rules, the last change to the body (Go's media finalizer: protocol
    /// `openai`, the encoded upload as the baseline).
    fn rules(&self, from: &str, upload: Vec<u8>) -> Vec<u8> {
        let model = parse_suffix(self.attempt.model).model_name;
        let original = upload.clone();
        let requested = media_exec::requested_model(self.request.validated().view().document).unwrap_or_default();
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

/// The model xAI is sent for a routed one: the 1.5 preview alias selects credentials under its
/// own name, but xAI receives `grok-imagine-video-1.5` (Go `canonicalXAIVideosModel`).
fn upstream_model(model: &str) -> &str {
    if model.go_eq_fold("grok-imagine-video-1.5-preview") {
        "grok-imagine-video-1.5"
    } else {
        model
    }
}
