//! The provider-neutral attempt loop every inference route runs (Go
//! `BaseAPIHandler.Execute*WithAuthManager` plus `Manager.Execute*`).
//!
//! Model to providers through the registry, credential selection across all of them,
//! per-credential alias pools, request-scoped rules, retry rounds and stream bootstrap.
//! Routes only parse their request and render the [`Done`] or [`Failure`].

use std::sync::Arc;
use std::time::Duration;

use axum::http::HeaderMap;
use bytes::{Bytes, BytesMut};
use ck_core::config::Config;
use ck_core::exec::{Caller, ExecError, ExecRequest, ExecStream, Operation, ResponseBody};
use ck_core::format::Format;
use ck_wire::gostr::GoStr;
use futures_util::StreamExt;

use crate::http::gojson;
use crate::http::respond;
use crate::runtime::state::{AcquireError, Completing, Lease, Outcome, Runtime, Selection};
use crate::scheduling::registry::{self, AliasResult, Registry};
use crate::scheduling::scheduler::{Policy, canonical_model};
use crate::scheduling::{classify, lcp};

mod attempt;

use attempt::{Arm, Ran};
pub use attempt::{Arms, Codec};

/// One client request, already parsed by its route.
#[derive(Clone)]
pub struct Call {
    /// Format of the client request (Go entry protocol).
    pub entry: Format,
    /// Format the client expects back.
    pub response: Format,
    pub operation: Operation,
    /// The model exactly as the client named it (gjson `String()`, untrimmed).
    pub model: String,
    pub body: Bytes,
    pub stream: bool,
    pub alt: Option<String>,
    pub headers: HeaderMap,
    pub caller: Caller,
    /// Skip registry routing and use this provider (Interactions agents).
    pub forced_provider: Option<String>,
    /// Model used for credential selection when it differs from `model`.
    pub selection_model: Option<String>,
    /// Go `execution_session_id`: set by transports with long-lived sessions.
    pub execution_session: Option<String>,
    /// The matched route ([`route_path`]).
    pub request_path: String,
    /// The downstream peer (Go `Request.RemoteAddr`), when the listener provides it.
    pub peer: Option<std::net::SocketAddr>,
    /// A turn of a long-lived downstream session (the Responses WebSocket), `None` for
    /// ordinary HTTP requests.
    pub turn: Option<Arc<SessionTurn>>,
    /// A media route (Go's `openai-image` / `openai-video` / `openai-speech` handler
    /// types), `None` for inference routes.
    pub media: Option<Arc<Media>>,
}

/// How a media route runs through [`run`]: which executor operation serves it, and the
/// same credential binding a [`SessionTurn`] has (`WithPinnedAuthID`,
/// `WithSelectedAuthIDCallback`), which the video routes use to keep a job on the
/// credential that created it.
pub struct Media {
    pub kind: MediaKind,
    /// Only this credential may serve the call.
    pub pinned: Option<String>,
    /// Called with each credential right before it is attempted.
    pub on_selected: Option<OnSelected>,
    /// The client gets an event stream (Go's image streams, also when emulated from a
    /// buffered call): until the result renders, [`serve`] holds the connection with
    /// `: keep-alive` frames every `requests.streaming.keepalive-seconds`
    /// (`waitImagesStreamExecution`) instead of the non-stream keep-alive. A failure that
    /// renders after that commit is replaced by the response's [`SseError`] event.
    pub sse: bool,
    /// `WithDisallowFreeAuth` (routed images): Codex credentials on the free plan are
    /// never selected (Go `isFreeCodexAuth`), in any round.
    pub disallow_free: bool,
}

/// The `event: error` frame a route attaches to an error response (as a response
/// extension), for [`serve`] to write instead when the event stream already committed
/// (`writeImagesStreamErrorEvent`).
#[derive(Clone)]
pub struct SseError(pub Bytes);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MediaKind {
    /// [`ck_exec::Executors::images`]; image-only models are allowed
    /// (`ExecuteImageWithAuthManager`).
    Images,
    /// [`ck_exec::Executors::videos`].
    Videos,
    /// [`ck_exec::Executors::speech`]; speech-only models are allowed, and no
    /// non-stream keep-alive runs: its newlines would prefix the audio.
    Speech,
}

impl Call {
    /// `WithPinnedAuthID` of a session turn or media call.
    pub(crate) fn pinned(&self) -> Option<String> {
        self.turn
            .as_ref()
            .and_then(|t| t.pinned.clone())
            .or_else(|| self.media.as_ref().and_then(|m| m.pinned.clone()))
    }

    /// `WithSelectedAuthIDCallback` of a session turn or media call.
    fn on_selected(&self) -> Option<&OnSelected> {
        self.turn
            .as_ref()
            .and_then(|t| t.on_selected.as_ref())
            .or_else(|| self.media.as_ref().and_then(|m| m.on_selected.as_ref()))
    }
}

/// How a session transport runs one turn through [`run`] (Go
/// `ExecuteStreamWithAuthManager` with `WithPinnedAuthID`,
/// `WithSelectedAuthIDCallback` and the execution session).
pub struct SessionTurn {
    /// Passed to [`ck_exec::Executors::execute_in_session`] for every attempt.
    pub session: ck_core::exec::ExecSession,
    /// Only this credential may serve the turn (`WithPinnedAuthID`); every retry round
    /// excludes all others.
    pub pinned: Option<String>,
    /// Called with each credential right before it is attempted
    /// (`WithSelectedAuthIDCallback`); the last call before success is the serving one.
    pub on_selected: Option<OnSelected>,
}

/// The [`SessionTurn::on_selected`] callback.
pub type OnSelected = Box<dyn Fn(&ck_core::credential::Credential) + Send + Sync>;

/// The downstream peer address, as the listener records it (`ConnectInfo`).
pub type Peer = Option<axum::Extension<axum::extract::ConnectInfo<std::net::SocketAddr>>>;

pub fn peer(peer: Peer) -> Option<std::net::SocketAddr> {
    peer.map(|axum::Extension(axum::extract::ConnectInfo(addr))| addr)
}

/// Go `RequestPathMetadataKey` (gin `FullPath()`): the matched route template in gin's
/// spelling (`/v1beta/models/*action`), or the URI path when no route matched.
pub fn route_path(matched: Option<&axum::extract::MatchedPath>, uri: &axum::http::Uri) -> String {
    let Some(matched) = matched else {
        return uri.path().to_owned();
    };
    matched
        .as_str()
        .split('/')
        .map(
            |segment| match segment.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
                Some(name) if name.starts_with('*') => name.to_owned(),
                Some(name) => format!(":{name}"),
                None => segment.to_owned(),
            },
        )
        .collect::<Vec<_>>()
        .join("/")
}

pub enum Done {
    Buffered {
        headers: HeaderMap,
        body: Bytes,
    },
    /// A stream whose first event already arrived. `rest` reports its lease outcome.
    Stream {
        headers: HeaderMap,
        first: Option<Bytes>,
        rest: ExecStream,
    },
}

#[derive(Debug, Clone)]
pub enum Failure {
    /// An executor or upstream error, rendered by the route's error shape.
    Exec(ExecError),
    /// No provider registered the model (Go `getRequestDetails`).
    UnknownModel(String),
    /// An image-only model on a non-image route.
    ImageOnly(String),
    /// A speech-only model on a non-speech route (Go `validateSpeechOnlyModel`).
    SpeechOnly(String),
    /// `auth_not_found` / `auth_unavailable`, enriched like Go.
    Unavailable {
        code: &'static str,
        providers: Vec<String>,
        model: String,
        cause: Option<String>,
        retry_after: Option<Duration>,
    },
    /// Every candidate is cooling down for this model.
    Cooldown {
        model: String,
        provider: String,
        wait: Duration,
        cause: Option<String>,
    },
}

impl Failure {
    pub fn status(&self) -> u16 {
        match self {
            Failure::Exec(e) => classify::response_status(e),
            Failure::UnknownModel(_) | Failure::SpeechOnly(_) => 400,
            Failure::ImageOnly(_) | Failure::Unavailable { .. } => 503,
            Failure::Cooldown { .. } => 429,
        }
    }

    /// Go `err.Error()`: what route error writers render.
    pub fn text(&self) -> String {
        match self {
            Failure::Exec(e) => classify::error_text(e),
            Failure::UnknownModel(model) => {
                let message = gojson::sjson_string(&format!("unknown provider for model {model}"));
                format!(
                    r#"{{"error":{{"message":{message},"type":"invalid_request_error","code":"model_not_found","param":"model"}}}}"#
                )
            }
            Failure::ImageOnly(model) => format!(
                "model {} is only supported on /v1/images/generations and /v1/images/edits",
                route_base_name(model)
            ),
            Failure::SpeechOnly(model) => format!(
                "model {} is only supported on /v1/audio/speech and /v1/tts",
                route_base_name(model)
            ),
            Failure::Unavailable {
                code,
                providers,
                model,
                cause,
                ..
            } => unavailable_text(code, "no auth available", providers, model, cause.as_deref()),
            Failure::Cooldown {
                model,
                provider,
                wait,
                cause,
            } => {
                let shown = if model.is_empty() { "requested model" } else { model };
                let mut message = format!("All credentials for model {shown} are cooling down");
                if !provider.is_empty() {
                    message.push_str(&format!(" via provider {provider}"));
                }
                let display = if !wait.is_zero() && *wait < Duration::from_secs(1) {
                    Duration::from_secs(1)
                } else {
                    Duration::from_secs((wait.as_secs_f64()).round() as u64)
                };
                let mut body = serde_json::Map::new();
                body.insert("code".into(), "model_cooldown".into());
                body.insert("model".into(), model.clone().into());
                body.insert("reset_time".into(), gojson::duration(display).into());
                body.insert("reset_seconds".into(), ceil_seconds(*wait).into());
                if !provider.is_empty() {
                    body.insert("provider".into(), provider.clone().into());
                }
                if let Some(summary) = cause.as_deref().map(upstream_summary).filter(|s| !s.is_empty()) {
                    message.push_str(&format!(" (last error: {summary})"));
                    body.insert("last_upstream_error".into(), summary.into());
                }
                body.insert("message".into(), message.into());
                gojson::sorted(&serde_json::json!({ "error": body }))
            }
        }
    }

    /// `Retry-After` from Go's `SafeResponseHeaders` (scheduler errors only).
    pub fn retry_after(&self) -> Option<u64> {
        match self {
            Failure::Cooldown { wait, .. } => Some(ceil_seconds(*wait)),
            Failure::Unavailable {
                retry_after: Some(wait),
                ..
            } if !wait.is_zero() => Some(ceil_seconds(*wait).max(1)),
            _ => None,
        }
    }

    /// An error the route returns untouched (claude_executor_fast_error.go).
    pub fn direct(&self) -> Option<&ExecError> {
        match self {
            Failure::Exec(e) if e.direct => Some(e),
            _ => None,
        }
    }
}

/// Go `enrichAuthSelectionError`: a selection error's `code: message` text with the
/// providers, the model and the last upstream error.
fn unavailable_text(code: &str, base: &str, providers: &[String], model: &str, cause: Option<&str>) -> String {
    let providers = if providers.is_empty() {
        "unknown".into()
    } else {
        providers.join(",")
    };
    let model = if gojson::trim(model).is_empty() {
        "unknown"
    } else {
        gojson::trim(model)
    };
    let mut detail = match cause
        .map(upstream_summary)
        .filter(|s| !s.is_empty() && !base.contains(s.as_str()))
    {
        Some(summary) => format!("{base} (providers={providers}, model={model}; last upstream error: {summary})"),
        None => format!("{base} (providers={providers}, model={model})"),
    };
    if format!(",{providers},").contains(",claude,") {
        detail.push_str("; check Claude auth/key session and cooldown state via /v0/management/auth-files");
    }
    format!("{code}: {detail}")
}

fn ceil_seconds(d: Duration) -> u64 {
    d.as_secs() + u64::from(d.subsec_nanos() > 0)
}

/// Go `ExtractUpstreamErrorSummary`: the upstream error's code and message, sanitized.
pub fn upstream_summary(raw: &str) -> String {
    let raw = raw.trim();
    if raw.is_empty() {
        return String::new();
    }
    let json_part = match raw.find(": {") {
        Some(i) if i < 50 => raw[i + 2..].trim(),
        _ => raw,
    };
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(json_part) {
        let s = |v: Option<&serde_json::Value>| gojson::trim(&gojson::gjson_string(v)).to_owned();
        let (mut code, mut message) = (String::new(), String::new());
        match v.get("error") {
            Some(e @ serde_json::Value::Object(_)) => {
                code = s(e.get("code"));
                if code.is_empty() {
                    code = s(e.get("type"));
                }
                message = s(e.get("message"));
            }
            Some(serde_json::Value::String(m)) => message = m.trim().to_owned(),
            _ => {}
        }
        if code.is_empty() && message.is_empty() {
            code = s(v.get("code"));
            if code.is_empty() {
                code = s(v.get("type"));
            }
            message = s(v.get("message"));
        }
        let summary = match (code.is_empty(), message.is_empty()) {
            (false, false) if code.go_eq_fold(&message) || message.go_lower().contains(&code.go_lower()) => message,
            (false, false) => format!("{code}: {message}"),
            (true, false) => message,
            (false, true) => code,
            (true, true) => String::new(),
        };
        if !summary.is_empty() {
            return crate::inference::sanitize::summary(&summary);
        }
    }
    crate::inference::sanitize::summary(raw)
}

/// Go `routeModelBaseName`: the part after the last `/`, unless that is empty.
fn route_base_name(model: &str) -> &str {
    let model = model.trim();
    model
        .rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or(model)
        .trim()
}

/// Go `validateImageOnlyModel` and `validateSpeechOnlyModel` on a suffix-free model:
/// image-only models serve only the image routes, speech-only models only the speech
/// routes.
fn check_route_only(base: &str, call: &Call) -> Result<(), RunError> {
    let name = route_base_name(base).go_lower();
    let kind = call.media.as_ref().map(|m| m.kind);
    if IMAGE_ONLY.contains(&name.as_str()) && kind != Some(MediaKind::Images) {
        return Err(Failure::ImageOnly(base.to_owned()).into());
    }
    if SPEECH_ONLY.contains(&name.as_str()) && kind != Some(MediaKind::Speech) {
        return Err(Failure::SpeechOnly(base.to_owned()).into());
    }
    Ok(())
}

/// Go `isXAISpeechOnlyModel`.
const SPEECH_ONLY: [&str; 2] = ["grok-tts", "grok-voice-tts-1.0"];

const IMAGE_ONLY: [&str; 8] = [
    "gpt-image-1.5",
    "gpt-image-2",
    "gpt-image-2.5-flare",
    "gpt-image-2.5-sunburst",
    "gpt-image-2.5",
    "grok-imagine-image",
    "grok-imagine-image-quality",
    "grok-imagine-image-2.0",
];

/// Go `getRequestDetails`: resolve `auto`, find the providers, keep the suffix.
fn route(rt: &Runtime, registry: &Registry, call: &Call) -> Result<(Vec<String>, String), RunError> {
    if let Some(provider) = &call.forced_provider {
        let model = gojson::trim(&call.model);
        check_route_only(gojson::trim(canonical_model_raw(model)), call)?;
        return Ok((vec![provider.clone()], model.to_owned()));
    }
    let model = call.model.as_str();
    let base = match model.rfind('(') {
        Some(i) if model.ends_with(')') => &model[..i],
        _ => model,
    };
    let resolved = if base == "auto" {
        let first = registry
            .resolve_auto(|client, model| rt.suspension(client, model))
            .unwrap_or_else(|| "auto".into());
        format!("{first}{}", &model[base.len()..])
    } else {
        model.to_owned()
    };
    let base = gojson::trim(canonical_model_raw(&resolved)).to_owned();
    check_route_only(&base, call)?;
    let mut providers = registry.providers(&base);
    if providers.is_empty() && base != resolved {
        providers = registry.providers(&resolved);
    }
    if providers.is_empty() {
        return Err(Failure::UnknownModel(call.model.clone()).into());
    }
    adjust_for_entry(call.entry, &mut providers);
    Ok((providers, resolved))
}

/// Go `adjustExecutionProvidersForEntryProtocol`.
fn adjust_for_entry(entry: Format, providers: &mut Vec<String>) {
    match entry {
        Format::Interactions => {
            if let Some(i) = providers.iter().position(|p| p == "gemini-interactions") {
                let p = providers.remove(i);
                providers.insert(0, p);
            }
        }
        Format::OpenAI | Format::OpenAIResponse | Format::Claude | Format::Gemini => {}
        _ => providers.retain(|p| p != "gemini-interactions"),
    }
}

/// `thinking.ParseSuffix(model).ModelName` without trimming.
fn canonical_model_raw(model: &str) -> &str {
    match model.rfind('(') {
        Some(i) if model.ends_with(')') => &model[..i],
        _ => model,
    }
}

/// Runs `call` and renders the result, adding Go's `X-Ck-Trace-Id` (selection time,
/// the selected credential's auth index and a request ID) once a credential was picked.
///
/// Non-stream generate calls run under Go's `StartNonStreamingKeepAlive`: when
/// `requests.nonstream-keepalive-interval` is set and no result arrived within one
/// interval, the response commits as 200 `application/json` and a `\n` goes out every
/// interval until the call finishes; the rendered body follows. The rendered status and
/// headers are lost then, as in Go, where they are written after the first keep-alive
/// flush. Media calls marked [`Media::sse`] hold with event-stream keep-alives instead.
pub async fn serve<F, Fut>(rt: &Arc<Runtime>, call: Call, render: F) -> axum::response::Response
where
    F: FnOnce(Result<Done, Failure>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = axum::response::Response> + Send + 'static,
{
    let sse = call.media.as_ref().is_some_and(|m| m.sse);
    let speech = call.media.as_ref().is_some_and(|m| m.kind == MediaKind::Speech);
    let interval = if sse {
        crate::http::respond::keepalive(&rt.config())
    } else {
        nonstream_keepalive(&call, &rt.config())
    };
    let trace = Arc::new(Trace::with_request_id(crate::observability::api::current_request_id()));
    let mut run = Box::pin({
        let (rt, trace) = (rt.clone(), trace.clone());
        async move {
            let result = run_with_bootstrap_retries(&rt, call, &trace).await;
            let cfg = rt.config();
            let upstream = upstream_headers(&cfg, &result);
            let result = match result {
                // Go's speech handler reads the downstream headers the execution hands
                // back (`downstreamHeadersAfterInterceptors`): the final filtered headers
                // under passthrough, else none.
                Ok(Done::Buffered { headers, body }) if speech => {
                    let headers = if respond::passthrough_headers(&cfg) {
                        respond::filter_upstream_headers(&headers)
                    } else {
                        HeaderMap::new()
                    };
                    Ok(Done::Buffered { headers, body })
                }
                result => result,
            };
            (result, upstream)
        }
    });
    // Upstream headers and the trace ID go on a rendered response; once a keep-alive
    // committed the headers they are lost, as in Go.
    let traced = {
        let (trace, rt) = (trace.clone(), rt.clone());
        move |mut response: axum::response::Response, upstream: Option<Upstream>| {
            // The same config snapshot decides passthrough for both header sources.
            match upstream {
                Some(Upstream::Success(headers)) => respond::write_upstream_headers(response.headers_mut(), &headers),
                Some(Upstream::Error(headers)) => respond::write_error_headers(response.headers_mut(), &headers),
                None => {}
            }
            if let Some(value) = trace.header() {
                response.headers_mut().insert("x-ck-trace-id", value);
            }
            // The model a model router chose, and the provider that served (cooker only).
            // Both are sent only when a router or `routing.providers` is in play, so a config
            // without them answers exactly as Go does.
            let routed_model = trace.routed_model();
            let routed = routed_model.is_some() || rt.policy().providers.is_set();
            if let Some(value) = routed_model.and_then(|m| axum::http::HeaderValue::from_str(&m).ok()) {
                response.headers_mut().insert("x-ck-routed-model", value);
            }
            if let Some(value) = trace
                .routed_provider(&rt)
                .filter(|_| routed)
                .and_then(|p| axum::http::HeaderValue::from_str(&p).ok())
            {
                response.headers_mut().insert("x-ck-routed-provider", value);
            }
            response
        }
    };
    let Some(interval) = interval else {
        let (result, upstream) = run.await;
        return traced(render(result).await, upstream);
    };
    tokio::select! {
        biased;
        (result, upstream) = &mut run => return traced(render(result).await, upstream),
        () = tokio::time::sleep(interval) => {}
    }
    // Go's handlers stop the keep-alive as soon as Execute returns: no beats while the
    // result renders (the video download, for one).
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    enum Event {
        Tick,
        Done(Result<Done, Failure>),
    }
    let events = futures_util::stream::unfold(Some((run, ticks)), |state| async move {
        let (mut run, mut ticks) = state?;
        tokio::select! {
            biased;
            (result, _) = &mut run => Some((Event::Done(result), None)),
            _ = ticks.tick() => Some((Event::Tick, Some((run, ticks)))),
        }
    });
    let beat = Bytes::from_static(if sse { b": keep-alive\n\n" } else { b"\n" });
    let once = |bytes: Bytes| futures_util::stream::iter([Ok::<_, axum::Error>(bytes)]);
    let first = once(beat.clone());
    let mut render = Some(render);
    let body = first.chain(events.flat_map(move |event| match event {
        Event::Tick => once(beat.clone()).left_stream(),
        Event::Done(result) => {
            let render = render.take().expect("the run finishes once");
            let rendered = futures_util::stream::once(render(result));
            let body = rendered.flat_map(move |response| match response.extensions().get::<SseError>() {
                Some(SseError(event)) if sse => once(event.clone()).left_stream(),
                _ => response.into_body().into_data_stream().right_stream(),
            });
            body.right_stream()
        }
    }));
    let body = axum::body::Body::from_stream(body);
    let mut response = if sse {
        crate::http::respond::sse(body)
    } else {
        let mut response = axum::response::Response::new(body);
        response.headers_mut().insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/json"),
        );
        response
    };
    let headers = response.headers_mut();
    // Go cpa_trace.go applies the trace ID when the first keep-alive commits headers.
    if let Some(value) = trace.header() {
        headers.insert("x-ck-trace-id", value);
    }
    response
}

/// Upstream response headers bound for the client under `requests.passthrough-headers`.
enum Upstream {
    /// Go `downstreamHeadersFromExecutor`, written by `WriteUpstreamHeaders`.
    Success(HeaderMap),
    /// Go `ErrorMessage.Addon`, written by `WriteErrorResponse`.
    Error(HeaderMap),
}

/// An error contributes only its [`ExecError::addon`] (Go `executionErrorMessage` reads
/// `Headers()`); speech errors, like most, carry none.
fn upstream_headers(cfg: &Config, result: &Result<Done, Failure>) -> Option<Upstream> {
    if !respond::passthrough_headers(cfg) {
        return None;
    }
    match result {
        Ok(Done::Buffered { headers, .. } | Done::Stream { headers, .. }) => {
            Some(Upstream::Success(respond::filter_upstream_headers(headers)))
        }
        Err(Failure::Exec(error)) if !error.direct => error.addon.as_deref().cloned().map(Upstream::Error),
        Err(_) => None,
    }
}

/// `requests.nonstream-keepalive-interval` seconds (Go `NonStreamingKeepAliveInterval`;
/// 0 or below disables it), for the calls whose Go handler starts the keep-alive: only
/// non-stream generate handlers do; the count-tokens handlers (code_handlers.go,
/// gemini_handlers.go) and the speech handler never call `StartNonStreamingKeepAlive`.
fn nonstream_keepalive(call: &Call, cfg: &Config) -> Option<Duration> {
    let speech = call.media.as_ref().is_some_and(|m| m.kind == MediaKind::Speech);
    if call.stream || call.operation != Operation::Generate || speech {
        return None;
    }
    let seconds = cfg
        .document
        .get("requests")
        .and_then(|r| r.get("nonstream-keepalive-interval"))
        .and_then(serde_yaml_ng::Value::as_i64)
        .unwrap_or(0);
    (seconds > 0).then(|| Duration::from_secs(seconds as u64))
}

/// `requests.streaming.bootstrap-retries` (Go `StreamingBootstrapRetries`).
fn bootstrap_retries(cfg: &Config) -> usize {
    cfg.document
        .get("requests")
        .and_then(|r| r.get("streaming"))
        .and_then(|s| s.get("bootstrap-retries"))
        .and_then(serde_yaml_ng::Value::as_i64)
        .unwrap_or(0)
        .max(0) as usize
}

/// Go handlers_stream.go: a stream that failed before its first payload is retried as a
/// whole request when the status is statusless, auth, quota, timeout or 5xx.
pub async fn run_with_bootstrap_retries(rt: &Arc<Runtime>, call: Call, trace: &Trace) -> Result<Done, Failure> {
    let mut call = call;
    // A model router (`model-routers`) names the model, for the registry to route.
    let mut picked = None;
    if call.forced_provider.is_none()
        && let Some(decision) = crate::inference::model_router::resolve(rt, &call).await
    {
        trace.set_routed_model(&decision.model);
        call.model = decision.model.clone();
        picked = Some(decision);
    }
    // A router's `fallback-models` sends the client's request again, before interceptors.
    let original = picked.as_ref().is_some_and(|p| p.falls_back()).then(|| call.clone());
    loop {
        // Go resolves providers before the lifecycle starts: an unknown model never starts one.
        let resolved = route(rt, &rt.registry(), &call).map_err(|e| e.failure)?;
        // The usage record's prompt prefix is the client's, before interceptors rewrite it.
        if call.operation == Operation::Generate && rt.usage_queue().accepts() {
            trace.prompt_prefix(call.entry, &call.body);
        }
        let scope = picked.as_ref().map(|p| p.scope.clone());
        let result = retry_bootstrap(rt, call, trace, &resolved, scope).await;
        // A conversation moves to a routed model only once it served.
        if let (Ok(_), Some(picked)) = (&result, &picked) {
            picked.commit(rt);
        }
        // Only a failure before any byte reached the client moves to another model, and
        // never one a `stop` rule ended.
        let next = match (&result, picked.as_mut(), &original) {
            (Err(failure), Some(picked), Some(original))
                if !trace.stopped() && crate::inference::model_router::fails_over(failure) =>
            {
                crate::inference::model_router::fall_back(rt, picked).map(|model| (model, original.clone()))
            }
            _ => None,
        };
        let Some((model, original)) = next else {
            return result;
        };
        trace.set_routed_model(&model);
        call = original;
        call.model = model;
    }
}

/// The bootstrap retry rounds of one resolved request.
async fn retry_bootstrap(
    rt: &Arc<Runtime>,
    call: Call,
    trace: &Trace,
    resolved: &(Vec<String>, String),
    scope: Option<Arc<crate::scheduling::provider_policy::Providers>>,
) -> Result<Done, Failure> {
    let max = if call.stream {
        bootstrap_retries(&rt.config())
    } else {
        0
    };
    let mut result = run_routed(rt, call.clone(), trace, resolved.clone(), scope.clone()).await;
    for _ in 0..max {
        let original = match &result {
            Err(RunError {
                failure: Failure::Exec(e),
                bootstrap: true,
            }) if bootstrap_eligible(classify::go_status(e)) => e.clone(),
            _ => break,
        };
        result = match run_routed(rt, call.clone(), trace, resolved.clone(), scope.clone()).await {
            Err(RunError {
                failure,
                bootstrap: false,
            }) if matches!(failure, Failure::Unavailable { .. }) && classify::response_status(&original) >= 500 => {
                Err(RunError {
                    failure: Failure::Exec(original),
                    bootstrap: false,
                })
            }
            Err(RunError {
                failure,
                bootstrap: false,
            }) => {
                return Err(failure);
            }
            other => other,
        };
    }
    result.map_err(|e| e.failure)
}

fn bootstrap_eligible(status: u16) -> bool {
    matches!(status, 0 | 401 | 402 | 403 | 408 | 429) || status >= 500
}

/// The trace ID of the last credential selected for a request.
#[derive(Default)]
pub struct Trace(
    std::sync::Mutex<Option<String>>,
    std::sync::OnceLock<String>,
    ck_core::exec::CaptureSink,
    /// When the request arrived, for the usage record's timings.
    Arc<crate::observability::api::Clock>,
    /// The client's prompt prefix
    /// (`crate::observability::usage_record::prompt_prefix`).
    std::sync::OnceLock<String>,
    /// The model a model router sent the request to ([`crate::inference::model_router`]).
    std::sync::Mutex<Option<String>>,
    /// The ID of the last credential selected for the request.
    std::sync::Mutex<Option<String>>,
    /// An attempt stopped the request (a `stop` rule or a request fault).
    std::sync::atomic::AtomicBool,
);

impl Trace {
    /// Uses the HTTP middleware identity; absent identity keeps internal execution's
    /// lazy UUID generation. Capture this before spawning background work.
    pub fn with_request_id(id: Option<String>) -> Self {
        let trace = Self(
            Default::default(),
            Default::default(),
            crate::observability::request_logging::current()
                .map(|log| log.capture_sink())
                .unwrap_or_default(),
            crate::observability::api::current_clock(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
        );
        if let Some(id) = id.filter(|id| !id.is_empty()) {
            let _ = trace.1.set(id);
        }
        trace
    }

    /// The request ID (Go `logging.GetRequestID`), created on first use.
    pub fn request_id(&self) -> String {
        self.1.get_or_init(request_id).clone()
    }

    /// Connection-owned transports pass their capture explicitly after leaving
    /// middleware task scope, along with the same middleware request ID.
    pub fn with_capture(mut self, capture: ck_core::exec::CaptureSink) -> Self {
        self.2 = capture;
        self
    }

    /// The `X-Ck-Trace-Id` value of the credential selected so far.
    fn header(&self) -> Option<axum::http::HeaderValue> {
        let id = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()?;
        axum::http::HeaderValue::from_str(&id).ok()
    }

    pub(crate) fn selected(&self, credential: &ck_core::credential::Credential) {
        *self.6.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(credential.id.clone());
        let index = ck_core::config::credentials::auth_index(credential);
        if index.is_empty() {
            return;
        }
        let request = self.1.get_or_init(request_id);
        let stamp = chrono::Local::now().format("%Y%m%d%H%M%S");
        *self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(format!("{stamp}-{index}-{request}"));
    }

    /// The trace ID once a credential was selected.
    pub(crate) fn id(&self) -> Option<String> {
        self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    fn set_routed_model(&self, model: &str) {
        *self.5.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(model.to_owned());
    }

    fn routed_model(&self) -> Option<String> {
        self.5.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    fn stop(&self) {
        self.7.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    fn stopped(&self) -> bool {
        self.7.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The provider selector (`routing.providers`) of the last selected credential.
    fn routed_provider(&self, rt: &Runtime) -> Option<String> {
        let id = self
            .6
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()?;
        let credential = rt.store().get(&id)?;
        Some(rt.policy().providers.name(&credential))
    }

    pub(crate) fn clock(&self) -> Arc<crate::observability::api::Clock> {
        self.3.clone()
    }

    /// The prompt prefix hash of the request's body, from the first body it is asked
    /// for: the client's body when [`run_with_bootstrap_retries`] asks.
    pub(crate) fn prompt_prefix(&self, format: Format, body: &[u8]) -> String {
        self.4
            .get_or_init(|| crate::observability::usage_record::prompt_prefix(format, body))
            .clone()
    }
}

/// A UUIDv7 request ID (Go `logging.GenerateRequestID`).
pub fn request_id() -> String {
    use std::hash::{BuildHasher, Hasher};
    let random = || {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        );
        h.finish()
    };
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let (a, b) = (random(), random());
    let hi = (millis << 16) | 0x7000 | (a & 0x0fff);
    let lo = (b & 0x3fff_ffff_ffff_ffff) | 0x8000_0000_0000_0000;
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        hi >> 32,
        (hi >> 16) & 0xffff,
        hi & 0xffff,
        lo >> 48,
        lo & 0xffff_ffff_ffff
    )
}

/// A terminal failure of [`run`]. `bootstrap` marks a stream that failed before its
/// first payload (Go `streamBootstrapError`).
pub struct RunError {
    pub failure: Failure,
    pub bootstrap: bool,
}

impl From<Failure> for RunError {
    fn from(failure: Failure) -> Self {
        Self {
            failure,
            bootstrap: false,
        }
    }
}

/// One attempt's failure.
#[derive(Clone)]
struct Fault {
    error: ExecError,
    /// The stream's headers, when it failed before its first payload (Go
    /// `streamBootstrapError`).
    bootstrap: Option<Box<HeaderMap>>,
}

impl Fault {
    fn into_run_error(self) -> RunError {
        RunError {
            failure: Failure::Exec(self.error),
            bootstrap: self.bootstrap.is_some(),
        }
    }
}

/// Runs one request through selection, execution and retry rounds (Go
/// `Manager.Execute*`). Selection failures take part in retry rounds too.
pub async fn run(rt: &Arc<Runtime>, call: Call, trace: &Trace) -> Result<Done, RunError> {
    let resolved = route(rt, &rt.registry(), &call)?;
    run_routed(rt, call, trace, resolved, None).await
}

/// What a request with a decoded exchange ([`run_exchange`]) came to.
pub enum Served {
    /// An old-arm attempt answered; the route renders it as for [`run`].
    Old(Done),
    /// A contract-arm attempt committed: the response carries its first deliverable bytes.
    Committed(axum::response::Response),
}

/// [`run`] for a request its route decoded into `codec`. Each candidate takes the contract
/// arm through `arms` or the old arm (D2-PLAN §3.2, §4); both share selection, retry rounds,
/// the credits fallback and one usage `Facts`.
pub async fn run_exchange<X: Arms>(
    rt: &Arc<Runtime>,
    arms: &X,
    call: Call,
    trace: &Trace,
    codec: Codec<'_>,
) -> Result<Served, RunError> {
    let resolved = route(rt, &rt.registry(), &call)?;
    let mut arm = Arm::new(arms, codec);
    run_loop(rt, call, trace, resolved, None, Some(&mut arm)).await
}

/// [`run`] for resolved providers and model.
async fn run_routed(
    rt: &Arc<Runtime>,
    call: Call,
    trace: &Trace,
    resolved: (Vec<String>, String),
    scope: Option<Arc<crate::scheduling::provider_policy::Providers>>,
) -> Result<Done, RunError> {
    let old = run_loop::<ck_exec::Executors>(rt, call, trace, resolved, scope, None).await?;
    match old {
        Served::Old(done) => Ok(done),
        Served::Committed(_) => unreachable!("only an exchange commits"),
    }
}

/// The selection loop; `arm` is the exchange of a request that may take the contract arm.
async fn run_loop<X: Arms>(
    rt: &Arc<Runtime>,
    call: Call,
    trace: &Trace,
    (providers, model): (Vec<String>, String),
    scope: Option<Arc<crate::scheduling::provider_policy::Providers>>,
    mut arm: Option<&mut Arm<'_, '_, X>>,
) -> Result<Served, RunError> {
    let (cfg, policy) = rt.request_snapshot();
    let registry = rt.registry();
    let aliases = registry::global_aliases(&cfg);
    let session = crate::scheduling::session::resolve(
        call.entry,
        &call.headers,
        &call.body,
        call.execution_session.as_deref(),
        &call.caller.principal,
    );
    // Go's usage reporter: one record per Generate attempt while the queue accepts.
    let usage = (call.operation == Operation::Generate && rt.usage_queue().accepts()).then(|| {
        Arc::new(crate::observability::usage_record::Facts::new(
            usage_client(&cfg, &call, trace, &session),
            call.entry,
            call.response,
            &call.model,
            &call.body,
            call.stream,
            trace,
        ))
    });
    // `WithPinnedAuthID`: every other credential is excluded in every round.
    let mut pinned_exclusion: Vec<String> = match call.pinned() {
        Some(pinned) if !pinned.is_empty() => rt
            .store()
            .snapshot()
            .iter()
            .filter(|c| c.id != pinned)
            .map(|c| c.id.clone())
            .collect(),
        _ => Vec::new(),
    };
    // `WithDisallowFreeAuth`: free-plan Codex credentials are ineligible in every round.
    if call.media.as_ref().is_some_and(|m| m.disallow_free) {
        for credential in rt.store().snapshot() {
            let free = credential.provider.trim().eq_ignore_ascii_case("codex")
                && credential
                    .attributes
                    .get("plan_type")
                    .is_some_and(|plan| plan.trim().eq_ignore_ascii_case("free"));
            if free && !pinned_exclusion.contains(&credential.id) {
                pinned_exclusion.push(credential.id.clone());
            }
        }
    }
    let mut selection = Selection {
        providers: providers.clone(),
        model: call.selection_model.clone().unwrap_or_else(|| model.clone()),
        session: session.id.clone(),
        session_parent: session.parent,
        session_fork: session.fork,
        exclude: pinned_exclusion.clone(),
        scope,
        ..Selection::default()
    };
    let request = ExecRequest {
        operation: call.operation,
        source_format: call.entry,
        response_format: call.response,
        requested_model: model.clone(),
        model: model.clone(),
        original_body: call.body.clone(),
        body: call.body.clone(),
        stream: call.stream,
        alt: call.alt.clone(),
        session: session.id,
        execution_session: call.execution_session.clone(),
        derived_session: session.derived,
        resolved_model: None,
        usage: ck_core::exec::UsageSink::default().with_capture(trace.2.clone()),
        request_path: call.request_path.clone(),
        headers: call.headers.clone(),
        caller: call.caller.clone(),
    };
    let compact = call.alt.as_deref() == Some("responses/compact");
    // Go `pickLCP`: with session affinity, an authenticated request without an explicit
    // session binds by its conversation prefix.
    if policy.session_affinity && !session.explicit {
        selection.lcp = lcp::Request::new(call.entry.as_str(), &call.body, &call.caller.principal).map(Arc::new);
    }
    // Go `preferredExecutionAttemptError`: the latest failure that reached upstream wins
    // over later selection failures.
    let mut upstream: Option<Fault> = None;
    loop {
        let mut last: Option<Fault> = None;
        // A destination refusal answers only when no candidate failed after sending.
        let mut skipped: Option<Fault> = None;
        let mut attempted: Vec<String> = Vec::new();
        let pick_failure = loop {
            if policy.max_retry_credentials > 0 && attempted.len() >= policy.max_retry_credentials {
                break None;
            }
            let lease = match rt.acquire(selection.clone(), &cfg, policy.clone(), &registry).await {
                Ok(lease) => lease,
                Err(AcquireError::Prepare { id, error }) => {
                    attempted.push(id.clone());
                    selection.exclude.push(id);
                    last = Some(Fault { error, bootstrap: None });
                    continue;
                }
                Err(AcquireError::Cooldown { wait, cause }) => {
                    let provider = if providers.len() == 1 {
                        providers[0].clone()
                    } else {
                        String::new()
                    };
                    break Some(Failure::Cooldown {
                        model: selection.model.clone(),
                        provider,
                        wait,
                        cause,
                    });
                }
                Err(AcquireError::Unavailable { retry_after, cause }) => {
                    break Some(Failure::Unavailable {
                        code: if retry_after.is_some() {
                            "auth_unavailable"
                        } else {
                            "auth_not_found"
                        },
                        providers: providers.clone(),
                        model: model.clone(),
                        cause,
                        retry_after,
                    });
                }
            };
            selection.exclude.push(lease.credential.id.clone());
            trace.selected(&lease.credential);
            let (mut models, alias) = registry::execution_models(&aliases, &lease.credential, &selection.model);
            let pooled = models.len() > 1;
            if pooled {
                // Go `nextModelPoolOffset`: rotate the alias pool once per selection.
                let key = format!(
                    "{}|{}|{}",
                    lease.credential.id.trim().go_lower(),
                    registry::provider_key(&lease.credential),
                    canonical_model(registry::strip_prefix(&selection.model, &lease.credential)).go_lower()
                );
                let offset = rt.next_pool_offset(&key, models.len());
                models.rotate_left(offset);
            }
            let selection_model = registry::selection_model(&aliases, &lease.credential, &selection.model);
            let models: Vec<String> = models
                .into_iter()
                .filter(|m| {
                    let state = registry::state_model(&selection_model, &selection.model, m, pooled);
                    !rt.store().blocked(&lease.credential, &state)
                })
                .collect();
            if models.is_empty() {
                continue;
            }
            attempted.push(lease.credential.id.clone());
            let target = Target {
                models: &models,
                selection_model: &selection_model,
                pooled,
                alias: &alias,
                compact,
                keep_model: call.selection_model.is_some(),
            };
            let ran = attempt(
                rt,
                &cfg,
                &policy,
                &call,
                trace,
                &request,
                usage.as_ref(),
                arm.as_deref_mut(),
                lease,
                target,
            )
            .await;
            match ran {
                Attempt::Done(done) => return Ok(Served::Old(done)),
                Attempt::Committed(response) => return Ok(Served::Committed(response)),
                Attempt::Skip(fault) => skipped = Some(fault),
                Attempt::Stop(fault) => {
                    trace.stop();
                    return Err(fault.into_run_error());
                }
                Attempt::Next(fault) => {
                    if classify::upstream_attempted(&fault.error) {
                        upstream = Some(fault.clone());
                    }
                    last = Some(fault);
                }
            }
        };
        // The round's error decides retries: an attempt error when there was one, else
        // the selection failure. The response reports the latest upstream attempt when
        // there was one (Go executeMixedOnce, preferredExecutionAttemptError).
        let round_error: RunError = match (last.or(skipped), pick_failure) {
            (Some(fault), _) => fault.into_run_error(),
            (None, Some(failure)) => failure.into(),
            (None, None) => Failure::Unavailable {
                code: "auth_not_found",
                providers: providers.clone(),
                model: model.clone(),
                cause: None,
                retry_after: None,
            }
            .into(),
        };
        // Go `shouldRetryAfterErrorWithAttempted` / `isRequestRetryRoundError`.
        let (status, retry_round) = match &round_error.failure {
            Failure::Exec(e) => (
                classify::go_status(e),
                classify::is_retry_round(e) && !classify::is_request_invalid(e),
            ),
            Failure::Cooldown { .. } => (429, true),
            Failure::Unavailable {
                retry_after: Some(_), ..
            } => (503, true),
            _ => (0, false),
        };
        let terminal = |round_error: RunError| upstream.clone().map(Fault::into_run_error).unwrap_or(round_error);
        let pinned = call.pinned();
        let credits = Credits {
            providers: &providers,
            model: &model,
            aliases: &aliases,
            pinned: pinned.as_deref(),
            compact,
            scope: selection.scope.clone(),
        };
        if !retry_round {
            return credits
                .fallback(
                    rt,
                    &cfg,
                    &policy,
                    &call,
                    trace,
                    &request,
                    usage.as_ref(),
                    arm.as_deref_mut(),
                    terminal(round_error),
                )
                .await;
        }
        let wait = {
            let admit = crate::runtime::state::admission(&registry, &aliases, &selection, &rt.executors);
            rt.store().retry_wait(&selection, &policy, status, &attempted, &admit)
        };
        let Some(wait) = wait else {
            return credits
                .fallback(
                    rt,
                    &cfg,
                    &policy,
                    &call,
                    trace,
                    &request,
                    usage.as_ref(),
                    arm.as_deref_mut(),
                    terminal(round_error),
                )
                .await;
        };
        if !wait.is_zero() {
            tokio::time::sleep(jitter(wait, policy.max_retry_interval)).await;
        }
        selection.retry_round += 1;
        selection.exclude.clone_from(&pinned_exclusion);
    }
}

/// Go `jitteredCooldownWait`: up to a quarter of the wait (at most 2s) extra, never past
/// the configured maximum, so synchronized clients spread out.
pub fn jitter(wait: Duration, max: Duration) -> Duration {
    let mut range = (wait / 4).min(Duration::from_secs(2));
    if !max.is_zero() {
        range = range.min(max.saturating_sub(wait));
    }
    if range.is_zero() {
        return wait;
    }
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    );
    wait + Duration::from_nanos(hasher.finish() % range.as_nanos().max(1) as u64)
}

/// Go's Antigravity credits fallback (`shouldAttemptAntigravityCreditsFallback`,
/// `tryAntigravityCreditsExecute` and its stream variant): after the last round, a
/// Claude-model request that ended on quota or with nothing selectable is tried once
/// more per Antigravity credential, in credits-hint order, with credits enabled.
struct Credits<'a> {
    providers: &'a [String],
    model: &'a str,
    aliases: &'a std::collections::HashMap<String, Vec<ck_core::registry::dynamic::OAuthAlias>>,
    pinned: Option<&'a str>,
    compact: bool,
    /// The request's provider scope (a router's), else `routing.providers` applies.
    scope: Option<Arc<crate::scheduling::provider_policy::Providers>>,
}

impl Credits<'_> {
    /// Whether `error` qualifies: credits on, an Antigravity provider, and a 429/503 or
    /// a selection that found every credential cooling or unavailable.
    fn wanted(&self, cfg: &Config, error: &RunError) -> bool {
        self.providers
            .iter()
            .any(|p| p.trim().eq_ignore_ascii_case("antigravity"))
            && ck_exec::antigravity_credits::enabled(cfg)
            && match &error.failure {
                Failure::Exec(e) => matches!(classify::go_status(e), 429 | 503),
                Failure::Cooldown { .. } => true,
                Failure::Unavailable { code, .. } => matches!(*code, "auth_not_found" | "auth_unavailable"),
                _ => false,
            }
    }

    #[allow(clippy::too_many_arguments)]
    async fn fallback<X: Arms>(
        &self,
        rt: &Arc<Runtime>,
        cfg: &Config,
        policy: &Policy,
        call: &Call,
        trace: &Trace,
        request: &ExecRequest,
        usage: Option<&Arc<crate::observability::usage_record::Facts>>,
        mut arm: Option<&mut Arm<'_, '_, X>>,
        error: RunError,
    ) -> Result<Served, RunError> {
        // Go's ExecuteCount has no credits fallback; Execute and ExecuteStream do.
        if request.operation == Operation::CountTokens
            || !self.wanted(cfg, &error)
            || !self.model.go_lower().contains("claude")
        {
            return Err(error);
        }
        let snapshot = rt.store().snapshot();
        let ids = snapshot
            .iter()
            .filter(|c| !c.disabled && c.provider.trim().eq_ignore_ascii_case("antigravity"))
            .filter(|c| {
                !crate::scheduling::provider_policy::Providers::out_of_scope(
                    self.scope.as_deref(),
                    &policy.providers,
                    c,
                    self.model,
                )
            })
            .filter(|c| self.pinned.is_none_or(|p| p.is_empty() || c.id == p))
            .map(|c| c.id.as_str());
        for id in ck_exec::antigravity_credits::order_candidates(ids) {
            let selection = Selection::new("antigravity", self.model);
            let Some(lease) = rt
                .lease_credential(&id, selection, cfg, ck_exec::antigravity_credits::request_on)
                .await
            else {
                continue;
            };
            let (models, alias) = registry::execution_models(self.aliases, &lease.credential, self.model);
            if models.is_empty() {
                continue;
            }
            let selection_model = registry::selection_model(self.aliases, &lease.credential, self.model);
            let target = Target {
                models: &models,
                selection_model: &selection_model,
                pooled: models.len() > 1,
                alias: &alias,
                compact: self.compact,
                keep_model: call.selection_model.is_some(),
            };
            // ponytail: a 401 inside a credits attempt retries with the refreshed
            // credential, which no longer carries the credits flag.
            let ran = attempt(
                rt,
                cfg,
                policy,
                call,
                trace,
                request,
                usage,
                arm.as_deref_mut(),
                lease,
                target,
            )
            .await;
            match ran {
                Attempt::Done(done) => return Ok(Served::Old(done)),
                Attempt::Committed(response) => return Ok(Served::Committed(response)),
                Attempt::Stop(_) | Attempt::Skip(_) | Attempt::Next(_) => {}
            }
        }
        Err(error)
    }
}

enum Attempt {
    Done(Done),
    /// A contract-arm attempt committed.
    Committed(axum::response::Response),
    /// Terminal for the whole request.
    Stop(Fault),
    /// Failed over; try the next credential.
    Next(Fault),
    /// Every model of this credential refused the request before sending; try the next.
    Skip(Fault),
}

/// What one selected credential executes.
struct Target<'a> {
    /// Upstream model candidates (more than one is an alias pool).
    models: &'a [String],
    selection_model: &'a str,
    pooled: bool,
    alias: &'a AliasResult,
    compact: bool,
    /// The request model is not the selection model (Interactions agents): execute
    /// the request model, keep cooldown state on the selection model.
    keep_model: bool,
}

#[allow(clippy::too_many_arguments)]
async fn attempt<X: Arms>(
    rt: &Arc<Runtime>,
    cfg: &Config,
    policy: &Policy,
    call: &Call,
    trace: &Trace,
    request: &ExecRequest,
    usage: Option<&Arc<crate::observability::usage_record::Facts>>,
    mut arm: Option<&mut Arm<'_, '_, X>>,
    mut lease: Lease,
    target: Target<'_>,
) -> Attempt {
    let route_model = lease.selection.model.clone();
    let mut last = None;
    let mut refreshed = false;
    let session = call.turn.as_ref().map(|turn| &turn.session);
    'models: for (i, upstream) in target.models.iter().enumerate() {
        // Go `stateModelForExecution`.
        let state = registry::state_model(target.selection_model, &route_model, upstream, target.pooled);
        lease.execution_model = state.clone();
        let mut req = attempt_request(
            request,
            cfg,
            &lease.credential,
            &route_model,
            upstream,
            target.keep_model,
        );
        // Go `syncMetadataSessionToContext`: an LCP pick's session is the attempt's
        // canonical session, for `$CPA-SESSION-ID` and the usage record.
        let lcp = lease.lcp.clone();
        if let Some(m) = &lcp {
            req.session = Some(ck_wire::session::bound_session_identity(&m.session));
        }
        if let Some(on_selected) = call.on_selected() {
            on_selected(&lease.credential);
        }
        let fault = 'arm: {
            if let Some(arm) = arm.as_deref_mut() {
                let ran = loop {
                    let ran = attempt::run(arm, rt, cfg, call, trace, &req, usage, &mut lease, upstream).await;
                    // Go `tryRefreshAfterUnauthorized`, once per credential, after the failed
                    // attempt was settled and discarded.
                    if let Ran::Failed(fault) = &ran
                        && !refreshed
                        && classify::is_unauthorized(&fault.error)
                        && let Some(current) = rt.refresh_after_unauthorized(&lease.credential, cfg).await
                    {
                        refreshed = true;
                        lease.credential = current;
                        continue;
                    }
                    break ran;
                };
                match ran {
                    Ran::Old => {}
                    Ran::Failed(fault) => break 'arm fault,
                    Ran::Committed(committed) => return Attempt::Committed(committed.spawn(rt, lease)),
                    Ran::Stop(fault) => {
                        lease.complete(Outcome::Neutral(fault.error.clone()));
                        return Attempt::Stop(fault);
                    }
                    Ran::Skip(fault) => {
                        let outcome = Outcome::Neutral(fault.error.clone());
                        if i + 1 < target.models.len() {
                            lease.note(&state, &outcome);
                            continue 'models;
                        }
                        lease.complete(outcome);
                        return last.map_or(Attempt::Skip(fault), Attempt::Next);
                    }
                }
            }
            let start = |credential: &ck_core::credential::Credential, req: &mut ExecRequest| {
                usage.map(|facts| {
                    let mut tracker =
                        crate::observability::usage_record::Tracker::start(rt, facts, credential, upstream);
                    if let Some(m) = &lcp {
                        tracker.lcp_session(m);
                    }
                    req.usage = tracker.sink().with_capture(req.capture().clone());
                    tracker
                })
            };
            let mut tracker = start(&lease.credential, &mut req);
            let execute = |credential: Arc<ck_core::credential::Credential>, req: ExecRequest| async move {
                let credential = rt.for_executor(&credential);
                match (session, call.media.as_ref().map(|m| m.kind)) {
                    (Some(session), _) => rt.executors.execute_in_session(&credential, req, cfg, session).await,
                    // The call names the path (Go's request_path metadata).
                    (None, Some(MediaKind::Images)) => {
                        let path = req.request_path.clone();
                        rt.executors.images(&credential, req, &path, cfg).await
                    }
                    (None, Some(MediaKind::Videos)) => {
                        let path = req.request_path.clone();
                        rt.executors.videos(&credential, req, &path, cfg).await
                    }
                    (None, Some(MediaKind::Speech)) => rt.executors.speech(&credential, req, cfg).await,
                    (None, None) => rt.executors.execute(&credential, req, cfg).await,
                }
            };
            tracing::trace!(target: "cpa_latency", stage = "selected");
            // Each pooled model is its own upstream dispatch, stamped right before it runs:
            // after preparation and the after-auth interceptor (Go `nextExecutionSeq`).
            lease.redispatch();
            let mut executed = execute(lease.credential.clone(), req.clone()).await;
            // Go `tryRefreshAfterUnauthorized`: one refresh-and-retry per credential.
            if let Err(error) = &executed
                && !refreshed
                && classify::is_unauthorized(error)
                && let Some(current) = rt.refresh_after_unauthorized(&lease.credential, cfg).await
            {
                refreshed = true;
                if let Some(t) = tracker.take() {
                    t.fail(error);
                }
                lease.credential = current;
                lease.redispatch();
                tracker = start(&lease.credential, &mut req);
                executed = execute(lease.credential.clone(), req).await;
            }
            if let (Ok(response), Some(t)) = (&executed, tracker.as_mut()) {
                t.arrived(&response.headers);
            }
            match executed {
                Ok(response) => match finish(call, response).await {
                    Ok(done) => {
                        let done = track_usage(done, tracker.take());
                        let done = if target.alias.force_mapping && !target.alias.original_alias.is_empty() {
                            rewrite_model(done, &target.alias.original_alias)
                        } else {
                            done
                        };
                        return Attempt::Done(match done {
                            Done::Stream { headers, first, rest } => {
                                // A good first chunk answers a bounded window's probe now,
                                // not when the stream ends.
                                lease.accepted();
                                Done::Stream {
                                    headers,
                                    first,
                                    rest: Completing::new(rest, lease).boxed(),
                                }
                            }
                            buffered => {
                                lease.complete(Outcome::Success);
                                buffered
                            }
                        });
                    }
                    Err(fault) => {
                        if let Some(t) = tracker.take() {
                            t.fail(&fault.error);
                        }
                        fault
                    }
                },
                Err(error) => {
                    if let Some(t) = tracker.take() {
                        t.fail(&error);
                    }
                    Fault { error, bootstrap: None }
                }
            }
        };
        let error = &fault.error;
        let action = policy.error_action(&lease.credential, error);
        let neutral = (target.compact && classify::is_compact_neutral(error) && !action.force_cooldown)
            || (call.operation == Operation::CountTokens
                && classify::is_count_endpoint_missing(error, upstream)
                && !action.force_cooldown);
        let outcome = if neutral {
            Outcome::Neutral(error.clone())
        } else {
            Outcome::Failure(error.clone())
        };
        let stop = if action.matched {
            action.stop
        } else {
            (target.compact && classify::is_compact_fault(error)) || classify::is_request_invalid(error)
        };
        // A credential-wide quota ends this credential's model pool.
        let last_here = stop || i + 1 == target.models.len() || classify::credential_scoped(error);
        // Go returns a stopping bootstrap failure as is: an error, not a stream, so the
        // handler's bootstrap retries never see it. Any other one goes on as a
        // `streamBootstrapError`, whose `Headers()` are the stream's.
        let mut fault = fault;
        if stop {
            fault.bootstrap = None;
        } else if let Some(headers) = &fault.bootstrap {
            fault.error.addon = Some(headers.clone());
        }
        if last_here {
            lease.complete(outcome);
            return if stop {
                Attempt::Stop(fault)
            } else {
                Attempt::Next(fault)
            };
        }
        lease.note(&state, &outcome);
        last = Some(fault);
    }
    Attempt::Next(last.expect("at least one model was attempted"))
}

/// Go's client request metadata for usage records (handlers.go `GetContextWithCancel`
/// and `syncMetadataSessionToContext`).
fn usage_client(
    cfg: &Config,
    call: &Call,
    trace: &Trace,
    session: &crate::scheduling::session::Session,
) -> crate::observability::usage_record::Client {
    let (api_key, access_provider) = (call.caller.principal.clone(), call.caller.provider.clone());
    crate::observability::usage_record::Client {
        session_id: session.id.clone().unwrap_or_default(),
        parent_session_id: session.parent.clone().unwrap_or_default(),
        is_fork: session.fork,
        node_kind: String::new(),
        is_compaction: false,
        session_source: session.source,
        request_id: trace.request_id(),
        // Session turns arrive on the WebSocket upgrade (a GET).
        endpoint: format!(
            "{} {}",
            if call.turn.is_some() { "GET" } else { "POST" },
            call.request_path
        ),
        api_key,
        access_provider,
        ..request_client(cfg, &call.headers, call.peer)
    }
}

/// The downstream peer's part of Go's client request metadata: addresses and agent.
// ponytail: `server.trusted-proxies` is read from the request's config snapshot; Go
// applies it at startup only.
pub(crate) fn request_client(
    cfg: &Config,
    headers: &axum::http::HeaderMap,
    peer: Option<std::net::SocketAddr>,
) -> crate::observability::usage_record::Client {
    let trusted = cfg.derived(|c| ck_core::config::TrustedProxies::new(&c.trusted_proxies));
    let text = |v: &axum::http::HeaderValue| String::from_utf8_lossy(v.as_bytes()).into_owned();
    let forwarded: Vec<String> = headers.get_all("x-forwarded-for").iter().map(text).collect();
    crate::observability::usage_record::Client {
        client_ip: peer.map(|p| p.ip().to_string()).unwrap_or_default(),
        resolved_client_ip: trusted
            .client_ip(peer, |name| headers.get(name).map(|v| v.as_bytes()))
            .trim()
            .to_owned(),
        x_forwarded_for: forwarded.join(", ").trim().to_owned(),
        user_agent: headers
            .get("user-agent")
            .map(text)
            .unwrap_or_default()
            .trim()
            .to_owned(),
        ..Default::default()
    }
}

/// Feeds a finished attempt's client-format response to its usage record: a buffered
/// body publishes now, a stream when it ends, fails or is dropped.
fn track_usage(done: Done, tracker: Option<crate::observability::usage_record::Tracker>) -> Done {
    let Some(mut tracker) = tracker else { return done };
    match done {
        Done::Buffered { headers, body } => {
            tracker.body(&body);
            tracker.succeed();
            Done::Buffered { headers, body }
        }
        Done::Stream { headers, first, rest } => {
            if let Some(first) = &first {
                tracker.event(first);
            }
            Done::Stream {
                headers,
                first,
                rest: Tracked {
                    inner: rest,
                    tracker: Some(tracker),
                }
                .boxed(),
            }
        }
    }
}

/// A client stream that reports its events to the attempt's usage record. The upstream
/// stream goes before the record publishes, so the record has the upstream body's size:
/// dropped on its end or error, and first when the client goes away (field order).
struct Tracked {
    inner: ExecStream,
    tracker: Option<crate::observability::usage_record::Tracker>,
}

impl futures_util::Stream for Tracked {
    type Item = Result<Bytes, ExecError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let item = std::task::ready!(self.inner.poll_next_unpin(cx));
        match &item {
            Some(Ok(event)) => {
                if let Some(t) = self.tracker.as_mut() {
                    t.event(event);
                }
            }
            // A boxed empty stream does not allocate.
            Some(Err(error)) => {
                self.inner = futures_util::stream::empty().boxed();
                if let Some(t) = self.tracker.take() {
                    t.fail(error);
                }
            }
            None => {
                self.inner = futures_util::stream::empty().boxed();
                if let Some(t) = self.tracker.take() {
                    t.succeed();
                }
            }
        }
        std::task::Poll::Ready(item)
    }
}

/// The request one upstream model attempt executes: the upstream model (unless the
/// request keeps its own model), with Go's `attachResolvedExecutionModelInfo` binding.
fn attempt_request(
    request: &ExecRequest,
    cfg: &Config,
    credential: &ck_core::credential::Credential,
    route_model: &str,
    upstream: &str,
    keep_model: bool,
) -> ExecRequest {
    let mut req = request.clone();
    if !keep_model {
        req.model = upstream.to_owned();
    }
    req.resolved_model = crate::net::capabilities::resolve_attempt(
        cfg,
        credential,
        route_model,
        upstream,
        keep_model.then_some(request.model.as_str()),
    );
    req
}

/// Bootstraps a stream (first event before committing) or buffers a body.
async fn finish(call: &Call, response: ck_core::exec::ExecResponse) -> Result<Done, Fault> {
    match response.body {
        ResponseBody::Buffered(body) => Ok(Done::Buffered {
            headers: response.headers,
            body,
        }),
        ResponseBody::Stream(mut stream) if call.stream => {
            let bootstrap = |error| Fault {
                error,
                bootstrap: Some(Box::new(response.headers.clone())),
            };
            let first = loop {
                match stream.next().await {
                    Some(Ok(bytes)) if bytes.is_empty() => continue,
                    Some(Ok(bytes)) => break bytes,
                    Some(Err(error)) => return Err(bootstrap(error)),
                    // Go conductor_stream.go: an empty stream is a failed attempt.
                    None => return Err(bootstrap(classify::empty_stream())),
                }
            };
            Ok(Done::Stream {
                headers: response.headers,
                first: Some(first),
                rest: stream,
            })
        }
        ResponseBody::Stream(mut stream) => {
            let mut body = BytesMut::new();
            while let Some(event) = stream.next().await {
                body.extend_from_slice(&event.map_err(|error| Fault { error, bootstrap: None })?);
            }
            Ok(Done::Buffered {
                headers: response.headers,
                body: body.freeze(),
            })
        }
    }
}

/// Go's force-mapped model rewrite: `rewriteModelInResponse` over a buffered body and
/// `StreamRewriter.RewriteChunk` over each stream event.
fn rewrite_model(done: Done, target: &str) -> Done {
    match done {
        Done::Buffered { headers, body } => Done::Buffered {
            headers,
            body: rewrite_body(&body, target),
        },
        Done::Stream { headers, first, rest } => {
            let target_owned = target.to_owned();
            let rest = rest
                .map(move |item| item.map(|event| rewrite_event(&event, &target_owned)))
                .boxed();
            Done::Stream {
                headers,
                first: first.map(|event| rewrite_event(&event, target)),
                rest,
            }
        }
    }
}

const MODEL_PATHS: [&str; 5] = [
    "model",
    "modelVersion",
    "response.model",
    "response.modelVersion",
    "message.model",
];

/// Go `rewriteModelInResponse`: `sjson.SetBytes` on each model path that exists, so
/// every other byte is kept.
fn rewrite_json(data: &[u8], target: &str) -> Vec<u8> {
    let mut out = data.to_vec();
    for path in MODEL_PATHS {
        if ck_wire::json::get(&out, path).exists() {
            ck_wire::json::set_str(&mut out, path, target);
        }
    }
    out
}

/// A buffered body: a JSON document, or SSE text rewritten line by line.
fn rewrite_body(body: &Bytes, target: &str) -> Bytes {
    if body.trim_ascii_start().starts_with(b"{") {
        Bytes::from(rewrite_json(body, target))
    } else {
        rewrite_lines(body, target)
    }
}

/// Go `StreamRewriter.RewriteChunk` for one complete event: a bare JSON chunk comes
/// back trimmed and rewritten, SSE text line by line.
// ponytail: executors emit whole events, so Go's buffering of partial and glued events
// is not ported; an invalid `data:` JSON line passes through unchanged.
fn rewrite_event(event: &Bytes, target: &str) -> Bytes {
    let trimmed = event.trim_ascii();
    if trimmed.starts_with(b"{") && ck_wire::json::valid(trimmed) {
        return Bytes::from(rewrite_json(trimmed, target));
    }
    rewrite_lines(event, target)
}

/// Go `rewriteSSEPayloadLines`: `data: {..}` / `data:{..}` lines holding valid JSON.
fn rewrite_lines(payload: &Bytes, target: &str) -> Bytes {
    let mut changed = false;
    let mut out = Vec::with_capacity(payload.len());
    for (i, line) in payload.split(|&b| b == b'\n').enumerate() {
        if i > 0 {
            out.push(b'\n');
        }
        let data = line.strip_prefix(b"data: ").or_else(|| line.strip_prefix(b"data:"));
        match data {
            Some(json) if json.starts_with(b"{") && ck_wire::json::valid(json) => {
                out.extend_from_slice(&line[..line.len() - json.len()]);
                let rewritten = rewrite_json(json, target);
                changed |= rewritten != json;
                out.extend_from_slice(&rewritten);
            }
            _ => out.extend_from_slice(line),
        }
    }
    if changed { Bytes::from(out) } else { payload.clone() }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stream's usage record publishes once its upstream stream is gone, so it has the
    /// upstream body size the stream reports when dropped: on an error the stream
    /// yields while it still holds its upstream body, at its end, and when the client
    /// goes away.
    #[tokio::test]
    async fn stream_records_publish_after_the_upstream_stream_is_dropped() {
        use ck_core::exec::{UpstreamStage, UsageSink};
        use std::task::Poll;
        /// What the upstream body reports when it is dropped.
        struct UpstreamBody(UsageSink);
        impl Drop for UpstreamBody {
            fn drop(&mut self) {
                self.0.upstream_stage(UpstreamStage::BodyRead { bytes: 42 });
            }
        }
        let cfg = Config::parse("observability:\n  usage:\n    usage-statistics-enabled: true\n").unwrap();
        let executors = ck_exec::Executors {
            claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
            codex: Default::default(),
            devices: Default::default(),
            openai: Default::default(),
            google: Default::default(),
        };
        let rt = Arc::new(crate::testing::runtime(cfg.clone(), Vec::new(), executors));
        rt.usage_queue().configure(true, &cfg);
        let mut metadata = serde_json::Map::new();
        metadata.insert("type".into(), "claude".into());
        let credential = ck_core::credential::Credential::from_file(
            std::path::Path::new("/auth"),
            std::path::Path::new("/auth/a.json"),
            metadata,
        )
        .unwrap();
        let facts = Arc::new(crate::observability::usage_record::Facts::new(
            Default::default(),
            Format::Claude,
            Format::Claude,
            "m",
            b"{}",
            true,
            &Trace::default(),
        ));
        for ending in ["error", "end", "client gone"] {
            let tracker = crate::observability::usage_record::Tracker::start(&rt, &facts, &credential, "m");
            let body = UpstreamBody(tracker.sink());
            // Polled, it answers without letting go of the upstream body.
            let rest = futures_util::stream::poll_fn(move |_| {
                let _held = &body;
                match ending {
                    "error" => Poll::Ready(Some(Err(ExecError::local(
                        502,
                        ck_core::exec::FailureScope::Request,
                        "restore failed",
                    )))),
                    "end" => Poll::Ready(None),
                    _ => Poll::Pending,
                }
            })
            .boxed();
            let done = track_usage(
                Done::Stream {
                    headers: HeaderMap::new(),
                    first: None,
                    rest,
                },
                Some(tracker),
            );
            let Done::Stream { mut rest, .. } = done else {
                unreachable!()
            };
            if ending == "client gone" {
                assert!(futures_util::poll!(rest.next()).is_pending());
                drop(rest);
            } else {
                assert!(!matches!(rest.next().await, Some(Ok(_))));
            }
            let records = rt.usage_queue().pop_oldest(10);
            assert_eq!(records.len(), 1, "{ending}");
            let record: serde_json::Value = serde_json::from_slice(&records[0]).unwrap();
            assert_eq!(record["upstream_response_bytes"], 42, "{ending}: {record}");
        }
    }

    /// Go starts the non-stream keep-alive in the non-stream generate handlers only, and
    /// `NonStreamingKeepAliveInterval` treats 0 and below as off.
    #[test]
    fn nonstream_keepalive_only_for_nonstream_generate() {
        let call = |operation, stream| Call {
            entry: Format::Claude,
            response: Format::Claude,
            operation,
            model: "m".into(),
            body: Bytes::new(),
            stream,
            alt: None,
            headers: HeaderMap::new(),
            caller: Caller {
                principal: String::new(),
                source: "",
                provider: String::new(),
            },
            forced_provider: None,
            selection_model: None,
            execution_session: None,
            request_path: String::new(),
            peer: None,
            turn: None,
            media: None,
        };
        let cfg = |yaml: &str| Config::parse(yaml).unwrap();
        let on = cfg("requests:\n  nonstream-keepalive-interval: 3\n");
        assert_eq!(
            nonstream_keepalive(&call(Operation::Generate, false), &on),
            Some(Duration::from_secs(3))
        );
        assert_eq!(nonstream_keepalive(&call(Operation::Generate, true), &on), None);
        assert_eq!(nonstream_keepalive(&call(Operation::CountTokens, false), &on), None);
        for off in [
            "{}\n",
            "requests:\n  nonstream-keepalive-interval: 0\n",
            "requests:\n  nonstream-keepalive-interval: -5\n",
        ] {
            assert_eq!(
                nonstream_keepalive(&call(Operation::Generate, false), &cfg(off)),
                None,
                "{off}"
            );
        }
        let configured = cfg("requests:\n  nonstream-keepalive-interval: 2\n");
        assert_eq!(
            nonstream_keepalive(&call(Operation::Generate, false), &configured),
            Some(Duration::from_secs(2))
        );
    }

    /// Each attempt carries Go's binding for its credential and upstream model; a kept
    /// request model (Go `restoreExecutionModel`) routes Codex by that model. Expected
    /// values are the Go goldens' "codex configured" and "codex restore" cases.
    #[test]
    fn attempts_bind_resolved_model_like_go() {
        let cfg = Config::parse(
            "config-version: 8\napi-keys:\n  codex:\n    - base-url: https://codex.example\n      models:\n        - name: gpt-6-sol\n          alias: sol\n          is-compat: true\n      keys:\n        - api-key: xk1\n",
        )
        .unwrap();
        let credential = ck_core::config::credentials::from_config(&cfg).remove(0);
        let request = ExecRequest {
            operation: Operation::Generate,
            source_format: Format::OpenAI,
            response_format: Format::OpenAI,
            requested_model: "sol".into(),
            model: "sol".into(),
            original_body: Bytes::new(),
            body: Bytes::new(),
            stream: false,
            alt: None,
            session: None,
            execution_session: None,
            derived_session: None,
            request_path: String::new(),
            headers: HeaderMap::new(),
            caller: Caller {
                principal: String::new(),
                source: "",
                provider: String::new(),
            },
            resolved_model: None,
            usage: Default::default(),
        };
        let req = attempt_request(&request, &cfg, &credential, "sol", "gpt-6-sol", false);
        assert_eq!(req.model, "gpt-6-sol");
        let bound = req.resolved_model.unwrap();
        assert_eq!(
            (bound.info.id.as_str(), bound.source, bound.is_compat()),
            ("gpt-6-sol", ck_core::exec::ResolvedSource::ApiKey, true)
        );
        let kept = attempt_request(&request, &cfg, &credential, "selection", "gpt-6-sol", true);
        assert_eq!(kept.model, "sol");
        let bound = kept.resolved_model.unwrap();
        assert_eq!((bound.info.id.as_str(), bound.is_compat()), ("sol", false));
    }

    #[test]
    fn failure_texts_match_go_shapes() {
        assert_eq!(
            Failure::UnknownModel("claude-sonnet-4-6".into()).text(),
            r#"{"error":{"message":"unknown provider for model claude-sonnet-4-6","type":"invalid_request_error","code":"model_not_found","param":"model"}}"#
        );
        let cooldown = Failure::Cooldown {
            model: "m".into(),
            provider: "claude".into(),
            wait: Duration::from_millis(59_400),
            cause: Some(r#"{"error":{"type":"rate_limit_error","message":"slow down"}}"#.into()),
        };
        assert_eq!(
            cooldown.text(),
            r#"{"error":{"code":"model_cooldown","last_upstream_error":"rate_limit_error: slow down","message":"All credentials for model m are cooling down via provider claude (last error: rate_limit_error: slow down)","model":"m","provider":"claude","reset_seconds":60,"reset_time":"59s"}}"#
        );
        assert_eq!(cooldown.retry_after(), Some(60));
        assert_eq!(
            (cooldown.status(), Failure::UnknownModel(String::new()).status()),
            (429, 400)
        );
        let none = Failure::Unavailable {
            code: "auth_not_found",
            providers: vec!["claude".into()],
            model: " m ".into(),
            cause: None,
            retry_after: None,
        };
        assert_eq!(
            none.text(),
            "auth_not_found: no auth available (providers=claude, model=m); check Claude auth/key session and cooldown state via /v0/management/auth-files"
        );
        assert_eq!((none.status(), none.retry_after()), (503, None));
    }

    #[test]
    fn force_mapping_rewrites_json_and_sse_model_fields() {
        let body = Bytes::from_static(br#"{"id":"x","model":"claude-opus-5","message":{"model":"claude-opus-5"}}"#);
        assert_eq!(
            rewrite_body(&body, "opus"),
            r#"{"id":"x","model":"opus","message":{"model":"opus"}}"#
        );
        let event = Bytes::from_static(b"event: message_start\ndata: {\"message\":{\"model\":\"up\"}}\n\n");
        assert_eq!(
            rewrite_event(&event, "alias"),
            "event: message_start\ndata: {\"message\":{\"model\":\"alias\"}}\n\n"
        );
        let untouched = Bytes::from_static(b"data: [DONE]\n\n");
        assert_eq!(rewrite_event(&untouched, "alias"), untouched);
        // sjson keeps every other byte: spacing, escapes, number spelling, CRLF.
        let crlf = Bytes::from_static(b"data: {\"model\" : \"up\", \"x\":\"\\u00e9\", \"n\":1.50}\r\n\r\n");
        assert_eq!(
            rewrite_event(&crlf, "alias"),
            "data: {\"model\" : \"alias\", \"x\":\"\\u00e9\", \"n\":1.50}\r\n\r\n"
        );
        // A path that is absent is not added; `data:` needs `{` right after the prefix.
        let spaced = Bytes::from_static(b"data:  {\"model\":\"up\"}\n\n");
        assert_eq!(rewrite_event(&spaced, "alias"), spaced);
        // Go trims a bare JSON stream chunk.
        let bare = Bytes::from_static(b" {\"modelVersion\":\"up\"}\n");
        assert_eq!(rewrite_event(&bare, "alias"), r#"{"modelVersion":"alias"}"#);
    }

    #[test]
    fn jitter_stays_within_go_bounds() {
        for _ in 0..50 {
            let w = jitter(Duration::from_secs(4), Duration::ZERO);
            assert!(w >= Duration::from_secs(4) && w < Duration::from_secs(5));
            let capped = jitter(Duration::from_secs(20), Duration::from_secs(21));
            assert!(capped >= Duration::from_secs(20) && capped < Duration::from_secs(21));
        }
        assert_eq!(
            jitter(Duration::from_secs(5), Duration::from_secs(5)),
            Duration::from_secs(5)
        );
        assert_eq!(jitter(Duration::ZERO, Duration::ZERO), Duration::ZERO);
    }

    #[test]
    fn summaries_follow_go_extract() {
        assert_eq!(
            upstream_summary(r#"{"error":{"code":"x","message":"boom"}}"#),
            "x: boom"
        );
        assert_eq!(
            upstream_summary(r#"{"error":{"type":"overloaded","message":"overloaded now"}}"#),
            "overloaded now"
        );
        assert_eq!(upstream_summary(r#"{"error":"flat"}"#), "flat");
        assert_eq!(upstream_summary("plain text"), "plain text");
        assert_eq!(upstream_summary("status 500: {\"message\":\"m\"}"), "m");
    }
}
