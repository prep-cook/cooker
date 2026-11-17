//! Call negotiation (`Handler.Handle`), hangup (`HandleHangup`) and the capability
//! stubs (capabilities.go).
//!
//! A WebRTC call terminates on the media relay; without one it answers 501. A stored call's
//! `oai-events` data channel runs as a live session ([`super::session::run_call`]), whose end
//! closes the call and both media peers.

use std::sync::Arc;

use axum::Extension;
use axum::body::Body;
use axum::extract::rejection::PathRejection;
use axum::extract::{OriginalUri, Path, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use ck_core::exec::{CaptureEvent, CaptureSink};
use ck_exec::codex_live::{self as live, BodyError, LiveTarget, ShapeError};
use futures_util::StreamExt;

use super::calls::Call;
use super::session::{self, Opened};
use super::{
    Live, Principal, header_session, live_error, panic_response, realtime_error, select_oauth, trace, with_trace,
};
use crate::runtime::state::Runtime;

pub(super) enum ReadError {
    TooLarge,
    /// The read failed; the status is 413 at `server.max-request-body-bytes`
    /// (Go `HTTPStatusFromErrorOr(err, 400)`), else 400.
    Read(String, u16),
}

/// `io.ReadAll(io.LimitReader(body, limit+1))`: more than `limit` bytes is too large.
pub(super) async fn read_limited(body: Body, limit: usize) -> Result<Vec<u8>, ReadError> {
    let mut stream = body.into_data_stream();
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| {
            let status = if crate::http::body_limit::is_too_large(&e) {
                413
            } else {
                400
            };
            ReadError::Read(e.to_string(), status)
        })?;
        if out.len() + chunk.len() > limit {
            return Err(ReadError::TooLarge);
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

/// `c.GetHeader`: the first value.
pub(super) fn header_text(headers: &HeaderMap, name: impl header::AsHeaderName) -> String {
    headers
        .get(name)
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        .unwrap_or_default()
}

/// `callResponseHeaders`: the upstream headers a live response may carry.
const CALL_RESPONSE_HEADERS: [&str; 5] = [
    "content-type",
    "location",
    "retry-after",
    "x-request-id",
    "openai-request-id",
];

/// `copyRealtimeHandshakeHeaders`.
pub(super) const HANDSHAKE_HEADERS: [&str; 3] = ["retry-after", "x-request-id", "openai-request-id"];

pub(super) fn copy_headers(to: &mut HeaderMap, from: &HeaderMap, names: &[&'static str]) {
    for name in names {
        for value in from.get_all(*name) {
            to.append(HeaderName::from_static(name), value.clone());
        }
    }
}

/// A handler's body written the way Go's server does: a missing Content-Type is
/// sniffed from a non-empty body (`http.DetectContentType`).
pub(super) fn written(status: u16, headers: HeaderMap, body: Vec<u8>) -> Response {
    tracked(status, headers, body, None)
}

/// [`written`], calling `on_failure` when the body is dropped before hyper took it (the
/// client went away first): part of Go's failed `c.Writer.Write`.
// ponytail: hyper takes the frame into its write buffer before the socket write, so a
// write that fails after that is not seen here and such a call lives until its sideband,
// hangup or expiry. Full parity needs a transport-level write result.
pub(super) fn tracked(
    status: u16,
    mut headers: HeaderMap,
    body: Vec<u8>,
    on_failure: Option<Box<dyn FnOnce() + Send>>,
) -> Response {
    if !headers.contains_key(header::CONTENT_TYPE) && !body.is_empty() {
        headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(sniff(&body)));
    }
    let status = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut response = (status, axum::body::Body::new(TrackedBody::new(body, on_failure))).into_response();
    *response.headers_mut() = headers;
    response
}

/// A one-frame body that knows whether hyper took its frame. Up to 2048 bytes it has an
/// exact size (Content-Length); past that none, so it goes out chunked, as Go's
/// `bufferBeforeChunkingSize` does, without the framing layer buffering it.
struct TrackedBody {
    data: Option<Bytes>,
    len: usize,
    on_failure: Option<Box<dyn FnOnce() + Send>>,
}

impl TrackedBody {
    fn new(body: Vec<u8>, on_failure: Option<Box<dyn FnOnce() + Send>>) -> Self {
        let len = body.len();
        Self {
            data: (len > 0).then(|| Bytes::from(body)),
            len,
            // An empty body has nothing to fail on.
            on_failure: on_failure.filter(|_| len > 0),
        }
    }
}

impl http_body::Body for TrackedBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        let frame = this.data.take().map(|data| {
            this.on_failure = None;
            Ok(http_body::Frame::data(data))
        });
        std::task::Poll::Ready(frame)
    }

    fn is_end_stream(&self) -> bool {
        self.data.is_none()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        let remaining = self.data.as_ref().map_or(0, Bytes::len);
        if self.len <= 2048 {
            http_body::SizeHint::with_exact(remaining as u64)
        } else {
            let mut hint = http_body::SizeHint::new();
            hint.set_lower(remaining as u64);
            hint
        }
    }
}

impl Drop for TrackedBody {
    fn drop(&mut self) {
        if let Some(on_failure) = self.on_failure.take() {
            on_failure();
        }
    }
}

/// `http.DetectContentType` for what a live upstream returns.
// ponytail: HTML, XML, UTF BOMs and text-versus-binary only; Go also recognises images,
// audio, archives and fonts. SDP and JSON are text/plain in Go too.
fn sniff(body: &[u8]) -> &'static str {
    let data = &body[..body.len().min(512)];
    if data.starts_with(b"\xFE\xFF") {
        return "text/plain; charset=utf-16be";
    }
    if data.starts_with(b"\xFF\xFE") {
        return "text/plain; charset=utf-16le";
    }
    if data.starts_with(b"\xEF\xBB\xBF") {
        return "text/plain; charset=utf-8";
    }
    let trimmed = &data[data
        .iter()
        .take_while(|b| matches!(b, b'\t' | b'\n' | 0x0c | b'\r' | b' '))
        .count()..];
    const HTML: [&[u8]; 17] = [
        b"<!DOCTYPE HTML",
        b"<HTML",
        b"<HEAD",
        b"<SCRIPT",
        b"<IFRAME",
        b"<H1",
        b"<DIV",
        b"<FONT",
        b"<TABLE",
        b"<A",
        b"<STYLE",
        b"<TITLE",
        b"<B",
        b"<BODY",
        b"<BR",
        b"<P",
        b"<!--",
    ];
    for tag in HTML {
        if trimmed.len() > tag.len()
            && trimmed[..tag.len()].eq_ignore_ascii_case(tag)
            && matches!(trimmed[tag.len()], b' ' | b'>')
        {
            return "text/html; charset=utf-8";
        }
    }
    if trimmed.starts_with(b"<?xml") {
        return "text/xml; charset=utf-8";
    }
    if data.starts_with(b"%PDF-") {
        return "application/pdf";
    }
    let binary = |b: &u8| matches!(b, 0x00..=0x08 | 0x0B | 0x0E..=0x1A | 0x1C..=0x1F);
    if data.iter().any(binary) {
        "application/octet-stream"
    } else {
        "text/plain; charset=utf-8"
    }
}

/// `Handler.Handle`: `POST /v1/live`, `/v1/realtime` and `/v1/realtime/calls`.
pub(super) async fn call(
    State(rt): State<Arc<Runtime>>,
    Extension(live_state): Extension<Arc<Live>>,
    Extension(principal): Extension<Principal>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let realtime = uri.path().starts_with("/v1/realtime");
    let fail = |status, message: &str| live_error(realtime, status, message);
    // Offers and answers carry ICE credentials; the request log keeps them out.
    if let Some(log) = crate::observability::request_logging::current() {
        log.redact_sdp();
    }
    let body = match read_limited(body, live::MAX_BODY).await {
        Ok(body) => body,
        Err(ReadError::TooLarge) => return fail(413, "Codex live request body too large"),
        Err(ReadError::Read(e, status)) => return fail(status, &format!("failed to read Codex live request: {e}")),
    };
    let secret_session = principal
        .secret
        .as_ref()
        .map(|g| g.session.as_slice())
        .unwrap_or_default();
    let shaped = live::prepare(&body, &header_text(&headers, header::CONTENT_TYPE))
        .and_then(|call| live::apply_client_secret(call, secret_session))
        .and_then(|call| {
            let requested = call.model.clone();
            live::rewrite_model(call).map(|call| (requested, call))
        });
    let (requested, call) = match shaped {
        Ok(shaped) => shaped,
        Err(ShapeError::Invalid(message)) => return fail(400, &message),
        Err(ShapeError::NilMap) => return panic_response(),
    };
    let (cfg, relay) = live_state.relays.snapshot(|| rt.config());
    let relay = match relay {
        Ok(Some(relay)) => relay,
        Ok(None) => return no_relay(realtime),
        Err(message) => return fail(503, &message),
    };
    let selection_headers = principal.selection_headers(&headers);
    let lease = match select_oauth(&rt, &cfg, None, &selection_headers, &body, None).await {
        Ok(lease) => lease,
        Err(rejection) => return rejection.render(realtime),
    };
    let credential = lease.credential.clone();
    // The media session and the lease, released unless the call keeps them.
    let mut held = Held {
        media: None,
        lease: Some(lease),
    };
    let trace = trace(&credential);
    let traced = |response| with_trace(response, &trace);
    let offer = match live::request_sdp(&call.body, &call.content_type) {
        Ok(offer) => offer,
        Err(message) => return traced(fail(400, &message)),
    };
    let route = super::relay::Route {
        proxy_url: ck_exec::proxy::Proxy::effective_url(&credential, &cfg),
        credential: media_credential_name(&credential),
        auth_index: ck_core::config::credentials::auth_index(&credential),
    };
    // Setup shares the lease: if this request goes away mid-setup, the relay keeps it
    // until the half-built session finished closing.
    let shared_lease = Arc::new(std::sync::Mutex::new(held.lease.take()));
    let created = relay.new_session(offer, route, Box::new(shared_lease.clone())).await;
    held.lease = shared_lease
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    let (media, upstream_offer) = match created {
        Ok(created) => created,
        Err(e) => return traced(fail(e.status, &e.message)),
    };
    held.media = Some(media.clone());
    let (upstream_body, upstream_content_type) =
        match live::replace_sdp(&call.body, &call.content_type, &upstream_offer) {
            Ok(replaced) => replaced,
            Err(ShapeError::Invalid(message)) => return traced(fail(400, &message)),
            Err(ShapeError::NilMap) => return traced(panic_response()),
        };
    let session_id = super::session_id(&selection_headers, &body);
    let mut upstream_headers = live::protocol_headers(&headers);
    upstream_headers.push(("Content-Type".into(), upstream_content_type));
    let capture = request_capture();
    let target = LiveTarget {
        credential: &credential,
        cfg: &cfg,
        client: &headers,
        session: header_session(&selection_headers, &body, None, ""),
        capture: capture.clone(),
    };
    let call_url = rt.executors.codex.live_endpoints().call_url.clone();
    let posted = rt
        .executors
        .codex
        .live_post(&target, &call_url, upstream_headers, Bytes::from(upstream_body))
        .await;
    let upstream = match posted {
        Ok(upstream) => upstream,
        Err(e) => {
            let status = if e.status == 0 { 502 } else { e.status };
            return traced(fail(status, &String::from_utf8_lossy(&e.body)));
        }
    };
    let status = upstream.status;
    let mut response_headers = HeaderMap::new();
    copy_headers(&mut response_headers, &upstream.headers, &CALL_RESPONSE_HEADERS);
    let location = header_text(&upstream.headers, header::LOCATION);
    let upstream_content_type = header_text(&upstream.headers, header::CONTENT_TYPE);
    record_metadata(&capture, status, &upstream.headers);
    let (mut data, read_error) = live::read_limited(upstream).await;
    record_body(&capture, &data, read_error.as_ref());
    if let Some(error) = read_error {
        let message = match error {
            BodyError::TooLarge => "Codex live response body too large",
            BodyError::Read(_) => "Failed to read Codex live response",
        };
        return traced(fail(502, message));
    }
    let success = (200..300).contains(&status);
    let mut on_failure: Option<Box<dyn FnOnce() + Send>> = None;
    if success {
        let call_id = live::call_id_from_location(&location);
        if call_id.is_empty() {
            return traced(fail(502, "Codex live response is missing a valid call ID"));
        }
        media.set_call_id(&call_id);
        if realtime && let Ok(value) = HeaderValue::from_str(&format!("/v1/realtime/calls/{call_id}")) {
            response_headers.insert(header::LOCATION, value);
        }
        let answer = match live::response_sdp(&data, &upstream_content_type) {
            Ok(answer) => answer,
            Err(message) => return traced(fail(502, &message)),
        };
        match media.accept_upstream_answer(answer).await {
            Ok(downstream) => data = downstream.into_bytes(),
            Err(e) => return traced(fail(e.status, &e.message)),
        }
        response_headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/sdp"));
        let stored = live_state.calls.put(
            &call_id,
            Call {
                auth_id: credential.id.clone(),
                session_id,
                owner_key: principal.key.clone(),
                owner_provider: principal.provider.clone(),
                secret_principal: principal
                    .secret
                    .as_ref()
                    .map(|g| g.principal.clone())
                    .unwrap_or_default(),
                media: Some(media.clone()),
                ..Call::default()
            },
        );
        if let Some(stored) = stored {
            held.media = None;
            // The data channel session ending, or the answer never reaching the client, ends
            // the call; weak, so the call can drop.
            let calls = Arc::downgrade(&live_state.calls);
            let (id, token) = (stored.call_id.clone(), stored.token);
            if let Some(events) = media.events() {
                let opened = Opened {
                    rt: rt.clone(),
                    settings: live_state.settings,
                    owner: principal.owner(),
                    events: ck_translate::realtime::Events::Realtime,
                    model: Some(requested.clone()),
                    upstream_model: call.model.clone(),
                    credential: credential.clone(),
                    call_id: Some(id.clone()),
                    facts: principal.facts(&cfg, &headers, &format!("POST {}", uri.path()), &requested),
                    hangup: stored.ended.clone(),
                };
                let (calls, id) = (calls.clone(), id.clone());
                tokio::spawn(async move {
                    let cause = session::run_call(opened, events, &*media).await;
                    if let Some(calls) = calls.upgrade() {
                        calls.complete_token(&id, token, cause);
                    }
                });
            }
            on_failure = Some(Box::new(move || {
                if let Some(calls) = calls.upgrade() {
                    calls.complete_token(&id, token, "response_write_failed");
                }
            }));
        }
    }
    traced(tracked(status, response_headers, data, on_failure))
}

/// This request's upstream capture, when request logging runs for it.
fn request_capture() -> CaptureSink {
    crate::observability::request_logging::current()
        .map(|log| log.capture_sink())
        .unwrap_or_default()
}

/// `RecordAPIResponseMetadata` with `callResponseHeaders`.
fn record_metadata(capture: &CaptureSink, status: u16, headers: &HeaderMap) {
    if capture.enabled() {
        capture.record(CaptureEvent::ResponseMetadata(
            status,
            &live::call_response_headers(headers),
        ));
    }
}

/// `AppendAPIResponseChunk` with what was read, then `RecordAPIResponseError` when the
/// read failed.
fn record_body(capture: &CaptureSink, data: &[u8], error: Option<&BodyError>) {
    capture.record(CaptureEvent::ResponseChunk(data));
    if let Some(error) = error {
        capture.record(CaptureEvent::ResponseError(&error.to_string()));
    }
}

/// What a live call request holds until the call keeps it: the media session, closed
/// with `request_not_retained`, and the credential lease, released once that close
/// finished (Go's deferred `CloseWithReason`, then the deferred selection `End`).
struct Held {
    media: Option<Arc<dyn super::relay::MediaSession>>,
    lease: Option<crate::runtime::state::Lease>,
}

impl Drop for Held {
    fn drop(&mut self) {
        let lease = self.lease.take();
        match self.media.take() {
            Some(session) => {
                session.close("request_not_retained");
                session.after_close(Box::new(move || drop(lease)));
            }
            None => drop(lease),
        }
    }
}

/// `mediaCredentialName`: a log-safe name for the credential (label, file name, index).
fn media_credential_name(credential: &ck_core::credential::Credential) -> String {
    let label = credential.label.trim();
    if !label.is_empty() {
        return label.to_owned();
    }
    if let ck_core::credential::Source::File(path) = &credential.source
        && let Some(name) = path.file_name().map(|n| n.to_string_lossy().trim().to_owned())
        && !name.is_empty()
    {
        return name;
    }
    ck_core::config::credentials::auth_index(credential)
}

/// The call ID path parameter, trimmed; `None` when it cannot be decoded.
fn path_call_id(path: Result<Path<String>, PathRejection>) -> Option<String> {
    path.ok()
        .map(|Path(id)| ck_wire::gostr::trim_space(id.as_bytes()).to_owned())
        .and_then(|id| String::from_utf8(id).ok())
}

/// `HandleHangup`: forwards to the upstream with the call's credential and forgets the
/// call when the upstream accepts.
pub(super) async fn hangup(
    State(rt): State<Arc<Runtime>>,
    Extension(live_state): Extension<Arc<Live>>,
    Extension(principal): Extension<Principal>,
    path: Result<Path<String>, PathRejection>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let Some(call_id) = path_call_id(path).filter(|id| live::valid_call_id(id)) else {
        return realtime_error(
            400,
            "Invalid Realtime call ID",
            "invalid_request_error",
            "invalid_call_id",
        );
    };
    let Some(stored) = live_state.calls.peek(&call_id) else {
        return realtime_error(
            404,
            "Realtime call not found",
            "invalid_request_error",
            "realtime_call_not_found",
        );
    };
    if !stored.owner_key.is_empty()
        && (principal.key != stored.owner_key || principal.provider != stored.owner_provider)
    {
        return realtime_error(
            403,
            "Realtime call belongs to another API principal",
            "invalid_request_error",
            "realtime_call_scope_mismatch",
        );
    }
    let cfg = rt.config();
    let selection_headers = principal.selection_headers(&headers);
    // Go: a pick pinned to the call's credential that ends with this request.
    let lease = match select_oauth(
        &rt,
        &cfg,
        Some(&stored.auth_id),
        &selection_headers,
        &[],
        Some(&call_id),
    )
    .await
    {
        Ok(lease) => lease,
        Err(rejection) => return rejection.render(true),
    };
    let credential = lease.credential.clone();
    let trace = trace(&credential);
    let traced = |response| with_trace(response, &trace);
    let body = match read_limited(body, live::MAX_BODY).await {
        Ok(body) => body,
        Err(ReadError::TooLarge) => {
            return traced(realtime_error(
                400,
                "Codex live request body too large",
                "invalid_request_error",
                "invalid_request",
            ));
        }
        Err(ReadError::Read(e, status)) => {
            return traced(realtime_error(
                status,
                &format!("failed to read Codex live request: {e}"),
                "invalid_request_error",
                "invalid_request",
            ));
        }
    };
    let mut upstream_headers = live::protocol_headers(&headers);
    let content_type = header_text(&headers, header::CONTENT_TYPE);
    let content_type = String::from_utf8_lossy(ck_wire::gostr::trim_space(content_type.as_bytes())).into_owned();
    if !content_type.is_empty() {
        upstream_headers.push(("Content-Type".into(), content_type));
    }
    let capture = request_capture();
    let target = LiveTarget {
        credential: &credential,
        cfg: &cfg,
        client: &headers,
        session: header_session(&selection_headers, &[], Some(&call_id), &stored.session_id),
        capture: capture.clone(),
    };
    let url = format!(
        "{}/realtime/calls/{call_id}/hangup",
        live::http_base(&rt.executors.codex.live_endpoints().api_base)
    );
    let posted = rt
        .executors
        .codex
        .live_post(&target, &url, upstream_headers, Bytes::from(body))
        .await;
    let upstream = match posted {
        Ok(upstream) => upstream,
        Err(e) => {
            let status = if e.status == 0 { 502 } else { e.status };
            return traced(realtime_error(
                status,
                &String::from_utf8_lossy(&e.body),
                "api_error",
                "realtime_upstream_unavailable",
            ));
        }
    };
    let status = upstream.status;
    let mut response_headers = HeaderMap::new();
    if let Some(content_type) = upstream.headers.get(header::CONTENT_TYPE).filter(|v| !v.is_empty()) {
        response_headers.insert(header::CONTENT_TYPE, content_type.clone());
    }
    copy_headers(&mut response_headers, &upstream.headers, &HANDSHAKE_HEADERS);
    record_metadata(&capture, status, &upstream.headers);
    let (data, read_error) = live::read_limited(upstream).await;
    record_body(&capture, &data, read_error.as_ref());
    drop(lease);
    if read_error.is_some() {
        return traced(realtime_error(
            502,
            "Failed to read Realtime hangup response",
            "api_error",
            "realtime_upstream_unavailable",
        ));
    }
    if (200..300).contains(&status) {
        live_state.calls.complete(&stored, "client_hangup");
    }
    traced(written(status, response_headers, data))
}

/// A WebRTC call without the media relay: nothing may carry its media past this host.
fn no_relay(realtime: bool) -> Response {
    const MESSAGE: &str = "WebRTC calls are not available on this server; connect over the Realtime WebSocket instead";
    if realtime {
        realtime_error(501, MESSAGE, "not_supported_error", "realtime_capability_not_supported")
    } else {
        live_error(false, 501, MESSAGE)
    }
}

/// `writeCapabilityNotSupported`.
fn not_supported(capability: &str) -> Response {
    realtime_error(
        501,
        &format!("{capability} are not supported by the ChatGPT/Codex OAuth upstream"),
        "not_supported_error",
        "realtime_capability_not_supported",
    )
}

pub(super) async fn translation() -> Response {
    not_supported("Realtime translation sessions")
}

pub(super) async fn transcription() -> Response {
    not_supported("Realtime transcription-only sessions")
}

/// `HandleSIPControl`: the action is the last path segment.
pub(super) async fn sip(OriginalUri(uri): OriginalUri) -> Response {
    let action = uri
        .path()
        .trim_matches('/')
        .rsplit('/')
        .next()
        .filter(|a| !a.trim().is_empty())
        .unwrap_or("control");
    not_supported(&format!("Realtime SIP {action}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracked_body_reports_only_bodies_that_never_left() {
        use http_body::Body as _;
        let fired = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let hook = || -> Option<Box<dyn FnOnce() + Send>> {
            let fired = fired.clone();
            Some(Box::new(move || {
                fired.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }))
        };
        drop(TrackedBody::new(b"v=0".to_vec(), hook()));
        assert_eq!(fired.load(std::sync::atomic::Ordering::SeqCst), 1, "dropped unsent");
        let mut sent = TrackedBody::new(b"v=0".to_vec(), hook());
        assert_eq!(sent.size_hint().exact(), Some(3));
        let waker = futures_util::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        let frame = std::pin::Pin::new(&mut sent).poll_frame(&mut cx);
        assert!(matches!(frame, std::task::Poll::Ready(Some(Ok(_)))));
        assert!(sent.is_end_stream());
        drop(sent);
        assert_eq!(
            fired.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "taken frames are not failures"
        );
        drop(TrackedBody::new(Vec::new(), hook()));
        assert_eq!(
            fired.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "empty bodies cannot fail"
        );
        let big = TrackedBody::new(vec![b'a'; 2049], None);
        assert_eq!(big.size_hint().exact(), None, "past 2048 bytes Go chunks");
        assert_eq!(TrackedBody::new(vec![b'a'; 2048], None).size_hint().exact(), Some(2048));
    }

    /// Values from Go 1.26 `http.DetectContentType`.
    #[test]
    fn sniff_matches_go_for_live_bodies() {
        assert_eq!(
            sniff(b"v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\n"),
            "text/plain; charset=utf-8"
        );
        assert_eq!(sniff(br#"{"sdp":"v=0"}"#), "text/plain; charset=utf-8");
        assert_eq!(sniff(b"  <html><body>x"), "text/html; charset=utf-8");
        assert_eq!(sniff(b"<htmlx"), "text/plain; charset=utf-8");
        assert_eq!(sniff(b"<?xml version"), "text/xml; charset=utf-8");
        assert_eq!(sniff(b"a\x00b"), "application/octet-stream");
        assert_eq!(sniff(b"\xEF\xBB\xBFx"), "text/plain; charset=utf-8");
    }
}

/// Go live_test.go's relay cases (`TestHandlerRelaysWebRTCMediaSDP`,
/// `TestHandlerClosesUnretainedMediaSession`) with a fake relay, and the call's data channel
/// as a live session.
#[cfg(test)]
mod relay_tests {
    use std::sync::Mutex;

    use axum::extract::State as AxumState;
    use ck_exec::session::Frame;
    use futures_util::future::BoxFuture;
    use tokio::sync::mpsc;

    use super::super::relay::{Events, Hold, MediaRelay, MediaSession, NewSession, RelayError, Route, Rtp};
    use super::*;

    #[derive(Default)]
    struct FakeSession {
        downstream: String,
        answer_error: Option<RelayError>,
        upstream_answer: Mutex<String>,
        call_id: Mutex<String>,
        call_id_at_accept: Mutex<String>,
        closed: Mutex<Option<String>>,
        events: Mutex<Option<Events>>,
    }

    /// The far ends of a fake call's data channel: what each peer sends, and what reaches it.
    struct Peers {
        client: mpsc::Sender<Frame>,
        to_client: mpsc::Receiver<Frame>,
        upstream: mpsc::Sender<Frame>,
        to_upstream: mpsc::Receiver<Frame>,
    }

    impl FakeSession {
        fn with_events(&self) -> Peers {
            let (client, from_client) = mpsc::channel(8);
            let (to_client_tx, to_client) = mpsc::channel(8);
            let (upstream, from_upstream) = mpsc::channel(8);
            let (to_upstream_tx, to_upstream) = mpsc::channel(8);
            *self.events.lock().unwrap() = Some(Events {
                from_client,
                to_client: to_client_tx,
                from_upstream,
                to_upstream: to_upstream_tx,
            });
            Peers {
                client,
                to_client,
                upstream,
                to_upstream,
            }
        }
    }

    impl MediaSession for FakeSession {
        fn accept_upstream_answer(&self, answer: String) -> BoxFuture<'_, Result<String, RelayError>> {
            Box::pin(async move {
                *self.upstream_answer.lock().unwrap() = answer;
                *self.call_id_at_accept.lock().unwrap() = self.call_id.lock().unwrap().clone();
                match &self.answer_error {
                    Some(e) => Err(e.clone()),
                    None => Ok(self.downstream.clone()),
                }
            })
        }
        fn set_call_id(&self, call_id: &str) {
            *self.call_id.lock().unwrap() = call_id.into();
        }
        fn events(&self) -> Option<Events> {
            self.events.lock().unwrap().take()
        }
        fn rtp(&self) -> Rtp {
            Rtp {
                client: [3, 30],
                upstream: [4, 40],
            }
        }
        fn close(&self, reason: &str) {
            self.closed.lock().unwrap().get_or_insert_with(|| reason.into());
        }
    }

    struct FakeRelay {
        session: Arc<FakeSession>,
        error: Option<RelayError>,
        seen: Mutex<Option<(String, Route)>>,
    }

    impl MediaRelay for FakeRelay {
        fn new_session(&self, offer: String, route: Route, _: Hold) -> BoxFuture<'_, NewSession> {
            Box::pin(async move {
                *self.seen.lock().unwrap() = Some((offer, route));
                match &self.error {
                    Some(e) => Err(e.clone()),
                    None => Ok((
                        self.session.clone() as Arc<dyn MediaSession>,
                        "v=0\r\no=gateway-offer\r\n".to_owned(),
                    )),
                }
            })
        }
    }

    type Seen = Arc<Mutex<Vec<Bytes>>>;

    async fn serve(app: axum::Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        url
    }

    /// A proxy whose upstream answers `status` with `location`, and its live state.
    async fn start(
        relay: Option<Arc<FakeRelay>>,
        status: u16,
        location: Option<&'static str>,
        yaml: &str,
    ) -> (String, Arc<Live>, Seen) {
        let (proxy, live, seen, _) = start_runtime(relay, status, location, yaml).await;
        (proxy, live, seen)
    }

    /// [`start`] and its runtime.
    async fn start_runtime(
        relay: Option<Arc<FakeRelay>>,
        status: u16,
        location: Option<&'static str>,
        yaml: &str,
    ) -> (String, Arc<Live>, Seen, Arc<Runtime>) {
        let seen = Seen::default();
        let upstream = axum::Router::new()
            .fallback(move |AxumState(seen): AxumState<Seen>, body: Bytes| async move {
                seen.lock().unwrap().push(body);
                let mut response =
                    (StatusCode::from_u16(status).unwrap(), "v=0\r\no=upstream-answer\r\n").into_response();
                response
                    .headers_mut()
                    .insert(header::CONTENT_TYPE, HeaderValue::from_static("application/sdp"));
                if let Some(location) = location {
                    response
                        .headers_mut()
                        .insert(header::LOCATION, HeaderValue::from_static(location));
                }
                response
            })
            .with_state(seen.clone());
        let upstream_url = serve(upstream).await;
        let executor = ck_exec::codex::CodexExecutor::with_client(
            wreq::Client::new(),
            ck_exec::codex_oauth::CodexOAuth::new(wreq::Client::new()),
        )
        .with_live_endpoints(format!("{upstream_url}/calls"), "ws://127.0.0.1:9/v1");
        let meta = serde_json::json!({
            "type": "codex", "access_token": "oauth-token", "email": "voice@example.com",
            // Go's test uses a SOCKS URL; `direct` proves the same precedence while the
            // upstream call still reaches the loopback mock.
            "proxy_url": "direct",
        });
        let credential = ck_core::credential::Credential::from_file(
            std::path::Path::new("/fake"),
            std::path::Path::new("/fake/codex-user.json"),
            meta.as_object().unwrap().clone(),
        )
        .unwrap();
        let rt = Arc::new(crate::testing::runtime(
            ck_core::config::Config::parse(yaml).unwrap(),
            vec![credential],
            ck_exec::Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
                codex: executor,
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
        ));
        let mut live = Live::default();
        live.relays.fixed = relay.map(|r| r as Arc<dyn MediaRelay>);
        let live = Arc::new(live);
        let app = axum::Router::new()
            .merge(super::super::routes_with(&rt, live.clone()))
            .with_state(rt.clone());
        (serve(app).await, live, seen, rt)
    }

    const BOUNDARY: &str = "media-relay-boundary";

    async fn post(proxy: &str) -> wreq::Response {
        post_to(proxy, "/v1/live").await
    }

    async fn post_to(proxy: &str, path: &str) -> wreq::Response {
        let body = format!(
            "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"sdp\"\r\n\r\nv=0\r\no=desktop-offer\r\n\r\n--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"session\"\r\n\r\n{{\"model\":\"gpt-live-1-codex\"}}\r\n--{BOUNDARY}--\r\n"
        );
        wreq::Client::new()
            .post(format!("{proxy}{path}"))
            .header("content-type", format!("multipart/form-data; boundary={BOUNDARY}"))
            .body(body)
            .send()
            .await
            .unwrap()
    }

    fn fake(answer_error: Option<RelayError>, error: Option<RelayError>) -> Arc<FakeRelay> {
        Arc::new(FakeRelay {
            session: Arc::new(FakeSession {
                downstream: "v=0\r\no=downstream-answer\r\n".into(),
                answer_error,
                ..FakeSession::default()
            }),
            error,
            seen: Mutex::default(),
        })
    }

    #[tokio::test]
    async fn relays_sdp_both_ways_and_keeps_the_session_with_the_call() {
        let relay = fake(None, None);
        let (proxy, live, seen) = start(
            Some(relay.clone()),
            201,
            Some("/v1/live/call-123"),
            "requests: {proxy-url: http://global-proxy.example:8080}",
        )
        .await;
        let response = post(&proxy).await;
        assert_eq!(response.status().as_u16(), 201);
        assert_eq!(response.headers()["content-type"], "application/sdp");
        assert_eq!(response.text().await.unwrap(), "v=0\r\no=downstream-answer\r\n");
        let (offer, route) = relay.seen.lock().unwrap().take().unwrap();
        assert_eq!(offer, "v=0\r\no=desktop-offer\r\n");
        assert_eq!(
            ck_exec::proxy::Proxy::parse(&route.proxy_url),
            ck_exec::proxy::Proxy::Direct,
            "the credential's proxy wins over the global one"
        );
        assert_eq!(route.credential, "voice@example.com");
        assert!(!route.auth_index.is_empty());
        let sent: serde_json::Value = serde_json::from_slice(&seen.lock().unwrap()[0]).unwrap();
        assert_eq!(
            sent["sdp"], "v=0\r\no=gateway-offer\r\n",
            "the relay's offer goes upstream"
        );
        assert_eq!(sent["session"]["model"], "gpt-live-1-codex");
        let session = &relay.session;
        assert_eq!(*session.upstream_answer.lock().unwrap(), "v=0\r\no=upstream-answer\r\n");
        assert_eq!(
            *session.call_id_at_accept.lock().unwrap(),
            "call-123",
            "call ID set before the answer"
        );
        assert!(session.closed.lock().unwrap().is_none(), "retained session stays open");
        assert!(live.calls.peek("call-123").unwrap().media.is_some());
    }

    /// No media goes straight to the upstream: without a relay, in a default build or with
    /// `codex.live-media-relay` disabled, every call answers 501 before the upstream is asked.
    #[tokio::test]
    async fn calls_without_the_relay_answer_501() {
        let (proxy, live, seen) = start(None, 201, Some("/v1/live/call-1"), "{}").await;
        let message = "WebRTC calls are not available on this server; connect over the Realtime WebSocket instead";
        // (route, response body)
        let rows = [
            ("/v1/live", format!(r#"{{"error":"{message}"}}"#)),
            (
                "/v1/realtime/calls",
                format!(
                    r#"{{"error":{{"message":"{message}","type":"not_supported_error","param":null,"code":"realtime_capability_not_supported"}}}}"#
                ),
            ),
        ];
        for (path, body) in rows {
            let response = post_to(&proxy, path).await;
            assert_eq!(response.status().as_u16(), 501, "{path}");
            let text = response.text().await.unwrap();
            let got: serde_json::Value = serde_json::from_str(&text).unwrap();
            let want: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(got, want, "{path}: {text}");
        }
        assert!(seen.lock().unwrap().is_empty(), "the upstream was never asked");
        assert!(live.calls.peek("call-1").is_none());
    }

    #[derive(Clone, Default)]
    struct LogCapture(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for LogCapture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl LogCapture {
        fn ends(&self) -> Vec<String> {
            let text = String::from_utf8(self.0.lock().unwrap().clone()).unwrap();
            text.lines()
                .filter(|line| line.contains("live_session_end"))
                .map(str::to_owned)
                .collect()
        }
    }

    async fn next(rx: &mut mpsc::Receiver<Frame>) -> Option<Frame> {
        tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("a message within 5 s")
    }

    fn text(json: &str) -> Frame {
        Frame::Text(Bytes::from(json.to_owned()))
    }

    #[derive(Clone, Copy, Debug)]
    enum Ending {
        ClientClose,
        ClientGone,
        UpstreamClose,
        UpstreamGone,
        Hangup,
    }

    /// A stored call's `oai-events` data channel is a live session: admitted client messages
    /// and upstream messages reach the other peer unchanged and in order, a refused one gets
    /// the `error` event and never reaches the upstream, `response.done` usage is recorded per
    /// response, and every end logs one `live_session_end`, forgets the call and closes both
    /// media peers.
    #[tokio::test]
    async fn data_channel_runs_as_a_live_session() {
        let capture = LogCapture::default();
        let writer = capture.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(move || writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let append = r#"{ "type" :"input_audio_buffer.append","audio":"AAAA"}"#;
        let create = r#"{"type":"response.create"}"#;
        let created = r#"{"type":"response.created","response":{"id":"resp_1","status":"in_progress"}}"#;
        let done = r#"{"type":"response.done","response":{"id":"resp_1","status":"completed","usage":{"total_tokens":15,"input_tokens":10,"output_tokens":5}}}"#;
        // (how it ends, cause, initiator, the reason the media closed with)
        let rows = [
            (Ending::ClientClose, "client_close", "client", "client_close"),
            (Ending::ClientGone, "client_gone", "client", "client_gone"),
            (Ending::UpstreamClose, "upstream_close", "upstream", "upstream_close"),
            (Ending::UpstreamGone, "upstream_gone", "upstream", "upstream_gone"),
            (Ending::Hangup, "hangup", "client", "client_hangup"),
        ];
        for (ending, cause, initiator, closed) in rows {
            capture.0.lock().unwrap().clear();
            tracing::callsite::rebuild_interest_cache();
            let relay = fake(None, None);
            let peers = relay.session.with_events();
            let (proxy, live, _, rt) = start_runtime(Some(relay.clone()), 201, Some("/v1/live/call-123"), "{}").await;
            rt.usage_queue().configure(
                true,
                &ck_core::config::Config::parse("observability: {usage: {usage-statistics-enabled: true}}\n").unwrap(),
            );
            let response = post(&proxy).await;
            assert_eq!(response.status().as_u16(), 201, "{}", response.text().await.unwrap());
            let Peers {
                client,
                mut to_client,
                upstream,
                mut to_upstream,
            } = peers;

            client.send(text(append)).await.unwrap();
            // Refused: an unknown event, and a model the call was not made for.
            client.send(text(r#"{"type":"bogus.event"}"#)).await.unwrap();
            client
                .send(text(
                    r#"{"type":"session.update","session":{"type":"realtime","model":"gpt-other"}}"#,
                ))
                .await
                .unwrap();
            client.send(text(create)).await.unwrap();
            assert_eq!(next(&mut to_upstream).await, Some(text(append)), "{ending:?}");
            for code in ["unknown_event_type", "invalid_value"] {
                let Some(Frame::Text(refused)) = next(&mut to_client).await else {
                    panic!("{ending:?}: no error event");
                };
                let refused: serde_json::Value = serde_json::from_slice(&refused).unwrap();
                assert_eq!(refused["type"], "error", "{ending:?}");
                assert_eq!(refused["error"]["code"], code, "{ending:?}: {refused}");
            }
            assert_eq!(
                next(&mut to_upstream).await,
                Some(text(create)),
                "{ending:?}: the refused events never went upstream"
            );
            upstream.send(text(created)).await.unwrap();
            upstream.send(text(done)).await.unwrap();
            assert_eq!(next(&mut to_client).await, Some(text(created)), "{ending:?}");
            assert_eq!(next(&mut to_client).await, Some(text(done)), "{ending:?}");

            let mut kept = Vec::new();
            match ending {
                Ending::ClientClose => client.send(Frame::Close(None)).await.unwrap(),
                Ending::ClientGone => drop(client),
                Ending::UpstreamClose => upstream.send(Frame::Close(None)).await.unwrap(),
                Ending::UpstreamGone => drop(upstream),
                // What an accepted hangup does.
                Ending::Hangup => {
                    let call = live.calls.peek("call-123").unwrap();
                    live.calls.complete(&call, "client_hangup");
                }
            }
            for _ in 0..500 {
                if !capture.ends().is_empty() && live.calls.peek("call-123").is_none() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert!(live.calls.peek("call-123").is_none(), "{ending:?}: the call ended");
            assert_eq!(
                relay.session.closed.lock().unwrap().as_deref(),
                Some(closed),
                "{ending:?}: both media peers closed"
            );
            let ends = capture.ends();
            assert_eq!(ends.len(), 1, "{ending:?}: {ends:?}");
            let end = &ends[0];
            for field in [
                format!("cause=\"{cause}\""),
                format!("initiator=\"{initiator}\""),
                "call_id=\"call-123\"".to_owned(),
                "client_rtp_packets=3".to_owned(),
                "upstream_rtp_bytes=40".to_owned(),
            ] {
                assert!(end.contains(&field), "{ending:?}: {field} missing in {end}");
            }
            for absent in ["ping_interval_s", "input_audio_buffer", "AAAA"] {
                assert!(!end.contains(absent), "{ending:?}: {absent} in {end}");
            }

            for _ in 0..500 {
                kept.extend(rt.usage_queue().pop_oldest(10));
                if kept.len() >= 2 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            let mut usage: Vec<(String, Option<u64>, Option<u64>, String)> = kept
                .iter()
                .map(|record| {
                    let record: serde_json::Value = serde_json::from_slice(record).unwrap();
                    (
                        record["usage_scope"].as_str().unwrap_or_default().to_owned(),
                        record["tokens"]["input_tokens"].as_u64(),
                        record["tokens"]["output_tokens"].as_u64(),
                        record["endpoint"].as_str().unwrap().to_owned(),
                    )
                })
                .collect();
            usage.sort();
            let endpoint = "POST /v1/live".to_owned();
            assert_eq!(
                usage,
                [
                    (String::new(), None, None, endpoint.clone()),
                    ("response:resp_1".into(), Some(10), Some(5), endpoint),
                ],
                "{ending:?}"
            );
        }
    }

    #[tokio::test]
    async fn unretained_sessions_close_with_request_not_retained() {
        // Upstream rejection: the status passes through.
        let relay = fake(None, None);
        let (proxy, live, _) = start(Some(relay.clone()), 401, Some("/v1/live/call-123"), "{}").await;
        assert_eq!(post(&proxy).await.status().as_u16(), 401);
        assert_eq!(
            relay.session.closed.lock().unwrap().as_deref(),
            Some("request_not_retained")
        );
        assert!(live.calls.peek("call-123").is_none());
        // The relay rejects the upstream answer.
        let relay = fake(Some(RelayError::new("invalid answer")), None);
        let (proxy, live, _) = start(Some(relay.clone()), 201, Some("/v1/live/call-123"), "{}").await;
        let response = post(&proxy).await;
        assert_eq!(response.status().as_u16(), 502);
        assert_eq!(response.text().await.unwrap(), r#"{"error":"invalid answer"}"#);
        assert_eq!(
            relay.session.closed.lock().unwrap().as_deref(),
            Some("request_not_retained")
        );
        assert!(live.calls.peek("call-123").is_none());
        // A relayed call needs a call ID.
        let relay = fake(None, None);
        let (proxy, _, _) = start(Some(relay.clone()), 201, None, "{}").await;
        let response = post(&proxy).await;
        assert_eq!(response.status().as_u16(), 502);
        assert_eq!(
            response.text().await.unwrap(),
            r#"{"error":"Codex live response is missing a valid call ID"}"#
        );
        assert_eq!(
            relay.session.closed.lock().unwrap().as_deref(),
            Some("request_not_retained")
        );
    }

    #[tokio::test]
    async fn relay_failures_never_reach_the_upstream() {
        let relay = fake(None, Some(RelayError::new("Codex live media relay capacity exhausted")));
        let (proxy, _, seen) = start(Some(relay), 201, Some("/v1/live/call-1"), "{}").await;
        let response = post(&proxy).await;
        assert_eq!(response.status().as_u16(), 502);
        assert_eq!(
            response.text().await.unwrap(),
            r#"{"error":"Codex live media relay capacity exhausted"}"#
        );
        assert!(seen.lock().unwrap().is_empty());
        // An unusable relay section answers 503 before any selection.
        let (proxy, _, seen) = start(
            None,
            201,
            Some("/v1/live/call-1"),
            "oauth:\n  providers:\n    codex:\n      live-media-relay:\n        allow-private-remote-ips: true\n        disable-private-remote-ips: true\n",
        )
        .await;
        let response = post(&proxy).await;
        assert_eq!(response.status().as_u16(), 503);
        assert!(response.text().await.unwrap().contains("cannot set both"));
        assert!(seen.lock().unwrap().is_empty());
    }
}
