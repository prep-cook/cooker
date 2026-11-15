//! Request-side usage facts and upstream capture through `Client::usage` (contract G4): what a
//! generation and a count report on each route, and that no response usage is parsed.

use std::sync::{Arc, Mutex};

use ck_core::exec::{CaptureEvent, CaptureObserver, CaptureSink, UsageObserver, UsageSink};
use ck_core::format::Format;

use super::*;

/// Every usage and capture call, by name.
#[derive(Default)]
struct Observed(Mutex<Vec<String>>);

impl Observed {
    fn push(&self, call: String) {
        self.0.lock().unwrap().push(call);
    }

    fn calls(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
}

impl UsageObserver for Observed {
    fn response_body(&self, _format: Format, _body: &[u8]) {
        self.push("response_body".into());
    }
    fn response_line(&self, _format: Format, _line: &[u8]) {
        self.push("response_line".into());
    }
    fn response_usage_line(&self, _format: Format, _line: &[u8]) {
        self.push("response_usage_line".into());
    }
    fn response_model(&self, _model: &str) {
        self.push("response_model".into());
    }
    fn request(&self, format: Format, payload: &[u8]) {
        self.push(format!(
            "request {} {}",
            format.as_str(),
            String::from_utf8_lossy(payload)
        ));
    }
    fn upstream_model(&self, model: &str) {
        self.push(format!("upstream_model {model}"));
    }
    fn round_trip_started(&self) {
        self.push("round_trip_started".into());
    }
    fn first_byte(&self) {
        self.push("first_byte".into());
    }
}

impl CaptureObserver for Observed {
    fn record(&self, event: CaptureEvent<'_>) {
        let call = match event {
            CaptureEvent::Request(request) => format!("capture request {} {}", request.provider, request.url),
            CaptureEvent::ResponseMetadata(status, _) => format!("capture metadata {status}"),
            CaptureEvent::ResponseChunk(_) => "capture chunk".into(),
            CaptureEvent::ResponseError(text) => format!("capture error {text}"),
            _ => "capture other".into(),
        };
        self.push(call);
    }
}

fn observed(exchange: &mut Exchange) -> Arc<Observed> {
    let observed = Arc::new(Observed::default());
    exchange.usage = UsageSink::new(observed.clone()).with_capture(CaptureSink::new(observed.clone()));
    observed
}

fn assert_has(calls: &[String], wanted: &[String]) {
    for call in wanted {
        assert!(calls.contains(call), "{call} missing from {calls:?}");
    }
    assert!(!calls.iter().any(|c| c.starts_with("response_")), "{calls:?}");
}

#[tokio::test]
async fn a_gemini_route_generation_reports_its_request_side_facts() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::sse(SSE)]).await;
    let mut exchange = exchange(&upstream);
    let observed = observed(&mut exchange);
    let run = run_on(&streams, &exchange, Client::Gemini, Operation::Generate, GEMINI, 64).await;
    produce(&streams, run.started.unwrap()).await.0.unwrap();
    let wire = String::from_utf8(upstream.captured()[0].body.clone()).unwrap();
    let url = format!("{}{MODELS}:streamGenerateContent?alt=sse", upstream.url());
    let calls = observed.calls();
    assert_has(
        &calls,
        &[
            format!("request gemini {wire}"),
            format!("upstream_model {MODEL}"),
            "round_trip_started".into(),
            format!("capture request vertex {url}"),
            "capture metadata 200".into(),
            "first_byte".into(),
            "capture chunk".into(),
        ],
    );
}

/// A count is captured but is not generation usage.
#[tokio::test]
async fn a_gemini_route_count_is_captured_only() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::json(r#"{"totalTokens":2}"#)]).await;
    let mut exchange = exchange(&upstream);
    let observed = observed(&mut exchange);
    let run = run_on(&streams, &exchange, Client::Gemini, Operation::CountTokens, GEMINI, 64).await;
    produce(&streams, run.started.unwrap()).await.0.unwrap();
    let calls = observed.calls();
    let url = format!("{}{MODELS}:countTokens", upstream.url());
    assert_has(
        &calls,
        &[format!("capture request vertex {url}"), "capture chunk".into()],
    );
    for call in ["request ", "upstream_model ", "round_trip_started", "first_byte"] {
        assert!(!calls.iter().any(|c| c.starts_with(call)), "{call} in {calls:?}");
    }
}

#[tokio::test]
async fn the_openapi_route_reports_its_request_side_facts() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::sse(openapi::chat_sse())]).await;
    let base = format!("{}/api", upstream.url());
    let mut exchange = Exchange::new(credential("vertex", &base, KEY), openapi::PARTNER, "/client");
    let observed = observed(&mut exchange);
    let body = r#"{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let run = run_on(&streams, &exchange, Client::Chat, Operation::Generate, body, 64).await;
    produce(&streams, run.started.unwrap()).await.0.unwrap();
    let wire = String::from_utf8(upstream.captured()[0].body.clone()).unwrap();
    let calls = observed.calls();
    assert_has(
        &calls,
        &[
            format!("request openai {wire}"),
            format!("upstream_model {}", openapi::PARTNER),
            "round_trip_started".into(),
            format!("capture request vertex {base}/v1/endpoints/openapi/chat/completions"),
            "capture metadata 200".into(),
            "first_byte".into(),
            "capture chunk".into(),
        ],
    );
}

/// A redirect `Location` may carry credentials; neither the failure nor its capture names it.
const LEAKY_REDIRECT: &str = "http://user:sk-review-secret@127.0.0.1:bad/?api_key=sk-review-secret";

#[tokio::test]
async fn a_redirect_location_never_reaches_the_failure_or_its_capture() {
    let streams = Streams::new();
    let redirect = Reply::status(302, "text/plain", "").header("location", LEAKY_REDIRECT);
    let upstream = StreamUpstream::start([redirect]).await;
    let mut exchange = exchange(&upstream);
    let observed = observed(&mut exchange);
    let run = run_on(&streams, &exchange, Client::Gemini, Operation::Generate, GEMINI, 64).await;
    let error = run.started.err().expect("the redirect fails the attempt");
    assert!(
        !String::from_utf8_lossy(&error.body).contains("sk-review-secret"),
        "{error:?}"
    );
    let calls = observed.calls();
    assert!(calls.iter().any(|c| c.starts_with("capture error")), "{calls:?}");
    assert!(!calls.iter().any(|c| c.contains("sk-review-secret")), "{calls:?}");
}
