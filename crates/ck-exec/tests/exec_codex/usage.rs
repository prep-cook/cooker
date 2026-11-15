//! Request-side usage facts and upstream capture (contract G4): the body sent, the model the
//! upstream is expected to serve, the round trip, its stages and first byte, and the
//! captured exchange. Response usage is the canonical stream's, never a `response_*` call.

use std::sync::{Arc, Mutex};

use ck_core::exec::{CaptureEvent, CaptureObserver, CaptureSink, UpstreamStage, UsageObserver, UsageSink};
use ck_core::format::Format;
use ck_translate::codec::Operation;

use crate::common::*;
use crate::support::Streams;
use crate::support::upstream::{Reply, StreamUpstream};

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
        self.push(format!("request {format:?} {}", String::from_utf8_lossy(payload)));
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
    fn upstream_stage(&self, _stage: UpstreamStage) {
        self.push("upstream_stage".into());
    }
}

impl CaptureObserver for Observed {
    fn record(&self, event: CaptureEvent<'_>) {
        let name = match event {
            CaptureEvent::Request(info) => format!("capture request {}", info.url),
            CaptureEvent::ResponseChunk(_) => "capture chunk".into(),
            _ => "capture other".into(),
        };
        self.push(name);
    }
}

/// Generation and compact report the body each sent upstream, in the format the old path
/// names (Codex for generation, Responses for compact), and capture their exchange.
#[tokio::test]
async fn request_side_facts_reach_the_usage_sink_and_response_usage_does_not() {
    let compaction = r#"{"id":"cmp_1","object":"response.compaction","created_at":4,"model":"gpt-5","output":[{"id":"i","type":"compaction","encrypted_content":"secret"}],"usage":{"input_tokens":3,"output_tokens":1,"total_tokens":4}}"#;
    let cases = [
        (
            Operation::Generate,
            Reply::sse(sse()),
            CHAT,
            "chat",
            "Codex",
            "/responses",
        ),
        (
            Operation::Compact,
            Reply::json(compaction),
            br#"{"model":"client-model","input":[{"role":"user","content":"hi"}]}"#.as_slice(),
            "responses",
            "OpenAIResponse",
            "/responses/compact",
        ),
    ];
    for (operation, reply, body, protocol, format, path) in cases {
        let upstream = StreamUpstream::start([reply]).await;
        let streams = Streams::new();
        let mut exchange = exchange(&upstream);
        let observed = Arc::new(Observed::default());
        exchange.usage = UsageSink::new(observed.clone()).with_capture(CaptureSink::new(observed.clone()));
        let (started, id, _request) = run(&streams, &exchange, protocol, operation, body, 64).await;
        finish(&streams, started.unwrap(), id).await.0.unwrap();
        let calls = observed.calls();
        let wire = String::from_utf8(upstream.captured()[0].body.clone()).unwrap();
        assert!(calls.contains(&format!("request {format} {wire}")), "{calls:?}");
        let expected = [
            format!("upstream_model {MODEL}"),
            "round_trip_started".into(),
            "first_byte".into(),
            "upstream_stage".into(),
            format!("capture request {}{path}", upstream.url()),
            "capture chunk".into(),
        ];
        for call in expected {
            assert!(calls.contains(&call), "{call} missing from {calls:?}");
        }
        assert!(!calls.iter().any(|c| c.starts_with("response_")), "{calls:?}");
    }
}
