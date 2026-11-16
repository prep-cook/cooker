//! Media routes on the execution contract (E-PLAN §2.3, E7), through the real router against a
//! local upstream: speech commits at its first audio byte and resumes on `/v1/streams` from a
//! byte cursor; video job ids belong to the caller that created them, and a full job table
//! refuses a create before anything is sent.

#[path = "support/stream_upstream.rs"]
mod stream_upstream;

use std::sync::Arc;
use std::time::{Duration, Instant};

use ck_core::config::Config;
use ck_exec::Executors;
use ck_exec::claude::ClaudeExecutor;
use stream_upstream::{Reply, StreamUpstream};

const AUDIO: &[u8] = b"RIFF\x00\x01\x02\x03WAVEdata";
const SPEECH: &str = r#"{"model":"tts-1","input":"hello","voice":"alloy","response_format":"wav"}"#;

struct Proxy {
    base: String,
    client: wreq::Client,
}

/// The router over a runtime whose xAI credential points at `upstream`, with two client keys and
/// a video job table of `capacity`.
async fn proxy(upstream: &StreamUpstream, capacity: usize) -> Proxy {
    let config = Config::parse(&format!(
        "access:\n  api-keys: [client-a, client-b]\napi-keys:\n  xai:\n    - name: xai-1\n      base-url: {}/v1\n      keys:\n        - api-key: sk-fake-xai-a\n        - api-key: sk-fake-xai-b\nrouting:\n  strategy: round-robin\n",
        upstream.url()
    ))
    .unwrap();
    let credentials = ck_core::config::credentials::load(&config);
    let openai = ck_exec::OpenAIExecutors {
        xai: ck_exec::xai::XaiExecutor::default().with_video_capacity(capacity),
        ..Default::default()
    };
    let executors = Executors {
        claude: ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: Default::default(),
        devices: Default::default(),
        openai,
        google: Default::default(),
    };
    let rt = Arc::new(ck_server::testing::runtime(config, credentials, executors));
    ck_server::install_registry(&rt);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let app = ck_server::router(rt);
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Proxy {
        base,
        client: wreq::Client::new(),
    }
}

impl Proxy {
    fn request(&self, method: wreq::Method, key: &str, path: &str) -> wreq::RequestBuilder {
        self.client
            .request(method, format!("{}{path}", self.base))
            .header("authorization", format!("Bearer {key}"))
    }

    fn speech(&self) -> wreq::RequestBuilder {
        self.request(wreq::Method::POST, "client-a", "/v1/audio/speech")
            .header("content-type", "application/json")
            .body(SPEECH)
    }
}

/// Speech reaches the client only from its first audio byte: until then nothing is sent, not
/// even the status line, and an attempt that ends before it, an empty answer included, fails over
/// to the next credential with the client none the wiser. When every credential answers empty,
/// the client gets Go's `empty_stream` failure.
#[tokio::test]
async fn speech_commits_at_its_first_audio_byte() {
    let late = Duration::from_millis(400);
    // (case, upstream replies, upstream requests, earliest response head, status)
    let cases = [
        (
            "the response head waits for the first audio byte",
            vec![Reply::ok("audio/wav", AUDIO).first_byte_after(late)],
            1,
            late,
            200,
        ),
        (
            "an answer cut before its first byte fails over",
            vec![
                Reply::ok("audio/wav", AUDIO).cut_after(0),
                Reply::ok("audio/wav", AUDIO),
            ],
            2,
            Duration::ZERO,
            200,
        ),
        (
            "an empty answer fails over",
            vec![Reply::ok("audio/wav", ""), Reply::ok("audio/wav", AUDIO)],
            2,
            Duration::ZERO,
            200,
        ),
        (
            "every credential answering empty is an empty stream",
            vec![Reply::ok("audio/wav", ""), Reply::ok("audio/wav", "")],
            2,
            Duration::ZERO,
            500,
        ),
    ];
    for (case, replies, sends, earliest, status) in cases {
        let upstream = StreamUpstream::start(replies).await;
        let proxy = proxy(&upstream, 4096).await;
        let started = Instant::now();
        let response = proxy.speech().send().await.unwrap();
        assert!(
            started.elapsed() >= earliest,
            "{case}: head after {:?}",
            started.elapsed()
        );
        assert_eq!(response.status(), status, "{case}");
        assert_eq!(upstream.requests(), sends, "{case}");
        if status != 200 {
            continue;
        }
        assert_eq!(response.headers()["content-type"], "audio/wav", "{case}");
        assert!(response.headers().contains_key("x-ck-stream-id"), "{case}");
        assert_eq!(&response.bytes().await.unwrap()[..], AUDIO, "{case}");
    }
}

/// A committed speech body resumes from a byte cursor (D3): the suffix after the cursor with the
/// audio's own media type, for the caller that made the request only, and nothing is sent upstream.
#[tokio::test]
async fn raw_audio_resumes_from_a_byte_cursor() {
    let upstream = StreamUpstream::start([Reply::ok("audio/wav", AUDIO)]).await;
    let proxy = proxy(&upstream, 4096).await;
    let response = proxy.speech().send().await.unwrap();
    let id = response.headers()["x-ck-stream-id"].to_str().unwrap().to_owned();
    assert_eq!(&response.bytes().await.unwrap()[..], AUDIO);
    // (case, client key, query, status, body after the cursor)
    type Case = (&'static str, &'static str, &'static str, u16, Option<&'static [u8]>);
    let cases: [Case; 5] = [
        ("from the start", "client-a", "after=v1.b.0", 200, Some(AUDIO)),
        ("after three bytes", "client-a", "after=v1.b.3", 200, Some(&AUDIO[3..])),
        ("at the end", "client-a", "after=v1.b.16", 200, Some(&[])),
        ("an event cursor on a raw body", "client-a", "after=v1.e.1", 400, None),
        ("another caller", "client-b", "after=v1.b.3", 410, None),
    ];
    for (case, key, query, status, body) in cases {
        let resumed = proxy
            .request(wreq::Method::GET, key, &format!("/v1/streams?{query}"))
            .header("x-ck-stream-id", &id)
            .send()
            .await
            .unwrap();
        assert_eq!(resumed.status(), status, "{case}");
        if let Some(body) = body {
            assert_eq!(resumed.headers()["content-type"], "audio/wav", "{case}");
            assert_eq!(&resumed.bytes().await.unwrap()[..], body, "{case}");
        }
    }
    assert_eq!(upstream.requests(), 1, "a resume sends nothing upstream");
}

/// A video job id is found only by the caller that created it, and a create while the job table
/// is full is refused with 503 (E-Q5): neither sends anything upstream.
#[tokio::test]
async fn video_jobs_belong_to_their_caller_and_a_full_table_sends_nothing() {
    let upstream = StreamUpstream::start([
        Reply::json(r#"{"request_id":"vid_1"}"#),
        Reply::json(r#"{"status":"pending"}"#),
    ])
    .await;
    let proxy = proxy(&upstream, 1).await;
    let create = r#"{"model":"grok-imagine-video","prompt":"waves"}"#;
    // (case, client key, method, path, status, upstream requests after the step)
    let steps = [
        ("a create", "client-a", "POST", "/v1/videos/generations", 200, 1),
        ("another caller's poll", "client-b", "GET", "/v1/videos/vid_1", 404, 1),
        ("the creator's poll", "client-a", "GET", "/v1/videos/vid_1", 200, 2),
        (
            "a create while the table is full",
            "client-b",
            "POST",
            "/v1/videos/generations",
            503,
            2,
        ),
    ];
    for (case, key, method, path, status, sends) in steps {
        let mut request = proxy.request(wreq::Method::from_bytes(method.as_bytes()).unwrap(), key, path);
        if method == "POST" {
            request = request.header("content-type", "application/json").body(create);
        }
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), status, "{case}");
        assert_eq!(upstream.requests(), sends, "{case}");
    }
}
