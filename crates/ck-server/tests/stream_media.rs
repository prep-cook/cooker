//! Media routes on the execution contract (E-PLAN §2.3, E7), through the real router against a
//! local upstream: speech commits at its first audio byte and resumes from a byte cursor; images
//! commit once the answer holds an image and resume from an event cursor; video jobs belong to
//! the caller that created them, are polled and downloaded on their credential and charged once,
//! and a full job table refuses a create before anything is sent; a deferred image job is polled
//! by its caller on its credential and charged once.

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
    rt: Arc<ck_server::Runtime>,
}

/// The router over a runtime whose xAI credential points at `upstream`, with two client keys and
/// a video job table of `capacity`.
async fn proxy(upstream: &StreamUpstream, capacity: usize) -> Proxy {
    proxy_with(upstream, capacity, "").await
}

/// [`proxy`] with `extra` configuration appended.
async fn proxy_with(upstream: &StreamUpstream, capacity: usize, extra: &str) -> Proxy {
    let xai = format!(
        "    - name: xai-1\n      base-url: {}/v1\n      keys:\n        - api-key: sk-fake-xai-a\n        - api-key: sk-fake-xai-b\n",
        upstream.url()
    );
    proxy_over(&xai, capacity, extra).await
}

/// The router over a runtime whose xAI credentials are `xai` (entries of `api-keys.xai`).
async fn proxy_over(xai: &str, capacity: usize, extra: &str) -> Proxy {
    let config = Config::parse(&format!(
        "access:\n  api-keys: [client-a, client-b]\napi-keys:\n  xai:\n{xai}routing:\n  strategy: round-robin\n{extra}"
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
    let app = ck_server::router(rt.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Proxy {
        base,
        client: wreq::Client::new(),
        rt,
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

const VIDEO_FILE: &[u8] = b"\x00\x00\x00\x18ftypmp42video";

/// The job's usage records, after the usage queue drained.
async fn job_usage(proxy: &Proxy, job: &str) -> Vec<serde_json::Value> {
    let mut records = Vec::new();
    for _ in 0..50 {
        records.extend(proxy.rt.usage_queue().pop_oldest(100));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    records
        .iter()
        .map(|r| serde_json::from_slice::<serde_json::Value>(r).unwrap())
        .filter(|r| r["usage_scope"] == format!("job:{job}"))
        .collect()
}

/// A video job belongs to the caller that created it: its polls go to the credential that created
/// it and answer the job as xAI sent it or as an OpenAI video object, pending or finished; its
/// content is the finished file with its host's media type, resumable from a byte cursor; its
/// usage is charged once, whichever request first reports it. Another caller's poll or content is
/// not found, a content request before the file exists fails as before, and a create while the job
/// table is full is refused with 503 (E-Q5): none of these sends anything.
#[tokio::test]
async fn video_jobs_are_served_to_their_caller_on_their_credential_and_charged_once() {
    let files = StreamUpstream::start([Reply::ok("video/mp4", VIDEO_FILE), Reply::ok("video/mp4", VIDEO_FILE)]).await;
    let url = format!("{}/v.mp4", files.url());
    let done = serde_json::json!({
        "status": "done",
        "model": "grok-imagine-video",
        "video": {"url": url, "duration": 8},
        "usage": {"cost_in_usd_ticks": 7}
    });
    let pending = r#"{"status":"pending","progress":40}"#;
    let upstream = StreamUpstream::start([
        Reply::json(r#"{"request_id":"vid_1"}"#),
        Reply::json(pending),
        Reply::json(pending),
        Reply::json(r#"{"status":"pending"}"#),
        Reply::json(done.to_string()),
        Reply::json(done.to_string()),
        Reply::json(done.to_string()),
        Reply::json(done.to_string()),
    ])
    .await;
    let proxy = proxy(&upstream, 1).await;
    let usage = Config::parse("observability: {usage: {usage-statistics-enabled: true}}\n").unwrap();
    proxy.rt.usage_queue().configure(true, &usage);
    let finished = serde_json::json!({
        "id": "vid_1",
        "object": "video",
        "status": "completed",
        "model": "grok-imagine-video",
        "video_url": url,
        "seconds": "8"
    });
    type Body = Option<serde_json::Value>;
    // (case, client key, method, path, status, client body, upstream requests after the step)
    let steps: [(&str, &str, &str, &str, u16, Body, usize); 13] = [
        (
            "an OpenAI create answers the job",
            "client-a",
            "POST",
            "/openai/v1/videos",
            200,
            Some(serde_json::json!({
                "id": "vid_1",
                "object": "video",
                "status": "queued",
                "model": "grok-imagine-video",
                "prompt": "waves",
                "seconds": "4",
                "size": "720x1280",
                "created_at": "<now>",
                "progress": 0
            })),
            1,
        ),
        (
            "another caller's poll",
            "client-b",
            "GET",
            "/v1/videos/vid_1",
            404,
            None,
            1,
        ),
        (
            "another caller's content",
            "client-b",
            "GET",
            "/openai/v1/videos/vid_1/content",
            404,
            None,
            1,
        ),
        (
            "a pending native poll",
            "client-a",
            "GET",
            "/v1/videos/vid_1",
            200,
            Some(serde_json::from_str(pending).unwrap()),
            2,
        ),
        (
            "a pending OpenAI poll",
            "client-a",
            "GET",
            "/openai/v1/videos/vid_1",
            200,
            Some(serde_json::json!({
                "id": "vid_1",
                "object": "video",
                "status": "queued",
                "progress": 40,
                "model": "grok-imagine-video"
            })),
            3,
        ),
        (
            "content before the file exists",
            "client-a",
            "GET",
            "/openai/v1/videos/vid_1/content",
            502,
            None,
            4,
        ),
        (
            "a finished OpenAI poll",
            "client-a",
            "GET",
            "/openai/v1/videos/vid_1",
            200,
            Some(finished),
            5,
        ),
        (
            "the content",
            "client-a",
            "GET",
            "/openai/v1/videos/vid_1/content",
            200,
            None,
            6,
        ),
        (
            "a finished native poll",
            "client-a",
            "GET",
            "/v1/videos/vid_1",
            200,
            Some(done.clone()),
            7,
        ),
        (
            "the content again",
            "client-a",
            "GET",
            "/openai/v1/videos/vid_1/content",
            200,
            None,
            8,
        ),
        (
            "a create while the table is full",
            "client-b",
            "POST",
            "/v1/videos/generations",
            503,
            None,
            8,
        ),
        (
            "an OpenAI create while full",
            "client-b",
            "POST",
            "/openai/v1/videos",
            503,
            None,
            8,
        ),
        (
            "an edit with an option xAI's edit lacks",
            "client-a",
            "POST",
            "/v1/videos/edits",
            400,
            None,
            8,
        ),
    ];
    let mut content = None;
    for (case, key, method, path, status, body, sends) in steps {
        let mut request = proxy.request(wreq::Method::from_bytes(method.as_bytes()).unwrap(), key, path);
        if method == "POST" {
            let model = if path.starts_with("/openai") {
                "sora-2"
            } else {
                "grok-imagine-video"
            };
            let mut create = serde_json::json!({"model": model, "prompt": "waves"});
            if path.ends_with("/edits") {
                create["duration"] = 5.into();
                create["video"] = serde_json::json!({"url": "https://v.x/a.mp4"});
            }
            let create = create.to_string();
            request = request.header("content-type", "application/json").body(create);
        }
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), status, "{case}");
        if path.ends_with("/content") && status == 200 {
            assert_eq!(response.headers()["content-type"], "video/mp4", "{case}");
            let id = response.headers()["x-ck-stream-id"].to_str().unwrap().to_owned();
            assert_eq!(&response.bytes().await.unwrap()[..], VIDEO_FILE, "{case}");
            // The finished OpenAI poll charged the job, and its record drains here; a content
            // request does not charge the job again.
            let jobs = job_usage(&proxy, "vid_1").await;
            let charged = usize::from(content.is_none());
            assert_eq!(jobs.len(), charged, "{case}: the job's usage records: {jobs:?}");
            content = Some(id);
        } else if let Some(body) = body {
            let mut got: serde_json::Value = serde_json::from_str(&response.text().await.unwrap()).unwrap();
            if got["created_at"].as_i64().is_some_and(|at| at > 1_700_000_000) {
                got["created_at"] = "<now>".into();
            }
            assert_eq!(got, body, "{case}");
        }
        assert_eq!(upstream.requests(), sends, "{case}");
    }
    let captured = upstream.captured();
    for poll in &captured[1..] {
        assert_eq!(poll.path, "/v1/videos/vid_1");
        assert_eq!(
            poll.header("authorization"),
            captured[0].header("authorization"),
            "a poll goes to the job's credential"
        );
    }
    let download = &files.captured()[0];
    assert_eq!(download.path, "/v.mp4");
    assert_eq!(
        download.header("authorization"),
        None,
        "the download carries no credential"
    );
    let jobs = job_usage(&proxy, "vid_1").await;
    assert!(jobs.is_empty(), "a later poll does not charge the job again: {jobs:?}");

    // The file resumes from a byte cursor, for its caller only, with nothing sent upstream.
    let id = content.expect("the content was served");
    // (case, client key, query, status, body after the cursor)
    type Resume = (&'static str, &'static str, &'static str, u16, Option<&'static [u8]>);
    let resumes: [Resume; 2] = [
        (
            "after four bytes",
            "client-a",
            "after=v1.b.4",
            200,
            Some(&VIDEO_FILE[4..]),
        ),
        ("another caller", "client-b", "after=v1.b.4", 410, None),
    ];
    for (case, key, query, status, body) in resumes {
        let resumed = proxy
            .request(wreq::Method::GET, key, &format!("/v1/streams?{query}"))
            .header("x-ck-stream-id", &id)
            .send()
            .await
            .unwrap();
        assert_eq!(resumed.status(), status, "{case}");
        if let Some(body) = body {
            assert_eq!(resumed.headers()["content-type"], "video/mp4", "{case}");
            assert_eq!(&resumed.bytes().await.unwrap()[..], body, "{case}");
        }
    }
    assert_eq!(
        (upstream.requests(), files.requests()),
        (8, 2),
        "a resume sends nothing"
    );
}

/// A job's content is downloaded through the proxy of the credential that created the job: of two
/// xAI credentials behind different proxies, only the job's sees the poll and the download. The
/// poll that found the file charges the job when no earlier poll did.
#[tokio::test]
async fn video_content_downloads_through_the_job_credentials_proxy() {
    let replies = || {
        [
            Reply::json(r#"{"request_id":"vid_p"}"#),
            Reply::json(
                r#"{"status":"done","video":{"url":"http://files.test/v.mp4"},"usage":{"cost_in_usd_ticks":7}}"#,
            ),
            Reply::ok("video/mp4", VIDEO_FILE),
        ]
    };
    let proxies = [
        StreamUpstream::start(replies()).await,
        StreamUpstream::start(replies()).await,
    ];
    let xai: String = proxies
        .iter()
        .enumerate()
        .map(|(i, p)| {
            format!(
                "    - name: xai-{i}\n      base-url: http://xai.test/v1\n      proxy-url: {}\n      keys:\n        - api-key: sk-fake-xai-{i}\n",
                p.url()
            )
        })
        .collect();
    let proxy = proxy_over(&xai, 4096, "").await;
    let usage = Config::parse("observability: {usage: {usage-statistics-enabled: true}}\n").unwrap();
    proxy.rt.usage_queue().configure(true, &usage);
    let response = proxy
        .request(wreq::Method::POST, "client-a", "/v1/videos/generations")
        .header("content-type", "application/json")
        .body(r#"{"model":"grok-imagine-video","prompt":"waves"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let response = proxy
        .request(wreq::Method::GET, "client-a", "/openai/v1/videos/vid_p/content")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(&response.bytes().await.unwrap()[..], VIDEO_FILE);
    assert_eq!(
        job_usage(&proxy, "vid_p").await.len(),
        1,
        "the content's poll charges the job"
    );
    let (job, other) = if proxies[0].requests() > 0 {
        (&proxies[0], &proxies[1])
    } else {
        (&proxies[1], &proxies[0])
    };
    let paths: Vec<String> = job.captured().into_iter().map(|c| c.path).collect();
    assert_eq!(
        paths,
        [
            "http://xai.test/v1/videos/generations",
            "http://xai.test/v1/videos/vid_p",
            "http://files.test/v.mp4"
        ]
    );
    assert_eq!(other.requests(), 0, "the other credential's proxy sees nothing");
}

const IMAGES: &str = r#"{"created":1700000001,"data":[{"b64_json":"QUJD"},{"url":"https://img.x/2.png"}]}"#;

/// A streamed image generation on xAI, which answers one buffered body.
fn image_stream(proxy: &Proxy, key: &str) -> wreq::RequestBuilder {
    proxy
        .request(wreq::Method::POST, key, "/v1/images/generations")
        .header("content-type", "application/json")
        .body(r#"{"model":"grok-imagine-image","prompt":"a cat","stream":true}"#)
}

/// Images reach the client only once the upstream answer holds an image: until then nothing is
/// sent, and an answer without one fails over to the next credential with the client none the
/// wiser. Each image is one event the stream can resume after, for its own caller only.
#[tokio::test]
async fn images_commit_at_their_first_image_and_resume_from_an_event_cursor() {
    let late = Duration::from_millis(400);
    // (case, upstream replies, upstream requests, earliest response head)
    let cases = [
        (
            "the response head waits for the upstream answer",
            vec![Reply::json(IMAGES).first_byte_after(late)],
            1,
            late,
        ),
        (
            "an answer without an image fails over",
            vec![
                Reply::json(r#"{"data":[{"b64_json":"","url":"","revised_prompt":"none"}]}"#),
                Reply::json(IMAGES),
            ],
            2,
            Duration::ZERO,
        ),
    ];
    for (case, replies, sends, earliest) in cases {
        let upstream = StreamUpstream::start(replies).await;
        let proxy = proxy(&upstream, 4096).await;
        let started = Instant::now();
        let response = image_stream(&proxy, "client-a").send().await.unwrap();
        assert!(
            started.elapsed() >= earliest,
            "{case}: head after {:?}",
            started.elapsed()
        );
        assert_eq!(response.status(), 200, "{case}");
        assert_eq!(response.headers()["content-type"], "text/event-stream", "{case}");
        let id = response.headers()["x-ck-stream-id"].to_str().unwrap().to_owned();
        let body = response.text().await.unwrap();
        assert_eq!(
            body.matches("event: image_generation.completed").count(),
            2,
            "{case}: {body}"
        );
        assert!(body.contains(r#""b64_json":"QUJD""#), "{case}: {body}");
        assert_eq!(upstream.requests(), sends, "{case}");

        // (resume case, client key, query, status, the only image in the suffix)
        let resumes = [
            (
                "after the first image",
                "client-a",
                "after=v1.e.1",
                200,
                Some("https://img.x/2.png"),
            ),
            (
                "a byte cursor on an event stream",
                "client-a",
                "after=v1.b.3",
                400,
                None,
            ),
            ("another caller", "client-b", "after=v1.e.1", 410, None),
        ];
        for (resume, key, query, status, image) in resumes {
            let resumed = proxy
                .request(wreq::Method::GET, key, &format!("/v1/streams?{query}"))
                .header("x-ck-stream-id", &id)
                .send()
                .await
                .unwrap();
            assert_eq!(resumed.status(), status, "{case}, {resume}");
            if let Some(image) = image {
                let suffix = resumed.text().await.unwrap();
                assert_eq!(
                    suffix.matches("event: image_generation.completed").count(),
                    1,
                    "{case}, {resume}: {suffix}"
                );
                assert!(suffix.contains(image), "{case}, {resume}: {suffix}");
            }
        }
        assert_eq!(upstream.requests(), sends, "{case}: a resume sends nothing upstream");
    }
}

/// A deferred xAI image job (X16): the create answers with xAI's job id, only its creator's polls
/// are admitted and they go to the credential that created it, a running job is 202, a finished
/// job is the Images response whose usage is recorded once however often it is polled, a failed
/// job is xAI's `error` body, and a create while the job table is full sends nothing (E-Q5).
#[tokio::test]
async fn deferred_image_jobs_belong_to_their_caller_and_charge_once() {
    let done = r#"{"data":[{"url":"https://img.x/1.png"}],"usage":{"cost_in_usd_ticks":7,"input_tokens":1,"output_tokens":2,"total_tokens":3}}"#;
    let failed = r#"{"error":{"code":"internal_error","message":"failed"}}"#;
    let upstream = StreamUpstream::start([
        Reply::json(r#"{"request_id":"img_1"}"#),
        Reply::status(202, "application/json", r#"{"status":"pending"}"#),
        Reply::json(done),
        Reply::json(done),
        Reply::json(failed),
    ])
    .await;
    let proxy = proxy(&upstream, 1).await;
    let usage = Config::parse("observability: {usage: {usage-statistics-enabled: true}}\n").unwrap();
    proxy.rt.usage_queue().configure(true, &usage);
    let create = r#"{"model":"grok-imagine-image","prompt":"a cat","deferred":true}"#;
    let images = serde_json::json!({"created":0,"data":[{"url":"https://img.x/1.png"}]});
    let mut charged = images.clone();
    charged["usage"] = serde_json::json!({"cost_in_usd_ticks":7,"input_tokens":1,"output_tokens":2,"total_tokens":3});
    // (case, client key, method, status, client body, upstream requests after the step)
    let steps = [
        (
            "a create",
            "client-a",
            "POST",
            200,
            Some(serde_json::json!({"request_id":"img_1"})),
            1,
        ),
        ("another caller's poll", "client-b", "GET", 404, None, 1),
        (
            "the creator's poll while pending",
            "client-a",
            "GET",
            202,
            Some(serde_json::json!({"status":"pending"})),
            2,
        ),
        ("the creator's poll when done", "client-a", "GET", 200, Some(charged), 3),
        ("a second done poll", "client-a", "GET", 200, Some(images), 4),
        (
            "a failed job",
            "client-a",
            "GET",
            200,
            Some(serde_json::from_str(failed).unwrap()),
            5,
        ),
        ("a create while the table is full", "client-b", "POST", 503, None, 5),
    ];
    for (case, key, method, status, body, sends) in steps {
        let request = if method == "POST" {
            proxy
                .request(wreq::Method::POST, key, "/v1/images/generations")
                .header("content-type", "application/json")
                .body(create)
        } else {
            proxy.request(wreq::Method::GET, key, "/v1/images/img_1")
        };
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), status, "{case}");
        let pending = (status == 202).then(|| response.headers()["x-ck-stream-id"].to_str().unwrap().to_owned());
        let text = response.text().await.unwrap();
        if let Some(body) = body {
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&text).unwrap(),
                body,
                "{case}: {text}"
            );
        }
        // A pending poll resumed from its start is still pending.
        if let Some(id) = pending {
            let resumed = proxy
                .request(wreq::Method::GET, key, "/v1/streams?after=v1.b.0")
                .header("x-ck-stream-id", &id)
                .send()
                .await
                .unwrap();
            assert_eq!(resumed.status(), 202, "{case}, resumed");
            assert_eq!(resumed.text().await.unwrap(), text, "{case}, resumed");
        }
        assert_eq!(upstream.requests(), sends, "{case}");
    }
    let captured = upstream.captured();
    for poll in &captured[1..] {
        assert_eq!(poll.path, "/v1/images/img_1");
        assert_eq!(
            poll.header("authorization"),
            captured[0].header("authorization"),
            "a poll goes to the job's credential"
        );
    }
    let jobs = job_usage(&proxy, "img_1").await;
    assert_eq!(jobs.len(), 1, "the job's usage is recorded once: {jobs:?}");
    assert_eq!(jobs[0]["tokens"]["total_tokens"], 3);

    // With image generation turned off the poll route is not found, as the image routes are.
    let off = proxy_with(&upstream, 1, "multimedia:\n  disable-image-generation: true\n").await;
    let response = off
        .request(wreq::Method::GET, "client-a", "/v1/images/img_1")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 404, "image generation off");
    assert_eq!(
        response.text().await.unwrap(),
        "",
        "the route is not found, not the job"
    );
    assert_eq!(upstream.requests(), 5, "image generation off sends nothing");
}

/// A deferred xAI Chat job (X11): the create returns xAI's request id and no completion;
/// only the creator's polls are admitted, they go to the credential that created it, a
/// running job is an empty 202 (no keep-alive newline), a finished job is the Chat
/// completion charged once however often it is polled, and a failed job is an error.
#[tokio::test]
async fn deferred_chat_jobs_belong_to_their_caller_and_charge_once() {
    let completion = r#"{"id":"done","object":"chat.completion","created":1,"model":"grok-4.3","choices":[{"index":0,"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}"#;
    let done: serde_json::Value = serde_json::from_str(completion).unwrap();
    let upstream = StreamUpstream::start([
        Reply::reset(),
        Reply::json(r#"{"request_id":"job_1"}"#),
        Reply::status(202, "application/json", "").first_byte_after(Duration::from_millis(1100)),
        Reply::json(completion),
        Reply::json(completion),
        Reply::status(500, "application/json", r#"{"error":{"message":"upstream failed"}}"#),
        Reply::json(r#"{"error":{"message":"job failed"}}"#),
    ])
    .await;
    let proxy = proxy_with(&upstream, 1, "requests:\n  nonstream-keepalive-interval: 1\n").await;
    let usage = Config::parse("observability: {usage: {usage-statistics-enabled: true}}\n").unwrap();
    proxy.rt.usage_queue().configure(true, &usage);
    let create = r#"{"model":"grok-4.3","deferred":true,"messages":[{"role":"user","content":"hello"}]}"#;
    // (case, client key, method, path, body, status, expected body, upstream requests after)
    let steps = [
        (
            "a create whose connection resets",
            "client-a",
            "POST",
            "/v1/chat/completions",
            Some(create),
            502,
            None,
            1,
        ),
        (
            "a create",
            "client-a",
            "POST",
            "/v1/chat/completions",
            Some(create),
            200,
            Some(serde_json::json!({"request_id":"job_1"})),
            2,
        ),
        (
            "another caller's poll",
            "client-b",
            "GET",
            "/v1/chat/deferred-completion/job_1",
            None,
            404,
            None,
            2,
        ),
        (
            "an unknown job",
            "client-a",
            "GET",
            "/v1/chat/deferred-completion/unknown",
            None,
            404,
            None,
            2,
        ),
        (
            "a poll while pending",
            "client-a",
            "GET",
            "/v1/chat/deferred-completion/job_1",
            None,
            202,
            None,
            3,
        ),
        (
            "the creator's poll when done",
            "client-a",
            "GET",
            "/v1/chat/deferred-completion/job_1",
            None,
            200,
            Some(done.clone()),
            4,
        ),
        (
            "a second done poll",
            "client-a",
            "GET",
            "/v1/chat/deferred-completion/job_1",
            None,
            200,
            Some(done.clone()),
            5,
        ),
        (
            "a failed poll",
            "client-a",
            "GET",
            "/v1/chat/deferred-completion/job_1",
            None,
            500,
            None,
            6,
        ),
        (
            "a failed job",
            "client-a",
            "GET",
            "/v1/chat/deferred-completion/job_1",
            None,
            502,
            None,
            7,
        ),
        (
            "a streamed create",
            "client-a",
            "POST",
            "/v1/chat/completions",
            Some(
                r#"{"model":"grok-4.3","deferred":true,"stream":true,"messages":[{"role":"user","content":"hello"}]}"#,
            ),
            400,
            None,
            7,
        ),
        (
            "a create while the table is full",
            "client-a",
            "POST",
            "/v1/chat/completions",
            Some(create),
            503,
            None,
            7,
        ),
    ];
    for (case, key, method, path, body, status, expected, sends) in steps {
        let mut request = proxy.request(
            if method == "POST" {
                wreq::Method::POST
            } else {
                wreq::Method::GET
            },
            key,
            path,
        );
        if let Some(body) = body {
            request = request.header("content-type", "application/json").body(body);
        }
        let response = request.send().await.unwrap();
        let got = response.status();
        let text = response.text().await.unwrap();
        assert_eq!(got, status, "{case}: {text}");
        if let Some(expected) = expected {
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&text).unwrap(),
                expected,
                "{case}: {text}"
            );
        }
        if status == 202 {
            assert!(text.is_empty(), "{case}: an empty 202, not a keep-alive prefix");
        }
        if status >= 500 {
            assert!(!text.is_empty(), "{case}: the failure is rendered");
        }
        assert_eq!(upstream.requests(), sends, "{case}: one request or none");
    }
    let captured = upstream.captured();
    assert_eq!(captured.len(), 7, "only the planned requests were sent");
    assert_eq!(
        (captured[1].method.as_str(), captured[1].path.as_str()),
        ("POST", "/v1/chat/completions")
    );
    let sent: serde_json::Value = serde_json::from_slice(&captured[1].body).unwrap();
    assert_eq!(sent["deferred"], true, "the create is deferred");
    assert_eq!(sent["model"], "grok-4.3");
    for poll in &captured[2..] {
        assert_eq!(poll.path, "/v1/chat/deferred-completion/job_1");
        assert_eq!(
            poll.header("authorization"),
            captured[1].header("authorization"),
            "a poll goes to the job's credential"
        );
    }
    let mut records = Vec::new();
    for _ in 0..50 {
        records.extend(proxy.rt.usage_queue().pop_oldest(100));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let records: Vec<serde_json::Value> = records
        .iter()
        .map(|r| serde_json::from_slice::<serde_json::Value>(r).unwrap())
        .collect();
    let charged = records.iter().filter(|r| r["tokens"]["total_tokens"] == 3).count();
    assert_eq!(charged, 1, "the completion is charged once: {records:?}");
}
