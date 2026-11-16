//! Images, speech and video jobs under the execution contract, against a local upstream: the
//! request each provider receives in its official shape, the answer as canonical records with
//! its usage read once in its own scope, refusals before any send, and failures.

mod support;

use ck_core::exec::{ExecError, FailureScope};
use ck_exec::codex::CodexExecutor;
use ck_exec::codex_oauth::CodexOAuth;
use ck_exec::openai_compat::OpenAICompatExecutor;
use ck_exec::pipeline;
use ck_exec::types::Execute;
use ck_exec::xai::XaiExecutor;
use ck_stream::{Read, Terminal, Writer};
use ck_translate::codec::document::Document;
use ck_translate::codec::event::Direction;
use ck_translate::codec::media::Registry;
use ck_translate::codec::{self, ClientCapability, Media, Operation, RequestFence, RequestView, ResponseEgress};
use ck_translate::{audio, images, video};
use serde_json::{Value, json};
use support::upstream::{Captured, Reply, StreamUpstream};
use support::{Exchange, Streams, credential, identity};

const LIMIT: u64 = 1 << 20;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Provider {
    Codex,
    /// A Codex ChatGPT login with cloaking off, so the client's headers reach the header rules.
    CodexLogin,
    /// A Sign in with ChatGPT credential, which has no direct Images endpoint.
    CodexSiwc,
    Compat,
    Xai,
}

impl Provider {
    /// The bearer token the upstream sees.
    fn token(self) -> &'static str {
        match self {
            Self::CodexLogin => "oauth-token",
            Self::CodexSiwc => "siwc-at",
            _ => "sk-up",
        }
    }
}

/// One client request: its operation, body, and the job a non-create video action names.
/// `claimed` is the operation the request view states when it differs from the body's.
#[derive(Clone, Copy)]
struct Ask {
    op: Media,
    body: &'static str,
    job: Option<&'static str>,
    claimed: Option<Media>,
}

const fn ask(op: Media, body: &'static str) -> Ask {
    Ask {
        op,
        body,
        job: None,
        claimed: None,
    }
}

const fn job(op: Media, body: &'static str, job: &'static str) -> Ask {
    Ask {
        op,
        body,
        job: Some(job),
        claimed: None,
    }
}

const GENERATE: &str = r#"{"model":"client-img","prompt":"a cat","size":"1024x1024"}"#;
const EDIT: &str =
    r#"{"model":"client-img","prompt":"add a hat","images":[{"image_url":"data:image/png;base64,AAEC"}]}"#;
const SPEECH: &str = r#"{"model":"tts-1","input":"hello","voice":"alloy","response_format":"wav"}"#;
const VIDEO: &str = r#"{"model":"client-video","prompt":"waves","duration":5}"#;

fn exchange(provider: Provider, upstream: &StreamUpstream, model: &str) -> Exchange {
    if provider == Provider::CodexLogin {
        let metadata = json!({"type": "codex", "access_token": "oauth-token", "account_id": "acct-1"});
        let path = std::path::Path::new("/fake/codex-oauth.json");
        let mut login = ck_core::credential::Credential::from_file(
            std::path::Path::new("/fake"),
            path,
            metadata.as_object().unwrap().clone(),
        )
        .unwrap();
        for (name, value) in [("base_url", upstream.url()), ("codex_disable_cloaking", "true".into())] {
            login.attributes.insert(name.into(), value);
        }
        return Exchange::new(login, model, "/v1/images/generations");
    }
    if provider == Provider::CodexSiwc {
        let mut siwc = credential("codex", &upstream.url(), "sk-up");
        siwc.metadata.insert("use_siwc".into(), true.into());
        siwc.metadata.insert("siwc".into(), json!({"access_token": "siwc-at"}));
        return Exchange::new(siwc, model, "/v1/images/generations");
    }
    let (name, base) = match provider {
        Provider::Codex | Provider::CodexLogin | Provider::CodexSiwc => ("codex", upstream.url()),
        Provider::Compat => ("openai-compatibility", format!("{}/v1", upstream.url())),
        Provider::Xai => ("xai", upstream.url()),
    };
    Exchange::new(credential(name, &base, "sk-up"), model, "/v1/images/generations")
}

type Held = (Writer, Document, Registry);
type Outcome = Result<(Result<(), ExecError>, Vec<(String, Value)>, Option<Terminal>, Writer), codec::Error>;

/// The client's request decoded by its own codec, held until each attempt re-fences it.
fn decoded(streams: &Streams, ask: Ask) -> Held {
    let mut w = streams.request();
    let document = match ask.op {
        Media::ImageGenerate | Media::ImageEdit => {
            let mode = if ask.op == Media::ImageEdit {
                images::Mode::Edit
            } else {
                images::Mode::Generate
            };
            let mut decoder = images::Decoder::new(&mut w, mode, LIMIT).unwrap();
            decoder.advance(&mut w, ask.body.as_bytes()).unwrap();
            decoder.finish(&mut w).unwrap()
        }
        Media::Speech => {
            let mut decoder = audio::Decoder::new(&mut w, LIMIT).unwrap();
            decoder.advance(&mut w, ask.body.as_bytes()).unwrap();
            decoder.finish(&mut w).unwrap()
        }
        op => {
            let action = match op {
                Media::VideoCreate => video::Action::Create,
                Media::VideoEdit => video::Action::Edit,
                Media::VideoExtend => video::Action::Extend,
                Media::VideoRetrieve => video::Action::Retrieve,
                _ => video::Action::Content,
            };
            let phase = video::Phase::Request(action);
            let mut decoder = video::Decoder::new(&mut w, video::Dialect::Xai, phase, ask.job, LIMIT).unwrap();
            if !ask.body.is_empty() {
                decoder.advance(&mut w, ask.body.as_bytes()).unwrap();
            }
            decoder.finish(&mut w).unwrap().operation
        }
    };
    let registry = Registry::new(&mut w).unwrap();
    (w, document, registry)
}

/// One attempt of `provider` on `exchange`: the client fence, this destination's re-fence (or
/// the client's alone when `client_only`), the pipeline, and the records the stream holds.
async fn run(provider: Provider, streams: &Streams, exchange: &Exchange, held: &mut Held, ask: Ask) -> Outcome {
    run_as(provider, streams, exchange, held, ask, false).await
}

async fn run_as(
    provider: Provider,
    streams: &Streams,
    exchange: &Exchange,
    held: &mut Held,
    ask: Ask,
    client_only: bool,
) -> Outcome {
    let op = Operation::Media(ask.claimed.unwrap_or(ask.op));
    match provider {
        Provider::Codex | Provider::CodexLogin | Provider::CodexSiwc => {
            let codex = CodexExecutor::with_client(wreq::Client::new(), CodexOAuth::new(wreq::Client::new()));
            run_on(&codex, streams, exchange, held, op, client_only).await
        }
        Provider::Compat => {
            let compat = OpenAICompatExecutor::with_client(wreq::Client::new());
            run_on(&compat, streams, exchange, held, op, client_only).await
        }
        Provider::Xai => {
            let xai = XaiExecutor::with_client(wreq::Client::new());
            run_on(&xai, streams, exchange, held, op, client_only).await
        }
    }
}

async fn run_on<E: Execute>(
    executor: &E,
    streams: &Streams,
    exchange: &Exchange,
    held: &mut Held,
    operation: Operation,
    client_only: bool,
) -> Outcome {
    let attempt = exchange.attempt(&streams.store);
    let (upload, document, media) = held;
    let view = RequestView {
        document,
        media,
        operation,
    };
    let client = RequestFence::default().end(view, &ClientCapability)?;
    let validated = if client_only {
        client
    } else {
        pipeline::refence(executor, &attempt, view)?
    };
    let response = streams.response(upload);
    let id = response.id();
    let handle = exchange.request(validated);
    let started = match pipeline::start(executor, &attempt, &handle, upload, response).await {
        Ok(started) => started,
        Err(error) => {
            let published = streams.read_to_end(id).await;
            let stream = streams.response(upload);
            return Ok((Err(error), records(&published.records), published.terminal, stream));
        }
    };
    let outcome = started.producer.await.expect("the producer does not panic");
    let published = streams.read_to_end(id).await;
    Ok((outcome, records(&published.records), published.terminal, started.stream))
}

/// Each record as its event name and data.
fn records(records: &[Vec<u8>]) -> Vec<(String, Value)> {
    records
        .iter()
        .map(|r| {
            let text = String::from_utf8(r.clone()).unwrap();
            let name = text
                .lines()
                .next()
                .unwrap()
                .strip_prefix("event: x_ck.")
                .unwrap()
                .to_owned();
            let data = text.lines().find_map(|l| l.strip_prefix("data: ")).unwrap();
            (name, serde_json::from_str(data).unwrap())
        })
        .collect()
}

fn usages(records: &[(String, Value)]) -> Vec<&Value> {
    records.iter().filter(|(k, _)| k == "usage").map(|(_, v)| v).collect()
}

/// The sent body as JSON; a multipart form as an object of its parts, a file part as
/// `<filename>;<content type>;<byte count>`.
fn sent_body(captured: &Captured) -> Value {
    let content_type = captured.header("content-type").unwrap_or_default();
    let Some(boundary) = content_type.strip_prefix("multipart/form-data; boundary=") else {
        return serde_json::from_slice(&captured.body).unwrap();
    };
    let body = String::from_utf8_lossy(&captured.body).into_owned();
    let mut form = serde_json::Map::new();
    for part in body.split(&format!("--{boundary}")).filter(|p| p.starts_with("\r\n")) {
        let (head, value) = part[2..].split_once("\r\n\r\n").unwrap();
        let value = value.strip_suffix("\r\n").unwrap();
        let name = head.split("name=\"").nth(1).unwrap().split('"').next().unwrap();
        let value = match head.split("filename=\"").nth(1) {
            Some(rest) => {
                let filename = rest.split('"').next().unwrap();
                let mime = head.split("Content-Type: ").nth(1).unwrap();
                format!("{filename};{mime};{}", value.len())
            }
            None => value.to_owned(),
        };
        form.insert(name.to_owned(), value.into());
    }
    Value::Object(form)
}

fn sse(events: &[Value]) -> String {
    events
        .iter()
        .map(|e| format!("event: {}\ndata: {e}\n\n", e["type"].as_str().unwrap()))
        .collect()
}

/// A Codex image tool stream: one finished image, the model's usage and the tool's.
fn codex_tool_events() -> String {
    sse(&[
        json!({"type":"response.created","response":{"created_at":1700000000}}),
        json!({"type":"response.output_item.done","item":{"id":"ig_1","type":"image_generation_call",
            "status":"completed","result":"AAEC","revised_prompt":"a cat","output_format":"png"}}),
        json!({"type":"response.completed","response":{"created_at":1700000000,
            "usage":{"input_tokens":5,"output_tokens":6,"total_tokens":11},
            "tool_usage":{"image_gen":{"input_tokens":50,"output_tokens":1056,"total_tokens":1106}},
            "tools":[{"type":"image_generation","model":"gpt-image-2"}]}}),
    ])
}

const IMAGES_JSON: &str = r#"{"created":5,"data":[{"b64_json":"AAEC","revised_prompt":"a cat"}],"usage":{"input_tokens":3,"output_tokens":4,"total_tokens":7}}"#;
const XAI_IMAGES_JSON: &str =
    r#"{"data":[{"b64_json":"AAEC","mime_type":"image/jpeg"}],"usage":{"cost_in_usd_ticks":700000000}}"#;

/// Every provider puts the request on the wire in its official shape: the endpoint, the
/// method, the upstream model the route resolved, and the members the destination defines.
#[tokio::test]
async fn media_requests_reach_the_upstream_in_the_official_shape() {
    // (name, provider, attempt model, request, reply, method, path, expected body members,
    // headers: `Some` is the value sent, `None` means the client's own value is not forwarded)
    type Headers = &'static [(&'static str, Option<&'static str>)];
    type Case = (
        &'static str,
        Provider,
        &'static str,
        Ask,
        Reply,
        &'static str,
        &'static str,
        Value,
        Headers,
    );
    let cases: Vec<Case> = vec![
        (
            "Codex gpt-image model to the backend's Images endpoint",
            Provider::Codex,
            "gpt-image-2",
            ask(Media::ImageGenerate, GENERATE),
            Reply::json(IMAGES_JSON),
            "POST",
            "/images/generations",
            json!({"model":"gpt-image-2","prompt":"a cat","size":"1024x1024"}),
            &[("content-type", Some("application/json")), ("user-agent", None)],
        ),
        (
            "Codex text model forces the image tool on Responses",
            Provider::Codex,
            "gpt-5",
            ask(Media::ImageGenerate, GENERATE),
            Reply::sse(codex_tool_events()),
            "POST",
            "/responses",
            json!({"model":"gpt-5.4-mini","stream":true,"store":false,"tool_choice":{"type":"image_generation"},
                "tools":[{"type":"image_generation","action":"generate","model":"client-img","size":"1024x1024"}]}),
            &[],
        ),
        (
            "OpenAI-compatible generation with the upstream model",
            Provider::Compat,
            "up-image",
            ask(Media::ImageGenerate, GENERATE),
            Reply::json(IMAGES_JSON),
            "POST",
            "/v1/images/generations",
            json!({"model":"up-image","prompt":"a cat","size":"1024x1024"}),
            &[],
        ),
        (
            "OpenAI-compatible edit as the Images multipart form",
            Provider::Compat,
            "up-image",
            ask(Media::ImageEdit, EDIT),
            Reply::json(IMAGES_JSON),
            "POST",
            "/v1/images/edits",
            json!({"model":"up-image","prompt":"add a hat","image":"image1;image/png;3"}),
            &[],
        ),
        (
            "Codex gpt-image model aliased to a custom upstream name stays direct",
            Provider::Codex,
            "relay-image-alias",
            ask(Media::ImageGenerate, r#"{"model":"gpt-image-2","prompt":"a cat"}"#),
            Reply::json(IMAGES_JSON),
            "POST",
            "/images/generations",
            json!({"model":"relay-image-alias","prompt":"a cat"}),
            &[],
        ),
        (
            "Codex image tool without a request model runs on the execution model",
            Provider::CodexSiwc,
            "gpt-image-1.5",
            ask(Media::ImageGenerate, r#"{"prompt":"a cat"}"#),
            Reply::sse(codex_tool_events()),
            "POST",
            "/responses",
            json!({"model":"gpt-5.4-mini","tools":[{"type":"image_generation","action":"generate","model":"gpt-image-1.5"}]}),
            &[],
        ),
        (
            "Codex image tool with a blank request model runs on the execution model",
            Provider::CodexSiwc,
            "gpt-image-1.5",
            ask(Media::ImageGenerate, r#"{"model":" ","prompt":"a cat"}"#),
            Reply::sse(codex_tool_events()),
            "POST",
            "/responses",
            json!({"model":"gpt-5.4-mini","tools":[{"type":"image_generation","action":"generate","model":"gpt-image-1.5"}]}),
            &[],
        ),
        (
            "Codex login sends its own User-Agent to the Images endpoint",
            Provider::CodexLogin,
            "gpt-image-2",
            ask(Media::ImageGenerate, GENERATE),
            Reply::json(IMAGES_JSON),
            "POST",
            "/images/generations",
            json!({"model":"gpt-image-2"}),
            &[("user-agent", None), ("chatgpt-account-id", Some("acct-1"))],
        ),
        (
            "xAI image request without a model names the resolved one",
            Provider::Xai,
            "grok-imagine-image",
            ask(Media::ImageGenerate, r#"{"prompt":"a cat"}"#),
            Reply::json(XAI_IMAGES_JSON),
            "POST",
            "/images/generations",
            json!({"model":"grok-imagine-image","prompt":"a cat"}),
            &[],
        ),
        (
            "xAI image generation",
            Provider::Xai,
            "grok-imagine-image",
            ask(
                Media::ImageGenerate,
                r#"{"model":"client-img","prompt":"a cat","aspect_ratio":"16:9"}"#,
            ),
            Reply::json(XAI_IMAGES_JSON),
            "POST",
            "/images/generations",
            json!({"model":"grok-imagine-image","prompt":"a cat","aspect_ratio":"16:9"}),
            &[],
        ),
        (
            "xAI speech with its voice and format",
            Provider::Xai,
            "grok-tts",
            ask(Media::Speech, SPEECH),
            Reply::ok("audio/wav", &b"RIFF\0\0"[..]),
            "POST",
            "/tts",
            json!({"text":"hello","voice_id":"ara","output_format":{"codec":"wav","sample_rate":24000}}),
            &[("accept", Some("*/*"))],
        ),
        (
            "xAI video creation",
            Provider::Xai,
            "grok-imagine-video",
            ask(Media::VideoCreate, VIDEO),
            Reply::json(r#"{"request_id":"vid_1"}"#),
            "POST",
            "/videos/generations",
            json!({"model":"grok-imagine-video","prompt":"waves","duration":5}),
            &[("x-idempotency-key", Some("idem-1"))],
        ),
        (
            "xAI video poll",
            Provider::Xai,
            "grok-imagine-video",
            job(Media::VideoRetrieve, r#"{"trace":"t1"}"#, "vid 1"),
            Reply::json(r#"{"status":"pending"}"#),
            "GET",
            "/videos/vid%201",
            Value::Null,
            &[],
        ),
    ];
    for (name, provider, model, ask, reply, method, path, members, headers) in cases {
        let upstream = StreamUpstream::start([reply]).await;
        let streams = Streams::new();
        let mut exchange = exchange(provider, &upstream, model);
        exchange
            .headers
            .insert("idempotency-key", http::HeaderValue::from_static("idem-1"));
        exchange
            .headers
            .insert("user-agent", http::HeaderValue::from_static("client-ua/1"));
        exchange.derived_session = Some("ctx:v1:0123456789abcdef".into());
        let mut held = decoded(&streams, ask);
        let (outcome, _, terminal, _) = run(provider, &streams, &exchange, &mut held, ask).await.unwrap();
        outcome.unwrap_or_else(|e| panic!("{name}: {e:?}"));
        assert_eq!(terminal, Some(Terminal::Done), "{name}");
        let sent = upstream.captured();
        assert_eq!(sent.len(), 1, "{name}");
        assert_eq!(
            (sent[0].method.as_str(), sent[0].path.as_str()),
            (method, path),
            "{name}"
        );
        let bearer = format!("Bearer {}", provider.token());
        assert_eq!(sent[0].header("authorization"), Some(bearer.as_str()), "{name}");
        for (header, value) in headers {
            match value {
                Some(value) => assert_eq!(sent[0].header(header), Some(*value), "{name}: {header}"),
                None => assert_ne!(
                    sent[0].header(header),
                    exchange.headers.get(*header).and_then(|v| v.to_str().ok()),
                    "{name}: {header}"
                ),
            }
        }
        if members.is_null() {
            assert!(sent[0].body.is_empty(), "{name}: a poll sends no body");
            continue;
        }
        let body = sent_body(&sent[0]);
        for (key, value) in members.as_object().unwrap() {
            assert_eq!(&body[key], value, "{name}: member {key}");
        }
        if matches!(provider, Provider::Codex | Provider::CodexLogin | Provider::CodexSiwc) {
            assert!(body["prompt_cache_key"].is_string(), "{name}: the session's cache key");
        }
    }
}

/// The answer becomes canonical records through the media codec: usage is published once, in
/// its own scope, before the result, and never invented where the body reports none.
#[tokio::test]
async fn media_answers_become_canonical_records_with_scoped_usage() {
    // (name, provider, model, request, reply, the result's record kind, usage (scope, final, counts))
    type Usage = (Option<&'static str>, bool, Value);
    type Case = (
        &'static str,
        Provider,
        &'static str,
        Ask,
        Reply,
        &'static str,
        Vec<Usage>,
    );
    let polled = |status: &str, usage: Option<Value>| {
        let mut body = json!({"request_id":"vid_1","status":status});
        if let Some(usage) = usage {
            body["usage"] = usage;
        }
        Reply::json(body.to_string())
    };
    let poll = job(Media::VideoRetrieve, "", "vid_1");
    let cost = json!({"cost_in_usd_ticks":500,"input_tokens":3});
    let cases: Vec<Case> = vec![
        (
            "OpenAI-compatible buffered images",
            Provider::Compat,
            "up-image",
            ask(Media::ImageGenerate, GENERATE),
            Reply::json(IMAGES_JSON),
            "operation",
            vec![(None, true, json!({"input_tokens":3,"output_tokens":4,"total_tokens":7}))],
        ),
        (
            "Codex image tool: the model's usage and the tool's",
            Provider::Codex,
            "gpt-5",
            ask(Media::ImageGenerate, GENERATE),
            Reply::sse(codex_tool_events()),
            "operation",
            vec![
                (
                    None,
                    true,
                    json!({"input_tokens":5,"output_tokens":6,"total_tokens":11}),
                ),
                (
                    Some("tool:gpt-image-2"),
                    true,
                    json!({"input_tokens":50,"output_tokens":1056,"total_tokens":1106}),
                ),
            ],
        ),
        (
            "xAI images keep the cost in native usage",
            Provider::Xai,
            "grok-imagine-image",
            ask(Media::ImageGenerate, r#"{"prompt":"a cat"}"#),
            Reply::json(XAI_IMAGES_JSON),
            "operation",
            vec![(None, true, json!({}))],
        ),
        (
            "speech reports no usage, so none is published",
            Provider::Xai,
            "grok-tts",
            ask(Media::Speech, SPEECH),
            Reply::ok("audio/wav", &b"RIFF\0\0"[..]),
            "media.end",
            vec![],
        ),
        (
            "an accepted video job has no usage",
            Provider::Xai,
            "grok-imagine-video",
            ask(Media::VideoCreate, VIDEO),
            Reply::json(r#"{"request_id":"vid_1"}"#),
            "operation",
            vec![],
        ),
        (
            "a pending poll without usage carries none",
            Provider::Xai,
            "grok-imagine-video",
            poll,
            polled("pending", None),
            "operation",
            vec![],
        ),
        (
            "a pending poll's reported usage is not final",
            Provider::Xai,
            "grok-imagine-video",
            poll,
            polled("pending", Some(cost.clone())),
            "operation",
            vec![(Some("job:vid_1"), false, json!({"input_tokens":3}))],
        ),
        (
            "a finished job's usage is final in the job scope",
            Provider::Xai,
            "grok-imagine-video",
            poll,
            polled("done", Some(cost.clone())),
            "operation",
            vec![(Some("job:vid_1"), true, json!({"input_tokens":3}))],
        ),
    ];
    for (name, provider, model, ask, reply, result, expected) in cases {
        let upstream = StreamUpstream::start([reply]).await;
        let streams = Streams::new();
        let exchange = exchange(provider, &upstream, model);
        let mut held = decoded(&streams, ask);
        let (outcome, records, terminal, _) = run(provider, &streams, &exchange, &mut held, ask).await.unwrap();
        outcome.unwrap_or_else(|e| panic!("{name}: {e:?}"));
        assert_eq!(terminal, Some(Terminal::Done), "{name}");
        let kinds: Vec<_> = records.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(kinds.first(), Some(&"response.start"), "{name}: {kinds:?}");
        assert_eq!(kinds.last(), Some(&"done"), "{name}: {kinds:?}");
        let at = kinds
            .iter()
            .position(|k| *k == result)
            .unwrap_or_else(|| panic!("{name}: {kinds:?}"));
        let usage = usages(&records);
        assert_eq!(usage.len(), expected.len(), "{name}: {kinds:?}");
        for (record, (scope, last, counts)) in usage.iter().zip(&expected) {
            assert_eq!(record["scope"].as_str(), *scope, "{name}");
            assert_eq!(record["usage"]["final"], *last, "{name}");
            for (key, value) in counts.as_object().unwrap() {
                assert_eq!(&record["usage"][key], value, "{name}: {key}");
            }
            assert!(record["usage"]["native"]["value"].is_object(), "{name}");
        }
        let first_usage = kinds.iter().position(|k| *k == "usage");
        assert!(first_usage.is_none_or(|u| u < at), "{name}: usage precedes the result");
        if provider == Provider::Xai && ask.op == Media::ImageGenerate {
            assert_eq!(
                usage[0]["usage"]["native"]["value"]["cost_in_usd_ticks"], 700000000,
                "{name}"
            );
        }
        if ask.op == Media::VideoRetrieve && !expected.is_empty() {
            assert_eq!(usage[0]["usage"]["native"]["value"]["cost_in_usd_ticks"], 500, "{name}");
        }
    }
}

/// The speech answer reaches the client byte for byte with the upstream's MIME type.
#[tokio::test]
async fn speech_audio_passes_through_byte_exact() {
    let audio_bytes: Vec<u8> = (0..=255u8).cycle().take(120_000).collect();
    let upstream = StreamUpstream::start([Reply::ok("audio/wav; rate=24000", audio_bytes.clone())]).await;
    let streams = Streams::new();
    let exchange = exchange(Provider::Xai, &upstream, "grok-tts");
    let ask = ask(Media::Speech, SPEECH);
    let mut held = decoded(&streams, ask);
    let (outcome, records, _, mut stream) = run(Provider::Xai, &streams, &exchange, &mut held, ask).await.unwrap();
    outcome.unwrap();
    assert!(usages(&records).is_empty());
    let id = stream.id();
    let mut egress = audio::Egress::new(&stream, Some(identity(Direction::Response)));
    let mut out = Vec::new();
    let mut reader = streams.reader(id);
    while let Some(read) = reader.next().await {
        if let Read::Record(lease) = read {
            egress
                .advance(&mut stream, lease, |bytes| {
                    let mut at = 0;
                    while at < bytes.len() {
                        let chunk = bytes.chunk(at).unwrap();
                        out.extend_from_slice(chunk);
                        at += chunk.len();
                    }
                    Ok(())
                })
                .unwrap();
        }
    }
    egress.finish().unwrap();
    assert_eq!(out, audio_bytes);
    let mime = egress.content_type().unwrap().map(|(doc, node)| {
        let mut chars = doc.chars(node).unwrap();
        let mut s = String::new();
        while let Some(c) = chars.next_scalar().unwrap() {
            s.push(c);
        }
        s
    });
    assert_eq!(mime.as_deref(), Some("audio/wav; rate=24000"));
}

/// A request the destination cannot express, or a fence that is not this destination's, is
/// refused before any byte is sent.
#[tokio::test]
async fn refused_media_requests_send_nothing() {
    // (name, provider, model, request, client fence only)
    type Case = (&'static str, Provider, &'static str, Ask, bool);
    let cases: Vec<Case> = vec![
        (
            "xAI has no image quality control",
            Provider::Xai,
            "grok-imagine-image",
            ask(Media::ImageGenerate, r#"{"prompt":"a cat","quality":"high"}"#),
            false,
        ),
        (
            "xAI speech has no instructions",
            Provider::Xai,
            "grok-tts",
            ask(
                Media::Speech,
                r#"{"model":"tts-1","input":"hi","voice":"alloy","instructions":"calm"}"#,
            ),
            false,
        ),
        (
            "the image tool makes one image per call",
            Provider::Codex,
            "gpt-5",
            ask(Media::ImageGenerate, r#"{"prompt":"a cat","n":2}"#),
            false,
        ),
        (
            "OpenAI-compatible relays have no aspect ratio",
            Provider::Compat,
            "up-image",
            ask(Media::ImageGenerate, r#"{"prompt":"a cat","aspect_ratio":"16:9"}"#),
            false,
        ),
        (
            "OpenAI-compatible relays serve no speech",
            Provider::Compat,
            "up-image",
            ask(Media::Speech, SPEECH),
            false,
        ),
        (
            "Codex serves no video",
            Provider::Codex,
            "gpt-5",
            ask(Media::VideoCreate, VIDEO),
            false,
        ),
        (
            "video content is not an xAI request",
            Provider::Xai,
            "grok-imagine-video",
            job(Media::VideoContent, "", "vid_1"),
            false,
        ),
        (
            "xAI video edits have no duration",
            Provider::Xai,
            "grok-imagine-video",
            job(
                Media::VideoEdit,
                r#"{"prompt":"slower","duration":5,"video":{"url":"https://v.x/a.mp4"}}"#,
                "vid_1",
            ),
            false,
        ),
        (
            "an edit view over a generation body",
            Provider::Compat,
            "up-image",
            Ask {
                claimed: Some(Media::ImageEdit),
                ..ask(Media::ImageGenerate, GENERATE)
            },
            false,
        ),
        (
            "a multipart edit carries image data, not a URL",
            Provider::Compat,
            "up-image",
            ask(
                Media::ImageEdit,
                r#"{"prompt":"add a hat","images":[{"image_url":"https://img.x/a.png"}]}"#,
            ),
            false,
        ),
        (
            "a client-only fence authorizes no upload",
            Provider::Xai,
            "grok-imagine-image",
            ask(Media::ImageGenerate, r#"{"prompt":"a cat"}"#),
            true,
        ),
        (
            "a client-only fence authorizes no Codex upload",
            Provider::Codex,
            "gpt-image-2",
            ask(Media::ImageGenerate, r#"{"prompt":"a cat"}"#),
            true,
        ),
    ];
    for (name, provider, model, ask, client_only) in cases {
        let upstream = StreamUpstream::start([Reply::json(IMAGES_JSON)]).await;
        let streams = Streams::new();
        let exchange = exchange(provider, &upstream, model);
        let mut held = decoded(&streams, ask);
        match run_as(provider, &streams, &exchange, &mut held, ask, client_only).await {
            Err(error) => assert!(!client_only, "{name}: refused at the fence: {error:?}"),
            Ok((outcome, records, _, _)) => {
                assert!(client_only, "{name}: the fence admitted it");
                assert!(outcome.is_err(), "{name}");
                assert!(records.iter().all(|(k, _)| k != "operation"), "{name}");
            }
        }
        assert_eq!(upstream.requests(), 0, "{name}");
    }
}

/// An upstream refusal is the attempt's failure, scoped for failover, with nothing published;
/// a cut or malformed answer ends in one error and no result or usage.
#[tokio::test]
async fn media_failures_publish_no_result() {
    // (name, provider, model, request, reply, the refusal's scope and whether it keeps the
    // upstream headers; an explicit request fault carries none, so it neither rotates nor cools)
    type Case = (
        &'static str,
        Provider,
        &'static str,
        Ask,
        Reply,
        Option<(FailureScope, bool)>,
    );
    let cases: Vec<Case> = vec![
        (
            "a rate-limited image request fails over",
            Provider::Compat,
            "up-image",
            ask(Media::ImageGenerate, GENERATE),
            Reply::status(429, "application/json", r#"{"error":{"message":"slow down"}}"#),
            Some((FailureScope::Model, true)),
        ),
        (
            "an unknown voice is the request's fault",
            Provider::Xai,
            "grok-tts",
            ask(Media::Speech, SPEECH),
            Reply::status(404, "application/json", r#"{"error":"voice not found"}"#),
            Some((FailureScope::Request, false)),
        ),
        (
            "a missing speech model is left to the upstream classification",
            Provider::Xai,
            "grok-tts",
            ask(Media::Speech, SPEECH),
            Reply::status(404, "application/json", r#"{"error":"model_not_found"}"#),
            Some((FailureScope::Request, true)),
        ),
        (
            "an unknown video job is refused with an empty stream",
            Provider::Xai,
            "grok-imagine-video",
            job(Media::VideoRetrieve, "", "vid_404"),
            Reply::status(404, "application/json", r#"{"error":"not found"}"#),
            Some((FailureScope::Request, true)),
        ),
        (
            "a cut image body publishes no image",
            Provider::Xai,
            "grok-imagine-image",
            ask(Media::ImageGenerate, r#"{"prompt":"a cat"}"#),
            Reply::json(XAI_IMAGES_JSON).short_by(10),
            None,
        ),
        (
            "a malformed job publishes no job and no usage",
            Provider::Xai,
            "grok-imagine-video",
            job(Media::VideoRetrieve, "", "vid_1"),
            Reply::json(r#"{"status":"done","usage":{"cost_in_usd_ticks":5}"#),
            None,
        ),
    ];
    for (name, provider, model, ask, reply, refused) in cases {
        let upstream = StreamUpstream::start([reply]).await;
        let streams = Streams::new();
        let exchange = exchange(provider, &upstream, model);
        let mut held = decoded(&streams, ask);
        let (outcome, records, terminal, _) = run(provider, &streams, &exchange, &mut held, ask).await.unwrap();
        let error = outcome.expect_err(name);
        match refused {
            Some((scope, headers)) => {
                assert_eq!(error.scope, scope, "{name}: {error:?}");
                assert_eq!(!error.headers.is_empty(), headers, "{name}: {error:?}");
                assert!(records.is_empty(), "{name}: a refused attempt leaves its stream empty");
            }
            None => {
                let kinds: Vec<_> = records.iter().map(|(k, _)| k.as_str()).collect();
                assert!(
                    !kinds.iter().any(|k| matches!(*k, "operation" | "usage")),
                    "{name}: {kinds:?}"
                );
                assert_eq!(kinds.iter().filter(|k| **k == "error").count(), 1, "{name}: {kinds:?}");
                assert_ne!(terminal, Some(Terminal::Done), "{name}");
            }
        }
        assert_eq!(upstream.requests(), 1, "{name}");
    }
}
