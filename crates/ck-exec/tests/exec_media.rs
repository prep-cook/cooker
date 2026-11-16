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
/// `claimed` is the operation the request view states when it differs from the body's. A poll's
/// job was `created` through the same executor first, unless the poll is of a foreign job.
#[derive(Clone, Copy)]
struct Ask {
    op: Media,
    body: &'static str,
    job: Option<&'static str>,
    claimed: Option<Media>,
    created: bool,
}

const fn ask(op: Media, body: &'static str) -> Ask {
    Ask {
        op,
        body,
        job: None,
        claimed: None,
        created: false,
    }
}

const fn job(op: Media, body: &'static str, job: &'static str) -> Ask {
    Ask {
        op,
        body,
        job: Some(job),
        claimed: None,
        created: true,
    }
}

/// A poll of a job this executor never created.
const fn foreign(op: Media, job: &'static str) -> Ask {
    Ask {
        created: false,
        ..self::job(op, "", job)
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
        Media::ImageRetrieve => images::retrieve(&mut w, ask.job.unwrap()).unwrap(),
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
            if let (Media::VideoRetrieve, Some(job), true) = (ask.op, ask.job, ask.created) {
                create_job(&xai, streams, job).await;
            }
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
    let mut handle = exchange.request(validated);
    let started = match pipeline::start(executor, &attempt, &mut handle, upload, response).await {
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

/// Creates `job` through `xai` as a client does: a create the upstream answers with the job.
async fn create_job(xai: &XaiExecutor, streams: &Streams, job: &str) {
    let upstream = StreamUpstream::start([Reply::json(json!({"request_id": job}).to_string())]).await;
    let exchange = exchange(Provider::Xai, &upstream, "grok-imagine-video");
    let create = ask(Media::VideoCreate, VIDEO);
    let mut held = decoded(streams, create);
    let operation = Operation::Media(Media::VideoCreate);
    let (outcome, ..) = run_on(xai, streams, &exchange, &mut held, operation, false)
        .await
        .unwrap();
    outcome.unwrap();
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
            "OpenAI-compatible edit naming its image by URL as the Images JSON body",
            Provider::Compat,
            "up-image",
            ask(
                Media::ImageEdit,
                r#"{"prompt":"add a hat","images":[{"image_url":"https://img.x/a.png"}]}"#,
            ),
            Reply::json(IMAGES_JSON),
            "POST",
            "/v1/images/edits",
            json!({"model":"up-image","prompt":"add a hat","images":[{"image_url":"https://img.x/a.png"}]}),
            &[("content-type", Some("application/json"))],
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
            &[("x-idempotency-key", None)],
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
        for name in ["idempotency-key", "x-idempotency-key"] {
            exchange.headers.insert(name, http::HeaderValue::from_static("idem-1"));
        }
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

/// The speech answer reaches the client byte for byte, with the upstream's media type when it
/// names audio and the requested format's own when it names none.
#[tokio::test]
async fn speech_audio_passes_through_byte_exact() {
    let audio_bytes: Vec<u8> = (0..=255u8).cycle().take(120_000).collect();
    // (upstream Content-Type, request, client media type)
    let cases = [
        ("audio/wav; rate=24000", SPEECH, "audio/wav; rate=24000"),
        (" Application/JSON ; charset=utf-8", SPEECH, "audio/wav"),
        (
            "text/plain",
            r#"{"model":"tts-1","input":"hello","voice":"alloy","response_format":"pcm"}"#,
            "audio/pcm",
        ),
        (
            "application/octet-stream",
            r#"{"model":"tts-1","input":"hello","voice":"alloy"}"#,
            "audio/mpeg",
        ),
    ];
    for (upstream_type, request, expected) in cases {
        let upstream = StreamUpstream::start([Reply::ok(upstream_type, audio_bytes.clone())]).await;
        let streams = Streams::new();
        let exchange = exchange(Provider::Xai, &upstream, "grok-tts");
        let ask = ask(Media::Speech, request);
        let mut held = decoded(&streams, ask);
        let (outcome, records, _, mut stream) = run(Provider::Xai, &streams, &exchange, &mut held, ask).await.unwrap();
        outcome.unwrap();
        assert!(usages(&records).is_empty(), "{upstream_type}");
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
        assert_eq!(out, audio_bytes, "{upstream_type}");
        let mime = egress.content_type().unwrap().map(|(doc, node)| {
            let mut chars = doc.chars(node).unwrap();
            let mut s = String::new();
            while let Some(c) = chars.next_scalar().unwrap() {
                s.push(c);
            }
            s
        });
        assert_eq!(mime.as_deref(), Some(expected), "{upstream_type}");
    }
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
            "a job this process never created is refused before anything is sent",
            Provider::Xai,
            "grok-imagine-video",
            foreign(Media::VideoRetrieve, "vid_404"),
            Reply::status(404, "application/json", r#"{"error":"not found"}"#),
            Some((FailureScope::Request, false)),
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
        // A poll of a job never created here is refused before it is sent.
        let foreign = ask.job.is_some() && !ask.created;
        assert_eq!(upstream.requests(), usize::from(!foreign), "{name}");
    }
}

/// The bodies a buffered video route reports to its usage record.
#[derive(Default)]
struct Reported(std::sync::Mutex<Vec<Value>>);

impl ck_core::exec::UsageObserver for Reported {
    fn response_body(&self, _: ck_core::format::Format, body: &[u8]) {
        self.0.lock().unwrap().push(serde_json::from_slice(body).unwrap());
    }

    fn response_line(&self, _: ck_core::format::Format, _: &[u8]) {}

    fn request(&self, _: ck_core::format::Format, _: &[u8]) {}
}

/// The route a video request takes through the xAI executor.
#[derive(Clone, Copy, Debug)]
enum Route {
    /// `XaiExecutor::videos`: the whole body, reported to the usage record.
    Buffered,
    /// The execution contract: canonical records through the video codec.
    Contract,
    /// The execution contract onto a stream whose event limit cannot hold the usage record, so
    /// the attempt fails without publishing it.
    ContractNoRoom,
}

/// One video request through `route`: a create answered with `job` when `body` is `None`, else a
/// poll of `job` answered with `body`. Whether its usage was the job's charge.
async fn video_step(
    xai: &XaiExecutor,
    upstream: &StreamUpstream,
    route: Route,
    job: &'static str,
    body: Option<&str>,
) -> bool {
    match route {
        Route::Contract | Route::ContractNoRoom => {
            let streams = match route {
                Route::ContractNoRoom => Streams::with_event_bytes(400),
                _ => Streams::new(),
            };
            let exchange = exchange(Provider::Xai, upstream, "grok-imagine-video");
            let ask = match body {
                None => ask(Media::VideoCreate, VIDEO),
                Some(_) => foreign(Media::VideoRetrieve, job),
            };
            let mut held = decoded(&streams, ask);
            let operation = Operation::Media(ask.op);
            let (outcome, records, ..) = run_on(xai, &streams, &exchange, &mut held, operation, false)
                .await
                .unwrap();
            match route {
                Route::ContractNoRoom => assert!(outcome.is_err(), "{job}: the usage record did not fit"),
                _ => outcome.unwrap_or_else(|e| panic!("{route:?} {job}: {e:?}")),
            }
            usages(&records)
                .iter()
                .any(|record| record["scope"] == format!("job:{job}"))
        }
        Route::Buffered => {
            let (path, payload) = match body {
                None => ("/v1/videos/generations".to_owned(), VIDEO.to_owned()),
                Some(_) => (format!("/v1/videos/{job}"), json!({"request_id": job}).to_string()),
            };
            let reported = std::sync::Arc::new(Reported::default());
            let payload = bytes::Bytes::from(payload);
            let request = ck_core::exec::ExecRequest {
                operation: ck_core::exec::Operation::Generate,
                source_format: ck_core::format::Format::OpenAI,
                response_format: ck_core::format::Format::OpenAI,
                model: "grok-imagine-video".into(),
                requested_model: "client-video".into(),
                body: payload.clone(),
                original_body: payload,
                stream: false,
                alt: None,
                session: None,
                execution_session: None,
                derived_session: None,
                request_path: path.clone(),
                headers: http::HeaderMap::new(),
                caller: ck_core::exec::Caller {
                    principal: "sk-client-test".into(),
                    source: "authorization",
                    provider: "config-inline".into(),
                },
                resolved_model: None,
                usage: ck_core::exec::UsageSink::new(reported.clone()),
            };
            let credential = credential("xai", &upstream.url(), "sk-up");
            xai.videos(&credential, request, &path, &ck_core::config::Config::default())
                .await
                .unwrap_or_else(|e| panic!("{route:?} {job}: {e:?}"));
            let bodies = reported.0.lock().unwrap();
            bodies.iter().any(|body| body.get("usage").is_some())
        }
    }
}

/// A video job's usage is charged once, at the first answer that reports it, whichever route
/// carries its create and its polls (E-Q13): both settle against the executor's one job table.
#[tokio::test]
async fn video_job_usage_is_charged_once_whichever_route_polls_it() {
    use Route::{Buffered, Contract, ContractNoRoom};
    let done = r#"{"status":"done","usage":{"cost_in_usd_ticks":7}}"#;
    let pending = r#"{"status":"pending","usage":{"cost_in_usd_ticks":7}}"#;
    let large = r#"{"status":"done","usage":{"cost_in_usd_ticks":7,"input_tokens":1000000000000000000,"output_tokens":1000000000000000000,"total_tokens":2000000000000000000}}"#;
    // (label, steps as (route, job, poll answer; `None` for the job's create), charged per step)
    type Case = (
        &'static str,
        Vec<(Route, &'static str, Option<&'static str>)>,
        Vec<bool>,
    );
    let cases: Vec<Case> = vec![
        (
            "ten polls charge once",
            std::iter::once((Contract, "vid_1", None))
                .chain(std::iter::repeat_n((Contract, "vid_1", Some(done)), 10))
                .collect(),
            std::iter::once(false)
                .chain(std::iter::once(true))
                .chain(std::iter::repeat_n(false, 9))
                .collect(),
        ),
        (
            "a buffered poll's charge holds for the contract's polls",
            vec![
                (Buffered, "vid_2", None),
                (Buffered, "vid_2", Some(done)),
                (Contract, "vid_2", Some(done)),
                (Contract, "vid_2", Some(done)),
            ],
            vec![false, true, false, false],
        ),
        (
            "a contract poll's charge holds for the buffered route",
            vec![
                (Contract, "vid_3", None),
                (Contract, "vid_3", Some(done)),
                (Buffered, "vid_3", Some(done)),
            ],
            vec![false, true, false],
        ),
        (
            "a pending report is the charge and the finished one is not",
            vec![
                (Contract, "vid_4", None),
                (Contract, "vid_4", Some(pending)),
                (Contract, "vid_4", Some(done)),
            ],
            vec![false, true, false],
        ),
        (
            "a report whose publication failed leaves the charge to the next poll",
            vec![
                (Contract, "vid_5", None),
                (ContractNoRoom, "vid_5", Some(large)),
                (Contract, "vid_5", Some(large)),
                (Contract, "vid_5", Some(large)),
            ],
            vec![false, false, true, false],
        ),
    ];
    for (label, steps, expected) in cases {
        let replies = steps.iter().map(|(_, job, body)| match body {
            None => Reply::json(json!({"request_id": job}).to_string()),
            Some(body) => Reply::json(*body),
        });
        let upstream = StreamUpstream::start(replies.collect::<Vec<_>>()).await;
        let xai = XaiExecutor::with_client(wreq::Client::new());
        let mut charged = Vec::new();
        for (route, job, body) in steps {
            charged.push(video_step(&xai, &upstream, route, job, body).await);
        }
        assert_eq!(charged, expected, "{label}");
    }
}

/// A video job id belongs to the caller that created it, and the job table is bounded (E-Q5):
/// another caller's poll is not found and a create while the table is full is refused with 503,
/// both before anything is sent.
#[tokio::test]
async fn video_jobs_belong_to_their_caller_and_a_full_table_sends_nothing() {
    let callers = ["sk-client-a", "sk-client-b"];
    // (caller, job, create or poll, refusal status, upstream requests after the step)
    let steps: [(usize, &str, bool, Option<u16>, usize); 6] = [
        (0, "vid_1", true, None, 1),
        (1, "vid_1", false, Some(404), 1),
        (0, "vid_1", false, None, 2),
        (1, "", true, Some(503), 2),
        (0, "", true, Some(503), 2),
        (0, "vid_1", false, None, 3),
    ];
    let upstream = StreamUpstream::start([
        Reply::json(r#"{"request_id":"vid_1"}"#),
        Reply::json(r#"{"status":"pending"}"#),
        Reply::json(r#"{"status":"pending"}"#),
    ])
    .await;
    let xai = XaiExecutor::with_client(wreq::Client::new()).with_video_capacity(1);
    for (i, (who, job, create, refused, sent)) in steps.into_iter().enumerate() {
        let streams = Streams::new();
        let mut exchange = exchange(Provider::Xai, &upstream, "grok-imagine-video");
        exchange.caller.principal = callers[who].into();
        let ask = if create {
            ask(Media::VideoCreate, VIDEO)
        } else {
            foreign(Media::VideoRetrieve, job)
        };
        let mut held = decoded(&streams, ask);
        let operation = Operation::Media(ask.op);
        let (outcome, records, ..) = run_on(&xai, &streams, &exchange, &mut held, operation, false)
            .await
            .unwrap();
        match refused {
            None => outcome.unwrap_or_else(|e| panic!("step {i}: {e:?}")),
            Some(status) => {
                let error = outcome.expect_err("refused");
                assert_eq!((error.status, error.scope), (status, FailureScope::Request), "step {i}");
                assert!(records.is_empty(), "step {i}: nothing published");
            }
        }
        assert_eq!(upstream.requests(), sent, "step {i}");
    }
}

const LARGE_DONE: &str = r#"{"data":[{"url":"https://x/1.png"}],"usage":{"cost_in_usd_ticks":7,"input_tokens":1000000000000000000,"output_tokens":1000000000000000000,"total_tokens":2000000000000000000}}"#;
const DEFERRED: &str = r#"{"model":"grok-imagine-image","prompt":"a cat","deferred":true,"storage_options":{"bucket":"b"},"output":{"upload_urls":["https://u/1"]}}"#;

/// A deferred xAI image create (X16) sends its controls and takes a place in the job table before
/// anything is sent, which it shares with video jobs: a full table refuses the next create with
/// 503 and sends nothing (E-Q5). The job is polled on the credential it was created on, and only
/// by its caller as an image job.
#[tokio::test]
async fn deferred_image_creates_take_a_place_and_polls_go_to_the_jobs_credential() {
    let callers = ["sk-client-a", "sk-client-b"];
    // (case, caller, request, refusal status, upstream requests after the step)
    let steps: [(&str, usize, Ask, Option<u16>, usize); 6] = [
        ("a deferred create", 0, ask(Media::ImageGenerate, DEFERRED), None, 1),
        (
            "a create while the table is full",
            1,
            ask(Media::ImageGenerate, DEFERRED),
            Some(503),
            1,
        ),
        (
            "a video create while the table is full",
            0,
            ask(Media::VideoCreate, VIDEO),
            Some(503),
            1,
        ),
        (
            "another caller's poll",
            1,
            foreign(Media::ImageRetrieve, "img_1"),
            Some(404),
            1,
        ),
        (
            "a video poll of the image job",
            0,
            foreign(Media::VideoRetrieve, "img_1"),
            Some(404),
            1,
        ),
        ("the creator's poll", 0, foreign(Media::ImageRetrieve, "img_1"), None, 2),
    ];
    let upstream = StreamUpstream::start([
        Reply::json(r#"{"request_id":"img_1"}"#),
        Reply::status(202, "application/json", r#"{"status":"pending"}"#),
        Reply::json(LARGE_DONE),
        Reply::json(LARGE_DONE),
        Reply::json(LARGE_DONE),
    ])
    .await;
    let executors = ck_exec::Executors {
        claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: Default::default(),
        devices: Default::default(),
        openai: ck_exec::OpenAIExecutors {
            xai: XaiExecutor::with_client(wreq::Client::new()).with_video_capacity(1),
            ..Default::default()
        },
        google: Default::default(),
    };
    let xai = &executors.openai.xai;
    for (case, who, ask, refused, sent) in steps {
        let streams = Streams::new();
        let mut exchange = exchange(Provider::Xai, &upstream, "grok-imagine-image");
        exchange.caller.principal = callers[who].into();
        let mut held = decoded(&streams, ask);
        let operation = Operation::Media(ask.op);
        let (outcome, records, ..) = run_on(xai, &streams, &exchange, &mut held, operation, false)
            .await
            .unwrap();
        match refused {
            None => outcome.unwrap_or_else(|e| panic!("{case}: {e:?}")),
            Some(status) => {
                let error = outcome.expect_err("refused");
                assert_eq!((error.status, error.scope), (status, FailureScope::Request), "{case}");
                assert!(records.is_empty(), "{case}: nothing published");
                if case == "a create while the table is full" {
                    let body: Value = serde_json::from_slice(&error.body).unwrap();
                    assert_eq!(body["error"]["code"], "image_jobs_full", "{case}");
                }
            }
        }
        assert_eq!(upstream.requests(), sent, "{case}");
        if case == "a deferred create" {
            let job = records
                .iter()
                .find(|(name, _)| name == "operation")
                .map(|(_, v)| v.clone());
            assert_eq!(
                job.unwrap()["value"],
                json!({"job_id":"img_1","status":"queued"}),
                "{case}"
            );
            let bound = executors
                .image_binding(&exchange.caller, "img_1")
                .expect("the job's binding");
            assert_eq!(bound.credential, exchange.credential.id, "{case}");
            assert_eq!(executors.video_binding(&exchange.caller, "img_1"), None, "{case}");
        }
    }
    // A finished poll's usage is the job's charge only once it is published (E-Q13).
    // (case, the stream holds the usage record, the poll publishes the job's usage)
    let polls = [
        ("a finished poll whose usage record does not fit", false, false),
        ("the next finished poll", true, true),
        ("a later finished poll", true, false),
    ];
    for (case, room, charged) in polls {
        let streams = if room {
            Streams::new()
        } else {
            Streams::with_event_bytes(400)
        };
        let mut exchange = exchange(Provider::Xai, &upstream, "grok-imagine-image");
        exchange.caller.principal = callers[0].into();
        let ask = foreign(Media::ImageRetrieve, "img_1");
        let mut held = decoded(&streams, ask);
        let operation = Operation::Media(ask.op);
        let (outcome, records, ..) = run_on(xai, &streams, &exchange, &mut held, operation, false)
            .await
            .unwrap();
        assert_eq!(outcome.is_ok(), room, "{case}");
        let job = usages(&records).iter().any(|usage| usage["scope"] == "job:img_1");
        assert_eq!(job, charged, "{case}");
    }
    let captured = upstream.captured();
    let sent = sent_body(&captured[0]);
    assert_eq!(sent["deferred"], true);
    assert_eq!(sent["storage_options"], json!({"bucket":"b"}));
    assert_eq!(sent["output"], json!({"upload_urls":["https://u/1"]}));
    assert_eq!(
        (captured[1].method.as_str(), captured[1].path.as_str()),
        ("GET", "/images/img_1")
    );
}
