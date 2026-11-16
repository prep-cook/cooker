//! Accounting consumes every billing dimension a provider's usage reports: counts the
//! canonical snapshot maps and provider fields kept only in its native value. A dimension the
//! upstream did not report stays absent, never zero.

mod stream_fixture;

use ck_server::observability::usage_snapshot::{self, CanonicalUsage, Dimension, ResponseFacts, Usage};
use ck_stream::{Store, Terminal, Writer};
use ck_translate::codec::event::{Direction, Emitter, Identity, Kind, LocalId, Scope};
use ck_translate::codec::{BodyMode, Operation, ResponseIngress};
use stream_fixture::Fixture;

const LIMIT: u64 = 1 << 20;

fn identity() -> Identity {
    Identity {
        request_id: LocalId::new("request").unwrap(),
        stream_id: LocalId::new("stream").unwrap(),
        attempt: 1,
        direction: Direction::Response,
    }
}

/// Runs one upstream body through a response ingress and reads the stream as the runtime's
/// usage reader does.
async fn account(run: impl FnOnce(&mut Writer, &Store)) -> CanonicalUsage {
    account_scopes(run)
        .await
        .attempt()
        .cloned()
        .expect("the body reported usage")
}

/// [`account`] by usage scope.
async fn account_scopes(run: impl FnOnce(&mut Writer, &Store)) -> Usage {
    let fixture = Fixture::new(|_| {});
    let (allocator, reader) = fixture
        .rt
        .streams()
        .with_store(|store| {
            let mut writer = store.create().unwrap();
            let reader = store.reader(writer.id(), 0).unwrap();
            run(&mut writer, store);
            let allocator = writer.allocator();
            writer.finish(Terminal::Done);
            (allocator, reader)
        })
        .unwrap();
    let (usage, _): (_, ResponseFacts) = usage_snapshot::read(allocator, reader).await;
    usage
}

fn ingest(ingress: &mut impl ResponseIngress, writer: &mut Writer, body: &[u8]) {
    ingress.advance(writer, body).unwrap();
    ingress.finish(writer).unwrap();
}

fn anthropic(body: &[u8], mode: BodyMode) -> impl FnOnce(&mut Writer, &Store) + '_ {
    move |writer, _| {
        let mut ingress =
            ck_translate::anthropic::response::Ingress::new(writer, identity(), Operation::Generate, mode, LIMIT)
                .unwrap();
        ingest(&mut ingress, writer, body);
    }
}

fn text(value: &str) -> Dimension {
    Dimension::Text(value.into())
}

#[tokio::test]
async fn anthropic_tier_speed_region_and_server_tools_reach_accounting() {
    let body = br#"{"id":"msg","type":"message","role":"assistant","model":"claude","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":5,"output_tokens":2,"cache_read_input_tokens":0,"cache_creation_input_tokens":0,"server_tool_use":{"web_search_requests":2,"web_fetch_requests":0},"service_tier":"priority","speed":"fast","inference_geo":"us"}}"#;
    let usage = account(anthropic(body, BodyMode::Buffered)).await;
    assert_eq!(usage.service_tier.as_deref(), Some("priority"));
    assert_eq!(
        usage.dimensions,
        vec![
            ("speed", text("fast")),
            ("inference_geo", text("us")),
            ("web_search_requests", Dimension::Count(2)),
            ("web_fetch_requests", Dimension::Count(0)),
        ]
    );
}

/// The streamed message's later usage replaces the counters it supplies; a counter and a field
/// the upstream never reported stay absent.
#[tokio::test]
async fn anthropic_unreported_dimensions_stay_absent() {
    let body = concat!(
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":5,\"output_tokens\":1,\"service_tier\":\"standard\",\"inference_geo\":null}}}\n\n",
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n",
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":4,\"server_tool_use\":{\"web_search_requests\":3}}}\n\n",
        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
    );
    let usage = account(anthropic(body.as_bytes(), BodyMode::Streaming)).await;
    assert_eq!(usage.service_tier.as_deref(), Some("standard"));
    assert_eq!(usage.dimensions, vec![("web_search_requests", Dimension::Count(3))]);
}

/// A dimension of the wrong kind, or a count that is not an unsigned integer, is unknown; the
/// token counts of the same snapshot still reach accounting.
#[tokio::test]
async fn malformed_dimensions_stay_absent() {
    let body = br#"{"id":"msg","type":"message","role":"assistant","model":"claude","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":5,"output_tokens":2,"server_tool_use":{"web_search_requests":-1,"web_fetch_requests":1.5},"speed":42,"service_tier":7}}"#;
    let usage = account(anthropic(body, BodyMode::Buffered)).await;
    assert_eq!((usage.input_tokens, usage.output_tokens), (Some(5), Some(2)));
    assert_eq!(usage.dimensions, Vec::new());
    assert_eq!(usage.service_tier, None);
}

#[tokio::test]
async fn gemini_tool_use_prompt_is_input_and_its_tier_reaches_accounting() {
    let body = br#"{"candidates":[{"content":{"role":"model","parts":[{"text":"a"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":100,"toolUsePromptTokenCount":20,"candidatesTokenCount":10,"thoughtsTokenCount":5,"totalTokenCount":135,"serviceTier":"flex"}}"#;
    let usage = account(|writer, _| {
        let mut ingress = ck_translate::gemini::response::Ingress::new(
            writer,
            identity(),
            Operation::Generate,
            BodyMode::Buffered,
            LIMIT,
        )
        .unwrap();
        ingest(&mut ingress, writer, body);
    })
    .await;
    assert_eq!(
        (usage.input_tokens, usage.output_tokens, usage.total_tokens),
        (Some(120), Some(15), Some(135))
    );
    assert_eq!(usage.service_tier.as_deref(), Some("flex"));
}

#[tokio::test]
async fn responses_cache_write_ttls_reach_accounting_in_their_own_buckets() {
    let body = br#"{"id":"r","object":"response","created_at":1,"model":"m","status":"completed","output":[{"id":"msg","type":"message","role":"assistant","content":[{"type":"output_text","text":"hi"}]}],"usage":{"input_tokens":53,"input_tokens_details":{"cached_tokens":0,"cache_write_tokens":7,"cache_write_tokens_details":{"ttl_30m_tokens":3,"ttl_12h_tokens":4}},"output_tokens":18,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":71}}"#;
    let usage = account(|writer, _| {
        let mut ingress = ck_translate::responses::response::Ingress::new(
            writer,
            identity(),
            Operation::Generate,
            BodyMode::Buffered,
            LIMIT,
        )
        .unwrap();
        ingest(&mut ingress, writer, body);
    })
    .await;
    assert_eq!(
        (
            usage.cache_write_30m_tokens,
            usage.cache_write_12h_tokens,
            usage.cache_write_5m_tokens,
            usage.cache_write_1h_tokens
        ),
        (Some(3), Some(4), None, None)
    );
}

/// Chat's modality and prediction details are dimensions; an explicit zero stays zero and an
/// unreported one is absent.
#[tokio::test]
async fn chat_modality_and_prediction_details_reach_accounting() {
    let body = br#"{"id":"c","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":8,"total_tokens":18,"prompt_tokens_details":{"cached_tokens":0,"audio_tokens":2,"text_tokens":5},"completion_tokens_details":{"reasoning_tokens":1,"audio_tokens":4,"accepted_prediction_tokens":6,"rejected_prediction_tokens":0}}}"#;
    let usage = account(|writer, store| {
        let mut ingress = ck_translate::codec::chat::response::Ingress::new(
            writer,
            store,
            identity(),
            Operation::Generate,
            BodyMode::Buffered,
            LIMIT,
        )
        .unwrap();
        ingest(&mut ingress, writer, body);
    })
    .await;
    assert_eq!(
        usage.dimensions,
        vec![
            ("input_audio_tokens", Dimension::Count(2)),
            ("input_text_tokens", Dimension::Count(5)),
            ("output_audio_tokens", Dimension::Count(4)),
            ("accepted_prediction_tokens", Dimension::Count(6)),
            ("rejected_prediction_tokens", Dimension::Count(0)),
        ]
    );
}

/// Each billed unit a Responses usage reports reaches accounting as its own dimension in its own
/// unit, buffered or streamed: Meta's per-image and per-frame counts, xAI's charge in ticks and
/// in nano-USD and its server-side tool counts, and Codex's rollout budget units as the number
/// the upstream wrote. An unreported, null or malformed unit stays absent; the token counts are
/// the upstream's. A streamed usage repeated on a later event replaces the earlier one rather
/// than adding to it.
#[tokio::test]
async fn responses_billed_units_reach_accounting() {
    let decimal = |n: &str| Dimension::Decimal(n.into());
    let tokens = serde_json::json!({"input_tokens":12,"input_tokens_details":{"cached_tokens":0},"output_tokens":3,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":15});
    for (counts, expected) in [
        (
            serde_json::json!({"images_processed":2}),
            vec![("images_processed", Dimension::Count(2))],
        ),
        (
            serde_json::json!({"video_frames_processed":48}),
            vec![("video_frames_processed", Dimension::Count(48))],
        ),
        (
            serde_json::json!({"images_processed":1,"video_frames_processed":0}),
            vec![
                ("images_processed", Dimension::Count(1)),
                ("video_frames_processed", Dimension::Count(0)),
            ],
        ),
        (
            serde_json::json!({"cost_in_usd_ticks":158100000,"cost_in_nano_usd":15810000,"num_sources_used":3,"num_server_side_tools_used":4,"server_side_tool_usage_details":{"web_search_calls":1,"x_search_calls":1,"x_posts_fetched":7,"x_users_fetched":2,"code_interpreter_calls":0,"file_search_calls":0,"document_search_calls":0,"image_generation_calls":0,"mcp_calls":1}}),
            vec![
                ("cost_in_usd_ticks", Dimension::Count(158100000)),
                ("cost_in_nano_usd", Dimension::Count(15810000)),
                ("num_sources_used", Dimension::Count(3)),
                ("num_server_side_tools_used", Dimension::Count(4)),
                ("web_search_calls", Dimension::Count(1)),
                ("x_search_calls", Dimension::Count(1)),
                ("x_posts_fetched", Dimension::Count(7)),
                ("x_users_fetched", Dimension::Count(2)),
                ("code_interpreter_calls", Dimension::Count(0)),
                ("file_search_calls", Dimension::Count(0)),
                ("document_search_calls", Dimension::Count(0)),
                ("image_generation_calls", Dimension::Count(0)),
                ("mcp_calls", Dimension::Count(1)),
            ],
        ),
        (
            serde_json::json!({"cost_in_usd_ticks":0,"server_side_tool_usage_details":{"x_posts_fetched":5}}),
            vec![
                ("cost_in_usd_ticks", Dimension::Count(0)),
                ("x_posts_fetched", Dimension::Count(5)),
            ],
        ),
        (
            serde_json::json!({"codex_rollout_budget_units":2.5}),
            vec![("codex_rollout_budget_units", decimal("2.5"))],
        ),
        (
            serde_json::json!({"codex_rollout_budget_units":3}),
            vec![("codex_rollout_budget_units", decimal("3"))],
        ),
        (serde_json::json!({}), Vec::new()),
        (
            serde_json::json!({"images_processed":null,"video_frames_processed":-1}),
            Vec::new(),
        ),
        (
            serde_json::json!({"cost_in_usd_ticks":"12","cost_in_nano_usd":1.5,"server_side_tool_usage_details":null,"codex_rollout_budget_units":"2.5"}),
            Vec::new(),
        ),
    ] {
        let mut usage = tokens.clone();
        usage
            .as_object_mut()
            .unwrap()
            .extend(counts.as_object().unwrap().clone());
        let response = |status: &str| serde_json::json!({"id":"r","object":"response","created_at":1,"model":"m","status":status,"output":[{"id":"msg","type":"message","role":"assistant","content":[{"type":"output_text","text":"mask"}]}],"usage":usage});
        let streamed: String = [
            serde_json::json!({"type":"response.created","sequence_number":0,"response":{"id":"r","object":"response","created_at":1,"model":"m","status":"in_progress","output":[],"usage":null}}),
            serde_json::json!({"type":"response.in_progress","sequence_number":1,"response":response("in_progress")}),
            serde_json::json!({"type":"response.completed","sequence_number":2,"response":response("completed")}),
        ]
        .iter()
        .map(|event| format!("event: {}\ndata: {event}\n\n", event["type"].as_str().unwrap()))
        .collect();
        for (mode, body) in [
            (BodyMode::Buffered, response("completed").to_string()),
            (BodyMode::Streaming, streamed),
        ] {
            let accounted = account(|writer, _| {
                let mut ingress = ck_translate::responses::response::Ingress::new(
                    writer,
                    identity(),
                    Operation::Generate,
                    mode,
                    LIMIT,
                )
                .unwrap();
                ingest(&mut ingress, writer, body.as_bytes());
            })
            .await;
            assert_eq!(accounted.dimensions, expected, "{counts} {mode:?}");
            assert_eq!(
                (accounted.input_tokens, accounted.output_tokens, accounted.total_tokens),
                (Some(12), Some(3), Some(15)),
                "{counts} {mode:?}"
            );
        }
    }
}

/// Snapshot records as a codec emits them: `(scope, input tokens)` each.
fn snapshots<'a>(rows: &'a [(Option<&'a str>, u64)]) -> impl FnOnce(&mut Writer, &Store) + 'a {
    move |writer, _| {
        let mut emitter = Emitter::new(identity());
        emitter
            .emit(
                writer,
                Kind::ResponseStart,
                Scope::default(),
                |e| e.bytes(b",\"id\":\"r\",\"model\":\"m\",\"created\":0"),
                |_| Ok(()),
            )
            .unwrap();
        for (scope, input) in rows {
            let payload = match scope {
                Some(scope) => format!(
                    ",\"scope\":\"{scope}\",\"usage\":{{\"native\":{{\"protocol\":\"responses\",\"value\":{{}}}},\"final\":true,\"input_tokens\":{input}}}"
                ),
                None => format!(
                    ",\"usage\":{{\"native\":{{\"protocol\":\"responses\",\"value\":{{}}}},\"final\":true,\"input_tokens\":{input}}}"
                ),
            };
            emitter
                .emit(
                    writer,
                    Kind::Usage,
                    Scope::default(),
                    |e| e.bytes(payload.as_bytes()),
                    |_| Ok(()),
                )
                .unwrap();
        }
    }
}

/// Usage is read per scope (E-Q13): a snapshot replaces only its own scope's, scopes are never
/// summed, the attempt's own scope stays apart from the named ones, and no published scope is
/// dropped, however many the stream holds.
#[tokio::test]
async fn usage_is_read_per_scope() {
    // 65 upstream responses, then the attempt's own usage.
    let names: Vec<String> = (0..=64).map(|n| format!("response:{n}")).collect();
    let mut many: Vec<(Option<&str>, u64)> = names.iter().zip(1..).map(|(s, n)| (Some(s.as_str()), n)).collect();
    many.push((None, 1000));
    // (label, snapshots in stream order, expected attempt input, expected named scopes)
    type Case<'a> = (&'a str, Vec<(Option<&'a str>, u64)>, Option<u64>, Vec<(String, u64)>);
    let cases: Vec<Case> = vec![
        ("attempt only", vec![(None, 1), (None, 2)], Some(2), vec![]),
        (
            "two scopes stay apart",
            vec![(None, 10), (Some("tool:gpt-image-2"), 50)],
            Some(10),
            vec![("tool:gpt-image-2".into(), 50)],
        ),
        (
            "replacement within a scope only",
            vec![
                (Some("job:v"), 1),
                (None, 2),
                (Some("tool:a"), 3),
                (Some("job:v"), 4),
                (None, 5),
            ],
            Some(5),
            vec![("job:v".into(), 4), ("tool:a".into(), 3)],
        ),
        (
            "a named scope alone leaves the attempt's unreported",
            vec![(Some("job:v"), 7)],
            None,
            vec![("job:v".into(), 7)],
        ),
        (
            "every published scope is kept",
            many,
            Some(1000),
            names.iter().cloned().zip(1..).collect(),
        ),
    ];
    for (label, rows, attempt, named) in cases {
        let usage = account_scopes(snapshots(&rows)).await;
        assert_eq!(usage.attempt().and_then(|u| u.input_tokens), attempt, "{label}");
        let scoped: Vec<(String, u64)> = usage
            .scoped()
            .map(|(scope, u)| (scope.to_owned(), u.input_tokens.unwrap()))
            .collect();
        assert_eq!(scoped, named, "{label}");
    }
}

/// The codecs' own scopes reach the reader: Codex's image tool apart from the model, and a video
/// job apart from the poll attempt, with xAI's charge for images and video jobs.
#[tokio::test]
async fn codec_usage_scopes_and_xai_media_charges_reach_accounting() {
    let image = br#"{"created":1,"data":[{"url":"https://example.invalid/a.png"}],"usage":{"cost_in_usd_ticks":7,"input_tokens":1}}"#;
    let usage = account_scopes(|writer, _| {
        let mut ingress = ck_translate::images::Ingress::new(
            writer,
            identity(),
            ck_translate::images::Wire::Xai,
            BodyMode::Buffered,
            LIMIT,
        )
        .unwrap();
        ingest(&mut ingress, writer, image);
    })
    .await;
    let attempt = usage.attempt().unwrap();
    assert_eq!(attempt.input_tokens, Some(1));
    assert_eq!(attempt.dimensions, vec![("cost_in_usd_ticks", Dimension::Count(7))]);
    assert_eq!(usage.scoped().count(), 0);

    let tool = br#"event: response.output_item.done
data: {"type":"response.output_item.done","item":{"type":"image_generation_call","result":"AAEC"}}

event: response.completed
data: {"type":"response.completed","response":{"created_at":1,"usage":{"input_tokens":5,"output_tokens":6,"total_tokens":11},"tool_usage":{"image_gen":{"input_tokens":50,"output_tokens":1056,"total_tokens":1106}},"tools":[{"type":"image_generation","model":"gpt-image-2"}]}}

"#;
    let usage = account_scopes(|writer, _| {
        let mut ingress = ck_translate::images::Ingress::new(
            writer,
            identity(),
            ck_translate::images::Wire::Codex,
            BodyMode::Streaming,
            LIMIT,
        )
        .unwrap();
        ingest(&mut ingress, writer, tool);
    })
    .await;
    assert_eq!(usage.attempt().and_then(|u| u.input_tokens), Some(5));
    let scoped: Vec<_> = usage.scoped().map(|(s, u)| (s.to_owned(), u.input_tokens)).collect();
    assert_eq!(scoped, vec![("tool:gpt-image-2".to_owned(), Some(50))]);

    let poll = br#"{"status":"done","usage":{"cost_in_usd_ticks":7}}"#;
    let usage = account_scopes(|writer, _| {
        let mut ingress = ck_translate::video::Ingress::new(
            writer,
            identity(),
            ck_translate::video::Dialect::Xai,
            ck_translate::video::Phase::Job,
            Some("vid_1"),
            LIMIT,
            Box::new(FirstReport),
        )
        .unwrap();
        ingest(&mut ingress, writer, poll);
    })
    .await;
    assert_eq!(usage.attempt(), None);
    let scoped: Vec<_> = usage
        .scoped()
        .map(|(s, u)| (s.to_owned(), u.dimensions.clone()))
        .collect();
    assert_eq!(
        scoped,
        vec![("job:vid_1".to_owned(), vec![("cost_in_usd_ticks", Dimension::Count(7))])]
    );
}

/// A job table for which every report is the job's first.
struct FirstReport;

impl ck_translate::video::Ledger for FirstReport {
    fn accepted(&mut self, _: &str) {}

    fn charge(
        &mut self,
        _: &str,
        publish: &mut dyn FnMut() -> ck_translate::codec::Result<()>,
    ) -> ck_translate::codec::Result<bool> {
        publish().map(|()| true)
    }
}
