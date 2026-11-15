//! Accounting consumes every billing dimension a provider's usage reports: counts the
//! canonical snapshot maps and provider fields kept only in its native value. A dimension the
//! upstream did not report stays absent, never zero.

mod stream_fixture;

use ck_server::observability::usage_snapshot::{self, CanonicalUsage, Dimension, ResponseFacts};
use ck_stream::{Store, Terminal, Writer};
use ck_translate::codec::event::{Direction, Identity, LocalId};
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
    usage.expect("the body reported usage")
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

/// Meta segmentation bills per image and per video frame (`usage.images_processed`,
/// `usage.video_frames_processed`). Each count the Responses usage reports reaches accounting
/// as its own dimension, buffered or streamed; an unreported or malformed one stays absent; the
/// token counts are the upstream's. A streamed usage repeated on a later event replaces the
/// earlier one rather than adding to it.
#[tokio::test]
async fn meta_segmentation_counts_reach_accounting() {
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
        (serde_json::json!({}), Vec::new()),
        (
            serde_json::json!({"images_processed":null,"video_frames_processed":-1}),
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
