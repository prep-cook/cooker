//! The executor pipeline (`docs/EXECUTION-CONTRACT.md` §4–5, §7, §9): fences, attempt start
//! and the runtime-owned producer, driven through a stub executor whose upstream body the
//! test feeds over a channel.

mod support;

use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use ck_core::exec::FailureScope;
use ck_stream::Terminal;
use ck_translate::codec::Operation;
use ck_translate::codec::event::Kind;
use support::{Answer, Fixture, Stub, WAIT, chunk, complete, cut, data, feed, finished};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_one_byte_feed_publishes_what_one_chunk_does() {
    let body = complete();
    let (whole, expected, terminal) = feed(Answer::Stream, vec![data(body.clone())]).await;
    whole.unwrap();
    assert_eq!(terminal, Terminal::Done);
    assert!(expected.last().unwrap().contains("x_ck.done"));
    let bytes = body.as_bytes().iter().map(|byte| data(vec![*byte])).collect();
    let (split, records, terminal) = feed(Answer::Stream, bytes).await;
    split.unwrap();
    assert_eq!(terminal, Terminal::Done);
    assert_eq!(records, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_upstream_finishes_while_the_reader_stalls() {
    let fixture = Fixture::new(4 * 1024 * 1024);
    let (stub, upstream) = Stub::new(Answer::Stream);
    let (started, _upload, id) = fixture.launch(&stub, Operation::Generate).await;
    // Attached before any upstream byte and never read until the producer ended.
    let stalled = fixture.store.reader(id, 0).unwrap();
    let deltas = (0..200).map(|_| chunk(r#"{"content":"0123456789"}"#, "null"));
    let body: String = deltas.chain([complete()]).collect();
    for piece in body.as_bytes().chunks(97) {
        upstream.send(Ok(Bytes::copy_from_slice(piece))).unwrap();
    }
    drop(upstream);
    finished(started.unwrap()).await.unwrap();
    drop(stalled);
    let (records, terminal) = fixture.records(id).await;
    assert_eq!(terminal, Terminal::Done);
    assert!(records.len() > 200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_producer_panic_is_producer_lost() {
    let fixture = Fixture::new(4 * 1024 * 1024);
    let (stub, upstream) = Stub::new(Answer::Panic);
    let (started, _upload, id) = fixture.launch(&stub, Operation::Generate).await;
    upstream.send(Ok(Bytes::from(complete()))).unwrap();
    let joined = tokio::time::timeout(WAIT, started.unwrap().producer).await.unwrap();
    assert!(joined.unwrap_err().is_panic());
    let (_, terminal) = fixture.records(id).await;
    assert_eq!(terminal, Terminal::ProducerLost);
    assert_eq!(terminal.error_code(), Some("stream.producer_lost"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancel_stops_the_producer() {
    let fixture = Fixture::new(4 * 1024 * 1024);
    let (stub, upstream) = Stub::new(Answer::Stream);
    let (started, _upload, id) = fixture.launch(&stub, Operation::Generate).await;
    let mut started = started.unwrap();
    started.stream.finish(Terminal::Cancelled);
    finished(started).await.unwrap();
    assert!(upstream.is_closed(), "the producer dropped the upstream body");
    assert_eq!(fixture.records(id).await.1, Terminal::Cancelled);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_discarded_stream_fails_the_attempt_as_gone() {
    let fixture = Fixture::new(4 * 1024 * 1024);
    let (stub, _upstream) = Stub::new(Answer::Stream);
    let (started, _upload, _) = fixture.launch(&stub, Operation::Generate).await;
    let mut started = started.unwrap();
    started.stream.finish(Terminal::Gone);
    assert_eq!(finished(started).await.unwrap_err().status, 410);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_flush_deadline_is_served_while_the_upstream_is_idle() {
    let fixture = Fixture::new(4 * 1024 * 1024);
    let (stub, upstream) = Stub::new(Answer::Stream);
    *stub.deadline.lock().unwrap() = Some(Duration::from_millis(20));
    let (started, _upload, _) = fixture.launch(&stub, Operation::Generate).await;
    let started = started.unwrap();
    tokio::time::timeout(WAIT, stub.flushed.notified()).await.unwrap();
    assert!(!started.producer.is_finished());
    drop(upstream);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_waiting_client_egress_reader_is_served_before_the_flush_deadline() {
    // (client egress reader, body pieces, each fed once the flushes it caused settled): the
    // buffered Chat ingress holds decoded audio until a flush, and its deadline is an hour
    // away, so only a client egress reader waiting at the published end flushes it. A piece that completes base64 an earlier flush could not publish reaches
    // the reader still waiting from that flush.
    let head = r#"{"id":"r","object":"chat.completion","model":"m","created":1,"choices":[{"index":0,"message":{"role":"assistant","audio":{"id":"a","data":""#;
    let whole = format!("{head}AAAA");
    let rows: [(bool, &[&str]); 3] = [(true, &[&whole]), (false, &[&whole]), (true, &[head, "A", "AAA"])];
    for (egress, pieces) in rows {
        let fixture = Fixture::new(4 * 1024 * 1024);
        let (stub, upstream) = Stub::new(Answer::Buffered);
        *stub.postpone.lock().unwrap() = Duration::from_secs(3600);
        let (started, _upload, id) = fixture.launch(&stub, Operation::Generate).await;
        let started = started.unwrap();
        let mut reader = if egress {
            fixture.store.egress_reader(id, 0)
        } else {
            fixture.store.reader(id, 0)
        }
        .unwrap();
        let (data, mut published) = tokio::sync::mpsc::unbounded_channel();
        let reading = tokio::spawn(async move {
            while let Some(ck_stream::Read::Record(lease)) = reader.next().await {
                if lease.record().kind == Kind::Data as u32 {
                    data.send(()).unwrap();
                }
            }
        });
        let (last, earlier) = pieces.split_last().unwrap();
        for piece in earlier {
            upstream.send(Ok(Bytes::copy_from_slice(piece.as_bytes()))).unwrap();
            // The reader has caught up and waits in one wait once no flush follows.
            while tokio::time::timeout(Duration::from_millis(100), stub.flushed.notified())
                .await
                .is_ok()
            {}
        }
        upstream.send(Ok(Bytes::copy_from_slice(last.as_bytes()))).unwrap();
        let wait = if egress { WAIT } else { Duration::from_millis(300) };
        let served = tokio::time::timeout(wait, published.recv()).await;
        assert_eq!(served.is_ok(), egress, "egress {egress}, {} pieces", pieces.len());
        assert!(!started.producer.is_finished());
        drop(upstream);
        finished(started).await.ok();
        reading.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_attempt_ends_its_response_stream_for_the_caller_to_discard() {
    let fixture = Fixture::new(4 * 1024 * 1024);
    let (stub, _upstream) = Stub::new(Answer::Refuse);
    let (started, _upload, id) = fixture.launch(&stub, Operation::Generate).await;
    let error = started.err().unwrap();
    assert_eq!((error.status, error.scope), (503, FailureScope::Transport));
    // The stream keeps its records until the server's finalizer discards it, so a usage
    // reader attached before the call reaches a terminal instead of losing them.
    assert_eq!(fixture.records(id).await.1, Terminal::UpstreamError);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_local_count_publishes_the_canonical_result() {
    let fixture = Fixture::new(4 * 1024 * 1024);
    let (stub, _upstream) = Stub::new(Answer::Local);
    let (started, _upload, id) = fixture.launch(&stub, Operation::CountTokens).await;
    let started = started.unwrap();
    assert_eq!(started.status, 200);
    finished(started).await.unwrap();
    let (records, terminal) = fixture.records(id).await;
    assert_eq!(terminal, Terminal::Done);
    assert_eq!(records.len(), 3);
    assert!(records[0].contains("x_ck.response.start") && records[0].contains(r#""model":"m""#));
    assert!(records[1].contains("x_ck.count_tokens") && records[1].contains(r#""input_tokens":12"#));
    assert!(records[2].contains("x_ck.done"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_producer_charges_the_exchange_quota() {
    let fixture = Fixture::new(4 * 1024 * 1024);
    let (stub, upstream) = Stub::new(Answer::Stream);
    let (started, mut upload, id) = fixture.launch(&stub, Operation::Generate).await;
    // The request stream takes what is left of the exchange's metadata quota.
    let mut permits = Vec::new();
    while let Ok(permit) = upload.reserve_metadata(256) {
        permits.push(permit);
    }
    let mut body = String::new();
    for _ in 0..64 {
        body.push_str(&chunk(r#"{"role":"assistant","content":"0123456789"}"#, "null"));
    }
    upstream.send(Ok(Bytes::from(body))).unwrap();
    let error = finished(started.unwrap()).await.unwrap_err();
    assert_eq!((error.status, error.scope), (503, FailureScope::Request));
    assert_eq!(fixture.records(id).await.1, Terminal::StorageExhausted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_due_flush_goes_before_a_ready_body() {
    let fixture = Fixture::new(4 * 1024 * 1024);
    let (stub, upstream) = Stub::new(Answer::Stream);
    *stub.deadline.lock().unwrap() = Some(Duration::ZERO);
    for _ in 0..10_000 {
        upstream.send(Ok(Bytes::from_static(b": keep\n\n"))).unwrap();
    }
    upstream.send(Ok(Bytes::from(complete()))).unwrap();
    drop(upstream);
    let (started, _upload, _) = fixture.launch(&stub, Operation::Generate).await;
    finished(started.unwrap()).await.unwrap();
    assert_eq!(stub.early.load(Ordering::Acquire), 0, "input went before a due flush");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eof_before_the_completion_boundary_publishes_error() {
    let open = chunk(r#"{"role":"assistant","content":"hel"}"#, "null");
    let (outcome, records, terminal) = feed(Answer::Stream, vec![data(open)]).await;
    assert_eq!(outcome.unwrap_err().status, 502);
    assert_eq!(terminal, Terminal::UpstreamError);
    assert!(records.last().unwrap().contains("event: x_ck.error"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_error_fails_with_its_own_scope() {
    let open = chunk(r#"{"role":"assistant","content":"hel"}"#, "null");
    let items = vec![data(open), cut(429, FailureScope::Credential)];
    let (outcome, records, terminal) = feed(Answer::Stream, items).await;
    let error = outcome.unwrap_err();
    assert_eq!((error.status, error.scope), (429, FailureScope::Credential));
    assert_eq!(terminal, Terminal::UpstreamError);
    assert!(records.iter().all(|record| !record.contains("x_ck.done")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_error_after_complete_json_fails() {
    let json = r#"{"id":"r","object":"chat.completion","model":"m","created":1,"choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}]}"#;
    let items = vec![data(json), cut(502, FailureScope::Transport)];
    let (outcome, records, terminal) = feed(Answer::Buffered, items).await;
    let error = outcome.unwrap_err();
    assert_eq!((error.status, error.scope), (502, FailureScope::Transport));
    assert_eq!(terminal, Terminal::UpstreamError);
    assert!(records.iter().all(|record| !record.contains("x_ck.done")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_error_after_the_completion_boundary_keeps_done() {
    let items = vec![data(complete()), cut(502, FailureScope::Transport)];
    let (outcome, records, terminal) = feed(Answer::Stream, items).await;
    outcome.unwrap();
    assert_eq!(terminal, Terminal::Done);
    assert!(records.last().unwrap().contains("x_ck.done"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_after_the_completion_boundary_keeps_done() {
    let body = format!("{}{}", complete(), chunk(r#"{"content":"late"}"#, "null"));
    let (outcome, records, terminal) = feed(Answer::Stream, vec![data(body)]).await;
    outcome.unwrap();
    assert_eq!(terminal, Terminal::Done);
    assert!(records.last().unwrap().contains("x_ck.done"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_native_upstream_error_fails_the_attempt() {
    let event = "event: error\ndata: {\"type\":\"error\",\"code\":\"server_error\",\"message\":\"boom\"}\n\n";
    let (outcome, records, terminal) = feed(Answer::Responses, vec![data(event)]).await;
    let error = outcome.unwrap_err();
    assert_eq!(terminal, Terminal::UpstreamError);
    let last = records.last().unwrap();
    assert!(last.contains("event: x_ck.error"));
    assert!(last.contains(&format!("\"status\":{}", error.status)), "{last}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gateway_defect_is_producer_lost() {
    let (outcome, records, terminal) = feed(Answer::Misbound, vec![data(complete())]).await;
    let error = outcome.unwrap_err();
    assert_eq!((error.status, error.scope), (500, FailureScope::Request));
    assert!(records.is_empty());
    assert_eq!(terminal, Terminal::ProducerLost);
}

const ANTHROPIC: &str = concat!(
    "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",",
    "\"role\":\"assistant\",\"model\":\"m\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":1,",
    "\"output_tokens\":0}}}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,",
    "\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":",
    "\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hel\"}}\n\n",
);
const RESPONSES: &str = concat!(
    "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",",
    "\"object\":\"response\",\"created_at\":1,\"model\":\"m\",\"status\":\"in_progress\",\"output\":[]}}\n\n",
    "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,",
    "\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"in_progress\",",
    "\"content\":[]}}\n\nevent: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",",
    "\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",",
    "\"text\":\"\",\"annotations\":[]}}\n\nevent: response.output_text.delta\ndata: {\"type\":",
    "\"response.output_text.delta\",\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"hel\"}\n\n",
);
const GEMINI: &str = concat!(
    "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"hel\"}]},\"index\":0}],",
    "\"modelVersion\":\"m\",\"responseId\":\"r\"}\n\n",
);

/// A body error cuts a streamed response of every protocol after some parts: the stream ends
/// with exactly one `error`, after the parts already published.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cut_stream_ends_with_one_error_in_every_protocol() {
    let chat = chunk(r#"{"role":"assistant","content":"hel"}"#, "null");
    for (protocol, answer, opening) in [
        ("chat", Answer::Stream, chat.as_str()),
        ("anthropic", Answer::Anthropic, ANTHROPIC),
        ("responses", Answer::Responses, RESPONSES),
        ("gemini", Answer::Gemini, GEMINI),
    ] {
        let items = vec![data(opening.to_owned()), cut(503, FailureScope::Transport)];
        let (outcome, records, terminal) = feed(answer, items).await;
        assert_eq!(outcome.unwrap_err().status, 503, "{protocol}");
        assert_eq!(terminal, Terminal::UpstreamError, "{protocol}");
        let (last, parts) = records.split_last().unwrap();
        assert!(
            last.contains("event: x_ck.error") && last.contains(r#""status":503"#),
            "{protocol}: {last}"
        );
        assert!(parts.iter().any(|part| part.contains("hel")), "{protocol}: {parts:?}");
        let terminal_part = |part: &&String| part.contains("x_ck.error") || part.contains("x_ck.done");
        assert!(!parts.iter().any(|part| terminal_part(&part)), "{protocol}");
    }
}

/// Grep gate: the pipeline passes upstream chunks on and never collects a body on the heap.
#[test]
fn the_pipeline_collects_no_body() {
    for (file, source) in [
        ("lib.rs", include_str!("../../src/lib.rs")),
        ("types.rs", include_str!("../../src/types.rs")),
        ("pipeline.rs", include_str!("../../src/pipeline.rs")),
        ("wire.rs", include_str!("../../src/wire.rs")),
        ("payload_barrier.rs", include_str!("../../src/payload_barrier.rs")),
        ("rawjson.rs", include_str!("../../src/rawjson.rs")),
    ] {
        assert!(!source.contains("BytesMut"), "{file} collects bytes in BytesMut");
    }
}
