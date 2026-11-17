//! Usage accounting reads only canonical `x_ck.usage` (`docs/EXECUTION-CONTRACT.md` §8): the
//! last snapshot replaces earlier ones, a missing count stays missing, and a resume
//! accounts nothing.

mod stream_fixture;

use ck_server::http::egress::Framing;
use ck_server::observability::usage_snapshot::{self, ResponseFacts, Usage};
use stream_fixture::*;

fn usage_chunk(usage: &str, tier: &str) -> bytes::Bytes {
    bytes::Bytes::from(format!(
        "data: {{\"id\":\"r\",\"model\":\"m\",\"created\":1,\"service_tier\":{tier},\"choices\":[],\"usage\":{usage}}}\n\n"
    ))
}

/// Spawns the runtime-owned usage reader on `live`'s response stream.
fn usage_reader(fixture: &Fixture, live: &Live) -> tokio::task::JoinHandle<(Usage, ResponseFacts)> {
    let streams = fixture.rt.streams();
    let reader = streams
        .with_store(|store| store.reader(live.response.id(), 0))
        .unwrap()
        .unwrap();
    streams.spawn(usage_snapshot::read(live.response.allocator(), reader))
}

#[tokio::test]
async fn the_last_snapshot_is_the_usage_once_however_often_the_stream_is_read() {
    let fixture = Fixture::new(|_| {});
    fixture.rt.usage_queue().configure(
        true,
        &ck_core::config::Config::parse("observability: {usage: {usage-statistics-enabled: true}}\n").unwrap(),
    );
    let app = fixture.app();
    let mut live = commit(&fixture, owner(KEY), Framing::Sse, STREAMED);
    let accounting = usage_reader(&fixture, &live);
    live.send(chunk(r#"{"role":"assistant","content":"hel"}"#, "null"));
    // A progress snapshot, then the final one: accounting takes the last, never the sum.
    live.send(usage_chunk(
        r#"{"prompt_tokens":10,"completion_tokens":5}"#,
        r#""flex""#,
    ));
    live.send(chunk(r#"{"content":"lo"}"#, "null"));
    live.send(chunk("{}", r#""stop""#));
    live.send(usage_chunk(
        r#"{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30,"prompt_tokens_details":{"cached_tokens":4}}"#,
        "null",
    ));
    live.send(done());
    live.end();

    let (usage, facts) = accounting.await.unwrap();
    let usage = usage.attempt().cloned().expect("the stream reported usage");
    assert_eq!(
        (
            usage.input_tokens,
            usage.output_tokens,
            usage.total_tokens,
            usage.cache_read_tokens
        ),
        (Some(10), Some(20), Some(30), Some(4))
    );
    // Counts the upstream never reported stay absent rather than zero.
    assert_eq!(
        (
            usage.reasoning_tokens,
            usage.cache_write_tokens,
            usage.cache_write_5m_tokens
        ),
        (None, None, None)
    );
    assert!(usage.final_snapshot);
    // The explicit `null` tier of the last chunk clears the earlier `flex`.
    assert_eq!(
        facts,
        ResponseFacts {
            model: Some("m".into()),
            service_tier: None
        }
    );

    // Client delivery and resumes, live or repeated, account nothing.
    for _ in 0..3 {
        let (text, clean) = body(resume(&app, &live, "v1.e.0").await).await;
        assert!(clean && text.contains("\"total_tokens\":30"), "{text}");
    }
    assert!(fixture.rt.usage_queue().pop_oldest(10).is_empty());
    // A second reader of the same stream sees the same snapshot, not an increment.
    let (again, _) = usage_reader(&fixture, &live).await.unwrap();
    assert_eq!(again.attempt(), Some(&usage));
}

/// Kimi reports usage in each choice's last chunk, with the same prompt and that choice's
/// completion: the attempt's usage counts the prompt once and every completion, an aggregate
/// replaces it, and a stream cut short keeps what its choices reported.
#[tokio::test]
async fn choice_usage_counts_the_shared_prompt_once() {
    let choice = |index: u64, delta: &str, finish: &str, usage: Option<u64>| {
        let usage = usage.map_or(String::new(), |completion| {
            format!(
                r#","usage":{{"prompt_tokens":10,"completion_tokens":{completion},"total_tokens":{}}}"#,
                10 + completion
            )
        });
        bytes::Bytes::from(format!(
            "data: {{\"id\":\"r\",\"model\":\"m\",\"created\":1,\"choices\":[{{\"index\":{index},\"delta\":{delta},\"finish_reason\":{finish}{usage}}}]}}\n\n"
        ))
    };
    let opening = || {
        vec![
            choice(0, r#"{"role":"assistant","content":"a"}"#, "null", None),
            choice(1, r#"{"role":"assistant","content":"b"}"#, "null", None),
        ]
    };
    let both = || {
        let mut chunks = opening();
        chunks.push(choice(0, "{}", r#""stop""#, Some(5)));
        chunks.push(choice(1, "{}", r#""stop""#, Some(7)));
        chunks
    };
    // (label, upstream chunks, whether the body completes, (input, output, total)).
    type Row = (&'static str, Vec<bytes::Bytes>, bool, (u64, u64, u64));
    let rows: Vec<Row> = vec![
        ("two choices without an aggregate", both(), true, (10, 12, 22)),
        (
            "an aggregate replaces the choices' usage",
            {
                let mut chunks = both();
                chunks.push(usage_chunk(
                    r#"{"prompt_tokens":10,"completion_tokens":13,"total_tokens":23}"#,
                    "null",
                ));
                chunks
            },
            true,
            (10, 13, 23),
        ),
        (
            "a cut stream keeps the usage its choices reported",
            {
                let mut chunks = opening();
                chunks.push(choice(0, "{}", r#""stop""#, Some(5)));
                chunks
            },
            false,
            (10, 5, 15),
        ),
    ];
    for (label, chunks, complete, expected) in rows {
        let fixture = Fixture::new(|_| {});
        let mut live = commit(&fixture, owner(KEY), Framing::Sse, STREAMED);
        let accounting = usage_reader(&fixture, &live);
        for chunk in chunks {
            live.send(chunk);
        }
        if complete {
            live.send(done());
            live.end();
        } else {
            live.fail();
        }
        let (usage, _) = accounting.await.unwrap();
        let usage = usage.attempt().cloned().unwrap_or_default();
        assert_eq!(
            (usage.input_tokens, usage.output_tokens, usage.total_tokens),
            (Some(expected.0), Some(expected.1), Some(expected.2)),
            "{label}: {usage:?}"
        );
    }
}

#[tokio::test]
async fn a_stream_without_usage_reports_none_not_zero() {
    let fixture = Fixture::new(|_| {});
    let mut live = commit(&fixture, owner(KEY), Framing::Sse, BUFFERED);
    let accounting = usage_reader(&fixture, &live);
    answer(&mut live);
    let (usage, facts) = accounting.await.unwrap();
    assert_eq!(usage, Usage::default());
    assert_eq!(facts.model.as_deref(), Some("m"));
}

/// The usage record's prompt-prefix hash of a client request: Chat, or Anthropic Messages
/// when `anthropic`.
fn prefix_of(fixture: &Fixture, anthropic: bool, body: &str) -> String {
    use ck_translate::anthropic::request::{AnthropicCapability, Ingress as AnthropicIngress};
    use ck_translate::codec::chat::request;
    use ck_translate::codec::event::{Direction, Identity, LocalId};
    use ck_translate::codec::{Operation, RequestIngress};
    fixture
        .rt
        .streams()
        .with_store(|store| {
            let mut stream = store.create().unwrap();
            let identity = Identity {
                request_id: LocalId::new("request").unwrap(),
                stream_id: LocalId::new("stream").unwrap(),
                attempt: 0,
                direction: Direction::Request,
            };
            let (operation, limit) = (Operation::Generate, 1 << 20);
            if anthropic {
                let mut ingress = AnthropicIngress::new(&mut stream, store, identity, operation, limit).unwrap();
                ingress.advance(&mut stream, body.as_bytes()).unwrap();
                let validated = ingress.finish(&mut stream, &AnthropicCapability::default()).unwrap();
                usage_snapshot::prompt_prefix(validated.view(), &mut stream).unwrap()
            } else {
                let mut ingress = request::Ingress::new(&mut stream, store, identity, operation, limit).unwrap();
                ingress.advance(&mut stream, body.as_bytes()).unwrap();
                let validated = ingress.finish(&mut stream, &request::ChatCapability).unwrap();
                usage_snapshot::prompt_prefix(validated.view(), &mut stream).unwrap()
            }
        })
        .unwrap()
}

fn prefix(fixture: &Fixture, body: &str) -> String {
    prefix_of(fixture, false, body)
}

#[tokio::test]
async fn the_prompt_prefix_is_the_same_across_client_protocols_and_member_order() {
    let fixture = Fixture::new(|_| {});
    let chat = prefix(
        &fixture,
        r#"{"model":"m","messages":[{"content":"be brief","role":"system"},{"role":"user","content":"hi"}]}"#,
    );
    let ordered = prefix(
        &fixture,
        r#"{"model":"m","messages":[{"role":"system","content":"be brief"},{"role":"user","content":"hi"}]}"#,
    );
    let anthropic = prefix_of(
        &fixture,
        true,
        r#"{"model":"m","max_tokens":16,"system":"be brief","messages":[{"role":"user","content":"hi"}]}"#,
    );
    assert_eq!(chat, ordered, "member order is not part of the prefix");
    assert_eq!(chat, anthropic, "the client protocol is not part of the prefix");
    assert_ne!(chat, "");
}

#[tokio::test]
async fn the_prompt_prefix_names_tools_and_leading_system_messages() {
    let fixture = Fixture::new(|_| {});
    let tools = r#""tools":[{"type":"function","function":{"name":"f","parameters":{"type":"object"}}}]"#;
    let with = |system: &str, user: &str| {
        format!(
            r#"{{"model":"m",{tools},"messages":[{{"role":"system","content":"{system}"}},{{"role":"user","content":"{user}"}}]}}"#
        )
    };
    let base = prefix(&fixture, &with("be brief", "hi"));
    assert_eq!(base.len(), 16);
    assert_eq!(
        prefix(&fixture, &with("be brief", "another turn")),
        base,
        "the conversation is not part of it"
    );
    assert_ne!(prefix(&fixture, &with("be long", "hi")), base);
    assert_ne!(
        prefix(
            &fixture,
            r#"{"model":"m","messages":[{"role":"system","content":"be brief"},{"role":"user","content":"hi"}]}"#
        ),
        base,
        "the tools are part of it"
    );
    assert_eq!(
        prefix(&fixture, r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#),
        ""
    );
}
