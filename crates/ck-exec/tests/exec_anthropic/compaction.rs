//! Anthropic native compaction on the contract path (owner ruling, Q7 reversal; D2 plan §2.7):
//! a compact is one buffered Messages call with the compaction beta, its signed block is
//! published unchanged, a later request replays it first, and no other destination takes it.

use super::*;
use ck_core::exec::{ResolvedModel, ResolvedSource};
use ck_exec::types::Attempt;
use ck_translate::codec::{Capability, RequestFence, RequestView};

/// Escapes and non-ASCII text, so a re-encoded block would differ from the upstream's bytes.
const SUMMARY: &str = r#"Asked about \"lifetimes\".\nCafé — done."#;
const SIGNATURE: &str = "EqQBCkgIBxABGAIqQGNvbXBhY3Rpb24tc2lnbmF0dXJlLWZpeHR1cmU=";
const BETA: &str = "compact-2026-09-04";
/// A Responses compact of a three-turn conversation, with OpenAI's own `context_management`,
/// which Anthropic refuses together with compaction.
const COMPACT: &[u8] = br#"{"model":"m","max_output_tokens":4096,"context_management":[{"type":"compaction","compact_threshold":1000}],"instructions":"Be brief.","input":[{"role":"user","content":"Explain lifetimes."},{"role":"assistant","content":"They bound references."},{"role":"user","content":"More."}]}"#;

/// Anthropic's answer, shaped like the live check: top-level counts 0, billed tokens only in
/// `iterations`.
fn compacted() -> String {
    format!(
        r#"{{"id":"msg_c","type":"message","role":"assistant","model":"claude-haiku-5-5","content":[{{"type":"compaction","content":"{SUMMARY}","signature":"{SIGNATURE}"}}],"stop_reason":"compaction","stop_sequence":null,"usage":{{"input_tokens":0,"output_tokens":0,"iterations":[{{"type":"compaction","input_tokens":119,"output_tokens":217}}]}}}}"#
    )
}

/// An Anthropic client's next turn after `block`, with the turns `block` summarized before it;
/// the block ends the conversation unless `ask`.
fn continuation(block: &Value, ask: bool) -> Vec<u8> {
    let mut body = json!({"model": "m", "max_tokens": 64, "system": "Be brief.", "messages": [
        {"role": "user", "content": "Explain lifetimes."},
        {"role": "assistant", "content": "They bound references."},
        {"role": "assistant", "content": [block]},
    ]});
    if ask {
        let messages = body["messages"].as_array_mut().unwrap();
        messages.push(json!({"role": "user", "content": "And now?"}));
    }
    serde_json::to_vec(&body).unwrap()
}

/// An API key thinking replay serves: the old arm keeps its generations (Q1).
struct ReplayKey {
    key: ck_core::credential::Credential,
    compat: ResolvedModel,
}

impl ReplayKey {
    fn new(fixture: &Fixture) -> Self {
        let mut key = fixture.exchange.credential.clone();
        key.attributes.insert("auth_kind".into(), "apikey".into());
        let info = json!({"id": SONNET, "is_compat": true}).as_object().unwrap().clone();
        let info = ck_core::registry::ModelInfo::from_raw(info).unwrap();
        let compat = ResolvedModel {
            info,
            source: ResolvedSource::ApiKey,
        };
        Self { key, compat }
    }

    fn attempt<'a>(&'a self, fixture: &'a Fixture) -> Attempt<'a> {
        Attempt {
            credential: &self.key,
            resolved_model: Some(&self.compat),
            ..fixture.exchange.attempt(&fixture.streams.store)
        }
    }
}

/// Feeds `body` one byte at a time, fences it for `attempt` and starts the attempt.
async fn run<I: RequestIngress>(
    fixture: &Fixture,
    attempt: &Attempt<'_>,
    ingress: &mut I,
    request: &mut Writer,
    body: &[u8],
) -> codec::Result<(Result<Started, ExecError>, u64)> {
    for byte in body.chunks(1) {
        ingress.advance(request, byte)?;
    }
    let validated = fixture.executors.fence(attempt, ingress, request)?;
    assert!(fixture.executors.refence(attempt, validated.view()).is_ok());
    let handle = fixture.exchange.request_with(validated, body);
    let response = fixture.streams.response(request);
    let id = response.id();
    Ok((fixture.executors.start(attempt, &handle, request, response).await, id))
}

fn responses_ingress(fixture: &Fixture, request: &mut Writer, operation: Operation) -> responses::request::Ingress {
    let (store, id) = (&fixture.streams.store, identity(Direction::Request));
    responses::request::Ingress::new(request, store, id, operation, LIMIT).unwrap()
}

fn anthropic_ingress(fixture: &Fixture, request: &mut Writer) -> anthropic::request::Ingress {
    let (store, id) = (&fixture.streams.store, identity(Direction::Request));
    anthropic::request::Ingress::new(request, store, id, Operation::Generate, LIMIT).unwrap()
}

fn has_beta(captured: &support::upstream::Captured) -> bool {
    captured
        .header("anthropic-beta")
        .is_some_and(|betas| betas.split(',').any(|b| b.trim() == BETA))
}

/// Compacts `COMPACT` on `attempt`; returns the canonical compact item.
async fn compact(fixture: &Fixture, upstream: &StreamUpstream, attempt: &Attempt<'_>) -> Value {
    let mut request = fixture.streams.request();
    let mut ingress = responses_ingress(fixture, &mut request, Operation::Compact);
    let (started, id) = run(fixture, attempt, &mut ingress, &mut request, COMPACT)
        .await
        .unwrap();
    started.unwrap().producer.await.unwrap().unwrap();
    let (sent, captured) = sent(upstream);
    assert_eq!(captured.path, "/v1/messages?beta=true");
    assert!(has_beta(&captured), "{:?}", captured.header("anthropic-beta"));
    assert_eq!(sent["compaction"], json!({"type": "summarize"}));
    // Another protocol's member stays in its own map (Q3), so it never meets the compaction.
    assert!(sent.get("context_management").is_none(), "{sent}");
    assert_eq!(sent["stream"], false);
    assert!(sent["max_tokens"].as_u64().is_some_and(|n| n > 0), "{sent}");
    assert_eq!(sent["messages"].as_array().map(Vec::len), Some(3), "{sent}");
    // The canonical item holds the block's own bytes, escapes included.
    let published = fixture.streams.read_to_end(id).await;
    assert_eq!(published.terminal, Some(Terminal::Done));
    let block = format!(r#""content":"{SUMMARY}","signature":"{SIGNATURE}""#);
    assert!(
        published
            .records
            .iter()
            .any(|r| r.windows(block.len()).any(|w| w == block.as_bytes()))
    );
    let (records, _) = events(&fixture.streams, id).await;
    let usage = &records.iter().rev().find(|(name, _)| name == "x_ck.usage").unwrap().1["usage"];
    assert_eq!(
        (&usage["input_tokens"], &usage["output_tokens"]),
        (&json!(119), &json!(217))
    );
    let item = records.iter().find(|(name, _)| name == "x_ck.compact").unwrap();
    item.1["item"].clone()
}

/// Sends `block`'s continuation on `attempt`; returns the upstream body and whether it carried
/// the compaction beta.
async fn replay(
    fixture: &Fixture,
    upstream: &StreamUpstream,
    attempt: &Attempt<'_>,
    block: &Value,
    ask: bool,
) -> Value {
    let mut request = fixture.streams.request();
    let mut ingress = anthropic_ingress(fixture, &mut request);
    let body = continuation(block, ask);
    let (started, _) = run(fixture, attempt, &mut ingress, &mut request, &body).await.unwrap();
    started.unwrap().producer.await.unwrap().unwrap();
    let (sent, captured) = sent(upstream);
    assert!(has_beta(&captured), "{:?}", captured.header("anthropic-beta"));
    // Never an edited block: the upload carries the upstream's bytes.
    let raw = format!(r#""content":"{SUMMARY}","signature":"{SIGNATURE}""#);
    assert!(captured.body.windows(raw.len()).any(|w| w == raw.as_bytes()), "{sent}");
    sent
}

/// Compact, then replay on the same credential: an API key, and one thinking replay serves,
/// whose compaction and continuation both take the contract path. The rolling cache marker
/// never lands inside the signed block, even when the block ends the conversation.
#[tokio::test]
async fn compact_then_replay_sends_the_block_first_and_unchanged() {
    for (replay_key, ask) in [(false, true), (true, true), (false, false)] {
        let reply = Reply::json(compacted()).byte_at_a_time();
        let upstream = StreamUpstream::start([reply, Reply::json(MESSAGE)]).await;
        let fixture = Fixture::new(&upstream, SONNET);
        let key = ReplayKey::new(&fixture);
        let attempt = match replay_key {
            true => key.attempt(&fixture),
            false => fixture.exchange.attempt(&fixture.streams.store),
        };
        let item = compact(&fixture, &upstream, &attempt).await;
        let native: Value = serde_json::from_str(&compacted()).unwrap();
        assert_eq!(
            item,
            json!({"type": "opaque", "protocol": "anthropic", "value": native["content"][0]})
        );
        let sent = replay(&fixture, &upstream, &attempt, &item["value"], ask).await;
        let messages = sent["messages"].as_array().unwrap();
        assert_eq!(
            messages[0],
            json!({"role": "assistant", "content": [native["content"][0]]})
        );
        assert_eq!(
            messages.len(),
            1 + usize::from(ask),
            "earlier turns stay behind: {sent}"
        );
        assert!(sent["system"].to_string().contains("Be brief."), "{sent}");
    }
}

/// A credential thinking replay serves answers its capability key; only its generations
/// without a compaction block stay on the old arm (D2 plan §2.7, part 1).
#[tokio::test]
async fn replay_eligible_compaction_takes_new_arm() {
    let upstream = StreamUpstream::start([]).await;
    let fixture = Fixture::new(&upstream, SONNET);
    let key = ReplayKey::new(&fixture);
    let attempt = key.attempt(&fixture);
    assert_eq!(fixture.executors.destination(&attempt), Some("anthropic"));
    let plain = br#"{"model":"m","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}"#;
    let mut request = fixture.streams.request();
    let mut ingress = anthropic_ingress(&fixture, &mut request);
    let verdict = run(&fixture, &attempt, &mut ingress, &mut request, plain).await;
    assert!(matches!(verdict.err(), Some(codec::Error::Destination)));
    // The same request re-fenced from an API key's client fence is refused as well.
    let mut request = fixture.streams.request();
    let mut ingress = anthropic_ingress(&fixture, &mut request);
    for byte in plain.chunks(1) {
        ingress.advance(&mut request, byte).unwrap();
    }
    let api_key = fixture.exchange.attempt(&fixture.streams.store);
    let validated = fixture.executors.fence(&api_key, &mut ingress, &mut request).unwrap();
    let refused = fixture.executors.refence(&attempt, validated.view());
    assert!(matches!(refused.err(), Some(codec::Error::Destination)));
    let handle = fixture.exchange.request_with(validated, plain);
    let response = fixture.streams.response(&request);
    let started = fixture.executors.start(&attempt, &handle, &mut request, response).await;
    assert!(
        started.is_err(),
        "start keeps the old arm's generation off the contract path"
    );
    assert_eq!(upstream.requests(), 0);
}

/// Every other destination refuses a request carrying the block (Q3, Q-C): it is skipped,
/// never sent elsewhere or dropped.
#[tokio::test]
async fn other_destinations_refuse_a_compaction_block() {
    let upstream = StreamUpstream::start([]).await;
    let fixture = Fixture::new(&upstream, SONNET);
    let native: Value = serde_json::from_str(&compacted()).unwrap();
    let mut request = fixture.streams.request();
    let mut ingress = anthropic_ingress(&fixture, &mut request);
    for byte in continuation(&native["content"][0], true).chunks(1) {
        ingress.advance(&mut request, byte).unwrap();
    }
    let attempt = fixture.exchange.attempt(&fixture.streams.store);
    let validated = fixture.executors.fence(&attempt, &mut ingress, &mut request).unwrap();
    fn refused(view: RequestView<'_>, capability: &impl Capability) -> bool {
        matches!(
            RequestFence::default().end(view, capability),
            Err(codec::Error::Unsupported)
        )
    }
    let view = validated.view();
    assert!(refused(view, &codec::chat::request::ChatCapability));
    assert!(refused(view, &responses::request::ResponsesCapability));
    assert!(refused(view, &gemini::request::GeminiCapability));
    assert_eq!(upstream.requests(), 0);
}

/// A streamed compaction and a compaction without a token limit are refused at the fence with
/// nothing sent.
#[tokio::test]
async fn streamed_and_unlimited_compactions_are_refused() {
    let upstream = StreamUpstream::start([]).await;
    let fixture = Fixture::new(&upstream, SONNET);
    let streamed = br#"{"model":"m","max_output_tokens":4096,"stream":true,"input":"hi"}"#;
    let mut request = fixture.streams.request();
    let mut ingress = responses_ingress(&fixture, &mut request, Operation::Compact);
    let verdict = fixture.fence(&mut ingress, &mut request, streamed);
    assert!(matches!(verdict, Err(codec::Error::Unsupported)), "{verdict:?}");
    // A compaction needs `max_tokens` like a generation: a model without a configured or
    // registry limit is skipped (D2 Q-H), and this fixture installs no registry.
    let mut request = fixture.streams.request();
    let mut ingress = responses_ingress(&fixture, &mut request, Operation::Compact);
    let verdict = fixture.fence(&mut ingress, &mut request, br#"{"model":"m","input":"hi"}"#);
    assert!(matches!(verdict, Err(codec::Error::MaxTokensRequired)), "{verdict:?}");
    assert_eq!(upstream.requests(), 0);
}

/// A Responses client receives the block only inside the cooker envelope (unit D2-6a); until
/// that exists, its egress refuses the item instead of writing the native block.
#[tokio::test]
async fn responses_egress_refuses_the_block_until_the_envelope_exists() {
    let upstream = StreamUpstream::start([Reply::json(compacted())]).await;
    let fixture = Fixture::new(&upstream, SONNET);
    let attempt = fixture.exchange.attempt(&fixture.streams.store);
    let mut request = fixture.streams.request();
    let mut ingress = responses_ingress(&fixture, &mut request, Operation::Compact);
    let (started, id) = run(&fixture, &attempt, &mut ingress, &mut request, COMPACT)
        .await
        .unwrap();
    let mut started = started.unwrap();
    (&mut started.producer).await.unwrap().unwrap();
    let mut egress = support::responses_egress(&mut started.stream, Operation::Compact, BodyMode::Buffered).unwrap();
    let mut reader = fixture.streams.reader(id);
    let mut refused = false;
    while let Some(Read::Record(lease)) = reader.next().await {
        let mut output = Vec::new();
        let step = egress.advance(&mut started.stream, lease, |bytes| {
            output.extend_from_slice(&lease_bytes(&bytes));
            Ok(())
        });
        assert!(!String::from_utf8_lossy(&output).contains(SIGNATURE));
        if matches!(step, Err(codec::Error::Unsupported)) {
            refused = true;
            break;
        }
        step.unwrap();
    }
    assert!(refused);
}
