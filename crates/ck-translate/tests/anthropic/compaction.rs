//! Anthropic native compaction (D2 plan §2.7): the response ingress publishes the one signed
//! block unchanged, usage comes from `iterations`, and request egress replays the block first.
use super::*;

/// Escapes and non-ASCII text, so a re-encoded block would differ from the upstream's bytes.
const SUMMARY: &str = r#"Asked about \"lifetimes\".\nCafé — done."#;
const SIGNATURE: &str = "EqQBCkgIBxABGAIqQGNvbXBhY3Rpb24tc2lnbmF0dXJlLWZpeHR1cmU=";

fn block() -> String {
    format!(r#"{{"type":"compaction","content":"{SUMMARY}","signature":"{SIGNATURE}"}}"#)
}

/// Anthropic's answer to an on-demand compaction, shaped like the live check: the top-level
/// counts are 0 and the billed tokens are in `iterations`.
fn compacted(content: &str, stop_reason: &str) -> String {
    format!(
        r#"{{"id":"msg_c","type":"message","role":"assistant","model":"claude-haiku-5-5","content":[{content}],"stop_reason":"{stop_reason}","stop_sequence":null,"usage":{{"input_tokens":0,"output_tokens":0,"iterations":[{{"type":"compaction","input_tokens":119,"output_tokens":217}}]}}}}"#
    )
}

/// Feeds `body` to a buffered compact ingress one byte at a time; returns the outcome and the
/// raw canonical records.
fn compact(body: &str) -> (codec::Result<()>, Vec<u8>) {
    let mut mapped = Mapped::new();
    let mut ingress = anthropic::response::Ingress::new(
        &mut mapped.writer,
        identity(Direction::Response),
        Operation::Compact,
        BodyMode::Buffered,
        1024 * 1024,
    )
    .unwrap();
    let mut outcome = Ok(());
    for byte in body.as_bytes().chunks(1) {
        outcome = outcome.and_then(|()| ingress.advance(&mut mapped.writer, byte));
    }
    let outcome = outcome.and_then(|()| ingress.finish(&mut mapped.writer));
    (outcome, mapped.output(&mapped.writer))
}

#[test]
fn a_compaction_publishes_its_block_unchanged_and_bills_its_iterations() {
    let (outcome, raw) = compact(&compacted(&block(), "compaction"));
    outcome.unwrap();
    // The block's own bytes, escapes included, are what the canonical item holds.
    let text = String::from_utf8(raw.clone()).unwrap();
    assert!(text.contains(&format!(r#""value":{}"#, block())), "{text}");
    let events = events(&raw);
    let items: Vec<_> = events.iter().filter_map(|e| e.get("item")).collect();
    let native: Value = serde_json::from_str(&block()).unwrap();
    assert_eq!(
        items,
        [&json!({"type": "opaque", "protocol": "anthropic", "value": native})]
    );
    let usage = events.iter().rev().find_map(|e| e.get("usage")).unwrap();
    assert_eq!(usage["final"], true);
    assert_eq!(
        (&usage["input_tokens"], &usage["output_tokens"], &usage["total_tokens"]),
        (&json!(119), &json!(217), &json!(336))
    );
}

#[test]
fn anything_but_one_compaction_block_is_invalid_upstream_data() {
    let text = r#"{"type":"text","text":"no"}"#;
    for body in [
        compacted(&block(), "end_turn"),
        compacted(text, "compaction"),
        compacted(&format!("{},{}", block(), block()), "compaction"),
        compacted("", "compaction"),
    ] {
        let (outcome, raw) = compact(&body);
        assert!(outcome.is_err(), "{body}");
        assert!(events(&raw).iter().all(|e| e.get("item").is_none()), "{body}");
    }
    let mut mapped = Mapped::new();
    let streaming = anthropic::response::Ingress::new(
        &mut mapped.writer,
        identity(Direction::Response),
        Operation::Compact,
        BodyMode::Streaming,
        1024,
    );
    assert!(matches!(streaming, Err(codec::Error::Unsupported)));
}

/// With server-side compaction the top level leaves the compaction iteration out, so the bill
/// is the sum of every iteration; without `iterations` it is the top level.
#[test]
fn every_iteration_is_billed() {
    let message = |usage: &str| {
        format!(
            r#"{{"id":"msg_1","type":"message","role":"assistant","model":"m","content":[{{"type":"text","text":"hi"}}],"stop_reason":"end_turn","stop_sequence":null,"usage":{usage}}}"#
        )
    };
    let billed = |usage: &str| {
        let body = message(usage);
        let (_, canonical) = response(
            body.as_bytes(),
            BodyMode::Buffered,
            BodyMode::Buffered,
            Operation::Generate,
            5,
        );
        let usage = canonical.iter().rev().find_map(|e| e.get("usage")).unwrap().clone();
        (usage["input_tokens"].clone(), usage["output_tokens"].clone())
    };
    let iterations = r#"{"input_tokens":30,"output_tokens":5,"iterations":[{"type":"compaction","input_tokens":119,"output_tokens":217},{"type":"message","input_tokens":30,"output_tokens":5}]}"#;
    assert_eq!(billed(iterations), (json!(149), json!(222)));
    assert_eq!(
        billed(r#"{"input_tokens":30,"output_tokens":5}"#),
        (json!(30), json!(5))
    );
    assert_eq!(
        billed(r#"{"input_tokens":30,"output_tokens":5,"iterations":[]}"#),
        (json!(30), json!(5))
    );
}

#[test]
fn a_replayed_block_goes_first_and_unchanged_and_earlier_turns_stay_behind() {
    let body = format!(
        r#"{{"model":"m","max_tokens":64,"system":"Be brief.","messages":[{{"role":"user","content":"Explain lifetimes."}},{{"role":"assistant","content":"They bound references."}},{{"role":"assistant","content":[{}]}},{{"role":"user","content":"And now?"}}]}}"#,
        block()
    );
    let (sent, _) = request(body.as_bytes(), Operation::Generate);
    let native: Value = serde_json::from_str(&block()).unwrap();
    assert_eq!(
        sent["messages"],
        json!([{"role": "assistant", "content": [native]}, {"role": "user", "content": "And now?"}])
    );
    assert_eq!(sent["system"], "Be brief.");
    // Anthropic accepts a block only at the head of a conversation message: one behind another
    // block or a tool call is misplaced, and one in the system prompt would be sent as system
    // text. Tool calls may follow the block in its message.
    let call = json!({"type": "tool_use", "id": "toolu_1", "name": "f", "input": {}});
    let result =
        json!({"role": "user", "content": [{"type": "tool_result", "tool_use_id": "toolu_1", "content": "ok"}]});
    let go = json!({"role": "user", "content": "go"});
    let text = json!({"type": "text", "text": "x"});
    for (body, accepted) in [
        (
            json!({"messages": [{"role": "assistant", "content": [&text, &native]}, &go]}),
            false,
        ),
        (json!({"system": [&native], "messages": [&go]}), false),
        (
            json!({"messages": [{"role": "assistant", "content": [&call, &native]}, &result]}),
            false,
        ),
        (
            json!({"messages": [{"role": "assistant", "content": [&native, &call]}, &result]}),
            true,
        ),
    ] {
        let mut body = body;
        body["model"] = json!("m");
        body["max_tokens"] = json!(64);
        let body = body.to_string();
        if accepted {
            let (sent, _) = request(body.as_bytes(), Operation::Generate);
            assert_eq!(sent["messages"][0]["content"], json!([&native, &call]), "{sent}");
            continue;
        }
        let mut mapped = Mapped::new();
        let mut ingress = anthropic::request::Ingress::new(
            &mut mapped.writer,
            &mapped.store,
            identity(Direction::Request),
            Operation::Generate,
            1024 * 1024,
        )
        .unwrap();
        let fenced = ingress
            .advance(&mut mapped.writer, body.as_bytes())
            .and_then(|()| ingress.finish(&mut mapped.writer, &anthropic::request::AnthropicCapability::default()));
        assert!(matches!(fenced, Err(codec::Error::Unsupported)), "{body}");
    }
}
