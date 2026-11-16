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

/// Fences an Anthropic client's `body` and encodes it for Anthropic; returns whether the request
/// carries a compaction block, as the Claude executor reads it for the beta, and the upload.
fn fenced(body: &str) -> codec::Result<(bool, Vec<u8>)> {
    let mut mapped = Mapped::new();
    let mut ingress = anthropic::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        1024 * 1024,
    )?;
    ingress.advance(&mut mapped.writer, body.as_bytes())?;
    let ready = ingress.finish(&mut mapped.writer, &anthropic::request::AnthropicCapability::default())?;
    let carries = anthropic::compaction::requested(ready.view())?;
    let egress = anthropic::request::Egress {
        destination: "anthropic",
        upstream_stream: true,
        default_max_tokens: None,
    };
    let output = egress.encode(&ready, &mut mapped.writer)?;
    Ok((carries, bytes(&output)))
}

/// Every placement and shape of a replayed block that Anthropic's on-demand compaction page
/// (https://platform.claude.com/docs/en/build-with-claude/compaction-on-demand) accepts: "Put
/// the block first in `messages`, either as an `assistant` message of its own or as the first
/// content block of the first message, whether that is a `user` or `assistant` message";
/// "`cache_control` on the block places a breakpoint after the summary"; "Send exactly one
/// `compaction` block ..., the newest". A JSON-escaped `type` is the same block. Each is a
/// block to the one test that both the placement check and the executor's beta use, and goes
/// upstream first, unchanged, with the turns before it left out (Settled 71). A block anywhere
/// else is refused, escaped or not.
#[test]
fn a_compaction_block_is_accepted_only_where_anthropic_reads_it_and_replays_first() {
    let native: Value = serde_json::from_str(&block()).unwrap();
    let mut cached = native.clone();
    cached["cache_control"] = json!({"type": "ephemeral"});
    let newer = json!({"type": "compaction", "content": "Newer.", "signature": "c2ln"});
    let call = json!({"type": "tool_use", "id": "toolu_1", "name": "f", "input": {}});
    let result =
        json!({"role": "user", "content": [{"type": "tool_result", "tool_use_id": "toolu_1", "content": "ok"}]});
    let next = json!({"role": "user", "content": "And now?"});
    let text = json!({"type": "text", "text": "x"});
    let own = json!({"role": "assistant", "content": [&native]});
    let request = |body: Value| {
        let mut body = body;
        body["model"] = json!("m");
        body["max_tokens"] = json!(64);
        body.to_string()
    };
    let escape = |body: Value| request(body).replacen(r#""type":"compaction""#, r#""type":"\u0063ompaction""#, 1);
    let user_first = json!({"role": "user", "content": [&native, {"type": "text", "text": "And now?"}]});
    let assistant_first = json!({"role": "assistant", "content": [&native, &text]});
    let called = json!({"role": "assistant", "content": [&native, &call]});
    let earlier = [
        json!({"role": "user", "content": "Explain lifetimes."}),
        json!({"role": "assistant", "content": "They bound references."}),
    ];
    let rows: Vec<(&str, String, Option<Value>)> = vec![
        (
            "an assistant message of its own",
            request(json!({"messages": [&own, &next]})),
            Some(own.clone()),
        ),
        (
            "first in the first message, a user one",
            request(json!({"messages": [&user_first]})),
            Some(user_first.clone()),
        ),
        (
            "first in the first message, an assistant one",
            request(json!({"messages": [&assistant_first, &next]})),
            Some(assistant_first.clone()),
        ),
        (
            "followed by a tool call",
            request(json!({"messages": [&called, &result]})),
            Some(called.clone()),
        ),
        (
            "with cache_control",
            request(json!({"messages": [{"role": "assistant", "content": [&cached]}, &next]})),
            Some(json!({"role": "assistant", "content": [&cached]})),
        ),
        (
            "with an escaped type",
            escape(json!({"messages": [&earlier[0], &own, &next]})),
            Some(own.clone()),
        ),
        (
            "behind the turns it summarized",
            request(json!({"system": "Be brief.", "messages": [&earlier[0], &earlier[1], &own, &next]})),
            Some(own.clone()),
        ),
        (
            "an older block before the newest",
            request(json!({"messages": [&own, &earlier[0], {"role": "assistant", "content": [&newer]}, &next]})),
            Some(json!({"role": "assistant", "content": [&newer]})),
        ),
        (
            "behind another block",
            request(json!({"messages": [{"role": "assistant", "content": [&text, &native]}, &next]})),
            None,
        ),
        (
            "behind another block, escaped",
            escape(json!({"messages": [{"role": "assistant", "content": [&text, &native]}, &next]})),
            None,
        ),
        (
            "behind a tool call",
            request(json!({"messages": [{"role": "assistant", "content": [&call, &native]}, &result]})),
            None,
        ),
        (
            "in the system prompt",
            request(json!({"system": [&native], "messages": [&next]})),
            None,
        ),
    ];
    for (row, body, first) in rows {
        let outcome = fenced(&body);
        let Some(first) = first else {
            assert!(matches!(outcome, Err(codec::Error::Unsupported)), "{row}: {body}");
            continue;
        };
        let (carries, upload) = outcome.unwrap_or_else(|e| panic!("{row}: {e:?}"));
        assert!(carries, "{row}");
        let sent: Value = serde_json::from_slice(&upload).unwrap();
        assert_eq!(sent["messages"][0], first, "{row}");
        assert!(
            sent.get("system").is_none() || sent["system"] == "Be brief.",
            "{row}: {sent}"
        );
    }
    // The block's own bytes, escapes included, are what goes upstream.
    let body = json!({"model": "m", "max_tokens": 64, "messages": [&own, &next]}).to_string();
    let body = body.replace(&serde_json::to_string(&native).unwrap(), &block());
    let (carries, upload) = fenced(&body).unwrap();
    assert!(carries);
    assert!(String::from_utf8(upload).unwrap().contains(&block()));
    // A request without a block carries none.
    let plain = json!({"model": "m", "max_tokens": 64, "messages": [&next]}).to_string();
    assert!(!fenced(&plain).unwrap().0);
}
