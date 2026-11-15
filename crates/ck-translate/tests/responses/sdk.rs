use super::*;

/// Runs Responses upstream `input` through Responses ingress into `client`'s egress, as the
/// server does: records published before an ingress failure, including its error record,
/// still reach the egress.
pub(super) fn to_client(input: &[u8], client: &str, downstream: BodyMode) -> Vec<u8> {
    let mut m = Mapped::new();
    let mut ingress = responses::response::Ingress::new(
        &mut m.writer,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Streaming,
        1_000_000,
    )
    .unwrap();
    let mut reader = m.store.reader(m.writer.id(), 0).unwrap();
    let mut failed = false;
    for chunk in input.chunks(37) {
        if ingress.advance(&mut m.writer, chunk).is_err() {
            failed = true;
            break;
        }
    }
    if !failed {
        let _ = ingress.finish(&mut m.writer);
    }
    let mut output = Vec::new();
    let mut sink = |chunk: ck_stream::ScratchLease| {
        output.extend_from_slice(&bytes(&chunk));
        Ok(())
    };
    let mut records = Vec::new();
    while let std::task::Poll::Ready(Some(ck_stream::Read::Record(record))) = reader.try_next() {
        records.push(record);
    }
    if client == "chat" {
        let mut egress = codec::chat::egress::Egress::new(
            &mut m.writer,
            identity(Direction::Response),
            Operation::Generate,
            downstream,
            true,
            |_| {},
        )
        .unwrap();
        for record in records {
            egress.advance(&mut m.writer, record, &mut sink).unwrap();
        }
        egress.finish().unwrap();
    } else {
        let mut egress = crate::client_egress(&mut m.writer, Operation::Generate, downstream).unwrap();
        for record in records {
            egress.advance(&mut m.writer, record, &mut sink).unwrap();
        }
        egress.finish().unwrap();
    }
    output
}

#[test]
fn foreign_opaque_events_and_carriers_never_reach_responses_clients() {
    let input = "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"},\"finish_reason\":null}]}\n\nevent: vendor\ndata: {\"v\":1}\n\nevent: vendor\ndata: plain\n\ndata: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n";
    for downstream in [BodyMode::Streaming, BodyMode::Buffered] {
        let output = super::cross::chat_to_responses(input.as_bytes(), BodyMode::Streaming, downstream);
        let text = String::from_utf8(output).unwrap();
        assert!(!text.contains("x_ck"), "{text}");
        assert!(!text.contains("vendor") && !text.contains("plain"), "{text}");
    }
}

/// Members the official `Response` schema always carries, for a source that does not supply
/// them: spec defaults for tool settings, and 0 for unreported cache counts.
#[test]
fn foreign_responses_carry_the_members_the_schema_requires() {
    let usage = json!({"prompt_tokens":5,"completion_tokens":1,"prompt_tokens_details":{"cache_write_tokens":3}});
    let input = json!({"id":"c","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"x"},"finish_reason":"stop"}],"usage":usage});
    for downstream in [BodyMode::Buffered, BodyMode::Streaming] {
        let output =
            super::cross::chat_to_responses(&serde_json::to_vec(&input).unwrap(), BodyMode::Buffered, downstream);
        let mut responses = vec![super::cross::final_response(&output, downstream)];
        if downstream == BodyMode::Streaming {
            let events = super::cross::client_events(&output);
            responses.push(events[0]["response"].clone());
            let delta = events
                .iter()
                .find(|e| e["type"] == "response.output_text.delta")
                .unwrap();
            assert_eq!(delta["logprobs"], json!([]));
            let done = events
                .iter()
                .find(|e| e["type"] == "response.output_text.done")
                .unwrap();
            assert_eq!(done["logprobs"], json!([]));
        }
        for response in responses {
            assert_eq!(response["parallel_tool_calls"], true);
            assert_eq!(response["tool_choice"], "auto");
            assert_eq!(response["tools"], json!([]));
        }
        let out = super::cross::final_response(&output, downstream);
        assert_eq!(
            out["usage"]["input_tokens_details"],
            json!({"cache_write_tokens":3,"cached_tokens":0})
        );
        assert!(out["usage"].get("cache_write_tokens").is_none());
    }
}

#[test]
fn native_tool_settings_and_buffered_text_survive_the_defaults() {
    let body = json!({"id":"r","model":"m","created_at":1,"status":"completed","parallel_tool_calls":false,"tool_choice":"none","tools":[{"type":"function","name":"f","parameters":{}}],"output":[{"id":"i","type":"message","role":"assistant","content":[{"type":"output_text","text":"hi","annotations":[]}]}]});
    for downstream in [BodyMode::Buffered, BodyMode::Streaming] {
        let (output, _) = response(
            &serde_json::to_vec(&body).unwrap(),
            BodyMode::Buffered,
            downstream,
            Operation::Generate,
        );
        let out = super::cross::final_response(&output, downstream);
        for name in ["parallel_tool_calls", "tool_choice", "tools"] {
            assert_eq!(out[name], body[name]);
        }
        if downstream == BodyMode::Streaming {
            let events = super::cross::client_events(&output);
            let delta = events
                .iter()
                .find(|e| e["type"] == "response.output_text.delta")
                .unwrap();
            assert_eq!(delta["logprobs"], json!([]));
        }
    }
}

/// A real OpenAI `error` event (nested `error`, no `response`) is the upstream's own failure:
/// a Responses client receives the official `error` event, which the SDK raises, and a Chat
/// client an upstream failure. Both carry the gateway's sanitized values, never upstream detail.
#[test]
fn openai_error_event_reaches_clients_as_a_sanitized_upstream_failure() {
    let created = json!({"type":"response.created","response":{"id":"resp_1","object":"response","created_at":1,"model":"m","status":"in_progress","output":[]},"sequence_number":0});
    let error =
        json!({"type":"insufficient_quota","code":"credit_balance_exhausted","message":"no credits","param":null});
    let event = json!({"type":"error","error":error,"sequence_number":1});
    let failed = json!({"type":"response.failed","response":{"id":"resp_1","object":"response","created_at":1,"model":"m","status":"failed","output":[],"error":error},"sequence_number":2});
    let input = format!(
        "event: response.created\ndata: {created}\n\nevent: error\ndata: {event}\n\nevent: response.failed\ndata: {failed}\n\n"
    );
    let sanitized = json!({"type":"server_error","code":"server_error","message":"Response failed"});
    for (client, mode) in [
        ("responses", BodyMode::Streaming),
        ("responses", BodyMode::Buffered),
        ("chat", BodyMode::Streaming),
        ("chat", BodyMode::Buffered),
    ] {
        let text = String::from_utf8(to_client(input.as_bytes(), client, mode)).unwrap();
        for detail in [
            "credit_balance_exhausted",
            "insufficient_quota",
            "no credits",
            "invalid_input",
        ] {
            assert!(!text.contains(detail), "{client} {mode:?}: {text}");
        }
        match (client, mode) {
            ("responses", BodyMode::Streaming) => {
                assert!(text.contains("event: error\n"), "{text}");
                let events = super::cross::client_events(text.as_bytes());
                let official = events.iter().find(|e| e["type"] == "error").unwrap();
                assert_eq!(official["error"], sanitized);
                let last = events.last().unwrap();
                assert_eq!(last["type"], "response.failed");
                assert_eq!(last["response"]["error"]["code"], "server_error");
            }
            ("responses", _) => {
                let body: Value = serde_json::from_str(&text).unwrap();
                assert_eq!(
                    (&body["status"], &body["error"]["code"]),
                    (&json!("failed"), &json!("server_error"))
                );
            }
            _ => {
                let line = text.lines().rev().find(|l| l.contains("\"error\"")).unwrap();
                let chat: Value = serde_json::from_str(line.strip_prefix("data: ").unwrap_or(line)).unwrap();
                assert_eq!(
                    chat["error"],
                    json!({"code":"server_error","message":"Response failed","status":502})
                );
            }
        }
    }
}

/// Members under cooker's reserved `x_ck` prefix never reach a client, whatever their origin.
#[test]
fn reserved_members_never_reach_responses_clients() {
    let created = json!({"type":"response.created","response":{"id":"r","output":[],"x_ck_opaque":[{"upstream":"marker"}],"x_ck":{"source":"marker"}}});
    let note = json!({"type":"response.vendor_note","sequence_number":9,"x_ck":{"source":"marker"},"note":"n"});
    let completed = json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[]}});
    let input = format!("data: {created}\n\nevent: response.vendor_note\ndata: {note}\n\ndata: {completed}\n\n");
    for downstream in [BodyMode::Buffered, BodyMode::Streaming] {
        let (output, _) = response(input.as_bytes(), BodyMode::Streaming, downstream, Operation::Generate);
        let text = String::from_utf8(output).unwrap();
        assert!(!text.contains("x_ck"), "{text}");
        if downstream == BodyMode::Streaming {
            let events = super::cross::client_events(text.as_bytes());
            let types: Vec<_> = events.iter().map(|e| e["type"].as_str().unwrap()).collect();
            assert_eq!(
                types,
                ["response.created", "response.vendor_note", "response.completed"]
            );
            for (at, event) in events.iter().enumerate() {
                assert_eq!(event["sequence_number"], at);
            }
            assert_eq!(events[1]["note"], "n");
        }
    }
}

/// `usage` as a Responses client receives it: a top-level `cache_write_tokens` moves into
/// `input_tokens_details`, and an unreported cache count is 0.
pub(super) fn official_usage(usage: &Value) -> Value {
    let mut usage = usage.clone();
    let Some(object) = usage.as_object_mut() else {
        return usage;
    };
    let write = object.remove("cache_write_tokens").filter(|v| !v.is_null());
    let details = object.entry("input_tokens_details").or_insert_with(|| json!({}));
    if !details.is_object() {
        *details = json!({});
    }
    if let Some(write) = write {
        details["cache_write_tokens"] = write;
    }
    for name in ["cached_tokens", "cache_write_tokens"] {
        if details[name].is_null() {
            details[name] = json!(0);
        }
    }
    usage
}

/// Native usage keeps its counts and takes the official placement and missing-value rule.
#[test]
fn native_usage_carries_both_cache_counts_in_the_details() {
    for (usage, details) in [
        (
            json!({"input_tokens":4,"output_tokens":1,"total_tokens":5,"input_tokens_details":{"cached_tokens":2}}),
            json!({"cached_tokens":2,"cache_write_tokens":0}),
        ),
        (
            json!({"input_tokens":4,"output_tokens":1,"total_tokens":5,"cache_write_tokens":3,"input_tokens_details":{"cached_tokens":2}}),
            json!({"cached_tokens":2,"cache_write_tokens":3}),
        ),
    ] {
        let body = json!({"id":"r","object":"response","status":"completed","output":[],"usage":usage});
        let stream = format!("data: {}\n\n", json!({"type":"response.completed","response":body}));
        for (input, mode) in [
            (serde_json::to_vec(&body).unwrap(), BodyMode::Buffered),
            (stream.into_bytes(), BodyMode::Streaming),
        ] {
            for downstream in [BodyMode::Buffered, BodyMode::Streaming] {
                let (output, _) = response(&input, mode, downstream, Operation::Generate);
                let out = super::cross::final_response(&output, downstream);
                assert_eq!(out["usage"]["input_tokens_details"], details, "{out}");
                assert!(out["usage"].get("cache_write_tokens").is_none(), "{out}");
                assert_eq!(out["usage"]["input_tokens"], 4);
            }
        }
    }
}

/// Streamed audio reaches a buffered client at the native `audio` member; a final response that
/// carries the audio keeps it as sent.
#[test]
fn streamed_audio_reaches_buffered_clients_natively() {
    let audio = json!({"data":"AAEC","transcript":"hello","mime_type":"audio/wav"});
    for final_audio in [None, Some(audio.clone())] {
        let mut completed = json!({"id":"r","status":"completed","output":[]});
        if let Some(audio) = &final_audio {
            completed["audio"] = audio.clone();
        }
        let input = wire(&[
            json!({"type":"response.created","response":{"id":"r"}}),
            json!({"type":"response.audio.delta","delta":"AAEC"}),
            json!({"type":"response.audio.done"}),
            json!({"type":"response.audio.transcript.delta","delta":"hello"}),
            json!({"type":"response.audio.transcript.done"}),
            json!({"type":"response.completed","response":completed}),
        ]);
        let (output, _) = response(
            input.as_bytes(),
            BodyMode::Streaming,
            BodyMode::Buffered,
            Operation::Generate,
        );
        let out: Value = serde_json::from_slice(&output).unwrap();
        let expected = final_audio.unwrap_or_else(|| json!({"data":"AAEC","transcript":"hello"}));
        assert_eq!(out["audio"], expected, "{out}");
    }
}

/// Input after the upstream `error` event is ignored however the network splits it: in the same
/// `advance` call as the event or in a later one.
#[test]
fn input_after_an_upstream_error_is_ignored_at_every_split() {
    let mut input = b"event: error\ndata: {\"type\":\"error\",\"error\":{\"code\":\"credit_balance_exhausted\",\"message\":\"billing https://vendor.invalid/private\",\"type\":\"insufficient_quota\"}}\n\n".to_vec();
    input.push(0xff);
    for split in 0..=input.len() {
        let mut m = Mapped::new();
        let mut ingress = responses::response::Ingress::new(
            &mut m.writer,
            identity(Direction::Response),
            Operation::Generate,
            BodyMode::Streaming,
            1024 * 1024,
        )
        .unwrap();
        let (head, tail) = input.split_at(split);
        let result = ingress
            .advance(&mut m.writer, head)
            .and_then(|()| ingress.advance(&mut m.writer, tail))
            .and_then(|()| ingress.finish(&mut m.writer));
        assert!(result.is_ok(), "split {split}: {result:?}");
        let last = events(&m).pop().unwrap();
        assert_eq!(
            (&last["code"], &last["scope"]),
            (&json!("server_error"), &json!("upstream")),
            "split {split}"
        );
    }
}
