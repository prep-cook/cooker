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
        let mut egress =
            responses::egress::Egress::new(&mut m.writer, Operation::Generate, downstream, |_, _| {}).unwrap();
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

/// A real OpenAI `error` event (nested `error`, no `response`) ends the client with the
/// canonical upstream failure, as `response.failed` does: a Responses client receives a failed
/// response and a Chat client the error chunk, never a truncated stream.
#[test]
fn openai_error_event_fails_the_client_response() {
    let created = json!({"type":"response.created","response":{"id":"resp_1","object":"response","created_at":1,"model":"m","status":"in_progress","output":[]},"sequence_number":0});
    let error = json!({"type":"error","error":{"type":"insufficient_quota","code":"credit_balance_exhausted","message":"no credits","param":null},"sequence_number":1});
    let input = format!("event: response.created\ndata: {created}\n\nevent: error\ndata: {error}\n\n");
    let stream = String::from_utf8(to_client(input.as_bytes(), "responses", BodyMode::Streaming)).unwrap();
    let last = super::cross::client_events(stream.as_bytes()).pop().unwrap();
    assert_eq!(last["type"], "response.failed");
    assert_eq!(last["response"]["status"], "failed");
    assert_eq!(last["response"]["error"]["code"], "server_error");
    let body: Value = serde_json::from_slice(&to_client(input.as_bytes(), "responses", BodyMode::Buffered)).unwrap();
    assert_eq!(body["status"], "failed");
    assert_eq!(body["id"], "resp_1");
    let chat: Value = serde_json::from_slice(&to_client(input.as_bytes(), "chat", BodyMode::Buffered)).unwrap();
    assert_eq!(chat["error"]["status"], 502);
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
