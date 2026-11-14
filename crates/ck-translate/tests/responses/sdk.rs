use super::*;

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
