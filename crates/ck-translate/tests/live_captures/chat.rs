//! Chat client regressions from the live captures; `run` and the fixtures are the parent's.

use super::*;

#[test]
fn chat_sends_the_role_once_in_the_first_chunk_of_a_choice() {
    for (source, name) in [
        (Source::Responses, "relay-medium-resp-reason1.sse"),
        (Source::Chat, "relay-chat-tools1.sse"),
        (Source::Anthropic, "a2_tools_t1_stream.sse"),
    ] {
        let input = fixture(name);
        let streamed = run(source, BodyMode::Streaming, &input, Client::Chat, BodyMode::Streaming);
        let deltas: Vec<Value> = events(&streamed)
            .into_iter()
            .filter_map(|event| event["choices"].get(0).map(|choice| choice["delta"].clone()))
            .collect();
        let roles: Vec<usize> = (0..deltas.len()).filter(|&i| deltas[i].get("role").is_some()).collect();
        assert_eq!(roles, [0], "{name}: {deltas:?}");
        assert_eq!(deltas[0]["role"], "assistant", "{name}");
        let buffered = run(source, BodyMode::Streaming, &input, Client::Chat, BodyMode::Buffered);
        let buffered: Value = serde_json::from_slice(&buffered).unwrap();
        assert_eq!(
            buffered["choices"][0]["message"]["role"], "assistant",
            "{name}: {buffered}"
        );
    }
}

#[test]
fn chat_tool_call_from_responses_takes_the_call_id() {
    let input = fixture("relay-resp-func1.sse");
    let call = "call_1a3425a5046f46e085ecc0bb2612582b";
    let buffered = run(
        Source::Responses,
        BodyMode::Streaming,
        &input,
        Client::Chat,
        BodyMode::Buffered,
    );
    let buffered: Value = serde_json::from_slice(&buffered).unwrap();
    assert_eq!(
        buffered["choices"][0]["message"]["tool_calls"][0]["id"], call,
        "{buffered}"
    );
    let streamed = run(
        Source::Responses,
        BodyMode::Streaming,
        &input,
        Client::Chat,
        BodyMode::Streaming,
    );
    let ids: Vec<Value> = events(&streamed)
        .iter()
        .filter_map(|event| event["choices"][0]["delta"]["tool_calls"][0].get("id").cloned())
        .collect();
    assert_eq!(ids, [call]);
}

#[test]
fn chat_stream_from_another_protocol_carries_only_chat_chunks() {
    // Native events and SSE fields of the source protocol stay out of the Chat stream.
    let anthropic = String::from_utf8(fixture("a1b_chat_stream.sse"))
        .unwrap()
        .replace("\ndata: ", "\n: note\nid: 7\ndata: ");
    for (source, input) in [
        (Source::Responses, fixture("relay-medium-resp-reason1.sse")),
        (Source::Anthropic, anthropic.into_bytes()),
    ] {
        let streamed = run(source, BodyMode::Streaming, &input, Client::Chat, BodyMode::Streaming);
        for record in std::str::from_utf8(&streamed).unwrap().split_terminator("\n\n") {
            let chunk = record
                .strip_prefix("data: ")
                .and_then(|data| serde_json::from_str::<Value>(data).ok());
            assert!(
                record == "data: [DONE]" || chunk.is_some_and(|chunk| chunk["object"] == "chat.completion.chunk"),
                "{record}"
            );
        }
    }
}
