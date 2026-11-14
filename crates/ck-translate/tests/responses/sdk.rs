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
