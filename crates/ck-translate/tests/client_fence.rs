//! Contract §4, G8: the client fence closes against `ClientCapability`, which no request egress
//! encodes for, so a request reaches an upstream only after a re-fence for the attempt's own
//! destination.

#[path = "semantic/mod.rs"]
mod semantic;

use ck_translate::codec::{
    CLIENT, Capability, ClientCapability, Error, Operation, RequestEgress, RequestFence, RequestIngress,
    chat::request as chat,
    event::{Direction, Identity, LocalId},
};
use ck_translate::{anthropic, gemini, responses};
use semantic::Mapped;
use serde_json::{Value, json};

fn identity() -> Identity {
    Identity {
        request_id: LocalId::new("request").unwrap(),
        stream_id: LocalId::new("stream").unwrap(),
        attempt: 1,
        direction: Direction::Request,
    }
}

fn chat_egress(destination: &'static str) -> chat::Egress {
    chat::Egress {
        destination,
        upstream_stream: false,
    }
}
fn responses_egress(destination: &'static str) -> responses::request::Egress {
    responses::request::Egress {
        destination,
        upstream_stream: false,
    }
}
fn anthropic_egress(destination: &'static str) -> anthropic::request::Egress {
    anthropic::request::Egress {
        destination,
        upstream_stream: false,
        default_max_tokens: Some(16),
    }
}
fn gemini_egress(destination: &'static str) -> gemini::request::Egress {
    gemini::request::Egress {
        destination,
        upstream_stream: false,
    }
}

/// Every request egress, under its own destination key and under the client key.
fn egresses() -> Vec<(&'static str, Box<dyn RequestEgress>)> {
    let mut all: Vec<(&'static str, Box<dyn RequestEgress>)> = Vec::new();
    for client in [false, true] {
        let key = |own| if client { CLIENT } else { own };
        all.push(("chat", Box::new(chat_egress(key("chat")))));
        all.push(("responses", Box::new(responses_egress(key("responses")))));
        all.push(("anthropic", Box::new(anthropic_egress(key("anthropic")))));
        all.push(("gemini", Box::new(gemini_egress(key("gemini")))));
    }
    all
}

/// Closes the client fence over `body`, checks that no request egress encodes the result, then
/// re-fences the client view for `own` and encodes it with `native`.
fn fence_then_refence(
    m: &mut Mapped,
    mut ingress: impl RequestIngress,
    body: &Value,
    own: &impl Capability,
    native: &dyn RequestEgress,
) {
    ingress
        .advance(&mut m.writer, &serde_json::to_vec(body).unwrap())
        .unwrap();
    let client = ingress.finish(&mut m.writer, &ClientCapability).unwrap();
    assert_eq!(client.destination(), CLIENT);
    for (name, egress) in egresses() {
        let refused = egress.encode(&client, &mut m.writer).err();
        assert!(matches!(refused, Some(Error::Destination)), "{name}: {refused:?}");
    }
    let published = String::from_utf8(m.output(&m.writer)).unwrap();
    assert!(published.contains("event: x_ck.request.end\n"), "{published}");
    let attempt = RequestFence::default().end(client.view(), own).unwrap();
    assert!(native.encode(&attempt, &mut m.writer).is_ok());
}

#[test]
fn no_request_egress_encodes_a_client_fenced_request() {
    let generate = Operation::Generate;
    let mut m = Mapped::new();
    let ingress = chat::Ingress::new(&mut m.writer, &m.store, identity(), generate, 1 << 20).unwrap();
    let body = json!({"model":"m","messages":[{"role":"user","content":"hi"}]});
    fence_then_refence(&mut m, ingress, &body, &chat::ChatCapability, &chat_egress("chat"));

    let mut m = Mapped::new();
    let ingress = responses::request::Ingress::new(&mut m.writer, &m.store, identity(), generate, 1 << 20).unwrap();
    let body = json!({"model":"m","input":"hi"});
    let capability = responses::request::ResponsesCapability;
    fence_then_refence(&mut m, ingress, &body, &capability, &responses_egress("responses"));

    let mut m = Mapped::new();
    let ingress = anthropic::request::Ingress::new(&mut m.writer, &m.store, identity(), generate, 1 << 20).unwrap();
    let body = json!({"model":"m","max_tokens":16,"messages":[{"role":"user","content":"hi"}]});
    let capability = anthropic::request::AnthropicCapability::default();
    fence_then_refence(&mut m, ingress, &body, &capability, &anthropic_egress("anthropic"));

    let mut m = Mapped::new();
    let ingress = gemini::request::Ingress::new(&mut m.writer, &m.store, identity(), generate, "m", 1 << 20).unwrap();
    let body = json!({"contents":[{"role":"user","parts":[{"text":"hi"}]}]});
    let capability = gemini::request::GeminiCapability;
    fence_then_refence(&mut m, ingress, &body, &capability, &gemini_egress("gemini"));
}

#[test]
fn reusable_prompts_preserve_model_omission_and_overrides() {
    for (body, accepted) in [
        (
            json!({"prompt":{"id":"pmpt_test","version":"2","variables":{"name":"Ada"}}}),
            true,
        ),
        (json!({"model":"override","prompt":{"id":"pmpt_test"}}), true),
        (json!({"input":"hi"}), false),
        (json!({"prompt":null}), false),
        (json!({"prompt":{}}), false),
        (json!({"model":7,"prompt":{"id":"pmpt_test"}}), false),
    ] {
        let mut m = Mapped::new();
        let mut ingress =
            responses::request::Ingress::new(&mut m.writer, &m.store, identity(), Operation::Generate, 1 << 20)
                .unwrap();
        ingress
            .advance(&mut m.writer, &serde_json::to_vec(&body).unwrap())
            .unwrap();
        let result = ingress.finish(&mut m.writer, &ClientCapability);
        assert_eq!(result.is_ok(), accepted, "{body}");
        if let Ok(client) = result {
            for capability in [
                &responses::request::ResponsesCapability as &dyn Capability,
                &responses::request::CodexCapability,
            ] {
                capability.validate(client.view()).unwrap();
            }
            let attempt = RequestFence::default()
                .end(client.view(), &responses::request::CodexCapability)
                .unwrap();
            let upload = responses_egress("codex").encode(&attempt, &mut m.writer).unwrap();
            let mut bytes = Vec::new();
            while bytes.len() < upload.len() {
                bytes.extend_from_slice(upload.chunk(bytes.len()).unwrap());
            }
            let encoded: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(encoded.get("model"), body.get("model"));
            assert_eq!(encoded["prompt"], body["prompt"]);
        }
    }
}
