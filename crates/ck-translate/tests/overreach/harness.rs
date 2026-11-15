//! Request translation driver and fixtures shared by the request fidelity targets.
use crate::semantic::Mapped;
use crate::{anthropic, codec, gemini, responses};
use codec::{
    Error, Operation, RequestEgress, RequestIngress,
    event::{Direction, Identity, LocalId},
};
use serde_json::{Value, json};

#[derive(Clone, Copy, Debug)]
pub(crate) enum P {
    Anthropic,
    Chat,
    Responses,
    Gemini,
}

pub(crate) fn identity() -> Identity {
    Identity {
        request_id: LocalId::new("request").unwrap(),
        stream_id: LocalId::new("stream").unwrap(),
        attempt: 1,
        direction: Direction::Request,
    }
}
pub(crate) fn bytes(lease: &ck_stream::ScratchLease) -> Vec<u8> {
    let mut out = Vec::new();
    while out.len() < lease.len() {
        out.extend_from_slice(lease.chunk(out.len()).unwrap());
    }
    out
}

/// The upstream body, or the ingress or fence refusal, of `input` fed in `chunks`; egress must
/// encode whatever the fence passed.
pub(crate) fn drive(
    m: &mut Mapped,
    mut ingress: impl RequestIngress,
    to: P,
    chunks: &mut dyn Iterator<Item = &[u8]>,
) -> Result<Value, Error> {
    for chunk in chunks {
        ingress.advance(&mut m.writer, chunk)?;
    }
    let output = match to {
        P::Anthropic => {
            let capability = anthropic::request::AnthropicCapability {
                default_max_tokens: Some(4096),
            };
            let ready = ingress.finish(&mut m.writer, &capability)?;
            anthropic::request::Egress {
                destination: "anthropic",
                upstream_stream: false,
                default_max_tokens: Some(4096),
            }
            .encode(&ready, &mut m.writer)
            .expect("a request the fence passed encodes")
        }
        P::Chat => {
            let ready = ingress.finish(&mut m.writer, &codec::chat::request::ChatCapability)?;
            codec::chat::request::Egress {
                destination: "chat",
                upstream_stream: false,
            }
            .encode(&ready, &mut m.writer)
            .expect("a request the fence passed encodes")
        }
        P::Responses => {
            let ready = ingress.finish(&mut m.writer, &responses::request::ResponsesCapability)?;
            responses::request::Egress {
                destination: "responses",
                upstream_stream: false,
            }
            .encode(&ready, &mut m.writer)
            .expect("a request the fence passed encodes")
        }
        P::Gemini => {
            let ready = ingress.finish(&mut m.writer, &gemini::request::GeminiCapability)?;
            gemini::request::Egress {
                destination: "gemini",
                upstream_stream: false,
            }
            .encode(&ready, &mut m.writer)
            .expect("a request the fence passed encodes")
        }
    };
    Ok(serde_json::from_slice(&bytes(&output)).unwrap())
}
pub(crate) fn once(from: P, to: P, chunks: &mut dyn Iterator<Item = &[u8]>) -> Result<Value, Error> {
    let mut m = Mapped::new();
    let (w, s, id, op, limit) = (&mut m.writer, &m.store, identity(), Operation::Generate, 1 << 20);
    match from {
        P::Anthropic => {
            let ingress = anthropic::request::Ingress::new(w, s, id, op, limit).unwrap();
            drive(&mut m, ingress, to, chunks)
        }
        P::Chat => {
            let ingress = codec::chat::request::Ingress::new(w, s, id, op, limit).unwrap();
            drive(&mut m, ingress, to, chunks)
        }
        P::Responses => {
            let ingress = responses::request::Ingress::new(w, s, id, op, limit).unwrap();
            drive(&mut m, ingress, to, chunks)
        }
        P::Gemini => {
            let ingress = gemini::request::Ingress::new(w, s, id, op, "gemini-x", limit).unwrap();
            drive(&mut m, ingress, to, chunks)
        }
    }
}
pub(crate) fn same(a: &Result<Value, Error>, b: &Result<Value, Error>) -> bool {
    match (a, b) {
        (Ok(a), Ok(b)) => a == b,
        (Err(a), Err(b)) => format!("{a:?}") == format!("{b:?}"),
        _ => false,
    }
}
/// Translates `input` from `from` to `to`; a 1-byte feed and a 7-byte feed must agree with
/// the whole body.
pub(crate) fn translate(from: P, to: P, input: &Value) -> Result<Value, Error> {
    let raw = serde_json::to_vec(input).unwrap();
    let whole = once(from, to, &mut std::iter::once(raw.as_slice()));
    for width in [1, 7] {
        let fed = once(from, to, &mut raw.chunks(width));
        assert!(
            same(&whole, &fed),
            "{from:?}->{to:?} width {width}: {whole:?} vs {fed:?}"
        );
    }
    whole
}
pub(crate) fn unsupported(result: Result<Value, Error>) {
    assert!(matches!(result, Err(Error::Unsupported)), "{result:?}");
}
/// The input items of a Responses body other than its plain messages.
pub(crate) fn items(out: &Value, kind: &str) -> Vec<Value> {
    out["input"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|i| i["type"] == kind)
        .cloned()
        .collect()
}

pub(crate) fn an_tool_result(content: Value) -> Value {
    json!({"max_tokens":100,"model":"claude-x","messages":[
        {"role":"user","content":"weather?"},
        {"role":"assistant","content":[{"type":"tool_use","id":"toolu_01B","name":"get_weather","input":{"city":"Oslo"}}]},
        {"role":"user","content":[content,{"type":"text","text":"try again"}]}],
        "tools":[{"name":"get_weather","description":"Weather","input_schema":{"type":"object","properties":{"city":{"type":"string"}}}}]})
}
pub(crate) fn gm_tool_result(response: Value) -> Value {
    json!({"contents":[
        {"role":"user","parts":[{"text":"weather?"}]},
        {"role":"model","parts":[{"functionCall":{"id":"fc_1","name":"get_weather","args":{"city":"Paris"}}}]},
        {"role":"user","parts":[{"functionResponse":{"id":"fc_1","name":"get_weather","response":response}}]}],
        "tools":[{"functionDeclarations":[{"name":"get_weather","parametersJsonSchema":{"type":"object"}}]}]})
}
pub(crate) fn gm_json(config: Value) -> Value {
    json!({"contents":[{"role":"user","parts":[{"text":"json"}]}],"generationConfig":config})
}
