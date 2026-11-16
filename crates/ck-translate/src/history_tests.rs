//! History projection and per-attempt admission (D2-PLAN §2.3, §2.4, §2.7; Q-C).
use crate::codec::{
    BodyMode, Capability, Operation, RequestEgress, RequestFence, RequestIngress, RequestView, ResponseIngress,
    document::Document,
    event::{Direction, Identity, LocalId},
    media::Registry,
    provenance::{Admission, Producer, admit, drop_foreign},
};
use crate::responses::history::{Stored, expand, project, truncate};
use ck_stream::{Config, ManualClock, Store, Writer};
use serde_json::{Value, json};
use std::sync::Arc;

struct Rig {
    store: Store,
    directory: std::path::PathBuf,
}

impl Rig {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!("ck-history-{}", uuid::Uuid::new_v4()));
        let mut config = Config::new(directory.clone());
        config.pool_bytes = Some(256 * ck_stream::FRAME_BYTES);
        config.max_event_bytes = 16 * ck_stream::FRAME_BYTES;
        config.max_metadata_bytes = 4 * 1024 * 1024;
        config.header_slots = 16;
        config.workers = 1;
        let store = Store::with_clock(config, Arc::new(ManualClock::default())).unwrap();
        Self { store, directory }
    }

    /// The canonical response a source's buffered `body` decodes to, fed in `width`-byte chunks.
    fn stored(&self, source: Source, width: usize) -> Writer {
        let mut w = self.store.create().unwrap();
        let (id, op, mode, limit) = (
            identity(Direction::Response),
            source.operation(),
            BodyMode::Buffered,
            1 << 20,
        );
        let body = source.body();
        let feed = |ingress: &mut dyn ResponseIngress, w: &mut Writer| {
            for chunk in body.chunks(width) {
                ingress.advance(w, chunk).unwrap();
            }
            ingress.finish(w).unwrap();
        };
        match source {
            Source::Anthropic | Source::AnthropicText => {
                let mut i = crate::anthropic::response::Ingress::new(&mut w, id, op, mode, limit).unwrap();
                feed(&mut i, &mut w);
            }
            Source::Responses
            | Source::Compaction
            | Source::SignedCompaction
            | Source::AntigravityClaude
            | Source::AntigravityTwoCalls => {
                let mut i = crate::responses::response::Ingress::new(&mut w, id, op, mode, limit).unwrap();
                feed(&mut i, &mut w);
            }
            Source::Gemini | Source::GeminiText => {
                let mut i = crate::gemini::response::Ingress::new(&mut w, id, op, mode, limit).unwrap();
                feed(&mut i, &mut w);
            }
            Source::Chat => {
                let mut i =
                    crate::codec::chat::response::Ingress::new(&mut w, &self.store, id, op, mode, limit).unwrap();
                feed(&mut i, &mut w);
            }
        }
        w
    }

    /// A request stream holding a Responses client's `body` as request records.
    fn request(&self, body: Value) -> Writer {
        let mut owner = self.store.create().unwrap();
        let mut ingress = crate::responses::request::Ingress::new(
            &mut owner,
            &self.store,
            identity(Direction::Request),
            Operation::Generate,
            1 << 20,
        )
        .unwrap();
        ingress
            .advance(&mut owner, &serde_json::to_vec(&body).unwrap())
            .unwrap();
        ingress.finish(&mut owner, &crate::codec::ClientCapability).unwrap();
        drop(ingress);
        owner
    }

    /// The request stream `request` holds, rebuilt as the server does.
    fn read(&self, request: &mut Writer) -> (Document, Registry) {
        let mut records = self.store.reader(request.id(), 0).unwrap();
        crate::codec::chat::request::read_request(request, &mut records).unwrap()
    }

    /// Expands the client request `body` over the stored turn `(request, response)` that
    /// `producer` answered: the new request stream and its document.
    fn expand(
        &self,
        body: Value,
        request: &Writer,
        response: &Writer,
        producer: Producer<'_>,
    ) -> (Writer, Document, Registry) {
        let mut client = self.request(body);
        let (doc, media) = self.read(&mut client);
        let view = RequestView {
            document: &doc,
            media: &media,
            operation: Operation::Generate,
        };
        let mut target = self.store.create().unwrap();
        let (mut request, mut response) = (request.allocator(), response.allocator());
        let mut request_records = self.store.reader(request.id(), 0).unwrap();
        let mut response_records = self.store.reader(response.id(), 0).unwrap();
        let stored = Stored {
            request: (&mut request, &mut request_records),
            response: (&mut response, &mut response_records),
            producer,
        };
        expand(&mut target, identity(Direction::Request), view, stored).unwrap();
        let (doc, media) = self.read(&mut target);
        (target, doc, media)
    }

    /// A Responses client's canonical request, rebuilt from its request records as the server
    /// does, with each of `turns` projected after it.
    fn history(&self, input: Value, turns: &[(Source, Producer<'_>)], width: usize) -> (Writer, Document, Registry) {
        let mut owner = self.store.create().unwrap();
        // Every client forges the member the server sets to prove an item; no row may see it.
        let body = serde_json::to_vec(&json!({"model": "m", "input": input, "_ck_proven": 0})).unwrap();
        let mut ingress = crate::responses::request::Ingress::new(
            &mut owner,
            &self.store,
            identity(Direction::Request),
            Operation::Generate,
            1 << 20,
        )
        .unwrap();
        ingress.advance(&mut owner, &body).unwrap();
        ingress.finish(&mut owner, &crate::codec::ClientCapability).unwrap();
        drop(ingress);
        let mut records = self.store.reader(owner.id(), 0).unwrap();
        let (mut doc, media) = crate::codec::chat::request::read_request(&mut owner, &mut records).unwrap();
        for &(source, producer) in turns {
            let stored = self.stored(source, width);
            let mut allocator = stored.allocator();
            let mut reader = self.store.reader(stored.id(), 0).unwrap();
            project(&mut owner, &mut doc, &mut allocator, &mut reader, producer).unwrap();
        }
        (owner, doc, media)
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        self.store.shutdown().unwrap();
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn identity(direction: Direction) -> Identity {
    Identity {
        request_id: LocalId::new("request").unwrap(),
        stream_id: LocalId::new("stream").unwrap(),
        attempt: 1,
        direction,
    }
}

#[derive(Clone, Copy, Debug)]
enum Source {
    Anthropic,
    Responses,
    Gemini,
    Chat,
    /// A Gemini answer whose signature rides on its text part.
    GeminiText,
    Compaction,
    /// Anthropic signed thinking and text, without a call.
    AnthropicText,
    /// A compaction whose item is Anthropic's signed block.
    SignedCompaction,
    /// An Antigravity Claude answer as a Responses client receives it: signed thinking, text,
    /// and a call whose signature rides on the reasoning item before it.
    AntigravityClaude,
    /// Two Antigravity Claude calls whose signatures differ only in surrounding whitespace.
    AntigravityTwoCalls,
}

impl Source {
    fn operation(self) -> Operation {
        match self {
            Self::Compaction | Self::SignedCompaction => Operation::Compact,
            _ => Operation::Generate,
        }
    }

    fn body(self) -> Vec<u8> {
        let body = match self {
            Self::Anthropic => json!({
                "id": "msg_1", "type": "message", "role": "assistant", "model": "claude",
                "content": [
                    {"type": "thinking", "thinking": "hmm", "signature": "SIG/abc+=="},
                    {"type": "redacted_thinking", "data": "REDACTED=="},
                    {"type": "text", "text": "hi"},
                    {"type": "tool_use", "id": "toolu_1", "name": "f", "input": {"a": 1}}
                ],
                "stop_reason": "tool_use", "usage": {"input_tokens": 1, "output_tokens": 2}
            }),
            Self::AnthropicText => json!({
                "id": "msg_1", "type": "message", "role": "assistant", "model": "claude",
                "content": [
                    {"type": "thinking", "thinking": "hmm", "signature": "SIG/abc+=="},
                    {"type": "text", "text": "hi"}
                ],
                "stop_reason": "end_turn", "usage": {"input_tokens": 1, "output_tokens": 2}
            }),
            Self::Responses => json!({
                "id": "resp_1", "object": "response", "model": "gpt", "status": "completed",
                "output": [
                    {"id": "rs_1", "type": "reasoning", "summary": [{"type": "summary_text", "text": "sum"}], "encrypted_content": "ENC=="},
                    {"id": "msg_1", "type": "message", "role": "assistant", "status": "completed", "content": [{"type": "output_text", "text": "hi", "annotations": []}]},
                    {"id": "fc_1", "type": "function_call", "call_id": "call_1", "name": "f", "arguments": "{\"a\":1}", "status": "completed"}
                ],
                "usage": {"input_tokens": 1, "output_tokens": 2, "total_tokens": 3}
            }),
            Self::Gemini => json!({
                "candidates": [{"content": {"role": "model", "parts": [
                    {"text": "think", "thought": true, "thoughtSignature": "GSIG=="},
                    {"text": "hi"},
                    {"functionCall": {"name": "f", "args": {"a": 1}}, "thoughtSignature": "G2=="}
                ]}, "finishReason": "STOP"}],
                "usageMetadata": {"promptTokenCount": 1, "candidatesTokenCount": 2, "totalTokenCount": 3},
                "modelVersion": "gemini"
            }),
            Self::Chat => json!({
                "id": "chatcmpl-1", "object": "chat.completion", "created": 1, "model": "gpt",
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "hi", "tool_calls": [
                    {"id": "call_1", "type": "function", "function": {"name": "f", "arguments": "{\"a\":1}"}}
                ]}, "finish_reason": "tool_calls"}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}
            }),
            Self::GeminiText => json!({
                "candidates": [{"content": {"role": "model", "parts": [
                    {"text": "hi", "thoughtSignature": "TSIG=="}
                ]}, "finishReason": "STOP"}],
                "modelVersion": "gemini"
            }),
            Self::Compaction => json!({
                "id": "resp_c", "object": "response.compaction",
                "output": [{"id": "cmp_1", "type": "compaction", "encrypted_content": "CMP=="}],
                "usage": {"input_tokens": 1, "output_tokens": 2, "total_tokens": 3}
            }),
            Self::AntigravityClaude => antigravity_answer(antigravity_items()),
            Self::AntigravityTwoCalls => antigravity_answer(json!([
                {"id": "rs_a", "type": "reasoning", "summary": [], "encrypted_content": CALL_SIG},
                {"id": "fc_a", "type": "function_call", "call_id": "call_a", "name": "f", "arguments": "{}", "status": "completed"},
                {"id": "rs_b", "type": "reasoning", "summary": [], "encrypted_content": CALL_SIG_TAB},
                {"id": "fc_b", "type": "function_call", "call_id": "call_b", "name": "g", "arguments": "{}", "status": "completed"}
            ])),
            Self::SignedCompaction => json!({
                "id": "resp_c", "object": "response.compaction",
                "output": [{"id": "cmp_1", "type": "compaction", "content": "sum", "signature": "SIG=="}],
                "usage": {"input_tokens": 1, "output_tokens": 2, "total_tokens": 3}
            }),
        };
        serde_json::to_vec(&body).unwrap()
    }
}

#[derive(Clone, Copy, Debug)]
enum To {
    Anthropic,
    Responses,
    Gemini,
    Chat,
    /// The Antigravity steps for a Claude model: their Responses egress, then the translator.
    Antigravity,
}

const CLAUDE_A: Producer<'static> = Producer {
    provider: "claude",
    credential: "aaaa",
};
const CLAUDE_B: Producer<'static> = Producer {
    provider: "claude",
    credential: "bbbb",
};
const CODEX_A: Producer<'static> = Producer {
    provider: "codex",
    credential: "cccc",
};
const CODEX_B: Producer<'static> = Producer {
    provider: "codex",
    credential: "dddd",
};
const GEMINI_A: Producer<'static> = Producer {
    provider: "gemini",
    credential: "eeee",
};
const GEMINI_B: Producer<'static> = Producer {
    provider: "gemini",
    credential: "ffff",
};
const ANTIGRAVITY_A: Producer<'static> = Producer {
    provider: "antigravity",
    credential: "gggg",
};
const ANTIGRAVITY_B: Producer<'static> = Producer {
    provider: "antigravity",
    credential: "hhhh",
};

/// Claude signatures in Antigravity's double-layer and single-layer forms, the second padded as
/// an upstream may send it.
const THINKING_SIG: &str = "RWhrS0Z3Z01FQUl5RVdOc1lYVmtaUzF6YjI1dVpYUXROQzAyR0FFPQ==";
const CALL_SIG: &str = " EhkKFwgMEAIyEWNsYXVkZS1zb25uZXQtNC02GAE=\n";
/// [`CALL_SIG`] with other padding: a second item holding the same signature differently.
const CALL_SIG_TAB: &str = "\tEhkKFwgMEAIyEWNsYXVkZS1zb25uZXQtNC02GAE= ";

/// The client's first message, then the items of [`Source::AntigravityClaude`] replayed by the
/// client itself, the call's result and an opaque item, each claiming proof.
fn client_replay() -> Value {
    let mut items = vec![json!({"role": "user", "content": "q"})];
    items.extend(antigravity_items().as_array().unwrap().iter().cloned());
    items.push(json!({"type": "function_call_output", "call_id": "call_1", "output": "done"}));
    // An item kind kept whole.
    items.push(json!({"type": "item_reference", "id": "ref_1"}));
    // A client cannot vouch for its own items, or anything in them, with the member the server
    // sets.
    for (i, item) in items.iter_mut().enumerate() {
        item["_ck_proven"] = json!(i);
    }
    items[2]["content"][0]["_ck_proven"] = json!(0);
    Value::Array(items)
}

/// The output items of [`Source::AntigravityClaude`].
fn antigravity_items() -> Value {
    json!([
        {"id": "rs_1", "type": "reasoning", "summary": [{"type": "summary_text", "text": "sum"}], "encrypted_content": THINKING_SIG},
        {"id": "msg_1", "type": "message", "role": "assistant", "status": "completed", "content": [{"type": "output_text", "text": "hi", "annotations": []}]},
        {"id": "rs_2", "type": "reasoning", "summary": [], "encrypted_content": CALL_SIG},
        {"id": "fc_1", "type": "function_call", "call_id": "call_1", "name": "f", "arguments": "{\"a\":1}", "status": "completed"}
    ])
}

fn antigravity_answer(output: Value) -> Value {
    json!({
        "id": "resp_a", "object": "response", "model": "claude-sonnet-4-6", "status": "completed",
        "output": output,
        "usage": {"input_tokens": 1, "output_tokens": 2, "total_tokens": 3}
    })
}

/// The upstream body for the candidate `attempt` after admission, or the admission refusal.
fn attempt(
    owner: &mut Writer,
    doc: &mut Document,
    media: &Registry,
    attempt: Producer<'_>,
    to: To,
) -> Result<Value, Admission> {
    match admit(doc, attempt).unwrap() {
        Admission::Refuse => return Err(Admission::Refuse),
        Admission::Drop => drop_foreign(owner, doc, attempt).unwrap(),
        Admission::Keep => {}
    }
    let view = RequestView {
        document: doc,
        media,
        operation: Operation::Generate,
    };
    let lease = match to {
        To::Anthropic => {
            let capability = crate::anthropic::request::AnthropicCapability {
                default_max_tokens: Some(4096),
                ..Default::default()
            };
            crate::anthropic::request::Egress {
                destination: capability.destination(),
                upstream_stream: false,
                default_max_tokens: Some(4096),
            }
            .encode(&fence(view, &capability), owner)
        }
        To::Responses => crate::responses::request::Egress {
            destination: "responses",
            upstream_stream: false,
        }
        .encode(&fence(view, &crate::responses::request::ResponsesCapability), owner),
        To::Gemini => crate::gemini::request::Egress {
            destination: "gemini",
            upstream_stream: false,
        }
        .encode(&fence(view, &crate::gemini::request::GeminiCapability), owner),
        To::Chat => crate::codec::chat::request::Egress {
            destination: "chat",
            upstream_stream: false,
        }
        .encode(&fence(view, &crate::codec::chat::request::ChatCapability), owner),
        To::Antigravity => crate::responses::request::Egress {
            destination: "antigravity",
            upstream_stream: false,
        }
        .encode(&fence(view, &crate::responses::request::AntigravityCapability), owner),
    }
    .unwrap();
    let mut out = Vec::new();
    while out.len() < lease.len() {
        out.extend_from_slice(lease.chunk(out.len()).unwrap());
    }
    if let To::Antigravity = to {
        let ctx = crate::RequestCtx {
            model: "claude-sonnet-4-6",
            stream: false,
            tool_search: false,
        };
        out = crate::antigravity_responses::request_envelope(&ctx, &out, None).unwrap();
    }
    Ok(serde_json::from_slice(&out).unwrap())
}

fn fence<'a>(view: RequestView<'a>, capability: &impl Capability) -> crate::codec::ValidatedRequest<'a> {
    RequestFence::default()
        .end(view, capability)
        .unwrap_or_else(|error| panic!("{} refused the history: {error:?}", capability.destination()))
}

/// The projected history in a destination body: what follows the client's first message.
fn history(out: &Value, to: To) -> Value {
    let list = match to {
        To::Anthropic | To::Chat => &out["messages"],
        To::Responses => &out["input"],
        To::Gemini => &out["contents"],
        To::Antigravity => &out["request"]["contents"],
    };
    Value::Array(list.as_array().unwrap()[1..].to_vec())
}

/// A stored response projected for its own producer reaches that producer's protocol with
/// every member it sent, signatures byte for byte, whatever the chunking of the upstream body.
#[test]
fn history_returns_to_its_producer_unchanged() {
    let rows = [
        (
            Source::Anthropic,
            CLAUDE_A,
            To::Anthropic,
            json!([{"role": "assistant", "content": [
                {"type": "thinking", "thinking": "hmm", "signature": "SIG/abc+=="},
                {"type": "redacted_thinking", "data": "REDACTED=="},
                {"type": "text", "text": "hi"},
                {"type": "tool_use", "id": "toolu_1", "name": "f", "input": {"a": 1}}
            ]}]),
        ),
        (
            Source::Responses,
            CODEX_A,
            To::Responses,
            json!([
                {"type": "reasoning", "summary": [{"type": "summary_text", "text": "sum"}], "encrypted_content": "ENC=="},
                {"role": "assistant", "content": [{"type": "output_text", "text": "hi"}]},
                {"type": "function_call", "call_id": "call_1", "name": "f", "arguments": "{\"a\":1}"}
            ]),
        ),
        (
            Source::Gemini,
            GEMINI_A,
            To::Gemini,
            // The call id is the canonical one the Responses client was shown.
            json!([{"role": "model", "parts": [
                {"text": "think", "thought": true, "thoughtSignature": "GSIG=="},
                {"text": "hi"},
                {"functionCall": {"name": "f", "id": "t2", "args": {"a": 1}}, "thoughtSignature": "G2=="}
            ]}]),
        ),
        (
            Source::Chat,
            CODEX_A,
            To::Chat,
            json!([{"role": "assistant", "content": [{"type": "text", "text": "hi"}], "tool_calls": [
                {"type": "function", "id": "call_1", "function": {"name": "f", "arguments": "{\"a\":1}"}}
            ]}]),
        ),
    ];
    let rig = Rig::new();
    for (source, producer, to, expected) in rows {
        let mut outputs = Vec::new();
        for width in [usize::MAX, 1] {
            let (mut owner, mut doc, media) = rig.history(json!("q"), &[(source, producer)], width);
            outputs.push(attempt(&mut owner, &mut doc, &media, producer, to).unwrap());
        }
        assert_eq!(history(&outputs[0], to), expected, "{source:?} -> {to:?}");
        assert_eq!(outputs[0], outputs[1], "{source:?}: 1-byte feed");
    }
}

/// Each provider member reaches only the account that produced it (Q-C): reasoning and
/// signatures from another account are dropped and the conversation goes on; a compaction item
/// from another account refuses the candidate. Turns keep their own producer across X, Y, Z.
#[test]
fn provider_members_reach_only_their_producer() {
    struct Row {
        case: &'static str,
        /// What the client replays itself, untagged, before the projected turns.
        input: Value,
        turns: &'static [(Source, Producer<'static>)],
        attempt: Producer<'static>,
        to: To,
        admission: Admission,
        present: &'static [&'static str],
        absent: &'static [&'static str],
    }
    const ALL: &[&str] = &["SIG/abc+==", "REDACTED==", "GSIG==", "G2==", "TSIG==", "ENC==", "CMP=="];
    let rows = [
        Row {
            case: "another Claude account",
            input: json!("q"),
            turns: &[(Source::Anthropic, CLAUDE_A)],
            attempt: CLAUDE_B,
            to: To::Anthropic,
            admission: Admission::Drop,
            present: &["\"hi\"", "toolu_1"],
            absent: &["SIG/abc+==", "REDACTED==", "hmm"],
        },
        Row {
            case: "Claude history at Gemini",
            input: json!("q"),
            turns: &[(Source::Anthropic, CLAUDE_A)],
            attempt: GEMINI_A,
            to: To::Gemini,
            admission: Admission::Drop,
            present: &["\"hi\""],
            absent: ALL,
        },
        Row {
            case: "Claude history at Responses",
            input: json!("q"),
            turns: &[(Source::Anthropic, CLAUDE_A)],
            attempt: CODEX_A,
            to: To::Responses,
            admission: Admission::Drop,
            present: &["\"hi\"", "toolu_1"],
            absent: ALL,
        },
        Row {
            case: "Gemini history at Claude",
            input: json!("q"),
            turns: &[(Source::Gemini, GEMINI_A)],
            attempt: CLAUDE_A,
            to: To::Anthropic,
            admission: Admission::Drop,
            // The call its signature rode on stays.
            present: &["\"hi\"", "\"name\":\"f\""],
            absent: &["GSIG==", "G2==", "think"],
        },
        Row {
            case: "another Gemini account: thought and call signatures",
            input: json!("q"),
            turns: &[(Source::Gemini, GEMINI_A)],
            attempt: GEMINI_B,
            to: To::Gemini,
            admission: Admission::Drop,
            present: &["\"hi\"", "\"name\":\"f\""],
            absent: &["GSIG==", "G2==", "think"],
        },
        Row {
            case: "another Gemini account: a signature on a text part",
            input: json!("q"),
            turns: &[(Source::GeminiText, GEMINI_A)],
            attempt: GEMINI_B,
            to: To::Gemini,
            admission: Admission::Drop,
            present: &["\"hi\""],
            absent: &["TSIG=="],
        },
        Row {
            case: "another Codex account",
            input: json!("q"),
            turns: &[(Source::Responses, CODEX_A)],
            attempt: CODEX_B,
            to: To::Responses,
            admission: Admission::Drop,
            present: &["\"hi\"", "call_1"],
            absent: &["ENC==", "sum", "reasoning"],
        },
        Row {
            case: "Codex history at Claude: no message is left empty",
            input: json!("q"),
            turns: &[(Source::Responses, CODEX_A)],
            attempt: CLAUDE_A,
            to: To::Anthropic,
            admission: Admission::Drop,
            present: &["\"hi\"", "call_1"],
            absent: &["ENC==", "sum", "\"content\":[]"],
        },
        Row {
            case: "Codex history at Chat",
            input: json!("q"),
            turns: &[(Source::Responses, CODEX_A)],
            attempt: CLAUDE_A,
            to: To::Chat,
            admission: Admission::Drop,
            present: &["\"hi\""],
            absent: ALL,
        },
        Row {
            case: "X -> Y -> Z: the third account gets neither",
            input: json!("q"),
            turns: &[(Source::Anthropic, CLAUDE_A), (Source::Gemini, GEMINI_A)],
            attempt: CODEX_A,
            to: To::Responses,
            admission: Admission::Drop,
            present: &["\"hi\""],
            absent: ALL,
        },
        Row {
            case: "X -> Y -> Z: back at X",
            input: json!("q"),
            turns: &[(Source::Anthropic, CLAUDE_A), (Source::Gemini, GEMINI_A)],
            attempt: CLAUDE_A,
            to: To::Anthropic,
            admission: Admission::Drop,
            present: &["SIG/abc+==", "REDACTED=="],
            absent: &["GSIG==", "G2=="],
        },
        Row {
            case: "X -> Y -> Z: back at Y",
            input: json!("q"),
            turns: &[(Source::Anthropic, CLAUDE_A), (Source::Gemini, GEMINI_A)],
            attempt: GEMINI_A,
            to: To::Gemini,
            admission: Admission::Drop,
            present: &["GSIG==", "G2=="],
            absent: &["SIG/abc+==", "REDACTED=="],
        },
        Row {
            case: "X -> Y: back at Y after X's members are dropped",
            input: json!("q"),
            turns: &[(Source::Anthropic, CLAUDE_A), (Source::Responses, CODEX_A)],
            attempt: CODEX_A,
            to: To::Responses,
            admission: Admission::Drop,
            present: &["ENC==", "sum"],
            absent: &["SIG/abc+==", "REDACTED=="],
        },
        Row {
            case: "a compaction item at its own account",
            input: json!("q"),
            turns: &[(Source::Responses, CODEX_A), (Source::Compaction, CODEX_A)],
            attempt: CODEX_A,
            to: To::Responses,
            admission: Admission::Keep,
            present: &["CMP==", "ENC=="],
            absent: &[],
        },
        Row {
            case: "a compaction item at another account",
            input: json!("q"),
            turns: &[(Source::Compaction, CODEX_A)],
            attempt: CODEX_B,
            to: To::Responses,
            admission: Admission::Refuse,
            present: &[],
            absent: &[],
        },
        Row {
            case: "a compaction item at another provider",
            input: json!("q"),
            turns: &[(Source::Anthropic, CLAUDE_A), (Source::Compaction, CODEX_A)],
            attempt: CLAUDE_A,
            to: To::Anthropic,
            admission: Admission::Refuse,
            present: &[],
            absent: &[],
        },
        Row {
            case: "Antigravity history at its own account: thinking and call signatures, bytes kept",
            input: json!("q"),
            turns: &[(Source::AntigravityClaude, ANTIGRAVITY_A)],
            attempt: ANTIGRAVITY_A,
            to: To::Antigravity,
            admission: Admission::Keep,
            present: &[
                THINKING_SIG,
                "\"thoughtSignature\":\" EhkKFwgMEAIyEWNsYXVkZS1zb25uZXQtNC02GAE=\\n\"",
            ],
            absent: &["_ck_"],
        },
        Row {
            case: "Antigravity history at its own account: each call keeps its own item's bytes",
            input: json!("q"),
            turns: &[(Source::AntigravityTwoCalls, ANTIGRAVITY_A)],
            attempt: ANTIGRAVITY_A,
            to: To::Antigravity,
            admission: Admission::Keep,
            present: &[
                "\"thoughtSignature\":\" EhkKFwgMEAIyEWNsYXVkZS1zb25uZXQtNC02GAE=\\n\"",
                "\"thoughtSignature\":\"\\tEhkKFwgMEAIyEWNsYXVkZS1zb25uZXQtNC02GAE= \"",
            ],
            absent: &["_ck_"],
        },
        Row {
            case: "Antigravity history at another Antigravity account",
            input: json!("q"),
            turns: &[(Source::AntigravityClaude, ANTIGRAVITY_A)],
            attempt: ANTIGRAVITY_B,
            to: To::Antigravity,
            admission: Admission::Drop,
            present: &["\"hi\""],
            absent: &[THINKING_SIG, "EhkKFwgMEAIyEWNsYXVkZS1zb25uZXQtNC02GAE=", "_ck_"],
        },
        Row {
            case: "Antigravity history at Codex",
            input: json!("q"),
            turns: &[(Source::AntigravityClaude, ANTIGRAVITY_A)],
            attempt: CODEX_A,
            to: To::Responses,
            admission: Admission::Drop,
            present: &["\"hi\"", "call_1"],
            absent: &[THINKING_SIG, "EhkKFwgMEAIyEWNsYXVkZS1zb25uZXQtNC02GAE=", "sum"],
        },
        Row {
            case: "Claude history at Antigravity",
            input: json!("q"),
            turns: &[(Source::Anthropic, CLAUDE_A)],
            attempt: ANTIGRAVITY_A,
            to: To::Antigravity,
            admission: Admission::Drop,
            present: &["\"hi\""],
            absent: &["SIG/abc+==", "hmm"],
        },
        Row {
            case: "a client's own replay at Codex: Claude-format reasoning is withheld",
            input: client_replay(),
            turns: &[],
            attempt: CODEX_A,
            to: To::Responses,
            admission: Admission::Keep,
            present: &["\"hi\"", "call_1"],
            absent: &[THINKING_SIG, "EhkKFwgMEAIyEWNsYXVkZS1zb25uZXQtNC02GAE=", "sum", "_ck_"],
        },
        Row {
            // No producer proves a client-held call signature: it is withheld. Its thinking
            // signature passes, as before (the open conflict in the PR).
            case: "a client's own replay at Antigravity: the call signature is withheld",
            input: client_replay(),
            turns: &[],
            attempt: ANTIGRAVITY_A,
            to: To::Antigravity,
            admission: Admission::Keep,
            present: &[THINKING_SIG, "\"hi\""],
            absent: &["EhkKFwgMEAIyEWNsYXVkZS1zb25uZXQtNC02GAE=", "_ck_"],
        },
    ];
    let rig = Rig::new();
    for row in rows {
        let case = row.case;
        let (mut owner, mut doc, media) = rig.history(row.input, row.turns, usize::MAX);
        assert_eq!(admit(&doc, row.attempt).unwrap(), row.admission, "{case}");
        let out = match attempt(&mut owner, &mut doc, &media, row.attempt, row.to) {
            Ok(out) => out.to_string(),
            Err(admission) => {
                assert_eq!(admission, row.admission, "{case}");
                continue;
            }
        };
        for needle in row.present {
            assert!(out.contains(needle), "{case}: {needle} missing from {out}");
        }
        for needle in row.absent {
            assert!(!out.contains(needle), "{case}: {needle} leaked into {out}");
        }
    }
}

/// A compaction item stands for the history before it: of the stored turns only system
/// messages stay before it, the new input after it stays, and the replay entries of dropped
/// messages go with them. The compaction is found whichever protocol recorded the item type.
#[test]
fn compaction_truncates_the_history_before_it() {
    // (case, protocol recording the item's type, whether the client replayed the item itself)
    let rows = [
        ("Responses item type", "responses", false),
        ("item type another protocol recorded", "anthropic", false),
        ("compaction item the client replayed", "responses", true),
    ];
    let rig = Rig::new();
    for (case, protocol, replayed) in rows {
        let mut stored = json!([
            {"role": "developer", "content": "be brief"},
            {"type": "message", "id": "msg_0", "role": "user", "content": "old"}
        ]);
        let mut turns = vec![(Source::Responses, CODEX_A), (Source::Compaction, CODEX_A)];
        if replayed {
            stored
                .as_array_mut()
                .unwrap()
                .push(json!({"type": "compaction", "encrypted_content": "CMP=="}));
            turns.clear();
        }
        let (mut owner, mut doc, media) = rig.history(stored, &turns, usize::MAX);
        recorded_by(&mut doc, protocol);
        new_input(&mut owner, &mut doc, "new");
        truncate(&mut owner, &mut doc).unwrap();
        // No provenance entry outlives the message it described.
        let x = doc.required(doc.root(), "x_ck").unwrap();
        for list in crate::codec::provenance::bound_lists(&doc, x)
            .unwrap()
            .into_iter()
            .flatten()
        {
            for entry in doc.children(list).unwrap() {
                if let Some((node, _)) = doc.origin(entry).unwrap() {
                    assert!(
                        crate::codec::provenance::attached(&doc, node).unwrap(),
                        "{case}: an orphaned entry"
                    );
                }
            }
        }
        let out = attempt(&mut owner, &mut doc, &media, CODEX_A, To::Responses).unwrap();
        let input = out["input"].as_array().unwrap();
        assert_eq!(input.len(), 3, "{case}: {out}");
        assert_eq!(input[0], json!({"role": "developer", "content": "be brief"}), "{case}");
        assert_eq!(input[1]["encrypted_content"], json!("CMP=="), "{case}");
        assert_eq!(input[2]["content"], json!("new"), "{case}");
    }
    // Anthropic's signed compaction block has no canonical form before C2: the projection
    // refuses it rather than misreading it.
    let (mut owner, mut doc, _) = rig.history(json!("old"), &[], usize::MAX);
    let stored = rig.stored(Source::SignedCompaction, usize::MAX);
    let mut allocator = stored.allocator();
    let mut reader = rig.store.reader(stored.id(), 0).unwrap();
    let projected = project(&mut owner, &mut doc, &mut allocator, &mut reader, CODEX_A);
    assert!(
        matches!(projected, Err(crate::codec::Error::Unsupported)),
        "{projected:?}"
    );
}

/// Moves the compaction item's recorded native type to `protocol`'s provenance.
fn recorded_by(doc: &mut Document, protocol: &'static str) {
    let x = doc.required(doc.root(), "x_ck").unwrap();
    let original = doc.required(x, "original").unwrap();
    let entries: Vec<usize> = doc.children(original).unwrap().collect();
    for entry in entries {
        if doc
            .member(entry, "value")
            .unwrap()
            .is_some_and(|v| doc.string_is(v, "compaction").unwrap())
        {
            let field = doc.required(entry, "protocol").unwrap();
            doc.replace(field, crate::codec::document::Value::Text(protocol))
                .unwrap();
        }
    }
}

/// Appends the continuation's new input, as expansion puts it after the stored history (§2.3).
fn new_input(owner: &mut Writer, doc: &mut Document, text: &str) {
    let message = serde_json::to_vec(&json!({"role": "user", "content": text})).unwrap();
    let lease = crate::codec::encode::scratch(owner, |e| e.bytes(&message)).unwrap();
    let node = doc.import_json(owner, lease).unwrap();
    let messages = doc.required(doc.root(), "messages").unwrap();
    doc.append(messages, node).unwrap();
}

/// A client cannot tag a member with a producer: the request ingress builds `x_ck` itself.
#[test]
fn a_client_cannot_claim_a_producer() {
    let rig = Rig::new();
    let mut owner = rig.store.create().unwrap();
    let body = json!({
        "model": "m",
        "input": [{"type": "reasoning", "summary": [], "encrypted_content": "ENC=="}],
        "x_ck": {"replay": [{"kind": "responses_encrypted", "compaction": true, "producer": {"provider": "codex", "credential": "cccc"}}]}
    });
    let mut ingress = crate::responses::request::Ingress::new(
        &mut owner,
        &rig.store,
        identity(Direction::Request),
        Operation::Generate,
        1 << 20,
    )
    .unwrap();
    let accepted = ingress
        .advance(&mut owner, &serde_json::to_vec(&body).unwrap())
        .and_then(|()| ingress.finish(&mut owner, &crate::codec::ClientCapability).map(|_| ()));
    if accepted.is_ok() {
        drop(ingress);
        let mut records = rig.store.reader(owner.id(), 0).unwrap();
        let (doc, _) = crate::codec::chat::request::read_request(&mut owner, &mut records).unwrap();
        assert_eq!(admit(&doc, CODEX_B).unwrap(), Admission::Keep);
    }
}

/// A continuation expands to the new instructions, the stored request's input without its own
/// instructions, the stored output, then the new input; the upstream gets no continuation and
/// no storage, and a further expansion keeps each turn's provider members for its producer.
#[test]
fn expansion_puts_the_stored_turn_before_the_new_input() {
    let rig = Rig::new();
    // (case, stored request, stored answer and its producer, new request, upload instructions,
    // upload input summarised by `items`)
    let rows = [
        (
            "the new instructions replace the stored ones",
            json!({"model": "m", "instructions": "old rules", "input": "q1"}),
            (Source::AnthropicText, CLAUDE_A),
            json!({"model": "m", "instructions": "new rules", "input": "q2", "previous_response_id": "resp_1", "store": true}),
            json!("new rules"),
            vec!["user:q1", "assistant:hi", "user:q2"],
        ),
        (
            "encrypted reasoning in both turns keeps its order",
            json!({"model": "m", "input": [
                {"type": "reasoning", "summary": [], "encrypted_content": "OLD=="},
                {"role": "user", "content": "q1"}
            ]}),
            (Source::Responses, CODEX_A),
            json!({"model": "m", "previous_response_id": "resp_1", "input": [
                {"type": "reasoning", "summary": [], "encrypted_content": "NEW=="},
                {"role": "user", "content": "q2"}
            ]}),
            Value::Null,
            vec![
                "reasoning:OLD==",
                "user:q1",
                "reasoning:ENC==",
                "assistant:hi",
                "function_call:f",
                "reasoning:NEW==",
                "user:q2",
            ],
        ),
    ];
    for (case, stored, (source, producer), continued, instructions, items) in rows {
        let first = rig.request(stored);
        let answer = rig.stored(source, usize::MAX);
        let (mut second, mut doc, media) = rig.expand(continued, &first, &answer, producer);
        let out = attempt(&mut second, &mut doc, &media, CODEX_A, To::Responses).unwrap();
        assert_eq!(out["instructions"], instructions, "{case}: {out}");
        assert_eq!(out["store"], json!(false), "{case}: {out}");
        assert!(out.get("previous_response_id").is_none(), "{case}: {out}");
        assert_eq!(summary(&out["input"]), items, "{case}: {out}");
    }

    let first = rig.request(json!({"model": "m", "instructions": "old rules", "input": "q1"}));
    let answer = rig.stored(Source::AnthropicText, usize::MAX);
    // A third turn continues the second, which another account answered: the first account
    // still gets its signature back, and nobody else does.
    let (second, _, _) = rig.expand(
        json!({"model": "m", "input": "q2", "previous_response_id": "resp_1"}),
        &first,
        &answer,
        CLAUDE_A,
    );
    let answer = rig.stored(Source::Chat, usize::MAX);
    let third = json!({"model": "m", "input": "q3", "previous_response_id": "resp_2"});
    for (producer, signed) in [(CLAUDE_A, true), (CLAUDE_B, false)] {
        let (mut owner, mut doc, media) = rig.expand(third.clone(), &second, &answer, CODEX_A);
        let out = attempt(&mut owner, &mut doc, &media, producer, To::Anthropic).unwrap();
        let messages = out["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 5, "{out}");
        assert_eq!(out.to_string().contains("SIG/abc+=="), signed, "{producer:?}: {out}");
    }

    // A compaction answer replaces the history before it.
    let compacted = rig.stored(Source::Compaction, usize::MAX);
    let next = json!({"model": "m", "input": "q2", "previous_response_id": "resp_c"});
    let (mut owner, mut doc, media) = rig.expand(next, &first, &compacted, CODEX_A);
    let out = attempt(&mut owner, &mut doc, &media, CODEX_A, To::Responses).unwrap();
    let input = out["input"].as_array().unwrap();
    assert_eq!(input.len(), 2, "{out}");
    assert_eq!(input[0]["encrypted_content"], json!("CMP=="), "{out}");
    drop(second);
}

/// One line per Responses input item: its kind and its text, encrypted content or call name.
fn summary(input: &Value) -> Vec<String> {
    input
        .as_array()
        .unwrap()
        .iter()
        .map(|item| match item["type"].as_str() {
            Some("reasoning") => format!("reasoning:{}", item["encrypted_content"].as_str().unwrap()),
            Some("function_call") => format!("function_call:{}", item["name"].as_str().unwrap()),
            _ => {
                let text = match &item["content"] {
                    Value::String(text) => text.clone(),
                    Value::Array(parts) => parts.iter().filter_map(|p| p["text"].as_str()).collect(),
                    other => other.to_string(),
                };
                format!("{}:{text}", item["role"].as_str().unwrap())
            }
        })
        .collect()
}
