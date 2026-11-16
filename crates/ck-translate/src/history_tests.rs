//! History projection and per-attempt admission (D2-PLAN §2.3, §2.4, §2.7; Q-C).
use crate::codec::{
    BodyMode, Capability, Operation, RequestEgress, RequestFence, RequestIngress, RequestView, ResponseIngress,
    document::Document,
    event::{Direction, Identity, LocalId},
    media::Registry,
    provenance::{Admission, Producer, admit, drop_foreign},
};
use crate::responses::history::{project, truncate};
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
            Source::Anthropic => {
                let mut i = crate::anthropic::response::Ingress::new(&mut w, id, op, mode, limit).unwrap();
                feed(&mut i, &mut w);
            }
            Source::Responses | Source::Compaction => {
                let mut i = crate::responses::response::Ingress::new(&mut w, id, op, mode, limit).unwrap();
                feed(&mut i, &mut w);
            }
            Source::Gemini => {
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

    /// A Responses client's canonical request, rebuilt from its request records as the server
    /// does, with each of `turns` projected after it.
    fn history(&self, input: Value, turns: &[(Source, Producer<'_>)], width: usize) -> (Writer, Document, Registry) {
        let mut owner = self.store.create().unwrap();
        let body = serde_json::to_vec(&json!({"model": "m", "input": input})).unwrap();
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
    Compaction,
}

impl Source {
    fn operation(self) -> Operation {
        match self {
            Self::Compaction => Operation::Compact,
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
            Self::Compaction => json!({
                "id": "resp_c", "object": "response.compaction",
                "output": [{"id": "cmp_1", "type": "compaction", "encrypted_content": "CMP=="}],
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
    }
    .unwrap();
    let mut out = Vec::new();
    while out.len() < lease.len() {
        out.extend_from_slice(lease.chunk(out.len()).unwrap());
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
        turns: &'static [(Source, Producer<'static>)],
        attempt: Producer<'static>,
        to: To,
        admission: Admission,
        present: &'static [&'static str],
        absent: &'static [&'static str],
    }
    const ALL: &[&str] = &["SIG/abc+==", "REDACTED==", "GSIG==", "G2==", "ENC==", "CMP=="];
    let rows = [
        Row {
            case: "another Claude account",
            turns: &[(Source::Anthropic, CLAUDE_A)],
            attempt: CLAUDE_B,
            to: To::Anthropic,
            admission: Admission::Drop,
            present: &["\"hi\"", "toolu_1"],
            absent: &["SIG/abc+==", "REDACTED==", "hmm"],
        },
        Row {
            case: "Claude history at Gemini",
            turns: &[(Source::Anthropic, CLAUDE_A)],
            attempt: GEMINI_A,
            to: To::Gemini,
            admission: Admission::Drop,
            present: &["\"hi\""],
            absent: ALL,
        },
        Row {
            case: "Claude history at Responses",
            turns: &[(Source::Anthropic, CLAUDE_A)],
            attempt: CODEX_A,
            to: To::Responses,
            admission: Admission::Drop,
            present: &["\"hi\"", "toolu_1"],
            absent: ALL,
        },
        Row {
            case: "Gemini history at Claude",
            turns: &[(Source::Gemini, GEMINI_A)],
            attempt: CLAUDE_A,
            to: To::Anthropic,
            admission: Admission::Drop,
            // The call its signature rode on stays.
            present: &["\"hi\"", "\"name\":\"f\""],
            absent: &["GSIG==", "G2==", "think"],
        },
        Row {
            case: "another Codex account",
            turns: &[(Source::Responses, CODEX_A)],
            attempt: CODEX_B,
            to: To::Responses,
            admission: Admission::Drop,
            present: &["\"hi\"", "call_1"],
            absent: &["ENC==", "sum", "reasoning"],
        },
        Row {
            case: "Codex history at Claude: no message is left empty",
            turns: &[(Source::Responses, CODEX_A)],
            attempt: CLAUDE_A,
            to: To::Anthropic,
            admission: Admission::Drop,
            present: &["\"hi\"", "call_1"],
            absent: &["ENC==", "sum", "\"content\":[]"],
        },
        Row {
            case: "Codex history at Chat",
            turns: &[(Source::Responses, CODEX_A)],
            attempt: CLAUDE_A,
            to: To::Chat,
            admission: Admission::Drop,
            present: &["\"hi\""],
            absent: ALL,
        },
        Row {
            case: "X -> Y -> Z: the third account gets neither",
            turns: &[(Source::Anthropic, CLAUDE_A), (Source::Gemini, GEMINI_A)],
            attempt: CODEX_A,
            to: To::Responses,
            admission: Admission::Drop,
            present: &["\"hi\""],
            absent: ALL,
        },
        Row {
            case: "X -> Y -> Z: back at X",
            turns: &[(Source::Anthropic, CLAUDE_A), (Source::Gemini, GEMINI_A)],
            attempt: CLAUDE_A,
            to: To::Anthropic,
            admission: Admission::Drop,
            present: &["SIG/abc+==", "REDACTED=="],
            absent: &["GSIG==", "G2=="],
        },
        Row {
            case: "X -> Y -> Z: back at Y",
            turns: &[(Source::Anthropic, CLAUDE_A), (Source::Gemini, GEMINI_A)],
            attempt: GEMINI_A,
            to: To::Gemini,
            admission: Admission::Drop,
            present: &["GSIG==", "G2=="],
            absent: &["SIG/abc+==", "REDACTED=="],
        },
        Row {
            case: "X -> Y: back at Y after X's members are dropped",
            turns: &[(Source::Anthropic, CLAUDE_A), (Source::Responses, CODEX_A)],
            attempt: CODEX_A,
            to: To::Responses,
            admission: Admission::Drop,
            present: &["ENC==", "sum"],
            absent: &["SIG/abc+==", "REDACTED=="],
        },
        Row {
            case: "a compaction item at its own account",
            turns: &[(Source::Responses, CODEX_A), (Source::Compaction, CODEX_A)],
            attempt: CODEX_A,
            to: To::Responses,
            admission: Admission::Keep,
            present: &["CMP==", "ENC=="],
            absent: &[],
        },
        Row {
            case: "a compaction item at another account",
            turns: &[(Source::Compaction, CODEX_A)],
            attempt: CODEX_B,
            to: To::Responses,
            admission: Admission::Refuse,
            present: &[],
            absent: &[],
        },
        Row {
            case: "a compaction item at another provider",
            turns: &[(Source::Anthropic, CLAUDE_A), (Source::Compaction, CODEX_A)],
            attempt: CLAUDE_A,
            to: To::Anthropic,
            admission: Admission::Refuse,
            present: &[],
            absent: &[],
        },
    ];
    let rig = Rig::new();
    for row in rows {
        let case = row.case;
        let (mut owner, mut doc, media) = rig.history(json!("q"), row.turns, usize::MAX);
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

/// A compaction item stands for the history before it: only system messages stay before it,
/// and the replay entries of dropped messages go with them.
#[test]
fn compaction_truncates_the_history_before_it() {
    let rig = Rig::new();
    let input = json!([
        {"role": "developer", "content": "be brief"},
        {"type": "message", "id": "msg_0", "role": "user", "content": "q"}
    ]);
    let turns = [(Source::Responses, CODEX_A), (Source::Compaction, CODEX_A)];
    let (mut owner, mut doc, media) = rig.history(input, &turns, usize::MAX);
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
                    "an orphaned entry"
                );
            }
        }
    }
    let out = attempt(&mut owner, &mut doc, &media, CODEX_A, To::Responses).unwrap();
    assert_eq!(
        out["input"],
        json!([
            {"role": "developer", "content": "be brief"},
            {"type": "compaction", "encrypted_content": "CMP=="}
        ])
    );
}

/// A client cannot tag a member with a producer: the request ingress builds `x_ck` itself.
#[test]
fn a_client_cannot_claim_a_producer() {
    let rig = Rig::new();
    let mut owner = rig.store.create().unwrap();
    let body = json!({
        "model": "m",
        "input": [{"type": "reasoning", "summary": [], "encrypted_content": "ENC=="}],
        "x_ck": {"replay": [{"kind": "responses_encrypted", "producer": {"provider": "codex", "credential": "cccc", "compaction": true}}]}
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
