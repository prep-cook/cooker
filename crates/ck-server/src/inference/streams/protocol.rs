//! The client protocol's response egress for a stream's [`Output`]. Live delivery and resume
//! build it the same way, so a resume re-encodes exactly the bytes the live client got.

use ck_stream::{Lease, Reader, ScratchLease, Writer};
use ck_translate::codec::document::{Document, Value};
use ck_translate::codec::event::{Direction, Identity, LocalId};
use ck_translate::codec::{self, BodyMode, RequestView, ResponseEgress, internal};
use ck_translate::{anthropic, gemini, responses};

use crate::http::egress::{Framing, Output, Protocol};

pub enum ProtocolEgress {
    Chat(codec::chat::egress::Egress),
    Anthropic(Box<anthropic::egress::Egress>),
    Responses(responses::egress::Egress<fn(&'static str, u64)>),
    Gemini(gemini::egress::Egress),
}

impl ProtocolEgress {
    /// The egress for `output` on the response stream `stream`. `identity` is the attempt's
    /// response identity ([`response_identity`]); `request` is the client's request view,
    /// whose Chat `stream_options.include_usage` decides the usage chunk.
    pub fn new(
        output: Output,
        stream: &mut Writer,
        identity: Identity,
        request: RequestView<'_>,
    ) -> codec::Result<Self> {
        let mode = match output.framing {
            Framing::Json => BodyMode::Buffered,
            Framing::Sse | Framing::JsonArray => BodyMode::Streaming,
        };
        let operation = output.operation;
        // Diagnostics belong to the live delivery; a resume repeats its encoding silently.
        Ok(match output.protocol {
            Protocol::Chat => {
                let usage = include_usage(request.document)?;
                Self::Chat(codec::chat::egress::Egress::new(
                    stream,
                    identity,
                    operation,
                    mode,
                    usage,
                    |_| {},
                )?)
            }
            Protocol::Anthropic => Self::Anthropic(Box::new(anthropic::egress::Egress::new(
                stream,
                identity,
                operation,
                mode,
                |_| {},
            )?)),
            Protocol::Responses => {
                // ponytail: D2-1 passes the exchange's response id and the committed attempt's
                // producer through `StreamHandle` and `Attached`. Until then the id derives from
                // the response stream, the same for live delivery and resume, and the producer is
                // a placeholder that nothing reads before D2-6a.
                let id = codec::identity::ResponseId::new("resp_", stream.id())?
                    .issued()
                    .ok_or(codec::Error::Schema)?;
                let producer = codec::identity::Producer {
                    provider: LocalId::new("unknown")?,
                    credential: [0; 32],
                };
                let ignore: fn(&'static str, u64) = |_, _| {};
                Self::Responses(responses::egress::Egress::new(
                    stream, id, producer, operation, mode, ignore,
                )?)
            }
            Protocol::Gemini => {
                let framing = match output.framing {
                    Framing::Json => gemini::response::Framing::Json,
                    Framing::Sse => gemini::response::Framing::Sse,
                    Framing::JsonArray => gemini::response::Framing::JsonArray,
                };
                Self::Gemini(gemini::egress::Egress::with_framing(
                    stream,
                    operation,
                    framing,
                    |_| {},
                )?)
            }
        })
    }
}

impl ResponseEgress for ProtocolEgress {
    fn advance(
        &mut self,
        owner: &mut Writer,
        input: Lease,
        output: impl FnMut(ScratchLease) -> codec::Result<()>,
    ) -> codec::Result<()> {
        match self {
            Self::Chat(egress) => egress.advance(owner, input, output),
            Self::Anthropic(egress) => egress.advance(owner, input, output),
            Self::Responses(egress) => egress.advance(owner, input, output),
            Self::Gemini(egress) => egress.advance(owner, input, output),
        }
    }

    fn finish(&mut self) -> codec::Result<()> {
        match self {
            Self::Chat(egress) => egress.finish(),
            Self::Anthropic(egress) => egress.finish(),
            Self::Responses(egress) => egress.finish(),
            Self::Gemini(egress) => egress.finish(),
        }
    }
}

/// The attempt identity the response stream's records carry, read from its first record:
/// the Chat and Anthropic egress accept only records of that identity. `Gone` when the
/// stream ends without a record.
pub async fn response_identity(stream: &mut Writer, reader: &mut Reader) -> codec::Result<Identity> {
    let lease = match reader.next().await {
        Some(ck_stream::Read::Record(lease)) => lease,
        _ => return Err(codec::Error::Storage(ck_stream::Error::Gone)),
    };
    let (_, doc) = internal::decode(stream, lease)?;
    let x = doc.required(doc.root(), "x_ck")?;
    Ok(Identity {
        request_id: local(&doc, doc.required(x, "request_id")?)?,
        stream_id: local(&doc, doc.required(x, "stream_id")?)?,
        attempt: doc.unsigned(doc.required(x, "attempt")?)?,
        direction: Direction::Response,
    })
}

fn local(doc: &Document, node: usize) -> codec::Result<LocalId> {
    let mut text = String::new();
    let mut chars = doc.chars(node)?;
    while let Some(c) = chars.next_scalar()? {
        text.push(c);
    }
    LocalId::new(&text)
}

/// Chat `stream_options.include_usage`; absent or `null` options are `false`.
fn include_usage(doc: &Document) -> codec::Result<bool> {
    let Some(options) = doc.member(doc.root(), "stream_options")? else {
        return Ok(false);
    };
    if !matches!(doc.node(options)?.value, Value::Object) {
        return Ok(false);
    }
    Ok(match doc.member(options, "include_usage")? {
        Some(flag) => matches!(doc.node(flag)?.value, Value::Bool(true)),
        None => false,
    })
}
