//! The client protocol's response egress for a stream's [`Output`]. Live delivery and resume
//! build it the same way, so a resume re-encodes exactly the bytes the live client got.

use axum::http::HeaderValue;
use ck_stream::{Lease, Reader, ScratchLease, Writer};
use ck_translate::codec::document::{Document, Value};
use ck_translate::codec::event::{Direction, Identity, LocalId};
use ck_translate::codec::{self, BodyMode, Operation, RequestView, ResponseEgress, internal};
use ck_translate::{anthropic, audio, gemini, images, responses, search, video};

use crate::http::egress::{Framing, Output, Protocol};

pub enum ProtocolEgress {
    Chat(codec::chat::egress::Egress),
    Anthropic(Box<anthropic::egress::Egress>),
    Responses(responses::egress::Egress<fn(&'static str, u64)>),
    Gemini(gemini::egress::Egress),
    /// A media file's bytes: speech audio or a finished video.
    Raw(audio::Egress),
    Images(Box<images::Egress>),
    Search(search::ResultEgress),
    Video(video::JobEgress),
}

impl ProtocolEgress {
    /// The egress for `output` on the response stream `stream`. `identity` is the attempt's
    /// response identity ([`response_identity`]); `request` is the client's request view,
    /// whose Chat `stream_options.include_usage` decides the usage chunk and whose Responses
    /// fields seed the response echo. `id` is the response id a Responses exchange issued
    /// (`responses_state`), the only id its client sees.
    pub fn new(
        output: Output,
        stream: &mut Writer,
        identity: Identity,
        request: RequestView<'_>,
        id: Option<LocalId>,
    ) -> codec::Result<Self> {
        let mode = match output.framing {
            Framing::Json | Framing::Raw => BodyMode::Buffered,
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
                // An exchange `responses_state::begin` did not prepare shows an id derived from
                // its response stream. The producer is a placeholder nothing reads: provenance
                // is tagged in the request history (D2-6a), not in the client's response.
                let id = match id {
                    Some(id) => id,
                    None => codec::identity::ResponseId::new("resp_", stream.id())?
                        .issued()
                        .ok_or(codec::Error::Schema)?,
                };
                let producer = codec::identity::Producer {
                    provider: LocalId::new("unknown")?,
                    credential: [0; 32],
                };
                let ignore: fn(&'static str, u64) = |_, _| {};
                let mut egress = responses::egress::Egress::new(stream, id, producer, operation, mode, ignore)?;
                if operation != Operation::Compact {
                    egress.echo(stream, request)?;
                }
                Self::Responses(egress)
            }
            Protocol::Speech | Protocol::Images(_) | Protocol::Search | Protocol::Video(_) => {
                Self::media(output, stream, identity)?
            }
            Protocol::Gemini => {
                let framing = match output.framing {
                    Framing::Json | Framing::Raw => gemini::response::Framing::Json,
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

impl ProtocolEgress {
    /// An egress that needs no request view: the speech audio's or a finished video's bytes, the
    /// Images response as one JSON body or its generation or edit events, the search result JSON,
    /// or a video job.
    pub fn media(output: Output, stream: &mut Writer, identity: Identity) -> codec::Result<Self> {
        Ok(match output.protocol {
            Protocol::Images(delivery) => {
                let format = match (output.framing, output.operation) {
                    (Framing::Sse, Operation::Media(codec::Media::ImageEdit)) => images::Format::EditEvents,
                    (Framing::Sse, _) => images::Format::GenerationEvents,
                    _ => images::Format::Json,
                };
                Self::Images(Box::new(images::Egress::new(stream, Some(identity), format, delivery)?))
            }
            Protocol::Search => Self::Search(search::ResultEgress::new(stream, Some(identity))),
            Protocol::Video(_) if output.framing == Framing::Raw => {
                Self::Raw(audio::Egress::of(stream, Some(identity), "video"))
            }
            Protocol::Video(dialect) => Self::Video(video::JobEgress::new(stream, Some(identity), dialect)),
            _ => Self::Raw(audio::Egress::new(stream, Some(identity))),
        })
    }

    /// The client's HTTP status: 202 for a deferred image job xAI is still running, else 200.
    pub fn status(&self) -> u16 {
        match self {
            Self::Images(egress) => egress.status(),
            _ => 200,
        }
    }

    /// Whether the response head is known: a raw media egress learns its `Content-Type` from the
    /// stream's `media.start`, an images egress its status from a job record or its first result,
    /// both before their first byte.
    pub fn typed(&self) -> bool {
        match self {
            Self::Raw(egress) => egress.content_type().is_ok_and(|mime| mime.is_some()),
            Self::Images(egress) => egress.decided(),
            _ => true,
        }
    }

    /// The body's `Content-Type`: the media type the upstream named for raw media, else the
    /// framing's own.
    pub fn content_type(&self, framing: Framing) -> HeaderValue {
        if let Self::Raw(egress) = self
            && let Ok(Some((doc, node))) = egress.content_type()
            && let Ok(mime) = text(doc, node)
            && let Ok(value) = HeaderValue::from_str(&mime)
        {
            return value;
        }
        HeaderValue::from_static(match framing {
            Framing::Sse => "text/event-stream",
            Framing::Raw => "application/octet-stream",
            Framing::Json | Framing::JsonArray => "application/json",
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
            Self::Raw(egress) => egress.advance(owner, input, output),
            Self::Images(egress) => egress.advance(owner, input, output),
            Self::Search(egress) => egress.advance(owner, input, output),
            Self::Video(egress) => egress.advance(owner, input, output),
        }
    }

    fn finish(&mut self) -> codec::Result<()> {
        match self {
            Self::Chat(egress) => egress.finish(),
            Self::Anthropic(egress) => egress.finish(),
            Self::Responses(egress) => egress.finish(),
            Self::Gemini(egress) => egress.finish(),
            Self::Raw(egress) => egress.finish(),
            Self::Images(egress) => egress.finish(),
            Self::Search(egress) => egress.finish(),
            Self::Video(egress) => egress.finish(),
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
    LocalId::new(&text(doc, node)?)
}

/// The string at `node`.
fn text(doc: &Document, node: usize) -> codec::Result<String> {
    let mut text = String::new();
    let mut chars = doc.chars(node)?;
    while let Some(c) = chars.next_scalar()? {
        text.push(c);
    }
    Ok(text)
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
