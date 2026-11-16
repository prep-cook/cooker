//! Client response egress (`docs/EXECUTION-CONTRACT.md`). The client protocol's
//! `ResponseEgress` replaces the route writers in `respond.rs`.
//!
//! Commitment is the first deliverable byte the client codec emits for a response. Before
//! it, nothing reaches the client: no status line, success header or keepalive, and an
//! attempt failure may fail over to a fresh attempt. Once deliverable bytes are scheduled
//! to the client, failover is disabled even if the socket write then fails, and a later
//! failure reaches the client as the stream's `error` terminal in its own protocol.

use axum::http::HeaderMap;
use axum::response::Response;
use ck_stream::{Terminal, Writer};
use ck_translate::codec::event::Identity;
use ck_translate::codec::{self, Operation, RequestView};
use futures_util::FutureExt;

use crate::inference::streams::{Cursor, Exchange, ProtocolEgress, Registry, Replay, Uncommitted, delivery};

/// Client protocols served over the internal stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    Anthropic,
    Chat,
    Responses,
    Gemini,
    /// OpenAI speech: the audio file itself.
    Speech,
    /// OpenAI Images, with results as base64 or URLs, as the client asked.
    Images(ck_translate::images::Delivery),
}

/// Client body framing. `Json` is a buffered body; `Sse` and `JsonArray` stream, the latter
/// as Gemini's `streamGenerateContent` without `alt=sse`. `Raw` is a media file streamed as
/// it arrives, with the upstream's media type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Framing {
    Json,
    Sse,
    JsonArray,
    Raw,
}

/// What response egress encodes with. Fixed when the exchange starts; a resume reuses it
/// and cannot replace it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Output {
    pub protocol: Protocol,
    pub operation: Operation,
    pub framing: Framing,
}

/// An attempt that ended before its first deliverable byte, with the exchange it leaves for
/// another attempt. Nothing reached the client.
pub struct Failed {
    pub exchange: Exchange,
    pub reason: Uncommitted,
}

/// One attempt's client protocol egress, built before its executor starts. The egress reads
/// the client's request view (Chat `stream_options.include_usage`, the Responses echo) while
/// the attempt still holds the decoded document, which the executor may release before the
/// upstream answers. An error surfaces at [`deliver`] as an uncommitted codec failure.
pub struct Prepared(codec::Result<(Writer, ProtocolEgress)>);

/// The [`Prepared`] egress of the attempt writing `response`; `identity` is the attempt's
/// response identity and `request` the client's request view.
pub fn prepare(exchange: &Exchange, response: &Writer, identity: Identity, request: RequestView<'_>) -> Prepared {
    let mut egress = response.allocator();
    let shown = exchange.turn.map(|turn| turn.id.text());
    let protocol = ProtocolEgress::new(exchange.output, &mut egress, identity, request, shown);
    Prepared(protocol.map(|protocol| (egress, protocol)))
}

/// Delivers one attempt's response stream to the client and commits at its first deliverable
/// byte. `response` is the attempt's allocation handle (`Started::stream`), which the caller
/// keeps to discard a failed attempt; `egress` is the attempt's [`prepare`]d egress; `headers`
/// are the trace, routing and filtered upstream headers sent at commitment.
///
/// Until the egress emits bytes the client gets nothing, and an attempt that ends first is
/// [`Failed`]. At commitment the exchange becomes a registered handle and the response
/// carries `X-Ck-Stream-Id`. Afterwards the client body only reads the stream: a slow or
/// failed write never fails over and never slows the producer.
pub async fn deliver(
    streams: &Registry,
    exchange: Exchange,
    response: &Writer,
    egress: Prepared,
    headers: HeaderMap,
) -> Result<Response, Box<Failed>> {
    let output = exchange.output;
    let reader = streams
        .with_store(|store| store.reader(response.id(), 0))
        .and_then(|reader| reader)
        .map_err(codec::Error::Storage);
    let publication = exchange.publication.clone();
    let replay = reader.and_then(|reader| {
        let (egress, protocol) = egress.0?;
        Ok(Replay::live(egress, reader, protocol, output.framing, publication))
    });
    let mut replay = match replay {
        Ok(replay) => replay,
        Err(error) => {
            return Err(Box::new(Failed {
                exchange,
                reason: Uncommitted::Codec(error),
            }));
        }
    };
    let first = match replay.first().await {
        Ok(first) => first,
        Err(reason) => return Err(Box::new(Failed { exchange, reason })),
    };
    // A producer that already failed while the first output was encoded loses the race:
    // its partial response does not commit.
    if let Some(terminal) = response.cancelled().now_or_never()
        && terminal != Terminal::Done
    {
        let reason = Uncommitted::Terminal(terminal);
        return Err(Box::new(Failed { exchange, reason }));
    }
    // A Responses turn's index slot is reserved at commitment (D2-PLAN §5.1 step 2).
    if let Some(publication) = &exchange.publication {
        publication.reserve();
    }
    let id = exchange.commit(streams, response.allocator());
    let mut delivered = delivery(replay, first, output.framing, &id, Cursor::start(output.framing));
    let own = std::mem::replace(delivered.headers_mut(), headers);
    delivered.headers_mut().extend(own);
    Ok(delivered)
}
