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
use ck_stream::Writer;
use ck_translate::codec::event::Identity;
use ck_translate::codec::{self, Operation, RequestView};

use crate::inference::streams::{Cursor, Exchange, ProtocolEgress, Registry, Replay, Uncommitted, delivery};

/// Client protocols served over the internal stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    Anthropic,
    Chat,
    Responses,
    Gemini,
}

/// Client body framing. `Json` is a buffered body; `Sse` and `JsonArray` stream, the latter
/// as Gemini's `streamGenerateContent` without `alt=sse`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Framing {
    Json,
    Sse,
    JsonArray,
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

/// Delivers one attempt's response stream to the client and commits at its first deliverable
/// byte. `response` is the attempt's allocation handle (`Started::stream`), which the caller
/// keeps to discard a failed attempt; `identity` is the attempt's response identity;
/// `request` is the client's request view; `headers` are the trace, routing and filtered
/// upstream headers sent at commitment.
///
/// Until the egress emits bytes the client gets nothing, and an attempt that ends first is
/// [`Failed`]. At commitment the exchange becomes a registered handle and the response
/// carries `X-Ck-Stream-Id`. Afterwards the client body only reads the stream: a slow or
/// failed write never fails over and never slows the producer.
pub async fn deliver(
    streams: &Registry,
    exchange: Exchange,
    response: &Writer,
    identity: Identity,
    request: RequestView<'_>,
    headers: HeaderMap,
) -> Result<Response, Box<Failed>> {
    let output = exchange.output;
    let reader = streams
        .with_store(|store| store.reader(response.id(), 0))
        .and_then(|reader| reader)
        .map_err(codec::Error::Storage);
    let mut egress = response.allocator();
    let replay = reader.and_then(|reader| {
        let protocol = ProtocolEgress::new(output, &mut egress, identity, request)?;
        Ok(Replay::live(egress, reader, protocol, output.framing))
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
    let id = exchange.commit(streams, response.allocator());
    let mut delivered = delivery(replay, first, output.framing, &id, Cursor::start(output.framing));
    let own = std::mem::replace(delivered.headers_mut(), headers);
    delivered.headers_mut().extend(own);
    Ok(delivered)
}
