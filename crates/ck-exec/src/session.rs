//! Duplex session contract (`docs/EXECUTION-CONTRACT.md` §12): the upstream half of the
//! Responses WebSocket and of live sessions (Realtime, Live, WebRTC sideband). Separate from
//! [`Execute`] and statically dispatched like it. Neither kind resumes or reattaches.
//!
//! - Responses WebSocket: every turn is one ordinary attempt under [`Execute`]'s capability,
//!   fence and ingress, and its upstream frames feed that attempt's producer. A client
//!   disconnect cancels the in-flight turn (Settled 56); history already accepted stays.
//! - Live sessions: every message, media or control, is forwarded as received with its opcode
//!   and payload unchanged, in one ordered loop per direction. Only the metadata projection
//!   (`ck_translate::codec::live::Projector`) enters the internal stream. A client disconnect
//!   closes the upstream session (Settled 55).

use std::future::Future;

use bytes::Bytes;
use ck_core::exec::ExecError;
use ck_translate::codec::live::Opcode;
use futures_util::stream::BoxStream;

use crate::types::{Attempt, Client, Execute, Request, Streams, Upstream};

/// One WebSocket message as received. Text payloads are the UTF-8 the transport validated.
/// Ping and pong stay inside each transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Frame {
    Text(Bytes),
    Binary(Bytes),
    /// A close frame; `None` when it carried no status code.
    Close(Option<Close>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Close {
    pub code: u16,
    /// At most 123 bytes: RFC 6455 §5.5 limits a close frame's payload to 125 bytes.
    pub reason: Bytes,
}

impl Frame {
    /// The opcode and payload a projector reads; `None` for a close frame.
    pub fn message(&self) -> Option<(Opcode, &[u8])> {
        match self {
            Self::Text(payload) => Some((Opcode::Text, payload)),
            Self::Binary(payload) => Some((Opcode::Binary, payload)),
            Self::Close(_) => None,
        }
    }
}

/// Upstream messages in arrival order. The stream ends after a [`Frame::Close`] when the
/// upstream closed, and without one when the connection was lost.
pub type Frames = BoxStream<'static, Frame>;

/// The client-to-upstream half of an open live session.
pub trait FrameSink: Send + 'static {
    /// Writes `frame` upstream unchanged; [`Frame::Close`] closes the upstream session.
    fn send(&mut self, frame: Frame) -> impl Future<Output = Result<(), ExecError>> + Send;
}

/// An open live session: both halves, pinned to the attempt's credential for its lifetime.
pub struct LiveUpstream<S> {
    pub sink: S,
    pub frames: Frames,
}

/// A provider's live-session transport. Implementations hold no per-session state.
pub trait Live: Send + Sync {
    type Sink: FrameSink;

    /// Dials the upstream session for `attempt`'s credential and model. Credentials are chosen
    /// or rotated only before this returns (E-Q10); a later loss closes the session with an
    /// error instead of reopening it on another credential.
    fn open(
        &self,
        attempt: &Attempt<'_>,
        client: &Client<'_>,
    ) -> impl Future<Output = Result<LiveUpstream<Self::Sink>, ExecError>> + Send;
}

/// Responses WebSocket turns over an upstream socket (Codex, xAI). A provider without one
/// runs each turn through [`Execute::execute`], and dropping the turn's body cancels it.
pub trait Turns: Execute {
    type Cancel: CancelTurn;

    /// [`Execute::execute`] for one turn of `Operation::Generate` on an upstream socket. The
    /// request carries the expanded history, so the turn fails over to any candidate.
    fn turn(
        &self,
        attempt: &Attempt<'_>,
        request: &mut Request<'_>,
        streams: Streams<'_>,
    ) -> impl Future<Output = Result<Turn<Self::Ingress, Self::Cancel>, ExecError>> + Send;
}

pub struct Turn<I, C> {
    pub upstream: Upstream<I>,
    pub cancel: C,
}

/// Cancels one in-flight turn when its client disconnects (Settled 56).
pub trait CancelTurn: Send + 'static {
    /// Sends the transport's cancel command when it has one, otherwise closes the upstream
    /// socket; a pooled socket never returns to its pool with the turn in flight.
    fn cancel(self) -> impl Future<Output = ()> + Send;
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;

    use super::*;

    struct Loopback;
    impl FrameSink for Loopback {
        async fn send(&mut self, _: Frame) -> Result<(), ExecError> {
            Ok(())
        }
    }

    struct Dialer;
    impl Live for Dialer {
        type Sink = Loopback;
        async fn open(&self, _: &Attempt<'_>, _: &Client<'_>) -> Result<LiveUpstream<Loopback>, ExecError> {
            Ok(LiveUpstream {
                sink: Loopback,
                frames: futures_util::stream::empty().boxed(),
            })
        }
    }

    // A transport whose futures are `Send` satisfies the contract with plain `async fn`.
    const _: fn() = || {
        fn live<L: Live>() {}
        live::<Dialer>();
    };

    #[test]
    fn frames_expose_opcode_and_payload_unchanged() {
        let payload = Bytes::from_static(b"{ \"type\" :\"input_audio_buffer.append\",\"audio\":\"AA==\"}");
        let close = Close {
            code: 1000,
            reason: Bytes::new(),
        };
        let cases = [
            (Frame::Text(payload.clone()), Some((Opcode::Text, &payload[..]))),
            (Frame::Binary(payload.clone()), Some((Opcode::Binary, &payload))),
            (Frame::Close(Some(close)), None),
            (Frame::Close(None), None),
        ];
        for (frame, expected) in cases {
            assert_eq!(frame.message(), expected, "{frame:?}");
        }
    }
}
