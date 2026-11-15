//! Live-session messages (Realtime, Live, WebRTC sideband): the read-only projector and
//! validator contract (`docs/INTERNAL-STREAM.md` §5.25-5.26, Settled 55).
//!
//! Every message, media or control, is forwarded as received in one ordered loop with its
//! opcode and payload bytes unchanged. A [`Projector`] only reads it: it never re-encodes a
//! message, decodes base64 or produces client-facing bytes. The internal stream receives
//! the metadata projection (event types, ids, usage, terminal facts, counters) and never a
//! media field. A live-session stream is never resumable.

use ck_stream::Writer;

use super::Result;

/// The side that sent a message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Sender {
    Client,
    Upstream,
}

/// The WebSocket frame type a message arrived with; forwarding keeps it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Opcode {
    Text,
    Binary,
}

/// One live session's projector and validator; one value per session.
pub trait Projector {
    /// Checks one client message against the session's admitted configuration before it is
    /// forwarded (E-Q14): media messages by message type only, other messages by their
    /// control fields, never by decoding a media payload. An `Err` refuses the message: it
    /// is not forwarded, and the client receives the protocol's own `error` event.
    fn admit(&mut self, opcode: Opcode, payload: &[u8]) -> Result<()>;

    /// Publishes the metadata projection of one forwarded message to the session stream.
    /// Media payloads and media fields are only counted, per sender; they never enter the
    /// stream, a scratch range or a media reference.
    fn project(&mut self, owner: &mut Writer, sender: Sender, opcode: Opcode, payload: &[u8]) -> Result<()>;

    /// Publishes the session's counters once, as its final record, when the session ends.
    fn finish(&mut self, owner: &mut Writer) -> Result<()>;
}
