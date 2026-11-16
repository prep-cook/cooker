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

/// Why a client message was refused. The boundary sends these in the protocol's own `error`
/// event; none of them carries message content.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Refusal {
    /// Stable machine-readable reason.
    pub code: &'static str,
    /// What happened and what the client can do instead.
    pub message: &'static str,
    /// The field at fault, as a dotted path, when one is.
    pub param: Option<&'static str>,
}

/// The session runtime's side of a projection (`ck-server` `inference/sessions.rs`): the
/// session stream, and the response lifecycle a projector reads from protocol fields.
///
/// Each response is its own exchange from its upstream start to its end. Its origin comes
/// only from a field that names a client request: a response is the client's when its start
/// names a request the client sent and the upstream has not refused; any other response is
/// the upstream's own (server VAD). Nothing is inferred from message order or counts.
pub trait Route {
    /// The session stream; every projection record goes here.
    fn session(&mut self) -> &mut Writer;

    /// An admitted client message asks for a response and carries `request`, an id a later
    /// response start of this protocol names. Call it only for such a protocol: an entry no
    /// event can name would wait forever. `Err` refuses the message, because the session
    /// already waits on as many requests as it keeps; the projector returns it from `admit`.
    fn requested(&mut self, request: &str) -> std::result::Result<(), Refusal>;

    /// The upstream refused the client request `request`; no response answers it.
    fn rejected(&mut self, request: &str);

    /// The upstream started response `id`. `request` is the client request a field of the
    /// start names, `None` when none does. A repeated start of an open `id` is ignored.
    fn started(&mut self, id: &str, request: Option<&str>);

    /// Response `id` ended (Realtime `response.done`). Unknown ids are ignored.
    fn done(&mut self, id: &str);
}

/// A bare stream is a route that only stores: the lifecycle reaches no runtime. Projector
/// tests and offline readers use it.
impl Route for Writer {
    fn session(&mut self) -> &mut Writer {
        self
    }
    fn requested(&mut self, _: &str) -> std::result::Result<(), Refusal> {
        Ok(())
    }
    fn rejected(&mut self, _: &str) {}
    fn started(&mut self, _: &str, _: Option<&str>) {}
    fn done(&mut self, _: &str) {}
}

/// One live session's projector and validator; one value per session.
pub trait Projector {
    /// The most bytes [`Self::finish`]'s record can take, at most the stream's event limit. The
    /// session runtime holds prefaulted pool frames of this size from the session's start
    /// (`Writer::reserve_final`), so the final counters are never refused for space.
    const FINAL_RECORD_BYTES: usize;

    /// Checks one client message against the session's admitted configuration before it is
    /// forwarded (E-Q14): media messages by message type only, other messages by their
    /// control fields, never by decoding a media payload. A refused message is not
    /// forwarded, and the client receives the protocol's own `error` event built from the
    /// [`Refusal`]. An admitted message is forwarded byte for byte. A message asking
    /// for a response that a later start can name is recorded with [`Route::requested`] here,
    /// before anything is sent.
    fn admit(&mut self, route: &mut impl Route, opcode: Opcode, payload: &[u8]) -> std::result::Result<(), Refusal>;

    /// Publishes the metadata projection of one forwarded message to `route.session()` and
    /// reports the response lifecycle the message carries through `route`. Media payloads and
    /// media fields are only counted, per sender; they never enter the stream, a scratch
    /// range or a media reference.
    fn project(&mut self, route: &mut impl Route, sender: Sender, opcode: Opcode, payload: &[u8]) -> Result<()>;

    /// Publishes the session's counters once, as its final record, when the session ends:
    /// one record of at most [`Self::FINAL_RECORD_BYTES`].
    fn finish(&mut self, owner: &mut Writer) -> Result<()>;
}
