//! OpenAI Realtime: the read-only projector and checker of a live session
//! (`codec::live::Projector`; S1 live standard §2.2 and §6.7, E10a).
//!
//! Every message, media or control, is forwarded as received by the session loop; nothing
//! here re-encodes a message, decodes base64 or produces client-facing bytes, and nothing is
//! kept for reconnection. [`Session`] reads each message in one linear scan:
//!
//! - [`Session::admit`] checks a client message before it is forwarded (E-Q14): a JSON text
//!   message whose `type` the session admits, no repeated member the gateway reads, the
//!   control fields the gateway reads typed as documented, and `session.model` unchanged.
//!   Media fields are checked by type only.
//! - [`Session::project`] publishes the metadata projection of a forwarded message: event
//!   type, ids, session model, statuses, rate limits and usage. Media messages (audio
//!   appends, audio deltas, reflected sideband audio, binary frames) are only counted, and
//!   text (instructions, transcripts, content, arguments, messages) is never read.
//! - [`Session::finish`] publishes the counters once, as the last record.
//!
//! Records (`docs/INTERNAL-STREAM.md` §4, §5.25-5.26), each with `x_ck.source =
//! {protocol: "openai_realtime", event, sender, ids?, output_index?, content_index?}`:
//!
//! - `response.start` for the session, when it opens;
//! - `operation` `realtime.control` for `session.update` (`session.configure`), upstream
//!   `session.created` and `session.updated`, `response.create` (its `request_id` is the
//!   client message ordinal), `response.cancel`, `input_audio_buffer.commit` and `.clear`
//!   (`audio.commit` and `audio.clear` on track `input`) and sideband `session.close`;
//! - `operation` `realtime.rates` for `rate_limits.updated`;
//! - `usage` for each upstream `response.done` that reports usage, then its `opaque` record;
//! - `opaque` for every other event type, named after it, with status, reason, error type
//!   and code where the event has them;
//! - `operation` `realtime.counters` at the end.
//!
//! Event names follow the OpenAI Realtime reference
//! (<https://developers.openai.com/api/reference/resources/realtime/client-events>,
//! <https://developers.openai.com/api/reference/resources/realtime/server-events>) and the
//! voice server controls guide for sideband commands and reflected audio
//! (<https://developers.openai.com/api/docs/guides/voice-server-controls>).

mod envelope;
mod project;
mod scan;

use ck_stream::Writer;

use crate::{
    boundary,
    codec::{
        Error, Result,
        event::{Emitter, Identity, Kind, LocalId, Scope},
        live::{Opcode, Projector, Refusal, Sender},
    },
};
use envelope::{At, Envelope, N, T, Text};
use scan::{Fault, Reader};

/// Client events of the Realtime API, with the beta `transcription_session.update`.
pub const CLIENT_EVENTS: [&str; 12] = [
    "session.update",
    "transcription_session.update",
    "input_audio_buffer.append",
    "input_audio_buffer.commit",
    "input_audio_buffer.clear",
    "output_audio_buffer.clear",
    "conversation.item.create",
    "conversation.item.retrieve",
    "conversation.item.truncate",
    "conversation.item.delete",
    "response.create",
    "response.cancel",
];

/// Sideband commands of the voice server controls guide (`response.create` is shared).
pub const SIDEBAND_COMMANDS: [&str; 7] = [
    "session.instructions.append",
    "session.thinking.append",
    "session.commentary.append",
    "session.input_audio.mute",
    "session.input_audio.unmute",
    "session.close",
    "response.item.create",
];

/// Events whose payload is media, and whether it is the root `delta` (else root `audio`).
const MEDIA_EVENTS: [(&str, bool); 5] = [
    ("input_audio_buffer.append", false),
    ("session.input_audio.append", false),
    ("response.output_audio.delta", true),
    ("response.audio.delta", true),
    ("session.output_audio.delta", true),
];

/// Responses whose output audio is tallied at once; a session past it fails visibly.
const MAX_RESPONSES: usize = 16;

/// The client event types a session admits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Events {
    /// [`CLIENT_EVENTS`]: a Realtime WebSocket.
    Realtime,
    /// [`CLIENT_EVENTS`] and [`SIDEBAND_COMMANDS`]: a call's sideband.
    Sideband,
}

impl Events {
    fn admits(self, event: &str) -> bool {
        CLIENT_EVENTS.contains(&event) || (self == Self::Sideband && SIDEBAND_COMMANDS.contains(&event))
    }
}

/// The session's admitted configuration, fixed when the upstream session opens.
#[derive(Clone, Copy, Debug)]
pub struct Admission {
    pub events: Events,
    /// The model the session was routed for; `session.update` may not name another.
    pub model: Option<LocalId>,
}

/// The session's `response.start`.
#[derive(Clone, Copy, Debug)]
pub struct Start<'a> {
    pub id: &'a str,
    pub model: &'a str,
    pub created: u64,
}

/// Per-sender counters. Wire bytes are forwarded payload lengths; decoded bytes follow from
/// base64 lengths and padding, and equal the payload length for a binary frame.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Counts {
    pub messages: u64,
    pub bytes: u64,
    pub media_messages: u64,
    pub media_bytes: u64,
    pub decoded_bytes: u64,
    /// Client messages [`Session::admit`] refused; they were not forwarded.
    pub refused: u64,
    /// Forwarded messages the projector could not read; they have no record.
    pub unreadable: u64,
}

/// One OpenAI Realtime session's projector and checker.
pub struct Session {
    owner: u64,
    emitter: Emitter,
    admission: Admission,
    client: Reader,
    upstream: Reader,
    /// The length of the client message `admit` just read, so `project` reuses that scan.
    admitted: Option<usize>,
    counts: [Counts; 2],
    /// Output audio decoded bytes of responses without `response.done` yet.
    responses: Vec<(Text, u64)>,
    closed: bool,
}

impl Session {
    /// Publishes the session's `response.start` to `owner`, the session stream.
    pub fn new(owner: &mut Writer, identity: Identity, start: &Start<'_>, admission: Admission) -> Result<Self> {
        let mut emitter = Emitter::new(identity);
        emitter.emit(
            owner,
            Kind::ResponseStart,
            Scope::default(),
            |e| {
                e.bytes(b",\"id\":")?;
                e.string(start.id)?;
                e.bytes(b",\"model\":")?;
                e.string(start.model)?;
                e.bytes(b",\"created\":")?;
                e.unsigned(start.created)
            },
            |_| Ok(()),
        )?;
        Ok(Self {
            owner: owner.id(),
            emitter,
            admission,
            client: Reader::new(),
            upstream: Reader::new(),
            admitted: None,
            counts: [Counts::default(); 2],
            responses: Vec::new(),
            closed: false,
        })
    }

    pub fn counts(&self, sender: Sender) -> Counts {
        self.counts[sender as usize]
    }

    fn check(&mut self, opcode: Opcode, payload: &[u8]) -> std::result::Result<(), Refusal> {
        if opcode == Opcode::Binary {
            return Err(refusal::BINARY);
        }
        let envelope = self.client.read(payload).map_err(refusal::fault)?;
        let event = match envelope.text(T::Type) {
            Text::Absent | Text::Null => return Err(refusal::missing("type")),
            text => text.get().filter(|event| self.admission.events.admits(event)),
        };
        let event = event.ok_or(refusal::EVENT)?;
        let required: &[(&str, bool)] = match event {
            "input_audio_buffer.append" => &[("audio", envelope.root_audio.is_some())],
            "session.update" => &[("session", envelope.entered(At::Session))],
            "conversation.item.create" => &[("item", envelope.entered(At::Item))],
            "conversation.item.retrieve" | "conversation.item.delete" => {
                &[("item_id", envelope.text(T::ItemId).get().is_some())]
            }
            "conversation.item.truncate" => &[
                ("item_id", envelope.text(T::ItemId).get().is_some()),
                ("content_index", envelope.count(N::ContentIndex).known().is_some()),
                ("audio_end_ms", envelope.count(N::AudioEndMs).known().is_some()),
            ],
            _ => &[],
        };
        if let Some(&(param, _)) = required.iter().find(|(_, present)| !present) {
            return Err(refusal::missing(param));
        }
        let model = envelope.text(T::Model);
        if let (Some(admitted), Text::Value(..) | Text::Unreadable) = (self.admission.model, model)
            && model.get() != Some(admitted.as_str())
        {
            return Err(refusal::MODEL);
        }
        Ok(())
    }
}

/// Tallies an upstream message's output audio against its response, or opens and closes the
/// response's tally; returns the output audio bytes of the response a `response.done` closes.
fn tally(responses: &mut Vec<(Text, u64)>, envelope: &Envelope, event: &str, audio: u64) -> Result<Option<u64>> {
    let (id, close) = match event {
        "response.created" => (envelope.text(T::ResponseObjectId), false),
        "response.done" => (envelope.text(T::ResponseObjectId), true),
        "response.output_audio.delta" | "response.audio.delta" => (envelope.text(T::ResponseId), false),
        _ => return Ok(None),
    };
    if id.get().is_none() {
        return Ok(None);
    }
    let at = responses.iter().position(|(open, _)| open == id);
    match (at, close) {
        (Some(at), true) => return Ok(Some(responses.remove(at).1)),
        (None, true) => {}
        (Some(at), false) => responses[at].1 += audio,
        (None, false) if responses.len() < MAX_RESPONSES => responses.push((*id, audio)),
        (None, false) => return Err(Error::Limit),
    }
    Ok(None)
}

impl Projector for Session {
    fn admit(&mut self, opcode: Opcode, payload: &[u8]) -> std::result::Result<(), Refusal> {
        self.admitted = None;
        let checked = self.check(opcode, payload);
        match checked {
            Ok(()) => self.admitted = Some(payload.len()),
            Err(_) => self.counts[Sender::Client as usize].refused += 1,
        }
        checked
    }

    fn project(&mut self, owner: &mut Writer, sender: Sender, opcode: Opcode, payload: &[u8]) -> Result<()> {
        boundary::enter(self.owner, owner, self.closed)?;
        let length = payload.len() as u64;
        let counts = &mut self.counts[sender as usize];
        counts.messages += 1;
        counts.bytes += length;
        let ordinal = counts.messages;
        if opcode == Opcode::Binary {
            counts.media_messages += 1;
            counts.media_bytes += length;
            counts.decoded_bytes += length;
            return Ok(());
        }
        let reused = sender == Sender::Client && self.admitted.take() == Some(payload.len());
        let reader = match sender {
            Sender::Client => &mut self.client,
            Sender::Upstream => &mut self.upstream,
        };
        if !reused && reader.read(payload).is_err() {
            counts.unreadable += 1;
            return Ok(());
        }
        let envelope = &reader.envelope;
        let event = envelope.text(T::Type).get().unwrap_or("");
        let media = MEDIA_EVENTS.iter().find(|(name, _)| *name == event);
        let delta = match media {
            Some((_, true)) => envelope.delta.map_or(0, |d| d.decoded()),
            _ => 0,
        };
        counts.decoded_bytes += envelope.root_audio.map_or(0, |a| a.decoded()) + delta + envelope.media;
        if media.is_some() {
            counts.media_messages += 1;
            counts.media_bytes += length;
        }
        let output_audio = match sender {
            Sender::Upstream => tally(&mut self.responses, envelope, event, delta)?,
            Sender::Client => None,
        };
        if media.is_some() {
            return Ok(());
        }
        let record = project::Record {
            envelope,
            event,
            sender,
            ordinal,
            output_audio,
        };
        record.publish(&mut self.emitter, owner, payload)
    }

    fn finish(&mut self, owner: &mut Writer) -> Result<()> {
        boundary::enter(self.owner, owner, self.closed)?;
        self.closed = true;
        project::counters(&mut self.emitter, owner, &self.counts, &self.responses)
    }
}

/// The refusals [`Session::admit`] returns.
mod refusal {
    use super::{Fault, Refusal};

    const fn with(code: &'static str, message: &'static str, param: Option<&'static str>) -> Refusal {
        Refusal { code, message, param }
    }

    pub(super) const BINARY: Refusal = with(
        "binary_not_allowed",
        "Binary messages are not Realtime events. Send each event as a JSON text message.",
        None,
    );
    pub(super) const EVENT: Refusal = with(
        "unknown_event_type",
        "This session does not accept this event type. Send a documented client event.",
        Some("type"),
    );
    pub(super) const MODEL: Refusal = with(
        "invalid_value",
        "The session model is fixed when the session opens. Open a new session to use another model.",
        Some("session.model"),
    );

    pub(super) fn missing(param: &'static str) -> Refusal {
        with(
            "missing_required_parameter",
            "The event lacks a required field. Add it and send the event again.",
            Some(param),
        )
    }

    pub(super) fn fault(fault: Fault) -> Refusal {
        match fault {
            Fault::Json => with(
                "invalid_json",
                "The message is not valid JSON. Send each event as one JSON object.",
                None,
            ),
            Fault::NotObject => with("invalid_event", "The event must be a JSON object.", None),
            Fault::TooDeep => with(
                "invalid_event",
                "The event nests more than 32 levels deep. Flatten it and send it again.",
                None,
            ),
            Fault::Duplicate(param) => with(
                "duplicate_parameter",
                "The event repeats a field. Send each field once.",
                Some(param),
            ),
            Fault::Kind(param) => with(
                "invalid_type",
                "A field has the wrong JSON type. Correct it and send the event again.",
                Some(param),
            ),
        }
    }
}
