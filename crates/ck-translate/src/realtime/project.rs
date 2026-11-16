//! The records of the metadata projection. Every value written here is a captured id,
//! enum-like string or count; no media or text payload. Usage, including its native form,
//! is written from the documented numeric fields alone.

use ck_stream::Writer;

use super::{
    Counts,
    envelope::{At, Count, Envelope, N, T, Text},
};
use crate::codec::{
    Error, Result,
    encode::Encoder,
    event::{Emitter, Kind, Scope},
    live::Sender,
};

/// `x_ck.source.ids` members: the root id, else the id of the object the event carries.
const IDS: [(&str, T, Option<T>); 6] = [
    ("event_id", T::EventId, None),
    ("item_id", T::ItemId, Some(T::ItemObjectId)),
    ("response_id", T::ResponseId, Some(T::ResponseObjectId)),
    ("previous_item_id", T::PreviousItemId, None),
    ("call_id", T::CallId, None),
    ("session_id", T::SessionId, None),
];

/// Canonical usage fields (`docs/INTERNAL-STREAM.md` §3.5) and details, from
/// `response.usage`.
const USAGE: [(&str, N); 6] = [
    ("input_tokens", N::Input),
    ("output_tokens", N::Output),
    ("total_tokens", N::Total),
    ("cache_read_tokens", N::Cached),
    ("input_audio_tokens", N::InAudio),
    ("output_audio_tokens", N::OutAudio),
];
const DETAILS: [(&str, N); 3] = [
    ("input_text_tokens", N::InText),
    ("output_text_tokens", N::OutText),
    ("input_image_tokens", N::InImage),
];

/// The documented `response.usage` tree; nothing else of the upstream object is written.
enum Native {
    Count(&'static str, N),
    Object(&'static str, At, &'static [Native]),
}

const NATIVE: &[Native] = &[
    Native::Count("input_tokens", N::Input),
    Native::Count("output_tokens", N::Output),
    Native::Count("total_tokens", N::Total),
    Native::Object(
        "input_token_details",
        At::InputDetails,
        &[
            Native::Count("cached_tokens", N::Cached),
            Native::Count("text_tokens", N::InText),
            Native::Count("audio_tokens", N::InAudio),
            Native::Count("image_tokens", N::InImage),
            Native::Object(
                "cached_tokens_details",
                At::Cached,
                &[
                    Native::Count("text_tokens", N::CachedText),
                    Native::Count("audio_tokens", N::CachedAudio),
                    Native::Count("image_tokens", N::CachedImage),
                ],
            ),
        ],
    ),
    Native::Object(
        "output_token_details",
        At::OutputDetails,
        &[
            Native::Count("text_tokens", N::OutText),
            Native::Count("audio_tokens", N::OutAudio),
        ],
    ),
];

/// Writes the members of `tree` the upstream sent, keeping JSON nulls as nulls.
fn native(e: &mut Encoder<'_, '_>, envelope: &Envelope, tree: &[Native]) -> Result<()> {
    e.bytes(b"{")?;
    let mut first = true;
    let mut name_of = |e: &mut Encoder<'_, '_>, name: &str| {
        e.bytes(if std::mem::take(&mut first) { b"" } else { b"," })?;
        e.string(name)?;
        e.bytes(b":")
    };
    for node in tree {
        match *node {
            Native::Count(name, slot) => match envelope.count(slot) {
                Count::Absent => {}
                Count::Null => {
                    name_of(e, name)?;
                    e.bytes(b"null")?;
                }
                Count::Known(value) => {
                    name_of(e, name)?;
                    e.unsigned(value)?;
                }
            },
            Native::Object(name, at, members) if envelope.entered(at) => {
                name_of(e, name)?;
                native(e, envelope, members)?;
            }
            Native::Object(name, at, _) if envelope.null(at) => {
                name_of(e, name)?;
                e.bytes(b"null")?;
            }
            Native::Object(..) => {}
        }
    }
    e.bytes(b"}")
}

/// One forwarded message's projection.
pub(super) struct Record<'a> {
    pub(super) envelope: &'a Envelope,
    pub(super) event: &'a str,
    pub(super) sender: Sender,
    /// The message's 1-based ordinal among its sender's forwarded messages.
    pub(super) ordinal: u64,
    /// Output audio decoded bytes of the response a `response.done` closes.
    pub(super) output_audio: Option<u64>,
}

impl Record<'_> {
    pub(super) fn publish(&self, emitter: &mut Emitter, owner: &mut Writer) -> Result<()> {
        let source = |e: &mut Encoder<'_, '_>| self.source(e);
        if self.sender == Sender::Upstream && self.event == "response.done" && self.envelope.entered(At::Usage) {
            emitter.emit(owner, Kind::Usage, Scope::default(), |e| self.usage(e), source)?;
        }
        let action = self.control();
        let rates = action.is_none() && self.sender == Sender::Upstream && self.event == "rate_limits.updated";
        let kind = if action.is_some() || rates {
            Kind::Operation
        } else {
            Kind::Opaque
        };
        let payload = |e: &mut Encoder<'_, '_>| match action {
            Some(action) => self.control_value(e, action),
            None if rates => self.rates(e),
            None => self.opaque(e),
        };
        emitter.emit(owner, kind, Scope::default(), payload, source)?;
        Ok(())
    }

    /// The `realtime.control` action of a mapped control message.
    fn control(&self) -> Option<&'static str> {
        Some(match (self.sender, self.event) {
            (Sender::Client, "session.update") => "session.configure",
            (Sender::Upstream, "session.created") => "session.created",
            (Sender::Upstream, "session.updated") => "session.updated",
            (Sender::Client, "response.create") => "response.create",
            (Sender::Client, "response.cancel") => "response.cancel",
            (Sender::Client, "input_audio_buffer.commit") => "audio.commit",
            (Sender::Client, "input_audio_buffer.clear") => "audio.clear",
            (Sender::Client, "session.close") => "session.close",
            _ => return None,
        })
    }

    fn source(&self, e: &mut Encoder<'_, '_>) -> Result<()> {
        e.bytes(b",\"source\":{\"protocol\":\"openai_realtime\"")?;
        if !self.event.is_empty() {
            e.bytes(b",\"event\":")?;
            e.string(self.event)?;
        }
        e.bytes(match self.sender {
            Sender::Client => b",\"sender\":\"client\"",
            Sender::Upstream => b",\"sender\":\"upstream\"",
        })?;
        let mut first = true;
        for (name, root, object) in IDS {
            let id = self.envelope.text(root).get();
            let Some(id) = id.or_else(|| object.and_then(|object| self.envelope.text(object).get())) else {
                continue;
            };
            e.bytes(if first { b",\"ids\":{" } else { b"," })?;
            first = false;
            member_string(e, name, id)?;
        }
        if !first {
            e.bytes(b"}")?;
        }
        for (name, slot) in [("output_index", N::OutputIndex), ("content_index", N::ContentIndex)] {
            if let Some(value) = self.envelope.count(slot).known() {
                e.bytes(b",")?;
                member_unsigned(e, name, value)?;
            }
        }
        e.bytes(b"}")
    }

    fn control_value(&self, e: &mut Encoder<'_, '_>, action: &str) -> Result<()> {
        e.bytes(b",\"kind\":\"realtime.control\",\"value\":{\"action\":")?;
        e.string(action)?;
        match action {
            "session.configure" | "session.created" | "session.updated" => {
                e.bytes(b",\"config\":{")?;
                if let Some(model) = self.envelope.text(T::Model).get() {
                    member_string(e, "model", model)?;
                }
                e.bytes(b"}")?;
            }
            "response.create" => {
                e.bytes(b",\"request_id\":\"")?;
                e.unsigned(self.ordinal)?;
                e.bytes(b"\"")?;
            }
            "response.cancel" => {
                if let Some(id) = self.envelope.text(T::ResponseId).get() {
                    e.bytes(b",")?;
                    member_string(e, "response_id", id)?;
                }
            }
            "audio.commit" | "audio.clear" => e.bytes(b",\"track_id\":\"input\"")?,
            _ => {}
        }
        e.bytes(b"}")
    }

    fn rates(&self, e: &mut Encoder<'_, '_>) -> Result<()> {
        e.bytes(b",\"kind\":\"realtime.rates\",\"value\":{\"limits\":[")?;
        let limits = self.envelope.limits().iter();
        for (index, (limit, (name, remaining))) in limits.filter_map(|l| Some((l, l.complete()?))).enumerate() {
            e.bytes(if index == 0 { b"{" } else { b",{" })?;
            member_string(e, "name", name)?;
            e.bytes(b",")?;
            member_unsigned(e, "remaining", remaining)?;
            if let Some(value) = limit.limit.known() {
                e.bytes(b",")?;
                member_unsigned(e, "limit", value)?;
            }
            if let Some(value) = limit.reset_ms {
                e.bytes(b",")?;
                member_unsigned(e, "reset_after_ms", value)?;
            }
            e.bytes(b"}")?;
        }
        e.bytes(b"]}")
    }

    fn opaque(&self, e: &mut Encoder<'_, '_>) -> Result<()> {
        if !self.event.is_empty() {
            e.bytes(b",\"name\":")?;
            e.string(self.event)?;
        }
        e.bytes(b",\"format\":\"json\",\"value\":{")?;
        let envelope = self.envelope;
        let mut first = true;
        let mut comma = |e: &mut Encoder<'_, '_>| e.bytes(if std::mem::take(&mut first) { b"" } else { b"," });
        for (name, slot) in [("status", T::Status), ("reason", T::Reason)] {
            if let Some(value) = envelope.text(slot).get() {
                comma(e)?;
                member_string(e, name, value)?;
            }
        }
        let error = [
            ("type", envelope.text(T::ErrorType), envelope.text(T::DetailsErrorType)),
            ("code", envelope.text(T::ErrorCode), envelope.text(T::DetailsErrorCode)),
            ("event_id", envelope.text(T::ErrorEventId), &Text::Absent),
        ];
        let error = error.map(|(name, root, details)| (name, root.get().or(details.get())));
        if error.iter().any(|(_, value)| value.is_some()) {
            comma(e)?;
            e.bytes(b"\"error\":{")?;
            let mut inner = true;
            for (name, value) in error {
                if let Some(value) = value {
                    e.bytes(if std::mem::take(&mut inner) { b"" } else { b"," })?;
                    member_string(e, name, value)?;
                }
            }
            e.bytes(b"}")?;
        }
        if let Some(bytes) = self.output_audio {
            comma(e)?;
            member_unsigned(e, "output_audio_bytes", bytes)?;
        }
        e.bytes(b"}")
    }

    fn usage(&self, e: &mut Encoder<'_, '_>) -> Result<()> {
        e.bytes(b",\"usage\":{\"native\":{\"protocol\":\"openai_realtime\",\"value\":")?;
        native(e, self.envelope, NATIVE)?;
        e.bytes(b"},\"final\":true")?;
        let envelope = self.envelope;
        for (name, slot) in USAGE {
            if let Some(value) = envelope.count(slot).known() {
                e.bytes(b",")?;
                member_unsigned(e, name, value)?;
            }
        }
        let derived = match (
            envelope.count(N::Total),
            envelope.count(N::Input),
            envelope.count(N::Output),
        ) {
            (Count::Absent, input, output) => input.known().zip(output.known()),
            _ => None,
        };
        if let Some((input, output)) = derived {
            e.bytes(b",")?;
            member_unsigned(e, "total_tokens", input.checked_add(output).ok_or(Error::Limit)?)?;
        }
        let mut first = true;
        for (name, slot) in DETAILS {
            if let Some(value) = envelope.count(slot).known() {
                e.bytes(if std::mem::take(&mut first) {
                    b",\"details\":{"
                } else {
                    b","
                })?;
                member_unsigned(e, name, value)?;
            }
        }
        if !first {
            e.bytes(b"}")?;
        }
        if derived.is_some() {
            e.bytes(b",\"derived\":[\"total_tokens\"]")?;
        }
        e.bytes(b"}")
    }
}

/// The final record: both senders' counters and the output audio of responses still open.
pub(super) fn counters(
    emitter: &mut Emitter,
    owner: &mut Writer,
    counts: &[Counts; 2],
    responses: &[(Text, u64)],
) -> Result<()> {
    let payload = |e: &mut Encoder<'_, '_>| {
        e.bytes(b",\"kind\":\"realtime.counters\",\"value\":{")?;
        for (index, (name, counts)) in [("client", counts[0]), ("upstream", counts[1])].into_iter().enumerate() {
            e.bytes(if index == 0 { b"\"" } else { b",\"" })?;
            e.bytes(name.as_bytes())?;
            e.bytes(b"\":{")?;
            let fields = [
                ("messages", counts.messages),
                ("bytes", counts.bytes),
                ("media_messages", counts.media_messages),
                ("media_bytes", counts.media_bytes),
                ("decoded_bytes", counts.decoded_bytes),
                ("refused", counts.refused),
                ("unreadable", counts.unreadable),
            ];
            for (index, (name, value)) in fields.into_iter().enumerate() {
                e.bytes(if index == 0 { b"" } else { b"," })?;
                member_unsigned(e, name, value)?;
            }
            e.bytes(b"}")?;
        }
        e.bytes(b",\"responses\":[")?;
        for (index, (id, bytes)) in responses.iter().enumerate() {
            e.bytes(if index == 0 { b"{" } else { b",{" })?;
            member_string(e, "response_id", id.get().unwrap_or_default())?;
            e.bytes(b",")?;
            member_unsigned(e, "output_audio_bytes", *bytes)?;
            e.bytes(b"}")?;
        }
        e.bytes(b"]}")
    };
    emitter.emit(owner, Kind::Operation, Scope::default(), payload, |_| Ok(()))?;
    Ok(())
}

fn member_string(e: &mut Encoder<'_, '_>, name: &str, value: &str) -> Result<()> {
    e.string(name)?;
    e.bytes(b":")?;
    e.string(value)
}

fn member_unsigned(e: &mut Encoder<'_, '_>, name: &str, value: u64) -> Result<()> {
    e.string(name)?;
    e.bytes(b":")?;
    e.unsigned(value)
}
