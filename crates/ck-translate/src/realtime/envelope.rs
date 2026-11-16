//! What one scan of a message keeps: the envelope, the control fields the gateway reads and
//! the size of each media field. Nothing here holds a media or text payload.

use super::{scan::Fault, schema::SUBTREES};

/// A captured string of at most [`Text::MAX`] printable ASCII bytes. Any other string is
/// `Unreadable`: projection omits it and checks match it against nothing.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum Text {
    #[default]
    Absent,
    Null,
    Unreadable,
    Value([u8; Text::MAX], u8),
}

impl Text {
    pub(crate) const MAX: usize = 64;

    pub(crate) fn get(&self) -> Option<&str> {
        match self {
            // Only printable ASCII is ever stored.
            Self::Value(bytes, len) => std::str::from_utf8(&bytes[..usize::from(*len)]).ok(),
            _ => None,
        }
    }

    pub(super) fn push(&mut self, c: char) {
        if let Self::Value(bytes, len) = self {
            let at = usize::from(*len);
            if (' '..='~').contains(&c) && at < Self::MAX {
                bytes[at] = c as u8;
                *len += 1;
            } else {
                *self = Self::Unreadable;
            }
        }
    }
}

/// A captured non-negative integer; `Null` is a JSON null, never zero.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum Count {
    #[default]
    Absent,
    Null,
    Known(u64),
}

impl Count {
    pub(crate) fn known(self) -> Option<u64> {
        match self {
            Self::Known(value) => Some(value),
            _ => None,
        }
    }
}

/// Captured strings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum T {
    Type,
    EventId,
    ItemId,
    ResponseId,
    PreviousItemId,
    CallId,
    SessionId,
    ItemObjectId,
    ResponseObjectId,
    Model,
    Status,
    Reason,
    ErrorType,
    ErrorCode,
    ErrorEventId,
    DetailsErrorType,
    DetailsErrorCode,
}
const TEXTS: usize = T::DetailsErrorCode as usize + 1;

/// Captured counts: indexes, truncation point and usage (`response.usage` or root `usage`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum N {
    ContentIndex,
    OutputIndex,
    AudioEndMs,
    Input,
    Output,
    Total,
    Cached,
    InText,
    InAudio,
    InImage,
    CachedText,
    CachedAudio,
    CachedImage,
    OutText,
    OutAudio,
}
const COUNTS: usize = N::OutAudio as usize + 1;

/// The object or array a value sits in. Containers the gateway does not read are `Other`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum At {
    Root,
    Session,
    Item,
    Response,
    Details,
    DetailsError,
    Usage,
    InputDetails,
    Cached,
    OutputDetails,
    Error,
    Limits,
    Limit,
    Other,
}

/// What a member's value is to the reader.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Role {
    Text(T),
    Count(N),
    /// An object, or an array for [`At::Limits`].
    Enter(At),
    /// Root `audio`: the media of `input_audio_buffer.append` and reflected input audio.
    RootAudio,
    /// Base64 media anywhere else: `audio` of an input or output audio content part.
    Audio,
    /// `image_url`: media only as a `data:` URI.
    Image,
    /// Root `delta`: media for audio delta events, text otherwise.
    Delta,
    LimitName,
    LimitLimit,
    LimitRemaining,
    LimitReset,
    /// An element of `rate_limits`.
    LimitItem,
    Skip,
}

/// Members the gateway reads, by container. The index is the member's duplicate bit; the
/// last column is its dotted path for refusals.
pub(crate) const FIELDS: [(At, &str, Role, &str); 46] = [
    (At::Root, "type", Role::Text(T::Type), "type"),
    (At::Root, "event_id", Role::Text(T::EventId), "event_id"),
    (At::Root, "item_id", Role::Text(T::ItemId), "item_id"),
    (At::Root, "response_id", Role::Text(T::ResponseId), "response_id"),
    (
        At::Root,
        "previous_item_id",
        Role::Text(T::PreviousItemId),
        "previous_item_id",
    ),
    (At::Root, "call_id", Role::Text(T::CallId), "call_id"),
    (At::Root, "content_index", Role::Count(N::ContentIndex), "content_index"),
    (At::Root, "output_index", Role::Count(N::OutputIndex), "output_index"),
    (At::Root, "audio_end_ms", Role::Count(N::AudioEndMs), "audio_end_ms"),
    (At::Root, "audio", Role::RootAudio, "audio"),
    (At::Root, "delta", Role::Delta, "delta"),
    (At::Root, "session", Role::Enter(At::Session), "session"),
    (At::Root, "item", Role::Enter(At::Item), "item"),
    (At::Root, "response", Role::Enter(At::Response), "response"),
    (At::Root, "error", Role::Enter(At::Error), "error"),
    (At::Root, "rate_limits", Role::Enter(At::Limits), "rate_limits"),
    // A completed input audio transcription's usage; read with `response.usage`'s members.
    (At::Root, "usage", Role::Enter(At::Usage), "usage"),
    (At::Session, "id", Role::Text(T::SessionId), "session.id"),
    (At::Session, "model", Role::Text(T::Model), "session.model"),
    (At::Item, "id", Role::Text(T::ItemObjectId), "item.id"),
    (At::Response, "id", Role::Text(T::ResponseObjectId), "response.id"),
    (At::Response, "status", Role::Text(T::Status), "response.status"),
    (
        At::Response,
        "status_details",
        Role::Enter(At::Details),
        "response.status_details",
    ),
    (At::Response, "usage", Role::Enter(At::Usage), "response.usage"),
    (
        At::Details,
        "reason",
        Role::Text(T::Reason),
        "response.status_details.reason",
    ),
    (
        At::Details,
        "error",
        Role::Enter(At::DetailsError),
        "response.status_details.error",
    ),
    (
        At::DetailsError,
        "type",
        Role::Text(T::DetailsErrorType),
        "response.status_details.error.type",
    ),
    (
        At::DetailsError,
        "code",
        Role::Text(T::DetailsErrorCode),
        "response.status_details.error.code",
    ),
    (
        At::Usage,
        "input_tokens",
        Role::Count(N::Input),
        "response.usage.input_tokens",
    ),
    (
        At::Usage,
        "output_tokens",
        Role::Count(N::Output),
        "response.usage.output_tokens",
    ),
    (
        At::Usage,
        "total_tokens",
        Role::Count(N::Total),
        "response.usage.total_tokens",
    ),
    (
        At::Usage,
        "input_token_details",
        Role::Enter(At::InputDetails),
        "response.usage.input_token_details",
    ),
    (
        At::Usage,
        "output_token_details",
        Role::Enter(At::OutputDetails),
        "response.usage.output_token_details",
    ),
    (
        At::InputDetails,
        "cached_tokens",
        Role::Count(N::Cached),
        "response.usage.input_token_details.cached_tokens",
    ),
    (
        At::InputDetails,
        "text_tokens",
        Role::Count(N::InText),
        "response.usage.input_token_details.text_tokens",
    ),
    (
        At::InputDetails,
        "audio_tokens",
        Role::Count(N::InAudio),
        "response.usage.input_token_details.audio_tokens",
    ),
    (
        At::InputDetails,
        "image_tokens",
        Role::Count(N::InImage),
        "response.usage.input_token_details.image_tokens",
    ),
    (
        At::InputDetails,
        "cached_tokens_details",
        Role::Enter(At::Cached),
        "response.usage.input_token_details.cached_tokens_details",
    ),
    (
        At::Cached,
        "text_tokens",
        Role::Count(N::CachedText),
        "response.usage.input_token_details.cached_tokens_details.text_tokens",
    ),
    (
        At::Cached,
        "audio_tokens",
        Role::Count(N::CachedAudio),
        "response.usage.input_token_details.cached_tokens_details.audio_tokens",
    ),
    (
        At::Cached,
        "image_tokens",
        Role::Count(N::CachedImage),
        "response.usage.input_token_details.cached_tokens_details.image_tokens",
    ),
    (
        At::OutputDetails,
        "text_tokens",
        Role::Count(N::OutText),
        "response.usage.output_token_details.text_tokens",
    ),
    (
        At::OutputDetails,
        "audio_tokens",
        Role::Count(N::OutAudio),
        "response.usage.output_token_details.audio_tokens",
    ),
    (At::Error, "type", Role::Text(T::ErrorType), "error.type"),
    (At::Error, "code", Role::Text(T::ErrorCode), "error.code"),
    (At::Error, "event_id", Role::Text(T::ErrorEventId), "error.event_id"),
];

/// `rate_limits[]` members, read per element; a repeated member overwrites the earlier one.
const LIMIT_FIELDS: [(&str, Role); 4] = [
    ("name", Role::LimitName),
    ("limit", Role::LimitLimit),
    ("remaining", Role::LimitRemaining),
    ("reset_seconds", Role::LimitReset),
];

/// The member `key` of a container at `at`: its role and, for a [`FIELDS`] member, its index.
pub(crate) fn member(at: At, key: &[u8]) -> (Role, Option<usize>) {
    if let Some(index) = FIELDS.iter().position(|&(a, k, ..)| a == at && k.as_bytes() == key) {
        return (FIELDS[index].2, Some(index));
    }
    let role = match (at, key) {
        (At::Limit, _) => LIMIT_FIELDS
            .iter()
            .find(|(k, _)| k.as_bytes() == key)
            .map_or(Role::Skip, |&(_, role)| role),
        (_, b"audio") => Role::Audio,
        (_, b"image_url") => Role::Image,
        _ => Role::Skip,
    };
    (role, None)
}

/// One `rate_limits[]` element.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct Limit {
    pub(crate) name: Text,
    pub(crate) limit: Count,
    pub(crate) remaining: Count,
    pub(crate) reset_ms: Option<u64>,
}

impl Limit {
    /// The name and remaining count, which `realtime.rates` requires.
    pub(crate) fn complete(&self) -> Option<(&str, u64)> {
        Some((self.name.get()?, self.remaining.known()?))
    }
}

/// The base64 characters of a media field and its trailing `=` count, from which the
/// decoded length follows without decoding (RFC 4648 §4).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct Base64 {
    pub(crate) chars: u64,
    pub(crate) padding: u64,
}

impl Base64 {
    pub(crate) fn decoded(self) -> u64 {
        self.chars.saturating_sub(self.padding).saturating_mul(3) / 4
    }
}

/// At most this many `rate_limits` elements are read; the rest are skipped.
pub(crate) const MAX_LIMITS: usize = 4;

/// One message's envelope and control fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Envelope {
    pub(crate) texts: [Text; TEXTS],
    pub(crate) counts: [Count; COUNTS],
    pub(crate) limits: [Limit; MAX_LIMITS],
    pub(crate) limit_count: usize,
    /// Containers read as objects (or `rate_limits` as an array), by [`At`] bit.
    pub(crate) entered: u16,
    /// Containers read as JSON null, by [`At`] bit.
    pub(crate) nulls: u16,
    /// The first schema fault inside each checked subtree (`schema::SUBTREES`).
    pub(crate) subtree: [Option<Fault>; SUBTREES.len()],
    /// Root `audio` as a string.
    pub(crate) root_audio: Option<Base64>,
    /// Root `delta` as a string; media only for audio delta events.
    pub(crate) delta: Option<Base64>,
    /// Decoded bytes of every other media field.
    pub(crate) media: u64,
}

impl Default for Envelope {
    fn default() -> Self {
        Self {
            texts: [Text::Absent; TEXTS],
            counts: [Count::Absent; COUNTS],
            limits: [Limit::default(); MAX_LIMITS],
            limit_count: 0,
            entered: 0,
            nulls: 0,
            subtree: [None; SUBTREES.len()],
            root_audio: None,
            delta: None,
            media: 0,
        }
    }
}

impl Envelope {
    pub(crate) fn text(&self, slot: T) -> &Text {
        &self.texts[slot as usize]
    }
    pub(crate) fn count(&self, slot: N) -> Count {
        self.counts[slot as usize]
    }
    pub(crate) fn entered(&self, at: At) -> bool {
        self.entered & (1 << at as u16) != 0
    }
    pub(crate) fn null(&self, at: At) -> bool {
        self.nulls & (1 << at as u16) != 0
    }
    pub(crate) fn limits(&self) -> &[Limit] {
        &self.limits[..self.limit_count]
    }
}
