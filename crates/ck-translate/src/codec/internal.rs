use super::{
    Error, Result,
    document::{Document, Value},
    event::{Direction, Identity, Kind},
    source::{Source, Span},
};
use ck_stream::{Lease, Position, Record, Writer};

const KINDS: &[Kind] = &[
    Kind::Chunk,
    Kind::RequestStart,
    Kind::RequestField,
    Kind::RequestEnd,
    Kind::ResponseStart,
    Kind::ItemStart,
    Kind::ItemEnd,
    Kind::BlockStart,
    Kind::BlockEnd,
    Kind::ReasoningSignature,
    Kind::ReasoningRedacted,
    Kind::ToolStart,
    Kind::ToolEnd,
    Kind::ToolResult,
    Kind::Annotation,
    Kind::Grounding,
    Kind::CodeExecution,
    Kind::UrlContext,
    Kind::Usage,
    Kind::CountTokens,
    Kind::Compact,
    Kind::MediaStart,
    Kind::Data,
    Kind::MediaTranscript,
    Kind::MediaEnd,
    Kind::Operation,
    Kind::Opaque,
    Kind::Heartbeat,
    Kind::Done,
    Kind::Error,
    Kind::Cancelled,
];

/// Decode a trusted canonical lease, retaining its bytes rather than copying SSE data.
/// Wire ingress must never call this function on externally assigned record kinds.
pub fn decode(owner: &mut Writer, lease: Lease) -> Result<(Kind, Document)> {
    read(owner, lease).map(|(kind, _, doc)| (kind, doc))
}

/// The correlation members of one record's `x_ck`, found in one walk over its members.
#[derive(Default)]
struct Envelope {
    seq: u64,
    request_id: Option<usize>,
    stream_id: Option<usize>,
    attempt: Option<usize>,
    direction: Option<usize>,
}

/// [`decode`], also returning the record's correlation members.
fn read(owner: &mut Writer, lease: Lease) -> Result<(Kind, Envelope, Document)> {
    let record = lease.record();
    if record.start.generation != owner.id() {
        return Err(Error::Position);
    }
    let kind = *KINDS.get(record.kind as usize).ok_or(Error::Schema)?;
    let mut frame = Frame::new();
    lease.visit(|chunk| frame.chunk(kind, chunk))?;
    let start = frame.data_start.ok_or(Error::Schema)?;
    let end = frame.data_end.ok_or(Error::Truncated)?;
    if frame.invalid || end + 2 != record.len() || frame.name_seen != kind.name().is_some() {
        return Err(Error::Schema);
    }
    let mut source = Source::new(owner)?;
    source.push(lease, advance(record.start, start)?..advance(record.start, end)?)?;
    let mut doc = Document::parse_internal(owner, source, record.len())?;
    let mut envelope = Envelope::default();
    let (mut v, mut seq) = (None, None);
    for child in doc.children(doc.required(doc.root(), "x_ck")?)? {
        let name = doc.node(child)?.name.ok_or(Error::Schema)?;
        let slot = if doc.name_is(name, "v")? {
            &mut v
        } else if doc.name_is(name, "seq")? {
            &mut seq
        } else if doc.name_is(name, "request_id")? {
            &mut envelope.request_id
        } else if doc.name_is(name, "stream_id")? {
            &mut envelope.stream_id
        } else if doc.name_is(name, "attempt")? {
            &mut envelope.attempt
        } else if doc.name_is(name, "direction")? {
            &mut envelope.direction
        } else {
            continue;
        };
        slot.get_or_insert(child);
    }
    envelope.seq = doc.unsigned(seq.ok_or(Error::Schema)?)?;
    if doc.unsigned(v.ok_or(Error::Schema)?)? != 1 || Some(envelope.seq) != frame.sequence {
        return Err(Error::Schema);
    }
    if kind == Kind::Chunk {
        super::chat::prepare_response(&mut doc)?;
    }
    Ok((kind, envelope, doc))
}

/// Checks every record of one canonical response stream an egress reads: it comes from the
/// writer generation the egress was built on with the next `seq`, and carries the `request_id`,
/// `stream_id` and `attempt` of the identity the egress was built with, or, for an egress built
/// without one, of the stream's first record, with `direction` `response`
/// (docs/INTERNAL-STREAM.md, §3.2). Any holder of the writer can append a record, so no record's
/// identity is taken from an earlier one. Ingress replay of published records uses [`decode`].
pub(crate) struct Binding {
    generation: u64,
    identity: Option<Identity>,
    next: u64,
}

impl Binding {
    pub(crate) fn new(owner: &Writer, identity: Option<Identity>) -> Self {
        Self {
            generation: owner.id(),
            identity,
            next: 0,
        }
    }

    /// Decodes the next record of the bound stream.
    pub(crate) fn decode(&mut self, owner: &mut Writer, lease: Lease) -> Result<(Kind, Document)> {
        if owner.id() != self.generation {
            return Err(Error::Position);
        }
        let (kind, envelope, doc) = read(owner, lease)?;
        if envelope.seq != self.next {
            return Err(Error::Schema);
        }
        let field = |slot: Option<usize>| slot.ok_or(Error::Schema);
        let identity = match self.identity {
            Some(identity) => identity,
            None => *self.identity.insert(Identity {
                request_id: super::identity::local(&doc, field(envelope.request_id)?)?,
                stream_id: super::identity::local(&doc, field(envelope.stream_id)?)?,
                attempt: doc.unsigned(field(envelope.attempt)?)?,
                direction: Direction::Response,
            }),
        };
        if doc.unsigned(field(envelope.attempt)?)? != identity.attempt
            || !string_equals(&doc, field(envelope.request_id)?, identity.request_id.as_str())?
            || !string_equals(&doc, field(envelope.stream_id)?, identity.stream_id.as_str())?
            || !string_equals(&doc, field(envelope.direction)?, "response")?
        {
            return Err(Error::Schema);
        }
        self.next = envelope.seq.checked_add(1).ok_or(Error::Limit)?;
        Ok((kind, doc))
    }
}

/// Whether string `node` is `value`: one byte comparison when the stored string has no escapes.
/// Bytes equal to a `value` without a backslash hold no escape, so they decode to `value`.
fn string_equals(doc: &Document, node: usize, value: &str) -> Result<bool> {
    if let Value::String(text) = doc.node(node)?.value
        && !value.contains('\\')
        && text.span.len() == value.len() as u64 + 2
        && doc.source(text)?.equals(
            Span {
                start: text.span.start + 1,
                end: text.span.end - 1,
            },
            value.as_bytes(),
        )?
    {
        return Ok(true);
    }
    doc.string_is(node, value)
}

/// SSE framing of one canonical record, scanned one byte at a time.
struct Frame {
    header: [u8; 96],
    header_len: usize,
    offset: u64,
    data_start: Option<u64>,
    data_end: Option<u64>,
    sequence: Option<u64>,
    name_seen: bool,
    line_start: u64,
    invalid: bool,
}

impl Frame {
    fn new() -> Self {
        Self {
            header: [0; 96],
            header_len: 0,
            offset: 0,
            data_start: None,
            data_end: None,
            sequence: None,
            name_seen: false,
            line_start: 0,
            invalid: false,
        }
    }

    /// Feeds `bytes`, stepping over the data line's payload, which only a newline ends.
    fn chunk(&mut self, kind: Kind, mut bytes: &[u8]) {
        while let Some((&byte, rest)) = bytes.split_first() {
            if self.data_start.is_some() && self.data_end.is_none() && byte != b'\n' {
                let payload = bytes.iter().position(|&b| b == b'\n').unwrap_or(bytes.len());
                self.offset += payload as u64;
                bytes = &bytes[payload..];
                continue;
            }
            self.byte(kind, byte);
            bytes = rest;
        }
    }

    fn byte(&mut self, kind: Kind, byte: u8) {
        if self.data_start.is_some() {
            if byte == b'\n' {
                if let Some(end) = self.data_end {
                    if self.offset != end + 1 {
                        self.invalid = true;
                    }
                } else {
                    self.data_end = Some(self.offset);
                }
            } else if self.data_end.is_some() {
                self.invalid = true;
            }
        } else if byte == b'\n' {
            self.header_line(kind);
            self.header_len = 0;
            self.line_start = self.offset + 1;
        } else {
            if self.header_len == self.header.len() {
                self.invalid = true;
            } else {
                self.header[self.header_len] = byte;
                self.header_len += 1;
            }
            if self.header_len == 6 && &self.header[..6] == b"data: " {
                self.data_start = Some(self.line_start + 6);
            }
        }
        self.offset += 1;
    }

    fn header_line(&mut self, kind: Kind) {
        let line = &self.header[..self.header_len];
        if let Some(name) = line.strip_prefix(b"event: ") {
            if self.name_seen || kind.name().is_none_or(|expected| name != expected.as_bytes()) {
                self.invalid = true;
            }
            self.name_seen = true;
        } else if let Some(id) = line.strip_prefix(b"id: ") {
            let mut parsed = Some(0u64);
            if id.is_empty() {
                self.invalid = true;
            }
            for &b in id {
                parsed = if b.is_ascii_digit() {
                    parsed
                        .and_then(|n| n.checked_mul(10))
                        .and_then(|n| n.checked_add(u64::from(b - b'0')))
                } else {
                    None
                };
            }
            if self.sequence.is_some() || parsed.is_none() {
                self.invalid = true;
            }
            self.sequence = parsed;
        } else {
            self.invalid = true;
        }
    }
}

/// Refuses a Gemini part that carries media by URI (`fileData`) at a client egress that is not
/// Gemini's: the canonical stream has no URI media record, and only Gemini egress restores the
/// part from its originals.
/// ponytail: a refusal rather than a URI media carrier; add one to INTERNAL-STREAM §3.3 to map
/// such parts to other protocols.
pub(crate) fn refuse_uri_media(kind: Kind, doc: &Document) -> Result<()> {
    if kind != Kind::BlockStart {
        return Ok(());
    }
    let x = doc.required(doc.root(), "x_ck")?;
    for entry in doc
        .member(x, "original")?
        .map(|o| doc.children(o))
        .transpose()?
        .into_iter()
        .flatten()
    {
        if let (Some(protocol), Some(pointer)) = (doc.member(entry, "protocol")?, doc.member(entry, "pointer")?)
            && doc.string_is(protocol, "gemini")?
            && doc.string_is(pointer, "/fileData")?
        {
            return Err(Error::Unsupported);
        }
    }
    Ok(())
}

pub(crate) fn next(owner: &mut Writer, reader: &mut ck_stream::Reader) -> Result<(Kind, Document)> {
    match reader.try_next() {
        std::task::Poll::Ready(Some(ck_stream::Read::Record(lease))) => decode(owner, lease),
        _ => Err(Error::Position),
    }
}

/// Takes the lease of a record this writer just committed, checked against that exact record,
/// so ingress can bind it from the coordinates it wrote instead of decoding it again.
pub(crate) fn committed(reader: &mut ck_stream::Reader, record: Record) -> Result<Lease> {
    match reader.try_next() {
        std::task::Poll::Ready(Some(ck_stream::Read::Record(lease))) if lease.record() == record => Ok(lease),
        _ => Err(Error::Position),
    }
}

fn advance(position: Position, delta: u64) -> Result<Position> {
    let offset = u64::from(position.offset).checked_add(delta).ok_or(Error::Position)?;
    Ok(Position {
        generation: position.generation,
        frame: position
            .frame
            .checked_add(offset / ck_stream::FRAME_BYTES as u64)
            .ok_or(Error::Position)?,
        offset: (offset % ck_stream::FRAME_BYTES as u64) as u32,
    })
}
