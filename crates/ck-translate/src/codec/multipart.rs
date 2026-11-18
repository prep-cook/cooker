//! Multipart transport ingress. Operation codecs interpret the ordered parts; this codec
//! preserves their names, filenames, MIME types and bytes under `x_ck.transport.parts`.
//! No request is admitted until HTTP EOF, the closing delimiter and the capability fence.

use std::time::{Duration, Instant};

use ck_stream::{MetadataPermit, Reader, Writer};
use nostd_multipart::{Event, Parser};

use super::{
    Capability, Error, Operation, RequestFence, RequestIngress, RequestView, Result, ValidatedRequest,
    document::{Document, Text, Value},
    encode::Encoder,
    event::{Direction, Emitter, Identity, Kind, Scope},
    internal,
    media::{self, Encoding, Publisher, Registry},
    source::{Source, Span},
    table::Table,
};

const HEADER_LINE: usize = 4096;
const MAX_HEADERS: usize = 32;
const INPUT_STEP: usize = 8192;
const FLUSH_DELAY: Duration = Duration::from_millis(100);

/// Client admission limits, independent of the store's shared frame/metadata quotas.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub body_bytes: u64,
    pub part_bytes: u64,
    pub parts: u64,
}

/// Decoded RFC 2046 boundary storage, owned separately so the parser can borrow it.
pub struct Boundary<'a> {
    content_type: &'a str,
    bytes: [u8; 70],
    len: usize,
    _budget: MetadataPermit,
}

impl<'a> Boundary<'a> {
    pub fn new(owner: &mut Writer, content_type: &'a str) -> Result<Self> {
        if content_type.len() > HEADER_LINE {
            return Err(Error::EventTooLarge);
        }
        let mut budget = owner.reserve_metadata(std::mem::size_of::<Self>() + HEADER_LINE)?;
        let mut normalized = [0; HEADER_LINE];
        let mut len = 0;
        let mut quoted = false;
        let mut input = content_type.bytes();
        while let Some(mut byte) = input.next() {
            if quoted && byte == b'\\' {
                byte = input.next().ok_or(Error::Schema)?;
                // Quotes and backslashes are not boundary bchars. Keep their quoting
                // intact for the dependency to reject in a boundary or skip in other parameters.
                if matches!(byte, b'"' | b'\\') {
                    normalized[len] = b'\\';
                    len += 1;
                }
            } else if byte == b'"' {
                quoted = !quoted;
            }
            normalized[len] = byte;
            len += 1;
        }
        let parser = Parser::<0, 0>::from_content_type(&normalized[..len]).map_err(parser_error)?;
        let boundary = parser.boundary();
        let mut bytes = [0; 70];
        bytes[..boundary.len()].copy_from_slice(boundary);
        budget.resize(std::mem::size_of::<Self>())?;
        Ok(Self {
            content_type,
            bytes,
            len: boundary.len(),
            _budget: budget,
        })
    }
}

/// The decoded boundary is borrowed for the lifetime of its incremental parser.
/// Parser headers have a fixed 4 KiB line and 32-header ceiling. Body storage is mmap only.
pub struct Ingress<'a> {
    parser: Parser<'a, HEADER_LINE, MAX_HEADERS>,
    state: State,
    closed: bool,
    closing: Closing,
    _parser_budget: MetadataPermit,
}

struct State {
    input: Source,
    cursor: u64,
    total: u64,
    limits: Limits,
    document: Document,
    registry: Registry,
    emitter: Emitter,
    reader: Reader,
    fence: RequestFence,
    operation: Operation,
    parts: usize,
    count: u64,
    current: Option<Part>,
    held: Option<Span>,
    deadline: Option<Instant>,
}

struct Part {
    node: usize,
    reference: Option<usize>,
    bytes: u64,
    publisher: Option<Publisher>,
    chunks: Table<Text>,
}

#[derive(Default)]
enum Closing {
    #[default]
    Padding,
    LineFeed,
    Epilogue,
}

impl Closing {
    fn advance(&mut self, bytes: &[u8]) -> Result<()> {
        for &byte in bytes {
            *self = match (&self, byte) {
                (Self::Padding, b' ' | b'\t') => Self::Padding,
                (Self::Padding, b'\r') => Self::LineFeed,
                (Self::LineFeed, b'\n') => Self::Epilogue,
                (Self::Epilogue, _) => return Ok(()),
                _ => return Err(Error::Schema),
            };
        }
        Ok(())
    }
}

impl<'a> Ingress<'a> {
    pub fn new(
        owner: &mut Writer,
        store: &ck_stream::Store,
        identity: Identity,
        operation: Operation,
        boundary: &'a Boundary<'_>,
        limits: Limits,
    ) -> Result<Self> {
        if identity.direction != Direction::Request {
            return Err(Error::Schema);
        }
        let budget = owner.reserve_metadata(std::mem::size_of::<Parser<'_, HEADER_LINE, MAX_HEADERS>>())?;
        let parser = Parser::new(&boundary.bytes[..boundary.len]).map_err(parser_error)?;
        let content_type = boundary.content_type;
        let document = Document::streaming(owner)?;
        let registry = Registry::new(owner)?;
        let mut state = State {
            input: Source::new(owner)?,
            cursor: 0,
            total: 0,
            limits,
            document,
            registry,
            emitter: Emitter::new(identity),
            reader: store.reader(owner.id(), 0)?,
            fence: RequestFence::default(),
            operation,
            parts: 0,
            count: 0,
            current: None,
            held: None,
            deadline: None,
        };
        let record = state.emitter.request_start(
            owner,
            RequestView {
                document: &state.document,
                media: &state.registry,
                operation,
            },
        )?;
        internal::committed(&mut state.reader, record)?;
        let root = state.document.root();
        let x = state.document.set(root, "x_ck", Value::Object)?;
        state.field(owner, x, None, "/x_ck", |e| e.bytes(b"{}"), false)?;
        let transport = state.document.set(x, "transport", Value::Object)?;
        state.field(owner, transport, None, "/x_ck/transport", |e| e.bytes(b"{}"), false)?;
        for (name, value, pointer) in [
            ("kind", "multipart", "/x_ck/transport/kind"),
            ("content_type", content_type, "/x_ck/transport/content_type"),
        ] {
            let node = state.document.set(transport, name, Value::Text(""))?;
            state.field(owner, node, None, pointer, |e| e.string(value), true)?;
        }
        state.parts = state.document.set(transport, "parts", Value::Array)?;
        state.field(
            owner,
            state.parts,
            None,
            "/x_ck/transport/parts",
            |e| e.bytes(b"[]"),
            false,
        )?;
        Ok(Self {
            parser,
            state,
            closed: false,
            closing: Closing::default(),
            _parser_budget: budget,
        })
    }

    fn check(&self, owner: &Writer) -> Result<()> {
        self.state.input.check_owner(owner)?;
        if self.closed { Err(Error::Closed) } else { Ok(()) }
    }

    fn drain(&mut self, owner: &mut Writer, eof: bool) -> Result<()> {
        loop {
            let start = self.state.cursor;
            let end = self.state.input.len().min(start.saturating_add(INPUT_STEP as u64));
            let span = Span { start, end };
            let mut copied = [0; INPUT_STEP];
            let input = match self.state.input.slice(span)? {
                Some(bytes) => bytes,
                None => {
                    let mut len = 0;
                    self.state.input.visit(span, |bytes| {
                        copied[len..len + bytes.len()].copy_from_slice(bytes);
                        len += bytes.len();
                        Ok(())
                    })?;
                    &copied[..len]
                }
            };
            // The dependency consumes only through the closing `--`. Validate its suffix
            // ourselves before it can discard those bytes as epilogue (RFC 2046 §5.1.1).
            if self.parser.is_finished() {
                self.closing.advance(input)?;
                self.state.cursor = end;
                self.state.input.release_before(end)?;
                if end == self.state.input.len() {
                    return Ok(());
                }
                continue;
            }
            let (used, event) = self
                .parser
                .parse(input, eof && end == self.state.input.len())
                .map_err(parser_error)?;
            // Data borrows input, headers borrow the parser. Only their positions or bounded
            // decoded header values survive this call; never retain a mapped pointer.
            let finished = matches!(event, Some(Event::Finished));
            let data = matches!(event, Some(Event::Data(_)));
            let progress = used != 0 || event.is_some();
            let header_event = match event {
                Some(Event::Header { name, value }) => Some(Event::Header { name, value }),
                Some(Event::PartStart) => Some(Event::PartStart),
                Some(Event::HeadersEnd) => Some(Event::HeadersEnd),
                Some(Event::PartEnd) => Some(Event::PartEnd),
                Some(Event::Finished) => Some(Event::Finished),
                Some(Event::Data(_)) | None => None,
            };
            if data {
                self.state.data(
                    owner,
                    Span {
                        start,
                        end: start + used as u64,
                    },
                )?;
            } else {
                self.state.event(owner, header_event)?;
            }
            self.state.cursor += used as u64;
            // Keep a short consumed tail so one-byte header feeds reuse the open scratch frame.
            let tail = if finished {
                self.state.cursor
            } else {
                self.state.cursor.saturating_sub(INPUT_STEP as u64)
            };
            let floor = self.state.held.map_or(tail, |held| held.start.min(tail));
            self.state.input.release_before(floor)?;
            if !progress {
                return Ok(());
            }
        }
    }

    fn fail(&mut self, owner: &mut Writer, error: Error) -> Error {
        self.closed = true;
        self.state.held = None;
        self.state.deadline = None;
        failure(&mut self.state.emitter, owner, &error).err().unwrap_or(error)
    }
}

impl RequestIngress for Ingress<'_> {
    fn flush_deadline(&self) -> Option<Instant> {
        self.state.deadline
    }

    fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> Result<()> {
        self.check(owner)?;
        let result = (|| {
            self.state.total = self.state.total.checked_add(bytes.len() as u64).ok_or(Error::Limit)?;
            if self.state.total > self.state.limits.body_bytes {
                return Err(Error::Limit);
            }
            if bytes.is_empty() {
                self.state.flush(owner)?;
            }
            for bytes in bytes.chunks(INPUT_STEP) {
                self.state.input.push_bytes(owner, bytes)?;
                self.drain(owner, false)?;
                if self.state.deadline.is_some_and(|deadline| owner.now() >= deadline) {
                    self.state.flush(owner)?;
                }
            }
            Ok(())
        })();
        result.map_err(|error| self.fail(owner, error))
    }

    fn finish<'b>(&'b mut self, owner: &mut Writer, destination: &impl Capability) -> Result<ValidatedRequest<'b>> {
        self.check(owner)?;
        if let Err(error) = self.drain(owner, true) {
            return Err(self.fail(owner, error));
        }
        if !self.parser.is_finished() || self.state.current.is_some() || matches!(self.closing, Closing::LineFeed) {
            return Err(self.fail(owner, Error::Truncated));
        }
        // Close even if capability validation fails: callers cannot retry with a weaker policy.
        self.closed = true;
        let state = &mut self.state;
        let view = RequestView {
            document: &state.document,
            media: &state.registry,
            operation: state.operation,
        };
        let ready = match state.fence.end(view, destination) {
            Ok(ready) => ready,
            Err(error) => {
                failure(&mut state.emitter, owner, &error)?;
                return Err(error);
            }
        };
        state.emitter.request_end(owner, &ready)?;
        state
            .emitter
            .emit(owner, Kind::Done, Scope::default(), |_| Ok(()), |_| Ok(()))?;
        Ok(ready)
    }
}

impl State {
    fn field(
        &mut self,
        owner: &mut Writer,
        node: usize,
        part: Option<u64>,
        member: &str,
        value: impl Fn(&mut Encoder<'_, '_>) -> Result<()>,
        scalar: bool,
    ) -> Result<()> {
        let (record, spans) = self.emitter.emit_field(
            owner,
            |e| {
                if let Some(index) = part {
                    e.bytes(b"\"/x_ck/transport/parts/")?;
                    e.unsigned(index)?;
                    if !member.is_empty() {
                        e.bytes(b"/")?;
                        e.escaped(member)?;
                    }
                    e.bytes(b"\"")
                } else {
                    e.string(member)
                }
            },
            |e| {
                value(e)?;
                Ok(scalar)
            },
        )?;
        let lease = internal::committed(&mut self.reader, record)?;
        self.document.bind_field(owner, node, lease, spans)
    }

    fn text(&mut self, owner: &mut Writer, name: &'static str, bytes: &[u8]) -> Result<()> {
        let text = std::str::from_utf8(bytes).map_err(|_| Error::Utf8)?;
        let part = self.current.as_ref().ok_or(Error::Schema)?.node;
        if self.document.member(part, name)?.is_some() {
            return Err(Error::Schema);
        }
        let node = self.document.set(part, name, Value::Text(""))?;
        self.field(owner, node, Some(self.count - 1), name, |e| e.string(text), true)
    }

    fn event(&mut self, owner: &mut Writer, event: Option<Event<'_, '_>>) -> Result<()> {
        match event {
            Some(Event::PartStart) => {
                if self.count >= self.limits.parts {
                    return Err(Error::Limit);
                }
                self.count += 1;
                let node = self.document.add(None, Value::Object)?;
                self.document.append(self.parts, node)?;
                self.field(owner, node, Some(self.count - 1), "", |e| e.bytes(b"{}"), false)?;
                self.current = Some(Part {
                    node,
                    reference: None,
                    bytes: 0,
                    publisher: None,
                    chunks: Table::new(owner)?,
                });
            }
            Some(Event::Header { name, value }) => {
                if name.eq_ignore_ascii_case(b"content-disposition") {
                    let disposition = nostd_multipart::content_disposition(value).map_err(parser_error)?;
                    if !disposition.is_form_data() {
                        return Err(Error::Schema);
                    }
                    let mut decoded = [0; HEADER_LINE];
                    let name = disposition.name().ok_or(Error::Schema)?;
                    self.text(
                        owner,
                        "name",
                        name.decode_into(&mut decoded).ok_or(Error::EventTooLarge)?,
                    )?;
                    if let Some(filename) = disposition.filename() {
                        self.text(
                            owner,
                            "filename",
                            filename.decode_into(&mut decoded).ok_or(Error::EventTooLarge)?,
                        )?;
                    }
                    if let Some(filename) = disposition.filename_star() {
                        self.text(owner, "filename_star", filename)?;
                    }
                } else if name.eq_ignore_ascii_case(b"content-type") {
                    self.text(owner, "content_type", value)?;
                }
                // Non-operation headers, especially Authorization/Cookie, never enter provenance.
            }
            Some(Event::HeadersEnd) => self.begin(owner)?,
            Some(Event::PartEnd) => {
                self.flush(owner)?;
                let mut part = self.current.take().ok_or(Error::Schema)?;
                part.publisher
                    .as_mut()
                    .ok_or(Error::Schema)?
                    .end(&mut self.emitter, owner)?;
                internal::next(owner, &mut self.reader)?;
                let id = media::local_id(b'm', self.count - 1)?;
                self.registry.adopt(
                    &mut self.document,
                    part.reference,
                    (id, "document", Encoding::Base64),
                    part.chunks,
                )?;
                let mime = self.document.member(part.node, "content_type")?;
                self.registry.update_mime(part.reference.ok_or(Error::Schema)?, mime);
            }
            Some(Event::Finished) | None => (),
            Some(Event::Data(_)) => unreachable!("data handled as mapped positions"),
        }
        Ok(())
    }

    fn begin(&mut self, owner: &mut Writer) -> Result<()> {
        let part = self.current.as_ref().ok_or(Error::Schema)?.node;
        self.document.required(part, "name")?;
        let id = media::local_id(b'm', self.count - 1)?;
        let reference = self.document.set(part, "data", Value::Object)?;
        self.document.set(reference, "type", Value::Text("x_ck_media"))?;
        self.document.set_id(reference, "media_id", id)?;
        self.document.set(reference, "value_kind", Value::Text("bytes"))?;
        self.field(
            owner,
            reference,
            Some(self.count - 1),
            "data",
            |e| {
                e.bytes(b"{\"type\":\"x_ck_media\",\"media_id\":")?;
                e.string(id.as_str())?;
                e.bytes(b",\"value_kind\":\"bytes\"}")
            },
            false,
        )?;
        let mime = self.document.member(part, "content_type")?;
        let publisher = Publisher::start(
            &mut self.emitter,
            owner,
            Scope {
                media_id: Some(id),
                ..Scope::default()
            },
            |e| {
                e.bytes(b",\"kind\":\"document\",\"encoding\":\"base64\"")?;
                if let Some(mime) = mime {
                    e.bytes(b",\"mime\":")?;
                    self.document.write(mime, e)?;
                }
                Ok(())
            },
        )?;
        internal::next(owner, &mut self.reader)?;
        let current = self.current.as_mut().ok_or(Error::Schema)?;
        current.reference = Some(reference);
        current.publisher = Some(publisher);
        Ok(())
    }

    fn data(&mut self, owner: &mut Writer, mut span: Span) -> Result<()> {
        let part = self.current.as_mut().ok_or(Error::Schema)?;
        part.bytes = part.bytes.checked_add(span.len()).ok_or(Error::Limit)?;
        if part.bytes > self.limits.part_bytes {
            return Err(Error::Limit);
        }
        while span.start < span.end {
            let held = self.held.get_or_insert(Span {
                start: span.start,
                end: span.start,
            });
            if held.end != span.start {
                return Err(Error::Position);
            }
            self.deadline.get_or_insert(owner.now() + FLUSH_DELAY);
            let take = (super::DATA_CHUNK_BYTES as u64 - held.len()).min(span.len());
            held.end += take;
            span.start += take;
            if held.len() == super::DATA_CHUNK_BYTES as u64 {
                self.flush(owner)?;
            }
        }
        Ok(())
    }

    fn flush(&mut self, owner: &mut Writer) -> Result<()> {
        let Some(span) = self.held.take() else {
            return Ok(());
        };
        self.deadline = None;
        let mut copied = [0; super::DATA_CHUNK_BYTES];
        let bytes = match self.input.slice(span)? {
            Some(bytes) => bytes,
            None => {
                let mut len = 0;
                self.input.visit(span, |bytes| {
                    copied[len..len + bytes.len()].copy_from_slice(bytes);
                    len += bytes.len();
                    Ok(())
                })?;
                &copied[..len]
            }
        };
        let part = self.current.as_mut().ok_or(Error::Schema)?;
        part.publisher
            .as_mut()
            .ok_or(Error::Schema)?
            .append(bytes, &mut self.emitter, owner)?;
        let (_, event) = internal::next(owner, &mut self.reader)?;
        let Value::String(text) = event.node(event.required(event.root(), "bytes")?)?.value else {
            return Err(Error::Schema);
        };
        part.chunks.push(self.document.retain_text(event, text)?)?;
        Ok(())
    }
}

fn parser_error(error: nostd_multipart::Error) -> Error {
    match error {
        nostd_multipart::Error::HeaderTooLong | nostd_multipart::Error::TooManyHeaders => Error::EventTooLarge,
        nostd_multipart::Error::UnexpectedEof => Error::Truncated,
        _ => Error::Schema,
    }
}

fn failure(emitter: &mut Emitter, owner: &mut Writer, error: &Error) -> Result<()> {
    if matches!(error, Error::Storage(_)) {
        return Ok(());
    }
    let (status, code, message) = match error {
        Error::Limit | Error::EventTooLarge => (413, "multipart.too_large", "Multipart request exceeds its limit"),
        _ => (400, "multipart.invalid", "Invalid multipart request"),
    };
    emitter.emit(
        owner,
        Kind::Error,
        Scope::default(),
        |e| {
            e.bytes(b",\"code\":")?;
            e.string(code)?;
            e.bytes(b",\"status\":")?;
            e.unsigned(status)?;
            e.bytes(b",\"message\":")?;
            e.string(message)?;
            e.bytes(b",\"scope\":\"client\"")
        },
        |_| Ok(()),
    )?;
    Ok(())
}
