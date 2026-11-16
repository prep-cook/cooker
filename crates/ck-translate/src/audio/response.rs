use std::time::{Duration, Instant};

use ck_stream::{Lease, ScratchLease, Writer};

use crate::{
    boundary,
    codec::{
        self, Error, ResponseEgress, ResponseIngress, Result,
        document::{Document, Value},
        encode,
        event::{Direction, Emitter, Identity, Kind, LocalId, Scope},
        internal::Binding,
        media::{self, Publisher},
        source::Source,
    },
};

/// One finite HTTP audio entity. Partial chunks live in mapped scratch until the
/// chunk ceiling or a flush. Owners schedule `flush_deadline` and flush immediately
/// when client egress is waiting; transport read boundaries do not end chunks.
pub struct Ingress {
    owner: u64,
    emitter: Emitter,
    publisher: Publisher,
    held: Source,
    since: Option<Instant>,
    received: u64,
    declared: Option<u64>,
    limit: u64,
    closed: bool,
}

impl Ingress {
    /// Called only for a successful HTTP response. Preserve the supplied content
    /// type exactly, including parameters; absence does not imply a default MIME.
    pub fn new(
        owner: &mut Writer,
        identity: Identity,
        content_type: Option<&str>,
        declared: Option<u64>,
        limit: u64,
    ) -> Result<Self> {
        if identity.direction != Direction::Response {
            return Err(Error::Schema);
        }
        let mut emitter = Emitter::new(identity);
        if declared.is_some_and(|length| length > limit) {
            codec::upstream_failure(&mut emitter, owner, 502, "upstream")?;
            return Err(Error::EventTooLarge);
        }
        emitter.emit(
            owner,
            Kind::ResponseStart,
            Scope::default(),
            |e| e.bytes(b",\"id\":\"audio\",\"model\":\"\",\"created\":0"),
            |e| e.bytes(b",\"synthetic\":[\"id\",\"model\",\"created\"]"),
        )?;
        let publisher = Publisher::start(
            &mut emitter,
            owner,
            Scope {
                media_id: Some(LocalId::new("m0")?),
                ..Scope::default()
            },
            |e| {
                e.bytes(b",\"kind\":\"audio\",\"encoding\":\"base64\"")?;
                if let Some(mime) = content_type {
                    e.bytes(b",\"mime\":")?;
                    e.string(mime)?;
                }
                Ok(())
            },
        )?;
        Ok(Self {
            owner: owner.id(),
            emitter,
            publisher,
            held: Source::new(owner)?,
            since: None,
            received: 0,
            declared,
            limit,
            closed: false,
        })
    }

    fn publish(&mut self, owner: &mut Writer) -> Result<()> {
        let span = self.held.whole();
        if !span.is_empty() {
            let held = &self.held;
            self.publisher
                .collect(&mut self.emitter, owner, |emit| held.visit(span, emit))?;
            self.held.release_before(span.end)?;
        }
        self.since = None;
        Ok(())
    }

    fn report<T>(&mut self, owner: &mut Writer, result: Result<T>) -> Result<T> {
        if let Err(error) = &result {
            self.closed = true;
            if !matches!(error, Error::Storage(_)) {
                self.publish(owner)?;
                codec::upstream_failure(&mut self.emitter, owner, 502, "upstream")?;
            }
        }
        result
    }
}

impl ResponseIngress for Ingress {
    fn flush_deadline(&self) -> Option<Instant> {
        self.since.map(|since| since + Duration::from_millis(100))
    }

    fn advance(&mut self, owner: &mut Writer, mut bytes: &[u8]) -> Result<()> {
        boundary::enter(self.owner, owner, self.closed)?;
        let result = (|| {
            if bytes.len() as u64 > self.limit.saturating_sub(self.received) {
                return Err(Error::EventTooLarge);
            }
            if self
                .declared
                .is_some_and(|length| bytes.len() as u64 > length.saturating_sub(self.received))
            {
                return Err(Error::Schema);
            }
            if bytes.is_empty() || self.flush_deadline().is_some_and(|deadline| owner.now() >= deadline) {
                self.publish(owner)?;
            }
            while !bytes.is_empty() {
                let capacity = self.publisher.capacity(&self.emitter)?;
                let held = self.held.whole().len() as usize;
                let count = bytes.len().min(capacity - held);
                self.since.get_or_insert_with(|| owner.now());
                self.held.push_bytes(owner, &bytes[..count])?;
                self.received += count as u64;
                bytes = &bytes[count..];
                if held + count == capacity {
                    self.publish(owner)?;
                }
            }
            Ok(())
        })();
        self.report(owner, result)
    }

    fn finish(&mut self, owner: &mut Writer) -> Result<()> {
        boundary::enter(self.owner, owner, self.closed)?;
        let result = (|| {
            if self.declared.is_some_and(|length| length != self.received) {
                return Err(Error::Truncated);
            }
            self.publish(owner)?;
            self.publisher.end(&mut self.emitter, owner)?;
            self.emitter
                .emit(owner, Kind::Done, Scope::default(), |_| Ok(()), |_| Ok(()))?;
            Ok(())
        })();
        let result = self.report(owner, result);
        self.closed = true;
        result
    }

    fn fail(&mut self, owner: &mut Writer, status: u16, scope: &'static str) -> Result<()> {
        boundary::enter(self.owner, owner, self.closed)?;
        self.closed = true;
        self.publish(owner)?;
        codec::upstream_failure(&mut self.emitter, owner, status, scope)
    }
}

/// Raw entity-body egress of one media object: speech audio, or a finished video file. Each
/// data record is validated before its decoded bytes reach the callback. Errors are returned
/// to the HTTP owner, never encoded as JSON inside the file. Once bytes were sent the owner
/// must abort the body.
pub struct Egress {
    binding: Binding,
    /// The `media.start` kind the stream must carry.
    kind: &'static str,
    started: bool,
    media: Option<(LocalId, Document)>,
    index: u64,
    offset: u64,
    ended: bool,
    closed: bool,
    success: bool,
}

impl Egress {
    /// The speech audio's egress.
    pub fn new(owner: &Writer, identity: Option<Identity>) -> Self {
        Self::of(owner, identity, "audio")
    }

    /// The egress of a media object of `kind` (`audio`, `video`).
    pub fn of(owner: &Writer, identity: Option<Identity>, kind: &'static str) -> Self {
        Self {
            binding: Binding::new(owner, identity),
            kind,
            started: false,
            media: None,
            index: 0,
            offset: 0,
            ended: false,
            closed: false,
            success: false,
        }
    }

    /// The mapped JSON string supplied by `media.start`, for the HTTP content-type
    /// header. Available before the first output callback; no MIME is synthesized.
    pub fn content_type(&self) -> Result<Option<(&Document, usize)>> {
        let Some((_, doc)) = &self.media else { return Ok(None) };
        Ok(doc.member(doc.root(), "mime")?.map(|node| (doc, node)))
    }

    fn record(
        &mut self,
        owner: &mut Writer,
        input: Lease,
        mut output: impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let (kind, doc) = self.binding.decode(owner, input)?;
        let root = doc.root();
        let x = doc.required(root, "x_ck")?;
        if matches!(kind, Kind::Error | Kind::Cancelled) {
            return Err(Error::Truncated);
        }
        if !self.started && kind != Kind::ResponseStart {
            return Err(Error::Schema);
        }
        match kind {
            Kind::ResponseStart if !self.started => self.started = true,
            Kind::MediaStart if self.media.is_none() => {
                if !boundary::is(&doc, root, "kind", self.kind)? || !boundary::is(&doc, root, "encoding", "base64")? {
                    return Err(Error::Schema);
                }
                if let Some(mime) = doc.member(root, "mime")? {
                    boundary::string(&doc, mime)?;
                }
                self.media = Some((media::media_id(&doc, x)?, doc));
            }
            Kind::Data | Kind::MediaEnd if !self.ended => {
                let id = self.media.as_ref().ok_or(Error::Schema)?.0;
                if media::media_id(&doc, x)? != id {
                    return Err(Error::Schema);
                }
                if kind == Kind::MediaEnd {
                    if doc.unsigned(doc.required(root, "bytes")?)? != self.offset
                        || !boundary::is(&doc, root, "status", "completed")?
                    {
                        return Err(Error::Truncated);
                    }
                    self.ended = true;
                } else {
                    if doc.unsigned(doc.required(root, "index")?)? != self.index
                        || doc.unsigned(doc.required(root, "offset")?)? != self.offset
                    {
                        return Err(Error::Schema);
                    }
                    let Value::String(text) = doc.node(doc.required(root, "bytes")?)?.value else {
                        return Err(Error::Schema);
                    };
                    let bytes = encode::scratch(owner, |e| media::decode_base64(&doc, text, |bytes| e.bytes(bytes)))?;
                    self.offset = self.offset.checked_add(bytes.len() as u64).ok_or(Error::Limit)?;
                    self.index = self.index.checked_add(1).ok_or(Error::Limit)?;
                    output(bytes)?;
                }
            }
            Kind::Done if self.ended => {
                self.closed = true;
                self.success = true;
            }
            // A job's usage snapshot is accounting's; the file does not carry it.
            Kind::Heartbeat | Kind::Usage => (),
            _ => return Err(Error::Schema),
        }
        Ok(())
    }
}

impl ResponseEgress for Egress {
    fn advance(
        &mut self,
        owner: &mut Writer,
        input: Lease,
        output: impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        if self.closed {
            return Err(Error::Closed);
        }
        let result = self.record(owner, input, output);
        if result.is_err() {
            self.closed = true;
        }
        result
    }

    fn finish(&mut self) -> Result<()> {
        if self.success {
            Ok(())
        } else {
            self.closed = true;
            Err(Error::Truncated)
        }
    }
}
