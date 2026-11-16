use crate::codec::{
    self, Capability, Error, Operation, RequestFence, RequestView, Result, ValidatedRequest,
    document::{Document, Name, Value},
    encode,
    event::{Emitter, Identity, Scope},
    media::Registry,
};
use ck_stream::{ScratchLease, Writer};

mod input;
mod meta;
pub use meta::validate as validate_meta;
mod native;
mod normalize;
mod reasoning;
mod validate;
mod write;

use normalize::normalize;
use validate::validate;
pub(super) use write::write_echo;
use write::write_request;

pub struct Ingress {
    owner: u64,
    /// The wire input until `prepare` consumes the last of it; then only `document` remains.
    input: Option<input::Input>,
    document: Option<Document>,
    media: Registry,
    emitter: Emitter,
    reader: ck_stream::Reader,
    fence: RequestFence,
    operation: Operation,
    closed: bool,
}
impl Ingress {
    pub fn new(
        owner: &mut Writer,
        store: &ck_stream::Store,
        identity: Identity,
        operation: Operation,
        limit: u64,
    ) -> Result<Self> {
        if operation == Operation::CountTokens {
            return Err(Error::Unsupported);
        }
        Ok(Self {
            owner: owner.id(),
            input: Some(input::Input::new(owner, limit)?),
            document: None,
            media: Registry::new(owner)?,
            emitter: Emitter::new(identity),
            reader: store.reader(owner.id(), 0)?,
            fence: RequestFence::default(),
            operation,
            closed: false,
        })
    }
    fn consume(&mut self, owner: &mut Writer, flush: bool) -> Result<()> {
        self.input.as_mut().ok_or(Error::Closed)?.consume(
            owner,
            &mut self.media,
            &mut self.emitter,
            &mut self.reader,
            self.operation,
            flush,
        )
    }
    fn prepare(&mut self, owner: &mut Writer) -> Result<()> {
        self.input.as_mut().ok_or(Error::Closed)?.pending.finish()?;
        self.consume(owner, true)?;
        // Release the wire scanner and node map before normalization grows the document, as
        // the Chat codec does after its last scan: nothing reads them past the last consume.
        let input = self.input.take().ok_or(Error::Closed)?;
        let published_media = input.published_media;
        let document = self.document.insert(input.into_document());
        self.media.validate_registrations(document, 0)?;
        normalize(document, owner, self.operation)?;
        self.media.relocate_ingress(document, 0)?;
        super::content::register(document, owner, &mut self.media)?;
        self.media.relocate_ingress(document, 0)?;
        // Parse the known Codex controls; the destination fence still authorizes them.
        validate(
            RequestView {
                document,
                media: &self.media,
                operation: self.operation,
            },
            "codex",
        )?;
        bindings(document, owner)?;
        let root = document.root();
        publish_fields(
            document,
            root,
            &mut [None; 128],
            (0, &self.media),
            &mut self.emitter,
            owner,
            &mut self.reader,
        )?;
        for index in published_media..self.media.len() {
            codec::media::publish(
                document,
                self.media.at(index),
                &mut self.emitter,
                owner,
                Scope::default(),
            )?;
            self.media.retain(document, index, owner, &mut self.reader)?;
        }
        document.retire_scratch()
    }
}
impl codec::RequestIngress for Ingress {
    fn flush_deadline(&self) -> Option<std::time::Instant> {
        self.input.as_ref()?.deadline()
    }

    fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> Result<()> {
        crate::boundary::enter(self.owner, owner, self.closed)?;
        let result = (|| {
            for chunk in bytes.chunks(8192) {
                self.input
                    .as_mut()
                    .ok_or(Error::Closed)?
                    .pending
                    .advance(owner, chunk)?;
                self.consume(owner, false)?;
            }
            if bytes.is_empty() {
                self.consume(owner, true)?;
            }
            Ok(())
        })();
        self.closed = result.is_err();
        crate::boundary::reported(result, &mut self.emitter, owner, true)
    }
    fn finish<'a>(&'a mut self, owner: &mut Writer, destination: &impl Capability) -> Result<ValidatedRequest<'a>> {
        crate::boundary::enter(self.owner, owner, self.closed)?;
        self.closed = true;
        let prepared = self.prepare(owner);
        crate::boundary::reported(prepared, &mut self.emitter, owner, true)?;
        let view = RequestView {
            document: self.document.as_ref().ok_or(Error::Closed)?,
            media: &self.media,
            operation: self.operation,
        };
        crate::boundary::end_request(&mut self.fence, view, destination, &mut self.emitter, owner)
    }
}

/// Serializes the trusted binding of every bound `x_ck` list entry, as
/// `codec::provenance::bindings` does, so the trusted record reader restores it (§4). All
/// pointers share one imported array: each import holds storage until the document retires its
/// scratch, so one import per entry exhausted the pool on requests with many original fields.
fn bindings(doc: &mut Document, owner: &mut Writer) -> Result<()> {
    let x = doc.required(doc.root(), "x_ck")?;
    let lease = encode::scratch(owner, |e| {
        e.bytes(b"[")?;
        let mut first = true;
        for list in codec::provenance::bound_lists(doc, x)?.into_iter().flatten() {
            for entry in doc.children(list)? {
                if let Some((parent, _)) = doc.origin(entry)? {
                    if !first {
                        e.bytes(b",")?;
                    }
                    first = false;
                    codec::provenance::pointer(doc, parent, e)?;
                }
            }
        }
        e.bytes(b"]")
    })?;
    let pointers = doc.import_json(owner, lease)?;
    for list in codec::provenance::bound_lists(doc, x)?.into_iter().flatten() {
        let mut next = doc.node(list)?.first;
        while let Some(entry) = next {
            next = doc.node(entry)?.next;
            let Some((_, carrier)) = doc.origin(entry)? else {
                continue;
            };
            let binding = doc.node(pointers)?.first.ok_or(Error::Schema)?;
            doc.detach(pointers, binding)?;
            doc.rename(binding, "binding")?;
            doc.append(entry, binding)?;
            if let Some(carrier) = carrier {
                doc.set(entry, "carrier", Value::Text(carrier))?;
            }
        }
    }
    Ok(())
}

pub struct ResponsesCapability;
impl Capability for ResponsesCapability {
    fn destination(&self) -> &'static str {
        "responses"
    }
    fn validate(&self, request: RequestView<'_>) -> Result<()> {
        validate(request, "responses")
    }
}

/// Responses semantics for the Antigravity steps, the only destination that reads Antigravity's
/// encrypted reasoning (Settled 62).
pub struct AntigravityCapability;
impl Capability for AntigravityCapability {
    fn destination(&self) -> &'static str {
        ANTIGRAVITY
    }
    fn validate(&self, request: RequestView<'_>) -> Result<()> {
        validate(request, ANTIGRAVITY)
    }
}

/// [`AntigravityCapability`]'s destination key.
pub(crate) const ANTIGRAVITY: &str = "antigravity";

/// Responses semantics with the isolated Codex request controls enabled.
pub struct CodexCapability;
impl Capability for CodexCapability {
    fn destination(&self) -> &'static str {
        "codex"
    }
    fn validate(&self, request: RequestView<'_>) -> Result<()> {
        validate(request, "codex")
    }
}

pub struct Egress {
    pub destination: &'static str,
    pub upstream_stream: bool,
}
impl codec::RequestEgress for Egress {
    fn encode(&self, request: &ValidatedRequest<'_>, owner: &mut Writer) -> Result<ScratchLease> {
        let view = request.for_destination(self.destination)?;
        view.document.check_owner(owner)?;
        if self.destination == "meta" {
            meta::validate(view)?;
        } else {
            validate(view, self.destination)?;
        }
        if view.operation == Operation::Compact && self.upstream_stream {
            return Err(Error::Unsupported);
        }
        encode::scratch(owner, |e| {
            write_request(view, self.upstream_stream, self.destination, e)
        })
    }
}

#[derive(Clone, Copy)]
enum Component {
    Name(Name),
    Index(u64),
}
fn publish_fields(
    doc: &mut Document,
    node: usize,
    path: &mut [Option<Component>; 128],
    (depth, media): (usize, &Registry),
    emitter: &mut Emitter,
    owner: &mut Writer,
    reader: &mut ck_stream::Reader,
) -> Result<()> {
    if depth == path.len() {
        return Err(Error::Limit);
    }
    publish_field(doc, node, &path[..depth], media, emitter, owner, reader)?;
    if media.get(doc, node)?.is_some() {
        return Ok(());
    }
    let mut next = doc.node(node)?.first;
    let array = matches!(doc.node(node)?.value, Value::Array);
    let mut index = 0;
    while let Some(child) = next {
        next = doc.node(child)?.next;
        path[depth] = Some(if array {
            Component::Index(index)
        } else {
            Component::Name(doc.node(child)?.name.ok_or(Error::Schema)?)
        });
        publish_fields(doc, child, path, (depth + 1, media), emitter, owner, reader)?;
        index += 1;
    }
    Ok(())
}

fn publish_field(
    doc: &mut Document,
    node: usize,
    path: &[Option<Component>],
    media: &Registry,
    emitter: &mut Emitter,
    owner: &mut Writer,
    reader: &mut ck_stream::Reader,
) -> Result<()> {
    if doc.published(node)? {
        return Ok(());
    }
    let registered = media.get(doc, node)?;
    let (record, spans) = emitter.emit_field(
        owner,
        |e| {
            e.bytes(b"\"")?;
            for part in path.iter().flatten() {
                e.bytes(b"/")?;
                match part {
                    Component::Index(i) => e.unsigned(*i)?,
                    Component::Name(name) => codec::provenance::write_pointer_segment(doc, *name, e)?,
                }
            }
            e.bytes(b"\"")
        },
        |e| codec::event::write_field_value(doc, node, registered, e),
    )?;
    let lease = codec::internal::committed(reader, record)?;
    // Binding a nameless object or array keeps no text of its record, neither a pointer name
    // nor a value, so adopting the record would only hold its lease until the request ends.
    let bound = doc.node(node)?;
    if bound.name.is_some() || !matches!(bound.value, Value::Object | Value::Array) {
        doc.bind_field(owner, node, lease, spans)?;
    }
    doc.mark_published(node);
    Ok(())
}
