//! Codex Alpha Search JSON ↔ the `alpha_search` operation value (§4 of INTERNAL-STREAM).
//!
//! This bounded document codec owns no transport or execution fence. Feed one HTTP body to
//! [`Decoder`], then use its mapped operation document at the request or response boundary.
//! `kind` and `value` are the `x_ck.operation` payload; `x_ck.original` holds native extras.
//! Model selection, prompt-cache stripping and HTTP status handling remain executor policy.

use ck_stream::{ScratchLease, Writer};

mod response;
pub use response::Ingress;

use crate::{
    boundary,
    codec::{
        self, Error, Result,
        document::{Document, Value},
        encode::{self, Encoder},
        json::Scanner,
        provenance,
        source::Source,
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase {
    Request,
    Result,
}

impl Phase {
    fn name(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Result => "result",
        }
    }

    fn fields(self) -> &'static [&'static str] {
        match self {
            Self::Request => &["query", "limit"],
            Self::Result => &["results"],
        }
    }
}

const RESULT_FIELDS: &[&str] = &["title", "uri", "text"];

/// Incremental syntax validation with payloads retained only in mapped storage. `finish`
/// consumes the decoder: an incomplete or invalid body cannot become a successful operation.
pub struct Decoder {
    source: Source,
    scanner: Scanner,
    phase: Phase,
    limit: u64,
    failed: bool,
}

impl Decoder {
    pub fn new(owner: &mut Writer, phase: Phase, limit: u64) -> Result<Self> {
        Ok(Self {
            source: Source::new(owner)?,
            scanner: Scanner::new(owner, limit)?,
            phase,
            limit,
            failed: false,
        })
    }

    pub fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> Result<()> {
        self.source.check_owner(owner)?;
        if self.failed {
            return Err(Error::Closed);
        }
        let result = (|| {
            if bytes.len() as u64 > self.limit.saturating_sub(self.source.len()) {
                return Err(Error::EventTooLarge);
            }
            self.source.push_bytes(owner, bytes)?;
            self.scanner.advance(&self.source)
        })();
        self.failed = result.is_err();
        result
    }

    pub fn finish(mut self, owner: &mut Writer) -> Result<Document> {
        self.source.check_owner(owner)?;
        if self.failed {
            return Err(Error::Closed);
        }
        self.scanner.finish(&self.source)?;
        let native = Document::from_scanned(owner, self.source, self.scanner)?;
        validate(&native, native.root(), self.phase)?;
        let mut operation = boundary::mapped(owner, |e| write_operation(&native, self.phase, e))?;
        provenance::restore_bindings(&mut operation)?;
        Ok(operation)
    }
}

/// Whether `operation` is a search request a destination may encode: an `alpha_search`
/// operation whose value is a request, which a result is not. The destination decides whether it serves the operation.
pub fn check_request(operation: &Document) -> Result<()> {
    let root = operation.root();
    if !boundary::is(operation, root, "kind", "alpha_search")? {
        return Err(Error::Unsupported);
    }
    validate(operation, operation.required(root, "value")?, Phase::Request)
}

/// Request egress for one destination, like every other protocol's: it encodes only what
/// that destination's own fence validated, so a client-only fence authorizes no upload.
pub struct Egress {
    pub destination: &'static str,
}

impl codec::RequestEgress for Egress {
    fn encode(&self, request: &codec::ValidatedRequest<'_>, output: &mut Writer) -> Result<ScratchLease> {
        let view = request.for_destination(self.destination)?;
        check_request(view.document)?;
        encode(output, view.document)
    }
}

/// Encode a trusted operation document, including native extras at their bound objects.
/// Known fields are read from `value`, not from a retained copy of the wire body. Call
/// `codec::provenance::restore_bindings` after reconstructing the document from canonical JSON.
pub fn encode(owner: &mut Writer, operation: &Document) -> Result<ScratchLease> {
    operation.check_owner(owner)?;
    let root = operation.root();
    if !boundary::is(operation, root, "kind", "alpha_search")? {
        return Err(Error::Unsupported);
    }
    let value = operation.required(root, "value")?;
    let phase = if boundary::is(operation, value, "phase", "request")? {
        Phase::Request
    } else if boundary::is(operation, value, "phase", "result")? {
        Phase::Result
    } else {
        return Err(Error::Schema);
    };
    validate(operation, value, phase)?;
    encode::scratch(owner, |e| {
        write_native(operation, value, phase.fields(), phase == Phase::Result, e)
    })
}

fn object(doc: &Document, node: usize) -> Result<()> {
    if matches!(doc.node(node)?.value, Value::Object) {
        Ok(())
    } else {
        Err(Error::Schema)
    }
}

fn validate(doc: &Document, root: usize, phase: Phase) -> Result<()> {
    object(doc, root)?;
    match phase {
        Phase::Request => {
            boundary::string(doc, doc.required(root, "query")?)?;
            if let Some(limit) = doc.member(root, "limit")? {
                doc.unsigned(limit)?;
            }
        }
        Phase::Result => {
            let results = doc.required(root, "results")?;
            if !matches!(doc.node(results)?.value, Value::Array) {
                return Err(Error::Schema);
            }
            for item in doc.children(results)? {
                object(doc, item)?;
                for name in RESULT_FIELDS {
                    if let Some(field) = doc.member(item, name)? {
                        boundary::string(doc, field)?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn write_operation(doc: &Document, phase: Phase, e: &mut Encoder<'_, '_>) -> Result<()> {
    let root = doc.root();
    e.bytes(b"{\"kind\":\"alpha_search\",\"value\":{\"phase\":")?;
    e.string(phase.name())?;
    for name in phase.fields() {
        if let Some(field) = doc.member(root, name)? {
            e.bytes(b",")?;
            e.string(name)?;
            e.bytes(b":")?;
            if phase == Phase::Result {
                e.bytes(b"[")?;
                for (i, item) in doc.children(field)?.enumerate() {
                    if i != 0 {
                        e.bytes(b",")?;
                    }
                    write_known(doc, item, RESULT_FIELDS, e)?;
                }
                e.bytes(b"]")?;
            } else {
                doc.write(field, e)?;
            }
        }
    }
    e.bytes(b"},\"x_ck\":{\"v\":1,\"original_protocol\":\"codex\",\"original\":[")?;
    let mut first = true;
    originals(doc, root, phase.fields(), None, &mut first, e)?;
    if phase == Phase::Result {
        for (i, item) in doc.children(doc.required(root, "results")?)?.enumerate() {
            originals(doc, item, RESULT_FIELDS, Some(i), &mut first, e)?;
        }
    }
    e.bytes(b"]}}")
}

fn originals(
    doc: &Document,
    parent: usize,
    known: &[&str],
    index: Option<usize>,
    first: &mut bool,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    for field in doc.children(parent)? {
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        if known
            .iter()
            .try_fold(false, |found, key| Ok::<_, Error>(found || doc.name_is(name, key)?))?
        {
            continue;
        }
        if !*first {
            e.bytes(b",")?;
        }
        *first = false;
        e.bytes(b"{\"protocol\":\"codex\",\"pointer\":\"")?;
        if let Some(index) = index {
            e.bytes(b"/results/")?;
            e.unsigned(index as u64)?;
        }
        e.bytes(b"/")?;
        provenance::write_pointer_segment(doc, name, e)?;
        e.bytes(b"\",\"binding\":\"/value")?;
        if let Some(index) = index {
            e.bytes(b"/results/")?;
            e.unsigned(index as u64)?;
        }
        e.bytes(b"\",\"value\":")?;
        doc.write(field, e)?;
        e.bytes(b"}")?;
    }
    Ok(())
}

fn write_known(doc: &Document, parent: usize, known: &[&str], e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"{")?;
    let mut first = true;
    for name in known {
        if let Some(field) = doc.member(parent, name)? {
            if !first {
                e.bytes(b",")?;
            }
            first = false;
            e.string(name)?;
            e.bytes(b":")?;
            doc.write(field, e)?;
        }
    }
    e.bytes(b"}")
}

fn write_native(doc: &Document, parent: usize, known: &[&str], results: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"{")?;
    let mut first = true;
    for name in known {
        if let Some(field) = doc.member(parent, name)? {
            if !first {
                e.bytes(b",")?;
            }
            first = false;
            e.string(name)?;
            e.bytes(b":")?;
            if results {
                e.bytes(b"[")?;
                for (i, item) in doc.children(field)?.enumerate() {
                    if i != 0 {
                        e.bytes(b",")?;
                    }
                    write_native(doc, item, RESULT_FIELDS, false, e)?;
                }
                e.bytes(b"]")?;
            } else {
                doc.write(field, e)?;
            }
        }
    }
    let x = doc.required(doc.root(), "x_ck")?;
    let original = doc.required(x, "original")?;
    for entry in doc.origins(parent)? {
        if doc.node(entry)?.parent != Some(original) || !boundary::is(doc, entry, "protocol", "codex")? {
            continue;
        }
        let pointer = doc.required(entry, "pointer")?;
        let name = provenance::leaf(doc.chars(pointer)?)?;
        if known
            .iter()
            .try_fold(false, |found, key| Ok::<_, Error>(found || name.is(key)?))?
        {
            continue;
        }
        if !first {
            e.bytes(b",")?;
        }
        first = false;
        provenance::write_segment(name, e)?;
        e.bytes(b":")?;
        doc.write(doc.required(entry, "value")?, e)?;
    }
    e.bytes(b"}")
}
