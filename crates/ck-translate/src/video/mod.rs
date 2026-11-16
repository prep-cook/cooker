//! Video job JSON ↔ the `video.request` and `video.job` operation values (§4, §5.23–5.24 of
//! INTERNAL-STREAM), for the OpenAI and xAI shapes. Finished video bytes are in [`content`].
//!
//! This bounded document codec owns no transport, job table or execution fence. Feed one HTTP
//! body to [`Decoder`], then use its mapped operation document at the request or response
//! boundary. Options keep their native names and units (`seconds` is not turned into
//! `duration`); an option the destination dialect lacks is refused by [`encode`], never
//! dropped. Unknown fields travel in `x_ck.original` and return only to their own protocol.
//! [`Ingress`] publishes a job response on a response stream; [`JobEgress`] writes it back to the
//! client.

pub mod content;
mod egress;
mod response;
mod table;
mod wire;

use ck_stream::Writer;

use crate::{
    boundary,
    codec::{
        Error, Result,
        document::{Document, Value},
        encode::Encoder,
        json::Scanner,
        provenance,
        source::Source,
    },
};
pub use egress::JobEgress;
pub use response::{Ingress, Ledger};
use table::{Shape, WIRE_STATUS, known, shape};
use wire::write_fraction;
pub use wire::{check_request, encode};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Dialect {
    OpenAi,
    Xai,
}

impl Dialect {
    /// The `x_ck.original_protocol` value.
    pub const fn name(self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::Xai => "xai",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Action {
    Create,
    Edit,
    Extend,
    Retrieve,
    Content,
}

const ACTIONS: [Action; 5] = [
    Action::Create,
    Action::Edit,
    Action::Extend,
    Action::Retrieve,
    Action::Content,
];

impl Action {
    const fn name(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Edit => "edit",
            Self::Extend => "extend",
            Self::Retrieve => "retrieve",
            Self::Content => "content",
        }
    }
}

/// What the body is. A request body carries options; `Retrieve` and `Content` may have none.
/// `Job` is a status document; `Accepted` is a create-style acknowledgement that names the
/// job and may omit its status (xAI answers `{"request_id": …}`), which then reads as queued.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase {
    Request(Action),
    Job,
    Accepted,
}

/// Usage a job reports once it is no longer pending. Absent fields stay absent.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Usage {
    pub cost_in_usd_ticks: Option<u64>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
}

pub struct Decoded {
    pub operation: Document,
    /// What this body reports, whatever the job's status: xAI's `usage` has no status condition
    /// (docs.x.ai `rest-api-reference/inference/videos.md`, GET `/v1/videos/{request_id}`), so a
    /// pending poll may carry it. Whether a report is the job's charge is the job table's
    /// decision ([`Ledger`]). Absent, null or empty `usage` is `None`; nothing is invented.
    pub usage: Option<Usage>,
}

const MAX_JOB_ID: usize = 256;
const JOB_FIELDS: [&str; 7] = [
    "id",
    "request_id",
    "status",
    "progress",
    "created_at",
    "completed_at",
    "error",
];

/// Incremental syntax validation with payloads retained only in mapped storage. `finish`
/// consumes the decoder: an incomplete or invalid body cannot become a successful operation.
pub struct Decoder {
    source: Source,
    scanner: Scanner,
    dialect: Dialect,
    phase: Phase,
    job_id: Option<String>,
    limit: u64,
    failed: bool,
}

impl Decoder {
    /// `job_id` is the cooker job the route resolved: mandatory for a retrieve or content request,
    /// forbidden for create, optional for an edit or extension, whose body names its source video
    /// as its dialect does (xAI by URL), and the fallback identity of a status body that has none.
    pub fn new(owner: &mut Writer, dialect: Dialect, phase: Phase, job_id: Option<&str>, limit: u64) -> Result<Self> {
        match (phase, job_id) {
            (Phase::Request(Action::Create | Action::Edit | Action::Extend), None) => {}
            (Phase::Request(Action::Create), Some(_)) | (Phase::Request(_), None) => return Err(Error::Schema),
            (_, Some("")) => return Err(Error::Schema),
            (_, Some(id)) if id.len() > MAX_JOB_ID => return Err(Error::Limit),
            _ => {}
        }
        Ok(Self {
            source: Source::new(owner)?,
            scanner: Scanner::new(owner, limit)?,
            dialect,
            phase,
            job_id: job_id.map(str::to_owned),
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

    pub fn finish(self, owner: &mut Writer) -> Result<Decoded> {
        self.finish_with(owner, None)
    }

    /// [`Self::finish`] with `echo`, an array of `x_ck.original` entries in other protocols, after
    /// the body's own native fields.
    fn finish_with(mut self, owner: &mut Writer, echo: Option<&Document>) -> Result<Decoded> {
        self.source.check_owner(owner)?;
        if self.failed {
            return Err(Error::Closed);
        }
        let native = if self.source.is_empty() {
            // Retrieve and content carry no body; every other body must exist.
            if !matches!(self.phase, Phase::Request(Action::Retrieve | Action::Content)) {
                return Err(Error::Truncated);
            }
            boundary::mapped(owner, |e| e.bytes(b"{}"))?
        } else {
            self.scanner.finish(&self.source)?;
            Document::from_scanned(owner, self.source, self.scanner)?
        };
        let id = self.job_id.as_deref();
        let usage = if validate(&native, self.phase, id)? {
            usage(&native)?
        } else {
            None
        };
        let (dialect, phase) = (self.dialect, self.phase);
        let mut operation = boundary::mapped(owner, |e| write_operation(&native, dialect, phase, id, echo, e))?;
        provenance::restore_bindings(&mut operation)?;
        Ok(Decoded { operation, usage })
    }
}

fn object(doc: &Document, node: usize) -> Result<()> {
    if matches!(doc.node(node)?.value, Value::Object) {
        Ok(())
    } else {
        Err(Error::Schema)
    }
}

/// An `error` that is exactly `{code, message}` maps; any other shape stays native.
fn mapped_error(doc: &Document, node: usize) -> Result<bool> {
    if !matches!(doc.node(node)?.value, Value::Object) {
        return Ok(false);
    }
    let (Some(code), Some(message)) = (doc.member(node, "code")?, doc.member(node, "message")?) else {
        return Ok(false);
    };
    if doc.children(node)?.count() != 2 {
        return Ok(false);
    }
    boundary::string(doc, code)?;
    boundary::string(doc, message)?;
    Ok(true)
}

/// Whether the field `key` at `node` is mapped into the operation value. A JSON null is
/// kept as a native field, so `completed_at: null` returns as written.
fn is_mapped(doc: &Document, node: usize, key: &str) -> Result<bool> {
    match doc.node(node)?.value {
        Value::Null => Ok(false),
        _ if key == "error" => mapped_error(doc, node),
        _ => Ok(true),
    }
}

fn mapped(doc: &Document, parent: usize, key: &str) -> Result<Option<usize>> {
    match doc.member(parent, key)? {
        Some(node) if is_mapped(doc, node, key)? => Ok(Some(node)),
        _ => Ok(None),
    }
}

fn count(doc: &Document, node: usize) -> Result<()> {
    if matches!(doc.node(node)?.value, Value::Number(_) | Value::Unsigned(_)) {
        Ok(())
    } else {
        boundary::string(doc, node)
    }
}

/// Validates the body and returns whether it is a job (a request has no usage to report).
fn validate(doc: &Document, phase: Phase, caller_id: Option<&str>) -> Result<bool> {
    let root = doc.root();
    object(doc, root)?;
    let action = match phase {
        Phase::Request(action) => action,
        Phase::Job | Phase::Accepted => return job_status(doc, phase, caller_id).map(|_| true),
    };
    for &key in known(action) {
        let Some(node) = mapped(doc, root, key)? else {
            continue;
        };
        match shape(key) {
            Shape::Text => boundary::string(doc, node)?,
            Shape::Count => count(doc, node)?,
            Shape::Structure => {
                if !matches!(doc.node(node)?.value, Value::Object | Value::Array) {
                    return Err(Error::Schema);
                }
            }
        }
    }
    Ok(false)
}

/// The wire id: `id` or `request_id`, never both.
fn wire_id(doc: &Document) -> Result<Option<usize>> {
    let root = doc.root();
    let (id, request) = (mapped(doc, root, "id")?, mapped(doc, root, "request_id")?);
    if id.is_some() && request.is_some() {
        return Err(Error::Schema);
    }
    let node = id.or(request);
    if let Some(node) = node {
        boundary::string(doc, node)?;
    }
    Ok(node)
}

fn job_status(doc: &Document, phase: Phase, caller_id: Option<&str>) -> Result<&'static str> {
    let root = doc.root();
    let id = wire_id(doc)?;
    if id.is_none() && (caller_id.is_none() || phase == Phase::Accepted) {
        return Err(Error::Schema);
    }
    for key in ["progress", "created_at", "completed_at"] {
        if let Some(node) = mapped(doc, root, key)? {
            let value = doc.unsigned(node)?;
            if key == "progress" && value > 100 {
                return Err(Error::Schema);
            }
        }
    }
    mapped(doc, root, "error")?;
    let Some(status) = mapped(doc, root, "status")? else {
        return if phase == Phase::Accepted {
            Ok("queued")
        } else {
            Err(Error::Schema)
        };
    };
    for (word, internal) in WIRE_STATUS {
        if doc.string_is(status, word)? {
            return Ok(internal);
        }
    }
    Err(Error::Schema)
}

fn usage(doc: &Document) -> Result<Option<Usage>> {
    let Some(node) = mapped(doc, doc.root(), "usage")? else {
        return Ok(None);
    };
    object(doc, node)?;
    let field = |name| -> Result<Option<u64>> { mapped(doc, node, name)?.map(|n| doc.unsigned(n)).transpose() };
    let usage = Usage {
        cost_in_usd_ticks: field("cost_in_usd_ticks")?,
        input_tokens: field("input_tokens")?,
        output_tokens: field("output_tokens")?,
        total_tokens: field("total_tokens")?,
    };
    Ok((usage != Usage::default()).then_some(usage))
}

fn write_operation(
    doc: &Document,
    dialect: Dialect,
    phase: Phase,
    caller_id: Option<&str>,
    echo: Option<&Document>,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    let (fields, implied) = match phase {
        Phase::Request(action) => {
            write_request_value(doc, action, caller_id, e)?;
            (known(action), [false; 2])
        }
        Phase::Job | Phase::Accepted => (&JOB_FIELDS[..], write_job_value(doc, phase, caller_id, e)?),
    };
    e.bytes(b"},\"x_ck\":{\"v\":1,\"original_protocol\":")?;
    e.string(dialect.name())?;
    for (flag, name) in implied.into_iter().zip(["id_implied", "status_implied"]) {
        if flag {
            e.bytes(b",")?;
            e.string(name)?;
            e.bytes(b":true")?;
        }
    }
    e.bytes(b",\"original\":[")?;
    let root = doc.root();
    let mut first = true;
    for field in doc.children(root)? {
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        let mut consumed = false;
        for key in fields {
            if doc.name_is(name, key)? {
                consumed = is_mapped(doc, field, key)?;
            }
        }
        if consumed {
            continue;
        }
        if !std::mem::replace(&mut first, false) {
            e.bytes(b",")?;
        }
        e.bytes(b"{\"protocol\":")?;
        e.string(dialect.name())?;
        e.bytes(b",\"pointer\":\"/")?;
        provenance::write_pointer_segment(doc, name, e)?;
        e.bytes(b"\",\"binding\":\"/value\",\"value\":")?;
        doc.write(field, e)?;
        e.bytes(b"}")?;
    }
    if let Some(echo) = echo {
        for entry in echo.children(echo.root())? {
            if !std::mem::replace(&mut first, false) {
                e.bytes(b",")?;
            }
            echo.write(entry, e)?;
        }
    }
    e.bytes(b"]}}")
}

/// `operation` with each member of the JSON object `members` added to `x_ck.original` as a native
/// field of `dialect`: what a route answers in `dialect` that the request it sends cannot carry.
/// The xAI executor returns such fields with the job ([`Ingress::echo`]), and only a `dialect`
/// client sees them.
pub fn with_originals(owner: &mut Writer, operation: &Document, dialect: Dialect, members: &[u8]) -> Result<Document> {
    let extra = boundary::mapped(owner, |e| e.bytes(members))?;
    object(&extra, extra.root())?;
    let root = operation.root();
    let x = operation.required(root, "x_ck")?;
    let original = operation.required(x, "original")?;
    let mut doc = boundary::mapped(owner, |e| {
        e.bytes(b"{\"kind\":")?;
        operation.write(operation.required(root, "kind")?, e)?;
        e.bytes(b",\"value\":")?;
        operation.write(operation.required(root, "value")?, e)?;
        e.bytes(b",\"x_ck\":{\"v\":1,\"original_protocol\":")?;
        operation.write(operation.required(x, "original_protocol")?, e)?;
        e.bytes(b",\"original\":[")?;
        let mut first = true;
        for entry in operation.children(original)? {
            if !std::mem::replace(&mut first, false) {
                e.bytes(b",")?;
            }
            operation.write(entry, e)?;
        }
        for field in extra.children(extra.root())? {
            if !std::mem::replace(&mut first, false) {
                e.bytes(b",")?;
            }
            e.bytes(b"{\"protocol\":")?;
            e.string(dialect.name())?;
            e.bytes(b",\"pointer\":\"/")?;
            provenance::write_pointer_segment(&extra, extra.node(field)?.name.ok_or(Error::Schema)?, e)?;
            e.bytes(b"\",\"binding\":\"/value\",\"value\":")?;
            extra.write(field, e)?;
            e.bytes(b"}")?;
        }
        e.bytes(b"]}}")
    })?;
    provenance::restore_bindings(&mut doc)?;
    Ok(doc)
}

fn write_request_value(doc: &Document, action: Action, id: Option<&str>, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"{\"kind\":\"video.request\",\"value\":{\"action\":")?;
    e.string(action.name())?;
    if let Some(id) = id {
        e.bytes(b",\"job_id\":")?;
        e.string(id)?;
    }
    for &key in known(action) {
        write_member(doc, key, e)?;
    }
    Ok(())
}

/// Returns whether the job id and the status were implied rather than on the wire.
fn write_job_value(doc: &Document, phase: Phase, id: Option<&str>, e: &mut Encoder<'_, '_>) -> Result<[bool; 2]> {
    let root = doc.root();
    e.bytes(b"{\"kind\":\"video.job\",\"value\":{\"job_id\":")?;
    let id_implied = match wire_id(doc)? {
        Some(node) => {
            doc.write(node, e)?;
            false
        }
        None => {
            e.string(id.ok_or(Error::Schema)?)?;
            true
        }
    };
    e.bytes(b",\"status\":")?;
    e.string(job_status(doc, phase, id)?)?;
    if let Some(node) = mapped(doc, root, "progress")? {
        e.bytes(b",\"progress\":")?;
        write_fraction(doc.unsigned(node)?, e)?;
    }
    for key in ["created_at", "completed_at", "error"] {
        write_member(doc, key, e)?;
    }
    Ok([id_implied, mapped(doc, root, "status")?.is_none()])
}

fn write_member(doc: &Document, key: &str, e: &mut Encoder<'_, '_>) -> Result<()> {
    if let Some(node) = mapped(doc, doc.root(), key)? {
        e.bytes(b",")?;
        e.string(key)?;
        e.bytes(b":")?;
        doc.write(node, e)?;
    }
    Ok(())
}
