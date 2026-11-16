//! Egress half of the video codec: operation values back to a dialect's wire shape.

use ck_stream::{ScratchLease, Writer};

use super::table::{STATUSES, accepts, known, wire_status};
use super::{ACTIONS, Action, Dialect, JOB_FIELDS};
use crate::{
    boundary,
    codec::{
        Error, Result,
        document::{Document, Value},
        encode::{self, Encoder},
        provenance,
    },
};

/// Writes `,"key":` before each member after the first.
struct Members(bool);

impl Members {
    fn key(&mut self, e: &mut Encoder<'_, '_>, name: &str) -> Result<()> {
        if !std::mem::replace(&mut self.0, false) {
            e.bytes(b",")?;
        }
        e.string(name)?;
        e.bytes(b":")
    }

    fn copy(&mut self, doc: &Document, parent: usize, name: &str, e: &mut Encoder<'_, '_>) -> Result<()> {
        if let Some(node) = doc.member(parent, name)? {
            self.key(e, name)?;
            doc.write(node, e)?;
        }
        Ok(())
    }
}

/// Progress is a percentage on the wire and a fraction in the operation value.
pub(super) fn write_fraction(percent: u64, e: &mut Encoder<'_, '_>) -> Result<()> {
    match percent {
        101.. => Err(Error::Schema),
        0 => e.bytes(b"0"),
        100 => e.bytes(b"1"),
        _ => {
            e.bytes(b"0.")?;
            e.unsigned(percent / 10)?;
            if !percent.is_multiple_of(10) {
                e.unsigned(percent % 10)?;
            }
            Ok(())
        }
    }
}

/// A fraction of at most two decimals back to a whole percentage.
// ponytail: finer progress than 1% is refused, not rounded; the wire has whole percentages.
fn percent(doc: &Document, node: usize) -> Result<u64> {
    let text = match doc.node(node)?.value {
        Value::Unsigned(n) if n <= 1 => return Ok(n * 100),
        Value::Number(text) => text,
        _ => return Err(Error::Schema),
    };
    let mut cursor = doc.source(text)?.cursor(text.span)?;
    let mut percent = match cursor.next_byte()? {
        Some(b'0') => 0,
        Some(b'1') => 100,
        _ => return Err(Error::Schema),
    };
    match cursor.next_byte()? {
        None => return Ok(percent),
        Some(b'.') => {}
        _ => return Err(Error::Schema),
    }
    let mut digits = 0;
    while let Some(byte) = cursor.next_byte()? {
        if !byte.is_ascii_digit() {
            return Err(Error::Schema);
        }
        let digit = u64::from(byte - b'0');
        match digits {
            0 => percent += digit * 10,
            1 => percent += digit,
            _ if digit != 0 => return Err(Error::Unsupported),
            _ => {}
        }
        digits += 1;
    }
    if digits == 0 || percent > 100 {
        return Err(Error::Schema);
    }
    Ok(percent)
}

fn for_each_original(
    doc: &Document,
    parent: usize,
    protocol: &str,
    mut visit: impl FnMut(usize, provenance::Segment<'_>) -> Result<()>,
) -> Result<()> {
    let original = doc.required(doc.required(doc.root(), "x_ck")?, "original")?;
    for entry in doc.origins(parent)? {
        if doc.node(entry)?.parent != Some(original) || !boundary::is(doc, entry, "protocol", protocol)? {
            continue;
        }
        visit(entry, provenance::leaf(doc.chars(doc.required(entry, "pointer")?)?)?)?;
    }
    Ok(())
}

fn has_original(doc: &Document, value: usize, protocol: &str, name: &str) -> Result<bool> {
    let mut found = false;
    for_each_original(doc, value, protocol, |_, leaf| {
        found |= leaf.is(name)?;
        Ok(())
    })?;
    Ok(found)
}

/// Native extras of `dialect`, except those an operation field now supersedes.
fn write_originals(
    doc: &Document,
    value: usize,
    dialect: Dialect,
    superseded: &[&str],
    members: &mut Members,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    for_each_original(doc, value, dialect.name(), |entry, leaf| {
        for &key in superseded {
            if leaf.is(key)? && (doc.member(value, key)?.is_some() || key == "id" || key == "request_id") {
                return Ok(());
            }
        }
        if !std::mem::replace(&mut members.0, false) {
            e.bytes(b",")?;
        }
        provenance::write_segment(leaf, e)?;
        e.bytes(b":")?;
        doc.write(doc.required(entry, "value")?, e)
    })
}

/// Encodes a trusted operation document in `dialect`'s wire shape. Call
/// `codec::provenance::restore_bindings` after reconstructing the document from canonical
/// JSON. A request without options or extras (retrieve, content) has no body: `None`, because
/// storage has no empty lease. For content, `variant` is a query parameter of the download URL,
/// not a body: the executor must read it from the operation value.
///
/// A job encodes its operation fields and its own dialect's native extras only. Fields an
/// OpenAI-shaped route also shows (`model`, `prompt`, `seconds`, `size`, `video_url`) come from
/// the request binding or the job table (E6, E7), not from this encoder.
pub fn encode(owner: &mut Writer, operation: &Document, dialect: Dialect) -> Result<Option<ScratchLease>> {
    operation.check_owner(owner)?;
    let root = operation.root();
    let value = operation.required(root, "value")?;
    if boundary::is(operation, root, "kind", "video.request")? {
        let mut action = None;
        for candidate in ACTIONS {
            if boundary::is(operation, value, "action", candidate.name())? {
                action = Some(candidate);
            }
        }
        let action = action.ok_or(Error::Schema)?;
        refuse_unsupported(operation, value, dialect, action)?;
        let empty = matches!(action, Action::Retrieve | Action::Content)
            && operation.children(value)?.count() <= 1 + usize::from(operation.member(value, "job_id")?.is_some())
            && !has_extras(operation, value, dialect)?;
        if empty {
            return Ok(None);
        }
        encode::scratch(owner, |e| write_request(operation, value, dialect, action, e)).map(Some)
    } else if boundary::is(operation, root, "kind", "video.job")? {
        encode::scratch(owner, |e| write_job(operation, value, dialect, e)).map(Some)
    } else {
        Err(Error::Unsupported)
    }
}

/// Whether `operation` is a `video.request` for `action` that `dialect` can express, without
/// writing: the check a destination's capability runs at the fence, so an option the dialect
/// lacks is refused before any attempt sends.
pub fn check_request(operation: &Document, action: Action, dialect: Dialect) -> Result<()> {
    let root = operation.root();
    if !boundary::is(operation, root, "kind", "video.request")? {
        return Err(Error::Unsupported);
    }
    let value = operation.required(root, "value")?;
    if !boundary::is(operation, value, "action", action.name())? {
        return Err(Error::Schema);
    }
    refuse_unsupported(operation, value, dialect, action)
}

fn has_extras(doc: &Document, value: usize, dialect: Dialect) -> Result<bool> {
    let mut any = false;
    for_each_original(doc, value, dialect.name(), |_, _| {
        any = true;
        Ok(())
    })?;
    Ok(any)
}

fn refuse_unsupported(doc: &Document, value: usize, dialect: Dialect, action: Action) -> Result<()> {
    for child in doc.children(value)? {
        let name = doc.node(child)?.name.ok_or(Error::Schema)?;
        if doc.name_is(name, "action")? || doc.name_is(name, "job_id")? {
            continue;
        }
        let mut supported = false;
        for &key in known(action) {
            if doc.name_is(name, key)? {
                supported = accepts(dialect, action, key);
            }
        }
        if !supported {
            return Err(Error::Unsupported);
        }
    }
    Ok(())
}

fn write_request(
    doc: &Document,
    value: usize,
    dialect: Dialect,
    action: Action,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    e.bytes(b"{")?;
    let mut members = Members(true);
    for &key in known(action) {
        members.copy(doc, value, key, e)?;
    }
    write_originals(doc, value, dialect, known(action), &mut members, e)?;
    e.bytes(b"}")
}

fn write_job(doc: &Document, value: usize, dialect: Dialect, e: &mut Encoder<'_, '_>) -> Result<()> {
    let x = doc.required(doc.root(), "x_ck")?;
    // The route supplied the id (and an acknowledgement implied "queued") when the body had none.
    // That is faithful only back to the dialect it came from: another dialect's job object
    // requires both, so they are written whenever the destination differs.
    let native = boundary::is(doc, x, "original_protocol", dialect.name())?;
    let implied = |name| -> Result<bool> { Ok(native && doc.member(x, name)?.is_some()) };
    e.bytes(b"{")?;
    let mut members = Members(true);
    if !implied("id_implied")? {
        members.key(e, if dialect == Dialect::OpenAi { "id" } else { "request_id" })?;
        doc.write(doc.required(value, "job_id")?, e)?;
    }
    // A job from another dialect has no `object`; OpenAI clients expect `video`.
    if dialect == Dialect::OpenAi && !native && !has_original(doc, value, dialect.name(), "object")? {
        members.key(e, "object")?;
        e.string("video")?;
    }
    if !implied("status_implied")? {
        let status = doc.required(value, "status")?;
        let mut word = None;
        for internal in STATUSES {
            if doc.string_is(status, internal)? {
                word = Some(wire_status(dialect, internal)?);
            }
        }
        members.key(e, "status")?;
        e.string(word.ok_or(Error::Schema)?)?;
    }
    if let Some(progress) = doc.member(value, "progress")? {
        members.key(e, "progress")?;
        e.unsigned(percent(doc, progress)?)?;
    }
    for key in ["created_at", "completed_at", "error"] {
        members.copy(doc, value, key, e)?;
    }
    write_originals(doc, value, dialect, &JOB_FIELDS, &mut members, e)?;
    e.bytes(b"}")
}
