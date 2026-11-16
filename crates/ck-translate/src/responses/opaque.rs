use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ck_stream::{ScratchLease, Writer};

use crate::codec::{
    Error, Result,
    document::{Document, Name, Value},
    encode::{self, Encoder},
};

/// The prefix of an `encrypted_content` that carries an Anthropic compaction block (Settled 71).
/// Standard and URL-safe base64 have no `.`, so it cannot open an OpenAI value.
const CARRIER: &str = "ck.anthropic-compaction.v1.";

/// Writes a Responses source's unknown event as that native event. A JSON object event carries
/// `sequence` as its `sequence_number`, so the client's numbering stays contiguous.
pub(super) fn write(doc: &Document, sequence: u64, e: &mut Encoder<'_, '_>) -> Result<()> {
    let root = doc.root();
    let x = doc.required(root, "x_ck")?;
    if let Some(source) = doc.member(x, "source")?
        && let Some(fields) = doc.member(source, "sse")?
    {
        for field in doc.children(fields)? {
            let name = doc.required(field, "name")?;
            if doc.string_is(name, "data")? {
                continue;
            }
            if doc
                .member(field, "comment")?
                .is_some_and(|n| matches!(doc.node(n).map(|n| n.value), Ok(Value::Bool(true))))
            {
                e.bytes(b":")?;
            } else {
                line(doc, name, e)?;
                e.bytes(b":")?;
            }
            e.bytes(b" ")?;
            line(doc, doc.required(field, "value")?, e)?;
            e.bytes(b"\n")?;
        }
    }
    e.bytes(b"data: ")?;
    let value = doc.required(root, "value")?;
    if super::is(doc, root, "format", "text")? {
        let mut chars = doc.chars(value)?;
        while let Some(c) = chars.next_scalar()? {
            match c {
                '\n' => e.bytes(b"\ndata: ")?,
                '\r' => return Err(Error::Schema),
                c => e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes())?,
            }
        }
    } else if matches!(doc.node(value)?.value, Value::Object) {
        e.bytes(b"{\"sequence_number\":")?;
        e.unsigned(sequence)?;
        for child in doc.children(value)? {
            let name = doc.node(child)?.name.ok_or(Error::Schema)?;
            if doc.name_is(name, "sequence_number")? || reserved(doc, name)? {
                continue;
            }
            e.bytes(b",")?;
            doc.write_name(name, e)?;
            e.bytes(b":")?;
            doc.write(child, e)?;
        }
        e.bytes(b"}")?;
    } else {
        doc.write(value, e)?;
    }
    e.bytes(b"\n\n")
}
/// Whether member `name` is `x_ck` or starts with `x_ck_`: the reserved prefix that no client
/// receives, whatever the member's origin.
pub(super) fn reserved(doc: &Document, name: Name) -> Result<bool> {
    let mut chars = doc.name_chars(name)?;
    for expected in "x_ck".chars() {
        if chars.next_scalar()? != Some(expected) {
            return Ok(false);
        }
    }
    Ok(matches!(chars.next_scalar()?, None | Some('_')))
}
fn line(doc: &Document, node: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let mut chars = doc.chars(node)?;
    while let Some(c) = chars.next_scalar()? {
        if matches!(c, '\r' | '\n') {
            return Err(Error::Schema);
        }
        e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes())?;
    }
    Ok(())
}

pub(super) fn preserve_carrier_collision(doc: &mut Document, incoming: usize) -> Result<()> {
    let Some(carrier) = doc.member(doc.root(), "x_ck_opaque")? else {
        return Ok(());
    };
    let Some(native) = doc.remove(incoming, "x_ck_opaque")? else {
        return Ok(());
    };
    let entry = doc.add(None, Value::Object)?;
    doc.append(carrier, entry)?;
    doc.set(entry, "name", Value::Text("responses.fields"))?;
    doc.set(entry, "format", Value::Text("json"))?;
    let value = doc.set(entry, "value", Value::Object)?;
    doc.append(value, native)?;
    let source = doc.set(entry, "source", Value::Object)?;
    doc.set(source, "protocol", Value::Text("responses"))?;
    Ok(())
}

/// Writes the carrier of `block`, the serialized native compaction block: [`CARRIER`] and the
/// block's bytes in unpadded URL-safe base64, so the block returns byte for byte.
pub(super) fn write_carrier(block: &ScratchLease, e: &mut Encoder<'_, '_>) -> Result<()> {
    let mut bytes = Vec::with_capacity(block.len());
    let mut offset = 0;
    while offset < block.len() {
        let chunk = block.chunk(offset)?;
        bytes.extend_from_slice(chunk);
        offset += chunk.len();
    }
    e.bytes(CARRIER.as_bytes())?;
    e.bytes(URL_SAFE_NO_PAD.encode(bytes).as_bytes())
}

/// The native block a carrier string `value` holds, imported into `doc`; `None` for any value
/// without the carrier prefix. A carrier that does not decode to a compaction block is refused.
pub(super) fn read_carrier(doc: &mut Document, owner: &mut Writer, value: usize) -> Result<Option<usize>> {
    let mut chars = doc.chars(value)?;
    for expected in CARRIER.chars() {
        if chars.next_scalar()? != Some(expected) {
            return Ok(None);
        }
    }
    let mut encoded = String::new();
    while let Some(c) = chars.next_scalar()? {
        encoded.push(c);
    }
    let bytes = URL_SAFE_NO_PAD.decode(encoded).map_err(|_| Error::Schema)?;
    let lease = encode::scratch(owner, |e| e.bytes(&bytes))?;
    let block = doc.import_json(owner, lease)?;
    if !matches!(doc.node(block)?.value, Value::Object) || !super::is(doc, block, "type", "compaction")? {
        return Err(Error::Schema);
    }
    super::string(doc, doc.required(block, "content")?)?;
    super::string(doc, doc.required(block, "signature")?)?;
    Ok(Some(block))
}
