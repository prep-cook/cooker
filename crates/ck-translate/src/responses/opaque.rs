use crate::codec::{
    Error, Result,
    document::{Document, Value},
    encode::Encoder,
};

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
            if doc.name_is(name, "sequence_number")? {
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
