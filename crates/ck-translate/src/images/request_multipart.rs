//! The `multipart/form-data` body of an image edit, rebuilt from the canonical request.

use super::request::{CONTROLS, Canonical, Control, KNOWN, PROTOCOL, address, present};
use crate::{
    boundary,
    codec::{
        Error, Result,
        document::{Document, Value},
        encode::Encoder,
        provenance,
    },
};

/// The delimiter and `Content-Disposition` name of one part; the caller closes the header.
fn form_part(name: &str, e: &mut Encoder<'_, '_>, boundary: &str) -> Result<()> {
    e.bytes(b"--")?;
    e.bytes(boundary.as_bytes())?;
    e.bytes(b"\r\nContent-Disposition: form-data; name=\"")?;
    for c in name.chars() {
        quoted_char(c, e)?;
    }
    e.bytes(b"\"")
}

/// Quoted-string content of a `Content-Disposition` parameter, escaped as browsers do.
fn quoted_char(c: char, e: &mut Encoder<'_, '_>) -> Result<()> {
    match c {
        '"' => e.bytes(b"%22"),
        '\r' => e.bytes(b"%0D"),
        '\n' => e.bytes(b"%0A"),
        c => e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes()),
    }
}

fn text_part(doc: &Document, name: &str, node: usize, boundary: &str, e: &mut Encoder<'_, '_>) -> Result<()> {
    form_part(name, e, boundary)?;
    e.bytes(b"\r\n\r\n")?;
    match doc.node(node)?.value {
        Value::Bool(_) | Value::Unsigned(_) | Value::Number(_) => doc.write(node, e)?,
        _ => {
            let mut chars = doc.chars(node)?;
            while let Some(c) = chars.next_scalar()? {
                e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes())?;
            }
        }
    }
    e.bytes(b"\r\n")
}

pub(super) fn form_body(
    doc: &Document,
    c: &Canonical,
    boundary: &str,
    same_protocol: bool,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    if boundary.len() < 32
        || !boundary
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(Error::Schema);
    }
    for (name, node) in [
        ("model", present(doc, c.root, "model")?),
        ("prompt", doc.member(c.root, "text")?),
        ("n", present(doc, c.root, "n")?),
    ] {
        if let Some(node) = node {
            text_part(doc, name, node, boundary, e)?;
        }
    }
    for (name, kind) in CONTROLS {
        if *kind == Control::Refs {
            continue;
        }
        if let Some(node) = present(doc, c.controls, name)? {
            text_part(doc, name, node, boundary, e)?;
        }
    }
    let mut count = 0;
    let single = present(doc, c.controls, "image")?;
    let images = present(doc, c.controls, "images")?;
    let total = usize::from(single.is_some()) + images.map_or(Ok(0), |n| doc.children(n).map(Iterator::count))?;
    for node in single
        .into_iter()
        .chain(images.map(|n| doc.children(n)).transpose()?.into_iter().flatten())
    {
        count += 1;
        file_part(
            doc,
            if total > 1 { "image[]" } else { "image" },
            node,
            count,
            boundary,
            e,
        )?;
    }
    if let Some(mask) = present(doc, c.controls, "mask")? {
        file_part(doc, "mask", mask, 0, boundary, e)?;
    }
    if same_protocol && let Some(original) = doc.member(c.x, "original")? {
        for entry in doc.children(original)? {
            if !boundary::is(doc, entry, "protocol", PROTOCOL)? {
                continue;
            }
            let mut name = provenance::leaf(doc.chars(doc.required(entry, "pointer")?)?)?;
            if KNOWN
                .iter()
                .try_fold(false, |found, key| Ok::<_, Error>(found || name.is(key)?))?
            {
                continue;
            }
            let value = doc.required(entry, "value")?;
            // A form field is text; any other JSON value has no form representation.
            if !matches!(doc.node(value)?.value, Value::String(_) | Value::Text(_)) {
                return Err(Error::Unsupported);
            }
            e.bytes(b"--")?;
            e.bytes(boundary.as_bytes())?;
            e.bytes(b"\r\nContent-Disposition: form-data; name=\"")?;
            while let Some(c) = name.next()? {
                quoted_char(c, e)?;
            }
            e.bytes(b"\"\r\n\r\n")?;
            let mut chars = doc.chars(value)?;
            while let Some(c) = chars.next_scalar()? {
                e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes())?;
            }
            e.bytes(b"\r\n")?;
        }
    }
    e.bytes(b"--")?;
    e.bytes(boundary.as_bytes())?;
    e.bytes(b"--\r\n")
}

/// A binary part from a data URL reference. A URL or file id has no bytes to send in a form.
fn file_part(
    doc: &Document,
    name: &str,
    node: usize,
    index: usize,
    boundary: &str,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    let url = address(doc, node)?.ok_or(Error::Unsupported)?;
    let mut mime = [0u8; 128];
    let mime_len = data_url_mime(doc, url, &mut mime)?;
    form_part(name, e, boundary)?;
    e.bytes(b"; filename=\"")?;
    match present(doc, node, "filename")? {
        Some(filename) => {
            let mut chars = doc.chars(filename)?;
            while let Some(c) = chars.next_scalar()? {
                quoted_char(c, e)?;
            }
        }
        None => {
            e.bytes(if name == "mask" { b"mask" } else { b"image" })?;
            if index != 0 {
                e.unsigned(index as u64)?;
            }
        }
    }
    e.bytes(b"\"\r\nContent-Type: ")?;
    e.bytes(&mime[..mime_len])?;
    e.bytes(b"\r\n\r\n")?;
    data_url_bytes(doc, url, |bytes| e.bytes(bytes))?;
    e.bytes(b"\r\n")
}

/// The media type of a `data:<type>;base64,` URL, which must name one.
fn data_url_mime(doc: &Document, node: usize, out: &mut [u8; 128]) -> Result<usize> {
    let mut chars = doc.chars(node)?;
    for expected in "data:".chars() {
        if chars.next_scalar()?.map(|c| c.to_ascii_lowercase()) != Some(expected) {
            return Err(Error::Unsupported);
        }
    }
    let mut len = 0;
    loop {
        let c = chars.next_scalar()?.ok_or(Error::Schema)?;
        if !c.is_ascii() || c.is_ascii_control() {
            return Err(Error::Schema);
        }
        if c == ',' {
            break;
        }
        if len == out.len() {
            return Err(Error::EventTooLarge);
        }
        out[len] = c as u8;
        len += 1;
    }
    // The media type must be named and followed by `;base64`: only that encoding has bytes to send.
    match len.checked_sub(7) {
        Some(mime) if mime > 0 && out[mime..len].eq_ignore_ascii_case(b";base64") => Ok(mime),
        _ => Err(Error::Unsupported),
    }
}

/// Decodes the payload of a data URL, one quartet at a time.
fn data_url_bytes(doc: &Document, node: usize, mut emit: impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
    use base64::Engine;
    let mut chars = doc.chars(node)?;
    while chars.next_scalar()?.ok_or(Error::Schema)? != ',' {}
    let mut quartet = [0u8; 4];
    let mut count = 0;
    let mut padded = false;
    while let Some(c) = chars.next_scalar()? {
        // Padding ends the data: more characters after it are not base64 of one image.
        if !c.is_ascii() || padded {
            return Err(Error::Schema);
        }
        quartet[count] = c as u8;
        count += 1;
        if count == 4 {
            padded = quartet[3] == b'=';
            let mut out = [0u8; 3];
            let len = base64::engine::general_purpose::STANDARD
                .decode_slice(quartet, &mut out)
                .map_err(|_| Error::Schema)?;
            emit(&out[..len])?;
            count = 0;
        }
    }
    if count != 0 {
        return Err(Error::Schema);
    }
    Ok(())
}
