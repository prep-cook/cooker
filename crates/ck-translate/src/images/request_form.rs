//! The E1 multipart document of an image edit, read into the canonical request of `request`.
//! Image parts become data URLs inside the mapped canonical document, so every destination
//! encodes from one shape; field order between text and file parts is not kept.

use ck_stream::Writer;

use super::request::{CONTROLS, Control, PROTOCOL, RequestFailure};
use crate::{
    boundary,
    codec::{Error, Media, Operation, RequestView, Result, document::Document, encode::Encoder},
};

/// Names handled outside `CONTROLS`: the canonical members and the file parts.
const SPECIAL: &[&str] = &["model", "prompt", "n", "image", "image[]", "mask"];
/// Longest numeric control, digits only.
const NUMBER: usize = 20;

/// Reads the parts of an admitted `image_edit` multipart document. Each text part is one control
/// of the same name as in the JSON body; `image` and `image[]` parts are the inputs in order.
/// A duplicate control, a malformed number or boolean and a file part without a `Content-Type`
/// are refused as client errors.
pub fn from_form(owner: &mut Writer, view: RequestView<'_>) -> Result<Document, RequestFailure> {
    if view.operation != Operation::Media(Media::ImageEdit) {
        return Err(Error::Unsupported.into());
    }
    let doc = view.document;
    doc.check_owner(owner)?;
    let x = doc.required(doc.root(), "x_ck")?;
    let transport = doc.required(x, "transport")?;
    let parts = doc.required(transport, "parts")?;
    let form = Form { view, parts };
    form.validate()?;
    Ok(boundary::mapped(owner, |e| form.write(e))?)
}

struct Form<'a> {
    view: RequestView<'a>,
    parts: usize,
}

impl Form<'_> {
    fn doc(&self) -> &Document {
        self.view.document
    }

    fn named(&self, part: usize, name: &str) -> Result<bool> {
        self.doc().string_is(self.doc().required(part, "name")?, name)
    }

    fn find(&self, name: &str) -> Result<Option<usize>> {
        let mut found = None;
        for part in self.doc().children(self.parts)? {
            if self.named(part, name)? {
                if found.is_some() {
                    return Err(Error::Schema);
                }
                found = Some(part);
            }
        }
        Ok(found)
    }

    fn data(&self, part: usize) -> Result<usize> {
        self.doc().required(part, "data")
    }

    fn raw(&self, part: usize, emit: impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
        self.view.media.raw_bytes(self.doc(), self.data(part)?, emit)
    }

    /// A short control value, read whole.
    fn small(&self, part: usize) -> Result<([u8; NUMBER], usize)> {
        let mut out = [0; NUMBER];
        let mut len = 0;
        self.raw(part, |bytes| {
            let end = len + bytes.len();
            out.get_mut(len..end).ok_or(Error::Schema)?.copy_from_slice(bytes);
            len = end;
            Ok(())
        })?;
        Ok((out, len))
    }

    /// A JSON count: digits only. Anything else, a leading zero included, fails the canonical
    /// document's own parse.
    fn number(&self, part: usize) -> Result<()> {
        let (digits, len) = self.small(part)?;
        let digits = &digits[..len];
        if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
            return Err(Error::Schema);
        }
        Ok(())
    }

    fn is_known(&self, part: usize) -> Result<bool> {
        for name in SPECIAL.iter().chain(CONTROLS.iter().map(|(name, _)| name)) {
            if self.named(part, name)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn utf8(&self, part: usize) -> Result<()> {
        let mut check = Utf8::default();
        self.raw(part, |bytes| check.feed(bytes))?;
        check.finish()
    }

    /// Everything `write` relies on, checked first: the writer runs twice (count, then store).
    fn validate(&self) -> Result<()> {
        for name in ["model", "prompt", "n", "mask"] {
            self.find(name)?;
        }
        for (name, kind) in CONTROLS {
            let Some(part) = self.find(name)? else { continue };
            match kind {
                Control::Text => self.utf8(part)?,
                Control::Unsigned => self.number(part)?,
                Control::Bool => {
                    let (value, len) = self.small(part)?;
                    if !matches!(&value[..len], b"true" | b"false") {
                        return Err(Error::Schema);
                    }
                }
                Control::Refs => {}
            }
        }
        for name in ["model", "prompt"] {
            if let Some(part) = self.find(name)? {
                self.utf8(part)?;
            }
        }
        if self.find("prompt")?.is_none() {
            return Err(Error::Schema);
        }
        if let Some(part) = self.find("n")? {
            self.number(part)?;
        }
        let mut inputs = 0;
        for part in self.doc().children(self.parts)? {
            // OpenAI names the input parts `image` or `image[]`; a part `images` is no field of
            // either API, and writing it as a text control would drop it.
            if self.named(part, "images")? {
                return Err(Error::Schema);
            }
            if self.named(part, "image")? || self.named(part, "image[]")? || self.named(part, "mask")? {
                self.doc().member(part, "content_type")?.ok_or(Error::Schema)?;
                inputs += usize::from(!self.named(part, "mask")?);
            } else if !self.is_known(part)? {
                self.utf8(part)?;
            }
        }
        if inputs == 0 {
            return Err(Error::Schema);
        }
        Ok(())
    }

    fn string(&self, part: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
        e.quoted_json(|e| self.raw(part, |bytes| e.bytes(bytes)))
    }

    fn file(&self, part: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
        e.bytes(b"{\"image_url\":")?;
        let mime = self.doc().member(part, "content_type")?;
        self.view
            .media
            .write_value(self.doc(), self.data(part)?, (mime, true), e)?;
        if let Some(filename) = self.doc().member(part, "filename")? {
            e.bytes(b",\"filename\":")?;
            self.doc().write(filename, e)?;
        }
        e.bytes(b"}")
    }

    fn write(&self, e: &mut Encoder<'_, '_>) -> Result<()> {
        e.bytes(b"{")?;
        if let Some(part) = self.find("model")? {
            e.bytes(b"\"model\":")?;
            self.string(part, e)?;
            e.bytes(b",")?;
        }
        e.bytes(b"\"text\":")?;
        self.string(self.find("prompt")?.ok_or(Error::Schema)?, e)?;
        if let Some(part) = self.find("n")? {
            e.bytes(b",\"n\":")?;
            self.raw(part, |bytes| e.bytes(bytes))?;
        }
        e.bytes(b",\"x_ck\":{\"v\":1,\"operation\":\"image_edit\",\"original_protocol\":")?;
        e.string(PROTOCOL)?;
        e.bytes(b",\"provider_parameters\":{\"images\":{")?;
        let mut first = true;
        let mut member = |e: &mut Encoder<'_, '_>, name: &str| -> Result<()> {
            if !first {
                e.bytes(b",")?;
            }
            first = false;
            e.string(name)?;
            e.bytes(b":")
        };
        for (name, kind) in CONTROLS {
            match kind {
                Control::Refs => {
                    if *name == "images" {
                        let mut inputs = self.doc().children(self.parts)?.filter(|part| {
                            self.named(*part, "image").unwrap_or(false) || self.named(*part, "image[]").unwrap_or(false)
                        });
                        member(e, "images")?;
                        e.bytes(b"[")?;
                        for (i, part) in inputs.by_ref().enumerate() {
                            if i != 0 {
                                e.bytes(b",")?;
                            }
                            self.file(part, e)?;
                        }
                        e.bytes(b"]")?;
                    } else if *name == "mask"
                        && let Some(part) = self.find("mask")?
                    {
                        member(e, "mask")?;
                        self.file(part, e)?;
                    }
                }
                Control::Text => {
                    if let Some(part) = self.find(name)? {
                        member(e, name)?;
                        self.string(part, e)?;
                    }
                }
                Control::Unsigned | Control::Bool => {
                    if let Some(part) = self.find(name)? {
                        member(e, name)?;
                        self.raw(part, |bytes| e.bytes(bytes))?;
                    }
                }
            }
        }
        e.bytes(b"}}")?;
        let mut first = true;
        for part in self.doc().children(self.parts)? {
            if self.is_known(part)? {
                continue;
            }
            e.bytes(if first { b",\"original\":[" } else { b"," })?;
            first = false;
            e.bytes(b"{\"protocol\":")?;
            e.string(PROTOCOL)?;
            e.bytes(b",\"pointer\":\"/")?;
            let mut chars = self.doc().chars(self.doc().required(part, "name")?)?;
            while let Some(c) = chars.next_scalar()? {
                match c {
                    '~' => e.bytes(b"~0")?,
                    '/' => e.bytes(b"~1")?,
                    c => e.escaped(c.encode_utf8(&mut [0; 4]))?,
                }
            }
            e.bytes(b"\",\"value\":")?;
            self.string(part, e)?;
            e.bytes(b"}")?;
        }
        if !first {
            e.bytes(b"]")?;
        }
        e.bytes(b"}}")
    }
}

/// Incremental UTF-8 validation of a part that is read in pieces.
#[derive(Default)]
struct Utf8 {
    held: [u8; 4],
    len: usize,
}

impl Utf8 {
    fn feed(&mut self, bytes: &[u8]) -> Result<()> {
        for &byte in bytes {
            self.held[self.len] = byte;
            self.len += 1;
            match std::str::from_utf8(&self.held[..self.len]) {
                Ok(_) => self.len = 0,
                Err(error) if error.error_len().is_none() => {}
                Err(_) => return Err(Error::Utf8),
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<()> {
        if self.len == 0 { Ok(()) } else { Err(Error::Utf8) }
    }
}
