//! What the Responses ingress made of the SSE events it was handed, for reasoning replay
//! (`xai_events`): whether an upstream failure ended the response, and the output of a
//! completion it accepted. The ingress keeps no state a caller can read, so this reader
//! follows it over the same bytes. Lines and fields follow its scanner
//! (`ck_translate::codec::sse`): a leading UTF-8 BOM is skipped; CR, LF or CRLF ends a line;
//! a field name runs to the first colon and its value loses one leading space; the last
//! `event` field names the event; `data` values join with LF; a blank line dispatches. The
//! joined data goes to the response stream's scratch and is parsed with the ingress's own
//! `Document::parse`, so malformed JSON is opaque here as there; a parsed event is classed
//! as the ingress classes it: by `type`, else by name, else by a non-null `error` member.

use ck_stream::Writer;
use ck_translate::codec::document::{Document, Value};
use ck_translate::codec::source::Source;
use ck_translate::codec::{self, encode};

use crate::xai_exec::{object_member, text_is};

const BOM: &[u8; 3] = b"\xef\xbb\xbf";
/// Longest field name that matters (`event`, `data`).
const FIELD: usize = 5;
/// Longest event name kept: a longer one names no class.
const NAME: usize = 32;
/// The output item types replay keeps.
const KEPT: [&str; 4] = ["reasoning", "message", "function_call", "custom_tool_call"];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Target {
    Event,
    Data,
    Other,
}

enum Class {
    Failure,
    Completed,
    Other,
}

/// One reader over the bytes the ingress receives.
#[derive(Default)]
pub(crate) struct Accepted {
    /// A failure event was dispatched, or the reader could not follow the ingress: no
    /// completion after it is cached, and no data is kept.
    pub(crate) failed: bool,
    /// Bytes read, up to the BOM's length, and those bytes.
    read: usize,
    head: [u8; 3],
    /// The previous byte was CR, so an LF right after it ends no line.
    cr: bool,
    /// The current line has a byte.
    started: bool,
    /// The current line's field name, up to [`FIELD`] bytes and an overflow mark.
    field: Vec<u8>,
    /// Where the current line's value goes once its colon was read.
    target: Option<Target>,
    /// The next value byte is the one leading space a value loses.
    space: bool,
    /// The current line's `event` value.
    value: Vec<u8>,
    /// The event's name so far.
    name: Option<Vec<u8>>,
    /// The event's joined data so far; `None` before its first data line.
    data: Option<Source>,
}

impl Accepted {
    /// Reads `bytes`, which the ingress has just accepted: the replay items of the last
    /// completion they dispatched with no failure before it, if any.
    pub(crate) fn read(&mut self, owner: &mut Writer, bytes: &[u8]) -> Option<Vec<Vec<u8>>> {
        let mut completed = None;
        let mut i = 0;
        while i < bytes.len() {
            let byte = bytes[i];
            i += 1;
            if self.read < BOM.len() {
                self.head[self.read] = byte;
                self.read += 1;
                if self.read == BOM.len() && self.head == *BOM {
                    self.reset_line();
                    continue;
                }
            }
            if std::mem::take(&mut self.cr) && byte == b'\n' {
                continue;
            }
            match byte {
                b'\r' | b'\n' => {
                    self.cr = byte == b'\r';
                    if let Some(items) = self.end_line(owner) {
                        completed = Some(items);
                    }
                }
                _ if self.target == Some(Target::Data) && !self.space => {
                    // The rest of a data value up to the line's end, in one write.
                    let end = bytes[i..]
                        .iter()
                        .position(|b| matches!(b, b'\r' | b'\n'))
                        .map_or(bytes.len(), |n| i + n);
                    self.push(owner, &bytes[i - 1..end]);
                    i = end;
                }
                _ => self.byte(owner, byte),
            }
        }
        completed
    }

    fn byte(&mut self, owner: &mut Writer, byte: u8) {
        self.started = true;
        let Some(target) = self.target else {
            if byte == b':' {
                self.open(owner);
            } else if self.field.len() <= FIELD {
                self.field.push(byte);
            }
            return;
        };
        if std::mem::take(&mut self.space) && byte == b' ' {
            return;
        }
        match target {
            Target::Event if self.value.len() <= NAME => self.value.push(byte),
            Target::Data => self.push(owner, &[byte]),
            _ => {}
        }
    }

    /// Appends to the event's data; a storage failure stops the reader.
    fn push(&mut self, owner: &mut Writer, bytes: &[u8]) {
        self.started = true;
        if let Some(data) = self.data.as_mut()
            && data.push_bytes(owner, bytes).is_err()
        {
            self.fail();
        }
    }

    /// The field name ended: its value starts.
    fn open(&mut self, owner: &mut Writer) {
        let target = match self.field.as_slice() {
            b"event" => Target::Event,
            b"data" => Target::Data,
            _ => Target::Other,
        };
        if target == Target::Data && !self.failed {
            match self.data.as_mut() {
                Some(_) => self.push(owner, b"\n"),
                None => match Source::new(owner) {
                    Ok(source) => self.data = Some(source),
                    Err(_) => self.fail(),
                },
            }
        }
        self.value.clear();
        self.target = Some(target);
        self.space = true;
    }

    fn end_line(&mut self, owner: &mut Writer) -> Option<Vec<Vec<u8>>> {
        if !self.started {
            let (data, name) = (self.data.take(), self.name.take());
            return self.dispatch(owner, data?, name.as_deref());
        }
        if self.target.is_none() {
            // A line without a colon is a field with an empty value.
            self.open(owner);
        }
        if self.target == Some(Target::Event) {
            self.name = Some(std::mem::take(&mut self.value));
        }
        self.reset_line();
        None
    }

    fn reset_line(&mut self) {
        self.started = false;
        self.field.clear();
        self.target = None;
        self.space = false;
    }

    fn fail(&mut self) {
        self.failed = true;
        self.data = None;
    }

    /// One event the ingress dispatched: the replay items of an accepted completion.
    fn dispatch(&mut self, owner: &mut Writer, data: Source, name: Option<&[u8]>) -> Option<Vec<Vec<u8>>> {
        if self.failed {
            return None;
        }
        let len = data.len();
        let doc = match Document::parse(owner, data, len) {
            Ok(doc) => doc,
            // Opaque text to the ingress, as an empty data value is.
            Err(codec::Error::Json | codec::Error::Truncated) => return None,
            Err(_) => {
                self.fail();
                return None;
            }
        };
        match class(&doc, name) {
            Ok(Class::Other) => None,
            Ok(Class::Completed) => match items(owner, &doc) {
                Ok(items) => items,
                Err(_) => {
                    self.fail();
                    None
                }
            },
            Ok(Class::Failure) | Err(_) => {
                self.fail();
                None
            }
        }
    }
}

/// The ingress's class of a parsed event, as far as replay needs it.
fn class(doc: &Document, name: Option<&[u8]>) -> codec::Result<Class> {
    let root = doc.root();
    if !matches!(doc.node(root)?.value, Value::Object) {
        return Ok(Class::Other);
    }
    let named = |kind: &[u8]| match kind {
        b"error" | b"response.failed" => Class::Failure,
        b"response.completed" => Class::Completed,
        _ => Class::Other,
    };
    if let Some(kind) = doc.member(root, "type")? {
        if !matches!(doc.node(kind)?.value, Value::String(_)) {
            return Ok(Class::Other);
        }
        for known in ["error", "response.failed", "response.completed"] {
            if doc.string_is(kind, known)? {
                return Ok(named(known.as_bytes()));
            }
        }
        return Ok(Class::Other);
    }
    if let Some(name) = name {
        return Ok(named(name));
    }
    Ok(match doc.member(root, "error")? {
        Some(error) if !matches!(doc.node(error)?.value, Value::Null) => Class::Failure,
        _ => Class::Other,
    })
}

/// The output items of a completion replay keeps, each written out on its own; `None`
/// when the completion has no output array.
fn items(owner: &mut Writer, doc: &Document) -> codec::Result<Option<Vec<Vec<u8>>>> {
    let Some(response) = object_member(doc, doc.root(), "response")? else {
        return Ok(None);
    };
    let Some(output) = object_member(doc, response, "output")? else {
        return Ok(None);
    };
    if !matches!(doc.node(output)?.value, Value::Array) {
        return Ok(None);
    }
    let mut items = Vec::new();
    for item in doc.children(output)? {
        let mut kept = false;
        for kind in KEPT {
            kept |= text_is(doc, item, "type", kind)?;
        }
        if !kept {
            continue;
        }
        let lease = encode::scratch(owner, |e| doc.write(item, e))?;
        let mut bytes = Vec::with_capacity(lease.len());
        while bytes.len() < lease.len() {
            bytes.extend_from_slice(lease.chunk(bytes.len()).map_err(codec::Error::Storage)?);
        }
        items.push(bytes);
    }
    Ok(Some(items))
}

#[cfg(test)]
mod tests {
    use super::*;

    const COMPLETED: &str = "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[{\"type\":\"reasoning\",\"id\":\"rs\",\"summary\":[],\"encrypted_content\":\"E\"},{\"type\":\"web_search_call\",\"id\":\"w\"}]}}\n\n";
    const ITEM: &str = "{\"type\":\"reasoning\",\"id\":\"rs\",\"summary\":[],\"encrypted_content\":\"E\"}";

    fn writer() -> (ck_stream::Store, std::path::PathBuf) {
        let directory = std::env::temp_dir().join(format!("ck-exec-xai-sse-{}", uuid::Uuid::new_v4()));
        let mut config = ck_stream::Config::new(directory.clone());
        config.pool_bytes = Some(64 * ck_stream::FRAME_BYTES);
        config.max_event_bytes = 4 * ck_stream::FRAME_BYTES;
        config.header_slots = 4;
        config.workers = 1;
        (ck_stream::Store::open(config).expect("open the test store"), directory)
    }

    /// The reader's verdict on `body` fed in `pieces`: whether it saw a failure, and the
    /// replay items of the last completion.
    fn verdict(owner: &mut Writer, pieces: &[&[u8]]) -> (bool, Option<Vec<String>>) {
        let mut accepted = Accepted::default();
        let mut items = None;
        for piece in pieces {
            if let Some(read) = accepted.read(owner, piece) {
                items = Some(read.into_iter().map(|i| String::from_utf8(i).unwrap()).collect());
            }
        }
        (accepted.failed, items)
    }

    /// Each body reads the same whole, at every two-way split and byte by byte: failures as
    /// the ingress classes them, malformed JSON as opaque text, a BOM skipped, and a
    /// completion only as a whole event.
    #[test]
    fn every_split_reads_as_the_ingress_does() {
        let (store, directory) = writer();
        let mut owner = store.create().unwrap();
        let failed = (true, None);
        let cached = (false, Some(vec![ITEM.to_owned()]));
        let completed_object = &COMPLETED["data: ".len()..COMPLETED.len() - 2];
        let cases = [
            (
                format!("data:{{\"type\":\"error\"}}junk\n\n{COMPLETED}"),
                cached.clone(),
            ),
            (format!("data:{{\"type\":\"error\"\n\n{COMPLETED}"), cached.clone()),
            (
                format!("\u{feff}data:{{\"type\":\"error\",\"message\":\"boom\"}}\n\n{COMPLETED}"),
                failed.clone(),
            ),
            (
                format!("\u{feff}event:error\ndata:{{\"message\":\"boom\"}}\n\n{COMPLETED}"),
                failed.clone(),
            ),
            (
                format!("data: {{\"message\":\"boom\"}}\r\nevent: error\r\n\r\n{COMPLETED}"),
                failed.clone(),
            ),
            (
                format!("data: {{\"type\":\"error\",\"n\":\ndata: {completed_object}\ndata: }}\n\n"),
                failed.clone(),
            ),
            (
                format!(": event: error\ndata: {{\"message\":\"boom\"}}\n\n{COMPLETED}"),
                cached.clone(),
            ),
            (format!("\u{feff}{COMPLETED}"), cached.clone()),
        ];
        for (body, want) in cases {
            let bytes = body.as_bytes();
            assert_eq!(verdict(&mut owner, &[bytes]), want, "{body:?}");
            for at in 0..=bytes.len() {
                assert_eq!(
                    verdict(&mut owner, &[&bytes[..at], &bytes[at..]]),
                    want,
                    "{body:?} at {at}"
                );
            }
            let single: Vec<&[u8]> = bytes.chunks(1).collect();
            assert_eq!(verdict(&mut owner, &single), want, "{body:?} byte by byte");
        }
        drop(owner);
        drop(store);
        let _ = std::fs::remove_dir_all(directory);
    }

    /// Once a failure is read, the reader keeps no data, even in the same read.
    #[test]
    fn a_failure_drops_the_data() {
        let (store, directory) = writer();
        let mut owner = store.create().unwrap();
        let mut accepted = Accepted::default();
        let body = format!("data:{{\"type\":\"error\"}}\r\rdata:{}", "x".repeat(1 << 16));
        assert!(accepted.read(&mut owner, body.as_bytes()).is_none());
        assert!(accepted.failed && accepted.data.is_none());
        drop(owner);
        drop(store);
        let _ = std::fs::remove_dir_all(directory);
    }
}
