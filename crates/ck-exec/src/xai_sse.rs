//! Whether the SSE events handed to the Responses ingress include an upstream failure, read
//! from the same bytes by the ingress's own rules (`ck_translate::codec::sse`): CR, LF or
//! CRLF ends a line; a line's field name runs to its first colon and its value loses one
//! leading space; the last `event` field names the event; `data` values join with LF; a
//! blank line dispatches the event. The ingress keeps no flag a caller can read, so replay
//! asks this reader before it caches a completion (`xai_events`).

use ck_wire::json::{self as gj, Kind};

/// Longest field name that matters (`event`, `data`).
const FIELD: usize = 5;
/// Longest event name kept: a longer one is no failure name.
const NAME: usize = 32;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Target {
    Event,
    Data,
    Other,
}

/// One reader over the bytes the ingress receives.
#[derive(Default)]
pub(crate) struct Failures {
    /// A failure event was dispatched.
    pub(crate) seen: bool,
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
    data: Option<Vec<u8>>,
}

impl Failures {
    /// Reads `bytes`. Data is kept only while `keep`: once a failure was seen, or when no
    /// completion would be cached, nothing is classified.
    pub(crate) fn read(&mut self, bytes: &[u8], keep: bool) {
        for &byte in bytes {
            if std::mem::take(&mut self.cr) && byte == b'\n' {
                continue;
            }
            match byte {
                b'\r' | b'\n' => {
                    self.cr = byte == b'\r';
                    self.end_line(keep);
                }
                _ => self.byte(byte, keep),
            }
        }
    }

    fn byte(&mut self, byte: u8, keep: bool) {
        self.started = true;
        let Some(target) = self.target else {
            if byte == b':' {
                self.open(keep);
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
            Target::Data => {
                if let Some(data) = self.data.as_mut().filter(|_| keep) {
                    data.push(byte);
                }
            }
            _ => {}
        }
    }

    /// The field name ended: its value starts.
    fn open(&mut self, keep: bool) {
        let target = match self.field.as_slice() {
            b"event" => Target::Event,
            b"data" => Target::Data,
            _ => Target::Other,
        };
        if target == Target::Data && keep {
            match self.data.as_mut() {
                Some(data) => data.push(b'\n'),
                None => self.data = Some(Vec::new()),
            }
        }
        self.value.clear();
        self.target = Some(target);
        self.space = true;
    }

    fn end_line(&mut self, keep: bool) {
        if !self.started {
            if let Some(data) = self.data.take() {
                self.seen |= failure(self.name.as_deref(), &data);
            }
            self.name = None;
            return;
        }
        if self.target.is_none() {
            // A line without a colon is a field with an empty value.
            self.open(keep);
        }
        if self.target == Some(Target::Event) {
            self.name = Some(std::mem::take(&mut self.value));
        }
        self.started = false;
        self.field.clear();
        self.target = None;
    }
}

/// The ingress's failure classes: `error` or `response.failed` by the event's `type`, else by
/// its name, else a non-null `error` member of an event with neither.
fn failure(name: Option<&[u8]>, data: &[u8]) -> bool {
    const FAILURES: [&[u8]; 2] = [b"error", b"response.failed"];
    if !gj::parse(data).is_object() {
        return false;
    }
    let kind = gj::get(data, "type");
    if kind.exists() {
        return kind.kind == Kind::String && FAILURES.contains(&&*kind.bytes());
    }
    if let Some(name) = name {
        return FAILURES.contains(&name);
    }
    let error = gj::get(data, "error");
    error.exists() && error.kind != Kind::Null
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seen(body: &str) -> bool {
        let mut failures = Failures::default();
        failures.read(body.as_bytes(), true);
        failures.seen
    }

    /// The ingress's failure forms count once their event ends, whatever the line endings
    /// or field order; others do not.
    #[test]
    fn failures_are_read_per_event() {
        assert!(seen("data: {\"message\":\"boom\"}\nevent: error\n\n"));
        assert!(seen("data: {\"type\":\ndata: \"error\",\"message\":\"boom\"}\n\n"));
        assert!(seen("event:error\r\ndata:{\"message\":\"boom\"}\r\n\r\n"));
        assert!(seen("data: {\"error\":{\"message\":\"boom\"}}\r\r"));
        assert!(!seen("data: {\"type\":\"error\"}\n"), "an event ends at its blank line");
        assert!(!seen("event: error\ndata: {\"type\":\"response.completed\"}\n\n"));
        assert!(!seen(": event: error\ndata: {\"message\":\"boom\"}\n\n"));
        assert!(!seen("event: error\nevent: response.created\ndata: {}\n\n"));
    }
}
