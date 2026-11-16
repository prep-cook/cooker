//! One linear, resumable scan of a Realtime message (RFC 8259 JSON). It validates the whole
//! message, decodes the keys and the few strings the gateway reads, and measures media
//! strings by length without decoding or copying them. A client message is also checked
//! against the documented control fields ([`Check`]). State is bounded: a fixed container
//! stack, one key buffer and the [`Envelope`]; a number of any length takes constant state.

use super::{
    check::Check,
    envelope::{At, Base64, Count, Envelope, FIELDS, MAX_LIMITS, Role, Text, member},
    schema,
};
use crate::codec::json::StringDecoder;

/// Containers nested deeper than this make the message unreadable.
pub(crate) const MAX_DEPTH: usize = 32;
const KEY: usize = 32;

/// Why a message could not be read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Fault {
    /// Not one complete JSON value, or invalid UTF-8 in a string.
    Json,
    /// The root is not an object.
    NotObject,
    TooDeep,
    /// A member the gateway reads or checks appears twice in its object.
    Duplicate(&'static str),
    /// A member the gateway reads or checks has the wrong JSON type.
    Kind(&'static str),
    /// A documented required member is absent.
    Missing(&'static str),
    /// A `type` or `role` names no documented variant.
    Value(&'static str),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    Start,
    Value,
    ArrayFirst,
    ObjectFirst,
    Key,
    Colon,
    After,
    KeyString,
    String,
    Number,
    Literal,
    End,
}

/// How a string value is kept.
#[derive(Clone, Copy, Debug)]
enum Sink {
    Skip,
    Text,
    Base64(Base64),
    /// A `data:` URI: `prefix` characters matched so far, then the base64 after the comma.
    DataUri {
        prefix: usize,
        data: Option<Base64>,
    },
}

#[derive(Clone, Copy, Debug)]
struct Frame {
    at: At,
    array: bool,
}

/// A JSON number: `uint` while it is a plain non-negative integer, `millis` while it is a
/// non-negative decimal without exponent, truncated to milliseconds (`reset_seconds`).
/// `fraction` counts retained fraction digits and stops at three, so state is the same
/// for any number of digits.
#[derive(Clone, Copy, Debug, Default)]
struct Number {
    step: u8,
    uint: Option<u64>,
    millis: Option<u64>,
    fraction: usize,
    integer: bool,
}

pub(crate) struct Reader {
    state: State,
    stack: [Frame; MAX_DEPTH],
    depth: usize,
    decoder: StringDecoder,
    key: [u8; KEY],
    key_len: usize,
    role: Role,
    index: Option<usize>,
    sink: Sink,
    number: Number,
    literal: &'static [u8],
    seen: u64,
    check: Check,
    pub(crate) envelope: Envelope,
}

impl Reader {
    pub(crate) fn new() -> Self {
        Self {
            state: State::Start,
            stack: [Frame {
                at: At::Root,
                array: false,
            }; MAX_DEPTH],
            depth: 0,
            decoder: StringDecoder::default(),
            key: [0; KEY],
            key_len: 0,
            role: Role::Skip,
            index: None,
            sink: Sink::Skip,
            number: Number::default(),
            literal: b"",
            seen: 0,
            check: Check::new(false),
            envelope: Envelope::default(),
        }
    }

    /// Reads one whole message; `validate` checks it as a client event.
    pub(crate) fn read(&mut self, message: &[u8], validate: bool) -> Result<&Envelope, Fault> {
        self.start(validate);
        self.feed(message)?;
        self.end()
    }

    pub(crate) fn start(&mut self, validate: bool) {
        *self = Self::new();
        self.check = Check::new(validate);
    }

    pub(crate) fn feed(&mut self, mut bytes: &[u8]) -> Result<(), Fault> {
        while let Some(&byte) = bytes.first() {
            let run = self.plain_run(bytes);
            if run > 0 {
                self.bulk(&bytes[..run]);
                bytes = &bytes[run..];
                continue;
            }
            if self.byte(byte)? {
                bytes = &bytes[1..];
            }
        }
        Ok(())
    }

    pub(crate) fn end(&mut self) -> Result<&Envelope, Fault> {
        if self.state != State::End {
            return Err(Fault::Json);
        }
        self.envelope.subtree = self.check.faults;
        Ok(&self.envelope)
    }

    /// The length of a leading run of plain string bytes the current sink only counts.
    fn plain_run(&self, bytes: &[u8]) -> usize {
        let counting = match self.sink {
            Sink::Skip | Sink::Base64(_) | Sink::DataUri { data: Some(_), .. } => true,
            Sink::Text | Sink::DataUri { data: None, .. } => false,
        };
        if self.state != State::String || !counting || !self.decoder.complete() || self.check.capturing() {
            return 0;
        }
        bytes
            .iter()
            .position(|&b| !(b' '..=b'~').contains(&b) || b == b'"' || b == b'\\')
            .unwrap_or(bytes.len())
    }

    fn bulk(&mut self, run: &[u8]) {
        let trailing = run.iter().rev().take_while(|&&b| b == b'=').count() as u64;
        let base64 = match &mut self.sink {
            Sink::Base64(base64) | Sink::DataUri { data: Some(base64), .. } => base64,
            _ => return,
        };
        base64.chars += run.len() as u64;
        base64.padding = if trailing == run.len() as u64 {
            base64.padding + trailing
        } else {
            trailing
        };
    }

    /// Handles one byte; returns false when the byte ended a number and must be read again.
    fn byte(&mut self, byte: u8) -> Result<bool, Fault> {
        let space = matches!(byte, b' ' | b'\t' | b'\n' | b'\r');
        match self.state {
            State::String | State::KeyString => self.string(byte)?,
            State::Number => return self.number(byte),
            State::Literal => {
                let (&expected, rest) = self.literal.split_first().ok_or(Fault::Json)?;
                if byte != expected {
                    return Err(Fault::Json);
                }
                self.literal = rest;
                if rest.is_empty() {
                    self.close_value();
                }
            }
            _ if space => {}
            State::Start => {
                if byte != b'{' {
                    return Err(Fault::NotObject);
                }
                self.role = Role::Enter(At::Root);
                self.value(byte)?;
            }
            State::Value => self.value(byte)?,
            State::ArrayFirst if byte == b']' => self.close_container(byte)?,
            State::ArrayFirst => self.value(byte)?,
            State::ObjectFirst | State::Key if byte == b'"' => {
                self.key_len = 0;
                self.state = State::KeyString;
            }
            State::ObjectFirst if byte == b'}' => self.close_container(byte)?,
            State::Colon if byte == b':' => self.state = State::Value,
            State::After if byte == b',' => self.next_member(),
            State::After if byte == b'}' || byte == b']' => self.close_container(byte)?,
            _ => return Err(Fault::Json),
        }
        Ok(true)
    }

    fn next_member(&mut self) {
        let top = self.stack[self.depth - 1];
        if top.array {
            self.role = if top.at == At::Limits {
                Role::LimitItem
            } else {
                Role::Skip
            };
            self.index = None;
            self.check.element(self.depth);
            self.state = State::Value;
        } else {
            self.state = State::Key;
        }
    }

    fn string(&mut self, byte: u8) -> Result<(), Fault> {
        if byte == b'"' && self.decoder.complete() {
            if self.state == State::KeyString {
                return self.end_key();
            }
            self.end_string();
            return Ok(());
        }
        let Some(c) = self.decoder.push(byte).map_err(|_| Fault::Json)? else {
            return Ok(());
        };
        if self.state == State::KeyString {
            // Names the gateway reads are short ASCII; anything else matches none.
            if self.key_len < KEY && c.is_ascii() {
                self.key[self.key_len] = c as u8;
                self.key_len += 1;
            } else {
                self.key_len = KEY + 1;
            }
            return Ok(());
        }
        self.check.char(self.depth, c);
        if let Sink::Text = self.sink {
            self.text().push(c);
            return Ok(());
        }
        match &mut self.sink {
            Sink::Skip | Sink::Text => {}
            Sink::Base64(base64) | Sink::DataUri { data: Some(base64), .. } => {
                base64.chars += 1;
                base64.padding = if c == '=' { base64.padding + 1 } else { 0 };
            }
            Sink::DataUri { prefix: 5, data } => {
                if c == ',' {
                    *data = Some(Base64::default());
                }
            }
            Sink::DataUri { prefix, .. } => {
                // The URI scheme is case-insensitive (RFC 2397, RFC 3986 §3.1).
                if c.is_ascii() && (c as u8).eq_ignore_ascii_case(&b"data:"[*prefix]) {
                    *prefix += 1;
                } else {
                    self.sink = Sink::Skip;
                }
            }
        }
        Ok(())
    }

    fn end_key(&mut self) -> Result<(), Fault> {
        let top = self.stack[self.depth - 1];
        let key = if self.key_len <= KEY {
            &self.key[..self.key_len]
        } else {
            b""
        };
        let (role, index) = member(top.at, key);
        if let Some(index) = index {
            if self.seen & (1 << index) != 0 {
                return Err(Fault::Duplicate(FIELDS[index].3));
            }
            self.seen |= 1 << index;
        }
        self.check.key(self.depth, key)?;
        self.role = role;
        self.index = index;
        self.state = State::Colon;
        Ok(())
    }

    fn param(&self) -> &'static str {
        match (self.index, self.role) {
            (Some(index), _) => FIELDS[index].3,
            _ => "rate_limits",
        }
    }

    fn value(&mut self, byte: u8) -> Result<(), Fault> {
        let role = self.role;
        let typed = !matches!(
            role,
            Role::Skip | Role::Audio | Role::Image | Role::Delta | Role::LimitItem
        );
        let kind = match byte {
            b'{' => schema::O,
            b'[' => schema::A,
            b'"' => schema::S,
            b't' | b'f' => schema::B,
            b'n' => schema::Z,
            _ => schema::I | schema::N,
        };
        self.check.value(self.depth, kind)?;
        match byte {
            b'{' | b'[' => self.open(byte == b'[', role, typed)?,
            b'"' => {
                self.sink = match role {
                    Role::Text(_) | Role::LimitName => {
                        *self.text() = Text::Value([0; Text::MAX], 0);
                        Sink::Text
                    }
                    Role::RootAudio | Role::Audio | Role::Delta => Sink::Base64(Base64::default()),
                    Role::Image => Sink::DataUri { prefix: 0, data: None },
                    Role::Skip | Role::LimitItem => Sink::Skip,
                    _ => return Err(Fault::Kind(self.param())),
                };
                self.check.string(self.depth);
                self.state = State::String;
            }
            b'-' | b'0'..=b'9' => {
                if typed
                    && !matches!(
                        role,
                        Role::Count(_) | Role::LimitLimit | Role::LimitRemaining | Role::LimitReset
                    )
                {
                    return Err(Fault::Kind(self.param()));
                }
                self.number = Number::default();
                self.state = State::Number;
                self.number(byte)?;
            }
            b'n' => {
                self.set_null();
                self.literal = b"ull";
                self.state = State::Literal;
            }
            b't' => (self.literal, self.state) = (b"rue", State::Literal),
            b'f' => (self.literal, self.state) = (b"alse", State::Literal),
            _ => return Err(Fault::Json),
        }
        Ok(())
    }

    fn open(&mut self, array: bool, role: Role, typed: bool) -> Result<(), Fault> {
        let at = match role {
            Role::Enter(at) if array == (at == At::Limits) => at,
            Role::LimitItem if !array && self.envelope.limit_count < MAX_LIMITS => At::Limit,
            _ if typed => return Err(Fault::Kind(self.param())),
            _ => At::Other,
        };
        if self.depth == MAX_DEPTH {
            return Err(Fault::TooDeep);
        }
        self.check.open(self.depth, array);
        if at != At::Other {
            self.envelope.entered |= 1 << at as u16;
        }
        self.stack[self.depth] = Frame { at, array };
        self.depth += 1;
        self.role = if at == At::Limits { Role::LimitItem } else { Role::Skip };
        self.index = None;
        self.state = if array { State::ArrayFirst } else { State::ObjectFirst };
        Ok(())
    }

    fn close_container(&mut self, byte: u8) -> Result<(), Fault> {
        let top = self.stack[self.depth - 1];
        if top.array != (byte == b']') {
            return Err(Fault::Json);
        }
        if !top.array {
            self.check.close(self.depth)?;
        }
        self.depth -= 1;
        if top.at == At::Limit {
            self.envelope.limit_count += 1;
        }
        self.close_value();
        Ok(())
    }

    fn close_value(&mut self) {
        self.state = if self.depth == 0 { State::End } else { State::After };
    }

    fn end_string(&mut self) {
        self.check.end_string();
        let sink = std::mem::replace(&mut self.sink, Sink::Skip);
        let base64 = match sink {
            Sink::Base64(base64) | Sink::DataUri { data: Some(base64), .. } => base64,
            _ => {
                self.close_value();
                return;
            }
        };
        match self.role {
            Role::RootAudio => self.envelope.root_audio = Some(base64),
            Role::Delta => self.envelope.delta = Some(base64),
            _ => self.envelope.media += base64.decoded(),
        }
        self.close_value();
    }

    fn number(&mut self, byte: u8) -> Result<bool, Fault> {
        let n = &mut self.number;
        let digit = byte.is_ascii_digit();
        // Steps: 0 start, 1 after '-', 2 after a leading '0', 3 integer digits, 4 after '.',
        // 5 fraction digits, 6 after 'e', 7 after the exponent sign, 8 exponent digits.
        let next = match (n.step, byte) {
            (0, b'-') => 1,
            (0 | 1, b'0') => 2,
            (0 | 1, _) if digit => 3,
            (3, _) if digit => 3,
            (2 | 3, b'.') => 4,
            (4 | 5, _) if digit => 5,
            (2 | 3 | 5, b'e' | b'E') => 6,
            (6, b'+' | b'-') => 7,
            (6..=8, _) if digit => 8,
            (2 | 3 | 5 | 8, _) => {
                self.end_number()?;
                return Ok(false);
            }
            _ => return Err(Fault::Json),
        };
        if n.step == 0 {
            n.uint = Some(0);
            n.millis = Some(0);
            n.integer = true;
        }
        let value = u64::from(byte.wrapping_sub(b'0'));
        match next {
            2 | 3 => {
                n.uint = n.uint.and_then(|v| v.checked_mul(10)?.checked_add(value));
                n.millis = n.uint.and_then(|v| v.checked_mul(1000));
            }
            4 => (n.uint, n.integer) = (None, false),
            5 => {
                if let Some(&scale) = [100, 10, 1].get(n.fraction) {
                    n.millis = n.millis.and_then(|v| v.checked_add(value * scale));
                    n.fraction += 1;
                }
            }
            6 => (n.uint, n.millis, n.integer) = (None, None, false),
            1 | 7 | 8 => (n.uint, n.millis) = (None, None),
            _ => {}
        }
        n.step = next;
        Ok(true)
    }

    fn end_number(&mut self) -> Result<(), Fault> {
        let Number {
            uint, millis, integer, ..
        } = self.number;
        self.check.number(self.depth, integer)?;
        let count = |value: Option<u64>| value.map(Count::Known);
        let limit = self.envelope.limit_count;
        match self.role {
            Role::Count(slot) => {
                self.envelope.counts[slot as usize] = count(uint).ok_or(Fault::Kind(self.param()))?;
            }
            Role::LimitLimit | Role::LimitRemaining if limit < MAX_LIMITS => {
                let value = count(uint).ok_or(Fault::Kind("rate_limits"))?;
                let entry = &mut self.envelope.limits[limit];
                if self.role == Role::LimitLimit {
                    entry.limit = value;
                } else {
                    entry.remaining = value;
                }
            }
            Role::LimitReset if limit < MAX_LIMITS => self.envelope.limits[limit].reset_ms = millis,
            _ => {}
        }
        self.close_value();
        Ok(())
    }

    fn set_null(&mut self) {
        let limit = self.envelope.limit_count.min(MAX_LIMITS - 1);
        match self.role {
            Role::Text(_) | Role::LimitName => *self.text() = Text::Null,
            Role::Count(slot) => self.envelope.counts[slot as usize] = Count::Null,
            Role::LimitLimit => self.envelope.limits[limit].limit = Count::Null,
            Role::LimitRemaining => self.envelope.limits[limit].remaining = Count::Null,
            Role::Enter(at) => self.envelope.nulls |= 1 << at as u16,
            _ => {}
        }
    }

    /// The text slot the current role writes. Only text roles call this.
    fn text(&mut self) -> &mut Text {
        let limit = self.envelope.limit_count.min(MAX_LIMITS - 1);
        match self.role {
            Role::Text(slot) => &mut self.envelope.texts[slot as usize],
            _ => &mut self.envelope.limits[limit].name,
        }
    }
}

#[cfg(test)]
mod tests;
