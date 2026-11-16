//! Finished video bytes as one media lifetime: `response.start`, `media.start` with the MIME
//! the upstream sent (none when it sent none; a MIME is never guessed), data records of at most one record's capacity, `media.end` with the
//! byte count, then `done`. Bytes are published exactly as received; nothing is re-encoded.
//!
//! A body that ends short of its declared length, runs past it, or is cut by the transport is
//! an error event and an error result, never a shorter success.

use std::time::{Duration, Instant};

use ck_stream::Writer;

use crate::{
    boundary,
    codec::{
        Error, Result,
        event::{Direction, Emitter, Identity, Kind, Scope},
        media::{Publisher, local_id},
    },
};

const DATA_BYTES: usize = 36_864;
const FLUSH_DELAY: Duration = Duration::from_millis(100);
const MAX_JOB_ID: usize = 256;
const MAX_MIME: usize = 255;

/// One download. Bytes collect until a record is full or 100 ms have passed since the oldest
/// buffered byte, so the published records do not depend on how the network sliced the body.
pub struct Download {
    owner: u64,
    emitter: Emitter,
    publisher: Publisher,
    expected: Option<u64>,
    received: u64,
    buffer: [u8; DATA_BYTES],
    buffered: usize,
    deadline: Option<Instant>,
    closed: bool,
}

impl Download {
    /// `expected` is the declared `Content-Length`, if any; `mime` is the upstream's
    /// `Content-Type` exactly as sent, or `None` when it sent none.
    pub fn new(
        owner: &mut Writer,
        identity: Identity,
        job_id: &str,
        mime: Option<&str>,
        expected: Option<u64>,
    ) -> Result<Self> {
        if identity.direction != Direction::Response
            || job_id.is_empty()
            || job_id.len() > MAX_JOB_ID
            || !mime.is_none_or(usable_mime)
        {
            return Err(Error::Schema);
        }
        let mut emitter = Emitter::new(identity);
        emitter.emit(
            owner,
            Kind::ResponseStart,
            Scope::default(),
            |e| {
                e.bytes(b",\"id\":")?;
                e.string(job_id)?;
                e.bytes(b",\"model\":\"\",\"created\":0")
            },
            |e| e.bytes(b",\"synthetic\":[\"model\",\"created\"]"),
        )?;
        let scope = Scope {
            media_id: Some(local_id(b'm', 0)?),
            ..Scope::default()
        };
        let publisher = Publisher::start(&mut emitter, owner, scope, |e| {
            e.bytes(b",\"kind\":\"video\",\"encoding\":\"base64\"")?;
            if let Some(mime) = mime {
                e.bytes(b",\"mime\":")?;
                e.string(mime)?;
            }
            Ok(())
        })?;
        Ok(Self {
            owner: owner.id(),
            emitter,
            publisher,
            expected,
            received: 0,
            buffer: [0; DATA_BYTES],
            buffered: 0,
            deadline: None,
            closed: false,
        })
    }

    /// When buffered bytes must be published; call [`Self::advance`] with no bytes then.
    pub fn flush_deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> Result<()> {
        boundary::enter(self.owner, owner, self.closed)?;
        let result = self.append(owner, bytes);
        result.map_err(|error| self.fail(owner, error))
    }

    fn append(&mut self, owner: &mut Writer, mut bytes: &[u8]) -> Result<()> {
        self.received = self.received.checked_add(bytes.len() as u64).ok_or(Error::Limit)?;
        if self.expected.is_some_and(|expected| self.received > expected) {
            return Err(Error::Limit);
        }
        while !bytes.is_empty() {
            let capacity = self.publisher.capacity(&self.emitter)?.min(DATA_BYTES);
            let take = bytes.len().min(capacity - self.buffered);
            self.buffer[self.buffered..self.buffered + take].copy_from_slice(&bytes[..take]);
            self.buffered += take;
            bytes = &bytes[take..];
            if self.buffered == capacity {
                self.flush(owner)?;
            } else if self.deadline.is_none() {
                self.deadline = Some(owner.now() + FLUSH_DELAY);
            }
        }
        if self.deadline.is_some_and(|deadline| owner.now() >= deadline) {
            self.flush(owner)?;
        }
        Ok(())
    }

    fn flush(&mut self, owner: &mut Writer) -> Result<()> {
        self.deadline = None;
        if self.buffered != 0 {
            let len = std::mem::take(&mut self.buffered);
            self.publisher.append(&self.buffer[..len], &mut self.emitter, owner)?;
        }
        Ok(())
    }

    /// Completes the download and returns its byte count. A shorter body than declared is
    /// [`Error::Truncated`].
    pub fn finish(mut self, owner: &mut Writer) -> Result<u64> {
        boundary::enter(self.owner, owner, self.closed)?;
        let result = (|| {
            if self.expected.is_some_and(|expected| self.received != expected) {
                return Err(Error::Truncated);
            }
            self.flush(owner)?;
            self.publisher.end(&mut self.emitter, owner)?;
            self.emitter
                .emit(owner, Kind::Done, Scope::default(), |_| Ok(()), |_| Ok(()))?;
            Ok(self.received)
        })();
        match result {
            Ok(received) => {
                self.closed = true;
                Ok(received)
            }
            Err(error) => Err(self.fail(owner, error)),
        }
    }

    /// The transport failed before the body ended.
    pub fn abort(mut self, owner: &mut Writer) -> Error {
        if let Err(error) = boundary::enter(self.owner, owner, self.closed) {
            return error;
        }
        self.fail(owner, Error::Truncated)
    }

    fn fail(&mut self, owner: &mut Writer, error: Error) -> Error {
        self.closed = true;
        self.deadline = None;
        boundary::reported(Err::<(), _>(error), &mut self.emitter, owner, false)
            .err()
            .unwrap_or(Error::Closed)
    }
}

/// `type/subtype` followed by optional parameters, printable ASCII only. The value is written
/// into a JSON string and echoed to clients, so controls and non-ASCII are refused.
fn usable_mime(mime: &str) -> bool {
    let essence = mime.split(';').next().unwrap_or_default();
    let mut parts = essence.split('/');
    let (Some(kind), Some(subtype), None) = (parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    mime.len() <= MAX_MIME
        && !kind.is_empty()
        && !subtype.is_empty()
        && mime.bytes().all(|b| (0x20..0x7f).contains(&b))
        && !essence.contains(' ')
}
