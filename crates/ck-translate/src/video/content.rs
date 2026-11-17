//! Finished video bytes as one media lifetime: `response.start`, `media.start` with the MIME
//! the upstream sent (none when it sent none; a MIME is never guessed), the job's usage snapshot
//! when the poll that found the file reports one ([`Download::charge`]), data records of at most
//! one record's capacity, `media.end` with the byte count, then `done`. Bytes are published
//! exactly as received; nothing is re-encoded.
//!
//! A body that ends short of its declared length, runs past it, or is cut by the transport is
//! an error event and an error result, never a shorter success.

use std::time::{Duration, Instant};

use ck_stream::Writer;

use super::{Decoded, Dialect, Ledger};
use crate::{
    boundary,
    codec::{
        self, Error, ResponseIngress, Result,
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
    job: String,
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
            job: job_id.to_owned(),
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

    /// Publishes the usage `decoded`, the poll that found the file, reported in `dialect`, when
    /// `ledger` finds it the job's charge (E-Q13), and answers whether it was.
    pub fn charge(
        &mut self,
        owner: &mut Writer,
        decoded: &Decoded,
        dialect: Dialect,
        ledger: &mut dyn Ledger,
    ) -> Result<bool> {
        boundary::enter(self.owner, owner, self.closed)?;
        let Some(usage) = decoded.usage else {
            return Ok(false);
        };
        let pending = super::response::pending(&decoded.operation)?;
        let Self { job, emitter, .. } = self;
        let job = job.as_str();
        ledger.charge(job, &mut || {
            super::response::usage(emitter, owner, job, dialect, &usage, pending)
        })
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

    fn failed(&mut self, owner: &mut Writer, error: Error) -> Error {
        self.closed = true;
        self.deadline = None;
        boundary::reported(Err::<(), _>(error), &mut self.emitter, owner, false)
            .err()
            .unwrap_or(Error::Closed)
    }
}

impl ResponseIngress for Download {
    fn flush_deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Appends `bytes`; no bytes publish what is buffered.
    fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> Result<()> {
        boundary::enter(self.owner, owner, self.closed)?;
        let result = if bytes.is_empty() {
            self.flush(owner)
        } else {
            self.append(owner, bytes)
        };
        result.map_err(|error| self.failed(owner, error))
    }

    /// Completes the download. A shorter body than declared is [`Error::Truncated`].
    fn finish(&mut self, owner: &mut Writer) -> Result<()> {
        boundary::enter(self.owner, owner, self.closed)?;
        let result = (|| {
            if self.expected.is_some_and(|expected| self.received != expected) {
                return Err(Error::Truncated);
            }
            self.flush(owner)?;
            self.publisher.end(&mut self.emitter, owner)?;
            self.emitter
                .emit(owner, Kind::Done, Scope::default(), |_| Ok(()), |_| Ok(()))
                .map(drop)
        })();
        match result {
            Ok(()) => {
                self.closed = true;
                Ok(())
            }
            Err(error) => Err(self.failed(owner, error)),
        }
    }

    /// The transport failed before the body ended: the bytes received so far, then the failure.
    fn fail(&mut self, owner: &mut Writer, status: u16, scope: &'static str) -> Result<()> {
        boundary::enter(self.owner, owner, self.closed)?;
        self.closed = true;
        self.deadline = None;
        self.flush(owner)?;
        codec::upstream_failure(&mut self.emitter, owner, status, scope)
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
