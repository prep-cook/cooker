//! The Alpha Search response as a canonical stream: `response.start`, one `operation`
//! record carrying the mapped result, then `done`; and that stream back to the client's
//! search JSON.
//!
//! The body is validated incrementally by [`Decoder`] in mapped storage and is never
//! collected in a heap buffer. Nothing but `response.start` is published before the whole
//! body validates, so a cut or malformed body ends in one `error` and no partial result.

use ck_stream::{Lease, ScratchLease, Writer};

use super::{Decoder, Phase};
use crate::{
    boundary,
    codec::{
        self, Error, ResponseEgress, ResponseIngress, Result,
        document::Document,
        event::{Direction, Emitter, Identity, Kind, Scope},
        internal::Binding,
        provenance,
    },
};

pub struct Ingress {
    owner: u64,
    emitter: Emitter,
    decoder: Option<Decoder>,
    closed: bool,
}

impl Ingress {
    /// Called only for a successful HTTP response. `limit` bounds the body.
    pub fn new(owner: &mut Writer, identity: Identity, limit: u64) -> Result<Self> {
        if identity.direction != Direction::Response {
            return Err(Error::Schema);
        }
        let mut emitter = Emitter::new(identity);
        emitter.emit(
            owner,
            Kind::ResponseStart,
            Scope::default(),
            |e| e.bytes(b",\"id\":\"alpha_search\",\"model\":\"\",\"created\":0"),
            |e| e.bytes(b",\"synthetic\":[\"id\",\"model\",\"created\"]"),
        )?;
        Ok(Self {
            owner: owner.id(),
            emitter,
            decoder: Some(Decoder::new(owner, Phase::Result, limit)?),
            closed: false,
        })
    }

    fn publish(&mut self, owner: &mut Writer, operation: &Document) -> Result<()> {
        let root = operation.root();
        let x = operation.required(root, "x_ck")?;
        let (kind, value) = (operation.required(root, "kind")?, operation.required(root, "value")?);
        let original = operation.required(x, "original")?;
        self.emitter.emit(
            owner,
            Kind::Operation,
            Scope::default(),
            |e| {
                e.bytes(b",\"kind\":")?;
                operation.write(kind, e)?;
                e.bytes(b",\"value\":")?;
                operation.write(value, e)
            },
            |e| {
                e.bytes(b",\"original_protocol\":\"codex\",\"original\":")?;
                operation.write(original, e)
            },
        )?;
        self.emitter
            .emit(owner, Kind::Done, Scope::default(), |_| Ok(()), |_| Ok(()))?;
        Ok(())
    }

    /// A failure other than storage exhaustion ends the stream with the canonical upstream
    /// failure; the error itself is returned to the owner either way.
    fn report<T>(&mut self, owner: &mut Writer, result: Result<T>) -> Result<T> {
        if let Err(error) = &result {
            self.closed = true;
            if !matches!(error, Error::Storage(_)) {
                codec::upstream_failure(&mut self.emitter, owner, 502, "upstream")?;
            }
        }
        result
    }
}

impl ResponseIngress for Ingress {
    fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> Result<()> {
        boundary::enter(self.owner, owner, self.closed)?;
        let result = self.decoder.as_mut().ok_or(Error::Closed)?.advance(owner, bytes);
        self.report(owner, result)
    }

    fn finish(&mut self, owner: &mut Writer) -> Result<()> {
        boundary::enter(self.owner, owner, self.closed)?;
        let result = self
            .decoder
            .take()
            .ok_or(Error::Closed)
            .and_then(|decoder| decoder.finish(owner))
            .and_then(|operation| self.publish(owner, &operation));
        let result = self.report(owner, result);
        self.closed = true;
        result
    }

    fn fail(&mut self, owner: &mut Writer, status: u16, scope: &'static str) -> Result<()> {
        boundary::enter(self.owner, owner, self.closed)?;
        self.closed = true;
        codec::upstream_failure(&mut self.emitter, owner, status, scope)
    }
}

/// The client's search JSON: the `operation` record's result with the native members its
/// `x_ck.original` kept, written as one body once the stream is `done`.
pub struct ResultEgress {
    binding: Binding,
    result: Option<Document>,
    done: bool,
    closed: bool,
}

impl ResultEgress {
    pub fn new(owner: &Writer, identity: Option<Identity>) -> Self {
        Self {
            binding: Binding::new(owner, identity),
            result: None,
            done: false,
            closed: false,
        }
    }

    fn record(
        &mut self,
        owner: &mut Writer,
        input: Lease,
        mut output: impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let (kind, mut doc) = self.binding.decode(owner, input)?;
        match kind {
            Kind::ResponseStart | Kind::Heartbeat => {}
            Kind::Operation if self.result.is_none() => {
                provenance::restore_bindings(&mut doc)?;
                self.result = Some(doc);
            }
            Kind::Done => {
                let result = self.result.as_ref().ok_or(Error::Schema)?;
                output(super::encode(owner, result)?)?;
                self.done = true;
            }
            Kind::Error | Kind::Cancelled => return Err(Error::Truncated),
            _ => return Err(Error::Schema),
        }
        Ok(())
    }
}

impl ResponseEgress for ResultEgress {
    fn advance(
        &mut self,
        owner: &mut Writer,
        input: Lease,
        output: impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        if self.closed || self.done {
            return Err(Error::Closed);
        }
        let result = self.record(owner, input, output);
        if result.is_err() {
            self.closed = true;
        }
        result
    }

    fn finish(&mut self) -> Result<()> {
        if self.done {
            Ok(())
        } else {
            self.closed = true;
            Err(Error::Truncated)
        }
    }
}
