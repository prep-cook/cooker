//! A video job response back to the client: the stream's one `video.job` operation record as the
//! client's job object ([`super::encode`]), written once the stream is done.

use ck_stream::{Lease, ScratchLease, Writer};

use super::Dialect;
use crate::codec::{
    Error, ResponseEgress, Result,
    document::Document,
    event::{Identity, Kind},
    internal::Binding,
    provenance,
};

pub struct JobEgress {
    binding: Binding,
    dialect: Dialect,
    job: Option<Document>,
    done: bool,
    closed: bool,
}

impl JobEgress {
    /// The job as `dialect` writes it: xAI's members as xAI sent them, or OpenAI's video object.
    pub fn new(owner: &Writer, identity: Option<Identity>, dialect: Dialect) -> Self {
        Self {
            binding: Binding::new(owner, identity),
            dialect,
            job: None,
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
            // The job's usage is accounting's; the client's job object does not repeat it.
            Kind::ResponseStart | Kind::Heartbeat | Kind::Usage => {}
            Kind::Operation if self.job.is_none() => {
                provenance::restore_bindings(&mut doc)?;
                self.job = Some(doc);
            }
            Kind::Done => {
                let job = self.job.as_ref().ok_or(Error::Schema)?;
                output(super::encode(owner, job, self.dialect)?.ok_or(Error::Schema)?)?;
                self.done = true;
            }
            Kind::Error | Kind::Cancelled => return Err(Error::Truncated),
            _ => return Err(Error::Schema),
        }
        Ok(())
    }
}

impl ResponseEgress for JobEgress {
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
