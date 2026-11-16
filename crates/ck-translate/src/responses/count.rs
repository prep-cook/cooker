//! The canonical `count_tokens` result back to the Responses `response.input_tokens` object,
//! written as one body once the stream is `done`.

use ck_stream::{Lease, ScratchLease, Writer};

use crate::codec::{
    Error, ResponseEgress, Result,
    document::Document,
    encode,
    event::{Identity, Kind},
    internal::Binding,
};

pub struct CountEgress {
    binding: Binding,
    count: Option<Document>,
    done: bool,
    closed: bool,
}

impl CountEgress {
    pub fn new(owner: &Writer, identity: Option<Identity>) -> Self {
        Self {
            binding: Binding::new(owner, identity),
            count: None,
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
        let (kind, doc) = self.binding.decode(owner, input)?;
        match kind {
            Kind::ResponseStart | Kind::Heartbeat | Kind::Usage => {}
            Kind::CountTokens if self.count.is_none() => self.count = Some(doc),
            Kind::Done => {
                let count = self.count.as_ref().ok_or(Error::Schema)?;
                let tokens = count.required(count.root(), "input_tokens")?;
                count.unsigned(tokens)?;
                output(encode::scratch(owner, |e| {
                    e.bytes(b"{\"object\":\"response.input_tokens\",\"input_tokens\":")?;
                    count.write(tokens, e)?;
                    e.bytes(b"}")
                })?)?;
                self.done = true;
            }
            Kind::Error | Kind::Cancelled => return Err(Error::Truncated),
            _ => return Err(Error::Schema),
        }
        Ok(())
    }
}

impl ResponseEgress for CountEgress {
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
