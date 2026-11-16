//! A video job response as a canonical stream: `response.start`, the job's usage snapshot when
//! the body reports one, one `video.job` operation record, then `done`.
//!
//! The body is validated incrementally by [`Decoder`] in mapped storage and never collected in
//! a heap buffer. Nothing but `response.start` is published before the whole body validates, so
//! a cut or malformed body ends in one `error`, with no job and no usage.
//!
//! Usage is read once per body and never invented: absent, null or empty `usage` publishes no
//! snapshot. It carries the `job:<id>` scope (E-Q13), so accounting keeps it apart from the
//! attempt's own usage, and it is `final` only once the job is no longer pending: a queued or
//! running job's report may still change. The job table ([`Ledger`]) decides whether a report is
//! the job's charge; a report it refuses publishes no snapshot.

use ck_stream::Writer;

use super::{Decoder, Dialect, Phase, Usage};
use crate::{
    boundary,
    codec::{
        self, Error, ResponseIngress, Result,
        document::Document,
        encode::Encoder,
        event::{Direction, Emitter, Identity, Kind, Scope},
    },
};

/// The job table a video response settles against (E-Q13): a job's usage is charged at the
/// first report and never again, whichever request carries it.
pub trait Ledger: Send {
    /// A create, edit or extension was accepted as `job`.
    fn accepted(&mut self, job: &str);
    /// Whether this report of `job`'s usage is its charge: true only for the job's first.
    fn charge(&mut self, job: &str) -> bool;
}

pub struct Ingress {
    owner: u64,
    emitter: Emitter,
    decoder: Option<Decoder>,
    dialect: Dialect,
    phase: Phase,
    ledger: Box<dyn Ledger>,
    closed: bool,
}

impl Ingress {
    /// Called only for a successful HTTP response. `phase` is [`Phase::Accepted`] for a create,
    /// edit or extend answer and [`Phase::Job`] for a poll; `job_id` is the job a poll asked for.
    /// `limit` bounds the body; `ledger` settles the job's usage.
    pub fn new(
        owner: &mut Writer,
        identity: Identity,
        dialect: Dialect,
        phase: Phase,
        job_id: Option<&str>,
        limit: u64,
        ledger: Box<dyn Ledger>,
    ) -> Result<Self> {
        if identity.direction != Direction::Response || matches!(phase, Phase::Request(_)) {
            return Err(Error::Schema);
        }
        let decoder = Decoder::new(owner, dialect, phase, job_id, limit)?;
        let mut emitter = Emitter::new(identity);
        emitter.emit(
            owner,
            Kind::ResponseStart,
            Scope::default(),
            |e| e.bytes(b",\"id\":\"video\",\"model\":\"\",\"created\":0"),
            |e| e.bytes(b",\"synthetic\":[\"id\",\"model\",\"created\"]"),
        )?;
        Ok(Self {
            owner: owner.id(),
            emitter,
            decoder: Some(decoder),
            dialect,
            phase,
            ledger,
            closed: false,
        })
    }

    fn publish(&mut self, owner: &mut Writer, operation: &Document, usage: Option<Usage>) -> Result<()> {
        let root = operation.root();
        let (kind, value) = (operation.required(root, "kind")?, operation.required(root, "value")?);
        let job = operation.required(value, "job_id")?;
        let id = text(operation, job)?;
        if self.phase == Phase::Accepted {
            self.ledger.accepted(&id);
        }
        if let Some(usage) = usage.filter(|_| self.ledger.charge(&id)) {
            let pending = ["queued", "running"].iter().try_fold(false, |found, status| {
                Ok::<_, Error>(found || boundary::is(operation, value, "status", status)?)
            })?;
            let protocol = self.dialect.name();
            self.emitter.emit(
                owner,
                Kind::Usage,
                Scope::default(),
                |e| {
                    e.bytes(b",\"scope\":\"job:")?;
                    operation.write_contents(job, e)?;
                    e.bytes(b"\",\"usage\":{\"native\":{\"protocol\":")?;
                    e.string(protocol)?;
                    e.bytes(b",\"value\":{")?;
                    counts(&usage, true, e)?;
                    e.bytes(b"}},\"final\":")?;
                    e.bytes(if pending { b"false" } else { b"true" })?;
                    counts(&usage, false, e)?;
                    e.bytes(b"}")
                },
                |_| Ok(()),
            )?;
        }
        let x = operation.required(root, "x_ck")?;
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
                for name in ["original_protocol", "id_implied", "status_implied", "original"] {
                    boundary::member(operation, x, name, e)?;
                }
                Ok(())
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

/// The reported counts, comma-separated after a first member unless `first`; `cost_in_usd_ticks`
/// exists only in the native value.
fn counts(usage: &Usage, native: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    let mut first = native;
    let mut count = |name: &str, value: Option<u64>| -> Result<()> {
        let Some(value) = value else {
            return Ok(());
        };
        if !std::mem::replace(&mut first, false) {
            e.bytes(b",")?;
        }
        e.string(name)?;
        e.bytes(b":")?;
        e.unsigned(value)
    };
    if native {
        count("cost_in_usd_ticks", usage.cost_in_usd_ticks)?;
    }
    count("input_tokens", usage.input_tokens)?;
    count("output_tokens", usage.output_tokens)?;
    count("total_tokens", usage.total_tokens)
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
            .and_then(|decoded| self.publish(owner, &decoded.operation, decoded.usage));
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

fn text(doc: &Document, node: usize) -> Result<String> {
    let mut text = String::new();
    let mut chars = doc.chars(node)?;
    while let Some(c) = chars.next_scalar()? {
        text.push(c);
    }
    Ok(text)
}
