//! The runtime-owned response producer (`docs/EXECUTION-CONTRACT.md`).
//!
//! After [`Execute`] returns an [`Upstream`], the producer owns the response stream's writer,
//! the upstream body and the response ingress, and drives `upstream bytes -> ResponseIngress
//! -> Writer` until a terminal record. It runs on its own task, independently of the client:
//! a slow or disconnected client never slows or stops upstream reads (§6.6). It schedules
//! `flush` at the ingress `flush_deadline`.
//!
//! Terminal records: a complete upstream publishes `done` and `Writer::finish(Done)`; an
//! upstream or decode failure publishes `error`; `Writer::cancelled` resolving (cancel,
//! shutdown, expiry, eviction) stops reading. A panic drops the writer, which publishes
//! `stream.producer_lost`, and so does a gateway defect. Nothing else ends a producer: cancel
//! goes through the allocation handle in [`Started::stream`], never through task abort.
//!
//! The functions here are generic over one executor; the selector in `lib.rs` calls them
//! with the provider's implementation.

use std::pin::pin;
use std::time::Duration;

use ck_core::exec::{ExecError, FailureScope};
use ck_stream::{Terminal, Writer};
use ck_translate::codec::event::{Emitter, Identity, Kind, Scope};
use ck_translate::codec::{
    self, Capability, Operation, RequestFence, RequestIngress, RequestView, ResponseIngress, ValidatedRequest,
};
use futures_util::StreamExt;
use http::HeaderMap;
use tokio::task::JoinHandle;

use crate::types::{Attempt, Body, Counted, Execute, Request, Streams, Upstream};

/// The producer task. Its output is the attempt's routing outcome: `Err` carries the
/// failure scope of an upstream that failed after answering, for the scheduler and
/// cooldown. Whether that failure may fail over is the server's commitment decision.
/// A cancel or shutdown is `Ok`: the upstream did not fail.
pub type Producer = JoinHandle<Result<(), ExecError>>;

/// An attempt whose upstream answered with a success status and whose producer is running.
pub struct Started {
    pub status: u16,
    /// Upstream headers; the server decides which, if any, reach the client.
    pub headers: HeaderMap,
    /// Allocation handle on the response stream (`Writer::allocator`). Response egress decodes
    /// and encodes with it while the producer runs and after it ends; `finish(Cancelled)` or
    /// `finish(Shutdown)` stops the producer, and `finish(Gone)` discards a failed attempt.
    pub stream: Writer,
    pub producer: Producer,
}

/// The capability key `executor` reaches for `attempt`.
pub fn destination<E: Execute>(executor: &E, attempt: &Attempt<'_>) -> &'static str {
    executor.capability(attempt).destination()
}

/// Contract §4 step 1: closes the client fence against the first attempt's destination.
pub fn fence<'a, E: Execute, I: RequestIngress>(
    executor: &E,
    attempt: &Attempt<'_>,
    ingress: &'a mut I,
    request: &mut Writer,
) -> codec::Result<ValidatedRequest<'a>> {
    ingress.finish(request, &executor.capability(attempt))
}

/// Contract §4 step 2: re-fences the client view for this attempt's destination.
pub fn refence<'a, E: Execute>(
    executor: &E,
    attempt: &Attempt<'_>,
    view: RequestView<'a>,
) -> codec::Result<ValidatedRequest<'a>> {
    RequestFence::default().end(view, &executor.capability(attempt))
}

/// Runs the operation of `request` with `executor` and starts the producer on `response`,
/// which must come from `Store::create_dependent(upload)`. An executor error discards the
/// response stream (`Terminal::Gone`) and is the attempt's failure.
pub async fn start<E: Execute>(
    executor: &E,
    attempt: &Attempt<'_>,
    request: &Request<'_>,
    upload: &mut Writer,
    mut response: Writer,
) -> Result<Started, ExecError> {
    let streams = Streams {
        request: upload,
        response: &mut response,
    };
    let upstream = match request.validated.view().operation {
        Operation::Generate => executor.execute(attempt, request, streams).await,
        Operation::Compact => executor.compact(attempt, request, streams).await,
        Operation::CountTokens => match executor.count_tokens(attempt, request, streams).await {
            Ok(Counted::Upstream(upstream)) => Ok(upstream),
            Ok(Counted::Local { input_tokens }) => return Ok(local_count(attempt, response, input_tokens)),
            Err(error) => Err(error),
        },
    };
    match upstream {
        Ok(upstream) => Ok(spawn(upstream, response)),
        Err(error) => {
            response.finish(Terminal::Gone);
            Err(error)
        }
    }
}

/// A local count's producer publishes the count as the attempt's response.
fn local_count(attempt: &Attempt<'_>, writer: Writer, input_tokens: u64) -> Started {
    let (identity, model) = (attempt.identity, attempt.model.to_owned());
    Started {
        status: 200,
        headers: HeaderMap::new(),
        stream: writer.allocator(),
        producer: tokio::spawn(async move { count_locally(writer, identity, &model, input_tokens).await }),
    }
}

fn spawn<I: ResponseIngress + Send + 'static>(upstream: Upstream<I>, writer: Writer) -> Started {
    Started {
        status: upstream.status,
        headers: upstream.headers,
        stream: writer.allocator(),
        producer: tokio::spawn(produce(upstream.body, upstream.ingress, writer)),
    }
}

/// Feeds `body` into `ingress` until the stream ends. Expired flush deadlines go first, so a
/// busy upstream cannot postpone them; a stopped stream goes before both.
async fn produce<I: ResponseIngress>(mut body: Body, mut ingress: I, mut writer: Writer) -> Result<(), ExecError> {
    let mut stopped = pin!(writer.cancelled());
    loop {
        let flush = ingress
            .flush_deadline()
            .map(|deadline| deadline.saturating_duration_since(writer.now()));
        let step = tokio::select! {
            biased;
            terminal = &mut stopped => return stopped_outcome(terminal),
            () = until(flush) => ingress.flush(&mut writer),
            chunk = body.next() => match chunk {
                Some(Ok(bytes)) => ingress.advance(&mut writer, &bytes),
                Some(Err(error)) => return interrupted(ingress, writer, error).await,
                None => match ingress.finish(&mut writer) {
                    Ok(()) => {
                        writer.finish(Terminal::Done);
                        return Ok(());
                    }
                    Err(error) => Err(error),
                },
            },
        };
        if let Err(error) = step {
            return refused(writer, error).await;
        }
    }
}

/// Sleeps `delay` (store-clock time left to a flush deadline); pending without one.
async fn until(delay: Option<Duration>) {
    match delay {
        Some(delay) => tokio::time::sleep(delay).await,
        None => std::future::pending().await,
    }
}

/// The upstream body failed: the ingress closes on what it has, so a cut stream publishes
/// `error`; the attempt fails with the body's own error and scope.
async fn interrupted<I: ResponseIngress>(
    mut ingress: I,
    mut writer: Writer,
    error: ExecError,
) -> Result<(), ExecError> {
    match ingress.finish(&mut writer) {
        Ok(()) => completed(writer),
        Err(refusal) if decoding(&refusal) => {
            writer.finish(Terminal::UpstreamError);
            Err(error)
        }
        Err(refusal) => refused(writer, refusal).await,
    }
}

/// The ingress refused input, a flush or the end of input.
///
/// - Decode failures: the ingress published `error` (codec contract); the stream ends with
///   `UpstreamError`.
/// - `Closed`: the producer stops at the first refusal, so a closed ingress closed itself at
///   its completion boundary and published `done`; the upstream sent more after it.
/// - Storage failures follow contract §9; defects end as producer lost (§7).
async fn refused(mut writer: Writer, error: codec::Error) -> Result<(), ExecError> {
    match error {
        _ if decoding(&error) => {
            writer.finish(Terminal::UpstreamError);
            Err(invalid())
        }
        codec::Error::Closed => completed(writer),
        codec::Error::Storage(error) => storage(writer, error).await,
        _ => Err(defect(writer)),
    }
}

/// Upstream input the ingress rejected; it published `error` for it.
fn decoding(error: &codec::Error) -> bool {
    match error {
        codec::Error::Json
        | codec::Error::Truncated
        | codec::Error::Schema
        | codec::Error::Utf8
        | codec::Error::DuplicateKey
        | codec::Error::Limit
        | codec::Error::EventTooLarge
        | codec::Error::Unsupported => true,
        codec::Error::Closed | codec::Error::Storage(_) | codec::Error::Position | codec::Error::Destination => false,
    }
}

/// The ingress already published `done`: the response is complete, and upstream input or a
/// body failure after its completion boundary is not part of it.
fn completed(mut writer: Writer) -> Result<(), ExecError> {
    tracing::warn!("upstream input after the completion boundary ignored");
    writer.finish(Terminal::Done);
    Ok(())
}

/// Contract §9 storage rows for a failure while the producer runs. ck-stream already ended
/// the stream on exhaustion, I/O failure and invalidation.
async fn storage(writer: Writer, error: ck_stream::Error) -> Result<(), ExecError> {
    match error {
        ck_stream::Error::StorageExhausted | ck_stream::Error::Io(_) => Err(exhausted()),
        ck_stream::Error::Gone => Err(gone()),
        // The stream already has a terminal; its row applies.
        ck_stream::Error::Closed => stopped_outcome(writer.cancelled().await),
        ck_stream::Error::InvalidPosition | ck_stream::Error::InvalidRecord | ck_stream::Error::IncompleteRecord => {
            Err(defect(writer))
        }
    }
}

/// The routing outcome of a stream another party ended.
fn stopped_outcome(terminal: Terminal) -> Result<(), ExecError> {
    match terminal {
        Terminal::Done | Terminal::Cancelled | Terminal::Shutdown => Ok(()),
        Terminal::Gone => Err(gone()),
        Terminal::StorageExhausted => Err(exhausted()),
        Terminal::UpstreamError | Terminal::ProducerLost => Err(lost()),
    }
}

/// A gateway defect after the producer started ends the stream as producer lost (§7).
fn defect(mut writer: Writer) -> ExecError {
    writer.finish(Terminal::ProducerLost);
    lost()
}

fn invalid() -> ExecError {
    ExecError::local(502, crate::upstream::scope_for(502), "invalid upstream response")
}

fn lost() -> ExecError {
    ExecError::local(502, FailureScope::Transport, "stream.producer_lost")
}

fn exhausted() -> ExecError {
    ExecError::local(503, FailureScope::Request, "stream.storage_exhausted")
}

fn gone() -> ExecError {
    ExecError::local(410, FailureScope::Request, "stream.gone")
}

/// Publishes a local token count as the attempt's response: `response.start`, the canonical
/// `count_tokens` and `done`. The id is the stream id and `created` is 0, both synthetic.
async fn count_locally(
    mut writer: Writer,
    identity: Identity,
    model: &str,
    input_tokens: u64,
) -> Result<(), ExecError> {
    let mut emitter = Emitter::new(identity);
    let published = (|| {
        emitter.emit(
            &mut writer,
            Kind::ResponseStart,
            Scope::default(),
            |e| {
                e.bytes(b",\"id\":")?;
                e.string(identity.stream_id.as_str())?;
                e.bytes(b",\"model\":")?;
                e.string(model)?;
                e.bytes(b",\"created\":0")
            },
            |e| e.bytes(b",\"synthetic\":[\"id\",\"created\"]"),
        )?;
        emitter.emit(
            &mut writer,
            Kind::CountTokens,
            Scope::default(),
            |e| {
                e.bytes(b",\"input_tokens\":")?;
                e.unsigned(input_tokens)
            },
            |_| Ok(()),
        )?;
        emitter.emit(&mut writer, Kind::Done, Scope::default(), |_| Ok(()), |_| Ok(()))
    })();
    match published {
        Ok(_) => {
            writer.finish(Terminal::Done);
            Ok(())
        }
        Err(codec::Error::Storage(error)) => storage(writer, error).await,
        Err(_) => Err(defect(writer)),
    }
}
