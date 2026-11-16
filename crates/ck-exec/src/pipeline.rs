//! The runtime-owned response producer (`docs/EXECUTION-CONTRACT.md`).
//!
//! After [`Execute`] returns an [`Upstream`], the producer owns the response stream's writer,
//! the upstream body and the response ingress, and drives `upstream bytes -> ResponseIngress
//! -> Writer` until a terminal record. It runs on its own task, independently of the client:
//! a slow or disconnected client never slows or stops upstream reads (§6.6). It schedules
//! `flush` at the ingress `flush_deadline`.
//!
//! The producer reads back the records the ingress publishes. A `done` record ends the stream
//! with `Writer::finish(Done)`. An `error` record, from a decode failure or the upstream's own
//! error event, ends it with `UpstreamError` and fails the attempt with the record's status
//! and scope. A body error before `done` has the ingress publish the upstream failure
//! (`ResponseIngress::fail`) and ends the stream with `UpstreamError` and the body error's
//! scope. `Writer::cancelled` resolving (cancel, shutdown, expiry, eviction) stops reading. A
//! panic drops the writer, which publishes `stream.producer_lost`, and a gateway defect ends
//! the stream the same way. Nothing else ends a producer: cancel goes through the allocation
//! handle in [`Started::stream`], never through task abort.
//!
//! The functions here are generic over one executor; the selector in `lib.rs` calls them
//! with the provider's implementation.

use std::pin::pin;
use std::task::Poll;
use std::time::Duration;

use ck_core::exec::{ExecError, FailureScope};
use ck_stream::{Lease, Read, Reader, Terminal, Writer};
use ck_translate::codec::event::{Emitter, Identity, Kind, Scope};
use ck_translate::codec::{self, Capability, Operation, RequestFence, RequestView, ResponseIngress, ValidatedRequest};
use futures_util::StreamExt;
use http::HeaderMap;
use tokio::task::JoinHandle;

use crate::session::{Turn, Turns};
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

/// Contract §4 step 2: re-fences the client view for this attempt's destination.
pub fn refence<'a, E: Execute>(
    executor: &E,
    attempt: &Attempt<'_>,
    view: RequestView<'a>,
) -> codec::Result<ValidatedRequest<'a>> {
    RequestFence::default().end(view, &executor.capability(attempt))
}

/// Runs the operation of `request` with `executor` and starts the producer on `response`,
/// which must come from `Store::create_dependent(upload)`. An executor error is the attempt's
/// failure and ends the response stream with `Terminal::UpstreamError`, which keeps its
/// records, so the attempt's usage reader still reaches a terminal and keeps a snapshot
/// published before the failure. The caller takes an allocation handle (`Writer::allocator`)
/// before this call and discards the stream with `finish(Gone)` once its usage reader is done.
pub async fn start<E: Execute>(
    executor: &E,
    attempt: &Attempt<'_>,
    request: &mut Request<'_>,
    upload: &mut Writer,
    mut response: Writer,
) -> Result<Started, ExecError> {
    let published = attach(attempt, &mut response)?;
    let streams = Streams {
        request: upload,
        response: &mut response,
    };
    let upstream = match request.validated().view().operation {
        Operation::Generate | Operation::Media(_) | Operation::AlphaSearch => {
            executor.execute(attempt, request, streams).await
        }
        Operation::Compact => executor.compact(attempt, request, streams).await,
        Operation::CountTokens => match executor.count_tokens(attempt, request, streams).await {
            Ok(Counted::Upstream(upstream)) => Ok(upstream),
            Ok(Counted::Local { .. }) if request.upstream_count_only() => Err(local_count_refused()),
            Ok(Counted::Local { input_tokens }) => return Ok(local_count(attempt, response, input_tokens)),
            Err(error) => Err(error),
        },
    };
    match upstream {
        Ok(upstream) => Ok(spawn(upstream, response, published)),
        Err(error) => {
            response.finish(Terminal::UpstreamError);
            Err(error)
        }
    }
}

/// [`start`] for one Responses WebSocket turn on `executor`'s upstream socket
/// ([`Turns::turn`]). The server keeps the returned cancel and calls it when the client
/// disconnects with the turn in flight.
pub async fn start_turn<E: Turns>(
    executor: &E,
    attempt: &Attempt<'_>,
    request: &mut Request<'_>,
    upload: &mut Writer,
    mut response: Writer,
) -> Result<(Started, E::Cancel), ExecError> {
    let published = attach(attempt, &mut response)?;
    let streams = Streams {
        request: upload,
        response: &mut response,
    };
    match executor.turn(attempt, request, streams).await {
        Ok(Turn { upstream, cancel }) => Ok((spawn(upstream, response, published), cancel)),
        Err(error) => {
            response.finish(Terminal::UpstreamError);
            Err(error)
        }
    }
}

/// Attaches the producer's reader of what the ingress publishes, before the upstream
/// request, so a refusal costs no upstream call.
fn attach(attempt: &Attempt<'_>, response: &mut Writer) -> Result<Published, ExecError> {
    match attempt.store.reader(response.id(), 0) {
        Ok(reader) => Ok(Published {
            reader,
            last: Last::Other,
        }),
        Err(error) => {
            response.finish(Terminal::UpstreamError);
            Err(stored(&error))
        }
    }
}

/// Starts the producer of an upstream that answered.
fn spawn<I: ResponseIngress + Send + 'static>(
    upstream: Upstream<I>,
    response: Writer,
    published: Published,
) -> Started {
    Started {
        status: upstream.status,
        headers: upstream.headers,
        stream: response.allocator(),
        producer: tokio::spawn(produce(upstream.body, upstream.ingress, response, published)),
    }
}

/// What refuses a local count for a request that takes only the destination's own.
pub const LOCAL_COUNT: &str = "this account has no token-counting endpoint";

fn local_count_refused() -> ExecError {
    ExecError::local(400, FailureScope::Request, LOCAL_COUNT)
}

/// Whether `error` is the refusal of a local count ([`Request::upstream_count`]).
pub fn refused_local_count(error: &ExecError) -> bool {
    error.status == 400 && error.scope == FailureScope::Request && error.headers.is_empty() && error.body == LOCAL_COUNT
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

/// Feeds `body` into `ingress` until the ingress publishes a terminal record or the stream
/// stops. A due flush deadline is served before any wait, so a ready body cannot postpone
/// it; a stopped stream goes before the body.
async fn produce<I: ResponseIngress>(
    mut body: Body,
    mut ingress: I,
    mut writer: Writer,
    mut published: Published,
) -> Result<(), ExecError> {
    let mut stopped = pin!(writer.cancelled());
    loop {
        let flush = match ingress.flush_deadline() {
            Some(deadline) if deadline <= writer.now() => {
                let flushed = ingress.flush(&mut writer);
                if let Some(outcome) = settle(flushed, false, &mut writer, &mut published).await {
                    return outcome;
                }
                continue;
            }
            deadline => deadline.map(|deadline| deadline.saturating_duration_since(writer.now())),
        };
        let (step, ended) = tokio::select! {
            biased;
            terminal = &mut stopped => return stopped_outcome(terminal),
            () = until(flush) => (ingress.flush(&mut writer), false),
            chunk = body.next() => match chunk {
                Some(Ok(bytes)) => (ingress.advance(&mut writer, &bytes), false),
                // Settling after every call returns at `done`, so the response is incomplete.
                Some(Err(error)) => return cut(ingress, writer, error).await,
                None => (ingress.finish(&mut writer), true),
            },
        };
        if let Some(outcome) = settle(step, ended, &mut writer, &mut published).await {
            return outcome;
        }
    }
}

/// The upstream body failed before `done`: the ingress publishes the upstream failure
/// without completing the response, and the attempt fails with the body error's own scope.
async fn cut<I: ResponseIngress>(mut ingress: I, mut writer: Writer, error: ExecError) -> Result<(), ExecError> {
    match ingress.fail(&mut writer, error.status, "upstream") {
        Ok(()) => {
            writer.finish(Terminal::UpstreamError);
            Err(error)
        }
        Err(codec::Error::Storage(failure)) => storage(&mut writer, failure).await,
        Err(_) => Err(defect(&mut writer)),
    }
}

/// Sleeps `delay` (store-clock time left to a flush deadline); pending without one.
async fn until(delay: Option<Duration>) {
    match delay {
        Some(delay) => tokio::time::sleep(delay).await,
        None => std::future::pending().await,
    }
}

/// Decides from one ingress call and the records it published whether the producer ends.
///
/// - `done` published: the response is complete; input refused after it is not part of it.
/// - `error` published: the upstream failed, with the record's status and scope, whether the
///   ingress refused the input or accepted a native error event.
/// - Storage failures follow contract §9.
/// - A refusal or an ended input without a terminal record is a gateway defect (§7).
async fn settle(
    step: codec::Result<()>,
    ended: bool,
    writer: &mut Writer,
    published: &mut Published,
) -> Option<Result<(), ExecError>> {
    if let Err(codec::Error::Storage(error)) = step {
        return Some(storage(writer, error).await);
    }
    match published.read() {
        Last::Done => {
            if step.is_err() {
                tracing::warn!("upstream input after the completion boundary ignored");
            }
            writer.finish(Terminal::Done);
            Some(Ok(()))
        }
        Last::Error(error) => {
            writer.finish(Terminal::UpstreamError);
            Some(Err(error))
        }
        Last::Other if step.is_err() || ended => Some(Err(defect(writer))),
        Last::Other => None,
    }
}

/// The ingress's published records, read back from the response stream as they commit, so
/// the producer learns which terminal record the ingress chose.
struct Published {
    reader: Reader,
    last: Last,
}

enum Last {
    Done,
    Error(ExecError),
    Other,
}

impl Published {
    /// The last record published so far.
    fn read(&mut self) -> Last {
        while let Poll::Ready(Some(Read::Record(lease))) = self.reader.try_next() {
            let kind = lease.record().kind;
            self.last = if kind == Kind::Done as u32 {
                Last::Done
            } else if kind == Kind::Error as u32 {
                Last::Error(failure(&lease))
            } else {
                Last::Other
            };
        }
        std::mem::replace(&mut self.last, Last::Other)
    }
}

/// The routing outcome an `error` record states: its status, and its scope mapped to the
/// scheduler's. An unreadable record is a gateway defect.
fn failure(lease: &Lease) -> ExecError {
    #[derive(serde::Deserialize)]
    struct Failure {
        status: u16,
        scope: String,
        message: String,
    }
    let mut record = Vec::with_capacity(lease.record().len() as usize);
    let visited = lease.visit(|chunk| record.extend_from_slice(chunk));
    let failure = record
        .windows(6)
        .position(|window| window == b"data: ")
        .and_then(|at| serde_json::from_slice::<Failure>(record[at + 6..].trim_ascii_end()).ok());
    match (visited, failure) {
        (Ok(()), Some(failure)) => {
            let scope = match failure.scope.as_str() {
                "upstream" => crate::upstream::scope_for(failure.status),
                _ => FailureScope::Request,
            };
            ExecError::local(failure.status, scope, failure.message)
        }
        _ => defective(),
    }
}

/// Contract §9 storage rows for a failure while the producer runs. ck-stream already ended
/// the stream on exhaustion, I/O failure and invalidation.
async fn storage(writer: &mut Writer, error: ck_stream::Error) -> Result<(), ExecError> {
    match error {
        // The stream already has a terminal; its row applies.
        ck_stream::Error::Closed => stopped_outcome(writer.cancelled().await),
        ck_stream::Error::InvalidPosition | ck_stream::Error::InvalidRecord | ck_stream::Error::IncompleteRecord => {
            Err(defect(writer))
        }
        error => Err(stored(&error)),
    }
}

/// The attempt failure of a storage error.
pub(crate) fn stored(error: &ck_stream::Error) -> ExecError {
    match error {
        ck_stream::Error::StorageExhausted | ck_stream::Error::Io(_) => {
            ExecError::local(503, FailureScope::Request, "stream.storage_exhausted")
        }
        ck_stream::Error::Gone => gone(),
        _ => defective(),
    }
}

/// The routing outcome of a stream another party ended.
fn stopped_outcome(terminal: Terminal) -> Result<(), ExecError> {
    match terminal {
        Terminal::Done | Terminal::Cancelled | Terminal::ClientDisconnected | Terminal::Shutdown => Ok(()),
        Terminal::Gone => Err(gone()),
        Terminal::StorageExhausted => Err(stored(&ck_stream::Error::StorageExhausted)),
        Terminal::UpstreamError | Terminal::ProducerLost => Err(defective()),
    }
}

/// A gateway defect after the producer started ends the stream as producer lost (§7).
fn defect(writer: &mut Writer) -> ExecError {
    writer.finish(Terminal::ProducerLost);
    defective()
}

/// A gateway defect: 500, never failed over, unlike a producer panic's 502.
fn defective() -> ExecError {
    ExecError::local(500, FailureScope::Request, "stream.producer_lost")
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
        Err(codec::Error::Storage(error)) => storage(&mut writer, error).await,
        Err(_) => Err(defect(&mut writer)),
    }
}
