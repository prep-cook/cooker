//! One attempt of the execution contract's arm (D2-PLAN §3.2 and §5.2): the per-attempt
//! decision, the attempt's response stream, and the finalizer that owns its end.
//!
//! The finalizer is the attempt's only owner after the executor returns. Records reach the
//! client only through [`egress::deliver`], which commits at the first deliverable byte; an
//! attempt that ends before it is settled in three ordered steps before the next attempt
//! starts:
//! 1. the response stream has a terminal that keeps its records (`UpstreamError` from
//!    `pipeline::start`, or the producer's own);
//! 2. the attempt's usage reader drains to that terminal and the tracker settles once;
//! 3. the stream is discarded with `Terminal::Gone`.
//!
//! Step 2 runs on the attempt's own stream task from the moment the attempt starts, so a
//! client that goes away neither drops the usage record nor loses its final usage. Step 3
//! belongs to the [`Exchange`], which owns every stream of the request and discards an
//! attempt's stream only after its settlement released it. The finalizer prevents
//! duplicate accounting in cooker; it cannot stop an upstream from billing a request whose
//! response failed before commitment.

use std::future::Future;
use std::sync::Arc;

use axum::http::HeaderMap;
use axum::response::Response;
use ck_core::config::Config;
use ck_core::exec::{ExecError, ExecRequest, FailureScope};
use ck_exec::pipeline::{Producer, Started};
use ck_exec::types::{Attempt, Client, Request};
use ck_stream::{Terminal, Writer};
use ck_translate::codec::event::{Direction, Identity, LocalId};
use ck_translate::codec::provenance::{self, Admission};
use ck_translate::codec::{self, OwnedRequest, RequestView, ValidatedRequest};
use futures_util::FutureExt;
use tokio::sync::oneshot;

use super::{Call, Fault, Trace};
use crate::http::egress::{self, Failed};
use crate::http::respond;
use crate::inference::responses_state::Publication;
use crate::inference::streams::{Exchange, Settled, Uncommitted};
use crate::observability::usage_record::{Facts, Tracker};
use crate::observability::usage_snapshot;
use crate::runtime::state::{Lease, Outcome, Runtime};
use crate::scheduling::classify;

/// The contract arm's executor selector: [`ck_exec::Executors`] in the server.
pub trait Arms: Send + Sync {
    /// `None`: the provider is not converted, so the attempt takes the old arm.
    fn destination(&self, attempt: &Attempt<'_>) -> Option<&'static str>;
    /// Re-fences the client view for this attempt's destination (contract §4 step 2).
    fn refence<'a>(&self, attempt: &Attempt<'_>, view: RequestView<'a>) -> codec::Result<ValidatedRequest<'a>>;
    /// Runs the attempt and starts its producer (`ck_exec::pipeline::start`).
    fn start(
        &self,
        attempt: &Attempt<'_>,
        request: &mut Request<'_>,
        upload: &mut Writer,
        response: Writer,
    ) -> impl Future<Output = Result<Started, ExecError>> + Send;
}

impl Arms for ck_exec::Executors {
    fn destination(&self, attempt: &Attempt<'_>) -> Option<&'static str> {
        ck_exec::Executors::destination(self, attempt)
    }

    fn refence<'a>(&self, attempt: &Attempt<'_>, view: RequestView<'a>) -> codec::Result<ValidatedRequest<'a>> {
        ck_exec::Executors::refence(self, attempt, view)
    }

    fn start(
        &self,
        attempt: &Attempt<'_>,
        request: &mut Request<'_>,
        upload: &mut Writer,
        response: Writer,
    ) -> impl Future<Output = Result<Started, ExecError>> + Send {
        ck_exec::Executors::start(self, attempt, request, upload, response)
    }
}

/// A client request its route decoded with the request ingress: the exchange and the
/// client's request document, closed against the `"client"` capability (contract G8).
pub struct Codec {
    pub exchange: Exchange,
    /// The document the ingress closed. The first attempt takes it; an attempt whose executor
    /// released it leaves the next one to rebuild it from the request stream.
    pub request: OwnedRequest,
    /// The exchange's codec request id; every attempt's identity carries it (G1).
    pub request_id: LocalId,
}

/// The exchange across the selection loop. It leaves at commitment.
pub(super) struct Arm<'x, X> {
    arms: &'x X,
    exchange: Option<Exchange>,
    /// The client's request document while no attempt holds or released it.
    request: Option<OwnedRequest>,
    operation: codec::Operation,
    request_id: LocalId,
    /// Attempts started so far; the next one is numbered one higher.
    started: u64,
}

impl<'x, X: Arms> Arm<'x, X> {
    pub(super) fn new(arms: &'x X, codec: Codec) -> Self {
        Self {
            arms,
            operation: codec.request.operation,
            exchange: Some(codec.exchange),
            request: Some(codec.request),
            request_id: codec.request_id,
            started: 0,
        }
    }
}

/// What one candidate came to.
pub(super) enum Ran {
    /// `destination()` is `None` or `refence` answered `Destination`: run the old arm.
    Old,
    /// The client got the first deliverable byte; the attempt settles on a stream task.
    Committed(Box<Committed>),
    /// The destination refused the request (`Unsupported`, `Schema`); nothing was sent.
    Skip(Fault),
    /// A resource failure or gateway defect: no failover, the credential is not at fault.
    Stop(Fault),
    /// An attempt failure before commitment, already settled and discarded.
    Failed(Fault),
}

/// Runs one candidate on the contract arm: decides it (§3.2), starts it, delivers it and,
/// unless it committed, settles and discards it before returning.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run<X: Arms>(
    arm: &mut Arm<'_, X>,
    rt: &Arc<Runtime>,
    cfg: &Config,
    call: &Call,
    trace: &Trace,
    req: &ExecRequest,
    usage: Option<&Arc<Facts>>,
    lease: &mut Lease,
    upstream: &str,
    old_only: bool,
) -> Ran {
    let store = match rt.streams().store() {
        Ok(store) => store,
        Err(error) => return Ran::Stop(fault(stored(&error))),
    };
    let credential = rt.for_executor(&lease.credential);
    let identity = Identity {
        request_id: arm.request_id,
        stream_id: LocalId::new(&uuid::Uuid::new_v4().simple().to_string()).expect("32 ASCII digits"),
        attempt: arm.started + 1,
        direction: Direction::Response,
    };
    let attempt = Attempt {
        credential: &credential,
        cfg,
        model: &req.model,
        headers: &call.headers,
        resolved_model: req.resolved_model.as_ref(),
        store: &store,
        identity,
    };
    // §3.2 step 2: one admission for every candidate, either arm. A compaction item another
    // account produced refuses the candidate (Q-C). Its reasoning and signatures are withheld
    // from this attempt only (Settled 73).
    let auth_index = ck_core::config::credentials::auth_index(&credential);
    let producer = provenance::Producer {
        provider: &credential.provider,
        credential: &auth_index,
    };
    // The client's document, or, once an attempt's executor released it, the same document
    // rebuilt from the request stream as resume does.
    let client = match arm.request.take() {
        Some(client) => client,
        None => match arm
            .exchange
            .as_mut()
            .map(|exchange| rebuild(&store, exchange, arm.operation))
        {
            Some(Ok(client)) => client,
            Some(Err(error)) => return stopped(arm.exchange.as_mut(), error).await,
            None => return Ran::Stop(fault(defect())),
        },
    };
    let admission = match provenance::admit(&client.document, producer) {
        Ok(admission) => admission,
        Err(_) => return Ran::Stop(fault(defect())),
    };
    let destination = arm.arms.destination(&attempt);
    if admission == Admission::Refuse {
        arm.request = Some(client);
        let destination = destination.unwrap_or("old arm");
        return Ran::Skip(skip(&attempt, trace, destination, Refusal::ForeignCompaction));
    }
    let withheld = admission == Admission::Drop;
    let Some(destination) = destination else {
        arm.request = Some(client);
        return old_arm(&attempt, trace, withheld);
    };
    // The withheld document is rebuilt from the request stream and edited for this attempt
    // only, so the client's stays whole for the next one.
    let (own, kept) = if withheld {
        let Some(exchange) = arm.exchange.as_mut() else {
            return Ran::Stop(fault(defect()));
        };
        match withhold(&store, exchange, arm.operation, producer) {
            Ok(own) => (own, Some(client)),
            Err(error) => {
                arm.request = Some(client);
                return stopped(arm.exchange.as_mut(), error).await;
            }
        }
    } else {
        (client, None)
    };
    let fenced = match own.fence(|view| arm.arms.refence(&attempt, view)) {
        Ok(fenced) => fenced,
        Err(refused) => {
            let (error, own) = *refused;
            arm.request = kept.or(Some(own));
            return match error {
                codec::Error::Destination => old_arm(&attempt, trace, withheld),
                error @ (codec::Error::Unsupported | codec::Error::Schema | codec::Error::MaxTokensRequired) => {
                    Ran::Skip(skip(&attempt, trace, destination, Refusal::Fence(error)))
                }
                error => stopped(arm.exchange.as_mut(), error).await,
            };
        }
    };
    if old_only {
        // The candidate passed admission and its fence; only the old arm serves it.
        arm.request = kept.or_else(|| Some(fenced.into_request()));
        return old_arm(&attempt, trace, withheld);
    }
    if withheld {
        log_withheld(&attempt, trace);
    }
    let Some(mut exchange) = arm.exchange.take() else {
        return Ran::Stop(fault(defect()));
    };
    let streams = store
        .create_dependent(&exchange.request)
        .and_then(|response| Ok((store.reader(response.id(), 0)?, response)));
    let (reader, response) = match streams {
        Ok(streams) => streams,
        Err(error) => {
            arm.exchange = Some(exchange);
            return Ran::Stop(fault(stored(&error)));
        }
    };
    arm.started += 1;
    let publication = Publication::new(rt.streams(), &exchange, &response, producer);
    exchange.publication = publication.clone();
    // From here the attempt's settlement runs on its own stream task, so a client that goes
    // away before commitment cannot drop the record or lose its final usage.
    let settled = exchange.attempt(response.allocator());
    let (begun, settlement) = settle_spawn(
        rt,
        usage,
        lease,
        upstream,
        (response.allocator(), reader),
        settled,
        publication,
    );
    let sink = match &settlement.sink {
        Some(sink) => sink.clone().with_capture(req.capture().clone()),
        None => req.usage.clone(),
    };
    let mut request = Request::owned(
        fenced,
        Client {
            caller: &call.caller,
            headers: &call.headers,
            route: &call.request_path,
            alt: call.alt.as_deref(),
            // A Gemini route names its stream choice in its action; other bodies state it.
            streaming: (call.entry == ck_core::format::Format::Gemini).then_some(call.stream),
            session: req.session.as_deref(),
            execution_session: req.execution_session.as_deref(),
            derived_session: req.derived_session.as_deref(),
            usage: &sink,
            // G5 reads the handler-rewritten body without a copy. Requests carry no cooker
            // envelope to unwrap (D2-6k cut, owner 2026-11-15).
            original: &call.body,
        },
    );
    // The client's document: the one this attempt edited is the withheld copy.
    let shown = kept
        .as_ref()
        .map_or_else(|| request.validated().view(), OwnedRequest::view);
    let egress = egress::prepare(&exchange, &response, identity, shown);
    lease.redispatch();
    let started = arm
        .arms
        .start(&attempt, &mut request, &mut exchange.request, response)
        .await;
    // A document the executor kept serves the next attempt, unless this one's was withheld.
    arm.request = kept.or_else(|| request.into_request());
    let Started {
        headers,
        stream,
        producer,
        ..
    } = match started {
        Ok(started) => started,
        Err(error) => {
            arm.exchange = Some(exchange);
            let _ = begun.send(Begun::Refused(error.clone()));
            let _ = settlement.done.await;
            return failed(error);
        }
    };
    let _ = begun.send(Begun::Started {
        producer,
        headers: headers.clone(),
    });
    let mut sent = HeaderMap::new();
    if let Some(value) = trace.header() {
        sent.insert("x-ck-trace-id", value);
    }
    if respond::passthrough_headers(cfg) {
        sent.extend(respond::filter_upstream_headers(&headers));
    } else if ck_exec::kimi::PROVIDERS.contains(&lease.credential.provider.as_str()) {
        sent.extend(super::kimi_proof(&respond::filter_upstream_headers(&headers)));
    }
    match egress::deliver(rt.streams(), exchange, &stream, egress, sent).await {
        Ok(response) => Ran::Committed(Box::new(Committed {
            response,
            done: settlement.done,
        })),
        Err(uncommitted) => {
            let Failed { exchange, reason } = *uncommitted;
            arm.exchange = Some(exchange);
            let mut stream = stream;
            // An egress defect stops the producer, which would otherwise run on unread.
            if matches!(reason, Uncommitted::Codec(_)) {
                stream.finish(Terminal::Cancelled);
            }
            failed(uncommitted_error(reason, settlement.done.await))
        }
    }
}

/// A committed attempt; its settlement reports the producer's outcome.
pub(super) struct Committed {
    response: Response,
    done: oneshot::Receiver<Result<(), ExecError>>,
}

impl Committed {
    /// Completes the lease with the producer's outcome on a stream task, so shutdown waits
    /// for it, and returns the client's response. Failover ended at commitment, so the
    /// outcome only reports to the scheduler; the usage record is already settled by then.
    pub(super) fn spawn(self, rt: &Runtime, lease: Lease) -> Response {
        let Self { response, done } = self;
        lease.accepted();
        rt.streams().spawn(async move {
            lease.complete(match done.await {
                Ok(Ok(())) => Outcome::Success,
                Ok(Err(error)) => Outcome::Failure(error),
                Err(_) => Outcome::Failure(producer_lost()),
            });
        });
        response
    }
}

/// What the request side hands an attempt's settlement once `start` returned.
enum Begun {
    /// The executor failed; `pipeline::start` ended the stream with `UpstreamError`.
    Refused(ExecError),
    /// The producer runs; its outcome is the attempt's.
    Started { producer: Producer, headers: HeaderMap },
}

/// The request side's end of an attempt's settlement.
struct Settlement {
    /// The tracker's usage sink, when the attempt is tracked.
    sink: Option<ck_core::exec::UsageSink>,
    /// The attempt's outcome, sent once its usage record is settled.
    done: oneshot::Receiver<Result<(), ExecError>>,
}

/// §5.2 step 2 on a stream task: waits for the attempt's outcome, reports the stream's end to
/// the index `publication`, drains the usage reader to the stream's terminal, settles the
/// tracker exactly once, releases `settled` so the exchange may discard the attempt's stream,
/// and then reports the outcome. It runs whether or not the client is still there; the index
/// never waits for the usage reader.
fn settle_spawn(
    rt: &Arc<Runtime>,
    usage: Option<&Arc<Facts>>,
    lease: &Lease,
    upstream: &str,
    (response, reader): (Writer, ck_stream::Reader),
    settled: Settled,
    publication: Option<Arc<Publication>>,
) -> (oneshot::Sender<Begun>, Settlement) {
    let mut tracker = usage.map(|facts| {
        let mut tracker = Tracker::start(rt, facts, &lease.credential, upstream);
        if let Some(m) = &lease.lcp {
            tracker.lcp_session(m);
        }
        tracker
    });
    let sink = tracker.as_ref().map(Tracker::sink);
    let (begun, started) = oneshot::channel();
    let (report, done) = oneshot::channel();
    #[cfg(any(test, feature = "testing"))]
    let holds = rt.clone();
    rt.streams().spawn(async move {
        let outcome = match started.await {
            Ok(Begun::Refused(error)) => Err(error),
            Ok(Begun::Started { producer, headers }) => {
                if let Some(tracker) = tracker.as_mut() {
                    tracker.arrived(&headers);
                }
                producer.await.unwrap_or_else(|_| Err(producer_lost()))
            }
            // The request went away inside `start`; the dropped writer ended the stream.
            Err(_) => Err(abandoned()),
        };
        if let Some(publication) = publication {
            #[cfg(any(test, feature = "testing"))]
            holds.streams().holds.publication.pass().await;
            let done = outcome.is_ok() && response.cancelled().now_or_never() == Some(Terminal::Done);
            publication.ended(done);
        }
        #[cfg(any(test, feature = "testing"))]
        holds.streams().holds.usage.pass().await;
        // Every outcome above left the stream with a terminal, so the reader ends.
        let (usage, facts) = usage_snapshot::read(response, reader).await;
        if let Some(mut tracker) = tracker {
            tracker.usage(&usage, &facts);
            match &outcome {
                Ok(()) => tracker.succeed(),
                Err(error) => tracker.fail(error),
            }
        }
        drop(settled);
        let _ = report.send(outcome);
    });
    (begun, Settlement { sink, done })
}

/// The failure of an attempt whose egress returned before its first deliverable byte, from
/// the reason and the settled outcome.
fn uncommitted_error(
    reason: Uncommitted,
    settled: Result<Result<(), ExecError>, oneshot::error::RecvError>,
) -> ExecError {
    match (reason, settled) {
        (Uncommitted::Codec(codec::Error::Storage(error)), _) => stored(&error),
        (Uncommitted::Codec(_), _) => defect(),
        (_, Err(_)) => defect(),
        (_, Ok(Err(error))) => error,
        (Uncommitted::Terminal(terminal), Ok(Ok(()))) => ended(terminal),
        (Uncommitted::Error, Ok(Ok(()))) => defect(),
    }
}

/// An old-arm attempt. It sends the client's own body, which never carries a tagged member, so
/// another account's reasoning is withheld from it as from a contract-arm attempt.
fn old_arm(attempt: &Attempt<'_>, trace: &Trace, withheld: bool) -> Ran {
    if withheld {
        log_withheld(attempt, trace);
    }
    Ran::Old
}

fn log_withheld(attempt: &Attempt<'_>, trace: &Trace) {
    tracing::info!(
        target: "ck_dispatch",
        request_id = %trace.request_id(),
        auth_index = %ck_core::config::credentials::auth_index(attempt.credential),
        provider = %attempt.credential.provider,
        "reasoning from another account withheld"
    );
}

/// Contract §9 for a request-side codec failure before anything was sent: a closed request
/// stream answers with its terminal.
async fn stopped(exchange: Option<&mut Exchange>, error: codec::Error) -> Ran {
    match error {
        codec::Error::Storage(ck_stream::Error::Closed) => match exchange {
            Some(exchange) => Ran::Stop(fault(ended(exchange.request.cancelled().await))),
            None => Ran::Stop(fault(defective())),
        },
        codec::Error::Storage(error) => Ran::Stop(fault(stored(&error))),
        _ => Ran::Stop(fault(defect())),
    }
}

/// The request document rebuilt from the exchange's request stream (contract §2).
fn rebuild(
    store: &ck_stream::Store,
    exchange: &mut Exchange,
    operation: codec::Operation,
) -> codec::Result<OwnedRequest> {
    let mut records = store.reader(exchange.request.id(), 0)?;
    let (document, media) = codec::chat::request::read_request(&mut exchange.request, &mut records)?;
    Ok(OwnedRequest {
        document,
        media,
        operation,
    })
}

/// The request document without the reasoning another account produced, rebuilt for one attempt.
fn withhold(
    store: &ck_stream::Store,
    exchange: &mut Exchange,
    operation: codec::Operation,
    producer: provenance::Producer<'_>,
) -> codec::Result<OwnedRequest> {
    let mut own = rebuild(store, exchange, operation)?;
    provenance::drop_foreign(&mut exchange.request, &mut own.document, producer)?;
    Ok(own)
}

/// Why a candidate is skipped before anything is sent.
enum Refusal {
    /// The destination's fence refused the request.
    Fence(codec::Error),
    /// The history holds a compaction item another account produced (Q-C).
    ForeignCompaction,
}

/// A destination refusal: logged without content, neutral for the credential, which the
/// selection already excluded for this request.
fn skip(attempt: &Attempt<'_>, trace: &Trace, destination: &str, refusal: Refusal) -> Fault {
    // `MaxTokensRequired` is the destination's own refusal and the client reads it as is (Q-H).
    let reason = match refusal {
        Refusal::Fence(codec::Error::Schema) => "the destination's schema refused the request",
        Refusal::Fence(codec::Error::MaxTokensRequired) => "max_tokens is required",
        Refusal::Fence(_) => "the destination does not support the request",
        Refusal::ForeignCompaction => "another account produced the compaction item",
    };
    // Only a missing limit names the setting that would have supplied it.
    let missing_setting = matches!(refusal, Refusal::Fence(codec::Error::MaxTokensRequired))
        .then_some("model-catalog default-max-tokens");
    tracing::info!(
        target: "ck_dispatch",
        request_id = %trace.request_id(),
        auth_index = %ck_core::config::credentials::auth_index(attempt.credential),
        provider = %attempt.credential.provider,
        upstream_model = %attempt.model,
        destination,
        reason,
        missing_setting,
        "candidate skipped"
    );
    fault(ExecError::local(400, FailureScope::Request, reason))
}

/// Whether the provider's old-arm `responses/compact` is a native upstream call. Every other
/// provider's old arm would answer with a local summary, a refusal or an error.
pub(super) fn native_compact(provider: &str) -> bool {
    provider == "codex" || provider == ck_exec::xai::PROVIDER || ck_exec::openai_compat::handles(provider)
}

/// The client's wording for a compact no candidate can serve natively.
const NO_NATIVE_COMPACT: &str = "This model does not support compaction; choose a model that does.";

/// An old-arm compact refused before any send: logged without content, neutral for the
/// credential like a destination refusal.
pub(super) fn no_native_compact(trace: &Trace, credential: &ck_core::credential::Credential, upstream: &str) -> Fault {
    tracing::info!(
        target: "ck_dispatch",
        request_id = %trace.request_id(),
        auth_index = %ck_core::config::credentials::auth_index(credential),
        provider = %credential.provider,
        upstream_model = %upstream,
        destination = "old",
        reason = "no native compact",
        "candidate skipped"
    );
    fault(ExecError::local(400, FailureScope::Request, NO_NATIVE_COMPACT))
}

fn fault(error: ExecError) -> Fault {
    Fault { error, bootstrap: None }
}

/// A resource failure or defect stops the request; any other attempt failure goes to the
/// scheduler's error policy.
fn failed(error: ExecError) -> Ran {
    let stops = error.scope == FailureScope::Request
        && error.headers.is_empty()
        && matches!(
            (error.status, &error.body[..]),
            (503, b"stream.storage_exhausted")
                | (410, b"stream.gone")
                | (500, b"stream.producer_lost")
                | (499 | 503, b"stream.cancelled" | b"stream.shutdown")
        );
    if stops {
        Ran::Stop(fault(error))
    } else {
        Ran::Failed(fault(error))
    }
}

/// Contract §9 for a storage error before commitment.
pub(crate) fn stored(error: &ck_stream::Error) -> ExecError {
    match error {
        ck_stream::Error::StorageExhausted | ck_stream::Error::Io(_) => {
            ExecError::local(503, FailureScope::Request, "stream.storage_exhausted")
        }
        ck_stream::Error::Gone => ExecError::local(410, FailureScope::Request, "stream.gone"),
        _ => defective(),
    }
}

/// The row of a stream that already has `terminal` (§3.2, `Storage(Closed)`).
fn ended(terminal: Terminal) -> ExecError {
    match terminal {
        // ponytail: the client status of a cancel or shutdown before commitment is not
        // specified; 499 and 503 name them until the routes switch (D2-3a).
        Terminal::Cancelled | Terminal::ClientDisconnected => {
            ExecError::local(499, FailureScope::Request, "stream.cancelled")
        }
        Terminal::Shutdown => ExecError::local(503, FailureScope::Request, "stream.shutdown"),
        Terminal::Gone => stored(&ck_stream::Error::Gone),
        Terminal::StorageExhausted => stored(&ck_stream::Error::StorageExhausted),
        // A complete answer that gave the client nothing ended before its first payload: the
        // upstream's failure, which fails over (contract §6).
        Terminal::Done => classify::empty_stream(),
        Terminal::UpstreamError | Terminal::ProducerLost => defective(),
    }
}

fn defect() -> ExecError {
    tracing::error!(target: "ck_dispatch", "gateway defect before commitment");
    defective()
}

/// A gateway defect: 500, never failed over.
fn defective() -> ExecError {
    ExecError::local(500, FailureScope::Request, "stream.producer_lost")
}

/// The client went away while the executor ran: the attempt's record fails with the cancel
/// status.
fn abandoned() -> ExecError {
    ExecError::local(499, FailureScope::Request, "stream.cancelled")
}

/// A producer panic: 502, an attempt failure that may fail over (contract §9).
fn producer_lost() -> ExecError {
    ExecError::local(502, FailureScope::Transport, "stream.producer_lost")
}
