//! The runtime-owned response producer (`docs/EXECUTION-CONTRACT.md`).
//!
//! After [`Execute`](crate::types::Execute) returns an [`Upstream`](crate::types::Upstream),
//! the producer owns the attempt's writer, the upstream body and the response ingress, and
//! drives `upstream bytes -> ResponseIngress -> Writer` until a terminal record. It runs on
//! its own task, independently of the client: a slow or disconnected client never slows or
//! stops upstream reads (§6.6). It schedules `flush` at the ingress `flush_deadline`.
//!
//! Terminal records: a complete upstream publishes `done`; an upstream or decode failure
//! publishes `error`; `Writer::cancelled` resolving (cancel, shutdown, expiry, eviction)
//! stops reading. A panic drops the writer, which publishes `stream.producer_lost`.

use ck_core::exec::ExecError;
use http::HeaderMap;
use tokio::task::JoinHandle;

/// The producer task. Its output is the attempt's routing outcome: `Err` carries the
/// failure scope of an upstream that failed after answering, for the scheduler and
/// cooldown. Whether that failure may fail over is the server's commitment decision.
pub type Producer = JoinHandle<Result<(), ExecError>>;

/// An attempt whose upstream answered with a success status and whose producer is running.
pub struct Started {
    pub status: u16,
    /// Upstream headers; the server decides which, if any, reach the client.
    pub headers: HeaderMap,
    pub producer: Producer,
}
