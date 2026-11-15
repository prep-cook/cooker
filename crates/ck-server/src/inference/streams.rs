//! Client exchanges and their retained response streams (`docs/EXECUTION-CONTRACT.md`,
//! `docs/INTERNAL-STREAM.md` §6.6–6.7).
//!
//! An [`Exchange`] is one client request. Its request stream holds the client request
//! records and every attempt's encoded upload. Each routing attempt writes its own response
//! stream, which holds the request stream. The attempt whose egress commits becomes the
//! exchange's [`StreamHandle`], registered under a fresh [`StreamId`]. Resume
//! (`GET /v1/streams`) authorizes the caller against the handle's owner before any cursor
//! lookup and re-encodes with the handle's output.

mod cursor;
mod protocol;
mod resume;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use axum::http::HeaderValue;
use ck_core::exec::Caller;
use ck_stream::{Store, Terminal, Writer};

use crate::http::egress::Output;

pub use cursor::{Cursor, Units};
pub use protocol::ProtocolEgress;
pub(crate) use resume::{Replay, delivery};
pub use resume::{Uncommitted, resume};

/// Request and response header carrying a [`StreamId`]. Request capture and logs redact it.
pub const STREAM_ID_HEADER: &str = "x-ck-stream-id";
/// Response header carrying the cursor a response starts after.
pub const CURSOR_HEADER: &str = "x-ck-stream-cursor";

/// The resume identity: 128 random bits, sent only in the `X-Ck-Stream-Id` header. It has
/// no `Debug` or `Display`, so it cannot reach a log through formatting.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamId(pub [u8; 16]);

impl StreamId {
    pub fn random() -> Self {
        let mut bytes = [0; 16];
        getrandom::fill(&mut bytes).expect("system randomness");
        Self(bytes)
    }

    /// The `X-Ck-Stream-Id` value: 32 lowercase hexadecimal digits.
    pub fn header_value(&self) -> HeaderValue {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut text = [0; 32];
        for (i, byte) in self.0.iter().enumerate() {
            text[2 * i] = HEX[usize::from(byte >> 4)];
            text[2 * i + 1] = HEX[usize::from(byte & 15)];
        }
        HeaderValue::from_bytes(&text).expect("hexadecimal digits")
    }

    /// The inverse of [`StreamId::header_value`]; `None` for any other text.
    pub fn parse(text: &[u8]) -> Option<Self> {
        let digit = |c: u8| match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            _ => None,
        };
        if text.len() != 32 {
            return None;
        }
        let mut bytes = [0; 16];
        for (byte, [high, low]) in bytes.iter_mut().zip(text.as_chunks::<2>().0) {
            *byte = (digit(*high)? << 4) | digit(*low)?;
        }
        Some(Self(bytes))
    }
}

/// The identity that created an exchange. A resume must match both parts.
#[derive(Clone, PartialEq, Eq)]
pub struct Owner {
    pub caller: Caller,
    /// The call scope (`sess_...`) of an ephemeral client key; `None` for any other key.
    pub scope: Option<String>,
}

/// One client request from ingress to commitment, and the owner of its streams until then.
///
/// The exchange is the one place that discards streams (contract §7): created together with
/// its request stream, it holds that stream and the in-flight attempt's response stream. A
/// new attempt discards the previous one's stream. Dropped without commitment, on any exit
/// (an answer from the old arm, a refusal, a routing error, a client disconnect or a panic),
/// it discards the attempt's stream and then the request stream, both once that attempt's
/// usage is settled ([`Settled`]). Commitment keeps both and disarms the guard.
pub struct Exchange {
    pub owner: Owner,
    pub output: Output,
    /// The request stream's producer. Request ingress publishes the client request into it,
    /// and each attempt's request egress encodes its upload into its scratch. At commitment
    /// the exchange finishes it with `Terminal::Done` and keeps an allocation handle.
    pub request: Writer,
    /// The in-flight attempt: an allocation handle on its response stream and its settlement.
    attempt: Option<(Writer, Arc<Mutex<Settlement>>)>,
    committed: bool,
}

/// Streams an attempt's settlement discards once it is done, or at once when it already is.
#[derive(Default)]
struct Settlement {
    settled: bool,
    discard: Vec<Writer>,
}

impl Settlement {
    fn discard(this: &Mutex<Self>, mut stream: Writer) {
        let mut settlement = this.lock().unwrap_or_else(PoisonError::into_inner);
        if settlement.settled {
            stream.finish(Terminal::Gone);
        } else {
            settlement.discard.push(stream);
        }
    }
}

/// An attempt's settlement token: dropped once the attempt's usage record is settled, by its
/// settlement task or by that task's unwinding, and only then may the exchange discard the
/// attempt's stream.
pub struct Settled(Arc<Mutex<Settlement>>);

impl Drop for Settled {
    fn drop(&mut self) {
        let discard = {
            let mut settlement = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            settlement.settled = true;
            std::mem::take(&mut settlement.discard)
        };
        for mut stream in discard {
            stream.finish(Terminal::Gone);
        }
    }
}

impl Exchange {
    /// Creates the request stream and its exchange.
    pub fn new(owner: Owner, output: Output, store: &Store) -> Result<Self, ck_stream::Error> {
        Ok(Self {
            owner,
            output,
            request: store.create()?,
            attempt: None,
            committed: false,
        })
    }

    /// Takes ownership of a new attempt's response stream (an allocation handle) and returns
    /// its settlement token. The previous attempt's stream is discarded.
    pub fn attempt(&mut self, response: Writer) -> Settled {
        let settlement = Arc::new(Mutex::new(Settlement::default()));
        if let Some((previous, settled)) = self.attempt.replace((response, settlement.clone())) {
            Settlement::discard(&settled, previous);
        }
        Settled(settlement)
    }

    /// Commitment without registration: the request stream ends with `Done` and the exchange
    /// becomes the handle `id` over `response`, the committing attempt's allocation handle.
    /// Both streams stay retained.
    pub fn into_handle(mut self, id: StreamId, response: Writer) -> StreamHandle {
        self.committed = true;
        self.attempt = None;
        self.request.finish(Terminal::Done);
        StreamHandle {
            id,
            owner: self.owner.clone(),
            output: self.output,
            response,
            request: self.request.allocator(),
        }
    }

    /// Commitment (`docs/EXECUTION-CONTRACT.md` §6): the attempt whose egress emitted the
    /// first deliverable byte becomes the exchange's handle, registered under a fresh id.
    pub(crate) fn commit(self, registry: &Registry, response: Writer) -> StreamId {
        let id = StreamId::random();
        registry.register(self.into_handle(id, response));
        id
    }
}

impl Drop for Exchange {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let request = self.request.allocator();
        match self.attempt.take() {
            // The request stream goes after the attempt that depends on it.
            Some((response, settlement)) => {
                Settlement::discard(&settlement, response);
                Settlement::discard(&settlement, request);
            }
            None => {
                let mut request = request;
                request.finish(Terminal::Gone);
            }
        }
    }
}

/// A committed response retained for delivery and resume until the response stream expires
/// or is evicted. A client disconnect releases only its reader.
pub struct StreamHandle {
    pub id: StreamId,
    pub owner: Owner,
    pub output: Output,
    /// Allocation handle on the committed response stream. Egress decodes and encodes with
    /// it; readers attach through `Store::reader(response.id(), ..)`; `finish(Cancelled)` is
    /// the explicit client cancel.
    pub response: Writer,
    /// Allocation handle on the request stream, which the response stream holds. Resume
    /// rebuilds the request view from its records (`codec::chat::request::read_request`).
    pub request: Writer,
}

impl StreamHandle {
    /// Expiry or eviction invalidated the response stream. A zero-byte metadata reservation
    /// fails with `Gone` exactly then, and unlike a reader it refreshes no last-read time.
    fn gone(&self) -> bool {
        self.response.allocator().reserve_metadata(0).is_err()
    }
}

/// What a resume of a live committed stream gets: fresh allocation handles, so concurrent
/// readers never share a codec owner.
pub(crate) struct Attached {
    pub output: Output,
    pub response: Writer,
    pub request: Writer,
}

/// The runtime's stream store, committed handles and stream tasks ([`crate::Runtime::streams`]).
#[derive(Default)]
pub struct Registry {
    /// Shared so an attempt can hold the store across its awaits (`ck_exec::types::Attempt`).
    store: RwLock<Option<Arc<Store>>>,
    handles: Mutex<Handles>,
    tasks: tokio_util::task::TaskTracker,
    closed: AtomicBool,
}

#[derive(Default)]
struct Handles {
    live: HashMap<StreamId, StreamHandle>,
    /// Registration sweeps out handles of invalidated streams once `live` reaches this size,
    /// then doubles it, so the sweep costs amortized constant time per registration.
    sweep_at: usize,
}

impl Registry {
    /// Installs the store. Refused after shutdown or when one is already open.
    pub fn install(&self, store: Store) -> Result<(), Store> {
        let mut slot = self.store.write().unwrap_or_else(PoisonError::into_inner);
        if slot.is_some() || self.closed.load(Ordering::Acquire) {
            return Err(store);
        }
        *slot = Some(Arc::new(store));
        Ok(())
    }

    /// Runs `f` on the store. `Closed` after shutdown.
    pub fn with_store<R>(&self, f: impl FnOnce(&Store) -> R) -> Result<R, ck_stream::Error> {
        self.store().map(|store| f(&store))
    }

    /// The store, for an attempt that holds it across awaits. `Closed` after shutdown.
    // ponytail: the store opens at first use from the §6.4 defaults (`$XDG_RUNTIME_DIR/cooker`);
    // Phase F opens it at startup from the deploy-only `stream.*` settings through `install`.
    pub fn store(&self) -> Result<Arc<Store>, ck_stream::Error> {
        if let Some(store) = self.store.read().unwrap_or_else(PoisonError::into_inner).as_ref() {
            return Ok(store.clone());
        }
        let mut slot = self.store.write().unwrap_or_else(PoisonError::into_inner);
        if self.closed.load(Ordering::Acquire) {
            return Err(ck_stream::Error::Closed);
        }
        if slot.is_none() {
            let config = ck_stream::Config::from_environment().map_err(ck_stream::Error::Io)?;
            *slot = Some(Arc::new(Store::open(config).map_err(ck_stream::Error::Io)?));
        }
        Ok(slot.as_ref().expect("opened above").clone())
    }

    /// Registers a committed handle under its id. After shutdown began, its response stops
    /// with `Shutdown` instead.
    pub fn register(&self, mut handle: StreamHandle) {
        let mut handles = self.handles.lock().unwrap_or_else(PoisonError::into_inner);
        if self.closed.load(Ordering::Acquire) {
            drop(handles);
            handle.response.finish(Terminal::Shutdown);
            return;
        }
        if handles.live.len() >= handles.sweep_at {
            handles.live.retain(|_, handle| !handle.gone());
            handles.sweep_at = (2 * handles.live.len()).max(64);
        }
        handles.live.insert(handle.id, handle);
    }

    /// Fresh allocation handles on the live stream `id` that `owner` created. `None` alike
    /// for an unknown, foreign, expired or evicted id.
    pub(crate) fn attach(&self, id: &StreamId, owner: &Owner) -> Option<Attached> {
        let handles = self.handles.lock().unwrap_or_else(PoisonError::into_inner);
        // Liveness is settled here, before any cursor check, and without refreshing the
        // stream's last read: an expired handle the sweep has not removed yet is unknown.
        let handle = handles.live.get(id).filter(|h| h.owner == *owner && !h.gone())?;
        Some(Attached {
            output: handle.output,
            response: handle.response.allocator(),
            request: handle.request.allocator(),
        })
    }

    /// The explicit client cancel (contract §7): the response stream ends with `Cancelled`
    /// and its producer stops; the stream stays retained. `false` where `attach` finds nothing.
    pub fn cancel(&self, id: &StreamId, owner: &Owner) -> bool {
        let Some(mut attached) = self.attach(id, owner) else {
            return false;
        };
        attached.response.finish(Terminal::Cancelled);
        true
    }

    /// Spawns a producer or a usage reader; shutdown waits for it after stopping its stream.
    pub fn spawn<F>(&self, task: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.tasks.spawn(task)
    }

    /// Refuses new registrations and stops every committed stream with `Shutdown`, which
    /// attached readers deliver before the listener closes their connections.
    pub fn begin_shutdown(&self) {
        let live = {
            let mut handles = self.handles.lock().unwrap_or_else(PoisonError::into_inner);
            self.closed.store(true, Ordering::Release);
            std::mem::take(&mut handles.live)
        };
        for (_, mut handle) in live {
            handle.response.finish(Terminal::Shutdown);
        }
    }

    /// After the listener closed: stops every remaining stream, including uncommitted
    /// exchanges, waits for the stream tasks and joins the store's maintenance thread.
    /// Leases still held keep their mapping until they drop.
    pub async fn finish_shutdown(&self) -> std::io::Result<()> {
        self.begin_shutdown();
        let store = self.store.write().unwrap_or_else(PoisonError::into_inner).take();
        // The listener closed, so no request handler holds the store any more; stream tasks
        // hold writers, never the store. A holder left over shuts it down when it drops.
        let stopped = match store.map(Arc::try_unwrap) {
            None => Ok(()),
            Some(Ok(mut store)) => store.shutdown(),
            Some(Err(_)) => {
                tracing::warn!("stream store still shared at shutdown; it stops when released");
                Ok(())
            }
        };
        self.tasks.close();
        self.tasks.wait().await;
        stopped
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::egress::{Framing, Protocol};

    #[test]
    fn registration_sweeps_out_handles_of_invalidated_streams() {
        let directory = std::env::temp_dir().join(format!("ck-registry-{}", uuid::Uuid::new_v4()));
        let mut config = ck_stream::Config::new(directory.clone());
        config.pool_bytes = Some(64 * ck_stream::FRAME_BYTES);
        config.workers = 1;
        let registry = Registry::default();
        assert!(registry.install(Store::open(config).unwrap()).is_ok());
        let owner = Owner {
            caller: Caller {
                principal: "sk".into(),
                source: "authorization",
                provider: "config-inline".into(),
            },
            scope: None,
        };
        let register = |gone: bool| {
            let (request, mut response) = registry
                .with_store(|store| {
                    let request = store.create().unwrap();
                    let response = store.create_dependent(&request).unwrap();
                    (request, response)
                })
                .unwrap();
            if gone {
                response.finish(Terminal::Gone);
            }
            registry.register(StreamHandle {
                id: StreamId::random(),
                owner: owner.clone(),
                output: Output {
                    protocol: Protocol::Chat,
                    operation: ck_translate::codec::Operation::Generate,
                    framing: Framing::Sse,
                },
                response: response.allocator(),
                request: request.allocator(),
            });
        };
        for _ in 0..64 {
            register(true);
        }
        assert_eq!(registry.handles.lock().unwrap().live.len(), 64);
        register(false);
        assert_eq!(
            registry.handles.lock().unwrap().live.len(),
            1,
            "the invalidated handles left"
        );
        drop(registry);
        std::fs::remove_dir_all(&directory).unwrap();
    }
}
