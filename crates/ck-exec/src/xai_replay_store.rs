//! The in-process Grok reasoning replay cache (internal/cache/xai_reasoning_replay_cache.go),
//! bounded by entry count, entry size and total size.
//!
//! An entry outlives the response stream it came from, so it lives on the heap; the byte
//! bounds keep that heap bounded. Go bounds only the entry count. A turn's items are charged
//! to a [`Reservation`] before they are allocated, so the bounds hold while they are built.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::xai_replay::ReplayScope;

/// `XAIReasoningReplayCacheTTL`.
const TTL: Duration = Duration::from_secs(3600);
/// `XAIReasoningReplayCacheMaxEntries`.
const MAX_ENTRIES: usize = 10240;
/// `XAIReasoningReplayCacheEvictBatchSize`.
const EVICT_BATCH: usize = 128;
/// The largest turn one entry keeps, as the shared signed-thinking replay bounds it.
const MAX_ENTRY_BYTES: usize = crate::replay::MAX_BYTES_PER_ENTRY;
/// The most bytes all entries keep together; the oldest entries go first beyond it.
const MAX_BYTES: usize = 256 << 20;

struct Entry {
    items: Vec<Vec<u8>>,
    bytes: usize,
    timestamp: Instant,
}

#[derive(Default)]
struct Entries {
    map: HashMap<String, Entry>,
    bytes: usize,
    /// Bytes reservations hold for turns still being built.
    reserved: usize,
}

impl Entries {
    fn remove(&mut self, key: &str) {
        if let Some(entry) = self.map.remove(key) {
            self.bytes -= entry.bytes;
        }
    }

    /// Drops the oldest entries: a batch past the count bound, then until the bytes fit.
    fn evict(&mut self, max_bytes: usize) {
        if self.map.len() <= MAX_ENTRIES && self.bytes <= max_bytes {
            return;
        }
        let mut oldest: Vec<(Instant, String)> = self.map.iter().map(|(k, e)| (e.timestamp, k.clone())).collect();
        oldest.sort();
        let batch = if self.map.len() > MAX_ENTRIES { EVICT_BATCH } else { 0 };
        for (i, (_, key)) in oldest.into_iter().enumerate() {
            if i >= batch && self.bytes <= max_bytes {
                break;
            }
            self.remove(&key);
        }
    }
}

/// The in-process replay cache (Go's package-level map).
// ponytail: Go also purges expired entries from a background ticker; here an expired
// entry is dropped when read and the count and byte bounds limit memory.
pub(crate) struct Store {
    entries: Mutex<Entries>,
    /// [`MAX_ENTRY_BYTES`] and [`MAX_BYTES`].
    limits: (usize, usize),
}

impl Default for Store {
    fn default() -> Self {
        Self::with_limits(MAX_ENTRY_BYTES, MAX_BYTES)
    }
}

/// `xaiReasoningReplayCacheKey`.
fn cache_key(scope: &ReplayScope) -> Option<String> {
    let (model, session) = (scope.model_name.trim(), scope.session_key.trim());
    (!model.is_empty() && !session.is_empty()).then(|| format!("xai-reasoning-replay\0{model}\0{session}"))
}

impl Store {
    pub(crate) fn with_limits(entry: usize, total: usize) -> Self {
        Self {
            entries: Mutex::default(),
            limits: (entry, total),
        }
    }

    /// `GetXAIReasoningReplayItemsRequired`: a hit refreshes the entry's TTL.
    pub(crate) fn get(&self, scope: &ReplayScope) -> Option<Vec<Vec<u8>>> {
        let key = cache_key(scope)?;
        let mut entries = self.lock();
        let now = Instant::now();
        let entry = entries.map.get_mut(&key)?;
        if now.duration_since(entry.timestamp) > TTL {
            entries.remove(&key);
            return None;
        }
        entry.timestamp = now;
        Some(entry.items.clone())
    }

    /// Keeps the normalized `items` of a turn; false when the turn is larger than one entry
    /// may be. The reservation the items were built under ends here.
    pub(crate) fn put(&self, scope: &ReplayScope, items: Vec<Vec<u8>>, mut held: Reservation<'_>) -> bool {
        let Some(key) = cache_key(scope) else {
            return false;
        };
        let bytes = items.iter().map(Vec::len).sum();
        if bytes > self.limits.0 {
            return false;
        }
        let mut entries = self.lock();
        entries.reserved -= std::mem::take(&mut held.bytes);
        entries.remove(&key);
        entries.bytes += bytes;
        let timestamp = Instant::now();
        entries.map.insert(
            key,
            Entry {
                items,
                bytes,
                timestamp,
            },
        );
        let room = self.limits.1 - entries.reserved;
        entries.evict(room);
        true
    }

    /// An empty reservation for a turn about to be built.
    pub(crate) fn reserve(&self) -> Reservation<'_> {
        Reservation { store: self, bytes: 0 }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Entries> {
        self.entries.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `DeleteXAIReasoningReplayItemRequired`.
    pub(crate) fn delete(&self, scope: &ReplayScope) {
        if let Some(key) = cache_key(scope) {
            self.lock().remove(&key);
        }
    }
}

/// Heap a turn's items take while they are built: charged against the entry bound and,
/// together with the entries, the total bound. Dropping it returns the bytes.
pub(crate) struct Reservation<'a> {
    store: &'a Store,
    bytes: usize,
}

impl Reservation<'_> {
    /// Runs `check` with `bytes` charged; `None`, without running it, when the charge is
    /// refused. What `check` allocates is freed when it returns, so the charge ends then.
    pub(crate) fn scratch<T>(&mut self, bytes: usize, check: impl FnOnce() -> T) -> Option<T> {
        if !self.charge(bytes) {
            return None;
        }
        let result = check();
        self.bytes -= bytes;
        self.store.lock().reserved -= bytes;
        Some(result)
    }

    /// Charges `bytes` more before they are allocated, evicting the oldest entries to make
    /// room; false, charging nothing, when the turn would outgrow an entry or reservations
    /// would outgrow the cache.
    pub(crate) fn charge(&mut self, bytes: usize) -> bool {
        let (entry, total) = self.store.limits;
        let Some(next) = self.bytes.checked_add(bytes).filter(|n| *n <= entry) else {
            return false;
        };
        let mut entries = self.store.lock();
        let Some(reserved) = entries.reserved.checked_add(bytes).filter(|r| *r <= total) else {
            return false;
        };
        entries.reserved = reserved;
        entries.evict(total - reserved);
        self.bytes = next;
        true
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.store.lock().reserved -= self.bytes;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(session: &str) -> ReplayScope {
        ReplayScope {
            model_name: "grok-4.3".into(),
            session_key: session.into(),
        }
    }

    /// A turn larger than an entry is refused, and the oldest entries leave once the total
    /// passes its bound; the byte count follows every insert, replacement and removal.
    #[test]
    fn entries_are_bounded_in_bytes() {
        let store = Store::with_limits(10, 25);
        assert!(!store.put(&scope("big"), vec![vec![b'x'; 11]], store.reserve()));
        for session in ["a", "b", "c"] {
            assert!(store.put(&scope(session), vec![vec![b'x'; 10]], store.reserve()));
        }
        let bytes = || store.entries.lock().unwrap().bytes;
        assert_eq!(bytes(), 20);
        assert!(store.get(&scope("a")).is_none(), "the oldest entry stays");
        assert!(store.get(&scope("c")).is_some());
        assert!(store.put(&scope("c"), vec![vec![b'x'; 4]], store.reserve()));
        assert_eq!(bytes(), 14);
        store.delete(&scope("b"));
        assert_eq!(bytes(), 4);
    }

    /// A reservation is refused past the entry bound or the cache's room, evicts the oldest
    /// entries to fit, and returns its bytes when dropped or when its turn is kept; a kept
    /// turn evicts entries to leave room for other reservations.
    #[test]
    fn reservations_bound_turns_before_they_are_built() {
        let store = Store::with_limits(10, 25);
        assert!(store.put(&scope("a"), vec![vec![b'x'; 10]], store.reserve()));
        assert!(store.put(&scope("b"), vec![vec![b'x'; 10]], store.reserve()));
        let state = || {
            let entries = store.entries.lock().unwrap();
            (entries.bytes, entries.reserved)
        };
        let mut first = store.reserve();
        assert!(first.charge(6) && !first.charge(5), "an entry holds 10 bytes");
        assert_eq!(state(), (10, 6), "the oldest entry made room");
        assert!(store.get(&scope("a")).is_none());
        let mut second = store.reserve();
        assert!(second.charge(10));
        assert!(!store.reserve().charge(10), "reservations fill the cache");
        assert_eq!(state(), (0, 16));
        drop(second);
        assert_eq!(state(), (0, 6));
        assert!(store.put(&scope("c"), vec![vec![b'x'; 4]], first));
        assert_eq!(state(), (4, 0));
        let mut other = store.reserve();
        assert!(other.charge(10));
        assert!(store.put(&scope("d"), vec![vec![b'x'; 10]], store.reserve()));
        assert!(store.put(&scope("e"), vec![vec![b'x'; 10]], store.reserve()));
        assert_eq!(state(), (10, 10), "entries leave room for what is reserved");
    }

    /// A kept turn's reservation becomes its entry in one step, so it does not count twice
    /// and evict an entry the cache has room for.
    #[test]
    fn a_kept_turn_hands_over_its_reservation() {
        let store = Store::with_limits(10, 25);
        assert!(store.put(&scope("a"), vec![vec![b'x'; 10]], store.reserve()));
        assert!(store.put(&scope("b"), vec![vec![b'x'; 10]], store.reserve()));
        let mut held = store.reserve();
        assert!(held.charge(5));
        assert!(store.put(&scope("c"), vec![vec![b'x'; 5]], held));
        assert_eq!(store.entries.lock().unwrap().bytes, 25);
        assert!(store.get(&scope("a")).is_some());
    }
}
