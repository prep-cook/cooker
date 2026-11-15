//! The in-process Grok reasoning replay cache (internal/cache/xai_reasoning_replay_cache.go),
//! bounded by entry count, entry size and total size.
//!
//! An entry outlives the response stream it came from, so it lives on the heap; the byte
//! bounds keep that heap bounded. Go bounds only the entry count.

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
    fn with_limits(entry: usize, total: usize) -> Self {
        Self {
            entries: Mutex::default(),
            limits: (entry, total),
        }
    }

    /// `GetXAIReasoningReplayItemsRequired`: a hit refreshes the entry's TTL.
    pub(crate) fn get(&self, scope: &ReplayScope) -> Option<Vec<Vec<u8>>> {
        let key = cache_key(scope)?;
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
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
    /// may be.
    pub(crate) fn put(&self, scope: &ReplayScope, items: Vec<Vec<u8>>) -> bool {
        let Some(key) = cache_key(scope) else {
            return false;
        };
        let bytes = items.iter().map(Vec::len).sum();
        if bytes > self.limits.0 {
            return false;
        }
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
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
        entries.evict(self.limits.1);
        true
    }

    /// `DeleteXAIReasoningReplayItemRequired`.
    pub(crate) fn delete(&self, scope: &ReplayScope) {
        if let Some(key) = cache_key(scope) {
            self.entries.lock().unwrap_or_else(|e| e.into_inner()).remove(&key);
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
        assert!(!store.put(&scope("big"), vec![vec![b'x'; 11]]));
        for session in ["a", "b", "c"] {
            assert!(store.put(&scope(session), vec![vec![b'x'; 10]]));
        }
        let bytes = || store.entries.lock().unwrap().bytes;
        assert_eq!(bytes(), 20);
        assert!(store.get(&scope("a")).is_none(), "the oldest entry stays");
        assert!(store.get(&scope("c")).is_some());
        assert!(store.put(&scope("c"), vec![vec![b'x'; 4]]));
        assert_eq!(bytes(), 14);
        store.delete(&scope("b"));
        assert_eq!(bytes(), 4);
    }
}
