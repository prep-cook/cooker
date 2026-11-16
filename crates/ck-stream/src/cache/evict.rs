use std::{
    collections::{HashMap, HashSet},
    ops::Bound::{Excluded, Unbounded},
    sync::Arc,
};

use super::Inner;
use crate::publication::{State, Stream};

/// A request stream and the dependents holding it, as far as an eviction walk has reached them.
#[derive(Default)]
struct Unit {
    members: Vec<u64>,
    returned: usize,
    leased: bool,
}

/// The request stream whose unit `stream` belongs to: its request, or itself while dependents
/// hold it. A dependent that is invalidated, or whose request is excluded or uncached, stands
/// alone.
fn root(stream: &Arc<Stream>, exclude: Option<u64>, cached: impl Fn(u64) -> bool) -> Option<Arc<Stream>> {
    match stream.request() {
        Some(request) if !stream.is_invalidated() && Some(request.id) != exclude && cached(request.id) => {
            Some(request.clone())
        }
        Some(_) => None,
        None => stream.is_held().then(|| Arc::clone(stream)),
    }
}

impl Inner {
    /// Invalidate the least recently read streams whose `gain` (what invalidation returns now)
    /// together covers `wanted`. Streams that would return nothing stay cached; if every
    /// candidate together cannot cover the request, nothing is invalidated and this returns false.
    /// A request stream and its dependents are one unit, chosen for what invalidating all of
    /// them returns once the walk has reached every member, the request included, so its
    /// position is that of its most recently read member. A unit with an outstanding reader
    /// lease on any member stays whole.
    pub(super) fn evict(&self, exclude: Option<u64>, wanted: usize, gain: fn(&Stream, &State) -> usize) -> bool {
        let _retention = self.retention.retain();
        let mut cache = self.cache.lock();
        let mut victims = Vec::new();
        let mut chosen = HashSet::new();
        let mut units = HashMap::<u64, Unit>::new();
        let mut found = 0;
        let mut refreshed = HashSet::new();
        let mut cursor = None;
        while found < wanted {
            let next = match cursor {
                None => cache.order.first(),
                Some(key) => cache.order.range((Excluded(key), Unbounded)).next(),
            };
            let Some(&key) = next else {
                break;
            };
            cursor = Some(key);
            if Some(key.1) == exclude || chosen.contains(&key.1) {
                continue;
            }
            let stream = cache.streams[&key.1].stream.clone();
            // A kept stream never returns frames, so neither does its unit.
            if stream.is_kept() {
                continue;
            }
            let state = stream.state.lock();
            // Queue timestamps are lower bounds. Once maintenance has observed a stream, the
            // fence holds off new leases, so one requeue at the real value is exact except for
            // an acknowledgement racing this pass, which then counts as arriving after it.
            let last_read = stream.last_read();
            if last_read != key.0 && refreshed.insert(key.1) {
                drop(state);
                cache.requeue(key, last_read);
                continue;
            }
            let own = gain(&stream, &state);
            let leased = stream.leased_frames(&state) > 0;
            drop(state);
            let root = root(&stream, exclude, |id| cache.streams.contains_key(&id));
            let (returned, members) = match root {
                Some(root) => {
                    let unit = units.entry(root.id).or_default();
                    unit.members.push(key.1);
                    unit.returned += own;
                    unit.leased |= leased;
                    if unit.members.len() <= root.holders() {
                        continue;
                    }
                    let unit = units.remove(&root.id).unwrap();
                    if unit.leased {
                        continue;
                    }
                    (unit.returned, unit.members)
                }
                None => (own, vec![key.1]),
            };
            if returned > 0 {
                found += returned;
                chosen.extend(members.iter().copied());
                victims.extend(members);
            }
        }
        if found < wanted {
            return false;
        }
        #[cfg(test)]
        if let Some(hook) = self.evict_hook.lock().take() {
            hook();
        }
        cache.evictions += victims.len() as u64;
        for id in victims {
            let stream = cache.remove(id).unwrap();
            stream.invalidate_locked(&mut stream.state.lock());
            cache.retire(&stream);
        }
        true
    }
}
