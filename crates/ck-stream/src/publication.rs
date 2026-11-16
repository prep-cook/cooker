use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use parking_lot::Mutex;

use crate::{Error, FRAME_BYTES, Limits, Position, Record, Terminal, cache::Retention, pool::Pool};

mod reader;
mod scratch;
mod wakeup;
mod writer;
use reader::Pinned;
pub use reader::{Lease, Read, Reader};
pub use scratch::{MetadataPermit, ScratchLease, ScratchReservation};
use wakeup::Wakeup;
pub use writer::{Reservation, Writer};

pub(crate) const READER_BYTES: usize = std::mem::size_of::<Reader>();
/// Table room a final record over `frames` frames takes: their frame entries and a run index
/// entry.
pub(crate) const fn final_metadata(frames: usize) -> usize {
    frames * std::mem::size_of::<Frame>() + std::mem::size_of::<Index>()
}

/// Consecutive record frames held for the producer's final record (`Writer::reserve_final`),
/// charged to the exchange quota with [`final_metadata`], and prefaulted for its first `len`
/// bytes.
pub(crate) struct FinalRoom {
    pub(crate) frames: Box<[u32]>,
    pub(crate) len: usize,
}

/// A stream's reference to a pool frame; readers' leases hold their own references.
pub(crate) struct Frame {
    pool: Option<u32>,
}

/// A run of consecutive records of one length, kind and correlation, laid end to end.
#[derive(Clone, Copy)]
pub(crate) struct Index {
    start: u64,
    sequence: u64,
    next_sequence: u64,
    len: u64,
    kind: u32,
    correlation: u32,
}

impl Index {
    fn extends(&self, next: &Self) -> bool {
        self.next_sequence == next.sequence
            && self.len == next.len
            && self.kind == next.kind
            && self.correlation == next.correlation
            && (self.next_sequence - self.sequence)
                .checked_mul(self.len)
                .and_then(|offset| self.start.checked_add(offset))
                == Some(next.start)
    }

    fn record(self, generation: u64, sequence: u64) -> Option<Record> {
        if !(self.sequence..self.next_sequence).contains(&sequence) {
            return None;
        }
        let start = (sequence - self.sequence)
            .checked_mul(self.len)?
            .checked_add(self.start)?;
        let end = start.checked_add(self.len)?;
        Some(Record {
            sequence,
            start: Position::at(generation, start),
            end: Position::at(generation, end),
            kind: self.kind,
            correlation: self.correlation,
        })
    }
}

/// The fewest entries a metadata table grows by, so small tables do not reallocate per push.
const MIN_GROWTH: usize = 4;

/// Grows `entries` to hold `needed` within `budget` bytes. A step adds a quarter of `needed`,
/// at least [`MIN_GROWTH`]: each step multiplies the capacity by at least 1.25, so pushes stay
/// amortized O(1), while the unused tail stays within a quarter of the use rather than the
/// half of a doubling step, leaving that headroom to the stream's other metadata.
///
/// With a `stream`, the growth is charged to its exchange quota before it is allocated, and the
/// charge then follows the capacity actually retained, also when the allocation fails.
fn reserve_metadata<T>(
    entries: &mut VecDeque<T>,
    needed: usize,
    budget: usize,
    stream: Option<&Stream>,
) -> Result<(), Error> {
    if needed <= entries.capacity() {
        return Ok(());
    }
    let size = std::mem::size_of::<T>();
    let maximum = budget / size;
    if needed > maximum {
        return Err(Error::StorageExhausted);
    }
    let before = entries.capacity();
    let capacity = needed.saturating_add((needed / 4).max(MIN_GROWTH)).min(maximum);
    let charged = (capacity - before) * size;
    if let Some(stream) = stream {
        stream.charge_metadata(charged)?;
    }
    let reserved = entries.try_reserve_exact(capacity - entries.len());
    if let Some(stream) = stream {
        let retained = (entries.capacity() - before) * size;
        if retained > charged {
            // The capacity exists either way; the next charge sees the ceiling exceeded.
            stream.quota().metadata.fetch_add(retained - charged, Ordering::AcqRel);
        } else {
            stream.release_metadata(charged - retained);
        }
    }
    reserved.map_err(|_| Error::StorageExhausted)
}

pub(crate) struct State {
    frames: VecDeque<Frame>,
    base_frame: u64,
    records: VecDeque<Index>,
    base_sequence: u64,
    next_sequence: u64,
    end: u64,
    terminal: Option<Terminal>,
    pub(crate) readers: usize,
    scratch_frames: usize,
    /// The scratch frame small leases pack into, and its first unleased byte. Only leases
    /// reference it; the lease drop that frees it clears this under the same lock.
    scratch_open: Option<(u32, usize)>,
    external_metadata: usize,
    // Taken at invalidation, so an evicted stream frees its header before its last handle drops.
    slot: Option<u32>,
    /// Frames charged when the stream ended with `StorageExhausted`.
    failure_frames: Option<u32>,
    /// Whether this dependent stream still holds its request (`Stream::request`); released
    /// when it is invalidated or dropped.
    holding: bool,
    /// Room for the producer's final record (`Writer::reserve_final`). While it is held, a
    /// failed charge returns `StorageExhausted` without ending the stream; any terminal frees it.
    final_room: Option<FinalRoom>,
    /// The next record is the final one and goes into `final_room` (`Writer::release_final`).
    final_next: bool,
}

impl State {
    fn empty() -> Self {
        Self {
            frames: VecDeque::new(),
            base_frame: 0,
            records: VecDeque::new(),
            base_sequence: 0,
            next_sequence: 0,
            end: 0,
            terminal: None,
            readers: 0,
            scratch_frames: 0,
            scratch_open: None,
            external_metadata: 0,
            slot: None,
            failure_frames: None,
            holding: false,
            final_room: None,
            final_next: false,
        }
    }

    /// Header slots that invalidation would return now.
    pub(crate) fn slot_count(_: &Stream, state: &Self) -> usize {
        usize::from(state.slot.is_some())
    }
}

/// Bytes the dependency link charges to the exchange's metadata quota.
const DEPENDENCY_BYTES: usize = std::mem::size_of::<Arc<Stream>>();

/// Frames and metadata bytes charged to one exchange: a request stream and the response streams
/// that depend on it (`Store::create_dependent`) share the stream ceilings (§6.4). A charge
/// checks and adds in one step, so concurrent handles never pass the ceiling together.
#[derive(Default)]
pub(crate) struct Quota {
    frames: AtomicUsize,
    metadata: AtomicUsize,
}

fn charge(used: &AtomicUsize, amount: usize, limit: usize) -> Result<(), Error> {
    if amount == 0 {
        return Ok(());
    }
    let mut current = used.load(Ordering::Acquire);
    loop {
        let total = current
            .checked_add(amount)
            .filter(|&total| total <= limit)
            .ok_or(Error::StorageExhausted)?;
        match used.compare_exchange_weak(current, total, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Ok(()),
            Err(actual) => current = actual,
        }
    }
}

#[repr(align(128))]
pub(crate) struct Stream {
    pub id: u64,
    pool: Arc<Pool>,
    retention: Arc<Retention>,
    limits: Limits,
    pub state: Mutex<State>,
    invalidated: AtomicBool,
    /// Neither evicted nor expired; `Writer::finish(Gone)` removes it (`Writer::keep_cached`).
    kept: AtomicBool,
    /// Clock milliseconds of the last read. Acknowledgements raise it without the state lock.
    last_read: AtomicU64,
    /// Frames the stream gave up while leases still pin them; they still count as live.
    detached: AtomicU64,
    notify: Wakeup,
    stopped: Wakeup,
    /// Client egress readers waiting at the published end, and how many such waits began.
    /// Only these readers ask the producer to publish what it holds early (`Writer::demand`).
    egress_waiting: AtomicUsize,
    egress_waits: AtomicU64,
    demand: tokio::sync::Notify,
    /// Dependent streams holding this one: while nonzero it neither expires nor is evicted
    /// on its own; evicting its last holders frees it with them.
    held: AtomicUsize,
    /// The request stream this response stream depends on; it charges that stream's quota.
    request: Option<Arc<Stream>>,
    quota: Quota,
}

impl Stream {
    pub(crate) fn new(
        id: u64,
        slot: u32,
        pool: Arc<Pool>,
        retention: Arc<Retention>,
        limits: Limits,
        now: Duration,
        request: Option<Arc<Stream>>,
    ) -> Self {
        Self {
            id,
            pool,
            retention,
            limits,
            notify: Wakeup::new(),
            stopped: Wakeup::new(),
            egress_waiting: AtomicUsize::new(0),
            egress_waits: AtomicU64::new(0),
            demand: tokio::sync::Notify::new(),
            invalidated: AtomicBool::new(false),
            kept: AtomicBool::new(false),
            last_read: AtomicU64::new(millis(now)),
            detached: AtomicU64::new(0),
            held: AtomicUsize::new(0),
            quota: Quota::default(),
            state: Mutex::new(State {
                slot: Some(slot),
                holding: request.is_some(),
                ..State::empty()
            }),
            request,
        }
    }

    pub(crate) fn is_invalidated(&self) -> bool {
        self.invalidated.load(Ordering::Acquire)
    }

    pub(crate) fn is_kept(&self) -> bool {
        self.kept.load(Ordering::Acquire)
    }

    /// Whether a dependent stream holds this one.
    pub(crate) fn is_held(&self) -> bool {
        self.held.load(Ordering::Acquire) > 0
    }

    pub(crate) fn holders(&self) -> usize {
        self.held.load(Ordering::Acquire)
    }

    pub(crate) fn request(&self) -> Option<&Arc<Stream>> {
        self.request.as_ref()
    }

    /// Charge the link of a new dependent to this stream's quota.
    pub(crate) fn charge_dependency(&self) -> Result<(), Error> {
        if self.request.is_some() {
            // A request never depends on a stream, so dependencies form no chain or cycle.
            return Err(Error::InvalidRecord);
        }
        self.charge_metadata(DEPENDENCY_BYTES)
    }

    /// Return the link charge of a dependent whose admission failed.
    pub(crate) fn release_dependency(&self) {
        self.release_metadata(DEPENDENCY_BYTES);
    }

    /// Link a new dependent stream: called under the cache lock, which eviction and expiry
    /// hold from planning to invalidation, and under this stream's state lock, which every
    /// invalidation takes, so a stream that is gone or leaving the cache takes no dependent.
    pub(crate) fn depend(&self, _state: &State) -> Result<(), Error> {
        if self.is_invalidated() {
            return Err(Error::Gone);
        }
        self.held.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    fn quota(&self) -> &Quota {
        self.request.as_ref().map_or(&self.quota, |request| &request.quota)
    }

    /// Charge `frames` to the exchange before allocating them.
    pub(super) fn charge_frames(&self, frames: usize) -> Result<(), Error> {
        charge(&self.quota().frames, frames, self.limits.max_frames as usize)
    }

    pub(super) fn release_frames(&self, frames: usize) {
        self.quota().frames.fetch_sub(frames, Ordering::AcqRel);
    }

    pub(super) fn charge_metadata(&self, bytes: usize) -> Result<(), Error> {
        charge(&self.quota().metadata, bytes, self.limits.max_metadata_bytes)
    }

    /// Exchange metadata bytes not yet charged.
    pub(super) fn metadata_available(&self) -> usize {
        self.limits
            .max_metadata_bytes
            .saturating_sub(self.quota().metadata.load(Ordering::Acquire))
    }

    pub(super) fn release_metadata(&self, bytes: usize) {
        self.quota().metadata.fetch_sub(bytes, Ordering::AcqRel);
    }

    /// Frames reader leases keep pinned: owned ones with another reference, and detached ones
    /// only leases still hold. Every record spans a frame, so this is zero exactly when no lease
    /// on the stream is outstanding. Counted here rather than per lease: an atomic update on
    /// every lease drop cost 5% of small-request throughput.
    pub(crate) fn leased_frames(&self, state: &State) -> u64 {
        let owned = state
            .frames
            .iter()
            .filter(|frame| frame.pool.is_some_and(|index| self.pool.references(index) > 1))
            .count();
        owned as u64 + self.detached.load(Ordering::Acquire)
    }

    /// Record that a client egress reader waits at the published end until the guard drops.
    fn egress_wait(&self) -> EgressWait<'_> {
        self.egress_waiting.fetch_add(1, Ordering::SeqCst);
        self.egress_waits.fetch_add(1, Ordering::SeqCst);
        self.demand.notify_waiters();
        EgressWait(&self.egress_waiting)
    }

    pub(crate) fn last_read(&self) -> Duration {
        Duration::from_millis(self.last_read.load(Ordering::Relaxed))
    }

    /// Raise the last-read time. An unlocked acknowledgement may store a sample older than a
    /// read that finished meanwhile, so the time never moves backwards. The time is kept in
    /// whole milliseconds, so reads within one millisecond skip the locked update.
    pub(crate) fn touch(&self, now: Duration) {
        let now = millis(now);
        if now > self.last_read.load(Ordering::Relaxed) {
            self.last_read.fetch_max(now, Ordering::Relaxed);
        }
    }

    /// Frames that invalidation would return to the pool now: owned and unleased.
    pub(crate) fn reclaimable(&self, state: &State) -> usize {
        state
            .frames
            .iter()
            .filter(|frame| frame.pool.is_some_and(|index| self.pool.references(index) == 1))
            .count()
    }

    /// Frames this stream charges to the exchange quota: owned, detached but still leased,
    /// and scratch.
    pub(super) fn live_frames(&self, state: &State) -> usize {
        state.frames.iter().filter(|frame| frame.pool.is_some()).count()
            + self.detached.load(Ordering::Acquire) as usize
            + state.scratch_frames
    }

    /// Reference every frame of a published record for a new lease, or name the terminal
    /// that ends the reader instead.
    fn pin(&self, state: &mut State, record: Record) -> Result<Pinned, Terminal> {
        let first = (record.start.frame - state.base_frame) as usize;
        let last = (last_frame(record) - state.base_frame) as usize;
        let frames = (first..=last).map(|index| state.frames[index].pool);
        for frame in frames.clone() {
            match frame {
                None => return Err(Terminal::Gone),
                Some(index) if self.pool.saturated(index) => return Err(Terminal::StorageExhausted),
                Some(_) => {}
            }
        }
        // The stream's own references keep every frame allocated while these are added.
        let (low, high) = (state.frames[first].pool, state.frames[last].pool);
        if let (Some(low), Some(high)) = (low, high)
            && low.abs_diff(high) as usize == last - first
            && frames
                .clone()
                .flatten()
                .zip(Pinned::Run { first: low, last: high }.iter())
                .all(|(a, b)| a == b)
        {
            frames.flatten().for_each(|index| self.pool.retain(index));
            return Ok(Pinned::Run { first: low, last: high });
        }
        // A scattered list outlives its reader, so it is charged until the lease frees it.
        let bytes = (last - first + 1) * std::mem::size_of::<u32>();
        if self.charge_metadata(bytes).is_err() {
            return Err(Terminal::StorageExhausted);
        }
        let frames: Box<[u32]> = frames.flatten().collect();
        frames.iter().for_each(|&index| self.pool.retain(index));
        state.external_metadata += bytes;
        Ok(Pinned::Many(frames))
    }

    /// Give up the stream's reference to a frame; leases keep it pinned until they end.
    fn detach(&self, frame: &mut Frame) {
        if let Some(index) = frame.pool.take() {
            // Count before releasing, so a racing last lease never decrements below zero.
            self.detached.fetch_add(1, Ordering::AcqRel);
            if self.pool.release(index) {
                self.detached.fetch_sub(1, Ordering::AcqRel);
                self.release_frames(1);
            }
        }
    }

    /// Metadata bytes this stream charges to the exchange quota.
    fn metadata(&self, state: &State) -> usize {
        Self::tables(state)
            + state.readers * READER_BYTES
            + state.external_metadata
            + if self.request.is_some() { DEPENDENCY_BYTES } else { 0 }
    }

    fn tables(state: &State) -> usize {
        state.frames.capacity() * std::mem::size_of::<Frame>() + state.records.capacity() * std::mem::size_of::<Index>()
    }

    fn write_reserved(&self, state: &State, start: Position, mut bytes: &[u8]) -> Result<(), Error> {
        let mut frame_index = (start.frame - state.base_frame) as usize;
        let mut offset = start.offset as usize;
        while !bytes.is_empty() {
            let len = bytes.len().min(FRAME_BYTES - offset);
            let frame = state.frames[frame_index].pool.ok_or(Error::Gone)?;
            // SAFETY: the sole Writer reserved this frame; the locked state prevents invalidation,
            // and this span is strictly after the immutable published end and within the frame.
            unsafe {
                self.pool.write(frame, offset, &bytes[..len]);
            }
            frame_index += 1;
            offset = 0;
            bytes = &bytes[len..];
        }
        Ok(())
    }

    fn publish(&self, state: &mut State, index: Index) {
        if let Some(last) = state.records.back_mut().filter(|last| last.extends(&index)) {
            last.next_sequence = index.next_sequence;
        } else {
            state.records.push_back(index);
        }
        state.next_sequence = index.next_sequence;
        state.end = index.start + index.len;
        // Callers publish only after checking invalidation under this lock, so the slot is held.
        if let Some(slot) = state.slot {
            self.pool.header(slot).committed.store(state.end, Ordering::Release);
        }
    }

    pub(crate) fn finish(&self, terminal: Terminal) {
        if terminal == Terminal::Gone {
            self.invalidate();
            return;
        }
        let mut state = self.state.lock();
        // Waiters registered after the first terminal see it when they recheck.
        if state.terminal.is_some() {
            return;
        }
        state.terminal = Some(terminal);
        self.free_final(&mut state);
        if terminal == Terminal::StorageExhausted {
            // Every failing path rolls back its partial reservation before it gets here, so this
            // is what the stream still holds, by the formula its frame limit uses. Frame counts
            // are bounded by the pool's u32 capacity.
            let frames = self.live_frames(&state) as u32;
            state.failure_frames = Some(frames);
            self.retention.failures.record(Some(frames));
        }
        drop(state);
        self.notify.wake();
        self.stopped.wake();
    }

    /// A charge failed: the stream ends with `StorageExhausted`, unless its producer holds
    /// room for a final record and ends it itself.
    pub(crate) fn exhausted(&self) {
        if self.state.lock().final_room.is_none() {
            self.finish(Terminal::StorageExhausted);
        }
    }

    /// Returns the final room's frames to the pool and their charges to the exchange quota.
    pub(crate) fn free_final(&self, state: &mut State) {
        state.final_next = false;
        if let Some(room) = state.final_room.take() {
            for &frame in &room.frames {
                self.pool.release(frame);
            }
            self.release_frames(room.frames.len());
            self.release_metadata(final_metadata(room.frames.len()));
        }
    }

    pub(crate) fn invalidate(&self) {
        let (mut state, _fence) = self.retention.lock_stable(self);
        self.invalidate_locked(&mut state);
    }

    pub(crate) fn invalidate_locked(&self, state: &mut State) {
        self.invalidated.store(true, Ordering::Release);
        state.terminal = Some(Terminal::Gone);
        self.free_final(state);
        for frame in &mut state.frames {
            self.detach(frame);
        }
        state.records.clear();
        if let Some(slot) = state.slot.take() {
            self.pool.release_slot(slot);
        }
        self.release_request(state);
        self.notify.wake();
        self.stopped.wake();
    }

    fn release_request(&self, state: &mut State) {
        if std::mem::take(&mut state.holding)
            && let Some(request) = &self.request
        {
            request.held.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        // Readers, leases and permits hold the stream, so only owned frames and tables remain.
        let mut state = std::mem::replace(self.state.get_mut(), State::empty());
        let frames = self.live_frames(&state);
        let metadata = self.metadata(&state);
        for frame in &mut state.frames {
            if let Some(index) = frame.pool.take() {
                self.pool.release(index);
            }
        }
        if let Some(slot) = state.slot.take() {
            self.pool.release_slot(slot);
        }
        self.release_request(&mut state);
        self.release_frames(frames);
        self.release_metadata(metadata);
        if self.request.is_none() {
            // Dependents hold their request, so they released their charges first.
            debug_assert_eq!(self.quota.frames.load(Ordering::Acquire), 0, "exchange frames leaked");
            debug_assert_eq!(
                self.quota.metadata.load(Ordering::Acquire),
                0,
                "exchange metadata leaked"
            );
        }
    }
}

struct EgressWait<'a>(&'a AtomicUsize);

impl Drop for EgressWait<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn last_frame(record: Record) -> u64 {
    if record.end.offset == 0 {
        record.end.frame - 1
    } else {
        record.end.frame
    }
}

fn millis(time: Duration) -> u64 {
    u64::try_from(time.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pushes `count` entries one by one and returns every capacity a growth step chose.
    fn steps(count: usize, budget: usize) -> Vec<usize> {
        let mut entries = VecDeque::<u64>::new();
        let mut capacities = Vec::new();
        for value in 0..count as u64 {
            let before = entries.capacity();
            let needed = entries.len() + 1;
            reserve_metadata(&mut entries, needed, budget, None).unwrap();
            if entries.capacity() != before {
                capacities.push(entries.capacity());
            }
            entries.push_back(value);
        }
        capacities
    }

    #[test]
    fn metadata_tables_grow_by_a_quarter_with_a_floor() {
        let capacities = steps(100_000, usize::MAX);
        // Small tables take the floor rather than one entry per push.
        assert_eq!(capacities[..3], [5, 10, 15]);
        for pair in capacities.windows(2) {
            let (before, after) = (pair[0], pair[1]);
            // `needed` is `before + 1`: a quarter of it, never a doubling.
            assert_eq!(after, before + 1 + ((before + 1) / 4).max(MIN_GROWTH));
        }
        // Geometric growth: about log(100,000) / log(1.25) steps, so pushes stay O(1) amortized.
        assert!(capacities.len() <= 50, "{} steps", capacities.len());
    }

    #[test]
    fn metadata_tables_stop_at_the_budget() {
        let budget = 100 * std::mem::size_of::<u64>();
        assert_eq!(*steps(100, budget).last().unwrap(), 100);
        let mut entries = VecDeque::<u64>::new();
        assert!(matches!(
            reserve_metadata(&mut entries, 101, budget, None),
            Err(Error::StorageExhausted)
        ));
        assert_eq!(entries.capacity(), 0);
    }
}
