use std::sync::{Arc, atomic::Ordering};

use parking_lot::MutexGuard;

use super::{Frame, Index, State, Stream, reserve_metadata};
use crate::{Error, FRAME_BYTES, Position, Record, Terminal, cache::Inner};

/// Most frames a reservation takes per release of the stream lock, which bounds one batch
/// at 256 KiB.
const TAKE_BATCH: usize = 16;

/// Exclusive stream producer. A disconnect must drop a reader, not this producer.
/// Dropping it before a terminal publishes `ProducerLost`.
///
/// [`Writer::allocator`] returns a second kind of handle on the same stream that never
/// publishes records: it allocates scratch and metadata under the exchange quota, also after
/// the terminal record, and its drop publishes nothing.
pub struct Writer {
    pub(super) inner: Arc<Inner>,
    pub(super) stream: Arc<Stream>,
    /// False for an allocation handle.
    pub(super) producer: bool,
}

impl Writer {
    pub(crate) fn new(inner: Arc<Inner>, stream: Arc<Stream>) -> Self {
        Self {
            inner,
            stream,
            producer: true,
        }
    }

    /// An allocation handle on this stream, for consumers that decode its records and encode
    /// output while the producer runs and after it ends: reading codecs need a writer of the
    /// stream's generation. It cannot begin records, append or release a prefix (`Closed`).
    /// Its `finish` publishes a terminal as the producer's would: `Cancelled` or `Shutdown`
    /// stops the producer, `Gone` discards the stream. Invalidation still ends its allocations.
    pub fn allocator(&self) -> Writer {
        Self {
            inner: self.inner.clone(),
            stream: self.stream.clone(),
            producer: false,
        }
    }

    pub(super) fn publishes(&self) -> Result<(), Error> {
        if self.producer { Ok(()) } else { Err(Error::Closed) }
    }
    pub(crate) fn stream(&self) -> &Arc<Stream> {
        &self.stream
    }

    pub(crate) fn store(&self) -> &Arc<Inner> {
        &self.inner
    }

    pub fn id(&self) -> u64 {
        self.stream.id
    }
    /// The store clock as an `Instant`, for codec flush deadlines: the monotonic time under
    /// [`Store::open`](crate::Store::open), and the injected clock's time since the store was
    /// created under [`Store::with_clock`](crate::Store::with_clock).
    pub fn now(&self) -> std::time::Instant {
        self.stream.retention.instant()
    }
    /// The published end. It stays valid after invalidation, when the header slot may be reused.
    pub fn committed(&self) -> Position {
        Position::at(self.id(), self.stream.state.lock().end)
    }

    /// Frames charged to this stream when it ended with `StorageExhausted`, for the producer's
    /// failure log; `None` for any other end or while it runs.
    pub fn storage_failure_frames(&self) -> Option<u32> {
        self.stream.state.lock().failure_frames
    }

    /// Metadata bytes charged against `max_metadata_bytes`, for the codec benchmark harness only.
    #[cfg(feature = "bench-hooks")]
    pub fn metadata_bytes(&self) -> usize {
        self.stream.metadata(&self.stream.state.lock())
    }

    /// Wait for the producer to stop, including expiry, eviction, cancellation and shutdown.
    /// The owned, cancellation-safe future does not borrow the writer and also resolves
    /// immediately after a terminal has already been published, including normal completion.
    pub fn cancelled(&self) -> impl Future<Output = Terminal> + Send + 'static {
        let stream = self.stream.clone();
        async move {
            loop {
                let notified = stream.stopped.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let _waiting = stream.stopped.waiting();
                if let Some(terminal) = stream.state.lock().terminal {
                    return terminal;
                }
                notified.await;
            }
        }
    }

    /// Keeps the stream out of eviction and expiry until it is invalidated, so its records
    /// outlive allocations by other exchanges and idle time: a live session's metadata stays
    /// readable for its accounting after the session ends. Allocations that would need its
    /// frames fail with `StorageExhausted` instead. Its owner ends it with `finish(Terminal::Gone)`
    /// once its readers are done, which removes it from the cache; until then it stays.
    pub fn keep_cached(&self) {
        self.stream.kept.store(true, Ordering::Release);
    }

    /// Client egress demand. Resolves with a new count once a client egress reader
    /// ([`Store::egress_reader`](crate::Store::egress_reader)) waits at the published end and
    /// has begun a wait since `served`, the count this producer last acted on (0 at first).
    /// Publishing nothing new leaves that reader waiting without a new wait, so a producer
    /// passing the count back is not woken again until a new wait. Owned and cancellation-safe.
    pub fn demand(&self, served: u64) -> impl Future<Output = u64> + Send + 'static {
        let stream = self.stream.clone();
        async move {
            loop {
                let notified = stream.demand.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let waits = stream.egress_waits.load(Ordering::SeqCst);
                if waits > served && stream.egress_waiting.load(Ordering::SeqCst) > 0 {
                    return waits;
                }
                notified.await;
            }
        }
    }

    /// Reserve the whole record before writing. It starts at the published end and may span
    /// frames. The caller's codec must finish its framing before committing this reservation.
    /// An invalid length is rejected without terminating the stream; storage failures are terminal.
    pub fn begin_record(&mut self, len: usize, kind: u32, correlation: u32) -> Result<Reservation<'_>, Error> {
        self.publishes()?;
        let (record, index, old_count, state) = self.reserve(len, kind, correlation)?;
        drop(state);
        Ok(Reservation {
            writer: self,
            record,
            index,
            old_count,
            written: 0,
            committed: false,
        })
    }

    fn reserve(
        &self,
        len: usize,
        kind: u32,
        correlation: u32,
    ) -> Result<(Record, Index, usize, MutexGuard<'_, State>), Error> {
        let result = self.reserve_locked(len, kind, correlation);
        if matches!(result, Err(Error::StorageExhausted | Error::Io(_))) {
            self.stream.exhausted();
        }
        result
    }

    fn reserve_locked(
        &self,
        len: usize,
        kind: u32,
        correlation: u32,
    ) -> Result<(Record, Index, usize, MutexGuard<'_, State>), Error> {
        if len == 0 || len > self.inner.limits.max_event_bytes {
            return Err(Error::InvalidRecord);
        }
        let mut state = self.stream.state.lock();
        if self.stream.invalidated.load(Ordering::Acquire) {
            return Err(Error::Gone);
        }
        if state.terminal.is_some() {
            return Err(Error::Closed);
        }
        // The final record goes into the room held for it, from a frame boundary.
        let into_room = state.final_next && state.final_room.as_ref().is_some_and(|room| len <= room.len);
        if state.final_next && !into_room {
            self.stream.free_final(&mut state);
        }
        let start = if into_room {
            state
                .end
                .div_ceil(FRAME_BYTES as u64)
                .checked_mul(FRAME_BYTES as u64)
                .ok_or(Error::StorageExhausted)?
        } else {
            state.end
        };
        let end = start.checked_add(len as u64).ok_or(Error::StorageExhausted)?;
        let needed = usize::try_from(end.div_ceil(FRAME_BYTES as u64) - state.base_frame)
            .map_err(|_| Error::StorageExhausted)?;
        let old_count = state.frames.len();
        let extra = needed.saturating_sub(old_count);
        let record = Record {
            sequence: state.next_sequence,
            start: Position::at(self.id(), start),
            end: Position::at(self.id(), end),
            kind,
            correlation,
        };
        let index = Index {
            start,
            sequence: record.sequence,
            next_sequence: record.sequence.checked_add(1).ok_or(Error::StorageExhausted)?,
            len: len as u64,
            kind,
            correlation,
        };
        let extends = state.records.back().is_some_and(|last| last.extends(&index));
        let records = state.records.len() + usize::from(!extends);
        let pinned = if into_room {
            self.take_final_room(&mut state, extra)
        } else {
            Box::default()
        };
        let held = pinned.len();
        let give_back = |frames: &[u32]| {
            for &frame in frames {
                self.inner.pool.release(frame);
            }
            self.stream.release_frames(frames.len());
        };
        // Charge every new frame before the lock is released to allocate them, so another
        // handle on the exchange cannot pass the quota meanwhile.
        if let Err(error) = self.stream.charge_frames(extra - held) {
            give_back(&pinned);
            return Err(error);
        }
        if let Err(error) = self.grow_tables(&mut state, needed, records) {
            self.stream.release_frames(extra - held);
            give_back(&pinned);
            return Err(error);
        }
        let state = self.take_frames(state, old_count, extra, start, end, &pinned)?;
        Ok((record, index, old_count, state))
    }

    /// Grow the frame table and run index to `frames` and `records` entries within the
    /// exchange's metadata ceiling.
    fn grow_tables(&self, state: &mut State, frames: usize, records: usize) -> Result<(), Error> {
        let before = Stream::tables(state);
        // What the tables may hold: what the exchange has left, plus what they hold now.
        let budget = self.stream.metadata_available() + before;
        let frame_budget = budget.saturating_sub(state.records.capacity().max(records) * std::mem::size_of::<Index>());
        reserve_metadata(&mut state.frames, frames, frame_budget, Some(&self.stream))?;
        let record_budget = budget.saturating_sub(state.frames.capacity() * std::mem::size_of::<Frame>());
        reserve_metadata(&mut state.records, records, record_budget, Some(&self.stream))
    }

    /// Back the bytes `start..end` of a reservation: prefault the pages it reaches in the
    /// last frame and append `extra` frames after the `old_count` the table held, the first
    /// of them `pinned`, the frames the final room supplies.
    fn take_frames<'a>(
        &'a self,
        mut state: MutexGuard<'a, State>,
        old_count: usize,
        extra: usize,
        start: u64,
        end: u64,
        pinned: &[u32],
    ) -> Result<MutexGuard<'a, State>, Error> {
        // A record continuing the last frame may reach pages that frame never needed yet.
        // The locked owner prefaults them; eviction cannot help, as it punches nothing.
        let first_new = (state.base_frame + old_count as u64) * FRAME_BYTES as u64;
        if start < first_new {
            let prefaulted = state
                .frames
                .back()
                .and_then(|frame| frame.pool)
                .ok_or(Error::Gone)
                .and_then(|frame| {
                    let needed = end.min(first_new) - (first_new - FRAME_BYTES as u64);
                    self.inner.pool.prefault(frame, needed as usize).map_err(Error::Io)
                });
            if let Err(error) = prefaulted {
                for &frame in pinned {
                    self.inner.pool.release(frame);
                }
                self.stream.release_frames(extra);
                return Err(error);
            }
        }
        // Allocation may evict another stream. Never enter cache ownership with metadata
        // locked: take frames in batches with it released, then install each batch at once.
        let mut pushed = 0;
        while pushed < extra {
            let mut batch = [0u32; TAKE_BATCH];
            let count = (extra - pushed).min(TAKE_BATCH);
            // The final record's room, prefaulted for the whole record when it was reserved.
            let room = pinned.len().saturating_sub(pushed).min(count);
            if room > 0 {
                batch[..room].copy_from_slice(&pinned[pushed..pushed + room]);
            }
            if count > room {
                drop(state);
                for index in room..count {
                    let taken = pushed + index;
                    let needed = (end - first_new - taken as u64 * FRAME_BYTES as u64).min(FRAME_BYTES as u64) as usize;
                    match self.inner.allocate(self.id(), extra - taken, needed) {
                        Ok(frame) => batch[index] = frame,
                        Err(error) => {
                            for &frame in &batch[..index] {
                                self.inner.pool.release(frame);
                            }
                            self.stream.release_frames(extra - pushed);
                            self.rollback(old_count);
                            return Err(error);
                        }
                    }
                }
                state = self.stream.state.lock();
                if self.stream.invalidated.load(Ordering::Acquire) || state.terminal.is_some() {
                    for &frame in &batch[..count] {
                        self.inner.pool.release(frame);
                    }
                    drop(state);
                    self.stream.release_frames(extra - pushed);
                    self.rollback(old_count);
                    return Err(Error::Gone);
                }
            }
            state
                .frames
                .extend(batch[..count].iter().map(|&frame| Frame { pool: Some(frame) }));
            pushed += count;
        }
        Ok(state)
    }

    /// Return the frames a reservation added after `old_count`. Frames invalidation detached
    /// meanwhile left the quota through the detach.
    fn rollback(&self, old_count: usize) {
        let (mut state, _fence) = self.stream.retention.lock_stable(&self.stream);
        while state.frames.len() > old_count {
            if let Some(frame) = state.frames.pop_back().unwrap().pool {
                self.inner.pool.release(frame);
                self.stream.release_frames(1);
            }
        }
    }

    pub fn append(&mut self, bytes: &[u8]) -> Result<Record, Error> {
        self.publishes()?;
        let mut state = self.stream.state.lock();
        if let Some(record) = self.append_in_frame(&mut state, bytes) {
            drop(state);
            self.stream.notify.wake();
            return Ok(record);
        }
        drop(state);
        let (record, index, old_count, mut state) = self.reserve(bytes.len(), 0, 0)?;
        if let Err(error) = self.stream.write_reserved(&state, record.start, bytes) {
            drop(state);
            self.rollback(old_count);
            return Err(error);
        }
        self.stream.publish(&mut state, index);
        drop(state);
        self.stream.notify.wake();
        Ok(record)
    }

    /// Publish a record that fits the populated part of the last frame and continues the last
    /// run, the common case for small records, without reserving. `None` leaves every other
    /// case, including each error, to the general path.
    fn append_in_frame(&self, state: &mut State, bytes: &[u8]) -> Option<Record> {
        let len = bytes.len() as u64;
        if bytes.is_empty() || bytes.len() > self.inner.limits.max_event_bytes {
            return None;
        }
        if self.stream.invalidated.load(Ordering::Acquire) || state.terminal.is_some() {
            return None;
        }
        let frame_number = (state.base_frame + state.frames.len() as u64).checked_sub(1)?;
        let frame_start = frame_number * FRAME_BYTES as u64;
        let (start, end) = (state.end, state.end.checked_add(len)?);
        if start < frame_start || end > frame_start + FRAME_BYTES as u64 {
            return None;
        }
        let frame = state.frames.back()?.pool?;
        let offset = (start - frame_start) as usize;
        if !self.inner.pool.is_ready(frame, offset + bytes.len()) {
            return None;
        }
        let sequence = state.next_sequence;
        let index = Index {
            start,
            sequence,
            next_sequence: sequence.checked_add(1)?,
            len,
            kind: 0,
            correlation: 0,
        };
        if !state.records.back().is_some_and(|last| last.extends(&index)) {
            return None;
        }
        // SAFETY: the sole Writer owns the unpublished bytes after the committed end of its
        // populated last frame; the locked state prevents invalidation.
        unsafe { self.inner.pool.write(frame, offset, bytes) };
        self.stream.publish(state, index);
        Some(Record {
            sequence,
            start: Position::at(self.id(), start),
            end: Position::at(self.id(), end),
            kind: 0,
            correlation: 0,
        })
    }

    pub fn finish(&mut self, terminal: Terminal) {
        self.stream.finish(terminal);
        if terminal == Terminal::Gone {
            self.inner.forget(&self.stream);
        }
    }

    /// Release a non-resumable canonical prefix. Response records must never use this API.
    /// Independent scratch leases release on drop. Frame and record numbers are never recycled.
    pub fn release_before(&mut self, position: Position) -> Result<(), Error> {
        self.publishes()?;
        let end = position.byte(self.id())?;
        let (mut state, _fence) = self.stream.retention.lock_stable(&self.stream);
        if end > state.end {
            return Err(Error::InvalidPosition);
        }
        while let Some(run) = state.records.front_mut() {
            let first_end = run.start + run.len;
            if end < first_end {
                break;
            }
            let removed = ((end - first_end) / run.len + 1).min(run.next_sequence - run.sequence);
            if removed == run.next_sequence - run.sequence {
                state.base_sequence = run.next_sequence;
                state.records.pop_front();
            } else {
                // Retained records were checked at append; this advances within that run.
                run.start += removed * run.len;
                run.sequence += removed;
                state.base_sequence = run.sequence;
                break;
            }
        }
        let boundary = state.records.front().map_or(position.frame, |record| {
            position.frame.min(record.start / FRAME_BYTES as u64)
        });
        let count = boundary.saturating_sub(state.base_frame) as usize;
        for frame in state.frames.iter_mut().take(count) {
            self.stream.detach(frame);
        }
        while state.frames.front().is_some_and(|frame| frame.pool.is_none()) {
            state.frames.pop_front();
            state.base_frame += 1;
        }
        Ok(())
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        // A producer that ends without a terminal is lost, never a client cancel.
        if self.producer {
            self.stream.finish(Terminal::ProducerLost);
        }
    }
}

/// An unpublished reservation; drop rolls back its newly allocated frames.
/// After a successful commit, further writes and commits return `Closed`.
pub struct Reservation<'a> {
    writer: &'a mut Writer,
    record: Record,
    index: Index,
    old_count: usize,
    written: usize,
    committed: bool,
}

impl Reservation<'_> {
    pub fn write(&mut self, bytes: &[u8]) -> Result<(), Error> {
        if self.committed {
            return Err(Error::Closed);
        }
        if bytes.len() > self.record.len() as usize - self.written {
            return Err(Error::InvalidPosition);
        }
        let stream = &self.writer.stream;
        let state = stream.state.lock();
        if stream.invalidated.load(Ordering::Acquire) {
            return Err(Error::Gone);
        }
        if state.terminal.is_some() {
            return Err(Error::Closed);
        }
        stream.write_reserved(
            &state,
            Position::at(stream.id, self.index.start + self.written as u64),
            bytes,
        )?;
        self.written += bytes.len();
        Ok(())
    }

    /// `IncompleteRecord` leaves the reservation open, so the caller can finish writing it.
    pub fn commit(&mut self) -> Result<Record, Error> {
        if self.committed {
            return Err(Error::Closed);
        }
        if self.written != self.record.len() as usize {
            return Err(Error::IncompleteRecord);
        }
        let stream = &self.writer.stream;
        let mut state = stream.state.lock();
        if stream.invalidated.load(Ordering::Acquire) {
            return Err(Error::Gone);
        }
        if state.terminal.is_some() {
            return Err(Error::Closed);
        }
        stream.publish(&mut state, self.index);
        self.committed = true;
        drop(state);
        stream.notify.wake();
        Ok(self.record)
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.writer.rollback(self.old_count);
        }
    }
}
