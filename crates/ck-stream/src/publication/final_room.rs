use std::sync::atomic::Ordering;

use super::{Frame, Index, State, Stream, Writer};
use crate::{Error, FRAME_BYTES, Terminal};

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

impl Stream {
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
}

impl Writer {
    /// Holds room for one final record of at most `len` bytes: the pool frames it spans from a
    /// frame boundary, prefaulted for `len` bytes, their frames of the exchange quota and the
    /// table room of one record. While it is held, a failed charge on this stream returns
    /// `StorageExhausted` without ending the stream, and other exchanges cannot take the frames.
    /// The producer calls [`Self::release_final`] and writes its final record, which goes into
    /// those frames, then finishes the stream with the terminal it chooses. Any terminal frees
    /// an unused room.
    pub fn reserve_final(&mut self, len: usize) -> Result<(), Error> {
        self.publishes()?;
        if len == 0 || len > self.inner.limits.max_event_bytes {
            return Err(Error::InvalidRecord);
        }
        {
            let state = self.stream.state.lock();
            if state.terminal.is_some() {
                return Err(Error::Closed);
            }
            if state.final_room.is_some() {
                return Err(Error::InvalidRecord);
            }
        }
        let count = len.div_ceil(FRAME_BYTES);
        self.stream.charge_frames(count)?;
        if let Err(error) = self.stream.charge_metadata(final_metadata(count)) {
            self.stream.release_frames(count);
            return Err(error);
        }
        let give_back = |frames: &[u32]| {
            for &frame in frames {
                self.inner.pool.release(frame);
            }
            self.stream.release_frames(count);
            self.stream.release_metadata(final_metadata(count));
        };
        // Allocation may evict another stream; no stream lock is held across it.
        let mut frames = Vec::with_capacity(count);
        for taken in 0..count {
            let needed = (len - taken * FRAME_BYTES).min(FRAME_BYTES);
            match self.inner.allocate(self.id(), count - taken, needed) {
                Ok(frame) => frames.push(frame),
                Err(error) => {
                    give_back(&frames);
                    return Err(error);
                }
            }
        }
        let mut state = self.stream.state.lock();
        if self.stream.invalidated.load(Ordering::Acquire) || state.terminal.is_some() {
            drop(state);
            give_back(&frames);
            return Err(Error::Closed);
        }
        state.final_room = Some(FinalRoom {
            frames: frames.into_boxed_slice(),
            len,
        });
        Ok(())
    }

    /// The next record is the final one. Unless it fits in place at the end of the last frame,
    /// it starts at a frame boundary in the room [`Self::reserve_final`] holds, so no allocation
    /// can refuse it. A record longer than the room frees the room and is reserved like any
    /// other.
    pub fn release_final(&mut self) {
        let mut state = self.stream.state.lock();
        if state.final_room.is_some() {
            state.final_next = true;
        }
    }

    /// Take the final room for the final record, which needs `extra` new frames, and return
    /// the frames it uses first.
    pub(super) fn take_final_room(&self, state: &mut State, extra: usize) -> Box<[u32]> {
        state.final_next = false;
        let pinned = state.final_room.take().map_or_else(Box::default, |room| room.frames);
        // The room's frames are already charged; its table room returns for the tables to take.
        if !pinned.is_empty() {
            self.stream.release_metadata(final_metadata(pinned.len()));
        }
        // A final record shorter than its room leaves the room's last frames unused.
        let held = pinned.len().min(extra);
        for &frame in &pinned[held..] {
            self.inner.pool.release(frame);
        }
        self.stream.release_frames(pinned.len() - held);
        pinned[..held].into()
    }
}
