use super::{Mapping, invalid};
use crate::FRAME_BYTES;
use std::io;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, Ordering};

/// Per-frame counters, one cache line per frame. Leases on different threads update the
/// counts of neighbouring frames; packed, every update moved a shared line between cores.
#[repr(align(64))]
pub(super) struct FrameState {
    /// References: the owner's plus one per reader lease. Zero means free.
    pub(super) refs: AtomicU32,
    /// Bytes from the frame's start whose pages are populated, so writing them cannot
    /// fault on tmpfs exhaustion. Only the frame's current owner changes it.
    pub(super) ready: AtomicU32,
}

/// Per-frame states in private anonymous memory, which stays unbacked until a frame is
/// first used; a heap allocation with this alignment would be written zero in full.
pub(super) struct FrameStates {
    pub(super) mapping: Mapping,
    len: usize,
}

impl FrameStates {
    /// States for `len` frames with the first `populated` marked wholly populated.
    pub(super) fn new(len: usize, populated: u32) -> io::Result<Self> {
        let bytes = len.checked_mul(std::mem::size_of::<FrameState>()).ok_or_else(invalid)?;
        // SAFETY: a fresh private anonymous mapping aliases nothing.
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                bytes,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let states = Self {
            mapping: Mapping {
                base: NonNull::new(base.cast()).ok_or_else(invalid)?,
                len: bytes,
            },
            len,
        };
        for state in &states[..populated as usize] {
            state.ready.store(FRAME_BYTES as u32, Ordering::Relaxed);
        }
        Ok(states)
    }
}

impl FrameStates {
    /// Zero the states of frames `first..=last`, which are free and cold, by dropping the
    /// whole pages that hold only them; the next touch maps a zero page. Without this, one
    /// cache line per frame of a past burst stays resident.
    pub(super) fn discard(&self, first: u32, last: u32, page: usize) -> io::Result<()> {
        let size = std::mem::size_of::<FrameState>();
        let start = (first as usize * size).next_multiple_of(page);
        let end = (last as usize + 1) * size / page * page;
        if end <= start {
            return Ok(());
        }
        // SAFETY: the page-aligned range lies inside this private anonymous mapping. Zero
        // bytes are the state of a free, cold frame, which every frame in it already holds,
        // and the decay pass calling this holds the cold list, so no other thread writes them.
        let result = unsafe {
            libc::madvise(
                self.mapping.base.as_ptr().add(start).cast(),
                end - start,
                libc::MADV_DONTNEED,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl Deref for FrameStates {
    type Target = [FrameState];
    fn deref(&self) -> &[FrameState] {
        // SAFETY: the page-aligned mapping holds `len` states for its whole life. Zero
        // bytes are valid atomics, and all access goes through them.
        unsafe { std::slice::from_raw_parts(self.mapping.base.as_ptr().cast(), self.len) }
    }
}
