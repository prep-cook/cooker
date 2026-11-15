//! The allocator of the crate's unit tests. Per thread, so parallel tests do not disturb each
//! other, it counts every byte allocated, and the live allocations of item size of a thread
//! that turned tracking on, with their peak across tracked threads.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A tracked allocation this large is item-sized; smaller ones are bookkeeping.
const LARGE: usize = 64 * 1024;

thread_local! {
    static BYTES: Cell<usize> = const { Cell::new(0) };
    static TRACKED: Cell<bool> = const { Cell::new(false) };
}
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

/// The bytes this thread has allocated.
pub(crate) fn allocated() -> usize {
    BYTES.with(Cell::get)
}

/// Turns tracking of this thread's live large allocations on or off.
pub(crate) fn track(on: bool) {
    TRACKED.with(|tracked| tracked.set(on));
}

/// The most bytes tracked threads held live in large allocations at once.
pub(crate) fn peak() -> usize {
    PEAK.load(Ordering::SeqCst)
}

fn tracked(size: usize) -> bool {
    size >= LARGE && TRACKED.try_with(Cell::get).unwrap_or(false)
}

fn add(size: usize) {
    let _ = BYTES.try_with(|bytes| bytes.set(bytes.get() + size));
    if tracked(size) {
        let live = LIVE.fetch_add(size, Ordering::SeqCst) + size;
        PEAK.fetch_max(live, Ordering::SeqCst);
    }
}

fn sub(size: usize) {
    if tracked(size) {
        let _ = LIVE.try_update(Ordering::SeqCst, Ordering::SeqCst, |live| {
            Some(live.saturating_sub(size))
        });
    }
}

struct Counting;

// SAFETY: forwards to the system allocator unchanged; the counters are const thread-locals
// and atomics, which never allocate.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded unchanged to the system allocator.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            add(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        sub(layout.size());
        // SAFETY: every pointer came from the system allocator.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: forwarded unchanged to the system allocator.
        let new = unsafe { System.realloc(ptr, layout, size) };
        if !new.is_null() {
            sub(layout.size());
            add(size);
        }
        new
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;
