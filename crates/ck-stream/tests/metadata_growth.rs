//! Canonical table growth charges the exchange's metadata quota before it allocates, and keeps
//! the charge equal to the capacity it retains when an allocation fails. A test-only global
//! allocator pauses or fails one allocation of a given size on the thread that armed it.

use ck_stream::{Config, Error, FRAME_BYTES, Store};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    sync::atomic::{AtomicBool, Ordering},
};

const METADATA: usize = 4096;
/// The dependency link a response stream charges.
const DEPENDENCY: usize = std::mem::size_of::<usize>();
/// The first growth of the frame table and of the run index: five entries each.
const FRAME_TABLE: usize = 5 * 8;
const RUN_INDEX: usize = 5 * 40;

#[derive(Clone, Copy)]
enum Fault {
    Fail(usize),
    Pause(usize),
}

thread_local! {
    static ARMED: Cell<Option<Fault>> = const { Cell::new(None) };
}
static PAUSED: AtomicBool = AtomicBool::new(false);
static RESUME: AtomicBool = AtomicBool::new(false);

struct Faulty;

unsafe impl GlobalAlloc for Faulty {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let armed = ARMED.try_with(Cell::get).ok().flatten();
        match armed {
            Some(Fault::Fail(size)) if layout.size() == size => {
                ARMED.with(|armed| armed.set(None));
                return std::ptr::null_mut();
            }
            Some(Fault::Pause(size)) if layout.size() == size => {
                ARMED.with(|armed| armed.set(None));
                PAUSED.store(true, Ordering::Release);
                while !RESUME.load(Ordering::Acquire) {
                    std::thread::yield_now();
                }
            }
            _ => {}
        }
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: every pointer came from the system allocator.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Faulty = Faulty;

fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::new(dir.path().join("pool"));
    config.pool_bytes = Some(16 * FRAME_BYTES);
    config.max_frames = Some(4);
    config.max_event_bytes = 4 * FRAME_BYTES;
    config.max_metadata_bytes = METADATA;
    config.header_slots = 8;
    config.workers = 1;
    let store = Store::open(config).unwrap();
    (dir, store)
}

/// While the response producer's run-index allocation is paused, its growth is already
/// charged, so the request's allocator cannot take the same headroom.
#[test]
fn table_growth_is_charged_before_it_allocates() {
    let (_dir, store) = store();
    let request = store.create().unwrap();
    let mut response = store.create_dependent(&request).unwrap();
    std::thread::scope(|scope| {
        let producer = scope.spawn(|| {
            ARMED.with(|armed| armed.set(Some(Fault::Pause(RUN_INDEX))));
            response.append(b"response").map(drop)
        });
        while !PAUSED.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        let mut allocator = request.allocator();
        let rest = allocator.reserve_metadata(METADATA - DEPENDENCY - FRAME_TABLE - RUN_INDEX);
        let over = allocator.reserve_metadata(1);
        RESUME.store(true, Ordering::Release);
        let appended = producer.join().unwrap();
        assert!(rest.is_ok());
        assert!(
            matches!(over, Err(Error::StorageExhausted)),
            "the paused growth was not charged"
        );
        assert!(appended.is_ok());
    });
}

/// A failed run-index allocation keeps the frame table it already grew charged, no more.
#[test]
fn a_failed_second_table_leaves_the_first_charged() {
    let (_dir, store) = store();
    let mut producer = store.create().unwrap();
    ARMED.with(|armed| armed.set(Some(Fault::Fail(RUN_INDEX))));
    assert!(matches!(producer.append(b"record"), Err(Error::StorageExhausted)));
    ARMED.with(|armed| armed.set(None));
    let mut allocator = producer.allocator();
    let rest = allocator.reserve_metadata(METADATA - FRAME_TABLE);
    assert!(rest.is_ok());
    assert!(
        matches!(allocator.reserve_metadata(1), Err(Error::StorageExhausted)),
        "the grown frame table was not charged"
    );
}
