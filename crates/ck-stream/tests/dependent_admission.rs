//! Eviction takes a request stream and all its dependents as one unit, a dependent's admission
//! charges its link before it may evict another stream for a header slot, and a request from
//! another store takes no dependent.

use ck_stream::{Config, Error, FRAME_BYTES, ManualClock, Store, Terminal};
use std::{sync::Arc, time::Duration};

const METADATA: usize = 64 * 1024;

fn open(frames: usize, slots: u32) -> (tempfile::TempDir, Store, Arc<ManualClock>) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::new(dir.path().join("pool"));
    config.pool_bytes = Some(frames * FRAME_BYTES);
    config.max_frames = Some(4);
    config.max_event_bytes = 4 * FRAME_BYTES;
    config.max_metadata_bytes = METADATA;
    config.header_slots = slots;
    config.workers = 1;
    let clock = Arc::new(ManualClock::default());
    let store = Store::with_clock(config, clock.clone()).unwrap();
    (dir, store, clock)
}

#[test]
fn dependents_that_free_nothing_do_not_pin_their_request() {
    let (_dir, store, clock) = open(4, 8);
    let mut request = store.create().unwrap();
    request.append(&[1; 4 * FRAME_BYTES]).unwrap();
    request.finish(Terminal::Done);
    for _ in 0..2 {
        store.create_dependent(&request).unwrap().finish(Terminal::Done);
    }
    clock.advance(Duration::from_secs(1));

    let mut other = store.create().unwrap();
    other.append(b"x").unwrap();
    assert!(matches!(store.reader(request.id(), 0), Err(Error::Gone)));
    assert_eq!(store.stats().evictions, 3);
}

#[test]
fn a_dependent_that_cannot_be_charged_evicts_nothing() {
    let (_dir, store, clock) = open(8, 2);
    let request = store.create().unwrap();
    let _full = request.allocator().reserve_metadata(METADATA).unwrap();
    clock.advance(Duration::from_secs(1));
    let mut unrelated = store.create().unwrap();
    unrelated.append(b"unrelated").unwrap();
    unrelated.finish(Terminal::Done);
    clock.advance(Duration::from_secs(1));

    assert!(matches!(store.create_dependent(&request), Err(Error::StorageExhausted)));
    assert_eq!(store.stats().evictions, 0);
    assert!(store.reader(unrelated.id(), 0).is_ok());
}

#[test]
fn a_request_from_another_store_takes_no_dependent() {
    let (_dir, store, _clock) = open(8, 8);
    let (_other_dir, other, _other_clock) = open(8, 8);
    let request = other.create().unwrap();
    assert!(matches!(store.create_dependent(&request), Err(Error::InvalidRecord)));
    assert_eq!(store.stats().cached_streams, 0);
}
