//! Dependent streams (`Store::create_dependent`): a response stream keeps the one request stream
//! it was encoded from, the link is taken under the cache's eviction synchronisation, a request
//! can never itself depend on a stream, and evicting the response frees its request with it.

use ck_stream::{Config, Error, FRAME_BYTES, ManualClock, Store, Terminal, Writer};
use std::{sync::Arc, time::Duration};

fn store(frames: usize, slots: u32) -> (tempfile::TempDir, Store, Arc<ManualClock>) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::new(dir.path().join("pool"));
    config.pool_bytes = Some(frames * FRAME_BYTES);
    config.max_frames = Some(4);
    config.max_event_bytes = 4 * FRAME_BYTES;
    config.header_slots = slots;
    config.workers = 1;
    let clock = Arc::new(ManualClock::default());
    let store = Store::with_clock(config, clock.clone()).unwrap();
    (dir, store, clock)
}

fn dependent(store: &Store, request: &Writer) -> Result<Writer, Error> {
    store.create_dependent(request)
}

#[test]
fn evicting_a_dependent_frees_its_request() {
    let (_dir, store, clock) = store(4, 8);
    let mut request = store.create().unwrap();
    request.append(&[1; 3 * FRAME_BYTES]).unwrap();
    request.finish(Terminal::Done);
    let mut response = dependent(&store, &request).unwrap();
    response.append(&[2; FRAME_BYTES]).unwrap();
    response.finish(Terminal::Done);
    clock.advance(Duration::from_secs(1));

    // The pool is full and the request is held: only evicting both frees two frames.
    let mut other = store.create().unwrap();
    other.append(&[3; 2 * FRAME_BYTES]).unwrap();
    assert!(matches!(store.reader(response.id(), 0), Err(Error::Gone)));
    assert!(matches!(store.reader(request.id(), 0), Err(Error::Gone)));
    assert_eq!(store.stats().evictions, 2);
}

#[test]
fn a_header_slot_for_a_dependent_never_evicts_its_request() {
    let (_dir, store, clock) = store(8, 2);
    let mut request = store.create().unwrap();
    request.append(b"request").unwrap();
    clock.advance(Duration::from_secs(1));
    let _other = store.create().unwrap();
    clock.advance(Duration::from_secs(1));

    // Both slots are taken and the request is the least recently read stream.
    let _response = dependent(&store, &request).unwrap();
    assert!(store.reader(request.id(), 0).is_ok());
}

#[test]
fn a_gone_request_takes_no_dependent() {
    let (_dir, store, _clock) = store(8, 8);
    let mut request = store.create().unwrap();
    request.append(b"request").unwrap();
    let mut discard = request.allocator();
    discard.finish(Terminal::Gone);
    assert!(matches!(dependent(&store, &request), Err(Error::Gone)));
}

#[test]
fn a_dependent_cannot_be_a_dependency() {
    let (_dir, store, _clock) = store(8, 8);
    let request = store.create().unwrap();
    let response = dependent(&store, &request).unwrap();
    assert!(matches!(dependent(&store, &response), Err(Error::InvalidRecord)));
}

#[test]
fn a_request_expires_once_its_dependent_has() {
    let (_dir, store, clock) = store(8, 8);
    let mut request = store.create().unwrap();
    request.append(b"request").unwrap();
    request.finish(Terminal::Done);
    let mut response = dependent(&store, &request).unwrap();
    response.append(b"response").unwrap();
    response.finish(Terminal::Done);
    clock.advance(Duration::from_secs(1));
    let attached = store.reader(response.id(), 0).unwrap();
    clock.advance(Duration::from_secs(7200));
    store.tick().unwrap();
    // Before the check below, whose reader reads the request.
    assert_eq!(
        store.stats().oldest_read_age,
        Some(Duration::ZERO),
        "a held request counts as read"
    );
    assert!(
        store.reader(request.id(), 0).is_ok(),
        "held while its dependent is cached"
    );

    drop(attached);
    clock.advance(Duration::from_secs(7200));
    store.tick().unwrap();
    assert!(matches!(store.reader(response.id(), 0), Err(Error::Gone)));
    // The request was last read before its dependent: the next tick expires it.
    store.tick().unwrap();
    assert!(matches!(store.reader(request.id(), 0), Err(Error::Gone)));
    assert_eq!(store.stats().cached_streams, 0);
}
