//! A request stream and its dependents are evicted as one unit: never while any member has an
//! outstanding reader lease, and only once the least-recently-read walk has reached every
//! member, so a recent read of the request protects the whole unit.

use ck_stream::{Config, Error, FRAME_BYTES, ManualClock, Read, Store, Terminal};
use std::{sync::Arc, task::Poll, time::Duration};

fn open() -> (tempfile::TempDir, Store, Arc<ManualClock>) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::new(dir.path().join("pool"));
    config.pool_bytes = Some(4 * FRAME_BYTES);
    config.max_frames = Some(4);
    config.max_event_bytes = 4 * FRAME_BYTES;
    config.header_slots = 8;
    config.workers = 1;
    let clock = Arc::new(ManualClock::default());
    let store = Store::with_clock(config, clock.clone()).unwrap();
    (dir, store, clock)
}

#[test]
fn a_leased_member_keeps_its_whole_unit() {
    let (_dir, store, clock) = open();
    let mut request = store.create().unwrap();
    request.append(&[1; 3 * FRAME_BYTES]).unwrap();
    request.finish(Terminal::Done);
    let mut read = store.create_dependent(&request).unwrap();
    read.append(&[2; FRAME_BYTES]).unwrap();
    read.finish(Terminal::Done);
    store.create_dependent(&request).unwrap().finish(Terminal::Done);
    let mut reader = store.reader(read.id(), 0).unwrap();
    let Poll::Ready(Some(Read::Record(lease))) = reader.try_next() else {
        panic!("the response record is readable");
    };
    clock.advance(Duration::from_secs(1));

    let mut other = store.create().unwrap();
    assert!(matches!(other.append(b"x"), Err(Error::StorageExhausted)));
    assert_eq!(store.stats().evictions, 0);
    assert!(lease.chunk(lease.record().start).is_ok());
    assert!(store.reader(request.id(), 0).is_ok());
}

#[test]
fn a_recently_read_request_protects_its_unit() {
    let (_dir, store, clock) = open();
    let mut request = store.create().unwrap();
    request.append(&[1; FRAME_BYTES]).unwrap();
    request.finish(Terminal::Done);
    for _ in 0..2 {
        store.create_dependent(&request).unwrap().finish(Terminal::Done);
    }
    clock.advance(Duration::from_secs(1));
    let mut unrelated = store.create().unwrap();
    unrelated.append(&[2; 3 * FRAME_BYTES]).unwrap();
    unrelated.finish(Terminal::Done);
    clock.advance(Duration::from_secs(1));
    drop(store.reader(request.id(), 0).unwrap());
    clock.advance(Duration::from_secs(1));

    let mut other = store.create().unwrap();
    other.append(b"x").unwrap();
    assert!(matches!(store.reader(unrelated.id(), 0), Err(Error::Gone)));
    assert!(store.reader(request.id(), 0).is_ok());
    assert_eq!(store.stats().evictions, 1);
}
