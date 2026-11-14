//! One exchange quota (INTERNAL-STREAM.md §6.4): a request stream and the response streams that
//! depend on it charge frames and metadata together, a reservation is charged whole before its
//! frames are allocated, and an allocation that fails after the terminal leaves the terminal.

use ck_stream::{Config, Error, FRAME_BYTES, Read, Store, Terminal};
use std::{sync::Barrier, task::Poll};

const METADATA: usize = 64 * 1024;

fn store(frames: usize, max_frames: u32) -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::new(dir.path().join("pool"));
    config.pool_bytes = Some(frames * FRAME_BYTES);
    config.max_frames = Some(max_frames);
    config.max_event_bytes = max_frames as usize * FRAME_BYTES;
    config.max_metadata_bytes = METADATA;
    config.header_slots = 8;
    config.workers = 1;
    let store = Store::open(config).unwrap();
    (dir, store)
}

/// The producer reserves the whole quota while an allocator on the same stream reserves half of
/// it: the frames the producer has not allocated yet are already charged, so both never succeed.
#[test]
fn a_producer_and_an_allocator_never_exceed_the_frame_quota_together() {
    const QUOTA: u32 = 256;
    let (_dir, store) = store(2 * QUOTA as usize, QUOTA);
    for _ in 0..64 {
        let mut producer = store.create().unwrap();
        let mut allocator = producer.allocator();
        let start = Barrier::new(2);
        std::thread::scope(|scope| {
            // Each reservation stays charged until both threads have tried theirs.
            let record = scope.spawn(|| {
                start.wait();
                producer.begin_record(QUOTA as usize * FRAME_BYTES, false, 0, 0)
            });
            let scratch = scope.spawn(|| {
                start.wait();
                allocator.begin_scratch(QUOTA as usize / 2 * FRAME_BYTES)
            });
            let (record, scratch) = (record.join().unwrap(), scratch.join().unwrap());
            assert!(!(record.is_ok() && scratch.is_ok()), "both reservations succeeded");
        });
        drop((producer, allocator));
        assert_eq!(store.used_frames(), 0, "every frame returns after both handles drop");
    }
}

#[test]
fn a_dependent_stream_shares_the_exchange_frame_quota() {
    let (_dir, store) = store(16, 4);
    let mut request = store.create().unwrap();
    request.append(&[1; 2 * FRAME_BYTES]).unwrap();
    let mut response = store.create_dependent(&request).unwrap();
    response.append(&[2; 2 * FRAME_BYTES]).unwrap();
    // Four frames are charged to the exchange: neither stream can take a fifth.
    assert!(matches!(
        request.allocator().begin_scratch(1),
        Err(Error::StorageExhausted)
    ));
    assert!(matches!(response.append(b"x"), Err(Error::StorageExhausted)));
}

#[test]
fn the_dependency_counts_in_metadata() {
    let (_dir, store) = store(16, 4);
    let mut request = store.create().unwrap();
    let _all = request.reserve_metadata(METADATA).unwrap();
    assert!(matches!(store.create_dependent(&request), Err(Error::StorageExhausted)));
}

/// Egress and resume allocate after the producer's terminal. A failure there belongs to that
/// one delivery: the stream keeps its terminal and a later delivery may still allocate.
#[test]
fn an_allocation_failure_after_the_terminal_keeps_the_terminal() {
    let (_dir, store) = store(16, 4);
    let mut producer = store.create().unwrap();
    producer.append(b"record").unwrap();
    producer.finish(Terminal::Done);
    let mut delivery = producer.allocator();
    assert!(matches!(
        delivery.reserve_metadata(METADATA + 1),
        Err(Error::StorageExhausted)
    ));
    assert_eq!(producer.storage_failure_frames(), None);

    let mut reader = store.reader(producer.id(), 0).unwrap();
    assert!(matches!(reader.try_next(), Poll::Ready(Some(Read::Record(_)))));
    assert!(matches!(
        reader.try_next(),
        Poll::Ready(Some(Read::Terminal(Terminal::Done)))
    ));
    let mut next = producer.allocator();
    assert!(next.reserve_metadata(1024).is_ok());
    assert!(next.begin_scratch(FRAME_BYTES).is_ok());
}
