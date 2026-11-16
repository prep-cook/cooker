//! One exchange quota (INTERNAL-STREAM.md §6.4): a request stream and the response streams that
//! depend on it charge frames and metadata together, a reservation is charged whole before its
//! frames are allocated, an allocation that fails after the terminal leaves the terminal, and
//! room held for a final record survives a full quota.

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
                producer.begin_record(QUOTA as usize * FRAME_BYTES, 0, 0)
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

/// With room held for a final record, records stop one frame early and a refused one leaves the
/// stream running; the final record then fits even when another exchange holds the rest of the
/// pool, and the producer picks the terminal. Without it, the refusal ends the stream. A
/// terminal frees room the stream never used.
#[test]
fn room_held_for_a_final_record_survives_a_full_quota_and_pool() {
    // (room held in bytes, final record bytes, pool frames, frames another exchange pins,
    // record bytes, records before the refusal, final record stored, terminal)
    let counters = b"counters".len();
    let two = FRAME_BYTES + 1;
    let cases = [
        (
            Some(counters),
            counters,
            16,
            0,
            FRAME_BYTES / 2,
            6,
            true,
            Terminal::Shutdown,
        ),
        (
            None,
            counters,
            16,
            0,
            FRAME_BYTES / 2,
            8,
            false,
            Terminal::StorageExhausted,
        ),
        // The other exchange pins every frame the producer does not hold.
        (
            Some(counters),
            counters,
            4,
            3,
            FRAME_BYTES / 2,
            0,
            true,
            Terminal::Shutdown,
        ),
        // The final record would fit the last frame's unpopulated tail; it still goes into the room.
        (
            Some(counters),
            counters,
            16,
            0,
            FRAME_BYTES * 2 / 3,
            4,
            true,
            Terminal::Shutdown,
        ),
        // A room over two frames holds both, also against the other exchange.
        (Some(two), two, 16, 0, FRAME_BYTES / 2, 4, true, Terminal::Shutdown),
        (Some(two), two, 4, 2, FRAME_BYTES / 2, 0, true, Terminal::Shutdown),
        // A final record shorter than its room returns the frame it does not use.
        (Some(two), counters, 16, 0, FRAME_BYTES / 2, 4, true, Terminal::Shutdown),
    ];
    for (room, last_bytes, pool, pinned, bytes, records, stored, terminal) in cases {
        let label = format!("room={room:?} final={last_bytes} pool={pool} pinned={pinned} bytes={bytes}");
        let (_dir, store) = store(pool, 4);
        let mut producer = store.create().unwrap();
        if let Some(room) = room {
            producer.reserve_final(room).unwrap();
        }
        let mut other = store.create().unwrap();
        let _pinned = (pinned > 0).then(|| other.begin_scratch(pinned * FRAME_BYTES).unwrap());
        let mut appended = 0;
        let refused = loop {
            match producer.append(&vec![1; bytes]) {
                Ok(_) => appended += 1,
                Err(error) => break error,
            }
        };
        assert!(matches!(refused, Error::StorageExhausted), "{label}: {refused:?}");
        assert_eq!(appended, records, "{label}: half-frame records before the refusal");
        producer.release_final();
        let final_record = vec![7; last_bytes];
        assert_eq!(producer.append(&final_record).is_ok(), stored, "{label}");
        producer.finish(Terminal::Shutdown);
        let mut reader = store.reader(producer.id(), 0).unwrap();
        let mut last = None;
        let ended = loop {
            match reader.try_next() {
                Poll::Ready(Some(Read::Record(lease))) => {
                    let mut bytes = Vec::new();
                    lease.visit(|chunk| bytes.extend_from_slice(chunk)).unwrap();
                    last = Some(bytes);
                }
                Poll::Ready(Some(Read::Terminal(terminal))) => break terminal,
                other => panic!("{label}: no terminal: {}", other.is_pending()),
            }
        };
        assert_eq!(last.as_deref() == Some(&final_record[..]), stored, "{label}");
        assert_eq!(ended, terminal, "{label}");
    }
    // Room a stream never used returns to the pool at its terminal, including `Gone`, which
    // invalidates the stream.
    let (_dir, store) = store(16, 4);
    for terminal in [Terminal::Shutdown, Terminal::Gone] {
        let mut producer = store.create().unwrap();
        producer.reserve_final(b"counters".len()).unwrap();
        assert_eq!(store.used_frames(), 1, "{terminal:?}: the room holds a pool frame");
        producer.finish(terminal);
        assert_eq!(store.used_frames(), 0, "{terminal:?}: the terminal frees it");
    }
    // Room a dropped producer still held returns with its stream: the stream's drop asserts
    // that its exchange quota is back to zero.
    let mut producer = store.create().unwrap();
    producer.reserve_final(b"counters".len()).unwrap();
    producer.append(&[1; FRAME_BYTES]).unwrap();
    drop(producer);
}
