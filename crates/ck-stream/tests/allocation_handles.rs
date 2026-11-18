//! Allocation handles (`Writer::allocator`): egress and resume allocate on a stream whose
//! producer finished, and an allocation handle never publishes.

use ck_stream::{Config, Error, FRAME_BYTES, Store, Terminal};

fn store(frames: usize) -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::new(dir.path().join("pool"));
    config.pool_bytes = Some(frames * FRAME_BYTES);
    config.max_frames = Some(4);
    config.max_event_bytes = 4 * FRAME_BYTES;
    config.header_slots = 8;
    config.workers = 1;
    let store = Store::open(config).unwrap();
    (dir, store)
}

#[test]
fn an_allocator_allocates_after_the_terminal_and_never_publishes() {
    let (_dir, store) = store(8);
    let mut producer = store.create().unwrap();
    producer.append(b"record").unwrap();
    let mut allocator = producer.allocator();
    producer.finish(Terminal::Done);
    assert!(matches!(producer.begin_scratch(1), Err(Error::Closed)));
    assert!(matches!(producer.reserve_metadata(1), Err(Error::Closed)));

    let mut scratch = allocator.begin_scratch(3).unwrap();
    scratch.write(b"out").unwrap();
    let lease = scratch.complete().unwrap();
    assert_eq!(lease.chunk(0).unwrap(), b"out");
    let mut permit = allocator.reserve_metadata(8).unwrap();
    permit.resize(16).unwrap();
    assert!(matches!(allocator.append(b"x"), Err(Error::Closed)));
    assert!(matches!(allocator.begin_record(1, 0, 0), Err(Error::Closed)));

    // Invalidation still ends allocation.
    allocator.finish(Terminal::Gone);
    assert!(matches!(allocator.begin_scratch(1), Err(Error::Gone)));
}

#[test]
fn dropping_an_allocator_publishes_nothing_and_its_finish_cancels() {
    let (_dir, store) = store(8);
    let producer = store.create().unwrap();
    drop(producer.allocator());
    let stopped = producer.cancelled();
    let mut control = producer.allocator();
    control.finish(Terminal::Cancelled);
    let terminal = futures_executor(stopped);
    assert_eq!(terminal, Terminal::Cancelled);
}

/// Polls a future to completion on this thread; the cancellation future never needs a runtime.
fn futures_executor<F: std::future::Future>(future: F) -> F::Output {
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
    }
}
