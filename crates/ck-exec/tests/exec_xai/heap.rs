//! Replay's heap: the bytes it allocates beyond delivery stay within one entry, and a turn
//! it cannot keep allocates none.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::time::Duration;

use super::replay::{ENC, TURN};
use super::*;

/// An allocation this large is item-sized; smaller ones are bookkeeping.
const LARGE: usize = 64 * 1024;
/// The most one replay entry keeps (`replay::MAX_BYTES_PER_ENTRY`).
const ENTRY: usize = 8 << 20;
/// Body bytes the upstream writes and flushes per chunk. The line buffer that joins a
/// 9.6 MB event grows with the sizes of the reads that feed it and each growth counts its
/// new size, so the total of one run moves by tens of megabytes with how the body is cut
/// (measured: 86 MB to 126 MB for the same turn). The two runs compared below must see
/// the same cut, so the upstream fixes it instead of leaving it to one big write.
const CHUNK: usize = 256 << 10;

thread_local! {
    static BYTES: Cell<usize> = const { Cell::new(0) };
}

/// Counts the bytes of large allocations and reallocations on the current thread.
struct Counting;

fn count(size: usize) {
    if size >= LARGE {
        let _ = BYTES.try_with(|bytes| bytes.set(bytes.get() + size));
    }
}

// SAFETY: every call is forwarded unchanged to the system allocator.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count(layout.size());
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: every pointer came from the system allocator.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count(size);
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The large-allocation bytes of one Responses turn whose upstream sends `body`, from caller
/// `principal` (none turns replay off). The producer runs on this test's thread.
async fn allocated(principal: &str, body: &str) -> usize {
    // Room for a 9.6 MB event and what the ingress makes of it.
    let streams = Streams::with_frames(1024);
    let upstream = StreamUpstream::start([Reply::sse(body.to_owned()).throttle(CHUNK, Duration::ZERO)]).await;
    let mut exchange = exchange(&upstream);
    exchange.caller.principal = principal.into();
    let before = BYTES.with(Cell::get);
    let input = (TURN, 64, Sessions::default());
    let run = run_on(
        &streams,
        &executors(),
        &exchange,
        Client::Responses,
        Operation::Generate,
        input,
    )
    .await;
    let (outcome, _, _) = produce(&streams, run.started.unwrap()).await;
    assert!(outcome.is_ok(), "{outcome:?}");
    BYTES.with(Cell::get) - before
}

/// A completion of `count` messages of 300,000 characters, after `lead` output items.
fn long_turn(lead: &[Value], count: usize) -> String {
    let text = "x".repeat(300_000);
    let mut output = lead.to_vec();
    output.extend((0..count).map(|_| message(&text)));
    event(json!({"type":"response.completed","response":response("completed", Value::Array(output))}))
}

/// Without an item that can anchor a replay, replay writes nothing out. With one, a turn
/// that fits an entry (27 messages, about 8.1 MB) is written out once, in its normalized
/// form, and a larger one (32 messages) stops before it outgrows the entry.
#[tokio::test]
async fn replay_allocates_within_an_entry() {
    let plain = long_turn(&[], 32);
    let (on, off) = (allocated("sk-a", &plain).await, allocated("", &plain).await);
    let extra = on.saturating_sub(off);
    assert!(extra < LARGE, "no anchor: {extra} bytes");
    let reasoning = json!({"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":ENC});
    for count in [27, 32] {
        let anchored = long_turn(std::slice::from_ref(&reasoning), count);
        let (on, off) = (allocated("sk-a", &anchored).await, allocated("", &anchored).await);
        let extra = on.saturating_sub(off);
        assert!(extra <= ENTRY, "{count} anchored messages: {extra} bytes");
    }
}
