//! Chat request bounds are checked over the mapped number with bounded working memory: a long
//! spelling of a valid value is accepted without a heap copy sized by the request.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use ck_translate::codec::{
    Operation, RequestIngress,
    chat::request::{ChatCapability, Ingress},
    event::{Direction, Identity, LocalId},
};

#[path = "semantic/mod.rs"]
mod semantic;

/// Records the largest allocation while `ACTIVE`; this binary holds one test, so nothing else
/// allocates concurrently.
struct Meter;
static ACTIVE: AtomicBool = AtomicBool::new(false);
static LARGEST: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Meter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ACTIVE.load(Ordering::Relaxed) {
            LARGEST.fetch_max(layout.size(), Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if ACTIVE.load(Ordering::Relaxed) {
            LARGEST.fetch_max(size, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: Meter = Meter;

#[test]
fn long_numeric_spellings_are_bounded_without_a_request_sized_copy() {
    let digits = "0".repeat(262_144);
    for (member, literal) in [
        ("temperature", format!("0.{digits}")),
        ("top_p", format!("1.{digits}")),
        ("frequency_penalty", format!("-0.{digits}1e-3")),
    ] {
        let mut mapped = semantic::Mapped::new();
        let identity = Identity {
            request_id: LocalId::new("request").unwrap(),
            stream_id: LocalId::new("stream").unwrap(),
            attempt: 1,
            direction: Direction::Request,
        };
        let mut ingress = Ingress::new(
            &mut mapped.writer,
            &mapped.store,
            identity,
            Operation::Generate,
            1 << 20,
        )
        .unwrap();
        let body = format!(r#"{{"model":"m","messages":[{{"role":"user","content":"hi"}}],"{member}":{literal}}}"#);
        ingress.advance(&mut mapped.writer, body.as_bytes()).unwrap();
        LARGEST.store(0, Ordering::Relaxed);
        ACTIVE.store(true, Ordering::Relaxed);
        let result = ingress.finish(&mut mapped.writer, &ChatCapability).map(|_| ());
        ACTIVE.store(false, Ordering::Relaxed);
        assert!(result.is_ok(), "{member}: {result:?}");
        let published = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
        assert!(published.contains("event: x_ck.request.end\n"), "{member}");
        let largest = LARGEST.load(Ordering::Relaxed);
        assert!(largest < 64 * 1024, "{member}: largest allocation {largest} bytes");
    }
}
