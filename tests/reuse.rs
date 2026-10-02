//! The same checks as `stress.rs`, under an allocator built to provoke the ABA problem.
//!
//! A freed block goes to the front of a free list and the very next allocation of that size gets it
//! back, so an address is reused as fast as it can be. If a queue ever frees a node that another
//! thread still holds a pointer to, that thread's compare-and-swap can succeed on a different node
//! at the same address, and the damage shows up as lost, duplicated or reordered items.
//!
//! Freed blocks are also overwritten with 0xDD, so a plain read after free sees garbage rather than
//! plausible old data.

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use common::{retry, run_mpmc};
use lfq::{ArrayQueue, MsQueue};

const CLASS: usize = 32;

/// Blocks of up to 32 bytes are recycled newest-first through one list; larger ones go to the system.
struct Eager {
    lock: AtomicBool,
    head: std::cell::UnsafeCell<*mut u8>,
}

unsafe impl Sync for Eager {}

static REUSED: AtomicUsize = AtomicUsize::new(0);

impl Eager {
    fn lock(&self) {
        while self.lock.compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed).is_err() {
            std::hint::spin_loop();
        }
    }

    fn unlock(&self) {
        self.lock.store(false, Ordering::Release);
    }
}

unsafe impl GlobalAlloc for Eager {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if l.size() <= CLASS && l.align() <= 16 {
            self.lock();
            let p = *self.head.get();
            if !p.is_null() {
                *self.head.get() = *(p as *mut *mut u8);
                self.unlock();
                REUSED.fetch_add(1, Ordering::Relaxed);
                return p;
            }
            self.unlock();
            return System.alloc(Layout::from_size_align_unchecked(CLASS, 16));
        }
        System.alloc(l)
    }

    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        if l.size() <= CLASS && l.align() <= 16 {
            std::ptr::write_bytes(p, 0xDD, CLASS);
            self.lock();
            *(p as *mut *mut u8) = *self.head.get();
            *self.head.get() = p;
            self.unlock();
        } else {
            System.dealloc(p, l)
        }
    }
}

#[global_allocator]
static A: Eager = Eager { lock: AtomicBool::new(false), head: std::cell::UnsafeCell::new(std::ptr::null_mut()) };

#[test]
fn ms_queue_is_safe_when_addresses_are_reused_at_once() {
    let before = REUSED.load(Ordering::Relaxed);
    for (p, c) in [(4, 4), (8, 8), (1, 8), (8, 1), (16, 16)] {
        let q = MsQueue::new();
        run_mpmc(p, c, 25_000, |v| q.push(v), || q.pop());
    }
    assert!(REUSED.load(Ordering::Relaxed) - before > 300_000, "the allocator reused too little to be a test");
}

#[test]
fn array_queue_is_safe_when_addresses_are_reused_at_once() {
    let q = ArrayQueue::new(16);
    run_mpmc(8, 8, 25_000, |v| retry(v, |v| q.push(v)), || q.pop());
}
