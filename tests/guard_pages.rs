//! The same checks as `stress.rs`, under an allocator where touching freed memory is a crash.
//!
//! Every allocation gets its own 4 KiB page from one large reserved region, never reused. Freeing a
//! block decommits its page, so any later read or write of it, by any thread, is an access
//! violation that ends the test run. A reclamation scheme that frees a node while another thread
//! can still read it is caught the first time that thread looks, even if the value it would have
//! read is thrown away by a later check (which is why `reuse.rs` and a poisoned allocator both miss
//! such reads).
//!
//! Windows only.

#![cfg(windows)]

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use common::{retry, run_mpmc};
use lfq::{ArrayQueue, MsQueue};
use windows_sys::Win32::System::Memory::{VirtualAlloc, VirtualFree, MEM_COMMIT, MEM_DECOMMIT, MEM_RESERVE, PAGE_READWRITE};

const PAGE: usize = 4096;
/// 64 GiB of address space is reserved up front (nothing is committed until used); at one page per
/// allocation that is 16 million allocations before the allocator gives up and falls back.
const REGION: usize = 64 << 30;

struct Guard {
    base: AtomicUsize,
    next: AtomicUsize,
}

static FREED: AtomicUsize = AtomicUsize::new(0);

impl Guard {
    fn region(&self) -> usize {
        let b = self.base.load(Ordering::Acquire);
        if b != 0 {
            return b;
        }
        let p = unsafe { VirtualAlloc(std::ptr::null(), REGION, MEM_RESERVE, PAGE_READWRITE) } as usize;
        assert!(p != 0, "could not reserve address space");
        match self.base.compare_exchange(0, p, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => p,
            Err(other) => {
                unsafe { VirtualFree(p as *mut _, 0, windows_sys::Win32::System::Memory::MEM_RELEASE) };
                other
            }
        }
    }

    fn in_region(&self, p: *mut u8) -> bool {
        let b = self.base.load(Ordering::Acquire);
        b != 0 && (p as usize) >= b && (p as usize) < b + REGION
    }
}

unsafe impl GlobalAlloc for Guard {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        // Only small, ordinarily aligned blocks are paged; the rest (the test harness's own buffers)
        // use the system heap.
        if l.size() > PAGE || l.align() > 64 {
            return System.alloc(l);
        }
        let base = self.region();
        let i = self.next.fetch_add(1, Ordering::Relaxed);
        if (i + 1) * PAGE > REGION {
            return System.alloc(l);
        }
        let page = (base + i * PAGE) as *mut _;
        let p = VirtualAlloc(page, PAGE, MEM_COMMIT, PAGE_READWRITE);
        assert!(!p.is_null());
        p as *mut u8
    }

    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        if self.in_region(p) {
            // The block sits at the start of its page.
            VirtualFree(p as *mut _, PAGE, MEM_DECOMMIT);
            FREED.fetch_add(1, Ordering::Relaxed);
        } else {
            System.dealloc(p, l)
        }
    }
}

#[global_allocator]
static A: Guard = Guard { base: AtomicUsize::new(0), next: AtomicUsize::new(0) };

#[test]
fn ms_queue_never_reads_a_node_after_freeing_it() {
    let before = FREED.load(Ordering::Relaxed);
    for (p, c) in [(4, 4), (8, 8), (1, 8), (8, 1)] {
        let q = MsQueue::new();
        run_mpmc(p, c, 10_000, |v| q.push(v), || q.pop());
    }
    assert!(FREED.load(Ordering::Relaxed) - before > 100_000, "too little was freed for this to be a test");
}

#[test]
fn array_queue_never_reads_an_item_after_freeing_it() {
    let q = ArrayQueue::new(16);
    run_mpmc(8, 8, 10_000, |v| retry(v, |v| q.push(v)), || q.pop());
}
