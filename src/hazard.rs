//! Hazard pointers (Maged Michael, 2004): safe memory reclamation for lock-free structures.
//!
//! A lock-free queue unlinks a node with a compare-and-swap, but other threads may still hold a
//! pointer to it, mid-read. Freeing it at once is a use-after-free; letting the allocator hand the
//! address out again is worse, because a stale compare-and-swap can then succeed on a different
//! node that happens to sit at the same address (the ABA problem).
//!
//! The rule: before dereferencing a shared pointer a thread *publishes* it in a hazard slot and then
//! checks the pointer is still current. A thread that unlinks a node puts it on a retired list and
//! frees it only when no hazard slot holds its address. Because a protected address is never freed,
//! it is never reused while anyone is relying on it, which is what rules out ABA.
//!
//! The ordering that matters is a store followed by a load on each side:
//!
//! ```text
//! reader:    publish hazard (store)   then   re-read the shared pointer (load)
//! reclaimer: unlink the node (store)  then   scan the hazard slots (load)
//! ```
//!
//! If both sides could read stale values, the reader would use a node the reclaimer frees. A
//! `SeqCst` fence between the store and the load on each side rules that out: whichever fence comes
//! first in the single total order of fences, the other side's load must see its predecessor's
//! store. The reader's fence sits in [`Guard::protect`] and [`Guard::set`], the reclaimer's at the
//! top of the scan. (On x86 a `SeqCst` store compiles to a full barrier anyway; the fences are
//! what the C++ memory model, and so Rust's, requires, and what ARM and POWER need.)
//!
//! Each [`Domain`] owns a list of records, one per concurrently active operation; a record holds two
//! hazard slots and the retired nodes that its last user did not get to free. Records are reused and
//! are only freed when the domain is dropped.

use crate::sync::{fence, AtomicBool, AtomicPtr, AtomicUsize, Ordering, UnsafeCell};

#[repr(align(128))]
struct Record<N> {
    /// Held by the one thread currently using this record.
    active: AtomicBool,
    hp: [AtomicPtr<N>; 2],
    /// Written once, before the record is published; read-only afterwards.
    next: *mut Record<N>,
    /// Touched only by the thread holding `active`, or by the domain's destructor.
    retired: UnsafeCell<Vec<*mut N>>,
}

pub(crate) struct Domain<N> {
    records: AtomicPtr<Record<N>>,
    count: AtomicUsize,
}

unsafe impl<N: Send> Send for Domain<N> {}
unsafe impl<N: Send> Sync for Domain<N> {}

/// A thread's claim on one record, for the duration of one queue operation.
pub(crate) struct Guard<'a, N> {
    domain: &'a Domain<N>,
    rec: &'a Record<N>,
}

impl<N> Domain<N> {
    pub(crate) fn new() -> Self {
        Domain { records: AtomicPtr::new(std::ptr::null_mut()), count: AtomicUsize::new(0) }
    }

    /// Claims a free record, or adds one.
    pub(crate) fn guard(&self) -> Guard<'_, N> {
        let mut cur = self.records.load(Ordering::Acquire);
        while !cur.is_null() {
            // Records are never freed while the domain lives, so the pointer stays valid.
            let rec = unsafe { &*cur };
            if !rec.active.load(Ordering::Relaxed)
                && rec.active.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok()
            {
                return Guard { domain: self, rec };
            }
            cur = rec.next;
        }
        let new = Box::into_raw(Box::new(Record {
            active: AtomicBool::new(true),
            hp: [AtomicPtr::new(std::ptr::null_mut()), AtomicPtr::new(std::ptr::null_mut())],
            next: std::ptr::null_mut(),
            retired: UnsafeCell::new(Vec::new()),
        }));
        let mut head = self.records.load(Ordering::Relaxed);
        loop {
            unsafe { (*new).next = head };
            match self.records.compare_exchange_weak(head, new, Ordering::Release, Ordering::Relaxed) {
                Ok(_) => break,
                Err(actual) => head = actual,
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        Guard { domain: self, rec: unsafe { &*new } }
    }

    /// How many retired nodes a thread collects before it scans for the ones it may free.
    fn threshold(&self) -> usize {
        (4 * self.count.load(Ordering::Relaxed)).max(16)
    }
}

impl<N> Drop for Domain<N> {
    fn drop(&mut self) {
        // No operation can be running: every guard borrows the domain.
        let mut cur = self.records.load(Ordering::Acquire);
        while !cur.is_null() {
            let rec = unsafe { Box::from_raw(cur) };
            rec.retired.with_mut(|v| {
                for p in unsafe { (*v).drain(..) } {
                    drop(unsafe { Box::from_raw(p) });
                }
            });
            cur = rec.next;
        }
    }
}

impl<N> Guard<'_, N> {
    /// Loads `src` and protects the result in hazard slot `slot` (0 or 1). The returned pointer, if
    /// non-null, will not be freed until the slot is overwritten or the guard is dropped.
    pub(crate) fn protect(&self, slot: usize, src: &AtomicPtr<N>) -> *mut N {
        let mut p = src.load(Ordering::Acquire);
        loop {
            // The gap this function exists to close: the node may be unlinked and freed between the
            // load above and the store below, which is why the pointer is re-checked afterwards.
            crate::chaos::point();
            // Store (the hazard) then load (the re-check): needs a total order with the reclaimer's
            // unlink-then-scan. The store itself can be relaxed; the fence orders it against the load.
            self.rec.hp[slot].store(p, Ordering::Relaxed);
            fence(Ordering::SeqCst);
            crate::chaos::point();
            let again = src.load(Ordering::Acquire);
            if again == p {
                return p;
            }
            p = again;
        }
    }

    /// Protects a pointer the caller read itself. The caller must then check that the pointer is
    /// still reachable, for example by re-reading the location it came from.
    pub(crate) fn set(&self, slot: usize, p: *mut N) {
        self.rec.hp[slot].store(p, Ordering::Relaxed);
        fence(Ordering::SeqCst);
    }

    /// Hands over a node that has been unlinked from the structure; it is freed once no hazard
    /// slot holds it.
    ///
    /// # Safety
    /// `p` came from `Box::into_raw`, is unreachable for any thread that has not already protected
    /// it, and is retired exactly once.
    pub(crate) unsafe fn retire(&self, p: *mut N) {
        let due = self.rec.retired.with_mut(|v| {
            let v = unsafe { &mut *v };
            v.push(p);
            v.len() >= self.domain.threshold()
        });
        if due {
            self.scan();
        }
    }

    fn scan(&self) {
        // Pairs with the fence in `protect`/`set`: the unlinks that put these nodes on the list are
        // ordered before the hazard loads below.
        fence(Ordering::SeqCst);
        let mut hazards = Vec::new();
        let mut cur = self.domain.records.load(Ordering::Acquire);
        while !cur.is_null() {
            let rec = unsafe { &*cur };
            for h in &rec.hp {
                let p = h.load(Ordering::Relaxed);
                if !p.is_null() {
                    hazards.push(p as usize);
                }
            }
            cur = rec.next;
        }
        hazards.sort_unstable();
        self.rec.retired.with_mut(|v| {
            unsafe { &mut *v }.retain(|&p| {
                if hazards.binary_search(&(p as usize)).is_ok() {
                    true
                } else {
                    drop(unsafe { Box::from_raw(p) });
                    false
                }
            });
        });
    }
}

impl<N> Drop for Guard<'_, N> {
    fn drop(&mut self) {
        self.rec.hp[0].store(std::ptr::null_mut(), Ordering::Release);
        self.rec.hp[1].store(std::ptr::null_mut(), Ordering::Release);
        // Release: the next thread to take this record sees the retired list we leave.
        self.rec.active.store(false, Ordering::Release);
    }
}
