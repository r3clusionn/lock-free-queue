//! An unbounded multi-producer, multi-consumer queue: the Michael-Scott linked-list queue with
//! hazard pointers for reclamation.
//!
//! The list always starts with a dummy node. `head` points at the dummy; the oldest item is in the
//! node after it. `tail` points at the last node, or at the one before it while a push is half done.
//!
//! * **push** links a new node after the last one with a compare-and-swap on that node's `next`
//!   (the linearization point), then swings `tail` forward. Anyone who finds `tail` lagging swings
//!   it forward too, so no thread ever waits for another to finish.
//! * **pop** reads the value out of the node after the dummy, then swings `head` to that node with a
//!   compare-and-swap. The winner owns the value; the old dummy is retired.
//!
//! Nodes are freed through [`crate::hazard`], never directly, which is what makes the compare-and-swaps
//! safe from ABA: a node some thread still points at cannot be freed and so cannot be reallocated at
//! the same address.

use std::mem::MaybeUninit;

use crate::hazard::Domain;
use crate::pad::CachePadded;
use crate::sync::{AtomicPtr, Ordering, UnsafeCell};

struct Node<T> {
    next: AtomicPtr<Node<T>>,
    /// Initialised in every node except the dummy at the head.
    value: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Node<T> {
    fn alloc(value: MaybeUninit<T>) -> *mut Node<T> {
        Box::into_raw(Box::new(Node { next: AtomicPtr::new(std::ptr::null_mut()), value: UnsafeCell::new(value) }))
    }
}

/// An unbounded queue that any number of threads may push to and pop from.
pub struct MsQueue<T> {
    head: CachePadded<AtomicPtr<Node<T>>>,
    tail: CachePadded<AtomicPtr<Node<T>>>,
    domain: Domain<Node<T>>,
}

unsafe impl<T: Send> Send for MsQueue<T> {}
unsafe impl<T: Send> Sync for MsQueue<T> {}

impl<T> Default for MsQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> MsQueue<T> {
    pub fn new() -> Self {
        let dummy = Node::alloc(MaybeUninit::uninit());
        MsQueue { head: CachePadded(AtomicPtr::new(dummy)), tail: CachePadded(AtomicPtr::new(dummy)), domain: Domain::new() }
    }

    /// Appends `value`. Never fails and never waits for another thread.
    pub fn push(&self, value: T) {
        let node = Node::alloc(MaybeUninit::new(value));
        let guard = self.domain.guard();
        loop {
            // Protect the last node: we are about to read and write its `next`.
            let tail = guard.protect(0, &self.tail);
            crate::chaos::point();
            let next = unsafe { (*tail).next.load(Ordering::Acquire) };
            if next.is_null() {
                // Release: publishes the node's value (written before this point) to whoever
                // follows `next`.
                let linked =
                    unsafe { (*tail).next.compare_exchange(std::ptr::null_mut(), node, Ordering::Release, Ordering::Relaxed) };
                if linked.is_ok() {
                    // Best effort: if this fails, somebody already swung the tail past us.
                    let _ = self.tail.compare_exchange(tail, node, Ordering::Release, Ordering::Relaxed);
                    return;
                }
            } else {
                // The tail is lagging behind a push that linked its node but has not moved it yet.
                let _ = self.tail.compare_exchange(tail, next, Ordering::Release, Ordering::Relaxed);
            }
        }
    }

    /// Removes the oldest item, if any.
    pub fn pop(&self) -> Option<T> {
        let guard = self.domain.guard();
        loop {
            let head = guard.protect(0, &self.head);
            crate::chaos::point();
            let tail = self.tail.load(Ordering::Acquire);
            let next = unsafe { (*head).next.load(Ordering::Acquire) };
            crate::chaos::point();
            // Protect `next` and then check `head` has not moved: while it has not, `next` cannot
            // have been retired (that takes `head` moving past it first).
            guard.set(1, next);
            if self.head.load(Ordering::Acquire) != head {
                continue;
            }
            if next.is_null() {
                return None;
            }
            if head == tail {
                // A push linked `next` but has not moved the tail; do it for them, so the tail is
                // never left pointing at a node we are about to retire.
                let _ = self.tail.compare_exchange(tail, next, Ordering::Release, Ordering::Relaxed);
                continue;
            }
            // Copy the value out before the swing: afterwards `next` is the new dummy and may be
            // retired by another pop at any time. If we lose the race the copy is forgotten, not
            // dropped, because the winner owns the value.
            crate::chaos::point();
            let value = unsafe { (*next).value.with(|p| (*p).assume_init_read()) };
            crate::chaos::point();
            // AcqRel: acquire to see the winner before us's work, release so the next pop that
            // reads `head` sees ours.
            if self.head.compare_exchange(head, next, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                // SAFETY: `head` is unreachable now and is retired exactly once, by the winner.
                unsafe { guard.retire(head) };
                return Some(value);
            }
            std::mem::forget(value);
        }
    }

    /// True if no item was waiting at the moment of the check.
    pub fn is_empty(&self) -> bool {
        let guard = self.domain.guard();
        let head = guard.protect(0, &self.head);
        unsafe { (*head).next.load(Ordering::Acquire).is_null() }
    }
}

impl<T> Drop for MsQueue<T> {
    fn drop(&mut self) {
        // Exclusive access: walk the list, drop the items still in it, free every node. Nodes
        // already retired are freed by the domain's destructor.
        let mut cur = self.head.load(Ordering::Acquire);
        let mut is_dummy = true;
        while !cur.is_null() {
            let node = unsafe { Box::from_raw(cur) };
            if !is_dummy {
                node.value.with_mut(|p| unsafe { (*p).assume_init_drop() });
            }
            is_dummy = false;
            cur = node.next.load(Ordering::Acquire);
        }
    }
}

impl<T> std::fmt::Debug for MsQueue<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MsQueue").field("empty", &self.is_empty()).finish()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn fifo_and_empty() {
        let q = MsQueue::new();
        assert_eq!(q.pop(), None::<u32>);
        assert!(q.is_empty());
        for i in 0..100 {
            q.push(i);
        }
        assert!(!q.is_empty());
        for i in 0..100 {
            assert_eq!(q.pop(), Some(i));
        }
        assert_eq!(q.pop(), None);
        // Reusable after it has been emptied.
        q.push(7);
        assert_eq!(q.pop(), Some(7));
    }

    #[test]
    fn interleaved_push_and_pop() {
        let q = MsQueue::new();
        let (mut next, mut expect) = (0u64, 0u64);
        for round in 0..20_000u64 {
            for _ in 0..(round % 5) {
                q.push(next);
                next += 1;
            }
            for _ in 0..(round % 3) {
                if let Some(v) = q.pop() {
                    assert_eq!(v, expect);
                    expect += 1;
                }
            }
        }
        while let Some(v) = q.pop() {
            assert_eq!(v, expect);
            expect += 1;
        }
        assert_eq!(expect, next);
    }

    #[test]
    fn items_left_behind_and_retired_nodes_are_all_freed_once() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        static LIVE: AtomicUsize = AtomicUsize::new(0);
        struct Counted;
        impl Counted {
            fn new() -> Self {
                LIVE.fetch_add(1, SeqCst);
                Counted
            }
        }
        impl Drop for Counted {
            fn drop(&mut self) {
                LIVE.fetch_sub(1, SeqCst);
            }
        }
        let q = MsQueue::new();
        for _ in 0..1000 {
            q.push(Counted::new());
        }
        for _ in 0..400 {
            drop(q.pop().unwrap());
        }
        assert_eq!(LIVE.load(SeqCst), 600);
        drop(q);
        assert_eq!(LIVE.load(SeqCst), 0);
    }

    #[test]
    fn a_losing_pop_does_not_drop_the_value_twice() {
        use std::sync::Arc;
        // Many consumers race for every item; each `Arc` must end with exactly one owner.
        let q = Arc::new(MsQueue::new());
        let items: Vec<Arc<()>> = (0..2000).map(|_| Arc::new(())).collect();
        for it in &items {
            q.push(it.clone());
        }
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let q = q.clone();
                std::thread::spawn(move || {
                    let mut n = 0;
                    while let Some(v) = q.pop() {
                        drop(v);
                        n += 1;
                    }
                    n
                })
            })
            .collect();
        let total: usize = threads.into_iter().map(|t| t.join().unwrap()).sum();
        assert_eq!(total, 2000);
        assert!(items.iter().all(|a| Arc::strong_count(a) == 1));
    }
}
