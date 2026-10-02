//! A bounded multi-producer, multi-consumer queue (after Dmitry Vyukov's bounded MPMC queue).
//!
//! Every slot carries a sequence number that says whose turn it is. For the slot at ring index `i`
//! and a ticket `pos` with `pos & mask == i`:
//!
//! * `seq == pos`      the slot is empty and a producer holding ticket `pos` may fill it;
//! * `seq == pos + 1`  the slot is full and a consumer holding ticket `pos` may empty it;
//! * after a consumer empties it, `seq = pos + capacity`, the empty value for the next lap.
//!
//! Producers claim tickets by compare-and-swap on `tail`, consumers on `head`. Whoever wins a ticket
//! owns that slot alone until it publishes the new sequence number with a release store; the
//! acquire load of the sequence number on the other side is what makes the slot's contents visible.
//!
//! **ABA.** A slot's sequence number grows by one lap each time around the ring and never repeats
//! on a 64-bit target, so a thread that was delayed for exactly one lap cannot mistake the slot for
//! the one it looked at earlier. (On a 32-bit target a thread stalled for 2^32 operations could.)
//!
//! **Not strictly linearizable at the edges.** If a consumer has claimed a slot but not finished
//! reading it, a producer one lap behind sees "not yet free" and reports full, though a moment later
//! there will be room. Likewise for empty. Callers that must not lose that race retry.

use std::mem::MaybeUninit;

use crate::pad::CachePadded;
use crate::sync::{AtomicUsize, Ordering, UnsafeCell};

struct Slot<T> {
    seq: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

/// A fixed-capacity queue that any number of threads may push to and pop from.
pub struct ArrayQueue<T> {
    buf: Box<[Slot<T>]>,
    mask: usize,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
}

unsafe impl<T: Send> Send for ArrayQueue<T> {}
unsafe impl<T: Send> Sync for ArrayQueue<T> {}

impl<T> ArrayQueue<T> {
    /// Creates a queue holding at least `capacity` items (rounded up to a power of two, minimum 2).
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(2).next_power_of_two();
        let buf = (0..cap).map(|i| Slot { seq: AtomicUsize::new(i), value: UnsafeCell::new(MaybeUninit::uninit()) }).collect();
        ArrayQueue { buf, mask: cap - 1, head: CachePadded(AtomicUsize::new(0)), tail: CachePadded(AtomicUsize::new(0)) }
    }

    /// The number of items the queue can hold.
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }

    /// Appends `value`, or gives it back if the queue is full.
    pub fn push(&self, value: T) -> Result<(), T> {
        let mut pos = self.tail.load(Ordering::Relaxed);
        loop {
            let slot = &self.buf[pos & self.mask];
            // Acquire: pairs with the consumer's release store of `pos + capacity`, so its read of
            // the old value is finished before we overwrite it.
            let seq = slot.seq.load(Ordering::Acquire);
            let diff = seq.wrapping_sub(pos) as isize;
            crate::chaos::point();
            if diff == 0 {
                // The slot is free for this ticket. Claiming it needs no ordering of its own: the
                // slot's data is guarded by `seq`, not by `tail`.
                match self.tail.compare_exchange_weak(pos, pos.wrapping_add(1), Ordering::Relaxed, Ordering::Relaxed) {
                    Ok(_) => {
                        slot.value.with_mut(|p| unsafe { (*p).write(value) });
                        // Release: publishes the value to the consumer that acquires this `seq`.
                        slot.seq.store(pos.wrapping_add(1), Ordering::Release);
                        return Ok(());
                    }
                    Err(actual) => pos = actual,
                }
            } else if diff < 0 {
                // The slot still holds last lap's value: the queue is full.
                return Err(value);
            } else {
                // Another producer already took this ticket; catch up.
                pos = self.tail.load(Ordering::Relaxed);
            }
        }
    }

    /// Removes the oldest item, if any.
    pub fn pop(&self) -> Option<T> {
        let mut pos = self.head.load(Ordering::Relaxed);
        loop {
            let slot = &self.buf[pos & self.mask];
            // Acquire: pairs with the producer's release store of `pos + 1`.
            let seq = slot.seq.load(Ordering::Acquire);
            let diff = seq.wrapping_sub(pos.wrapping_add(1)) as isize;
            crate::chaos::point();
            if diff == 0 {
                match self.head.compare_exchange_weak(pos, pos.wrapping_add(1), Ordering::Relaxed, Ordering::Relaxed) {
                    Ok(_) => {
                        let v = slot.value.with(|p| unsafe { (*p).assume_init_read() });
                        // Release: the read above is finished before a producer may reuse the slot.
                        slot.seq.store(pos.wrapping_add(self.mask).wrapping_add(1), Ordering::Release);
                        return Some(v);
                    }
                    Err(actual) => pos = actual,
                }
            } else if diff < 0 {
                // Nothing has been published for this ticket yet: empty.
                return None;
            } else {
                pos = self.head.load(Ordering::Relaxed);
            }
        }
    }

    /// An estimate of the number of items; exact only while no other thread is using the queue.
    pub fn len(&self) -> usize {
        loop {
            // Read tail, then head, then tail again: if tail did not move, the pair is a consistent
            // snapshot (head can only have been read after the first tail, so it is <= the second).
            let tail = self.tail.load(Ordering::SeqCst);
            let head = self.head.load(Ordering::SeqCst);
            if self.tail.load(Ordering::SeqCst) == tail {
                return tail.wrapping_sub(head).min(self.capacity());
            }
        }
    }

    /// True when `len()` is zero.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> Drop for ArrayQueue<T> {
    fn drop(&mut self) {
        // `&mut self`: no other thread can be mid-operation, so every ticket below `tail` has been
        // published and popping until empty drops exactly the items left.
        while self.pop().is_some() {}
    }
}

impl<T> std::fmt::Debug for ArrayQueue<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrayQueue").field("capacity", &self.capacity()).field("len", &self.len()).finish()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn fifo_full_and_empty() {
        let q = ArrayQueue::new(4);
        assert_eq!(q.pop(), None::<u32>);
        for i in 0..4 {
            q.push(i).unwrap();
        }
        assert_eq!(q.push(9), Err(9));
        assert_eq!(q.len(), 4);
        for i in 0..4 {
            assert_eq!(q.pop(), Some(i));
        }
        assert_eq!((q.pop(), q.len(), q.is_empty()), (None, 0, true));
    }

    #[test]
    fn laps_keep_working() {
        let q = ArrayQueue::new(8);
        let mut next = 0usize;
        let mut expect = 0usize;
        for round in 0..5000 {
            let n = 1 + round % 8;
            for _ in 0..n {
                q.push(next).unwrap();
                next += 1;
            }
            for _ in 0..n {
                assert_eq!(q.pop(), Some(expect));
                expect += 1;
            }
        }
    }

    #[test]
    fn tickets_survive_wrapping_past_usize_max() {
        let q = ArrayQueue::new(4);
        // Position the ring a few tickets before the wrap: every slot's sequence must agree.
        let start = usize::MAX - 2;
        q.head.0.store(start, Ordering::Relaxed);
        q.tail.0.store(start, Ordering::Relaxed);
        for i in 0..4 {
            // The slot for ticket `start + i` has index (start + i) & mask.
            let idx = start.wrapping_add(i) & q.mask;
            q.buf[idx].seq.store(start.wrapping_add(i), Ordering::Relaxed);
        }
        for round in 0..20u32 {
            for i in 0..4 {
                q.push(round * 10 + i).unwrap();
            }
            assert!(q.push(0).is_err());
            for i in 0..4 {
                assert_eq!(q.pop(), Some(round * 10 + i));
            }
            assert_eq!(q.pop(), None);
        }
    }

    #[test]
    fn dropping_a_non_empty_queue_drops_each_item_once() {
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
        let q = ArrayQueue::new(16);
        for _ in 0..10 {
            q.push(Counted::new()).ok().unwrap();
        }
        drop(q.pop());
        assert_eq!(LIVE.load(SeqCst), 9);
        let back = {
            for _ in 0..7 {
                q.push(Counted::new()).ok().unwrap();
            }
            q.push(Counted::new()).err().unwrap()
        };
        assert_eq!(LIVE.load(SeqCst), 17);
        drop(back);
        drop(q);
        assert_eq!(LIVE.load(SeqCst), 0);
    }
}
