//! A bounded single-producer, single-consumer ring buffer.
//!
//! One thread owns the [`Producer`], one owns the [`Consumer`]. Each side writes only its own index
//! and reads the other's, so no compare-and-swap is needed, only a release store to publish and an
//! acquire load to observe:
//!
//! * The producer writes the slot, then `tail.store(t + 1, Release)`. The consumer's
//!   `tail.load(Acquire)` that sees `t + 1` therefore sees the slot's contents.
//! * The consumer reads the slot, then `head.store(h + 1, Release)`. The producer's
//!   `head.load(Acquire)` that sees `h + 1` therefore knows the slot may be overwritten.
//!
//! Both sides also keep a cached copy of the other side's index and reload it only when the cache
//! says full or empty, so on a busy queue most operations touch no shared cache line but their own.

use std::mem::MaybeUninit;

use crate::pad::CachePadded;
use crate::sync::{Arc, AtomicUsize, Ordering, UnsafeCell};

struct Inner<T> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,
    /// Next slot to read. Written only by the consumer.
    head: CachePadded<AtomicUsize>,
    /// Next slot to write. Written only by the producer.
    tail: CachePadded<AtomicUsize>,
}

// The slots are handed from the producer to the consumer by the release/acquire pairs above, and a
// slot is only ever touched by one side at a time.
unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        // Both ends are gone, so this is the only reference; load without synchronisation.
        let (mut h, t) = (self.head.load(Ordering::Relaxed), self.tail.load(Ordering::Relaxed));
        while h != t {
            self.buf[h & self.mask].with_mut(|p| unsafe { (*p).assume_init_drop() });
            h = h.wrapping_add(1);
        }
    }
}

/// The sending half. Not `Clone`: there is exactly one producer.
pub struct Producer<T> {
    inner: Arc<Inner<T>>,
    tail: usize,
    cached_head: usize,
}

/// The receiving half. Not `Clone`: there is exactly one consumer.
pub struct Consumer<T> {
    inner: Arc<Inner<T>>,
    head: usize,
    cached_tail: usize,
}

/// Creates a queue holding at least `capacity` items (rounded up to a power of two, minimum 2).
pub fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let cap = capacity.max(2).next_power_of_two();
    let buf: Box<[_]> = (0..cap).map(|_| UnsafeCell::new(MaybeUninit::uninit())).collect();
    let inner =
        Arc::new(Inner { buf, mask: cap - 1, head: CachePadded(AtomicUsize::new(0)), tail: CachePadded(AtomicUsize::new(0)) });
    (Producer { inner: inner.clone(), tail: 0, cached_head: 0 }, Consumer { inner, head: 0, cached_tail: 0 })
}

impl<T> Producer<T> {
    /// Appends `value`, or gives it back if the queue is full.
    pub fn push(&mut self, value: T) -> Result<(), T> {
        let cap = self.inner.mask + 1;
        if self.tail.wrapping_sub(self.cached_head) == cap {
            // Looks full; look at the real head. Acquire pairs with the consumer's release store, so
            // everything it did with the freed slot happens before our write below.
            self.cached_head = self.inner.head.load(Ordering::Acquire);
            if self.tail.wrapping_sub(self.cached_head) == cap {
                return Err(value);
            }
        }
        self.inner.buf[self.tail & self.inner.mask].with_mut(|p| unsafe { (*p).write(value) });
        self.tail = self.tail.wrapping_add(1);
        self.inner.tail.store(self.tail, Ordering::Release);
        Ok(())
    }

    /// The number of items the queue can hold.
    pub fn capacity(&self) -> usize {
        self.inner.mask + 1
    }

    /// Free slots right now. The consumer may free more at any moment, never fewer.
    pub fn free(&self) -> usize {
        let head = self.inner.head.load(Ordering::Acquire);
        self.capacity() - self.tail.wrapping_sub(head)
    }
}

impl<T> Consumer<T> {
    /// Removes the oldest item, if any.
    pub fn pop(&mut self) -> Option<T> {
        if self.head == self.cached_tail {
            // Looks empty; look at the real tail. Acquire pairs with the producer's release store.
            self.cached_tail = self.inner.tail.load(Ordering::Acquire);
            if self.head == self.cached_tail {
                return None;
            }
        }
        let v = self.inner.buf[self.head & self.inner.mask].with(|p| unsafe { (*p).assume_init_read() });
        self.head = self.head.wrapping_add(1);
        self.inner.head.store(self.head, Ordering::Release);
        Some(v)
    }

    /// The number of items the queue can hold.
    pub fn capacity(&self) -> usize {
        self.inner.mask + 1
    }

    /// Items waiting right now. The producer may add more at any moment, never fewer.
    pub fn len(&self) -> usize {
        self.inner.tail.load(Ordering::Acquire).wrapping_sub(self.head)
    }

    /// True when nothing is waiting right now.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn fifo_order_and_full_empty() {
        let (mut p, mut c) = channel::<u32>(4);
        assert_eq!(p.capacity(), 4);
        assert_eq!(c.pop(), None);
        for i in 0..4 {
            p.push(i).unwrap();
        }
        assert_eq!(p.push(99), Err(99));
        assert_eq!((p.free(), c.len()), (0, 4));
        for i in 0..4 {
            assert_eq!(c.pop(), Some(i));
        }
        assert_eq!(c.pop(), None);
        assert!(c.is_empty());
    }

    #[test]
    fn capacity_rounds_up_to_a_power_of_two() {
        for (ask, got) in [(0, 2), (1, 2), (2, 2), (3, 4), (5, 8), (100, 128), (1024, 1024)] {
            assert_eq!(channel::<u8>(ask).0.capacity(), got, "asked for {ask}");
        }
    }

    #[test]
    fn wraps_around_many_times() {
        let (mut p, mut c) = channel::<usize>(8);
        let (mut sent, mut got) = (0, 0);
        for round in 0..1000 {
            let n = 1 + round % 8;
            for _ in 0..n {
                p.push(sent).unwrap();
                sent += 1;
            }
            for _ in 0..n {
                assert_eq!(c.pop(), Some(got));
                got += 1;
            }
        }
        assert_eq!(c.pop(), None);
    }

    #[test]
    fn the_indices_survive_wrapping_past_usize_max() {
        let (mut p, mut c) = channel::<u32>(4);
        // Start 3 items before the wrap by moving the indices directly.
        let start = usize::MAX - 2;
        p.tail = start;
        p.cached_head = start;
        p.inner.head.0.store(start, Ordering::Relaxed);
        p.inner.tail.0.store(start, Ordering::Relaxed);
        c.head = start;
        c.cached_tail = start;
        for i in 0..4 {
            p.push(i).unwrap();
        }
        assert_eq!(p.push(4), Err(4));
        assert_eq!(c.len(), 4);
        for i in 0..4 {
            assert_eq!(c.pop(), Some(i));
        }
        assert_eq!(c.pop(), None);
        p.push(7).unwrap();
        assert_eq!(c.pop(), Some(7));
    }

    #[test]
    fn items_left_in_the_queue_are_dropped_exactly_once() {
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
        let (mut p, mut c) = channel::<Counted>(8);
        for _ in 0..6 {
            p.push(Counted::new()).ok().unwrap();
        }
        drop(c.pop());
        drop(c.pop());
        assert_eq!(LIVE.load(SeqCst), 4);
        // A rejected push hands the value back instead of dropping or leaking it.
        for _ in 0..4 {
            p.push(Counted::new()).ok().unwrap();
        }
        let back = p.push(Counted::new()).err().unwrap();
        assert_eq!(LIVE.load(SeqCst), 9);
        drop(back);
        drop(p);
        drop(c);
        assert_eq!(LIVE.load(SeqCst), 0);
    }

    #[test]
    fn zero_sized_items_work() {
        let (mut p, mut c) = channel::<()>(4);
        for _ in 0..4 {
            p.push(()).unwrap();
        }
        assert!(p.push(()).is_err());
        assert_eq!(c.pop(), Some(()));
    }

    #[test]
    fn a_million_items_cross_threads_in_order() {
        let (mut p, mut c) = channel::<u64>(64);
        let t = std::thread::spawn(move || {
            for i in 0..1_000_000u64 {
                let mut v = i;
                while let Err(back) = p.push(v) {
                    v = back;
                    std::hint::spin_loop();
                }
            }
        });
        let mut expect = 0u64;
        while expect < 1_000_000 {
            if let Some(v) = c.pop() {
                assert_eq!(v, expect);
                expect += 1;
            } else {
                std::hint::spin_loop();
            }
        }
        t.join().unwrap();
        assert_eq!(c.pop(), None);
    }
}
