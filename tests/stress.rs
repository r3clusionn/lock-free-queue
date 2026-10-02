//! Real threads, real contention, the checks in `common`.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use common::{item, retry, run_mpmc, Item};
use lfq::{spsc, ArrayQueue, MsQueue};

#[test]
fn array_queue_many_producers_many_consumers() {
    for (p, c, cap) in [(1, 1, 2), (4, 4, 8), (8, 2, 64), (2, 8, 64), (8, 8, 1024), (6, 6, 2)] {
        let q = ArrayQueue::new(cap);
        run_mpmc(p, c, 20_000, |v| retry(v, |v| q.push(v)), || q.pop());
        assert_eq!(q.pop().map(|_| ()), None);
        assert!(q.is_empty());
    }
}

#[test]
fn ms_queue_many_producers_many_consumers() {
    for (p, c) in [(1, 1), (4, 4), (8, 2), (2, 8), (8, 8), (16, 16)] {
        let q = MsQueue::new();
        run_mpmc(p, c, 20_000, |v| q.push(v), || q.pop());
        assert!(q.is_empty());
    }
}

#[test]
fn spsc_carries_boxes_in_order() {
    let (mut p, mut c) = spsc::channel::<Box<Item>>(128);
    let t = thread::spawn(move || {
        for seq in 0..300_000u32 {
            let mut v = item(0, seq);
            while let Err(back) = p.push(v) {
                v = back;
                std::hint::spin_loop();
            }
        }
    });
    for seq in 0..300_000u32 {
        loop {
            if let Some(it) = c.pop() {
                assert_eq!((it.producer, it.seq), (0, seq));
                break;
            }
            std::hint::spin_loop();
        }
    }
    t.join().unwrap();
}

/// Each thread alternates push and pop on the same queue: the head and the tail are both hot and
/// every thread is a producer and a consumer at once.
fn pairs<Q: Sync>(q: &Q, push: impl Fn(&Q, usize) + Sync, pop: impl Fn(&Q) -> Option<usize> + Sync) {
    const THREADS: usize = 8;
    const OPS: usize = 100_000;
    let pushed = AtomicUsize::new(0);
    let popped = AtomicUsize::new(0);
    let sum_in = AtomicUsize::new(0);
    let sum_out = AtomicUsize::new(0);
    thread::scope(|s| {
        for t in 0..THREADS {
            let (pushed, popped, sum_in, sum_out, push, pop) = (&pushed, &popped, &sum_in, &sum_out, &push, &pop);
            s.spawn(move || {
                for i in 0..OPS {
                    let v = t * OPS + i + 1;
                    push(q, v);
                    pushed.fetch_add(1, Ordering::Relaxed);
                    sum_in.fetch_add(v, Ordering::Relaxed);
                    if let Some(x) = pop(q) {
                        popped.fetch_add(1, Ordering::Relaxed);
                        sum_out.fetch_add(x, Ordering::Relaxed);
                    }
                }
            });
        }
    });
    while let Some(x) = pop(q) {
        popped.fetch_add(1, Ordering::Relaxed);
        sum_out.fetch_add(x, Ordering::Relaxed);
    }
    assert_eq!(pushed.load(Ordering::Relaxed), THREADS * OPS);
    assert_eq!(popped.load(Ordering::Relaxed), THREADS * OPS);
    assert_eq!(sum_in.load(Ordering::Relaxed), sum_out.load(Ordering::Relaxed));
}

#[test]
fn every_thread_both_pushes_and_pops() {
    let a = ArrayQueue::new(16);
    pairs(&a, |q, v| retry(v, |v| q.push(v)), |q| q.pop());
    let m = MsQueue::new();
    pairs(&m, |q, v| q.push(v), |q| q.pop());
}

#[test]
fn a_queue_can_be_dropped_while_other_threads_hold_the_arc() {
    // The last Arc to go does the drop; items still inside must be freed exactly once.
    let items: Vec<Arc<()>> = (0..1000).map(|_| Arc::new(())).collect();
    let q = Arc::new(MsQueue::new());
    let a = Arc::new(ArrayQueue::new(2048));
    for it in &items {
        q.push(it.clone());
        a.push(it.clone()).ok().unwrap();
    }
    let hs: Vec<_> = (0..4)
        .map(|_| {
            let (q, a) = (q.clone(), a.clone());
            thread::spawn(move || {
                for _ in 0..100 {
                    drop(q.pop());
                    drop(a.pop());
                }
            })
        })
        .collect();
    drop(q);
    drop(a);
    for h in hs {
        h.join().unwrap();
    }
    assert!(items.iter().all(|a| Arc::strong_count(a) == 1));
}
