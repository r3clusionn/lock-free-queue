//! Loom models: small scenarios run under every thread interleaving (up to the preemption bound),
//! with loom's atomics and cells, so a missing release/acquire shows up as a data race or a wrong
//! result rather than as a rare failure on someone else's machine.
//!
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test --release --test loom
//! ```
//!
//! Set `LOOM_MAX_PREEMPTIONS` to widen the search (the default here is 3).

#![cfg(loom)]

use loom::sync::Arc;
use loom::thread;

use lfq::{spsc, ArrayQueue, MsQueue};

fn model(f: impl Fn() + Sync + Send + 'static) {
    let mut b = loom::model::Builder::new();
    if b.preemption_bound.is_none() {
        b.preemption_bound = Some(3);
    }
    b.check(f);
}

#[test]
fn spsc_values_arrive_complete_and_in_order() {
    model(|| {
        let (mut p, mut c) = spsc::channel::<Box<u32>>(2);
        let t = thread::spawn(move || {
            for i in 0..3u32 {
                let mut v = Box::new(i);
                // The queue holds two, so the third push may have to wait for the consumer.
                loop {
                    match p.push(v) {
                        Ok(()) => break,
                        Err(back) => {
                            v = back;
                            thread::yield_now();
                        }
                    }
                }
            }
        });
        let mut got = Vec::new();
        while got.len() < 3 {
            match c.pop() {
                Some(v) => got.push(*v),
                None => thread::yield_now(),
            }
        }
        t.join().unwrap();
        assert_eq!(got, [0, 1, 2]);
    });
}

#[test]
fn spsc_leftovers_are_dropped_once() {
    model(|| {
        let (mut p, c) = spsc::channel::<Arc<()>>(2);
        let token = Arc::new(());
        let t = {
            let token = token.clone();
            thread::spawn(move || {
                let _ = p.push(token.clone());
                let _ = p.push(token);
            })
        };
        drop(c);
        t.join().unwrap();
        // Both ends are gone: every pushed clone must have been dropped with the queue.
        assert_eq!(Arc::strong_count(&token), 1);
    });
}

#[test]
fn array_queue_two_producers_one_consumer() {
    model(|| {
        let q = Arc::new(ArrayQueue::new(2));
        let producers: Vec<_> = (0..2u32)
            .map(|id| {
                let q = q.clone();
                thread::spawn(move || {
                    let mut v = Box::new(id);
                    loop {
                        match q.push(v) {
                            Ok(()) => break,
                            Err(back) => {
                                v = back;
                                thread::yield_now();
                            }
                        }
                    }
                })
            })
            .collect();
        let mut got = Vec::new();
        while got.len() < 2 {
            match q.pop() {
                Some(v) => got.push(*v),
                None => thread::yield_now(),
            }
        }
        for p in producers {
            p.join().unwrap();
        }
        got.sort();
        assert_eq!(got, [0, 1]);
    });
}

#[test]
fn array_queue_one_producer_two_consumers_each_item_once() {
    model(|| {
        let q = Arc::new(ArrayQueue::new(2));
        q.push(Box::new(10u32)).unwrap();
        q.push(Box::new(20u32)).unwrap();
        let consumers: Vec<_> = (0..2)
            .map(|_| {
                let q = q.clone();
                thread::spawn(move || q.pop().map(|b| *b))
            })
            .collect();
        let mut got: Vec<u32> = consumers.into_iter().filter_map(|t| t.join().unwrap()).collect();
        // Both pops race for two items that are already there: each must get exactly one of them.
        got.sort();
        assert_eq!(got, [10, 20]);
    });
}

#[test]
fn array_queue_slot_is_not_reused_before_it_is_read() {
    model(|| {
        // Capacity 2, three items: the third push must wait until a pop has finished with a slot.
        let q = Arc::new(ArrayQueue::new(2));
        let producer = {
            let q = q.clone();
            thread::spawn(move || {
                for i in 0..3u32 {
                    let mut v = Box::new(i);
                    loop {
                        match q.push(v) {
                            Ok(()) => break,
                            Err(back) => {
                                v = back;
                                thread::yield_now();
                            }
                        }
                    }
                }
            })
        };
        let mut got = Vec::new();
        while got.len() < 3 {
            match q.pop() {
                Some(v) => got.push(*v),
                None => thread::yield_now(),
            }
        }
        producer.join().unwrap();
        assert_eq!(got, [0, 1, 2]);
    });
}

#[test]
fn ms_queue_push_then_pop_across_threads() {
    model(|| {
        let q = Arc::new(MsQueue::new());
        let t = {
            let q = q.clone();
            thread::spawn(move || {
                q.push(Box::new(1u32));
                q.push(Box::new(2u32));
            })
        };
        let mut got = Vec::new();
        while got.len() < 2 {
            match q.pop() {
                Some(v) => got.push(*v),
                None => thread::yield_now(),
            }
        }
        t.join().unwrap();
        assert_eq!(got, [1, 2]);
    });
}

#[test]
fn ms_queue_two_consumers_race_for_one_item() {
    model(|| {
        let q = Arc::new(MsQueue::new());
        q.push(Box::new(5u32));
        let a = {
            let q = q.clone();
            thread::spawn(move || q.pop().map(|b| *b))
        };
        let b = {
            let q = q.clone();
            thread::spawn(move || q.pop().map(|b| *b))
        };
        let (a, b) = (a.join().unwrap(), b.join().unwrap());
        // Exactly one wins; the loser must not drop the box it copied.
        assert!(matches!((a, b), (Some(5), None) | (None, Some(5))), "{a:?} {b:?}");
    });
}

#[test]
fn ms_queue_producer_and_consumer_race_on_an_empty_queue() {
    model(|| {
        let q = Arc::new(MsQueue::new());
        let p = {
            let q = q.clone();
            thread::spawn(move || q.push(Box::new(9u32)))
        };
        let first = q.pop().map(|b| *b);
        p.join().unwrap();
        let rest = q.pop().map(|b| *b);
        // The item is seen exactly once, either by the racing pop or by the one after the join.
        assert_eq!(first.into_iter().chain(rest).collect::<Vec<_>>(), [9]);
    });
}
