//! A multi-producer, multi-consumer checker shared by the stress tests.
//!
//! Every item is a heap allocation carrying its producer, its sequence number within that producer
//! and a check value. The run verifies, for any queue:
//! * every item comes out exactly once (none lost, none duplicated);
//! * one consumer sees the items of one producer in the order that producer pushed them;
//! * no item's contents were corrupted on the way (the check value ties the fields together, and an
//!   item freed too early and reused or poisoned fails it or crashes the dereference).

#![allow(dead_code)]

use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::thread;

pub struct Item {
    pub producer: u32,
    pub seq: u32,
    pub check: u64,
}

pub fn item(producer: u32, seq: u32) -> Box<Item> {
    Box::new(Item { producer, seq, check: ((producer as u64) << 32 | seq as u64) ^ 0x5a5a_5a5a_a5a5_a5a5 })
}

/// Runs `producers` pushing threads and `consumers` popping threads, `per_producer` items each.
/// `push` must retry internally if the queue can be full.
pub fn run_mpmc<P, C>(producers: u32, consumers: u32, per_producer: u32, push: P, pop: C)
where
    P: Fn(Box<Item>) + Sync,
    C: Fn() -> Option<Box<Item>> + Sync,
{
    let total = producers as usize * per_producer as usize;
    let seen: Vec<AtomicU8> = (0..total).map(|_| AtomicU8::new(0)).collect();
    let taken = AtomicUsize::new(0);
    thread::scope(|s| {
        for p in 0..producers {
            let push = &push;
            s.spawn(move || {
                for seq in 0..per_producer {
                    push(item(p, seq));
                }
            });
        }
        for _ in 0..consumers {
            let (pop, seen, taken) = (&pop, &seen, &taken);
            s.spawn(move || {
                let mut last: Vec<i64> = vec![-1; producers as usize];
                while taken.load(Ordering::Relaxed) < total {
                    match pop() {
                        Some(it) => {
                            let expect = ((it.producer as u64) << 32 | it.seq as u64) ^ 0x5a5a_5a5a_a5a5_a5a5;
                            assert_eq!(it.check, expect, "item corrupted: producer {} seq {}", it.producer, it.seq);
                            assert!(it.producer < producers && it.seq < per_producer, "item out of range");
                            let l = &mut last[it.producer as usize];
                            assert!(*l < it.seq as i64, "producer {} went backwards: {} after {}", it.producer, it.seq, *l);
                            *l = it.seq as i64;
                            let idx = it.producer as usize * per_producer as usize + it.seq as usize;
                            let before = seen[idx].fetch_add(1, Ordering::Relaxed);
                            assert_eq!(before, 0, "item {}/{} delivered twice", it.producer, it.seq);
                            taken.fetch_add(1, Ordering::Relaxed);
                        }
                        None => thread::yield_now(),
                    }
                }
            });
        }
    });
    assert_eq!(taken.load(Ordering::Relaxed), total);
    assert!(seen.iter().all(|c| c.load(Ordering::Relaxed) == 1), "some item was never delivered");
}

/// A push that retries while a bounded queue is full.
pub fn retry<T>(mut v: T, mut f: impl FnMut(T) -> Result<(), T>) {
    loop {
        match f(v) {
            Ok(()) => return,
            Err(back) => {
                v = back;
                thread::yield_now();
            }
        }
    }
}
