//! Throughput under contention: `cargo run --release --example bench -- [--items N] [--runs R]`.
//!
//! Producers and consumers each run on their own thread, started together. Every item is a `u64`, so
//! the time is the queue's. The clock runs from the moment the threads are released until the last
//! item has been taken out. Each cell is the median of `--runs` runs, in millions of items per
//! second; higher is better. Threads spin when the queue is full or empty (no sleeping), so keep
//! producers + consumers at or below the number of logical processors.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Barrier, Mutex};
use std::thread;
use std::time::Instant;

use crossbeam_queue::{ArrayQueue as CbArray, SegQueue as CbSeg};
use lfq::{spsc, ArrayQueue, MsQueue};

trait Queue: Sync {
    fn push(&self, v: u64);
    fn pop(&self) -> Option<u64>;
}

fn spin_until(mut f: impl FnMut() -> bool) {
    while !f() {
        std::hint::spin_loop();
    }
}

impl Queue for ArrayQueue<u64> {
    fn push(&self, v: u64) {
        let mut v = v;
        spin_until(|| match ArrayQueue::push(self, v) {
            Ok(()) => true,
            Err(b) => {
                v = b;
                false
            }
        });
    }
    fn pop(&self) -> Option<u64> {
        ArrayQueue::pop(self)
    }
}

impl Queue for CbArray<u64> {
    fn push(&self, v: u64) {
        let mut v = v;
        spin_until(|| match CbArray::push(self, v) {
            Ok(()) => true,
            Err(b) => {
                v = b;
                false
            }
        });
    }
    fn pop(&self) -> Option<u64> {
        CbArray::pop(self)
    }
}

impl Queue for MsQueue<u64> {
    fn push(&self, v: u64) {
        MsQueue::push(self, v)
    }
    fn pop(&self) -> Option<u64> {
        MsQueue::pop(self)
    }
}

impl Queue for CbSeg<u64> {
    fn push(&self, v: u64) {
        CbSeg::push(self, v)
    }
    fn pop(&self) -> Option<u64> {
        CbSeg::pop(self)
    }
}

/// A bounded or unbounded queue behind one lock.
struct Locked {
    q: Mutex<VecDeque<u64>>,
    cap: usize,
}

impl Queue for Locked {
    fn push(&self, v: u64) {
        spin_until(|| {
            let mut q = self.q.lock().unwrap();
            if q.len() < self.cap {
                q.push_back(v);
                true
            } else {
                false
            }
        });
    }
    fn pop(&self) -> Option<u64> {
        self.q.lock().unwrap().pop_front()
    }
}

/// Runs the contended scenario once and returns millions of items per second.
fn run<Q: Queue>(q: &Q, producers: usize, consumers: usize, items: usize) -> f64 {
    let per = items / producers;
    let total = per * producers;
    let taken = AtomicUsize::new(0);
    let start = Barrier::new(producers + consumers + 1);
    let mut elapsed = 0.0;
    thread::scope(|s| {
        for p in 0..producers {
            let (start, q) = (&start, q);
            s.spawn(move || {
                start.wait();
                for i in 0..per {
                    q.push((p * per + i) as u64);
                }
            });
        }
        for _ in 0..consumers {
            let (start, q, taken) = (&start, q, &taken);
            s.spawn(move || {
                start.wait();
                let mut sum = 0u64;
                while taken.load(Ordering::Relaxed) < total {
                    match q.pop() {
                        Some(v) => {
                            sum = sum.wrapping_add(v);
                            taken.fetch_add(1, Ordering::Relaxed);
                        }
                        None => std::hint::spin_loop(),
                    }
                }
                std::hint::black_box(sum);
            });
        }
        start.wait();
        let t = Instant::now();
        // The scope joins everything when it ends; poll for the finish so the clock stops there.
        while taken.load(Ordering::Relaxed) < total {
            std::hint::spin_loop();
        }
        elapsed = t.elapsed().as_secs_f64();
    });
    total as f64 / elapsed / 1e6
}

fn run_spsc_lfq(items: usize) -> f64 {
    let (mut p, mut c) = spsc::channel::<u64>(1024);
    let start = Barrier::new(3);
    let mut rate = 0.0;
    thread::scope(|s| {
        let start = &start;
        s.spawn(move || {
            start.wait();
            for i in 0..items as u64 {
                let mut v = i;
                while let Err(b) = p.push(v) {
                    v = b;
                    std::hint::spin_loop();
                }
            }
        });
        let h = s.spawn(move || {
            start.wait();
            let mut got = 0;
            let mut sum = 0u64;
            while got < items {
                match c.pop() {
                    Some(v) => {
                        sum = sum.wrapping_add(v);
                        got += 1;
                    }
                    None => std::hint::spin_loop(),
                }
            }
            std::hint::black_box(sum);
        });
        start.wait();
        let t = Instant::now();
        h.join().unwrap();
        rate = items as f64 / t.elapsed().as_secs_f64() / 1e6;
    });
    rate
}

fn run_spsc_mpsc(items: usize) -> f64 {
    let (tx, rx) = mpsc::channel::<u64>();
    let start = Barrier::new(3);
    let mut rate = 0.0;
    thread::scope(|s| {
        let start = &start;
        s.spawn(move || {
            start.wait();
            for i in 0..items as u64 {
                tx.send(i).unwrap();
            }
        });
        let h = s.spawn(move || {
            start.wait();
            let mut sum = 0u64;
            for _ in 0..items {
                loop {
                    match rx.try_recv() {
                        Ok(v) => {
                            sum = sum.wrapping_add(v);
                            break;
                        }
                        Err(_) => std::hint::spin_loop(),
                    }
                }
            }
            std::hint::black_box(sum);
        });
        start.wait();
        let t = Instant::now();
        h.join().unwrap();
        rate = items as f64 / t.elapsed().as_secs_f64() / 1e6;
    });
    rate
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |name: &str, default: usize| {
        args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).and_then(|v| v.parse().ok()).unwrap_or(default)
    };
    let items = arg("--items", 4_000_000);
    let runs = arg("--runs", 5);
    let cpus = thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    println!("{items} items per run, median of {runs} runs, {cpus} logical processors, millions of items per second\n");

    println!("One producer, one consumer");
    println!("| Queue | M items/s |");
    println!("|---|---|");
    let spsc = median((0..runs).map(|_| run_spsc_lfq(items)).collect());
    println!("| lfq::spsc (1024 slots) | {spsc:.1} |");
    let r = median((0..runs).map(|_| run(&ArrayQueue::new(1024), 1, 1, items)).collect());
    println!("| lfq::ArrayQueue (1024 slots) | {r:.1} |");
    let r = median((0..runs).map(|_| run(&CbArray::new(1024), 1, 1, items)).collect());
    println!("| crossbeam ArrayQueue (1024 slots) | {r:.1} |");
    let r = median((0..runs).map(|_| run(&MsQueue::new(), 1, 1, items)).collect());
    println!("| lfq::MsQueue (unbounded) | {r:.1} |");
    let r = median((0..runs).map(|_| run(&CbSeg::new(), 1, 1, items)).collect());
    println!("| crossbeam SegQueue (unbounded) | {r:.1} |");
    let r = median((0..runs).map(|_| run_spsc_mpsc(items)).collect());
    println!("| std::sync::mpsc (unbounded) | {r:.1} |");
    let r = median((0..runs).map(|_| run(&Locked { q: Mutex::new(VecDeque::new()), cap: usize::MAX }, 1, 1, items)).collect());
    println!("| Mutex<VecDeque> (unbounded) | {r:.1} |");

    let shapes: Vec<(usize, usize)> = [(2, 2), (4, 4), (8, 8), (16, 16)].into_iter().filter(|(p, c)| p + c <= cpus).collect();
    for (title, bounded) in [("Bounded, 1024 slots", true), ("Unbounded", false)] {
        println!("\n{title}, producers x consumers");
        print!("| Queue |");
        for (p, c) in &shapes {
            print!(" {p}x{c} |");
        }
        println!();
        print!("|---|");
        for _ in &shapes {
            print!("---|");
        }
        println!();
        macro_rules! row {
            ($name:expr, $make:expr) => {{
                print!("| {} |", $name);
                for &(p, c) in &shapes {
                    let r = median((0..runs).map(|_| run(&$make, p, c, items)).collect());
                    print!(" {r:.1} |");
                }
                println!();
            }};
        }
        if bounded {
            row!("lfq::ArrayQueue", ArrayQueue::new(1024));
            row!("crossbeam ArrayQueue", CbArray::new(1024));
            row!("Mutex<VecDeque>", Locked { q: Mutex::new(VecDeque::new()), cap: 1024 });
        } else {
            row!("lfq::MsQueue", MsQueue::new());
            row!("crossbeam SegQueue", CbSeg::new());
            row!("Mutex<VecDeque>", Locked { q: Mutex::new(VecDeque::new()), cap: usize::MAX });
        }
    }
}
