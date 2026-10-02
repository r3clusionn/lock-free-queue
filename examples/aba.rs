//! The ABA problem, replayed step by step on a small lock-free stack.
//!
//! A stack of indices into a fixed arena: `next[i]` is the index below `i`, and `head` is the top.
//! `pop` reads the top and the index below it, then swings `head` with a compare-and-swap. That is
//! correct only if "head is still the same index" means "nothing changed". The schedule below shows
//! when it does not, and how a version tag in the head fixes it.
//!
//!     cargo run --release --example aba

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::SeqCst};

const NIL: u32 = u32::MAX;
const NAMES: [char; 3] = ['A', 'B', 'C'];

/// `head` holds `tag << 32 | index`. With `tagged == false` the tag is never changed, so the word is
/// just the index, which is what a naive implementation compares.
struct Stack {
    head: AtomicU64,
    next: [AtomicU32; 3],
    tagged: bool,
}

/// What a half-finished pop has seen.
#[derive(Clone, Copy)]
struct Seen {
    head_word: u64,
    top: u32,
    below: u32,
}

impl Stack {
    fn new(tagged: bool) -> Stack {
        // A (index 0) on top of B (1) on top of C (2).
        Stack { head: AtomicU64::new(0), next: [AtomicU32::new(1), AtomicU32::new(2), AtomicU32::new(NIL)], tagged }
    }

    fn word(&self, old: u64, index: u32) -> u64 {
        let tag = if self.tagged { (old >> 32).wrapping_add(1) } else { 0 };
        tag << 32 | index as u64
    }

    /// The first half of a pop: read the top and what is under it.
    fn pop_begin(&self) -> Option<Seen> {
        let w = self.head.load(SeqCst);
        let top = w as u32;
        (top != NIL).then(|| Seen { head_word: w, top, below: self.next[top as usize].load(SeqCst) })
    }

    /// The second half: swing the head to what was under the top, if the head still reads the same.
    fn pop_finish(&self, s: Seen) -> bool {
        self.head.compare_exchange(s.head_word, self.word(s.head_word, s.below), SeqCst, SeqCst).is_ok()
    }

    fn pop(&self) -> Option<u32> {
        loop {
            let s = self.pop_begin()?;
            if self.pop_finish(s) {
                return Some(s.top);
            }
        }
    }

    fn push(&self, index: u32) {
        loop {
            let w = self.head.load(SeqCst);
            self.next[index as usize].store(w as u32, SeqCst);
            if self.head.compare_exchange(w, self.word(w, index), SeqCst, SeqCst).is_ok() {
                return;
            }
        }
    }

    fn contents(&self) -> String {
        let mut out = Vec::new();
        let mut i = self.head.load(SeqCst) as u32;
        while i != NIL && out.len() < 6 {
            out.push(NAMES[i as usize].to_string());
            i = self.next[i as usize].load(SeqCst);
        }
        if out.is_empty() {
            "(empty)".into()
        } else {
            out.join(" -> ")
        }
    }
}

fn replay(tagged: bool) {
    println!("--- {} head ---", if tagged { "tagged (index + version)" } else { "untagged (index only)" });
    let s = Stack::new(tagged);
    println!("start:                     {}", s.contents());

    let t1 = s.pop_begin().unwrap();
    println!(
        "thread 1 starts a pop:     sees top {} with {} below it, then is descheduled",
        NAMES[t1.top as usize], NAMES[t1.below as usize]
    );

    let a = s.pop().unwrap();
    let b = s.pop().unwrap();
    println!("thread 2 pops {} and {}:     {}", NAMES[a as usize], NAMES[b as usize], s.contents());
    println!("thread 2 uses {} elsewhere (it is no longer in the stack)", NAMES[b as usize]);
    s.push(a);
    println!("thread 2 pushes {} back:    {}", NAMES[a as usize], s.contents());

    let ok = s.pop_finish(t1);
    println!("thread 1 resumes, CAS {:<8}  {}", if ok { "succeeds" } else { "fails" }, s.contents());
    if ok {
        println!(
            "  -> head is now {}, which thread 2 owns. The stack points at memory that is in use.",
            NAMES[t1.below as usize]
        );
    } else {
        println!("  -> the version changed, so the CAS failed; thread 1 retries from the real top.");
        let again = s.pop().unwrap();
        println!("  -> retry pops {} and leaves {}", NAMES[again as usize], s.contents());
    }
    println!();
}

fn main() {
    replay(false);
    replay(true);
    println!("The queues in this crate avoid ABA differently:");
    println!("  ArrayQueue  every slot's sequence number grows by one lap each pass, so it never repeats;");
    println!("  MsQueue     a node some thread has published as a hazard pointer is not freed, so its");
    println!("              address cannot come back as a different node while that thread depends on it;");
    println!("  spsc        no compare-and-swap, one writer per index.");
}
