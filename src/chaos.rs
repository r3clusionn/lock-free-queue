//! Delay injection for testing, compiled in only with `--features chaos`.
//!
//! The dangerous moments in a lock-free structure are the gaps between one atomic operation and the
//! next: a thread has read a pointer and has not yet used it. Left alone, threads are rarely
//! descheduled in those gaps. With the feature on, [`point`] sometimes yields or sleeps there, so
//! the stress tests meet the rare interleavings far more often. Without the feature it is nothing.

#[cfg(feature = "chaos")]
pub(crate) fn point() {
    use std::cell::Cell;
    thread_local! {
        static STATE: Cell<u64> = const { Cell::new(0) };
    }
    STATE.with(|s| {
        let mut x = s.get();
        if x == 0 {
            // Seed from the address of the thread-local itself: different for every thread.
            x = (s as *const _ as u64) | 1;
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        match x % 64 {
            0..=7 => std::thread::yield_now(),
            8 => std::thread::sleep(std::time::Duration::from_micros(50)),
            9..=10 => (0..200).for_each(|_| std::hint::spin_loop()),
            _ => {}
        }
    });
}

#[cfg(not(feature = "chaos"))]
#[inline(always)]
pub(crate) fn point() {}
