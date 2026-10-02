//! One place that decides whether the queues run on the standard library's atomics or on loom's.
//!
//! Under `RUSTFLAGS="--cfg loom"` every atomic, cell, thread and `Arc` is replaced by loom's model,
//! which runs the code under every thread interleaving (up to a preemption bound) and reports data
//! races and use-after-free. Otherwise this is a thin layer over `std`.

#[cfg(loom)]
pub(crate) use loom::cell::UnsafeCell;
#[cfg(loom)]
pub(crate) use loom::sync::atomic::{fence, AtomicBool, AtomicPtr, AtomicUsize, Ordering};
#[cfg(loom)]
pub(crate) use loom::sync::Arc;

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{fence, AtomicBool, AtomicPtr, AtomicUsize, Ordering};
#[cfg(not(loom))]
pub(crate) use std::sync::Arc;

/// `std`'s `UnsafeCell` with the closure-based accessors loom's has, so the queues are written once.
#[cfg(not(loom))]
pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

#[cfg(not(loom))]
impl<T> UnsafeCell<T> {
    pub(crate) const fn new(v: T) -> Self {
        Self(std::cell::UnsafeCell::new(v))
    }

    #[inline(always)]
    pub(crate) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
        f(self.0.get())
    }

    #[inline(always)]
    pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.0.get())
    }
}
