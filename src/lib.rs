//! Lock-free queues with the memory orderings spelled out.
//!
//! | Queue | Threads | Capacity | Reclamation |
//! |---|---|---|---|
//! | [`spsc`] | one producer, one consumer | bounded | none needed |
//! | [`ArrayQueue`] | any number of each | bounded | none needed |
//! | [`MsQueue`] | any number of each | unbounded | hazard pointers |
//!
//! ```
//! use lfq::{spsc, ArrayQueue, MsQueue};
//! use std::sync::Arc;
//!
//! // One producer thread, one consumer thread. The two halves are not Clone.
//! let (mut tx, mut rx) = spsc::channel::<u64>(1024);
//! std::thread::spawn(move || while tx.push(7).is_err() {});
//! let v = loop {
//!     if let Some(v) = rx.pop() {
//!         break v;
//!     }
//! };
//! assert_eq!(v, 7);
//!
//! // Bounded, shared by any number of threads. `push` hands the item back when full.
//! let q = Arc::new(ArrayQueue::new(1024));
//! q.push(1).unwrap();
//! assert_eq!(q.pop(), Some(1));
//!
//! // Unbounded: `push` never fails.
//! let q = Arc::new(MsQueue::new());
//! q.push("hello");
//! assert_eq!(q.pop(), Some("hello"));
//! ```
//!
//! Every `unsafe` block and every atomic ordering has a comment saying what it relies on. Build with
//! `RUSTFLAGS="--cfg loom"` to run the loom models in `tests/loom.rs`, which explore the thread
//! interleavings of small scenarios.

mod array_queue;
mod chaos;
mod hazard;
mod ms_queue;
mod pad;
pub mod spsc;
mod sync;

pub use array_queue::ArrayQueue;
pub use ms_queue::MsQueue;
