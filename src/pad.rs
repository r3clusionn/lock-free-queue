//! Cache-line padding, so two counters that different threads write do not share a line.

use std::ops::Deref;

/// Aligned to 128 bytes: a 64-byte line on most x86 parts, and Intel's adjacent-line prefetcher
/// pulls lines in pairs, so 128 keeps the two apart there too.
#[repr(align(128))]
pub(crate) struct CachePadded<T>(pub(crate) T);

impl<T> Deref for CachePadded<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &T {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn padded_values_do_not_share_a_cache_line() {
        assert_eq!(std::mem::align_of::<CachePadded<u8>>(), 128);
        assert_eq!(std::mem::size_of::<CachePadded<u8>>(), 128);
        let pair = [CachePadded(1u64), CachePadded(2u64)];
        let a = &*pair[0] as *const u64 as usize;
        let b = &*pair[1] as *const u64 as usize;
        assert!(b - a >= 128);
    }
}
