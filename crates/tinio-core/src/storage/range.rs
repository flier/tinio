//! Byte-range semantics for partial object reads.

use super::error::{Error, invalid_range};

/// A byte range for partial reads (the S3 `Range` header semantics).
///
/// # Examples
///
/// ```rust
/// use tinio_core::ByteRange;
///
/// // bytes=0-1023
/// let range = ByteRange::Inclusive(0, 1023);
/// // bytes=1024- (open-ended)
/// let from = ByteRange::From(1024);
/// // bytes=-512 (last 512 bytes)
/// let suffix = ByteRange::Suffix(512);
///
/// assert_ne!(range, from);
/// assert_eq!(suffix, ByteRange::Suffix(512));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteRange {
    /// `bytes=N-` — from byte N to the end of the object.
    From(u64),
    /// `bytes=A-B` — the inclusive range A..=B.
    Inclusive(u64, u64),
    /// `bytes=-N` — the last N bytes of the object.
    Suffix(u64),
}

impl ByteRange {
    /// Resolve this range against an object of `size` bytes into the
    /// inclusive `(start, end)` slice `[start..=end]`.
    ///
    /// Open-ended and suffix ranges clamp to the object; a range whose
    /// start exceeds the end after clamping, or any range on a zero-byte
    /// object, is [`Error::InvalidRange`] (the S3 mapping layer answers 416
    /// per AWS).
    pub fn resolve(self, size: u64) -> Result<(u64, u64), Error> {
        if size == 0 {
            return Err(invalid_range(self, size));
        }
        let last = size.saturating_sub(1);
        let (start, end) = match self {
            ByteRange::From(s) => (s, last),
            ByteRange::Inclusive(s, e) => (s, e.min(last)),
            ByteRange::Suffix(n) => (size.saturating_sub(n), last),
        };
        if start > end {
            return Err(invalid_range(self, size));
        }
        Ok((start, end))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_range_variants() {
        assert_eq!(ByteRange::From(0), ByteRange::From(0));
        assert_eq!(ByteRange::Inclusive(1, 10), ByteRange::Inclusive(1, 10));
        assert_eq!(ByteRange::Suffix(100), ByteRange::Suffix(100));
        assert_ne!(ByteRange::From(0), ByteRange::Inclusive(0, 0));
    }

    #[test]
    fn byte_range_resolve_clamps_and_rejects_unsatisfiable() {
        assert_eq!(ByteRange::Inclusive(8, 99).resolve(10).unwrap(), (8, 9));
        assert_eq!(ByteRange::Suffix(100).resolve(10).unwrap(), (0, 9));
        assert_eq!(ByteRange::From(0).resolve(10).unwrap(), (0, 9));
        assert!(ByteRange::From(10).resolve(10).is_err());
        assert!(ByteRange::Suffix(0).resolve(10).is_err());
        assert!(ByteRange::Inclusive(0, 0).resolve(0).is_err());
    }
}
