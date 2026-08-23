//! Nanosecond timestamp helpers for stored metadata.

use std::time::{Duration, SystemTime};

/// Unix time in nanoseconds (stored backend timestamps; `0` on a pre-epoch
/// clock).
pub fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Convert a stored nanosecond timestamp back into a [`SystemTime`].
pub fn from_nanos(n: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_nanos(n)
}
