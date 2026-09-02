//! Nanosecond timestamp helpers for stored metadata.

use std::time::{Duration, SystemTime};

/// Unix time in nanoseconds (stored backend timestamps; `0` on a pre-epoch
/// clock).
pub fn now_nanos() -> u64 {
    to_nanos(SystemTime::now())
}

/// Unix nanoseconds of a [`SystemTime`] (`0` on a pre-epoch clock).
pub fn to_nanos(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Convert a stored nanosecond timestamp back into a [`SystemTime`].
pub fn from_nanos(n: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_nanos(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_round_trips_to_zero() {
        assert_eq!(to_nanos(SystemTime::UNIX_EPOCH), 0);
        assert_eq!(from_nanos(0), SystemTime::UNIX_EPOCH);
    }

    #[test]
    fn nanos_are_since_epoch() {
        // 1 s after epoch = 1e9 ns. Values must be multiples of the
        // platform clock resolution: Windows `SystemTime` ticks at
        // 100 ns, so a sub-tick offset (42 ns) reads back as 0 — the
        // stored unit is ns, the representable resolution is platform-.
        let t = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        assert_eq!(to_nanos(t), 1_000_000_000);
        let t = SystemTime::UNIX_EPOCH + Duration::from_nanos(500);
        assert_eq!(to_nanos(t), 500);
    }

    #[test]
    fn to_from_round_trip_preserves_instants() {
        // 100 ns-aligned values round-trip on every platform (Windows
        // `SystemTime` resolves to 100 ns ticks).
        for nanos in [0u64, 100, 500, 1_234_567_800, 10_000_000_000] {
            let t = from_nanos(nanos);
            assert_eq!(to_nanos(t), nanos, "round trip at {nanos} ns");
        }
    }

    #[test]
    fn pre_epoch_clocks_map_to_zero() {
        // A clock before the Unix epoch must never wrap/panic — `0` is
        // the documented sentinel (stored backend timestamps).
        let pre = SystemTime::UNIX_EPOCH - Duration::from_secs(1);
        assert_eq!(to_nanos(pre), 0);
    }

    #[test]
    fn now_nanos_is_positive_and_advances() {
        let a = now_nanos();
        assert!(a > 0);
        let b = now_nanos();
        assert!(b >= a);
        // And the round-trip holds for a live clock reading.
        assert_eq!(to_nanos(from_nanos(b)), b);
    }
}
