//! The pass's clock seam (design LC4): evaluation reads one instant per pass,
//! and tests pin it.

use std::time::SystemTime;

use smart_default::SmartDefault;

/// The instant the pass evaluates every rule against.
///
/// One trait for both the wall clock and a pinned instant, so the engine has
/// no clock-shaped branch and its tests never depend on when they run.
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
///
/// use tinio_lifecycle::{Clock, FixedClock, SystemClock};
///
/// let pinned: &dyn Clock = &FixedClock(SystemTime::UNIX_EPOCH);
/// assert_eq!(pinned.now(), SystemTime::UNIX_EPOCH);
/// assert!(SystemClock.now() > SystemTime::UNIX_EPOCH);
/// ```
pub trait Clock: Send + Sync {
    /// The current instant.
    fn now(&self) -> SystemTime;
}

/// The wall clock: what a served run evaluates against.
///
/// # Examples
///
/// ```rust
/// use tinio_lifecycle::{Clock, SystemClock};
///
/// assert!(SystemClock.now() > std::time::SystemTime::UNIX_EPOCH);
/// ```
#[derive(Debug, Clone, Copy, SmartDefault)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// A clock pinned to one instant: the engine tests' seam, so a rule's due
/// date is never a question about when the test ran.
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
///
/// use tinio_lifecycle::{Clock, FixedClock};
///
/// assert_eq!(
///     FixedClock(SystemTime::UNIX_EPOCH).now(),
///     SystemTime::UNIX_EPOCH
/// );
/// ```
#[derive(Debug, Clone, Copy)]
pub struct FixedClock(pub SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}
