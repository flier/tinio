use std::time::Duration;

use garde::Validate;
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

/// The background ETag scanner (`[scanner]`; presence = on, FR-024).
///
/// Keys are Minio-aligned (`mc admin config set myminio scanner ...`).
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use tinio_config::scanner::Config;
///
/// let s = Config::default();
/// assert_eq!(s.delay, 10.0);
/// assert_eq!(s.max_wait, Duration::from_secs(15));
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Config {
    /// Seconds between scan iterations (pacing/throttle), >= 0.
    #[serde(default = "delay")]
    #[garde(range(min = 0.0))]
    #[default = 10.0]
    pub delay: f64,
    /// Max time to wait for a scan slot when throttled.
    #[serde(default = "max_wait", with = "humantime_serde")]
    #[default(_code = "Duration::from_secs(15)")]
    pub max_wait: Duration,
    /// Full-tree scan cycle (re-scan for out-of-band changes).
    #[serde(default = "cycle", with = "humantime_serde")]
    #[default(_code = "Duration::from_secs(24 * 60 * 60)")]
    pub cycle: Duration,
}

fn delay() -> f64 {
    Config::default().delay
}

fn max_wait() -> Duration {
    Config::default().max_wait
}

fn cycle() -> Duration {
    Config::default().cycle
}
