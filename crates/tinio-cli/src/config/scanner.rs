use std::time::Duration;

use garde::Validate;
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

use super::nonzero_duration;

/// The background ETag scanner (`[scanner]`; presence = on, FR-024).
///
/// Keys are Minio-aligned (`mc admin config set myminio scanner ...`).
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use tinio_cli::config::scanner::Config;
///
/// let s = Config::default();
/// assert_eq!(s.delay, 10.0);
/// assert_eq!(s.max_wait, Duration::from_secs(15));
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
#[serde(default)]
pub struct Config {
    /// Seconds between scan iterations (pacing/throttle), >= 0.
    #[garde(range(min = 0.0))]
    #[default(_code = "DEFAULT_DELAY")]
    pub delay: f64,
    /// Max time to wait for a scan slot when throttled. Floored at 1s by
    /// the parser (`config::nonzero_duration`): a zero wait would spin the
    /// pacing loop instead of yielding to request traffic.
    #[serde(with = "humantime_serde")]
    #[garde(custom(nonzero_duration))]
    #[default(_code = "DEFAULT_MAX_WAIT")]
    pub max_wait: Duration,
    /// Full-tree scan cycle (re-scan for out-of-band changes). The same 1s
    /// floor as `max_wait`: a zero cycle re-scans the tree back to back.
    #[serde(with = "humantime_serde")]
    #[garde(custom(nonzero_duration))]
    #[default(_code = "DEFAULT_CYCLE")]
    pub cycle: Duration,
}

/// Default seconds between scan iterations.
const DEFAULT_DELAY: f64 = 10.0;

/// Default max time to wait for a scan slot when throttled.
const DEFAULT_MAX_WAIT: Duration = Duration::from_secs(15);

/// Default full-tree scan cycle (re-scan for out-of-band changes).
const DEFAULT_CYCLE: Duration = Duration::from_secs(24 * 60 * 60);
