use garde::Validate;
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

use crate::{_core::storage::DEFAULT_MAX_CONCURRENT_UPLOADS, _server::s3::Capabilities};

/// S3 section (`[s3]`; runtime level, FR-021). Disabled capability groups
/// return `NotImplemented`.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::config::s3::Config;
///
/// let s3 = Config::default();
/// assert!(s3.capabilities.multipart);
/// assert!(!s3.sig_v2); // deprecated, off by default
/// assert_eq!(s3.temp_ttl_hours, 24);
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Config {
    /// Runtime capability toggles (FR-021). Flattened so keys stay at `[s3]`.
    #[serde(flatten)]
    #[garde(dive)]
    pub capabilities: Capabilities,
    /// SigV2 verification (deprecated; enabling prints a startup warning).
    #[serde(default)]
    pub sig_v2: bool,
    /// Stale temp-write sweep timeout (hours). The floor of 1 is the
    /// parser's (garde `range(min = 1)`): a zero arms the sweeper
    /// maximally — it removes a staged write temp mid-PUT — so it is
    /// refused rather than read as the section's "unlimited" convention.
    #[serde(default = "temp_ttl_hours")]
    #[default = 24]
    #[garde(range(min = 1))]
    pub temp_ttl_hours: u64,
    /// Abandoned-upload sweep timeout (days). The floor of 1 is the
    /// parser's (garde `range(min = 1)`): a zero aborts every in-flight
    /// multipart upload on the first pass, so it is refused rather than
    /// read as "unlimited".
    #[serde(default = "multipart_expire_days")]
    #[default = 7]
    #[garde(range(min = 1))]
    pub multipart_expire_days: u64,
    /// Noncurrent versions retained per key: the sweep keeps the newest N
    /// and expires the rest (default 0 = keep every noncurrent version,
    /// the safe reading of an unset key).
    #[serde(default = "noncurrent_keep")]
    #[default = 0]
    pub noncurrent_keep: u64,
    /// Age (days) after which a noncurrent version is expired, measured
    /// from when it stopped being current (default 0 = never expire by
    /// age). With `noncurrent_keep` set too, whichever threshold is met
    /// first expires the version.
    #[serde(default = "noncurrent_expire_days")]
    #[default = 0]
    pub noncurrent_expire_days: u64,
    /// Minimum age (minutes) before an unreferenced block may be
    /// reclaimed (default 10): the grace narrows the window in which a
    /// publish still in flight stores a block the live set does not yet
    /// know about, so a zero is refused at parse rather than silently
    /// disarming the guard.
    #[serde(default = "block_grace_minutes")]
    #[default = 10]
    #[garde(range(min = 1))]
    pub block_grace_minutes: u64,
    /// Warn when the versioned buckets' block stores hold more than this
    /// many bytes (default 0 = never warn, the safe reading of an unset
    /// key). The store has no hard cap, so this threshold is the
    /// capacity-planning signal: the sweep compares the block walk's own
    /// byte total against it and logs a warning above it.
    #[serde(default = "block_bytes_warn")]
    #[default = 0]
    pub block_bytes_warn: u64,
    /// Cap on concurrently in-progress multipart uploads (default 1000):
    /// without a cap an authenticated client can accumulate an unbounded
    /// number of uploads (each up to 10,000 parts), exhausting disk,
    /// inodes, and metadata rows. Shared with the filesystem backend via
    /// [`tinio_core::storage::DEFAULT_MAX_CONCURRENT_UPLOADS`].
    #[serde(default = "max_concurrent_uploads")]
    #[default(_code = "DEFAULT_MAX_CONCURRENT_UPLOADS")]
    #[garde(range(min = 1))]
    pub max_concurrent_uploads: u32,
}

fn temp_ttl_hours() -> u64 {
    Config::default().temp_ttl_hours
}

fn multipart_expire_days() -> u64 {
    Config::default().multipart_expire_days
}

fn noncurrent_keep() -> u64 {
    Config::default().noncurrent_keep
}

fn noncurrent_expire_days() -> u64 {
    Config::default().noncurrent_expire_days
}

fn block_grace_minutes() -> u64 {
    Config::default().block_grace_minutes
}

fn block_bytes_warn() -> u64 {
    Config::default().block_bytes_warn
}

fn max_concurrent_uploads() -> u32 {
    DEFAULT_MAX_CONCURRENT_UPLOADS
}
