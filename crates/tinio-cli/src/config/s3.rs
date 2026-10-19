use garde::Validate;
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

use crate::{
    _core::{multipart::DEFAULT_EXPIRE_DAYS, storage::DEFAULT_MAX_CONCURRENT_UPLOADS},
    _server::s3::Capabilities,
};

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
/// assert_eq!(s3.multipart_expire_days, None);
/// assert_eq!(s3.multipart_expire_days(), 7);
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
#[serde(default)]
pub struct Config {
    /// Runtime capability toggles (FR-021). Flattened so keys stay at `[s3]`.
    #[serde(flatten)]
    #[garde(dive)]
    pub capabilities: Capabilities,
    /// SigV2 verification (deprecated; enabling prints a startup warning).
    pub sig_v2: bool,
    /// Stale temp-write sweep timeout (hours). The floor of 1 is the
    /// parser's (garde `range(min = 1)`): a zero arms the sweeper
    /// maximally — it removes a staged write temp mid-PUT — so it is
    /// refused rather than read as the section's "unlimited" convention.
    #[default(_code = "DEFAULT_TEMP_TTL_HOURS")]
    #[garde(range(min = 1))]
    pub temp_ttl_hours: u64,
    /// Abandoned-upload sweep timeout (days). The floor of 1 is the
    /// parser's (a zero aborts every in-flight multipart upload on the
    /// first pass, so it is refused rather than read as "unlimited").
    /// **`None` means the operator did not write the key**: the effective
    /// default is [`Config::multipart_expire_days`]'s, and the pg
    /// abandoned-upload stage runs only for an explicit key, so a default
    /// configuration keeps its pre-lifecycle behavior there.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(custom(validate_multipart_expire_days))]
    pub multipart_expire_days: Option<u64>,
    /// Noncurrent versions retained per key: the sweep keeps the newest N
    /// and expires the rest (default 0 = keep every noncurrent version,
    /// the safe reading of an unset key).
    #[default = 0]
    pub noncurrent_keep: u64,
    /// Age (days) after which a noncurrent version is expired, measured
    /// from when it stopped being current (default 0 = never expire by
    /// age). With `noncurrent_keep` set too, whichever threshold is met
    /// first expires the version.
    #[default = 0]
    pub noncurrent_expire_days: u64,
    /// Minimum age (minutes) before an unreferenced block may be
    /// reclaimed (default 10): the grace narrows the window in which a
    /// publish still in flight stores a block the live set does not yet
    /// know about, so a zero is refused at parse rather than silently
    /// disarming the guard.
    #[default(_code = "DEFAULT_BLOCK_GRACE_MINUTES")]
    #[garde(range(min = 1))]
    pub block_grace_minutes: u64,
    /// Warn when the versioned buckets' block stores hold more than this
    /// many bytes (default 0 = never warn, the safe reading of an unset
    /// key). The store has no hard cap, so this threshold is the
    /// capacity-planning signal: the sweep compares the block walk's own
    /// byte total against it and logs a warning above it.
    #[default = 0]
    pub block_bytes_warn: u64,
    /// Cap on concurrently in-progress multipart uploads (default 1000):
    /// without a cap an authenticated client can accumulate an unbounded
    /// number of uploads (each up to 10,000 parts), exhausting disk,
    /// inodes, and metadata rows. Shared with the filesystem backend via
    /// [`tinio_core::storage::DEFAULT_MAX_CONCURRENT_UPLOADS`].
    #[default(_code = "DEFAULT_MAX_CONCURRENT_UPLOADS")]
    #[garde(range(min = 1))]
    pub max_concurrent_uploads: u32,
}

/// Default stale temp-write sweep timeout (hours).
const DEFAULT_TEMP_TTL_HOURS: u64 = 24;

impl Config {
    /// Keys a serialized default omits because the field's default is `None`.
    /// The parser still accepts them; keep this list beside the field.
    pub(crate) const OPTIONAL_KEYS: &'static [&'static str] = &["multipart_expire_days"];

    /// The effective abandoned-upload timeout: the operator's value, or the default.
    pub fn multipart_expire_days(&self) -> u64 {
        self.multipart_expire_days
            .unwrap_or(DEFAULT_MULTIPART_EXPIRE_DAYS)
    }
}

/// A written `multipart_expire_days` must be positive (absent = the default).
fn validate_multipart_expire_days(value: &Option<u64>, _context: &()) -> garde::Result {
    if value.is_some_and(|days| days == 0) {
        return Err(garde::Error::new(
            "multipart_expire_days must be positive (omit the key for the default)",
        ));
    }
    Ok(())
}

/// Default abandoned-upload sweep timeout (days).
const DEFAULT_MULTIPART_EXPIRE_DAYS: u64 = DEFAULT_EXPIRE_DAYS;

/// Default minimum age (minutes) before an unreferenced block is reclaimed.
const DEFAULT_BLOCK_GRACE_MINUTES: u64 = 10;
