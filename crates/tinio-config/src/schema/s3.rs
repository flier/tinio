use garde::Validate;
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

/// The AWS documented ListBuckets page-size ceiling (2025-03 API):
/// `max-buckets` above it is invalid at the wire, and it doubles as the
/// default `max_buckets` cap. One home for the number — the server's
/// wire-level validation references it instead of re-defining it.
pub const MAX_BUCKETS: u32 = 10_000;

/// Runtime capability toggles of the `[s3]` section (FR-021). Disabled
/// groups return `NotImplemented`. Flattened into [`Config`] so the TOML
/// keys stay at `[s3]` (not a nested `[s3.capabilities]` table).
///
/// # Examples
///
/// ```rust
/// use tinio_config::s3::Capabilities;
///
/// let caps = Capabilities::default();
/// assert!(caps.multipart && caps.copy_object);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Capabilities {
    /// Multipart operations + `upload_part_copy`.
    #[serde(default = "multipart")]
    #[default = true]
    pub multipart: bool,
    /// Server-side `copy_object`.
    #[serde(default = "copy_object")]
    #[default = true]
    pub copy_object: bool,
    /// ListObjects (V1).
    #[serde(default = "list_objects_v1")]
    #[default = true]
    pub list_objects_v1: bool,
    /// ListObjectsV2.
    #[serde(default = "list_objects_v2")]
    #[default = true]
    pub list_objects_v2: bool,
    /// DeleteObjects (batch).
    #[serde(default = "delete_objects")]
    #[default = true]
    pub delete_objects: bool,

    /// Cap on the ListBuckets page size: larger `max-buckets` requests
    /// are clamped to this value. 0 = unlimited (no clamp). Default
    /// [`MAX_BUCKETS`] (10,000) — the AWS documented maximum. Values
    /// above [`MAX_BUCKETS`] are rejected at parse (F04): the wire
    /// rejects any `max-buckets` above 10,000 before the clamp could
    /// act, so a larger cap is dead configuration.
    #[serde(default = "max_buckets")]
    #[default = 10000]
    #[garde(range(min = 0, max = MAX_BUCKETS))]
    pub max_buckets: u32,

    /// Cap on the ListObjects page size: larger `max-keys` requests are
    /// clamped to this value. 0 = unlimited (no clamp). Default 0 —
    /// unlimited, preserving current behavior (AWS documents no max-keys
    /// cap).
    #[serde(default = "max_keys")]
    #[default = 0]
    pub max_keys: u32,

    /// Escape hatch for the pre-existing listing surfaces: when true,
    /// `max-keys` (V1/V2), `max-parts`, and `max-uploads` accept 0 —
    /// and clamp negative values to 0 — answering the empty page the
    /// pre-2026-08 behavior answered instead of `InvalidArgument`.
    /// ListBuckets keeps the AWS-documented 1..=10,000 validation
    /// regardless. Default false (strict).
    #[serde(default)]
    #[default = false]
    pub allow_zero_page_size: bool,
}

/// S3 section (`[s3]`; runtime level, FR-021). Disabled capability groups
/// return `NotImplemented`.
///
/// # Examples
///
/// ```rust
/// use tinio_config::s3::Config;
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
    /// Stale temp-write sweep timeout (hours).
    #[serde(default = "temp_ttl_hours")]
    #[default = 24]
    pub temp_ttl_hours: u64,
    /// Abandoned-upload sweep timeout (days).
    #[serde(default = "multipart_expire_days")]
    #[default = 7]
    pub multipart_expire_days: u64,
    /// Cap on concurrently in-progress multipart uploads (default 1000):
    /// without a cap an authenticated client can accumulate an unbounded
    /// number of uploads (each up to 10,000 parts), exhausting disk,
    /// inodes, and metadata rows. Shared with the filesystem backend via
    /// [`tinio_core::storage::DEFAULT_MAX_CONCURRENT_UPLOADS`].
    #[serde(default = "max_concurrent_uploads")]
    #[default = 1000]
    #[garde(range(min = 1))]
    pub max_concurrent_uploads: u32,
}

fn multipart() -> bool {
    Capabilities::default().multipart
}

fn copy_object() -> bool {
    Capabilities::default().copy_object
}

fn list_objects_v1() -> bool {
    Capabilities::default().list_objects_v1
}

fn list_objects_v2() -> bool {
    Capabilities::default().list_objects_v2
}

fn delete_objects() -> bool {
    Capabilities::default().delete_objects
}

fn max_buckets() -> u32 {
    Capabilities::default().max_buckets
}

fn max_keys() -> u32 {
    Capabilities::default().max_keys
}

impl From<&Config> for Capabilities {
    fn from(config: &Config) -> Self {
        config.capabilities
    }
}

fn temp_ttl_hours() -> u64 {
    Config::default().temp_ttl_hours
}

fn multipart_expire_days() -> u64 {
    Config::default().multipart_expire_days
}

fn max_concurrent_uploads() -> u32 {
    Config::default().max_concurrent_uploads
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config as RootConfig, Error};

    #[test]
    fn max_concurrent_uploads_defaults_to_1000() {
        assert_eq!(Config::default().max_concurrent_uploads, 1000);
        let config = RootConfig::parse("version = 1\n[s3]").unwrap();
        assert_eq!(config.s3.as_ref().unwrap().max_concurrent_uploads, 1000);
    }

    #[test]
    fn max_concurrent_uploads_parses_when_set() {
        let config = RootConfig::parse("version = 1\n[s3]\nmax_concurrent_uploads = 5").unwrap();
        assert_eq!(config.s3.as_ref().unwrap().max_concurrent_uploads, 5);
    }

    #[test]
    fn max_concurrent_uploads_rejects_zero() {
        let err = RootConfig::parse("version = 1\n[s3]\nmax_concurrent_uploads = 0").unwrap_err();
        assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
    }

    #[test]
    fn capabilities_flatten_into_s3_section() {
        let config =
            RootConfig::parse("version = 1\n[s3]\nmultipart = false\ncopy_object = false").unwrap();
        let caps = config.s3.as_ref().unwrap().capabilities;
        assert!(!caps.multipart);
        assert!(!caps.copy_object);
        assert!(caps.delete_objects);
        assert!(caps.list_objects_v1);
        assert!(caps.list_objects_v2);
    }

    #[test]
    fn max_buckets_and_max_keys_defaults() {
        // max_buckets = 10,000 (the AWS documented ceiling);
        // max_keys = 0 = unlimited, preserving current behavior.
        // The default is pinned to [`MAX_BUCKETS`] — the single home of
        // the number (the wire-level ceiling of the server references
        // it); the derive attribute needs a literal, so the test is the
        // equality pin.
        assert_eq!(Capabilities::default().max_buckets, MAX_BUCKETS);
        assert_eq!(MAX_BUCKETS, 10_000);
        assert_eq!(Capabilities::default().max_keys, 0);
        let config = RootConfig::parse("version = 1\n[s3]").unwrap();
        let caps = config.s3.as_ref().unwrap().capabilities;
        assert_eq!(caps.max_buckets, 10_000);
        assert_eq!(caps.max_keys, 0);
    }

    #[test]
    fn max_buckets_above_the_aws_ceiling_is_rejected_at_parse() {
        // F04: a cap above 10,000 is dead configuration — the wire
        // rejects any `max-buckets` above 10,000 before the clamp could
        // act — so it is rejected at config load, never silently
        // accepted.
        let err = RootConfig::parse("version = 1\n[s3]\nmax_buckets = 10001").unwrap_err();
        assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
        // The ceiling itself parses.
        let config = RootConfig::parse("version = 1\n[s3]\nmax_buckets = 10000").unwrap();
        assert_eq!(config.s3.as_ref().unwrap().capabilities.max_buckets, 10_000);
        // 0 (no clamp) stays legal — the docs' "0 = unlimited".
        let config = RootConfig::parse("version = 1\n[s3]\nmax_buckets = 0").unwrap();
        assert_eq!(config.s3.as_ref().unwrap().capabilities.max_buckets, 0);
    }

    #[test]
    fn max_buckets_and_max_keys_parse_and_accept_zero() {
        let config = RootConfig::parse("version = 1\n[s3]\nmax_buckets = 3\nmax_keys = 5").unwrap();
        let caps = config.s3.as_ref().unwrap().capabilities;
        assert_eq!(caps.max_buckets, 3);
        assert_eq!(caps.max_keys, 5);
        // 0 is legal and meaningful for both knobs ("no clamp").
        let config = RootConfig::parse("version = 1\n[s3]\nmax_buckets = 0\nmax_keys = 0").unwrap();
        let caps = config.s3.as_ref().unwrap().capabilities;
        assert_eq!(caps.max_buckets, 0);
        assert_eq!(caps.max_keys, 0);
    }

    #[test]
    fn allow_zero_page_size_defaults_off_and_parses() {
        assert!(!Capabilities::default().allow_zero_page_size);
        let config = RootConfig::parse("version = 1\n[s3]\nallow_zero_page_size = true").unwrap();
        assert!(
            config
                .s3
                .as_ref()
                .unwrap()
                .capabilities
                .allow_zero_page_size
        );
        // The knob flows through the capability pipeline.
        let caps = Capabilities::from(config.s3.as_ref().unwrap());
        assert!(caps.allow_zero_page_size);
    }

    #[test]
    fn capabilities_from_maps_config() {
        let config = RootConfig::parse(
            "version = 1\n[s3]\nmultipart = false\nmax_buckets = 7\nmax_keys = 9",
        )
        .unwrap();
        let caps = Capabilities::from(config.s3.as_ref().unwrap());
        assert!(!caps.multipart);
        assert!(
            caps.copy_object && caps.list_objects_v1 && caps.list_objects_v2 && caps.delete_objects
        );
        assert_eq!(caps.max_buckets, 7);
        assert_eq!(caps.max_keys, 9);
    }
}
