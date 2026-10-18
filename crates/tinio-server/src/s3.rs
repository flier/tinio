//! The `[s3]` section's capability half: the runtime feature toggles the server
//! serves, and the page-size caps its wire validation reads. The sweep and
//! multipart limits are the CLI's section (they configure the CLI-owned sweeper),
//! so this module is the whole of what the server itself must know.

#[cfg(feature = "config")]
use garde::Validate;
#[cfg(feature = "config")]
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

/// The AWS documented ListBuckets page-size ceiling (2025-03 API):
/// `max-buckets` above it is invalid at the wire, and it doubles as the
/// default `max_buckets` cap. One home for the number — the server's
/// wire-level validation references it instead of re-defining it.
pub const MAX_BUCKETS: u32 = 10_000;

/// Default select-engine thread stack (X3): parquet 59.3 footer schema
/// recursion and `arrow_json` can nest unboundedly — a stack overflow is
/// not a catchable panic. One home for the byte count used by
/// [`Capabilities::select_stack_bytes`]'s default.
pub const DEFAULT_SELECT_STACK_BYTES: u32 = 16 * 1024 * 1024;

/// Runtime capability toggles of the `[s3]` section (FR-021). Disabled
/// groups return `NotImplemented`. Flattened into the section's `Config` so
/// the TOML keys stay at `[s3]` (not a nested `[s3.capabilities]` table).
///
/// # Examples
///
/// ```rust
/// use tinio_server::s3::Capabilities;
///
/// let caps = Capabilities::default();
/// assert!(caps.multipart && caps.copy_object);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, SmartDefault)]
#[cfg_attr(feature = "config", derive(Serialize, Deserialize, Validate))]
#[cfg_attr(feature = "config", garde(allow_unvalidated))]
pub struct Capabilities {
    /// Multipart operations + `upload_part_copy`.
    #[cfg_attr(feature = "config", serde(default = "multipart"))]
    #[default = true]
    pub multipart: bool,
    /// Server-side `copy_object`.
    #[cfg_attr(feature = "config", serde(default = "copy_object"))]
    #[default = true]
    pub copy_object: bool,
    /// ListObjects (V1).
    #[cfg_attr(feature = "config", serde(default = "list_objects_v1"))]
    #[default = true]
    pub list_objects_v1: bool,
    /// ListObjectsV2.
    #[cfg_attr(feature = "config", serde(default = "list_objects_v2"))]
    #[default = true]
    pub list_objects_v2: bool,
    /// DeleteObjects (batch).
    #[cfg_attr(feature = "config", serde(default = "delete_objects"))]
    #[default = true]
    pub delete_objects: bool,

    /// Cap on the ListBuckets page size: larger `max-buckets` requests
    /// are clamped to this value. 0 = unlimited (no clamp). Default
    /// [`MAX_BUCKETS`] (10,000) — the AWS documented maximum. Values
    /// above [`MAX_BUCKETS`] are rejected at parse (F04): the wire
    /// rejects any `max-buckets` above 10,000 before the clamp could
    /// act, so a larger cap is dead configuration.
    #[cfg_attr(feature = "config", serde(default = "max_buckets"))]
    #[default = 10000]
    #[cfg_attr(feature = "config", garde(range(min = 0, max = MAX_BUCKETS)))]
    pub max_buckets: u32,

    /// Cap on the ListObjects page size: larger `max-keys` requests are
    /// clamped to this value. 0 = unlimited (no clamp). Default 0 —
    /// unlimited, preserving current behavior (AWS documents no max-keys
    /// cap).
    #[cfg_attr(feature = "config", serde(default = "max_keys"))]
    #[default = 0]
    pub max_keys: u32,

    /// Escape hatch for the pre-existing listing surfaces: when true,
    /// `max-keys` (V1/V2), `max-parts`, and `max-uploads` accept 0 —
    /// and clamp negative values to 0 — answering the empty page the
    /// pre-2026-08 behavior answered instead of `InvalidArgument`.
    /// ListBuckets keeps the AWS-documented 1..=10,000 validation
    /// regardless. Default false (strict).
    #[cfg_attr(feature = "config", serde(default))]
    #[default = false]
    pub allow_zero_page_size: bool,

    /// Validate and echo `x-amz-checksum-*` on multipart uploads
    /// (spec 2026-08-31). Default false: checksums are accepted and
    /// dropped, exactly as before.
    #[cfg_attr(feature = "config", serde(default))]
    #[default = false]
    pub checksum: bool,

    /// Object and bucket tagging (Get/Put/Delete*Tagging). Default on.
    #[cfg_attr(feature = "config", serde(default = "tagging"))]
    #[default = true]
    pub tagging: bool,

    /// Bucket CORS (Get/Put/DeleteBucketCors). Default on.
    #[cfg_attr(feature = "config", serde(default = "cors"))]
    #[default = true]
    pub cors: bool,

    /// S3 Select (`SelectObjectContent`). Default on.
    #[cfg_attr(feature = "config", serde(default = "select"))]
    #[default = true]
    pub select: bool,

    /// Bucket/object ACL reads and writes (Get/Put*Acl). Default on.
    #[cfg_attr(feature = "config", serde(default = "acl"))]
    #[default = true]
    pub acl: bool,

    /// Object versioning (Get/PutBucketVersioning, ListObjectVersions,
    /// and the version-addressed object operations). Default on; off
    /// answers `NotImplemented` on the three versioning operations and
    /// refuses every version-addressed request outright — a request
    /// naming a version is never answered from the current object.
    #[cfg_attr(feature = "config", serde(default = "versioning"))]
    #[default = true]
    pub versioning: bool,

    /// Cap on concurrently streaming SelectObjectContent responses
    /// (default 4): each job owns a body forwarder and a seat on the
    /// select rayon pool — without a cap, concurrent selects exhaust
    /// threads and memory. Held for the whole response (dropped on stream
    /// end/cancel).
    #[cfg_attr(feature = "config", serde(default = "select_concurrency"))]
    #[default = 4]
    #[cfg_attr(feature = "config", garde(range(min = 1)))]
    pub select_concurrency: u32,

    /// Stack size (bytes) of each select rayon worker (default
    /// [`DEFAULT_SELECT_STACK_BYTES`] = 16 MiB). X3: parquet/arrow deep
    /// recursion needs more than the OS default; a stack overflow aborts
    /// the process.
    #[cfg_attr(feature = "config", serde(default = "select_stack_bytes"))]
    #[default(DEFAULT_SELECT_STACK_BYTES)]
    #[cfg_attr(feature = "config", garde(range(min = 1024 * 1024)))]
    pub select_stack_bytes: u32,
}

#[cfg(feature = "config")]
fn multipart() -> bool {
    Capabilities::default().multipart
}

#[cfg(feature = "config")]
fn copy_object() -> bool {
    Capabilities::default().copy_object
}

#[cfg(feature = "config")]
fn list_objects_v1() -> bool {
    Capabilities::default().list_objects_v1
}

#[cfg(feature = "config")]
fn list_objects_v2() -> bool {
    Capabilities::default().list_objects_v2
}

#[cfg(feature = "config")]
fn delete_objects() -> bool {
    Capabilities::default().delete_objects
}

#[cfg(feature = "config")]
fn max_buckets() -> u32 {
    Capabilities::default().max_buckets
}

#[cfg(feature = "config")]
fn max_keys() -> u32 {
    Capabilities::default().max_keys
}

#[cfg(feature = "config")]
fn tagging() -> bool {
    Capabilities::default().tagging
}

#[cfg(feature = "config")]
fn cors() -> bool {
    Capabilities::default().cors
}

#[cfg(feature = "config")]
fn select() -> bool {
    Capabilities::default().select
}

#[cfg(feature = "config")]
fn acl() -> bool {
    Capabilities::default().acl
}

#[cfg(feature = "config")]
fn versioning() -> bool {
    Capabilities::default().versioning
}

#[cfg(feature = "config")]
fn select_concurrency() -> u32 {
    Capabilities::default().select_concurrency
}

#[cfg(feature = "config")]
fn select_stack_bytes() -> u32 {
    Capabilities::default().select_stack_bytes
}

#[cfg(all(test, feature = "config"))]
mod schema_tests {
    use garde::Validate;

    use super::*;

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
    }

    #[test]
    fn max_buckets_above_the_aws_ceiling_is_rejected_at_parse() {
        // F04: a cap above 10,000 is dead configuration — the wire
        // rejects any `max-buckets` above 10,000 before the clamp could
        // act — so it is rejected at config load, never silently
        // accepted.
        let caps: Capabilities = toml::from_str("max_buckets = 10001").unwrap();
        assert!(caps.validate().is_err());
        // The ceiling itself parses.
        let caps: Capabilities = toml::from_str("max_buckets = 10000").unwrap();
        caps.validate().unwrap();
        // 0 (no clamp) stays legal — the docs' "0 = unlimited".
        let caps: Capabilities = toml::from_str("max_buckets = 0").unwrap();
        caps.validate().unwrap();
    }

    #[test]
    fn max_buckets_and_max_keys_parse_and_accept_zero() {
        let caps: Capabilities = toml::from_str("max_buckets = 3\nmax_keys = 5").unwrap();
        caps.validate().unwrap();
        assert_eq!(caps.max_buckets, 3);
        assert_eq!(caps.max_keys, 5);
        // 0 is legal and meaningful for both knobs ("no clamp").
        let caps: Capabilities = toml::from_str("max_buckets = 0\nmax_keys = 0").unwrap();
        caps.validate().unwrap();
        assert_eq!(caps.max_buckets, 0);
        assert_eq!(caps.max_keys, 0);
    }

    #[test]
    fn allow_zero_page_size_defaults_off_and_parses() {
        assert!(!Capabilities::default().allow_zero_page_size);
        let caps: Capabilities = toml::from_str("allow_zero_page_size = true").unwrap();
        caps.validate().unwrap();
        assert!(caps.allow_zero_page_size);
    }

    #[test]
    fn checksum_defaults_off_and_parses() {
        assert!(!Capabilities::default().checksum);
        let caps: Capabilities = toml::from_str("checksum = true").unwrap();
        caps.validate().unwrap();
        assert!(caps.checksum);
    }

    #[test]
    fn tagging_defaults_on_and_can_be_disabled() {
        // The default config has tagging enabled.
        let caps = Capabilities::default();
        assert!(caps.tagging);
        // A config with tagging: false round-trips.
        let toml = "tagging = false";
        let caps: Capabilities = toml::from_str(toml).unwrap();
        caps.validate().unwrap();
        assert!(!caps.tagging);
    }

    #[test]
    fn cors_defaults_on_and_can_be_disabled() {
        // The default config has CORS enabled.
        let caps = Capabilities::default();
        assert!(caps.cors);
        // A config with cors: false round-trips.
        let toml = "cors = false";
        let caps: Capabilities = toml::from_str(toml).unwrap();
        caps.validate().unwrap();
        assert!(!caps.cors);
    }

    #[test]
    fn select_defaults_on_and_can_be_disabled() {
        // The default config has select enabled (FR-021 parity).
        let caps = Capabilities::default();
        assert!(caps.select);
        assert_eq!(caps.select_concurrency, 4);
        assert_eq!(caps.select_stack_bytes, DEFAULT_SELECT_STACK_BYTES);
        // A config with select: false round-trips.
        let toml = "select = false";
        let caps: Capabilities = toml::from_str(toml).unwrap();
        caps.validate().unwrap();
        assert!(!caps.select);
    }

    #[test]
    fn acl_defaults_on_and_can_be_disabled() {
        // The default config has the ACL ops enabled.
        let caps = Capabilities::default();
        assert!(caps.acl);
        // A config with acl: false round-trips.
        let toml = "acl = false";
        let caps: Capabilities = toml::from_str(toml).unwrap();
        caps.validate().unwrap();
        assert!(!caps.acl);
    }

    #[test]
    fn versioning_defaults_on_and_can_be_disabled() {
        assert!(
            Capabilities::default().versioning,
            "a core-surface capability defaults on, like tagging"
        );
        let caps: Capabilities = toml::from_str("versioning = false").unwrap();
        caps.validate().unwrap();
        assert!(!caps.versioning);
    }

    #[test]
    fn select_concurrency_parses_and_rejects_zero() {
        let caps: Capabilities = toml::from_str("select_concurrency = 2").unwrap();
        caps.validate().unwrap();
        assert_eq!(caps.select_concurrency, 2);
        let caps: Capabilities = toml::from_str("select_concurrency = 0").unwrap();
        assert!(caps.validate().is_err());
    }

    #[test]
    fn select_stack_bytes_parses_and_rejects_undersize() {
        let caps: Capabilities = toml::from_str("select_stack_bytes = 33554432").unwrap();
        caps.validate().unwrap();
        assert_eq!(caps.select_stack_bytes, 32 * 1024 * 1024);
        let caps: Capabilities = toml::from_str("select_stack_bytes = 1024").unwrap();
        assert!(caps.validate().is_err());
    }
}
