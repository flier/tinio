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
#[cfg_attr(feature = "config", serde(default))]
pub struct Capabilities {
    /// Multipart operations + `upload_part_copy`.
    #[default(_code = "DEFAULT_MULTIPART")]
    pub multipart: bool,
    /// Server-side `copy_object`.
    #[default(_code = "DEFAULT_COPY_OBJECT")]
    pub copy_object: bool,
    /// ListObjects (V1).
    #[default(_code = "DEFAULT_LIST_OBJECTS_V1")]
    pub list_objects_v1: bool,
    /// ListObjectsV2.
    #[default(_code = "DEFAULT_LIST_OBJECTS_V2")]
    pub list_objects_v2: bool,
    /// DeleteObjects (batch).
    #[default(_code = "DEFAULT_DELETE_OBJECTS")]
    pub delete_objects: bool,

    /// Cap on the ListBuckets page size: larger `max-buckets` requests
    /// are clamped to this value. 0 = unlimited (no clamp). Default
    /// [`MAX_BUCKETS`] (10,000) — the AWS documented maximum. Values
    /// above [`MAX_BUCKETS`] are rejected at parse (F04): the wire
    /// rejects any `max-buckets` above 10,000 before the clamp could
    /// act, so a larger cap is dead configuration.
    #[default(_code = "DEFAULT_MAX_BUCKETS")]
    #[cfg_attr(feature = "config", garde(range(min = 0, max = MAX_BUCKETS)))]
    pub max_buckets: u32,

    /// Cap on the ListObjects page size: larger `max-keys` requests are
    /// clamped to this value. 0 = unlimited (no clamp). Default 0 —
    /// unlimited, preserving current behavior (AWS documents no max-keys
    /// cap).
    #[default = 0]
    pub max_keys: u32,

    /// Escape hatch for the pre-existing listing surfaces: when true,
    /// `max-keys` (V1/V2), `max-parts`, and `max-uploads` accept 0 —
    /// and clamp negative values to 0 — answering the empty page the
    /// pre-2026-08 behavior answered instead of `InvalidArgument`.
    /// ListBuckets keeps the AWS-documented 1..=10,000 validation
    /// regardless. Default false (strict).
    #[default = false]
    pub allow_zero_page_size: bool,

    /// Validate and echo `x-amz-checksum-*` on multipart uploads
    /// (spec 2026-08-31). Default false: checksums are accepted and
    /// dropped, exactly as before.
    #[default = false]
    pub checksum: bool,

    /// Object and bucket tagging (Get/Put/Delete*Tagging). Default on.
    #[default(_code = "DEFAULT_TAGGING")]
    pub tagging: bool,

    /// Bucket CORS (Get/Put/DeleteBucketCors). Default on.
    #[default(_code = "DEFAULT_CORS")]
    pub cors: bool,

    /// S3 Select (`SelectObjectContent`). Default on.
    #[default(_code = "DEFAULT_SELECT")]
    pub select: bool,

    /// Bucket/object ACL reads and writes (Get/Put*Acl). Default on.
    #[default(_code = "DEFAULT_ACL")]
    pub acl: bool,

    /// Object versioning (Get/PutBucketVersioning, ListObjectVersions,
    /// and the version-addressed object operations). Default on; off
    /// answers `NotImplemented` on the three versioning operations and
    /// refuses every version-addressed request outright — a request
    /// naming a version is never answered from the current object.
    #[default(_code = "DEFAULT_VERSIONING")]
    pub versioning: bool,

    /// Bucket lifecycle (Get/Put/DeleteBucketLifecycle). Default on; off
    /// answers `NotImplemented` on the three operations.
    #[default(_code = "DEFAULT_LIFECYCLE")]
    pub lifecycle: bool,

    /// S3 Object Lock (Get/PutObjectLockConfiguration,
    /// Get/PutObjectRetention, Get/PutObjectLegalHold). Default on; off
    /// answers `NotImplemented` on the six operations, while the stored
    /// configurations, default retention and delete protection stay in
    /// force (OL2, OL20).
    #[default(_code = "DEFAULT_OBJECT_LOCK")]
    pub object_lock: bool,

    /// Cap on concurrently streaming SelectObjectContent responses
    /// (default 4): each job owns a body forwarder and a seat on the
    /// select rayon pool — without a cap, concurrent selects exhaust
    /// threads and memory. Held for the whole response (dropped on stream
    /// end/cancel).
    #[default(_code = "DEFAULT_SELECT_CONCURRENCY")]
    #[cfg_attr(feature = "config", garde(range(min = 1)))]
    pub select_concurrency: u32,

    /// Stack size (bytes) of each select rayon worker (default
    /// [`DEFAULT_SELECT_STACK_BYTES`] = 16 MiB). X3: parquet/arrow deep
    /// recursion needs more than the OS default; a stack overflow aborts
    /// the process.
    #[default(DEFAULT_SELECT_STACK_BYTES)]
    #[cfg_attr(feature = "config", garde(range(min = 1024 * 1024)))]
    pub select_stack_bytes: u32,
}

/// Multipart operations + `upload_part_copy`. Default on.
const DEFAULT_MULTIPART: bool = true;

/// Server-side `copy_object`. Default on.
const DEFAULT_COPY_OBJECT: bool = true;

/// ListObjects (V1). Default on.
const DEFAULT_LIST_OBJECTS_V1: bool = true;

/// ListObjectsV2. Default on.
const DEFAULT_LIST_OBJECTS_V2: bool = true;

/// DeleteObjects (batch). Default on.
const DEFAULT_DELETE_OBJECTS: bool = true;

/// ListBuckets page-size cap: the AWS documented ceiling, so the number has
/// one home ([`MAX_BUCKETS`]).
const DEFAULT_MAX_BUCKETS: u32 = MAX_BUCKETS;

/// Object and bucket tagging. Default on.
const DEFAULT_TAGGING: bool = true;

/// Bucket CORS. Default on.
const DEFAULT_CORS: bool = true;

/// S3 Select. Default on.
const DEFAULT_SELECT: bool = true;

/// Bucket/object ACLs. Default on.
const DEFAULT_ACL: bool = true;

/// Object versioning. Default on.
const DEFAULT_VERSIONING: bool = true;

/// Bucket lifecycle. Default on.
const DEFAULT_LIFECYCLE: bool = true;

/// S3 Object Lock. Default on.
const DEFAULT_OBJECT_LOCK: bool = true;

/// Concurrent SelectObjectContent responses. Default 4.
const DEFAULT_SELECT_CONCURRENCY: u32 = 4;

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
        // it); the test pins both back to the documented literal.
        assert_eq!(Capabilities::default().max_buckets, MAX_BUCKETS);
        assert_eq!(MAX_BUCKETS, 10_000);
        assert_eq!(Capabilities::default().max_keys, 0);
    }

    #[test]
    fn absent_keys_take_the_struct_default() {
        // The struct-level serde default is the only default path: an
        // empty section lands on the `SmartDefault` values.
        let caps: Capabilities = toml::from_str("").unwrap();
        assert_eq!(caps, Capabilities::default());
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
    fn lifecycle_defaults_on_and_can_be_disabled() {
        assert!(
            Capabilities::default().lifecycle,
            "a core-surface capability defaults on, like tagging"
        );
        let caps: Capabilities = toml::from_str("lifecycle = false").unwrap();
        caps.validate().unwrap();
        assert!(!caps.lifecycle);
    }

    #[test]
    fn object_lock_defaults_on_and_can_be_disabled() {
        assert!(
            Capabilities::default().object_lock,
            "a core-surface capability defaults on, like lifecycle"
        );
        let caps: Capabilities = toml::from_str("object_lock = false").unwrap();
        caps.validate().unwrap();
        assert!(!caps.object_lock);
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
