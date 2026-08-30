use garde::Validate;
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

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
}
