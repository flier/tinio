use garde::Validate;
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

/// S3 capability toggles (`[s3]`; runtime level, FR-021). Disabled groups
/// return `NotImplemented`.
///
/// # Examples
///
/// ```rust
/// use tinio_config::s3::Config;
///
/// let s3 = Config::default();
/// assert!(s3.multipart);
/// assert!(!s3.sig_v2); // deprecated, off by default
/// assert_eq!(s3.temp_ttl_hours, 24);
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Config {
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
}

fn multipart() -> bool {
    Config::default().multipart
}

fn copy_object() -> bool {
    Config::default().copy_object
}

fn list_objects_v1() -> bool {
    Config::default().list_objects_v1
}

fn list_objects_v2() -> bool {
    Config::default().list_objects_v2
}

fn delete_objects() -> bool {
    Config::default().delete_objects
}

fn temp_ttl_hours() -> u64 {
    Config::default().temp_ttl_hours
}

fn multipart_expire_days() -> u64 {
    Config::default().multipart_expire_days
}
