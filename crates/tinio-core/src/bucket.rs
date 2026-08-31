//! Bucket name validation and bucket metadata.

use std::time::SystemTime;

use derive_more::{AsRef, Deref, Display, Into};
use garde::Error as GardeError;

use crate::storage::{self, Error::*};

/// A bucket: a top-level collection of objects.
///
/// The creation time comes from the backend's persisted state (for tinio-fs:
/// `buckets.json`), lazily recorded on first sight of a pre-existing
/// directory.
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
///
/// use tinio_core::bucket::Bucket;
///
/// let bucket = Bucket {
///     name: "data".into(),
///     creation_time: SystemTime::UNIX_EPOCH,
/// };
/// assert_eq!(bucket.name.as_ref(), "data");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bucket {
    /// S3 bucket name (validated per FR-012).
    pub name: Name,
    /// Bucket creation timestamp.
    pub creation_time: SystemTime,
}

/// A validated bucket name (FR-012).
///
/// Constructed via [`name`], which enforces the S3 naming rules:
/// 3–63 chars, lowercase letters/digits/dots/hyphens, no leading/trailing
/// dot or hyphen, no adjacent dots; a leading dot is additionally rejected
/// as a reserved-name collision (FR-020).
///
/// Deref/AsRef expose the inner string; `"data".into()` builds a name from
/// a trusted literal (panics on invalid input).
///
/// # Examples
///
/// ```rust
/// use tinio_core::{
///     bucket,
///     storage::{self, Error::*},
/// };
///
/// let name = bucket::name("my-bucket").unwrap();
/// assert_eq!(name.as_ref(), "my-bucket");
///
/// for bad in ["ab", "Big_Name", "-lead", "a..b"] {
///     assert!(matches!(bucket::name(bad), Err(InvalidBucketName(_))));
/// }
///
/// let from_literal: bucket::Name = "my-bucket".into();
/// assert_eq!(from_literal, name);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Display, Deref, AsRef, Into)]
#[display("{}", _0)]
#[deref(forward)]
pub struct Name(String);

/// Validate and wrap a bucket name from an untrusted source (FR-012).
pub fn name(name: impl Into<String>) -> Result<Name, storage::Error> {
    let name = name.into();
    if validate_bucket_name(&name).is_err() {
        return Err(InvalidBucketName(name));
    }
    Ok(Name(name))
}

impl From<&str> for Name {
    /// Trusted-input convenience (panics on invalid names — use [`name`] for
    /// untrusted input).
    fn from(name: &str) -> Self {
        self::name(name).expect("valid bucket name")
    }
}

impl From<String> for Name {
    fn from(name: String) -> Self {
        self::name(name).expect("valid bucket name")
    }
}

fn validate_bucket_name(name: &str) -> garde::Result {
    let len = name.len();
    if len < 3 {
        return Err(GardeError::new(format!(
            "{name:?}: bucket names must be at least 3 characters"
        )));
    }
    if len > 63 {
        return Err(GardeError::new(format!(
            "{name:?}: bucket names must be at most 63 characters"
        )));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-')
    {
        return Err(GardeError::new(format!(
            "{name:?}: only lowercase letters, digits, dots and hyphens are allowed"
        )));
    }
    if name.starts_with('.') || name.starts_with('-') {
        return Err(GardeError::new(format!(
            "{name:?}: must not start with a dot or hyphen"
        )));
    }
    if name.ends_with('.') || name.ends_with('-') {
        return Err(GardeError::new(format!(
            "{name:?}: must not end with a dot or hyphen"
        )));
    }
    if name.contains("..") {
        return Err(GardeError::new(format!(
            "{name:?}: adjacent dots are not allowed"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use super::*;
    use crate::_util::testing::assert_send_sync;

    #[test]
    fn bucket_name_validates_and_exposes() {
        let n = name("my-bucket").unwrap();
        assert_eq!(n.as_ref(), "my-bucket");
        assert_eq!(n.to_string(), "my-bucket");
        assert_eq!(String::from(n.clone()), "my-bucket");
        let from_literal: Name = "my-bucket".into();
        assert_eq!(from_literal, n);
        let from_string: Name = "my-bucket".to_string().into();
        assert_eq!(from_string, n);
        assert!(name("Big").is_err());
    }

    #[test]
    fn bucket_name_from_owned_string() {
        // The owned-string conversion (used by wire payloads) accepts a
        // valid name.
        let n = Name::from("my-bucket".to_string());
        assert_eq!(n.as_ref(), "my-bucket");
    }

    #[test]
    #[should_panic]
    fn bucket_name_from_invalid_panics() {
        let _: Name = "Big".into();
    }

    #[test]
    fn valid_bucket_names_accepted() {
        for name in ["aaa", "data", "my-bucket", "my.bucket", "123", "a.b-c.d"] {
            validate_bucket_name(name).unwrap_or_else(|e| panic!("{name:?} should be valid: {e}"));
        }
    }

    #[test]
    fn invalid_bucket_names_rejected() {
        let too_long = "a".repeat(64);
        for name in [
            "",
            "a",
            "ab",
            "BIG",
            "under_score",
            "-lead",
            "trail-",
            ".lead",
            "trail.",
            "a..b",
            "sp ace",
            "sla/sh",
            too_long.as_str(),
        ] {
            assert!(
                validate_bucket_name(name).is_err(),
                "{name:?} must be rejected"
            );
        }
    }

    #[test]
    fn bucket_dot_segments_rejected() {
        for name in [".", "..", "...", "a..", "..a"] {
            assert!(
                validate_bucket_name(name).is_err(),
                "{name:?} must be rejected"
            );
        }
    }

    #[test]
    fn bucket_equality() {
        let t = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let b = Bucket {
            name: "data".into(),
            creation_time: t,
        };
        let b2 = Bucket {
            name: "data".into(),
            creation_time: t,
        };
        assert_eq!(b, b2);
    }

    #[test]
    fn bucket_types_are_send_sync_and_static() {
        assert_send_sync::<Name>();
        assert_send_sync::<Bucket>();
    }
}
