//! Multipart upload state and part metadata.

use std::time::SystemTime;

use derive_more::{AsRef, Deref, Display, Into};

use crate::{
    bucket, checksum,
    etag::ETag,
    object,
    storage::{self, Error::*},
};

/// Inclusive part-number range (S3 / the data model).
const MIN_PART: u32 = 1;
const MAX_PART: u32 = 10_000;

/// The S3 minimum size of every non-final multipart part (5 MiB,
/// `EntityTooSmall` on completion). The final part has no minimum.
pub const MIN_PART_BYTES: u64 = 5 * 1024 * 1024;

/// A validated multipart part number (`1..=10000`).
///
/// Untrusted input goes through [`part_number`]. [`From<u32>`] is for
/// trusted literals and panics on an out-of-range value.
///
/// # Examples
///
/// ```rust
/// use tinio_core::{multipart, storage::Error::*};
///
/// let n = multipart::part_number(1).unwrap();
/// assert_eq!(u32::from(n), 1);
/// assert!(matches!(
///     multipart::part_number(0),
///     Err(InvalidPartNumber(0))
/// ));
/// let trusted: multipart::PartNumber = 7.into();
/// assert_eq!(u32::from(trusted), 7);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, Deref, AsRef, Into)]
#[display("{}", _0)]
pub struct PartNumber(u32);

/// Validate a part number from an untrusted source.
pub fn part_number(n: u32) -> Result<PartNumber, storage::Error> {
    if !(MIN_PART..=MAX_PART).contains(&n) {
        return Err(InvalidPartNumber(n));
    }
    Ok(PartNumber(n))
}

impl From<u32> for PartNumber {
    /// Trusted-input convenience (panics on an invalid number — use
    /// [`part_number`] for untrusted input).
    fn from(n: u32) -> Self {
        part_number(n).expect("valid part number")
    }
}

/// One part in a [`crate::MultipartOps::complete_multipart_upload`] request
/// (S3 `CompleteMultipartUpload`: ordered `{partNumber, ETag}`).
///
/// # Examples
///
/// ```rust
/// use tinio_core::{CompletedPart, ETag};
///
/// let part = CompletedPart {
///     part_number: 1.into(),
///     etag: ETag::from_content(b"abc"),
/// };
/// assert_eq!(u32::from(part.part_number), 1);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedPart {
    /// Part number (`1..=10000`).
    pub part_number: PartNumber,
    /// ETag of the uploaded part (must match the stored part).
    pub etag: ETag,
}

/// State of an in-progress multipart upload.
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
///
/// use tinio_core::MultipartUpload;
///
/// let upload = MultipartUpload {
///     upload_id: "f47ac10b-58cc-4372-a567-0e02b2c3d479".into(),
///     bucket: "data".into(),
///     key: "big.bin".into(),
///     initiated_at: SystemTime::UNIX_EPOCH,
///     checksum: None,
/// };
/// assert_eq!(upload.upload_id, "f47ac10b-58cc-4372-a567-0e02b2c3d479");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartUpload {
    /// Upload identifier (UUID v4 in tinio-fs), unique per upload.
    pub upload_id: String,
    /// Target bucket.
    pub bucket: bucket::Name,
    /// Target object key.
    pub key: object::Key,
    /// Upload initiation timestamp (used for idle-expiration).
    pub initiated_at: SystemTime,
    /// The create-time checksum spec (`None` = no checksum upload).
    pub checksum: Option<checksum::Upload>,
}

/// Metadata of a single uploaded multipart part.
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
///
/// use tinio_core::PartInfo;
///
/// let part = PartInfo {
///     part_number: 1.into(),
///     size: 5_242_880,
///     etag: "d41d8cd98f00b204e9800998ecf8427e".into(),
///     last_modified: SystemTime::UNIX_EPOCH,
///     checksum: None,
/// };
/// assert_eq!(u32::from(part.part_number), 1);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartInfo {
    /// Part number (`1..=10000`).
    pub part_number: PartNumber,
    /// Part size in bytes.
    pub size: u64,
    /// Part ETag (content MD5 hex of the part body).
    pub etag: ETag,
    /// Last write time of the part (used for idle-expiration).
    pub last_modified: SystemTime,
    /// The stored checksum of the part (`None` = none was computed).
    pub checksum: Option<checksum::Part>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::_util::testing::assert_send_sync;

    #[test]
    fn part_info_round_trip() {
        let p = PartInfo {
            part_number: 7.into(),
            size: 100,
            etag: "d41d8cd98f00b204e9800998ecf8427e".into(),
            last_modified: SystemTime::UNIX_EPOCH,
            checksum: None,
        };
        assert_eq!(u32::from(p.part_number), 7);
        assert_eq!(p.size, 100);
    }

    #[test]
    fn multipart_upload_state() {
        let m = MultipartUpload {
            upload_id: "uuid-v4".into(),
            bucket: "data".into(),
            key: "big.bin".into(),
            initiated_at: SystemTime::UNIX_EPOCH,
            checksum: None,
        };
        assert_eq!(m.upload_id, "uuid-v4");
        assert_eq!(m.key.as_ref(), "big.bin");
    }

    #[test]
    fn multipart_types_are_send_sync_and_static() {
        assert_send_sync::<PartInfo>();
        assert_send_sync::<MultipartUpload>();
        assert_send_sync::<PartNumber>();
        assert_send_sync::<CompletedPart>();
    }

    #[test]
    #[should_panic]
    fn part_number_from_invalid_panics() {
        let _: PartNumber = 0.into();
    }
}
