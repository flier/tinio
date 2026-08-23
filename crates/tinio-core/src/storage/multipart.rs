//! Multipart upload types and the [`MultipartOps`] contract category.

use async_trait::async_trait;

use crate::{
    bucket,
    multipart::{CompletedPart, MultipartUpload, PartInfo},
    object,
};

use super::{Storage, body::BodyStream};

/// Parameters of a [`MultipartOps::list_parts`] call.
///
/// # Examples
///
/// ```rust
/// use tinio_core::{bucket, object, ListPartsParams};
///
/// let params = ListPartsParams {
///     bucket: bucket::name("data").unwrap(),
///     key: object::key("big.bin").unwrap(),
///     upload_id: "uuid".into(),
///     max_parts: 1000,
///     part_number_marker: None,
/// };
/// assert_eq!(params.upload_id, "uuid");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartsParams {
    /// Bucket of the upload.
    pub bucket: bucket::Name,
    /// Key of the upload.
    pub key: object::Key,
    /// Upload identifier.
    pub upload_id: String,
    /// Maximum number of parts per page (default 1000).
    pub max_parts: usize,
    /// Resume after this part number (exclusive).
    pub part_number_marker: Option<u32>,
}

/// One page of a part listing.
///
/// # Examples
///
/// ```rust
/// use tinio_core::PartsListing;
///
/// let page = PartsListing {
///     parts: vec![],
///     truncated: false,
///     next_part_number_marker: None,
/// };
/// assert!(!page.truncated);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartsListing {
    /// Part metadata in part-number order.
    pub parts: Vec<PartInfo>,
    /// Whether more parts exist after this page.
    pub truncated: bool,
    /// Resume marker for the next page.
    pub next_part_number_marker: Option<u32>,
}

/// Parameters of a [`MultipartOps::list_multipart_uploads`] call.
///
/// # Examples
///
/// ```rust
/// use tinio_core::{bucket, ListUploadsParams};
///
/// let params = ListUploadsParams {
///     bucket: bucket::name("data").unwrap(),
///     prefix: "big".into(),
///     delimiter: None,
///     key_marker: None,
///     upload_id_marker: None,
///     max_uploads: 1000,
/// };
/// assert_eq!(params.prefix, "big");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListUploadsParams {
    /// Bucket of the uploads.
    pub bucket: bucket::Name,
    /// Only uploads whose key starts with this prefix are returned.
    pub prefix: String,
    /// Group keys after the delimiter into common prefixes.
    pub delimiter: Option<String>,
    /// Resume after this key (exclusive).
    pub key_marker: Option<String>,
    /// Resume after this upload id — refines `key_marker` to a position
    /// inside a same-key group (S3 `upload-id-marker`).
    pub upload_id_marker: Option<String>,
    /// Maximum number of uploads per page (default 1000).
    pub max_uploads: usize,
}

/// One page of a multipart-upload listing.
///
/// # Examples
///
/// ```rust
/// use tinio_core::UploadsListing;
///
/// let page = UploadsListing {
///     uploads: vec![],
///     common_prefixes: vec![],
///     truncated: false,
///     next_key_marker: None,
///     next_upload_id_marker: None,
/// };
/// assert!(page.uploads.is_empty());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadsListing {
    /// In-progress uploads in key order.
    pub uploads: Vec<MultipartUpload>,
    /// Rolled-up prefixes (with the delimiter appended).
    pub common_prefixes: Vec<String>,
    /// Whether more uploads exist after this page.
    pub truncated: bool,
    /// Resume marker for the next page.
    pub next_key_marker: Option<String>,
    /// The upload-id half of the resume marker (paired with
    /// `next_key_marker` to resume inside a same-key group).
    pub next_upload_id_marker: Option<String>,
}

/// Multipart operations of the storage contract.
///
/// # Examples
///
/// The category traits are only callable on a complete backend — the
/// methods are bound by `Self: Storage`:
///
/// ```rust
/// use tinio_core::storage::{MultipartOps, Storage};
///
/// // Multipart operations are callable on any complete backend.
/// fn needs_multipart_ops<S: MultipartOps + Storage>() {}
/// ```
#[async_trait]
pub trait MultipartOps: Send + Sync + 'static {
    /// Start a multipart upload; returns the upload state with a fresh
    /// upload id.
    async fn create_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<MultipartUpload, <Self as Storage>::Error>
    where
        Self: Storage;

    /// Upload one part (number 1..=10000). `NoSuchUpload` when the upload
    /// does not exist.
    async fn upload_part(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        part_number: crate::multipart::PartNumber,
        body: BodyStream,
    ) -> Result<PartInfo, <Self as Storage>::Error>
    where
        Self: Storage;

    /// List the parts of an upload.
    async fn list_parts(
        &self,
        params: ListPartsParams,
    ) -> Result<PartsListing, <Self as Storage>::Error>
    where
        Self: Storage;

    /// Assemble the listed parts into the final object (streaming, atomic).
    ///
    /// `parts` is the client's `CompleteMultipartUpload` list: strictly
    /// ascending numbers, each ETag matching the stored part. Extra stored
    /// parts that are not listed are discarded. Empty `parts` is [`super::Error::NoParts`].
    /// A missing / mismatched / out-of-order part is [`super::Error::InvalidPart`].
    /// Returns the composed object metadata (ETag `MD5-of-MD5s-N`, FR-022).
    async fn complete_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        parts: &[CompletedPart],
    ) -> Result<object::Info, <Self as Storage>::Error>
    where
        Self: Storage;

    /// Abort an upload and remove its parts.
    async fn abort_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<(), <Self as Storage>::Error>
    where
        Self: Storage;

    /// List in-progress uploads of a bucket.
    async fn list_multipart_uploads(
        &self,
        params: ListUploadsParams,
    ) -> Result<UploadsListing, <Self as Storage>::Error>
    where
        Self: Storage;
}
