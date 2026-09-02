//! Multipart upload types and the [`MultipartOps`] contract category.

use std::sync::Arc;

use async_trait::async_trait;

use super::{Storage, body::BodyStream, range::ByteRange};
use crate::{
    bucket, checksum,
    multipart::{CompletedPart, MultipartUpload, PartInfo, PartNumber},
    object,
};

/// Parameters of a [`MultipartOps::list_parts`] call.
///
/// # Examples
///
/// ```rust
/// use tinio_core::{ListPartsParams, bucket, object};
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
/// use tinio_core::{ListUploadsParams, bucket};
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
    /// upload id. `checksum` is the create-time checksum spec
    /// (persisted; echoed by `get_multipart_upload`/`list_multipart_uploads`).
    async fn create_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        checksum: Option<checksum::Upload>,
    ) -> Result<MultipartUpload, <Self as Storage>::Error>
    where
        Self: Storage;

    /// The persisted upload state (create-time checksum algorithm/type
    /// included). `NoSuchUpload` when the upload does not exist or the
    /// key does not match.
    async fn get_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<MultipartUpload, <Self as Storage>::Error>
    where
        Self: Storage;

    /// Upload one part (number 1..=10000). `checksum` is the server's
    /// tee slot (spec 2026-08-31): the backend persists its digest in
    /// the SAME transaction as the part row when present — a re-upload
    /// overwrites both atomically, so no CAS is needed — and clears the
    /// checksum row when absent (a re-uploaded part must not keep a
    /// stale value). The backends never hash; the slot's `etag` cell
    /// also supplies the part ETag. `NoSuchUpload` when the upload does
    /// not exist.
    async fn upload_part(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        part_number: PartNumber,
        body: BodyStream,
        checksum: Option<Arc<checksum::PartChecksum>>,
    ) -> Result<PartInfo, <Self as Storage>::Error>
    where
        Self: Storage;

    /// Server-side copy of `src` (optionally a byte range) into the part
    /// `part_number` of the upload `upload_id` (S3 UploadPartCopy). The
    /// default implementation streams the source through the body
    /// contract (get range → upload part); a backend may override with a
    /// filesystem-level copy. A part's ETag is always the content MD5 of
    /// the part bytes (the range, when present). The copy path carries
    /// no client checksum (R1) — no tee slot.
    #[allow(clippy::too_many_arguments)]
    async fn copy_part(
        &self,
        src_bucket: &bucket::Name,
        src_key: &object::Key,
        dst_bucket: &bucket::Name,
        dst_key: &object::Key,
        upload_id: &str,
        part_number: PartNumber,
        range: Option<ByteRange>,
    ) -> Result<PartInfo, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        let get = self.get_object(src_bucket, src_key, range).await?;
        self.upload_part(dst_bucket, dst_key, upload_id, part_number, get.body, None)
            .await
    }

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
    /// parts that are not listed are discarded. Empty `parts` is [`Error::NoParts`].
    /// A missing / mismatched / out-of-order part is [`Error::InvalidPart`].
    /// A non-final listed part below [`crate::multipart::MIN_PART_BYTES`] is
    /// [`Error::PartTooSmall`] — the S3 5 MiB rule, enforced authoritatively
    /// here against the part state the commit consumes (a concurrent part
    /// overwrite between the S3 layer's listing and this call is caught
    /// here, not by the pre-check).
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
