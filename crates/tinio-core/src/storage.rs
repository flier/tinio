//! The async storage contract.
//!
//! The extension seam of tinio (task T012): every backend (`tinio-fs` today;
//! `tinio-s3`, `tinio-webdav` planned) implements [`Storage`], and the S3
//! mapping layer speaks only to this contract — no backend code.
//!
//! The interface is split by operation category — [`BucketOps`],
//! [`ObjectOps`], [`MultipartOps`] — and aggregated by [`Storage`], so
//! implementations and consumers can group by concern. Category methods
//! reference the aggregate's associated error (`<Self as Storage>::Error`),
//! declared once on [`Storage`]; it must convert into the contract error
//! [`Error`] for the mapping layer and the conformance harness.
//!
//! Methods are `async fn` via the `async_trait` macro — every returned future is
//! `Send`, which the s3s/hyper hosting layers require. The contract is used
//! generically (`S: Storage`), not as `dyn Storage`: the associated error
//! type and the category-method bounds (`where Self: Storage`) make it
//! dyn-incompatible by design. Bodies flow as [`BodyStream`] — a
//! `Send` stream of `bytes::Bytes` chunks — so neither side ever buffers a
//! whole object (constitution V). Bucket names and object keys arrive
//! pre-validated as [`crate::bucket::Name`] / [`crate::object::Key`] (untrusted input MUST go
//! through their checked constructors before the contract is called).

use std::{
    error::Error as StdError,
    io,
    num::ParseIntError,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};

use crate::{
    bucket::{self, Bucket},
    etag::{self, ETag},
    multipart::{CompletedPart, MultipartUpload, PartInfo},
    object,
};

/// A backend-agnostic storage failure.
///
/// All backend operations report failures with this type. It is split into
/// two not-found variants on purpose: the S3 mapping layer must distinguish
/// a missing bucket (`NoSuchBucket`) from a missing object (`NoSuchKey`),
/// and backends can naturally tell them apart (e.g. by which path component
/// is absent).
///
/// # Examples
///
/// ```rust
/// use tinio_core::storage::{self, Error::*};
///
/// let err = NoSuchBucket("data".into());
/// assert_eq!(err.to_string(), "no such bucket: `data`");
///
/// // I/O errors convert into the domain error transparently.
/// let io_err: storage::Error =
///     std::io::Error::new(std::io::ErrorKind::NotFound, "gone").into();
/// assert!(matches!(io_err, Io(_)));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The referenced bucket does not exist.
    #[error("no such bucket: `{0}`")]
    NoSuchBucket(bucket::Name),
    /// The referenced object (key) does not exist.
    #[error("no such object: `{0}`")]
    NoSuchKey(object::Key),
    /// The referenced multipart upload does not exist.
    #[error("no such multipart upload: `{0}`")]
    NoSuchUpload(String),
    /// The entity already exists (e.g. bucket creation on an existing name).
    #[error("already exists: `{0}`")]
    AlreadyExists(bucket::Name),
    /// The bucket still contains objects and cannot be deleted.
    #[error("bucket is not empty: `{0}`")]
    NotEmpty(bucket::Name),
    /// The object key violates the universal validation rules (traversal,
    /// absolute path, control characters — FR-006). The payload is the
    /// rejected input — it cannot be [`object::Key`].
    #[error("invalid key: `{0}`")]
    InvalidKey(String),
    /// The bucket name violates the S3 naming rules (FR-012). The payload
    /// is the rejected input — it cannot be [`bucket::Name`].
    #[error("invalid bucket name: `{0}`")]
    InvalidBucketName(String),
    /// Stored or wire-format ETag could not be parsed.
    #[error("invalid etag: {0}")]
    InvalidETag(#[from] etag::Error),
    /// Part number outside `1..=10000`.
    #[error("invalid part number: {0}")]
    InvalidPartNumber(u32),
    /// Complete listed a part that is missing, out of order, or whose ETag
    /// does not match the stored part.
    #[error("invalid part: {0}")]
    InvalidPart(u32),
    /// Complete called with no parts uploaded.
    #[error("no parts uploaded")]
    NoParts,
    /// A multipart part-key suffix is not a `u32`.
    #[error("invalid part key: {0}")]
    InvalidPartKey(#[from] ParseIntError),
    /// A byte range cannot be satisfied (mapped to `InvalidRange`, HTTP 416).
    #[error("invalid byte range: requested {range:?} on object of {size} bytes")]
    InvalidRange {
        /// The requested range.
        range: ByteRange,
        /// Size of the object in bytes.
        size: u64,
    },
    /// The operation is refused (reserved `.tinio` segment — FR-020;
    /// read-only mode — FR-023).
    #[error("access denied: `{0}`")]
    AccessDenied(object::Key),
    /// A backend I/O failure; the underlying error is preserved.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

impl Error {
    /// The referenced bucket does not exist.
    #[inline]
    pub fn no_such_bucket(name: &bucket::Name) -> Self {
        Self::NoSuchBucket(name.clone())
    }

    /// The referenced object (key) does not exist.
    #[inline]
    pub fn no_such_key(key: &object::Key) -> Self {
        Self::NoSuchKey(key.clone())
    }

    /// The referenced multipart upload does not exist.
    #[inline]
    pub fn no_such_upload(upload_id: &str) -> Self {
        Self::NoSuchUpload(upload_id.into())
    }

    /// The entity already exists (e.g. bucket creation on an existing name).
    #[inline]
    pub fn already_exists(name: &bucket::Name) -> Self {
        Self::AlreadyExists(name.clone())
    }

    /// The bucket still contains objects and cannot be deleted.
    #[inline]
    pub fn not_empty(name: &bucket::Name) -> Self {
        Self::NotEmpty(name.clone())
    }

    /// Invalid object key (rejected input — it cannot be [`object::Key`]).
    #[inline]
    pub fn invalid_key(raw: String) -> Self {
        Self::InvalidKey(raw)
    }

    /// Invalid bucket name (rejected input — it cannot be [`bucket::Name`]).
    #[inline]
    pub fn invalid_bucket_name(raw: String) -> Self {
        Self::InvalidBucketName(raw)
    }

    /// Stored or wire-format ETag could not be parsed.
    #[inline]
    pub fn invalid_etag(err: etag::Error) -> Self {
        Self::InvalidETag(err)
    }

    /// Part number outside `1..=10000`.
    #[inline]
    pub fn invalid_part_number(part_number: u32) -> Self {
        Self::InvalidPartNumber(part_number)
    }

    /// Complete listed a missing, out-of-order, or ETag-mismatched part.
    #[inline]
    pub fn invalid_part(part_number: u32) -> Self {
        Self::InvalidPart(part_number)
    }

    /// Complete called with no parts uploaded.
    #[inline]
    pub fn no_parts() -> Self {
        Self::NoParts
    }

    /// A multipart part-key suffix is not a `u32`.
    #[inline]
    pub fn invalid_part_key(err: ParseIntError) -> Self {
        Self::InvalidPartKey(err)
    }

    /// A byte range cannot be satisfied (the S3 mapping layer answers 416).
    #[inline]
    pub fn invalid_range(range: ByteRange, size: u64) -> Self {
        Self::InvalidRange { range, size }
    }

    /// The operation is refused (reserved `.tinio` segment or read-only mode).
    #[inline]
    pub fn access_denied(key: &object::Key) -> Self {
        Self::AccessDenied(key.clone())
    }

    /// A backend I/O failure; the underlying error is preserved.
    #[inline]
    pub fn io(err: io::Error) -> Self {
        Self::Io(err)
    }
}

/// A `Send` stream of body chunks.
///
/// Upload bodies (put/part) and download bodies (get) flow through this
/// type. Chunks are `bytes::Bytes`, so both sides can stream with bounded
/// buffers and zero-copy chunk sharing.
///
/// # Examples
///
/// ```rust
/// use futures::stream;
/// use tinio_core::BodyStream;
///
/// let body: BodyStream = Box::pin(stream::empty());
/// ```
pub type BodyStream = BoxStream<'static, io::Result<Bytes>>;

/// Drain a [`BodyStream`] into an owned buffer.
///
/// The contract streams bodies, but backends that materialize uploads (the
/// in-memory backend) collect them via this helper.
pub async fn collect_body(mut body: BodyStream) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    while let Some(chunk) = body.next().await {
        out.extend_from_slice(&chunk?);
    }
    Ok(out)
}

/// Unix time in nanoseconds (stored backend timestamps; `0` on a pre-epoch
/// clock).
pub fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Convert a stored nanosecond timestamp back into a [`SystemTime`].
pub fn from_nanos(n: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_nanos(n)
}

/// A byte range for partial reads (the S3 `Range` header semantics).
///
/// # Examples
///
/// ```rust
/// use tinio_core::ByteRange;
///
/// // bytes=0-1023
/// let range = ByteRange::Inclusive(0, 1023);
/// // bytes=1024- (open-ended)
/// let from = ByteRange::From(1024);
/// // bytes=-512 (last 512 bytes)
/// let suffix = ByteRange::Suffix(512);
///
/// assert_ne!(range, from);
/// assert_eq!(suffix, ByteRange::Suffix(512));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteRange {
    /// `bytes=N-` — from byte N to the end of the object.
    From(u64),
    /// `bytes=A-B` — the inclusive range A..=B.
    Inclusive(u64, u64),
    /// `bytes=-N` — the last N bytes of the object.
    Suffix(u64),
}

impl ByteRange {
    /// Resolve this range against an object of `size` bytes into the
    /// inclusive `(start, end)` slice `[start..=end]`.
    ///
    /// Open-ended and suffix ranges clamp to the object; a range whose
    /// start exceeds the end after clamping, or any range on a zero-byte
    /// object, is [`Error::InvalidRange`] (the S3 mapping layer answers 416
    /// per AWS).
    pub fn resolve(self, size: u64) -> Result<(u64, u64), Error> {
        if size == 0 {
            return Err(Error::invalid_range(self, size));
        }
        let (start, end) = match self {
            ByteRange::From(s) => (s, size.saturating_sub(1)),
            ByteRange::Inclusive(s, e) => (s, e.min(size.saturating_sub(1))),
            ByteRange::Suffix(n) => (size.saturating_sub(n), size.saturating_sub(1)),
        };
        if start > end {
            return Err(Error::invalid_range(self, size));
        }
        Ok((start, end))
    }
}

/// Result of a successful object write.
///
/// # Examples
///
/// ```rust
/// use tinio_core::{ETag, PutObjectResult};
///
/// let result = PutObjectResult {
///     etag: ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
/// };
/// assert!(result.etag.as_str().starts_with("d41d8"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutObjectResult {
    /// ETag of the stored object (content MD5 hex, or the composed
    /// `"<md5hex>-N"` form for multipart).
    pub etag: ETag,
}

/// Result of a successful object read.
///
/// `served_range` is `Some((start, end))` (inclusive) when the response is a
/// partial read (HTTP 206), `None` for a full read (HTTP 200).
///
/// # Examples
///
/// ```rust
/// use tinio_core::{ETag, object, GetObjectResult};
///
/// let result = GetObjectResult {
///     info: object::Info {
///         key: object::key("a.txt").unwrap(),
///         size: 5,
///         last_modified: std::time::SystemTime::UNIX_EPOCH,
///         etag: ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
///     },
///     body: Box::pin(futures::stream::empty()),
///     served_range: Some((0, 4)),
/// };
/// assert_eq!(result.served_range, Some((0, 4)));
/// ```
pub struct GetObjectResult {
    /// Metadata of the served object.
    pub info: object::Info,
    /// The object body (full or partial per `served_range`).
    pub body: BodyStream,
    /// Inclusive byte range actually served; `None` = full object.
    pub served_range: Option<(u64, u64)>,
}

impl std::fmt::Debug for GetObjectResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GetObjectResult")
            .field("info", &self.info)
            .field("served_range", &self.served_range)
            .field("body", &"<stream>")
            .finish()
    }
}

/// Parameters of a [`ObjectOps::list_objects`] call — the S3 listing
/// semantics (prefix filtering, delimiter grouping, pagination).
///
/// # Examples
///
/// ```rust
/// use tinio_core::{bucket, ListObjectsParams};
///
/// let params = ListObjectsParams {
///     bucket: bucket::name("data").unwrap(),
///     prefix: "dir/".into(),
///     delimiter: Some("/".into()),
///     start_after: Some("dir/b.txt".into()),
///     max_keys: 100,
/// };
/// assert_eq!(params.max_keys, 100);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListObjectsParams {
    /// Bucket to list.
    pub bucket: bucket::Name,
    /// Only keys starting with this prefix are returned.
    pub prefix: String,
    /// Group keys after the delimiter into common prefixes (e.g. `"/"`).
    pub delimiter: Option<String>,
    /// Resume the listing after this key (exclusive).
    pub start_after: Option<String>,
    /// Maximum number of results per page (default 1000).
    pub max_keys: usize,
}

/// One page of a listing.
///
/// # Examples
///
/// ```rust
/// use tinio_core::ObjectListing;
///
/// let page = ObjectListing {
///     objects: vec![],
///     common_prefixes: vec!["dir/".into()],
///     truncated: true,
///     next_start_after: Some("dir/z.txt".into()),
/// };
/// assert_eq!(page.common_prefixes, ["dir/"]);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectListing {
    /// Object metadata in key order (lexicographic, S3 semantics).
    pub objects: Vec<object::Info>,
    /// Rolled-up prefixes (with the delimiter appended).
    pub common_prefixes: Vec<String>,
    /// Whether more results exist after this page.
    pub truncated: bool,
    /// Resume marker for the next page (`start_after` of the next call).
    pub next_start_after: Option<String>,
}

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
}

/// Group, filter, and paginate an already key-sorted item list — the shared
/// S3 listing engine for both object and multipart-upload listings.
///
/// `items` must be ordered by key (backends sort their scans); key-sorted
/// input makes delimiter rollups contiguous, so common prefixes deduplicate
/// against the last one in O(1). Keys strictly after `marker` (exclusive)
/// are kept, then the page is truncated to `max`. Returns the page, the
/// rolled-up prefixes, the truncation flag, and the resume marker (the last
/// key of the page when truncated).
///
/// # Examples
///
/// ```rust
/// use tinio_core::storage::group_and_paginate;
///
/// let items = vec!["a.txt".to_string(), "dir/x".to_string()];
/// let (keys, prefixes, truncated, next) = group_and_paginate(
///     items,
///     "",
///     Some("/"),
///     None,
///     1000,
///     |k| k.as_str(),
/// );
/// assert_eq!(keys, ["a.txt"]);
/// assert_eq!(prefixes, ["dir/"]);
/// assert!(!truncated);
/// assert_eq!(next, None);
/// ```
pub fn group_and_paginate<T>(
    items: Vec<T>,
    prefix: &str,
    delimiter: Option<&str>,
    marker: Option<&str>,
    max: usize,
    key_of: impl Fn(&T) -> &str,
) -> (Vec<T>, Vec<String>, bool, Option<String>) {
    // Merge objects and common prefixes into one lexicographic stream.
    // S3 `MaxKeys` counts both; the continuation token is the last returned
    // entry (object key or prefix), exclusive for the next page.
    let mut entries: Vec<(String, Option<T>)> = Vec::new();
    if let Some(delim) = delimiter {
        let mut prefixes = Vec::new();
        for item in items {
            let key = key_of(&item).to_string();
            if let Some(rest) = key.strip_prefix(prefix)
                && let Some((head, _)) = rest.split_once(delim)
            {
                let cp = format!("{prefix}{head}{delim}");
                if prefixes.last().map(String::as_str) != Some(cp.as_str()) {
                    prefixes.push(cp);
                }
                continue;
            }
            entries.push((key, Some(item)));
        }
        entries.extend(prefixes.into_iter().map(|cp| (cp, None)));
        entries.sort_by(|a, b| a.0.cmp(&b.0));
    } else {
        for item in items {
            let key = key_of(&item).to_string();
            entries.push((key, Some(item)));
        }
    }
    if let Some(after) = marker {
        entries.retain(|(key, _)| key.as_str() > after);
    }
    let truncated = entries.len() > max;
    let next = if truncated {
        entries
            .get(max.saturating_sub(1).min(entries.len().saturating_sub(1)))
            .map(|(key, _)| key.clone())
    } else {
        None
    };
    entries.truncate(max);
    let mut keys = Vec::new();
    let mut common_prefixes = Vec::new();
    for (key, item) in entries {
        match item {
            Some(item) => keys.push(item),
            None => common_prefixes.push(key),
        }
    }
    (keys, common_prefixes, truncated, next)
}

/// Bucket operations of the storage contract.
///
/// Implementations MUST reject invalid bucket names with
/// [`Error::InvalidBucketName`] **before any filesystem access**
/// (FR-012 — names are pre-validated by [`bucket::name`]; the check is a
/// defensive backstop), and report a missing bucket as
/// [`Error::NoSuchBucket`] — the S3 mapping layer relies on it.
///
/// # Examples
///
/// The category traits are only callable on a complete backend — the
/// methods are bound by `Self: Storage`:
///
/// ```rust
/// use tinio_core::storage::{BucketOps, Storage};
///
/// // Bucket operations are callable on any complete backend.
/// fn needs_bucket_ops<S: BucketOps + Storage>() {}
/// ```
#[async_trait]
pub trait BucketOps: Send + Sync + 'static {
    /// Create a bucket. `AlreadyExists` when the name is taken.
    async fn create_bucket(&self, name: &bucket::Name) -> Result<(), <Self as Storage>::Error>
    where
        Self: Storage;

    /// Delete an empty bucket. `NoSuchBucket` when missing, `NotEmpty` when
    /// it still contains objects.
    async fn delete_bucket(&self, name: &bucket::Name) -> Result<(), <Self as Storage>::Error>
    where
        Self: Storage;

    /// Bucket metadata; `NoSuchBucket` when missing.
    async fn head_bucket(&self, name: &bucket::Name) -> Result<Bucket, <Self as Storage>::Error>
    where
        Self: Storage;

    /// All buckets, in name order.
    async fn list_buckets(&self) -> Result<Vec<Bucket>, <Self as Storage>::Error>
    where
        Self: Storage;
}

/// Object operations of the storage contract.
///
/// Implementations MUST reject invalid keys with
/// [`Error::InvalidKey`] **before any filesystem access** (FR-006 —
/// keys are pre-validated by [`object::key`]; the check is a defensive
/// backstop), refuse writes whose key is reserved (`.tinio` segment,
/// FR-020) with [`Error::AccessDenied`], and implement folder-marker
/// semantics: a key ending in `/` is never an object (put creates a
/// directory, get/head report `NoSuchKey`). A missing bucket is
/// [`Error::NoSuchBucket`], a missing object [`Error::NoSuchKey`].
///
/// # Examples
///
/// The category traits are only callable on a complete backend — the
/// methods are bound by `Self: Storage`:
///
/// ```rust
/// use tinio_core::storage::{ObjectOps, Storage};
///
/// // Object operations are callable on any complete backend.
/// fn needs_object_ops<S: ObjectOps + Storage>() {}
/// ```
#[async_trait]
pub trait ObjectOps: Send + Sync + 'static {
    /// Stream an object body into storage (atomic on the backend side —
    /// last completed write wins, never a torn object, FR-011).
    async fn put_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        body: BodyStream,
    ) -> Result<PutObjectResult, <Self as Storage>::Error>
    where
        Self: Storage;

    /// Stream an object body out, optionally partial (Range semantics).
    async fn get_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        range: Option<ByteRange>,
    ) -> Result<GetObjectResult, <Self as Storage>::Error>
    where
        Self: Storage;

    /// Object metadata; `NoSuchKey` when missing.
    async fn head_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<object::Info, <Self as Storage>::Error>
    where
        Self: Storage;

    /// Delete an object. S3 semantics: idempotent — missing objects are Ok.
    async fn delete_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<(), <Self as Storage>::Error>
    where
        Self: Storage;

    /// List objects with prefix filtering, delimiter grouping, and
    /// pagination (S3 semantics).
    async fn list_objects(
        &self,
        params: ListObjectsParams,
    ) -> Result<ObjectListing, <Self as Storage>::Error>
    where
        Self: Storage;
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
    /// parts that are not listed are discarded. Empty `parts` is [`Error::NoParts`].
    /// A missing / mismatched / out-of-order part is [`Error::InvalidPart`].
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

/// The storage backend contract: the aggregation of [`BucketOps`],
/// [`ObjectOps`], and [`MultipartOps`].
///
/// Implementations implement the three categories and declare the shared
/// error type once, on the aggregate. The category methods reference
/// `<Self as Storage>::Error`, so they are only usable on a complete
/// backend (`S: Storage`).
///
/// The conformance harness (`tinio_core::testing`, behind the `testing`
/// feature) verifies the behavioral contract; every backend must pass it.
///
/// # Examples
///
/// Implement the three categories, then declare the error type on the
/// aggregate (see `tinio_mem::MemoryStorage` for a complete reference
/// implementation):
///
/// ```ignore
/// impl BucketOps for X { ... }
/// impl ObjectOps for X { ... }
/// impl MultipartOps for X { ... }
/// impl Storage for X { type Error = MyError; }
///
/// use tinio_core::bucket;
/// use tinio_mem::MemoryStorage;
///
/// let storage = MemoryStorage::new().unwrap();
/// let bucket = bucket::name("data").unwrap();
/// let buckets = tokio::runtime::Runtime::new()
///     .unwrap()
///     .block_on(async {
///         storage.create_bucket(&bucket).await.unwrap();
///         storage.list_buckets().await.unwrap()
///     });
/// assert_eq!(buckets.len(), 1);
/// ```
///
/// (`ignore`: a runnable example would require dev-depending on a backend
/// crate, which would create a dependency cycle with `tinio-mem`.)
pub trait Storage: Send + Sync + 'static + BucketOps + ObjectOps + MultipartOps {
    /// The backend error type, shared across all operation categories.
    ///
    /// It must convert into the contract error [`Error`] so the S3 mapping
    /// layer and the conformance harness can translate any backend failure.
    type Error: StdError + Send + Sync + 'static + Into<crate::storage::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{etag::ETag, object, testing::assert_send_sync};
    use std::time::SystemTime;

    #[test]
    fn byte_range_variants() {
        assert_eq!(ByteRange::From(0), ByteRange::From(0));
        assert_eq!(ByteRange::Inclusive(1, 10), ByteRange::Inclusive(1, 10));
        assert_eq!(ByteRange::Suffix(100), ByteRange::Suffix(100));
        assert_ne!(ByteRange::From(0), ByteRange::Inclusive(0, 0));
    }

    #[test]
    fn listing_types_construct() {
        let info = object::Info {
            key: object::key("a.txt").unwrap(),
            size: 1,
            last_modified: SystemTime::UNIX_EPOCH,
            etag: ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
        };
        let listing = ObjectListing {
            objects: vec![info],
            common_prefixes: vec!["dir/".into()],
            truncated: true,
            next_start_after: Some("a.txt".into()),
        };
        assert_eq!(listing.objects.len(), 1);
        assert_eq!(listing.common_prefixes, ["dir/"]);
        assert!(listing.truncated);
        assert_eq!(listing.next_start_after.as_deref(), Some("a.txt"));
    }

    #[test]
    fn put_and_get_results_construct() {
        let put = PutObjectResult {
            etag: ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
        };
        assert_eq!(put.etag.as_str(), "d41d8cd98f00b204e9800998ecf8427e");

        let get = GetObjectResult {
            info: object::Info {
                key: object::key("a.txt").unwrap(),
                size: 0,
                last_modified: SystemTime::UNIX_EPOCH,
                etag: ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
            },
            body: Box::pin(futures::stream::empty()),
            served_range: Some((0, 0)),
        };
        assert_eq!(get.served_range, Some((0, 0)));
    }

    #[test]
    fn displays_variants() {
        let cases = [
            (
                Error::NoSuchBucket("my-bucket".into()),
                "no such bucket: `my-bucket`",
            ),
            (
                Error::NoSuchKey("dir/file.txt".into()),
                "no such object: `dir/file.txt`",
            ),
            (
                Error::NoSuchUpload("abc-123".into()),
                "no such multipart upload: `abc-123`",
            ),
            (
                Error::AlreadyExists("my-bucket".into()),
                "already exists: `my-bucket`",
            ),
            (
                Error::NotEmpty("my-bucket".into()),
                "bucket is not empty: `my-bucket`",
            ),
            (
                Error::InvalidKey("../evil".into()),
                "invalid key: `../evil`",
            ),
            (
                Error::InvalidBucketName("Bad_Name".into()),
                "invalid bucket name: `Bad_Name`",
            ),
            (
                Error::InvalidETag(crate::etag::Error::InvalidFormat),
                "invalid etag: invalid ETag format",
            ),
            (Error::InvalidPartNumber(0), "invalid part number: 0"),
            (Error::InvalidPart(2), "invalid part: 2"),
            (Error::NoParts, "no parts uploaded"),
            (
                Error::InvalidRange {
                    range: ByteRange::From(10),
                    size: 5,
                },
                "invalid byte range: requested From(10) on object of 5 bytes",
            ),
            (
                Error::AccessDenied("a/.tinio/b".into()),
                "access denied: `a/.tinio/b`",
            ),
            (
                Error::Io(io::Error::from(io::ErrorKind::NotFound)),
                "I/O error: entity not found",
            ),
        ];
        for (err, expected) in cases {
            assert_eq!(err.to_string(), expected);
        }

        let src = "x".parse::<u32>().unwrap_err();
        assert_eq!(
            Error::InvalidPartKey(src.clone()).to_string(),
            format!("invalid part key: {src}")
        );
    }

    #[test]
    fn original_errors_convert_with_from() {
        let err = Error::from(io::Error::other("boom"));
        assert!(matches!(err, Error::Io(_)));

        let err = Error::from(crate::etag::Error::InvalidFormat);
        assert!(matches!(err, Error::InvalidETag(_)));

        let src = "x".parse::<u32>().unwrap_err();
        let err = Error::from(src.clone());
        assert!(matches!(err, Error::InvalidPartKey(_)));
        assert_eq!(
            err.source().map(ToString::to_string).as_deref(),
            Some(src.to_string().as_str())
        );
    }

    #[test]
    fn errors_are_send_sync_and_static() {
        assert_send_sync::<Error>();
    }

    #[test]
    fn byte_range_resolve_clamps_and_rejects_unsatisfiable() {
        assert_eq!(ByteRange::Inclusive(8, 99).resolve(10).unwrap(), (8, 9));
        assert_eq!(ByteRange::Suffix(100).resolve(10).unwrap(), (0, 9));
        assert_eq!(ByteRange::From(0).resolve(10).unwrap(), (0, 9));
        assert!(ByteRange::From(10).resolve(10).is_err());
        assert!(ByteRange::Suffix(0).resolve(10).is_err());
        assert!(ByteRange::Inclusive(0, 0).resolve(0).is_err());
    }

    #[test]
    fn delimiter_listing_counts_prefixes_toward_max_and_paginates() {
        let items = ["a.txt", "b.txt", "dir/c.txt"].map(str::to_string).to_vec();
        let (keys, prefixes, truncated, next) =
            group_and_paginate(items.clone(), "", Some("/"), None, 1, String::as_str);
        assert_eq!(keys, ["a.txt"]);
        assert!(
            prefixes.is_empty(),
            "common prefixes must not leak onto every page: {prefixes:?}"
        );
        assert!(truncated);
        assert_eq!(next.as_deref(), Some("a.txt"));

        let (keys, prefixes, truncated, next) = group_and_paginate(
            items.clone(),
            "",
            Some("/"),
            Some("a.txt"),
            1,
            String::as_str,
        );
        assert_eq!(keys, ["b.txt"]);
        assert!(prefixes.is_empty());
        assert!(truncated);
        assert_eq!(next.as_deref(), Some("b.txt"));

        let (keys, prefixes, truncated, next) =
            group_and_paginate(items, "", Some("/"), Some("b.txt"), 1, String::as_str);
        assert!(keys.is_empty());
        assert_eq!(prefixes, ["dir/"]);
        assert!(!truncated);
        assert_eq!(next, None);
    }

    #[test]
    fn get_object_result_debug_redacts_body() {
        use futures::stream;
        let info = object::Info {
            key: object::key("a.txt").unwrap(),
            size: 1,
            last_modified: SystemTime::UNIX_EPOCH,
            etag: ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
        };
        let result = GetObjectResult {
            info,
            body: Box::pin(stream::empty()),
            served_range: None,
        };
        let debug = format!("{result:?}");
        assert!(debug.contains("<stream>"), "{debug}");
    }

    #[test]
    fn error_constructor_helpers() {
        assert!(matches!(
            Error::invalid_key("../evil".into()),
            Error::InvalidKey(_)
        ));
        assert!(matches!(
            Error::invalid_bucket_name("Bad_Name".into()),
            Error::InvalidBucketName(_)
        ));
        assert!(matches!(
            Error::invalid_etag(crate::etag::Error::InvalidFormat),
            Error::InvalidETag(_)
        ));
    }
}
