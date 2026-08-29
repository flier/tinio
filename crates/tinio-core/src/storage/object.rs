//! Object read/write types and the [`ObjectOps`] contract category.

use async_trait::async_trait;
use derive_more::Debug;

use crate::{bucket, etag::ETag, object};

use super::{Storage, body::BodyStream, range::ByteRange};

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
#[derive(Debug)]
pub struct GetObjectResult {
    /// Metadata of the served object.
    pub info: object::Info,
    /// The object body (full or partial per `served_range`).
    #[debug("<stream>")]
    pub body: BodyStream,
    /// Inclusive byte range actually served; `None` = full object.
    pub served_range: Option<(u64, u64)>,
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

/// Object operations of the storage contract.
///
/// Implementations MUST reject invalid keys with
/// [`super::Error::InvalidKey`] **before any filesystem access** (FR-006 —
/// keys are pre-validated by [`object::key`]; the check is a defensive
/// backstop), refuse writes whose key is reserved (`.tinio` segment,
/// FR-020) with [`super::Error::AccessDenied`], and implement folder-marker
/// semantics: a key ending in `/` is never an object (put creates a
/// directory, get/head report `NoSuchKey`). A missing bucket is
/// [`super::Error::NoSuchBucket`], a missing object [`super::Error::NoSuchKey`].
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
    /// A request body staged for a later [`ObjectOps::commit_object`] —
    /// backend-specific (a temp file, an in-memory buffer, ...).
    type StagedBody: Send + Sync + 'static;

    /// Stream an object body into storage (atomic on the backend side —
    /// last completed write wins, never a torn object, FR-011). The
    /// default implementation is [`ObjectOps::stage_body`] followed by
    /// [`ObjectOps::commit_object`].
    async fn put_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        body: BodyStream,
    ) -> Result<PutObjectResult, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        let staged = self.stage_body(bucket, key, body).await?;
        self.commit_object(bucket, key, staged).await
    }

    /// The streaming phase of a write: buffer `body` outside the
    /// backend's write locks, so a slow client never stalls other
    /// writers. The stage is cheap to discard — the body is published
    /// only by the later [`ObjectOps::commit_object`]. Validation that can
    /// fail before any body is read (bucket, key) still rejects here.
    async fn stage_body(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        body: BodyStream,
    ) -> Result<Self::StagedBody, <Self as Storage>::Error>
    where
        Self: Storage;

    /// The mutation phase of a write: atomically publish a staged body
    /// onto `key` (atomic on the backend side — last completed write
    /// wins, never a torn object, FR-011). Re-checks everything the
    /// stage checked, under the backend's mutation lock, so the commit
    /// is safe against concurrent bucket deletion.
    async fn commit_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        staged: Self::StagedBody,
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

    /// Server-side copy of `src` into `dst` (S3 CopyObject): the source
    /// content is stored under `dst` atomically (FR-011), and the
    /// source's metadata is NOT carried over — a copy is a fresh object
    /// (its mtime is the copy time; its ETag is the content's). The
    /// default implementation streams the source through the body
    /// contract (get → put); a backend may override with a
    /// filesystem-level copy (same filesystem, zero userspace
    /// buffering) and may reuse the source's ETag for a full copy of a
    /// single-form source (the content MD5 is unchanged by a copy).
    /// `NoSuchKey` when the source does not exist; `NoSuchBucket` when
    /// either bucket does not.
    async fn copy_object(
        &self,
        src_bucket: &bucket::Name,
        src_key: &object::Key,
        dst_bucket: &bucket::Name,
        dst_key: &object::Key,
    ) -> Result<PutObjectResult, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        let get = self.get_object(src_bucket, src_key, None).await?;
        self.put_object(dst_bucket, dst_key, get.body).await
    }

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

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use futures::stream;

    use super::*;

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
            body: Box::pin(stream::empty()),
            served_range: Some((0, 0)),
        };
        assert_eq!(get.served_range, Some((0, 0)));
    }

    #[test]
    fn get_object_result_debug_redacts_body() {
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
}
