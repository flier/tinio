//! Object read/write types and the [`ObjectOps`] contract category.

use std::sync::Arc;

use async_trait::async_trait;
use derive_more::Debug;

use super::{Storage, body::BodyStream, range::ByteRange};
use crate::{bucket, checksum, etag::ETag, multipart::ObjectPart, object};

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
/// use std::time::SystemTime;
///
/// use futures::stream;
/// use tinio_core::{ETag, GetObjectResult, object};
///
/// let result = GetObjectResult {
///     info: object::Info {
///         key: object::key("a.txt").unwrap(),
///         size: 5,
///         last_modified: SystemTime::UNIX_EPOCH,
///         etag: ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
///         tags: object::Tags::empty(),
///         checksum: None,
///     },
///     body: Box::pin(stream::empty()),
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
/// use tinio_core::{ListObjectsParams, bucket};
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
    /// A request body staged for a later [`ObjectOps::commit_object`] —
    /// backend-specific (a temp file, an in-memory buffer, ...).
    type StagedBody: Send + Sync + 'static;

    /// Stream an object body into storage (atomic on the backend side —
    /// last completed write wins, never a torn object, FR-011). The
    /// default implementation is [`ObjectOps::stage_body`] followed by
    /// [`ObjectOps::commit_object`] with empty tags (the tagged write
    /// paths call the pair directly); it returns the committed ETag.
    async fn put_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        body: BodyStream,
    ) -> Result<PutObjectResult, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        let staged = self.stage_body(bucket, key, body, None).await?;
        let info = self
            .commit_object(bucket, key, staged, object::Tags::empty())
            .await?;
        Ok(PutObjectResult { etag: info.etag })
    }

    /// The streaming phase of a write: buffer `body` outside the
    /// backend's write locks, so a slow client never stalls other
    /// writers. The stage is cheap to discard — the body is published
    /// only by the later [`ObjectOps::commit_object`]. Validation that can
    /// fail before any body is read (bucket, key) still rejects here.
    /// `checksum` is the server's tee slot (the
    /// [`crate::storage::MultipartOps::upload_part`] pattern): the
    /// interface wraps the body when the client sent a single
    /// `x-amz-checksum-*` header under the `checksum` toggle, the digest
    /// is computed while the body streams, a mismatch fails the staging
    /// (the multipart path's checksum-mismatch error), and the validated
    /// digest rides into the later [`ObjectOps::commit_object`]; absent,
    /// no digest is computed.
    async fn stage_body(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        body: BodyStream,
        checksum: Option<Arc<checksum::PartChecksum>>,
    ) -> Result<Self::StagedBody, <Self as Storage>::Error>
    where
        Self: Storage;

    /// The mutation phase of a write: atomically publish a staged body
    /// onto `key` (atomic on the backend side — last completed write
    /// wins, never a torn object, FR-011). Re-checks everything the
    /// stage checked, under the backend's mutation lock, so the commit
    /// is safe against concurrent bucket deletion. `tags` — validated by
    /// the interface — and the stage's tee digest (when the stage
    /// carried one) are recorded atomically with the write, with no
    /// post-commit tag window. Returns the committed object metadata.
    async fn commit_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        staged: Self::StagedBody,
        tags: object::Tags,
    ) -> Result<object::Info, <Self as Storage>::Error>
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
    /// content is stored under `dst` atomically (FR-011), and the copy
    /// is a fresh object — its mtime is the copy time and its ETag is
    /// the content's; its metadata is what the caller passes, not what
    /// the source holds. `tags` is the new object's tag set and
    /// `checksum` the recorded checksum carried into its record (the
    /// interface passes the source's recorded value — a full copy's
    /// bytes are the source's — or the directive's replacement; `None`
    /// stores none); a copy never inherits the source's retained parts.
    /// The default implementation streams the source through the body
    /// contract (get → stage → commit, carrying `tags`); a backend may
    /// override with a filesystem-level copy (same filesystem, zero
    /// userspace buffering), carrying `checksum` too and reusing the
    /// source's ETag for a single-form source (the content MD5 is
    /// unchanged by a copy). `NoSuchKey` when the source does not
    /// exist; `NoSuchBucket` when either bucket does not.
    async fn copy_object(
        &self,
        src_bucket: &bucket::Name,
        src_key: &object::Key,
        dst_bucket: &bucket::Name,
        dst_key: &object::Key,
        tags: object::Tags,
        _checksum: Option<checksum::Recorded>,
    ) -> Result<object::Info, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        let get = self.get_object(src_bucket, src_key, None).await?;
        let staged = self.stage_body(dst_bucket, dst_key, get.body, None).await?;
        self.commit_object(dst_bucket, dst_key, staged, tags).await
    }

    /// Atomically move `src` to `dst` (S3 RenameObject): the object's
    /// metadata — mtime, tags, recorded checksum, retained parts —
    /// moves with it; a rename is not a fresh object. An existing `dst`
    /// is overwritten. `NoSuchKey` when `src` is missing; `NoSuchBucket`
    /// when the bucket does not.
    async fn rename_object(
        &self,
        bucket: &bucket::Name,
        src: &object::Key,
        dst: &object::Key,
    ) -> Result<object::Info, <Self as Storage>::Error>
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

    /// The object's tag set (S3 GetObjectTagging). `NoSuchKey` when the
    /// object is missing.
    async fn get_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<object::Tags, <Self as Storage>::Error>
    where
        Self: Storage;

    /// Replace the object's tag set (S3 PutObjectTagging — replace-all,
    /// no merge). `NoSuchKey` when the object is missing.
    async fn put_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<(), <Self as Storage>::Error>
    where
        Self: Storage;

    /// Remove the object's tag set (S3 DeleteObjectTagging). S3
    /// semantics: idempotent — a missing object is Ok.
    async fn delete_object_tags(
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

    /// The retained part rows of a completed multipart object (S3
    /// GetObjectAttributes `ObjectParts`): the parts the object was
    /// composed of at its last multipart completion, in part-number
    /// order, with the stored per-part checksums. Empty for an object
    /// that was not multipart-completed (a plain put or copy has no
    /// parts).
    async fn list_object_parts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Vec<ObjectPart>, <Self as Storage>::Error>
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
            tags: object::Tags::empty(),
            checksum: None,
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
                tags: object::Tags::empty(),
                checksum: None,
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
            tags: object::Tags::empty(),
            checksum: None,
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
