//! The [`BucketOps`] contract category.

use async_trait::async_trait;

use super::Storage;
use crate::{
    bucket::{self, Bucket},
    object,
};

/// Parameters of a [`BucketOps::list_buckets`] call — the S3 listing
/// semantics (prefix filtering, pagination).
///
/// The page size is permissive (like `ListObjectsParams`): `max_buckets =
/// 0` requests an empty page — the `< 1` rejection is a wire-level policy
/// of the S3 mapping layer.
///
/// # Examples
///
/// ```rust
/// use tinio_core::ListBucketsParams;
///
/// let params = ListBucketsParams {
///     prefix: "data".into(),
///     start_after: Some("data-b".into()),
///     max_buckets: 100,
/// };
/// assert_eq!(params.max_buckets, 100);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListBucketsParams {
    /// Only buckets whose name starts with this prefix are returned.
    pub prefix: String,
    /// Resume the listing after this bucket name (exclusive).
    pub start_after: Option<String>,
    /// Maximum number of buckets per page (default 10_000 at the mapping).
    pub max_buckets: usize,
}

/// One page of a [`BucketOps::list_buckets`] listing.
///
/// # Examples
///
/// ```rust
/// use tinio_core::BucketsListing;
///
/// let page = BucketsListing {
///     buckets: vec![],
///     truncated: true,
///     next_start_after: Some("zeta".into()),
/// };
/// assert!(page.truncated);
/// assert_eq!(page.next_start_after.as_deref(), Some("zeta"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketsListing {
    /// Bucket metadata in name order (lexicographic, S3 semantics).
    pub buckets: Vec<Bucket>,
    /// Whether more results exist after this page.
    pub truncated: bool,
    /// Resume marker for the next page (`start_after` of the next call).
    pub next_start_after: Option<String>,
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

    /// List buckets, in name order, per S3 listing semantics: only names
    /// starting with `params.prefix`, a page of at most
    /// `params.max_buckets` entries resuming after `params.start_after`
    /// (exclusive). `truncated` + `next_start_after` mark a page with
    /// more results (`max_buckets = 0` requests an empty, untruncated
    /// page — strictness is a mapping-layer policy).
    async fn list_buckets(
        &self,
        params: ListBucketsParams,
    ) -> Result<BucketsListing, <Self as Storage>::Error>
    where
        Self: Storage;

    /// The bucket's tag set (S3 GetBucketTagging). `NoSuchBucket` when
    /// the bucket is missing.
    async fn get_bucket_tags(
        &self,
        name: &bucket::Name,
    ) -> Result<object::Tags, <Self as Storage>::Error>
    where
        Self: Storage;

    /// Replace the bucket's tag set (S3 PutBucketTagging — replace-all,
    /// no merge). `NoSuchBucket` when the bucket is missing.
    async fn put_bucket_tags(
        &self,
        name: &bucket::Name,
        tags: &object::Tags,
    ) -> Result<(), <Self as Storage>::Error>
    where
        Self: Storage;

    /// Remove the bucket's tag set (S3 DeleteBucketTagging). S3
    /// semantics: idempotent — a missing bucket is Ok.
    async fn delete_bucket_tags(&self, name: &bucket::Name) -> Result<(), <Self as Storage>::Error>
    where
        Self: Storage;
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use super::*;
    use crate::bucket;

    #[test]
    fn buckets_types_construct() {
        // Mirrors the object listing's `listing_types_construct`: the
        // pagination types are plain data; `max_buckets = 0` is the
        // contract's empty-page request (strictness is wire-level).
        let params = ListBucketsParams {
            prefix: "data".into(),
            start_after: Some("data-a".into()),
            max_buckets: 100,
        };
        assert_eq!(params.max_buckets, 100);
        let listing = BucketsListing {
            buckets: vec![Bucket {
                name: bucket::name("data").unwrap(),
                creation_time: SystemTime::UNIX_EPOCH,
            }],
            truncated: true,
            next_start_after: Some("zeta".into()),
        };
        assert_eq!(listing.buckets.len(), 1);
        assert!(listing.truncated);
        assert_eq!(listing.next_start_after.as_deref(), Some("zeta"));
    }
}
