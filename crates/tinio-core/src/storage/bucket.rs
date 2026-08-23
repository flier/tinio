//! The [`BucketOps`] contract category.

use async_trait::async_trait;

use crate::bucket::{self, Bucket};

use super::Storage;

/// Bucket operations of the storage contract.
///
/// Implementations MUST reject invalid bucket names with
/// [`super::Error::InvalidBucketName`] **before any filesystem access**
/// (FR-012 — names are pre-validated by [`bucket::name`]; the check is a
/// defensive backstop), and report a missing bucket as
/// [`super::Error::NoSuchBucket`] — the S3 mapping layer relies on it.
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
