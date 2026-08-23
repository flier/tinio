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

mod body;
mod bucket;
mod error;
mod listing;
mod multipart;
mod object;
mod range;
mod time;

use std::error::Error as StdError;

pub use body::{BodyStream, collect_body};
pub use bucket::BucketOps;
pub use error::{
    Error, access_denied, already_exists, invalid_bucket_name, invalid_etag, invalid_key,
    invalid_part, invalid_part_key, invalid_part_number, invalid_range, io, no_parts,
    no_such_bucket, no_such_key, no_such_upload, not_empty,
};
pub use listing::group_and_paginate;
pub use multipart::{
    ListPartsParams, ListUploadsParams, MultipartOps, PartsListing, UploadsListing,
};
pub use object::{GetObjectResult, ListObjectsParams, ObjectListing, ObjectOps, PutObjectResult};
pub use range::ByteRange;
pub use time::{from_nanos, now_nanos};

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
