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
    Error, access_denied, already_exists, entity_too_large, invalid_bucket_name, invalid_etag,
    invalid_key, invalid_part, invalid_part_key, invalid_part_number, invalid_range, io, no_parts,
    no_such_bucket, no_such_key, no_such_upload, not_empty, part_too_small, too_many_uploads,
};
pub use listing::{
    common_prefix, group_and_paginate, group_and_paginate_ordered, group_and_paginate_unordered,
    key_marker_order, paginate_ordered, split_uploads_order, uploads_order,
};
pub use multipart::{
    ListPartsParams, ListUploadsParams, MultipartOps, PartsListing, UploadsListing,
};
pub use object::{GetObjectResult, ListObjectsParams, ObjectListing, ObjectOps, PutObjectResult};
pub use range::ByteRange;
pub use time::{from_nanos, now_nanos, to_nanos};

/// The write-lock histogram bucket upper bounds, microseconds
/// (pipeline-spec.md §4): `<10, <100, <1k, <5k, <20k, <100k, >100k`. A
/// duration `d` lands in bucket `i` where `bounds[i-1] <= d < bounds[i]`
/// (bucket 0: `d < 10 µs`); the open last bucket holds `d >= 100 000 µs`.
/// One home for the histogram spec, shared by the fs backend's bucketing
/// ([`crate::storage`] consumer `tinio-fs` — `write_lock_bucket`) and the
/// server's prometheus families (the `le=` bounds are positional with
/// the buckets, so the two consumers can never drift apart).
pub const WRITE_LOCK_BUCKET_BOUNDS_US: [u64; 6] = [10, 100, 1_000, 5_000, 20_000, 100_000];

/// The write-lock histogram bucket count: one per bound plus the open
/// overflow bucket (`>100k µs`).
pub const WRITE_LOCK_BUCKETS: usize = WRITE_LOCK_BUCKET_BOUNDS_US.len() + 1;

/// The storage backend contract: the aggregation of [`BucketOps`],
/// [`ObjectOps`], and [`MultipartOps`].
///
/// Implementations implement the three categories and declare the shared
/// error type once, on the aggregate. The category methods reference
/// `<Self as Storage>::Error`, so they are only usable on a complete
/// backend (`S: Storage`).
///
/// The conformance harness (`tinio_util::testing`, behind the `testing`
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
/// use tokio::runtime::Runtime;
///
/// let storage = MemoryStorage::new().unwrap();
/// let bucket = bucket::name("data").unwrap();
/// let buckets = Runtime::new()
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
    type Error: StdError + Send + Sync + 'static + Into<Error>;
}

/// The default symlink policy of the filesystem backend: `false` = reject
/// access resolving through a link and exclude link entries from listings
/// (secure default — a link inside a bucket cannot escape the storage
/// root). Shared by the `[storage.fs]` config schema and `tinio-fs`
/// `FsOptions`, so the two defaults cannot drift.
pub const DEFAULT_FOLLOW_SYMLINKS: bool = false;

/// The default compact trigger of the filesystem backend: the state
/// database is compacted at startup when its fragmentation reaches this
/// percentage. Shared by the `[storage.fs]` config schema and `tinio-fs`
/// `FsOptions` (contracts/config.md is the prose home).
pub const DEFAULT_COMPACT_THRESHOLD_PERCENT: u8 = 20;

/// The validation bounds of the compact trigger (`[storage.fs]
/// compact_threshold_percent`, 5..=90 — meta-redb-spec Q2). Shared by the
/// config schema and `FsOptions` so the two validations cannot drift.
pub const COMPACT_THRESHOLD_MIN_PERCENT: u8 = 5;
pub const COMPACT_THRESHOLD_MAX_PERCENT: u8 = 90;

/// The default meta-batch entry-count threshold of the filesystem backend
/// (`[storage.fs] meta_batch_size`): the cold list/scanner producers flush
/// one write-pipeline batch once it holds this many entries. The knee of
/// the task-2.5 `set_batch` benchmark (pipeline-spec.md Q6) — shared by
/// the config schema and `FsOptions`, so the two defaults cannot drift.
pub const DEFAULT_META_BATCH_SIZE: u16 = 128;

/// The validation bounds of the meta-batch entry count (`[storage.fs]
/// meta_batch_size`, 1..=4096). Shared by the config schema and `FsOptions`
/// so the two validations cannot drift.
pub const META_BATCH_SIZE_MIN: u16 = 1;
pub const META_BATCH_SIZE_MAX: u16 = 4096;

/// The default meta-batch byte threshold of the filesystem backend
/// (`[storage.fs] meta_batch_bytes`): the producers flush once the
/// estimated batch size (≈ 56 B + key length per entry, pipeline-spec.md
/// Q5) reaches this. 262144 = 256 KiB, calibrated with `meta_batch_size`
/// by the task-2.5 benchmark — shared by the config schema and `FsOptions`.
pub const DEFAULT_META_BATCH_BYTES: u32 = 262144;

/// The validation bounds of the meta-batch byte threshold (`[storage.fs]
/// meta_batch_bytes`, 1024..=16 MiB). Shared by the config schema and
/// `FsOptions` so the two validations cannot drift.
pub const META_BATCH_BYTES_MIN: u32 = 1024;
pub const META_BATCH_BYTES_MAX: u32 = 16 * 1024 * 1024;

/// The default cap on concurrently in-progress multipart uploads
/// (`[s3] max_concurrent_uploads`): an authenticated client can otherwise
/// accumulate an unbounded number of uploads (each up to 10,000 parts),
/// exhausting disk, inodes, and metadata rows. Shared by the `[s3]` config
/// schema and the filesystem backend so the two defaults cannot drift.
pub const DEFAULT_MAX_CONCURRENT_UPLOADS: u32 = 1000;
