//! Storage backend contract for the tinio S3 server.
//!
//! This crate is the extension seam of the project: it defines the
//! backend-agnostic domain errors, the async `Storage` contract, the `Cleanup`
//! contract, the generic task-pipeline contract, and key validation — all
//! without any HTTP or filesystem implementation. Concrete backends (tinio-fs
//! is the v1 one) implement the contract and must pass the conformance test
//! harness behind the `testing` feature of `tinio-util`.
//!
//! Contract domain types: [`Bucket`] and [`bucket::Name`], [`object::Key`]
//! and [`object::Info`], [`ETag`], [`MultipartUpload`] and [`PartInfo`].
//! The newtypes carry validation in [`object::key`],
//! [`bucket::name`], and [`ETag::new`]: untrusted input MUST go
//! through the checked constructors before any backend is called.

pub mod bucket;
pub mod cleanup;
pub mod etag;
pub mod multipart;
pub mod object;
pub mod pipeline;
pub mod storage;

pub use self::bucket::Bucket;
pub use self::etag::ETag;
pub use self::multipart::{CompletedPart, MultipartUpload, PartInfo, PartNumber};
pub use self::storage::{
    BodyStream, BucketOps, ByteRange, GetObjectResult, ListObjectsParams, ListPartsParams,
    ListUploadsParams, MultipartOps, ObjectListing, ObjectOps, PartsListing, PutObjectResult,
    Storage, UploadsListing, WRITE_LOCK_BUCKET_BOUNDS_US, WRITE_LOCK_BUCKETS, collect_body,
    from_nanos, group_and_paginate, group_and_paginate_ordered, group_and_paginate_unordered,
    key_marker_order, now_nanos, paginate_ordered, split_uploads_order, to_nanos, uploads_order,
};
