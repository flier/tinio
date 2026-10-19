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
//! and [`object::Info`], [`ETag`], [`MultipartUpload`] and [`PartInfo`],
//! [`checksum::Algorithm`] and [`checksum::Type`].
//! The newtypes carry validation in [`object::key`],
//! [`bucket::name`], and [`ETag::new`]: untrusted input MUST go
//! through the checked constructors before any backend is called.

#[cfg(test)]
extern crate tinio_util as _util;

pub mod acl;
pub mod blob;
pub mod bucket;
pub mod checksum;
pub mod cleanup;
pub mod content;
pub mod cors;
pub mod etag;
pub mod lifecycle;
pub mod meta;
pub mod mode;
pub mod multipart;
pub mod object;
pub mod percent;
pub mod pipeline;
pub mod severity;
pub mod storage;
pub mod version;

// The contract types' hand-written PostgreSQL mapping, behind the crate's
// `sqlx` feature (see the module docs). Named `sql_types`, never `sqlx`: a
// crate-root `mod sqlx` would make `sqlx::...` paths inside this crate
// resolve to the module instead of the `sqlx` crate.
#[cfg(feature = "sqlx")]
mod sql_types;

pub use self::{
    blob::BlockHash,
    bucket::Bucket,
    etag::ETag,
    mode::AccessMode,
    multipart::{CompletedPart, MultipartUpload, PartInfo, PartNumber},
    storage::{
        BodyStream, BucketOps, BucketsListing, ByteRange, GetObjectResult, ListBucketsParams,
        ListObjectsParams, ListPartsParams, ListUploadsParams, MultipartOps, ObjectListing,
        ObjectOps, PartsListing, PutObjectResult, Storage, TagWriteResult, UnorderedPager,
        UploadsListing, WRITE_LOCK_BUCKET_BOUNDS_US, WRITE_LOCK_BUCKETS, collect_body, from_nanos,
        group_and_paginate, group_and_paginate_ordered, group_and_paginate_unordered,
        key_marker_order, now_nanos, paginate_ordered, split_uploads_order, to_nanos,
        uploads_order,
    },
    version::{Id, State},
};
