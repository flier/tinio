//! The S3 protocol mapping layer (tasks T047–T050).
//!
//! [`S3Backend`] implements the s3s `S3` trait over the `tinio-core`
//! storage contract — the s3s framework handles routing, XML, error codes,
//! and (later) SigV4 verification; this module maps the ~30 implemented
//! operations onto [`Storage`], translating backend errors into S3 error
//! codes. The operation groups live in `buckets.rs`, `objects.rs`,
//! `listing.rs`, and `multipart.rs` as inherent methods; the `S3` impl in
//! `s3.rs` delegates to them.
//!
//! Capability groups are strippable at compile time (`multipart`, `copy`,
//! `list-v1`, `list-v2` cargo features) and disableable at runtime
//! ([`Capabilities`], from the `[s3]` config section) — disabled groups
//! answer `NotImplemented` (FR-021).

mod capabilities;
mod conditions;
mod errors;
mod locks;
mod s3;

pub(crate) mod buckets;
pub(crate) mod listing;
pub(crate) mod multipart;
pub(crate) mod objects;
#[cfg(test)]
pub(crate) mod testutil;

pub use capabilities::Capabilities;
pub(crate) use conditions::{ConditionFailure, ConditionalHeaders, condition_error};
pub(crate) use errors::map_backend_error;

use std::{io, sync::Arc, time::SystemTime};

use futures::TryStreamExt;
use s3s::{S3Error, S3Result, dto, s3_error};
use tinio_core::{
    BodyStream, ETag, bucket, object,
    storage::{Error as StorageError, Storage},
};
use tinio_util::lockmap;

/// The S3 mapping over one [`Storage`] backend.
///
/// # Examples
///
/// ```rust
/// use s3s::{S3, S3Request, dto};
/// use tinio_core::bucket;
/// use tinio_core::storage::BucketOps;
/// use tinio_mem::MemoryStorage;
/// use tinio_server::backend::S3Backend;
///
/// fn request(input: dto::CreateBucketInput) -> S3Request<dto::CreateBucketInput> {
///     S3Request {
///         input,
///         method: http::Method::PUT,
///         uri: http::Uri::default(),
///         headers: http::HeaderMap::new(),
///         extensions: http::Extensions::new(),
///         credentials: None,
///         region: None,
///         service: None,
///         trailing_headers: None,
///     }
/// }
///
/// let storage = MemoryStorage::new().unwrap();
/// let backend = S3Backend::new(storage, Default::default());
/// let result = tokio::runtime::Runtime::new().unwrap().block_on(async {
///     backend
///         .create_bucket(request(dto::CreateBucketInput {
///             bucket: "data".into(),
///             ..Default::default()
///         }))
///         .await
///         .unwrap()
///         .output
///         .location
///         .unwrap()
/// });
/// assert_eq!(result, "/data");
/// ```
#[derive(Debug, Clone)]
pub struct S3Backend<S: Storage> {
    /// The storage backend all operations map onto.
    pub(crate) storage: Arc<S>,
    /// Runtime capability toggles.
    pub(crate) caps: Capabilities,
    /// Serializes writes per object: a conditional put's head-check and
    /// commit are one critical section for that key against every other
    /// writer (put, copy, multipart complete, delete) — RFC 7232
    /// exclusivity — without stalling unrelated keys (see
    /// [`lockmap::Map`] for the eviction semantics).
    pub(crate) conditional_put_locks: lockmap::Map<String>,
}

impl<S: Storage> S3Backend<S> {
    /// Construct the mapping over `storage` with the given toggles.
    pub fn new(storage: S, caps: Capabilities) -> Self {
        Self {
            storage: Arc::new(storage),
            caps,
            conditional_put_locks: lockmap::Map::new(),
        }
    }

    /// The storage backend (for direct contract access in tests/harness).
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// The capability toggles.
    pub fn capabilities(&self) -> Capabilities {
        self.caps
    }

    /// Runtime capability gate (FR-021): disabled groups answer
    /// `NotImplemented`.
    pub(crate) fn require_cap(enabled: bool, name: &'static str) -> S3Result<()> {
        if enabled {
            Ok(())
        } else {
            Err(s3_error!(NotImplemented, "{name} is disabled"))
        }
    }

    /// The bucket of a request input, validated (FR-012).
    pub(crate) fn bucket(&self, raw: String) -> Result<bucket::Name, S3Error> {
        bucket::name(raw).map_err(|err| match err {
            StorageError::InvalidBucketName(name) => {
                s3_error!(InvalidBucketName, "invalid bucket name: {name}")
            }
            _ => s3_error!(InvalidArgument),
        })
    }

    /// The key of a request input, validated (FR-006).
    pub(crate) fn key(&self, raw: String) -> Result<object::Key, S3Error> {
        object::key(raw).map_err(|_| s3_error!(InvalidArgument, "invalid object key"))
    }

    /// A `StreamingBlob` request body into the contract's [`BodyStream`].
    pub(crate) fn stream_in(body: Option<dto::StreamingBlob>) -> BodyStream {
        match body {
            Some(body) => Box::pin(body.map_err(io::Error::other)),
            None => Box::pin(futures::stream::empty()),
        }
    }

    /// The contract's [`BodyStream`] into a response `StreamingBlob`.
    pub(crate) fn stream_out(body: BodyStream) -> dto::StreamingBlob {
        dto::StreamingBlob::wrap(body)
    }

    /// The wire ETag of a contract ETag (the framework emits the quotes).
    pub(crate) fn etag_wire(etag: &ETag) -> dto::ETag {
        dto::ETag::Strong(etag.as_str())
    }

    /// The response `LastModified` timestamp of a [`SystemTime`].
    pub(crate) fn last_modified(t: SystemTime) -> dto::LastModified {
        dto::LastModified::from(t)
    }

    /// The source of a `CopyObject`/`UploadPartCopy` request into a
    /// (bucket, key) pair (the framework parses the header into
    /// [`dto::CopySource`]).
    #[cfg(feature = "copy")]
    pub(crate) fn copy_source(
        &self,
        source: &dto::CopySource,
    ) -> Result<(bucket::Name, object::Key), S3Error> {
        match source {
            dto::CopySource::Bucket { bucket, key, .. } => {
                Ok((self.bucket(bucket.to_string())?, self.key(key.to_string())?))
            }
            _ => Err(s3_error!(InvalidArgument, "unsupported copy source")),
        }
    }

    /// The inferred Content-Type of a key (mime_guess; fallback
    /// `application/octet-stream`, FR-022).
    pub(crate) fn content_type(key: &str) -> String {
        mime_guess::from_path(key)
            .first_or_octet_stream()
            .essence_str()
            .to_string()
    }
}
