//! The S3 protocol mapping layer (tasks T047–T050).
//!
//! [`S3Backend`] implements the s3s `S3` trait over the `tinio-core`
//! storage contract — the s3s framework handles routing, XML, error codes,
//! and (later) SigV4 verification; this module maps the ~30 implemented
//! operations onto [`Storage`], translating backend errors into S3 error
//! codes. The operation groups live in `buckets.rs`, `objects.rs`,
//! `listing.rs`, and `multipart.rs` as inherent methods; the `S3` impl in
//! this file delegates to them.
//!
//! Capability groups are strippable at compile time (`multipart`, `copy`,
//! `list-v1`, `list-v2` cargo features) and disableable at runtime
//! ([`Capabilities`], from the `[s3]` config section) — disabled groups
//! answer `NotImplemented` (FR-021).

pub(crate) mod buckets;
pub(crate) mod listing;
pub(crate) mod multipart;
pub(crate) mod objects;
#[cfg(test)]
pub(crate) mod testutil;

use std::{collections::HashMap, io, sync::Arc, time::SystemTime};

use async_trait::async_trait;
use futures::TryStreamExt;
use s3s::{S3, S3Error, S3Request, S3Response, S3Result, dto, s3_error};
use tinio_core::{
    BodyStream, ETag, bucket, object,
    storage::{Error as StorageError, Storage},
};

/// Runtime capability toggles of the `[s3]` config section (FR-021).
///
/// # Examples
///
/// ```rust
/// use tinio_server::backend::Capabilities;
///
/// let caps = Capabilities::default();
/// assert!(caps.multipart && caps.copy_object);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capabilities {
    /// Multipart operations + UploadPartCopy.
    pub multipart: bool,
    /// CopyObject.
    pub copy_object: bool,
    /// ListObjects (V1).
    pub list_objects_v1: bool,
    /// ListObjectsV2.
    pub list_objects_v2: bool,
    /// DeleteObjects (batch).
    pub delete_objects: bool,
}

impl Default for Capabilities {
    fn default() -> Self {
        Self {
            multipart: true,
            copy_object: true,
            list_objects_v1: true,
            list_objects_v2: true,
            delete_objects: true,
        }
    }
}

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
    /// exclusivity — without stalling unrelated keys.
    pub(crate) conditional_put_locks:
        Arc<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
}

/// Held per-object lock for a conditional PUT. Evicts the map slot when
/// this is the last handle, so the table does not grow without bound.
pub(crate) struct ObjectLock {
    id: String,
    slot: Option<Arc<tokio::sync::Mutex<()>>>,
    map: Arc<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for ObjectLock {
    fn drop(&mut self) {
        // Release the mutex first so a waiter can proceed, then evict
        // the map slot only if we were the last handle.
        drop(self.guard.take());
        let Some(slot) = self.slot.take() else {
            return;
        };
        if Arc::strong_count(&slot) != 2 {
            return;
        }
        let Ok(mut map) = self.map.try_lock() else {
            return;
        };
        if Arc::strong_count(&slot) == 2 {
            map.remove(&self.id);
        }
    }
}

impl<S: Storage> S3Backend<S> {
    /// Construct the mapping over `storage` with the given toggles.
    pub fn new(storage: S, caps: Capabilities) -> Self {
        Self {
            storage: Arc::new(storage),
            caps,
            conditional_put_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
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

    /// Per-object lock for conditional PUT (RFC 7232 exclusivity).
    pub(crate) async fn lock_object(&self, bucket: &bucket::Name, key: &object::Key) -> ObjectLock {
        let id = format!("{bucket}/{key}");
        let map = Arc::clone(&self.conditional_put_locks);
        let slot = {
            let mut map = map.lock().await;
            map.entry(id.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let guard = Arc::clone(&slot).lock_owned().await;
        ObjectLock {
            id,
            slot: Some(slot),
            map,
            guard: Some(guard),
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

    /// A response timestamp back into a [`SystemTime`] (conditional-header
    /// comparison).
    pub(crate) fn to_system_time(t: dto::Timestamp) -> SystemTime {
        time::OffsetDateTime::from(t).into()
    }

    /// Evaluate the conditional headers against the object's ETag and
    /// mtime, reporting the failing one.
    fn eval_conditions(
        etag: &ETag,
        last_modified: SystemTime,
        if_match: Option<&dto::IfMatch>,
        if_none_match: Option<&dto::IfNoneMatch>,
        if_modified_since: Option<dto::IfModifiedSince>,
        if_unmodified_since: Option<dto::IfUnmodifiedSince>,
    ) -> Result<(), ConditionFailure> {
        let wire = etag.as_str();
        if let Some(cond) = if_match {
            let ok = cond.is_any()
                || cond
                    .as_etag()
                    .map(|e| e.strong_cmp(&dto::ETag::Strong(wire.to_string())))
                    .unwrap_or(false);
            if !ok {
                return Err(ConditionFailure::Match);
            }
        }
        if let Some(cond) = if_none_match {
            let matched = cond.is_any()
                || cond
                    .as_etag()
                    .map(|e| e.weak_cmp(&dto::ETag::Strong(wire.to_string())))
                    .unwrap_or(false);
            if matched {
                return Err(ConditionFailure::NoneMatch);
            }
        }
        if if_none_match.is_none()
            && let Some(since) = if_modified_since
            && last_modified <= Self::to_system_time(since)
        {
            return Err(ConditionFailure::ModifiedSince);
        }
        // RFC 9110 §13.1.4: If-Match takes precedence — the date header
        // is ignored while it is present.
        if if_match.is_none()
            && let Some(since) = if_unmodified_since
            && last_modified > Self::to_system_time(since)
        {
            return Err(ConditionFailure::UnmodifiedSince);
        }
        Ok(())
    }

    /// Evaluate conditional headers against the object's ETag and mtime
    /// (RFC 7232). The read path answers 304 for If-None-Match /
    /// If-Modified-Since and 412 for If-Match / If-Unmodified-Since; the
    /// write path answers 412 for every failure (never 304).
    pub(crate) fn check_conditions(
        etag: &ETag,
        last_modified: SystemTime,
        if_match: Option<&dto::IfMatch>,
        if_none_match: Option<&dto::IfNoneMatch>,
        if_modified_since: Option<dto::IfModifiedSince>,
        if_unmodified_since: Option<dto::IfUnmodifiedSince>,
        write_path: bool,
    ) -> S3Result<()> {
        Self::eval_conditions(
            etag,
            last_modified,
            if_match,
            if_none_match,
            if_modified_since,
            if_unmodified_since,
        )
        .map_err(|fail| condition_error(fail, write_path))
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

/// The conditional header whose evaluation failed (the read and write
/// paths map them to different S3 error codes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConditionFailure {
    Match,
    NoneMatch,
    ModifiedSince,
    UnmodifiedSince,
}

/// Map a failed condition onto its S3 error: the write path always
/// answers `412`; the read path answers `304` for the not-modified
/// conditions (RFC 7232).
fn condition_error(fail: ConditionFailure, write_path: bool) -> S3Error {
    if !write_path
        && matches!(
            fail,
            ConditionFailure::NoneMatch | ConditionFailure::ModifiedSince
        )
    {
        return s3_error!(NotModified, "not modified");
    }
    let message = match fail {
        ConditionFailure::Match => "If-Match failed",
        ConditionFailure::NoneMatch => "If-None-Match matched",
        ConditionFailure::ModifiedSince => "not modified since",
        ConditionFailure::UnmodifiedSince => "If-Unmodified-Since failed",
    };
    s3_error!(PreconditionFailed, "{message}")
}

/// Map a backend error (any `S::Error`, which converts into the contract
/// error) onto its S3 error code (FR-005).
pub(crate) fn map_backend_error<E: Into<StorageError>>(err: E) -> S3Error {
    match err.into() {
        StorageError::NoSuchBucket(_) => s3_error!(NoSuchBucket),
        StorageError::NoSuchKey(_) => s3_error!(NoSuchKey),
        StorageError::NoSuchUpload(id) => s3_error!(NoSuchUpload, "no such upload: {id}"),
        // A duplicate create on a locally-owned bucket answers
        // `BucketAlreadyOwnedByYou` (AWS/MinIO semantics) — clients such as
        // rclone treat this as the idempotent-create case and continue.
        StorageError::AlreadyExists(_) => s3_error!(BucketAlreadyOwnedByYou),
        StorageError::NotEmpty(_) => s3_error!(BucketNotEmpty),
        StorageError::InvalidKey(key) => s3_error!(InvalidArgument, "invalid object key: {key}"),
        StorageError::InvalidBucketName(name) => {
            s3_error!(InvalidBucketName, "invalid bucket name: {name}")
        }
        StorageError::InvalidETag(_) => s3_error!(InvalidArgument, "invalid ETag"),
        StorageError::InvalidPartNumber(n) => {
            s3_error!(InvalidArgument, "invalid part number: {n}")
        }
        StorageError::InvalidPart(n) => s3_error!(InvalidPart, "invalid part: {n}"),
        StorageError::NoParts => s3_error!(InvalidRequest, "no parts uploaded"),
        StorageError::InvalidPartKey(_) => s3_error!(InvalidArgument, "invalid part key"),
        StorageError::InvalidRange { .. } => s3_error!(InvalidRange),
        StorageError::AccessDenied(_) => s3_error!(AccessDenied),
        StorageError::Io(err) => s3_error!(InternalError, "storage I/O error: {err}"),
    }
}

#[async_trait]
impl<S: Storage> S3 for S3Backend<S> {
    // --- buckets (T047) ---
    async fn create_bucket(
        &self,
        req: S3Request<dto::CreateBucketInput>,
    ) -> S3Result<S3Response<dto::CreateBucketOutput>> {
        self.op_create_bucket(req).await
    }

    async fn delete_bucket(
        &self,
        req: S3Request<dto::DeleteBucketInput>,
    ) -> S3Result<S3Response<dto::DeleteBucketOutput>> {
        self.op_delete_bucket(req).await
    }

    async fn head_bucket(
        &self,
        req: S3Request<dto::HeadBucketInput>,
    ) -> S3Result<S3Response<dto::HeadBucketOutput>> {
        self.op_head_bucket(req).await
    }

    async fn list_buckets(
        &self,
        req: S3Request<dto::ListBucketsInput>,
    ) -> S3Result<S3Response<dto::ListBucketsOutput>> {
        self.op_list_buckets(req).await
    }

    async fn get_bucket_location(
        &self,
        req: S3Request<dto::GetBucketLocationInput>,
    ) -> S3Result<S3Response<dto::GetBucketLocationOutput>> {
        self.op_get_bucket_location(req).await
    }

    // --- objects + copy (T048) ---
    async fn put_object(
        &self,
        req: S3Request<dto::PutObjectInput>,
    ) -> S3Result<S3Response<dto::PutObjectOutput>> {
        self.op_put_object(req).await
    }

    async fn get_object(
        &self,
        req: S3Request<dto::GetObjectInput>,
    ) -> S3Result<S3Response<dto::GetObjectOutput>> {
        self.op_get_object(req).await
    }

    async fn head_object(
        &self,
        req: S3Request<dto::HeadObjectInput>,
    ) -> S3Result<S3Response<dto::HeadObjectOutput>> {
        self.op_head_object(req).await
    }

    async fn delete_object(
        &self,
        req: S3Request<dto::DeleteObjectInput>,
    ) -> S3Result<S3Response<dto::DeleteObjectOutput>> {
        self.op_delete_object(req).await
    }

    async fn delete_objects(
        &self,
        req: S3Request<dto::DeleteObjectsInput>,
    ) -> S3Result<S3Response<dto::DeleteObjectsOutput>> {
        self.op_delete_objects(req).await
    }

    async fn get_object_tagging(
        &self,
        req: S3Request<dto::GetObjectTaggingInput>,
    ) -> S3Result<S3Response<dto::GetObjectTaggingOutput>> {
        self.op_get_object_tagging(req).await
    }

    #[cfg(feature = "copy")]
    async fn copy_object(
        &self,
        req: S3Request<dto::CopyObjectInput>,
    ) -> S3Result<S3Response<dto::CopyObjectOutput>> {
        self.op_copy_object(req).await
    }

    // --- listing (T049) ---
    #[cfg(feature = "list-v1")]
    async fn list_objects(
        &self,
        req: S3Request<dto::ListObjectsInput>,
    ) -> S3Result<S3Response<dto::ListObjectsOutput>> {
        self.op_list_objects(req).await
    }

    #[cfg(feature = "list-v2")]
    async fn list_objects_v2(
        &self,
        req: S3Request<dto::ListObjectsV2Input>,
    ) -> S3Result<S3Response<dto::ListObjectsV2Output>> {
        self.op_list_objects_v2(req).await
    }

    // --- multipart (T050) ---
    #[cfg(feature = "multipart")]
    async fn create_multipart_upload(
        &self,
        req: S3Request<dto::CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::CreateMultipartUploadOutput>> {
        self.op_create_multipart_upload(req).await
    }

    #[cfg(feature = "multipart")]
    async fn upload_part(
        &self,
        req: S3Request<dto::UploadPartInput>,
    ) -> S3Result<S3Response<dto::UploadPartOutput>> {
        self.op_upload_part(req).await
    }

    #[cfg(all(feature = "multipart", feature = "copy"))]
    async fn upload_part_copy(
        &self,
        req: S3Request<dto::UploadPartCopyInput>,
    ) -> S3Result<S3Response<dto::UploadPartCopyOutput>> {
        self.op_upload_part_copy(req).await
    }

    #[cfg(feature = "multipart")]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<dto::CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::CompleteMultipartUploadOutput>> {
        self.op_complete_multipart_upload(req).await
    }

    #[cfg(feature = "multipart")]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<dto::AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::AbortMultipartUploadOutput>> {
        self.op_abort_multipart_upload(req).await
    }

    #[cfg(feature = "multipart")]
    async fn list_parts(
        &self,
        req: S3Request<dto::ListPartsInput>,
    ) -> S3Result<S3Response<dto::ListPartsOutput>> {
        self.op_list_parts(req).await
    }

    #[cfg(feature = "multipart")]
    async fn list_multipart_uploads(
        &self,
        req: S3Request<dto::ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<dto::ListMultipartUploadsOutput>> {
        self.op_list_multipart_uploads(req).await
    }
}
