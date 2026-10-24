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
//! `list-v1`, `list-v2`, `cors` cargo features) and disableable at runtime
//! ([`Capabilities`], from the `[s3]` config section) — a disabled group's
//! operations answer `NotImplemented` (FR-021), refused by the
//! [`CapabilityS3`](crate::capability::CapabilityS3) decorator wrapped
//! around this mapping, not by these handlers.
//!
//! **403 ambiguity (the ACL plane, spec 2026-09-05)**: with owners/ACLs
//! enforced, a 403 `AccessDenied` can mean an ACL denial at the access
//! layer (`tinio-auth`'s `S3Access`), an OS-level `PermissionDenied`
//! from the filesystem backend, or the symlink-policy refusal (the
//! `follow_symlinks = false` answer) — the last two reach the same S3
//! code through [`Storage`] contract `Error::AccessDenied`, which
//! `errors::map_backend_error` maps onto `AccessDenied` and logs the
//! storage source at debug. The wire cannot distinguish the cases;
//! debug-level logs are the only forensic tool. **`/metrics` stays
//! outside the ACL check** (grilling ruling): the scrape endpoint is the
//! management plane's token-gated route (`tinio-api`, T075), not a
//! data-plane path, so it never enters the access-layer pipeline — the
//! data-plane listener routes `/metrics` as the bucket named `metrics`
//! like any other first path segment.

#[cfg(feature = "acl")]
use s3s::auth::Credentials;
use tracing::{Span, field};

#[cfg(feature = "acl")]
use crate::_core::acl::Acl;
#[cfg(feature = "acl")]
pub(crate) mod acls;
mod conditions;
#[cfg(feature = "cors")]
pub(crate) mod cors;
mod errors;
#[cfg(feature = "lifecycle")]
pub(crate) mod lifecycle;
mod lock_headers;
mod locks;
pub(crate) use lock_headers::LockHeaders;
#[cfg(feature = "object-lock")]
pub(crate) mod object_lock;
/// The stripped-build runtime refusal test (OL20): compiled only without the
/// feature, so the six operations are absent from the dispatch by construction.
#[cfg(all(test, not(feature = "object-lock")))]
mod object_lock_stripped;
pub(crate) mod ops;
mod s3;
mod versioning;

pub(crate) mod buckets;
pub(crate) mod checksum;
pub(crate) mod listing;
pub(crate) mod multipart;
pub(crate) mod objects;
pub(crate) mod select;
pub(crate) mod tags;
#[cfg(test)]
pub(crate) mod testutil;

#[cfg(test)]
mod tests {
    use std::io;

    use bytes::Bytes;
    use futures::{FutureExt, StreamExt, stream};
    use s3s::S3ErrorCode;

    use super::*;
    use crate::_mem::MemoryStorage;

    fn backend() -> S3Backend<MemoryStorage> {
        S3Backend::new(MemoryStorage::new().unwrap(), Default::default())
    }

    #[test]
    fn capabilities_accessor_returns_the_toggles() {
        let caps = Capabilities {
            multipart: false,
            ..Default::default()
        };
        let backend = S3Backend::new(MemoryStorage::new().unwrap(), caps);
        assert_eq!(backend.capabilities(), caps);
    }

    #[test]
    fn stream_in_wraps_or_empties_a_body() {
        // No body: the handlers get an empty stream, never a panic.
        let mut empty = S3Backend::<MemoryStorage>::stream_in(None);
        assert!(empty.next().now_or_never().unwrap().is_none());
        // A present body is wrapped into the contract's body stream,
        // each chunk surfacing as `io::Result<Bytes>`.
        let stream = stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"x"))]);
        let body = StreamingBlob::wrap(stream);
        let mut streamed = S3Backend::<MemoryStorage>::stream_in(Some(body));
        assert_eq!(
            streamed.next().now_or_never().unwrap().unwrap().unwrap(),
            Bytes::from_static(b"x")
        );
    }

    #[test]
    fn bucket_validates_the_request_input() {
        let backend = backend();
        assert!(backend.bucket("valid-bucket".to_string()).is_ok());
        let err = backend.bucket("UPPER".to_string()).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidBucketName, "{err:?}");
    }

    #[test]
    fn clamp_page_size_zero_cap_is_no_clamp() {
        assert_eq!(clamp_page_size(5, 0), 5);
        assert_eq!(clamp_page_size(10_000, 0), 10_000);
        assert_eq!(clamp_page_size(3, 10_000), 3);
        assert_eq!(clamp_page_size(50_000, 10_000), 10_000);
    }

    #[test]
    fn normalize_page_size_boundary_and_escape_hatch() {
        // Strict (default): < 1 is rejected before any storage call.
        assert!(normalize_page_size(0, "n", false).is_err());
        assert!(normalize_page_size(-1, "n", false).is_err());
        assert_eq!(normalize_page_size(1, "n", false).unwrap(), 1);
        assert_eq!(normalize_page_size(10_000, "n", false).unwrap(), 10_000);
        // Escape hatch: < 1 clamps to the legacy empty page (negatives
        // included — the old `.max(0)`).
        assert_eq!(normalize_page_size(0, "n", true).unwrap(), 0);
        assert_eq!(normalize_page_size(-1, "n", true).unwrap(), 0);
        assert_eq!(normalize_page_size(5, "n", true).unwrap(), 5);
    }
}

use std::{io::Error as IoError, sync::Arc, time::SystemTime};

/// The checksum-spec cache's hard bound — see
/// [`S3Backend::put_checksum_spec`]. Roughly 200-400 bytes per entry
/// (a UUID key + the spec), so this is a few MB worst case; the API
/// abort/complete eviction covers the live-upload paths, this covers
/// the uploads the store-level sweep aborts out from under the backend.
#[cfg(feature = "multipart")]
const CHECKSUM_SPEC_CACHE_CAP: usize = 8192;

#[cfg(feature = "select")]
use std::sync::OnceLock;
#[cfg(feature = "multipart")]
use std::{
    collections::HashMap,
    sync::{Mutex, PoisonError},
};

mod object_lock_cache;
pub(crate) use conditions::{
    ConditionalHeaders, DeleteConditions, check_write_shape, checked_if_match_size, decide_fetch,
    decide_range_error, generation_changed, parse_if_range,
};
#[cfg(feature = "multipart")]
pub(crate) use conditions::{check_complete_conditions, same_whole_second};
#[cfg(feature = "copy")]
pub(crate) use conditions::{parse_etag_condition_header, parse_etag_condition_value};
pub(crate) use errors::map_backend_error;
use futures::{TryStreamExt, stream};
use mime_guess;
pub use object_lock_cache::ObjectLockCache;
#[cfg(feature = "select")]
use rayon::ThreadPool;
use s3s::{
    S3Error, S3Result,
    dto::{self, ETag as WireETag, LastModified, Range, StreamingBlob},
    s3_error,
};
#[cfg(feature = "select")]
use tokio::sync::Semaphore;

#[cfg(feature = "acl")]
use self::acls::object_write_acl;
#[cfg(feature = "acl")]
use crate::_auth::MayBypass;
#[cfg(feature = "acl")]
use crate::_auth::canned::GrantHeaders;
#[cfg(feature = "acl")]
use crate::_auth::identity::Identity;
#[cfg(feature = "copy")]
use crate::_auth::source::copy_source_of;
#[cfg(feature = "acl")]
use crate::_core::acl;
#[cfg(feature = "multipart")]
use crate::_core::checksum as core_checksum;
#[cfg(feature = "object-lock")]
use crate::_core::object_lock::Config as ObjectLockConfig;
pub use crate::s3::Capabilities;
use crate::{
    _core::{
        BodyStream, ETag, bucket, object,
        object_lock::{Bypass, Lock},
        storage::{ByteRange, Error as StorageError, Storage, resolve_write_lock},
        version,
    },
    _util::lockmap::{self, Map},
};

/// The `x-amz-version-id` header value of a version identity, when the
/// HTTP header rules accept it — the one guard the two identity carriers
/// share: [`set_version_headers`] inserts what this returns, and
/// [`S3Backend::version_id_member`] answers the identity only when it is
/// `Some`, so an unrenderable identity is skipped by both instead of
/// failing the response's renderer. The identifier is validated for length
/// only (constitution II), so a rejected wire is a wire-shape failure, not
/// a caller error.
pub(crate) fn version_id_header(id: Option<&version::Id>) -> Option<http::HeaderValue> {
    http::HeaderValue::from_str(&version::wire_of(id)).ok()
}

/// Set the versioning headers of an answer from a version identity — the
/// `x-amz-version-id` AWS carries on the writes and reads of a bucket that
/// records versions (the literal `null` for the null version), and
/// `x-amz-delete-marker: true` when the answer is about a delete marker.
///
/// The caller owns the *when*: a bucket that records no versions answers
/// without `x-amz-version-id` (operator ruling 2026-10-03 for the plain,
/// non-version-addressed `GET`/`HEAD`), and only the caller knows whether
/// it read the state. The identity rides the header verbatim; a value the
/// header rules reject is skipped rather than panicking
/// ([`version_id_header`] is that one guard).
pub(crate) fn set_version_headers(
    out: &mut http::HeaderMap,
    id: Option<&version::Id>,
    is_delete_marker: bool,
) {
    if let Some(value) = version_id_header(id) {
        out.insert("x-amz-version-id", value);
    }
    if is_delete_marker {
        out.insert(
            "x-amz-delete-marker",
            http::HeaderValue::from_static("true"),
        );
    }
}

/// The `versionId` a copy source names — the bucket form of
/// `x-amz-copy-source`; the ARN forms this workspace refuses
/// ([`S3Backend::copy_source`]) carry none of their own.
#[cfg(feature = "copy")]
pub(crate) fn copy_source_version(source: &dto::CopySource) -> Option<&str> {
    match source {
        dto::CopySource::Bucket { version_id, .. } => version_id.as_deref(),
        _ => None,
    }
}

/// The IMF-fixdate value of a `Last-Modified` header, rendered by s3s's own
/// formatter so a raw-header site (a marker's `405`) matches every other
/// answer's [`S3Backend::last_modified`].
pub(crate) fn last_modified_header(t: SystemTime) -> Option<http::HeaderValue> {
    let mut buf = Vec::new();
    dto::LastModified::from(t)
        .format(dto::TimestampFormat::HttpDate, &mut buf)
        .ok()?;
    http::HeaderValue::from_bytes(&buf).ok()
}

/// OL15's request-extension bit: the governance-bypass authority the ACL
/// pipeline (`tinio_auth::AclAccess`) resolved for this request. A build or
/// data plane without that pipeline leaves no bit, and no bit is no
/// authority (fail closed).
pub(crate) fn extension_may_bypass(extensions: &http::Extensions) -> bool {
    #[cfg(feature = "acl")]
    {
        extensions
            .get::<MayBypass>()
            .is_some_and(|bit| bit.granted())
    }
    #[cfg(not(feature = "acl"))]
    {
        let _ = extensions;
        false
    }
}

/// OL15's `header && may_bypass`: the only way a governance bypass rides
/// the storage call. The header alone is a request, never an authority.
pub(crate) fn bypass_of(requested: bool, may_bypass: bool) -> Bypass {
    if requested && may_bypass {
        Bypass::Governance
    } else {
        Bypass::None
    }
}

/// The checksum facts the write-lock gate reads (OL16): the request's
/// `Content-MD5` wherever it arrived (the DTO field or the raw header), the
/// headers themselves, whether the request carries trailing headers, and
/// whether one of the DTO's own checksum fields is present (`PostObject`'s
/// mapped form fields never reach the headers).
pub(crate) struct LockChecksum<'a> {
    pub(crate) dto_md5: Option<&'a str>,
    pub(crate) headers: &'a http::HeaderMap,
    pub(crate) trailing_headers: bool,
    pub(crate) dto_checksum: bool,
}

/// The S3 mapping over one [`Storage`] backend.
///
/// # Examples
///
/// ```rust
/// use http::{Extensions, HeaderMap, Method, Uri};
/// use s3s::{S3, S3Request, dto};
/// use tinio_core::{bucket, storage::BucketOps};
/// use tinio_mem::MemoryStorage;
/// use tinio_server::backend::S3Backend;
/// use tokio::runtime::Runtime;
///
/// fn request(input: dto::CreateBucketInput) -> S3Request<dto::CreateBucketInput> {
///     S3Request {
///         input,
///         method: Method::PUT,
///         uri: Uri::default(),
///         headers: HeaderMap::new(),
///         extensions: Extensions::new(),
///         credentials: None,
///         region: None,
///         service: None,
///         trailing_headers: None,
///     }
/// }
///
/// let storage = MemoryStorage::new().unwrap();
/// let backend = S3Backend::new(storage, Default::default());
/// let result = Runtime::new().unwrap().block_on(async {
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
    /// The uploads' persisted checksum specs, cached read-through (F04):
    /// `upload_id` → the create-time spec (`None` = the upload carries
    /// no spec). Keyed by the upload id alone — a UUID (both backends
    /// generate `Uuid::new_v4()`), so the hit path borrows the request's
    /// upload_id with no key allocation. A spec is immutable after
    /// create, so an entry never goes stale — and the storage layer
    /// still enforces existence at write time, so a cached spec of an
    /// aborted upload can never resurrect a part. Saves the serialized
    /// pre-body `get_multipart_upload` read on every `UploadPart` (10k
    /// parts = 10k reads) and the second read in `ListParts`. Entries
    /// live for the upload's lifetime: abort/complete evict them, the
    /// create seed is skipped while the checksum toggle is off (the
    /// readers are gated the same way), and the map is hard-bounded at
    /// [`CHECKSUM_SPEC_CACHE_CAP`] — the store-level sweep can abort an
    /// upload without the backend hearing, so that stale entry would
    /// otherwise live forever.
    #[cfg(feature = "multipart")]
    pub(crate) checksum_specs: Arc<Mutex<HashMap<String, Option<Arc<core_checksum::Upload>>>>>,
    /// The process-local `has_lock_config` negative cache (OL18): `bucket` →
    /// whether the bucket stores an Object Lock configuration. A `false` entry
    /// makes every subsequent GET/HEAD/range skip the version-lock table
    /// entirely; a bucket with a configuration still pays the one config read
    /// the first time and the per-version point read on every answer. Keyed
    /// by bucket name (the name is bounded at 63 bytes). Consulted only by the
    /// GET/HEAD echo, and only while the capability and the bucket's
    /// version-recording bit are on; written with the resulting fact by
    /// `op_put_bucket_object_lock`, `op_create_bucket` and `op_delete_bucket`
    /// (the three request paths that can change it).
    pub(crate) object_lock_cache: ObjectLockCache,
    /// Cap on concurrently streaming select responses
    /// ([`Capabilities::select_concurrency`]): gates admission of the
    /// whole job (forwarder + channels + response). One semaphore per
    /// backend instance — not process-global. The permit rides the
    /// response stream (dropped on end or cancel).
    #[cfg(feature = "select")]
    pub(crate) select_semaphore: Arc<Semaphore>,
    /// The rayon pool for the sync select engine (same width as
    /// [`Self::select_semaphore`], stack
    /// [`Capabilities::select_stack_bytes`] — X3 parquet/arrow recursion).
    /// Reuses threads across jobs instead of `thread::Builder::spawn` per
    /// request. **Built on first use**, through [`Self::select_pool`]:
    /// pool construction is fallible (it spawns OS threads), and the
    /// select op is the site that has a `Result` to answer a refused build
    /// through, so no constructor panics for every connection (design
    /// 2026-10-09 D5).
    #[cfg(feature = "select")]
    pub(crate) select_pool: Arc<OnceLock<Arc<ThreadPool>>>,
    /// The identity map (feature `acl`): `Some` = the auth-configured
    /// principals and default owner, `None` = the legacy/no-identity
    /// mode (the ACL ops answer with the core default-owner pair and
    /// no identity name resolution; Task 11's write paths record the
    /// empty owner wire under it — review B4).
    #[cfg(feature = "acl")]
    pub(crate) identity: Option<Arc<Identity>>,
}

impl<S: Storage> S3Backend<S> {
    /// Construct the mapping over `storage` with the given toggles.
    pub fn new(storage: S, caps: Capabilities) -> Self {
        // Delegates to the shared-storage constructor (wrapping first) so
        // the route, decorator, and backend share one storage handle.
        Self::new_shared(Arc::new(storage), caps)
    }

    /// Construct the mapping over a shared `storage` handle with the given
    /// toggles — the shared-`Arc<S>` constructor.
    ///
    /// UNGATED: it exists in every build (feature-off builds keep a
    /// constructor via [`S3Backend::new`], per spec §5); only the cors
    /// wiring is `#[cfg(feature = "cors")]`. The feature `acl` plane
    /// wires the same `Arc<S>` into both the mapping and the
    /// authorization pipeline — one backend, one share.
    pub fn new_shared(storage: Arc<S>, caps: Capabilities) -> Self {
        Self::new_shared_with_object_lock_cache(storage, caps, ObjectLockCache::default())
    }

    /// Construct the mapping over a shared `storage` handle with the caller's Object Lock cache
    /// (OL18). **The management plane's admin shares the same handle**, so a configuration it
    /// writes invalidates the backend's `has_lock_config` answer without the backend seeing the
    /// request.
    pub fn new_shared_with_object_lock_cache(
        storage: Arc<S>,
        caps: Capabilities,
        object_lock_cache: ObjectLockCache,
    ) -> Self {
        Self {
            storage,
            caps,
            conditional_put_locks: Map::new(),
            #[cfg(feature = "multipart")]
            checksum_specs: Arc::new(Mutex::new(HashMap::new())),
            object_lock_cache,
            #[cfg(feature = "select")]
            select_semaphore: Arc::new(Semaphore::new(caps.select_concurrency as usize)),
            #[cfg(feature = "select")]
            select_pool: Arc::new(OnceLock::new()),
            #[cfg(feature = "acl")]
            identity: None,
        }
    }

    /// A clone of this backend's Object Lock cache handle (OL18), for a writer outside the
    /// request paths — the management plane's admin.
    pub fn object_lock_cache(&self) -> ObjectLockCache {
        self.object_lock_cache.clone()
    }

    /// The effective Object Lock of a write (OL7, OL16, OL20), resolved
    /// **once**: the request's [`LockHeaders`] over the bucket's default
    /// rule, evaluated at one clock reading. The same value drives the
    /// checksum requirement, so a concurrent configuration change cannot
    /// make the two disagree.
    ///
    /// Always compiled: [`LockHeaders`]'s two `cfg` constructors are the
    /// whole feature split, so every write surface passes one value here; a
    /// stripped build's empty headers still apply the bucket's stored
    /// default and gate it on the checksum that lock requires.
    ///
    /// A request carrying a lock header on a bucket with no configuration is
    /// OL17's `400 InvalidRequest` — the store's resolver would otherwise
    /// place the explicit lock on a bucket that never opted in.
    pub(crate) async fn write_lock(
        &self,
        bucket: &bucket::Name,
        lock_headers: &LockHeaders,
        checksum: LockChecksum<'_>,
    ) -> S3Result<Option<Lock>> {
        let now = SystemTime::now();
        let config = self
            .storage
            .get_bucket_object_lock(bucket)
            .await
            .map_err(map_backend_error)?;
        if lock_headers.requested {
            if let Some(retention) = lock_headers.retention.as_ref() {
                retention
                    .validate(now)
                    .map_err(|invalid| errors::invalid_error(invalid.kind, invalid.reason, None))?;
            }
            if config.is_none() {
                return Err(s3_error!(
                    InvalidRequest,
                    "Bucket is missing Object Lock Configuration"
                ));
            }
        }
        let lock = resolve_write_lock(
            config.as_ref(),
            lock_headers.retention.as_ref(),
            lock_headers.legal_hold,
            now,
        );
        let content_md5 = checksum::request_content_md5(checksum.dto_md5, checksum.headers)?;
        checksum::require_lock_checksum(
            lock.as_ref(),
            checksum.headers,
            checksum.trailing_headers,
            content_md5.as_deref(),
            checksum.dto_checksum,
        )?;
        Ok(lock)
    }

    /// Attach the identity map (feature `acl`): the ACL ops' `Owner`
    /// elements and (Task 11) the write-path owner recording resolve
    /// through it. `None` (the default) is the legacy/no-identity mode.
    #[cfg(feature = "acl")]
    pub fn with_identity(mut self, identity: Arc<Identity>) -> Self {
        self.identity = Some(identity);
        self
    }

    /// The storage backend (for direct contract access in tests/harness).
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// The capability toggles.
    pub fn capabilities(&self) -> Capabilities {
        self.caps
    }

    /// The select job pool (X3), built on first use: a rayon pool whose
    /// threads carry [`Capabilities::select_stack_bytes`] — parquet footer
    /// schema recursion and `arrow_json` nesting would otherwise abort the
    /// process on a stack overflow (`catch_unwind` cannot hold one). The
    /// build spawns OS threads, so it is fallible; the refusal is logged
    /// and answered as the request's generic 500 (design 2026-10-09 D5:
    /// the detail stays in the log, the body carries no internal string).
    #[cfg(feature = "select")]
    pub(crate) fn select_pool(&self) -> S3Result<Arc<ThreadPool>> {
        if let Some(pool) = self.select_pool.get() {
            return Ok(Arc::clone(pool));
        }
        let pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(self.caps.select_concurrency as usize)
                .stack_size(self.caps.select_stack_bytes as usize)
                .thread_name(|i| format!("tinio-select-{i}"))
                .build()
                .map_err(|err| {
                    tracing::error!(
                        error = %err,
                        "select job pool could not be built — select requests answer 500"
                    );
                    s3_error!(InternalError, "select job pool unavailable")
                })?,
        );
        // A concurrent first use may have won the race: the winner's pool is
        // the one stored, and this call still answers with a pool either way.
        Ok(Arc::clone(self.select_pool.get_or_init(|| pool)))
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

    /// Record the object identity on the operation span (`tinio.s3.operation`).
    ///
    /// The handler runs inside the decorator's span; a second span per handler
    /// would split one operation's identity across two rows.
    pub(crate) fn record_operation_identity(bucket: &bucket::Name, key: Option<&object::Key>) {
        let span = Span::current();
        span.record("bucket", field::display(bucket));
        if let Some(key) = key {
            span.record("key", field::display(key));
        }
    }

    /// The source of a `CopyObject`/`UploadPartCopy` request into a
    /// (bucket, key) pair — the framework has already parsed the header into
    /// [`dto::CopySource`], and the pair is validated by the auth crate's one
    /// parse authority (P1/P6), so the ACL and the handler cannot disagree
    /// about which key is being acted on. Anything that is not a
    /// `<bucket>/<key>` pair the ACL would authorize (an ARN, a malformed
    /// value) is the server's 400.
    #[cfg(feature = "copy")]
    pub(crate) fn copy_source(
        &self,
        source: &dto::CopySource,
    ) -> Result<(bucket::Name, object::Key), S3Error> {
        copy_source_of(source).ok_or_else(|| s3_error!(InvalidArgument, "unsupported copy source"))
    }

    /// A `StreamingBlob` request body into the contract's [`BodyStream`].
    pub(crate) fn stream_in(body: Option<dto::StreamingBlob>) -> BodyStream {
        match body {
            Some(body) => Box::pin(body.map_err(IoError::other)),
            None => Box::pin(stream::empty()),
        }
    }

    /// The upload's persisted checksum spec, cached read-through (F04):
    /// the spec is immutable after create, so a cache hit is always
    /// current; the storage layer still enforces existence at write
    /// time, so a stale entry of an aborted upload can never resurrect
    /// a part (the write answers `NoSuchUpload` itself).
    #[cfg(feature = "multipart")]
    pub(crate) async fn upload_checksum_spec(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> S3Result<Option<Arc<core_checksum::Upload>>> {
        // The map's `get`/`remove` borrow the request's upload_id — no
        // key allocation on the hit path (the id is the whole key; see
        // the field doc). Bucket/key still feed the storage fallback
        // read on a miss.
        if let Some(spec) = self
            .checksum_specs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(upload_id)
            .cloned()
        {
            return Ok(spec);
        }
        let spec = self
            .storage
            .get_multipart_upload(bucket, key, upload_id)
            .await
            .map_err(map_backend_error)?
            .checksum
            .map(Arc::new);
        self.put_checksum_spec(upload_id.to_string(), spec.clone());
        Ok(spec)
    }

    /// Insert a spec into the cache, bounded at
    /// [`CHECKSUM_SPEC_CACHE_CAP`]: past the cap one arbitrary entry is
    /// evicted per insert. Eviction is always safe — the cache is
    /// read-through, so a miss (even of a live upload) just re-reads the
    /// storage row, which still enforces existence.
    #[cfg(feature = "multipart")]
    pub(crate) fn put_checksum_spec(
        &self,
        upload_id: String,
        spec: Option<Arc<core_checksum::Upload>>,
    ) {
        let mut map = self
            .checksum_specs
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        map.insert(upload_id, spec);
        if map.len() > CHECKSUM_SPEC_CACHE_CAP
            && let Some(id) = map.keys().next().cloned()
        {
            map.remove(&id);
        }
    }

    /// Forget the cached spec of a finished upload (abort/complete): the
    /// entry would otherwise outlive the upload it describes — the cache
    /// would grow with every upload ever created, not the live ones.
    #[cfg(feature = "multipart")]
    pub(crate) fn evict_checksum_spec(&self, upload_id: &str) {
        self.checksum_specs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(upload_id);
    }

    /// The bucket's stored Object Lock configuration, read through — the
    /// authoritative read the six lock operations and the write path use
    /// (OL18's cache is the GET/HEAD echo's alone).
    #[cfg(feature = "object-lock")]
    pub(crate) async fn object_lock_config(
        &self,
        bucket: &bucket::Name,
    ) -> S3Result<Option<ObjectLockConfig>> {
        self.storage
            .get_bucket_object_lock(bucket)
            .await
            .map_err(map_backend_error)
    }

    /// Whether the bucket stores an Object Lock configuration, through the
    /// process-local cache (OL18): the GET/HEAD echo's first probe per bucket
    /// reads the configuration once; a `false` answer then skips the
    /// version-lock read for every subsequent GET/HEAD/range. The capability
    /// off answers `false` without a read or a cache entry — the echo is a
    /// surface the toggle owns, while enforcement (the write path's default
    /// retention and the delete protection) never consults this cache.
    ///
    /// The miss path fills a missing entry through
    /// [`ObjectLockCache::merge`], whose `entry().or_insert(read)` never
    /// overwrites an entry a write path installed while the read was in
    /// flight: the interleaving "reader sees no configuration →
    /// `op_put_bucket_object_lock` commits and inserts `true` → the reader
    /// tries to insert `false`" keeps the writer's `true`, so a new
    /// configuration cannot be hidden for the process's lifetime (the OL18
    /// "a stale `false` must not hide a new config" rule).
    #[cfg(feature = "object-lock")]
    pub(crate) async fn has_lock_config(&self, bucket: &bucket::Name) -> S3Result<bool> {
        if !self.caps.object_lock {
            return Ok(false);
        }
        if let Some(hit) = self.object_lock_cache.cached(bucket) {
            return Ok(hit);
        }
        let has = self
            .storage
            .get_bucket_object_lock(bucket)
            .await
            .map_err(map_backend_error)?
            .is_some();
        Ok(self.object_lock_cache.merge(bucket, has))
    }

    /// The contract's [`BodyStream`] into a response `StreamingBlob`.
    pub(crate) fn stream_out(body: BodyStream) -> dto::StreamingBlob {
        StreamingBlob::wrap(body)
    }

    /// The wire ETag of a contract ETag (the framework emits the quotes).
    pub(crate) fn etag_wire(etag: &ETag) -> dto::ETag {
        WireETag::Strong(etag.as_str())
    }

    /// The response `LastModified` timestamp of a [`SystemTime`].
    pub(crate) fn last_modified(t: SystemTime) -> dto::LastModified {
        LastModified::from(t)
    }

    /// The inferred Content-Type of a key (mime_guess; fallback
    /// `application/octet-stream`, FR-022).
    pub(crate) fn content_type(key: &str) -> String {
        mime_guess::from_path(key)
            .first_or_octet_stream()
            .essence_str()
            .to_string()
    }

    /// The requester's owner for the write paths: the identity map's
    /// principal (the authenticated user's canonical ID, or the
    /// anonymous special ID for an unsigned request), or `None` under
    /// no-identity mode — the row records the empty owner wire (review
    /// B4, never the default owner). The write paths resolve through it;
    /// the ACL ops use the row-owner resolution instead.
    ///
    /// The resolution is fallible — credentials naming an access key the
    /// map does not carry are refused with the `InvalidAccessKeyId` their
    /// signature was verified under, never a panic — so a caller must
    /// propagate that refusal instead of reading `None` (which is the
    /// no-identity mode alone) as "the identity is attached".
    #[cfg(feature = "acl")]
    pub(crate) fn owner_for(
        &self,
        credentials: Option<&Credentials>,
    ) -> S3Result<Option<acl::OwnerId>> {
        match self.identity.as_ref() {
            Some(identity) => identity.principal(credentials).map(Some),
            None => Ok(None),
        }
    }

    /// The request's principal under the enforced ACL mode — the ONE spelling
    /// of the "enforced mode resolves the requester" gate (create, listing
    /// filter, per-key delete gate). The toggle on: [`Self::owner_for`]'s
    /// resolution, fail-closed — an unresolved principal is the request
    /// failure that function answers, never a silent `None` read as
    /// "no identity". The toggle off (or no identity attached): `None`, and
    /// the caller keeps its legacy path.
    #[cfg(feature = "acl")]
    pub(crate) fn principal_for(
        &self,
        credentials: Option<&Credentials>,
    ) -> S3Result<Option<acl::OwnerId>> {
        if self.caps.acl {
            self.owner_for(credentials)
        } else {
            Ok(None)
        }
    }

    /// The lazy-resolved owner of a row (review B4): the recorded owner
    /// element, or the default owner — the identity's configured
    /// default, the core built-in [`acl::default_owner_id`] in
    /// no-identity mode.
    #[cfg(feature = "acl")]
    pub(crate) fn row_owner(&self, row: Option<&acl::OwnerId>) -> acl::OwnerId {
        match row {
            Some(id) => id.clone(),
            None => match &self.identity {
                Some(identity) => identity.default_owner.clone(),
                None => acl::default_owner_id(),
            },
        }
    }

    /// The write-path ACL of an OBJECT surface (Task 11): the
    /// capability gate + the identity-mode owner and the request's
    /// canned / `x-amz-grant-*` expansion — `Some((owner, grants-only
    /// acl))` under identity mode with the toggle on, `None` under
    /// no-identity / toggle-off (the caller records the empty owner wire
    /// and the private default — review B4 rule 3). The bucket owner is
    /// resolved lazily from the bucket row — only a canned
    /// `bucket-owner-*` expansion needs it (review A6).
    #[cfg(feature = "acl")]
    pub(crate) async fn write_acl_for(
        &self,
        credentials: Option<&Credentials>,
        bucket: &bucket::Name,
        canned: Option<&str>,
        headers: GrantHeaders<'_>,
    ) -> S3Result<Option<(acl::OwnerId, Acl)>> {
        if !self.caps.acl {
            return Ok(None);
        }
        let Some(owner) = self.owner_for(credentials)? else {
            // No identity attached: the toggle-off above's shape — there is
            // no requester to record and no owner to expand against.
            return Ok(None);
        };
        let bucket_owner = if matches!(
            canned,
            Some("bucket-owner-read" | "bucket-owner-full-control")
        ) {
            self.row_owner(
                self.storage
                    .get_bucket_acl(bucket)
                    .await
                    .map_err(map_backend_error)?
                    .owner
                    .as_ref(),
            )
        } else {
            // The expansion touches the bucket owner only for the
            // `bucket-owner-*` names — the owner stands in.
            owner.clone()
        };
        let acl = object_write_acl(&owner, &bucket_owner, canned, &headers)?;
        Ok(Some((owner, acl)))
    }

    /// The dto `Owner` element of an ACL row: the display name resolved
    /// through the identity map (an unknown ID — the anonymous one
    /// included — is ID-only, AWS's unknown-account shape); no-identity
    /// mode resolves the lazy default-owner pair for a row without one
    /// and ID-only for a recorded one.
    #[cfg(feature = "acl")]
    pub(crate) fn acl_owner(&self, row: Option<&acl::OwnerId>) -> dto::Owner {
        match &self.identity {
            Some(identity) => identity.owner(row),
            None => dto::Owner {
                id: Some(self.row_owner(row).as_str().to_string()),
                display_name: row
                    .is_none()
                    .then(|| acl::DEFAULT_OWNER_DISPLAY_NAME.to_string()),
            },
        }
    }
}

/// The wire `Range` header into the contract's [`ByteRange`] — the GET
/// mapping (all three S3 shapes). The strict copy-source form
/// ([`copy_source_range`]) accepts only the closed `bytes=first-last`
/// shape on top of this mapping.
pub(crate) fn byte_range(r: dto::Range) -> ByteRange {
    match r {
        Range::Int {
            first,
            last: Some(last),
        } => ByteRange::Inclusive(first, last),
        Range::Int { first, last: None } => ByteRange::From(first),
        Range::Suffix { length } => ByteRange::Suffix(length),
    }
}

/// Normalize the S3 wire `delimiter`: an empty `delimiter=` value means
/// "no delimiter" (clients like mc always send it) — a `Some("")` would
/// roll every key up into an empty common prefix and empty the page.
/// One home for the boundary rule, shared by the object, upload and
/// version listings.
#[cfg(any(
    feature = "multipart",
    feature = "list-v1",
    feature = "list-v2",
    feature = "versioning"
))]
pub(crate) fn normalize_delimiter(delimiter: Option<String>) -> Option<String> {
    delimiter.filter(|d| !d.is_empty())
}

/// The wire `CommonPrefix` elements of a listing's delimiter roll-up — the
/// one mapping the object, upload and version listings share (the core
/// listings hand the roll-up as plain strings).
#[cfg(any(
    feature = "multipart",
    feature = "list-v1",
    feature = "list-v2",
    feature = "versioning"
))]
pub(crate) fn common_prefixes(prefixes: Vec<String>) -> Vec<dto::CommonPrefix> {
    prefixes
        .into_iter()
        .map(|prefix| dto::CommonPrefix {
            prefix: Some(prefix),
        })
        .collect()
}

/// Clamp a requested page size to the configured cap. `cap = 0` means
/// "no clamp" — a literal `min(requested, 0)` would turn the permissive
/// contract's `max = 0` empty-page semantics on for every uncapped
/// listing (the default `[s3] max_keys` config). One home for the
/// boundary rule, shared by the ListBuckets and ListObjects mappings.
pub(crate) fn clamp_page_size(requested: usize, cap: u32) -> usize {
    if cap == 0 {
        requested
    } else {
        requested.min(cap as usize)
    }
}

/// The unified listing page-size policy (design 2026-08-29): a page
/// size < 1 is rejected before any storage call unless `allow_zero` —
/// the `[s3] allow_zero_page_size` escape hatch of the pre-existing
/// surfaces — which restores the legacy clamp-to-0 empty page
/// (negatives included, the old `.max(0)`). ListBuckets does not use
/// this helper: its AWS-documented 1..=10,000 range is always strict.
pub(crate) fn normalize_page_size(
    requested: i32,
    param: &str,
    allow_zero: bool,
) -> S3Result<usize> {
    if requested < 1 {
        if allow_zero {
            return Ok(0);
        }
        return Err(s3_error!(InvalidArgument, "{param} must be at least 1"));
    }
    Ok(requested as usize)
}
