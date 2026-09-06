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
//! ([`Capabilities`], from the `[s3]` config section) — disabled groups
//! answer `NotImplemented` (FR-021).
//!
//! **403 ambiguity (the ACL plane, spec 2026-09-05)**: with owners/ACLs
//! enforced, a 403 `AccessDenied` can mean an ACL denial at the access
//! layer (`tinio-auth`'s `S3Access`), an OS-level `PermissionDenied`
//! from the filesystem backend, or the symlink-policy refusal (the
//! `follow_symlinks = false` answer) — the last two reach the same S3
//! code through [`Storage`] contract `Error::AccessDenied`, which
//! [`errors::map_backend_error`] maps onto `AccessDenied` and logs the
//! storage source at debug. The wire cannot distinguish the cases;
//! debug-level logs are the only forensic tool. **`/metrics` stays
//! outside the ACL check** (grilling ruling): the reserved
//! management-plane GET is intercepted pre-route in `data.rs` and is not
//! subject to the access-layer pipeline.

mod conditions;
#[cfg(feature = "cors")]
pub(crate) mod cors;
#[cfg(feature = "acl")]
pub(crate) mod acls;
mod errors;
mod locks;
mod s3;

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

#[cfg(feature = "multipart")]
use std::{collections::HashMap, sync::Mutex};

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
#[cfg(feature = "select")]
use rayon::ThreadPool;
#[cfg(feature = "copy")]
use s3s::dto::CopySource;
use s3s::{
    S3Error, S3Result,
    dto::{self, ETag as WireETag, LastModified, Range, StreamingBlob},
    s3_error,
};
#[cfg(feature = "select")]
use tokio::sync::Semaphore;

pub use crate::_config::s3::Capabilities;
#[cfg(feature = "multipart")]
use crate::_core::checksum as core_checksum;
use crate::{
    _core::{
        BodyStream, ETag, bucket, object,
        storage::{ByteRange, Error as StorageError, Storage},
    },
    _util::lockmap::{self, Map},
};
#[cfg(feature = "acl")]
use crate::_auth::canned::GrantHeaders;
#[cfg(feature = "acl")]
use crate::_core::acl;
#[cfg(feature = "acl")]
use crate::_auth::identity::Identity;
#[cfg(feature = "acl")]
use self::acls::object_write_acl;

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
    /// Cap on concurrently streaming select responses
    /// ([`Capabilities::select_concurrency`]): gates admission of the
    /// whole job (forwarder + channels + response). One semaphore per
    /// backend instance — not process-global. The permit rides the
    /// response stream (dropped on end or cancel).
    #[cfg(feature = "select")]
    pub(crate) select_semaphore: Arc<Semaphore>,
    /// Pre-sized rayon pool for the sync select engine (same width as
    /// [`Self::select_semaphore`], stack
    /// [`Capabilities::select_stack_bytes`] — X3 parquet/arrow recursion).
    /// Reuses threads across jobs instead of `thread::Builder::spawn` per
    /// request.
    #[cfg(feature = "select")]
    pub(crate) select_pool: Arc<ThreadPool>,
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
        Self {
            storage,
            caps,
            conditional_put_locks: Map::new(),
            #[cfg(feature = "multipart")]
            checksum_specs: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(feature = "select")]
            select_semaphore: Arc::new(Semaphore::new(caps.select_concurrency as usize)),
            #[cfg(feature = "select")]
            select_pool: Arc::new(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(caps.select_concurrency as usize)
                    .stack_size(caps.select_stack_bytes as usize)
                    .thread_name(|i| format!("tinio-select-{i}"))
                    .build()
                    .expect("select rayon pool"),
            ),
            #[cfg(feature = "acl")]
            identity: None,
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
            .expect("checksum-spec cache poisoned")
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
            .expect("checksum-spec cache poisoned");
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
            .expect("checksum-spec cache poisoned")
            .remove(upload_id);
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

    /// The source of a `CopyObject`/`UploadPartCopy` request into a
    /// (bucket, key) pair (the framework parses the header into
    /// [`dto::CopySource`]).
    #[cfg(feature = "copy")]
    pub(crate) fn copy_source(
        &self,
        source: &dto::CopySource,
    ) -> Result<(bucket::Name, object::Key), S3Error> {
        match source {
            CopySource::Bucket { bucket, key, .. } => {
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

    /// The requester's owner for the write paths: the identity map's
    /// principal (the authenticated user's canonical ID, or the
    /// anonymous special ID for an unsigned request), or `None` under
    /// no-identity mode — the row records the empty owner wire (review
    /// B4, never the default owner). The write paths resolve through it;
    /// the ACL ops use the row-owner resolution instead.
    #[cfg(feature = "acl")]
    pub(crate) fn owner_for(
        &self,
        credentials: Option<&s3s::auth::Credentials>,
    ) -> Option<acl::OwnerId> {
        self.identity
            .as_ref()
            .map(|identity| identity.principal(credentials))
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
        credentials: Option<&s3s::auth::Credentials>,
        bucket: &bucket::Name,
        canned: Option<&str>,
        headers: GrantHeaders<'_>,
    ) -> S3Result<Option<(acl::OwnerId, acl::Acl)>> {
        if !self.caps.acl || self.identity.is_none() {
            return Ok(None);
        }
        let owner = self
            .owner_for(credentials)
            .expect("identity attached above");
        let bucket_owner =
            if matches!(canned, Some("bucket-owner-read" | "bucket-owner-full-control")) {
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
/// One home for the boundary rule, shared by the object and upload
/// listings.
#[cfg(any(feature = "multipart", feature = "list-v1", feature = "list-v2"))]
pub(crate) fn normalize_delimiter(delimiter: Option<String>) -> Option<String> {
    delimiter.filter(|d| !d.is_empty())
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
