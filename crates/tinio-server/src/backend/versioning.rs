//! The versioning operations and the one guard every version-addressed
//! request passes (the design's fail-open rule: a request naming a version is
//! refused outright when versioning is unavailable, never answered from the
//! current object).
//!
//! Two floors decide whether versioning is available, and the guard states
//! which one answered: with the cargo feature compiled in, the `[s3]
//! versioning` capability decides; without it, every version-addressed
//! request is refused and the three operations below are compiled out.
//!
//! The wire helpers the object operations share
//! ([`S3Backend::version_address`], the version headers, the plain miss's
//! marker disclosure) live here too and are built in every configuration:
//! the operations that carry a `versionId` are not feature-gated
//! themselves, so the guard is what keeps a stripped build from answering
//! one from the current object.

use std::time::SystemTime;

use http::{HeaderMap, HeaderValue, Uri, header::LAST_MODIFIED};
use s3s::{S3Error, S3Result, s3_error};
#[cfg(feature = "versioning")]
use s3s::{S3Request, S3Response, dto};
#[cfg(test)]
use tinio_util::testing::object_key;

use super::{S3Backend, map_backend_error, set_version_headers, version_id_header};
#[cfg(feature = "versioning")]
use super::{clamp_page_size, common_prefixes, normalize_delimiter, normalize_page_size};
use crate::_core::{
    bucket, object,
    storage::{Error as StorageError, GetObjectResult, Storage},
    version,
};

/// Refuse a version-addressed request when this server does not offer
/// versioning. One body, two floors, and the message names which one
/// answered: with the feature compiled in, the `[s3] versioning` toggle
/// decides; without it, every such request is refused whatever the
/// capability says.
///
/// A plain request (no version named) always passes: current versions keep
/// serving on a root whose versions are no longer addressable, which is the
/// design's rule for a toggle switched off on a root that holds version data.
pub(crate) fn require_versioning<S: Storage>(
    backend: &S3Backend<S>,
    version: Option<&str>,
) -> S3Result<()> {
    if version.is_none() {
        return Ok(());
    }
    if !cfg!(feature = "versioning") {
        return Err(s3_error!(NotImplemented, "versioning is not compiled in"));
    }
    if !backend.capabilities().versioning {
        return Err(s3_error!(NotImplemented, "versioning is disabled"));
    }
    Ok(())
}

// --- the wire helpers every object operation shares ---

/// The `versionId` a request's raw query carries — the source for the two
/// write handlers whose input does not surface it: s3s ships two
/// `PutObjectInput`s and two `CreateMultipartUploadInput`s, and only the
/// ones behind `#[cfg(feature = "minio")]` carry a `version_id` field
/// (the struct and the `deserialize_http` that parses `versionId` into it
/// are gated together; this workspace enables only s3s's default
/// features). Only its presence is read — the guarded writes give the
/// value no meaning (AWS defines none for either operation), and the
/// fail-open rule keys on the request carrying it. A bare `versionId` (no
/// `=`) reads as the empty value, the way the DTO-backed sites see it.
pub(crate) fn query_version_id(uri: &Uri) -> Option<&str> {
    uri.query()?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        (key == "versionId").then_some(value)
    })
}

/// The `405 Method Not Allowed` answer of a `GET`/`HEAD` addressed at a
/// delete marker's version id (the design's delete-marker rule):
/// `Last-Modified` and `x-amz-delete-marker: true` — and deliberately no
/// `Allow` header, which AWS models no member for.
///
/// The plan pinned an `S3Response` with `status = Some(405)`; s3s 0.17's
/// generated operation path never reads `S3Response::status` (its `call`
/// forwards only the output, the headers and the extensions, and
/// `serialize_http` derives the status from the output's own shape), so
/// that answer would render as a `200`. The error route is the one that
/// reaches the wire, and it renders AWS's `MethodNotAllowed` body.
///
/// The map carries the body's own `content-type`: s3s's `serialize_error`
/// writes the XML body first and then *assigns* this map over the
/// response's headers (`ops/mod.rs`'s `res.headers = headers`), so a map
/// without it would ship the XML body unlabelled.
pub(crate) fn marker_error(id: Option<&version::Id>, last_modified: SystemTime) -> S3Error {
    let mut headers = HeaderMap::new();
    if let Some(value) = super::last_modified_header(last_modified) {
        headers.insert(LAST_MODIFIED, value);
    }
    set_version_headers(&mut headers, id, true);
    headers.insert("content-type", HeaderValue::from_static("application/xml"));
    let mut err = s3_error!(MethodNotAllowed);
    err.set_headers(headers);
    err
}

/// The object half of a version-addressed head: a marker address is the
/// shared `405` rule ([`marker_error`]), never a metadata answer.
pub(crate) fn object_of(result: version::HeadResult) -> S3Result<object::Info> {
    match result {
        version::HeadResult::Object(info) => Ok(info),
        version::HeadResult::Marker { id, last_modified } => {
            Err(marker_error(id.as_ref(), last_modified))
        }
    }
}

/// The object half of a version-addressed body read — the same `405` rule
/// as [`object_of`]. The copy paths do not come here: a marker source is
/// their `404 NoSuchKey` (a marker holds no bytes to copy).
pub(crate) fn read_object_of(result: version::ReadResult) -> S3Result<GetObjectResult> {
    match result {
        version::ReadResult::Object(result) => Ok(result),
        version::ReadResult::Marker { id, last_modified } => {
            Err(marker_error(id.as_ref(), last_modified))
        }
    }
}

impl<S: Storage> S3Backend<S> {
    /// The one wire-value path of the version-addressed handlers: the
    /// fail-open guard first ([`require_versioning`]), then the one parse
    /// ([`version::Address::from_wire`](crate::_core::version::Address::from_wire))
    /// — so every site refuses a version-addressed request on a server that
    /// cannot answer it, keeps the shared `400` for a malformed value, and
    /// reads the literal `null` as the null version. Every handler whose
    /// input surfaces a `versionId` reads it through here, so the guard can
    /// never be dropped from a site.
    ///
    /// The address carries the request's presence: the handlers branch on
    /// [`Address::is_addressed`](crate::_core::version::Address::is_addressed)
    /// rather than on a second `Option`, so the wire value is read once.
    pub(crate) fn version_address(&self, raw: Option<&str>) -> S3Result<version::Address> {
        require_versioning(self, raw)?;
        version::Address::from_wire(raw).map_err(map_backend_error)
    }

    /// Whether the bucket state says an addressed answer carries a version
    /// identity — the one home of the rule the addressed tag answers used
    /// to re-spell: a specific id names a recording bucket by construction,
    /// so it needs no read, while the null address is answerable only on a
    /// bucket that records versions. A plain (`Address::Current`) caller
    /// never comes here — its `Info`/write result carries the same bit
    /// (`bucket_records_versions`) — so that arm is a trap, not a
    /// behavior: it answers the fail-closed `false` (a header-less answer,
    /// never a wrong one) and trips a `debug_assert!` that names the
    /// caller, instead of quietly adding a state read to a plain path.
    ///
    /// This stays a state read of its own rather than a fold into
    /// [`Self::version_id_member`]: that carrier is deliberately
    /// state-free — its callers own the *when*, the plain ones off a value
    /// they already hold — and folding the read in would make it async and
    /// charge the plain paths for a bit they carry.
    pub(crate) async fn records_for(
        &self,
        bucket: &bucket::Name,
        address: &version::Address,
    ) -> bool {
        debug_assert!(
            address.is_addressed(),
            "the plain address carries its bucket state on the Info/write result; \
             no addressed path calls here"
        );
        match address {
            version::Address::Id(_) => true,
            version::Address::Null => self.records_versions(bucket).await,
            version::Address::Current => false,
        }
    }

    /// Whether the bucket records versions (enabled or suspended) — the
    /// "does this answer carry `x-amz-version-id`" rule wherever no
    /// object `Info` is in hand (the destination-conditional's missing-key
    /// check, the delete-marker probe, and the addressed tag answers
    /// through [`Self::records_for`]). The plain read/write
    /// paths do **not** come here: their producers report the state on
    /// the `Info` (`bucket_records_versions`). One state read; a read
    /// that fails answers "no", the conservative reading (no header
    /// rather than a wrong one).
    pub(crate) async fn records_versions(&self, bucket: &bucket::Name) -> bool {
        matches!(
            self.storage().bucket_versioning(bucket).await,
            Ok(state) if state.records_versions()
        )
    }

    /// The one predicate behind the two identity answers: an answer
    /// carries a version identity whenever the identity is a specific one
    /// (which names a recording bucket by construction) or — for the null
    /// identity — the bucket records versions. The state is the
    /// producer's: every `Info` reports it (`bucket_records_versions`),
    /// so neither helper reads the bucket state on this path.
    fn answers_version_id(id: Option<&version::Id>, records_versions: bool) -> bool {
        id.is_some() || records_versions
    }

    /// The version headers of an answer that carries a version identity (a
    /// read or a write): the identity is answered through the shared
    /// predicate — the null version included, spelled `null` — and a
    /// bucket that records none answers without it. `records_versions` is
    /// the answered object's own carrier (`Info::bucket_records_versions`).
    pub(crate) fn echo_version_id(
        &self,
        out: &mut HeaderMap,
        id: Option<&version::Id>,
        records_versions: bool,
    ) {
        if Self::answers_version_id(id, records_versions) {
            set_version_headers(out, id, false);
        }
    }

    /// The output-member form of [`S3Backend::echo_version_id`]'s rule: the
    /// `version_id` a tagging answer carries is the same identity the header
    /// helper sets — answered whenever the shared predicate says so (the null
    /// version included, spelled `null`), absent on a bucket that records
    /// none. The tagging dto outputs render the member as the
    /// `x-amz-version-id` header (`s3s`'s `serialize_http`), so the two
    /// must agree. The callers hand the state in: a plain path takes it off
    /// the `Info` its head answered; a version-addressed path whose address
    /// is the null version (no `Info` in hand) reads it — those paths are
    /// not the plain hot path this carrier exists for.
    ///
    /// **The header guard is part of the rule**: an identity whose wire
    /// form the header rules reject answers no member, exactly as the
    /// header carrier sets no header ([`version_id_header`], the one shared
    /// guard) — handing it to `s3s`'s renderer would fail the whole
    /// response, where the member merely goes absent.
    pub(crate) fn version_id_member(
        &self,
        id: Option<&version::Id>,
        records_versions: bool,
    ) -> Option<String> {
        let wire = version::wire_of(id);
        (Self::answers_version_id(id, records_versions) && version_id_header(id).is_some())
            .then_some(wire)
    }

    /// The plain read's 404: a key whose current version is a delete marker
    /// answers `x-amz-delete-marker: true` with it (the design's delete
    /// rules — a plain `GET`/`HEAD` of a marked key is a 404 that says so).
    /// The plain storage reads see no marker (a marked key reads as
    /// `NoSuchKey`), so the marker is probed with the bounded per-key
    /// `VersionOps::current_version` — one exact-key read, never a
    /// `ListObjectVersions` page over the bucket.
    ///
    /// The map carries the body's `content-type` for the reason
    /// [`marker_error`] states: it replaces the response's headers
    /// wholesale.
    pub(crate) async fn no_such_object(&self, bucket: &bucket::Name, key: &object::Key) -> S3Error {
        let mut err = map_backend_error(StorageError::NoSuchKey(key.clone()));
        if self.miss_hides_a_marker(bucket, key).await {
            let mut headers = HeaderMap::new();
            headers.insert("x-amz-delete-marker", HeaderValue::from_static("true"));
            headers.insert("content-type", HeaderValue::from_static("application/xml"));
            err.set_headers(headers);
        }
        err
    }

    /// Whether the key's current version is a delete marker. Best-effort:
    /// a probe that cannot answer leaves the plain 404, so a probe
    /// failure never invents a marker. A bucket that records no versions
    /// short-circuits before the probe — a bucket that hides no marker
    /// pays neither the state read's caller nor a listing.
    async fn miss_hides_a_marker(&self, bucket: &bucket::Name, key: &object::Key) -> bool {
        if !self.records_versions(bucket).await {
            return false;
        }
        matches!(
            self.storage().current_version(bucket, key).await,
            Ok(version::CurrentVersion::Marker)
        )
    }
}

// --- the three operations ---
//
// Declared here and dispatched from `s3.rs`; the table's cross-check
// (`table_matches_the_s3_dispatch`) holds the two in step.

#[cfg(feature = "versioning")]
impl<S: Storage> S3Backend<S> {
    /// The hard page ceiling for `op_list_object_versions` (2026-10-06
    /// review): AWS documents a 1,000-key maximum for this listing, and the
    /// ceiling is also what bounds the walk's memory — one page's entries
    /// plus one window's rows — so a huge client `max-keys` cannot
    /// materialize a bucket's whole history. A larger request is answered
    /// as 1,000 (the echoed `max-keys` carries the effective value).
    const VERSIONS_PAGE_MAX: usize = 1000;

    /// `GetBucketVersioning` — the bucket's versioning state.
    ///
    /// The wire carries two states; the third, unversioned, is the
    /// *absence* of `Status` (AWS's own shape), so a fresh bucket answers
    /// an element with neither child rather than an `Unversioned` value.
    ///
    /// MFA delete is never reported: the protection cannot be provided and
    /// a request asking for it is refused (the write below), so the
    /// element stays absent exactly as it does on a bucket that was never
    /// configured with it.
    pub(crate) async fn op_get_bucket_versioning(
        &self,
        req: S3Request<dto::GetBucketVersioningInput>,
    ) -> S3Result<S3Response<dto::GetBucketVersioningOutput>> {
        let bucket = self.bucket(req.input.bucket)?;
        let state = self
            .storage
            .bucket_versioning(&bucket)
            .await
            .map_err(map_backend_error)?;
        let status = match state {
            version::State::Enabled => Some(dto::BucketVersioningStatus::from_static(
                dto::BucketVersioningStatus::ENABLED,
            )),
            version::State::Suspended => Some(dto::BucketVersioningStatus::from_static(
                dto::BucketVersioningStatus::SUSPENDED,
            )),
            version::State::Unversioned => None,
        };
        Ok(S3Response::new(dto::GetBucketVersioningOutput {
            mfa_delete: None,
            status,
        }))
    }

    /// `PutBucketVersioning` — write the bucket's versioning state.
    ///
    /// A request carrying `MfaDelete=Enabled` is refused with
    /// `NotImplemented`: a local server has no MFA device, and accepting
    /// the flag would be a false protection (the design's non-goal). The
    /// refusal comes before any state read or write, so it holds whatever
    /// `Status` the request also carries and the bucket is untouched.
    /// `Disabled` is the legitimate no-op; an **unrecognized** value is
    /// refused `400 InvalidArgument`, exactly as an unrecognized `Status`
    /// is — the wire enum is closed, and a value this server cannot read
    /// must never fall through as `Disabled`.
    ///
    /// A request that names no state — `Status` absent, and `MfaDelete`
    /// absent or `Disabled` — answers `200` and changes nothing (the
    /// operator ruling: an empty request is a no-op, not a `400`). It is
    /// still a request against a bucket, so the one state read that proves
    /// the bucket exists runs, and a missing bucket answers `404`.
    ///
    /// Authorization is the plane's, not this handler's: the ACL layer
    /// decides this write exactly as it decides every other bucket write
    /// (the operator ruling — no operation-level owner check).
    pub(crate) async fn op_put_bucket_versioning(
        &self,
        req: S3Request<dto::PutBucketVersioningInput>,
    ) -> S3Result<S3Response<dto::PutBucketVersioningOutput>> {
        let bucket = self.bucket(req.input.bucket)?;
        let configuration = req.input.versioning_configuration;
        // The MFA flag is a closed enum too: the protection this server
        // cannot provide is refused, never accepted-and-ignored; the
        // legitimate no-op is read as such; anything else is refused like
        // an unrecognized `Status` (the check below).
        if let Some(mfa) = configuration.mfa_delete.as_ref() {
            if mfa.as_str() == dto::MFADelete::ENABLED {
                return Err(s3_error!(NotImplemented, "MFA delete is not supported"));
            }
            if mfa.as_str() != dto::MFADelete::DISABLED {
                return Err(s3_error!(
                    InvalidArgument,
                    "invalid mfa delete: {}",
                    mfa.as_str()
                ));
            }
        }
        let Some(status) = configuration.status else {
            // No state named: this read is the bucket check, nothing else.
            self.storage
                .bucket_versioning(&bucket)
                .await
                .map_err(map_backend_error)?;
            return Ok(S3Response::new(dto::PutBucketVersioningOutput {}));
        };
        // The wire enum is closed (AWS): an unrecognized value is refused,
        // never defaulted — the literal `Unversioned` included, which
        // would otherwise silently revert a bucket (the design: a bucket
        // can be suspended but never returns to unversioned).
        let state = if status.as_str() == dto::BucketVersioningStatus::ENABLED {
            version::State::Enabled
        } else if status.as_str() == dto::BucketVersioningStatus::SUSPENDED {
            version::State::Suspended
        } else {
            return Err(s3_error!(
                InvalidArgument,
                "invalid versioning status: {}",
                status.as_str()
            ));
        };
        self.storage
            .set_bucket_versioning(&bucket, state)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::PutBucketVersioningOutput {}))
    }

    /// `ListObjectVersions` — one page of the bucket's versions: the two
    /// wire lists AWS splits them into (`Version` and `DeleteMarker`) fed
    /// from the one entry stream the listing returns, newest first within
    /// each key.
    ///
    /// `IsLatest` is derived from position: [`version::Entry`] carries no
    /// flag, and the backend returns each key's current version first — a
    /// delete marker included — so the page's first entry of a key is that
    /// key's latest. The one exception is the entry a resumed page starts
    /// on: a request that carried a `version-id-marker` resumes *after* that
    /// version, so its first entry of the key the marker named is
    /// noncurrent.
    ///
    /// The request's `version-id-marker` reaches the listing as the raw
    /// wire string ([`version::IdMarker`]), never parsed into a
    /// [`version::Id`]: any value is a legal cursor, and the backend
    /// compares it against the cursors the listing itself wrote.
    pub(crate) async fn op_list_object_versions(
        &self,
        req: S3Request<dto::ListObjectVersionsInput>,
    ) -> S3Result<S3Response<dto::ListObjectVersionsOutput>> {
        let bucket = self.bucket(req.input.bucket)?;
        // The object listings' page-size policy, shared: AWS's default is
        // 1,000, a size below 1 is rejected unless the
        // `[s3] allow_zero_page_size` escape hatch restores the legacy
        // empty page, and the `[s3] max_keys` cap clamps the request
        // (0 = no clamp). This listing additionally applies its own hard
        // ceiling (`Self::VERSIONS_PAGE_MAX`, AWS's documented maximum for
        // the operation), which is also what bounds the walk's memory. The
        // echoed element carries the effective value.
        let max_keys = clamp_page_size(
            normalize_page_size(
                req.input.max_keys.unwrap_or(1000),
                "max-keys",
                self.caps.allow_zero_page_size,
            )?,
            self.caps.max_keys,
        )
        .min(Self::VERSIONS_PAGE_MAX);
        // An empty `delimiter=` means "no delimiter" — the boundary rule
        // has one home.
        let delimiter = normalize_delimiter(req.input.delimiter);
        let listing = self
            .storage
            .list_object_versions(
                &bucket,
                version::ListParams {
                    prefix: req.input.prefix.clone(),
                    delimiter: delimiter.clone(),
                    key_marker: req.input.key_marker.clone(),
                    version_id_marker: req.input.version_id_marker.clone().map(version::IdMarker),
                    max_keys,
                },
            )
            .await
            .map_err(map_backend_error)?;
        // A page resumed after a version starts mid-key: a
        // `version-id-marker` is sent only with a `key_marker`, and the
        // page's first entry whose key equals that marker is then a
        // *noncurrent* version of it (a lone `key_marker` skips the whole
        // key, and an unmatched marker falls back to that same skip).
        // `IsLatest` is a property of the version, so that entry must not
        // claim it; every other key's first entry is still its current
        // version.
        let resumed_key = req
            .input
            .version_id_marker
            .as_deref()
            .and(req.input.key_marker.as_deref());
        let mut versions = Vec::new();
        let mut delete_markers = Vec::new();
        let mut previous_key: Option<&str> = None;
        for entry in &listing.entries {
            let first_of_key = previous_key != Some(entry.key.as_ref());
            let is_latest = first_of_key && resumed_key != Some(entry.key.as_ref());
            previous_key = Some(entry.key.as_ref());
            let version_id = version::wire_of(entry.id.as_ref());
            if entry.is_delete_marker {
                delete_markers.push(dto::DeleteMarkerEntry {
                    is_latest: Some(is_latest),
                    key: Some(entry.key.as_ref().to_owned()),
                    last_modified: Some(Self::last_modified(entry.last_modified)),
                    version_id: Some(version_id),
                    ..Default::default()
                });
            } else {
                versions.push(dto::ObjectVersion {
                    e_tag: Some(Self::etag_wire(&entry.etag)),
                    is_latest: Some(is_latest),
                    key: Some(entry.key.as_ref().to_owned()),
                    last_modified: Some(Self::last_modified(entry.last_modified)),
                    size: Some(entry.size as i64),
                    version_id: Some(version_id),
                    ..Default::default()
                });
            }
        }
        // The cursors: `KeyMarker`/`VersionIdMarker` echo the request's own
        // (the client's place in the walk — absent when it sent none), and
        // the listing's resume pair rides `Next*`, only on a truncated page
        // (the fields' own "in a truncated response" rule). A page that
        // truncated on a rolled-up common prefix resumes at the prefix:
        // `NextKeyMarker` names it and `NextVersionIdMarker` stays empty.
        let (next_key_marker, next_version_id_marker) = if listing.is_truncated {
            (listing.next_key_marker, listing.next_version_id_marker)
        } else {
            (None, None)
        };
        Ok(S3Response::new(dto::ListObjectVersionsOutput {
            common_prefixes: Some(common_prefixes(listing.common_prefixes)),
            delete_markers: Some(delete_markers),
            delimiter,
            is_truncated: Some(listing.is_truncated),
            key_marker: req.input.key_marker,
            max_keys: Some(max_keys as i32),
            name: Some(String::from(bucket)),
            next_key_marker,
            next_version_id_marker,
            prefix: req.input.prefix,
            version_id_marker: req.input.version_id_marker,
            versions: Some(versions),
            ..Default::default()
        }))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "versioning")]
    use std::io;

    #[cfg(feature = "versioning")]
    use bytes::Bytes;
    #[cfg(feature = "versioning")]
    use futures::{StreamExt, stream};
    #[cfg(feature = "versioning")]
    use http::{HeaderMap, HeaderValue, StatusCode};
    #[cfg(feature = "versioning")]
    use s3s::{
        S3, S3Response,
        dto::{self, StreamingBlob},
    };

    use super::*;
    #[cfg(feature = "versioning")]
    use crate::_core::{
        bucket,
        storage::{ObjectOps, VersionOps},
        version,
        version::State,
    };
    #[cfg(feature = "versioning")]
    use crate::_util::testing::body;
    #[cfg(all(feature = "copy", feature = "versioning"))]
    use crate::_util::testing::read_body;
    #[cfg(all(feature = "acl", feature = "versioning"))]
    use crate::backend::testutil::{canonical_grantee, grant, group_grantee, valid_md5};
    use crate::backend::{Capabilities, testutil::setup_with_caps};
    #[cfg(feature = "versioning")]
    use crate::{_mem::MemoryStorage, backend::testutil::s3_request};

    /// The server's test graph compiles the backends' version machinery in —
    /// the `versioning` feature's forwarding entries are the only thing that
    /// makes the dev-dependency backends carry it (their self dev-dependency
    /// applies only when testing those crates themselves). A state written
    /// through `MemoryStorage` round-trips; against a stripped backend the
    /// trait's own default answers `Unversioned` and this test fails. Gated
    /// like the machinery it asks about: a stripped build keeps the backends
    /// stripped too, and that is a shape to compile, not to fail in.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn the_test_graphs_backends_carry_the_version_machinery() {
        let (backend, bucket) = setup_with_caps(Default::default()).await;
        let b = bucket::name(&bucket).unwrap();
        backend
            .storage()
            .set_bucket_versioning(&b, State::Enabled)
            .await
            .unwrap();
        assert_eq!(
            backend.storage().bucket_versioning(&b).await.unwrap(),
            State::Enabled
        );
    }

    /// The member carrier's guard: an identity whose wire form the HTTP
    /// header rules reject answers **no member** — the header carrier's own
    /// skip, through the one shared guard — instead of handing `s3s`'s
    /// renderer a value that would fail the whole response. The identifier
    /// is validated for length only, so a control character parses as an
    /// id and is exactly the unrenderable wire a test can construct.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn an_unrenderable_version_id_answers_no_member() {
        let (backend, _) = setup_with_caps(Default::default()).await;
        let unrenderable = version::Id::new("bad\nid").expect("the length rule accepts it");
        assert!(
            HeaderValue::from_str(unrenderable.as_str()).is_err(),
            "the fixture is unrenderable"
        );
        assert_eq!(
            backend.version_id_member(Some(&unrenderable), true),
            None,
            "an unrenderable identity answers no member"
        );
        // The control: a renderable identity answers its wire form under
        // the same predicate the header carrier applies.
        let renderable = version::Id::new("v1").unwrap();
        assert_eq!(
            backend
                .version_id_member(Some(&renderable), false)
                .as_deref(),
            Some("v1")
        );
    }

    /// The fixture bucket with `state` written through the storage.
    #[cfg(feature = "versioning")]
    async fn versioned_setup(state: State) -> (S3Backend<MemoryStorage>, String) {
        let (backend, b) = setup_with_caps(Default::default()).await;
        backend
            .storage()
            .set_bucket_versioning(&bucket::name(&b).unwrap(), state)
            .await
            .unwrap();
        (backend, b)
    }

    /// The versioned fixture with the checksum toggle on (the
    /// version-addressed checksum tests must opt in — off is
    /// accept-and-drop).
    #[cfg(feature = "versioning")]
    async fn versioned_checksum_setup(state: State) -> (S3Backend<MemoryStorage>, String) {
        let (backend, b) = setup_with_caps(Capabilities {
            checksum: true,
            ..Default::default()
        })
        .await;
        backend
            .storage()
            .set_bucket_versioning(&bucket::name(&b).unwrap(), state)
            .await
            .unwrap();
        (backend, b)
    }

    /// PUT one object through the handler with a client-sent SHA256
    /// checksum (the checksum toggle on).
    #[cfg(feature = "versioning")]
    async fn put_checked(
        backend: &S3Backend<MemoryStorage>,
        b: &str,
        key: &str,
        data: &'static [u8],
        sha256: &str,
    ) -> S3Response<dto::PutObjectOutput> {
        backend
            .put_object(s3_request(dto::PutObjectInput {
                bucket: b.to_string(),
                key: key.to_string(),
                body: blob(data),
                checksum_sha256: Some(sha256.to_string()),
                ..Default::default()
            }))
            .await
            .expect("the checksummed object writes")
    }

    /// A one-chunk `StreamingBlob` body.
    #[cfg(feature = "versioning")]
    fn blob(data: &'static [u8]) -> Option<StreamingBlob> {
        Some(StreamingBlob::wrap(stream::once(async move {
            Ok::<_, io::Error>(Bytes::from_static(data))
        })))
    }

    /// PUT one object through the handler.
    #[cfg(feature = "versioning")]
    async fn put(
        backend: &S3Backend<MemoryStorage>,
        b: &str,
        key: &str,
        data: &'static [u8],
    ) -> S3Response<dto::PutObjectOutput> {
        backend
            .put_object(s3_request(dto::PutObjectInput {
                bucket: b.to_string(),
                key: key.to_string(),
                body: blob(data),
                ..Default::default()
            }))
            .await
            .expect("the object writes")
    }

    /// Collect a response's `StreamingBlob` into its bytes.
    #[cfg(feature = "versioning")]
    async fn read_blob(body: StreamingBlob) -> Vec<u8> {
        let mut body = body;
        let mut out = Vec::new();
        while let Some(chunk) = body.next().await {
            out.extend_from_slice(&chunk.expect("a body chunk"));
        }
        out
    }

    /// One response header as an owned string.
    #[cfg(feature = "versioning")]
    fn header(headers: &HeaderMap, name: &str) -> Option<String> {
        headers
            .get(name)
            .map(|value| value.to_str().expect("an ASCII header").to_owned())
    }

    /// The identity a version-creating write answered.
    #[cfg(feature = "versioning")]
    fn written_version(response: &S3Response<dto::PutObjectOutput>) -> String {
        header(&response.headers, "x-amz-version-id").expect("a version-creating write answers it")
    }

    /// Review Focus 3: a `GET`/`HEAD` addressed at a delete marker's
    /// version id answers `405` with `Last-Modified` and
    /// `x-amz-delete-marker: true` and no `Allow`; a plain read of the
    /// same key answers `404` with the marker disclosed; a read of a
    /// content version is not a `405`.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_get_addressed_at_a_delete_marker_answers_405_with_the_marker_headers() {
        let (backend, b) = versioned_setup(State::Enabled).await;

        let first = written_version(&put(&backend, &b, "notes.txt", b"first").await);

        // A plain DELETE inserts a marker, and the DELETE answers the
        // marker's identity (the design's delete rules).
        let deleted = backend
            .delete_object(s3_request(dto::DeleteObjectInput {
                bucket: b.clone(),
                key: "notes.txt".into(),
                ..Default::default()
            }))
            .await
            .expect("the delete inserts a marker");
        let marker = header(&deleted.headers, "x-amz-version-id").expect("the marker's identity");
        assert_ne!(marker, first, "the delete created a new version");
        assert_eq!(
            header(&deleted.headers, "x-amz-delete-marker").as_deref(),
            Some("true"),
            "the delete answers that it made a marker"
        );

        // The plain GET: 404 NoSuchKey, with the marker disclosed.
        let err = backend
            .get_object(s3_request(dto::GetObjectInput {
                bucket: b.clone(),
                key: "notes.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchKey");
        assert_eq!(
            header(
                err.headers().expect("the marker rides the 404"),
                "x-amz-delete-marker"
            )
            .as_deref(),
            Some("true"),
        );
        assert_eq!(
            header(
                err.headers().expect("the marker rides the 404"),
                "content-type"
            )
            .as_deref(),
            Some("application/xml"),
            "the disclosed 404 still labels its XML body"
        );

        // The plain HEAD carries the same disclosure.
        let err = backend
            .head_object(s3_request(dto::HeadObjectInput {
                bucket: b.clone(),
                key: "notes.txt".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchKey");
        assert_eq!(
            header(
                err.headers().expect("the marker rides the 404"),
                "x-amz-delete-marker"
            )
            .as_deref(),
            Some("true"),
        );

        // The version-addressed GET of the marker: 405, `Last-Modified`,
        // `x-amz-delete-marker`, no `Allow`.
        let get = |version_id: String| {
            backend.get_object(s3_request(dto::GetObjectInput {
                bucket: b.clone(),
                key: "notes.txt".into(),
                version_id: Some(version_id),
                ..Default::default()
            }))
        };
        let err = get(marker.clone()).await.unwrap_err();
        assert_eq!(err.status_code(), Some(StatusCode::METHOD_NOT_ALLOWED));
        assert_eq!(err.code().as_str(), "MethodNotAllowed");
        let headers = err.headers().expect("the 405 carries the marker's headers");
        assert!(headers.contains_key("last-modified"), "the marker's stamp");
        assert_eq!(
            header(headers, "x-amz-delete-marker").as_deref(),
            Some("true")
        );
        assert_eq!(
            header(headers, "x-amz-version-id").as_deref(),
            Some(marker.as_str())
        );
        assert_eq!(
            header(headers, "content-type").as_deref(),
            Some("application/xml"),
            "the 405 still labels its XML body"
        );
        assert!(
            !headers.contains_key("allow"),
            "AWS models no Allow member for the rule"
        );

        // The HEAD twin answers the same.
        let err = backend
            .head_object(s3_request(dto::HeadObjectInput {
                bucket: b.clone(),
                key: "notes.txt".into(),
                version_id: Some(marker.clone()),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.status_code(), Some(StatusCode::METHOD_NOT_ALLOWED));
        assert_eq!(err.code().as_str(), "MethodNotAllowed");
        assert!(
            err.headers()
                .expect("the marker headers")
                .contains_key("last-modified")
        );

        // A content version is not a 405: the version reads back.
        let got = get(first.clone()).await.expect("a content version serves");
        assert_eq!(
            header(&got.headers, "x-amz-version-id").as_deref(),
            Some(first.as_str())
        );
        assert_eq!(read_blob(got.output.body.unwrap()).await, b"first");

        // If-Range still gates the Range on a version-addressed read
        // (RFC 9110 §13.1.5): a matching validator serves the range, a
        // stale one drops it for the full body.
        let ranged = |if_range: &'static str| {
            let mut req = s3_request(dto::GetObjectInput {
                bucket: b.clone(),
                key: "notes.txt".into(),
                version_id: Some(first.clone()),
                range: Some(dto::Range::Int {
                    first: 0,
                    last: Some(1),
                }),
                ..Default::default()
            });
            req.headers.insert("if-range", if_range.parse().unwrap());
            backend.get_object(req)
        };
        let got = ranged("\"8b04d5e3775d298e78455efc5ca404d5\"")
            .await
            .expect("a matching validator serves the range");
        assert_eq!(got.output.content_range.as_deref(), Some("bytes 0-1/5"));
        let got = ranged("\"deadbeefdeadbeefdeadbeefdeadbeef\"")
            .await
            .expect("a stale validator drops the range");
        assert_eq!(got.output.content_range, None);
        assert_eq!(read_blob(got.output.body.unwrap()).await, b"first");
    }

    /// Review Focus 2: a well-formed but unknown version id is `404
    /// NoSuchVersion`; a value that cannot be an id is `400`
    /// `InvalidArgument` — never collapsed into the null version.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn an_unknown_version_is_no_such_version_and_a_malformed_one_is_invalid_argument() {
        let (backend, b) = versioned_setup(State::Enabled).await;
        put(&backend, &b, "notes.txt", b"first").await;
        let get = |version_id: String| {
            backend.get_object(s3_request(dto::GetObjectInput {
                bucket: b.clone(),
                key: "notes.txt".into(),
                version_id: Some(version_id),
                ..Default::default()
            }))
        };

        let err = get("zzz".to_string()).await.unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchVersion");

        let err = get("x".repeat(version::MAX_ID_LEN + 1)).await.unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidArgument");
        assert_eq!(err.status_code(), Some(StatusCode::BAD_REQUEST));

        // The control: the literal `null` names the null version — an
        // unversioned bucket's only version, addressed by its literal —
        // and is read back as such, never as a malformed value.
        let (backend, b) = setup_with_caps(Default::default()).await;
        backend
            .storage()
            .put_object(
                &bucket::name(&b).unwrap(),
                &object_key("notes.txt"),
                body(b"before"),
            )
            .await
            .unwrap();
        let got = backend
            .get_object(s3_request(dto::GetObjectInput {
                bucket: b.clone(),
                key: "notes.txt".into(),
                version_id: Some("null".to_string()),
                ..Default::default()
            }))
            .await
            .expect("the null version is addressable by its literal");
        assert_eq!(
            header(&got.headers, "x-amz-version-id").as_deref(),
            Some("null")
        );
        assert_eq!(read_blob(got.output.body.unwrap()).await, b"before");
    }

    /// Review Focus 1 on the wire: a version-addressed request against a
    /// server whose toggle is off is refused, never answered from the
    /// current object.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_version_addressed_read_is_refused_when_the_toggle_is_off() {
        let (backend, b) = setup_with_caps(Capabilities {
            versioning: false,
            ..Default::default()
        })
        .await;
        put(&backend, &b, "notes.txt", b"first").await;
        let err = backend
            .get_object(s3_request(dto::GetObjectInput {
                bucket: b.clone(),
                key: "notes.txt".into(),
                version_id: Some("v1".to_string()),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented");
        assert_eq!(err.status_code(), Some(StatusCode::NOT_IMPLEMENTED));
        // ...and the plain read still serves.
        backend
            .get_object(s3_request(dto::GetObjectInput {
                bucket: b.clone(),
                key: "notes.txt".into(),
                ..Default::default()
            }))
            .await
            .expect("a plain request is untouched");
    }

    /// Every version-creating write answers `x-amz-version-id`: the fresh
    /// identity on an enabled bucket, the literal `null` on a suspended
    /// one — and nothing on an unversioned one.
    #[cfg(all(feature = "versioning", feature = "copy", feature = "multipart"))]
    #[tokio::test]
    async fn every_version_creating_write_reports_its_version_id() {
        use s3s::dto::{CompleteMultipartUploadInput, CopyObjectInput, CopySource};

        /// PUT, COPY and CompleteMultipartUpload of one fixture each.
        async fn writes(backend: &S3Backend<MemoryStorage>, b: &str) -> Vec<String> {
            put(backend, b, "src.txt", b"first").await;

            let copy = backend
                .copy_object(s3_request(
                    CopyObjectInput::builder()
                        .bucket(b.to_string())
                        .key("dst.txt".to_string())
                        .copy_source(CopySource::parse(&format!("{b}/src.txt")).unwrap())
                        .build()
                        .unwrap(),
                ))
                .await
                .expect("the copy writes");
            assert!(copy.output.copy_object_result.is_some());

            let upload = backend
                .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                    bucket: b.to_string(),
                    key: "big.bin".into(),
                    ..Default::default()
                }))
                .await
                .expect("the upload opens")
                .output
                .upload_id
                .expect("an upload id");
            let part = backend
                .upload_part(s3_request(dto::UploadPartInput {
                    bucket: b.to_string(),
                    key: "big.bin".into(),
                    upload_id: upload.clone(),
                    part_number: 1,
                    body: blob(b"payload"),
                    ..Default::default()
                }))
                .await
                .expect("the part uploads")
                .output
                .e_tag
                .expect("a part ETag");
            let complete = backend
                .complete_multipart_upload(s3_request(CompleteMultipartUploadInput {
                    bucket: b.to_string(),
                    key: "big.bin".into(),
                    upload_id: upload,
                    multipart_upload: Some(dto::CompletedMultipartUpload {
                        parts: Some(vec![dto::CompletedPart {
                            part_number: Some(1),
                            e_tag: Some(part),
                            ..Default::default()
                        }]),
                    }),
                    ..Default::default()
                }))
                .await
                .expect("the upload completes");

            let put_id = written_version(&put(backend, b, "again.txt", b"second").await);
            vec![
                put_id,
                header(&copy.headers, "x-amz-version-id").expect("the copy answers it"),
                header(&complete.headers, "x-amz-version-id").expect("the complete answers it"),
            ]
        }

        let (backend, b) = versioned_setup(State::Enabled).await;
        let ids = writes(&backend, &b).await;
        for id in &ids {
            assert!(
                version::Id::new(id).is_ok(),
                "an enabled bucket's version id is a legal wire value: {id}"
            );
        }
        assert_ne!(ids[0], ids[1], "each write mints its own version");
        assert_ne!(ids[1], ids[2], "each write mints its own version");

        // The suspended bucket writes the null version in place: every
        // write answers the literal `null`.
        let (backend, b) = versioned_setup(State::Suspended).await;
        for id in writes(&backend, &b).await {
            assert_eq!(
                id, "null",
                "a suspended bucket's write names the null version"
            );
        }

        // An unversioned bucket records nothing, so it answers nothing.
        let (backend, b) = setup_with_caps(Default::default()).await;
        let response = put(&backend, &b, "plain.txt", b"first").await;
        assert_eq!(header(&response.headers, "x-amz-version-id"), None);
    }

    /// The read half of the version-identity rule: a plain `GET`/`HEAD` of
    /// a recording bucket answers `x-amz-version-id` with the identity of
    /// the version it served — the write's own id on an enabled bucket,
    /// the literal `null` on a suspended one — and an unversioned bucket's
    /// plain reads answer no header. The write-side leg above pins the
    /// same helper through the writes; the state now rides the producer's
    /// `Info` (`bucket_records_versions`), and these legs hold the wire
    /// behavior that carrier exists to keep.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_plain_read_reports_the_served_version_id() {
        /// The `x-amz-version-id` a plain GET and HEAD of `key` answer.
        async fn plain_reads(
            backend: &S3Backend<MemoryStorage>,
            b: &str,
            key: &str,
        ) -> Vec<Option<String>> {
            let get = backend
                .get_object(s3_request(dto::GetObjectInput {
                    bucket: b.to_string(),
                    key: key.to_string(),
                    ..Default::default()
                }))
                .await
                .expect("the plain read serves");
            let head = backend
                .head_object(s3_request(dto::HeadObjectInput {
                    bucket: b.to_string(),
                    key: key.to_string(),
                    ..Default::default()
                }))
                .await
                .expect("the plain head serves");
            vec![
                header(&get.headers, "x-amz-version-id"),
                header(&head.headers, "x-amz-version-id"),
            ]
        }

        let (backend, b) = versioned_setup(State::Enabled).await;
        let written = written_version(&put(&backend, &b, "notes.txt", b"first").await);
        for id in plain_reads(&backend, &b, "notes.txt").await {
            assert_eq!(
                id.as_deref(),
                Some(written.as_str()),
                "an enabled bucket's plain read names the version it served"
            );
        }

        let (backend, b) = versioned_setup(State::Suspended).await;
        put(&backend, &b, "notes.txt", b"first").await;
        for id in plain_reads(&backend, &b, "notes.txt").await {
            assert_eq!(
                id.as_deref(),
                Some("null"),
                "a suspended bucket's plain read names the null version it served"
            );
        }

        let (backend, b) = setup_with_caps(Default::default()).await;
        put(&backend, &b, "plain.txt", b"first").await;
        for id in plain_reads(&backend, &b, "plain.txt").await {
            assert_eq!(
                id, None,
                "an unversioned bucket's plain read answers no version header"
            );
        }
    }

    /// `DeleteObjects` threads each entry's version id through the same
    /// delete path and answers the `Marker` triple the single-object
    /// DELETE renders as headers.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_batch_delete_threads_version_ids_and_answers_markers() {
        let (backend, b) = versioned_setup(State::Enabled).await;
        let older = written_version(&put(&backend, &b, "notes.txt", b"first").await);
        let newer = written_version(&put(&backend, &b, "notes.txt", b"second").await);
        put(&backend, &b, "plain.txt", b"body").await;

        let out = backend
            .delete_objects(s3_request(
                dto::DeleteObjectsInput::builder()
                    .bucket(b.clone())
                    .delete(dto::Delete {
                        objects: vec![
                            dto::ObjectIdentifier {
                                key: "notes.txt".into(),
                                version_id: Some(older.clone()),
                                ..Default::default()
                            },
                            dto::ObjectIdentifier {
                                key: "notes.txt".into(),
                                version_id: Some(newer.clone()),
                                ..Default::default()
                            },
                            dto::ObjectIdentifier {
                                key: "plain.txt".into(),
                                ..Default::default()
                            },
                        ],
                        quiet: None,
                    })
                    .build()
                    .expect("the input builds"),
            ))
            .await
            .expect("the batch deletes");
        assert_eq!(out.output.errors, None, "every entry deletes");
        let deleted = out.output.deleted.expect("three entries");
        assert_eq!(deleted.len(), 3);

        // A content version deleted by id: the removed identity, no marker.
        assert_eq!(deleted[0].version_id.as_deref(), Some(older.as_str()));
        assert_eq!(deleted[0].delete_marker, None);
        assert_eq!(deleted[0].delete_marker_version_id, None);
        assert_eq!(deleted[1].version_id.as_deref(), Some(newer.as_str()));
        assert_eq!(deleted[1].delete_marker, None);

        // The plain entry created a marker: no request version id, the
        // created marker named by `DeleteMarkerVersionId` (AWS's shape).
        assert_eq!(deleted[2].version_id, None);
        assert_eq!(deleted[2].delete_marker, Some(true));
        let marker = deleted[2]
            .delete_marker_version_id
            .as_deref()
            .expect("the created marker's identity");
        assert!(version::Id::new(marker).is_ok(), "{marker}");
    }

    /// A batch entry naming the literal `null` addresses the null version:
    /// the success entry names the version the request asked for
    /// (`VersionId: "null"`, AWS's shape) and removes exactly it — the
    /// presence the address carries, not the value it spells. The plain
    /// entry in the same batch names no version, the contrast.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_batch_entry_naming_the_null_literal_answers_the_null_identity() {
        let (backend, b) = versioned_setup(State::Suspended).await;
        put(&backend, &b, "zero.txt", b"null-body").await;

        let out = backend
            .delete_objects(s3_request(
                dto::DeleteObjectsInput::builder()
                    .bucket(b.clone())
                    .delete(dto::Delete {
                        objects: vec![
                            dto::ObjectIdentifier {
                                key: "zero.txt".into(),
                                version_id: Some("null".into()),
                                ..Default::default()
                            },
                            dto::ObjectIdentifier {
                                key: "zero.txt".into(),
                                ..Default::default()
                            },
                        ],
                        quiet: None,
                    })
                    .build()
                    .expect("the input builds"),
            ))
            .await
            .expect("the batch deletes");
        assert_eq!(out.output.errors, None, "both entries delete");
        let deleted = out.output.deleted.expect("two entries");

        // The null-literal entry removed that content version; the plain
        // entry (a marker on the now-unmarkered key) names none — the same
        // pair the single-object DELETE renders.
        assert_eq!(deleted[0].version_id.as_deref(), Some("null"));
        assert_eq!(deleted[0].delete_marker, None, "a content version");
        assert_eq!(deleted[0].delete_marker_version_id, None);
        assert_eq!(deleted[1].version_id, None, "a plain entry names none");
        assert_eq!(deleted[1].delete_marker, Some(true));
    }

    /// A failed batch entry names the version the request asked to delete
    /// (AWS's `Error` element carries `VersionId`), not the current
    /// object: an unknown id answers `NoSuchVersion` with the requested
    /// value on the entry.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_failed_batch_entry_names_the_version_it_addressed() {
        let (backend, b) = versioned_setup(State::Enabled).await;
        put(&backend, &b, "notes.txt", b"body").await;

        let out = backend
            .delete_objects(s3_request(
                dto::DeleteObjectsInput::builder()
                    .bucket(b.clone())
                    .delete(dto::Delete {
                        objects: vec![dto::ObjectIdentifier {
                            key: "notes.txt".into(),
                            version_id: Some("zzz".into()),
                            ..Default::default()
                        }],
                        quiet: None,
                    })
                    .build()
                    .expect("the input builds"),
            ))
            .await
            .expect("the batch answers");
        assert_eq!(out.output.deleted, None, "nothing was deleted");
        let errors = out.output.errors.expect("one error entry");
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].code.as_deref(), Some("NoSuchVersion"));
        assert_eq!(errors[0].key.as_deref(), Some("notes.txt"));
        assert_eq!(
            errors[0].version_id.as_deref(),
            Some("zzz"),
            "the entry names the version it was asked to delete"
        );
    }

    /// The versioned conditional arms: `If-None-Match: *` applies to the
    /// current version only, and `If-Match` against no current version is
    /// `404`, not `412` — on a bucket that records versions.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn conditional_writes_see_only_the_current_version() {
        async fn put_if(
            backend: &S3Backend<MemoryStorage>,
            b: &str,
            key: &str,
            if_match: Option<&str>,
            if_none_match: Option<&str>,
        ) -> S3Result<S3Response<dto::PutObjectOutput>> {
            backend
                .put_object(s3_request(dto::PutObjectInput {
                    bucket: b.to_string(),
                    key: key.to_string(),
                    body: blob(b"x"),
                    if_match: if_match.map(|v| v.parse().unwrap()),
                    if_none_match: if_none_match.map(|v| v.parse().unwrap()),
                    ..Default::default()
                }))
                .await
        }

        let (backend, b) = versioned_setup(State::Enabled).await;
        put(&backend, &b, "notes.txt", b"first").await;

        // A live current version: `If-None-Match: *` fails.
        let err = put_if(&backend, &b, "notes.txt", None, Some("*"))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "PreconditionFailed");

        // The current version is a delete marker: `If-None-Match: *`
        // succeeds (no current version to match).
        backend
            .delete_object(s3_request(dto::DeleteObjectInput {
                bucket: b.clone(),
                key: "notes.txt".into(),
                ..Default::default()
            }))
            .await
            .expect("the marker inserts");
        put_if(&backend, &b, "notes.txt", None, Some("*"))
            .await
            .expect("a marker is no current version");

        // An `If-Match` against no current version is 404 — on the
        // never-existing key and on the marked one alike.
        let err = put_if(&backend, &b, "absent.txt", Some("*"), None)
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchKey");
        assert_eq!(err.status_code(), Some(StatusCode::NOT_FOUND));

        backend
            .delete_object(s3_request(dto::DeleteObjectInput {
                bucket: b.clone(),
                key: "notes.txt".into(),
                ..Default::default()
            }))
            .await
            .expect("the second marker inserts");
        let err = put_if(&backend, &b, "notes.txt", Some("*"), None)
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchKey");

        // A bucket that records nothing keeps the legacy 412.
        let (backend, b) = setup_with_caps(Default::default()).await;
        let err = put_if(&backend, &b, "absent.txt", Some("*"), None)
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "PreconditionFailed");
    }

    /// A version-addressed `DELETE` evaluates its content conditionals
    /// (`If-Match` / `x-amz-if-match-size`) against the version it names,
    /// never the key's current object; a marker address answers 412 under
    /// any provided header (it has no content), and an unknown version
    /// keeps its 404. The plain path is untouched (pinned by the suites
    /// that keep passing).
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_version_addressed_delete_gates_the_named_version() {
        async fn delete(
            backend: &S3Backend<MemoryStorage>,
            b: &str,
            version_id: &str,
            if_match: Option<String>,
            if_match_size: Option<i64>,
        ) -> S3Result<S3Response<dto::DeleteObjectOutput>> {
            backend
                .delete_object(s3_request(dto::DeleteObjectInput {
                    bucket: b.to_string(),
                    key: "notes.txt".into(),
                    version_id: Some(version_id.to_string()),
                    if_match: if_match.map(|v| v.parse().unwrap()),
                    if_match_size,
                    ..Default::default()
                }))
                .await
        }

        let (backend, b) = versioned_setup(State::Enabled).await;
        let bn = bucket::name(&b).unwrap();
        let first = written_version(&put(&backend, &b, "notes.txt", b"first").await);
        put(&backend, &b, "notes.txt", b"second").await;
        let first_id = version::id(&first).unwrap();
        let addressed = match backend
            .storage()
            .head_object_version(&bn, &object_key("notes.txt"), Some(&first_id))
            .await
            .unwrap()
        {
            version::HeadResult::Object(info) => info,
            other => panic!("the first write's version is content, not {other:?}"),
        };
        let current = backend
            .storage()
            .head_object(&bn, &object_key("notes.txt"))
            .await
            .unwrap();
        assert_ne!(
            addressed.etag, current.etag,
            "the fixture's versions are distinguishable"
        );

        // The ETag the CURRENT object satisfies, aimed at the OLDER
        // version: evaluating against the current object would let this
        // delete pass — evaluating against the addressed version, it 412s.
        let err = delete(
            &backend,
            &b,
            &first,
            Some(format!("\"{}\"", current.etag.as_str())),
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code().as_str(), "PreconditionFailed");
        assert_eq!(err.status_code(), Some(StatusCode::PRECONDITION_FAILED));

        // The same rule through the size header: the current object's
        // size (6) never gates the addressed 5-byte version.
        let err = delete(&backend, &b, &first, None, Some(current.size as i64))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "PreconditionFailed");

        // The addressed version's own ETag matches: the delete proceeds
        // and names the version it removed.
        let removed = delete(
            &backend,
            &b,
            &first,
            Some(format!("\"{}\"", addressed.etag.as_str())),
            None,
        )
        .await
        .expect("the addressed version's own condition is satisfied");
        assert_eq!(
            header(&removed.headers, "x-amz-version-id").as_deref(),
            Some(first.as_str())
        );
        assert_eq!(header(&removed.headers, "x-amz-delete-marker"), None);

        // A marker is addressable but has no content: any provided
        // conditional fails honestly, and the marker survives it.
        let stacked = backend
            .delete_object(s3_request(dto::DeleteObjectInput {
                bucket: b.clone(),
                key: "notes.txt".into(),
                ..Default::default()
            }))
            .await
            .expect("a plain delete stacks a marker");
        let marker = header(&stacked.headers, "x-amz-version-id").expect("the marker's identity");
        let err = delete(&backend, &b, &marker, Some("*".to_string()), None)
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "PreconditionFailed");
        assert_eq!(err.status_code(), Some(StatusCode::PRECONDITION_FAILED));
        let removed = delete(&backend, &b, &marker, None, None)
            .await
            .expect("the unconditional marker delete is untouched");
        assert_eq!(
            header(&removed.headers, "x-amz-delete-marker").as_deref(),
            Some("true")
        );

        // An unknown version keeps its own 404 under a condition — a
        // condition cannot rescue a version that is not there.
        let err = delete(&backend, &b, "zzz", Some("*".to_string()), None)
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchVersion");
        assert_eq!(err.status_code(), Some(StatusCode::NOT_FOUND));
    }

    /// The corpus's stacking rule, asserted: two plain DELETEs of one key
    /// leave TWO delete markers in the version listing — each plain
    /// delete is a new marker on top, the newer one latest, and none is
    /// collapsed into the one below it.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn repeated_plain_deletes_stack_markers_in_the_listing() {
        async fn plain_delete(
            backend: &S3Backend<MemoryStorage>,
            b: &str,
        ) -> S3Response<dto::DeleteObjectOutput> {
            backend
                .delete_object(s3_request(dto::DeleteObjectInput {
                    bucket: b.to_string(),
                    key: "notes.txt".into(),
                    ..Default::default()
                }))
                .await
                .expect("the plain delete stacks a marker")
        }

        let (backend, b) = versioned_setup(State::Enabled).await;
        let content = written_version(&put(&backend, &b, "notes.txt", b"body").await);
        let first = header(
            &plain_delete(&backend, &b).await.headers,
            "x-amz-version-id",
        )
        .expect("the first marker's identity");
        let second = header(
            &plain_delete(&backend, &b).await.headers,
            "x-amz-version-id",
        )
        .expect("the second marker's identity");
        assert_ne!(first, second, "every plain delete mints a new marker");

        let out = backend
            .list_object_versions(s3_request(dto::ListObjectVersionsInput {
                bucket: b.clone(),
                ..Default::default()
            }))
            .await
            .expect("the listing serves")
            .output;
        let markers = out.delete_markers.expect("the marker list is present");
        let listed: Vec<(&str, Option<bool>)> = markers
            .iter()
            .map(|m| (m.version_id.as_deref().unwrap(), m.is_latest))
            .collect();
        assert_eq!(
            listed,
            [(second.as_str(), Some(true)), (first.as_str(), Some(false)),],
            "both markers are listed, newest first"
        );
        assert!(
            markers
                .iter()
                .all(|m| m.key.as_deref() == Some("notes.txt") && m.last_modified.is_some()),
            "each marker carries its own key and stamp"
        );
        let versions = out.versions.expect("the version list is present");
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].version_id.as_deref(), Some(content.as_str()));
        assert_eq!(
            versions[0].is_latest,
            Some(false),
            "the stacked markers hide the content version"
        );
    }

    /// The disclosure half of the stacking rule: after repeated plain
    /// deletes the plain `404` of the marked key still carries
    /// `x-amz-delete-marker: true` — the listing leg above pins the
    /// markers, this pins the header the miss probe discloses.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn repeated_plain_deletes_keep_the_disclosure_header_on_the_plain_miss() {
        /// The plain `GET` and `HEAD` of a marked key, both expected to
        /// miss.
        async fn plain_misses(backend: &S3Backend<MemoryStorage>, b: &str) -> Vec<S3Error> {
            let get = backend
                .get_object(s3_request(dto::GetObjectInput {
                    bucket: b.to_string(),
                    key: "notes.txt".into(),
                    ..Default::default()
                }))
                .await
                .expect_err("the plain read misses");
            let head = backend
                .head_object(s3_request(dto::HeadObjectInput {
                    bucket: b.to_string(),
                    key: "notes.txt".into(),
                    ..Default::default()
                }))
                .await
                .expect_err("the plain head misses");
            vec![get, head]
        }

        let (backend, b) = versioned_setup(State::Enabled).await;
        put(&backend, &b, "notes.txt", b"body").await;
        for _ in 0..2 {
            backend
                .delete_object(s3_request(dto::DeleteObjectInput {
                    bucket: b.clone(),
                    key: "notes.txt".into(),
                    ..Default::default()
                }))
                .await
                .expect("the plain delete stacks a marker");
        }
        for err in plain_misses(&backend, &b).await {
            assert_eq!(err.code().as_str(), "NoSuchKey");
            assert_eq!(
                header(
                    err.headers().expect("the marker rides the 404"),
                    "x-amz-delete-marker"
                )
                .as_deref(),
                Some("true"),
                "the plain miss of a stacked-marker key discloses the marker"
            );
            assert_eq!(
                header(
                    err.headers().expect("the marker rides the 404"),
                    "content-type"
                )
                .as_deref(),
                Some("application/xml"),
                "the disclosed 404 still labels its XML body"
            );
        }
    }

    /// A copy source naming a version reads THAT version's bytes — the
    /// destination is an ordinary versioned write.
    #[cfg(all(feature = "versioning", feature = "copy"))]
    #[tokio::test]
    async fn a_copy_reads_the_source_version_it_names() {
        use s3s::dto::{CopyObjectInput, CopySource};

        let (backend, b) = versioned_setup(State::Enabled).await;
        let first = written_version(&put(&backend, &b, "src.txt", b"first").await);
        put(&backend, &b, "src.txt", b"second").await;

        let copy = backend
            .copy_object(s3_request(
                CopyObjectInput::builder()
                    .bucket(b.to_string())
                    .key("dst.txt".to_string())
                    .copy_source(
                        CopySource::parse(&format!("{b}/src.txt?versionId={first}")).unwrap(),
                    )
                    .build()
                    .unwrap(),
            ))
            .await
            .expect("the version-addressed copy writes");
        let dst = header(&copy.headers, "x-amz-version-id")
            .expect("the copy answers the destination's identity");
        assert_ne!(dst, first, "the destination is its own version");

        let got = backend
            .storage()
            .get_object(&bucket::name(&b).unwrap(), &object_key("dst.txt"), None)
            .await
            .expect("the destination reads");
        assert_eq!(
            read_body(got.body).await.unwrap(),
            b"first",
            "the copy's bytes are the version it named"
        );

        // A source version that names nothing is `404 NoSuchVersion`.
        let err = backend
            .copy_object(s3_request(
                CopyObjectInput::builder()
                    .bucket(b.to_string())
                    .key("dst.txt".to_string())
                    .copy_source(CopySource::parse(&format!("{b}/src.txt?versionId=zzz")).unwrap())
                    .build()
                    .unwrap(),
            ))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchVersion");
    }

    /// T155: a version-addressed copy records the source version's
    /// recorded checksum on the destination — the ordinary copy path's
    /// carry (`checksum = info.checksum`), threaded through the staged
    /// write's tee slot, so the bytes are streamed once. The source's
    /// CURRENT version records none, so a destination that echoes a
    /// checksum can only have taken it from the addressed version.
    #[cfg(all(feature = "versioning", feature = "copy"))]
    #[tokio::test]
    async fn a_version_addressed_copy_records_the_destination_checksum() {
        use s3s::dto::{CopyObjectInput, CopySource};

        const SHA256_FIRST: &str = "p5N7ZLjKpY8Dchu2us9ceMsjX+vg5wsbhM2ZVBRhoI4=";
        let (backend, b) = versioned_checksum_setup(State::Enabled).await;
        let first =
            written_version(&put_checked(&backend, &b, "src.txt", b"first", SHA256_FIRST).await);
        put(&backend, &b, "src.txt", b"second").await;

        backend
            .copy_object(s3_request(
                CopyObjectInput::builder()
                    .bucket(b.to_string())
                    .key("dst.txt".to_string())
                    .copy_source(
                        CopySource::parse(&format!("{b}/src.txt?versionId={first}")).unwrap(),
                    )
                    .checksum_algorithm(Some("SHA256".parse().unwrap()))
                    .build()
                    .unwrap(),
            ))
            .await
            .expect("the version-addressed copy writes");

        // The destination's checksum reads back through the HEAD echo ...
        let head = backend
            .head_object(s3_request(dto::HeadObjectInput {
                bucket: b.clone(),
                key: "dst.txt".into(),
                ..Default::default()
            }))
            .await
            .expect("the destination heads")
            .output;
        assert_eq!(head.checksum_sha256.as_deref(), Some(SHA256_FIRST));
        // ... and through the GetObjectAttributes Checksum attribute.
        let out = backend
            .get_object_attributes(s3_request(dto::GetObjectAttributesInput {
                bucket: b.clone(),
                key: "dst.txt".into(),
                object_attributes: vec!["Checksum".parse().unwrap()],
                ..Default::default()
            }))
            .await
            .expect("the destination's attributes serve")
            .output;
        assert_eq!(
            out.checksum
                .expect("the copy recorded the checksum")
                .checksum_sha256
                .as_deref(),
            Some(SHA256_FIRST)
        );
    }

    /// Residual 1 of the wave-C review: a version-addressed copy of a
    /// COMPOSITE (multipart-completed) source version records the
    /// destination's checksum under the source's kind — exactly what the
    /// plain path's carried `Recorded` records for the same source —
    /// never the staged write's `FULL_OBJECT` default. The plain copy
    /// runs while the completed object is current, then the source is
    /// overwritten, so the version-addressed copy can only have taken the
    /// kind from the addressed version's row.
    #[cfg(all(feature = "versioning", feature = "copy", feature = "multipart"))]
    #[tokio::test]
    async fn a_version_addressed_copy_keeps_the_source_versions_checksum_kind() {
        use s3s::dto::{
            CompleteMultipartUploadInput, CopyObjectInput, CopySource, CreateMultipartUploadInput,
            UploadPartInput,
        };

        use crate::_core::checksum;

        let (backend, b) = versioned_checksum_setup(State::Enabled).await;
        // The source: a multipart-completed object. The create names the
        // algorithm (an upload-level spec), so the completion records the
        // derived COMPOSITE value.
        let upload = backend
            .create_multipart_upload(s3_request(CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                checksum_algorithm: Some("SHA256".parse().unwrap()),
                ..Default::default()
            }))
            .await
            .expect("the upload opens")
            .output
            .upload_id
            .expect("an upload id");
        let part = backend
            .upload_part(s3_request(UploadPartInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload.clone(),
                part_number: 1,
                body: blob(b"hello"),
                ..Default::default()
            }))
            .await
            .expect("the part uploads")
            .output
            .e_tag
            .expect("a part ETag");
        let complete = backend
            .complete_multipart_upload(s3_request(CompleteMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload,
                multipart_upload: Some(dto::CompletedMultipartUpload {
                    parts: Some(vec![dto::CompletedPart {
                        part_number: Some(1),
                        e_tag: Some(part),
                        ..Default::default()
                    }]),
                }),
                ..Default::default()
            }))
            .await
            .expect("the upload completes");
        let composed =
            header(&complete.headers, "x-amz-version-id").expect("the completion's version");

        // The plain path's answer for the same source, while it is the
        // current object: the carried `Recorded` keeps COMPOSITE.
        backend
            .copy_object(s3_request(
                CopyObjectInput::builder()
                    .bucket(b.to_string())
                    .key("plain.txt".to_string())
                    .copy_source(CopySource::parse(&format!("{b}/big.bin")).unwrap())
                    .build()
                    .unwrap(),
            ))
            .await
            .expect("the plain copy writes");

        // Overwrite the source so the addressed version is no longer the
        // current object — the discriminating case.
        put(&backend, &b, "big.bin", b"second").await;
        backend
            .copy_object(s3_request(
                CopyObjectInput::builder()
                    .bucket(b.to_string())
                    .key("versioned.txt".to_string())
                    .copy_source(
                        CopySource::parse(&format!("{b}/big.bin?versionId={composed}")).unwrap(),
                    )
                    .build()
                    .unwrap(),
            ))
            .await
            .expect("the version-addressed copy writes");

        let b_name = bucket::name(&b).unwrap();
        let plain = backend
            .storage()
            .head_object(&b_name, &object_key("plain.txt"))
            .await
            .expect("the plain destination heads");
        let versioned = backend
            .storage()
            .head_object(&b_name, &object_key("versioned.txt"))
            .await
            .expect("the versioned destination heads");
        assert_eq!(
            plain.etag, versioned.etag,
            "both destinations hold the same bytes"
        );
        let plain = plain.checksum.expect("the plain copy recorded a checksum");
        let versioned = versioned
            .checksum
            .expect("the version-addressed copy recorded a checksum");
        assert_eq!(
            plain.kind,
            checksum::Type::Composite,
            "the plain path carries the multipart source's kind"
        );
        assert_eq!(
            versioned.kind,
            checksum::Type::Composite,
            "the version-addressed copy records the source version's kind"
        );
        assert_eq!(
            versioned, plain,
            "both copies of the same source record the same value and kind"
        );
    }

    /// `UploadPartCopy` reads the version its source names, range
    /// included — the copy-part primitive cannot.
    #[cfg(all(feature = "versioning", feature = "multipart", feature = "copy"))]
    #[tokio::test]
    async fn an_upload_part_copy_reads_the_source_version_it_names() {
        use s3s::dto::{CompleteMultipartUploadInput, CopySource, UploadPartCopyInput};

        let (backend, b) = versioned_setup(State::Enabled).await;
        let first = written_version(&put(&backend, &b, "src.txt", b"first").await);
        put(&backend, &b, "src.txt", b"second").await;

        let upload = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                ..Default::default()
            }))
            .await
            .expect("the upload opens")
            .output
            .upload_id
            .expect("an upload id");
        let part = backend
            .upload_part_copy(s3_request(UploadPartCopyInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload.clone(),
                part_number: 1,
                copy_source: CopySource::parse(&format!("{b}/src.txt?versionId={first}")).unwrap(),
                copy_source_range: Some("bytes=0-2".to_string()),
                copy_source_if_match: None,
                copy_source_if_modified_since: None,
                copy_source_if_none_match: None,
                copy_source_if_unmodified_since: None,
                copy_source_sse_customer_algorithm: None,
                copy_source_sse_customer_key: None,
                copy_source_sse_customer_key_md5: None,
                expected_bucket_owner: None,
                expected_source_bucket_owner: None,
                request_payer: None,
                sse_customer_algorithm: None,
                sse_customer_key: None,
                sse_customer_key_md5: None,
            }))
            .await
            .expect("the version-addressed part copies")
            .output
            .copy_part_result
            .expect("a part result")
            .e_tag
            .expect("a part ETag");
        backend
            .complete_multipart_upload(s3_request(CompleteMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload,
                multipart_upload: Some(dto::CompletedMultipartUpload {
                    parts: Some(vec![dto::CompletedPart {
                        part_number: Some(1),
                        e_tag: Some(part),
                        ..Default::default()
                    }]),
                }),
                ..Default::default()
            }))
            .await
            .expect("the upload completes");

        let got = backend
            .storage()
            .get_object(&bucket::name(&b).unwrap(), &object_key("big.bin"), None)
            .await
            .expect("the composed object reads");
        assert_eq!(
            read_body(got.body).await.unwrap(),
            b"fir",
            "the part is the named version's bytes, cut to the copy range"
        );
    }

    /// Operator directive 2026-10-05, superseding R12's refusal: a
    /// version-addressed `GetObjectAttributes` describes the VERSION it
    /// names — that version's own ETag, size, storage class and identity —
    /// never the current object; an unknown id is `404 NoSuchVersion`, a
    /// malformed value the shared `400 InvalidArgument`, and the plain
    /// request is untouched.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn version_addressed_attributes_describe_the_version_they_name() {
        async fn attributes(
            backend: &S3Backend<MemoryStorage>,
            b: &str,
            version_id: Option<String>,
            attrs: &[&str],
        ) -> S3Result<S3Response<dto::GetObjectAttributesOutput>> {
            backend
                .get_object_attributes(s3_request(dto::GetObjectAttributesInput {
                    bucket: b.to_string(),
                    key: "notes.txt".into(),
                    version_id,
                    object_attributes: attrs.iter().map(|a| a.parse().unwrap()).collect(),
                    ..Default::default()
                }))
                .await
        }

        let (backend, b) = versioned_setup(State::Enabled).await;
        let first = written_version(&put(&backend, &b, "notes.txt", b"first").await);
        // The current object differs from the first version in size and
        // ETag — the discriminating case: an answer composed from the
        // current object fails every assertion below.
        let current = written_version(&put(&backend, &b, "notes.txt", b"second!").await);

        let out = attributes(
            &backend,
            &b,
            Some(first.clone()),
            &["ETag", "ObjectSize", "StorageClass"],
        )
        .await
        .expect("the version-addressed request serves")
        .output;
        assert_eq!(
            out.e_tag.unwrap().as_strong().unwrap(),
            "8b04d5e3775d298e78455efc5ca404d5",
            "the first version's ETag, not the current object's"
        );
        assert_eq!(out.object_size, Some(5), "the first version's size");
        assert_eq!(
            out.storage_class.as_ref().map(|c| c.as_str()),
            Some("STANDARD"),
            "the current path's fixed storage class"
        );
        assert_eq!(
            out.version_id.as_deref(),
            Some(first.as_str()),
            "the answer names the version it described"
        );
        assert!(
            out.object_parts.is_none(),
            "no per-version part list exists — absent, never invented"
        );

        // The plain request describes the current object and names that
        // object's identity — the current version, not the addressed one.
        let out = attributes(&backend, &b, None, &["ETag", "ObjectSize"])
            .await
            .expect("the plain request serves")
            .output;
        assert_eq!(
            out.e_tag.unwrap().as_strong().unwrap(),
            "26de793d084efdd1c4134652c5494af1",
            "the current object's own ETag"
        );
        assert_eq!(out.object_size, Some(7));
        assert_eq!(
            out.version_id.as_deref(),
            Some(current.as_str()),
            "the plain answer names the current version"
        );

        // A malformed value: the request-shape 400, through the same parse
        // every version-addressed site takes.
        let err = attributes(
            &backend,
            &b,
            Some("x".repeat(version::MAX_ID_LEN + 1)),
            &["ETag"],
        )
        .await
        .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidArgument");
        assert_eq!(err.status_code(), Some(StatusCode::BAD_REQUEST));

        // A well-formed but unknown id: the contract's 404 NoSuchVersion.
        let err = attributes(&backend, &b, Some("zzz".to_string()), &["ETag"])
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchVersion");
        assert_eq!(err.status_code(), Some(StatusCode::NOT_FOUND));

        // The literal `null` is a version address too: an enabled bucket
        // mints unique ids, so no null version exists and the answer is
        // the same NoSuchVersion — never the 501, never the current
        // object.
        let err = attributes(&backend, &b, Some("null".to_string()), &["ETag"])
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchVersion");

        // And the toggle-off refusal precedes the parse (a version on a
        // server without versioning is the family's exact text).
        let (backend, b) = setup_with_caps(Capabilities {
            versioning: false,
            ..Default::default()
        })
        .await;
        put(&backend, &b, "notes.txt", b"first").await;
        let err = attributes(&backend, &b, Some("zzz".to_string()), &["ETag"])
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented");
        assert_eq!(err.message(), Some("versioning is disabled"));
    }

    /// The plain `GetObjectAttributes` answer carries the served version's
    /// identity — the R23-class rule plain GET/HEAD/tagging already follow,
    /// and AWS answers `x-amz-version-id` on GetObjectAttributes too: the
    /// head the handler reads reports it on the `Info`, and s3s renders the
    /// member as that header. A suspended bucket names the null version it
    /// served, and an unversioned bucket answers no member at all.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn plain_attributes_report_the_served_identity() {
        async fn attributes(
            backend: &S3Backend<MemoryStorage>,
            b: &str,
            key: &str,
        ) -> dto::GetObjectAttributesOutput {
            backend
                .get_object_attributes(s3_request(dto::GetObjectAttributesInput {
                    bucket: b.to_string(),
                    key: key.to_string(),
                    object_attributes: vec!["ETag".parse().unwrap()],
                    ..Default::default()
                }))
                .await
                .expect("the plain request serves")
                .output
        }

        // An enabled bucket: the plain answer names the current version,
        // and follows an overwrite to the new current.
        let (backend, b) = versioned_setup(State::Enabled).await;
        let first = written_version(&put(&backend, &b, "notes.txt", b"first").await);
        assert_eq!(
            attributes(&backend, &b, "notes.txt")
                .await
                .version_id
                .as_deref(),
            Some(first.as_str()),
            "the plain answer names the version it served"
        );
        let second = written_version(&put(&backend, &b, "notes.txt", b"second").await);
        assert_eq!(
            attributes(&backend, &b, "notes.txt")
                .await
                .version_id
                .as_deref(),
            Some(second.as_str()),
            "and follows the overwrite to the new current version"
        );

        // A suspended bucket writes — and serves — the null version, and
        // the answer names it in place.
        let (backend, b) = versioned_setup(State::Suspended).await;
        put(&backend, &b, "zero.txt", b"body").await;
        assert_eq!(
            attributes(&backend, &b, "zero.txt")
                .await
                .version_id
                .as_deref(),
            Some("null"),
            "the suspended bucket's plain answer names the null version"
        );

        // An unversioned bucket answers no version identity (the same
        // predicate the tagging path applies).
        let (backend, b) = setup_with_caps(Default::default()).await;
        put(&backend, &b, "plain.txt", b"body").await;
        assert_eq!(
            attributes(&backend, &b, "plain.txt").await.version_id,
            None,
            "an unversioned bucket answers no identity member"
        );
    }

    /// A version-addressed `GetObjectAttributes` follows the family's
    /// delete-marker rule: a marker address is the shared `405
    /// MethodNotAllowed` — the marker's `Last-Modified`,
    /// `x-amz-delete-marker` and `x-amz-version-id` headers, no `Allow` —
    /// not an attribute set.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn version_addressed_attributes_follow_the_marker_rule() {
        let (backend, b) = versioned_setup(State::Enabled).await;
        put(&backend, &b, "notes.txt", b"first").await;
        let deleted = backend
            .delete_object(s3_request(dto::DeleteObjectInput {
                bucket: b.clone(),
                key: "notes.txt".into(),
                ..Default::default()
            }))
            .await
            .expect("the plain delete inserts a marker");
        let marker = header(&deleted.headers, "x-amz-version-id").expect("the marker's identity");

        let err = backend
            .get_object_attributes(s3_request(dto::GetObjectAttributesInput {
                bucket: b.clone(),
                key: "notes.txt".into(),
                version_id: Some(marker.clone()),
                object_attributes: vec!["ETag".parse().unwrap()],
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.status_code(), Some(StatusCode::METHOD_NOT_ALLOWED));
        assert_eq!(err.code().as_str(), "MethodNotAllowed");
        let headers = err.headers().expect("the 405 carries the marker's headers");
        assert!(headers.contains_key("last-modified"), "the marker's stamp");
        assert_eq!(
            header(headers, "x-amz-delete-marker").as_deref(),
            Some("true")
        );
        assert_eq!(
            header(headers, "x-amz-version-id").as_deref(),
            Some(marker.as_str())
        );
        assert!(
            !headers.contains_key("allow"),
            "AWS models no Allow member for the rule"
        );
    }

    /// The version-addressed `Checksum` attribute is the VERSION's own
    /// recorded value (the design's per-version metadata parity), echoed
    /// with the recorded kind — while the current object records none, so
    /// the assertion is discriminating.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn version_addressed_attributes_echo_the_versions_own_checksum() {
        const SHA256_FIRST: &str = "p5N7ZLjKpY8Dchu2us9ceMsjX+vg5wsbhM2ZVBRhoI4=";
        let (backend, b) = versioned_checksum_setup(State::Enabled).await;
        let first =
            written_version(&put_checked(&backend, &b, "notes.txt", b"first", SHA256_FIRST).await);
        put(&backend, &b, "notes.txt", b"second").await;

        let out = backend
            .get_object_attributes(s3_request(dto::GetObjectAttributesInput {
                bucket: b.clone(),
                key: "notes.txt".into(),
                version_id: Some(first),
                object_attributes: vec!["Checksum".parse().unwrap()],
                ..Default::default()
            }))
            .await
            .expect("the version-addressed request serves")
            .output;
        let checksum = out.checksum.expect("the version's recorded checksum");
        assert_eq!(checksum.checksum_sha256.as_deref(), Some(SHA256_FIRST));
        assert_eq!(
            checksum.checksum_type.as_ref().map(|t| t.as_str()),
            Some("FULL_OBJECT"),
            "the recorded kind rides the attribute echo"
        );
    }

    /// Residual 2 of the wave-C review: a version-addressed
    /// `GetObjectAttributes` that explicitly asks for `ObjectParts` is
    /// refused (`501`, naming the attribute) — the plan set models no
    /// per-version part list, and a response silently lacking what was
    /// asked would be worse than the refusal. The contrast is the
    /// current-version path, which serves the retained part list; a
    /// version-addressed request without the attribute is untouched
    /// (`max_parts` / `part_number_marker` stay inert without it), and
    /// the toggle-off refusal keeps its precedence.
    #[cfg(all(feature = "versioning", feature = "multipart"))]
    #[tokio::test]
    async fn version_addressed_attributes_refuse_the_unmodelled_object_parts() {
        use s3s::dto::{CompleteMultipartUploadInput, CreateMultipartUploadInput, UploadPartInput};

        /// One attribute request over the fixture key.
        async fn attributes(
            backend: &S3Backend<MemoryStorage>,
            b: &str,
            version_id: Option<String>,
            attrs: &[&str],
        ) -> S3Result<S3Response<dto::GetObjectAttributesOutput>> {
            backend
                .get_object_attributes(s3_request(dto::GetObjectAttributesInput {
                    bucket: b.to_string(),
                    key: "big.bin".into(),
                    version_id,
                    object_attributes: attrs.iter().map(|a| a.parse().unwrap()).collect(),
                    ..Default::default()
                }))
                .await
        }

        let (backend, b) = versioned_setup(State::Enabled).await;
        // The current object is multipart-completed: it retains one part,
        // so the plain path has something to serve — the contrast.
        let upload = backend
            .create_multipart_upload(s3_request(CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                ..Default::default()
            }))
            .await
            .expect("the upload opens")
            .output
            .upload_id
            .expect("an upload id");
        let part = backend
            .upload_part(s3_request(UploadPartInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload.clone(),
                part_number: 1,
                body: blob(b"hello"),
                ..Default::default()
            }))
            .await
            .expect("the part uploads")
            .output
            .e_tag
            .expect("a part ETag");
        let complete = backend
            .complete_multipart_upload(s3_request(CompleteMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload,
                multipart_upload: Some(dto::CompletedMultipartUpload {
                    parts: Some(vec![dto::CompletedPart {
                        part_number: Some(1),
                        e_tag: Some(part),
                        ..Default::default()
                    }]),
                }),
                ..Default::default()
            }))
            .await
            .expect("the upload completes");
        let composed =
            header(&complete.headers, "x-amz-version-id").expect("the completion's version");

        // The current-version path serves the retained part list.
        let out = attributes(&backend, &b, None, &["ObjectParts"])
            .await
            .expect("the plain ObjectParts request serves")
            .output;
        let served = out
            .object_parts
            .expect("the current path serves the retained list");
        assert_eq!(
            served.parts.as_ref().map(Vec::len),
            Some(1),
            "the completed object's one part"
        );

        // The same attribute on a version address: refused, never a
        // container silently lacking it.
        let err = attributes(
            &backend,
            &b,
            Some(composed.clone()),
            &["ETag", "ObjectParts"],
        )
        .await
        .unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented");
        assert_eq!(err.status_code(), Some(StatusCode::NOT_IMPLEMENTED));
        assert!(
            err.message().is_some_and(|m| m.contains("ObjectParts")),
            "the refusal names the attribute: {err:?}"
        );

        // Without it the version-addressed request is unchanged — the
        // pagination members alone stay inert (AWS consults them only
        // for ObjectParts).
        let out = backend
            .get_object_attributes(s3_request(dto::GetObjectAttributesInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                version_id: Some(composed.clone()),
                object_attributes: vec!["ETag".parse().unwrap()],
                max_parts: Some(1),
                part_number_marker: Some(1),
                ..Default::default()
            }))
            .await
            .expect("the version-addressed request without ObjectParts serves")
            .output;
        assert!(out.object_parts.is_none(), "no container was requested");
        assert_eq!(
            out.version_id.as_deref(),
            Some(composed.as_str()),
            "the answer still describes the version it names"
        );

        // An unknown id is still the family's `404 NoSuchVersion`: the
        // version resolution precedes the attribute refusal.
        let err = attributes(&backend, &b, Some("zzz".to_string()), &["ObjectParts"])
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchVersion");

        // The toggle-off refusal precedes even the parse: the family's
        // exact text, never the ObjectParts refusal.
        let (backend, b) = setup_with_caps(Capabilities {
            versioning: false,
            ..Default::default()
        })
        .await;
        put(&backend, &b, "big.bin", b"first").await;
        let err = attributes(&backend, &b, Some("zzz".to_string()), &["ObjectParts"])
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented");
        assert_eq!(err.message(), Some("versioning is disabled"));
    }

    /// The R12 branch keys on the address, not on a specific id: the
    /// literal `null` is a version address too, so a `?versionId=null`
    /// attributes request describes the null version (identity `null`, its
    /// own size), and an explicit `ObjectParts` on that address is refused
    /// exactly as a specific id's is.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_null_version_address_is_an_attributes_address_too() {
        async fn attributes(
            backend: &S3Backend<MemoryStorage>,
            b: &str,
            attrs: &[&str],
        ) -> S3Result<S3Response<dto::GetObjectAttributesOutput>> {
            backend
                .get_object_attributes(s3_request(dto::GetObjectAttributesInput {
                    bucket: b.to_string(),
                    key: "zero.bin".into(),
                    version_id: Some("null".into()),
                    object_attributes: attrs.iter().map(|a| a.parse().unwrap()).collect(),
                    ..Default::default()
                }))
                .await
        }

        let (backend, b) = versioned_setup(State::Suspended).await;
        put(&backend, &b, "zero.bin", b"null-body").await;

        let out = attributes(&backend, &b, &["ETag", "ObjectSize"])
            .await
            .expect("the null-address attributes request serves")
            .output;
        assert_eq!(
            out.version_id.as_deref(),
            Some("null"),
            "the answer names the null version it described"
        );
        assert_eq!(out.object_size, Some(9), "the null version's own size");

        let err = attributes(&backend, &b, &["ObjectParts"])
            .await
            .unwrap_err();
        assert_eq!(
            err.code().as_str(),
            "NotImplemented",
            "the refusal keys on presence, not on a specific id"
        );
        assert!(err.message().is_some_and(|m| m.contains("ObjectParts")));
    }

    /// Review Focus 4's listing half: `ListObjectVersions` splits one entry
    /// stream across the wire's two lists — content versions and delete
    /// markers — newest first within each key, `IsLatest` on the key's
    /// current version, and a truncated page names the last returned entry
    /// through its resume pair (`Next*`) — never all four cursors: the echo
    /// pair carries the request's own back.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_version_listing_page_is_capped_at_aws_maximum() {
        let (backend, b) = versioned_setup(State::Enabled).await;
        put(&backend, &b, "notes.txt", b"first").await;

        let out = backend
            .list_object_versions(s3_request(dto::ListObjectVersionsInput {
                bucket: b.clone(),
                max_keys: Some(100_000),
                ..Default::default()
            }))
            .await
            .expect("the listing serves")
            .output;
        assert_eq!(
            out.max_keys,
            Some(1000),
            "the ceiling answers as 1,000 and the echo carries the effective value"
        );
        assert_eq!(out.is_truncated, Some(false));
    }

    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn list_object_versions_separates_versions_from_markers_newest_first() {
        let (backend, b) = versioned_setup(State::Enabled).await;
        let older = written_version(&put(&backend, &b, "notes.txt", b"first").await);
        let newer = written_version(&put(&backend, &b, "notes.txt", b"second").await);
        let plain = written_version(&put(&backend, &b, "plain.txt", b"body").await);

        // A plain DELETE stacks a marker on top of both versions; it is the
        // key's current version from here on.
        let deleted = backend
            .delete_object(s3_request(dto::DeleteObjectInput {
                bucket: b.clone(),
                key: "notes.txt".into(),
                ..Default::default()
            }))
            .await
            .expect("the delete inserts a marker");
        let marker = header(&deleted.headers, "x-amz-version-id").expect("the marker's identity");

        let list =
            |input: dto::ListObjectVersionsInput| backend.list_object_versions(s3_request(input));

        let out = list(dto::ListObjectVersionsInput {
            bucket: b.clone(),
            ..Default::default()
        })
        .await
        .expect("the listing serves")
        .output;
        assert_eq!(out.name.as_deref(), Some("data"));
        assert_eq!(out.is_truncated, Some(false));
        assert_eq!(out.max_keys, Some(1000), "the default page size is echoed");
        assert_eq!(out.prefix, None);
        assert_eq!(out.delimiter, None);
        assert!(out.common_prefixes.as_ref().unwrap().is_empty());
        assert_eq!(out.key_marker, None);
        assert_eq!(out.version_id_marker, None);
        assert_eq!(out.next_key_marker, None);
        assert_eq!(out.next_version_id_marker, None);

        // The content versions, in entry order: each key's newest first,
        // "notes.txt" sorting before "plain.txt". `IsLatest` marks the key's
        // current version — for the marked key that is the delete marker,
        // never the superseded object versions.
        let versions = out.versions.expect("the version list is present");
        let listed: Vec<(&str, Option<bool>)> = versions
            .iter()
            .map(|v| (v.version_id.as_deref().unwrap(), v.is_latest))
            .collect();
        assert_eq!(
            listed,
            [
                (newer.as_str(), Some(false)),
                (older.as_str(), Some(false)),
                (plain.as_str(), Some(true)),
            ]
        );
        let first = &versions[0];
        assert_eq!(first.key.as_deref(), Some("notes.txt"));
        assert_eq!(first.size, Some(6), "'second' is six bytes");
        assert!(first.e_tag.is_some(), "a content version carries its ETag");
        assert!(first.last_modified.is_some());
        assert_eq!(versions[2].key.as_deref(), Some("plain.txt"));
        assert_eq!(versions[2].size, Some(4));

        let markers = out.delete_markers.expect("the marker list is present");
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].key.as_deref(), Some("notes.txt"));
        assert_eq!(markers[0].version_id.as_deref(), Some(marker.as_str()));
        assert_eq!(
            markers[0].is_latest,
            Some(true),
            "the marker is the key's current version"
        );
        assert!(markers[0].last_modified.is_some());

        // A page of two cuts after the marker and the newest object version.
        // Only the resume pair (`Next*`) names that last returned entry, as
        // the wire documents ("marks the last key/version returned"); the
        // echo pair carries the request's own cursors back — empty here,
        // since the request sent none.
        let page = list(dto::ListObjectVersionsInput {
            bucket: b.clone(),
            max_keys: Some(2),
            ..Default::default()
        })
        .await
        .expect("the truncated page serves")
        .output;
        assert_eq!(page.is_truncated, Some(true));
        assert_eq!(page.max_keys, Some(2));
        assert_eq!(page.delete_markers.as_ref().unwrap().len(), 1);
        assert_eq!(page.versions.as_ref().unwrap().len(), 1);
        assert_eq!(
            page.versions.as_ref().unwrap()[0].version_id.as_deref(),
            Some(newer.as_str())
        );
        assert_eq!(
            page.key_marker, None,
            "the request sent no cursors, so the echo is empty"
        );
        assert_eq!(page.version_id_marker, None);
        assert_eq!(page.next_key_marker.as_deref(), Some("notes.txt"));
        assert_eq!(page.next_version_id_marker.as_deref(), Some(newer.as_str()));
        assert_eq!(
            page.delete_markers.as_ref().unwrap()[0].is_latest,
            Some(true),
            "the marker is the page's first entry of its key"
        );
        assert_eq!(
            page.versions.as_ref().unwrap()[0].is_latest,
            Some(false),
            "the version it superseded is not"
        );

        // A continuation page echoes the cursors it was sent — which differ
        // from the resume pair it answers, because the page cut one version
        // later. `IsLatest` follows the version, not the position: the entry
        // the walk resumed on is notes.txt's noncurrent version.
        let continuation = list(dto::ListObjectVersionsInput {
            bucket: b.clone(),
            key_marker: page.next_key_marker.clone(),
            version_id_marker: page.next_version_id_marker.clone(),
            max_keys: Some(1),
            ..Default::default()
        })
        .await
        .expect("the cursors resume the walk")
        .output;
        assert_eq!(continuation.is_truncated, Some(true));
        assert_eq!(
            continuation.key_marker.as_deref(),
            Some("notes.txt"),
            "the response echoes the request's cursors"
        );
        assert_eq!(
            continuation.version_id_marker.as_deref(),
            Some(newer.as_str())
        );
        assert_eq!(continuation.next_key_marker.as_deref(), Some("notes.txt"));
        assert_eq!(
            continuation.next_version_id_marker.as_deref(),
            Some(older.as_str()),
            "the resume pair moves on, the echo does not"
        );
        let resumed = continuation.versions.unwrap();
        assert_eq!(resumed.len(), 1);
        assert_eq!(resumed[0].version_id.as_deref(), Some(older.as_str()));
        assert_eq!(
            resumed[0].is_latest,
            Some(false),
            "a page resumed after a version starts on a noncurrent one"
        );

        // The rest of the walk is untruncated — the echo still names where
        // the client was — and the next key's first entry is its latest.
        let rest = list(dto::ListObjectVersionsInput {
            bucket: b.clone(),
            key_marker: continuation.next_key_marker,
            version_id_marker: continuation.next_version_id_marker,
            ..Default::default()
        })
        .await
        .expect("the walk ends")
        .output;
        assert_eq!(rest.is_truncated, Some(false));
        assert_eq!(rest.delete_markers.as_ref().unwrap().len(), 0);
        assert_eq!(rest.key_marker.as_deref(), Some("notes.txt"));
        assert_eq!(rest.version_id_marker.as_deref(), Some(older.as_str()));
        let end = rest.versions.unwrap();
        assert_eq!(end.len(), 1);
        assert_eq!(end[0].version_id.as_deref(), Some(plain.as_str()));
        assert_eq!(
            end[0].is_latest,
            Some(true),
            "the next key's first entry is that key's current version"
        );

        let odd = list(dto::ListObjectVersionsInput {
            bucket: b.clone(),
            key_marker: Some("notes.txt".into()),
            version_id_marker: Some("x".repeat(version::MAX_ID_LEN + 1)),
            ..Default::default()
        })
        .await
        .expect("a cursor the wire may send is never a 400")
        .output;
        assert_eq!(
            odd.versions.unwrap()[0].key.as_deref(),
            Some("plain.txt"),
            "an overlong cursor matches no version and resumes at the key boundary"
        );

        // The page-size policy is the object listings' too: a size below 1
        // is `InvalidArgument`, never a silent empty page.
        let err = list(dto::ListObjectVersionsInput {
            bucket: b.clone(),
            max_keys: Some(0),
            ..Default::default()
        })
        .await
        .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidArgument");

        // The delimiter is echoed, and an empty `delimiter=` value means
        // "no delimiter" (the boundary rule the object listings share) —
        // neither key rolls up, so the page is the same either way.
        let delimiter = |delimiter: String| {
            list(dto::ListObjectVersionsInput {
                bucket: b.clone(),
                delimiter: Some(delimiter),
                ..Default::default()
            })
        };
        let rolled = delimiter("/".into())
            .await
            .expect("the delimiter listing serves")
            .output;
        assert_eq!(rolled.delimiter.as_deref(), Some("/"));
        assert!(rolled.common_prefixes.as_ref().unwrap().is_empty());
        let empty = delimiter(String::new())
            .await
            .expect("an empty delimiter is no delimiter")
            .output;
        assert_eq!(empty.delimiter, None);
        assert_eq!(empty.versions.as_ref().unwrap().len(), 3);

        // A marker an overwrite superseded is not the key's latest either.
        let third = written_version(&put(&backend, &b, "notes.txt", b"third").await);
        let superseded = list(dto::ListObjectVersionsInput {
            bucket: b.clone(),
            ..Default::default()
        })
        .await
        .expect("the listing serves after the overwrite")
        .output;
        let markers = superseded.delete_markers.unwrap();
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].version_id.as_deref(), Some(marker.as_str()));
        assert_eq!(
            markers[0].is_latest,
            Some(false),
            "the overwrite superseded the marker"
        );
        let versions = superseded.versions.unwrap();
        assert_eq!(versions.len(), 4);
        assert_eq!(versions[0].version_id.as_deref(), Some(third.as_str()));
        assert_eq!(versions[0].is_latest, Some(true));

        // The null version renders its literal on the wire.
        let (suspended, sb) = versioned_setup(State::Suspended).await;
        put(&suspended, &sb, "notes.txt", b"body").await;
        let null = suspended
            .list_object_versions(s3_request(dto::ListObjectVersionsInput {
                bucket: sb.clone(),
                ..Default::default()
            }))
            .await
            .expect("the suspended bucket lists")
            .output;
        let versions = null.versions.unwrap();
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].version_id.as_deref(), Some("null"));
        assert_eq!(versions[0].is_latest, Some(true));
    }

    /// The version listing's delimiter roll-up, mirroring the object
    /// listings: keys under one delimiter group collapse into a non-empty
    /// `CommonPrefixes`, and their versions leave the `Version` list.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn version_listing_rolls_up_common_prefixes() {
        let (backend, b) = versioned_setup(State::Enabled).await;
        for key in ["top.txt", "dir/a.txt", "dir/b.txt"] {
            put(&backend, &b, key, b"body").await;
        }
        let out = backend
            .list_object_versions(s3_request(dto::ListObjectVersionsInput {
                bucket: b.clone(),
                delimiter: Some("/".into()),
                ..Default::default()
            }))
            .await
            .expect("the delimiter listing serves")
            .output;
        assert_eq!(out.delimiter.as_deref(), Some("/"));
        let prefixes: Vec<&str> = out
            .common_prefixes
            .as_ref()
            .expect("the prefix list is present")
            .iter()
            .map(|prefix| prefix.prefix.as_deref().expect("a named prefix"))
            .collect();
        assert_eq!(prefixes, ["dir/"], "two keys under `dir/` roll up once");
        let listed: Vec<&str> = out
            .versions
            .as_ref()
            .expect("the version list is present")
            .iter()
            .map(|version| version.key.as_deref().expect("a named key"))
            .collect();
        assert_eq!(
            listed,
            ["top.txt"],
            "a rolled-up key's versions leave the page's Version list"
        );
        assert!(
            out.delete_markers
                .as_ref()
                .expect("the marker list is present")
                .is_empty()
        );
        assert_eq!(out.is_truncated, Some(false));
    }

    /// The `[s3] max_keys` capability clamp reaches the version listing
    /// too — the object listings'
    /// `v1_echoes_the_effective_page_size_after_a_clamp` leg's twin: the
    /// response echoes the effective value and the page truncates at it.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn version_listing_echoes_the_clamped_page_size() {
        let (backend, b) = setup_with_caps(Capabilities {
            max_keys: 2,
            ..Default::default()
        })
        .await;
        backend
            .storage()
            .set_bucket_versioning(&bucket::name(&b).unwrap(), State::Enabled)
            .await
            .unwrap();
        for key in ["a.txt", "b.txt", "c.txt"] {
            put(&backend, &b, key, b"body").await;
        }
        let out = backend
            .list_object_versions(s3_request(dto::ListObjectVersionsInput {
                bucket: b.clone(),
                max_keys: Some(1000),
                ..Default::default()
            }))
            .await
            .expect("the listing serves")
            .output;
        assert_eq!(
            out.max_keys,
            Some(2),
            "the response echoes the effective (clamped) page size"
        );
        assert_eq!(out.is_truncated, Some(true));
        assert_eq!(
            out.versions.as_ref().expect("the version list").len(),
            2,
            "the page truncates at the clamp"
        );
    }

    /// The versioning state round-trips across the two handlers: a fresh
    /// bucket answers no `Status` at all (unversioned is the *absence* of
    /// the element, never an `Unversioned` value), `Enabled` and
    /// `Suspended` write and read back, `MfaDelete=Enabled` is refused
    /// outright — the design's non-goal, a false protection being worse
    /// than an error — without touching the state, and a request that
    /// names no state is a no-op.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn the_versioning_state_round_trips_and_mfa_delete_is_refused() {
        let (backend, b) = setup_with_caps(Default::default()).await;

        let get = || {
            backend.get_bucket_versioning(s3_request(dto::GetBucketVersioningInput {
                bucket: b.clone(),
                ..Default::default()
            }))
        };
        let put = |configuration: dto::VersioningConfiguration| {
            backend.put_bucket_versioning(s3_request(dto::PutBucketVersioningInput {
                bucket: b.clone(),
                checksum_algorithm: None,
                content_md5: None,
                expected_bucket_owner: None,
                mfa: None,
                versioning_configuration: configuration,
            }))
        };
        fn status(out: &dto::GetBucketVersioningOutput) -> Option<&str> {
            out.status.as_ref().map(|value| value.as_str())
        }

        // A fresh bucket is unversioned: no `Status` element, and no MFA
        // delete element either (the protection can never be enabled).
        let out = get().await.expect("the state reads").output;
        assert_eq!(
            status(&out),
            None,
            "unversioned is the absence of Status, never an `Unversioned` value"
        );
        assert_eq!(out.mfa_delete, None, "MFA delete is never reported");

        // Enable: accepted, and read back.
        put(dto::VersioningConfiguration {
            status: Some(dto::BucketVersioningStatus::from_static(
                dto::BucketVersioningStatus::ENABLED,
            )),
            mfa_delete: None,
        })
        .await
        .expect("the enable is accepted");
        let out = get().await.expect("the state reads").output;
        assert_eq!(
            status(&out),
            Some(dto::BucketVersioningStatus::ENABLED),
            "the enabled state round-trips"
        );

        // Suspend: accepted, and read back.
        put(dto::VersioningConfiguration {
            status: Some(dto::BucketVersioningStatus::from_static(
                dto::BucketVersioningStatus::SUSPENDED,
            )),
            mfa_delete: None,
        })
        .await
        .expect("the suspend is accepted");
        let out = get().await.expect("the state reads").output;
        assert_eq!(
            status(&out),
            Some(dto::BucketVersioningStatus::SUSPENDED),
            "the suspended state round-trips"
        );

        // MFA delete is refused outright — whatever `Status` says, before
        // any state change — and the state is untouched.
        let err = put(dto::VersioningConfiguration {
            status: None,
            mfa_delete: Some(dto::MFADelete::from_static(dto::MFADelete::ENABLED)),
        })
        .await
        .unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented");
        assert_eq!(err.status_code(), Some(StatusCode::NOT_IMPLEMENTED));
        assert_eq!(err.message(), Some("MFA delete is not supported"));
        let out = get().await.expect("the state reads").output;
        assert_eq!(
            status(&out),
            Some(dto::BucketVersioningStatus::SUSPENDED),
            "the refused request changed nothing"
        );

        // The refusal holds when the request also names a state: the
        // protection is refused before any read or write, so the valid
        // `Status=Enabled` beside it changes nothing either.
        let err = put(dto::VersioningConfiguration {
            status: Some(dto::BucketVersioningStatus::from_static(
                dto::BucketVersioningStatus::ENABLED,
            )),
            mfa_delete: Some(dto::MFADelete::from_static(dto::MFADelete::ENABLED)),
        })
        .await
        .unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented");
        assert_eq!(err.message(), Some("MFA delete is not supported"));
        let out = get().await.expect("the state reads").output;
        assert_eq!(
            status(&out),
            Some(dto::BucketVersioningStatus::SUSPENDED),
            "the combined refusal changed nothing"
        );

        // `MfaDelete` is a closed enum too: an unrecognized value is
        // refused `400 InvalidArgument` like an unrecognized `Status`,
        // never read as the `Disabled` no-op.
        let err = put(dto::VersioningConfiguration {
            status: Some(dto::BucketVersioningStatus::from_static(
                dto::BucketVersioningStatus::ENABLED,
            )),
            mfa_delete: Some(dto::MFADelete::from("Sometimes".to_string())),
        })
        .await
        .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidArgument");
        assert_eq!(err.status_code(), Some(StatusCode::BAD_REQUEST));
        assert_eq!(err.message(), Some("invalid mfa delete: Sometimes"));
        let out = get().await.expect("the state reads").output;
        assert_eq!(
            status(&out),
            Some(dto::BucketVersioningStatus::SUSPENDED),
            "the refused MFA value changed nothing"
        );

        // A request that names no state changes nothing: an empty
        // configuration is a no-op answering 200 (the operator ruling),
        // and `MfaDelete=Disabled` names the only MFA state this server
        // provides, so it too leaves the state alone.
        put(dto::VersioningConfiguration {
            status: None,
            mfa_delete: None,
        })
        .await
        .expect("an empty configuration is a no-op, not a 400");
        put(dto::VersioningConfiguration {
            status: None,
            mfa_delete: Some(dto::MFADelete::from_static(dto::MFADelete::DISABLED)),
        })
        .await
        .expect("MfaDelete=Disabled asks for the protection state we have");
        let out = get().await.expect("the state reads").output;
        assert_eq!(
            status(&out),
            Some(dto::BucketVersioningStatus::SUSPENDED),
            "a request that names no state leaves the state alone"
        );

        // The wire enum is closed: an unrecognized `Status` is refused
        // (400 `InvalidArgument`), never defaulted — so the literal
        // `Unversioned` cannot silently revert a bucket either.
        let err = put(dto::VersioningConfiguration {
            status: Some(dto::BucketVersioningStatus::from("Unversioned".to_string())),
            mfa_delete: None,
        })
        .await
        .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidArgument");
        assert_eq!(err.status_code(), Some(StatusCode::BAD_REQUEST));
        let out = get().await.expect("the state reads").output;
        assert_eq!(
            status(&out),
            Some(dto::BucketVersioningStatus::SUSPENDED),
            "the refused status changed nothing"
        );

        // A no-op request is still a request against a bucket: a missing
        // bucket answers 404 `NoSuchBucket`, never a silent 200.
        let err = backend
            .put_bucket_versioning(s3_request(dto::PutBucketVersioningInput {
                bucket: "absent".to_string(),
                checksum_algorithm: None,
                content_md5: None,
                expected_bucket_owner: None,
                mfa: None,
                versioning_configuration: dto::VersioningConfiguration {
                    status: None,
                    mfa_delete: None,
                },
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchBucket");
        assert_eq!(err.status_code(), Some(StatusCode::NOT_FOUND));
    }

    /// The helper is the guard the un-gated handlers call: it must refuse a
    /// version-addressed request whenever versioning is not available.
    #[tokio::test]
    async fn a_version_addressed_request_is_refused_when_the_toggle_is_off() {
        let (backend, _bucket) = setup_with_caps(Capabilities {
            versioning: false,
            ..Default::default()
        })
        .await;
        let err = require_versioning(&backend, Some("v1")).unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented");
        assert!(
            require_versioning(&backend, None).is_ok(),
            "a plain request is untouched"
        );
    }

    /// The three wire states, through the one address parse — and the
    /// guard's precedence over the parse: with versioning unavailable every
    /// named version answers the family's `501`, a malformed value
    /// included, never the `400` a parse-first order would give it.
    #[tokio::test]
    async fn the_version_address_reads_the_three_states_behind_the_guard() {
        let (off, _bucket) = setup_with_caps(Capabilities {
            versioning: false,
            ..Default::default()
        })
        .await;
        for raw in [Some(""), Some("null"), Some("v1")] {
            let err = off.version_address(raw).unwrap_err();
            assert_eq!(
                err.code().as_str(),
                "NotImplemented",
                "the guard answers before the parse: {raw:?}"
            );
            // The floor that answered: the capability with the feature
            // compiled in, the build itself without it.
            #[cfg(feature = "versioning")]
            assert_eq!(err.message(), Some("versioning is disabled"));
            #[cfg(not(feature = "versioning"))]
            assert_eq!(err.message(), Some("versioning is not compiled in"));
        }
        assert!(
            !off.version_address(None).unwrap().is_addressed(),
            "a plain request passes the guard in every build"
        );

        #[cfg(feature = "versioning")]
        {
            let (backend, _bucket) = setup_with_caps(Default::default()).await;
            assert!(
                !backend.version_address(None).unwrap().is_addressed(),
                "no parameter: the current version"
            );
            let null = backend.version_address(Some("null")).unwrap();
            assert!(
                null.is_addressed() && null.id().is_none(),
                "the literal: the null version — a state `id()` alone cannot \
                 tell from the current one"
            );
            let specific = backend.version_address(Some("v1")).unwrap();
            assert_eq!(specific.id().map(|id| id.as_str()), Some("v1"));
            let err = backend.version_address(Some("")).unwrap_err();
            assert_eq!(
                err.code().as_str(),
                "InvalidArgument",
                "a malformed value is the shared 400"
            );
        }
    }

    /// The moved rule's one home, both arms — the specific-id one the wire
    /// shows as `records_for`'s no-read short-circuit, and the null one on
    /// a bucket that records no versions, which the wire can never show
    /// (the guard refuses such a request before any storage read). The
    /// plain address is not exercised: it is a `debug_assert!`-guarded
    /// trap in `records_for` (the plain paths carry their bit on the
    /// `Info`/write result), not a behavior.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn records_for_reads_the_bucket_state_only_for_the_null_address() {
        let (backend, b) = setup_with_caps(Default::default()).await;
        let bucket = bucket::name(&b).unwrap();
        assert!(
            backend
                .records_for(&bucket, &version::Address::Id(version::mint()))
                .await,
            "a specific id names a recording bucket by construction"
        );
        assert!(
            !backend.records_for(&bucket, &version::Address::Null).await,
            "an unversioned bucket records none"
        );
        backend
            .storage()
            .set_bucket_versioning(&bucket, State::Suspended)
            .await
            .unwrap();
        assert!(
            backend.records_for(&bucket, &version::Address::Null).await,
            "a suspended bucket records versions"
        );
    }

    /// A `PutObject` request carrying `versionId` — a parameter AWS
    /// defines no semantics for on a write, and one s3s's non-`minio`
    /// `PutObjectInput` does not surface, so the handler reads the raw
    /// query — passes the fail-open guard: the family's exact refusal
    /// when versioning is unavailable, and the ordinary write, never the
    /// value the request named, when it is.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_put_carrying_a_version_id_is_guarded_and_otherwise_ignored() {
        // The handler reads `versionId` from the raw query (s3s's
        // non-`minio` `PutObjectInput` drops the parameter), so the
        // fixture writes it onto the URI the way a client sends it.
        let put_request = |b: &str, query: &str| {
            let mut req = s3_request(dto::PutObjectInput {
                bucket: b.to_string(),
                key: "notes.txt".into(),
                body: blob(b"first"),
                ..Default::default()
            });
            req.uri = format!("/{b}/notes.txt?{query}")
                .parse()
                .expect("the fixture URI parses");
            req
        };

        // Toggle off: refused before any body is staged, while a query
        // without `versionId` is not a version address and still serves.
        let (backend, b) = setup_with_caps(Capabilities {
            versioning: false,
            ..Default::default()
        })
        .await;
        let err = backend
            .put_object(put_request(&b, "versionId=v1"))
            .await
            .unwrap_err();
        assert_versioning_refused(err);
        backend
            .put_object(put_request(&b, "x=1"))
            .await
            .expect("a query without versionId is not a version address");

        // Versioning available: the parameter is ignored — the write is
        // the ordinary versioned write, and the version it mints is its
        // own, never the value the request named.
        let (backend, b) = versioned_setup(State::Enabled).await;
        let response = backend
            .put_object(put_request(&b, "versionId=v1"))
            .await
            .expect("the guarded write serves");
        assert_ne!(
            written_version(&response),
            "v1",
            "the parameter is ignored, not the version written"
        );
        let got = backend
            .get_object(s3_request(dto::GetObjectInput {
                bucket: b.clone(),
                key: "notes.txt".into(),
                ..Default::default()
            }))
            .await
            .expect("the written object reads back");
        assert_eq!(read_blob(got.output.body.unwrap()).await, b"first");
    }

    /// The same rule for a `CreateMultipartUpload` request carrying
    /// `versionId`: refused when versioning is unavailable, ignored when
    /// it is — the create is the ordinary create.
    #[cfg(all(feature = "versioning", feature = "multipart"))]
    #[tokio::test]
    async fn a_create_multipart_upload_carrying_a_version_id_is_guarded_and_otherwise_ignored() {
        // The raw query again: `?uploads&versionId=…` as the client
        // sends it (the non-`minio` `CreateMultipartUploadInput` drops
        // the parameter).
        let create_request = |b: &str, query: &str| {
            let mut req = s3_request(dto::CreateMultipartUploadInput {
                bucket: b.to_string(),
                key: "big.bin".into(),
                ..Default::default()
            });
            req.uri = format!("/{b}/big.bin?{query}")
                .parse()
                .expect("the fixture URI parses");
            req
        };

        let (backend, b) = setup_with_caps(Capabilities {
            versioning: false,
            ..Default::default()
        })
        .await;
        let err = backend
            .create_multipart_upload(create_request(&b, "uploads&versionId=v1"))
            .await
            .unwrap_err();
        assert_versioning_refused(err);
        backend
            .create_multipart_upload(create_request(&b, "uploads"))
            .await
            .expect("the plain create is untouched");

        // Versioning available: the parameter is ignored — the upload
        // opens as the plain one.
        let (backend, b) = versioned_setup(State::Enabled).await;
        let out = backend
            .create_multipart_upload(create_request(&b, "uploads&versionId=v1"))
            .await
            .expect("the guarded create serves")
            .output;
        assert!(out.upload_id.is_some(), "the upload opens as the plain one");
    }

    /// PUT one object carrying the `x-amz-tagging` header.
    #[cfg(all(feature = "acl", feature = "versioning"))]
    async fn put_tagged(
        backend: &S3Backend<MemoryStorage>,
        b: &str,
        key: &str,
        data: &'static [u8],
        tagging: &str,
    ) -> S3Response<dto::PutObjectOutput> {
        backend
            .put_object(s3_request(dto::PutObjectInput {
                bucket: b.to_string(),
                key: key.to_string(),
                body: blob(data),
                tagging: Some(dto::TaggingHeader::from(tagging)),
                ..Default::default()
            }))
            .await
            .expect("the tagged object writes")
    }

    /// A `GetObjectTagging` through the handler: `None` reads the current
    /// version, `Some(id)` the named one.
    #[cfg(all(feature = "acl", feature = "versioning"))]
    async fn get_tagging(
        backend: &S3Backend<MemoryStorage>,
        b: &str,
        key: &str,
        version: Option<&str>,
    ) -> dto::GetObjectTaggingOutput {
        backend
            .get_object_tagging(s3_request(dto::GetObjectTaggingInput {
                bucket: b.to_string(),
                key: key.to_string(),
                version_id: version.map(str::to_owned),
                ..Default::default()
            }))
            .await
            .expect("the tag read answers")
            .output
    }

    /// A `PutObjectTagging` through the handler.
    #[cfg(all(feature = "acl", feature = "versioning"))]
    async fn put_tagging(
        backend: &S3Backend<MemoryStorage>,
        b: &str,
        key: &str,
        version: Option<&str>,
        tag_set: Vec<dto::Tag>,
    ) -> dto::PutObjectTaggingOutput {
        backend
            .put_object_tagging(s3_request(dto::PutObjectTaggingInput {
                bucket: b.to_string(),
                checksum_algorithm: None,
                content_md5: None,
                expected_bucket_owner: None,
                key: key.to_string(),
                request_payer: None,
                tagging: dto::Tagging { tag_set },
                version_id: version.map(str::to_owned),
            }))
            .await
            .expect("the tag put writes")
            .output
    }

    /// A `DeleteObjectTagging` through the handler.
    #[cfg(all(feature = "acl", feature = "versioning"))]
    async fn delete_tagging(
        backend: &S3Backend<MemoryStorage>,
        b: &str,
        key: &str,
        version: Option<&str>,
    ) -> dto::DeleteObjectTaggingOutput {
        backend
            .delete_object_tagging(s3_request(dto::DeleteObjectTaggingInput {
                bucket: b.to_string(),
                key: key.to_string(),
                version_id: version.map(str::to_owned),
                ..Default::default()
            }))
            .await
            .expect("the tag delete answers")
            .output
    }

    /// One `k=v` pair as a dto tag set.
    #[cfg(all(feature = "acl", feature = "versioning"))]
    fn one_tag(key: &str, value: &str) -> Vec<dto::Tag> {
        vec![dto::Tag {
            key: Some(key.into()),
            value: Some(value.into()),
        }]
    }

    /// The guard's refusal of a version-addressed request: the family's
    /// `NotImplemented` and exact text.
    #[cfg(feature = "versioning")]
    fn assert_versioning_refused(err: s3s::S3Error) {
        assert_eq!(err.code().as_str(), "NotImplemented");
        assert_eq!(err.message(), Some("versioning is disabled"));
    }

    /// A `GetObjectAcl` through the handler.
    #[cfg(all(feature = "acl", feature = "versioning"))]
    async fn get_grants(
        backend: &S3Backend<MemoryStorage>,
        b: &str,
        key: &str,
        version: Option<&str>,
    ) -> Vec<dto::Grant> {
        backend
            .get_object_acl(s3_request(dto::GetObjectAclInput {
                bucket: b.to_string(),
                key: key.to_string(),
                version_id: version.map(str::to_owned),
                ..Default::default()
            }))
            .await
            .expect("the ACL read answers")
            .output
            .grants
            .unwrap_or_default()
    }

    /// Tagging and ACLs are per-version metadata (the design's
    /// "Per-version metadata"): a version-addressed `?tagging`/`?acl`
    /// request reads and writes exactly that version's row, while a plain
    /// request keeps the current-version path — one version's metadata
    /// write never bleeds into another's, and the version delete clears
    /// exactly the named version's set. The tagging answers carry the
    /// `version_id` member (s3s renders it as the `x-amz-version-id`
    /// header): the addressed version on a version-addressed call, the
    /// current identity on a plain one.
    #[cfg(all(feature = "acl", feature = "versioning"))]
    #[tokio::test]
    async fn tags_and_acls_are_version_scoped() {
        use crate::_core::acl;

        let (backend, b) = versioned_setup(State::Enabled).await;

        // v1 carries tags A; the overwrite (the current v2) carries B.
        let first =
            written_version(&put_tagged(&backend, &b, "notes.txt", b"first", "env=a").await);
        let second =
            written_version(&put_tagged(&backend, &b, "notes.txt", b"second", "env=b").await);
        assert_ne!(first, second, "each write creates its own version");

        // The read routes to the version the request names and echoes that
        // version's identity; the plain read answers the current version's
        // set and identity.
        let addressed = get_tagging(&backend, &b, "notes.txt", Some(&first)).await;
        assert_eq!(addressed.tag_set, one_tag("env", "a"));
        assert_eq!(
            addressed.version_id.as_deref(),
            Some(first.as_str()),
            "the version-addressed answer echoes the addressed version"
        );
        assert_eq!(
            get_tagging(&backend, &b, "notes.txt", Some(&second))
                .await
                .tag_set,
            one_tag("env", "b")
        );
        let current = get_tagging(&backend, &b, "notes.txt", None).await;
        assert_eq!(current.tag_set, one_tag("env", "b"));
        assert_eq!(
            current.version_id.as_deref(),
            Some(second.as_str()),
            "the plain answer echoes the current version"
        );

        // A version-addressed put replaces that version's set only, and
        // answers the addressed identity.
        let replaced = put_tagging(
            &backend,
            &b,
            "notes.txt",
            Some(&first),
            one_tag("env", "v1-only"),
        )
        .await;
        assert_eq!(replaced.version_id.as_deref(), Some(first.as_str()));
        assert_eq!(
            get_tagging(&backend, &b, "notes.txt", Some(&first))
                .await
                .tag_set,
            one_tag("env", "v1-only")
        );
        assert_eq!(
            get_tagging(&backend, &b, "notes.txt", Some(&second))
                .await
                .tag_set,
            one_tag("env", "b"),
            "the sibling version is untouched"
        );
        assert_eq!(
            get_tagging(&backend, &b, "notes.txt", None).await.tag_set,
            one_tag("env", "b"),
            "the current version is untouched"
        );

        // A version-addressed delete clears exactly that version's set.
        let cleared = delete_tagging(&backend, &b, "notes.txt", Some(&first)).await;
        assert_eq!(cleared.version_id.as_deref(), Some(first.as_str()));
        assert!(
            get_tagging(&backend, &b, "notes.txt", Some(&first))
                .await
                .tag_set
                .is_empty()
        );
        assert_eq!(
            get_tagging(&backend, &b, "notes.txt", None).await.tag_set,
            one_tag("env", "b")
        );

        // The plain put and delete answer the current identity and never
        // touch the history version.
        let plain_put = put_tagging(&backend, &b, "notes.txt", None, one_tag("env", "plain")).await;
        assert_eq!(
            plain_put.version_id.as_deref(),
            Some(second.as_str()),
            "the plain put echoes the current version"
        );
        assert!(
            get_tagging(&backend, &b, "notes.txt", Some(&first))
                .await
                .tag_set
                .is_empty(),
            "the plain put left the history version alone"
        );
        let plain_delete = delete_tagging(&backend, &b, "notes.txt", None).await;
        assert_eq!(
            plain_delete.version_id.as_deref(),
            Some(second.as_str()),
            "the plain delete echoes the current version"
        );
        assert!(
            get_tagging(&backend, &b, "notes.txt", None)
                .await
                .tag_set
                .is_empty()
        );

        // The ACL rides the same route. The no-identity writes record the
        // private default — the baseline every version starts from. (The
        // sibling's baseline is asserted only after the version-scoped
        // write below: with both versions still at the default, comparing
        // the current version's grants with themselves would have no
        // discriminating power.)
        let baseline = get_grants(&backend, &b, "notes.txt", None).await;

        // A grant-header put on v1 (the current path's expansion: the
        // owner keeps FULL_CONTROL, the named principal gains READ)
        // leaves the sibling and the current version alone.
        let other = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100";
        backend
            .put_object_acl(s3_request(dto::PutObjectAclInput {
                bucket: b.clone(),
                key: "notes.txt".into(),
                grant_read: Some(format!(r#"id="{other}""#)),
                content_md5: Some(valid_md5()),
                version_id: Some(first.clone()),
                ..Default::default()
            }))
            .await
            .expect("the version-addressed ACL put writes");
        let after_version_put = get_grants(&backend, &b, "notes.txt", Some(&first)).await;
        assert_ne!(
            after_version_put, baseline,
            "the version-addressed put really replaced the addressed version's grants — the sibling/current checks below compare against a value the address no longer holds"
        );
        assert_eq!(
            after_version_put.len(),
            2,
            "the expansion keeps the owner's FULL_CONTROL"
        );
        assert!(after_version_put.contains(&grant(
            canonical_grantee(acl::default_owner_id().as_str()),
            dto::Permission::FULL_CONTROL
        )));
        assert!(
            after_version_put.contains(&grant(canonical_grantee(other), dto::Permission::READ))
        );
        assert_eq!(
            get_grants(&backend, &b, "notes.txt", Some(&second)).await,
            baseline,
            "the sibling version is untouched"
        );
        assert_eq!(
            get_grants(&backend, &b, "notes.txt", None).await,
            baseline,
            "the current version is untouched"
        );

        // And the plain put writes the current version only: the canned
        // public-read expansion lands there while v1's own set stays
        // exactly what its address last wrote.
        backend
            .put_object_acl(s3_request(dto::PutObjectAclInput {
                bucket: b.clone(),
                key: "notes.txt".into(),
                acl: Some(dto::ObjectCannedACL::from_static("public-read")),
                content_md5: Some(valid_md5()),
                ..Default::default()
            }))
            .await
            .expect("the current-version ACL put writes");
        assert_eq!(
            get_grants(&backend, &b, "notes.txt", None).await,
            vec![
                grant(
                    canonical_grantee(acl::default_owner_id().as_str()),
                    dto::Permission::FULL_CONTROL
                ),
                grant(group_grantee(acl::GROUP_ALL_USERS), dto::Permission::READ),
            ]
        );
        assert_eq!(
            get_grants(&backend, &b, "notes.txt", Some(&first)).await,
            after_version_put,
            "the version-addressed set keeps its own value"
        );
    }

    /// The literal `null` addresses the null version, never the current
    /// one: a suspended write stays the null version when a later enable
    /// supersedes it into history, and the tagging/ACL operations answer
    /// it by its literal spelling (read, write and clear alike).
    #[cfg(all(feature = "acl", feature = "versioning"))]
    #[tokio::test]
    async fn the_null_version_is_addressable_on_tagging_and_acl() {
        let (backend, b) = versioned_setup(State::Suspended).await;
        let null_write = put_tagged(&backend, &b, "zero.txt", b"null", "env=null").await;
        assert_eq!(
            header(&null_write.headers, "x-amz-version-id").as_deref(),
            Some("null"),
            "a suspended write records the null version"
        );

        // Re-enable and supersede it: the null version enters history while
        // the plain path reads the new current version.
        backend
            .storage()
            .set_bucket_versioning(&bucket::name(&b).unwrap(), State::Enabled)
            .await
            .unwrap();
        let current =
            written_version(&put_tagged(&backend, &b, "zero.txt", b"next", "env=current").await);

        let null_read = get_tagging(&backend, &b, "zero.txt", Some("null")).await;
        assert_eq!(null_read.tag_set, one_tag("env", "null"));
        assert_eq!(
            null_read.version_id.as_deref(),
            Some("null"),
            "the literal is echoed as the null spelling"
        );
        assert_eq!(
            get_tagging(&backend, &b, "zero.txt", None).await.tag_set,
            one_tag("env", "current")
        );
        assert_eq!(
            get_tagging(&backend, &b, "zero.txt", Some(&current))
                .await
                .tag_set,
            one_tag("env", "current")
        );

        // A null-addressed put replaces the null version's set only.
        let replaced = put_tagging(
            &backend,
            &b,
            "zero.txt",
            Some("null"),
            one_tag("env", "null-only"),
        )
        .await;
        assert_eq!(replaced.version_id.as_deref(), Some("null"));
        assert_eq!(
            get_tagging(&backend, &b, "zero.txt", Some("null"))
                .await
                .tag_set,
            one_tag("env", "null-only")
        );
        assert_eq!(
            get_tagging(&backend, &b, "zero.txt", None).await.tag_set,
            one_tag("env", "current")
        );

        // The ACL rides the same literal: the null-addressed put lands on
        // the null version and leaves the current version's grants alone.
        let other = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100";
        let before_null = get_grants(&backend, &b, "zero.txt", Some("null")).await;
        let before_current = get_grants(&backend, &b, "zero.txt", None).await;
        backend
            .put_object_acl(s3_request(dto::PutObjectAclInput {
                bucket: b.clone(),
                key: "zero.txt".into(),
                grant_read: Some(format!(r#"id="{other}""#)),
                content_md5: Some(valid_md5()),
                version_id: Some("null".into()),
                ..Default::default()
            }))
            .await
            .expect("the null-addressed ACL put writes");
        let after_null = get_grants(&backend, &b, "zero.txt", Some("null")).await;
        assert_ne!(after_null, before_null, "the null version's grants changed");
        assert!(after_null.contains(&grant(canonical_grantee(other), dto::Permission::READ)));
        assert_eq!(
            get_grants(&backend, &b, "zero.txt", None).await,
            before_current,
            "the current version's grants are untouched"
        );
        assert_eq!(
            get_grants(&backend, &b, "zero.txt", Some(&current)).await,
            before_current
        );

        // A null-addressed delete clears the null version's set only.
        let cleared = delete_tagging(&backend, &b, "zero.txt", Some("null")).await;
        assert_eq!(cleared.version_id.as_deref(), Some("null"));
        assert!(
            get_tagging(&backend, &b, "zero.txt", Some("null"))
                .await
                .tag_set
                .is_empty()
        );
        assert_eq!(
            get_tagging(&backend, &b, "zero.txt", None).await.tag_set,
            one_tag("env", "current")
        );
    }

    /// The plain tagging path on a bucket that records the null version:
    /// the write answers the literal `null` identity off its own answer
    /// (no post-write head), and the delete names the same version it
    /// cleared.
    #[cfg(all(feature = "acl", feature = "versioning"))]
    #[tokio::test]
    async fn plain_tagging_reports_the_null_identity_on_a_suspended_bucket() {
        let (backend, b) = versioned_setup(State::Suspended).await;
        let put_written = put_tagged(&backend, &b, "note.txt", b"body", "env=one").await;
        assert_eq!(
            header(&put_written.headers, "x-amz-version-id").as_deref(),
            Some("null"),
            "a suspended write records the null version"
        );

        // The tag put lands on the same version the write did and names
        // it — the value, not merely a header.
        let tagged = put_tagging(&backend, &b, "note.txt", None, one_tag("env", "two")).await;
        assert_eq!(
            tagged.version_id.as_deref(),
            Some("null"),
            "the plain tag put names the null version it landed on"
        );
        assert_eq!(
            get_tagging(&backend, &b, "note.txt", None)
                .await
                .version_id
                .as_deref(),
            Some("null"),
            "the plain read echoes the same identity"
        );

        let cleared = delete_tagging(&backend, &b, "note.txt", None).await;
        assert_eq!(
            cleared.version_id.as_deref(),
            Some("null"),
            "the plain tag delete names the null version it cleared"
        );
    }

    /// The fail-open hazard the routing must not reopen: with the toggle
    /// off, each of the five operations refuses a version-addressed request
    /// with the guard's family text — never the current object's metadata —
    /// while the plain path keeps serving, and an unversioned bucket
    /// answers no version identity on it (the read/write header rule).
    #[cfg(all(feature = "acl", feature = "versioning"))]
    #[tokio::test]
    async fn a_version_addressed_tagging_or_acl_request_is_refused_when_the_toggle_is_off() {
        let caps = Capabilities {
            versioning: false,
            ..Default::default()
        };
        let (backend, b) = setup_with_caps(caps).await;
        backend
            .storage()
            .put_object(&bucket::name(&b).unwrap(), &object_key("t.txt"), body(b"x"))
            .await
            .unwrap();

        assert_versioning_refused(
            backend
                .get_object_tagging(s3_request(dto::GetObjectTaggingInput {
                    bucket: b.clone(),
                    key: "t.txt".into(),
                    version_id: Some("v1".into()),
                    ..Default::default()
                }))
                .await
                .expect_err("the guard refuses a version-addressed tag read"),
        );
        assert_versioning_refused(
            backend
                .put_object_tagging(s3_request(dto::PutObjectTaggingInput {
                    bucket: b.clone(),
                    checksum_algorithm: None,
                    content_md5: None,
                    expected_bucket_owner: None,
                    key: "t.txt".into(),
                    request_payer: None,
                    tagging: dto::Tagging {
                        tag_set: one_tag("k", "v"),
                    },
                    version_id: Some("v1".into()),
                }))
                .await
                .expect_err("the guard refuses a version-addressed tag put"),
        );
        assert_versioning_refused(
            backend
                .delete_object_tagging(s3_request(dto::DeleteObjectTaggingInput {
                    bucket: b.clone(),
                    key: "t.txt".into(),
                    version_id: Some("v1".into()),
                    ..Default::default()
                }))
                .await
                .expect_err("the guard refuses a version-addressed tag delete"),
        );
        assert_versioning_refused(
            backend
                .get_object_acl(s3_request(dto::GetObjectAclInput {
                    bucket: b.clone(),
                    key: "t.txt".into(),
                    version_id: Some("v1".into()),
                    ..Default::default()
                }))
                .await
                .expect_err("the guard refuses a version-addressed ACL read"),
        );
        assert_versioning_refused(
            backend
                .put_object_acl(s3_request(dto::PutObjectAclInput {
                    bucket: b.clone(),
                    key: "t.txt".into(),
                    acl: Some(dto::ObjectCannedACL::from_static("public-read")),
                    content_md5: Some(valid_md5()),
                    version_id: Some("v1".into()),
                    ..Default::default()
                }))
                .await
                .expect_err("the guard refuses a version-addressed ACL put"),
        );

        // The plain path is untouched, and the unversioned bucket carries
        // no version identity on it.
        let plain = get_tagging(&backend, &b, "t.txt", None).await;
        assert!(plain.tag_set.is_empty());
        assert!(
            plain.version_id.is_none(),
            "an unversioned bucket answers no version id"
        );
        let plain_put = put_tagging(&backend, &b, "t.txt", None, one_tag("k", "v")).await;
        assert!(plain_put.version_id.is_none());
        assert_eq!(
            get_tagging(&backend, &b, "t.txt", None).await.tag_set,
            one_tag("k", "v"),
            "the plain path kept serving while the toggle is off"
        );
        let plain_delete = delete_tagging(&backend, &b, "t.txt", None).await;
        assert!(plain_delete.version_id.is_none());
        assert!(
            get_tagging(&backend, &b, "t.txt", None)
                .await
                .tag_set
                .is_empty()
        );
    }
}
