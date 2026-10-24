//! The pre-route authorization pipeline: [`AclAccess`] implements the
//! s3s `S3Access` seam (spec §4) — principal resolution, the
//! `x-amz-expected-bucket-owner` header, the operation→requirement
//! mapping, the B1 destination-existence head, owner bypass, grant
//! evaluation with the group rules, and the fail-closed storage-error
//! posture (`NoSuch*` → the deny-first three tiers, review B3).

use std::sync::Arc;

use http::Extensions;
use parse_display::Display;
use s3s::{
    S3Error, S3Result,
    access::{S3Access, S3AccessContext},
    path::S3Path,
    s3_error,
};
use tracing::{Span, field, instrument};

pub use crate::bypass::{MayBypass, may_bypass_authority};
use crate::{
    _core::{
        acl::{
            Acl, GROUP_ALL_USERS, GROUP_AUTHENTICATED_USERS, Grantee, GroupUri, OwnerId,
            Permission, can_delete,
        },
        bucket, object, percent,
        storage::{self, Storage},
        version,
    },
    bypass::{bypass_requested, is_bypass_authority_op},
    error::{AccessDecision, classify},
    identity::Identity,
    matrix::{OpRule, Requirement, rule_for},
    metrics, source,
};

/// The s3s `S3Access` authorization pipeline over a [`Storage`] backend
/// and the configured [`Identity`] map.
pub struct AclAccess<S: Storage> {
    storage: Arc<S>,
    identity: Arc<Identity>,
}

impl<S: Storage> AclAccess<S> {
    /// Wire the pipeline to a storage backend and the identity map.
    pub fn new(storage: Arc<S>, identity: Arc<Identity>) -> Self {
        Self { storage, identity }
    }
}

/// One request as the pipeline sees it — the plain inputs of
/// `S3AccessContext` (s3s keeps that type's fields `pub(crate)` with no
/// public constructor, so the testable core takes the extracted values
/// and [`AclAccess::check`] adapts the context into it).
///
/// The fields are owned: `check` must release its immutable borrow of the
/// context before it can hand the decision's extension bit back through
/// `S3AccessContext::extensions_mut`.
pub(crate) struct Request {
    /// The requester's canonical ID (resolved by
    /// [`Identity::principal`]).
    pub(crate) principal: OwnerId,
    /// Whether the request carried verified credentials.
    pub(crate) authenticated: bool,
    /// The s3s operation name.
    pub(crate) op: String,
    /// The route path (root/bucket/object).
    pub(crate) path: S3Path,
    /// The raw request query (the `uploadId` and `versionId` parameters).
    pub(crate) query: String,
    /// `x-amz-expected-bucket-owner`.
    pub(crate) expected_bucket_owner: Option<String>,
    /// `x-amz-copy-source` (CopyObject, UploadPartCopy).
    pub(crate) copy_source: Option<String>,
    /// `x-amz-rename-source` (RenameObject).
    pub(crate) rename_source: Option<String>,
    /// Whether the request asks for the governance bypass
    /// (`x-amz-bypass-governance-retention`, s3s's bool parse).
    pub(crate) bypass_requested: bool,
}

/// The bounded reason a decision span records — an enum, not a free string, so
/// a new reason is a code change and never a new metric series.
#[derive(Clone, Copy, Display)]
#[display(style = "kebab-case")]
enum Reason {
    Owner,
    Grant,
    Authenticated,
    Missing,
    UploadCreator,
    OwnerMismatch,
    MalformedHeader,
    MalformedSource,
    MalformedVersion,
    /// The address names a delete marker: a real, addressable version
    /// with no metadata of its own. The refusal is intentional (403),
    /// never a backend failure.
    DeleteMarker,
    NoResource,
    NoGrant,
    StorageError,
}

impl Reason {
    fn record(self) {
        // `Display` renders the kebab-case label, so the span field and the
        // decision vocabulary cannot drift from the enum. `display` defers the
        // formatting to the subscriber, so a disabled span allocates nothing.
        Span::current().record("reason", field::display(self));
    }
}

/// Allow with a reason on the active decision span.
fn allow(reason: Reason) -> S3Result<()> {
    reason.record();
    Ok(())
}

/// Deny with a reason on the active decision span.
fn deny(reason: Reason) -> S3Result<()> {
    Err(deny_with(reason))
}

/// The error form of [`deny`], for the `?` and `map_err` sites.
fn deny_with(reason: Reason) -> S3Error {
    reason.record();
    denied()
}

#[async_trait::async_trait]
impl<S: Storage> S3Access for AclAccess<S> {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        // Precondition (Task 7 review note): s3s invokes `S3Access` only
        // AFTER signature verification against the auth provider —
        // `ConfigAuth` rejects unknown access keys — so `credentials()`
        // is a verified signed user. A key that is somehow not configured
        // still reads the same refusal it verifies with, never a panic.
        let principal = self.identity.principal(cx.credentials())?;
        let headers = cx.headers();
        // The decision span lives on `evaluate`: this async_trait trampoline
        // must not open a second span for the same decision.
        let request = Request {
            principal,
            authenticated: cx.credentials().is_some(),
            op: cx.s3_op().name().to_owned(),
            path: owned_path(cx.s3_path()),
            query: cx.uri().query().unwrap_or("").to_owned(),
            // A present-but-unreadable (non-UTF-8) expected-bucket-owner
            // header fails closed (403) — never a silent skip; the source
            // headers fail closed the same way (None → deny downstream).
            expected_bucket_owner: match headers.get("x-amz-expected-bucket-owner") {
                Some(value) => Some(
                    value
                        .to_str()
                        .map_err(|_| deny_with(Reason::MalformedHeader))?
                        .to_owned(),
                ),
                None => None,
            },
            copy_source: headers
                .get("x-amz-copy-source")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned),
            rename_source: headers
                .get("x-amz-rename-source")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned),
            bypass_requested: bypass_requested(headers),
        };
        // The owned request releases the context's immutable borrows, so the
        // extensions can take the resolved authority bit.
        self.evaluate(&request, cx.extensions_mut()).await
    }
}

impl<S: Storage> AclAccess<S> {
    /// The authorization decision: one `info` span and one family sample around
    /// the gate pipeline.
    #[instrument(
        level = "info",
        target = "tinio::auth",
        skip_all,
        fields(op = request.op.as_str(), decision = field::Empty, reason = field::Empty)
    )]
    pub(crate) async fn evaluate(
        &self,
        request: &Request,
        extensions: &mut Extensions,
    ) -> S3Result<()> {
        let result = self.authorize(request).await;
        let decision = if result.is_ok() { "allowed" } else { "denied" };
        Span::current().record("decision", decision);
        metrics::record_authorization(result.is_ok());
        // OL15: the governance-bypass authority of an ALLOWED request rides
        // the extensions into the handler. An op with no bypass (and a
        // denied request) gets no bit; the handler's `header && may_bypass`
        // reads an absent bit as no authority.
        if result.is_ok() && is_bypass_authority_op(&request.op) {
            let granted = request.bypass_requested && self.may_bypass(request).await;
            extensions.insert(MayBypass::new(granted));
        }
        result
    }

    /// OL15's authority, resolved from the resource the request ADDRESSES
    /// through the same [`Self::addressed_acl`] resolution the operation
    /// gate uses (R23 closes the Task 7 duplicate): a `versionId` selects
    /// that version's ACL (`version_acl`; the literal `null` selects the
    /// null version), and only an unaddressed request reads the current
    /// object's ACL. The bucket owner has parity over every version. Any
    /// read failure — including a malformed address — answers `false`: the
    /// handler owns the wire error, and no authority is ever inferred from
    /// a resource the pipeline could not resolve.
    async fn may_bypass(&self, request: &Request) -> bool {
        let Some((bucket_name, key)) = request.path.as_object() else {
            return false;
        };
        let (Ok(bucket_name), Ok(key)) = (bucket::name(bucket_name), object::key(key)) else {
            return false;
        };
        let Ok(address) = addressed_version(request) else {
            return false;
        };
        let Ok(bucket_acl) = self.storage.get_bucket_acl(&bucket_name).await else {
            return false;
        };
        let bucket_owner = self.resolved_owner(&bucket_acl);
        if request.principal == bucket_owner {
            return true;
        }
        let Ok(acl) = self.addressed_acl(&bucket_name, &key, &address).await else {
            return false;
        };
        may_bypass_authority(
            &request.principal,
            request.authenticated,
            &acl,
            &bucket_owner,
            &self.identity.default_owner,
        )
    }

    /// The ACL of the resource an address names — the shared "address → ACL"
    /// resolution of the operation gate and [`Self::may_bypass`]: the
    /// addressed version's ACL for a version-addressed request
    /// (`version_acl`; the literal `null` address is the null version's
    /// `None` identity), the current object's for
    /// [`version::Address::Current`]. A backend read failure is returned
    /// unchanged so each caller applies its own posture (the gate's
    /// deny-first tiers, the authority's fail-closed `false`).
    async fn addressed_acl(
        &self,
        bucket_name: &bucket::Name,
        key: &object::Key,
        address: &version::Address,
    ) -> Result<Acl, <S as Storage>::Error> {
        if address.is_addressed() {
            self.storage
                .version_acl(bucket_name, key, address.id())
                .await
        } else {
            self.storage.get_object_acl(bucket_name, key).await
        }
    }

    /// The authorization pipeline (spec §4 order): the
    /// expected-bucket-owner header, the DeleteObjects coarse gate
    /// BEFORE `rule_for` (review B2), the operation mapping, the
    /// resource gates (owner bypass → grants), the B1 existence head,
    /// and the fail-closed / three-tier storage-error posture (B3).
    async fn authorize(&self, request: &Request) -> S3Result<()> {
        // (2) expected_bucket_owner: a mismatch on an existing bucket →
        // 403; a missing bucket skips (the handler answers NoSuchBucket).
        if let Some(expected) = &request.expected_bucket_owner
            && let Some(bucket_name) = request.path.get_bucket_name()
        {
            let name = bucket::name(bucket_name).map_err(|_| deny_with(Reason::NoResource))?;
            let bucket_acl = match self.storage.get_bucket_acl(&name).await {
                Ok(acl) => Some(acl),
                Err(err) => match err.into() {
                    storage::Error::NoSuchBucket(_) => None,
                    _ => return deny(Reason::StorageError),
                },
            };
            if let Some(acl) = bucket_acl
                && self.resolved_owner(&acl).as_str() != expected.as_str()
            {
                return deny(Reason::OwnerMismatch);
            }
        }
        // (3) DeleteObjects: the coarse gate special-cases BEFORE
        // `rule_for` (review B2 — the matrix cannot express it).
        if request.op == "DeleteObjects" {
            return self.delete_objects_gate(request).await;
        }
        let rule = rule_for(&request.op);
        if is_upload_scoped(&request.op) {
            return self.upload_gate(request, rule).await;
        }
        if rule.reads_source {
            return self.source_gate(request, rule).await;
        }
        self.resource_gate(request, rule).await
    }

    /// The DeleteObjects coarse gate (review B2): bucket owner or ANY
    /// bucket-ACL grant to P (any permission — a WRITE-gated whole-op
    /// 403 would wrongly block per-key own-object deletes). The per-key
    /// checks are the handler's (Task 12).
    async fn delete_objects_gate(&self, request: &Request) -> S3Result<()> {
        let Some(bucket_name) = request.path.get_bucket_name() else {
            return deny(Reason::NoResource);
        };
        let name = bucket::name(bucket_name).map_err(|_| deny_with(Reason::NoResource))?;
        let bucket_acl = match self.storage.get_bucket_acl(&name).await {
            Ok(acl) => acl,
            Err(err) => return self.missing_tiers(&name, &err.into(), request).await,
        };
        if self.is_owner(&bucket_acl, &request.principal) {
            return allow(Reason::Owner);
        }
        if bucket_acl
            .grants
            .iter()
            .any(|g| grantee_matches(&g.grantee, &request.principal, request.authenticated))
        {
            return allow(Reason::Grant);
        }
        deny(Reason::NoGrant)
    }

    /// Upload-scoped ops (UploadPart, UploadPartCopy, Complete, Abort,
    /// ListParts): the resource is the upload row resolved via the
    /// `uploadId` query parameter — the S3Path key alone cannot identify
    /// one of several in-flight uploads. The gate is creator-or-bucket
    /// owner (the upload row's stored owner is "the creator"; no grant
    /// satisfies it). UploadPartCopy additionally needs source READ;
    /// Complete additionally applies the B1 destination head.
    async fn upload_gate(&self, request: &Request, rule: OpRule) -> S3Result<()> {
        let (bucket_name, key) = object_resource(request)?;
        let upload_id = query_param(&request.query, "uploadId").unwrap_or_default();
        let upload = match self
            .storage
            .get_multipart_upload(&bucket_name, &key, &upload_id)
            .await
        {
            Ok(upload) => upload,
            Err(err) => return self.missing_tiers(&bucket_name, &err.into(), request).await,
        };
        // creator-or-bucket-owner (the single-key rule's one home —
        // [`can_delete`], see [`object_or_bucket_owner`]: the upload
        // row's raw owner is the object leg, the bucket row supplies the
        // bucket leg).
        let creator = upload
            .owner
            .as_ref()
            .unwrap_or(&self.identity.default_owner);
        if creator == &request.principal {
            Reason::UploadCreator.record();
        } else {
            let bucket_acl = match self.storage.get_bucket_acl(&bucket_name).await {
                Ok(acl) => acl,
                Err(err) => return self.missing_tiers(&bucket_name, &err.into(), request).await,
            };
            if !can_delete(
                &request.principal,
                &self.resolved_owner(&bucket_acl),
                &self.identity.default_owner,
                upload.owner.as_ref(),
            ) {
                return deny(Reason::NoGrant);
            }
            Reason::Owner.record();
        }
        if rule.reads_source {
            // UploadPartCopy: copy parity on the source (source READ).
            let (src_bucket, src_key) =
                match request.copy_source.as_deref().and_then(source::copy_source) {
                    Some(pair) => pair,
                    None => return deny(Reason::MalformedSource),
                };
            self.object_gate(&src_bucket, &src_key, Requirement::ObjectRead, request)
                .await?;
        }
        if rule.existence_dispatch {
            // CompleteMultipartUpload: the B1 destination head on top of
            // the upload row's gate.
            self.destination_head(&bucket_name, &key, request).await?;
        }
        Ok(())
    }

    /// CopyObject / RenameObject: the base requirement evaluates against
    /// the SOURCE, parsed by the one authority in [`crate::source`], plus
    /// the destination-bucket WRITE overlay and the B1 destination head.
    /// A RenameObject source is the key of the request's own bucket
    /// (never a bucket split); a CopyObject source is `/bucket/key`.
    async fn source_gate(&self, request: &Request, rule: OpRule) -> S3Result<()> {
        let (dst_bucket, dst_key) = object_resource(request)?;
        let (src_bucket, src_key) = match if request.op == "RenameObject" {
            request
                .rename_source
                .as_deref()
                .and_then(source::rename_source)
                .map(|key| (dst_bucket.clone(), key))
        } else {
            request.copy_source.as_deref().and_then(source::copy_source)
        } {
            Some(pair) => pair,
            // A missing/malformed source header fails closed — the check
            // runs before s3s deserializes the input.
            None => return deny(Reason::MalformedSource),
        };
        match rule.base {
            // CopyObject: source object READ (owner bypass + source ACL).
            Requirement::ObjectRead => {
                self.object_gate(&src_bucket, &src_key, Requirement::ObjectRead, request)
                    .await?;
            }
            // RenameObject: source delete-parity (rename removes the
            // source) — P == O(source object) or P == O(source bucket).
            Requirement::ObjectOrBucketOwner => {
                self.object_or_bucket_owner(&src_bucket, &src_key, request)
                    .await?;
            }
            _ => return deny(Reason::NoGrant), // unreachable per the matrix — fail closed
        }
        if rule.writes_destination_bucket {
            self.bucket_gate(&dst_bucket, Requirement::BucketWrite, request)
                .await?;
        }
        if rule.existence_dispatch {
            self.destination_head(&dst_bucket, &dst_key, request)
                .await?;
        }
        Ok(())
    }

    /// The S3Path-resource branch: bucket gates, object gates, the
    /// PutObject existence dispatch (B1 — an existing destination key
    /// replaces the base with destination owner parity), the
    /// owner-only defaults, and the root-level Authenticated gate.
    async fn resource_gate(&self, request: &Request, rule: OpRule) -> S3Result<()> {
        match &request.path {
            S3Path::Root => match rule.base {
                // ListBuckets: any signed principal.
                Requirement::Authenticated => {
                    if request.authenticated {
                        allow(Reason::Authenticated)
                    } else {
                        deny(Reason::NoGrant)
                    }
                }
                _ => deny(Reason::NoGrant), // no resource owner — fail closed
            },
            S3Path::Bucket { bucket } => {
                let name =
                    bucket::name(bucket.as_ref()).map_err(|_| deny_with(Reason::NoResource))?;
                match rule.base {
                    // CreateBucket: an authenticated principal only.
                    Requirement::Authenticated => {
                        if request.authenticated {
                            allow(Reason::Authenticated)
                        } else {
                            deny(Reason::NoGrant)
                        }
                    }
                    // Policy-only bucket ops and resolved-but-unmapped
                    // ops: owner only (default B).
                    Requirement::OwnerOnly => self.bucket_owner_only(&name, request).await,
                    Requirement::BucketRead
                    | Requirement::BucketWrite
                    | Requirement::BucketReadAcp
                    | Requirement::BucketWriteAcp => {
                        // PostObject (existence_dispatch on a bucket path)
                        // runs the base only — the B1 existence head for
                        // PostObject is enforced at the handler
                        // (op_put_object sees the form key via s3s's
                        // default delegation), Task 11/12; the access
                        // layer applies the base BucketWrite only —
                        // recorded deviation per the B1 amendment.
                        self.bucket_gate(&name, rule.base, request).await
                    }
                    _ => deny(Reason::NoGrant),
                }
            }
            S3Path::Object { bucket, key } => {
                let name =
                    bucket::name(bucket.as_ref()).map_err(|_| deny_with(Reason::NoResource))?;
                let key = object::key(key.as_ref()).map_err(|_| deny_with(Reason::NoResource))?;
                match rule.base {
                    Requirement::ObjectRead
                    | Requirement::ObjectReadAcp
                    | Requirement::ObjectWriteAcp => {
                        self.object_gate(&name, &key, rule.base, request).await
                    }
                    // PutObject: the B1 destination head REPLACES the
                    // base (an existing key needs destination owner
                    // parity; a missing key runs bucket WRITE).
                    // CreateMultipartUpload (object path, no dispatch,
                    // not upload-scoped): the base bucket-WRITE gate.
                    Requirement::BucketWrite => {
                        if rule.existence_dispatch {
                            self.put_object_gate(&name, &key, request).await
                        } else {
                            self.bucket_gate(&name, Requirement::BucketWrite, request)
                                .await
                        }
                    }
                    // DeleteObject, object tagging, GetObjectAttributes and
                    // Object Lock's four per-version operations: object-owner-
                    // or-bucket-owner parity (version-addressed where the
                    // request names a version).
                    Requirement::ObjectOrBucketOwner => {
                        self.object_or_bucket_owner(&name, &key, request).await
                    }
                    // Resolved-but-unmapped object ops: the object owner
                    // only (default B).
                    Requirement::OwnerOnly => self.object_owner_only(&name, &key, request).await,
                    _ => deny(Reason::NoGrant),
                }
            }
        }
    }

    /// PutObject's destination head (review B1): an existing key needs
    /// destination owner parity (a WRITE grantee may create new keys but
    /// not overwrite — classic ACL); a missing key runs the base
    /// (bucket WRITE).
    async fn put_object_gate(
        &self,
        bucket_name: &bucket::Name,
        key: &object::Key,
        request: &Request,
    ) -> S3Result<()> {
        match self.storage.get_object_acl(bucket_name, key).await {
            Ok(dst_acl) => {
                self.destination_owner_parity(bucket_name, &dst_acl, request)
                    .await
            }
            Err(err) => match err.into() {
                storage::Error::NoSuchKey(_) => {
                    self.bucket_gate(bucket_name, Requirement::BucketWrite, request)
                        .await
                }
                err => self.missing_tiers(bucket_name, &err, request).await,
            },
        }
    }

    /// The B1 destination head: an existing destination needs owner
    /// parity on top of the op's other gates; a missing destination adds
    /// nothing.
    async fn destination_head(
        &self,
        bucket_name: &bucket::Name,
        key: &object::Key,
        request: &Request,
    ) -> S3Result<()> {
        match self.storage.get_object_acl(bucket_name, key).await {
            Ok(acl) => {
                self.destination_owner_parity(bucket_name, &acl, request)
                    .await
            }
            Err(err) => match err.into() {
                storage::Error::NoSuchKey(_) => Ok(()),
                err => self.missing_tiers(bucket_name, &err, request).await,
            },
        }
    }

    /// An existing destination's owner-parity gate (B1): P == O
    /// (destination object) or P == O(destination bucket) — no grant
    /// satisfies it (the single-key rule's one home —
    /// [`can_delete`], see [`object_or_bucket_owner`]).
    async fn destination_owner_parity(
        &self,
        bucket_name: &bucket::Name,
        dst_acl: &Acl,
        request: &Request,
    ) -> S3Result<()> {
        if self.is_owner(dst_acl, &request.principal) {
            return allow(Reason::Owner);
        }
        let bucket_acl = match self.storage.get_bucket_acl(bucket_name).await {
            Ok(acl) => acl,
            Err(err) => return self.missing_tiers(bucket_name, &err.into(), request).await,
        };
        if can_delete(
            &request.principal,
            &self.resolved_owner(&bucket_acl),
            &self.identity.default_owner,
            dst_acl.owner.as_ref(),
        ) {
            allow(Reason::Owner)
        } else {
            deny(Reason::NoGrant)
        }
    }

    /// A bucket-resource gate: owner bypass, then grant evaluation over
    /// the bucket ACL.
    async fn bucket_gate(
        &self,
        bucket_name: &bucket::Name,
        requirement: Requirement,
        request: &Request,
    ) -> S3Result<()> {
        let bucket_acl = match self.storage.get_bucket_acl(bucket_name).await {
            Ok(acl) => acl,
            Err(err) => return self.missing_tiers(bucket_name, &err.into(), request).await,
        };
        if self.is_owner(&bucket_acl, &request.principal) {
            return allow(Reason::Owner);
        }
        if grant_satisfies(&bucket_acl, requirement, request) {
            return allow(Reason::Grant);
        }
        deny(Reason::NoGrant)
    }

    /// An object-resource gate: owner bypass, then grant evaluation over
    /// the object ACL.
    async fn object_gate(
        &self,
        bucket_name: &bucket::Name,
        key: &object::Key,
        requirement: Requirement,
        request: &Request,
    ) -> S3Result<()> {
        let object_acl = match self.storage.get_object_acl(bucket_name, key).await {
            Ok(acl) => acl,
            Err(err) => return self.missing_tiers(bucket_name, &err.into(), request).await,
        };
        if self.is_owner(&object_acl, &request.principal) {
            return allow(Reason::Owner);
        }
        if grant_satisfies(&object_acl, requirement, request) {
            return allow(Reason::Grant);
        }
        deny(Reason::NoGrant)
    }

    /// Object-owner-or-bucket-owner parity (delete/overwrite parity, the
    /// default-A class): no grant satisfies the gate. The single-key
    /// rule's ONE home is [`can_delete`] (`tinio_core::acl` — the
    /// handler-side per-key DeleteObjects check runs the same helper).
    ///
    /// R23: for the object operations the matrix marks
    /// `version_addressed` the object leg is the ACL of the version the
    /// request ADDRESSES (`version_acl` through [`Self::addressed_acl`]) —
    /// the current object may be a different object entirely. An
    /// unaddressed request keeps the current object's ACL. The bucket
    /// owner has parity over every version and short-circuits before the
    /// address is resolved; a malformed address fails closed.
    async fn object_or_bucket_owner(
        &self,
        bucket_name: &bucket::Name,
        key: &object::Key,
        request: &Request,
    ) -> S3Result<()> {
        let bucket_acl = match self.storage.get_bucket_acl(bucket_name).await {
            Ok(acl) => acl,
            Err(err) => return self.missing_tiers(bucket_name, &err.into(), request).await,
        };
        let bucket_owner = self.resolved_owner(&bucket_acl);
        if request.principal == bucket_owner {
            return allow(Reason::Owner);
        }
        let object_acl = if rule_for(&request.op).version_addressed {
            let address = match addressed_version(request) {
                Ok(address) => address,
                // A malformed `versionId` names no resource to authorize:
                // fail closed (a request the pipeline cannot resolve is
                // never authorized the way the current object would be).
                Err(()) => return deny(Reason::MalformedVersion),
            };
            match self.addressed_acl(bucket_name, key, &address).await {
                Ok(acl) => acl,
                Err(err) => {
                    let err: storage::Error = err.into();
                    // `version_acl` folds a delete marker and an unknown id
                    // into `NoSuchVersion`; only the genuinely absent
                    // version may ride the missing-version pass-through to
                    // the handler's OL17 404. Everything else — the marker,
                    // unreadable state — takes its own tier.
                    if matches!(err, storage::Error::NoSuchVersion(_)) {
                        return self
                            .addressed_version_tiers(bucket_name, key, &address)
                            .await;
                    }
                    return self.missing_tiers(bucket_name, &err, request).await;
                }
            }
        } else {
            match self.storage.get_object_acl(bucket_name, key).await {
                Ok(acl) => acl,
                Err(err) => return self.missing_tiers(bucket_name, &err.into(), request).await,
            }
        };
        if can_delete(
            &request.principal,
            &bucket_owner,
            &self.identity.default_owner,
            object_acl.owner.as_ref(),
        ) {
            allow(Reason::Owner)
        } else {
            deny(Reason::NoGrant)
        }
    }

    /// Owner-only gates (policy-only bucket ops, unmapped ops — default
    /// B): P == O(bucket) only; grants never satisfy.
    async fn bucket_owner_only(
        &self,
        bucket_name: &bucket::Name,
        request: &Request,
    ) -> S3Result<()> {
        let bucket_acl = match self.storage.get_bucket_acl(bucket_name).await {
            Ok(acl) => acl,
            Err(err) => return self.missing_tiers(bucket_name, &err.into(), request).await,
        };
        if self.is_owner(&bucket_acl, &request.principal) {
            allow(Reason::Owner)
        } else {
            deny(Reason::NoGrant)
        }
    }

    /// Owner-only gates on an object resource: P == O(object) only.
    async fn object_owner_only(
        &self,
        bucket_name: &bucket::Name,
        key: &object::Key,
        request: &Request,
    ) -> S3Result<()> {
        let object_acl = match self.storage.get_object_acl(bucket_name, key).await {
            Ok(acl) => acl,
            Err(err) => return self.missing_tiers(bucket_name, &err.into(), request).await,
        };
        if self.is_owner(&object_acl, &request.principal) {
            allow(Reason::Owner)
        } else {
            deny(Reason::NoGrant)
        }
    }

    /// The deny-first `NoSuch*` tiers (review B3): the bucket owner
    /// → pass through (the handler's 404/204); a bucket-READ
    /// grantee → pass through; a genuinely missing bucket (its own
    /// row read answers `NoSuchBucket`) → pass through to the
    /// handler's `NoSuchBucket` (AWS + contract FR-005); otherwise
    /// 403 (no existence leak for an absent row on an existing
    /// resource). Every other storage error fails closed.
    async fn missing_tiers(
        &self,
        bucket_name: &bucket::Name,
        err: &storage::Error,
        request: &Request,
    ) -> S3Result<()> {
        if !matches!(classify(err), AccessDecision::PassThroughMissing) {
            return deny(Reason::StorageError);
        }
        let bucket_acl = match self.storage.get_bucket_acl(bucket_name).await {
            Ok(acl) => acl,
            // A genuinely missing bucket (the tier proof's own read
            // answers NoSuchBucket) passes through — the handler answers
            // NoSuchBucket, AWS + contract FR-005 (the existence oracle
            // the B3 never-reveal tiers closed). Any other storage
            // failure still fails closed.
            Err(probe) => match probe.into() {
                storage::Error::NoSuchBucket(_) => return allow(Reason::Missing),
                _ => return deny(Reason::StorageError),
            },
        };
        if self.is_owner(&bucket_acl, &request.principal) {
            return allow(Reason::Owner);
        }
        if grant_satisfies(&bucket_acl, Requirement::BucketRead, request) {
            return allow(Reason::Grant);
        }
        deny(Reason::NoGrant)
    }

    /// Classify an addressed-ACL read that answered `NoSuchVersion`. The
    /// stores report a **delete marker** and a genuinely unknown id with
    /// the same class (`marker_has_no_metadata`), so the class alone cannot
    /// decide: a marker is a real, addressable version, not a missing
    /// resource, and a non-owner's marker delete is 403
    /// (`Reason::DeleteMarker` — an intentional denial, never a backend
    /// failure), while an id that names nothing is the handler's
    /// `404 NoSuchVersion` (OL17). A content version that the ACL read
    /// failed to resolve is unreadable state and also fails closed.
    async fn addressed_version_tiers(
        &self,
        bucket_name: &bucket::Name,
        key: &object::Key,
        address: &version::Address,
    ) -> S3Result<()> {
        // The shared head split. This caller keeps its own statuses: a marker
        // and an unresolved object fail closed (`403`), a miss is the
        // handler's `404`, and any other storage error fails closed.
        match version::split_head(
            self.storage
                .head_object_version(bucket_name, key, address.id())
                .await,
        ) {
            version::HeadSplit::Marker { .. } => deny(Reason::DeleteMarker),
            version::HeadSplit::Object(_) => deny(Reason::StorageError),
            // The genuine miss: the handler answers `404 NoSuchVersion`
            // (a missing key keeps its own `404 NoSuchKey`).
            version::HeadSplit::Missing(_) => allow(Reason::Missing),
            version::HeadSplit::Failed(_) => deny(Reason::StorageError),
        }
    }

    /// The resolved owner of a resource row: the stored owner element,
    /// or the identity's default (lazy) owner when the row has none.
    fn resolved_owner(&self, acl: &Acl) -> OwnerId {
        acl.owner
            .clone()
            .unwrap_or_else(|| self.identity.default_owner.clone())
    }

    /// Owner bypass: P == O — allowed regardless of grants
    /// (implicit-owner rule).
    fn is_owner(&self, acl: &Acl, principal: &OwnerId) -> bool {
        match &acl.owner {
            Some(owner) => owner == principal,
            None => &self.identity.default_owner == principal,
        }
    }
}

/// The owned copy of a route path — [`S3Path`] is not `Clone`, and the
/// request must release its borrow of the access context before the
/// extension bit can be written.
fn owned_path(path: &S3Path) -> S3Path {
    match path {
        S3Path::Root => S3Path::root(),
        S3Path::Bucket { bucket } => S3Path::bucket(bucket),
        S3Path::Object { bucket, key } => S3Path::object(bucket, key),
    }
}

/// The (bucket, key) of an object-path request (upload-scoped and
/// source-reading ops always resolve to S3Path::Object); anything else
/// fails closed.
fn object_resource(request: &Request) -> Result<(bucket::Name, object::Key), S3Error> {
    let (b, k) = request
        .path
        .as_object()
        .ok_or_else(|| deny_with(Reason::NoResource))?;
    Ok((
        bucket::name(b).map_err(|_| deny_with(Reason::NoResource))?,
        object::key(k).map_err(|_| deny_with(Reason::NoResource))?,
    ))
}

/// Fail closed: the canonical `AccessDenied` response (AWS's default
/// message).
fn denied() -> S3Error {
    s3_error!(AccessDenied, "Access Denied")
}

/// The version address a request's `versionId` names — the ONE parse the
/// operation gate and the bypass authority share. An absent parameter is
/// [`version::Address::Current`]; a malformed value is an error, never a
/// silent fall back to the current object's ACL. The literal `null` is
/// [`version::Address::Null`] (the null version), never a version id.
fn addressed_version(request: &Request) -> Result<version::Address, ()> {
    match query_param(&request.query, "versionId") {
        None => Ok(version::Address::Current),
        Some(raw) => version::Address::from_wire(Some(&raw)).map_err(|_| ()),
    }
}

/// The upload-scoped operations whose resource is the upload row (spec
/// §4) — the S3Path key alone cannot identify one of several in-flight
/// uploads.
fn is_upload_scoped(op: &str) -> bool {
    matches!(
        op,
        "UploadPart"
            | "UploadPartCopy"
            | "CompleteMultipartUpload"
            | "AbortMultipartUpload"
            | "ListParts"
    )
}

/// The value of one query parameter, form-decoded (mirroring s3s's
/// `serde_urlencoded` query parse: `%XX` sequences, then `+` as space).
fn query_param(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == name).then(|| form_decode(value))
    })
}

/// Form decoding (`application/x-www-form-urlencoded`, matching s3s's
/// `serde_urlencoded` query parse): `+` as space, then `%XX` sequences
/// (`%2B` still decodes to a literal `+`).
fn form_decode(raw: &str) -> String {
    percent::decode(&raw.replace('+', "%20"))
}

/// Grantee membership: canonical IDs compare to P; AllUsers covers any P
/// (anonymous included); AuthenticatedUsers any signed P; LogDelivery
/// (and any unknown URI) is never satisfiable — fail closed.
pub(crate) fn grantee_matches(grantee: &Grantee, principal: &OwnerId, authenticated: bool) -> bool {
    match grantee {
        Grantee::Canonical(id) => id == principal,
        Grantee::Group(GroupUri(uri)) => match uri.as_str() {
            GROUP_ALL_USERS => true,
            GROUP_AUTHENTICATED_USERS => authenticated,
            _ => false,
        },
    }
}

/// Permission → requirement satisfaction (the AWS grant semantics).
fn permission_satisfies(requirement: Requirement, permission: Permission) -> bool {
    match permission {
        Permission::FullControl => true,
        Permission::Read => matches!(
            requirement,
            Requirement::BucketRead | Requirement::ObjectRead
        ),
        Permission::Write => matches!(requirement, Requirement::BucketWrite),
        Permission::ReadAcp => {
            matches!(
                requirement,
                Requirement::BucketReadAcp | Requirement::ObjectReadAcp
            )
        }
        Permission::WriteAcp => {
            matches!(
                requirement,
                Requirement::BucketWriteAcp | Requirement::ObjectWriteAcp
            )
        }
    }
}

/// Grant evaluation: the ACL grants the requirement to P.
fn grant_satisfies(acl: &Acl, requirement: Requirement, request: &Request) -> bool {
    acl.grants.iter().any(|grant| {
        grantee_matches(&grant.grantee, &request.principal, request.authenticated)
            && permission_satisfies(requirement, grant.permission)
    })
}

#[cfg(test)]
pub(crate) mod tests;
