//! The pre-route authorization pipeline: [`AclAccess`] implements the
//! s3s `S3Access` seam (spec §4) — principal resolution, the
//! `x-amz-expected-bucket-owner` header, the operation→requirement
//! mapping, the B1 destination-existence head, owner bypass, grant
//! evaluation with the group rules, and the fail-closed storage-error
//! posture (`NoSuch*` → the deny-first three tiers, review B3).

use std::sync::Arc;

use s3s::{
    S3Error, S3Result,
    access::{S3Access, S3AccessContext},
    path::S3Path,
    s3_error,
};

use crate::{
    _core::{
        acl::{
            Acl, GROUP_ALL_USERS, GROUP_AUTHENTICATED_USERS, Grantee, GroupUri, OwnerId,
            Permission, can_delete,
        },
        bucket, object, percent,
        storage::{self, Storage},
    },
    error::{AccessDecision, classify},
    identity::Identity,
    matrix::{OpRule, Requirement, rule_for},
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
struct Request<'a> {
    /// The requester's canonical ID (resolved by
    /// [`Identity::principal`]).
    principal: OwnerId,
    /// Whether the request carried verified credentials.
    authenticated: bool,
    /// The s3s operation name.
    op: &'a str,
    /// The route path (root/bucket/object).
    path: &'a S3Path,
    /// The raw request query (the `uploadId` parameter).
    query: &'a str,
    /// `x-amz-expected-bucket-owner`.
    expected_bucket_owner: Option<&'a str>,
    /// `x-amz-copy-source` (CopyObject, UploadPartCopy).
    copy_source: Option<&'a str>,
    /// `x-amz-rename-source` (RenameObject).
    rename_source: Option<&'a str>,
}

#[async_trait::async_trait]
impl<S: Storage> S3Access for AclAccess<S> {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        // Precondition (Task 7 review note): s3s invokes `S3Access` only
        // AFTER signature verification against the auth provider —
        // `ConfigAuth` rejects unknown access keys — so `credentials()`
        // is a verified signed user and `Identity::principal`'s
        // expect-panic on an unknown key is unreachable here.
        let principal = self.identity.principal(cx.credentials());
        let headers = cx.headers();
        self.evaluate(&Request {
            principal,
            authenticated: cx.credentials().is_some(),
            op: cx.s3_op().name(),
            path: cx.s3_path(),
            query: cx.uri().query().unwrap_or(""),
            // A present-but-unreadable (non-UTF-8) expected-bucket-owner
            // header fails closed (403) — never a silent skip; the source
            // headers fail closed the same way (None → deny downstream).
            expected_bucket_owner: match headers.get("x-amz-expected-bucket-owner") {
                Some(value) => Some(value.to_str().map_err(|_| denied())?),
                None => None,
            },
            copy_source: headers
                .get("x-amz-copy-source")
                .and_then(|v| v.to_str().ok()),
            rename_source: headers
                .get("x-amz-rename-source")
                .and_then(|v| v.to_str().ok()),
        })
        .await
    }
}

impl<S: Storage> AclAccess<S> {
    /// The authorization pipeline (spec §4 order): the
    /// expected-bucket-owner header, the DeleteObjects coarse gate
    /// BEFORE `rule_for` (review B2), the operation mapping, the
    /// resource gates (owner bypass → grants), the B1 existence head,
    /// and the fail-closed / three-tier storage-error posture (B3).
    async fn evaluate(&self, request: &Request<'_>) -> S3Result<()> {
        // (2) expected_bucket_owner: a mismatch on an existing bucket →
        // 403; a missing bucket skips (the handler answers NoSuchBucket).
        if let Some(expected) = request.expected_bucket_owner
            && let Some(bucket_name) = request.path.get_bucket_name()
        {
            let name = bucket::name(bucket_name).map_err(|_| denied())?;
            let bucket_acl = match self.storage.get_bucket_acl(&name).await {
                Ok(acl) => Some(acl),
                Err(err) => match err.into() {
                    storage::Error::NoSuchBucket(_) => None,
                    _ => return Err(denied()),
                },
            };
            if let Some(acl) = bucket_acl
                && self.resolved_owner(&acl).as_str() != expected
            {
                return Err(denied());
            }
        }
        // (3) DeleteObjects: the coarse gate special-cases BEFORE
        // `rule_for` (review B2 — the matrix cannot express it).
        if request.op == "DeleteObjects" {
            return self.delete_objects_gate(request).await;
        }
        let rule = rule_for(request.op);
        if is_upload_scoped(request.op) {
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
    async fn delete_objects_gate(&self, request: &Request<'_>) -> S3Result<()> {
        let Some(bucket_name) = request.path.get_bucket_name() else {
            return Err(denied());
        };
        let name = bucket::name(bucket_name).map_err(|_| denied())?;
        let bucket_acl = match self.storage.get_bucket_acl(&name).await {
            Ok(acl) => acl,
            Err(err) => return self.missing_tiers(&name, &err.into(), request).await,
        };
        if self.is_owner(&bucket_acl, &request.principal) {
            return Ok(());
        }
        if bucket_acl
            .grants
            .iter()
            .any(|g| grantee_matches(&g.grantee, &request.principal, request.authenticated))
        {
            return Ok(());
        }
        Err(denied())
    }

    /// Upload-scoped ops (UploadPart, UploadPartCopy, Complete, Abort,
    /// ListParts): the resource is the upload row resolved via the
    /// `uploadId` query parameter — the S3Path key alone cannot identify
    /// one of several in-flight uploads. The gate is creator-or-bucket
    /// owner (the upload row's stored owner is "the creator"; no grant
    /// satisfies it). UploadPartCopy additionally needs source READ;
    /// Complete additionally applies the B1 destination head.
    async fn upload_gate(&self, request: &Request<'_>, rule: OpRule) -> S3Result<()> {
        let (bucket_name, key) = object_resource(request)?;
        let upload_id = query_param(request.query, "uploadId").unwrap_or_default();
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
        if creator != &request.principal {
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
                return Err(denied());
            }
        }
        if rule.reads_source {
            // UploadPartCopy: copy parity on the source (source READ).
            let (src_bucket, src_key) = match request.copy_source.and_then(parse_source) {
                Some(pair) => pair,
                None => return Err(denied()),
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
    /// the SOURCE (parsed from `x-amz-copy-source` /
    /// `x-amz-rename-source`, both `/bucket/key`, URL-encoded per the
    /// s3s dto), plus the destination-bucket WRITE overlay and the B1
    /// destination head.
    async fn source_gate(&self, request: &Request<'_>, rule: OpRule) -> S3Result<()> {
        let (dst_bucket, dst_key) = object_resource(request)?;
        let source = if request.op == "RenameObject" {
            request.rename_source
        } else {
            request.copy_source
        };
        let (src_bucket, src_key) = match source.and_then(parse_source) {
            Some(pair) => pair,
            // A missing/malformed source header fails closed — the check
            // runs before s3s deserializes the input.
            None => return Err(denied()),
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
            _ => return Err(denied()), // unreachable per the matrix — fail closed
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
    async fn resource_gate(&self, request: &Request<'_>, rule: OpRule) -> S3Result<()> {
        match request.path {
            S3Path::Root => match rule.base {
                // ListBuckets: any signed principal.
                Requirement::Authenticated => {
                    if request.authenticated {
                        Ok(())
                    } else {
                        Err(denied())
                    }
                }
                _ => Err(denied()), // no resource owner — fail closed
            },
            S3Path::Bucket { bucket } => {
                let name = bucket::name(bucket.as_ref()).map_err(|_| denied())?;
                match rule.base {
                    // CreateBucket: an authenticated principal only.
                    Requirement::Authenticated => {
                        if request.authenticated {
                            Ok(())
                        } else {
                            Err(denied())
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
                    _ => Err(denied()),
                }
            }
            S3Path::Object { bucket, key } => {
                let name = bucket::name(bucket.as_ref()).map_err(|_| denied())?;
                let key = object::key(key.as_ref()).map_err(|_| denied())?;
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
                    // DeleteObject, object tagging, GetObjectAttributes:
                    // object-owner-or-bucket-owner parity.
                    Requirement::ObjectOrBucketOwner => {
                        self.object_or_bucket_owner(&name, &key, request).await
                    }
                    // Resolved-but-unmapped object ops: the object owner
                    // only (default B).
                    Requirement::OwnerOnly => self.object_owner_only(&name, &key, request).await,
                    _ => Err(denied()),
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
        request: &Request<'_>,
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
        request: &Request<'_>,
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
        request: &Request<'_>,
    ) -> S3Result<()> {
        if self.is_owner(dst_acl, &request.principal) {
            return Ok(());
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
            Ok(())
        } else {
            Err(denied())
        }
    }

    /// A bucket-resource gate: owner bypass, then grant evaluation over
    /// the bucket ACL.
    async fn bucket_gate(
        &self,
        bucket_name: &bucket::Name,
        requirement: Requirement,
        request: &Request<'_>,
    ) -> S3Result<()> {
        let bucket_acl = match self.storage.get_bucket_acl(bucket_name).await {
            Ok(acl) => acl,
            Err(err) => return self.missing_tiers(bucket_name, &err.into(), request).await,
        };
        if self.is_owner(&bucket_acl, &request.principal) {
            return Ok(());
        }
        if grant_satisfies(&bucket_acl, requirement, request) {
            return Ok(());
        }
        Err(denied())
    }

    /// An object-resource gate: owner bypass, then grant evaluation over
    /// the object ACL.
    async fn object_gate(
        &self,
        bucket_name: &bucket::Name,
        key: &object::Key,
        requirement: Requirement,
        request: &Request<'_>,
    ) -> S3Result<()> {
        let object_acl = match self.storage.get_object_acl(bucket_name, key).await {
            Ok(acl) => acl,
            Err(err) => return self.missing_tiers(bucket_name, &err.into(), request).await,
        };
        if self.is_owner(&object_acl, &request.principal) {
            return Ok(());
        }
        if grant_satisfies(&object_acl, requirement, request) {
            return Ok(());
        }
        Err(denied())
    }

    /// Object-owner-or-bucket-owner parity (delete/overwrite parity, the
    /// default-A class): no grant satisfies the gate. The single-key
    /// rule's ONE home is [`can_delete`] (`tinio_core::acl` — the
    /// handler-side per-key DeleteObjects check runs the same helper).
    async fn object_or_bucket_owner(
        &self,
        bucket_name: &bucket::Name,
        key: &object::Key,
        request: &Request<'_>,
    ) -> S3Result<()> {
        let object_acl = match self.storage.get_object_acl(bucket_name, key).await {
            Ok(acl) => acl,
            Err(err) => return self.missing_tiers(bucket_name, &err.into(), request).await,
        };
        let bucket_acl = match self.storage.get_bucket_acl(bucket_name).await {
            Ok(acl) => acl,
            Err(err) => return self.missing_tiers(bucket_name, &err.into(), request).await,
        };
        if can_delete(
            &request.principal,
            &self.resolved_owner(&bucket_acl),
            &self.identity.default_owner,
            object_acl.owner.as_ref(),
        ) {
            Ok(())
        } else {
            Err(denied())
        }
    }

    /// Owner-only gates (policy-only bucket ops, unmapped ops — default
    /// B): P == O(bucket) only; grants never satisfy.
    async fn bucket_owner_only(
        &self,
        bucket_name: &bucket::Name,
        request: &Request<'_>,
    ) -> S3Result<()> {
        let bucket_acl = match self.storage.get_bucket_acl(bucket_name).await {
            Ok(acl) => acl,
            Err(err) => return self.missing_tiers(bucket_name, &err.into(), request).await,
        };
        if self.is_owner(&bucket_acl, &request.principal) {
            Ok(())
        } else {
            Err(denied())
        }
    }

    /// Owner-only gates on an object resource: P == O(object) only.
    async fn object_owner_only(
        &self,
        bucket_name: &bucket::Name,
        key: &object::Key,
        request: &Request<'_>,
    ) -> S3Result<()> {
        let object_acl = match self.storage.get_object_acl(bucket_name, key).await {
            Ok(acl) => acl,
            Err(err) => return self.missing_tiers(bucket_name, &err.into(), request).await,
        };
        if self.is_owner(&object_acl, &request.principal) {
            Ok(())
        } else {
            Err(denied())
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
        request: &Request<'_>,
    ) -> S3Result<()> {
        if !matches!(classify(err), AccessDecision::PassThroughMissing) {
            return Err(denied());
        }
        let bucket_acl = match self.storage.get_bucket_acl(bucket_name).await {
            Ok(acl) => acl,
            // A genuinely missing bucket (the tier proof's own read
            // answers NoSuchBucket) passes through — the handler answers
            // NoSuchBucket, AWS + contract FR-005 (the existence oracle
            // the B3 never-reveal tiers closed). Any other storage
            // failure still fails closed.
            Err(probe) => match probe.into() {
                storage::Error::NoSuchBucket(_) => return Ok(()),
                _ => return Err(denied()),
            },
        };
        if self.is_owner(&bucket_acl, &request.principal) {
            return Ok(());
        }
        if grant_satisfies(&bucket_acl, Requirement::BucketRead, request) {
            return Ok(());
        }
        Err(denied())
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

/// The (bucket, key) of an object-path request (upload-scoped and
/// source-reading ops always resolve to S3Path::Object); anything else
/// fails closed.
fn object_resource(request: &Request<'_>) -> Result<(bucket::Name, object::Key), S3Error> {
    let (b, k) = request.path.as_object().ok_or_else(denied)?;
    Ok((
        bucket::name(b).map_err(|_| denied())?,
        object::key(k).map_err(|_| denied())?,
    ))
}

/// Fail closed: the canonical `AccessDenied` response (AWS's default
/// message).
fn denied() -> S3Error {
    s3_error!(AccessDenied, "Access Denied")
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

/// Parse a copy/rename source header — `/bucket/key`, both segments
/// URL-encoded (the s3s dto marks the header value URL-encoded).
fn parse_source(raw: &str) -> Option<(bucket::Name, object::Key)> {
    let raw = raw.strip_prefix('/').unwrap_or(raw);
    let (b, k) = raw.split_once('/')?;
    Some((
        bucket::name(percent::decode(b)).ok()?,
        object::key(percent::decode(k)).ok()?,
    ))
}

/// Grantee membership: canonical IDs compare to P; AllUsers covers any P
/// (anonymous included); AuthenticatedUsers any signed P; LogDelivery
/// (and any unknown URI) is never satisfiable — fail closed.
fn grantee_matches(grantee: &Grantee, principal: &OwnerId, authenticated: bool) -> bool {
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
fn grant_satisfies(acl: &Acl, requirement: Requirement, request: &Request<'_>) -> bool {
    acl.grants.iter().any(|grant| {
        grantee_matches(&grant.grantee, &request.principal, request.authenticated)
            && permission_satisfies(requirement, grant.permission)
    })
}

#[cfg(test)]
mod tests {
    use std::{io, sync::Arc};

    use bytes::Bytes;
    use futures::stream;
    use s3s::path::S3Path;
    use tinio_mem::MemoryStorage;

    use super::{AclAccess, OpRule, Request, Requirement, rule_for};
    use crate::{
        _core::{
            acl::{
                ANONYMOUS_CANONICAL_ID, Acl, GROUP_ALL_USERS, GROUP_AUTHENTICATED_USERS, Grant,
                Grantee, GroupUri, OwnerId, Permission, default_owner_id,
            },
            bucket, object,
            storage::{BucketOps, MultipartOps, ObjectOps},
        },
        derive_canonical_id,
        identity::{Identity, User},
    };

    // Fixture identities: two fixed canonical owners distinct from the
    // configured users, and the anonymous principal.
    fn uid() -> OwnerId {
        OwnerId::new("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899").unwrap()
    }

    fn other() -> OwnerId {
        OwnerId::new("ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100").unwrap()
    }

    fn anon() -> OwnerId {
        OwnerId::new(ANONYMOUS_CANONICAL_ID).unwrap()
    }

    fn alice() -> OwnerId {
        derive_canonical_id("AKID")
    }

    fn bob() -> OwnerId {
        derive_canonical_id("BKID")
    }

    fn identity() -> Arc<Identity> {
        Arc::new(Identity::test(vec![
            User::test("AKID", "secret", "alice"),
            User::test("BKID", "secret", "bob"),
        ]))
    }

    fn canonical(id: &OwnerId, permission: Permission) -> Grant {
        Grant {
            grantee: Grantee::Canonical(id.clone()),
            permission,
        }
    }

    fn group(uri: &str, permission: Permission) -> Grant {
        Grant {
            grantee: Grantee::Group(GroupUri(uri.into())),
            permission,
        }
    }

    fn private(owner: &OwnerId) -> Acl {
        Acl::default_private(Some(owner.clone()))
    }

    async fn storage_with_bucket(owner: &OwnerId, grants: Vec<Grant>) -> Arc<MemoryStorage> {
        let storage = MemoryStorage::new().unwrap();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(
                &b,
                Some(owner),
                &Acl {
                    owner: Some(owner.clone()),
                    grants,
                },
            )
            .await
            .unwrap();
        Arc::new(storage)
    }

    async fn put_object(storage: &MemoryStorage, key: &str, owner: Option<&OwnerId>, acl: &Acl) {
        let b = bucket::name("data").unwrap();
        let k = object::key(key).unwrap();
        let staged = storage
            .stage_body(&b, &k, Box::pin(stream::empty::<io::Result<Bytes>>()), None)
            .await
            .unwrap();
        storage
            .commit_object(&b, &k, staged, object::Tags::empty(), owner, acl)
            .await
            .unwrap();
    }

    async fn upload(storage: &MemoryStorage, key: &str, owner: Option<&OwnerId>) -> String {
        let b = bucket::name("data").unwrap();
        let k = object::key(key).unwrap();
        storage
            .create_multipart_upload(
                &b,
                &k,
                None,
                object::Tags::empty(),
                owner,
                &Acl::default_private(owner.cloned()),
            )
            .await
            .unwrap()
            .upload_id
    }

    fn req<'a>(
        principal: OwnerId,
        authenticated: bool,
        op: &'a str,
        path: &'a S3Path,
    ) -> Request<'a> {
        Request {
            principal,
            authenticated,
            op,
            path,
            query: "",
            expected_bucket_owner: None,
            copy_source: None,
            rename_source: None,
        }
    }

    async fn assert_allowed(access: &AclAccess<MemoryStorage>, request: &Request<'_>) {
        access.evaluate(request).await.unwrap();
    }

    async fn assert_denied(access: &AclAccess<MemoryStorage>, request: &Request<'_>) {
        let err = access
            .evaluate(request)
            .await
            .expect_err("expected an AccessDenied rejection");
        assert_eq!(err.code().as_str(), "AccessDenied");
    }

    #[tokio::test]
    async fn anonymous_denied_on_private_bucket() {
        let owner = uid();
        let storage = storage_with_bucket(&owner, vec![]).await;
        put_object(&storage, "a.txt", Some(&owner), &private(&owner)).await;
        let access = AclAccess::new(storage, identity());
        let path = S3Path::object("data", "a.txt");
        assert_denied(&access, &req(anon(), false, "GetObject", &path)).await;
    }

    #[tokio::test]
    async fn public_read_allows_anonymous_get() {
        let owner = uid();
        let storage = storage_with_bucket(&owner, vec![]).await;
        let acl = Acl {
            owner: Some(owner.clone()),
            grants: vec![group(GROUP_ALL_USERS, Permission::Read)],
        };
        put_object(&storage, "a.txt", Some(&owner), &acl).await;
        let access = AclAccess::new(storage, identity());
        let path = S3Path::object("data", "a.txt");
        assert_allowed(&access, &req(anon(), false, "GetObject", &path)).await;
    }

    #[tokio::test]
    async fn owner_bypass_ignores_grants() {
        // The implicit-owner rule: P == O passes without any grant —
        // the ACL carries no owner FULL_CONTROL grant at all.
        let owner = uid();
        let storage = storage_with_bucket(&owner, vec![]).await;
        let acl = Acl {
            owner: Some(alice().clone()),
            grants: vec![],
        };
        put_object(&storage, "a.txt", Some(&alice()), &acl).await;
        let access = AclAccess::new(storage, identity());
        let path = S3Path::object("data", "a.txt");
        assert_allowed(&access, &req(alice(), true, "GetObject", &path)).await;
        assert_denied(&access, &req(bob(), true, "GetObject", &path)).await;
    }

    #[tokio::test]
    async fn signed_user_without_grant_is_denied() {
        // Signature verification itself is s3s's auth-provider layer
        // (ConfigAuth) — out of scope here; the access-level counterpart
        // is a valid signed user holding no grant.
        let owner = uid();
        let storage = storage_with_bucket(&owner, vec![]).await;
        put_object(&storage, "a.txt", Some(&owner), &private(&owner)).await;
        let access = AclAccess::new(storage, identity());
        let path = S3Path::object("data", "a.txt");
        assert_denied(&access, &req(bob(), true, "GetObject", &path)).await;
    }

    #[tokio::test]
    async fn expected_bucket_owner_mismatch_denies() {
        let owner = uid();
        let storage = storage_with_bucket(&owner, vec![]).await;
        put_object(&storage, "a.txt", Some(&owner), &private(&owner)).await;
        let access = AclAccess::new(storage, identity());
        let path = S3Path::object("data", "a.txt");
        let mut r = req(owner.clone(), true, "GetObject", &path);
        let stranger = other();
        r.expected_bucket_owner = Some(stranger.as_str());
        assert_denied(&access, &r).await;
        r.expected_bucket_owner = Some(owner.as_str());
        assert_allowed(&access, &r).await;
    }

    #[tokio::test]
    async fn missing_object_tiers_pass_through_for_owner_and_reader() {
        // The deny-first three tiers (review B3): the bucket owner and
        // a bucket-READ grantee pass through (the handler's 404); anyone
        // else gets 403 — no existence leak.
        let owner = uid();
        let storage = storage_with_bucket(&owner, vec![canonical(&bob(), Permission::Read)]).await;
        let access = AclAccess::new(storage, identity());
        let path = S3Path::object("data", "ghost");
        assert_allowed(&access, &req(owner.clone(), true, "GetObject", &path)).await;
        assert_allowed(&access, &req(bob(), true, "GetObject", &path)).await;
        assert_denied(&access, &req(anon(), false, "GetObject", &path)).await;
    }

    #[tokio::test]
    async fn missing_bucket_passes_through_for_any_principal() {
        // The B3 tiers prove owner/grant over the bucket row; a genuinely
        // missing bucket has none, so the access check passes through and
        // the handler answers NoSuchBucket — AWS + contract FR-005, for
        // every principal (there is no owner to prove).
        let storage = MemoryStorage::new().unwrap();
        let access = AclAccess::new(Arc::new(storage), identity());
        let path = S3Path::bucket("ghost");
        assert_allowed(&access, &req(uid(), true, "ListObjectsV2", &path)).await;
        assert_allowed(&access, &req(anon(), false, "ListObjectsV2", &path)).await;
        // DeleteBucket's owner-only gate on a missing bucket passes
        // through too (the handler enforces existence → NoSuchBucket).
        assert_allowed(&access, &req(uid(), true, "DeleteBucket", &path)).await;
    }

    #[tokio::test]
    async fn overwrite_needs_object_or_bucket_owner() {
        // Review B1: in a public-write bucket, an anonymous PUT onto an
        // existing key is 403 (overwrite = delete parity); onto a
        // missing key the bucket-WRITE grant suffices.
        let owner = uid();
        let storage =
            storage_with_bucket(&owner, vec![group(GROUP_ALL_USERS, Permission::Write)]).await;
        put_object(&storage, "a.txt", Some(&owner), &private(&owner)).await;
        let access = AclAccess::new(storage, identity());
        assert_denied(
            &access,
            &req(anon(), false, "PutObject", &S3Path::object("data", "a.txt")),
        )
        .await;
        assert_allowed(
            &access,
            &req(
                anon(),
                false,
                "PutObject",
                &S3Path::object("data", "new.txt"),
            ),
        )
        .await;
    }

    #[tokio::test]
    async fn copy_object_overwrite_checks_destination_owner() {
        // Review B1: O is the DESTINATION object/bucket — an anonymous
        // copy OVER an existing destination is 403 (source READ and
        // destination WRITE grants notwithstanding); onto a missing key
        // the source READ + destination bucket WRITE suffice.
        let owner = uid();
        let storage =
            storage_with_bucket(&owner, vec![group(GROUP_ALL_USERS, Permission::Write)]).await;
        let src_acl = Acl {
            owner: Some(owner.clone()),
            grants: vec![group(GROUP_ALL_USERS, Permission::Read)],
        };
        put_object(&storage, "src.txt", Some(&owner), &src_acl).await;
        put_object(&storage, "dst.txt", Some(&owner), &private(&owner)).await;
        let access = AclAccess::new(storage, identity());
        let dst = S3Path::object("data", "dst.txt");
        let mut r = req(anon(), false, "CopyObject", &dst);
        r.copy_source = Some("/data/src.txt");
        assert_denied(&access, &r).await;
        let dst2 = S3Path::object("data", "dst2.txt");
        let mut r = req(anon(), false, "CopyObject", &dst2);
        r.copy_source = Some("/data/src.txt");
        assert_allowed(&access, &r).await;
    }

    #[tokio::test]
    async fn rename_object_overwrite_checks_destination_owner() {
        // Review B1: rename source parity (the anonymous source owner)
        // + destination bucket WRITE pass, but an existing destination
        // still needs destination owner parity.
        let owner = uid();
        let storage =
            storage_with_bucket(&owner, vec![group(GROUP_ALL_USERS, Permission::Write)]).await;
        put_object(&storage, "src.txt", Some(&anon()), &private(&anon())).await;
        put_object(&storage, "dst.txt", Some(&owner), &private(&owner)).await;
        let access = AclAccess::new(storage, identity());
        let dst = S3Path::object("data", "dst.txt");
        let mut r = req(anon(), false, "RenameObject", &dst);
        r.rename_source = Some("/data/src.txt");
        assert_denied(&access, &r).await;
        let dst2 = S3Path::object("data", "dst2.txt");
        let mut r = req(anon(), false, "RenameObject", &dst2);
        r.rename_source = Some("/data/src.txt");
        assert_allowed(&access, &r).await;
    }

    #[tokio::test]
    async fn upload_part_copy_requires_source_read_and_initiator() {
        // UploadPartCopy: the upload creator gate (from the uploadId
        // row) plus source READ (from the source ACL) — compound row.
        let owner = uid();
        let storage = storage_with_bucket(&owner, vec![]).await;
        put_object(&storage, "secret.txt", Some(&owner), &private(&owner)).await;
        let src_acl = Acl {
            owner: Some(owner.clone()),
            grants: vec![group(GROUP_ALL_USERS, Permission::Read)],
        };
        put_object(&storage, "src.txt", Some(&owner), &src_acl).await;
        let upload_id = upload(&storage, "dst.txt", Some(&alice())).await;
        let query = format!("uploadId={upload_id}");
        let access = AclAccess::new(storage, identity());
        let path = S3Path::object("data", "dst.txt");
        // Creator, private source → 403 (source READ leg).
        let mut r = req(alice(), true, "UploadPartCopy", &path);
        r.query = &query;
        r.copy_source = Some("/data/secret.txt");
        assert_denied(&access, &r).await;
        // Creator + source READ (AllUsers) → allowed.
        r.copy_source = Some("/data/src.txt");
        assert_allowed(&access, &r).await;
        // Non-initiator with the source grant → 403 (upload-row leg).
        let mut r = req(bob(), true, "UploadPartCopy", &path);
        r.query = &query;
        r.copy_source = Some("/data/src.txt");
        assert_denied(&access, &r).await;
    }

    #[tokio::test]
    async fn upload_scoped_ops_resolve_upload_row_via_upload_id() {
        // ListParts/UploadPart/Abort read the upload creator from the
        // uploadId-resolved row, not the S3Path object — the object at
        // the key is owned by BOB, so an object-row gate would allow BOB
        // and deny ALICE; the upload row does the reverse.
        let owner = uid();
        let storage = storage_with_bucket(&owner, vec![]).await;
        put_object(&storage, "dst.txt", Some(&bob()), &private(&bob())).await;
        let upload_id = upload(&storage, "dst.txt", Some(&alice())).await;
        let query = format!("uploadId={upload_id}");
        let access = AclAccess::new(storage, identity());
        let path = S3Path::object("data", "dst.txt");
        for op in ["ListParts", "UploadPart", "AbortMultipartUpload"] {
            let mut r = req(alice(), true, op, &path);
            r.query = &query;
            assert_allowed(&access, &r).await;
            let mut r = req(bob(), true, op, &path);
            r.query = &query;
            assert_denied(&access, &r).await;
        }
        // Complete applies the B1 destination head on top of the upload
        // gate: the creator is not the existing destination's owner.
        let mut r = req(alice(), true, "CompleteMultipartUpload", &path);
        r.query = &query;
        assert_denied(&access, &r).await;
    }

    #[tokio::test]
    async fn delete_objects_coarse_gate_owner_or_any_grantee() {
        // Review B2: the coarse gate is the bucket owner OR any
        // bucket-ACL grant to P — a READ-only grantee passes (the
        // per-key checks are the handler's); no grant at all → whole-op
        // 403.
        let owner = uid();
        let storage =
            storage_with_bucket(&owner, vec![canonical(&alice(), Permission::Read)]).await;
        let access = AclAccess::new(storage, identity());
        let path = S3Path::bucket("data");
        assert_allowed(&access, &req(owner.clone(), true, "DeleteObjects", &path)).await;
        assert_allowed(&access, &req(alice(), true, "DeleteObjects", &path)).await;
        assert_denied(&access, &req(bob(), true, "DeleteObjects", &path)).await;
        assert_denied(&access, &req(anon(), false, "DeleteObjects", &path)).await;
    }

    #[tokio::test]
    async fn unmapped_op_is_owner_only() {
        // Default B: a resolved-but-unmapped op (GetBucketPolicy has no
        // §4 row) is owner-only — a signed non-owner gets 403.
        let owner = uid();
        let storage = storage_with_bucket(&owner, vec![]).await;
        let access = AclAccess::new(storage, identity());
        let path = S3Path::bucket("data");
        assert_allowed(&access, &req(owner.clone(), true, "GetBucketPolicy", &path)).await;
        assert_denied(&access, &req(alice(), true, "GetBucketPolicy", &path)).await;
    }

    #[tokio::test]
    async fn authenticated_users_group_matches_any_signed_user() {
        let owner = uid();
        let storage = storage_with_bucket(&owner, vec![]).await;
        let acl = Acl {
            owner: Some(owner.clone()),
            grants: vec![group(GROUP_AUTHENTICATED_USERS, Permission::Read)],
        };
        put_object(&storage, "a.txt", Some(&owner), &acl).await;
        let access = AclAccess::new(storage, identity());
        let path = S3Path::object("data", "a.txt");
        assert_allowed(&access, &req(alice(), true, "GetObject", &path)).await;
        assert_denied(&access, &req(anon(), false, "GetObject", &path)).await;
    }

    #[tokio::test]
    async fn root_ops_require_authentication() {
        // ListBuckets / CreateBucket: the Authenticated requirement — any
        // signed principal passes, anonymous 403 (a bucket needs a
        // creator owner; no resource to grant against).
        let storage = Arc::new(MemoryStorage::new().unwrap());
        let access = AclAccess::new(storage, identity());
        let root = S3Path::root();
        assert_denied(&access, &req(anon(), false, "ListBuckets", &root)).await;
        assert_allowed(&access, &req(alice(), true, "ListBuckets", &root)).await;
        let bucket = S3Path::bucket("data");
        assert_denied(&access, &req(anon(), false, "CreateBucket", &bucket)).await;
        assert_allowed(&access, &req(alice(), true, "CreateBucket", &bucket)).await;
    }

    #[tokio::test]
    async fn lazy_default_owner_gets_owner_bypass() {
        // A row with no owner element (the empty wire, review B4)
        // resolves to the identity's default owner — that principal
        // passes the owner-parity gates.
        let storage = Arc::new(MemoryStorage::new().unwrap());
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        let access = AclAccess::new(storage, identity());
        let path = S3Path::bucket("data");
        assert_allowed(
            &access,
            &req(default_owner_id(), true, "GetBucketLocation", &path),
        )
        .await;
        assert_denied(&access, &req(alice(), true, "GetBucketLocation", &path)).await;
    }

    #[tokio::test]
    async fn create_multipart_upload_denied_without_bucket_write() {
        // CMU resolves to an object path (POST /bucket/key?uploads) but
        // is NOT upload-scoped and carries no existence dispatch — the
        // base bucket-WRITE gate applies (regression: the object-path
        // BucketWrite branch once denied every CMU).
        let owner = uid();
        let storage = storage_with_bucket(&owner, vec![]).await;
        let access = AclAccess::new(storage, identity());
        let path = S3Path::object("data", "big.txt");
        assert_denied(&access, &req(anon(), false, "CreateMultipartUpload", &path)).await;
        assert_denied(&access, &req(bob(), true, "CreateMultipartUpload", &path)).await;
        assert_allowed(
            &access,
            &req(owner.clone(), true, "CreateMultipartUpload", &path),
        )
        .await;
    }

    #[tokio::test]
    async fn create_multipart_upload_write_grantee_allowed() {
        // CMU on a public-write bucket: the anonymous WRITE grantee is
        // allowed (no existence head — there is no destination key yet).
        let owner = uid();
        let storage =
            storage_with_bucket(&owner, vec![group(GROUP_ALL_USERS, Permission::Write)]).await;
        let access = AclAccess::new(storage, identity());
        let path = S3Path::object("data", "big.txt");
        assert_allowed(&access, &req(anon(), false, "CreateMultipartUpload", &path)).await;
    }

    #[tokio::test]
    async fn post_object_runs_the_base_only() {
        // s3s routes POST to S3Path::Bucket — the form key is
        // unavailable pre-deserialization, so the B1 destination head is
        // not enforceable at the access layer; PostObject evaluates the
        // base (bucket WRITE) only.
        let owner = uid();
        let storage =
            storage_with_bucket(&owner, vec![group(GROUP_ALL_USERS, Permission::Write)]).await;
        let access = AclAccess::new(storage, identity());
        let path = S3Path::bucket("data");
        assert_allowed(&access, &req(anon(), false, "PostObject", &path)).await;
    }

    #[test]
    fn delete_objects_rule_is_the_owner_only_default() {
        // Task 8 carry: the coarse gate (bucket owner or any bucket-ACL
        // grant) is a prose bullet the 10-requirement matrix cannot
        // express, so `DeleteObjects` resolves to the default-B rule —
        // the gate in `evaluate` must special-case the op BEFORE
        // `rule_for`.
        assert_eq!(
            rule_for("DeleteObjects"),
            OpRule {
                base: Requirement::OwnerOnly,
                reads_source: false,
                writes_destination_bucket: false,
                existence_dispatch: false,
            }
        );
    }
}
