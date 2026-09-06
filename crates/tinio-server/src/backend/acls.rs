//! ACL wire helpers of the four ACL ops (spec 2026-09-05, Task 10) and
//! the write-path headers (Task 11): the shared Content-MD5 gate, the
//! dto grants ↔ core `AclGrants` conversions, and the write-path
//! `x-amz-acl`/`x-amz-grant-*` expansion.
//!
//! The request-input path is strict: an unknown group URI, a malformed
//! canonical ID, an email grantee, a permission outside the five AWS
//! strings, and a grant set over the per-ACL cap all answer 400
//! `InvalidArgument`. The codec's self-heal (`Acl::from_grants_wire`)
//! applies to stored-row decode only, never to request input.

use base64::{Engine, engine::general_purpose::STANDARD};
use s3s::{S3Result, dto, s3_error};

use crate::{
    _auth::canned::{
        GrantHeaders, canned_bucket_grants, canned_object_grants, expand_acl, grants_from_headers,
    },
    _core::acl::{
        ACL_GRANTS_MAX, Acl, AclGrants, Grant, Grantee, GroupUri, OwnerId, Permission,
    },
};

/// Content-MD5 on the put-ACL ops (spec §5, review A7): required, AWS
/// three-state behavior — missing → 400 `InvalidRequest` with the AWS
/// message, malformed (not a well-formed 16-byte base64 digest) → 400
/// `InvalidDigest`. Digest equality against the XML body is not
/// verifiable here — s3s's deserializer consumes the body and
/// `S3Request` retains no raw bytes (recorded deviation; AWS answers
/// `BadDigest` for a mismatch, unreachable). Shared home for the CORS
/// merge (2026-09-05-s3-cors-design.md owns the same helper).
pub(crate) fn require_content_md5(md5: Option<&str>) -> S3Result<()> {
    let md5 = md5.ok_or_else(|| {
        s3_error!(
            InvalidRequest,
            "Missing required header for this request: Content-MD5"
        )
    })?;
    let raw = STANDARD
        .decode(md5)
        .map_err(|_| s3_error!(InvalidDigest, "The Content-MD5 you specified is not valid"))?;
    if raw.len() == 16 {
        Ok(())
    } else {
        Err(s3_error!(
            InvalidDigest,
            "The Content-MD5 you specified is not valid"
        ))
    }
}

/// The fail-closed policy-Owner check (spec Decisions): an
/// `AccessControlPolicy` body whose `Owner` does not match the row
/// owner (lazy resolved — review B4) answers 400 `InvalidArgument`; a
/// missing or malformed body Owner is equally invalid — never
/// self-healed.
pub(crate) fn policy_owner_matches(policy_owner: Option<&dto::Owner>, row_owner: &OwnerId) -> S3Result<()> {
    let Some(policy_owner) = policy_owner else {
        return Err(s3_error!(
            InvalidArgument,
            "the ACL policy body is missing its Owner"
        ));
    };
    let Some(id) = policy_owner.id.as_deref() else {
        return Err(s3_error!(
            InvalidArgument,
            "the ACL policy body Owner is missing its ID"
        ));
    };
    let id = OwnerId::new(id)
        .map_err(|_| s3_error!(InvalidArgument, "invalid canonical ID in ACL policy body: {id}"))?;
    if id == *row_owner {
        Ok(())
    } else {
        Err(s3_error!(
            InvalidArgument,
            "the ACL policy body Owner does not match the row owner"
        ))
    }
}

/// The single-key delete rule (spec 2026-09-05, review B2): P ==
/// O(object) or P == O(bucket) — the access layer's `ObjectOrBucketOwner`
/// gate for DeleteObject, mirrored for the handler-side per-key check of
/// DeleteObjects (the op-level coarse gate, owner-or-any-grantee, runs
/// separately — grants are irrelevant to this rule). Both owners arrive
/// RESOLVED (an empty owner wire's lazy default, review B4 — callers
/// resolve via [`S3Backend::row_owner`] before calling).
pub(crate) fn can_delete(
    principal: &OwnerId,
    bucket_owner: &OwnerId,
    object_owner: &OwnerId,
) -> bool {
    principal == object_owner || principal == bucket_owner
}

/// Exactly one ACL grant source per request (AWS's put-ACL source rule:
/// the canned ACL, the `x-amz-grant-*` header set, or the policy body
/// are mutually exclusive; none is equally invalid).
fn single_source(
    canned: Option<&str>,
    headers: &GrantHeaders<'_>,
    policy: Option<&[dto::Grant]>,
) -> S3Result<()> {
    let sources = canned.is_some() as u8 + headers_requested(headers) as u8 + policy.is_some() as u8;
    match sources {
        0 => Err(s3_error!(
            InvalidArgument,
            "a canned ACL, x-amz-grant-* headers or a policy body is required"
        )),
        1 => Ok(()),
        _ => Err(s3_error!(
            InvalidArgument,
            "a canned ACL, x-amz-grant-* headers and a policy body are mutually exclusive"
        )),
    }
}

fn headers_requested(headers: &GrantHeaders<'_>) -> bool {
    headers.full_control.is_some()
        || headers.read.is_some()
        || headers.read_acp.is_some()
        || headers.write.is_some()
        || headers.write_acp.is_some()
}

/// The grant set of a `PutBucketAcl` request: exactly one of the canned
/// ACL (bucket expansion — the `bucket-owner-*` names are private on
/// buckets), the grant headers, or the policy body. The strict
/// request-level validation applies to every source.
pub(crate) fn bucket_acl_grants(
    canned: Option<&str>,
    headers: &GrantHeaders<'_>,
    policy: Option<&[dto::Grant]>,
    owner: &OwnerId,
) -> S3Result<AclGrants> {
    single_source(canned, headers, policy)?;
    if let Some(canned) = canned {
        return canned_bucket_grants(owner, canned);
    }
    if headers_requested(headers) {
        return grants_from_headers(owner, headers);
    }
    grants_from_policy(policy.unwrap_or_default())
}

/// The grant set of a `PutObjectAcl` request — like
/// [`bucket_acl_grants`], with the object canned expansion (the
/// `bucket-owner-*` names additionally grant the bucket owner, review
/// A6).
pub(crate) fn object_acl_grants(
    canned: Option<&str>,
    headers: &GrantHeaders<'_>,
    policy: Option<&[dto::Grant]>,
    owner: &OwnerId,
    bucket_owner: &OwnerId,
) -> S3Result<AclGrants> {
    single_source(canned, headers, policy)?;
    if let Some(canned) = canned {
        return canned_object_grants(owner, bucket_owner, canned);
    }
    if headers_requested(headers) {
        return grants_from_headers(owner, headers);
    }
    grants_from_policy(policy.unwrap_or_default())
}

/// A policy-body grant list into core `AclGrants` — strict validation
/// mirroring the request-level header rules (unknown group URI,
/// malformed canonical ID, email grantees, unknown permission, the
/// 100-grant cap → 400 `InvalidArgument`); the codec's self-heal is
/// read-path only.
pub(crate) fn grants_from_policy(grants: &[dto::Grant]) -> S3Result<AclGrants> {
    let mut out = Vec::new();
    for dto_grant in grants {
        let permission = match dto_grant.permission.as_ref() {
            Some(p) => Permission::from_wire(p.as_str())
                .map_err(|_| s3_error!(InvalidArgument, "invalid ACL permission: {}", p.as_str()))?,
            None => {
                return Err(s3_error!(
                    InvalidArgument,
                    "an ACL grant is missing its permission"
                ));
            }
        };
        let grantee = dto_grant
            .grantee
            .as_ref()
            .ok_or_else(|| s3_error!(InvalidArgument, "an ACL grant is missing its grantee"))
            .and_then(grantee_from_dto)?;
        out.push(Grant { grantee, permission });
        if out.len() > ACL_GRANTS_MAX {
            return Err(s3_error!(
                InvalidArgument,
                "ACL grant set exceeds the maximum of {ACL_GRANTS_MAX} grants"
            ));
        }
    }
    Ok(out)
}

/// One policy-body grantee into the core grantee — strict (see
/// [`grants_from_policy`]): only the canonical-ID and the three
/// documented group-URI forms are admitted; a grantee carrying the
/// other address form alongside its type is domain-invalid.
fn grantee_from_dto(grantee: &dto::Grantee) -> S3Result<Grantee> {
    match grantee.type_.as_str() {
        dto::Type::CANONICAL_USER => {
            if grantee.email_address.is_some() || grantee.uri.is_some() {
                return Err(s3_error!(
                    InvalidArgument,
                    "a canonical-user grantee must carry an ID only"
                ));
            }
            let Some(id) = grantee.id.as_deref() else {
                return Err(s3_error!(
                    InvalidArgument,
                    "a canonical-user grantee is missing its ID"
                ));
            };
            OwnerId::new(id)
                .map(Grantee::Canonical)
                .map_err(|_| s3_error!(InvalidArgument, "invalid canonical ID in ACL grantee: {id}"))
        }
        dto::Type::GROUP => {
            if grantee.email_address.is_some() || grantee.id.is_some() {
                return Err(s3_error!(
                    InvalidArgument,
                    "a group grantee must carry a URI only"
                ));
            }
            let uri = GroupUri(
                grantee
                    .uri
                    .clone()
                    .ok_or_else(|| s3_error!(InvalidArgument, "a group grantee is missing its URI"))?,
            );
            if uri.is_valid() {
                Ok(Grantee::Group(uri))
            } else {
                Err(s3_error!(
                    InvalidArgument,
                    "invalid group URI in ACL grantee: {}",
                    uri.0
                ))
            }
        }
        _ => Err(s3_error!(
            InvalidArgument,
            "email grantees are not supported: {}",
            grantee.type_.as_str()
        )),
    }
}

/// One core grant into its dto form (the ACL-op responses; the grants
/// echo the row's canonical set — the wire codec canonicalized what the
/// writes stored).
pub(crate) fn grant_dto(grant: &Grant) -> dto::Grant {
    let grantee = match &grant.grantee {
        Grantee::Canonical(id) => dto::Grantee {
            type_: dto::Type::from_static(dto::Type::CANONICAL_USER),
            id: Some(id.as_str().to_string()),
            uri: None,
            display_name: None,
            email_address: None,
        },
        Grantee::Group(uri) => dto::Grantee {
            type_: dto::Type::from_static(dto::Type::GROUP),
            uri: Some(uri.0.clone()),
            id: None,
            display_name: None,
            email_address: None,
        },
    };
    dto::Grant {
        grantee: Some(grantee),
        permission: Some(dto::Permission::from_static(grant.permission.as_wire())),
    }
}

/// A core grant set into the dto grant list.
pub(crate) fn grants_dto(grants: &AclGrants) -> Vec<dto::Grant> {
    grants.iter().map(grant_dto).collect()
}

// --- write-path headers (spec §5, Task 11) ---

/// The `x-amz-grant-*` input fields of one write request into the
/// parsed header set (the five fields every write surface shares — only
/// the bucket surface carries `grant_write`).
pub(crate) fn grant_headers<'a>(
    full_control: Option<&'a str>,
    read: Option<&'a str>,
    read_acp: Option<&'a str>,
    write: Option<&'a str>,
    write_acp: Option<&'a str>,
) -> GrantHeaders<'a> {
    GrantHeaders {
        full_control,
        read,
        read_acp,
        write,
        write_acp,
    }
}

/// The write-path ACL of an OBJECT surface (PutObject, CopyObject,
/// CreateMultipartUpload): canned expansion or grant-header parse via
/// [`expand_acl`] — the shared object-flavored composer (the
/// `bucket-owner-*` names reference the bucket owner; canned + grant
/// headers → 400; strict request-level validation). A write with
/// neither header records the owner's private default — the write-path
/// rule, NOT the put-ACL ops' exactly-one-source 400. The returned row
/// is grants-only (`owner: None`); the caller passes the owner element
/// separately.
pub(crate) fn object_write_acl(
    owner: &OwnerId,
    bucket_owner: &OwnerId,
    canned: Option<&str>,
    headers: &GrantHeaders<'_>,
) -> S3Result<Acl> {
    expand_acl(owner, bucket_owner, canned, *headers)
}

/// The write-path ACL of a BUCKET surface (CreateBucket): like
/// [`object_write_acl`], but composed through [`canned_bucket_grants`]
/// DIRECTLY — [`expand_acl`] is object-flavored, and the
/// `bucket-owner-*` names are ignored on buckets (private, per AWS).
pub(crate) fn bucket_write_acl(
    owner: &OwnerId,
    canned: Option<&str>,
    headers: &GrantHeaders<'_>,
) -> S3Result<Acl> {
    let grants = match canned {
        Some(canned) => {
            if headers_requested(headers) {
                return Err(s3_error!(
                    InvalidArgument,
                    "cannot combine a canned ACL with x-amz-grant-* headers"
                ));
            }
            canned_bucket_grants(owner, canned)?
        }
        None => grants_from_headers(owner, headers)?,
    };
    Ok(Acl { owner: None, grants })
}
