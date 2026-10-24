//! The governance-bypass authority (design 2026-10-20-s3-object-lock,
//! OL15): the request header, the operations that consult the authority,
//! the authority rule over a resolved ACL, and the extension bit the
//! authorization pipeline hands to the handler.

use http::HeaderMap;

use crate::{
    _core::acl::{Acl, OwnerId, Permission, can_delete},
    access::grantee_matches,
};

/// The governance-bypass authority the pipeline resolved for a request
/// (design 2026-10-20-s3-object-lock, OL15), carried to the handler in the
/// s3s request extensions.
///
/// It is the authority only: the handler still requires the request's
/// `x-amz-bypass-governance-retention` header and passes
/// `Bypass::Governance` only when both are true. No bit is no authority.
///
/// # Examples
///
/// ```rust
/// use tinio_auth::MayBypass;
///
/// assert!(MayBypass::new(true).granted());
/// assert!(!MayBypass::new(false).granted());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MayBypass(bool);

impl MayBypass {
    /// Wrap a resolved authority bit.
    pub fn new(granted: bool) -> Self {
        Self(granted)
    }

    /// Whether the caller may pass `Bypass::Governance` to the storage.
    pub fn granted(self) -> bool {
        self.0
    }
}

/// OL15's authority rule over one resolved ACL: the owner of the addressed
/// version, a `FULL_CONTROL` grantee of that ACL, or the bucket owner.
///
/// `acl` is the addressed version's ACL (`version_acl`) for an addressed
/// request and the current object's otherwise. `lazy_default` resolves an
/// empty owner element, exactly as [`can_delete`] does.
///
/// # Examples
///
/// ```rust
/// use tinio_auth::may_bypass_authority;
/// use tinio_core::acl::{Acl, OwnerId, Permission};
///
/// let owner =
///     OwnerId::new("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899").unwrap();
/// let acl = Acl::default_private(Some(owner.clone()));
/// // The owner and the bucket owner may bypass; a stranger may not.
/// assert!(may_bypass_authority(&owner, true, &acl, &owner, &owner));
/// assert!(!may_bypass_authority(
///     &OwnerId::new("ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100",).unwrap(),
///     true,
///     &acl,
///     &owner,
///     &owner,
/// ));
///
/// // A FULL_CONTROL grantee of the addressed version may bypass too.
/// let granted =
///     OwnerId::new("00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff").unwrap();
/// let acl = Acl {
///     owner: Some(owner.clone()),
///     grants: vec![tinio_core::acl::Grant {
///         grantee: tinio_core::acl::Grantee::Canonical(granted.clone()),
///         permission: Permission::FullControl,
///     }],
/// };
/// assert!(may_bypass_authority(&granted, true, &acl, &owner, &owner));
/// ```
pub fn may_bypass_authority(
    principal: &OwnerId,
    authenticated: bool,
    acl: &Acl,
    bucket_owner: &OwnerId,
    lazy_default: &OwnerId,
) -> bool {
    can_delete(principal, bucket_owner, lazy_default, acl.owner.as_ref())
        || acl.grants.iter().any(|grant| {
            grantee_matches(&grant.grantee, principal, authenticated)
                && grant.permission == Permission::FullControl
        })
}

/// The operations whose storage effect consults the governance bypass and
/// whose authority this layer resolves. `DeleteObjects` is per entry: the
/// batch body is unavailable before deserialization, so the handler
/// resolves each entry's addressed version itself.
pub(crate) fn is_bypass_authority_op(op: &str) -> bool {
    matches!(op, "DeleteObject" | "PutObjectRetention")
}

/// Whether the raw header asks for the governance bypass. s3s's bool parse
/// accepts only `true`/`True` (anything else is the framework's `400`
/// before this layer runs), so a present `false` is not a request.
pub(crate) fn bypass_requested(headers: &HeaderMap) -> bool {
    matches!(
        headers
            .get("x-amz-bypass-governance-retention")
            .map(|value| value.as_bytes()),
        Some(b"true" | b"True")
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use http::{Extensions, HeaderMap, HeaderValue};
    use s3s::path::S3Path;
    use tinio_mem::MemoryStorage;

    use super::{MayBypass, bypass_requested, is_bypass_authority_op, may_bypass_authority};
    use crate::{
        _core::{
            acl::{Acl, Permission},
            bucket, object,
            storage::VersionOps,
        },
        access::{
            AclAccess, Request,
            tests::{
                acl_of, alice, anon, bob, canonical, identity, put_version, req, uid,
                versioned_storage,
            },
        },
    };

    /// Evaluate one request and answer the decision plus the authority bit
    /// the pipeline inserted into the request extensions.
    async fn decision(
        access: &AclAccess<MemoryStorage>,
        request: &Request,
    ) -> (bool, Option<MayBypass>) {
        let mut extensions = Extensions::new();
        let allowed = access.evaluate(request, &mut extensions).await.is_ok();
        (allowed, extensions.get::<MayBypass>().copied())
    }

    #[tokio::test]
    async fn only_the_addressed_versions_authority_carries_the_bit() {
        // The two-version fixture: ALICE owns the older version, BOB the
        // current one. R23 makes the base gate follow the address, so the
        // current owner is refused an addressed request (and carries no
        // bit); the unaddressed request still resolves the current object.
        let uid = uid();
        let storage = versioned_storage(&uid).await;
        let older = put_version(&storage, "k", &alice(), &acl_of(&alice())).await;
        put_version(&storage, "k", &bob(), &acl_of(&bob())).await;
        let access = AclAccess::new(Arc::clone(&storage), identity());
        let path = S3Path::object("data", "k");
        let query = format!("versionId={older}");

        for op in ["DeleteObject", "PutObjectRetention"] {
            let mut addressed = req(bob(), true, op, &path);
            addressed.query = query.clone();
            addressed.bypass_requested = true;
            let (allowed, bit) = decision(&access, &addressed).await;
            assert!(
                !allowed,
                "{op}: the current owner is not the addressed version's owner"
            );
            assert_eq!(bit, None, "{op}: a refused request carries no bit");
        }

        // The addressed version's owner passes and carries her authority.
        for op in ["DeleteObject", "PutObjectRetention"] {
            let mut addressed = req(alice(), true, op, &path);
            addressed.query = query.clone();
            addressed.bypass_requested = true;
            let (allowed, bit) = decision(&access, &addressed).await;
            assert!(allowed, "{op}: the addressed owner passes the gate");
            assert_eq!(bit, Some(MayBypass::new(true)), "{op}");
        }

        // An unaddressed request resolves the CURRENT object's ACL (BOB's).
        let mut current = req(bob(), true, "DeleteObject", &path);
        current.bypass_requested = true;
        let (allowed, bit) = decision(&access, &current).await;
        assert!(allowed, "the current owner passes the delete gate");
        assert_eq!(bit, Some(MayBypass::new(true)));

        // The bucket owner has parity over any addressed version.
        let mut bucket_owner = req(uid.clone(), true, "DeleteObject", &path);
        bucket_owner.query = query.clone();
        bucket_owner.bypass_requested = true;
        let (allowed, bit) = decision(&access, &bucket_owner).await;
        assert!(allowed, "the bucket owner passes the delete gate");
        assert_eq!(bit, Some(MayBypass::new(true)));

        // The authority function over the resolved addressed ACL: ALICE
        // owns it, BOB does not — the one rule the pipeline applies.
        let b = bucket::name("data").unwrap();
        let k = object::key("k").unwrap();
        let addressed_acl = storage.version_acl(&b, &k, Some(&older)).await.unwrap();
        let id = identity();
        assert!(may_bypass_authority(
            &alice(),
            true,
            &addressed_acl,
            &uid,
            &id.default_owner
        ));
        assert!(!may_bypass_authority(
            &bob(),
            true,
            &addressed_acl,
            &uid,
            &id.default_owner
        ));
    }

    #[tokio::test]
    async fn a_full_control_grant_on_the_addressed_version_does_not_satisfy_the_gate() {
        // The addressed version's ACL grants BOB FULL_CONTROL (the owner
        // stays ALICE); the current version is BOB's. The base gate is the
        // owner-parity class — grants never satisfy it (the object-tagging
        // precedent) — so R23 refuses BOB despite the grant and the current
        // ownership. The authority function alone would grant him the bit;
        // that is the layering, not the pipeline's answer.
        let uid = uid();
        let storage = versioned_storage(&uid).await;
        let granted = Acl {
            owner: Some(alice()),
            grants: vec![canonical(&bob(), Permission::FullControl)],
        };
        let older = put_version(&storage, "k", &alice(), &granted).await;
        put_version(&storage, "k", &bob(), &acl_of(&bob())).await;
        let access = AclAccess::new(Arc::clone(&storage), identity());
        let path = S3Path::object("data", "k");
        let mut addressed = req(bob(), true, "DeleteObject", &path);
        addressed.query = format!("versionId={older}");
        addressed.bypass_requested = true;
        let (allowed, bit) = decision(&access, &addressed).await;
        assert!(!allowed, "a grant does not satisfy the owner-parity gate");
        assert_eq!(bit, None);

        // The resolved addressed ACL is what the authority rule reads.
        let b = bucket::name("data").unwrap();
        let k = object::key("k").unwrap();
        let addressed_acl = storage.version_acl(&b, &k, Some(&older)).await.unwrap();
        assert!(may_bypass_authority(
            &bob(),
            true,
            &addressed_acl,
            &uid,
            &identity().default_owner
        ));
    }

    #[tokio::test]
    async fn the_bit_requires_the_header() {
        // Without the header there is nothing to authorize: the bit is
        // present and false, never an authority of its own. The caller is
        // the addressed version's owner, so the gate admits her.
        let uid = uid();
        let storage = versioned_storage(&uid).await;
        let older = put_version(&storage, "k", &alice(), &acl_of(&alice())).await;
        put_version(&storage, "k", &bob(), &acl_of(&bob())).await;
        let access = AclAccess::new(storage, identity());
        let path = S3Path::object("data", "k");
        let mut addressed = req(alice(), true, "DeleteObject", &path);
        addressed.query = format!("versionId={older}");
        let (allowed, bit) = decision(&access, &addressed).await;
        assert!(allowed);
        assert_eq!(bit, Some(MayBypass::new(false)));
    }

    #[tokio::test]
    async fn a_denied_request_never_carries_the_bit() {
        // Authorization runs first: a caller the operation gate refuses
        // never reaches the authority resolution, so no bit is inserted.
        let uid = uid();
        let storage = versioned_storage(&uid).await;
        let older = put_version(&storage, "k", &alice(), &acl_of(&alice())).await;
        put_version(&storage, "k", &bob(), &acl_of(&bob())).await;
        let access = AclAccess::new(storage, identity());
        let path = S3Path::object("data", "k");
        let mut addressed = req(anon(), false, "DeleteObject", &path);
        addressed.query = format!("versionId={older}");
        addressed.bypass_requested = true;
        let (allowed, bit) = decision(&access, &addressed).await;
        assert!(!allowed, "the anonymous caller is refused");
        assert_eq!(bit, None, "no authority is resolved for a refused request");
    }

    #[tokio::test]
    async fn a_malformed_version_address_carries_no_authority() {
        // The bucket owner passes the gate on parity and reaches the
        // handler's answer for the malformed address; the authority path
        // must not fall back to the current object's ACL on the way.
        let uid = uid();
        let storage = versioned_storage(&uid).await;
        put_version(&storage, "k", &bob(), &acl_of(&bob())).await;
        let access = AclAccess::new(storage, identity());
        let path = S3Path::object("data", "k");
        let mut addressed = req(uid.clone(), true, "DeleteObject", &path);
        addressed.query = "versionId=".to_owned();
        addressed.bypass_requested = true;
        let (allowed, bit) = decision(&access, &addressed).await;
        assert!(allowed, "the bucket owner passes the operation gate");
        assert_eq!(bit, Some(MayBypass::new(false)), "fail closed");
    }

    fn header(value: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-bypass-governance-retention",
            HeaderValue::from_static(value),
        );
        headers
    }

    #[test]
    fn bypass_requested_accepts_only_the_true_spellings() {
        // s3s's bool parse admits exactly `true`/`True`; the header adaptor
        // must read the same set and nothing else (an absent header and a
        // present `false` are both "not asked").
        assert!(bypass_requested(&header("true")));
        assert!(bypass_requested(&header("True")));
        assert!(!bypass_requested(&header("false")));
        assert!(!bypass_requested(&header("False")));
        assert!(!bypass_requested(&HeaderMap::new()));
        assert!(!bypass_requested(&header("garbage")));
    }

    #[test]
    fn bypass_authority_ops_are_the_delete_and_retention_writes() {
        // OL15's authority rides the two operations whose storage effect
        // consults the bypass; the legal-hold write and the batch delete
        // resolve their own (the batch per entry).
        assert!(is_bypass_authority_op("DeleteObject"));
        assert!(is_bypass_authority_op("PutObjectRetention"));
        assert!(!is_bypass_authority_op("PutObjectLegalHold"));
        assert!(!is_bypass_authority_op("DeleteObjects"));
        assert!(!is_bypass_authority_op("GetObjectRetention"));
    }
}
