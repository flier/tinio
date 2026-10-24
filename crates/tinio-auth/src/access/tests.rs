use std::{io, sync::Arc};

use bytes::Bytes;
use futures::stream;
use http::Extensions;
use s3s::path::S3Path;
use tinio_mem::MemoryStorage;
use tinio_util::testing::{metrics::counter, spans};

use super::{AclAccess, OpRule, Reason, Request, Requirement, owned_path, rule_for};
use crate::{
    _core::{
        acl::{
            Acl, GROUP_ALL_USERS, GROUP_AUTHENTICATED_USERS, Grant, Grantee, GroupUri, OwnerId,
            Permission, default_owner_id,
        },
        bucket, object,
        storage::{BucketOps, MultipartOps, ObjectOps, VersionOps},
        version,
    },
    derive_canonical_id,
    identity::{Identity, User},
};

// Fixture identities: two fixed canonical owners distinct from the
// configured users, and the anonymous principal.
pub(crate) fn uid() -> OwnerId {
    OwnerId::new("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899").unwrap()
}

fn other() -> OwnerId {
    OwnerId::new("ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100").unwrap()
}

pub(crate) fn anon() -> OwnerId {
    OwnerId::anonymous()
}

pub(crate) fn alice() -> OwnerId {
    derive_canonical_id("AKID")
}

pub(crate) fn bob() -> OwnerId {
    derive_canonical_id("BKID")
}

pub(crate) fn identity() -> Arc<Identity> {
    Arc::new(Identity::test(vec![
        User::test("AKID", "secret", "alice"),
        User::test("BKID", "secret", "bob"),
    ]))
}

pub(crate) fn canonical(id: &OwnerId, permission: Permission) -> Grant {
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
        .stage_body(
            &b,
            &k,
            Box::pin(stream::empty::<io::Result<Bytes>>()),
            None,
            None,
        )
        .await
        .unwrap();
    storage
        .commit_object(&b, &k, staged, object::Tags::empty(), owner, acl, None)
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
            None,
        )
        .await
        .unwrap()
        .upload_id
}

pub(crate) fn req(principal: OwnerId, authenticated: bool, op: &str, path: &S3Path) -> Request {
    Request {
        principal,
        authenticated,
        op: op.to_owned(),
        path: owned_path(path),
        query: String::new(),
        expected_bucket_owner: None,
        copy_source: None,
        rename_source: None,
        bypass_requested: false,
    }
}

async fn assert_allowed(access: &AclAccess<MemoryStorage>, request: &Request) {
    access
        .evaluate(request, &mut Extensions::new())
        .await
        .unwrap();
}

async fn assert_denied(access: &AclAccess<MemoryStorage>, request: &Request) {
    let err = access
        .evaluate(request, &mut Extensions::new())
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
    r.expected_bucket_owner = Some(stranger.as_str().to_owned());
    assert_denied(&access, &r).await;
    r.expected_bucket_owner = Some(owner.as_str().to_owned());
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
    r.copy_source = Some("/data/src.txt".to_owned());
    assert_denied(&access, &r).await;
    let dst2 = S3Path::object("data", "dst2.txt");
    let mut r = req(anon(), false, "CopyObject", &dst2);
    r.copy_source = Some("/data/src.txt".to_owned());
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
    r.rename_source = Some("/src.txt".to_owned());
    assert_denied(&access, &r).await;
    let dst2 = S3Path::object("data", "dst2.txt");
    let mut r = req(anon(), false, "RenameObject", &dst2);
    r.rename_source = Some("/src.txt".to_owned());
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
    r.query = query.clone();
    r.copy_source = Some("/data/secret.txt".to_owned());
    assert_denied(&access, &r).await;
    // Creator + source READ (AllUsers) → allowed.
    r.copy_source = Some("/data/src.txt".to_owned());
    assert_allowed(&access, &r).await;
    // Non-initiator with the source grant → 403 (upload-row leg).
    let mut r = req(bob(), true, "UploadPartCopy", &path);
    r.query = query.clone();
    r.copy_source = Some("/data/src.txt".to_owned());
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
        r.query = query.clone();
        assert_allowed(&access, &r).await;
        let mut r = req(bob(), true, op, &path);
        r.query = query.clone();
        assert_denied(&access, &r).await;
    }
    // Complete applies the B1 destination head on top of the upload
    // gate: the creator is not the existing destination's owner.
    let mut r = req(alice(), true, "CompleteMultipartUpload", &path);
    r.query = query.clone();
    assert_denied(&access, &r).await;
}

#[tokio::test]
async fn delete_objects_coarse_gate_owner_or_any_grantee() {
    // Review B2: the coarse gate is the bucket owner OR any
    // bucket-ACL grant to P — a READ-only grantee passes (the
    // per-key checks are the handler's); no grant at all → whole-op
    // 403.
    let owner = uid();
    let storage = storage_with_bucket(&owner, vec![canonical(&alice(), Permission::Read)]).await;
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
            version_addressed: false,
        }
    );
}

// --- the Object Lock authority (design 2026-10-20-s3-object-lock, OL15) ---

/// A private ACL owned by `owner`.
pub(crate) fn acl_of(owner: &OwnerId) -> Acl {
    Acl {
        owner: Some(owner.clone()),
        grants: vec![],
    }
}

/// A memory store whose `data` bucket is owned by `owner` and records
/// versions, so a key can hold an addressed history beside a different
/// current version.
pub(crate) async fn versioned_storage(owner: &OwnerId) -> Arc<MemoryStorage> {
    let storage = MemoryStorage::new().unwrap();
    let b = bucket::name("data").unwrap();
    storage
        .create_bucket(&b, Some(owner), &acl_of(owner))
        .await
        .unwrap();
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    Arc::new(storage)
}

/// One version of `key`, answering the identity the request would
/// address with `versionId`.
pub(crate) async fn put_version(
    storage: &MemoryStorage,
    key: &str,
    owner: &OwnerId,
    acl: &Acl,
) -> version::Id {
    let b = bucket::name("data").unwrap();
    let k = object::key(key).unwrap();
    let staged = storage
        .stage_body(
            &b,
            &k,
            Box::pin(stream::empty::<io::Result<Bytes>>()),
            None,
            None,
        )
        .await
        .unwrap();
    storage
        .commit_object(
            &b,
            &k,
            staged,
            object::Tags::empty(),
            Some(owner),
            acl,
            None,
        )
        .await
        .unwrap()
        .version_id
        .expect("a recording bucket gives every write an identity")
}

#[tokio::test]
async fn the_six_lock_operations_gate_on_their_base_requirement() {
    // The Object Lock matrix rows (spec §4): the bucket configuration is
    // the owner-only bucket-configuration family, and all four
    // per-version rows take the object-tagging owner parity. The owner
    // passes; a signed non-owner and an anonymous caller are denied.
    let owner = uid();
    let storage = storage_with_bucket(&owner, vec![]).await;
    put_object(&storage, "a.txt", Some(&owner), &private(&owner)).await;
    let access = AclAccess::new(storage, identity());
    let bucket_path = S3Path::bucket("data");
    let object_path = S3Path::object("data", "a.txt");
    for op in ["GetObjectLockConfiguration", "PutObjectLockConfiguration"] {
        assert_allowed(&access, &req(owner.clone(), true, op, &bucket_path)).await;
        assert_denied(&access, &req(bob(), true, op, &bucket_path)).await;
        assert_denied(&access, &req(anon(), false, op, &bucket_path)).await;
    }
    for op in [
        "GetObjectRetention",
        "GetObjectLegalHold",
        "PutObjectRetention",
        "PutObjectLegalHold",
    ] {
        assert_allowed(&access, &req(owner.clone(), true, op, &object_path)).await;
        assert_denied(&access, &req(bob(), true, op, &object_path)).await;
        assert_denied(&access, &req(anon(), false, op, &object_path)).await;
    }

    // The escalation guard: a bucket-WRITE grantee (even AllUsers) may
    // create keys but must not read or write the Object Lock
    // configuration — enabling it plus a default COMPLIANCE retention
    // would make every later object undeletable.
    let writable =
        storage_with_bucket(&uid(), vec![group(GROUP_ALL_USERS, Permission::Write)]).await;
    let writable_access = AclAccess::new(writable, identity());
    assert_allowed(
        &writable_access,
        &req(anon(), false, "PutObject", &S3Path::object("data", "new")),
    )
    .await;
    for op in ["GetObjectLockConfiguration", "PutObjectLockConfiguration"] {
        assert_denied(&writable_access, &req(anon(), false, op, &bucket_path)).await;
        assert_denied(&writable_access, &req(bob(), true, op, &bucket_path)).await;
    }

    // A bucket READ grantee may list but must not see the lock metadata
    // (the object-tagging sibling denies the same way).
    let readable = storage_with_bucket(&uid(), vec![canonical(&bob(), Permission::Read)]).await;
    put_object(&readable, "a.txt", Some(&uid()), &private(&uid())).await;
    let readable_access = AclAccess::new(readable, identity());
    assert_allowed(
        &readable_access,
        &req(bob(), true, "ListObjectsV2", &bucket_path),
    )
    .await;
    for op in ["GetObjectRetention", "GetObjectLegalHold"] {
        assert_denied(&readable_access, &req(bob(), true, op, &object_path)).await;
    }
}

#[tokio::test]
async fn a_denial_books_the_denied_decision() {
    let owner = uid();
    let storage = storage_with_bucket(&owner, vec![]).await;
    let access = AclAccess::new(storage, identity());
    let path = S3Path::object("data", "a.txt");
    let before = counter("tinio_auth_authorizations_total", &[("decision", "denied")]);
    assert_denied(&access, &req(bob(), true, "GetObject", &path)).await;
    let after = counter("tinio_auth_authorizations_total", &[("decision", "denied")]);
    assert!(
        after > before,
        "the denial must increment the family: {before} -> {after}"
    );
}

#[tokio::test]
async fn an_allowance_books_the_allowed_decision() {
    let owner = uid();
    let storage = storage_with_bucket(&owner, vec![]).await;
    let access = AclAccess::new(storage, identity());
    let path = S3Path::bucket("data");
    let before = counter(
        "tinio_auth_authorizations_total",
        &[("decision", "allowed")],
    );
    assert_allowed(
        &access,
        &req(owner.clone(), true, "GetBucketLocation", &path),
    )
    .await;
    let after = counter(
        "tinio_auth_authorizations_total",
        &[("decision", "allowed")],
    );
    assert!(
        after > before,
        "the allowance must increment the family: {before} -> {after}"
    );
}

/// Evaluate one request under the process-wide span recorder and return
/// whether it was allowed plus the `reason` its decision span carried.
/// The recorder filters to this thread, so parallel tests cannot supply
/// the span; the `op` and `decision` fields are asserted with it.
async fn recorded_decision(
    access: &AclAccess<MemoryStorage>,
    request: &Request,
) -> (bool, Option<String>) {
    spans::install();
    let capture = spans::capture();
    let before = capture.spans_on_this_thread().len();
    let allowed = access
        .evaluate(request, &mut Extensions::new())
        .await
        .is_ok();
    let recorded = capture.spans_on_this_thread();
    let span = recorded[before..]
        .iter()
        .find(|span| span.name == "evaluate")
        .unwrap_or_else(|| panic!("the decision span: {recorded:#?}"));
    assert_eq!(span.field("op"), Some(request.op.as_str()));
    assert_eq!(
        span.field("decision"),
        Some(if allowed { "allowed" } else { "denied" })
    );
    (allowed, span.field("reason").map(str::to_string))
}

/// Every reachable decision branch records its bounded reason on the
/// decision span: owner bypass, a grant, the authenticated-only root and
/// bucket branches, the missing-resource tiers, the upload creator, the
/// expected-owner mismatch, a malformed source header, a missing grant,
/// and a missing resource path.
#[tokio::test]
async fn each_decision_records_its_bounded_reason() {
    let owner = uid();
    let storage = storage_with_bucket(
        &owner,
        vec![
            canonical(&bob(), Permission::Read),
            group(GROUP_ALL_USERS, Permission::Read),
        ],
    )
    .await;
    put_object(&storage, "a.txt", Some(&owner), &private(&owner)).await;
    let access = AclAccess::new(Arc::clone(&storage), identity());

    let (allowed, reason) = recorded_decision(
        &access,
        &req(
            owner.clone(),
            true,
            "GetBucketLocation",
            &S3Path::bucket("data"),
        ),
    )
    .await;
    assert!(allowed, "the owner bypasses the grant gate");
    assert_eq!(reason.as_deref(), Some("owner"));

    let (allowed, reason) = recorded_decision(
        &access,
        &req(anon(), false, "ListObjectsV2", &S3Path::bucket("data")),
    )
    .await;
    assert!(allowed, "an AllUsers READ grant satisfies BucketRead");
    assert_eq!(reason.as_deref(), Some("grant"));

    let (allowed, reason) =
        recorded_decision(&access, &req(alice(), true, "ListBuckets", &S3Path::root())).await;
    assert!(allowed, "ListBuckets needs only a signed principal");
    assert_eq!(reason.as_deref(), Some("authenticated"));

    let (allowed, reason) = recorded_decision(
        &access,
        &req(bob(), true, "GetObject", &S3Path::object("data", "a.txt")),
    )
    .await;
    assert!(!allowed, "a signed non-owner holds no object grant");
    assert_eq!(reason.as_deref(), Some("no-grant"));

    let bucket_path = S3Path::bucket("data");
    let mut mismatch = req(owner.clone(), true, "GetBucketLocation", &bucket_path);
    let stranger = other();
    mismatch.expected_bucket_owner = Some(stranger.as_str().to_owned());
    let (allowed, reason) = recorded_decision(&access, &mismatch).await;
    assert!(!allowed, "an expected-owner mismatch denies");
    assert_eq!(reason.as_deref(), Some("owner-mismatch"));

    let (allowed, reason) = recorded_decision(
        &access,
        &req(
            anon(),
            false,
            "CopyObject",
            &S3Path::object("data", "dst.txt"),
        ),
    )
    .await;
    assert!(!allowed, "a missing copy source fails closed");
    assert_eq!(reason.as_deref(), Some("malformed-source"));

    let (allowed, reason) = recorded_decision(
        &access,
        &req(owner.clone(), true, "DeleteObjects", &S3Path::root()),
    )
    .await;
    assert!(!allowed, "the coarse gate needs a bucket path");
    assert_eq!(reason.as_deref(), Some("no-resource"));

    let upload_id = upload(&storage, "dst.txt", Some(&alice())).await;
    let query = format!("uploadId={upload_id}");
    let path = S3Path::object("data", "dst.txt");
    let mut creator = req(alice(), true, "ListParts", &path);
    creator.query = query.clone();
    let (allowed, reason) = recorded_decision(&access, &creator).await;
    assert!(allowed, "the upload's creator owns the upload row");
    assert_eq!(reason.as_deref(), Some("upload-creator"));

    let empty = Arc::new(MemoryStorage::new().unwrap());
    let empty_access = AclAccess::new(empty, identity());
    let (allowed, reason) = recorded_decision(
        &empty_access,
        &req(anon(), false, "ListObjectsV2", &S3Path::bucket("ghost")),
    )
    .await;
    assert!(allowed, "a genuinely missing bucket passes through");
    assert_eq!(reason.as_deref(), Some("missing"));
}

/// The reason vocabulary is closed and kebab-case: a new reason is a code
/// change, never a client-chosen series (`StorageError` is listed though
/// the mem backend cannot induce it, so the whole domain is pinned).
#[test]
fn every_reason_renders_its_bounded_label() {
    for (reason, label) in [
        (Reason::Owner, "owner"),
        (Reason::Grant, "grant"),
        (Reason::Authenticated, "authenticated"),
        (Reason::Missing, "missing"),
        (Reason::UploadCreator, "upload-creator"),
        (Reason::OwnerMismatch, "owner-mismatch"),
        (Reason::MalformedHeader, "malformed-header"),
        (Reason::MalformedSource, "malformed-source"),
        (Reason::MalformedVersion, "malformed-version"),
        (Reason::DeleteMarker, "delete-marker"),
        (Reason::NoResource, "no-resource"),
        (Reason::NoGrant, "no-grant"),
        (Reason::StorageError, "storage-error"),
    ] {
        assert_eq!(reason.to_string(), label);
    }
}

// --- R23: the version-addressed base gate ---

/// A versioned `data` bucket owned by `bucket_owner` holding two versions
/// of key `k`: the older owned by `older_owner`, the current by
/// `current_owner`. Answers the older version's identity.
async fn two_versions(
    bucket_owner: &OwnerId,
    older_owner: &OwnerId,
    current_owner: &OwnerId,
) -> (Arc<MemoryStorage>, version::Id) {
    let storage = versioned_storage(bucket_owner).await;
    let older = put_version(&storage, "k", older_owner, &acl_of(older_owner)).await;
    put_version(&storage, "k", current_owner, &acl_of(current_owner)).await;
    (storage, older)
}

#[tokio::test]
async fn the_addressed_versions_owner_passes_the_version_gate() {
    // ALICE owns the older version, BOB the current one. The base gate
    // of every version-addressed object operation authorizes the
    // ADDRESSED version, so ALICE passes despite not owning the current
    // object (R23).
    let uid = uid();
    let (storage, older) = two_versions(&uid, &alice(), &bob()).await;
    let access = AclAccess::new(storage, identity());
    let path = S3Path::object("data", "k");
    let query = format!("versionId={older}");
    for op in [
        "DeleteObject",
        "PutObjectRetention",
        "GetObjectRetention",
        "PutObjectLegalHold",
        "GetObjectLegalHold",
        "GetObjectTagging",
        "PutObjectTagging",
        "DeleteObjectTagging",
        "GetObjectAttributes",
    ] {
        let mut addressed = req(alice(), true, op, &path);
        addressed.query = query.clone();
        assert_allowed(&access, &addressed).await;
    }
}

#[tokio::test]
async fn the_current_owner_is_not_the_addressed_versions_owner() {
    // BOB owns the current object but not the older version ALICE owns:
    // the addressed gate denies him for every version-addressed
    // operation (pre-R23 the current-object ACL admitted him). An
    // unaddressed request still gates against the current object.
    let uid = uid();
    let (storage, older) = two_versions(&uid, &alice(), &bob()).await;
    let access = AclAccess::new(storage, identity());
    let path = S3Path::object("data", "k");
    let query = format!("versionId={older}");
    for op in [
        "DeleteObject",
        "PutObjectRetention",
        "GetObjectRetention",
        "PutObjectLegalHold",
        "GetObjectLegalHold",
        "GetObjectTagging",
        "PutObjectTagging",
        "DeleteObjectTagging",
        "GetObjectAttributes",
    ] {
        let mut addressed = req(bob(), true, op, &path);
        addressed.query = query.clone();
        assert_denied(&access, &addressed).await;
    }
    // The unaddressed request keeps the current object's ACL: BOB
    // passes, ALICE (the older version's owner) does not.
    assert_allowed(&access, &req(bob(), true, "DeleteObject", &path)).await;
    assert_denied(&access, &req(alice(), true, "DeleteObject", &path)).await;
}

#[tokio::test]
async fn a_non_owner_is_denied_on_the_addressed_version() {
    let uid = uid();
    let (storage, older) = two_versions(&uid, &alice(), &bob()).await;
    let access = AclAccess::new(storage, identity());
    let path = S3Path::object("data", "k");
    for op in [
        "DeleteObject",
        "PutObjectRetention",
        "GetObjectRetention",
        "PutObjectLegalHold",
        "GetObjectLegalHold",
        "GetObjectTagging",
        "PutObjectTagging",
        "DeleteObjectTagging",
        "GetObjectAttributes",
    ] {
        let mut addressed = req(anon(), false, op, &path);
        addressed.query = format!("versionId={older}");
        assert_denied(&access, &addressed).await;
    }
}

#[tokio::test]
async fn the_null_address_authorizes_the_null_version() {
    // The null version is written before versioning is enabled; the
    // literal `null` then names it — not a version id. ALICE owns it,
    // BOB owns the current version.
    let uid = uid();
    let storage = storage_with_bucket(&uid, vec![]).await;
    put_object(&storage, "k", Some(&alice()), &acl_of(&alice())).await;
    let b = bucket::name("data").unwrap();
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    put_version(&storage, "k", &bob(), &acl_of(&bob())).await;
    let access = AclAccess::new(storage, identity());
    let path = S3Path::object("data", "k");
    let mut null_address = req(alice(), true, "DeleteObject", &path);
    null_address.query = "versionId=null".to_owned();
    assert_allowed(&access, &null_address).await;
    let mut denied = req(bob(), true, "DeleteObject", &path);
    denied.query = "versionId=null".to_owned();
    assert_denied(&access, &denied).await;
    // The unaddressed request still reads the current (BOB's) object.
    assert_allowed(&access, &req(bob(), true, "DeleteObject", &path)).await;
}

#[tokio::test]
async fn a_well_formed_missing_version_passes_through_to_the_handler() {
    // Contract OL17: a version id that names no version is the handler's
    // `404 NoSuchVersion`, not a gate refusal. The pass-through is the
    // store's `NoSuchVersion` specifically; the caller owns neither the
    // current object nor the bucket, so no other tier admits her.
    let uid = uid();
    let (storage, _older) = two_versions(&uid, &alice(), &bob()).await;
    let access = AclAccess::new(storage, identity());
    let path = S3Path::object("data", "k");
    let missing = format!("versionId={}", version::id("missing-version").unwrap());
    for op in ["DeleteObject", "PutObjectRetention"] {
        let mut request = req(alice(), true, op, &path);
        request.query = missing.clone();
        assert_allowed(&access, &request).await;
    }
}

#[tokio::test]
async fn a_non_owner_cannot_delete_an_addressed_marker() {
    // A marker is reported as `NoSuchVersion` by `version_acl`, but it
    // is a real, addressable version: it must not ride the
    // missing-version pass-through. A marker carries no ACL, so the
    // content version's owner proves nothing at the marker's address;
    // the gate refuses her and the marker survives. Bucket-owner parity
    // still short-circuits before the address read.
    let uid = uid();
    let storage = versioned_storage(&uid).await;
    put_version(&storage, "k", &alice(), &acl_of(&alice())).await;
    let b = bucket::name("data").unwrap();
    let k = object::key("k").unwrap();
    let marker = storage.delete_object(&b, &k).await.unwrap();
    let marker_id = marker
        .id
        .clone()
        .expect("a recorded marker has an identity");
    let access = AclAccess::new(Arc::clone(&storage), identity());
    let path = S3Path::object("data", "k");
    let mut addressed = req(alice(), true, "DeleteObject", &path);
    addressed.query = format!("versionId={marker_id}");
    assert_denied(&access, &addressed).await;
    // The refusal is the marker rule, not a backend failure: the
    // decision span carries its own bounded reason.
    let (allowed, reason) = recorded_decision(&access, &addressed).await;
    assert!(!allowed);
    assert_eq!(reason.as_deref(), Some("delete-marker"));
    assert!(
        matches!(
            storage
                .head_object_version(&b, &k, Some(&marker_id))
                .await
                .unwrap(),
            version::HeadResult::Marker { .. }
        ),
        "the refused gate leaves the marker in place"
    );
    let mut owner = req(uid.clone(), true, "DeleteObject", &path);
    owner.query = format!("versionId={marker_id}");
    assert_allowed(&access, &owner).await;
}

#[tokio::test]
async fn a_malformed_version_address_fails_closed() {
    // A `versionId` the address parse rejects names no resource: the
    // gate refuses instead of falling back to the current object's ACL
    // (the handler answers the malformed request only for a caller the
    // gate admits).
    let uid = uid();
    let (storage, _older) = two_versions(&uid, &alice(), &bob()).await;
    let access = AclAccess::new(storage, identity());
    let path = S3Path::object("data", "k");
    for op in ["DeleteObject", "PutObjectRetention"] {
        let mut malformed = req(bob(), true, op, &path);
        malformed.query = "versionId=".to_owned();
        assert_denied(&access, &malformed).await;
    }
    let mut malformed = req(anon(), false, "GetObjectRetention", &path);
    malformed.query = "versionId=".to_owned();
    assert_denied(&access, &malformed).await;
}
