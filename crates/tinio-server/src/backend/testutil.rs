//! Shared unit-test helpers of the backend modules.

#[cfg(feature = "acl")]
use std::sync::Arc;

use http::{Extensions, HeaderMap, Method, Uri};
use s3s::S3Request;
#[cfg(feature = "acl")]
use s3s::dto;

use super::{Capabilities, S3Backend};
#[cfg(feature = "acl")]
use crate::_auth::identity::{Identity, User, derive_canonical_id};
#[cfg(feature = "acl")]
use crate::_core::acl;
use crate::{
    _core::{acl::Acl, bucket, storage::BucketOps},
    _mem::MemoryStorage,
    capability::CapabilityS3,
};

/// A minimal `S3Request` with default headers (tests fill the input).
pub(crate) fn s3_request<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: Method::GET,
        uri: Uri::default(),
        headers: HeaderMap::new(),
        extensions: Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// A fresh backend over `MemoryStorage` with a `data` bucket created;
/// returns the bucket name as a string.
pub(crate) async fn setup() -> (S3Backend<MemoryStorage>, String) {
    setup_with_caps(Default::default()).await
}

/// Like [`setup`], with explicit runtime capabilities (tests opt in to
/// the checksum toggle).
pub(crate) async fn setup_with_caps(caps: Capabilities) -> (S3Backend<MemoryStorage>, String) {
    let backend = S3Backend::new(MemoryStorage::new().unwrap(), caps);
    let storage = backend.storage();
    let b = "data".to_string();
    storage
        .create_bucket(
            &bucket::name(&b).unwrap(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    (backend, b)
}

/// Like [`setup_with_caps`], through the capability decorator: the `[s3]`
/// gates live there now (FR-021), so a test that asserts a refusal must
/// drive `CapabilityS3`, not a bare `S3Backend`. The wrapped backend is
/// reachable via [`CapabilityS3::inner`] for fixture setup.
pub(crate) async fn setup_gated_with_caps(
    caps: Capabilities,
) -> (CapabilityS3<S3Backend<MemoryStorage>>, String) {
    let (backend, bucket) = setup_with_caps(caps).await;
    (CapabilityS3::new(backend, caps), bucket)
}

/// The identity map of the ACL write-path fixtures: two signed users
/// (`AKID` alice, `BKID` bob) and the built-in default owner.
#[cfg(feature = "acl")]
pub(crate) fn acl_identity() -> Arc<Identity> {
    Arc::new(Identity::test(vec![
        User::test("AKID", "secret", "alice"),
        User::test("BKID", "secret", "bob"),
    ]))
}

/// The fixture user's canonical ID for an access key.
#[cfg(feature = "acl")]
pub(crate) fn user_id(access_key: &str) -> acl::OwnerId {
    derive_canonical_id(access_key)
}

#[cfg(feature = "acl")]
use s3s::auth::Credentials;
#[cfg(feature = "acl")]
pub(crate) fn credentials_for(access_key: &str) -> Credentials {
    use s3s::auth::{Credentials, SecretKey};
    Credentials {
        access_key: access_key.into(),
        secret_key: SecretKey::from("secret"),
    }
}

/// A request carrying the fixture user's credentials.
#[cfg(feature = "acl")]
pub(crate) fn signed_request<T>(input: T) -> S3Request<T> {
    let mut req = s3_request(input);
    req.credentials = Some(credentials_for("AKID"));
    req
}

/// A POST request: a PostObject form upload reaches `op_put_object`
/// through s3s's default delegation carrying the original POST method.
#[cfg(feature = "acl")]
pub(crate) fn post_request<T>(input: T) -> S3Request<T> {
    let mut req = s3_request(input);
    req.method = Method::POST;
    req
}

/// A backend with the fixture identity attached (the enforced-mode
/// shape of the ACL write-path fixtures).
#[cfg(feature = "acl")]
pub(crate) fn acl_backend() -> S3Backend<MemoryStorage> {
    S3Backend::new(MemoryStorage::new().unwrap(), Default::default()).with_identity(acl_identity())
}

// --- the ACL request fixtures (one home for the three test modules'
// copies: objects, buckets, versioning) ---

/// A well-formed `Content-MD5`: base64 of 16 zero bytes (form-checked only —
/// the body is consumed before the handler).
pub(crate) const VALID_MD5: &str = "AAAAAAAAAAAAAAAAAAAAAA==";

/// [`VALID_MD5`] as the owned DTO field.
pub(crate) fn valid_md5() -> String {
    VALID_MD5.to_owned()
}

/// A canonical-user grantee.
#[cfg(feature = "acl")]
pub(crate) fn canonical_grantee(id: &str) -> dto::Grantee {
    dto::Grantee {
        type_: dto::Type::from_static(dto::Type::CANONICAL_USER),
        id: Some(id.into()),
        uri: None,
        display_name: None,
        email_address: None,
    }
}

/// A group grantee.
#[cfg(feature = "acl")]
pub(crate) fn group_grantee(uri: &str) -> dto::Grantee {
    dto::Grantee {
        type_: dto::Type::from_static(dto::Type::GROUP),
        uri: Some(uri.into()),
        id: None,
        display_name: None,
        email_address: None,
    }
}

/// A dto grant of one grantee.
#[cfg(feature = "acl")]
pub(crate) fn grant(grantee: dto::Grantee, permission: &'static str) -> dto::Grant {
    dto::Grant {
        grantee: Some(grantee),
        permission: Some(dto::Permission::from_static(permission)),
    }
}
