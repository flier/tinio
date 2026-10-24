//! Shared fixtures for the Object Lock handler tests (OL1–OL20, OL29).
//!
//! A backend over `MemoryStorage` carries the fixtures: a lock-enabled bucket
//! created through the S3 header, the writes' three lock headers, and the
//! delete paths. The planted-lock tests (`set_version_lock`) cover the rows
//! the S3 surface cannot reach on an always-`Enabled` lock bucket — an
//! expired governance retention and the null version's lock, which is exactly
//! the identity-wire case OL18 pins.

use std::{
    io,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use futures::stream;
use s3s::dto;
pub(super) use tinio_util::testing::metrics::protected_deletions;

#[cfg(feature = "acl")]
use crate::{_auth::MayBypass, backend::testutil::acl_identity};
use crate::{
    _core::object_lock::{DefaultRetention, LegalHold, Mode, Period, Rule},
    _mem::MemoryStorage,
    backend::{
        S3Backend,
        object_lock::{
            default_retention_to_dto, hold_status_to_dto, mode_to_dto, retention_mode_to_dto,
        },
        testutil::{VALID_MD5, s3_request},
    },
};

pub(super) const LOCKED: &str = "locked";

pub(super) fn future() -> SystemTime {
    SystemTime::now() + Duration::from_secs(3600)
}

/// A backend over `MemoryStorage` with a lock-enabled bucket created through
/// the S3 header (versioning `Enabled` + the configuration).
pub(super) async fn lock_backend() -> (S3Backend<MemoryStorage>, String) {
    let backend = S3Backend::new(MemoryStorage::new().unwrap(), Default::default());
    backend
        .op_create_bucket(s3_request(dto::CreateBucketInput {
            bucket: LOCKED.into(),
            object_lock_enabled_for_bucket: Some(true),
            ..Default::default()
        }))
        .await
        .expect("the create-bucket header creates the lock-enabled bucket");
    (backend, LOCKED.to_owned())
}

/// The `payload` body every `PutObject` fixture carries.
fn put_body() -> dto::StreamingBlob {
    dto::StreamingBlob::wrap(stream::once(async {
        Ok::<_, io::Error>(Bytes::from_static(b"payload"))
    }))
}

pub(super) fn put(
    bucket: &str,
    key: &str,
    retention: Option<Mode>,
    hold: Option<LegalHold>,
    md5: Option<&'static str>,
) -> s3s::S3Request<dto::PutObjectInput> {
    s3_request(dto::PutObjectInput {
        bucket: bucket.into(),
        key: key.into(),
        body: Some(put_body()),
        content_md5: md5.map(str::to_owned),
        object_lock_mode: retention.map(mode_to_dto),
        object_lock_retain_until_date: retention
            .map(|_| dto::ObjectLockRetainUntilDate::from(future())),
        object_lock_legal_hold_status: hold.map(hold_status_to_dto),
        ..Default::default()
    })
}

/// A `PutObject` with the lock headers spelled as raw strings — the
/// validation matrix's shape (the dto enums are string wrappers, so an
/// unknown literal must reach the handler as one).
pub(super) fn put_with_raw_headers(
    bucket: &str,
    key: &str,
    mode: Option<&str>,
    date: Option<SystemTime>,
    hold: Option<&str>,
    md5: Option<&'static str>,
) -> s3s::S3Request<dto::PutObjectInput> {
    s3_request(dto::PutObjectInput {
        bucket: bucket.into(),
        key: key.into(),
        body: Some(put_body()),
        content_md5: md5.map(str::to_owned),
        object_lock_mode: mode.map(|mode| dto::ObjectLockMode::from(mode.to_owned())),
        object_lock_retain_until_date: date.map(dto::ObjectLockRetainUntilDate::from),
        object_lock_legal_hold_status: hold
            .map(|hold| dto::ObjectLockLegalHoldStatus::from(hold.to_owned())),
        ..Default::default()
    })
}

pub(super) fn get_config(bucket: &str) -> s3s::S3Request<dto::GetObjectLockConfigurationInput> {
    s3_request(dto::GetObjectLockConfigurationInput {
        bucket: bucket.into(),
        ..Default::default()
    })
}

pub(super) fn put_config(
    bucket: &str,
    rule: Option<Rule>,
) -> s3s::S3Request<dto::PutObjectLockConfigurationInput> {
    let configuration = dto::ObjectLockConfiguration {
        object_lock_enabled: Some(dto::ObjectLockEnabled::from_static(
            dto::ObjectLockEnabled::ENABLED,
        )),
        rule: rule.map(|rule| dto::ObjectLockRule {
            default_retention: Some(default_retention_to_dto(&rule.default_retention)),
        }),
    };
    s3_request(dto::PutObjectLockConfigurationInput {
        bucket: bucket.into(),
        object_lock_configuration: Some(configuration),
        content_md5: Some(VALID_MD5.to_owned()),
        ..Default::default()
    })
}

pub(super) fn get_retention(
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> s3s::S3Request<dto::GetObjectRetentionInput> {
    s3_request(dto::GetObjectRetentionInput {
        bucket: bucket.into(),
        key: key.into(),
        version_id: version_id.map(str::to_owned),
        ..Default::default()
    })
}

pub(super) fn put_retention(
    bucket: &str,
    key: &str,
    mode: Mode,
    version_id: Option<&str>,
) -> s3s::S3Request<dto::PutObjectRetentionInput> {
    s3_request(dto::PutObjectRetentionInput {
        bucket: bucket.into(),
        key: key.into(),
        version_id: version_id.map(str::to_owned),
        content_md5: Some(VALID_MD5.to_owned()),
        retention: Some(dto::ObjectLockRetention {
            mode: Some(retention_mode_to_dto(mode)),
            retain_until_date: Some(dto::Date::from(future())),
        }),
        ..Default::default()
    })
}

pub(super) fn put_hold(
    bucket: &str,
    key: &str,
    hold: LegalHold,
    version_id: Option<&str>,
) -> s3s::S3Request<dto::PutObjectLegalHoldInput> {
    s3_request(dto::PutObjectLegalHoldInput {
        bucket: bucket.into(),
        key: key.into(),
        version_id: version_id.map(str::to_owned),
        content_md5: Some(VALID_MD5.to_owned()),
        legal_hold: Some(dto::ObjectLockLegalHold {
            status: Some(hold_status_to_dto(hold)),
        }),
        ..Default::default()
    })
}

pub(super) fn get_hold(
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> s3s::S3Request<dto::GetObjectLegalHoldInput> {
    s3_request(dto::GetObjectLegalHoldInput {
        bucket: bucket.into(),
        key: key.into(),
        version_id: version_id.map(str::to_owned),
        ..Default::default()
    })
}

pub(super) fn version_of(resp: &s3s::S3Response<dto::PutObjectOutput>) -> String {
    resp.headers
        .get("x-amz-version-id")
        .and_then(|value| value.to_str().ok())
        .expect("the write answers the version it created")
        .to_owned()
}

pub(super) fn delete_version(
    bucket: &str,
    key: &str,
    version_id: &str,
) -> s3s::S3Request<dto::DeleteObjectInput> {
    s3_request(dto::DeleteObjectInput {
        bucket: bucket.into(),
        key: key.into(),
        version_id: Some(version_id.to_owned()),
        ..Default::default()
    })
}

/// A `PutObject` with an explicit retention ending at `until` — the
/// governance-bypass fixtures need a longer window than [`future`] gives.
pub(super) fn put_until(
    bucket: &str,
    key: &str,
    mode: Mode,
    until: SystemTime,
) -> s3s::S3Request<dto::PutObjectInput> {
    let mut req = put(bucket, key, Some(mode), None, Some(VALID_MD5));
    req.input.object_lock_retain_until_date = Some(dto::ObjectLockRetainUntilDate::from(until));
    req
}

/// The delete request with `x-amz-bypass-governance-retention: true`.
pub(super) fn delete_with_bypass(
    bucket: &str,
    key: &str,
    version_id: &str,
) -> s3s::S3Request<dto::DeleteObjectInput> {
    let mut req = delete_version(bucket, key, version_id);
    req.input.bypass_governance_retention = Some(true);
    req
}

/// A `PutObjectRetention` proposing `until` (with the bypass header when
/// `bypass`).
pub(super) fn put_retention_until(
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    mode: Mode,
    until: SystemTime,
    bypass: bool,
) -> s3s::S3Request<dto::PutObjectRetentionInput> {
    let mut req = put_retention(bucket, key, mode, version_id);
    req.input.retention = Some(dto::ObjectLockRetention {
        mode: Some(retention_mode_to_dto(mode)),
        retain_until_date: Some(dto::Date::from(until)),
    });
    req.input.bypass_governance_retention = Some(bypass);
    req
}

/// Plant the authority bit the ACL pipeline would insert for a granted
/// caller (OL15).
#[cfg(feature = "acl")]
pub(super) fn grant<T>(req: &mut s3s::S3Request<T>) {
    req.extensions.insert(MayBypass::new(true));
}

/// A lock-enabled bucket whose backend carries the fixture identity — the
/// per-entry `DeleteObjects` authority resolves through it.
#[cfg(feature = "acl")]
pub(super) async fn lock_backend_with_identity() -> (S3Backend<MemoryStorage>, String) {
    let (backend, bucket) = lock_backend().await;
    (backend.with_identity(acl_identity()), bucket)
}

pub(super) fn rule() -> Rule {
    Rule {
        default_retention: DefaultRetention {
            mode: Mode::Compliance,
            period: Period::Days(30),
        },
    }
}

pub(super) fn code(err: &s3s::S3Error) -> &str {
    err.code().as_str()
}

// --- the six round trips and the create header ---
