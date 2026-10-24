//! Delete, delete-marker, multipart and checksum tests: the protected-delete
//! refusals and GOVERNANCE bypass, the `DeleteObjects` per-entry authority,
//! the protected-deletion metrics, the multipart create intent, the marker
//! rules and the checksum a lock requires (OL9, OL12, OL15, OL16, OL20).

use std::{io, time::Duration};

use bytes::Bytes;
use futures::stream;
use http::HeaderValue;
use s3s::dto;

use super::common::*;
#[cfg(feature = "acl")]
use crate::backend::testutil::credentials_for;
use crate::{
    _core::object_lock::{LegalHold, Mode},
    backend::{
        object_lock::mode_to_dto,
        testutil::{VALID_MD5, s3_request},
    },
    metrics::test_lock,
};

#[tokio::test]
async fn a_lock_with_a_retention_requires_a_checksum() {
    let (backend, b) = lock_backend().await;
    let err = backend
        .op_put_object(put(&b, "k", Some(Mode::Compliance), None, None))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidRequest", "{err:?}");

    let err = backend
        .op_put_object(put(
            &b,
            "k",
            Some(Mode::Compliance),
            None,
            Some("not-base64!"),
        ))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidDigest", "{err:?}");

    // A checksum header of any algorithm satisfies the requirement.
    let mut req = put(&b, "k", Some(Mode::Compliance), None, None);
    req.headers
        .insert("x-amz-checksum-crc32", HeaderValue::from_static("AAAAAA=="));
    backend.op_put_object(req).await.unwrap();

    // A legal-hold-only lock carries no retention: no digest needed.
    backend
        .op_put_object(put(&b, "h", None, Some(LegalHold::On), None))
        .await
        .unwrap();
}

#[tokio::test]
async fn a_default_retention_requires_a_checksum_on_every_write() {
    let (backend, b) = lock_backend().await;
    backend
        .op_put_object_lock_configuration(put_config(&b, Some(rule())))
        .await
        .unwrap();
    let err = backend
        .op_put_object(put(&b, "k", None, None, None))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidRequest", "{err:?}");
    backend
        .op_put_object(put(&b, "k", None, None, Some(VALID_MD5)))
        .await
        .unwrap();
    // The default was applied to the stored version.
    let got = backend
        .op_get_object_retention(get_retention(&b, "k", None))
        .await
        .unwrap();
    assert_eq!(
        got.output
            .retention
            .unwrap()
            .mode
            .as_ref()
            .map(|m| m.as_str()),
        Some(dto::ObjectLockRetentionMode::COMPLIANCE)
    );
}

#[tokio::test]
async fn a_protected_version_cannot_be_deleted() {
    let (backend, b) = lock_backend().await;
    let put = backend
        .op_put_object(put(&b, "k", Some(Mode::Compliance), None, Some(VALID_MD5)))
        .await
        .unwrap();
    let version = version_of(&put);
    let err = backend
        .op_delete_object(delete_version(&b, "k", &version))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "AccessDenied", "{err:?}");
    // The version is still readable.
    backend
        .op_head_object(s3_request(dto::HeadObjectInput {
            bucket: b.clone(),
            key: "k".into(),
            version_id: Some(version),
            ..Default::default()
        }))
        .await
        .expect("the protected version survives");

    // A plain DELETE is never blocked: it writes a marker.
    backend
        .op_delete_object(s3_request(dto::DeleteObjectInput {
            bucket: b,
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
}

#[tokio::test]
async fn a_governance_delete_without_the_authority_refuses() {
    // The header alone is not authority (the ACL-off / no-identity shape:
    // no pipeline inserted the extension bit).
    let (backend, b) = lock_backend().await;
    let written = backend
        .op_put_object(put_until(
            &b,
            "k",
            Mode::Governance,
            future() + Duration::from_secs(3600),
        ))
        .await
        .unwrap();
    let version = version_of(&written);
    let err = backend
        .op_delete_object(delete_with_bypass(&b, "k", &version))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "AccessDenied", "{err:?}");
    backend
        .op_head_object(s3_request(dto::HeadObjectInput {
            bucket: b.clone(),
            key: "k".into(),
            version_id: Some(version),
            ..Default::default()
        }))
        .await
        .expect("the governance version survives");
}

#[cfg(feature = "acl")]
#[tokio::test]
async fn a_governance_delete_bypasses_with_the_header_and_the_authority() {
    let (backend, b) = lock_backend().await;
    let written = backend
        .op_put_object(put_until(
            &b,
            "k",
            Mode::Governance,
            future() + Duration::from_secs(3600),
        ))
        .await
        .unwrap();
    let version = version_of(&written);

    // The authority alone is not the header.
    let mut no_header = delete_version(&b, "k", &version);
    grant(&mut no_header);
    let err = backend.op_delete_object(no_header).await.unwrap_err();
    assert_eq!(code(&err), "AccessDenied", "{err:?}");

    let mut both = delete_with_bypass(&b, "k", &version);
    grant(&mut both);
    backend
        .op_delete_object(both)
        .await
        .expect("the header and the authority bypass governance");
}

#[cfg(feature = "acl")]
#[tokio::test]
async fn a_compliance_delete_refuses_even_with_the_authority() {
    let (backend, b) = lock_backend().await;
    let written = backend
        .op_put_object(put_until(
            &b,
            "k",
            Mode::Compliance,
            future() + Duration::from_secs(3600),
        ))
        .await
        .unwrap();
    let version = version_of(&written);
    let mut both = delete_with_bypass(&b, "k", &version);
    grant(&mut both);
    let err = backend.op_delete_object(both).await.unwrap_err();
    assert_eq!(code(&err), "AccessDenied", "{err:?}");
}

fn delete_objects_request(
    bucket: &str,
    entries: Vec<(&str, String)>,
    bypass: bool,
) -> s3s::S3Request<dto::DeleteObjectsInput> {
    s3_request(dto::DeleteObjectsInput {
        bucket: bucket.into(),
        delete: dto::Delete {
            objects: entries
                .into_iter()
                .map(|(key, version_id)| dto::ObjectIdentifier {
                    key: key.into(),
                    version_id: Some(version_id),
                    ..Default::default()
                })
                .collect(),
            quiet: None,
        },
        bypass_governance_retention: Some(bypass),
        checksum_algorithm: None,
        expected_bucket_owner: None,
        mfa: None,
        request_payer: None,
    })
}

#[tokio::test]
async fn delete_objects_without_the_authority_refuses_the_protected_entry() {
    // The batch resolves the authority per entry: without an identity
    // plane no entry carries it, so the governance entry is this batch's
    // per-key AccessDenied while the open entry proceeds (OL17).
    let (backend, b) = lock_backend().await;
    let protected = backend
        .op_put_object(put_until(
            &b,
            "protected",
            Mode::Governance,
            future() + Duration::from_secs(3600),
        ))
        .await
        .unwrap();
    let open = backend
        .op_put_object(put(&b, "open", None, None, None))
        .await
        .unwrap();
    let out = backend
        .op_delete_objects(delete_objects_request(
            &b,
            vec![
                ("protected", version_of(&protected)),
                ("open", version_of(&open)),
            ],
            true,
        ))
        .await
        .unwrap();
    let errors = out.output.errors.unwrap_or_default();
    assert_eq!(errors.len(), 1, "the protected entry is the batch's error");
    assert_eq!(errors[0].key.as_deref(), Some("protected"));
    assert_eq!(errors[0].code.as_deref(), Some("AccessDenied"));
    let deleted = out.output.deleted.unwrap_or_default();
    assert_eq!(deleted.len(), 1, "the rest proceeds");
    assert_eq!(deleted[0].key.as_deref(), Some("open"));
}

#[cfg(feature = "acl")]
#[tokio::test]
async fn delete_objects_bypasses_each_protected_entry_with_the_authority() {
    // The identity plane's principal owns the bucket and the entries, so
    // every entry resolves the authority and the header rides each delete.
    let (backend, b) = lock_backend_with_identity().await;
    let protected = backend
        .op_put_object(put_until(
            &b,
            "protected",
            Mode::Governance,
            future() + Duration::from_secs(3600),
        ))
        .await
        .unwrap();
    let open = backend
        .op_put_object(put(&b, "open", None, None, None))
        .await
        .unwrap();
    let out = backend
        .op_delete_objects(delete_objects_request(
            &b,
            vec![
                ("protected", version_of(&protected)),
                ("open", version_of(&open)),
            ],
            true,
        ))
        .await
        .unwrap();
    assert_eq!(
        out.output.errors.unwrap_or_default().len(),
        0,
        "both entries resolve the authority"
    );
    assert_eq!(out.output.deleted.unwrap_or_default().len(), 2);
}

#[cfg(feature = "acl")]
#[tokio::test]
async fn delete_objects_resolves_each_entrys_addressed_authority() {
    // The addressed version belongs to the identity plane's default owner,
    // the current version to ALICE. ALICE passes the per-key gate (the
    // current object's ACL) but the addressed version's authority is not
    // hers, so the governance entry is refused while the open one proceeds.
    let (backend, b) = lock_backend_with_identity().await;
    let protected = backend
        .op_put_object(put_until(
            &b,
            "k",
            Mode::Governance,
            future() + Duration::from_secs(3600),
        ))
        .await
        .unwrap();
    let protected_version = version_of(&protected);
    let mut current = put(&b, "k", None, None, None);
    current.credentials = Some(credentials_for("AKID"));
    backend.op_put_object(current).await.unwrap();
    let mut open = put(&b, "open", None, None, None);
    open.credentials = Some(credentials_for("AKID"));
    let open = backend.op_put_object(open).await.unwrap();
    let mut request = delete_objects_request(
        &b,
        vec![("k", protected_version), ("open", version_of(&open))],
        true,
    );
    request.credentials = Some(credentials_for("AKID"));
    let out = backend.op_delete_objects(request).await.unwrap();
    let errors = out.output.errors.unwrap_or_default();
    assert_eq!(errors.len(), 1, "the addressed older version is protected");
    assert_eq!(errors[0].key.as_deref(), Some("k"));
    assert_eq!(errors[0].code.as_deref(), Some("AccessDenied"));
    assert_eq!(
        out.output.deleted.unwrap_or_default().len(),
        1,
        "the open entry proceeds"
    );
}

#[tokio::test]
async fn delete_objects_reports_a_protected_entry_per_key() {
    let (backend, b) = lock_backend().await;
    let protected = backend
        .op_put_object(put(
            &b,
            "protected",
            Some(Mode::Compliance),
            None,
            Some(VALID_MD5),
        ))
        .await
        .unwrap();
    let open = backend
        .op_put_object(put(&b, "open", None, None, None))
        .await
        .unwrap();
    let (protected, open) = (version_of(&protected), version_of(&open));
    let out = backend
        .op_delete_objects(s3_request(dto::DeleteObjectsInput {
            bucket: b,
            delete: dto::Delete {
                objects: vec![
                    dto::ObjectIdentifier {
                        key: "protected".into(),
                        version_id: Some(protected),
                        ..Default::default()
                    },
                    dto::ObjectIdentifier {
                        key: "open".into(),
                        version_id: Some(open),
                        ..Default::default()
                    },
                ],
                quiet: None,
            },
            bypass_governance_retention: None,
            checksum_algorithm: None,
            expected_bucket_owner: None,
            mfa: None,
            request_payer: None,
        }))
        .await
        .unwrap();
    let errors = out.output.errors.unwrap_or_default();
    assert_eq!(errors.len(), 1, "the protected entry is the batch's error");
    assert_eq!(errors[0].key.as_deref(), Some("protected"));
    assert_eq!(errors[0].code.as_deref(), Some("AccessDenied"));
    let deleted = out.output.deleted.unwrap_or_default();
    assert_eq!(deleted.len(), 1, "the rest proceeds");
    assert_eq!(deleted[0].key.as_deref(), Some("open"));
}

/// OL25: the single version-addressed refusal records
/// `operation=delete_object` with the lock's reason.

#[tokio::test]
async fn the_single_protected_delete_records_the_operation_and_reason() {
    let _window = test_lock::window();
    let (backend, b) = lock_backend().await;
    let written = backend
        .op_put_object(put(&b, "k", Some(Mode::Compliance), None, Some(VALID_MD5)))
        .await
        .unwrap();
    let version = version_of(&written);

    let before = protected_deletions("delete_object", "compliance");
    let before_bulk = protected_deletions("delete_objects", "compliance");
    let err = backend
        .op_delete_object(delete_version(&b, "k", &version))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "AccessDenied", "{err:?}");
    assert_eq!(
        protected_deletions("delete_object", "compliance") - before,
        1.0,
        "the refusal is recorded under operation=delete_object, reason=compliance"
    );
    assert_eq!(
        protected_deletions("delete_objects", "compliance") - before_bulk,
        0.0,
        "the single delete never books the bulk operation"
    );
}

/// OL25: a `DeleteObjects` entry's refusal records `operation=delete_objects`
/// with the lock's reason, while the unprotected entry proceeds.

#[tokio::test]
async fn the_bulk_protected_delete_records_the_operation_and_reason() {
    let _window = test_lock::window();
    let (backend, b) = lock_backend().await;
    let protected = backend
        .op_put_object(put(
            &b,
            "protected",
            Some(Mode::Compliance),
            None,
            Some(VALID_MD5),
        ))
        .await
        .unwrap();
    let open = backend
        .op_put_object(put(&b, "open", None, None, None))
        .await
        .unwrap();
    let before = protected_deletions("delete_objects", "compliance");
    let before_single = protected_deletions("delete_object", "compliance");
    let out = backend
        .op_delete_objects(delete_objects_request(
            &b,
            vec![
                ("protected", version_of(&protected)),
                ("open", version_of(&open)),
            ],
            false,
        ))
        .await
        .unwrap();
    assert_eq!(
        out.output.errors.unwrap_or_default().len(),
        1,
        "the protected entry is the batch's error"
    );
    assert_eq!(
        protected_deletions("delete_objects", "compliance") - before,
        1.0,
        "the refusal is recorded under operation=delete_objects, reason=compliance"
    );
    assert_eq!(
        protected_deletions("delete_object", "compliance") - before_single,
        0.0,
        "the batch never books the single operation"
    );
}

#[tokio::test]
async fn a_complete_multipart_upload_rejects_lock_headers() {
    let (backend, b) = lock_backend().await; // OL6/OL17: every lock header and the bypass header is refused; the
    // create-time intent is the only lock fact a completion acts on.
    for header in [
        "x-amz-object-lock-mode",
        "x-amz-object-lock-retain-until-date",
        "x-amz-object-lock-legal-hold",
        "x-amz-bypass-governance-retention",
    ] {
        let mut req = s3_request(dto::CompleteMultipartUploadInput {
            bucket: b.clone(),
            key: "k".into(),
            upload_id: "u".into(),
            ..Default::default()
        });
        req.headers
            .insert(header, HeaderValue::from_static("GOVERNANCE"));
        let err = backend.op_complete_multipart_upload(req).await.unwrap_err();
        assert_eq!(code(&err), "InvalidRequest", "{header}: {err:?}");
        assert!(
            err.message()
                .is_some_and(|message| message.contains(header)),
            "the refusal names {header}: {err:?}"
        );
    }
}

#[tokio::test]
async fn the_create_time_lock_intent_rides_the_completion() {
    let (backend, b) = lock_backend().await;
    // A retention-bearing create without a checksum is the same 400 as
    // PutObject's (OL16).
    let err = backend
        .op_create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
            bucket: b.clone(),
            key: "k".into(),
            object_lock_mode: Some(mode_to_dto(Mode::Compliance)),
            object_lock_retain_until_date: Some(dto::ObjectLockRetainUntilDate::from(future())),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidRequest", "{err:?}");

    let created = backend
        .op_create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
            bucket: b.clone(),
            key: "k".into(),
            object_lock_mode: Some(mode_to_dto(Mode::Compliance)),
            object_lock_retain_until_date: Some(dto::ObjectLockRetainUntilDate::from(future())),
            checksum_algorithm: Some(dto::ChecksumAlgorithm::from_static(
                dto::ChecksumAlgorithm::CRC32,
            )),
            ..Default::default()
        }))
        .await
        .unwrap();
    let upload_id = created.output.upload_id.unwrap();
    let part = backend
        .op_upload_part(s3_request(dto::UploadPartInput {
            bucket: b.clone(),
            key: "k".into(),
            upload_id: upload_id.clone(),
            part_number: 1,
            body: Some(dto::StreamingBlob::wrap(stream::iter(vec![Ok::<
                _,
                io::Error,
            >(
                Bytes::from_static(b"data"),
            )]))),
            ..Default::default()
        }))
        .await
        .unwrap();
    let etag = part.output.e_tag.unwrap();
    backend
        .op_complete_multipart_upload(s3_request(dto::CompleteMultipartUploadInput {
            bucket: b.clone(),
            key: "k".into(),
            upload_id,
            multipart_upload: Some(dto::CompletedMultipartUpload {
                parts: Some(vec![dto::CompletedPart {
                    part_number: Some(1),
                    e_tag: Some(etag),
                    ..Default::default()
                }]),
            }),
            ..Default::default()
        }))
        .await
        .unwrap();
    let got = backend
        .op_get_object_retention(get_retention(&b, "k", None))
        .await
        .unwrap();
    assert!(
        got.output.retention.is_some(),
        "the create-time intent rode the completion"
    );
}

// --- the error and marker classes ---

#[tokio::test]
async fn the_marker_rules_are_the_version_models() {
    let (backend, b) = lock_backend().await;
    backend
        .op_put_object(put(&b, "m", None, None, None))
        .await
        .unwrap();
    let deleted = backend
        .op_delete_object(s3_request(dto::DeleteObjectInput {
            bucket: b.clone(),
            key: "m".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    let marker = deleted
        .headers
        .get("x-amz-version-id")
        .and_then(|value| value.to_str().ok())
        .expect("the plain delete writes a marker")
        .to_owned();
    // No version id, current is a marker: 404 NoSuchKey.
    let err = backend
        .op_get_object_retention(get_retention(&b, "m", None))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "NoSuchKey", "{err:?}");
    // Explicitly addressed marker: 405 MethodNotAllowed.
    let err = backend
        .op_get_object_retention(get_retention(&b, "m", Some(&marker)))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "MethodNotAllowed", "{err:?}");
    let err = backend
        .op_put_object_legal_hold(put_hold(&b, "m", LegalHold::On, Some(&marker)))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "MethodNotAllowed", "{err:?}");
}

// --- the capability, read-only and the cache ---
