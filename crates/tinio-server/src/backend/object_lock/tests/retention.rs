//! Retention tests: the write-header validation, the retention document
//! shape, the expired-retention inert rule, and the GOVERNANCE bypass /
//! COMPLIANCE refusal on retention updates (OL8, OL11, OL15, OL17, R29).

use std::time::{Duration, SystemTime};

use s3s::dto;

use super::common::*;
use crate::{
    _core::{
        bucket,
        meta::VersionMeta,
        object,
        object_lock::{LegalHold, Lock, Mode, Retention},
        version,
    },
    backend::{
        object_lock::retention_mode_to_dto,
        testutil::{VALID_MD5, s3_request},
    },
};

#[tokio::test]
async fn the_write_headers_are_validated() {
    let (backend, b) = lock_backend().await;
    // One half of the retention pair.
    for req in [
        put_with_raw_headers(&b, "k", Some("GOVERNANCE"), None, None, Some(VALID_MD5)),
        put_with_raw_headers(&b, "k", None, Some(future()), None, Some(VALID_MD5)),
    ] {
        let err = backend.op_put_object(req).await.unwrap_err();
        assert_eq!(code(&err), "InvalidRequest", "{err:?}");
    }
    // Unknown wire literals are the closed enums' `InvalidArgument`.
    let err = backend
        .op_put_object(put_with_raw_headers(
            &b,
            "k",
            Some("BOGUS"),
            Some(future()),
            None,
            Some(VALID_MD5),
        ))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidArgument", "{err:?}");
    let err = backend
        .op_put_object(put_with_raw_headers(
            &b,
            "k",
            None,
            None,
            Some("MAYBE"),
            None,
        ))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidArgument", "{err:?}");
    // A past retain-until date is the semantic `InvalidRequest`.
    let err = backend
        .op_put_object(put_with_raw_headers(
            &b,
            "k",
            Some("COMPLIANCE"),
            Some(SystemTime::now() - Duration::from_secs(60)),
            None,
            Some(VALID_MD5),
        ))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidRequest", "{err:?}");
}

#[tokio::test]
async fn the_retention_xml_requires_a_complete_pair() {
    let (backend, b) = lock_backend().await;
    // Mode only, date only, and an empty element: all MalformedXML. An
    // absent element is not one of these — R29 makes it the clear (covered
    // by `an_absent_retention_element_clears_and_a_partial_pair_stays_malformed`).
    for (mode, date) in [
        (Some("COMPLIANCE"), None),
        (None, Some(future())),
        (None, None),
    ] {
        let mut input = put_retention(&b, "k", Mode::Compliance, None);
        input.input.retention = Some(dto::ObjectLockRetention {
            mode: mode.map(|m| dto::ObjectLockRetentionMode::from(m.to_owned())),
            retain_until_date: date.map(dto::Date::from),
        });
        let err = backend.op_put_object_retention(input).await.unwrap_err();
        assert_eq!(code(&err), "MalformedXML", "{mode:?}: {err:?}");
    }
    // A complete pair with a past date is the semantic refusal.
    let mut past = put_retention(&b, "k", Mode::Compliance, None);
    past.input.retention = Some(dto::ObjectLockRetention {
        mode: Some(dto::ObjectLockRetentionMode::from_static(
            dto::ObjectLockRetentionMode::COMPLIANCE,
        )),
        retain_until_date: Some(dto::Date::from(SystemTime::now() - Duration::from_secs(60))),
    });
    let err = backend.op_put_object_retention(past).await.unwrap_err();
    assert_eq!(code(&err), "InvalidRequest", "{err:?}");
}

#[tokio::test]
async fn an_expired_governance_retention_is_inert() {
    // An unexpired governance retention refuses the delete; the *expired*
    // one is inert (OL11). The past date cannot be written through the
    // request path, so the lock is planted test-only on the version the
    // write created.
    let (backend, b) = lock_backend().await;
    let name = bucket::name(&b).unwrap();
    let key = object::key("k").unwrap();
    let written = backend
        .op_put_object(put(&b, "k", None, None, None))
        .await
        .unwrap();
    let version = version_of(&written);
    let id = version::id(&version).unwrap();
    VersionMeta::set_version_lock(
        backend.storage().metadata().as_ref(),
        &name,
        &key,
        Some(&id),
        Some(&Lock {
            retention: Some(Retention {
                mode: Mode::Governance,
                retain_until: SystemTime::now() - Duration::from_secs(60),
            }),
            legal_hold: LegalHold::Off,
        }),
    )
    .await
    .unwrap();
    backend
        .op_delete_object(delete_version(&b, "k", &version))
        .await
        .expect("an expired retention does not block the delete");
}

// --- OL15: the governance-bypass authority ---

#[tokio::test]
async fn a_governance_retention_update_without_the_authority_refuses() {
    let (backend, b) = lock_backend().await;
    let written = backend
        .op_put_object(put_until(
            &b,
            "k",
            Mode::Governance,
            future() + Duration::from_secs(7200),
        ))
        .await
        .unwrap();
    let version = version_of(&written);
    let err = backend
        .op_put_object_retention(put_retention_until(
            &b,
            "k",
            Some(&version),
            Mode::Governance,
            future(),
            true,
        ))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "AccessDenied", "{err:?}");
}

#[cfg(feature = "acl")]
#[tokio::test]
async fn a_governance_retention_update_bypasses_with_the_header_and_the_authority() {
    let (backend, b) = lock_backend().await;
    let written = backend
        .op_put_object(put_until(
            &b,
            "k",
            Mode::Governance,
            future() + Duration::from_secs(7200),
        ))
        .await
        .unwrap();
    let version = version_of(&written);
    let requested = future();

    // The authority alone is not the header.
    let mut no_header =
        put_retention_until(&b, "k", Some(&version), Mode::Governance, requested, false);
    grant(&mut no_header);
    let err = backend
        .op_put_object_retention(no_header)
        .await
        .unwrap_err();
    assert_eq!(code(&err), "AccessDenied", "{err:?}");

    let mut both = put_retention_until(&b, "k", Some(&version), Mode::Governance, requested, true);
    grant(&mut both);
    backend
        .op_put_object_retention(both)
        .await
        .expect("the header and the authority shorten the retention");
    let got = backend
        .op_get_object_retention(get_retention(&b, "k", Some(&version)))
        .await
        .unwrap();
    let retention = got.output.retention.expect("the shortened retention");
    assert_eq!(
        retention.mode.as_ref().map(|mode| mode.as_str()),
        Some(dto::ObjectLockRetentionMode::GOVERNANCE)
    );
    let stored: SystemTime =
        time::OffsetDateTime::from(retention.retain_until_date.expect("the retained date")).into();
    assert_eq!(stored, requested);
}

#[cfg(feature = "acl")]
#[tokio::test]
async fn a_compliance_retention_update_refuses_even_with_the_authority() {
    let (backend, b) = lock_backend().await;
    let written = backend
        .op_put_object(put_until(
            &b,
            "k",
            Mode::Compliance,
            future() + Duration::from_secs(7200),
        ))
        .await
        .unwrap();
    let version = version_of(&written);
    let mut both = put_retention_until(&b, "k", Some(&version), Mode::Compliance, future(), true);
    grant(&mut both);
    let err = backend.op_put_object_retention(both).await.unwrap_err();
    assert_eq!(code(&err), "AccessDenied", "{err:?}");
}

/// Minor 6 (R29): `PutObjectRetention` with no `Retention` element clears the
/// stored retention, while a present-but-partial element stays the
/// malformed document (`400 MalformedXML`).
#[cfg(feature = "acl")]
#[tokio::test]
async fn an_absent_retention_element_clears_and_a_partial_pair_stays_malformed() {
    let (backend, b) = lock_backend().await;
    let written = backend
        .op_put_object(put_until(
            &b,
            "k",
            Mode::Governance,
            future() + Duration::from_secs(7200),
        ))
        .await
        .expect("a governance-retained write");
    let version = version_of(&written);

    // The clear is the absent element, and governance needs the bypass plus
    // the authority.
    let mut clear = s3_request(dto::PutObjectRetentionInput {
        bucket: b.clone(),
        key: "k".into(),
        version_id: Some(version.clone()),
        content_md5: Some(VALID_MD5.to_owned()),
        bypass_governance_retention: Some(true),
        retention: None,
        ..Default::default()
    });
    grant(&mut clear);
    backend
        .op_put_object_retention(clear)
        .await
        .expect("the absent element clears the retention");
    let err = backend
        .op_get_object_retention(get_retention(&b, "k", Some(&version)))
        .await
        .unwrap_err();
    assert_eq!(
        code(&err),
        "NoSuchObjectLockConfiguration",
        "the retention is gone: {err:?}"
    );

    // A present element with one half is still malformed, not a clear.
    for retention in [
        dto::ObjectLockRetention {
            mode: Some(retention_mode_to_dto(Mode::Governance)),
            retain_until_date: None,
        },
        dto::ObjectLockRetention {
            mode: None,
            retain_until_date: Some(dto::Date::from(future())),
        },
    ] {
        let err = backend
            .op_put_object_retention(s3_request(dto::PutObjectRetentionInput {
                bucket: b.clone(),
                key: "k".into(),
                version_id: Some(version.clone()),
                content_md5: Some(VALID_MD5.to_owned()),
                retention: Some(retention),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(code(&err), "MalformedXML", "{err:?}");
    }
}
