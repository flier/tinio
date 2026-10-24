//! Legal hold tests: the retention/legal-hold round trip and the
//! `PutObjectLegalHold` document shape (OL8, OL17).

use s3s::dto;

use super::common::*;
use crate::{
    _core::object_lock::{LegalHold, Mode},
    backend::testutil::VALID_MD5,
};

#[tokio::test]
async fn the_retention_and_legal_hold_operations_round_trip() {
    let (backend, b) = lock_backend().await;
    let put = backend
        .op_put_object(put(&b, "k", Some(Mode::Governance), None, Some(VALID_MD5)))
        .await
        .unwrap();
    let version = version_of(&put);

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
        Some(dto::ObjectLockRetentionMode::GOVERNANCE)
    );

    backend
        .op_put_object_legal_hold(put_hold(&b, "k", LegalHold::On, None))
        .await
        .unwrap();
    let hold = backend
        .op_get_object_legal_hold(get_hold(&b, "k", None))
        .await
        .unwrap();
    assert_eq!(
        hold.output
            .legal_hold
            .unwrap()
            .status
            .as_ref()
            .map(|s| s.as_str()),
        Some(dto::ObjectLockLegalHoldStatus::ON)
    );

    // Setting a retention replaces only that fact; the hold survives.
    backend
        .op_put_object_retention(put_retention(&b, "k", Mode::Compliance, Some(&version)))
        .await
        .unwrap();
    let hold = backend
        .op_get_object_legal_hold(get_hold(&b, "k", Some(&version)))
        .await
        .unwrap();
    assert_eq!(
        hold.output
            .legal_hold
            .unwrap()
            .status
            .as_ref()
            .map(|s| s.as_str()),
        Some(dto::ObjectLockLegalHoldStatus::ON)
    );

    // Clearing the hold keeps the retention (OFF echoes now).
    backend
        .op_put_object_legal_hold(put_hold(&b, "k", LegalHold::Off, None))
        .await
        .unwrap();
    let hold = backend
        .op_get_object_legal_hold(get_hold(&b, "k", None))
        .await
        .unwrap();
    assert_eq!(
        hold.output
            .legal_hold
            .unwrap()
            .status
            .as_ref()
            .map(|s| s.as_str()),
        Some(dto::ObjectLockLegalHoldStatus::OFF)
    );
    assert!(
        backend
            .op_get_object_retention(get_retention(&b, "k", None))
            .await
            .unwrap()
            .output
            .retention
            .is_some(),
        "the compliance retention the clear did not touch is still there"
    );
}

#[tokio::test]
async fn the_legal_hold_xml_requires_a_status() {
    let (backend, b) = lock_backend().await;
    // The element itself is required.
    let mut absent = put_hold(&b, "k", LegalHold::On, None);
    absent.input.legal_hold = None;
    let err = backend.op_put_object_legal_hold(absent).await.unwrap_err();
    assert_eq!(code(&err), "MalformedXML", "{err:?}");
    // A missing or unknown Status is MalformedXML.
    for status in [None, Some("MAYBE")] {
        let mut input = put_hold(&b, "k", LegalHold::On, None);
        input.input.legal_hold = Some(dto::ObjectLockLegalHold {
            status: status.map(|s| dto::ObjectLockLegalHoldStatus::from(s.to_owned())),
        });
        let err = backend.op_put_object_legal_hold(input).await.unwrap_err();
        assert_eq!(code(&err), "MalformedXML", "{status:?}: {err:?}");
    }
}
