//! Delete, batch, drain and upload legs of the Object Lock conformance suite.
//!
//! The bucket and per-version legs stay in the parent module. These four call
//! the parent's fixtures.

use std::time::SystemTime;

use super::{
    super::fixtures::{bucket_at, bucket_name, governance_reservation, key, t0, version_row},
    compliance_reservation, expired_reservation, period_config,
};
use crate::_core::{
    acl::Acl,
    meta::{
        BucketMeta, Metadata, UploadMeta, upload,
        version::{Kind, VersionMeta},
    },
    object, object_lock, storage, version,
};

/// The delete leg: protection, bypass, compliance, legal hold, expiry, the
/// current version addressed by id and as null, and markers.
pub(super) async fn object_lock_delete_leg<V>(store: &V)
where
    V: Metadata + VersionMeta,
    <V as VersionMeta>::Error: From<storage::Error>,
{
    let bucket = bucket_name("conf-lock-delete");
    let k = key("delete.txt");
    let v1 = version::Id::new("d1").expect("a valid version id");
    let v2 = version::Id::new("d2").expect("a valid version id");
    BucketMeta::create(store, &bucket, &bucket_at(t0()))
        .await
        .unwrap();
    store
        .set_bucket_state(&bucket, version::State::Enabled)
        .await
        .unwrap();
    store
        .set_object_lock(&bucket, &period_config(None))
        .await
        .unwrap();
    store
        .put_version(&bucket, &k, &version_row(1, Some(v1.clone()), Kind::Object))
        .await
        .unwrap();
    store
        .put_version(&bucket, &k, &version_row(2, Some(v2.clone()), Kind::Object))
        .await
        .unwrap();
    // The null version's own row: the current version the delete addresses as
    // `None` (OL5/OL8).
    store
        .put_version(&bucket, &k, &version_row(3, None, Kind::Object))
        .await
        .unwrap();
    store
        .advance_slot(&bucket, &k, Some(v1.clone()), 1)
        .await
        .unwrap();

    // Governance: refused without the bypass, removed with it, lock row and
    // all — the current version addressed by its id.
    store
        .set_retention(
            &bucket,
            &k,
            Some(&v1),
            Some(&governance_reservation()),
            object_lock::Bypass::None,
            SystemTime::now(),
        )
        .await
        .unwrap();
    assert!(
        matches!(
            store
                .expire_with(&bucket, &k, 1, object_lock::Bypass::None)
                .await
                .unwrap_err()
                .into(),
            storage::Error::Locked(object_lock::RefusalReason::Governance)
        ),
        "a protected current version is refused without the bypass"
    );
    assert!(
        store.get_version(&bucket, &k, 1).await.unwrap().is_some(),
        "the refused delete removed nothing"
    );
    // The no-bypass form shares that refusal: `expire` delegates to
    // `expire_with(Bypass::None)`, so a protected row is never removed through
    // it.
    assert!(
        matches!(
            store.expire(&bucket, &k, 1).await.unwrap_err().into(),
            storage::Error::Locked(object_lock::RefusalReason::Governance)
        ),
        "the no-bypass expire refuses a protected row"
    );
    assert!(
        store.get_version(&bucket, &k, 1).await.unwrap().is_some(),
        "the refused expire removed nothing"
    );
    store
        .expire_with(&bucket, &k, 1, object_lock::Bypass::Governance)
        .await
        .unwrap();
    assert!(
        store.get_version(&bucket, &k, 1).await.unwrap().is_none(),
        "the bypassed delete removes the row"
    );
    assert_eq!(
        store.version_lock(&bucket, &k, Some(&v1)).await.unwrap(),
        None,
        "the delete removes the lock row with the version"
    );

    // Compliance ignores the bypass.
    store
        .set_retention(
            &bucket,
            &k,
            Some(&v2),
            Some(&compliance_reservation()),
            object_lock::Bypass::None,
            SystemTime::now(),
        )
        .await
        .unwrap();
    assert!(
        matches!(
            store
                .expire_with(&bucket, &k, 2, object_lock::Bypass::Governance)
                .await
                .unwrap_err()
                .into(),
            storage::Error::Locked(object_lock::RefusalReason::Compliance)
        ),
        "a compliance retention has no bypass"
    );
    assert!(
        store.get_version(&bucket, &k, 2).await.unwrap().is_some(),
        "the compliance refusal removed nothing"
    );

    // The current version is now the null version; the delete addresses it by
    // the null identity, not by an id.
    store.advance_slot(&bucket, &k, None, 3).await.unwrap();
    store
        .set_legal_hold(&bucket, &k, None, object_lock::LegalHold::On)
        .await
        .unwrap();
    assert!(
        matches!(
            store
                .expire_with(&bucket, &k, 3, object_lock::Bypass::Governance)
                .await
                .unwrap_err()
                .into(),
            storage::Error::Locked(object_lock::RefusalReason::LegalHold)
        ),
        "a legal hold refuses in either mode, bypass or not"
    );
    // Clear the hold through a lock write, then confirm an expired retention
    // is inert for the same delete.
    store
        .set_legal_hold(&bucket, &k, None, object_lock::LegalHold::Off)
        .await
        .unwrap();
    store
        .set_retention(
            &bucket,
            &k,
            None,
            Some(&expired_reservation()),
            object_lock::Bypass::None,
            SystemTime::now(),
        )
        .await
        .unwrap();
    store
        .expire_with(&bucket, &k, 3, object_lock::Bypass::None)
        .await
        .unwrap();
    assert!(
        store.get_version(&bucket, &k, 3).await.unwrap().is_none(),
        "an expired retention is inert: the null version's delete proceeds"
    );
    assert_eq!(
        store.version_lock(&bucket, &k, None).await.unwrap(),
        None,
        "the removed null version's lock row is gone"
    );

    // Markers are never protected: a marker row has no lock and deletes
    // without a bypass.
    store
        .put_version(&bucket, &k, &version_row(4, None, Kind::Marker))
        .await
        .unwrap();
    store
        .expire_with(&bucket, &k, 4, object_lock::Bypass::None)
        .await
        .unwrap();
    assert!(
        store.get_version(&bucket, &k, 4).await.unwrap().is_none(),
        "an unprotected marker deletes without a bypass"
    );

    // A current slot with **no version row**: the delete resolves the slot's
    // identity and removes the lock row the row-less version owns (OL8).
    let dangling = version::Id::new("d9").expect("a valid version id");
    store
        .advance_slot(&bucket, &k, Some(dangling.clone()), 9)
        .await
        .unwrap();
    store
        .set_version_lock(
            &bucket,
            &k,
            Some(&dangling),
            Some(&object_lock::Lock {
                retention: Some(governance_reservation()),
                legal_hold: object_lock::LegalHold::Off,
            }),
        )
        .await
        .unwrap();
    assert!(
        matches!(
            store
                .expire_with(&bucket, &k, 9, object_lock::Bypass::None)
                .await
                .unwrap_err()
                .into(),
            storage::Error::Locked(object_lock::RefusalReason::Governance)
        ),
        "a row-less current version is protected through its slot identity"
    );
    store
        .expire_with(&bucket, &k, 9, object_lock::Bypass::Governance)
        .await
        .unwrap();
    assert_eq!(
        store
            .version_lock(&bucket, &k, Some(&dangling))
            .await
            .unwrap(),
        None,
        "the row-less version's delete removes its lock row"
    );
}

/// The batch leg: `expire_many_report` skips and reports protected
/// sequences, and the old `expire_many` delegates with the report discarded.
pub(super) async fn object_lock_batch_leg<V>(store: &V)
where
    V: Metadata + VersionMeta,
{
    let bucket = bucket_name("conf-lock-batch");
    let k = key("batch.txt");
    let v2 = version::Id::new("b2").expect("a valid version id");
    BucketMeta::create(store, &bucket, &bucket_at(t0()))
        .await
        .unwrap();
    store
        .set_bucket_state(&bucket, version::State::Enabled)
        .await
        .unwrap();
    store
        .set_object_lock(&bucket, &period_config(None))
        .await
        .unwrap();
    for (seq, id) in [
        (1, version::Id::new("b1").expect("a valid version id")),
        (2, v2.clone()),
        (3, version::Id::new("b3").expect("a valid version id")),
    ] {
        store
            .put_version(&bucket, &k, &version_row(seq, Some(id), Kind::Object))
            .await
            .unwrap();
    }
    store
        .advance_slot(&bucket, &k, Some(v2.clone()), 2)
        .await
        .unwrap();
    // seq 2 is governed; seq 3 carries an expired retention (inert).
    store
        .set_retention(
            &bucket,
            &k,
            Some(&v2),
            Some(&governance_reservation()),
            object_lock::Bypass::None,
            SystemTime::now(),
        )
        .await
        .unwrap();
    let v3 = version::Id::new("b3").expect("a valid version id");
    store
        .set_retention(
            &bucket,
            &k,
            Some(&v3),
            Some(&expired_reservation()),
            object_lock::Bypass::None,
            SystemTime::now(),
        )
        .await
        .unwrap();

    let report = store
        .expire_many_report(&bucket, &k, &[1, 2, 3])
        .await
        .unwrap();
    assert_eq!(report.removed, 2, "the two removable rows are removed");
    assert_eq!(
        report.protected,
        vec![(2, object_lock::RefusalReason::Governance)],
        "the protected sequence is reported with its reason"
    );
    assert!(
        store.get_version(&bucket, &k, 1).await.unwrap().is_none()
            && store.get_version(&bucket, &k, 3).await.unwrap().is_none(),
        "the unprotected and expired rows are gone"
    );
    assert!(
        store.get_version(&bucket, &k, 2).await.unwrap().is_some(),
        "the protected row remains"
    );
    assert!(
        store
            .version_lock(&bucket, &k, Some(&v2))
            .await
            .unwrap()
            .is_some(),
        "the protected row keeps its lock"
    );

    let again = store
        .expire_many_report(&bucket, &k, &[1, 2, 3])
        .await
        .unwrap();
    assert_eq!(again.removed, 0, "a re-run removes nothing");
    assert_eq!(
        again.protected,
        vec![(2, object_lock::RefusalReason::Governance)],
        "a re-run still reports the protected sequence"
    );

    // The old method keeps its signature and discards the report: a protected
    // sequence is skipped, never an error.
    store.expire_many(&bucket, &k, &[2]).await.unwrap();
    assert!(
        store.get_version(&bucket, &k, 2).await.unwrap().is_some(),
        "expire_many skips a protected sequence silently"
    );
    assert!(
        store
            .version_lock(&bucket, &k, Some(&v2))
            .await
            .unwrap()
            .is_some(),
        "the skipped sequence keeps its lock"
    );
    store
        .expire_with(&bucket, &k, 2, object_lock::Bypass::Governance)
        .await
        .unwrap();
}

/// The drain leg: a protected version refuses the drain and removes nothing;
/// once cleared, the drain removes the rows and an orphan lock row, and
/// re-running it is a no-op (OL8).
pub(super) async fn object_lock_drain_leg<V>(store: &V)
where
    V: Metadata + VersionMeta,
    <V as VersionMeta>::Error: From<storage::Error>,
{
    let bucket = bucket_name("conf-lock-drain");
    let k = key("drain-lock.txt");
    let governed = version::Id::new("gd").expect("a valid version id");
    let orphan = version::Id::new("orphan").expect("a valid version id");
    BucketMeta::create(store, &bucket, &bucket_at(t0()))
        .await
        .unwrap();
    store
        .set_bucket_state(&bucket, version::State::Enabled)
        .await
        .unwrap();
    store
        .set_object_lock(&bucket, &period_config(None))
        .await
        .unwrap();
    store
        .put_version(
            &bucket,
            &k,
            &version_row(
                1,
                Some(version::Id::new("free").expect("a valid version id")),
                Kind::Object,
            ),
        )
        .await
        .unwrap();
    store
        .put_version(
            &bucket,
            &k,
            &version_row(2, Some(governed.clone()), Kind::Object),
        )
        .await
        .unwrap();
    store
        .advance_slot(&bucket, &k, Some(governed.clone()), 2)
        .await
        .unwrap();
    store
        .set_retention(
            &bucket,
            &k,
            Some(&governed),
            Some(&governance_reservation()),
            object_lock::Bypass::None,
            SystemTime::now(),
        )
        .await
        .unwrap();
    // The orphan: a lock row whose identity has no version row — the residue
    // only the raw writer can plant (OL8's drain rule).
    store
        .set_version_lock(
            &bucket,
            &k,
            Some(&orphan),
            Some(&object_lock::Lock {
                retention: Some(governance_reservation()),
                legal_hold: object_lock::LegalHold::Off,
            }),
        )
        .await
        .unwrap();

    // The row-less current version: the normal current shape after a write
    // (a slot and a lock, no version row of its own). The drain must refuse it
    // as a protected version — it is not an orphan (OL7/OL8).
    let rowless_key = key("drain-rowless.txt");
    let rowless = version::Id::new("rowless").expect("a valid version id");
    store
        .advance_slot_with(
            &bucket,
            &rowless_key,
            Some(rowless.clone()),
            1,
            Some(&object_lock::Lock {
                retention: Some(governance_reservation()),
                legal_hold: object_lock::LegalHold::Off,
            }),
        )
        .await
        .unwrap();

    assert!(
        matches!(
            VersionMeta::drain_bucket(store, &bucket)
                .await
                .unwrap_err()
                .into(),
            storage::Error::Locked(object_lock::RefusalReason::Governance)
        ),
        "a drain with a protected version is refused"
    );
    assert!(
        store.get_version(&bucket, &k, 1).await.unwrap().is_some()
            && store.get_version(&bucket, &k, 2).await.unwrap().is_some(),
        "the refused drain removed nothing"
    );
    assert!(
        store
            .version_lock(&bucket, &k, Some(&orphan))
            .await
            .unwrap()
            .is_some(),
        "the refused drain kept the orphan lock row"
    );

    store
        .expire_with(&bucket, &k, 2, object_lock::Bypass::Governance)
        .await
        .unwrap();
    assert!(
        matches!(
            VersionMeta::drain_bucket(store, &bucket)
                .await
                .unwrap_err()
                .into(),
            storage::Error::Locked(object_lock::RefusalReason::Governance)
        ),
        "the row-less current version's lock refuses the drain after the row is gone"
    );
    assert!(
        store.get_version(&bucket, &k, 1).await.unwrap().is_some()
            && store
                .version_lock(&bucket, &rowless_key, Some(&rowless))
                .await
                .unwrap()
                .is_some(),
        "the second refusal removed nothing either"
    );

    store
        .expire_with(&bucket, &rowless_key, 1, object_lock::Bypass::Governance)
        .await
        .unwrap();
    VersionMeta::drain_bucket(store, &bucket).await.unwrap();
    assert!(
        store.bucket_rows(&bucket).await.unwrap().is_empty(),
        "the drain removes the remaining rows"
    );
    assert_eq!(
        store
            .version_lock(&bucket, &k, Some(&orphan))
            .await
            .unwrap(),
        None,
        "the drain removes an orphan lock row"
    );
    assert_eq!(
        store
            .version_lock(&bucket, &rowless_key, Some(&rowless))
            .await
            .unwrap(),
        None,
        "the row-less current version's lock leaves with its bypassed removal"
    );
    VersionMeta::drain_bucket(store, &bucket).await.unwrap();
    assert!(
        store.bucket_rows(&bucket).await.unwrap().is_empty(),
        "a second drain is a no-op"
    );
}

/// The upload-intent leg: the lock is written atomically with the upload row,
/// reads back, survives a re-create with a lock, and clears idempotently.
pub(super) async fn object_lock_upload_leg<V>(store: &V)
where
    V: Metadata + UploadMeta,
{
    let bucket = bucket_name("conf-lock-upload");
    BucketMeta::create(store, &bucket, &bucket_at(t0()))
        .await
        .unwrap();
    let row = upload::Row::from_values(
        "upload-1",
        "big.bin",
        t0(),
        object::Tags::empty(),
        Acl::default_private(None),
    )
    .expect("a valid upload row");

    assert_eq!(
        store.upload_lock(&bucket, "upload-1").await.unwrap(),
        None,
        "an upload with no intent answers None"
    );
    let intent = object_lock::Lock {
        retention: Some(governance_reservation()),
        legal_hold: object_lock::LegalHold::Off,
    };
    store
        .create_with_lock(&bucket, "upload-1", &row, Some(&intent))
        .await
        .unwrap();
    assert_eq!(
        store.upload_lock(&bucket, "upload-1").await.unwrap(),
        Some(intent.clone()),
        "the intent reads back"
    );
    assert!(
        UploadMeta::get(
            store,
            &bucket,
            &object::key("big.bin").expect("a valid key"),
            "upload-1",
        )
        .await
        .unwrap()
        .is_some(),
        "the upload row was written in the same call"
    );

    // A re-create with no intent clears the stored one; a re-create with an
    // intent replaces it.
    store
        .create_with_lock(&bucket, "upload-1", &row, None)
        .await
        .unwrap();
    assert_eq!(
        store.upload_lock(&bucket, "upload-1").await.unwrap(),
        None,
        "a re-create without an intent stores none"
    );
    store
        .create_with_lock(&bucket, "upload-1", &row, Some(&intent))
        .await
        .unwrap();
    store.clear_upload_lock(&bucket, "upload-1").await.unwrap();
    assert_eq!(
        store.upload_lock(&bucket, "upload-1").await.unwrap(),
        None,
        "clearing removes the intent"
    );
    store.clear_upload_lock(&bucket, "upload-1").await.unwrap();
    assert_eq!(
        store.upload_lock(&bucket, "never-existed").await.unwrap(),
        None,
        "an absent upload answers no intent, idempotently"
    );
}
