//! Object Lock's conformance legs: the bucket configuration, the per-version
//! retention and legal hold, the delete enforcement, the batch report, the
//! drain's protection, and the multipart upload intent.
//!
//! Three entry points — a backend calls the ones it implements:
//!
//! - [`assert_object_lock_conformance`] — Object Lock's bucket configuration,
//!   per-version retention/legal hold (current, history and null), the delete
//!   enforcement with the governance bypass, compliance/legal-hold/expiry,
//!   `expire_many_report`, and the drain's protected refusal and orphan
//!   removal. A **new entry point**, not folded into
//!   [`super::metadata::assert_version_conformance`] (R17): the lock tables
//!   land in the stores after the contract.
//! - [`assert_object_lock_upload_conformance`] — the multipart lock intent
//!   written atomically with the upload row, its replacement and its clear.
//! - [`storage::assert_storage_object_lock`] — the Storage-level journeys
//!   (current and null deletes, a marker's lock row, the governance bypass,
//!   default-retention resolution, the versioned commit, copy, and the
//!   all-or-none create). Not folded into [`assert_object_lock_conformance`]:
//!   that suite is `Metadata + VersionMeta`, and these journeys drive `Storage`.
//!
//! The corrupt-row legs stay per-store — only the backend can plant a row its
//! decoder rejects.

use std::time::{Duration, SystemTime};

pub use super::fixtures::reservation;
use super::fixtures::{bucket_at, bucket_name, governance_reservation, key, t0, version_row};
use crate::_core::{
    meta::{
        BucketMeta, Metadata, UploadMeta,
        version::{Kind, VersionMeta},
    },
    object_lock, storage as core_storage, version,
};

mod enforcement;
pub mod storage;

/// Assert the Object Lock contract's bucket-configuration, per-version,
/// delete, batch and drain rules (OL4–OL8, OL21).
///
/// A **new entry point**, deliberately not part of
/// [`super::metadata::assert_version_conformance`] (R17): the stores wire it
/// from their own test files once their lock tables land. The corrupt-row legs
/// stay per-store — only the backend can plant a row its decoder rejects.
///
/// # Panics
///
/// On the first violated rule; the message names the rule.
pub async fn assert_object_lock_conformance<V>(store: &V)
where
    V: Metadata + VersionMeta,
    <V as VersionMeta>::Error: From<core_storage::Error>,
{
    object_lock_bucket_leg(store).await;
    object_lock_version_leg(store).await;
    enforcement::object_lock_delete_leg(store).await;
    enforcement::object_lock_batch_leg(store).await;
    enforcement::object_lock_drain_leg(store).await;
}

/// Assert the multipart lock-intent contract (OL6, OL21).
///
/// A **new entry point** for the same reason as
/// [`assert_object_lock_conformance`] (R17).
///
/// # Panics
///
/// On the first violated rule; the message names the rule.
pub async fn assert_object_lock_upload_conformance<V>(store: &V)
where
    V: Metadata + UploadMeta,
{
    enforcement::object_lock_upload_leg(store).await;
}

/// A compliance retention, with the same safety margin.
pub(super) fn compliance_reservation() -> object_lock::Retention {
    reservation(object_lock::Mode::Compliance, Duration::from_secs(3_600))
}

/// An already-expired governance retention: inert to every rule (OL11).
pub(super) fn expired_reservation() -> object_lock::Retention {
    object_lock::Retention {
        mode: object_lock::Mode::Governance,
        retain_until: SystemTime::now() - Duration::from_secs(3_600),
    }
}

/// One enabled lock configuration with an optional default rule.
pub fn lock_config(rule: Option<object_lock::Rule>) -> object_lock::Config {
    object_lock::Config {
        enabled: true,
        rule,
    }
}

/// An enabled configuration whose default retention is `mode` over `days`
/// days — the fixture the backends' default-retention tests share.
pub fn default_rule(mode: object_lock::Mode, days: u32) -> object_lock::Config {
    lock_config(Some(object_lock::Rule {
        default_retention: object_lock::DefaultRetention {
            mode,
            period: object_lock::Period::Days(days),
        },
    }))
}

/// One enabled lock configuration whose default retention is a governance
/// period.
pub(super) fn period_config(period: Option<object_lock::Period>) -> object_lock::Config {
    lock_config(period.map(|period| object_lock::Rule {
        default_retention: object_lock::DefaultRetention {
            mode: object_lock::Mode::Governance,
            period,
        },
    }))
}

/// The bucket-metadata leg: the configuration round-trips, decodes to no rule
/// for an enabled bit only, clears idempotently, and reports a missing bucket
/// the way the lifecycle read does.
async fn object_lock_bucket_leg<V>(store: &V)
where
    V: Metadata + VersionMeta,
{
    let bucket = bucket_name("conf-lock-bucket");
    BucketMeta::create(store, &bucket, &bucket_at(t0()))
        .await
        .unwrap();
    // OL13: a configuration write requires versioning `Enabled`.
    VersionMeta::set_bucket_state(store, &bucket, version::State::Enabled)
        .await
        .unwrap();

    assert_eq!(
        store.object_lock(&bucket).await.unwrap(),
        None,
        "a bucket that stores no lock configuration answers None"
    );
    let configured = period_config(Some(object_lock::Period::Days(30)));
    assert!(
        store.set_object_lock(&bucket, &configured).await.unwrap(),
        "a recorded bucket accepts the configuration"
    );
    assert_eq!(
        store.object_lock(&bucket).await.unwrap(),
        Some(configured.clone()),
        "the configuration round-trips — the enabled bit and the rule"
    );
    assert_eq!(
        store.object_lock(&bucket).await.unwrap(),
        Some(object_lock::Config::from_rule_wire(
            true,
            &configured.rule_wire()
        )),
        "the stored configuration is the model's rule-wire round trip"
    );

    let enabled_only = period_config(None);
    assert!(
        store.set_object_lock(&bucket, &enabled_only).await.unwrap(),
        "a rule-less configuration is stored too"
    );
    assert_eq!(
        store.object_lock(&bucket).await.unwrap(),
        Some(enabled_only),
        "an enabled bit with no rule reads back with no rule"
    );

    assert!(
        store.clear_object_lock(&bucket).await.unwrap(),
        "a recorded bucket clears its configuration"
    );
    assert_eq!(
        store.object_lock(&bucket).await.unwrap(),
        None,
        "the clear removes the configuration"
    );
    assert!(
        store.clear_object_lock(&bucket).await.unwrap(),
        "clearing again is idempotent and still presence"
    );

    let missing = bucket_name("conf-lock-bucket-missing");
    assert!(
        matches!(
            store.object_lock(&missing).await.unwrap_err().into(),
            core_storage::Error::NoSuchBucket(_)
        ),
        "a configuration read on a missing bucket is NoSuchBucket"
    );
    assert!(
        !store.set_object_lock(&missing, &configured).await.unwrap(),
        "a configuration write on a missing bucket writes nothing"
    );
    assert!(
        !store.clear_object_lock(&missing).await.unwrap(),
        "a clear on a missing bucket answers no presence"
    );
}

/// The identity leg: a lock on a named version, on the null version, and on a
/// history version; the write-path guards (configuration and versioning); the
/// legal-hold preservation and the governance bypass.
async fn object_lock_version_leg<V>(store: &V)
where
    V: Metadata + VersionMeta,
{
    let bucket = bucket_name("conf-lock-version");
    let k = key("locked.txt");
    let v1 = version::Id::new("v1").expect("a valid version id");
    let v2 = version::Id::new("v2").expect("a valid version id");
    BucketMeta::create(store, &bucket, &bucket_at(t0()))
        .await
        .unwrap();
    store
        .set_bucket_state(&bucket, version::State::Enabled)
        .await
        .unwrap();
    store
        .set_object_lock(&bucket, &period_config(Some(object_lock::Period::Days(1))))
        .await
        .unwrap();
    store
        .put_version(&bucket, &k, &version_row(1, Some(v1.clone()), Kind::Object))
        .await
        .unwrap();
    store
        .put_version(&bucket, &k, &version_row(2, None, Kind::Object))
        .await
        .unwrap();
    store
        .put_version(&bucket, &k, &version_row(3, Some(v2.clone()), Kind::Marker))
        .await
        .unwrap();
    store
        .advance_slot(&bucket, &k, Some(v1.clone()), 1)
        .await
        .unwrap();

    assert_eq!(
        store.version_lock(&bucket, &k, Some(&v1)).await.unwrap(),
        None,
        "a version with no lock row answers None"
    );
    assert_eq!(
        store
            .get_object_legal_hold(&bucket, &k, Some(&v1))
            .await
            .unwrap(),
        object_lock::LegalHold::Off,
        "a version with no lock row has no legal hold"
    );

    let reservation = governance_reservation();
    store
        .set_retention(
            &bucket,
            &k,
            Some(&v1),
            Some(&reservation),
            object_lock::Bypass::None,
            SystemTime::now(),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .get_object_retention(&bucket, &k, Some(&v1))
            .await
            .unwrap(),
        Some(reservation),
        "the retention reads back through the retention read"
    );
    assert_eq!(
        store.version_lock(&bucket, &k, Some(&v1)).await.unwrap(),
        Some(object_lock::Lock {
            retention: Some(reservation),
            legal_hold: object_lock::LegalHold::Off,
        }),
        "the lock reads back whole"
    );

    let held = store
        .set_legal_hold(&bucket, &k, Some(&v1), object_lock::LegalHold::On)
        .await
        .unwrap();
    assert_eq!(
        held.retention,
        Some(reservation),
        "the hold preserves the retention"
    );
    assert_eq!(
        store
            .get_object_legal_hold(&bucket, &k, Some(&v1))
            .await
            .unwrap(),
        object_lock::LegalHold::On,
        "the legal hold reads back through the hold read"
    );

    assert!(
        matches!(
            store
                .set_retention(
                    &bucket,
                    &k,
                    Some(&v1),
                    None,
                    object_lock::Bypass::None,
                    SystemTime::now(),
                )
                .await
                .unwrap_err()
                .into(),
            core_storage::Error::Locked(object_lock::RefusalReason::Governance)
        ),
        "clearing an unexpired governance retention without the bypass is refused"
    );
    store
        .set_retention(
            &bucket,
            &k,
            Some(&v1),
            None,
            object_lock::Bypass::Governance,
            SystemTime::now(),
        )
        .await
        .unwrap();
    assert_eq!(
        store.version_lock(&bucket, &k, Some(&v1)).await.unwrap(),
        Some(object_lock::Lock {
            retention: None,
            legal_hold: object_lock::LegalHold::On,
        }),
        "the bypass clears the retention and never changes the legal hold"
    );
    store
        .set_legal_hold(&bucket, &k, Some(&v1), object_lock::LegalHold::Off)
        .await
        .unwrap();
    assert_eq!(
        store.version_lock(&bucket, &k, Some(&v1)).await.unwrap(),
        None,
        "a lock with neither a retention nor a hold is not a lock row"
    );

    // A lock follows its identity from current to history: a slot advance
    // with no lock never clears one (OL7).
    let v3 = version::Id::new("v3").expect("a valid version id");
    store
        .set_retention(
            &bucket,
            &k,
            Some(&v1),
            Some(&reservation),
            object_lock::Bypass::None,
            SystemTime::now(),
        )
        .await
        .unwrap();
    store
        .put_version(&bucket, &k, &version_row(4, Some(v3.clone()), Kind::Object))
        .await
        .unwrap();
    store
        .advance_slot_with(&bucket, &k, Some(v3), 4, None)
        .await
        .unwrap();
    assert_eq!(
        store.version_lock(&bucket, &k, Some(&v1)).await.unwrap(),
        Some(object_lock::Lock {
            retention: Some(reservation),
            legal_hold: object_lock::LegalHold::Off,
        }),
        "the lock follows the identity into history and advance_slot_with(None) keeps it"
    );

    // The null version's lock: the literal `null` identity, addressed as
    // `None` (OL5).
    store.advance_slot(&bucket, &k, None, 2).await.unwrap();
    store
        .set_legal_hold(&bucket, &k, None, object_lock::LegalHold::On)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_object_legal_hold(&bucket, &k, None)
            .await
            .unwrap(),
        object_lock::LegalHold::On,
        "the null version carries its own lock"
    );

    // Identity refusals: an unknown version, a delete marker, and the null
    // identity on a key that has no row at all. The contract pins all three
    // as `NoSuchVersion`.
    let unknown = version::Id::new("never-written").expect("a valid version id");
    assert!(
        matches!(
            store
                .set_retention(
                    &bucket,
                    &k,
                    Some(&unknown),
                    Some(&reservation),
                    object_lock::Bypass::None,
                    SystemTime::now(),
                )
                .await
                .unwrap_err()
                .into(),
            core_storage::Error::NoSuchVersion(_)
        ),
        "a lock write on an unknown version is NoSuchVersion"
    );
    assert!(
        matches!(
            store
                .set_retention(
                    &bucket,
                    &k,
                    Some(&v2),
                    Some(&reservation),
                    object_lock::Bypass::None,
                    SystemTime::now(),
                )
                .await
                .unwrap_err()
                .into(),
            core_storage::Error::NoSuchVersion(_)
        ),
        "a lock write on a delete marker is NoSuchVersion"
    );
    let absent = key("absent.txt");
    assert!(
        matches!(
            store
                .set_retention(
                    &bucket,
                    &absent,
                    None,
                    Some(&reservation),
                    object_lock::Bypass::None,
                    SystemTime::now(),
                )
                .await
                .unwrap_err()
                .into(),
            core_storage::Error::NoSuchVersion(_)
        ),
        "a lock write on an absent key with the null identity is NoSuchVersion"
    );

    // The two write-path preconditions: an enabled configuration and Enabled
    // versioning, both asserted inside the write (OL5, OL13).
    let plain = bucket_name("conf-lock-unconfigured");
    BucketMeta::create(store, &plain, &bucket_at(t0()))
        .await
        .unwrap();
    store
        .set_bucket_state(&plain, version::State::Enabled)
        .await
        .unwrap();
    store
        .put_version(&plain, &k, &version_row(1, Some(v1.clone()), Kind::Object))
        .await
        .unwrap();
    store
        .advance_slot(&plain, &k, Some(v1.clone()), 1)
        .await
        .unwrap();
    assert!(
        matches!(
            store
                .set_retention(
                    &plain,
                    &k,
                    Some(&v1),
                    Some(&reservation),
                    object_lock::Bypass::None,
                    SystemTime::now(),
                )
                .await
                .unwrap_err()
                .into(),
            core_storage::Error::InvalidBucketState(_)
        ),
        "a lock write on a bucket with no configuration is InvalidBucketState"
    );
    store
        .set_object_lock(&plain, &period_config(None))
        .await
        .unwrap();
    store
        .set_bucket_state(&plain, version::State::Suspended)
        .await
        .unwrap();
    assert!(
        matches!(
            store
                .set_retention(
                    &plain,
                    &k,
                    Some(&v1),
                    Some(&reservation),
                    object_lock::Bypass::None,
                    SystemTime::now(),
                )
                .await
                .unwrap_err()
                .into(),
            core_storage::Error::InvalidBucketState(_)
        ),
        "a lock write on a suspended bucket is InvalidBucketState"
    );
}
