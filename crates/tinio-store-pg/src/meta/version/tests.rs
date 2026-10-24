use std::time::{Duration, SystemTime};

use tinio_util::testing::metadata::reservation;
use tokio::time::sleep;

use super::KeyVersions;
use crate::{
    _core::{
        ETag,
        acl::Acl,
        blob::{BlobRef, BlockHash},
        bucket,
        meta::{
            BucketMeta, ObjectMeta, VersionMeta,
            object::Row as ObjectRow,
            version::{Expired, Kind, Stored},
        },
        object, object_lock, storage,
        version::{Entry, Id, IdMarker, ListParams, State, paginate, wire_of},
    },
    Error, PgStore, begin,
    tests::fixtures::{bucket_row, key, name, scratch},
    timestamp::{unix_micros, unix_seconds as at},
};

/// Plant one raw lock row — the only writer that can make a corrupt row is
/// the test's own SQL, so it lives here and not in the shared suite (OL21).
async fn plant_lock(
    store: &PgStore,
    bucket: &bucket::Name,
    target: &object::Key,
    wire: &str,
    mode: Option<&str>,
    retain_until: Option<i64>,
    legal_hold: Option<&str>,
) {
    sqlx::query(
        "INSERT INTO version_object_lock (bucket, key, version_id, mode, retain_until, legal_hold) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(bucket.as_ref())
    .bind(target.as_ref())
    .bind(wire)
    .bind(mode)
    .bind(retain_until)
    .bind(legal_hold)
    .execute(store.as_driver())
    .await
    .expect("the raw lock row");
}

/// The lock decode is fail-closed on every bad arm (OL5, OL10): a corrupt
/// mode, a mode with no date, a date with no mode and a corrupt hold all
/// answer `Corrupt`, never "unlocked", and the delete that reads one
/// refuses and removes nothing.
#[tokio::test]
async fn a_corrupt_or_partial_lock_row_fails_closed() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-lock-corrupt");
    bucket_row(&store, &data).await;
    let target = key("corrupt.txt");
    let v1 = Id::new("v1").expect("a valid version id");
    let corrupt = |err: Error| matches!(storage::Error::from(err), storage::Error::Corrupt(_));

    // A mode outside the closed set.
    plant_lock(&store, &data, &target, "v1", Some("BAD"), None, Some("OFF")).await;
    assert!(corrupt(
        VersionMeta::version_lock(&store, &data, &target, Some(&v1))
            .await
            .unwrap_err()
    ));
    // A date with no mode is the partial pair, read conservatively as
    // compliance. (A bigint is always a representable instant on these
    // hosts, so this is the date column's corrupt arm.)
    sqlx::query(
        "UPDATE version_object_lock SET mode = NULL, retain_until = 123 \
         WHERE bucket = $1 AND key = $2 AND version_id = 'v1'",
    )
    .bind(data.as_ref())
    .bind(target.as_ref())
    .execute(store.as_driver())
    .await
    .unwrap();
    assert!(corrupt(
        VersionMeta::get_object_retention(&store, &data, &target, Some(&v1))
            .await
            .unwrap_err()
    ));
    // A mode with no date is the other half of the pair.
    sqlx::query(
        "UPDATE version_object_lock SET mode = 'GOVERNANCE', retain_until = NULL \
         WHERE bucket = $1 AND key = $2 AND version_id = 'v1'",
    )
    .bind(data.as_ref())
    .bind(target.as_ref())
    .execute(store.as_driver())
    .await
    .unwrap();
    assert!(corrupt(
        VersionMeta::get_object_legal_hold(&store, &data, &target, Some(&v1))
            .await
            .unwrap_err()
    ));
    // A hold outside ON/OFF.
    sqlx::query(
        "UPDATE version_object_lock SET mode = NULL, retain_until = NULL, legal_hold = 'MAYBE' \
         WHERE bucket = $1 AND key = $2 AND version_id = 'v1'",
    )
    .bind(data.as_ref())
    .bind(target.as_ref())
    .execute(store.as_driver())
    .await
    .unwrap();
    assert!(corrupt(
        VersionMeta::version_lock(&store, &data, &target, Some(&v1))
            .await
            .unwrap_err()
    ));

    // The delete reads the row too: a corrupt lock on the current version
    // fails the delete closed, and the row stays.
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 1))
        .await
        .unwrap();
    VersionMeta::advance_slot(&store, &data, &target, Some(v1.clone()), 1)
        .await
        .unwrap();
    assert!(corrupt(
        VersionMeta::expire_with(&store, &data, &target, 1, object_lock::Bypass::Governance)
            .await
            .unwrap_err()
    ));
    assert!(
        VersionMeta::get_version(&store, &data, &target, 1)
            .await
            .unwrap()
            .is_some(),
        "the refused delete removed nothing"
    );
    assert!(corrupt(
        VersionMeta::drain_bucket(&store, &data).await.unwrap_err()
    ));
    assert!(
        VersionMeta::get_version(&store, &data, &target, 1)
            .await
            .unwrap()
            .is_some(),
        "the refused drain removed nothing"
    );
    db.drop_db().await;
}

/// OL8's drain: a protected version refuses and removes nothing (an
/// orphan lock row included), and after the protection is gone the drain
/// removes the rows and the orphan.
#[tokio::test]
async fn the_drain_refuses_a_protected_version_and_removes_an_orphan() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-lock-drain");
    bucket_row(&store, &data).await;
    let target = key("drain-raw.txt");
    let v1 = Id::new("v1").expect("a valid version id");
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 1))
        .await
        .unwrap();
    VersionMeta::advance_slot(&store, &data, &target, Some(v1.clone()), 1)
        .await
        .unwrap();
    let until = Some(
        i64::try_from(storage::to_nanos(
            reservation(object_lock::Mode::Governance, Duration::from_secs(3_600)).retain_until,
        ))
        .unwrap(),
    );
    plant_lock(
        &store,
        &data,
        &target,
        "v1",
        Some("GOVERNANCE"),
        until,
        Some("OFF"),
    )
    .await;
    // The orphan: a lock row whose identity has neither a version row nor
    // the current slot.
    plant_lock(
        &store,
        &data,
        &target,
        "orphan",
        Some("GOVERNANCE"),
        until,
        Some("OFF"),
    )
    .await;

    assert!(matches!(
        storage::Error::from(VersionMeta::drain_bucket(&store, &data).await.unwrap_err()),
        storage::Error::Locked(object_lock::RefusalReason::Governance)
    ));
    assert!(
        VersionMeta::get_version(&store, &data, &target, 1)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        VersionMeta::version_lock(
            &store,
            &data,
            &target,
            Some(&Id::new("orphan").expect("a valid version id"))
        )
        .await
        .unwrap()
        .is_some(),
        "the refused drain kept the orphan"
    );
    VersionMeta::expire_with(&store, &data, &target, 1, object_lock::Bypass::Governance)
        .await
        .unwrap();
    VersionMeta::drain_bucket(&store, &data).await.unwrap();
    assert!(
        VersionMeta::bucket_rows(&store, &data)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        VersionMeta::version_lock(
            &store,
            &data,
            &target,
            Some(&Id::new("orphan").expect("a valid version id"))
        )
        .await
        .unwrap(),
        None,
        "the drain removes an orphan lock row"
    );
    db.drop_db().await;
}

/// OL7: a null in-place replacement of an identity that already owns an
/// active lock is refused before any write; an ordinary overwrite with a
/// new identity leaves the old lock intact.
#[tokio::test]
async fn a_null_in_place_replacement_of_a_locked_identity_is_refused() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-lock-null-overwrite");
    bucket_row(&store, &data).await;
    let target = key("null-overwrite.txt");
    let v1 = Id::new("v1").expect("a valid version id");
    let v2 = Id::new("v2").expect("a valid version id");
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 1))
        .await
        .unwrap();
    let reserved = reservation(object_lock::Mode::Governance, Duration::from_secs(3_600));
    VersionMeta::advance_slot_with(
        &store,
        &data,
        &target,
        Some(v1.clone()),
        1,
        Some(&object_lock::Lock {
            retention: Some(reserved),
            legal_hold: object_lock::LegalHold::Off,
        }),
    )
    .await
    .unwrap();
    assert!(matches!(
        storage::Error::from(
            VersionMeta::advance_slot_with(&store, &data, &target, None, 2, None)
                .await
                .unwrap_err()
        ),
        storage::Error::Locked(object_lock::RefusalReason::Governance)
    ));
    assert_eq!(
        VersionMeta::current_slot(&store, &data, &target)
            .await
            .unwrap()
            .expect("the slot")
            .version,
        Some(v1.clone()),
        "the refused overwrite moved nothing"
    );
    // An ordinary overwrite creates a new identity and leaves the old
    // lock in place (OL7/OL12).
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v2), 3))
        .await
        .unwrap();
    VersionMeta::advance_slot_with(&store, &data, &target, Some(v2), 3, None)
        .await
        .unwrap();
    assert_eq!(
        VersionMeta::version_lock(&store, &data, &target, Some(&v1))
            .await
            .unwrap(),
        Some(object_lock::Lock {
            retention: Some(reserved),
            legal_hold: object_lock::LegalHold::Off,
        }),
        "the old identity keeps its lock"
    );
    db.drop_db().await;
}

/// OL13's write-skew guard: a retention write takes the bucket row's lock
/// **before** it reads the configuration and the versioning state, so it
/// waits behind a concurrent bucket-row writer rather than deciding from a
/// snapshot taken before that writer committed.
#[tokio::test]
async fn a_lock_write_waits_on_the_bucket_row_before_it_reads_the_configuration() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-lock-bucket-wait");
    bucket_row(&store, &data).await;
    let target = key("waited.txt");
    let v1 = Id::new("v1").expect("a valid version id");
    VersionMeta::set_bucket_state(&store, &data, State::Enabled)
        .await
        .unwrap();
    BucketMeta::set_object_lock(
        &store,
        &data,
        &object_lock::Config {
            enabled: true,
            rule: None,
        },
    )
    .await
    .unwrap();
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 1))
        .await
        .unwrap();
    VersionMeta::advance_slot(&store, &data, &target, Some(v1.clone()), 1)
        .await
        .unwrap();

    let mut tx = begin(store.pool()).await.unwrap();
    sqlx::query("SELECT 1 FROM buckets WHERE name = $1 FOR UPDATE")
        .bind(data.as_ref())
        .execute(&mut *tx)
        .await
        .unwrap();
    let reserved = reservation(object_lock::Mode::Governance, Duration::from_secs(3_600));
    let racing = store.clone();
    let bucket = data.clone();
    let racing_key = target.clone();
    let task = tokio::spawn(async move {
        VersionMeta::set_retention(
            &racing,
            &bucket,
            &racing_key,
            Some(&Id::new("v1").expect("a valid version id")),
            Some(&reserved),
            object_lock::Bypass::None,
            SystemTime::now(),
        )
        .await
    });
    let mut saw_waiter = false;
    for _ in 0..50 {
        let waiting: i64 =
            sqlx::query_scalar(r#"SELECT count(*) AS "waiting!" FROM pg_locks WHERE NOT granted"#)
                .fetch_one(store.as_driver())
                .await
                .unwrap();
        if waiting > 0 {
            saw_waiter = true;
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }
    assert!(
        saw_waiter,
        "set_retention never waited on the bucket row's lock"
    );
    tx.commit().await.unwrap();
    task.await.expect("the retention task").unwrap();
    assert_eq!(
        VersionMeta::get_object_retention(&store, &data, &target, Some(&v1))
            .await
            .unwrap(),
        Some(reserved),
        "the write landed after the bucket row's lock was released"
    );
    db.drop_db().await;
}

/// The combined lock update is one transaction: a refused retention leaves
/// the legal-hold member unapplied and the stored row unchanged (OL23), while
/// an accepted update applies both members at once.
#[tokio::test]
async fn a_refused_retention_leaves_the_legal_hold_unapplied() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-lock-atomic");
    bucket_row(&store, &data).await;
    let target = key("atomic.txt");
    let v1 = Id::new("v1").expect("a valid version id");
    VersionMeta::set_bucket_state(&store, &data, State::Enabled)
        .await
        .unwrap();
    BucketMeta::set_object_lock(
        &store,
        &data,
        &object_lock::Config {
            enabled: true,
            rule: None,
        },
    )
    .await
    .unwrap();
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 1))
        .await
        .unwrap();
    VersionMeta::advance_slot(&store, &data, &target, Some(v1.clone()), 1)
        .await
        .unwrap();
    let now = SystemTime::now();
    let compliance = reservation(object_lock::Mode::Compliance, Duration::from_secs(3_600));
    VersionMeta::set_lock(
        &store,
        &data,
        &target,
        Some(&v1),
        Some(Some(&compliance)),
        None,
        object_lock::Bypass::None,
        now,
    )
    .await
    .unwrap();
    let before = VersionMeta::version_lock(&store, &data, &target, Some(&v1))
        .await
        .unwrap()
        .expect("the lock row");
    assert_eq!(before.legal_hold, object_lock::LegalHold::Off);

    // A compliance shortening is refused even with the governance bypass; the
    // legal-hold member present in the same call never lands.
    let shorter = reservation(object_lock::Mode::Compliance, Duration::from_secs(1_800));
    let refused = VersionMeta::set_lock(
        &store,
        &data,
        &target,
        Some(&v1),
        Some(Some(&shorter)),
        Some(object_lock::LegalHold::On),
        object_lock::Bypass::Governance,
        now,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            storage::Error::from(refused),
            storage::Error::Locked(object_lock::RefusalReason::Compliance)
        ),
        "the compliance retention refuses the update"
    );
    assert_eq!(
        VersionMeta::version_lock(&store, &data, &target, Some(&v1))
            .await
            .unwrap(),
        Some(before),
        "the refused update wrote neither member"
    );
    assert_eq!(
        VersionMeta::get_object_legal_hold(&store, &data, &target, Some(&v1))
            .await
            .unwrap(),
        object_lock::LegalHold::Off,
        "the hold was not applied"
    );

    // An accepted combined update applies the extended retention and the hold.
    let later = reservation(object_lock::Mode::Compliance, Duration::from_secs(7_200));
    let updated = VersionMeta::set_lock(
        &store,
        &data,
        &target,
        Some(&v1),
        Some(Some(&later)),
        Some(object_lock::LegalHold::On),
        object_lock::Bypass::None,
        now,
    )
    .await
    .unwrap()
    .expect("both members are facts");
    assert_eq!(updated.retention, Some(later));
    assert_eq!(updated.legal_hold, object_lock::LegalHold::On);
    assert_eq!(
        VersionMeta::version_lock(&store, &data, &target, Some(&v1))
            .await
            .unwrap(),
        Some(updated),
        "the one transaction stored both members"
    );
    db.drop_db().await;
}

/// R21: the guarded versioning write refuses `Suspended` while an enabled
/// Object Lock configuration exists and leaves the state unchanged; with
/// no enabled configuration the suspend lands.
#[tokio::test]
async fn the_guarded_state_write_refuses_a_suspend_over_an_enabled_lock_config() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-version-guarded-suspend");
    bucket_row(&store, &data).await;
    VersionMeta::set_bucket_state(&store, &data, State::Enabled)
        .await
        .unwrap();
    BucketMeta::set_object_lock(
        &store,
        &data,
        &object_lock::Config {
            enabled: true,
            rule: None,
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        storage::Error::from(
            VersionMeta::set_bucket_state_guarded(&store, &data, State::Suspended)
                .await
                .unwrap_err()
        ),
        storage::Error::InvalidBucketState(_)
    ));
    assert_eq!(
        VersionMeta::bucket_state(&store, &data).await.unwrap(),
        State::Enabled,
        "the refused suspend changed nothing"
    );
    // A literally absent configuration does not refuse the suspend either:
    // the guard keys on an enabled row, not on the bucket's lock history.
    let absent = name("plane-version-guarded-suspend-absent");
    bucket_row(&store, &absent).await;
    VersionMeta::set_bucket_state(&store, &absent, State::Enabled)
        .await
        .unwrap();
    assert_eq!(
        BucketMeta::object_lock(&store, &absent).await.unwrap(),
        None,
        "the bucket stores no configuration at all"
    );
    VersionMeta::set_bucket_state_guarded(&store, &absent, State::Suspended)
        .await
        .unwrap();
    assert_eq!(
        VersionMeta::bucket_state(&store, &absent).await.unwrap(),
        State::Suspended,
        "an absent configuration does not refuse the suspend"
    );
    // A disabled configuration does not refuse the suspend.
    BucketMeta::set_object_lock(
        &store,
        &data,
        &object_lock::Config {
            enabled: false,
            rule: None,
        },
    )
    .await
    .unwrap();
    VersionMeta::set_bucket_state_guarded(&store, &data, State::Suspended)
        .await
        .unwrap();
    assert_eq!(
        VersionMeta::bucket_state(&store, &data).await.unwrap(),
        State::Suspended
    );
    db.drop_db().await;
}

/// OL13's write skew, both directions. A suspend and a configuration
/// write take the same bucket row, so whichever commits second sees the
/// first's result: exactly one refuses, and the stored pair never holds
/// `Suspended` beside an enabled configuration. Before the configuration
/// write checked the state in its own transaction, both could commit.
#[tokio::test]
async fn a_config_write_and_a_suspend_serialize_on_the_bucket_row() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-lock-config-suspend-race");
    bucket_row(&store, &data).await;
    VersionMeta::set_bucket_state(&store, &data, State::Enabled)
        .await
        .unwrap();

    // Hold the bucket row so both writers queue behind the same lock.
    let mut tx = begin(store.pool()).await.unwrap();
    sqlx::query("SELECT 1 FROM buckets WHERE name = $1 FOR UPDATE")
        .bind(data.as_ref())
        .execute(&mut *tx)
        .await
        .unwrap();
    let suspending = store.clone();
    let suspend_bucket = data.clone();
    let suspend = tokio::spawn(async move {
        VersionMeta::set_bucket_state_guarded(&suspending, &suspend_bucket, State::Suspended).await
    });
    let configuring = store.clone();
    let config_bucket = data.clone();
    let config = tokio::spawn(async move {
        BucketMeta::set_object_lock(
            &configuring,
            &config_bucket,
            &object_lock::Config {
                enabled: true,
                rule: None,
            },
        )
        .await
    });
    let mut waiters: i64 = 0;
    for _ in 0..50 {
        waiters =
            sqlx::query_scalar(r#"SELECT count(*) AS "waiting!" FROM pg_locks WHERE NOT granted"#)
                .fetch_one(store.as_driver())
                .await
                .unwrap();
        if waiters >= 2 && !suspend.is_finished() && !config.is_finished() {
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }
    assert!(
        waiters >= 2 && !suspend.is_finished() && !config.is_finished(),
        "both writers wait on the bucket row ({waiters} waiters)"
    );
    tx.commit().await.unwrap();

    let suspend = suspend.await.expect("the suspend task");
    let config = config.await.expect("the configuration task");
    let refusals = usize::from(suspend.is_err()) + usize::from(config.is_err());
    assert_eq!(
        refusals, 1,
        "exactly one of the two refuses: suspend={suspend:?}, config={config:?}"
    );
    let state = VersionMeta::bucket_state(&store, &data).await.unwrap();
    let stored = BucketMeta::object_lock(&store, &data).await.unwrap();
    assert!(
        !(state == State::Suspended && stored.is_some_and(|config| config.enabled)),
        "a suspended bucket never stores an enabled configuration"
    );
    db.drop_db().await;
}

/// The global lock order's shared prefix: the drain and the delete family
/// both take the bucket row first, so the drain's
/// `bucket → locks → rows` half and the delete's
/// `bucket → slot → rows → locks` half cannot form a cycle. The leg holds
/// the bucket row and proves both primitives wait on it, then that both
/// complete (in either order) without a deadlock.
#[tokio::test]
async fn the_delete_family_and_the_drain_serialize_on_the_bucket_row() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-lock-drain-order");
    bucket_row(&store, &data).await;
    let target = key("ordered.txt");
    let v1 = Id::new("v1").expect("a valid version id");
    VersionMeta::set_bucket_state(&store, &data, State::Enabled)
        .await
        .unwrap();
    BucketMeta::set_object_lock(
        &store,
        &data,
        &object_lock::Config {
            enabled: true,
            rule: None,
        },
    )
    .await
    .unwrap();
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 1))
        .await
        .unwrap();
    VersionMeta::advance_slot(&store, &data, &target, Some(v1.clone()), 1)
        .await
        .unwrap();
    VersionMeta::set_version_lock(
        &store,
        &data,
        &target,
        Some(&v1),
        Some(&object_lock::Lock {
            retention: Some(reservation(
                object_lock::Mode::Governance,
                Duration::from_secs(3_600),
            )),
            legal_hold: object_lock::LegalHold::Off,
        }),
    )
    .await
    .unwrap();

    let mut tx = begin(store.pool()).await.unwrap();
    sqlx::query("SELECT 1 FROM buckets WHERE name = $1 FOR UPDATE")
        .bind(data.as_ref())
        .execute(&mut *tx)
        .await
        .unwrap();
    let draining = store.clone();
    let drain_bucket = data.clone();
    let drain =
        tokio::spawn(async move { VersionMeta::drain_bucket(&draining, &drain_bucket).await });
    let expiring = store.clone();
    let expire_bucket = data.clone();
    let expire_key = target.clone();
    let expire = tokio::spawn(async move {
        VersionMeta::expire_with(
            &expiring,
            &expire_bucket,
            &expire_key,
            1,
            object_lock::Bypass::Governance,
        )
        .await
    });
    let mut waiters: i64 = 0;
    for _ in 0..50 {
        waiters =
            sqlx::query_scalar(r#"SELECT count(*) AS "waiting!" FROM pg_locks WHERE NOT granted"#)
                .fetch_one(store.as_driver())
                .await
                .unwrap();
        // Row-lock waiters show up as tuple locks or as transaction-id
        // waits on the locking transaction; the count is the cluster's,
        // so both tasks are also required to still be running.
        if waiters >= 2 && !drain.is_finished() && !expire.is_finished() {
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }
    assert!(
        waiters >= 2 && !drain.is_finished() && !expire.is_finished(),
        "both the drain and the delete wait on the bucket row ({waiters} waiters)"
    );
    tx.commit().await.unwrap();
    // Either order is legal: the drain may refuse the protected version
    // (removing nothing) and the bypassed delete then removes it, or the
    // delete lands first and the drain finds nothing to refuse. Neither
    // may deadlock, and the bypassed delete always succeeds.
    match drain.await.expect("the drain task") {
        Ok(()) => {}
        Err(err) => assert!(
            matches!(
                storage::Error::from(err),
                storage::Error::Locked(object_lock::RefusalReason::Governance)
            ),
            "the drain either succeeds or refuses the protected version"
        ),
    }
    expire.await.expect("the expire task").unwrap();
    assert!(
        VersionMeta::get_version(&store, &data, &target, 1)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        VersionMeta::version_lock(&store, &data, &target, Some(&Id::new("v1").unwrap()))
            .await
            .unwrap(),
        None
    );
    db.drop_db().await;
}

/// The emptiness decision includes version and lock rows (OL8): a bucket
/// holding a version row or an orphan lock row is not empty, and the drain
/// (which removes both) is what makes it deletable.
#[tokio::test]
async fn remove_if_empty_refuses_version_and_lock_rows() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-lock-remove-empty");
    bucket_row(&store, &data).await;
    let target = key("version-only.txt");
    let v1 = Id::new("v1").expect("a valid version id");
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 1))
        .await
        .unwrap();
    assert!(matches!(
        BucketMeta::remove_if_empty(&store, &data).await,
        Err(Error::NotEmpty(_))
    ));
    VersionMeta::drain_bucket(&store, &data).await.unwrap();
    plant_lock(&store, &data, &target, "orphan", None, None, Some("OFF")).await;
    assert!(matches!(
        BucketMeta::remove_if_empty(&store, &data).await,
        Err(Error::NotEmpty(_))
    ));
    VersionMeta::drain_bucket(&store, &data).await.unwrap();
    BucketMeta::remove_if_empty(&store, &data).await.unwrap();
    assert!(
        BucketMeta::created_at(&store, &data)
            .await
            .unwrap()
            .is_none(),
        "an empty bucket, its lock rows drained, deletes"
    );
    db.drop_db().await;
}

fn version(id: Option<&Id>, seq: u64) -> Stored {
    Stored {
        id: id.cloned(),
        blob: BlobRef::single(BlockHash::of(b"plane"), 5),
        size: 5,
        etag: ETag::EMPTY,
        created: at(10),
        became_noncurrent: 0,
        kind: Kind::Object,
        tags: object::Tags::empty(),
        checksum: None,
        owner: None,
        acl: Acl::default_private(None),
        seq,
    }
}

#[test]
fn listing_marker_follows_the_slot_column_when_the_row_is_newer() {
    let row = super::StoredListing {
        key: "k".to_owned(),
        etag: String::new(),
        size: 0,
        mtime: time::OffsetDateTime::UNIX_EPOCH,
        identity: 0,
        tags: object::Tags::empty(),
        checksum: String::new(),
        acl: Acl::default_private(None),
        slot_seq: Some(1),
        version_id: None,
        is_delete_marker: Some(true),
        slot_kind: None,
        row_seq: Some(3),
        id: None,
        blob: None,
        row_size: None,
        row_etag: None,
        created: None,
        became_noncurrent: None,
        row_kind: Some(Kind::Object),
        row_tags: None,
        row_checksum: None,
        row_acl: None,
    };
    let (_, _, marker) = row.current();
    assert!(
        marker,
        "a newer object row does not clear the slot's marker"
    );
}

fn marker(seq: u64) -> Stored {
    Stored {
        kind: Kind::Marker,
        ..version(None, seq)
    }
}

/// The caller's object row for a restore: the marker's own shape — empty
/// content, no file identity.
fn restored_row() -> ObjectRow {
    ObjectRow {
        etag: ETag::EMPTY,
        size: 0,
        mtime: at(10),
        identity: 0,
        tags: object::Tags::empty(),
        checksum: None,
        owner: None,
        acl: Acl::default_private(None),
    }
}

#[tokio::test]
async fn the_creation_time_round_trips_with_sub_second_precision() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-version-micros");
    bucket_row(&store, &data).await;
    let target = key("micros.txt");
    let created = unix_micros(1_700_000_000_222_333);
    let mut row = version(None, 1);
    row.created = created;
    VersionMeta::put_version(&store, &data, &target, &row)
        .await
        .unwrap();
    let got = VersionMeta::get_version(&store, &data, &target, 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        got.created, created,
        "timestamptz keeps the sub-second digits"
    );
    db.drop_db().await;
}

#[tokio::test]
async fn version_rows_round_trip_and_the_index_follows_them() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-version-rows");
    bucket_row(&store, &data).await;
    let target = key("v.txt");
    let v1 = Id::new("v1").expect("a valid version id");
    VersionMeta::set_bucket_state(&store, &data, State::Enabled)
        .await
        .unwrap();
    assert_eq!(
        VersionMeta::bucket_state(&store, &data).await.unwrap(),
        State::Enabled
    );
    assert_eq!(
        VersionMeta::bucket_state(&store, &name("plane-version-absent"))
            .await
            .unwrap(),
        State::Unversioned,
        "a bucket with no state row is unversioned"
    );
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 1))
        .await
        .unwrap();
    let stored = VersionMeta::get_version(&store, &data, &target, 1)
        .await
        .unwrap()
        .expect("the row just written");
    assert_eq!(stored.id.as_ref(), Some(&v1));
    assert_eq!(stored.seq, 1);
    assert_eq!(
        VersionMeta::version_seq_of(&store, &data, &target, Some(&v1))
            .await
            .unwrap(),
        Some(1)
    );
    assert_eq!(
        VersionMeta::latest(&store, &data, &target)
            .await
            .unwrap()
            .map(|(seq, _)| seq),
        Some(1)
    );
    assert_eq!(
        VersionMeta::bucket_rows(&store, &data).await.unwrap().len(),
        1
    );
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 2))
        .await
        .unwrap();
    assert_eq!(
        VersionMeta::list_versions(&store, &data, &target)
            .await
            .unwrap()
            .iter()
            .map(|row| row.seq)
            .collect::<Vec<_>>(),
        vec![2, 1],
        "the history reads newest first"
    );
    assert_eq!(
        VersionMeta::version_seq_of(&store, &data, &target, Some(&v1))
            .await
            .unwrap(),
        Some(2),
        "the index entry followed the row"
    );
    db.drop_db().await;
}

#[tokio::test]
async fn every_bucket_state_label_round_trips() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-version-state");
    bucket_row(&store, &data).await;
    for state in [State::Unversioned, State::Enabled, State::Suspended] {
        VersionMeta::set_bucket_state(&store, &data, state)
            .await
            .unwrap();
        assert_eq!(
            VersionMeta::bucket_state(&store, &data).await.unwrap(),
            state,
            "the bucket_state enum type round-trips {state:?}"
        );
    }
    db.drop_db().await;
}

#[tokio::test]
async fn put_version_refuses_a_different_row_and_preserves_the_stored_stamp() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-version-conflict");
    bucket_row(&store, &data).await;
    let target = key("v.txt");
    let v1 = Id::new("v1").expect("a valid version id");
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 1))
        .await
        .unwrap();
    let original = VersionMeta::get_version(&store, &data, &target, 1)
        .await
        .unwrap()
        .unwrap();
    let mut clash = version(Some(&v1), 1);
    clash.blob = BlobRef::single(BlockHash::of(b"other"), 5);
    assert!(
        matches!(
            VersionMeta::put_version(&store, &data, &target, &clash).await,
            Err(Error::VersionConflict { .. })
        ),
        "a row naming different content at the same sequence is refused"
    );
    assert_eq!(
        VersionMeta::get_version(&store, &data, &target, 1)
            .await
            .unwrap()
            .unwrap(),
        original,
        "the refusal overwrote nothing"
    );
    // The same version rewritten adopts the row and keeps its stamp.
    let mut adoption = version(Some(&v1), 1);
    adoption.became_noncurrent = 77;
    VersionMeta::put_version(&store, &data, &target, &adoption)
        .await
        .unwrap();
    assert_eq!(
        VersionMeta::get_version(&store, &data, &target, 1)
            .await
            .unwrap()
            .unwrap()
            .became_noncurrent,
        0,
        "the stored stamp is the authority"
    );
    assert!(matches!(
        VersionMeta::put_version(&store, &name("plane-version-absent"), &target, &adoption).await,
        Err(Error::NoSuchBucket(_))
    ));
    db.drop_db().await;
}

/// S2/S7's marker arm: the restored marker's bit outlives the row it came
/// from — `restore_slot` consumes that row, so the recorded slot column
/// (the `is_delete_marker` column in `sql/migrations/0001_init.sql`) is what
/// `current_slot` answers from afterwards. The
/// shared suite cannot pin this (a shared leg may not depend on one store's
/// slot shape), so the store's own suite does.
#[tokio::test]
async fn restore_slot_restores_a_marker_and_the_bit_survives_its_row() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-version-restore-marker");
    bucket_row(&store, &data).await;
    let target = key("restore.txt");
    let v1 = Id::new("v1").expect("a valid version id");
    let v3 = Id::new("v3").expect("a valid version id");
    // The successor is a delete marker; the removed current version's row
    // sits above it and the slot names that one.
    let mut successor = version(Some(&v1), 1);
    successor.kind = Kind::Marker;
    successor.blob = BlobRef::empty();
    successor.size = 0;
    VersionMeta::put_version(&store, &data, &target, &successor)
        .await
        .unwrap();
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v3), 3))
        .await
        .unwrap();
    VersionMeta::advance_slot(&store, &data, &target, Some(v3), 3)
        .await
        .unwrap();
    assert!(
        !VersionMeta::current_slot(&store, &data, &target)
            .await
            .unwrap()
            .expect("the slot is recorded")
            .is_delete_marker,
        "the removed current version is an object version"
    );

    VersionMeta::restore_slot(&store, &data, &target, &restored_row(), Some(v1.clone()), 1)
        .await
        .unwrap();

    let slot = VersionMeta::current_slot(&store, &data, &target)
        .await
        .unwrap()
        .expect("the slot is recorded");
    assert_eq!(slot.version.as_ref(), Some(&v1));
    assert!(
        slot.is_delete_marker,
        "the restored marker's bit survives the row it came from"
    );
    assert!(
        VersionMeta::get_version(&store, &data, &target, 1)
            .await
            .unwrap()
            .is_none(),
        "the consumed marker row is gone"
    );
    // S5: the row the slot moved off stopped being current, in the same
    // commit.
    assert_ne!(
        VersionMeta::get_version(&store, &data, &target, 3)
            .await
            .unwrap()
            .expect("the moved-off row stays")
            .became_noncurrent,
        0,
        "the restore stamps the row the slot moves off"
    );
    db.drop_db().await;
}

/// The listing and the point read name one slot. A newer version row does
/// not move the slot, and the bit comes from the row at the slot's sequence
/// even when that row was written after the slot recorded `false`.
#[tokio::test]
async fn listing_reads_the_marker_at_the_slot_not_the_newest_row() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-version-list-marker");
    bucket_row(&store, &data).await;
    let target = key("listed.txt");
    let v1 = Id::new("v1").expect("a valid version id");
    let v3 = Id::new("v3").expect("a valid version id");
    VersionMeta::advance_slot(&store, &data, &target, Some(v1.clone()), 1)
        .await
        .unwrap();
    let mut marker_row = version(Some(&v1), 1);
    marker_row.kind = Kind::Marker;
    marker_row.blob = BlobRef::empty();
    marker_row.size = 0;
    VersionMeta::put_version(&store, &data, &target, &marker_row)
        .await
        .unwrap();
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v3), 3))
        .await
        .unwrap();
    assert!(
        VersionMeta::current_slot(&store, &data, &target)
            .await
            .unwrap()
            .expect("the slot is recorded")
            .is_delete_marker,
        "the row at the slot's sequence is a marker"
    );
    let listed = store.bucket_versions(&data).await.unwrap();
    let key_versions = listed
        .iter()
        .find(|row| row.key == target)
        .expect("the key is listed");
    assert!(
        key_versions.is_delete_marker,
        "the listing uses the slot row, not the newest row"
    );
    db.drop_db().await;
}

#[tokio::test]
async fn advance_slot_materializes_the_object_row_and_never_rewinds() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-version-slot");
    bucket_row(&store, &data).await;
    let target = key("marker.txt");
    let v1 = Id::new("v1").expect("a valid version id");
    let v2 = Id::new("v2").expect("a valid version id");
    VersionMeta::advance_slot(&store, &data, &target, Some(v1.clone()), 3)
        .await
        .unwrap();
    let slot = VersionMeta::current_slot(&store, &data, &target)
        .await
        .unwrap()
        .expect("the slot is recorded");
    assert!(slot.exists);
    assert_eq!(slot.version.as_ref(), Some(&v1));
    assert!(!slot.is_delete_marker);
    let materialized = ObjectMeta::get(&store, &data, &target)
        .await
        .unwrap()
        .expect("the advance materialized the object row");
    assert_eq!(materialized.etag, ETag::EMPTY);
    assert!(
        matches!(
            VersionMeta::advance_slot(&store, &data, &target, Some(v2), 2).await,
            Err(Error::SlotRewind { .. })
        ),
        "a real identity's slot never moves backwards"
    );
    VersionMeta::advance_slot(&store, &data, &target, None, 3)
        .await
        .unwrap();
    assert!(
        VersionMeta::current_slot(&store, &data, &target)
            .await
            .unwrap()
            .unwrap()
            .version
            .is_none(),
        "the same sequence is the crash window's own re-run"
    );
    // A marker at the slot's sequence flips the marker bit.
    VersionMeta::put_version(&store, &data, &target, &marker(3))
        .await
        .unwrap();
    VersionMeta::advance_slot(&store, &data, &target, None, 3)
        .await
        .unwrap();
    assert!(
        VersionMeta::current_slot(&store, &data, &target)
            .await
            .unwrap()
            .unwrap()
            .is_delete_marker
    );
    let absent = name("plane-version-absent");
    assert!(matches!(
        VersionMeta::advance_slot(&store, &absent, &target, None, 1).await,
        Err(Error::NoSuchBucket(_))
    ));
    assert!(matches!(
        VersionMeta::set_bucket_state_guarded(&store, &absent, State::Enabled).await,
        Err(Error::NoSuchBucket(_))
    ));
    db.drop_db().await;
}

/// The 2026-10-10 identity-scoping ruling, on the one path that reaches it
/// in production: a suspended bucket's in-place write to the **null** slot
/// is not a backward move. `plan_write`'s `(Put, Suspended)` arm over a real
/// current version answers `(Identity::Null, seq 0)`, and the engine's
/// advance runs **after** the supersede, the publish and the object-row
/// write have committed — so the pre-ruling guard refused a write that had
/// already taken effect and the scenario answered 500. The contract's
/// `advance_slot` doc states the rule ("An incoming `None` (the null
/// version) is admitted in place whatever the sequence … `(Some(id), n) →
/// (None, 0)` is not a backward move"), redb carries it (`id.is_some()`),
/// and `mark_current_marker` already carried it here. The companion leg
/// pins that admitting the null write does not weaken the guard.
#[tokio::test]
async fn advance_slot_admits_the_null_identitys_in_place_move() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-version-null-in-place");
    bucket_row(&store, &data).await;
    let target = key("suspended.txt");
    let v1 = Id::new("v1").expect("a valid version id");
    let v2 = Id::new("v2").expect("a valid version id");
    // An enabled write's slot, high on the key: a real identity at seq 5,
    // whose row the supersede committed (`became_noncurrent == 0`, the
    // crash window's adopted row).
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 5))
        .await
        .unwrap();
    VersionMeta::advance_slot(&store, &data, &target, Some(v1), 5)
        .await
        .unwrap();
    // The companion first: a lower-sequence move that keeps a **real**
    // identity is still the backward move the guard exists to refuse.
    assert!(
        matches!(
            VersionMeta::advance_slot(&store, &data, &target, Some(v2), 2).await,
            Err(Error::SlotRewind { .. })
        ),
        "a real identity never moves backwards"
    );
    // The suspended bucket's in-place write: the plan's `(Null, 0)` over a
    // real current version lands rather than being refused.
    VersionMeta::advance_slot(&store, &data, &target, None, 0)
        .await
        .expect("the null version's in-place slot move is not a rewind");
    let slot = VersionMeta::current_slot(&store, &data, &target)
        .await
        .unwrap()
        .expect("the slot is recorded");
    assert!(
        slot.exists && slot.version.is_none() && !slot.is_delete_marker,
        "the slot records the null identity: {slot:?}"
    );
    // The in-place move stamps the real version it moved off, in the same
    // commit: the null write supersedes the row at seq 5, so that row is
    // history (redb's own suite pins the same arm).
    assert_ne!(
        VersionMeta::get_version(&store, &data, &target, 5)
            .await
            .unwrap()
            .expect("the version the null write superseded")
            .became_noncurrent,
        0,
        "the null write's in-place move stamps the version it moved off"
    );
    db.drop_db().await;
}

/// The advance stamps the row it moves off (the 2026-10-09 ruling, the rule
/// [`VersionMeta::restore_slot`] shares — RG5). The supersede commits the
/// outgoing version's row with `became_noncurrent == 0` (the crash window's
/// adopted row, not history yet), and the transaction that records the
/// incoming identity stamps it in the **same commit**, so a row that is
/// history says so and its retention clock starts when the version actually
/// stopped being current. redb pins the same rule in its own suite; pg had
/// the restore half (`restore_slot`'s stamp) but not the forward half, and
/// no leg looked — this is the missing one.
#[tokio::test]
async fn advance_slot_stamps_the_row_it_moves_off() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-advance-stamp");
    bucket_row(&store, &data).await;
    let target = key("stamp.txt");
    let v1 = Id::new("v1").expect("a valid version id");
    let v2 = Id::new("v2").expect("a valid version id");
    // Two rows, each committed the way a supersede commits them: the
    // outgoing current version at seq 1, the incoming at seq 2, both with
    // `became_noncurrent == 0`.
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 1))
        .await
        .unwrap();
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v2), 2))
        .await
        .unwrap();
    // The slot records the current version's own row: not history yet.
    VersionMeta::advance_slot(&store, &data, &target, Some(v1), 1)
        .await
        .unwrap();
    assert_eq!(
        VersionMeta::get_version(&store, &data, &target, 1)
            .await
            .unwrap()
            .expect("the current version's own row")
            .became_noncurrent,
        0,
        "the row the slot records is the current version's: not history yet"
    );
    // The advance moves the slot off seq 1: that row stopped being current
    // in this commit, so it carries the stamp. The incoming version's own
    // row at seq 2 does not.
    VersionMeta::advance_slot(&store, &data, &target, Some(v2), 2)
        .await
        .unwrap();
    assert_ne!(
        VersionMeta::get_version(&store, &data, &target, 1)
            .await
            .unwrap()
            .expect("the moved-off row stays")
            .became_noncurrent,
        0,
        "the advance stamps the row it moves off"
    );
    assert_eq!(
        VersionMeta::get_version(&store, &data, &target, 2)
            .await
            .unwrap()
            .expect("the incoming version's row")
            .became_noncurrent,
        0,
        "the incoming version's own row is not history"
    );
    // The same-sequence re-run is the crash window's own commit: it moves
    // off nothing, so the current version's row stays unstamped.
    VersionMeta::advance_slot(&store, &data, &target, Some(Id::new("v2").unwrap()), 2)
        .await
        .unwrap();
    assert_eq!(
        VersionMeta::get_version(&store, &data, &target, 2)
            .await
            .unwrap()
            .expect("the current version's row")
            .became_noncurrent,
        0,
        "a same-sequence re-run does not stamp the current version's row"
    );
    // A move off a sequence holding no row is a no-op, not an error.
    VersionMeta::advance_slot(&store, &data, &target, None, 9)
        .await
        .unwrap();
    assert!(
        VersionMeta::get_version(&store, &data, &target, 9)
            .await
            .unwrap()
            .is_none(),
        "an advance onto a free sequence writes no version row"
    );
    db.drop_db().await;
}

/// The `ObjectMeta::remove`/`remove_many` slot clear: a slot **is** object-row
/// state (the contract's own `advance_slot` rule), so removing a key's row must
/// remove its slot — a row-less slot would make the next advance from below the
/// stale sequence a `SlotRewind`. The shared conformance suite cannot pin this
/// (the slot is one store's shape), so the store's own suite does; `Cleanup`'s
/// batch retirement is the production caller of the batch half.
#[tokio::test]
async fn removing_a_row_leaves_the_key_slot_recorded() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-remove-slot");
    bucket_row(&store, &data).await;
    let target = key("removed.txt");
    VersionMeta::advance_slot(
        &store,
        &data,
        &target,
        Some(Id::new("v1").expect("a valid version id")),
        3,
    )
    .await
    .unwrap();
    assert!(ObjectMeta::remove(&store, &data, &target).await.unwrap());
    // The key's slot is **not** a row removal's to retire (R7): a slot with no
    // object row is state the contract admits, and the calls that retire one
    // are the version plane's own plus the bucket teardown.
    assert!(
        VersionMeta::current_slot(&store, &data, &target)
            .await
            .unwrap()
            .is_some(),
        "removing the object row leaves the key's slot recorded"
    );
    // A later write advances from the sequence that slot names, so it is a
    // forward move and never a rewind.
    VersionMeta::advance_slot(
        &store,
        &data,
        &target,
        Some(Id::new("v2").expect("a valid version id")),
        4,
    )
    .await
    .unwrap();

    // The batch removal leaves its keys' slots recorded too.
    let second = key("removed-too.txt");
    VersionMeta::advance_slot(
        &store,
        &data,
        &second,
        Some(Id::new("v3").expect("a valid version id")),
        5,
    )
    .await
    .unwrap();
    ObjectMeta::remove_many(&store, &data, &[target.clone(), second.clone()])
        .await
        .unwrap();
    for key in [&target, &second] {
        assert!(
            VersionMeta::current_slot(&store, &data, key)
                .await
                .unwrap()
                .is_some(),
            "a batch removal leaves every key's slot recorded"
        );
    }
    db.drop_db().await;
}

#[tokio::test]
async fn the_null_identity_displaces_the_older_null_row() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-version-null");
    bucket_row(&store, &data).await;
    let target = key("null.txt");
    VersionMeta::put_version(&store, &data, &target, &version(None, 1))
        .await
        .unwrap();
    let v1 = Id::new("v1").expect("a valid version id");
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 2))
        .await
        .unwrap();
    VersionMeta::put_version(&store, &data, &target, &version(None, 4))
        .await
        .unwrap();
    assert_eq!(
        VersionMeta::version_seq_of(&store, &data, &target, None)
            .await
            .unwrap(),
        Some(4),
        "the index's null entry names the row just committed"
    );
    assert!(
        VersionMeta::get_version(&store, &data, &target, 1)
            .await
            .unwrap()
            .is_none(),
        "the older null-id row was displaced"
    );
    let mut seqs = VersionMeta::list_versions(&store, &data, &target)
        .await
        .unwrap()
        .iter()
        .map(|row| row.seq)
        .collect::<Vec<_>>();
    seqs.sort_unstable();
    assert_eq!(seqs, vec![2, 4], "exactly one null-id row remains");
    db.drop_db().await;
}

/// OL8: a sequence listed twice names one version. A protected row and a
/// protected row-less current version are each reported once, not once per
/// occurrence: the first pass consumes the entry, so without an explicit
/// guard the second would fall through to the row-less arm and report the
/// same refusal again.
#[tokio::test]
async fn expire_many_report_reports_a_repeated_protected_sequence_once() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-lock-expire-repeat");
    bucket_row(&store, &data).await;
    let target = key("repeat.txt");
    let v1 = Id::new("v1").expect("a valid version id");
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 1))
        .await
        .unwrap();
    // Advance past the row so the current slot names an identity with no
    // version row: the row-less protected arm.
    let phantom = Id::new("phantom").expect("a valid version id");
    VersionMeta::advance_slot(&store, &data, &target, Some(phantom), 2)
        .await
        .unwrap();
    let until = Some(
        i64::try_from(storage::to_nanos(
            reservation(object_lock::Mode::Governance, Duration::from_secs(3_600)).retain_until,
        ))
        .unwrap(),
    );
    plant_lock(
        &store,
        &data,
        &target,
        "v1",
        Some("GOVERNANCE"),
        until,
        Some("OFF"),
    )
    .await;
    plant_lock(
        &store,
        &data,
        &target,
        "phantom",
        Some("GOVERNANCE"),
        until,
        Some("OFF"),
    )
    .await;

    let report = VersionMeta::expire_many_report(&store, &data, &target, &[1, 1, 2, 2])
        .await
        .unwrap();
    assert_eq!(report.removed, 0, "both protected versions stay");
    assert_eq!(
        report.protected,
        vec![
            (1, object_lock::RefusalReason::Governance),
            (2, object_lock::RefusalReason::Governance),
        ],
        "each listed sequence is reported once, in first-occurrence order"
    );
    assert!(
        VersionMeta::get_version(&store, &data, &target, 1)
            .await
            .unwrap()
            .is_some(),
        "the protected row survives"
    );
    db.drop_db().await;
}

/// The set-based batch retires every unprotected row, its index entry and its
/// lock row in one pass: an object row, a marker (which names no lock row), and
/// a row-less current slot (whose lock row goes while its slot stays recorded).
/// The report counts retired rows only, exactly as the per-row path did.
#[tokio::test]
async fn expire_many_report_removes_a_mixed_batch_and_clears_its_locks() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-lock-expire-batch");
    bucket_row(&store, &data).await;
    let target = key("batch.txt");
    let v1 = Id::new("v1").expect("a valid version id");
    let v3 = Id::new("v3").expect("a valid version id");
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 1))
        .await
        .unwrap();
    VersionMeta::put_version(&store, &data, &target, &marker(2))
        .await
        .unwrap();
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v3), 3))
        .await
        .unwrap();
    // The slot names an identity with no version row: the batch's row-less
    // arm, whose lock row must go while the slot itself stays recorded.
    let phantom = Id::new("phantom").expect("a valid version id");
    VersionMeta::advance_slot(&store, &data, &target, Some(phantom), 4)
        .await
        .unwrap();
    // An expired GOVERNANCE retention still carries a row, so the batch has
    // to remove it; it is no protection.
    for wire in ["v1", "v3", "phantom"] {
        plant_lock(
            &store,
            &data,
            &target,
            wire,
            Some("GOVERNANCE"),
            Some(1),
            Some("OFF"),
        )
        .await;
    }

    let report = VersionMeta::expire_many_report(&store, &data, &target, &[1, 2, 3, 4])
        .await
        .unwrap();
    assert_eq!(report.removed, 3, "the object rows and the marker retired");
    assert!(
        report.protected.is_empty(),
        "expired retentions protect nothing: {:?}",
        report.protected
    );
    assert!(
        VersionMeta::bucket_rows(&store, &data)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        VersionMeta::version_seq_of(&store, &data, &target, Some(&v1))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        VersionMeta::version_seq_of(&store, &data, &target, Some(&v3))
            .await
            .unwrap(),
        None
    );
    let locks: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM version_object_lock WHERE bucket = $1 AND key = $2",
    )
    .bind(data.as_ref())
    .bind(target.as_ref())
    .fetch_one(store.as_driver())
    .await
    .unwrap();
    assert_eq!(locks, 0, "every lock row the batch owned is gone");
    assert!(
        VersionMeta::current_slot(&store, &data, &target)
            .await
            .unwrap()
            .is_some(),
        "the row-less slot stays recorded: only its index entry and lock go"
    );
    db.drop_db().await;
}

/// `expire_with` answers the store's verdict: a retired row is `Removed`; a
/// sequence with no row (including the row-less current slot, which removes no
/// version row) is `Nothing`. A refusal stays the contract's `Locked` error.
#[tokio::test]
async fn expire_with_answers_the_verdict() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-version-expire-verdict");
    bucket_row(&store, &data).await;
    let target = key("verdict.txt");
    let v1 = Id::new("v1").expect("a valid version id");
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 1))
        .await
        .unwrap();
    assert_eq!(
        VersionMeta::expire_with(&store, &data, &target, 1, object_lock::Bypass::None)
            .await
            .unwrap(),
        Expired::Removed
    );
    assert_eq!(
        VersionMeta::expire_with(&store, &data, &target, 1, object_lock::Bypass::None)
            .await
            .unwrap(),
        Expired::Nothing,
        "an already-removed row is nothing"
    );
    assert_eq!(
        VersionMeta::expire_with(&store, &data, &target, 99, object_lock::Bypass::None)
            .await
            .unwrap(),
        Expired::Nothing,
        "a sequence no row and no slot names is nothing"
    );
    // The row-less current slot removes its index entry and lock, never a
    // version row, so it too answers `Nothing`.
    let phantom = Id::new("phantom").expect("a valid version id");
    VersionMeta::advance_slot(&store, &data, &target, Some(phantom), 2)
        .await
        .unwrap();
    assert_eq!(
        VersionMeta::expire_with(&store, &data, &target, 2, object_lock::Bypass::None)
            .await
            .unwrap(),
        Expired::Nothing
    );
    db.drop_db().await;
}

#[tokio::test]
async fn expire_and_the_bucket_drain_retire_the_index_entries() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-version-expire");
    bucket_row(&store, &data).await;
    let target = key("sweep.txt");
    let v1 = Id::new("v1").expect("a valid version id");
    let v2 = Id::new("v2").expect("a valid version id");
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 1))
        .await
        .unwrap();
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v2), 2))
        .await
        .unwrap();
    VersionMeta::expire(&store, &data, &target, 2)
        .await
        .unwrap();
    VersionMeta::expire(&store, &data, &target, 2)
        .await
        .unwrap();
    assert!(
        VersionMeta::get_version(&store, &data, &target, 2)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        VersionMeta::version_seq_of(&store, &data, &target, Some(&v2))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        VersionMeta::version_seq_of(&store, &data, &target, Some(&v1))
            .await
            .unwrap(),
        Some(1),
        "the other identity's entry is untouched"
    );
    VersionMeta::expire_many(&store, &data, &target, &[1])
        .await
        .unwrap();
    VersionMeta::expire_many(&store, &data, &target, &[1])
        .await
        .unwrap();
    assert!(
        VersionMeta::bucket_rows(&store, &data)
            .await
            .unwrap()
            .is_empty()
    );
    // Another bucket's rows survive the drain.
    let other = name("plane-version-other");
    bucket_row(&store, &other).await;
    VersionMeta::put_version(&store, &other, &target, &version(Some(&v1), 1))
        .await
        .unwrap();
    VersionMeta::drain_bucket(&store, &data).await.unwrap();
    assert_eq!(
        VersionMeta::bucket_rows(&store, &other)
            .await
            .unwrap()
            .len(),
        1
    );
    db.drop_db().await;
}

/// A drain retires the version rows and their index entries only (R7): the
/// key's **slot** stays recorded, because retiring it is the bucket
/// teardown's step and not this call's.
#[tokio::test]
async fn drain_bucket_leaves_the_slot_recorded() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-version-drain-keeps-slot");
    bucket_row(&store, &data).await;
    let target = key("drained.txt");
    let v1 = Id::new("v1").expect("a valid version id");
    VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 1))
        .await
        .unwrap();
    VersionMeta::advance_slot(&store, &data, &target, Some(v1), 1)
        .await
        .unwrap();
    VersionMeta::drain_bucket(&store, &data).await.unwrap();
    assert!(
        VersionMeta::bucket_rows(&store, &data)
            .await
            .unwrap()
            .is_empty(),
        "the drain retired the version rows"
    );
    assert_eq!(
        VersionMeta::version_seq_of(&store, &data, &target, Some(&Id::new("v1").unwrap()))
            .await
            .unwrap(),
        None,
        "the drain retired the index entry"
    );
    assert!(
        VersionMeta::current_slot(&store, &data, &target)
            .await
            .unwrap()
            .is_some(),
        "a drain alone leaves the slot recorded: the teardown retires it"
    );
    db.drop_db().await;
}

/// One listing entry per version, in the listing's own order — the
/// projection `crates/tinio-pg/src/version.rs`'s listing body builds from
/// the `KeyVersions` this crate answers: the current version from the slot
/// and the object row, then the history newest first.
fn entries_of(versions: &[KeyVersions]) -> Vec<Entry> {
    let mut entries = Vec::new();
    for key_versions in versions {
        entries.push(Entry {
            key: key_versions.key.clone(),
            id: key_versions.version.clone(),
            seq: key_versions.seq,
            is_delete_marker: key_versions.is_delete_marker,
            size: key_versions.elements.size,
            last_modified: key_versions.elements.mtime,
            etag: key_versions.elements.etag.clone(),
        });
        for stored in &key_versions.history {
            entries.push(Entry {
                key: key_versions.key.clone(),
                id: stored.id.clone(),
                seq: stored.seq,
                is_delete_marker: stored.kind == Kind::Marker,
                size: stored.size,
                last_modified: stored.created,
                etag: stored.etag.clone(),
            });
        }
    }
    entries
}

/// Seed `count` version rows for `key_of_key`, the slot naming the newest —
/// `count == 0` leaves the slot naming a sequence no row carries, which is
/// the key the join answers a NULL-sequence row for.
async fn seed_versions(
    store: &PgStore,
    data: &bucket::Name,
    key_of_key: &object::Key,
    count: u64,
    stem: &str,
) {
    let id = |seq: u64| Id::new(&format!("{stem}-v{seq}")).expect("a valid version id");
    for seq in 1..=count {
        VersionMeta::put_version(store, data, key_of_key, &version(Some(&id(seq)), seq))
            .await
            .unwrap();
    }
    // The slot names the newest row — or, for `count == 0`, a sequence no
    // row carries: the key the join answers a NULL-sequence row for.
    let newest = count.max(1);
    VersionMeta::advance_slot(store, data, key_of_key, Some(id(newest)), newest)
        .await
        .unwrap();
}

/// The bounded listing read answers exactly the pages the whole-bucket read
/// answers: for every prefix, delimiter, key marker, version-id marker and
/// page size, `bucket_versions_page` + `version::paginate` equals
/// `bucket_versions` + `version::paginate` — the shape the listing body
/// read before the page was bounded (380k rows for one 1000-key page).
///
/// The oracle is the unbounded read, so a bound that dropped a row, split a
/// key's history across chunks or stopped before the page's cut had decided
/// its truncation would show up here as a different page.
#[tokio::test]
async fn the_bounded_listing_answers_the_whole_bucket_reads_pages() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-version-bounded");
    bucket_row(&store, &data).await;
    // A key with more rows than a page, a nested one, one of its own, one
    // carrying the largest code point, and one whose slot names a sequence
    // no row carries.
    for (raw, count, stem) in [
        ("a/one.txt", 5u64, "one"),
        ("a/nested/three.txt", 2, "three"),
        ("a/two.txt", 1, "two"),
        ("b/four.txt", 3, "four"),
        ("\u{10FFFF}key", 1, "last"),
    ] {
        seed_versions(&store, &data, &key(raw), count, stem).await;
    }
    seed_versions(&store, &data, &key("c/slotless.txt"), 0, "slotless").await;
    let whole = entries_of(&store.bucket_versions(&data).await.unwrap());
    assert_eq!(whole.len(), 5 + 2 + 1 + 3 + 1 + 1, "the corpus landed");
    let one_version = wire_of(Some(&Id::new("one-v2").expect("a valid version id")));
    let mut combos = 0usize;
    for prefix in [None, Some(""), Some("a/"), Some("z/")] {
        for delimiter in [None, Some("/")] {
            for (key_marker, version_id_marker) in [
                (None, None),
                (Some("a/one.txt"), None),
                (Some("a/one.txt"), Some(one_version.as_str())),
                (Some("a/nested/three.txt"), None),
                (Some("zzz"), None),
            ] {
                for max_keys in [0usize, 1, 2, 3, 5, 1000] {
                    combos += 1;
                    let params = ListParams {
                        prefix: prefix.map(str::to_owned),
                        delimiter: delimiter.map(str::to_owned),
                        key_marker: key_marker.map(str::to_owned),
                        version_id_marker: version_id_marker.map(|wire| IdMarker(wire.to_owned())),
                        max_keys,
                    };
                    // The oracle is the whole-bucket read's entries for the
                    // **prefix's own keys**: the bounded read narrows to the
                    // prefix's byte range, exactly as fs's and mem's walks
                    // filter their key streams, so the page a filtered
                    // stream cuts is the one it must answer.
                    let within_prefix: Vec<Entry> = whole
                        .iter()
                        .filter(|entry| entry.key.as_ref().starts_with(prefix.unwrap_or_default()))
                        .cloned()
                        .collect();
                    let expected = paginate(&within_prefix, &params);
                    let bounded =
                        entries_of(&store.bucket_versions_page(&data, &params).await.unwrap());
                    let got = paginate(&bounded, &params);
                    assert_eq!(
                        got.entries, expected.entries,
                        "prefix={prefix:?} delimiter={delimiter:?} key_marker={key_marker:?} \
                         version_id_marker={version_id_marker:?} max_keys={max_keys}"
                    );
                    assert_eq!(
                        got.common_prefixes, expected.common_prefixes,
                        "prefix={prefix:?} delimiter={delimiter:?} max_keys={max_keys}"
                    );
                    assert_eq!(got.is_truncated, expected.is_truncated, "truncation");
                    assert_eq!(got.next_key_marker, expected.next_key_marker, "resume key");
                    assert_eq!(
                        got.next_version_id_marker, expected.next_version_id_marker,
                        "resume version"
                    );
                }
            }
        }
    }
    assert_eq!(combos, 240, "the matrix ran");
    db.drop_db().await;
}

/// The listing's prefix narrows the **read**: a bucket holding keys inside
/// and outside the prefix answers only the prefix's own, with the entries,
/// truncation flags and resume markers a **filtered** stream cuts — the
/// answer fs's and mem's walks serve, because they filter their keys as they
/// walk them. Before the read narrowed, the same request carried the
/// bucket's other keys as entries (asserted at the end of this leg), which is
/// the backend divergence it closes.
#[tokio::test]
async fn the_prefix_narrows_the_listing_to_its_own_keys() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-version-prefix");
    bucket_row(&store, &data).await;
    for (raw, count, stem) in [
        ("dir/one.txt", 3u64, "one"),
        ("dir/two.txt", 1, "two"),
        ("dir/sub/three.txt", 2, "three"),
        ("z/late.txt", 4, "late"),
    ] {
        seed_versions(&store, &data, &key(raw), count, stem).await;
    }
    let whole = entries_of(&store.bucket_versions(&data).await.unwrap());
    let within: Vec<Entry> = whole
        .iter()
        .filter(|entry| entry.key.as_ref().starts_with("dir/"))
        .cloned()
        .collect();
    assert!(
        within.len() < whole.len(),
        "the corpus straddles the prefix"
    );
    let mut combos = 0usize;
    for delimiter in [None, Some("/")] {
        for key_marker in [None, Some("dir/one.txt")] {
            for max_keys in [1usize, 2, 3, 1000] {
                combos += 1;
                let params = ListParams {
                    prefix: Some("dir/".to_owned()),
                    delimiter: delimiter.map(str::to_owned),
                    key_marker: key_marker.map(str::to_owned),
                    version_id_marker: None,
                    max_keys,
                };
                let expected = paginate(&within, &params);
                let got = paginate(
                    &entries_of(&store.bucket_versions_page(&data, &params).await.unwrap()),
                    &params,
                );
                assert_eq!(
                    got.entries, expected.entries,
                    "delimiter={delimiter:?} key_marker={key_marker:?} max_keys={max_keys}"
                );
                assert_eq!(got.common_prefixes, expected.common_prefixes, "roll-up");
                assert_eq!(got.is_truncated, expected.is_truncated, "truncation");
                assert_eq!(got.next_key_marker, expected.next_key_marker, "resume key");
                assert_eq!(
                    got.next_version_id_marker, expected.next_version_id_marker,
                    "resume version"
                );
                assert!(
                    got.entries
                        .iter()
                        .all(|entry| entry.key.as_ref().starts_with("dir/")),
                    "a page of a prefixed listing carries only the prefix's keys"
                );
            }
        }
    }
    assert_eq!(combos, 16, "the matrix ran");
    // The divergence this closes: the whole-bucket read the listing used to
    // cut from serves the out-of-prefix key as an entry.
    let unprefixed = paginate(
        &whole,
        &ListParams {
            prefix: Some("dir/".to_owned()),
            max_keys: 1000,
            ..Default::default()
        },
    );
    assert!(
        unprefixed
            .entries
            .iter()
            .any(|entry| entry.key.as_ref().starts_with("z/")),
        "the page cut from the whole bucket carries the keys the prefix never asked for"
    );
    db.drop_db().await;
}
