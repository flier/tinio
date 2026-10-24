//! Filesystem Object Lock. The shared Storage journeys live in
//! `tinio_util::testing::object_lock::storage`. What stays here is that
//! suite's row fixture and the journeys that have no memory-backend twin.

use std::sync::{Mutex, PoisonError};

use async_trait::async_trait;

use super::*;
use crate::_util::testing::object_lock::{
    default_rule, lock_config,
    storage::{StorageLockFixture, assert_storage_object_lock},
};

/// Keeps each journey's temp root alive until the shared suite returns.
struct FsLockFixture {
    roots: Mutex<Vec<tempfile::TempDir>>,
}

impl FsLockFixture {
    fn new() -> Self {
        Self {
            roots: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl StorageLockFixture for FsLockFixture {
    type Store = FsStorage;

    async fn open(&self) -> FsStorage {
        let (root, storage) = testutil::storage();
        self.roots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(root);
        storage
    }

    async fn plant_lock(
        &self,
        storage: &FsStorage,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<&version::Id>,
        lock: &object_lock::Lock,
    ) {
        let (b, k, id) = (bucket.to_string(), key.to_string(), version::wire_of(id));
        let lock = lock.clone();
        storage
            .handle()
            .write(move |txn| -> Result<(), DatabaseError> {
                version_object_lock::Table::open(txn)?.put(&b, &k, &id, &lock)?;
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn clear_lock(
        &self,
        storage: &FsStorage,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<&version::Id>,
    ) {
        let (b, k, id) = (bucket.to_string(), key.to_string(), version::wire_of(id));
        storage
            .handle()
            .write(move |txn| -> Result<(), DatabaseError> {
                version_object_lock::Table::open(txn)?.remove(&b, &k, &id)?;
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn lock_present(
        &self,
        storage: &FsStorage,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<&version::Id>,
    ) -> bool {
        let (b, k, id) = (bucket.to_string(), key.to_string(), version::wire_of(id));
        storage
            .handle()
            .read(move |txn| -> Result<bool, DatabaseError> {
                Ok(version_object_lock::Table::open_readonly(txn)?
                    .get(&b, &k, &id)?
                    .is_some())
            })
            .unwrap()
    }

    async fn stored_lock(
        &self,
        storage: &FsStorage,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<&version::Id>,
    ) -> Option<object_lock::Lock> {
        storage
            .version_plane(bucket)
            .version_rows
            .version_lock(bucket, key, id)
            .await
            .unwrap()
    }

    async fn fail_versioning_step(
        &self,
        storage: &FsStorage,
        bucket: &bucket::Name,
        config: &object_lock::Config,
    ) -> Result<(), storage::Error> {
        storage
            .ops()
            .create_bucket_with_lock(
                bucket,
                None,
                &Acl::default_private(None),
                config,
                || async { Err(storage::corrupt("the versioning step failed")) },
            )
            .await
    }
}

#[tokio::test]
async fn the_storage_object_lock_journeys() {
    let fixture = FsLockFixture::new();
    assert_storage_object_lock::<FsStorage, _>(&fixture).await;
}

/// OL8/R11: the store's `expire_with` judges the **recorded** identity — its
/// no-row fallback judges the current slot even when the addressed sequence
/// differs from the slot's — not the gated `locate` identity. In the
/// degraded-null state the gate answers the null wire while the row still
/// records an identity, so a lock on the recorded wire must refuse with the
/// file still present and unchanged.
#[tokio::test]
async fn a_degraded_current_delete_judges_the_recorded_identitys_lock() {
    let (root, storage) = testutil::storage();
    let (b, key) = testutil::versioned_bucket(&storage, version::State::Enabled, &[b"first"]).await;
    let recorded = storage
        .head_object(&b, &key)
        .await
        .unwrap()
        .version_id
        .expect("an enabled write mints an identity");
    let rows = FsLockFixture::new();
    rows.plant_lock(
        &storage,
        &b,
        &key,
        Some(&recorded),
        &object_lock::Lock {
            retention: Some(reservation(
                object_lock::Mode::Governance,
                Duration::from_secs(3_600),
            )),
            legal_hold: object_lock::LegalHold::Off,
        },
    )
    .await;

    // Drift the file out of band: the gate now answers the null version while
    // the row still records `recorded`.
    let path = root.path().join("data").join("notes.txt");
    let edited = b"an out-of-band edit, longer than the original";
    fs::write(&path, edited).unwrap();

    let refused = storage
        .delete_object_version(&b, &key, None, object_lock::Bypass::None)
        .await
        .expect_err("the recorded identity's lock refuses the degraded delete");
    assert!(
        matches!(
            storage::Error::from(refused),
            storage::Error::Locked(object_lock::RefusalReason::Governance)
        ),
        "the pre-check judged the recorded wire, not the gated null one"
    );
    assert_eq!(
        fs::read(&path).unwrap(),
        edited,
        "the refused delete never unlinked the protected file"
    );
    assert!(
        rows.lock_present(&storage, &b, &key, Some(&recorded)).await,
        "the refusal leaves the lock row"
    );
}

/// OL8: the degraded-null delete **without** a lock reclaims the drifted file.
/// The store's verdict is a success (nothing is protected), `restore` clears
/// the metadata, and the unreferenced key-path file is unlinked — the sweep
/// never sees a key-path file, so the delete is its only reclamation point.
#[tokio::test]
async fn a_degraded_current_delete_without_a_lock_unlinks_the_drifted_file() {
    let (root, storage) = testutil::storage();
    let (b, key) = testutil::versioned_bucket(&storage, version::State::Enabled, &[b"first"]).await;

    // Drift the file out of band: the gate answers the null version while the
    // row still records a real identity.
    let path = root.path().join("data").join("notes.txt");
    let edited = b"an out-of-band edit, longer than the original";
    fs::write(&path, edited).unwrap();

    storage
        .delete_object_version(&b, &key, None, object_lock::Bypass::None)
        .await
        .expect("no lock protects the degraded delete");
    assert!(
        !path.exists(),
        "the unreferenced file is reclaimed by the delete itself"
    );
    assert!(
        storage.head_object(&b, &key).await.is_err(),
        "the key holds no object afterwards"
    );
}

/// OL8/OL12: a retention/hold write serializes with the delete path's
/// pre-check and unlink through the bucket mutation lock, so a lock can never
/// be granted between them and turn the store's re-check into a refusal after
/// the protected bytes were removed.
#[tokio::test]
async fn a_lock_write_waits_on_the_bucket_mutation_lock() {
    let (_root, storage) = testutil::storage();
    let (b, key) = testutil::versioned_bucket(&storage, version::State::Enabled, &[b"first"]).await;
    storage
        .put_bucket_object_lock(&b, &lock_config(None))
        .await
        .unwrap();
    let id = storage
        .head_object(&b, &key)
        .await
        .unwrap()
        .version_id
        .expect("an enabled write mints an identity");
    let now = SystemTime::now();
    let requested = object_lock::Retention {
        mode: object_lock::Mode::Governance,
        retain_until: now + Duration::from_secs(3_600),
    };

    let guard = storage.lock_bucket_mutations(&b).await;
    let rows = storage.version_plane(&b).version_rows;
    let setter = tokio::spawn(async move {
        rows.set_retention(
            &b,
            &key,
            Some(&id),
            Some(&requested),
            object_lock::Bypass::None,
            now,
        )
        .await
    });
    testutil::wait_for_lock_waiter().await;
    assert!(
        !setter.is_finished(),
        "a retention write waits on the bucket mutation lock"
    );
    drop(guard);
    let stored = setter.await.unwrap().unwrap();
    assert!(
        stored.is_some(),
        "the retention lands once the mutation guard is released"
    );
}

/// OL23: the combined update is one store transaction. A compliance
/// shortening is refused while a legal-hold member is present, so the hold
/// never lands and the stored lock is unchanged; an accepted update applies
/// both members.
#[tokio::test]
async fn a_refused_retention_leaves_the_legal_hold_unapplied() {
    let (_root, storage) = testutil::storage();
    let (b, key) = testutil::versioned_bucket(&storage, version::State::Enabled, &[b"first"]).await;
    storage
        .put_bucket_object_lock(&b, &lock_config(None))
        .await
        .unwrap();
    let id = storage
        .head_object(&b, &key)
        .await
        .unwrap()
        .version_id
        .expect("an enabled write mints an identity");
    let now = SystemTime::now();
    let compliance = object_lock::Retention {
        mode: object_lock::Mode::Compliance,
        retain_until: now + Duration::from_secs(3_600),
    };
    let address = version::Address::Id(id.clone());
    storage
        .set_retention(
            &b,
            &key,
            &address,
            Some(&compliance),
            object_lock::Bypass::None,
            now,
        )
        .await
        .unwrap();
    let before = storage
        .object_lock(&b, &key, Some(&id))
        .await
        .unwrap()
        .expect("the lock row");

    let shorter = object_lock::Retention {
        mode: object_lock::Mode::Compliance,
        retain_until: now + Duration::from_secs(1_800),
    };
    let refused = storage
        .update_lock(
            &b,
            &key,
            &address,
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
        storage.object_lock(&b, &key, Some(&id)).await.unwrap(),
        Some(before),
        "the refused update wrote neither member"
    );
    assert_eq!(
        storage
            .get_object_legal_hold(&b, &key, Some(&id))
            .await
            .unwrap(),
        object_lock::LegalHold::Off,
        "the hold was not applied"
    );

    let later = object_lock::Retention {
        mode: object_lock::Mode::Compliance,
        retain_until: now + Duration::from_secs(7_200),
    };
    let updated = storage
        .update_lock(
            &b,
            &key,
            &address,
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
        storage.object_lock(&b, &key, Some(&id)).await.unwrap(),
        Some(updated),
        "the one transaction stored both members"
    );
}

/// OL13/R21: suspending a lock-enabled fs bucket is `InvalidBucketState` (the
/// store's guarded write), and a bucket without a configuration still
/// suspends.
#[tokio::test]
async fn suspending_a_lock_enabled_bucket_is_invalid_bucket_state() {
    let (_root, storage) = testutil::storage();
    let (b, _key) =
        testutil::versioned_bucket(&storage, version::State::Enabled, &[b"first"]).await;
    storage
        .put_bucket_object_lock(&b, &lock_config(None))
        .await
        .unwrap();

    let refused = storage
        .set_bucket_versioning(&b, version::State::Suspended)
        .await
        .expect_err("a lock-enabled bucket is never suspended");
    assert!(
        matches!(
            storage::Error::from(refused),
            storage::Error::InvalidBucketState(_)
        ),
        "the suspend is InvalidBucketState"
    );
    assert_eq!(
        storage.bucket_versioning(&b).await.unwrap(),
        version::State::Enabled,
        "the refused suspend left the state unchanged"
    );

    let plain = bucket::name("plain").unwrap();
    storage
        .create_bucket(&plain, None, &Acl::default_private(None))
        .await
        .unwrap();
    storage
        .set_bucket_versioning(&plain, version::State::Enabled)
        .await
        .unwrap();
    storage
        .set_bucket_versioning(&plain, version::State::Suspended)
        .await
        .unwrap();
    assert_eq!(
        storage.bucket_versioning(&plain).await.unwrap(),
        version::State::Suspended,
        "a bucket with no configuration still suspends"
    );
}

/// OL16: the checksum decision and the applied lock share one fact — a
/// default-retention configuration resolves a retention (the checksum is
/// required), while an enabled-but-rule-less configuration with a hold
/// resolves a lock that carries **no** retention (the checksum is not), and
/// with no hold resolves nothing at all.
#[tokio::test]
async fn the_checksum_requirement_and_the_stored_lock_share_one_fact() {
    let (_root, storage) = testutil::storage();
    let now = SystemTime::now();

    let with_default = bucket::name("default").unwrap();
    storage
        .create_bucket_with_lock(
            &with_default,
            None,
            &Acl::default_private(None),
            &default_rule(object_lock::Mode::Governance, 1),
        )
        .await
        .unwrap();
    let default = storage
        .resolve_write_lock(&with_default, None, object_lock::LegalHold::Off, now)
        .await
        .unwrap()
        .expect("a lock");
    assert!(
        default.retention.is_some(),
        "the default retention requires a checksum"
    );

    let hold_only = bucket::name("hold-only").unwrap();
    storage
        .create_bucket_with_lock(
            &hold_only,
            None,
            &Acl::default_private(None),
            &lock_config(None),
        )
        .await
        .unwrap();
    assert!(
        storage
            .resolve_write_lock(&hold_only, None, object_lock::LegalHold::Off, now)
            .await
            .unwrap()
            .is_none(),
        "an enabled configuration with no rule and no hold locks nothing"
    );
    let held = storage
        .resolve_write_lock(&hold_only, None, object_lock::LegalHold::On, now)
        .await
        .unwrap()
        .expect("the hold is a lock");
    assert!(
        held.retention.is_none(),
        "a legal-hold-only lock carries no retention, so it requires no checksum"
    );
}
