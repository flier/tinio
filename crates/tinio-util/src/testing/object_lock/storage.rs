//! Storage-level Object Lock journeys shared by the filesystem and memory
//! backends.
//!
//! [`super::assert_object_lock_conformance`] drives `Metadata + VersionMeta`.
//! These journeys drive `Storage`: a current-version
//! delete, the null version, a marker's untouched lock row, the governance
//! bypass, default-retention resolution, the versioned commit, copy, and the
//! all-or-none create. Planting a lock row, reading it back, and injecting the
//! create's versioning-step failure stay on [`StorageLockFixture`] — each
//! backend's own table and its ops receiver, which [`Storage`] does not expose.

use std::time::{Duration, SystemTime};

use async_trait::async_trait;

use super::{
    super::{body, bucket_name, fixtures::governance_reservation, object_key},
    default_rule, lock_config,
};
use crate::_core::{
    Storage,
    acl::Acl,
    bucket, object, object_lock,
    storage::{self, BucketOps, ObjectOps, VersionOps},
    version,
};

/// The backend-only seams the shared journeys cannot see.
///
/// `open` is one fresh storage per journey (an fs temp root stays alive for
/// the fixture's life). The lock-row methods plant, clear, and read the
/// identity's row through the backend's own table. [`Self::fail_versioning_step`]
/// is the ops composition both backends already drive: the versioning step
/// returns `Corrupt`, and the composition removes the bucket it created.
#[async_trait]
pub trait StorageLockFixture: Send + Sync {
    /// The storage the journeys drive.
    type Store: Storage;

    /// A fresh storage for one journey.
    async fn open(&self) -> Self::Store;

    /// Plant `lock` on the identity (`None` is the null version).
    async fn plant_lock(
        &self,
        storage: &Self::Store,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<&version::Id>,
        lock: &object_lock::Lock,
    );

    /// Remove a planted lock row.
    async fn clear_lock(
        &self,
        storage: &Self::Store,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<&version::Id>,
    );

    /// Whether the identity's lock row is still stored.
    async fn lock_present(
        &self,
        storage: &Self::Store,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<&version::Id>,
    ) -> bool;

    /// The identity's stored lock row, or none when the row is absent.
    async fn stored_lock(
        &self,
        storage: &Self::Store,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<&version::Id>,
    ) -> Option<object_lock::Lock>;

    /// Run the ops composition's lock-enabled create with a versioning step
    /// that fails. The error is the composition's own `Corrupt`.
    async fn fail_versioning_step(
        &self,
        storage: &Self::Store,
        bucket: &bucket::Name,
        config: &object_lock::Config,
    ) -> Result<(), storage::Error>;
}

/// Assert the Storage-level Object Lock journeys.
///
/// `S` is the storage both backends implement. `F` plants lock rows and opens
/// a fresh `S` per journey. One panic stops the rest; the message names the rule.
///
/// # Panics
///
/// On the first violated rule.
pub async fn assert_storage_object_lock<S, F>(fixture: &F)
where
    S: Storage,
    F: StorageLockFixture<Store = S>,
{
    a_current_version_delete_enforces_and_removes_its_lock(fixture).await;
    a_null_current_version_delete_enforces_and_removes_its_lock(fixture).await;
    a_marker_delete_never_observes_or_removes_a_lock_row(fixture).await;
    a_governance_bypass_deletes_a_protected_current_version(fixture).await;
    a_compliance_retention_refuses_the_bypass(fixture).await;
    a_governance_bypass_deletes_a_protected_history_version(fixture).await;
    the_effective_write_lock_resolves_the_default_and_the_override(fixture).await;
    the_versioned_commit_carries_the_lock_and_never_clears_it(fixture).await;
    a_protected_null_in_place_replacement_is_refused(fixture).await;
    a_copy_takes_the_destination_lock_and_never_the_source(fixture).await;
    create_bucket_with_lock_is_all_or_none(fixture).await;
}

fn contract(err: impl Into<storage::Error>) -> storage::Error {
    err.into()
}

fn governance_lock() -> object_lock::Lock {
    object_lock::Lock {
        retention: Some(governance_reservation()),
        legal_hold: object_lock::LegalHold::Off,
    }
}

/// Governance retention plus a legal hold — the lock the versioned commit stores.
fn carried_lock() -> object_lock::Lock {
    object_lock::Lock {
        retention: Some(governance_reservation()),
        legal_hold: object_lock::LegalHold::On,
    }
}

/// A `data` bucket at `state`, with `bodies` committed in order at `notes.txt`.
async fn versioned<S: Storage>(
    storage: &S,
    state: version::State,
    bodies: &[&[u8]],
) -> (bucket::Name, object::Key) {
    let bucket = bucket_name("data");
    let key = object_key("notes.txt");
    storage
        .create_bucket(&bucket, None, &Acl::default_private(None))
        .await
        .unwrap();
    storage.set_bucket_versioning(&bucket, state).await.unwrap();
    for bytes in bodies {
        storage
            .put_object(&bucket, &key, body(bytes.to_vec()))
            .await
            .unwrap();
    }
    (bucket, key)
}

async fn current_id<S: Storage>(
    storage: &S,
    bucket: &bucket::Name,
    key: &object::Key,
) -> version::Id {
    storage
        .head_object(bucket, key)
        .await
        .unwrap()
        .version_id
        .expect("an enabled write mints an identity")
}

async fn put_locked<S: Storage>(
    storage: &S,
    bucket: &bucket::Name,
    key: &object::Key,
    bytes: &[u8],
    lock: Option<&object_lock::Lock>,
) -> object::Info {
    let staged = storage
        .stage_body(bucket, key, body(bytes.to_vec()), None, None)
        .await
        .unwrap();
    storage
        .commit_object(
            bucket,
            key,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            lock,
        )
        .await
        .unwrap()
}

/// OL8: deleting the current version by its id enforces the identity's lock,
/// and the lock row leaves with the version.
async fn a_current_version_delete_enforces_and_removes_its_lock<F>(fixture: &F)
where
    F: StorageLockFixture,
{
    let storage = fixture.open().await;
    let (bucket, key) = versioned(&storage, version::State::Enabled, &[b"first"]).await;
    let id = current_id(&storage, &bucket, &key).await;
    fixture
        .plant_lock(&storage, &bucket, &key, Some(&id), &governance_lock())
        .await;

    let refused = storage
        .delete_object_version(&bucket, &key, Some(&id), object_lock::Bypass::None)
        .await
        .unwrap_err();
    let refused = contract(refused);
    assert!(
        matches!(
            &refused,
            storage::Error::Locked(object_lock::RefusalReason::Governance)
        ),
        "the current-version delete arm enforces the lock: {refused:?}"
    );
    assert!(
        storage.get_object(&bucket, &key, None).await.is_ok(),
        "a refused delete leaves the object and the slot intact"
    );
    assert!(
        fixture
            .lock_present(&storage, &bucket, &key, Some(&id))
            .await,
        "a refused delete leaves the lock row"
    );

    fixture.clear_lock(&storage, &bucket, &key, Some(&id)).await;
    let removed = storage
        .delete_object_version(&bucket, &key, Some(&id), object_lock::Bypass::None)
        .await
        .unwrap();
    assert_eq!(removed.id.as_ref(), Some(&id));
    assert!(
        !fixture
            .lock_present(&storage, &bucket, &key, Some(&id))
            .await,
        "the lock row is removed with the version"
    );
}

/// OL8: the null version is addressed as `None` and its lock is enforced and
/// removed through the same arm.
async fn a_null_current_version_delete_enforces_and_removes_its_lock<F>(fixture: &F)
where
    F: StorageLockFixture,
{
    let storage = fixture.open().await;
    let (bucket, key) = versioned(&storage, version::State::Enabled, &[b"first"]).await;
    storage
        .set_bucket_versioning(&bucket, version::State::Suspended)
        .await
        .unwrap();
    storage
        .put_object(&bucket, &key, body(b"null!".to_vec()))
        .await
        .unwrap();
    assert_eq!(
        storage.head_object(&bucket, &key).await.unwrap().version_id,
        None,
        "a suspended in-place write produces the null version"
    );
    fixture
        .plant_lock(&storage, &bucket, &key, None, &governance_lock())
        .await;

    let refused = storage
        .delete_object_version(&bucket, &key, None, object_lock::Bypass::None)
        .await
        .unwrap_err();
    let refused = contract(refused);
    assert!(
        matches!(
            &refused,
            storage::Error::Locked(object_lock::RefusalReason::Governance)
        ),
        "the null identity's lock is enforced: {refused:?}"
    );
    assert!(
        storage.get_object(&bucket, &key, None).await.is_ok(),
        "a refused null delete leaves the object intact"
    );

    fixture.clear_lock(&storage, &bucket, &key, None).await;
    storage
        .delete_object_version(&bucket, &key, None, object_lock::Bypass::None)
        .await
        .unwrap();
    assert!(
        !fixture.lock_present(&storage, &bucket, &key, None).await,
        "the null version's lock row is removed with the version"
    );
}

/// R19: the marker arm removes the marker without observing or removing a lock
/// row planted under its identity.
async fn a_marker_delete_never_observes_or_removes_a_lock_row<F>(fixture: &F)
where
    F: StorageLockFixture,
{
    let storage = fixture.open().await;
    let (bucket, key) = versioned(&storage, version::State::Enabled, &[b"first"]).await;
    let marker = storage.delete_object(&bucket, &key).await.unwrap();
    let marker_id = marker
        .id
        .clone()
        .expect("an enabled bucket's marker carries an identity");
    fixture
        .plant_lock(
            &storage,
            &bucket,
            &key,
            Some(&marker_id),
            &object_lock::Lock {
                retention: Some(object_lock::Retention {
                    mode: object_lock::Mode::Compliance,
                    retain_until: SystemTime::UNIX_EPOCH + Duration::from_secs(4_000_000_000),
                }),
                legal_hold: object_lock::LegalHold::On,
            },
        )
        .await;

    let removed = storage
        .delete_object_version(&bucket, &key, Some(&marker_id), object_lock::Bypass::None)
        .await
        .unwrap();
    assert!(removed.is_delete_marker);
    assert!(
        fixture
            .lock_present(&storage, &bucket, &key, Some(&marker_id))
            .await,
        "R19: the marker arm never observes or removes a lock row"
    );
}

/// OL11: a governance bypass deletes a protected current version, and the
/// lock row leaves with it.
async fn a_governance_bypass_deletes_a_protected_current_version<F>(fixture: &F)
where
    F: StorageLockFixture,
{
    let storage = fixture.open().await;
    let (bucket, key) = versioned(&storage, version::State::Enabled, &[b"first"]).await;
    let id = current_id(&storage, &bucket, &key).await;
    fixture
        .plant_lock(&storage, &bucket, &key, Some(&id), &governance_lock())
        .await;

    let removed = storage
        .delete_object_version(&bucket, &key, Some(&id), object_lock::Bypass::Governance)
        .await
        .expect("the bypass lets the governance lock through");
    assert_eq!(removed.id.as_ref(), Some(&id));
    assert!(
        !fixture
            .lock_present(&storage, &bucket, &key, Some(&id))
            .await,
        "the lock row is removed with the version"
    );
    assert!(
        storage.get_object(&bucket, &key, None).await.is_err(),
        "the only version's delete removes the key"
    );
}

/// OL11: COMPLIANCE refuses a delete even when the request carries the
/// governance bypass, leaving the object and the lock row intact.
async fn a_compliance_retention_refuses_the_bypass<F>(fixture: &F)
where
    F: StorageLockFixture,
{
    let storage = fixture.open().await;
    let (bucket, key) = versioned(&storage, version::State::Enabled, &[b"first"]).await;
    let id = current_id(&storage, &bucket, &key).await;
    fixture
        .plant_lock(
            &storage,
            &bucket,
            &key,
            Some(&id),
            &object_lock::Lock {
                retention: Some(object_lock::Retention {
                    mode: object_lock::Mode::Compliance,
                    retain_until: SystemTime::now() + Duration::from_secs(3_600),
                }),
                legal_hold: object_lock::LegalHold::Off,
            },
        )
        .await;

    let refused = storage
        .delete_object_version(&bucket, &key, Some(&id), object_lock::Bypass::Governance)
        .await
        .expect_err("compliance ignores the bypass");
    let refused = contract(refused);
    assert!(
        matches!(
            &refused,
            storage::Error::Locked(object_lock::RefusalReason::Compliance)
        ),
        "compliance refuses in either mode: {refused:?}"
    );
    assert!(
        storage.get_object(&bucket, &key, None).await.is_ok(),
        "a refused delete leaves the object"
    );
    assert!(
        fixture
            .lock_present(&storage, &bucket, &key, Some(&id))
            .await,
        "a refused delete leaves the lock row"
    );
}

/// OL8: the noncurrent-version arm forwards the bypass to the history delete.
async fn a_governance_bypass_deletes_a_protected_history_version<F>(fixture: &F)
where
    F: StorageLockFixture,
{
    let storage = fixture.open().await;
    let (bucket, key) = versioned(&storage, version::State::Enabled, &[b"first"]).await;
    let history = current_id(&storage, &bucket, &key).await;
    storage
        .put_object(&bucket, &key, body(b"second".to_vec()))
        .await
        .unwrap();
    fixture
        .plant_lock(&storage, &bucket, &key, Some(&history), &governance_lock())
        .await;

    let refused = storage
        .delete_object_version(&bucket, &key, Some(&history), object_lock::Bypass::None)
        .await
        .expect_err("the history version's lock refuses");
    let refused = contract(refused);
    assert!(
        matches!(
            &refused,
            storage::Error::Locked(object_lock::RefusalReason::Governance)
        ),
        "the history arm enforces the identity's lock: {refused:?}"
    );

    storage
        .delete_object_version(
            &bucket,
            &key,
            Some(&history),
            object_lock::Bypass::Governance,
        )
        .await
        .expect("the bypass removes the history version");
    assert!(
        !fixture
            .lock_present(&storage, &bucket, &key, Some(&history))
            .await,
        "the history lock row leaves with the version"
    );
}

/// OL10/OL16: the effective lock resolves once from the stored configuration —
/// the default, the explicit override, a legal-hold-only write, and a bucket
/// with no configuration. The hold-only bucket (no retention, so no checksum)
/// is the fact mem's combined resolution journey asserted beside the default.
async fn the_effective_write_lock_resolves_the_default_and_the_override<F>(fixture: &F)
where
    F: StorageLockFixture,
{
    let storage = fixture.open().await;
    let bucket = bucket_name("locked");
    let config = default_rule(object_lock::Mode::Governance, 7);
    storage
        .create_bucket_with_lock(&bucket, None, &Acl::default_private(None), &config)
        .await
        .unwrap();
    assert_eq!(
        storage.bucket_versioning(&bucket).await.unwrap(),
        version::State::Enabled
    );
    assert_eq!(
        storage.get_bucket_object_lock(&bucket).await.unwrap(),
        Some(config)
    );

    let now = SystemTime::now();
    let default = storage
        .resolve_write_lock(&bucket, None, object_lock::LegalHold::Off, now)
        .await
        .unwrap()
        .expect("the default retention is a lock");
    assert_eq!(
        default.retention.expect("the default").retain_until,
        now + Duration::from_secs(7 * 86_400)
    );

    let explicit = object_lock::Retention {
        mode: object_lock::Mode::Compliance,
        retain_until: now + Duration::from_secs(60),
    };
    let resolved = storage
        .resolve_write_lock(&bucket, Some(&explicit), object_lock::LegalHold::Off, now)
        .await
        .unwrap()
        .expect("the explicit retention is a lock");
    assert_eq!(resolved.retention, Some(explicit));

    let held = storage
        .resolve_write_lock(&bucket, None, object_lock::LegalHold::On, now)
        .await
        .unwrap()
        .expect("the hold alone is a lock");
    assert_eq!(held.legal_hold, object_lock::LegalHold::On);
    assert!(
        held.retention.is_some(),
        "a legal-hold-only write still receives the default retention"
    );

    let hold_only = bucket_name("hold-only");
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
    let hold = storage
        .resolve_write_lock(&hold_only, None, object_lock::LegalHold::On, now)
        .await
        .unwrap()
        .expect("the hold is a lock");
    assert!(
        hold.retention.is_none(),
        "a legal-hold-only lock carries no retention, so it requires no checksum"
    );

    let plain = bucket_name("plain");
    storage
        .create_bucket(&plain, None, &Acl::default_private(None))
        .await
        .unwrap();
    assert!(
        storage
            .resolve_write_lock(&plain, None, object_lock::LegalHold::Off, now)
            .await
            .unwrap()
            .is_none(),
        "a bucket with no configuration resolves no lock"
    );
}

/// OL7: the lock rides the versioned commit; an ordinary overwrite mints a new
/// identity and leaves the outgoing version's lock intact, and an unlocked
/// write stores no row.
async fn the_versioned_commit_carries_the_lock_and_never_clears_it<F>(fixture: &F)
where
    F: StorageLockFixture,
{
    let storage = fixture.open().await;
    let (bucket, key) = versioned(&storage, version::State::Enabled, &[]).await;
    let lock = carried_lock();

    let first = put_locked(&storage, &bucket, &key, b"one", Some(&lock)).await;
    let first_id = first.version_id.clone().expect("an identity");
    assert_eq!(
        fixture
            .stored_lock(&storage, &bucket, &key, Some(&first_id))
            .await,
        Some(lock.clone()),
        "the lock commits with the current version"
    );

    let second = put_locked(&storage, &bucket, &key, b"two", None).await;
    let second_id = second.version_id.clone().expect("an identity");
    assert_ne!(first_id, second_id);
    assert_eq!(
        fixture
            .stored_lock(&storage, &bucket, &key, Some(&first_id))
            .await,
        Some(lock),
        "an ordinary overwrite leaves the outgoing version's lock intact"
    );
    assert!(
        fixture
            .stored_lock(&storage, &bucket, &key, Some(&second_id))
            .await
            .is_none(),
        "an unlocked write stores no row"
    );
}

/// OL7/OL12: a null in-place replacement of an identity that owns an active
/// lock is refused, and the refusal leaves the lock row in place.
async fn a_protected_null_in_place_replacement_is_refused<F>(fixture: &F)
where
    F: StorageLockFixture,
{
    let storage = fixture.open().await;
    let (bucket, key) = versioned(&storage, version::State::Suspended, &[]).await;
    let first = put_locked(&storage, &bucket, &key, b"one", None).await;
    assert!(
        first.version_id.is_none(),
        "a suspended bucket writes the null version"
    );
    fixture
        .plant_lock(&storage, &bucket, &key, None, &governance_lock())
        .await;

    let staged = storage
        .stage_body(&bucket, &key, body(b"two".to_vec()), None, None)
        .await
        .unwrap();
    let err = storage
        .commit_object(
            &bucket,
            &key,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .expect_err("the locked null identity is a protected overwrite");
    let err = contract(err);
    assert!(
        matches!(
            &err,
            storage::Error::Locked(object_lock::RefusalReason::Governance)
        ),
        "the store refuses with Locked: {err:?}"
    );
    assert!(
        fixture.lock_present(&storage, &bucket, &key, None).await,
        "the refusal leaves the lock row in place"
    );
}

/// OL12: a copy never inherits the source version's lock; only the
/// destination's resolved lock applies.
async fn a_copy_takes_the_destination_lock_and_never_the_source<F>(fixture: &F)
where
    F: StorageLockFixture,
{
    let storage = fixture.open().await;
    let (bucket, src) = versioned(&storage, version::State::Enabled, &[]).await;
    let dst = object_key("copy.bin");
    let source = put_locked(&storage, &bucket, &src, b"source", Some(&governance_lock())).await;
    let source_id = source.version_id.clone().expect("an identity");

    let copied = storage
        .copy_object(
            &bucket,
            &src,
            &bucket,
            &dst,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
            None,
        )
        .await
        .unwrap();
    assert!(
        fixture
            .stored_lock(&storage, &bucket, &dst, copied.version_id.as_ref())
            .await
            .is_none(),
        "a copy with no destination lock is unlocked"
    );
    assert!(
        fixture
            .stored_lock(&storage, &bucket, &src, Some(&source_id))
            .await
            .is_some(),
        "the source keeps its own lock"
    );

    let destination_lock = object_lock::Lock {
        retention: Some(object_lock::Retention {
            mode: object_lock::Mode::Compliance,
            retain_until: SystemTime::now() + Duration::from_secs(7_200),
        }),
        legal_hold: object_lock::LegalHold::On,
    };
    let locked = storage
        .copy_object(
            &bucket,
            &src,
            &bucket,
            &dst,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
            Some(&destination_lock),
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .stored_lock(&storage, &bucket, &dst, locked.version_id.as_ref())
            .await,
        Some(destination_lock),
        "the destination's own lock applies"
    );
}

/// OL13: `create_bucket_with_lock` is all-or-none — a success leaves the
/// bucket, `Enabled` versioning and the stored configuration; a taken name
/// leaves those facts untouched; a failure of the versioning step leaves no
/// bucket behind.
async fn create_bucket_with_lock_is_all_or_none<F>(fixture: &F)
where
    F: StorageLockFixture,
{
    let storage = fixture.open().await;
    let config = default_rule(object_lock::Mode::Governance, 1);
    let bucket = bucket_name("locked");
    storage
        .create_bucket_with_lock(&bucket, None, &Acl::default_private(None), &config)
        .await
        .unwrap();
    assert_eq!(
        storage.bucket_versioning(&bucket).await.unwrap(),
        version::State::Enabled
    );
    assert_eq!(
        storage.get_bucket_object_lock(&bucket).await.unwrap(),
        Some(config.clone())
    );

    let err = storage
        .create_bucket_with_lock(
            &bucket,
            None,
            &Acl::default_private(None),
            &lock_config(None),
        )
        .await
        .expect_err("the name is taken");
    let err = contract(err);
    assert!(
        matches!(&err, storage::Error::AlreadyExists(_)),
        "the taken name refuses: {err:?}"
    );
    assert_eq!(
        storage.get_bucket_object_lock(&bucket).await.unwrap(),
        Some(config.clone())
    );
    assert_eq!(
        storage.bucket_versioning(&bucket).await.unwrap(),
        version::State::Enabled,
        "the refused create left the state unchanged"
    );

    let partial = bucket_name("partial");
    let err = fixture
        .fail_versioning_step(&storage, &partial, &config)
        .await
        .expect_err("the injected step failure");
    assert!(matches!(&err, storage::Error::Corrupt(_)), "{err:?}");
    assert!(
        storage.head_bucket(&partial).await.is_err(),
        "the compensation removed the partially created bucket"
    );
}
