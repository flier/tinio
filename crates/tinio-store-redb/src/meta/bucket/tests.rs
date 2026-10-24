use std::time::{Duration, SystemTime};

use tinio_util::testing::metadata::{assert_bucket_meta_conformance, lifecycle_config};

use crate::{
    _core::{
        acl::{Acl, AclGrants, OwnerId},
        bucket, cors,
        etag::ETag,
        lifecycle,
        meta::{BucketMeta, VersionMeta, bucket as row},
        object, object_lock, storage,
        version::State,
    },
    Error, RedbMetadata,
    bucket_lifecycle::Table as LifecycleTable,
    bucket_object_lock::Table as ObjectLockTable,
    object_meta::{self, Stored},
};

fn t(secs: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
}

/// A grant set (the owner's own default grants).
fn grants() -> AclGrants {
    Acl::default_private(Some(owner())).grants
}

fn owner() -> OwnerId {
    OwnerId::new("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").unwrap()
}

fn tags(pairs: &[(&str, &str)]) -> object::Tags {
    object::Tags::from_pairs(
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned())),
    )
    .unwrap()
}

/// The tag set as owned pairs — [`object::Tags::iter`] yields borrowed
/// `(&str, &str)` tuples, so a leg comparing against `Vec<(String,
/// String)>` needs this side (the shared suite's `pairs` helper).
fn pairs(tags: &object::Tags) -> Vec<(String, String)> {
    tags.iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
}

/// Plant a lifecycle row through the raw table handle — the state a bucket
/// that vanished out of band leaves behind, which no contract call can
/// write (the setter refuses a bucket with no row).
async fn plant(store: &RedbMetadata, bucket: &str, config: lifecycle::Config) {
    let bucket = bucket.to_owned();
    store
        .driver()
        .write(move |txn| LifecycleTable::open(txn)?.set(&bucket, &config))
        .await
        .unwrap();
}

/// One valid object row to plant for the occupancy probe.
fn held_row() -> Stored {
    Stored {
        etag: "d41d8cd98f00b204e9800998ecf8427e".parse::<ETag>().unwrap(),
        size: 0,
        mtime: 0,
        file_identity: 0,
        tags: object::Tags::empty(),
        checksum: None,
        owner: None,
        acl: Acl::default_private(None),
        version_id: None,
        version_seq: 0,
        is_delete_marker: false,
    }
}

/// The suite's bucket leg against a fresh in-memory implementation.
#[tokio::test]
async fn bucket_meta_conforms() {
    let store = RedbMetadata::in_memory().unwrap();
    assert_bucket_meta_conformance(&store).await;
}

/// The lifecycle setters answer bucket presence, never change: an
/// identical re-write and a clear on a bucket that stores none both
/// answer `true`, and only a missing bucket answers `false`.
#[tokio::test]
async fn the_lifecycle_setters_answer_bucket_presence() {
    let store = RedbMetadata::in_memory().unwrap();
    let name = bucket::name("lifecycle").unwrap();
    let config = lifecycle_config("expire-logs");

    // No bucket row: nothing is written and both setters answer `false`.
    assert!(!store.set_lifecycle(&name, &config).await.unwrap());
    assert!(store.lifecycle(&name).await.unwrap().is_none());
    assert!(!store.clear_lifecycle(&name).await.unwrap());
    assert!(store.lifecycles().await.unwrap().is_empty());

    store.create(&name, &row::Row::at(t(1))).await.unwrap();
    assert!(store.set_lifecycle(&name, &config).await.unwrap());
    assert_eq!(store.lifecycle(&name).await.unwrap(), Some(config.clone()));
    assert!(
        store.set_lifecycle(&name, &config).await.unwrap(),
        "an identical configuration is still presence"
    );
    assert_eq!(
        store.lifecycles().await.unwrap(),
        vec![(name.clone(), config)]
    );
    assert!(store.clear_lifecycle(&name).await.unwrap());
    assert!(store.lifecycle(&name).await.unwrap().is_none());
    assert!(
        store.clear_lifecycle(&name).await.unwrap(),
        "a bucket that stores no configuration is still present"
    );
    assert!(store.lifecycles().await.unwrap().is_empty());
}

/// LC19's other half, which the shared suite cannot reach: once `remove`
/// drains the row no contract call leaves a stale one behind, so the plant
/// is raw. Both creation paths clear it in the write that lands the bucket
/// row, and `remove_if_empty` drains it exactly as `remove` does.
#[tokio::test]
async fn the_creation_paths_clear_a_stale_lifecycle_row() {
    let store = RedbMetadata::in_memory().unwrap();
    let name = bucket::name("stale-lifecycle").unwrap();
    let key = name.to_string();

    plant(&store, &key, lifecycle_config("expire-logs")).await;
    store.create(&name, &row::Row::at(t(1))).await.unwrap();
    assert!(
        store.lifecycle(&name).await.unwrap().is_none(),
        "create clears the stale row in the write that lands the bucket row"
    );

    plant(&store, &key, lifecycle_config("expire-logs")).await;
    store.record_full(&name, &row::Row::at(t(2))).await.unwrap();
    assert!(
        store.lifecycle(&name).await.unwrap().is_none(),
        "record_full (the lazy policy's re-create) clears it too"
    );

    assert!(
        store
            .set_lifecycle(&name, &lifecycle_config("expire-logs"))
            .await
            .unwrap()
    );
    store.remove_if_empty(&name).await.unwrap();
    assert!(
        store.lifecycle(&name).await.unwrap().is_none(),
        "remove_if_empty takes the rule with the bucket"
    );
    assert!(store.row(&name).await.unwrap().is_none());
}

/// The refusals D22 pins that the bucket leg alone cannot reach:
/// `create` answers `AlreadyExists` for a recorded name, and
/// `remove_if_empty` answers `NoSuchBucket` for a missing row and
/// `NotEmpty` while another category's row still names the bucket.
#[tokio::test]
async fn refusals_project_onto_the_contract_error() {
    let store = RedbMetadata::in_memory().unwrap();
    let name = bucket::name("refusing").unwrap();
    store.create(&name, &row::Row::at(t(1))).await.unwrap();
    let refused: storage::Error = store
        .create(&name, &row::Row::at(t(2)))
        .await
        .unwrap_err()
        .into();
    assert!(
        matches!(refused, storage::Error::AlreadyExists(_)),
        "{refused:?}"
    );

    let key = name.to_string();
    let planted = key.clone();
    store
        .driver()
        .write(move |txn| -> Result<(), Error> {
            object_meta::Table::open(txn)?.put(&planted, "held.txt", &held_row())
        })
        .await
        .unwrap();
    let refused: storage::Error = store.remove_if_empty(&name).await.unwrap_err().into();
    assert!(
        matches!(refused, storage::Error::NotEmpty(_)),
        "an object row keeps the bucket non-empty, got {refused:?}"
    );
    store
        .driver()
        .write(move |txn| -> Result<(), Error> {
            object_meta::Table::open(txn)?.remove(&key, "held.txt")
        })
        .await
        .unwrap();

    store.remove_if_empty(&name).await.unwrap();
    let refused: storage::Error = store.remove_if_empty(&name).await.unwrap_err().into();
    assert!(
        matches!(refused, storage::Error::NoSuchBucket(_)),
        "a removed row refuses as NoSuchBucket, got {refused:?}"
    );
}

/// The create-or-update tag write is ONE transaction in both arms: a
/// first-sight record and a replace are each one write transaction, and
/// the record lands WITH the tags (a reader never sees a bare row).
/// The write-transaction count is the driver's own (`write_lock_stats`,
/// one per `timed_write`, commit or abort).
#[tokio::test]
async fn set_tags_or_create_records_and_replaces_in_one_transaction() {
    let store = RedbMetadata::in_memory().unwrap();
    let name = bucket::name("first-sight-tags").unwrap();
    let writes = || store.driver().write_lock_stats().count;

    // First sight: one write transaction, the row recorded with the
    // tags at `now`.
    let before = writes();
    store
        .set_tags_or_create(&name, &tags(&[("env", "prod")]), t(7))
        .await
        .unwrap();
    assert_eq!(
        writes() - before,
        1,
        "the first-sight tag record is one write"
    );
    let recorded = store.row(&name).await.unwrap().expect("the recorded row");
    assert_eq!(recorded.created, t(7), "the first-sight row carries `now`");
    assert_eq!(
        pairs(&recorded.tags),
        vec![("env".to_owned(), "prod".to_owned())]
    );
    assert!(recorded.cors.is_none(), "a fresh row has no other element");

    // Replace: one write transaction, the creation time and the other
    // elements ride.
    let config = cors::Config {
        rules: vec![cors::Rule {
            id: Some("kept".to_owned()),
            allowed_methods: vec!["GET".to_owned()],
            allowed_origins: vec!["*".to_owned()],
            allowed_headers: None,
            expose_headers: None,
            max_age_seconds: None,
        }],
    };
    store.set_cors(&name, &config).await.unwrap();
    let before = writes();
    store
        .set_tags_or_create(&name, &tags(&[("team", "core")]), t(9))
        .await
        .unwrap();
    assert_eq!(writes() - before, 1, "the tag replace is one write");
    let replaced = store.row(&name).await.unwrap().unwrap();
    assert_eq!(replaced.created, t(7), "the recorded time is preserved");
    assert_eq!(
        pairs(&replaced.tags),
        vec![("team".to_owned(), "core".to_owned())]
    );
    assert_eq!(
        replaced.cors,
        Some(config),
        "the other elements are preserved"
    );

    // An identical set is a no-op body (the transaction still opens and
    // aborts): the row is unchanged.
    let before = writes();
    store
        .set_tags_or_create(&name, &tags(&[("team", "core")]), t(11))
        .await
        .unwrap();
    assert_eq!(
        writes() - before,
        1,
        "an identical set is still one transaction"
    );
    assert_eq!(store.row(&name).await.unwrap().unwrap(), replaced);
}

/// The grants create-or-update write is ONE transaction in both arms and
/// preserves the recorded owner element: the first-sight row carries the
/// grants and no owner, a later write replaces only the grants.
#[tokio::test]
async fn set_grants_or_create_preserves_the_owner_element() {
    let store = RedbMetadata::in_memory().unwrap();
    let name = bucket::name("first-sight-grants").unwrap();
    let writes = || store.driver().write_lock_stats().count;
    let held = owner();

    // First sight: one write transaction, the grants recorded, no owner.
    let before = writes();
    store
        .set_grants_or_create(&name, &grants(), t(3))
        .await
        .unwrap();
    assert_eq!(
        writes() - before,
        1,
        "the first-sight grant record is one write"
    );
    let recorded = store.row(&name).await.unwrap().expect("the recorded row");
    assert_eq!(recorded.created, t(3));
    assert_eq!(recorded.acl.grants, grants());
    assert_eq!(
        recorded.owner, None,
        "a bucket never sighted through the API carries no owner"
    );

    // A recorded owner (the create-time ACL row) rides the later grants
    // write: the row read and the write are the same transaction, so the
    // owner's writer cannot slip between them.
    let mut with_owner = recorded.clone();
    with_owner.owner = Some(held.clone());
    store.record_full(&name, &with_owner).await.unwrap();
    let empty: AclGrants = Vec::new();
    let before = writes();
    store
        .set_grants_or_create(&name, &empty, t(5))
        .await
        .unwrap();
    assert_eq!(writes() - before, 1, "the grant replace is one write");
    let replaced = store.row(&name).await.unwrap().unwrap();
    assert_eq!(
        replaced.owner,
        Some(held),
        "the recorded owner is carried over"
    );
    assert!(replaced.acl.grants.is_empty(), "the grants are replaced");
    assert_eq!(replaced.created, t(3), "the recorded time is preserved");

    // An identical set is a no-op body: the row is unchanged.
    let before = writes();
    store
        .set_grants_or_create(&name, &empty, t(13))
        .await
        .unwrap();
    assert_eq!(
        writes() - before,
        1,
        "an identical set is still one transaction"
    );
    assert_eq!(store.row(&name).await.unwrap().unwrap(), replaced);
}

/// One enabled Object Lock configuration with an optional default rule.
fn lock_config(period: Option<object_lock::Period>) -> object_lock::Config {
    object_lock::Config {
        enabled: true,
        rule: period.map(|period| object_lock::Rule {
            default_retention: object_lock::DefaultRetention {
                mode: object_lock::Mode::Governance,
                period,
            },
        }),
    }
}

/// Plant a configuration row without a bucket row — the state a bucket
/// that vanished out of band leaves behind, which no contract write can
/// produce (the setter refuses a bucket with no row).
async fn plant_config(store: &RedbMetadata, bucket: &str, config: object_lock::Config) {
    let bucket = bucket.to_owned();
    store
        .driver()
        .write(move |txn| ObjectLockTable::open(txn)?.set(&bucket, &config))
        .await
        .unwrap();
}

/// The configuration obeys the lifecycle read's presence rule and clears
/// idempotently (`Ok(true)` is presence, never change).
#[tokio::test]
async fn the_object_lock_configuration_obeys_the_presence_rule() {
    let store = RedbMetadata::in_memory().unwrap();
    let name = bucket::name("object-lock").unwrap();
    let config = lock_config(Some(object_lock::Period::Days(30)));

    // A missing bucket row: the read is NoSuchBucket, the writes answer
    // `false` and write nothing.
    let refused: storage::Error = store.object_lock(&name).await.unwrap_err().into();
    assert!(
        matches!(refused, storage::Error::NoSuchBucket(_)),
        "{refused:?}"
    );
    assert!(!store.set_object_lock(&name, &config).await.unwrap());
    assert!(!store.clear_object_lock(&name).await.unwrap());

    store.create(&name, &row::Row::at(t(1))).await.unwrap();
    // OL13: a configuration write requires versioning `Enabled`.
    store.set_bucket_state(&name, State::Enabled).await.unwrap();
    assert_eq!(store.object_lock(&name).await.unwrap(), None);
    assert!(store.set_object_lock(&name, &config).await.unwrap());
    assert_eq!(
        store.object_lock(&name).await.unwrap(),
        Some(config.clone())
    );
    assert!(
        store.set_object_lock(&name, &config).await.unwrap(),
        "an identical write is still presence"
    );
    assert!(store.clear_object_lock(&name).await.unwrap());
    assert_eq!(store.object_lock(&name).await.unwrap(), None);
    assert!(
        store.clear_object_lock(&name).await.unwrap(),
        "a clear on a bucket that stores none is still presence"
    );
}

/// OL13: the configuration write reads the versioning state in its own
/// transaction — an unversioned or suspended bucket refuses and stores
/// nothing, and an `Enabled` one accepts.
#[tokio::test]
async fn the_configuration_write_requires_enabled_versioning() {
    let store = RedbMetadata::in_memory().unwrap();
    let name = bucket::name("lock-config-state").unwrap();
    let config = lock_config(None);
    store.create(&name, &row::Row::at(t(1))).await.unwrap();

    for state in [State::Unversioned, State::Suspended] {
        store.set_bucket_state(&name, state).await.unwrap();
        let refused = store.set_object_lock(&name, &config).await.unwrap_err();
        assert!(
            matches!(
                storage::Error::from(refused),
                storage::Error::InvalidBucketState(_)
            ),
            "a {state:?} bucket refuses the configuration"
        );
        assert!(
            store.object_lock(&name).await.unwrap().is_none(),
            "the refused write stored nothing"
        );
    }

    store.set_bucket_state(&name, State::Enabled).await.unwrap();
    assert!(store.set_object_lock(&name, &config).await.unwrap());
    assert_eq!(store.object_lock(&name).await.unwrap(), Some(config));
}

/// A corrupt rule is no rule while the enabled bit survives (OL4): the
/// enforcement fact never depends on a decodable default.
#[tokio::test]
async fn a_corrupt_rule_reads_as_no_rule_and_keeps_the_enabled_bit() {
    let store = RedbMetadata::in_memory().unwrap();
    let name = bucket::name("corrupt-lock-rule").unwrap();
    store.create(&name, &row::Row::at(t(1))).await.unwrap();
    let bucket = name.to_string();
    store
        .driver()
        .write(move |txn| -> Result<(), Error> {
            let mut table = ObjectLockTable::open(txn)?;
            table
                .0
                .insert(bucket.as_str(), (true, "not a rule"))
                .map_err(Error::from)?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        store.object_lock(&name).await.unwrap(),
        Some(object_lock::Config {
            enabled: true,
            rule: None
        })
    );
}

/// OL4: **every** bucket-row writer clears a stale configuration in its
/// own transaction — `create`, `record`, `record_if_absent`, the lazy
/// first-sight writers, `record_full`, `remove` and `remove_if_empty`.
#[tokio::test]
async fn every_bucket_row_writer_clears_a_stale_object_lock_configuration() {
    let store = RedbMetadata::in_memory().unwrap();
    let name = bucket::name("stale-lock").unwrap();
    let key = name.to_string();
    let config = lock_config(Some(object_lock::Period::Days(1)));

    // A stale row with no bucket row, then each writer that creates one.
    plant_config(&store, &key, config.clone()).await;
    store.create(&name, &row::Row::at(t(1))).await.unwrap();
    assert!(store.object_lock(&name).await.unwrap().is_none(), "create");
    store.remove(&name).await.unwrap();

    plant_config(&store, &key, config.clone()).await;
    store.record(&name, t(2)).await.unwrap();
    assert!(store.object_lock(&name).await.unwrap().is_none(), "record");
    store.remove(&name).await.unwrap();

    plant_config(&store, &key, config.clone()).await;
    let (_, inserted) = store.record_if_absent(&name, t(3)).await.unwrap();
    assert!(inserted);
    assert!(
        store.object_lock(&name).await.unwrap().is_none(),
        "record_if_absent's insert"
    );
    // An existing row's writer is not a bucket-row write: the
    // configuration rides.
    store.set_bucket_state(&name, State::Enabled).await.unwrap();
    store.set_object_lock(&name, &config).await.unwrap();
    let (_, inserted) = store.record_if_absent(&name, t(4)).await.unwrap();
    assert!(!inserted);
    assert_eq!(
        store.object_lock(&name).await.unwrap(),
        Some(config.clone())
    );
    store.remove(&name).await.unwrap();

    plant_config(&store, &key, config.clone()).await;
    store
        .set_tags_or_create(&name, &tags(&[("env", "prod")]), t(5))
        .await
        .unwrap();
    assert!(
        store.object_lock(&name).await.unwrap().is_none(),
        "set_tags_or_create's first-sight insert"
    );
    store.set_object_lock(&name, &config).await.unwrap();
    store
        .set_tags_or_create(&name, &tags(&[("env", "dev")]), t(6))
        .await
        .unwrap();
    assert_eq!(
        store.object_lock(&name).await.unwrap(),
        Some(config.clone()),
        "an existing row's tag write keeps the configuration"
    );
    store.remove(&name).await.unwrap();

    plant_config(&store, &key, config.clone()).await;
    store
        .set_lifecycle_or_create(&name, &lifecycle_config("expire-logs"), t(7))
        .await
        .unwrap();
    assert!(
        store.object_lock(&name).await.unwrap().is_none(),
        "set_lifecycle_or_create's first-sight insert"
    );
    store.remove(&name).await.unwrap();

    // The grants writer's first-sight insert is a bucket-row write too;
    // replacing an existing row's grants is not.
    plant_config(&store, &key, config.clone()).await;
    store
        .set_grants_or_create(&name, &grants(), t(9))
        .await
        .unwrap();
    assert!(
        store.object_lock(&name).await.unwrap().is_none(),
        "set_grants_or_create's first-sight insert"
    );
    store.set_object_lock(&name, &config).await.unwrap();
    let empty: AclGrants = Vec::new();
    store
        .set_grants_or_create(&name, &empty, t(10))
        .await
        .unwrap();
    assert_eq!(
        store.object_lock(&name).await.unwrap(),
        Some(config.clone()),
        "an existing row's grant write keeps the configuration"
    );
    store.remove(&name).await.unwrap();

    // `merge_lifecycle_or_create` is the lazy `merge_row` writer: its
    // first-sight bucket row clears the configuration; a merge over an
    // existing row does not (`create_at` is spent only when the row is
    // new).
    plant_config(&store, &key, config.clone()).await;
    assert_eq!(
        store
            .merge_lifecycle_or_create(&name, &lifecycle_config("expire-logs").rules, t(11))
            .await
            .unwrap(),
        lifecycle::Merged::Stored
    );
    assert!(
        store.object_lock(&name).await.unwrap().is_none(),
        "merge_lifecycle_or_create's first-sight insert"
    );
    store.set_object_lock(&name, &config).await.unwrap();
    store
        .merge_lifecycle_or_create(&name, &lifecycle_config("keep-logs").rules, t(12))
        .await
        .unwrap();
    assert_eq!(
        store.object_lock(&name).await.unwrap(),
        Some(config.clone()),
        "a merge over an existing row keeps the configuration"
    );
    store.remove(&name).await.unwrap();

    store.create(&name, &row::Row::at(t(8))).await.unwrap();
    store.set_object_lock(&name, &config).await.unwrap();
    store.record_full(&name, &row::Row::at(t(8))).await.unwrap();
    assert!(
        store.object_lock(&name).await.unwrap().is_none(),
        "record_full (the lazy policy's re-create)"
    );

    store.set_object_lock(&name, &config).await.unwrap();
    store.remove_if_empty(&name).await.unwrap();
    let refused: storage::Error = store.object_lock(&name).await.unwrap_err().into();
    assert!(
        matches!(refused, storage::Error::NoSuchBucket(_)),
        "remove_if_empty takes the configuration with the bucket (the row is gone)"
    );

    store.create(&name, &row::Row::at(t(9))).await.unwrap();
    store.set_object_lock(&name, &config).await.unwrap();
    store.remove(&name).await.unwrap();
    let refused: storage::Error = store.object_lock(&name).await.unwrap_err().into();
    assert!(
        matches!(refused, storage::Error::NoSuchBucket(_)),
        "remove takes the configuration with the bucket (the row is gone)"
    );
}
