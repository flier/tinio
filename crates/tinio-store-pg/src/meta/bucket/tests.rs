use std::{
    future::Future,
    slice::from_ref,
    time::{Duration, SystemTime},
};

use tinio_util::testing::object_key;
use tokio::time::sleep;

use crate::{
    _core::{
        ETag,
        acl::{Acl, AclGrants, GROUP_ALL_USERS, Grant, Grantee, GroupUri, OwnerId, Permission},
        bucket::Name,
        meta::{BucketMeta, VersionMeta, bucket::Row as BucketRow, object::Row as ObjectRow},
        object, object_lock,
        version::State,
    },
    Error, PgStore, begin,
    meta::{assert_bucket_row, object::upsert_object},
    tests::fixtures::{bucket_row, bucket_tags, name, owner, scratch},
    timestamp::{unix_micros, unix_seconds as at},
};

#[tokio::test]
async fn the_creation_time_round_trips_with_sub_second_precision() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-bucket-micros");
    let created = unix_micros(1_700_000_000_123_456);
    BucketMeta::create(&store, &data, &BucketRow::at(created))
        .await
        .unwrap();
    assert_eq!(
        BucketMeta::created_at(&store, &data).await.unwrap(),
        Some(created),
        "timestamptz keeps the sub-second digits"
    );
    assert_eq!(
        BucketMeta::row(&store, &data)
            .await
            .unwrap()
            .unwrap()
            .created,
        created
    );
    db.drop_db().await;
}

#[tokio::test]
async fn the_bucket_row_round_trips_and_orders_by_bytes() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-bucket-data");
    let other = name("plane-bucket-other");
    let mut row = BucketRow::at(at(10));
    row.tags = bucket_tags(&[("env", "prod")]);
    BucketMeta::create(&store, &data, &row).await.unwrap();
    BucketMeta::create(&store, &other, &BucketRow::at(at(20)))
        .await
        .unwrap();
    assert_eq!(
        BucketMeta::created_at(&store, &data).await.unwrap(),
        Some(at(10))
    );
    let reloaded = BucketMeta::row(&store, &data).await.unwrap().unwrap();
    assert_eq!(reloaded.tags, row.tags);
    assert_eq!(reloaded.created, at(10));
    let all = BucketMeta::load_all(&store).await.unwrap();
    assert_eq!(
        all.iter().map(|(name, _)| name.clone()).collect::<Vec<_>>(),
        vec![data.to_string(), other.to_string()],
        "the listing orders the names by their UTF-8 bytes"
    );
    let many = BucketMeta::load_many(&store, &[other.clone(), name("plane-bucket-absent")])
        .await
        .unwrap();
    assert_eq!(many, vec![Some(at(20)), None]);
    // One answer per occurrence: a name asked for twice is answered twice,
    // not `None` on its second read.
    assert_eq!(
        BucketMeta::load_many(&store, &[other.clone(), other.clone()])
            .await
            .unwrap(),
        vec![Some(at(20)), Some(at(20))],
        "a duplicated name answers its creation time per occurrence"
    );
    assert_eq!(
        BucketMeta::owners(&store, from_ref(&data)).await.unwrap(),
        vec![None]
    );
    assert!(matches!(
        BucketMeta::create(&store, &data, &row).await,
        Err(Error::AlreadyExists(_))
    ));
    db.drop_db().await;
}

#[tokio::test]
async fn the_element_setters_leave_a_missing_row_alone_and_the_creators_land() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-bucket-setters");
    assert!(
        !BucketMeta::set_tags(&store, &data, &bucket_tags(&[("a", "b")]))
            .await
            .unwrap()
    );
    assert!(!BucketMeta::clear_tags(&store, &data).await.unwrap());
    assert!(
        !BucketMeta::set_acl(&store, &data, None, &AclGrants::new())
            .await
            .unwrap()
    );
    assert!(!BucketMeta::clear_cors(&store, &data).await.unwrap());
    BucketMeta::set_tags_or_create(&store, &data, &bucket_tags(&[("a", "b")]), at(1))
        .await
        .unwrap();
    assert_eq!(
        BucketMeta::created_at(&store, &data).await.unwrap(),
        Some(at(1))
    );
    BucketMeta::set_grants_or_create(
        &store,
        &data,
        &Acl::default_private(Some(owner())).grants,
        at(2),
    )
    .await
    .unwrap();
    // The creation time is the first writer's, never the second's.
    assert_eq!(
        BucketMeta::created_at(&store, &data).await.unwrap(),
        Some(at(1))
    );
    assert_eq!(
        BucketMeta::row(&store, &data).await.unwrap().unwrap().tags,
        bucket_tags(&[("a", "b")])
    );
    // record clears the recorded elements; record_if_absent keeps the stamp.
    BucketMeta::record(&store, &data, at(5)).await.unwrap();
    let recorded = BucketMeta::row(&store, &data).await.unwrap().unwrap();
    assert_eq!(recorded.created, at(5));
    assert!(recorded.tags.is_empty());
    assert!(recorded.owner.is_none());
    assert_eq!(
        BucketMeta::record_if_absent(&store, &data, at(9))
            .await
            .unwrap(),
        (at(5), false)
    );
    db.drop_db().await;
}

#[tokio::test]
async fn acl_elements_round_trip_and_the_bucket_lifecycle_is_idempotent() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-bucket-acl");
    BucketMeta::create(&store, &data, &BucketRow::at(at(1)))
        .await
        .unwrap();
    let owner = owner();
    let grants = Acl::default_private(Some(owner.clone())).grants;
    assert!(
        BucketMeta::set_acl(&store, &data, Some(&owner), &grants)
            .await
            .unwrap()
    );
    let reloaded = BucketMeta::row(&store, &data).await.unwrap().unwrap();
    assert_eq!(reloaded.owner.as_ref(), Some(&owner));
    assert_eq!(
        reloaded.acl.grants, grants,
        "the decoded ACL carries the grants; the owner rides the composite attribute"
    );
    assert_eq!(
        BucketMeta::owners(&store, from_ref(&data)).await.unwrap(),
        vec![Some(owner)]
    );
    assert!(
        BucketMeta::set_tags(&store, &data, &bucket_tags(&[("x", "y")]))
            .await
            .unwrap()
    );
    assert!(BucketMeta::clear_tags(&store, &data).await.unwrap());
    assert!(
        BucketMeta::row(&store, &data)
            .await
            .unwrap()
            .unwrap()
            .tags
            .is_empty()
    );
    // remove_if_empty removes a bucket with nothing in it and refuses an
    // absent one.
    BucketMeta::remove_if_empty(&store, &data).await.unwrap();
    assert!(
        BucketMeta::created_at(&store, &data)
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        BucketMeta::remove_if_empty(&store, &data).await,
        Err(Error::NoSuchBucket(_))
    ));
    BucketMeta::remove(&store, &data).await.unwrap();
    BucketMeta::remove(&store, &data).await.unwrap();
    db.drop_db().await;
}

/// Every ACL shape the contract can hold round-trips through the stored
/// composite: an owner with no grants, a single grant, several grants (whose
/// stored order is canonical) and no owner at all.
#[tokio::test]
async fn every_acl_shape_round_trips_through_the_composite() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-bucket-acl-shapes");
    BucketMeta::create(&store, &data, &BucketRow::at(at(1)))
        .await
        .unwrap();
    let owner = owner();
    let other = OwnerId::new("f".repeat(64)).expect("a second canonical id");
    let canonical = Grant {
        grantee: Grantee::Canonical(owner.clone()),
        permission: Permission::FullControl,
    };
    let group = Grant {
        grantee: Grantee::Group(GroupUri(GROUP_ALL_USERS.to_owned())),
        permission: Permission::Read,
    };
    // The owner alone: the composite's grants are an empty array.
    assert!(
        BucketMeta::set_acl(&store, &data, Some(&owner), &AclGrants::new())
            .await
            .unwrap()
    );
    let reloaded = BucketMeta::row(&store, &data).await.unwrap().unwrap();
    assert_eq!(reloaded.owner.as_ref(), Some(&owner));
    assert!(reloaded.acl.grants.is_empty());
    assert_eq!(
        BucketMeta::owners(&store, from_ref(&data)).await.unwrap(),
        vec![Some(owner.clone())]
    );
    // One grant.
    let one = vec![canonical.clone()];
    assert!(
        BucketMeta::set_acl(&store, &data, Some(&other), &one)
            .await
            .unwrap()
    );
    let reloaded = BucketMeta::row(&store, &data).await.unwrap().unwrap();
    assert_eq!(reloaded.owner.as_ref(), Some(&other));
    assert_eq!(reloaded.acl.grants, one);
    // Several grants, stored in the canonical order whatever order they
    // arrive in.
    let several = vec![group.clone(), canonical.clone()];
    assert!(
        BucketMeta::set_acl(&store, &data, Some(&other), &several)
            .await
            .unwrap()
    );
    assert_eq!(
        BucketMeta::row(&store, &data)
            .await
            .unwrap()
            .unwrap()
            .acl
            .grants,
        vec![canonical, group]
    );
    // No owner and no grant: the composite's owner attribute is NULL.
    assert!(
        BucketMeta::set_acl(&store, &data, None, &AclGrants::new())
            .await
            .unwrap()
    );
    let reloaded = BucketMeta::row(&store, &data).await.unwrap().unwrap();
    assert!(reloaded.owner.is_none());
    assert!(reloaded.acl.grants.is_empty());
    assert_eq!(
        BucketMeta::owners(&store, from_ref(&data)).await.unwrap(),
        vec![None]
    );
    db.drop_db().await;
}

/// An object that commits while bucket removal is waiting on the bucket
/// lock is visible to the emptiness check. The check is the statement
/// after the lock, so it does not keep the snapshot from before the wait.
#[tokio::test]
async fn a_put_that_commits_while_remove_waits_keeps_the_bucket() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-bucket-remove-race");
    BucketMeta::create(&store, &data, &BucketRow::at(at(1)))
        .await
        .unwrap();
    let racing = store.clone();
    let bucket = data.clone();
    let err = object_commits_while_delete_waits(&store, &data, async move {
        BucketMeta::remove_if_empty(&racing, &bucket).await
    })
    .await
    .expect_err("the object landed before the delete could commit");
    assert!(matches!(err, Error::NotEmpty(_)));
    assert!(BucketMeta::row(&store, &data).await.unwrap().is_some());
    db.drop_db().await;
}

/// The same wait, for the store's own bucket teardown
/// (`PgStore::teardown_bucket`, `crate::teardown`), which drops the whole
/// namespace and shares the bucket-row lock with `remove_if_empty`. The
/// object committed while the teardown held no snapshot yet, so the bucket
/// stays.
#[tokio::test]
async fn a_put_that_commits_while_teardown_waits_keeps_the_bucket() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-bucket-teardown-race");
    BucketMeta::create(&store, &data, &BucketRow::at(at(1)))
        .await
        .unwrap();
    let racing = store.clone();
    let bucket = data.clone();
    let err = object_commits_while_delete_waits(&store, &data, async move {
        racing.teardown_bucket(&bucket).await
    })
    .await
    .expect_err("the object landed before the teardown could commit");
    assert!(matches!(err, Error::NotEmpty(_)));
    assert!(BucketMeta::row(&store, &data).await.unwrap().is_some());
    db.drop_db().await;
}

/// The stored rule wire is not trusted: a corrupt rule decodes to no rule,
/// while the enabled bit — the fact that drives enforcement — survives
/// (OL4).
#[tokio::test]
async fn a_corrupt_rule_reads_as_no_rule_with_the_enabled_bit() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-bucket-lock-rule");
    BucketMeta::create(&store, &data, &BucketRow::at(at(1)))
        .await
        .unwrap();
    sqlx::query("INSERT INTO bucket_object_lock (bucket, enabled, rule) VALUES ($1, TRUE, $2)")
        .bind(data.as_ref())
        .bind("GOVERNANCE,DAYS,not-a-number")
        .execute(store.as_driver())
        .await
        .unwrap();
    assert_eq!(
        BucketMeta::object_lock(&store, &data).await.unwrap(),
        Some(object_lock::Config {
            enabled: true,
            rule: None
        }),
        "the enabled bit survives a rule the model cannot read"
    );
    db.drop_db().await;
}

/// OL4's writer rule: a writer that installs or replaces the bucket row
/// clears the configuration, an element writer and the lazy first-sight
/// path keep an existing bucket's configuration, and a delete (the schema's
/// `ON DELETE CASCADE`) takes it with the bucket.
#[tokio::test]
async fn the_bucket_row_writers_clear_the_configuration_and_element_writers_keep_it() {
    use crate::_core::lifecycle;

    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-bucket-lock-writers");
    let config = object_lock::Config {
        enabled: true,
        rule: None,
    };
    BucketMeta::create(&store, &data, &BucketRow::at(at(1)))
        .await
        .unwrap();
    // OL13: a configuration write requires versioning `Enabled`.
    VersionMeta::set_bucket_state(&store, &data, State::Enabled)
        .await
        .unwrap();
    assert!(
        BucketMeta::set_object_lock(&store, &data, &config)
            .await
            .unwrap()
    );
    // An existing bucket's element writers and lazy creates keep it.
    BucketMeta::record_if_absent(&store, &data, at(9))
        .await
        .unwrap();
    BucketMeta::set_tags_or_create(&store, &data, &bucket_tags(&[("a", "b")]), at(9))
        .await
        .unwrap();
    BucketMeta::set_grants_or_create(
        &store,
        &data,
        &Acl::default_private(Some(owner())).grants,
        at(9),
    )
    .await
    .unwrap();
    BucketMeta::set_lifecycle_or_create(
        &store,
        &data,
        &lifecycle::Config { rules: Vec::new() },
        at(9),
    )
    .await
    .unwrap();
    assert_eq!(
        BucketMeta::object_lock(&store, &data).await.unwrap(),
        Some(config.clone()),
        "an existing bucket's element writers keep the configuration"
    );
    // `record` and `record_full` install a fresh bucket row: both clear it.
    BucketMeta::record(&store, &data, at(10)).await.unwrap();
    assert_eq!(
        BucketMeta::object_lock(&store, &data).await.unwrap(),
        None,
        "record is a bucket-row writer and clears the configuration"
    );
    assert!(
        BucketMeta::set_object_lock(&store, &data, &config)
            .await
            .unwrap()
    );
    BucketMeta::record_full(&store, &data, &BucketRow::at(at(11)))
        .await
        .unwrap();
    assert_eq!(
        BucketMeta::object_lock(&store, &data).await.unwrap(),
        None,
        "record_full is a bucket-row writer and clears the configuration"
    );
    // A refused create writes nothing, so the stored configuration rides.
    assert!(
        BucketMeta::set_object_lock(&store, &data, &config)
            .await
            .unwrap()
    );
    assert!(matches!(
        BucketMeta::create(&store, &data, &BucketRow::at(at(12))).await,
        Err(Error::AlreadyExists(_))
    ));
    assert_eq!(
        BucketMeta::object_lock(&store, &data).await.unwrap(),
        Some(config),
        "a refused create leaves the configuration alone"
    );
    // `remove` relies on the schema's cascade.
    BucketMeta::remove(&store, &data).await.unwrap();
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM bucket_object_lock WHERE bucket = $1")
        .bind(data.as_ref())
        .fetch_one(store.as_driver())
        .await
        .unwrap();
    assert_eq!(rows, 0, "the FK cascade removes the configuration");
    db.drop_db().await;
}

/// Wait until some backend of **this test's database** is blocked on a
/// lock. The scope matters: the suite runs several scratch databases at
/// once, so the probe must not see another leg's waiter. The wait event,
/// not `pg_locks`, is the fact: a statement waiting on a concurrent
/// transaction's row (delete, or an uncommitted insert) waits on a
/// `transactionid` lock, whose `pg_locks.database` is `NULL`.
async fn wait_for_a_blocked_statement(store: &PgStore) -> bool {
    for _ in 0..200 {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) \
             FROM pg_stat_activity AS a \
             WHERE a.datname = current_database() \
                 AND a.pid <> pg_backend_pid() \
                 AND a.wait_event_type = 'Lock'",
        )
        .fetch_one(store.pool())
        .await
        .expect("the lock probe");
        if waiting > 0 {
            return true;
        }
        sleep(Duration::from_millis(10)).await;
    }
    false
}

/// The first-sight writer a presence-race leg drives, so the tag and ACL
/// arms share one interleaving rather than two near-copies.
#[derive(Clone)]
enum FirstSight {
    /// `set_tags_or_create`.
    Tags(object::Tags),
    /// `set_grants_or_create`.
    Grants(AclGrants),
}

impl FirstSight {
    /// Drive the arm against `bucket`.
    async fn write(self, store: &PgStore, bucket: &Name, now: SystemTime) -> Result<(), Error> {
        match self {
            Self::Tags(tags) => BucketMeta::set_tags_or_create(store, bucket, &tags, now).await,
            Self::Grants(grants) => {
                BucketMeta::set_grants_or_create(store, bucket, &grants, now).await
            }
        }
    }

    /// Whether the stored bucket row carries this arm's element.
    fn landed(&self, row: &BucketRow) -> bool {
        match self {
            Self::Tags(tags) => row.tags == *tags,
            Self::Grants(grants) => row.acl.grants == *grants,
        }
    }
}

/// **The presence-race seam, deterministically.** A concurrent
/// `create_bucket_with_lock` owns an **uncommitted** bucket row and Object
/// Lock configuration when the first-sight writer starts. The writer's
/// statement waits on that row, then runs against the committed one.
///
/// The documented outcome is the one the write-effect rule exists for: the
/// configuration is **not** cleared (it is not this statement's first
/// sight), and the writer's own element still lands (no lost update). An
/// implementation that took a presence read before its insert would see
/// "absent", then clear the configuration a concurrent create had committed
/// in the window — the lost update this leg forces.
#[tokio::test]
async fn a_concurrent_lock_enabled_create_survives_the_first_sight_writers() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    for (label, first_sight) in [
        (
            "plane-bucket-race-tags",
            FirstSight::Tags(bucket_tags(&[("a", "b")])),
        ),
        (
            "plane-bucket-race-grants",
            FirstSight::Grants(Acl::default_private(Some(owner())).grants),
        ),
    ] {
        let data = name(label);
        let expected = first_sight.clone();
        // The concurrent create: bucket row and configuration in the same
        // transaction, still uncommitted.
        let mut create = begin(store.pool()).await.unwrap();
        super::insert_bucket_row(&mut create, &data, &BucketRow::at(at(1)))
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO bucket_object_lock (bucket, enabled, rule) \
             VALUES ($1, TRUE, '')",
        )
        .bind(data.as_ref())
        .execute(&mut *create)
        .await
        .unwrap();

        let writer_store = store.clone();
        let writer_bucket = data.clone();
        let writer = tokio::spawn(async move {
            first_sight
                .write(&writer_store, &writer_bucket, at(2))
                .await
        });
        assert!(
            wait_for_a_blocked_statement(&store).await,
            "the first-sight write never waited on the uncommitted create"
        );
        create.commit().await.unwrap();
        writer.await.expect("the writer task").unwrap();

        assert_eq!(
            BucketMeta::object_lock(&store, &data).await.unwrap(),
            Some(object_lock::Config {
                enabled: true,
                rule: None,
            }),
            "{label}: the concurrent create's configuration survives"
        );
        assert_eq!(
            BucketMeta::created_at(&store, &data).await.unwrap(),
            Some(at(1)),
            "{label}: the create's stamp is the first writer's"
        );
        let row = BucketMeta::row(&store, &data).await.unwrap().unwrap();
        assert!(
            expected.landed(&row),
            "{label}: the first-sight writer's element still lands"
        );
    }
    db.drop_db().await;
}

/// **The lock writers' presence pre-read, deterministically.** The clear
/// takes the bucket row `FOR UPDATE` before it deletes the configuration.
/// A concurrent transaction holds that row with an uncommitted **delete**:
/// the pre-read waits, then re-evaluates against the committed state, finds
/// no bucket, and answers `false` — it never writes against the stale
/// state. The cascade removes the configuration with the row, so no orphan
/// configuration is left behind either.
#[tokio::test]
async fn a_lock_clear_waits_for_an_uncommitted_delete_and_writes_nothing() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-bucket-clear-race");
    bucket_row(&store, &data).await;
    sqlx::query("INSERT INTO bucket_object_lock (bucket, enabled, rule) VALUES ($1, TRUE, '')")
        .bind(data.as_ref())
        .execute(store.pool())
        .await
        .unwrap();

    let mut remove = begin(store.pool()).await.unwrap();
    sqlx::query("DELETE FROM buckets WHERE name = $1")
        .bind(data.as_ref())
        .execute(&mut *remove)
        .await
        .unwrap();
    let clear_store = store.clone();
    let clear_bucket = data.clone();
    let clear =
        tokio::spawn(
            async move { BucketMeta::clear_object_lock(&clear_store, &clear_bucket).await },
        );
    assert!(
        wait_for_a_blocked_statement(&store).await,
        "the clear's presence pre-read never waited on the uncommitted delete"
    );
    remove.commit().await.unwrap();

    assert!(
        !clear.await.expect("the clear task").unwrap(),
        "the pre-read re-evaluated against the committed state and found no bucket"
    );
    let keys: i64 = sqlx::query_scalar("SELECT count(*) FROM buckets WHERE name = $1")
        .bind(data.as_ref())
        .fetch_one(store.pool())
        .await
        .unwrap();
    let configs: i64 =
        sqlx::query_scalar("SELECT count(*) FROM bucket_object_lock WHERE bucket = $1")
            .bind(data.as_ref())
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(keys, 0, "the delete committed");
    assert_eq!(configs, 0, "the cascade left no orphan configuration");
    db.drop_db().await;
}

/// Hold the bucket lock, wait until `delete` is blocked on it, commit an
/// object, then let `delete` run.
async fn object_commits_while_delete_waits(
    store: &PgStore,
    bucket: &Name,
    delete: impl Future<Output = Result<(), Error>> + Send + 'static,
) -> Result<(), Error> {
    let mut tx = begin(store.pool()).await?;
    assert_bucket_row(&mut tx, bucket).await?;
    let task = tokio::spawn(delete);
    let mut saw_waiter = false;
    for _ in 0..50 {
        let waiting: i64 =
            sqlx::query_scalar!(r#"SELECT count(*) AS "waiting!" FROM pg_locks WHERE NOT granted"#)
                .fetch_one(store.pool())
                .await?;
        if waiting > 0 {
            saw_waiter = true;
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }
    assert!(saw_waiter, "the delete never waited on the bucket lock");
    let key = object_key("kept");
    upsert_object(
        &mut tx,
        bucket,
        &key,
        &ObjectRow {
            etag: ETag::EMPTY,
            size: 0,
            mtime: SystemTime::UNIX_EPOCH,
            identity: 0,
            tags: object::Tags::empty(),
            checksum: None,
            owner: None,
            acl: Acl::default_private(None),
        },
    )
    .await?;
    tx.commit().await?;
    task.await.expect("the delete task")
}
