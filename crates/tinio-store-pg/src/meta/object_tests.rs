use std::{slice::from_ref, time::Duration};

use tokio::time::sleep;

use crate::{
    _core::{
        ETag,
        acl::Acl,
        meta::{
            BucketMeta, ObjectMeta, VersionMeta,
            object::{Identity, PartRow, Row as ObjectRow},
        },
        object,
        version::Id,
    },
    Error, PgStore, begin,
    tests::fixtures::{ETAG_A, bucket_row, key, name, owner, scratch, tags},
    timestamp::{unix_micros, unix_seconds as at},
};

const ETAG_B: &str = "0cc175b9c0f1b6a831c399e269772661";

fn etag(value: &str) -> ETag {
    ETag::new(value).expect("a valid etag literal")
}

fn row(size: u64, etag_wire: &str) -> ObjectRow {
    ObjectRow {
        etag: etag(etag_wire),
        size,
        mtime: at(10),
        identity: 7,
        tags: object::Tags::empty(),
        checksum: None,
        owner: None,
        acl: Acl::default_private(None),
    }
}

fn identity(size: u64, etag_wire: &str) -> Identity {
    Identity {
        etag: etag(etag_wire),
        size,
        mtime: at(11),
        file_identity: 77,
    }
}

#[tokio::test]
async fn the_modification_time_round_trips_with_sub_second_precision() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-object-micros");
    bucket_row(&store, &data).await;
    let target = key("micros.bin");
    let mtime = unix_micros(1_700_000_000_654_321);
    let mut stored = row(6, ETAG_A);
    stored.mtime = mtime;
    ObjectMeta::put(&store, &data, &target, &stored)
        .await
        .unwrap();
    let got = ObjectMeta::get(&store, &data, &target)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.mtime, mtime, "timestamptz keeps the sub-second digits");
    db.drop_db().await;
}

#[tokio::test]
async fn object_rows_round_trip_and_walk_in_byte_order() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-object-walk");
    bucket_row(&store, &data).await;
    // The ordering pin: "C" gives data < data-2 < data/ < data_, while a
    // locale collation reorders them.
    for name in ["data/", "data-2", "data", "data_"] {
        ObjectMeta::put(&store, &data, &key(name), &row(name.len() as u64, ETAG_A))
            .await
            .unwrap();
    }
    let walked = ObjectMeta::walk(&store, &data).await.unwrap();
    assert_eq!(
        walked
            .iter()
            .map(|(k, _)| k.to_string())
            .collect::<Vec<_>>(),
        vec!["data", "data-2", "data/", "data_"],
        "the walk orders by the key's UTF-8 bytes"
    );
    let healed = ObjectMeta::walk_healing(&store, &data).await.unwrap();
    assert_eq!(healed.len(), 4);
    assert!(healed.iter().all(|(_, row)| row.is_some()));
    let loaded = ObjectMeta::load(&store, &data, &[key("data-2"), key("missing")])
        .await
        .unwrap();
    assert_eq!(loaded[0].as_ref().map(|row| row.size), Some(6));
    assert!(loaded[1].is_none());
    // An answer is per **occurrence**: a key asked for twice is answered
    // twice, not `None` on its second read (a repeated key is not a missing
    // one).
    let repeated = ObjectMeta::load(
        &store,
        &data,
        &[key("data-2"), key("data-2"), key("missing")],
    )
    .await
    .unwrap();
    assert_eq!(
        repeated
            .iter()
            .map(|row| row.as_ref().map(|row| row.size))
            .collect::<Vec<_>>(),
        vec![Some(6), Some(6), None],
        "a duplicated key answers its row per occurrence"
    );
    assert!(
        ObjectMeta::get(&store, &data, &key("missing"))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        ObjectMeta::remove(&store, &data, &key("data"))
            .await
            .unwrap()
    );
    assert!(
        !ObjectMeta::remove(&store, &data, &key("data"))
            .await
            .unwrap()
    );
    ObjectMeta::drain_bucket(&store, &data).await.unwrap();
    assert!(ObjectMeta::walk(&store, &data).await.unwrap().is_empty());
    db.drop_db().await;
}

#[tokio::test]
async fn tag_sets_round_trip_through_jsonb() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-object-tags");
    bucket_row(&store, &data).await;
    let target = key("tagged.bin");
    // The empty set is the jsonb empty object, not the wire's empty string.
    ObjectMeta::put(&store, &data, &target, &row(6, ETAG_A))
        .await
        .unwrap();
    assert_eq!(
        ObjectMeta::get(&store, &data, &target)
            .await
            .unwrap()
            .unwrap()
            .tags,
        object::Tags::empty()
    );
    // Several pairs, including the characters the fs/mem wire encoding
    // escapes ('=', '+', space) and non-ASCII letters.
    let set = tags(&[
        ("a=b", "x+y"),
        ("tag key", "value with space"),
        ("schlüssel", "größer"),
    ]);
    ObjectMeta::ensure_tags(&store, &data, &target, &set)
        .await
        .unwrap();
    assert_eq!(
        ObjectMeta::get(&store, &data, &target)
            .await
            .unwrap()
            .unwrap()
            .tags,
        set,
        "the stored jsonb object decodes to the same tag set"
    );
    assert_eq!(
        ObjectMeta::walk(&store, &data).await.unwrap()[0].1.tags,
        set,
        "the ordered walk serves the same set"
    );
    let replaced = tags(&[("only", "one")]);
    assert!(
        ObjectMeta::set_tags(&store, &data, &target, &replaced)
            .await
            .unwrap()
    );
    assert_eq!(
        ObjectMeta::get(&store, &data, &target)
            .await
            .unwrap()
            .unwrap()
            .tags,
        replaced
    );
    assert!(
        ObjectMeta::clear_tags(&store, &data, &target)
            .await
            .unwrap()
    );
    assert!(
        ObjectMeta::get(&store, &data, &target)
            .await
            .unwrap()
            .unwrap()
            .tags
            .is_empty()
    );
    // A bucket's own tag set rides the same column type and the same
    // contract mapping, so a re-read decodes the same jsonb object.
    let bucket_tags = tags(&[("bucket=key", "bucket+value")]);
    assert!(
        BucketMeta::set_tags(&store, &data, &bucket_tags)
            .await
            .unwrap()
    );
    assert_eq!(
        BucketMeta::row(&store, &data).await.unwrap().unwrap().tags,
        bucket_tags
    );
    db.drop_db().await;
}

#[tokio::test]
async fn put_with_parts_retains_the_list_and_every_overwrite_clears_it() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-object-parts");
    bucket_row(&store, &data).await;
    let target = key("multi.bin");
    let parts = vec![
        PartRow::from_raw(2, 6, "", ""),
        PartRow::from_raw(1, 5, "", ""),
    ];
    ObjectMeta::put_with_parts(&store, &data, &target, &row(11, ETAG_A), &parts)
        .await
        .unwrap();
    let listed = ObjectMeta::parts(&store, &data, &target).await.unwrap();
    assert_eq!(
        listed.iter().map(|part| part.number).collect::<Vec<_>>(),
        vec![1, 2],
        "the retained list orders numerically"
    );
    ObjectMeta::put(&store, &data, &target, &row(5, ETAG_A))
        .await
        .unwrap();
    assert!(
        ObjectMeta::parts(&store, &data, &target)
            .await
            .unwrap()
            .is_empty(),
        "put clears the retained parts"
    );
    ObjectMeta::put_with_parts(&store, &data, &target, &row(11, ETAG_A), &parts)
        .await
        .unwrap();
    ObjectMeta::put_guarded(&store, &data, &target, &row(7, ETAG_B))
        .await
        .unwrap();
    assert!(
        ObjectMeta::parts(&store, &data, &target)
            .await
            .unwrap()
            .is_empty(),
        "put_guarded clears the retained parts too"
    );
    ObjectMeta::put_with_parts(&store, &data, &target, &row(11, ETAG_A), &parts)
        .await
        .unwrap();
    ObjectMeta::put_batch(&store, &data, &[(target.clone(), row(7, ETAG_B))])
        .await
        .unwrap();
    assert!(
        ObjectMeta::parts(&store, &data, &target)
            .await
            .unwrap()
            .is_empty(),
        "put_batch clears the retained parts too"
    );
    // A part number that appears twice keeps its **last** row, which is what
    // the reference store's part table answers; the unnest insert would
    // otherwise abort the whole write on the relation's primary key.
    let doubled = vec![
        PartRow::from_raw(1, 5, "", ""),
        PartRow::from_raw(2, 6, "", ""),
        PartRow::from_raw(1, 9, "", ""),
    ];
    ObjectMeta::put_with_parts(&store, &data, &target, &row(17, ETAG_A), &doubled)
        .await
        .unwrap();
    assert_eq!(
        ObjectMeta::parts(&store, &data, &target)
            .await
            .unwrap()
            .iter()
            .map(|part| (part.number, part.size))
            .collect::<Vec<_>>(),
        vec![(1, 9), (2, 6)],
        "a duplicated part number keeps the last write"
    );
    db.drop_db().await;
}

#[tokio::test]
async fn move_carries_the_row_and_its_parts_and_is_a_noop_when_the_source_is_absent() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-object-move");
    bucket_row(&store, &data).await;
    let source = key("source.bin");
    let trailer = key("trailer.bin");
    let elsewhere = key("elsewhere.bin");
    let parts = vec![PartRow::from_raw(1, 5, "", "")];
    ObjectMeta::put_with_parts(&store, &data, &source, &row(11, ETAG_A), &parts)
        .await
        .unwrap();
    ObjectMeta::put(&store, &data, &trailer, &row(3, ETAG_B))
        .await
        .unwrap();
    ObjectMeta::move_(&store, &data, &source, &trailer)
        .await
        .unwrap();
    assert!(
        ObjectMeta::get(&store, &data, &source)
            .await
            .unwrap()
            .is_none(),
        "the source row is gone"
    );
    assert_eq!(
        ObjectMeta::get(&store, &data, &trailer)
            .await
            .unwrap()
            .map(|row| row.size),
        Some(11),
        "the destination carries the moved row"
    );
    assert_eq!(
        ObjectMeta::parts(&store, &data, &trailer)
            .await
            .unwrap()
            .len(),
        1,
        "and the moved part list"
    );
    // A same-key move and a move from an absent source are no-ops.
    ObjectMeta::move_(&store, &data, &trailer, &trailer)
        .await
        .unwrap();
    assert!(
        ObjectMeta::get(&store, &data, &trailer)
            .await
            .unwrap()
            .is_some()
    );
    ObjectMeta::move_(&store, &data, &source, &elsewhere)
        .await
        .unwrap();
    assert!(
        ObjectMeta::get(&store, &data, &elsewhere)
            .await
            .unwrap()
            .is_none()
    );
    db.drop_db().await;
}

#[tokio::test]
async fn the_guarded_writers_refuse_an_absent_bucket_and_put_preserving_carries_elements() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let absent = name("plane-object-absent");
    let target = key("a.txt");
    assert!(matches!(
        ObjectMeta::put_guarded(&store, &absent, &target, &row(1, ETAG_A)).await,
        Err(Error::NoSuchBucket(_))
    ));
    assert!(matches!(
        ObjectMeta::put_with_parts(&store, &absent, &target, &row(1, ETAG_A), &[]).await,
        Err(Error::NoSuchBucket(_))
    ));
    assert!(matches!(
        ObjectMeta::put_preserving(&store, &absent, &target, &identity(1, ETAG_A)).await,
        Err(Error::NoSuchBucket(_))
    ));
    // The batch writer is a guarded writer too: a bucket with no row refuses
    // the whole batch rather than planting object rows under it.
    assert!(matches!(
        ObjectMeta::put_batch(&store, &absent, &[(target.clone(), row(1, ETAG_A))]).await,
        Err(Error::NoSuchBucket(_))
    ));
    let data = name("plane-object-preserving");
    bucket_row(&store, &data).await;
    let mut recorded = row(5, ETAG_A);
    recorded.tags = tags(&[("keep", "me")]);
    recorded.owner = Some(owner());
    ObjectMeta::put(&store, &data, &target, &recorded)
        .await
        .unwrap();
    let preserved = ObjectMeta::put_preserving(&store, &data, &target, &identity(9, ETAG_B))
        .await
        .unwrap();
    assert_eq!(preserved.owner.as_ref(), Some(&owner()));
    assert_eq!(preserved.tags, tags(&[("keep", "me")]));
    let healed = ObjectMeta::get(&store, &data, &target)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (healed.size, healed.identity, healed.mtime),
        (9, 77, at(11))
    );
    assert_eq!(healed.tags, tags(&[("keep", "me")]));
    // A rewrite of a key with no row creates one.
    let fresh = key("fresh.txt");
    let preserved = ObjectMeta::put_preserving(&store, &data, &fresh, &identity(4, ETAG_A))
        .await
        .unwrap();
    assert!(preserved.tags.is_empty());
    assert!(
        ObjectMeta::get(&store, &data, &fresh)
            .await
            .unwrap()
            .is_some()
    );
    db.drop_db().await;
}

#[tokio::test]
async fn a_corrupt_row_reads_absent_fails_the_walk_and_heals() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-object-corrupt");
    bucket_row(&store, &data).await;
    let healthy = key("healthy.bin");
    let corrupt = key("corrupt.bin");
    ObjectMeta::put(&store, &data, &healthy, &row(5, ETAG_A))
        .await
        .unwrap();
    sqlx::query_file!("sql/meta/object/plant_corrupt_row.sql", &data, &corrupt)
        .execute(store.pool())
        .await
        .unwrap();
    // A retained part list with no row it belongs to: the heal must take it
    // with the row, or the next object written at the key inherits a part list
    // that is not its own.
    sqlx::query!(
        "INSERT INTO object_parts (bucket, key, number, size, algorithm, value) VALUES ($1, $2, 1, 1, '', '')",
        &data,
        &corrupt
    )
    .execute(store.pool())
    .await
    .unwrap();
    assert!(
        ObjectMeta::get(&store, &data, &corrupt)
            .await
            .unwrap()
            .is_none(),
        "a point read treats a corrupt row as absent"
    );
    assert!(matches!(
        ObjectMeta::walk(&store, &data).await,
        Err(Error::Corrupt { .. })
    ));
    let healed = ObjectMeta::walk_healing(&store, &data).await.unwrap();
    assert_eq!(healed.len(), 2, "the healing walk yields every key");
    let served = healed
        .iter()
        .find(|(k, _)| k.to_string() == "corrupt.bin")
        .expect("the corrupt key is reported");
    assert!(served.1.is_none(), "and its row is served as absent");
    let remaining = sqlx::query_scalar!(
        r#"SELECT count(*) AS "rows!" FROM object_meta WHERE bucket = $1"#,
        &data
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(remaining, 1, "the heal write removed the corrupt row");
    let parts = sqlx::query_scalar!(
        r#"SELECT count(*) AS "rows!" FROM object_parts WHERE bucket = $1"#,
        &data
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(parts, 0, "…and the part list that belonged to it");
    db.drop_db().await;
}

/// A stored `bigint` the contract cannot name refuses the read instead of
/// being served as the value an `as` cast would invent: part 0 for 2^32, a
/// length near `u64::MAX` for a negative size.
#[tokio::test]
async fn a_stored_value_outside_the_contract_refuses_the_read_it_cannot_serve() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-object-truncated");
    bucket_row(&store, &data).await;
    let target = key("stored.bin");
    let parts = vec![PartRow::from_raw(1, 5, "", "")];
    ObjectMeta::put_with_parts(&store, &data, &target, &row(5, ETAG_A), &parts)
        .await
        .unwrap();
    // 2^32 is a part number the contract's `u32` cannot name: the read refuses
    // rather than answering part 0, which no client ever wrote.
    sqlx::query!(
        "UPDATE object_parts SET number = 4294967296 WHERE bucket = $1 AND key = $2",
        &data,
        &target
    )
    .execute(store.pool())
    .await
    .unwrap();
    assert!(matches!(
        ObjectMeta::parts(&store, &data, &target).await,
        Err(Error::Corrupt { .. })
    ));
    sqlx::query!(
        "UPDATE object_parts SET number = 1, size = -1 WHERE bucket = $1 AND key = $2",
        &data,
        &target
    )
    .execute(store.pool())
    .await
    .unwrap();
    assert!(matches!(
        ObjectMeta::parts(&store, &data, &target).await,
        Err(Error::Corrupt { .. })
    ));
    // An object row whose stored size or identity is outside the contract
    // reads as absent, exactly as a domain-invalid ETag does, so a length is
    // never served from a value the contract cannot hold.
    sqlx::query!(
        "UPDATE object_meta SET size = -1 WHERE bucket = $1 AND key = $2",
        &data,
        &target
    )
    .execute(store.pool())
    .await
    .unwrap();
    assert!(
        ObjectMeta::get(&store, &data, &target)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        ObjectMeta::load(&store, &data, from_ref(&target))
            .await
            .unwrap()[0]
            .is_none()
    );
    db.drop_db().await;
}

/// The version slot is not the object plane's (the 2026-10-13 ruling):
/// `remove` takes the row and its retained parts and leaves the key's slot
/// recorded, because a slot with no object row is a state the contract admits
/// and only the version plane's own calls or the bucket teardown retire one.
#[tokio::test]
async fn a_row_removal_leaves_the_keys_version_slot_alone() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-object-slot");
    bucket_row(&store, &data).await;
    let target = key("versioned.bin");
    ObjectMeta::put(&store, &data, &target, &row(5, ETAG_A))
        .await
        .unwrap();
    let id = Id::new("01234567-89ab-cdef-0123-456789abcdef").expect("a valid version id");
    VersionMeta::advance_slot(&store, &data, &target, Some(id), 3)
        .await
        .unwrap();
    assert!(ObjectMeta::remove(&store, &data, &target).await.unwrap());
    assert!(
        ObjectMeta::get(&store, &data, &target)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        VersionMeta::current_slot(&store, &data, &target)
            .await
            .unwrap()
            .is_some(),
        "the slot outlives the row: only the version plane retires one"
    );
    db.drop_db().await;
}

/// A valid row a concurrent writer commits between the healing traversal's
/// read and its heal write survives: the heal reaches only the rows a locked
/// re-read still cannot decode.
#[tokio::test]
async fn a_valid_row_written_after_the_heal_read_survives() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-object-heal-race");
    bucket_row(&store, &data).await;
    let corrupt = key("heal-race.bin");
    sqlx::query_file!("sql/meta/object/plant_corrupt_row.sql", &data, &corrupt)
        .execute(store.pool())
        .await
        .unwrap();
    // The writer holds the row as a valid one, uncommitted: the traversal's
    // own read still sees the corrupt version, and the heal's locked re-read
    // waits on this transaction.
    let mut writer = begin(store.pool()).await.unwrap();
    sqlx::query!(
        "UPDATE object_meta SET etag = $3, size = 4 WHERE bucket = $1 AND key = $2",
        &data,
        &corrupt,
        ETAG_A
    )
    .execute(&mut *writer)
    .await
    .unwrap();
    let racing = store.clone();
    let bucket = data.clone();
    let task = tokio::spawn(async move { ObjectMeta::walk_healing(&racing, &bucket).await });
    assert!(
        wait_for_a_waiter(&store).await,
        "the heal never waited on the row's lock"
    );
    writer.commit().await.unwrap();
    let walked = task.await.expect("the heal task").unwrap();
    assert_eq!(walked.len(), 1, "the traversal reports the key it read");
    assert!(
        ObjectMeta::get(&store, &data, &corrupt)
            .await
            .unwrap()
            .is_some(),
        "the row the concurrent writer committed is not the heal's to delete"
    );
    db.drop_db().await;
}

/// A move whose destination a concurrent writer materializes while the move is
/// in flight must not fail it: the moved row stands, which is what the
/// reference store's last-writer-wins move answers.
#[tokio::test]
async fn a_move_onto_a_key_a_concurrent_writer_materializes_still_lands() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-object-move-race");
    bucket_row(&store, &data).await;
    let source = key("race-source.bin");
    let destination = key("race-destination.bin");
    ObjectMeta::put(&store, &data, &source, &row(11, ETAG_A))
        .await
        .unwrap();
    // A part list at the destination with no row of its own, held by this
    // transaction: the move's destination clear is a statement a lock can
    // park it on, so the test can put a concurrent write exactly between the
    // move's destination clear and its own row write — the window an
    // unconditional `UPDATE … SET key` answers with a unique violation.
    sqlx::query!(
        "INSERT INTO object_parts (bucket, key, number, size, algorithm, value) VALUES ($1, $2, 1, 1, '', '')",
        &data,
        &destination
    )
    .execute(store.pool())
    .await
    .unwrap();
    let mut parked = begin(store.pool()).await.unwrap();
    sqlx::query!(
        "DELETE FROM object_parts WHERE bucket = $1 AND key = $2",
        &data,
        &destination
    )
    .execute(&mut *parked)
    .await
    .unwrap();
    let racing = store.clone();
    let bucket = data.clone();
    let from = source.clone();
    let to = destination.clone();
    let task = tokio::spawn(async move { ObjectMeta::move_(&racing, &bucket, &from, &to).await });
    assert!(
        wait_for_a_waiter(&store).await,
        "the move never waited on the destination clear"
    );
    // The concurrent writer materializes the destination while the move waits.
    sqlx::query!(
        "INSERT INTO object_meta (bucket, key, etag, size, mtime, identity, tags, checksum, acl) VALUES ($1, $2, $3, 3, to_timestamp(10), 7, '{}', '', row(NULL, ARRAY[]::acl_grant[])::acl)",
        &data,
        &destination,
        ETAG_B
    )
    .execute(store.pool())
    .await
    .unwrap();
    parked.commit().await.unwrap();
    task.await
        .expect("the move task")
        .expect("a concurrent write to the destination is not a move failure");
    assert!(
        ObjectMeta::get(&store, &data, &source)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        ObjectMeta::get(&store, &data, &destination)
            .await
            .unwrap()
            .map(|row| (row.size, row.etag.to_string())),
        Some((11, ETAG_A.to_owned())),
        "the move's own row is the one that stands"
    );
    db.drop_db().await;
}

/// Wait until a call is blocked on a lock inside this test's own scratch
/// database — the signal that it has reached the statement the test parked it
/// on. `pg_locks` is cluster-wide and a row wait is recorded as a
/// `transactionid` lock (which carries no database), so the probe joins it to
/// the waiting session and keeps only the sessions in this database; a
/// neighbour's waiter must not release this test early.
async fn wait_for_a_waiter(store: &PgStore) -> bool {
    for _ in 0..100 {
        let waiting: i64 = sqlx::query_scalar!(
            r#"SELECT count(*) AS "waiting!" FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid WHERE NOT l.granted AND a.datname = current_database()"#,
        )
        .fetch_one(store.pool())
        .await
        .expect("the lock probe");
        if waiting > 0 {
            return true;
        }
        sleep(Duration::from_millis(20)).await;
    }
    false
}
