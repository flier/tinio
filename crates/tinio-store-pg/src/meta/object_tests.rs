use crate::{
    _core::{
        ETag,
        acl::Acl,
        meta::{
            BucketMeta, ObjectMeta,
            object::{Identity, PartRow, Row as ObjectRow},
        },
        object,
    },
    Error,
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
    db.drop_db().await;
}
