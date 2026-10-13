use std::{
    collections::BTreeSet,
    time::{Duration, SystemTime},
};

use tokio::time::sleep;

use crate::{
    _core::{
        ETag, MultipartUpload,
        acl::Acl,
        bucket, checksum,
        meta::{UploadMeta, upload::Row as UploadRow},
        multipart::{self, PartNumber},
        object,
        storage::{group_and_paginate_unordered, uploads_order},
    },
    Error, PgStore,
    tests::fixtures::{ETAG_A, bucket_row, key, name, scratch, tags},
    timestamp::{unix_micros, unix_seconds as at},
};

const ETAG_B: &str = "0cc175b9c0f1b6a831c399e269772661";

/// The host clock in unix milliseconds, for the leg that bounds a stored
/// write stamp.
fn now_millis() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("a clock after the epoch")
            .as_millis(),
    )
    .expect("a millisecond stamp the store can hold")
}

/// The stored write stamp of one part row, in unix milliseconds.
async fn stamp_of(store: &PgStore, bucket: &bucket::Name, upload_id: &str, number: i64) -> i64 {
    sqlx::query_scalar!(
        r#"SELECT modified_at AS "stamp!" FROM parts WHERE bucket = $1 AND upload_id = $2 AND number = $3"#,
        bucket,
        upload_id,
        number
    )
    .fetch_one(store.pool())
    .await
    .expect("the part row the test just wrote")
}

fn part(number: u32) -> PartNumber {
    multipart::part_number(number).expect("a valid part number")
}

fn state(upload_id: &str, target: &object::Key) -> UploadRow {
    UploadRow {
        upload_id: upload_id.to_owned(),
        key: target.clone(),
        initiated_at: at(10),
        checksum: None,
        tags: tags(&[("from", "upload")]),
        owner: None,
        acl: Acl::default_private(None),
    }
}

#[tokio::test]
async fn the_initiation_time_round_trips_with_sub_second_precision() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-upload-micros");
    bucket_row(&store, &data).await;
    let target = key("micros.bin");
    let upload_id = "plane-upload-micros";
    let initiated_at = unix_micros(1_700_000_000_111_222);
    let mut upload = state(upload_id, &target);
    upload.initiated_at = initiated_at;
    UploadMeta::create(&store, &data, upload_id, &upload)
        .await
        .unwrap();
    let got = UploadMeta::get(&store, &data, &target, upload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        got.initiated_at, initiated_at,
        "timestamptz keeps the sub-second digits"
    );
    db.drop_db().await;
}

#[tokio::test]
async fn upload_rows_round_trip_and_the_key_check_binds() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-upload-life");
    bucket_row(&store, &data).await;
    let target = key("big.bin");
    let upload_id = "plane-upload-1";
    let absent = key("other.bin");
    assert!(
        UploadMeta::get(&store, &data, &target, upload_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(!UploadMeta::has_uploads(&store, &data).await.unwrap());
    UploadMeta::create(&store, &data, upload_id, &state(upload_id, &target))
        .await
        .unwrap();
    UploadMeta::create(&store, &data, upload_id, &state(upload_id, &target))
        .await
        .unwrap();
    let got = UploadMeta::get(&store, &data, &target, upload_id)
        .await
        .unwrap()
        .expect("the upload just created");
    assert_eq!(got.initiated_at, at(10));
    assert_eq!(got.tags, tags(&[("from", "upload")]));
    assert!(
        UploadMeta::get(&store, &data, &absent, upload_id)
            .await
            .unwrap()
            .is_none(),
        "an upload recorded under another key is not this key's"
    );
    assert!(UploadMeta::has_uploads(&store, &data).await.unwrap());
    for number in [3u32, 1, 2] {
        UploadMeta::put_part(
            &store,
            &data,
            &target,
            upload_id,
            part(number),
            &ETag::new(ETAG_A).unwrap(),
            None,
        )
        .await
        .unwrap();
    }
    assert!(matches!(
        UploadMeta::put_part(
            &store,
            &data,
            &absent,
            upload_id,
            part(4),
            &ETag::new(ETAG_A).unwrap(),
            None,
        )
        .await,
        Err(Error::NoSuchUpload(_))
    ));
    let listed = UploadMeta::list_parts(&store, &data, upload_id, part(2))
        .await
        .unwrap();
    assert_eq!(
        listed.iter().map(|part| part.number).collect::<Vec<_>>(),
        vec![2, 3],
        "the parts order numerically from the bound up"
    );
    let records = UploadMeta::part_records(&store, &data, &target, upload_id, &[part(3), part(9)])
        .await
        .unwrap()
        .expect("the key matches");
    assert!(records[0].is_some());
    assert!(records[1].is_none());
    // One answer per occurrence: a number asked for twice answers its record
    // twice, not `None` on its second read.
    let repeated = UploadMeta::part_records(
        &store,
        &data,
        &target,
        upload_id,
        &[part(3), part(3), part(9)],
    )
    .await
    .unwrap()
    .expect("the key matches");
    assert!(
        repeated[0].is_some() && repeated[1].is_some() && repeated[2].is_none(),
        "a duplicated part number answers its record per occurrence"
    );
    assert!(
        UploadMeta::part_records(&store, &data, &absent, upload_id, &[part(1)])
            .await
            .unwrap()
            .is_none(),
        "the join refuses a key the upload does not record"
    );
    assert_eq!(
        UploadMeta::list_uploads(&store, &data).await.unwrap().len(),
        1
    );
    assert_eq!(UploadMeta::walk_uploads(&store).await.unwrap().len(), 1);
    UploadMeta::drain_upload(&store, &data, upload_id)
        .await
        .unwrap();
    assert!(!UploadMeta::has_uploads(&store, &data).await.unwrap());
    assert!(
        UploadMeta::list_parts(&store, &data, upload_id, part(1))
            .await
            .unwrap()
            .is_empty()
    );
    db.drop_db().await;
}

#[tokio::test]
async fn every_algorithm_round_trips_through_a_fresh_database() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-upload-algos");
    bucket_row(&store, &data).await;
    for (index, algorithm) in checksum::Algorithm::ALL.into_iter().enumerate() {
        let target = key(&format!("algos/{index}.bin"));
        let upload_id = format!("plane-upload-algo-{index}");
        let mut upload = state(&upload_id, &target);
        upload.checksum = Some(checksum::Upload {
            algorithm,
            r#type: None,
        });
        UploadMeta::create(&store, &data, &upload_id, &upload)
            .await
            .unwrap();
        let got = UploadMeta::get(&store, &data, &target, &upload_id)
            .await
            .unwrap()
            .expect("the upload just created");
        assert_eq!(
            got.checksum,
            Some(checksum::Upload {
                algorithm,
                r#type: None
            }),
            "the upload spec's algorithm round-trips through upload_checksums"
        );
        let tee = checksum::PartChecksum::default();
        tee.digest
            .set(checksum::Part {
                algorithm,
                value: checksum::Value("AA==".to_owned()),
            })
            .expect("a fresh tee");
        UploadMeta::put_part(
            &store,
            &data,
            &target,
            &upload_id,
            part(1),
            &ETag::new(ETAG_A).unwrap(),
            Some(&tee),
        )
        .await
        .unwrap();
        let listed = UploadMeta::list_parts(&store, &data, &upload_id, part(1))
            .await
            .unwrap();
        let stored = listed.into_iter().next().expect("the part just written");
        assert_eq!(
            stored.checksum,
            Some(checksum::Part {
                algorithm,
                value: checksum::Value("AA==".to_owned())
            }),
            "the part checksum's algorithm round-trips through part_checksums"
        );
    }
    db.drop_db().await;
}

#[tokio::test]
async fn list_parts_page_is_bounded_and_reports_its_raw_last() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-upload-page");
    bucket_row(&store, &data).await;
    let target = key("paged.bin");
    let upload_id = "plane-upload-page";
    UploadMeta::create(&store, &data, upload_id, &state(upload_id, &target))
        .await
        .unwrap();
    for number in 1..=5u32 {
        UploadMeta::put_part(
            &store,
            &data,
            &target,
            upload_id,
            part(number),
            &ETag::new(ETAG_A).unwrap(),
            None,
        )
        .await
        .unwrap();
    }
    let page = UploadMeta::list_parts_page(&store, &data, upload_id, 0, 2)
        .await
        .unwrap();
    assert_eq!(
        page.rows.iter().map(|part| part.number).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(page.more);
    assert_eq!(page.raw_last, Some(2));
    // max == 0 is empty and never reads as truncated.
    let empty = UploadMeta::list_parts_page(&store, &data, upload_id, 0, 0)
        .await
        .unwrap();
    assert!(empty.rows.is_empty() && !empty.more && empty.raw_last.is_none());
    let tail = UploadMeta::list_parts_page(&store, &data, upload_id, 4, 10)
        .await
        .unwrap();
    assert_eq!(
        tail.rows.iter().map(|part| part.number).collect::<Vec<_>>(),
        vec![4, 5]
    );
    assert!(!tail.more);
    assert_eq!(tail.raw_last, Some(5));
    db.drop_db().await;
}

#[tokio::test]
async fn drain_upload_checked_refuses_a_mismatched_key_and_drains_the_match() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-upload-checked");
    bucket_row(&store, &data).await;
    let target = key("checked.bin");
    let upload_id = "plane-upload-checked";
    UploadMeta::create(&store, &data, upload_id, &state(upload_id, &target))
        .await
        .unwrap();
    UploadMeta::put_part(
        &store,
        &data,
        &target,
        upload_id,
        part(1),
        &ETag::new(ETAG_A).unwrap(),
        None,
    )
    .await
    .unwrap();
    assert!(
        matches!(
            UploadMeta::drain_upload_checked(&store, &data, &key("other.bin"), upload_id).await,
            Err(Error::NoSuchUpload(_))
        ),
        "a mismatched key refuses and removes nothing"
    );
    assert!(UploadMeta::has_uploads(&store, &data).await.unwrap());
    UploadMeta::drain_upload_checked(&store, &data, &target, upload_id)
        .await
        .unwrap();
    assert!(!UploadMeta::has_uploads(&store, &data).await.unwrap());
    assert!(
        UploadMeta::list_parts(&store, &data, upload_id, part(1))
            .await
            .unwrap()
            .is_empty(),
        "the drain took the parts with the row"
    );
    assert!(matches!(
        UploadMeta::drain_upload_checked(&store, &data, &target, upload_id).await,
        Err(Error::NoSuchUpload(_))
    ));
    db.drop_db().await;
}

#[tokio::test]
async fn create_guarded_refuses_an_absent_bucket_and_drain_bucket_clears_parts() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let absent = name("plane-upload-absent");
    let target = key("guarded.bin");
    let upload_id = "plane-upload-guarded";
    assert!(matches!(
        UploadMeta::create_guarded(&store, &absent, upload_id, &state(upload_id, &target)).await,
        Err(Error::NoSuchBucket(_))
    ));
    assert!(
        UploadMeta::get(&store, &absent, &target, upload_id)
            .await
            .unwrap()
            .is_none(),
        "the refused guarded create wrote no upload row"
    );
    let data = name("plane-upload-guarded");
    bucket_row(&store, &data).await;
    UploadMeta::create_guarded(&store, &data, upload_id, &state(upload_id, &target))
        .await
        .unwrap();
    UploadMeta::put_part(
        &store,
        &data,
        &target,
        upload_id,
        part(1),
        &ETag::new(ETAG_A).unwrap(),
        None,
    )
    .await
    .unwrap();
    UploadMeta::drain_bucket(&store, &data).await.unwrap();
    UploadMeta::drain_bucket(&store, &data).await.unwrap();
    assert!(!UploadMeta::has_uploads(&store, &data).await.unwrap());
    assert!(
        UploadMeta::list_parts(&store, &data, upload_id, part(1))
            .await
            .unwrap()
            .is_empty()
    );
    db.drop_db().await;
}

#[tokio::test]
async fn the_uploads_page_filters_rolls_up_and_resumes_after_its_marker() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-upload-listing");
    bucket_row(&store, &data).await;
    for (upload_id, target) in [("u1", "dir/a"), ("u2", "dir/b"), ("u3", "root")] {
        let target = key(target);
        UploadMeta::create(&store, &data, upload_id, &state(upload_id, &target))
            .await
            .unwrap();
    }
    // max == 0 is empty and never reads as truncated.
    let (uploads, prefixes, more, marker) =
        UploadMeta::list_uploads_page(&store, &data, "", None, None, 0)
            .await
            .unwrap();
    assert!(uploads.is_empty() && prefixes.is_empty() && !more && marker.is_none());
    // A delimiter rolls the shared group up and keeps the plain key.
    let (uploads, prefixes, more, marker) =
        UploadMeta::list_uploads_page(&store, &data, "", Some("/"), None, 10)
            .await
            .unwrap();
    assert_eq!(prefixes, vec!["dir/".to_owned()]);
    assert_eq!(
        uploads
            .iter()
            .map(|upload| upload.key.to_string())
            .collect::<Vec<_>>(),
        vec!["root"]
    );
    assert!(!more && marker.is_none());
    // A truncated page ends on the rolled-up prefix and resumes past it.
    let (uploads, prefixes, more, marker) =
        UploadMeta::list_uploads_page(&store, &data, "", Some("/"), None, 1)
            .await
            .unwrap();
    assert!(uploads.is_empty());
    assert_eq!(prefixes, vec!["dir/".to_owned()]);
    assert!(more);
    assert_eq!(marker.as_deref(), Some("dir/"));
    let (uploads, _, more, marker) =
        UploadMeta::list_uploads_page(&store, &data, "", Some("/"), marker.as_deref(), 10)
            .await
            .unwrap();
    assert_eq!(
        uploads
            .iter()
            .map(|upload| upload.key.to_string())
            .collect::<Vec<_>>(),
        vec!["root"]
    );
    assert!(!more && marker.is_none());
    // The prefix filter keeps the delimiter out of the same-key group and
    // the composite marker resumes inside it.
    let (uploads, _, more, marker) =
        UploadMeta::list_uploads_page(&store, &data, "dir/", None, None, 1)
            .await
            .unwrap();
    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0].key.to_string(), "dir/a");
    assert_eq!(uploads[0].upload_id, "u1");
    assert!(more);
    assert_eq!(marker.as_deref(), Some("dir/a\u{0}u1"));
    let (uploads, _, more, marker) =
        UploadMeta::list_uploads_page(&store, &data, "dir/", None, marker.as_deref(), 10)
            .await
            .unwrap();
    assert_eq!(
        uploads
            .iter()
            .map(|upload| upload.key.to_string())
            .collect::<Vec<_>>(),
        vec!["dir/b"]
    );
    assert!(!more && marker.is_none());
    db.drop_db().await;
}

/// The corpus the bounded-listing legs share: a delimiter group with several
/// members, a nested group, a key of its own, and the keys that pin the prefix
/// bound's arithmetic — one carrying `U+10FFFF` immediately after the prefix,
/// one whose body *is* that code point (a prefix with no successor), and one a
/// bound of `prefix || chr(1114111)` would silently drop.
const LISTING_KEYS: [&str; 7] = [
    "dir/a.txt",
    "dir/b.txt",
    "dir/sub/c.txt",
    "hot/x",
    "hot/\u{10FFFF}",
    "hot/\u{10FFFF}x",
    "zebra.txt",
];

/// Seed the shared corpus into `data` — two uploads per key, three for the
/// first, so a chunk boundary falls inside a same-key group and the cursor's
/// resume *within* a key is exercised — and answer the bucket's whole upload
/// set (the listing's materialized read).
async fn seed_listing_corpus(store: &PgStore, data: &bucket::Name) -> Vec<MultipartUpload> {
    for (index, raw) in LISTING_KEYS.iter().enumerate() {
        let target = key(raw);
        let scoops = if index == 0 { 3 } else { 2 };
        for scoop in 0..scoops {
            let upload_id = format!("upload-{index}-{scoop}");
            UploadMeta::create(store, data, &upload_id, &state(&upload_id, &target))
                .await
                .unwrap();
        }
    }
    UploadMeta::list_uploads(store, data).await.unwrap()
}

/// The bounded listing read answers exactly what the shared engine answers over
/// the materialized set — the same page, roll-up, truncation flag and resume
/// marker for the same `(prefix, delimiter, marker, max_uploads)`, over a matrix
/// that covers the delimiter roll-up, the resume inside a same-key group, a
/// marker inside a rolled-up prefix and the prefix bound's last-code-point
/// arithmetic. The oracle is the engine fed the prefix's whole upload set, which
/// is the shape this read had before it was bounded (F17).
#[tokio::test]
async fn the_bounded_page_answers_what_the_materialized_engine_answers() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-upload-bounded");
    bucket_row(&store, &data).await;
    let all = seed_listing_corpus(&store, &data).await;
    assert_eq!(all.len(), LISTING_KEYS.len() * 2 + 1, "the corpus landed");
    // The markers: a row's own composite order, a bare `key-marker`'s
    // `U+10FFFF` sentinel, a rolled-up prefix (no row at all) and one past
    // every row in the bucket.
    let markers = [
        None,
        Some("dir/a.txt\u{0}upload-0-1"),
        Some("dir/a.txt\u{10FFFF}"),
        Some("dir/"),
        Some("zzz\u{0}upload-9-9"),
    ];
    let mut combos = 0usize;
    for prefix in [
        "",
        "dir/",
        "dir/sub/",
        "hot/",
        "hot/\u{10FFFF}",
        "hot\u{10FFFF}",
        "z",
        "q",
    ] {
        for delimiter in [None, Some("/")] {
            for marker in markers {
                for max in [0usize, 1, 3, 1000] {
                    combos += 1;
                    let materialized: Vec<MultipartUpload> = all
                        .iter()
                        .filter(|upload| upload.key.as_ref().starts_with(prefix))
                        .cloned()
                        .collect();
                    let expected = group_and_paginate_unordered(
                        materialized,
                        prefix,
                        delimiter,
                        marker,
                        max,
                        |upload: &MultipartUpload| upload.key.as_ref(),
                        |upload: &MultipartUpload| uploads_order(&upload.key, &upload.upload_id),
                    );
                    let got = UploadMeta::list_uploads_page(
                        &store, &data, prefix, delimiter, marker, max,
                    )
                    .await
                    .unwrap();
                    assert_eq!(
                        got, expected,
                        "prefix={prefix:?} delimiter={delimiter:?} marker={marker:?} max={max}"
                    );
                }
            }
        }
    }
    assert_eq!(combos, 320, "the matrix ran");
    db.drop_db().await;
}

/// The prefix bound is exact at the largest code point: a key carrying
/// `U+10FFFF` immediately after the prefix is listed, and so is a key whose
/// body *is* that code point (a prefix with no successor, which the read serves
/// without an upper bound rather than inventing one that drops it).
#[tokio::test]
async fn the_prefix_bound_keeps_a_key_carrying_the_last_code_point() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-upload-bound-last");
    bucket_row(&store, &data).await;
    seed_listing_corpus(&store, &data).await;
    for (prefix, expected) in [
        ("hot/", vec!["hot/x", "hot/\u{10FFFF}", "hot/\u{10FFFF}x"]),
        ("hot/\u{10FFFF}", vec!["hot/\u{10FFFF}", "hot/\u{10FFFF}x"]),
        ("hot\u{10FFFF}", vec![]),
    ] {
        let (uploads, prefixes, more, marker) =
            UploadMeta::list_uploads_page(&store, &data, prefix, None, None, 1000)
                .await
                .unwrap();
        let keys: Vec<String> = uploads
            .iter()
            .map(|upload| upload.key.to_string())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        assert_eq!(
            keys, expected,
            "prefix={prefix:?}: a bound of `prefix || chr(1114111)` drops the key beyond it"
        );
        assert!(prefixes.is_empty() && !more && marker.is_none());
    }
    db.drop_db().await;
}

/// A stored checksum label outside the API's ten algorithms heals: the part or
/// the upload is served **without** its checksum instead of failing the read —
/// the contract's own rule for that column, and the difference between a
/// listing that survives a bad row and `Cleanup`'s sweep failing outright.
#[tokio::test]
async fn a_corrupt_checksum_label_heals_every_read() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-upload-heal");
    bucket_row(&store, &data).await;
    let target = key("heal.bin");
    let upload_id = "plane-upload-heal";
    let mut upload = state(upload_id, &target);
    upload.checksum = Some(checksum::Upload {
        algorithm: checksum::Algorithm::Crc32,
        r#type: None,
    });
    UploadMeta::create(&store, &data, upload_id, &upload)
        .await
        .unwrap();
    let tee = checksum::PartChecksum::default();
    tee.digest
        .set(checksum::Part {
            algorithm: checksum::Algorithm::Crc32,
            value: checksum::Value("AA==".to_owned()),
        })
        .expect("a fresh tee");
    UploadMeta::put_part(
        &store,
        &data,
        &target,
        upload_id,
        part(1),
        &ETag::new(ETAG_A).unwrap(),
        Some(&tee),
    )
    .await
    .unwrap();
    // Both checksum rows now name an algorithm no read can decode.
    sqlx::query!(
        "UPDATE part_checksums SET algorithm = 'BLAKE3' WHERE bucket = $1 AND upload_id = $2",
        &data,
        upload_id
    )
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query!(
        "UPDATE upload_checksums SET algorithm = 'BLAKE3' WHERE bucket = $1 AND upload_id = $2",
        &data,
        upload_id
    )
    .execute(store.pool())
    .await
    .unwrap();
    let listed = UploadMeta::list_parts(&store, &data, upload_id, part(1))
        .await
        .unwrap();
    assert_eq!(listed.len(), 1, "the part is still listed");
    assert!(
        listed[0].checksum.is_none(),
        "…served without the label no read can decode"
    );
    let page = UploadMeta::list_parts_page(&store, &data, upload_id, 0, 10)
        .await
        .unwrap();
    assert_eq!(page.rows.len(), 1);
    assert!(page.rows[0].checksum.is_none());
    let records = UploadMeta::part_records(&store, &data, &target, upload_id, &[part(1)])
        .await
        .unwrap()
        .expect("the key matches");
    assert!(records[0].as_ref().is_some_and(|r| r.checksum.is_none()));
    let got = UploadMeta::get(&store, &data, &target, upload_id)
        .await
        .unwrap()
        .expect("the upload is still there");
    assert!(
        got.checksum.is_none(),
        "the upload is served without the spec no read can decode"
    );
    assert_eq!(
        UploadMeta::list_uploads(&store, &data).await.unwrap().len(),
        1
    );
    let (uploads, _, _, _) = UploadMeta::list_uploads_page(&store, &data, "", None, None, 10)
        .await
        .unwrap();
    assert_eq!(uploads.len(), 1);
    assert_eq!(
        UploadMeta::walk_uploads(&store).await.unwrap().len(),
        1,
        "the sweep's own walk serves the upload rather than failing on the row"
    );
    db.drop_db().await;
}

/// A stored part number the contract's `u32` cannot name is not served: it
/// reads as absent, exactly as a domain-invalid ETag does, instead of being
/// answered as the part its `as` truncation would invent (2^32 would answer
/// part 0).
#[tokio::test]
async fn a_stored_part_number_outside_the_contract_reads_as_absent() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-upload-number");
    bucket_row(&store, &data).await;
    let target = key("stored.bin");
    let upload_id = "plane-upload-number";
    UploadMeta::create(&store, &data, upload_id, &state(upload_id, &target))
        .await
        .unwrap();
    UploadMeta::put_part(
        &store,
        &data,
        &target,
        upload_id,
        part(1),
        &ETag::new(ETAG_A).unwrap(),
        None,
    )
    .await
    .unwrap();
    sqlx::query!(
        "UPDATE parts SET number = 4294967296 WHERE bucket = $1 AND upload_id = $2",
        &data,
        upload_id
    )
    .execute(store.pool())
    .await
    .unwrap();
    assert!(
        UploadMeta::list_parts(&store, &data, upload_id, part(1))
            .await
            .unwrap()
            .is_empty(),
        "the row the contract cannot name is not served as another part"
    );
    let records = UploadMeta::part_records(&store, &data, &target, upload_id, &[part(1)])
        .await
        .unwrap()
        .expect("the key matches");
    assert!(records[0].is_none());
    db.drop_db().await;
}

/// Every part write stamps the part's **own** modification time: the per-part
/// write stamp the listing serves as its `LastModified`, which is not the
/// upload's creation instant — a part uploaded later must report the later
/// instant.
#[tokio::test]
async fn a_part_write_stamps_the_parts_own_modification_time() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-upload-stamp");
    bucket_row(&store, &data).await;
    let target = key("stamped.bin");
    let upload_id = "plane-upload-stamp";
    UploadMeta::create(&store, &data, upload_id, &state(upload_id, &target))
        .await
        .unwrap();
    let before = now_millis();
    UploadMeta::put_part(
        &store,
        &data,
        &target,
        upload_id,
        part(1),
        &ETag::new(ETAG_A).unwrap(),
        None,
    )
    .await
    .unwrap();
    let first = stamp_of(&store, &data, upload_id, 1).await;
    let after = now_millis();
    assert!(
        first >= before - 1_000 && first <= after + 1_000,
        "the stamp is the write's own unix-millisecond instant ({first}), not the upload's own initiation time"
    );
    // A rewrite moves it: the row's stamp is the last write's, so a part
    // uploaded later does not inherit the earlier instant.
    sleep(Duration::from_millis(50)).await;
    UploadMeta::put_part(
        &store,
        &data,
        &target,
        upload_id,
        part(1),
        &ETag::new(ETAG_B).unwrap(),
        None,
    )
    .await
    .unwrap();
    let rewritten = stamp_of(&store, &data, upload_id, 1).await;
    assert!(
        rewritten > first,
        "the rewrite moves the part's own stamp ({first} -> {rewritten})"
    );
    db.drop_db().await;
}

/// **The ruling**: every listed part reports **its own** write stamp, never the
/// upload's creation time. Two parts of one upload, written seconds apart, are
/// projected the way the backend projects them — `to_info(size, upload's
/// creation)` — and must answer two different instants, both later than the
/// upload's own.
///
/// This is the contract-level half of the ruling: the projection reads the
/// part row's stamp, so a row that carries none still answers the fallback
/// ([`a_part_write_stamps_the_parts_own_modification_time`]'s fallback arm).
#[tokio::test]
async fn two_parts_of_one_upload_report_their_own_write_stamps() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("plane-upload-two-stamps");
    bucket_row(&store, &data).await;
    let target = key("two-parts.bin");
    let upload_id = "plane-upload-two-stamps";
    UploadMeta::create(&store, &data, upload_id, &state(upload_id, &target))
        .await
        .unwrap();
    UploadMeta::put_part(
        &store,
        &data,
        &target,
        upload_id,
        part(1),
        &ETag::new(ETAG_A).unwrap(),
        None,
    )
    .await
    .unwrap();
    sleep(Duration::from_millis(1_100)).await;
    UploadMeta::put_part(
        &store,
        &data,
        &target,
        upload_id,
        part(2),
        &ETag::new(ETAG_B).unwrap(),
        None,
    )
    .await
    .unwrap();
    // The upload's own creation instant — what this round replaces as the
    // listing's `LastModified`.
    let created = UploadMeta::get(&store, &data, &target, upload_id)
        .await
        .unwrap()
        .expect("the upload the test just created")
        .initiated_at;
    let rows = UploadMeta::list_parts(&store, &data, upload_id, part(1))
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "both parts are listed");
    let first = rows[0].to_info(0, created).unwrap().last_modified;
    let second = rows[1].to_info(0, created).unwrap().last_modified;
    assert!(
        first > created,
        "part 1 answers its own write stamp ({first:?}), not the upload's creation time ({created:?})"
    );
    assert!(
        second > first,
        "the second part's stamp is strictly later than the first's ({first:?} -> {second:?})"
    );
    db.drop_db().await;
}
