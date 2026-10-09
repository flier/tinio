use crate::{
    _core::{
        ETag,
        acl::Acl,
        checksum,
        meta::{UploadMeta, upload::Row as UploadRow},
        multipart::{self, PartNumber},
        object,
    },
    Error,
    tests::fixtures::{ETAG_A, bucket_row, key, name, scratch, tags},
    timestamp::{unix_micros, unix_seconds as at},
};

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
