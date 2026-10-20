//! The per-upload part lock (`Hooks::lock_part`): the engine's part write takes
//! it, and two concurrent writes of one part number cannot leave a part row
//! that disagrees with the generation it names.
//!
//! Both legs run only with `TINIO_PG_DSN` set; offline they skip.

#![expect(
    clippy::absolute_paths,
    reason = "test support: a test proves the public paths"
)]

use tinio_util::testing::bucket_name;
mod support;

use std::{sync::Arc, time::Duration};

use support::{require_dsn, scratch_config};
use tinio_core::{
    acl::Acl,
    bucket,
    content::ContentKey,
    meta::UploadMeta,
    multipart, object,
    storage::{BucketOps, MultipartOps},
};
use tinio_pg::{Access, PgStorage};
use tinio_util::testing::body;

/// The part number both writers of a round write, and the content key the lock
/// and the invariant are keyed by.
fn part(upload_id: &str) -> Result<ContentKey, String> {
    Ok(ContentKey::Part {
        upload_id: upload_id.to_owned(),
        number: multipart::part_number(1).map_err(|err| err.to_string())?,
    })
}

/// Create the bucket and one multipart upload, and answer the upload's id.
///
/// The two fallible steps are answered as `String`s so the helper carries no
/// `expect` of its own: this file is an integration target, where clippy's
/// tests exemption covers the `#[tokio::test]` bodies but not the helpers
/// beside them.
async fn upload(storage: &PgStorage, bucket: &bucket::Name) -> Result<String, String> {
    let acl = Acl::default_private(None);
    storage
        .create_bucket(bucket, None, &acl)
        .await
        .map_err(|err| err.to_string())?;
    let key = object::key("locked.bin").map_err(|err| err.to_string())?;
    Ok(storage
        .create_multipart_upload(bucket, &key, None, object::Tags::empty(), None, &acl)
        .await
        .map_err(|err| err.to_string())?
        .upload_id)
}

/// **The engine waits on the lock.** With the lock held from outside, a part
/// write through the engine cannot finish — it is blocked on the same
/// `(bucket, upload_id)` advisory lock — and it completes once the holder
/// releases. With `NoGuard` this leg fails at the first assertion: the write
/// finishes while the lock is held.
#[tokio::test]
async fn the_engine_waits_for_the_per_upload_part_lock() {
    if !require_dsn() {
        return;
    }
    let db = tinio_pg::_store_pg::test_support::TestDb::create()
        .await
        .expect("a scratch database");
    let storage = PgStorage::connect(&scratch_config(&db), Access::ReadWrite)
        .await
        .expect("a writable store over the scratch database");
    let bucket = bucket_name("part-lock-waits");
    let upload_id = upload(&storage, &bucket).await.expect("the upload");
    let key = object::key("locked.bin").expect("a valid key");

    // The lock, held exactly as a concurrent writer's guard would hold it.
    let held = storage
        .store()
        .lock_part(&bucket, &upload_id)
        .await
        .expect("the per-upload lock");

    let writer = tokio::spawn({
        let storage = storage.clone();
        let bucket = bucket.clone();
        let upload_id = upload_id.clone();
        async move {
            storage
                .upload_part(
                    &bucket,
                    &key,
                    &upload_id,
                    multipart::part_number(1).unwrap(),
                    body("held"),
                    None,
                )
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !writer.is_finished(),
        "a part write must wait on the upload's advisory lock, not race it"
    );

    drop(held);
    let info = writer
        .await
        .expect("the writer task")
        .expect("the part write completes once the lock is free");
    assert_eq!(info.size, 4, "the part's bytes are the writer's own");

    db.drop_db().await;
}

/// **The row and the bytes are one writer's.** Every round runs two concurrent
/// writes of the *same* part number with bodies of **different lengths**, then
/// compares the part row the completion would read against the generation the
/// reader would serve. The lock makes the publish and the row write one
/// writer's pair; without it the two orders interleave and the sizes disagree.
#[tokio::test]
async fn concurrent_same_part_writes_keep_the_row_agreed_with_its_generation() {
    if !require_dsn() {
        return;
    }
    let db = tinio_pg::_store_pg::test_support::TestDb::create()
        .await
        .expect("a scratch database");
    let first = Arc::new(
        PgStorage::connect(&scratch_config(&db), Access::ReadWrite)
            .await
            .expect("a writable store over the scratch database"),
    );
    let second = Arc::clone(&first);
    let bucket = bucket_name("part-lock-agrees");
    let upload_id = upload(&first, &bucket).await.expect("the upload");
    let key = object::key("locked.bin").expect("a valid key");

    for round in 0..6 {
        let short = tokio::spawn({
            let storage = Arc::clone(&first);
            let bucket = bucket.clone();
            let upload_id = upload_id.clone();
            let key = key.clone();
            async move {
                storage
                    .upload_part(
                        &bucket,
                        &key,
                        &upload_id,
                        multipart::part_number(1).unwrap(),
                        body("A"),
                        None,
                    )
                    .await
            }
        });
        let long = tokio::spawn({
            let storage = Arc::clone(&second);
            let bucket = bucket.clone();
            let upload_id = upload_id.clone();
            let key = key.clone();
            async move {
                storage
                    .upload_part(
                        &bucket,
                        &key,
                        &upload_id,
                        multipart::part_number(1).unwrap(),
                        body("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                        None,
                    )
                    .await
            }
        });
        let (short, long) = tokio::join!(short, long);
        let short = short.expect("the task").expect("the short write");
        let long = long.expect("the task").expect("the long write");
        assert_ne!(short.size, long.size, "the two bodies differ in length");

        // The row the completion reads (its recorded ETag) and the generation a
        // reader would serve (the live span's length), read separately so a
        // mixture of the two writers is visible.
        let rows = UploadMeta::list_parts(
            first.store(),
            &bucket,
            &upload_id,
            multipart::part_number(1).unwrap(),
        )
        .await
        .expect("the part rows");
        let row = rows.first().expect("a part row after both writers");
        let spans = first
            .store()
            .live_lens(&bucket, &[part(&upload_id).expect("a valid part number")])
            .await
            .expect("the live generation");
        let span = spans
            .first()
            .and_then(Option::as_ref)
            .expect("a live part generation");

        let served = if span.len == short.size {
            &short
        } else {
            &long
        };
        assert_eq!(
            span.len, served.size,
            "round {round}: the live generation is one writer's whole body"
        );
        assert_eq!(
            row.etag, served.etag,
            "round {round}: the part row's ETag names the bytes its generation serves"
        );
    }

    db.drop_db().await;
}
