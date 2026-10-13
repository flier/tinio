use std::{io, sync::Mutex};

use futures::{future, stream};

use super::*;
use crate::{
    _core::multipart::part_number,
    _store_redb::{Error as RowStoreError, part as part_rows},
    BucketRow,
    testutil::{Base, Call, ContentCall, body},
};

/// A bucket with a recorded row — the guarded policy's requirement.
async fn bucket(base: &Base, name: &str) -> bucket::Name {
    let name = bucket::name(name).unwrap();
    base.ops(BucketRow::Guaranteed)
        .create_bucket(&name, None, &Acl::default_private(None))
        .await
        .unwrap();
    name
}

/// The timeline's events of one part write, as labels: the window the part
/// lock's span is a claim about.
fn window(calls: &[Call]) -> Vec<&'static str> {
    calls
        .iter()
        .map(|call| match call {
            Call::Content(ContentCall::Stage(_)) => "stage",
            Call::Content(ContentCall::Publish(..)) => "publish",
            Call::Content(ContentCall::Remove(_)) => "remove",
            Call::Body => "body",
            Call::LockPart { .. } => "lock",
            Call::LockReleased { .. } => "release",
            Call::PartRow { .. } => "row",
            other => panic!("unexpected event on the timeline: {other:?}"),
        })
        .collect()
}

#[tokio::test]
async fn the_upload_lifecycle_records_and_serves_the_upload_row() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("big.bin").unwrap();
    let parts = base.multipart(BucketRow::Guaranteed);

    let created = parts
        .create_multipart_upload(
            &name,
            &key,
            None,
            object::Tags::from_pairs([("team".to_owned(), "core".to_owned())]).unwrap(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    assert!(
        uuid::Uuid::parse_str(&created.upload_id).is_ok(),
        "the create allocates a v4 UUID: {}",
        created.upload_id
    );
    let served = parts
        .get_multipart_upload(&name, &key, &created.upload_id)
        .await
        .unwrap();
    assert_eq!(served.upload_id, created.upload_id);
    assert_eq!(served.tags, created.tags, "the row echoes the create");

    // The identity is `(bucket, key, upload_id)`: another key is not an
    // upload at all.
    let other = object::key("other.bin").unwrap();
    let err = parts
        .get_multipart_upload(&name, &other, &created.upload_id)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NoSuchUpload(_)), "{err:?}");

    // The abort drains the records in one checked transaction: the upload
    // is gone afterwards, and a retry is the contract's NoSuchUpload (the
    // S3 surface's own idempotence sits above).
    parts
        .abort_multipart_upload(&name, &key, &created.upload_id)
        .await
        .unwrap();
    let err = parts
        .abort_multipart_upload(&name, &key, &created.upload_id)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NoSuchUpload(_)), "{err:?}");
}

#[tokio::test]
async fn a_guarded_store_refuses_the_create_of_a_bucket_it_does_not_hold() {
    // The policy is the receiver's: a row-backed store records the upload
    // guarded, so a bucket whose row is missing refuses inside the
    // contract's transaction (the TOCTOU-free create).
    let base = Base::new();
    let name = bucket::name("ghost").unwrap();
    let key = object::key("a.bin").unwrap();
    let err = base
        .multipart(BucketRow::Guaranteed)
        .create_multipart_upload(
            &name,
            &key,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NoSuchBucket(_)), "{err:?}");
}

#[tokio::test]
async fn the_part_pipeline_publishes_the_bytes_then_writes_the_row() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("big.bin").unwrap();
    let (parts, content) = base.recording_multipart(BucketRow::Guaranteed);
    let upload = parts
        .create_multipart_upload(
            &name,
            &key,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();

    let part = parts
        .upload_part(
            &name,
            &key,
            &upload.upload_id,
            part_number(1).unwrap(),
            body(&[b"hel", b"lo"]),
            None,
        )
        .await
        .unwrap();
    assert_eq!(part.size, 5, "the counted bytes are the served size");
    assert_eq!(part.etag, ETag::from_content(b"hello"));
    let part_key = ContentKey::Part {
        upload_id: upload.upload_id.clone(),
        number: part.part_number,
    };
    assert!(
        content.saw(|call| matches!(call, ContentCall::Stage(key) if key == &part_key)),
        "the part bytes cross the content seam keyed as a part: {:?}",
        content.calls()
    );
    assert!(
        content.saw(|call| matches!(call, ContentCall::Publish(key, _) if key == &part_key)),
        "and they are published there"
    );

    // The row the page serves: part-number order, the recorded ETag.
    let page = parts
        .list_parts(&name, &key, &upload.upload_id, None, 1000)
        .await
        .unwrap();
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].number, 1);
    assert_eq!(page.rows[0].etag, part.etag);
    assert!(!page.truncated);
    assert_eq!(page.next_part_number_marker, None);
}

/// The test's own body stream: two chunks, each logging `Call::Body` as it
/// is yielded — the only way to observe *when* the engine read the body, so
/// a guard taken before the stream fails the window below.
fn logging_body(timeline: Arc<Mutex<Vec<Call>>>) -> BodyStream {
    let chunks = [b"fi".as_slice(), b"ve!"].into_iter().map(move |chunk| {
        timeline.lock().expect("the call log").push(Call::Body);
        Ok(bytes::Bytes::copy_from_slice(chunk))
    });
    Box::pin(stream::iter(chunks))
}

#[tokio::test]
async fn the_part_lock_spans_the_body_the_publish_and_the_row() {
    // The lock's window (obligation 13, E15): the guard is taken once the
    // body has been **read** — not when the stage opened — and released
    // once the part row is written. A slow body therefore never holds it,
    // and two part writes of one upload cannot interleave their publish
    // with their row.
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("big.bin").unwrap();
    let (parts, _content) = base.recording_multipart(BucketRow::Guaranteed);
    let upload = parts
        .create_multipart_upload(
            &name,
            &key,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();

    parts
        .upload_part(
            &name,
            &key,
            &upload.upload_id,
            part_number(1).unwrap(),
            logging_body(base.hooks().timeline()),
            None,
        )
        .await
        .unwrap();

    let calls = base.hooks().calls();
    assert_eq!(
        window(&calls),
        ["stage", "body", "body", "lock", "publish", "row", "release"],
        "the guard is taken after the body was read, held across the publish and the row, released with the row: {calls:?}"
    );
    // The lock names the upload it guards.
    assert!(
        calls.iter().any(|call| matches!(
            call,
            Call::LockPart { upload_id } if upload_id == &upload.upload_id
        )),
        "{calls:?}"
    );
}

#[tokio::test]
async fn a_refused_part_row_write_takes_this_calls_publish_back() {
    // The row write is the operation's answer once the bytes are
    // published, and a part row is the only thing that names a part's
    // bytes: the engine takes its own publish back and answers the
    // refusal, leaving no bytes nothing can reach.
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("big.bin").unwrap();
    let (parts, content) = base.recording_multipart(BucketRow::Guaranteed);
    let upload = parts
        .create_multipart_upload(
            &name,
            &key,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    // The row plane refuses the part row (the upload's own key would be
    // re-checked inside the contract's transaction; this double's refusal
    // is the same refusal one step earlier).
    base.counted().refuse_writes_for(&name, &key);

    let err = parts
        .upload_part(
            &name,
            &key,
            &upload.upload_id,
            part_number(1).unwrap(),
            body(&[b"late"]),
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Io(_)), "{err:?}");
    let part_key = ContentKey::Part {
        upload_id: upload.upload_id.clone(),
        number: part_number(1).unwrap(),
    };
    assert!(
        content.saw(|call| matches!(call, ContentCall::Remove(key) if key == &part_key)),
        "the refused write took its own publish back: {:?}",
        content.calls()
    );
    assert_eq!(
        window(&base.hooks().calls()),
        ["stage", "lock", "publish", "remove", "release"],
        "the take-back happens with the guard still held"
    );
    assert!(
        base.content.open(&name, &part_key, None).await.is_err(),
        "no bytes survive the refusal"
    );
}

#[tokio::test]
async fn a_part_write_for_an_upload_that_is_not_there_reads_nothing() {
    // The identity check runs before the body: an UploadPart for an
    // aborted or completed upload must not read the client's stream, and
    // leaves no part file behind.
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("big.bin").unwrap();
    let (parts, content) = base.recording_multipart(BucketRow::Guaranteed);

    let err = parts
        .upload_part(
            &name,
            &key,
            "0f8fad5b-d9cb-469f-a165-70867728950e",
            part_number(1).unwrap(),
            body(&[b"never"]),
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NoSuchUpload(_)), "{err:?}");
    assert!(
        content.calls().is_empty(),
        "the stage never opened: {:?}",
        content.calls()
    );
}

#[tokio::test]
async fn a_body_failure_on_a_live_upload_answers_the_streams_error() {
    // The body streams before the row transaction: a failing stream is
    // the client's own error, and nothing is committed for it.
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("big.bin").unwrap();
    let parts = base.multipart(BucketRow::Guaranteed);
    let upload = parts
        .create_multipart_upload(
            &name,
            &key,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    let failing = Box::pin(stream::once(future::ready(Err::<bytes::Bytes, io::Error>(
        io::Error::other("boom"),
    ))));
    let err = parts
        .upload_part(
            &name,
            &key,
            &upload.upload_id,
            part_number(1).unwrap(),
            failing,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Io(_)), "{err:?}");
    assert!(
        parts
            .list_parts(&name, &key, &upload.upload_id, None, 1000)
            .await
            .unwrap()
            .rows
            .is_empty(),
        "a failed stream commits no row"
    );
}

#[tokio::test]
async fn a_body_failure_that_raced_an_abort_answers_no_such_upload() {
    // The abort lands while the body streams (the upload is served once
    // more, then it is gone), so the failure the stream reports is not the
    // client's — the upload's absence is.
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("big.bin").unwrap();
    let parts = base.multipart(BucketRow::Guaranteed);
    let upload = parts
        .create_multipart_upload(
            &name,
            &key,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    base.counted()
        .vanish_upload_after_get(&name, &upload.upload_id);
    let boom: Result<bytes::Bytes, io::Error> = Err(io::Error::other("boom"));
    let failing = Box::pin(stream::once(future::ready(boom)));
    let err = parts
        .upload_part(
            &name,
            &key,
            &upload.upload_id,
            part_number(1).unwrap(),
            failing,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NoSuchUpload(_)), "{err:?}");
}

#[tokio::test]
async fn the_page_resumes_by_part_number_and_stops_at_the_marker() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("big.bin").unwrap();
    let parts = base.multipart(BucketRow::Guaranteed);
    let upload = parts
        .create_multipart_upload(
            &name,
            &key,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    for n in [3u32, 1, 2] {
        parts
            .upload_part(
                &name,
                &key,
                &upload.upload_id,
                part_number(n).unwrap(),
                body(&[b"x"]),
                None,
            )
            .await
            .unwrap();
    }

    let first = parts
        .list_parts(&name, &key, &upload.upload_id, None, 2)
        .await
        .unwrap();
    let numbers: Vec<u32> = first.rows.iter().map(|row| row.number).collect();
    assert_eq!(numbers, [1, 2], "the page is in part-number order");
    assert!(first.truncated);
    assert_eq!(first.next_part_number_marker, Some(2));
    let second = parts
        .list_parts(
            &name,
            &key,
            &upload.upload_id,
            first.next_part_number_marker,
            2,
        )
        .await
        .unwrap();
    let numbers: Vec<u32> = second.rows.iter().map(|row| row.number).collect();
    assert_eq!(numbers, [3], "the marker is exclusive");
    assert!(!second.truncated);
    assert_eq!(second.next_part_number_marker, None);

    // `max_parts = 0` asks for nothing — and names no marker, or a client
    // resuming on it would skip the first part of the next page forever.
    let empty = parts
        .list_parts(&name, &key, &upload.upload_id, None, 0)
        .await
        .unwrap();
    assert!(empty.rows.is_empty());
    assert!(!empty.truncated);
    assert_eq!(empty.next_part_number_marker, None);

    // A marker of `u32::MAX` has no successor: the page is empty rather
    // than re-serving the marker part.
    let beyond = parts
        .list_parts(&name, &key, &upload.upload_id, Some(u32::MAX), 10)
        .await
        .unwrap();
    assert!(beyond.rows.is_empty());
    assert!(!beyond.truncated);
}

#[tokio::test]
async fn a_truncated_page_whose_rows_all_self_healed_still_names_a_marker() {
    // The one corner where the two marker rules differ: the contract's
    // page joins the raw rows and self-heals a domain-invalid stored ETag
    // away, so a page can serve nothing while the raw read served rows.
    // The marker must still advance (`Store::list_parts`'s rule, and the
    // contract's `raw_last`), or the client re-requests the same page
    // forever.
    //
    // Only out-of-band tampering reaches this page — `put_part` takes a
    // validated `&ETag` — so the raw row is written through the table seam
    // its own implementation exposes.
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("big.bin").unwrap();
    let parts = base.multipart(BucketRow::Guaranteed);
    let upload = parts
        .create_multipart_upload(
            &name,
            &key,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    for n in [1u32, 2] {
        parts
            .upload_part(
                &name,
                &key,
                &upload.upload_id,
                part_number(n).unwrap(),
                body(&[b"x"]),
                None,
            )
            .await
            .unwrap();
    }
    let (bucket, upload_id) = (name.clone(), upload.upload_id.clone());
    base.meta
        .driver()
        .write(move |txn| -> Result<(), RowStoreError> {
            let mut rows = part_rows::Table::open(txn)?;
            rows.insert((&*bucket, upload_id.as_str(), 1u32), "not-an-etag")?;
            Ok(())
        })
        .await
        .unwrap();

    // A one-row page whose row self-heals away: served nothing, truncated,
    // and the marker is the raw read's own last number.
    let page = parts
        .list_parts(&name, &key, &upload.upload_id, None, 1)
        .await
        .unwrap();
    assert!(
        page.rows.is_empty(),
        "the tampered row self-heals away: {:?}",
        page.rows
    );
    assert!(page.truncated, "the raw read saw the second row");
    assert_eq!(
        page.next_part_number_marker,
        Some(1),
        "the marker must name the raw page's last number, not nothing"
    );
    // Resuming on it advances: the tampered row is re-read, dropped, and
    // the page serves the row after it.
    let next = parts
        .list_parts(
            &name,
            &key,
            &upload.upload_id,
            page.next_part_number_marker,
            10,
        )
        .await
        .unwrap();
    let numbers: Vec<u32> = next.rows.iter().map(|row| row.number).collect();
    assert_eq!(numbers, [2], "the client advances instead of looping");
    assert!(!next.truncated);
}

#[tokio::test]
async fn the_listing_pages_and_resumes_inside_a_same_key_group() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let parts = base.multipart(BucketRow::Guaranteed);
    for key in ["dir/a.bin", "dir/b.bin", "z.bin"] {
        parts
            .create_multipart_upload(
                &name,
                &object::key(key).unwrap(),
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
    }
    let params = |key_marker: Option<String>, upload_id_marker: Option<String>, max: usize| {
        ListUploadsParams {
            bucket: name.clone(),
            prefix: String::new(),
            delimiter: Some("/".into()),
            key_marker,
            upload_id_marker,
            max_uploads: max,
        }
    };
    let page = parts
        .list_multipart_uploads(params(None, None, 1))
        .await
        .unwrap();
    assert!(page.uploads.is_empty(), "{:?}", page.uploads);
    assert_eq!(page.common_prefixes, ["dir/"]);
    assert!(page.truncated, "the rollup truncates the page");
    assert_eq!(page.next_key_marker.as_deref(), Some("dir/"));

    let page = parts
        .list_multipart_uploads(params(page.next_key_marker, page.next_upload_id_marker, 10))
        .await
        .unwrap();
    let keys: Vec<&str> = page
        .uploads
        .iter()
        .map(|upload| upload.key.as_ref().as_str())
        .collect();
    assert_eq!(keys, ["z.bin"], "the resume marker skips the rollup");
    assert!(!page.truncated);
}

#[tokio::test]
async fn a_lazy_store_records_the_upload_row_plainly() {
    // A bucket that was never sighted: the lazy policy's create records
    // the upload row without requiring the bucket's.
    let base = Base::new();
    let name = bucket::name("handmade").unwrap();
    base.content.create_bucket(&name).await.unwrap();
    let key = object::key("a.bin").unwrap();
    let parts = base.multipart(BucketRow::Lazy);
    let upload = parts
        .create_multipart_upload(
            &name,
            &key,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    assert!(
        parts
            .get_multipart_upload(&name, &key, &upload.upload_id)
            .await
            .is_ok()
    );
}
