use std::{io, iter, time::Duration};

use bytes::Bytes;
use futures::{StreamExt, stream};

use super::{PUBLISH_BATCH_CHUNKS, publish_plan};
use crate::{
    _core::{
        bucket,
        content::{ContentKey, ObjectContent, StagedObject, stage_body},
        object,
        storage::{BodyStream, ByteRange},
    },
    _sqlx::query,
    Error, PgStore,
    tests::fixtures::scratch,
};

const MIB: usize = 1024 * 1024;

/// Read a key through `open`, expecting bytes at every step.
async fn read(
    store: &PgStore,
    bucket: &bucket::Name,
    key: &ContentKey,
    range: Option<ByteRange>,
) -> Vec<u8> {
    let mut body = store
        .open(bucket, key, range)
        .await
        .expect("the key is live");
    let mut out = Vec::new();
    while let Some(chunk) = body.next().await {
        out.extend_from_slice(&chunk.expect("the body reads"));
    }
    out
}

/// Stage, write and publish one body.
async fn put(store: &PgStore, bucket: &bucket::Name, key: &ContentKey, bytes: &[u8]) {
    let mut staged = store.stage(bucket, key).await.expect("the stage opens");
    staged
        .write(Bytes::copy_from_slice(bytes))
        .await
        .expect("the bytes stage");
    store
        .publish(bucket, staged, key)
        .await
        .expect("the publish lands");
}

/// The walked object keys of one `scan`.
async fn walked(
    store: &PgStore,
    bucket: &bucket::Name,
    prefix: &str,
    after: Option<&object::Key>,
    limit: usize,
) -> Vec<object::Key> {
    store
        .scan(bucket, prefix, after, limit)
        .await
        .expect("the walk runs")
        .into_iter()
        .map(|entry| entry.key)
        .collect()
}

/// One deterministic chunk: byte `j` of chunk `i` is `(i + j) & 0xFF`, so a
/// dropped or repeated chunk changes the read.
fn pattern_chunk(index: usize, len: usize) -> Vec<u8> {
    (0..len)
        .map(|offset| (index as u8).wrapping_add(offset as u8))
        .collect()
}

/// A staged write is invisible until publish, and an abandoned stage is residue that
/// neither the walk nor `is_empty` counts.
#[tokio::test]
async fn a_staged_write_is_invisible_until_publish_and_residue_is_never_content() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let bucket = bucket::name("conf-content-visibility").expect("a valid bucket name");
    let key = object::key("live.bin").expect("a valid object key");
    let content = ContentKey::Object(key.clone());

    let mut staged = store
        .stage(&bucket, &content)
        .await
        .expect("the stage opens");
    staged
        .write(Bytes::from_static(b"hello"))
        .await
        .expect("the bytes stage");
    assert!(
        store.open(&bucket, &content, None).await.is_err(),
        "a staged write is invisible before publish"
    );
    assert!(
        store
            .stat(&bucket, &key)
            .await
            .expect("the probe runs")
            .is_none(),
        "a staged write has no entry"
    );
    assert!(
        store.is_empty(&bucket).await.expect("the probe runs"),
        "a staged write is not content"
    );

    store
        .publish(&bucket, staged, &content)
        .await
        .expect("the publish lands");
    assert_eq!(read(&store, &bucket, &content, None).await, b"hello");
    let entry = store
        .stat(&bucket, &key)
        .await
        .expect("the probe runs")
        .expect("the published entry");
    assert_eq!(entry.len, 5);
    assert_eq!(entry.identity, 0);
    assert_eq!(
        entry.mtime, None,
        "the content plane keeps no timestamp (D42)"
    );
    assert!(
        !store.is_empty(&bucket).await.expect("the probe runs"),
        "a live generation is content"
    );

    // An abandoned stage is residue: invisible, never listed, never content.
    let residue = object::key("residue.bin").expect("a valid object key");
    let residue_content = ContentKey::Object(residue.clone());
    let mut abandoned = store
        .stage(&bucket, &residue_content)
        .await
        .expect("the stage opens");
    abandoned
        .write(Bytes::from_static(b"residue"))
        .await
        .expect("the bytes stage");
    drop(abandoned);
    assert!(
        store.open(&bucket, &residue_content, None).await.is_err(),
        "residue is not readable"
    );
    assert!(
        store
            .stat(&bucket, &residue)
            .await
            .expect("the probe runs")
            .is_none(),
        "residue has no entry"
    );
    assert!(
        store
            .scan(&bucket, "", None, 100)
            .await
            .expect("the walk runs")
            .iter()
            .all(|entry| entry.key != residue),
        "residue is never listed (PG23)"
    );

    store
        .remove(&bucket, &content)
        .await
        .expect("the removal lands");
    assert!(
        store.is_empty(&bucket).await.expect("the probe runs"),
        "only residue remains, and residue is not content"
    );
    store
        .remove_bucket(&bucket)
        .await
        .expect("the bucket drains");
    db.drop_db().await;
}

/// The bounded-publish leg: a stream larger than 100 MiB lands in 1 MiB rows and 4 MiB
/// batches, and reads back byte for byte without the store holding it whole.
#[tokio::test]
async fn a_hundred_megabyte_stream_publishes_in_bounded_batches_and_round_trips() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    const CHUNK: usize = 1024 * 1024;
    const CHUNKS: usize = 100;
    const TAIL: usize = 7;
    const SIZE: usize = CHUNKS * CHUNK + TAIL;

    let bucket = bucket::name("conf-content-big").expect("a valid bucket name");
    let key = object::key("big/stream.bin").expect("a valid object key");
    let content = ContentKey::Object(key.clone());

    let chunks = (0..CHUNKS)
        .map(|index| Ok::<_, io::Error>(Bytes::from(pattern_chunk(index, CHUNK))))
        .chain(iter::once(Ok(Bytes::from(pattern_chunk(CHUNKS, TAIL)))));
    let body: BodyStream = Box::pin(stream::iter(chunks));

    let mut staged = store
        .stage(&bucket, &content)
        .await
        .expect("the stage opens");
    let landed = stage_body(&mut staged, body)
        .await
        .expect("the stream stages");
    assert_eq!(landed as usize, SIZE, "stage_body answers the landed bytes");
    store
        .publish(&bucket, staged, &content)
        .await
        .expect("the publish lands");

    let entry = store
        .stat(&bucket, &key)
        .await
        .expect("the probe runs")
        .expect("the published entry");
    assert_eq!(entry.len, SIZE as u64);

    let plan = publish_plan(SIZE as u64);
    assert_eq!(plan.chunks, CHUNKS + 1, "1 MiB rows, the tail included");
    assert_eq!(
        plan.batches,
        plan.chunks.div_ceil(PUBLISH_BATCH_CHUNKS),
        "the plan bounds each statement at PUBLISH_BATCH_BYTES"
    );

    let mut body = store
        .open(&bucket, &content, None)
        .await
        .expect("the live generation opens");
    let mut position = 0usize;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.expect("the body reads");
        for byte in chunk {
            let expected = ((position / CHUNK) as u8).wrapping_add((position % CHUNK) as u8);
            assert_eq!(byte, expected, "byte {position} of the round trip");
            position += 1;
        }
    }
    assert_eq!(position, SIZE, "the whole stream round-trips");

    store
        .remove(&bucket, &content)
        .await
        .expect("the removal lands");
    assert!(store.is_empty(&bucket).await.expect("the probe runs"));
    store
        .remove_bucket(&bucket)
        .await
        .expect("the bucket drains");
    db.drop_db().await;
}

/// A shrink-overwrite serves only the live generation, and a range that starts past the
/// live length is an error, never a clamp.
#[tokio::test]
async fn a_ranged_read_serves_only_the_live_generation() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let bucket = bucket::name("conf-content-range").expect("a valid bucket name");
    let key = object::key("shrink.bin").expect("a valid object key");
    let content = ContentKey::Object(key.clone());

    put(&store, &bucket, &content, &vec![b'a'; 3 * MIB]).await;
    put(&store, &bucket, &content, &vec![b'b'; MIB]).await;

    let whole = read(&store, &bucket, &content, None).await;
    assert_eq!(whole.len(), MIB, "only the live generation is served");
    assert!(
        whole.iter().all(|byte| *byte == b'b'),
        "no byte of the superseded 3 MiB generation survives"
    );
    assert_eq!(
        read(&store, &bucket, &content, Some(ByteRange::Suffix(3))).await,
        b"bbb"
    );
    assert_eq!(
        read(&store, &bucket, &content, Some(ByteRange::Inclusive(2, 5))).await,
        b"bbbb"
    );
    assert_eq!(
        read(
            &store,
            &bucket,
            &content,
            Some(ByteRange::Inclusive((MIB - 2) as u64, 10 * MIB as u64)),
        )
        .await,
        b"bb",
        "an end past the tail resolves against the live length"
    );

    let Err(err) = store
        .open(&bucket, &content, Some(ByteRange::From(MIB as u64)))
        .await
    else {
        panic!("a start at the live end is refused, never clamped");
    };
    assert!(matches!(err, Error::InvalidRange { .. }), "{err}");
    let Err(err) = store
        .open(
            &bucket,
            &content,
            Some(ByteRange::Inclusive(4 * MIB as u64, 5 * MIB as u64)),
        )
        .await
    else {
        panic!("a start past the live end is refused, never clamped");
    };
    assert!(matches!(err, Error::InvalidRange { .. }), "{err}");

    store
        .remove(&bucket, &content)
        .await
        .expect("the removal lands");
    store
        .remove_bucket(&bucket)
        .await
        .expect("the bucket drains");
    db.drop_db().await;
}

/// A live generation of zero bytes is content: it is listed, its stat entry
/// says zero, and it opens as an empty body. A range against it is refused
/// rather than clamped — the range is resolved before the empty short-circuit,
/// exactly as it is on the block plane.
#[tokio::test]
async fn a_zero_length_object_is_live_and_refuses_a_range() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let bucket = bucket::name("conf-content-empty").expect("a valid bucket name");
    let key = object::key("empty.bin").expect("a valid object key");
    let content = ContentKey::Object(key.clone());

    put(&store, &bucket, &content, b"").await;
    assert_eq!(
        read(&store, &bucket, &content, None).await,
        Vec::<u8>::new(),
        "a published zero-byte object reads as an empty body"
    );
    let entry = store
        .stat(&bucket, &key)
        .await
        .expect("the probe runs")
        .expect("the live entry");
    assert_eq!(entry.len, 0);
    assert!(
        !store.is_empty(&bucket).await.expect("the probe runs"),
        "a live generation of zero bytes is content"
    );
    assert_eq!(
        walked(&store, &bucket, "", None, 100).await,
        vec![key.clone()],
        "the empty object is enumerated"
    );

    let Err(err) = store
        .open(&bucket, &content, Some(ByteRange::Inclusive(0, 0)))
        .await
    else {
        panic!("a range against a zero-byte object is an error, not an empty body");
    };
    assert!(matches!(err, Error::InvalidRange { size: 0, .. }), "{err}");
    assert!(err.to_string().contains("on 0 bytes"), "{err}");

    store
        .remove(&bucket, &content)
        .await
        .expect("the removal lands");
    assert!(
        store
            .stat(&bucket, &key)
            .await
            .expect("the probe runs")
            .is_none(),
        "a removed object has no entry"
    );
    store
        .remove_bucket(&bucket)
        .await
        .expect("the bucket drains");
    db.drop_db().await;
}

/// A publish whose generation a later allocation superseded does not error: it
/// re-homes its generation onto the head's next free sequence and flips that,
/// so its bytes become live (last-writer-wins) and no 5xx escapes. The later
/// allocation then re-homes in turn and wins as the last writer.
#[tokio::test]
async fn a_superseded_publish_re_homes_and_flips_instead_of_erroring() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let bucket = bucket::name("conf-content-race").expect("a valid bucket name");
    let key = object::key("raced.bin").expect("a valid object key");
    let content = ContentKey::Object(key.clone());

    let mut first = store
        .stage(&bucket, &content)
        .await
        .expect("the first stage opens");
    first
        .write(Bytes::from_static(b"first"))
        .await
        .expect("the bytes stage");
    let mut second = store
        .stage(&bucket, &content)
        .await
        .expect("the second stage opens");
    second
        .write(Bytes::from_static(b"second"))
        .await
        .expect("the bytes stage");

    // `first` lost the allocation race (the head's `next_seq` moved on to
    // `second`): it re-homes and lands, so the concurrent write resolves to one
    // of the two bodies rather than a 500.
    store
        .publish(&bucket, first, &content)
        .await
        .expect("a superseded publish converges, it never errors");
    assert_eq!(read(&store, &bucket, &content, None).await, b"first");
    assert!(
        !store.is_empty(&bucket).await.expect("the probe runs"),
        "the re-homed generation is live content"
    );

    // The later allocation publishes next and wins, as the last writer.
    store
        .publish(&bucket, second, &content)
        .await
        .expect("the later allocation re-homes and flips the head");
    assert_eq!(read(&store, &bucket, &content, None).await, b"second");
    assert!(
        !store.is_empty(&bucket).await.expect("the probe runs"),
        "the last writer is live content"
    );

    store
        .remove(&bucket, &content)
        .await
        .expect("the removal lands");
    store
        .remove_bucket(&bucket)
        .await
        .expect("the bucket drains");
    db.drop_db().await;
}

/// A same-key race never tears the object: whichever generation is live, a full
/// read returns one whole body's bytes, never a mix of the two. The interleaving
/// is forced — two staged handles published in sequence — so the leg does not
/// depend on wall-clock luck.
#[tokio::test]
async fn a_same_key_race_never_tears_the_object() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let bucket = bucket::name("conf-content-no-tear").expect("a valid bucket name");
    let key = object::key("raced.bin").expect("a valid object key");
    let content = ContentKey::Object(key.clone());

    let a = vec![b'a'; 3 * MIB + 17];
    let mut b = vec![b'b'; 3 * MIB + 17];
    b[0] = b'A'; // distinct content, same length

    let mut first = store
        .stage(&bucket, &content)
        .await
        .expect("the first stage opens");
    first
        .write(Bytes::from(a.clone()))
        .await
        .expect("the bytes stage");
    let mut second = store
        .stage(&bucket, &content)
        .await
        .expect("the second stage opens");
    second
        .write(Bytes::from(b.clone()))
        .await
        .expect("the bytes stage");

    store
        .publish(&bucket, first, &content)
        .await
        .expect("the superseded publish converges");
    assert_eq!(
        read(&store, &bucket, &content, None).await,
        a,
        "the loser's body is whole, not a mixture"
    );
    store
        .publish(&bucket, second, &content)
        .await
        .expect("the later publish lands");
    assert_eq!(
        read(&store, &bucket, &content, None).await,
        b,
        "the winner's body is whole, not a mixture"
    );

    store
        .remove(&bucket, &content)
        .await
        .expect("the removal lands");
    store
        .remove_bucket(&bucket)
        .await
        .expect("the bucket drains");
    db.drop_db().await;
}

/// The generation a later allocation superseded must survive the whole window
/// between that allocation and its own publish — not merely the length of the
/// re-home transaction. Driven explicitly: `first` streams a batch that commits
/// (a body past the 4 MiB publish batch, so its chunk rows reach the table
/// before any publish), the `second` allocation then supersedes it, an orphan
/// sweep runs in that window, and only then does the loser publish. The per-row
/// write stamp must keep the fresh superseded rows out of the sweep, so nothing
/// is reclaimed and the re-home flips a body whole.
#[tokio::test]
async fn a_superseded_generation_survives_a_sweep_in_its_window() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let bucket = bucket::name("conf-content-race-sweep").expect("a valid bucket name");
    let key = object::key("raced.bin").expect("a valid object key");
    let content = ContentKey::Object(key.clone());

    // Five 1 MiB chunks: `write` flushes the first batch of four, so four rows
    // are committed under `first`'s sequence before the publish.
    let first_body = vec![b'a'; 5 * MIB];
    let mut first = store
        .stage(&bucket, &content)
        .await
        .expect("the first stage opens");
    first
        .write(Bytes::from(first_body.clone()))
        .await
        .expect("the bytes stage");
    let mut second = store
        .stage(&bucket, &content)
        .await
        .expect("the second stage opens");
    second
        .write(Bytes::from_static(b"second body"))
        .await
        .expect("the bytes stage");

    // The second allocation committed, so `first`'s already-committed rows are
    // superseded: neither live nor the head's `next_seq`. A sweep runs now, in
    // that window. The generation is only moments old, so its rows are inside
    // the grace and the per-row stamp keeps them.
    let orphans = store
        .orphan_chunks(Duration::from_secs(3600))
        .await
        .expect("the scan runs");
    let reclaimed = store
        .reclaim_orphan_chunks(&orphans)
        .await
        .expect("the sweep runs");
    assert_eq!(reclaimed, 0, "a fresh superseded generation is not residue");

    // `first` re-homes and flips a complete body. A reclaimed row would make the
    // move short (the row-count check rolls back) or the read answer `Corrupt`.
    store
        .publish(&bucket, first, &content)
        .await
        .expect("the superseded publish converges whole");
    assert_eq!(read(&store, &bucket, &content, None).await, first_body);

    store
        .publish(&bucket, second, &content)
        .await
        .expect("the later publish lands");
    assert_eq!(read(&store, &bucket, &content, None).await, b"second body");

    store
        .remove(&bucket, &content)
        .await
        .expect("the removal lands");
    store
        .remove_bucket(&bucket)
        .await
        .expect("the bucket drains");
    db.drop_db().await;
}

/// A staged generation whose allocation has aged stays whole while any of its
/// chunks was written inside the grace window. Aging every chunk then makes
/// the generation residue. A generation missing a chunk does not flip.
#[tokio::test]
async fn an_in_flight_generation_survives_while_a_chunk_is_fresh() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let bucket = bucket::name("conf-content-inflight").expect("a valid bucket name");
    let key = object::key("inflight.bin").expect("a valid object key");
    let content = ContentKey::Object(key);

    let body = vec![b'a'; 5 * MIB];
    let mut staged = store
        .stage(&bucket, &content)
        .await
        .expect("the stage opens");
    staged
        .write(Bytes::from(body))
        .await
        .expect("the bytes stage");

    let pool = store.pool();
    query(
        "UPDATE content_heads SET allocated_at = now() - interval '2 hours' \
         WHERE bucket = $1 AND ckey = $2",
    )
    .bind(&staged.bucket)
    .bind(&staged.ckey)
    .execute(&**pool)
    .await
    .expect("the allocation ages");
    query(
        "UPDATE content SET published = now() - interval '2 hours' \
         WHERE bucket = $1 AND ckey = $2 AND chunk_no <> 0",
    )
    .bind(&staged.bucket)
    .bind(&staged.ckey)
    .execute(&**pool)
    .await
    .expect("the earlier chunks age");

    let orphans = store
        .orphan_chunks(Duration::from_secs(600))
        .await
        .expect("the scan runs");
    let reclaimed = store
        .reclaim_orphan_chunks(&orphans)
        .await
        .expect("the sweep runs");
    assert_eq!(
        reclaimed, 0,
        "one fresh chunk protects the whole staged generation"
    );

    query(
        "UPDATE content SET published = now() - interval '2 hours' \
         WHERE bucket = $1 AND ckey = $2",
    )
    .bind(&staged.bucket)
    .bind(&staged.ckey)
    .execute(&**pool)
    .await
    .expect("the last chunk ages");
    let orphans = store
        .orphan_chunks(Duration::from_secs(600))
        .await
        .expect("the scan runs");
    let reclaimed = store
        .reclaim_orphan_chunks(&orphans)
        .await
        .expect("the sweep runs");
    assert!(
        reclaimed > 0,
        "a staged generation whose every chunk has aged is residue"
    );

    let mut again = store
        .stage(&bucket, &content)
        .await
        .expect("a second stage opens");
    again
        .write(Bytes::from(vec![b'b'; 5 * MIB]))
        .await
        .expect("the bytes stage");
    query("DELETE FROM content WHERE bucket = $1 AND ckey = $2 AND seq = $3 AND chunk_no = 0")
        .bind(&again.bucket)
        .bind(&again.ckey)
        .bind(again.seq)
        .execute(&**pool)
        .await
        .expect("one chunk leaves");
    let err = store
        .publish(&bucket, again, &content)
        .await
        .expect_err("a short generation does not flip");
    assert!(
        matches!(err, Error::PublishRaced),
        "the refusal is the publish race, got {err}"
    );

    store
        .remove_bucket(&bucket)
        .await
        .expect("the bucket drains");
    db.drop_db().await;
}

/// A key with no live generation is `MissingContent`, on both halves of the
/// content key space, and it is never served as an empty body.
#[tokio::test]
async fn a_key_with_no_live_generation_is_missing_content() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let bucket = bucket::name("conf-content-missing").expect("a valid bucket name");
    let key = object::key("absent.bin").expect("a valid object key");
    let content = ContentKey::Object(key.clone());

    let Err(err) = store.open(&bucket, &content, None).await else {
        panic!("a key with no bytes is an error, not an empty body");
    };
    assert!(
        matches!(&err, Error::MissingContent(ContentKey::Object(named)) if named == &key),
        "{err}"
    );
    assert!(err.to_string().contains("no content for"), "{err}");

    let part = ContentKey::Part {
        upload_id: "3b8a2f10-4d7c-4e21-9a6b-2f5c8e0d7b91".to_owned(),
        number: 1.into(),
    };
    let Err(err) = store.open(&bucket, &part, None).await else {
        panic!("a part key with no bytes is an error, not an empty body");
    };
    assert!(
        matches!(&err, Error::MissingContent(ContentKey::Part { number, .. }) if u32::from(*number) == 1),
        "{err}"
    );
    assert!(
        store
            .stat(&bucket, &key)
            .await
            .expect("the probe runs")
            .is_none(),
        "absence is `None` from stat and an error from open"
    );
    db.drop_db().await;
}

/// A live generation whose rows are not a contiguous run from zero is a corrupt
/// stored row, never a short body: both a gap at the head of the run and a run
/// that stops early are the same answer.
#[tokio::test]
async fn a_generation_that_is_not_a_contiguous_run_is_corrupt() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let bucket = bucket::name("conf-content-corrupt").expect("a valid bucket name");
    let body: Vec<u8> = (0..MIB + 10).map(|offset| (offset % 251) as u8).collect();
    let head = object::key("gap/head.bin").expect("a valid object key");
    let tail = object::key("gap/tail.bin").expect("a valid object key");
    put(&store, &bucket, &ContentKey::Object(head.clone()), &body).await;
    put(&store, &bucket, &ContentKey::Object(tail.clone()), &body).await;

    // One generation loses its first row, the other its last.
    for (key, chunk_no) in [(&head, 0), (&tail, 1)] {
        sqlx::query_file!(
            "sql/tests/plant_content_gap.sql",
            &bucket,
            format!("o:{key}"),
            1i64,
            chunk_no
        )
        .execute(store.as_driver())
        .await
        .expect("the plant");
    }

    for key in [&head, &tail] {
        let err = store
            .open(&bucket, &ContentKey::Object(key.clone()), None)
            .await
            .err()
            .expect("a generation that is not a contiguous run is refused");
        assert!(matches!(err, Error::Corrupt { .. }), "{err}");
        assert!(
            err.to_string().contains(&format!("{bucket}/o:{key}")),
            "the failure names the row it read: {err}"
        );
    }
    db.drop_db().await;
}

/// A live head whose encoded key is not an object key is a corrupt row, not a
/// key to serve: the walk reports it rather than yielding a key no caller could
/// have written.
#[tokio::test]
async fn a_head_whose_encoded_key_is_not_an_object_key_is_corrupt() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let bucket = bucket::name("conf-content-badkey").expect("a valid bucket name");
    // The empty key: it starts with the object prefix, so the walk selects it,
    // and nothing can decode back into a valid object key.
    sqlx::query_file!("sql/tests/plant_content_head_undecodable_key.sql", &bucket)
        .execute(store.as_driver())
        .await
        .expect("the plant");

    let err = store
        .scan(&bucket, "", None, 100)
        .await
        .expect_err("an undecodable stored key is reported, never yielded");
    assert!(
        matches!(&err, Error::Corrupt { key, .. } if key == "o:"),
        "{err}"
    );
    assert!(
        err.to_string().contains(&format!("{bucket}/o:")),
        "the failure names the row it read: {err}"
    );
    // It still holds the bucket: the emptiness rule reads the live sequence,
    // which this row has.
    assert!(!store.is_empty(&bucket).await.expect("the probe runs"));
    db.drop_db().await;
}

/// Rename and copy move the bytes and the head together; a self-move is the identity and
/// a batch removal is one atomic, idempotent call.
#[tokio::test]
async fn rename_and_copy_move_the_bytes_and_the_head() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let bucket = bucket::name("conf-content-move").expect("a valid bucket name");
    let a = object::key("a.bin").expect("a valid object key");
    let b = object::key("b.bin").expect("a valid object key");
    let c = object::key("c.bin").expect("a valid object key");
    let a_content = ContentKey::Object(a.clone());
    let b_content = ContentKey::Object(b.clone());
    let c_content = ContentKey::Object(c.clone());
    put(&store, &bucket, &a_content, b"hello").await;

    store
        .copy(&bucket, &a, &b)
        .await
        .expect("the duplicate lands");
    assert_eq!(read(&store, &bucket, &a_content, None).await, b"hello");
    assert_eq!(read(&store, &bucket, &b_content, None).await, b"hello");

    store
        .rename(&bucket, &b, &c)
        .await
        .expect("the rename lands");
    assert!(
        store.open(&bucket, &b_content, None).await.is_err(),
        "rename leaves no source bytes"
    );
    assert_eq!(read(&store, &bucket, &c_content, None).await, b"hello");

    store
        .rename(&bucket, &c, &c)
        .await
        .expect("a self-rename is a no-op");
    store
        .copy(&bucket, &c, &c)
        .await
        .expect("duplicating a key onto itself is a no-op");
    assert_eq!(
        read(&store, &bucket, &c_content, None).await,
        b"hello",
        "a self-move leaves the bytes where they are (R14)"
    );

    let missing = object::key("missing.bin").expect("a valid object key");
    assert!(
        store.rename(&bucket, &missing, &a).await.is_err(),
        "a rename with no source bytes fails"
    );
    assert!(
        store.copy(&bucket, &missing, &a).await.is_err(),
        "a duplicate with no source bytes fails"
    );

    store
        .remove_many(&bucket, &[])
        .await
        .expect("an empty batch removes nothing");
    assert!(
        store.open(&bucket, &a_content, None).await.is_ok(),
        "an empty batch removes nothing"
    );

    store
        .remove_many(&bucket, &[a_content.clone(), c_content.clone()])
        .await
        .expect("the batch lands");
    assert!(store.open(&bucket, &a_content, None).await.is_err());
    assert!(store.open(&bucket, &c_content, None).await.is_err());
    store
        .remove_many(&bucket, &[a_content, c_content])
        .await
        .expect("a repeat batch is idempotent");
    assert!(store.is_empty(&bucket).await.expect("the probe runs"));

    store
        .remove_bucket(&bucket)
        .await
        .expect("the bucket drains");
    db.drop_db().await;
}

/// The walk is byte-ordered, prefix-bounded, exclusive on `after`, capped, and over
/// live object keys only.
#[tokio::test]
async fn scan_is_byte_ordered_over_live_object_keys_only() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let bucket = bucket::name("conf-content-scan").expect("a valid bucket name");
    let names = ["alpha", "alpha-1", "alpine", "beta"];
    for name in names {
        let key = object::key(name).expect("a valid object key");
        put(&store, &bucket, &ContentKey::Object(key), b"x").await;
    }

    // A part inside the walked prefix proves the `kind = 'object'` exclusion is real.
    let part = ContentKey::Part {
        upload_id: "7a2e1c3d-8b4f-4a6e-b0c1-3e5d9f2a7c48".to_owned(),
        number: 1.into(),
    };
    put(&store, &bucket, &part, b"part").await;
    assert_eq!(read(&store, &bucket, &part, None).await, b"part");

    assert_eq!(
        walked(&store, &bucket, "", None, 100).await,
        vec![
            object::key("alpha").expect("a valid object key"),
            object::key("alpha-1").expect("a valid object key"),
            object::key("alpine").expect("a valid object key"),
            object::key("beta").expect("a valid object key"),
        ],
        "scan is ascending by the key's UTF-8 bytes"
    );
    assert_eq!(
        walked(&store, &bucket, "alpha", None, 100).await,
        vec![
            object::key("alpha").expect("a valid object key"),
            object::key("alpha-1").expect("a valid object key"),
        ],
        "prefix is a bound the walk must not scan past"
    );
    let after = object::key("alpha").expect("a valid object key");
    assert_eq!(
        walked(&store, &bucket, "alpha", Some(&after), 100).await,
        vec![object::key("alpha-1").expect("a valid object key")],
        "after is exclusive"
    );
    assert_eq!(
        store
            .scan(&bucket, "", None, 2)
            .await
            .expect("the walk runs")
            .len(),
        2,
        "limit caps the page"
    );
    assert!(
        store
            .scan(&bucket, "", None, 0)
            .await
            .expect("the walk runs")
            .is_empty(),
        "a zero limit is an empty page"
    );
    let below = object::key("a").expect("a valid object key");
    assert_eq!(
        walked(&store, &bucket, "alpha", Some(&below), 100).await,
        vec![
            object::key("alpha").expect("a valid object key"),
            object::key("alpha-1").expect("a valid object key"),
        ],
        "an after below the prefix cannot hide the prefix's page"
    );

    // The scanned entry and the point probe agree on the B1 inputs.
    let scanned = store
        .scan(&bucket, "alpha", None, 100)
        .await
        .expect("the walk runs");
    let stat = store
        .stat(&bucket, &object::key("alpha").expect("a valid object key"))
        .await
        .expect("the probe runs")
        .expect("the published entry");
    let entry = scanned
        .iter()
        .find(|entry| entry.key.as_ref() == "alpha")
        .expect("alpha is in the page");
    assert_eq!(entry.len, stat.len, "scan and stat agree on the length");
    assert_eq!(entry.identity, stat.identity);
    assert_eq!(entry.mtime, stat.mtime);

    for name in names {
        let key = object::key(name).expect("a valid object key");
        store
            .remove(&bucket, &ContentKey::Object(key))
            .await
            .expect("the removal lands");
    }
    store
        .remove(&bucket, &part)
        .await
        .expect("the removal lands");
    assert!(store.is_empty(&bucket).await.expect("the probe runs"));
    store
        .remove_bucket(&bucket)
        .await
        .expect("the bucket drains");
    db.drop_db().await;
}
