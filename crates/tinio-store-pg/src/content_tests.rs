use std::{io, iter, time::Duration};

use bytes::Bytes;
use futures::{StreamExt, stream};
#[cfg(test)]
use tinio_util::testing::{bucket_name, object_key, part_number};

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
    cleanup::OrphanChunk,
    tests::fixtures::{bucket_row, scratch},
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

/// A bucket name whose row exists. [`ObjectContent::stage`] locks that row.
async fn named(store: &PgStore, raw: &str) -> bucket::Name {
    let name = bucket_name(raw);
    bucket_row(store, &name).await;
    name
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
    let bucket = named(&store, "conf-content-visibility").await;
    let key = object_key("live.bin");
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
    let residue = object_key("residue.bin");
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

    let bucket = named(&store, "conf-content-big").await;
    let key = object_key("big/stream.bin");
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
    assert_eq!(
        plan.chunks,
        CHUNKS as u64 + 1,
        "1 MiB rows, the tail included"
    );
    assert_eq!(
        plan.batches,
        plan.chunks.div_ceil(PUBLISH_BATCH_CHUNKS as u64),
        "the plan bounds each statement at PUBLISH_BATCH_BYTES"
    );

    // The read of that whole body is cut into bounded windows, in order, each
    // one statement: the arithmetic a read's resident bytes follow (R4).
    let windows = super::chunk_windows(
        0,
        (SIZE / super::CONTENT_CHUNK_BYTES) as i32,
        super::READ_WINDOW_CHUNKS as i32,
    );
    assert_eq!(
        windows.len(),
        (CHUNKS + 1).div_ceil(super::READ_WINDOW_CHUNKS),
        "one window per READ_WINDOW_CHUNKS chunks"
    );
    assert!(
        windows
            .iter()
            .all(|(first, last)| last - first < super::READ_WINDOW_CHUNKS as i32),
        "no window is wider than the bound, whatever the body's size"
    );
    assert_eq!(windows.first().copied(), Some((0, 7)));

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
    let bucket = named(&store, "conf-content-range").await;
    let key = object_key("shrink.bin");
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
    let bucket = named(&store, "conf-content-empty").await;
    let key = object_key("empty.bin");
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
    let bucket = named(&store, "conf-content-race").await;
    let key = object_key("raced.bin");
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
    let bucket = named(&store, "conf-content-no-tear").await;
    let key = object_key("raced.bin");
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
    let bucket = named(&store, "conf-content-race-sweep").await;
    let key = object_key("raced.bin");
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
        .reclaim_orphan_chunks(&orphans, Duration::from_secs(3600))
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
    let bucket = named(&store, "conf-content-inflight").await;
    let key = object_key("inflight.bin");
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
        .reclaim_orphan_chunks(&orphans, Duration::from_secs(600))
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
        .reclaim_orphan_chunks(&orphans, Duration::from_secs(600))
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
    let bucket = named(&store, "conf-content-missing").await;
    let key = object_key("absent.bin");
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
        number: part_number(1),
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

/// A live generation whose rows are not a contiguous run from zero is refused: a
/// gap inside the read's **first** window — the one `open` resolves with the
/// head — is the crate's `Corrupt`, because nothing has been served from it yet;
/// a gap in a later window is `UnexpectedEof` on the body stream, after the
/// earlier windows may already have been yielded. A run that stops early and one
/// that loses its first chunk are the same answer.
#[tokio::test]
async fn a_generation_that_is_not_a_contiguous_run_is_corrupt() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let bucket = named(&store, "conf-content-corrupt").await;
    let body: Vec<u8> = (0..MIB + 10).map(|offset| (offset % 251) as u8).collect();
    let head = object_key("gap/head.bin");
    let tail = object_key("gap/tail.bin");
    let late = object_key("gap/late.bin");
    put(&store, &bucket, &ContentKey::Object(head.clone()), &body).await;
    put(&store, &bucket, &ContentKey::Object(tail.clone()), &body).await;
    let long = vec![b'x'; (super::READ_WINDOW_CHUNKS + 2) * MIB];
    put(&store, &bucket, &ContentKey::Object(late.clone()), &long).await;

    // One generation loses its first row, one its last, one a row past the
    // first window.
    for (key, chunk_no) in [(&head, 0), (&tail, 1), (&late, super::READ_WINDOW_CHUNKS)] {
        sqlx::query_file!(
            "sql/tests/plant_content_gap.sql",
            &bucket,
            format!("o:{key}"),
            1i64,
            chunk_no as i32
        )
        .execute(store.as_driver())
        .await
        .expect("the plant");
    }

    for key in [&head, &tail] {
        let Err(err) = store
            .open(&bucket, &ContentKey::Object(key.clone()), None)
            .await
        else {
            panic!("a gap in the first window is refused before the first byte");
        };
        assert!(
            matches!(&err, Error::Corrupt { key: row, .. } if row == &format!("o:{key}")),
            "{err}"
        );
        assert!(
            err.to_string().contains(&format!("{bucket}/o:{key}")),
            "the failure names the row it read: {err}"
        );
    }

    let mut body = store
        .open(&bucket, &ContentKey::Object(late.clone()), None)
        .await
        .expect("the head and its first window are whole");
    let mut refused = None;
    while let Some(chunk) = body.next().await {
        if let Err(err) = chunk {
            refused = Some(err);
            break;
        }
    }
    let err = refused.expect("a gap past the first window is an error on the body stream");
    assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    assert!(
        err.to_string().contains(&format!("{bucket}/o:{late}")),
        "the failure names the row it read: {err}"
    );
    db.drop_db().await;
}

/// A read is served one bounded window at a time, in order, whatever the
/// object's size: the whole body round-trips and each window is fetched as its
/// own statement. A ranged read crossing window boundaries serves exactly the
/// bytes asked for, and a one-byte read is one byte.
#[tokio::test]
async fn a_read_is_served_in_bounded_windows_and_a_range_crosses_them() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let bucket = named(&store, "conf-content-window").await;
    let key = object_key("windowed.bin");
    let content = ContentKey::Object(key.clone());

    // Three windows and a tail: the read is four statements, not one.
    let chunks = super::READ_WINDOW_CHUNKS + 2;
    let body: Vec<u8> = (0..chunks * MIB + 3)
        .map(|offset| (offset % 251) as u8)
        .collect();
    put(&store, &bucket, &content, &body).await;
    assert_eq!(read(&store, &bucket, &content, None).await, body);

    // A range that starts inside the second window and ends inside the last
    // chunk of the third: both edges are sliced, and the windows between are
    // whole.
    let start = MIB + 17;
    let end = 2 * MIB + 5;
    assert_eq!(
        read(
            &store,
            &bucket,
            &content,
            Some(ByteRange::Inclusive(start as u64, end as u64))
        )
        .await,
        body[start..=end],
        "a range crossing window boundaries is served exactly"
    );
    assert_eq!(
        read(
            &store,
            &bucket,
            &content,
            Some(ByteRange::From((body.len() - 1) as u64))
        )
        .await,
        body[body.len() - 1..],
        "a one-byte read is the last byte, not a window's worth"
    );
    assert_eq!(
        read(&store, &bucket, &content, Some(ByteRange::Inclusive(0, 0))).await,
        body[..1],
        "a one-byte read from the head is one byte"
    );
    assert_eq!(
        read(
            &store,
            &bucket,
            &content,
            Some(ByteRange::Suffix((2 * MIB + 1) as u64))
        )
        .await,
        body[body.len() - (2 * MIB + 1)..],
        "a suffix spans two windows"
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

/// A removal that races a read answers not-found, never corruption: the reader
/// that has already resolved its head and its first window serves those bytes,
/// and the deleted key's next read is the contract's missing-content answer.
/// The alternative is the 5xx this closes: a head read on one connection and a
/// chunk read on another, with the delete committed between them.
#[tokio::test]
async fn a_removal_racing_a_read_is_missing_content_never_corruption() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let bucket = named(&store, "conf-content-race-remove").await;
    let key = object_key("raced-read.bin");
    let content = ContentKey::Object(key.clone());

    // One window's worth: the bytes are resolved with the head, so the delete
    // that lands next cannot shorten the read the caller already holds.
    let body: Vec<u8> = (0..2 * MIB).map(|offset| (offset % 251) as u8).collect();
    put(&store, &bucket, &content, &body).await;

    let mut open = store
        .open(&bucket, &content, None)
        .await
        .expect("the head is live");
    store
        .remove(&bucket, &content)
        .await
        .expect("the racing removal lands");

    let mut served = Vec::new();
    while let Some(chunk) = open.next().await {
        served.extend_from_slice(&chunk.expect("the bytes the head resolved are served"));
    }
    assert_eq!(served, body, "the read in flight is never reported corrupt");

    let Err(err) = store.open(&bucket, &content, None).await else {
        panic!("a deleted key is an error, never an empty body");
    };
    assert!(
        matches!(&err, Error::MissingContent(ContentKey::Object(named)) if named == &key),
        "a delete racing a read is not-found, never corruption: {err}"
    );
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
    let bucket = named(&store, "conf-content-badkey").await;
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
    let bucket = named(&store, "conf-content-move").await;
    let a = object_key("a.bin");
    let b = object_key("b.bin");
    let c = object_key("c.bin");
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

    let missing = object_key("missing.bin");
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

/// **A copy (and a rename) of a multi-batch object is written one bounded
/// statement a window, and reads back byte for byte.** The source generation
/// spans more than one [`super::MOVE_BATCH_CHUNKS`]-row window, so the statement
/// boundary is where a copy that mis-ordered its windows -- or flipped before
/// the last one landed -- would show: the windows cover `0..chunks` in order and
/// the flip's guard counts the run, so a body that is not the source's own bytes
/// cannot go live.
#[tokio::test]
async fn a_copy_of_a_multi_batch_object_round_trips_in_bounded_windows() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let bucket = named(&store, "conf-content-copy-batches").await;
    let from = object_key("copy-src/big.bin");
    let to = object_key("copy-dst/big.bin");

    // Three copy windows' worth of chunks: a boundary falls inside the body.
    let chunks = super::MOVE_BATCH_CHUNKS as usize * 2 + 1;
    let body: Vec<u8> = (0..chunks * MIB)
        .map(|offset| (offset % 251) as u8)
        .collect();
    put(&store, &bucket, &ContentKey::Object(from.clone()), &body).await;

    // The copy's plan is the re-home's own: one bounded statement per window,
    // covering the run in order. A single statement over the whole generation --
    // the shape PG22 excludes -- would be one window here.
    assert_eq!(
        super::copy_batches(super::publish_plan(body.len() as u64).chunks),
        vec![(0, 7), (8, 15), (16, 16)],
        "the copy's windows cover the run in `0..chunks` order"
    );

    store
        .copy(&bucket, &from, &to)
        .await
        .expect("the copy lands");
    assert_eq!(
        read(&store, &bucket, &ContentKey::Object(to.clone()), None).await,
        body,
        "a multi-batch copy reads back byte for byte"
    );
    assert_eq!(
        read(&store, &bucket, &ContentKey::Object(from.clone()), None).await,
        body,
        "the copy leaves the source where it was"
    );

    // Rename is the same bounded copy followed by a source drain: the bytes move.
    let moved = object_key("copy-moved/big.bin");
    store
        .rename(&bucket, &to, &moved)
        .await
        .expect("the rename lands");
    assert_eq!(
        read(&store, &bucket, &ContentKey::Object(moved.clone()), None).await,
        body,
        "a multi-batch rename reads back byte for byte"
    );
    assert!(
        store
            .open(&bucket, &ContentKey::Object(to), None)
            .await
            .is_err(),
        "the rename drains its source"
    );

    store
        .remove_many(
            &bucket,
            &[ContentKey::Object(from), ContentKey::Object(moved)],
        )
        .await
        .expect("the objects drain");
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
    let bucket = named(&store, "conf-content-scan").await;
    let names = ["alpha", "alpha-1", "alpine", "beta"];
    for name in names {
        let key = object_key(name);
        put(&store, &bucket, &ContentKey::Object(key), b"x").await;
    }

    // A part inside the walked prefix proves the `kind = 'object'` exclusion is real.
    let part = ContentKey::Part {
        upload_id: "7a2e1c3d-8b4f-4a6e-b0c1-3e5d9f2a7c48".to_owned(),
        number: part_number(1),
    };
    put(&store, &bucket, &part, b"part").await;
    assert_eq!(read(&store, &bucket, &part, None).await, b"part");

    assert_eq!(
        walked(&store, &bucket, "", None, 100).await,
        vec![
            object_key("alpha"),
            object_key("alpha-1"),
            object_key("alpine"),
            object_key("beta"),
        ],
        "scan is ascending by the key's UTF-8 bytes"
    );
    assert_eq!(
        walked(&store, &bucket, "alpha", None, 100).await,
        vec![object_key("alpha"), object_key("alpha-1"),],
        "prefix is a bound the walk must not scan past"
    );
    let after = object_key("alpha");
    assert_eq!(
        walked(&store, &bucket, "alpha", Some(&after), 100).await,
        vec![object_key("alpha-1")],
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
    assert_eq!(
        store
            .scan(&bucket, "", None, usize::MAX)
            .await
            .expect("a limit that does not fit an i64 reads as no bound")
            .len(),
        4,
        "an unrepresentable limit is never a negative count"
    );
    let below = object_key("a");
    assert_eq!(
        walked(&store, &bucket, "alpha", Some(&below), 100).await,
        vec![object_key("alpha"), object_key("alpha-1"),],
        "an after below the prefix cannot hide the prefix's page"
    );

    // The scanned entry and the point probe agree on the B1 inputs.
    let scanned = store
        .scan(&bucket, "alpha", None, 100)
        .await
        .expect("the walk runs");
    let stat = store
        .stat(&bucket, &object_key("alpha"))
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
        let key = object_key(name);
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

/// Naming a live generation in a reclaim list does not delete it. An in-place
/// publish keeps the sequence, so the scanned key is not proof of residue.
#[tokio::test]
async fn reclaim_leaves_a_live_generation_named_in_the_list() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let bucket = named(&store, "conf-content-reclaim-live").await;
    let object_key = object_key("live.bin");
    let content = ContentKey::Object(object_key);
    let body = b"live-body";
    put(&store, &bucket, &content, body).await;
    let head = store
        .live_generation(&bucket, &content)
        .await
        .expect("the head reads")
        .expect("the generation is live");
    let (_, ckey) = super::content_key(&content);
    let gone = store
        .reclaim_orphan_chunks(
            &[OrphanChunk {
                bucket: bucket.clone(),
                kind: super::ContentKind::Object,
                ckey,
                seq: head.seq,
            }],
            Duration::from_secs(0),
        )
        .await
        .expect("the reclaim runs");
    assert_eq!(gone, 0, "a live generation is not residue");
    assert_eq!(read(&store, &bucket, &content, None).await, body);
    db.drop_db().await;
}

/// A drain keeps the head and moves its sequence past the drained generation
/// (R2), so a stage that outlived the drain can never reuse the generation id:
/// its rows land at its own sequence, not over another writer's, and a body the
/// drain has taken rows from cannot flip at all — a short generation would be
/// served as `Corrupt` on a `live_len` the head reports whole. The key still
/// publishes after the drain.
#[tokio::test]
async fn a_drain_keeps_the_generation_id_out_of_reach() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let bucket = named(&store, "conf-content-drained-stage").await;
    let object_key = object_key("drained.bin");
    let content = ContentKey::Object(object_key.clone());

    // A body past one publish batch, so the stage has already committed rows of
    // its own before the drain runs — the generation the drain takes apart.
    let body: Vec<u8> = (0..(super::PUBLISH_BATCH_CHUNKS + 1) * MIB)
        .map(|offset| (offset % 251) as u8)
        .collect();
    let mut stale = store
        .stage(&bucket, &content)
        .await
        .expect("the stage opens");
    stale
        .write(Bytes::from(body.clone()))
        .await
        .expect("the bytes stage");
    let stale_seq = stale.seq;

    // The drain, then another writer's publish over the same key.
    store
        .remove(&bucket, &content)
        .await
        .expect("the removal lands");
    put(&store, &bucket, &content, b"live").await;
    let live = store
        .live_generation(&bucket, &content)
        .await
        .expect("the head reads")
        .expect("the generation is live");
    assert!(
        live.seq != stale_seq,
        "the drained generation's id is never handed out again: {stale_seq} then {}",
        live.seq
    );

    // The stage that outlived the drain cannot go live: its first batch went
    // with the drain, so its own row-count check refuses the flip rather than
    // splicing a tail into the live writer's generation.
    let err = store
        .publish(&bucket, stale, &content)
        .await
        .expect_err("a drained stage holds only a tail");
    assert!(
        matches!(err, Error::PublishRaced),
        "the refusal is the publish race, got {err}"
    );
    assert_eq!(
        read(&store, &bucket, &content, None).await,
        b"live",
        "the live writer's bytes stay whole"
    );

    // And the key still publishes after the drain.
    put(&store, &bucket, &content, b"after").await;
    assert_eq!(read(&store, &bucket, &content, None).await, b"after");

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

/// A superseded publish's re-home is bounded: the re-keyed rows are moved in
/// windows of [`super::MOVE_BATCH_CHUNKS`], each its own statement and its own
/// transaction, and the flip still lands with the body whole.
#[tokio::test]
async fn a_superseded_publish_re_homes_in_bounded_batches() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let bucket = named(&store, "conf-content-rehome-batches").await;
    let key = object_key("raced/big.bin");
    let content = ContentKey::Object(key.clone());

    // Three move windows' worth of chunks.
    let chunks = super::MOVE_BATCH_CHUNKS as usize * 2 + 1;
    let body: Vec<u8> = (0..chunks * MIB)
        .map(|offset| (offset % 251) as u8)
        .collect();
    let mut first = store
        .stage(&bucket, &content)
        .await
        .expect("the first stage opens");
    first
        .write(Bytes::from(body.clone()))
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

    // The plan the re-home rewrites that generation with: one bounded statement
    // per window, covering the whole run. A single statement over the
    // generation — the shape PG22 excludes — would be one window here.
    let plan = super::move_batches(super::publish_plan(body.len() as u64).chunks);
    assert_eq!(
        plan.len(),
        chunks.div_ceil(super::MOVE_BATCH_CHUNKS as usize),
        "one statement per MOVE_BATCH_CHUNKS rows"
    );
    assert!(
        plan.iter()
            .all(|(from, to)| to - from < super::MOVE_BATCH_CHUNKS as i32),
        "no statement moves more than the bound, whatever the body's size"
    );
    assert_eq!(
        plan,
        vec![(0, 7), (8, 15), (16, 16)],
        "the windows cover the run in order"
    );

    // The flip still lands on the re-keyed generation, whole.
    store
        .publish(&bucket, first, &content)
        .await
        .expect("the superseded publish converges");
    assert_eq!(read(&store, &bucket, &content, None).await, body);

    store
        .publish(&bucket, second, &content)
        .await
        .expect("the later publish lands");
    assert_eq!(read(&store, &bucket, &content, None).await, b"second");

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

/// A guarded byte compare reads a multi-window generation one window at a time
/// under the head's lock: a difference inside a later window is still seen, and a
/// refused compare leaves the bytes where they are.
#[tokio::test]
async fn a_guarded_removal_compares_a_multi_window_generation() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let bucket = named(&store, "conf-content-guard-window").await;
    let key = object_key("guarded.bin");
    let content = ContentKey::Object(key.clone());

    let windows = super::READ_WINDOW_CHUNKS + 1;
    let body: Vec<u8> = (0..windows * MIB)
        .map(|offset| (offset % 251) as u8)
        .collect();
    put(&store, &bucket, &content, &body).await;

    // A difference past the first window: the compare has to read on to see it.
    let mut other = body.clone();
    other[super::READ_WINDOW_CHUNKS * MIB] ^= 0xFF;
    assert!(
        !store
            .remove_if_matches(&bucket, &content, &other)
            .await
            .expect("the compare runs"),
        "a difference in a later window is seen"
    );
    assert_eq!(
        read(&store, &bucket, &content, None).await,
        body,
        "a refused compare leaves the bytes"
    );
    assert!(
        store
            .remove_if_matches(&bucket, &content, &body)
            .await
            .expect("the compare runs"),
        "the whole generation matches"
    );
    assert!(store.open(&bucket, &content, None).await.is_err());
    db.drop_db().await;
}

/// The live object heads `starts_with` selects for `prefix`, in collation
/// order: the definer the walk's byte range has to reproduce row for row. The
/// cursor is `after`'s encoded key, exclusive, or no bound at all.
async fn defined_by_starts_with(
    store: &PgStore,
    bucket: &bucket::Name,
    prefix: &str,
    after: Option<&object::Key>,
) -> Vec<String> {
    crate::_sqlx::query_scalar!(
        "SELECT ckey AS \"ckey!\" FROM content_heads \
         WHERE bucket = $1 AND kind = 'object'::content_kind AND live_seq IS NOT NULL \
         AND starts_with(ckey, $2) \
         AND ($3::text IS NULL OR ckey COLLATE \"C\" > $3) \
         ORDER BY ckey COLLATE \"C\"",
        bucket.as_ref(),
        format!("{}{prefix}", super::OBJECT_CKEY_PREFIX),
        after.map(super::object_ckey)
    )
    .fetch_all(store.as_driver())
    .await
    .expect("the definer reads")
}

/// The walked page as the encoded keys the definer answers with.
async fn walked_ckeys(
    store: &PgStore,
    bucket: &bucket::Name,
    prefix: &str,
    after: Option<&object::Key>,
) -> Vec<String> {
    walked(store, bucket, prefix, after, 100)
        .await
        .iter()
        .map(super::object_ckey)
        .collect()
}

/// The walk's byte range is the `starts_with` definer exactly, over the keys a
/// coarser upper bound drops: a key whose first character past the prefix is
/// `U+10FFFF` — the last code point, a legal object key — and keys carrying
/// more after it. Parity is checked row for row against the definer, for a
/// plain prefix, for that noncharacter, for the empty prefix, with and without
/// a cursor.
#[tokio::test]
async fn scan_lists_every_key_the_prefix_definer_selects() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let bucket = named(&store, "conf-content-scan-edge").await;
    let names = [
        "x",
        "x\u{10FFFF}",
        "x\u{10FFFF}a",
        "x\u{10FFFF}\u{10FFFF}",
        "x\u{10FFFF}/y",
        "x/a",
        "xπ",
        "xÿ",
        "xĀ",
        "xy",
        "y",
    ];
    for name in names {
        let key = object_key(name);
        put(&store, &bucket, &ContentKey::Object(key), b"x").await;
    }
    // A part inside the walked key space, so the kind discriminator is real here
    // too: it is never listed, and the definer query agrees.
    let part = ContentKey::Part {
        upload_id: "5c1f2d3e-4a5b-4c6d-8e9f-0a1b2c3d4e5f".to_owned(),
        number: part_number(1),
    };
    put(&store, &bucket, &part, b"part").await;

    let last = object_key("x\u{10FFFF}");
    for (prefix, after) in [
        ("", None),
        ("x", None),
        ("x", Some(&last)),
        ("x\u{10FFFF}", None),
        ("x\u{10FFFF}a", None),
        ("xπ", None),
        ("x/", None),
        ("xy", None),
        ("z", None),
    ] {
        let listed = walked_ckeys(&store, &bucket, prefix, after).await;
        assert_eq!(
            listed,
            defined_by_starts_with(&store, &bucket, prefix, after).await,
            "the walk and the definer agree for prefix {prefix:?}, after {after:?}"
        );
    }

    // The page is not vacuously empty: the noncharacter keys are listed, and the
    // cursor only takes what it should.
    let page = walked_ckeys(&store, &bucket, "x", None).await;
    assert_eq!(
        page.len(),
        10,
        "every `x` object, the noncharacters included"
    );
    assert!(
        page.contains(&"o:x\u{10FFFF}a".to_owned()),
        "a key past the noncharacter is listed: {page:?}"
    );
    let after = walked_ckeys(&store, &bucket, "x", Some(&last)).await;
    assert!(
        !after.contains(&"o:x\u{10FFFF}".to_owned())
            && after.contains(&"o:x\u{10FFFF}a".to_owned()),
        "the cursor is exclusive and still spans the noncharacter: {after:?}"
    );

    for name in names {
        let key = object_key(name);
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

/// The prefix bound is the prefix set's own successor: the last character
/// incremented, with a trailing `U+10FFFF` (or a lone surrogate) dropped
/// instead, and the object key space's `o:` as the floor the strip stops at.
#[test]
fn the_prefix_bound_is_the_prefix_sets_own_successor() {
    assert_eq!(super::prefix_upper("o:x"), "o:y");
    assert_eq!(super::prefix_upper("o:"), "o;");
    assert_eq!(super::prefix_upper("o:x/"), "o:x0");
    assert_eq!(super::prefix_upper("o:xπ"), "o:xρ");
    assert_eq!(
        super::prefix_upper("o:x\u{10FFFF}"),
        "o:y",
        "the last code point has no successor: the character before it carries the bound"
    );
    assert_eq!(super::prefix_upper("o:x\u{10FFFF}\u{10FFFF}"), "o:y");
    assert_eq!(super::prefix_upper("o:x\u{D7FF}"), "o:y", "a surrogate too");
    assert_eq!(
        super::prefix_upper(""),
        "o;",
        "an empty prefix still bounds the object key space it belongs to"
    );
}

/// A removal clears the live generation and keeps the head, so the next
/// allocation continues the sequence instead of starting again at 1.
#[tokio::test]
async fn remove_keeps_the_head_sequence() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let bucket = named(&store, "conf-content-seq").await;
    let object_key = object_key("seq.bin");
    let content = ContentKey::Object(object_key);
    put(&store, &bucket, &content, b"one").await;
    let first = store
        .live_generation(&bucket, &content)
        .await
        .expect("the head reads")
        .expect("the generation is live")
        .seq;
    store
        .remove(&bucket, &content)
        .await
        .expect("the removal lands");
    put(&store, &bucket, &content, b"two").await;
    let second = store
        .live_generation(&bucket, &content)
        .await
        .expect("the head reads")
        .expect("the generation is live")
        .seq;
    assert!(
        second > first,
        "the head kept its sequence: {first} then {second}"
    );
    assert_eq!(read(&store, &bucket, &content, None).await, b"two");
    db.drop_db().await;
}
