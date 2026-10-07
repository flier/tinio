//! The engine's own tests: the state machine over the in-crate port double.

use crate::{
    _core::{
        blob::{BlockHash, BlockStore},
        bucket::Name,
        etag::ETag,
        meta::version::Kind,
        version::State,
    },
    error::Error,
    ports::Current,
    testutil::{bucket, fixture, key, recorded},
};

#[tokio::test]
async fn a_supersede_moves_the_outgoing_file_into_the_block_store_and_stamps_the_row() {
    let fixture = fixture();
    let bucket = bucket();
    let key = key();
    // The identity the row must carry is the one the key's current slot
    // already records; the write path that plants it is Task 4's, so the
    // double plants it here.
    fixture.ports.set_slot(&bucket, &key, recorded("v1", 1));
    fixture.ports.write_content(&bucket, &key, b"first").await;
    let versions = &fixture.versions;

    versions.set_state(&bucket, State::Enabled).await.unwrap();
    assert_eq!(versions.state(&bucket).await.unwrap(), State::Enabled);

    versions.supersede(&bucket, &key).await.unwrap();

    let (seq, row) = versions
        .latest(&bucket, &key)
        .await
        .unwrap()
        .expect("a version row");
    assert_eq!(
        (seq, row.id.as_ref().expect("a named version").as_str()),
        (1, "v1"),
        "the row carries the outgoing identity and sequence the slot recorded, never a fresh one"
    );
    assert_eq!(row.size, 5);
    assert_eq!(row.kind, Kind::Object);
    assert_ne!(
        row.became_noncurrent, 0,
        "the row is stamped when it stops being current"
    );
    assert!(
        versions
            .blobs()
            .blocks()
            .contains(row.blob.hash())
            .await
            .unwrap(),
        "the content is in the block store"
    );
    assert_eq!(row.blob.hash(), &BlockHash::of(b"first"));
}

#[tokio::test]
async fn a_second_supersede_of_the_same_version_rewrites_the_same_row() {
    // The crash window between the row's commit and the key path's overwrite
    // is recovered by re-running the supersede: the slot still records the
    // same version, so the same row is rewritten — never a second identity
    // for the same content.
    let fixture = fixture();
    let bucket = bucket();
    let key = key();
    fixture.ports.set_slot(&bucket, &key, recorded("v1", 1));
    fixture.ports.write_content(&bucket, &key, b"first").await;
    let versions = &fixture.versions;

    versions.supersede(&bucket, &key).await.unwrap();
    versions.supersede(&bucket, &key).await.unwrap();

    assert_eq!(
        fixture.ports.row_count(&bucket, &key),
        1,
        "one row, not two"
    );
    let (seq, row) = versions.latest(&bucket, &key).await.unwrap().unwrap();
    assert_eq!(
        (seq, row.id.as_ref().expect("a named version").as_str()),
        (1, "v1")
    );
}

#[tokio::test]
async fn materializing_a_version_writes_its_bytes_back_to_the_key_path() {
    let fixture = fixture();
    let bucket = bucket();
    let key = key();
    fixture.ports.set_slot(&bucket, &key, recorded("v1", 1));
    fixture.ports.write_content(&bucket, &key, b"first").await;
    let versions = &fixture.versions;

    versions.supersede(&bucket, &key).await.unwrap();
    let (_, row) = versions.latest(&bucket, &key).await.unwrap().unwrap();

    // The write that follows a supersede overwrites the key path; the
    // materialize puts the stored version back.
    fixture.ports.write_content(&bucket, &key, b"second").await;
    let published = versions
        .materialize(&bucket, &key, &row)
        .await
        .unwrap()
        .expect("an object row publishes content");
    assert_eq!(published.size, 5, "the publish answers the byte count");
    assert_eq!(
        published.etag,
        ETag::from_content(b"first"),
        "and the content ETag the backend's write path computed"
    );

    assert_eq!(
        fixture.ports.read_content(&bucket, &key).await.as_deref(),
        Some(b"first".as_slice())
    );
}

#[tokio::test]
async fn a_supersede_race_retries_once_and_commits_the_version_that_was_current() {
    let fixture = fixture();
    let bucket = bucket();
    let key = key();
    fixture.ports.set_slot(&bucket, &key, recorded("v1", 1));
    fixture.ports.write_content(&bucket, &key, b"first").await;
    let versions = &fixture.versions;

    // The out-of-band write: the content becomes the moved version's, and the
    // slot the double installs at the first read moves v1 → v2. The design's
    // retry re-resolves and reads again, so the settled v2 is what lands in
    // history.
    fixture.ports.write_content(&bucket, &key, b"second").await;
    fixture.ports.swap_slot_on_open(recorded("v2", 2));

    versions.supersede(&bucket, &key).await.unwrap();

    let (seq, row) = versions
        .latest(&bucket, &key)
        .await
        .unwrap()
        .expect("the retry commits a row");
    assert_eq!(
        (seq, row.id.as_ref().expect("a named version").as_str()),
        (2, "v2"),
        "the version that was actually current, not the one the first resolve named"
    );
    assert_eq!(
        row.blob.hash(),
        &BlockHash::of(b"second"),
        "and the bytes it actually holds"
    );
}

#[tokio::test]
async fn a_slot_that_keeps_moving_still_answers_the_race() {
    let fixture = fixture();
    let bucket = bucket();
    let key = key();
    fixture.ports.set_slot(&bucket, &key, recorded("v1", 1));
    fixture.ports.write_content(&bucket, &key, b"first").await;
    let versions = &fixture.versions;

    // Two moves, so both the attempt and its one retry are refused: only then
    // does the call answer the race, and it writes no row.
    fixture.ports.write_content(&bucket, &key, b"second").await;
    fixture
        .ports
        .swap_slots_on_open([recorded("v2", 2), recorded("v3", 3)]);

    let err = versions.supersede(&bucket, &key).await.unwrap_err();
    let raced = match err {
        Error::SupersedeRace { key } => key,
        other => panic!("the race is named, got {other:?}"),
    };
    assert_eq!(raced, key, "the error names the key whose content moved");
    assert_eq!(
        fixture.ports.row_count(&bucket, &key),
        0,
        "the race writes no row"
    );
}

#[tokio::test]
async fn a_threaded_slot_supersedes_once_and_a_stale_one_recovers_on_the_retry() {
    // The write paths' shape: the slot is read once, for the branch
    // decision, and handed to `supersede_with` — which must still
    // re-resolve after the content read (the supersede race is unchanged).
    let fixture = fixture();
    let bucket = bucket();
    let key = key();
    fixture.ports.set_slot(&bucket, &key, recorded("v1", 1));
    fixture.ports.write_content(&bucket, &key, b"first").await;
    let versions = &fixture.versions;

    let slot = versions.current(&bucket, &key).await.unwrap();
    versions.supersede_with(&bucket, &key, &slot).await.unwrap();
    let (seq, row) = versions.latest(&bucket, &key).await.unwrap().unwrap();
    assert_eq!(
        (seq, row.id.as_ref().expect("a named version").as_str()),
        (1, "v1"),
        "the threaded slot's identity and sequence are what the row carries"
    );

    // A stale threaded slot — the caller read it, then the slot moved once —
    // recovers on the design's retry: the fresh resolve is the version that
    // lands in history.
    let stale = versions.current(&bucket, &key).await.unwrap();
    fixture.ports.set_slot(&bucket, &key, recorded("v2", 2));
    fixture.ports.write_content(&bucket, &key, b"second").await;
    versions
        .supersede_with(&bucket, &key, &stale)
        .await
        .unwrap();
    let (seq, row) = versions.latest(&bucket, &key).await.unwrap().unwrap();
    assert_eq!(
        (seq, row.id.as_ref().expect("a named version").as_str()),
        (2, "v2"),
        "the retry committed the moved slot's version"
    );
    assert_eq!(row.blob.hash(), &BlockHash::of(b"second"));
}

#[tokio::test]
async fn a_supersede_whose_slot_names_a_version_with_no_content_is_a_race() {
    // The slot and the content port disagree: no identity may be stamped onto
    // content of unknown provenance.
    let fixture = fixture();
    let bucket = bucket();
    let key = key();
    fixture.ports.set_slot(&bucket, &key, recorded("v1", 1));

    let err = fixture.versions.supersede(&bucket, &key).await.unwrap_err();
    assert!(matches!(err, Error::SupersedeRace { .. }), "got {err:?}");
    assert_eq!(fixture.ports.row_count(&bucket, &key), 0);
}

#[tokio::test]
async fn superseding_a_key_with_no_current_version_is_a_no_op() {
    let fixture = fixture();
    let bucket = bucket();
    let key = key();

    fixture.versions.supersede(&bucket, &key).await.unwrap();

    assert!(
        fixture
            .versions
            .latest(&bucket, &key)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn a_delete_marker_supersedes_to_a_marker_row_with_the_empty_blob_and_no_read() {
    let fixture = fixture();
    let bucket = bucket();
    let key = key();
    // A marker is current: the slot records it and there is no content.
    fixture.ports.set_slot(
        &bucket,
        &key,
        Current {
            exists: true,
            version: Some("m1".parse().unwrap()),
            is_delete_marker: true,
            seq: 3,
            degraded: false,
        },
    );
    let versions = &fixture.versions;

    versions.supersede(&bucket, &key).await.unwrap();

    let (seq, row) = versions
        .latest(&bucket, &key)
        .await
        .unwrap()
        .expect("a marker row");
    assert_eq!(
        (seq, row.id.as_ref().expect("a named version").as_str()),
        (3, "m1")
    );
    assert_eq!(row.kind, Kind::Marker);
    assert!(row.blob.is_empty(), "a marker's row carries the empty blob");
    assert_eq!(
        versions.blobs().blocks().len(),
        0,
        "a marker has no content to publish"
    );
}

#[tokio::test]
async fn the_null_version_supersedes_at_sequence_zero() {
    let fixture = fixture();
    let bucket = bucket();
    let key = key();
    // A file written before the bucket was versioned: current, no identity.
    fixture.ports.set_slot(
        &bucket,
        &key,
        Current {
            exists: true,
            version: None,
            is_delete_marker: false,
            seq: 0,
            degraded: false,
        },
    );
    fixture.ports.write_content(&bucket, &key, b"first").await;

    fixture.versions.supersede(&bucket, &key).await.unwrap();

    let (seq, row) = fixture
        .versions
        .latest(&bucket, &key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(seq, 0, "the null version's sequence is zero");
    assert_eq!(row.id, None, "the null version has no identity");
}

#[tokio::test]
async fn materializing_a_marker_is_a_documented_no_op_that_never_rewrites_the_key() {
    // Documents today's behavior: the engine answers a silent `Ok` for a
    // marker row (a reviewer flagged the silence). The call must read no
    // block and publish nothing — this pins that it leaves whatever the key
    // path holds untouched, so a future change to "error on a marker" is a
    // deliberate one.
    let fixture = fixture();
    let bucket = bucket();
    let key = key();
    fixture.ports.set_slot(
        &bucket,
        &key,
        Current {
            exists: true,
            version: Some("m1".parse().unwrap()),
            is_delete_marker: true,
            seq: 3,
            degraded: false,
        },
    );
    let versions = &fixture.versions;
    versions.supersede(&bucket, &key).await.unwrap();
    let (_, marker) = versions.latest(&bucket, &key).await.unwrap().unwrap();

    // Content that appeared at the key path after the marker was recorded.
    fixture.ports.write_content(&bucket, &key, b"live").await;
    let result = versions.materialize(&bucket, &key, &marker).await;

    assert!(result.is_ok(), "the marker arm answers Ok, not an error");
    assert_eq!(
        fixture.ports.read_content(&bucket, &key).await.as_deref(),
        Some(b"live".as_slice()),
        "a marker materialize is a no-op: the key path keeps whatever it held"
    );
    assert_eq!(
        versions.blobs().blocks().len(),
        0,
        "and it never reads the block store (the empty blob was never published)"
    );
}

#[tokio::test]
async fn expiring_a_version_drops_its_row_and_its_index_entry() {
    let fixture = fixture();
    let bucket = bucket();
    let key = key();
    fixture.ports.set_slot(&bucket, &key, recorded("v1", 1));
    fixture.ports.write_content(&bucket, &key, b"first").await;
    let versions = &fixture.versions;
    versions.supersede(&bucket, &key).await.unwrap();
    assert_eq!(
        fixture.ports.index_seq(&bucket, &key, "v1"),
        Some(1),
        "the supersede committed the row's index entry"
    );

    versions.expire(&bucket, &key, 1).await.unwrap();
    versions.expire(&bucket, &key, 1).await.unwrap(); // idempotent

    assert!(
        versions.latest(&bucket, &key).await.unwrap().is_none(),
        "the key's last row is gone, so latest answers None"
    );
    assert_eq!(fixture.ports.row_count(&bucket, &key), 0);
    assert_eq!(
        fixture.ports.index_seq(&bucket, &key, "v1"),
        None,
        "the index entry went with the row: a version-addressed read can no longer resolve it"
    );
}

#[tokio::test]
async fn latest_answers_the_newest_sequence_of_that_key() {
    let fixture = fixture();
    let bucket = bucket();
    let key = key();
    let versions = &fixture.versions;

    for (id, seq, content) in [("v1", 1u64, b"one".as_slice()), ("v2", 2, b"two")] {
        fixture.ports.set_slot(&bucket, &key, recorded(id, seq));
        fixture.ports.write_content(&bucket, &key, content).await;
        versions.supersede(&bucket, &key).await.unwrap();
    }

    let (seq, row) = versions.latest(&bucket, &key).await.unwrap().unwrap();
    assert_eq!(
        (seq, row.id.as_ref().expect("a named version").as_str()),
        (2, "v2"),
        "the highest sequence wins"
    );
}

#[tokio::test]
async fn expiring_a_batch_drops_every_row_in_one_port_call() {
    let fixture = fixture();
    let bucket = bucket();
    let key = key();
    let versions = &fixture.versions;

    for (id, seq, content) in [
        ("v1", 1u64, b"one".as_slice()),
        ("v2", 2, b"two"),
        ("v3", 3, b"three"),
    ] {
        fixture.ports.set_slot(&bucket, &key, recorded(id, seq));
        fixture.ports.write_content(&bucket, &key, content).await;
        versions.supersede(&bucket, &key).await.unwrap();
    }
    assert_eq!(fixture.ports.row_count(&bucket, &key), 3);

    versions.expire_many(&bucket, &key, &[1, 2]).await.unwrap();
    assert_eq!(
        fixture.ports.batch_removals(),
        1,
        "the batched path is one port call, not one per version"
    );
    assert_eq!(
        fixture.ports.row_count(&bucket, &key),
        1,
        "every listed row is dropped"
    );
    assert_eq!(fixture.ports.index_seq(&bucket, &key, "v1"), None);
    assert_eq!(fixture.ports.index_seq(&bucket, &key, "v2"), None);
    assert_eq!(
        fixture.ports.index_seq(&bucket, &key, "v3"),
        Some(3),
        "an unlisted version keeps its row and its index entry"
    );

    // Idempotent, like the single-version call: dropping what is already
    // gone is Ok.
    versions.expire_many(&bucket, &key, &[1, 2]).await.unwrap();
    assert_eq!(fixture.ports.row_count(&bucket, &key), 1);
}

#[tokio::test]
async fn live_blocks_unions_every_rows_block_list() {
    let fixture = fixture();
    let bucket = bucket();
    let key = key();
    let versions = &fixture.versions;
    assert!(versions.live_blocks(&bucket).await.unwrap().is_empty());

    for (id, seq, content) in [("v1", 1u64, b"one".as_slice()), ("v2", 2, b"two")] {
        fixture.ports.set_slot(&bucket, &key, recorded(id, seq));
        fixture.ports.write_content(&bucket, &key, content).await;
        versions.supersede(&bucket, &key).await.unwrap();
    }

    let live = versions.live_blocks(&bucket).await.unwrap();
    assert_eq!(live.len(), 2, "one block per stored version");
    assert!(live.contains(&BlockHash::of(b"one")));
    assert!(live.contains(&BlockHash::of(b"two")));
}

#[tokio::test]
async fn live_blocks_unions_this_buckets_rows_and_excludes_another_buckets() {
    let fixture = fixture();
    let demo = bucket();
    let other = Name::from("other");
    let key = key();
    let versions = &fixture.versions;

    for (b, content, id) in [
        (&demo, b"demo-bytes".as_slice(), "v1"),
        (&other, b"other-bytes".as_slice(), "v2"),
    ] {
        fixture.ports.set_slot(b, &key, recorded(id, 1));
        fixture.ports.write_content(b, &key, content).await;
        versions.supersede(b, &key).await.unwrap();
    }

    let live = versions.live_blocks(&demo).await.unwrap();
    assert_eq!(live.len(), 1, "one stored version, one block");
    assert!(live.contains(&BlockHash::of(b"demo-bytes")));
    assert!(
        !live.contains(&BlockHash::of(b"other-bytes")),
        "another bucket's blocks are not live for this bucket"
    );
    let live_other = versions.live_blocks(&other).await.unwrap();
    assert_eq!(live_other.len(), 1);
    assert!(live_other.contains(&BlockHash::of(b"other-bytes")));
}

#[tokio::test]
async fn a_key_with_no_history_contributes_no_live_blocks() {
    let fixture = fixture();
    let bucket = bucket();

    assert!(
        fixture
            .versions
            .live_blocks(&bucket)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn the_bucket_state_round_trips_and_reads_current_through_the_gate() {
    let fixture = fixture();
    let bucket = bucket();
    let key = key();
    let versions = &fixture.versions;
    assert_eq!(versions.state(&bucket).await.unwrap(), State::Unversioned);

    versions.set_state(&bucket, State::Suspended).await.unwrap();

    assert_eq!(versions.state(&bucket).await.unwrap(), State::Suspended);
    assert!(!versions.current(&bucket, &key).await.unwrap().exists);
    fixture.ports.set_slot(&bucket, &key, recorded("v1", 1));
    let current = versions.current(&bucket, &key).await.unwrap();
    assert!(current.exists);
    assert_eq!(current.version.as_ref().unwrap().as_str(), "v1");
    assert_eq!(current.seq, 1);
    assert!(!current.is_null());
}
