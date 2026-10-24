use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use redb::{ReadableDatabase, WriteTransaction};
use tinio_util::testing::{
    metadata::{assert_version_conformance, reservation},
    spans::one_named,
};
use tracing::Level;

use crate::{
    _core::{
        ETag,
        acl::Acl,
        blob::{BlobRef, BlockHash},
        bucket,
        meta::{
            BucketMeta, ObjectMeta, UploadMeta, VersionMeta, bucket as bucket_row,
            object::{PartRow, Row as CallerRow},
            version::{Current, Stored},
        },
        object, object_lock, storage, to_nanos,
        version::{Id, State},
    },
    Driver, Error, RedbMetadata, bucket_object_lock,
    object_meta::{Stored as ObjectStored, Table as ObjectTable},
    open::{self, META_DB_FILE},
    state,
    testutil::capture_spans,
    upload_object_lock,
    version::{Kind as RowKind, Row as VersionRow, Table as VersionTable, encode_blob},
    version_object_lock::{self, Table as LockTable},
};

/// Plant the bucket row the guarded writers probe. The bucket plane's own
/// conformance leg lives in `assert_bucket_meta_conformance`.
async fn bucket_with(store: &RedbMetadata, bucket: &bucket::Name) {
    BucketMeta::create(store, bucket, &bucket_row::Row::at(SystemTime::UNIX_EPOCH))
        .await
        .unwrap();
}

/// The suite's version leg against a fresh in-memory implementation.
#[tokio::test]
async fn version_meta_conforms() {
    let store = RedbMetadata::in_memory().unwrap();
    assert_version_conformance(&store).await;
}

/// The slot columns are recorded **on the `object_meta` row** — the
/// landed table the scanner's orphan rule and the fs gate read — and
/// `advance_slot` derives the marker bit from the version row at the
/// slot's sequence.
#[tokio::test]
async fn advance_slot_records_the_slot_columns_on_the_object_row() {
    let store = RedbMetadata::in_memory().unwrap();
    let bucket = bucket::name("slot-columns").unwrap();
    let key = object::key("a.txt").unwrap();
    bucket_with(&store, &bucket).await;

    // A delete-marker version row at sequence 1, planted through the
    // table layer: this leg reads the object row, not the index.
    let (name, stored_key) = (bucket.to_string(), key.to_string());
    let marker = VersionRow {
        id: "v1".to_owned(),
        blob: encode_blob(&BlobRef::empty()),
        size: 0,
        etag: String::new(),
        created: 0,
        became_noncurrent: 0,
        kind: RowKind::Marker,
        tags: String::new(),
        checksum: String::new(),
        owner: String::new(),
        acl: String::new(),
        seq: 1,
    };
    let driver = store.driver();
    driver
        .write(move |txn: &mut WriteTransaction| -> Result<(), Error> {
            VersionTable::open(txn)?.put(&name, &stored_key, &marker)
        })
        .await
        .unwrap();

    store
        .advance_slot(&bucket, &key, Some(Id::new("v1").unwrap()), 1)
        .await
        .unwrap();
    let recorded = raw_row(&driver, &bucket, &key);
    assert_eq!(
        recorded.version_id.as_ref().map(|id| id.as_str()),
        Some("v1"),
        "the recorded identity is the slot's"
    );
    assert_eq!(recorded.version_seq, 1, "and so is the sequence");
    assert!(
        recorded.is_delete_marker,
        "the version row's marker kind is recorded as the row's own flag"
    );

    // The dangling advance: no row at sequence 9, so the recorded flag
    // is false while the identity is the recorded fact.
    store.advance_slot(&bucket, &key, None, 9).await.unwrap();
    let recorded = raw_row(&driver, &bucket, &key);
    assert_eq!(recorded.version_id, None);
    assert_eq!(recorded.version_seq, 9);
    assert!(
        !recorded.is_delete_marker,
        "a sequence with no version row is not a marker"
    );

    // The contract's own read agrees with the recorded columns.
    let slot: Option<Current> = store.current_slot(&bucket, &key).await.unwrap();
    let slot = slot.expect("a recorded slot");
    assert!(slot.exists && slot.version.is_none() && !slot.is_delete_marker);
}

/// The contract's two row reads inherit the table layer's corrupt-row
/// rules: the newest row's read **fails closed** — a corrupt newest row
/// answers `None`, never an older decodable version handed back as
/// current — and the bucket-wide walk **skips** the corrupt row instead of
/// failing. Only this side can plant one (D43).
#[tokio::test]
async fn the_contract_row_reads_heal_the_way_the_table_layer_rules() {
    let store = RedbMetadata::in_memory().unwrap();
    let bucket = bucket::name("corrupt-reads").unwrap();
    let key = object::key("a.txt").unwrap();
    let (name, stored_key) = (bucket.to_string(), key.to_string());

    let row = |seq: u64, blob: String| VersionRow {
        id: format!("v{seq}"),
        blob,
        size: 5,
        etag: String::new(),
        created: 0,
        became_noncurrent: 0,
        kind: RowKind::Object,
        tags: String::new(),
        checksum: String::new(),
        owner: String::new(),
        acl: String::new(),
        seq,
    };
    let good = encode_blob(&BlobRef::single(BlockHash::of(b"good"), 5));
    let driver = store.driver();
    driver
        .write(move |txn: &mut WriteTransaction| -> Result<(), Error> {
            let mut table = VersionTable::open(txn)?;
            table.put(&name, &stored_key, &row(1, good))?;
            // The newest row's shape is unreadable: no caller can repair it
            // from the contract, and it must not read as absent history.
            table.put(&name, &stored_key, &row(2, "garbage".to_owned()))
        })
        .await
        .unwrap();

    assert!(
        store.latest(&bucket, &key).await.unwrap().is_none(),
        "a corrupt newest row answers None, never the older version"
    );
    let rows = store.bucket_rows(&bucket).await.unwrap();
    assert_eq!(
        rows.iter().map(|stored| stored.seq).collect::<Vec<_>>(),
        vec![1],
        "the bucket walk skips the corrupt row"
    );
}

/// A slot never moves backwards: a lower-sequence advance is refused (the
/// same shaped refusal `put_version` raises for a route it cannot honour),
/// the slot it would have rewound is left untouched, and a same-sequence
/// re-run still lands.
#[tokio::test]
async fn advance_slot_refuses_to_move_the_slot_backwards() {
    let store = RedbMetadata::in_memory().unwrap();
    let bucket = bucket::name("monotonic").unwrap();
    let key = object::key("a.txt").unwrap();
    bucket_with(&store, &bucket).await;

    store
        .advance_slot(&bucket, &key, Some(Id::new("v5").unwrap()), 5)
        .await
        .unwrap();
    let err = store
        .advance_slot(&bucket, &key, Some(Id::new("v2").unwrap()), 2)
        .await
        .unwrap_err();
    // The backwards move is refused, not silently swallowed.
    assert!(matches!(err, Error::Refused(_)), "refused: {err:?}");
    let slot = store
        .current_slot(&bucket, &key)
        .await
        .unwrap()
        .expect("a recorded slot");
    assert_eq!(slot.version.as_ref().map(|id| id.as_str()), Some("v5"));

    // The crash window's own re-run sits at the same sequence: allowed.
    store
        .advance_slot(&bucket, &key, Some(Id::new("v5").unwrap()), 5)
        .await
        .unwrap();
    // A higher sequence still moves forward.
    store.advance_slot(&bucket, &key, None, 6).await.unwrap();
    let after = store
        .current_slot(&bucket, &key)
        .await
        .unwrap()
        .expect("a recorded slot");
    assert!(after.version.is_none() && !after.is_delete_marker);
}

/// A suspended bucket's in-place write to the **null** slot is not a
/// backward move (RC1, 2026-10-10): the null version's `seq = 0` is its own
/// numbering, so an advance from a recorded `(Some(id), 1)` slot onto the
/// null identity lands, records the null slot and stamps the version it
/// moved off. The plan for `(Put, Suspended)` over a real current version
/// answers exactly that pair (`identity = Null, seq = 0`), and before the
/// ruling the guard refused it — after the supersede, the publish and the
/// row write had committed, so the write took effect and then errored.
#[tokio::test]
async fn advance_slot_admits_the_null_identitys_in_place_move() {
    let store = RedbMetadata::in_memory().unwrap();
    let bucket = bucket::name("null-in-place").unwrap();
    let key = object::key("a.txt").unwrap();
    bucket_with(&store, &bucket).await;
    plant(&store, &bucket, &key, &[stamped_row(1, "v1", 0)]).await;

    // The enabled write's slot, then the suspended bucket's in-place write:
    // the same key, the null identity, the null version's own sequence.
    store
        .advance_slot(&bucket, &key, Some(Id::new("v1").unwrap()), 1)
        .await
        .unwrap();
    store
        .advance_slot(&bucket, &key, None, 0)
        .await
        .expect("the null version's in-place slot move is not a rewind");

    let slot = store
        .current_slot(&bucket, &key)
        .await
        .unwrap()
        .expect("a recorded slot");
    assert!(
        slot.exists && slot.version.is_none() && !slot.is_delete_marker,
        "the slot records the null identity: {slot:?}"
    );
    assert_ne!(
        stamp_of(&store, &bucket, &key, 1).await,
        0,
        "the version the null write moved off stopped being current"
    );
}

/// The companion of the null-identity rule: a move that keeps a **real**
/// identity and lowers the sequence is still the backward move the guard
/// exists to refuse — admitting the null write must not weaken it — and the
/// same-sequence re-run and the forward move behave as they always did.
#[tokio::test]
async fn advance_slot_still_refuses_a_lower_sequence_within_a_real_identity() {
    let store = RedbMetadata::in_memory().unwrap();
    let bucket = bucket::name("real-identity-rewind").unwrap();
    let key = object::key("a.txt").unwrap();
    bucket_with(&store, &bucket).await;
    plant(
        &store,
        &bucket,
        &key,
        &[stamped_row(5, "v5", 0), stamped_row(2, "v2", 0)],
    )
    .await;
    store
        .advance_slot(&bucket, &key, Some(Id::new("v5").unwrap()), 5)
        .await
        .unwrap();

    let refused = store
        .advance_slot(&bucket, &key, Some(Id::new("v2").unwrap()), 2)
        .await;
    assert!(matches!(refused, Err(Error::Refused(_))), "{refused:?}");
    let slot = store
        .current_slot(&bucket, &key)
        .await
        .unwrap()
        .expect("a recorded slot");
    assert_eq!(
        slot.version.as_ref().map(|id| id.as_str()),
        Some("v5"),
        "the slot the refused move would have rewound is untouched"
    );

    // The crash window's own re-run still lands, and so does a forward move:
    // the admission is the null identity's alone.
    store
        .advance_slot(&bucket, &key, Some(Id::new("v5").unwrap()), 5)
        .await
        .unwrap();
    store
        .advance_slot(&bucket, &key, Some(Id::new("v9").unwrap()), 9)
        .await
        .unwrap();
}

/// A slot advance stamps the row it moves off (the 2026-10-09 ruling): the
/// transaction that records the new current version records when the
/// superseded one stopped being current, in the same commit — so a row that
/// is history says so and its retention clock starts when the version
/// actually stopped being current, never earlier (the moment the supersede
/// committed it) and never at a re-run's later clock.
#[tokio::test]
async fn advance_slot_stamps_the_row_it_moves_off() {
    let store = RedbMetadata::in_memory().unwrap();
    let bucket = bucket::name("advance-stamps").unwrap();
    let key = object::key("a.txt").unwrap();
    bucket_with(&store, &bucket).await;
    // The current version's own row at sequence 1, written the way a
    // supersede commits it (`became_noncurrent == 0` — the crash window's
    // adopted row), and the incoming version's row at sequence 2.
    plant(
        &store,
        &bucket,
        &key,
        &[stamped_row(1, "v1", 0), stamped_row(2, "v2", 0)],
    )
    .await;
    store
        .advance_slot(&bucket, &key, Some(Id::new("v1").unwrap()), 1)
        .await
        .unwrap();
    assert_eq!(
        stamp_of(&store, &bucket, &key, 1).await,
        0,
        "the row the slot records is the current version's: not history yet"
    );

    let before = to_nanos(SystemTime::now());
    store
        .advance_slot(&bucket, &key, Some(Id::new("v2").unwrap()), 2)
        .await
        .unwrap();

    let stamped = stamp_of(&store, &bucket, &key, 1).await;
    assert_ne!(
        stamped, 0,
        "the row the advance moved off records when it stopped being current"
    );
    assert!(
        stamped >= before,
        "the stamp is the advancing transaction's own clock, in unix nanos"
    );
    assert_eq!(
        stamp_of(&store, &bucket, &key, 2).await,
        0,
        "the incoming version's own row is not history"
    );

    // A move off a sequence that holds no row is a no-op, not an error.
    store.advance_slot(&bucket, &key, None, 9).await.unwrap();
    assert!(
        store.get_version(&bucket, &key, 9).await.unwrap().is_none(),
        "an advance onto a free sequence writes no version row"
    );

    // The stamp rides the advancing transaction, so a refused advance
    // commits neither half: the row it would have moved off keeps the stamp
    // it had. One commit, or none — the atom the slot write and the stamp
    // share, which is what makes a crash between them unobservable.
    plant(&store, &bucket, &key, &[stamped_row(6, "v6", 0)]).await;
    let refused = store
        .advance_slot(&bucket, &key, Some(Id::new("v6").unwrap()), 6)
        .await;
    assert!(refused.is_err(), "a slot never moves backwards");
    assert_eq!(
        stamp_of(&store, &bucket, &key, 6).await,
        0,
        "a refused advance leaves the row it would have moved off unstamped"
    );

    // A key with no `object_meta` row has no **recorded** slot, so the
    // advance moves off nothing: the version row (the null version) at the
    // fabricated row's own default sequence is not stamped, because no
    // writer ever recorded a slot naming it.
    let fresh = object::key("b.txt").unwrap();
    plant(&store, &bucket, &fresh, &[stamped_row(0, "null", 0)]).await;
    store
        .advance_slot(&bucket, &fresh, Some(Id::new("v7").unwrap()), 1)
        .await
        .unwrap();
    assert_eq!(
        stamp_of(&store, &bucket, &fresh, 0).await,
        0,
        "an advance that materializes the object row moves off no recorded slot"
    );
}

/// The **first** stamp wins: a row that already says it is history keeps the
/// moment it stopped being current, so a re-run of the advance never moves
/// the retention clock — and a store that stamps the row when it creates it
/// (mem's supersede, the double stamp the contract allows) is not
/// contradicted by the advance that follows.
///
/// A same-sequence advance is the crash window's own re-run, not a move off
/// anything: the row the slot still records is left unstamped, because the
/// stamp is what marks a row history to a reader that knows no slot.
#[tokio::test]
async fn advance_slot_never_moves_a_stamp_it_finds() {
    let store = RedbMetadata::in_memory().unwrap();
    let bucket = bucket::name("advance-keeps-stamp").unwrap();
    let key = object::key("a.txt").unwrap();
    bucket_with(&store, &bucket).await;
    plant(
        &store,
        &bucket,
        &key,
        &[stamped_row(1, "v1", 7), stamped_row(2, "v2", 0)],
    )
    .await;

    // The slot records the stamped row — the shape mem's supersede leaves,
    // where the outgoing row carries its own stamp before the slot moves off
    // it — and the move away leaves that stamp exactly as it was.
    store
        .advance_slot(&bucket, &key, Some(Id::new("v1").unwrap()), 1)
        .await
        .unwrap();
    assert_eq!(
        stamp_of(&store, &bucket, &key, 1).await,
        7,
        "a row that already says it is history is not restamped"
    );
    store
        .advance_slot(&bucket, &key, Some(Id::new("v2").unwrap()), 2)
        .await
        .unwrap();
    assert_eq!(
        stamp_of(&store, &bucket, &key, 1).await,
        7,
        "the first stamp is when the version stopped being current"
    );

    // The crash window's own re-run sits at the sequence the slot already
    // names: nothing moved off it, so the current version's row stays
    // unstamped — a stamp here would read as a live version that is history.
    store
        .advance_slot(&bucket, &key, Some(Id::new("v2").unwrap()), 2)
        .await
        .unwrap();
    assert_eq!(
        stamp_of(&store, &bucket, &key, 2).await,
        0,
        "a same-sequence re-run does not stamp the current version's row"
    );
}

/// The deliberate divergence `advance_slot` documents: an advance onto a
/// key with no `object_meta` row materializes one, so an
/// [`ObjectMeta::get`] read reports a recorded object for a key whose
/// content side holds nothing.
#[tokio::test]
async fn advance_slot_materializes_the_object_row_for_a_key_with_no_content() {
    let store = RedbMetadata::in_memory().unwrap();
    let bucket = bucket::name("fabricated").unwrap();
    let key = object::key("a.txt").unwrap();
    bucket_with(&store, &bucket).await;

    let before = ObjectMeta::get(&store, &bucket, &key).await.unwrap();
    assert!(before.is_none(), "no slot, no row");

    store.advance_slot(&bucket, &key, None, 7).await.unwrap();

    let row = ObjectMeta::get(&store, &bucket, &key).await.unwrap();
    let row = row.expect("the slot's row is materialized");
    assert_eq!((row.etag, row.size), (ETag::EMPTY, 0));
    let recorded = raw_row(&store.driver(), &bucket, &key);
    assert_eq!(recorded.version_seq, 7);
}

/// The slot writer probes the bucket row in its own transaction, exactly as
/// `put_guarded` and `set_bucket_state_guarded` do: a concurrent
/// `remove_if_empty` cannot be raced into having the advance materialize an
/// object row (and its slot columns) for a bucket that is gone.
#[tokio::test]
async fn advance_slot_refuses_a_bucket_with_no_row() {
    let store = RedbMetadata::in_memory().unwrap();
    let bucket = bucket::name("slot-refusal").unwrap();
    let key = object::key("a.txt").unwrap();

    let refusal: storage::Error = store
        .advance_slot(&bucket, &key, Some(Id::new("v1").unwrap()), 1)
        .await
        .unwrap_err()
        .into();
    assert!(
        matches!(refusal, storage::Error::NoSuchBucket(_)),
        "{refusal:?}"
    );
    assert!(
        ObjectMeta::get(&store, &bucket, &key)
            .await
            .unwrap()
            .is_none(),
        "a refused advance writes no object row"
    );
    assert!(
        store.get_version(&bucket, &key, 1).await.unwrap().is_none(),
        "and no version row"
    );

    bucket_with(&store, &bucket).await;
    store
        .advance_slot(&bucket, &key, Some(Id::new("v1").unwrap()), 1)
        .await
        .unwrap();
    let recorded = raw_row(&store.driver(), &bucket, &key);
    assert_eq!(
        recorded.version_id.as_ref().map(|id| id.as_str()),
        Some("v1")
    );
    assert_eq!(recorded.version_seq, 1);
}

/// The restore's commit, end to end: the slot's **backwards real-identity
/// move** lands — the one move `advance_slot` refuses — and every fact of
/// the commit rides the one transaction: the object row the caller hands in
/// and its slot columns, the cleared retained part list, the consumed row
/// with its index entry, and the stamp the move owes the row it moves off.
#[tokio::test]
async fn restore_slot_lands_the_backwards_move_with_every_fact_of_the_commit() {
    let store = RedbMetadata::in_memory().unwrap();
    let bucket = bucket::name("restore-object").unwrap();
    let key = object::key("a.txt").unwrap();
    bucket_with(&store, &bucket).await;
    let v1 = Id::new("v1").unwrap();
    let v3 = Id::new("v3").unwrap();
    // The successor's own row at the lower sequence and the removed
    // current version's row above it, both with their index entries (the
    // writer commits the two together, which is the state a restore
    // consumes).
    store
        .put_version(&bucket, &key, &version_row_of(&stamped_row(1, "v1", 0)))
        .await
        .unwrap();
    store
        .put_version(&bucket, &key, &version_row_of(&stamped_row(3, "v3", 0)))
        .await
        .unwrap();
    assert_eq!(
        store
            .version_seq_of(&bucket, &key, Some(&v1))
            .await
            .unwrap(),
        Some(1),
        "the successor's index entry is planted"
    );
    // The object the removed version leaves behind: a retained part list
    // (a completed multipart object) and the slot at the removed row.
    ObjectMeta::put_with_parts(
        &store,
        &bucket,
        &key,
        &caller_row(9, 7),
        &[part_row(1), part_row(2)],
    )
    .await
    .unwrap();
    store
        .advance_slot(&bucket, &key, Some(v3.clone()), 3)
        .await
        .unwrap();
    assert_eq!(store.parts(&bucket, &key).await.unwrap().len(), 2);

    let restored = caller_row(5, 42);
    store
        .restore_slot(&bucket, &key, &restored, Some(v1.clone()), 1)
        .await
        .unwrap();

    // S3: the object row is the caller's elements wholesale…
    assert_eq!(
        ObjectMeta::get(&store, &bucket, &key).await.unwrap(),
        Some(restored.clone())
    );
    // …the recorded slot columns name the restored version…
    let recorded = raw_row(&store.driver(), &bucket, &key);
    assert_eq!(
        recorded.version_id.as_ref().map(|id| id.as_str()),
        Some("v1")
    );
    assert_eq!(
        recorded.version_seq, 1,
        "the slot moved backwards, to the restored version's own sequence"
    );
    // …and the contract's read answers the same pair (S6's move landed).
    assert_eq!(
        store.current_slot(&bucket, &key).await.unwrap(),
        Some(Current {
            exists: true,
            version: Some(v1.clone()),
            is_delete_marker: false,
        })
    );
    // S3: the retained part list is cleared — a restore is a fresh object.
    assert!(
        store.parts(&bucket, &key).await.unwrap().is_empty(),
        "the restored row carries no retained parts"
    );
    // S4: the consumed row and its index entry are gone, in this commit.
    assert!(
        store.get_version(&bucket, &key, 1).await.unwrap().is_none(),
        "the restored version is current, so it has no history row"
    );
    assert_eq!(
        store
            .version_seq_of(&bucket, &key, Some(&v1))
            .await
            .unwrap(),
        None,
        "…and no index entry either"
    );
    assert_eq!(
        store
            .version_seq_of(&bucket, &key, Some(&v3))
            .await
            .unwrap(),
        Some(3),
        "the index entry of another version is untouched"
    );
    // S5: the row the slot moved off stopped being current, in the same
    // commit.
    assert_ne!(
        stamp_of(&store, &bucket, &key, 3).await,
        0,
        "the row the slot moved off is stamped history"
    );
}

/// S2/S7's marker arm: the marker bit is the **consumed row's** kind, never
/// the caller's, and it survives the row it came from — after the call
/// `current_slot` still answers a marker, with the row gone.
#[tokio::test]
async fn restore_slot_restores_a_delete_marker_whose_bit_survives_its_row() {
    let store = RedbMetadata::in_memory().unwrap();
    let bucket = bucket::name("restore-marker").unwrap();
    let key = object::key("a.txt").unwrap();
    bucket_with(&store, &bucket).await;
    let v1 = Id::new("v1").unwrap();
    store
        .put_version(&bucket, &key, &version_row_of(&marker_version_row(1, "v1")))
        .await
        .unwrap();
    store
        .put_version(&bucket, &key, &version_row_of(&stamped_row(3, "v3", 0)))
        .await
        .unwrap();
    store
        .advance_slot(&bucket, &key, Some(Id::new("v3").unwrap()), 3)
        .await
        .unwrap();

    // The caller carries a marker-shaped row (empty content) and no kind:
    // the kind is the version row's own fact.
    store
        .restore_slot(&bucket, &key, &caller_row(0, 0), Some(v1.clone()), 1)
        .await
        .unwrap();

    let slot = store
        .current_slot(&bucket, &key)
        .await
        .unwrap()
        .expect("a recorded slot");
    assert_eq!(slot.version.as_ref().map(|id| id.as_str()), Some("v1"));
    assert!(
        slot.is_delete_marker,
        "the restored marker's bit survives the row it came from"
    );
    let recorded = raw_row(&store.driver(), &bucket, &key);
    assert!(
        recorded.is_delete_marker,
        "…because the written row records the consumed row's marker kind"
    );
    assert!(
        store.get_version(&bucket, &key, 1).await.unwrap().is_none(),
        "the consumed marker row is gone"
    );
}

/// R2's two arms: a sequence holding no row and a row naming another
/// identity are both refused, and the refusal commits neither half — the
/// version rows, the slot and the object row are exactly as they were.
#[tokio::test]
async fn restore_slot_refuses_an_absent_row_and_an_identity_mismatch() {
    let store = RedbMetadata::in_memory().unwrap();
    let bucket = bucket::name("restore-refusal").unwrap();
    let key = object::key("a.txt").unwrap();
    bucket_with(&store, &bucket).await;
    let v1 = Id::new("v1").unwrap();
    let v2 = Id::new("v2").unwrap();
    store
        .put_version(&bucket, &key, &version_row_of(&stamped_row(1, "v1", 0)))
        .await
        .unwrap();
    store
        .put_version(&bucket, &key, &version_row_of(&stamped_row(2, "v2", 0)))
        .await
        .unwrap();
    store
        .advance_slot(&bucket, &key, Some(v2.clone()), 2)
        .await
        .unwrap();
    let before = ObjectMeta::get(&store, &bucket, &key).await.unwrap();

    // No version row at the sequence the caller names: a bug or a lost
    // race, never a slot naming a version the store does not hold.
    let absent = store
        .restore_slot(&bucket, &key, &caller_row(5, 42), Some(v1.clone()), 9)
        .await;
    assert!(matches!(absent, Err(Error::Refused(_))), "{absent:?}");
    // The row at that sequence exists, but it names another version.
    let mismatched = store
        .restore_slot(&bucket, &key, &caller_row(5, 42), Some(v1.clone()), 2)
        .await;
    assert!(
        matches!(mismatched, Err(Error::Refused(_))),
        "{mismatched:?}"
    );

    // Neither refusal committed anything: both rows are intact, the slot
    // still names the current version, and no object row was written.
    assert_eq!(
        store
            .get_version(&bucket, &key, 1)
            .await
            .unwrap()
            .map(|row| row.id),
        Some(Some(v1.clone())),
        "the row an absent-row refusal named is untouched"
    );
    assert_eq!(
        store
            .get_version(&bucket, &key, 2)
            .await
            .unwrap()
            .map(|row| row.id),
        Some(Some(v2.clone())),
        "the row an identity mismatch named is untouched"
    );
    assert_eq!(
        store
            .version_seq_of(&bucket, &key, Some(&v1))
            .await
            .unwrap(),
        Some(1),
        "…and its index entry"
    );
    let slot = store
        .current_slot(&bucket, &key)
        .await
        .unwrap()
        .expect("a recorded slot");
    assert_eq!(slot.version.as_ref().map(|id| id.as_str()), Some("v2"));
    assert_eq!(
        ObjectMeta::get(&store, &bucket, &key).await.unwrap(),
        before,
        "a refused restore writes no object row"
    );
}

/// The in-place re-run: a restore at the sequence the slot already names
/// lands, and moves off nothing — the same-sequence exemption S5 shares
/// with `advance_slot`, so no other row of the key is stamped either.
#[tokio::test]
async fn restore_slot_lands_in_place_and_stamps_nothing() {
    let store = RedbMetadata::in_memory().unwrap();
    let bucket = bucket::name("restore-in-place").unwrap();
    let key = object::key("a.txt").unwrap();
    bucket_with(&store, &bucket).await;
    let v2 = Id::new("v2").unwrap();
    store
        .put_version(&bucket, &key, &version_row_of(&stamped_row(1, "v1", 0)))
        .await
        .unwrap();
    store
        .put_version(&bucket, &key, &version_row_of(&stamped_row(2, "v2", 0)))
        .await
        .unwrap();
    store
        .advance_slot(&bucket, &key, Some(v2.clone()), 2)
        .await
        .unwrap();

    store
        .restore_slot(&bucket, &key, &caller_row(5, 42), Some(v2.clone()), 2)
        .await
        .unwrap();

    assert!(
        store.get_version(&bucket, &key, 2).await.unwrap().is_none(),
        "the row is consumed like any other restore"
    );
    assert!(
        store.get_version(&bucket, &key, 1).await.unwrap().is_some(),
        "the untouched older row stays"
    );
    assert_eq!(
        stamp_of(&store, &bucket, &key, 1).await,
        0,
        "a move at the slot's own sequence stamps no other row"
    );
    let slot = store
        .current_slot(&bucket, &key)
        .await
        .unwrap()
        .expect("a recorded slot");
    assert_eq!(slot.version.as_ref().map(|id| id.as_str()), Some("v2"));
    let recorded = raw_row(&store.driver(), &bucket, &key);
    assert_eq!(recorded.version_seq, 2, "the slot stayed where it was");
}

/// A rewrite of the same version keeps the **stored** stamp and ignores the
/// caller's; a free sequence stores the caller's stamp. This is the stamp
/// source the engine no longer reads for itself.
#[tokio::test]
async fn put_version_preserves_the_stored_stamp_over_the_callers() {
    let store = RedbMetadata::in_memory().unwrap();
    let bucket = bucket::name("stamp").unwrap();
    let key = object::key("a.txt").unwrap();
    bucket_with(&store, &bucket).await;
    let (name, stored_key) = (bucket.to_string(), key.to_string());

    // The row already stored at sequence 4, stamped noncurrent.
    let planted = VersionRow {
        id: "v1".to_owned(),
        blob: encode_blob(&BlobRef::single(BlockHash::of(b"x"), 1)),
        size: 1,
        etag: String::new(),
        created: 0,
        became_noncurrent: 777,
        kind: RowKind::Object,
        tags: String::new(),
        checksum: String::new(),
        owner: String::new(),
        acl: String::new(),
        seq: 4,
    };
    let driver = store.driver();
    {
        let planted = planted.clone();
        driver
            .write(move |txn: &mut WriteTransaction| -> Result<(), Error> {
                VersionTable::open(txn)?.put(&name, &stored_key, &planted)
            })
            .await
            .unwrap();
    }

    // The caller's row names the same version but carries a different
    // stamp: the stored one must win.
    let mut caller = super::contract_row(&planted).expect("a decodable row");
    caller.became_noncurrent = 999;
    store.put_version(&bucket, &key, &caller).await.unwrap();
    let stored = store.get_version(&bucket, &key, 4).await.unwrap();
    let stored = stored.expect("the rewritten row");
    assert_eq!(stored.became_noncurrent, 777);

    // A sequence with no row stores the caller's stamp.
    caller.seq = 5;
    caller.became_noncurrent = 42;
    store.put_version(&bucket, &key, &caller).await.unwrap();
    let fresh = store.get_version(&bucket, &key, 5).await.unwrap();
    let fresh = fresh.expect("the new row");
    assert_eq!(fresh.became_noncurrent, 42);
}

/// The version-row write probes the bucket row in its own transaction,
/// exactly as the object-row guarded writers do: a version row is not less
/// of a row than the object row beside it.
#[tokio::test]
async fn put_version_refuses_a_bucket_with_no_row() {
    let store = RedbMetadata::in_memory().unwrap();
    let bucket = bucket::name("version-refusal").unwrap();
    let key = object::key("a.txt").unwrap();
    let planted = VersionRow {
        id: "v1".to_owned(),
        blob: encode_blob(&BlobRef::single(BlockHash::of(b"x"), 1)),
        size: 1,
        etag: String::new(),
        created: 0,
        became_noncurrent: 0,
        kind: RowKind::Object,
        tags: String::new(),
        checksum: String::new(),
        owner: String::new(),
        acl: String::new(),
        seq: 1,
    };
    let row = super::contract_row(&planted).expect("a decodable row");

    let refusal: storage::Error = store
        .put_version(&bucket, &key, &row)
        .await
        .unwrap_err()
        .into();
    assert!(
        matches!(refusal, storage::Error::NoSuchBucket(_)),
        "{refusal:?}"
    );
    assert!(
        store.get_version(&bucket, &key, 1).await.unwrap().is_none(),
        "a refused version write leaves no row"
    );

    bucket_with(&store, &bucket).await;
    store.put_version(&bucket, &key, &row).await.unwrap();
    assert!(
        store.get_version(&bucket, &key, 1).await.unwrap().is_some(),
        "the version row lands once the bucket row exists"
    );
}

/// One table-layer object version row for the stamp legs: one byte of
/// content, `became_noncurrent` as given.
fn stamped_row(seq: u64, id: &str, became_noncurrent: u64) -> VersionRow {
    VersionRow {
        id: id.to_owned(),
        blob: encode_blob(&BlobRef::single(BlockHash::of(b"stamp"), 1)),
        size: 1,
        etag: String::new(),
        created: 0,
        became_noncurrent,
        kind: RowKind::Object,
        tags: String::new(),
        checksum: String::new(),
        owner: String::new(),
        acl: String::new(),
        seq,
    }
}

/// One table-layer delete-marker row for the restore legs: no content, the
/// empty blob, no ETag — the shape a marker's row has.
fn marker_version_row(seq: u64, id: &str) -> VersionRow {
    VersionRow {
        id: id.to_owned(),
        blob: encode_blob(&BlobRef::empty()),
        size: 0,
        etag: String::new(),
        created: 0,
        became_noncurrent: 0,
        kind: RowKind::Marker,
        tags: String::new(),
        checksum: String::new(),
        owner: String::new(),
        acl: String::new(),
        seq,
    }
}

/// The contract row a table-layer row decodes to — the shape
/// [`VersionMeta::put_version`] takes, so the planted row carries its index
/// entry (the state a restore consumes).
fn version_row_of(row: &VersionRow) -> Stored {
    super::contract_row(row).expect("a decodable version row")
}

/// The caller's object row for a restore: distinctive elements, so a leg
/// can tell the row the call wrote from anything the store fabricated.
fn caller_row(size: u64, identity: u64) -> CallerRow {
    CallerRow {
        etag: "0cc175b9c0f1b6a831c399e269772661".parse::<ETag>().unwrap(),
        size,
        mtime: SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
        identity,
        tags: object::Tags::empty(),
        checksum: None,
        owner: None,
        acl: Acl::default_private(None),
    }
}

/// One retained part of a completed multipart object.
fn part_row(number: u32) -> PartRow {
    PartRow {
        number,
        size: 5,
        checksum: None,
    }
}

/// Plant version rows through the table layer: the contract has no
/// unguarded row writer, so only the store's own suite can stage a row that
/// the slot has not moved off yet.
async fn plant(
    store: &RedbMetadata,
    bucket: &bucket::Name,
    key: &object::Key,
    rows: &[VersionRow],
) {
    let (name, stored_key) = (bucket.to_string(), key.to_string());
    let rows = rows.to_vec();
    store
        .driver()
        .write(move |txn: &mut WriteTransaction| -> Result<(), Error> {
            let mut table = VersionTable::open(txn)?;
            for row in &rows {
                table.put(&name, &stored_key, row)?;
            }
            Ok(())
        })
        .await
        .unwrap();
}

/// The stamp a version row carries.
async fn stamp_of(store: &RedbMetadata, bucket: &bucket::Name, key: &object::Key, seq: u64) -> u64 {
    store
        .get_version(bucket, key, seq)
        .await
        .unwrap()
        .expect("the version row stays")
        .became_noncurrent
}

/// The `object_meta` row as the table layer sees it.
fn raw_row(driver: &Arc<Driver>, bucket: &bucket::Name, key: &object::Key) -> ObjectStored {
    let (bucket, key) = (bucket.to_string(), key.to_string());
    driver
        .read(|txn| ObjectTable::open_readonly(txn)?.get(&bucket, &key))
        .unwrap()
        .expect("the slot's object row")
}

/// The three slot/version commits open one info span each on the storage
/// target: one span per contract call, not per row, and the identity the
/// commit moves is the recorded field.
#[tokio::test]
async fn version_commits_open_one_info_span_each() {
    let capture = capture_spans();
    let store = RedbMetadata::in_memory().unwrap();
    let bucket = bucket::name("span-version").unwrap();
    let key = object::key("slot.txt").unwrap();
    bucket_with(&store, &bucket).await;
    plant(&store, &bucket, &key, &[marker_version_row(1, "v1")]).await;

    let watermark = capture.watermark();
    store
        .advance_slot(&bucket, &key, Some(Id::new("v1").unwrap()), 1)
        .await
        .unwrap();
    store
        .restore_slot(
            &bucket,
            &key,
            &caller_row(5, 7),
            Some(Id::new("v1").unwrap()),
            1,
        )
        .await
        .unwrap();
    store
        .put_version(&bucket, &key, &version_row_of(&stamped_row(2, "v2", 0)))
        .await
        .unwrap();

    let spans = capture.spans_since(watermark);
    let one = |name: &str| one_named(&spans, name);

    let advance = one("advance_slot");
    assert_eq!(advance.target, "tinio::storage");
    assert_eq!(advance.level, Level::INFO);
    assert_eq!(advance.field("bucket"), Some("span-version"));
    assert_eq!(advance.field("key"), Some("slot.txt"));
    assert_eq!(advance.field("version"), Some("v1"));

    let restore = one("restore_slot");
    assert_eq!(restore.target, "tinio::storage");
    assert_eq!(restore.level, Level::INFO);
    assert_eq!(restore.field("version"), Some("v1"));
    assert_eq!(restore.field("bytes"), Some("5"));

    let put = one("put_version");
    assert_eq!(put.target, "tinio::storage");
    assert_eq!(put.level, Level::INFO);
    assert_eq!(put.field("version"), Some("v2"));
    assert_eq!(put.field("kind"), Some("object"));
    assert_eq!(put.field("bytes"), Some("1"));
}

// The Object Lock legs only this store can reach: the raw rows a decoder
// must refuse, the marker identity R19 excludes, the protected in-place
// overwrite, a root written before the three tables existed (OL5, OL7,
// OL8, OL21), and the R21 guarded versioning write.

/// An enabled configuration with no default rule.
fn lock_config() -> object_lock::Config {
    object_lock::Config {
        enabled: true,
        rule: None,
    }
}

/// R21: the guarded versioning write refuses `Suspended` while an enabled
/// Object Lock configuration exists, and leaves the state unchanged; with
/// no enabled configuration the suspend lands.
#[tokio::test]
async fn the_guarded_state_write_refuses_a_suspend_over_an_enabled_lock_config() {
    let store = RedbMetadata::in_memory().unwrap();
    let bucket = bucket::name("guarded-suspend-lock").unwrap();
    bucket_with(&store, &bucket).await;
    VersionMeta::set_bucket_state(&store, &bucket, State::Enabled)
        .await
        .unwrap();
    BucketMeta::set_object_lock(&store, &bucket, &lock_config())
        .await
        .unwrap();
    let refused = VersionMeta::set_bucket_state_guarded(&store, &bucket, State::Suspended)
        .await
        .unwrap_err();
    assert!(
        matches!(
            storage::Error::from(refused),
            storage::Error::InvalidBucketState(_)
        ),
        "an enabled configuration refuses the suspend"
    );
    assert_eq!(
        VersionMeta::bucket_state(&store, &bucket).await.unwrap(),
        State::Enabled,
        "the refused suspend changed nothing"
    );
    // A disabled configuration does not refuse it.
    BucketMeta::set_object_lock(
        &store,
        &bucket,
        &object_lock::Config {
            enabled: false,
            rule: None,
        },
    )
    .await
    .unwrap();
    VersionMeta::set_bucket_state_guarded(&store, &bucket, State::Suspended)
        .await
        .unwrap();
    assert_eq!(
        VersionMeta::bucket_state(&store, &bucket).await.unwrap(),
        State::Suspended
    );
}

/// The bucket, configuration and versioning state a lock write requires.
async fn lockable(store: &RedbMetadata, bucket: &bucket::Name) {
    bucket_with(store, bucket).await;
    store
        .set_bucket_state(bucket, State::Enabled)
        .await
        .unwrap();
    store.set_object_lock(bucket, &lock_config()).await.unwrap();
}

/// Plant a lock row through the raw table — the corrupt and partial shapes
/// no contract write produces.
async fn plant_lock(
    store: &RedbMetadata,
    bucket: &bucket::Name,
    key: &object::Key,
    id: &str,
    columns: (Option<&'static str>, Option<i64>, Option<&'static str>),
) {
    let (b, k, id) = (bucket.to_string(), key.to_string(), id.to_owned());
    store
        .driver()
        .write(move |txn| -> Result<(), Error> {
            let mut table = LockTable::open(txn)?;
            table
                .0
                .insert((b.as_str(), k.as_str(), id.as_str()), columns)
                .map_err(Error::from)?;
            Ok(())
        })
        .await
        .unwrap();
}

/// Whether the raw table still holds the identity's lock row.
fn raw_lock_present(
    store: &RedbMetadata,
    bucket: &bucket::Name,
    key: &object::Key,
    id: &str,
) -> bool {
    let (b, k, id) = (bucket.to_string(), key.to_string(), id.to_owned());
    store
        .driver()
        .read(move |txn| -> Result<bool, Error> {
            // The raw row, not the decoding read: a planted corrupt row
            // is what this probe asks about.
            Ok(LockTable::open_readonly(txn)?
                .0
                .get((b.as_str(), k.as_str(), id.as_str()))?
                .is_some())
        })
        .unwrap()
}

/// A corrupt mode, a partial pair and a missing hold each fail **closed**
/// on every lock read and on the delete path, and a refusal removes
/// nothing (OL5, OL10).
#[tokio::test]
async fn a_corrupt_lock_row_fails_closed_on_the_reads_and_the_delete() {
    let store = RedbMetadata::in_memory().unwrap();
    let bucket = bucket::name("corrupt-lock").unwrap();
    let key = object::key("a.txt").unwrap();
    lockable(&store, &bucket).await;
    store
        .put_version(&bucket, &key, &version_row_of(&stamped_row(1, "v1", 0)))
        .await
        .unwrap();
    store
        .advance_slot(&bucket, &key, Some(Id::new("v1").unwrap()), 1)
        .await
        .unwrap();

    for (label, columns) in [
        ("mode", (Some("NOT-A-MODE"), Some(1), Some("OFF"))),
        ("partial", (Some("GOVERNANCE"), None, Some("OFF"))),
        ("hold", (Some("GOVERNANCE"), Some(1), None)),
    ] {
        plant_lock(&store, &bucket, &key, "v1", columns).await;
        for refused in [
            VersionMeta::version_lock(&store, &bucket, &key, Some(&Id::new("v1").unwrap()))
                .await
                .map(|lock| lock.map(|_| ())),
            VersionMeta::get_object_retention(&store, &bucket, &key, Some(&Id::new("v1").unwrap()))
                .await
                .map(|retention| retention.map(|_| ())),
        ] {
            assert!(
                matches!(
                    storage::Error::from(refused.unwrap_err()),
                    storage::Error::Corrupt(_)
                ),
                "the {label} row must fail closed on the read path"
            );
        }
        let refused = store
            .expire_with(&bucket, &key, 1, object_lock::Bypass::Governance)
            .await
            .unwrap_err();
        assert!(
            matches!(storage::Error::from(refused), storage::Error::Corrupt(_)),
            "the {label} row must fail closed on the delete path"
        );
        assert!(
            store.get_version(&bucket, &key, 1).await.unwrap().is_some(),
            "the refused delete removed nothing"
        );
        assert!(
            raw_lock_present(&store, &bucket, &key, "v1"),
            "the refused delete kept the unreadable row"
        );
    }
}

/// R19: a delete-marker identity never observes a lock row (its own delete
/// proceeds without a bypass) and never removes one, and a marker's slot
/// advance never writes one.
#[tokio::test]
async fn a_delete_marker_never_observes_or_removes_a_lock_row() {
    let store = RedbMetadata::in_memory().unwrap();
    let bucket = bucket::name("marker-lock").unwrap();
    let key = object::key("a.txt").unwrap();
    lockable(&store, &bucket).await;
    store
        .put_version(&bucket, &key, &version_row_of(&marker_version_row(2, "m1")))
        .await
        .unwrap();
    // A row the request path can never write, planted under the marker's
    // identity: the delete must not read it, and must not remove it.
    plant_lock(
        &store,
        &bucket,
        &key,
        "m1",
        (Some("COMPLIANCE"), Some(i64::MAX), Some("ON")),
    )
    .await;

    store
        .expire_with(&bucket, &key, 2, object_lock::Bypass::None)
        .await
        .unwrap();
    assert!(
        store.get_version(&bucket, &key, 2).await.unwrap().is_none(),
        "a marker deletes without the bypass the row would have needed"
    );
    assert!(
        raw_lock_present(&store, &bucket, &key, "m1"),
        "R19: the marker's delete never removes a lock row"
    );

    // A slot advance onto a marker with a requested lock writes no row
    // either (markers never get one).
    store
        .put_version(&bucket, &key, &version_row_of(&marker_version_row(3, "m2")))
        .await
        .unwrap();
    VersionMeta::advance_slot_with(
        &store,
        &bucket,
        &key,
        Some(Id::new("m2").unwrap()),
        3,
        Some(&object_lock::Lock {
            retention: Some(reservation(
                object_lock::Mode::Governance,
                Duration::from_secs(3_600),
            )),
            legal_hold: object_lock::LegalHold::Off,
        }),
    )
    .await
    .unwrap();
    assert!(
        !raw_lock_present(&store, &bucket, &key, "m2"),
        "a marker never gets a lock row"
    );
}

/// A sequence listed twice is one version: the batch judges, counts and
/// reports it once (first occurrence), the same rule the pg batch applies.
#[tokio::test]
async fn expire_many_report_reports_a_repeated_protected_sequence_once() {
    let store = RedbMetadata::in_memory().unwrap();
    let bucket = bucket::name("expire-repeat").unwrap();
    let key = object::key("repeat.txt").unwrap();
    lockable(&store, &bucket).await;
    let until = Some(
        i64::try_from(to_nanos(
            reservation(object_lock::Mode::Governance, Duration::from_secs(3_600)).retain_until,
        ))
        .unwrap(),
    );
    for (seq, id) in [(1_u64, "v1"), (2, "v2")] {
        store
            .put_version(&bucket, &key, &version_row_of(&stamped_row(seq, id, 0)))
            .await
            .unwrap();
        plant_lock(
            &store,
            &bucket,
            &key,
            id,
            (Some("GOVERNANCE"), until, Some("OFF")),
        )
        .await;
    }

    let report = VersionMeta::expire_many_report(&store, &bucket, &key, &[1, 1, 2, 2])
        .await
        .unwrap();
    assert_eq!(report.removed, 0, "both protected versions stay");
    assert_eq!(
        report.protected,
        vec![
            (1, object_lock::RefusalReason::Governance),
            (2, object_lock::RefusalReason::Governance),
        ],
        "each sequence is judged and reported once, in first-occurrence order"
    );
}

/// OL7: a lock commits with the slot advance, a `None` advance never
/// clears it, and a null in-place replacement of a protected identity is
/// refused.
#[tokio::test]
async fn the_slot_advance_commits_a_lock_and_refuses_a_protected_in_place_write() {
    let store = RedbMetadata::in_memory().unwrap();
    let bucket = bucket::name("advance-lock").unwrap();
    let key = object::key("a.txt").unwrap();
    lockable(&store, &bucket).await;

    let lock = object_lock::Lock {
        retention: Some(reservation(
            object_lock::Mode::Governance,
            Duration::from_secs(3_600),
        )),
        legal_hold: object_lock::LegalHold::Off,
    };
    store
        .put_version(&bucket, &key, &version_row_of(&stamped_row(1, "v1", 0)))
        .await
        .unwrap();
    store
        .advance_slot_with(&bucket, &key, Some(Id::new("v1").unwrap()), 1, Some(&lock))
        .await
        .unwrap();
    assert_eq!(
        store
            .version_lock(&bucket, &key, Some(&Id::new("v1").unwrap()))
            .await
            .unwrap(),
        Some(lock.clone()),
        "the lock commits with the current version"
    );

    // A later ordinary advance with no lock leaves it intact.
    store
        .put_version(&bucket, &key, &version_row_of(&stamped_row(2, "v2", 0)))
        .await
        .unwrap();
    store
        .advance_slot_with(&bucket, &key, Some(Id::new("v2").unwrap()), 2, None)
        .await
        .unwrap();
    assert_eq!(
        store
            .version_lock(&bucket, &key, Some(&Id::new("v1").unwrap()))
            .await
            .unwrap(),
        Some(lock),
        "advance_slot_with(None) never clears a lock row"
    );

    // The protected in-place replacement: the null identity replaces the
    // current null version, which owns an active hold.
    store.advance_slot(&bucket, &key, None, 0).await.unwrap();
    store
        .set_legal_hold(&bucket, &key, None, object_lock::LegalHold::On)
        .await
        .unwrap();
    let refused = store
        .advance_slot_with(&bucket, &key, None, 0, None)
        .await
        .unwrap_err();
    assert!(
        matches!(
            storage::Error::from(refused),
            storage::Error::Locked(object_lock::RefusalReason::LegalHold)
        ),
        "a protected in-place overwrite is refused"
    );
    // A new identity is an ordinary overwrite: it lands, and the null
    // identity keeps its lock.
    store
        .advance_slot_with(&bucket, &key, Some(Id::new("v3").unwrap()), 3, None)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_object_legal_hold(&bucket, &key, None)
            .await
            .unwrap(),
        object_lock::LegalHold::On,
        "the replaced identity's lock rides untouched"
    );
}

/// The combined lock update is one transaction: a refused retention leaves
/// the legal hold member unapplied and the stored row unchanged (OL23), while
/// an accepted update applies both members at once.
#[tokio::test]
async fn a_refused_retention_leaves_the_legal_hold_unapplied() {
    let store = RedbMetadata::in_memory().unwrap();
    let bucket = bucket::name("atomic-lock").unwrap();
    let key = object::key("atomic.txt").unwrap();
    lockable(&store, &bucket).await;
    let id = Id::new("v1").unwrap();
    store
        .put_version(&bucket, &key, &version_row_of(&stamped_row(1, "v1", 0)))
        .await
        .unwrap();
    store
        .advance_slot(&bucket, &key, Some(id.clone()), 1)
        .await
        .unwrap();
    let now = SystemTime::now();
    let compliance = reservation(object_lock::Mode::Compliance, Duration::from_secs(3_600));
    store
        .set_lock(
            &bucket,
            &key,
            Some(&id),
            Some(Some(&compliance)),
            None,
            object_lock::Bypass::None,
            now,
        )
        .await
        .unwrap();
    let before = store
        .version_lock(&bucket, &key, Some(&id))
        .await
        .unwrap()
        .expect("the lock row");
    assert_eq!(before.legal_hold, object_lock::LegalHold::Off);

    // A compliance shortening is refused even with the governance bypass; the
    // legal-hold member present in the same call never lands.
    let shorter = reservation(object_lock::Mode::Compliance, Duration::from_secs(1_800));
    let refused = store
        .set_lock(
            &bucket,
            &key,
            Some(&id),
            Some(Some(&shorter)),
            Some(object_lock::LegalHold::On),
            object_lock::Bypass::Governance,
            now,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            storage::Error::from(refused),
            storage::Error::Locked(object_lock::RefusalReason::Compliance)
        ),
        "the compliance retention refuses the update"
    );
    assert_eq!(
        store.version_lock(&bucket, &key, Some(&id)).await.unwrap(),
        Some(before),
        "the refused update wrote neither member"
    );
    assert_eq!(
        store
            .get_object_legal_hold(&bucket, &key, Some(&id))
            .await
            .unwrap(),
        object_lock::LegalHold::Off,
        "the hold was not applied"
    );

    // An accepted combined update applies the extended retention and the hold.
    let later = reservation(object_lock::Mode::Compliance, Duration::from_secs(7_200));
    let updated = store
        .set_lock(
            &bucket,
            &key,
            Some(&id),
            Some(Some(&later)),
            Some(object_lock::LegalHold::On),
            object_lock::Bypass::None,
            now,
        )
        .await
        .unwrap()
        .expect("both members are facts");
    assert_eq!(updated.retention, Some(later));
    assert_eq!(updated.legal_hold, object_lock::LegalHold::On);
    assert_eq!(
        store.version_lock(&bucket, &key, Some(&id)).await.unwrap(),
        Some(updated),
        "the one transaction stored both members"
    );
}

/// The read-only pre-feature root: a root written before the three tables
/// existed serves no locks through the read-only handles, and the writable
/// open (`ensure_all`) creates them (OL4).
#[tokio::test]
async fn a_read_only_pre_feature_root_serves_no_locks() {
    let dir = tempfile::tempdir().unwrap();
    {
        // The state this build's read-only gate accepts, without the
        // shared schema's tables at all — the shape a pre-feature root
        // has once the lock tables are newer than it.
        let db = redb::Database::create(dir.path().join(META_DB_FILE)).unwrap();
        let mut txn = db.begin_write().unwrap();
        state::Table::open(&mut txn)
            .unwrap()
            .write_version(state::FORMAT_VERSION)
            .unwrap();
        txn.commit().unwrap();
    }
    let read_only = open::open_read_only(dir.path()).unwrap();
    {
        let txn = read_only.begin_read().unwrap();
        assert_eq!(
            bucket_object_lock::Table::open_readonly(&txn)
                .unwrap()
                .get("b")
                .unwrap(),
            None
        );
        assert_eq!(
            version_object_lock::Table::open_readonly(&txn)
                .unwrap()
                .get("b", "k", "v1")
                .unwrap(),
            None
        );
        assert_eq!(
            upload_object_lock::Table::open_readonly(&txn)
                .unwrap()
                .get("b", "u1")
                .unwrap(),
            None
        );
    }
    drop(read_only);

    // The writable open ensures the tables and the contract serves no
    // locks for a root that stored none.
    let store = RedbMetadata::open(dir.path()).unwrap();
    let bucket = bucket::name("bkt").unwrap();
    bucket_with(&store, &bucket).await;
    assert_eq!(store.object_lock(&bucket).await.unwrap(), None);
    let key = object::key("k").unwrap();
    assert_eq!(
        store
            .version_lock(&bucket, &key, Some(&Id::new("v1").unwrap()))
            .await
            .unwrap(),
        None
    );
    assert_eq!(store.upload_lock(&bucket, "u1").await.unwrap(), None);
}
