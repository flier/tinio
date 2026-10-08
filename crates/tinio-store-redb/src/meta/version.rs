//! `VersionMeta` over the three version tables — Task 8's implementation.
//!
//! The row half is [`crate::version`]'s `(bucket, key, seq)` table and
//! [`crate::version_index`]'s identity index; every write and removal pairs
//! the two in **one** transaction (an index entry is written with its row and
//! retired with it — `remove_pointing_at` — so the two can never disagree).
//! The bucket half is [`crate::bucket_versioning`]'s one row per bucket; a
//! missing row is [`State::Unversioned`].
//!
//! The **current slot** lives in the `object_meta` row's three slot columns
//! ([`crate::object_meta::Stored`]'s `version_id`, `version_seq`,
//! `is_delete_marker`) — the landed table the scanner's orphan rule reads.
//! [`VersionMeta::advance_slot`] is its only contract writer: it records the
//! identity and the sequence, and derives the marker bit from the version row
//! at that sequence in the same transaction, so the recorded flag and the
//! contract's rule (the **version row's kind** decides the marker) cannot
//! disagree for a slot the contract wrote. [`VersionMeta::current_slot`]
//! resolves the marker from that row — the contract's own rule — and answers
//! `None` only when the key has no `object_meta` row at all.

use std::io;

use async_trait::async_trait;

use crate::{
    _core::{
        ETag,
        acl::Acl,
        bucket,
        meta::{
            VersionMeta,
            version::{Current, Stored},
        },
        object, storage,
        version::{self as contract, Id, State},
    },
    Error, RedbMetadata,
    bucket::Table as BucketTable,
    bucket_versioning::Table as StateTable,
    object_meta::{Stored as ObjectRow, Table as ObjectTable},
    version::{Kind as RowKind, Row as VersionRow, Table as VersionTable},
    version_index::Table as IndexTable,
};

/// Encode one contract row into the table layer's row: the elements
/// [`Stored::to_raw`] emits are the table's value tuple in order, and the
/// row's sequence is the key's third element.
fn table_row(stored: &Stored) -> VersionRow {
    let (id, blob, size, etag, created, became_noncurrent, kind, tags, checksum, owner, acl) =
        stored.to_raw();
    VersionRow {
        id,
        blob,
        size,
        etag,
        created,
        became_noncurrent,
        kind: kind
            .parse()
            .expect("the contract kind wire names a table kind"),
        tags,
        checksum,
        owner,
        acl,
        seq: stored.seq,
    }
}

/// Decode one table row back into the contract's row — `None` for a row the
/// contract's decoder rejects. The table's own decode already dropped the rows
/// whose blob, kind or identifier it could not read, so this is the contract's
/// second, identical check.
fn contract_row(row: &VersionRow) -> Option<Stored> {
    Stored::from_raw(
        row.seq,
        &row.id,
        &row.blob,
        row.size,
        &row.etag,
        row.created,
        row.became_noncurrent,
        &row.kind.to_string(),
        &row.tags,
        &row.checksum,
        &row.owner,
        &row.acl,
    )
}

/// The `object_meta` row a slot advance materializes for a key that has none:
/// the slot columns are the whole point, so the content-derived elements start
/// empty (the ordinary object writer's `put` replaces the row wholesale). This
/// is the deliberate divergence the contract's `advance_slot` documents — a
/// recorded slot (a delete marker's included) IS object-row state, so a key
/// with no content still answers an object row to `ObjectMeta::get`/`walk`.
fn empty_object_row() -> ObjectRow {
    ObjectRow {
        etag: ETag::EMPTY,
        size: 0,
        mtime: 0,
        file_identity: 0,
        tags: object::Tags::empty(),
        checksum: None,
        owner: None,
        acl: Acl::default_private(None),
        version_id: None,
        version_seq: 0,
        is_delete_marker: false,
    }
}

#[async_trait]
impl VersionMeta for RedbMetadata {
    type Error = Error;

    /// A bucket with no row — or one torn down inside this read's own window —
    /// answers `Unversioned` rather than refusing: the read half of the state
    /// write's TOCTOU window. Harmless, because a read writes nothing; the
    /// write half is [`VersionMeta::set_bucket_state_guarded`].
    async fn bucket_state(&self, bucket: &bucket::Name) -> Result<State, Error> {
        let bucket = bucket.to_string();
        self.driver.read(move |txn| {
            Ok(StateTable::open_readonly(txn)?
                .get(&bucket)?
                .unwrap_or_default())
        })
    }

    /// The unguarded state write — the contract's default. A teardown that
    /// commits between the caller's bucket check and this transaction leaves
    /// its state row behind for a bucket that is gone;
    /// [`VersionMeta::set_bucket_state_guarded`] is the variant that closes
    /// that window.
    async fn set_bucket_state(&self, bucket: &bucket::Name, state: State) -> Result<(), Error> {
        let bucket = bucket.to_string();
        self.driver
            .write(move |txn| StateTable::open(txn)?.set(&bucket, state))
            .await
    }

    /// The guarded state write: the bucket row is read in the transaction that
    /// writes the state row, so a concurrent `remove_if_empty` cannot race it
    /// (D22's refusal projection, the `put_guarded` shape).
    async fn set_bucket_state_guarded(
        &self,
        bucket: &bucket::Name,
        state: State,
    ) -> Result<(), Error> {
        let refusal = storage::no_such_bucket(bucket);
        let bucket = bucket.to_string();
        self.driver
            .write(move |txn| -> Result<(), Error> {
                if !BucketTable::open(txn)?.exists(&bucket)? {
                    return Err(Error::Refused(refusal));
                }
                StateTable::open(txn)?.set(&bucket, state)
            })
            .await
    }

    async fn current_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<Current>, Error> {
        let bucket = bucket.to_string();
        let key = key.to_string();
        self.driver.read(move |txn| {
            // No row: the key has nothing recorded, so it has no slot.
            let Some(row) = ObjectTable::open_readonly(txn)?.get(&bucket, &key)? else {
                return Ok(None);
            };
            // The marker bit is the version row's kind at the slot's own
            // sequence: a row that is gone (an out-of-band expiry) is a
            // recorded slot that is not a marker.
            let marker = VersionTable::open_readonly(txn)?
                .get(&bucket, &key, row.version_seq)?
                .is_some_and(|version| version.kind == RowKind::Marker);
            Ok(Some(Current {
                exists: true,
                version: row.version_id,
                is_delete_marker: marker,
            }))
        })
    }

    async fn advance_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<Id>,
        seq: u64,
    ) -> Result<(), Error> {
        let refusal = storage::no_such_bucket(bucket);
        let bucket = bucket.to_string();
        let key = key.to_string();
        self.driver
            .write(move |txn| {
                // The bucket row is probed in this transaction, first, exactly
                // as the object-row guarded writers probe it: the advance is a
                // write, and a materialized object row is not less of one.
                if !BucketTable::open(txn)?.exists(&bucket)? {
                    return Err(Error::Refused(refusal));
                }
                // The slot is the version-row fact the contract resolves the
                // marker from, so the bit is derived here — in the advancing
                // transaction — never carried by the caller.
                let marker = VersionTable::open(txn)?
                    .get(&bucket, &key, seq)?
                    .is_some_and(|row| row.kind == RowKind::Marker);
                let mut table = ObjectTable::open(txn)?;
                // A key with no `object_meta` row gets one here: the slot
                // columns need a home, and the fabricated row's content-derived
                // elements start empty (see `empty_object_row`). The effect is
                // the deliberate rule the contract documents — an
                // `ObjectMeta::get`/`walk` read then reports a recorded object
                // for a key whose content side holds nothing.
                let mut row = match table.get(&bucket, &key)? {
                    Some(row) => row,
                    None => empty_object_row(),
                };
                // The monotonic guard (2026-10-07 review): a slot never moves
                // backwards. The write transaction serialises commits but does
                // not order them by sequence, so a lower-sequence advance
                // committing after a higher-sequence one would leave the slot
                // below the newest version it already named — the very state the
                // slot exists to prevent (`current_slot` would then resolve the
                // marker bit off the stale row). A move to the **same** sequence
                // is the crash window's own re-run and is allowed; a move to a
                // **lower** one is refused, exactly as `put_version` refuses a
                // row another version already occupies.
                if seq < row.version_seq {
                    tracing::warn!(
                        bucket = %bucket,
                        key = %key,
                        seq,
                        current = row.version_seq,
                        "refusing to move a version slot backwards"
                    );
                    return Err(Error::Refused(storage::io(io::Error::other(
                        "a version slot never moves backwards",
                    ))));
                }
                row.version_id = id;
                row.version_seq = seq;
                row.is_delete_marker = marker;
                table.put(&bucket, &key, &row)
            })
            .await
    }

    async fn put_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &Stored,
    ) -> Result<(), Error> {
        let refusal = storage::no_such_bucket(bucket);
        let bucket = bucket.to_string();
        let key = key.to_string();
        let encoded = table_row(row);
        let id = encoded.id.clone();
        let seq = encoded.seq;
        // The one identity two rows can share: the null wire. Committing it
        // triggers the convergence displacement below.
        let converges_null = row.id.is_none();
        let conflicted = self
            .driver
            .write(move |txn| -> Result<Option<String>, Error> {
                // The version-row write is a guarded write too: the bucket row
                // is probed in this transaction, first, exactly as the
                // object-row guarded writers probe it.
                if !BucketTable::open(txn)?.exists(&bucket)? {
                    return Err(Error::Refused(refusal));
                }
                // The row-overwrite guard: the row already at this sequence may
                // only be rewritten by the **same** version — the crash-window
                // adoption path re-runs a supersede whose row is already
                // committed. A different identity or kind, or the same identity
                // and kind naming different content (the null-versus-null
                // shape, where only the content address separates the two), is
                // another version's committed row: overwriting it would drop
                // that version's blob out of the live set. Refuse, and name the
                // state to the caller instead.
                let existing = VersionTable::open(txn)?.get(&bucket, &key, seq)?;
                if let Some(existing) = &existing
                    && (existing.id != encoded.id
                        || existing.kind != encoded.kind
                        || existing.blob != encoded.blob
                        || existing.size != encoded.size)
                {
                    // The context goes to the log, not the wire (2026-10-06
                    // review): the client answers the family's generic internal
                    // error, while an operator gets the bucket, key, sequence
                    // and the identity sitting there.
                    tracing::warn!(
                        bucket = %bucket,
                        key = %key,
                        seq,
                        existing = %existing.id,
                        "refusing to overwrite a version row that names a different version"
                    );
                    return Ok(Some(existing.id.clone()));
                }
                // The stamp source (2026-10-07 review): a row already stored at
                // this sequence carries the authoritative `became_noncurrent`,
                // and it wins over whatever the caller handed in. The crash
                // window's adopted row is rewritten by the very supersede that
                // wrote it, and a stamped row must never be un-stamped by it —
                // read here, in the writing transaction, so no other commit can
                // slip a stamp in between a read and this write.
                let stamp = match &existing {
                    Some(existing) => existing.became_noncurrent,
                    None => encoded.became_noncurrent,
                };
                let stored = VersionRow {
                    became_noncurrent: stamp,
                    ..encoded
                };
                // The one-null-version rule: a key holds at most one `null`-id
                // row, so the index's single null entry always names it. A row
                // the index points at for this identity, at another sequence, is
                // displaced — removed with its index entry in this same
                // transaction — rather than left unaddressable.
                if converges_null {
                    let stale = IndexTable::open(txn)?.seq_of(&bucket, &key, &stored.id)?;
                    if let Some(stale_seq) = stale
                        && stale_seq != seq
                    {
                        VersionTable::open(txn)?.remove(&bucket, &key, stale_seq)?;
                        IndexTable::open(txn)?
                            .remove_pointing_at(&bucket, &key, &stored.id, stale_seq)?;
                    }
                }
                VersionTable::open(txn)?.put(&bucket, &key, &stored)?;
                IndexTable::open(txn)?.put(&bucket, &key, &id, seq)?;
                Ok(None)
            })
            .await?;
        match conflicted {
            Some(_) => Err(Error::Refused(storage::io(io::Error::other(
                "a version row conflicts with the version being superseded",
            )))),
            None => Ok(()),
        }
    }

    async fn latest(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<(u64, Stored)>, Error> {
        let bucket = bucket.to_string();
        let key = key.to_string();
        self.driver.read(move |txn| {
            let Some((seq, row)) = VersionTable::open_readonly(txn)?.latest(&bucket, &key)? else {
                return Ok(None);
            };
            // The table's own `latest` already fails closed on a corrupt newest
            // row (it answers `None`, never an older row); the contract's
            // second, identical decode keeps the two from disagreeing.
            Ok(contract_row(&row).map(|stored| (seq, stored)))
        })
    }

    async fn bucket_rows(&self, bucket: &bucket::Name) -> Result<Vec<Stored>, Error> {
        let bucket = bucket.to_string();
        // A whole-bucket walk: the decode and the allocation run on the
        // blocking pool (the driver's own rule for traversals).
        self.driver
            .read_blocking(move |txn| {
                let table = VersionTable::open_readonly(txn)?;
                let mut rows = Vec::new();
                // The healing walk: a row whose shape is corrupt names no blob,
                // so it is skipped rather than failing the walk.
                table.for_bucket_of(&bucket, |_, row| {
                    if let Some(stored) = contract_row(&row) {
                        rows.push(stored);
                    }
                    Ok(())
                })?;
                Ok(rows)
            })
            .await
    }

    async fn get_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<Option<Stored>, Error> {
        let bucket = bucket.to_string();
        let key = key.to_string();
        self.driver.read(move |txn| {
            Ok(VersionTable::open_readonly(txn)?
                .get(&bucket, &key, seq)?
                .as_ref()
                .and_then(contract_row))
        })
    }

    async fn list_versions(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Vec<Stored>, Error> {
        let bucket = bucket.to_string();
        let key = key.to_string();
        // A key's history is a full walk of its rows: the decode and the
        // allocation run on the blocking pool (the driver's own rule for
        // traversals).
        self.driver
            .read_blocking(move |txn| {
                let table = VersionTable::open_readonly(txn)?;
                let mut rows = Vec::new();
                table.for_key(&bucket, &key, |row| {
                    if let Some(stored) = contract_row(&row) {
                        rows.push(stored);
                    }
                    Ok(())
                })?;
                Ok(rows)
            })
            .await
    }

    async fn expire(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<(), Error> {
        // The single-row form is the batch of one: [`VersionMeta::expire_many`]
        // runs the same body in the same one write transaction, so the
        // atomicity rule is unchanged.
        self.expire_many(bucket, key, &[seq]).await
    }

    async fn expire_many(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seqs: &[u64],
    ) -> Result<(), Error> {
        let bucket = bucket.to_string();
        let key = key.to_string();
        let seqs = seqs.to_vec();
        self.driver
            .write(move |txn| {
                // One transaction for the whole listed history (the batch is
                // the unit): a crash leaves every listed row or none of them.
                for seq in seqs {
                    let row = VersionTable::open(txn)?.get(&bucket, &key, seq)?;
                    VersionTable::open(txn)?.remove(&bucket, &key, seq)?;
                    if let Some(row) = row {
                        // The entry goes only when it still points at this
                        // row: two rows can share one identity wire (the null
                        // version), and the index names only the newest of
                        // them.
                        IndexTable::open(txn)?.remove_pointing_at(&bucket, &key, &row.id, seq)?;
                    }
                }
                Ok(())
            })
            .await
    }

    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let bucket = bucket.to_string();
        self.driver
            .write(move |txn| {
                // The rows and their index entries retire in **one** write
                // transaction (the plan's Global Constraint): the index can
                // never be left naming a row the drain removed. Both tables
                // drain their own bucket range — the interval boundary is the
                // table's, never hand-rolled here.
                VersionTable::open(txn)?.drain_bucket(&bucket)?;
                IndexTable::open(txn)?.drain_bucket(&bucket)?;
                Ok(())
            })
            .await
    }

    async fn version_seq_of(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<&Id>,
    ) -> Result<Option<u64>, Error> {
        let bucket = bucket.to_string();
        let key = key.to_string();
        let id = contract::wire_of(id);
        self.driver
            .read(move |txn| IndexTable::open_readonly(txn)?.seq_of(&bucket, &key, &id))
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::SystemTime};

    use redb::WriteTransaction;
    use tinio_util::testing::metadata::assert_version_conformance;

    use crate::{
        _core::{
            ETag,
            blob::{BlobRef, BlockHash},
            bucket,
            meta::{BucketMeta, ObjectMeta, VersionMeta, bucket as bucket_row, version::Current},
            object, storage,
            version::Id,
        },
        Driver, Error, RedbMetadata,
        object_meta::{Stored as ObjectStored, Table as ObjectTable},
        version::{Kind as RowKind, Row as VersionRow, Table as VersionTable, encode_blob},
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
            recorded.version_id.as_ref().map(Id::as_str),
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
        assert_eq!(slot.version.as_ref().map(Id::as_str), Some("v5"));

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
        assert_eq!(recorded.version_id.as_ref().map(Id::as_str), Some("v1"));
        assert_eq!(recorded.version_seq, 1);
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

    /// The `object_meta` row as the table layer sees it.
    fn raw_row(driver: &Arc<Driver>, bucket: &bucket::Name, key: &object::Key) -> ObjectStored {
        let (bucket, key) = (bucket.to_string(), key.to_string());
        driver
            .read(|txn| ObjectTable::open_readonly(txn)?.get(&bucket, &key))
            .unwrap()
            .expect("the slot's object row")
    }
}
