//! VersionMeta over the version_rows, version_index, bucket_versioning and
//! current_slots relations.
//!
//! A version row and its index entry commit together or not at all: the index
//! is keyed by the row's identity wire (bucket, key, id), so the two writes
//! share one transaction and a removal retires the entry only when it still
//! points at the sequence being removed (two rows can share the null wire, and
//! the index names only the newest of them).
//!
//! The current slot is separate state (current_slots): the slot records the
//! identity, the sequence and the delete-marker bit, and current_slot resolves
//! the bit from the version row at that sequence when the row exists, falling
//! back to the recorded column when it does not — a **restore** consumes the
//! row whose kind the bit came from, so the bit must outlive it (migration
//! 0002). Nothing is derived from an object row, so a content rewrite can never
//! silently un-version an object.

use async_trait::async_trait;
use sqlx::{query_file_as, query_file_scalar};
use time::OffsetDateTime;

use crate::{
    _core::{
        ETag,
        acl::Acl,
        bucket, checksum,
        meta::{
            VersionMeta,
            object::Row as ObjectRow,
            version::{Current, Kind, Stored, encode_blob},
        },
        object, storage,
        version::{Id, State, wire_of},
    },
    Error, begin,
    meta::{acl_composite, assert_bucket_row, object::upsert_object},
    store::PgStore,
    timestamp::{from_timestamp, to_timestamp},
};

/// The eleven columns every version read selects, in the order
/// `sql/meta/version/*.sql` spells them: the row's sequence, its identifier,
/// blob, size and the derived elements, then the metadata wires and the ACL
/// composite (which carries the owner).
///
/// The field order is the column order -- the macro builds the struct from the
/// statement's own columns -- so the decode below is a typed conversion, not a
/// positional read.
struct StoredVersion {
    seq: i64,
    id: String,
    blob: String,
    size: i64,
    etag: String,
    created: OffsetDateTime,
    became_noncurrent: i64,
    kind: Kind,
    tags: object::Tags,
    checksum: String,
    acl: Acl,
}

/// The slot read's columns: the current version's identity wire, which the
/// left join leaves NULL for a slot whose row is gone; the bit the slot's own
/// record carries (a restored marker outlives the row it came from); and the
/// row's kind, NULL when the row is gone. The row's kind decides when the row
/// is there; the recorded bit answers when it is not.
struct StoredSlot {
    version_id: Option<String>,
    is_delete_marker: bool,
    kind: Option<Kind>,
}

/// The columns `restore_slot` reads back for its premise: the identity the
/// caller's `id` must equal and the kind that decides the slot's marker bit.
struct StoredRestore {
    id: String,
    kind: Kind,
}

/// The columns `put_version` reads back for its conflict check: the identity,
/// blob, size and kind it must match, and the stamp it must adopt.
struct StoredExisting {
    id: String,
    blob: String,
    size: i64,
    kind: Kind,
    became_noncurrent: i64,
}

/// The one column `bucket_state` reads: the bucket's versioning state.
struct StoredState {
    state: State,
}

/// One expired row's key: the sequence it sat at and the identity wire the
/// index entry must still point at.
struct StoredExpired {
    seq: i64,
    id: String,
}

impl StoredVersion {
    /// The sequence and the contract row; `None` is a corrupt blob, kind or
    /// identifier -- what the row is and where its content lives cannot be
    /// guessed, so the row reads as absent (self-healing). The row is consumed:
    /// its tag set and ACL move into the contract row rather than being copied.
    fn decode(self) -> Option<(u64, Stored)> {
        let seq = self.seq as u64;
        Stored::from_values(
            seq,
            &self.id,
            &self.blob,
            self.size as u64,
            &self.etag,
            from_timestamp(self.created),
            self.became_noncurrent as u64,
            self.kind,
            self.tags,
            &self.checksum,
            self.acl,
        )
        .map(|stored| (seq, stored))
    }

    /// The contract row alone, for the reads that do not answer the sequence.
    fn contract(self) -> Option<Stored> {
        self.decode().map(|(_, stored)| stored)
    }
}

#[async_trait]
impl VersionMeta for PgStore {
    type Error = Error;

    /// A bucket with no row answers Unversioned rather than refusing: the read
    /// half of the state write's window, and a read writes nothing.
    async fn bucket_state(&self, bucket: &bucket::Name) -> Result<State, Error> {
        let row = query_file_as!(StoredState, "sql/meta/version/bucket_state.sql", bucket)
            .fetch_optional(self.pool())
            .await?;
        Ok(row.map_or(State::Unversioned, |row| row.state))
    }

    /// The unguarded state write: a teardown that commits between the caller's
    /// bucket check and this statement leaves its state row behind for a
    /// bucket that is gone. set_bucket_state_guarded closes that window.
    async fn set_bucket_state(&self, bucket: &bucket::Name, state: State) -> Result<(), Error> {
        sqlx::query_file!("sql/meta/version/set_bucket_state.sql", bucket, state as _)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn set_bucket_state_guarded(
        &self,
        bucket: &bucket::Name,
        state: State,
    ) -> Result<(), Error> {
        let mut tx = begin(self.pool()).await?;
        assert_bucket_row(&mut tx, bucket).await?;
        sqlx::query_file!("sql/meta/version/set_bucket_state.sql", bucket, state as _)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn current_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<Current>, Error> {
        let row = query_file_as!(StoredSlot, "sql/meta/version/current_slot.sql", bucket, key)
            .fetch_optional(self.pool())
            .await?;
        Ok(row.map(|row| Current {
            exists: true,
            version: row.version_id.as_deref().and_then(Id::from_wire),
            // The row's kind decides when the row is there — a slot must never
            // be overridden by a stale recorded bit — and the recorded bit
            // answers when the row is gone, which is the state a restore
            // leaves (it consumes the row it derived the bit from).
            is_delete_marker: row.kind.map_or(row.is_delete_marker, Kind::is_marker),
        }))
    }

    async fn advance_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<Id>,
        seq: u64,
    ) -> Result<(), Error> {
        let seq_value = seq as i64;
        let mut tx = begin(self.pool()).await?;
        // The advance writes, and a materialized object row is not less of a
        // write: the bucket row is probed in this transaction, first.
        assert_bucket_row(&mut tx, bucket).await?;
        let current = query_file_scalar!("sql/meta/version/advance_slot.sql", bucket, key)
            .fetch_optional(&mut *tx)
            .await?;
        if let Some(current) = current {
            // A move to the SAME sequence is the crash window's own re-run and
            // is allowed; a move to a lower one is refused.
            if seq_value < current {
                tx.rollback().await?;
                return Err(Error::SlotRewind {
                    bucket: bucket.clone(),
                    key: key.to_string(),
                    seq,
                });
            }
        }
        sqlx::query_file!(
            "sql/meta/version/advance_slot_upsert.sql",
            bucket,
            key,
            id as _,
            seq_value
        )
        .execute(&mut *tx)
        .await?;
        // The key's object row carries the slot's own record, so a key with no
        // object row gets one here. Its content-derived elements start empty;
        // the ETag is the empty-content ETag, a decodable value, so the row a
        // reader serves is not the point read's self-heal.
        sqlx::query_file!(
            "sql/meta/version/advance_slot_object_row.sql",
            bucket,
            key,
            ETag::EMPTY.as_str()
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// The restore's commit, in one transaction: the bucket row is asserted;
    /// the version row at `seq` is read `FOR UPDATE` and is the call's premise
    /// (it must exist and name `id` — its kind is the slot's marker bit, never
    /// the caller's); the row and its index entry are consumed; the object row
    /// is upserted from the caller's row with the retained part list cleared;
    /// the slot is written naming the restored version (a **backwards** move,
    /// so no monotonic guard applies — [`VersionMeta::advance_slot`]'s rule
    /// stays absolute); and the row the slot moves off is stamped noncurrent.
    async fn restore_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &ObjectRow,
        id: Option<Id>,
        seq: u64,
    ) -> Result<(), Error> {
        let wire = wire_of(id.as_ref());
        let seq_value = seq as i64;
        let mut tx = begin(self.pool()).await?;
        assert_bucket_row(&mut tx, bucket).await?;
        // The row the slot moves off: the sequence the slot records, locked
        // like `advance_slot`'s own read. A key with no slot moves off nothing.
        let moved_off = query_file_scalar!("sql/meta/version/advance_slot.sql", bucket, key)
            .fetch_optional(&mut *tx)
            .await?;
        // The version row at `seq` is the call's premise: it must exist and
        // name the identity the caller hands in, else the refusal — never a
        // slot naming a version the store does not hold.
        let consumed = query_file_as!(
            StoredRestore,
            "sql/meta/version/restore_slot.sql",
            bucket,
            key,
            seq_value
        )
        .fetch_optional(&mut *tx)
        .await?;
        let Some(consumed) = consumed else {
            tx.rollback().await?;
            return Err(Error::RestoreConflict {
                bucket: bucket.clone(),
                key: key.to_string(),
                seq,
            });
        };
        if consumed.id != wire {
            tx.rollback().await?;
            return Err(Error::RestoreConflict {
                bucket: bucket.clone(),
                key: key.to_string(),
                seq,
            });
        }
        // The consumed row and its index entry retire in this same transaction
        // (the `expire` body's two statements): the restored version IS the
        // current version, so it has no row of its own. The `FOR UPDATE` read
        // above holds the row, so the delete takes it; its answer (the wire) is
        // already in hand. The index entry goes only when it still points at
        // this row.
        let _ = query_file_scalar!("sql/meta/version/expire.sql", bucket, key, seq_value)
            .fetch_optional(&mut *tx)
            .await?;
        sqlx::query_file!(
            "sql/meta/version/delete_version_index.sql",
            bucket,
            key,
            consumed.id.as_str(),
            seq_value
        )
        .execute(&mut *tx)
        .await?;
        // The object row, wholesale from the caller's elements, and a write is
        // a fresh object: the retained part list goes in the same transaction.
        upsert_object(&mut tx, bucket, key, row).await?;
        sqlx::query_file!("sql/meta/object/clear_object_parts.sql", bucket, key)
            .execute(&mut *tx)
            .await?;
        // The slot, naming the restored version at its own sequence and
        // carrying the consumed row's kind: this write is what lets the bit
        // outlive the row it came from.
        sqlx::query_file!(
            "sql/meta/version/restore_slot_upsert.sql",
            bucket,
            key,
            id as _,
            seq_value,
            consumed.kind.is_marker()
        )
        .execute(&mut *tx)
        .await?;
        // The stamp the move owes the row it moves off, in this same commit,
        // under RG5's two exemptions: a move at the same sequence moves off
        // nothing, and a sequence holding no row has nothing to stamp — the
        // statement's `became_noncurrent = 0` is the first-stamp-wins rule.
        if let Some(moved_off) = moved_off.filter(|moved_off| *moved_off != seq_value) {
            sqlx::query_file!(
                "sql/meta/version/restore_slot_stamp.sql",
                bucket,
                key,
                moved_off,
                storage::now_nanos() as i64
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn put_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &Stored,
    ) -> Result<(), Error> {
        let id = wire_of(row.id.as_ref());
        let blob = encode_blob(&row.blob);
        let size = row.size;
        let etag = row.etag.as_str();
        let created = row.created;
        let became_noncurrent = row.became_noncurrent;
        let checksum = row
            .checksum
            .as_ref()
            .map(checksum::Recorded::to_wire)
            .unwrap_or_default();
        let acl = acl_composite(row.owner.as_ref(), &row.acl.grants);
        let kind = row.kind;
        let seq = row.seq as i64;
        let mut tx = begin(self.pool()).await?;
        assert_bucket_row(&mut tx, bucket).await?;
        let existing = query_file_as!(
            StoredExisting,
            "sql/meta/version/put_version.sql",
            bucket,
            key,
            seq
        )
        .fetch_optional(&mut *tx)
        .await?;
        let mut stamp = became_noncurrent;
        if let Some(existing) = &existing {
            if existing.id != id
                || existing.blob != blob
                || existing.size != size as i64
                || existing.kind != kind
            {
                tx.rollback().await?;
                return Err(Error::VersionConflict {
                    bucket: bucket.clone(),
                    key: key.to_string(),
                    seq: row.seq,
                });
            }
            // Whenever a row already sits at this sequence, the stamp it
            // carries is the authority: a stamped row is never un-stamped by
            // the rewrite that adopts it.
            stamp = existing.became_noncurrent as u64;
        }
        if row.id.is_none() {
            // Committing the null identity displaces any other null-id row of
            // the key, with its index entry, in this transaction.
            sqlx::query_file!(
                "sql/meta/version/put_version_displace_null.sql",
                bucket,
                key,
                seq
            )
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query_file!(
            "sql/meta/version/put_version_upsert.sql",
            bucket,
            key,
            seq,
            id,
            blob,
            size as i64,
            etag,
            to_timestamp(created),
            stamp as i64,
            kind as _,
            (&row.tags) as _,
            checksum,
            acl as _
        )
        .execute(&mut *tx)
        .await?;
        // The entry of another sequence under this identity must not survive
        // the move; the entry of this sequence is (re)installed below.
        sqlx::query_file!(
            "sql/meta/version/put_version_drop_stale_index.sql",
            bucket,
            key,
            seq,
            id
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query_file!(
            "sql/meta/version/put_version_index.sql",
            bucket,
            key,
            id,
            seq
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn get_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<Option<Stored>, Error> {
        let row = query_file_as!(
            StoredVersion,
            "sql/meta/version/get_version.sql",
            bucket,
            key,
            seq as i64
        )
        .fetch_optional(self.pool())
        .await?;
        Ok(row.and_then(StoredVersion::contract))
    }

    async fn latest(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<(u64, Stored)>, Error> {
        let row = query_file_as!(StoredVersion, "sql/meta/version/latest.sql", bucket, key)
            .fetch_optional(self.pool())
            .await?;
        // The newest row's decode is the read: a corrupt newest row answers
        // absent, never an older version.
        Ok(row.and_then(StoredVersion::decode))
    }

    async fn bucket_rows(&self, bucket: &bucket::Name) -> Result<Vec<Stored>, Error> {
        let rows = query_file_as!(StoredVersion, "sql/meta/version/bucket_rows.sql", bucket)
            .fetch_all(self.pool())
            .await?;
        // The healing walk: a row whose shape is corrupt names no blob, so it
        // is skipped rather than failing the walk.
        Ok(rows
            .into_iter()
            .filter_map(StoredVersion::contract)
            .collect())
    }

    async fn list_versions(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Vec<Stored>, Error> {
        let rows = query_file_as!(
            StoredVersion,
            "sql/meta/version/list_versions.sql",
            bucket,
            key
        )
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .filter_map(StoredVersion::contract)
            .collect())
    }

    async fn expire(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<(), Error> {
        let seq_value = seq as i64;
        let mut tx = begin(self.pool()).await?;
        let removed = query_file_scalar!("sql/meta/version/expire.sql", bucket, key, seq_value)
            .fetch_optional(&mut *tx)
            .await?;
        if let Some(id) = removed {
            // The index entry goes only when it still points at THIS sequence:
            // two rows can share one identity wire (the null version), and the
            // index names only the newest of them.
            sqlx::query_file!(
                "sql/meta/version/delete_version_index.sql",
                bucket,
                key,
                id,
                seq_value
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn expire_many(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seqs: &[u64],
    ) -> Result<(), Error> {
        if seqs.is_empty() {
            return Ok(());
        }
        let wires: Vec<i64> = seqs.iter().map(|seq| *seq as i64).collect();
        let mut tx = begin(self.pool()).await?;
        let removed = query_file_as!(
            StoredExpired,
            "sql/meta/version/expire_many.sql",
            bucket,
            key,
            &wires
        )
        .fetch_all(&mut *tx)
        .await?;
        if !removed.is_empty() {
            let (ids, seqs): (Vec<String>, Vec<i64>) =
                removed.into_iter().map(|row| (row.id, row.seq)).unzip();
            sqlx::query_file!(
                "sql/meta/version/delete_version_index_many.sql",
                bucket,
                key,
                &ids,
                &seqs
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let mut tx = begin(self.pool()).await?;
        sqlx::query!("DELETE FROM version_rows WHERE bucket = $1", bucket)
            .execute(&mut *tx)
            .await?;
        sqlx::query!("DELETE FROM version_index WHERE bucket = $1", bucket)
            .execute(&mut *tx)
            .await?;
        // The history is gone. A leftover slot would make the next advance
        // from sequence 1 a rewind, and there is no other call that clears it.
        sqlx::query!("DELETE FROM current_slots WHERE bucket = $1", bucket)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn version_seq_of(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<&Id>,
    ) -> Result<Option<u64>, Error> {
        let row = sqlx::query_scalar!(
            "SELECT seq FROM version_index WHERE bucket = $1 AND key = $2 AND id = $3",
            bucket,
            key,
            wire_of(id)
        )
        .fetch_optional(self.pool())
        .await?;
        Ok(row.map(|seq| seq as u64))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        _core::{
            ETag,
            acl::Acl,
            blob::{BlobRef, BlockHash},
            meta::{
                ObjectMeta, VersionMeta,
                object::Row as ObjectRow,
                version::{Kind, Stored},
            },
            object,
            version::{Id, State},
        },
        Error,
        tests::fixtures::{bucket_row, key, name, scratch},
        timestamp::{unix_micros, unix_seconds as at},
    };

    fn version(id: Option<&Id>, seq: u64) -> Stored {
        Stored {
            id: id.cloned(),
            blob: BlobRef::single(BlockHash::of(b"plane"), 5),
            size: 5,
            etag: ETag::EMPTY,
            created: at(10),
            became_noncurrent: 0,
            kind: Kind::Object,
            tags: object::Tags::empty(),
            checksum: None,
            owner: None,
            acl: Acl::default_private(None),
            seq,
        }
    }

    fn marker(seq: u64) -> Stored {
        Stored {
            kind: Kind::Marker,
            ..version(None, seq)
        }
    }

    /// The caller's object row for a restore: the marker's own shape — empty
    /// content, no file identity.
    fn restored_row() -> ObjectRow {
        ObjectRow {
            etag: ETag::EMPTY,
            size: 0,
            mtime: at(10),
            identity: 0,
            tags: object::Tags::empty(),
            checksum: None,
            owner: None,
            acl: Acl::default_private(None),
        }
    }

    #[tokio::test]
    async fn the_creation_time_round_trips_with_sub_second_precision() {
        let Some((db, store)) = scratch().await else {
            return;
        };
        let data = name("plane-version-micros");
        bucket_row(&store, &data).await;
        let target = key("micros.txt");
        let created = unix_micros(1_700_000_000_222_333);
        let mut row = version(None, 1);
        row.created = created;
        VersionMeta::put_version(&store, &data, &target, &row)
            .await
            .unwrap();
        let got = VersionMeta::get_version(&store, &data, &target, 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            got.created, created,
            "timestamptz keeps the sub-second digits"
        );
        db.drop_db().await;
    }

    #[tokio::test]
    async fn version_rows_round_trip_and_the_index_follows_them() {
        let Some((db, store)) = scratch().await else {
            return;
        };
        let data = name("plane-version-rows");
        bucket_row(&store, &data).await;
        let target = key("v.txt");
        let v1 = Id::new("v1").expect("a valid version id");
        VersionMeta::set_bucket_state(&store, &data, State::Enabled)
            .await
            .unwrap();
        assert_eq!(
            VersionMeta::bucket_state(&store, &data).await.unwrap(),
            State::Enabled
        );
        assert_eq!(
            VersionMeta::bucket_state(&store, &name("plane-version-absent"))
                .await
                .unwrap(),
            State::Unversioned,
            "a bucket with no state row is unversioned"
        );
        VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 1))
            .await
            .unwrap();
        let stored = VersionMeta::get_version(&store, &data, &target, 1)
            .await
            .unwrap()
            .expect("the row just written");
        assert_eq!(stored.id.as_ref(), Some(&v1));
        assert_eq!(stored.seq, 1);
        assert_eq!(
            VersionMeta::version_seq_of(&store, &data, &target, Some(&v1))
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            VersionMeta::latest(&store, &data, &target)
                .await
                .unwrap()
                .map(|(seq, _)| seq),
            Some(1)
        );
        assert_eq!(
            VersionMeta::bucket_rows(&store, &data).await.unwrap().len(),
            1
        );
        VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 2))
            .await
            .unwrap();
        assert_eq!(
            VersionMeta::list_versions(&store, &data, &target)
                .await
                .unwrap()
                .iter()
                .map(|row| row.seq)
                .collect::<Vec<_>>(),
            vec![2, 1],
            "the history reads newest first"
        );
        assert_eq!(
            VersionMeta::version_seq_of(&store, &data, &target, Some(&v1))
                .await
                .unwrap(),
            Some(2),
            "the index entry followed the row"
        );
        db.drop_db().await;
    }

    #[tokio::test]
    async fn every_bucket_state_label_round_trips() {
        let Some((db, store)) = scratch().await else {
            return;
        };
        let data = name("plane-version-state");
        bucket_row(&store, &data).await;
        for state in [State::Unversioned, State::Enabled, State::Suspended] {
            VersionMeta::set_bucket_state(&store, &data, state)
                .await
                .unwrap();
            assert_eq!(
                VersionMeta::bucket_state(&store, &data).await.unwrap(),
                state,
                "the bucket_state enum type round-trips {state:?}"
            );
        }
        db.drop_db().await;
    }

    #[tokio::test]
    async fn put_version_refuses_a_different_row_and_preserves_the_stored_stamp() {
        let Some((db, store)) = scratch().await else {
            return;
        };
        let data = name("plane-version-conflict");
        bucket_row(&store, &data).await;
        let target = key("v.txt");
        let v1 = Id::new("v1").expect("a valid version id");
        VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 1))
            .await
            .unwrap();
        let original = VersionMeta::get_version(&store, &data, &target, 1)
            .await
            .unwrap()
            .unwrap();
        let mut clash = version(Some(&v1), 1);
        clash.blob = BlobRef::single(BlockHash::of(b"other"), 5);
        assert!(
            matches!(
                VersionMeta::put_version(&store, &data, &target, &clash).await,
                Err(Error::VersionConflict { .. })
            ),
            "a row naming different content at the same sequence is refused"
        );
        assert_eq!(
            VersionMeta::get_version(&store, &data, &target, 1)
                .await
                .unwrap()
                .unwrap(),
            original,
            "the refusal overwrote nothing"
        );
        // The same version rewritten adopts the row and keeps its stamp.
        let mut adoption = version(Some(&v1), 1);
        adoption.became_noncurrent = 77;
        VersionMeta::put_version(&store, &data, &target, &adoption)
            .await
            .unwrap();
        assert_eq!(
            VersionMeta::get_version(&store, &data, &target, 1)
                .await
                .unwrap()
                .unwrap()
                .became_noncurrent,
            0,
            "the stored stamp is the authority"
        );
        assert!(matches!(
            VersionMeta::put_version(&store, &name("plane-version-absent"), &target, &adoption)
                .await,
            Err(Error::NoSuchBucket(_))
        ));
        db.drop_db().await;
    }

    /// S2/S7's marker arm: the restored marker's bit outlives the row it came
    /// from — `restore_slot` consumes that row, so the recorded slot column
    /// (migration 0002) is what `current_slot` answers from afterwards. The
    /// shared suite cannot pin this (a shared leg may not depend on one store's
    /// slot shape), so the store's own suite does.
    #[tokio::test]
    async fn restore_slot_restores_a_marker_and_the_bit_survives_its_row() {
        let Some((db, store)) = scratch().await else {
            return;
        };
        let data = name("plane-version-restore-marker");
        bucket_row(&store, &data).await;
        let target = key("restore.txt");
        let v1 = Id::new("v1").expect("a valid version id");
        let v3 = Id::new("v3").expect("a valid version id");
        // The successor is a delete marker; the removed current version's row
        // sits above it and the slot names that one.
        let mut successor = version(Some(&v1), 1);
        successor.kind = Kind::Marker;
        successor.blob = BlobRef::empty();
        successor.size = 0;
        VersionMeta::put_version(&store, &data, &target, &successor)
            .await
            .unwrap();
        VersionMeta::put_version(&store, &data, &target, &version(Some(&v3), 3))
            .await
            .unwrap();
        VersionMeta::advance_slot(&store, &data, &target, Some(v3), 3)
            .await
            .unwrap();
        assert!(
            !VersionMeta::current_slot(&store, &data, &target)
                .await
                .unwrap()
                .expect("the slot is recorded")
                .is_delete_marker,
            "the removed current version is an object version"
        );

        VersionMeta::restore_slot(&store, &data, &target, &restored_row(), Some(v1.clone()), 1)
            .await
            .unwrap();

        let slot = VersionMeta::current_slot(&store, &data, &target)
            .await
            .unwrap()
            .expect("the slot is recorded");
        assert_eq!(slot.version.as_ref(), Some(&v1));
        assert!(
            slot.is_delete_marker,
            "the restored marker's bit survives the row it came from"
        );
        assert!(
            VersionMeta::get_version(&store, &data, &target, 1)
                .await
                .unwrap()
                .is_none(),
            "the consumed marker row is gone"
        );
        // S5: the row the slot moved off stopped being current, in the same
        // commit.
        assert_ne!(
            VersionMeta::get_version(&store, &data, &target, 3)
                .await
                .unwrap()
                .expect("the moved-off row stays")
                .became_noncurrent,
            0,
            "the restore stamps the row the slot moves off"
        );
        db.drop_db().await;
    }

    #[tokio::test]
    async fn advance_slot_materializes_the_object_row_and_never_rewinds() {
        let Some((db, store)) = scratch().await else {
            return;
        };
        let data = name("plane-version-slot");
        bucket_row(&store, &data).await;
        let target = key("marker.txt");
        let v1 = Id::new("v1").expect("a valid version id");
        VersionMeta::advance_slot(&store, &data, &target, Some(v1.clone()), 3)
            .await
            .unwrap();
        let slot = VersionMeta::current_slot(&store, &data, &target)
            .await
            .unwrap()
            .expect("the slot is recorded");
        assert!(slot.exists);
        assert_eq!(slot.version.as_ref(), Some(&v1));
        assert!(!slot.is_delete_marker);
        let materialized = ObjectMeta::get(&store, &data, &target)
            .await
            .unwrap()
            .expect("the advance materialized the object row");
        assert_eq!(materialized.etag, ETag::EMPTY);
        assert!(
            matches!(
                VersionMeta::advance_slot(&store, &data, &target, None, 2).await,
                Err(Error::SlotRewind { .. })
            ),
            "a slot never moves backwards"
        );
        VersionMeta::advance_slot(&store, &data, &target, None, 3)
            .await
            .unwrap();
        assert!(
            VersionMeta::current_slot(&store, &data, &target)
                .await
                .unwrap()
                .unwrap()
                .version
                .is_none(),
            "the same sequence is the crash window's own re-run"
        );
        // A marker at the slot's sequence flips the marker bit.
        VersionMeta::put_version(&store, &data, &target, &marker(3))
            .await
            .unwrap();
        VersionMeta::advance_slot(&store, &data, &target, None, 3)
            .await
            .unwrap();
        assert!(
            VersionMeta::current_slot(&store, &data, &target)
                .await
                .unwrap()
                .unwrap()
                .is_delete_marker
        );
        let absent = name("plane-version-absent");
        assert!(matches!(
            VersionMeta::advance_slot(&store, &absent, &target, None, 1).await,
            Err(Error::NoSuchBucket(_))
        ));
        assert!(matches!(
            VersionMeta::set_bucket_state_guarded(&store, &absent, State::Enabled).await,
            Err(Error::NoSuchBucket(_))
        ));
        db.drop_db().await;
    }

    #[tokio::test]
    async fn the_null_identity_displaces_the_older_null_row() {
        let Some((db, store)) = scratch().await else {
            return;
        };
        let data = name("plane-version-null");
        bucket_row(&store, &data).await;
        let target = key("null.txt");
        VersionMeta::put_version(&store, &data, &target, &version(None, 1))
            .await
            .unwrap();
        let v1 = Id::new("v1").expect("a valid version id");
        VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 2))
            .await
            .unwrap();
        VersionMeta::put_version(&store, &data, &target, &version(None, 4))
            .await
            .unwrap();
        assert_eq!(
            VersionMeta::version_seq_of(&store, &data, &target, None)
                .await
                .unwrap(),
            Some(4),
            "the index's null entry names the row just committed"
        );
        assert!(
            VersionMeta::get_version(&store, &data, &target, 1)
                .await
                .unwrap()
                .is_none(),
            "the older null-id row was displaced"
        );
        let mut seqs = VersionMeta::list_versions(&store, &data, &target)
            .await
            .unwrap()
            .iter()
            .map(|row| row.seq)
            .collect::<Vec<_>>();
        seqs.sort_unstable();
        assert_eq!(seqs, vec![2, 4], "exactly one null-id row remains");
        db.drop_db().await;
    }

    #[tokio::test]
    async fn expire_and_the_bucket_drain_retire_the_index_entries() {
        let Some((db, store)) = scratch().await else {
            return;
        };
        let data = name("plane-version-expire");
        bucket_row(&store, &data).await;
        let target = key("sweep.txt");
        let v1 = Id::new("v1").expect("a valid version id");
        let v2 = Id::new("v2").expect("a valid version id");
        VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 1))
            .await
            .unwrap();
        VersionMeta::put_version(&store, &data, &target, &version(Some(&v2), 2))
            .await
            .unwrap();
        VersionMeta::expire(&store, &data, &target, 2)
            .await
            .unwrap();
        VersionMeta::expire(&store, &data, &target, 2)
            .await
            .unwrap();
        assert!(
            VersionMeta::get_version(&store, &data, &target, 2)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            VersionMeta::version_seq_of(&store, &data, &target, Some(&v2))
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            VersionMeta::version_seq_of(&store, &data, &target, Some(&v1))
                .await
                .unwrap(),
            Some(1),
            "the other identity's entry is untouched"
        );
        VersionMeta::expire_many(&store, &data, &target, &[1])
            .await
            .unwrap();
        VersionMeta::expire_many(&store, &data, &target, &[1])
            .await
            .unwrap();
        assert!(
            VersionMeta::bucket_rows(&store, &data)
                .await
                .unwrap()
                .is_empty()
        );
        // Another bucket's rows survive the drain.
        let other = name("plane-version-other");
        bucket_row(&store, &other).await;
        VersionMeta::put_version(&store, &other, &target, &version(Some(&v1), 1))
            .await
            .unwrap();
        VersionMeta::drain_bucket(&store, &data).await.unwrap();
        assert_eq!(
            VersionMeta::bucket_rows(&store, &other)
                .await
                .unwrap()
                .len(),
            1
        );
        db.drop_db().await;
    }
}
