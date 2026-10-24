use std::time::SystemTime;

use async_trait::async_trait;
use sqlx::{query_file_as, query_file_scalar};
use tracing::instrument;

use super::{
    SlotGuard, StoredExisting, StoredRestore, Verdict, advance_slot_write, assert_lockable,
    expire_batch, expire_row, lock_for_update, locked_delete_refusal, restore_slot_write,
    write_lock_row,
};
use crate::{
    _core::{
        ETag,
        acl::Acl,
        bucket, checksum,
        meta::{
            VersionMeta,
            object::Row as ObjectRow,
            version::{
                Current, ExpireReport, Expired, Kind, Stored, encode_blob, slot_move_rewinds,
            },
        },
        object, object_lock, storage,
        version::{Id, State, wire_of},
    },
    Error, begin,
    meta::{
        LockRow, acl_composite, assert_bucket_row, bucket_lock_enabled, bucket_row_present,
        object::upsert_object,
        version::{StoredState, StoredVersion},
    },
    store::PgStore,
    timestamp::to_timestamp,
};

#[async_trait]
impl VersionMeta for PgStore {
    type Error = Error;

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
        // R21: the versioning/lock cross-check under the bucket row's lock. A
        // retention write holds the same row while it reads the configuration
        // and the state (OL13), so a suspend cannot commit between those reads
        // and the retention write; and a lock-enabled bucket can never be
        // suspended.
        let enabled = bucket_lock_enabled(&mut tx, bucket).await?;
        if state == State::Suspended && enabled {
            tx.rollback().await?;
            return Err(Error::Storage(storage::invalid_bucket_state(bucket)));
        }
        sqlx::query_file!("sql/meta/version/set_bucket_state.sql", bucket, state as _)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    #[instrument(
        name = "advance_slot",
        target = "tinio::storage",
        level = "debug",
        skip(self),
        fields(bucket = %bucket, key = %key)
    )]
    async fn advance_slot_with(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<Id>,
        seq: u64,
        lock: Option<&object_lock::Lock>,
    ) -> Result<(), Error> {
        let seq_value = seq as i64;
        let wire = wire_of(id.as_ref());
        let mut tx = begin(self.pool()).await?;
        // The advance writes, and a materialized object row is not less of a
        // write: the bucket row is probed in this transaction, first.
        assert_bucket_row(&mut tx, bucket).await?;
        // The slot's guard facts, locked: the sequence the move plan compares
        // against, and the identity a null in-place write would replace.
        let current = query_file_as!(
            SlotGuard,
            "sql/meta/version/slot_for_update.sql",
            bucket,
            key
        )
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(current) = &current
            && slot_move_rewinds(id.as_ref(), seq, current.seq as u64)
        {
            tx.rollback().await?;
            return Err(Error::SlotRewind {
                bucket: bucket.clone(),
                key: key.to_string(),
                seq,
            });
        }
        // OL7: a null in-place replacement of an identity that already owns an
        // active lock is a protected overwrite. A marker owns no lock (R19),
        // and an ordinary overwrite creates a new identity, leaving the old
        // lock intact.
        if id.is_none()
            && let Some(slot) = current.as_ref().filter(|slot| !slot.is_delete_marker)
            && let Some(reason) = locked_delete_refusal(
                &mut tx,
                bucket,
                key,
                &slot.wire(),
                SystemTime::now(),
                object_lock::Bypass::None,
            )
            .await?
        {
            tx.rollback().await?;
            return Err(Error::Storage(storage::locked(reason)));
        }
        // The slot's marker bit comes from the version row at `seq`; a marker
        // never gets a lock row (OL5, R19), and an advance onto a free
        // sequence is not a marker.
        let marker = sqlx::query_scalar!(
            "SELECT kind = 'marker' AS \"marker!\" FROM version_rows WHERE bucket = $1 AND key = $2 AND seq = $3",
            bucket,
            key,
            seq_value
        )
        .fetch_optional(&mut *tx)
        .await?
        .unwrap_or(false);
        // The slot write and the stamp the move owes the row it moves off, in this one
        // transaction (the 2026-10-09 ruling): the supersede committed the outgoing version's
        // row with `became_noncurrent == 0` — the crash window's adopted row, not history yet —
        // and this commit records when it stopped being current, so a row that is history says
        // so and its retention clock starts when the version actually stopped being current.
        advance_slot_write(
            &mut tx,
            bucket,
            key,
            id,
            seq_value,
            current.as_ref().map(|slot| slot.seq),
        )
        .await?;
        // OL7: the lock commits with the current version, and a slot advance
        // never removes an existing lock row. Only a lock that carries a fact
        // is stored: an empty lock is "unlocked", and writing it back would
        // create a row that protects nothing (OL5).
        if !marker && let Some(lock) = lock.filter(|lock| lock.carries_fact()) {
            let (mode, retain_until, hold) = lock.columns();
            sqlx::query_file!(
                "sql/meta/version/lock_upsert.sql",
                bucket,
                key,
                wire,
                mode.as_deref(),
                retain_until,
                hold.as_str()
            )
            .execute(&mut *tx)
            .await?;
        }
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
    #[instrument(
        target = "tinio::storage",
        level = "debug",
        skip(self, row),
        fields(bucket = %bucket, key = %key)
    )]
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
        // outlive the row it came from. The stamp the move owes the row it moves
        // off rides the same write, in this same commit.
        restore_slot_write(
            &mut tx,
            bucket,
            key,
            id,
            seq_value,
            consumed.kind.is_marker(),
            moved_off,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    #[instrument(
        target = "tinio::storage",
        level = "debug",
        skip(self, row),
        fields(bucket = %bucket, key = %key)
    )]
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

    /// The permanent delete, with the lock enforced by `expire_row` inside
    /// the one transaction that removes the row, its index entry and its lock
    /// row (OL8).
    ///
    /// The target row (or, for a row-less current version, the slot) is read
    /// `FOR UPDATE` before its lock is enforced, so a concurrent retention
    /// write either lands before this check or waits for it. The answer is the
    /// store's verdict: `Removed` when a row retired, `Nothing` when the
    /// sequence named no version at all.
    async fn expire_with(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
        bypass: object_lock::Bypass,
    ) -> Result<Expired, Error> {
        let mut tx = begin(self.pool()).await?;
        // The global lock order is bucket row → slot → version row → lock row.
        // The bucket row is the serialization point the drain and the delete
        // family share, so neither can deadlock the other; an absent bucket
        // holds no row for anything to wait on and is left to the reads below.
        let _ = bucket_row_present(&mut tx, bucket).await?;
        match expire_row(&mut tx, bucket, key, seq, bypass, SystemTime::now()).await? {
            Verdict::Protected(reason) => {
                tx.rollback().await?;
                Err(Error::Storage(storage::locked(reason)))
            }
            Verdict::Removed => {
                tx.commit().await?;
                Ok(Expired::Removed)
            }
            // A cleared row-less slot and an absent sequence both removed no
            // version row: the contract's `Nothing` verdict.
            Verdict::Cleared | Verdict::Absent => {
                tx.commit().await?;
                Ok(Expired::Nothing)
            }
        }
    }

    /// The batched permanent delete: one transaction for the whole listed
    /// history, skipped and reporting every protected sequence (OL8).
    ///
    /// The batch is the transaction's unit — a crash leaves every listed row
    /// or none of them — and the bucket row's lock is its first statement, so
    /// two batch deletes on one bucket serialize. The reads are set-based
    /// (`expire_batch`): the bucket row, the key's slot, every addressed
    /// version row, and their lock rows are each read once, not once per
    /// sequence. A sequence listed twice names one version and is reported at
    /// most once, in first-occurrence order.
    async fn expire_many_report(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seqs: &[u64],
    ) -> Result<ExpireReport, Error> {
        if seqs.is_empty() {
            return Ok(ExpireReport::default());
        }
        let now = SystemTime::now();
        let mut tx = begin(self.pool()).await?;
        // The global lock order is bucket row → slot → version row → lock row.
        // The bucket row serializes the delete family, the drain and every
        // guarded writer, so no cycle can form.
        let _ = bucket_row_present(&mut tx, bucket).await?;
        let plan = expire_batch(&mut tx, bucket, key, seqs, now).await?;
        // The rows, the index entries and the lock rows each go in one
        // statement: the set-based deletes that replace the per-sequence loop.
        if !plan.row_seqs.is_empty() {
            sqlx::query_file!(
                "sql/meta/version/expire_many.sql",
                bucket,
                key,
                &plan.row_seqs
            )
            .execute(&mut *tx)
            .await?;
        }
        if !plan.index_pairs.is_empty() {
            let (index_seqs, ids): (Vec<i64>, Vec<String>) = plan.index_pairs.into_iter().unzip();
            sqlx::query_file!(
                "sql/meta/version/delete_version_index_many.sql",
                bucket,
                key,
                &ids,
                &index_seqs
            )
            .execute(&mut *tx)
            .await?;
        }
        if !plan.lock_wires.is_empty() {
            sqlx::query_file!(
                "sql/meta/version/lock_clear_many.sql",
                bucket,
                key,
                &plan.lock_wires
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(ExpireReport {
            removed: plan.removed,
            protected: plan.protected,
        })
    }

    /// Retire a bucket's version rows and their index entries.
    ///
    /// A key's **slot** is not this call's to retire (R7, written into
    /// [`VersionMeta::drain_bucket`]):
    /// the slot is the object plane's per-key state, and the bucket teardown —
    /// which holds the bucket row's lock and removes the whole namespace — is
    /// what clears it. Draining the rows alone leaves the slot recorded, so a
    /// refusal that rolled a teardown back has not lost the version plane.
    ///
    /// The bucket row's lock is taken first, so a drain serializes with the
    /// guarded version-plane writers (`assert_bucket_row`, the crate's one
    /// serialization point for them) rather than cutting between them. A bucket
    /// with no row is `NoSuchBucket`, exactly as every other guarded writer
    /// answers.
    ///
    /// OL8: the drain refuses, removing nothing, while a lock row still owns a
    /// version — a `Kind::Object` version row or a non-marker current slot. An
    /// orphan lock row (one that resolves to neither) never refuses, and the
    /// drain removes it with the rest. A corrupt owned lock row fails closed.
    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let mut tx = begin(self.pool()).await?;
        assert_bucket_row(&mut tx, bucket).await?;
        // The owner query returns only lock rows that resolve to a version, so
        // an orphan is never decoded and never refuses; `FOR UPDATE OF l`
        // holds each owned row until this transaction ends.
        let owned: Vec<LockRow> =
            query_file_as!(LockRow, "sql/meta/version/drain_lock_owners.sql", bucket)
                .fetch_all(&mut *tx)
                .await?;
        let now = SystemTime::now();
        for row in owned {
            let stored = match row.decode() {
                Ok(stored) => stored,
                Err(err) => {
                    tx.rollback().await?;
                    return Err(err);
                }
            };
            if let Some(reason) =
                object_lock::delete_refusal(stored.as_ref(), now, object_lock::Bypass::None)
            {
                tx.rollback().await?;
                return Err(Error::Storage(storage::locked(reason)));
            }
        }
        sqlx::query!("DELETE FROM version_rows WHERE bucket = $1", bucket)
            .execute(&mut *tx)
            .await?;
        sqlx::query!("DELETE FROM version_index WHERE bucket = $1", bucket)
            .execute(&mut *tx)
            .await?;
        sqlx::query!("DELETE FROM version_object_lock WHERE bucket = $1", bucket)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// The test-only raw lock writer: an upsert for `Some`, a removal for
    /// `None`, in one statement each (OL5).
    #[cfg(any(test, feature = "testing"))]
    async fn set_version_lock(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
        lock: Option<&object_lock::Lock>,
    ) -> Result<(), Error> {
        write_lock_row(self.pool(), bucket, key, &wire_of(version), lock).await
    }

    async fn version_lock(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<Option<object_lock::Lock>, Error> {
        let wire = wire_of(version);
        let row = query_file_as!(LockRow, "sql/meta/version/lock_get.sql", bucket, key, wire)
            .fetch_optional(self.pool())
            .await?;
        row.map_or(Ok(None), LockRow::decode)
    }

    async fn get_object_retention(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<Option<object_lock::Retention>, Error> {
        Ok(VersionMeta::version_lock(self, bucket, key, version)
            .await?
            .and_then(|lock| lock.retention))
    }

    async fn get_object_legal_hold(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<object_lock::LegalHold, Error> {
        Ok(VersionMeta::version_lock(self, bucket, key, version)
            .await?
            .map(|lock| lock.legal_hold)
            .unwrap_or(object_lock::LegalHold::Off))
    }

    /// The request's retention update, read-modify-write in one transaction
    /// (OL5, OL13): the bucket row, then the version row or current slot, then
    /// the lock row are locked in that order, and the model's
    /// [`object_lock::apply_retention`] decides. A lock that carries neither a
    /// retention nor a hold is removed, not stored.
    async fn set_retention(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
        requested: Option<&object_lock::Retention>,
        bypass: object_lock::Bypass,
        now: SystemTime,
    ) -> Result<Option<object_lock::Lock>, Error> {
        let wire = wire_of(version);
        let mut tx = begin(self.pool()).await?;
        if let Err(err) = assert_lockable(&mut tx, bucket, key, &wire).await {
            tx.rollback().await?;
            return Err(err);
        }
        let current = match lock_for_update(&mut tx, bucket, key, &wire).await {
            Ok(current) => current,
            Err(err) => {
                tx.rollback().await?;
                return Err(err);
            }
        };
        let updated = match object_lock::apply_retention(current.as_ref(), requested, bypass, now) {
            Ok(updated) => updated,
            Err(reason) => {
                tx.rollback().await?;
                return Err(Error::Storage(storage::locked(reason)));
            }
        };
        write_lock_row(&mut *tx, bucket, key, &wire, updated.as_ref()).await?;
        tx.commit().await?;
        Ok(updated)
    }

    /// The legal-hold write, in one transaction (OL5, OL13): the current
    /// retention is preserved and a hold-only lock is stored; clearing the
    /// hold on a version with no retention removes the row.
    async fn set_legal_hold(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
        hold: object_lock::LegalHold,
    ) -> Result<object_lock::Lock, Error> {
        let wire = wire_of(version);
        let mut tx = begin(self.pool()).await?;
        if let Err(err) = assert_lockable(&mut tx, bucket, key, &wire).await {
            tx.rollback().await?;
            return Err(err);
        }
        let current = match lock_for_update(&mut tx, bucket, key, &wire).await {
            Ok(current) => current,
            Err(err) => {
                tx.rollback().await?;
                return Err(err);
            }
        };
        let updated = object_lock::apply_legal_hold(current.as_ref(), hold);
        write_lock_row(&mut *tx, bucket, key, &wire, updated.as_ref()).await?;
        tx.commit().await?;
        Ok(updated.unwrap_or_default())
    }

    /// The combined retention and legal-hold update, read-modify-write in one
    /// transaction (OL5, OL13, OL23): the bucket row, the version row or
    /// current slot, then the lock row are locked in that order, and the
    /// model's [`object_lock::apply_lock`] decides both members before the one
    /// write. A refused retention writes neither member.
    async fn set_lock(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
        retention: Option<Option<&object_lock::Retention>>,
        legal_hold: Option<object_lock::LegalHold>,
        bypass: object_lock::Bypass,
        now: SystemTime,
    ) -> Result<Option<object_lock::Lock>, Error> {
        let wire = wire_of(version);
        let mut tx = begin(self.pool()).await?;
        if let Err(err) = assert_lockable(&mut tx, bucket, key, &wire).await {
            tx.rollback().await?;
            return Err(err);
        }
        let current = match lock_for_update(&mut tx, bucket, key, &wire).await {
            Ok(current) => current,
            Err(err) => {
                tx.rollback().await?;
                return Err(err);
            }
        };
        let updated =
            match object_lock::apply_lock(current.as_ref(), retention, legal_hold, bypass, now) {
                Ok(updated) => updated,
                Err(reason) => {
                    tx.rollback().await?;
                    return Err(Error::Storage(storage::locked(reason)));
                }
            };
        write_lock_row(&mut *tx, bucket, key, &wire, updated.as_ref()).await?;
        tx.commit().await?;
        Ok(updated)
    }

    /// A bucket with no row answers Unversioned rather than refusing: the read
    /// half of the state write's window, and a read writes nothing.
    async fn bucket_state(&self, bucket: &bucket::Name) -> Result<State, Error> {
        let row = query_file_as!(StoredState, "sql/meta/version/bucket_state.sql", bucket)
            .fetch_optional(self.pool())
            .await?;
        Ok(row.map_or(State::Unversioned, |row| row.state))
    }

    async fn current_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<Current>, Error> {
        // The slot row's own decode is `slot_record`'s, so this and the gate cannot disagree:
        // the sequence it also reads is simply dropped here.
        Ok(self
            .slot_record(bucket, key)
            .await?
            .map(|(_seq, current)| current))
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
