//! The VersionMeta command plane and the Object Lock rules over PostgreSQL.
//!
//! Every write here takes the same lock order: the bucket row, the key's
//! current slot, the version row, then its lock row (OL5, OL8, OL13). The
//! bucket row is the serialization point the delete family and the drain
//! share, so no two writers can deadlock each other.
//!
//! Object Lock has one enforcement decision, [`object_lock::may_delete`]. The
//! single-sequence [`expire_row`] and the set-based [`expire_batch`] both ask
//! it, so neither the delete family nor the retention sweep spells the rule a
//! second time: the row path resolves the version ([`Verdict::Removed`]), the
//! row-less current slot ([`Verdict::Cleared`]), nothing
//! ([`Verdict::Absent`]) or a refusal ([`Verdict::Protected`]).

use std::{
    collections::{HashMap, HashSet},
    time::SystemTime,
};

use sqlx::{query_file_as, query_file_scalar};

use crate::{
    _core::{
        ETag,
        acl::Acl,
        bucket,
        meta::{
            object::Row as ObjectRow,
            version::{Kind, slot_move_rewinds},
        },
        object, object_lock, storage,
        version::{Id, State, wire_of},
    },
    Error, PgTx, begin,
    meta::{
        LockRow, assert_bucket_row, bucket_lock_enabled, bucket_row_present, decode_lock,
        object::upsert_object, version::StoredState,
    },
    store::PgStore,
};

/// The current slot's guard columns: the identity wire, the sequence and the
/// marker bit, read `FOR UPDATE`. The identity is the wire itself (NULL is the
/// null version), so no `Id` parse is needed to compare it.
struct SlotGuard {
    version_id: Option<String>,
    seq: i64,
    is_delete_marker: bool,
}

impl SlotGuard {
    /// The identity's wire: the stored id, or the literal `null`.
    fn wire(&self) -> String {
        self.version_id.clone().unwrap_or_else(|| "null".to_owned())
    }
}

/// One version row a delete or a lock write addresses: its identity and kind.
struct RowKind {
    id: String,
    kind: Kind,
}

/// Read one version's lock row inside `tx`, taking its row lock. A corrupt or
/// partial row is the contract's `Corrupt` error, never an absent lock.
async fn lock_for_update(
    tx: &mut PgTx,
    bucket: &bucket::Name,
    key: &object::Key,
    wire: &str,
) -> Result<Option<object_lock::Lock>, Error> {
    let row = query_file_as!(
        LockRow,
        "sql/meta/version/lock_get_for_update.sql",
        bucket,
        key,
        wire
    )
    .fetch_optional(&mut **tx)
    .await?;
    row.map_or(Ok(None), LockRow::decode)
}

/// Assert the identity and the two write-path preconditions a lock write
/// requires, inside its transaction (OL5, OL13): the bucket row exists (and is
/// locked first, closing the Read Committed write skew with a concurrent
/// versioning or configuration write), the stored configuration is enabled,
/// versioning is `Enabled`, and the addressed identity is the current slot or
/// an object-version row — a marker, an absent key and an absent version all
/// refuse with `NoSuchVersion`.
///
/// The lock order is the bucket row, then the key's current slot or version
/// row: the same order `advance_slot_with` and `restore_slot` take, so a lock
/// write and a version write cannot deadlock each other.
async fn assert_lockable(
    tx: &mut PgTx,
    bucket: &bucket::Name,
    key: &object::Key,
    wire: &str,
) -> Result<(), Error> {
    if !bucket_row_present(tx, bucket).await? {
        return Err(Error::NoSuchBucket(bucket.clone()));
    }
    if !bucket_lock_enabled(tx, bucket).await? {
        return Err(Error::Storage(storage::invalid_bucket_state(bucket)));
    }
    let state = query_file_as!(StoredState, "sql/meta/version/bucket_state.sql", bucket)
        .fetch_optional(&mut **tx)
        .await?;
    if !matches!(
        state,
        Some(StoredState {
            state: State::Enabled
        })
    ) {
        return Err(Error::Storage(storage::invalid_bucket_state(bucket)));
    }
    let slot = query_file_as!(
        SlotGuard,
        "sql/meta/version/slot_for_update.sql",
        bucket,
        key
    )
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(slot) = slot
        && !slot.is_delete_marker
        && slot.wire() == wire
    {
        return Ok(());
    }
    let seq = sqlx::query_scalar!(
        "SELECT seq FROM version_index WHERE bucket = $1 AND key = $2 AND id = $3",
        bucket,
        key,
        wire
    )
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(seq) = seq {
        let row = query_file_as!(
            RowKind,
            "sql/meta/version/row_for_update.sql",
            bucket,
            key,
            seq
        )
        .fetch_optional(&mut **tx)
        .await?;
        if row.is_some_and(|row| row.kind == Kind::Object) {
            return Ok(());
        }
    }
    Err(Error::Storage(storage::no_such_version(wire)))
}

/// Write (upsert) or clear one version's lock row inside the caller's
/// transaction (OL5): only a lock that carries a fact is stored, so an empty
/// lock removes the row exactly as an explicit clear does.
pub(crate) async fn write_lock_row<'e, E>(
    executor: E,
    bucket: &bucket::Name,
    key: &object::Key,
    wire: &str,
    lock: Option<&object_lock::Lock>,
) -> Result<(), Error>
where
    E: _sqlx::Executor<'e, Database = _sqlx::Postgres>,
{
    match lock.filter(|lock| lock.carries_fact()) {
        Some(lock) => {
            let (mode, retain_until, hold) = lock.columns();
            _sqlx::query_file!(
                "sql/meta/version/lock_upsert.sql",
                bucket,
                key,
                wire,
                mode.as_deref(),
                retain_until,
                hold.as_str()
            )
            .execute(executor)
            .await?;
        }
        None => {
            _sqlx::query_file!("sql/meta/version/lock_clear.sql", bucket, key, wire)
                .execute(executor)
                .await?;
        }
    }
    Ok(())
}

/// Read `wire`'s lock row `FOR UPDATE` and judge a delete against it. The
/// `FOR UPDATE` hold survives the caller's delete, so a concurrent retention
/// write either lands before the check or waits for it.
async fn locked_delete_refusal(
    tx: &mut PgTx,
    bucket: &bucket::Name,
    key: &object::Key,
    wire: &str,
    now: SystemTime,
    bypass: object_lock::Bypass,
) -> Result<Option<object_lock::RefusalReason>, Error> {
    let stored = lock_for_update(tx, bucket, key, wire).await?;
    Ok(object_lock::delete_refusal(stored.as_ref(), now, bypass))
}

/// What [`expire_row`] did for one addressed sequence.
pub(super) enum Verdict {
    /// A version row was removed: an object version with its lock row, or a
    /// delete marker's row alone.
    Removed,
    /// No version row at the sequence: the current slot's identity and its
    /// lock row were cleared (the row-less current version, OL8).
    Cleared,
    /// Nothing named the sequence, or the sequence is not the current slot.
    Absent,
    /// A lock refused the removal; nothing was removed.
    Protected(object_lock::RefusalReason),
}

/// Remove the version at `seq` under its lock, in the caller's transaction
/// (OL8, R19).
///
/// The slot is read `FOR UPDATE` first, then the version row, then the lock
/// row — the crate's one lock order. A row at `seq` is checked through its own
/// identity and kind: an object version's lock is enforced and removed with
/// it, and a delete marker is never observed and never touches a lock row. A
/// sequence with no row resolves the current slot: when it is that slot, the
/// slot's object identity is the version this call deletes, so its lock row is
/// removed too; when it is not, nothing is removed.
pub(super) async fn expire_row(
    tx: &mut PgTx,
    bucket: &bucket::Name,
    key: &object::Key,
    seq: u64,
    bypass: object_lock::Bypass,
    now: SystemTime,
) -> Result<Verdict, Error> {
    let seq_value = seq as i64;
    let slot = query_file_as!(
        SlotGuard,
        "sql/meta/version/slot_for_update.sql",
        bucket,
        key
    )
    .fetch_optional(&mut **tx)
    .await?;
    let row = query_file_as!(
        RowKind,
        "sql/meta/version/row_for_update.sql",
        bucket,
        key,
        seq_value
    )
    .fetch_optional(&mut **tx)
    .await?;
    match row {
        Some(row) => {
            if row.kind == Kind::Object
                && let Some(reason) =
                    locked_delete_refusal(tx, bucket, key, &row.id, now, bypass).await?
            {
                return Ok(Verdict::Protected(reason));
            }
            sqlx::query_file_scalar!("sql/meta/version/expire.sql", bucket, key, seq_value)
                .fetch_optional(&mut **tx)
                .await?;
            // The entry goes only when it still points at this row: two rows
            // can share one identity wire (the null version), and the index
            // names only the newest of them.
            sqlx::query_file!(
                "sql/meta/version/delete_version_index.sql",
                bucket,
                key,
                row.id.as_str(),
                seq_value
            )
            .execute(&mut **tx)
            .await?;
            // R19: a delete marker never observes or removes a lock row.
            if row.kind == Kind::Object {
                sqlx::query_file!(
                    "sql/meta/version/lock_clear.sql",
                    bucket,
                    key,
                    row.id.as_str()
                )
                .execute(&mut **tx)
                .await?;
            }
            Ok(Verdict::Removed)
        }
        None => {
            // Only the addressed sequence that the current slot names is the
            // row-less current version; anything else is an already-removed
            // row (a no-op). A marker is never observed (R19).
            let Some(slot) = slot.filter(|slot| slot.seq == seq_value && !slot.is_delete_marker)
            else {
                return Ok(Verdict::Absent);
            };
            let wire = slot.wire();
            if let Some(reason) = locked_delete_refusal(tx, bucket, key, &wire, now, bypass).await?
            {
                return Ok(Verdict::Protected(reason));
            }
            sqlx::query_file!(
                "sql/meta/version/delete_version_index.sql",
                bucket,
                key,
                wire.as_str(),
                seq_value
            )
            .execute(&mut **tx)
            .await?;
            sqlx::query_file!(
                "sql/meta/version/lock_clear.sql",
                bucket,
                key,
                wire.as_str()
            )
            .execute(&mut **tx)
            .await?;
            Ok(Verdict::Cleared)
        }
    }
}

/// One version row a batch addresses, read `FOR UPDATE` with its kind.
struct SequencedRow {
    seq: i64,
    id: String,
    kind: Kind,
}

/// One lock row a batch addresses, with the identity it belongs to: the three
/// columns [`decode_lock`](crate::meta::decode_lock) turns into a lock.
struct WireLockRow {
    version_id: String,
    mode: Option<String>,
    retain_until: Option<i64>,
    legal_hold: Option<String>,
}

impl WireLockRow {
    /// The identity and its lock; a corrupt row is the contract's `Corrupt`
    /// error, never an absent lock (OL5).
    fn decode(self) -> Result<(String, Option<object_lock::Lock>), Error> {
        let lock = decode_lock(
            self.mode.as_deref(),
            self.retain_until,
            self.legal_hold.as_deref(),
        )?;
        Ok((self.version_id, lock))
    }
}

/// A batch's verdicts and the set-based writes they call for.
///
/// The batch reads once (the slot, the rows, their locks) and decides with the
/// same [`object_lock::may_delete`] the single path uses; this plan is what the
/// caller turns into the three set-based deletes.
pub(super) struct BatchPlan {
    /// The listed sequences whose row is removed, in first-occurrence order.
    pub(super) row_seqs: Vec<i64>,
    /// Every index entry to retire: a removed row's `(seq, id)` and a cleared
    /// slot's `(seq, wire)`.
    pub(super) index_pairs: Vec<(i64, String)>,
    /// The lock rows to remove: a removed object row's identity and a cleared
    /// slot's wire. A marker never names one (R19).
    pub(super) lock_wires: Vec<String>,
    /// The listed sequences a lock refused, in input order.
    pub(super) protected: Vec<(u64, object_lock::RefusalReason)>,
    /// How many version rows the batch removes.
    pub(super) removed: u64,
}

/// The set-based permanent delete: one slot read, one `FOR UPDATE` row read
/// and one lock read for the whole listed history, then the verdicts.
///
/// The work is the sum of [`expire_row`]'s arms, but the reads are the batch's:
/// the slot is read once, the rows are selected with `seq = ANY(...)` in one
/// statement, and their lock rows in one more. The lock order is unchanged —
/// the caller has already locked the bucket row, then this reads the slot, the
/// version rows and the lock rows. A sequence listed twice names one version
/// (the first occurrence decides its place in the answer), so a repeat never
/// removes or reports twice.
///
/// A protected sequence is skipped: it contributes no delete and is reported
/// once, in input order. A corrupt lock row fails closed.
async fn expire_batch(
    tx: &mut PgTx,
    bucket: &bucket::Name,
    key: &object::Key,
    seqs: &[u64],
    now: SystemTime,
) -> Result<BatchPlan, Error> {
    // Each listed sequence is one version, however often the caller lists it:
    // a repeat must not report the same removal or protection twice.
    let mut seen: HashSet<u64> = HashSet::new();
    let mut listed: Vec<i64> = Vec::with_capacity(seqs.len());
    for seq in seqs {
        if seen.insert(*seq) {
            listed.push(*seq as i64);
        }
    }
    // The slot, locked once: the row-less current version's guard.
    let slot = query_file_as!(
        SlotGuard,
        "sql/meta/version/slot_for_update.sql",
        bucket,
        key
    )
    .fetch_optional(&mut **tx)
    .await?;
    // Every addressed row, locked in one statement.
    let rows: Vec<SequencedRow> = query_file_as!(
        SequencedRow,
        "sql/meta/version/rows_for_update_many.sql",
        bucket,
        key,
        &listed
    )
    .fetch_all(&mut **tx)
    .await?;
    let mut by_seq: HashMap<i64, SequencedRow> =
        rows.into_iter().map(|row| (row.seq, row)).collect();
    // The row-less slot arm addresses the slot when no row sits at its
    // sequence; it is the only slot this batch judges.
    let slot_wire = slot
        .as_ref()
        .filter(|slot| {
            !slot.is_delete_marker && listed.contains(&slot.seq) && !by_seq.contains_key(&slot.seq)
        })
        .map(SlotGuard::wire);
    // The lock rows to judge: every object row's identity and the row-less
    // slot's wire. A marker never owns a lock (R19).
    let mut wires: Vec<String> = by_seq
        .values()
        .filter(|row| row.kind == Kind::Object)
        .map(|row| row.id.clone())
        .collect();
    if let Some(wire) = &slot_wire
        && !wires.contains(wire)
    {
        wires.push(wire.clone());
    }
    let mut locks: HashMap<String, Option<object_lock::Lock>> = HashMap::new();
    if !wires.is_empty() {
        let stored: Vec<WireLockRow> = query_file_as!(
            WireLockRow,
            "sql/meta/version/locks_for_update_many.sql",
            bucket,
            key,
            &wires
        )
        .fetch_all(&mut **tx)
        .await?;
        for row in stored {
            let (version_id, lock) = row.decode()?;
            locks.insert(version_id, lock);
        }
    }
    let lock_of = |wire: &str| locks.get(wire).and_then(Option::as_ref);
    let mut plan = BatchPlan {
        row_seqs: Vec::new(),
        index_pairs: Vec::new(),
        lock_wires: Vec::new(),
        protected: Vec::new(),
        removed: 0,
    };
    for seq in listed {
        if let Some(row) = by_seq.remove(&seq) {
            if row.kind == Kind::Object {
                if let Some(reason) =
                    object_lock::delete_refusal(lock_of(&row.id), now, object_lock::Bypass::None)
                {
                    plan.protected.push((seq as u64, reason));
                    continue;
                }
                // R19: a delete marker never observes or removes a lock row.
                plan.lock_wires.push(row.id.clone());
            }
            plan.index_pairs.push((seq, row.id));
            plan.row_seqs.push(seq);
            plan.removed += 1;
        } else if let Some(slot) = slot
            .as_ref()
            .filter(|slot| slot.seq == seq && !slot.is_delete_marker)
        {
            // Only the addressed sequence the current slot names is the
            // row-less current version; a marker is never observed (R19).
            let wire = slot.wire();
            if let Some(reason) =
                object_lock::delete_refusal(lock_of(&wire), now, object_lock::Bypass::None)
            {
                plan.protected.push((seq as u64, reason));
                continue;
            }
            plan.index_pairs.push((seq, wire.clone()));
            plan.lock_wires.push(wire);
        }
    }
    Ok(plan)
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

/// The stamp a slot move owes the row it moves off: a move at `seq` itself moves
/// off nothing, and a sequence holding no row has nothing to stamp. The stamp
/// statement's `became_noncurrent = 0` predicate is the first-stamp-wins rule the
/// retention clock depends on.
async fn stamp_moved_off(
    tx: &mut PgTx,
    bucket: &bucket::Name,
    key: &object::Key,
    seq: i64,
    moved_off: Option<i64>,
) -> Result<(), Error> {
    if let Some(moved_off) = moved_off.filter(|moved_off| *moved_off != seq) {
        sqlx::query_file!(
            "sql/meta/version/stamp_moved_off.sql",
            bucket,
            key,
            moved_off,
            storage::now_nanos() as i64
        )
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

/// The advancing slot write: the upsert that records `(id, seq)`, then the stamp
/// the move owes the row it moves off. The version row at `seq` decides the
/// delete-marker bit, and `advance_slot_upsert.sql` derives it in the same
/// statement.
async fn advance_slot_write(
    tx: &mut PgTx,
    bucket: &bucket::Name,
    key: &object::Key,
    id: Option<Id>,
    seq: i64,
    moved_off: Option<i64>,
) -> Result<(), Error> {
    sqlx::query_file!(
        "sql/meta/version/advance_slot_upsert.sql",
        bucket,
        key,
        id as _,
        seq
    )
    .execute(&mut **tx)
    .await?;
    stamp_moved_off(tx, bucket, key, seq, moved_off).await
}

/// The restoring slot write: the upsert that records `(id, seq, marker)`, then
/// the stamp the move owes the row it moves off. The marker bit is explicit — a
/// restore or a suspended delete carries it — because a marker holds no version
/// row for the statement to derive it from.
async fn restore_slot_write(
    tx: &mut PgTx,
    bucket: &bucket::Name,
    key: &object::Key,
    id: Option<Id>,
    seq: i64,
    marker: bool,
    moved_off: Option<i64>,
) -> Result<(), Error> {
    sqlx::query_file!(
        "sql/meta/version/restore_slot_upsert.sql",
        bucket,
        key,
        id as _,
        seq,
        marker
    )
    .execute(&mut **tx)
    .await?;
    stamp_moved_off(tx, bucket, key, seq, moved_off).await
}

/// Drop one key's slot row. The slot table is this module's; an object removal
/// calls this so the object plane never names `current_slots`.
pub(crate) async fn clear_slot(
    tx: &mut PgTx,
    bucket: &bucket::Name,
    key: &object::Key,
) -> Result<(), Error> {
    sqlx::query!(
        "DELETE FROM current_slots WHERE bucket = $1 AND key = $2",
        bucket,
        key
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}

impl PgStore {
    /// Put `slot` back as the key's current slot, including a backwards move.
    ///
    /// A completion that advanced the slot and then failed has to return the
    /// key to the slot it opened with. `VersionMeta::advance_slot` refuses
    /// that direction; this write does not, and it records the marker bit it
    /// is given rather than re-deriving it from a version row the undo may
    /// already have consumed.
    ///
    /// # Errors
    ///
    /// The driver's answer when the write fails.
    pub async fn write_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<&Id>,
        seq: u64,
        is_delete_marker: bool,
    ) -> Result<(), Error> {
        let wire = wire_of(id);
        sqlx::query_file!(
            "sql/meta/version/restore_slot_upsert.sql",
            bucket,
            key,
            wire,
            seq as i64,
            is_delete_marker
        )
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// Drop the key's slot row. A first versioned write that advanced a slot
    /// and then failed leaves none: there was no slot to put back.
    ///
    /// # Errors
    ///
    /// The driver's answer when the delete fails.
    pub async fn delete_slot(&self, bucket: &bucket::Name, key: &object::Key) -> Result<(), Error> {
        let mut tx = begin(self.pool()).await?;
        clear_slot(&mut tx, bucket, key).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Drop the lock row a completion's slot advance stored.
    ///
    /// The completion's compensation (`take_back` in `tinio-pg`) restores
    /// the head and the slot but the slot writes do not touch lock rows, so a
    /// failed drain after `close_put_write` would leave the create-time intent
    /// behind. This removes exactly that row; an absent row is a no-op.
    ///
    /// # Errors
    ///
    /// The driver's answer when the delete fails.
    pub async fn clear_version_lock(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<(), Error> {
        write_lock_row(self.pool(), bucket, key, &wire_of(version), None).await
    }
}

impl PgStore {
    pub async fn mark_current_marker(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<Id>,
        seq: u64,
        moved_off: Option<u64>,
    ) -> Result<(), Error> {
        let seq_value = seq as i64;
        let mut tx = begin(self.pool()).await?;
        assert_bucket_row(&mut tx, bucket).await?;
        // The slot's own sequence, locked: the move's guard is `advance_slot`'s
        // (a version identity never moves backwards; the null identity is admitted in place).
        let current = query_file_scalar!("sql/meta/version/advance_slot.sql", bucket, key)
            .fetch_optional(&mut *tx)
            .await?;
        if let Some(current) = current
            && slot_move_rewinds(id.as_ref(), seq, current as u64)
        {
            tx.rollback().await?;
            return Err(Error::SlotRewind {
                bucket: bucket.clone(),
                key: key.to_string(),
                seq,
            });
        }
        // The slot write, the marker's bit explicit — a marker holds no row for the upsert to
        // derive it from — and the stamp the move owes the version it moves off, carried by the
        // caller because the delete path cleared the slot before this call.
        restore_slot_write(
            &mut tx,
            bucket,
            key,
            id,
            seq_value,
            true,
            moved_off.map(|moved_off| moved_off as i64),
        )
        .await?;
        // The key's object row is what makes the marker's key hold a current version at all:
        // a key with no row serves nothing (PG23), so the marker materializes the empty row —
        // with the marker's **own creation stamp**, never the epoch `advance_slot`'s
        // materialization writes, because a current marker serves this row's `mtime` as its
        // `LastModified` (fs records the marker's own stamp in its row the same way).
        upsert_object(
            &mut tx,
            bucket,
            key,
            &ObjectRow {
                etag: ETag::EMPTY,
                size: 0,
                mtime: SystemTime::now(),
                identity: 0,
                tags: object::Tags::empty(),
                checksum: None,
                owner: None,
                acl: Acl::default_private(None),
            },
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

// The `VersionMeta` impl is the rest of this module. It lives in its own file
// so this one stays under a thousand lines; a child module keeps the impl's
// helpers private to the plane while giving tools the real file boundary.
mod version_meta;
