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

use std::time::SystemTime;

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
            version::{Current, Kind, Slot, Stored, encode_blob, slot_move_rewinds},
        },
        object, storage,
        version::{Id, State, wire_of},
    },
    Error, PgTx, begin,
    meta::{
        acl_composite, assert_bucket_row,
        object::{object_row, upsert_object},
    },
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

/// The slot read's columns, in `current_slot.sql`'s order: the sequence the
/// slot records, the current version's identity wire (which the left join
/// leaves NULL for a slot whose row is gone), the bit the slot's own record
/// carries (a restored marker outlives the row it came from), and the row's
/// kind, NULL when the row is gone. The row's kind decides when the row is
/// there; the recorded bit answers when it is not.
struct StoredSlot {
    seq: i64,
    version_id: Option<String>,
    is_delete_marker: bool,
    kind: Option<Kind>,
}

/// One row of `bucket_listing.sql`: a key's object row and slot, then one of
/// its version rows -- the whole of what a version listing serves a key, read
/// bucket-wide in one statement. A key with no version row (or none the two
/// joins matched) answers a single row whose version columns are NULL.
///
/// The field order is the column order -- the macro builds the struct from the
/// statement's own columns -- so the decode below is a typed conversion, not a
/// positional read.
struct StoredListing {
    key: String,
    etag: String,
    size: i64,
    mtime: OffsetDateTime,
    identity: i64,
    tags: object::Tags,
    checksum: String,
    acl: Acl,
    slot_seq: Option<i64>,
    version_id: Option<String>,
    is_delete_marker: Option<bool>,
    row_seq: Option<i64>,
    id: Option<String>,
    blob: Option<String>,
    row_size: Option<i64>,
    row_etag: Option<String>,
    created: Option<OffsetDateTime>,
    became_noncurrent: Option<i64>,
    row_kind: Option<Kind>,
    row_tags: Option<object::Tags>,
    row_checksum: Option<String>,
    row_acl: Option<Acl>,
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

impl StoredListing {
    /// The key's object row, decoded by [`object_row`] -- the strict walk's rule, so a
    /// listing refuses a row whose elements cannot be read rather than serving the key as
    /// empty.
    fn elements(&self) -> Option<ObjectRow> {
        object_row(
            &self.etag,
            self.size,
            self.mtime,
            self.identity,
            self.tags.clone(),
            &self.checksum,
            self.acl.clone(),
        )
    }

    /// The key's current version as the gate resolves it for a key whose object row is there:
    /// the sequence the slot records (0 for a key with no slot row, the state a bucket that
    /// never recorded a version leaves), the recorded identity, and the marker bit.
    ///
    /// The bit comes from the version row **at the slot's own sequence** when the key holds one
    /// -- `current_slot.sql`'s rule, which this row reproduces because it carries every version
    /// row rather than the one the slot names -- and from the slot's recorded bit when it does
    /// not (a restored marker outlives the row it came from). The gate and this listing
    /// therefore answer one slot the same way, which is what a second reader of the same state
    /// owes the first.
    fn current(&self) -> (u64, Option<Id>, bool) {
        let at_slot = match (self.slot_seq, self.row_seq) {
            (Some(slot), Some(row)) if slot == row => self.row_kind,
            _ => None,
        };
        (
            self.slot_seq.unwrap_or(0) as u64,
            self.version_id.as_deref().and_then(Id::from_wire),
            at_slot.map_or(self.is_delete_marker.unwrap_or(false), Kind::is_marker),
        )
    }

    /// This row's version row as the contract's own. `None` is the join's half-empty row -- a
    /// key the statement served without a version row -- and a domain-invalid blob or
    /// identifier, which self-heals as absent ([`Stored::from_values`]' rule, the same decode
    /// `list_versions` applies) rather than failing the listing.
    fn stored(&self) -> Option<Stored> {
        Stored::from_values(
            self.row_seq? as u64,
            self.id.as_deref()?,
            self.blob.as_deref()?,
            self.row_size? as u64,
            self.row_etag.as_deref()?,
            from_timestamp(self.created?),
            self.became_noncurrent? as u64,
            self.row_kind?,
            self.row_tags.clone()?,
            self.row_checksum.as_deref()?,
            self.row_acl.clone()?,
        )
    }
}

/// One key's versions as [`PgStore::bucket_versions`] returns them: the key, the facts its
/// object row and its slot record for the **current** version, and its version rows newest
/// first -- the shape a `ListObjectVersions` page is cut from, one key at a time.
///
/// The current version is read from the object row and the slot, never from a version row of
/// its own: a delete marker's row carries the empty elements `advance_slot` materialized, so no
/// stale element of the object the marker hid can be served as the current version's.
pub struct KeyVersions {
    /// The key the versions belong to.
    pub key: object::Key,
    /// The current version's recorded identity; `None` is the null version.
    pub version: Option<Id>,
    /// The sequence the key's slot records, or 0 for a key with no slot row.
    pub seq: u64,
    /// Whether the current version is a delete marker.
    pub is_delete_marker: bool,
    /// The current version's recorded elements: this key's object row.
    pub elements: ObjectRow,
    /// The key's history, newest first: every version row that is not the current version's own
    /// ([`Stored::is_history`], the contract's own predicate).
    pub history: Vec<Stored>,
}

impl KeyVersions {
    /// The entry a key's first listing row opens: its object row's elements beside the facts
    /// the slot records, and the version row that same row carries (a key's newest joined row
    /// is the current version's own where the slot names it, and history where it does not).
    fn open(row: &StoredListing, key: object::Key, bucket: &bucket::Name) -> Result<Self, Error> {
        let elements = row.elements().ok_or_else(|| Error::Corrupt {
            bucket: bucket.clone(),
            key: key.to_string(),
        })?;
        let (seq, version, is_delete_marker) = row.current();
        let mut opened = Self {
            key,
            version,
            seq,
            is_delete_marker,
            elements,
            history: Vec::new(),
        };
        opened.absorb(row);
        Ok(opened)
    }

    /// Absorb one further listing row of this key: the version row it carries when that row is
    /// history. A row at the slot's own sequence is the current version's own row (a marker's),
    /// so it is never listed twice.
    fn absorb(&mut self, row: &StoredListing) {
        let at = Slot::At(self.seq);
        if let Some(stored) = row.stored()
            && stored.is_history(at)
        {
            self.history.push(stored);
        }
    }
}

impl PgStore {
    /// Make a key's current version a **delete marker that holds no version row**: the slot
    /// names `(id, seq)` and records the marker bit, and the key's object row is materialized
    /// with the marker's own creation stamp (a current marker serves that row's `mtime` as its
    /// `LastModified`, so it must be the marker's, never the epoch).
    ///
    /// The contract's [`VersionMeta::put_version`] cannot express this state, and the reason is
    /// a rule the contract states about itself: a **null-identity** row displaces the key's
    /// other null-id row (the one-null-version model). A suspended `DELETE` makes a marker
    /// whose identity **is** the null version, and the key usually still holds the superseded
    /// null version as its own history row — the state fs's and mem's suites pin, where the
    /// marker must not consume it. PostgreSQL's slot is where the marker's facts belong (a
    /// delete marker is exactly a recorded slot with no bytes), so this call writes them there:
    /// the same statement `restore_slot` writes its slot with, carrying the bit explicitly,
    /// and the same monotonic guard `advance_slot` applies.
    ///
    /// `moved_off` is the sequence of the version this marker supersedes — the row the slot
    /// moves off — stamped noncurrent in this same transaction (the RG5 rule `advance_slot`
    /// and `restore_slot` apply). The caller carries it because the versioned delete clears
    /// the key's slot through `ObjectMeta::remove` **before** this call, so the slot read
    /// below is `None` and the moved-off fact is otherwise gone. `None` means the marker
    /// moves off nothing (a first delete, or a bucket whose delete superseded no real
    /// version).
    ///
    /// A current version carries no index entry, so none is written; the identity is read from
    /// the slot by every caller that needs it ([`VersionMeta::current_slot`] answers it, and
    /// [`VersionMeta::advance_slot`] derives the bit from a row only when one is there).
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

    /// The key's slot row in one read: the sequence it records beside the contract's
    /// [`Current`] facts. `None` for a key with no slot.
    ///
    /// A gate resolve needs both facts together — the sequence a write's monotonic move plans
    /// from (the contract's [`VersionMeta::current_slot`] shape carries none) and the identity
    /// and marker bit [`Current`] carries — so they are one statement rather than the two
    /// sequential reads they used to be.
    pub async fn slot_record(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<(u64, Current)>, Error> {
        let row = query_file_as!(StoredSlot, "sql/meta/version/current_slot.sql", bucket, key)
            .fetch_optional(self.pool())
            .await?;
        Ok(row.map(|row| {
            (
                row.seq as u64,
                Current {
                    exists: true,
                    version: row.version_id.as_deref().and_then(Id::from_wire),
                    // The row's kind decides when the row is there — a slot must never
                    // be overridden by a stale recorded bit — and the recorded bit
                    // answers when the row is gone, which is the state a restore
                    // leaves (it consumes the row it derived the bit from).
                    is_delete_marker: row.kind.map_or(row.is_delete_marker, Kind::is_marker),
                },
            )
        }))
    }

    /// A bucket's versions, one key at a time: every key the bucket holds, each with the facts
    /// its slot records and its version rows newest first -- one key per group of
    /// [`KeyVersions`], read in **one** statement.
    ///
    /// Resolving a listing one key at a time cost a slot read and a version-row read per key,
    /// so a page holding a handful of keys paid `1 + 2N` round trips and the count grew with the
    /// bucket. The statement reads the same rows either way -- a listing is the bucket's own
    /// read, however it is cut -- so what is bounded here is the round trips a page pays, which
    /// is the cost the caller sees.
    ///
    /// The statement's own order is the listing's: keys ascending, each key's version rows
    /// newest first, so the caller cuts its page without sorting.
    pub async fn bucket_versions(&self, bucket: &bucket::Name) -> Result<Vec<KeyVersions>, Error> {
        let rows = query_file_as!(StoredListing, "sql/meta/version/bucket_listing.sql", bucket)
            .fetch_all(self.pool())
            .await?;
        let mut versions: Vec<KeyVersions> = Vec::new();
        for row in rows {
            let key = object::key(&row.key).map_err(|_| Error::Corrupt {
                bucket: bucket.clone(),
                key: row.key.clone(),
            })?;
            // Every row of one key arrives together, the statement ordering by key: the key's
            // object row and slot are read where the key first appears, and the rows that
            // follow contribute their own version row alone.
            match versions.last_mut() {
                Some(open) if open.key == key => open.absorb(&row),
                _ => versions.push(KeyVersions::open(&row, key, bucket)?),
            }
        }
        Ok(versions)
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
        // The slot row's own decode is `slot_record`'s, so this and the gate cannot disagree:
        // the sequence it also reads is simply dropped here.
        Ok(self
            .slot_record(bucket, key)
            .await?
            .map(|(_seq, current)| current))
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
        // The slot write and the stamp the move owes the row it moves off, in this one
        // transaction (the 2026-10-09 ruling): the supersede committed the outgoing version's
        // row with `became_noncurrent == 0` — the crash window's adopted row, not history yet —
        // and this commit records when it stopped being current, so a row that is history says
        // so and its retention clock starts when the version actually stopped being current.
        // `current` is the slot's sequence read `FOR UPDATE` above, before this write moved it.
        advance_slot_write(&mut tx, bucket, key, id, seq_value, current).await?;
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
    /// (the `is_delete_marker` column in `sql/migrations/0001_init.sql`) is what
    /// `current_slot` answers from afterwards. The
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
        let v2 = Id::new("v2").expect("a valid version id");
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
                VersionMeta::advance_slot(&store, &data, &target, Some(v2), 2).await,
                Err(Error::SlotRewind { .. })
            ),
            "a real identity's slot never moves backwards"
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

    /// The 2026-10-10 identity-scoping ruling, on the one path that reaches it
    /// in production: a suspended bucket's in-place write to the **null** slot
    /// is not a backward move. `plan_write`'s `(Put, Suspended)` arm over a real
    /// current version answers `(Identity::Null, seq 0)`, and the engine's
    /// advance runs **after** the supersede, the publish and the object-row
    /// write have committed — so the pre-ruling guard refused a write that had
    /// already taken effect and the scenario answered 500. The contract's
    /// `advance_slot` doc states the rule ("An incoming `None` (the null
    /// version) is admitted in place whatever the sequence … `(Some(id), n) →
    /// (None, 0)` is not a backward move"), redb carries it (`id.is_some()`),
    /// and `mark_current_marker` already carried it here. The companion leg
    /// pins that admitting the null write does not weaken the guard.
    #[tokio::test]
    async fn advance_slot_admits_the_null_identitys_in_place_move() {
        let Some((db, store)) = scratch().await else {
            return;
        };
        let data = name("plane-version-null-in-place");
        bucket_row(&store, &data).await;
        let target = key("suspended.txt");
        let v1 = Id::new("v1").expect("a valid version id");
        let v2 = Id::new("v2").expect("a valid version id");
        // An enabled write's slot, high on the key: a real identity at seq 5,
        // whose row the supersede committed (`became_noncurrent == 0`, the
        // crash window's adopted row).
        VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 5))
            .await
            .unwrap();
        VersionMeta::advance_slot(&store, &data, &target, Some(v1), 5)
            .await
            .unwrap();
        // The companion first: a lower-sequence move that keeps a **real**
        // identity is still the backward move the guard exists to refuse.
        assert!(
            matches!(
                VersionMeta::advance_slot(&store, &data, &target, Some(v2), 2).await,
                Err(Error::SlotRewind { .. })
            ),
            "a real identity never moves backwards"
        );
        // The suspended bucket's in-place write: the plan's `(Null, 0)` over a
        // real current version lands rather than being refused.
        VersionMeta::advance_slot(&store, &data, &target, None, 0)
            .await
            .expect("the null version's in-place slot move is not a rewind");
        let slot = VersionMeta::current_slot(&store, &data, &target)
            .await
            .unwrap()
            .expect("the slot is recorded");
        assert!(
            slot.exists && slot.version.is_none() && !slot.is_delete_marker,
            "the slot records the null identity: {slot:?}"
        );
        // The in-place move stamps the real version it moved off, in the same
        // commit: the null write supersedes the row at seq 5, so that row is
        // history (redb's own suite pins the same arm).
        assert_ne!(
            VersionMeta::get_version(&store, &data, &target, 5)
                .await
                .unwrap()
                .expect("the version the null write superseded")
                .became_noncurrent,
            0,
            "the null write's in-place move stamps the version it moved off"
        );
        db.drop_db().await;
    }

    /// The advance stamps the row it moves off (the 2026-10-09 ruling, the rule
    /// [`VersionMeta::restore_slot`] shares — RG5). The supersede commits the
    /// outgoing version's row with `became_noncurrent == 0` (the crash window's
    /// adopted row, not history yet), and the transaction that records the
    /// incoming identity stamps it in the **same commit**, so a row that is
    /// history says so and its retention clock starts when the version actually
    /// stopped being current. redb pins the same rule in its own suite; pg had
    /// the restore half (`restore_slot`'s stamp) but not the forward half, and
    /// no leg looked — this is the missing one.
    #[tokio::test]
    async fn advance_slot_stamps_the_row_it_moves_off() {
        let Some((db, store)) = scratch().await else {
            return;
        };
        let data = name("plane-advance-stamp");
        bucket_row(&store, &data).await;
        let target = key("stamp.txt");
        let v1 = Id::new("v1").expect("a valid version id");
        let v2 = Id::new("v2").expect("a valid version id");
        // Two rows, each committed the way a supersede commits them: the
        // outgoing current version at seq 1, the incoming at seq 2, both with
        // `became_noncurrent == 0`.
        VersionMeta::put_version(&store, &data, &target, &version(Some(&v1), 1))
            .await
            .unwrap();
        VersionMeta::put_version(&store, &data, &target, &version(Some(&v2), 2))
            .await
            .unwrap();
        // The slot records the current version's own row: not history yet.
        VersionMeta::advance_slot(&store, &data, &target, Some(v1), 1)
            .await
            .unwrap();
        assert_eq!(
            VersionMeta::get_version(&store, &data, &target, 1)
                .await
                .unwrap()
                .expect("the current version's own row")
                .became_noncurrent,
            0,
            "the row the slot records is the current version's: not history yet"
        );
        // The advance moves the slot off seq 1: that row stopped being current
        // in this commit, so it carries the stamp. The incoming version's own
        // row at seq 2 does not.
        VersionMeta::advance_slot(&store, &data, &target, Some(v2), 2)
            .await
            .unwrap();
        assert_ne!(
            VersionMeta::get_version(&store, &data, &target, 1)
                .await
                .unwrap()
                .expect("the moved-off row stays")
                .became_noncurrent,
            0,
            "the advance stamps the row it moves off"
        );
        assert_eq!(
            VersionMeta::get_version(&store, &data, &target, 2)
                .await
                .unwrap()
                .expect("the incoming version's row")
                .became_noncurrent,
            0,
            "the incoming version's own row is not history"
        );
        // The same-sequence re-run is the crash window's own commit: it moves
        // off nothing, so the current version's row stays unstamped.
        VersionMeta::advance_slot(&store, &data, &target, Some(Id::new("v2").unwrap()), 2)
            .await
            .unwrap();
        assert_eq!(
            VersionMeta::get_version(&store, &data, &target, 2)
                .await
                .unwrap()
                .expect("the current version's row")
                .became_noncurrent,
            0,
            "a same-sequence re-run does not stamp the current version's row"
        );
        // A move off a sequence holding no row is a no-op, not an error.
        VersionMeta::advance_slot(&store, &data, &target, None, 9)
            .await
            .unwrap();
        assert!(
            VersionMeta::get_version(&store, &data, &target, 9)
                .await
                .unwrap()
                .is_none(),
            "an advance onto a free sequence writes no version row"
        );
        db.drop_db().await;
    }

    /// The `ObjectMeta::remove`/`remove_many` slot clear: a slot **is** object-row
    /// state (the contract's own `advance_slot` rule), so removing a key's row must
    /// remove its slot — a row-less slot would make the next advance from below the
    /// stale sequence a `SlotRewind`. The shared conformance suite cannot pin this
    /// (the slot is one store's shape), so the store's own suite does; `Cleanup`'s
    /// batch retirement is the production caller of the batch half.
    #[tokio::test]
    async fn removing_a_row_removes_the_key_slot_it_belongs_to() {
        let Some((db, store)) = scratch().await else {
            return;
        };
        let data = name("plane-remove-slot");
        bucket_row(&store, &data).await;
        let target = key("removed.txt");
        // A slot well above the sequence the next write plans with.
        VersionMeta::advance_slot(
            &store,
            &data,
            &target,
            Some(Id::new("v1").expect("a valid version id")),
            3,
        )
        .await
        .unwrap();
        assert!(ObjectMeta::remove(&store, &data, &target).await.unwrap());
        assert!(
            VersionMeta::current_slot(&store, &data, &target)
                .await
                .unwrap()
                .is_none(),
            "removing the object row removes the key's slot"
        );
        // The next write plans from zero and is not refused as a rewind.
        VersionMeta::advance_slot(
            &store,
            &data,
            &target,
            Some(Id::new("v2").expect("a valid version id")),
            1,
        )
        .await
        .unwrap();

        // The batch clear, in the one transaction the contract states.
        let second = key("removed-too.txt");
        VersionMeta::advance_slot(
            &store,
            &data,
            &second,
            Some(Id::new("v3").expect("a valid version id")),
            5,
        )
        .await
        .unwrap();
        ObjectMeta::remove_many(&store, &data, &[target.clone(), second.clone()])
            .await
            .unwrap();
        for key in [&target, &second] {
            assert!(
                VersionMeta::current_slot(&store, &data, key)
                    .await
                    .unwrap()
                    .is_none(),
                "a batch removal clears every key's slot"
            );
        }
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
