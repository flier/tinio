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
//! [`VersionMeta::advance_slot`] is its forward contract writer: it records the
//! identity and the sequence, and derives the marker bit from the version row
//! at that sequence in the same transaction, so the recorded flag and the
//! contract's rule (the **version row's kind** decides the marker) cannot
//! disagree for a slot the contract wrote. That transaction is also the one
//! that stamps the row it moves off noncurrent (the 2026-10-09 ruling): it
//! records when the superseded version stopped being current, in the same
//! commit, so a row that is history says so
//! ([`crate::version::Row::is_history`]) and the retention clock never starts
//! before it did. [`VersionMeta::restore_slot`] is the slot's other writer —
//! the restore's commit, the one backwards move to a real identity — and it
//! writes the same three columns plus the consumed row's kind as the marker
//! bit. [`VersionMeta::current_slot`] resolves the marker from the column or
//! from the version row at the slot's sequence, so a restored marker's bit
//! outlives the row it came from, and answers `None` only when the key has no
//! `object_meta` row at all.

use std::{io, time::SystemTime};

use async_trait::async_trait;
use tracing::instrument;

use crate::{
    _core::{
        ETag,
        acl::Acl,
        bucket,
        meta::{
            VersionMeta,
            object::Row as CallerRow,
            version::{
                Current, ExpireReport, Expired, Kind as ContractKind, Stored, slot_move_rewinds,
            },
        },
        object, object_lock, storage, to_nanos,
        version::{self as contract, Id, State},
    },
    Error, RedbMetadata,
    bucket::Table as BucketTable,
    bucket_object_lock::Table as ConfigTable,
    bucket_versioning::Table as StateTable,
    meta::version_lock::{self, LockOwned},
    object_meta::{Stored as ObjectRow, Table as ObjectTable},
    object_part::Table as PartTable,
    version::{Kind as RowKind, Row as VersionRow, Table as VersionTable},
    version_index::Table as IndexTable,
    version_object_lock::{self, Table as LockTable},
};

/// The table kind of a contract row's kind: the two enumerations carry the same
/// two wires (the contract's `Kind::as_wire` produces exactly what the table's
/// `Kind` parses), so the mapping is total — a contract-side kind the table
/// cannot store is a compile error here, never a runtime parse failure
/// (style.md §Panics: eliminate over annotate).
fn table_kind(kind: ContractKind) -> RowKind {
    match kind {
        ContractKind::Object => RowKind::Object,
        ContractKind::Marker => RowKind::Marker,
    }
}

/// Encode one contract row into the table layer's row: the elements
/// [`Stored::to_raw`] emits are the table's value tuple in order — the kind
/// element among them is the typed field's [`table_kind`] mapping, the same
/// wire — and the row's sequence is the key's third element.
fn table_row(stored: &Stored) -> VersionRow {
    let (id, blob, size, etag, created, became_noncurrent, _kind, tags, checksum, owner, acl) =
        stored.to_raw();
    VersionRow {
        id,
        blob,
        size,
        etag,
        created,
        became_noncurrent,
        kind: table_kind(stored.kind),
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

/// The `object_meta` row a restore installs: the caller's elements, the slot
/// columns naming `(id, seq)` — the contract's object row carries no version
/// columns (D37), so the restore's caller cannot hand them in — and the marker
/// bit the consumed version row's [`RowKind`] decided, never the caller.
fn restored_row(caller: &CallerRow, id: Option<Id>, seq: u64, marker: bool) -> ObjectRow {
    ObjectRow {
        etag: caller.etag.clone(),
        size: caller.size,
        mtime: to_nanos(caller.mtime),
        file_identity: caller.identity,
        tags: caller.tags.clone(),
        checksum: caller.checksum.clone(),
        owner: caller.owner.clone(),
        acl: caller.acl.clone(),
        version_id: id,
        version_seq: seq,
        is_delete_marker: marker,
    }
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
    /// (D22's refusal projection, the `put_guarded` shape). R21: the same
    /// transaction refuses `Suspended` while an enabled Object Lock
    /// configuration exists — a suspend could otherwise commit between a
    /// retention write's configuration and state reads (OL13).
    async fn set_bucket_state_guarded(
        &self,
        bucket: &bucket::Name,
        state: State,
    ) -> Result<(), Error> {
        let refusal = storage::no_such_bucket(bucket);
        let suspended = storage::invalid_bucket_state(bucket);
        let bucket = bucket.to_string();
        self.driver
            .write(move |txn| -> Result<(), Error> {
                if !BucketTable::open(txn)?.exists(&bucket)? {
                    return Err(Error::Refused(refusal));
                }
                if state == State::Suspended && ConfigTable::open(txn)?.is_enabled(&bucket)? {
                    return Err(Error::Refused(suspended));
                }
                StateTable::open(txn)?.set(&bucket, state)
            })
            .await
    }

    /// The key's current slot. The marker bit is the object row's own
    /// `is_delete_marker` column **or** the version row's kind at the slot's
    /// sequence: both writers of a contract slot derive the bit from the same
    /// row, so the two agree for every slot the contract wrote, and the column
    /// is what answers after a **restore** — this call consumes the version row
    /// it derived the bit from, and the restored marker must not read back as
    /// an object version. The version-row clause stays for the rows a slot
    /// names without the column being written from them, and a slot whose row
    /// is gone (an out-of-band expiry) records no bit, so it is not a marker.
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
            let marker = row.is_delete_marker
                || VersionTable::open_readonly(txn)?
                    .get(&bucket, &key, row.version_seq)?
                    .is_some_and(|version| version.kind == RowKind::Marker);
            Ok(Some(Current {
                exists: true,
                version: row.version_id,
                is_delete_marker: marker,
            }))
        })
    }

    #[instrument(
        name = "advance_slot",
        target = "tinio::storage",
        level = "info",
        skip(self),
        fields(
            bucket = %bucket,
            key = %key,
            version = id.as_deref().map_or("null", String::as_str),
        )
    )]
    async fn advance_slot_with(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<Id>,
        seq: u64,
        lock: Option<&object_lock::Lock>,
    ) -> Result<(), Error> {
        let refusal = storage::no_such_bucket(bucket);
        let bucket = bucket.to_string();
        let key = key.to_string();
        let lock = lock.cloned();
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
                // A key with no `object_meta` row gets one here: the slot
                // columns need a home, and the fabricated row's content-derived
                // elements start empty (see `empty_object_row`). The effect is
                // the deliberate rule the contract documents — an
                // `ObjectMeta::get`/`walk` read then reports a recorded object
                // for a key whose content side holds nothing.
                let recorded = ObjectTable::open(txn)?.get(&bucket, &key)?;
                // OL7: a null in-place replacement of an identity that already
                // owns an active lock is a protected overwrite. The identity
                // the slot records is the one this write would replace; a
                // marker owns no lock (R19), and an ordinary overwrite creates
                // a new identity, leaving the old lock intact.
                if id.is_none()
                    && let Some(slot) = recorded.as_ref().filter(|slot| !slot.is_delete_marker)
                {
                    let wire = contract::wire_of(slot.version_id.as_ref());
                    if let Some(reason) = version_lock::locked_wire_reason(
                        txn,
                        &bucket,
                        &key,
                        &wire,
                        object_lock::Bypass::None,
                        SystemTime::now(),
                    )? {
                        tracing::warn!(
                            bucket = %bucket,
                            key = %key,
                            version = %wire,
                            reason = %reason,
                            "refusing a protected in-place overwrite"
                        );
                        return Err(Error::Refused(storage::locked(reason)));
                    }
                }
                // The sequence of the slot this advance moves off: the superseded
                // version's own row. A key with no object row has no **recorded**
                // slot — the fabricated row above is only a home for the columns
                // — so this advance moves off nothing, exactly as it does when
                // the sequence holds no row at all.
                let moved_off = recorded.as_ref().map(|row| row.version_seq);
                let mut row = recorded.unwrap_or_else(empty_object_row);
                // The slot never moves backwards within one identity (see
                // `slot_move_rewinds`): a lower-sequence advance of a real
                // version is refused exactly as `put_version` refuses a row
                // another version already occupies.
                if slot_move_rewinds(id.as_ref(), seq, row.version_seq) {
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
                let wire = contract::wire_of(id.as_ref());
                row.version_id = id;
                row.version_seq = seq;
                row.is_delete_marker = marker;
                ObjectTable::open(txn)?.put(&bucket, &key, &row)?;
                // OL7: the lock commits with the current version; the lock
                // writer owns the empty-lock and marker rules.
                version_lock::put_advance_lock(txn, &bucket, &key, &wire, lock.as_ref(), marker)?;
                // The stamp the move owes the row it moves off (the 2026-10-09
                // ruling): the supersede commits the outgoing version's row with
                // `became_noncurrent == 0` — the crash window's adopted row, not
                // history yet — and this transaction, the one that records the
                // new current version, records when it stopped being current, in
                // the **same commit**. No crash between the two can leave a row
                // that is history without saying so, and the retention clock
                // starts when the version actually stopped being current.
                //
                // The first stamp wins: a row that already carries one keeps the
                // moment it stopped being current, so a re-run never moves the
                // retention clock, and a store that stamps the row where it
                // creates it (mem's supersede — the double stamp the contract
                // allows) is not contradicted here. A move at the **same**
                // sequence moves off nothing — the row the slot names is still
                // the current version's own, and stamping it would read as
                // history without a slot to qualify it — and a sequence holding
                // no row has nothing to stamp.
                if let Some(moved_off) = moved_off.filter(|moved_off| *moved_off != seq) {
                    let mut versions = VersionTable::open(txn)?;
                    if versions
                        .get(&bucket, &key, moved_off)?
                        .is_some_and(|superseded| superseded.became_noncurrent == 0)
                    {
                        versions.stamp_noncurrent(
                            &bucket,
                            &key,
                            moved_off,
                            to_nanos(SystemTime::now()),
                        )?;
                    }
                }
                Ok(())
            })
            .await
    }

    /// The restore's commit: the bucket row is probed in this transaction
    /// first; the version row at `seq` is read **in** this transaction and is
    /// the call's premise (it must exist and name `id` — its kind is the
    /// object row's marker bit, never the caller's); the row and its index
    /// entry are consumed; the object row is written wholesale from the
    /// caller's row plus the slot columns, with the retained part list
    /// cleared; and the row the slot moves off is stamped, all in the one
    /// commit. No monotonic guard applies: the backwards move to a real
    /// identity is this call's own definition, so
    /// [`VersionMeta::advance_slot`]'s rule stays absolute.
    #[instrument(
        target = "tinio::storage",
        level = "info",
        skip(self, row),
        fields(
            bucket = %bucket,
            key = %key,
            version = id.as_deref().map_or("null", String::as_str),
            bytes = row.size,
        )
    )]
    async fn restore_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &CallerRow,
        id: Option<Id>,
        seq: u64,
    ) -> Result<(), Error> {
        let refusal = storage::no_such_bucket(bucket);
        let bucket = bucket.to_string();
        let key = key.to_string();
        let row = row.clone();
        let wire = contract::wire_of(id.as_ref());
        self.driver
            .write(move |txn| {
                // The bucket row is probed in this transaction, first, exactly
                // as the object-row guarded writers and `advance_slot` probe
                // it: the restore writes an object row, and one for a bucket
                // that is gone must not outlive it.
                if !BucketTable::open(txn)?.exists(&bucket)? {
                    return Err(Error::Refused(refusal));
                }
                // The version row at `seq` is the call's premise (the caller
                // named a version it had just read): it must exist and it must
                // name the identity the caller hands in, else the refusal —
                // never a slot naming a version the store does not hold. Its
                // kind supplies the object row's marker bit: the slot is a
                // version-row fact, not a kind the caller carries.
                let Some(consumed) = VersionTable::open(txn)?.get(&bucket, &key, seq)? else {
                    tracing::warn!(
                        bucket = %bucket,
                        key = %key,
                        seq,
                        "refusing to restore a version row that is absent"
                    );
                    return Err(Error::Refused(storage::io(io::Error::other(
                        "a restore names a version row that is absent",
                    ))));
                };
                if consumed.id != wire {
                    tracing::warn!(
                        bucket = %bucket,
                        key = %key,
                        seq,
                        consumed = %consumed.id,
                        named = %wire,
                        "refusing to restore a version row of another identity"
                    );
                    return Err(Error::Refused(storage::io(io::Error::other(
                        "a restore names a version row of another identity",
                    ))));
                }
                let marker = consumed.kind == RowKind::Marker;
                // The row the slot moves off: the recorded slot's own sequence,
                // read before the object row is replaced. A key with no object
                // row has no recorded slot, so this move (like `advance_slot`'s)
                // moves off nothing.
                let moved_off = ObjectTable::open(txn)?
                    .get(&bucket, &key)?
                    .map(|recorded| recorded.version_seq);
                // The consumed row and its index entry retire in this same
                // transaction: the restored version IS the current version, so
                // it has no row of its own — leaving one would make it both
                // current and addressable as history. The entry goes only when
                // it still points at this row (`expire_many`'s rule).
                VersionTable::open(txn)?.remove(&bucket, &key, seq)?;
                IndexTable::open(txn)?.remove_pointing_at(&bucket, &key, &consumed.id, seq)?;
                // The object row, wholesale: the caller's elements, the slot
                // columns naming `(id, seq)` and the kind-derived marker bit —
                // the caller carries no version columns (D37).
                ObjectTable::open(txn)?.put(&bucket, &key, &restored_row(&row, id, seq, marker))?;
                // A write is a fresh object: the retained part list goes in the
                // same transaction (`ObjectMeta::put`'s rule).
                PartTable::open(txn)?.remove_key(&bucket, &key)?;
                // The stamp the move owes the row it moves off, under
                // `advance_slot`'s rule and its two exemptions exactly: a move
                // at the same sequence moves off nothing, and a sequence holding
                // no row has nothing to stamp. The first stamp wins.
                if let Some(moved_off) = moved_off.filter(|moved_off| *moved_off != seq) {
                    let mut versions = VersionTable::open(txn)?;
                    if versions
                        .get(&bucket, &key, moved_off)?
                        .is_some_and(|superseded| superseded.became_noncurrent == 0)
                    {
                        versions.stamp_noncurrent(
                            &bucket,
                            &key,
                            moved_off,
                            to_nanos(SystemTime::now()),
                        )?;
                    }
                }
                Ok(())
            })
            .await
    }

    #[instrument(
        target = "tinio::storage",
        level = "info",
        skip(self, row),
        fields(
            bucket = %bucket,
            key = %key,
            version = row.id.as_deref().map_or("null", String::as_str),
            kind = %row.kind,
            bytes = row.size,
        )
    )]
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

    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let bucket = bucket.to_string();
        self.driver
            .write(move |txn| {
                // OL8: the drain refuses, removing nothing, while any version
                // row of the bucket is protected. The check runs before the
                // first removal; an orphan lock row (one that resolves to no
                // version) never refuses — the drain removes it. R19: a lock
                // row a marker owns is exactly such an orphan — a marker is
                // never a `may_delete` subject, so its row protects nothing
                // here and leaves with the rest.
                let now = SystemTime::now();
                // The lock rows are collected first: redb hands out one
                // writable table at a time from a transaction, and the check
                // needs the version and index tables beside the lock rows.
                let rows: Vec<(String, String, Option<String>, Option<i64>, Option<String>)> = {
                    let locks = LockTable::open(txn)?;
                    let mut rows = Vec::new();
                    locks.for_bucket(&bucket, |key, wire, mode, until, hold| {
                        rows.push((
                            key.to_owned(),
                            wire.to_owned(),
                            mode.map(str::to_owned),
                            until,
                            hold.map(str::to_owned),
                        ));
                        Ok(())
                    })?;
                    rows
                };
                let mut protected = None;
                for (key, wire, mode, until, hold) in rows {
                    // [`LockOwned`] is the ownership rule's one home (OL7/
                    // OL8, R19); only a lock that resolves to neither an
                    // object version row nor the key's non-marker current
                    // slot is an orphan the drain removes.
                    if !txn.lock_owned(&bucket, &key, &wire)? {
                        continue;
                    }
                    let lock =
                        version_object_lock::decode(mode.as_deref(), until, hold.as_deref())?;
                    if let Some(reason) =
                        object_lock::delete_refusal(lock.as_ref(), now, object_lock::Bypass::None)
                    {
                        protected = Some(reason);
                    }
                }
                if let Some(reason) = protected {
                    return Err(Error::Refused(storage::locked(reason)));
                }
                // The rows and their index entries retire in **one** write
                // transaction (the plan's Global Constraint): the index can
                // never be left naming a row the drain removed. Both tables
                // drain their own bucket range — the interval boundary is the
                // table's, never hand-rolled here. The bucket's lock rows
                // leave with them, an orphan included.
                VersionTable::open(txn)?.drain_bucket(&bucket)?;
                IndexTable::open(txn)?.drain_bucket(&bucket)?;
                LockTable::open(txn)?.drain_bucket(&bucket)?;
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

    // The Object Lock contract methods: the lock row is keyed by the version
    // identity, and every rule lives in [`version_lock`]; these are the
    // contract's forwarding signatures (OL5, OL7, OL8).
    #[cfg(any(test, feature = "testing"))]
    async fn set_version_lock(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
        lock: Option<&object_lock::Lock>,
    ) -> Result<(), Error> {
        version_lock::set_version_lock(self, bucket, key, version, lock).await
    }

    async fn version_lock(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<Option<object_lock::Lock>, Error> {
        version_lock::version_lock(self, bucket, key, version)
    }

    async fn get_object_retention(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<Option<object_lock::Retention>, Error> {
        Ok(version_lock::version_lock(self, bucket, key, version)?.and_then(|lock| lock.retention))
    }

    async fn get_object_legal_hold(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<object_lock::LegalHold, Error> {
        Ok(version_lock::version_lock(self, bucket, key, version)?
            .map(|lock| lock.legal_hold)
            .unwrap_or(object_lock::LegalHold::Off))
    }

    async fn set_retention(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
        requested: Option<&object_lock::Retention>,
        bypass: object_lock::Bypass,
        now: SystemTime,
    ) -> Result<Option<object_lock::Lock>, Error> {
        version_lock::set_retention(self, bucket, key, version, requested, bypass, now).await
    }

    async fn set_legal_hold(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
        hold: object_lock::LegalHold,
    ) -> Result<object_lock::Lock, Error> {
        version_lock::set_legal_hold(self, bucket, key, version, hold).await
    }

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
        version_lock::set_lock(
            self, bucket, key, version, retention, legal_hold, bypass, now,
        )
        .await
    }

    /// The permanent delete, with the lock enforced inside the one transaction
    /// that removes the row, its index entry and its lock row (OL8).
    async fn expire_with(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
        bypass: object_lock::Bypass,
    ) -> Result<Expired, Error> {
        version_lock::expire_with(self, bucket, key, seq, bypass).await
    }

    /// The batched permanent delete: one transaction, protected sequences
    /// skipped and reported, the rest removed with their lock rows (OL8).
    async fn expire_many_report(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seqs: &[u64],
    ) -> Result<ExpireReport, Error> {
        version_lock::expire_many_report(self, bucket, key, seqs).await
    }
}

#[cfg(test)]
mod tests;
