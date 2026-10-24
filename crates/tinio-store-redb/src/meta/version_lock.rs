//! The Object Lock row of [`VersionMeta`](crate::_core::meta::VersionMeta) —
//! one read and one writer per lock rule, over the
//! [`crate::version_object_lock`] table.
//!
//! The row is keyed by the version identity wire (the null version's wire is
//! the literal `null`), a marker owns no row, and an empty lock is not a row
//! ([`object_lock::Lock::carries_fact`]). Every update below loads the row,
//! asks the model for the row that should remain — [`object_lock::apply_retention`],
//! [`object_lock::apply_legal_hold`] — and stores exactly that answer, inside
//! the one transaction that also probes the write-path preconditions. The
//! delete-side enforcement ([`expire_row`]) lives here too, and a null
//! in-place overwrite asks [`locked_wire_reason`] for the same answer
//! without removing the row.

use std::{collections::HashSet, time::SystemTime};

use crate::{
    _core::{
        bucket,
        meta::version::{ExpireReport, Expired},
        object, object_lock, storage,
        version::{self as contract, Id, State},
    },
    Error, RedbMetadata,
    bucket::Table as BucketTable,
    bucket_object_lock::Table as ConfigTable,
    bucket_versioning::Table as StateTable,
    object_meta::Table as ObjectTable,
    version::{Kind as RowKind, Table as VersionTable},
    version_index::Table as IndexTable,
    version_object_lock::Table as LockTable,
};

/// Whether a lock row's identity resolves to a version — the ownership rule
/// the bucket drain enforces and the doctor's orphan scan reports (OL8,
/// R19).
///
/// A lock belongs to a version when either an object version row of that
/// identity exists through the version index, or the key's current slot names
/// the same identity and is not a marker — the row-less current version the
/// versioned write commits (OL7/OL8). A marker owns no lock: R19 says a
/// marker is never a `may_delete` subject, so a lock row under a marker's
/// identity is an orphan, not a protected version.
///
/// The callers read through different transaction types — the drain's and
/// the lock writers' write transactions and the doctor's read one — so the
/// rule is a trait both implement rather than a free function over one
/// table-handle type. Keeping it beside the lock writers is what stops the
/// doctor's orphan report from drifting from the drain's ownership decision.
pub(crate) trait LockOwned {
    /// Whether `version` (a version-id wire) resolves to a version of
    /// `bucket`/`key`.
    fn lock_owned(&mut self, bucket: &str, key: &str, version: &str) -> Result<bool, Error>;
}

/// The rule's one body over a transaction whose tables open the given way.
macro_rules! lock_owned_body {
    ($self:ident, $open:ident, $bucket:ident, $key:ident, $version:ident) => {{
        let seq = IndexTable::$open($self)?.seq_of($bucket, $key, $version)?;
        if let Some(seq) = seq
            && let Some(row) = VersionTable::$open($self)?.get($bucket, $key, seq)?
            && row.kind == RowKind::Object
        {
            return Ok(true);
        }
        Ok(ObjectTable::$open($self)?
            .get($bucket, $key)?
            .is_some_and(|slot| {
                !slot.is_delete_marker && contract::wire_of(slot.version_id.as_ref()) == $version
            }))
    }};
}

impl LockOwned for redb::ReadTransaction {
    fn lock_owned(&mut self, bucket: &str, key: &str, version: &str) -> Result<bool, Error> {
        lock_owned_body!(self, open_readonly, bucket, key, version)
    }
}

impl LockOwned for redb::WriteTransaction {
    fn lock_owned(&mut self, bucket: &str, key: &str, version: &str) -> Result<bool, Error> {
        lock_owned_body!(self, open, bucket, key, version)
    }
}

/// Store `lock` as the row at `wire`, or remove the row when it is `None` —
/// the one "an empty lock is not a row" write (OL5).
fn store_lock(
    txn: &mut redb::WriteTransaction,
    bucket: &str,
    key: &str,
    wire: &str,
    lock: Option<&object_lock::Lock>,
) -> Result<(), Error> {
    match lock {
        Some(lock) => LockTable::open(txn)?.put(bucket, key, wire, lock),
        None => LockTable::open(txn)?.remove(bucket, key, wire),
    }
}

/// Record the lock an advancing slot carries — OL7's writer, called inside
/// `advance_slot_with`'s transaction. A marker never gets a row (OL5), only a
/// lock that carries a fact is stored (an empty lock protects nothing), and a
/// slot advance **never removes** an existing row, so the `None` answer is a
/// no-op here rather than [`store_lock`]'s removal.
pub(crate) fn put_advance_lock(
    txn: &mut redb::WriteTransaction,
    bucket: &str,
    key: &str,
    wire: &str,
    lock: Option<&object_lock::Lock>,
    marker: bool,
) -> Result<(), Error> {
    if !marker && let Some(lock) = lock.filter(|lock| lock.carries_fact()) {
        LockTable::open(txn)?.put(bucket, key, wire, lock)?;
    }
    Ok(())
}

/// One version's lock row, or `None` when it has none. A corrupt or partial
/// row is [`storage::Error::Corrupt`], never `Ok(None)` (OL5).
pub(crate) fn version_lock(
    store: &RedbMetadata,
    bucket: &bucket::Name,
    key: &object::Key,
    version: Option<&Id>,
) -> Result<Option<object_lock::Lock>, Error> {
    let bucket = bucket.to_string();
    let key = key.to_string();
    let wire = contract::wire_of(version);
    store
        .driver()
        .read(move |txn| LockTable::open_readonly(txn)?.get(&bucket, &key, &wire))
}

/// Replace one version's lock row (the test and testing-feature writer): a
/// lock that carries no fact removes the row instead.
#[cfg(any(test, feature = "testing"))]
pub(crate) async fn set_version_lock(
    store: &RedbMetadata,
    bucket: &bucket::Name,
    key: &object::Key,
    version: Option<&Id>,
    lock: Option<&object_lock::Lock>,
) -> Result<(), Error> {
    let bucket = bucket.to_string();
    let key = key.to_string();
    let wire = contract::wire_of(version);
    let lock = lock.cloned();
    store
        .driver()
        .write(move |txn| {
            store_lock(
                txn,
                &bucket,
                &key,
                &wire,
                lock.as_ref().filter(|lock| lock.carries_fact()),
            )
        })
        .await
}

/// Apply one retention update: the model decides the row, this writer stores
/// it (or removes it), in the transaction that also proves the identity and
/// the write-path preconditions ([`assert_lockable`]).
pub(crate) async fn set_retention(
    store: &RedbMetadata,
    bucket: &bucket::Name,
    key: &object::Key,
    version: Option<&Id>,
    requested: Option<&object_lock::Retention>,
    bypass: object_lock::Bypass,
    now: SystemTime,
) -> Result<Option<object_lock::Lock>, Error> {
    let name = bucket.clone();
    let object_key = key.clone();
    let version = version.cloned();
    let requested = requested.copied();
    let bucket = bucket.to_string();
    let key = key.to_string();
    store
        .driver()
        .write(move |txn| -> Result<Option<object_lock::Lock>, Error> {
            assert_lockable(txn, &name, &object_key, version.as_ref())?;
            let wire = contract::wire_of(version.as_ref());
            let current = LockTable::open(txn)?.get(&bucket, &key, &wire)?;
            let updated =
                object_lock::apply_retention(current.as_ref(), requested.as_ref(), bypass, now)
                    .map_err(|reason| Error::Refused(storage::locked(reason)))?;
            store_lock(txn, &bucket, &key, &wire, updated.as_ref())?;
            Ok(updated)
        })
        .await
}

/// Apply one legal-hold change: the model decides the row
/// ([`object_lock::apply_legal_hold`]), this writer stores it or removes it in
/// the same transaction. The hold never touches the retention.
pub(crate) async fn set_legal_hold(
    store: &RedbMetadata,
    bucket: &bucket::Name,
    key: &object::Key,
    version: Option<&Id>,
    hold: object_lock::LegalHold,
) -> Result<object_lock::Lock, Error> {
    let name = bucket.clone();
    let object_key = key.clone();
    let version = version.cloned();
    let bucket = bucket.to_string();
    let key = key.to_string();
    store
        .driver()
        .write(move |txn| -> Result<object_lock::Lock, Error> {
            assert_lockable(txn, &name, &object_key, version.as_ref())?;
            let wire = contract::wire_of(version.as_ref());
            let current = LockTable::open(txn)?.get(&bucket, &key, &wire)?;
            let updated = object_lock::apply_legal_hold(current.as_ref(), hold);
            store_lock(txn, &bucket, &key, &wire, updated.as_ref())?;
            Ok(updated.unwrap_or_default())
        })
        .await
}

/// Apply the combined lock update: the retention member and the legal-hold
/// member are decided by [`object_lock::apply_lock`] and the resulting row is
/// stored — or removed — in **one** transaction, the same transaction that
/// proves the identity and the write-path preconditions ([`assert_lockable`]).
/// A refused retention writes neither member.
#[expect(
    clippy::too_many_arguments,
    reason = "the combined write's field set — store, bucket, key, identity, both members, bypass and clock — is the request's own"
)]
pub(crate) async fn set_lock(
    store: &RedbMetadata,
    bucket: &bucket::Name,
    key: &object::Key,
    version: Option<&Id>,
    retention: Option<Option<&object_lock::Retention>>,
    legal_hold: Option<object_lock::LegalHold>,
    bypass: object_lock::Bypass,
    now: SystemTime,
) -> Result<Option<object_lock::Lock>, Error> {
    let name = bucket.clone();
    let object_key = key.clone();
    let version = version.cloned();
    // The borrowed member cannot cross into the write closure; copy the
    // retention so the transaction owns its inputs.
    let retention = retention.map(|requested| requested.copied());
    let bucket = bucket.to_string();
    let key = key.to_string();
    store
        .driver()
        .write(move |txn| -> Result<Option<object_lock::Lock>, Error> {
            assert_lockable(txn, &name, &object_key, version.as_ref())?;
            let wire = contract::wire_of(version.as_ref());
            let current = LockTable::open(txn)?.get(&bucket, &key, &wire)?;
            let updated = object_lock::apply_lock(
                current.as_ref(),
                retention.as_ref().map(Option::as_ref),
                legal_hold,
                bypass,
                now,
            )
            .map_err(|reason| Error::Refused(storage::locked(reason)))?;
            store_lock(txn, &bucket, &key, &wire, updated.as_ref())?;
            Ok(updated)
        })
        .await
}

/// The permanent delete, with the lock enforced inside the one transaction
/// that removes the row, its index entry and its lock row (OL8).
pub(crate) async fn expire_with(
    store: &RedbMetadata,
    bucket: &bucket::Name,
    key: &object::Key,
    seq: u64,
    bypass: object_lock::Bypass,
) -> Result<Expired, Error> {
    let bucket = bucket.to_string();
    let key = key.to_string();
    let now = SystemTime::now();
    store
        .driver()
        .write(move |txn| -> Result<Expired, Error> {
            match expire_row(txn, &bucket, &key, seq, bypass, now, true)? {
                Outcome::Protected(reason) => Err(Error::Refused(storage::locked(reason))),
                // The row-less current version is the addressed version too:
                // its slot cleared, so the caller that owns the bytes unlinks.
                Outcome::Removed | Outcome::Cleared => Ok(Expired::Removed),
                Outcome::Absent => Ok(Expired::Nothing),
            }
        })
        .await
}

/// The batched permanent delete: one transaction, protected sequences
/// skipped and reported, the rest removed with their lock rows (OL8).
pub(crate) async fn expire_many_report(
    store: &RedbMetadata,
    bucket: &bucket::Name,
    key: &object::Key,
    seqs: &[u64],
) -> Result<ExpireReport, Error> {
    let bucket = bucket.to_string();
    let key = key.to_string();
    // A sequence listed twice is one version: judge it, count it and report a
    // refusal for it once (the first occurrence), exactly as the pg batch does.
    let mut seen = HashSet::new();
    let seqs: Vec<u64> = seqs
        .iter()
        .copied()
        .filter(|seq| seen.insert(*seq))
        .collect();
    store
        .driver()
        .write(move |txn| {
            // One transaction for the whole listed history (the batch is
            // the unit): a crash leaves every listed row or none of them.
            let now = SystemTime::now();
            let mut report = ExpireReport::default();
            for seq in seqs {
                match expire_row(
                    txn,
                    &bucket,
                    &key,
                    seq,
                    object_lock::Bypass::None,
                    now,
                    false,
                )? {
                    Outcome::Removed => report.removed += 1,
                    Outcome::Protected(reason) => report.protected.push((seq, reason)),
                    Outcome::Cleared | Outcome::Absent => {}
                }
            }
            Ok(report)
        })
        .await
}

/// What [`expire_row`] did for one listed sequence.
///
/// The core [`Expired`] verdict the caller receives cannot carry a refusal
/// (that is [`storage::Error::Locked`]), so this row-level outcome keeps the
/// protected arm and the two success arms apart; [`expire_with`] maps it at
/// the boundary — `Removed` and `Cleared` to [`Expired::Removed`] (both took
/// the addressed version away, the second as the row-less current slot) and
/// `Absent` to [`Expired::Nothing`].
enum Outcome {
    /// A version row was removed and counted.
    Removed,
    /// No version row at the sequence: the current slot's identity and its
    /// lock row were cleared (the row-less current version, OL8).
    Cleared,
    /// Nothing named the sequence, or the sequence is not the current slot.
    Absent,
    /// A lock refused the removal; nothing was removed.
    Protected(object_lock::RefusalReason),
}

/// The refusal `wire`'s lock would give a delete, loaded inside the caller's
/// transaction (OL7). `Ok(None)` means the write may replace that identity.
/// The row stays either way — an overwrite is not a delete.
pub(crate) fn locked_wire_reason(
    txn: &mut redb::WriteTransaction,
    bucket: &str,
    key: &str,
    wire: &str,
    bypass: object_lock::Bypass,
    now: SystemTime,
) -> Result<Option<object_lock::RefusalReason>, Error> {
    let lock = LockTable::open(txn)?.get(bucket, key, wire)?;
    Ok(object_lock::delete_refusal(lock.as_ref(), now, bypass))
}

/// Remove the version at `seq` under its lock, in the caller's transaction
/// (OL8, R19).
///
/// A row at `seq` is checked through its own identity and kind: an object
/// version's lock is enforced and removed with it, and a delete marker is
/// never observed and never touches a lock row. A sequence with no row
/// resolves the current slot, whose object identity is judged with the same
/// [`object_lock::delete_refusal`] — the contract's "no row at `seq` → judge
/// the current slot". `judge_divergent` widens that judgement to a slot that
/// does not name `seq`: the single-delete caller passes `true` so a divergent
/// recorded identity (the fs degraded null, where the gate answers sequence 0
/// while the slot records another) cannot slip a protected version past the
/// delete, while the batch caller passes `false` because the contract says a
/// listed sequence with no row is **skipped**, never reported protected.
/// Only a sequence the slot actually names is the row-less current version the
/// call clears; a divergent call is a no-op after its judgement.
fn expire_row(
    txn: &mut redb::WriteTransaction,
    bucket: &str,
    key: &str,
    seq: u64,
    bypass: object_lock::Bypass,
    now: SystemTime,
    judge_divergent: bool,
) -> Result<Outcome, Error> {
    let row = VersionTable::open(txn)?.get(bucket, key, seq)?;
    match row {
        Some(row) => {
            if row.kind == RowKind::Object {
                let lock = LockTable::open(txn)?.get(bucket, key, &row.id)?;
                if let Some(reason) = object_lock::delete_refusal(lock.as_ref(), now, bypass) {
                    return Ok(Outcome::Protected(reason));
                }
            }
            VersionTable::open(txn)?.remove(bucket, key, seq)?;
            // The entry goes only when it still points at this row: two rows
            // can share one identity wire (the null version), and the index
            // names only the newest of them.
            IndexTable::open(txn)?.remove_pointing_at(bucket, key, &row.id, seq)?;
            if row.kind == RowKind::Object {
                LockTable::open(txn)?.remove(bucket, key, &row.id)?;
            }
            Ok(Outcome::Removed)
        }
        None => {
            let slot = ObjectTable::open(txn)?.get(bucket, key)?;
            let Some(slot) = slot else {
                return Ok(Outcome::Absent);
            };
            // A marker is never observed (R19).
            if slot.is_delete_marker {
                return Ok(Outcome::Absent);
            }
            // The batch skips a listed sequence with no row: it never judges
            // a slot that does not name the addressed sequence.
            if !judge_divergent && slot.version_seq != seq {
                return Ok(Outcome::Absent);
            }
            let wire = contract::wire_of(slot.version_id.as_ref());
            let lock = LockTable::open(txn)?.get(bucket, key, &wire)?;
            if let Some(reason) = object_lock::delete_refusal(lock.as_ref(), now, bypass) {
                return Ok(Outcome::Protected(reason));
            }
            // Only the addressed sequence that the current slot names is the
            // row-less current version; anything else is an already-removed
            // row, or the degraded slot whose recorded identity was judged
            // above and owns no row at the addressed sequence.
            if slot.version_seq != seq {
                return Ok(Outcome::Absent);
            }
            IndexTable::open(txn)?.remove_pointing_at(bucket, key, &wire, seq)?;
            LockTable::open(txn)?.remove(bucket, key, &wire)?;
            Ok(Outcome::Cleared)
        }
    }
}

/// Assert the identity and the two write-path preconditions a lock write
/// requires, inside its transaction (OL5, OL13): the bucket row exists, the
/// stored configuration is enabled, versioning is `Enabled`, and the
/// addressed identity is the current slot or an object-version row — a
/// marker and an absent key refuse with `NoSuchVersion`.
fn assert_lockable(
    txn: &mut redb::WriteTransaction,
    bucket: &bucket::Name,
    key: &object::Key,
    version: Option<&Id>,
) -> Result<(), Error> {
    let b = bucket.as_ref().as_str();
    let k = key.as_ref().as_str();
    if !BucketTable::open(txn)?.exists(b)? {
        return Err(Error::Refused(storage::no_such_bucket(bucket)));
    }
    if !ConfigTable::open(txn)?.is_enabled(b)? {
        return Err(Error::Refused(storage::invalid_bucket_state(bucket)));
    }
    if StateTable::open(txn)?.get(b)?.unwrap_or_default() != State::Enabled {
        return Err(Error::Refused(storage::invalid_bucket_state(bucket)));
    }
    let wire = contract::wire_of(version);
    // The identity half is the drain's own ownership rule ([`LockOwned`]): a
    // current slot or an object-version row of that wire, and never a marker.
    if txn.lock_owned(b, k, &wire)? {
        return Ok(());
    }
    Err(Error::Refused(storage::no_such_version(wire)))
}
