//! The three metadata planes over PostgreSQL.
//!
//! One module per contract: [bucket] carries BucketMeta, [object] ObjectMeta,
//! [upload] UploadMeta, and [version] the standalone VersionMeta (never a
//! supertrait of Metadata). Each plane decodes its own statements into a row
//! struct declared beside them, so the column order lives with the statement
//! that selects it.
//!
//! # Statement rules
//!
//! Every statement is a fixed SQL literal: values are bound parameters and
//! table and column identifiers come only from the migration schema (PG26).
//! No statement is assembled by formatting a value or an identifier into it.
//! Every ordered read repeats COLLATE "C" on the key column it orders by
//! (PG4): the database's default collation is not "C", so an unqualified
//! ORDER BY would order by the server's locale, not the key's UTF-8 bytes.
//!
//! A mutating method is either one statement — which is its own transaction —
//! or opens exactly one explicit transaction for every statement it runs. A
//! transaction never spans two contract calls and is never held across a
//! non-database await.

use std::{borrow::Borrow, collections::HashMap, hash::Hash};

use crate::{
    _core::{
        acl::{Acl, AclGrants, OwnerId},
        bucket::Name as BucketName,
        meta::Metadata,
        object_lock, storage,
    },
    Error, PgTx,
    store::PgStore,
};

mod lifecycle;

pub mod bucket;
pub mod object;
pub mod upload;
pub mod version;
mod version_lock;

impl Metadata for PgStore {
    type Error = Error;
}

/// The one ACL composite a contract row's split elements make: the row keeps
/// its owner element beside its grant set, while the four `acl` columns carry
/// both, so a writer assembles the contract's [`Acl`] through the contract's
/// own re-join ([`Acl::from_parts`], the inverse of the split every row's
/// decode applies) before binding it. The value is exactly the contract's own
/// fields; the canonical write (unknown group URIs dropped, order and dedupe)
/// is [`Acl`]'s own sqlx mapping.
pub(crate) fn acl_composite(owner: Option<&OwnerId>, grants: &AclGrants) -> Acl {
    Acl::from_parts(owner.cloned(), grants.clone())
}

/// Project a lookup map back onto the caller's own order: one answer **per
/// occurrence** of a requested key, `None` where the statement returned no row
/// for it.
///
/// The four reads that answer in the caller's order -- buckets by name, owners
/// by name, objects by key, part records by number -- all have the same shape:
/// the statement returns the rows it found, unordered, and the contract's
/// answer has one element per requested key, in the requested key's own value
/// (the input's order, `load_many`'s two names or `part_records`' two numbers
/// that repeat included). The map holds the answer's own row type, and a row
/// that does not decode is simply not in it, so an absent row and a
/// healed-away one answer the same `None`.
///
/// A key that occurs twice answers its row **twice**: the answer is per
/// occurrence, so the map is only *drained*, not consumed by the first reader.
/// The last occurrence moves the row out and an earlier one clones it, so a
/// caller whose keys do not repeat -- every point-listing read -- keeps the
/// move-only path.
pub(crate) fn project<'a, Q, M, V>(
    wanted: impl Iterator<Item = &'a Q>,
    found: &mut HashMap<M, V>,
) -> Vec<Option<V>>
where
    M: Borrow<Q> + Eq + Hash,
    Q: Eq + Hash + ?Sized + 'a,
    V: Clone,
{
    let wanted: Vec<&'a Q> = wanted.collect();
    let mut left: HashMap<&'a Q, usize> = HashMap::with_capacity(wanted.len());
    for key in &wanted {
        *left.entry(key).or_default() += 1;
    }
    wanted
        .into_iter()
        .map(|key| {
            // Every key was counted above, so a missing entry is unreachable;
            // it answers `None` rather than panicking a read either way.
            let remaining = left.get_mut(key).map_or(0, |left| {
                *left = left.saturating_sub(1);
                *left
            });
            if remaining == 0 {
                found.remove(key)
            } else {
                found.get(key).cloned()
            }
        })
        .collect()
}

/// Whether the bucket has a row, taking that row's lock (FOR UPDATE).
///
/// This is the answer, not a refusal: the caller decides for itself what an
/// absent row means. The two lifecycle setters ask it because their table has
/// no row for a bucket that stores no configuration, so `rows_affected` alone
/// cannot tell that bucket from a missing one. A caller that owes the
/// `NoSuchBucket` refusal asks [`assert_bucket_row`] instead.
pub(crate) async fn bucket_row_present(tx: &mut PgTx, bucket: &BucketName) -> Result<bool, Error> {
    let present = sqlx::query_scalar!("SELECT 1 FROM buckets WHERE name = $1 FOR UPDATE", bucket)
        .fetch_optional(&mut **tx)
        .await?;
    Ok(present.is_some())
}

/// Assert inside tx that bucket has a row, refusing with Error::NoSuchBucket
/// when it does not.
///
/// The read takes the row's lock (FOR UPDATE), which is what makes the
/// assertion a guard for the write beside it. A concurrent `remove_if_empty`
/// or the store's own bucket teardown (`teardown_bucket`) locks this same row,
/// then checks emptiness in a later statement of that transaction. This writer
/// blocks until that delete commits or rolls back. A delete that was waiting
/// on the lock sees this writer's committed object or upload, because the
/// check is not the statement that waited.
pub(crate) async fn assert_bucket_row(tx: &mut PgTx, bucket: &BucketName) -> Result<(), Error> {
    if !bucket_row_present(tx, bucket).await? {
        return Err(Error::NoSuchBucket(bucket.clone()));
    }
    Ok(())
}

/// Whether the bucket's Object Lock configuration is enabled, read inside
/// `tx`.
///
/// One statement for the two places that need the bit: a lock write refuses
/// unless it is set (`InvalidBucketState`, OL13), and a versioning suspend
/// refuses while it is set (R21). An absent row is not enabled. The caller
/// owns the refusal, because the two callers ask different questions of the
/// same column; the statement is spelled once here.
pub(crate) async fn bucket_lock_enabled(tx: &mut PgTx, bucket: &BucketName) -> Result<bool, Error> {
    let enabled = sqlx::query_scalar!(
        "SELECT enabled FROM bucket_object_lock WHERE bucket = $1",
        bucket
    )
    .fetch_optional(&mut **tx)
    .await?;
    Ok(enabled.unwrap_or(false))
}

/// Decode one `version_object_lock` or `upload_object_lock` row through the
/// model's reader: a corrupt mode, date, legal hold or partial pair is the
/// contract's `Corrupt` error, never an absent lock (OL5, OL10).
///
/// The caller's row struct supplies the three columns it selected; this is the
/// one place both lock tables turn them into the contract type, so the two
/// tables cannot decode differently.
pub(crate) fn decode_lock(
    mode: Option<&str>,
    retain_until: Option<i64>,
    legal_hold: Option<&str>,
) -> Result<Option<object_lock::Lock>, Error> {
    object_lock::Lock::from_columns(mode, retain_until, legal_hold).map_err(|refusal| {
        tracing::warn!(
            %refusal,
            "a stored object lock row fails closed: corruption is not an absent lock"
        );
        Error::Storage(storage::corrupt(refusal.detail))
    })
}

/// One stored lock row's three columns, in the order every lock statement
/// selects them. The decode is [`decode_lock`]'s, so the version lock table,
/// the drain and the upload intent cannot read a corrupt row differently.
pub(crate) struct LockRow {
    mode: Option<String>,
    retain_until: Option<i64>,
    legal_hold: Option<String>,
}

impl LockRow {
    /// The contract may hold a lock with neither a retention nor a hold, which
    /// the model reads as no lock; this is the same answer for a stored row.
    pub(crate) fn decode(self) -> Result<Option<object_lock::Lock>, Error> {
        decode_lock(
            self.mode.as_deref(),
            self.retain_until,
            self.legal_hold.as_deref(),
        )
    }
}
