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
    },
    Error, PgTx,
    store::PgStore,
};

pub mod bucket;
pub mod object;
pub mod upload;
pub mod version;

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

/// Project a lookup map back onto the caller's own order: one answer per
/// requested key, `None` where the statement returned no row for it.
///
/// The four reads that answer in the caller's order -- buckets by name, owners
/// by name, objects by key, part records by number -- all have the same shape:
/// the statement returns the rows it found, unordered, and the contract's
/// answer has one element per requested key. The map holds the answer's own
/// row type, and a row that does not decode is simply not in it, so an absent
/// row and a healed-away one answer the same `None`. The map is consumed: each
/// answer moves its row out instead of cloning it.
pub(crate) fn project<'a, Q, M, V>(
    wanted: impl Iterator<Item = &'a Q>,
    found: &mut HashMap<M, V>,
) -> Vec<Option<V>>
where
    M: Borrow<Q> + Eq + Hash,
    Q: Eq + Hash + ?Sized + 'a,
{
    wanted.map(|key| found.remove(key)).collect()
}

/// Assert inside tx that bucket has a row, refusing with Error::NoSuchBucket
/// when it does not.
///
/// The read takes the row's lock (FOR UPDATE), so a concurrent
/// remove_if_empty — which locks the same bucket row before it deletes —
/// blocks behind this transaction and re-checks emptiness after the guarded
/// write commits. That is what makes the guarded writers race-free rather
/// than merely read-then-write: the refusal is decided against the row the
/// writer later commits beside, not against a snapshot it has already left.
pub(crate) async fn assert_bucket_row(tx: &mut PgTx, bucket: &BucketName) -> Result<(), Error> {
    let present = sqlx::query_scalar!("SELECT 1 FROM buckets WHERE name = $1 FOR UPDATE", bucket)
        .fetch_optional(&mut **tx)
        .await?;
    if present.is_none() {
        return Err(Error::NoSuchBucket(bucket.clone()));
    }
    Ok(())
}
