//! `(bucket, key)` → `(etag hex, size, mtime unix nanos, file identity,
//! tags wire, checksum wire)` — the tags and checksum elements are empty
//! strings when the object has none (spec 2026-08-31). The checksum wire
//! is `<algorithm wire>:<base64 value>:<kind>` — e.g.
//! `CRC32:NhCmhg==:FULL_OBJECT` — with the kind recorded at write time so
//! read paths never derive it.

use redb::{ReadableTable, TableDefinition};
use tinio_core::{checksum, etag::ETag, object};

use crate::{
    error::Error,
    scan::{drain_pair, for_each_pair},
    table::{self, TableDef},
};

/// The per-table marker: the table definition for the shared handle arms.
#[doc(hidden)]
pub enum Def {}

impl TableDef for Def {
    type Key = (&'static str, &'static str);
    type Value = (&'static str, u64, u64, u64, &'static str, &'static str);

    const DEF: TableDefinition<'static, Self::Key, Self::Value> =
        TableDefinition::new("object_meta");
}

/// Handle to the object-meta table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

/// One stored `OBJECT_META` entry, validated into domain types (the row
/// shape is `(etag hex, size, mtime unix nanos, file identity, tags wire,
/// checksum wire)`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    /// ETag (single MD5 or composed `-N` form).
    pub etag: ETag,
    /// Object size in bytes at record time.
    pub size: u64,
    /// Object mtime in unix nanoseconds at record time.
    pub mtime: u64,
    /// File identity at record time (`0` marks an unavailable platform
    /// identity).
    pub file_identity: u64,
    /// The object's tag set (empty when none, or when the stored wire is
    /// domain-invalid — self-healing like the etag).
    pub tags: object::Tags,
    /// The recorded object checksum (`None` when none, or when the stored
    /// element is domain-invalid — self-healing like the etag).
    pub checksum: Option<checksum::Recorded>,
}

/// Validate one raw `OBJECT_META` row into [`Stored`] — `None` on a
/// domain-invalid etag (self-healing: the caller treats it as missing
/// and recomputes). The tags and checksum elements self-heal to
/// empty/`None` on a domain-invalid wire — the row itself is still
/// served (its etag is valid), exactly like the read paths treat a
/// garbage checksum spec. Shared by the point read
/// [`Table::get`] and the gating traversal
/// [`Table::for_bucket_gated`] — the single home of the rule.
pub fn validate(
    (etag, size, mtime, file_identity, tags, checksum): (&str, u64, u64, u64, &str, &str),
) -> Option<Stored> {
    Some(Stored {
        etag: ETag::new(etag).ok()?,
        size,
        mtime,
        file_identity,
        tags: object::Tags::from_wire_limited(tags, object::OBJECT_TAGS_MAX),
        checksum: checksum::Recorded::from_wire_opt(checksum),
    })
}

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// One stored entry, if present and domain-valid (`None` on a corrupt
    /// etag — self-healing; the caller recomputes).
    pub fn get(&self, bucket: &str, key: &str) -> Result<Option<Stored>, Error> {
        let Some(guard) = self.0.get((bucket, key))? else {
            return Ok(None);
        };
        Ok(validate(guard.value()))
    }

    /// Visit every row of `bucket` (contiguous from `(bucket, "")`) with
    /// per-row [`Self::get`] semantics — the gating-load traversal
    /// (pipeline-spec.md P2, R1): a domain-invalid key skips the row, a
    /// domain-invalid etag reports `stored: None` (treated as missing —
    /// the caller recomputes and rewrites, self-healing). A corrupt row
    /// never fails the walk.
    pub fn for_bucket_gated<F>(&self, bucket: &str, mut visit: F) -> Result<(), Error>
    where
        F: FnMut(object::Key, Option<Stored>) -> Result<(), Error>,
    {
        for_each_pair(
            &self.0,
            (bucket, ""),
            |b, _| b == bucket,
            |_, raw_key, value| {
                let Ok(key) = object::key(raw_key) else {
                    return Ok(()); // invalid key domain → skip the row
                };
                // Same row validation as the point read and the gate
                // (invalid etag → None — self-healing).
                let stored = validate(value);
                visit(key, stored)
            },
        )
    }
}

impl<'txn> table::Table<'txn, Def> {
    /// Upsert one row — the key plus the [`Stored`] payload (one
    /// struct per row; the wire elements are encoded here, the one
    /// encode home). The interface-validated `tags` and the recorded
    /// checksum ride the same row (write-path atomicity: persisted with
    /// the etag, never a post-commit tag window), and `checksum` is stored
    /// with its recorded kind (`FULL_OBJECT` for plain PUTs, `COMPOSITE`
    /// for multipart completions, the source's kind for copies).
    pub fn put(&mut self, bucket: &str, key: &str, meta: &Stored) -> Result<(), Error> {
        let etag_hex = meta.etag.as_str();
        let tags_wire = meta.tags.to_wire();
        let checksum_wire = meta
            .checksum
            .as_ref()
            .map(|c| c.to_wire())
            .unwrap_or_default();
        self.0.insert(
            (bucket, key),
            (
                etag_hex.as_str(),
                meta.size,
                meta.mtime,
                meta.file_identity,
                tags_wire.as_str(),
                checksum_wire.as_str(),
            ),
        )?;
        Ok(())
    }

    /// Remove the entry for `key` (idempotent).
    pub fn remove(&mut self, bucket: &str, key: &str) -> Result<(), Error> {
        self.0.remove((bucket, key))?;
        Ok(())
    }

    /// Delete every row of `bucket` (entries are contiguous from
    /// `(bucket, "")` — mismatch break, see `crate::scan`).
    pub fn drain_bucket(&mut self, bucket: &str) -> Result<(), Error> {
        drain_pair(&mut self.0, (bucket, ""), |b, _| b == bucket)
    }
}
