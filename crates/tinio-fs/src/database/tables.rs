//! Typed handles to redb tables (`StateTable`, `BucketsTable`, …).

use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
    path::Path,
    time::SystemTime,
};

use redb::{ReadableTable, Table, TableDefinition};

use super::{
    error::{Error, corrupt_meta, unsupported_version},
    scan::{drain_pair, drain_triple, for_each_pair},
};
use crate::{
    _core::{checksum, etag::ETag, from_nanos, object, to_nanos},
    bucket,
};

/// `Deref`/`DerefMut` plus `open` / `ensure` / `open_readonly` for a table handle.
macro_rules! table_impl {
    ($name:ident, $def:ident, $key:ty, $val:ty) => {
        table_impl!($name, $def, $key, $val, ensure);
    };
    ($name:ident, $def:ident, $key:ty, $val:ty, no_ensure) => {
        table_impl!(@deref $name);
        table_impl!(@write $name, $def, $key, $val, no_ensure);
        table_impl!(@read $name, $def, $key, $val);
    };
    ($name:ident, $def:ident, $key:ty, $val:ty, ensure) => {
        table_impl!(@deref $name);
        table_impl!(@write $name, $def, $key, $val, ensure);
        table_impl!(@read $name, $def, $key, $val);
    };
    (@deref $name:ident) => {
        impl<'txn, T> Deref for $name<'txn, T> {
            type Target = T;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl<'txn, T> DerefMut for $name<'txn, T> {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }
    };
    (@write $name:ident, $def:ident, $key:ty, $val:ty, ensure) => {
        impl<'txn> $name<'txn, redb::Table<'txn, $key, $val>> {
            /// Open the table in a write transaction.
            pub fn open(
                txn: &'txn mut redb::WriteTransaction,
            ) -> Result<Self, Error> {
                Ok(Self(txn.open_table($def)?, PhantomData))
            }

            /// Create the table if this is a fresh database.
            pub fn ensure(txn: &mut redb::WriteTransaction) -> Result<(), Error> {
                txn.open_table($def)?;
                Ok(())
            }
        }
    };
    (@write $name:ident, $def:ident, $key:ty, $val:ty, no_ensure) => {
        impl<'txn> $name<'txn, redb::Table<'txn, $key, $val>> {
            /// Open the table in a write transaction.
            pub fn open(
                txn: &'txn mut redb::WriteTransaction,
            ) -> Result<Self, Error> {
                Ok(Self(txn.open_table($def)?, PhantomData))
            }
        }
    };
    (@read $name:ident, $def:ident, $key:ty, $val:ty) => {
        impl<'txn> $name<'txn, redb::ReadOnlyTable<$key, $val>> {
            /// Open the table in a read transaction.
            pub fn open_readonly(
                txn: &'txn redb::ReadTransaction,
            ) -> Result<Self, Error> {
                Ok(Self(txn.open_table($def)?, PhantomData))
            }
        }
    };
}

// --- BUCKETS ---

type BucketKey = &'static str;

/// `name` → `(created-at unix nanos, tags wire)` — the tags element is
/// empty when the bucket has none (spec 2026-08-31).
type BucketValue = (u64, &'static str);
const BUCKETS: TableDefinition<BucketKey, BucketValue> = TableDefinition::new("buckets");

/// Handle to the `BUCKETS` table (writable or read-only).
pub struct BucketsTable<'txn, T>(T, PhantomData<&'txn ()>);

table_impl!(BucketsTable, BUCKETS, BucketKey, BucketValue);

impl<'txn, T> BucketsTable<'txn, T>
where
    T: ReadableTable<BucketKey, BucketValue>,
{
    /// Creation time of `name`, if recorded.
    pub fn get(&self, name: &bucket::Name) -> Result<Option<SystemTime>, Error> {
        Ok(self
            .0
            .get(&**name)?
            .map(|guard| from_nanos(guard.value().0)))
    }

    /// The stored row of `name`: `(creation time, tags wire raw)` (owned —
    /// the guard cannot outlive the closure).
    pub fn row(&self, name: &bucket::Name) -> Result<Option<(SystemTime, String)>, Error> {
        Ok(self
            .0
            .get(&**name)?
            .map(|guard| (from_nanos(guard.value().0), guard.value().1.to_string())))
    }

    /// Visit every recorded bucket in name order.
    pub fn for_each<F>(&self, mut visit: F) -> Result<(), Error>
    where
        F: FnMut(&str, SystemTime) -> Result<(), Error>,
    {
        for item in self.0.iter()? {
            let (k, v) = item?;
            visit(k.value(), from_nanos(v.value().0))?;
        }
        Ok(())
    }
}

impl<'txn> BucketsTable<'txn, Table<'txn, BucketKey, BucketValue>> {
    /// Record (or overwrite) the creation time of `name` — the tags
    /// element is cleared (a fresh row has no tags; the tagging ops use
    /// [`Self::put_full`] to preserve the creation time).
    pub fn put(&mut self, name: &bucket::Name, created_at: SystemTime) -> Result<(), Error> {
        self.0.insert(&**name, (to_nanos(created_at), ""))?;
        Ok(())
    }

    /// Record (or overwrite) the whole row: the creation time AND the
    /// tags wire (`put_bucket_tags`'s row upsert — the creation time is
    /// preserved from the stored row, first-sighted when absent).
    pub fn put_full(
        &mut self,
        name: &bucket::Name,
        created_at: SystemTime,
        tags_wire: &str,
    ) -> Result<(), Error> {
        self.0.insert(&**name, (to_nanos(created_at), tags_wire))?;
        Ok(())
    }

    /// Insert `now` when absent; return the stored creation time. The
    /// stored row's tags element rides the re-insert (a first-sight
    /// upsert must not clear the tag set the tagging write just recorded
    /// — mirror the `put_full`-style callers, which keep
    /// `(created_at, existing_tags)`).
    pub fn get_or_insert(
        &mut self,
        name: &bucket::Name,
        now: SystemTime,
    ) -> Result<SystemTime, Error> {
        let (created, tags) = match self.0.get(&**name)? {
            Some(guard) => (guard.value().0, guard.value().1.to_string()),
            None => (to_nanos(now), String::new()),
        };
        self.0.insert(&**name, (created, tags.as_str()))?;
        Ok(from_nanos(created))
    }

    /// Remove the entry of `name` (idempotent).
    pub fn remove(&mut self, name: &bucket::Name) -> Result<(), Error> {
        self.0.remove(&**name)?;
        Ok(())
    }
}

// --- OBJECT_META ---

type MetaKey = (&'static str, &'static str);

/// `(bucket, key)` → `(etag hex, size, mtime unix nanos, file identity,
/// tags wire, checksum wire)` — the tags and checksum elements are empty
/// strings when the object has none (spec 2026-08-31). The checksum wire
/// is `<algorithm wire>:<base64 value>:<kind>` — e.g.
/// `CRC32:NhCmhg==:FULL_OBJECT` — with the kind recorded at write time so
/// read paths never derive it.
type MetaValue = (&'static str, u64, u64, u64, &'static str, &'static str);
const OBJECT_META: TableDefinition<MetaKey, MetaValue> = TableDefinition::new("object_meta");

/// Handle to the `OBJECT_META` table (writable or read-only).
pub struct ObjectMetaTable<'txn, T>(T, PhantomData<&'txn ()>);

table_impl!(ObjectMetaTable, OBJECT_META, MetaKey, MetaValue);

/// One stored `OBJECT_META` entry, validated into domain types (the row
/// shape is `(etag hex, size, mtime unix nanos, file identity, tags wire,
/// checksum wire)`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredMeta {
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

/// Validate one raw `OBJECT_META` row into [`StoredMeta`] — `None` on a
/// domain-invalid etag (self-healing: the caller treats it as missing
/// and recomputes). The tags and checksum elements self-heal to
/// empty/`None` on a domain-invalid wire — the row itself is still
/// served (its etag is valid), exactly like the read paths treat a
/// garbage checksum spec. Shared by the point read
/// [`ObjectMetaTable::get`] and the gating traversal
/// [`ObjectMetaTable::for_bucket_gated`] — the single home of the rule.
fn validate_stored(
    (etag, size, mtime, file_identity, tags, checksum): (&str, u64, u64, u64, &str, &str),
) -> Option<StoredMeta> {
    Some(StoredMeta {
        etag: ETag::new(etag).ok()?,
        size,
        mtime,
        file_identity,
        tags: object::Tags::parse_wire_limited(tags, object::OBJECT_TAGS_MAX).unwrap_or_default(),
        checksum: checksum::Recorded::from_wire_opt(checksum),
    })
}

impl<'txn, T> ObjectMetaTable<'txn, T>
where
    T: ReadableTable<MetaKey, MetaValue>,
{
    /// One stored entry, if present and domain-valid (`None` on a corrupt
    /// etag — self-healing; the caller recomputes).
    pub fn get(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<StoredMeta>, Error> {
        let Some(guard) = self.0.get((&**bucket, &**key))? else {
            return Ok(None);
        };
        Ok(validate_stored(guard.value()))
    }

    /// Visit every row of `bucket` (contiguous from `(bucket, "")`).
    /// Domain-invalid key/etag rows fail the walk (`CorruptMeta`) — point
    /// reads still treat a bad entry as missing so the caller can self-heal.
    pub fn for_bucket<F>(&self, bucket: &bucket::Name, mut visit: F) -> Result<(), Error>
    where
        F: FnMut(object::Key, ETag, u64, u64) -> Result<(), Error>,
    {
        let bucket = &**bucket;
        for_each_pair(
            &self.0,
            (bucket, ""),
            |b, _| b == bucket,
            |_, raw_key, (etag, size, mtime, _, _, _)| {
                let key = object::key(raw_key).map_err(|err| corrupt_meta(raw_key, err))?;
                let etag = ETag::new(etag).map_err(|err| corrupt_meta(raw_key, err))?;
                visit(key, etag, size, mtime)
            },
        )
    }

    /// Visit every row of `bucket` with per-row [`Self::get`] semantics —
    /// the gating-load traversal (pipeline-spec.md P2, R1): a
    /// domain-invalid key skips the row, a domain-invalid etag reports
    /// `stored: None` (treated as missing — the caller recomputes and
    /// rewrites, self-healing). Unlike [`Self::for_bucket`], a corrupt
    /// row never fails the walk.
    pub fn for_bucket_gated<F>(&self, bucket: &bucket::Name, mut visit: F) -> Result<(), Error>
    where
        F: FnMut(object::Key, Option<StoredMeta>) -> Result<(), Error>,
    {
        let bucket = &**bucket;
        for_each_pair(
            &self.0,
            (bucket, ""),
            |b, _| b == bucket,
            |_, raw_key, (etag, size, mtime, file_identity, tags, checksum)| {
                let Ok(key) = object::key(raw_key) else {
                    return Ok(()); // invalid key domain → skip the row
                };
                // Same row validation as the point read and the gate
                // (invalid etag → None — self-healing).
                let stored = validate_stored((etag, size, mtime, file_identity, tags, checksum));
                visit(key, stored)
            },
        )
    }
}

impl<'txn> ObjectMetaTable<'txn, Table<'txn, MetaKey, MetaValue>> {
    /// Upsert the meta entry for `key` — the tags and the recorded
    /// checksum ride in the same row (write-path atomicity: the
    /// interface-validated `tags` and the recorded checksum are persisted
    /// with the etag, never a post-commit tag window). `checksum` is
    /// stored with its recorded kind (`FULL_OBJECT` for plain PUTs,
    /// `COMPOSITE` for multipart completions, the source's kind for
    /// copies) — read paths never derive it.
    #[allow(clippy::too_many_arguments)]
    /// Upsert one row — the key plus the [`StoredMeta`] payload (one
    /// struct per row; the wire elements are encoded here, the one
    /// encode home).
    pub fn put(
        &mut self,
        bucket: &bucket::Name,
        key: &object::Key,
        meta: &StoredMeta,
    ) -> Result<(), Error> {
        let etag_hex = meta.etag.as_str();
        let tags_wire = meta.tags.to_wire();
        let checksum_wire = meta
            .checksum
            .as_ref()
            .map(|c| c.to_wire())
            .unwrap_or_default();
        self.0.insert(
            (&**bucket, &**key),
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
    pub fn remove(&mut self, bucket: &bucket::Name, key: &object::Key) -> Result<(), Error> {
        self.0.remove((&**bucket, &**key))?;
        Ok(())
    }

    /// Delete every row of `bucket` (entries are contiguous from
    /// `(bucket, "")` — mismatch break, see `database::scan`).
    pub fn drain_bucket(&mut self, bucket: &bucket::Name) -> Result<(), Error> {
        let bucket = &**bucket;
        drain_pair(&mut self.0, (bucket, ""), |b, _| b == bucket)
    }
}

// --- UPLOADS ---

type UploadKey = (&'static str, &'static str);

/// `(bucket, upload_id)` → `(key, initiated-at unix nanos, tags wire)` —
/// the tags element is the create-time object tag set (spec 2026-08-31,
/// applied to the completed object; empty when none).
type UploadValue = (&'static str, u64, &'static str);
const UPLOADS: TableDefinition<UploadKey, UploadValue> = TableDefinition::new("uploads");

/// Handle to the `UPLOADS` table (writable or read-only).
pub struct UploadsTable<'txn, T>(T, PhantomData<&'txn ()>);

table_impl!(UploadsTable, UPLOADS, UploadKey, UploadValue);

impl<'txn, T> UploadsTable<'txn, T>
where
    T: ReadableTable<UploadKey, UploadValue>,
{
    /// Whether `bucket` has any upload row. Entries of one bucket are
    /// contiguous from `(bucket, "")` — the first key at or after that
    /// lower bound is in the bucket iff any exist (see [`Self::drain_bucket`]).
    pub fn has_bucket(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        let bucket = &**bucket;
        let mut iter = self.0.range((bucket, "")..)?;
        match iter.next() {
            Some(item) => {
                let (k, _) = item?;
                let (b, _) = k.value();
                Ok(b == bucket)
            }
            None => Ok(false),
        }
    }

    /// Whether the upload exists and records `key` (S3 identity is
    /// `(bucket, key, uploadId)`).
    pub fn key_matches(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<bool, Error> {
        Ok(self
            .0
            .get((&**bucket, upload_id))?
            .map(|guard| guard.value().0 == &**key)
            .unwrap_or(false))
    }

    /// The stored row, present only when the upload exists AND records
    /// `key` (S3 identity is `(bucket, key, uploadId)`) — the
    /// `key_matches` + `get` pair of `get_upload` in one lookup. Returns
    /// `(key, initiated-at, tags wire)` (owned — the guard cannot outlive
    /// the closure).
    pub fn get_matching(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<Option<(String, u64, String)>, Error> {
        Ok(self
            .0
            .get((&**bucket, upload_id))?
            .map(|guard| {
                (
                    guard.value().0.to_string(),
                    guard.value().1,
                    guard.value().2.to_string(),
                )
            })
            .filter(|(stored_key, _, _)| stored_key == &**key))
    }

    /// Visit every upload row of `bucket` (contiguous from `(bucket, "")`).
    pub fn for_bucket<F>(&self, bucket: &bucket::Name, mut visit: F) -> Result<(), Error>
    where
        F: FnMut(&str, (&str, u64, &str)) -> Result<(), Error>,
    {
        let bucket = &**bucket;
        for_each_pair(
            &self.0,
            (bucket, ""),
            |b, _| b == bucket,
            |_, upload_id, value| visit(upload_id, value),
        )
    }

    /// Visit every upload row across all buckets.
    pub fn for_each<F>(&self, mut visit: F) -> Result<(), Error>
    where
        F: FnMut(&str, &str, &str, u64, &str) -> Result<(), Error>,
    {
        for item in self.0.iter()? {
            let (k, v) = item?;
            let (b, upload_id) = k.value();
            let (key, initiated_at, tags_wire) = v.value();
            visit(b, upload_id, key, initiated_at, tags_wire)?;
        }
        Ok(())
    }
}

impl<'txn> UploadsTable<'txn, Table<'txn, UploadKey, UploadValue>> {
    /// Upsert one upload row — the create-time tags wire rides in the
    /// row (spec 2026-08-31).
    pub fn put(
        &mut self,
        bucket: &bucket::Name,
        upload_id: &str,
        key: &object::Key,
        initiated_at: SystemTime,
        tags_wire: &str,
    ) -> Result<(), Error> {
        let key_str = key.to_string();
        self.0.insert(
            (&**bucket, upload_id),
            (key_str.as_str(), to_nanos(initiated_at), tags_wire),
        )?;
        Ok(())
    }

    /// Remove one upload row (idempotent).
    pub fn remove(&mut self, bucket: &bucket::Name, upload_id: &str) -> Result<(), Error> {
        self.0.remove((&**bucket, upload_id))?;
        Ok(())
    }

    /// Delete every upload row of `bucket`.
    pub fn drain_bucket(&mut self, bucket: &bucket::Name) -> Result<(), Error> {
        let bucket = &**bucket;
        drain_pair(&mut self.0, (bucket, ""), |b, _| b == bucket)
    }
}

// --- PARTS ---

type PartKey = (&'static str, &'static str, u32);

/// `(bucket, upload_id, part_number)` → etag hex.
const PARTS: TableDefinition<PartKey, &'static str> = TableDefinition::new("parts");

/// Handle to the `PARTS` table (writable or read-only).
pub struct PartsTable<'txn, T>(T, PhantomData<&'txn ()>);

table_impl!(PartsTable, PARTS, PartKey, &'static str);

impl<'txn, T> PartsTable<'txn, T>
where
    T: ReadableTable<PartKey, &'static str>,
{
    /// Stored etag hex of one part, if present.
    pub fn get_hex(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        n: u32,
    ) -> Result<Option<String>, Error> {
        Ok(self
            .0
            .get((&**bucket, upload_id, n))?
            .map(|guard| guard.value().to_string()))
    }

    /// Page of `(part_number, etag_hex)` from `start`, capped at `max`,
    /// with a truncated flag (one lookahead past the page).
    pub fn list_from(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        start: u32,
        max: usize,
    ) -> Result<(Vec<(u32, String)>, bool), Error> {
        let bucket = &**bucket;
        let iter = self.0.range((bucket, upload_id, start)..)?;
        let mut recorded = Vec::new();
        let mut truncated = false;
        for item in iter {
            let (k, v) = item?;
            let (b, id, n) = k.value();
            if b != bucket || id != upload_id {
                break;
            }
            if recorded.len() == max {
                truncated = true;
                break;
            }
            recorded.push((n, v.value().to_string()));
        }
        Ok((recorded, truncated))
    }
}

impl<'txn> PartsTable<'txn, Table<'txn, PartKey, &'static str>> {
    /// Upsert one part etag.
    pub fn put(
        &mut self,
        bucket: &bucket::Name,
        upload_id: &str,
        n: u32,
        etag: &ETag,
    ) -> Result<(), Error> {
        let etag_hex = etag.as_str();
        self.0
            .insert((&**bucket, upload_id, n), etag_hex.as_str())?;
        Ok(())
    }

    /// Delete every part row of `bucket`.
    pub fn drain_bucket(&mut self, bucket: &bucket::Name) -> Result<(), Error> {
        let bucket = &**bucket;
        drain_triple(&mut self.0, (bucket, "", 0), |b, _, _| b == bucket)
    }

    /// Delete every part row of one upload.
    pub fn drain_upload(&mut self, bucket: &bucket::Name, upload_id: &str) -> Result<(), Error> {
        let bucket = &**bucket;
        drain_triple(&mut self.0, (bucket, upload_id, 0), |b, id, _| {
            b == bucket && id == upload_id
        })
    }
}

// --- UPLOAD_CHECKSUMS ---

/// `(bucket, upload_id)` → `(algorithm wire name, checksum-type wire
/// name or "")` — the upload's create-time checksum spec (spec
/// 2026-08-31). `""` for a checksum type that was never fixed.
type UploadChecksumValue = (&'static str, &'static str);
const UPLOAD_CHECKSUMS: TableDefinition<UploadKey, UploadChecksumValue> =
    TableDefinition::new("upload_checksums");

/// Handle to the `UPLOAD_CHECKSUMS` table (writable or read-only).
pub struct UploadChecksumsTable<'txn, T>(T, PhantomData<&'txn ()>);

table_impl!(
    UploadChecksumsTable,
    UPLOAD_CHECKSUMS,
    UploadKey,
    UploadChecksumValue
);

impl<'txn, T> UploadChecksumsTable<'txn, T>
where
    T: ReadableTable<UploadKey, UploadChecksumValue>,
{
    /// The stored row: `(algorithm wire name, checksum-type wire name or
    /// "")` (owned — the guard cannot outlive the closure).
    pub fn get(&self, bucket: &str, upload_id: &str) -> Result<Option<(String, String)>, Error> {
        Ok(self
            .0
            .get((bucket, upload_id))?
            .map(|v| (v.value().0.to_string(), v.value().1.to_string())))
    }
}

impl<'txn> UploadChecksumsTable<'txn, Table<'txn, UploadKey, UploadChecksumValue>> {
    /// Insert or replace the upload's checksum spec.
    pub fn put(
        &mut self,
        bucket: &str,
        upload_id: &str,
        algorithm: &str,
        checksum_type: &str,
    ) -> Result<(), Error> {
        self.0
            .insert((bucket, upload_id), (algorithm, checksum_type))?;
        Ok(())
    }

    /// Remove the row (idempotent).
    pub fn remove(&mut self, bucket: &str, upload_id: &str) -> Result<(), Error> {
        self.0.remove((bucket, upload_id))?;
        Ok(())
    }

    /// Delete every row of `bucket` (bucket teardown).
    pub fn drain_bucket(&mut self, bucket: &bucket::Name) -> Result<(), Error> {
        let bucket = &**bucket;
        drain_pair(&mut self.0, (bucket, ""), |b, _| b == bucket)
    }
}

// --- PART_CHECKSUMS ---

/// `(bucket, upload_id, part_number)` → `(algorithm wire name, base64
/// value)` — one part's computed checksum (spec 2026-08-31).
type PartChecksumValue = (&'static str, &'static str);
const PART_CHECKSUMS: TableDefinition<PartKey, PartChecksumValue> =
    TableDefinition::new("part_checksums");

/// Handle to the `PART_CHECKSUMS` table (writable or read-only).
pub struct PartChecksumsTable<'txn, T>(T, PhantomData<&'txn ()>);

table_impl!(
    PartChecksumsTable,
    PART_CHECKSUMS,
    PartKey,
    PartChecksumValue
);

impl<'txn, T> PartChecksumsTable<'txn, T>
where
    T: ReadableTable<PartKey, PartChecksumValue>,
{
    /// The stored row: `(algorithm wire name, base64 value)` (owned —
    /// the guard cannot outlive the closure).
    pub fn get(
        &self,
        bucket: &str,
        upload_id: &str,
        part_number: u32,
    ) -> Result<Option<(String, String)>, Error> {
        Ok(self
            .0
            .get((bucket, upload_id, part_number))?
            .map(|v| (v.value().0.to_string(), v.value().1.to_string())))
    }
}

impl<'txn> PartChecksumsTable<'txn, Table<'txn, PartKey, PartChecksumValue>> {
    /// Insert or replace the part's checksum.
    pub fn put(
        &mut self,
        bucket: &str,
        upload_id: &str,
        part_number: u32,
        algorithm: &str,
        value: &str,
    ) -> Result<(), Error> {
        self.0
            .insert((bucket, upload_id, part_number), (algorithm, value))?;
        Ok(())
    }

    /// Remove the part's checksum row (idempotent — re-upload clears the
    /// stale value).
    pub fn remove(&mut self, bucket: &str, upload_id: &str, part_number: u32) -> Result<(), Error> {
        self.0.remove((bucket, upload_id, part_number))?;
        Ok(())
    }

    /// Delete every row of one upload (mirror `PartsTable::drain_upload`).
    pub fn drain_upload(&mut self, bucket: &bucket::Name, upload_id: &str) -> Result<(), Error> {
        let bucket = &**bucket;
        drain_triple(&mut self.0, (bucket, upload_id, 0), |b, id, _| {
            b == bucket && id == upload_id
        })
    }

    /// Delete every row of `bucket` (bucket teardown).
    pub fn drain_bucket(&mut self, bucket: &bucket::Name) -> Result<(), Error> {
        let bucket = &**bucket;
        drain_triple(&mut self.0, (bucket, "", 0), |b, _, _| b == bucket)
    }
}

// --- OBJECT_PARTS ---

type ObjectPartKey = (&'static str, &'static str, u32);

/// `(bucket, key, part_number)` → `(size, algorithm wire name or "",
/// base64 checksum value or "")` — the completed object's retained part
/// list (spec 2026-08-31, GetObjectAttributes ObjectParts): the parts
/// the object was composed of at its last multipart completion, in part
/// order, with the stored per-part checksums. `""` marks a part stored
/// without a checksum. The key shape mirrors `PARTS` (same `(bucket,
/// upload-id/object-key, part_number)` ordering).
type ObjectPartValue = (u64, &'static str, &'static str);
const OBJECT_PARTS: TableDefinition<ObjectPartKey, ObjectPartValue> =
    TableDefinition::new("object_parts");

/// Handle to the `OBJECT_PARTS` table (writable or read-only).
pub struct ObjectPartsTable<'txn, T>(T, PhantomData<&'txn ()>);

table_impl!(
    ObjectPartsTable,
    OBJECT_PARTS,
    ObjectPartKey,
    ObjectPartValue
);

impl<'txn, T> ObjectPartsTable<'txn, T>
where
    T: ReadableTable<ObjectPartKey, ObjectPartValue>,
{
    /// The key's rows in part-number order: `(part number, size,
    /// algorithm wire name, base64 checksum value)` (owned — the guard
    /// cannot outlive the closure).
    pub fn list(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Vec<(u32, u64, String, String)>, Error> {
        let bucket = &**bucket;
        let key = &**key;
        let mut out = Vec::new();
        for item in self.0.range((bucket, key, 0)..)? {
            let (k, v) = item?;
            let (b, stored_key, n) = k.value();
            if b != bucket || stored_key != key {
                break;
            }
            let (size, algorithm, value) = v.value();
            out.push((n, size, algorithm.to_string(), value.to_string()));
        }
        Ok(out)
    }
}

impl<'txn> ObjectPartsTable<'txn, Table<'txn, ObjectPartKey, ObjectPartValue>> {
    /// Upsert one part row.
    pub fn put(
        &mut self,
        bucket: &bucket::Name,
        key: &object::Key,
        part_number: u32,
        size: u64,
        algorithm: &str,
        value: &str,
    ) -> Result<(), Error> {
        self.0
            .insert((&**bucket, &**key, part_number), (size, algorithm, value))?;
        Ok(())
    }

    /// Delete every row of `key` — an overwrite/copy/delete must not
    /// leave a completed object's stale parts behind (the new object has
    /// none). Idempotent.
    pub fn remove_key(&mut self, bucket: &bucket::Name, key: &object::Key) -> Result<(), Error> {
        let bucket = &**bucket;
        let key = &**key;
        drain_triple(&mut self.0, (bucket, key, 0), |b, k, _| {
            b == bucket && k == key
        })
    }

    /// Delete every row of `bucket` (bucket teardown).
    pub fn drain_bucket(&mut self, bucket: &bucket::Name) -> Result<(), Error> {
        let bucket = &**bucket;
        drain_triple(&mut self.0, (bucket, "", 0), |b, _, _| b == bucket)
    }
}

// --- STATE ---

type StateKey = &'static str;

/// The `STATE` table's format version — ONE current version, no
/// migration (F06, user decision): any stored version that is not this
/// one is refused on open. The version stays `1` — additive schema
/// changes (the multipart-checksum tables, and the tagging rows /
/// `OBJECT_PARTS` of this change) do NOT bump it (user ruling
/// 2026-09-02: dev-local databases are disposable, no compatibility
/// machinery); any stored version other than `1` is still refused by the
/// gate — a stale v2 database errors with `UnsupportedVersion` and the
/// operator deletes it, and a stale same-version database written in an
/// older row format may error at row decode — the same remedy.
const STATE_VERSION: u64 = 1;
/// The `STATE` version key.
const STATE_VERSION_KEY: &str = "version";
/// The `STATE` compact-needed marker key (0 = clean, 1 = needs compact).
const COMPACT_NEEDED_KEY: &str = "compact_needed";

/// `"version"` → the current [`STATE_VERSION`]; `"compact_needed"` → 0/1
/// (compact marker).
const STATE: TableDefinition<StateKey, u64> = TableDefinition::new("state");

/// Handle to the `STATE` table (writable or read-only).
pub struct StateTable<'txn, T>(T, PhantomData<&'txn ()>);

table_impl!(StateTable, STATE, StateKey, u64, no_ensure);

impl<'txn, T> StateTable<'txn, T>
where
    T: ReadableTable<StateKey, u64>,
{
    /// One owned value for `key`, if present.
    fn stored(&self, key: &str) -> Result<Option<u64>, Error> {
        Ok(self.0.get(key)?.map(|guard| guard.value()))
    }

    /// Read the `compact_needed` marker (absent → `false`).
    pub fn compact_marker(&self) -> Result<bool, Error> {
        Ok(self.stored(COMPACT_NEEDED_KEY)?.is_some_and(|v| v != 0))
    }
}

impl<'txn> StateTable<'txn, Table<'txn, StateKey, u64>> {
    /// Check the format version: write on first open, reject ANY
    /// mismatch (one current version — F06; no migration).
    pub fn ensure_version(&mut self, path: &Path) -> Result<u64, Error> {
        match self.stored(STATE_VERSION_KEY)? {
            None => {
                self.0.insert(STATE_VERSION_KEY, STATE_VERSION)?;
                Ok(STATE_VERSION)
            }
            Some(found) if found == STATE_VERSION => Ok(found),
            Some(found) => Err(unsupported_version(path, found, STATE_VERSION)),
        }
    }

    /// Write the `compact_needed` marker (`false` = clean).
    pub fn set_compact_marker_value(&mut self, needed: bool) -> Result<(), Error> {
        self.0.insert(COMPACT_NEEDED_KEY, u64::from(needed))?;
        Ok(())
    }
}
