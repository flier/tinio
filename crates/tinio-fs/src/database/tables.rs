//! Typed handles to redb tables (`StateTable`, `BucketsTable`, …).

use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
    path::Path,
    time::SystemTime,
};

use redb::{ReadableTable, Table, TableDefinition};
use tinio_core::{etag::ETag, from_nanos, object, to_nanos};

use super::{
    error::{Error, corrupt_meta, unsupported_version},
    scan::{drain_pair, drain_triple, for_each_pair},
};
use crate::bucket;

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

/// `name` → created-at unix nanos.
const BUCKETS: TableDefinition<BucketKey, u64> = TableDefinition::new("buckets");

/// Handle to the `BUCKETS` table (writable or read-only).
pub struct BucketsTable<'txn, T>(T, PhantomData<&'txn ()>);

table_impl!(BucketsTable, BUCKETS, BucketKey, u64);

impl<'txn, T> BucketsTable<'txn, T>
where
    T: ReadableTable<BucketKey, u64>,
{
    /// Creation time of `name`, if recorded.
    pub fn get(&self, name: &bucket::Name) -> Result<Option<SystemTime>, Error> {
        Ok(self.0.get(&**name)?.map(|guard| from_nanos(guard.value())))
    }

    /// Visit every recorded bucket in name order.
    pub fn for_each<F>(&self, mut visit: F) -> Result<(), Error>
    where
        F: FnMut(&str, SystemTime) -> Result<(), Error>,
    {
        for item in self.0.iter()? {
            let (k, v) = item?;
            visit(k.value(), from_nanos(v.value()))?;
        }
        Ok(())
    }
}

impl<'txn> BucketsTable<'txn, Table<'txn, BucketKey, u64>> {
    /// Record (or overwrite) the creation time of `name`.
    pub fn put(&mut self, name: &bucket::Name, created_at: SystemTime) -> Result<(), Error> {
        self.0.insert(&**name, to_nanos(created_at))?;
        Ok(())
    }

    /// Insert `now` when absent; return the stored creation time.
    pub fn get_or_insert(
        &mut self,
        name: &bucket::Name,
        now: SystemTime,
    ) -> Result<SystemTime, Error> {
        let created = match self.0.get(&**name)? {
            Some(guard) => guard.value(),
            None => to_nanos(now),
        };
        self.0.insert(&**name, created)?;
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
type MetaValue = (&'static str, u64, u64, u64);

/// `(bucket, key)` → `(etag hex, size, mtime unix nanos, file identity)`.
const OBJECT_META: TableDefinition<MetaKey, MetaValue> = TableDefinition::new("object_meta");

/// Handle to the `OBJECT_META` table (writable or read-only).
pub struct ObjectMetaTable<'txn, T>(T, PhantomData<&'txn ()>);

table_impl!(ObjectMetaTable, OBJECT_META, MetaKey, MetaValue);

/// One stored `OBJECT_META` entry, validated into domain types (the row
/// shape is `(etag hex, size, mtime unix nanos, file identity)`).
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
}

/// Validate one raw `OBJECT_META` row into [`StoredMeta`] — `None` on a
/// domain-invalid etag (self-healing: the caller treats it as missing
/// and recomputes). Shared by the point read [`ObjectMetaTable::get`]
/// and the gating traversal [`ObjectMetaTable::for_bucket_gated`] — the
/// single home of the rule.
fn validate_stored(
    (etag, size, mtime, file_identity): (&str, u64, u64, u64),
) -> Option<StoredMeta> {
    Some(StoredMeta {
        etag: ETag::new(etag).ok()?,
        size,
        mtime,
        file_identity,
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
            |_, raw_key, (etag, size, mtime, _)| {
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
            |_, raw_key, (etag, size, mtime, file_identity)| {
                let Ok(key) = object::key(raw_key) else {
                    return Ok(()); // invalid key domain → skip the row
                };
                // Same row validation as the point read and the gate
                // (invalid etag → None — self-healing).
                let stored = validate_stored((etag, size, mtime, file_identity));
                visit(key, stored)
            },
        )
    }
}

impl<'txn> ObjectMetaTable<'txn, Table<'txn, MetaKey, MetaValue>> {
    /// Upsert the meta entry for `key`.
    pub fn put(
        &mut self,
        bucket: &bucket::Name,
        key: &object::Key,
        etag: &ETag,
        size: u64,
        mtime: SystemTime,
        identity: u64,
    ) -> Result<(), Error> {
        let etag_hex = etag.as_str();
        self.0.insert(
            (&**bucket, &**key),
            (etag_hex.as_str(), size, to_nanos(mtime), identity),
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
type UploadValue = (&'static str, u64);

/// `(bucket, upload_id)` → `(key, initiated-at unix nanos)`.
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

    /// Visit every upload row of `bucket` (contiguous from `(bucket, "")`).
    pub fn for_bucket<F>(&self, bucket: &bucket::Name, mut visit: F) -> Result<(), Error>
    where
        F: FnMut(&str, (&str, u64)) -> Result<(), Error>,
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
        F: FnMut(&str, &str, &str, u64) -> Result<(), Error>,
    {
        for item in self.0.iter()? {
            let (k, v) = item?;
            let (b, upload_id) = k.value();
            let (key, initiated_at) = v.value();
            visit(b, upload_id, key, initiated_at)?;
        }
        Ok(())
    }
}

impl<'txn> UploadsTable<'txn, Table<'txn, UploadKey, UploadValue>> {
    /// Upsert one upload row.
    pub fn put(
        &mut self,
        bucket: &bucket::Name,
        upload_id: &str,
        key: &object::Key,
        initiated_at: SystemTime,
    ) -> Result<(), Error> {
        let key_str = key.to_string();
        self.0.insert(
            (&**bucket, upload_id),
            (key_str.as_str(), to_nanos(initiated_at)),
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

// --- STATE ---

type StateKey = &'static str;

/// The `STATE` table's format version.
const STATE_VERSION: u64 = 1;
/// The `STATE` version key.
const STATE_VERSION_KEY: &str = "version";
/// The `STATE` compact-needed marker key (0 = clean, 1 = needs compact).
const COMPACT_NEEDED_KEY: &str = "compact_needed";

/// `"version"` → 1; `"compact_needed"` → 0/1 (compact marker).
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
    /// Check the format version: write on first open, reject on mismatch.
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
