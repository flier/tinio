//! `(bucket, upload_id)` → `(key, initiated-at unix nanos, tags wire)` —
//! the tags element is the create-time object tag set (spec 2026-08-31,
//! applied to the completed object; empty when none).

use std::time::SystemTime;

use redb::{ReadableTable, TableDefinition};
use tinio_core::to_nanos;

use crate::{
    error::Error,
    scan::{drain_pair, for_each_pair, has_prefix_pair},
    table::{self, TableDef},
};

/// The per-table marker: the table definition for the shared handle arms.
#[doc(hidden)]
pub enum Def {}

impl TableDef for Def {
    type Key = (&'static str, &'static str);
    type Value = (&'static str, u64, &'static str);

    const DEF: TableDefinition<'static, Self::Key, Self::Value> = TableDefinition::new("uploads");
}

/// Handle to the uploads table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// Whether `bucket` has any upload row. Entries of one bucket are
    /// contiguous from `(bucket, "")` — the first key at or after that
    /// lower bound is in the bucket iff any exist (see [`Self::drain_bucket`]).
    pub fn has_bucket(&self, bucket: &str) -> Result<bool, Error> {
        has_prefix_pair(&self.0, (bucket, ""), |b, _| b == bucket)
    }

    /// Whether the upload exists and records `key` (S3 identity is
    /// `(bucket, key, uploadId)`).
    pub fn key_matches(&self, bucket: &str, key: &str, upload_id: &str) -> Result<bool, Error> {
        Ok(self
            .0
            .get((bucket, upload_id))?
            .map(|guard| guard.value().0 == key)
            .unwrap_or(false))
    }

    /// The stored row, present only when the upload exists AND records
    /// `key` (S3 identity is `(bucket, key, uploadId)`) — the
    /// `key_matches` + `get` pair of `get_upload` in one lookup. Returns
    /// `(key, initiated-at, tags wire)` (owned — the guard cannot outlive
    /// the closure).
    pub fn get_matching(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Option<(String, u64, String)>, Error> {
        Ok(self
            .0
            .get((bucket, upload_id))?
            .map(|guard| {
                (
                    guard.value().0.to_string(),
                    guard.value().1,
                    guard.value().2.to_string(),
                )
            })
            .filter(|(stored_key, _, _)| stored_key == key))
    }

    /// Visit every upload row of `bucket` (contiguous from `(bucket, "")`).
    pub fn for_bucket<F>(&self, bucket: &str, mut visit: F) -> Result<(), Error>
    where
        F: FnMut(&str, (&str, u64, &str)) -> Result<(), Error>,
    {
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

impl<'txn> table::Table<'txn, Def> {
    /// Upsert one upload row — the create-time tags wire rides in the
    /// row (spec 2026-08-31).
    pub fn put(
        &mut self,
        bucket: &str,
        upload_id: &str,
        key: &str,
        initiated_at: SystemTime,
        tags_wire: &str,
    ) -> Result<(), Error> {
        let key_str = key.to_string();
        self.0.insert(
            (bucket, upload_id),
            (key_str.as_str(), to_nanos(initiated_at), tags_wire),
        )?;
        Ok(())
    }

    /// Remove one upload row (idempotent).
    pub fn remove(&mut self, bucket: &str, upload_id: &str) -> Result<(), Error> {
        self.0.remove((bucket, upload_id))?;
        Ok(())
    }

    /// Delete every upload row of `bucket`.
    pub fn drain_bucket(&mut self, bucket: &str) -> Result<(), Error> {
        drain_pair(&mut self.0, (bucket, ""), |b, _| b == bucket)
    }
}
