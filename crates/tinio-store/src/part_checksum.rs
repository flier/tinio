//! `(bucket, upload_id, part_number)` → `(algorithm wire name, base64
//! value)` — one part's computed checksum (spec 2026-08-31).

use redb::{ReadableTable, TableDefinition};

use crate::{
    error::Error,
    scan::{drain_triple, has_prefix_triple},
    table::{self, TableDef},
};

/// The per-table marker: the table definition for the shared handle arms.
#[doc(hidden)]
pub enum Def {}

impl TableDef for Def {
    type Key = (&'static str, &'static str, u32);
    type Value = (&'static str, &'static str);

    const DEF: TableDefinition<'static, Self::Key, Self::Value> =
        TableDefinition::new("part_checksums");
}

/// Handle to the part-checksums table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
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

    /// Whether `upload_id` has any checksum row (the list-parts "rows may
    /// race" probe — one contiguous block from `(bucket, upload_id, 0)`).
    pub fn has_upload(&self, bucket: &str, upload_id: &str) -> Result<bool, Error> {
        has_prefix_triple(&self.0, (bucket, upload_id, 0), |b, id, _| {
            b == bucket && id == upload_id
        })
    }
}

impl<'txn> table::Table<'txn, Def> {
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

    /// Delete every row of one upload (mirror [`crate::part::Table::drain_upload`]).
    pub fn drain_upload(&mut self, bucket: &str, upload_id: &str) -> Result<(), Error> {
        drain_triple(&mut self.0, (bucket, upload_id, 0), |b, id, _| {
            b == bucket && id == upload_id
        })
    }

    /// Delete every row of `bucket` (bucket teardown).
    pub fn drain_bucket(&mut self, bucket: &str) -> Result<(), Error> {
        drain_triple(&mut self.0, (bucket, "", 0), |b, _, _| b == bucket)
    }
}
