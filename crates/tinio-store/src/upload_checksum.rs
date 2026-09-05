//! `(bucket, upload_id)` → `(algorithm wire name, checksum-type wire
//! name or "")` — the upload's create-time checksum spec (spec
//! 2026-08-31). `""` for a checksum type that was never fixed.

use redb::{ReadableTable, TableDefinition};

use crate::{
    error::Error,
    scan::drain_pair,
    table::{self, TableDef},
};

/// The per-table marker: the table definition for the shared handle arms.
#[doc(hidden)]
pub enum Def {}

impl TableDef for Def {
    type Key = (&'static str, &'static str);
    type Value = (&'static str, &'static str);

    const DEF: TableDefinition<'static, Self::Key, Self::Value> =
        TableDefinition::new("upload_checksums");
}

/// Handle to the upload-checksums table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
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

impl<'txn> table::Table<'txn, Def> {
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
    pub fn drain_bucket(&mut self, bucket: &str) -> Result<(), Error> {
        drain_pair(&mut self.0, (bucket, ""), |b, _| b == bucket)
    }
}
