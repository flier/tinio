//! `(bucket, upload_id)` → `(algorithm wire name, checksum-type wire
//! name or "")` — the upload's create-time checksum spec (spec
//! 2026-08-31). `""` for a checksum type that was never fixed.

use redb::{ReadableTable, TableDefinition};

use crate::{
    _core::checksum,
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
    /// The stored create-time spec. `None` is a missing row or a
    /// domain-invalid wire (self-healing — the upload is served without
    /// a spec, F07).
    pub fn get(&self, bucket: &str, upload_id: &str) -> Result<Option<checksum::Upload>, Error> {
        Ok(self
            .0
            .get((bucket, upload_id))?
            .and_then(|v| checksum::Upload::from_wire_opt(v.value().0, v.value().1)))
    }
}

impl<'txn> table::Table<'txn, Def> {
    /// Insert or replace the upload's checksum spec (encoded here).
    pub fn put(
        &mut self,
        bucket: &str,
        upload_id: &str,
        spec: &checksum::Upload,
    ) -> Result<(), Error> {
        let (algorithm, checksum_type) = spec.to_wire();
        self.0.insert(
            (bucket, upload_id),
            (algorithm.as_str(), checksum_type.as_str()),
        )?;
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
