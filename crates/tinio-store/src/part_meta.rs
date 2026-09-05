//! `(bucket, upload_id, part_number)` → `(size, mtime)`, the mem-local
//! part stat rows (the fs backend reads size/mtime from the file stat —
//! mem stores the pair alongside the content bytes).

use redb::{ReadableTable, TableDefinition};

use crate::{
    error::Error,
    scan::drain_triple,
    table::{self, TableDef},
};

/// The per-table marker: the table definition for the shared handle arms.
#[doc(hidden)]
pub enum Def {}

impl TableDef for Def {
    type Key = (&'static str, &'static str, u32);
    type Value = (u64, u64);

    const DEF: TableDefinition<'static, Self::Key, Self::Value> = TableDefinition::new("part_meta");
}

/// Handle to the part-meta table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// The stored `(size, mtime unix nanos)` of one part, if present.
    pub fn get(
        &self,
        bucket: &str,
        upload_id: &str,
        part_number: u32,
    ) -> Result<Option<(u64, u64)>, Error> {
        Ok(self
            .0
            .get((bucket, upload_id, part_number))?
            .map(|g| g.value()))
    }
}

impl<'txn> table::Table<'txn, Def> {
    /// Upsert the `(size, mtime)` row of one part.
    pub fn put(
        &mut self,
        bucket: &str,
        upload_id: &str,
        part_number: u32,
        size: u64,
        mtime: u64,
    ) -> Result<(), Error> {
        self.0
            .insert((bucket, upload_id, part_number), (size, mtime))?;
        Ok(())
    }

    /// Delete every part row of one upload.
    pub fn drain_upload(&mut self, bucket: &str, upload_id: &str) -> Result<(), Error> {
        drain_triple(&mut self.0, (bucket, upload_id, 0), |b, id, _| {
            b == bucket && id == upload_id
        })
    }
}
