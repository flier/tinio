//! `(bucket, upload_id, part_number)` → part bytes (mem-local content
//! table — no fs counterpart; the fs backend keeps parts on disk and
//! stores only their etags in [`crate::part`]).

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
    type Value = &'static [u8];

    const DEF: TableDefinition<'static, Self::Key, Self::Value> = TableDefinition::new("part_data");
}

/// Handle to the part-data table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// The part bytes of `(bucket, upload_id, part_number)`, if present
    /// (zero-copy guard).
    pub fn get(
        &self,
        bucket: &str,
        upload_id: &str,
        part_number: u32,
    ) -> Result<Option<redb::AccessGuard<'_, &'static [u8]>>, Error> {
        Ok(self.0.get((bucket, upload_id, part_number))?)
    }

    /// The total part bytes of one upload — the abort accounting (the
    /// removed byte count), walked before the drain.
    pub fn total_len(&self, bucket: &str, upload_id: &str) -> Result<u64, Error> {
        let mut total = 0u64;
        for item in self.0.range((bucket, upload_id, 0)..)? {
            let (k, v) = item?;
            let (b, id, _) = k.value();
            if b != bucket || id != upload_id {
                break;
            }
            total += v.value().len() as u64;
        }
        Ok(total)
    }
}

impl<'txn> table::Table<'txn, Def> {
    /// Upsert the part bytes of `(bucket, upload_id, part_number)`.
    pub fn put(
        &mut self,
        bucket: &str,
        upload_id: &str,
        part_number: u32,
        data: &[u8],
    ) -> Result<(), Error> {
        self.0.insert((bucket, upload_id, part_number), data)?;
        Ok(())
    }

    /// Delete every part row of one upload.
    pub fn drain_upload(&mut self, bucket: &str, upload_id: &str) -> Result<(), Error> {
        drain_triple(&mut self.0, (bucket, upload_id, 0), |b, id, _| {
            b == bucket && id == upload_id
        })
    }
}
