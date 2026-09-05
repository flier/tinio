//! `(bucket, upload_id, part_number)` → etag hex.

use redb::{ReadableTable, TableDefinition};
use tinio_core::etag::ETag;

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
    type Value = &'static str;

    const DEF: TableDefinition<'static, Self::Key, Self::Value> = TableDefinition::new("parts");
}

/// Handle to the parts table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// Stored etag hex of one part, if present.
    pub fn get_hex(&self, bucket: &str, upload_id: &str, n: u32) -> Result<Option<String>, Error> {
        Ok(self
            .0
            .get((bucket, upload_id, n))?
            .map(|guard| guard.value().to_string()))
    }

    /// Page of `(part_number, etag_hex)` from `start`, capped at `max`,
    /// with a truncated flag (one lookahead past the page).
    pub fn list_from(
        &self,
        bucket: &str,
        upload_id: &str,
        start: u32,
        max: usize,
    ) -> Result<(Vec<(u32, String)>, bool), Error> {
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

impl<'txn> table::Table<'txn, Def> {
    /// Upsert one part etag.
    pub fn put(&mut self, bucket: &str, upload_id: &str, n: u32, etag: &ETag) -> Result<(), Error> {
        let etag_hex = etag.as_str();
        self.0.insert((bucket, upload_id, n), etag_hex.as_str())?;
        Ok(())
    }

    /// Delete every part row of `bucket`.
    pub fn drain_bucket(&mut self, bucket: &str) -> Result<(), Error> {
        drain_triple(&mut self.0, (bucket, "", 0), |b, _, _| b == bucket)
    }

    /// Delete every part row of one upload.
    pub fn drain_upload(&mut self, bucket: &str, upload_id: &str) -> Result<(), Error> {
        drain_triple(&mut self.0, (bucket, upload_id, 0), |b, id, _| {
            b == bucket && id == upload_id
        })
    }
}
