//! `(bucket, key, part_number)` → `(size, algorithm wire name or "",
//! base64 checksum value or "")` — the completed object's retained part
//! list (spec 2026-08-31, GetObjectAttributes ObjectParts): the parts
//! the object was composed of at its last multipart completion, in part
//! order, with the stored per-part checksums. `""` marks a part stored
//! without a checksum. The key shape mirrors `PARTS` (same `(bucket,
//! upload-id/object-key, part_number)` ordering).

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
    type Value = (u64, &'static str, &'static str);

    const DEF: TableDefinition<'static, Self::Key, Self::Value> =
        TableDefinition::new("object_parts");
}

/// Handle to the object-parts table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// The key's rows in part-number order: `(part number, size,
    /// algorithm wire name, base64 checksum value)` (owned — the guard
    /// cannot outlive the closure).
    pub fn list(&self, bucket: &str, key: &str) -> Result<Vec<(u32, u64, String, String)>, Error> {
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

impl<'txn> table::Table<'txn, Def> {
    /// Upsert one part row.
    pub fn put(
        &mut self,
        bucket: &str,
        key: &str,
        part_number: u32,
        size: u64,
        algorithm: &str,
        value: &str,
    ) -> Result<(), Error> {
        self.0
            .insert((bucket, key, part_number), (size, algorithm, value))?;
        Ok(())
    }

    /// Delete every row of `key` — an overwrite/copy/delete must not
    /// leave a completed object's stale parts behind (the new object has
    /// none). Idempotent.
    pub fn remove_key(&mut self, bucket: &str, key: &str) -> Result<(), Error> {
        drain_triple(&mut self.0, (bucket, key, 0), |b, k, _| {
            b == bucket && k == key
        })
    }

    /// Delete every row of `bucket` (bucket teardown).
    pub fn drain_bucket(&mut self, bucket: &str) -> Result<(), Error> {
        drain_triple(&mut self.0, (bucket, "", 0), |b, _, _| b == bucket)
    }
}
