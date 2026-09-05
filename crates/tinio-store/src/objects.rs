//! `(bucket, key)` → object bytes (mem-local content table — no fs
//! counterpart; the fs backend stores objects on disk).

use redb::{ReadableTable, TableDefinition};

use crate::{
    error::Error,
    scan::has_prefix_pair,
    table::{self, TableDef},
};

/// The per-table marker: the table definition for the shared handle arms.
#[doc(hidden)]
pub enum Def {}

impl TableDef for Def {
    type Key = (&'static str, &'static str);
    type Value = &'static [u8];

    const DEF: TableDefinition<'static, Self::Key, Self::Value> = TableDefinition::new("objects");
}

/// Handle to the objects table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// The object bytes of `key`, if present (zero-copy guard — the
    /// caller slices/copies inside the transaction).
    pub fn get(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<redb::AccessGuard<'_, &'static [u8]>>, Error> {
        Ok(self.0.get((bucket, key))?)
    }

    /// Whether `bucket` has any object row. The entries of one bucket are
    /// contiguous from `(bucket, "")` — the first key at or after the
    /// lower bound is in the bucket iff any exist.
    pub fn has_bucket(&self, bucket: &str) -> Result<bool, Error> {
        has_prefix_pair(&self.0, (bucket, ""), |b, _| b == bucket)
    }
}

impl<'txn> table::Table<'txn, Def> {
    /// Upsert the object bytes of `key`.
    pub fn put(&mut self, bucket: &str, key: &str, data: &[u8]) -> Result<(), Error> {
        self.0.insert((bucket, key), data)?;
        Ok(())
    }

    /// Remove the object bytes of `key` (idempotent).
    pub fn remove(&mut self, bucket: &str, key: &str) -> Result<(), Error> {
        self.0.remove((bucket, key))?;
        Ok(())
    }
}
