//! `name` → `(created-at unix nanos, tags wire)` — the tags element is
//! empty when the bucket has none (spec 2026-08-31).

use std::time::SystemTime;

use redb::{ReadableTable, TableDefinition};
use tinio_core::{from_nanos, to_nanos};

use crate::{
    error::Error,
    table::{self, TableDef},
};

/// The per-table marker: the table definition for the shared handle arms.
#[doc(hidden)]
pub enum Def {}

impl TableDef for Def {
    type Key = &'static str;
    type Value = (u64, &'static str);

    const DEF: TableDefinition<'static, Self::Key, Self::Value> = TableDefinition::new("buckets");
}

/// Handle to the buckets table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// Whether `name` is recorded.
    pub fn exists(&self, name: &str) -> Result<bool, Error> {
        Ok(self.0.get(name)?.is_some())
    }

    /// Creation time of `name`, if recorded.
    pub fn get(&self, name: &str) -> Result<Option<SystemTime>, Error> {
        Ok(self.0.get(name)?.map(|guard| from_nanos(guard.value().0)))
    }

    /// The stored row of `name`: `(creation time, tags wire raw)` (owned —
    /// the guard cannot outlive the closure).
    pub fn row(&self, name: &str) -> Result<Option<(SystemTime, String)>, Error> {
        Ok(self
            .0
            .get(name)?
            .map(|guard| (from_nanos(guard.value().0), guard.value().1.to_string())))
    }

    /// Visit every recorded bucket in name order.
    pub fn for_each<F>(&self, mut visit: F) -> Result<(), Error>
    where
        F: FnMut(&str, SystemTime) -> Result<(), Error>,
    {
        for item in self.0.iter()? {
            let (k, v) = item?;
            visit(k.value(), from_nanos(v.value().0))?;
        }
        Ok(())
    }
}

impl<'txn> table::Table<'txn, Def> {
    /// Record (or overwrite) the creation time of `name` — the tags
    /// element is cleared (a fresh row has no tags; the tagging ops use
    /// [`Self::put_full`] to preserve the creation time).
    pub fn put(&mut self, name: &str, created_at: SystemTime) -> Result<(), Error> {
        self.put_full(name, created_at, "")
    }

    /// Record (or overwrite) the whole row: the creation time AND the
    /// tags wire (`put_bucket_tags`'s row upsert — the creation time is
    /// preserved from the stored row, first-sighted when absent).
    pub fn put_full(
        &mut self,
        name: &str,
        created_at: SystemTime,
        tags_wire: &str,
    ) -> Result<(), Error> {
        self.0.insert(name, (to_nanos(created_at), tags_wire))?;
        Ok(())
    }

    /// Insert `now` when absent; return the stored creation time. The
    /// stored row's tags element rides the re-insert (a first-sight
    /// upsert must not clear the tag set the tagging write just recorded
    /// — mirror the `put_full`-style callers, which keep
    /// `(created_at, existing_tags)`).
    pub fn get_or_insert(&mut self, name: &str, now: SystemTime) -> Result<SystemTime, Error> {
        let (created, tags) = match self.0.get(name)? {
            Some(guard) => (guard.value().0, guard.value().1.to_string()),
            None => (to_nanos(now), String::new()),
        };
        self.0.insert(name, (created, tags.as_str()))?;
        Ok(from_nanos(created))
    }

    /// Remove the entry of `name` (idempotent).
    pub fn remove(&mut self, name: &str) -> Result<(), Error> {
        self.0.remove(name)?;
        Ok(())
    }
}
