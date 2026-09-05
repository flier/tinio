//! `name` → `(created-at unix nanos, tags wire, owner wire, acl wire,
//! cors wire)` — a wire element is `''` when the bucket has none (the
//! owner/ACL elements ride the ACL plan, the CORS element the CORS plan,
//! spec 2026-09-05).

use std::time::SystemTime;

use redb::{ReadableTable, TableDefinition};
use tinio_core::{cors::CorsConfig, from_nanos, to_nanos};

use crate::{
    error::Error,
    table::{self, TableDef},
};

/// The per-table marker: the table definition for the shared handle arms.
#[doc(hidden)]
pub enum Def {}

impl TableDef for Def {
    type Key = &'static str;
    type Value = (u64, &'static str, &'static str, &'static str, &'static str);

    const DEF: TableDefinition<'static, Self::Key, Self::Value> = TableDefinition::new("buckets");
}

/// Handle to the buckets table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

/// Self-healing decode of the stored CORS wire: an empty or corrupt wire
/// is an empty config (`''` = "no configuration" = 404 on get).
///
/// Interim: dead until the CORS data plane lands (Task 5's fs/mem
/// write/read paths consume it through the store, where it goes `pub`).
#[allow(dead_code)]
pub(crate) fn decode_cors_wire(wire: &str) -> CorsConfig {
    if wire.is_empty() {
        CorsConfig::default()
    } else {
        CorsConfig::from_wire(wire)
    }
}

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

    /// The stored row of `name`: `(creation time, tags, owner, acl, cors)`
    /// — the four wires raw (owned; the guard cannot outlive the closure).
    #[allow(clippy::type_complexity)] // the 5-tuple IS the pinned row shape
    pub fn row(
        &self,
        name: &str,
    ) -> Result<Option<(SystemTime, String, String, String, String)>, Error> {
        Ok(self.0.get(name)?.map(|guard| {
            let (created, tags, owner, acl, cors) = guard.value();
            (
                from_nanos(created),
                tags.to_string(),
                owner.to_string(),
                acl.to_string(),
                cors.to_string(),
            )
        }))
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
    /// Record (or overwrite) the creation time of `name` — the four wire
    /// elements are cleared (a fresh row has no tags/owner/ACL/CORS; the
    /// tagging ops use [`Self::put_full`] to preserve the creation time).
    pub fn put(&mut self, name: &str, created_at: SystemTime) -> Result<(), Error> {
        self.put_full(name, created_at, "", "", "", "")
    }

    /// Record (or overwrite) the whole row: the creation time AND the four
    /// wire elements (`put_bucket_tags`'s row upsert — the creation time
    /// is preserved from the stored row, first-sighted when absent).
    pub fn put_full(
        &mut self,
        name: &str,
        created_at: SystemTime,
        tags_wire: &str,
        owner_wire: &str,
        acl_wire: &str,
        cors_wire: &str,
    ) -> Result<(), Error> {
        self.0.insert(
            name,
            (
                to_nanos(created_at),
                tags_wire,
                owner_wire,
                acl_wire,
                cors_wire,
            ),
        )?;
        Ok(())
    }

    /// Insert `now` when absent; return the stored creation time. The
    /// stored row's wires ride the re-insert (a first-sight upsert must
    /// not clear the tag set the tagging write just recorded — mirror the
    /// `put_full`-style callers, which keep all wires).
    pub fn get_or_insert(&mut self, name: &str, now: SystemTime) -> Result<SystemTime, Error> {
        let (created, tags, owner, acl, cors) = match self.0.get(name)? {
            Some(guard) => {
                let (created, tags, owner, acl, cors) = guard.value();
                (
                    created,
                    tags.to_string(),
                    owner.to_string(),
                    acl.to_string(),
                    cors.to_string(),
                )
            }
            None => (
                to_nanos(now),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
            ),
        };
        self.0.insert(
            name,
            (
                created,
                tags.as_str(),
                owner.as_str(),
                acl.as_str(),
                cors.as_str(),
            ),
        )?;
        Ok(from_nanos(created))
    }

    /// Remove the entry of `name` (idempotent).
    pub fn remove(&mut self, name: &str) -> Result<(), Error> {
        self.0.remove(name)?;
        Ok(())
    }
}
