//! `name` → `(created-at unix nanos, tags wire, owner wire, acl wire,
//! cors wire)` — a wire element is `''` when the bucket has none (the
//! owner/ACL elements ride the ACL plan, the CORS element the CORS plan,
//! spec 2026-09-05).

use std::time::SystemTime;

use redb::{ReadableTable, TableDefinition};
use tinio_core::{cors, from_nanos, to_nanos};

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

/// The stored bucket row, bundled so the wire elements are accessed by
/// name. The redb VALUE stays the pinned 5-tuple — the conversion lives
/// in `BucketRow::from_value`/`BucketRow::to_value`, the only place
/// that touches element positions (the arity/order pins in the store
/// tests guard the tuple itself).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketRow {
    pub created: SystemTime,
    pub tags: String,
    pub owner: String,
    pub acl: String,
    pub cors: String,
}

impl BucketRow {
    /// A fresh row: the creation time with no wire elements.
    pub fn at(created: SystemTime) -> Self {
        Self {
            created,
            tags: String::new(),
            owner: String::new(),
            acl: String::new(),
            cors: String::new(),
        }
    }

    fn from_value(value: (u64, &str, &str, &str, &str)) -> Self {
        let (created, tags, owner, acl, cors) = value;
        Self {
            created: from_nanos(created),
            tags: tags.to_string(),
            owner: owner.to_string(),
            acl: acl.to_string(),
            cors: cors.to_string(),
        }
    }

    fn to_value(&self) -> (u64, &str, &str, &str, &str) {
        (
            to_nanos(self.created),
            &self.tags,
            &self.owner,
            &self.acl,
            &self.cors,
        )
    }
}

/// Self-healing decode of the stored CORS wire: an empty or corrupt wire
/// is "no configuration" (`None`; the `''` wire = 404 on get), a
/// decodable wire is the config (`Some`). The G2 normalization stops
/// here — callers never filter empty rule sets themselves. The encode
/// half is [`cors::Config::to_wire`]; both ride [`Table::cors`] /
/// [`Table::put_cors`].
fn decode_cors_wire(wire: &str) -> Option<cors::Config> {
    let config = cors::Config::from_wire(wire);
    (!config.rules.is_empty()).then_some(config)
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

    /// The stored row of `name` (owned; the guard cannot outlive the
    /// closure).
    pub fn row(&self, name: &str) -> Result<Option<BucketRow>, Error> {
        Ok(self
            .0
            .get(name)?
            .map(|guard| BucketRow::from_value(guard.value())))
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

    /// The stored CORS configuration of `name`. Outer `None` is a missing
    /// row (the caller maps that to `NoSuchBucket` or "no configuration"
    /// per backend); inner `None` is the `''`/corrupt wire (self-healing).
    pub fn cors(&self, name: &str) -> Result<Option<Option<cors::Config>>, Error> {
        Ok(self.row(name)?.map(|row| decode_cors_wire(&row.cors)))
    }
}

impl<'txn> table::Table<'txn, Def> {
    /// Record (or overwrite) the creation time of `name` — the four wire
    /// elements are cleared (a fresh row has no tags/owner/ACL/CORS; the
    /// tagging ops use [`Self::put_full`] to preserve the creation time).
    pub fn put(&mut self, name: &str, created_at: SystemTime) -> Result<(), Error> {
        self.put_full(name, &BucketRow::at(created_at))
    }

    /// Record (or overwrite) the whole row: the creation time AND the four
    /// wire elements (`put_bucket_tags`'s row upsert — the creation time
    /// is preserved from the stored row, first-sighted when absent).
    pub fn put_full(&mut self, name: &str, row: &BucketRow) -> Result<(), Error> {
        self.0.insert(name, row.to_value())?;
        Ok(())
    }

    /// Insert `now` when absent; return the stored creation time. The
    /// stored row's wires ride the re-insert (a first-sight upsert must
    /// not clear the tag set the tagging write just recorded — mirror the
    /// `put_full`-style callers, which keep all wires).
    pub fn get_or_insert(&mut self, name: &str, now: SystemTime) -> Result<SystemTime, Error> {
        let row = match self.0.get(name)? {
            Some(guard) => BucketRow::from_value(guard.value()),
            None => BucketRow::at(now),
        };
        let created = row.created;
        self.0.insert(name, row.to_value())?;
        Ok(created)
    }

    /// Remove the entry of `name` (idempotent).
    pub fn remove(&mut self, name: &str) -> Result<(), Error> {
        self.0.remove(name)?;
        Ok(())
    }

    /// Replace the CORS configuration of `name` (empty rules → `''` wire
    /// by the codec). `Ok(None)` = no row; `Ok(Some(false))` = identical
    /// wire (no write); `Ok(Some(true))` = rewritten.
    pub fn put_cors(&mut self, name: &str, config: &cors::Config) -> Result<Option<bool>, Error> {
        self.rewrite_cors(name, &config.to_wire())
    }

    /// Clear the CORS configuration of `name` (`''` wire). Same
    /// presence/change outcome as [`Self::put_cors`].
    pub fn clear_cors(&mut self, name: &str) -> Result<Option<bool>, Error> {
        self.rewrite_cors(name, "")
    }

    fn rewrite_cors(&mut self, name: &str, wire: &str) -> Result<Option<bool>, Error> {
        let Some(row) = self.row(name)? else {
            return Ok(None);
        };
        if row.cors == wire {
            return Ok(Some(false));
        }
        self.put_full(
            name,
            &BucketRow {
                cors: wire.to_string(),
                ..row
            },
        )?;
        Ok(Some(true))
    }
}
