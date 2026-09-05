//! `"version"` → the format version, `"compact_needed"` → 0/1 — the fs
//! lifecycle table: the open-time version gate and the compact marker.
//! mem has no version gate and does not import this module.

use redb::{ReadableTable, TableDefinition};

use crate::{
    error::Error,
    table::{self, TableDef},
};

/// The format version — ONE current version, no migration (F06, user
/// decision): any stored version that is not this one is refused on open.
/// The version stays `1` — additive schema changes (the multipart-checksum
/// tables, and the tagging rows / `OBJECT_PARTS`) do NOT bump it (user
/// ruling 2026-09-02: dev-local databases are disposable, no
/// compatibility machinery); any stored version other than `1` is still
/// refused by the gate — a stale v2 database errors and the operator
/// deletes it, and a stale same-version database written in an older row
/// format may error at row decode — the same remedy.
pub const FORMAT_VERSION: u64 = 1;
/// The version key.
pub const VERSION_KEY: &str = "version";
/// The compact-needed marker key (0 = clean, 1 = needs compact).
pub const COMPACT_NEEDED_KEY: &str = "compact_needed";

/// The per-table marker: the table definition for the shared handle arms.
#[doc(hidden)]
pub enum Def {}

impl TableDef for Def {
    type Key = &'static str;
    type Value = u64;

    const DEF: TableDefinition<'static, Self::Key, Self::Value> = TableDefinition::new("state");
}

/// Handle to the state table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// The stored format version, if written.
    pub fn version(&self) -> Result<Option<u64>, Error> {
        Ok(self.0.get(VERSION_KEY)?.map(|guard| guard.value()))
    }

    /// The `compact_needed` marker (absent → `false`).
    pub fn compact_marker(&self) -> Result<bool, Error> {
        Ok(self
            .0
            .get(COMPACT_NEEDED_KEY)?
            .map(|guard| guard.value() != 0)
            .unwrap_or(false))
    }
}

impl<'txn> table::Table<'txn, Def> {
    /// Write the format version (first open).
    pub fn write_version(&mut self, version: u64) -> Result<(), Error> {
        self.0.insert(VERSION_KEY, version)?;
        Ok(())
    }

    /// Write the `compact_needed` marker (`false` = clean).
    pub fn set_compact_marker(&mut self, needed: bool) -> Result<(), Error> {
        self.0.insert(COMPACT_NEEDED_KEY, u64::from(needed))?;
        Ok(())
    }
}
