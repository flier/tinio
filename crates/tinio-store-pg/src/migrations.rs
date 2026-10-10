//! The crate's numbered migrations, embedded in the binary and applied
//! forward-only.
//!
//! PG11: each migration is one numbered SQL file applied in its own
//! transaction, under an advisory lock held on a dedicated out-of-pool
//! connection. A database newer than the binary is refused with the pg remedy
//! -- the required version and a dump/restore path, never a rebuild, because
//! these tables hold the only copy of the bytes. The spike's RESET_SQL is not
//! carried over: `connect` migrates and never drops.
//!
//! `sqlx::migrate!` embeds `sql/migrations/` at compile time and parses each
//! version out of its `{version}_{description}.sql` name, so no Rust file lists
//! the migrations. The runner in `store` still applies them with this crate's
//! own bookkeeping -- the `schema_version` table -- rather than sqlx's
//! `_sqlx_migrations`: the schema, and the refusal a newer database gets, are
//! the store's contract, not the driver's.

use sqlx::migrate::{Migration, Migrator};

/// The newest schema version this binary migrates to.
///
/// A literal rather than a walk over [`MIGRATOR`], so it stays usable in a
/// `const`; `the_schema_version_is_the_newest_embedded_migration` pins it to
/// the embedded set.
pub const SCHEMA_VERSION: i64 = 2;

/// Every migration, embedded at compile time, in version order.
pub static MIGRATOR: Migrator = sqlx::migrate!("./sql/migrations");

/// The advisory lock key the migration runner holds on its dedicated
/// connection. Distinct from the ownership lock's key, so a migrating process
/// never blocks itself on the ownership guard.
pub const MIGRATION_LOCK_KEY: i64 = 0x7469_6E69_6F5F_6D67;

/// The advisory lock key the writable-ownership guard holds for the process's
/// lifetime (PG12).
pub const OWNERSHIP_LOCK_KEY: i64 = 0x7469_6E69_6F5F_6F77;

/// The table recording which migrations this database has applied.
pub const MIGRATION_TABLE: &str = "schema_version";

/// The DDL that records an applied migration. It is created outside the
/// migration list so version 1 can record itself.
pub(crate) const VERSION_TABLE_SQL: &str = include_str!("../sql/store/version_table.sql");

/// The migrations a database at `applied` still needs, in version order.
///
/// # Examples
///
/// ```
/// use tinio_store_pg::migrations::{SCHEMA_VERSION, pending};
///
/// // A database at the binary's version pends nothing; an empty one pends
/// // every embedded migration, in version order.
/// assert_eq!(pending(SCHEMA_VERSION).count(), 0);
/// assert!(pending(0).count() > 0);
/// ```
pub fn pending(applied: i64) -> impl Iterator<Item = &'static Migration> {
    MIGRATOR
        .iter()
        .filter(move |migration| migration.version > applied)
}

#[cfg(test)]
mod tests {
    use super::{MIGRATOR, SCHEMA_VERSION, pending};

    #[test]
    fn the_schema_version_is_the_newest_embedded_migration() {
        assert_eq!(
            SCHEMA_VERSION,
            MIGRATOR.iter().last().map(|m| m.version).unwrap_or(0)
        );
    }

    #[test]
    fn the_migrations_are_numbered_from_one_without_gaps() {
        for (index, migration) in MIGRATOR.iter().enumerate() {
            assert_eq!(migration.version, index as i64 + 1);
        }
    }

    #[test]
    fn only_a_newer_database_pends_nothing() {
        assert_eq!(pending(SCHEMA_VERSION).count(), 0);
        assert_eq!(pending(0).count(), MIGRATOR.iter().count());
        assert_eq!(pending(SCHEMA_VERSION + 1).count(), 0);
    }
}
