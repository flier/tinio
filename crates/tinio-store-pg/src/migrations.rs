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
//! the store's contract, not the driver's. That bookkeeping records each
//! migration's **checksum** beside its version (R5), which is what a later
//! binary compares a database against before it serves it.

use sqlx::migrate::{Migration, Migrator};

/// The newest schema version this binary migrates to.
///
/// A literal rather than a walk over [`MIGRATOR`], so it stays usable in a
/// `const`; `the_schema_version_is_the_newest_embedded_migration` pins it to
/// the embedded set.
pub const SCHEMA_VERSION: i64 = 1;

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
///
/// `checksum` is sha-384 of the migration's own text, hex-rendered, recorded by the runner as it
/// applies the step (R5). Its `DEFAULT ''` is for a row written by hand (a test's planted version)
/// and never matches an embedded digest, so a row that does not record one is refused rather than
/// trusted.
pub(crate) const VERSION_TABLE_SQL: &str = include_str!("../sql/store/version_table.sql");

/// The checksum this binary records for the migration at `version`, as hex, or
/// `None` when this binary carries no migration at that version (an empty
/// database, or one ahead of this binary, whose own refusal states its version).
///
/// The digest is `sqlx`'s own over the migration's text, taken from the
/// **embedded** file, so the comparison a store makes ([`crate::store`]) is
/// between the database's record and the bytes this binary was built from (R5).
pub fn applied_checksum(version: i64) -> Option<String> {
    MIGRATOR
        .iter()
        .find(|migration| migration.version == version)
        .map(|migration| hex::encode(&*migration.checksum))
}

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
    use super::{MIGRATOR, SCHEMA_VERSION, applied_checksum, pending};

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

    /// Every embedded migration has a recorded checksum to compare a database
    /// against, and a version this binary does not carry has none: the two
    /// answers a store's checksum gate can get (R5).
    #[test]
    fn a_checksum_is_offered_for_every_embedded_version_and_no_other() {
        assert_eq!(applied_checksum(0), None, "nothing applied has no digest");
        assert_eq!(
            applied_checksum(SCHEMA_VERSION + 1),
            None,
            "a version ahead of the binary is the newer-database refusal's"
        );
        let mut digests = Vec::new();
        for migration in MIGRATOR.iter() {
            let digest = applied_checksum(migration.version).expect("an embedded version has one");
            assert_eq!(
                digest.len(),
                96,
                "sha-384 renders as 96 hex characters: {digest}"
            );
            assert!(
                digest.chars().all(|c| c.is_ascii_hexdigit()),
                "the recorded digest is hex: {digest}"
            );
            assert!(
                !digests.contains(&digest),
                "two migrations carry one digest: {digest}"
            );
            digests.push(digest);
        }
        assert_eq!(digests.len(), MIGRATOR.iter().count());
    }

    /// A database at the binary's version, or ahead of it, pends nothing; the
    /// forward path from an empty one is the whole embedded set, contiguous and
    /// capped at [`SCHEMA_VERSION`] however many files the history holds.
    #[test]
    fn a_newer_database_pends_nothing_and_the_forward_path_is_contiguous() {
        assert_eq!(pending(SCHEMA_VERSION).count(), 0);
        assert_eq!(pending(SCHEMA_VERSION + 1).count(), 0);
        // The forward path from an empty database is exactly the embedded set,
        // one step per version: with a one-file history that is the single step
        // to 1, and the property holds unchanged however many files there are.
        let from_empty: Vec<i64> = pending(0).map(|migration| migration.version).collect();
        assert_eq!(from_empty, (1..=SCHEMA_VERSION).collect::<Vec<i64>>());
    }
}
