//! Shared redb table layer for the tinio storage backends.
//!
//! tinio-fs (on-disk `meta.redb`) and tinio-mem (`InMemoryBackend`) both
//! persist the same derived-metadata rows in redb tables; the schema,
//! per-table handles, and scan/drain helpers live here so a schema
//! change lands once (row decoding delegates to tinio-core's wire
//! codecs — the parse-display-derived names). See
//! `docs/superpowers/specs/2026-09-03-shared-store-table-layer-design.md`.

pub mod bucket;
pub mod error;
pub mod meta;
pub mod object_part;
pub mod objects;
pub mod part;
pub mod part_checksum;
pub mod part_data;
pub mod part_meta;
pub mod scan;
pub mod state;
pub mod store;
pub mod table;
pub mod upload;
pub mod upload_checksum;

#[cfg(test)]
mod contract;

pub use self::error::Error;

/// Create the seven shared tables inside a write transaction
/// (idempotent). Backends create their local tables in the same
/// transaction alongside.
pub fn ensure_all(txn: &mut redb::WriteTransaction) -> Result<(), Error> {
    bucket::Table::ensure(txn)?;
    meta::Table::ensure(txn)?;
    upload::Table::ensure(txn)?;
    part::Table::ensure(txn)?;
    upload_checksum::Table::ensure(txn)?;
    part_checksum::Table::ensure(txn)?;
    object_part::Table::ensure(txn)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use redb::{Database, ReadableDatabase, TableHandle};

    use super::*;

    #[test]
    fn ensure_all_creates_exactly_the_seven_tables_idempotently() {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .unwrap();
        {
            let mut txn = db.begin_write().unwrap();
            ensure_all(&mut txn).unwrap();
            txn.commit().unwrap();
        }
        // Idempotent: a second open-time ensure on the same db is a no-op.
        {
            let mut txn = db.begin_write().unwrap();
            ensure_all(&mut txn).unwrap();
            txn.commit().unwrap();
        }
        let txn = db.begin_read().unwrap();
        let mut names: Vec<String> = txn
            .list_tables()
            .unwrap()
            .map(|h| h.name().to_string())
            .collect();
        names.sort_unstable();
        assert_eq!(
            names,
            [
                "buckets",
                "object_meta",
                "object_parts",
                "part_checksums",
                "parts",
                "upload_checksums",
                "uploads",
            ]
        );
    }
}
