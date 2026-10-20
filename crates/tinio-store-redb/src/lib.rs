//! The shared redb implementation for the tinio storage backends.
//!
//! tinio-fs (on-disk `meta.redb`) and tinio-mem (`InMemoryBackend`) both
//! persist the same derived-metadata rows in redb tables; the schema,
//! per-table handles, and scan/drain helpers live here so a schema
//! change lands once (row decoding delegates to tinio-core's wire
//! codecs — the parse-display-derived names). See
//! `docs/superpowers/specs/2026-09-03-shared-store-table-layer-design.md`.
//!
//! The crate also owns the redb **driver** relocated from tinio-fs
//! (plan 2026-10-03 Task 4): [`Driver`] (the async transaction handle
//! with the blocking-pool hop and the write-lock histograms,
//! [`WriteLockSnapshot`]), [`open::open`] (the open plus the format gate —
//! [`state::FORMAT_VERSION`] stays declared in [`state`]), and
//! [`compact`] (offline compaction evaluation and execution).

use std::{path::Path, sync::Arc};

#[doc(hidden)]
pub extern crate tinio_core as _core;

use crate::_core::meta::Metadata;

mod acl;
pub mod bucket;
pub mod bucket_lifecycle;
pub mod bucket_versioning;
pub mod compact;
pub mod content;
pub mod driver;
pub mod error;
pub mod meta;
pub mod object_meta;
pub mod object_part;
// The three content byte tables: tinio-mem still reaches their handles by
// path, so they stay `pub` — hidden from the docs until P2 privatizes them
// (plan 2026-10-03 Task 8 Step 3).
#[doc(hidden)]
pub mod objects;
pub mod open;
pub mod part;
pub mod part_checksum;
#[doc(hidden)]
pub mod part_data;
#[doc(hidden)]
pub mod part_meta;
pub mod part_stamp;
pub mod scan;
pub mod state;
pub mod store;
pub mod table;
pub mod upload;
pub mod upload_checksum;
pub mod version;
pub mod version_index;

#[cfg(test)]
mod contract;

#[cfg(test)]
mod testutil;

pub use self::{
    acl::{decode_acl_wire, decode_owner_wire},
    compact::{Compaction, Stats, compact_if_needed, needs_compact},
    content::RedbContent,
    driver::Driver,
    error::Error,
    open::{Integrity, Open, check_integrity, meta_db_path, open, open_read_only},
};

/// The write-lock histogram snapshot lives in `tinio-core`, beside the bucket bounds it is
/// positional with; re-exported here so a store consumer keeps naming it through its backend.
pub use crate::_core::WriteLockSnapshot;

/// The shared metadata implementation: the four metadata contracts
/// over one [`Driver`] — `BucketMeta`, `ObjectMeta` and `UploadMeta`
/// (the aggregate's supertraits) plus `VersionMeta`. The bytes side is
/// [`RedbContent`], a type of its own: the content plane's two tables are
/// the backend's own, and the engine composes it with wrappers of its own.
///
/// tinio-fs's `bucket`/`meta`/`multipart` stores and tinio-mem's storage
/// hold one of these behind an `Arc` and delegate their contract
/// operations to it, mapping [`Error`] onto their own facade error at the
/// boundary (design 2026-10-03 §2). The category implementations live in
/// [`meta`]; the content contract's is [`content`].
///
/// # Examples
///
/// ```rust
/// use tinio_store_redb::RedbMetadata;
///
/// let metadata = RedbMetadata::in_memory().unwrap();
/// // The handle the backend's other stores share.
/// let driver = metadata.driver();
/// assert!(std::sync::Arc::strong_count(&driver) >= 2);
/// ```
#[derive(Debug, Clone)]
pub struct RedbMetadata {
    /// The driver every category runs its transactions through.
    driver: Arc<Driver>,
}

impl RedbMetadata {
    /// Open (or create) the state database at `state_dir` — the backend
    /// construction path (the relocated open ensures the shared schema and
    /// gates the format version).
    pub fn open(state_dir: &Path) -> Result<Self, Error> {
        Ok(Self::from_driver(Driver::open(state_dir)?))
    }

    /// A fresh in-memory database with the shared schema ensured — the
    /// tinio-mem and conformance construction path.
    pub fn in_memory() -> Result<Self, Error> {
        let db = store::open_in_memory()?;
        {
            let mut txn = db.begin_write()?;
            ensure_all(&mut txn)?;
            txn.commit()?;
        }
        Ok(Self::from_driver(Driver::new(db)))
    }

    /// Wrap a driver the caller already owns: tinio-fs opens one state
    /// database and hands the same handle to every store.
    pub fn from_driver(driver: Arc<Driver>) -> Self {
        Self { driver }
    }

    /// The driver this implementation runs its transactions through.
    pub fn driver(&self) -> Arc<Driver> {
        Arc::clone(&self.driver)
    }

    /// The shared store handle behind the driver — the handle a backend
    /// whose own tables live in this database (tinio-mem's version blobs and
    /// part-stat rows) keeps beside this implementation, so both serve one
    /// database ([`Driver::store_handle`]).
    pub fn store_handle(&self) -> Arc<store::Handle> {
        self.driver.store_handle()
    }
}

impl Metadata for RedbMetadata {
    type Error = Error;
}

/// Create the twelve shared tables inside a write transaction
/// (idempotent). Backends create their local tables in the same
/// transaction alongside.
pub fn ensure_all(txn: &mut redb::WriteTransaction) -> Result<(), Error> {
    bucket::Table::ensure(txn)?;
    object_meta::Table::ensure(txn)?;
    upload::Table::ensure(txn)?;
    part::Table::ensure(txn)?;
    upload_checksum::Table::ensure(txn)?;
    part_checksum::Table::ensure(txn)?;
    part_stamp::Table::ensure(txn)?;
    object_part::Table::ensure(txn)?;
    version::Table::ensure(txn)?;
    version_index::Table::ensure(txn)?;
    bucket_versioning::Table::ensure(txn)?;
    bucket_lifecycle::Table::ensure(txn)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use redb::{Database, ReadableDatabase, TableHandle, backends::InMemoryBackend};

    use super::*;
    use crate::table::TableDef;

    /// The compile-time equality pin: the sole `Same` impl is the blanket
    /// `impl<T> Same<T> for T`, so a pair of distinct types never
    /// satisfies the bound — `assert_same` fails to resolve (E0277)
    /// unless both type arguments are the same type.
    trait Same<T> {}
    impl<T> Same<T> for T {}

    fn assert_same<A: Same<B>, B>() {}

    #[test]
    fn ensure_all_creates_exactly_the_twelve_tables_idempotently() {
        let db = Database::builder()
            .create_with_backend(InMemoryBackend::new())
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
                "bucket_lifecycle",
                "bucket_versioning",
                "buckets",
                "object_meta",
                "object_parts",
                "part_checksums",
                "part_stamps",
                "parts",
                "upload_checksums",
                "uploads",
                "version_index",
                "versions",
            ]
        );
    }

    #[test]
    fn buckets_value_slot_is_the_five_tuple() {
        // The BUCKETS row pins the ACL 4-tuple with the CORS element
        // appended fifth: `(created_at_nanos, tags_wire, owner_wire,
        // acl_wire, cors_wire)`. redb 4.x exposes no runtime arity
        // accessor (`TableDefinition`/`Value` have no `value_arity`), so
        // the pin is this type-level equality — a wrong-arity or
        // reordered `Def::Value` fails the build (E0277).
        assert_same::<
            <bucket::Def as TableDef>::Value,
            (u64, &'static str, &'static str, &'static str, &'static str),
        >();
    }
}
