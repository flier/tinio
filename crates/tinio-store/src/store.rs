//! The shared database access handle: transaction entry, commit/abort,
//! and the closure-passing `read`/`write` pattern live here once — the
//! backends stop managing redb transactions directly. The handle is
//! **synchronous** (redb transactions are fast; in-memory and on-disk
//! alike — the fs backend adds its own blocking-pool hop around the same
//! core; see `tinio-fs`'s `Handle`).

use std::sync::Arc;

use redb::{Database, ReadTransaction, ReadableDatabase, WriteTransaction};

use crate::error::Error;

/// A database-backed store (marker: the fs `Handle` and mem
/// `MemoryStorage` wrap [`Handle`]).
pub trait Store {}

/// The shared access handle to a redb database.
///
/// Callers run one transaction as a closure — `read(|txn| …)` /
/// `write(|txn| …)` — and never touch `begin_*` / `commit` / `abort`
/// themselves. The closure's error type is generic (`E: From<Error>`),
/// so each backend accumulates its own error; the shared error's five
/// `#[from]` conversions ride the transaction entry points.
#[derive(Debug)]
pub struct Handle {
    pub(crate) db: Database,
}

impl Handle {
    /// Wrap a ready database.
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    /// Wrap a ready database behind an owned `Arc` (the shared-clone
    /// pattern of the fs `Handle`).
    pub fn new_shared(db: Database) -> Arc<Self> {
        Arc::new(Self::new(db))
    }

    /// The wrapped database (table creation, stats, test hooks).
    pub fn db(&self) -> &Database {
        &self.db
    }

    /// Run `f` against a read transaction. Zero-copy guards live only
    /// inside `f`; return owned copies (guards are invalid once the
    /// transaction ends).
    pub fn read<T, E>(&self, f: impl FnOnce(&ReadTransaction) -> Result<T, E>) -> Result<T, E>
    where
        E: From<Error>,
    {
        let txn = self.db.begin_read().map_err(Error::from)?;
        f(&txn)
    }

    /// Run `f` against a write transaction — committed on success,
    /// aborted on error. Multi-table operations run as one write closure.
    pub fn write<T, E>(&self, f: impl FnOnce(&mut WriteTransaction) -> Result<T, E>) -> Result<T, E>
    where
        E: From<Error>,
    {
        let mut txn = self.db.begin_write().map_err(Error::from)?;
        match f(&mut txn) {
            Ok(value) => {
                txn.commit().map_err(Error::from)?;
                Ok(value)
            }
            Err(err) => {
                let _ = txn.abort();
                Err(err)
            }
        }
    }
}

impl Store for Handle {}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use super::*;
    use crate::{bucket, testutil::handle};

    #[test]
    fn handle_write_commits_on_success_and_aborts_on_error() {
        let h = handle();
        // A failing write closure aborts the transaction — nothing is visible.
        let err = h.write(|txn| -> Result<(), Error> {
            bucket::Table::open(txn)?.put("rolled-back", SystemTime::UNIX_EPOCH)?;
            Err(Error::Storage(redb::StorageError::Corrupted("boom".into())))
        });
        assert!(err.is_err());
        assert!(
            !h.read(|txn| bucket::Table::open_readonly(txn)?.exists("rolled-back"))
                .unwrap()
        );
        // A succeeding write commits.
        h.write(|txn| -> Result<(), Error> {
            bucket::Table::open(txn)?.put("committed", SystemTime::UNIX_EPOCH)?;
            Ok(())
        })
        .unwrap();
        assert!(
            h.read(|txn| bucket::Table::open_readonly(txn)?.exists("committed"))
                .unwrap()
        );
    }
}
