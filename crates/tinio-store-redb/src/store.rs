//! The shared database access handle: the **arm** a run reaches its state
//! database through (writable, read-only, or the degraded empty view) and
//! the transaction entry — commit/abort and the closure-passing
//! `read`/`write` pattern — live here once, so the backends stop managing
//! redb transactions and write refusals themselves. The handle is
//! **synchronous** (redb transactions are fast; in-memory and on-disk
//! alike — the fs backend adds its own blocking-pool hop and write-lock
//! histograms around the same core; see `tinio-fs`'s `Handle`).

use std::sync::Arc;

use derive_more::Debug;
use redb::{
    Database, ReadOnlyDatabase, ReadTransaction, ReadableDatabase, WriteTransaction,
    backends::InMemoryBackend,
};

use crate::error::Error;

/// A database-backed store (marker: mem's `MemoryStorage` wraps
/// [`Handle`]; the fs `Handle` wraps one too — it keeps only what is
/// genuinely fs-specific, the blocking-pool hop and its write-lock
/// histograms, and no arm of its own — FR-023 amended).
pub trait Store {}

/// An empty **in-memory** database: no file, and no schema of its own —
/// the missing-table stand-in's backing database ([`crate::table`]), whose
/// tables are created on demand and never written, and the base of the
/// degraded read-only view, which [`Handle::empty`] builds up with the
/// shared schema before serving it.
pub fn open_in_memory() -> Result<Database, Error> {
    Ok(Database::builder().create_with_backend(InMemoryBackend::new())?)
}

/// The database a [`Handle`] wraps — one arm per way a run reaches its
/// metadata.
///
/// * `Writable` — the read-write arm, opened for writing (the only arm
///   that accepts a write).
/// * `ReadOnly` — the read-only arm (FR-023 amended): the same database
///   opened read-only, so a read-only run *serves* the metadata a
///   read-write run wrote (objects, tags, ACLs, CORS, the version index)
///   instead of losing it.
/// * `Empty` — the degrade arm: a read-only run whose database cannot be
///   opened (absent, corrupt, a format this build refuses, or a
///   read-write server holding the file — redb refuses a read-only open
///   concurrent with a `Database`). An empty in-memory database: every
///   table reads as empty, objects keep serving from their files, and
///   nothing is written anywhere.
///
/// The two non-writable arms refuse writes ([`Error::ReadOnly`]): the
/// read-only policy refuses every mutating operation upstream and a
/// read-only run runs neither the scanner nor the sweeper, so a write
/// here is a programming error — except on the read paths that rewrite a
/// row they could not hit, where the refusal is the designed "not
/// written back" (tinio-fs's `Error::is_read_only`).
///
/// `Debug` names each arm and not the handle it wraps, because
/// `redb::ReadOnlyDatabase` has no `Debug`: a variant-level `#[debug(...)]`
/// carries the bare name (`Writable`, `ReadOnly`, `Empty`).
#[derive(Debug)]
enum Db {
    /// The database, opened for writing.
    #[debug("Writable")]
    Writable(Database),
    /// The database, opened read-only.
    #[debug("ReadOnly")]
    ReadOnly(ReadOnlyDatabase),
    /// The empty in-memory view a read-only run degrades to.
    #[debug("Empty")]
    Empty(Database),
}

/// The shared access handle to a state database.
///
/// Callers run one transaction as a closure — `read(|txn| …)` /
/// `write(|txn| …)` — and never touch `begin_*` / `commit` / `abort`
/// themselves. The closure's error type is generic (`E: From<Error>`),
/// so each backend accumulates its own error; the shared error's five
/// `#[from]` conversions ride the transaction entry points. Every arm
/// reads; only the writable one writes, and the refusal is the arm's own
/// answer, not a check each caller repeats.
#[derive(Debug)]
pub struct Handle {
    db: Db,
}

impl Handle {
    /// Wrap a ready database — the writable arm.
    pub fn new(db: Database) -> Self {
        Self {
            db: Db::Writable(db),
        }
    }

    /// Wrap a database opened **read-only** — the arm a read-only run
    /// serves the metadata a read-write run wrote from (FR-023 amended).
    pub fn new_read_only(db: ReadOnlyDatabase) -> Self {
        Self {
            db: Db::ReadOnly(db),
        }
    }

    /// Wrap an empty in-memory database — the degraded read-only view:
    /// every table reads as empty, every write is refused
    /// ([`Error::ReadOnly`]).
    pub fn empty() -> Result<Self, Error> {
        let db = open_in_memory()?;
        // The schema is part of "empty", even in memory: a schema-less
        // database would answer a base-table read with redb's
        // `TableDoesNotExist`, and the missing-table tolerance `Table`
        // grants belongs to the version tables alone (spec 2026-10-03) —
        // the degrade arm's promise is that *every* table reads as empty.
        // The `state` table joins `ensure_all`'s ten here: it is the
        // store's own module but stays out of `ensure_all` (mem has no
        // lifecycle table), while the fs read paths the degrade arm serves
        // ask it too.
        {
            let mut txn = db.begin_write()?;
            crate::ensure_all(&mut txn)?;
            crate::state::Table::ensure(&mut txn)?;
            txn.commit()?;
        }
        Ok(Self { db: Db::Empty(db) })
    }

    /// Wrap a ready database behind an owned `Arc` (the shared-clone
    /// pattern of the backends' handles).
    pub fn new_shared(db: Database) -> Arc<Self> {
        Arc::new(Self::new(db))
    }

    /// The wrapped database, when the arm holds one — the `Writable` and
    /// `Empty` arms (the read-only arm wraps a [`ReadOnlyDatabase`],
    /// which is a different type). Table creation, stats, test hooks.
    ///
    /// Exists for the backends' unit tests — today the three `tinio-mem`
    /// sites that read or tamper with a table straight in the database
    /// (`storage.db.db().unwrap().begin_read()/begin_write()`, in
    /// `bucket.rs`'s garbage-ACL heal test, `object.rs`'s
    /// `mem_object_parts_lifecycle` and `storage.rs`'s orphan scan); no
    /// product path calls it.
    /// The `Empty` arm answers `Some(…)` too, but that database is the
    /// in-memory degraded view — empty by construction, not the real
    /// state file — so a test that reaches it through this handle sees
    /// no rows, not the rows it wrote.
    pub fn db(&self) -> Option<&Database> {
        match &self.db {
            Db::Writable(db) | Db::Empty(db) => Some(db),
            Db::ReadOnly(_) => None,
        }
    }

    /// Whether the wrapped arm accepts writes — the `Writable` arm alone.
    /// The read-only and empty arms answer [`Error::ReadOnly`] on every
    /// write path.
    pub fn is_writable(&self) -> bool {
        matches!(self.db, Db::Writable(_))
    }

    /// The read transaction this handle's arm opens — one begin-and-map
    /// for every arm: both `Database` (the writable and empty arms) and
    /// `ReadOnlyDatabase` implement [`redb::ReadableDatabase`], so the
    /// three arms answer the same call and the error mapping has one home.
    fn begin_read(&self) -> Result<ReadTransaction, Error> {
        fn begin(db: &impl ReadableDatabase) -> Result<ReadTransaction, Error> {
            db.begin_read().map_err(Error::from)
        }
        match &self.db {
            Db::Writable(db) | Db::Empty(db) => begin(db),
            Db::ReadOnly(db) => begin(db),
        }
    }

    /// Run `f` against a read transaction. Zero-copy guards live only
    /// inside `f`; return owned copies (guards are invalid once the
    /// transaction ends).
    pub fn read<T, E>(&self, f: impl FnOnce(&ReadTransaction) -> Result<T, E>) -> Result<T, E>
    where
        E: From<Error>,
    {
        let txn = self.begin_read()?;
        f(&txn)
    }

    /// Begin one write transaction — or refuse with [`Error::ReadOnly`]
    /// when the wrapped arm does not take writes. [`Self::write`] uses
    /// it; a caller that times the transaction (tinio-fs's handle records
    /// its write-lock histograms around `begin_write`'s return) takes it
    /// directly and owns the commit/abort.
    pub fn begin_write(&self) -> Result<WriteTransaction, Error> {
        let Db::Writable(db) = &self.db else {
            return Err(Error::ReadOnly);
        };
        Ok(db.begin_write()?)
    }

    /// Run `f` against a write transaction — committed on success,
    /// aborted on error. Multi-table operations run as one write closure.
    pub fn write<T, E>(&self, f: impl FnOnce(&mut WriteTransaction) -> Result<T, E>) -> Result<T, E>
    where
        E: From<Error>,
    {
        let mut txn = self.begin_write().map_err(E::from)?;
        match f(&mut txn) {
            Ok(value) => {
                txn.commit().map_err(Error::from).map_err(E::from)?;
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
