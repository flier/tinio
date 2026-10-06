//! The shared redb error core: the five redb-mapping variants that
//! tinio-fs and tinio-mem nest/alias (spec 2026-09-03, grilling Q6), plus
//! the read-only refusal the shared handle's arms answer ([`Error::ReadOnly`],
//! not a redb failure at all — no transaction exists to fail).
//! fs-lifecycle variants (`Compaction`/`Io`/`UnsupportedVersion`/
//! `CorruptMeta`) stay in tinio-fs; tinio-mem has no compaction, version
//! gate, or db-layer file I/O.

/// A redb failure: open, transaction, table, storage, or commit — or a
/// write refused by a handle whose database is not open for writing.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Database open/create failed.
    #[error("database error: {0}")]
    Open(#[from] redb::DatabaseError),
    /// A transaction failed.
    #[error("transaction error: {0}")]
    Transaction(#[from] redb::TransactionError),
    /// Opening a table failed.
    #[error("table error: {0}")]
    Table(#[from] redb::TableError),
    /// A get/insert/range failed.
    #[error("storage error: {0}")]
    Storage(#[from] redb::StorageError),
    /// Commit failed.
    #[error("commit error: {0}")]
    Commit(#[from] redb::CommitError),
    /// A write was attempted through a handle whose arm does not take
    /// writes — the read-only and empty arms of [`crate::store::Handle`]
    /// (FR-023 amended).
    ///
    /// The read-only policy refuses every mutating operation upstream and
    /// a read-only run runs neither the scanner nor the sweeper, so this
    /// is a programming error in most flows — **except** on the read
    /// paths that rewrite a row they could not hit (a cold or stale
    /// object's ensured entry), where the refusal is the designed
    /// outcome and means "recomputed but not written back" (tinio-fs's
    /// `Error::is_read_only` asks for it through the nesting).
    #[error("the state database is read-only")]
    ReadOnly,
}

#[cfg(test)]
mod tests {
    use redb::{
        CommitError::TransactionPoisoned,
        DatabaseError::DatabaseAlreadyOpen,
        StorageError::{Corrupted, ValueTooLarge},
        TableError::TableDoesNotExist,
        TransactionError::Storage as TxnStorage,
    };

    use super::*;

    #[test]
    fn every_variant_wraps_its_redb_kind() {
        let cases: [(Error, &str); 5] = [
            (Error::from(DatabaseAlreadyOpen), "database error:"),
            (
                Error::from(TxnStorage(ValueTooLarge(1))),
                "transaction error:",
            ),
            (Error::from(TableDoesNotExist("x".into())), "table error:"),
            (Error::from(Corrupted("boom".into())), "storage error:"),
            (Error::from(TransactionPoisoned), "commit error:"),
        ];
        for (err, prefix) in cases {
            assert!(err.to_string().starts_with(prefix), "{err}");
        }
    }

    #[test]
    fn errors_are_send_sync_and_static() {
        fn assert_send_sync<T: Send + Sync + 'static>() {}
        assert_send_sync::<Error>();
    }
}
