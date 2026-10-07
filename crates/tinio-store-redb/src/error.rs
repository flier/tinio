//! The shared redb error core: the five redb-mapping variants that
//! tinio-fs and tinio-mem nest/alias (spec 2026-09-03, grilling Q6), plus
//! the read-only refusal the shared handle's arms answer ([`Error::ReadOnly`],
//! not a redb failure at all — no transaction exists to fail), and the
//! contract-domain refusal ([`Error::Refused`]) the metadata
//! implementations raise when a decision inside their transaction is the
//! contract error itself (design 2026-10-03, D22).
//!
//! The relocation of tinio-fs's database driver (plan 2026-10-03 Task 4)
//! brought three lifecycle variants here with the code that raises them:
//! [`Error::Io`] (the state directory the open creates), [`Error::Compaction`]
//! (offline compaction) and [`Error::UnsupportedVersion`] (the open-time
//! format gate, which reads `FORMAT_VERSION` from [`crate::state`] and
//! never re-declares it). tinio-fs keeps its own same-named variants for
//! the fs-facing error surface and maps these onto them at its re-export
//! shim; tinio-mem has no compaction, version gate, or db-layer file I/O,
//! so it never constructs them. `CorruptMeta` stays fs-local (it carries
//! a domain validation failure of an fs-read row).

use std::{io, path::PathBuf};

use crate::_core::storage;

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
    /// Filesystem I/O around the state database — the state directory
    /// the open creates ([`crate::open::open`]).
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// A compaction failed ([`crate::compact::compact_if_needed`]).
    #[error("compaction error: {0}")]
    Compaction(#[from] redb::CompactionError),
    /// The `STATE` table version does not match (the open-time gate,
    /// [`crate::open::open`]): the version read from disk is refused, never
    /// migrated — one current version, no compatibility machinery (F06).
    #[error("unsupported {} version {found} (expected {expected})", .path.display())]
    UnsupportedVersion {
        /// The state file path.
        path: PathBuf,
        /// The version read from disk.
        found: u64,
        /// The supported version.
        expected: u64,
    },
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
    /// A **contract-domain refusal** the shared implementation decided
    /// inside its own transaction (design 2026-10-03, D22): the
    /// already-exists of `BucketMeta::create`, the missing bucket of
    /// `ObjectMeta::put_guarded`, the missing/non-empty outcomes of
    /// `BucketMeta::remove_if_empty`. The payload is the contract error
    /// the operation answers, so the projection onto
    /// [`crate::_core::storage::Error`] is verbatim.
    ///
    /// **Every consumer facade owes this variant an unwrap arm.** A backend
    /// that converts `RedbMetadata`'s failures wholesale through its own
    /// database arm (tinio-mem's `DatabaseError`, tinio-fs's lifted
    /// `Database`) turns an `AlreadyExists` / `NoSuchBucket` / `NotEmpty`
    /// refusal into an I/O failure, and no test or lint catches it until an
    /// S3 status code is wrong. The arm is `Refused(err) => err.into()`
    /// **before** the database arm — tinio-fs's `database::map_contract_error`
    /// is the pattern to copy (the payload is already a
    /// [`crate::_core::storage::Error`], so the unwrap is total).
    #[error(transparent)]
    Refused(#[from] storage::Error),
    /// A stored row the domain rejects — the corrupt row the strict
    /// traversal refuses instead of serving or healing: the contract's
    /// `ObjectMeta::walk` outcome, distinct from a redb failure (review
    /// round 2, item 28 follow-up).
    ///
    /// **A consumer facade owes this variant a domain arm beside
    /// [`Error::Refused`]**: tinio-fs maps it onto its own fs-local
    /// `CorruptMeta` (`database::map_contract_error`, the pattern to copy),
    /// so the doctor's walk reports the fs error rather than an I/O failure.
    #[error("corrupt row {key:?}: {source}")]
    CorruptRow {
        /// The key of the rejected row.
        key: String,
        /// The validation failure behind the rejection.
        source: storage::Error,
    },
}

impl Error {
    /// Whether this failure is the write refusal of a non-writable arm
    /// ([`Error::ReadOnly`]) rather than a redb failure — the question the
    /// read paths that recompute a row ask before deciding the
    /// recomputation "is simply not written back" (tinio-fs's
    /// `Error::is_read_only` asks it through the nesting).
    pub fn is_read_only(&self) -> bool {
        matches!(self, Self::ReadOnly)
    }
}

/// The projection onto the contract error (design 2026-10-03 property 7):
/// a [`Error::Refused`] payload is the contract failure itself and rides
/// through verbatim — a refusal is visible as a projection (D22) — while
/// every redb failure lifts to the I/O-class contract failure, the same
/// lift tinio-fs and tinio-mem give their own database errors.
impl From<Error> for storage::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Refused(err) => err,
            other => storage::Error::Io(io::Error::other(other)),
        }
    }
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
    use crate::_core::bucket;

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

    /// A contract refusal projects verbatim onto the contract error (D22);
    /// every redb failure lifts to the I/O-class arm.
    #[test]
    fn contract_refusals_project_and_redb_failures_lift() {
        let name = bucket::name("data").unwrap();
        let refused = Error::Refused(storage::no_such_bucket(&name));
        assert!(matches!(
            storage::Error::from(refused),
            storage::Error::NoSuchBucket(_)
        ));
        assert!(matches!(
            storage::Error::from(Error::ReadOnly),
            storage::Error::Io(_)
        ));
    }
}
