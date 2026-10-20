//! The state-database surface of the fs backend — a re-export shim over
//! the relocated redb driver, plus the fs policy shell around it.
//!
//! The redb machinery moved to `tinio-store-redb` (plan 2026-10-03 Task 4):
//! [`Driver`] (the async transaction handle with the blocking-pool hop and
//! the write-lock histograms), [`open`] (open-or-create, the format gate
//! and the schema ensure) and [`compact_if_needed`] (offline compaction).
//! This module re-exports them under the names every fs call site, the
//! metrics consumer and the doctor already use.
//!
//! What stays here is fs policy:
//!
//! * the state-dir **layout** decisions live in [`crate::path`] and
//!   [`crate::FsOptions`] (which directory is the state dir, the read-only
//!   relocation rule); [`open`] is the fs-facing entry point over the
//!   redb half;
//! * the CHK014 **permission hardening** — the state directory is `0700`
//!   and `meta.redb` is `0600` on unix, regardless of the process umask,
//!   after every open (and on Windows the files inherit the creating user's
//!   default ACLs: std has no ACL API, and the state dir is created under
//!   the storage root's `.tinio` segment);
//! * the **error mapping** back onto this crate's [`Error`]
//!   ([`crate::database::Error`]): the three lifecycle variants the moved
//!   code raises keep their fs-local shape, so the error text, the nesting
//!   and the `From` lift onto [`crate::Error`] are exactly what they were
//!   before the relocation.
//!
//! The strict object-meta walk moved to the shared implementation
//! (`tinio-store-redb`'s `meta::object::for_bucket_strict`, plan
//! 2026-10-03 Task 6 / review round 2 item 28); what stays here is the
//! error mapping and the fs-local `corrupt_meta` the walk reports through.

// redb error nesting: the shared [`crate::_store_redb::Error`] under
// `Redb`, plus the fs-local variants the pipeline reports. The
// version-mismatch constructor moved to the relocated open
// (tinio-store-redb), whose failure the fs re-export shim maps onto
// [`Error::UnsupportedVersion`] — the variant and its text stay here.

/// A redb or state-database failure.
///
/// Conversions derive via thiserror: `#[from]` on `Io`, `Compaction`,
/// and `Redb` emits `From<io::Error>`, `From<redb::CompactionError>`,
/// and From<[`crate::_store_redb::Error`]> — all three are hand-free, and
/// no `From` impls are hand-written. The five raw redb errors are not
/// forwarded (`From` is not transitive): they hop through the shared
/// error first, wrapped explicitly at the fs sites (`Error::Redb(e.into())`).
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Filesystem I/O around the state database.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// A compaction failed.
    #[error("compaction error: {0}")]
    Compaction(#[from] redb::CompactionError),
    /// The `STATE` table version does not match.
    #[error(
        "unsupported {} version {found} (expected {expected})",
        .path.display()
    )]
    UnsupportedVersion {
        /// The state file path.
        path: PathBuf,
        /// The version read from disk.
        found: u64,
        /// The supported version.
        expected: u64,
    },
    /// A stored `OBJECT_META` row failed domain validation (key or etag).
    #[error("corrupt object_meta entry for key `{key}`: {source}")]
    CorruptMeta {
        /// The raw key as stored.
        key: String,
        /// The domain validation failure (`InvalidKey` / etag parse).
        #[source]
        source: storage::Error,
    },
    /// A shared redb failure (the kinds of [`crate::_store_redb::Error`] — the
    /// five redb mappings and the read-only refusal the shared handle's
    /// arms answer).
    #[error(transparent)]
    Redb(#[from] _store_redb::Error),
}

impl Error {
    /// Whether this failure is "another process holds the store" — the redb lock's
    /// `DatabaseAlreadyOpen`, which is a fact about the calling run rather than about the
    /// database: `doctor` reports it as "not inspected", never as corruption.
    ///
    /// The predicate lives here because this crate owns the error and the `_store_redb` nesting: a
    /// consumer asking the question would otherwise have to reach `redb`'s variant through a
    /// message string or take a dependency on `tinio-store-redb` to match the shape.
    pub fn is_already_open(&self) -> bool {
        matches!(
            self,
            Self::Redb(_store_redb::Error::Open(
                redb::DatabaseError::DatabaseAlreadyOpen
            ))
        )
    }

    /// Whether this failure is "the handle is read-only" — the write was
    /// refused because the run opened the root's database read-only
    /// (FR-023 amended), not because anything is broken. The refusal is
    /// the shared handle's arm answering
    /// ([`crate::_store_redb::Error::ReadOnly`]), nested here like every other
    /// shared failure.
    ///
    /// The read paths that recompute a missing or stale entry ask this to
    /// serve the recomputed row without persisting it: in a read-only run
    /// that recomputation "is simply not written back" (design 2026-10-03,
    /// **Read paths**), and the read still answers.
    pub fn is_read_only(&self) -> bool {
        matches!(self, Self::Redb(_store_redb::Error::ReadOnly))
    }
}

/// A stored `OBJECT_META` row failed domain validation.
#[inline]
pub(crate) fn corrupt_meta(key: impl Into<String>, source: storage::Error) -> Error {
    record_corrupt_rows(Backend::Fs, 1);
    Error::CorruptMeta {
        key: key.into(),
        source,
    }
}

#[cfg(unix)]
use std::{fs, fs::Permissions, os::unix::fs::PermissionsExt};
use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use redb::{Database, ReadTransaction, WriteTransaction};
use tracing::instrument;

pub use crate::_store_redb::{
    WriteLockSnapshot,
    compact::{Compaction, Stats, needs_compact},
    open::{Integrity, Open, meta_db_path},
};
use crate::{
    _core::storage,
    _store_redb::{
        self,
        compact::compact_if_needed as compact_if_needed_impl,
        open::{check_integrity as check_integrity_impl, open as open_impl},
    },
    _util::metrics::{Backend, record_corrupt_rows},
};

/// The fs-facing state-database driver: the shared
/// [`crate::_store_redb::Driver`] with this crate's [`Error`] pinned as
/// the closure error type.
///
/// The shared driver's closure methods are generic over the error the
/// closure raises — its redb failures convert through `From<Error>`,
/// which is what lets one driver serve any store crate — and the fs stores
/// answer **this** crate's error, because `CorruptMeta` (a domain
/// validation failure of an fs-read row) has no home in the shared core.
/// This type fixes that parameter, so every fs store keeps the closure
/// signatures it had before the relocation (`Result<T, database::Error>`
/// inside `read`/`write`), and it fixes nothing else: the behaviour, the
/// blocking-pool hop, the timing and the histograms are the shared
/// driver's — this type owns no redb logic and forwards.
///
/// `WriteLockSnapshot`, the open/compaction surface and the error mapping
/// around them are the rest of this module.
#[derive(Debug)]
pub struct Driver(Arc<_store_redb::Driver>);

impl Driver {
    /// Wrap an **already-open** database — the construction the storage
    /// orchestration uses after the fs-side [`open`] (hardening) and
    /// offline compaction.
    pub fn new(db: Database) -> Arc<Self> {
        Arc::new(Self(_store_redb::Driver::new(db)))
    }

    /// The empty arm: an in-memory database holding the schema and nothing
    /// else — the degraded read-only view [`crate::FsStorage`] falls back
    /// to when the root's own database cannot be read.
    ///
    /// # Errors
    ///
    /// [`Error::Redb`] when the in-memory database cannot be created.
    pub fn empty() -> Result<Arc<Self>, Error> {
        Ok(Arc::new(Self(
            _store_redb::Driver::empty().map_err(map_driver_error)?,
        )))
    }

    /// The **read-only** arm over `<state_dir>/meta.redb`, creating
    /// nothing — the read-only run's metadata source (FR-023 amended).
    ///
    /// # Errors
    ///
    /// The arms are the caller's to report: a missing database, a corrupt
    /// one, or a format this build refuses — the read-only run then serves
    /// objects with an empty metadata view.
    pub fn open_read_only(state_dir: &Path) -> Result<Arc<Self>, Error> {
        Ok(Arc::new(Self(
            _store_redb::Driver::open_read_only(state_dir).map_err(map_driver_error)?,
        )))
    }

    /// The shared handle behind this driver — the construction path of the
    /// metadata contract implementation over the same state database
    /// ([`crate::_store_redb::RedbMetadata::from_driver`]): one database,
    /// many store facades.
    pub(crate) fn shared(&self) -> Arc<_store_redb::Driver> {
        Arc::clone(&self.0)
    }

    /// Run `f` against a read transaction — every arm reads.
    ///
    /// Zero-copy guards live only inside `f`; return owned copies.
    pub fn read<T>(
        &self,
        f: impl FnOnce(&ReadTransaction) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.0.read(f)
    }

    /// Run `f` against a read transaction on the tokio blocking pool — the
    /// read-side counterpart of [`Self::write``]'s hop: a full-bucket
    /// traversal must never execute inline on a runtime worker.
    pub async fn read_blocking<T>(
        self: &Arc<Self>,
        f: impl FnOnce(&ReadTransaction) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error>
    where
        T: Send + 'static,
    {
        self.0.read_blocking(f).await
    }

    /// Run `f` against a write transaction — committed on success, aborted
    /// on error, timed into the write-lock histograms, and executed on the
    /// tokio blocking pool (a commit is an fsync).
    #[instrument(target = "tinio::storage", level = "debug", skip_all)]
    pub async fn write<T>(
        self: &Arc<Self>,
        f: impl FnOnce(&mut WriteTransaction) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error>
    where
        T: Send + 'static,
    {
        self.0.write(f).await
    }

    /// The no-op-aware sibling of [`Self::write``]: `None` drops the transaction
    /// without the commit fsync (the idempotent tag clears and row-missing
    /// probes cost nothing).
    pub(crate) async fn write_if<T>(
        self: &Arc<Self>,
        f: impl FnOnce(&mut WriteTransaction) -> Result<Option<T>, Error> + Send + 'static,
    ) -> Result<Option<T>, Error>
    where
        T: Send + 'static,
    {
        self.0.write_if(f).await
    }

    /// The write-transaction timing snapshot behind the server's
    /// `tinio_write_lock_*` metrics (pipeline-spec.md §4) — a cheap atomic
    /// snapshot, never a lock.
    pub fn write_lock_stats(&self) -> WriteLockSnapshot {
        self.0.write_lock_stats()
    }

    /// Evaluate fragmentation and update the `compact_needed` marker (one
    /// write transaction; the sweep calls this once per round).
    pub async fn evaluate_compact(self: &Arc<Self>, threshold_percent: u8) -> Result<bool, Error> {
        self.0
            .evaluate_compact(threshold_percent)
            .await
            .map_err(map_driver_error)
    }

    /// Write the `compact_needed` marker (one write transaction).
    pub async fn mark_compact_needed(self: &Arc<Self>, needed: bool) -> Result<(), Error> {
        self.0
            .mark_compact_needed(needed)
            .await
            .map_err(map_driver_error)
    }
}

#[cfg(test)]
pub(crate) use crate::_store_redb::state::Table as StateTable;

/// The state-directory mode on unix (CHK014): private to the owning user.
#[cfg(unix)]
const STATE_DIR_MODE: u32 = 0o700;

/// The state-database file mode on unix (CHK014): private to the owning
/// user.
#[cfg(unix)]
const META_DB_MODE: u32 = 0o600;

/// Open (or create) the state database at `<state_dir>/meta.redb` and
/// harden the private state on unix (CHK014).
///
/// The directory is created and hardened **before** the database exists,
/// and the file is hardened immediately after creation, so the modes hold
/// however the process umask is set. The tables are created and the
/// `STATE` version is gated inside the relocated
/// [`crate::_store_redb::open::open`] (one write transaction: a missing version
/// is written, a mismatch is [`Error::UnsupportedVersion`]).
///
/// # Errors
///
/// [`Error::Io`] when the state directory or its modes cannot be set;
/// `Error::Redb(Open(..))` when the file exists but is not a valid redb
/// database (corrupted state — the metadata is derivable and the file can
/// be deleted for a rebuild); [`Error::UnsupportedVersion`] on a version
/// mismatch.
#[instrument(target = "tinio::storage", level = "info", skip_all, name = "fs.open")]
pub fn open(state_dir: &Path) -> Result<Open, Error> {
    #[cfg(unix)]
    {
        fs::create_dir_all(state_dir)?;
        fs::set_permissions(state_dir, Permissions::from_mode(STATE_DIR_MODE))?;
    }
    let opened = open_impl(state_dir).map_err(map_driver_error)?;
    #[cfg(unix)]
    fs::set_permissions(
        meta_db_path(state_dir),
        Permissions::from_mode(META_DB_MODE),
    )?;
    Ok(opened)
}

/// Run a full integrity check on the state database at `state_dir` — the
/// fs-side mechanism behind doctor's integrity check (meta-redb-spec
/// §5.8/§5.9). Opens the file exclusively: no server may be running.
///
/// # Errors
///
/// `Error::Redb(Open(..))` when the file does not exist or cannot be
/// opened (doctor reports it; `check_integrity` itself is redb's automatic
/// repair path).
pub fn check_integrity(state_dir: &Path) -> Result<Integrity, Error> {
    check_integrity_impl(state_dir).map_err(map_driver_error)
}

/// Evaluate and run offline compaction on a **not-yet-shared** `Database`
/// — the only moment `&mut` is available (meta-redb-spec §5.9: `Database`
/// is not `Clone`, and once wrapped in `Arc<Driver>` a mutable reference
/// is structurally impossible). The runtime never compacts: the server
/// startup orchestration and `doctor --fix` call this offline, before
/// [`crate::FsStorage`] construction. `compact_needed` and `stats`
/// come from [`open`].
pub fn compact_if_needed(
    db: &mut Database,
    compact_needed: bool,
    stats: Stats,
    threshold_percent: u8,
) -> Result<Compaction, Error> {
    compact_if_needed_impl(db, compact_needed, stats, threshold_percent).map_err(map_driver_error)
}

/// Map the relocated driver's failure onto this crate's database error.
///
/// The three lifecycle variants the moved code raises — the state-directory
/// I/O, the compaction failure and the open-time version gate — keep their
/// fs-local variants (the same text and the same nesting the fs-facing
/// surface had before the relocation); every redb-mapping variant rides the
/// derived [`Error::Redb`] nesting.
fn map_driver_error(err: _store_redb::Error) -> Error {
    match err {
        _store_redb::Error::Io(err) => Error::Io(err),
        _store_redb::Error::Compaction(err) => Error::Compaction(err),
        _store_redb::Error::UnsupportedVersion {
            path,
            found,
            expected,
        } => Error::UnsupportedVersion {
            path,
            found,
            expected,
        },
        other => Error::Redb(other),
    }
}

/// Map a **contract failure** from the shared metadata implementation onto
/// this crate's [`crate::Error`].
///
/// A [`_store_redb::Error::Refused`] payload is the contract error the
/// operation answered (design 2026-10-03 D22 — a refusal is visible as a
/// projection): it keeps its domain arm ([`crate::Error::Storage`]) so the
/// S3 mapping still sees `NoSuchBucket`/`NotEmpty`/`AlreadyExists`, instead
/// of degrading to the I/O arm the database nesting lifts to. Every redb
/// failure rides the same mapping as the driver's own
/// ([`map_driver_error`]).
pub(crate) fn map_contract_error(err: _store_redb::Error) -> crate::Error {
    match err {
        _store_redb::Error::Refused(err) => err.into(),
        _store_redb::Error::CorruptRow { key, source } => corrupt_meta(key, source).into(),
        other => map_driver_error(other).into(),
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use super::*;
    use crate::_util::testing::assert_send_sync;

    /// The shared error nests under [`Error::Redb`] per kind — the lift the
    /// fs-facing surface has always had.
    #[test]
    fn redb_errors_wrap_per_kind() {
        use redb::{
            StorageError::ValueTooLarge, TableError::TableDoesNotExist,
            TransactionError::Storage as TxnStorage,
        };

        let err = Error::Redb(TableDoesNotExist("t".into()).into());
        assert!(
            matches!(err, Error::Redb(_store_redb::Error::Table(_))),
            "{err}"
        );
        let err = Error::Redb(TxnStorage(ValueTooLarge(1)).into());
        assert!(
            matches!(err, Error::Redb(_store_redb::Error::Transaction(_))),
            "{err}"
        );
        assert!(
            Error::Redb(ValueTooLarge(1).into())
                .to_string()
                .contains("storage error")
        );
    }

    #[test]
    fn errors_are_send_sync_and_static() {
        assert_send_sync::<Error>();
    }

    /// The version gate's failure keeps the fs-local variant through the
    /// relocated open (`Error::Database(UnsupportedVersion { .. })`, the
    /// same nesting and text as before the move).
    #[test]
    fn open_maps_the_version_gate_onto_the_fs_variant() {
        let root = tempfile::tempdir().unwrap();
        let state = root.path().join("state");
        let opened = open(&state).unwrap();
        {
            let mut txn = opened.db.begin_write().unwrap();
            StateTable::open(&mut txn)
                .unwrap()
                .insert("version", 9)
                .unwrap();
            txn.commit().unwrap();
        }
        drop(opened);

        let err: crate::Error = open(&state).unwrap_err().into();
        assert!(
            matches!(
                err,
                crate::Error::Database(Error::UnsupportedVersion {
                    found: 9,
                    expected: 1,
                    ..
                })
            ),
            "{err}"
        );
    }

    /// A database I/O failure unwraps to the public [`crate::Error::Io`] arm
    /// — the lift that keeps a bad state dir an I/O error at the crate
    /// boundary.
    #[test]
    fn open_maps_io_onto_the_crate_io() {
        let err: crate::Error = Error::Io(io::Error::other("boom")).into();
        assert!(matches!(err, crate::Error::Io(_)));
    }

    #[cfg(unix)]
    #[test]
    fn state_dir_and_meta_db_are_hardened() {
        let root = tempfile::tempdir().unwrap();
        let state = root.path().join("state");
        let opened = open(&state).unwrap();
        drop(opened);

        let dir_mode = fs::metadata(&state).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            dir_mode, STATE_DIR_MODE,
            "state dir must be {STATE_DIR_MODE:o}, got {dir_mode:o}"
        );

        let db_mode = fs::metadata(meta_db_path(&state))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            db_mode, META_DB_MODE,
            "meta.redb must be {META_DB_MODE:o}, got {db_mode:o}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn open_hardens_pre_existing_state() {
        // Re-opening an existing state dir also enforces the modes
        // (idempotent; CHK014 must hold after every startup).
        let root = tempfile::tempdir().unwrap();
        let state = root.path().join("state");
        drop(open(&state).unwrap());
        fs::set_permissions(&state, Permissions::from_mode(0o755)).unwrap();
        fs::set_permissions(meta_db_path(&state), Permissions::from_mode(0o644)).unwrap();

        drop(open(&state).unwrap());

        let dir_mode = fs::metadata(&state).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            dir_mode, STATE_DIR_MODE,
            "state dir must be re-hardened to {STATE_DIR_MODE:o}, got {dir_mode:o}"
        );
        let db_mode = fs::metadata(meta_db_path(&state))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            db_mode, META_DB_MODE,
            "meta.redb must be re-hardened to {META_DB_MODE:o}, got {db_mode:o}"
        );
    }
}
