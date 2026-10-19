//! The redb half of the state-database open: `<state-dir>/meta.redb` (meta-redb-spec §5).
//!
//! Relocated from tinio-fs's `database/open.rs` (plan 2026-10-03 Task 4). What
//! moves here is the redb half: the file name and path, the create, the
//! open-time **version gate** (`FORMAT_VERSION` is read from
//! [`crate::state`] and never re-declared), the shared-schema ensure and the
//! compact marker/stats snapshot — one write transaction, so a fresh
//! database is created, gated and measured atomically. What stays in
//! tinio-fs is the policy around it: which directory is the state dir, the
//! CHK014 permission hardening, and the fs-facing error mapping (see
//! `tinio_fs::database`).
//!
//! The state directory is created here when missing (a database cannot
//! exist without its directory); the directory is private to the owning
//! user only because tinio-fs hardens it.

use std::{
    fs,
    path::{Path, PathBuf},
};

use redb::{Database, ReadOnlyDatabase, ReadableDatabase};

use crate::{
    compact::{Stats, snapshot},
    ensure_all,
    error::Error,
    state,
};

/// The state-database file name inside the state directory.
pub const META_DB_FILE: &str = "meta.redb";

/// The state-database file path of a state directory.
pub fn meta_db_path(state_dir: &Path) -> PathBuf {
    state_dir.join(META_DB_FILE)
}

/// An opened state database plus `STATE` metadata read during [`open`].
#[derive(Debug)]
pub struct Open {
    /// The opened redb database.
    pub db: Database,
    /// The `STATE.compact_needed` marker at open time (absent → `false`).
    pub compact_needed: bool,
    /// Fragmentation snapshot from the open-time write transaction (for
    /// [`crate::compact::compact_if_needed`]).
    pub stats: Stats,
}

/// Open (or create) the state database at `<state_dir>/meta.redb`.
///
/// The state dir is created if missing. The tables are created in one
/// write transaction, and the `STATE` version is checked: a missing version
/// is written (fresh database), a mismatch fails with
/// [`Error::UnsupportedVersion`]. The `compact_needed` and `stats` fields
/// are read in the same write transaction for the startup compact path.
///
/// # Errors
///
/// [`Error::Io`] when the state directory cannot be created;
/// `Error::Open(..)` when the file exists but is not a valid redb
/// database (corrupted state — the metadata is derivable and the file can
/// be deleted for a rebuild); [`Error::UnsupportedVersion`] on a version
/// mismatch.
pub fn open(state_dir: &Path) -> Result<Open, Error> {
    fs::create_dir_all(state_dir)?;
    let path = meta_db_path(state_dir);
    let db = Database::create(&path)?;
    let (compact_needed, stats) = {
        let mut txn = db.begin_write()?;
        let compact_needed = {
            let mut state_table = state::Table::open(&mut txn)?;
            ensure_version(&mut state_table, &path)?;
            state_table.compact_marker()?
        };
        ensure_all(&mut txn)?;
        let stats = snapshot(&txn.stats()?);
        txn.commit()?;
        (compact_needed, stats)
    };
    Ok(Open {
        db,
        compact_needed,
        stats,
    })
}

/// Open (without creating) the state database at `<state_dir>/meta.redb`
/// **read-only** — the read-only run's one metadata source (FR-023
/// amended, design 2026-10-03 **Read paths**).
///
/// Nothing is created or written: the state dir, the file and the tables
/// must all be there already, and the file is left exactly as it was
/// found. The format gate runs here, once, so every later read works on a
/// database this build wrote: the `STATE` table's version row must exist
/// and must be [`state::FORMAT_VERSION`] (a missing table or row is a
/// database this build did not write; a different version is one it does
/// not read).
///
/// # Errors
///
/// `Error::Open(_)` when the file is absent, cannot be opened read-only,
/// or is not a state database (the `STATE` table is missing) —
/// [`ReadOnlyDatabase::open`]'s and the table open's failures — and
/// [`Error::UnsupportedVersion`] on a version row that is missing (`0`
/// stands in for it) or different. Every arm is the caller's to report:
/// a read-only run degrades to serving objects from their files with an
/// empty metadata view.
///
/// # Examples
///
/// ```rust
/// use tinio_store_redb::open;
///
/// let dir = tempfile::tempdir().unwrap();
/// let state = dir.path().join("state");
///
/// // Nothing to read yet: the open creates nothing — not the state
/// // directory, not the database — and answers the failure for the
/// // caller to report.
/// assert!(open::open_read_only(&state).is_err());
/// assert!(!state.exists(), "the open created {}", state.display());
///
/// // A read-write run creates the database...
/// drop(open::open(&state).unwrap());
/// // ...and the read-only open then serves it, creating nothing.
/// assert!(open::open_read_only(&state).is_ok());
/// ```
pub fn open_read_only(state_dir: &Path) -> Result<ReadOnlyDatabase, Error> {
    let path = meta_db_path(state_dir);
    let db = ReadOnlyDatabase::open(&path)?;
    let version = {
        let txn = db.begin_read()?;
        // The shared read-only handle, not a raw `open_table`: the
        // missing-table stand-in is scoped to the three version tables and
        // `bucket_lifecycle` (`TableDef::MAY_BE_ABSENT`, spec 2026-10-03 and
        // spec 2026-10-18), and `state` is not one of them — a missing `STATE`
        // table propagates redb's error through the same arm the raw open used
        // to raise, so the bypass had nothing left to bypass (a database with
        // no `STATE` table is not one this build wrote, not one whose
        // metadata is empty).
        let table = state::Table::open_readonly(&txn)?;
        table.version()?
    };
    match version {
        Some(found) if found == state::FORMAT_VERSION => Ok(db),
        Some(found) => Err(unsupported_version(&path, found)),
        None => Err(unsupported_version(&path, 0)),
    }
}

/// The outcome of a state-database integrity check (doctor reporting,
/// meta-redb-spec §5.8).
#[derive(Debug)]
pub enum Integrity {
    /// The database passed the check.
    Healthy,
    /// The check failed but was repaired automatically.
    Repaired,
    /// The check failed and could not be repaired (external tampering or
    /// bit rot) — the file must be deleted and rebuilt (the metadata is
    /// derivable and recomputed on demand).
    Corrupted(redb::DatabaseError),
}

/// Run a full integrity check on the state database at `state_dir` —
/// the mechanism behind doctor's integrity check, called by the doctor
/// orchestration in `tinio-cli`'s `commands/doctor/` for both its
/// diagnostics pass and its repair (meta-redb-spec §5.8/§5.9). Opens the
/// file exclusively: no server may be running.
///
/// # Errors
///
/// `Error::Open(..)` when the file does not exist or cannot be opened
/// (doctor reports it; `check_integrity` itself is redb's automatic repair
/// path).
pub fn check_integrity(state_dir: &Path) -> Result<Integrity, Error> {
    let path = meta_db_path(state_dir);
    let mut db = Database::open(&path)?;
    match db.check_integrity() {
        Ok(true) => Ok(Integrity::Healthy),
        Ok(false) => Ok(Integrity::Repaired),
        Err(err) => Ok(Integrity::Corrupted(err)),
    }
}

/// Check the format version: write [`state::FORMAT_VERSION`] on first
/// open, reject ANY mismatch (one current version — F06; no migration).
/// The gate's error carries the state file path, so the operator is told
/// which database to delete.
fn ensure_version(state_table: &mut state::Table<'_>, path: &Path) -> Result<u64, Error> {
    match state_table.version()? {
        None => {
            state_table.write_version(state::FORMAT_VERSION)?;
            Ok(state::FORMAT_VERSION)
        }
        Some(found) if found == state::FORMAT_VERSION => Ok(found),
        Some(found) => Err(unsupported_version(path, found)),
    }
}

/// The version-mismatch error of [`ensure_version`] and [`open_read_only`].
#[inline]
fn unsupported_version(path: &Path, found: u64) -> Error {
    Error::UnsupportedVersion {
        path: path.to_path_buf(),
        found,
        expected: state::FORMAT_VERSION,
    }
}

#[cfg(test)]
mod tests {
    use redb::{ReadableDatabase, TableHandle};

    use super::*;
    use crate::{
        bucket, object_meta, object_part, part, part_checksum, part_stamp, upload, upload_checksum,
    };

    fn open_db() -> (tempfile::TempDir, Database, Stats) {
        let dir = tempfile::tempdir().unwrap();
        let opened = open(dir.path()).unwrap();
        (dir, opened.db, opened.stats)
    }

    #[tokio::test]
    async fn open_creates_database_and_all_tables() {
        let (dir, db, _) = open_db();
        assert!(dir.path().join("meta.redb").exists());
        // Every table is openable (read transactions refuse missing
        // tables — the open-time write transaction created them).
        let txn = db.begin_read().unwrap();
        object_meta::Table::open_readonly(&txn).unwrap();
        bucket::Table::open_readonly(&txn).unwrap();
        upload::Table::open_readonly(&txn).unwrap();
        part::Table::open_readonly(&txn).unwrap();
        upload_checksum::Table::open_readonly(&txn).unwrap();
        part_checksum::Table::open_readonly(&txn).unwrap();
        part_stamp::Table::open_readonly(&txn).unwrap();
        object_part::Table::open_readonly(&txn).unwrap();
        state::Table::open_readonly(&txn).unwrap();
    }

    /// The on-disk byte-format contract: a freshly opened `meta.redb`
    /// contains exactly the twelve shared store tables plus the fs-local
    /// `state` table, and nothing else (shared-store-table-layer
    /// 2026-09-03 Goal 4 / spec §4 byte-format guard — this crate's own
    /// `ensure_all` test pins the twelve shared names; this pins the full
    /// schema the driver's open creates). Moved from tinio-fs's
    /// `database/open.rs`, 2026-10-01/2026-10-03.
    #[test]
    fn open_creates_exactly_the_schema_tables() {
        let dir = tempfile::tempdir().unwrap();
        let opened = open(dir.path()).unwrap();
        let txn = opened.db.begin_write().unwrap();
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
                "state",
                "upload_checksums",
                "uploads",
                "version_index",
                "versions",
            ]
        );
    }

    #[tokio::test]
    async fn stored_v1_databases_open() {
        // F06 + user ruling 2026-09-02: the version stays 1 — additive
        // schema changes (the checksum tables, the tagging rows /
        // `OBJECT_PARTS`) do NOT bump it, and a stored 1 opens (dev-local
        // databases are disposable: a stale same-version database written in
        // an older row format may fail at row decode — the operator deletes
        // it; that is not the version gate's job).
        let (dir, db, _) = open_db();
        {
            let mut txn = db.begin_write().unwrap();
            state::Table::open(&mut txn)
                .unwrap()
                .insert("version", 1)
                .unwrap();
            txn.commit().unwrap();
        }
        drop(db);
        let opened = open(dir.path()).unwrap();
        assert!(opened.db.begin_read().is_ok());
        drop(dir);
    }

    #[tokio::test]
    async fn open_twice_is_idempotent() {
        let (dir, _, _) = open_db();
        // A second open on the same file must succeed.
        let db = open(dir.path()).unwrap().db;
        let mut txn = db.begin_write().unwrap();
        let mut state_table = state::Table::open(&mut txn).unwrap();
        let version = ensure_version(&mut state_table, dir.path()).unwrap();
        assert_eq!(version, 1);
    }

    #[tokio::test]
    async fn version_mismatch_is_unsupported_version() {
        let (dir, db, _) = open_db();
        // Bump the version out-of-band.
        let mut txn = db.begin_write().unwrap();
        state::Table::open(&mut txn)
            .unwrap()
            .insert("version", 9)
            .unwrap();
        txn.commit().unwrap();
        // The first handle holds the redb file lock — drop it before
        // re-opening.
        drop(db);
        let err = open(dir.path()).unwrap_err();
        assert!(
            matches!(
                err,
                Error::UnsupportedVersion {
                    found: 9,
                    expected: 1,
                    ..
                }
            ),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn open_fresh_state_dir_creates_dir() {
        let base = tempfile::tempdir().unwrap();
        let nested = base.path().join("a/b");
        let db = open(&nested).unwrap().db;
        assert!(nested.join("meta.redb").exists());
        drop(db);
    }

    #[tokio::test]
    async fn corrupt_file_is_an_error() {
        let (dir, _, _) = open_db();
        fs::write(dir.path().join("meta.redb"), b"not a redb file").unwrap();
        let err = open(dir.path()).unwrap_err();
        assert!(matches!(err, Error::Open(_)), "{err:?}");
    }

    #[tokio::test]
    async fn check_integrity_reports_healthy() {
        let (dir, db, _) = open_db();
        // The integrity check opens the file exclusively — drop the
        // test handle first.
        drop(db);
        assert!(matches!(
            check_integrity(dir.path()).unwrap(),
            Integrity::Healthy
        ));
    }

    #[tokio::test]
    async fn check_integrity_fails_on_garbage() {
        let (dir, db, _) = open_db();
        drop(db);
        fs::write(dir.path().join("meta.redb"), b"junk").unwrap();
        // Unreadable file: the open itself fails (doctor reports the
        // database as unrecoverable — metadata is derivable).
        assert!(check_integrity(dir.path()).is_err());
    }

    /// FR-023 amended: the read-only open serves a database this build
    /// wrote and refuses a version it does not read — the gate runs once,
    /// at open.
    #[tokio::test]
    async fn read_only_open_gates_the_version() {
        let (dir, db, _) = open_db();
        let mut txn = db.begin_write().unwrap();
        state::Table::open(&mut txn)
            .unwrap()
            .set_compact_marker(true)
            .unwrap();
        txn.commit().unwrap();
        drop(db);

        // A database this build wrote opens and creates nothing.
        let before = fs::metadata(dir.path().join("meta.redb")).unwrap().len();
        drop(open_read_only(dir.path()).unwrap());
        assert_eq!(
            fs::metadata(dir.path().join("meta.redb")).unwrap().len(),
            before
        );

        // A version this build does not read is refused with the path.
        let db = Database::open(dir.path().join("meta.redb")).unwrap();
        let mut txn = db.begin_write().unwrap();
        state::Table::open(&mut txn)
            .unwrap()
            .insert("version", 7)
            .unwrap();
        txn.commit().unwrap();
        drop(db);
        let err = open_read_only(dir.path()).err().unwrap();
        assert!(
            matches!(
                err,
                Error::UnsupportedVersion {
                    found: 7,
                    expected: 1,
                    ..
                }
            ),
            "{err:?}"
        );
    }
}
