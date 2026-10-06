//! Database open and integrity check.

#[cfg(unix)]
use std::fs::Permissions;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{
    fs,
    path::{Path, PathBuf},
};

use redb::{Database, ReadOnlyDatabase, ReadableDatabase};

use super::{
    compact::{Stats, snapshot},
    error::{Error, unsupported_version},
    tables::ensure_version,
};
use crate::_store::{ensure_all, state};

const META_DB_FILE: &str = "meta.redb";

/// The state-directory mode on unix (CHK014): private to the owning user.
#[cfg(unix)]
const STATE_DIR_MODE: u32 = 0o700;

/// The state-database file mode on unix (CHK014): private to the owning
/// user.
#[cfg(unix)]
const META_DB_MODE: u32 = 0o600;

fn meta_db_path(state_dir: &Path) -> PathBuf {
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
    /// [`super::compact::compact_if_needed`]).
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
/// On unix the private state is hardened after creation, per the project's
/// CHK014 requirement: the state directory is `0700` and `meta.redb` is
/// `0600`, regardless of the process umask. On Windows the files inherit
/// the creating user's default ACLs (std has no ACL API; the state dir is
/// created under the storage root's `.tinio` segment).
///
/// # Errors
///
/// `Error::Redb(Open(..))` when the file exists but is not a valid redb
/// database (corrupted state — the metadata is derivable and the file can
/// be deleted for a rebuild); `UnsupportedVersion` on a version mismatch.
pub fn open(state_dir: &Path) -> Result<Open, Error> {
    fs::create_dir_all(state_dir)?;
    #[cfg(unix)]
    fs::set_permissions(state_dir, Permissions::from_mode(STATE_DIR_MODE))?;
    let path = meta_db_path(state_dir);
    let db = Database::create(&path).map_err(|e| Error::Redb(e.into()))?;
    #[cfg(unix)]
    fs::set_permissions(&path, Permissions::from_mode(META_DB_MODE))?;
    let (compact_needed, stats) = {
        let mut txn = db.begin_write().map_err(|e| Error::Redb(e.into()))?;
        let compact_needed = {
            let mut state = state::Table::open(&mut txn)?;
            ensure_version(&mut state, &path)?;
            state.compact_marker()?
        };
        ensure_all(&mut txn)?;
        let stats = snapshot(&txn.stats().map_err(|e| Error::Redb(e.into()))?);
        txn.commit().map_err(|e| Error::Redb(e.into()))?;
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
/// `Error::Redb(_)` when the file is absent, cannot be opened read-only,
/// or is not a state database (the `STATE` table is missing) —
/// [`ReadOnlyDatabase::open`]'s and the table open's failures, unwrapped
/// from the shared error as this `open`'s are; [`Error::UnsupportedVersion`]
/// on a version row that is missing (`0` stands in for it) or different.
/// Every arm is the caller's to report: a read-only run degrades to
/// serving objects from their files with an empty metadata view.
///
/// # Examples
///
/// ```rust
/// use tinio_fs::database;
///
/// let dir = tempfile::tempdir().unwrap();
/// let state = dir.path().join("state");
///
/// // Nothing to read yet: the open creates nothing — not the state
/// // directory, not the database — and answers the failure for the
/// // caller to report.
/// assert!(database::open_read_only(&state).is_err());
/// assert!(!state.exists(), "the open created {}", state.display());
///
/// // A read-write run creates the database...
/// drop(database::open(&state).unwrap());
/// // ...and the read-only open then serves it, creating nothing.
/// assert!(database::open_read_only(&state).is_ok());
/// ```
pub fn open_read_only(state_dir: &Path) -> Result<ReadOnlyDatabase, Error> {
    let path = meta_db_path(state_dir);
    let db = ReadOnlyDatabase::open(&path).map_err(|e| Error::Redb(e.into()))?;
    let version = {
        let txn = db.begin_read().map_err(|e| Error::Redb(e.into()))?;
        // The shared read-only handle, not a raw `open_table`: the
        // missing-table stand-in is scoped to the three version tables
        // (`TableDef::MAY_BE_ABSENT`, spec 2026-10-03), and `state` is not
        // one of them — a missing `STATE` table propagates redb's error
        // through the same arm the raw open used to raise, so the bypass
        // had nothing left to bypass (a database with no `STATE` table is
        // not one this build wrote, not one whose metadata is empty).
        let table = state::Table::open_readonly(&txn)?;
        table.version()?
    };
    match version {
        Some(found) if found == state::FORMAT_VERSION => Ok(db),
        Some(found) => Err(unsupported_version(&path, found, state::FORMAT_VERSION)),
        None => Err(unsupported_version(&path, 0, state::FORMAT_VERSION)),
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use redb::TableHandle;

    use super::*;

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

    /// The on-disk byte-format contract: a freshly opened `meta.redb` contains
    /// exactly the ten shared store tables plus the fs-local `state` table,
    /// and nothing else (shared-store-table-layer 2026-09-03 Goal 4 / spec §4
    /// byte-format guard — tinio-store's own `ensure_all` test pins the ten
    /// shared names; this pins the full fs schema). Moved inline from
    /// `tests/layout.rs`, 2026-10-01; the three version tables joined it with
    /// the versioning plan (2026-10-03).
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
                "bucket_versioning",
                "buckets",
                "object_meta",
                "object_parts",
                "part_checksums",
                "parts",
                "state",
                "upload_checksums",
                "uploads",
                "version_index",
                "versions",
            ]
        );
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
/// the fs-side mechanism behind doctor's integrity check, called by the
/// doctor orchestration in `tinio-cli`'s `commands/doctor/` for both its
/// diagnostics pass and its repair (meta-redb-spec §5.8/§5.9). Opens the
/// file exclusively: no server may be running.
///
/// # Errors
///
/// `Error::Redb(Open(..))` when the file does not exist or cannot be
/// opened (doctor reports it; `check_integrity` itself is redb's automatic
/// repair path).
pub fn check_integrity(state_dir: &Path) -> Result<Integrity, Error> {
    let path = meta_db_path(state_dir);
    let mut db = Database::open(&path).map_err(|e| Error::Redb(e.into()))?;
    match db.check_integrity() {
        Ok(true) => Ok(Integrity::Healthy),
        Ok(false) => Ok(Integrity::Repaired),
        Err(err) => Ok(Integrity::Corrupted(err)),
    }
}
