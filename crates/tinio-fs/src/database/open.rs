//! Database open and integrity check.

use std::{
    fs,
    path::{Path, PathBuf},
};

use redb::Database;

use super::{
    compact::{Stats, snapshot},
    error::Error,
    tables::{BucketsTable, ObjectMetaTable, PartsTable, StateTable, UploadsTable},
};

const META_DB_FILE: &str = "meta.redb";

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
/// The state dir is created if missing. The five tables are created in one
/// write transaction, and the `STATE` version is checked: a missing version
/// is written (fresh database), a mismatch fails with
/// [`Error::UnsupportedVersion`]. The `compact_needed` and `stats` fields
/// are read in the same write transaction for the startup compact path.
///
/// # Errors
///
/// `Error::Open` when the file exists but is not a valid redb database
/// (corrupted state — the metadata is derivable and the file can be deleted
/// for a rebuild); `UnsupportedVersion` on a version mismatch.
pub fn open(state_dir: &Path) -> Result<Open, Error> {
    fs::create_dir_all(state_dir)?;
    let path = meta_db_path(state_dir);
    let db = Database::create(&path)?;
    let (compact_needed, stats) = {
        let mut txn = db.begin_write()?;
        let compact_needed = {
            let mut state = StateTable::open(&mut txn)?;
            state.ensure_version(&path)?;
            state.compact_marker()?
        };
        ObjectMetaTable::ensure(&mut txn)?;
        BucketsTable::ensure(&mut txn)?;
        UploadsTable::ensure(&mut txn)?;
        PartsTable::ensure(&mut txn)?;
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
/// the fs-side mechanism behind doctor's integrity check (the CLI wiring
/// lands with T073/T074, meta-redb-spec §5.8/§5.9). Opens the file
/// exclusively: no server may be running.
///
/// # Errors
///
/// `Error::Open` when the file does not exist or cannot be opened (doctor
/// reports it; `check_integrity` itself is redb's automatic repair path).
pub fn check_integrity(state_dir: &Path) -> Result<Integrity, Error> {
    let path = meta_db_path(state_dir);
    let mut db = Database::open(&path)?;
    match db.check_integrity() {
        Ok(true) => Ok(Integrity::Healthy),
        Ok(false) => Ok(Integrity::Repaired),
        Err(err) => Ok(Integrity::Corrupted(err)),
    }
}
