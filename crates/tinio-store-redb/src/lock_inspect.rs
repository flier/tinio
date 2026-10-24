//! A read-only scan of the three Object Lock tables for the doctor (OL31).
//!
//! The decoders self-heal: a corrupt bucket rule loses the rule and keeps the
//! enabled bit, and a corrupt per-version row fails closed only on the paths
//! that read it. The doctor exists to **report** what the readers silently
//! heal or refuse, so this scan reads the stored columns and asks the model's
//! own decoder which rows fail, then resolves every lock row to its version —
//! a current slot or an object version row — to find the ones that resolve to
//! nothing. It writes nothing, in any mode: the doctor is report-only, and the
//! fs startup repair is the only writer of these shapes (OL31).

use redb::{Database, ReadableDatabase, ReadableTable};

// The result vocabulary and the one rendering of a scan are the core's
// (`object_lock_inspect`); this module owns only the redb scan body. Re-exported
// so every existing `lock_inspect::Scan` path still resolves.
pub use crate::_core::object_lock_inspect::{
    CorruptConfig, CorruptIntent, CorruptLock, InspectKind, OrphanLock, Scan,
};
use crate::{
    _core::object_lock::{Config, Lock},
    bucket_object_lock,
    error::Error,
    meta::version_lock::LockOwned,
    upload_object_lock, version_object_lock,
};

/// One stored lock row, owned (the guards end with the scan).
struct StoredLock {
    bucket: String,
    key: String,
    version: String,
    mode: Option<String>,
    retain_until: Option<i64>,
    legal_hold: Option<String>,
}

/// Scan the three Object Lock tables of `db` in one read transaction.
///
/// A row that carries no lock fact at all (all three columns absent, or a hold
/// `OFF` with no retention) is not a lock — the model says so — and is neither
/// corrupt nor an orphan. Every other undecodable row is reported, and every
/// row that decodes or not is resolved against the version index and the
/// key's current slot: a lock that resolves to neither is an orphan (the
/// drain's own rule, OL8/OL31).
///
/// # Examples
///
/// ```rust
/// use redb::{Database, backends::InMemoryBackend};
/// use tinio_store_redb::{ensure_all, lock_inspect};
///
/// let db = Database::builder()
///     .create_with_backend(InMemoryBackend::new())
///     .unwrap();
/// {
///     let mut txn = db.begin_write().unwrap();
///     ensure_all(&mut txn).unwrap();
///     txn.commit().unwrap();
/// }
/// assert!(lock_inspect::scan(&db).unwrap().is_clean());
/// ```
pub fn scan(db: &Database) -> Result<Scan, Error> {
    let mut txn = db.begin_read()?;

    let corrupt_configs = {
        let table = bucket_object_lock::Table::open_readonly(&txn)?;
        let mut rows = Vec::new();
        table.for_each(|bucket, enabled, rule| {
            // An empty wire is "no rule". A non-empty wire that decodes to no
            // rule is the corruption the decoder heals — and the enabled bit
            // survives it.
            if !rule.is_empty() && Config::from_rule_wire(enabled, rule).rule.is_none() {
                rows.push(CorruptConfig {
                    bucket: bucket.to_owned(),
                    enabled,
                });
            }
            Ok(())
        })?;
        rows
    };

    let stored: Vec<StoredLock> = {
        let table = version_object_lock::Table::open_readonly(&txn)?;
        let mut rows = Vec::new();
        for item in table.0.iter()? {
            let (key, value) = item?;
            let (bucket, object_key, version) = key.value();
            let (mode, retain_until, legal_hold) = value.value();
            rows.push(StoredLock {
                bucket: bucket.to_owned(),
                key: object_key.to_owned(),
                version: version.to_owned(),
                mode: mode.map(str::to_owned),
                retain_until,
                legal_hold: legal_hold.map(str::to_owned),
            });
        }
        rows
    };

    let mut corrupt_locks = Vec::new();
    let mut orphan_locks = Vec::new();
    for row in stored {
        let decoded = Lock::from_columns(
            row.mode.as_deref(),
            row.retain_until,
            row.legal_hold.as_deref(),
        );
        // A row carrying no lock fact is not a lock: neither corrupt nor an
        // orphan (the model's own wrapping rule).
        if matches!(decoded, Ok(None)) {
            continue;
        }
        // The drain's own ownership rule ([`LockOwned`]): an object version
        // row through the index, or the key's non-marker current slot.
        let owned = txn.lock_owned(&row.bucket, &row.key, &row.version)?;
        if !owned {
            orphan_locks.push(OrphanLock {
                bucket: row.bucket.clone(),
                key: row.key.clone(),
                version: row.version.clone(),
            });
        }
        if let Err(refusal) = decoded {
            corrupt_locks.push(CorruptLock {
                bucket: row.bucket,
                key: row.key,
                version: row.version,
                detail: refusal.detail,
            });
        }
    }

    let corrupt_intents = {
        let table = upload_object_lock::Table::open_readonly(&txn)?;
        let mut rows = Vec::new();
        for item in table.0.iter()? {
            let (key, value) = item?;
            let (bucket, upload) = key.value();
            let (mode, retain_until, legal_hold) = value.value();
            if let Err(refusal) = Lock::from_columns(mode, retain_until, legal_hold) {
                rows.push(CorruptIntent {
                    bucket: bucket.to_owned(),
                    upload: upload.to_owned(),
                    detail: refusal.detail,
                });
            }
        }
        rows
    };

    Ok(Scan {
        corrupt_configs,
        corrupt_locks,
        corrupt_intents,
        orphan_locks,
    })
}
