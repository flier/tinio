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
//!
//! **Why this module has its own scan body.** The two stores own their own
//! tables and their own statements: redb reads a table walk and this crate
//! reads SQL, and neither crate depends on the other (the fs backend must build
//! without PostgreSQL, and pg without redb). The shared half is the model's
//! decoders — [`Config`] and [`Lock`] — and the result vocabulary, which lives
//! in `tinio-core`'s `object_lock_inspect` and is re-exported here, so a
//! corrupt row is classified by exactly the same code and reported with the
//! same words on either backend.
//!
//! The statements live in `sql/diagnostics/`, are macro-checked, and are
//! pinned in `.sqlx/` like every other statement of the crate. Nothing here
//! writes and nothing here takes a row lock: a diagnosis must not mutate its
//! subject (PG11).

use sqlx::{query_file, query_file_as};

// The result vocabulary and the one rendering of a scan are the core's
// (`object_lock_inspect`); this module owns only the SQL scan body. Re-exported
// so every existing `lock_inspect::Scan` path still resolves.
pub use crate::_core::object_lock_inspect::{
    CorruptConfig, CorruptIntent, CorruptLock, InspectKind, OrphanLock, Scan,
};
use crate::{
    _core::object_lock::{Config, Lock},
    Error, begin,
    pool::Pool,
};

/// One stored bucket configuration row, owned (the scan drops the rows).
struct ConfigRow {
    bucket: String,
    enabled: bool,
    rule: String,
}

/// One stored lock row with the drain's ownership answer, owned (the guards
/// end with the scan).
struct LockScanRow {
    bucket: String,
    key: String,
    version_id: String,
    mode: Option<String>,
    retain_until: Option<i64>,
    legal_hold: Option<String>,
    owned: bool,
}

/// One stored multipart intent row, owned.
struct IntentRow {
    bucket: String,
    upload_id: String,
    mode: Option<String>,
    retain_until: Option<i64>,
    legal_hold: Option<String>,
}

/// Scan the three Object Lock tables of `pool`.
///
/// A lock row that carries no lock fact at all (all three columns absent, or a
/// hold `OFF` with no retention) is not a lock — the model says so — and is
/// neither corrupt nor an orphan. Every other undecodable row is reported, and
/// every row that decodes or not is resolved against the version index and the
/// key's current slot: a lock that resolves to neither is an orphan (the
/// drain's own rule, OL8/OL31).
///
/// # Errors
///
/// The driver's answer when a statement fails; a diagnosis that cannot read a
/// table must say so rather than answer "clean".
pub async fn scan(pool: &Pool) -> Result<Scan, Error> {
    // One repeatable-read snapshot for all three tables: a lock row and the
    // version or slot that owns it must be read from the same state, or a
    // concurrent write could make a row look orphaned between statements (the
    // redb scan's one read transaction, over SQL).
    let mut tx = begin(pool).await?;
    query_file!("sql/diagnostics/lock_scan_begin.sql")
        .execute(&mut *tx)
        .await?;

    let configs = query_file_as!(ConfigRow, "sql/diagnostics/lock_scan_configs.sql")
        .fetch_all(&mut *tx)
        .await?;
    let corrupt_configs = configs
        .into_iter()
        .filter(|row| {
            // An empty wire is "no rule". A non-empty wire that decodes to no
            // rule is the corruption the decoder heals — and the enabled bit
            // survives it.
            !row.rule.is_empty()
                && Config::from_rule_wire(row.enabled, &row.rule)
                    .rule
                    .is_none()
        })
        .map(|row| CorruptConfig {
            bucket: row.bucket,
            enabled: row.enabled,
        })
        .collect();

    let stored = query_file_as!(LockScanRow, "sql/diagnostics/lock_scan_locks.sql")
        .fetch_all(&mut *tx)
        .await?;
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
        // The drain's own ownership rule, evaluated by the statement: an
        // object version row through the index, or the key's non-marker
        // current slot.
        if !row.owned {
            orphan_locks.push(OrphanLock {
                bucket: row.bucket.clone(),
                key: row.key.clone(),
                version: row.version_id.clone(),
            });
        }
        if let Err(refusal) = decoded {
            corrupt_locks.push(CorruptLock {
                bucket: row.bucket,
                key: row.key,
                version: row.version_id,
                detail: refusal.detail,
            });
        }
    }

    let intents = query_file_as!(IntentRow, "sql/diagnostics/lock_scan_intents.sql")
        .fetch_all(&mut *tx)
        .await?;
    let corrupt_intents = intents
        .into_iter()
        .filter_map(|row| {
            Lock::from_columns(
                row.mode.as_deref(),
                row.retain_until,
                row.legal_hold.as_deref(),
            )
            .err()
            .map(|refusal| CorruptIntent {
                bucket: row.bucket,
                upload: row.upload_id,
                detail: refusal.detail,
            })
        })
        .collect();

    // The scan is report-only: a rollback ends the read snapshot and writes
    // nothing (there is nothing a read-only transaction could commit).
    tx.rollback().await?;

    Ok(Scan {
        corrupt_configs,
        corrupt_locks,
        corrupt_intents,
        orphan_locks,
    })
}

#[cfg(test)]
#[path = "lock_inspect_db_tests.rs"]
mod db_tests;
