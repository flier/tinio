//! `(bucket, key, version_id wire)` → `(mode, retain_until unix nanos,
//! legal_hold)` — the per-version Object Lock row (design 2026-10-20, OL5).
//! The identity is the version-id wire, so the null version's key element is
//! the literal `null` and a lock-free version has no row. The row follows its
//! identity from current to history; a delete marker never gets one. The
//! decode is [`Lock::from_columns`]'s: a corrupt or partial row fails
//! **closed** — the caller sees a `Corrupt` contract error, never "unlocked".

use redb::{ReadableTable, TableDefinition};

use crate::{
    _core::{object_lock::Lock, storage},
    error::Error,
    scan::drain_triple_str,
    table::{self, TableDef},
};

/// The per-table marker: the table definition for the shared handle arms.
#[doc(hidden)]
pub enum Def {}

impl TableDef for Def {
    type Key = (&'static str, &'static str, &'static str);
    type Value = (
        Option<&'static str>, // the retention mode wire
        Option<i64>,          // retain-until, unix nanoseconds
        Option<&'static str>, // the legal-hold wire
    );

    const DEF: TableDefinition<'static, Self::Key, Self::Value> =
        TableDefinition::new("version_object_lock");
    /// A root written before the Object Lock feature has no
    /// `version_object_lock` table, and an absent row already means
    /// "unlocked": the missing table reads as empty
    /// ([`TableDef::MAY_BE_ABSENT`], OL4).
    const MAY_BE_ABSENT: bool = true;
}

/// Handle to the version Object Lock table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

/// Decode one stored row, fail-closed on every bad arm.
///
/// The three columns are the mode text, the retain-until instant and the
/// legal-hold text ([`Lock::from_columns`]). `Ok(None)` is an absent or
/// empty row; a corrupt mode, date, hold or a partial pair is the contract's
/// `Corrupt` error, so an unreadable row can never read as "unlocked".
pub(crate) fn decode(
    mode: Option<&str>,
    retain_until: Option<i64>,
    legal_hold: Option<&str>,
) -> Result<Option<Lock>, Error> {
    Lock::from_columns(mode, retain_until, legal_hold).map_err(|refusal| {
        tracing::warn!(
            %refusal,
            "a stored object lock row fails closed: corruption is not an absent lock"
        );
        Error::from(storage::corrupt(refusal.detail))
    })
}

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// One version's lock, or `None` when it has no row. A corrupt or partial
    /// row is [`storage::Error::Corrupt`], never `Ok(None)` (OL5).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use redb::{Database, ReadableDatabase, backends::InMemoryBackend};
    /// use tinio_store_redb::{ensure_all, version_object_lock};
    ///
    /// let db = Database::builder()
    ///     .create_with_backend(InMemoryBackend::new())
    ///     .unwrap();
    /// {
    ///     let mut txn = db.begin_write().unwrap();
    ///     ensure_all(&mut txn).unwrap();
    ///     txn.commit().unwrap();
    /// }
    /// let txn = db.begin_read().unwrap();
    /// let table = version_object_lock::Table::open_readonly(&txn).unwrap();
    /// assert_eq!(table.get("b", "k", "null").unwrap(), None);
    /// ```
    pub fn get(&self, bucket: &str, key: &str, id: &str) -> Result<Option<Lock>, Error> {
        let Some(guard) = self.0.get((bucket, key, id))? else {
            return Ok(None);
        };
        let (mode, retain_until, legal_hold) = guard.value();
        decode(mode, retain_until, legal_hold)
    }

    /// Whether the bucket holds any lock row (the bucket-emptiness probe).
    pub fn has_bucket(&self, bucket: &str) -> Result<bool, Error> {
        let mut iter = self.0.range((bucket, "", "")..)?;
        match iter.next() {
            Some(item) => {
                let (key, _) = item?;
                Ok(key.value().0 == bucket)
            }
            None => Ok(false),
        }
    }

    /// Visit every lock row of one bucket: the identity key, the identity
    /// wire and the three stored columns. The row the caller decodes is the
    /// same row [`Self::get`] serves.
    pub fn for_bucket<F>(&self, bucket: &str, mut visit: F) -> Result<(), Error>
    where
        F: FnMut(&str, &str, Option<&str>, Option<i64>, Option<&str>) -> Result<(), Error>,
    {
        for item in self.0.range((bucket, "", "")..)? {
            let (key, value) = item?;
            let (owner, object_key, id) = key.value();
            if owner != bucket {
                break;
            }
            let (mode, retain_until, legal_hold) = value.value();
            visit(object_key, id, mode, retain_until, legal_hold)?;
        }
        Ok(())
    }
}

impl<'txn> table::Table<'txn, Def> {
    /// Record one version's lock — an upsert of the whole row.
    pub fn put(&mut self, bucket: &str, key: &str, id: &str, lock: &Lock) -> Result<(), Error> {
        // The stored columns are the model's own encoder (the inverse of
        // `from_columns`), so every lock table writes the same three wires.
        let (mode, retain_until, legal_hold) = lock.columns();
        self.0.insert(
            (bucket, key, id),
            (mode.as_deref(), retain_until, Some(legal_hold.as_str())),
        )?;
        Ok(())
    }

    /// Remove one version's lock row (idempotent).
    pub fn remove(&mut self, bucket: &str, key: &str, id: &str) -> Result<(), Error> {
        self.0.remove((bucket, key, id))?;
        Ok(())
    }

    /// Delete every lock row of one bucket (bucket teardown and the drain; the
    /// rows of one bucket are contiguous from `(bucket, "", "")`).
    pub fn drain_bucket(&mut self, bucket: &str) -> Result<(), Error> {
        drain_triple_str(&mut self.0, (bucket, "", ""), |owner, _, _| owner == bucket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        _core::{
            object_lock::{LegalHold, Mode},
            storage::Error as StorageError,
        },
        testutil::{handle, lock},
    };

    #[test]
    fn the_lock_round_trips_under_the_null_identity_too() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            assert_eq!(
                t.get("b", "k", "v1")?,
                None,
                "a lock-free version has no row"
            );
            t.put("b", "k", "v1", &lock(Mode::Compliance, 700, LegalHold::Off))?;
            t.put(
                "b",
                "k",
                "null",
                &lock(Mode::Governance, 900, LegalHold::On),
            )?;
            assert_eq!(
                t.get("b", "k", "v1")?,
                Some(lock(Mode::Compliance, 700, LegalHold::Off))
            );
            assert_eq!(
                t.get("b", "k", "null")?,
                Some(lock(Mode::Governance, 900, LegalHold::On)),
                "the null version's identity is the literal `null`"
            );
            t.remove("b", "k", "v1")?;
            t.remove("b", "k", "v1")?; // idempotent
            assert_eq!(t.get("b", "k", "v1")?, None);
            assert!(
                t.get("b", "k", "null")?.is_some(),
                "the other identity rides"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn a_hold_only_row_is_a_lock_and_an_empty_row_is_not() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            // A hold with no retention: a legal-hold-only lock.
            t.put(
                "b",
                "k",
                "hold",
                &Lock {
                    retention: None,
                    legal_hold: LegalHold::On,
                },
            )?;
            assert_eq!(
                t.get("b", "k", "hold")?,
                Some(Lock {
                    retention: None,
                    legal_hold: LegalHold::On
                })
            );
            // A row with no retention and no hold is not a lock.
            t.insert(("b", "k", "empty"), (None, None, Some("OFF")))
                .map_err(Error::from)?;
            assert_eq!(t.get("b", "k", "empty")?, None);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn every_corrupt_row_fails_closed() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            // A corrupt mode.
            t.insert(
                ("b", "k", "mode"),
                (Some("NOT-A-MODE"), Some(1), Some("OFF")),
            )
            .map_err(Error::from)?;
            // A partial pair, both shapes: the mode without its date, and the
            // date without its mode (the retain-until half present alone).
            t.insert(
                ("b", "k", "partial"),
                (Some("GOVERNANCE"), None, Some("OFF")),
            )
            .map_err(Error::from)?;
            t.insert(("b", "k", "date"), (None, Some(1), Some("OFF")))
                .map_err(Error::from)?;
            // An absent legal hold beside a retention is corruption; an
            // all-absent row is "no lock", separately below.
            t.insert(("b", "k", "hold"), (Some("GOVERNANCE"), Some(1), None))
                .map_err(Error::from)?;
            // An unknown legal hold.
            t.insert(("b", "k", "bad-hold"), (None, None, Some("MAYBE")))
                .map_err(Error::from)?;
            // A row carrying no fact at all is not a lock, never an error.
            t.insert(("b", "k", "empty"), (None, None, None))
                .map_err(Error::from)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let t = Table::open_readonly(txn)?;
            for id in ["mode", "partial", "date", "hold", "bad-hold"] {
                let err = t.get("b", "k", id).unwrap_err();
                assert!(
                    matches!(StorageError::from(err), StorageError::Corrupt(_)),
                    "the {id} row must fail closed as Corrupt"
                );
            }
            assert_eq!(t.get("b", "k", "empty")?, None, "no fact is no lock");
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn the_bucket_scan_and_drain_take_that_buckets_rows_only() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            t.put("b", "k1", "v1", &lock(Mode::Governance, 10, LegalHold::Off))?;
            t.put(
                "b",
                "k2",
                "null",
                &lock(Mode::Compliance, 20, LegalHold::On),
            )?;
            t.put(
                "other",
                "k",
                "v1",
                &lock(Mode::Governance, 30, LegalHold::Off),
            )?;
            assert!(t.has_bucket("b")?);
            assert!(!t.has_bucket("b2")?, "a prefix-adjacent bucket is not `b`");
            let mut seen = Vec::new();
            t.for_bucket("b", |key, id, _, _, _| {
                seen.push((key.to_owned(), id.to_owned()));
                Ok(())
            })?;
            assert_eq!(
                seen,
                vec![
                    ("k1".to_owned(), "v1".to_owned()),
                    ("k2".to_owned(), "null".to_owned())
                ]
            );
            t.drain_bucket("b")?;
            assert!(!t.has_bucket("b")?);
            assert!(
                t.get("other", "k", "v1")?.is_some(),
                "the other bucket rides"
            );
            Ok(())
        })
        .unwrap();
    }
}
