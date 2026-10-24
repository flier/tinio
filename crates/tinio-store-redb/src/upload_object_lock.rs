//! `(bucket, upload_id)` → `(mode, retain_until unix nanos, legal_hold)` — the
//! multipart upload's Object Lock intent (design 2026-10-20, OL6). The intent
//! is resolved once at create and written in the upload row's transaction, so
//! a mid-upload configuration change cannot alter it; completion applies it
//! and abort, expiry and teardown clear it. The columns are the version lock
//! row's, and the same [`Lock::from_columns`] decode fails closed.

use redb::{ReadableTable, TableDefinition};

use crate::{
    _core::object_lock::Lock,
    error::Error,
    scan::drain_pair,
    table::{self, TableDef},
    version_object_lock,
};
/// The per-table marker: the table definition for the shared handle arms.
#[doc(hidden)]
pub enum Def {}

impl TableDef for Def {
    type Key = (&'static str, &'static str);
    type Value = (
        Option<&'static str>, // the retention mode wire
        Option<i64>,          // retain-until, unix nanoseconds
        Option<&'static str>, // the legal-hold wire
    );

    const DEF: TableDefinition<'static, Self::Key, Self::Value> =
        TableDefinition::new("upload_object_lock");
    /// A root written before the Object Lock feature has no
    /// `upload_object_lock` table, and an absent row already means
    /// "no intent": the missing table reads as empty
    /// ([`TableDef::MAY_BE_ABSENT`], OL4).
    const MAY_BE_ABSENT: bool = true;
}

/// Handle to the upload Object Lock table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// An upload's stored lock intent, or `None` when it stores none (OL6). A
    /// corrupt row is [`crate::_core::storage::Error::Corrupt`] — an unreadable
    /// intent may hide a lock, so it never reads as "no intent".
    ///
    /// # Examples
    ///
    /// ```rust
    /// use redb::{Database, ReadableDatabase, backends::InMemoryBackend};
    /// use tinio_store_redb::{ensure_all, upload_object_lock};
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
    /// let table = upload_object_lock::Table::open_readonly(&txn).unwrap();
    /// assert_eq!(table.get("b", "upload-1").unwrap(), None);
    /// ```
    pub fn get(&self, bucket: &str, upload_id: &str) -> Result<Option<Lock>, Error> {
        let Some(guard) = self.0.get((bucket, upload_id))? else {
            return Ok(None);
        };
        let (mode, retain_until, legal_hold) = guard.value();
        // The intent's columns are the version lock's own; one decoder (with
        // its warn-and-Corrupt mapping) serves both, so the two can never
        // disagree about what a corrupt row reads as (OL5/OL6).
        version_object_lock::decode(mode, retain_until, legal_hold)
    }

    /// Whether the bucket holds any upload lock row (the bucket-emptiness
    /// probe).
    pub fn has_bucket(&self, bucket: &str) -> Result<bool, Error> {
        let mut iter = self.0.range((bucket, "")..)?;
        match iter.next() {
            Some(item) => {
                let (key, _) = item?;
                Ok(key.value().0 == bucket)
            }
            None => Ok(false),
        }
    }
}

impl<'txn> table::Table<'txn, Def> {
    /// Record one upload's lock intent (upsert).
    pub fn put(&mut self, bucket: &str, upload_id: &str, lock: &Lock) -> Result<(), Error> {
        // The intent's columns are the model's own encoder — the same one the
        // version lock row uses — so the two cannot disagree about what a
        // lock row stores.
        let (mode, retain_until, legal_hold) = lock.columns();
        self.0.insert(
            (bucket, upload_id),
            (mode.as_deref(), retain_until, Some(legal_hold.as_str())),
        )?;
        Ok(())
    }

    /// Remove one upload's intent (idempotent) — completion's consume and the
    /// abort/expiry/teardown's removal.
    pub fn remove(&mut self, bucket: &str, upload_id: &str) -> Result<(), Error> {
        self.0.remove((bucket, upload_id))?;
        Ok(())
    }

    /// Delete every intent row of one bucket (bucket teardown; the rows of one
    /// bucket are contiguous from `(bucket, "")`).
    pub fn drain_bucket(&mut self, bucket: &str) -> Result<(), Error> {
        drain_pair(&mut self.0, (bucket, ""), |owner, _| owner == bucket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        _core::{
            object_lock::{LegalHold, Mode},
            storage,
        },
        testutil::{handle, lock},
    };

    #[test]
    fn the_intent_round_trips_and_clears_idempotently() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            assert_eq!(t.get("b", "upload-1")?, None);
            t.put("b", "upload-1", &lock(Mode::Governance, 10, LegalHold::Off))?;
            assert_eq!(
                t.get("b", "upload-1")?,
                Some(lock(Mode::Governance, 10, LegalHold::Off))
            );
            // A re-create replaces the intent.
            t.put("b", "upload-1", &lock(Mode::Compliance, 20, LegalHold::On))?;
            assert_eq!(
                t.get("b", "upload-1")?,
                Some(lock(Mode::Compliance, 20, LegalHold::On))
            );
            t.remove("b", "upload-1")?;
            t.remove("b", "upload-1")?; // idempotent
            assert_eq!(t.get("b", "upload-1")?, None);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn a_corrupt_intent_fails_closed() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?
                .insert(("b", "upload-1"), (Some("GOVERNANCE"), None, Some("OFF")))
                .map_err(Error::from)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let err = Table::open_readonly(txn)?.get("b", "upload-1").unwrap_err();
            assert!(
                matches!(storage::Error::from(err), storage::Error::Corrupt(_)),
                "a partial intent is Corrupt, never \"no intent\""
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn a_bucket_drain_takes_that_buckets_intents_only() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            t.put("b", "upload-1", &lock(Mode::Governance, 1, LegalHold::Off))?;
            t.put(
                "other",
                "upload-1",
                &lock(Mode::Governance, 2, LegalHold::Off),
            )?;
            assert!(t.has_bucket("b")?);
            assert!(!t.has_bucket("b2")?, "a prefix-adjacent bucket is not `b`");
            t.drain_bucket("b")?;
            assert!(!t.has_bucket("b")?);
            assert!(
                t.get("other", "upload-1")?.is_some(),
                "the other bucket rides"
            );
            Ok(())
        })
        .unwrap();
    }
}
