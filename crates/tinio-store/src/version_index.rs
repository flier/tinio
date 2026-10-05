//! `(bucket, key, id wire)` → `seq` — the version point-lookup index
//! (spec 2026-10-03-s3-versioning): the sequence of the version row a
//! `(bucket, key, version_id)` request names, so a version-addressed read
//! never scans the row table. The `id` element is the same wire form the
//! version row carries — the literal `null` for the null version, never an
//! empty string. Every write and removal happens in the same transaction
//! as the row it points at, so the two never disagree.

use redb::{ReadableTable, TableDefinition};

use crate::{
    error::Error,
    scan::drain_triple_str,
    table::{self, TableDef},
};

/// The per-table marker: the table definition for the shared handle arms.
#[doc(hidden)]
pub enum Def {}

impl TableDef for Def {
    type Key = (&'static str, &'static str, &'static str);
    type Value = u64;

    const DEF: TableDefinition<'static, Self::Key, Self::Value> =
        TableDefinition::new("version_index");
}

/// Handle to the version-index table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// The sequence of the version `id` names under `key` — `None` when the
    /// index has no such entry (a missing object, key, or version).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use redb::{Database, ReadableDatabase, backends::InMemoryBackend};
    /// use tinio_store::{ensure_all, version_index};
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
    /// let table = version_index::Table::open_readonly(&txn).unwrap();
    /// assert_eq!(table.seq_of("b", "k", "abc").unwrap(), None);
    /// ```
    pub fn seq_of(&self, bucket: &str, key: &str, id: &str) -> Result<Option<u64>, Error> {
        Ok(self.0.get((bucket, key, id))?.map(|guard| guard.value()))
    }
}

impl<'txn> table::Table<'txn, Def> {
    /// Point `(bucket, key, id)` at `seq` — the index half of the version
    /// row write, in the row's own transaction.
    pub fn put(&mut self, bucket: &str, key: &str, id: &str, seq: u64) -> Result<(), Error> {
        self.0.insert((bucket, key, id), seq)?;
        Ok(())
    }

    /// Remove the index entry of `id` unconditionally (idempotent). The
    /// row-removal paths use [`Self::remove_pointing_at`] instead, so that
    /// one row cannot retire an entry another row still needs.
    pub fn remove(&mut self, bucket: &str, key: &str, id: &str) -> Result<(), Error> {
        self.0.remove((bucket, key, id))?;
        Ok(())
    }

    /// Remove the index entry of `id` only when it currently points at
    /// `seq`: the row removal's own-transaction partner. Two rows can share
    /// one identity wire (the literal `null` is every null version's), and
    /// the index names only the newest write among them — so a row may only
    /// retire the entry it installed, or expiring one version would drop the
    /// entry that still resolves another.
    pub fn remove_pointing_at(
        &mut self,
        bucket: &str,
        key: &str,
        id: &str,
        seq: u64,
    ) -> Result<(), Error> {
        if self.seq_of(bucket, key, id)? == Some(seq) {
            self.0.remove((bucket, key, id))?;
        }
        Ok(())
    }

    /// Delete every index entry of `bucket` (bucket teardown; the entries
    /// of one bucket are contiguous from `(bucket, "", "")` — the mismatch
    /// break is the interval boundary).
    pub fn drain_bucket(&mut self, bucket: &str) -> Result<(), Error> {
        drain_triple_str(&mut self.0, (bucket, "", ""), |b, _, _| b == bucket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::handle;

    #[test]
    fn the_index_answers_seq_of_and_removes_idempotently() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut i = Table::open(txn)?;
            i.put("b", "k", "v1", 1)?;
            i.put("b", "k", "v2", 2)?;
            i.put("b", "other", "v1", 9)?;
            i.put("other", "k", "v1", 7)?;
            // The literal null is an ordinary index key too.
            i.put("b", "k", "null", 3)?;
            // An upsert to the same identity moves the sequence (the row
            // write replaces the version in place).
            i.put("b", "k", "v1", 4)?;
            assert_eq!(i.seq_of("b", "k", "v1")?, Some(4));
            assert_eq!(i.seq_of("b", "k", "null")?, Some(3));
            assert_eq!(i.seq_of("b", "other", "v1")?, Some(9));
            assert_eq!(i.seq_of("other", "k", "v1")?, Some(7));
            assert_eq!(i.seq_of("b", "k", "v9")?, None, "an unknown version");
            assert_eq!(i.seq_of("nope", "k", "v1")?, None, "an unknown bucket");
            i.remove("b", "k", "v1")?;
            i.remove("b", "k", "v1")?; // idempotent
            assert_eq!(i.seq_of("b", "k", "v1")?, None);
            assert_eq!(i.seq_of("b", "k", "v2")?, Some(2), "the other rows ride");
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let i = Table::open_readonly(txn)?;
            assert_eq!(i.seq_of("b", "k", "v2")?, Some(2), "the write committed");
            assert_eq!(i.seq_of("b", "k", "v1")?, None, "the removal committed");
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn a_bucket_drain_removes_that_buckets_entries_only() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut i = Table::open(txn)?;
            i.put("b", "k", "v1", 1)?;
            i.put("b", "other", "v1", 1)?;
            i.put("other", "k", "v1", 1)?;
            Ok(())
        })
        .unwrap();
        // The drain runs in its own transaction (drain and insert in one
        // transaction trips a redb page-manager assertion).
        h.write(|txn| -> Result<(), Error> { Table::open(txn)?.drain_bucket("b") })
            .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let i = Table::open_readonly(txn)?;
            assert_eq!(i.seq_of("b", "k", "v1")?, None);
            assert_eq!(i.seq_of("b", "other", "v1")?, None);
            assert_eq!(i.seq_of("other", "k", "v1")?, Some(1));
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn a_removal_leaves_neighbouring_entries_and_a_drain_stops_at_the_bucket_edge() {
        // Bucket names and keys that share a prefix ("b"/"b2", "k"/"kk") are
        // the drain's boundary: the walk breaks on the first tuple that is
        // not the drained bucket, so a prefix-adjacent name that sorts just
        // after it must never be swallowed, and an identity removal touches
        // exactly its own tuple.
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut i = Table::open(txn)?;
            i.put("b", "k", "v1", 1)?;
            i.put("b", "kk", "v1", 2)?; // the prefix-adjacent key
            i.put("b", "k", "v2", 5)?; // the same key, another identity
            i.put("b2", "k", "v1", 3)?; // the prefix-adjacent bucket
            i.put("b2", "k", "v2", 4)?;
            i.remove("b", "k", "v1")?;
            assert_eq!(
                i.seq_of("b", "k", "v1")?,
                None,
                "the removed entry stops resolving"
            );
            assert_eq!(
                i.seq_of("b", "k", "v2")?,
                Some(5),
                "the same key's other identity rides"
            );
            assert_eq!(i.seq_of("b", "kk", "v1")?, Some(2), "the other key rides");
            assert_eq!(
                i.seq_of("b2", "k", "v1")?,
                Some(3),
                "the other bucket rides"
            );
            Ok(())
        })
        .unwrap();
        h.write(|txn| -> Result<(), Error> { Table::open(txn)?.drain_bucket("b") })
            .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let i = Table::open_readonly(txn)?;
            assert_eq!(i.seq_of("b", "k", "v2")?, None, "the drain took both keys");
            assert_eq!(i.seq_of("b", "kk", "v1")?, None);
            assert_eq!(
                i.seq_of("b2", "k", "v1")?,
                Some(3),
                "the prefix-adjacent bucket survives the drain"
            );
            assert_eq!(i.seq_of("b2", "k", "v2")?, Some(4));
            Ok(())
        })
        .unwrap();
    }
}
