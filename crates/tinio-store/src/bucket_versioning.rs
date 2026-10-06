//! `bucket` → `state wire` — the bucket's versioning state (spec
//! 2026-10-03-s3-versioning): `Unversioned` (no row, or the state written
//! back), `Enabled`, or `Suspended`, spelled by [`State`]'s wire names.
//! The table is the
//! one row per bucket the versioning state needs; absent means
//! `Unversioned`, and a corrupt wire self-heals to it — a row that cannot
//! say its state never turns versioning on.

use redb::{ReadableTable, TableDefinition};

use crate::{
    _core::version::State,
    error::Error,
    table::{self, TableDef},
};

/// The per-table marker: the table definition for the shared handle arms.
#[doc(hidden)]
pub enum Def {}

impl TableDef for Def {
    type Key = &'static str;
    type Value = &'static str;

    const DEF: TableDefinition<'static, Self::Key, Self::Value> =
        TableDefinition::new("bucket_versioning");
    /// A root written before the version machinery has no
    /// `bucket_versioning` table, and an absent row already means
    /// `Unversioned`: the missing table reads as empty
    /// ([`TableDef::MAY_BE_ABSENT`], spec 2026-10-03).
    const MAY_BE_ABSENT: bool = true;
}

/// Handle to the bucket-versioning table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// The bucket's versioning state. `None` is a bucket with no row — it
    /// is unversioned; a corrupt wire self-heals to `Unversioned` too
    /// (`State`'s default), so a row that cannot be read never enables
    /// versioning.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use redb::{Database, ReadableDatabase, backends::InMemoryBackend};
    /// use tinio_store::{bucket_versioning, ensure_all};
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
    /// let table = bucket_versioning::Table::open_readonly(&txn).unwrap();
    /// assert_eq!(table.get("data").unwrap(), None);
    /// ```
    pub fn get(&self, bucket: &str) -> Result<Option<State>, Error> {
        Ok(self
            .0
            .get(bucket)?
            .map(|guard| guard.value().parse::<State>().unwrap_or_default()))
    }

    /// How many buckets hold version data: one range scan of the table,
    /// counting the rows that do not read [`State::Unversioned`] — a corrupt
    /// wire reads as `Unversioned` too, the same self-heal [`Self::get`]
    /// applies, so a row that cannot say its state never counts. One walk of
    /// the table, never a per-bucket lookup: this is the toggle-off startup
    /// warning's number, and a large root must stay cheap.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use redb::{Database, ReadableDatabase, backends::InMemoryBackend};
    /// use tinio_store::{_core::version::State, bucket_versioning, ensure_all};
    ///
    /// let db = Database::builder()
    ///     .create_with_backend(InMemoryBackend::new())
    ///     .unwrap();
    /// {
    ///     let mut txn = db.begin_write().unwrap();
    ///     ensure_all(&mut txn).unwrap();
    ///     {
    ///         let mut table = bucket_versioning::Table::open(&mut txn).unwrap();
    ///         table.set("data", State::Enabled).unwrap();
    ///         table.set("backup", State::Suspended).unwrap();
    ///     }
    ///     txn.commit().unwrap();
    /// }
    /// let txn = db.begin_read().unwrap();
    /// let table = bucket_versioning::Table::open_readonly(&txn).unwrap();
    /// assert_eq!(table.versioned_count().unwrap(), 2);
    /// ```
    pub fn versioned_count(&self) -> Result<usize, Error> {
        let mut count = 0;
        for entry in self.0.iter()? {
            let (_, value) = entry?;
            if value.value().parse::<State>().unwrap_or_default() != State::Unversioned {
                count += 1;
            }
        }
        Ok(count)
    }
}

impl<'txn> table::Table<'txn, Def> {
    /// Record the bucket's versioning state (upsert; the state's wire name
    /// is the stored element).
    pub fn set(&mut self, bucket: &str, state: State) -> Result<(), Error> {
        self.0.insert(bucket, state.to_string().as_str())?;
        Ok(())
    }

    /// Remove the bucket's state row (bucket teardown). Idempotent: the
    /// table holds one row per bucket, so draining a bucket is this one
    /// removal.
    pub fn drain_bucket(&mut self, bucket: &str) -> Result<(), Error> {
        self.0.remove(bucket)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::handle;

    #[test]
    fn the_state_round_trips_under_its_wire_names() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            for state in [State::Enabled, State::Suspended, State::Unversioned] {
                t.set("data", state)?;
                assert_eq!(t.get("data")?, Some(state), "{state} round-trips");
            }
            assert_eq!(t.get("other")?, None, "an unrecorded bucket has no row");
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            assert_eq!(
                Table::open_readonly(txn)?.get("data")?,
                Some(State::Unversioned),
                "the last write is what the row carries"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn an_absent_row_and_an_explicit_unversioned_row_both_read_as_unversioned() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            assert_eq!(t.get("data")?, None, "an unset bucket has no row");
            assert_eq!(
                t.get("never-set")?.unwrap_or_default(),
                State::Unversioned,
                "the caller's documented default for an absent row"
            );
            // Writing `Unversioned` back is a row (the recorded state), not
            // an absence: the two shapes are distinct at the table, and both
            // read as unversioned through the caller's default.
            t.set("data", State::Unversioned)?;
            assert_eq!(t.get("data")?, Some(State::Unversioned));
            // An upsert keeps the one-row-per-bucket shape.
            t.set("data", State::Enabled)?;
            assert_eq!(t.get("data")?, Some(State::Enabled));
            t.set("data", State::Suspended)?;
            assert_eq!(t.get("data")?, Some(State::Suspended));
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let t = Table::open_readonly(txn)?;
            assert_eq!(
                t.get("data")?,
                Some(State::Suspended),
                "the last write is what the row carries"
            );
            assert_eq!(
                t.get("never-set")?.unwrap_or_default(),
                State::Unversioned,
                "absent means unversioned, never an error"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn a_corrupt_state_wire_self_heals_to_unversioned() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?
                .insert("data", "not-a-state")
                .map_err(Error::from)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            assert_eq!(
                Table::open_readonly(txn)?.get("data")?,
                Some(State::Unversioned),
                "a corrupt state is the default, never Enabled"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn the_versioned_count_is_one_scan_and_never_counts_a_silent_row() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            assert_eq!(
                t.versioned_count()?,
                0,
                "an empty table holds no version data"
            );
            t.set("enabled", State::Enabled)?;
            t.set("suspended", State::Suspended)?;
            // A recorded `Unversioned` row holds no version data, and a
            // corrupt wire reads as `Unversioned` (the point read's self-heal):
            // neither counts.
            t.set("explicit", State::Unversioned)?;
            t.insert("corrupt", "not-a-state").map_err(Error::from)?;
            assert_eq!(t.versioned_count()?, 2);
            t.drain_bucket("enabled")?;
            assert_eq!(t.versioned_count()?, 1);
            t.drain_bucket("suspended")?;
            assert_eq!(
                t.versioned_count()?,
                0,
                "the explicit Unversioned row and the corrupt one both read unversioned"
            );
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            assert_eq!(
                Table::open_readonly(txn)?.versioned_count()?,
                0,
                "the read handle sees the same rows the write handle wrote"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn a_bucket_drain_removes_that_buckets_row_only() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            t.set("b", State::Enabled)?;
            t.set("other", State::Enabled)?;
            t.drain_bucket("b")?;
            t.drain_bucket("b")?; // idempotent
            assert_eq!(t.get("b")?, None);
            assert_eq!(t.get("other")?, Some(State::Enabled));
            Ok(())
        })
        .unwrap();
    }
}
