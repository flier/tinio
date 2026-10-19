//! `bucket` → `config wire` — the bucket's lifecycle configuration (spec
//! 2026-10-18, LC6). One row per configured bucket; absent means the bucket
//! stores no configuration, and a wire that does not decode self-heals to it
//! ([`Config::from_wire`]) — a row that cannot be read never becomes a rule.

use redb::{ReadableTable, TableDefinition};

use crate::{
    _core::lifecycle::Config,
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
        TableDefinition::new("bucket_lifecycle");
    /// A root written before the lifecycle machinery has no
    /// `bucket_lifecycle` table, and an absent row already means "no
    /// configuration": the missing table reads as empty
    /// ([`TableDef::MAY_BE_ABSENT`]).
    const MAY_BE_ABSENT: bool = true;
}

/// Handle to the bucket-lifecycle table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// The bucket's lifecycle configuration. `None` is a bucket with no row,
    /// and a corrupt wire reads the same way — the read path's self-heal, so
    /// a row that cannot be decoded never expires anything.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use redb::{Database, ReadableDatabase, backends::InMemoryBackend};
    /// use tinio_store_redb::{bucket_lifecycle, ensure_all};
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
    /// let table = bucket_lifecycle::Table::open_readonly(&txn).unwrap();
    /// assert_eq!(table.get("data").unwrap(), None);
    /// ```
    pub fn get(&self, bucket: &str) -> Result<Option<Config>, Error> {
        Ok(self
            .0
            .get(bucket)?
            .and_then(|guard| Config::from_wire(guard.value())))
    }

    /// Visit every recorded configuration in bucket-name order, skipping a
    /// row that does not decode (the same self-heal [`Self::get`] applies).
    pub fn for_each<F>(&self, mut visit: F) -> Result<(), Error>
    where
        F: FnMut(&str, Config) -> Result<(), Error>,
    {
        for item in self.0.iter()? {
            let (bucket, wire) = item?;
            if let Some(config) = Config::from_wire(wire.value()) {
                visit(bucket.value(), config)?;
            }
        }
        Ok(())
    }
}

impl<'txn> table::Table<'txn, Def> {
    /// Record the bucket's lifecycle configuration (upsert; the JSON wire is
    /// the stored element). An identical configuration is written again: the
    /// caller's `bool` is bucket presence, never change.
    pub fn set(&mut self, bucket: &str, config: &Config) -> Result<(), Error> {
        self.0.insert(bucket, config.to_wire().as_str())?;
        Ok(())
    }

    /// Remove the bucket's configuration row (bucket teardown). Idempotent:
    /// the table holds one row per bucket, so draining a bucket is this one
    /// removal.
    pub fn drain_bucket(&mut self, bucket: &str) -> Result<(), Error> {
        self.0.remove(bucket)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        _core::lifecycle::{Expiration, Filter, Rule, Status},
        testutil::handle,
    };

    /// A configuration with one rule — the shape every leg stores.
    fn config(id: &str) -> Config {
        Config {
            rules: vec![Rule {
                id: Some(id.to_owned()),
                status: Status::Enabled,
                filter: Filter {
                    prefix: Some("logs/".to_owned()),
                    ..Filter::default()
                },
                expiration: Some(Expiration {
                    days: Some(30),
                    date: None,
                    expired_object_delete_marker: None,
                }),
                noncurrent_version_expiration: None,
                abort_incomplete_multipart_upload: None,
            }],
        }
    }

    #[test]
    fn the_config_round_trips_and_an_unset_bucket_has_no_row() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            assert_eq!(t.get("data")?, None, "an unset bucket has no row");
            t.set("data", &config("one"))?;
            assert_eq!(t.get("data")?, Some(config("one")));
            // An upsert keeps the one-row-per-bucket shape.
            t.set("data", &config("two"))?;
            assert_eq!(t.get("data")?, Some(config("two")));
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            assert_eq!(
                Table::open_readonly(txn)?.get("data")?,
                Some(config("two")),
                "the last write is what the row carries"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn a_corrupt_wire_reads_as_no_configuration() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?
                .insert("data", "not json")
                .map_err(Error::from)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            assert_eq!(
                Table::open_readonly(txn)?.get("data")?,
                None,
                "a corrupt configuration is no configuration, never a rule"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn the_scan_visits_every_row_in_name_order_and_skips_a_corrupt_one() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            assert!(
                {
                    let mut seen = Vec::new();
                    t.for_each(|name, _| {
                        seen.push(name.to_owned());
                        Ok(())
                    })?;
                    seen
                }
                .is_empty(),
                "an empty table holds no configuration"
            );
            t.set("b", &config("one"))?;
            t.set("a", &config("two"))?;
            t.insert("corrupt", "not json").map_err(Error::from)?;
            let mut seen = Vec::new();
            t.for_each(|name, config| {
                seen.push((name.to_owned(), config));
                Ok(())
            })?;
            assert_eq!(
                seen,
                vec![
                    ("a".to_owned(), config("two")),
                    ("b".to_owned(), config("one"))
                ],
                "name order, and the corrupt row is no configuration"
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
            t.set("b", &config("one"))?;
            t.set("other", &config("two"))?;
            t.drain_bucket("b")?;
            t.drain_bucket("b")?; // idempotent
            assert_eq!(t.get("b")?, None);
            assert_eq!(t.get("other")?, Some(config("two")));
            Ok(())
        })
        .unwrap();
    }
}
