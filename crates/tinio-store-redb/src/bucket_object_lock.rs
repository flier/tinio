//! `bucket` → `(enabled, rule wire)` — the bucket's Object Lock configuration
//! (design 2026-10-20, OL4). One row per configured bucket; absent means the
//! bucket stores no configuration. The rule wire is the model's
//! ([`Config::rule_wire`]), whose empty string is "no rule"; the decode never
//! errors — a corrupt rule loses the rule and keeps the enabled bit, which is
//! the fact that drives enforcement.

use redb::{ReadableTable, TableDefinition};

use crate::{
    _core::object_lock::Config,
    error::Error,
    table::{self, TableDef},
};

/// The per-table marker: the table definition for the shared handle arms.
#[doc(hidden)]
pub enum Def {}

impl TableDef for Def {
    type Key = &'static str;
    type Value = (bool, &'static str);

    const DEF: TableDefinition<'static, Self::Key, Self::Value> =
        TableDefinition::new("bucket_object_lock");
    /// A root written before the Object Lock feature has no
    /// `bucket_object_lock` table, and an absent row already means "no
    /// configuration": the missing table reads as empty
    /// ([`TableDef::MAY_BE_ABSENT`], OL4).
    const MAY_BE_ABSENT: bool = true;
}

/// Handle to the bucket Object Lock table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// The bucket's Object Lock configuration, or `None` when it stores none.
    /// A stored rule that does not decode is no rule, while the enabled bit
    /// survives it ([`Config::from_rule_wire`], OL4).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use redb::{Database, ReadableDatabase, backends::InMemoryBackend};
    /// use tinio_store_redb::{bucket_object_lock, ensure_all};
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
    /// let table = bucket_object_lock::Table::open_readonly(&txn).unwrap();
    /// assert_eq!(table.get("data").unwrap(), None);
    /// ```
    pub fn get(&self, bucket: &str) -> Result<Option<Config>, Error> {
        Ok(self.0.get(bucket)?.map(|guard| {
            let (enabled, wire) = guard.value();
            Config::from_rule_wire(enabled, wire)
        }))
    }

    /// Whether the bucket's stored configuration is enabled. The read that
    /// needs only the enforcement bit does not decode the rule; an absent
    /// configuration is not enabled.
    pub fn is_enabled(&self, bucket: &str) -> Result<bool, Error> {
        Ok(self.0.get(bucket)?.is_some_and(|guard| guard.value().0))
    }

    /// Visit every stored configuration row: the bucket, the enabled bit and
    /// the **raw** rule wire.
    ///
    /// The raw wire, not the decoded rule: a caller that reports or acts on a
    /// stored row the decoder would heal (the doctor's report, the fs startup
    /// repair) must see what is actually stored, and the enabled bit is the
    /// fact either of them needs without decoding anything (OL4, OL31).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use redb::{Database, ReadableDatabase, backends::InMemoryBackend};
    /// use tinio_store_redb::{_core::object_lock::Config, bucket_object_lock, ensure_all};
    ///
    /// let db = Database::builder()
    ///     .create_with_backend(InMemoryBackend::new())
    ///     .unwrap();
    /// {
    ///     let mut txn = db.begin_write().unwrap();
    ///     ensure_all(&mut txn).unwrap();
    ///     bucket_object_lock::Table::open(&mut txn)
    ///         .unwrap()
    ///         .set(
    ///             "data",
    ///             &Config {
    ///                 enabled: true,
    ///                 rule: None,
    ///             },
    ///         )
    ///         .unwrap();
    ///     txn.commit().unwrap();
    /// }
    /// let txn = db.begin_read().unwrap();
    /// let table = bucket_object_lock::Table::open_readonly(&txn).unwrap();
    /// let mut rows = Vec::new();
    /// table
    ///     .for_each(|bucket, enabled, rule| {
    ///         rows.push((bucket.to_owned(), enabled, rule.to_owned()));
    ///         Ok(())
    ///     })
    ///     .unwrap();
    /// assert_eq!(rows, [("data".to_owned(), true, String::new())]);
    /// ```
    pub fn for_each<F>(&self, mut visit: F) -> Result<(), Error>
    where
        F: FnMut(&str, bool, &str) -> Result<(), Error>,
    {
        for item in self.0.iter()? {
            let (key, value) = item?;
            let (enabled, rule) = value.value();
            visit(key.value(), enabled, rule)?;
        }
        Ok(())
    }
}

impl<'txn> table::Table<'txn, Def> {
    /// Record the bucket's Object Lock configuration (upsert). The caller owns
    /// presence: a missing bucket row is not this table's decision.
    pub fn set(&mut self, bucket: &str, config: &Config) -> Result<(), Error> {
        let wire = config.rule_wire();
        self.0.insert(bucket, (config.enabled, wire.as_str()))?;
        Ok(())
    }

    /// Remove the bucket's configuration row (bucket teardown, and the clear).
    /// Idempotent: the table holds one row per bucket.
    pub fn drain_bucket(&mut self, bucket: &str) -> Result<(), Error> {
        self.0.remove(bucket)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        _core::object_lock::{DefaultRetention, Mode, Period, Rule},
        testutil::handle,
    };

    fn config(period: Option<Period>) -> Config {
        Config {
            enabled: true,
            rule: period.map(|period| Rule {
                default_retention: DefaultRetention {
                    mode: Mode::Compliance,
                    period,
                },
            }),
        }
    }

    #[test]
    fn the_configuration_round_trips_the_enabled_bit_and_the_rule() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            assert_eq!(t.get("data")?, None, "an unset bucket has no row");
            assert!(!t.is_enabled("data")?);

            t.set("data", &config(Some(Period::Days(30))))?;
            assert_eq!(t.get("data")?, Some(config(Some(Period::Days(30)))));
            assert!(t.is_enabled("data")?);

            // The rule-less configuration is a row too: enabled, no default.
            t.set("data", &config(None))?;
            assert_eq!(t.get("data")?, Some(config(None)));
            assert!(t.is_enabled("data")?);
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            assert_eq!(
                Table::open_readonly(txn)?.get("data")?,
                Some(config(None)),
                "the last write is what the row carries"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn a_corrupt_rule_loses_the_rule_and_keeps_the_enabled_bit() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?
                .0
                .insert("data", (true, "not a rule"))
                .map_err(Error::from)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let table = Table::open_readonly(txn)?;
            assert_eq!(
                table.get("data")?,
                Some(Config {
                    enabled: true,
                    rule: None
                }),
                "the enabled bit survives a damaged rule"
            );
            assert!(
                table.is_enabled("data")?,
                "enforcement does not depend on a decodable rule"
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
            t.set("b", &config(None))?;
            t.set("other", &config(Some(Period::Days(1))))?;
            t.drain_bucket("b")?;
            t.drain_bucket("b")?; // idempotent
            assert_eq!(t.get("b")?, None);
            assert_eq!(t.get("other")?, Some(config(Some(Period::Days(1)))));
            Ok(())
        })
        .unwrap();
    }
}
