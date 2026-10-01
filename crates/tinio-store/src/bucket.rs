//! `name` → `(created-at unix nanos, tags wire, owner wire, acl wire,
//! cors wire)` — a wire element is `''` when the bucket has none (the
//! owner/ACL elements ride the ACL plan, the CORS element the CORS plan,
//! spec 2026-09-05).

use std::time::SystemTime;

use redb::{ReadableTable, TableDefinition};

use crate::{
    _core::{cors, from_nanos, object, to_nanos},
    error::Error,
    table::{self, TableDef},
};

/// The per-table marker: the table definition for the shared handle arms.
#[doc(hidden)]
pub enum Def {}

impl TableDef for Def {
    type Key = &'static str;
    type Value = (u64, &'static str, &'static str, &'static str, &'static str);

    const DEF: TableDefinition<'static, Self::Key, Self::Value> = TableDefinition::new("buckets");
}

/// Handle to the buckets table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

/// The stored bucket row, bundled so the wire elements are accessed by
/// name. The redb VALUE stays the pinned 5-tuple — the conversion lives
/// in `BucketRow::from_value`/`BucketRow::to_value`, the only place
/// that touches element positions (the arity/order pins in the store
/// tests guard the tuple itself).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketRow {
    pub created: SystemTime,
    pub tags: String,
    pub owner: String,
    pub acl: String,
    pub cors: String,
}

impl BucketRow {
    /// A fresh row: the creation time with no wire elements.
    pub fn at(created: SystemTime) -> Self {
        Self {
            created,
            tags: String::new(),
            owner: String::new(),
            acl: String::new(),
            cors: String::new(),
        }
    }

    fn from_value(value: (u64, &str, &str, &str, &str)) -> Self {
        let (created, tags, owner, acl, cors) = value;
        Self {
            created: from_nanos(created),
            tags: tags.to_string(),
            owner: owner.to_string(),
            acl: acl.to_string(),
            cors: cors.to_string(),
        }
    }

    fn to_value(&self) -> (u64, &str, &str, &str, &str) {
        (
            to_nanos(self.created),
            &self.tags,
            &self.owner,
            &self.acl,
            &self.cors,
        )
    }
}

/// Self-healing decode of the stored tags wire: empty or corrupt → the
/// empty set (cap [`object::BUCKET_TAGS_MAX`]). The encode half is
/// [`object::Tags::to_wire`]; both ride [`Table::tags`] /
/// [`Table::put_tags`].
fn decode_tags_wire(wire: &str) -> object::Tags {
    object::Tags::from_wire_limited(wire, object::BUCKET_TAGS_MAX)
}

/// Self-healing decode of the stored CORS wire: an empty or corrupt wire
/// is "no configuration" (`None`; the `''` wire = 404 on get), a
/// decodable wire is the config (`Some`). The G2 normalization stops
/// here — callers never filter empty rule sets themselves. The encode
/// half is [`cors::Config::to_wire`]; both ride [`Table::cors`] /
/// [`Table::put_cors`].
fn decode_cors_wire(wire: &str) -> Option<cors::Config> {
    let config = cors::Config::from_wire(wire);
    (!config.rules.is_empty()).then_some(config)
}

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// Whether `name` is recorded.
    pub fn exists(&self, name: &str) -> Result<bool, Error> {
        Ok(self.0.get(name)?.is_some())
    }

    /// Creation time of `name`, if recorded.
    pub fn get(&self, name: &str) -> Result<Option<SystemTime>, Error> {
        Ok(self.0.get(name)?.map(|guard| from_nanos(guard.value().0)))
    }

    /// The stored row of `name` (owned; the guard cannot outlive the
    /// closure).
    pub fn row(&self, name: &str) -> Result<Option<BucketRow>, Error> {
        Ok(self
            .0
            .get(name)?
            .map(|guard| BucketRow::from_value(guard.value())))
    }

    /// Visit every recorded bucket in name order.
    pub fn for_each<F>(&self, mut visit: F) -> Result<(), Error>
    where
        F: FnMut(&str, SystemTime) -> Result<(), Error>,
    {
        for item in self.0.iter()? {
            let (k, v) = item?;
            visit(k.value(), from_nanos(v.value().0))?;
        }
        Ok(())
    }

    /// The stored tag set of `name`. `None` is a missing row (the caller
    /// maps that to `NoSuchBucket` or the empty set per backend); `Some`
    /// is the decoded set (empty when the wire is `''`/corrupt —
    /// self-healing).
    pub fn tags(&self, name: &str) -> Result<Option<object::Tags>, Error> {
        Ok(self.row(name)?.map(|row| decode_tags_wire(&row.tags)))
    }

    /// The stored CORS configuration of `name`. Outer `None` is a missing
    /// row (the caller maps that to `NoSuchBucket` or "no configuration"
    /// per backend); inner `None` is the `''`/corrupt wire (self-healing).
    pub fn cors(&self, name: &str) -> Result<Option<Option<cors::Config>>, Error> {
        Ok(self.row(name)?.map(|row| decode_cors_wire(&row.cors)))
    }
}

impl<'txn> table::Table<'txn, Def> {
    /// Record (or overwrite) the creation time of `name` — the four wire
    /// elements are cleared (a fresh row has no tags/owner/ACL/CORS; the
    /// tagging ops use [`Self::put_tags`] to preserve the creation time).
    pub fn put(&mut self, name: &str, created_at: SystemTime) -> Result<(), Error> {
        self.put_full(name, &BucketRow::at(created_at))
    }

    /// Record (or overwrite) the whole row: the creation time AND the four
    /// wire elements (the element accessors' row upsert — the creation
    /// time is preserved from the stored row).
    pub fn put_full(&mut self, name: &str, row: &BucketRow) -> Result<(), Error> {
        self.0.insert(name, row.to_value())?;
        Ok(())
    }

    /// Insert `now` when absent; return the stored creation time. The
    /// stored row's wires ride the re-insert (a first-sight upsert must
    /// not clear the tag set the tagging write just recorded — mirror the
    /// `put_full`-style callers, which keep all wires).
    pub fn get_or_insert(&mut self, name: &str, now: SystemTime) -> Result<SystemTime, Error> {
        let row = match self.0.get(name)? {
            Some(guard) => BucketRow::from_value(guard.value()),
            None => BucketRow::at(now),
        };
        let created = row.created;
        self.0.insert(name, row.to_value())?;
        Ok(created)
    }

    /// Remove the entry of `name` (idempotent).
    pub fn remove(&mut self, name: &str) -> Result<(), Error> {
        self.0.remove(name)?;
        Ok(())
    }

    /// Replace the tag set of `name`. `Ok(None)` = no row; `Ok(Some(false))`
    /// = identical wire (no write); `Ok(Some(true))` = rewritten.
    pub fn put_tags(&mut self, name: &str, tags: &object::Tags) -> Result<Option<bool>, Error> {
        let wire = tags.to_wire();
        self.rewrite_element(name, |row| set_wire(&mut row.tags, wire))
    }

    /// Clear the tag set of `name` (`''` wire). Same presence/change
    /// outcome as [`Self::put_tags`].
    pub fn clear_tags(&mut self, name: &str) -> Result<Option<bool>, Error> {
        self.rewrite_element(name, |row| set_wire(&mut row.tags, String::new()))
    }

    /// The [`Self::put_tags`] row-miss arm of the fs backend: a missing
    /// row is recorded at `created_at` WITH the tag set (the lazy
    /// first-sight upsert — one write, not a `put` + re-rewrite). The
    /// mem backend keeps the tri-state: there a row-miss IS a missing
    /// bucket. `Ok(true)` = created or changed; `Ok(false)` = identical
    /// wire (no write).
    pub fn put_tags_or_create(
        &mut self,
        name: &str,
        tags: &object::Tags,
        created_at: SystemTime,
    ) -> Result<bool, Error> {
        match self.put_tags(name, tags)? {
            Some(changed) => Ok(changed),
            None => {
                self.put_full(
                    name,
                    &BucketRow {
                        tags: tags.to_wire(),
                        ..BucketRow::at(created_at)
                    },
                )?;
                Ok(true)
            }
        }
    }

    /// Replace the CORS configuration of `name` (empty rules → `''` wire
    /// by the codec). `Ok(None)` = no row; `Ok(Some(false))` = identical
    /// wire (no write); `Ok(Some(true))` = rewritten.
    pub fn put_cors(&mut self, name: &str, config: &cors::Config) -> Result<Option<bool>, Error> {
        let wire = config.to_wire();
        self.rewrite_element(name, |row| set_wire(&mut row.cors, wire))
    }

    /// Clear the CORS configuration of `name` (`''` wire). Same
    /// presence/change outcome as [`Self::put_cors`].
    pub fn clear_cors(&mut self, name: &str) -> Result<Option<bool>, Error> {
        self.rewrite_element(name, |row| set_wire(&mut row.cors, String::new()))
    }

    /// The one element rewrite (tags, CORS, and the owner/ACL elements
    /// of the ACL plan): fetch the row, run the element setter, and
    /// `put_full` only when it changed — the row's other elements ride
    /// untouched. `Ok(None)` = no row; `Ok(Some(false))` = no change
    /// (no write); `Ok(Some(true))` = rewritten.
    fn rewrite_element(
        &mut self,
        name: &str,
        mutate: impl FnOnce(&mut BucketRow) -> bool,
    ) -> Result<Option<bool>, Error> {
        let Some(mut row) = self.row(name)? else {
            return Ok(None);
        };
        if !mutate(&mut row) {
            return Ok(Some(false));
        }
        self.put_full(name, &row)?;
        Ok(Some(true))
    }
}

/// Compare-and-replace one stored wire element: `false` when equal (the
/// setter is a no-op — the rewrite skips the `put_full`), `true` after
/// replacing.
fn set_wire(field: &mut String, wire: String) -> bool {
    if *field == wire {
        return false;
    }
    *field = wire;
    true
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use redb::{Database, TableDefinition, backends::InMemoryBackend};

    use super::*;
    use crate::{_core::object::Tags, testutil::handle};

    #[test]
    fn bucket_put_get_put_full_and_get_or_insert() {
        let h = handle();
        let now = SystemTime::UNIX_EPOCH + Duration::from_nanos(42);
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            // Absent bucket -> no row.
            assert!(!t.exists("data")?);
            assert!(t.get("data")?.is_none());
            // Record: put writes (created, empty tags/owner/acl/cors).
            t.put("data", now)?;
            assert!(t.exists("data")?);
            assert_eq!(t.get("data")?, Some(now));
            let row = t.row("data")?.expect("present");
            assert_eq!(row.created, now);
            assert_eq!(row.tags, "");
            assert_eq!(row.owner, "");
            assert_eq!(row.acl, "");
            assert_eq!(row.cors, "");
            // The tagging write upserts the whole row — every wire is
            // preserved at its own slot (the same-typed `&str` wires must
            // not swap elements).
            t.put_full(
                "data",
                &BucketRow {
                    tags: "env=prod".into(),
                    owner: "owner:w".into(),
                    acl: "acl:w".into(),
                    cors: "cors:w".into(),
                    ..BucketRow::at(now)
                },
            )?;
            let row = t.row("data")?.unwrap();
            assert_eq!(row.tags, "env=prod");
            assert_eq!(row.owner, "owner:w");
            assert_eq!(row.acl, "acl:w");
            assert_eq!(row.cors, "cors:w");
            // The list/head first-sight upsert must keep the first time AND
            // the stored wires (never clear them).
            let recorded = t.get_or_insert("data", now + Duration::from_secs(1))?;
            assert_eq!(recorded, now);
            let row = t.row("data")?.unwrap();
            assert_eq!(
                (row.created, row.tags, row.owner, row.acl, row.cors),
                (
                    now,
                    "env=prod".to_string(),
                    "owner:w".to_string(),
                    "acl:w".to_string(),
                    "cors:w".to_string(),
                )
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn bucket_iterates_in_name_order_and_remove_is_idempotent() {
        let h = handle();
        let now = SystemTime::UNIX_EPOCH;
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            t.put("zeta", now)?;
            t.put("alpha", now)?;
            t.put("mid", now)?;
            let mut names = Vec::new();
            t.for_each(|name, _| {
                names.push(name.to_string());
                Ok(())
            })?;
            assert_eq!(names, ["alpha", "mid", "zeta"]);
            t.remove("mid")?;
            t.remove("mid")?; // idempotent
            Ok(())
        })
        .unwrap();
        let names = h
            .read(|txn| -> Result<Vec<String>, Error> {
                let t = Table::open_readonly(txn)?;
                let mut out = Vec::new();
                t.for_each(|n, _| {
                    out.push(n.to_string());
                    Ok(())
                })?;
                Ok(out)
            })
            .unwrap();
        assert_eq!(names, ["alpha", "zeta"]);
    }

    #[test]
    fn bucket_cors_accessors_round_trip_and_self_heal() {
        let h = handle();
        let now = SystemTime::UNIX_EPOCH;
        let cfg = cors::Config {
            rules: vec![cors::Rule {
                id: Some("one".into()),
                allowed_methods: vec!["GET".into()],
                allowed_origins: vec!["*".into()],
                allowed_headers: None,
                expose_headers: None,
                max_age_seconds: None,
            }],
        };
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            // Missing row: outer None; writes are no-ops.
            assert!(t.cors("data")?.is_none());
            assert!(t.put_cors("data", &cfg)?.is_none());
            assert!(t.clear_cors("data")?.is_none());
            t.put("data", now)?;
            // Fresh row: present, no configuration.
            assert_eq!(t.cors("data")?, Some(None));
            assert_eq!(t.put_cors("data", &cfg)?, Some(true));
            assert_eq!(t.cors("data")?, Some(Some(cfg.clone())));
            // Identical put skips the write.
            assert_eq!(t.put_cors("data", &cfg)?, Some(false));
            // Empty rules normalize to "no configuration".
            assert_eq!(t.put_cors("data", &cors::Config::default())?, Some(true));
            assert_eq!(t.cors("data")?, Some(None));
            t.put_cors("data", &cfg)?;
            assert_eq!(t.clear_cors("data")?, Some(true));
            assert_eq!(t.cors("data")?, Some(None));
            assert_eq!(t.clear_cors("data")?, Some(false));
            Ok(())
        })
        .unwrap();
        // A garbage CORS wire self-heals on the table accessor.
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?
                .insert("data", (0u64, "", "", "", "a,b,c"))
                .map_err(Error::from)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            assert_eq!(
                Table::open_readonly(txn)?.cors("data")?,
                Some(None),
                "a corrupt CORS wire is no configuration"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn bucket_tags_accessors_round_trip_and_self_heal() {
        let h = handle();
        let now = SystemTime::UNIX_EPOCH;
        let tags = Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            // Missing row: None; writes are no-ops.
            assert!(t.tags("data")?.is_none());
            assert!(t.put_tags("data", &tags)?.is_none());
            assert!(t.clear_tags("data")?.is_none());
            t.put("data", now)?;
            // Fresh row: present, empty set.
            assert_eq!(t.tags("data")?, Some(Tags::empty()));
            assert_eq!(t.put_tags("data", &tags)?, Some(true));
            assert_eq!(t.tags("data")?, Some(tags.clone()));
            // Identical put skips the write.
            assert_eq!(t.put_tags("data", &tags)?, Some(false));
            // Empty set normalizes to the '' wire.
            assert_eq!(t.put_tags("data", &Tags::empty())?, Some(true));
            assert_eq!(t.tags("data")?, Some(Tags::empty()));
            t.put_tags("data", &tags)?;
            assert_eq!(t.clear_tags("data")?, Some(true));
            assert_eq!(t.tags("data")?, Some(Tags::empty()));
            assert_eq!(t.clear_tags("data")?, Some(false));
            Ok(())
        })
        .unwrap();
        // A garbage tags wire self-heals on the table accessor.
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?
                .insert("data", (0u64, "garbage", "", "", ""))
                .map_err(Error::from)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            assert_eq!(
                Table::open_readonly(txn)?.tags("data")?,
                Some(Tags::empty()),
                "a corrupt tags wire is the empty set"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn bucket_put_tags_or_create_records_the_first_sight_row() {
        let h = handle();
        let tags = Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
        let zero = SystemTime::UNIX_EPOCH;
        let later = SystemTime::UNIX_EPOCH + Duration::from_secs(9);
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            // Row-miss: ONE upsert records the creation time AND the tags
            // (the fs first-sight policy, replacing the old put + re-rewrite).
            assert!(t.put_tags_or_create("data", &tags, zero)?);
            assert_eq!(t.get("data")?, Some(zero));
            assert_eq!(t.tags("data")?, Some(tags.clone()));
            // Existing row: tags replaced, the recorded creation time kept.
            let new_tags = Tags::from_pairs([("team".into(), "core".into())]).unwrap();
            assert!(t.put_tags_or_create("data", &new_tags, later)?);
            assert_eq!(t.get("data")?, Some(zero));
            assert_eq!(t.tags("data")?, Some(new_tags.clone()));
            // Identical set: no write.
            assert!(!t.put_tags_or_create("data", &new_tags, later)?);
            Ok(())
        })
        .unwrap();
    }

    /// The on-disk format guard (final-review F2, no-migration ruling): a
    /// `buckets` table written under a LEGACY tuple arity must NOT open under
    /// the current 5-tuple definition. redb binds the key/value type names at
    /// the `TableDefinition`, so `check_match` answers `TableTypeMismatch`
    /// at open — an old state dir fails loudly (never silently misreads a
    /// row); there is no migration, and the documented recovery is deleting
    /// the state dir.
    #[test]
    fn legacy_buckets_arity_fails_loudly_on_open() {
        // The pre-tagging row shape: (created_at_nanos, tags_wire).
        const LEGACY: TableDefinition<'static, &'static str, (u64, &'static str)> =
            TableDefinition::new("buckets");

        let db = Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .unwrap();
        {
            let txn = db.begin_write().unwrap();
            {
                let mut table = txn.open_table(LEGACY).unwrap();
                table.insert("legacy-bucket", (42u64, "tag=wire")).unwrap();
            }
            txn.commit().unwrap();
        }
        // Opening the SAME table under the CURRENT definition (the store's
        // 5-tuple) must fail at open with the table-type mismatch.
        let mut txn = db.begin_write().unwrap();
        let err = Table::open(&mut txn)
            .err()
            .expect("legacy arity must not open");
        assert!(
            matches!(
                err,
                Error::Table(redb::TableError::TableTypeMismatch { .. })
            ),
            "{err:?}"
        );
    }
}
