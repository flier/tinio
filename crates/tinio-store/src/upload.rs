//! `(bucket, upload_id)` → `(key, initiated-at unix nanos, tags wire,
//! owner wire, acl wire)` — the tags, owner and ACL elements are the
//! create-time object values (spec 2026-08-31; owner/ACL spec
//! 2026-09-05, applied to the completed object; empty when none).

use std::time::SystemTime;

use redb::{ReadableTable, TableDefinition};

use crate::{
    _core::{object, object::OBJECT_TAGS_MAX, to_nanos},
    error::Error,
    scan::{drain_pair, for_each_pair, has_prefix_pair},
    table::{self, TableDef},
};

/// The per-table marker: the table definition for the shared handle arms.
#[doc(hidden)]
pub enum Def {}

impl TableDef for Def {
    type Key = (&'static str, &'static str);
    type Value = (&'static str, u64, &'static str, &'static str, &'static str);

    const DEF: TableDefinition<'static, Self::Key, Self::Value> = TableDefinition::new("uploads");
}

/// Handle to the uploads table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

/// Self-healing decode of the stored tags wire: empty or corrupt → the
/// empty set (cap [`object::OBJECT_TAGS_MAX`]). The encode half is
/// [`object::Tags::to_wire`]; both ride the point reads and the scan
/// visitors.
fn decode_tags_wire(wire: &str) -> object::Tags {
    object::Tags::from_wire_limited(wire, OBJECT_TAGS_MAX)
}

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// Whether `bucket` has any upload row. Entries of one bucket are
    /// contiguous from `(bucket, "")` — the first key at or after that
    /// lower bound is in the bucket iff any exist (see [`Self::drain_bucket`]).
    pub fn has_bucket(&self, bucket: &str) -> Result<bool, Error> {
        has_prefix_pair(&self.0, (bucket, ""), |b, _| b == bucket)
    }

    /// Whether the upload exists and records `key` (S3 identity is
    /// `(bucket, key, uploadId)`).
    pub fn key_matches(&self, bucket: &str, key: &str, upload_id: &str) -> Result<bool, Error> {
        Ok(self
            .0
            .get((bucket, upload_id))?
            .map(|guard| guard.value().0 == key)
            .unwrap_or(false))
    }

    /// The stored row, present only when the upload exists AND records
    /// `key` (S3 identity is `(bucket, key, uploadId)`) — the
    /// `key_matches` + `get` pair of `get_upload` in one lookup. Returns
    /// `(key, initiated-at, tags wire, owner wire, acl wire)` (owned —
    /// the guard cannot outlive the closure).
    #[allow(clippy::type_complexity)]
    pub fn get_matching(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Option<(String, u64, String, String, String)>, Error> {
        Ok(self
            .0
            .get((bucket, upload_id))?
            .map(|guard| {
                (
                    guard.value().0.to_string(),
                    guard.value().1,
                    guard.value().2.to_string(),
                    guard.value().3.to_string(),
                    guard.value().4.to_string(),
                )
            })
            .filter(|(stored_key, ..)| stored_key == key))
    }

    /// The create-time tags of the upload, present only when the upload
    /// exists AND records `key` — the identity check of
    /// [`Self::get_matching`] with the rest of the row dropped (the
    /// completion paths read the tags element alone).
    pub fn tags(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Option<object::Tags>, Error> {
        let Some(guard) = self.0.get((bucket, upload_id))? else {
            return Ok(None);
        };
        if guard.value().0 != key {
            return Ok(None);
        }
        Ok(Some(decode_tags_wire(guard.value().2)))
    }

    /// Visit every upload row of `bucket` (contiguous from `(bucket, "")`).
    /// Tags are decoded here (empty or corrupt wire → empty set, the
    /// self-heal of [`Self::get_matching`]).
    pub fn for_bucket<F>(&self, bucket: &str, mut visit: F) -> Result<(), Error>
    where
        F: FnMut(&str, (&str, u64, &str, &str, &str)) -> Result<(), Error>,
    {
        for_each_pair(
            &self.0,
            (bucket, ""),
            |b, _| b == bucket,
            |_, upload_id, value| visit(upload_id, value),
        )
    }

    /// Visit every upload row across all buckets. Tags are decoded here
    /// (empty or corrupt wire → empty set). The orphan-cleanup liveness
    /// scan ignores the tags and key.
    pub fn for_each<F>(&self, mut visit: F) -> Result<(), Error>
    where
        F: FnMut(&str, &str, &str, u64, &str, &str, &str) -> Result<(), Error>,
    {
        for item in self.0.iter()? {
            let (k, v) = item?;
            let (b, upload_id) = k.value();
            let (key, initiated_at, tags_wire, owner_wire, acl_wire) = v.value();
            visit(
                b,
                upload_id,
                key,
                initiated_at,
                tags_wire,
                owner_wire,
                acl_wire,
            )?;
        }
        Ok(())
    }
}

impl<'txn> table::Table<'txn, Def> {
    /// Upsert one upload row — the create-time tags, owner and ACL wires
    /// ride in the row (spec 2026-08-31; owner/ACL spec 2026-09-05).
    #[allow(clippy::too_many_arguments)]
    pub fn put(
        &mut self,
        bucket: &str,
        upload_id: &str,
        key: &str,
        initiated_at: SystemTime,
        tags_wire: &str,
        owner_wire: &str,
        acl_wire: &str,
    ) -> Result<(), Error> {
        let key_str = key.to_string();
        self.0.insert(
            (bucket, upload_id),
            (
                key_str.as_str(),
                to_nanos(initiated_at),
                tags_wire,
                owner_wire,
                acl_wire,
            ),
        )?;
        Ok(())
    }

    /// Remove one upload row (idempotent).
    pub fn remove(&mut self, bucket: &str, upload_id: &str) -> Result<(), Error> {
        self.0.remove((bucket, upload_id))?;
        Ok(())
    }

    /// Delete every upload row of `bucket`.
    pub fn drain_bucket(&mut self, bucket: &str) -> Result<(), Error> {
        drain_pair(&mut self.0, (bucket, ""), |b, _| b == bucket)
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use super::*;
    use crate::{_core::object::Tags, testutil::handle};

    #[test]
    fn upload_rows_key_match_bucket_scan_and_for_each() {
        let h = handle();
        let key = object::key("big.bin").unwrap();
        let tags = Tags::from_pairs([("k".into(), "v".into())]).unwrap();
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            assert!(!t.has_bucket("data")?);
            t.put(
                "data",
                "u1",
                &key,
                SystemTime::UNIX_EPOCH,
                &tags.to_wire(),
                "",
                "",
            )?;
            t.put("data", "u2", &key, SystemTime::UNIX_EPOCH, "", "", "")?;
            t.put("other", "u9", &key, SystemTime::UNIX_EPOCH, "", "", "")?;
            assert!(t.has_bucket("data")?);
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let t = Table::open_readonly(txn)?;
            // key_matches / get_matching bind the S3 identity (bucket,key,id).
            assert!(t.key_matches("data", &key, "u1")?);
            assert!(!t.key_matches("data", "wrong-key", "u1")?);
            assert!(!t.key_matches("data", &key, "u5")?);
            let (got_key, initiated, tags_wire, _, _) =
                t.get_matching("data", &key, "u1")?.unwrap();
            assert_eq!(got_key, &*key);
            assert_eq!(initiated, 0);
            assert_eq!(t.tags("data", &key, "u1")?.unwrap(), tags);
            assert!(!tags_wire.is_empty());
            assert!(t.get_matching("data", &key, "u5")?.is_none());
            // The bucket scan visits only this bucket's uploads, in key order.
            let mut ids = Vec::new();
            t.for_bucket("data", |id, _| {
                ids.push(id.to_string());
                Ok(())
            })?;
            assert_eq!(ids, ["u1", "u2"]);
            // The whole-table walk.
            let mut all = Vec::new();
            t.for_each(|b, id, _, _, _, _, _| {
                all.push((b.to_string(), id.to_string()));
                Ok(())
            })?;
            assert_eq!(
                all,
                vec![
                    ("data".into(), "u1".into()),
                    ("data".into(), "u2".into()),
                    ("other".into(), "u9".into()),
                ]
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn upload_tags_accessors_round_trip_and_self_heal() {
        let h = handle();
        let key = object::key("big.bin").unwrap();
        let tags = Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            t.put(
                "data",
                "u1",
                &key,
                SystemTime::UNIX_EPOCH,
                &tags.to_wire(),
                "",
                "",
            )?;
            assert_eq!(t.tags("data", &key, "u1")?.unwrap(), tags);
            t.put("data", "u1", &key, SystemTime::UNIX_EPOCH, "", "", "")?;
            assert!(t.tags("data", &key, "u1")?.unwrap().is_empty());
            Ok(())
        })
        .unwrap();
        // A garbage tags wire self-heals on the table accessor.
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?
                .insert(("data", "u1"), ("big.bin", 0u64, "garbage", "", ""))
                .map_err(Error::from)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let t = Table::open_readonly(txn)?;
            assert!(
                t.tags("data", &key, "u1")?.unwrap().is_empty(),
                "a corrupt upload tags wire is the empty set"
            );
            t.for_bucket("data", |_, (_, _, tags_wire, _, _)| {
                assert!(
                    object::Tags::from_wire_limited(tags_wire, OBJECT_TAGS_MAX).is_empty(),
                    "the bucket scan self-heals the same way"
                );
                Ok(())
            })?;
            t.for_each(|_, _, _, _, tags_wire, _, _| {
                assert!(
                    object::Tags::from_wire_limited(tags_wire, OBJECT_TAGS_MAX).is_empty(),
                    "the whole-table walk self-heals the same way"
                );
                Ok(())
            })?;
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn upload_tags_accessor_answers_the_identity_check() {
        let h = handle();
        let key = object::key("big.bin").unwrap();
        let tags = Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            // Missing row: None.
            assert!(t.tags("data", &key, "u1")?.is_none());
            t.put(
                "data",
                "u1",
                &key,
                SystemTime::UNIX_EPOCH,
                &tags.to_wire(),
                "",
                "",
            )?;
            // S3 identity is (bucket, key, uploadId): a matching key answers
            // the tags, a mismatched key or upload id answers None (the
            // caller's NoSuchUpload arm).
            assert_eq!(t.tags("data", &key, "u1")?, Some(tags.clone()));
            assert!(
                t.tags("data", &object::key("other.bin").unwrap(), "u1")?
                    .is_none()
            );
            assert!(t.tags("data", &key, "u2")?.is_none());
            Ok(())
        })
        .unwrap();
        // A corrupt tags wire still answers Some (the upload exists) with the
        // self-healed empty set.
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?
                .insert(("data", "u1"), ("big.bin", 0u64, "garbage", "", ""))
                .map_err(Error::from)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            assert_eq!(
                Table::open_readonly(txn)?.tags("data", &key, "u1")?,
                Some(Tags::empty()),
                "a corrupt upload tags wire is the empty set"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn upload_drain_bucket_removes_only_the_bucket() {
        let h = handle();
        let key = object::key("k").unwrap();
        // Insert into a write txn, then drain in a separate one (drain and
        // insert in the same txn trips a redb page-manager assertion).
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            t.put("data", "u1", &key, SystemTime::UNIX_EPOCH, "", "", "")?;
            t.put("other", "u9", &key, SystemTime::UNIX_EPOCH, "", "", "")?;
            Ok(())
        })
        .unwrap();
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            t.drain_bucket("data")?;
            assert!(!t.has_bucket("data")?);
            Ok(())
        })
        .unwrap();
        assert!(
            !h.read(|txn| Table::open_readonly(txn)?.key_matches("data", &key, "u1"))
                .unwrap()
        );
    }
}
