//! `(bucket, upload_id)` → `(key, initiated-at unix nanos, tags wire,
//! owner wire, acl wire)` — the tags, owner and ACL elements are the
//! create-time object values (spec 2026-08-31; owner/ACL spec
//! 2026-09-05, applied to the completed object; empty when none).

use std::time::SystemTime;

use redb::{ReadableTable, TableDefinition};

use crate::{
    _core::{object, to_nanos},
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
    object::Tags::from_wire_limited(wire, object::OBJECT_TAGS_MAX)
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
