//! `(bucket, key)` → `(etag hex, size, mtime unix nanos, file identity,
//! tags wire, checksum wire, owner wire, acl wire)` — the tags, checksum,
//! owner and ACL elements are empty strings when the object has none
//! (spec 2026-08-31; owner/ACL spec 2026-09-05). The checksum wire is
//! `<algorithm wire>:<base64 value>:<kind>` — e.g.
//! `CRC32:NhCmhg==:FULL_OBJECT` — with the kind recorded at write time so
//! read paths never derive it.

use redb::{ReadableTable, TableDefinition};

use crate::{
    _core::{
        acl::{Acl, OwnerId},
        checksum,
        etag::ETag,
        object,
        object::OBJECT_TAGS_MAX,
    },
    acl::{decode_acl_wire, decode_owner_wire},
    error::Error,
    scan::{drain_pair, for_each_pair},
    table::{self, TableDef},
};

/// The per-table marker: the table definition for the shared handle arms.
#[doc(hidden)]
pub enum Def {}

impl TableDef for Def {
    type Key = (&'static str, &'static str);
    type Value = (
        &'static str,
        u64,
        u64,
        u64,
        &'static str,
        &'static str,
        &'static str,
        &'static str,
    );

    const DEF: TableDefinition<'static, Self::Key, Self::Value> =
        TableDefinition::new("object_meta");
}

/// Handle to the object-meta table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

/// One stored `OBJECT_META` entry, validated into domain types (the row
/// shape is `(etag hex, size, mtime unix nanos, file identity, tags wire,
/// checksum wire, owner wire, acl wire)`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    /// ETag (single MD5 or composed `-N` form).
    pub etag: ETag,
    /// Object size in bytes at record time.
    pub size: u64,
    /// Object mtime in unix nanoseconds at record time.
    pub mtime: u64,
    /// File identity at record time (`0` marks an unavailable platform
    /// identity).
    pub file_identity: u64,
    /// The object's tag set (empty when none, or when the stored wire is
    /// domain-invalid — self-healing like the etag).
    pub tags: object::Tags,
    /// The recorded object checksum (`None` when none, or when the stored
    /// element is domain-invalid — self-healing like the etag).
    pub checksum: Option<checksum::Recorded>,
    /// The object's owner canonical ID (`None` when none recorded, or
    /// when the stored wire is domain-invalid — self-healing like the
    /// tags; the auth layer resolves the default owner for such rows).
    pub owner: Option<OwnerId>,
    /// The object's access-control list — the owner's own element rides
    /// [`Self::owner`]; the grants wire decodes with
    /// `from_grants_wire`'s self-heal (empty/domain-invalid → the
    /// private default with no owner).
    pub acl: Acl,
}

/// Validate one raw `OBJECT_META` row into [`Stored`] — `None` on a
/// domain-invalid etag (self-healing: the caller treats it as missing
/// and recomputes). The tags, checksum, owner and ACL elements
/// self-heal to empty/`None`/the private default on a domain-invalid
/// wire — the row itself is still served (its etag is valid), exactly
/// like the read paths treat a garbage checksum spec. Shared by the
/// point read [`Table::get`] and the gating traversal
/// [`Table::for_bucket_gated`] — the single home of the rule.
pub fn validate(
    (etag, size, mtime, file_identity, tags, checksum, owner_wire, acl_wire): (
        &str,
        u64,
        u64,
        u64,
        &str,
        &str,
        &str,
        &str,
    ),
) -> Option<Stored> {
    Some(Stored {
        etag: ETag::new(etag).ok()?,
        size,
        mtime,
        file_identity,
        tags: object::Tags::from_wire_limited(tags, OBJECT_TAGS_MAX),
        checksum: checksum::Recorded::from_wire_opt(checksum),
        owner: decode_owner_wire(owner_wire),
        acl: decode_acl_wire(acl_wire),
    })
}

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// One stored entry, if present and domain-valid (`None` on a corrupt
    /// etag — self-healing; the caller recomputes).
    pub fn get(&self, bucket: &str, key: &str) -> Result<Option<Stored>, Error> {
        let Some(guard) = self.0.get((bucket, key))? else {
            return Ok(None);
        };
        Ok(validate(guard.value()))
    }

    /// Visit every row of `bucket` (contiguous from `(bucket, "")`) with
    /// per-row [`Self::get`] semantics — the gating-load traversal
    /// (pipeline-spec.md P2, R1): a domain-invalid key skips the row, a
    /// domain-invalid etag reports `stored: None` (treated as missing —
    /// the caller recomputes and rewrites, self-healing). A corrupt row
    /// never fails the walk.
    pub fn for_bucket_gated<F>(&self, bucket: &str, mut visit: F) -> Result<(), Error>
    where
        F: FnMut(object::Key, Option<Stored>) -> Result<(), Error>,
    {
        for_each_pair(
            &self.0,
            (bucket, ""),
            |b, _| b == bucket,
            |_, raw_key, value| {
                let Ok(key) = object::key(raw_key) else {
                    return Ok(()); // invalid key domain → skip the row
                };
                // Same row validation as the point read and the gate
                // (invalid etag → None — self-healing).
                let stored = validate(value);
                visit(key, stored)
            },
        )
    }
}

impl<'txn> table::Table<'txn, Def> {
    /// Upsert one row — the key plus the [`Stored`] payload (one
    /// struct per row; the wire elements are encoded here, the one
    /// encode home). The interface-validated `tags`, the recorded
    /// checksum and the owner/ACL ride the same row (write-path
    /// atomicity: persisted with the etag, never a post-commit window),
    /// and `checksum` is stored with its recorded kind (`FULL_OBJECT`
    /// for plain PUTs, `COMPOSITE` for multipart completions, the
    /// source's kind for copies).
    pub fn put(&mut self, bucket: &str, key: &str, meta: &Stored) -> Result<(), Error> {
        let etag_hex = meta.etag.as_str();
        let tags_wire = meta.tags.to_wire();
        let checksum_wire = meta
            .checksum
            .as_ref()
            .map(|c| c.to_wire())
            .unwrap_or_default();
        let owner_wire = meta.owner.as_ref().map(|o| o.as_str()).unwrap_or_default();
        let acl_wire = meta.acl.to_grants_wire();
        self.0.insert(
            (bucket, key),
            (
                etag_hex.as_str(),
                meta.size,
                meta.mtime,
                meta.file_identity,
                tags_wire.as_str(),
                checksum_wire.as_str(),
                owner_wire,
                acl_wire.as_str(),
            ),
        )?;
        Ok(())
    }

    /// Replace `key`'s tags element, preserving the row's other
    /// elements. `Ok(None)` = no row; `Ok(Some(false))` = identical set
    /// (no write); `Ok(Some(true))` = rewritten.
    pub fn put_tags(
        &mut self,
        bucket: &str,
        key: &str,
        tags: &object::Tags,
    ) -> Result<Option<bool>, Error> {
        self.rewrite_tags(bucket, key, tags)
    }

    /// Clear `key`'s tags element (idempotent). Same presence/change
    /// outcome as [`Self::put_tags`].
    pub fn clear_tags(&mut self, bucket: &str, key: &str) -> Result<Option<bool>, Error> {
        self.rewrite_tags(bucket, key, &object::Tags::empty())
    }

    /// The object-row tags rewrite: fetch (self-healing), compare the
    /// sets, and re-put the whole row with the new element — the other
    /// elements ride untouched, and nothing is created for a missing row.
    fn rewrite_tags(
        &mut self,
        bucket: &str,
        key: &str,
        tags: &object::Tags,
    ) -> Result<Option<bool>, Error> {
        let Some(mut stored) = self.get(bucket, key)? else {
            return Ok(None);
        };
        if stored.tags == *tags {
            return Ok(Some(false));
        }
        stored.tags = tags.clone();
        self.put(bucket, key, &stored)?;
        Ok(Some(true))
    }

    /// Remove the entry for `key` (idempotent).
    pub fn remove(&mut self, bucket: &str, key: &str) -> Result<(), Error> {
        self.0.remove((bucket, key))?;
        Ok(())
    }

    /// Delete every row of `bucket` (entries are contiguous from
    /// `(bucket, "")` — mismatch break, see `crate::scan`).
    pub fn drain_bucket(&mut self, bucket: &str) -> Result<(), Error> {
        drain_pair(&mut self.0, (bucket, ""), |b, _| b == bucket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        _core::object::Tags,
        testutil::{etag, handle},
    };

    #[test]
    fn object_meta_walk_self_heals_a_corrupt_etag_row() {
        let h = handle();
        let valid = Stored {
            etag: etag("d41d8cd98f00b204e9800998ecf8427e"),
            size: 1,
            mtime: 0,
            file_identity: 0,
            tags: Tags::empty(),
            checksum: None,
            owner: None,
            acl: Acl::default_private(None),
        };
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            t.put("data", &object::key("ok.txt").unwrap(), &valid)?;
            // A corrupt-etag row (written by a stale writer) self-heals on
            // the walk rather than failing it — the gating load's discipline.
            t.insert(
                ("data", "bad-etag"),
                ("not-an-etag", 1u64, 0u64, 0u64, "", "", "", ""),
            )
            .map_err(Error::from)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let t = Table::open_readonly(txn)?;
            let mut saw_valid = false;
            let mut saw_corrupt = false;
            t.for_bucket_gated("data", |key, stored| {
                if &*key == "ok.txt" {
                    saw_valid = true;
                    assert!(stored.is_some());
                } else if &*key == "bad-etag" {
                    saw_corrupt = true;
                    assert!(stored.is_none(), "corrupt etag row self-heals");
                }
                Ok(())
            })?;
            assert!(saw_valid && saw_corrupt);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn object_meta_tag_accessors_rewrite_only_the_element() {
        let h = handle();
        let key = object::key("a.txt").unwrap();
        let tags = Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
        let stored = Stored {
            etag: etag("d41d8cd98f00b204e9800998ecf8427e"),
            size: 3,
            mtime: 1,
            file_identity: 2,
            tags: Tags::empty(),
            checksum: None,
            owner: None,
            acl: Acl::default_private(None),
        };
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            // Missing row: no presence outcome, nothing created.
            assert!(t.put_tags("data", &key, &tags)?.is_none());
            assert!(t.clear_tags("data", &key)?.is_none());
            t.put("data", &key, &stored)?;
            // Set rewrites the tags element, the row's other elements ride.
            assert_eq!(t.put_tags("data", &key, &tags)?, Some(true));
            let got = t.get("data", &key)?.unwrap();
            assert_eq!(got.tags, tags);
            assert_eq!(got.size, 3);
            assert_eq!(got.etag, stored.etag);
            // Identical set: no write.
            assert_eq!(t.put_tags("data", &key, &tags)?, Some(false));
            // Clear: true once, then the already-empty no-write.
            assert_eq!(t.clear_tags("data", &key)?, Some(true));
            assert_eq!(t.clear_tags("data", &key)?, Some(false));
            assert!(t.get("data", &key)?.unwrap().tags.is_empty());
            Ok(())
        })
        .unwrap();
        // A corrupt tags wire self-heals on the read AND the put heals the
        // row back to a clean wire (the row's etag stays valid throughout).
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            t.insert(
                ("data", "a.txt"),
                (
                    "d41d8cd98f00b204e9800998ecf8427e",
                    3u64,
                    1u64,
                    2u64,
                    "garbage",
                    "",
                    "",
                    "",
                ),
            )
            .map_err(Error::from)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            assert!(
                Table::open_readonly(txn)?
                    .get("data", &key)?
                    .unwrap()
                    .tags
                    .is_empty()
            );
            Ok(())
        })
        .unwrap();
        h.write(|txn| -> Result<(), Error> {
            assert_eq!(Table::open(txn)?.put_tags("data", &key, &tags)?, Some(true));
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let got = Table::open_readonly(txn)?.get("data", &key)?.unwrap();
            assert_eq!(got.tags, tags, "the rewrite normalized the corrupt wire");
            assert_eq!(got.size, 3, "the other elements survived the heal");
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn object_meta_missing_get_absent_remove_and_drain_bucket() {
        let h = handle();
        let key = object::key("dir/a.txt").unwrap();
        let stored = Stored {
            etag: etag("d41d8cd98f00b204e9800998ecf8427e"),
            size: 11,
            mtime: 0,
            file_identity: 7,
            tags: Tags::empty(),
            checksum: None,
            owner: None,
            acl: Acl::default_private(None),
        };
        // The missing-row contract: an absent bucket or key answers None,
        // and removing a row that does not exist is a no-op.
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            assert!(t.get("nope", &key)?.is_none());
            assert!(t.get("data", &object::key("nope.txt").unwrap())?.is_none());
            t.remove("nope", &key)?;
            t.remove("data", &object::key("nope.txt").unwrap())?;
            Ok(())
        })
        .unwrap();
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?.put("data", &key, &stored)?;
            Ok(())
        })
        .unwrap();
        // Remove deletes the row, and re-removing it (now absent) still
        // answers Ok — idempotent.
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            t.remove("data", &key)?;
            assert!(t.get("data", &key)?.is_none());
            t.remove("data", &key)?;
            assert!(t.get("data", &key)?.is_none());
            Ok(())
        })
        .unwrap();
        // Drain removes every row of the bucket (its own txn: drain and
        // insert in one txn trips a redb page-manager assertion).
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?.put("data", &key, &stored)?;
            Ok(())
        })
        .unwrap();
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            t.drain_bucket("data")?;
            assert!(t.get("data", &key)?.is_none());
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            assert!(Table::open_readonly(txn)?.get("data", &key)?.is_none());
            Ok(())
        })
        .unwrap();
    }
}
