//! `(bucket, key)` → `(etag hex, size, mtime unix nanos, file identity,
//! tags wire, checksum wire, owner wire, acl wire, version id wire,
//! version sequence, delete-marker flag)` — the tags, checksum, owner and
//! ACL elements are
//! empty strings when the object has none (spec 2026-08-31; owner/ACL spec
//! 2026-09-05). The checksum wire is
//! `<algorithm wire>:<base64 value>:<kind>` — e.g.
//! `CRC32:NhCmhg==:FULL_OBJECT` — with the kind recorded at write time so
//! read paths never derive it. The two version elements are the current
//! version's identity (spec 2026-10-03-s3-versioning): the version id's
//! wire form — the literal `null` for the null version, never an empty
//! string — and its per-key sequence (`0` = none, i.e. the bucket was
//! unversioned when the object was written). The last element is the
//! **recorded** delete-marker state: the simple-DELETE path writes `true`
//! and every object write clears it. The flag is recorded, never inferred
//! from the row's content shape, because that shape cannot tell a delete
//! marker's row from a genuinely zero-byte object's row on a filesystem
//! where the file identity is unavailable (both are size `0`, the
//! empty-content ETag and identity `0`), and a marker is a state the API
//! created rather than one the reader may guess. The row format change is
//! additive at the table level but not at the row level: `FORMAT_VERSION`
//! stays `1` (the 2026-09-02 ruling), so a database written in an older
//! tuple shape errors at decode and the remedy is delete-and-rebuild.

use redb::{ReadableTable, TableDefinition};

use crate::{
    _core::{
        acl::{Acl, OwnerId},
        checksum,
        etag::ETag,
        object,
        object::OBJECT_TAGS_MAX,
        version,
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
        &'static str, // etag hex
        u64,          // size
        u64,          // mtime unix nanos
        u64,          // file identity
        &'static str, // tags wire
        &'static str, // checksum wire
        &'static str, // owner wire
        &'static str, // acl wire
        &'static str, // version id wire (the literal "null" = null version)
        u64,          // version sequence (0 = none)
        bool,         // delete-marker flag (true = the row describes a marker)
    );

    const DEF: TableDefinition<'static, Self::Key, Self::Value> =
        TableDefinition::new("object_meta");
}

/// Handle to the object-meta table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

/// One stored `OBJECT_META` entry, validated into domain types (the row
/// shape is `(etag hex, size, mtime unix nanos, file identity, tags wire,
/// checksum wire, owner wire, acl wire, version id wire, version
/// sequence, delete-marker flag)`).
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
    /// The current version's identifier (`None` = the null version, or a
    /// row written while the bucket was unversioned — the stored element
    /// is the literal `null`; a corrupt element also reads as the null
    /// version, self-healing like the tags).
    pub version_id: Option<version::Id>,
    /// The current version's sequence (`0` = none — the bucket was
    /// unversioned when the object was written).
    pub version_seq: u64,
    /// Whether the row describes a current delete marker: written by the
    /// simple-DELETE path, cleared by every object write. A **recorded**
    /// state, never inferred from the row's content shape — see the module
    /// doc.
    pub is_delete_marker: bool,
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
    (
        etag,
        size,
        mtime,
        file_identity,
        tags,
        checksum,
        owner_wire,
        acl_wire,
        version_id,
        version_seq,
        is_delete_marker,
    ): (&str, u64, u64, u64, &str, &str, &str, &str, &str, u64, bool),
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
        version_id: version::Id::from_wire(version_id),
        version_seq,
        is_delete_marker,
    })
}

/// The five metadata wires of one row — the recorded ETag, the tags wire,
/// the checksum wire, the owner wire and the ACL grants wire — in the order
/// [`Table::put`] encodes them and the version row's metadata elements take
/// them (the supersede's `carried` elements). `None` — a version superseded
/// with no `object_meta` row — contributes five empty elements. The single
/// home of the extraction: the module's own encoder and both backends'
/// supersedes call it.
pub fn wires(stored: Option<&Stored>) -> (String, String, String, String, String) {
    match stored {
        Some(stored) => (
            stored.etag.as_str(),
            stored.tags.to_wire(),
            stored
                .checksum
                .as_ref()
                .map(checksum::Recorded::to_wire)
                .unwrap_or_default(),
            stored
                .owner
                .as_ref()
                .map(|owner| owner.as_str().to_owned())
                .unwrap_or_default(),
            stored.acl.to_grants_wire(),
        ),
        None => Default::default(),
    }
}

/// The object-file orphan classification of one recorded delete-marker flag
/// — the single home of the rule. A marker's row describes a file that is
/// absent **by design**: `is_absent` is its steady state, not corruption,
/// so the object-file orphan passes (the scanner's orphan reclamation and
/// the doctor's meta-orphan repair) must never reclaim it — its identity,
/// sequence and index entry survive every pass. A row that is not a marker
/// may have lost its file and remains an orphan candidate.
pub const fn orphan_candidate(is_delete_marker: bool) -> bool {
    !is_delete_marker
}

impl Stored {
    /// This row's [`orphan_candidate`] classification.
    pub const fn is_orphan_candidate(&self) -> bool {
        orphan_candidate(self.is_delete_marker)
    }
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
    /// source's kind for copies). The current version's identity rides
    /// the same row: `version_id`'s wire form — the literal `null` for
    /// [`Stored::version_id`]` == None` — and its sequence.
    pub fn put(&mut self, bucket: &str, key: &str, meta: &Stored) -> Result<(), Error> {
        let (etag_hex, tags_wire, checksum_wire, owner_wire, acl_wire) = wires(Some(meta));
        let version_wire = version::wire_of(meta.version_id.as_ref());
        self.0.insert(
            (bucket, key),
            (
                etag_hex.as_str(),
                meta.size,
                meta.mtime,
                meta.file_identity,
                tags_wire.as_str(),
                checksum_wire.as_str(),
                owner_wire.as_str(),
                acl_wire.as_str(),
                version_wire.as_str(),
                meta.version_seq,
                meta.is_delete_marker,
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
            version_id: None,
            version_seq: 0,
            is_delete_marker: false,
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
                (
                    "not-an-etag",
                    1u64,
                    0u64,
                    0u64,
                    "",
                    "",
                    "",
                    "",
                    "null",
                    0u64,
                    false,
                ),
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
            version_id: None,
            version_seq: 0,
            is_delete_marker: false,
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
                    "null",
                    0u64,
                    false,
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
    fn the_current_identity_elements_round_trip_and_null_encodes_as_the_literal() {
        let h = handle();
        let key = object::key("a.txt").unwrap();
        let base = Stored {
            version_id: None,
            version_seq: 0,
            is_delete_marker: false,
            etag: etag("d41d8cd98f00b204e9800998ecf8427e"),
            size: 4,
            mtime: 7,
            file_identity: 9,
            tags: Tags::empty(),
            checksum: None,
            owner: None,
            acl: Acl::default_private(None),
        };
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?.put("data", &key, &base)?;
            Ok(())
        })
        .unwrap();
        // The raw row: element 8 is the version id wire, element 9 the
        // sequence. `None` is the literal `null`, never an empty string, and
        // `0` is the no-sequence marker.
        h.read(|txn| -> Result<(), Error> {
            let t = Table::open_readonly(txn)?;
            let (id_wire, seq) = {
                let guard = t.0.get(("data", "a.txt")).unwrap().expect("present");
                let (.., id_wire, seq, _marker) = guard.value();
                (id_wire.to_owned(), seq)
            };
            assert_eq!(id_wire, "null", "the null version's wire is the literal");
            assert_ne!(id_wire, "", "…never the empty-string sentinel");
            assert_eq!(seq, 0, "zero is the no-sequence marker");
            let got = t.get("data", &key)?.expect("present");
            assert_eq!(got, base, "the null identity round-trips");
            Ok(())
        })
        .unwrap();
        // A real identity round-trips identically.
        let id = version::Id::new("v-1").unwrap();
        let with_id = Stored {
            version_id: Some(id.clone()),
            version_seq: 7,
            ..base.clone()
        };
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?.put("data", &key, &with_id)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let t = Table::open_readonly(txn)?;
            let (id_wire, seq) = {
                let guard = t.0.get(("data", "a.txt")).unwrap().expect("present");
                let (.., id_wire, seq, _marker) = guard.value();
                (id_wire.to_owned(), seq)
            };
            assert_eq!(
                id_wire,
                id.as_str(),
                "the id's own wire form is the element"
            );
            assert_eq!(seq, 7, "the sequence element is the row's own");
            let got = t.get("data", &key)?.expect("present");
            assert_eq!(got, with_id, "a real identity round-trips");
            assert_eq!(got.version_id.as_ref(), Some(&id));
            Ok(())
        })
        .unwrap();
        // A corrupt element self-heals to the null version: a row that
        // cannot say its identity never invents one.
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?
                .insert(
                    ("data", "a.txt"),
                    (
                        "d41d8cd98f00b204e9800998ecf8427e",
                        4u64,
                        7u64,
                        9u64,
                        "",
                        "",
                        "",
                        "",
                        "",
                        0u64,
                        false,
                    ),
                )
                .map_err(Error::from)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let got = Table::open_readonly(txn)?
                .get("data", &key)?
                .expect("the valid etag still serves");
            assert_eq!(
                got.version_id, None,
                "an empty id wire reads as the null version, not an error"
            );
            assert_eq!(got.version_seq, 0, "the sequence heals with the identity");
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn object_meta_missing_get_absent_remove_and_drain_bucket() {
        let h = handle();
        let key = object::key("dir/a.txt").unwrap();
        let stored = Stored {
            version_id: None,
            version_seq: 0,
            is_delete_marker: false,
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
