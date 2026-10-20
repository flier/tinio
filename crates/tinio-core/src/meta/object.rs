//! The decoded object row, the completed object's retained part rows, and the
//! two shapes [`ObjectMeta`] names without defining.
//!
//! The stored value is the eleven-element tuple the versioning set left:
//! `(etag hex, size, mtime unix nanos, file identity, tags wire, checksum
//! wire, owner wire, acl grants wire, version id wire, version sequence,
//! delete-marker flag)`. [`Row`] is the decoded form; it carries **no version
//! columns** — version identity is version-domain state and rides
//! [`super::version::Stored`] and the key's current slot (D37).
//!
//! The self-heal rule is the row's own: a domain-invalid ETag makes the whole
//! row read as absent ([`Row::from_values`] answers `None` — the reader
//! recomputes and rewrites on the next read), while every other element
//! self-heals individually inside its accessor.
//!
//! Both traversals rest on exactly that decode: a stored row whose ETag does not
//! decode is the **corrupt row** [`ObjectMeta::walk`] fails on and
//! [`ObjectMeta::walk_healing`] reports as `None` (then heals). Planting such
//! a row needs raw table access, so the executable traversal legs live in the
//! consumer that has it (D43): P1 Task 6's
//! `crates/tinio-store-redb/src/meta/object.rs` and P2 Task 2 for mem. The
//! decode halves both branch on are pinned here.

use std::time::SystemTime;

use async_trait::async_trait;

use super::{Metadata, bucket as bucket_row};
use crate::{
    ETag,
    acl::{self, Acl, OwnerId},
    bucket, checksum, multipart, object,
    storage::{self, to_nanos},
};

/// The content-derived description of one object — the four elements
/// [`super::ObjectMeta::put_preserving`] refreshes.
///
/// `file_identity` is the platform file identity at record time (`0` marks an
/// unavailable identity), never a version identifier.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// The content ETag.
    pub etag: ETag,
    /// The content length in bytes.
    pub size: u64,
    /// The content's last modification time.
    pub mtime: SystemTime,
    /// The platform file identity at record time (`0` = unavailable).
    pub file_identity: u64,
}

/// The elements a content-derived rewrite carries over from the row it
/// replaces — the answer of [`super::ObjectMeta::put_preserving`], and
/// [`Preserved::none`] when there was no previous row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Preserved {
    /// The replaced row's tag set (empty when it had none).
    pub tags: object::Tags,
    /// The replaced row's recorded checksum (`None` when it had none).
    pub checksum: Option<checksum::Recorded>,
    /// The replaced row's recorded owner element.
    pub owner: Option<OwnerId>,
    /// The replaced row's grant set.
    pub acl: Acl,
}

impl Preserved {
    /// The zero state: nothing to carry over, because the row the rewrite
    /// replaces is absent — no tags, no checksum, no owner, and the private
    /// default in place of its grant set. The one home of the "no previous
    /// row" value every backend's preserving write answers.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_core::{acl::Acl, meta::object::Preserved, object::Tags};
    ///
    /// let preserved = Preserved::none();
    /// assert_eq!(preserved.tags, Tags::empty());
    /// assert_eq!(preserved.checksum, None);
    /// assert_eq!(preserved.owner, None);
    /// assert_eq!(preserved.acl, Acl::default_private(None));
    /// ```
    pub fn none() -> Self {
        Self {
            tags: object::Tags::empty(),
            checksum: None,
            owner: None,
            acl: Acl::default_private(None),
        }
    }
}

/// A decoded object row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// The content ETag (single MD5 or the composed `-N` form).
    pub etag: ETag,
    /// The content length in bytes at record time.
    pub size: u64,
    /// The content's last modification time at record time.
    pub mtime: SystemTime,
    /// The platform file identity at record time (`0` = unavailable).
    pub identity: u64,
    /// The object's tag set (empty when none).
    pub tags: object::Tags,
    /// The recorded object checksum (`None` when none).
    pub checksum: Option<checksum::Recorded>,
    /// The recorded owner element (`None` = the empty owner wire).
    pub owner: Option<OwnerId>,
    /// The object's grant set.
    pub acl: Acl,
}

impl Row {
    /// Decode one stored value tuple from the contract's own values.
    ///
    /// `None` is a domain-invalid ETag — the whole row reads as absent, the
    /// point read's self-heal. A domain-invalid checksum element self-heals to
    /// `None` and the row is still served; the tags and the ACL arrive as the
    /// contract values, already healed by the backend's own jsonb and
    /// composite decoders, and the row splits the ACL ([`Acl::into_parts`])
    /// exactly as its wire decode does.
    pub fn from_values(
        etag: &str,
        size: u64,
        mtime: SystemTime,
        identity: u64,
        tags: object::Tags,
        checksum: &str,
        acl: Acl,
    ) -> Option<Self> {
        let (owner, grants) = acl.into_parts();
        Some(Self {
            etag: etag.parse::<ETag>().ok()?,
            size,
            mtime,
            identity,
            tags,
            checksum: checksum::Recorded::from_wire_opt(checksum),
            owner,
            acl: Acl::from_parts(None, grants),
        })
    }

    /// Encode the row back into its value tuple's elements.
    pub fn to_raw(&self) -> Wires {
        Wires {
            etag: self.etag.as_str().to_owned(),
            size: self.size,
            mtime: to_nanos(self.mtime),
            identity: self.identity,
            tags: self.tags.to_wire(),
            checksum: self
                .checksum
                .as_ref()
                .map(checksum::Recorded::to_wire)
                .unwrap_or_default(),
            owner: bucket_row::encode_owner(self.owner.as_ref()),
            grants: bucket_row::encode_grants(&self.acl),
        }
    }

    /// The recorded elements a content-derived rewrite carries over from this
    /// row.
    pub fn preserved(&self) -> Preserved {
        Preserved {
            tags: self.tags.clone(),
            checksum: self.checksum.clone(),
            owner: self.owner.clone(),
            acl: self.acl.clone(),
        }
    }

    /// This row with the content-derived elements refreshed from `identity`
    /// and the recorded elements carried over.
    pub fn with_identity(&self, identity: &Identity) -> Self {
        Self {
            etag: identity.etag.clone(),
            size: identity.size,
            mtime: identity.mtime,
            identity: identity.file_identity,
            ..self.clone()
        }
    }

    /// The ETag wire elements of one row — the five metadata wires in the order
    /// the stored tuple encodes them: `(etag, tags, checksum, owner, grants)`.
    /// A missing row contributes five empty elements.
    pub fn wires(row: Option<&Self>) -> Wires5 {
        match row {
            Some(row) => {
                let wires = row.to_raw();
                (
                    wires.etag,
                    wires.tags,
                    wires.checksum,
                    wires.owner,
                    wires.grants,
                )
            }
            None => Default::default(),
        }
    }
}

/// The five metadata wire elements of one object row, in stored order.
pub type Wires5 = (String, String, String, String, String);

/// The stored object tuple's elements, owned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wires {
    /// The ETag hex wire.
    pub etag: String,
    /// The content length in bytes.
    pub size: u64,
    /// The content's mtime in unix nanoseconds.
    pub mtime: u64,
    /// The platform file identity (`0` = unavailable).
    pub identity: u64,
    /// The tag-set wire.
    pub tags: String,
    /// The checksum wire.
    pub checksum: String,
    /// The owner wire.
    pub owner: String,
    /// The ACL grants wire.
    pub grants: String,
}

/// One retained part of a completed multipart object — the `OBJECT_PARTS`
/// row served by GetObjectAttributes (`ObjectPart`): part number, size and the
/// stored per-part checksum. No ETag: the S3 dto has none. Not to be confused
/// with an upload's [`super::upload::PartRow`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartRow {
    /// Part number.
    pub number: u32,
    /// Part size in bytes.
    pub size: u64,
    /// The stored per-part checksum (`None` = none was computed, or the stored
    /// wire is domain-invalid — self-healing).
    pub checksum: Option<checksum::Part>,
}

impl PartRow {
    /// Validate one raw `OBJECT_PARTS` row. A domain-invalid checksum wire
    /// self-heals to `None` — the part is still listed (F07).
    pub fn from_raw(number: u32, size: u64, algorithm: &str, value: &str) -> Self {
        Self {
            number,
            size,
            checksum: checksum::Part::from_wire_opt(algorithm, value),
        }
    }

    /// Encode the row back into its value tuple's elements.
    pub fn to_raw(&self) -> (u64, String, String) {
        match self.checksum.as_ref() {
            Some(part) => {
                let (algorithm, value) = part.to_wire();
                (self.size, algorithm.to_owned(), value.to_owned())
            }
            None => (self.size, String::new(), String::new()),
        }
    }
}

impl TryFrom<PartRow> for multipart::ObjectPart {
    type Error = storage::Error;

    /// A stored part number outside the API's `1..=10000` is not a
    /// representable [`multipart::ObjectPart`]: the S3 surface has no such
    /// part, so the projection refuses with
    /// [`storage::Error::InvalidPartNumber`], naming the number — the row
    /// is the backend's (a stored `u32`), while the projection is the
    /// consumer's, so a row this crate did not write must not panic a
    /// `parts()` read.
    fn try_from(row: PartRow) -> Result<Self, Self::Error> {
        Ok(Self {
            part_number: multipart::part_number(row.number)?,
            size: row.size,
            checksum: row.checksum,
        })
    }
}

impl From<&multipart::ObjectPart> for PartRow {
    /// A validated part number (`1..=10000`) is always a representable
    /// stored `u32`, so the encode direction cannot fail.
    fn from(part: &multipart::ObjectPart) -> Self {
        Self {
            number: u32::from(part.part_number),
            size: part.size,
            checksum: part.checksum.clone(),
        }
    }
}

/// Object rows: the content identity, the recorded metadata elements, the
/// retained part list and the two traversals.
///
/// # Examples
///
/// ```ignore
/// use tinio_core::{bucket, meta::{Metadata, ObjectMeta}, object};
///
/// async fn round_trip<M: ObjectMeta + Metadata>(store: &M, bucket: &bucket::Name) {
///     let key = object::key("a.txt").unwrap();
///     assert!(store.get(bucket, &key).await.unwrap().is_none());
///     assert!(store.walk_healing(bucket).await.unwrap().is_empty());
/// }
/// ```
#[async_trait]
pub trait ObjectMeta: Send + Sync + 'static {
    /// One object row, or `None` when the bucket or the key has none. A corrupt
    /// row reads as `None` (self-healing).
    async fn get(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<Row>, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Upsert one object row. **Clears the key's retained part list** in the
    /// same transaction: an overwrite is a fresh object, so the parts of the
    /// object it replaces must not survive it (review item 5).
    async fn put(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &Row,
    ) -> Result<(), <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Upsert one object row **only when the bucket row exists**: the refusal
    /// projects to [`storage::Error::NoSuchBucket`], so a concurrent
    /// `remove_if_empty` cannot be raced.
    async fn put_guarded(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &Row,
    ) -> Result<(), <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// The object row **and its retained part list** in one transaction.
    ///
    /// The writer asserts the bucket row inside that transaction (a bucket
    /// with no row refuses and projects that refusal), so it cannot be raced by
    /// a `remove_if_empty` — the same guard [`ObjectMeta::put_guarded`] carries.
    async fn put_with_parts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &Row,
        parts: &[PartRow],
    ) -> Result<(), <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// The retained part list (`OBJECT_PARTS`) of a completed multipart
    /// object, in ascending part-number order.
    async fn parts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Vec<PartRow>, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// The row's content-derived elements refreshed and the recorded elements
    /// (`tags`, `checksum`, `owner`, `acl`) carried over — the read-path
    /// heal. The answer is what the row records after the refresh.
    ///
    /// It materializes a row, so it carries the same in-transaction bucket-row
    /// assertion as [`ObjectMeta::put_guarded`].
    async fn put_preserving(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        identity: &Identity,
    ) -> Result<Preserved, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// The row **and its retained part list** moved in one transaction. A
    /// `from` with no row is a no-op, and so is a move **onto the same key**
    /// (`from == to`): the row is already where it is asked to go, so nothing
    /// is rewritten and the answer is `Ok(())` — a self-move is never recorded
    /// as a move.
    async fn move_(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Replace the row's tag set. `Ok(false)` is a missing row; `Ok(true)` is
    /// a write.
    async fn set_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<bool, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Clear the row's tag set (idempotent). Same presence outcome as
    /// [`ObjectMeta::set_tags`].
    async fn clear_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<bool, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Record the tag set on the row, creating no row for a missing key and
    /// replacing an identical set without a write.
    async fn ensure_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<(), <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Replace the row's owner and grant set. `Ok(false)` is a missing row;
    /// `Ok(true)` a recorded one (review item 9).
    async fn set_acl(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        owner: Option<&acl::OwnerId>,
        grants: &acl::AclGrants,
    ) -> Result<bool, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Upsert many rows in one transaction, in input order. **Clears every
    /// written key's retained part list**, exactly as [`ObjectMeta::put`] does:
    /// an overwrite is a fresh object, so the parts of the object it replaces
    /// must not survive it (review item 5).
    async fn put_batch(
        &self,
        bucket: &bucket::Name,
        rows: &[(object::Key, Row)],
    ) -> Result<(), <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Remove one row. `Ok(false)` is a missing row; `Ok(true)` was present.
    ///
    /// **The key's version slot is not this call's.** `current_slots` is the
    /// version plane's own state ([`VersionMeta::current_slot`](crate::meta::VersionMeta::current_slot)), a slot with no
    /// object row is a recorded state the contract admits, and the calls that
    /// retire a slot are the version plane's own `advance_slot` and
    /// `restore_slot` plus the bucket teardown — never a row removal (the
    /// 2026-10-13 ruling, which the fs and redb stores already follow and pg
    /// followed only in its version plane). The retained part list *is* this
    /// call's: it goes with the row, exactly as it does for
    /// [`ObjectMeta::remove_many`].
    async fn remove(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<bool, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Remove many rows in one transaction (idempotent).
    async fn remove_many(
        &self,
        bucket: &bucket::Name,
        keys: &[object::Key],
    ) -> Result<(), <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// One call per listing page (never a point read per key): the rows of
    /// `keys`, in the input's order, `None` for a key with no row.
    async fn load(
        &self,
        bucket: &bucket::Name,
        keys: &[object::Key],
    ) -> Result<Vec<Option<Row>>, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Strict traversal of the bucket, in ascending key order: a corrupt row
    /// **fails** the traversal.
    async fn walk(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Vec<(object::Key, Row)>, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Healing traversal of the bucket, in ascending key order: a corrupt row
    /// is **healed** and the traversal never fails. The `Option` is the heal
    /// decision itself — `None` is a key whose row was corrupt and not served,
    /// `Some` a served row — which is what a caller that must count or report
    /// the healed keys needs; `walk` keeps failing instead (review item 7). A
    /// heal **write** that fails is logged and the read still succeeds: a
    /// store is never made unreadable by its own repair.
    async fn walk_healing(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Vec<(object::Key, Option<Row>)>, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Whether the bucket holds **any object row** — the bucket teardown's
    /// metadata-plane probe.
    ///
    /// A current **delete marker** is a row here and writes no content, so the
    /// content plane alone cannot answer it: a bucket whose only remaining
    /// state is a marker must refuse the teardown (S3's own rule, and the
    /// predicate the pg backend's teardown already carries:
    /// `exists(SELECT 1 FROM object_meta WHERE bucket = $1)`). The retained
    /// part list is this plane's too — a row with no part rows still counts.
    ///
    /// The probe is over the bucket's own key range and **decodes nothing**,
    /// so a domain-invalid key row counts exactly as a healthy one does — the
    /// newly agreed cross-backend rule, which pg's `exists` has always
    /// answered. Version rows and index entries are another plane's and are
    /// deliberately **not** consulted: they do not block a teardown.
    ///
    /// Deliberately not defaulted: the only bodies this trait's other methods
    /// could spell are the two traversals, and neither answers the rule —
    /// `walk` fails on a corrupt row and `walk_healing` skips a
    /// domain-invalid key row, while a probe must count both and never fail.
    async fn has_objects(&self, bucket: &bucket::Name) -> Result<bool, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Remove every object row of the bucket (idempotent).
    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), <Self as Metadata>::Error>
    where
        Self: Metadata;
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    const ETAG: &str = "d41d8cd98f00b204e9800998ecf8427e";

    /// The mtime the stored tuple pins (7 unix nanoseconds).
    fn at() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_nanos(7)
    }

    /// The non-empty tag set the stored tuple pins.
    fn tag_set() -> object::Tags {
        object::Tags::from_wire_limited("env=prod", object::OBJECT_TAGS_MAX)
    }

    /// One object row with the pinned size and identity, decoded from the
    /// contract's own values.
    fn object_row(etag: &str, checksum: &str, tags: object::Tags, acl: Acl) -> Option<Row> {
        Row::from_values(etag, 4, at(), 9, tags, checksum, acl)
    }

    /// The whole ACL of a row: the split owner and grant set rejoined
    /// ([`Acl::from_parts`]) the way a backend keeps them in one stored value.
    fn whole_acl(row: &Row) -> Acl {
        Acl::from_parts(row.owner.clone(), row.acl.grants.clone())
    }

    #[test]
    fn an_object_row_round_trips_through_its_wires() {
        let row = object_row(ETAG, "", tag_set(), Acl::default_private(None))
            .expect("a valid etag row is served");
        assert_eq!(row.etag.as_str(), ETAG);
        assert_eq!(row.size, 4);
        assert_eq!(row.mtime, at());
        assert_eq!(row.identity, 9);
        assert_eq!(row.tags.iter().collect::<Vec<_>>(), [("env", "prod")]);
        assert_eq!(row.checksum, None);
        let wires = row.to_raw();
        assert_eq!(
            Row::from_values(
                &wires.etag,
                wires.size,
                storage::from_nanos(wires.mtime),
                wires.identity,
                row.tags.clone(),
                &wires.checksum,
                whole_acl(&row),
            )
            .unwrap(),
            row
        );
    }

    /// The corrupt row `walk` fails on: its content identity does not decode,
    /// so the strict traversal has no `Row` to yield. The store-level leg that
    /// plants this row needs raw table access (P1 Task 6's meta/object.rs;
    /// D43) — this is its decode half.
    #[test]
    fn a_corrupt_etag_makes_the_whole_row_read_as_absent() {
        // The point read's self-heal: a row that cannot say its content's
        // identity is not served at all — the reader recomputes and rewrites.
        assert_eq!(
            object_row("not-an-etag", "", tag_set(), Acl::default_private(None)),
            None
        );
    }

    /// The row `walk_healing` serves as `Some` and heals: a valid identity
    /// with a corrupt metadata element, which never fails the traversal. This
    /// is the decode half of the healing leg; the tag and ACL self-heal lives
    /// in each backend's own decoder.
    #[test]
    fn a_corrupt_checksum_element_self_heals_and_the_row_is_still_served() {
        let owner = OwnerId::new("a".repeat(64)).expect("a canonical id");
        let row = object_row(
            ETAG,
            "CRC32:@@:NOPE",
            tag_set(),
            Acl::default_private(Some(owner.clone())),
        )
        .expect("the etag is valid and the row is served");
        assert_eq!(row.checksum, None, "a corrupt checksum wire is no checksum");
        assert_eq!(row.owner, Some(owner));
        assert_eq!(row.acl.grants.len(), 1, "the owned grant rides the ACL");
        assert_eq!(row.size, 4, "the content-derived elements ride untouched");
    }

    #[test]
    fn a_part_row_round_trips_and_self_heals_its_checksum() {
        let part = PartRow {
            number: 2,
            size: 100,
            checksum: Some(checksum::Part {
                algorithm: checksum::Algorithm::Crc32,
                value: checksum::Value("NhCmhg==".into()),
            }),
        };
        let (size, algorithm, value) = part.to_raw();
        assert_eq!(PartRow::from_raw(2, size, &algorithm, &value), part);
        // A garbage wire lists the part without a checksum (F07).
        assert_eq!(PartRow::from_raw(3, 10, "BLAKE3", "AA==").checksum, None);
    }

    #[test]
    fn the_object_part_conversions_round_trip() {
        let part = multipart::ObjectPart {
            part_number: multipart::part_number(3).expect("a valid part number"),
            size: 50,
            checksum: None,
        };
        let row = PartRow::from(&part);
        assert_eq!(row.number, 3);
        assert_eq!(multipart::ObjectPart::try_from(row).unwrap(), part);
    }

    /// A stored part number outside the API range is not a representable
    /// `ObjectPart`: the projection refuses and names the number instead of
    /// panicking. The row itself (`PartRow`) is the backend's raw `u32` and
    /// is served either way; only the projection into the S3 shape can fail.
    #[test]
    fn an_out_of_range_stored_part_number_refuses_the_object_part_projection() {
        for number in [0, 10_001, u32::MAX] {
            let row = PartRow {
                number,
                size: 5,
                checksum: None,
            };
            assert!(
                matches!(
                    multipart::ObjectPart::try_from(row),
                    Err(storage::Error::InvalidPartNumber(n)) if n == number
                ),
                "a stored part number of {number} must refuse the ObjectPart projection"
            );
        }
    }

    #[test]
    fn put_preserving_carries_the_recorded_elements_over() {
        let row = object_row(ETAG, "", tag_set(), Acl::default_private(None)).unwrap();
        let refreshed = row.with_identity(&Identity {
            etag: "900150983cd24fb0d6963f7d28e17f72".parse::<ETag>().unwrap(),
            size: 11,
            mtime: SystemTime::UNIX_EPOCH,
            file_identity: 3,
        });
        assert_eq!(refreshed.etag.as_str(), "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(refreshed.size, 11);
        assert_eq!(refreshed.mtime, SystemTime::UNIX_EPOCH);
        assert_eq!(refreshed.identity, 3);
        assert_eq!(
            refreshed.preserved(),
            Preserved {
                tags: row.tags.clone(),
                checksum: None,
                owner: None,
                acl: Acl::default_private(None),
            }
        );
    }
}
