//! `(bucket, key, seq)` → `(id wire, blob wire, size, etag wire, created unix
//! nanos, became-noncurrent unix nanos, kind wire, tags wire, checksum wire,
//! owner wire, acl wire)` — one version row of an object (spec
//! 2026-10-03-s3-versioning): a superseded version or delete marker, and the
//! **current** slot for the delete-marker form a backend keeps as a row. A row
//! is written with `became_noncurrent == 0` — the supersede commits it before
//! the current slot moves off it, so the crash window's adopted row is not
//! history yet — and the transaction that moves the current slot stamps it
//! with the moment it turned noncurrent; [`Row::is_history`] is the one
//! predicate a reader asks. The key's `seq` is the per-key
//! sequence the writers mint (descending = newest first) and is the row's only
//! ordering source; the value's `id` element is the version's wire form with
//! the literal `null` for the null version — the same convention
//! [`crate::object_meta`]'s codec uses, so there is no empty-string sentinel to
//! remember. The `blob` element is the encoded whole [`BlobRef`] (see
//! [`encode_blob`] / [`decode_blob`]): the row carries the block list, not a
//! bare content hash, because v1's one-block policy is a seam — splitting a
//! blob into chunks later changes writers, not this format. A delete marker
//! stores the empty blob ([`BlobRef::single`] of `BlockHash::of(b"")`,
//! `0`), so the row needs no absent value and [`Row::blob`] stays total.
//!
//! The `etag` element is the version's recorded ETag, in the same wire form
//! the `object_meta` row uses ([`ETag::as_str`]): the version-addressed read
//! and the listing serve it straight from the row, with no whole-blob rehash.
//! The value is copied from the `object_meta` row when the version is
//! superseded, never recomputed from the content — a rehash would be both a
//! whole-blob read per version and *wrong* for a multipart-composed
//! `MD5-of-MD5s-N` ETag, which no plain content hash can reproduce. The row
//! format change is additive at the table level but not at the row level:
//! `FORMAT_VERSION` stays `1` (the 2026-09-02 ruling), so a database written
//! in the ten-element shape errors at decode and the remedy is
//! delete-and-rebuild.

use std::ops::Bound;

use parse_display::{Display, FromStr};
use redb::{ReadableTable, TableDefinition};

use crate::{
    _core::{
        acl::{Acl, OwnerId},
        blob::BlobRef,
        checksum,
        etag::ETag,
        from_nanos, object,
        object::OBJECT_TAGS_MAX,
        version,
    },
    acl::{decode_acl_wire, decode_owner_wire},
    error::Error,
    object_meta,
    scan::drain_triple_u64,
    table::{self, TableDef},
};

/// The per-table marker: the table definition for the shared handle arms.
#[doc(hidden)]
pub enum Def {}

impl TableDef for Def {
    type Key = (&'static str, &'static str, u64);
    type Value = (
        &'static str, // the version id wire (the literal "null" = null version)
        &'static str, // the encoded blob (the block list lives here)
        u64,          // content length in bytes
        &'static str, // the version's ETag wire (hex, optional `-N` suffix)
        u64,          // created, unix nanoseconds
        u64,          // became noncurrent, unix nanoseconds (0 = current)
        &'static str, // "object" | "marker"
        &'static str, // tags wire
        &'static str, // checksum wire
        &'static str, // owner wire
        &'static str, // acl wire
    );

    const DEF: TableDefinition<'static, Self::Key, Self::Value> = TableDefinition::new("versions");
    /// A root written before the version machinery has no `versions`
    /// table, and "no history" must not read as "this root cannot be
    /// read": the missing table reads as empty
    /// ([`TableDef::MAY_BE_ABSENT`], spec 2026-10-03).
    const MAY_BE_ABSENT: bool = true;
}

/// Handle to the version table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

/// Whether a version row is an object version or a delete marker.
///
/// # Examples
///
/// ```rust
/// use tinio_store_redb::version::Kind;
///
/// assert_eq!(Kind::Object.to_string(), "object");
/// assert_eq!(Kind::Marker.to_string(), "marker");
/// assert_eq!("marker".parse::<Kind>().unwrap(), Kind::Marker);
/// assert!("garbage".parse::<Kind>().is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, FromStr)]
pub enum Kind {
    /// A stored object version: the row's blob is the version's content.
    #[display("object")]
    Object,
    /// A delete marker: the row's blob is the empty blob.
    #[display("marker")]
    Marker,
}

impl Kind {
    /// Read the stored wire form. A wire outside the two names is not a
    /// kind, so its row decodes as missing (see `decode_row`).
    fn from_wire(wire: &str) -> Option<Self> {
        wire.parse().ok()
    }
}

/// One stored version row (the value tuple documented at the module level,
/// plus the key's `seq` — every read fills it from the key).
///
/// The `id`, `blob`, `etag`, `tags`, `checksum`, `owner` and `acl` elements
/// stay in their wire form; the typed accessors ([`Row::id`], [`Row::blob`],
/// [`Row::etag`], …) decode them, so a caller never parses an element by
/// hand. The writer takes `seq` from the row (the key's third element), so
/// the two never disagree.
///
/// # Examples
///
/// ```rust
/// use tinio_store_redb::{
///     _core::blob::{BlobRef, BlockHash},
///     version::{Kind, Row, encode_blob},
/// };
///
/// let row = Row {
///     id: "3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY".into(),
///     blob: encode_blob(&BlobRef::single(BlockHash::of(b"x"), 1)),
///     size: 1,
///     etag: "d41d8cd98f00b204e9800998ecf8427e".into(),
///     created: 0,
///     became_noncurrent: 0,
///     kind: Kind::Object,
///     tags: String::new(),
///     checksum: String::new(),
///     owner: String::new(),
///     acl: String::new(),
///     seq: 7,
/// };
///
/// assert_eq!(
///     row.id().unwrap().as_str(),
///     "3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY"
/// );
/// assert_eq!(row.blob().len(), 1);
/// assert_eq!(row.etag().as_str(), "d41d8cd98f00b204e9800998ecf8427e");
/// assert_eq!(row.seq, 7);
/// ```
/// What a caller knows about a key's **current slot** when it asks
/// [`Row::is_history`]. The three states are not interchangeable: a row that
/// is not at the slot's sequence is history whatever its stamp says, so a
/// caller that knows the sequence and one that knows the key has no current
/// version must not be read as "no fact at all".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Slot {
    /// The key's current version sits at this sequence — the sequence the
    /// slot records, or the one the served version was written with.
    At(u64),
    /// The key has **no** current version at all: no slot exists (its
    /// `object_meta` row is gone), or the slot's content is gone out of band
    /// while the row still described it, so the key serves nothing and every
    /// version row is history.
    Gone,
    /// The caller holds no slot fact (a whole-table walk with no per-key
    /// view, a backend whose rows carry the slot itself): the stamp alone
    /// answers, and an unstamped row is the current version's own.
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// The version's identifier wire form — the literal `null` for the
    /// null version (decode with [`Row::id`]).
    pub id: String,
    /// The encoded blob ([`encode_blob`]'s wire; decode with
    /// [`Row::blob`]).
    pub blob: String,
    /// The version's content length in bytes (0 for a delete marker).
    pub size: u64,
    /// The version's ETag wire — the recorded form, empty for a marker
    /// (decode with [`Row::etag`]).
    pub etag: String,
    /// When the version was created, in unix nanoseconds.
    pub created: u64,
    /// When the version became noncurrent, in unix nanoseconds (`0` while it
    /// is not history — the current version's own row, the crash window's
    /// adopted row included; see [`Row::is_history`]).
    pub became_noncurrent: u64,
    /// Whether the row is an object version or a delete marker.
    pub kind: Kind,
    /// The version's tag set wire (empty when none; [`Row::tags`] decodes).
    pub tags: String,
    /// The version's recorded checksum wire ([`Row::checksum`] decodes).
    pub checksum: String,
    /// The version's owner wire ([`Row::owner`] decodes).
    pub owner: String,
    /// The version's ACL grants wire ([`Row::acl`] decodes).
    pub acl: String,
    /// The per-key sequence: the key's third element, filled by every read.
    pub seq: u64,
}

impl Row {
    /// The version's identifier — `None` is the null version. A corrupt
    /// element answers `None` too ([`version::Id::from_wire`]'s rule: a
    /// malformed value reads as no version).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_store_redb::version::Row;
    ///
    /// let mut row = Row {
    ///     id: "abc".into(),
    ///     blob: String::new(),
    ///     size: 0,
    ///     etag: String::new(),
    ///     created: 0,
    ///     became_noncurrent: 0,
    ///     kind: tinio_store_redb::version::Kind::Marker,
    ///     tags: String::new(),
    ///     checksum: String::new(),
    ///     owner: String::new(),
    ///     acl: String::new(),
    ///     seq: 1,
    /// };
    /// assert_eq!(row.id().unwrap().as_str(), "abc");
    ///
    /// row.id = "null".into();
    /// assert_eq!(row.id(), None, "the literal is the null version");
    /// ```
    pub fn id(&self) -> Option<version::Id> {
        version::Id::from_wire(&self.id)
    }

    /// The version's blob, decoded through [`decode_blob`] — the only
    /// reader of the `blob` element.
    ///
    /// A row the table returned always decodes: the readers drop a row
    /// whose `blob` element is corrupt (self-healing, like a corrupt
    /// etag), so this accessor is total. The fallback — the empty blob —
    /// exists only for a hand-built row; it is never content the table
    /// returned.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_store_redb::{
    ///     _core::blob::{BlobRef, BlockHash},
    ///     version::{Kind, Row, encode_blob},
    /// };
    ///
    /// let blob = BlobRef::single(BlockHash::of(b"hello"), 5);
    /// let row = Row {
    ///     id: "null".into(),
    ///     blob: encode_blob(&blob),
    ///     size: 5,
    ///     etag: String::new(),
    ///     created: 0,
    ///     became_noncurrent: 0,
    ///     kind: Kind::Object,
    ///     tags: String::new(),
    ///     checksum: String::new(),
    ///     owner: String::new(),
    ///     acl: String::new(),
    ///     seq: 1,
    /// };
    /// assert_eq!(row.blob(), blob);
    /// ```
    pub fn blob(&self) -> BlobRef {
        decode_blob(&self.blob).unwrap_or_else(BlobRef::empty)
    }

    /// The version's recorded ETag — the row's `etag` element decoded with
    /// [`str::parse`]. The empty element (a delete marker, or a row written
    /// by a writer that recorded none) and a domain-invalid element
    /// self-heal to the empty-content ETag, like every other stored
    /// metadata wire; the element is never a content hash of the row's
    /// blob, so a multipart-composed `MD5-of-MD5s-N` value survives
    /// verbatim.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_store_redb::version::{Kind, Row};
    ///
    /// let mut row = Row {
    ///     id: "null".into(),
    ///     blob: String::new(),
    ///     size: 0,
    ///     etag: "065947336a2f2a95ba8899f3675c3be6-2".into(),
    ///     created: 0,
    ///     became_noncurrent: 0,
    ///     kind: Kind::Object,
    ///     tags: String::new(),
    ///     checksum: String::new(),
    ///     owner: String::new(),
    ///     acl: String::new(),
    ///     seq: 1,
    /// };
    /// assert_eq!(row.etag().as_str(), "065947336a2f2a95ba8899f3675c3be6-2");
    ///
    /// row.etag = String::new();
    /// assert_eq!(row.etag().as_str(), "d41d8cd98f00b204e9800998ecf8427e");
    /// ```
    pub fn etag(&self) -> ETag {
        self.etag.parse::<ETag>().unwrap_or(ETag::EMPTY)
    }

    /// The version's tag set — an empty or corrupt wire self-heals to the
    /// empty set, like every other stored tags wire.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_store_redb::version::{Kind, Row};
    ///
    /// let row = Row {
    ///     id: "null".into(),
    ///     blob: String::new(),
    ///     size: 0,
    ///     etag: String::new(),
    ///     created: 0,
    ///     became_noncurrent: 0,
    ///     kind: Kind::Object,
    ///     tags: "env=prod".into(),
    ///     checksum: String::new(),
    ///     owner: String::new(),
    ///     acl: String::new(),
    ///     seq: 1,
    /// };
    /// assert!(row.tags().iter().any(|(k, v)| k == "env" && v == "prod"));
    /// ```
    pub fn tags(&self) -> object::Tags {
        object::Tags::from_wire_limited(&self.tags, OBJECT_TAGS_MAX)
    }

    /// The version's recorded checksum (`None` when none, or when the
    /// stored element is domain-invalid — self-healing like the tags).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_store_redb::version::{Kind, Row};
    ///
    /// let row = Row {
    ///     id: "null".into(),
    ///     blob: String::new(),
    ///     size: 0,
    ///     etag: String::new(),
    ///     created: 0,
    ///     became_noncurrent: 0,
    ///     kind: Kind::Object,
    ///     tags: String::new(),
    ///     checksum: "CRC32:NhCmhg==:FULL_OBJECT".into(),
    ///     owner: String::new(),
    ///     acl: String::new(),
    ///     seq: 1,
    /// };
    /// assert!(row.checksum().is_some());
    /// ```
    pub fn checksum(&self) -> Option<checksum::Recorded> {
        checksum::Recorded::from_wire_opt(&self.checksum)
    }

    /// The version's owner canonical ID (`None` when none recorded, or
    /// when the stored element is domain-invalid — self-healing like the
    /// tags).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_store_redb::version::{Kind, Row};
    ///
    /// let row = Row {
    ///     id: "null".into(),
    ///     blob: String::new(),
    ///     size: 0,
    ///     etag: String::new(),
    ///     created: 0,
    ///     became_noncurrent: 0,
    ///     kind: Kind::Object,
    ///     tags: String::new(),
    ///     checksum: String::new(),
    ///     owner: String::new(),
    ///     acl: String::new(),
    ///     seq: 1,
    /// };
    /// assert_eq!(row.owner(), None, "an empty wire carries no owner");
    /// ```
    pub fn owner(&self) -> Option<OwnerId> {
        decode_owner_wire(&self.owner)
    }

    /// The version's access-control list — the grants wire decodes with
    /// [`decode_acl_wire`]'s self-heal (an empty or domain-invalid wire is
    /// the private default with no owner).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_store_redb::version::{Kind, Row};
    ///
    /// let row = Row {
    ///     id: "null".into(),
    ///     blob: String::new(),
    ///     size: 0,
    ///     etag: String::new(),
    ///     created: 0,
    ///     became_noncurrent: 0,
    ///     kind: Kind::Object,
    ///     tags: String::new(),
    ///     checksum: String::new(),
    ///     owner: String::new(),
    ///     acl: String::new(),
    ///     seq: 1,
    /// };
    /// assert!(row.acl().grants.is_empty());
    /// ```
    pub fn acl(&self) -> Acl {
        decode_acl_wire(&self.acl)
    }

    /// Whether this row is a **history** version rather than the current
    /// version's own row.
    ///
    /// The stamp is the authority for the row that **is** the current
    /// version's. A supersede commits the outgoing version's row **before**
    /// the key path (or the current slot) is moved off it, so the row is
    /// written with `became_noncurrent == 0` — not history yet, which is
    /// exactly the crash window's adopted row — and the transaction that
    /// moves the current slot stamps it. Every reader that needs the
    /// distinction asks this question instead of re-deriving it from the
    /// current slot's sequence; what the caller knows about that slot is
    /// [`Slot`]: its sequence ([`Slot::At`]), that the key has no current
    /// version at all ([`Slot::Gone`]), or nothing ([`Slot::Unknown`]).
    ///
    /// A row that is not at the slot's sequence is history **whatever its
    /// stamp says**: an unstamped row at another sequence is the residue of a
    /// publish that never finished moving the slot (the unix chown-failure
    /// path aborts after the rename; an out-of-band file removal inside the
    /// crash window leaves the same shape), and no reader may serve it as a
    /// second "latest" version or keep it alive for good. A row at a key with
    /// no current version at all is history too, for the same reason.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_store_redb::version::{Kind, Row, Slot};
    ///
    /// let mut row = Row {
    ///     id: "null".into(),
    ///     blob: String::new(),
    ///     size: 0,
    ///     etag: String::new(),
    ///     created: 0,
    ///     became_noncurrent: 0,
    ///     kind: Kind::Object,
    ///     tags: String::new(),
    ///     checksum: String::new(),
    ///     owner: String::new(),
    ///     acl: String::new(),
    ///     seq: 4,
    /// };
    /// assert!(
    ///     !row.is_history(Slot::At(4)),
    ///     "the current slot's own row is not history"
    /// );
    /// assert!(
    ///     row.is_history(Slot::At(5)),
    ///     "an unstamped row the slot has moved off is history"
    /// );
    /// assert!(
    ///     row.is_history(Slot::Gone),
    ///     "a key with no current version has no current version's row"
    /// );
    /// assert!(
    ///     !row.is_history(Slot::Unknown),
    ///     "an unstamped row is the current version's when the caller knows no slot"
    /// );
    ///
    /// row.became_noncurrent = 7;
    /// assert!(
    ///     row.is_history(Slot::Unknown),
    ///     "the stamp makes it history for every reader"
    /// );
    /// ```
    pub fn is_history(&self, slot: Slot) -> bool {
        match slot {
            Slot::At(seq) => self.became_noncurrent != 0 || seq != self.seq,
            Slot::Gone => true,
            Slot::Unknown => self.became_noncurrent != 0,
        }
    }

    /// The [`object::Info`] this row describes for `key` — the version-
    /// addressed read and head's answer for a noncurrent version. The ETag
    /// element is the value the version was written with (a composed
    /// `MD5-of-MD5s-N` form included), the stamp is the row's creation
    /// time, and the identity is the row's own (the null version answers
    /// `None`). One home for the field mapping the backends share;
    /// `bucket_records_versions` is the caller's — the state read its own
    /// transaction already took (a row exists only on a bucket that
    /// records versions, but the caller's read is the honest source).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::object;
    /// use tinio_store_redb::version::{Kind, Row};
    ///
    /// let row = Row {
    ///     id: "abc".into(),
    ///     blob: String::new(),
    ///     size: 5,
    ///     etag: "d41d8cd98f00b204e9800998ecf8427e".into(),
    ///     created: 0,
    ///     became_noncurrent: 7,
    ///     kind: Kind::Object,
    ///     tags: String::new(),
    ///     checksum: String::new(),
    ///     owner: String::new(),
    ///     acl: String::new(),
    ///     seq: 1,
    /// };
    /// let info = row.info(&object::key("a.txt").expect("a valid object key"), true);
    /// assert_eq!(info.size, 5);
    /// assert_eq!(info.version_id.unwrap().as_str(), "abc");
    /// ```
    pub fn info(&self, key: &object::Key, bucket_records_versions: bool) -> object::Info {
        object::Info {
            key: key.clone(),
            size: self.size,
            last_modified: from_nanos(self.created),
            etag: self.etag(),
            tags: self.tags(),
            checksum: self.checksum(),
            owner: self.owner(),
            acl: self.acl(),
            version_id: self.id(),
            bucket_records_versions,
        }
    }

    /// The [`crate::object_meta::Stored`] row this version takes when it becomes
    /// the key's current version again (a restore, a materialize): the
    /// identity, sequence and metadata elements ride the row, the kind
    /// decides the recorded marker flag, and the `size`/`mtime`/
    /// `file_identity` arguments describe the published file — the row's
    /// own size and creation stamp for a marker, which publishes nothing.
    /// One home for the nine-field mapping the backends share.
    pub fn stored(
        &self,
        seq: u64,
        size: u64,
        mtime: u64,
        file_identity: u64,
    ) -> object_meta::Stored {
        object_meta::Stored {
            version_id: self.id(),
            version_seq: seq,
            is_delete_marker: self.kind == Kind::Marker,
            etag: self.etag(),
            size,
            mtime,
            file_identity,
            tags: self.tags(),
            checksum: self.checksum(),
            owner: self.owner(),
            acl: self.acl(),
        }
    }
}

/// The blob wire codec has one home, `tinio_core`'s `meta::version` (plan
/// 2026-10-03 Task 1 Step 4); this re-export keeps the table crate's
/// `version::{decode_blob, encode_blob}` path working for its consumers.
pub use crate::_core::meta::version::{decode_blob, encode_blob};

/// Decode one stored value tuple (with the key's `seq`) into a [`Row`]:
/// `None` when the `blob` or `kind` element is corrupt. Those two are the
/// row's shape — what a version is and where its content lives — so a row
/// that cannot say either is treated as missing (self-healing, like the
/// etag in [`crate::object_meta::validate`]); the metadata wires self-heal inside
/// their accessors instead.
fn decode_row(
    seq: u64,
    (id, blob, size, etag, created, became_noncurrent, kind, tags, checksum, owner, acl): (
        &str,
        &str,
        u64,
        &str,
        u64,
        u64,
        &str,
        &str,
        &str,
        &str,
        &str,
    ),
) -> Option<Row> {
    decode_blob(blob)?;
    // The identity element must be the literal `null` or a well-formed id
    // (2026-10-06 review): a corrupt id must not self-heal to the **null
    // version** — a meaningful value, so the row would list and read as the
    // null version while the index still keys it by its raw wire. A row that
    // fails this check reads as missing, like the blob/kind checks above.
    if id != "null" && version::Id::new(id).is_err() {
        return None;
    }
    Some(Row {
        id: id.to_owned(),
        blob: blob.to_owned(),
        size,
        etag: etag.to_owned(),
        created,
        became_noncurrent,
        kind: Kind::from_wire(kind)?,
        tags: tags.to_owned(),
        checksum: checksum.to_owned(),
        owner: owner.to_owned(),
        acl: acl.to_owned(),
        seq,
    })
}

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// One version row, if present and decodable (`None` also covers a row
    /// whose blob/kind/id element is corrupt — self-healing, the row is
    /// treated as missing).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use redb::{Database, ReadableDatabase, backends::InMemoryBackend};
    /// use tinio_store_redb::{ensure_all, version};
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
    /// let table = version::Table::open_readonly(&txn).unwrap();
    /// assert!(table.get("b", "k", 0).unwrap().is_none());
    /// ```
    pub fn get(&self, bucket: &str, key: &str, seq: u64) -> Result<Option<Row>, Error> {
        let Some(guard) = self.0.get((bucket, key, seq))? else {
            return Ok(None);
        };
        Ok(decode_row(seq, guard.value()))
    }

    /// The newest version row of `key` — its sequence and row — or `None`
    /// when the key has no version (the highest key sequence wins; ordering
    /// comes from the sequence, never from the identifier).
    ///
    /// The walk is descending and answers from the first row, so the
    /// current-version hot path reads one row, never the key's history. A
    /// newest row whose `blob`/`kind` element is corrupt **fails closed**:
    /// the answer is `None` — the key reads as having no stored version —
    /// rather than the older decodable version being handed back as if it
    /// were current (a versioned GET must never serve stale content as the
    /// latest).
    pub fn latest(&self, bucket: &str, key: &str) -> Result<Option<(u64, Row)>, Error> {
        // Both bounds sit on the key's own tuples, so the reversed range
        // holds exactly this key's rows, newest first.
        let Some(item) = self
            .0
            .range((bucket, key, u64::MIN)..=(bucket, key, u64::MAX))?
            .next_back()
        else {
            return Ok(None);
        };
        let (k, v) = item?;
        let (_, _, seq) = k.value();
        Ok(decode_row(seq, v.value()).map(|row| (seq, row)))
    }

    /// Visit `key`'s versions newest first (descending sequence). A row
    /// whose shape is corrupt is skipped, never fails the walk.
    pub fn for_key<F>(&self, bucket: &str, key: &str, mut visit: F) -> Result<(), Error>
    where
        F: FnMut(Row) -> Result<(), Error>,
    {
        // Both bounds sit on the key's own tuples, so the reversed range is
        // exactly this key's rows — no unrelated key is ever walked.
        for item in self
            .0
            .range((bucket, key, u64::MIN)..=(bucket, key, u64::MAX))?
            .rev()
        {
            let (k, v) = item?;
            let (_, _, seq) = k.value();
            if let Some(row) = decode_row(seq, v.value()) {
                visit(row)?;
            }
        }
        Ok(())
    }

    /// The newest rows of `key`, at most `limit`, newest first — the
    /// bounded sibling of [`Self::for_key`], for a caller that needs only
    /// the top of a key's history (the restore's successor lookup reads at
    /// most two rows, where the full walk would decode tags, checksums,
    /// owners and ACLs for every version the key ever held). A row whose
    /// shape is corrupt is skipped, never fails the walk, and does not
    /// count toward `limit` — the answer holds at most `limit` decodable
    /// rows, exactly the rows a bounded prefix of the `for_key` walk would
    /// visit.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use redb::{Database, ReadableDatabase, backends::InMemoryBackend};
    /// use tinio_store_redb::{
    ///     _core::blob::{BlobRef, BlockHash},
    ///     ensure_all, version,
    /// };
    ///
    /// let db = Database::builder()
    ///     .create_with_backend(InMemoryBackend::new())
    ///     .unwrap();
    /// {
    ///     let mut txn = db.begin_write().unwrap();
    ///     ensure_all(&mut txn).unwrap();
    ///     {
    ///         let mut table = version::Table::open(&mut txn).unwrap();
    ///         for seq in [1u64, 2, 3] {
    ///             table
    ///                 .put(
    ///                     "b",
    ///                     "k",
    ///                     &version::Row {
    ///                         id: format!("v{seq}"),
    ///                         blob: version::encode_blob(&BlobRef::single(BlockHash::of(b"x"), 1)),
    ///                         size: 1,
    ///                         etag: String::new(),
    ///                         created: 0,
    ///                         became_noncurrent: 1,
    ///                         kind: version::Kind::Object,
    ///                         tags: String::new(),
    ///                         checksum: String::new(),
    ///                         owner: String::new(),
    ///                         acl: String::new(),
    ///                         seq,
    ///                     },
    ///                 )
    ///                 .unwrap();
    ///         }
    ///     }
    ///     txn.commit().unwrap();
    /// }
    /// let txn = db.begin_read().unwrap();
    /// let table = version::Table::open_readonly(&txn).unwrap();
    /// let newest = table.newest("b", "k", 2).unwrap();
    /// let seqs: Vec<u64> = newest.iter().map(|(seq, _)| *seq).collect();
    /// assert_eq!(seqs, [3, 2], "the newest rows, newest first");
    /// assert!(table.newest("b", "k", 0).unwrap().is_empty());
    /// ```
    pub fn newest(&self, bucket: &str, key: &str, limit: usize) -> Result<Vec<(u64, Row)>, Error> {
        let mut out = Vec::new();
        if limit == 0 {
            return Ok(out); // a zero bound never opens the range
        }
        // Both bounds sit on the key's own tuples, so the reversed range is
        // exactly this key's rows — no unrelated key is ever walked.
        for item in self
            .0
            .range((bucket, key, u64::MIN)..=(bucket, key, u64::MAX))?
            .rev()
        {
            let (k, v) = item?;
            let (_, _, seq) = k.value();
            if let Some(row) = decode_row(seq, v.value()) {
                out.push((seq, row));
                if out.len() == limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    /// Visit every row of one bucket in key order (key, ascending
    /// sequence) — the one bucket-scoped walk; there is no whole-table
    /// sibling, so a caller can never mean one bucket and walk every one.
    /// The key's first element *is* the bucket, so a
    /// bucket's rows are one contiguous interval: the walk starts at the
    /// bucket's first tuple and stops at the first key naming another bucket
    /// (the interval boundary [`Self::drain_bucket`] uses too; an exclusive
    /// upper bound would be wrong — see `crate::scan`'s note on prefix
    /// ranges). A row whose shape is corrupt is skipped, never fails the
    /// walk.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use redb::{Database, ReadableDatabase, backends::InMemoryBackend};
    /// use tinio_store_redb::{
    ///     _core::blob::{BlobRef, BlockHash},
    ///     ensure_all, version,
    /// };
    ///
    /// let db = Database::builder()
    ///     .create_with_backend(InMemoryBackend::new())
    ///     .unwrap();
    /// {
    ///     let mut txn = db.begin_write().unwrap();
    ///     ensure_all(&mut txn).unwrap();
    ///     {
    ///         let mut table = version::Table::open(&mut txn).unwrap();
    ///         for (bucket, key, seq) in [("data", "a", 1u64), ("data", "b", 2), ("other", "c", 3)] {
    ///             table
    ///                 .put(
    ///                     bucket,
    ///                     key,
    ///                     &version::Row {
    ///                         id: format!("v{seq}"),
    ///                         blob: version::encode_blob(&BlobRef::single(BlockHash::of(b"x"), 1)),
    ///                         size: 1,
    ///                         etag: String::new(),
    ///                         created: 0,
    ///                         became_noncurrent: 1,
    ///                         kind: version::Kind::Object,
    ///                         tags: String::new(),
    ///                         checksum: String::new(),
    ///                         owner: String::new(),
    ///                         acl: String::new(),
    ///                         seq,
    ///                     },
    ///                 )
    ///                 .unwrap();
    ///         }
    ///     }
    ///     txn.commit().unwrap();
    /// }
    /// let txn = db.begin_read().unwrap();
    /// let table = version::Table::open_readonly(&txn).unwrap();
    /// let mut seen = Vec::new();
    /// table
    ///     .for_bucket_of("data", |key, row| {
    ///         seen.push((key.to_owned(), row.seq));
    ///         Ok(())
    ///     })
    ///     .unwrap();
    /// assert_eq!(seen, [("a".to_owned(), 1), ("b".to_owned(), 2)]);
    /// assert!(
    ///     table.for_bucket_of("nope", |_, _| Ok(())).is_ok(),
    ///     "a bucket with no rows walks nothing"
    /// );
    /// ```
    pub fn for_bucket_of<F>(&self, bucket: &str, mut visit: F) -> Result<(), Error>
    where
        F: FnMut(&str, Row) -> Result<(), Error>,
    {
        for item in self.0.range((bucket, "", u64::MIN)..)? {
            let (k, v) = item?;
            let (name, key, seq) = k.value();
            if name != bucket {
                break;
            }
            if let Some(row) = decode_row(seq, v.value()) {
                visit(key, row)?;
            }
        }
        Ok(())
    }

    /// [`Self::for_bucket_of`] from a **bound**, with an early stop: the same
    /// rows in the same `(key, seq)` order with the same self-healing decode,
    /// but the walk starts at `from` — `Included` names the first key (its
    /// lowest sequence first), `Excluded` the key just below it (the key one
    /// bucket earlier is never touched, so a caller resuming after a key pays
    /// no re-read of it), `Unbounded` the bucket's first row — and ends as
    /// soon as `visit` answers `Ok(false)`.
    ///
    /// The bound names a **key**, not a `(key, seq)` position: a caller that
    /// stops inside a key resumes at that same key (`Included`) and skips the
    /// sequences it already read itself. A row whose shape is corrupt is
    /// skipped, never fails the walk, and never reaches `visit` — exactly
    /// [`Self::for_bucket_of`]'s rule; the keys of one bucket are contiguous,
    /// so the mismatch break is the interval boundary.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::ops::Bound;
    ///
    /// use redb::{Database, ReadableDatabase, backends::InMemoryBackend};
    /// use tinio_store_redb::{
    ///     _core::blob::{BlobRef, BlockHash},
    ///     ensure_all, version,
    /// };
    ///
    /// let db = Database::builder()
    ///     .create_with_backend(InMemoryBackend::new())
    ///     .unwrap();
    /// {
    ///     let mut txn = db.begin_write().unwrap();
    ///     ensure_all(&mut txn).unwrap();
    ///     {
    ///         let mut table = version::Table::open(&mut txn).unwrap();
    ///         for (key, seq) in [("a", 1u64), ("b", 2), ("c", 3)] {
    ///             table
    ///                 .put(
    ///                     "data",
    ///                     key,
    ///                     &version::Row {
    ///                         id: format!("v{seq}"),
    ///                         blob: version::encode_blob(&BlobRef::single(BlockHash::of(b"x"), 1)),
    ///                         size: 1,
    ///                         etag: String::new(),
    ///                         created: 0,
    ///                         became_noncurrent: 1,
    ///                         kind: version::Kind::Object,
    ///                         tags: String::new(),
    ///                         checksum: String::new(),
    ///                         owner: String::new(),
    ///                         acl: String::new(),
    ///                         seq,
    ///                     },
    ///                 )
    ///                 .unwrap();
    ///         }
    ///     }
    ///     txn.commit().unwrap();
    /// }
    /// let txn = db.begin_read().unwrap();
    /// let table = version::Table::open_readonly(&txn).unwrap();
    /// let mut seen = Vec::new();
    /// table
    ///     .for_bucket_from("data", Bound::Excluded("a"), |key, row| {
    ///         seen.push((key.to_owned(), row.seq));
    ///         Ok(false) // stop after the first row past the bound
    ///     })
    ///     .unwrap();
    /// assert_eq!(seen, [("b".to_owned(), 2)]);
    /// ```
    pub fn for_bucket_from<F>(
        &self,
        bucket: &str,
        from: Bound<&str>,
        mut visit: F,
    ) -> Result<(), Error>
    where
        F: FnMut(&str, Row) -> Result<bool, Error>,
    {
        // The bound names a key: every sequence of `Included`'s key is in
        // (the lowest one is `u64::MIN`), and none of `Excluded`'s is (its
        // own highest sequence is `u64::MAX`) — redb's own bound forms.
        let start = match from {
            Bound::Included(key) => Bound::Included((bucket, key, u64::MIN)),
            Bound::Excluded(key) => Bound::Excluded((bucket, key, u64::MAX)),
            Bound::Unbounded => Bound::Included((bucket, "", u64::MIN)),
        };
        for item in self.0.range((start, Bound::Unbounded))? {
            let (k, v) = item?;
            let (name, key, seq) = k.value();
            if name != bucket {
                break; // the interval boundary: this bucket's rows ended
            }
            if let Some(row) = decode_row(seq, v.value())
                && !visit(key, row)?
            {
                break;
            }
        }
        Ok(())
    }
}

impl<'txn> table::Table<'txn, Def> {
    /// Upsert one version row — the key `(bucket, key, seq)` (the sequence
    /// rides the row) plus the encoded value tuple. The wire elements are
    /// encoded here, the one encode home: the `id` string is already the
    /// version's wire form (the literal `null` for the null version), the
    /// blob goes through [`encode_blob`], the `etag` string is already the
    /// [`ETag::as_str`] wire, and the `kind` through its wire name.
    pub fn put(&mut self, bucket: &str, key: &str, row: &Row) -> Result<(), Error> {
        let kind = row.kind.to_string();
        self.0.insert(
            (bucket, key, row.seq),
            (
                row.id.as_str(),
                row.blob.as_str(),
                row.size,
                row.etag.as_str(),
                row.created,
                row.became_noncurrent,
                kind.as_str(),
                row.tags.as_str(),
                row.checksum.as_str(),
                row.owner.as_str(),
                row.acl.as_str(),
            ),
        )?;
        Ok(())
    }

    /// Remove one version row (idempotent).
    pub fn remove(&mut self, bucket: &str, key: &str, seq: u64) -> Result<(), Error> {
        self.0.remove((bucket, key, seq))?;
        Ok(())
    }

    /// Stamp one committed row noncurrent: its `became_noncurrent` becomes
    /// `now`, everything else stays. Idempotent, and a sequence that holds no
    /// row is `Ok` — a supersede whose row a concurrent path already dropped
    /// has nothing to stamp.
    ///
    /// This is the publish's half of the row's state: the supersede commits
    /// the outgoing version's row with `became_noncurrent == 0` (the crash
    /// window's adopted row — the version is still current), and the
    /// transaction that moves the current slot off it stamps it here, so a
    /// row that is history says so ([`Row::is_history`]) and its retention
    /// clock starts when the version stopped being current.
    pub fn stamp_noncurrent(
        &mut self,
        bucket: &str,
        key: &str,
        seq: u64,
        now: u64,
    ) -> Result<(), Error> {
        let Some(mut row) = self.get(bucket, key, seq)? else {
            return Ok(());
        };
        row.became_noncurrent = now;
        self.put(bucket, key, &row)
    }

    /// Delete every version row of `bucket` (bucket teardown; the rows of
    /// one bucket are contiguous from `(bucket, "", 0)` — the mismatch
    /// break is the interval boundary).
    pub fn drain_bucket(&mut self, bucket: &str) -> Result<(), Error> {
        drain_triple_u64(&mut self.0, (bucket, "", 0), |b, _, _| b == bucket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        _core::blob::{self, BlobRef, BlockRef},
        testutil::handle,
    };

    /// The tests' row: a 1-byte object version with the empty content's
    /// ETag wire — a real wire, so a round-trip assertion discriminates an
    /// element that failed to survive the codec.
    fn row(id: &str, seq: u64) -> Row {
        Row {
            id: id.to_owned(),
            blob: encode_blob(&BlobRef::single(blob::BlockHash::of(b"x"), 1)),
            size: 1,
            etag: ETag::EMPTY.as_str(),
            created: 0,
            became_noncurrent: 0,
            kind: Kind::Object,
            tags: String::new(),
            checksum: String::new(),
            owner: String::new(),
            acl: String::new(),
            seq,
        }
    }

    #[test]
    fn versions_are_read_newest_first_and_indexed_by_id() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            // The shared handles borrow the write transaction exclusively, so
            // the two tables are two scoped passes (the backends' pattern).
            {
                let mut v = Table::open(txn)?;
                for seq in [1u64, 2, 3] {
                    v.put("b", "k", &row(&format!("v{seq}"), seq))?;
                }
            }
            {
                let mut i = crate::version_index::Table::open(txn)?;
                for seq in [1u64, 2, 3] {
                    i.put("b", "k", &format!("v{seq}"), seq)?;
                }
            }
            Ok(())
        })
        .unwrap();

        let seen: Vec<u64> = h
            .read(|txn| -> Result<Vec<u64>, Error> {
                let v = Table::open_readonly(txn)?;
                let mut seqs = Vec::new();
                v.for_key("b", "k", |row| {
                    seqs.push(row.seq);
                    Ok(())
                })?;
                Ok(seqs)
            })
            .unwrap();
        assert_eq!(seen, vec![3, 2, 1], "newest first");
        assert_eq!(
            h.read(|txn| crate::version_index::Table::open_readonly(txn)?.seq_of("b", "k", "v2"))
                .unwrap(),
            Some(2)
        );
    }

    #[test]
    fn a_bucket_drain_removes_every_row_of_that_bucket_only() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut v = Table::open(txn)?;
            v.put("b", "k", &row("v1", 1))?;
            v.put("other", "k", &row("v1", 1))?;
            Ok(())
        })
        .unwrap();
        // The drain runs in its own transaction (drain and insert in one
        // transaction trips a redb page-manager assertion).
        h.write(|txn| -> Result<(), Error> { Table::open(txn)?.drain_bucket("b") })
            .unwrap();
        let left = h
            .read(|txn| -> Result<(Vec<u64>, Vec<u64>), Error> {
                let v = Table::open_readonly(txn)?;
                let mut drained = Vec::new();
                v.for_bucket_of("b", |_, row| {
                    drained.push(row.seq);
                    Ok(())
                })?;
                let mut other = Vec::new();
                v.for_bucket_of("other", |_, row| {
                    other.push(row.seq);
                    Ok(())
                })?;
                Ok((drained, other))
            })
            .unwrap();
        assert_eq!(left.0, Vec::<u64>::new(), "the drained bucket is empty");
        assert_eq!(left.1, vec![1], "the other bucket's row survives");
    }

    #[test]
    fn get_latest_and_remove_read_one_keys_rows() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut v = Table::open(txn)?;
            for seq in [1u64, 2, 3] {
                v.put("b", "k", &row(&format!("v{seq}"), seq))?;
            }
            v.put("b", "other", &row("v9", 9))?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let v = Table::open_readonly(txn)?;
            // The point read fills `seq` from the key, not from the value.
            let got = v.get("b", "k", 2)?.expect("present");
            assert_eq!(got.seq, 2);
            assert_eq!(got.id, "v2");
            assert!(v.get("b", "k", 4)?.is_none(), "a missing sequence is None");
            assert!(v.get("b", "nope", 1)?.is_none());
            // `latest` is the newest sequence of that key alone.
            let (seq, latest) = v.latest("b", "k")?.expect("present");
            assert_eq!((seq, latest.id.as_str()), (3, "v3"));
            assert_eq!(v.latest("b", "other")?.unwrap().1.id, "v9");
            assert!(v.latest("b", "nope")?.is_none());
            Ok(())
        })
        .unwrap();
        h.write(|txn| -> Result<(), Error> {
            let mut v = Table::open(txn)?;
            v.remove("b", "k", 2)?;
            v.remove("b", "k", 2)?; // idempotent
            assert!(v.get("b", "k", 2)?.is_none());
            assert!(v.get("b", "k", 3)?.is_some());
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn the_point_reads_and_removal_never_cross_a_key_or_bucket_boundary() {
        // The table key is `(bucket, key, seq)`, so a key's rows and a
        // bucket's rows are contiguous intervals: the bounds on the key's own
        // tuples are the whole boundary, and prefix-adjacent names must not
        // leak in. `for_bucket` is the one whole-table walk (its doc says so);
        // the bucket boundary there is the caller's `bucket == …` filter,
        // which the final assertion pins.
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut v = Table::open(txn)?;
            v.put("b", "k", &row("v1", 1))?;
            v.put("b", "k", &row("v2", 2))?;
            v.put("b", "kk", &row("v9", 9))?;
            v.put("b2", "k", &row("v8", 8))?;
            Ok(())
        })
        .unwrap();
        h.write(|txn| -> Result<(), Error> { Table::open(txn)?.remove("b", "k", 1) })
            .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let v = Table::open_readonly(txn)?;
            assert!(v.get("b", "k", 1)?.is_none(), "the removed row is gone");
            assert!(
                v.get("b", "kk", 9)?.is_some(),
                "the prefix-adjacent key rides the removal"
            );
            assert!(
                v.get("b2", "k", 8)?.is_some(),
                "the prefix-adjacent bucket rides the removal"
            );
            let mut seen = Vec::new();
            v.for_key("b", "k", |row| {
                seen.push(row.seq);
                Ok(())
            })?;
            assert_eq!(
                seen,
                [2],
                "for_key walks the key's own rows, not the prefix-adjacent key's"
            );
            let (seq, latest) = v.latest("b", "k")?.expect("present");
            assert_eq!(
                (seq, latest.id.as_str()),
                (2, "v2"),
                "latest is the key's own newest row"
            );

            // The bucket-scoped walk stops at the bucket boundary: the
            // prefix-adjacent key rides `b`, while `b2` — whose rows are
            // prefix-adjacent in the key *tuple* — is never visited.
            let mut this_bucket = Vec::new();
            v.for_bucket_of("b", |key, row| {
                this_bucket.push((key.to_owned(), row.seq));
                Ok(())
            })?;
            assert_eq!(
                this_bucket,
                [("k".to_owned(), 2), ("kk".to_owned(), 9)],
                "the walk reports the bucket's own rows in key order"
            );
            let mut adjacent = Vec::new();
            v.for_bucket_of("b2", |key, row| {
                adjacent.push((key.to_owned(), row.seq));
                Ok(())
            })?;
            assert_eq!(
                adjacent,
                [("k".to_owned(), 8)],
                "a prefix-adjacent bucket's rows belong to their own walk"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn a_rows_blob_round_trips_through_the_table_block_list_and_all() {
        // The codec's unit test proves `encode_blob`/`decode_blob` are
        // inverses; this closes the row seam — what `put` writes into the
        // value's blob element is what `get`/`latest` decode back.
        let h = handle();
        let chunked = BlobRef::new(
            blob::BlockHash::of(b"hello"),
            5,
            vec![
                BlockRef {
                    hash: blob::BlockHash::of(b"hel"),
                    len: 3,
                },
                BlockRef {
                    hash: blob::BlockHash::of(b"lo"),
                    len: 2,
                },
            ],
        )
        .unwrap();
        let stored_row = Row {
            blob: encode_blob(&chunked),
            size: 5,
            ..row("v1", 7)
        };
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?.put("b", "k", &stored_row)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let v = Table::open_readonly(txn)?;
            let got = v.get("b", "k", 7)?.expect("the row round-trips");
            assert_eq!(got, stored_row, "every element survives the encode/decode");
            assert_eq!(
                got.blob(),
                chunked,
                "the decoded block list is the one that was encoded"
            );
            assert_eq!(
                got.blob().blocks().len(),
                2,
                "a chunked blob is not flattened by the row format"
            );
            let (seq, latest) = v.latest("b", "k")?.expect("present");
            assert_eq!(
                (seq, latest.blob()),
                (7, chunked),
                "latest decodes the same blob"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn a_delete_marker_row_round_trips_the_empty_blob() {
        let h = handle();
        let marker = Row {
            id: "null".into(),
            blob: encode_blob(&BlobRef::single(blob::BlockHash::of(b""), 0)),
            size: 0,
            etag: String::new(),
            created: 7,
            became_noncurrent: 0,
            kind: Kind::Marker,
            tags: String::new(),
            checksum: String::new(),
            owner: String::new(),
            acl: String::new(),
            seq: 1,
        };
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?.put("b", "k", &marker)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let got = Table::open_readonly(txn)?.get("b", "k", 1)?.unwrap();
            assert_eq!(got, marker);
            assert_eq!(got.kind, Kind::Marker);
            assert!(got.blob().is_empty(), "the marker's blob is the empty blob");
            assert_eq!(got.id(), None, "the literal null reads as the null version");
            assert_eq!(
                got.etag(),
                ETag::EMPTY,
                "the marker's empty etag wire self-heals to the empty-content ETag"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn a_corrupt_shape_row_reads_as_missing_and_never_fails_the_walk() {
        let h = handle();
        let good = row("v1", 1);
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            t.put("b", "k", &good)?;
            // A garbage blob element (written by a stale/corrupt writer) and
            // an unknown kind element: both are the row's shape, so neither
            // row is served.
            t.insert(
                ("b", "k", 2),
                ("v2", "garbage", 1u64, "", 0, 0, "object", "", "", "", ""),
            )
            .map_err(Error::from)?;
            t.insert(
                ("b", "k", 3),
                (
                    "v3",
                    good.blob.as_str(),
                    1u64,
                    "",
                    0,
                    0,
                    "sometime",
                    "",
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
            let t = Table::open_readonly(txn)?;
            assert!(
                t.get("b", "k", 2)?.is_none(),
                "a corrupt blob row is missing"
            );
            assert!(
                t.get("b", "k", 3)?.is_none(),
                "an unknown kind row is missing"
            );
            let mut seen = Vec::new();
            t.for_key("b", "k", |row| {
                seen.push(row.seq);
                Ok(())
            })?;
            assert_eq!(seen, [1], "the walk skips the corrupt rows");
            // The current-version read fails closed: the newest row is
            // corrupt, so the answer is "no stored version" — the older
            // decodable row (seq 1) is never promoted to current.
            assert!(
                t.latest("b", "k")?.is_none(),
                "a corrupt newest row answers None, never an older version"
            );
            // A key whose newest row is intact still answers it.
            Ok(())
        })
        .unwrap();
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            t.put("b", "intact", &row("v9", 9))?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let t = Table::open_readonly(txn)?;
            assert_eq!(
                t.latest("b", "intact")?.map(|(seq, row)| (seq, row.id)),
                Some((9, "v9".to_owned())),
                "an intact newest row is what `latest` hands back"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn the_blob_codec_round_trips_and_rechecks_every_invariant() {
        // The empty blob (a delete marker's shape) is a valid wire.
        let empty = BlobRef::single(blob::BlockHash::of(b""), 0);
        assert_eq!(decode_blob(&encode_blob(&empty)), Some(empty.clone()));
        // A multi-block blob round-trips too: the list, not the v1 shape, is
        // what the format carries.
        let chunked = BlobRef::new(
            blob::BlockHash::of(b"hello"),
            5,
            vec![
                BlockRef {
                    hash: blob::BlockHash::of(b"hel"),
                    len: 3,
                },
                BlockRef {
                    hash: blob::BlockHash::of(b"lo"),
                    len: 2,
                },
            ],
        )
        .unwrap();
        assert_eq!(decode_blob(&encode_blob(&chunked)), Some(chunked.clone()));
        assert_eq!(
            encode_blob(&chunked),
            format!(
                "{}:5:{}:3,{}:2",
                chunked.hash().to_hex(),
                chunked.blocks()[0].hash.to_hex(),
                chunked.blocks()[1].hash.to_hex()
            )
        );

        // The decoder re-checks rather than trusting the row:
        let hash = chunked.hash().to_hex();
        assert_eq!(decode_blob(""), None, "no content hash");
        assert_eq!(decode_blob("garbage"), None, "no elements");
        assert_eq!(
            decode_blob(&format!("{hash}:5:not-hex:3")),
            None,
            "bad block hash"
        );
        assert_eq!(
            decode_blob(&format!("{hash}:6:{}:5", empty.hash().to_hex())),
            None,
            "the declared length must equal the blocks' sum"
        );
        assert_eq!(
            decode_blob(&format!("{hash}:0:")),
            None,
            "at least one block"
        );
        assert_eq!(
            decode_blob(&format!(
                "{hash}:0:{}:0,{}:0",
                empty.hash().to_hex(),
                empty.hash().to_hex()
            )),
            None,
            "a zero-length block is legal only as the list's sole entry"
        );
        assert_eq!(
            decode_blob(&format!(
                "{hash}:{}:{}:{},{}:5",
                u64::MAX,
                empty.hash().to_hex(),
                u64::MAX,
                empty.hash().to_hex()
            )),
            None,
            "a sum that overflows is rejected, never wrapped"
        );
    }

    #[test]
    fn the_row_accessors_self_heal_their_metadata_wires() {
        let mut row = row("v1", 1);
        row.tags = "garbage".into();
        row.checksum = "garbage".into();
        row.owner = "garbage".into();
        row.acl = "garbage".into();
        row.etag = "garbage".into();
        assert!(row.tags().is_empty());
        assert_eq!(row.checksum(), None);
        assert_eq!(row.owner(), None);
        assert!(row.acl().grants.is_empty());
        assert_eq!(
            row.etag(),
            ETag::EMPTY,
            "a domain-invalid etag wire self-heals like the other metadata wires"
        );
        assert!(
            row.blob().len() == 1,
            "the shape wires are validated at decode; the metadata wires heal here"
        );
    }

    #[test]
    fn a_rows_etag_round_trips_through_the_table_composed_form_and_all() {
        // The element is the recorded ETag, not a content hash: a composed
        // multipart value survives the codec verbatim.
        let h = handle();
        let composed = Row {
            etag: "065947336a2f2a95ba8899f3675c3be6-2".into(),
            ..row("v1", 4)
        };
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?.put("b", "k", &composed)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let v = Table::open_readonly(txn)?;
            let got = v.get("b", "k", 4)?.expect("the row round-trips");
            assert_eq!(got, composed, "the etag element survives the encode/decode");
            assert_eq!(got.etag().as_str(), "065947336a2f2a95ba8899f3675c3be6-2");
            let (_, latest) = v.latest("b", "k")?.expect("present");
            assert_eq!(latest.etag(), got.etag());
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn the_bounded_newest_read_answers_only_the_top_of_the_key() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut v = Table::open(txn)?;
            for seq in [1u64, 2, 3, 4, 5] {
                v.put("b", "k", &row(&format!("v{seq}"), seq))?;
            }
            v.put("b", "other", &row("v9", 9))?;
            // The newest row is corrupt: the bounded read must keep walking
            // (like `for_key`) instead of counting it toward the limit.
            v.insert(
                ("b", "k", 6),
                ("v6", "garbage", 1u64, "", 0, 0, "object", "", "", "", ""),
            )
            .map_err(Error::from)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let v = Table::open_readonly(txn)?;
            let top: Vec<u64> = v.newest("b", "k", 2)?.iter().map(|(seq, _)| *seq).collect();
            assert_eq!(top, [5, 4], "the two newest decodable rows, newest first");
            let one: Vec<u64> = v.newest("b", "k", 1)?.iter().map(|(seq, _)| *seq).collect();
            assert_eq!(one, [5]);
            assert!(
                v.newest("b", "k", 0)?.is_empty(),
                "a zero bound reads nothing"
            );
            assert_eq!(
                v.newest("b", "k", usize::MAX)?.len(),
                5,
                "a generous bound is the whole decodable history"
            );
            assert!(
                v.newest("b", "nope", 2)?.is_empty(),
                "another key's rows never ride the range"
            );
            assert_eq!(
                v.newest("b", "other", 2)?.len(),
                1,
                "the prefix-adjacent key is its own range"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn the_history_predicate_reads_the_stamp_and_the_slots_sequence() {
        let mut row = row("v1", 4);
        // The supersede's row: committed before the slot moves off it.
        assert!(
            !row.is_history(Slot::At(4)),
            "the current slot's own row is not history"
        );
        assert!(
            row.is_history(Slot::At(9)),
            "a row the slot has moved off is history"
        );
        assert!(
            row.is_history(Slot::Gone),
            "a key with no current version has no current version's row"
        );
        assert!(
            !row.is_history(Slot::Unknown),
            "with no slot in hand the stamp is the whole answer"
        );
        row.became_noncurrent = 7;
        assert!(
            row.is_history(Slot::At(4)),
            "a stamped row is history whatever the slot says"
        );
        assert!(row.is_history(Slot::Unknown));
    }

    #[test]
    fn stamping_a_row_marks_it_history_and_leaves_everything_else() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?.put("b", "k", &row("v1", 4))?;
            Ok(())
        })
        .unwrap();
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?.stamp_noncurrent("b", "k", 4, 9)?;
            // A sequence that holds no row has nothing to stamp: `Ok`.
            Table::open(txn)?.stamp_noncurrent("b", "k", 5, 9)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let v = Table::open_readonly(txn)?;
            let got = v.get("b", "k", 4)?.expect("the row stays");
            assert_eq!(got.became_noncurrent, 9);
            assert!(got.is_history(Slot::Unknown));
            assert_eq!(got.id, "v1", "only the stamp changed");
            assert_eq!(got.seq, 4);
            assert!(v.get("b", "k", 5)?.is_none(), "no row, nothing stamped");
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn the_bounded_walk_starts_at_the_bound_and_stops_where_it_is_told() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut v = Table::open(txn)?;
            for (key, seq) in [("a", 1u64), ("b", 1), ("b", 2), ("c", 1)] {
                v.put("data", key, &row(&format!("v{seq}"), seq))?;
            }
            // The next bucket's rows sort after every `data` row: the
            // bounded walk must never leave its own interval.
            v.put("dataz", "a", &row("v1", 1))?;
            Ok(())
        })
        .unwrap();

        let walk = |from: Bound<&str>, stop_after: usize| -> Vec<(String, u64)> {
            h.read(|txn| -> Result<Vec<(String, u64)>, Error> {
                let v = Table::open_readonly(txn)?;
                let mut seen = Vec::new();
                v.for_bucket_from("data", from, |key, row| {
                    seen.push((key.to_owned(), row.seq));
                    Ok(seen.len() < stop_after)
                })?;
                Ok(seen)
            })
            .unwrap()
        };

        assert_eq!(
            walk(Bound::Unbounded, usize::MAX),
            [
                ("a".to_owned(), 1),
                ("b".to_owned(), 1),
                ("b".to_owned(), 2),
                ("c".to_owned(), 1),
            ],
            "the bucket's rows only, key order, sequence ascending"
        );
        // The bound names a KEY: an inclusive bound reads that key from its
        // lowest sequence, an exclusive one skips the key whole.
        assert_eq!(
            walk(Bound::Included("b"), usize::MAX),
            [
                ("b".to_owned(), 1),
                ("b".to_owned(), 2),
                ("c".to_owned(), 1),
            ],
            "an inclusive bound names the first key, its own sequences all in"
        );
        assert_eq!(
            walk(Bound::Excluded("b"), usize::MAX),
            [("c".to_owned(), 1)],
            "an exclusive bound never reads the named key's rows"
        );
        // The early stop ends the walk at the row it answered `false` on.
        assert_eq!(
            walk(Bound::Unbounded, 2),
            [("a".to_owned(), 1), ("b".to_owned(), 1)]
        );
        assert_eq!(
            walk(Bound::Excluded("c"), usize::MAX),
            [],
            "a bound past the bucket's last key ends immediately"
        );
    }

    #[test]
    fn a_row_with_a_corrupt_identity_reads_as_missing() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut v = Table::open(txn)?;
            let mut bad = row("v1", 1);
            bad.id = String::new();
            v.put("b", "k", &bad)?;
            v.put("b", "k2", &row("v2", 1))?;
            Ok(())
        })
        .unwrap();

        let missing = h
            .read(|txn| -> Result<Option<Row>, Error> {
                Table::open_readonly(txn)?.get("b", "k", 1)
            })
            .unwrap();
        assert!(
            missing.is_none(),
            "a corrupt id must read as a missing row, never as the null version"
        );
        let valid = h
            .read(|txn| -> Result<Option<Row>, Error> {
                Table::open_readonly(txn)?.get("b", "k2", 1)
            })
            .unwrap();
        assert_eq!(valid.unwrap().id.as_str(), "v2");
    }
}
