//! The version rows and the bucket's versioning state — the standalone
//! [`VersionMeta`] contract (never a supertrait of
//! [`Metadata`](super::Metadata), D20).
//!
//! The stored version key is `(bucket, key, seq)` and its value is the
//! eleven-element tuple the versioning set left: `(id wire, encoded blob, size,
//! etag wire, created unix nanos, became-noncurrent unix nanos, kind wire, tags
//! wire, checksum wire, owner wire, acl grants wire)`. [`Stored`] is the
//! decoded form — the id is an `Option<version::Id>` (the literal `null` is
//! the null version) and the blob a decoded [`BlobRef`].
//!
//! The key's **current slot** is the current version's identity and
//! delete-marker state, answered by [`VersionMeta::current_slot`] as a
//! [`Current`] — never a `(id, seq)` tuple (round-2 item 3: the null version
//! and a delete marker are both legitimate currents). It is normally the newest
//! version row's identity; a restore moves it back to a real identity
//! ([`VersionMeta::restore_slot`], the one move [`VersionMeta::advance_slot`]
//! refuses). It is version-domain state, so it rides neither the object row
//! (D37) nor a version element of its own — the backend records it where its
//! own slot facts live. A row's **sequence** is its key's third element, and
//! the rule that turns a row into history is [`Stored::is_history`], whose
//! [`Slot`] input is the caller's slot fact.
//!
//! The blob codec here is the port of the table crate's `encode_blob`/
//! `decode_blob` (R3: tinio-core may not depend on the table crate, so the
//! helpers are ported, and the table crate's own copies retire as its call
//! sites switch over).

use std::{error::Error as StdError, time::SystemTime};

use async_trait::async_trait;

use super::bucket as bucket_meta;
use crate::{
    ETag,
    acl::{Acl, OwnerId},
    blob::{BlobRef, BlockHash, BlockRef},
    bucket, checksum, object,
    storage::{self, from_nanos},
    version::{self, State},
};

/// Whether a version row is an object version or a delete marker.
///
/// `version_rows.kind` is PostgreSQL's own `version_kind` enum, so the
/// `sqlx`-gated derive below owns the mapping: the type name is the DDL's and
/// `lowercase` reproduces this type's `Display` labels.
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx",
    sqlx(type_name = "version_kind", rename_all = "lowercase")
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display)]
pub enum Kind {
    /// A stored object version: the row's blob is the version's content.
    #[display("object")]
    Object,
    /// A delete marker: the row's blob is the empty blob.
    #[display("marker")]
    Marker,
}

impl Kind {
    /// Read the stored wire form; a wire outside the two names is no kind, so
    /// its row reads as absent.
    pub fn from_wire(wire: &str) -> Option<Self> {
        match wire {
            "object" => Some(Self::Object),
            "marker" => Some(Self::Marker),
            _ => None,
        }
    }

    /// The stored wire form.
    pub fn as_wire(self) -> &'static str {
        match self {
            Self::Object => "object",
            Self::Marker => "marker",
        }
    }

    /// Whether the row describes a delete marker.
    pub fn is_marker(self) -> bool {
        self == Self::Marker
    }
}

/// The key's current slot, as the contract answers it.
///
/// The **null version** and a **delete marker** are both legitimate currents,
/// so this shape can say all three states: no current at all (`None`), a
/// current that is the null version (`Some` with `version: None`), and a
/// current that is a specific version or a marker. It carries **no sequence**
/// (round-2 item 3): the sequence is version-domain state the backend reads
/// internally, and a caller that needs it asks [`VersionMeta::list_versions`]
/// or [`VersionMeta::version_seq_of`]. The backend's **gated** view — the
/// F01/F04 staleness rule a read applies — stays the backend's own port; this is
/// the rows' raw fact.
///
/// # Examples
///
/// ```rust
/// use tinio_core::meta::version::Current;
///
/// let absent = Current::default();
/// assert!(!absent.exists);
///
/// let null = Current {
///     exists: true,
///     ..absent
/// };
/// assert!(
///     null.is_null(),
///     "a current with no identity is the null version"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Current {
    /// Whether the key has a current version at all.
    pub exists: bool,
    /// The current version's identity (`None` = the null version).
    pub version: Option<version::Id>,
    /// Whether the current version is a delete marker (which holds no
    /// content).
    pub is_delete_marker: bool,
}

/// What a caller knows about a key's **current slot** when it asks
/// [`Stored::is_history`]. The three states are not interchangeable: a row that
/// is not at the slot's sequence is history whatever its stamp says, so a
/// caller that knows the sequence and one that knows the key has no current
/// version at all must not be read as "no fact at all" — this is the ported
/// table layer's rule (R3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Slot {
    /// The key's current version sits at this sequence — the sequence the
    /// slot records, or the one the served version was written with.
    At(u64),
    /// The key has **no** current version at all: no slot exists, or the
    /// slot's content is gone out of band while the row still described it, so
    /// the key serves nothing and every version row is history.
    Gone,
    /// The caller holds no slot fact (a whole-table walk with no per-key
    /// view): the stamp alone answers, and an unstamped row is the current
    /// version's own.
    Unknown,
}

impl Current {
    /// Whether the current slot is the null version: something is recorded,
    /// but its identity is the absence of an id.
    pub fn is_null(&self) -> bool {
        self.exists && self.version.is_none() && !self.is_delete_marker
    }
}

/// Whether moving a key's current slot to `seq` under `incoming` is a
/// **backwards move within one identity** — the one rule every slot writer
/// applies before it records a move.
///
/// A slot records the newest version it has been moved to, and the write
/// transactions that commit such a move are serialised but not ordered by
/// sequence: a lower-sequence advance of a real version committing after a
/// higher-sequence one would leave the slot below the newest version it
/// already named, which is the state the slot exists to prevent. A move to the
/// **same** sequence is the crash window's own re-run, so it is not one.
///
/// The null version is not part of that numbering. Its `seq = 0` is its own
/// space, so a suspended bucket's in-place write — the plan's `(Null, 0)` over
/// a real current version — is not a rewind of it; only a real incoming
/// identity is measured against the recorded sequence.
pub fn slot_move_rewinds(incoming: Option<&version::Id>, seq: u64, recorded: u64) -> bool {
    incoming.is_some() && seq < recorded
}

/// A decoded version row. `seq` is the row key's third element, so a row
/// always carries the sequence it is stored at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    /// The version's identifier (`None` = the null version).
    pub id: Option<version::Id>,
    /// The version's content, decoded from the row's blob element.
    pub blob: BlobRef,
    /// The version's content length in bytes (`0` for a delete marker).
    pub size: u64,
    /// The recorded ETag (the empty-content ETag for a row that recorded
    /// none).
    pub etag: ETag,
    /// When the version was created.
    pub created: SystemTime,
    /// When the version became noncurrent (`0` = not history yet).
    pub became_noncurrent: u64,
    /// Whether the row is an object version or a delete marker.
    pub kind: Kind,
    /// The version's tag set (empty when none).
    pub tags: object::Tags,
    /// The version's recorded checksum (`None` when none).
    pub checksum: Option<checksum::Recorded>,
    /// The version's owner element (`None` = the empty owner wire).
    pub owner: Option<OwnerId>,
    /// The version's grant set.
    pub acl: Acl,
    /// The per-key sequence the row is stored at.
    pub seq: u64,
}

impl Stored {
    /// Decode one stored value tuple, with the key's `seq`.
    ///
    /// `None` is a domain-invalid blob, kind or identifier element: what the
    /// row *is* and where its content lives cannot be guessed, so the row reads
    /// as absent (self-healing). A corrupt identifier must never heal to the
    /// **null version** — a meaningful value the index does not key — exactly
    /// as the landed table layer rules. The metadata wires self-heal
    /// individually.
    #[expect(
        clippy::too_many_arguments,
        reason = "one argument per element of the value tuple the row decodes from — the tuple is the storage wire shape, so the arity is the row's, not the function's"
    )]
    pub fn from_raw(
        seq: u64,
        id: &str,
        blob: &str,
        size: u64,
        etag: &str,
        created: u64,
        became_noncurrent: u64,
        kind: &str,
        tags: &str,
        checksum: &str,
        owner: &str,
        grants: &str,
    ) -> Option<Self> {
        let blob = decode_blob(blob)?;
        if id != "null" && version::Id::new(id).is_err() {
            return None;
        }
        Some(Self {
            id: version::Id::from_wire(id),
            blob,
            size,
            etag: ETag::new(etag).unwrap_or(ETag::EMPTY),
            created: from_nanos(created),
            became_noncurrent,
            kind: Kind::from_wire(kind)?,
            tags: object::Tags::from_wire_limited(tags, object::OBJECT_TAGS_MAX),
            checksum: checksum::Recorded::from_wire_opt(checksum),
            owner: bucket_meta::decode_owner(owner),
            acl: bucket_meta::decode_acl(grants),
            seq,
        })
    }

    /// Decode one stored value tuple from the contract's own values.
    ///
    /// `None` is a domain-invalid blob or identifier element (what the row is
    /// and where its content lives cannot be guessed, so it reads as absent);
    /// the `kind` is the contract's own and the tags and ACL arrive whole
    /// (the ACL carrying the owner, which the row splits out through
    /// [`Acl::into_parts`]).
    #[expect(
        clippy::too_many_arguments,
        reason = "one argument per element of the contract's value tuple the row decodes from — the tuple is the storage wire shape, so the arity is the row's, not the function's"
    )]
    pub fn from_values(
        seq: u64,
        id: &str,
        blob: &str,
        size: u64,
        etag: &str,
        created: SystemTime,
        became_noncurrent: u64,
        kind: Kind,
        tags: object::Tags,
        checksum: &str,
        acl: Acl,
    ) -> Option<Self> {
        let blob = decode_blob(blob)?;
        if id != "null" && version::Id::new(id).is_err() {
            return None;
        }
        let (owner, grants) = acl.into_parts();
        Some(Self {
            id: version::Id::from_wire(id),
            blob,
            size,
            etag: ETag::new(etag).unwrap_or(ETag::EMPTY),
            created,
            became_noncurrent,
            kind,
            tags,
            checksum: checksum::Recorded::from_wire_opt(checksum),
            owner,
            acl: Acl::from_parts(None, grants),
            seq,
        })
    }

    /// Encode the row back into its value tuple's elements.
    pub fn to_raw(
        &self,
    ) -> (
        String,
        String,
        u64,
        String,
        u64,
        u64,
        String,
        String,
        String,
        String,
        String,
    ) {
        (
            version::wire_of(self.id.as_ref()),
            encode_blob(&self.blob),
            self.size,
            self.etag.as_str().to_owned(),
            storage::to_nanos(self.created),
            self.became_noncurrent,
            self.kind.as_wire().to_owned(),
            self.tags.to_wire(),
            self.checksum
                .as_ref()
                .map(checksum::Recorded::to_wire)
                .unwrap_or_default(),
            bucket_meta::encode_owner(self.owner.as_ref()),
            bucket_meta::encode_grants(&self.acl),
        )
    }

    /// Whether the row is a **history** version rather than the current
    /// version's own row: the stamp is authoritative, and a row at another
    /// sequence than the key's current slot is history whatever its stamp says.
    /// `slot` is what the caller knows about the key's current slot — see
    /// [`Slot`].
    pub fn is_history(&self, slot: Slot) -> bool {
        match slot {
            Slot::At(seq) => self.became_noncurrent != 0 || seq != self.seq,
            Slot::Gone => true,
            Slot::Unknown => self.became_noncurrent != 0,
        }
    }
}

/// Encode a blob for the row's blob element — [`decode_blob`]'s exact inverse:
/// `<content-hash-hex>:<total-len>:<block-hash-hex>:<block-len>[,<block-hash-hex>:<block-len>…]`.
/// The block list is written in content order, so a chunked blob round-trips
/// unchanged.
///
/// # Examples
///
/// ```rust
/// use tinio_core::{
///     blob::{BlobRef, BlockHash},
///     meta::version::{decode_blob, encode_blob},
/// };
///
/// let blob = BlobRef::single(BlockHash::of(b"hello"), 5);
/// let wire = encode_blob(&blob);
/// assert_eq!(
///     wire,
///     format!("{}:5:{}:5", blob.hash().to_hex(), blob.hash().to_hex())
/// );
/// assert_eq!(decode_blob(&wire), Some(blob));
/// ```
pub fn encode_blob(blob: &BlobRef) -> String {
    let mut out = format!("{}:{}", blob.hash().to_hex(), blob.len());
    for (i, block) in blob.blocks().iter().enumerate() {
        out.push(if i == 0 { ':' } else { ',' });
        out.push_str(&block.hash.to_hex());
        out.push(':');
        out.push_str(&block.len.to_string());
    }
    out
}

/// Decode the row's blob element, re-checking [`BlobRef::new`]'s invariants
/// rather than trusting the stored bytes: at least one block, the declared
/// length equal to the blocks' (non-overflowing) sum, and a zero-length block
/// only as the list's sole entry. `None` is any wire that is not a valid
/// encoded blob — the caller treats such a row as missing, so a decoded row is
/// never an invalid [`BlobRef`].
pub fn decode_blob(wire: &str) -> Option<BlobRef> {
    let (hash, rest) = wire.split_once(':')?;
    let (len, rest) = rest.split_once(':')?;
    let hash = BlockHash::new(hash).ok()?;
    let len = len.parse::<u64>().ok()?;
    let mut blocks = Vec::new();
    for entry in rest.split(',') {
        let (block_hash, block_len) = entry.split_once(':')?;
        blocks.push(BlockRef {
            hash: BlockHash::new(block_hash).ok()?,
            len: block_len.parse::<u64>().ok()?,
        });
    }
    BlobRef::new(hash, len, blocks).ok()
}

/// The version rows, the bucket's versioning state and the key's current slot.
///
/// # Examples
///
/// ```ignore
/// use tinio_core::{bucket, meta::{Metadata, VersionMeta, version::Current}, object, version::State};
///
/// async fn versioned<M: VersionMeta + Metadata>(store: &M, bucket: &bucket::Name) {
///     store.set_bucket_state(bucket, State::Enabled).await.unwrap();
///     assert!(store.bucket_state(bucket).await.unwrap().records_versions());
///     let key = object::key("a.txt").unwrap();
///     let _slot: Option<Current> = store.current_slot(bucket, &key).await.unwrap();
/// }
/// ```
#[async_trait]
pub trait VersionMeta: Send + Sync + 'static {
    /// This backend's version-row error; it must project into
    /// [`storage::Error`].
    type Error: StdError + Send + Sync + 'static + Into<storage::Error>;

    /// The bucket's versioning state; a bucket with no row is
    /// [`State::Unversioned`].
    async fn bucket_state(&self, bucket: &bucket::Name) -> Result<State, Self::Error>;

    /// Record the bucket's versioning state. The caller owns the transition
    /// rule (a versioned bucket may be suspended but never returns to
    /// `Unversioned`).
    async fn set_bucket_state(
        &self,
        bucket: &bucket::Name,
        state: State,
    ) -> Result<(), Self::Error>;

    /// Record the bucket's versioning state **only when the bucket row
    /// exists**: the check runs inside the write transaction, so a concurrent
    /// [`BucketMeta::remove_if_empty`](crate::meta::BucketMeta::remove_if_empty)
    /// cannot be raced by a state write, and the refusal projects to
    /// [`crate::storage::Error::NoSuchBucket`].
    ///
    /// [`set_bucket_state`](VersionMeta::set_bucket_state) stays unguarded — a
    /// bucket plane whose rows materialize lazily would refuse a bucket it has
    /// never written a row for — so the guard is opt-in and the default is the
    /// plain write.
    async fn set_bucket_state_guarded(
        &self,
        bucket: &bucket::Name,
        state: State,
    ) -> Result<(), Self::Error> {
        VersionMeta::set_bucket_state(self, bucket, state).await
    }

    /// The key's current slot, or `None` when the key has **nothing recorded** —
    /// no slot state, so no identity and no version row to resolve a kind from.
    /// A key whose slot is recorded always answers `Some`, and the returned
    /// [`Current`] resolves the slot's two facts from the version row at its
    /// sequence, the contract's own [`VersionMeta::put_version`] and
    /// [`VersionMeta::get_version`] surface:
    ///
    /// - `exists` is `true` (a slot is recorded);
    /// - `version` is the recorded identity (`None` is the null version);
    /// - `is_delete_marker` is `true` exactly when the row at the slot's
    ///   sequence is a marker, **or** the slot's own record carries the bit a
    ///   restore wrote ([`VersionMeta::restore_slot`] consumes the row it
    ///   derived the bit from, so that row alone cannot answer it).
    ///
    /// A slot advanced to a sequence with no version row — no foreign key binds
    /// the two, so a row can be expired out of band — is still `Some`: its
    /// identity is the recorded fact, and a slot that records no marker bit
    /// answers `false`, because a missing row is not a marker.
    ///
    /// The read is **not** gated: [`Metadata::get`](super::ObjectMeta::get)
    /// stays the caller's current view (the backend's F01/F04 rule), and the
    /// current slot is the rows' own fact (D37) — see [`Current`] for why the
    /// null version and a delete marker need the three-state shape.
    async fn current_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<Current>, Self::Error>;

    /// Move the key's current slot to `(id, seq)` in one transaction.
    ///
    /// **This is the only forward writer of a key's current slot**: the sole
    /// other writer is [`VersionMeta::restore_slot`], the restore's commit,
    /// whose move is backwards to a real identity by definition — this call's
    /// guard is absolute and stays that way. Beside the delete-marker path it
    /// is the only caller allowed to move the version identity the object row's
    /// slot columns record: the ordinary object-row writers
    /// ([`ObjectMeta::put`](super::ObjectMeta::put),
    /// [`ObjectMeta::put_batch`](super::ObjectMeta::put_batch),
    /// [`ObjectMeta::put_preserving`](super::ObjectMeta::put_preserving))
    /// **preserve** the stored version columns, so a content rewrite can never
    /// silently un-version an object.
    ///
    /// The slot is a **version-row fact**, not a kind this call carries:
    /// `(id, seq)` names the row [`VersionMeta::current_slot`] resolves the
    /// identity and the marker bit from, so advancing to a marker's sequence
    /// records the marker without a flag. A sequence with no row is legal — the
    /// slot is the recorded fact — and reads back with `is_delete_marker:
    /// false` (a restore's marker is the one slot whose bit outlives its row).
    ///
    /// **A slot never moves backwards — within one identity.** The
    /// implementation compares `seq` against the sequence the slot already
    /// records and **refuses** a lower one **when the incoming identity is a
    /// version** (`Some`), projecting the refusal into [`storage::Error`] the
    /// way [`VersionMeta::put_version`]'s same-sequence conflict is projected;
    /// a move to the **same** sequence is the crash window's own re-run and is
    /// allowed. An incoming **`None`** (the null version) is admitted in place
    /// whatever the sequence: the null slot's numbering is its own space, and
    /// the D37 model writes the null version by replacing the current one, so
    /// `(Some(id), n) → (None, 0)` is not a backward move (round 41, the
    /// suspended-bucket PUT). Without the guard a lower-sequence advance of the
    /// **same** identity committing after a higher-sequence one would leave the
    /// slot below the newest version it already named, so
    /// [`VersionMeta::current_slot`] would resolve the marker bit off the stale
    /// row — the state the slot exists to prevent.
    ///
    /// **The row the slot moves off is stamped [`Stored::became_noncurrent`]**
    /// in the same commit, under the two exemptions
    /// [`VersionMeta::restore_slot`] names: a move at the **same** sequence
    /// moves off nothing, and a sequence holding no row has nothing to stamp.
    /// A row that already carries a stamp keeps it (the first stamp wins). The
    /// retention passes age a noncurrent version from that stamp, so a backend
    /// that skipped it would leave the row younger than it is.
    ///
    /// A key with **no** `object_meta` row gets one materialized to carry the
    /// slot columns, its content-derived elements (ETag, size, mtime, file
    /// identity) starting empty — the deliberate rule, not an accident, and it
    /// holds **within a bucket that has its own row**: an implementation that
    /// guards its writers refuses a bucket with no row rather than
    /// materializing state for it. A
    /// recorded slot is object-row state whether or not content sits behind it
    /// (a **delete marker** is exactly this: a recorded slot with no bytes), so
    /// an object-row read ([`ObjectMeta::get`](super::ObjectMeta::get),
    /// [`ObjectMeta::walk`](super::ObjectMeta::walk)) then reports a recorded
    /// object for a key whose content side holds nothing. The fabricated
    /// elements never outlive the first real write: the ordinary object-row
    /// writer ([`ObjectMeta::put`](super::ObjectMeta::put)) replaces the row
    /// wholesale.
    async fn advance_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<version::Id>,
        seq: u64,
    ) -> Result<(), Self::Error>;

    /// Make a version current again — the **restore's commit**, in one
    /// transaction: the object row the restored version becomes, the slot moved
    /// back to the version's own sequence, the version row and the index entry
    /// the restore consumes, and the key's cleared retained part list.
    ///
    /// The slot move is deliberate and is the one move
    /// [`VersionMeta::advance_slot`] refuses: a restore moves the slot
    /// **backwards to a real identity**, so this call applies **no monotonic
    /// guard** — the guard stays absolute for [`VersionMeta::advance_slot`],
    /// because the exception is the call, never a parameter on the shared one.
    /// Everything below commits together or not at all:
    ///
    /// - **The bucket row is asserted inside this transaction.** A bucket with
    ///   no row refuses, projecting to [`storage::Error::NoSuchBucket`] (the
    ///   guarded-writer precedent: [`ObjectMeta::put_guarded`](super::ObjectMeta::put_guarded),
    ///   [`ObjectMeta::put_with_parts`](super::ObjectMeta::put_with_parts),
    ///   [`VersionMeta::advance_slot`]).
    /// - **The version row at `seq` is this call's premise.** It must exist and
    ///   its identity must be `id`'s (`None` is the null version), or the call
    ///   refuses with the implementation's own error, projecting into
    ///   [`storage::Error`] — never `Ok`, never a panic (the
    ///   [`VersionMeta::put_version`] same-sequence conflict's precedent). Its
    ///   [`Kind`] is what the written object row's marker bit records: the
    ///   caller never carries a kind, because the slot is a version-row fact.
    /// - **The object row is written wholesale**: the caller's content and
    ///   recorded elements (a [`super::object::Row`]), the slot columns naming
    ///   `(id, seq)`, and the derived marker bit. The write **clears the key's
    ///   retained part list** in the same transaction (the
    ///   [`ObjectMeta::put`](super::ObjectMeta::put) rule — an overwrite is a
    ///   fresh object).
    /// - **The consumed row at `seq` and its index entry are removed** in the
    ///   same transaction: the restored version is the current version, and a
    ///   current version has no history row of its own — leaving one would make
    ///   it both current and addressable as history.
    /// - **The row the slot moves off is stamped [`Stored::became_noncurrent`]**
    ///   in the same commit, under the rule and the two exemptions
    ///   [`VersionMeta::advance_slot`] applies: a move at the **same** sequence
    ///   moves off nothing, and a sequence holding no row has nothing to stamp.
    ///   A row that already carries a stamp keeps it (the first stamp wins).
    /// - **The marker bit survives the row it came from**:
    ///   [`VersionMeta::current_slot`] answers `is_delete_marker: true` after a
    ///   restored marker, because the bit is recorded where the slot's facts
    ///   live rather than derived again from the row this call consumed.
    ///
    /// The call is **not idempotent**, and that is deliberate: it is written
    /// once per restore, not re-run by a crash-window adoption path — a second
    /// run finds the row consumed and refuses.
    ///
    /// What the call deliberately does not do: it **publishes nothing** (the
    /// content half stays the caller's — the version's bytes are streamed back
    /// onto the key path **before** this call, so a crash between the two
    /// leaves the version's row, and so its content, recoverable); it **writes
    /// no part list** (it clears it); it touches no upload records; it takes no
    /// `backwards: bool`; it **does not decide which version becomes current**
    /// (the successor rule is [`Stored::is_history`] over the caller's own slot
    /// fact, and the scan is the caller's); and it **does not create the
    /// version's row when absent** — it consumes it.
    async fn restore_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &super::object::Row,
        id: Option<version::Id>,
        seq: u64,
    ) -> Result<(), Self::Error>;

    /// Upsert one version row at `row.seq` — the row and its index entry
    /// committing together, or not at all: the index is keyed by the row's
    /// identity wire, so the two must never disagree.
    ///
    /// Two rules bind every implementation, because each protects a row the
    /// index still names:
    ///
    /// - **A row at `row.seq` naming a different version is refused**, never
    ///   overwritten — a different identifier or kind, or the same identifier
    ///   and kind naming different content (the blob and the byte count: the
    ///   identity wire alone cannot separate two null-id rows). Overwriting it
    ///   would replace another version's content with this one's and drop that
    ///   version's blob out of the live set for the sweep: silent,
    ///   unrecoverable loss. The *same* version's row is rewritten (the
    ///   crash-window adoption path), **preserving the stored
    ///   [`Stored::became_noncurrent`] stamp**: whenever a row already sits at
    ///   `row.seq` the stamp it carries is the authority and the caller's value
    ///   is **ignored**, so a stamped row is never un-stamped by the rewrite
    ///   that adopts it. A sequence with no row stores the caller's stamp.
    /// - **Committing the null identity displaces any other null-id row of the
    ///   key** — removed with its index entry in the same transaction. The
    ///   index can name only one of two rows sharing the `null` wire, so a
    ///   second null-id row would be permanently unaddressable, and would stay
    ///   unaddressable after the row it displaced expired. A key therefore
    ///   holds at most one `null`-id row (AWS's one-null-version model).
    ///
    /// Both rules are the store's, not the caller's: the caller hands the whole
    /// row and the implementation enforces them inside its own transaction. The
    /// refusal's error is the implementation's own, projecting into
    /// [`storage::Error`].
    async fn put_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &Stored,
    ) -> Result<(), Self::Error>;

    /// One version row, or `None` when it is absent or its shape is corrupt.
    async fn get_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<Option<Stored>, Self::Error>;

    /// The key's newest version row — its sequence and row — or `None` when
    /// the key has none.
    ///
    /// The read is the key's **highest sequence**, never a walk of its
    /// history: the versioned read path's one-row read. A newest row whose
    /// shape is corrupt (its blob, kind or identifier element) **fails
    /// closed**: the answer is `None` — the key reads as having no stored
    /// version — rather than an older decodable row being handed back as if it
    /// were current, so a versioned read never serves stale content as the
    /// latest. The metadata wires self-heal, as everywhere.
    async fn latest(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<(u64, Stored)>, Self::Error>;

    /// Every **decodable** version row the bucket holds, in no particular
    /// order — the sweep's live-block read (liveness is derived from what the
    /// rows name, never counted).
    ///
    /// The walk is **healing**: a row whose shape is corrupt is skipped, never
    /// fails the walk — it names no blob, so it keeps no block alive. The
    /// strict walk is [`ObjectMeta::walk`](super::ObjectMeta::walk)'s; this is
    /// the row half's healing read.
    async fn bucket_rows(&self, bucket: &bucket::Name) -> Result<Vec<Stored>, Self::Error>;

    /// The key's version rows, newest first.
    async fn list_versions(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Vec<Stored>, Self::Error>;

    /// Remove the row at `seq` and its index entry in one transaction
    /// (idempotent).
    async fn expire(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<(), Self::Error>;

    /// Remove the rows at `seqs` and their index entries in **one**
    /// transaction — the retention sweep's shape.
    ///
    /// The batch is the unit, not a convenience: a pass retires a whole
    /// history, and a transaction per version would be a durable commit — and
    /// so a crash point — per version, leaving a half-retired history behind.
    /// Idempotent like [`Self::expire`], and with the same all-or-nothing
    /// unit: a crash leaves every listed row or none of them. A sequence with
    /// no row is skipped, never an error.
    async fn expire_many(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seqs: &[u64],
    ) -> Result<(), Self::Error>;

    /// Remove **every** version row the bucket holds, with its version-index
    /// entries, in one transaction — the whole-bucket drain a bucket teardown
    /// needs.
    ///
    /// The bucket is the unit, exactly as [`Self::expire_many`]'s listed
    /// history is for one key: a row and its index entry retire together, so
    /// the index can never be left naming a row the drain removed. Idempotent
    /// like [`Self::expire`] and [`Self::expire_many`] — re-running it removes
    /// nothing — and a bucket that holds no version row answers `Ok`, never an
    /// error.
    ///
    /// Only the version rows are this call's: the bucket's versioning state is
    /// [`Self::bucket_state`]'s own row, which the caller that tears a bucket
    /// down retires separately.
    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), Self::Error>;

    /// The sequence the version `id` names under `key` — the point-lookup
    /// index read, `None` when the index has no such entry.
    async fn version_seq_of(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<&version::Id>,
    ) -> Result<Option<u64>, Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob;

    fn row(id: &str, seq: u64) -> Stored {
        Stored {
            id: version::Id::from_wire(id),
            blob: BlobRef::single(blob::BlockHash::of(b"x"), 1),
            size: 1,
            etag: ETag::EMPTY,
            created: from_nanos(0),
            became_noncurrent: 0,
            kind: Kind::Object,
            tags: object::Tags::empty(),
            checksum: None,
            owner: None,
            acl: Acl::default_private(None),
            seq,
        }
    }

    #[test]
    fn a_version_row_round_trips_through_its_wires() {
        let row = row("v1", 3);
        let (id, blob, size, etag, created, became, kind, tags, checksum, owner, grants) =
            row.to_raw();
        assert_eq!(
            Stored::from_raw(
                row.seq, &id, &blob, size, &etag, created, became, &kind, &tags, &checksum, &owner,
                &grants
            ),
            Some(row)
        );
    }

    #[test]
    fn the_null_identity_encodes_as_the_literal_and_decodes_as_none() {
        let null = row("null", 0);
        let (id, ..) = null.to_raw();
        assert_eq!(id, "null", "the literal is the null version's wire");
        assert_ne!(id, "", "never the empty-string sentinel");
        assert_eq!(null.id, None);
    }

    #[test]
    fn a_corrupt_blob_kind_or_identifier_makes_the_row_read_as_absent() {
        let good = row("v1", 1);
        let (id, blob, size, etag, created, became, kind, tags, checksum, owner, grants) =
            good.to_raw();
        // A corrupt blob: the row cannot say where its content lives.
        assert_eq!(
            Stored::from_raw(
                1, &id, "garbage", size, &etag, created, became, &kind, &tags, &checksum, &owner,
                &grants
            ),
            None
        );
        // A corrupt kind: the row cannot say what it is.
        assert_eq!(
            Stored::from_raw(
                1, &id, &blob, size, &etag, created, became, "nope", &tags, &checksum, &owner,
                &grants
            ),
            None
        );
        // A corrupt identifier must not heal to the null version.
        assert_eq!(
            Stored::from_raw(
                1, "", &blob, size, &etag, created, became, &kind, &tags, &checksum, &owner,
                &grants
            ),
            None
        );
        // The metadata wires heal individually and the row still serves.
        let healed = Stored::from_raw(
            1,
            &id,
            &blob,
            size,
            &etag,
            created,
            became,
            &kind,
            "team=%zz&",
            "CRC32:@@:NOPE",
            "bad-owner",
            "id=short,READ",
        )
        .expect("the shape elements are valid");
        assert!(healed.tags.is_empty());
        assert_eq!(healed.checksum, None);
        assert_eq!(healed.owner, None);
        assert!(healed.acl.grants.is_empty());
        assert_eq!(healed.size, size, "the content elements ride untouched");
    }

    #[test]
    fn the_current_slot_shape_tells_the_null_version_from_a_marker() {
        let absent = Current::default();
        assert!(!absent.exists);
        assert!(
            !absent.is_null(),
            "no current at all is not the null version"
        );
        let null = Current {
            exists: true,
            ..absent
        };
        assert!(null.is_null());
        let marker = Current {
            is_delete_marker: true,
            ..null.clone()
        };
        assert!(!marker.is_null(), "a delete marker is not the null version");
        let named = Current {
            version: version::Id::from_wire("v1"),
            ..null.clone()
        };
        assert!(!named.is_null());
    }

    #[test]
    fn the_blob_codec_round_trips_and_rejects_garbage() {
        let blob = BlobRef::single(blob::BlockHash::of(b"hello"), 5);
        let wire = encode_blob(&blob);
        assert_eq!(
            wire,
            format!("{}:5:{}:5", blob.hash().to_hex(), blob.hash().to_hex())
        );
        assert_eq!(decode_blob(&wire), Some(blob));
        assert_eq!(decode_blob(""), None);
        assert_eq!(decode_blob("garbage"), None);
        assert_eq!(decode_blob("not-hex:0:not-hex:0"), None);
        // A chunked blob round-trips unchanged.
        let chunked = BlobRef::new(
            blob::BlockHash::of(b"hello"),
            5,
            vec![
                BlockRef {
                    hash: blob::BlockHash::of(b"he"),
                    len: 2,
                },
                BlockRef {
                    hash: blob::BlockHash::of(b"llo"),
                    len: 3,
                },
            ],
        )
        .unwrap();
        assert_eq!(decode_blob(&encode_blob(&chunked)), Some(chunked));
    }

    #[test]
    fn a_rows_history_verdict_reads_the_slot_and_the_stamp() {
        let mut row = row("v1", 4);
        assert!(
            !row.is_history(Slot::At(4)),
            "the current slot's own row is not history"
        );
        assert!(
            row.is_history(Slot::At(5)),
            "a row the slot has moved off is history whatever its stamp says"
        );
        assert!(
            row.is_history(Slot::Gone),
            "a key with no current version has no current version's row"
        );
        assert!(
            !row.is_history(Slot::Unknown),
            "an unstamped row with no slot fact is a write the slot has not adopted"
        );
        row.became_noncurrent = 700;
        assert!(row.is_history(Slot::At(4)));
        assert!(row.is_history(Slot::Gone));
        assert!(row.is_history(Slot::Unknown));
    }
}
