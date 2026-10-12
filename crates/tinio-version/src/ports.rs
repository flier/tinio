//! The ports the engine still declares, beside the contracts it runs on.
//!
//! The engine is written against the persistence contracts
//! ([`Metadata`](tinio_core::meta::Metadata) and
//! [`VersionMeta`](tinio_core::meta::VersionMeta)) and the content contract
//! ([`ObjectContent`](tinio_core::content::ObjectContent)): the rows, the
//! current slot, the content reads and the publish are the backends' contract
//! implementations', not a private port's. Two ports stay. `CurrentRows`
//! survives because no contract counterpart can replace it — the metadata
//! contract's point read deliberately does **not** gate, while the engine must
//! only ever see the backend's **gated** current (the F01/F04 rule a read
//! applies) — so the gated slot stays a backend port. `MarkerWrite` survives
//! because a version-creating delete leaves a **slot with no version row**, a
//! state no contract write expresses: `put_version` writes a row (and a
//! null-identity one displaces the key's other null row, the history a
//! suspended `DELETE` must leave), and `advance_slot` derives the marker bit
//! from a row a marker does not have. The block plane needs no port at all:
//! the engine holds [`Store`](tinio_blob::Store) directly.
//!
//! Every port method answers [`storage::Error`] — the engine has one error
//! type, and an implementation projects its own in with `.map_err(Into::into)`.

use async_trait::async_trait;

use crate::_core::{ETag, bucket::Name, object, storage, version};

/// The key's current slot, as the engine sees it.
///
/// The implementation has already applied its own staleness gate (the
/// size/mtime/file-identity rule a read uses), so a recorded identity the
/// content no longer matches arrives as the **null** current — `exists` with
/// no version — exactly as the design's out-of-band-edit rule has it. The
/// engine therefore only ever sees gated truth.
///
/// # Examples
///
/// ```rust
/// use tinio_version::Current;
///
/// let missing = Current::default();
/// assert!(
///     !missing.exists,
///     "a key with nothing recorded has no current slot"
/// );
///
/// let null = Current {
///     exists: true,
///     version: None,
///     is_delete_marker: false,
///     seq: 0,
///     degraded: false,
/// };
/// assert!(
///     null.is_null(),
///     "a current file with no recorded identity is the null version"
/// );
///
/// let marker = Current {
///     is_delete_marker: true,
///     ..null.clone()
/// };
/// assert!(!marker.is_null(), "a delete marker is not the null version");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Current {
    /// Whether the key has a current version at all — a file or a delete
    /// marker.
    pub exists: bool,
    /// The current version's identity (`None` = the null version).
    pub version: Option<version::Id>,
    /// Whether the current version is a delete marker (which holds no
    /// content).
    pub is_delete_marker: bool,
    /// The sequence the current version already records (`0` for a key with
    /// no version, and for a null version written while the bucket was
    /// unversioned). The shared write policy records a fresh null version at
    /// sequence `0` while `0` is free and at the key's next sequence when a
    /// retired null version's row occupies it, so a nonzero sequence does not
    /// imply a non-null version.
    pub seq: u64,
    /// Whether the gate **degraded** the slot: the content a recorded
    /// identity named no longer matches, so the identity is not served and
    /// `version` is `None`. The slot then records no version the history can
    /// name — the row a supersede would write claims the null identity while
    /// the slot records none, and its `seq` is the null's canonical `0`
    /// rather than one the slot took — so a write neither moves it into
    /// history nor treats it as the recorded null version (`Current::is_null`
    /// cannot tell the two apart; this flag can). A backend whose slot and
    /// rows are one transaction never sets it; the fs backend's stale-file
    /// gate is the one that does.
    pub degraded: bool,
}

impl Current {
    /// Whether the current version is the null version: the key holds
    /// content but records no identity for it — a write from before the
    /// bucket was versioned, a suspended-bucket write, or an identity the
    /// gate could not confirm.
    pub fn is_null(&self) -> bool {
        self.exists && self.version.is_none() && !self.is_delete_marker
    }
}

/// The key's current slot, gated by the implementation.
///
/// # Examples
///
/// ```rust
/// use async_trait::async_trait;
/// use tinio_core::{bucket::Name, object, storage};
/// use tinio_version::{Current, CurrentRows};
///
/// struct Slots;
///
/// #[async_trait]
/// impl CurrentRows for Slots {
///     async fn current(&self, _: &Name, _: &object::Key) -> Result<Current, storage::Error> {
///         Ok(Current {
///             exists: true,
///             version: None,
///             is_delete_marker: false,
///             seq: 0,
///             degraded: false,
///         })
///     }
/// }
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let bucket = tinio_core::bucket::name("demo").unwrap();
/// let slot = Slots
///     .current(&bucket, &object::key("a.txt").unwrap())
///     .await
///     .unwrap();
/// assert!(
///     slot.is_null(),
///     "a file with no recorded identity is the null version"
/// );
/// # });
/// ```
#[async_trait]
pub trait CurrentRows: Send + Sync + 'static {
    /// The key's current slot, with the implementation's staleness gate
    /// already applied (a recorded identity the content no longer matches
    /// answers the null current, never a stale identifier).
    async fn current(&self, bucket: &Name, key: &object::Key) -> Result<Current, storage::Error>;
}

/// The marker write: the one write a version-creating delete needs that no
/// contract call expresses.
///
/// A delete marker is a **slot with no version row**. The contract's
/// [`put_version`](tinio_core::meta::VersionMeta::put_version) writes a version
/// row, and a **null-identity** row displaces the key's other null-id row — the
/// superseded null version a suspended `DELETE` must leave in history — while
/// [`advance_slot`](tinio_core::meta::VersionMeta::advance_slot) derives the
/// marker bit from a row a marker does not have. So the engine's
/// version-creating delete hands the marker's slot write here, and the
/// implementation records the bit, materializes the key's object row and
/// stamps the version the slot moves off, in one transaction of its own model.
///
/// # Examples
///
/// ```rust
/// use async_trait::async_trait;
/// use tinio_core::{bucket::Name, object, storage, version::Id};
/// use tinio_version::MarkerWrite;
///
/// struct Slots;
///
/// #[async_trait]
/// impl MarkerWrite for Slots {
///     async fn mark_current_marker(
///         &self,
///         _: &Name,
///         _: &object::Key,
///         _: Option<Id>,
///         _: u64,
///         _: Option<u64>,
///     ) -> Result<(), storage::Error> {
///         Ok(())
///     }
/// }
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let bucket = tinio_core::bucket::name("demo").unwrap();
/// Slots
///     .mark_current_marker(&bucket, &object::key("a.txt").unwrap(), None, 0, None)
///     .await
///     .unwrap();
/// # });
/// ```
#[async_trait]
pub trait MarkerWrite: Send + Sync + 'static {
    /// Make `id` the key's current version and record the **marker** bit: the
    /// marker's slot, the key's (empty) object row, and the stamp this move
    /// owes `moved_off` — the sequence of the version the marker supersedes,
    /// or `None` when it supersedes none.
    async fn mark_current_marker(
        &self,
        bucket: &Name,
        key: &object::Key,
        id: Option<version::Id>,
        seq: u64,
        moved_off: Option<u64>,
    ) -> Result<(), storage::Error>;
}

/// What the engine's [`materialize`](crate::Versions::materialize) left at the
/// key: the **version's recorded ETag** and the number of bytes its publish
/// streamed there.
///
/// The ETag is the row's own, never a recomputed content MD5: restoring a
/// version must not change what its ETag is, and a multipart-composed
/// `MD5-of-MD5s-N` form has no content-derived equivalent (a content MD5 over
/// the same bytes is the single-upload form). A caller that needs the published
/// bytes' own digest hashes them itself.
///
/// The count is the **landed** byte count, never the requested length — the
/// bytes the engine streamed into the staged object, which is what the write
/// path committed. A caller recording it in a row must record the landed
/// count: the read path's staleness gate compares the row against the file.
///
/// # Examples
///
/// ```rust
/// use tinio_core::ETag;
/// use tinio_version::PublishedBody;
///
/// let published = PublishedBody {
///     etag: ETag::EMPTY,
///     size: 5,
/// };
/// assert_eq!(published.size, 5);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedBody {
    /// The ETag the version's row records.
    pub etag: ETag,
    /// The number of bytes streamed to the key's content slot.
    pub size: u64,
}
