//! The narrow ports the version engine is written against.
//!
//! The engine holds no table, no path and no file handle: it reads and writes
//! versions through three traits a backend implements, so the state machine is
//! testable without a filesystem and a second backend inherits it unchanged.
//! Two of the three mirror the persistence contracts this work line is
//! heading for — `VersionRows` becomes `VersionMeta`, `ContentPort` becomes
//! `ObjectContent` — so the swap is a type substitution plus one error
//! projection at each call site. `CurrentRows` has no contract counterpart
//! (`Metadata::get` deliberately does not gate) and stays a backend port.
//!
//! Every port method answers [`storage::Error`] — the engine has one error
//! type, and an implementation projects its own error in with
//! `.map_err(Into::into)`.

use async_trait::async_trait;

use crate::{
    _core::{
        BodyStream, ETag,
        blob::BlobRef,
        bucket::Name,
        object, storage,
        version::{self, State},
    },
    _store::version::Row,
};

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

/// The outgoing version `supersede` hands to
/// [`VersionRows::put_version_row`].
///
/// The engine fills the three things it knows: the identity and sequence the
/// current slot recorded (never a freshly minted id, never a bumped
/// sequence) and the blob the block engine stored. The row's remaining
/// elements — its creation time, its tags, checksum, owner and ACL wires —
/// belong to the state the implementation already keeps for that version, so
/// the engine never handles them.
///
/// # Examples
///
/// ```rust
/// use tinio_core::blob::{BlobRef, BlockHash};
/// use tinio_version::Superseded;
///
/// let superseded = Superseded {
///     id: Some("3/L4kqtJlcpXroDTDmJrmSpXd3dIbrHY".parse().unwrap()),
///     seq: 7,
///     blob: BlobRef::single(BlockHash::of(b"first"), 5),
///     is_delete_marker: false,
/// };
/// assert_eq!(superseded.seq, 7);
/// assert_eq!(superseded.blob.len(), 5);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Superseded {
    /// The identity the key's current slot recorded (`None` = the null
    /// version).
    pub id: Option<version::Id>,
    /// The sequence that version already had.
    pub seq: u64,
    /// The content the block engine stored — the empty blob for a delete
    /// marker, which holds none.
    pub blob: BlobRef,
    /// Whether the outgoing version is a delete marker.
    pub is_delete_marker: bool,
}

/// The version rows, their point-lookup index, and the bucket's versioning
/// state — the persistence half of the state machine.
///
/// Implementations must keep each mutation atomic across the two tables: the
/// row and its index entry are one unit, or a version-addressed read resolves
/// an identity to a sequence that holds nothing.
///
/// # Examples
///
/// A minimal implementor: one bucket's state, one key's rows in a map.
///
/// ```rust
/// use std::{collections::BTreeMap, sync::Mutex};
///
/// use async_trait::async_trait;
/// use tinio_core::{bucket::Name, object, storage, version::State};
/// use tinio_store::version::Row;
/// use tinio_version::{Superseded, VersionRows};
///
/// #[derive(Default)]
/// struct Rows {
///     state: Mutex<State>,
///     rows: Mutex<BTreeMap<u64, Row>>,
/// }
///
/// #[async_trait]
/// impl VersionRows for Rows {
///     async fn state(&self, _: &Name) -> Result<State, storage::Error> {
///         Ok(*self.state.lock().unwrap())
///     }
///
///     async fn set_state(&self, _: &Name, state: State) -> Result<(), storage::Error> {
///         *self.state.lock().unwrap() = state;
///         Ok(())
///     }
///
///     async fn put_version_row(
///         &self,
///         _: &Name,
///         _: &object::Key,
///         superseded: &Superseded,
///     ) -> Result<(), storage::Error> {
///         self.rows
///             .lock()
///             .unwrap()
///             .insert(superseded.seq, row(superseded));
///         Ok(())
///     }
///
///     async fn remove_version_row(
///         &self,
///         _: &Name,
///         _: &object::Key,
///         seq: u64,
///     ) -> Result<(), storage::Error> {
///         self.rows.lock().unwrap().remove(&seq);
///         Ok(())
///     }
///
///     async fn latest(
///         &self,
///         _: &Name,
///         _: &object::Key,
///     ) -> Result<Option<(u64, Row)>, storage::Error> {
///         Ok(self
///             .rows
///             .lock()
///             .unwrap()
///             .iter()
///             .next_back()
///             .map(|(seq, row)| (*seq, row.clone())))
///     }
///
///     async fn bucket_rows(&self, _: &Name) -> Result<Vec<Row>, storage::Error> {
///         Ok(self.rows.lock().unwrap().values().cloned().collect())
///     }
/// }
/// # fn row(superseded: &Superseded) -> Row {
/// #     Row {
/// #         id: tinio_core::version::wire_of(superseded.id.as_ref()),
/// #         blob: tinio_store::version::encode_blob(&superseded.blob),
/// #         size: superseded.blob.len(),
/// #         etag: String::new(),
/// #         created: 0,
/// #         became_noncurrent: 1,
/// #         kind: tinio_store::version::Kind::Object,
/// #         tags: String::new(),
/// #         checksum: String::new(),
/// #         owner: String::new(),
/// #         acl: String::new(),
/// #         seq: superseded.seq,
/// #     }
/// # }
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// #     let rows = Rows::default();
/// #     let bucket = tinio_core::bucket::name("demo").unwrap();
/// #     assert_eq!(rows.state(&bucket).await.unwrap(), State::Unversioned);
/// #     rows.set_state(&bucket, State::Enabled).await.unwrap();
/// #     assert_eq!(rows.state(&bucket).await.unwrap(), State::Enabled);
/// # });
/// ```
#[async_trait]
pub trait VersionRows: Send + Sync + 'static {
    /// The bucket's versioning state.
    async fn state(&self, bucket: &Name) -> Result<State, storage::Error>;

    /// Record the bucket's versioning state.
    async fn set_state(&self, bucket: &Name, state: State) -> Result<(), storage::Error>;

    /// Commit `superseded` as the outgoing version of `key`: its row at
    /// `superseded.seq` and its index entry under `superseded.id` — one
    /// transaction, so a crash leaves either the whole version in history or
    /// none of it.
    ///
    /// The row is committed **before** the current slot moves off it (the
    /// design's crash-safety order), so it is not history yet: an
    /// implementation may leave `became_noncurrent` at `0` and let the
    /// write's own state transaction stamp it (the fs backend's ordering —
    /// the stamp rides the commit that records the new current version), or
    /// stamp it here when the slot move is the same transaction (mem's does).
    /// [`Row::is_history`](tinio_store::version::Row::is_history) is what
    /// every reader asks either way, so the row says what it is.
    ///
    /// A re-supersede of the same version is an upsert of the same row: the
    /// row's key is its sequence and its index entry is its identity, so the
    /// crash window between this commit and the key path's overwrite recovers
    /// by rewriting, never by minting a second identity for the same content.
    async fn put_version_row(
        &self,
        bucket: &Name,
        key: &object::Key,
        superseded: &Superseded,
    ) -> Result<(), storage::Error>;

    /// Drop the row at `seq` and its index entry — one transaction, the
    /// removal's own partner of [`Self::put_version_row`]. Idempotent: a
    /// sequence that holds no row is `Ok`.
    async fn remove_version_row(
        &self,
        bucket: &Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<(), storage::Error>;

    /// Drop every listed sequence of `key` and its index entry in **one**
    /// transaction — the retention sweep's shape, where one pass retires a
    /// whole history and a transaction per version would be a durable commit
    /// per version. Idempotent, like [`Self::remove_version_row`], and with
    /// the same all-or-nothing unit: a crash leaves every listed row or none
    /// of them.
    ///
    /// The default is the per-sequence loop, which is correct for any
    /// implementation but pays one transaction per sequence; an
    /// implementation whose removals are one transaction anyway (this
    /// backend's rows live in one database) overrides it.
    async fn remove_version_rows(
        &self,
        bucket: &Name,
        key: &object::Key,
        seqs: &[u64],
    ) -> Result<(), storage::Error> {
        for seq in seqs {
            self.remove_version_row(bucket, key, *seq).await?;
        }
        Ok(())
    }

    /// The newest row of `key` — its sequence and its row — or `None` when
    /// the key has no version. Ordering comes from the sequence, never from
    /// the identifier.
    async fn latest(
        &self,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<Option<(u64, Row)>, storage::Error>;

    /// Every version row of `bucket` — the live-block walk's source, which
    /// needs the whole set to build the live set anyway.
    async fn bucket_rows(&self, bucket: &Name) -> Result<Vec<Row>, storage::Error>;
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

/// What a [`ContentPort::publish`] left at the key: the content ETag and the
/// byte count its writer computed while streaming. The caller records them in
/// the current version's metadata row, so a publish never forces a re-open, a
/// second stat or a re-hash to describe what it just wrote.
///
/// The ETag is the writer's own — the content MD5 of the streamed bytes. A
/// multipart-composed version's recorded ETag (`MD5-of-MD5s-N`) is a
/// different value by construction, and it lives in the version's row; a
/// caller that must preserve the composed form reads it there rather than
/// from this value.
///
/// # Examples
///
/// ```rust
/// use tinio_version::PublishedBody;
///
/// let published = PublishedBody::from_content(b"hello");
/// assert_eq!(published.size, 5);
/// assert_eq!(published.etag.as_str(), "5d41402abc4b2a76b9719d911017c592");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedBody {
    /// The content ETag the backend's write path computed while streaming
    /// the body.
    pub etag: ETag,
    /// The number of bytes streamed to the key's content slot.
    pub size: u64,
}

impl PublishedBody {
    /// The publication of an in-memory body: its content MD5 and length —
    /// the answer every port double gives.
    pub fn from_content(bytes: &[u8]) -> Self {
        Self {
            etag: ETag::from_content(bytes),
            size: bytes.len() as u64,
        }
    }
}

/// The key's content on the object-store side: the outgoing version's read
/// and the incoming version's publication.
///
/// # Examples
///
/// ```rust
/// use std::sync::Mutex;
///
/// use async_trait::async_trait;
/// use tinio_version::{ContentPort, PublishedBody};
/// use tinio_core::{BodyStream, bucket::Name, collect_body, object, storage};
///
/// #[derive(Default)]
/// struct Slot(Mutex<Option<Vec<u8>>>);
///
/// #[async_trait]
/// impl ContentPort for Slot {
///     async fn open(&self, _: &Name, _: &object::Key) -> Result<Option<BodyStream>, storage::Error> {
///         Ok(self.0.lock().unwrap().clone().map(|bytes| {
///             Box::pin(futures::stream::once(async move { Ok(bytes.into()) })) as BodyStream
///         }))
///     }
///     async fn publish(&self, _: &Name, _: &object::Key, body: BodyStream) -> Result<PublishedBody, storage::Error> {
///         let bytes = collect_body(body).await.map_err(storage::io)?;
///         let published = PublishedBody::from_content(&bytes);
///         *self.0.lock().unwrap() = Some(bytes);
///         Ok(published)
///     }
/// }
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// #     let bucket = tinio_core::bucket::name("demo").unwrap();
/// #     let key = object::key("a.txt").unwrap();
/// #     let slot = Slot::default();
/// #     assert!(slot.open(&bucket, &key).await.unwrap().is_none());
/// #     let published = slot.publish(&bucket, &key, tinio_util::testing::body(b"hello")).await.unwrap();
/// #     assert_eq!(published.size, 5);
/// #     let bytes = collect_body(slot.open(&bucket, &key).await.unwrap().unwrap()).await.unwrap();
/// #     assert_eq!(bytes, b"hello");
/// # });
/// ```
#[async_trait]
pub trait ContentPort: Send + Sync + 'static {
    /// Open the content `key` holds now as a stream, or `None` when it holds
    /// none. The read must stream: neither the supersede nor the materialize
    /// buffers an object whole.
    async fn open(
        &self,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<Option<BodyStream>, storage::Error>;

    /// Stage `body` and publish it at `key` — the backend's own atomic
    /// temp-and-rename path, so a reader never sees a torn object — and
    /// answer what landed: the ETag the write path computed while streaming
    /// and the byte count it wrote.
    async fn publish(
        &self,
        bucket: &Name,
        key: &object::Key,
        body: BodyStream,
    ) -> Result<PublishedBody, storage::Error>;
}
