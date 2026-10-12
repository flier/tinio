//! The version state machine: supersede, materialize, expire.
//!
//! One rule every backend shares: **the current version is whatever that
//! backend's object store holds**. The engine never invents a second
//! authority for it — it reads the key's current slot through
//! [`CurrentRows`], moves content through the content contract
//! ([`ObjectContent`](tinio_core::content::ObjectContent)) and the shared blob
//! engine, and records what it moved through the persistence contracts
//! ([`Metadata`](tinio_core::meta::Metadata) and
//! [`VersionMeta`](tinio_core::meta::VersionMeta)). Nothing here knows a path,
//! a table or a file.
//!
//! # The supersede protocol
//!
//! The crash-safety order the design pins, as this engine implements it:
//!
//! 1. The incoming content streams to the backend's stage as today — the
//!    caller's path, outside this engine.
//! 2. The outgoing content is read through the content contract's
//!    [`open`](tinio_core::content::ObjectContent::open) and written
//!    into the block store in the same streaming pass. Blocks are
//!    content-addressed and immutable, so a partial write is inert garbage,
//!    never a torn blob.
//! 3. The version row — and its index entry — are committed **before** the
//!    key path is overwritten. A crash after this point and before the
//!    overwrite leaves the old content at the key path *and* a row describing
//!    it; recovery **adopts** that row (its content is real, and it is the
//!    version that was current) rather than discarding it, and because the
//!    row carries the identity and sequence the current slot already records,
//!    re-running the supersede rewrites the same row rather than minting a
//!    second identity for the same content.
//! 4. The backend's atomic rename publishes the incoming content and its
//!    transaction records the new current identity — again outside this
//!    engine.
//! 5. A crash anywhere before step 4 leaves the previous state intact and the
//!    incoming bytes as stage residue, which the backend's existing sweep
//!    already reclaims.
//!
//! # The supersede race
//!
//! The outgoing file can be rewritten out of band while the engine reads it.
//! What lands in history must be the version that was actually current, never
//! a half-written stranger, so [`Versions::supersede`] resolves the key's
//! gated current slot **again after the read**: a recorded identity the
//! content no longer matches makes that second resolve answer the null
//! current (or nothing at all), which is exactly the change the race names.
//! The call re-resolves and retries the read-and-publish **once** against the
//! moved slot, so a single out-of-band change still lands the version that
//! was actually current; only a second disagreement answers
//! [`Error::SupersedeRace`] and writes no row — the blobs published by the
//! failed attempts are unreferenced garbage the sweep reclaims.

use std::time::SystemTime;

use crate::{
    _blob::{LiveSet, Store},
    _core::{
        ETag,
        acl::Acl,
        blob::{BlobRef, BlockStore},
        bucket::Name,
        content::{ContentKey, ObjectContent, stage_body},
        meta::{
            Metadata, ObjectMeta, VersionMeta,
            version::{Kind, Stored},
        },
        object, storage,
        version::State,
    },
    error::Error,
    ports::{Current, CurrentRows, PublishedBody},
};

/// How many resolve-read-verify attempts a supersede makes: the first try and
/// the design's one retry against the moved slot.
const SUPERSEDE_ATTEMPTS: usize = 2;

/// Project a contract failure onto the engine's own error.
///
/// Every contract method answers its implementation's error, which each
/// contract requires to convert into [`storage::Error`]; the engine has one
/// error type, so the conversion happens once, here.
fn projected(err: impl Into<storage::Error>) -> Error {
    Error::Storage(err.into())
}

/// The version state machine over one backend's contracts and block store.
///
/// `R` is the row plane ([`Metadata`] and [`VersionMeta`]). `G` is the gate
/// ([`CurrentRows`]): the one read of the key's current slot. A backend whose
/// rows are also the gate passes that type twice and leaves `G` at its default,
/// which is `R`. `C` is the content plane ([`ObjectContent`]) and `B` the block
/// store the shared blob engine runs over. The engine takes content **keys**,
/// never paths: the content contract is the only thing that knows where a key's
/// content lives.
///
/// # Examples
///
/// Every call runs over one backend's contracts plus the shared blob engine;
/// the crate's own tests drive this shape over the in-crate contract double.
///
/// ```no_run
/// # use tinio_blob::{LocalBlocks, Store};
/// # use tinio_core::{
/// #     content::ObjectContent,
/// #     meta::{Metadata, VersionMeta},
/// # };
/// # use tinio_version::{CurrentRows, Versions};
/// # #[allow(dead_code)]
/// # fn demo<R, C>(rows: R, content: C, path: &std::path::Path)
/// # where
/// #     R: Metadata + VersionMeta + CurrentRows + Clone,
/// #     C: ObjectContent,
/// # {
/// let versions = Versions::new(
///     rows.clone(),
///     rows,
///     content,
///     Store::new(LocalBlocks::new(path)),
/// );
/// # let _ = versions;
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Versions<R, C, B, G = R> {
    /// The version rows and the object row a history record is copied from.
    rows: R,
    /// The key's gated current slot.
    gate: G,
    /// The key's content on the object-store side.
    content: C,
    /// The shared blob engine over this backend's block store.
    blobs: Store<B>,
}

/// The **single-handle** engine: a backend whose rows are also their own gate (`G = R`), which is
/// every backend but pg. [`Versions::new`] keeps the two-handle form pg's separate gate needs; a
/// non-pg call site takes this one instead of spelling the same handle twice.
impl<R, C, B> Versions<R, C, B, R>
where
    R: Metadata + VersionMeta + CurrentRows + Clone,
    C: ObjectContent,
    B: BlockStore,
{
    /// Serve the state machine over one handle that is both the row plane and the gate — the
    /// degenerate `G = R` case, which [`Versions::new`] spells by passing the same handle twice.
    pub fn rows_as_gate(rows: R, content: C, blobs: Store<B>) -> Self {
        Versions::new(rows.clone(), rows, content, blobs)
    }
}

impl<R, C, B, G> Versions<R, C, B, G>
where
    R: Metadata + VersionMeta,
    G: CurrentRows,
    C: ObjectContent,
    B: BlockStore,
{
    /// Serve the state machine over a backend's contracts and block store.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tinio_blob::{LocalBlocks, Store};
    /// # use tinio_core::{
    /// #     content::ObjectContent,
    /// #     meta::{Metadata, VersionMeta},
    /// # };
    /// # use tinio_version::{CurrentRows, Versions};
    /// # #[allow(dead_code)]
    /// # fn demo<R, C>(rows: R, content: C, path: &std::path::Path)
    /// # where
    /// #     R: Metadata + VersionMeta + CurrentRows + Clone,
    /// #     C: ObjectContent,
    /// # {
    /// let versions = Versions::new(
    ///     rows.clone(),
    ///     rows,
    ///     content,
    ///     Store::new(LocalBlocks::new(path)),
    /// );
    /// # let _ = versions;
    /// # }
    /// ```
    pub fn new(rows: R, gate: G, content: C, blobs: Store<B>) -> Self {
        Self {
            rows,
            gate,
            content,
            blobs,
        }
    }

    /// The bucket's versioning state.
    ///
    /// # Examples
    ///
    /// A backend that never recorded a state answers the default, which is
    /// [`State::Unversioned`].
    ///
    /// ```no_run
    /// # use tinio_blob::LocalBlocks;
    /// # use tinio_core::{bucket, content::ObjectContent, meta::{Metadata, VersionMeta}, object, version::*};
    /// # use tinio_version::{CurrentRows, Versions};
    /// # #[allow(dead_code)]
    /// # fn demo<R, C>(versions: &Versions<R, C, LocalBlocks>)
    /// # where
    /// #     R: Metadata + VersionMeta + CurrentRows,
    /// #     C: ObjectContent,
    /// # {
    /// #     tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let bucket = bucket::name("demo").unwrap();
    /// assert_eq!(versions.state(&bucket).await.unwrap(), State::Unversioned);
    /// # });
    /// # }
    /// ```
    pub async fn state(&self, bucket: &Name) -> Result<State, Error> {
        self.rows.bucket_state(bucket).await.map_err(projected)
    }

    /// Record the bucket's versioning state.
    ///
    /// The unguarded flip is deliberate: this engine is generic over its row
    /// plane, and a backend whose bucket rows materialize lazily (or a
    /// harness driving synthetic state) would be refused by the guarded
    /// variant. A consumer that needs the guard against a racing teardown
    /// calls [`VersionMeta::set_bucket_state_guarded`] directly — mem does,
    /// because its bucket rows always exist first.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tinio_blob::LocalBlocks;
    /// # use tinio_core::{bucket, content::ObjectContent, meta::{Metadata, VersionMeta}, object, version::*};
    /// # use tinio_version::{CurrentRows, Versions};
    /// # #[allow(dead_code)]
    /// # fn demo<R, C>(versions: &Versions<R, C, LocalBlocks>)
    /// # where
    /// #     R: Metadata + VersionMeta + CurrentRows,
    /// #     C: ObjectContent,
    /// # {
    /// #     tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let bucket = bucket::name("demo").unwrap();
    /// versions.set_state(&bucket, State::Enabled).await.unwrap();
    /// assert_eq!(versions.state(&bucket).await.unwrap(), State::Enabled);
    /// # });
    /// # }
    /// ```
    pub async fn set_state(&self, bucket: &Name, state: State) -> Result<(), Error> {
        self.rows
            .set_bucket_state(bucket, state)
            .await
            .map_err(projected)
    }

    /// The key's current slot, as the backend's own gate answers it: a
    /// recorded identity the content no longer matches arrives as the null
    /// current, never as a stale identifier.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tinio_blob::LocalBlocks;
    /// # use tinio_core::{bucket, content::ObjectContent, meta::{Metadata, VersionMeta}, object, version::*};
    /// # use tinio_version::{CurrentRows, Versions};
    /// # #[allow(dead_code)]
    /// # fn demo<R, C>(versions: &Versions<R, C, LocalBlocks>)
    /// # where
    /// #     R: Metadata + VersionMeta + CurrentRows,
    /// #     C: ObjectContent,
    /// # {
    /// #     tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let bucket = bucket::name("demo").unwrap();
    /// let key = object::key("notes.txt").unwrap();
    /// let current = versions.current(&bucket, &key).await.unwrap();
    /// assert!(current.exists);
    /// assert_eq!(current.version.unwrap().as_str(), "v1");
    /// assert_eq!(current.seq, 1);
    /// # });
    /// # }
    /// ```
    pub async fn current(&self, bucket: &Name, key: &object::Key) -> Result<Current, Error> {
        Ok(self.gate.current(bucket, key).await?)
    }

    /// Move the key's current version into history.
    ///
    /// The content the current slot points at is streamed into the block
    /// store, and one [`VersionMeta::put_version`] call commits its row and
    /// index entry.
    ///
    /// The row's identity and sequence are the ones the
    /// current slot already records — this call never mints either — so
    /// re-running it after the crash window between its commit and the key
    /// path's overwrite rewrites the same row. Whether the row is stamped
    /// noncurrent here or by the caller's own state transaction is the
    /// implementation's — this call commits the row before the slot moves off
    /// it, and the trailing state transaction stamps it; readers ask
    /// [`Stored::became_noncurrent`].
    ///
    /// This form reads the key's gated slot itself; a caller that already
    /// holds it (the write paths read it for their branch decision under the
    /// lock the supersede runs under) passes it to
    /// [`Self::supersede_with`], so the slot is read once, not twice.
    ///
    /// A key with no current version is a no-op. A current **delete marker**
    /// holds no content: its history row is the marker, with the empty blob,
    /// and no read happens.
    ///
    /// # Errors
    ///
    /// [`Error::SupersedeRace`] when the key's gated current slot moved
    /// between the resolve and the read's end — the bytes just stored belong
    /// to no version this call can name, and nothing is committed for them. A
    /// slot that names a version while the content port answers no content is
    /// the same disagreement. Either disagreement re-resolves and retries the
    /// read-and-publish once; only a second one is answered as the error. The
    /// null version is the exception: a null current whose content port holds
    /// nothing (an out-of-band removal) has nothing to preserve, so a confirmed
    /// slot answers `Ok` with no row.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tinio_blob::LocalBlocks;
    /// # use tinio_core::{bucket, content::ObjectContent, meta::{Metadata, VersionMeta}, object, version::*};
    /// # use tinio_version::{CurrentRows, Versions};
    /// # #[allow(dead_code)]
    /// # fn demo<R, C>(versions: &Versions<R, C, LocalBlocks>)
    /// # where
    /// #     R: Metadata + VersionMeta + CurrentRows,
    /// #     C: ObjectContent,
    /// # {
    /// #     tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let bucket = bucket::name("demo").unwrap();
    /// let key = object::key("notes.txt").unwrap();
    /// versions.supersede(&bucket, &key).await.unwrap();
    /// let (seq, row) = versions.latest(&bucket, &key).await.unwrap().unwrap();
    /// assert_eq!((seq, row.id.as_ref().expect("a named version").as_str()), (1, "v1"));
    /// assert_eq!(
    ///     row.size, 5,
    ///     "the outgoing content moved into the block store"
    /// );
    /// # });
    /// # }
    /// ```
    pub async fn supersede(&self, bucket: &Name, key: &object::Key) -> Result<(), Error> {
        let before = self.gate.current(bucket, key).await?;
        self.supersede_with(bucket, key, &before).await
    }

    /// The history row for the version `before` names.
    ///
    /// Its identity and sequence are the slot's — this call never mints either
    /// — and its content address is the blob just stored. Its metadata wires
    /// come from the object row the slot resolved against: the **recorded**
    /// ETag (the composed `-N` form included, which no re-hash could
    /// reproduce) plus the tags, checksum, owner and ACL, each reusing its wire
    /// encoding. A slot with no object row (a marker whose slot is kept
    /// elsewhere) contributes the empty wires and this call's clock.
    ///
    /// The row is written with `became_noncurrent: 0` — this call commits it
    /// before the current slot moves off it, so it is not history yet, and the
    /// trailing state transaction stamps it. A stamp the stored row already
    /// carries is preserved regardless: the contract's
    /// [`VersionMeta::put_version`] keeps the stored stamp and ignores the
    /// caller's whenever a row already sits at `row.seq`, so the crash window's
    /// adopted row is never un-stamped by the supersede that rewrites it. Every
    /// other field is this call's, and the implementation's own rules (the
    /// row-overwrite guard and the one-null-version convergence) are enforced
    /// inside `put_version`'s transaction, not here.
    async fn version_row(
        &self,
        bucket: &Name,
        key: &object::Key,
        before: &Current,
        blob: BlobRef,
        kind: Kind,
    ) -> Result<Stored, Error> {
        let object = ObjectMeta::get(&self.rows, bucket, key)
            .await
            .map_err(|err| Error::Storage(err.into()))?;
        let (etag, created, tags, checksum, owner, acl) = match object {
            Some(row) => (
                row.etag,
                row.mtime,
                row.tags,
                row.checksum,
                row.owner,
                row.acl,
            ),
            None => (
                ETag::EMPTY,
                SystemTime::now(),
                object::Tags::empty(),
                None,
                None,
                Acl::default_private(None),
            ),
        };
        Ok(Stored {
            id: before.version.clone(),
            size: blob.len(),
            blob,
            etag,
            created,
            became_noncurrent: 0,
            kind,
            tags,
            checksum,
            owner,
            acl,
            seq: before.seq,
        })
    }

    /// [`Self::supersede`] with the key's gated slot already in hand — the
    /// write paths' shape: `plan_write` and `delete_versioned` read the slot
    /// for their own branch decision under the same bucket mutation lock the
    /// supersede runs under, so re-reading it here would repeat a state
    /// database read, a bucket stat and a key resolve for an answer the
    /// caller must already hold (the branch it took is a function of that
    /// slot).
    ///
    /// `before` must be this key's own [`CurrentRows::current`] answer, read
    /// under the caller's lock. The post-read re-resolve is unaffected: the
    /// slot is checked against a fresh read after the content read, and a
    /// moved slot re-resolves and retries the read-and-publish once against
    /// the moved slot; only a second disagreement answers
    /// [`Error::SupersedeRace`] with no row written.
    pub async fn supersede_with(
        &self,
        bucket: &Name,
        key: &object::Key,
        before: &Current,
    ) -> Result<(), Error> {
        // The design's retry: one re-resolve and read-and-publish after a
        // race, against the slot the fresh resolve named; only a second
        // disagreement is the error.
        let mut before = before.clone();
        for attempt in 0..SUPERSEDE_ATTEMPTS {
            if !before.exists {
                return Ok(()); // nothing is current: nothing to move into history
            }
            // What the read found: `Some` is a version to move, `None` a
            // disagreement the retry may heal — except for the null version
            // with no content, which has nothing to preserve at all.
            let mut empty_null = false;
            let superseded = if before.is_delete_marker {
                // A marker holds no content: its history row carries the empty
                // blob, and the read is not attempted.
                Some(
                    self.version_row(bucket, key, &before, BlobRef::empty(), Kind::Marker)
                        .await?,
                )
            } else {
                match self
                    .content
                    .open(bucket, &ContentKey::Object(key.clone()), None)
                    .await
                {
                    Ok(body) => {
                        let blob = self
                            .blobs
                            .put(body)
                            .await
                            .map_err(|err| Error::Storage(err.into()))?;
                        Some(
                            self.version_row(bucket, key, &before, blob, Kind::Object)
                                .await?,
                        )
                    }
                    Err(err) => {
                        // The content contract's `open` has no `None` arm
                        // (D23): a key whose content is gone answers its own
                        // not-found error, which projects onto the contract's
                        // `NoSuchKey` for an object. That is the disagreement
                        // the old absent answer was — the read found no content
                        // to move into history — and the retry below is what
                        // decides whether it was a race. Any other failure is a
                        // real one and is answered as itself.
                        let projected: storage::Error = err.into();
                        if !matches!(projected, storage::Error::NoSuchKey(_)) {
                            return Err(Error::Storage(projected));
                        }
                        if before.is_null() {
                            // The null version holds no content (an out-of-band
                            // removal): nothing is current to move into history,
                            // so there is no disagreement to answer.
                            empty_null = true;
                        }
                        None
                    }
                }
            };
            // Re-resolve through the gate: the recorded identity the read was
            // resolved against must still be the slot's truth, or the content
            // changed under the read.
            let after = self.gate.current(bucket, key).await?;
            if after == before {
                if let Some(superseded) = superseded {
                    self.rows
                        .put_version(bucket, key, &superseded)
                        .await
                        .map_err(projected)?;
                    return Ok(());
                }
                if empty_null {
                    return Ok(()); // an empty null version: nothing to preserve
                }
            }
            if attempt + 1 == SUPERSEDE_ATTEMPTS {
                // The second attempt disagreed too: the race is real, and no
                // identity may be stamped onto content this call cannot name.
                break;
            }
            before = after;
        }
        Err(Error::SupersedeRace { key: key.clone() })
    }

    /// Write a stored version's content back onto the key's content slot —
    /// the materialize half of the move, and the only path that turns history
    /// back into a file. Answers what the backend's publish left at the key
    /// (the ETag and byte count its write path computed while streaming), so
    /// the caller describes the restored content in its current-version
    /// metadata without re-opening, re-statting or re-hashing the file it
    /// just wrote; a delete marker's row holds no content, so the call is a
    /// no-op for it and answers `None`.
    ///
    /// The row's decoded blob streams out of the block store and into the
    /// backend's staged publish; neither side buffers the object whole.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tinio_blob::LocalBlocks;
    /// # use tinio_core::{bucket, content::ObjectContent, meta::{Metadata, VersionMeta}, object, version::*};
    /// # use tinio_version::{CurrentRows, Versions};
    /// # #[allow(dead_code)]
    /// # fn demo<R, C>(versions: &Versions<R, C, LocalBlocks>)
    /// # where
    /// #     R: Metadata + VersionMeta + CurrentRows,
    /// #     C: ObjectContent,
    /// # {
    /// #     tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let bucket = bucket::name("demo").unwrap();
    /// let key = object::key("notes.txt").unwrap();
    /// versions.supersede(&bucket, &key).await.unwrap();
    /// let (_, row) = versions.latest(&bucket, &key).await.unwrap().unwrap();
    ///
    /// // The key's current content went elsewhere; materializing the row
    /// // writes the stored version back onto it.
    /// let published = versions.materialize(&bucket, &key, &row).await.unwrap();
    /// assert_eq!(published.expect("the object row publishes").size, 5);
    /// # });
    /// # }
    /// ```
    pub async fn materialize(
        &self,
        bucket: &Name,
        key: &object::Key,
        entry: &Stored,
    ) -> Result<Option<PublishedBody>, Error> {
        if entry.kind == Kind::Marker {
            return Ok(None); // a marker holds no content to write back
        }
        let body = self
            .blobs
            .open(&entry.blob, None)
            .await
            .map_err(|err| Error::Storage(err.into()))?;
        // The staged pair, not a whole-body buffer: the blob's chunks stream
        // into the backend's stage and the count is what **landed** there —
        // the bytes the publish then makes visible, which is what a caller
        // records in the restored version's row (the read path's staleness
        // gate compares that row against the file).
        let content_key = ContentKey::Object(key.clone());
        let mut staged = self
            .content
            .stage(bucket, &content_key)
            .await
            .map_err(|err| Error::Storage(err.into()))?;
        let size = stage_body(&mut staged, body).await?;
        self.content
            .publish(bucket, staged, &content_key)
            .await
            .map_err(|err| Error::Storage(err.into()))?;
        Ok(Some(PublishedBody {
            etag: entry.etag.clone(),
            size,
        }))
    }

    /// Drop one version: its row and its index entry, in one transaction.
    ///
    /// Idempotent — a sequence that holds no row is `Ok`. The content's
    /// blocks are not reclaimed here: liveness is derived, and the block
    /// sweep drops what no row points at.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tinio_blob::LocalBlocks;
    /// # use tinio_core::{bucket, content::ObjectContent, meta::{Metadata, VersionMeta}, object, version::*};
    /// # use tinio_version::{CurrentRows, Versions};
    /// # #[allow(dead_code)]
    /// # fn demo<R, C>(versions: &Versions<R, C, LocalBlocks>)
    /// # where
    /// #     R: Metadata + VersionMeta + CurrentRows,
    /// #     C: ObjectContent,
    /// # {
    /// #     tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let bucket = bucket::name("demo").unwrap();
    /// let key = object::key("notes.txt").unwrap();
    /// versions.supersede(&bucket, &key).await.unwrap();
    /// versions.expire(&bucket, &key, 1).await.unwrap();
    /// assert!(versions.latest(&bucket, &key).await.unwrap().is_none());
    /// # });
    /// # }
    /// ```
    pub async fn expire(&self, bucket: &Name, key: &object::Key, seq: u64) -> Result<(), Error> {
        self.rows.expire(bucket, key, seq).await.map_err(projected)
    }

    /// Drop several versions of one key in one contract call: the same rows
    /// and index entries [`Self::expire`] drops, committed as **one**
    /// transaction. A retention pass over a long history pays one durable
    /// commit — and one crash point — rather than one per version. The caller
    /// has already resolved the sequences it means (the sweep holds the whole
    /// history in hand); this call re-reads nothing.
    ///
    /// Idempotent like [`Self::expire`]. The content's blocks are not
    /// reclaimed here either: liveness is derived, and the block sweep
    /// drops what no row points at.
    pub async fn expire_many(
        &self,
        bucket: &Name,
        key: &object::Key,
        seqs: &[u64],
    ) -> Result<(), Error> {
        self.rows
            .expire_many(bucket, key, seqs)
            .await
            .map_err(projected)
    }

    /// The newest version row of `key` — its sequence and its row — or
    /// `None` when the key has no history. Ordering comes from the stored
    /// sequence, never from the identifier.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tinio_blob::LocalBlocks;
    /// # use tinio_core::{bucket, content::ObjectContent, meta::{Metadata, VersionMeta}, object, version::*};
    /// # use tinio_version::{CurrentRows, Versions};
    /// # #[allow(dead_code)]
    /// # fn demo<R, C>(versions: &Versions<R, C, LocalBlocks>)
    /// # where
    /// #     R: Metadata + VersionMeta + CurrentRows,
    /// #     C: ObjectContent,
    /// # {
    /// #     tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let bucket = bucket::name("demo").unwrap();
    /// let key = object::key("notes.txt").unwrap();
    /// assert!(
    ///     versions.latest(&bucket, &key).await.unwrap().is_none(),
    ///     "no history yet"
    /// );
    /// versions.supersede(&bucket, &key).await.unwrap();
    /// let (seq, row) = versions.latest(&bucket, &key).await.unwrap().unwrap();
    /// assert_eq!((seq, row.id.as_ref().expect("a named version").as_str()), (1, "v1"));
    /// # });
    /// # }
    /// ```
    pub async fn latest(
        &self,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<Option<(u64, Stored)>, Error> {
        self.rows.latest(bucket, key).await.map_err(projected)
    }

    /// The bucket's live blocks: every block the decoded blob of every
    /// version row names, for the sweep. Liveness is derived, never counted —
    /// a block absent from this set is unreachable by definition.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tinio_blob::LocalBlocks;
    /// # use tinio_core::{bucket, content::ObjectContent, meta::{Metadata, VersionMeta}, object, version::*};
    /// # use tinio_version::{CurrentRows, Versions};
    /// # #[allow(dead_code)]
    /// # fn demo<R, C>(versions: &Versions<R, C, LocalBlocks>)
    /// # where
    /// #     R: Metadata + VersionMeta + CurrentRows,
    /// #     C: ObjectContent,
    /// # {
    /// #     tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// use tinio_core::blob::BlockHash;
    /// let bucket = bucket::name("demo").unwrap();
    /// let key = object::key("notes.txt").unwrap();
    /// assert!(
    ///     versions.live_blocks(&bucket).await.unwrap().is_empty(),
    ///     "no history, no live blocks"
    /// );
    /// versions.supersede(&bucket, &key).await.unwrap();
    /// let live = versions.live_blocks(&bucket).await.unwrap();
    /// assert!(live.contains(&BlockHash::of(b"first")));
    /// # });
    /// # }
    /// ```
    pub async fn live_blocks(&self, bucket: &Name) -> Result<LiveSet, Error> {
        let rows = self.rows.bucket_rows(bucket).await.map_err(projected)?;
        Ok(LiveSet::from_blobs(rows.into_iter().map(|row| row.blob)))
    }

    /// The blob engine this instance runs over — the write path's read side
    /// and the tests' probe.
    pub fn blobs(&self) -> &Store<B> {
        &self.blobs
    }
}

// The state machine's own tests, in the sibling file this module declares
// (`docs/tests.md`'s placement rule): they run the engine over the in-crate
// port double in `crate::testutil`.
#[cfg(test)]
mod tests;
