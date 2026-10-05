//! The version state machine: supersede, materialize, expire.
//!
//! One rule every backend shares: **the current version is whatever that
//! backend's object store holds**. The engine never invents a second
//! authority for it — it reads the key's current slot through
//! [`CurrentRows`], moves content through [`ContentPort`] and the shared blob
//! engine, and records what it moved through [`VersionRows`]. Nothing here
//! knows a path, a table or a file.
//!
//! # The supersede protocol
//!
//! The crash-safety order the design pins, as this engine implements it:
//!
//! 1. The incoming content streams to the backend's stage as today — the
//!    caller's path, outside this engine.
//! 2. The outgoing content is read through [`ContentPort::open`] and written
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

use crate::{
    _blob::{LiveSet, Store},
    _core::{
        blob::{BlobRef, BlockStore},
        bucket::Name,
        object,
        version::State,
    },
    _store::version::{Kind, Row},
    error::Error,
    ports::{ContentPort, Current, CurrentRows, PublishedBody, Superseded, VersionRows},
};

/// How many resolve-read-verify attempts a supersede makes: the first try and
/// the design's one retry against the moved slot.
const SUPERSEDE_ATTEMPTS: usize = 2;

/// The version state machine over one backend's ports and block store.
///
/// `R` is the backend's rows (one type answers both [`VersionRows`] and
/// [`CurrentRows`]), `C` its content port, and `B` the block store the shared
/// blob engine runs over. The engine takes content **keys**, never paths: the
/// content port is the only thing that knows where a key's content lives.
///
/// # Examples
///
/// The protocol over an in-memory double: enable versioning, supersede the
/// current version, and read the row back.
///
/// ```rust
/// # use std::{collections::HashMap, sync::{Arc, Mutex}};
/// # use async_trait::async_trait;
/// # use tinio_blob::{LocalBlocks, Store};
/// # use tinio_core::{BodyStream, blob::BlockStore, bucket::{self, Name}, collect_body, object, storage, version::State};
/// # use tinio_store::version::{self, Kind, Row};
/// # use tinio_version::{ContentPort, Current, CurrentRows, PublishedBody, Superseded, VersionRows, Versions};
/// #
/// # #[derive(Clone, Default)]
/// # struct Ports { state: Arc<Mutex<State>>, slot: Arc<Mutex<Current>>, rows: Arc<Mutex<HashMap<u64, Row>>>, file: Arc<Mutex<Option<Vec<u8>>>> }
/// #
/// # #[async_trait]
/// # impl VersionRows for Ports {
/// #     async fn state(&self, _: &Name) -> Result<State, storage::Error> { Ok(*self.state.lock().unwrap()) }
/// #     async fn set_state(&self, _: &Name, state: State) -> Result<(), storage::Error> { *self.state.lock().unwrap() = state; Ok(()) }
/// #     async fn put_version_row(&self, _: &Name, _: &object::Key, s: &Superseded) -> Result<(), storage::Error> {
/// #         let kind = if s.is_delete_marker { Kind::Marker } else { Kind::Object };
/// #         let id = s.id.as_ref().map_or_else(|| "null".to_owned(), |id| id.as_str().to_owned());
/// #         self.rows.lock().unwrap().insert(s.seq, Row { id, blob: version::encode_blob(&s.blob), size: s.blob.len(), etag: String::new(), created: 0, became_noncurrent: 1, kind, tags: String::new(), checksum: String::new(), owner: String::new(), acl: String::new(), seq: s.seq });
/// #         Ok(())
/// #     }
/// #     async fn remove_version_row(&self, _: &Name, _: &object::Key, seq: u64) -> Result<(), storage::Error> { self.rows.lock().unwrap().remove(&seq); Ok(()) }
/// #     async fn latest(&self, _: &Name, _: &object::Key) -> Result<Option<(u64, Row)>, storage::Error> { Ok(self.rows.lock().unwrap().iter().max_by_key(|(seq, _)| **seq).map(|(seq, row)| (*seq, row.clone()))) }
/// #     async fn bucket_rows(&self, _: &Name) -> Result<Vec<Row>, storage::Error> { Ok(self.rows.lock().unwrap().values().cloned().collect()) }
/// # }
/// #
/// # #[async_trait]
/// # impl CurrentRows for Ports { async fn current(&self, _: &Name, _: &object::Key) -> Result<Current, storage::Error> { Ok(self.slot.lock().unwrap().clone()) } }
/// #
/// # #[async_trait]
/// # impl ContentPort for Ports {
/// #     async fn open(&self, _: &Name, _: &object::Key) -> Result<Option<BodyStream>, storage::Error> { Ok(self.file.lock().unwrap().clone().map(|bytes| Box::pin(futures::stream::once(async move { Ok(bytes.into()) })) as BodyStream)) }
/// #     async fn publish(&self, _: &Name, _: &object::Key, body: BodyStream) -> Result<PublishedBody, storage::Error> { let bytes = collect_body(body).await.map_err(storage::io)?; let published = PublishedBody::from_content(&bytes); *self.file.lock().unwrap() = Some(bytes); Ok(published) }
/// # }
/// #
/// # fn fixture() -> (tempfile::TempDir, Ports, Versions<Ports, Ports, LocalBlocks>) {
/// #     let dir = tempfile::tempdir().unwrap();
/// #     let ports = Ports::default();
/// #     *ports.slot.lock().unwrap() = Current { exists: true, version: Some("v1".parse().unwrap()), is_delete_marker: false, seq: 1, degraded: false };
/// #     *ports.file.lock().unwrap() = Some(b"first".to_vec());
/// #     let versions = Versions::new(ports.clone(), ports.clone(), Store::new(LocalBlocks::new(dir.path())));
/// #     (dir, ports, versions)
/// # }
/// #
/// # let (_dir, _ports, versions) = fixture();
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let bucket = bucket::name("demo").unwrap();
/// let key = object::key("notes.txt").unwrap();
///
/// versions.set_state(&bucket, State::Enabled).await.unwrap();
/// assert_eq!(versions.state(&bucket).await.unwrap(), State::Enabled);
///
/// versions.supersede(&bucket, &key).await.unwrap();
/// let (seq, row) = versions.latest(&bucket, &key).await.unwrap().expect("a version row");
/// assert_eq!(seq, 1, "the sequence the current slot recorded");
/// assert_eq!(row.id.as_str(), "v1", "the identity the current slot recorded");
/// assert_eq!(row.size, 5);
/// assert!(versions.blobs().blocks().contains(row.blob().hash()).await.unwrap());
///
/// let published = versions.materialize(&bucket, &key, &row).await.unwrap();
/// assert_eq!(published.expect("content published").size, 5);
/// versions.expire(&bucket, &key, seq).await.unwrap();
/// assert!(versions.latest(&bucket, &key).await.unwrap().is_none());
/// # });
/// ```
#[derive(Debug, Clone)]
pub struct Versions<R, C, B> {
    /// The version rows and the key's current slot.
    rows: R,
    /// The key's content on the object-store side.
    content: C,
    /// The shared blob engine over this backend's block store.
    blobs: Store<B>,
}

impl<R, C, B> Versions<R, C, B>
where
    R: VersionRows + CurrentRows,
    C: ContentPort,
    B: BlockStore,
{
    /// Serve the state machine over a backend's ports and block store.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tinio_blob::{LocalBlocks, Store};
    /// # use tinio_version::{ContentPort, CurrentRows, VersionRows, Versions};
    /// # #[allow(dead_code)]
    /// # fn demo<R, C>(rows: R, content: C, path: &std::path::Path)
    /// # where
    /// #     R: VersionRows + CurrentRows,
    /// #     C: ContentPort,
    /// # {
    /// let versions = Versions::new(rows, content, Store::new(LocalBlocks::new(path)));
    /// # let _ = versions;
    /// # }
    /// ```
    pub fn new(rows: R, content: C, blobs: Store<B>) -> Self {
        Self {
            rows,
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
    /// # use tinio_core::{bucket, object, version::*};
    /// # use tinio_version::{ContentPort, CurrentRows, VersionRows, Versions};
    /// # #[allow(dead_code)]
    /// # fn demo<R, C>(versions: &Versions<R, C, LocalBlocks>)
    /// # where
    /// #     R: VersionRows + CurrentRows,
    /// #     C: ContentPort,
    /// # {
    /// #     tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let bucket = bucket::name("demo").unwrap();
    /// assert_eq!(versions.state(&bucket).await.unwrap(), State::Unversioned);
    /// # });
    /// # }
    /// ```
    pub async fn state(&self, bucket: &Name) -> Result<State, Error> {
        Ok(self.rows.state(bucket).await?)
    }

    /// Record the bucket's versioning state.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tinio_blob::LocalBlocks;
    /// # use tinio_core::{bucket, object, version::*};
    /// # use tinio_version::{ContentPort, CurrentRows, VersionRows, Versions};
    /// # #[allow(dead_code)]
    /// # fn demo<R, C>(versions: &Versions<R, C, LocalBlocks>)
    /// # where
    /// #     R: VersionRows + CurrentRows,
    /// #     C: ContentPort,
    /// # {
    /// #     tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let bucket = bucket::name("demo").unwrap();
    /// versions.set_state(&bucket, State::Enabled).await.unwrap();
    /// assert_eq!(versions.state(&bucket).await.unwrap(), State::Enabled);
    /// # });
    /// # }
    /// ```
    pub async fn set_state(&self, bucket: &Name, state: State) -> Result<(), Error> {
        Ok(self.rows.set_state(bucket, state).await?)
    }

    /// The key's current slot, as the backend's own gate answers it: a
    /// recorded identity the content no longer matches arrives as the null
    /// current, never as a stale identifier.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tinio_blob::LocalBlocks;
    /// # use tinio_core::{bucket, object, version::*};
    /// # use tinio_version::{ContentPort, CurrentRows, VersionRows, Versions};
    /// # #[allow(dead_code)]
    /// # fn demo<R, C>(versions: &Versions<R, C, LocalBlocks>)
    /// # where
    /// #     R: VersionRows + CurrentRows,
    /// #     C: ContentPort,
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
        Ok(self.rows.current(bucket, key).await?)
    }

    /// Move the key's current version into history.
    ///
    /// The content the current slot points at is streamed into the block
    /// store, and one [`VersionRows`] call commits its row and index entry.
    /// The row's identity and sequence are the ones the
    /// current slot already records — this call never mints either — so
    /// re-running it after the crash window between its commit and the key
    /// path's overwrite rewrites the same row. Whether the row is stamped
    /// noncurrent here or by the caller's own state transaction is the
    /// implementation's (`VersionRows::put_version_row`); readers ask
    /// [`Row::is_history`](tinio_store::version::Row::is_history).
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
    /// # use tinio_core::{bucket, object, version::*};
    /// # use tinio_version::{ContentPort, CurrentRows, VersionRows, Versions};
    /// # #[allow(dead_code)]
    /// # fn demo<R, C>(versions: &Versions<R, C, LocalBlocks>)
    /// # where
    /// #     R: VersionRows + CurrentRows,
    /// #     C: ContentPort,
    /// # {
    /// #     tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let bucket = bucket::name("demo").unwrap();
    /// let key = object::key("notes.txt").unwrap();
    /// versions.supersede(&bucket, &key).await.unwrap();
    /// let (seq, row) = versions.latest(&bucket, &key).await.unwrap().unwrap();
    /// assert_eq!((seq, row.id.as_str()), (1, "v1"));
    /// assert_eq!(
    ///     row.size, 5,
    ///     "the outgoing content moved into the block store"
    /// );
    /// # });
    /// # }
    /// ```
    pub async fn supersede(&self, bucket: &Name, key: &object::Key) -> Result<(), Error> {
        let before = self.rows.current(bucket, key).await?;
        self.supersede_with(bucket, key, &before).await
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
                Some(Superseded {
                    id: before.version.clone(),
                    seq: before.seq,
                    blob: BlobRef::empty(),
                    is_delete_marker: true,
                })
            } else {
                match self.content.open(bucket, key).await? {
                    Some(body) => {
                        let blob = self
                            .blobs
                            .put(body)
                            .await
                            .map_err(|err| Error::Storage(err.into()))?;
                        Some(Superseded {
                            id: before.version.clone(),
                            seq: before.seq,
                            blob,
                            is_delete_marker: false,
                        })
                    }
                    None if before.is_null() => {
                        // The null version holds no content (an out-of-band
                        // removal): nothing is current to move into history,
                        // so there is no disagreement to answer.
                        empty_null = true;
                        None
                    }
                    None => None,
                }
            };
            // Re-resolve through the gate: the recorded identity the read was
            // resolved against must still be the slot's truth, or the content
            // changed under the read.
            let after = self.rows.current(bucket, key).await?;
            if after == before {
                if let Some(superseded) = superseded {
                    self.rows.put_version_row(bucket, key, &superseded).await?;
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
    /// # use tinio_core::{bucket, object, version::*};
    /// # use tinio_version::{ContentPort, CurrentRows, VersionRows, Versions};
    /// # #[allow(dead_code)]
    /// # fn demo<R, C>(versions: &Versions<R, C, LocalBlocks>)
    /// # where
    /// #     R: VersionRows + CurrentRows,
    /// #     C: ContentPort,
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
        entry: &Row,
    ) -> Result<Option<PublishedBody>, Error> {
        if entry.kind == Kind::Marker {
            return Ok(None); // a marker holds no content to write back
        }
        let body = self
            .blobs
            .open(&entry.blob(), None)
            .await
            .map_err(|err| Error::Storage(err.into()))?;
        Ok(Some(self.content.publish(bucket, key, body).await?))
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
    /// # use tinio_core::{bucket, object, version::*};
    /// # use tinio_version::{ContentPort, CurrentRows, VersionRows, Versions};
    /// # #[allow(dead_code)]
    /// # fn demo<R, C>(versions: &Versions<R, C, LocalBlocks>)
    /// # where
    /// #     R: VersionRows + CurrentRows,
    /// #     C: ContentPort,
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
        Ok(self.rows.remove_version_row(bucket, key, seq).await?)
    }

    /// Drop several versions of one key in a single port call: the same
    /// rows and index entries [`Self::expire`] drops, committed as one
    /// transaction by a port that can, so a retention pass over a long
    /// history pays one durable commit rather than one per version. The
    /// caller has already resolved the sequences it means (the sweep holds
    /// the whole history in hand); this call re-reads nothing.
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
        Ok(self.rows.remove_version_rows(bucket, key, seqs).await?)
    }

    /// The newest version row of `key` — its sequence and its row — or
    /// `None` when the key has no history. Ordering comes from the stored
    /// sequence, never from the identifier.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tinio_blob::LocalBlocks;
    /// # use tinio_core::{bucket, object, version::*};
    /// # use tinio_version::{ContentPort, CurrentRows, VersionRows, Versions};
    /// # #[allow(dead_code)]
    /// # fn demo<R, C>(versions: &Versions<R, C, LocalBlocks>)
    /// # where
    /// #     R: VersionRows + CurrentRows,
    /// #     C: ContentPort,
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
    /// assert_eq!((seq, row.id.as_str()), (1, "v1"));
    /// # });
    /// # }
    /// ```
    pub async fn latest(
        &self,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<Option<(u64, Row)>, Error> {
        Ok(self.rows.latest(bucket, key).await?)
    }

    /// The bucket's live blocks: every block the decoded blob of every
    /// version row names, for the sweep. Liveness is derived, never counted —
    /// a block absent from this set is unreachable by definition.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tinio_blob::LocalBlocks;
    /// # use tinio_core::{bucket, object, version::*};
    /// # use tinio_version::{ContentPort, CurrentRows, VersionRows, Versions};
    /// # #[allow(dead_code)]
    /// # fn demo<R, C>(versions: &Versions<R, C, LocalBlocks>)
    /// # where
    /// #     R: VersionRows + CurrentRows,
    /// #     C: ContentPort,
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
        let rows = self.rows.bucket_rows(bucket).await?;
        Ok(LiveSet::from_blobs(rows.into_iter().map(|row| row.blob())))
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
