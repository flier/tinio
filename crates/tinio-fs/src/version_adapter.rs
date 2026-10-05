//! The filesystem implementation of the version engine's three ports.
//!
//! `tinio-version` owns the state machine and knows nothing about paths,
//! tables or files; this module is the fs side of the seam. It maps a content
//! **key** to the key path (`resolve_key`, so the symlink policy and the
//! containment proof apply exactly as they do to every other object
//! operation), reads and publishes that content through the file stream and
//! the atomic writer, and keeps the version rows, their point-lookup index and
//! the bucket's state in the state database's redb tables.
//!
//! Two rules are this side's rather than the engine's:
//!
//! - **The current-slot gate.** [`CurrentRows::current`] applies the same
//!   size/mtime/file-identity staleness rule the read paths use (`F01`/`F04`):
//!   a recorded identity the content no longer matches — an out-of-band edit,
//!   or a file removed out of band while the row still described it — is
//!   reported as the **null** version, exactly as the design's
//!   out-of-band-edit rule has it, so the engine only ever sees gated truth.
//! - **The two-table transaction.** A version row and its index entry are
//!   written — and dropped — in one redb write transaction, in the shape
//!   `complete_object_state` already uses. The crash window between that
//!   commit and the key path's overwrite is the engine's protocol step 3; the
//!   atomicity that makes it recoverable is this module's.
//! - **The one-null-version rule.** A key holds at most one `null`-id row in
//!   the version table (AWS's model): the index is keyed by the identity and
//!   can name only one of two rows sharing the `null` wire, so when a
//!   supersede commits the null identity and an older null-id row already
//!   sits in history, that row is replaced — removed with its index entry in
//!   the same transaction — rather than a second one added, and no older null
//!   row is ever stranded unaddressable.
//! - **The row-overwrite guard.** The row at the outgoing version's sequence
//!   is rewritten only when the row already sitting there names the *same*
//!   version — same identity, same kind, same content address (the blob hash
//!   and the byte count) — which is exactly the adoption path the crash
//!   window needs. A row naming anything else at that sequence is refused
//!   loudly rather than overwritten: the row is what keeps that version's
//!   blob in the live set, and replacing it would drop a committed version's
//!   content for the sweep to reclaim. The content address is part of the
//!   check because the identity wire cannot separate two null versions, which
//!   share it by construction.

use std::{
    io::{Error as IoError, ErrorKind},
    path::PathBuf,
    time::SystemTime,
};

use async_trait::async_trait;
use tokio::fs;

use crate::{
    _core::{
        BodyStream,
        bucket::Name,
        object, storage, to_nanos,
        version::{State, wire_of},
    },
    _store::{bucket_versioning, meta, version as rows, version_index},
    _version::{
        ContentPort, Current, CurrentRows, PublishedBody, Superseded, VersionRows, converges_null,
    },
    Error, FsStorage,
    error::contract,
    fsutil,
    meta::{Content, Verdict, gate},
};

/// The refusal of a supersede that would overwrite a row naming a **different**
/// version — a different identity, kind or content: an I/O-class contract
/// error (`ErrorKind::Other`, never a not-found) naming the key, the sequence
/// and the identity sitting there.
///
/// The reachable state behind this refusal is the null-versus-null shape: a
/// committed row at sequence 0 and a current null version whose content
/// differs from it — the identity wire cannot tell the two null versions
/// apart, so only the content address can. A row still describing the current
/// version is not this state: the gate reports content the row no longer
/// matches (an out-of-band edit or removal) as the null version, whose
/// supersede is a no-op while it holds no content.
fn row_conflict(bucket: &Name, key: &object::Key, seq: u64, existing: &str) -> storage::Error {
    storage::io(IoError::other(format!(
        "refusing to overwrite the version row of `{key}` at sequence {seq} in bucket `{bucket}`: \
         it holds version `{existing}`, not the version being superseded"
    )))
}

/// The key's current slot as the gate left it: the port's [`Current`],
/// the `object_meta` row the gate judged, and the key path the gate resolved
/// and judged.
///
/// One read serves both the engine (which takes `current` and `stored`) and
/// the version-addressed read paths (which open `path` instead of resolving
/// the key a second time), so a request never resolves the same key twice.
#[derive(Debug, Clone)]
pub(crate) struct Judged {
    /// The gated slot — the port's answer.
    pub(crate) current: Current,
    /// The judged `object_meta` row (`None` when none records the key).
    pub(crate) stored: Option<meta::Stored>,
    /// The resolved key path, when the key has a file (`None` for a marker's
    /// slot or a file removed out of band — the states with nothing to
    /// open). Opening it is the caller's, so a metadata-only caller never
    /// pays for a handle it does not use.
    pub(crate) path: Option<PathBuf>,
}

/// The fs implementation of [`VersionRows`] and [`CurrentRows`]: the state
/// database's version tables, and the key's gated current slot.
#[derive(Debug, Clone)]
pub(crate) struct Rows {
    /// The backend this adapter serves (its state-database handle, its meta
    /// store and its path mapping).
    storage: FsStorage,
}

impl Rows {
    /// Serve a backend's rows.
    pub(crate) fn new(storage: FsStorage) -> Self {
        Self { storage }
    }

    /// The key's gated current slot — the port's [`CurrentRows::current`]
    /// answer — with the `object_meta` row the gate was applied to and the
    /// key path it judged, all from one read: a caller that needs the row or
    /// the file (the version read paths' `slot`) neither reads the row nor
    /// resolves the path again.
    ///
    /// A row whose etag is domain-invalid is reported as absent — the port's
    /// own self-heal, so the two answers can never disagree.
    pub(crate) async fn current_with_row(
        &self,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<Judged, storage::Error> {
        // No recorded row: the key has no current version at all.
        let Some(stored) = self
            .storage
            .meta_store()
            .stored_entry(bucket, key)
            .await
            .map_err(contract)?
        else {
            return Ok(Judged {
                current: Current::default(),
                stored: None,
                path: None,
            });
        };
        let dir = self.storage.ensure_bucket(bucket).await.map_err(contract)?;
        let target = self
            .storage
            .resolve_key(&dir, key)
            .await
            .map_err(contract)?;
        let content = match fs::metadata(&target).await {
            Ok(metadata) => Content::Present {
                size: metadata.len(),
                mtime: metadata.modified().map_err(contract)?,
                identity: fsutil::file_identity(&target, &metadata),
            },
            // The file is gone. What that means is the gate's own decision
            // below: the row's **recorded** marker flag says which state this
            // is — the simple-DELETE path writes it before removing the file,
            // and every object write clears it — while a row that still
            // describes content is an out-of-band removal, where the mirror
            // wins. The flag is read, never inferred from the row's shape: a
            // genuinely zero-byte object's row is shape-identical to a
            // marker's on a filesystem without a file identity.
            Err(err) if err.kind() == ErrorKind::NotFound => Content::Missing,
            Err(err) => return Err(contract(err)),
        };
        // The gate's verdict — the ONE home of the decision
        // (`meta::gate`), shared with the plain read's
        // `resolve_object_info`, so the two can never disagree about what is
        // current — and the slot it names.
        let slot = match gate(&stored, content) {
            // The recorded identity names what the key holds, or the key
            // holds nothing and the row is the recorded delete marker, which
            // is the current version itself. A row whose file is still there
            // is content whatever it was written for, so the marker flag is
            // the one the missing file state reads.
            Verdict::Live => Current {
                exists: true,
                version: stored.version_id.clone(),
                is_delete_marker: stored.is_delete_marker && matches!(content, Content::Missing),
                seq: stored.version_seq,
                degraded: false,
            },
            // The recorded identity is stale: the content drifted from the
            // row ([`entry_matches`]), or the file is gone while the row
            // still described content. The object's version identity
            // degrades to the null version — a stale identifier is never
            // served as if it still named content. The row still records an
            // identity while the slot records none, which the `degraded` flag
            // carries: the two are not the same state as a recorded null
            // version.
            Verdict::Degraded => Current {
                exists: true,
                version: None,
                is_delete_marker: false,
                seq: 0,
                degraded: stored.version_id.is_some(),
            },
        };
        // The path rides along only when the key has a file to open: a
        // marker's slot and a file removed out of band have nothing.
        let path = match content {
            Content::Present { .. } => Some(target),
            Content::Missing => None,
        };
        Ok(Judged {
            current: slot,
            stored: Some(stored),
            path,
        })
    }
}

#[async_trait]
impl CurrentRows for Rows {
    async fn current(&self, bucket: &Name, key: &object::Key) -> Result<Current, storage::Error> {
        Ok(self.current_with_row(bucket, key).await?.current)
    }
}

#[async_trait]
impl VersionRows for Rows {
    async fn state(&self, bucket: &Name) -> Result<State, storage::Error> {
        let bucket = bucket.clone();
        self.storage
            .handle()
            .read(move |txn| {
                Ok(bucket_versioning::Table::open_readonly(txn)?
                    .get(&bucket)?
                    .unwrap_or_default())
            })
            .map_err(contract)
    }

    async fn set_state(&self, bucket: &Name, state: State) -> Result<(), storage::Error> {
        let bucket = bucket.clone();
        self.storage
            .handle()
            .write(move |txn| {
                bucket_versioning::Table::open(txn)?.set(&bucket, state)?;
                Ok(())
            })
            .await
            .map_err(contract)
    }

    async fn put_version_row(
        &self,
        bucket: &Name,
        key: &object::Key,
        superseded: &Superseded,
    ) -> Result<(), storage::Error> {
        let bucket = bucket.clone();
        let key = key.clone();
        let seq = superseded.seq;
        let superseded = superseded.clone();
        let (reported_bucket, reported_key) = (bucket.clone(), key.clone());
        let conflicted = self
            .storage
            .handle()
            .write(move |txn| {
                // The outgoing version's metadata rides in the object-meta row
                // this transaction reads — full per-version parity (the
                // recorded ETag — the composed `-N` form included, which no
                // re-hash could reproduce — plus tags, checksum, owner, ACL,
                // each reusing its wire encoding) and the stamp the version
                // was written with. A missing row (a marker whose slot is kept
                // elsewhere) contributes the empty wires and this
                // transaction's clock.
                let stored = {
                    let table = meta::Table::open(txn)?;
                    table.get(&bucket, &key)?
                };
                let now = to_nanos(SystemTime::now());
                let (etag, tags, checksum, owner, acl) = meta::wires(stored.as_ref());
                // The row is committed **before** the key path is
                // overwritten, so it is not history yet: `became_noncurrent`
                // stays `0` (the crash window's adopted row — its version is
                // still the current one), and the write's own state
                // transaction stamps it when the current slot moves off it
                // (`Row::is_history`). A re-supersede keeps whatever stamp the
                // row already carries: the adoption path rewrites its own row,
                // and a stamped row must never be un-stamped by it.
                let existing = {
                    let table = rows::Table::open(txn)?;
                    table.get(&bucket, &key, seq)?
                };
                let stamp = existing
                    .as_ref()
                    .map_or(0, |existing| existing.became_noncurrent);
                let row = rows::Row {
                    id: wire_of(superseded.id.as_ref()),
                    blob: rows::encode_blob(&superseded.blob),
                    size: superseded.blob.len(),
                    etag,
                    created: stored.as_ref().map_or(now, |stored| stored.mtime),
                    became_noncurrent: stamp,
                    kind: if superseded.is_delete_marker {
                        rows::Kind::Marker
                    } else {
                        rows::Kind::Object
                    },
                    tags,
                    checksum,
                    owner,
                    acl,
                    seq: superseded.seq,
                };
                // The row at this sequence may only be rewritten by the same
                // version (the crash-window adoption path re-runs a supersede
                // whose row is already committed). A *different* version
                // there — a different identity or kind, or the same identity
                // and kind naming different content (the null-vs-null shape,
                // where the identity wire alone cannot tell two versions
                // apart) — is another version's committed row: overwriting it
                // would replace that version's content with this one's and
                // drop its blob out of the live set — silent, unrecoverable
                // loss. Refuse, and name the state. The content address is
                // compared by its blob (hash + shape) and its byte count.
                if let Some(existing) = &existing
                    && (existing.id != row.id
                        || existing.kind != row.kind
                        || existing.blob != row.blob
                        || existing.size != row.size)
                {
                    return Ok(Some(existing.id.clone()));
                }
                // The null-version convergence (AWS's one-null-version model):
                // a key holds at most one `null`-id row, so the index's single
                // `null` entry always names it and no older row is stranded.
                // When this supersede commits the null identity and an older
                // null-id row already sits in history, that row is replaced —
                // removed with its index entry in this same transaction —
                // rather than a second null-id row being added: the index is
                // keyed by the identity and names only one of them, so the
                // older row would be permanently unaddressable (and would stay
                // so after the pointed-at row expired). The row at this
                // supersede's own sequence is never touched — that is the
                // crash-window adoption path above. The trigger is the write
                // plan's one rule, never a second spelling of it.
                if converges_null(superseded.id.as_ref()) {
                    // The key holds at most one null-id row (the convergence
                    // rule this transaction enforces), so the index's single
                    // `null` entry names the row to displace — one point
                    // read, never a walk of the key's whole history. An entry
                    // naming this supersede's own sequence is the crash-window
                    // adoption of its own row: nothing to displace.
                    let stale = {
                        let index = version_index::Table::open(txn)?;
                        index.seq_of(bucket.as_ref().as_str(), key.as_ref().as_str(), &row.id)?
                    };
                    if let Some(seq) = stale
                        && seq != row.seq
                    {
                        {
                            let mut table = rows::Table::open(txn)?;
                            table.remove(&bucket, &key, seq)?;
                        }
                        {
                            let mut index = version_index::Table::open(txn)?;
                            index.remove_pointing_at(&bucket, &key, &row.id, seq)?;
                        }
                    }
                }
                // One transaction, one table handle at a time (the shared
                // handle borrows the transaction exclusively): the row and its
                // index entry commit together or not at all.
                {
                    let mut table = rows::Table::open(txn)?;
                    table.put(&bucket, &key, &row)?;
                }
                {
                    let mut index = version_index::Table::open(txn)?;
                    index.put(&bucket, &key, &row.id, row.seq)?;
                }
                Ok(None)
            })
            .await
            .map_err(contract)?;
        match conflicted {
            Some(existing) => Err(row_conflict(
                &reported_bucket,
                &reported_key,
                seq,
                &existing,
            )),
            None => Ok(()),
        }
    }

    async fn remove_version_row(
        &self,
        bucket: &Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<(), storage::Error> {
        let bucket = bucket.clone();
        let key = key.clone();
        self.storage
            .handle()
            .write(move |txn| {
                // The index entry is keyed by the identity, so the row is read
                // in the same transaction that drops it — the two never
                // disagree. The entry goes only when it points at this row:
                // two rows can share the null identity, and expiring one must
                // not retire the other's entry. Idempotent: a sequence that
                // holds no row is `Ok`.
                let id = {
                    let table = rows::Table::open(txn)?;
                    table.get(&bucket, &key, seq)?.map(|row| row.id)
                };
                let Some(id) = id else {
                    return Ok(());
                };
                {
                    let mut table = rows::Table::open(txn)?;
                    table.remove(&bucket, &key, seq)?;
                }
                {
                    let mut index = version_index::Table::open(txn)?;
                    index.remove_pointing_at(&bucket, &key, &id, seq)?;
                }
                Ok(())
            })
            .await
            .map_err(contract)
    }

    /// One transaction for the whole list: the retention sweep's batch. The
    /// index entries are read and dropped with their rows in that same
    /// transaction, so the two never disagree, and a crash leaves every
    /// listed row or none of them.
    async fn remove_version_rows(
        &self,
        bucket: &Name,
        key: &object::Key,
        seqs: &[u64],
    ) -> Result<(), storage::Error> {
        let bucket = bucket.clone();
        let key = key.clone();
        let seqs = seqs.to_vec();
        self.storage
            .handle()
            .write(move |txn| {
                for seq in seqs {
                    let id = {
                        let table = rows::Table::open(txn)?;
                        table.get(&bucket, &key, seq)?.map(|row| row.id)
                    };
                    let Some(id) = id else {
                        continue; // idempotent: a sequence holding no row is Ok
                    };
                    {
                        let mut table = rows::Table::open(txn)?;
                        table.remove(&bucket, &key, seq)?;
                    }
                    {
                        let mut index = version_index::Table::open(txn)?;
                        index.remove_pointing_at(&bucket, &key, &id, seq)?;
                    }
                }
                Ok(())
            })
            .await
            .map_err(contract)
    }

    async fn latest(
        &self,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<Option<(u64, rows::Row)>, storage::Error> {
        let bucket = bucket.clone();
        let key = key.clone();
        self.storage
            .handle()
            .read(move |txn| Ok(rows::Table::open_readonly(txn)?.latest(&bucket, &key)?))
            .map_err(contract)
    }

    async fn bucket_rows(&self, bucket: &Name) -> Result<Vec<rows::Row>, storage::Error> {
        let bucket = bucket.clone();
        self.storage
            .handle()
            .read_blocking(move |txn| {
                let table = rows::Table::open_readonly(txn)?;
                let mut out = Vec::new();
                table.for_bucket_of(bucket.as_ref(), |_, row| {
                    out.push(row);
                    Ok(())
                })?;
                Ok(out)
            })
            .await
            .map_err(contract)
    }
}

/// The fs implementation of [`ContentPort`]: the key's content at its key
/// path, streamed out and published through the atomic writer.
#[derive(Debug, Clone)]
pub(crate) struct Files {
    /// The backend this adapter serves.
    storage: FsStorage,
}

impl Files {
    /// Serve a backend's content.
    pub(crate) fn new(storage: FsStorage) -> Self {
        Self { storage }
    }

    /// The key's target path, with the backend's usual guards: the bucket
    /// must exist, and the symlink policy and containment proof run exactly
    /// as they do on the object paths.
    async fn target(&self, bucket: &Name, key: &object::Key) -> Result<PathBuf, Error> {
        let dir = self.storage.ensure_bucket(bucket).await?;
        self.storage.resolve_key(&dir, key).await
    }
}

#[async_trait]
impl ContentPort for Files {
    async fn open(
        &self,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<Option<BodyStream>, storage::Error> {
        let target = self.target(bucket, key).await.map_err(contract)?;
        let file = match fs::File::open(&target).await {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(contract(err)),
        };
        let len = file.metadata().await.map_err(contract)?.len();
        // The whole file, streamed in bounded chunks by the shared helper the
        // read path serves ranges with — the supersede never buffers the
        // object whole (constitution V). The end-of-body policy matches the
        // read path: the range is a clamp on the file served as-is.
        Ok(Some(
            fsutil::file_stream(file, 0, len.saturating_sub(1), fsutil::ShortRead::EndBody).await,
        ))
    }

    async fn publish(
        &self,
        bucket: &Name,
        key: &object::Key,
        body: BodyStream,
    ) -> Result<PublishedBody, storage::Error> {
        let target = self.target(bucket, key).await.map_err(contract)?;
        // The existing temp-and-rename path: a reader never sees a torn
        // object, and a failed publish leaves the key path untouched. The
        // writer answers the content MD5 it computed while streaming and
        // the byte count it wrote, so the caller describes the restored
        // version without a second read of the file it just wrote.
        let (etag, size) = self
            .storage
            .writer()
            .write(&target, body)
            .await
            .map_err(contract)?;
        Ok(PublishedBody { etag, size })
    }
}

#[cfg(test)]
mod tests {
    use std::fs as std_fs;

    use super::*;
    use crate::{
        _core::{
            ETag, acl,
            blob::{BlobRef, BlockHash, BlockStore},
            bucket,
            object::Tags,
            to_nanos, version,
        },
        testutil,
    };

    /// A storage with a `demo` bucket directory, its name and a key.
    async fn fixture() -> (tempfile::TempDir, FsStorage, Name, object::Key) {
        let (root, storage) = testutil::storage();
        fs::create_dir(root.path().join("demo")).await.unwrap();
        (
            root,
            storage,
            bucket::name("demo").unwrap(),
            object::key("notes.txt").unwrap(),
        )
    }

    /// The key's target path.
    async fn target(storage: &FsStorage, bucket: &Name, key: &object::Key) -> PathBuf {
        let dir = storage.ensure_bucket(bucket).await.unwrap();
        storage.resolve_key(&dir, key).await.unwrap()
    }

    /// Plant a versioned object: the file at the key path plus the
    /// `object_meta` row recording its identity and sequence — the shape
    /// Task 4's write path commits.
    async fn plant(
        storage: &FsStorage,
        bucket: &Name,
        key: &object::Key,
        bytes: &[u8],
        id: Option<&str>,
        seq: u64,
    ) {
        let path = target(storage, bucket, key).await;
        fs::write(&path, bytes).await.unwrap();
        let metadata = std_fs::metadata(&path).unwrap();
        let stored = meta::Stored {
            version_id: id.map(|id| version::Id::new(id).unwrap()),
            version_seq: seq,
            is_delete_marker: false,
            etag: ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
            size: metadata.len(),
            mtime: to_nanos(metadata.modified().unwrap()),
            file_identity: fsutil::file_identity(&path, &metadata),
            tags: Tags::empty(),
            checksum: None,
            owner: None,
            acl: acl::Acl::default_private(None),
        };
        let bucket = bucket.clone();
        let key = key.clone();
        storage
            .handle()
            .write(move |txn| {
                meta::Table::open(txn)?.put(&bucket, &key, &stored)?;
                Ok(())
            })
            .await
            .unwrap();
    }

    /// Plant a delete marker: the marker-shaped row the delete path writes
    /// (no content, the empty-content ETag, no file identity) with no file at
    /// the key path — the row `current_with_row` reads as a marker.
    async fn plant_marker(
        storage: &FsStorage,
        bucket: &Name,
        key: &object::Key,
        id: Option<&str>,
        seq: u64,
    ) {
        let stored = meta::Stored {
            version_id: id.map(|id| version::Id::new(id).unwrap()),
            version_seq: seq,
            is_delete_marker: true,
            etag: ETag::EMPTY,
            size: 0,
            mtime: to_nanos(SystemTime::now()),
            file_identity: 0,
            tags: Tags::empty(),
            checksum: None,
            owner: None,
            acl: acl::Acl::default_private(None),
        };
        let bucket = bucket.clone();
        let key = key.clone();
        storage
            .handle()
            .write(move |txn| {
                meta::Table::open(txn)?.put(&bucket, &key, &stored)?;
                Ok(())
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_supersede_moves_the_file_into_the_block_store_and_leaves_the_row_not_yet_history() {
        let (_root, storage, bucket, key) = fixture().await;
        plant(&storage, &bucket, &key, b"first", Some("v1"), 1).await;
        let versions = storage.versions(&bucket);

        versions.set_state(&bucket, State::Enabled).await.unwrap();
        assert_eq!(versions.state(&bucket).await.unwrap(), State::Enabled);
        versions.supersede(&bucket, &key).await.unwrap();

        let (seq, row) = versions
            .latest(&bucket, &key)
            .await
            .unwrap()
            .expect("a version row");
        assert_eq!(
            (seq, row.id.as_str()),
            (1, "v1"),
            "the row carries the identity and sequence the meta row recorded"
        );
        assert_eq!(row.size, 5);
        assert_eq!(row.kind, rows::Kind::Object);
        assert_eq!(
            row.etag(),
            ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
            "the row carries the ETag the object_meta row recorded (here deliberately \
             not the content's MD5), never a hash of the bytes"
        );
        // The engine's supersede alone is the crash window's first half: the
        // row is committed while the version is still current, so it is not
        // history yet — `became_noncurrent` stays 0 and `is_history` says no
        // for the slot's own sequence. The write's state transaction stamps it
        // (see `the_publish_stamps_the_superseded_row_as_history` in the
        // version tests).
        assert_eq!(
            row.became_noncurrent, 0,
            "the row is not stamped until the current slot moves off it"
        );
        assert!(
            !row.is_history(rows::Slot::At(1)),
            "the adopted row is the current version's own row"
        );
        let block = versions.blobs().blocks().block_path(row.blob().hash());
        assert!(
            block.starts_with(storage.blobs_dir()),
            "the block lives under the bucket's own state-dir subtree: {}",
            block.display()
        );
        assert!(
            versions
                .blobs()
                .blocks()
                .contains(row.blob().hash())
                .await
                .unwrap(),
            "the content is in the block store"
        );
    }

    #[tokio::test]
    async fn a_re_supersede_adopts_its_own_row() {
        // The crash window's recovery: the same version superseded twice
        // rewrites the row it already committed, never a second identity.
        let (_root, storage, bucket, key) = fixture().await;
        plant(&storage, &bucket, &key, b"first", Some("v1"), 1).await;
        let versions = storage.versions(&bucket);

        versions.supersede(&bucket, &key).await.unwrap();
        versions.supersede(&bucket, &key).await.unwrap();

        let (seq, row) = versions.latest(&bucket, &key).await.unwrap().unwrap();
        assert_eq!(
            (seq, row.id.as_str(), row.kind),
            (1, "v1", rows::Kind::Object)
        );
        assert_eq!(row.size, 5);
    }

    #[tokio::test]
    async fn an_out_of_band_removal_degrades_the_slot_and_supersedes_nothing() {
        // The file is gone while `object_meta` still records the committed
        // version: the design's out-of-band rule — the identity degrades to
        // the null version and the next write treats the missing file as the
        // null version it now is, preserving nothing. The committed row is
        // the newer, row-shaped truth and survives untouched.
        let (_root, storage, bucket, key) = fixture().await;
        plant(&storage, &bucket, &key, b"first", Some("v1"), 1).await;
        let versions = storage.versions(&bucket);
        versions.supersede(&bucket, &key).await.unwrap();
        let (_, before) = versions.latest(&bucket, &key).await.unwrap().unwrap();

        let path = target(&storage, &bucket, &key).await;
        fs::remove_file(&path).await.unwrap();
        let current = versions.current(&bucket, &key).await.unwrap();
        assert!(
            current.is_null(),
            "no file plus a row still describing content is the null version, not a marker"
        );
        assert_eq!(current.seq, 0, "the null version's sequence");

        versions.supersede(&bucket, &key).await.unwrap();
        let (_, after) = versions.latest(&bucket, &key).await.unwrap().unwrap();
        assert_eq!(
            after, before,
            "nothing was current to preserve: the committed version's row is byte-identical"
        );
        assert_eq!(after.kind, rows::Kind::Object);
        assert_eq!(
            after.blob(),
            BlobRef::single(BlockHash::of(b"first"), 5),
            "and its content still reads"
        );
    }

    #[tokio::test]
    async fn a_supersede_that_would_overwrite_different_content_at_the_same_identity_is_refused() {
        // The null-vs-null shape: two distinct versions share the identity
        // wire (`null`, the null version's) and the kind, so the identity
        // check alone cannot tell them apart — only the content address can.
        // A guard that compared identity and kind only would overwrite the
        // committed row and drop its blob out of the live set.
        let (_root, storage, bucket, key) = fixture().await;
        let rows = Rows::new(storage.clone());
        let committed = BlobRef::single(BlockHash::of(b"first"), 5);
        rows.put_version_row(
            &bucket,
            &key,
            &Superseded {
                id: None,
                seq: 0,
                blob: committed.clone(),
                is_delete_marker: false,
            },
        )
        .await
        .unwrap();

        // The same identity, kind and byte count — a different content
        // address (`first` and `secnd` are both five bytes).
        let err = rows
            .put_version_row(
                &bucket,
                &key,
                &Superseded {
                    id: None,
                    seq: 0,
                    blob: BlobRef::single(BlockHash::of(b"secnd"), 5),
                    is_delete_marker: false,
                },
            )
            .await
            .unwrap_err();
        let storage::Error::Io(io) = err else {
            panic!("expected the refusal as a contract I/O error, got {err:?}");
        };
        assert_eq!(io.kind(), ErrorKind::Other, "the refusal is loud: {io}");
        assert!(
            io.to_string().contains("sequence 0"),
            "and names the sequence it would have destroyed: {io}"
        );

        // The committed row is untouched, content address included.
        let (_, row) = storage
            .versions(&bucket)
            .latest(&bucket, &key)
            .await
            .unwrap()
            .expect("the committed row survives");
        assert_eq!(
            (row.seq, row.id.as_str(), row.kind),
            (0, "null", rows::Kind::Object)
        );
        assert_eq!(
            row.blob(),
            committed,
            "the committed version's content still reads"
        );
    }

    #[tokio::test]
    async fn materializing_a_version_writes_its_bytes_back_onto_the_key_path() {
        let (_root, storage, bucket, key) = fixture().await;
        plant(&storage, &bucket, &key, b"first", Some("v1"), 1).await;
        let versions = storage.versions(&bucket);
        versions.supersede(&bucket, &key).await.unwrap();
        let (_, row) = versions.latest(&bucket, &key).await.unwrap().unwrap();

        // The overwrite that follows a supersede; the materialize restores.
        let path = target(&storage, &bucket, &key).await;
        fs::write(&path, b"second").await.unwrap();
        let published = versions
            .materialize(&bucket, &key, &row)
            .await
            .unwrap()
            .expect("an object row publishes content");
        assert_eq!(published.size, 5, "the publish answers the byte count");
        assert_eq!(
            published.etag,
            ETag::from_content(b"first"),
            "and the content MD5 its write path computed while streaming"
        );

        assert_eq!(fs::read(&path).await.unwrap(), b"first");
    }

    #[tokio::test]
    async fn current_applies_the_staleness_gate_and_tells_a_marker_row_from_a_missing_file() {
        let (_root, storage, bucket, key) = fixture().await;
        let versions = storage.versions(&bucket);

        // Nothing recorded: no current version.
        assert!(!versions.current(&bucket, &key).await.unwrap().exists);

        // A recorded version whose file matches: the real identity.
        plant(&storage, &bucket, &key, b"first", Some("v1"), 1).await;
        let current = versions.current(&bucket, &key).await.unwrap();
        assert!(current.exists && !current.is_delete_marker);
        assert_eq!(current.version.as_ref().unwrap().as_str(), "v1");
        assert_eq!(current.seq, 1);
        assert!(!current.is_null());

        // The file is replaced out of band: the gate degrades the identity to
        // the null version, never a stale identifier.
        let path = target(&storage, &bucket, &key).await;
        fs::write(&path, b"replaced out of band, and longer")
            .await
            .unwrap();
        let current = versions.current(&bucket, &key).await.unwrap();
        assert!(current.exists && current.is_null());
        assert_eq!(current.seq, 0, "the null version's sequence");

        // The file is gone while its row still describes the content it held:
        // an out-of-band removal, which the mirror reports as the null version
        // — not a delete marker the API never created.
        fs::remove_file(&path).await.unwrap();
        let current = versions.current(&bucket, &key).await.unwrap();
        assert!(
            current.exists && current.is_null() && !current.is_delete_marker,
            "the removal degrades the identity to null, never to a marker"
        );
        assert!(
            current.version.is_none(),
            "and the recorded id is not served"
        );

        // Only a row whose **recorded** marker flag is set — what the delete
        // path writes before it removes the file — reads as a delete marker.
        plant_marker(&storage, &bucket, &key, Some("m1"), 3).await;
        let current = versions.current(&bucket, &key).await.unwrap();
        assert!(current.exists && current.is_delete_marker);
        assert_eq!(current.version.as_ref().unwrap().as_str(), "m1");
        assert_eq!(current.seq, 3);
        assert!(!current.is_null(), "a marker is not the null version");
    }

    #[tokio::test]
    async fn a_zero_byte_object_row_never_reads_as_a_marker() {
        // The residual the recorded flag closes: a genuinely zero-byte
        // object's row on a filesystem whose file identity is unavailable is
        // shape-identical to a marker's row (size 0, the empty-content ETag,
        // identity 0), so the shape cannot discriminate. The object write
        // clears the flag, and the absence of the file after an out-of-band
        // removal must therefore degrade to the null version — never the
        // marker the API never created.
        let (_root, storage, bucket, key) = fixture().await;
        let versions = storage.versions(&bucket);
        let stored = meta::Stored {
            version_id: Some(version::Id::new("z1").unwrap()),
            version_seq: 1,
            is_delete_marker: false,
            etag: ETag::EMPTY,
            size: 0,
            mtime: to_nanos(SystemTime::now()),
            file_identity: 0,
            tags: Tags::empty(),
            checksum: None,
            owner: None,
            acl: acl::Acl::default_private(None),
        };
        let write_bucket = bucket.clone();
        let write_key = key.clone();
        storage
            .handle()
            .write(move |txn| {
                meta::Table::open(txn)?.put(&write_bucket, &write_key, &stored)?;
                Ok(())
            })
            .await
            .unwrap();

        // The row's shape matches a marker's exactly; only the flag differs.
        let current = versions.current(&bucket, &key).await.unwrap();
        assert!(
            current.exists && current.is_null() && !current.is_delete_marker,
            "a zero-byte object's row must never read as a marker: {current:?}"
        );
        assert_eq!(current.seq, 0, "the removal degrades to the null version");
    }

    #[tokio::test]
    async fn an_out_of_band_edit_supersedes_as_the_null_version() {
        let (_root, storage, bucket, key) = fixture().await;
        plant(&storage, &bucket, &key, b"first", Some("v1"), 1).await;
        let versions = storage.versions(&bucket);

        let path = target(&storage, &bucket, &key).await;
        fs::write(&path, b"edited out of band").await.unwrap();
        versions.supersede(&bucket, &key).await.unwrap();

        let (seq, row) = versions.latest(&bucket, &key).await.unwrap().unwrap();
        assert_eq!(
            (seq, row.id.as_str()),
            (0, "null"),
            "the gated slot is the null version"
        );
        assert_eq!(row.size, 18);
    }

    #[tokio::test]
    async fn a_marker_current_supersedes_without_content() {
        let (_root, storage, bucket, key) = fixture().await;
        plant_marker(&storage, &bucket, &key, Some("m1"), 3).await;
        let versions = storage.versions(&bucket);

        versions.supersede(&bucket, &key).await.unwrap();

        let (seq, row) = versions.latest(&bucket, &key).await.unwrap().unwrap();
        assert_eq!((seq, row.id.as_str()), (3, "m1"));
        assert_eq!(row.kind, rows::Kind::Marker);
        assert!(row.blob().is_empty(), "a marker carries the empty blob");
        assert!(
            versions
                .live_blocks(&bucket)
                .await
                .unwrap()
                .contains(&BlockHash::of(b"")),
            "the marker's live reference is the empty blob's block"
        );
        assert!(
            !versions
                .blobs()
                .blocks()
                .contains(&BlockHash::of(b"first"))
                .await
                .unwrap(),
            "a marker's supersede reads no content"
        );
    }

    #[tokio::test]
    async fn the_committed_version_row_and_its_index_entry_drop_together() {
        let (_root, storage, bucket, key) = fixture().await;
        plant(&storage, &bucket, &key, b"first", Some("v1"), 1).await;
        let versions = storage.versions(&bucket);
        versions.supersede(&bucket, &key).await.unwrap();
        assert!(
            versions
                .live_blocks(&bucket)
                .await
                .unwrap()
                .contains(&BlockHash::of(b"first"))
        );

        let indexed = |storage: &FsStorage| {
            let bucket = bucket.clone();
            let key = key.clone();
            storage
                .handle()
                .read(move |txn| {
                    Ok(version_index::Table::open_readonly(txn)?.seq_of(&bucket, &key, "v1")?)
                })
                .unwrap()
        };
        assert_eq!(indexed(&storage), Some(1));

        versions.expire(&bucket, &key, 1).await.unwrap();
        versions.expire(&bucket, &key, 1).await.unwrap(); // idempotent

        assert!(versions.latest(&bucket, &key).await.unwrap().is_none());
        assert_eq!(
            indexed(&storage),
            None,
            "the index entry drops with the row"
        );
    }

    #[tokio::test]
    async fn removing_a_version_row_that_holds_nothing_is_a_no_op() {
        // The port's missing-row branch: a sequence with no row removes
        // nothing and errors nothing, leaving a neighbouring version's row
        // and index entry untouched — the idempotence the sweep's expiry and
        // a repeated version delete both stand on.
        let (_root, storage, bucket, key) = fixture().await;
        plant(&storage, &bucket, &key, b"first", Some("v1"), 1).await;
        let versions = storage.versions(&bucket);
        versions.supersede(&bucket, &key).await.unwrap();
        let rows = Rows::new(storage.clone());

        let indexed = |storage: &FsStorage| {
            let bucket = bucket.clone();
            let key = key.clone();
            storage
                .handle()
                .read(move |txn| {
                    Ok(version_index::Table::open_readonly(txn)?.seq_of(&bucket, &key, "v1")?)
                })
                .unwrap()
        };

        // A sequence the key never used: nothing drops.
        rows.remove_version_row(&bucket, &key, 7).await.unwrap();
        assert!(
            rows.latest(&bucket, &key).await.unwrap().is_some(),
            "the committed row is untouched"
        );
        assert_eq!(indexed(&storage), Some(1), "its index entry rides");

        // The row that IS there still drops, with its index entry...
        rows.remove_version_row(&bucket, &key, 1).await.unwrap();
        assert!(rows.latest(&bucket, &key).await.unwrap().is_none());
        assert_eq!(indexed(&storage), None);

        // ...and repeating it takes the missing-row branch again.
        rows.remove_version_row(&bucket, &key, 1).await.unwrap();
    }

    #[tokio::test]
    async fn superseding_the_null_version_converges_on_one_null_id_row() {
        // The null identity is one wire for what would be two rows (a
        // superseded null object version and a null-identity marker); the
        // write side converges them, so the index's single `null` entry
        // always names the one null-id row and no older row is stranded
        // unaddressable once the pointed-at row expires.
        let (_root, storage, bucket, key) = fixture().await;
        let rows = Rows::new(storage.clone());
        rows.put_version_row(
            &bucket,
            &key,
            &Superseded {
                id: None,
                seq: 0,
                blob: BlobRef::single(BlockHash::of(b"first"), 5),
                is_delete_marker: false,
            },
        )
        .await
        .unwrap();
        rows.put_version_row(
            &bucket,
            &key,
            &Superseded {
                id: None,
                seq: 3,
                blob: BlobRef::single(BlockHash::of(b""), 0),
                is_delete_marker: true,
            },
        )
        .await
        .unwrap();

        let indexed = |storage: &FsStorage| {
            let bucket = bucket.clone();
            let key = key.clone();
            storage
                .handle()
                .read(move |txn| {
                    Ok(version_index::Table::open_readonly(txn)?.seq_of(&bucket, &key, "null")?)
                })
                .unwrap()
        };
        assert_eq!(
            indexed(&storage),
            Some(3),
            "the index names the converged row"
        );
        let surviving = {
            let bucket = bucket.clone();
            let key = key.clone();
            storage
                .handle()
                .read(move |txn| {
                    let mut seen = Vec::new();
                    rows::Table::open_readonly(txn)?.for_key(&bucket, &key, |row| {
                        seen.push(row);
                        Ok(())
                    })?;
                    Ok(seen)
                })
                .unwrap()
        };
        assert_eq!(
            surviving.len(),
            1,
            "the supersede replaced the older null row instead of adding a second: {surviving:?}"
        );
        assert_eq!(surviving[0].seq, 3);
        assert_eq!(surviving[0].kind, rows::Kind::Marker);
        assert_eq!(
            rows.latest(&bucket, &key).await.unwrap().unwrap().0,
            3,
            "the converged row is the key's newest"
        );
    }
}
