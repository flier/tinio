//! The local filesystem implementation of [`BlockStore`].
//!
//! Layout under the root directory the store is given: blocks live at
//! `<root>/<aa>/<hex>` where `<aa>` is the address's first two hex characters
//! (a 256-way fan-out, so no directory grows with the store), and blocks being
//! staged live at `<root>/tmp/<uuid>`. `tmp` is reserved: the address fan-out
//! is hex, so it can never collide, and neither [`list`](BlockStore::list) nor
//! [`walk`](BlockStore::walk) descends into it. A **dropped** stage removes its
//! own temp (below); what is left for the caller's cleanup to reap is the
//! residue of a process that died mid-stage (the fs backend's startup repair
//! and sweep, plan 2).
//!
//! **The store is written on the shared filesystem primitives**
//! ([`tinio_util::fsutil`], 2026-10-03) rather than on copies of them: the
//! commit protocol ([`tinio_util::fsutil::commit`]) creates the fan-out
//! directory, renames the temp onto the block and syncs the directory — with
//! the EXDEV fallback a cross-volume root needs; the staged temp's `Drop` uses
//! [`tinio_util::fsutil::remove_staged_temp`], whose unlink runs off the
//! runtime worker; the served body is
//! [`tinio_util::fsutil::file_stream`]; and the directory work is
//! [`tinio_util::fsutil::ensure_dir`], [`tinio_util::fsutil::ok_if_missing`]
//! and [`tinio_util::fsutil::entries_of`]. The copies this file used to hold
//! had already drifted from the fs backend's (the cross-volume fallback and
//! the off-worker unlink were missing here), which is why the primitives moved
//! down into `tinio-util`.
//!
//! `publish` is durable before it returns: the staged content is fsynced, then
//! renamed, then the fan-out directory is fsynced. The directory sync is unix
//! only — on Windows std cannot open a directory handle, and no heavyweight
//! dependency is added for a directory flush (`tinio-fs` documents the same
//! limitation for its own writes).
//!
//! A symlink planted at a canonical block path is followed by the read path
//! (`contains`, `open`) but skipped by `walk`/`list`, which list regular files
//! only — so it is served but never swept or counted. The asymmetry is
//! deliberate and recorded: the block directory is private state (nothing but
//! this store writes it), so a planted link is the operator's own doing, and
//! the two halves must not be "fixed" apart from each other.

use std::{
    io,
    path::{Path, PathBuf},
    vec::IntoIter,
};

use bytes::Bytes;
use futures::{StreamExt, stream};
use tokio::{
    fs::{self, File},
    io::{AsyncWriteExt, BufWriter},
};
use uuid::Uuid;

use crate::{
    _core::{
        ByteRange,
        blob::{BlockHash, BlockStore, Entry, EntryStream, Published, StagedBlock},
        storage::BodyStream,
    },
    _util::fsutil::{
        DirOwnerUid, ShortRead, commit, ensure_dir, entries_of, file_stream, ok_if_missing,
        remove_staged_temp,
    },
    Error,
};

/// A block store over a local directory.
///
/// # Examples
///
/// ```rust
/// use bytes::Bytes;
/// use tinio_blob::LocalBlocks;
/// use tinio_core::blob::{BlockHash, BlockStore, Published, StagedBlock};
/// use tokio::runtime::Runtime;
///
/// let dir = tempfile::tempdir().unwrap();
/// let store = LocalBlocks::new(dir.path());
/// let hash = BlockHash::of(b"hello");
///
/// let runtime = Runtime::new().unwrap();
/// let mut staged = runtime.block_on(store.stage()).unwrap();
/// runtime
///     .block_on(staged.write(Bytes::from_static(b"hello")))
///     .unwrap();
/// assert_eq!(
///     runtime.block_on(store.publish(staged, &hash)).unwrap(),
///     Published::Stored
/// );
/// assert!(runtime.block_on(store.contains(&hash)).unwrap());
/// ```
#[derive(Debug, Clone)]
pub struct LocalBlocks {
    root: PathBuf,
}

impl LocalBlocks {
    /// Serve blocks under `root`; `new` creates nothing itself — `stage` creates
    /// `<root>/tmp` and `publish` creates the fan-out directory it needs.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_blob::LocalBlocks;
    ///
    /// let dir = tempfile::tempdir().unwrap();
    /// let root = dir.path().join("blobs");
    /// // A root is just a path: `new` accepts anything path-like and creates
    /// // nothing itself.
    /// let store = LocalBlocks::new(&root);
    /// assert!(!root.exists());
    /// assert_eq!(store.temp_dir(), root.join("tmp"));
    /// ```
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The path a block with this address is published at.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_blob::LocalBlocks;
    /// use tinio_core::BlockHash;
    ///
    /// let dir = tempfile::tempdir().unwrap();
    /// let store = LocalBlocks::new(dir.path());
    /// let hash = BlockHash::of(b"hello");
    /// let hex = hash.to_hex();
    ///
    /// assert_eq!(
    ///     store.block_path(&hash),
    ///     dir.path().join(&hex[..2]).join(&hex)
    /// );
    /// ```
    pub fn block_path(&self, hash: &BlockHash) -> PathBuf {
        let hex = hash.to_hex();
        self.root.join(&hex[..2]).join(&hex)
    }

    /// The directory staged blocks are written to.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_blob::LocalBlocks;
    ///
    /// let dir = tempfile::tempdir().unwrap();
    /// let store = LocalBlocks::new(dir.path());
    /// assert_eq!(store.temp_dir(), dir.path().join("tmp"));
    /// ```
    pub fn temp_dir(&self) -> PathBuf {
        self.root.join("tmp")
    }
}

#[async_trait::async_trait]
impl BlockStore for LocalBlocks {
    type Error = Error;
    type Staged = Staged;

    async fn stage(&self) -> Result<Self::Staged, Self::Error> {
        let dir = self.temp_dir();
        let path = dir.join(Uuid::new_v4().to_string());
        // The temp directory exists after the first stage, so the file creation
        // is attempted first and only a missing directory pays for the create
        // and the retry — a missing parent reports `NotFound` wherever this
        // store runs. This is the per-block ingest path. The create is the
        // shared `ensure_dir`: mode-hardened private residue (spec 2026-09-05
        // §5a, the fs backend's rule) and `None` — the tmp dir is the server
        // user's.
        let file = match File::create(&path).await {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                ensure_dir(&dir, DirOwnerUid::new(None)).await?;
                File::create(&path).await?
            }
            Err(err) => return Err(err.into()),
        };
        Ok(Staged {
            path: Some(path),
            file: Some(BufWriter::new(file)),
        })
    }

    async fn publish(
        &self,
        mut staged: Self::Staged,
        hash: &BlockHash,
    ) -> Result<Published, Self::Error> {
        // The handle leaves the stage first, so the finish sequence below names
        // it explicitly; the stage's `Drop` then has nothing left to close.
        let mut file = staged.file.take().ok_or_else(|| {
            // Unreachable by ownership — only `publish` takes the handle — but
            // the contract method returns a `Result`, and a library path must
            // not panic (constitution II).
            io::Error::other("the stage's file was already taken")
        })?;
        let target = self.block_path(hash);
        if fs::try_exists(&target).await? {
            // The dedup hit: discard the staged bytes. The handle closes before
            // the unlink — the workspace's staged-temp convention (`Staged`'s
            // own `Drop` closes first too), not a platform requirement.
            drop(file);
            // The temp is disposed of right here, so its path is taken and
            // `Drop` has nothing left to unlink: the "not there" case (a
            // concurrent `tmp/` sweep reaped it) is the same stored block the
            // commit below reads as a hit.
            let temp = staged.take_path()?;
            // A failed unlink must not turn a publish that already found the
            // block into an error: the block is present, which is what the
            // caller asked for, and the startup repair/sweep reaps the
            // residue.
            if let Err(err) = fs::remove_file(&temp).await
                && err.kind() != io::ErrorKind::NotFound
            {
                tracing::warn!(error = %err, "a dedup publish left its staged temp behind");
            }
            return Ok(Published::Existing);
        }
        file.flush().await?;
        let file = file.into_inner();
        file.sync_all().await?;
        // The bytes are on disk: close the handle before the rename makes the
        // name visible — the same close-before-rename order `tinio-fs`'s
        // `AtomicWriter::write_temp` keeps, a workspace convention rather than
        // a platform rule.
        drop(file);
        // From here the commit is what hands the temp on, so its path is taken:
        // `Drop` would otherwise pay a guaranteed-failing unlink on every
        // publish. The shared protocol creates the fan-out directory (the
        // steady-state probe; the first block in a prefix pays for the create),
        // renames — with the cross-volume fallback a root on another volume
        // needs — syncs the directory, and removes the temp best-effort on
        // failure, so the paths that never reach a stored block leave no
        // residue. `Some(&self.root)` bounds the ancestor sync the first block
        // into a new fan-out directory needs (the root is this store's
        // bucket-root analogue): the root exists, or is created by this same
        // call, so the walk stops there rather than climbing to the filesystem
        // root.
        let temp = staged.take_path()?;
        match commit(&temp, &target, Some(&self.root), DirOwnerUid::new(None)).await {
            Ok(()) => Ok(Published::Stored),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                // The temp vanished (a crashed stage's residue was reaped, or a
                // concurrent publish won): a concurrent publish under the same
                // hash stores identical bytes, so a present target is a hit.
                if fs::try_exists(&target).await? {
                    Ok(Published::Existing)
                } else {
                    Err(Error::Io(err))
                }
            }
            Err(err) => Err(Error::Io(err)),
        }
    }

    async fn contains(&self, hash: &BlockHash) -> Result<bool, Self::Error> {
        Ok(fs::try_exists(self.block_path(hash)).await?)
    }

    async fn open(
        &self,
        hash: &BlockHash,
        range: Option<ByteRange>,
    ) -> Result<BodyStream, Self::Error> {
        let path = self.block_path(hash);
        // A block that is not there is `NotFound` I/O: the contract pins that
        // shape, so a caller — and a sweep racing a read — tells "not there"
        // from any other failure by the error kind.
        let file = File::open(&path).await?;
        // The one open is the length source too (the same merge the fs
        // backend's reads make) — `resolve` clamps against it.
        let len = file.metadata().await?.len();
        let (start, end) = match range {
            // `resolve` answers an inclusive pair.
            Some(range) => range.resolve(len).map_err(Error::from)?,
            // No bytes exist to stream — not a 1-byte read, which could
            // overshoot if the block grew after the stat.
            None if len == 0 => return Ok(Box::pin(stream::empty())),
            None => (0, len - 1),
        };
        // Served as a stream, never materialized whole (constitution V): the
        // shared ranged stream hands off at most `CHUNK_SIZE` per iteration,
        // from one reused buffer. The file ending before the range is met is an
        // error (`ShortRead::Error`): `end` never exceeds the length read at
        // open, so the bytes must still be there — a short read is truncation
        // or corruption, never a normal end of the block. The fs backend, whose
        // range is a clamp on an object it serves as-is, keeps the end-of-body
        // policy instead.
        Ok(file_stream(file, start, end, ShortRead::Error).await)
    }

    async fn remove(&self, hash: &BlockHash) -> Result<(), Self::Error> {
        // Idempotent by contract: a block that is not there is already removed
        // — a repeated sweep pass, or a sweep racing a reader.
        ok_if_missing(fs::remove_file(self.block_path(hash)).await).map_err(Error::Io)
    }

    async fn list(
        &self,
        after: Option<&BlockHash>,
        limit: usize,
    ) -> Result<Vec<Entry>, Self::Error> {
        // `list` is the walk stopped at the limit: one page, from the same
        // one-pass traversal a whole walk makes.
        let mut walk = self.walk(after).await?;
        let mut out = Vec::new();
        while out.len() < limit {
            let Some(entry) = walk.next().await else {
                break;
            };
            out.push(entry?);
        }
        Ok(out)
    }

    async fn walk(
        &self,
        after: Option<&BlockHash>,
    ) -> Result<EntryStream<Self::Error>, Self::Error> {
        let cursor = after.map(BlockHash::to_hex);
        let prefixes = self.fan_out_dirs(cursor.as_deref()).await?;
        let state = Walk {
            root: self.root.clone(),
            prefixes: prefixes.into_iter(),
            cursor,
            pending: Vec::new().into_iter(),
        };
        // Each fan-out directory is opened when the stream reaches it, read
        // whole, and dropped before its entries are handed out — so a sweep can
        // remove the entry it was just given without disturbing the walk.
        Ok(Box::pin(stream::try_unfold(
            state,
            |mut state| async move {
                loop {
                    if let Some(entry) = state.pending.next() {
                        return Ok(Some((entry, state)));
                    }
                    let Some(prefix) = state.prefixes.next() else {
                        return Ok(None);
                    };
                    state.pending = fan_out_entries(&state.root, &prefix, state.cursor.as_deref())
                        .await?
                        .into_iter();
                }
            },
        )))
    }
}

impl LocalBlocks {
    /// The store's fan-out directory names, ascending — a root that is not
    /// there yet holds no blocks and has none.
    async fn fan_out_dirs(&self, cursor: Option<&str>) -> Result<Vec<String>, Error> {
        // The shared listing reads a missing root as empty (a store with no
        // blocks yet) and propagates every other read error.
        let mut prefixes = Vec::new();
        for (path, name) in entries_of(&self.root).await.map_err(Error::Io)? {
            // Two lowercase hex digits — `tmp` is three characters, so the
            // length test excludes it, and lowercase only keeps the sort below
            // in address order (`AB` sorts before `aa`). The name alone is not
            // enough: a *file* wearing a fan-out name must be skipped, or the
            // per-directory read below fails with `NotADirectory` and every
            // walk — and so every sweep — errors until a human removes it.
            let is_dir = fs::metadata(&path)
                .await
                .map(|meta| meta.is_dir())
                .unwrap_or(false);
            if is_dir && name.len() == 2 && name.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f'))
            {
                prefixes.push(name);
            }
        }
        prefixes.sort();
        // A directory below the cursor's own prefix holds only addresses below
        // the cursor, so it is skipped whole rather than walked and filtered.
        if let Some(cursor) = cursor {
            prefixes.retain(|prefix| prefix.as_str() >= &cursor[..2]);
        }
        Ok(prefixes)
    }
}

/// The walk's state: the fan-out directories still to read (ascending), the
/// cursor's hex form, and the current directory's entries.
struct Walk {
    root: PathBuf,
    prefixes: IntoIter<String>,
    cursor: Option<String>,
    pending: IntoIter<Entry>,
}

/// One fan-out directory's blocks that sort strictly after `cursor`, ascending.
///
/// `read_dir` promises no order, so the directory is read whole — once per
/// walk — and its block entries are sorted before any is emitted: in
/// filesystem order the walk would neither ascend nor resume correctly.
async fn fan_out_entries(
    root: &Path,
    prefix: &str,
    cursor: Option<&str>,
) -> Result<Vec<Entry>, Error> {
    let dir = root.join(prefix);
    let mut entries = fs::read_dir(&dir).await?;
    let mut page = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        // Lowercase hex is order-preserving bytewise, so the cursor filter runs
        // on the raw name: a name at or below the cursor's hex form is an
        // address at or below the cursor, and only names that can be emitted
        // are decoded.
        if cursor.is_some_and(|cursor| name <= cursor) {
            continue;
        }
        let Ok(hash) = BlockHash::new(name) else {
            continue;
        };
        // The invariant, not the spelling: an entry is a block only if it sits
        // exactly where `block_path` puts it — its own address's fan-out
        // directory, under its canonical name — so a file in the wrong
        // directory (or with a non-canonical name) is skipped rather than
        // listed forever as a block `remove` can never find.
        let hex = hash.to_hex();
        if root.join(&hex[..2]).join(&hex) != entry.path() {
            continue;
        }
        page.push((hash, entry));
    }
    page.sort_by_key(|(hash, _)| *hash);
    let mut out = Vec::with_capacity(page.len());
    for (hash, entry) in page {
        let meta = match entry.metadata().await {
            Ok(meta) => meta,
            // The entry vanished between the directory read and the stat — a
            // sweep racing another sweep's removes. Skipping it keeps the walk
            // going and its order intact, where failing would abort a page
            // because the store changed under it.
            Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
            Err(err) => return Err(Error::Io(err)),
        };
        // A regular file only: `publish` writes blocks as files, so a directory
        // named like an address would otherwise be listed as a block whose
        // removal then fails and aborts the walk.
        if !meta.is_file() {
            continue;
        }
        out.push(Entry {
            hash,
            len: meta.len(),
            published: meta.modified()?,
        });
    }
    Ok(out)
}

/// A block staged as a file under the store's `tmp/`.
///
/// Both handles are `Option`s so a stage can hand them on: the file handle is
/// taken by [`publish`](BlockStore::publish) so it can be closed **before** the
/// temp is renamed — closing a staged temp's handle before its path is handed
/// on is this workspace's convention, not a platform demand (`tinio-fs`'s
/// `AtomicWriter::write_temp` lets its handle close at scope end before
/// `commit` renames, and `publish` keeps the same order) — and the path is
/// taken with it, so [`Drop`] never unlinks a temp that has already been
/// renamed away or removed.
#[derive(Debug)]
pub struct Staged {
    path: Option<PathBuf>,
    file: Option<BufWriter<File>>,
}

impl Staged {
    /// Take the temp's path: the temp has been renamed away or removed, so
    /// [`Drop`] must not unlink it again.
    fn take_path(&mut self) -> Result<PathBuf, Error> {
        self.path.take().ok_or_else(|| {
            // Unreachable by ownership — only `publish` takes the path, and it
            // takes it once — but a library path must not panic
            // (constitution II).
            Error::Io(io::Error::other("the stage's temp path was already taken"))
        })
    }
}

impl Drop for Staged {
    /// Best-effort: a stage that is abandoned removes its own temp, so the
    /// only residue `tmp/` ever holds is a process that died mid-stage.
    ///
    /// The unlink is the shared [`tinio_util::fsutil::remove_staged_temp`]
    /// discipline: a drop can land on a runtime worker, so the removal runs on
    /// the blocking pool (and the call never waits for it — the startup sweep
    /// is the backstop), and a shutting-down runtime cannot panic here.
    fn drop(&mut self) {
        drop(self.file.take());
        if let Some(path) = self.path.take() {
            remove_staged_temp(path);
        }
    }
}

#[async_trait::async_trait]
impl StagedBlock for Staged {
    async fn write(&mut self, chunk: Bytes) -> io::Result<()> {
        // The `Result` the trait already returns is the way out: unreachable by
        // ownership (only `publish` takes the handle), but a library path must
        // not panic (constitution II).
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| io::Error::other("the stage's file was already taken"))?;
        file.write_all(&chunk).await
    }
}

#[cfg(test)]
mod tests {
    use std::{fs as std_fs, time::Duration};

    use bytes::Bytes;
    use futures::StreamExt;
    use tokio::{task, time::timeout};

    use super::*;
    use crate::{
        _core::{
            blob::{BlockHash, BlockStore, Published, StagedBlock},
            storage::collect_body,
        },
        _util::fsutil::CHUNK_SIZE,
        testing::page_entries,
    };

    /// The read size the served stream cuts a block into (constitution V):
    /// taken from the shared bound, so the tests that size a block to span
    /// several chunks cannot silently stop spanning them.
    const CHUNK: u64 = CHUNK_SIZE as u64;

    async fn staged(store: &LocalBlocks, bytes: &[u8]) -> <LocalBlocks as BlockStore>::Staged {
        let mut staged = store.stage().await.unwrap();
        staged.write(Bytes::copy_from_slice(bytes)).await.unwrap();
        staged
    }

    #[tokio::test]
    async fn publishing_a_block_makes_it_visible_under_its_hash() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());
        let hash = BlockHash::of(b"hello");

        assert!(!store.contains(&hash).await.unwrap());
        assert_eq!(
            store
                .publish(staged(&store, b"hello").await, &hash)
                .await
                .unwrap(),
            Published::Stored
        );
        assert!(store.contains(&hash).await.unwrap());
        assert_eq!(
            std_fs::read(store.block_path(&hash)).unwrap(),
            b"hello",
            "the file is named by the hash and holds the block's bytes"
        );
    }

    #[tokio::test]
    async fn publishing_an_existing_block_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());
        let hash = BlockHash::of(b"hello");

        assert_eq!(
            store
                .publish(staged(&store, b"hello").await, &hash)
                .await
                .unwrap(),
            Published::Stored
        );
        assert_eq!(
            store
                .publish(staged(&store, b"hello").await, &hash)
                .await
                .unwrap(),
            Published::Existing,
            "the second publish reports the dedup hit"
        );
        assert_eq!(std_fs::read(store.block_path(&hash)).unwrap(), b"hello");
    }

    #[tokio::test]
    async fn publishing_over_a_vanished_temp_still_reports_the_dedup_hit() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());
        let hash = BlockHash::of(b"hello");
        store
            .publish(staged(&store, b"hello").await, &hash)
            .await
            .unwrap();

        // A second stage of the same content whose temp vanished before the
        // publish — a concurrent `tmp/` sweep reaped it. The block is stored
        // either way, so the publish answers the dedup hit: the same condition
        // the rename fallback below already reads as one.
        let second = staged(&store, b"hello").await;
        // Unlinked behind the stage's back: publish has not taken the path yet,
        // and only the file is gone.
        std_fs::remove_file(second.path.as_ref().unwrap()).unwrap();
        assert_eq!(
            store.publish(second, &hash).await.unwrap(),
            Published::Existing
        );
    }

    #[tokio::test]
    async fn a_dropped_stage_removes_its_temp_and_is_never_a_block() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());
        let hash = BlockHash::of(b"hello");

        let staged = staged(&store, b"hello").await;
        drop(staged);

        assert!(
            !store.contains(&hash).await.unwrap(),
            "nothing is visible before publish"
        );
        // The removal is async (the unlink runs on the blocking pool — the
        // shared `remove_staged_temp` discipline); wait for it.
        let temp = store.temp_dir();
        timeout(Duration::from_secs(5), async {
            loop {
                if std_fs::read_dir(&temp).unwrap().next().is_none() {
                    return;
                }
                task::yield_now().await;
            }
        })
        .await
        .expect(
            "the dropped stage removed its own temp; only a process that died mid-stage leaves residue",
        );
    }

    #[tokio::test]
    async fn opening_a_block_serves_it_whole_or_by_range() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());
        let hash = BlockHash::of(b"hello");
        store
            .publish(staged(&store, b"hello").await, &hash)
            .await
            .unwrap();

        assert_eq!(
            collect_body(store.open(&hash, None).await.unwrap())
                .await
                .unwrap(),
            b"hello"
        );
        let one = store
            .open(&hash, Some(ByteRange::Inclusive(0, 0)))
            .await
            .unwrap();
        assert_eq!(
            collect_body(one).await.unwrap(),
            b"h",
            "bytes=0-0 is one byte, not zero"
        );
        let all = store.open(&hash, Some(ByteRange::From(0))).await.unwrap();
        assert_eq!(
            collect_body(all).await.unwrap(),
            b"hello",
            "a range covering the block serves it whole"
        );
        assert!(
            store.open(&hash, Some(ByteRange::From(5))).await.is_err(),
            "a start past the end is InvalidRange"
        );
    }

    #[tokio::test]
    async fn a_block_spanning_several_chunks_streams_whole_and_by_range() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());
        // Two full chunks and a short tail: the read loop hands off `CHUNK`
        // bytes per iteration, so a block that does not fit in one chunk is
        // where a mis-sized or never-filled buffer would show.
        let payload: Vec<u8> = (0..(CHUNK as usize * 2 + 7))
            .map(|i| (i % 251) as u8)
            .collect();
        let hash = BlockHash::of(&payload);
        store
            .publish(staged(&store, &payload).await, &hash)
            .await
            .unwrap();

        assert_eq!(
            collect_body(store.open(&hash, None).await.unwrap())
                .await
                .unwrap(),
            payload,
            "a multi-chunk block reads back byte for byte"
        );
        let across = Some(ByteRange::Inclusive(1, CHUNK + 1));
        assert_eq!(
            collect_body(store.open(&hash, across).await.unwrap())
                .await
                .unwrap(),
            &payload[1..CHUNK as usize + 2],
            "a range spanning the chunk boundary serves exactly its own bytes"
        );
    }

    #[tokio::test]
    async fn opening_a_missing_block_is_not_found_and_never_a_zero_length_body() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());
        // Matched, not `unwrap_err`: `BodyStream` is not `Debug`, so the `Ok`
        // side cannot be printed.
        let Err(err) = store.open(&BlockHash::of(b"gone"), None).await else {
            panic!("a missing block must not open, not even as an empty body");
        };
        let Error::Io(io) = err else {
            panic!("expected an I/O error, got {err:?}")
        };
        assert_eq!(io.kind(), io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn list_pages_in_address_order_and_skips_the_temp_directory() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());
        let mut hashes = Vec::new();
        for bytes in [b"a".as_slice(), b"b", b"c"] {
            let hash = BlockHash::of(bytes);
            store
                .publish(staged(&store, bytes).await, &hash)
                .await
                .unwrap();
            hashes.push(hash);
        }
        let _temp = staged(&store, b"pending").await;

        // `page_entries` asserts the page rules — a page within the limit, one
        // page ascending, a page resuming strictly after the cursor — as it
        // walks; what stays here is what this test owns: which blocks the walk
        // must find.
        let mut listed: Vec<_> = page_entries(&store, 2)
            .await
            .unwrap()
            .into_iter()
            .map(|entry| entry.hash)
            .collect();
        listed.sort();
        hashes.sort();
        assert_eq!(
            listed, hashes,
            "every block is listed exactly once, temps excluded"
        );
    }

    #[tokio::test]
    async fn list_pages_across_fan_out_directories_in_ascending_address_order() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());

        // `read_dir` promises no order, so this pins the whole page walk in one
        // shape: four addresses in one fan-out directory (brute-forced onto a
        // shared two-hex-digit prefix, which exercises the per-directory sort)
        // and three more under fan-out directories of their own, so a page
        // boundary falls both inside a directory and between directories.
        let mut payloads = vec![b"shared-fan-out-0".to_vec()];
        let prefix = BlockHash::of(&payloads[0]).to_hex()[..2].to_string();
        let mut candidate = 1;
        while payloads.len() < 4 {
            assert!(
                candidate < 1_000_000,
                "no four payloads share the {prefix} fan-out directory"
            );
            let payload = format!("shared-fan-out-{candidate}").into_bytes();
            if BlockHash::of(&payload)
                .to_hex()
                .starts_with(prefix.as_str())
            {
                payloads.push(payload);
            }
            candidate += 1;
        }
        let mut other_prefixes: Vec<String> = Vec::new();
        let mut candidate = 1;
        while other_prefixes.len() < 3 {
            assert!(
                candidate < 1_000_000,
                "no three payloads land in fan-out directories of their own"
            );
            let payload = format!("other-fan-out-{candidate}").into_bytes();
            let other = BlockHash::of(&payload).to_hex()[..2].to_string();
            if other != prefix && !other_prefixes.contains(&other) {
                other_prefixes.push(other);
                payloads.push(payload);
            }
            candidate += 1;
        }

        let mut published = Vec::new();
        for payload in &payloads {
            let hash = BlockHash::of(payload);
            store
                .publish(staged(&store, payload).await, &hash)
                .await
                .unwrap();
            published.push(hash);
        }
        published.sort();

        let seen: Vec<_> = page_entries(&store, 2)
            .await
            .unwrap()
            .into_iter()
            .map(|entry| entry.hash)
            .collect();
        assert_eq!(
            seen, published,
            "every published block is listed exactly once, in ascending address order"
        );
    }

    #[tokio::test]
    async fn list_skips_entries_that_are_not_blocks_the_store_wrote() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());

        // A fan-out directory holding only entries `publish` never writes:
        // `block_path` names blocks with the lowercase hex form of a 64-byte
        // address and stores them as regular files, so none of these is a
        // block.
        let fan_out = dir.path().join("ab");
        // A directory whose name parses as an address: listing it would report
        // a directory as a block, and the sweep's `remove` would then fail on
        // it and abort the walk.
        std_fs::create_dir_all(fan_out.join(format!("ab{}", "0".repeat(62)))).unwrap();
        // A file whose address is spelled in uppercase: on a case-sensitive
        // filesystem `remove` would look for the lowercase name, find nothing
        // and answer `Ok`, so a sweep would report the same phantom block
        // removed on every pass, forever.
        std_fs::write(fan_out.join(format!("AB{}", "1".repeat(62))), b"imposter").unwrap();
        // A name that is not an address at all.
        std_fs::write(fan_out.join("notes.txt"), b"junk").unwrap();
        // A canonically-named address filed under the *wrong* fan-out
        // directory: `block_path` would put it under `ab/`, so `remove` maps it
        // to a path that is not there and answers `Ok` — the same phantom
        // block, removed forever.
        let misfiled = dir.path().join("cd");
        std_fs::create_dir_all(&misfiled).unwrap();
        std_fs::write(misfiled.join(format!("ab{}", "2".repeat(62))), b"misfiled").unwrap();

        assert!(
            store.list(None, 10).await.unwrap().is_empty(),
            "only a canonical, lowercase, regular file under its own fan-out directory is a block"
        );

        // An imposter entry never hides or displaces a real block either.
        let hash = BlockHash::of(b"hello");
        store
            .publish(staged(&store, b"hello").await, &hash)
            .await
            .unwrap();
        assert_eq!(
            store
                .list(None, 10)
                .await
                .unwrap()
                .iter()
                .map(|entry| entry.hash)
                .collect::<Vec<_>>(),
            [hash],
            "the real block is listed beside the imposters"
        );
    }

    #[tokio::test]
    async fn a_truncated_block_errors_instead_of_serving_short() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());
        let hash = BlockHash::of(b"hello");
        store
            .publish(staged(&store, b"hello").await, &hash)
            .await
            .unwrap();

        // The length is measured when the body is opened; truncating the file
        // after that must surface as an error, not as a normal end of stream.
        let mut body = store.open(&hash, None).await.unwrap();
        std_fs::OpenOptions::new()
            .write(true)
            .open(store.block_path(&hash))
            .unwrap()
            .set_len(2)
            .unwrap();

        let mut served = Vec::new();
        let err = loop {
            match body.next().await {
                Some(Ok(chunk)) => served.extend_from_slice(&chunk),
                Some(Err(err)) => break err,
                None => panic!("a truncated block must not end as a normal body"),
            }
        };
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(
            served, b"he",
            "the bytes still on disk are served before the error"
        );
    }

    #[tokio::test]
    async fn removing_a_block_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());
        let hash = BlockHash::of(b"hello");
        store
            .publish(staged(&store, b"hello").await, &hash)
            .await
            .unwrap();

        store.remove(&hash).await.unwrap();
        assert!(!store.contains(&hash).await.unwrap());
        store.remove(&hash).await.unwrap();
    }

    #[tokio::test]
    async fn a_dedup_hit_discards_the_staged_bytes_rather_than_adopting_them() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());
        let hash = BlockHash::of(b"hello");
        store
            .publish(staged(&store, b"hello").await, &hash)
            .await
            .unwrap();

        // The staged bytes do **not** hash to `hash`. Renaming them over the
        // target would pass every same-bytes dedup test while corrupting a
        // block every reader addresses by its original content.
        assert_eq!(
            store
                .publish(staged(&store, b"garbage").await, &hash)
                .await
                .unwrap(),
            Published::Existing
        );
        assert_eq!(
            std_fs::read(store.block_path(&hash)).unwrap(),
            b"hello",
            "the dedup hit discarded the staged bytes instead of renaming them into place"
        );
        assert_eq!(
            collect_body(store.open(&hash, None).await.unwrap())
                .await
                .unwrap(),
            b"hello",
            "the stored block still serves its own content after the hit"
        );
    }

    #[tokio::test]
    async fn a_dedup_publish_leaves_no_temp_behind() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());
        let hash = BlockHash::of(b"hello");
        store
            .publish(staged(&store, b"hello").await, &hash)
            .await
            .unwrap();

        assert_eq!(
            store
                .publish(staged(&store, b"garbage").await, &hash)
                .await
                .unwrap(),
            Published::Existing
        );

        // The dedup arm reaps its own temp; `Drop` cannot, because the path
        // was taken before the unlink. The unlink is off the async worker in
        // general (the shared staged-temp discipline), so wait for it rather
        // than asserting the directory is empty at the instant publish
        // returns.
        let temp = store.temp_dir();
        timeout(Duration::from_secs(5), async {
            loop {
                if std_fs::read_dir(&temp).unwrap().next().is_none() {
                    return;
                }
                task::yield_now().await;
            }
        })
        .await
        .expect("the dedup publish reaped its staged temp; only a process that died mid-stage leaves residue");
    }

    #[tokio::test]
    async fn a_fan_out_directory_that_vanishes_mid_walk_ends_the_walk_with_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());

        // Two blocks with different fan-out prefixes, so the walk reads the
        // second directory only after it has emitted the first directory's
        // entry — the moment the removal below lands in.
        let mut payloads = Vec::new();
        let mut prefixes: Vec<String> = Vec::new();
        let mut candidate = 1;
        while prefixes.len() < 2 {
            assert!(
                candidate < 1_000_000,
                "no two payloads land in fan-out directories of their own"
            );
            let payload = format!("mid-walk-{candidate}").into_bytes();
            let prefix = BlockHash::of(&payload).to_hex()[..2].to_string();
            if !prefixes.contains(&prefix) {
                prefixes.push(prefix);
                payloads.push(payload);
            }
            candidate += 1;
        }
        let mut hashes = Vec::new();
        for payload in &payloads {
            let hash = BlockHash::of(payload);
            store
                .publish(staged(&store, payload).await, &hash)
                .await
                .unwrap();
            hashes.push(hash);
        }
        hashes.sort();
        prefixes.sort();

        let mut walk = store.walk(None).await.unwrap();
        let first = walk
            .next()
            .await
            .expect("the walk yields a first entry")
            .unwrap();
        assert_eq!(first.hash, hashes[0], "the walk ascends");

        // The directory of the *other* block, removed behind the walk's back.
        std_fs::remove_dir_all(dir.path().join(&prefixes[1])).unwrap();

        // Deliberate pin: today a fan-out directory that vanishes after the
        // walk snapshotted the prefix list surfaces as a `NotFound` I/O error
        // that ends the walk — it is neither skipped nor retried. Nothing
        // states or tested this before; the assertion exists so a change of
        // behaviour (e.g. skipping a vanished directory) must be deliberate.
        let err = walk
            .next()
            .await
            .expect("the vanished directory is reported, not skipped")
            .unwrap_err();
        let Error::Io(io) = err else {
            panic!("expected an I/O error, got {err:?}");
        };
        assert_eq!(io.kind(), io::ErrorKind::NotFound);
        assert!(
            walk.next().await.is_none(),
            "the walk ends after the error rather than resuming"
        );
    }

    #[tokio::test]
    async fn a_root_that_was_never_created_lists_and_walks_empty() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("never-created");
        let store = LocalBlocks::new(&root);

        assert!(
            store.list(None, 10).await.unwrap().is_empty(),
            "an absent root holds no blocks"
        );
        let mut walk = store.walk(None).await.unwrap();
        assert!(
            walk.next().await.is_none(),
            "a walk of an absent root ends immediately"
        );
        assert!(!root.exists(), "reading an absent root must not create it");
    }

    #[tokio::test]
    async fn a_file_wearing_a_fan_out_name_never_aborts_the_walk() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());
        let hash = BlockHash::of(b"hello");
        store
            .publish(staged(&store, b"hello").await, &hash)
            .await
            .unwrap();
        // A stray *file* named like a fan-out directory must be skipped, not
        // probed as one: otherwise every walk — and so every sweep — errors
        // until a human removes it. "ab" cannot collide with the real block's
        // own fan-out directory ("hello" hashes to the "2c" prefix).
        fs::write(dir.path().join("ab"), b"stray").await.unwrap();

        let entries = store.list(None, 10).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].hash, hash);
        let mut walk = store.walk(None).await.unwrap();
        let mut seen = Vec::new();
        while let Some(entry) = walk.next().await {
            seen.push(entry.unwrap().hash);
        }
        assert_eq!(seen, vec![hash]);
    }
}
