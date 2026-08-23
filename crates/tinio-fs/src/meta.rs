//! The ETag metadata store (task T039).
//!
//! Git-style 2-hex fan-out layout (data-model.md):
//! `meta/objects/<bucket>/<2hex>/<sha1hex>.json` = `{key, etag, size, mtime}`
//! where the hash is SHA-1 of the object key. The fan-out avoids huge flat
//! directories and Windows path-length limits; bucket deletion is a subtree
//! removal.
//!
//! Entries are served only when size + mtime match the object file
//! (FR-022); otherwise the ETag is recomputed streaming and the entry
//! rewritten. All writes are atomic (temp + rename) under an in-process
//! lock, so concurrent writers never produce torn JSON. Orphaned entries
//! (entry whose object file is gone) are reclaimed by the scanner through
//! [`crate::FsCleanup`].
//!
//! `tinio-core` domain types stay serde-free (constitution I); the stored
//! JSON record uses plain strings and is validated into the domain types on
//! read.

use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use tinio_core::{
    bucket,
    etag::ETag,
    object::{self, Key},
    to_nanos,
};
use tokio::sync::Mutex;

use crate::{
    Error,
    fsutil::ok_if_missing,
    path::META_DIR_NAME,
    write::{AtomicWriter, md5_of_file},
};

/// A validated meta record (the parsed form of the stored JSON entry).
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
/// use tinio_fs::MetaRecord;
///
/// let record = MetaRecord {
///     key: "dir/file.txt".into(),
///     etag: "d41d8cd98f00b204e9800998ecf8427e".into(),
///     size: 4,
///     mtime: 0,
/// };
/// assert!(record.matches(4, SystemTime::UNIX_EPOCH));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaRecord {
    /// Object key (validated).
    pub key: Key,
    /// ETag (single MD5 or composed `-N` form).
    pub etag: ETag,
    /// Object size in bytes at record time.
    pub size: u64,
    /// Object mtime in unix nanoseconds at record time.
    pub mtime: u64,
}

impl MetaRecord {
    /// Whether the recorded size + mtime still match the object file
    /// (FR-022 — served only on a match; else recomputed).
    pub fn matches(&self, size: u64, mtime: SystemTime) -> bool {
        self.size == size && self.mtime == to_nanos(mtime)
    }
}

/// The on-disk JSON record (plain strings; validated on read).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredEntry {
    key: String,
    etag: String,
    size: u64,
    mtime: u64,
}

impl StoredEntry {
    fn into_record(self) -> Option<MetaRecord> {
        let key = object::key(self.key).ok()?;
        let etag = ETag::new(&self.etag).ok()?;
        Some(MetaRecord {
            key,
            etag,
            size: self.size,
            mtime: self.mtime,
        })
    }
}

/// The ETag metadata store of a state dir.
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
/// use tinio_core::{ETag, bucket, object};
/// use tinio_fs::MetaStore;
///
/// let state = tempfile::tempdir().unwrap();
/// let store = MetaStore::new(state.path());
/// let bucket = bucket::name("data").unwrap();
/// let key = object::key("dir/file.txt").unwrap();
/// let etag = ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap();
/// tokio::runtime::Runtime::new().unwrap().block_on(async {
///     store.set(&bucket, &key, &etag, 4, SystemTime::UNIX_EPOCH).await.unwrap();
///     let record = store.get(&bucket, &key).await.unwrap().unwrap();
///     assert_eq!(record.etag, etag);
///     assert_eq!(record.size, 4);
///     assert!(record.matches(4, SystemTime::UNIX_EPOCH));
/// });
/// ```
#[derive(Debug, Clone)]
pub struct MetaStore {
    /// `<state-dir>/meta/objects/`.
    root: PathBuf,
    /// Atomic writer (staging under `<state-dir>/tmp/`).
    writer: AtomicWriter,
    /// In-process lock: serializes atomic writes (no torn JSON).
    lock: Arc<Mutex<()>>,
}

impl MetaStore {
    /// Create a store rooted at `<state_dir>/meta/objects/`.
    pub fn new(state_dir: &Path) -> Self {
        Self {
            root: state_dir.join(META_DIR_NAME).join("objects"),
            writer: AtomicWriter::new(state_dir),
            lock: Arc::new(Mutex::new(())),
        }
    }

    /// The meta root (`<state-dir>/meta/objects/` — the cleanup stages
    /// walk this tree).
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    /// The stored record for `key`, if any (unvalidated against the object
    /// file — the caller compares via [`MetaRecord::matches`]).
    pub async fn get(&self, bucket: &bucket::Name, key: &Key) -> Result<Option<MetaRecord>, Error> {
        let path = self.entry_path(bucket, key);
        let bytes = match tokio::fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        // A corrupt or domain-invalid entry cannot be trusted: report it
        // as missing so the caller recomputes the ETag from the object
        // file and rewrites the entry (self-healing, FR-022) instead of
        // failing every read of that object with 500.
        let stored: StoredEntry = match serde_json::from_slice(&bytes) {
            Ok(stored) => stored,
            Err(_) => return Ok(None),
        };
        let Some(record) = stored.into_record() else {
            return Ok(None);
        };
        Ok(Some(record))
    }

    /// The ETag of `key` when the stored entry still matches the object
    /// file (`size` + `mtime`), else `None` (recompute needed, FR-022).
    pub async fn etag_matching(
        &self,
        bucket: &bucket::Name,
        key: &Key,
        size: u64,
        mtime: SystemTime,
    ) -> Result<Option<ETag>, Error> {
        let Some(record) = self.get(bucket, key).await? else {
            return Ok(None);
        };
        if record.matches(size, mtime) {
            Ok(Some(record.etag))
        } else {
            Ok(None)
        }
    }

    /// The ETag of an object file at `path`, ensuring a matching entry:
    /// the stored entry when it still matches (`size` + `mtime`), else
    /// the content MD5 recomputed streaming and the entry rewritten
    /// (FR-022). Returns the ETag and whether it was (re)computed — one
    /// read drives the decision, so a stale entry costs one read, not two.
    pub async fn ensure_etag(
        &self,
        bucket: &bucket::Name,
        key: &Key,
        path: &Path,
        size: u64,
        mtime: SystemTime,
    ) -> Result<(ETag, bool), Error> {
        if let Some(record) = self.get(bucket, key).await? {
            if record.matches(size, mtime) {
                return Ok((record.etag, false));
            }
            // Timestamp jitter (antivirus, indexer) must not rewrite a
            // multipart `MD5-of-MD5s-N` ETag into a content MD5. Same
            // size + composed record → keep the form, refresh mtime.
            if matches!(record.etag, ETag::Composed(_, _)) && record.size == size {
                self.set(bucket, key, &record.etag, size, mtime).await?;
                return Ok((record.etag, false));
            }
        }
        let etag = ETag::Single(md5_of_file(path).await?);
        self.set(bucket, key, &etag, size, mtime).await?;
        Ok((etag, true))
    }

    /// The ETag of an object file at `path`: the stored entry when it still
    /// matches (`size` + `mtime`), else the content MD5 recomputed
    /// streaming and the entry rewritten (FR-022).
    pub async fn etag_for_file(
        &self,
        bucket: &bucket::Name,
        key: &Key,
        path: &Path,
        size: u64,
        mtime: SystemTime,
    ) -> Result<ETag, Error> {
        Ok(self.ensure_etag(bucket, key, path, size, mtime).await?.0)
    }

    /// Store (or overwrite) the entry for `key` — atomic temp+rename under
    /// the in-process lock.
    pub async fn set(
        &self,
        bucket: &bucket::Name,
        key: &Key,
        etag: &ETag,
        size: u64,
        mtime: SystemTime,
    ) -> Result<(), Error> {
        let stored = StoredEntry {
            key: key.to_string(),
            etag: etag.as_str(),
            size,
            mtime: to_nanos(mtime),
        };
        let json = serde_json::to_vec(&stored)?;
        let path = self.entry_path(bucket, key);
        let _guard = self.lock.lock().await;
        self.writer.write_bytes(&path, &json).await
    }

    /// Remove the entry for `key` (idempotent — a missing entry is Ok).
    pub async fn remove(&self, bucket: &bucket::Name, key: &Key) -> Result<(), Error> {
        let path = self.entry_path(bucket, key);
        let _guard = self.lock.lock().await;
        ok_if_missing(tokio::fs::remove_file(&path).await)?;
        Ok(())
    }

    /// Walk every stored entry of `bucket` (the scanner's reclamation pass
    /// and `doctor`'s meta-orphan check read this).
    pub async fn walk(&self, bucket: &bucket::Name) -> Result<Vec<MetaRecord>, Error> {
        let dir = self.root.join(&**bucket);
        let mut out = Vec::new();
        // Iterative walk (no async recursion): worklist of directories.
        let mut stack = vec![dir];
        while let Some(dir) = stack.pop() {
            let mut entries = match tokio::fs::read_dir(&dir).await {
                Ok(entries) => entries,
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            };
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                if entry.file_type().await?.is_dir() {
                    stack.push(path);
                } else if path.extension().and_then(|e| e.to_str()) == Some("json") {
                    let bytes = match tokio::fs::read(&path).await {
                        Ok(bytes) => bytes,
                        // A file may vanish between listing and reading
                        // (bucket deleted concurrently) — treat as gone.
                        Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                        Err(err) => return Err(err.into()),
                    };
                    if let Ok(stored) = serde_json::from_slice::<StoredEntry>(&bytes)
                        && let Some(record) = stored.into_record()
                    {
                        out.push(record);
                    }
                    // Corrupt entries are skipped silently; `doctor`
                    // reports them through FsCleanup.
                }
            }
        }
        Ok(out)
    }

    /// Remove the whole meta subtree of `bucket` (lazy orphan cleanup on
    /// bucket delete).
    pub async fn remove_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let dir = self.root.join(&**bucket);
        ok_if_missing(tokio::fs::remove_dir_all(&dir).await)?;
        Ok(())
    }

    /// `<meta-root>/<bucket>/<2hex>/<sha1hex>.json` for a key.
    fn entry_path(&self, bucket: &bucket::Name, key: &Key) -> PathBuf {
        let digest = Sha1::digest(key.as_bytes());
        let hash = hex::encode(digest);
        self.root
            .join(&**bucket)
            .join(&hash[..2])
            .join(format!("{hash}.json"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::rt;
    use std::time::Duration;
    use tinio_core::testing::etag;

    fn mtime(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn set_get_round_trip() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let store = MetaStore::new(state.path());
            let b = bucket::name("data").unwrap();
            let k = object::key("dir/file.txt").unwrap();
            store
                .set(
                    &b,
                    &k,
                    &etag("d41d8cd98f00b204e9800998ecf8427e"),
                    4,
                    mtime(100),
                )
                .await
                .unwrap();
            let record = store.get(&b, &k).await.unwrap().unwrap();
            assert_eq!(record.key, k);
            assert_eq!(record.etag, etag("d41d8cd98f00b204e9800998ecf8427e"));
            assert_eq!(record.size, 4);
            assert!(record.matches(4, mtime(100)));
            assert!(!record.matches(5, mtime(100)));
            assert!(!record.matches(4, mtime(101)));
        });
    }

    #[test]
    fn get_missing_is_none() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let store = MetaStore::new(state.path());
            let b = bucket::name("data").unwrap();
            let k = object::key("nope.txt").unwrap();
            assert!(store.get(&b, &k).await.unwrap().is_none());
            assert_eq!(
                store.etag_matching(&b, &k, 0, mtime(0)).await.unwrap(),
                None
            );
        });
    }

    #[test]
    fn etag_matching_requires_size_and_mtime() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let store = MetaStore::new(state.path());
            let b = bucket::name("data").unwrap();
            let k = object::key("a.txt").unwrap();
            let e = etag("d41d8cd98f00b204e9800998ecf8427e");
            store.set(&b, &k, &e, 10, mtime(42)).await.unwrap();
            assert_eq!(
                store.etag_matching(&b, &k, 10, mtime(42)).await.unwrap(),
                Some(e.clone())
            );
            assert_eq!(
                store.etag_matching(&b, &k, 11, mtime(42)).await.unwrap(),
                None
            );
            assert_eq!(
                store.etag_matching(&b, &k, 10, mtime(43)).await.unwrap(),
                None
            );
        });
    }

    #[test]
    fn remove_deletes_and_is_idempotent() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let store = MetaStore::new(state.path());
            let b = bucket::name("data").unwrap();
            let k = object::key("a.txt").unwrap();
            store
                .set(
                    &b,
                    &k,
                    &etag("d41d8cd98f00b204e9800998ecf8427e"),
                    1,
                    mtime(1),
                )
                .await
                .unwrap();
            store.remove(&b, &k).await.unwrap();
            assert!(store.get(&b, &k).await.unwrap().is_none());
            store.remove(&b, &k).await.unwrap(); // idempotent
        });
    }

    #[test]
    fn overwrite_replaces_entry() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let store = MetaStore::new(state.path());
            let b = bucket::name("data").unwrap();
            let k = object::key("a.txt").unwrap();
            store
                .set(
                    &b,
                    &k,
                    &etag("d41d8cd98f00b204e9800998ecf8427e"),
                    1,
                    mtime(1),
                )
                .await
                .unwrap();
            store
                .set(
                    &b,
                    &k,
                    &etag("5eb63bbbe01eeed093cb22bb8f5acdc3"),
                    2,
                    mtime(2),
                )
                .await
                .unwrap();
            let record = store.get(&b, &k).await.unwrap().unwrap();
            assert_eq!(record.size, 2);
            assert_eq!(record.etag, etag("5eb63bbbe01eeed093cb22bb8f5acdc3"));
        });
    }

    #[test]
    fn walk_returns_all_entries_and_remove_bucket_clears() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let store = MetaStore::new(state.path());
            let b = bucket::name("data").unwrap();
            for (i, key) in ["a.txt", "dir/b.txt", "dir/sub/c.txt"].iter().enumerate() {
                store
                    .set(
                        &b,
                        &object::key(*key).unwrap(),
                        &etag("d41d8cd98f00b204e9800998ecf8427e"),
                        i as u64,
                        mtime(i as u64),
                    )
                    .await
                    .unwrap();
            }
            let mut keys: Vec<String> = store
                .walk(&b)
                .await
                .unwrap()
                .into_iter()
                .map(|r| r.key.to_string())
                .collect();
            keys.sort();
            assert_eq!(keys, ["a.txt", "dir/b.txt", "dir/sub/c.txt"]);

            store.remove_bucket(&b).await.unwrap();
            assert!(store.walk(&b).await.unwrap().is_empty());
            store.remove_bucket(&b).await.unwrap(); // idempotent
        });
    }

    #[test]
    fn entries_with_unicode_keys() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let store = MetaStore::new(state.path());
            let b = bucket::name("data").unwrap();
            let k = object::key("ümlaut/文件/with spaces.txt").unwrap();
            store
                .set(
                    &b,
                    &k,
                    &etag("d41d8cd98f00b204e9800998ecf8427e"),
                    0,
                    mtime(0),
                )
                .await
                .unwrap();
            let record = store.get(&b, &k).await.unwrap().unwrap();
            assert_eq!(record.key, k);
        });
    }

    #[test]
    fn corrupt_entry_is_treated_as_missing() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let store = MetaStore::new(state.path());
            let b = bucket::name("data").unwrap();
            let k = object::key("a.txt").unwrap();
            let path = store.entry_path(&b, &k);
            tokio::fs::create_dir_all(path.parent().unwrap())
                .await
                .unwrap();
            tokio::fs::write(&path, b"not json").await.unwrap();
            // Reported as missing (the caller recomputes from the object
            // file) — never a 500 on reads.
            assert!(store.get(&b, &k).await.unwrap().is_none());
            // walk() skips corrupt entries silently.
            assert!(store.walk(&b).await.unwrap().is_empty());
        });
    }

    #[test]
    fn corrupt_entry_self_heals_on_recompute() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let store = MetaStore::new(state.path());
            let b = bucket::name("data").unwrap();
            let k = object::key("a.txt").unwrap();
            let file = state.path().join("a.txt");
            tokio::fs::write(&file, b"hello").await.unwrap();
            // Corrupt the entry, then the recompute path must rewrite it.
            let entry = store.entry_path(&b, &k);
            tokio::fs::create_dir_all(entry.parent().unwrap())
                .await
                .unwrap();
            tokio::fs::write(&entry, b"not json").await.unwrap();
            let metadata = tokio::fs::metadata(&file).await.unwrap();
            let etag = store
                .etag_for_file(&b, &k, &file, metadata.len(), metadata.modified().unwrap())
                .await
                .unwrap();
            assert_eq!(etag, ETag::from_content(b"hello"));
            assert!(store.get(&b, &k).await.unwrap().is_some());
        });
    }

    #[test]
    fn composed_etag_survives_mtime_only_drift() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let store = MetaStore::new(state.path());
            let b = bucket::name("data").unwrap();
            let k = object::key("mp.bin").unwrap();
            let file = state.path().join("mp.bin");
            tokio::fs::write(&file, b"hello").await.unwrap();
            let metadata = tokio::fs::metadata(&file).await.unwrap();
            let composed = ETag::new("5d41402abc4b2a76b9719d911017c592-2").unwrap();
            store
                .set(
                    &b,
                    &k,
                    &composed,
                    metadata.len(),
                    metadata.modified().unwrap(),
                )
                .await
                .unwrap();
            let handle = std::fs::File::options().write(true).open(&file).unwrap();
            handle
                .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_600_000_000))
                .unwrap();
            drop(handle);
            let metadata = tokio::fs::metadata(&file).await.unwrap();
            let etag = store
                .etag_for_file(&b, &k, &file, metadata.len(), metadata.modified().unwrap())
                .await
                .unwrap();
            assert_eq!(etag, composed);
            assert!(matches!(etag, ETag::Composed(_, 2)));
        });
    }
}
