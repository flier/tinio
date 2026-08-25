//! The ETag metadata store (task T039, migrated to redb per meta-redb-spec).
//!
//! Entries live in the `OBJECT_META` table of `<state-dir>/meta.redb`:
//! key `(bucket, key)`, value `(etag hex, size, mtime unix nanos)`. The
//! composite key keeps one bucket's entries contiguous, so `walk` and
//! `remove_bucket` are cheap prefix range scans.
//!
//! Entries are served only when size + mtime match the object file
//! (FR-022); otherwise the ETag is recomputed streaming and the entry
//! rewritten. Redb transactions replace the old temp+rename writes under an
//! in-process lock: single-entry operations are one short transaction, and
//! a bucket removal is one atomic range deletion.
//!
//! `tinio-core` domain types stay serde-free (constitution I); the stored
//! value uses plain strings and is validated into the domain types on read.
//! A domain-invalid stored value is reported as missing (the caller
//! recomputes from the object file and rewrites — self-healing, FR-022);
//! under redb's table-level consistency a single bad entry no longer
//! exists on its own.

use std::{
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime},
};

use tinio_core::{etag::ETag, from_nanos, object, to_nanos};

use crate::bucket;

pub use crate::bucket::{Name, name};
pub use object::{Key, key};

use crate::{Error, database, write::md5_of_file};

/// The jitter-window fallback when no file identity is available (a
/// stored identity of `0` — platforms without one, or filesystems
/// without file IDs): a same-size mtime drift within this window is
/// treated as a touch (antivirus/indexer) and keeps a multipart
/// `MD5-of-MD5s-N` ETag; a larger drift is re-hashed. Where the file
/// identity exists (unix dev+inode; Windows volume serial + file index)
/// the comparison is exact and this window is unused.
const COMPOSED_MTIME_JITTER: Duration = Duration::from_secs(60);

/// The absolute time difference between two instants (symmetric — an
/// mtime drift into the past counts the same as into the future).
fn mtime_drift(a: SystemTime, b: SystemTime) -> Duration {
    a.duration_since(b)
        .unwrap_or_else(|_| b.duration_since(a).unwrap_or_default())
}

/// Whether a stored `(size, mtime)` still matches the object file
/// (FR-022 — served only on a match; else recomputed). The single home of
/// the rule: [`Record::matches`] and the hot-path tuple reads share it.
fn entry_matches(stored_size: u64, stored_mtime: u64, size: u64, mtime: SystemTime) -> bool {
    stored_size == size && stored_mtime == to_nanos(mtime)
}

/// A validated meta record (the parsed form of the stored entry).
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
/// use tinio_fs::meta::Record;
///
/// let record = Record {
///     key: "dir/file.txt".into(),
///     etag: "d41d8cd98f00b204e9800998ecf8427e".into(),
///     size: 4,
///     mtime: 0,
/// };
/// assert!(record.matches(4, SystemTime::UNIX_EPOCH));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Object key (validated).
    pub key: object::Key,
    /// ETag (single MD5 or composed `-N` form).
    pub etag: ETag,
    /// Object size in bytes at record time.
    pub size: u64,
    /// Object mtime in unix nanoseconds at record time.
    pub mtime: u64,
}

impl Record {
    /// Whether the recorded size + mtime still match the object file
    /// (FR-022 — served only on a match; else recomputed).
    pub fn matches(&self, size: u64, mtime: SystemTime) -> bool {
        entry_matches(self.size, self.mtime, size, mtime)
    }
}

/// The ETag metadata store of a state dir.
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
/// use tinio_core::{ETag, bucket, object};
/// use tinio_fs::meta;
/// let state = tempfile::tempdir().unwrap();
/// let store = meta::store(state.path()).unwrap();
/// let bucket = bucket::name("data").unwrap();
/// let key = object::key("dir/file.txt").unwrap();
/// let etag = ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap();
/// tokio::runtime::Runtime::new().unwrap().block_on(async {
///     store.set(&bucket, &key, &etag, 4, SystemTime::UNIX_EPOCH, 0).await.unwrap();
///     let record = store.get(&bucket, &key).await.unwrap().unwrap();
///     assert_eq!(record.etag, etag);
///     assert_eq!(record.size, 4);
///     assert!(record.matches(4, SystemTime::UNIX_EPOCH));
/// });
/// ```
#[derive(Debug, Clone)]
pub struct Store {
    /// The shared state-database handle (the redb single writer replaces
    /// the old in-process lock).
    handle: Arc<database::Handle>,
}

impl Store {
    /// Create a store over a shared state-database handle (the `FsStorage`
    /// construction path — one handle across all stores).
    pub(crate) fn from_handle(handle: Arc<database::Handle>) -> Self {
        Self { handle }
    }

    /// The stored [`database::StoredMeta`] of `key` — the hot-path read, without
    /// building a [`Record`] (no key clone).
    /// A domain-invalid stored value cannot be trusted: it is reported as
    /// missing so the caller recomputes the ETag from the object file and
    /// rewrites the entry (self-healing, FR-022) instead of failing every
    /// read of that object with 500.
    async fn stored_entry(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<database::StoredMeta>, Error> {
        self.handle
            .read(|txn| database::ObjectMetaTable::open_readonly(txn)?.get(bucket, key))
            .map_err(Into::into)
    }

    /// The stored record for `key`, if any (unvalidated against the object
    /// file — the caller compares via [`Record::matches`]).
    pub async fn get(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<Record>, Error> {
        let Some(stored) = self.stored_entry(bucket, key).await? else {
            return Ok(None);
        };
        Ok(Some(Record {
            key: key.clone(),
            etag: stored.etag,
            size: stored.size,
            mtime: stored.mtime,
        }))
    }

    /// The ETag of `key` when the stored entry still matches the object
    /// file (`size` + `mtime`), else `None` (recompute needed, FR-022).
    pub async fn etag_matching(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        size: u64,
        mtime: SystemTime,
    ) -> Result<Option<ETag>, Error> {
        let Some(stored) = self.stored_entry(bucket, key).await? else {
            return Ok(None);
        };
        if entry_matches(stored.size, stored.mtime, size, mtime) {
            Ok(Some(stored.etag))
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
        key: &object::Key,
        path: &Path,
        size: u64,
        mtime: SystemTime,
    ) -> Result<(ETag, bool), Error> {
        if let Some(stored) = self.stored_entry(bucket, key).await? {
            if entry_matches(stored.size, stored.mtime, size, mtime) {
                return Ok((stored.etag, false));
            }
            // Timestamp jitter (antivirus, indexer) must not rewrite a
            // multipart `MD5-of-MD5s-N` ETag into a content MD5. The file
            // identity distinguishes the two precisely: a touch keeps the
            // same file (identity unchanged) → keep the form, refresh
            // mtime; a same-size replacement (new file renamed over —
            // identity changed) → re-hash, or the wrong ETag would be
            // served forever. Where the platform exposes no identity
            // (stored 0), the mtime jitter window is the fallback.
            if matches!(stored.etag, ETag::Composed(_, _)) && stored.size == size {
                let metadata = tokio::fs::metadata(path).await?;
                let current = crate::fsutil::file_identity(path, &metadata);
                let same_file = if stored.file_identity != 0 && current != 0 {
                    current == stored.file_identity
                } else {
                    mtime_drift(mtime, from_nanos(stored.mtime)) <= COMPOSED_MTIME_JITTER
                };
                if same_file {
                    self.set(bucket, key, &stored.etag, size, mtime, current)
                        .await?;
                    return Ok((stored.etag, false));
                }
                let (digest, _) = md5_of_file(path).await?;
                let etag = ETag::Single(digest);
                self.set(bucket, key, &etag, size, mtime, current).await?;
                return Ok((etag, true));
            }
        }
        let (digest, metadata) = md5_of_file(path).await?;
        let etag = ETag::Single(digest);
        let identity = crate::fsutil::file_identity(path, &metadata);
        self.set(bucket, key, &etag, size, mtime, identity).await?;
        Ok((etag, true))
    }

    /// The ETag of an object file at `path`: the stored entry when it still
    /// matches (`size` + `mtime`), else the content MD5 recomputed
    /// streaming and the entry rewritten (FR-022).
    pub async fn etag_for_file(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        path: &Path,
        size: u64,
        mtime: SystemTime,
    ) -> Result<ETag, Error> {
        Ok(self.ensure_etag(bucket, key, path, size, mtime).await?.0)
    }

    /// Store (or overwrite) the entry for `key` — one write transaction.
    /// `identity` is the file identity at record time (see
    /// [`crate::fsutil::file_identity`]); `0` marks an unavailable
    /// platform identity.
    pub async fn set(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        etag: &ETag,
        size: u64,
        mtime: SystemTime,
        identity: u64,
    ) -> Result<(), Error> {
        self.handle
            .write(|txn| {
                database::ObjectMetaTable::open(txn)?.put(bucket, key, etag, size, mtime, identity)
            })
            .map_err(Into::into)
    }

    /// Remove the entry for `key` (idempotent — a missing entry is Ok).
    pub async fn remove(&self, bucket: &bucket::Name, key: &object::Key) -> Result<(), Error> {
        self.handle
            .write(|txn| database::ObjectMetaTable::open(txn)?.remove(bucket, key))
            .map_err(Into::into)
    }

    /// Walk every stored entry of `bucket` in key order (the scanner's
    /// reclamation pass and `doctor`'s meta-orphan check read this).
    pub async fn walk(&self, bucket: &bucket::Name) -> Result<Vec<Record>, Error> {
        self.handle
            .read(|txn| {
                let table = database::ObjectMetaTable::open_readonly(txn)?;
                let mut out = Vec::new();
                table.for_bucket(bucket, |key, etag, size, mtime| {
                    out.push(Record {
                        key,
                        etag,
                        size,
                        mtime,
                    });
                    Ok(())
                })?;
                Ok(out)
            })
            .map_err(Into::into)
    }

    /// Remove the whole meta subtree of `bucket` (one atomic range
    /// deletion). Test-only since the production teardown goes through
    /// [`crate::FsStorage::remove_bucket_state`].
    #[cfg(test)]
    pub async fn remove_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        self.handle
            .write(|txn| database::ObjectMetaTable::open(txn)?.drain_bucket(bucket))
            .map_err(Into::into)
    }
}

/// Create a store over its **own** state database at `<state_dir>`.
///
/// Each call opens the `meta.redb` file exclusively — creating two
/// standalone stores (of any kind) over the same state dir at once fails
/// with `DatabaseAlreadyOpen`. Production code constructs one
/// [`crate::FsStorage`] per root and shares its single handle; this
/// constructor is for standalone/embedded use and tests.
///
/// # Errors
///
/// When the state database cannot be opened (a corrupt or unwritable
/// `meta.redb`).
#[inline]
pub fn store(state_dir: &Path) -> Result<Store, Error> {
    Ok(Store::from_handle(database::Handle::open(state_dir)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{database, meta, testutil::rt};
    use std::time::Duration;
    use tinio_core::{bucket, object};
    use tinio_util::testing::etag;

    fn mtime(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    /// Inject an invalid etag value straight into the table (defensive
    /// read-path test — the public API can never write one). Runs on its
    /// own handle, dropped before the store opens the same file (redb's
    /// file lock is exclusive).
    fn corrupt_entry(state_dir: &Path, bucket: &str, key: &str) {
        let db = database::open(state_dir).unwrap().db;
        let mut txn = db.begin_write().unwrap();
        {
            let mut table = database::ObjectMetaTable::open(&mut txn).unwrap();
            table
                .insert((bucket, key), ("not-an-etag", 1, 1, 0))
                .unwrap();
        }
        txn.commit().unwrap();
    }

    #[test]
    fn set_get_round_trip() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let store = meta::store(state.path()).unwrap();
            let b = bucket::name("data").unwrap();
            let k = object::key("dir/file.txt").unwrap();
            store
                .set(
                    &b,
                    &k,
                    &etag("d41d8cd98f00b204e9800998ecf8427e"),
                    4,
                    mtime(100),
                    0,
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
            let store = meta::store(state.path()).unwrap();
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
            let store = meta::store(state.path()).unwrap();
            let b = bucket::name("data").unwrap();
            let k = object::key("a.txt").unwrap();
            let e = etag("d41d8cd98f00b204e9800998ecf8427e");
            store.set(&b, &k, &e, 10, mtime(42), 0).await.unwrap();
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
            let store = meta::store(state.path()).unwrap();
            let b = bucket::name("data").unwrap();
            let k = object::key("a.txt").unwrap();
            store
                .set(
                    &b,
                    &k,
                    &etag("d41d8cd98f00b204e9800998ecf8427e"),
                    1,
                    mtime(1),
                    0,
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
            let store = meta::store(state.path()).unwrap();
            let b = bucket::name("data").unwrap();
            let k = object::key("a.txt").unwrap();
            store
                .set(
                    &b,
                    &k,
                    &etag("d41d8cd98f00b204e9800998ecf8427e"),
                    1,
                    mtime(1),
                    0,
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
                    0,
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
            let store = meta::store(state.path()).unwrap();
            let b = bucket::name("data").unwrap();
            for (i, key_str) in ["a.txt", "dir/b.txt", "dir/sub/c.txt"].iter().enumerate() {
                store
                    .set(
                        &b,
                        &object::key(*key_str).unwrap(),
                        &etag("d41d8cd98f00b204e9800998ecf8427e"),
                        i as u64,
                        mtime(i as u64),
                        0,
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
            let store = meta::store(state.path()).unwrap();
            let b = bucket::name("data").unwrap();
            let k = object::key("ümlaut/文件/with spaces.txt").unwrap();
            store
                .set(
                    &b,
                    &k,
                    &etag("d41d8cd98f00b204e9800998ecf8427e"),
                    0,
                    mtime(0),
                    0,
                )
                .await
                .unwrap();
            let record = store.get(&b, &k).await.unwrap().unwrap();
            assert_eq!(record.key, k);
        });
    }

    #[test]
    fn bucket_scan_boundaries_exclude_other_buckets() {
        // Keys of `data` must never bleed into a bucket whose name has
        // `data` as a prefix, and vice versa.
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let store = meta::store(state.path()).unwrap();
            let data = bucket::name("data").unwrap();
            let data_x = bucket::name("data-x").unwrap();
            for (b, k) in [
                (&data, "a.txt"),
                (&data, "z.txt"),
                (&data_x, "a.txt"),
                (&data_x, "z.txt"),
            ] {
                store
                    .set(
                        b,
                        &object::key(k).unwrap(),
                        &etag("d41d8cd98f00b204e9800998ecf8427e"),
                        1,
                        mtime(1),
                        0,
                    )
                    .await
                    .unwrap();
            }
            let keys: Vec<String> = store
                .walk(&data)
                .await
                .unwrap()
                .into_iter()
                .map(|r| r.key.to_string())
                .collect();
            assert_eq!(keys, ["a.txt", "z.txt"]);
            let keys: Vec<String> = store
                .walk(&data_x)
                .await
                .unwrap()
                .into_iter()
                .map(|r| r.key.to_string())
                .collect();
            assert_eq!(keys, ["a.txt", "z.txt"]);

            store.remove_bucket(&data).await.unwrap();
            assert!(store.walk(&data).await.unwrap().is_empty());
            assert_eq!(store.walk(&data_x).await.unwrap().len(), 2);
        });
    }

    #[test]
    fn corrupt_entry_is_treated_as_missing() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            // Corrupt before the store opens (the redb file lock is
            // exclusive per handle).
            corrupt_entry(state.path(), "data", "a.txt");
            let store = meta::store(state.path()).unwrap();
            let b = bucket::name("data").unwrap();
            let k = object::key("a.txt").unwrap();
            // Reported as missing (the caller recomputes from the object
            // file) — never a 500 on reads.
            assert!(store.get(&b, &k).await.unwrap().is_none());
            // walk() surfaces corrupt rows (scanner/doctor must not skip).
            assert!(store.walk(&b).await.is_err());
        });
    }

    #[test]
    fn corrupt_entry_self_heals_on_recompute() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let file = state.path().join("a.txt");
            tokio::fs::write(&file, b"hello").await.unwrap();
            // Corrupt the entry first, then the recompute path must
            // rewrite it.
            corrupt_entry(state.path(), "data", "a.txt");
            let store = meta::store(state.path()).unwrap();
            let b = bucket::name("data").unwrap();
            let k = object::key("a.txt").unwrap();
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
    fn composed_etag_survives_touch() {
        // A touch (antivirus/indexer) keeps the same file — the identity
        // is unchanged (unix), and even a jitter-scale mtime change falls
        // inside the window fallback elsewhere — the multipart
        // `MD5-of-MD5s-N` form is preserved.
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let store = meta::store(state.path()).unwrap();
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
                    crate::fsutil::file_identity(&file, &metadata),
                )
                .await
                .unwrap();
            let handle = std::fs::File::options().write(true).open(&file).unwrap();
            handle
                .set_modified(metadata.modified().unwrap() + Duration::from_secs(30))
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

    #[test]
    fn composed_etag_rehashes_on_replacement() {
        // A same-size out-of-band replacement (a NEW file renamed over —
        // the identity changes on unix; the mtime drift is beyond the
        // window fallback elsewhere) must be re-hashed — the stale
        // composed ETag is never served forever (code-review #9).
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let store = meta::store(state.path()).unwrap();
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
                    crate::fsutil::file_identity(&file, &metadata),
                )
                .await
                .unwrap();
            // Replace with a fresh file (old mtime, so the fallback window
            // also rules out a touch).
            let replacement = state.path().join("replacement.bin");
            tokio::fs::write(&replacement, b"hello").await.unwrap();
            let handle = std::fs::File::options()
                .write(true)
                .open(&replacement)
                .unwrap();
            handle
                .set_modified(metadata.modified().unwrap() + Duration::from_secs(120))
                .unwrap();
            drop(handle);
            tokio::fs::rename(&replacement, &file).await.unwrap();
            let metadata = tokio::fs::metadata(&file).await.unwrap();
            let etag = store
                .etag_for_file(&b, &k, &file, metadata.len(), metadata.modified().unwrap())
                .await
                .unwrap();
            // Re-hashed from the (unchanged) content — the composed form
            // is replaced by the content MD5.
            assert_eq!(etag, ETag::from_content(b"hello"));
        });
    }

    #[cfg(unix)]
    #[test]
    fn composed_etag_rehashes_on_quick_replacement() {
        // The identity beats the clock: a same-size replacement within
        // the old 60 s window is still detected (new inode), where the
        // mtime fallback would wrongly keep the composed form.
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let store = meta::store(state.path()).unwrap();
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
                    crate::fsutil::file_identity(&file, &metadata),
                )
                .await
                .unwrap();
            let replacement = state.path().join("replacement.bin");
            tokio::fs::write(&replacement, b"hello").await.unwrap();
            // Fresh mtime — inside the old 60 s window.
            tokio::fs::rename(&replacement, &file).await.unwrap();
            let metadata = tokio::fs::metadata(&file).await.unwrap();
            let etag = store
                .etag_for_file(&b, &k, &file, metadata.len(), metadata.modified().unwrap())
                .await
                .unwrap();
            assert_eq!(etag, ETag::from_content(b"hello"));
        });
    }
}
