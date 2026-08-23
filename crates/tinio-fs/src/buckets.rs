//! Bucket creation times (task T040).
//!
//! `buckets.json` = `{"version": 1, "buckets": {name: created_at_nanos}}`
//! (data-model.md), written atomically (temp + rename) under an in-process
//! lock. Pre-existing directories get their creation time lazily recorded
//! on first sight; orphaned entries are pruned on bucket delete and at
//! startup repair (through [`crate::FsCleanup`]).

use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::SystemTime,
};

use serde::{Deserialize, Serialize};
use tinio_core::{bucket, from_nanos, to_nanos};
use tokio::sync::Mutex as AsyncMutex;

use crate::{
    Error,
    backend::{corrupt_state_file, unsupported_state_version},
    path::BUCKETS_FILE,
    write::AtomicWriter,
};

/// The `buckets.json` file format version.
const VERSION: u32 = 1;

/// The on-disk buckets file.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BucketsFile {
    version: u32,
    buckets: HashMap<String, u64>,
}

/// Bucket-name → creation-time store (`buckets.json`).
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
/// use tinio_core::bucket;
/// use tinio_fs::BucketStore;
///
/// let state = tempfile::tempdir().unwrap();
/// let store = BucketStore::new(state.path());
/// let name = bucket::name("data").unwrap();
/// tokio::runtime::Runtime::new().unwrap().block_on(async {
///     store.record(&name, SystemTime::UNIX_EPOCH).await.unwrap();
///     let created = store.created_at(&name).await.unwrap().unwrap();
///     assert_eq!(created, SystemTime::UNIX_EPOCH);
/// });
/// ```
#[derive(Debug, Clone)]
pub struct BucketStore {
    /// `<state-dir>/buckets.json`.
    path: PathBuf,
    /// Atomic writer (staging under `<state-dir>/tmp/`).
    writer: AtomicWriter,
    /// In-process lock: serializes atomic writes (no torn JSON).
    lock: Arc<AsyncMutex<()>>,
    /// The parsed `buckets.json`, cached after the first read so per-call
    /// lookups (head_bucket, list_buckets) never re-read the file — and
    /// never deep-clone the whole map either (the `Arc` is shared).
    /// Write-through: every `save` replaces it; the file stays the source
    /// of truth (single-instance binding means no external writers).
    /// Shared across clones (they serve the same on-disk file).
    cache: Arc<Mutex<Option<Arc<BucketsFile>>>>,
}

impl BucketStore {
    /// Create a store at `<state_dir>/buckets.json`.
    pub fn new(state_dir: &Path) -> Self {
        Self {
            path: state_dir.join(BUCKETS_FILE),
            writer: AtomicWriter::new(state_dir),
            lock: Arc::new(AsyncMutex::new(())),
            cache: Arc::new(Mutex::new(None)),
        }
    }

    /// The recorded creation time of a bucket, if any.
    pub async fn created_at(&self, name: &bucket::Name) -> Result<Option<SystemTime>, Error> {
        // The read shares the async lock with the writers: a lock-free
        // cold fill could overwrite the cache with a stale file after a
        // concurrent save, and that stale map would become the base of
        // the next save — losing the writer's entry.
        let _guard = self.lock.lock().await;
        let file = self.load().await?;
        Ok(file.buckets.get(name.as_ref()).copied().map(from_nanos))
    }

    /// The creation time of a bucket, lazily recorded on first sight:
    /// a pre-existing directory without an entry gets `now` recorded
    /// (data-model.md) and returned.
    pub async fn get_or_record(
        &self,
        name: &bucket::Name,
        now: SystemTime,
    ) -> Result<SystemTime, Error> {
        // Load-modify-save is one critical section: two concurrent
        // first-sights must not lose each other's entry.
        let _guard = self.lock.lock().await;
        let mut file = self.load().await?;
        if let Some(&created) = file.buckets.get(name.as_ref()) {
            return Ok(from_nanos(created));
        }
        let created = to_nanos(now);
        let file = Arc::make_mut(&mut file);
        file.buckets.insert(name.to_string(), created);
        self.save_locked(file).await?;
        Ok(from_nanos(created))
    }

    /// Record (or overwrite) the creation time of a bucket.
    pub async fn record(&self, name: &bucket::Name, created_at: SystemTime) -> Result<(), Error> {
        let _guard = self.lock.lock().await;
        let mut file = self.load().await?;
        let file = Arc::make_mut(&mut file);
        file.buckets.insert(name.to_string(), to_nanos(created_at));
        self.save_locked(file).await
    }

    /// Remove the entry of a bucket (idempotent).
    pub async fn remove(&self, name: &bucket::Name) -> Result<(), Error> {
        let _guard = self.lock.lock().await;
        let mut file = self.load().await?;
        let file = Arc::make_mut(&mut file);
        if file.buckets.remove(name.as_ref()).is_some() {
            self.save_locked(file).await?;
        }
        Ok(())
    }

    /// Every recorded bucket, in name order (startup repair prunes entries
    /// whose directory is gone through [`crate::FsCleanup`]).
    pub async fn load_all(&self) -> Result<Vec<(String, SystemTime)>, Error> {
        // See `created_at`: the cold fill must not race a writer's save.
        let _guard = self.lock.lock().await;
        let file = self.load().await?;
        let mut out: Vec<(String, SystemTime)> = file
            .buckets
            .iter()
            .map(|(name, nanos)| (name.clone(), from_nanos(*nanos)))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    /// The parsed-file cache (poison-tolerant: a panicked holder already
    /// lost its data, so the file is re-read on the next miss).
    fn cache(&self) -> std::sync::MutexGuard<'_, Option<Arc<BucketsFile>>> {
        self.cache.lock().unwrap_or_else(|p| p.into_inner())
    }

    async fn load(&self) -> Result<Arc<BucketsFile>, Error> {
        if let Some(file) = self.cache().clone() {
            return Ok(file);
        }
        let file = Arc::new(self.read_file().await?);
        *self.cache() = Some(file.clone());
        Ok(file)
    }

    async fn read_file(&self) -> Result<BucketsFile, Error> {
        let bytes = match tokio::fs::read(&self.path).await {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Ok(BucketsFile {
                    version: VERSION,
                    buckets: HashMap::new(),
                });
            }
            Err(err) => return Err(err.into()),
        };
        let file: BucketsFile =
            serde_json::from_slice(&bytes).map_err(|err| corrupt_state_file(&self.path, err))?;
        if file.version != VERSION {
            return Err(unsupported_state_version(&self.path, file.version, VERSION));
        }
        Ok(file)
    }

    /// Write `file` to disk and refresh the cache. The caller holds
    /// `lock` (the load-modify-save critical section).
    async fn save_locked(&self, file: &BucketsFile) -> Result<(), Error> {
        let json = serde_json::to_vec(file)?;
        self.writer.write_bytes(&self.path, &json).await?;
        *self.cache() = Some(Arc::new(file.clone()));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::rt;
    use std::time::Duration;
    use tinio_core::testing::assert_send_sync;

    fn t(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn record_and_read_back() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let store = BucketStore::new(state.path());
            let name = bucket::name("data").unwrap();
            assert!(store.created_at(&name).await.unwrap().is_none());
            store.record(&name, t(100)).await.unwrap();
            assert_eq!(store.created_at(&name).await.unwrap(), Some(t(100)));
        });
    }

    #[test]
    fn get_or_record_lazily_records_first_sight() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let store = BucketStore::new(state.path());
            let name = bucket::name("data").unwrap();
            let first = store.get_or_record(&name, t(1)).await.unwrap();
            assert_eq!(first, t(1));
            // Second sight returns the recorded value, not the new one.
            let second = store.get_or_record(&name, t(2)).await.unwrap();
            assert_eq!(second, t(1));
        });
    }

    #[test]
    fn remove_prunes_entry() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let store = BucketStore::new(state.path());
            let name = bucket::name("data").unwrap();
            store.record(&name, t(1)).await.unwrap();
            store.remove(&name).await.unwrap();
            assert!(store.created_at(&name).await.unwrap().is_none());
            store.remove(&name).await.unwrap(); // idempotent
        });
    }

    #[test]
    fn load_all_returns_sorted_entries() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let store = BucketStore::new(state.path());
            store
                .record(&bucket::name("zeta").unwrap(), t(3))
                .await
                .unwrap();
            store
                .record(&bucket::name("alpha").unwrap(), t(1))
                .await
                .unwrap();
            let all = store.load_all().await.unwrap();
            let names: Vec<&str> = all.iter().map(|(n, _)| n.as_str()).collect();
            assert_eq!(names, ["alpha", "zeta"]);
        });
    }

    #[test]
    fn file_format_has_version() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let store = BucketStore::new(state.path());
            store
                .record(&bucket::name("data").unwrap(), t(42))
                .await
                .unwrap();
            let raw = tokio::fs::read_to_string(state.path().join("buckets.json"))
                .await
                .unwrap();
            let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
            assert_eq!(parsed["version"], 1);
            assert_eq!(parsed["buckets"]["data"].as_u64(), Some(42_000_000_000));
        });
    }

    #[test]
    fn corrupt_file_is_an_error() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let store = BucketStore::new(state.path());
            tokio::fs::write(state.path().join("buckets.json"), b"junk")
                .await
                .unwrap();
            assert!(
                store
                    .created_at(&bucket::name("data").unwrap())
                    .await
                    .is_err()
            );
        });
    }

    #[test]
    fn bucket_store_is_send_sync() {
        assert_send_sync::<BucketStore>();
    }
}
