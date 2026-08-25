//! Bucket creation times (task T040, migrated to redb per meta-redb-spec).
//!
//! `BUCKETS` table of `<state-dir>/meta.redb`: `name` → created-at unix
//! nanos. Pre-existing directories get their creation time lazily recorded
//! on first sight; orphaned entries are pruned on bucket delete and at
//! startup repair (through [`crate::FsCleanup`]). Redb transactions replace
//! the old load-modify-save of `buckets.json` under an in-process lock — a
//! first-sight record is one atomic upsert, so concurrent first-sights
//! cannot lose each other's entry.

use std::{path::Path, sync::Arc, time::SystemTime};

use tinio_core::bucket;

pub use bucket::{Name, name};

use crate::{Error, database};

/// Bucket-name → creation-time store (`BUCKETS` table).
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
/// use tinio_fs::bucket;
///
/// let state = tempfile::tempdir().unwrap();
/// let store = bucket::store(state.path()).unwrap();
/// let name = bucket::name("data").unwrap();
/// tokio::runtime::Runtime::new().unwrap().block_on(async {
///     store.record(&name, SystemTime::UNIX_EPOCH).await.unwrap();
///     let created = store.created_at(&name).await.unwrap().unwrap();
///     assert_eq!(created, SystemTime::UNIX_EPOCH);
/// });
/// ```
#[derive(Debug, Clone)]
pub struct Store {
    /// The shared state-database handle (the redb single writer replaces
    /// the old in-process lock and the parsed-file cache).
    handle: Arc<database::Handle>,
}

impl Store {
    /// Create a store over a shared state-database handle (the `FsStorage`
    /// construction path — one handle across all stores).
    pub(crate) fn from_handle(handle: Arc<database::Handle>) -> Self {
        Self { handle }
    }

    /// The recorded creation time of a bucket, if any.
    pub async fn created_at(&self, name: &bucket::Name) -> Result<Option<SystemTime>, Error> {
        self.handle
            .read(|txn| database::BucketsTable::open_readonly(txn)?.get(name))
            .map_err(Into::into)
    }

    /// The creation time of a bucket, lazily recorded on first sight:
    /// a pre-existing directory without an entry gets `now` recorded
    /// (data-model.md) and returned. Existing rows take a read
    /// transaction (`HeadBucket` must not grab the exclusive write lock
    /// on every call); a missing row is an atomic upsert so concurrent
    /// first-sights converge.
    pub async fn get_or_record(
        &self,
        name: &bucket::Name,
        now: SystemTime,
    ) -> Result<SystemTime, Error> {
        if let Some(created) = self.created_at(name).await? {
            return Ok(created);
        }
        self.handle
            .write(|txn| database::BucketsTable::open(txn)?.get_or_insert(name, now))
            .map_err(Into::into)
    }

    /// Record (or overwrite) the creation time of a bucket.
    pub async fn record(&self, name: &bucket::Name, created_at: SystemTime) -> Result<(), Error> {
        self.handle
            .write(|txn| database::BucketsTable::open(txn)?.put(name, created_at))
            .map_err(Into::into)
    }

    /// Remove the entry of a bucket (idempotent). Test-only since the
    /// production teardown removes the row inside
    /// [`crate::FsStorage::remove_bucket_state`].
    #[cfg(test)]
    pub async fn remove(&self, name: &bucket::Name) -> Result<(), Error> {
        self.handle
            .write(|txn| database::BucketsTable::open(txn)?.remove(name))
            .map_err(Into::into)
    }

    /// Every recorded bucket, in name order (startup repair prunes entries
    /// whose directory is gone through [`crate::FsCleanup`]).
    pub async fn load_all(&self) -> Result<Vec<(String, SystemTime)>, Error> {
        self.handle
            .read(|txn| {
                let table = database::BucketsTable::open_readonly(txn)?;
                let mut out = Vec::new();
                table.for_each(|name, created_at| {
                    out.push((name.to_string(), created_at));
                    Ok(())
                })?;
                Ok(out)
            })
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
    use crate::{
        bucket,
        database::{self, StateTable},
        testutil::rt,
    };
    use std::time::Duration;
    use tinio_util::testing::assert_send_sync;

    fn t(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn record_and_read_back() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let store = bucket::store(state.path()).unwrap();
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
            let store = bucket::store(state.path()).unwrap();
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
            let store = bucket::store(state.path()).unwrap();
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
            let store = bucket::store(state.path()).unwrap();
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
    fn state_version_is_written_and_validated() {
        // The buckets.json version check moved to the STATE table
        // (meta-redb-spec task 3): a fresh database records version 1, and
        // a mismatch fails to open.
        rt(async {
            let state = tempfile::tempdir().unwrap();
            {
                let db = database::open(state.path()).unwrap().db;
                let mut txn = db.begin_write().unwrap();
                {
                    let mut state = StateTable::open(&mut txn).unwrap();
                    state.insert("version", 9).unwrap();
                }
                txn.commit().unwrap();
            }
            let err: Error = database::open(state.path()).unwrap_err().into();
            assert!(
                matches!(
                    err,
                    Error::Database(database::Error::UnsupportedVersion {
                        path: _,
                        found: 9,
                        expected: 1
                    })
                ),
                "{err:?}"
            );
        });
    }

    #[test]
    fn store_is_send_sync() {
        assert_send_sync::<bucket::Store>();
    }
}
