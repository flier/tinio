//! Bucket creation times, tags, owner/ACL, and CORS (task T040, migrated
//! to redb per meta-redb-spec; the elements per specs 2026-08-31 and
//! 2026-09-05).
//!
//! `BUCKETS` table of `<state-dir>/meta.redb`: `name` →
//! `(created-at unix nanos, tags wire, owner wire, acl wire, cors wire)`
//! — a wire element is `''` when unset. Pre-existing directories get
//! their creation time lazily recorded on first sight; orphaned entries
//! are pruned on bucket delete and at startup repair (through
//! [`crate::FsCleanup`]). Redb transactions replace the old
//! load-modify-save of `buckets.json` under an in-process lock — a
//! first-sight record is one atomic upsert, so concurrent first-sights
//! cannot lose each other's entry.

use std::{path::Path, sync::Arc, time::SystemTime};

pub use crate::_core::bucket::{Name, name};
use crate::{
    _core::{cors, object, storage::no_such_bucket},
    _store::bucket,
    Error,
    database::{self, BUCKET_TAGS_MAX, Handle},
};

/// Bucket-name → creation-time store (`BUCKETS` table).
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
///
/// use tinio_fs::bucket;
/// use tokio::runtime::Runtime;
///
/// let state = tempfile::tempdir().unwrap();
/// let store = bucket::store(state.path()).unwrap();
/// let name = bucket::name("data").unwrap();
/// Runtime::new().unwrap().block_on(async {
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
    pub async fn created_at(&self, name: &Name) -> Result<Option<SystemTime>, Error> {
        self.handle
            .read(|txn| {
                bucket::Table::open_readonly(txn)?
                    .get(name)
                    .map_err(Into::into)
            })
            .map_err(Into::into)
    }

    /// The recorded tag set of a bucket — empty when the row is absent
    /// (a pre-existing bucket never tagged through the API) or its wire
    /// is domain-invalid (self-healing, like the object rows).
    pub async fn tags(&self, name: &Name) -> Result<object::Tags, Error> {
        let name = name.clone();
        self.handle
            .read(move |txn| {
                let table = bucket::Table::open_readonly(txn)?;
                let row = table.row(&name)?;
                Ok(row.map_or_else(object::Tags::empty, |row| {
                    object::Tags::from_wire_limited(&row.tags, BUCKET_TAGS_MAX)
                }))
            })
            .map_err(Into::into)
    }

    /// Replace the bucket's tag set, preserving the creation time and the
    /// other wire elements — one read-modify-write transaction (the row's
    /// created-at is kept; a missing row is lazily recorded with `now`,
    /// the first-sight policy of [`Self::get_or_record`]). The caller
    /// answers `NoSuchBucket` for a missing bucket directory before
    /// calling.
    pub async fn set_tags(&self, name: &Name, tags: &object::Tags) -> Result<(), Error> {
        let name = name.clone();
        let tags = tags.clone();
        let tags_wire = tags.to_wire();
        self.handle
            .write_if(move |txn| {
                let mut table = bucket::Table::open(txn)?;
                let Some(row) = table.row(&name)? else {
                    // No row yet — the lazy first-sight record (a real
                    // change: it creates the entry).
                    table.put_full(
                        &name,
                        &bucket::BucketRow {
                            tags: tags_wire,
                            ..bucket::BucketRow::at(SystemTime::now())
                        },
                    )?;
                    return Ok(Some(()));
                };
                if row.tags == tags_wire {
                    return Ok(None);
                }
                table.put_full(
                    &name,
                    &bucket::BucketRow {
                        tags: tags_wire,
                        ..row
                    },
                )?;
                Ok(Some(()))
            })
            .await
            .map(|_| ())
            .map_err(Into::into)
    }

    /// Clear the bucket's tag set, preserving the creation time and the
    /// other wire elements (idempotent — a missing row is a no-op; the
    /// caller answers `NoSuchBucket` for a missing bucket directory
    /// before calling).
    pub async fn clear_tags(&self, name: &Name) -> Result<(), Error> {
        let name = name.clone();
        self.handle
            .write_if(move |txn| {
                let mut table = bucket::Table::open(txn)?;
                let Some(row) = table.row(&name)? else {
                    // Nothing to change — no commit (no fsync).
                    return Ok(None);
                };
                if row.tags.is_empty() {
                    return Ok(None);
                }
                table.put_full(
                    &name,
                    &bucket::BucketRow {
                        tags: String::new(),
                        ..row
                    },
                )?;
                Ok(Some(()))
            })
            .await
            .map(|_| ())
            .map_err(Into::into)
    }

    /// The bucket's CORS configuration, in stored order — `None` when
    /// the bucket has no CORS configuration: no state record (a bucket
    /// never seen through the API), the `''` wire (the cleared or
    /// zero-rule state), or a corrupt wire (self-healing, like the store
    /// [`bucket::decode_cors_wire`]).
    pub async fn cors(&self, name: &Name) -> Result<Option<cors::CorsConfig>, Error> {
        let name = name.clone();
        self.handle
            .read(move |txn| {
                let table = bucket::Table::open_readonly(txn)?;
                let row = table.row(&name)?;
                Ok(row.and_then(|row| bucket::decode_cors_wire(&row.cors)))
            })
            .map_err(Into::into)
    }

    /// Replace the bucket's CORS configuration, preserving the creation
    /// time and the other wire elements (replace-all, no merge). A bucket
    /// without a state record answers `NoSuchBucket`. The empty rule set
    /// normalizes to the `''` wire by the codec itself (op-review G2 — a
    /// zero-rule config is "no configuration", never a non-empty row).
    pub async fn set_cors(&self, name: &Name, config: &cors::CorsConfig) -> Result<(), Error> {
        self.rewrite_cors(name, &config.to_wire()).await
    }

    /// Remove the bucket's CORS configuration, preserving the creation
    /// time and the other wire elements (`''` = "no configuration").
    /// A bucket without a state record answers `NoSuchBucket`.
    pub async fn clear_cors(&self, name: &Name) -> Result<(), Error> {
        self.rewrite_cors(name, "").await
    }

    /// The shared CORS row rewrite: set or clear the CORS wire,
    /// preserving the other elements. An identical wire is a clean no-op
    /// (`write_if`'s `Ok(None)` abort — no commit, no fsync).
    /// `None` cuts two ways: the unchanged-wire abort and the row-miss
    /// (`NoSuchBucket`) — one read breaks the tie (the write_if
    /// closure's error type is the database error, not the storage
    /// error, so the miss cannot ride the `Err` arm).
    async fn rewrite_cors(&self, name: &Name, wire: &str) -> Result<(), Error> {
        let row_name = name.clone();
        let wire = wire.to_string();
        let changed = self
            .handle
            .write_if(move |txn| {
                let mut table = bucket::Table::open(txn)?;
                let Some(row) = table.row(&row_name)? else {
                    return Ok(None);
                };
                if wire == row.cors {
                    return Ok(None);
                }
                table.put_full(
                    &row_name,
                    &bucket::BucketRow {
                        cors: wire,
                        ..row
                    },
                )?;
                Ok(Some(()))
            })
            .await?
            .is_some();
        if changed {
            return Ok(());
        }
        if self.created_at(name).await?.is_none() {
            return Err(no_such_bucket(name).into());
        }
        Ok(())
    }

    /// The creation time of a bucket, lazily recorded on first sight:
    /// a pre-existing directory without an entry gets `now` recorded
    /// (data-model.md) and returned. Existing rows take a read
    /// transaction (`HeadBucket` must not grab the exclusive write lock
    /// on every call); a missing row is an atomic upsert so concurrent
    /// first-sights converge.
    pub async fn get_or_record(&self, name: &Name, now: SystemTime) -> Result<SystemTime, Error> {
        if let Some(created) = self.created_at(name).await? {
            return Ok(created);
        }
        let name = name.clone();
        self.handle
            .write(move |txn| {
                bucket::Table::open(txn)?
                    .get_or_insert(&name, now)
                    .map_err(Into::into)
            })
            .await
            .map_err(Into::into)
    }

    /// The creation time of a bucket, recorded on first sight in ONE
    /// write transaction — the record-only form of [`Self::get_or_record`]
    /// for callers that already established the row is missing (the
    /// `list_buckets` miss arm, whose `load_many` did the pre-read): the
    /// atomic upsert stays, the read transaction goes. Concurrent
    /// first-sights converge exactly like `get_or_record`'s write arm —
    /// the upsert reads the stored value inside the single-writer
    /// transaction.
    pub async fn get_or_insert(&self, name: &Name, now: SystemTime) -> Result<SystemTime, Error> {
        let name = name.clone();
        self.handle
            .write(move |txn| {
                bucket::Table::open(txn)?
                    .get_or_insert(&name, now)
                    .map_err(Into::into)
            })
            .await
            .map_err(Into::into)
    }

    /// The creation times of the given buckets, in order — one read
    /// transaction for the whole page (the ListBuckets analogue of
    /// `meta::Store::load_entries`; a missing entry is `None` — the
    /// caller lazily records first sight).
    pub async fn load_many(&self, names: &[Name]) -> Result<Vec<Option<SystemTime>>, Error> {
        self.handle
            .read(|txn| {
                let table = bucket::Table::open_readonly(txn)?;
                names
                    .iter()
                    .map(|name| table.get(name))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(Into::into)
            })
            .map_err(Into::into)
    }

    /// Record (or overwrite) the creation time of a bucket.
    pub async fn record(&self, name: &Name, created_at: SystemTime) -> Result<(), Error> {
        let name = name.clone();
        self.handle
            .write(move |txn| {
                bucket::Table::open(txn)?
                    .put(&name, created_at)
                    .map_err(Into::into)
            })
            .await
            .map_err(Into::into)
    }

    /// Remove the entry of a bucket (idempotent). Test-only since the
    /// production teardown removes the row inside
    /// [`FsStorage::remove_bucket_state`].
    #[cfg(test)]
    pub async fn remove(&self, name: &Name) -> Result<(), Error> {
        let name = name.clone();
        self.handle
            .write(move |txn| bucket::Table::open(txn)?.remove(&name).map_err(Into::into))
            .await
            .map_err(Into::into)
    }

    /// Every recorded bucket, in name order (startup repair prunes entries
    /// whose directory is gone through [`crate::FsCleanup`]).
    pub async fn load_all(&self) -> Result<Vec<(String, SystemTime)>, Error> {
        self.handle
            .read(|txn| {
                let table = bucket::Table::open_readonly(txn)?;
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
    Ok(Store::from_handle(Handle::open(state_dir)?))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{
        _core::storage::Error as StorageError,
        _util::testing::assert_send_sync,
        bucket,
        database::{self, Error::UnsupportedVersion, StateTable},
    };

    fn t(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[tokio::test]
    async fn record_and_read_back() {
        let state = tempfile::tempdir().unwrap();
        let store = bucket::store(state.path()).unwrap();
        let name = bucket::name("data").unwrap();
        assert!(store.created_at(&name).await.unwrap().is_none());
        store.record(&name, t(100)).await.unwrap();
        assert_eq!(store.created_at(&name).await.unwrap(), Some(t(100)));
    }

    #[tokio::test]
    async fn get_or_record_lazily_records_first_sight() {
        let state = tempfile::tempdir().unwrap();
        let store = bucket::store(state.path()).unwrap();
        let name = bucket::name("data").unwrap();
        let first = store.get_or_record(&name, t(1)).await.unwrap();
        assert_eq!(first, t(1));
        // Second sight returns the recorded value, not the new one.
        let second = store.get_or_record(&name, t(2)).await.unwrap();
        assert_eq!(second, t(1));
    }

    #[tokio::test]
    async fn remove_prunes_entry() {
        let state = tempfile::tempdir().unwrap();
        let store = bucket::store(state.path()).unwrap();
        let name = bucket::name("data").unwrap();
        store.record(&name, t(1)).await.unwrap();
        store.remove(&name).await.unwrap();
        assert!(store.created_at(&name).await.unwrap().is_none());
        store.remove(&name).await.unwrap(); // idempotent
    }

    #[tokio::test]
    async fn load_all_returns_sorted_entries() {
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
    }

    #[tokio::test]
    async fn fs_store_cors_round_trip_preserves_order_and_optional_fields() {
        let state = tempfile::tempdir().unwrap();
        let store = bucket::store(state.path()).unwrap();
        let name = bucket::name("data").unwrap();
        store.record(&name, t(100)).await.unwrap();
        assert_eq!(
            store.cors(&name).await.unwrap(),
            None,
            "an unconfigured bucket answers None"
        );
        let cfg = cors::CorsConfig {
            rules: vec![
                cors::CorsRule {
                    id: Some("one".into()),
                    allowed_methods: vec!["GET".into()],
                    allowed_origins: vec!["*".into()],
                    allowed_headers: Some(vec!["x-amz-*".into()]),
                    expose_headers: Some(vec!["ETag".into()]),
                    max_age_seconds: Some(60),
                },
                cors::CorsRule {
                    id: None,
                    allowed_methods: vec!["PUT".into(), "DELETE".into()],
                    allowed_origins: vec!["https://example.com".into()],
                    allowed_headers: None,
                    expose_headers: None,
                    max_age_seconds: None,
                },
            ],
        };
        store.set_cors(&name, &cfg).await.unwrap();
        assert_eq!(store.cors(&name).await.unwrap(), Some(cfg.clone())); // order + fields preserved
        // The row's other elements survive the CORS writes.
        assert_eq!(store.created_at(&name).await.unwrap(), Some(t(100)));
        store.clear_cors(&name).await.unwrap();
        assert_eq!(store.cors(&name).await.unwrap(), None); // clear → "no config"
    }

    #[tokio::test]
    async fn fs_store_cors_empty_config_normalizes_to_no_config() {
        // op-review G2: a zero-rule config stored through the backend must
        // be indistinguishable from "no configuration" ('' wire → get → None).
        let state = tempfile::tempdir().unwrap();
        let store = bucket::store(state.path()).unwrap();
        let name = bucket::name("data").unwrap();
        store.record(&name, t(100)).await.unwrap();
        store
            .set_cors(&name, &cors::CorsConfig::default())
            .await
            .unwrap();
        assert_eq!(store.cors(&name).await.unwrap(), None);
    }

    #[tokio::test]
    async fn fs_store_cors_missing_row_is_no_such_bucket_for_writes() {
        // The write accessors answer NoSuchBucket when the state row is
        // absent (a bucket that is not recorded cannot be configured); the
        // missing-BUCKET probes (the real "missing bucket") are answered by
        // the BucketOps impl through `ensure_bucket`.
        let state = tempfile::tempdir().unwrap();
        let store = bucket::store(state.path()).unwrap();
        let name = bucket::name("data").unwrap();
        store.record(&name, t(100)).await.unwrap();
        store.remove(&name).await.unwrap();
        let cfg = cors::CorsConfig {
            rules: vec![cors::CorsRule {
                id: None,
                allowed_methods: vec!["GET".into()],
                allowed_origins: vec!["*".into()],
                allowed_headers: None,
                expose_headers: None,
                max_age_seconds: None,
            }],
        };
        let err = store.set_cors(&name, &cfg).await.unwrap_err();
        assert!(
            matches!(err, Error::Storage(StorageError::NoSuchBucket(_))),
            "{err:?}"
        );
        let err = store.clear_cors(&name).await.unwrap_err();
        assert!(
            matches!(err, Error::Storage(StorageError::NoSuchBucket(_))),
            "{err:?}"
        );
        // The read probe: no record = no configuration (a bucket without a
        // state row has no CORS configuration, never an error).
        assert_eq!(store.cors(&name).await.unwrap(), None);
    }

    #[tokio::test]
    async fn state_version_is_written_and_validated() {
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
                Error::Database(UnsupportedVersion {
                    path: _,
                    found: 9,
                    expected: 1
                })
            ),
            "{err:?}"
        );
    }

    #[test]
    fn store_is_send_sync() {
        assert_send_sync::<bucket::Store>();
    }
}
