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
//! load-modify-save of `buckets.json` under an in-process lock.
//!
//! A first-sight record is one commit: the creation-time paths ride
//! `record_if_absent`'s insert-or-keep upsert, and the tagging/ACL paths
//! ride the contract's create-or-update twins
//! ([`BucketMeta::set_tags_or_create`],
//! [`BucketMeta::set_grants_or_create`]) — the row's presence is decided
//! and its element written inside that one transaction, so the row is
//! never visible without the element the caller asked for and a crash
//! leaves either no row or the complete one.
//!
//! The store is a thin facade over the shared contract implementation
//! (plan 2026-10-03 Task 5): every method delegates to
//! [`crate::_store_redb::RedbMetadata`]'s [`BucketMeta`] and maps the
//! contract error onto this crate's at the boundary — the row decode, the
//! existence-conditioned decisions and the atomic upserts are the
//! contract's, and the signatures this module's callers use are unchanged.

use std::{path::Path, sync::Arc, time::SystemTime};

pub use crate::_core::bucket::{Name, name};
use crate::{
    _core::{
        acl,
        acl::{Acl, AclGrants},
        cors,
        meta::{BucketMeta, bucket as row},
        object,
        storage::no_such_bucket,
    },
    _store_redb::RedbMetadata,
    Error,
    database::{self, Driver},
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
    /// The shared contract implementation over the state database (one
    /// redb handle, shared with the backend's other stores — the redb
    /// single writer replaces the old in-process lock and the parsed-file
    /// cache).
    meta: Arc<RedbMetadata>,
}

impl Store {
    /// Create a store over a shared state-database handle (the `FsStorage`
    /// construction path — one handle across all stores).
    pub(crate) fn from_handle(handle: Arc<database::Driver>) -> Self {
        Self {
            meta: Arc::new(RedbMetadata::from_driver(handle.shared())),
        }
    }

    /// The recorded creation time of a bucket, if any.
    pub async fn created_at(&self, name: &Name) -> Result<Option<SystemTime>, Error> {
        self.meta
            .created_at(name)
            .await
            .map_err(database::map_contract_error)
    }

    /// The recorded tag set of a bucket — empty when the row is absent
    /// (a pre-existing bucket never tagged through the API) or its wire
    /// is domain-invalid (self-healing, like the object rows).
    pub async fn tags(&self, name: &Name) -> Result<object::Tags, Error> {
        Ok(self
            .meta
            .row(name)
            .await
            .map_err(database::map_contract_error)?
            .map_or_else(object::Tags::empty, |row| row.tags))
    }

    /// Replace the bucket's tag set, preserving the creation time and the
    /// other wire elements.
    ///
    /// One contract call, one write transaction
    /// ([`BucketMeta::set_tags_or_create`]): the presence decision and the
    /// element write commit together, so no reader ever sees the row
    /// without its tags and a crash leaves either no row or the complete
    /// one. A row-less bucket (a directory never seen through the API) is
    /// recorded first sight with `now` AND the tag set; a present row's
    /// other elements ride untouched. The caller answers `NoSuchBucket`
    /// for a missing bucket directory before calling.
    pub async fn set_tags(&self, name: &Name, tags: &object::Tags) -> Result<(), Error> {
        self.meta
            .set_tags_or_create(name, tags, SystemTime::now())
            .await
            .map_err(database::map_contract_error)
    }

    /// Clear the bucket's tag set, preserving the creation time and the
    /// other wire elements (idempotent — a missing row is a no-op; the
    /// caller answers `NoSuchBucket` for a missing bucket directory
    /// before calling).
    pub async fn clear_tags(&self, name: &Name) -> Result<(), Error> {
        self.meta
            .clear_tags(name)
            .await
            .map(|_| ())
            .map_err(database::map_contract_error)
    }

    /// The bucket's ACL — the owner element recombined with the stored
    /// grant set (the row form keeps them apart; a domain-invalid wire
    /// self-heals via the shared decode helpers). A row-less bucket (a
    /// directory never seen through the API — the lazy first-sight
    /// record) answers the default private ACL with no owner.
    pub async fn acl(&self, name: &Name) -> Result<Acl, Error> {
        Ok(self
            .meta
            .row(name)
            .await
            .map_err(database::map_contract_error)?
            .map_or_else(
                || Acl::default_private(None),
                |row| Acl {
                    owner: row.owner,
                    grants: row.acl.grants,
                },
            ))
    }

    /// Replace the bucket's grant set, preserving the owner element and
    /// the other wires.
    ///
    /// One contract call, one write transaction
    /// ([`BucketMeta::set_grants_or_create`]): the owner is read and
    /// carried over **inside** that transaction, so the owner's own writer
    /// — `create_bucket`'s create-time record — can neither slip its owner
    /// between a read and a write nor lose it to them. The old
    /// cross-transaction read-modify-write leaned on the caller's bucket
    /// mutation lock to close that window.
    ///
    /// A row-less bucket (a directory never seen through the API) is
    /// recorded first sight with `now` and the grant set, and **no owner**
    /// — a bucket the API never sighted has none to carry.
    pub async fn set_acl(&self, name: &Name, grants: &AclGrants) -> Result<(), Error> {
        self.meta
            .set_grants_or_create(name, grants, SystemTime::now())
            .await
            .map_err(database::map_contract_error)
    }

    /// The bucket's CORS configuration, in stored order — `None` when
    /// the bucket has no CORS configuration: no state record (a bucket
    /// never seen through the API), the `''` wire (the cleared or
    /// zero-rule state), or a corrupt wire (self-healing, the table's
    /// CORS accessor).
    pub async fn cors(&self, name: &Name) -> Result<Option<cors::Config>, Error> {
        Ok(self
            .meta
            .row(name)
            .await
            .map_err(database::map_contract_error)?
            .and_then(|row| row.cors))
    }

    /// Replace the bucket's CORS configuration, preserving the creation
    /// time and the other wire elements (replace-all, no merge). A bucket
    /// without a state record answers `NoSuchBucket`. The empty rule set
    /// normalizes to the `''` wire by the codec itself (op-review G2 — a
    /// zero-rule config is "no configuration", never a non-empty row).
    pub async fn set_cors(&self, name: &Name, config: &cors::Config) -> Result<(), Error> {
        if self
            .meta
            .set_cors(name, config)
            .await
            .map_err(database::map_contract_error)?
        {
            return Ok(());
        }
        Err(no_such_bucket(name).into())
    }

    /// Remove the bucket's CORS configuration, preserving the creation
    /// time and the other wire elements (`''` = "no configuration").
    /// A bucket without a state record answers `NoSuchBucket`.
    pub async fn clear_cors(&self, name: &Name) -> Result<(), Error> {
        if self
            .meta
            .clear_cors(name)
            .await
            .map_err(database::map_contract_error)?
        {
            return Ok(());
        }
        Err(no_such_bucket(name).into())
    }

    /// The recorded owner element of one bucket (`None` = the row is
    /// missing, a legacy bucket never sighted through the API — the auth
    /// layer's lazy default owner; a garbage wire reads `None` too, the
    /// shared decode self-heal). The one-row form of [`Self::owners`] for
    /// a single bucket's ownership probe.
    pub async fn owner(&self, name: &Name) -> Result<Option<acl::OwnerId>, Error> {
        Ok(self
            .meta
            .row(name)
            .await
            .map_err(database::map_contract_error)?
            .and_then(|row| row.owner))
    }

    /// The recorded owner element of each `Name`, index-aligned with the
    /// input (`None` = the row is missing, a legacy bucket never sighted
    /// through the API — the auth layer's lazy default owner; a garbage
    /// wire reads `None` too, the shared decode self-heal). One read
    /// transaction for the whole page — the list-buckets walk-time
    /// owner filter's row probe.
    pub async fn owners(&self, names: &[Name]) -> Result<Vec<Option<acl::OwnerId>>, Error> {
        self.meta
            .owners(names)
            .await
            .map_err(database::map_contract_error)
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
        self.meta
            .record_if_absent(name, now)
            .await
            .map_err(database::map_contract_error)
            .or_else(|err| not_recorded(now, err))
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
        self.meta
            .record_if_absent(name, now)
            .await
            .map_err(database::map_contract_error)
            .or_else(|err| not_recorded(now, err))
    }

    /// The creation times of the given buckets, in order — one read
    /// transaction for the whole page (the ListBuckets analogue of
    /// `meta::Store::load_entries`; a missing entry is `None` — the
    /// caller lazily records first sight).
    pub async fn load_many(&self, names: &[Name]) -> Result<Vec<Option<SystemTime>>, Error> {
        self.meta
            .load_many(names)
            .await
            .map_err(database::map_contract_error)
    }

    /// Record (or overwrite) the creation time of a bucket.
    pub async fn record(&self, name: &Name, created_at: SystemTime) -> Result<(), Error> {
        self.meta
            .record(name, created_at)
            .await
            .map_err(database::map_contract_error)
    }

    /// Record (or overwrite) the creation time of `name` WITH the owner
    /// and ACL wires (the create-time ACL rows — the creation time is
    /// preserved by the CORS/tagging rewrite paths).
    pub async fn record_full(
        &self,
        name: &Name,
        created_at: SystemTime,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
    ) -> Result<(), Error> {
        let row = row::Row {
            created: created_at,
            tags: object::Tags::empty(),
            owner: owner.cloned(),
            acl: Acl {
                owner: None,
                grants: acl.grants.clone(),
            },
            cors: None,
        };
        self.meta
            .record_full(name, &row)
            .await
            .map_err(database::map_contract_error)
    }

    /// Remove the entry of a bucket (idempotent). Test-only since the
    /// production teardown removes the row inside
    /// [`FsStorage::remove_bucket_state`].
    #[cfg(test)]
    pub async fn remove(&self, name: &Name) -> Result<(), Error> {
        self.meta
            .remove(name)
            .await
            .map_err(database::map_contract_error)
    }

    /// Every recorded bucket, in name order (startup repair prunes entries
    /// whose directory is gone through [`crate::FsCleanup`]).
    pub async fn load_all(&self) -> Result<Vec<(String, SystemTime)>, Error> {
        self.meta
            .load_all()
            .await
            .map_err(database::map_contract_error)
    }
}

/// The first-sight creation time a **read-only** handle's refused record
/// answers (FR-023 amended, design 2026-10-03 **Read paths**): `now` — the
/// value the write would have recorded — served without writing anything.
/// Every other error propagates: a read-only run degrades, it does not
/// swallow real failures.
fn not_recorded(now: SystemTime, err: Error) -> Result<SystemTime, Error> {
    if err.is_read_only() {
        Ok(now)
    } else {
        Err(err)
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
    Ok(Store::from_handle(Driver::new(
        database::open(state_dir)?.db,
    )))
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
        testutil::{all_users_read_grant, owner_id},
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
        let cfg = cors::Config {
            rules: vec![
                cors::Rule {
                    id: Some("one".into()),
                    allowed_methods: vec!["GET".into()],
                    allowed_origins: vec!["*".into()],
                    allowed_headers: Some(vec!["x-amz-*".into()]),
                    expose_headers: Some(vec!["ETag".into()]),
                    max_age_seconds: Some(60),
                },
                cors::Rule {
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
        let cfg = cors::Config {
            rules: vec![cors::Rule {
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
    async fn fs_store_set_tags_lazily_records_the_first_sight_row() {
        let state = tempfile::tempdir().unwrap();
        let store = bucket::store(state.path()).unwrap();
        let name = bucket::name("data").unwrap();
        let tags = object::Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
        // A pre-existing-but-unrecorded bucket: set_tags records the row
        // WITH the tags in one write (the lazy first-sight policy).
        store.set_tags(&name, &tags).await.unwrap();
        assert_eq!(store.tags(&name).await.unwrap(), tags);
        let created = store.created_at(&name).await.unwrap().unwrap();
        assert!(created <= SystemTime::now());
        // A later set replaces the tags; the recorded creation time rides.
        let replaced = object::Tags::from_pairs([("team".into(), "core".into())]).unwrap();
        store.set_tags(&name, &replaced).await.unwrap();
        assert_eq!(store.tags(&name).await.unwrap(), replaced);
        assert_eq!(store.created_at(&name).await.unwrap(), Some(created));
    }

    #[tokio::test]
    async fn fs_store_set_acl_lazily_records_the_first_sight_row() {
        let state = tempfile::tempdir().unwrap();
        let store = bucket::store(state.path()).unwrap();
        let name = bucket::name("data").unwrap();
        let grants = vec![all_users_read_grant()];
        // A pre-existing-but-unrecorded bucket: the miss is ONE transaction
        // carrying the grant set — a reader never sees the row without it
        // — and no owner (a directory never seen through the API has none
        // to carry).
        store.set_acl(&name, &grants).await.unwrap();
        assert_eq!(store.acl(&name).await.unwrap().grants, grants);
        assert!(store.owner(&name).await.unwrap().is_none());
        let created = store.created_at(&name).await.unwrap().unwrap();
        // A recorded owner rides the later grant writes: the owner element
        // is read and carried back inside the contract's own transaction
        // (the C1 lost-update window closed by serialization, not by the
        // caller's lock).
        let owner = owner_id();
        store
            .record_full(
                &name,
                created,
                Some(&owner),
                &Acl::default_private(Some(owner.clone())),
            )
            .await
            .unwrap();
        let replaced = Vec::new();
        store.set_acl(&name, &replaced).await.unwrap();
        assert_eq!(store.owner(&name).await.unwrap(), Some(owner));
        assert!(store.acl(&name).await.unwrap().grants.is_empty());
        assert_eq!(
            store.created_at(&name).await.unwrap(),
            Some(created),
            "the ACL writes keep the recorded creation time"
        );
    }

    #[tokio::test]
    async fn fs_store_clear_tags_is_idempotent_and_preserves_the_row() {
        let state = tempfile::tempdir().unwrap();
        let store = bucket::store(state.path()).unwrap();
        let name = bucket::name("data").unwrap();
        // A row-less bucket: no-op (nothing to clear, nothing created).
        store.clear_tags(&name).await.unwrap();
        assert!(store.tags(&name).await.unwrap().is_empty());
        let tags = object::Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
        store.set_tags(&name, &tags).await.unwrap();
        let created = store.created_at(&name).await.unwrap().unwrap();
        store.clear_tags(&name).await.unwrap();
        assert!(store.tags(&name).await.unwrap().is_empty());
        assert_eq!(
            store.created_at(&name).await.unwrap(),
            Some(created),
            "the clear keeps the recorded creation time"
        );
        store.clear_tags(&name).await.unwrap(); // idempotent
    }

    #[tokio::test]
    async fn fs_store_tags_self_heal_a_corrupt_wire() {
        // The read-side tolerance ruling: a bucket row whose tags element
        // is domain-invalid serves the empty set (mirroring the fs
        // cap-50 `parse_wire_limited` discipline). Rows are API-written;
        // the garbage below is a direct database write (tampering). The
        // injection opens the file itself, so it runs before the store's
        // exclusive handle.
        let state = tempfile::tempdir().unwrap();
        {
            let db = database::open(state.path()).unwrap().db;
            let mut txn = db.begin_write().unwrap();
            {
                let mut table = crate::_store_redb::bucket::Table::open(&mut txn).unwrap();
                table
                    .insert("data", (1u64, "team=%zz&", "", "", ""))
                    .unwrap();
            }
            txn.commit().unwrap();
        }
        let store = bucket::store(state.path()).unwrap();
        let name = bucket::name("data").unwrap();
        assert!(store.tags(&name).await.unwrap().is_empty());
        // The row itself survives (the created time is still answered).
        assert!(store.created_at(&name).await.unwrap().is_some());
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
