use std::{
    collections::HashSet,
    io,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use futures::stream;
use tinio_util::testing::{
    metrics::{protected_deletions, test_lock},
    spans,
};
use tokio::{
    runtime,
    sync::{Barrier, watch},
    time::{sleep, timeout},
};
use tracing::{Level, subscriber::with_default};
use tracing_subscriber::prelude::*;

use super::*;
use crate::{
    _core::{
        BodyStream, BucketOps, BucketsListing, ByteRange, Class, GetObjectResult,
        ListBucketsParams, ListObjectsParams, ListPartsParams, ListUploadsParams, MultipartOps,
        ObjectListing, ObjectOps, PartsListing, PutObjectResult, Storage, TagWriteResult,
        UploadsListing,
        acl::{self, Acl, AclGrants},
        bucket::{self, Bucket},
        checksum, cors, lifecycle,
        multipart::{CompletedPart, MultipartUpload, ObjectPart, PartInfo, PartNumber},
        object, object_lock,
        storage::{self, LifecycleRules, VersionOps},
        version::{self, CurrentVersion, Id, ListParams, Listing, Marker, State},
    },
    _mem::MemoryStorage,
    clock::FixedClock,
    metrics::action_count,
};

/// The tier backend's own `transition` is a later task, so the pass is tested
/// against the contract's seam: `MemoryStorage` answers every storage
/// operation, while this double owns the three tier hooks — a class read from
/// its own placement map, and a recording `transition`.
struct TierDouble {
    inner: MemoryStorage,
    /// The class each key is placed in. A key absent here is `STANDARD`.
    classes: Mutex<HashMap<String, Class>>,
    /// Every completed `transition`, in completion order.
    calls: Mutex<Vec<(String, Class)>>,
    /// Keys whose current slot the double reports as a delete marker: a
    /// listing that names a marker's key, which an unversioned bucket's own
    /// store would never list.
    markers: Mutex<HashSet<String>>,
    /// Keys whose `transition` the store fails, and keys whose object is gone
    /// by the time the move runs.
    failing: Mutex<HashSet<String>>,
    vanished: Mutex<HashSet<String>>,
    /// Moves currently inside `transition`, and the high-water mark over one
    /// pass.
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    /// A rendezvous both workers must reach, so only a pass with two workers
    /// can finish it.
    barrier: Option<Arc<Barrier>>,
}

impl TierDouble {
    fn new() -> Self {
        Self {
            inner: MemoryStorage::new().expect("an in-memory store"),
            classes: Mutex::new(HashMap::new()),
            calls: Mutex::new(Vec::new()),
            markers: Mutex::new(HashSet::new()),
            failing: Mutex::new(HashSet::new()),
            vanished: Mutex::new(HashSet::new()),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
            barrier: None,
        }
    }

    fn with_barrier(mut self, barrier: Arc<Barrier>) -> Self {
        self.barrier = Some(barrier);
        self
    }

    /// Place `key` in `class` before the pass runs.
    fn set_class(&self, key: &object::Key, class: Class) {
        self.classes
            .lock()
            .expect("the class map")
            .insert(key.to_string(), class);
    }

    /// Report `key`'s current slot as a delete marker.
    fn mark(&self, key: &object::Key) {
        self.markers
            .lock()
            .expect("the marker set")
            .insert(key.to_string());
    }

    /// Fail `key`'s move: the store refuses it.
    fn fail(&self, key: &object::Key) {
        self.failing
            .lock()
            .expect("the failure set")
            .insert(key.to_string());
    }

    /// Remove `key` between its listing and its move: the store answers that it
    /// is already gone.
    fn vanish(&self, key: &object::Key) {
        self.vanished
            .lock()
            .expect("the vanished set")
            .insert(key.to_string());
    }

    fn calls(&self) -> Vec<(String, Class)> {
        self.calls.lock().expect("the call log").clone()
    }

    fn max_in_flight(&self) -> usize {
        self.max_in_flight.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl LifecycleRules for TierDouble {
    async fn lifecycles(&self) -> Result<Vec<(bucket::Name, lifecycle::Config)>, storage::Error> {
        self.inner.lifecycles().await
    }
}

#[async_trait]
impl Storage for TierDouble {
    type Error = storage::Error;

    fn supports_tiers(&self) -> bool {
        true
    }

    async fn storage_class(
        &self,
        _bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Class, storage::Error> {
        Ok(self
            .classes
            .lock()
            .expect("the class map")
            .get(key.as_ref())
            .copied()
            .unwrap_or(Class::Standard))
    }

    async fn transition(
        &self,
        _bucket: &bucket::Name,
        key: &object::Key,
        class: Class,
        _now: SystemTime,
    ) -> Result<(), storage::Error> {
        if self
            .failing
            .lock()
            .expect("the failure set")
            .contains(key.as_ref())
        {
            return Err(storage::Error::Io(io::Error::other("boom")));
        }
        if self
            .vanished
            .lock()
            .expect("the vanished set")
            .contains(key.as_ref())
        {
            return Err(storage::Error::NoSuchKey(key.clone()));
        }
        let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);
        if let Some(barrier) = &self.barrier {
            barrier.wait().await;
        }
        self.calls
            .lock()
            .expect("the call log")
            .push((key.to_string(), class));
        self.classes
            .lock()
            .expect("the class map")
            .insert(key.to_string(), class);
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl BucketOps for TierDouble {
    async fn create_bucket(
        &self,
        name: &bucket::Name,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
    ) -> Result<(), storage::Error> {
        Ok(self.inner.create_bucket(name, owner, acl).await?)
    }

    async fn create_bucket_with_lock(
        &self,
        name: &bucket::Name,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
        config: &object_lock::Config,
    ) -> Result<(), storage::Error> {
        Ok(self
            .inner
            .create_bucket_with_lock(name, owner, acl, config)
            .await?)
    }

    async fn get_bucket_object_lock(
        &self,
        name: &bucket::Name,
    ) -> Result<Option<object_lock::Config>, storage::Error> {
        Ok(self.inner.get_bucket_object_lock(name).await?)
    }

    async fn put_bucket_object_lock(
        &self,
        name: &bucket::Name,
        config: &object_lock::Config,
    ) -> Result<(), storage::Error> {
        Ok(self.inner.put_bucket_object_lock(name, config).await?)
    }

    async fn delete_bucket(&self, name: &bucket::Name) -> Result<(), storage::Error> {
        Ok(self.inner.delete_bucket(name).await?)
    }

    async fn head_bucket(&self, name: &bucket::Name) -> Result<Bucket, storage::Error> {
        Ok(self.inner.head_bucket(name).await?)
    }

    async fn list_buckets(
        &self,
        params: ListBucketsParams,
        owner: Option<&acl::OwnerId>,
        lazy_default: Option<&acl::OwnerId>,
    ) -> Result<BucketsListing, storage::Error> {
        Ok(self.inner.list_buckets(params, owner, lazy_default).await?)
    }

    async fn get_bucket_tags(&self, name: &bucket::Name) -> Result<object::Tags, storage::Error> {
        Ok(self.inner.get_bucket_tags(name).await?)
    }

    async fn put_bucket_tags(
        &self,
        name: &bucket::Name,
        tags: &object::Tags,
    ) -> Result<(), storage::Error> {
        Ok(self.inner.put_bucket_tags(name, tags).await?)
    }

    async fn delete_bucket_tags(&self, name: &bucket::Name) -> Result<(), storage::Error> {
        Ok(self.inner.delete_bucket_tags(name).await?)
    }

    async fn get_bucket_cors(
        &self,
        name: &bucket::Name,
    ) -> Result<Option<cors::Config>, storage::Error> {
        Ok(self.inner.get_bucket_cors(name).await?)
    }

    async fn put_bucket_cors(
        &self,
        name: &bucket::Name,
        cors: &cors::Config,
    ) -> Result<(), storage::Error> {
        Ok(self.inner.put_bucket_cors(name, cors).await?)
    }

    async fn delete_bucket_cors(&self, name: &bucket::Name) -> Result<(), storage::Error> {
        Ok(self.inner.delete_bucket_cors(name).await?)
    }

    async fn get_bucket_lifecycle(
        &self,
        name: &bucket::Name,
    ) -> Result<Option<lifecycle::Config>, storage::Error> {
        Ok(self.inner.get_bucket_lifecycle(name).await?)
    }

    async fn put_bucket_lifecycle(
        &self,
        name: &bucket::Name,
        config: &lifecycle::Config,
    ) -> Result<(), storage::Error> {
        Ok(self.inner.put_bucket_lifecycle(name, config).await?)
    }

    async fn merge_bucket_lifecycle(
        &self,
        name: &bucket::Name,
        rules: &[lifecycle::Rule],
    ) -> Result<lifecycle::Merged, storage::Error> {
        Ok(self.inner.merge_bucket_lifecycle(name, rules).await?)
    }

    async fn delete_bucket_lifecycle(&self, name: &bucket::Name) -> Result<(), storage::Error> {
        Ok(self.inner.delete_bucket_lifecycle(name).await?)
    }

    async fn get_bucket_acl(&self, name: &bucket::Name) -> Result<Acl, storage::Error> {
        Ok(self.inner.get_bucket_acl(name).await?)
    }

    async fn put_bucket_acl(
        &self,
        name: &bucket::Name,
        grants: &AclGrants,
    ) -> Result<(), storage::Error> {
        Ok(self.inner.put_bucket_acl(name, grants).await?)
    }
}

#[async_trait]
impl ObjectOps for TierDouble {
    type StagedBody = <MemoryStorage as ObjectOps>::StagedBody;

    async fn put_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        body: BodyStream,
    ) -> Result<PutObjectResult, storage::Error> {
        Ok(self.inner.put_object(bucket, key, body).await?)
    }

    async fn stage_body(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        body: BodyStream,
        tee: Option<Arc<checksum::PartChecksum>>,
        carried: Option<checksum::Recorded>,
    ) -> Result<Self::StagedBody, storage::Error> {
        Ok(self
            .inner
            .stage_body(bucket, key, body, tee, carried)
            .await?)
    }

    async fn resolve_write_lock(
        &self,
        bucket: &bucket::Name,
        explicit: Option<&object_lock::Retention>,
        legal_hold: object_lock::LegalHold,
        now: SystemTime,
    ) -> Result<Option<object_lock::Lock>, storage::Error> {
        Ok(self
            .inner
            .resolve_write_lock(bucket, explicit, legal_hold, now)
            .await?)
    }

    async fn commit_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        staged: Self::StagedBody,
        tags: object::Tags,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
        lock: Option<&object_lock::Lock>,
    ) -> Result<object::Info, storage::Error> {
        Ok(self
            .inner
            .commit_object(bucket, key, staged, tags, owner, acl, lock)
            .await?)
    }

    async fn get_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        range: Option<ByteRange>,
    ) -> Result<GetObjectResult, storage::Error> {
        Ok(self.inner.get_object(bucket, key, range).await?)
    }

    async fn rename_object(
        &self,
        bucket: &bucket::Name,
        src: &object::Key,
        dst: &object::Key,
    ) -> Result<object::Info, storage::Error> {
        Ok(self.inner.rename_object(bucket, src, dst).await?)
    }

    async fn head_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<object::Info, storage::Error> {
        Ok(self.inner.head_object(bucket, key).await?)
    }

    async fn delete_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Marker, storage::Error> {
        Ok(self.inner.delete_object(bucket, key).await?)
    }

    async fn get_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<object::Tags, storage::Error> {
        Ok(self.inner.get_object_tags(bucket, key).await?)
    }

    async fn put_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<TagWriteResult, storage::Error> {
        Ok(self.inner.put_object_tags(bucket, key, tags).await?)
    }

    async fn delete_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<TagWriteResult, storage::Error> {
        Ok(self.inner.delete_object_tags(bucket, key).await?)
    }

    async fn get_object_acl(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Acl, storage::Error> {
        Ok(self.inner.get_object_acl(bucket, key).await?)
    }

    async fn put_object_acl(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        grants: &AclGrants,
    ) -> Result<(), storage::Error> {
        Ok(self.inner.put_object_acl(bucket, key, grants).await?)
    }

    async fn list_objects(
        &self,
        params: ListObjectsParams,
    ) -> Result<ObjectListing, storage::Error> {
        Ok(self.inner.list_objects(params).await?)
    }

    async fn list_object_parts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Vec<ObjectPart>, storage::Error> {
        Ok(self.inner.list_object_parts(bucket, key).await?)
    }
}

#[async_trait]
impl MultipartOps for TierDouble {
    async fn create_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        checksum: Option<checksum::Upload>,
        tags: object::Tags,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
        lock: Option<&object_lock::Lock>,
    ) -> Result<MultipartUpload, storage::Error> {
        Ok(self
            .inner
            .create_multipart_upload(bucket, key, checksum, tags, owner, acl, lock)
            .await?)
    }

    async fn get_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<MultipartUpload, storage::Error> {
        Ok(self
            .inner
            .get_multipart_upload(bucket, key, upload_id)
            .await?)
    }

    async fn upload_part(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        part_number: PartNumber,
        body: BodyStream,
        checksum: Option<Arc<checksum::PartChecksum>>,
    ) -> Result<PartInfo, storage::Error> {
        Ok(self
            .inner
            .upload_part(bucket, key, upload_id, part_number, body, checksum)
            .await?)
    }

    async fn list_parts(&self, params: ListPartsParams) -> Result<PartsListing, storage::Error> {
        Ok(self.inner.list_parts(params).await?)
    }

    async fn complete_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        parts: &[CompletedPart],
        checksum: Option<checksum::Recorded>,
    ) -> Result<object::Info, storage::Error> {
        Ok(self
            .inner
            .complete_multipart_upload(bucket, key, upload_id, parts, checksum)
            .await?)
    }

    async fn abort_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<(), storage::Error> {
        Ok(self
            .inner
            .abort_multipart_upload(bucket, key, upload_id)
            .await?)
    }

    async fn list_multipart_uploads(
        &self,
        params: ListUploadsParams,
    ) -> Result<UploadsListing, storage::Error> {
        Ok(self.inner.list_multipart_uploads(params).await?)
    }
}

#[async_trait]
impl VersionOps for TierDouble {
    async fn bucket_versioning(&self, bucket: &bucket::Name) -> Result<State, storage::Error> {
        Ok(self.inner.bucket_versioning(bucket).await?)
    }

    async fn set_bucket_versioning(
        &self,
        bucket: &bucket::Name,
        state: State,
    ) -> Result<(), storage::Error> {
        Ok(self.inner.set_bucket_versioning(bucket, state).await?)
    }

    async fn delete_object_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
        bypass: object_lock::Bypass,
    ) -> Result<Marker, storage::Error> {
        Ok(self
            .inner
            .delete_object_version(bucket, key, version, bypass)
            .await?)
    }

    async fn list_object_versions(
        &self,
        bucket: &bucket::Name,
        params: ListParams,
    ) -> Result<Listing, storage::Error> {
        Ok(self.inner.list_object_versions(bucket, params).await?)
    }

    async fn current_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<CurrentVersion, storage::Error> {
        if self
            .markers
            .lock()
            .expect("the marker set")
            .contains(key.as_ref())
        {
            return Ok(CurrentVersion::Marker);
        }
        Ok(self.inner.current_version(bucket, key).await?)
    }

    async fn version_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<object::Tags, storage::Error> {
        Ok(self.inner.version_tags(bucket, key, version).await?)
    }
}

/// A double holding one bucket with `keys` written, all at the wall clock's
/// now.
async fn tier_bucket(storage: &TierDouble, name: &str, keys: &[&str]) -> bucket::Name {
    let bucket = bucket::name(name).expect("a bucket name");
    storage
        .create_bucket(&bucket, None, &Acl::default_private(None))
        .await
        .expect("a bucket");
    for key in keys {
        storage
            .put_object(
                &bucket,
                &object::key(*key).expect("an object key"),
                Box::pin(stream::empty()),
            )
            .await
            .expect("an object");
    }
    bucket
}

/// A pass over the tier double, a hundred days on so every day count is due,
/// with `concurrency` workers and no pause.
fn tier_runner(storage: &Arc<TierDouble>, concurrency: usize) -> Runner<TierDouble> {
    let (_stop, stopped) = watch::channel(false);
    Runner::new(
        Arc::clone(storage),
        Options {
            delay: Duration::ZERO,
            concurrency,
            ..Options::default()
        },
        Arc::new(FixedClock(
            SystemTime::now() + Duration::from_secs(100 * 86_400),
        )),
        stopped,
    )
}

/// A noncurrent rule by version count rather than age: deterministic
/// regardless of the wall clock (a `Days` rule counts to the next midnight).
fn noncurrent_budget(keep: i32) -> lifecycle::Rule {
    lifecycle::Rule {
        noncurrent_version_expiration: Some(lifecycle::NoncurrentVersionExpiration {
            noncurrent_days: None,
            newer_noncurrent_versions: Some(keep),
        }),
        // `NewerNoncurrentVersions` requires its own filter.
        filter: lifecycle::Filter {
            prefix: Some("a".into()),
            ..lifecycle::Filter::default()
        },
        ..rule()
    }
}

/// A lock-enabled versioned bucket holding three versions of one key: the two
/// older ones are history, the newest is current. Answers the oldest version's
/// id.
async fn locked_history(
    mode: object_lock::Mode,
) -> (Arc<MemoryStorage>, bucket::Name, object::Key, version::Id) {
    let storage = Arc::new(MemoryStorage::new().unwrap());
    let bucket = bucket::name("locked").unwrap();
    let key = object::key("a.txt").unwrap();
    storage
        .create_bucket(&bucket, None, &Acl::default_private(None))
        .await
        .unwrap();
    storage
        .set_bucket_versioning(&bucket, State::Enabled)
        .await
        .unwrap();
    storage
        .put_bucket_object_lock(
            &bucket,
            &object_lock::Config {
                enabled: true,
                rule: None,
            },
        )
        .await
        .unwrap();
    for _ in 0..3 {
        storage
            .put_object(&bucket, &key, Box::pin(stream::empty()))
            .await
            .unwrap();
    }
    let entries = storage
        .list_object_versions(
            &bucket,
            ListParams {
                max_keys: PAGE,
                ..ListParams::default()
            },
        )
        .await
        .unwrap()
        .entries;
    assert_eq!(entries.len(), 3, "three versions of one key");
    let old = entries[2]
        .id
        .clone()
        .expect("a versioned write mints an id");
    storage
        .set_retention(
            &bucket,
            &key,
            &version::Address::Id(old.clone()),
            Some(&object_lock::Retention {
                mode,
                retain_until: SystemTime::now() + Duration::from_secs(86_400),
            }),
            object_lock::Bypass::None,
            SystemTime::now(),
        )
        .await
        .unwrap();
    (storage, bucket, key, old)
}

/// A clock a minute on, so a zero-day noncurrent threshold is already due.
fn due_runner(storage: &Arc<MemoryStorage>) -> Runner<MemoryStorage> {
    let (_stop, stopped) = watch::channel(false);
    Runner::new(
        Arc::clone(storage),
        Options {
            delay: Duration::ZERO,
            ..Options::default()
        },
        Arc::new(FixedClock(
            SystemTime::now() + Duration::from_secs(100 * 86_400),
        )),
        stopped,
    )
}

/// OL19: a `Locked` noncurrent expiration is a counted skip, not an error. The
/// version stays, nothing is counted as expired or failed, and the metric
/// records the lifecycle operation with the refusal reason.
#[tokio::test]
async fn a_locked_noncurrent_version_is_a_counted_skip() {
    let (storage, bucket, _key, _old) = locked_history(object_lock::Mode::Governance).await;
    let before = protected_deletions("lifecycle", "governance");
    let runner = due_runner(&storage);
    let seeded = runner
        .seed(&[Seed {
            bucket: bucket.clone(),
            rule: noncurrent_budget(1),
        }])
        .await;
    let summary = runner.pass().await;

    assert_eq!(seeded.buckets_with_rules, 1, "the rule was stored");
    assert_eq!(summary.protected_versions, 1, "the skip is counted");
    assert_eq!(summary.expired_versions, 0, "nothing was removed");
    assert_eq!(summary.errors, 0, "a protected version is not an error");
    assert_eq!(
        storage
            .list_object_versions(
                &bucket,
                ListParams {
                    max_keys: PAGE,
                    ..ListParams::default()
                },
            )
            .await
            .unwrap()
            .entries
            .len(),
        3,
        "the protected history row survives"
    );
    assert_eq!(
        protected_deletions("lifecycle", "governance") - before,
        1.0,
        "the refusal is recorded under operation=lifecycle, reason=governance"
    );
}

/// OL19: current-version expiration is not lock-protected — a DELETE without a
/// version id writes a marker even on a lock-enabled bucket.
#[tokio::test]
async fn the_current_version_still_expires_to_a_marker_on_a_locked_bucket() {
    let storage = Arc::new(MemoryStorage::new().unwrap());
    let bucket = bucket::name("locked").unwrap();
    let key = object::key("a.txt").unwrap();
    storage
        .create_bucket(&bucket, None, &Acl::default_private(None))
        .await
        .unwrap();
    storage
        .set_bucket_versioning(&bucket, State::Enabled)
        .await
        .unwrap();
    storage
        .put_bucket_object_lock(
            &bucket,
            &object_lock::Config {
                enabled: true,
                rule: None,
            },
        )
        .await
        .unwrap();
    storage
        .put_object(&bucket, &key, Box::pin(stream::empty()))
        .await
        .unwrap();

    let runner = due_runner(&storage);
    runner
        .seed(&[Seed {
            bucket: bucket.clone(),
            rule: expires(1),
        }])
        .await;
    let summary = runner.pass().await;

    assert_eq!(summary.expired_objects, 1, "the current version expires");
    assert_eq!(summary.protected_versions, 0, "no lock refuses a marker");
    assert_eq!(
        storage.current_version(&bucket, &key).await.unwrap(),
        CurrentVersion::Marker,
        "a versioned bucket's expiration writes a marker"
    );
}

/// OL19: `ExpiredObjectDeleteMarker` removes a marker (not protected), even on
/// a lock-enabled bucket.
#[tokio::test]
async fn an_expired_object_delete_marker_is_removed_on_a_locked_bucket() {
    let storage = Arc::new(MemoryStorage::new().unwrap());
    let bucket = bucket::name("locked").unwrap();
    let key = object::key("a.txt").unwrap();
    storage
        .create_bucket(&bucket, None, &Acl::default_private(None))
        .await
        .unwrap();
    storage
        .set_bucket_versioning(&bucket, State::Enabled)
        .await
        .unwrap();
    storage
        .put_bucket_object_lock(
            &bucket,
            &object_lock::Config {
                enabled: true,
                rule: None,
            },
        )
        .await
        .unwrap();
    storage
        .put_object(&bucket, &key, Box::pin(stream::empty()))
        .await
        .unwrap();
    let content = storage
        .list_object_versions(
            &bucket,
            ListParams {
                max_keys: PAGE,
                ..ListParams::default()
            },
        )
        .await
        .unwrap()
        .entries[0]
        .id
        .clone();
    storage.delete_object(&bucket, &key).await.unwrap();
    storage
        .delete_object_version(&bucket, &key, content.as_ref(), object_lock::Bypass::None)
        .await
        .unwrap();

    let runner = due_runner(&storage);
    runner
        .seed(&[Seed {
            bucket: bucket.clone(),
            rule: lifecycle::Rule {
                expiration: Some(lifecycle::Expiration {
                    days: None,
                    date: None,
                    expired_object_delete_marker: Some(true),
                }),
                ..rule()
            },
        }])
        .await;
    let summary = runner.pass().await;

    assert_eq!(summary.expired_versions, 1, "the marker is removed");
    assert_eq!(summary.protected_versions, 0, "a marker is never protected");
    assert!(
        storage
            .list_object_versions(
                &bucket,
                ListParams {
                    max_keys: PAGE,
                    ..ListParams::default()
                },
            )
            .await
            .unwrap()
            .entries
            .is_empty(),
        "the key's only version (the marker) is gone"
    );
}

/// A versioned bucket holding one key.
async fn marked() -> (Arc<MemoryStorage>, bucket::Name, object::Key) {
    let storage = Arc::new(MemoryStorage::new().unwrap());
    let bucket = bucket::name("data").unwrap();
    storage
        .create_bucket(&bucket, None, &Acl::default_private(None))
        .await
        .unwrap();
    storage
        .set_bucket_versioning(&bucket, State::Enabled)
        .await
        .unwrap();
    let key = object::key("a.txt").unwrap();
    storage
        .put_object(&bucket, &key, Box::pin(stream::empty()))
        .await
        .unwrap();
    storage.delete_object(&bucket, &key).await.unwrap();
    (storage, bucket, key)
}

fn runner(storage: &Arc<MemoryStorage>) -> Runner<MemoryStorage> {
    let (_stop, stopped) = watch::channel(false);
    Runner::new(
        Arc::clone(storage),
        Options {
            delay: Duration::ZERO,
            ..Options::default()
        },
        Arc::new(FixedClock(SystemTime::now())),
        stopped,
    )
}

/// LC2's guard on its own: the object listing can name a due key whose
/// current slot is a marker (a store that lists its content, not its
/// rows), and the pass must not expire that key again.
#[tokio::test]
async fn a_marked_key_is_not_due() {
    let (storage, bucket, key) = marked().await;
    let runner = runner(&storage);
    assert!(
        !runner.due(&bucket, &key).await.unwrap(),
        "a key whose current slot is a delete marker has no current version to expire"
    );
    // The same key put back is due again: the guard reads the slot, not a
    // remembered key.
    storage
        .put_object(&bucket, &key, Box::pin(stream::empty()))
        .await
        .unwrap();
    assert!(runner.due(&bucket, &key).await.unwrap());
}

/// The pass's accounting seam, pinned where it lives: a delete is
/// idempotent, so an item that vanished first is the outcome the action
/// wanted and counts as done; anything else is one error.
///
/// Pinned here rather than through an engine leg because no `tinio-mem`
/// path reaches the branch — its deletes answer `Ok` for a missing row,
/// which is exactly why the seam exists.
#[test]
fn a_key_that_vanished_is_counted_not_failed() {
    let _guard = test_lock::window();
    let before = action_count(Kind::Object, "error");
    let mut summary = Summary::default();
    for gone in [
        storage::Error::NoSuchKey(object::key("a.txt").unwrap()),
        storage::Error::NoSuchVersion("v1".into()),
        storage::Error::NoSuchUpload("u1".into()),
        storage::Error::NoSuchBucket(bucket::name("data").unwrap()),
    ] {
        assert!(
            counted(Err::<(), _>(gone), &mut summary, Kind::Object),
            "an item already gone is the outcome the action wanted"
        );
    }
    assert_eq!(summary.errors, 0);

    let broken: storage::Error = storage::Error::Io(io::Error::other("boom"));
    assert!(
        !counted(Err::<(), _>(broken), &mut summary, Kind::Object),
        "a real failure is not the action's outcome"
    );
    assert_eq!(summary.errors, 1);
    assert_eq!(
        action_count(Kind::Object, "error") - before,
        1.0,
        "a failed action records one outcome=\"error\" sample"
    );
}

/// The read seam's one classification (R41): a bucket that vanished
/// mid-pass is gone, not broken, so it costs no `errors`; every other read
/// failure does.
#[test]
fn a_vanished_bucket_is_gone_not_broken() {
    let _guard = test_lock::window();
    let before = action_count(Kind::Object, "error");
    let bucket = bucket::name("data").unwrap();
    let mut summary = Summary::default();
    read_failed(
        storage::Error::NoSuchBucket(bucket.clone()),
        &bucket,
        &mut summary,
        "list a bucket's objects",
        Kind::Object,
    );
    assert_eq!(summary.errors, 0);
    read_failed(
        storage::Error::Io(io::Error::other("boom")),
        &bucket,
        &mut summary,
        "list a bucket's objects",
        Kind::Object,
    );
    assert_eq!(summary.errors, 1);
    assert_eq!(
        action_count(Kind::Object, "error") - before,
        1.0,
        "a failed action records one outcome=\"error\" sample"
    );
}

/// A stop covers the chain the walk ends on as well as the entry the
/// listing loop last saw. A marker that is its key's only version is the
/// chain only the trailing call can reach: the walk's own per-entry check
/// guards a version row, and this chain has none.
#[tokio::test]
async fn a_stopping_chain_is_left_whole() {
    let (storage, bucket, key) = marked().await;
    // Remove the version behind the marker: the marker is now the key's
    // only version, which `ExpiredObjectDeleteMarker` would remove.
    let hidden = storage
        .list_object_versions(
            &bucket,
            ListParams {
                max_keys: PAGE,
                ..ListParams::default()
            },
        )
        .await
        .unwrap()
        .entries[1]
        .id
        .clone();
    storage
        .delete_object_version(&bucket, &key, hidden.as_ref(), object_lock::Bypass::None)
        .await
        .unwrap();
    let (stop, stopped) = watch::channel(false);
    let runner = Runner::new(
        Arc::clone(&storage),
        Options {
            delay: Duration::ZERO,
            ..Options::default()
        },
        Arc::new(FixedClock(SystemTime::now())),
        stopped,
    );
    runner
        .seed(&[Seed {
            bucket: bucket.clone(),
            rule: lifecycle::Rule {
                id: None,
                status: lifecycle::Status::Enabled,
                filter: lifecycle::Filter::default(),
                expiration: Some(lifecycle::Expiration {
                    days: None,
                    date: None,
                    expired_object_delete_marker: Some(true),
                }),
                transitions: Vec::new(),
                noncurrent_version_expiration: None,
                abort_incomplete_multipart_upload: None,
            },
        }])
        .await;
    let config = storage
        .get_bucket_lifecycle(&bucket)
        .await
        .unwrap()
        .unwrap();
    let listing = || ListParams {
        max_keys: PAGE,
        ..ListParams::default()
    };
    let chain = storage
        .list_object_versions(&bucket, listing())
        .await
        .unwrap()
        .entries;
    stop.send(true).unwrap();

    let mut summary = Summary::default();
    let mut sweep = Sweep {
        bucket: &bucket,
        config: &config,
        now: SystemTime::now(),
        reads_tags: false,
        summary: &mut summary,
    };
    assert!(
        !runner.expire_chain(&mut sweep, &chain).await,
        "a chain reached after the stop does not carry on"
    );
    assert_eq!(summary.expired_versions, 0);
    assert_eq!(
        storage
            .list_object_versions(&bucket, listing())
            .await
            .unwrap()
            .entries
            .len(),
        1,
        "the marker is still there: nothing was acted on"
    );
}

#[test]
fn a_day_count_rounds_forward_to_midnight() {
    let noon = SystemTime::UNIX_EPOCH + Duration::from_secs(43_200);
    let due = lifecycle::midnight_utc(noon, 1);
    assert!(
        !due_on_midnight(noon, 1, due - Duration::from_secs(1)),
        "one raw day is not yet the next midnight"
    );
    assert!(due_on_midnight(noon, 1, due));
    assert!(
        due_on_midnight(noon, 0, noon),
        "a zero day count is due at the anchor"
    );
    assert!(
        !due_on_midnight(due, 1, noon),
        "a clock that stepped back expires nothing"
    );
}

/// An enabled rule on every object.
fn rule() -> lifecycle::Rule {
    lifecycle::Rule {
        id: None,
        status: lifecycle::Status::Enabled,
        filter: lifecycle::Filter::default(),
        expiration: None,
        transitions: Vec::new(),
        noncurrent_version_expiration: None,
        abort_incomplete_multipart_upload: None,
    }
}

/// Expire the current version `days` after it was created.
fn expires(days: i32) -> lifecycle::Rule {
    lifecycle::Rule {
        expiration: Some(lifecycle::Expiration {
            days: Some(days),
            date: None,
            expired_object_delete_marker: None,
        }),
        ..rule()
    }
}

/// Expire a noncurrent version `days` after it became noncurrent.
fn noncurrent(days: i32) -> lifecycle::Rule {
    lifecycle::Rule {
        noncurrent_version_expiration: Some(lifecycle::NoncurrentVersionExpiration {
            noncurrent_days: Some(days),
            newer_noncurrent_versions: None,
        }),
        ..rule()
    }
}

/// Abort an incomplete upload `days` after it was initiated.
fn aborts(days: i32) -> lifecycle::Rule {
    lifecycle::Rule {
        abort_incomplete_multipart_upload: Some(lifecycle::AbortIncompleteMultipartUpload {
            days_after_initiation: days,
        }),
        ..rule()
    }
}

/// The transition this build accepts (T11): the one action a backend without tiers refuses.
fn transitions(days: i32) -> lifecycle::Rule {
    lifecycle::Rule {
        id: Some("archive".into()),
        transitions: vec![lifecycle::Transition {
            days: Some(days),
            date: None,
            storage_class: Class::StandardIa,
        }],
        ..rule()
    }
}

/// T11: the tier gate covers the seed. A backend without tiers refuses the merge — counted as
/// the write failure it is — and stores nothing, so no rule is left promising a migration the
/// backend cannot run.
#[tokio::test]
async fn a_non_tier_seed_refuses_transitions() {
    let storage = Arc::new(MemoryStorage::new().unwrap());
    let bucket = bucket::name("logs").unwrap();
    storage
        .create_bucket(&bucket, None, &Acl::default_private(None))
        .await
        .unwrap();
    let runner = due_runner(&storage);
    let seeded = runner
        .seed(&[Seed {
            bucket: bucket.clone(),
            rule: transitions(30),
        }])
        .await;

    assert_eq!(
        seeded.buckets_with_rules, 0,
        "the refused bucket took no rule"
    );
    assert_eq!(seeded.errors, 1, "the refusal is a counted failure");
    assert_eq!(
        storage.get_bucket_lifecycle(&bucket).await.unwrap(),
        None,
        "nothing was written"
    );
}

/// The pass is the T0 unit: one `info` span, one `debug` child per action,
/// the chain walk under its version action, and the summary event inside it.
#[test]
fn a_pass_spans_its_cycle_and_its_actions() {
    let runtime = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let runner = runtime.block_on(async {
        let storage = Arc::new(MemoryStorage::new().unwrap());
        let data = bucket::name("data").unwrap();
        let archive = bucket::name("archive").unwrap();
        for name in [&data, &archive] {
            storage
                .create_bucket(name, None, &Acl::default_private(None))
                .await
                .unwrap();
        }
        storage
            .set_bucket_versioning(&archive, State::Enabled)
            .await
            .unwrap();
        storage
            .put_object(
                &data,
                &object::key("a.txt").unwrap(),
                Box::pin(stream::empty()),
            )
            .await
            .unwrap();
        let logs = object::key("logs/b.txt").unwrap();
        for _ in 0..2 {
            storage
                .put_object(&archive, &logs, Box::pin(stream::empty()))
                .await
                .unwrap();
        }
        storage
            .create_multipart_upload(
                &archive,
                &object::key("big.bin").unwrap(),
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
                None,
            )
            .await
            .unwrap();
        let (_stop, stopped) = watch::channel(false);
        let runner = Runner::new(
            Arc::clone(&storage),
            Options {
                delay: Duration::ZERO,
                ..Options::default()
            },
            Arc::new(FixedClock(
                SystemTime::now() + Duration::from_secs(100 * 86_400),
            )),
            stopped,
        );
        // The seed merges by rule id: two id-less rules would replace each
        // other, so the archive's two actions need their own ids.
        let mut versioned = noncurrent(1);
        versioned.id = Some("noncurrent".into());
        let mut uploads = aborts(1);
        uploads.id = Some("abort".into());
        runner
            .seed(&[
                Seed {
                    bucket: data,
                    rule: expires(1),
                },
                Seed {
                    bucket: archive.clone(),
                    rule: versioned,
                },
                Seed {
                    bucket: archive,
                    rule: uploads,
                },
            ])
            .await;
        runner
    });

    let capture = spans::Capture::new();
    with_default(tracing_subscriber::registry().with(capture.clone()), || {
        runtime.block_on(async {
            let summary = runner.pass().await;
            assert_eq!(summary.buckets_with_rules, 2, "both buckets were swept");
        });
    });

    let recorded = capture.spans_on_this_thread();
    let spans = &recorded;
    let pass = spans
        .iter()
        .find(|span| span.name == "tinio.lifecycle.pass")
        .expect("the pass opens its info span");
    assert_eq!(pass.target, "tinio::lifecycle");
    assert_eq!(pass.level, Level::INFO);
    assert!(
        pass.parent.is_none(),
        "the pass is the root of its own tree"
    );
    let field = |name: &str| {
        pass.fields
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    };
    // The whole summary rides on the span — every field the close line prints.
    for name in [
        "buckets_with_rules",
        "expired_objects",
        "transitioned_objects",
        "expired_versions",
        "protected_versions",
        "aborted_uploads",
        "errors",
    ] {
        assert!(
            field(name).is_some(),
            "the summary's {name} is recorded: {:?}",
            pass.fields
        );
    }
    assert_eq!(
        field("buckets_with_rules"),
        Some("2"),
        "the summary rides on the span, so the close line prints it: {:?}",
        pass.fields
    );
    assert_eq!(
        field("errors"),
        Some("0"),
        "no action failed in this pass: {:?}",
        pass.fields
    );

    for action in ["expire_objects", "expire_versions", "abort_uploads"] {
        let span = spans
            .iter()
            .find(|span| span.name == action)
            .unwrap_or_else(|| panic!("the pass opened {action}: {spans:?}"));
        assert_eq!(span.target, "tinio::lifecycle");
        assert_eq!(span.level, Level::DEBUG);
        assert_eq!(span.parent, Some(pass.id), "{action} is a pass child");
    }
    let versions = spans
        .iter()
        .find(|span| span.name == "expire_versions")
        .expect("the version action ran");
    assert!(
        spans
            .iter()
            .any(|span| span.name == "expire_chain" && span.parent == Some(versions.id)),
        "the chain walk is a child of the version action: {spans:?}"
    );
}

/// The run loop's body is the unit, so each cycle opens its own `info` span
/// above that cycle's pass.
#[test]
fn a_run_cycle_owns_a_span_above_its_pass() {
    let runtime = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let capture = spans::Capture::new();
    let (stop, stopped) = watch::channel(false);
    let runner = Runner::new(
        Arc::new(MemoryStorage::new().unwrap()),
        Options {
            delay: Duration::ZERO,
            cycle: Duration::from_millis(10),
            ..Options::default()
        },
        Arc::new(FixedClock(SystemTime::now())),
        stopped,
    );
    with_default(tracing_subscriber::registry().with(capture.clone()), || {
        runtime.block_on(async move {
            let run = tokio::spawn(async move { runner.run().await });
            sleep(Duration::from_millis(50)).await;
            stop.send(true).unwrap();
            run.await.unwrap();
        });
    });

    let recorded = capture.spans_on_this_thread();
    let spans = &recorded;
    let runs: Vec<_> = spans
        .iter()
        .filter(|span| span.name == "tinio.lifecycle.run")
        .collect();
    assert!(!runs.is_empty(), "the loop opens one span per cycle");
    assert!(
        runs.iter()
            .all(|span| span.target == "tinio::lifecycle" && span.level == Level::INFO),
        "the cycle span is the lifecycle layer's info span: {runs:?}"
    );
    let pass = spans
        .iter()
        .find(|span| span.name == "tinio.lifecycle.pass")
        .expect("a cycle ran its pass");
    assert!(
        runs.iter().any(|run| pass.parent == Some(run.id)),
        "the pass is a child of the cycle that drove it: {spans:?}"
    );
}

/// T11: a due transition asks the backend to move the object, and the object
/// itself stays — a transition is a move between planes, never a deletion.
#[tokio::test]
async fn a_due_transition_demotes() {
    let storage = Arc::new(TierDouble::new());
    let bucket = tier_bucket(&storage, "data", &["a.txt"]).await;
    let key = object::key("a.txt").expect("an object key");
    let runner = tier_runner(&storage, 1);
    let seeded = runner
        .seed(&[Seed {
            bucket: bucket.clone(),
            rule: transitions(30),
        }])
        .await;
    assert_eq!(
        seeded.buckets_with_rules, 1,
        "a backend with tiers takes the rule"
    );

    let summary = runner.pass().await;

    assert_eq!(summary.transitioned_objects, 1, "the due object moved");
    assert_eq!(summary.errors, 0);
    assert_eq!(
        storage.calls(),
        vec![("a.txt".to_string(), Class::StandardIa)],
        "one move, to the rule's class"
    );
    assert_eq!(
        storage.storage_class(&bucket, &key).await.expect("a class"),
        Class::StandardIa
    );
    assert!(
        storage.head_object(&bucket, &key).await.is_ok(),
        "a transition moves the object, never deletes it"
    );
}

/// T11: a rule naming the class the object already holds is a no-op — the
/// class is read before the move, so the backend is never asked.
#[tokio::test]
async fn a_transition_to_the_current_class_is_a_noop() {
    let storage = Arc::new(TierDouble::new());
    let bucket = tier_bucket(&storage, "data", &["a.txt"]).await;
    let key = object::key("a.txt").expect("an object key");
    storage.set_class(&key, Class::StandardIa);
    let runner = tier_runner(&storage, 1);
    runner
        .seed(&[Seed {
            bucket,
            rule: transitions(30),
        }])
        .await;

    let summary = runner.pass().await;

    assert_eq!(summary.transitioned_objects, 0, "nothing changed class");
    assert_eq!(summary.errors, 0);
    assert!(
        storage.calls().is_empty(),
        "the object already holds the target class, so no move is asked for"
    );
}

/// T11: versioned buckets skip transitions only, even when one rule carries
/// both actions. Expiration still runs — on a versioned bucket it writes a
/// delete marker — while the due transition is never asked for.
#[tokio::test]
async fn a_versioned_bucket_still_expires() {
    let storage = Arc::new(TierDouble::new());
    let bucket = tier_bucket(&storage, "data", &["a.txt"]).await;
    storage
        .set_bucket_versioning(&bucket, State::Enabled)
        .await
        .expect("versioning");
    let key = object::key("a.txt").expect("an object key");
    let runner = tier_runner(&storage, 1);
    let mut rule = expires(1);
    rule.id = Some("archive".into());
    rule.transitions = transitions(30).transitions;
    runner
        .seed(&[Seed {
            bucket: bucket.clone(),
            rule,
        }])
        .await;

    let summary = runner.pass().await;

    assert_eq!(
        summary.expired_objects, 1,
        "a versioned bucket's expiration runs"
    );
    assert_eq!(
        summary.transitioned_objects, 0,
        "the same rule's transition stands down"
    );
    assert!(
        storage.calls().is_empty(),
        "the due transition is never asked for"
    );
    assert_eq!(
        storage
            .current_version(&bucket, &key)
            .await
            .expect("a current version"),
        CurrentVersion::Marker,
        "the expired current version is marked"
    );
}

/// T11: on a versioned bucket the transition alone stands down; a second rule
/// in the same bucket still expires.
#[tokio::test]
async fn a_versioned_bucket_skips_only_the_transition() {
    let storage = Arc::new(TierDouble::new());
    let bucket = tier_bucket(&storage, "data", &["archive/a.txt", "tmp/b.txt"]).await;
    storage
        .set_bucket_versioning(&bucket, State::Enabled)
        .await
        .expect("versioning");
    let archive_key = object::key("archive/a.txt").expect("an object key");
    let tmp_key = object::key("tmp/b.txt").expect("an object key");
    let runner = tier_runner(&storage, 1);
    let mut archive = transitions(30);
    archive.filter = lifecycle::Filter {
        prefix: Some("archive/".into()),
        ..lifecycle::Filter::default()
    };
    let mut expire = expires(1);
    expire.id = Some("expire".into());
    expire.filter = lifecycle::Filter {
        prefix: Some("tmp/".into()),
        ..lifecycle::Filter::default()
    };
    runner
        .seed(&[
            Seed {
                bucket: bucket.clone(),
                rule: archive,
            },
            Seed {
                bucket: bucket.clone(),
                rule: expire,
            },
        ])
        .await;

    let summary = runner.pass().await;

    assert!(
        storage.calls().is_empty(),
        "a bucket that records versions never transitions"
    );
    assert_eq!(summary.transitioned_objects, 0);
    assert_eq!(
        summary.expired_objects, 1,
        "the same bucket's expiration still runs"
    );
    assert!(
        storage.head_object(&bucket, &archive_key).await.is_ok(),
        "the transition's object stays where it was"
    );
    assert_eq!(
        storage
            .current_version(&bucket, &tmp_key)
            .await
            .expect("a current version"),
        CurrentVersion::Marker,
        "the expired key is marked"
    );
}

/// T11: a suspended bucket skips the transition for the same reason a versioned
/// one does — a transition-only rule promises a class the bucket's own version
/// chain cannot keep — while the bucket's expiration still runs.
#[tokio::test]
async fn a_suspended_bucket_skips_only_the_transition() {
    let storage = Arc::new(TierDouble::new());
    let bucket = tier_bucket(&storage, "data", &["archive/a.txt", "tmp/b.txt"]).await;
    storage
        .set_bucket_versioning(&bucket, State::Suspended)
        .await
        .expect("versioning");
    let archive_key = object::key("archive/a.txt").expect("an object key");
    let tmp_key = object::key("tmp/b.txt").expect("an object key");
    let runner = tier_runner(&storage, 1);
    let mut archive = transitions(30);
    archive.filter = lifecycle::Filter {
        prefix: Some("archive/".into()),
        ..lifecycle::Filter::default()
    };
    let mut expire = expires(1);
    expire.id = Some("expire".into());
    expire.filter = lifecycle::Filter {
        prefix: Some("tmp/".into()),
        ..lifecycle::Filter::default()
    };
    runner
        .seed(&[
            Seed {
                bucket: bucket.clone(),
                rule: archive,
            },
            Seed {
                bucket: bucket.clone(),
                rule: expire,
            },
        ])
        .await;

    let summary = runner.pass().await;

    assert!(
        storage.calls().is_empty(),
        "a suspended bucket never transitions"
    );
    assert_eq!(summary.transitioned_objects, 0);
    assert_eq!(
        summary.expired_objects, 1,
        "the same bucket's expiration still runs"
    );
    assert!(
        storage.head_object(&bucket, &archive_key).await.is_ok(),
        "the transition's object stays where it was"
    );
    assert_eq!(
        storage
            .current_version(&bucket, &tmp_key)
            .await
            .expect("a current version"),
        CurrentVersion::Marker,
        "the expired key is marked"
    );
}

/// T11: expiration is evaluated first and wins when both are due, so the
/// object is deleted rather than moved.
#[tokio::test]
async fn expiration_wins_when_both_are_due() {
    let storage = Arc::new(TierDouble::new());
    let bucket = tier_bucket(&storage, "data", &["a.txt"]).await;
    let key = object::key("a.txt").expect("an object key");
    let runner = tier_runner(&storage, 1);
    let mut both = transitions(30);
    both.expiration = Some(lifecycle::Expiration {
        days: Some(1),
        date: None,
        expired_object_delete_marker: None,
    });
    runner
        .seed(&[Seed {
            bucket: bucket.clone(),
            rule: both,
        }])
        .await;

    let summary = runner.pass().await;

    assert_eq!(summary.expired_objects, 1, "expiration is evaluated first");
    assert_eq!(
        summary.transitioned_objects, 0,
        "the object was expired, not moved"
    );
    assert!(
        storage.calls().is_empty(),
        "a due expiration leaves nothing for the transition"
    );
    assert!(
        storage.head_object(&bucket, &key).await.is_err(),
        "the object is gone"
    );
}

/// T11: a delete marker holds no content, so a listing that names a marker's
/// key is never transitioned — the guard reads the current slot, not the
/// listing.
#[tokio::test]
async fn a_delete_marker_is_never_transitioned() {
    let storage = Arc::new(TierDouble::new());
    let bucket = tier_bucket(&storage, "data", &["a.txt"]).await;
    let key = object::key("a.txt").expect("an object key");
    storage.mark(&key);
    let runner = tier_runner(&storage, 1);
    runner
        .seed(&[Seed {
            bucket,
            rule: transitions(30),
        }])
        .await;

    let summary = runner.pass().await;

    assert_eq!(
        summary.transitioned_objects, 0,
        "a marker has nothing to move"
    );
    assert_eq!(summary.errors, 0, "a skipped marker is not an error");
    assert!(
        storage.calls().is_empty(),
        "the marker's key is never transitioned"
    );
}

/// T14: `Options.concurrency` drives the walk. Both moves are inside the
/// backend's `transition` at once, so only a pass that runs two workers can
/// pass the double's barrier.
#[tokio::test]
async fn transitions_run_with_two_workers() {
    let barrier = Arc::new(Barrier::new(2));
    let storage = Arc::new(TierDouble::new().with_barrier(Arc::clone(&barrier)));
    let bucket = tier_bucket(&storage, "data", &["a.txt", "b.txt"]).await;
    let runner = tier_runner(&storage, 2);
    runner
        .seed(&[Seed {
            bucket,
            rule: transitions(30),
        }])
        .await;

    let summary = timeout(Duration::from_secs(10), runner.pass())
        .await
        .expect("both moves reach the barrier together");

    assert_eq!(summary.transitioned_objects, 2, "both due objects moved");
    assert_eq!(
        storage.max_in_flight(),
        2,
        "two workers ran two transitions at once"
    );
    assert_eq!(storage.calls().len(), 2);
}

/// The action family's contract covers a transition as it covers every other
/// action: a completed move and an object already gone both record `ok`, and a
/// store refusal records `error`, so the two series stay paired.
#[tokio::test]
async fn a_transition_records_its_outcome() {
    let _guard = test_lock::window();
    let storage = Arc::new(TierDouble::new());
    let moved = tier_bucket(&storage, "moved", &["a.txt"]).await;
    let gone = tier_bucket(&storage, "gone", &["b.txt"]).await;
    let broken = tier_bucket(&storage, "broken", &["c.txt"]).await;
    storage.vanish(&object::key("b.txt").expect("an object key"));
    storage.fail(&object::key("c.txt").expect("an object key"));
    let runner = tier_runner(&storage, 1);
    for bucket in [moved, gone, broken] {
        runner
            .seed(&[Seed {
                bucket,
                rule: transitions(30),
            }])
            .await;
    }
    let before_ok = action_count(Kind::Object, "ok");
    let before_error = action_count(Kind::Object, "error");

    let summary = runner.pass().await;

    assert_eq!(summary.transitioned_objects, 1, "one move completed");
    assert_eq!(summary.errors, 1, "one store refusal is counted");
    assert_eq!(
        action_count(Kind::Object, "ok") - before_ok,
        2.0,
        "a completed move and an object already gone both record ok"
    );
    assert_eq!(
        action_count(Kind::Object, "error") - before_error,
        1.0,
        "the store's refusal records the only error"
    );
}
