//! The in-crate contract double the engine's own tests run against: rows in a
//! map, content in a tempdir, an in-memory block store.
//!
//! The engine runs on the persistence and content contracts, so the double
//! implements them. [`VersionMeta`], [`CurrentRows`] and the content plane are
//! real — they are what the engine exercises — while the metadata categories'
//! other calls are `unimplemented!()`: no engine path reaches them, and the
//! contracts' own conformance suite runs against real backends, never this
//! double. [`Fake::write_content`] is the write path's shape in one call: the
//! bytes **and** the object row that describes them, because the engine reads
//! that row for the outgoing version's metadata wires.

// **A test context that states its own exemption**: `lib.rs` declares this file `#[cfg(test)]`,
// so the panic family is free by rule (design 2026-10-09 D1) and by `clippy.toml`, but
// `unimplemented` has no `allow-*-in-tests` key — the 57 contract calls no engine path reaches
// are exactly the sites that key would have covered (D7, docs/style.md §Panics).
#![allow(
    clippy::unimplemented,
    reason = "test context: `unimplemented` has no in-tests key (docs/style.md §Panics)"
)]
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::SystemTime,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use tinio_util::testing::{
    blocks::MemBlocks,
    metadata::{assert_object_lock_conformance, assert_object_lock_upload_conformance},
};
#[cfg(test)]
use tinio_util::testing::{bucket_name, object_key};
use tokio::fs;

use crate::{
    _blob::Store,
    _core::{
        BodyStream, ByteRange, ETag, MultipartUpload, acl,
        bucket::{self, Name},
        checksum,
        content::{ContentEntry, ContentKey, ObjectContent, StagedObject},
        cors, lifecycle,
        meta::{
            BucketMeta, Metadata, ObjectMeta, UploadMeta, VersionMeta, bucket as bucket_row,
            object as object_row, upload as upload_row,
            version::{
                Current as RowCurrent, ExpireReport, Expired as ExpireVerdict, Kind, Stored,
            },
        },
        multipart, object, object_lock, storage,
        version::{self, State},
    },
    Versions,
    ports::{Current, CurrentRows},
};

/// The engine over the double, plus the handles the tests probe.
pub(crate) struct Fixture {
    /// The content root's owner — dropping it removes the content, so the
    /// fixture holds it for the test's whole life.
    pub(crate) _dir: tempfile::TempDir,
    /// The contracts, for planting slots and reading content.
    pub(crate) ports: Fake,
    /// The engine under test.
    pub(crate) versions: Versions<Fake, Fake, MemBlocks>,
}

/// Build a fixture: a fresh tempdir, its contracts, and the engine over the
/// in-memory block store.
pub(crate) fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let ports = Fake::new(dir.path());
    let versions = Versions::rows_as_gate(
        ports.clone(),
        ports.clone(),
        Store::new(MemBlocks::default()),
    );
    Fixture {
        _dir: dir,
        ports,
        versions,
    }
}

/// A bucket name for the tests.
pub(crate) fn bucket() -> Name {
    bucket_name("demo")
}

/// A key for the tests.
pub(crate) fn key() -> object::Key {
    object_key("notes.txt")
}

/// The contract double: every call the engine makes, over maps and one tempdir.
#[derive(Clone, Default)]
pub(crate) struct Fake {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    /// The bucket versioning states, defaulting to unversioned.
    states: Mutex<HashMap<String, State>>,
    /// The keys' current slots, in the port's own shape (it carries the
    /// sequence and the degraded bit the contract's read deliberately drops).
    slots: Mutex<HashMap<(String, String), Current>>,
    /// The version rows, keyed as the table is.
    rows: Mutex<BTreeMap<(String, String, u64), Stored>>,
    /// The version index, keyed as the table is: `(bucket, key, id wire)` →
    /// `seq`. The contract keeps it in step with the rows, so the double must
    /// too — a test that expires a version can then see the entry go.
    index: Mutex<BTreeMap<(String, String, String), u64>>,
    /// The object rows the engine reads a version's metadata wires from.
    objects: Mutex<HashMap<(String, String), object_row::Row>>,
    /// The bucket lifecycle configurations, keyed by bucket name.
    lifecycle: Mutex<BTreeMap<String, lifecycle::Config>>,
    /// The bucket names that have a row — the double keeps no elements, but
    /// the lock configuration's presence outcome needs the row's existence.
    buckets: Mutex<HashSet<String>>,
    /// The bucket Object Lock configurations, keyed by bucket name.
    configs: Mutex<BTreeMap<String, object_lock::Config>>,
    /// The per-version locks, keyed as the lock table is:
    /// `(bucket, key, version-id wire)` (`wire_of(None)` is the literal
    /// `null`).
    locks: Mutex<BTreeMap<(String, String, String), object_lock::Lock>>,
    /// The multipart lock intents, keyed by `(bucket, upload_id)`.
    upload_locks: Mutex<BTreeMap<(String, String), object_lock::Lock>>,
    /// The upload rows written through `create_with_lock` — the row half the
    /// lock intent is atomic with.
    uploads: Mutex<BTreeMap<(String, String), upload_row::Row>>,
    /// The content root; the double keeps its bytes in real files under it, so
    /// a test can read back what a publish landed.
    root: PathBuf,
    /// The slots to install on the next content reads, one per read — the
    /// out-of-band write landing between the resolve and the read. A single
    /// slot is one move; a queue makes the slot keep moving.
    swap_on_open: Mutex<VecDeque<Current>>,
    /// How many batch removals the engine has asked for.
    batch_removals: Mutex<usize>,
}

impl Fake {
    /// A double whose content lives under `root`.
    pub(crate) fn new(root: &Path) -> Self {
        Self {
            inner: Arc::new(Inner {
                root: root.to_path_buf(),
                ..Inner::default()
            }),
        }
    }

    /// Where `key`'s content lives (`<root>/<bucket>/<key>`).
    fn content_path(&self, bucket: &Name, key: &object::Key) -> PathBuf {
        self.inner
            .root
            .join(bucket.as_ref())
            .join(key.as_ref().as_str())
    }

    /// Plant the key's current slot.
    pub(crate) fn set_slot(&self, bucket: &Name, key: &object::Key, slot: Current) {
        self.inner
            .slots
            .lock()
            .unwrap()
            .insert((bucket.to_string(), key.to_string()), slot);
    }

    /// Install `slot` when the next content read runs — the change under the
    /// read the race test needs.
    pub(crate) fn swap_slot_on_open(&self, slot: Current) {
        self.inner.swap_on_open.lock().unwrap().push_back(slot);
    }

    /// Install each of `slots` on the next reads, in order — a double whose
    /// slot moves on every read, so the retry races too.
    pub(crate) fn swap_slots_on_open(&self, slots: impl IntoIterator<Item = Current>) {
        self.inner.swap_on_open.lock().unwrap().extend(slots);
    }

    /// The write path's shape: the bytes land, and the object row describing
    /// them is recorded (the engine reads that row for the outgoing version's
    /// metadata wires — the recorded ETag included).
    pub(crate) async fn write_content(&self, bucket: &Name, key: &object::Key, bytes: &[u8]) {
        let path = self.content_path(bucket, key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.unwrap();
        }
        fs::write(path, bytes).await.unwrap();
        self.set_object_row(bucket, key, ETag::from_content(bytes), bytes.len() as u64);
    }

    /// Plant the object row the engine reads a version's metadata wires from.
    pub(crate) fn set_object_row(&self, bucket: &Name, key: &object::Key, etag: ETag, size: u64) {
        self.inner.objects.lock().unwrap().insert(
            (bucket.to_string(), key.to_string()),
            object_row::Row {
                etag,
                size,
                mtime: SystemTime::now(),
                identity: 0,
                tags: object::Tags::empty(),
                checksum: None,
                owner: None,
                acl: acl::Acl::default_private(None),
            },
        );
    }

    /// Read `key`'s content back.
    pub(crate) async fn read_content(&self, bucket: &Name, key: &object::Key) -> Option<Vec<u8>> {
        fs::read(self.content_path(bucket, key)).await.ok()
    }

    /// How many version rows the key holds.
    pub(crate) fn row_count(&self, bucket: &Name, key: &object::Key) -> usize {
        self.inner
            .rows
            .lock()
            .unwrap()
            .keys()
            .filter(|(b, k, _)| b == bucket.as_ref() && k == key.as_ref())
            .count()
    }

    /// How many batch removals the engine has asked this double for — the
    /// batched path's own probe.
    pub(crate) fn batch_removals(&self) -> usize {
        *self.inner.batch_removals.lock().unwrap()
    }

    /// The row at one sequence of one key.
    fn row_at(&self, bucket: &Name, key: &object::Key, seq: u64) -> Option<Stored> {
        self.inner
            .rows
            .lock()
            .unwrap()
            .get(&(bucket.to_string(), key.to_string(), seq))
            .cloned()
    }

    /// Drop one row and its index entry (the one removal body both expiry
    /// calls run).
    fn drop_row(&self, bucket: &Name, key: &object::Key, seq: u64) {
        let removed =
            self.inner
                .rows
                .lock()
                .unwrap()
                .remove(&(bucket.to_string(), key.to_string(), seq));
        if let Some(row) = removed {
            self.inner.index.lock().unwrap().remove(&(
                bucket.to_string(),
                key.to_string(),
                version::wire_of(row.id.as_ref()),
            ));
        }
    }

    /// The sequence the index names for `id` (`None` = no entry) — the
    /// test's window onto the contract's index half.
    pub(crate) fn index_seq(&self, bucket: &Name, key: &object::Key, id: &str) -> Option<u64> {
        self.inner
            .index
            .lock()
            .unwrap()
            .get(&(bucket.to_string(), key.to_string(), id.to_owned()))
            .copied()
    }

    fn content_slot(&self, bucket: &Name, key: &object::Key) -> Current {
        self.inner
            .slots
            .lock()
            .unwrap()
            .get(&(bucket.to_string(), key.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    /// The identity a removal at `seq` addresses: the row's own, or — when no
    /// row exists — the current slot's, provided the slot records this
    /// sequence. `None` is nothing to remove.
    fn identity_at(
        &self,
        bucket: &Name,
        key: &object::Key,
        seq: u64,
    ) -> Option<Option<version::Id>> {
        if let Some(row) = self.row_at(bucket, key, seq) {
            return Some(row.id);
        }
        let slot = self.content_slot(bucket, key);
        (slot.exists && slot.seq == seq).then_some(slot.version)
    }

    /// The lock table's key for an identity: the version-id wire, whose `None`
    /// spelling is the literal `null`.
    fn lock_key(
        bucket: &Name,
        key: &object::Key,
        id: Option<&version::Id>,
    ) -> (String, String, String) {
        (bucket.to_string(), key.to_string(), version::wire_of(id))
    }

    /// The stored lock for an identity, or `None` (the double's map holds the
    /// decoded value, so there is no corrupt row to fail closed on).
    fn stored_lock(
        &self,
        bucket: &Name,
        key: &object::Key,
        id: Option<&version::Id>,
    ) -> Option<object_lock::Lock> {
        self.inner
            .locks
            .lock()
            .unwrap()
            .get(&Self::lock_key(bucket, key, id))
            .cloned()
    }

    /// Whether the identity a lock write addresses exists: the key's current
    /// slot names it and is not a marker, or a `Kind::Object` version row
    /// carries it. A delete marker and an absent key are refused (OL5).
    fn identity_exists(&self, bucket: &Name, key: &object::Key, id: Option<&version::Id>) -> bool {
        let slot = self.content_slot(bucket, key);
        if slot.exists && !slot.is_delete_marker && slot.version.as_ref() == id {
            return true;
        }
        let (bucket, key) = (bucket.as_ref(), key.as_ref());
        self.inner
            .rows
            .lock()
            .unwrap()
            .iter()
            .any(|((row_bucket, row_key, _), row)| {
                row_bucket == bucket
                    && row_key == key
                    && row.kind == Kind::Object
                    && row.id.as_ref() == id
            })
    }

    /// The request path's two preconditions, in one place (OL5, OL13): the
    /// bucket's configuration is enabled and its versioning state is
    /// `Enabled`. Both refusals are [`storage::Error::InvalidBucketState`].
    fn lock_writes_allowed(&self, bucket: &Name) -> Result<(), storage::Error> {
        let enabled = self
            .inner
            .configs
            .lock()
            .unwrap()
            .get(bucket.as_ref())
            .is_some_and(|config| config.enabled);
        if !enabled {
            return Err(storage::invalid_bucket_state(bucket));
        }
        let state = self
            .inner
            .states
            .lock()
            .unwrap()
            .get(bucket.as_ref())
            .copied()
            .unwrap_or_default();
        if state != State::Enabled {
            return Err(storage::invalid_bucket_state(bucket));
        }
        Ok(())
    }
}

#[async_trait]
#[expect(
    unused_variables,
    reason = "this category's calls are all `unimplemented!` in the double, so their parameters go unread"
)]
impl BucketMeta for Fake {
    async fn created_at(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Option<SystemTime>, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn load_many(
        &self,
        buckets: &[bucket::Name],
    ) -> Result<Vec<Option<SystemTime>>, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn load_all(&self) -> Result<Vec<(String, SystemTime)>, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn row(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Option<bucket_row::Row>, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn owners(
        &self,
        buckets: &[bucket::Name],
    ) -> Result<Vec<Option<acl::OwnerId>>, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn record(
        &self,
        bucket: &bucket::Name,
        created: SystemTime,
    ) -> Result<(), <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn create(
        &self,
        bucket: &bucket::Name,
        row: &bucket_row::Row,
    ) -> Result<(), <Self as Metadata>::Error> {
        // The double keeps only the row's existence: the lock configuration's
        // presence outcome is the one element the contract suite reads.
        let _ = row;
        self.inner
            .buckets
            .lock()
            .unwrap()
            .insert(bucket.to_string());
        Ok(())
    }

    async fn record_if_absent(
        &self,
        bucket: &bucket::Name,
        now: SystemTime,
    ) -> Result<(SystemTime, bool), <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn record_full(
        &self,
        bucket: &bucket::Name,
        row: &bucket_row::Row,
    ) -> Result<(), <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn set_tags(
        &self,
        bucket: &bucket::Name,
        tags: &object::Tags,
    ) -> Result<bool, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn set_tags_or_create(
        &self,
        bucket: &bucket::Name,
        tags: &object::Tags,
        now: SystemTime,
    ) -> Result<(), <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn clear_tags(&self, bucket: &bucket::Name) -> Result<bool, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn set_acl(
        &self,
        bucket: &bucket::Name,
        owner: Option<&acl::OwnerId>,
        grants: &acl::AclGrants,
    ) -> Result<bool, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn set_grants_or_create(
        &self,
        bucket: &bucket::Name,
        grants: &acl::AclGrants,
        now: SystemTime,
    ) -> Result<(), <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn set_cors(
        &self,
        bucket: &bucket::Name,
        config: &cors::Config,
    ) -> Result<bool, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn clear_cors(&self, bucket: &bucket::Name) -> Result<bool, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn lifecycle(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Option<lifecycle::Config>, <Self as Metadata>::Error> {
        Ok(self
            .inner
            .lifecycle
            .lock()
            .unwrap()
            .get(bucket.as_ref())
            .cloned())
    }

    async fn lifecycles(
        &self,
    ) -> Result<Vec<(bucket::Name, lifecycle::Config)>, <Self as Metadata>::Error> {
        Ok(self
            .inner
            .lifecycle
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(bucket, config)| {
                // Keys are written from `&bucket::Name`, so an unparsable one
                // cannot occur in the double; the scan still refuses to invent
                // a bucket for it.
                bucket::name(bucket.clone())
                    .ok()
                    .map(|name| (name, config.clone()))
            })
            .collect())
    }

    /// The double keeps no bucket rows, so every name it is asked about is a
    /// bucket: both setters answer the presence `true`, never a change.
    async fn set_lifecycle(
        &self,
        bucket: &bucket::Name,
        config: &lifecycle::Config,
    ) -> Result<bool, <Self as Metadata>::Error> {
        self.set_lifecycle_or_create(bucket, config, SystemTime::now())
            .await?;
        Ok(true)
    }

    async fn set_lifecycle_or_create(
        &self,
        bucket: &bucket::Name,
        config: &lifecycle::Config,
        _now: SystemTime,
    ) -> Result<(), <Self as Metadata>::Error> {
        // The double keeps no bucket rows: every name it is asked about is a
        // bucket, so the or-create form is the plain write.
        self.inner
            .lifecycle
            .lock()
            .unwrap()
            .insert(bucket.to_string(), config.clone());
        Ok(())
    }

    async fn merge_lifecycle(
        &self,
        bucket: &bucket::Name,
        rules: &[lifecycle::Rule],
    ) -> Result<lifecycle::Merged, <Self as Metadata>::Error> {
        self.merge_lifecycle_or_create(bucket, rules, SystemTime::now())
            .await
    }

    async fn merge_lifecycle_or_create(
        &self,
        bucket: &bucket::Name,
        rules: &[lifecycle::Rule],
        _now: SystemTime,
    ) -> Result<lifecycle::Merged, <Self as Metadata>::Error> {
        let mut rows = self.inner.lifecycle.lock().unwrap();
        let current = rows
            .get(bucket.as_ref())
            .cloned()
            .unwrap_or(lifecycle::Config { rules: Vec::new() });
        match current.apply_rules(rules) {
            Ok(config) => {
                rows.insert(bucket.to_string(), config);
                Ok(lifecycle::Merged::Stored)
            }
            Err(invalid) => Ok(lifecycle::Merged::Refused(invalid)),
        }
    }

    async fn clear_lifecycle(
        &self,
        bucket: &bucket::Name,
    ) -> Result<bool, <Self as Metadata>::Error> {
        self.inner.lifecycle.lock().unwrap().remove(bucket.as_ref());
        Ok(true)
    }

    async fn object_lock(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Option<object_lock::Config>, <Self as Metadata>::Error> {
        if !self.inner.buckets.lock().unwrap().contains(bucket.as_ref()) {
            return Err(storage::no_such_bucket(bucket));
        }
        Ok(self
            .inner
            .configs
            .lock()
            .unwrap()
            .get(bucket.as_ref())
            .cloned())
    }

    async fn set_object_lock(
        &self,
        bucket: &bucket::Name,
        config: &object_lock::Config,
    ) -> Result<bool, <Self as Metadata>::Error> {
        if !self.inner.buckets.lock().unwrap().contains(bucket.as_ref()) {
            return Ok(false);
        }
        self.inner
            .configs
            .lock()
            .unwrap()
            .insert(bucket.to_string(), config.clone());
        Ok(true)
    }

    async fn clear_object_lock(
        &self,
        bucket: &bucket::Name,
    ) -> Result<bool, <Self as Metadata>::Error> {
        if !self.inner.buckets.lock().unwrap().contains(bucket.as_ref()) {
            return Ok(false);
        }
        self.inner.configs.lock().unwrap().remove(bucket.as_ref());
        Ok(true)
    }

    async fn remove(&self, bucket: &bucket::Name) -> Result<(), <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn remove_if_empty(
        &self,
        bucket: &bucket::Name,
    ) -> Result<(), <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }
}

#[async_trait]
#[expect(
    unused_variables,
    reason = "the calls no engine path reaches are `unimplemented!` in the double, so their parameters go unread"
)]
impl ObjectMeta for Fake {
    async fn get(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<object_row::Row>, <Self as Metadata>::Error> {
        Ok(self
            .inner
            .objects
            .lock()
            .unwrap()
            .get(&(bucket.to_string(), key.to_string()))
            .cloned())
    }

    async fn put(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &object_row::Row,
    ) -> Result<(), <Self as Metadata>::Error> {
        self.set_object_row(bucket, key, row.etag.clone(), row.size);
        Ok(())
    }

    async fn put_guarded(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &object_row::Row,
    ) -> Result<(), <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn put_with_parts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &object_row::Row,
        parts: &[object_row::PartRow],
    ) -> Result<(), <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn parts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Vec<object_row::PartRow>, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn has_objects(&self, bucket: &bucket::Name) -> Result<bool, <Self as Metadata>::Error> {
        // The double's row map is its whole object plane, so the teardown
        // probe is answerable here rather than `unimplemented!`: the double
        // stores no delete-marker bit, so a recorded row is what it counts.
        let name = bucket.to_string();
        Ok(self
            .inner
            .objects
            .lock()
            .unwrap()
            .keys()
            .any(|(b, _)| *b == name))
    }

    async fn put_preserving(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        identity: &object_row::Identity,
    ) -> Result<object_row::Preserved, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn move_(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn set_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<bool, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn clear_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<bool, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn ensure_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<(), <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn set_acl(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        owner: Option<&acl::OwnerId>,
        grants: &acl::AclGrants,
    ) -> Result<bool, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn put_batch(
        &self,
        bucket: &bucket::Name,
        rows: &[(object::Key, object_row::Row)],
    ) -> Result<(), <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn remove(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<bool, <Self as Metadata>::Error> {
        Ok(self
            .inner
            .objects
            .lock()
            .unwrap()
            .remove(&(bucket.to_string(), key.to_string()))
            .is_some())
    }

    async fn remove_many(
        &self,
        bucket: &bucket::Name,
        keys: &[object::Key],
    ) -> Result<(), <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn load(
        &self,
        bucket: &bucket::Name,
        keys: &[object::Key],
    ) -> Result<Vec<Option<object_row::Row>>, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn walk(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Vec<(object::Key, object_row::Row)>, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn walk_healing(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Vec<(object::Key, Option<object_row::Row>)>, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }
}

#[async_trait]
#[expect(
    unused_variables,
    reason = "this category's calls are all `unimplemented!` in the double, so their parameters go unread"
)]
impl UploadMeta for Fake {
    async fn create(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        row: &upload_row::Row,
    ) -> Result<(), <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    /// The multipart intent is real: Task 5b's engine tests assert that the
    /// effective lock is fixed at create and that abort/complete clear it.
    async fn create_with_lock(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        row: &upload_row::Row,
        lock: Option<&object_lock::Lock>,
    ) -> Result<(), <Self as Metadata>::Error> {
        let _ = row;
        let at = (bucket.to_string(), upload_id.to_owned());
        // The row and the intent land together (the double's one map write
        // each: there is no transaction to lose one half of).
        self.inner
            .uploads
            .lock()
            .unwrap()
            .insert(at.clone(), row.clone());
        let mut locks = self.inner.upload_locks.lock().unwrap();
        match lock {
            Some(lock) => {
                locks.insert(at, lock.clone());
            }
            None => {
                locks.remove(&at);
            }
        }
        Ok(())
    }

    async fn upload_lock(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
    ) -> Result<Option<object_lock::Lock>, <Self as Metadata>::Error> {
        Ok(self
            .inner
            .upload_locks
            .lock()
            .unwrap()
            .get(&(bucket.to_string(), upload_id.to_owned()))
            .cloned())
    }

    async fn clear_upload_lock(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
    ) -> Result<(), <Self as Metadata>::Error> {
        self.inner
            .upload_locks
            .lock()
            .unwrap()
            .remove(&(bucket.to_string(), upload_id.to_owned()));
        Ok(())
    }

    async fn get(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<Option<upload_row::Row>, <Self as Metadata>::Error> {
        Ok(self
            .inner
            .uploads
            .lock()
            .unwrap()
            .get(&(bucket.to_string(), upload_id.to_owned()))
            .filter(|row| row.key == *key)
            .cloned())
    }

    async fn put_part(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        number: multipart::PartNumber,
        etag: &ETag,
        checksum: Option<&checksum::PartChecksum>,
    ) -> Result<(), <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn list_parts(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        from: multipart::PartNumber,
    ) -> Result<Vec<upload_row::PartRow>, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn list_parts_page(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        from: u32,
        max: usize,
    ) -> Result<upload_row::PartsPage, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn part_records(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        numbers: &[multipart::PartNumber],
    ) -> Result<Option<Vec<Option<upload_row::PartRecord>>>, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn list_uploads(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Vec<MultipartUpload>, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn list_uploads_page(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<&str>,
        max_uploads: usize,
    ) -> Result<(Vec<MultipartUpload>, Vec<String>, bool, Option<String>), <Self as Metadata>::Error>
    {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn has_uploads(&self, bucket: &bucket::Name) -> Result<bool, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn walk_uploads(&self) -> Result<Vec<MultipartUpload>, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn drain_upload(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
    ) -> Result<(), <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn drain_upload_checked(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<(), <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }
}

#[async_trait]
#[expect(
    unused_variables,
    reason = "the calls no engine path reaches are `unimplemented!` in the double, so their parameters go unread"
)]
impl ObjectContent for Fake {
    type Error = storage::Error;
    type Staged = FakeStaged;

    async fn stage(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
    ) -> Result<Self::Staged, Self::Error> {
        Ok(FakeStaged::default())
    }

    async fn publish(
        &self,
        bucket: &bucket::Name,
        staged: Self::Staged,
        key: &ContentKey,
    ) -> Result<(), Self::Error> {
        let ContentKey::Object(key) = key else {
            unimplemented!("the double publishes object bytes only")
        };
        let path = self.content_path(bucket, key);
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent).await;
        }
        // The visibility point, and nothing else: the contract writes no row
        // here — the caller's metadata write is its own, after this returns.
        fs::write(&path, &staged.bytes).await.map_err(storage::io)?;
        Ok(())
    }

    async fn open(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        range: Option<ByteRange>,
    ) -> Result<BodyStream, Self::Error> {
        // The out-of-band write lands here, between the resolve and the read.
        if let Some(slot) = self.inner.swap_on_open.lock().unwrap().pop_front()
            && let ContentKey::Object(key) = key
        {
            self.set_slot(bucket, key, slot);
        }
        let ContentKey::Object(key) = key else {
            unimplemented!("the double opens object bytes only")
        };
        match fs::read(self.content_path(bucket, key)).await {
            Ok(bytes) => Ok(Box::pin(stream::once(
                async move { Ok(Bytes::from(bytes)) },
            ))),
            // The contract's absent-object answer (D23's pinned return has no
            // `None` arm): the engine's gone-content branch reads exactly this.
            Err(err) if err.kind() == io::ErrorKind::NotFound => Err(storage::no_such_key(key)),
            Err(err) => Err(storage::io(err)),
        }
    }

    async fn remove(&self, bucket: &bucket::Name, key: &ContentKey) -> Result<(), Self::Error> {
        let ContentKey::Object(key) = key else {
            unimplemented!("the double removes object bytes only")
        };
        match fs::remove_file(self.content_path(bucket, key)).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(storage::io(err)),
        }
    }

    async fn rename(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Self::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn copy(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Self::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn scan(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        after: Option<&object::Key>,
        limit: usize,
    ) -> Result<Vec<ContentEntry>, Self::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn stat(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<ContentEntry>, Self::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn create_bucket(&self, bucket: &bucket::Name) -> Result<(), Self::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn remove_bucket(&self, bucket: &bucket::Name) -> Result<(), Self::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn is_empty(&self, bucket: &bucket::Name) -> Result<bool, Self::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }
}

/// The double's staged object: bytes held in memory until the publish.
#[derive(Debug, Default)]
pub(crate) struct FakeStaged {
    /// The bytes the caller streamed into the stage.
    bytes: Vec<u8>,
}

#[async_trait]
impl StagedObject for FakeStaged {
    type Error = storage::Error;

    async fn write(&mut self, chunk: Bytes) -> Result<(), storage::Error> {
        self.bytes.extend_from_slice(&chunk);
        Ok(())
    }
}

/// The marker the metadata aggregate needs: this backend's row error, declared
/// once.
impl Metadata for Fake {
    type Error = storage::Error;
}

#[async_trait]
impl VersionMeta for Fake {
    type Error = storage::Error;

    async fn bucket_state(&self, bucket: &bucket::Name) -> Result<State, storage::Error> {
        Ok(self
            .inner
            .states
            .lock()
            .unwrap()
            .get(bucket.as_ref())
            .copied()
            .unwrap_or_default())
    }

    async fn set_bucket_state(
        &self,
        bucket: &bucket::Name,
        state: State,
    ) -> Result<(), storage::Error> {
        self.inner
            .states
            .lock()
            .unwrap()
            .insert(bucket.to_string(), state);
        Ok(())
    }

    async fn current_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<RowCurrent>, storage::Error> {
        let slot = self.content_slot(bucket, key);
        Ok(slot.exists.then(|| RowCurrent {
            exists: true,
            version: slot.version.clone(),
            is_delete_marker: slot.is_delete_marker,
        }))
    }

    async fn advance_slot_with(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<version::Id>,
        seq: u64,
        lock: Option<&object_lock::Lock>,
    ) -> Result<(), storage::Error> {
        // The slot is a version-row fact: the marker bit comes from the row at
        // the slot's sequence, never from the caller.
        let is_delete_marker = self
            .row_at(bucket, key, seq)
            .is_some_and(|row| row.kind.is_marker());
        self.set_slot(
            bucket,
            key,
            Current {
                exists: true,
                version: id.clone(),
                is_delete_marker,
                seq,
                degraded: false,
            },
        );
        // `Some` commits the lock with the slot move; `None` never clears one
        // (OL7): only the removal primitives do.
        if let Some(lock) = lock {
            self.inner
                .locks
                .lock()
                .unwrap()
                .insert(Self::lock_key(bucket, key, id.as_ref()), lock.clone());
        }
        Ok(())
    }

    /// The restore's commit over the double's own tables: the row at `seq` is
    /// the premise (it must exist and name `id`, and its kind is the slot's
    /// marker bit — never the caller's); the consumed row and its index entry
    /// go together; the caller's object row is recorded; and the slot names the
    /// restored version at its own sequence — a **backwards** move, so no guard
    /// applies. The double keeps no retained part list (`ObjectMeta::parts` is
    /// not a call the engine makes), so S3's clear has nothing to clear here.
    async fn restore_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &object_row::Row,
        id: Option<version::Id>,
        seq: u64,
    ) -> Result<(), storage::Error> {
        let Some(consumed) = self.row_at(bucket, key, seq) else {
            return Err(storage::io(io::Error::other(
                "a restore names a version row that is absent",
            )));
        };
        if version::wire_of(consumed.id.as_ref()) != version::wire_of(id.as_ref()) {
            return Err(storage::io(io::Error::other(
                "a restore names a version row of another identity",
            )));
        }
        self.inner
            .objects
            .lock()
            .unwrap()
            .insert((bucket.to_string(), key.to_string()), row.clone());
        self.drop_row(bucket, key, seq);
        self.set_slot(
            bucket,
            key,
            Current {
                exists: true,
                version: id,
                is_delete_marker: consumed.kind.is_marker(),
                seq,
                degraded: false,
            },
        );
        Ok(())
    }

    async fn put_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &Stored,
    ) -> Result<(), storage::Error> {
        let wire = version::wire_of(row.id.as_ref());
        let (bucket_name, key_name) = (bucket.to_string(), key.to_string());
        let at = (bucket_name.clone(), key_name.clone(), row.seq);
        let mut rows = self.inner.rows.lock().unwrap();
        // The row-overwrite guard: the row at a sequence may only be rewritten
        // by the same version.
        if let Some(existing) = rows.get(&at)
            && (existing.id != row.id
                || existing.kind != row.kind
                || existing.blob != row.blob
                || existing.size != row.size)
        {
            return Err(storage::io(io::Error::other(
                "a version row conflicts with the version being superseded",
            )));
        }
        // The one-null-version rule: the null identity displaces the row the
        // index's single null entry names, with that entry.
        if row.id.is_none() {
            let stale = self
                .inner
                .index
                .lock()
                .unwrap()
                .get(&(bucket_name.clone(), key_name.clone(), wire.clone()))
                .copied();
            if let Some(stale) = stale
                && stale != row.seq
            {
                rows.remove(&(bucket_name.clone(), key_name.clone(), stale));
                self.inner.index.lock().unwrap().remove(&(
                    bucket_name.clone(),
                    key_name.clone(),
                    wire.clone(),
                ));
            }
        }
        // This double stamps on commit — the mem-side choice the contract
        // allows — and preserves a stamp the row already carries.
        let mut stored = row.clone();
        stored.became_noncurrent = rows
            .get(&at)
            .map_or(1, |existing| existing.became_noncurrent.max(1));
        rows.insert(at, stored);
        drop(rows);
        self.inner
            .index
            .lock()
            .unwrap()
            .insert((bucket_name, key_name, wire), row.seq);
        Ok(())
    }

    async fn get_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<Option<Stored>, storage::Error> {
        Ok(self.row_at(bucket, key, seq))
    }

    async fn latest(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<(u64, Stored)>, storage::Error> {
        Ok(self
            .inner
            .rows
            .lock()
            .unwrap()
            .range(
                (bucket.to_string(), key.to_string(), u64::MIN)
                    ..=(bucket.to_string(), key.to_string(), u64::MAX),
            )
            .next_back()
            .map(|(_, row)| (row.seq, row.clone())))
    }

    async fn bucket_rows(&self, bucket: &bucket::Name) -> Result<Vec<Stored>, storage::Error> {
        Ok(self
            .inner
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|((b, _, _), _)| b == bucket.as_ref())
            .map(|(_, row)| row.clone())
            .collect())
    }

    async fn list_versions(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Vec<Stored>, storage::Error> {
        let mut rows: Vec<Stored> = self
            .inner
            .rows
            .lock()
            .unwrap()
            .range(
                (bucket.to_string(), key.to_string(), u64::MIN)
                    ..=(bucket.to_string(), key.to_string(), u64::MAX),
            )
            .map(|(_, row)| row.clone())
            .collect();
        rows.reverse();
        Ok(rows)
    }

    async fn set_version_lock(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&version::Id>,
        lock: Option<&object_lock::Lock>,
    ) -> Result<(), storage::Error> {
        let at = Self::lock_key(bucket, key, version);
        let mut locks = self.inner.locks.lock().unwrap();
        match lock {
            Some(lock) => {
                locks.insert(at, lock.clone());
            }
            None => {
                locks.remove(&at);
            }
        }
        Ok(())
    }

    async fn version_lock(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&version::Id>,
    ) -> Result<Option<object_lock::Lock>, storage::Error> {
        Ok(self.stored_lock(bucket, key, version))
    }

    async fn get_object_retention(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&version::Id>,
    ) -> Result<Option<object_lock::Retention>, storage::Error> {
        Ok(self
            .stored_lock(bucket, key, version)
            .and_then(|lock| lock.retention))
    }

    async fn get_object_legal_hold(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&version::Id>,
    ) -> Result<object_lock::LegalHold, storage::Error> {
        Ok(self
            .stored_lock(bucket, key, version)
            .map_or(object_lock::LegalHold::Off, |lock| lock.legal_hold))
    }

    async fn set_retention(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&version::Id>,
        requested: Option<&object_lock::Retention>,
        bypass: object_lock::Bypass,
        now: SystemTime,
    ) -> Result<Option<object_lock::Lock>, storage::Error> {
        if !self.identity_exists(bucket, key, version) {
            return Err(storage::no_such_version(version::wire_of(version)));
        }
        self.lock_writes_allowed(bucket)?;
        let current = self.stored_lock(bucket, key, version);
        let updated = object_lock::apply_retention(current.as_ref(), requested, bypass, now)
            .map_err(storage::locked)?;
        let at = Self::lock_key(bucket, key, version);
        let mut locks = self.inner.locks.lock().unwrap();
        match &updated {
            Some(lock) => {
                locks.insert(at, lock.clone());
            }
            None => {
                locks.remove(&at);
            }
        }
        Ok(updated)
    }

    async fn set_legal_hold(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&version::Id>,
        hold: object_lock::LegalHold,
    ) -> Result<object_lock::Lock, storage::Error> {
        if !self.identity_exists(bucket, key, version) {
            return Err(storage::no_such_version(version::wire_of(version)));
        }
        self.lock_writes_allowed(bucket)?;
        let current = self.stored_lock(bucket, key, version);
        // The same rule the stores apply: the model decides whether a row
        // remains, and "no row" is the empty lock this contract answers.
        let updated = object_lock::apply_legal_hold(current.as_ref(), hold);
        let at = Self::lock_key(bucket, key, version);
        let mut locks = self.inner.locks.lock().unwrap();
        let lock = match updated {
            Some(lock) => {
                locks.insert(at, lock.clone());
                lock
            }
            None => {
                locks.remove(&at);
                object_lock::Lock::default()
            }
        };
        Ok(lock)
    }

    /// The combined update: the same identity and write-path assertions as
    /// the single-member writers, one model decision and one map write, so a
    /// refused retention leaves neither member applied.
    async fn set_lock(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&version::Id>,
        retention: Option<Option<&object_lock::Retention>>,
        legal_hold: Option<object_lock::LegalHold>,
        bypass: object_lock::Bypass,
        now: SystemTime,
    ) -> Result<Option<object_lock::Lock>, storage::Error> {
        if !self.identity_exists(bucket, key, version) {
            return Err(storage::no_such_version(version::wire_of(version)));
        }
        self.lock_writes_allowed(bucket)?;
        let current = self.stored_lock(bucket, key, version);
        let updated = object_lock::apply_lock(current.as_ref(), retention, legal_hold, bypass, now)
            .map_err(storage::locked)?;
        let at = Self::lock_key(bucket, key, version);
        let mut locks = self.inner.locks.lock().unwrap();
        match &updated {
            Some(lock) => {
                locks.insert(at, lock.clone());
            }
            None => {
                locks.remove(&at);
            }
        }
        Ok(updated)
    }

    async fn expire_with(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
        bypass: object_lock::Bypass,
    ) -> Result<ExpireVerdict, storage::Error> {
        let Some(identity) = self.identity_at(bucket, key, seq) else {
            return Ok(ExpireVerdict::Nothing);
        };
        let lock = self.stored_lock(bucket, key, identity.as_ref());
        if let Some(reason) = object_lock::delete_refusal(lock.as_ref(), SystemTime::now(), bypass)
        {
            return Err(storage::locked(reason));
        }
        self.drop_row(bucket, key, seq);
        self.inner
            .locks
            .lock()
            .unwrap()
            .remove(&Self::lock_key(bucket, key, identity.as_ref()));
        Ok(ExpireVerdict::Removed)
    }

    /// The batched call, counted so a test can see that the engine's batched
    /// path used it: the double has no transaction to save, so the override
    /// exists to record the call, not to be cheaper. Protected sequences are
    /// skipped and reported (OL8).
    async fn expire_many_report(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seqs: &[u64],
    ) -> Result<ExpireReport, storage::Error> {
        *self.inner.batch_removals.lock().unwrap() += 1;
        let mut report = ExpireReport::default();
        for seq in seqs {
            let had_row = self.row_at(bucket, key, *seq).is_some();
            let Some(identity) = self.identity_at(bucket, key, *seq) else {
                continue;
            };
            let lock = self.stored_lock(bucket, key, identity.as_ref());
            if let Some(reason) = object_lock::delete_refusal(
                lock.as_ref(),
                SystemTime::now(),
                object_lock::Bypass::None,
            ) {
                report.protected.push((*seq, reason));
            } else {
                self.drop_row(bucket, key, *seq);
                self.inner.locks.lock().unwrap().remove(&Self::lock_key(
                    bucket,
                    key,
                    identity.as_ref(),
                ));
                if had_row {
                    report.removed += 1;
                }
            }
        }
        Ok(report)
    }

    /// The bucket-wide drain (OL8): refuse, removing nothing, when any version
    /// row is protected; otherwise remove every row, its index entry and every
    /// lock row of the bucket — an orphan row included. Re-running is a no-op.
    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), storage::Error> {
        let name = bucket.to_string();
        let rows: Vec<(String, Stored)> = self
            .inner
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|((row_bucket, _, _), _)| row_bucket == &name)
            .map(|((_, key, _), row)| (key.clone(), row.clone()))
            .collect();
        for (key, row) in &rows {
            // R19: a marker owns no lock, so a marker-owned lock row is an
            // orphan — the drain removes it, it never refuses the drain.
            if row.kind.is_marker() {
                continue;
            }
            let lock = self
                .inner
                .locks
                .lock()
                .unwrap()
                .get(&(name.clone(), key.clone(), version::wire_of(row.id.as_ref())))
                .cloned();
            if let Some(reason) = object_lock::delete_refusal(
                lock.as_ref(),
                SystemTime::now(),
                object_lock::Bypass::None,
            ) {
                return Err(storage::locked(reason));
            }
        }
        // The row-less current version (OL7/OL8): a lock advanced with the
        // slot and with no version row of its own is a protected version, not
        // an orphan, so the drain refuses it too.
        let slots: Vec<(String, Current)> = self
            .inner
            .slots
            .lock()
            .unwrap()
            .iter()
            .filter(|((slot_bucket, _), _)| slot_bucket == &name)
            .map(|((_, key), slot)| (key.clone(), slot.clone()))
            .collect();
        for (key, slot) in &slots {
            if !slot.exists || slot.is_delete_marker {
                continue;
            }
            let wire = version::wire_of(slot.version.as_ref());
            let lock = self
                .inner
                .locks
                .lock()
                .unwrap()
                .get(&(name.clone(), key.clone(), wire))
                .cloned();
            if let Some(reason) = object_lock::delete_refusal(
                lock.as_ref(),
                SystemTime::now(),
                object_lock::Bypass::None,
            ) {
                return Err(storage::locked(reason));
            }
        }
        self.inner
            .rows
            .lock()
            .unwrap()
            .retain(|(row_bucket, _, _), _| row_bucket != &name);
        self.inner
            .index
            .lock()
            .unwrap()
            .retain(|(index_bucket, _, _), _| index_bucket != &name);
        self.inner
            .locks
            .lock()
            .unwrap()
            .retain(|(lock_bucket, _, _), _| lock_bucket != &name);
        Ok(())
    }

    async fn version_seq_of(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<&version::Id>,
    ) -> Result<Option<u64>, storage::Error> {
        Ok(self
            .inner
            .index
            .lock()
            .unwrap()
            .get(&(bucket.to_string(), key.to_string(), version::wire_of(id)))
            .copied())
    }
}

#[async_trait]
impl CurrentRows for Fake {
    async fn current(&self, bucket: &Name, key: &object::Key) -> Result<Current, storage::Error> {
        Ok(self.content_slot(bucket, key))
    }
}

/// The identity and sequence a version-creating write would record for the
/// outgoing version, as Task 4's write branch plants it.
pub(crate) fn recorded(id: &str, seq: u64) -> Current {
    Current {
        exists: true,
        version: Some(version::Id::new(id).unwrap()),
        is_delete_marker: false,
        seq,
        degraded: false,
    }
}

/// The shared Object Lock legs over the double: the engine tests' store must
/// satisfy the same contract the real stores do (Task 2's green witness while
/// the redb/pg tables land in Tasks 3/4).
#[tokio::test]
async fn the_shared_object_lock_legs_hold_over_the_double() {
    let dir = tempfile::tempdir().unwrap();
    let store = Fake::new(dir.path());
    assert_object_lock_conformance(&store).await;
    assert_object_lock_upload_conformance(&store).await;
}
