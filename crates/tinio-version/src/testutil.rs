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

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::SystemTime,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use tokio::fs;

use crate::{
    _blob::Store,
    _core::{
        BodyStream, ByteRange, ETag, MultipartUpload, acl,
        blob::{BlockHash, BlockStore, Entry, EntryStream, Published, StagedBlock},
        bucket::{self, Name},
        checksum,
        content::{ContentEntry, ContentKey, ObjectContent, StagedObject},
        cors,
        meta::{
            BucketMeta, Metadata, ObjectMeta, UploadMeta, VersionMeta, bucket as bucket_row,
            object as object_row, upload as upload_row,
            version::{Current as RowCurrent, Stored},
        },
        multipart, object, storage,
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
    let versions = Versions::new(
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
    Name::from("demo")
}

/// A key for the tests.
pub(crate) fn key() -> object::Key {
    object::Key::from("notes.txt")
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

/// The wire form an identity is indexed under (`None` is the literal
/// `null`).
fn id_wire(id: Option<&version::Id>) -> String {
    id.map_or_else(|| "null".to_owned(), |id| id.as_str().to_owned())
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
                id_wire(row.id.as_ref()),
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
}

#[async_trait]
#[async_trait]
#[allow(unused_variables)]
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
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn record_if_absent(
        &self,
        bucket: &bucket::Name,
        now: SystemTime,
    ) -> Result<SystemTime, <Self as Metadata>::Error> {
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
#[allow(unused_variables)]
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
#[async_trait]
#[allow(unused_variables)]
impl UploadMeta for Fake {
    async fn create(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        row: &upload_row::Row,
    ) -> Result<(), <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
    }

    async fn get(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<Option<upload_row::Row>, <Self as Metadata>::Error> {
        unimplemented!("the engine's double implements only the calls the engine makes")
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
#[allow(unused_variables)]
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

    async fn advance_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<version::Id>,
        seq: u64,
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
                version: id,
                is_delete_marker,
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
        let wire = id_wire(row.id.as_ref());
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

    async fn expire(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<(), storage::Error> {
        self.drop_row(bucket, key, seq);
        Ok(())
    }

    /// The batch call, counted so a test can see that the engine's batched path
    /// used it: the double has no transaction to save, so the override exists
    /// to record the call, not to be cheaper.
    async fn expire_many(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seqs: &[u64],
    ) -> Result<(), storage::Error> {
        *self.inner.batch_removals.lock().unwrap() += 1;
        for seq in seqs {
            self.drop_row(bucket, key, *seq);
        }
        Ok(())
    }

    /// The engine never drains a bucket's rows (teardown is the backend's), so
    /// the double refuses the call rather than pretending to have a bucket's
    /// rows to remove.
    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), storage::Error> {
        let _ = bucket;
        unimplemented!("the engine's double implements only the calls the engine makes")
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
            .get(&(bucket.to_string(), key.to_string(), id_wire(id)))
            .copied())
    }
}

#[async_trait]
impl CurrentRows for Fake {
    async fn current(&self, bucket: &Name, key: &object::Key) -> Result<Current, storage::Error> {
        Ok(self.content_slot(bucket, key))
    }
}

/// An in-memory block store (the shape `tinio_core::blob`'s own example
/// pins): staged bytes are held in memory, filed on publish.
#[derive(Debug, Default)]
pub(crate) struct MemBlocks(Mutex<BTreeMap<BlockHash, Vec<u8>>>);

impl MemBlocks {
    /// How many blocks the store holds.
    pub(crate) fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }
}

/// The sink a staged block's bytes are held in.
#[derive(Default)]
pub(crate) struct Sink(Vec<u8>);

#[async_trait]
impl StagedBlock for Sink {
    async fn write(&mut self, chunk: Bytes) -> io::Result<()> {
        self.0.extend_from_slice(&chunk);
        Ok(())
    }
}

#[async_trait]
impl BlockStore for MemBlocks {
    type Error = storage::Error;
    type Staged = Sink;

    async fn stage(&self) -> Result<Self::Staged, Self::Error> {
        Ok(Sink::default())
    }

    async fn publish(
        &self,
        staged: Self::Staged,
        hash: &BlockHash,
    ) -> Result<Published, Self::Error> {
        let mut blocks = self.0.lock().unwrap();
        if blocks.contains_key(hash) {
            return Ok(Published::Existing);
        }
        blocks.insert(*hash, staged.0);
        Ok(Published::Stored)
    }

    async fn contains(&self, hash: &BlockHash) -> Result<bool, Self::Error> {
        Ok(self.0.lock().unwrap().contains_key(hash))
    }

    async fn open(
        &self,
        hash: &BlockHash,
        range: Option<ByteRange>,
    ) -> Result<BodyStream, Self::Error> {
        let blocks = self.0.lock().unwrap();
        let bytes = blocks
            .get(hash)
            .ok_or_else(|| storage::io(io::Error::new(io::ErrorKind::NotFound, "no such block")))?;
        let slice = match range {
            Some(range) => {
                let (start, end) = range.resolve(bytes.len() as u64)?;
                bytes[start as usize..=end as usize].to_vec()
            }
            None => bytes.clone(),
        };
        Ok(Box::pin(stream::once(
            async move { Ok(Bytes::from(slice)) },
        )))
    }

    async fn remove(&self, hash: &BlockHash) -> Result<(), Self::Error> {
        self.0.lock().unwrap().remove(hash);
        Ok(())
    }

    async fn list(
        &self,
        after: Option<&BlockHash>,
        limit: usize,
    ) -> Result<Vec<Entry>, Self::Error> {
        Ok(self.entries(after).into_iter().take(limit).collect())
    }

    async fn walk(
        &self,
        after: Option<&BlockHash>,
    ) -> Result<EntryStream<Self::Error>, Self::Error> {
        Ok(Box::pin(stream::iter(
            self.entries(after).into_iter().map(Ok),
        )))
    }
}

impl MemBlocks {
    /// The stored blocks after `after`, in address order.
    fn entries(&self, after: Option<&BlockHash>) -> Vec<Entry> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(hash, _)| after.is_none_or(|after| *hash > after))
            .map(|(hash, bytes)| Entry {
                hash: *hash,
                len: bytes.len() as u64,
                published: SystemTime::now(),
            })
            .collect()
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
