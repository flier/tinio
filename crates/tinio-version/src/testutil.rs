//! The in-crate port double the engine's own tests run against: rows in a
//! map, content in a tempdir, an in-memory block store.

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
        BodyStream, ByteRange,
        blob::{BlockHash, BlockStore, Entry, EntryStream, Published, StagedBlock},
        bucket::Name,
        collect_body, object, storage,
        version::{self, State},
    },
    _store::version::{self as rows, Kind, Row},
    Versions,
    ports::{ContentPort, Current, CurrentRows, PublishedBody, Superseded, VersionRows},
};

/// The engine over the double, plus the handles the tests probe.
pub(crate) struct Fixture {
    /// The content root's owner — dropping it removes the content, so the
    /// fixture holds it for the test's whole life.
    pub(crate) _dir: tempfile::TempDir,
    /// The ports, for planting slots and reading content.
    pub(crate) ports: Fake,
    /// The engine under test.
    pub(crate) versions: Versions<Fake, Fake, MemBlocks>,
}

/// Build a fixture: a fresh tempdir, its ports, and the engine over the
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

/// The port double: every port implemented over maps and one tempdir.
#[derive(Clone, Default)]
pub(crate) struct Fake {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    /// The bucket versioning states, defaulting to unversioned.
    states: Mutex<HashMap<String, State>>,
    /// The keys' current slots.
    slots: Mutex<HashMap<(String, String), Current>>,
    /// The version rows, keyed as the table is.
    rows: Mutex<BTreeMap<(String, String, u64), Row>>,
    /// The version index, keyed as the table is: `(bucket, key, id wire)` →
    /// `seq`. The port contract keeps it in step with the rows, so the double
    /// must too — a test that expires a version can then see the entry go.
    index: Mutex<BTreeMap<(String, String, String), u64>>,
    /// The content root; `None` until a content root is given.
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

    /// Write `bytes` at `key`'s content path.
    pub(crate) async fn write_content(&self, bucket: &Name, key: &object::Key, bytes: &[u8]) {
        let path = self.content_path(bucket, key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.unwrap();
        }
        fs::write(path, bytes).await.unwrap();
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

    /// Drop one row and its index entry (the one removal body both port
    /// methods run).
    fn drop_row(&self, bucket: &Name, key: &object::Key, seq: u64) {
        let removed =
            self.inner
                .rows
                .lock()
                .unwrap()
                .remove(&(bucket.to_string(), key.to_string(), seq));
        if let Some(row) = removed {
            self.inner
                .index
                .lock()
                .unwrap()
                .remove(&(bucket.to_string(), key.to_string(), row.id));
        }
    }

    /// The sequence the index names for `id` (`None` = no entry) — the
    /// test's window onto the port contract's index half.
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
impl VersionRows for Fake {
    async fn state(&self, bucket: &Name) -> Result<State, storage::Error> {
        Ok(self
            .inner
            .states
            .lock()
            .unwrap()
            .get(bucket.as_ref())
            .copied()
            .unwrap_or_default())
    }

    async fn set_state(&self, bucket: &Name, state: State) -> Result<(), storage::Error> {
        self.inner
            .states
            .lock()
            .unwrap()
            .insert(bucket.to_string(), state);
        Ok(())
    }

    async fn put_version_row(
        &self,
        bucket: &Name,
        key: &object::Key,
        superseded: &Superseded,
    ) -> Result<(), storage::Error> {
        let id_wire = version::wire_of(superseded.id.as_ref());
        let row = Row {
            id: id_wire.clone(),
            blob: rows::encode_blob(&superseded.blob),
            size: superseded.blob.len(),
            etag: String::new(),
            created: 0,
            became_noncurrent: 1,
            kind: if superseded.is_delete_marker {
                Kind::Marker
            } else {
                Kind::Object
            },
            tags: String::new(),
            checksum: String::new(),
            owner: String::new(),
            acl: String::new(),
            seq: superseded.seq,
        };
        // The row and its index entry are one unit (the port contract): the
        // double commits them together here.
        self.inner.index.lock().unwrap().insert(
            (bucket.to_string(), key.to_string(), id_wire),
            superseded.seq,
        );
        self.inner
            .rows
            .lock()
            .unwrap()
            .insert((bucket.to_string(), key.to_string(), superseded.seq), row);
        Ok(())
    }

    async fn remove_version_row(
        &self,
        bucket: &Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<(), storage::Error> {
        self.drop_row(bucket, key, seq);
        Ok(())
    }

    /// The batch port call, counted so a test can see that the engine's
    /// batched path used it: the double has no transaction to save, so the
    /// override exists to record the call, not to be cheaper.
    async fn remove_version_rows(
        &self,
        bucket: &Name,
        key: &object::Key,
        seqs: &[u64],
    ) -> Result<(), storage::Error> {
        *self.inner.batch_removals.lock().unwrap() += 1;
        for seq in seqs {
            self.drop_row(bucket, key, *seq);
        }
        Ok(())
    }

    async fn latest(
        &self,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<Option<(u64, Row)>, storage::Error> {
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

    async fn bucket_rows(&self, bucket: &Name) -> Result<Vec<Row>, storage::Error> {
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
}

#[async_trait]
impl CurrentRows for Fake {
    async fn current(&self, bucket: &Name, key: &object::Key) -> Result<Current, storage::Error> {
        Ok(self.content_slot(bucket, key))
    }
}

#[async_trait]
impl ContentPort for Fake {
    async fn open(
        &self,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<Option<BodyStream>, storage::Error> {
        // The out-of-band write lands here, between the resolve and the read.
        if let Some(slot) = self.inner.swap_on_open.lock().unwrap().pop_front() {
            self.set_slot(bucket, key, slot);
        }
        match fs::read(self.content_path(bucket, key)).await {
            Ok(bytes) => Ok(Some(Box::pin(stream::once(async move {
                Ok(Bytes::from(bytes))
            })))),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(storage::io(err)),
        }
    }

    async fn publish(
        &self,
        bucket: &Name,
        key: &object::Key,
        body: BodyStream,
    ) -> Result<PublishedBody, storage::Error> {
        let bytes = collect_body(body).await.map_err(storage::io)?;
        let published = PublishedBody::from_content(&bytes);
        self.write_content(bucket, key, &bytes).await;
        Ok(published)
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
