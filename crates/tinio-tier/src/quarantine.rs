//! Quarantine for a buffered write whose flush never succeeded.
//!
//! A batch write is acknowledged before the filesystem sees its bytes, so a
//! flush that fails every retry would otherwise either wedge the layer or lose
//! an acknowledged write silently. Instead the entry's bytes and the metadata
//! needed to restore it are written under the layer's private directory —
//! `<root>/.tinio/quarantine/<id>.json` beside `<id>.bin` — and the entry is
//! dropped from the buffer. An operator reads the list with
//! [`TierStorage::quarantined`] and clears it with
//! [`TierStorage::clear_quarantine`].
//!
//! Both files of a pair are written through a temporary sibling and renamed
//! into place, so a crash mid-write leaves a partial pair that a read ignores;
//! the reader never serves a record whose bytes are missing.

use std::{
    ffi::OsStr,
    fs, io,
    path::{Path, PathBuf},
    sync::atomic::Ordering as MemoryOrdering,
    time::SystemTime,
};

use serde::{Deserialize, Serialize};
#[cfg(test)]
use tinio_util::testing::metrics::test_lock;
use tinio_util::{fsutil::ok_if_missing, metrics::record_tier_quarantine};

use crate::{
    _core::{bucket, checksum, content::ObjectContent, object, storage},
    Buffered, Error, TierStorage, atomic,
    storage::LAYER_DIR,
};

/// The directory under the layer's private directory the pairs live in.
const QUARANTINE_DIR: &str = "quarantine";

/// The nanoseconds in one second, for persisting a [`SystemTime`] as its
/// seconds and subsecond nanoseconds.
const NANOS_PER_SEC: u64 = 1_000_000_000;

/// One write a flush failed to publish, as an operator reads it.
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
///
/// use tinio_core::{bucket, object};
/// use tinio_tier::Quarantined;
///
/// let record = Quarantined {
///     bucket: bucket::name("data").expect("a valid bucket name"),
///     key: object::key("a.bin").expect("a valid object key"),
///     recorded: SystemTime::UNIX_EPOCH,
///     len: 3,
/// };
/// assert_eq!(record.len, 3);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Quarantined {
    /// The bucket the buffered write belonged to.
    pub bucket: bucket::Name,
    /// The buffered object's key.
    pub key: object::Key,
    /// When the write was quarantined.
    pub recorded: SystemTime,
    /// The number of bytes the quarantine file holds.
    pub len: u64,
}

/// A persisted quarantine record.
///
/// The fields are the buffered write's body length plus the metadata a plain
/// batch write carries — key, size, modification time, ETag and the recorded
/// checksum; a buffered write never holds tags, an owner, an ACL or a lock,
/// and never belongs to a versioned bucket — so the object can be restored or
/// deleted by hand.
#[derive(Debug, Serialize, Deserialize)]
struct Record {
    bucket: String,
    key: String,
    recorded_secs: u64,
    recorded_nanos: u32,
    length: u64,
    info: RecordInfo,
}

/// The buffered [`object::Info`] fields a record keeps.
#[derive(Debug, Serialize, Deserialize)]
struct RecordInfo {
    size: u64,
    last_modified_secs: u64,
    last_modified_nanos: u32,
    etag: String,
    /// The recorded checksum's persisted wire form, or `None` when the object
    /// carried none.
    checksum: Option<String>,
}

impl<C: ObjectContent> TierStorage<C> {
    /// The buffered writes a failed flush quarantined, oldest first.
    ///
    /// A read-only run answers this too: it only reads the layer's state
    /// directory and creates nothing.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when the quarantine directory or a pair cannot be read,
    /// [`Error::Json`] when a complete pair's record cannot be decoded, and
    /// [`Error::Storage`] when a record names an invalid bucket or key.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{sync::Arc, time::Duration};
    ///
    /// use tinio_fs::{FsStorage, testing};
    /// use tinio_store_redb::RedbContent;
    /// use tinio_tier::{Config, FlushMode, Index, TierStorage};
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let fs =
    ///     Arc::new(FsStorage::new(root.path(), testing::fs_options()).expect("the fs store opens"));
    /// let tier = TierStorage::from_parts(
    ///     fs,
    ///     RedbContent::in_memory().expect("an in-memory cold plane"),
    ///     Arc::new(Index::open_in_memory().expect("the index opens")),
    ///     Config {
    ///         capacity_bytes: 1024 * 1024,
    ///         max_object_bytes: 64 * 1024,
    ///         flush_mode: FlushMode::Batch,
    ///         batch_bytes: 8 * 1024,
    ///         batch_interval: Duration::from_secs(1),
    ///         heat_window: Duration::from_secs(60),
    ///         promote_reads: 3,
    ///         stats_flush: Duration::from_secs(10),
    ///         high_water_percent: 90,
    ///         low_water_percent: 80,
    ///         demote_idle: Duration::from_secs(3600),
    ///         dwell: Duration::from_secs(3600),
    ///         scan_interval: Duration::from_secs(60),
    ///         move_concurrency: 1,
    ///         index_compact_percent: 30,
    ///         gc_grace: Duration::from_secs(300),
    ///     },
    /// );
    /// assert!(
    ///     tier.quarantined()
    ///         .expect("the state directory is readable")
    ///         .is_empty()
    /// );
    /// ```
    pub fn quarantined(&self) -> Result<Vec<Quarantined>, Error> {
        list(self.fs().root())
    }

    /// Remove every quarantined pair, answering how many pairs were removed.
    ///
    /// The bytes stay in `<root>/.tinio/quarantine/` until this is called: an
    /// operator decides whether a quarantined write is restored by hand or
    /// discarded. A run that opened the layer read-only refuses. A removal
    /// that fails part-way still leaves the counter and gauge reporting what
    /// the directory actually holds, and the removal's own error is returned
    /// unchanged.
    ///
    /// # Errors
    ///
    /// [`Error::ReadOnly`] for a read-only run, and [`Error::Io`] when the
    /// directory or one of its files cannot be removed.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{sync::Arc, time::Duration};
    ///
    /// use tinio_fs::{FsStorage, testing};
    /// use tinio_store_redb::RedbContent;
    /// use tinio_tier::{Config, FlushMode, Index, TierStorage};
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let fs =
    ///     Arc::new(FsStorage::new(root.path(), testing::fs_options()).expect("the fs store opens"));
    /// let tier = TierStorage::from_parts(
    ///     fs,
    ///     RedbContent::in_memory().expect("an in-memory cold plane"),
    ///     Arc::new(Index::open_in_memory().expect("the index opens")),
    ///     Config {
    ///         capacity_bytes: 1024 * 1024,
    ///         max_object_bytes: 64 * 1024,
    ///         flush_mode: FlushMode::Batch,
    ///         batch_bytes: 8 * 1024,
    ///         batch_interval: Duration::from_secs(1),
    ///         heat_window: Duration::from_secs(60),
    ///         promote_reads: 3,
    ///         stats_flush: Duration::from_secs(10),
    ///         high_water_percent: 90,
    ///         low_water_percent: 80,
    ///         demote_idle: Duration::from_secs(3600),
    ///         dwell: Duration::from_secs(3600),
    ///         scan_interval: Duration::from_secs(60),
    ///         move_concurrency: 1,
    ///         index_compact_percent: 30,
    ///         gc_grace: Duration::from_secs(300),
    ///     },
    /// );
    /// assert_eq!(
    ///     tier.clear_quarantine().expect("there is nothing to clear"),
    ///     0
    /// );
    /// ```
    pub fn clear_quarantine(&self) -> Result<usize, Error> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        let outcome = clear(self.fs().root());
        // A removal can fail part-way, so the counter follows a fresh
        // decode-free count of what is actually left — on the error path too.
        // A count that cannot run leaves the counter and gauge untouched.
        if let Ok(remaining) = count(self.fs().root()) {
            self.quarantine_count
                .store(remaining, MemoryOrdering::Relaxed);
            #[cfg(test)]
            let _gate = test_lock::writer();
            record_tier_quarantine(remaining);
        }
        outcome
    }
}

/// Persist `entry` as a quarantine pair under `root`'s state directory.
pub(crate) fn record(
    root: &Path,
    bucket: &bucket::Name,
    entry: &Buffered,
) -> Result<Quarantined, Error> {
    let recorded = SystemTime::now();
    let dir = directory(root);
    fs::create_dir_all(&dir)?;
    let (recorded_secs, recorded_nanos) = parts(recorded);
    let (last_modified_secs, last_modified_nanos) = parts(entry.info.last_modified);
    let record = Record {
        bucket: bucket.to_string(),
        key: entry.info.key.to_string(),
        recorded_secs,
        recorded_nanos,
        length: entry.bytes.len() as u64,
        info: RecordInfo {
            size: entry.info.size,
            last_modified_secs,
            last_modified_nanos,
            etag: entry.info.etag.to_string(),
            checksum: entry
                .info
                .checksum
                .as_ref()
                .map(checksum::Recorded::to_wire),
        },
    };
    let json = serde_json::to_string(&record)?;
    let id = uuid::Uuid::new_v4().to_string();
    write_pair(&dir, &id, json.as_bytes(), &entry.bytes)?;
    Ok(Quarantined {
        bucket: bucket.clone(),
        key: entry.info.key.clone(),
        recorded,
        len: entry.bytes.len() as u64,
    })
}

/// The complete pairs under `root`, without decoding a single record.
///
/// This is the gauge's seed: it stats each `.json` file's `.bin` sibling and
/// never parses the record, so opening a root with a large quarantine is not
/// a decode pass over it.
pub(crate) fn count(root: &Path) -> Result<u64, Error> {
    Ok(pairs(root)?.complete.len() as u64)
}

/// The quarantined pairs under `root`, oldest first; an absent directory is an
/// empty list, which is what a root that never quarantined anything answers.
pub(crate) fn list(root: &Path) -> Result<Vec<Quarantined>, Error> {
    let mut records = pairs(root)?
        .complete
        .iter()
        .map(|path| read_record(path))
        .collect::<Result<Vec<_>, _>>()?;
    records.sort_by(|left, right| {
        (left.recorded, left.bucket.as_ref(), left.key.as_ref()).cmp(&(
            right.recorded,
            right.bucket.as_ref(),
            right.key.as_ref(),
        ))
    });
    Ok(records)
}

/// Remove every quarantine file under `root`, answering how many complete
/// pairs were removed.
pub(crate) fn clear(root: &Path) -> Result<usize, Error> {
    let pairs = pairs(root)?;
    // A partial pair and a leftover temporary file are cleared too; they are
    // never a record, so they are not counted.
    for path in &pairs.all {
        ok_if_missing(fs::remove_file(path)).map_err(Error::Io)?;
    }
    Ok(pairs.complete.len())
}

/// The quarantine directory's entries: every path, and the `.json` path of
/// each complete pair, whose `.bin` sibling exists.
///
/// An absent directory holds neither. The pair test is one `.bin` stat, never
/// a record decode, so the count stays cheap and a pair still being written —
/// or one a crash interrupted — is ignored until both halves are in place.
struct Pairs {
    /// Every entry under the directory, complete or not.
    all: Vec<PathBuf>,
    /// The `.json` half of every complete pair.
    complete: Vec<PathBuf>,
}

/// Read the quarantine directory once, splitting its entries into all paths
/// and the complete pairs.
fn pairs(root: &Path) -> Result<Pairs, Error> {
    let dir = directory(root);
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            return Ok(Pairs {
                all: Vec::new(),
                complete: Vec::new(),
            });
        }
        Err(err) => return Err(Error::Io(err)),
    };
    let mut pairs = Pairs {
        all: Vec::new(),
        complete: Vec::new(),
    };
    for entry in entries {
        let path = entry?.path();
        if path.extension().and_then(OsStr::to_str) == Some("json")
            && let Some(id) = path.file_stem().and_then(OsStr::to_str)
            && path.with_file_name(format!("{id}.bin")).try_exists()?
        {
            pairs.complete.push(path.clone());
        }
        pairs.all.push(path);
    }
    Ok(pairs)
}

/// The quarantine directory under the layer's state directory.
fn directory(root: &Path) -> PathBuf {
    root.join(LAYER_DIR).join(QUARANTINE_DIR)
}

/// Write both halves of one pair through temporary siblings and rename them
/// into place, syncing the directory once both renames landed.
fn write_pair(dir: &Path, id: &str, json: &[u8], bytes: &[u8]) -> Result<(), Error> {
    let json_path = dir.join(format!("{id}.json"));
    let bin_path = dir.join(format!("{id}.bin"));
    let json_temp = dir.join(format!("{id}.json.tmp"));
    let bin_temp = dir.join(format!("{id}.bin.tmp"));
    // The record is renamed before its bytes: a crash between the two leaves a
    // record whose pair the reader ignores.
    atomic::write_atomic_pair(
        atomic::PairHalf {
            destination: &json_path,
            temp: &json_temp,
            bytes: json,
        },
        atomic::PairHalf {
            destination: &bin_path,
            temp: &bin_temp,
            bytes,
        },
    )?;
    Ok(())
}

/// Read one complete pair's record.
fn read_record(path: &Path) -> Result<Quarantined, Error> {
    let raw = fs::read(path)?;
    let record: Record = serde_json::from_slice(&raw)?;
    Ok(Quarantined {
        bucket: bucket::name(record.bucket)?,
        key: object::key(record.key)?,
        recorded: system_time(record.recorded_secs, record.recorded_nanos),
        len: record.length,
    })
}

/// The (seconds, nanoseconds) pair a [`SystemTime`] is persisted as; a time
/// before the epoch is recorded as the epoch itself.
fn parts(time: SystemTime) -> (u64, u32) {
    let nanos = storage::to_nanos(time);
    let secs = nanos / NANOS_PER_SEC;
    (secs, (nanos % NANOS_PER_SEC) as u32)
}

/// The [`SystemTime`] a persisted pair names; an out-of-range record saturates
/// rather than panicking on decoded input.
fn system_time(secs: u64, nanos: u32) -> SystemTime {
    let nanos = secs
        .saturating_mul(NANOS_PER_SEC)
        .saturating_add(u64::from(nanos.min(NANOS_PER_SEC as u32 - 1)));
    storage::from_nanos(nanos)
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use bytes::Bytes;
    use tinio_core::{
        ETag,
        acl::Acl,
        bucket, checksum, object,
        storage::{BucketOps, ListObjectsParams, ObjectOps},
    };
    use tinio_fs::{FsStorage, testing};
    use tinio_store_redb::RedbContent;
    use tinio_util::testing::{
        body,
        metrics::{gauge, histogram_count, test_lock},
    };
    use tokio::runtime::Runtime;

    use super::*;
    use crate::{Config, FlushMode, Index, TierStorage};

    fn config() -> Config {
        Config {
            capacity_bytes: 1024 * 1024,
            max_object_bytes: 64 * 1024,
            flush_mode: FlushMode::Batch,
            batch_bytes: 8 * 1024,
            batch_interval: Duration::from_secs(1),
            heat_window: Duration::from_secs(60),
            promote_reads: 2,
            stats_flush: Duration::from_secs(10),
            high_water_percent: 90,
            low_water_percent: 80,
            demote_idle: Duration::from_secs(3600),
            dwell: Duration::from_secs(3600),
            scan_interval: Duration::from_secs(60),
            move_concurrency: 1,
            index_compact_percent: 30,
            gc_grace: Duration::from_secs(300),
        }
    }

    /// A tier layer over a real root with its bucket created; the returned
    /// directory keeps the root alive.
    async fn fixture() -> (tempfile::TempDir, TierStorage<RedbContent>, bucket::Name) {
        let root = tempfile::tempdir().expect("a temporary root");
        let fs = FsStorage::new(root.path(), testing::fs_options()).expect("the fs store opens");
        let tier = TierStorage::from_parts(
            Arc::new(fs),
            RedbContent::in_memory().expect("an in-memory cold plane"),
            Arc::new(Index::open_in_memory().expect("the index opens")),
            config(),
        );
        let bucket = bucket::name("data").expect("a valid bucket name");
        tier.create_bucket(&bucket, None, &Acl::default_private(None))
            .await
            .expect("the bucket is created");
        (root, tier, bucket)
    }

    /// Buffer a plain write, answering the key it waits under.
    async fn buffer(
        tier: &TierStorage<RedbContent>,
        bucket: &bucket::Name,
        name: &str,
        bytes: &[u8],
    ) -> object::Key {
        let key = object::key(name).expect("a valid object key");
        let staged = tier
            .stage_body(bucket, &key, body(bytes), None, None)
            .await
            .expect("the body stages");
        tier.commit_object(
            bucket,
            &key,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .expect("the write is acknowledged");
        key
    }

    /// The buffered entry a record is written for.
    fn buffered(name: &str, bytes: &'static [u8]) -> Buffered {
        let data = Bytes::from_static(bytes);
        Buffered {
            info: object::Info {
                key: object::key(name).expect("a valid object key"),
                size: data.len() as u64,
                last_modified: SystemTime::UNIX_EPOCH,
                etag: ETag::from_content(&data),
                tags: object::Tags::empty(),
                checksum: None,
                owner: None,
                acl: Acl::default_private(None),
                version_id: None,
                bucket_records_versions: false,
            },
            bytes: data,
        }
    }

    impl Record {
        /// Rebuild the buffered write this record carries with `bytes` as its
        /// body — the recovery step a record exists for.
        fn buffered(&self, bytes: Bytes) -> Buffered {
            Buffered {
                info: object::Info {
                    key: object::key(self.key.clone()).expect("the record holds a valid key"),
                    size: self.info.size,
                    last_modified: system_time(
                        self.info.last_modified_secs,
                        self.info.last_modified_nanos,
                    ),
                    etag: self
                        .info
                        .etag
                        .parse()
                        .expect("the record holds a valid ETag"),
                    tags: object::Tags::empty(),
                    checksum: self
                        .info
                        .checksum
                        .as_deref()
                        .and_then(checksum::Recorded::from_wire_opt),
                    owner: None,
                    acl: Acl::default_private(None),
                    version_id: None,
                    bucket_records_versions: false,
                },
                bytes,
            }
        }
    }

    #[test]
    fn a_poison_entry_is_quarantined_and_the_drain_continues() {
        Runtime::new().expect("a runtime").block_on(async {
            let (root, tier, bucket) = fixture().await;
            // The poison key sorts first, so the good keys after it prove the
            // drain continued past it rather than being the lucky order.
            let poison = buffer(&tier, &bucket, "a-poison.bin", b"poison").await;
            let before = buffer(&tier, &bucket, "b-good.bin", b"good").await;
            let after = buffer(&tier, &bucket, "c-good.bin", b"after").await;
            assert!(tier.buffer().contains(&bucket, &poison));

            // A directory where the object's file belongs fails every publish
            // attempt of the poison key only.
            fs::create_dir_all(root.path().join("data").join("a-poison.bin"))
                .expect("the blocker exists");

            tier.shutdown().await.expect("the drain reaches the end");

            for (key, expected) in [(&before, &b"good"[..]), (&after, &b"after"[..])] {
                assert_eq!(
                    fs::read(root.path().join("data").join(key.as_ref()))
                        .expect("the good write lands"),
                    expected,
                    "a good entry after the poison is published"
                );
            }
            assert!(
                !tier.buffer().contains(&bucket, &poison),
                "the quarantined entry left the buffer"
            );
            assert!(tier.buffer().is_empty(), "the good entries were published");

            let quarantined = tier.quarantined().expect("the quarantine is listed");
            assert_eq!(quarantined.len(), 1);
            assert_eq!(quarantined[0].bucket, bucket);
            assert_eq!(quarantined[0].key, poison);
            assert_eq!(quarantined[0].len, 6);
            let dir = directory(root.path());
            let id = id_of(&dir);
            assert_eq!(
                fs::read(dir.join(format!("{id}.bin"))).expect("the pair's bytes exist"),
                b"poison"
            );
            let record = fs::read_to_string(dir.join(format!("{id}.json")))
                .expect("the pair's record exists");
            assert!(record.contains("\"length\":6"), "{record}");
            assert!(record.contains("a-poison.bin"), "{record}");

            // Neither a later listing nor a second shutdown is wedged.
            tier.list_objects(ListObjectsParams {
                bucket: bucket.clone(),
                prefix: String::new(),
                delimiter: None,
                start_after: None,
                max_keys: 100,
            })
            .await
            .expect("the listing still runs");
            tier.shutdown().await.expect("a second shutdown is a no-op");
            assert_eq!(tier.quarantined().expect("the list is read").len(), 1);

            assert_eq!(tier.clear_quarantine().expect("the files clear"), 1);
            assert!(tier.quarantined().expect("the list is read").is_empty());
            assert_eq!(fs::read_dir(dir).expect("the directory is read").count(), 0);
        });
    }

    #[test]
    fn an_unwritable_quarantine_directory_drops_the_entry_without_failing_the_drain() {
        Runtime::new().expect("a runtime").block_on(async {
            let (root, tier, bucket) = fixture().await;
            let good = buffer(&tier, &bucket, "good.bin", b"good").await;
            let poison = buffer(&tier, &bucket, "poison.bin", b"poison").await;
            // A file where the quarantine directory belongs makes every
            // bookkeeping write fail.
            fs::create_dir_all(root.path().join(".tinio")).expect("the layer directory exists");
            fs::write(
                root.path().join(".tinio").join("quarantine"),
                b"not a directory",
            )
            .expect("the blocker exists");
            fs::create_dir_all(root.path().join("data").join("poison.bin"))
                .expect("the blocker exists");

            tier.shutdown().await.expect("the drain reaches the end");

            assert!(tier.buffer().is_empty(), "the poison entry was dropped");
            assert_eq!(
                fs::read(root.path().join("data").join("good.bin")).expect("the good write lands"),
                b"good"
            );
            let refused = tier
                .quarantined()
                .expect_err("the blocker denies a listing");
            assert!(matches!(refused, Error::Io(_)), "{refused}");
            assert!(!tier.buffer().contains(&bucket, &poison));
            assert!(!tier.buffer().contains(&bucket, &good));
        });
    }

    /// A record keeps every metadata field a recovery restores, the recorded
    /// checksum included.
    #[test]
    fn a_record_round_trips_the_buffered_metadata() {
        let root = tempfile::tempdir().expect("a temporary root");
        let bucket = bucket::name("data").expect("a valid bucket name");
        let mut entry = buffered("checked.bin", b"body");
        entry.info.checksum = checksum::Recorded::from_wire_opt("CRC32:NhCmhg==:FULL_OBJECT");
        record(root.path(), &bucket, &entry).expect("the pair is written");

        let dir = directory(root.path());
        let id = id_of(&dir);
        let raw = fs::read(dir.join(format!("{id}.json"))).expect("the record is read");
        let restored: Record = serde_json::from_slice(&raw).expect("the record decodes");
        let recovered = restored.buffered(Bytes::from_static(b"body"));

        assert_eq!(recovered.bytes, entry.bytes);
        assert_eq!(recovered.info.key, entry.info.key);
        assert_eq!(recovered.info.size, entry.info.size);
        assert_eq!(recovered.info.etag, entry.info.etag);
        assert_eq!(recovered.info.last_modified, entry.info.last_modified);
        assert_eq!(
            recovered.info.checksum, entry.info.checksum,
            "the recorded checksum is restored"
        );
    }

    /// A pair with only one half — or a leftover temporary file — is never a
    /// record.
    #[test]
    fn a_partial_pair_is_ignored_by_the_reading_paths() {
        let root = tempfile::tempdir().expect("a temporary root");
        let bucket = bucket::name("data").expect("a valid bucket name");
        record(root.path(), &bucket, &buffered("a.bin", b"x")).expect("the pair is written");
        let dir = directory(root.path());
        let id = id_of(&dir);
        fs::remove_file(dir.join(format!("{id}.bin"))).expect("the bytes are removed");
        fs::write(dir.join("stray.bin"), b"orphan bytes").expect("the stray half exists");
        fs::write(dir.join("stray.json.tmp"), b"{}").expect("the leftover temp exists");

        assert!(
            list(root.path()).expect("the listing runs").is_empty(),
            "neither half of a partial pair is listed"
        );
        assert_eq!(
            count(root.path()).expect("the count runs"),
            0,
            "the count also ignores a partial pair"
        );
    }

    /// A clear books what the directory actually holds: every pair gone reads
    /// zero, and a removal that fails part-way books the remaining count and
    /// returns the removal's own error.
    #[test]
    fn clear_quarantine_books_the_remaining_count() {
        let _window = test_lock::window();
        let root = tempfile::tempdir().expect("a temporary root");
        let bucket = bucket::name("data").expect("a valid bucket name");
        for name in ["a.bin", "b.bin"] {
            record(root.path(), &bucket, &buffered(name, b"x")).expect("the pair is written");
        }
        let fs = FsStorage::new(root.path(), testing::fs_options()).expect("the fs store opens");
        let tier = TierStorage::from_parts(
            Arc::new(fs),
            RedbContent::in_memory().expect("an in-memory cold plane"),
            Arc::new(Index::open_in_memory().expect("the index opens")),
            config(),
        );
        assert_eq!(
            gauge("tinio_tier_quarantine", &[]),
            2.0,
            "the open books the backlog"
        );

        assert_eq!(tier.clear_quarantine().expect("the pairs clear"), 2);
        assert_eq!(
            gauge("tinio_tier_quarantine", &[]),
            0.0,
            "an empty directory reads zero"
        );

        // One fresh pair and a directory the removal cannot unlink: the clear
        // fails after the attempt, so the gauge reports what is left.
        record(root.path(), &bucket, &buffered("c.bin", b"y")).expect("the pair is written");
        fs::create_dir_all(directory(root.path()).join("blocked")).expect("the blocker exists");
        let refused = tier
            .clear_quarantine()
            .expect_err("the blocker denies a removal");
        assert!(matches!(refused, Error::Io(_)), "{refused}");
        let remaining = count(root.path()).expect("the count runs");
        assert_eq!(
            gauge("tinio_tier_quarantine", &[]),
            remaining as f64,
            "the gauge follows what the directory actually holds"
        );
        assert_eq!(
            tier.quarantined().expect("the list is read").len() as u64,
            remaining
        );
    }

    /// The id of the one record in `dir`, for asserting the pair exists.
    fn id_of(dir: &Path) -> String {
        fs::read_dir(dir)
            .expect("the quarantine directory is read")
            .filter_map(|entry| {
                let path = entry.expect("the entry is read").path();
                (path.extension().and_then(OsStr::to_str) == Some("json")).then(|| {
                    path.file_stem()
                        .expect("a name")
                        .to_string_lossy()
                        .into_owned()
                })
            })
            .next()
            .expect("one pair exists")
    }

    /// A drain that selected an entry but published none — every attempt was
    /// quarantined — is not a flush: it books no duration sample.
    #[test]
    fn an_all_quarantined_drain_books_no_flush_duration() {
        let _window = test_lock::window();
        Runtime::new().expect("a runtime").block_on(async {
            let (root, tier, bucket) = fixture().await;
            let poison = buffer(&tier, &bucket, "poison.bin", b"poison").await;
            // A directory where the object's file belongs fails every attempt.
            fs::create_dir_all(root.path().join("data").join("poison.bin"))
                .expect("the blocker exists");

            let before = histogram_count("tinio_tier_flush_duration_seconds", &[]);
            tier.shutdown().await.expect("the drain reaches the end");

            assert!(
                !tier.buffer().contains(&bucket, &poison),
                "the quarantined entry left the buffer"
            );
            assert_eq!(
                histogram_count("tinio_tier_flush_duration_seconds", &[]) - before,
                0.0,
                "a drain that published nothing is not a flush"
            );
        });
    }
}
