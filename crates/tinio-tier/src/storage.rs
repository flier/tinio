//! The tier decorator: one storage contract over the memory, warm and cold
//! planes.
//!
//! [`TierStorage`] decorates an [`FsStorage`] and adds the two planes beside
//! it: a [`Cache`] in front of the filesystem and an [`ObjectContent`] cold
//! store behind it, placed by the layer's [`Index`]. The read path is the
//! spec's order (T6) — the object cache, then the filesystem, then the cold
//! store — with the cold reads reconciled against the row the filesystem
//! kept (T5). Every byte-producing mutation invalidates the cached entry
//! (T16); the placement and cold-copy resets a write performs belong to the
//! write path's own task, so the interceptors here only forward and
//! invalidate.
//!
//! A write stages through the filesystem's streaming temp (sync writes) or,
//! when batch-eligible, into a bounded memory buffer that spills to that same
//! stage past `max_object_bytes` (T7); only the commit decides whether the
//! buffered bytes wait for a flush.
//!
//! Reads take a per-key read lock and hold it through the body stream, so a
//! move or a delete cannot swap the plane under a half-served body (T14).

mod locks;
mod marker;
mod plane;

use std::{
    cmp::Ordering,
    fs,
    future::Future,
    io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering as MemoryOrdering},
    },
    time::SystemTime,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream};
use locks::{BucketGate, BucketReadGuard, KeyLocks, ReadGuard, WriteGuard};
use marker::{
    index_identity_if_present, open_index_for_bind, open_index_read_only, open_index_writable,
    open_mutation_file, read_marker, record_marker, remove_marker_temp, root_wire, write_marker,
    write_marker_if_different,
};
use plane::{Plane, WriteMeta};
use tinio_util::metrics::{
    Backend, MoveDirection, TierPlane, record_read_only, record_tier_bytes, record_tier_hit,
    record_tier_miss, record_tier_move, record_tier_quarantine,
};
#[cfg(test)]
use tinio_util::testing::metrics::test_lock;
use tokio::{fs as async_fs, task};

use crate::{
    _core::{
        AccessMode, Class,
        acl::{self, Acl, AclGrants},
        bucket, checksum,
        content::{ContentKey, ObjectContent, StagedObject, stage_body},
        cors, lifecycle, multipart, object, object_lock,
        storage::{
            self, BodyStream, BucketOps, BucketsListing, ByteRange, GetObjectResult,
            LifecycleRules, ListBucketsParams, ListObjectsParams, ListPartsParams,
            ListUploadsParams, MultipartOps, ObjectListing, ObjectOps, PartsListing, Storage,
            TagWriteResult, UploadsListing, VersionOps, collect_body,
        },
        version,
    },
    _fs::{
        FsContent, FsOptions, FsStaged, FsStorage, path,
        tier::{self, Rows},
    },
    _pg, Admission, Buffer, Buffered, Cache, ColdInfo, ColdStore, Config, Error, FlushMode, Heat,
    Index, MemInfo, Placement, State, Tier, TierStaged,
    buffer::FlushPlane,
    mover::MoveTimer,
    pg, quarantine,
    stage::stage_bounded,
};

/// The tier decorator over one filesystem root and one cold content plane.
///
/// Construction takes the two planes and the layer's configuration; the
/// index, the cache and the heat counters are the layer's own and are built
/// from the configuration. The cold plane is a type parameter so the layer
/// is exercised without PostgreSQL — [`crate::ColdStore`] is the production
/// choice and an in-memory [`ObjectContent`] serves tests and examples.
///
/// The handle is cloneable: every clone shares the same cache, heat
/// counters, index and per-key locks.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use tinio_core::storage::Storage;
/// use tinio_fs::testing;
/// use tinio_store_redb::RedbContent;
/// use tinio_tier::{Config, FlushMode, TierStorage};
///
/// let root = tempfile::tempdir().expect("a temporary root");
/// let tier = TierStorage::open(
///     root.path(),
///     testing::fs_options(),
///     Config {
///         capacity_bytes: 1024 * 1024,
///         max_object_bytes: 64 * 1024,
///         flush_mode: FlushMode::Sync,
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
///     RedbContent::in_memory().expect("an in-memory cold plane"),
/// )
/// .expect("the tier layer opens");
///
/// assert!(tier.supports_tiers());
/// ```
#[derive(Clone)]
pub struct TierStorage<C: ObjectContent> {
    /// The warm plane the layer decorates.
    fs: Arc<FsStorage>,
    /// The warm plane's row seam — the reconcile input and the retained
    /// parts.
    pub(crate) rows: tier::Rows,
    /// The cold plane's bytes.
    pub(crate) cold: C,
    /// The layer's placement, warm-candidate and heat index.
    pub(crate) index: Arc<Index>,
    /// The whole-object cache in front of the filesystem.
    cache: Arc<Cache>,
    /// The batch write buffer: acknowledged writes waiting for a flush (T7).
    buffer: Arc<Buffer>,
    /// The read counters the promotion pass consumes.
    heat: Arc<Heat>,
    /// The runtime limits.
    pub(crate) config: Config,
    /// The per-key read/write locks (T14).
    pub(crate) locks: KeyLocks,
    /// The per-bucket gates a listing holds and a finalize briefly takes
    /// (T14).
    pub(crate) gates: BucketGate,
    /// Whether this run opened the layer read-only (T27): it creates and
    /// writes nothing, so every path that would must refuse instead.
    pub(crate) read_only: bool,
    /// The quarantine records waiting, seeded once from the state directory
    /// and then kept without a per-poison directory scan.
    pub(crate) quarantine_count: Arc<AtomicU64>,
}

impl<C: ObjectContent> TierStorage<C> {
    /// Open the layer over `root` with `options`, `config` and `cold`.
    ///
    /// Construction is fail-closed and ordered (T17): the private directory,
    /// then the marker `<root>/.tinio/tier.mode` — written once, owner-only,
    /// through a temporary file renamed into place and fsynced with its
    /// directory — and only then the index `<root>/.tinio/tier.redb` and its
    /// placements. Both record the root identity; a marker and an index that
    /// disagree are refused, never reconciled. The configuration is validated
    /// before anything is opened.
    ///
    /// A read-only run ([`AccessMode::ReadOnly`] in
    /// [`FsOptions::access`](tinio_fs::FsOptions::access)) creates nothing
    /// (T27): it opens the existing index and cold plane read-only, requires
    /// the index, starts no background task and performs no repair, and serves
    /// cold reads from the placement's cold metadata.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] for a configuration that violates a cross-field
    /// rule; [`Error::Fs`] when the root cannot be opened; [`Error::Io`],
    /// [`Error::CorruptMarker`] or [`Error::MarkerMismatch`] when the marker
    /// cannot be read or disagrees with the index; [`Error::Index`] when the
    /// index cannot be created; [`Error::IndexMissing`] when a read-only run
    /// finds no index.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    ///
    /// use tinio_fs::testing;
    /// use tinio_store_redb::RedbContent;
    /// use tinio_tier::{Config, FlushMode, TierStorage};
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let tier = TierStorage::open(
    ///     root.path(),
    ///     testing::fs_options(),
    ///     Config {
    ///         capacity_bytes: 1024 * 1024,
    ///         max_object_bytes: 64 * 1024,
    ///         flush_mode: FlushMode::Sync,
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
    ///     RedbContent::in_memory().expect("an in-memory cold plane"),
    /// )
    /// .expect("the tier layer opens");
    ///
    /// assert_eq!(tier.cache().len_bytes(), 0);
    /// ```
    pub fn open(
        root: impl Into<PathBuf>,
        options: FsOptions,
        config: Config,
        cold: C,
    ) -> Result<Self, Error> {
        config.validate()?;
        let access = options.access;
        let fs = FsStorage::new(root, options)?;
        // The layer's own directory is the root's, never a relocated fs state
        // dir: a read-only run always reads `<root>/.tinio`, so the write run
        // must record there too (T17).
        let layer = fs.root().to_path_buf();
        let index = if access.is_read_only() {
            let marker = read_marker(&marker_path(&layer))?;
            open_index_read_only(&layer, marker)?
        } else {
            // The marker is created and fsynced before the index is opened.
            let marker = record_marker(&layer)?;
            open_index_writable(&layer, &marker, config.index_compact_percent)?
        };
        Ok(Self::assemble(
            Arc::new(fs),
            cold,
            Arc::new(index),
            config,
            access.is_read_only(),
        ))
    }

    /// Assemble the layer from the pieces a caller already owns — the
    /// test-support constructor that skips the root, options and open.
    ///
    /// Ownership and the root-identity check are part of the pg construction,
    /// so this constructor takes neither: the caller hands in the cold plane
    /// it already opened and the index it already bound.
    ///
    /// It always reports the layer writable: [`TierStorage::open`] and
    /// [`TierStorage::open_pg`] take the read-only flag from their access
    /// mode, and this constructor has no options to carry one. A read-only fs
    /// handed in here still books `read_only` as false, so the read-only paths
    /// are exercised through the real constructors rather than through this
    /// test-support one.
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
    ///         flush_mode: FlushMode::Sync,
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
    ///
    /// assert_eq!(tier.cache().len_bytes(), 0);
    /// ```
    #[cfg(feature = "testing")]
    pub fn from_parts(fs: Arc<FsStorage>, cold: C, index: Arc<Index>, config: Config) -> Self {
        Self::assemble(fs, cold, index, config, false)
    }

    /// The one assembly shared by both constructors.
    fn assemble(
        fs: Arc<FsStorage>,
        cold: C,
        index: Arc<Index>,
        config: Config,
        read_only: bool,
    ) -> Self {
        let rows = fs.tier_rows();
        let budget = Arc::new(crate::Budget::new(config.capacity_bytes));
        let cache = Arc::new(Cache::new(Arc::clone(&budget), config.max_object_bytes));
        let buffer = Arc::new(Buffer::new(budget, config.batch_bytes));
        let heat = Arc::new(Heat::new(config.heat_window, config.promote_reads));
        // Seeded without decoding a record: a stat failure starts from zero and
        // records nothing, so the gauge is never a fabricated count.
        let seeded = quarantine::count(fs.root()).ok();
        let quarantine_count = Arc::new(AtomicU64::new(seeded.unwrap_or(0)));
        if let Some(seeded) = seeded {
            #[cfg(test)]
            let _gate = test_lock::writer();
            record_tier_quarantine(seeded);
        }
        // The one construction both open paths share, so a degraded open and a
        // fresh one report alike: the fs and pg backends book their own
        // `read_only` series at the same seam.
        #[cfg(test)]
        let _gate = test_lock::writer();
        record_read_only(Backend::Tier, read_only);
        Self {
            fs,
            rows,
            cold,
            index,
            cache,
            buffer,
            heat,
            config,
            locks: KeyLocks::default(),
            gates: BucketGate::default(),
            read_only,
            quarantine_count,
        }
    }

    /// The warm filesystem plane the layer decorates.
    pub fn fs(&self) -> &FsStorage {
        &self.fs
    }

    /// The filesystem plane's row handle: the tier seam the layer and the
    /// operator's verification read and restore object rows through.
    pub fn tier_rows(&self) -> Rows {
        self.fs.tier_rows()
    }

    /// The layer's placement, heat and warm-candidate index.
    pub fn index(&self) -> &Index {
        &self.index
    }

    /// The whole-object cache in front of the filesystem.
    pub fn cache(&self) -> &Cache {
        &self.cache
    }

    /// The batch write buffer: the plain writes acknowledged before the
    /// filesystem saw them (T7).
    ///
    /// The buffer shares the cache's byte budget and holds at most
    /// `batch_bytes`; a client listing, a shutdown and every element or byte
    /// operation on a buffered key flush it (T8).
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
    ///
    /// assert!(tier.buffer().is_empty());
    /// ```
    pub fn buffer(&self) -> &Buffer {
        &self.buffer
    }

    /// Drain the batch buffer: every acknowledged write reaches the warm
    /// plane before the layer is dropped (T8).
    ///
    /// Every entry is attempted three times past its first failure. An entry
    /// that still fails is written to the layer's quarantine — the drain never
    /// stops on it — so the shutdown always reaches the end and a second call
    /// is a no-op. A quarantine pair that cannot be written is logged and the
    /// entry dropped, because one poison write must not wedge the layer.
    ///
    /// # Errors
    ///
    /// The call answers `Ok` today: every entry is published or quarantined,
    /// and a quarantine write that itself fails is logged and dropped. The
    /// `Result` remains part of the shutdown contract for a future drain that
    /// can fail.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    ///
    /// use tinio_core::{
    ///     acl::Acl,
    ///     bucket, object,
    ///     storage::{BucketOps, ObjectOps},
    /// };
    /// use tinio_fs::testing;
    /// use tinio_store_redb::RedbContent;
    /// use tinio_tier::{Config, FlushMode, TierStorage};
    /// use tokio::runtime::Runtime;
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let tier = TierStorage::open(
    ///     root.path(),
    ///     testing::fs_options(),
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
    ///     RedbContent::in_memory().expect("an in-memory cold plane"),
    /// )
    /// .expect("the tier layer opens");
    /// Runtime::new().expect("a runtime").block_on(async {
    ///     let name = bucket::name("data").expect("a valid bucket name");
    ///     tier.create_bucket(&name, None, &Acl::default_private(None))
    ///         .await
    ///         .expect("the bucket is created");
    ///     let key = object::key("a.bin").expect("a valid key");
    ///     tier.put_object(&name, &key, tinio_util::testing::body(b"hi"))
    ///         .await
    ///         .expect("the write is acknowledged");
    ///     assert!(!root.path().join("data").join("a.bin").exists());
    ///     tier.shutdown().await.expect("the buffer drains");
    ///     assert!(root.path().join("data").join("a.bin").exists());
    /// });
    /// ```
    pub async fn shutdown(&self) -> Result<(), Error> {
        self.flush_all().await
    }

    /// Drain the batch buffer now — the periodic flush, without the
    /// end-of-run meaning [`TierStorage::shutdown`] carries.
    ///
    /// # Errors
    ///
    /// The same contract as [`TierStorage::shutdown`]: every entry is
    /// published or quarantined, so the call answers `Ok` today.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    ///
    /// use tinio_core::{
    ///     acl::Acl,
    ///     bucket, object,
    ///     storage::{BucketOps, ObjectOps},
    /// };
    /// use tinio_fs::testing;
    /// use tinio_store_redb::RedbContent;
    /// use tinio_tier::{Config, FlushMode, TierStorage};
    /// use tokio::runtime::Runtime;
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let tier = TierStorage::open(
    ///     root.path(),
    ///     testing::fs_options(),
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
    ///     RedbContent::in_memory().expect("an in-memory cold plane"),
    /// )
    /// .expect("the tier layer opens");
    /// Runtime::new().expect("a runtime").block_on(async {
    ///     let name = bucket::name("data").expect("a valid bucket name");
    ///     tier.create_bucket(&name, None, &Acl::default_private(None))
    ///         .await
    ///         .expect("the bucket is created");
    ///     let key = object::key("a.bin").expect("a valid key");
    ///     tier.put_object(&name, &key, tinio_util::testing::body(b"hi"))
    ///         .await
    ///         .expect("the write is acknowledged");
    ///     assert!(!root.path().join("data").join("a.bin").exists());
    ///     tier.flush().await.expect("the buffer drains");
    ///     assert!(root.path().join("data").join("a.bin").exists());
    /// });
    /// ```
    pub async fn flush(&self) -> Result<(), Error> {
        self.flush_all().await
    }

    /// The read counters the promotion pass consumes.
    pub fn heat(&self) -> &Heat {
        &self.heat
    }

    /// The cold plane's content handle.
    pub fn cold(&self) -> &C {
        &self.cold
    }

    /// Create `bucket`'s namespace on the cold plane — the demote path's first
    /// step, so a bucket costs a namespace only once something is demoted into
    /// it (T18).
    ///
    /// The call is idempotent: a namespace that already exists is left as it
    /// is. The reserved namespace is never a demote target.
    ///
    /// # Errors
    ///
    /// [`Error::Storage`] with the access-denied class for the reserved
    /// namespace, and the cold plane's own failure projected through
    /// [`Error::Storage`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::bucket;
    /// use tinio_fs::testing;
    /// use tinio_store_redb::RedbContent;
    /// use tinio_tier::{Config, FlushMode, TierStorage};
    /// use tokio::runtime::Runtime;
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let tier = TierStorage::open(
    ///     root.path(),
    ///     testing::fs_options(),
    ///     Config {
    ///         capacity_bytes: 1024 * 1024,
    ///         max_object_bytes: 64 * 1024,
    ///         flush_mode: FlushMode::Sync,
    ///         batch_bytes: 8 * 1024,
    ///         batch_interval: std::time::Duration::from_secs(1),
    ///         heat_window: std::time::Duration::from_secs(60),
    ///         promote_reads: 3,
    ///         stats_flush: std::time::Duration::from_secs(10),
    ///         high_water_percent: 90,
    ///         low_water_percent: 80,
    ///         demote_idle: std::time::Duration::from_secs(3600),
    ///         dwell: std::time::Duration::from_secs(3600),
    ///         scan_interval: std::time::Duration::from_secs(60),
    ///         move_concurrency: 1,
    ///         index_compact_percent: 30,
    ///         gc_grace: std::time::Duration::from_secs(300),
    ///     },
    ///     RedbContent::in_memory().expect("an in-memory cold plane"),
    /// )
    /// .expect("the tier layer opens");
    /// Runtime::new().expect("a runtime").block_on(async {
    ///     let name = bucket::name("data").expect("a valid bucket name");
    ///     tier.ensure_cold_bucket(&name)
    ///         .await
    ///         .expect("the namespace is created");
    ///     tier.ensure_cold_bucket(&name)
    ///         .await
    ///         .expect("a second demote is a no-op");
    /// });
    /// ```
    pub async fn ensure_cold_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        Self::refuse_reserved(bucket)?;
        self.cold
            .create_bucket(bucket)
            .await
            .map_err(|err| Error::Storage(err.into()))?;
        Ok(())
    }

    /// Refuse the reserved namespace at every operation's entry (T18).
    pub(crate) fn refuse_reserved(name: &bucket::Name) -> Result<(), Error> {
        if pg::is_reserved(name) {
            return Err(storage::access_denied_bucket(name).into());
        }
        Ok(())
    }

    /// Take the bucket read gate a listing holds for its whole duration
    /// (T14).
    pub(crate) async fn gate_bucket_read(&self, bucket: &bucket::Name) -> BucketReadGuard {
        self.gates.read(bucket).await
    }

    /// The one seam a client listing pauses at before it reads the planes
    /// (T8, T11): the batch buffer's flush lands here. The metrics scan takes
    /// the merge below directly, so it never flushes.
    pub(crate) async fn before_client_listing(&self) -> Result<(), Error> {
        self.flush_all().await
    }

    /// The cold plane's content handle.
    pub(crate) fn fs_content(&self) -> FsContent {
        self.fs.content()
    }

    /// The object's settled plane, reconciled against the warm row (T5).
    ///
    /// A key with no placement, or with a placement the layer did not settle
    /// in the cold plane, is the warm plane's. A cold placement is checked
    /// against the row and the file: no row means the object was deleted, a
    /// file with a different ETag means a completed overwrite (`fs` wins),
    /// a file with the same ETag is a crash before the file's removal
    /// (served cold, the file left to garbage collection), and no file is
    /// the settled cold object.
    ///
    /// Only the deleted outcome reconciles the stored state, because T5
    /// assigns "clear placement/pg" to it alone. An overwrite leaves the
    /// placement and the cold copy untouched — canonical reconciliation is
    /// the write path's reset and the garbage pass's; the read only drops the
    /// cached bytes it can no longer trust.
    async fn resolve(&self, bucket: &bucket::Name, key: &object::Key) -> Result<Plane, Error> {
        let Some(placement) = self.index.placement(bucket, key)? else {
            return Ok(Plane::Warm);
        };
        if !placement.is_cold_live() {
            return Ok(Plane::Warm);
        }
        let Some(cold) = placement.cold.as_ref() else {
            return Err(Error::Storage(storage::corrupt(
                "a pg placement carries no cold metadata",
            )));
        };
        let info = cold.to_info()?;
        let Some(facts) = self.rows.facts(bucket, key).await? else {
            self.retire(bucket, key, Some(cold.last_modified)).await;
            return Ok(Plane::Deleted);
        };
        if self.warm_won_overwrite(bucket, key, &facts, &info).await? {
            self.cache.remove(bucket, key);
            return Ok(Plane::Warm);
        }
        Ok(Plane::Cold(info))
    }

    /// The T5 overwrite test shared by the read and both promotion halves: the
    /// warm file exists and its row names a different object than the cold
    /// snapshot, so a completed warm write already won.
    ///
    /// Deliberately ETag-only: the question is whether a *different* object
    /// won the warm plane, so size and time cannot make a same-ETag file lose.
    /// The full snapshot identity is [`row_matches_snapshot`].
    async fn warm_won_overwrite(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        facts: &tier::Facts,
        cold: &object::Info,
    ) -> Result<bool, Error> {
        Ok(facts.etag != cold.etag && self.fs_content().stat(bucket, key).await?.is_some())
    }

    /// Finish a deleted object: drop the dead placement, the cached bytes, the
    /// warm candidate recorded at `warm`, the heat row and the cold copy.
    ///
    /// A failure is reported and never turns the read's `NoSuchKey` into an
    /// I/O failure: the object is gone either way, and the leftover is the
    /// garbage pass's.
    pub(crate) async fn retire(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        warm: Option<SystemTime>,
    ) {
        if let Err(err) = self
            .remove_traces(
                bucket,
                key,
                Removal {
                    warm,
                    cold: ColdRemoval::BestEffort,
                },
            )
            .await
        {
            tracing::warn!(error = %err, "tier reconcile: the removal was incomplete");
        }
    }

    /// Drop every trace of `key` a mutation replaces: the cached bytes, the
    /// buffered write, the in-memory heat counter, its queued promotion and the
    /// durable heat row (T9, T16).
    pub(crate) fn invalidate(&self, bucket: &bucket::Name, key: &object::Key) {
        self.cache.remove(bucket, key);
        self.buffer.remove(bucket, key);
        self.invalidate_heat(bucket, key);
    }

    /// Drop `key`'s in-memory heat counter, its queued promotion and its
    /// durable heat row (T9).
    ///
    /// The bytes-invalidating reset and the buffered flush share this half of
    /// [`Self::invalidate`]; the flush keeps its own buffer entry until it
    /// publishes it.
    ///
    /// Residual race: `Mover::flush_stats` snapshots the counters, absorbs
    /// them, and only then forgets what the index accepted. A snapshot taken
    /// before this call can therefore re-absorb a counter this call removed and
    /// recreate the durable row after `remove_heat` deleted it. The window is
    /// one flush pass and the next mutation of the key closes it again; the
    /// spec asks for no lock between a write and the flush, so the race is
    /// documented rather than serialized away.
    fn invalidate_heat(&self, bucket: &bucket::Name, key: &object::Key) {
        self.heat.forget_key(bucket, key);
        // The in-memory counter and the queued promotion are gone above; the
        // durable row is deleted only when there is one to delete. An
        // unreadable row is assumed present, so the delete is attempted as
        // before rather than leaving a stale row behind.
        let has_row = match self.index.heat_last_read(bucket, key) {
            Ok(row) => row.is_some(),
            Err(err) => {
                tracing::warn!(error = %err, "tier write: the heat row was not read");
                true
            }
        };
        if has_row && let Err(err) = self.index.remove_heat(bucket, key) {
            tracing::warn!(error = %err, "tier write: the heat row was not removed");
        }
    }

    /// Serve a warm object, answering from the cache on a full-object hit.
    ///
    /// Both the cache hit and the filesystem read are the warm plane serving
    /// the object, so both book one warm hit and the bytes served.
    async fn serve_warm(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        range: Option<ByteRange>,
    ) -> Result<GetObjectResult, Error> {
        self.heat.record_read(bucket, key);
        if range.is_none()
            && let Some(cached) = self.cache.entry(bucket, key)
        {
            match cached.info {
                // The entry carries the metadata it was filled from, and every
                // mutation invalidates the cache, so a hit needs no
                // `head_object` round trip: the object identity the entry was
                // filled for is the one the object still has.
                Some(info) if info.size == cached.data.len() as u64 => {
                    let served = GetObjectResult {
                        info: (*info).clone(),
                        body: bytes_body(cached.data),
                        served_range: None,
                    };
                    record_served(TierPlane::Warm, &served);
                    return Ok(served);
                }
                Some(_) => {
                    // A self-inconsistent entry: drop it and read the file.
                    self.cache.remove(bucket, key);
                }
                // An entry admitted without metadata (the bytes-only cache
                // seam) still needs the file's identity to answer.
                None => match self.fs.head_object(bucket, key).await {
                    Ok(info) if info.size == cached.data.len() as u64 => {
                        let served = GetObjectResult {
                            info,
                            body: bytes_body(cached.data),
                            served_range: None,
                        };
                        record_served(TierPlane::Warm, &served);
                        return Ok(served);
                    }
                    Ok(_) => {
                        self.cache.remove(bucket, key);
                    }
                    Err(err) => {
                        self.cache.remove(bucket, key);
                        // Only an absent object is a miss; an invalid range or
                        // an I/O failure is a failed read.
                        return Err(warm_error(err));
                    }
                },
            }
        }
        match self.fs.get_object(bucket, key, range).await {
            Ok(served) => {
                record_served(TierPlane::Warm, &served);
                Ok(served)
            }
            Err(err) => Err(warm_error(err)),
        }
    }

    /// Serve a cold object: verify its copy, fill the cache only on a full
    /// GET (T6), and answer the cold authority's metadata (T2).
    ///
    /// The whole-object cache is the warm plane's memory tier, so a cache hit
    /// books a warm hit even when the object's placement is cold. Only a
    /// cache miss falls through to the cold plane, which books the warm miss
    /// and then its own hit or miss.
    async fn serve_cold(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        info: object::Info,
        range: Option<ByteRange>,
    ) -> Result<GetObjectResult, Error> {
        self.heat.record_read(bucket, key);
        if range.is_none()
            && let Some(bytes) = self.cache.get(bucket, key)
        {
            if bytes.len() as u64 == info.size {
                let served = GetObjectResult {
                    info,
                    body: bytes_body(bytes),
                    served_range: None,
                };
                record_served(TierPlane::Warm, &served);
                return Ok(served);
            }
            self.cache.remove(bucket, key);
        }
        record_miss(TierPlane::Warm);
        let outcome = self.serve_cold_plane(bucket, key, info, range).await;
        match &outcome {
            Ok(served) => record_served(TierPlane::Cold, served),
            // A missing or short cold copy is a miss; an invalid range or an
            // I/O failure is a failed read.
            Err(err) if tier_miss(err) => record_miss(TierPlane::Cold),
            Err(_) => {}
        }
        outcome
    }

    /// The cold plane's half of [`Self::serve_cold`]: open the copy's body,
    /// check the length of a full GET it collects and fill the cache (T6).
    async fn serve_cold_plane(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        info: object::Info,
        range: Option<ByteRange>,
    ) -> Result<GetObjectResult, Error> {
        // `open` is the length's source: it re-reads the live generation's
        // head, so a separate `stat` would repeat that query. The bytes the
        // open yields are what a full body is checked against below.
        let served_range = range.map(|range| range.resolve(info.size)).transpose()?;
        let body = self
            .cold
            .open(bucket, &ContentKey::Object(key.clone()), range)
            .await
            .map_err(cold_failure)?;
        if range.is_none() && info.size <= self.config.max_object_bytes {
            let bytes = Bytes::from(collect_body(body).await.map_err(storage::io)?);
            if bytes.len() as u64 != info.size {
                return Err(Error::ColdLength {
                    key: key.clone(),
                    expected: info.size,
                    actual: bytes.len() as u64,
                });
            }
            self.cache
                .insert_with_info(bucket, key, info.clone(), bytes.clone());
            return Ok(GetObjectResult {
                info,
                body: bytes_body(bytes),
                served_range: None,
            });
        }
        Ok(GetObjectResult {
            info,
            body,
            served_range,
        })
    }

    /// Take the read side of a key's lock and hold it through the body.
    async fn locked_read(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        range: Option<ByteRange>,
    ) -> Result<GetObjectResult, Error> {
        self.flush_key(bucket, key).await?;
        let guard = self.locks.read(bucket, key).await;
        let got = match self.resolve(bucket, key).await? {
            Plane::Deleted => {
                // The resolve found a cold placement whose row is gone: the
                // warm plane answered "absent", and the cold plane was never
                // attempted, so this is one attempted-tier miss.
                record_miss(TierPlane::Warm);
                Err(storage::no_such_key(key).into())
            }
            Plane::Warm => self.serve_warm(bucket, key, range).await,
            Plane::Cold(info) => self.serve_cold(bucket, key, info, range).await,
        }?;
        Ok(GetObjectResult {
            body: guarded(got.body, guard),
            ..got
        })
    }

    /// Whether the bucket records versions — the rule that decides whether a
    /// null version address is the layer's own or the version engine's.
    pub(crate) async fn records_versions(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        Ok(self.fs.bucket_versioning(bucket).await?.records_versions())
    }

    /// Stage a write that requests `class` — the class-aware streaming phase
    /// (T7).
    ///
    /// [`ObjectOps::stage_body`](tinio_core::storage::ObjectOps::stage_body)
    /// delegates here with [`Class::Standard`]. A non-`STANDARD` class is
    /// never batch-eligible: the body streams through the filesystem stage,
    /// because a classed write is demoted before the response is answered and
    /// must not be waiting in the memory buffer when it is. Batch mode, an
    /// unversioned bucket and the standard class are the staging
    /// preconditions; the commit applies the request-shaped ones (tags,
    /// owner, ACL and lock).
    ///
    /// # Errors
    ///
    /// The body stream's I/O failure, and the filesystem stage's validation
    /// or I/O failure.
    ///
    /// # Examples
    ///
    /// A classed write streams even in batch mode:
    ///
    /// ```rust
    /// use std::time::Duration;
    ///
    /// use futures::stream;
    /// use tinio_core::{
    ///     Class,
    ///     acl::Acl,
    ///     bucket, object,
    ///     storage::{BodyStream, BucketOps},
    /// };
    /// use tinio_fs::testing;
    /// use tinio_store_redb::RedbContent;
    /// use tinio_tier::{Config, FlushMode, TierStaged, TierStorage};
    /// use tokio::runtime::Runtime;
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let tier = TierStorage::open(
    ///     root.path(),
    ///     testing::fs_options(),
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
    ///     RedbContent::in_memory().expect("an in-memory cold plane"),
    /// )
    /// .expect("the tier layer opens");
    /// Runtime::new().expect("a runtime").block_on(async {
    ///     let name = bucket::name("data").expect("a valid bucket name");
    ///     tier.create_bucket(&name, None, &Acl::default_private(None))
    ///         .await
    ///         .expect("the bucket is created");
    ///     let key = object::key("classed.bin").expect("a valid key");
    ///     let body: BodyStream = Box::pin(stream::iter([Ok(bytes::Bytes::from_static(b"hi"))]));
    ///     let staged = tier
    ///         .stage_body_with_class(&name, &key, body, None, None, Class::StandardIa)
    ///         .await
    ///         .expect("the body stages");
    ///     assert!(matches!(staged, TierStaged::Fs(_)), "classed writes stream");
    /// });
    /// ```
    pub async fn stage_body_with_class(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        body: BodyStream,
        tee: Option<Arc<checksum::PartChecksum>>,
        carried: Option<checksum::Recorded>,
        class: Class,
    ) -> Result<TierStaged, Error> {
        Self::refuse_reserved(bucket)?;
        if self.batch_eligible(bucket, class).await? {
            stage_bounded(
                &self.fs,
                self.config.max_object_bytes,
                bucket,
                key,
                body,
                tee,
                carried,
            )
            .await
        } else {
            Ok(TierStaged::Fs(
                self.fs.stage_body(bucket, key, body, tee, carried).await?,
            ))
        }
    }

    /// Whether a write with `class` may be staged in the bounded memory
    /// buffer: batch mode, an unversioned bucket and no storage class (T7).
    async fn batch_eligible(&self, bucket: &bucket::Name, class: Class) -> Result<bool, Error> {
        if self.config.flush_mode != FlushMode::Batch || class != Class::Standard {
            return Ok(false);
        }
        Ok(!self.records_versions(bucket).await?)
    }

    /// Refuse a null-version address whose cold placement names a real
    /// identity (T6): on an unversioned bucket the address must match the
    /// placement's version, which is the null version.
    ///
    /// The check runs on the reconciled plane, so a placement the read would
    /// reconcile away cannot refuse the address it no longer owns.
    async fn ensure_null_placement(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<(), Error> {
        match self.resolve(bucket, key).await? {
            Plane::Cold(info) => match info.version_id {
                None => Ok(()),
                Some(id) => Err(storage::no_such_version(id.to_string()).into()),
            },
            Plane::Warm | Plane::Deleted => Ok(()),
        }
    }

    /// Whether an absent `version` addresses the layer's own null object: the
    /// address belongs to the layer only on a bucket that records no versions.
    async fn null_object_address(
        &self,
        bucket: &bucket::Name,
        version: Option<&version::Id>,
    ) -> Result<bool, Error> {
        Ok(version.is_none() && !self.records_versions(bucket).await?)
    }

    /// Whether `version` names the layer's own null object, after refusing a
    /// cold placement that names a real identity (T6).
    ///
    /// `false` leaves the address to the filesystem's version engine. A delete
    /// uses [`Self::null_object_address`] instead: the address routes to a
    /// plain delete without a read-side placement guard.
    async fn null_address_route(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&version::Id>,
    ) -> Result<bool, Error> {
        if !self.null_object_address(bucket, version).await? {
            return Ok(false);
        }
        self.ensure_null_placement(bucket, key).await?;
        Ok(true)
    }

    /// Whether `key`'s index placement says a live cold plane owns the object.
    ///
    /// The subresource reads that the filesystem row answers (retention, legal
    /// hold and the lock read) classify by this flag instead of resolving the
    /// key: booking a metric must never make a read reconcile or retire state.
    fn cold_placed(&self, bucket: &bucket::Name, key: &object::Key) -> Result<bool, Error> {
        Ok(self
            .index
            .placement(bucket, key)?
            .is_some_and(|placement| placement.is_cold_live()))
    }

    /// One metadata read's booking and plane dispatch.
    ///
    /// A deleted key books the one attempted-tier miss and answers
    /// `NoSuchKey`; a warm read books through [`warm_meta`]; a cold hit books
    /// the warm miss and the cold hit and answers the placement's metadata
    /// through `cold`. The warm read is a future, so it runs only on the warm
    /// arm and a cold or deleted answer pays no filesystem call.
    async fn book_metadata<T>(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        warm: impl Future<Output = Result<T, _fs::Error>>,
        cold: impl FnOnce(object::Info) -> T,
    ) -> Result<T, Error> {
        match self.resolve(bucket, key).await? {
            Plane::Deleted => {
                record_miss(TierPlane::Warm);
                Err(storage::no_such_key(key).into())
            }
            Plane::Warm => warm_meta(warm.await),
            Plane::Cold(info) => {
                record_miss(TierPlane::Warm);
                record_hit(TierPlane::Cold);
                Ok(cold(info))
            }
        }
    }

    /// Whether `key`'s current placement carries a cold copy: the gate a warm
    /// reset or flush reads before removing one.
    ///
    /// A key that was never cold pays no cold-plane call. An unreadable
    /// placement is assumed to carry a copy, so the remove is attempted as
    /// before rather than leaving an orphan behind.
    fn placement_carries_cold(&self, bucket: &bucket::Name, key: &object::Key) -> bool {
        match self.index.placement(bucket, key) {
            Ok(placement) => placement.is_some_and(|placement| placement.cold.is_some()),
            Err(err) => {
                tracing::warn!(error = %err, "tier write: the placement was not read");
                true
            }
        }
    }

    /// Reset `key` to the warm plane after a byte-producing write (T16).
    ///
    /// The destination's placement becomes a live warm placement, its cached
    /// bytes are dropped and any cold copy is removed. The caller holds the
    /// key's write lock, so no read interleaves the reset with the write that
    /// produced the bytes.
    ///
    /// The warm file already answers the read after the write, so a cold
    /// removal that fails leaves an orphan for the garbage pass rather than
    /// failing the request that has already published its bytes.
    pub(crate) async fn reset_to_fs(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        mtime: SystemTime,
    ) -> Result<(), Error> {
        // The placement the reset replaces is the only evidence a cold copy
        // exists: a key that was never cold pays no cold-plane remove.
        let had_cold = self.placement_carries_cold(bucket, key);
        // Every warm placement is a capacity candidate for the mover (T21); a
        // folder marker is a directory and is never moved. The placement and
        // the candidate land in one index transaction — one fsync, and the
        // reset never observes half of its own state.
        let warm = (!key.is_folder_marker()).then_some(mtime);
        self.index.set_placement_and_warm(
            bucket,
            key,
            &Placement {
                tier: Tier::Fs,
                state: State::Live,
                cold: None,
                mtime,
            },
            warm,
        )?;
        self.invalidate(bucket, key);
        self.discard_cold(bucket, key, had_cold).await;
        Ok(())
    }

    /// Remove `key`'s cold copy when `had_cold`, reporting a failure without
    /// failing the caller: the warm plane is already the object's plane, so the
    /// leftover is garbage collection's.
    ///
    /// `had_cold` is the caller's read of the placement before the reset or
    /// flush changed it; the remove never re-reads the index, so a placement
    /// the caller already replaced or cleared cannot hide the copy.
    pub(crate) async fn discard_cold(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        had_cold: bool,
    ) {
        if !had_cold {
            return;
        }
        if let Err(err) = self
            .cold
            .remove(bucket, &ContentKey::Object(key.clone()))
            .await
        {
            tracing::warn!(error = %err, "tier write: the cold copy was not removed");
        }
    }

    /// Remove every trace of `key` a finished removal owns: the placement, the
    /// warm candidate recorded at `warm`, the cached bytes, the buffered
    /// write, the heat counters and — when the path owns it — the cold copy.
    ///
    /// The one removal helper, so a new path cannot forget the warm candidate
    /// or the heat row. A [`ColdRemoval::Strict`] copy is removed first, so a
    /// failure leaves the placement for a retry; a [`ColdRemoval::BestEffort`]
    /// copy is removed last and only logged, because the warm plane already
    /// answers the read.
    pub(crate) async fn remove_traces(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        removal: Removal,
    ) -> Result<(), Error> {
        if removal.cold == ColdRemoval::Strict {
            self.cold
                .remove(bucket, &ContentKey::Object(key.clone()))
                .await
                .map_err(cold_failure)?;
        }
        self.index.clear(bucket, key)?;
        if let Some(mtime) = removal.warm {
            self.index.warm_remove(bucket, key, mtime)?;
        }
        self.invalidate(bucket, key);
        if removal.cold == ColdRemoval::BestEffort {
            self.discard_cold(bucket, key, true).await;
        }
        Ok(())
    }
}

/// How a removal path owns the key's cold copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ColdRemoval {
    /// The path does not own a cold copy; remove nothing.
    None,
    /// Remove the copy before the placement is cleared. A failure fails the
    /// caller and leaves the placement for a retry (the delete and tombstone
    /// paths).
    Strict,
    /// The bytes are already unreachable; remove the copy last and report a
    /// failure without failing the caller (the read, promotion, rename and
    /// collection reconciles).
    BestEffort,
}

/// What a removal path owns besides the key's placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Removal {
    /// The warm candidate's recorded time, when the path knows it.
    pub(crate) warm: Option<SystemTime>,
    /// How the cold copy is removed.
    pub(crate) cold: ColdRemoval,
}

/// The warm bytes a promotion staged before it took the key's write lock
/// (T13, T14).
///
/// The copy runs outside the lock; the finalize re-reads the placement and
/// the row, so a write or a move that settled the key while the bytes were
/// copying owns the key. Dropping a [`PreparedPromotion::Staged`] removes its
/// staged temp.
///
/// The timer [`MoveTimer`] the preparation started travels with the result and
/// drops at the end of [`TierStorage::finish_promotion`], so the booked
/// promotion duration covers the publish and the finalize, exactly as a
/// demote's timer covers its own.
pub(crate) enum PreparedPromotion {
    /// The key was not cold when the copy was prepared: the finalize has
    /// nothing to do, and no promotion was attempted, so no timer started.
    NotCold,
    /// No copy is needed — the row is gone or the warm file already won — but
    /// the finalize still settles the placement under the lock.
    Settle {
        /// The placement the attempt was prepared against.
        placement: Placement,
        /// The attempted promotion's timer.
        timer: MoveTimer,
    },
    /// The cold bytes are staged, with the placement snapshot they describe.
    Staged {
        /// The placement the copy was prepared against.
        placement: Placement,
        /// The staged warm copy.
        staged: Box<FsStaged>,
        /// The attempted promotion's timer.
        timer: MoveTimer,
    },
}

impl<C: ObjectContent> TierStorage<C> {
    /// Stage a cold object's copy on the warm plane's temp without holding
    /// the key's lock (T13, T14).
    ///
    /// Answers [`PreparedPromotion::NotCold`] when the key has no live pg
    /// placement, [`PreparedPromotion::Settle`] when its row is gone or its
    /// warm file already won (T5), and [`PreparedPromotion::Staged`] with the
    /// copy otherwise. The caller takes the key's write side and calls
    /// [`Self::finish_promotion`]; dropping the result discards the staged
    /// bytes.
    ///
    /// # Errors
    ///
    /// The cold open/copy failure (a missing copy or a length mismatch is
    /// reported by name) and the filesystem stage failure.
    pub(crate) async fn prepare_promotion(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<PreparedPromotion, Error> {
        let Some(placement) = self.index.placement(bucket, key)? else {
            return Ok(PreparedPromotion::NotCold);
        };
        if !placement.is_cold_live() {
            return Ok(PreparedPromotion::NotCold);
        }
        let Some(cold) = placement.cold.as_ref() else {
            return Err(Error::Storage(storage::corrupt(
                "a pg placement carries no cold metadata",
            )));
        };
        let info = cold.to_info()?;
        // Every sample from here on is an attempted promotion: a copy that
        // fails or is abandoned books its duration too, while the move counter
        // is booked only when the bytes actually land warm. The timer is
        // carried into the finalize so the whole move is measured.
        let timer = MoveTimer::new(MoveDirection::Promote);
        let Some(facts) = self.rows.facts(bucket, key).await? else {
            // The row is gone: the finalize retires the stale placement.
            return Ok(PreparedPromotion::Settle { placement, timer });
        };
        if self.warm_won_overwrite(bucket, key, &facts, &info).await? {
            // A completed overwrite already won on the warm plane (T5); the
            // finalize resets the placement without a copy.
            return Ok(PreparedPromotion::Settle { placement, timer });
        }
        let content_key = ContentKey::Object(key.clone());
        let content = self.fs_content();
        let mut staged = content.stage(bucket, &content_key).await?;
        let body = self
            .cold
            .open(bucket, &content_key, None)
            .await
            .map_err(cold_failure)?;
        // The opened body carries the copy's length: the one pump counts what
        // lands, so the recorded-size check needs no separate stat.
        let landed = stage_body(&mut staged, body).await?;
        if landed != info.size {
            return Err(Error::ColdLength {
                key: key.clone(),
                expected: info.size,
                actual: landed,
            });
        }
        Ok(PreparedPromotion::Staged {
            placement,
            staged: Box::new(staged),
            timer,
        })
    }

    /// Finish a promotion under the key's write lock (T13, T14).
    ///
    /// The caller holds the key's write lock. The placement and the row are
    /// re-read under it: a key another write or move settled while the copy
    /// ran is left alone and answers `false`. The staged copy publishes, the
    /// file is stamped with the cold authority's modification time, the row is
    /// restored, the placement becomes warm and the cold copy is removed.
    /// Answers whether a promotion happened.
    ///
    /// # Errors
    ///
    /// The filesystem publish or stamp failure, the row write, and the index
    /// writes.
    pub(crate) async fn finish_promotion(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        prepared: PreparedPromotion,
    ) -> Result<bool, Error> {
        let (snapshot, staged, _timer) = match prepared {
            PreparedPromotion::NotCold => return Ok(false),
            PreparedPromotion::Settle { placement, timer } => (placement, None, timer),
            PreparedPromotion::Staged {
                placement,
                staged,
                timer,
            } => (placement, Some(staged), timer),
        };
        let Some(current) = self.index.placement(bucket, key)? else {
            return Ok(false);
        };
        if current != snapshot {
            // A write, a move or a delete settled the key while the bytes
            // copied: it owns the key now.
            return Ok(false);
        }
        let Some(cold) = snapshot.cold.as_ref() else {
            return Err(Error::Storage(storage::corrupt(
                "a pg placement carries no cold metadata",
            )));
        };
        let info = cold.to_info()?;
        let Some(facts) = self.rows.facts(bucket, key).await? else {
            // The row is gone: the object was deleted out from under the
            // placement. Retire the stale placement exactly as a read would.
            self.retire(bucket, key, Some(cold.last_modified)).await;
            return Ok(false);
        };
        if self.warm_won_overwrite(bucket, key, &facts, &info).await? {
            // A completed overwrite already won on the warm plane (T5); the
            // cold copy is the stale half of the crash.
            self.reset_to_fs(bucket, key, facts.mtime).await?;
            return Ok(false);
        }
        let Some(staged) = staged else {
            return Ok(false);
        };
        let content_key = ContentKey::Object(key.clone());
        self.fs_content()
            .publish(bucket, *staged, &content_key)
            .await?;
        self.set_file_mtime(bucket, key, cold.last_modified).await?;
        self.rows.set_row(bucket, &info, cold.last_modified).await?;
        self.index.set_placement(
            bucket,
            key,
            &Placement {
                tier: Tier::Fs,
                state: State::Live,
                cold: None,
                mtime: cold.last_modified,
            },
        )?;
        self.index.warm_replace(bucket, key, cold.last_modified)?;
        // The settled snapshot carried the cold authority (checked above), so
        // this promotion always has a cold copy to drop.
        self.discard_cold(bucket, key, true).await;
        // The migration point: the bytes changed plane, so this is one
        // promotion — whoever drove it (the mover, a copy, a rename or the
        // versioning pass).
        #[cfg(test)]
        let _gate = test_lock::writer();
        record_tier_move(MoveDirection::Promote);
        Ok(true)
    }

    /// Stamp the promoted file with the cold authority's modification time
    /// (T4, T15): the publish wrote the object's bytes under the fresh
    /// instant, while the object's recorded time is the one no move may
    /// change.
    ///
    /// The path is recomposed here and opened under the warm plane's symlink
    /// policy, so a link planted at the object path cannot redirect the
    /// stamp outside the storage root.
    pub(crate) async fn set_file_mtime(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        mtime: SystemTime,
    ) -> Result<(), Error> {
        let bucket_dir = path::bucket_path(self.fs.root(), bucket)?;
        let file = path::key_path(&bucket_dir, key, false)?;
        let handle = self.open_mutation(&file).await?;
        task::spawn_blocking(move || handle.set_modified(mtime))
            .await
            .map_err(|err| Error::Io(io::Error::other(err)))??;
        Ok(())
    }

    /// Stamp an object's warm file with `mtime` — the `testing` feature's
    /// seam for the symlink-policy test.
    ///
    /// # Errors
    ///
    /// The same failures as `Self::set_file_mtime`.
    #[cfg(feature = "testing")]
    pub async fn set_file_mtime_for_test(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        mtime: SystemTime,
    ) -> Result<(), Error> {
        self.set_file_mtime(bucket, key, mtime).await
    }

    /// Open `file` for a metadata mutation under the warm plane's symlink
    /// policy (T15).
    ///
    /// With following disabled, every existing component from the file up to
    /// (and excluding) the storage root is probed and a symlink — or, on
    /// Windows, any reparse point — refuses the write; the leaf is then
    /// opened no-follow, so a link swapped in between the walk and the open
    /// cannot escape the root either. With following enabled the policy is
    /// off and the open follows, exactly as the filesystem backend's own
    /// object operations do.
    async fn open_mutation(&self, file: &Path) -> Result<fs::File, Error> {
        let follow = *self.fs.follow_symlinks();
        if !follow {
            let root = self.fs.root();
            let mut current = file.to_path_buf();
            loop {
                match async_fs::symlink_metadata(&current).await {
                    Ok(metadata) if _fs::is_symlink_or_reparse(&metadata) => {
                        return Err(Error::Io(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "symlink",
                        )));
                    }
                    Ok(_) => {}
                    // A missing component is not an escape: the parents a
                    // caller composes may not exist yet.
                    Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                    Err(err) => return Err(Error::Io(err)),
                }
                if current == *root || !current.pop() {
                    break;
                }
            }
        }
        let file = file.to_path_buf();
        task::spawn_blocking(move || open_mutation_file(&file, follow))
            .await
            .map_err(|err| Error::Io(io::Error::other(err)))?
            .map_err(Error::Io)
    }

    /// Delete `key` under the write lock, with the tombstone garbage
    /// collection finishes after a crash (T13).
    ///
    /// The tombstone is written before the delete starts. A failure settles
    /// the placement against the row the delete left: the previous placement
    /// is restored while the row survives, and the `Deleting` tombstone stays
    /// written once the row is gone. A success clears the row, the placement
    /// and the cold copy.
    ///
    /// The tombstone's recorded time is the instant the delete started, not
    /// the object's own modification time: garbage collection finishes only
    /// tombstones older than the grace (T13), so the two clocks must not be
    /// confused.
    pub(crate) async fn delete_locked(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<version::Marker, Error> {
        let previous = self.index.placement(bucket, key)?;
        // The warm candidate is keyed by the row's recorded time; read it
        // before the delete removes the row.
        let warm_mtime = self.rows.facts(bucket, key).await?.map(|facts| facts.mtime);
        let tombstone = Placement {
            tier: previous
                .as_ref()
                .map_or(Tier::Fs, |placement| placement.tier),
            state: State::Deleting,
            cold: previous
                .as_ref()
                .and_then(|placement| placement.cold.clone()),
            mtime: SystemTime::now(),
        };
        self.index.set_placement(bucket, key, &tombstone)?;
        let settled = async {
            let marker = self.fs.delete_object(bucket, key).await?;
            self.remove_traces(
                bucket,
                key,
                Removal {
                    warm: warm_mtime,
                    cold: ColdRemoval::Strict,
                },
            )
            .await?;
            Ok::<_, Error>(marker)
        }
        .await;
        match settled {
            Ok(marker) => Ok(marker),
            Err(err) => {
                if let Err(settle) = self
                    .settle_failed_delete(bucket, key, previous.as_ref(), &tombstone, warm_mtime)
                    .await
                {
                    tracing::error!(
                        error = %settle,
                        "tier delete: the placement was not settled"
                    );
                }
                Err(err)
            }
        }
    }

    /// Settle a failed delete's placement (T13).
    ///
    /// A failure before the object's row disappears rolls the tombstone back
    /// to the placement it replaced. A failure after the row is gone leaves
    /// the tombstone in place: a restored live placement over an absent row is
    /// invisible to reads and to garbage collection, while a `Deleting` row is
    /// what the garbage pass finishes.
    ///
    /// Only an absent row is "gone": a facts read that fails is propagated, so
    /// a transient metadata failure never deletes a surviving object's warm
    /// candidate.
    async fn settle_failed_delete(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        previous: Option<&Placement>,
        tombstone: &Placement,
        warm_mtime: Option<SystemTime>,
    ) -> Result<(), Error> {
        let row_survives = self.rows.facts(bucket, key).await?.is_some();
        if row_survives {
            match previous {
                Some(placement) => self.index.set_placement(bucket, key, placement)?,
                None => self.index.clear(bucket, key)?,
            }
        } else {
            self.index.set_placement(bucket, key, tombstone)?;
            // The row is gone, so a warm candidate can never become a
            // demotion: retire it while its recorded time is still known.
            if let Some(mtime) = warm_mtime {
                self.index.warm_remove(bucket, key, mtime)?;
            }
        }
        Ok(())
    }

    /// The cold objects `bucket` settles in the cold plane, in key order.
    fn cold_placements(&self, bucket: &bucket::Name) -> Result<Vec<object::Key>, Error> {
        let mut cold = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let page = self
                .index
                .scan_placement(bucket, "", after.as_deref(), PLACEMENT_PAGE)?;
            let done = page.len() < PLACEMENT_PAGE;
            for (key, placement) in page {
                after = Some(key.as_ref().to_owned());
                if placement.is_cold_live() {
                    cold.push(key);
                }
            }
            if done {
                return Ok(cold);
            }
        }
    }

    /// Promote every cold object of `bucket` and recheck (T16).
    ///
    /// The caller holds the bucket write gate, so no move finalizes while the
    /// pass runs; a cold placement that appears during it (a synchronous
    /// classed write) is caught by the next pass, and the pass ends only when
    /// a scan finds none.
    async fn promote_bucket_cold(&self, bucket: &bucket::Name) -> Result<(), Error> {
        for _ in 0..MAX_GATE_PASSES {
            let cold = self.cold_placements(bucket)?;
            if cold.is_empty() {
                return Ok(());
            }
            for key in cold {
                // The copy is staged outside the key's lock; only the
                // revalidation and the publish take it (T14).
                let prepared = self.prepare_promotion(bucket, &key).await?;
                let _guard = self.locks.write(bucket, &key).await;
                self.finish_promotion(bucket, &key, prepared).await?;
            }
        }
        if self.cold_placements(bucket)?.is_empty() {
            return Ok(());
        }
        Err(Error::Storage(storage::corrupt(
            "versioning could not promote every cold object",
        )))
    }

    /// Flush every write buffered for `bucket` before an operation that must
    /// see each acknowledged object (T8): the bucket teardown and the
    /// versioning change that turns a plain write into a versioned one.
    async fn flush_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        self.buffer.flush_bucket(bucket, self).await
    }

    /// Flush every buffered write (T8): the client listings' pre-flight and
    /// the shutdown's drain.
    async fn flush_all(&self) -> Result<(), Error> {
        self.buffer.flush(self).await
    }

    /// Flush `key`'s buffered write before an element or byte operation that
    /// must see the acknowledged object (T8).
    pub(crate) async fn flush_key(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<(), Error> {
        self.buffer.flush_key(bucket, key, self).await
    }

    /// Publish one buffered entry on the warm plane (T8).
    ///
    /// The flush writes the bytes through the fs content commit, stamps the
    /// file with the buffered time, restores the buffered [`object::Info`] as
    /// the row, and clears the destination's placement, heat and cold copy.
    /// The entry stays buffered when any step fails, so a later flush retries
    /// it.
    async fn write_buffered(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        entry: &Buffered,
    ) -> Result<(), Error> {
        // The placement the flush replaces is the only evidence a cold copy
        // exists; it is read before the bytes publish and passed down, so the
        // clear below cannot lose it.
        let had_cold = self.placement_carries_cold(bucket, key);
        let content = self.fs_content();
        let content_key = ContentKey::Object(key.clone());
        let mut staged = content.stage(bucket, &content_key).await?;
        staged.write(entry.bytes.clone()).await?;
        content.publish(bucket, staged, &content_key).await?;
        self.set_file_mtime(bucket, key, entry.info.last_modified)
            .await?;
        self.rows
            .set_row(bucket, &entry.info, entry.info.last_modified)
            .await?;
        self.index.clear(bucket, key)?;
        // The object is warm from here on: drop the cold copy before any later
        // step can fail, so the attempt that knew about it removed it.
        self.discard_cold(bucket, key, had_cold).await;
        // A flushed buffered write is a warm object like any other, so the
        // mover's capacity pass must see it (T21).
        self.index
            .warm_replace(bucket, key, entry.info.last_modified)?;
        self.cache.remove(bucket, key);
        // A buffered overwrite replaces the object's bytes exactly as a
        // synchronous one does, so it drops the heat the old bytes earned
        // (T9); without this the counter and the durable row outlive the
        // object they described.
        self.invalidate_heat(bucket, key);
        Ok(())
    }

    /// Offer one plain write to the buffer, flushing when it is full (T7).
    ///
    /// The key's write lock is taken only for the admission itself: a flush
    /// must not run under it, because a flush takes each key's write lock
    /// after the flight lock. An entry the buffer cannot hold is answered
    /// back so the caller writes it through the filesystem.
    async fn buffer_write(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        entry: Buffered,
    ) -> Result<Option<Buffered>, Error> {
        let mut entry = entry;
        loop {
            let admission = {
                let _guard = self.locks.write(bucket, key).await;
                self.buffer.commit(bucket, entry)
            };
            match admission {
                Admission::Buffered => {
                    self.cache.remove(bucket, key);
                    return Ok(None);
                }
                Admission::Full(back) => {
                    self.flush_all().await?;
                    entry = back;
                }
                Admission::Refused(back) => return Ok(Some(back)),
            }
        }
    }

    /// Write a memory-staged body through the filesystem (T7's non-plain
    /// fallback and the buffer's refusal path).
    async fn commit_mem(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        bytes: Bytes,
        staged: MemInfo,
        meta: &WriteMeta<'_>,
    ) -> Result<object::Info, Error> {
        let _guard = self.locks.write(bucket, key).await;
        let body = self
            .fs
            .stage_body(bucket, key, bytes_body(bytes), None, staged.checksum)
            .await?;
        let info = self
            .fs
            .commit_object(
                bucket,
                key,
                body,
                meta.tags.clone(),
                meta.owner,
                meta.acl,
                meta.lock,
            )
            .await?;
        self.reset_to_fs(bucket, key, info.last_modified).await?;
        Ok(info)
    }

    /// Whether a commit is the plain, batch-eligible shape (T7): empty tags,
    /// the default ACL, no owner, no lock, batch mode and an unversioned
    /// bucket. A class is never required here because a classed write stages
    /// on the filesystem and never reaches the memory arm.
    ///
    /// A folder marker is never buffered: it is a directory the filesystem's
    /// commit creates, not a body a flush could publish, so the marker keeps
    /// `FsStorage::commit_object`'s own branch and its `ETag::EMPTY` answer.
    async fn plain_buffered(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        meta: &WriteMeta<'_>,
    ) -> Result<bool, Error> {
        if key.is_folder_marker()
            || !meta.tags.is_empty()
            || meta.owner.is_some()
            || meta.lock.is_some()
            || meta.acl != &Acl::default_private(None)
        {
            return Ok(false);
        }
        self.batch_eligible(bucket, Class::Standard).await
    }

    /// Record a cold object's changed metadata on the placement authority and
    /// its warm row mirror (T2).
    ///
    /// The caller holds the key's write lock. The row is restored from the
    /// cold authority, so a later promotion reproduces exactly what the cold
    /// metadata answers.
    async fn store_cold_metadata(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        info: &object::Info,
        placement: &Placement,
    ) -> Result<(), Error> {
        let cold = ColdInfo::from_info(info);
        self.index.set_placement(
            bucket,
            key,
            &Placement {
                tier: placement.tier,
                state: placement.state,
                cold: Some(cold.clone()),
                mtime: placement.mtime,
            },
        )?;
        self.rows.set_row(bucket, info, cold.last_modified).await?;
        Ok(())
    }

    /// The placement a cold-aware metadata write found under its key's write
    /// lock. The resolution has already answered [`Plane::Cold`], so a missing
    /// placement is an inconsistent index, not a missing object.
    fn cold_placement(&self, bucket: &bucket::Name, key: &object::Key) -> Result<Placement, Error> {
        self.index.placement(bucket, key)?.ok_or_else(|| {
            Error::Storage(storage::corrupt(
                "a cold-aware write found no placement under its lock",
            ))
        })
    }

    /// Apply `mutate` to a cold object's resolved info and record the result
    /// on the placement authority and its warm row (T2).
    ///
    /// The caller holds the key's write lock and has already resolved the plane
    /// to cold. The placement is re-read under that lock, so the store never
    /// writes back a placement the resolution observed before the lock; every
    /// cold metadata mutation shares this one invariant.
    async fn mutate_cold_metadata(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        info: object::Info,
        mutate: impl FnOnce(&mut object::Info),
    ) -> Result<object::Info, Error> {
        let placement = self.cold_placement(bucket, key)?;
        let mut updated = info;
        mutate(&mut updated);
        self.store_cold_metadata(bucket, key, &updated, &placement)
            .await?;
        Ok(updated)
    }

    /// Take the write side for two keys, in key order so two crossing moves
    /// cannot deadlock (T14); a degenerate pair takes the one lock.
    async fn lock_pair(
        &self,
        first: (&bucket::Name, &object::Key),
        second: (&bucket::Name, &object::Key),
    ) -> (WriteGuard, Option<WriteGuard>) {
        let left = (first.0.as_ref(), first.1.as_ref());
        let right = (second.0.as_ref(), second.1.as_ref());
        match left.cmp(&right) {
            Ordering::Equal => (self.locks.write(first.0, first.1).await, None),
            Ordering::Less => {
                let held = self.locks.write(first.0, first.1).await;
                (held, Some(self.locks.write(second.0, second.1).await))
            }
            Ordering::Greater => {
                let held = self.locks.write(second.0, second.1).await;
                (held, Some(self.locks.write(first.0, first.1).await))
            }
        }
    }
}

#[async_trait]
impl<C: ObjectContent> FlushPlane for TierStorage<C> {
    /// Publish one buffered entry under the key's write lock (T8).
    ///
    /// The buffer's flight lock is held by the caller, so this takes the
    /// lock in the consistent flight-then-key order. The stamp re-check
    /// under the key lock makes a buffered entry a commit replaced or
    /// superseded a no-op: the newer write owns the key.
    async fn flush_entry(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<(), Error> {
        let _guard = self.locks.write(bucket, key).await;
        let Some(entry) = self.buffer.peek(bucket, key, seq) else {
            return Ok(());
        };
        self.write_buffered(bucket, key, &entry).await?;
        self.buffer.finish(bucket, key, seq);
        Ok(())
    }

    /// Persist a poisoned entry under the layer's state directory (T8).
    ///
    /// The bytes and the metadata a flush would have restored land as one
    /// quarantine pair; the entry itself is the buffer's to drop after this
    /// answers. A read-only run never writes, so it refuses rather than
    /// creating a quarantine record.
    async fn quarantine_entry(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<(), Error> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        let _guard = self.locks.write(bucket, key).await;
        let Some(entry) = self.buffer.peek(bucket, key, seq) else {
            return Ok(());
        };
        quarantine::record(self.fs.root(), bucket, &entry)?;
        // The counter is booked from the running total, never from a directory
        // scan: one poison must not decode every record.
        let waiting = self
            .quarantine_count
            .fetch_add(1, MemoryOrdering::Relaxed)
            .saturating_add(1);
        #[cfg(test)]
        let _gate = test_lock::writer();
        record_tier_quarantine(waiting);
        Ok(())
    }
}

/// The placement scan's page size for the bucket-wide passes.
const PLACEMENT_PAGE: usize = 256;

/// How many cold-scan passes a versioning gate runs before reporting that it
/// could not drain the bucket.
const MAX_GATE_PASSES: usize = 8;

/// Record one body read the named plane served: the hit and the bytes it
/// handed the caller.
///
/// A full GET books the object's size; a ranged GET books the inclusive
/// extent `end - start + 1` the body actually carries. Recording is
/// fire-and-forget: it can never turn a served read into an error.
fn record_served(plane: TierPlane, served: &GetObjectResult) {
    let bytes = match served.served_range {
        Some((start, end)) => end.saturating_sub(start).saturating_add(1),
        None => served.info.size,
    };
    #[cfg(test)]
    let _gate = test_lock::writer();
    record_tier_hit(plane);
    record_tier_bytes(plane, bytes);
}

/// Record one metadata read the named plane served. A HEAD serves no body,
/// so it books a hit and no bytes.
fn record_hit(plane: TierPlane) {
    #[cfg(test)]
    let _gate = test_lock::writer();
    record_tier_hit(plane);
}

/// Record that `plane` did not hold the read's object.
fn record_miss(plane: TierPlane) {
    #[cfg(test)]
    let _gate = test_lock::writer();
    record_tier_miss(plane);
}

/// Whether a storage error means the plane did not hold the read's object: a
/// missing key or version. Any other error (an invalid range, an I/O failure)
/// is a failed read, not a miss, and books no miss sample.
fn storage_miss(err: &storage::Error) -> bool {
    matches!(
        err,
        storage::Error::NoSuchKey(_) | storage::Error::NoSuchVersion(_)
    )
}

/// Whether a filesystem-plane error projects a missing key or version.
fn fs_miss(err: &_fs::Error) -> bool {
    match err {
        _fs::Error::Storage(inner) => storage_miss(inner),
        _ => false,
    }
}

/// Book a warm metadata read that answered: a warm hit on success, and — only
/// when the error is an absent object — a warm miss on failure.
fn warm_meta<T>(result: Result<T, _fs::Error>) -> Result<T, Error> {
    match result {
        Ok(value) => {
            record_hit(TierPlane::Warm);
            Ok(value)
        }
        Err(err) => Err(warm_error(err)),
    }
}

/// Book the warm miss an absent-object error carries, and project the error.
///
/// An invalid range or an I/O failure is a failed read, not a miss, and books
/// no miss sample.
fn warm_error(err: _fs::Error) -> Error {
    if fs_miss(&err) {
        record_miss(TierPlane::Warm);
    }
    err.into()
}

/// Book one filesystem-row subresource read: a cold-placed key books the warm
/// miss and the cold hit a HEAD would, a warm key books the warm hit.
///
/// The three Object Lock reads (retention, legal hold and the lock) classify
/// by `TierStorage::cold_placed` rather than resolving the key, so booking a
/// metric never reconciles or retires state.
fn book_lock_read<T>(cold: bool, result: Result<T, _fs::Error>) -> Result<T, Error> {
    match result {
        Ok(value) => {
            if cold {
                record_miss(TierPlane::Warm);
                record_hit(TierPlane::Cold);
            } else {
                record_hit(TierPlane::Warm);
            }
            Ok(value)
        }
        Err(err) => Err(warm_error(err)),
    }
}

/// Whether the warm row still describes the cold snapshot exactly: the ETag,
/// the size and the recorded modification time all agree.
///
/// This is the "row still describes this cold snapshot" rule the demote
/// revalidation, the copying resolution and the promotion paths share. The
/// T5 overwrite test deliberately checks the ETag alone; see
/// [`TierStorage::warm_won_overwrite`].
pub(crate) fn row_matches_snapshot(facts: &tier::Facts, snapshot: &object::Info) -> bool {
    facts.etag == snapshot.etag
        && facts.size == snapshot.size
        && facts.mtime == snapshot.last_modified
}

/// Whether a tier error means the plane could not serve the read because the
/// object was absent or its cold copy was short. Any other error is a failed
/// read, not a miss.
fn tier_miss(err: &Error) -> bool {
    match err {
        Error::ColdLength { .. } => true,
        Error::Storage(inner) => storage_miss(inner),
        _ => false,
    }
}

/// One chunk carrying `bytes`.
fn bytes_body(bytes: Bytes) -> BodyStream {
    Box::pin(stream::iter([Ok(bytes)]))
}

/// The `Info` a buffered plain commit acknowledges (T7, T8).
///
/// It carries what the commit would have written — the content ETag, the
/// recorded checksum, the size and a fresh modification time — over the
/// plain request's empty tags, no owner and default private ACL. The flush
/// restores exactly this metadata, so the acknowledged answer is the object's
/// recorded state.
fn buffered_info(key: &object::Key, bytes: &Bytes, staged: &MemInfo) -> object::Info {
    object::Info {
        key: key.clone(),
        size: bytes.len() as u64,
        last_modified: SystemTime::now(),
        etag: staged.etag.clone(),
        tags: object::Tags::empty(),
        checksum: staged.checksum.clone(),
        owner: None,
        acl: Acl::default_private(None),
        version_id: None,
        bucket_records_versions: false,
    }
}

/// Keep a per-key read guard alive for as long as the body it guards.
fn guarded(body: BodyStream, guard: ReadGuard) -> BodyStream {
    Box::pin(stream::unfold(
        (body, guard),
        |(mut body, guard)| async move {
            let chunk = body.next().await?;
            Some((chunk, (body, guard)))
        },
    ))
}

/// Project a cold plane's failure onto the layer's error.
pub(crate) fn cold_failure<E: Into<storage::Error>>(err: E) -> Error {
    Error::Storage(err.into())
}

// Every pure delegation refuses the reserved names, then hands the call to
// the filesystem backend unchanged; the shape lives once here.
macro_rules! forward_to_fs {
    ($self:ident, $reserved:expr, $name:ident($($arg:expr),* $(,)?)) => {{
        Self::refuse_reserved($reserved)?;
        Ok($self.fs.$name($($arg),*).await?)
    }};
}

#[async_trait]
impl<C: ObjectContent> Storage for TierStorage<C> {
    type Error = Error;

    fn supports_tiers(&self) -> bool {
        true
    }

    async fn storage_class(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Class, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        // Only a settled cold object is IA: a placement that is still copying,
        // already deleting, or carries no cold metadata does not serve from
        // the cold tier yet.
        match self.index.placement(bucket, key)? {
            Some(placement) if placement.is_cold_live() && placement.cold.is_some() => {
                Ok(Class::StandardIa)
            }
            _ => Ok(Class::Standard),
        }
    }

    /// Move one object to `class` (T10, T13).
    ///
    /// `STANDARD_IA` demotes the object into the cold plane; asking for the
    /// object's current class is a no-op. `STANDARD` is refused with
    /// [`storage::Error::InvalidRequest`] — this backend transitions down
    /// into cold only (T11), and the S3 surface answers a promotion request
    /// with `InvalidRequest`. The refusal comes before the current-class
    /// check, so a promotion request on an already-warm object is still
    /// refused rather than answered as a no-op.
    ///
    /// The move is cancel-safe: the lifecycle runner may drop this future at
    /// any await, and the worst state it leaves is a `copying` placement that
    /// garbage collection settles after the grace.
    async fn transition(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        class: Class,
        now: SystemTime,
    ) -> Result<(), <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        // An acknowledged write may still sit in the batch buffer: flush it
        // before the class read and the row probe, or a key that exists only
        // as a buffered entry would answer `NoSuchKey` (T8, T10).
        self.flush_key(bucket, key).await?;
        match class {
            Class::Standard => {
                return Err(storage::Error::InvalidRequest(
                    "a tier transition can only target STANDARD_IA",
                )
                .into());
            }
            Class::StandardIa => {}
        }
        if self.storage_class(bucket, key).await? == class {
            return Ok(());
        }
        if self.rows.info(bucket, key).await?.is_none() {
            return Err(storage::no_such_key(key).into());
        }
        self.demote(bucket, key, now).await?;
        Ok(())
    }

    /// The metrics reader's listing: the same bounded merge as a client
    /// listing, without the pre-flight flush a client listing takes (T20).
    async fn metrics_scan(
        &self,
        params: ListObjectsParams,
    ) -> Result<ObjectListing, <Self as Storage>::Error> {
        Self::refuse_reserved(&params.bucket)?;
        self.scan_objects_merged(params).await
    }
}

#[async_trait]
impl<C: ObjectContent> LifecycleRules for TierStorage<C> {
    async fn lifecycles(&self) -> Result<Vec<(bucket::Name, lifecycle::Config)>, storage::Error> {
        self.fs.lifecycles().await
    }
}

#[async_trait]
impl<C: ObjectContent> BucketOps for TierStorage<C> {
    async fn create_bucket(
        &self,
        name: &bucket::Name,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
    ) -> Result<(), <Self as Storage>::Error> {
        forward_to_fs!(self, name, create_bucket(name, owner, acl))
    }

    async fn create_bucket_with_lock(
        &self,
        name: &bucket::Name,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
        config: &object_lock::Config,
    ) -> Result<(), <Self as Storage>::Error> {
        forward_to_fs!(
            self,
            name,
            create_bucket_with_lock(name, owner, acl, config)
        )
    }

    async fn get_bucket_object_lock(
        &self,
        name: &bucket::Name,
    ) -> Result<Option<object_lock::Config>, <Self as Storage>::Error> {
        forward_to_fs!(self, name, get_bucket_object_lock(name))
    }

    async fn put_bucket_object_lock(
        &self,
        name: &bucket::Name,
        config: &object_lock::Config,
    ) -> Result<(), <Self as Storage>::Error> {
        forward_to_fs!(self, name, put_bucket_object_lock(name, config))
    }

    async fn delete_bucket(&self, name: &bucket::Name) -> Result<(), <Self as Storage>::Error> {
        Self::refuse_reserved(name)?;
        // The bucket write gate spans the emptiness decision and the teardown,
        // so no listing or move finalize sees a half-deleted bucket (T14).
        let _gate = self.gates.write(name).await;
        self.flush_bucket(name).await?;
        // The filesystem sees no cold object's file, so the index is the one
        // place that knows a cold object still lives here (T18).
        if !self.cold_placements(name)?.is_empty() {
            return Err(storage::not_empty(name).into());
        }
        self.fs.delete_bucket(name).await?;
        self.index.remove_bucket(name)?;
        self.cold
            .remove_bucket(name)
            .await
            .map_err(|err| Error::Storage(err.into()))?;
        Ok(())
    }

    async fn head_bucket(
        &self,
        name: &bucket::Name,
    ) -> Result<tinio_core::Bucket, <Self as Storage>::Error> {
        forward_to_fs!(self, name, head_bucket(name))
    }

    async fn list_buckets(
        &self,
        params: ListBucketsParams,
        owner: Option<&acl::OwnerId>,
        lazy_default: Option<&acl::OwnerId>,
    ) -> Result<BucketsListing, <Self as Storage>::Error> {
        let mut listing = self.fs.list_buckets(params, owner, lazy_default).await?;
        listing
            .buckets
            .retain(|entry| !pg::is_reserved(&entry.name));
        Ok(listing)
    }

    async fn get_bucket_tags(
        &self,
        name: &bucket::Name,
    ) -> Result<object::Tags, <Self as Storage>::Error> {
        forward_to_fs!(self, name, get_bucket_tags(name))
    }

    async fn put_bucket_tags(
        &self,
        name: &bucket::Name,
        tags: &object::Tags,
    ) -> Result<(), <Self as Storage>::Error> {
        forward_to_fs!(self, name, put_bucket_tags(name, tags))
    }

    async fn delete_bucket_tags(
        &self,
        name: &bucket::Name,
    ) -> Result<(), <Self as Storage>::Error> {
        forward_to_fs!(self, name, delete_bucket_tags(name))
    }

    async fn get_bucket_cors(
        &self,
        name: &bucket::Name,
    ) -> Result<Option<cors::Config>, <Self as Storage>::Error> {
        forward_to_fs!(self, name, get_bucket_cors(name))
    }

    async fn put_bucket_cors(
        &self,
        name: &bucket::Name,
        cors: &cors::Config,
    ) -> Result<(), <Self as Storage>::Error> {
        forward_to_fs!(self, name, put_bucket_cors(name, cors))
    }

    async fn delete_bucket_cors(
        &self,
        name: &bucket::Name,
    ) -> Result<(), <Self as Storage>::Error> {
        forward_to_fs!(self, name, delete_bucket_cors(name))
    }

    async fn get_bucket_lifecycle(
        &self,
        name: &bucket::Name,
    ) -> Result<Option<lifecycle::Config>, <Self as Storage>::Error> {
        forward_to_fs!(self, name, get_bucket_lifecycle(name))
    }

    async fn put_bucket_lifecycle(
        &self,
        name: &bucket::Name,
        config: &lifecycle::Config,
    ) -> Result<(), <Self as Storage>::Error> {
        forward_to_fs!(self, name, put_bucket_lifecycle(name, config))
    }

    async fn merge_bucket_lifecycle(
        &self,
        name: &bucket::Name,
        rules: &[lifecycle::Rule],
    ) -> Result<lifecycle::Merged, <Self as Storage>::Error> {
        forward_to_fs!(self, name, merge_bucket_lifecycle(name, rules))
    }

    async fn delete_bucket_lifecycle(
        &self,
        name: &bucket::Name,
    ) -> Result<(), <Self as Storage>::Error> {
        forward_to_fs!(self, name, delete_bucket_lifecycle(name))
    }

    async fn get_bucket_acl(&self, name: &bucket::Name) -> Result<Acl, <Self as Storage>::Error> {
        forward_to_fs!(self, name, get_bucket_acl(name))
    }

    async fn put_bucket_acl(
        &self,
        name: &bucket::Name,
        grants: &AclGrants,
    ) -> Result<(), <Self as Storage>::Error> {
        forward_to_fs!(self, name, put_bucket_acl(name, grants))
    }
}

#[async_trait]
impl<C: ObjectContent> ObjectOps for TierStorage<C> {
    type StagedBody = TierStaged;

    async fn stage_body(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        body: BodyStream,
        tee: Option<Arc<checksum::PartChecksum>>,
        carried: Option<checksum::Recorded>,
    ) -> Result<Self::StagedBody, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        self.stage_body_with_class(bucket, key, body, tee, carried, Class::Standard)
            .await
    }

    async fn resolve_write_lock(
        &self,
        bucket: &bucket::Name,
        explicit: Option<&object_lock::Retention>,
        legal_hold: object_lock::LegalHold,
        now: SystemTime,
    ) -> Result<Option<object_lock::Lock>, <Self as Storage>::Error> {
        forward_to_fs!(
            self,
            bucket,
            resolve_write_lock(bucket, explicit, legal_hold, now)
        )
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
    ) -> Result<object::Info, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        match staged {
            TierStaged::Fs(staged) => {
                let _guard = self.locks.write(bucket, key).await;
                let info = self
                    .fs
                    .commit_object(bucket, key, staged, tags, owner, acl, lock)
                    .await?;
                // T16: the destination's cached bytes never survive a write,
                // and the object the write replaced is now warm — its cold
                // copy is stale.
                self.reset_to_fs(bucket, key, info.last_modified).await?;
                Ok(info)
            }
            TierStaged::Mem {
                bytes,
                info: staged_info,
            } => {
                // Task 11's one decision point (T7): a plain commit — empty
                // tags, the default ACL, no owner, no lock, an unversioned
                // bucket, a non-marker key — inserts the bytes and the fixed
                // metadata into the write buffer and answers a synthesized
                // `Info`; every other commit takes the filesystem
                // write-through below. A classed write never reaches this
                // arm: its stage answered `Fs`, and a folder marker keeps the
                // filesystem's marker commit.
                let meta = WriteMeta {
                    tags: &tags,
                    owner,
                    acl,
                    lock,
                };
                if self.plain_buffered(bucket, key, &meta).await? {
                    let info = buffered_info(key, &bytes, &staged_info);
                    let entry = Buffered {
                        bytes,
                        info: info.clone(),
                    };
                    if let Some(refused) = self.buffer_write(bucket, key, entry).await? {
                        return self
                            .commit_mem(bucket, key, refused.bytes, staged_info, &meta)
                            .await;
                    }
                    Ok(info)
                } else {
                    self.commit_mem(bucket, key, bytes, staged_info, &meta)
                        .await
                }
            }
        }
    }

    async fn get_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        range: Option<ByteRange>,
    ) -> Result<GetObjectResult, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        self.locked_read(bucket, key, range).await
    }

    async fn copy_object(
        &self,
        src_bucket: &bucket::Name,
        src_key: &object::Key,
        dst_bucket: &bucket::Name,
        dst_key: &object::Key,
        tags: object::Tags,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
        checksum: Option<checksum::Recorded>,
        lock: Option<&object_lock::Lock>,
    ) -> Result<object::Info, <Self as Storage>::Error> {
        Self::refuse_reserved(src_bucket)?;
        Self::refuse_reserved(dst_bucket)?;
        // fs's copy opens the source file, so a buffered source is flushed
        // first (T8). The source's cold copy is staged before the pair lock
        // (T14); the revalidation and publish happen under it.
        self.flush_key(src_bucket, src_key).await?;
        let source = self.prepare_promotion(src_bucket, src_key).await?;
        // The source is promoted while both keys are held: fs's copy opens
        // the source file, which a cold source does not have yet (T16).
        let _guards = self
            .lock_pair((src_bucket, src_key), (dst_bucket, dst_key))
            .await;
        self.finish_promotion(src_bucket, src_key, source).await?;
        let info = self
            .fs
            .copy_object(
                src_bucket, src_key, dst_bucket, dst_key, tags, owner, acl, checksum, lock,
            )
            .await?;
        self.reset_to_fs(dst_bucket, dst_key, info.last_modified)
            .await?;
        Ok(info)
    }

    async fn rename_object(
        &self,
        bucket: &bucket::Name,
        src: &object::Key,
        dst: &object::Key,
    ) -> Result<object::Info, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        self.flush_key(bucket, src).await?;
        // The source's pre-write placement is the evidence its cold copy
        // exists; the promotion below replaces it.
        let source_had_cold = self.placement_carries_cold(bucket, src);
        // The source's cold copy is staged before the pair lock; the
        // revalidation and publish take it (T14).
        let source = self.prepare_promotion(bucket, src).await?;
        // One lock each, taken in key order so two crossing renames cannot
        // deadlock; a degenerate self-move takes the one card.
        let _guards = self.lock_pair((bucket, src), (bucket, dst)).await;
        self.finish_promotion(bucket, src, source).await?;
        let info = self.fs.rename_object(bucket, src, dst).await?;
        // The source key names nothing any more; the destination is warm and
        // whatever the move replaced goes with it (T16).
        self.remove_traces(
            bucket,
            src,
            Removal {
                warm: Some(info.last_modified),
                cold: if source_had_cold {
                    ColdRemoval::BestEffort
                } else {
                    ColdRemoval::None
                },
            },
        )
        .await?;
        self.reset_to_fs(bucket, dst, info.last_modified).await?;
        Ok(info)
    }

    async fn head_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<object::Info, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        self.flush_key(bucket, key).await?;
        let _guard = self.locks.read(bucket, key).await;
        // A HEAD serves metadata only, so it books the hit and no bytes.
        self.book_metadata(bucket, key, self.fs.head_object(bucket, key), |info| info)
            .await
    }

    async fn delete_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<version::Marker, <Self as Storage>::Error> {
        self.delete_tombstone(bucket, key).await
    }

    async fn get_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<object::Tags, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        self.flush_key(bucket, key).await?;
        let _guard = self.locks.read(bucket, key).await;
        // A tag read is a metadata read: it books the serving plane's hit and
        // no bytes, exactly as a HEAD does. A cold object's tags are the cold
        // authority's, so the warm plane missed and the cold plane served.
        self.book_metadata(bucket, key, self.fs.get_object_tags(bucket, key), |info| {
            info.tags
        })
        .await
    }

    async fn put_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<TagWriteResult, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        self.flush_key(bucket, key).await?;
        let _guard = self.locks.write(bucket, key).await;
        let answer = match self.resolve(bucket, key).await? {
            Plane::Cold(info) => {
                let updated = self
                    .mutate_cold_metadata(bucket, key, info, |updated| {
                        updated.tags = tags.clone();
                    })
                    .await?;
                TagWriteResult {
                    version_id: updated.version_id,
                    bucket_records_versions: updated.bucket_records_versions,
                }
            }
            Plane::Warm => self.fs.put_object_tags(bucket, key, tags).await?,
            Plane::Deleted => return Err(storage::no_such_key(key).into()),
        };
        self.invalidate(bucket, key);
        Ok(answer)
    }

    async fn delete_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<TagWriteResult, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        self.flush_key(bucket, key).await?;
        let _guard = self.locks.write(bucket, key).await;
        let answer = match self.resolve(bucket, key).await? {
            Plane::Cold(info) => {
                self.mutate_cold_metadata(bucket, key, info, |updated| {
                    updated.tags = object::Tags::empty();
                })
                .await?;
                TagWriteResult::untouched()
            }
            Plane::Warm => self.fs.delete_object_tags(bucket, key).await?,
            Plane::Deleted => TagWriteResult::untouched(),
        };
        self.invalidate(bucket, key);
        Ok(answer)
    }

    async fn get_object_acl(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Acl, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        self.flush_key(bucket, key).await?;
        let _guard = self.locks.read(bucket, key).await;
        // The ACL read books like the tag read: a metadata hit on the plane
        // that answers, and no bytes.
        self.book_metadata(bucket, key, self.fs.get_object_acl(bucket, key), |info| {
            Acl {
                owner: info.owner,
                grants: info.acl.grants,
            }
        })
        .await
    }

    async fn put_object_acl(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        grants: &AclGrants,
    ) -> Result<(), <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        self.flush_key(bucket, key).await?;
        let _guard = self.locks.write(bucket, key).await;
        match self.resolve(bucket, key).await? {
            Plane::Cold(info) => {
                self.mutate_cold_metadata(bucket, key, info, |updated| {
                    updated.acl = Acl {
                        owner: updated.owner.clone(),
                        grants: grants.clone(),
                    };
                })
                .await?;
            }
            Plane::Warm => self.fs.put_object_acl(bucket, key, grants).await?,
            Plane::Deleted => return Err(storage::no_such_key(key).into()),
        }
        self.invalidate(bucket, key);
        Ok(())
    }

    async fn list_objects(
        &self,
        params: ListObjectsParams,
    ) -> Result<ObjectListing, <Self as Storage>::Error> {
        Self::refuse_reserved(&params.bucket)?;
        self.list_objects_merged(params).await
    }

    async fn list_object_parts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Vec<multipart::ObjectPart>, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        self.flush_key(bucket, key).await?;
        let _guard = self.locks.read(bucket, key).await;
        match self.resolve(bucket, key).await? {
            Plane::Deleted => Err(storage::no_such_key(key).into()),
            Plane::Warm => Ok(self.fs.list_object_parts(bucket, key).await?),
            Plane::Cold(_) => Ok(self.rows.parts(bucket, key).await?),
        }
    }
}

#[async_trait]
impl<C: ObjectContent> MultipartOps for TierStorage<C> {
    async fn create_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        checksum: Option<checksum::Upload>,
        tags: object::Tags,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
        lock: Option<&object_lock::Lock>,
    ) -> Result<multipart::MultipartUpload, <Self as Storage>::Error> {
        forward_to_fs!(
            self,
            bucket,
            create_multipart_upload(bucket, key, checksum, tags, owner, acl, lock)
        )
    }

    async fn get_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<multipart::MultipartUpload, <Self as Storage>::Error> {
        forward_to_fs!(self, bucket, get_multipart_upload(bucket, key, upload_id))
    }

    async fn upload_part(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        part_number: multipart::PartNumber,
        body: BodyStream,
        checksum: Option<Arc<checksum::PartChecksum>>,
    ) -> Result<multipart::PartInfo, <Self as Storage>::Error> {
        forward_to_fs!(
            self,
            bucket,
            upload_part(bucket, key, upload_id, part_number, body, checksum)
        )
    }

    async fn copy_part(
        &self,
        src_bucket: &bucket::Name,
        src_key: &object::Key,
        dst_bucket: &bucket::Name,
        dst_key: &object::Key,
        upload_id: &str,
        part_number: multipart::PartNumber,
        range: Option<ByteRange>,
    ) -> Result<multipart::PartInfo, <Self as Storage>::Error> {
        Self::refuse_reserved(src_bucket)?;
        Self::refuse_reserved(dst_bucket)?;
        // fs's fast path opens the source file, so a buffered source is
        // flushed first and a cold one is staged before the pair lock (T8,
        // T14, T16). The destination object's cold copy is staged too: the
        // promotion is invisible to a reader (the same bytes move to the warm
        // plane), while T16's reset leaves the key warm with no pg copy. A
        // fresh destination — no row and no cold placement — is only
        // invalidated: an UploadPartCopy writes a part, so it must not plant a
        // placement or a capacity candidate for an object that does not exist
        // (I2).
        self.flush_key(src_bucket, src_key).await?;
        let source = self.prepare_promotion(src_bucket, src_key).await?;
        let destination_facts = self.rows.facts(dst_bucket, dst_key).await?;
        let destination_mtime = destination_facts
            .as_ref()
            .map_or_else(SystemTime::now, |facts| facts.mtime);
        let destination_present = destination_facts.is_some()
            || self
                .index
                .placement(dst_bucket, dst_key)?
                .is_some_and(|placement| placement.tier == Tier::Pg);
        let destination = self.prepare_promotion(dst_bucket, dst_key).await?;
        let _guards = self
            .lock_pair((src_bucket, src_key), (dst_bucket, dst_key))
            .await;
        self.finish_promotion(src_bucket, src_key, source).await?;
        self.finish_promotion(dst_bucket, dst_key, destination)
            .await?;
        let part = self
            .fs
            .copy_part(
                src_bucket,
                src_key,
                dst_bucket,
                dst_key,
                upload_id,
                part_number,
                range,
            )
            .await?;
        if destination_present {
            self.reset_to_fs(dst_bucket, dst_key, destination_mtime)
                .await?;
        } else {
            self.invalidate(dst_bucket, dst_key);
        }
        Ok(part)
    }

    async fn list_parts(
        &self,
        params: ListPartsParams,
    ) -> Result<PartsListing, <Self as Storage>::Error> {
        forward_to_fs!(self, &params.bucket, list_parts(params))
    }

    async fn complete_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        parts: &[multipart::CompletedPart],
        checksum: Option<checksum::Recorded>,
    ) -> Result<object::Info, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        let _guard = self.locks.write(bucket, key).await;
        let info = self
            .fs
            .complete_multipart_upload(bucket, key, upload_id, parts, checksum)
            .await?;
        // The completed object replaces whatever the key held, cold or warm
        // (T16).
        self.reset_to_fs(bucket, key, info.last_modified).await?;
        Ok(info)
    }

    async fn abort_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<(), <Self as Storage>::Error> {
        forward_to_fs!(self, bucket, abort_multipart_upload(bucket, key, upload_id))
    }

    async fn list_multipart_uploads(
        &self,
        params: ListUploadsParams,
    ) -> Result<UploadsListing, <Self as Storage>::Error> {
        forward_to_fs!(self, &params.bucket, list_multipart_uploads(params))
    }
}

#[async_trait]
impl<C: ObjectContent> VersionOps for TierStorage<C> {
    async fn bucket_versioning(
        &self,
        bucket: &bucket::Name,
    ) -> Result<version::State, <Self as Storage>::Error> {
        forward_to_fs!(self, bucket, bucket_versioning(bucket))
    }

    async fn set_bucket_versioning(
        &self,
        bucket: &bucket::Name,
        state: version::State,
    ) -> Result<(), <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        // The gate holds the bucket's write side, so no move finalizes and no
        // listing assembles a page while the bucket changes class (T14, T16).
        let _gate = self.gates.write(bucket).await;
        if state.records_versions() {
            // Every acknowledged write is visible before the class changes,
            // and every cold object is promoted: a bucket that records
            // versions keeps its objects on the warm plane.
            self.flush_bucket(bucket).await?;
            self.promote_bucket_cold(bucket).await?;
        }
        self.fs.set_bucket_versioning(bucket, state).await?;
        // The transition changes the version facts every cached object's
        // metadata carries, so the memory plane must not answer them from an
        // entry filled under the bucket's previous state.
        self.cache.remove_bucket(bucket);
        Ok(())
    }

    async fn get_object_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&version::Id>,
        range: Option<ByteRange>,
    ) -> Result<version::ReadResult, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        if self.null_address_route(bucket, key, version).await? {
            return Ok(version::ReadResult::Object(
                self.locked_read(bucket, key, range).await?,
            ));
        }
        // The filesystem's version engine serves every versioned body, and a
        // versioned bucket keeps its objects warm (T16), so this is a warm
        // body read. A delete marker has no body and books nothing.
        match self
            .fs
            .get_object_version(bucket, key, version, range)
            .await
        {
            Ok(version::ReadResult::Object(served)) => {
                record_served(TierPlane::Warm, &served);
                Ok(version::ReadResult::Object(served))
            }
            Ok(marker @ version::ReadResult::Marker { .. }) => Ok(marker),
            Err(err) => Err(warm_error(err)),
        }
    }

    async fn head_object_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&version::Id>,
    ) -> Result<version::HeadResult, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        if self.null_address_route(bucket, key, version).await? {
            return Ok(version::HeadResult::Object(
                self.head_object(bucket, key).await?,
            ));
        }
        // The filesystem's version engine serves every versioned HEAD, and a
        // versioned bucket keeps its objects warm (T16); a HEAD books a hit
        // and no bytes. A delete marker books nothing.
        match self.fs.head_object_version(bucket, key, version).await {
            Ok(version::HeadResult::Object(info)) => {
                record_hit(TierPlane::Warm);
                Ok(version::HeadResult::Object(info))
            }
            Ok(marker @ version::HeadResult::Marker { .. }) => Ok(marker),
            Err(err) => Err(warm_error(err)),
        }
    }

    async fn delete_object_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&version::Id>,
        bypass: object_lock::Bypass,
    ) -> Result<version::Marker, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        // T6 scopes the null-version address's object-delete routing to a
        // bucket that records no versions: there the address is the layer's
        // own and the key is the object. On an enabled or suspended bucket
        // `versionId=null` names the null version, and the filesystem's
        // version op removes it — or the null-identity marker it became —
        // without inserting a new marker.
        if self.null_object_address(bucket, version).await? {
            return self.delete_object(bucket, key).await;
        }
        self.flush_key(bucket, key).await?;
        let _guard = self.locks.write(bucket, key).await;
        let marker = self
            .fs
            .delete_object_version(bucket, key, version, bypass)
            .await?;
        self.invalidate(bucket, key);
        Ok(marker)
    }

    async fn list_object_versions(
        &self,
        bucket: &bucket::Name,
        params: version::ListParams,
    ) -> Result<version::Listing, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        self.list_object_versions_merged(bucket, params).await
    }

    async fn current_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<version::CurrentVersion, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        self.flush_key(bucket, key).await?;
        // The filesystem's version engine answers this metadata read, and a
        // versioned bucket keeps its objects warm (T16): it books a warm hit
        // or miss and no bytes.
        warm_meta(self.fs.current_version(bucket, key).await)
    }

    async fn version_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&version::Id>,
    ) -> Result<object::Tags, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        if self.null_address_route(bucket, key, version).await? {
            return self.get_object_tags(bucket, key).await;
        }
        // An explicit version is served by the filesystem's version engine
        // (T16 keeps a versioned bucket warm): one warm hit or miss, no bytes.
        warm_meta(self.fs.version_tags(bucket, key, version).await)
    }

    async fn set_version_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&version::Id>,
        tags: object::Tags,
    ) -> Result<(), <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        if self.null_address_route(bucket, key, version).await? {
            self.put_object_tags(bucket, key, &tags).await?;
            return Ok(());
        }
        Ok(self.fs.set_version_tags(bucket, key, version, tags).await?)
    }

    async fn version_acl(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&version::Id>,
    ) -> Result<Acl, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        if self.null_address_route(bucket, key, version).await? {
            return self.get_object_acl(bucket, key).await;
        }
        // The explicit-version ACL read books exactly as the tag read does.
        warm_meta(self.fs.version_acl(bucket, key, version).await)
    }

    async fn set_version_acl(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&version::Id>,
        acl: &Acl,
    ) -> Result<(), <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        if self.null_address_route(bucket, key, version).await? {
            self.put_object_acl(bucket, key, &acl.grants).await?;
            return Ok(());
        }
        Ok(self.fs.set_version_acl(bucket, key, version, acl).await?)
    }

    async fn get_object_retention(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&version::Id>,
    ) -> Result<Option<object_lock::Retention>, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        self.flush_key(bucket, key).await?;
        // The retention lives in the filesystem row (the cold metadata carries
        // no lock), but the read is classified by the placement that governs
        // the key: a cold object books the warm miss and the cold hit a HEAD
        // would, and an absent row books the one attempted-tier miss.
        let cold = self.cold_placed(bucket, key)?;
        book_lock_read(
            cold,
            self.fs.get_object_retention(bucket, key, version).await,
        )
    }

    async fn get_object_legal_hold(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&version::Id>,
    ) -> Result<object_lock::LegalHold, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        self.flush_key(bucket, key).await?;
        // The legal-hold read classifies exactly as the retention read does.
        let cold = self.cold_placed(bucket, key)?;
        book_lock_read(
            cold,
            self.fs.get_object_legal_hold(bucket, key, version).await,
        )
    }

    async fn object_lock(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&version::Id>,
    ) -> Result<Option<object_lock::Lock>, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        self.flush_key(bucket, key).await?;
        // The lock read classifies exactly as the retention read does.
        let cold = self.cold_placed(bucket, key)?;
        book_lock_read(cold, self.fs.object_lock(bucket, key, version).await)
    }

    async fn set_retention(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        address: &version::Address,
        requested: Option<&object_lock::Retention>,
        bypass: object_lock::Bypass,
        now: SystemTime,
    ) -> Result<Option<object_lock::Lock>, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        self.flush_key(bucket, key).await?;
        Ok(self
            .fs
            .set_retention(bucket, key, address, requested, bypass, now)
            .await?)
    }

    async fn set_legal_hold(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        address: &version::Address,
        hold: object_lock::LegalHold,
    ) -> Result<object_lock::Lock, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        self.flush_key(bucket, key).await?;
        Ok(self.fs.set_legal_hold(bucket, key, address, hold).await?)
    }

    async fn update_lock(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        address: &version::Address,
        retention: Option<Option<&object_lock::Retention>>,
        legal_hold: Option<object_lock::LegalHold>,
        bypass: object_lock::Bypass,
        now: SystemTime,
    ) -> Result<Option<object_lock::Lock>, <Self as Storage>::Error> {
        Self::refuse_reserved(bucket)?;
        self.flush_key(bucket, key).await?;
        Ok(self
            .fs
            .update_lock(bucket, key, address, retention, legal_hold, bypass, now)
            .await?)
    }
}

/// Construction and root binding over the production PostgreSQL cold plane
/// (T18).
impl TierStorage<ColdStore> {
    /// Open the tier over `root` with PostgreSQL as the cold plane.
    ///
    /// The root identity is bound to the database before the layer is served:
    /// a fresh index over an empty database mints one and records it in both
    /// places; an index and a database that record the same identity reopen;
    /// a fresh index over a non-empty database refuses unless `adopt` is set;
    /// and `adopt` refuses a database whose recorded identity differs from the
    /// index's. A warm root that already holds the reserved bucket name
    /// refuses the conversion outright.
    ///
    /// The index is the layer's own at `<root>/.tinio/tier.redb`, and the
    /// marker that precedes it records the same identity (T17). The identity
    /// is resolved — from the database, an older index, or a fresh UUID —
    /// before the marker is written, so a fresh pg construction creates the
    /// marker exactly once; only an explicit `--adopt` resync rewrites it
    /// afterwards. A read-only run (`AccessMode::ReadOnly`) opens the fs
    /// plane, the index and the cold plane read-only, requires the index, and
    /// starts nothing (T27): the pg connection takes no ownership lock and
    /// spawns no re-verify task.
    ///
    /// `access` governs all three planes; it is written into `options` so the
    /// fs plane cannot write during a read-only run.
    ///
    /// # Errors
    ///
    /// [`Error::Cold`] when the cold plane cannot be opened (which includes a
    /// second writable instance's ownership refusal), [`Error::AdoptRequired`]
    /// for a fresh index over a non-empty database, [`Error::RootMismatch`]
    /// for `--adopt` over a foreign root, [`Error::ReservedRoot`] for a warm
    /// root that already holds the reserved namespace, [`Error::IndexMissing`]
    /// for a read-only run without an index, and the marker errors
    /// [`Error::Io`], [`Error::CorruptMarker`], [`Error::MarkerMismatch`].
    ///
    /// # Examples
    ///
    /// An unreachable cold plane refuses the construction:
    ///
    /// ```rust
    /// use tinio_core::AccessMode;
    /// use tinio_fs::testing;
    /// use tinio_pg::Config as PgConfig;
    /// use tinio_tier::{Config, Error, FlushMode, TierStorage};
    /// use tinio_util::secret::{Secret, SecretKey};
    /// use tokio::runtime::Runtime;
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let pg = PgConfig::new(
    ///     Secret::from(SecretKey::from(
    ///         "postgres://tinio:tinio@127.0.0.1:1/tinio?sslmode=disable".to_owned(),
    ///     )),
    ///     1,
    /// );
    /// let config = Config {
    ///     capacity_bytes: 1024 * 1024,
    ///     max_object_bytes: 64 * 1024,
    ///     flush_mode: FlushMode::Sync,
    ///     batch_bytes: 8 * 1024,
    ///     batch_interval: std::time::Duration::from_secs(1),
    ///     heat_window: std::time::Duration::from_secs(60),
    ///     promote_reads: 3,
    ///     stats_flush: std::time::Duration::from_secs(10),
    ///     high_water_percent: 90,
    ///     low_water_percent: 80,
    ///     demote_idle: std::time::Duration::from_secs(3600),
    ///     dwell: std::time::Duration::from_secs(3600),
    ///     scan_interval: std::time::Duration::from_secs(60),
    ///     move_concurrency: 1,
    ///     index_compact_percent: 30,
    ///     gc_grace: std::time::Duration::from_secs(300),
    /// };
    /// let refused = Runtime::new()
    ///     .expect("a runtime")
    ///     .block_on(TierStorage::open_pg(
    ///         root.path(),
    ///         testing::fs_options(),
    ///         config,
    ///         &pg,
    ///         AccessMode::ReadWrite,
    ///         false,
    ///     ));
    /// assert!(matches!(refused, Err(Error::Cold(_))));
    /// ```
    pub async fn open_pg(
        root: impl Into<PathBuf>,
        options: FsOptions,
        config: Config,
        pg_config: &_pg::Config,
        access: AccessMode,
        adopt: bool,
    ) -> Result<Self, Error> {
        config.validate()?;
        let mut options = options;
        // One mode for the whole construction: a read-only run must not let
        // the fs plane write behind the read-only index and cold plane.
        options.access = access;
        let fs = FsStorage::new(root, options)?;
        Self::refuse_a_reserved_root(&fs).await?;
        let layer = fs.root().to_path_buf();
        if access.is_read_only() {
            let marker = read_marker(&marker_path(&layer))?;
            let index = open_index_read_only(&layer, marker)?;
            let cold = pg::connect(pg_config, access).await?;
            let tier = Self::assemble(Arc::new(fs), cold, Arc::new(index), config, true);
            tier.bind_root(access, adopt, None, None).await?;
            return Ok(tier);
        }
        // The root identity is resolved before the marker is written, so a
        // fresh pg construction creates the marker exactly once (T17): the
        // marker precedes the index, and the only later write is `--adopt`'s
        // resync below.
        // A leftover from a crashed write never confuses this one.
        remove_marker_temp(&marker_path(&layer))?;
        let cold = pg::connect(pg_config, access).await?;
        let recorded = read_marker(&marker_path(&layer))?;
        let (target, cold_root) = match recorded {
            Some(id) => (id, None),
            None => {
                let (id, facts) = Self::resolve_root_identity(&layer, &cold, adopt).await?;
                (id, Some(facts))
            }
        };
        if recorded.is_none() {
            write_marker(&marker_path(&layer), target)?;
        }
        let index = open_index_for_bind(&layer, Some(target), config.index_compact_percent)?;
        let tier = Self::assemble(Arc::new(fs), cold, Arc::new(index), config, false);
        tier.bind_root(access, adopt, Some(target), cold_root.as_ref())
            .await?;
        if adopt {
            // The one allowed second write: `--adopt` may have bound this root
            // to the database's identity instead of the marker's provisional
            // record.
            let identity = tier.index.root_id()?.ok_or_else(|| {
                Error::Storage(storage::corrupt("the pg bind recorded no root identity"))
            })?;
            write_marker_if_different(&marker_path(tier.fs.root()), identity)?;
        }
        Ok(tier)
    }

    /// The identity a fresh marker records on a pg construction, resolved
    /// before the marker is written (T17, T18).
    ///
    /// The cold database's identity wins, an older index's identity is reused
    /// when the database records none, and a UUID is minted only when both are
    /// absent; every refusal `bind_root` would raise is raised here first, so a
    /// refused construction writes no marker.
    ///
    /// The cold identity and emptiness this resolves travel back to
    /// [`TierStorage::bind_root`], so the bind does not repeat those queries
    /// against PostgreSQL.
    async fn resolve_root_identity(
        root: &Path,
        cold: &ColdStore,
        adopt: bool,
    ) -> Result<([u8; ROOT_ID_LEN], ColdRoot), Error> {
        let cold_id = pg::root_id(cold).await?;
        let index_id = index_identity_if_present(&index_path(root))?;
        match classify_root(cold_id, index_id).action(adopt)? {
            RootAction::Agreed(id) | RootAction::AdoptCold(id) => Ok((
                id,
                ColdRoot {
                    id: Some(id),
                    empty: None,
                },
            )),
            RootAction::IndexOnly(index) => {
                // The emptiness is only consulted without `adopt`; recording
                // it saves the bind from asking the same question again.
                let empty = if adopt {
                    None
                } else {
                    if !pg::is_empty(cold).await? {
                        return Err(Error::AdoptRequired);
                    }
                    Some(true)
                };
                Ok((index, ColdRoot { id: None, empty }))
            }
            RootAction::Empty => {
                if !pg::is_empty(cold).await? {
                    return Err(Error::AdoptRequired);
                }
                Ok((
                    *uuid::Uuid::new_v4().as_bytes(),
                    ColdRoot {
                        id: None,
                        empty: Some(true),
                    },
                ))
            }
        }
    }

    /// Refuse a warm root that already holds the reserved bucket name: the pg
    /// plane's root identity lives under that name, so the root cannot be
    /// converted to tier mode (T18).
    async fn refuse_a_reserved_root(fs: &FsStorage) -> Result<(), Error> {
        let listing = fs
            .list_buckets(
                ListBucketsParams {
                    prefix: String::new(),
                    start_after: None,
                    max_buckets: 10_000,
                },
                None,
                None,
            )
            .await?;
        if let Some(entry) = listing
            .buckets
            .iter()
            .find(|entry| pg::is_reserved(&entry.name))
        {
            return Err(Error::ReservedRoot {
                name: entry.name.to_string(),
            });
        }
        Ok(())
    }

    /// Bind this index's root identity to the cold database (T18).
    ///
    /// The match is over the two recorded identities and the database's
    /// emptiness: equal identities reopen, a fresh index adopts only with
    /// `adopt`, an unmarked database binds a recorded identity (and, with
    /// `adopt`, an existing index's identity), and an empty pair records
    /// `target` — the identity the marker already carries or was just written
    /// with — so the marker never trails the index it recorded.
    ///
    /// `cold_root` carries the identity and emptiness the pg construction
    /// already read, so the bind re-asks PostgreSQL only for what the
    /// construction did not observe.
    async fn bind_root(
        &self,
        access: AccessMode,
        adopt: bool,
        target: Option<[u8; ROOT_ID_LEN]>,
        cold_root: Option<&ColdRoot>,
    ) -> Result<(), Error> {
        let cold = match cold_root {
            Some(facts) => facts.id,
            None => pg::root_id(&self.cold).await?,
        };
        let index = self.index.root_id()?;
        match classify_root(cold, index).action(adopt && !access.is_read_only())? {
            RootAction::Agreed(_) => Ok(()),
            RootAction::AdoptCold(cold) => {
                self.index.set_root_id(cold)?;
                Ok(())
            }
            RootAction::IndexOnly(index) => {
                if access.is_read_only() {
                    return Ok(());
                }
                if adopt || self.cold_is_empty(cold_root).await? {
                    pg::record_root(&self.cold, index).await?;
                    Ok(())
                } else {
                    Err(Error::AdoptRequired)
                }
            }
            RootAction::Empty => {
                if !self.cold_is_empty(cold_root).await? {
                    return Err(Error::AdoptRequired);
                }
                if access.is_read_only() {
                    return Ok(());
                }
                let id = target.ok_or_else(|| {
                    Error::Storage(storage::corrupt(
                        "a pg bind without a marker has no root identity to record",
                    ))
                })?;
                self.index.set_root_id(id)?;
                pg::record_root(&self.cold, id).await?;
                Ok(())
            }
        }
    }

    /// Whether the cold database is empty, answering an observation the
    /// construction already made when it has one.
    async fn cold_is_empty(&self, known: Option<&ColdRoot>) -> Result<bool, Error> {
        match known.and_then(|facts| facts.empty) {
            Some(empty) => Ok(empty),
            None => pg::is_empty(&self.cold).await,
        }
    }
}

/// The cold root facts a pg construction already read, so the bind does not
/// repeat those queries (T17, T18).
struct ColdRoot {
    /// The identity the cold database records, or `None` when it records
    /// none.
    id: Option<[u8; ROOT_ID_LEN]>,
    /// Whether the database was observed empty; `None` when the resolution
    /// path did not need the answer.
    empty: Option<bool>,
}

/// The cold and index root identities as one pair (T17, T18).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RootPair {
    /// Both sides record the same identity.
    Agreed([u8; ROOT_ID_LEN]),
    /// Only the cold database records an identity.
    ColdOnly([u8; ROOT_ID_LEN]),
    /// Only the index records an identity.
    IndexOnly([u8; ROOT_ID_LEN]),
    /// Neither side records an identity.
    Empty,
    /// The two record different identities.
    Mismatch {
        /// The root identity the cold database records.
        cold: [u8; ROOT_ID_LEN],
        /// The root identity the index records.
        index: [u8; ROOT_ID_LEN],
    },
}

/// What a classified [`RootPair`] licenses its caller to do, once the caller's
/// adopting action is applied (T17, T18).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RootAction {
    /// Both sides record the same identity.
    Agreed([u8; ROOT_ID_LEN]),
    /// Only the cold database records an identity and the caller adopted it:
    /// the index takes the cold identity.
    AdoptCold([u8; ROOT_ID_LEN]),
    /// Only the index records an identity.
    IndexOnly([u8; ROOT_ID_LEN]),
    /// Neither side records an identity.
    Empty,
}

impl RootPair {
    /// Resolve the pair against whether the caller may adopt, refusing the
    /// answers no caller can use.
    ///
    /// The mismatch and the cold-only answer the caller did not adopt are
    /// refused here, in one place, so both the pg construction and the bind
    /// raise the same errors. `may_adopt` is the caller's own guard: the bind
    /// passes `adopt` only when its access mode may write.
    fn action(self, may_adopt: bool) -> Result<RootAction, Error> {
        match self {
            RootPair::Agreed(id) => Ok(RootAction::Agreed(id)),
            RootPair::ColdOnly(id) if may_adopt => Ok(RootAction::AdoptCold(id)),
            RootPair::ColdOnly(_) => Err(Error::AdoptRequired),
            RootPair::IndexOnly(id) => Ok(RootAction::IndexOnly(id)),
            RootPair::Empty => Ok(RootAction::Empty),
            RootPair::Mismatch { cold, index } => Err(Error::RootMismatch {
                cold: root_wire(cold),
                index: root_wire(index),
            }),
        }
    }
}

/// The identity matrix the pg construction and the bind share: what the cold
/// and index records, taken together, say about the root. The callers resolve
/// each answer through [`RootPair::action`], which carries the shared refusals;
/// each caller then maps the action onto its own work.
fn classify_root(cold: Option<[u8; ROOT_ID_LEN]>, index: Option<[u8; ROOT_ID_LEN]>) -> RootPair {
    match (cold, index) {
        (Some(cold), Some(index)) if cold == index => RootPair::Agreed(cold),
        (Some(cold), Some(index)) => RootPair::Mismatch { cold, index },
        (Some(cold), None) => RootPair::ColdOnly(cold),
        (None, Some(index)) => RootPair::IndexOnly(index),
        (None, None) => RootPair::Empty,
    }
}

/// The tier layer's private directory under a root (T17), shared with the
/// filesystem plane's reserved state directory.
pub(crate) const LAYER_DIR: &str = path::STATE_DIR_NAME;

/// The marker file's name under [`LAYER_DIR`].
const MARKER_FILE: &str = "tier.mode";

/// The index file's name under [`LAYER_DIR`].
const INDEX_FILE: &str = "tier.redb";

/// The root identity both the marker and the index record: a UUID's 16 bytes.
const ROOT_ID_LEN: usize = 16;

/// The marker path a root records its tier identity at.
///
/// The marker holds the 16 raw bytes of the root's UUID, exactly as the index's
/// metadata table does. It lives in the root's own private directory — never in
/// a relocated filesystem state dir — because a read-only run always reads
/// `<root>/.tinio` (T17, T27).
///
/// # Examples
///
/// ```rust
/// use tinio_tier::marker_path;
///
/// let root = tempfile::tempdir().expect("a temporary root");
/// assert!(marker_path(root.path()).ends_with(".tinio/tier.mode"));
/// ```
pub fn marker_path(root: impl AsRef<Path>) -> PathBuf {
    root.as_ref().join(LAYER_DIR).join(MARKER_FILE)
}

/// The index path a root keeps its placements at.
///
/// # Examples
///
/// ```rust
/// use tinio_tier::index_path;
///
/// let root = tempfile::tempdir().expect("a temporary root");
/// assert!(index_path(root.path()).ends_with(".tinio/tier.redb"));
/// ```
pub fn index_path(root: impl AsRef<Path>) -> PathBuf {
    root.as_ref().join(LAYER_DIR).join(INDEX_FILE)
}

/// Whether `root` is a tier root: its marker, its index or both are present
/// (T17).
///
/// This is the seam a non-tier start (the `fs` and `pg` backends' wiring, and
/// `tinio doctor`) asks before serving a root: either file means tier mode
/// owns the root, even when the other is missing. A stat failure other than
/// "not found" is an error, so an unreadable root fails closed.
///
/// # Errors
///
/// [`Error::Io`] when the root or either file cannot be inspected.
///
/// # Examples
///
/// ```rust
/// use tinio_tier::is_tier_root;
///
/// let root = tempfile::tempdir().expect("a temporary root");
/// assert!(!is_tier_root(root.path()).expect("the seam answers"));
/// ```
pub fn is_tier_root(root: impl AsRef<Path>) -> Result<bool, Error> {
    let (marker, index) = tier_evidence(root.as_ref())?;
    Ok(marker || index)
}

/// Refuse a non-tier start over `root` when the marker, the index or both
/// make it a tier root (T17).
///
/// The consumers are the `fs` and `pg` start paths and `tinio doctor`: each
/// calls this before serving, and the refusal names what it found.
///
/// # Errors
///
/// [`Error::TierRoot`] for a root carrying either file, and [`Error::Io`] when
/// the root cannot be inspected.
///
/// # Examples
///
/// ```rust
/// use tinio_tier::refuse_non_tier_root;
///
/// let root = tempfile::tempdir().expect("a temporary root");
/// refuse_non_tier_root(root.path()).expect("a plain root may start as fs or pg");
/// ```
pub fn refuse_non_tier_root(root: impl AsRef<Path>) -> Result<(), Error> {
    let (marker, index) = tier_evidence(root.as_ref())?;
    if marker || index {
        return Err(Error::TierRoot { marker, index });
    }
    Ok(())
}

/// Whether the marker and the index are present under `root`.
fn tier_evidence(root: &Path) -> Result<(bool, bool), Error> {
    Ok((
        path_present(&marker_path(root))?,
        path_present(&index_path(root))?,
    ))
}

/// Whether `path` exists; a failure is an error rather than "absent", except
/// a parent that is a file: nothing can exist under it, so the path is absent.
fn path_present(path: &Path) -> Result<bool, Error> {
    match path.try_exists() {
        Err(err) if err.kind() == io::ErrorKind::NotADirectory => Ok(false),
        Err(err) => Err(Error::Io(err)),
        Ok(present) => Ok(present),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::Arc,
        thread,
        time::{Duration, SystemTime},
    };

    use bytes::Bytes;
    use tinio_core::{
        AccessMode, Class, ETag,
        acl::Acl,
        bucket, object,
        storage::{BucketOps, Error as StorageError, MultipartOps, ObjectOps, Storage},
    };
    use tinio_fs::{FsStorage, testing};
    use tinio_pg::Config as PgConfig;
    use tinio_store_pg::test_support::{TestDb, require_dsn};
    use tinio_store_redb::RedbContent;
    use tinio_util::{
        metrics::record_tier_quarantine,
        secret::{Secret, SecretKey},
        testing::{body, metrics::gauge},
    };
    use tokio::runtime::Runtime;

    use super::{marker::marker_temp, *};
    use crate::{ColdInfo, Config, FlushMode, Index, Placement, State, Tier, TierStorage};

    fn config(flush_mode: FlushMode) -> Config {
        Config {
            capacity_bytes: 1024 * 1024,
            max_object_bytes: 64 * 1024,
            flush_mode,
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

    /// Open a real root through the public constructor.
    fn open(root: &Path) -> TierStorage<RedbContent> {
        TierStorage::open(
            root,
            testing::fs_options(),
            config(FlushMode::Sync),
            RedbContent::in_memory().expect("an in-memory cold plane"),
        )
        .expect("the tier layer opens")
    }

    /// **A tier root with cap 1 refuses a second live multipart upload.** The cap is
    /// construction-time input on the warm plane's options, so a tier run gets the same
    /// `[s3] max_concurrent_uploads` refusal a plain fs run does.
    #[test]
    fn a_tier_root_refuses_a_second_live_multipart_upload() {
        let root = tempfile::tempdir().expect("a temporary root");
        let tier = TierStorage::open(
            root.path(),
            tinio_fs::FsOptions {
                max_concurrent_uploads: 1,
                ..testing::fs_options()
            },
            config(FlushMode::Sync),
            RedbContent::in_memory().expect("an in-memory cold plane"),
        )
        .expect("the tier layer opens");
        Runtime::new().expect("a runtime").block_on(async {
            let name = bucket::name("data").expect("a valid bucket name");
            let key = object::key("upload.bin").expect("a valid object key");
            tier.create_bucket(&name, None, &Acl::default_private(None))
                .await
                .expect("the bucket is created");
            tier.create_multipart_upload(
                &name,
                &key,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
                None,
            )
            .await
            .expect("the first upload opens");
            let refused = tier
                .create_multipart_upload(
                    &name,
                    &key,
                    None,
                    object::Tags::empty(),
                    None,
                    &Acl::default_private(None),
                    None,
                )
                .await
                .expect_err("the cap refuses the second live upload");
            let refused: StorageError = refused.into();
            assert!(
                matches!(refused, StorageError::TooManyMultipartUploads { limit: 1 }),
                "the refusal is the upload-count one: {refused}"
            );
        });
    }

    /// The buffered write a quarantine record is seeded from in tests.
    fn quarantined_entry() -> Buffered {
        let data = Bytes::from_static(b"lost");
        Buffered {
            info: object::Info {
                key: object::key("lost.bin").expect("a valid key"),
                size: 4,
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

    /// The `tier.mode*` names a layer directory holds, sorted.
    fn marker_names(root: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(root.join(".tinio"))
            .expect("the layer directory is read")
            .filter_map(|entry| {
                let name = entry.expect("the entry is read").file_name();
                let name = name.to_string_lossy();
                name.starts_with("tier.mode").then(|| name.into_owned())
            })
            .collect();
        names.sort();
        names
    }

    #[test]
    fn the_identity_matrix_classifies_every_pair() {
        let first = [1; ROOT_ID_LEN];
        let second = [2; ROOT_ID_LEN];

        assert_eq!(
            classify_root(Some(first), Some(first)),
            RootPair::Agreed(first)
        );
        assert_eq!(
            classify_root(Some(first), Some(second)),
            RootPair::Mismatch {
                cold: first,
                index: second,
            }
        );
        assert_eq!(classify_root(Some(first), None), RootPair::ColdOnly(first));
        assert_eq!(classify_root(None, Some(first)), RootPair::IndexOnly(first));
        assert_eq!(classify_root(None, None), RootPair::Empty);
    }

    #[test]
    fn a_fresh_marker_is_written_once_without_a_temp() {
        let root = tempfile::tempdir().expect("a temporary root");
        let tier = open(root.path());

        let marker = fs::read(marker_path(root.path())).expect("the marker is read");
        assert_eq!(marker.len(), ROOT_ID_LEN);
        assert_eq!(
            marker.as_slice(),
            tier.index()
                .root_id()
                .expect("the index answers")
                .expect("the index records an identity"),
            "the marker and the index record one identity"
        );
        assert_eq!(
            marker_names(root.path()),
            vec!["tier.mode".to_owned()],
            "one marker file and no temporary file"
        );
    }

    #[test]
    fn a_leftover_marker_temp_is_ignored_and_removed() {
        let root = tempfile::tempdir().expect("a temporary root");
        fs::create_dir_all(root.path().join(".tinio")).expect("the layer directory exists");
        let temp = marker_temp(&marker_path(root.path()));
        fs::write(&temp, b"a crashed write").expect("the leftover temp exists");

        let tier = open(root.path());

        assert_eq!(
            fs::read(marker_path(root.path()))
                .expect("the marker is read")
                .len(),
            ROOT_ID_LEN,
            "the leftover temp did not confuse the construction"
        );
        assert!(!temp.exists(), "the leftover temp is removed on the open");
        assert!(tier.index().root_id().expect("the index answers").is_some());
    }

    /// A regular file handed in as the root carries no tier evidence: the
    /// probe answers "absent" for the paths under it (ENOTDIR), so the
    /// backend's own "not a directory" refusal is what the caller reports.
    #[test]
    fn a_regular_file_is_not_a_tier_root() {
        let root = tempfile::tempdir().expect("a temporary root");
        let file = root.path().join("a-file");
        fs::write(&file, b"not a root").expect("the file exists");

        assert!(!is_tier_root(&file).expect("the seam answers"));
        refuse_non_tier_root(&file).expect("a file carries no tier evidence");
    }

    /// An index that exists but cannot be read is refused before the marker is
    /// written: the identity is never guessed over an unreadable record.
    #[test]
    fn an_unreadable_index_refuses_before_the_marker() {
        let root = tempfile::tempdir().expect("a temporary root");
        fs::create_dir_all(root.path().join(".tinio")).expect("the layer directory exists");
        fs::write(index_path(root.path()), b"not a database").expect("the corrupt index exists");

        let refused = TierStorage::open(
            root.path(),
            testing::fs_options(),
            config(FlushMode::Sync),
            RedbContent::in_memory().expect("an in-memory cold plane"),
        )
        .err()
        .expect("the unreadable index refuses the construction");

        assert!(matches!(refused, Error::Index(_)), "{refused}");
        assert!(
            !marker_path(root.path()).exists(),
            "the marker was not written over a guessed identity"
        );
        assert!(!marker_temp(&marker_path(root.path())).exists());
    }

    /// The `--adopt` resync is the one allowed second marker write: a marker
    /// that disagrees with the bound identity is replaced by it.
    #[test]
    fn the_adopt_resync_replaces_the_marker() {
        let root = tempfile::tempdir().expect("a temporary root");
        let path = marker_path(root.path());
        let provisional = [7u8; ROOT_ID_LEN];
        let bound = [9u8; ROOT_ID_LEN];
        write_marker(&path, provisional).expect("the marker is written");

        write_marker_if_different(&path, bound).expect("the resync writes");

        assert_eq!(fs::read(&path).expect("the marker is read"), bound);
        assert!(!marker_temp(&path).exists(), "the resync renames its temp");
        assert_eq!(
            marker_names(root.path()),
            vec!["tier.mode".to_owned()],
            "the resync leaves one marker file"
        );
    }

    /// A fresh pg construction creates the marker once, and an `--adopt`
    /// reopen whose marker disagrees with the database replaces it with the
    /// bound identity.
    #[test]
    fn a_pg_construction_writes_the_marker_once_and_adopt_resyncs_it() {
        if !require_dsn() {
            return;
        }
        let db = Runtime::new()
            .expect("a runtime")
            .block_on(TestDb::create())
            .expect("a scratch database");
        let root = tempfile::tempdir().expect("a temporary root");
        let pg_config = PgConfig::new(Secret::from(SecretKey::from(db.dsn.clone())), 4);
        let root_path = root.path().to_path_buf();
        let first = on_its_own_runtime({
            let pg_config = pg_config.clone();
            let root_path = root_path.clone();
            move || {
                let tier = Runtime::new()
                    .expect("a runtime")
                    .block_on(TierStorage::open_pg(
                        &root_path,
                        testing::fs_options(),
                        config(FlushMode::Sync),
                        &pg_config,
                        AccessMode::ReadWrite,
                        false,
                    ))
                    .expect("the pg layer opens");
                let marker = fs::read(marker_path(&root_path)).expect("the marker is read");
                drop(tier);
                marker
            }
        });
        assert_eq!(first.len(), ROOT_ID_LEN);
        assert_eq!(
            marker_names(&root_path),
            vec!["tier.mode".to_owned()],
            "a fresh pg construction leaves one marker file and no temp"
        );

        // An index that predates its identity and a marker from an interrupted
        // mint: only `--adopt` can bind them to the database's identity.
        fs::remove_file(index_path(&root_path)).expect("the index is removed");
        fs::write(marker_path(&root_path), [7u8; ROOT_ID_LEN])
            .expect("the provisional marker exists");
        on_its_own_runtime({
            let pg_config = pg_config.clone();
            let root_path = root_path.clone();
            move || {
                let tier = Runtime::new()
                    .expect("a runtime")
                    .block_on(TierStorage::open_pg(
                        &root_path,
                        testing::fs_options(),
                        config(FlushMode::Sync),
                        &pg_config,
                        AccessMode::ReadWrite,
                        true,
                    ))
                    .expect("the adopt open binds the root");
                drop(tier);
            }
        });
        assert_eq!(
            fs::read(marker_path(&root_path)).expect("the marker is read"),
            first,
            "adopt resynced the marker to the database's identity"
        );
        Runtime::new().expect("a runtime").block_on(db.drop_db());
    }

    /// Run one phase of a restart leg on a runtime of its own, so the
    /// ownership lock the previous phase held is gone before the next open.
    fn on_its_own_runtime<T, F>(f: F) -> T
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        thread::spawn(f).join().expect("the phase ends")
    }

    /// A read-only run lists the quarantine but never clears it: it is one of
    /// the paths that would write the layer's state directory (T27).
    #[test]
    fn clear_quarantine_refuses_a_read_only_run() {
        let root = tempfile::tempdir().expect("a temporary root");
        drop(open(root.path()));
        let bucket = bucket::name("data").expect("a valid bucket name");
        let entry = quarantined_entry();
        quarantine::record(root.path(), &bucket, &entry).expect("the record is written");
        let mut options = testing::fs_options();
        options.access = AccessMode::ReadOnly;

        let tier = TierStorage::open(
            root.path(),
            options,
            config(FlushMode::Sync),
            RedbContent::in_memory().expect("an in-memory cold plane"),
        )
        .expect("the read-only layer opens");

        let quarantined = tier.quarantined().expect("the quarantine is read");
        assert_eq!(quarantined.len(), 1, "the read-only run lists the entry");
        assert_eq!(quarantined[0].key, entry.info.key);
        assert_eq!(quarantined[0].len, 4);
        assert!(matches!(tier.clear_quarantine(), Err(Error::ReadOnly)));
    }

    /// A construction books the backlog it seeds: a root holding one pair
    /// reports 1 as soon as it is opened, writable or read-only.
    #[test]
    fn opening_books_the_seeded_quarantine_gauge() {
        let _window = test_lock::window();
        let root = tempfile::tempdir().expect("a temporary root");
        let bucket = bucket::name("data").expect("a valid bucket name");
        quarantine::record(root.path(), &bucket, &quarantined_entry())
            .expect("the pair is written");

        drop(open(root.path()));
        assert_eq!(
            gauge("tinio_tier_quarantine", &[]),
            1.0,
            "the writable open books the seeded backlog"
        );

        record_tier_quarantine(0);
        let mut options = testing::fs_options();
        options.access = AccessMode::ReadOnly;
        let tier = TierStorage::open(
            root.path(),
            options,
            config(FlushMode::Sync),
            RedbContent::in_memory().expect("an in-memory cold plane"),
        )
        .expect("the read-only layer opens");
        assert_eq!(
            gauge("tinio_tier_quarantine", &[]),
            1.0,
            "the read-only open books the seeded backlog"
        );
        assert_eq!(tier.quarantined().expect("the list is read").len(), 1);
    }

    /// A construction books whether the layer serves read-only, on the same
    /// series the fs and pg backends use, so a served `/metrics` reports the
    /// tier's own access mode. The test-support constructor takes no access
    /// mode, so it books writable whatever fs it is handed.
    #[test]
    fn opening_books_the_tier_read_only_gauge() {
        let _window = test_lock::window();
        let root = tempfile::tempdir().expect("a temporary root");
        drop(open(root.path()));
        assert_eq!(
            gauge("tinio_storage_read_only", &[("backend", "tier")]),
            0.0,
            "a writable open books zero"
        );

        let mut options = testing::fs_options();
        options.access = AccessMode::ReadOnly;
        drop(
            TierStorage::open(
                root.path(),
                options,
                config(FlushMode::Sync),
                RedbContent::in_memory().expect("an in-memory cold plane"),
            )
            .expect("the read-only layer opens"),
        );
        assert_eq!(
            gauge("tinio_storage_read_only", &[("backend", "tier")]),
            1.0,
            "a read-only open books one"
        );

        let fs = FsStorage::new(root.path(), testing::fs_options()).expect("the fs opens");
        drop(TierStorage::from_parts(
            Arc::new(fs),
            RedbContent::in_memory().expect("an in-memory cold plane"),
            Arc::new(Index::open_in_memory().expect("the index opens")),
            config(FlushMode::Sync),
        ));
        assert_eq!(
            gauge("tinio_storage_read_only", &[("backend", "tier")]),
            0.0,
            "the test-support constructor always books writable"
        );
    }

    #[test]
    fn storage_class_reports_ia_only_for_a_live_cold_placement() {
        Runtime::new().expect("a runtime").block_on(async {
            let root = tempfile::tempdir().expect("a temporary root");
            let fs = FsStorage::new(root.path(), testing::fs_options()).expect("the fs opens");
            let tier = TierStorage::from_parts(
                Arc::new(fs),
                RedbContent::in_memory().expect("an in-memory cold plane"),
                Arc::new(Index::open_in_memory().expect("the index opens")),
                config(FlushMode::Sync),
            );
            let bucket = bucket::name("data").expect("a valid bucket name");
            tier.create_bucket(&bucket, None, &Acl::default_private(None))
                .await
                .expect("the bucket is created");
            let key = object::key("a.bin").expect("a valid key");
            tier.put_object(&bucket, &key, body(b"abc"))
                .await
                .expect("the object is written");
            let info = tier
                .head_object(&bucket, &key)
                .await
                .expect("the object is headed");
            assert_eq!(
                tier.storage_class(&bucket, &key)
                    .await
                    .expect("the class answers"),
                Class::Standard,
                "a warm object is STANDARD"
            );

            let placement = |state: State, cold: Option<ColdInfo>| Placement {
                tier: Tier::Pg,
                state,
                cold,
                mtime: info.last_modified,
            };
            let cold = Some(ColdInfo::from_info(&info));
            for (state, expected) in [
                (State::Live, Class::StandardIa),
                (State::Copying, Class::Standard),
                (State::Deleting, Class::Standard),
            ] {
                tier.index()
                    .set_placement(&bucket, &key, &placement(state, cold.clone()))
                    .expect("the placement is written");
                assert_eq!(
                    tier.storage_class(&bucket, &key)
                        .await
                        .expect("the class answers"),
                    expected,
                    "a {state:?} cold placement answers {expected:?}"
                );
            }

            tier.index()
                .set_placement(&bucket, &key, &placement(State::Live, None))
                .expect("the placement is written");
            assert_eq!(
                tier.storage_class(&bucket, &key)
                    .await
                    .expect("the class answers"),
                Class::Standard,
                "a placement with no cold metadata is STANDARD"
            );
        });
    }

    /// A transition can only demote: a warm object's `STANDARD` request is
    /// refused and leaves the warm plane in place.
    #[test]
    fn a_standard_transition_refuses_and_leaves_the_object_warm() {
        Runtime::new().expect("a runtime").block_on(async {
            let root = tempfile::tempdir().expect("a temporary root");
            let tier = open(root.path());
            let bucket = bucket::name("data").expect("a valid bucket name");
            tier.create_bucket(&bucket, None, &Acl::default_private(None))
                .await
                .expect("the bucket is created");
            let key = object::key("kept.bin").expect("a valid key");
            tier.put_object(&bucket, &key, body(b"abc"))
                .await
                .expect("the object is written");

            let refused = tier
                .transition(&bucket, &key, Class::Standard, SystemTime::now())
                .await;

            assert!(
                matches!(
                    refused,
                    Err(Error::Storage(storage::Error::InvalidRequest(_)))
                ),
                "{refused:?}"
            );
            assert_eq!(
                tier.storage_class(&bucket, &key)
                    .await
                    .expect("the class answers"),
                Class::Standard,
                "the refused promotion left the object warm"
            );
            assert!(
                tier.index()
                    .placement(&bucket, &key)
                    .expect("the placement reads")
                    .is_some_and(|placement| placement.is_warm_live()),
                "the refused promotion left the placement warm"
            );
        });
    }
}
