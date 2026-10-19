//! `PgStorage`: the `Storage` categories over the shared ops engine.
//!
//! The operation bodies are the engine's (`tinio_ops`); this module holds what only this
//! backend can do — the construction path with the access mode and the ownership lock, the
//! receivers over one store, the error projection, and the three bodies the engine
//! deliberately leaves to a backend (the multipart completion, the bucket teardown and the
//! version facts a plane-free read cannot fill).
//!
//! # The two policies
//!
//! Declared, never inferred: a metadata-row write that fails after the content is published is
//! the operation's answer (`MetadataFailure::Fail`) — the row **is** the object here, so a
//! write whose row did not land has not happened, and the engine compensates rather than
//! logging and healing — and a bucket requires its row (`BucketRow::Guaranteed`), because a
//! bucket in PostgreSQL is a row and nothing else. Both match `tinio-mem`'s pair; fs's
//! `Warn`/`Lazy` is the shape of a store whose content is the truth, which pg is not (PG23).

use std::{sync::Arc, time::SystemTime};

use async_trait::async_trait;
use derive_more::Debug;
use secrecy::ExposeSecret;
#[cfg(feature = "versioning")]
use tinio_core::storage::VersionOps;
use tinio_core::{
    acl::{Acl, AclGrants, OwnerId},
    bucket::{self, Bucket},
    checksum,
    content::ContentKey,
    cors, lifecycle,
    meta::{BucketMeta, UploadMeta, VersionMeta, bucket::Row as MetaBucket},
    multipart::{self, CompletedPart, ObjectPart, PartNumber},
    object,
    storage::{
        self, BodyStream, BucketOps, BucketsListing, ByteRange, GetObjectResult, ListBucketsParams,
        ListObjectsParams, ListPartsParams, ListUploadsParams, MultipartOps, ObjectListing,
        ObjectOps, PartsListing, Storage, TagWriteResult, UploadsListing,
    },
    version,
};
use tinio_ops::{
    BucketOps as EngineBucketOps, BucketRow, MetadataFailure, MultipartOps as EngineMultipartOps,
    ObjectOps as EngineObjectOps, StagedBody,
};

#[cfg(feature = "versioning")]
use crate::version::{Plane, plane};
use crate::{
    _store_pg::{Access, PgStore, content::PgStaged, pool::Pool, store::OWNERSHIP_RECHECK},
    Config, Error,
    hooks::PgHooks,
    readonly::Subject,
};

/// The pg backend: the `Storage` categories over one store and its pool.
///
/// The store is held behind an `Arc` because the engine's receivers take `Arc<M>` and
/// `Arc<C>` handles of the same value — pg's row plane and content plane are one store, so
/// the two handles are two clones of one allocation rather than two stores.
///
/// # Examples
///
/// ```ignore
/// use tinio_pg::{Access, Config, PgStorage};
/// use tinio_util::secret::{Secret, SecretKey};
///
/// # async fn example() -> Result<(), tinio_pg::Error> {
/// let config = Config::new(
///     Secret::from(SecretKey::from("postgres://user:password@db.example/tinio".to_owned())),
///     8,
/// );
/// // A writable instance migrates the database, takes the ownership lock and holds it for
/// // the process's life (PG11/PG12); a read-only one verifies the schema, takes no lock and
/// // answers the mutating operations at the entry.
/// let storage = PgStorage::connect(&config, Access::ReadWrite).await?;
/// let buckets = storage.list_buckets(ListBucketsParams::default(), None, None).await?;
/// # let _ = buckets;
/// # Ok(())
/// # }
/// ```
///
/// (`ignore`: every construction needs a live PostgreSQL server, so a runnable example would
/// be a fake — the constructor's own doc says what the two modes do and what a refusal names.)
#[derive(Debug, Clone)]
pub struct PgStorage {
    /// The store: the row plane, the content plane and the pool every statement runs on.
    pub(crate) store: Arc<PgStore>,
    /// The version plane (spec E16): the state engine, the version rows and the block plane
    /// over the same store. Built once, beside the store — every version body takes it, and
    /// rebuilding it per call would mint a second `Versions` for one database on every
    /// request.
    #[cfg(feature = "versioning")]
    #[debug(skip)]
    pub(crate) plane: Arc<Plane>,
}

impl PgStorage {
    /// Construct a pg backend for `config` in `access`'s mode.
    ///
    /// The dsn and the pool size are the caller's resolution — `[storage.pg]`'s section with
    /// `TINIO_PG_DSN`'s override folded onto it (PG5) — and the mode is the run's
    /// (PG6/PG12), so this call is the whole construction:
    ///
    /// - **A writable instance migrates**: the schema is brought to this binary's version
    ///   under the migration lock, and a database ahead of the binary is refused with the pg
    ///   remedy (PG11).
    /// - **A writable instance takes the ownership lock** ([`PgStore::hold_ownership`]) on a
    ///   dedicated out-of-pool connection. A second writable instance is refused here, and
    ///   the refusal names the session that holds the lock (PG12). The lock is then held for
    ///   the process's life by a task that re-verifies it on the store's cadence; a lost lock
    ///   terminates the process rather than letting a second writer write beside this one.
    /// - **A read-only instance does neither**: it verifies the schema version and refuses a
    ///   mismatch, and it takes no lock, so it starts beside the writer.
    ///
    /// # Errors
    ///
    /// The store's refusals, projected: a connect failure (host and database named, the dsn
    /// redacted), a non-UTF-8 server, a schema mismatch or a newer database, and the
    /// ownership lock's holder.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use tinio_pg::{Access, Config, PgStorage};
    ///
    /// # async fn example() -> Result<(), tinio_pg::Error> {
    /// let config = Config::new(dsn, 8);
    /// let storage = PgStorage::connect(&config, Access::ReadOnly).await?;
    /// # let _ = storage;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// (`ignore`: the construction opens a pool against a live server and, writable, migrates
    /// it — an example that ran would need the compose fixture up. What it would have shown is
    /// the pair above: the mode is the whole difference, and a refusal names its remedy.)
    pub async fn connect(config: &Config, access: Access) -> Result<Self, Error> {
        // The session is chosen by the mode (PG6, layer 2): a read-only instance's pool opens
        // every connection with `default_transaction_read_only = on` as a startup option, so the
        // setting is in place before the first statement rather than established by one (PG26).
        let dsn = config.dsn().expose_secret();
        let pool = if access.is_writable() {
            Pool::open(dsn.as_ref(), config.pool())
        } else {
            Pool::open_read_only(dsn.as_ref(), config.pool())
        }
        .map_err(storage::Error::from)?;
        Self::connect_with_pool(pool, access).await
    }

    /// Construct a pg backend over an already-built pool — [`PgStorage::connect`]'s second
    /// half, for a caller that built the pool itself (a test's scratch database, a doctor's
    /// inspection).
    ///
    /// **The session the store serves on is `access`'s, whatever pool was handed in.** `access`
    /// decides the entry guard (PG6, layer 1) and the ownership lock (PG12), and the store puts
    /// the pool into that mode before it serves: a pool that already carries the mode is used as
    /// it is, and an [`Access::ReadOnly`] store handed a writable pool gets one whose connections
    /// are opened with `default_transaction_read_only = on` (the store's `Pool::reopened_for`) —
    /// the session layer (PG6, layer 2) is therefore always on for a read-only instance, never
    /// left to the caller's pool. The
    /// opposite direction, a **writable** store over a read-only pool, is refused: that mode
    /// cannot be rebuilt from the pool.
    ///
    /// So the layers line up as the deployment expects: a read-only instance is refused by the
    /// entry guard, and by the session behind it; a **writable** session rejects nothing, which is
    /// where the role's missing `INSERT`/`UPDATE`/`DELETE` grants (PG6, layer 3) become the one
    /// layer still standing — the deployment contract, and the layer the conformance suite's
    /// layer-3 leg exercises by driving `tinio-store-pg`'s contract directly over a restricted
    /// role, past this backend's guard. A read-only deployment pairs [`Access::ReadOnly`] with a
    /// role without DML grants either way.
    ///
    /// # Errors
    ///
    /// As [`PgStorage::connect`].
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use tinio_pg::{Access, PgStorage};
    /// use tinio_store_pg::pool::Pool;
    ///
    /// # async fn example() -> Result<(), tinio_pg::Error> {
    /// let pool = Pool::open(dsn, 4).map_err(tinio_core::storage::Error::from)?;
    /// let storage = PgStorage::connect_with_pool(pool, Access::ReadWrite).await?;
    /// # let _ = storage;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// (`ignore`: the same live server as [`PgStorage::connect`]'s example, with the pool the
    /// caller built. What it would have shown is that the mode and the guard are this call's
    /// too — `connect` only adds the pool.)
    pub async fn connect_with_pool(pool: Pool, access: Access) -> Result<Self, Error> {
        let store = PgStore::connect_with_pool(pool, access)
            .await
            .map_err(storage::Error::from)?;
        if access.is_writable() {
            // PG12's ownership: the guard is taken **before** the storage exists, so a second
            // writable instance's refusal is this construction's answer and no operation can
            // ever run under a lock this process does not hold. `verify_forever` consumes the
            // guard and never returns while the lock holds — the re-verify probes the lock on
            // the guard's own dedicated connection (which is also what keeps the connection's
            // TCP path warm), and a lost lock logs and exits with
            // `OWNERSHIP_LOST_EXIT`. The task owns the guard for the process's life, so the
            // lock is held as long as this process serves; dropping the storage neither
            // releases it nor stops the re-verify, which is deliberate: a second writer
            // must never start beside a live first one.
            let guard = store.hold_ownership().await.map_err(storage::Error::from)?;
            tokio::spawn(guard.verify_forever(OWNERSHIP_RECHECK));
        }
        let store = Arc::new(store);
        #[cfg(feature = "versioning")]
        let plane = Arc::new(plane(Arc::clone(&store)));
        Ok(Self {
            store,
            #[cfg(feature = "versioning")]
            plane,
        })
    }

    /// The store this backend serves: the row plane, the content plane and the pool.
    ///
    /// Its own surface is public (`tinio_store_pg`), so a caller that needs the store directly
    /// — a doctor reading the schema state, a `Cleanup` pass through `inspect` — takes it here
    /// rather than holding a second store over the same pool.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use tinio_pg::{Access, PgStorage};
    ///
    /// # async fn example(storage: PgStorage) {
    /// // The database the pool is connected to, which is the identity a diagnostic prints.
    /// assert!(!storage.store().database().is_empty());
    /// # }
    /// ```
    ///
    /// (`ignore`: the value must be constructed against a live server, which the constructor's
    /// example records. What it would have shown is that the accessor hands out the store the
    /// backend runs on, never a second one.)
    pub fn store(&self) -> &PgStore {
        &self.store
    }

    /// The bucket-group receiver over this backend's handles.
    ///
    /// The two policies are pg's, declared and never inferred: a metadata-row write that fails
    /// after the content is published is the **caller's** to compensate
    /// ([`MetadataFailure::Fail`] — the row is the object), and the bucket's row **is** the
    /// bucket ([`BucketRow::Guaranteed`]).
    fn buckets(&self) -> EngineBucketOps<PgStore, PgStore, PgHooks> {
        EngineBucketOps {
            meta: Arc::clone(&self.store),
            content: Arc::clone(&self.store),
            hooks: self.hooks(),
            metadata_failure: MetadataFailure::Fail,
            bucket_row: BucketRow::Guaranteed,
        }
    }

    /// The engine's [`Hooks`](tinio_ops::Hooks) seam over this backend's store: the empty
    /// bucket-delete action and the store's per-upload part lock.
    fn hooks(&self) -> PgHooks {
        PgHooks::new(Arc::clone(&self.store))
    }

    /// The object-group receiver over this backend's handles, with the same two policies.
    pub(crate) fn objects(&self) -> EngineObjectOps<PgStore, PgStore> {
        EngineObjectOps {
            meta: Arc::clone(&self.store),
            content: Arc::clone(&self.store),
            metadata_failure: MetadataFailure::Fail,
            bucket_row: BucketRow::Guaranteed,
        }
    }

    /// The multipart-group receiver: the same handles and the same bucket-row policy, plus the
    /// seam whose part lock is the store's per-upload advisory lock (see [`PgHooks`](crate::hooks)).
    fn parts(&self) -> EngineMultipartOps<PgStore, PgStore, PgHooks> {
        EngineMultipartOps {
            meta: Arc::clone(&self.store),
            content: Arc::clone(&self.store),
            hooks: self.hooks(),
            bucket_row: BucketRow::Guaranteed,
        }
    }

    /// Whether this bucket records versions (its state row is `Enabled` or `Suspended`).
    ///
    /// The store's contract, not the version plane's: the state lives in a table this crate
    /// reads in every build, so the plain listing's `records_versions` bit is true in a
    /// stripped build too. What a stripped build cannot do is act on it — every version
    /// operation is refused there (PG16's twin), which is the same degradation fs's
    /// feature-off build has.
    async fn records_versions(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        let state = VersionMeta::bucket_state(&*self.store, bucket)
            .await
            .map_err(storage::Error::from)?;
        Ok(state.records_versions())
    }

    /// The key rules an object operation opens with, before any plane is touched (FR-006/FR-020).
    ///
    /// The reserved segment is refused **at any depth**, and the class is the operation's: a
    /// **write** answers the contract's access-denied refusal — the caller asked for a key the
    /// API reserves, and a create must not silently succeed — while a **read** answers the
    /// not-found class, because the key is not an object and never was.
    fn refuse_reserved(key: &object::Key) -> Result<(), Error> {
        if key.is_reserved() {
            return Err(storage::access_denied(key).into());
        }
        Ok(())
    }

    /// The read-side rule: a reserved key and a folder-marker key are both no object — the
    /// marker is a namespace entry, not content (`GET`/`HEAD` of one answer `NoSuchKey`, while
    /// `PUT` creates it and `DELETE` removes it).
    fn refuse_non_object(key: &object::Key) -> Result<(), Error> {
        if key.is_reserved() || key.is_folder_marker() {
            return Err(storage::no_such_key(key).into());
        }
        Ok(())
    }

    /// The bucket teardown: one transaction that refuses a non-empty bucket and
    /// otherwise removes the whole namespace together (R3).
    ///
    /// The engine's `delete_bucket` runs its three probes (live content, upload
    /// rows and object rows — a delete marker is one) and then the seam, which
    /// is empty for pg ([`PgHooks`](crate::hooks)). Those probes refuse a
    /// marker-only bucket up front, but they run before this teardown's
    /// transaction and outside its lock, so the store re-probes the same rows
    /// inside it: an object row, an upload or a live generation refuses the
    /// delete before any version row is removed, and a refusal rolls everything
    /// back. Blocks stay for the sweep, because liveness is derived from the
    /// version rows.
    ///
    /// # Errors
    ///
    /// `NoSuchBucket` for a bucket with no row, `NotEmpty` when an object row,
    /// an upload or a live generation still names it.
    async fn teardown_bucket(&self, name: &bucket::Name) -> Result<(), Error> {
        // One transaction. A marker-only bucket is not empty, and a refusal
        // must not already have committed the version deletes.
        self.store.teardown_bucket(name).await?;
        Ok(())
    }
}

impl Storage for PgStorage {
    type Error = Error;
}

#[async_trait]
impl storage::LifecycleRules for PgStorage {
    async fn lifecycles(&self) -> Result<Vec<(bucket::Name, lifecycle::Config)>, storage::Error> {
        BucketMeta::lifecycles(self.store())
            .await
            .map_err(Into::into)
    }
}

#[async_trait]
impl BucketOps for PgStorage {
    async fn create_bucket(
        &self,
        name: &bucket::Name,
        owner: Option<&OwnerId>,
        acl: &Acl,
    ) -> Result<(), Error> {
        self.writable(Subject::Bucket(name))?;
        // The row is the namespace [`ObjectContent::stage`] locks. The engine
        // records it after the content plane's create, and that create inserts
        // the same row, so the engine's following insert would answer
        // `AlreadyExists` on the first create. Write the caller's row here.
        let mut row = MetaBucket::at(SystemTime::now());
        row.owner = owner.cloned();
        row.acl = Acl {
            owner: None,
            grants: acl.grants.clone(),
        };
        BucketMeta::create(&*self.store, name, &row)
            .await
            .map_err(storage::Error::from)?;
        Ok(())
    }

    async fn delete_bucket(&self, name: &bucket::Name) -> Result<(), Error> {
        self.writable(Subject::Bucket(name))?;
        // The engine's three probes (live content, upload rows and object rows —
        // a delete marker is one) run first and write nothing. The store's own
        // statement then refuses a delete marker — an object row with no live
        // bytes — and deletes the version rows only in the same statement that
        // removes the bucket.
        self.buckets()
            .delete_bucket(name)
            .await
            .map_err(Error::from)?;
        self.teardown_bucket(name).await
    }

    async fn head_bucket(&self, name: &bucket::Name) -> Result<Bucket, Error> {
        self.buckets().head_bucket(name).await.map_err(Error::from)
    }

    async fn list_buckets(
        &self,
        params: ListBucketsParams,
        owner: Option<&OwnerId>,
        lazy_default: Option<&OwnerId>,
    ) -> Result<BucketsListing, Error> {
        self.buckets()
            .list_buckets(params, owner, lazy_default)
            .await
            .map_err(Error::from)
    }

    async fn get_bucket_tags(&self, name: &bucket::Name) -> Result<object::Tags, Error> {
        self.buckets()
            .get_bucket_tags(name)
            .await
            .map_err(Error::from)
    }

    async fn put_bucket_tags(&self, name: &bucket::Name, tags: &object::Tags) -> Result<(), Error> {
        self.writable(Subject::Bucket(name))?;
        self.buckets()
            .put_bucket_tags(name, tags)
            .await
            .map_err(Error::from)
    }

    async fn delete_bucket_tags(&self, name: &bucket::Name) -> Result<(), Error> {
        self.writable(Subject::Bucket(name))?;
        self.buckets()
            .delete_bucket_tags(name)
            .await
            .map_err(Error::from)
    }

    async fn get_bucket_cors(&self, name: &bucket::Name) -> Result<Option<cors::Config>, Error> {
        self.buckets()
            .get_bucket_cors(name)
            .await
            .map_err(Error::from)
    }

    async fn put_bucket_cors(
        &self,
        name: &bucket::Name,
        config: &cors::Config,
    ) -> Result<(), Error> {
        self.writable(Subject::Bucket(name))?;
        self.buckets()
            .put_bucket_cors(name, config)
            .await
            .map_err(Error::from)
    }

    async fn delete_bucket_cors(&self, name: &bucket::Name) -> Result<(), Error> {
        self.writable(Subject::Bucket(name))?;
        self.buckets()
            .delete_bucket_cors(name)
            .await
            .map_err(Error::from)
    }

    async fn get_bucket_lifecycle(
        &self,
        name: &bucket::Name,
    ) -> Result<Option<lifecycle::Config>, Error> {
        self.buckets()
            .get_bucket_lifecycle(name)
            .await
            .map_err(Error::from)
    }

    async fn put_bucket_lifecycle(
        &self,
        name: &bucket::Name,
        config: &lifecycle::Config,
    ) -> Result<(), Error> {
        self.writable(Subject::Bucket(name))?;
        self.buckets()
            .put_bucket_lifecycle(name, config)
            .await
            .map_err(Error::from)
    }

    async fn merge_bucket_lifecycle(
        &self,
        name: &bucket::Name,
        rules: &[lifecycle::Rule],
    ) -> Result<lifecycle::Merged, Error> {
        self.writable(Subject::Bucket(name))?;
        self.buckets()
            .merge_bucket_lifecycle(name, rules)
            .await
            .map_err(Error::from)
    }

    async fn delete_bucket_lifecycle(&self, name: &bucket::Name) -> Result<(), Error> {
        self.writable(Subject::Bucket(name))?;
        self.buckets()
            .delete_bucket_lifecycle(name)
            .await
            .map_err(Error::from)
    }

    async fn get_bucket_acl(&self, name: &bucket::Name) -> Result<Acl, Error> {
        self.buckets()
            .get_bucket_acl(name)
            .await
            .map_err(Error::from)
    }

    async fn put_bucket_acl(&self, name: &bucket::Name, grants: &AclGrants) -> Result<(), Error> {
        self.writable(Subject::Bucket(name))?;
        self.buckets()
            .put_bucket_acl(name, grants)
            .await
            .map_err(Error::from)
    }
}

#[async_trait]
impl ObjectOps for PgStorage {
    /// The engine's staged body over this store's content plane: `stage_body` and
    /// `commit_object` are engine-owned on every backend, and the value is the engine's own.
    type StagedBody = StagedBody<PgStaged>;

    async fn stage_body(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        body: BodyStream,
        tee: Option<Arc<checksum::PartChecksum>>,
        carried: Option<checksum::Recorded>,
    ) -> Result<Self::StagedBody, Error> {
        self.writable(Subject::Key(key))?;
        Self::refuse_reserved(key)?;
        self.objects()
            .stage_body(bucket, key, body, tee, carried)
            .await
            .map_err(Error::from)
    }

    async fn commit_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        staged: Self::StagedBody,
        tags: object::Tags,
        owner: Option<&OwnerId>,
        acl: &Acl,
    ) -> Result<object::Info, Error> {
        self.writable(Subject::Key(key))?;
        Self::refuse_reserved(key)?;
        self.commit_put(bucket, key, staged, tags, owner, acl).await
    }

    async fn get_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        range: Option<ByteRange>,
    ) -> Result<GetObjectResult, Error> {
        Self::refuse_non_object(key)?;
        let mut result = self
            .objects()
            .get_object(bucket, key, range)
            .await
            .map_err(Error::from)?;
        self.fill_version_facts(bucket, key, &mut result.info)
            .await?;
        Ok(result)
    }

    /// Server-side copy: the source's bytes stream into a fresh stage and the **same**
    /// commit path every other write takes.
    ///
    /// The trait's default would do exactly this and drop the caller's `checksum` (its body
    /// has no parameter for it), and the store's own same-bucket `ObjectContent::copy` fast
    /// path would bypass the version branch and the composed-record rules a commit owns — so
    /// the body is spelled here, and the four lines are the whole of what "copy is a fresh
    /// object" means: the source's bytes, the caller's tags/owner/ACL (never the source's),
    /// and the caller's recorded checksum.
    async fn copy_object(
        &self,
        src_bucket: &bucket::Name,
        src_key: &object::Key,
        dst_bucket: &bucket::Name,
        dst_key: &object::Key,
        tags: object::Tags,
        owner: Option<&OwnerId>,
        acl: &Acl,
        checksum: Option<checksum::Recorded>,
    ) -> Result<object::Info, Error> {
        self.writable(Subject::Key(dst_key))?;
        let get = self.get_object(src_bucket, src_key, None).await?;
        let staged = self
            .stage_body(dst_bucket, dst_key, get.body, None, checksum)
            .await?;
        self.commit_object(dst_bucket, dst_key, staged, tags, owner, acl)
            .await
    }

    async fn rename_object(
        &self,
        bucket: &bucket::Name,
        src: &object::Key,
        dst: &object::Key,
    ) -> Result<object::Info, Error> {
        // The guard precedes even the degenerate `src == dst` arm: a rename is a write-class
        // operation, so a read-only instance refuses it rather than answering a head's result.
        self.writable(Subject::Key(dst))?;
        // The degenerate move is idempotent, exactly as fs and mem answer it (the engine's
        // `rename_object` carries the same guard, and it must run before the key refusals so a
        // `src == dst` marker reads as `NoSuchKey` like a head, not as a bad destination).
        if src == dst {
            return self.head_object(bucket, src).await;
        }
        // The key rules first, exactly as fs and mem apply them at this point — the engine's
        // `rename_object` carries no guard of its own, so the backend is where they live: a
        // reserved destination is a write through the reserved segment (the contract's
        // access-denied refusal), a marker destination cannot hold an object (`InvalidKey`),
        // and a reserved or marker **source** is never an object (`NoSuchKey`, like head).
        // Without them a rename onto `a/.tinio/b` would materialize a reserved key on pg where
        // fs and mem refuse it.
        Self::refuse_reserved(dst)?;
        if dst.is_folder_marker() {
            return Err(storage::invalid_key(dst.to_string()).into());
        }
        Self::refuse_non_object(src)?;
        // The contract's rename guard: a bucket that records versions refuses the move with
        // the access-denied class naming `dst` (the contract's defaulted rule, applied by the
        // trait's own `ensure_rename_allowed` — never spelled here). A stripped build has no
        // state to consult and takes the version-free path, the same degradation every
        // version operation has there (PG16).
        #[cfg(feature = "versioning")]
        VersionOps::ensure_rename_allowed(self, bucket, dst).await?;
        let mut info = self
            .objects()
            .rename_object(bucket, src, dst)
            .await
            .map_err(Error::from)?;
        self.fill_version_facts(bucket, dst, &mut info).await?;
        Ok(info)
    }

    async fn head_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<object::Info, Error> {
        Self::refuse_non_object(key)?;
        let mut info = self
            .objects()
            .head_object(bucket, key)
            .await
            .map_err(Error::from)?;
        self.fill_version_facts(bucket, key, &mut info).await?;
        Ok(info)
    }

    async fn delete_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<version::Marker, Error> {
        self.writable(Subject::Key(key))?;
        // A delete is a write-class operation, so the reserved segment refuses it in that
        // class; a folder marker is deletable (it is how a marker is removed) and the
        // delete is idempotent either way.
        Self::refuse_reserved(key)?;
        // A bucket that records versions creates a delete marker. The branch
        // lives with the version plane, so this body is the same call in every
        // build; a stripped build's answer is `None` and the plain delete runs.
        if let Some(marker) = self.delete_if_versioned(bucket, key).await? {
            return Ok(marker);
        }
        self.objects()
            .delete_object(bucket, key)
            .await
            .map_err(Error::from)
    }

    async fn get_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<object::Tags, Error> {
        self.objects()
            .get_object_tags(bucket, key)
            .await
            .map_err(Error::from)
    }

    async fn put_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<TagWriteResult, Error> {
        self.writable(Subject::Key(key))?;
        let mut answer = self
            .objects()
            .put_object_tags(bucket, key, tags)
            .await
            .map_err(Error::from)?;
        self.fill_tag_facts(bucket, key, &mut answer).await?;
        Ok(answer)
    }

    async fn delete_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<TagWriteResult, Error> {
        self.writable(Subject::Key(key))?;
        let mut answer = self
            .objects()
            .delete_object_tags(bucket, key)
            .await
            .map_err(Error::from)?;
        self.fill_tag_facts(bucket, key, &mut answer).await?;
        Ok(answer)
    }

    async fn get_object_acl(&self, bucket: &bucket::Name, key: &object::Key) -> Result<Acl, Error> {
        self.objects()
            .get_object_acl(bucket, key)
            .await
            .map_err(Error::from)
    }

    async fn put_object_acl(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        grants: &AclGrants,
    ) -> Result<(), Error> {
        self.writable(Subject::Key(key))?;
        self.objects()
            .put_object_acl(bucket, key, grants)
            .await
            .map_err(Error::from)
    }

    async fn list_objects(&self, params: ListObjectsParams) -> Result<ObjectListing, Error> {
        // The bucket row first: a missing bucket is **not** an empty listing. The engine's
        // bucket-row policy does not cover this path, and the state read below cannot either —
        // the store answers `Unversioned` for a bucket with no row, which would list an empty page.
        if BucketMeta::row(&*self.store, &params.bucket)
            .await
            .map_err(storage::Error::from)?
            .is_none()
        {
            return Err(storage::no_such_bucket(&params.bucket).into());
        }
        // The listing's records-versions bit decides whether a key whose current version is a
        // delete marker is listed at all, so it is read from the store's own state row rather
        // than assumed.
        let records = self.records_versions(&params.bucket).await?;
        self.objects()
            .list_objects(params, records)
            .await
            .map_err(Error::from)
    }

    async fn list_object_parts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Vec<ObjectPart>, Error> {
        self.objects()
            .list_object_parts(bucket, key)
            .await
            .map_err(Error::from)
    }
}

#[async_trait]
impl MultipartOps for PgStorage {
    async fn create_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        checksum: Option<checksum::Upload>,
        tags: object::Tags,
        owner: Option<&OwnerId>,
        acl: &Acl,
    ) -> Result<multipart::MultipartUpload, Error> {
        self.writable(Subject::Key(key))?;
        // The reserved segment refuses a multipart create too: the upload is the write path's
        // first step, and a create that succeeded would be a backdoor for materializing it
        // (FR-020).
        Self::refuse_reserved(key)?;
        self.parts()
            .create_multipart_upload(bucket, key, checksum, tags, owner, acl)
            .await
            .map_err(Error::from)
    }

    async fn get_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<multipart::MultipartUpload, Error> {
        self.parts()
            .get_multipart_upload(bucket, key, upload_id)
            .await
            .map_err(Error::from)
    }

    async fn upload_part(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        part_number: PartNumber,
        body: BodyStream,
        checksum: Option<Arc<checksum::PartChecksum>>,
    ) -> Result<multipart::PartInfo, Error> {
        self.writable(Subject::Key(key))?;
        self.parts()
            .upload_part(bucket, key, upload_id, part_number, body, checksum)
            .await
            .map_err(Error::from)
    }

    async fn list_parts(&self, params: ListPartsParams) -> Result<PartsListing, Error> {
        // The engine owns the upload's identity check, the marker arithmetic and
        // the page's rows and truncation; the sizes are the backend's to join
        // (the contract carries no part stat, D23), and they are one read of the
        // parts' content keys. A row whose generation is gone is skipped, the
        // self-heal every backend's join applies.
        //
        // The mtime is **the part's own** write stamp, which the page's rows
        // carry (`parts.modified_at`, the instant the part row was last
        // written): the ruling fixes a listed part's `LastModified` as its own
        // stamp rather than the upload's creation time, and the upload's
        // `initiated_at` is only what a row recording none falls back to.
        let page = self
            .parts()
            .list_parts(
                &params.bucket,
                &params.key,
                &params.upload_id,
                params.part_number_marker,
                params.max_parts,
            )
            .await
            .map_err(Error::from)?;
        let Some(upload) =
            UploadMeta::get(&*self.store, &params.bucket, &params.key, &params.upload_id)
                .await
                .map_err(storage::Error::from)?
        else {
            return Err(storage::no_such_upload(&params.upload_id).into());
        };
        let keys: Vec<ContentKey> = page
            .rows
            .iter()
            .map(|row| ContentKey::Part {
                upload_id: params.upload_id.clone(),
                number: row.number.into(),
            })
            .collect();
        let lens = self
            .store
            .live_lens(&params.bucket, &keys)
            .await
            .map_err(storage::Error::from)?;
        let mut parts = Vec::with_capacity(page.rows.len());
        for (row, span) in page.rows.iter().zip(lens) {
            let Some(span) = span else {
                continue;
            };
            parts.push(row.to_info(span.len, upload.initiated_at)?);
        }
        Ok(PartsListing {
            parts,
            truncated: page.truncated,
            next_part_number_marker: page.next_part_number_marker,
        })
    }

    /// Assemble the listed parts into the object — the one body the engine does not carry, and
    /// the reason is the engine's own note: a completion's commit half is a **slot-carrying
    /// parts write**, which no contract call expresses.
    ///
    /// The body is three phases, and the order is the whole of its correctness:
    ///
    /// 1. **Verify** — the parts are joined against the upload's stored rows in the requested
    ///    order and every rule the completion must enforce is checked before the first write:
    ///    the list is non-empty, the numbers strictly ascend, each ETag matches the stored one,
    ///    and a non-final part meets the S3 minimum ([`multipart::check_part_minimum`], the shared rule).
    ///    A refused completion therefore leaves the upload exactly as it was.
    /// 2. **Assemble** — the parts' bytes stream, in order, into one stage under the object's
    ///    own key. Each part is the generation the check measured, so an upload that lands
    ///    during the copy does not change the bytes the ETag was computed from. The publish
    ///    flips the key's head in one statement, so a partial assembly is invisible by
    ///    construction (PG21): no reader can see a shorter object with a composed ETag, or a
    ///    longer one with a shorter ETag. The version branch is opened **before** the publish,
    ///    because the supersede has to move the outgoing version into the block plane while
    ///    its bytes are still the key's — the publish overwrites them.
    /// 3. **Commit and consume** — the row and the retained part list land in one write (a
    ///    completion's purpose is to record what the object was composed of, which is exactly
    ///    what the engine's versioned put deliberately clears), the slot moves to the identity
    ///    the plan minted, then the upload's rows leave in one transaction and its parts' bytes
    ///    after their rows — every stored part, the unlisted extras included, because the
    ///    contract's rule discards them.
    ///
    /// A failure of the row write, the slot move, or the upload drain after the publish
    /// takes the write back while this publish still owns the head, so a retry still
    /// completes this same upload. A newer writer who already replaced the head is left
    /// alone, and the caller still sees the failure. A bucket that records versions runs
    /// the engine's versioned undo and, when the slot already moved, puts the slot it
    /// opened with back. A bucket that does not restores the content head to the
    /// generation that was live before this publish, or clears it when this publish was
    /// the key's first, and puts the previous object row back with the part list it
    /// held. Part bytes deleted after a successful drain are best-effort.
    ///
    /// # Errors
    ///
    /// `NoSuchBucket` for a missing bucket, `NoSuchUpload` for a missing or key-mismatched
    /// upload, `NoParts` for an empty list, `InvalidPart` for a missing, out-of-order or
    /// mismatched part, `PartTooSmall` for a non-final part below the minimum, and the planes'
    /// own failures.
    async fn complete_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        parts: &[CompletedPart],
        checksum: Option<checksum::Recorded>,
    ) -> Result<object::Info, Error> {
        self.writable(Subject::Key(key))?;
        self.finish_multipart_upload(bucket, key, upload_id, parts, checksum)
            .await
    }

    async fn abort_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<(), Error> {
        self.writable(Subject::Key(key))?;
        // The part numbers are read **before** the drain, because the rows are what names the
        // bytes; the engine's drain is the identity check and the one transaction that retires
        // them, and the bytes leave after their rows (a row must never outlive its bytes).
        let stored = UploadMeta::list_parts(&*self.store, bucket, upload_id, 1.into())
            .await
            .map_err(storage::Error::from)?;
        self.parts()
            .abort_multipart_upload(bucket, key, upload_id)
            .await
            .map_err(Error::from)?;
        let removed: Vec<ContentKey> = stored
            .iter()
            .map(|row| ContentKey::Part {
                upload_id: upload_id.to_owned(),
                number: row.number.into(),
            })
            .collect();
        // The rows are gone. A byte-delete failure must not fail the abort:
        // the client would retry against an upload that no longer exists, and
        // the sweep reclaims the parts.
        self.remove_part_bytes_best_effort(bucket, &removed).await;
        Ok(())
    }

    async fn list_multipart_uploads(
        &self,
        params: ListUploadsParams,
    ) -> Result<UploadsListing, Error> {
        self.parts()
            .list_multipart_uploads(params)
            .await
            .map_err(Error::from)
    }
}
