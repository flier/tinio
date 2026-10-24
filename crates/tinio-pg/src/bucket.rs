//! The bucket group of [`PgStorage`]: the `BucketOps` impl, the lifecycle-rule read, and the two
//! inherent helpers only bucket operations take.
//!
//! The bodies are the ops engine's over this backend's store; what stays here is the read-only
//! entry guard on every mutation, the row pg's bucket **is** — the caller's row is written before
//! the engine's create, so the first create does not read as `AlreadyExists` — and the teardown
//! the engine's own probes cannot express. The object and multipart forwards stay in
//! [`crate::storage`]; this module only answers for the bucket surface.

use std::{sync::Arc, time::SystemTime};

use async_trait::async_trait;
use tinio_core::{
    acl::{Acl, AclGrants, OwnerId},
    bucket::{self, Bucket},
    cors, lifecycle,
    meta::{BucketMeta, bucket::Row as MetaBucket},
    object, object_lock,
    storage::{self, BucketOps, BucketsListing, ListBucketsParams},
};
use tinio_ops::{BucketOps as EngineBucketOps, BucketRow, MetadataFailure};

use crate::{_store_pg::PgStore, Error, PgStorage, hooks::PgHooks, readonly::Subject};

impl PgStorage {
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

    /// The bucket teardown: one transaction that refuses a non-empty bucket and
    /// otherwise removes the whole namespace together (R3).
    ///
    /// The engine's `delete_bucket` runs its three probes (live content, upload
    /// rows and object rows — a delete marker is one) and then the seam, which
    /// is empty for pg ([`PgHooks`](crate::hooks)). Those probes refuse a
    /// marker-only bucket up front, but they run before this teardown's
    /// transaction and outside its lock, so the store re-probes the same rows
    /// inside it: an object row, an upload, a live generation, a version row, a
    /// version lock row or an upload lock row refuses the `delete` with
    /// `NotEmpty` before anything is removed, and a refusal rolls everything
    /// back. A bucket holding version history is therefore not empty (OL8), and
    /// no teardown ever destroys a version row or a lock row it did not see.
    /// Blocks stay for the sweep, because liveness is derived from the version
    /// rows.
    ///
    /// # Errors
    ///
    /// `NoSuchBucket` for a bucket with no row, `NotEmpty` when an object row,
    /// an upload, a live generation, a version row or an Object Lock row still
    /// names it.
    async fn teardown_bucket(&self, name: &bucket::Name) -> Result<(), Error> {
        // One transaction. A marker-only bucket is not empty, and a refusal
        // must not already have committed the version deletes.
        self.store
            .teardown_bucket(name)
            .await
            .map_err(Error::from)?;
        Ok(())
    }
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
            .map_err(Error::from)?;
        Ok(())
    }

    /// The one-step lock-enabled create (OL13): the bucket row, its `Enabled`
    /// versioning state and the configuration commit in one store transaction,
    /// so a crash leaves no partial lock-enabled bucket. The default this
    /// overrides fails closed, which is why the atomic store body exists.
    async fn create_bucket_with_lock(
        &self,
        name: &bucket::Name,
        owner: Option<&OwnerId>,
        acl: &Acl,
        config: &object_lock::Config,
    ) -> Result<(), Error> {
        self.writable(Subject::Bucket(name))?;
        let mut row = MetaBucket::at(SystemTime::now());
        row.owner = owner.cloned();
        row.acl = Acl {
            owner: None,
            grants: acl.grants.clone(),
        };
        self.store
            .create_bucket_with_lock(name, &row, config)
            .await
            .map_err(Error::from)
    }

    async fn get_bucket_object_lock(
        &self,
        name: &bucket::Name,
    ) -> Result<Option<object_lock::Config>, Error> {
        self.buckets()
            .get_bucket_object_lock(name)
            .await
            .map_err(Error::from)
    }

    async fn put_bucket_object_lock(
        &self,
        name: &bucket::Name,
        config: &object_lock::Config,
    ) -> Result<(), Error> {
        self.writable(Subject::Bucket(name))?;
        self.buckets()
            .put_bucket_object_lock(name, config)
            .await
            .map_err(Error::from)
    }

    async fn delete_bucket(&self, name: &bucket::Name) -> Result<(), Error> {
        self.writable(Subject::Bucket(name))?;
        // The engine's three probes (live content, upload rows and object rows —
        // a delete marker is one) run first and write nothing. The store's own
        // statement then refuses a bucket holding an object row (a marker
        // included), an upload, a live generation, a version row or an Object
        // Lock row, and removes a clear namespace in one statement. A bucket
        // with version history is not empty (OL8): it is refused here rather
        // than silently drained.
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
