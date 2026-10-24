//! The `BucketOps` implementation for [`MemoryStorage`].
//!
//! Every body is the shared ops engine's ([`crate::_ops`]): the row plane is
//! the crate's metadata handle and the content plane is the accounting
//! decorator, so the whole bucket group is the engine's with no mem-specific
//! arm. What stays here is the error projection.
//!
//! The one step only this backend can take — the bucket teardown, which must
//! stay **one** write transaction with its emptiness probes (ruling
//! 2026-10-08: two transactions would let a bucket re-created between them
//! lose the new bucket's rows and bytes) — is the `Hooks` implementation
//! beside the receiver ([`crate::storage::MemHooks`]). mem takes no version
//! plane (spec E16): its version group and its version-creating writes stay
//! mem-local.

// The create-with-lock override's inputs — the row shape, the content
// plane's create step and the create time — exist only in the versioning
// build, so the imports are gated with it.
#[cfg(feature = "versioning")]
use std::time::SystemTime;

use async_trait::async_trait;
#[cfg(test)]
use tinio_util::testing::part_number;

#[cfg(test)]
use crate::_core::bucket::name;
#[cfg(feature = "versioning")]
use crate::_core::{content::ObjectContent, meta::bucket as bucket_row};
use crate::{
    _core::{
        Bucket, BucketOps, BucketsListing, ListBucketsParams, acl,
        acl::{Acl, AclGrants},
        bucket::Name,
        cors, lifecycle,
        meta::{ObjectMeta, UploadMeta},
        object, object_lock, storage,
    },
    _ops::Hooks,
    Error,
    storage::MemoryStorage,
};
#[async_trait]
impl BucketOps for MemoryStorage {
    async fn create_bucket(
        &self,
        name: &Name,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
    ) -> Result<(), Error> {
        // The engine body: the content plane's create (a no-op for a store
        // whose bucket IS its row) then the create-or-refuse row write (D22).
        self.ops()
            .create_bucket(name, owner, acl)
            .await
            .map_err(Error::from)
    }

    async fn create_bucket_with_lock(
        &self,
        name: &Name,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
        config: &object_lock::Config,
    ) -> Result<(), Error> {
        #[cfg(feature = "versioning")]
        {
            // OL13's atomic form, not the engine's compensating composition:
            // the bucket row, its `Enabled` state and the stored
            // configuration are ONE redb write transaction
            // (`RedbMetadata::create_bucket_with_lock`), so no failure can
            // leave a partial lock-enabled bucket and no undo step exists.
            // The request-path validation is the engine body's, kept here
            // verbatim (a client that bypasses the engine must not store a
            // configuration the wire would refuse).
            if let Err(invalid) = config.validate() {
                tracing::warn!(
                    bucket = %name,
                    reason = invalid.reason,
                    "refusing to create a bucket with an Object Lock configuration the request path would refuse"
                );
                return Err(storage::invalid_bucket_state(name).into());
            }
            // The namespace comes first, exactly as the engine's
            // `create_bucket` orders it (a no-op for mem's content plane —
            // the row is the bucket — but the content contract's own step).
            self.content.create_bucket(name).await?;
            let mut recorded = bucket_row::Row::at(SystemTime::now());
            recorded.owner = owner.cloned();
            recorded.acl = Acl {
                owner: None,
                grants: acl.grants.clone(),
            };
            self.metadata
                .create_bucket_with_lock(name, &recorded, config)
                .await
                .map_err(Error::from)
        }
        #[cfg(not(feature = "versioning"))]
        {
            let _ = (owner, acl, config);
            Err(storage::access_denied_bucket(name).into())
        }
    }

    async fn get_bucket_object_lock(
        &self,
        name: &Name,
    ) -> Result<Option<object_lock::Config>, Error> {
        self.ops()
            .get_bucket_object_lock(name)
            .await
            .map_err(Error::from)
    }

    async fn put_bucket_object_lock(
        &self,
        name: &Name,
        config: &object_lock::Config,
    ) -> Result<(), Error> {
        self.ops()
            .put_bucket_object_lock(name, config)
            .await
            .map_err(Error::from)
    }

    async fn delete_bucket(&self, name: &Name) -> Result<(), Error> {
        // The engine's probes (empty content, no in-progress upload) then the
        // hook, which owns the whole teardown: mem's ONE write transaction
        // (ruling 2026-10-08) with the probes re-run inside it, so a concurrent
        // write cannot slip between the check and the removal. The hook's
        // refusals are the answer — `NoSuchBucket` here, exactly as the
        // contract's `remove_if_empty` would project it.
        let ops = self.ops();
        match ops.delete_bucket(name).await {
            // The engine's emptiness probe is the content plane's (byte rows,
            // the contract's own rule), while mem's emptiness authority is its
            // ROW planes — for mem the row is the object. The two differ in one
            // state: a bucket whose rows are gone while byte rows linger (the
            // part bytes a failed `remove_many` orphaned). The teardown sweeps
            // that residue, so it must stay deletable instead of refusing
            // forever and stranding the bytes it meters.
            Err(storage::Error::NotEmpty(_)) if self.rows_are_empty(name).await? => ops
                .hooks
                .before_bucket_delete(name)
                .await
                .map_err(Error::from),
            other => other.map_err(Error::from),
        }
    }

    async fn head_bucket(&self, name: &Name) -> Result<Bucket, Error> {
        // The engine's: the recorded creation time, and `NoSuchBucket` for a
        // row-less bucket — the row IS the bucket in mem (`BucketRow::
        // Guaranteed`, the receiver's declared policy).
        self.ops().head_bucket(name).await.map_err(Error::from)
    }

    async fn list_buckets(
        &self,
        params: ListBucketsParams,
        owner: Option<&acl::OwnerId>,
        lazy_default: Option<&acl::OwnerId>,
    ) -> Result<BucketsListing, Error> {
        // The engine's: BUCKETS is keyed by name, so the row walk is already
        // name order, and the prefix filter, the owner filter (with the
        // effective-owner comparison) and the continuation marker are the
        // shared bodies' — the same ones fs takes where its sweep feeds them.
        self.ops()
            .list_buckets(params, owner, lazy_default)
            .await
            .map_err(Error::from)
    }

    async fn get_bucket_tags(&self, name: &Name) -> Result<object::Tags, Error> {
        self.ops().get_bucket_tags(name).await.map_err(Error::from)
    }

    async fn put_bucket_tags(&self, name: &Name, tags: &object::Tags) -> Result<(), Error> {
        self.ops()
            .put_bucket_tags(name, tags)
            .await
            .map_err(Error::from)
    }

    async fn delete_bucket_tags(&self, name: &Name) -> Result<(), Error> {
        self.ops()
            .delete_bucket_tags(name)
            .await
            .map_err(Error::from)
    }

    async fn get_bucket_cors(&self, name: &Name) -> Result<Option<cors::Config>, Error> {
        self.ops().get_bucket_cors(name).await.map_err(Error::from)
    }

    async fn put_bucket_cors(&self, name: &Name, config: &cors::Config) -> Result<(), Error> {
        self.ops()
            .put_bucket_cors(name, config)
            .await
            .map_err(Error::from)
    }

    async fn delete_bucket_cors(&self, name: &Name) -> Result<(), Error> {
        self.ops()
            .delete_bucket_cors(name)
            .await
            .map_err(Error::from)
    }

    async fn get_bucket_lifecycle(&self, name: &Name) -> Result<Option<lifecycle::Config>, Error> {
        self.ops()
            .get_bucket_lifecycle(name)
            .await
            .map_err(Error::from)
    }

    async fn put_bucket_lifecycle(
        &self,
        name: &Name,
        config: &lifecycle::Config,
    ) -> Result<(), Error> {
        self.ops()
            .put_bucket_lifecycle(name, config)
            .await
            .map_err(Error::from)
    }

    async fn merge_bucket_lifecycle(
        &self,
        name: &Name,
        rules: &[lifecycle::Rule],
    ) -> Result<lifecycle::Merged, Error> {
        self.ops()
            .merge_bucket_lifecycle(name, rules)
            .await
            .map_err(Error::from)
    }

    async fn delete_bucket_lifecycle(&self, name: &Name) -> Result<(), Error> {
        self.ops()
            .delete_bucket_lifecycle(name)
            .await
            .map_err(Error::from)
    }

    async fn get_bucket_acl(&self, name: &Name) -> Result<Acl, Error> {
        self.ops().get_bucket_acl(name).await.map_err(Error::from)
    }

    async fn put_bucket_acl(&self, name: &Name, grants: &AclGrants) -> Result<(), Error> {
        self.ops()
            .put_bucket_acl(name, grants)
            .await
            .map_err(Error::from)
    }
}

impl MemoryStorage {
    /// Whether mem's row planes hold neither an object row nor an upload row
    /// for `name` — mem's emptiness authority (the row IS the object), asked
    /// only to tell the engine's `NotEmpty` apart from mem's own. Both halves
    /// are the shared contract probes, so the rule has one home even here:
    /// `ObjectMeta::has_objects` is the very predicate the engine's teardown
    /// composition takes, and `UploadMeta::has_uploads` the one it takes
    /// beside it.
    ///
    /// What the two answers differ on: a bucket whose row planes are empty
    /// while byte rows linger — the part bytes a failed `remove_many`
    /// orphaned. That is a state the engine's probes cannot see, which is why
    /// mem's override keeps its own fallback instead of dropping it.
    async fn rows_are_empty(&self, name: &Name) -> Result<bool, Error> {
        Ok(!self.metadata.has_objects(name).await? && !self.metadata.has_uploads(name).await?)
    }
}

#[cfg(test)]
mod tests;
