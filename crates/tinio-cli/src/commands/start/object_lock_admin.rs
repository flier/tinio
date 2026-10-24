//! The management plane's Object Lock admin (OL23).
//!
//! The four routes are served from [`ObjectLockStorageAdmin`], so the plane carries this run's
//! storage handle and the cache handle the data plane shares.

use std::{sync::Arc, time::SystemTime};

use crate::{
    _api::object_lock::ObjectLockAdmin,
    _core::{
        bucket, object,
        object_lock::{self, Bypass, LegalHold, Retention},
        storage::{self, Error as StorageError, Storage},
        version::{self, Address, HeadResult, State as VersionState},
    },
    _server::ObjectLockCache,
};

/// The management plane's Object Lock admin (OL23): the four routes are served from it, so the
/// plane carries this run's storage handle and the cache handle the data plane shares.
///
/// **The cache is the point of the handle.** A configuration written through the plane does not
/// pass through `S3Backend`'s own write paths, so the `has_lock_config` negative cache would
/// otherwise keep answering a stale `false`; every successful bucket write inserts the fact it
/// produced into the shared [`ObjectLockCache`], which is the same handle
/// [`PlaneConfig`](crate::_server::PlaneConfig) hands the data plane.
#[cfg(feature = "object-lock")]
struct ObjectLockStorageAdmin<S> {
    storage: S,
    cache: ObjectLockCache,
}

#[cfg(feature = "object-lock")]
impl<S: Storage + Send + Sync> ObjectLockStorageAdmin<S> {
    /// The version identity the contract's reads take, resolved through OL29's address: `Current`
    /// is the current slot's identity (which may be the null one), `Null` the null version's and
    /// `Id` the id. A marker is the plane's `404 NoSuchVersion`, as are an absent key and an
    /// absent identity.
    async fn identity(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        address: &Address,
    ) -> Result<Option<version::Id>, StorageError> {
        match address {
            // `head_object` answers an object, not a `HeadResult`. Wrapping a hit as
            // `HeadResult::Object` lets it share `version::split_head`. A missing key is this
            // plane's `NoSuchVersion` naming the key; a `NoSuchVersion` or any other error is
            // the head's own. A marker arm cannot occur on this wrap and would still be
            // `NoSuchVersion`.
            Address::Current => {
                match version::split_head(
                    self.storage
                        .head_object(bucket, key)
                        .await
                        .map(HeadResult::Object),
                ) {
                    version::HeadSplit::Object(info) => Ok(info.version_id),
                    version::HeadSplit::Marker { .. }
                    | version::HeadSplit::Missing(storage::Error::NoSuchKey(_)) => {
                        Err(storage::no_such_version(key.as_ref()))
                    }
                    version::HeadSplit::Missing(err) | version::HeadSplit::Failed(err) => Err(err),
                }
            }
            Address::Null => Self::identity_of(
                version::split_head(self.storage.head_object_version(bucket, key, None).await),
                "null",
            ),
            Address::Id(id) => Self::identity_of(
                version::split_head(
                    self.storage
                        .head_object_version(bucket, key, Some(id))
                        .await,
                ),
                id.as_str(),
            ),
        }
    }

    /// A version head's identity. A marker is `NoSuchVersion` naming `marker` (the null literal or
    /// the requested id). A miss and any other storage error are the head's own error.
    fn identity_of(
        split: version::HeadSplit,
        marker: &str,
    ) -> Result<Option<version::Id>, StorageError> {
        match split {
            version::HeadSplit::Object(info) => Ok(info.version_id),
            version::HeadSplit::Marker { .. } => Err(storage::no_such_version(marker)),
            version::HeadSplit::Missing(err) | version::HeadSplit::Failed(err) => Err(err),
        }
    }
}

#[cfg(feature = "object-lock")]
#[async_trait::async_trait]
impl<S: Storage + Send + Sync> ObjectLockAdmin for ObjectLockStorageAdmin<S> {
    async fn bucket_config(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Option<object_lock::Config>, StorageError> {
        self.storage
            .get_bucket_object_lock(bucket)
            .await
            .map_err(Into::into)
    }

    async fn put_bucket_config(
        &self,
        bucket: &bucket::Name,
        config: &object_lock::Config,
    ) -> Result<(), StorageError> {
        // The CLI's OL24 convenience: enabling a bucket (with or without a rule) enables
        // versioning first when it is unversioned. A suspended bucket cannot be lock-enabled —
        // its versioning refuses the write, and the plane answers `409`.
        match self
            .storage
            .bucket_versioning(bucket)
            .await
            .map_err(Into::into)?
        {
            VersionState::Unversioned => {
                self.storage
                    .set_bucket_versioning(bucket, VersionState::Enabled)
                    .await
                    .map_err(Into::into)?;
            }
            VersionState::Enabled => {}
            VersionState::Suspended => return Err(storage::invalid_bucket_state(bucket)),
        }
        self.storage
            .put_bucket_object_lock(bucket, config)
            .await
            .map_err(Into::into)?;
        // The plane's configuration is now visible to the same process's GET/HEAD echo (OL18):
        // the store write did not pass through the backend's cache writers, so the admin inserts
        // the fact its own write produced (`true`).
        self.cache.insert(bucket, true);
        Ok(())
    }

    async fn object_lock(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        address: &Address,
    ) -> Result<Option<object_lock::Lock>, StorageError> {
        let identity = self.identity(bucket, key, address).await?;
        // One store read for the whole lock (the contract's composed default, or a store's
        // single-row override), not two single-value reads.
        self.storage
            .object_lock(bucket, key, identity.as_ref())
            .await
            .map_err(Into::into)
    }

    async fn put_retention(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        address: &Address,
        retention: Option<&Retention>,
        bypass: Bypass,
    ) -> Result<(), StorageError> {
        self.storage
            .set_retention(bucket, key, address, retention, bypass, SystemTime::now())
            .await
            .map_err(Into::into)?;
        Ok(())
    }

    async fn put_legal_hold(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        address: &Address,
        hold: LegalHold,
    ) -> Result<(), StorageError> {
        self.storage
            .set_legal_hold(bucket, key, address, hold)
            .await
            .map_err(Into::into)?;
        Ok(())
    }

    /// The object route's atomic update (OL23): one store transaction applies the retention
    /// member and the legal-hold member, so a refused retention writes neither.
    async fn update_lock(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        address: &Address,
        retention: Option<Option<&Retention>>,
        legal_hold: Option<LegalHold>,
        bypass: Bypass,
    ) -> Result<(), StorageError> {
        self.storage
            .update_lock(
                bucket,
                key,
                address,
                retention,
                legal_hold,
                bypass,
                SystemTime::now(),
            )
            .await
            .map_err(Into::into)?;
        Ok(())
    }
}

/// The plane's Object Lock admin, or `None` when this run injects none (OL23): `[s3] object_lock
/// = false` turns off the routes **and** the data plane's echo, and a run that carries no cache
/// carries no admin. The capability is read through `capabilities_for` — the same reader the
/// data plane uses — so the two cannot disagree about this run's answer.
#[cfg(feature = "object-lock")]
pub(super) fn object_lock_admin<S: Storage + Clone + Send + Sync + 'static>(
    storage: &S,
    cache: Option<&ObjectLockCache>,
) -> Option<Arc<dyn ObjectLockAdmin>> {
    let cache = cache?;
    Some(Arc::new(ObjectLockStorageAdmin {
        storage: storage.clone(),
        cache: cache.clone(),
    }))
}
