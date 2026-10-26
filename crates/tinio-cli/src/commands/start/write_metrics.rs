//! The write-failure decorator the serving path wraps a backend in.
//!
//! One home for `tinio_storage_write_failures_total`: every mutating `Storage` method records
//! its failure here, under the backend's closed label, so a new call site inside a backend cannot
//! silently skip the family. Reads forward untouched.
//!
//! **Concrete wrappers rather than one generic one**: `Storage` is the supertrait of every
//! category, and a generic wrapper's associated-error projection does not normalize while the
//! category impls are being checked (a supertrait cycle). The backends' own impls are concrete,
//! so every wrapper is generated from one `metrics_wrapper` macro: the fs and pg wrappers through
//! its general form, and the tiered backend through its `tier:` form — for the PostgreSQL cold
//! store in production and for a database-free cold plane in tests.

use std::{sync::Arc, time::SystemTime};

use async_trait::async_trait;
use tinio_util::metrics::{Backend, WriteFailureKind, WriteOp, record_write_failure};

#[cfg(feature = "pg")]
use crate::_pg::{Error as PgError, PgStorage};
use crate::{
    _core::{
        ByteRange, Class,
        acl::{self, Acl, AclGrants},
        bucket::{self, Bucket},
        checksum, cors, lifecycle,
        multipart::{CompletedPart, MultipartUpload, ObjectPart, PartInfo, PartNumber},
        object, object_lock,
        storage::{
            BodyStream, BucketOps, BucketsListing, Error, GetObjectResult, LifecycleRules,
            ListBucketsParams, ListObjectsParams, ListPartsParams, ListUploadsParams, MultipartOps,
            ObjectListing, ObjectOps, PartsListing, PutObjectResult, Storage, TagWriteResult,
            UploadsListing, VersionOps,
        },
        version::{self, CurrentVersion, Id, ListParams, Listing, Marker, State},
    },
    _fs::{Error as FsError, FsStorage},
    _tier::{ColdStore, Error as TierError, TierStaged, TierStorage},
};

/// One backend behind the write-failure recorder: reads forward, writes record on error.
pub(super) struct MetricsStorage<S> {
    inner: S,
}

impl<S> MetricsStorage<S> {
    /// Wrap the backend.
    pub(super) fn new(inner: S) -> Self {
        Self { inner }
    }
}

/// A backend error that knows its write-failure class.
trait WriteFailureKindOf {
    fn failure_kind(&self) -> WriteFailureKind;
}

impl WriteFailureKindOf for FsError {
    fn failure_kind(&self) -> WriteFailureKind {
        if self.is_read_only() {
            WriteFailureKind::ReadOnly
        } else {
            WriteFailureKind::Io
        }
    }
}

#[cfg(feature = "pg")]
impl WriteFailureKindOf for PgError {
    fn failure_kind(&self) -> WriteFailureKind {
        if self.is_read_only() {
            WriteFailureKind::ReadOnly
        } else {
            WriteFailureKind::Io
        }
    }
}

impl WriteFailureKindOf for TierError {
    fn failure_kind(&self) -> WriteFailureKind {
        match self {
            Self::ReadOnly => WriteFailureKind::ReadOnly,
            Self::Fs(err) if err.is_read_only() => WriteFailureKind::ReadOnly,
            Self::Storage(err) if err.is_read_only() => WriteFailureKind::ReadOnly,
            Self::Storage(Error::EntityTooLarge { .. }) => WriteFailureKind::Cap,
            _ => WriteFailureKind::Io,
        }
    }
}

/// One backend's metrics decorator, generated once for every concrete
/// backend.
///
/// The impls stay concrete — a generic wrapper's associated-error
/// projection does not normalize while the category impls are checked —
/// while the backend, its error, its failure label and its staged handle
/// are macro parameters. Every mutating method forwards to the backend and
/// books a failure under the backend's closed label; reads forward
/// untouched. The `tier:` form is this same body plus the four contract
/// hooks the tier layer overrides, written once, so a new `Storage` method
/// is added once.
macro_rules! metrics_wrapper {
    // The tiered backend's form: the general body plus the four contract hooks the tier layer
    // overrides. `supports_tiers`, `storage_class`, `transition` and `metrics_scan` are
    // defaulted on the contract, so a wrapper that lost one would silently degrade to "no
    // tiers", `Standard`, a refusal or a flushing listing with no compile error — hence they
    // are forwarded here, once, for every cold plane.
    (tier: $cold:ty) => {
        metrics_wrapper! {
            backend: TierStorage<$cold>,
            error: TierError,
            label: Backend::Tier,
            staged: TierStaged,
            storage: {
                fn supports_tiers(&self) -> bool {
                    self.inner.supports_tiers()
                }

                async fn storage_class(
                    &self,
                    bucket: &bucket::Name,
                    key: &object::Key,
                ) -> Result<Class, <Self as Storage>::Error> {
                    self.inner.storage_class(bucket, key).await
                }

                async fn transition(
                    &self,
                    bucket: &bucket::Name,
                    key: &object::Key,
                    class: Class,
                    now: SystemTime,
                ) -> Result<(), <Self as Storage>::Error> {
                    self.inner
                        .transition(bucket, key, class, now)
                        .await
                        .map_err(|err| self.failed(WriteOp::Put, err))
                }

                async fn metrics_scan(
                    &self,
                    params: ListObjectsParams,
                ) -> Result<ObjectListing, <Self as Storage>::Error> {
                    self.inner.metrics_scan(params).await
                }
            }
        }
    };

    (
        backend: $backend:ty,
        error: $error:ty,
        label: $label:expr,
        staged: $staged:ty,
        storage: { $($storage_extra:tt)* }
    ) => {
        impl MetricsStorage<$backend> {
            /// Record one failed write and hand the backend's error back for
            /// the caller's `?`/`map_err`.
            fn failed(&self, op: WriteOp, err: $error) -> $error {
                record_write_failure($label, op, err.failure_kind());
                err
            }
        }

        /// Contract hooks are forwarded explicitly rather than left to
        /// their defaults: a wrapper that lost one would silently degrade
        /// to "no tiers", `Standard`, a refusal or a flushing listing with
        /// no compile error. The fs and pg backends keep the trait's
        /// defaults, so their generated impl forwards only the associated
        /// error; the tier wrapper supplies its hooks through `storage`.
        #[async_trait]
        impl Storage for MetricsStorage<$backend> {
            type Error = $error;
            $($storage_extra)*
        }

        #[async_trait]
        impl BucketOps for MetricsStorage<$backend> {
            async fn create_bucket(
                &self,
                name: &bucket::Name,
                owner: Option<&acl::OwnerId>,
                acl: &Acl,
            ) -> Result<(), <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .create_bucket(name, owner, acl)
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }

            /// Forwards the lock-enabled create (OL27/OL28): a default body here would
            /// silently answer the contract's fail-closed refusal on the real server,
            /// so every new method is forwarded explicitly.
            async fn create_bucket_with_lock(
                &self,
                name: &bucket::Name,
                owner: Option<&acl::OwnerId>,
                acl: &Acl,
                config: &object_lock::Config,
            ) -> Result<(), <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .create_bucket_with_lock(name, owner, acl, config)
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }

            /// Forwards the Object Lock configuration reads and writes (OL27/OL28): a
            /// default body here would silently answer the contract's fail-closed
            /// refusal on the real server.
            async fn get_bucket_object_lock(
                &self,
                name: &bucket::Name,
            ) -> Result<Option<object_lock::Config>, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.get_bucket_object_lock(name).await
            }

            async fn put_bucket_object_lock(
                &self,
                name: &bucket::Name,
                config: &object_lock::Config,
            ) -> Result<(), <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .put_bucket_object_lock(name, config)
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }

            async fn delete_bucket(
                &self,
                name: &bucket::Name,
            ) -> Result<(), <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .delete_bucket(name)
                    .await
                    .map_err(|err| self.failed(WriteOp::Delete, err))
            }

            async fn head_bucket(
                &self,
                name: &bucket::Name,
            ) -> Result<Bucket, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.head_bucket(name).await
            }

            async fn list_buckets(
                &self,
                params: ListBucketsParams,
                owner: Option<&acl::OwnerId>,
                lazy_default: Option<&acl::OwnerId>,
            ) -> Result<BucketsListing, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.list_buckets(params, owner, lazy_default).await
            }

            async fn get_bucket_tags(
                &self,
                name: &bucket::Name,
            ) -> Result<object::Tags, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.get_bucket_tags(name).await
            }

            async fn put_bucket_tags(
                &self,
                name: &bucket::Name,
                tags: &object::Tags,
            ) -> Result<(), <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .put_bucket_tags(name, tags)
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }

            async fn delete_bucket_tags(
                &self,
                name: &bucket::Name,
            ) -> Result<(), <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .delete_bucket_tags(name)
                    .await
                    .map_err(|err| self.failed(WriteOp::Delete, err))
            }

            async fn get_bucket_cors(
                &self,
                name: &bucket::Name,
            ) -> Result<Option<cors::Config>, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.get_bucket_cors(name).await
            }

            async fn put_bucket_cors(
                &self,
                name: &bucket::Name,
                cors: &cors::Config,
            ) -> Result<(), <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .put_bucket_cors(name, cors)
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }

            async fn delete_bucket_cors(
                &self,
                name: &bucket::Name,
            ) -> Result<(), <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .delete_bucket_cors(name)
                    .await
                    .map_err(|err| self.failed(WriteOp::Delete, err))
            }

            async fn get_bucket_lifecycle(
                &self,
                name: &bucket::Name,
            ) -> Result<Option<lifecycle::Config>, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.get_bucket_lifecycle(name).await
            }

            async fn put_bucket_lifecycle(
                &self,
                name: &bucket::Name,
                config: &lifecycle::Config,
            ) -> Result<(), <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .put_bucket_lifecycle(name, config)
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }

            async fn merge_bucket_lifecycle(
                &self,
                name: &bucket::Name,
                rules: &[lifecycle::Rule],
            ) -> Result<lifecycle::Merged, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .merge_bucket_lifecycle(name, rules)
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }

            async fn delete_bucket_lifecycle(
                &self,
                name: &bucket::Name,
            ) -> Result<(), <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .delete_bucket_lifecycle(name)
                    .await
                    .map_err(|err| self.failed(WriteOp::Delete, err))
            }

            async fn get_bucket_acl(
                &self,
                name: &bucket::Name,
            ) -> Result<Acl, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.get_bucket_acl(name).await
            }

            async fn put_bucket_acl(
                &self,
                name: &bucket::Name,
                grants: &AclGrants,
            ) -> Result<(), <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .put_bucket_acl(name, grants)
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }
        }

        #[async_trait]
        impl ObjectOps for MetricsStorage<$backend> {
            type StagedBody = $staged;

            async fn put_object(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                body: BodyStream,
            ) -> Result<PutObjectResult, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .put_object(bucket, key, body)
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }

            /// The staged handle is named concretely: the wrapper's associated type
            /// resolves to the backend's own staged type, which keeps the trait
            /// projection out of the generic impl's signatures.
            async fn stage_body(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                body: BodyStream,
                tee: Option<Arc<checksum::PartChecksum>>,
                carried: Option<checksum::Recorded>,
            ) -> Result<$staged, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .stage_body(bucket, key, body, tee, carried)
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }

            /// Forwards the effective write-lock resolution (OL27/OL28): every
            /// backend's answer is forwarded untouched, so the serving path never
            /// receives a defaulted "unlocked".
            async fn resolve_write_lock(
                &self,
                bucket: &bucket::Name,
                explicit: Option<&object_lock::Retention>,
                legal_hold: object_lock::LegalHold,
                now: SystemTime,
            ) -> Result<Option<object_lock::Lock>, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .resolve_write_lock(bucket, explicit, legal_hold, now)
                    .await
            }

            async fn commit_object(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                staged: $staged,
                tags: object::Tags,
                owner: Option<&acl::OwnerId>,
                acl: &Acl,
                lock: Option<&object_lock::Lock>,
            ) -> Result<object::Info, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .commit_object(bucket, key, staged, tags, owner, acl, lock)
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }

            async fn get_object(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                range: Option<ByteRange>,
            ) -> Result<GetObjectResult, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.get_object(bucket, key, range).await
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
                _checksum: Option<checksum::Recorded>,
                lock: Option<&object_lock::Lock>,
            ) -> Result<object::Info, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .copy_object(
                        src_bucket, src_key, dst_bucket, dst_key, tags, owner, acl, _checksum, lock,
                    )
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }

            async fn rename_object(
                &self,
                bucket: &bucket::Name,
                src: &object::Key,
                dst: &object::Key,
            ) -> Result<object::Info, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .rename_object(bucket, src, dst)
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }

            async fn head_object(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
            ) -> Result<object::Info, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.head_object(bucket, key).await
            }

            async fn delete_object(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
            ) -> Result<version::Marker, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .delete_object(bucket, key)
                    .await
                    .map_err(|err| self.failed(WriteOp::Delete, err))
            }

            async fn get_object_tags(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
            ) -> Result<object::Tags, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.get_object_tags(bucket, key).await
            }

            async fn put_object_tags(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                tags: &object::Tags,
            ) -> Result<TagWriteResult, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .put_object_tags(bucket, key, tags)
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }

            async fn delete_object_tags(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
            ) -> Result<TagWriteResult, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .delete_object_tags(bucket, key)
                    .await
                    .map_err(|err| self.failed(WriteOp::Delete, err))
            }

            async fn get_object_acl(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
            ) -> Result<Acl, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.get_object_acl(bucket, key).await
            }

            async fn put_object_acl(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                grants: &AclGrants,
            ) -> Result<(), <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .put_object_acl(bucket, key, grants)
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }

            async fn list_objects(
                &self,
                params: ListObjectsParams,
            ) -> Result<ObjectListing, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.list_objects(params).await
            }

            async fn list_object_parts(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
            ) -> Result<Vec<ObjectPart>, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.list_object_parts(bucket, key).await
            }
        }

        #[async_trait]
        impl MultipartOps for MetricsStorage<$backend> {
            async fn create_multipart_upload(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                checksum: Option<checksum::Upload>,
                tags: object::Tags,
                owner: Option<&acl::OwnerId>,
                acl: &Acl,
                lock: Option<&object_lock::Lock>,
            ) -> Result<MultipartUpload, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .create_multipart_upload(bucket, key, checksum, tags, owner, acl, lock)
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }

            async fn get_multipart_upload(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                upload_id: &str,
            ) -> Result<MultipartUpload, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .get_multipart_upload(bucket, key, upload_id)
                    .await
            }

            async fn upload_part(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                upload_id: &str,
                part_number: PartNumber,
                body: BodyStream,
                checksum: Option<Arc<checksum::PartChecksum>>,
            ) -> Result<PartInfo, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .upload_part(bucket, key, upload_id, part_number, body, checksum)
                    .await
                    .map_err(|err| self.failed(WriteOp::Part, err))
            }

            async fn copy_part(
                &self,
                src_bucket: &bucket::Name,
                src_key: &object::Key,
                dst_bucket: &bucket::Name,
                dst_key: &object::Key,
                upload_id: &str,
                part_number: PartNumber,
                range: Option<ByteRange>,
            ) -> Result<PartInfo, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .copy_part(
                        src_bucket,
                        src_key,
                        dst_bucket,
                        dst_key,
                        upload_id,
                        part_number,
                        range,
                    )
                    .await
                    .map_err(|err| self.failed(WriteOp::Part, err))
            }

            async fn list_parts(
                &self,
                params: ListPartsParams,
            ) -> Result<PartsListing, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.list_parts(params).await
            }

            async fn complete_multipart_upload(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                upload_id: &str,
                parts: &[CompletedPart],
                checksum: Option<checksum::Recorded>,
            ) -> Result<object::Info, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .complete_multipart_upload(bucket, key, upload_id, parts, checksum)
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }

            async fn abort_multipart_upload(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                upload_id: &str,
            ) -> Result<(), <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .abort_multipart_upload(bucket, key, upload_id)
                    .await
                    .map_err(|err| self.failed(WriteOp::Delete, err))
            }

            async fn list_multipart_uploads(
                &self,
                params: ListUploadsParams,
            ) -> Result<UploadsListing, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.list_multipart_uploads(params).await
            }
        }

        #[async_trait]
        impl VersionOps for MetricsStorage<$backend> {
            async fn bucket_versioning(
                &self,
                bucket: &bucket::Name,
            ) -> Result<State, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.bucket_versioning(bucket).await
            }

            async fn set_bucket_versioning(
                &self,
                bucket: &bucket::Name,
                state: State,
            ) -> Result<(), <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .set_bucket_versioning(bucket, state)
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }

            async fn ensure_rename_allowed(
                &self,
                bucket: &bucket::Name,
                dst: &object::Key,
            ) -> Result<(), <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.ensure_rename_allowed(bucket, dst).await
            }

            async fn get_object_version(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                version: Option<&Id>,
                range: Option<ByteRange>,
            ) -> Result<version::ReadResult, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .get_object_version(bucket, key, version, range)
                    .await
            }

            async fn head_object_version(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                version: Option<&Id>,
            ) -> Result<version::HeadResult, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.head_object_version(bucket, key, version).await
            }

            async fn delete_object_version(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                version: Option<&Id>,
                bypass: object_lock::Bypass,
            ) -> Result<Marker, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .delete_object_version(bucket, key, version, bypass)
                    .await
                    .map_err(|err| self.failed(WriteOp::Delete, err))
            }

            async fn list_object_versions(
                &self,
                bucket: &bucket::Name,
                params: ListParams,
            ) -> Result<Listing, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.list_object_versions(bucket, params).await
            }

            async fn current_version(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
            ) -> Result<CurrentVersion, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.current_version(bucket, key).await
            }

            async fn version_tags(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                version: Option<&Id>,
            ) -> Result<object::Tags, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.version_tags(bucket, key, version).await
            }

            async fn set_version_tags(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                version: Option<&Id>,
                tags: object::Tags,
            ) -> Result<(), <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .set_version_tags(bucket, key, version, tags)
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }

            async fn version_acl(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                version: Option<&Id>,
            ) -> Result<acl::Acl, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.version_acl(bucket, key, version).await
            }

            async fn set_version_acl(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                version: Option<&Id>,
                acl: &acl::Acl,
            ) -> Result<(), <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .set_version_acl(bucket, key, version, acl)
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }

            /// The server's version-lock read (OL28): a decorator that did not forward
            /// it would answer the contract's fail-closed default on the real server.
            /// Reads forward untouched.
            async fn get_object_retention(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                version: Option<&Id>,
            ) -> Result<Option<object_lock::Retention>, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.get_object_retention(bucket, key, version).await
            }

            async fn get_object_legal_hold(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                version: Option<&Id>,
            ) -> Result<object_lock::LegalHold, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.get_object_legal_hold(bucket, key, version).await
            }

            /// The echo's single lock read (OL28): forwarded so the decorator does not
            /// fall back to the default's two reads.
            async fn object_lock(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                version: Option<&Id>,
            ) -> Result<Option<object_lock::Lock>, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner.object_lock(bucket, key, version).await
            }

            /// The server's retention write (R22), forwarded so the decorator never
            /// answers the contract's fail-closed default.
            async fn set_retention(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                address: &version::Address,
                requested: Option<&object_lock::Retention>,
                bypass: object_lock::Bypass,
                now: SystemTime,
            ) -> Result<Option<object_lock::Lock>, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .set_retention(bucket, key, address, requested, bypass, now)
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }

            /// The server's legal-hold write (R22), forwarded for the same reason.
            async fn set_legal_hold(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                address: &version::Address,
                hold: object_lock::LegalHold,
            ) -> Result<object_lock::Lock, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .set_legal_hold(bucket, key, address, hold)
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }

            /// The combined retention and legal-hold write (OL23): forwarded so the
            /// decorator never answers the contract's fail-closed default.
            async fn update_lock(
                &self,
                bucket: &bucket::Name,
                key: &object::Key,
                address: &version::Address,
                retention: Option<Option<&object_lock::Retention>>,
                legal_hold: Option<object_lock::LegalHold>,
                bypass: object_lock::Bypass,
                now: SystemTime,
            ) -> Result<Option<object_lock::Lock>, <Self as Storage>::Error>
            where
                Self: Storage,
            {
                self.inner
                    .update_lock(bucket, key, address, retention, legal_hold, bypass, now)
                    .await
                    .map_err(|err| self.failed(WriteOp::Put, err))
            }
        }

        #[async_trait]
        impl LifecycleRules for MetricsStorage<$backend> {
            async fn lifecycles(&self) -> Result<Vec<(bucket::Name, lifecycle::Config)>, Error> {
                self.inner.lifecycles().await
            }
        }
    };
}

metrics_wrapper! {
    backend: FsStorage,
    error: <FsStorage as Storage>::Error,
    label: Backend::Fs,
    staged: <FsStorage as ObjectOps>::StagedBody,
    storage: {}
}

#[cfg(feature = "pg")]
metrics_wrapper! {
    backend: PgStorage,
    error: <PgStorage as Storage>::Error,
    label: Backend::Pg,
    staged: <PgStorage as ObjectOps>::StagedBody,
    storage: {}
}

metrics_wrapper!(tier: ColdStore);

// The test instantiation exercises the same wrapper over the filesystem
// content plane, so no database is needed to read the wrapper's forwarding.
#[cfg(test)]
use crate::_fs::FsContent;

#[cfg(test)]
metrics_wrapper!(tier: FsContent);

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use prometheus::default_registry;
    use tinio_util::{
        metrics::{
            MoveDirection, TierPlane, record_tier_bytes, record_tier_cache_eviction,
            record_tier_flush_duration, record_tier_hit, record_tier_miss, record_tier_move,
            record_tier_move_duration, record_tier_orphans, record_tier_quarantine,
        },
        testing::body,
    };
    use tokio::sync::Mutex;

    use super::*;
    use crate::{
        _core::storage::collect_body,
        _fs::testing::fs_options,
        _tier::{Config, FlushMode, pg::ROOT_NAMESPACE},
    };

    /// Serializes this module's tier read-metric legs: the families are
    /// process-global, and `tinio-tier`'s own test-only writer gate is compiled
    /// out of this dependency (it is not a `cfg(test)` build), so a plain delta
    /// needs the tests that read through the wrapper to take turns. An async
    /// mutex because the guard spans awaits.
    static READS: Mutex<()> = Mutex::const_new(());

    /// The cold plane of the test tier: the filesystem content plane, so the
    /// wrapper is exercised over a real content plane without a database.
    type TestCold = FsContent;

    fn tier_config() -> Config {
        Config {
            capacity_bytes: 1024 * 1024,
            max_object_bytes: 64 * 1024,
            flush_mode: FlushMode::Sync,
            batch_bytes: 8 * 1024,
            batch_interval: Duration::from_secs(1),
            heat_window: Duration::from_secs(60),
            promote_reads: 3,
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

    /// A wrapper over a real tier layer: a temporary warm root, an in-memory
    /// index and a second temporary root as the cold content plane. The inner
    /// handle is returned too, so a test can compare the wrapper's answer with
    /// the layer's own answer instead of pinning one implementation.
    fn wrapped_tier() -> (
        tempfile::TempDir,
        tempfile::TempDir,
        TierStorage<TestCold>,
        MetricsStorage<TierStorage<TestCold>>,
    ) {
        let warm = tempfile::tempdir().expect("a temporary warm root");
        let cold = tempfile::tempdir().expect("a temporary cold root");
        let cold_fs =
            Arc::new(FsStorage::new(cold.path(), fs_options()).expect("the cold plane opens"));
        let tier = TierStorage::open(warm.path(), fs_options(), tier_config(), cold_fs.content())
            .expect("the tier layer opens");
        let inner = tier.clone();
        (
            warm,
            cold,
            inner,
            MetricsStorage::<TierStorage<TestCold>>::new(tier),
        )
    }

    /// The current value of one labelled counter series, `0` when the series
    /// does not exist yet.
    fn counter(name: &str, labels: &[(&str, &str)]) -> f64 {
        for family in default_registry().gather() {
            if family.name() != name {
                continue;
            }
            for metric in family.get_metric() {
                let matched = labels.iter().all(|(label, value)| {
                    metric
                        .get_label()
                        .iter()
                        .any(|pair| pair.name() == *label && pair.value() == *value)
                });
                if matched {
                    return metric.get_counter().value();
                }
            }
        }
        0.0
    }

    #[tokio::test]
    async fn the_tier_wrapper_forwards_a_read() {
        // This leg serves an object body, so it books the same hit/bytes series
        // the read-metric legs assert on.
        let _reads = READS.lock().await;
        let (_warm, _cold, inner, wrapper) = wrapped_tier();
        let name = bucket::name("data").expect("a valid bucket name");
        wrapper
            .create_bucket(&name, None, &Acl::default_private(None))
            .await
            .expect("the bucket is created");
        let key = object::key("a.bin").expect("a valid key");
        wrapper
            .put_object(&name, &key, body(b"forwarded"))
            .await
            .expect("the write lands");
        let got = wrapper
            .get_object(&name, &key, None)
            .await
            .expect("the read is forwarded");
        let bytes = collect_body(got.body).await.expect("the body is readable");
        assert_eq!(bytes, b"forwarded", "the wrapper serves the inner bytes");

        // The defaulted tier hook answers `false`; only a forwarding wrapper
        // reports the tier layer's override.
        assert!(
            wrapper.supports_tiers(),
            "the wrapper forwards `supports_tiers`"
        );

        // The reserved namespace is refused by the tier layer's own
        // `storage_class`; the contract default would answer `Standard`.
        let reserved = bucket::name(ROOT_NAMESPACE).expect("a valid bucket name");
        let refused = wrapper
            .storage_class(&reserved, &key)
            .await
            .expect_err("the tier layer refuses the reserved namespace");
        assert!(
            matches!(refused, TierError::Storage(Error::AccessDenied(_))),
            "the wrapper forwards `storage_class`"
        );

        // Forwarding means the wrapper answers what the layer answers, not a
        // pinned value: the same missing key and the same instant through both
        // handles must reach the same outcome, whatever the mover's real
        // refusal is (Task 12 replaces the contract default).
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let missing = object::key("missing.bin").expect("a valid key");
        let inner_moved = inner
            .transition(&name, &missing, Class::StandardIa, now)
            .await;
        let wrapper_moved = wrapper
            .transition(&name, &missing, Class::StandardIa, now)
            .await;
        let forwarded = match (&inner_moved, &wrapper_moved) {
            (Ok(()), Ok(())) => true,
            (Err(inner_err), Err(wrapper_err)) => inner_err.to_string() == wrapper_err.to_string(),
            _ => false,
        };
        assert!(
            forwarded,
            "the wrapper forwards `transition`: inner {inner_moved:?}, wrapper {wrapper_moved:?}"
        );

        // The metrics scan answers the tier layer's own listing.
        let listing = wrapper
            .metrics_scan(ListObjectsParams {
                bucket: name,
                prefix: String::new(),
                delimiter: None,
                start_after: None,
                max_keys: 10,
            })
            .await
            .expect("the metrics scan is forwarded");
        assert!(
            listing.objects.iter().any(|entry| entry.key == key),
            "the wrapper forwards `metrics_scan`"
        );
    }

    #[tokio::test]
    async fn the_tier_wrapper_records_a_tier_failure() {
        // The classifier's designed arms: a read-only refusal and a capacity
        // refusal are their own classes, everything else is `io`.
        assert_eq!(
            TierError::ReadOnly.failure_kind(),
            WriteFailureKind::ReadOnly
        );
        assert_eq!(
            TierError::Storage(Error::EntityTooLarge { size: 2, limit: 1 }).failure_kind(),
            WriteFailureKind::Cap
        );

        let (_warm, _cold, _inner, wrapper) = wrapped_tier();
        let name = bucket::name("data").expect("a valid bucket name");
        wrapper
            .create_bucket(&name, None, &Acl::default_private(None))
            .await
            .expect("the bucket is created");
        let kept = object::key("kept.bin").expect("a valid key");
        wrapper
            .put_object(&name, &kept, body(b"kept"))
            .await
            .expect("the object lands");
        // The `delete` series belongs to this test alone: the sibling test's
        // transition failure records `put`, so the exact delta cannot race it.
        let before = counter(
            "tinio_storage_write_failures_total",
            &[("backend", "tier"), ("op", "delete"), ("kind", "io")],
        );
        let refused = wrapper
            .delete_bucket(&name)
            .await
            .expect_err("a non-empty bucket is refused");
        let projected: Error = refused.into();
        assert!(
            matches!(projected, Error::NotEmpty(_)),
            "the inner refusal is handed back unchanged"
        );
        let after = counter(
            "tinio_storage_write_failures_total",
            &[("backend", "tier"), ("op", "delete"), ("kind", "io")],
        );
        assert_eq!(
            after,
            before + 1.0,
            "the failed tier delete is recorded under the tier backend"
        );
    }

    /// The tier hits one plane has booked.
    fn tier_hits(plane: &str) -> f64 {
        counter("tinio_tier_hits_total", &[("tier", plane)])
    }

    /// The tier bytes one plane has served.
    fn tier_served(plane: &str) -> f64 {
        counter("tinio_tier_bytes_total", &[("tier", plane)])
    }

    /// **A subresource GET carries no object body**, so it books one hit on the plane that served
    /// the metadata and no bytes: the byte family means "object content a tier plane served", and
    /// a tags/ACL/attributes read serves metadata the placement already held. The assertion is the
    /// whole pair of families, so a decorator that booked no hit, a second hit, the wrong plane's
    /// hit or a byte would fail it; a cold placement books the hit on `cold` instead.
    #[tokio::test]
    async fn a_subresource_get_books_one_tier_hit_and_no_bytes() {
        // The exact-assert gate: tier reads in sibling tests book the same
        // series, so the delta is only meaningful while no other-thread reader
        // holds it.
        let _reads = READS.lock().await;
        let (_warm, _cold, _inner, wrapper) = wrapped_tier();
        let name = bucket::name("data").expect("a valid bucket name");
        wrapper
            .create_bucket(&name, None, &Acl::default_private(None))
            .await
            .expect("the bucket is created");
        let key = object::key("tagged.bin").expect("a valid key");
        wrapper
            .put_object(&name, &key, body(b"tagged"))
            .await
            .expect("the write lands");

        let warm_hits = tier_hits("warm");
        let cold_hits = tier_hits("cold");
        let warm_bytes = tier_served("warm");
        let cold_bytes = tier_served("cold");

        let tags = wrapper
            .get_object_tags(&name, &key)
            .await
            .expect("the subresource read is served");
        assert!(tags.is_empty(), "the fixture's tags are empty");

        assert_eq!(
            (tier_hits("warm") - warm_hits, tier_hits("cold") - cold_hits),
            (1.0, 0.0),
            "a metadata-only subresource read books one hit for the tier that served it"
        );
        assert_eq!(
            (
                tier_served("warm") - warm_bytes,
                tier_served("cold") - cold_bytes
            ),
            (0.0, 0.0),
            "a metadata-only subresource read serves no bytes"
        );
    }

    /// **An explicit-version GET serves a body**, so it books one warm hit and the extent it
    /// handed the caller — exactly once, and never a second time for the plain GET path that
    /// already owns its own booking.
    #[tokio::test]
    async fn an_explicit_version_get_books_the_body_it_served() {
        let _reads = READS.lock().await;
        let (_warm, _cold, _inner, wrapper) = wrapped_tier();
        let name = bucket::name("data").expect("a valid bucket name");
        wrapper
            .create_bucket(&name, None, &Acl::default_private(None))
            .await
            .expect("the bucket is created");
        wrapper
            .set_bucket_versioning(&name, State::Enabled)
            .await
            .expect("the bucket records versions");
        let key = object::key("versioned.bin").expect("a valid key");
        wrapper
            .put_object(&name, &key, body(b"versioned"))
            .await
            .expect("the write lands");
        let id = wrapper
            .list_object_versions(
                &name,
                ListParams {
                    max_keys: 10,
                    ..ListParams::default()
                },
            )
            .await
            .expect("the version listing runs")
            .entries
            .first()
            .and_then(|entry| entry.id.clone())
            .expect("the versioned write minted an id");

        let warm_hits = tier_hits("warm");
        let warm_bytes = tier_served("warm");
        let read = wrapper
            .get_object_version(&name, &key, Some(&id), None)
            .await
            .expect("the explicit-version read is served");
        let version::ReadResult::Object(served) = read else {
            panic!("the fixture's version is an object");
        };
        let bytes = collect_body(served.body).await.expect("the body reads");
        assert_eq!(bytes, b"versioned");

        assert_eq!(
            tier_hits("warm") - warm_hits,
            1.0,
            "one explicit-version body read books one warm hit"
        );
        assert_eq!(
            tier_served("warm") - warm_bytes,
            9.0,
            "the bytes it served are booked once"
        );
    }

    #[tokio::test]
    async fn the_tier_families_register_once() {
        // These direct recorder calls book the same series the read-metric legs
        // assert on, so they take the same gate.
        let _reads = READS.lock().await;
        for tier in [TierPlane::Warm, TierPlane::Cold] {
            record_tier_hit(tier);
            record_tier_miss(tier);
            record_tier_bytes(tier, 64);
        }
        for direction in [MoveDirection::Promote, MoveDirection::Demote] {
            record_tier_move(direction);
            record_tier_move_duration(direction, Duration::from_millis(4));
        }
        record_tier_flush_duration(Duration::from_millis(2));
        record_tier_orphans(1);
        record_tier_cache_eviction(1);
        record_tier_quarantine(1);
        // A second round must not register the families again.
        record_tier_hit(TierPlane::Warm);
        record_tier_cache_eviction(1);
        record_tier_quarantine(2);

        let gathered = default_registry().gather();
        for name in [
            "tinio_tier_hits_total",
            "tinio_tier_misses_total",
            "tinio_tier_bytes_total",
            "tinio_tier_moves_total",
            "tinio_tier_move_duration_seconds",
            "tinio_tier_flush_duration_seconds",
            "tinio_tier_orphans",
            "tinio_tier_cache_evictions_total",
            "tinio_tier_quarantine",
        ] {
            assert_eq!(
                gathered
                    .iter()
                    .filter(|family| family.name() == name)
                    .count(),
                1,
                "{name} is registered exactly once"
            );
        }
    }

    /// The production instantiation Task 21 serves is the wrapper over the
    /// concrete PostgreSQL cold store.
    #[test]
    fn the_production_tier_wrapper_is_a_storage() {
        fn assert_storage<S: Storage>() {}
        assert_storage::<MetricsStorage<TierStorage<ColdStore>>>();
    }
}
