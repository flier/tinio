//! The write-failure decorator the serving path wraps a backend in.
//!
//! One home for `tinio_storage_write_failures_total`: every mutating `Storage` method records
//! its failure here, under the backend's closed label, so a new call site inside a backend cannot
//! silently skip the family. Reads forward untouched.
//!
//! **Two concrete wrappers rather than one generic one**: `Storage` is the supertrait of every
//! category, and a generic wrapper's associated-error projection does not normalize while the
//! category impls are being checked (a supertrait cycle). The backends' own impls are concrete,
//! so these are too.

use std::sync::Arc;

use async_trait::async_trait;
use tinio_util::metrics::{Backend, WriteFailureKind, WriteOp, record_write_failure};

#[cfg(feature = "pg")]
use crate::_pg::{Error as PgError, PgStorage};
use crate::{
    _core::{
        ByteRange,
        acl::{self, Acl, AclGrants},
        bucket::{self, Bucket},
        checksum, cors, lifecycle,
        multipart::{CompletedPart, MultipartUpload, ObjectPart, PartInfo, PartNumber},
        object,
        storage::{
            BodyStream, BucketOps, BucketsListing, Error, GetObjectResult, LifecycleRules,
            ListBucketsParams, ListObjectsParams, ListPartsParams, ListUploadsParams, MultipartOps,
            ObjectListing, ObjectOps, PartsListing, PutObjectResult, Storage, TagWriteResult,
            UploadsListing, VersionOps,
        },
        version::{self, CurrentVersion, Id, ListParams, Listing, Marker, State},
    },
    _fs::{Error as FsError, FsStorage},
};

/// One backend behind the write-failure recorder: reads forward, writes record on error.
pub(super) struct MetricsStorage<S> {
    inner: S,
}

/// The closed failure class of one backend error.
fn failure_kind<E: WriteFailureKindOf>(err: &E) -> WriteFailureKind {
    err.failure_kind()
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
impl MetricsStorage<FsStorage> {
    /// Wrap the backend.
    pub(super) fn new(inner: FsStorage) -> Self {
        Self { inner }
    }

    /// Record one failed write and hand the backend's error back for the caller's
    /// `?`/`map_err`.
    fn failed(
        &self,
        op: WriteOp,
        err: <FsStorage as Storage>::Error,
    ) -> <FsStorage as Storage>::Error {
        record_write_failure(Backend::Fs, op, failure_kind(&err));
        err
    }
}

impl Storage for MetricsStorage<FsStorage> {
    type Error = <FsStorage as Storage>::Error;
}

#[async_trait]
impl BucketOps for MetricsStorage<FsStorage> {
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

    async fn delete_bucket(&self, name: &bucket::Name) -> Result<(), <Self as Storage>::Error>
    where
        Self: Storage,
    {
        self.inner
            .delete_bucket(name)
            .await
            .map_err(|err| self.failed(WriteOp::Delete, err))
    }

    async fn head_bucket(&self, name: &bucket::Name) -> Result<Bucket, <Self as Storage>::Error>
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

    async fn delete_bucket_tags(&self, name: &bucket::Name) -> Result<(), <Self as Storage>::Error>
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

    async fn delete_bucket_cors(&self, name: &bucket::Name) -> Result<(), <Self as Storage>::Error>
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

    async fn get_bucket_acl(&self, name: &bucket::Name) -> Result<Acl, <Self as Storage>::Error>
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
impl ObjectOps for MetricsStorage<FsStorage> {
    type StagedBody = <FsStorage as ObjectOps>::StagedBody;

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

    async fn stage_body(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        body: BodyStream,
        tee: Option<Arc<checksum::PartChecksum>>,
        carried: Option<checksum::Recorded>,
    ) -> Result<Self::StagedBody, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        self.inner
            .stage_body(bucket, key, body, tee, carried)
            .await
            .map_err(|err| self.failed(WriteOp::Put, err))
    }

    async fn commit_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        staged: Self::StagedBody,
        tags: object::Tags,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
    ) -> Result<object::Info, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        self.inner
            .commit_object(bucket, key, staged, tags, owner, acl)
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
    ) -> Result<object::Info, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        self.inner
            .copy_object(
                src_bucket, src_key, dst_bucket, dst_key, tags, owner, acl, _checksum,
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
impl MultipartOps for MetricsStorage<FsStorage> {
    async fn create_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        checksum: Option<checksum::Upload>,
        tags: object::Tags,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
    ) -> Result<MultipartUpload, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        self.inner
            .create_multipart_upload(bucket, key, checksum, tags, owner, acl)
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
impl VersionOps for MetricsStorage<FsStorage> {
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
    ) -> Result<Marker, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        self.inner
            .delete_object_version(bucket, key, version)
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
}

#[async_trait]
impl LifecycleRules for MetricsStorage<FsStorage> {
    async fn lifecycles(&self) -> Result<Vec<(bucket::Name, lifecycle::Config)>, Error> {
        self.inner.lifecycles().await
    }
}

#[cfg(feature = "pg")]
impl MetricsStorage<PgStorage> {
    /// Wrap the backend.
    pub(super) fn new(inner: PgStorage) -> Self {
        Self { inner }
    }

    /// Record one failed write and hand the backend's error back for the caller's
    /// `?`/`map_err`.
    fn failed(
        &self,
        op: WriteOp,
        err: <PgStorage as Storage>::Error,
    ) -> <PgStorage as Storage>::Error {
        record_write_failure(Backend::Pg, op, failure_kind(&err));
        err
    }
}

#[cfg(feature = "pg")]
impl Storage for MetricsStorage<PgStorage> {
    type Error = <PgStorage as Storage>::Error;
}

#[cfg(feature = "pg")]
#[async_trait]
impl BucketOps for MetricsStorage<PgStorage> {
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

    async fn delete_bucket(&self, name: &bucket::Name) -> Result<(), <Self as Storage>::Error>
    where
        Self: Storage,
    {
        self.inner
            .delete_bucket(name)
            .await
            .map_err(|err| self.failed(WriteOp::Delete, err))
    }

    async fn head_bucket(&self, name: &bucket::Name) -> Result<Bucket, <Self as Storage>::Error>
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

    async fn delete_bucket_tags(&self, name: &bucket::Name) -> Result<(), <Self as Storage>::Error>
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

    async fn delete_bucket_cors(&self, name: &bucket::Name) -> Result<(), <Self as Storage>::Error>
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

    async fn get_bucket_acl(&self, name: &bucket::Name) -> Result<Acl, <Self as Storage>::Error>
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

#[cfg(feature = "pg")]
#[async_trait]
impl ObjectOps for MetricsStorage<PgStorage> {
    type StagedBody = <PgStorage as ObjectOps>::StagedBody;

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

    async fn stage_body(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        body: BodyStream,
        tee: Option<Arc<checksum::PartChecksum>>,
        carried: Option<checksum::Recorded>,
    ) -> Result<Self::StagedBody, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        self.inner
            .stage_body(bucket, key, body, tee, carried)
            .await
            .map_err(|err| self.failed(WriteOp::Put, err))
    }

    async fn commit_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        staged: Self::StagedBody,
        tags: object::Tags,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
    ) -> Result<object::Info, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        self.inner
            .commit_object(bucket, key, staged, tags, owner, acl)
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
    ) -> Result<object::Info, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        self.inner
            .copy_object(
                src_bucket, src_key, dst_bucket, dst_key, tags, owner, acl, _checksum,
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

#[cfg(feature = "pg")]
#[async_trait]
impl MultipartOps for MetricsStorage<PgStorage> {
    async fn create_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        checksum: Option<checksum::Upload>,
        tags: object::Tags,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
    ) -> Result<MultipartUpload, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        self.inner
            .create_multipart_upload(bucket, key, checksum, tags, owner, acl)
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

#[cfg(feature = "pg")]
#[async_trait]
impl VersionOps for MetricsStorage<PgStorage> {
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
    ) -> Result<Marker, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        self.inner
            .delete_object_version(bucket, key, version)
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
}

#[cfg(feature = "pg")]
#[async_trait]
impl LifecycleRules for MetricsStorage<PgStorage> {
    async fn lifecycles(&self) -> Result<Vec<(bucket::Name, lifecycle::Config)>, Error> {
        self.inner.lifecycles().await
    }
}
