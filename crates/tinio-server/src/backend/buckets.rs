//! Bucket operations of the S3 mapping layer (task T047).
//!
//! CreateBucket/DeleteBucket/HeadBucket/ListBuckets/GetBucketLocation over
//! the storage contract. Creation dates come from the backend
//! (`buckets.json`); GetBucketLocation always answers `us-east-1`
//! (s3-surface.md). Storage errors map to S3 codes via
//! [`map_backend_error`](crate::backend::map_backend_error).

use s3s::{
    S3Request, S3Response, S3Result,
    dto::{self, BucketLocationConstraint, DeleteBucketOutput, HeadBucketOutput},
};
use tinio_core::storage::Storage;

use crate::backend::{S3Backend, map_backend_error};

impl<S: Storage> S3Backend<S> {
    pub(crate) async fn op_create_bucket(
        &self,
        req: S3Request<dto::CreateBucketInput>,
    ) -> S3Result<S3Response<dto::CreateBucketOutput>> {
        let name = self.bucket(req.input.bucket)?;
        self.storage
            .create_bucket(&name)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::CreateBucketOutput {
            location: Some(format!("/{name}")),
        }))
    }

    pub(crate) async fn op_delete_bucket(
        &self,
        req: S3Request<dto::DeleteBucketInput>,
    ) -> S3Result<S3Response<dto::DeleteBucketOutput>> {
        let name = self.bucket(req.input.bucket)?;
        self.storage
            .delete_bucket(&name)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(DeleteBucketOutput::default()))
    }

    pub(crate) async fn op_head_bucket(
        &self,
        req: S3Request<dto::HeadBucketInput>,
    ) -> S3Result<S3Response<dto::HeadBucketOutput>> {
        let name = self.bucket(req.input.bucket)?;
        self.storage
            .head_bucket(&name)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    pub(crate) async fn op_list_buckets(
        &self,
        _req: S3Request<dto::ListBucketsInput>,
    ) -> S3Result<S3Response<dto::ListBucketsOutput>> {
        let buckets = self
            .storage
            .list_buckets()
            .await
            .map_err(map_backend_error)?;
        let buckets = buckets
            .into_iter()
            .map(|b| dto::Bucket {
                name: Some(b.name.to_string()),
                creation_date: Some(Self::last_modified(b.creation_time)),
                ..Default::default()
            })
            .collect();
        Ok(S3Response::new(dto::ListBucketsOutput {
            buckets: Some(buckets),
            ..Default::default()
        }))
    }

    pub(crate) async fn op_get_bucket_location(
        &self,
        req: S3Request<dto::GetBucketLocationInput>,
    ) -> S3Result<S3Response<dto::GetBucketLocationOutput>> {
        // Existence is checked per AWS (a missing bucket → NoSuchBucket).
        let name = self.bucket(req.input.bucket)?;
        self.storage
            .head_bucket(&name)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::GetBucketLocationOutput {
            location_constraint: Some(BucketLocationConstraint::from("us-east-1".to_string())),
        }))
    }
}

#[cfg(test)]
mod tests {
    use s3s::{S3, dto::ListBucketsInput};
    use tinio_core::storage::{self, BucketOps, Error::NoSuchBucket, ObjectOps};
    use tinio_mem::MemoryStorage;
    use tinio_util::testing::{assert_conformance, body};
    use tokio::runtime::Runtime;

    use super::*;
    use crate::backend::testutil::s3_request;

    fn backend() -> S3Backend<MemoryStorage> {
        S3Backend::new(MemoryStorage::new().unwrap(), Default::default())
    }

    #[tokio::test]
    async fn create_head_list_location_delete() {
        let backend = backend();
        // The storage contract is exposed for setup/teardown.
        let storage = backend.storage();
        let err: storage::Error = storage
            .head_bucket(&"data".into())
            .await
            .unwrap_err()
            .into();
        assert!(matches!(err, NoSuchBucket(_)));

        let create = backend
            .create_bucket(s3_request(dto::CreateBucketInput {
                bucket: "data".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(create.output.location.as_deref(), Some("/data"));

        // Duplicate create → BucketAlreadyExists.
        let err = backend
            .create_bucket(s3_request(dto::CreateBucketInput {
                bucket: "data".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "BucketAlreadyOwnedByYou");

        // Head.
        backend
            .head_bucket(s3_request(dto::HeadBucketInput {
                bucket: "data".into(),
                ..Default::default()
            }))
            .await
            .unwrap();

        // List.
        let list = backend
            .list_buckets(s3_request(ListBucketsInput::default()))
            .await
            .unwrap();
        let names: Vec<String> = list
            .output
            .buckets
            .unwrap()
            .into_iter()
            .filter_map(|b| b.name)
            .collect();
        assert_eq!(names, ["data"]);

        // Location.
        let loc = backend
            .get_bucket_location(s3_request(dto::GetBucketLocationInput {
                bucket: "data".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(
            loc.output.location_constraint.unwrap().as_str(),
            "us-east-1"
        );

        // Delete.
        backend
            .delete_bucket(s3_request(dto::DeleteBucketInput {
                bucket: "data".into(),
                ..Default::default()
            }))
            .await
            .unwrap();

        // Missing bucket → NoSuchBucket.
        let err = backend
            .head_bucket(s3_request(dto::HeadBucketInput {
                bucket: "data".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchBucket");
    }

    #[tokio::test]
    async fn invalid_bucket_names_rejected() {
        let backend = backend();
        let err = backend
            .create_bucket(s3_request(dto::CreateBucketInput {
                bucket: "Bad_Name".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidBucketName");
    }

    #[tokio::test]
    async fn delete_non_empty_is_bucket_not_empty() {
        let backend = backend();
        backend
            .create_bucket(s3_request(dto::CreateBucketInput {
                bucket: "data".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let storage = backend.storage();
        storage
            .put_object(&"data".into(), &"a.txt".into(), body(b"x"))
            .await
            .unwrap();
        let err = backend
            .delete_bucket(s3_request(dto::DeleteBucketInput {
                bucket: "data".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "BucketNotEmpty");
    }

    #[test]
    fn backend_conformance_backing() {
        // The mapping's storage backend must pass the conformance harness
        // (the reference in-memory backend does; the fs backend is asserted
        // in tinio-fs).
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let storage = MemoryStorage::new().unwrap();
            assert_conformance(&storage).await;
        });
    }
}
