use async_trait::async_trait;
use s3s::{S3, S3Request, S3Response, S3Result, dto};

use super::S3Backend;
use crate::_core::storage::Storage;

#[async_trait]
impl<S: Storage> S3 for S3Backend<S> {
    // --- buckets (T047) ---
    async fn create_bucket(
        &self,
        req: S3Request<dto::CreateBucketInput>,
    ) -> S3Result<S3Response<dto::CreateBucketOutput>> {
        self.op_create_bucket(req).await
    }

    async fn delete_bucket(
        &self,
        req: S3Request<dto::DeleteBucketInput>,
    ) -> S3Result<S3Response<dto::DeleteBucketOutput>> {
        self.op_delete_bucket(req).await
    }

    async fn head_bucket(
        &self,
        req: S3Request<dto::HeadBucketInput>,
    ) -> S3Result<S3Response<dto::HeadBucketOutput>> {
        self.op_head_bucket(req).await
    }

    async fn list_buckets(
        &self,
        req: S3Request<dto::ListBucketsInput>,
    ) -> S3Result<S3Response<dto::ListBucketsOutput>> {
        self.op_list_buckets(req).await
    }

    async fn get_bucket_location(
        &self,
        req: S3Request<dto::GetBucketLocationInput>,
    ) -> S3Result<S3Response<dto::GetBucketLocationOutput>> {
        self.op_get_bucket_location(req).await
    }

    // --- bucket tagging (spec 2026-08-31) ---
    async fn get_bucket_tagging(
        &self,
        req: S3Request<dto::GetBucketTaggingInput>,
    ) -> S3Result<S3Response<dto::GetBucketTaggingOutput>> {
        self.op_get_bucket_tagging(req).await
    }

    async fn put_bucket_tagging(
        &self,
        req: S3Request<dto::PutBucketTaggingInput>,
    ) -> S3Result<S3Response<dto::PutBucketTaggingOutput>> {
        self.op_put_bucket_tagging(req).await
    }

    async fn delete_bucket_tagging(
        &self,
        req: S3Request<dto::DeleteBucketTaggingInput>,
    ) -> S3Result<S3Response<dto::DeleteBucketTaggingOutput>> {
        self.op_delete_bucket_tagging(req).await
    }

    // --- bucket CORS (spec 2026-09-05) ---
    #[cfg(feature = "cors")]
    async fn get_bucket_cors(
        &self,
        req: S3Request<dto::GetBucketCorsInput>,
    ) -> S3Result<S3Response<dto::GetBucketCorsOutput>> {
        self.op_get_bucket_cors(req).await
    }

    #[cfg(feature = "cors")]
    async fn put_bucket_cors(
        &self,
        req: S3Request<dto::PutBucketCorsInput>,
    ) -> S3Result<S3Response<dto::PutBucketCorsOutput>> {
        self.op_put_bucket_cors(req).await
    }

    #[cfg(feature = "cors")]
    async fn delete_bucket_cors(
        &self,
        req: S3Request<dto::DeleteBucketCorsInput>,
    ) -> S3Result<S3Response<dto::DeleteBucketCorsOutput>> {
        self.op_delete_bucket_cors(req).await
    }

    // --- objects + copy (T048) ---
    async fn put_object(
        &self,
        req: S3Request<dto::PutObjectInput>,
    ) -> S3Result<S3Response<dto::PutObjectOutput>> {
        self.op_put_object(req).await
    }

    async fn get_object(
        &self,
        req: S3Request<dto::GetObjectInput>,
    ) -> S3Result<S3Response<dto::GetObjectOutput>> {
        self.op_get_object(req).await
    }

    async fn head_object(
        &self,
        req: S3Request<dto::HeadObjectInput>,
    ) -> S3Result<S3Response<dto::HeadObjectOutput>> {
        self.op_head_object(req).await
    }

    async fn get_object_attributes(
        &self,
        req: S3Request<dto::GetObjectAttributesInput>,
    ) -> S3Result<S3Response<dto::GetObjectAttributesOutput>> {
        self.op_get_object_attributes(req).await
    }

    async fn delete_object(
        &self,
        req: S3Request<dto::DeleteObjectInput>,
    ) -> S3Result<S3Response<dto::DeleteObjectOutput>> {
        self.op_delete_object(req).await
    }

    async fn delete_objects(
        &self,
        req: S3Request<dto::DeleteObjectsInput>,
    ) -> S3Result<S3Response<dto::DeleteObjectsOutput>> {
        self.op_delete_objects(req).await
    }

    async fn get_object_tagging(
        &self,
        req: S3Request<dto::GetObjectTaggingInput>,
    ) -> S3Result<S3Response<dto::GetObjectTaggingOutput>> {
        self.op_get_object_tagging(req).await
    }

    async fn put_object_tagging(
        &self,
        req: S3Request<dto::PutObjectTaggingInput>,
    ) -> S3Result<S3Response<dto::PutObjectTaggingOutput>> {
        self.op_put_object_tagging(req).await
    }

    async fn delete_object_tagging(
        &self,
        req: S3Request<dto::DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<dto::DeleteObjectTaggingOutput>> {
        self.op_delete_object_tagging(req).await
    }

    #[cfg(feature = "copy")]
    async fn copy_object(
        &self,
        req: S3Request<dto::CopyObjectInput>,
    ) -> S3Result<S3Response<dto::CopyObjectOutput>> {
        self.op_copy_object(req).await
    }

    #[cfg(feature = "copy")]
    async fn rename_object(
        &self,
        req: S3Request<dto::RenameObjectInput>,
    ) -> S3Result<S3Response<dto::RenameObjectOutput>> {
        self.op_rename_object(req).await
    }

    // --- listing (T049) ---
    #[cfg(feature = "list-v1")]
    async fn list_objects(
        &self,
        req: S3Request<dto::ListObjectsInput>,
    ) -> S3Result<S3Response<dto::ListObjectsOutput>> {
        self.op_list_objects(req).await
    }

    #[cfg(feature = "list-v2")]
    async fn list_objects_v2(
        &self,
        req: S3Request<dto::ListObjectsV2Input>,
    ) -> S3Result<S3Response<dto::ListObjectsV2Output>> {
        self.op_list_objects_v2(req).await
    }

    // --- multipart (T050) ---
    #[cfg(feature = "multipart")]
    async fn create_multipart_upload(
        &self,
        req: S3Request<dto::CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::CreateMultipartUploadOutput>> {
        self.op_create_multipart_upload(req).await
    }

    #[cfg(feature = "multipart")]
    async fn upload_part(
        &self,
        req: S3Request<dto::UploadPartInput>,
    ) -> S3Result<S3Response<dto::UploadPartOutput>> {
        self.op_upload_part(req).await
    }

    #[cfg(all(feature = "multipart", feature = "copy"))]
    async fn upload_part_copy(
        &self,
        req: S3Request<dto::UploadPartCopyInput>,
    ) -> S3Result<S3Response<dto::UploadPartCopyOutput>> {
        self.op_upload_part_copy(req).await
    }

    #[cfg(feature = "multipart")]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<dto::CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::CompleteMultipartUploadOutput>> {
        self.op_complete_multipart_upload(req).await
    }

    #[cfg(feature = "multipart")]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<dto::AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::AbortMultipartUploadOutput>> {
        self.op_abort_multipart_upload(req).await
    }

    #[cfg(feature = "multipart")]
    async fn list_parts(
        &self,
        req: S3Request<dto::ListPartsInput>,
    ) -> S3Result<S3Response<dto::ListPartsOutput>> {
        self.op_list_parts(req).await
    }

    #[cfg(feature = "multipart")]
    async fn list_multipart_uploads(
        &self,
        req: S3Request<dto::ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<dto::ListMultipartUploadsOutput>> {
        self.op_list_multipart_uploads(req).await
    }
}
