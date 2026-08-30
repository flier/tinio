//! Shared unit-test helpers of the backend modules.

use http::{Extensions, HeaderMap, Method, Uri};
use s3s::S3Request;

use super::S3Backend;
use crate::{
    _core::{bucket, storage::BucketOps},
    _mem::MemoryStorage,
};

/// A minimal `S3Request` with default headers (tests fill the input).
pub(crate) fn s3_request<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: Method::GET,
        uri: Uri::default(),
        headers: HeaderMap::new(),
        extensions: Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// A fresh backend over `MemoryStorage` with a `data` bucket created;
/// returns the bucket name as a string.
pub(crate) async fn setup() -> (S3Backend<MemoryStorage>, String) {
    let backend = S3Backend::new(MemoryStorage::new().unwrap(), Default::default());
    let storage = backend.storage();
    let b = "data".to_string();
    storage
        .create_bucket(&bucket::name(&b).unwrap())
        .await
        .unwrap();
    (backend, b)
}
