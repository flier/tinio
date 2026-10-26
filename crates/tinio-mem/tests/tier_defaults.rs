//! The tier contract's defaults on a backend without tiers.
//!
//! `tinio-mem` implements neither storage classes nor transitions, so it is
//! the reference for the contract's defaults: a class read answers
//! `STANDARD`, a transition is refused, the metrics scan is the plain listing,
//! and the tier label exists for the metrics vocabulary.

use std::time::SystemTime;

use bytes::Bytes;
use futures::stream;
use tinio_core::{
    BodyStream, BucketOps, Class, ListObjectsParams, ObjectOps, Storage, acl, bucket, object,
    storage::{self, Error as StorageError},
};
use tinio_mem::{Error as MemError, MemoryStorage};
use tinio_util::metrics::Backend;

#[tokio::test]
async fn a_non_tier_backend_defaults_to_standard_and_refuses_transition() {
    let backend = MemoryStorage::new().unwrap();
    let bucket = bucket::name("data").unwrap();
    let key = object::key("dir/a.txt").unwrap();

    assert!(
        !backend.supports_tiers(),
        "the in-memory backend has no tiers"
    );
    assert_eq!(
        backend.storage_class(&bucket, &key).await.unwrap(),
        Class::Standard,
        "every object of a non-tier backend reads as STANDARD"
    );

    let refused = backend
        .transition(&bucket, &key, Class::StandardIa, SystemTime::now())
        .await
        .unwrap_err();
    assert!(
        matches!(refused, MemError::Storage(StorageError::NoTiers)),
        "the default refusal is NoTiers: {refused:?}"
    );
    assert_eq!(
        storage::Error::NoTiers.to_string(),
        "storage tiers are not supported by this backend"
    );
}

#[tokio::test]
async fn metrics_scan_defaults_to_list_objects() {
    let backend = MemoryStorage::new().unwrap();
    let bucket = bucket::name("data").unwrap();
    let key = object::key("dir/a.txt").unwrap();
    backend
        .create_bucket(&bucket, None, &acl::Acl::default_private(None))
        .await
        .unwrap();
    let body: BodyStream = Box::pin(stream::iter([Ok(Bytes::from_static(b"payload"))]));
    backend.put_object(&bucket, &key, body).await.unwrap();

    let scan = backend
        .metrics_scan(ListObjectsParams {
            bucket: bucket.clone(),
            prefix: "dir/".into(),
            delimiter: None,
            start_after: None,
            max_keys: 10,
        })
        .await
        .unwrap();
    let listed = backend
        .list_objects(ListObjectsParams {
            bucket,
            prefix: "dir/".into(),
            delimiter: None,
            start_after: None,
            max_keys: 10,
        })
        .await
        .unwrap();

    assert_eq!(
        scan, listed,
        "the default metrics scan is the plain listing, page for page"
    );
    let keys: Vec<String> = scan
        .objects
        .iter()
        .map(|info| info.key.to_string())
        .collect();
    assert_eq!(keys, ["dir/a.txt"]);
}

#[test]
fn the_tier_backend_label_is_available() {
    assert_eq!(Backend::Tier.as_str(), "tier");
}
