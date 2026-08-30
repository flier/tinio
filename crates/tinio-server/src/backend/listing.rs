#![cfg(any(feature = "list-v1", feature = "list-v2"))]

//! S3 listing V1/V2 of the mapping layer (task T049).
//!
//! ListObjects (V1) and ListObjectsV2 (V2) over the storage contract's
//! shared listing engine — prefix filtering, delimiter grouping, and
//! pagination per S3 semantics (FR-004). Compile-time gates `list-v1` /
//! `list-v2` and the runtime `[s3]` toggles answer `NotImplemented`
//! (FR-021).

use s3s::{S3Request, S3Response, S3Result, dto};

use crate::{
    _core::storage::{ListObjectsParams, Storage},
    backend::{S3Backend, map_backend_error, normalize_delimiter},
};

/// One mapped listing page shared by the V1/V2 XML surfaces.
struct ListPage {
    bucket: String,
    prefix: Option<String>,
    delimiter: Option<String>,
    start_after: Option<String>,
    max_keys: i32,
    contents: Vec<dto::Object>,
    common_prefixes: Vec<dto::CommonPrefix>,
    truncated: bool,
    next_start_after: Option<String>,
}

impl<S: Storage> S3Backend<S> {
    /// The shared listing core: fetch and map one page of objects and
    /// common prefixes (FR-004); only the continuation token differs
    /// between the V1 (`marker`) and V2 (`continuation_token`) surfaces.
    async fn list_page(
        &self,
        bucket: String,
        prefix: Option<String>,
        delimiter: Option<String>,
        start_after: Option<String>,
        max_keys: Option<i32>,
    ) -> S3Result<ListPage> {
        let bucket = self.bucket(bucket)?;
        let max_keys = max_keys.unwrap_or(1000).max(0) as usize;
        // An empty `delimiter=` value means "no delimiter" (S3 semantics;
        // clients like mc always send it) — a `Some("")` would roll every
        // object up into an empty common prefix and empty the page. The
        // boundary rule has one home (`normalize_delimiter`).
        let delimiter = normalize_delimiter(delimiter);
        let page = self
            .storage
            .list_objects(ListObjectsParams {
                bucket: bucket.clone(),
                prefix: prefix.clone().unwrap_or_default(),
                delimiter: delimiter.clone(),
                start_after: start_after.clone(),
                max_keys,
            })
            .await
            .map_err(map_backend_error)?;
        Ok(ListPage {
            bucket: bucket.to_string(),
            prefix,
            delimiter,
            start_after,
            max_keys: max_keys as i32,
            contents: page
                .objects
                .into_iter()
                .map(|o| dto::Object {
                    e_tag: Some(Self::etag_wire(&o.etag)),
                    key: Some(o.key.to_string()),
                    last_modified: Some(Self::last_modified(o.last_modified)),
                    size: Some(o.size as i64),
                    ..Default::default()
                })
                .collect(),
            common_prefixes: page
                .common_prefixes
                .into_iter()
                .map(|p| dto::CommonPrefix { prefix: Some(p) })
                .collect(),
            truncated: page.truncated,
            next_start_after: page.next_start_after,
        })
    }

    #[cfg(feature = "list-v1")]
    pub(crate) async fn op_list_objects(
        &self,
        req: S3Request<dto::ListObjectsInput>,
    ) -> S3Result<S3Response<dto::ListObjectsOutput>> {
        Self::require_cap(self.caps.list_objects_v1, "ListObjects")?;
        let page = self
            .list_page(
                req.input.bucket,
                req.input.prefix,
                req.input.delimiter,
                req.input.marker,
                req.input.max_keys,
            )
            .await?;
        Ok(S3Response::new(dto::ListObjectsOutput {
            name: Some(page.bucket),
            prefix: page.prefix,
            marker: page.start_after,
            max_keys: Some(page.max_keys),
            is_truncated: Some(page.truncated),
            next_marker: page.next_start_after,
            contents: Some(page.contents),
            common_prefixes: Some(page.common_prefixes),
            delimiter: page.delimiter,
            ..Default::default()
        }))
    }

    #[cfg(feature = "list-v2")]
    pub(crate) async fn op_list_objects_v2(
        &self,
        req: S3Request<dto::ListObjectsV2Input>,
    ) -> S3Result<S3Response<dto::ListObjectsV2Output>> {
        Self::require_cap(self.caps.list_objects_v2, "ListObjectsV2")?;
        let continuation_token = req.input.continuation_token.clone();
        let start_after = req.input.start_after.clone();
        let page = self
            .list_page(
                req.input.bucket,
                req.input.prefix,
                req.input.delimiter,
                continuation_token.clone().or(start_after.clone()),
                req.input.max_keys,
            )
            .await?;
        Ok(S3Response::new(dto::ListObjectsV2Output {
            name: Some(page.bucket),
            prefix: page.prefix,
            max_keys: Some(page.max_keys),
            key_count: Some(page.contents.len() as i32),
            continuation_token,
            is_truncated: Some(page.truncated),
            next_continuation_token: page.next_start_after,
            contents: Some(page.contents),
            common_prefixes: Some(page.common_prefixes),
            delimiter: page.delimiter,
            start_after,
            ..Default::default()
        }))
    }
}

#[cfg(test)]
mod tests {
    use s3s::S3;

    use super::*;
    use crate::{
        _core::{bucket, object, storage::ObjectOps},
        _mem::MemoryStorage,
        _util::testing::body,
        backend::testutil::{s3_request, setup as base_setup},
    };

    async fn setup() -> (S3Backend<MemoryStorage>, String) {
        let (backend, b) = base_setup().await;
        let storage = backend.storage();
        for key in ["a.txt", "b.txt", "dir/c.txt", "dir/sub/d.txt"] {
            storage
                .put_object(
                    &bucket::name(&b).unwrap(),
                    &object::key(key).unwrap(),
                    body(format!("{key}!")),
                )
                .await
                .unwrap();
        }
        (backend, b)
    }

    #[cfg(feature = "list-v1")]
    #[tokio::test]
    async fn empty_delimiter_means_no_delimiter() {
        // `delimiter=` (empty) is sent by clients like mc as "no
        // delimiter" — it must not roll every object into an empty
        // common prefix (an empty page made mc's recursive delete
        // believe the bucket was empty).
        let (backend, b) = setup().await;
        let list = backend
            .list_objects(s3_request(dto::ListObjectsInput {
                bucket: b.clone(),
                delimiter: Some(String::new()),
                ..Default::default()
            }))
            .await
            .unwrap();
        let out = list.output;
        assert_eq!(out.contents.as_ref().unwrap().len(), 4);
        assert!(
            out.common_prefixes.as_ref().unwrap().is_empty(),
            "an empty delimiter must not produce common prefixes: {:?}",
            out.common_prefixes
        );
    }

    #[cfg(feature = "list-v1")]
    #[tokio::test]
    async fn v1_full_and_delimiter_listing() {
        let (backend, b) = setup().await;
        let list = backend
            .list_objects(s3_request(dto::ListObjectsInput {
                bucket: b.clone(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let out = list.output;
        assert_eq!(out.name.as_deref(), Some("data"));
        assert_eq!(out.contents.as_ref().unwrap().len(), 4);
        assert_eq!(out.is_truncated, Some(false));

        let list = backend
            .list_objects(s3_request(dto::ListObjectsInput {
                bucket: b.clone(),
                delimiter: Some("/".into()),
                ..Default::default()
            }))
            .await
            .unwrap();
        let out = list.output;
        assert_eq!(out.contents.as_ref().unwrap().len(), 2);
        let prefixes: Vec<String> = out
            .common_prefixes
            .unwrap()
            .into_iter()
            .filter_map(|p| p.prefix)
            .collect();
        assert_eq!(prefixes, ["dir/"]);
    }

    #[cfg(feature = "list-v2")]
    #[tokio::test]
    async fn v2_pagination_and_prefix() {
        let (backend, b) = setup().await;
        let list = backend
            .list_objects_v2(s3_request(dto::ListObjectsV2Input {
                bucket: b.clone(),
                max_keys: Some(2),
                ..Default::default()
            }))
            .await
            .unwrap();
        let out = list.output;
        assert_eq!(out.key_count, Some(2));
        assert_eq!(out.is_truncated, Some(true));
        let token = out.next_continuation_token.unwrap();

        let list = backend
            .list_objects_v2(s3_request(dto::ListObjectsV2Input {
                bucket: b.clone(),
                continuation_token: Some(token),
                max_keys: Some(1000),
                ..Default::default()
            }))
            .await
            .unwrap();
        let out = list.output;
        assert_eq!(out.key_count, Some(2));
        assert_eq!(out.is_truncated, Some(false));

        // Prefixed listing.
        let list = backend
            .list_objects_v2(s3_request(dto::ListObjectsV2Input {
                bucket: b,
                prefix: Some("dir/".into()),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(list.output.key_count, Some(2));
    }

    #[cfg(feature = "list-v1")]
    #[tokio::test]
    async fn v1_missing_bucket_is_no_such_bucket() {
        let (backend, _) = setup().await;
        let err = backend
            .list_objects(s3_request(dto::ListObjectsInput {
                bucket: "ghost".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchBucket");
    }
}
