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
    backend::{
        S3Backend, clamp_page_size, map_backend_error, normalize_delimiter, normalize_page_size,
    },
};

/// One mapped listing page shared by the V1/V2 XML surfaces.
struct ListPage {
    bucket: String,
    prefix: Option<String>,
    delimiter: Option<String>,
    /// The request's start-after, echoed back only by the V1 surface
    /// (`Marker`); V2 carries its own continuation token.
    #[cfg(feature = "list-v1")]
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
        let requested = max_keys.unwrap_or(1000);
        // Unified page-size policy: a page size < 1 is rejected before
        // any storage call (AWS documents no max-keys range; the
        // strictness is deliberate) — unless the
        // `[s3] allow_zero_page_size` escape hatch restores the legacy
        // clamp-to-0 empty page. The configured `[s3] max_keys` cap
        // clamps the requested size (0 = no clamp); the echoed
        // `MaxKeys` element carries the effective value.
        let max_keys = clamp_page_size(
            normalize_page_size(requested, "max-keys", self.caps.allow_zero_page_size)?,
            self.caps.max_keys,
        );
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
            bucket: String::from(bucket),
            prefix,
            delimiter,
            #[cfg(feature = "list-v1")]
            start_after,
            max_keys: max_keys as i32,
            contents: page
                .objects
                .into_iter()
                .map(|o| dto::Object {
                    e_tag: Some(Self::etag_wire(&o.etag)),
                    key: Some(String::from(o.key)),
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
        let continuation_token = req.input.continuation_token;
        let start_after = req.input.start_after;
        let page = self
            .list_page(
                req.input.bucket,
                req.input.prefix,
                req.input.delimiter,
                // The winner feeds the listing; the echo keeps both
                // originals, so the chosen token alone is cloned
                // (`or_else` spares the loser).
                continuation_token.clone().or_else(|| start_after.clone()),
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
        _core::{
            bucket, object,
            storage::{BucketOps, ObjectOps},
        },
        _mem::MemoryStorage,
        _util::testing::body,
        backend::{
            Capabilities,
            testutil::{s3_request, setup as base_setup},
        },
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

    #[cfg(feature = "list-v1")]
    #[tokio::test]
    async fn v1_allow_zero_page_size_restores_the_legacy_empty_page() {
        // The `[s3] allow_zero_page_size` escape hatch: 0 and negative
        // values answer the empty page (the legacy `.max(0)` clamp),
        // never InvalidArgument.
        let backend = S3Backend::new(
            MemoryStorage::new().unwrap(),
            Capabilities {
                allow_zero_page_size: true,
                ..Default::default()
            },
        );
        let storage = backend.storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        storage
            .put_object(&b, &object::key("a.txt").unwrap(), body("a"))
            .await
            .unwrap();
        for max_keys in [0, -1] {
            let out = backend
                .list_objects(s3_request(dto::ListObjectsInput {
                    bucket: "data".into(),
                    max_keys: Some(max_keys),
                    ..Default::default()
                }))
                .await
                .unwrap()
                .output;
            assert_eq!(out.max_keys, Some(0), "max-keys = {max_keys}");
            assert!(out.contents.as_ref().unwrap().is_empty());
            assert_eq!(out.is_truncated, Some(false));
        }
    }

    #[cfg(feature = "list-v2")]
    #[tokio::test]
    async fn v2_allow_zero_page_size_restores_the_legacy_empty_page() {
        // The `[s3] allow_zero_page_size` escape hatch: 0 answers the
        // empty page (the legacy behavior), never InvalidArgument.
        let backend = S3Backend::new(
            MemoryStorage::new().unwrap(),
            Capabilities {
                allow_zero_page_size: true,
                ..Default::default()
            },
        );
        let storage = backend.storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        storage
            .put_object(&b, &object::key("a.txt").unwrap(), body("a"))
            .await
            .unwrap();
        for max_keys in [0, -1] {
            let out = backend
                .list_objects_v2(s3_request(dto::ListObjectsV2Input {
                    bucket: "data".into(),
                    max_keys: Some(max_keys),
                    ..Default::default()
                }))
                .await
                .unwrap()
                .output;
            assert_eq!(out.max_keys, Some(0), "max-keys = {max_keys}");
            assert!(out.contents.as_ref().unwrap().is_empty());
            assert_eq!(out.is_truncated, Some(false));
        }
    }

    #[cfg(feature = "list-v1")]
    #[tokio::test]
    async fn v1_echoes_the_effective_page_size_after_a_clamp() {
        let backend = S3Backend::new(
            MemoryStorage::new().unwrap(),
            Capabilities {
                max_keys: 2,
                ..Default::default()
            },
        );
        let storage = backend.storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        for key in ["a.txt", "b.txt", "c.txt"] {
            storage
                .put_object(&b, &object::key(key).unwrap(), body(key))
                .await
                .unwrap();
        }
        let out = backend
            .list_objects(s3_request(dto::ListObjectsInput {
                bucket: "data".into(),
                max_keys: Some(1000),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(
            out.max_keys,
            Some(2),
            "the response echoes the effective (clamped) page size"
        );
        assert_eq!(out.is_truncated, Some(true));
        assert_eq!(out.contents.as_ref().unwrap().len(), 2);
    }

    #[cfg(feature = "list-v2")]
    #[tokio::test]
    async fn v2_echoes_the_effective_page_size_after_a_clamp() {
        let backend = S3Backend::new(
            MemoryStorage::new().unwrap(),
            Capabilities {
                max_keys: 2,
                ..Default::default()
            },
        );
        let storage = backend.storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        for key in ["a.txt", "b.txt", "c.txt"] {
            storage
                .put_object(&b, &object::key(key).unwrap(), body(key))
                .await
                .unwrap();
        }
        let out = backend
            .list_objects_v2(s3_request(dto::ListObjectsV2Input {
                bucket: "data".into(),
                max_keys: Some(1000),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(out.max_keys, Some(2));
        assert_eq!(out.is_truncated, Some(true));
        assert_eq!(out.key_count, Some(2));
    }
}
