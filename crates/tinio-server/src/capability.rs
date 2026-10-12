//! The `[s3]` capability gate as a decorator (FR-021).
//!
//! Refusals used to be the first statement of each handler
//! (`S3Backend::require_cap`, `require_multipart`); they now read the shared
//! operation table instead, so "which toggle refuses which operation" is one
//! column rather than a five-module grep. Disabled operations answer
//! `NotImplemented` with `"<Operation> is disabled"`.
//!
//! Only the *refusals* moved: the toggles' data-path effects (dropping tags
//! and ACL headers, the checksum tee, the CORS route and response
//! decoration) stay in the handlers.

use async_trait::async_trait;
use s3s::{S3, S3Request, S3Response, S3Result, dto, s3_error};

use crate::{
    backend::{Capabilities, ops::tinio_s3_ops},
    metrics::wire_name,
};

/// Wraps an `S3` implementation and refuses the operations its `[s3]`
/// capability disables.
#[derive(Debug, Clone)]
pub struct CapabilityS3<T> {
    inner: T,
    caps: Capabilities,
}

impl<T> CapabilityS3<T> {
    /// Wrap `inner`, gating it with `caps`.
    pub fn new(inner: T, caps: Capabilities) -> Self {
        Self { inner, caps }
    }

    /// The wrapped implementation.
    pub fn inner(&self) -> &T {
        &self.inner
    }
}

macro_rules! capability_impl {
    (
        read { $( $(#[handler($rop:ident)])? $(#[gated($($rg:ident $(: $rf:literal)?),+)])? $(#[on_ok($rh:expr)])? fn $r:ident ( $ri:ident ) -> $ro:ident; )* }
        write { $( $(#[handler($wop:ident)])? $(#[gated($($wg:ident $(: $wf:literal)?),+)])? $(#[on_ok($wh:expr)])? fn $w:ident ( $wi:ident ) -> $wo:ident; )* }
    ) => {
        #[async_trait]
        impl<T: S3 + Send + Sync> S3 for CapabilityS3<T> {
            $(
                $( $( $(#[cfg(feature = $rf)])? )+ )?
                async fn $r(&self, req: S3Request<dto::$ri>) -> S3Result<S3Response<dto::$ro>> {
                    if !(true $( $( && self.caps.$rg )+ )?) {
                        let name = wire_name!($ri);
                        return Err(s3_error!(NotImplemented, "{name} is disabled"));
                    }
                    self.inner.$r(req).await
                }
            )*
            $(
                $( $( $(#[cfg(feature = $wf)])? )+ )?
                async fn $w(&self, req: S3Request<dto::$wi>) -> S3Result<S3Response<dto::$wo>> {
                    if !(true $( $( && self.caps.$wg )+ )?) {
                        let name = wire_name!($wi);
                        return Err(s3_error!(NotImplemented, "{name} is disabled"));
                    }
                    self.inner.$w(req).await
                }
            )*
        }
    };
}

tinio_s3_ops!(capability_impl);

#[cfg(test)]
mod tests {
    use s3s::{S3Error, dto};

    use super::*;
    use crate::backend::{
        Capabilities,
        testutil::{s3_request, setup_gated_with_caps, setup_with_caps},
    };

    /// Drive one `tagging`-gated operation through the decorator, by wire
    /// name. The gate lives here now, so the test must wrap the backend —
    /// and by value: s3s implements `S3` for concrete types only (no
    /// blanket impl for `&T`, `Box<T>` or `Arc<T>`), so a `&S3Backend`
    /// would not satisfy `T: S3`.
    ///
    /// Every row here is gated by `tagging`, a capability with no cargo
    /// feature: a `#[gated(<cap>: "<feature>")]` row is stripped with its
    /// feature (s3s's trait default would answer instead), so driving one
    /// of those would quietly stop testing the gate in a stripped build.
    /// `PutBucketTaggingInput` and `PutObjectTaggingInput` have a required
    /// `tagging` field — no `Default` derive — and are built explicitly,
    /// as `backend/buckets.rs` and `backend/objects.rs` build them.
    async fn refusal<T: S3 + Send + Sync>(backend: &CapabilityS3<T>, op: &str) -> S3Error {
        match op {
            "GetBucketTagging" => backend
                .get_bucket_tagging(s3_request(dto::GetBucketTaggingInput {
                    bucket: "data".into(),
                    ..Default::default()
                }))
                .await
                .unwrap_err(),
            "PutBucketTagging" => backend
                .put_bucket_tagging(s3_request(dto::PutBucketTaggingInput {
                    bucket: "data".into(),
                    tagging: dto::Tagging { tag_set: vec![] },
                    checksum_algorithm: None,
                    content_md5: None,
                    expected_bucket_owner: None,
                }))
                .await
                .unwrap_err(),
            "DeleteBucketTagging" => backend
                .delete_bucket_tagging(s3_request(dto::DeleteBucketTaggingInput {
                    bucket: "data".into(),
                    ..Default::default()
                }))
                .await
                .unwrap_err(),
            "GetObjectTagging" => backend
                .get_object_tagging(s3_request(dto::GetObjectTaggingInput {
                    bucket: "data".into(),
                    key: "a.txt".into(),
                    ..Default::default()
                }))
                .await
                .unwrap_err(),
            "PutObjectTagging" => backend
                .put_object_tagging(s3_request(dto::PutObjectTaggingInput {
                    bucket: "data".into(),
                    key: "a.txt".into(),
                    tagging: dto::Tagging { tag_set: vec![] },
                    checksum_algorithm: None,
                    content_md5: None,
                    expected_bucket_owner: None,
                    request_payer: None,
                    version_id: None,
                }))
                .await
                .unwrap_err(),
            "DeleteObjectTagging" => backend
                .delete_object_tagging(s3_request(dto::DeleteObjectTaggingInput {
                    bucket: "data".into(),
                    key: "a.txt".into(),
                    ..Default::default()
                }))
                .await
                .unwrap_err(),
            other => panic!("unhandled op {other}"),
        }
    }

    /// A disabled toggle answers `NotImplemented` under the operation's
    /// wire name — the decorator's own refusal, no handler involved. Every
    /// `tagging`-gated row: all six, reading and writing, the ones any
    /// build compiles.
    #[tokio::test]
    async fn a_disabled_toggle_refuses_with_the_operation_name() {
        let caps = Capabilities {
            tagging: false,
            ..Default::default()
        };
        let (backend, _bucket) = setup_gated_with_caps(caps).await;
        for op in [
            "GetBucketTagging",
            "PutBucketTagging",
            "DeleteBucketTagging",
            "GetObjectTagging",
            "PutObjectTagging",
            "DeleteObjectTagging",
        ] {
            let err = refusal(&backend, op).await;
            assert_eq!(err.code().as_str(), "NotImplemented", "{op}: {err:?}");
            let expected = format!("{op} is disabled");
            assert_eq!(err.message(), Some(expected.as_str()), "{op}: {err:?}");
        }
    }

    /// The complement: with the toggle on the decorator forwards, so the
    /// same operations reach the handler and succeed on the fixture bucket.
    #[tokio::test]
    async fn an_enabled_toggle_reaches_the_handler() {
        let (inner, bucket) = setup_with_caps(Default::default()).await;
        let wrapped = CapabilityS3::new(inner, Default::default());
        let out = wrapped
            .get_bucket_tagging(s3_request(dto::GetBucketTaggingInput {
                bucket: bucket.clone(),
                ..Default::default()
            }))
            .await
            .expect("GetBucketTagging with the toggle on is not refused");
        assert!(out.output.tag_set.is_empty());
        wrapped
            .put_bucket_tagging(s3_request(dto::PutBucketTaggingInput {
                bucket,
                tagging: dto::Tagging { tag_set: vec![] },
                checksum_algorithm: None,
                content_md5: None,
                expected_bucket_owner: None,
            }))
            .await
            .expect("PutBucketTagging with the toggle on is not refused");
    }

    /// Drive one `versioning`-gated operation through the decorator, by
    /// wire name — [`refusal`]'s shape, for the three rows whose
    /// `#[gated(versioning: "versioning")]` clause strips them (and this
    /// test with them) from a build without the feature.
    #[cfg(feature = "versioning")]
    async fn versioning_refusal<T: S3 + Send + Sync>(
        backend: &CapabilityS3<T>,
        op: &str,
    ) -> S3Error {
        match op {
            "GetBucketVersioning" => backend
                .get_bucket_versioning(s3_request(dto::GetBucketVersioningInput {
                    bucket: "data".into(),
                    ..Default::default()
                }))
                .await
                .unwrap_err(),
            "PutBucketVersioning" => backend
                .put_bucket_versioning(s3_request(dto::PutBucketVersioningInput {
                    bucket: "data".into(),
                    versioning_configuration: dto::VersioningConfiguration {
                        status: Some(dto::BucketVersioningStatus::from_static(
                            dto::BucketVersioningStatus::ENABLED,
                        )),
                        mfa_delete: None,
                    },
                    checksum_algorithm: None,
                    content_md5: None,
                    expected_bucket_owner: None,
                    mfa: None,
                }))
                .await
                .unwrap_err(),
            "ListObjectVersions" => backend
                .list_object_versions(s3_request(dto::ListObjectVersionsInput {
                    bucket: "data".into(),
                    ..Default::default()
                }))
                .await
                .unwrap_err(),
            other => panic!("unhandled op {other}"),
        }
    }

    /// A disabled `versioning` toggle answers `NotImplemented` under each
    /// operation's wire name — the decorator's own refusal, the family's
    /// exact sentence, no handler involved.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_disabled_versioning_toggle_refuses_the_three_versioning_rows() {
        let caps = Capabilities {
            versioning: false,
            ..Default::default()
        };
        let (backend, _bucket) = setup_gated_with_caps(caps).await;
        for op in [
            "GetBucketVersioning",
            "PutBucketVersioning",
            "ListObjectVersions",
        ] {
            let err = versioning_refusal(&backend, op).await;
            assert_eq!(err.code().as_str(), "NotImplemented", "{op}: {err:?}");
            let expected = format!("{op} is disabled");
            assert_eq!(err.message(), Some(expected.as_str()), "{op}: {err:?}");
        }
    }

    /// The complement: with the toggle on the decorator forwards the same
    /// three operations, so they reach the handler and answer the
    /// unversioned fixture bucket — an empty state, an accepted write, an
    /// empty listing — rather than the gate's `NotImplemented`.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn an_enabled_versioning_toggle_reaches_the_handler() {
        let (inner, bucket) = setup_with_caps(Default::default()).await;
        let wrapped = CapabilityS3::new(inner, Default::default());
        let out = wrapped
            .get_bucket_versioning(s3_request(dto::GetBucketVersioningInput {
                bucket: bucket.clone(),
                ..Default::default()
            }))
            .await
            .expect("GetBucketVersioning with the toggle on is not refused");
        assert_eq!(out.output.status, None, "the fixture bucket is unversioned");
        wrapped
            .put_bucket_versioning(s3_request(dto::PutBucketVersioningInput {
                bucket: bucket.clone(),
                versioning_configuration: dto::VersioningConfiguration {
                    status: Some(dto::BucketVersioningStatus::from_static(
                        dto::BucketVersioningStatus::ENABLED,
                    )),
                    mfa_delete: None,
                },
                checksum_algorithm: None,
                content_md5: None,
                expected_bucket_owner: None,
                mfa: None,
            }))
            .await
            .expect("PutBucketVersioning with the toggle on is not refused");
        let listed = wrapped
            .list_object_versions(s3_request(dto::ListObjectVersionsInput {
                bucket,
                ..Default::default()
            }))
            .await
            .expect("ListObjectVersions with the toggle on is not refused");
        assert!(
            listed.output.versions.unwrap_or_default().is_empty(),
            "the fixture bucket holds no object version"
        );
    }
}
