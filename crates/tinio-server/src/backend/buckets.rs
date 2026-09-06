//! Bucket operations of the S3 mapping layer (task T047).
//!
//! CreateBucket/DeleteBucket/HeadBucket/ListBuckets/GetBucketLocation over
//! the storage contract. Creation dates come from the backend
//! (`buckets.json`); GetBucketLocation always answers `us-east-1`
//! (s3-surface.md). Storage errors map to S3 codes via
//! [`map_backend_error`](crate::backend::map_backend_error).
//!
//! The bucket-tagging trio (Get|Put|DeleteBucketTagging, spec 2026-08-31)
//! lives here too, gated on `caps.tagging` — the toggle off answers
//! `NotImplemented` (FR-021), the write validates through the core tag
//! type at the 50-tag bucket cap.

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use s3s::{
    S3Request, S3Response, S3Result,
    dto::{
        Bucket, BucketLocationConstraint, CreateBucketInput, CreateBucketOutput, DeleteBucketInput,
        DeleteBucketOutput, DeleteBucketTaggingInput, DeleteBucketTaggingOutput,
        GetBucketLocationInput, GetBucketLocationOutput, GetBucketTaggingInput,
        GetBucketTaggingOutput, HeadBucketInput, HeadBucketOutput, ListBucketsInput,
        ListBucketsOutput, PutBucketTaggingInput, PutBucketTaggingOutput,
    },
    s3_error,
};

use crate::{
    _config::s3::MAX_BUCKETS,
    _core::{
        acl, object,
        storage::{ListBucketsParams, Storage},
    },
    backend::{
        S3Backend, clamp_page_size, map_backend_error,
        tags::{tag_set_from_tags, tags_from_tag_set},
    },
};

#[cfg(feature = "acl")]
use crate::{
    _auth::canned::GrantHeaders,
    backend::acls::{
        bucket_acl_grants, bucket_write_acl, grant_headers, grants_dto, policy_owner_matches,
        require_content_md5,
    },
};
#[cfg(feature = "acl")]
use s3s::dto;

/// The ListBuckets default page size when `max-buckets` is absent — the
/// AWS documented default (2025-03 API), the config's `max_buckets` cap
/// default ([`MAX_BUCKETS`]): one home for the number.
const DEFAULT_MAX_BUCKETS: i32 = MAX_BUCKETS as i32;

impl<S: Storage> S3Backend<S> {
    pub(crate) async fn op_create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let name = self.bucket(req.input.bucket)?;
        // Write-path ACL (spec 2026-09-05, Task 11): an identity-mode
        // create resolves the requester's owner and expands the
        // request's canned / `x-amz-grant-*` headers — through
        // `canned_bucket_grants` DIRECTLY (carry 1): `expand_acl` is
        // object-flavored, and the `bucket-owner-*` names are ignored
        // on buckets. An unheadered create records the requester's
        // private default. No-identity mode keeps the Task 5 defaults
        // (B4 rule 3).
        #[cfg(feature = "acl")]
        let write_acl: Option<(acl::OwnerId, acl::Acl)> = if self.caps.acl
            && self.identity.is_some()
        {
            let owner = self
                .owner_for(req.credentials.as_ref())
                .expect("identity attached above");
            let acl = bucket_write_acl(
                &owner,
                req.input.acl.as_ref().map(|c| c.as_str()),
                &grant_headers(
                    req.input.grant_full_control.as_deref(),
                    req.input.grant_read.as_deref(),
                    req.input.grant_read_acp.as_deref(),
                    req.input.grant_write.as_deref(),
                    req.input.grant_write_acp.as_deref(),
                ),
            )?;
            Some((owner, acl))
        } else {
            None
        };
        #[cfg(not(feature = "acl"))]
        let write_acl: Option<(acl::OwnerId, acl::Acl)> = None;
        // The no-identity / toggle-off default (Task 5).
        let default_acl = acl::Acl::default_private(None);
        let (owner, acl) = match &write_acl {
            Some((owner, acl)) => (Some(owner), acl),
            None => (None, &default_acl),
        };
        self.storage
            .create_bucket(&name, owner, acl)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(CreateBucketOutput {
            location: Some(format!("/{name}")),
        }))
    }

    pub(crate) async fn op_delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let name = self.bucket(req.input.bucket)?;
        self.storage
            .delete_bucket(&name)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(DeleteBucketOutput::default()))
    }

    pub(crate) async fn op_head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let name = self.bucket(req.input.bucket)?;
        self.storage
            .head_bucket(&name)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    pub(crate) async fn op_list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let ListBucketsInput {
            bucket_region: _,
            continuation_token,
            max_buckets,
            prefix,
        } = req.input;
        // AWS documents `max-buckets` as 1..=10,000: out-of-range values
        // answer InvalidArgument — never a silent clamp that would hand
        // a buggy client a ContinuationToken it did not ask for. (The
        // contract keeps the engine's `max = 0` empty-page semantics
        // for direct calls.)
        if let Some(max) = max_buckets
            && !(1..=DEFAULT_MAX_BUCKETS).contains(&max)
        {
            return Err(s3_error!(
                InvalidArgument,
                "max-buckets must be between 1 and {DEFAULT_MAX_BUCKETS}"
            ));
        }
        // The continuation token is the URL-safe no-pad base64 of the
        // previous page's last bucket name — opaque to clients (AWS:
        // "obfuscated and is not a real bucket"), no server-side token
        // state. Bad base64 AND non-UTF-8 payloads answer
        // InvalidArgument; the empty token decodes to the empty marker,
        // which skips nothing.
        let start_after = continuation_token
            .map(|token| {
                // T08: legitimate tokens are the base64 of a bucket name
                // (≤63 ASCII chars), so a longer token can only be
                // malformed — rejected BEFORE the decode, which would
                // allocate ~¾ of the input length en route to
                // InvalidArgument (the token length is bounded only by
                // the HTTP head buffer).
                if token.len() > 256 {
                    return Err(s3_error!(InvalidArgument, "invalid continuation token"));
                }
                let bytes = URL_SAFE_NO_PAD
                    .decode(token.as_bytes())
                    .map_err(|_| s3_error!(InvalidArgument, "invalid continuation token"))?;
                String::from_utf8(bytes)
                    .map_err(|_| s3_error!(InvalidArgument, "invalid continuation token"))
            })
            .transpose()?;
        // The configured cap clamps the requested page size — and the
        // default (a cap of 5 clamps the no-parameter request to 5).
        let requested = max_buckets.unwrap_or(DEFAULT_MAX_BUCKETS) as usize;
        let effective = clamp_page_size(requested, self.caps.max_buckets);
        // The enforced-mode owner filter (spec 2026-09-05): under the
        // active rewrite (feature + `caps.acl` + identity) the listing
        // yields only the requester's buckets — the contract-level
        // filter runs DURING the walk, before pagination, so the
        // continuation-token math applies to the filtered set (review
        // P2#4). The walk compares the EFFECTIVE owner: a row whose
        // owner wire is empty (the legacy shape, review B4) belongs to
        // the identity's default owner — the lazy-default leg, the
        // same resolution as `row_owner` — and matches only that
        // principal. Feature-off / toggle-off / no-identity modes keep
        // the unfiltered legacy listing (`None, None`).
        #[cfg(feature = "acl")]
        let (owner, lazy_default): (Option<acl::OwnerId>, Option<acl::OwnerId>) = match (
            self.caps.acl,
            self.identity.as_ref(),
        ) {
            // Fail-closed (fix-round Minor #2): the enforced filter
            // must never degrade to the unfiltered listing via an
            // unresolved principal — the identity is present here, so
            // the resolution cannot fail (the same invariant as the
            // write paths' `owner_for(...).expect`).
            (true, Some(identity)) => {
                let principal = self
                    .owner_for(req.credentials.as_ref())
                    .expect("identity attached above");
                (Some(principal), Some(identity.default_owner.clone()))
            }
            _ => (None, None),
        };
        #[cfg(not(feature = "acl"))]
        let (owner, lazy_default): (Option<acl::OwnerId>, Option<acl::OwnerId>) = (None, None);
        let listing = self
            .storage
            .list_buckets(
                ListBucketsParams {
                    prefix: prefix.clone().unwrap_or_default(),
                    start_after,
                    max_buckets: effective,
                },
                owner.as_ref(),
                lazy_default.as_ref(),
            )
            .await
            .map_err(map_backend_error)?;
        let buckets = listing
            .buckets
            .into_iter()
            .map(|b| Bucket {
                name: Some(String::from(b.name)),
                creation_date: Some(Self::last_modified(b.creation_time)),
                ..Default::default()
            })
            .collect();
        // `ContinuationToken` presence is the truncation signal (s3s
        // 0.15 has no `IsTruncated` on this wire); the engine returns
        // the resume marker only when truncated. `Prefix` is echoed iff
        // the client sent one (AWS). The output `Owner` element is the
        // filtered listing's single owner — the requester — display
        // resolved via the identity map; `None` on the unfiltered
        // legacy paths, gated identically to the filter above.
        #[cfg(feature = "acl")]
        let owner = owner.as_ref().map(|id| self.acl_owner(Some(id)));
        #[cfg(not(feature = "acl"))]
        let owner: Option<s3s::dto::Owner> = None;
        Ok(S3Response::new(ListBucketsOutput {
            buckets: Some(buckets),
            continuation_token: listing
                .next_start_after
                .map(|name| URL_SAFE_NO_PAD.encode(name.as_bytes())),
            owner,
            prefix,
        }))
    }

    pub(crate) async fn op_get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        // Existence is checked per AWS (a missing bucket → NoSuchBucket).
        let name = self.bucket(req.input.bucket)?;
        self.storage
            .head_bucket(&name)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(GetBucketLocationOutput {
            location_constraint: Some(BucketLocationConstraint::from("us-east-1".to_string())),
        }))
    }

    /// GetBucketTagging — the bucket's real tag set (spec 2026-08-31).
    /// A missing bucket answers 404 `NoSuchBucket` (AWS: only the delete
    /// is idempotent on the bucket's existence); a tag-less bucket
    /// answers the empty set.
    pub(crate) async fn op_get_bucket_tagging(
        &self,
        req: S3Request<GetBucketTaggingInput>,
    ) -> S3Result<S3Response<GetBucketTaggingOutput>> {
        Self::require_cap(self.caps.tagging, "GetBucketTagging")?;
        let bucket = self.bucket(req.input.bucket)?;
        let tags = self
            .storage
            .get_bucket_tags(&bucket)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(GetBucketTaggingOutput {
            tag_set: tag_set_from_tags(&tags),
        }))
    }

    /// PutBucketTagging — replace the bucket's tag set (replace-all, no
    /// merge). The dto `TagSet` is validated through the core type
    /// (duplicate keys, the ≤50 bucket cap → `InvalidTag` 400) before
    /// the contract call; a missing bucket answers 404 `NoSuchBucket`.
    /// No per-bucket lock (spec decision — AWS gives no atomicity
    /// guarantee between tag and data writes; last-writer-wins is
    /// accepted, as on the object surface).
    pub(crate) async fn op_put_bucket_tagging(
        &self,
        req: S3Request<PutBucketTaggingInput>,
    ) -> S3Result<S3Response<PutBucketTaggingOutput>> {
        Self::require_cap(self.caps.tagging, "PutBucketTagging")?;
        let bucket = self.bucket(req.input.bucket)?;
        let tags = tags_from_tag_set(&req.input.tagging.tag_set, object::BUCKET_TAGS_MAX)?;
        self.storage
            .put_bucket_tags(&bucket, &tags)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(PutBucketTaggingOutput::default()))
    }

    /// DeleteBucketTagging — remove the bucket's tag set. Idempotent:
    /// a missing bucket — and a bucket with no tag set — answers 204.
    pub(crate) async fn op_delete_bucket_tagging(
        &self,
        req: S3Request<DeleteBucketTaggingInput>,
    ) -> S3Result<S3Response<DeleteBucketTaggingOutput>> {
        Self::require_cap(self.caps.tagging, "DeleteBucketTagging")?;
        let bucket = self.bucket(req.input.bucket)?;
        self.storage
            .delete_bucket_tags(&bucket)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(DeleteBucketTaggingOutput::default()))
    }

    /// GetBucketAcl — the bucket's real ACL (spec 2026-09-05). A
    /// missing bucket answers 404 `NoSuchBucket` (the bucket-tags
    /// precedent). The Owner element resolves through the identity map
    /// (an unknown ID — the anonymous one included — is ID-only, AWS's
    /// unknown-account shape) and the grants echo the row's canonical
    /// set (the wire codec canonicalized what the writes stored).
    #[cfg(feature = "acl")]
    pub(crate) async fn op_get_bucket_acl(
        &self,
        req: S3Request<dto::GetBucketAclInput>,
    ) -> S3Result<S3Response<dto::GetBucketAclOutput>> {
        Self::require_cap(self.caps.acl, "GetBucketAcl")?;
        let bucket = self.bucket(req.input.bucket)?;
        let acl = self
            .storage
            .get_bucket_acl(&bucket)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::GetBucketAclOutput {
            owner: Some(self.acl_owner(acl.owner.as_ref())),
            grants: Some(grants_dto(&acl.grants)),
        }))
    }

    /// PutBucketAcl — replace the bucket's grant set (replace-all, no
    /// merge; the owner element is preserved by the store, never
    /// changed by a put — contract ruling). `Content-MD5` is present
    /// and well-formed (spec §5 — missing 400 `InvalidRequest` with the
    /// AWS message, malformed 400 `InvalidDigest`); exactly one grant
    /// source is given (canned ACL / grant headers / policy body — a
    /// combination or none answers 400 `InvalidArgument`); a policy
    /// body whose Owner does not match the row owner (the lazy default
    /// when the row records none — review B4) answers 400
    /// `InvalidArgument` (fail-closed). The grant set is validated
    /// strictly — the codec's self-heal is read-path only. A missing
    /// bucket answers 404 `NoSuchBucket`.
    #[cfg(feature = "acl")]
    pub(crate) async fn op_put_bucket_acl(
        &self,
        req: S3Request<dto::PutBucketAclInput>,
    ) -> S3Result<S3Response<dto::PutBucketAclOutput>> {
        Self::require_cap(self.caps.acl, "PutBucketAcl")?;
        require_content_md5(req.input.content_md5.as_deref())?;
        let bucket = self.bucket(req.input.bucket)?;
        // The ACL row read doubles as the existence check (NoSuchBucket
        // 404) and the owner the policy Owner must match.
        let row = self
            .storage
            .get_bucket_acl(&bucket)
            .await
            .map_err(map_backend_error)?;
        let owner = self.row_owner(row.owner.as_ref());
        let policy = req.input.access_control_policy.as_ref();
        if let Some(policy) = policy {
            policy_owner_matches(policy.owner.as_ref(), &owner)?;
        }
        let grants = bucket_acl_grants(
            req.input.acl.as_ref().map(|c| c.as_str()),
            &GrantHeaders {
                full_control: req.input.grant_full_control.as_deref(),
                read: req.input.grant_read.as_deref(),
                read_acp: req.input.grant_read_acp.as_deref(),
                write: req.input.grant_write.as_deref(),
                write_acp: req.input.grant_write_acp.as_deref(),
            },
            policy.and_then(|p| p.grants.as_deref()),
            &owner,
        )?;
        self.storage
            .put_bucket_acl(&bucket, &grants)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::PutBucketAclOutput::default()))
    }
}

#[cfg(test)]
mod tests {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use s3s::{S3, dto};
    use tokio::runtime::Runtime;

    use super::{
        CreateBucketInput, DeleteBucketInput, GetBucketLocationInput, HeadBucketInput,
        ListBucketsInput, ListBucketsOutput, *,
    };
    use crate::{
        _core::{
            acl, bucket,
            storage::{self, BucketOps, Error::NoSuchBucket, ObjectOps},
        },
        _mem::MemoryStorage,
        _util::testing::{assert_conformance, body},
        backend::{
            Capabilities,
            testutil::{s3_request, setup},
        },
    };
    #[cfg(feature = "acl")]
    use std::sync::Arc;
    #[cfg(feature = "acl")]
    use crate::_auth::identity::{Identity, User};
    #[cfg(feature = "acl")]
    use crate::backend::testutil::{acl_backend, credentials_for, signed_request, user_id};

    fn backend() -> S3Backend<MemoryStorage> {
        S3Backend::new(MemoryStorage::new().unwrap(), Default::default())
    }

    fn backend_with(caps: Capabilities) -> S3Backend<MemoryStorage> {
        S3Backend::new(MemoryStorage::new().unwrap(), caps)
    }

    /// A fresh backend with a `data` bucket created; returns its
    /// validated name.
    async fn setup_name() -> (S3Backend<MemoryStorage>, bucket::Name) {
        let (backend, b) = setup().await;
        (backend, bucket::name(b.as_str()).unwrap())
    }

    /// The URL-safe no-pad base64 of a bucket name — the continuation
    /// token a client would send back.
    fn token(name: &str) -> String {
        URL_SAFE_NO_PAD.encode(name.as_bytes())
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
            .create_bucket(s3_request(CreateBucketInput {
                bucket: "data".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(create.output.location.as_deref(), Some("/data"));

        // Duplicate create → BucketAlreadyExists.
        let err = backend
            .create_bucket(s3_request(CreateBucketInput {
                bucket: "data".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "BucketAlreadyOwnedByYou");

        // Head.
        backend
            .head_bucket(s3_request(HeadBucketInput {
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
            .get_bucket_location(s3_request(GetBucketLocationInput {
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
            .delete_bucket(s3_request(DeleteBucketInput {
                bucket: "data".into(),
                ..Default::default()
            }))
            .await
            .unwrap();

        // Missing bucket → NoSuchBucket.
        let err = backend
            .head_bucket(s3_request(HeadBucketInput {
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
            .create_bucket(s3_request(CreateBucketInput {
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
            .create_bucket(s3_request(CreateBucketInput {
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
            .delete_bucket(s3_request(DeleteBucketInput {
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

    #[tokio::test]
    async fn list_buckets_paginates_and_resumes() {
        let backend = backend();
        let storage = backend.storage();
        for name in ["zeta", "alpha-1", "alpha-2", "mid", "beta-1", "beta-2"] {
            storage
                .create_bucket(
                    &bucket::name(name).unwrap(),
                    None,
                    &acl::Acl::default_private(None),
                )
                .await
                .unwrap();
        }
        let names = |out: &ListBucketsOutput| {
            out.buckets
                .as_ref()
                .unwrap()
                .iter()
                .filter_map(|b| b.name.clone())
                .collect::<Vec<_>>()
        };
        let page1 = backend
            .list_buckets(s3_request(ListBucketsInput {
                max_buckets: Some(2),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(names(&page1), ["alpha-1", "alpha-2"]);
        assert_eq!(page1.prefix, None, "no prefix sent, none echoed");
        let t1 = page1.continuation_token.clone().unwrap();

        let page2 = backend
            .list_buckets(s3_request(ListBucketsInput {
                continuation_token: Some(t1),
                max_buckets: Some(2),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(names(&page2), ["beta-1", "beta-2"]);
        let t2 = page2.continuation_token.unwrap();

        let page3 = backend
            .list_buckets(s3_request(ListBucketsInput {
                continuation_token: Some(t2),
                max_buckets: Some(2),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(names(&page3), ["mid", "zeta"]);
        assert!(
            page3.continuation_token.is_none(),
            "the final page must carry no continuation token"
        );

        // Token exhaustion: a stale-but-decodable token past the end
        // yields an empty page (a plain start_after marker — no error).
        let exhausted = backend
            .list_buckets(s3_request(ListBucketsInput {
                continuation_token: Some(token("zzz")),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert!(names(&exhausted).is_empty());
        assert!(exhausted.continuation_token.is_none());
    }

    #[tokio::test]
    async fn list_buckets_prefix_filters_and_echoes() {
        let backend = backend();
        let storage = backend.storage();
        for name in ["alpha-1", "alpha-2", "beta-1"] {
            storage
                .create_bucket(
                    &bucket::name(name).unwrap(),
                    None,
                    &acl::Acl::default_private(None),
                )
                .await
                .unwrap();
        }
        let out = backend
            .list_buckets(s3_request(ListBucketsInput {
                prefix: Some("alpha".into()),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        let names: Vec<String> = out
            .buckets
            .unwrap()
            .into_iter()
            .filter_map(|b| b.name)
            .collect();
        assert_eq!(names, ["alpha-1", "alpha-2"]);
        assert_eq!(
            out.prefix.as_deref(),
            Some("alpha"),
            "the prefix is echoed when the client sent one"
        );
    }

    #[tokio::test]
    async fn list_buckets_ignores_bucket_region() {
        // `bucket-region` is accepted and ignored (single-region server —
        // GetBucketLocation answers us-east-1).
        let backend = backend();
        let storage = backend.storage();
        storage
            .create_bucket(
                &bucket::name("data").unwrap(),
                None,
                &acl::Acl::default_private(None),
            )
            .await
            .unwrap();
        let out = backend
            .list_buckets(s3_request(ListBucketsInput {
                bucket_region: Some("us-west-2".into()),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(out.buckets.as_ref().unwrap().len(), 1);
        assert!(out.continuation_token.is_none());
    }

    #[tokio::test]
    async fn list_buckets_rejects_page_size_below_one() {
        let backend = backend();
        for max in [0, -1] {
            let err = backend
                .list_buckets(s3_request(ListBucketsInput {
                    max_buckets: Some(max),
                    ..Default::default()
                }))
                .await
                .unwrap_err();
            assert_eq!(
                err.code().as_str(),
                "InvalidArgument",
                "max_buckets = {max}"
            );
        }
    }

    #[tokio::test]
    async fn list_buckets_stays_strict_under_the_zero_page_escape_hatch() {
        // The escape hatch restores the legacy empty page on the
        // pre-existing surfaces only; ListBuckets keeps the
        // AWS-documented 1..=10,000 validation regardless.
        let backend = backend_with(Capabilities {
            allow_zero_page_size: true,
            ..Default::default()
        });
        for max in [0, -1, 10_001] {
            let err = backend
                .list_buckets(s3_request(ListBucketsInput {
                    max_buckets: Some(max),
                    ..Default::default()
                }))
                .await
                .unwrap_err();
            assert_eq!(
                err.code().as_str(),
                "InvalidArgument",
                "max_buckets = {max}"
            );
        }
    }

    #[tokio::test]
    async fn list_buckets_clamps_to_the_configured_cap() {
        let backend = backend_with(Capabilities {
            max_buckets: 3,
            ..Default::default()
        });
        let storage = backend.storage();
        for name in ["zeta", "alpha-1", "alpha-2", "mid", "beta-1", "beta-2"] {
            storage
                .create_bucket(
                    &bucket::name(name).unwrap(),
                    None,
                    &acl::Acl::default_private(None),
                )
                .await
                .unwrap();
        }
        // A max-buckets = 10 request clamps to the cap (3), truncated.
        let out = backend
            .list_buckets(s3_request(ListBucketsInput {
                max_buckets: Some(10),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(out.buckets.as_ref().unwrap().len(), 3);
        assert!(out.continuation_token.is_some());
        // The no-parameter default (10,000) clamps to the cap too.
        let out = backend
            .list_buckets(s3_request(ListBucketsInput::default()))
            .await
            .unwrap()
            .output;
        assert_eq!(out.buckets.as_ref().unwrap().len(), 3);
        assert!(out.continuation_token.is_some());
    }

    #[tokio::test]
    async fn list_buckets_rejects_page_size_above_the_aws_ceiling() {
        // AWS documents max-buckets 1..=10,000: an out-of-range request
        // is InvalidArgument — never a silent clamp that would hand a
        // buggy client a ContinuationToken it did not ask for.
        let backend = backend();
        for max in [10_001, 50_000] {
            let err = backend
                .list_buckets(s3_request(ListBucketsInput {
                    max_buckets: Some(max),
                    ..Default::default()
                }))
                .await
                .unwrap_err();
            assert_eq!(
                err.code().as_str(),
                "InvalidArgument",
                "max_buckets = {max}"
            );
        }
        // The ceiling itself is legal.
        let storage = backend.storage();
        storage
            .create_bucket(
                &bucket::name("data").unwrap(),
                None,
                &acl::Acl::default_private(None),
            )
            .await
            .unwrap();
        let out = backend
            .list_buckets(s3_request(ListBucketsInput {
                max_buckets: Some(DEFAULT_MAX_BUCKETS),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        let names: Vec<String> = out
            .buckets
            .unwrap()
            .into_iter()
            .filter_map(|b| b.name)
            .collect();
        assert_eq!(names, ["data"]);
        assert!(out.continuation_token.is_none());
    }

    #[tokio::test]
    async fn list_buckets_rejects_an_overlong_continuation_token() {
        // T08: legitimate tokens are the base64 of a bucket name (≤63
        // ASCII chars — ≤84 token bytes); a token beyond the 256-byte
        // bound can only be malformed, so it is rejected before the
        // decode allocates proportional to its length.
        let backend = backend();
        let overlong = "a".repeat(300);
        let err = backend
            .list_buckets(s3_request(ListBucketsInput {
                continuation_token: Some(overlong),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidArgument");
        // The boundary itself is legal: a 256-byte token decodes to a
        // stale marker past the end — an empty page, never an error.
        // (`'A'` decodes to NUL bytes — valid UTF-8 — while `'b'` decodes
        // to 0xB6/0xDB, invalid UTF-8, which is correctly rejected.)
        let boundary = "A".repeat(256);
        let out = backend
            .list_buckets(s3_request(ListBucketsInput {
                continuation_token: Some(boundary),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert!(out.buckets.as_ref().unwrap().is_empty());
        assert!(out.continuation_token.is_none());
    }

    #[tokio::test]
    async fn list_buckets_default_page_size_is_ten_thousand() {
        // The AWS-documented default (10,000) applies when no
        // max-buckets is sent: 10,001 buckets yield a truncated page of
        // 10,000 plus a continuation token.
        let backend = backend();
        let storage = backend.storage();
        for i in 0..10_001 {
            storage
                .create_bucket(
                    &bucket::name(format!("b-{i}")).unwrap(),
                    None,
                    &acl::Acl::default_private(None),
                )
                .await
                .unwrap();
        }
        let out = backend
            .list_buckets(s3_request(ListBucketsInput::default()))
            .await
            .unwrap()
            .output;
        assert_eq!(out.buckets.as_ref().unwrap().len(), 10_000);
        assert!(out.continuation_token.is_some());
        // The token resumes exactly onto the remaining bucket.
        let out = backend
            .list_buckets(s3_request(ListBucketsInput {
                continuation_token: out.continuation_token,
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(out.buckets.as_ref().unwrap().len(), 1);
        assert!(out.continuation_token.is_none());
    }

    #[tokio::test]
    async fn list_buckets_rejects_bad_tokens() {
        let backend = backend();
        // Undecodable base64.
        let err = backend
            .list_buckets(s3_request(ListBucketsInput {
                continuation_token: Some("!!!not-base64!!!".into()),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidArgument");
        // Base64 of non-UTF-8 bytes.
        let raw = URL_SAFE_NO_PAD.encode([0xFF]);
        let err = backend
            .list_buckets(s3_request(ListBucketsInput {
                continuation_token: Some(raw),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidArgument");
    }

    #[tokio::test]
    async fn list_buckets_empty_token_is_a_noop() {
        // The empty token decodes to the empty marker — it skips nothing.
        let backend = backend();
        let storage = backend.storage();
        for name in ["alpha", "beta"] {
            storage
                .create_bucket(
                    &bucket::name(name).unwrap(),
                    None,
                    &acl::Acl::default_private(None),
                )
                .await
                .unwrap();
        }
        let out = backend
            .list_buckets(s3_request(ListBucketsInput {
                continuation_token: Some(String::new()),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        let names: Vec<String> = out
            .buckets
            .unwrap()
            .into_iter()
            .filter_map(|b| b.name)
            .collect();
        assert_eq!(names, ["alpha", "beta"]);
        // A stale-but-decodable token resumes like a start_after marker.
        let out = backend
            .list_buckets(s3_request(ListBucketsInput {
                continuation_token: Some(token("alpha")),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        let names: Vec<String> = out
            .buckets
            .unwrap()
            .into_iter()
            .filter_map(|b| b.name)
            .collect();
        assert_eq!(names, ["beta"]);
    }

    #[cfg(feature = "acl")]
    #[tokio::test]
    async fn list_buckets_filters_by_requester() {
        // The enforced-mode owner filter (spec 2026-09-05): the
        // contract-level filter runs during the walk, before pagination
        // (review P2#4), so the continuation-token math applies to the
        // filtered set. Each principal sees only the buckets whose
        // EFFECTIVE owner is theirs — the recorded owner, or the
        // identity's default owner for a legacy row (empty owner wire,
        // review B4, the lazy-default leg): the default principal's
        // listing includes the legacy row, a non-default principal's
        // does not. The output Owner element is the requester's, display
        // resolved via the identity map.
        let alice = user_id("AKID");
        let bob = user_id("BKID");
        // The fixture identity with alice AS the default owner — the
        // lazy-default leg's principal (a plain `acl_backend()` default
        // owner has no signed credentials of its own).
        let backend = S3Backend::new(MemoryStorage::new().unwrap(), Default::default())
            .with_identity(Arc::new(Identity {
                users: vec![
                    User::test("AKID", "secret", "alice"),
                    User::test("BKID", "secret", "bob"),
                ]
                .into_iter()
                .map(|u| (u.access_key.clone(), u))
                .collect(),
                default_owner: alice.clone(),
                default_display_name: "tinio".into(),
            }));
        let storage = backend.storage();
        for (bn, owner) in [
            ("alpha-a", Some(&alice)),
            ("alpha-b", Some(&alice)),
            ("beta-b", Some(&bob)),
        ] {
            let owner = owner.cloned();
            storage
                .create_bucket(
                    &bucket::name(bn).unwrap(),
                    owner.as_ref(),
                    &acl::Acl::default_private(owner.clone()),
                )
                .await
                .unwrap();
        }
        // A legacy-shape bucket: no owner element ever recorded — it
        // belongs to the default principal (alice here).
        storage
            .create_bucket(
                &bucket::name("legacy").unwrap(),
                None,
                &acl::Acl::default_private(None),
            )
            .await
            .unwrap();
        async fn list_owned(
            backend: &S3Backend<MemoryStorage>,
            access_key: &str,
            max_buckets: Option<i32>,
            continuation_token: Option<String>,
        ) -> ListBucketsOutput {
            let mut req = s3_request(ListBucketsInput {
                max_buckets,
                continuation_token,
                ..Default::default()
            });
            req.credentials = Some(credentials_for(access_key));
            backend.list_buckets(req).await.unwrap().output
        }
        let alice_list = list_owned(&backend, "AKID", None, None).await;
        let names: Vec<&str> = alice_list
            .buckets
            .as_ref()
            .unwrap()
            .iter()
            .filter_map(|b| b.name.as_deref())
            .collect();
        assert_eq!(names, ["alpha-a", "alpha-b", "legacy"]);
        let owner = alice_list.owner.as_ref().unwrap();
        assert_eq!(owner.id.as_deref(), Some(alice.as_str()));
        assert_eq!(owner.display_name.as_deref(), Some("alice"));
        let bob_list = list_owned(&backend, "BKID", None, None).await;
        let names: Vec<&str> = bob_list
            .buckets
            .as_ref()
            .unwrap()
            .iter()
            .filter_map(|b| b.name.as_deref())
            .collect();
        assert_eq!(names, ["beta-b"]);
        assert_eq!(
            bob_list.owner.as_ref().unwrap().display_name.as_deref(),
            Some("bob")
        );
        // The continuation tokens count the FILTERED set: a page of 1
        // over alice's walk yields exactly her three buckets across
        // three pages — bob's bucket never leaks in, and the legacy
        // row pages like any other of hers.
        let page1 = list_owned(&backend, "AKID", Some(1), None).await;
        assert_eq!(page1.buckets.as_ref().unwrap().len(), 1);
        let resume = page1.continuation_token.clone();
        assert!(
            resume.is_some(),
            "a truncated page must carry a resume marker"
        );
        let page2 = list_owned(&backend, "AKID", Some(1), resume).await;
        assert_eq!(page2.buckets.as_ref().unwrap().len(), 1);
        assert_eq!(
            page2.buckets.as_ref().unwrap()[0].name.as_deref(),
            Some("alpha-b")
        );
        let page3 = list_owned(&backend, "AKID", Some(1), page2.continuation_token.clone()).await;
        assert_eq!(page3.buckets.as_ref().unwrap().len(), 1);
        assert_eq!(
            page3.buckets.as_ref().unwrap()[0].name.as_deref(),
            Some("legacy")
        );
        assert!(page3.continuation_token.is_none());
    }

    #[cfg(feature = "acl")]
    #[tokio::test]
    async fn list_buckets_toggle_off_keeps_the_unfiltered_listing() {
        // The toggle-off mode (accept-and-drop): `caps.acl = false`
        // keeps the unfiltered legacy listing — every bucket, no Owner
        // element — even with the identity attached.
        let backend = S3Backend::new(
            MemoryStorage::new().unwrap(),
            Capabilities {
                acl: false,
                ..Default::default()
            },
        )
        .with_identity(crate::backend::testutil::acl_identity());
        let storage = backend.storage();
        let alice = user_id("AKID");
        storage
            .create_bucket(
                &bucket::name("alpha").unwrap(),
                Some(&alice),
                &acl::Acl::default_private(Some(alice.clone())),
            )
            .await
            .unwrap();
        storage
            .create_bucket(
                &bucket::name("beta").unwrap(),
                None,
                &acl::Acl::default_private(None),
            )
            .await
            .unwrap();
        let mut req = s3_request(ListBucketsInput::default());
        req.credentials = Some(credentials_for("BKID"));
        let out = backend.list_buckets(req).await.unwrap().output;
        let names: Vec<&str> = out
            .buckets
            .as_ref()
            .unwrap()
            .iter()
            .filter_map(|b| b.name.as_deref())
            .collect();
        assert_eq!(names, ["alpha", "beta"]);
        assert!(
            out.owner.is_none(),
            "the toggle-off listing carries no Owner element"
        );
    }

    #[tokio::test]
    async fn bucket_tagging_ops_round_trip() {
        let (backend, b) = setup_name().await;
        let tags = vec![dto::Tag {
            key: Some("team".into()),
            value: Some("core".into()),
        }];
        backend
            .put_bucket_tagging(s3_request(dto::PutBucketTaggingInput {
                bucket: b.to_string(),
                tagging: dto::Tagging {
                    tag_set: tags.clone(),
                },
                checksum_algorithm: None,
                content_md5: None,
                expected_bucket_owner: None,
            }))
            .await
            .unwrap();
        let got = backend
            .get_bucket_tagging(s3_request(dto::GetBucketTaggingInput {
                bucket: b.to_string(),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(got.output.tag_set, tags);
        backend
            .delete_bucket_tagging(s3_request(dto::DeleteBucketTaggingInput {
                bucket: b.to_string(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let got = backend
            .get_bucket_tagging(s3_request(dto::GetBucketTaggingInput {
                bucket: b.to_string(),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert!(got.output.tag_set.is_empty());
    }

    #[tokio::test]
    async fn bucket_tagging_ops_missing_bucket_semantics() {
        // get/put on a missing bucket answer NoSuchBucket (AWS); delete
        // is idempotent — a missing bucket answers success.
        let backend = backend();
        let err = backend
            .get_bucket_tagging(s3_request(dto::GetBucketTaggingInput {
                bucket: "ghost".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchBucket");
        let err = backend
            .put_bucket_tagging(s3_request(dto::PutBucketTaggingInput {
                bucket: "ghost".into(),
                tagging: dto::Tagging { tag_set: vec![] },
                checksum_algorithm: None,
                content_md5: None,
                expected_bucket_owner: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchBucket");
        backend
            .delete_bucket_tagging(s3_request(dto::DeleteBucketTaggingInput {
                bucket: "ghost".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn put_bucket_tagging_validation_rejects_bad_sets() {
        let (backend, b) = setup_name().await;
        let put = |tag_set: Vec<dto::Tag>| {
            backend.put_bucket_tagging(s3_request(dto::PutBucketTaggingInput {
                bucket: b.to_string(),
                tagging: dto::Tagging { tag_set },
                checksum_algorithm: None,
                content_md5: None,
                expected_bucket_owner: None,
            }))
        };
        // Duplicate keys → InvalidTag.
        let err = put(vec![
            dto::Tag {
                key: Some("k".into()),
                value: Some("1".into()),
            },
            dto::Tag {
                key: Some("k".into()),
                value: Some("2".into()),
            },
        ])
        .await
        .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidTag");
        // Past the 50-tag bucket cap → InvalidTag; the boundary itself
        // (50) is accepted (AWS-verified per-surface limit).
        let over: Vec<dto::Tag> = (0..51)
            .map(|i| dto::Tag {
                key: Some(format!("k-{i}")),
                value: Some("v".into()),
            })
            .collect();
        let err = put(over).await.unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidTag");
        let at_cap: Vec<dto::Tag> = (0..50)
            .map(|i| dto::Tag {
                key: Some(format!("k-{i}")),
                value: Some("v".into()),
            })
            .collect();
        put(at_cap).await.unwrap();
    }

    #[tokio::test]
    async fn tagging_toggle_off_gates_the_bucket_tagging_ops() {
        // The tagging toggle off answers NotImplemented (FR-021). The
        // gate fires before the bucket check — no bucket is needed.
        let backend = backend_with(Capabilities {
            tagging: false,
            ..Default::default()
        });
        let err = backend
            .get_bucket_tagging(s3_request(dto::GetBucketTaggingInput {
                bucket: "data".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented");
        let err = backend
            .put_bucket_tagging(s3_request(dto::PutBucketTaggingInput {
                bucket: "data".into(),
                tagging: dto::Tagging { tag_set: vec![] },
                checksum_algorithm: None,
                content_md5: None,
                expected_bucket_owner: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented");
        let err = backend
            .delete_bucket_tagging(s3_request(dto::DeleteBucketTaggingInput {
                bucket: "data".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented");
    }

    // --- bucket ACL ops (spec 2026-09-05) ---

    /// A well-formed Content-MD5 value (base64 of 16 zero bytes).
    #[cfg(feature = "acl")]
    fn valid_md5() -> String {
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [0u8; 16])
    }

    #[cfg(feature = "acl")]
    fn canonical_grantee(id: &str) -> dto::Grantee {
        dto::Grantee {
            type_: dto::Type::from_static(dto::Type::CANONICAL_USER),
            id: Some(id.into()),
            uri: None,
            display_name: None,
            email_address: None,
        }
    }

    #[cfg(feature = "acl")]
    fn group_grantee(uri: &str) -> dto::Grantee {
        dto::Grantee {
            type_: dto::Type::from_static(dto::Type::GROUP),
            uri: Some(uri.into()),
            id: None,
            display_name: None,
            email_address: None,
        }
    }

    #[cfg(feature = "acl")]
    fn grant(grantee: dto::Grantee, permission: &'static str) -> dto::Grant {
        dto::Grant {
            grantee: Some(grantee),
            permission: Some(dto::Permission::from_static(permission)),
        }
    }

    #[tokio::test]
    #[cfg(feature = "acl")]
    async fn bucket_acl_ops_round_trip() {
        let (backend, b) = setup_name().await;
        // The canned surface: `public-read` expands to the owner's
        // FULL_CONTROL plus AllUsers READ (the child-owner canned ACLs
        // are ignored on buckets, per AWS — the lazy default owner is
        // both owner and bucket owner here).
        backend
            .put_bucket_acl(s3_request(dto::PutBucketAclInput {
                bucket: b.to_string(),
                acl: Some(dto::BucketCannedACL::from_static("public-read")),
                content_md5: Some(valid_md5()),
                ..Default::default()
            }))
            .await
            .unwrap();
        let out = backend
            .get_bucket_acl(s3_request(dto::GetBucketAclInput {
                bucket: b.to_string(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        let default_id = acl::default_owner_id();
        let owner = out.owner.unwrap();
        assert_eq!(owner.id.as_deref(), Some(default_id.as_str()));
        assert_eq!(
            owner.display_name.as_deref(),
            Some(acl::DEFAULT_OWNER_DISPLAY_NAME)
        );
        assert_eq!(
            out.grants.as_deref(),
            Some(
                [
                    grant(
                        canonical_grantee(default_id.as_str()),
                        dto::Permission::FULL_CONTROL,
                    ),
                    grant(group_grantee(acl::GROUP_ALL_USERS), dto::Permission::READ),
                ]
                .as_slice()
            )
        );
        // The body-policy surface replaces the set (the lazy default
        // owner's ID as the policy Owner — what GET just echoed).
        let policy = dto::AccessControlPolicy {
            owner: Some(dto::Owner {
                id: Some(default_id.as_str().to_string()),
                display_name: None,
            }),
            grants: Some(vec![grant(
                group_grantee(acl::GROUP_ALL_USERS),
                dto::Permission::READ,
            )]),
        };
        backend
            .put_bucket_acl(s3_request(dto::PutBucketAclInput {
                bucket: b.to_string(),
                access_control_policy: Some(policy),
                content_md5: Some(valid_md5()),
                ..Default::default()
            }))
            .await
            .unwrap();
        let out = backend
            .get_bucket_acl(s3_request(dto::GetBucketAclInput {
                bucket: b.to_string(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(
            out.grants.as_deref(),
            Some([grant(group_grantee(acl::GROUP_ALL_USERS), dto::Permission::READ)].as_slice())
        );
    }

    #[tokio::test]
    #[cfg(feature = "acl")]
    async fn bucket_acl_ops_missing_bucket_semantics() {
        let backend = backend();
        let err = backend
            .get_bucket_acl(s3_request(dto::GetBucketAclInput {
                bucket: "ghost".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchBucket");
        let err = backend
            .put_bucket_acl(s3_request(dto::PutBucketAclInput {
                bucket: "ghost".into(),
                access_control_policy: Some(dto::AccessControlPolicy::default()),
                content_md5: Some(valid_md5()),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchBucket");
    }

    #[tokio::test]
    #[cfg(feature = "acl")]
    async fn put_bucket_acl_rejects_missing_or_mixed_grant_sources() {
        let (backend, b) = setup_name().await;
        // No ACL source at all → InvalidArgument (AWS: exactly one of
        // canned, grant headers, or a body).
        let err = backend
            .put_bucket_acl(s3_request(dto::PutBucketAclInput {
                bucket: b.to_string(),
                content_md5: Some(valid_md5()),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidArgument");
        // Canned + grant headers together → InvalidArgument (the
        // write-path conflict rule).
        let err = backend
            .put_bucket_acl(s3_request(dto::PutBucketAclInput {
                bucket: b.to_string(),
                acl: Some(dto::BucketCannedACL::from_static("public-read")),
                grant_read: Some(r#"id="aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899""#.into()),
                content_md5: Some(valid_md5()),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidArgument");
        // Canned + body together → the same conflict.
        let err = backend
            .put_bucket_acl(s3_request(dto::PutBucketAclInput {
                bucket: b.to_string(),
                acl: Some(dto::BucketCannedACL::from_static("public-read")),
                access_control_policy: Some(dto::AccessControlPolicy::default()),
                content_md5: Some(valid_md5()),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidArgument");
    }

    // --- write-path ACL headers (Task 11) ---

    #[tokio::test]
    #[cfg(feature = "acl")]
    async fn create_bucket_with_canned_acl_public_read() {
        // CreateBucket + `x-amz-acl`: the write records the canned
        // expansion and the requester's owner. The composition goes
        // through `canned_bucket_grants` DIRECTLY — `expand_acl` is
        // object-flavored, and `bucket-owner-read`/`bucket-owner-full-
        // control` are ignored on buckets (AWS).
        let backend = acl_backend();
        backend
            .create_bucket(signed_request(CreateBucketInput {
                bucket: "data".into(),
                acl: Some(dto::BucketCannedACL::from_static("public-read")),
                ..Default::default()
            }))
            .await
            .unwrap();
        let row = backend
            .storage()
            .get_bucket_acl(&bucket::name("data").unwrap())
            .await
            .unwrap();
        assert_eq!(
            row.owner.as_ref().map(|o| o.as_str()).unwrap(),
            user_id("AKID").as_str()
        );
        assert_eq!(
            row.grants,
            vec![
                acl::Grant {
                    grantee: acl::Grantee::Canonical(user_id("AKID")),
                    permission: acl::Permission::FullControl,
                },
                acl::Grant {
                    grantee: acl::Grantee::Group(acl::GroupUri(acl::GROUP_ALL_USERS.into())),
                    permission: acl::Permission::Read,
                },
            ]
        );
        // The bucket-owner-* names on CreateBucket are private (AWS) —
        // a composition through `expand_acl` would emit the
        // bucket-owner grant.
        backend
            .create_bucket(signed_request(CreateBucketInput {
                bucket: "other".into(),
                acl: Some(dto::BucketCannedACL::from_static("bucket-owner-read")),
                ..Default::default()
            }))
            .await
            .unwrap();
        let row = backend
            .storage()
            .get_bucket_acl(&bucket::name("other").unwrap())
            .await
            .unwrap();
        assert_eq!(
            row.grants,
            vec![acl::Grant {
                grantee: acl::Grantee::Canonical(user_id("AKID")),
                permission: acl::Permission::FullControl,
            }]
        );
    }

    #[tokio::test]
    #[cfg(feature = "acl")]
    async fn bucket_acl_owner_resolves_through_identity() {
        use std::sync::Arc;

        use crate::_auth::identity::{Identity, User};
        // A recorded row owner: the identity map's user, with its
        // display name resolved on the response.
        let user = User::test("AKID", "secret", "user1");
        let canonical_id = user.canonical_id.clone();
        let identity = Arc::new(Identity::test(vec![user]));
        let backend = S3Backend::new(MemoryStorage::new().unwrap(), Default::default())
            .with_identity(identity);
        backend
            .storage()
            .create_bucket(
                &bucket::name("data").unwrap(),
                Some(&canonical_id),
                &acl::Acl::default_private(Some(canonical_id.clone())),
            )
            .await
            .unwrap();
        let out = backend
            .get_bucket_acl(s3_request(dto::GetBucketAclInput {
                bucket: "data".into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        let owner = out.owner.unwrap();
        assert_eq!(owner.id.as_deref(), Some(canonical_id.as_str()));
        assert_eq!(owner.display_name.as_deref(), Some("user1"));
        // An unknown row owner resolves ID-only (AWS's unknown-account
        // shape — no display name).
        let unknown = acl::OwnerId::new(
            "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
        )
        .unwrap();
        backend
            .storage()
            .create_bucket(
                &bucket::name("other").unwrap(),
                Some(&unknown),
                &acl::Acl::default_private(Some(unknown.clone())),
            )
            .await
            .unwrap();
        let out = backend
            .get_bucket_acl(s3_request(dto::GetBucketAclInput {
                bucket: "other".into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        let owner = out.owner.unwrap();
        assert_eq!(owner.id.as_deref(), Some(unknown.as_str()));
        assert_eq!(owner.display_name, None);
    }

    #[tokio::test]
    #[cfg(feature = "acl")]
    async fn bucket_acl_toggle_off_gates_the_ops() {
        let backend = backend_with(Capabilities {
            acl: false,
            ..Default::default()
        });
        let err = backend
            .get_bucket_acl(s3_request(dto::GetBucketAclInput {
                bucket: "data".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented");
        let err = backend
            .put_bucket_acl(s3_request(dto::PutBucketAclInput {
                bucket: "data".into(),
                access_control_policy: Some(dto::AccessControlPolicy::default()),
                content_md5: Some(valid_md5()),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented");
    }
}
