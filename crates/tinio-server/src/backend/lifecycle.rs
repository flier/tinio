//! Bucket lifecycle configuration operations of the S3 mapping layer
//! (design 2026-10-18, LC7-LC9, LC18).
//!
//! GetBucketLifecycleConfiguration/PutBucketLifecycleConfiguration/
//! DeleteBucketLifecycle over the storage contract, runtime-gated on
//! `caps.lifecycle` by [`CapabilityS3`](crate::capability::CapabilityS3)
//! (feature-off builds omit the forwards entirely — s3s's trait defaults
//! answer "is not implemented yet"). The wire is `/?lifecycle` (LC7): a
//! `PUT` answers `200` with an empty body and replaces the whole
//! configuration, a `GET` with nothing stored answers
//! `404 NoSuchLifecycleConfiguration`, a `DELETE` answers `204` and is
//! idempotent. Read-only mode needs nothing here: the two writes are write
//! rows of the operation table, so the decorator refuses them (LC18).
//!
//! **Validation is the request path's, never the read path's** (LC8, the CORS
//! rule). The `PUT` maps the dto onto the stored model, then asks
//! [`Config::validate`](crate::_core::lifecycle::Config::validate), which
//! answers its own class: a schema violation is `MalformedXML`, a semantic
//! one `InvalidRequest` naming the offending rule's id.
//!
//! **Three refusals are the dto's, not the model's**, and they come first,
//! in this order within each rule:
//!
//! 1. a transition this backend cannot honour (LC9, T11): a
//!    `NoncurrentVersionTransition` anywhere, and a `Transition` on a
//!    backend whose [`Storage::supports_tiers`] is `false` — accepting one
//!    would promise a migration that cannot happen. A tier-capable backend
//!    maps `STANDARD_IA` into the stored model and back;
//! 2. a `Status` the model has no variant for (neither `Enabled` nor
//!    `Disabled`);
//! 3. a rule with both a `Filter` and a legacy rule-level `Prefix` — the
//!    same predicate twice. A rule with neither is every object, the empty
//!    `Filter` the model already stores (LC8).
//!
//! Ordering the transition first is deliberate: a document that carries one
//! is told what this server cannot do, rather than that something else
//! about the same rule is malformed. The message is
//! [`TRANSITIONS_NEED_TIERS`](crate::_core::lifecycle::TRANSITIONS_NEED_TIERS),
//! the one sentence every write path refuses with.

use http::HeaderMap;
use s3s::{S3Error, S3Request, S3Response, S3Result, dto, s3_error};

use crate::{
    _core::{
        Class,
        lifecycle::{
            AbortIncompleteMultipartUpload, And, Config, Expiration, Filter,
            NoncurrentVersionExpiration, Rule, Status, TRANSITIONS_NEED_TIERS, Tag, Transition,
        },
        storage::Storage,
    },
    backend::{S3Backend, errors::invalid_error, map_backend_error},
};

impl<S: Storage> S3Backend<S> {
    pub(crate) async fn op_get_bucket_lifecycle(
        &self,
        req: S3Request<dto::GetBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<dto::GetBucketLifecycleConfigurationOutput>> {
        let bucket = self.bucket(req.input.bucket)?;
        match self
            .storage
            .get_bucket_lifecycle(&bucket)
            .await
            .map_err(map_backend_error)?
        {
            Some(config) => Ok(S3Response::new(
                dto::GetBucketLifecycleConfigurationOutput {
                    rules: Some(config.rules.into_iter().map(rule_to_dto).collect()),
                    transition_default_minimum_object_size: None,
                },
            )),
            None => Err(s3_error!(NoSuchLifecycleConfiguration)),
        }
    }

    pub(crate) async fn op_put_bucket_lifecycle(
        &self,
        req: S3Request<dto::PutBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<dto::PutBucketLifecycleConfigurationOutput>> {
        // The bucket, then the checksum — the CORS put's order: a name the
        // protocol cannot carry is refused before the request's content is
        // looked at.
        let dto::PutBucketLifecycleConfigurationInput {
            bucket,
            lifecycle_configuration,
            ..
        } = req.input;
        let bucket = self.bucket(bucket)?;
        require_checksum(&req.headers, req.trailing_headers.is_some())?;
        let config = config_from_dto(lifecycle_configuration, self.storage.supports_tiers())?;
        config.validate().map_err(|invalid| {
            invalid_error(invalid.kind, invalid.reason, invalid.rule.as_deref())
        })?;
        self.storage
            .put_bucket_lifecycle(&bucket, &config)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(
            dto::PutBucketLifecycleConfigurationOutput::default(),
        ))
    }

    pub(crate) async fn op_delete_bucket_lifecycle(
        &self,
        req: S3Request<dto::DeleteBucketLifecycleInput>,
    ) -> S3Result<S3Response<dto::DeleteBucketLifecycleOutput>> {
        let bucket = self.bucket(req.input.bucket)?;
        self.storage
            .delete_bucket_lifecycle(&bucket)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::DeleteBucketLifecycleOutput::default()))
    }
}

/// The checksum a `PUT` requires (LC8): `Content-MD5`, or any
/// `x-amz-checksum-*` header, trailer or parsed trailing header.
///
/// `Content-MD5` is form-checked only — 16-byte base64 — because equality
/// against the body is unverifiable here: s3s's XML deserializer consumes the
/// body before the handler runs and `S3Request` retains no raw bytes, so the
/// mismatch arm (AWS's `BadDigest`) is unreachable in this framework version.
/// `cors.rs` and `acls.rs` carry the same recorded deviation. Any other
/// algorithm's value is not compared either, for the same reason; a truncated
/// or garbled body fails the XML parse before the digest could matter.
fn require_checksum(headers: &HeaderMap, has_trailing_headers: bool) -> S3Result<()> {
    super::checksum::require_any_checksum(headers, has_trailing_headers)
}

/// A schema refusal, naming the rule when it has an id.
fn malformed(id: Option<&str>, reason: &str) -> S3Error {
    match id {
        Some(id) => s3_error!(MalformedXML, "lifecycle rule {id}: {reason}"),
        None => s3_error!(MalformedXML, "{reason}"),
    }
}

/// A semantic refusal, naming the rule when it has an id.
fn invalid_request(id: Option<&str>, reason: &str) -> S3Error {
    match id {
        Some(id) => s3_error!(InvalidRequest, "lifecycle rule {id}: {reason}"),
        None => s3_error!(InvalidRequest, "{reason}"),
    }
}

/// The reason a noncurrent-version transition is refused even on a tier backend (T11): this
/// build transitions current versions only.
const NONCURRENT_TRANSITION: &str =
    "noncurrent version transitions are not supported by this server";

// --- dto to the stored model ---

/// The request body as the stored model. Three refusals are the dto's own and
/// happen here, before [`Config::validate`] sees the document: the
/// transitions this backend cannot honour (LC9, T11), a `Status` outside
/// `Enabled`/`Disabled`, and the filter/legacy-`Prefix` shape (see the module
/// docs for the order and why).
fn config_from_dto(
    xml: Option<dto::BucketLifecycleConfiguration>,
    supports_tiers: bool,
) -> S3Result<Config> {
    let Some(xml) = xml else {
        return Err(malformed(
            None,
            "the request carries no lifecycle configuration",
        ));
    };
    let rules = xml
        .rules
        .into_iter()
        .map(|rule| rule_from_dto(rule, supports_tiers))
        .collect::<S3Result<Vec<_>>>()?;
    Ok(Config { rules })
}

fn rule_from_dto(mut rule: dto::LifecycleRule, supports_tiers: bool) -> S3Result<Rule> {
    let id = rule.id.take();
    // A backend without tiers keeps today's refusal for either transition
    // spelling. A tier backend honours a current-version transition but still
    // refuses the noncurrent one: this build has no model for it (T11).
    if !supports_tiers
        && (rule.transitions.is_some() || rule.noncurrent_version_transitions.is_some())
    {
        return Err(invalid_request(id.as_deref(), TRANSITIONS_NEED_TIERS));
    }
    if rule.noncurrent_version_transitions.is_some() {
        return Err(invalid_request(id.as_deref(), NONCURRENT_TRANSITION));
    }
    let status = match rule.status.as_str() {
        dto::ExpirationStatus::ENABLED => Status::Enabled,
        dto::ExpirationStatus::DISABLED => Status::Disabled,
        other => {
            return Err(invalid_request(
                id.as_deref(),
                &format!("Status is Enabled or Disabled, not {other}"),
            ));
        }
    };
    let filter = filter_from_dto(rule.filter, rule.prefix.as_deref(), id.as_deref())?;
    let abort_incomplete_multipart_upload = rule
        .abort_incomplete_multipart_upload
        .map(|abort| abort_from_dto(abort, id.as_deref()))
        .transpose()?;
    Ok(Rule {
        id,
        status,
        filter,
        expiration: rule.expiration.map(expiration_from_dto),
        transitions: rule
            .transitions
            .unwrap_or_default()
            .into_iter()
            .map(transition_from_dto)
            .collect(),
        noncurrent_version_expiration: rule.noncurrent_version_expiration.map(noncurrent_from_dto),
        abort_incomplete_multipart_upload,
    })
}

/// One wire transition as the stored model (T11).
///
/// The class maps through the model's own parser; a spelling this build does not accept lands on
/// `STANDARD`, which [`Config::validate`] refuses with `Transition StorageClass is
/// STANDARD_IA` — the semantic class an unknown target gets on the request path.
fn transition_from_dto(transition: dto::Transition) -> Transition {
    Transition {
        days: transition.days,
        date: transition
            .date
            .map(|date| time::OffsetDateTime::from(date).unix_timestamp()),
        storage_class: transition
            .storage_class
            .and_then(|class| class.as_str().parse::<Class>().ok())
            .unwrap_or_default(),
    }
}

/// The rule's selector: a `Filter`, or the legacy rule-level `Prefix` in its
/// place. Neither means every object (LC8). Both spell one predicate twice.
fn filter_from_dto(
    filter: Option<dto::LifecycleRuleFilter>,
    prefix: Option<&str>,
    id: Option<&str>,
) -> S3Result<Filter> {
    let filter = match (filter, prefix) {
        (Some(_), Some(_)) => {
            return Err(malformed(
                id,
                "Filter and the rule-level Prefix name the same predicate",
            ));
        }
        (None, None) => return Ok(Filter::default()),
        (None, Some(prefix)) => {
            return Ok(Filter {
                prefix: Some(prefix.to_owned()),
                ..Filter::default()
            });
        }
        (Some(filter), None) => filter,
    };
    Ok(Filter {
        prefix: filter.prefix,
        tag: filter.tag.map(|tag| tag_from_dto(tag, id)).transpose()?,
        size_greater_than: size_from_dto(filter.object_size_greater_than, id)?,
        size_less_than: size_from_dto(filter.object_size_less_than, id)?,
        and: filter.and.map(|and| and_from_dto(and, id)).transpose()?,
    })
}

fn and_from_dto(and: dto::LifecycleRuleAndOperator, id: Option<&str>) -> S3Result<And> {
    Ok(And {
        prefix: and.prefix,
        tags: and
            .tags
            .unwrap_or_default()
            .into_iter()
            .map(|tag| tag_from_dto(tag, id))
            .collect::<S3Result<Vec<_>>>()?,
        size_greater_than: size_from_dto(and.object_size_greater_than, id)?,
        size_less_than: size_from_dto(and.object_size_less_than, id)?,
    })
}

fn tag_from_dto(tag: dto::Tag, id: Option<&str>) -> S3Result<Tag> {
    match (tag.key, tag.value) {
        (Some(key), Some(value)) => Ok(Tag { key, value }),
        _ => Err(invalid_request(
            id,
            "a tag filter carries a Key and a Value",
        )),
    }
}

/// A size predicate is a non-negative count of bytes; the model carries it
/// unsigned (LC8's order and ceiling live in `validate`).
fn size_from_dto(size: Option<i64>, id: Option<&str>) -> S3Result<Option<u64>> {
    size.map(|size| {
        u64::try_from(size)
            .map_err(|_| invalid_request(id, "an object size is a non-negative number of bytes"))
    })
    .transpose()
}

fn expiration_from_dto(expiration: dto::LifecycleExpiration) -> Expiration {
    Expiration {
        days: expiration.days,
        date: expiration
            .date
            .map(|date| time::OffsetDateTime::from(date).unix_timestamp()),
        expired_object_delete_marker: expiration.expired_object_delete_marker,
    }
}

fn noncurrent_from_dto(
    expiration: dto::NoncurrentVersionExpiration,
) -> NoncurrentVersionExpiration {
    NoncurrentVersionExpiration {
        noncurrent_days: expiration.noncurrent_days,
        newer_noncurrent_versions: expiration.newer_noncurrent_versions,
    }
}

fn abort_from_dto(
    abort: dto::AbortIncompleteMultipartUpload,
    id: Option<&str>,
) -> S3Result<AbortIncompleteMultipartUpload> {
    abort
        .days_after_initiation
        .map(|days_after_initiation| AbortIncompleteMultipartUpload {
            days_after_initiation,
        })
        .ok_or_else(|| {
            invalid_request(
                id,
                "AbortIncompleteMultipartUpload carries DaysAfterInitiation",
            )
        })
}

// --- the stored model back to the dto ---

fn rule_to_dto(rule: Rule) -> dto::LifecycleRule {
    dto::LifecycleRule {
        abort_incomplete_multipart_upload: rule.abort_incomplete_multipart_upload.map(|abort| {
            dto::AbortIncompleteMultipartUpload {
                days_after_initiation: Some(abort.days_after_initiation),
            }
        }),
        expiration: rule.expiration.map(expiration_to_dto),
        filter: Some(filter_to_dto(rule.filter)),
        id: rule.id,
        noncurrent_version_expiration: rule.noncurrent_version_expiration.map(noncurrent_to_dto),
        noncurrent_version_transitions: None,
        // The legacy spelling is accepted on the way in and never emitted:
        // the filter carries the predicate.
        prefix: None,
        status: match rule.status {
            Status::Enabled => dto::ExpirationStatus::from_static(dto::ExpirationStatus::ENABLED),
            Status::Disabled => dto::ExpirationStatus::from_static(dto::ExpirationStatus::DISABLED),
        },
        // An empty list is absent on the wire (T11), so a `GET` body carries no `Transition`
        // member unless the stored rule has one.
        transitions: (!rule.transitions.is_empty()).then(|| {
            rule.transitions
                .into_iter()
                .map(transition_to_dto)
                .collect()
        }),
    }
}

/// One stored transition back on the wire (T11): the class's S3 spelling, and a `Date` the
/// framework's timestamp cannot hold is dropped for the same reason an oversized size is.
fn transition_to_dto(transition: Transition) -> dto::Transition {
    dto::Transition {
        date: transition
            .date
            .and_then(|date| time::OffsetDateTime::from_unix_timestamp(date).ok())
            .map(dto::Date::from),
        days: transition.days,
        storage_class: Some(dto::TransitionStorageClass::from(
            transition.storage_class.as_str().to_owned(),
        )),
    }
}

/// An empty filter is the model's "every object", and the dto's empty
/// `Filter` says the same thing (LC8), so it is emitted rather than omitted —
/// a `GET` body a client can `PUT` back unaltered.
fn filter_to_dto(filter: Filter) -> dto::LifecycleRuleFilter {
    dto::LifecycleRuleFilter {
        and: filter.and.map(and_to_dto),
        object_size_greater_than: size_to_dto(filter.size_greater_than),
        object_size_less_than: size_to_dto(filter.size_less_than),
        prefix: filter.prefix,
        tag: filter.tag.map(|tag| dto::Tag {
            key: Some(tag.key),
            value: Some(tag.value),
        }),
    }
}

fn and_to_dto(and: And) -> dto::LifecycleRuleAndOperator {
    dto::LifecycleRuleAndOperator {
        object_size_greater_than: size_to_dto(and.size_greater_than),
        object_size_less_than: size_to_dto(and.size_less_than),
        prefix: and.prefix,
        tags: (!and.tags.is_empty()).then(|| {
            and.tags
                .into_iter()
                .map(|tag| dto::Tag {
                    key: Some(tag.key),
                    value: Some(tag.value),
                })
                .collect()
        }),
    }
}

/// A size the wire cannot carry is dropped rather than truncated: only a
/// plane-written row can hold one (`validate` bounds a request's at 50 TB).
fn size_to_dto(size: Option<u64>) -> Option<i64> {
    size.and_then(|size| i64::try_from(size).ok())
}

fn expiration_to_dto(expiration: Expiration) -> dto::LifecycleExpiration {
    dto::LifecycleExpiration {
        // A `Date` the framework's timestamp cannot hold is not carried for
        // the same reason a size too large for the wire is not.
        date: expiration
            .date
            .and_then(|date| time::OffsetDateTime::from_unix_timestamp(date).ok())
            .map(dto::Date::from),
        days: expiration.days,
        expired_object_delete_marker: expiration.expired_object_delete_marker,
    }
}

fn noncurrent_to_dto(expiration: NoncurrentVersionExpiration) -> dto::NoncurrentVersionExpiration {
    dto::NoncurrentVersionExpiration {
        noncurrent_days: expiration.noncurrent_days,
        newer_noncurrent_versions: expiration.newer_noncurrent_versions,
    }
}

#[cfg(test)]
mod tests {
    use http::HeaderValue;
    use s3s::{S3, dto};

    use super::{NONCURRENT_TRANSITION, config_from_dto, rule_to_dto};
    use crate::{
        _core::{
            Class,
            lifecycle::{TRANSITIONS_NEED_TIERS, Transition},
        },
        backend::{
            Capabilities,
            checksum::CONTENT_MD5,
            testutil::{s3_request, setup, setup_gated_with_caps},
        },
    };

    /// A well-formed `Content-MD5`: base64 of 16 zero bytes. The value is
    /// only form-checked (see the handler's `require_checksum`), so any
    /// 16-byte digest passes.
    const VALID_MD5: &str = "AAAAAAAAAAAAAAAAAAAAAA==";

    /// A midnight UTC `Expiration` date (2023-11-14T00:00:00Z).
    const MIDNIGHT: i64 = 1_699_920_000;

    fn enabled() -> dto::ExpirationStatus {
        dto::ExpirationStatus::from_static(dto::ExpirationStatus::ENABLED)
    }

    fn disabled() -> dto::ExpirationStatus {
        dto::ExpirationStatus::from_static(dto::ExpirationStatus::DISABLED)
    }

    /// A rule whose only action is `Expiration.Days`, filtered by a prefix.
    fn days_rule(id: &str, prefix: &str, days: i32) -> dto::LifecycleRule {
        dto::LifecycleRule {
            abort_incomplete_multipart_upload: None,
            expiration: Some(dto::LifecycleExpiration {
                days: Some(days),
                date: None,
                expired_object_delete_marker: None,
            }),
            filter: Some(dto::LifecycleRuleFilter {
                prefix: Some(prefix.to_owned()),
                ..Default::default()
            }),
            id: Some(id.to_owned()),
            noncurrent_version_expiration: None,
            noncurrent_version_transitions: None,
            prefix: None,
            status: enabled(),
            transitions: None,
        }
    }

    /// A rule carrying the one transition this build accepts (T11): `STANDARD_IA`, whose
    /// `Days` sits on the AWS floor.
    fn transition_rule(id: &str, days: i32) -> dto::LifecycleRule {
        let mut rule = days_rule(id, "logs/", days);
        rule.transitions = Some(vec![dto::Transition {
            days: Some(days),
            storage_class: Some(dto::TransitionStorageClass::from_static(
                dto::TransitionStorageClass::STANDARD_IA,
            )),
            ..Default::default()
        }]);
        rule
    }

    /// A rule with no filter and no legacy prefix: every object.
    fn bare_rule(id: Option<&str>) -> dto::LifecycleRule {
        dto::LifecycleRule {
            abort_incomplete_multipart_upload: None,
            expiration: Some(dto::LifecycleExpiration {
                days: Some(30),
                date: None,
                expired_object_delete_marker: None,
            }),
            filter: None,
            id: id.map(str::to_owned),
            noncurrent_version_expiration: None,
            noncurrent_version_transitions: None,
            prefix: None,
            status: enabled(),
            transitions: None,
        }
    }

    fn configuration(rules: Vec<dto::LifecycleRule>) -> dto::BucketLifecycleConfiguration {
        dto::BucketLifecycleConfiguration { rules }
    }

    /// A `PUT` carrying a well-formed `Content-MD5`.
    fn put(
        bucket: &str,
        rules: Vec<dto::LifecycleRule>,
    ) -> s3s::S3Request<dto::PutBucketLifecycleConfigurationInput> {
        let mut req = s3_request(dto::PutBucketLifecycleConfigurationInput {
            bucket: bucket.to_owned(),
            lifecycle_configuration: Some(configuration(rules)),
            ..Default::default()
        });
        req.headers
            .insert(CONTENT_MD5, HeaderValue::from_static(VALID_MD5));
        req
    }

    fn get(bucket: &str) -> s3s::S3Request<dto::GetBucketLifecycleConfigurationInput> {
        s3_request(dto::GetBucketLifecycleConfigurationInput {
            bucket: bucket.to_owned(),
            ..Default::default()
        })
    }

    fn delete(bucket: &str) -> s3s::S3Request<dto::DeleteBucketLifecycleInput> {
        s3_request(dto::DeleteBucketLifecycleInput {
            bucket: bucket.to_owned(),
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn a_missing_configuration_answers_no_such_lifecycle_configuration() {
        let (backend, b) = setup().await;
        let err = backend
            .op_get_bucket_lifecycle(get(&b))
            .await
            .expect_err("a bucket with no configuration answers 404");
        assert_eq!(
            err.code().as_str(),
            "NoSuchLifecycleConfiguration",
            "{err:?}"
        );
        // `DELETE` is idempotent on a bucket that stores none (LC7).
        backend
            .op_delete_bucket_lifecycle(delete(&b))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_missing_content_md5_is_invalid_request() {
        let (backend, b) = setup().await;
        let mut req = put(&b, vec![days_rule("expire", "logs/", 30)]);
        req.headers.remove(CONTENT_MD5);
        let err = backend.op_put_bucket_lifecycle(req).await.unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequest", "{err:?}");
        assert!(
            err.message()
                .is_some_and(|m| m.contains("Missing required header for this request")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_malformed_content_md5_is_invalid_digest() {
        let (backend, b) = setup().await;
        for md5 in ["not-base64!", "aGVsbG8="] {
            let mut req = put(&b, vec![days_rule("expire", "logs/", 30)]);
            req.headers
                .insert(CONTENT_MD5, HeaderValue::from_str(md5).unwrap());
            let err = backend.op_put_bucket_lifecycle(req).await.unwrap_err();
            assert_eq!(err.code().as_str(), "InvalidDigest", "{md5}: {err:?}");
        }
        // NOTE: a digest that is well-formed but does not match the body
        // cannot be built as a request: s3s's deserializer consumes the body
        // before the handler runs and `S3Request` keeps no raw bytes, so the
        // mismatch arm (AWS's `BadDigest`) is unreachable in this framework
        // version — the same recorded deviation `cors.rs` and `acls.rs` carry.
    }

    #[tokio::test]
    async fn a_checksum_header_of_any_algorithm_is_accepted() {
        // LC8: a checksum is `Content-MD5` *or* any `x-amz-checksum-*`.
        let (backend, b) = setup().await;
        let mut req = put(&b, vec![days_rule("expire", "logs/", 30)]);
        req.headers.remove(CONTENT_MD5);
        req.headers
            .insert("x-amz-checksum-crc32", HeaderValue::from_static("AAAAAA=="));
        backend.op_put_bucket_lifecycle(req).await.unwrap();
    }

    #[tokio::test]
    async fn two_top_level_predicates_are_malformed_xml() {
        let (backend, b) = setup().await;
        let mut rule = days_rule("two", "logs/", 30);
        rule.filter = Some(dto::LifecycleRuleFilter {
            prefix: Some("logs/".to_owned()),
            object_size_greater_than: Some(500),
            ..Default::default()
        });
        let err = backend
            .op_put_bucket_lifecycle(put(&b, vec![rule]))
            .await
            .unwrap_err();
        // LC8: a document that does not match the schema is `MalformedXML`,
        // while every semantic violation is `InvalidRequest`.
        assert_eq!(err.code().as_str(), "MalformedXML", "{err:?}");
        assert!(err.message().is_some_and(|m| m.contains("two")), "{err:?}");
    }

    #[tokio::test]
    async fn an_absent_filter_is_every_object() {
        let (backend, b) = setup().await;
        let bare = bare_rule(Some("bare"));
        backend
            .op_put_bucket_lifecycle(put(&b, vec![bare.clone()]))
            .await
            .unwrap();
        let mut expected = bare;
        expected.filter = Some(dto::LifecycleRuleFilter::default());
        let got = backend.op_get_bucket_lifecycle(get(&b)).await.unwrap();
        assert_eq!(got.output.rules, Some(vec![expected]));

        // The legacy rule-level `Prefix` is the accepted spelling of the same
        // predicate — and only when no `Filter` is present.
        let mut legacy = bare_rule(Some("legacy"));
        legacy.prefix = Some("logs/".to_owned());
        backend
            .op_put_bucket_lifecycle(put(&b, vec![legacy.clone()]))
            .await
            .unwrap();
        let mut expected = legacy.clone();
        expected.filter = Some(dto::LifecycleRuleFilter {
            prefix: Some("logs/".to_owned()),
            ..Default::default()
        });
        // The wire's legacy spelling is never emitted twice: the filter
        // carries the predicate.
        expected.prefix = None;
        let got = backend.op_get_bucket_lifecycle(get(&b)).await.unwrap();
        assert_eq!(got.output.rules, Some(vec![expected]));

        let mut both = bare_rule(Some("both"));
        both.prefix = Some("logs/".to_owned());
        both.filter = Some(dto::LifecycleRuleFilter {
            prefix: Some("other/".to_owned()),
            ..Default::default()
        });
        let err = backend
            .op_put_bucket_lifecycle(put(&b, vec![both]))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "MalformedXML", "{err:?}");
    }

    #[tokio::test]
    async fn an_empty_filter_is_every_object() {
        let (backend, b) = setup().await;
        let mut rule = days_rule("everything", "logs/", 30);
        rule.filter = Some(dto::LifecycleRuleFilter::default());
        backend
            .op_put_bucket_lifecycle(put(&b, vec![rule]))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_duplicate_id_is_invalid_request() {
        let (backend, b) = setup().await;
        let err = backend
            .op_put_bucket_lifecycle(put(
                &b,
                vec![days_rule("dup", "logs/", 30), days_rule("dup", "tmp/", 60)],
            ))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequest", "{err:?}");
        assert!(
            err.message().is_some_and(|m| m.contains("dup")),
            "the refusal names the rule: {err:?}"
        );
    }

    #[tokio::test]
    async fn a_transition_is_invalid_request_naming_the_tiers() {
        let (backend, b) = setup().await;
        // LC9: tinio has no storage tiers, so a transition is refused rather
        // than silently stored as an action that will never happen.
        let mut transitioned = days_rule("archive", "logs/", 30);
        transitioned.transitions = Some(vec![dto::Transition {
            days: Some(90),
            ..Default::default()
        }]);
        let mut noncurrent = days_rule("noncurrent", "logs/", 30);
        noncurrent.noncurrent_version_transitions = Some(vec![dto::NoncurrentVersionTransition {
            noncurrent_days: Some(30),
            ..Default::default()
        }]);
        for rule in [transitioned, noncurrent] {
            let err = backend
                .op_put_bucket_lifecycle(put(&b, vec![rule]))
                .await
                .unwrap_err();
            assert_eq!(err.code().as_str(), "InvalidRequest", "{err:?}");
            assert!(
                err.message().is_some_and(|m| m.contains("storage tiers")),
                "{err:?}"
            );
        }

        // The refusal is the rule's own fault and is answered before the
        // model's: the same body with a schema violation beside the
        // transition still names the tiers.
        let mut shadowed = bare_rule(None);
        shadowed.transitions = Some(vec![dto::Transition::default()]);
        let err = backend
            .op_put_bucket_lifecycle(put(&b, vec![shadowed]))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequest", "{err:?}");
        assert!(
            err.message().is_some_and(|m| m.contains("storage tiers")),
            "the transition is named, not the shape violation: {err:?}"
        );
    }

    #[tokio::test]
    async fn a_non_tier_s3_put_refuses_transitions() {
        // T11: the tier gate covers the S3 surface. A backend without tiers keeps today's
        // refusal — the one sentence every write path uses — and stores nothing.
        let (backend, b) = setup().await;
        let err = backend
            .op_put_bucket_lifecycle(put(&b, vec![transition_rule("archive", 30)]))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequest", "{err:?}");
        assert!(
            err.message()
                .is_some_and(|message| message.contains(TRANSITIONS_NEED_TIERS)),
            "{err:?}"
        );
        assert_eq!(
            backend
                .op_get_bucket_lifecycle(get(&b))
                .await
                .unwrap_err()
                .code()
                .as_str(),
            "NoSuchLifecycleConfiguration",
            "the refused configuration was never stored"
        );
    }

    #[test]
    fn a_tier_surface_round_trips() {
        // T11: on a tier-capable backend the dto's `STANDARD_IA` reaches the stored model and
        // comes back for both ways of saying "when" — the `Days` floor and a midnight `Date` —
        // so a GET body a client can PUT again carries the class.
        let mut xml = transition_rule("archive", 30);
        xml.transitions
            .as_mut()
            .expect("the fixture carries a transition")
            .push(dto::Transition {
                date: Some(dto::Date::from(
                    time::OffsetDateTime::from_unix_timestamp(MIDNIGHT).expect("midnight UTC"),
                )),
                days: None,
                storage_class: Some(dto::TransitionStorageClass::from_static(
                    dto::TransitionStorageClass::STANDARD_IA,
                )),
            });
        let config = config_from_dto(Some(configuration(vec![xml.clone()])), true)
            .expect("a tier-capable backend accepts a STANDARD_IA transition");
        config
            .validate()
            .expect("the mapped configuration is one the model stores");
        assert_eq!(
            config.rules[0].transitions,
            vec![
                Transition {
                    days: Some(30),
                    date: None,
                    storage_class: Class::StandardIa,
                },
                Transition {
                    days: None,
                    date: Some(MIDNIGHT),
                    storage_class: Class::StandardIa,
                },
            ]
        );
        let wire = rule_to_dto(config.rules[0].clone());
        assert_eq!(
            wire.transitions, xml.transitions,
            "both spellings round-trip"
        );

        // A noncurrent transition has no model at all, so it stays refused even where tiers
        // exist: the tier answer widens only the current-version gate (T11).
        let mut noncurrent = days_rule("noncurrent", "logs/", 30);
        noncurrent.noncurrent_version_transitions = Some(vec![dto::NoncurrentVersionTransition {
            noncurrent_days: Some(30),
            ..Default::default()
        }]);
        let err = config_from_dto(Some(configuration(vec![noncurrent])), true)
            .expect_err("a noncurrent transition is refused on a tier backend too");
        assert_eq!(err.code().as_str(), "InvalidRequest", "{err:?}");
        assert!(
            err.message()
                .is_some_and(|message| message.contains(NONCURRENT_TRANSITION)),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_put_then_get_round_trips() {
        let (backend, b) = setup().await;
        let rules = vec![
            dto::LifecycleRule {
                abort_incomplete_multipart_upload: None,
                expiration: Some(dto::LifecycleExpiration {
                    days: Some(30),
                    date: None,
                    expired_object_delete_marker: None,
                }),
                filter: Some(dto::LifecycleRuleFilter {
                    and: Some(dto::LifecycleRuleAndOperator {
                        object_size_greater_than: Some(100),
                        object_size_less_than: Some(1_000),
                        prefix: Some("logs/".to_owned()),
                        tags: Some(vec![dto::Tag {
                            key: Some("k".to_owned()),
                            value: Some("v".to_owned()),
                        }]),
                    }),
                    ..Default::default()
                }),
                id: Some("archive".to_owned()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                status: enabled(),
                transitions: None,
            },
            dto::LifecycleRule {
                abort_incomplete_multipart_upload: Some(dto::AbortIncompleteMultipartUpload {
                    days_after_initiation: Some(7),
                }),
                expiration: Some(dto::LifecycleExpiration {
                    days: None,
                    date: Some(dto::Date::from(
                        time::OffsetDateTime::from_unix_timestamp(MIDNIGHT).unwrap(),
                    )),
                    expired_object_delete_marker: None,
                }),
                filter: Some(dto::LifecycleRuleFilter {
                    prefix: Some("tmp/".to_owned()),
                    ..Default::default()
                }),
                id: Some("uploads".to_owned()),
                noncurrent_version_expiration: Some(dto::NoncurrentVersionExpiration {
                    noncurrent_days: Some(10),
                    newer_noncurrent_versions: None,
                }),
                noncurrent_version_transitions: None,
                prefix: None,
                status: disabled(),
                transitions: None,
            },
        ];
        let out = backend
            .op_put_bucket_lifecycle(put(&b, rules.clone()))
            .await
            .unwrap();
        // LC7: the `PUT` answers 200 with an empty body.
        assert_eq!(
            out.output,
            dto::PutBucketLifecycleConfigurationOutput::default()
        );
        let got = backend.op_get_bucket_lifecycle(get(&b)).await.unwrap();
        assert_eq!(
            got.output.rules,
            Some(rules.clone()),
            "the wire round-trips"
        );

        // LC7: the `PUT` replaces the whole configuration.
        backend
            .op_put_bucket_lifecycle(put(&b, vec![days_rule("only", "a/", 1)]))
            .await
            .unwrap();
        let got = backend.op_get_bucket_lifecycle(get(&b)).await.unwrap();
        assert_eq!(
            got.output.rules,
            Some(vec![days_rule("only", "a/", 1)]),
            "a put replaces, never merges"
        );

        // LC7: `DELETE` clears it and is idempotent (s3s answers 204).
        backend
            .op_delete_bucket_lifecycle(delete(&b))
            .await
            .unwrap();
        backend
            .op_delete_bucket_lifecycle(delete(&b))
            .await
            .unwrap();
        let err = backend
            .op_get_bucket_lifecycle(get(&b))
            .await
            .expect_err("the configuration is gone");
        assert_eq!(err.code().as_str(), "NoSuchLifecycleConfiguration");
    }

    #[tokio::test]
    async fn the_capability_off_answers_not_implemented() {
        // LC7: the compile-time feature and the runtime capability are one
        // fact; the gate is the decorator's (FR-021), so the refusal is
        // driven through it, not the bare backend.
        let (backend, b) = setup_gated_with_caps(Capabilities {
            lifecycle: false,
            ..Default::default()
        })
        .await;
        let err = backend
            .put_bucket_lifecycle_configuration(put(&b, vec![days_rule("expire", "logs/", 30)]))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented", "{err:?}");
        let err = backend
            .get_bucket_lifecycle_configuration(get(&b))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented", "{err:?}");
        // And nothing was stored behind the gate.
        assert_eq!(
            backend
                .inner()
                .op_get_bucket_lifecycle(get(&b))
                .await
                .unwrap_err()
                .code()
                .as_str(),
            "NoSuchLifecycleConfiguration"
        );
    }
}
