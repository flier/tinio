//! Bucket CORS operations of the S3 mapping layer (spec 2026-09-05).
//!
//! GetBucketCors/PutBucketCors/DeleteBucketCors over the storage
//! contract, gated on `caps.cors` (feature-off builds omit the forwards
//! entirely — s3s's trait defaults answer "is not implemented yet").
//! The put validates the config on the request path (400 InvalidRequest
//! on any violation) — the storage codec's self-heal applies to rows,
//! never input — and enforces the Content-MD5 header three-state.

use base64::{Engine, engine::general_purpose::STANDARD};
use s3s::{S3Request, S3Response, S3Result, dto, s3_error};

use crate::{
    _core::{
        cors::{
            CORS_CONFIG_BYTES_MAX, CORS_METHODS, CORS_RULE_ID_MAX, CORS_RULES_MAX, CorsConfig,
            CorsRule,
        },
        storage::Storage,
    },
    backend::{S3Backend, map_backend_error},
};

impl<S: Storage> S3Backend<S> {
    pub(crate) async fn op_get_bucket_cors(
        &self,
        req: S3Request<dto::GetBucketCorsInput>,
    ) -> S3Result<S3Response<dto::GetBucketCorsOutput>> {
        Self::require_cap(self.caps.cors, "GetBucketCors")?;
        let bucket = self.bucket(req.input.bucket)?;
        match self
            .storage
            .get_bucket_cors(&bucket)
            .await
            .map_err(map_backend_error)?
        {
            Some(config) => Ok(S3Response::new(dto::GetBucketCorsOutput {
                cors_rules: Some(cors_rules_to_dto(&config)),
            })),
            None => Err(s3_error!(
                NoSuchCORSConfiguration,
                "The CORS configuration does not exist"
            )),
        }
    }

    pub(crate) async fn op_put_bucket_cors(
        &self,
        req: S3Request<dto::PutBucketCorsInput>,
    ) -> S3Result<S3Response<dto::PutBucketCorsOutput>> {
        Self::require_cap(self.caps.cors, "PutBucketCors")?;
        let bucket = self.bucket(req.input.bucket)?;
        validate_content_md5(req.input.content_md5.as_deref())?;
        let config = cors_config_from_dto(&req.input.cors_configuration)?;
        self.storage
            .put_bucket_cors(&bucket, &config)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::PutBucketCorsOutput::default()))
    }

    pub(crate) async fn op_delete_bucket_cors(
        &self,
        req: S3Request<dto::DeleteBucketCorsInput>,
    ) -> S3Result<S3Response<dto::DeleteBucketCorsOutput>> {
        Self::require_cap(self.caps.cors, "DeleteBucketCors")?;
        let bucket = self.bucket(req.input.bucket)?;
        self.storage
            .delete_bucket_cors(&bucket)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::DeleteBucketCorsOutput::default()))
    }
}

/// Content-MD5 on PutBucketCors, AWS three-state (grilling Q8): missing →
/// 400 InvalidRequest with the verified AWS message; malformed (not
/// 16-byte base64) → 400 InvalidDigest; digest equality vs the XML body is
/// unverifiable here (s3s consumes the body — recorded deviation, AWS's
/// BadDigest path unreachable).
/// NOTE (coordination): the ACL plan's A7 ruled "missing OR malformed →
/// InvalidDigest" for the put-ACL ops; when ACL merges, its helper is
/// aligned to this three-state behavior (or this helper is made shared
/// with the AWS behavior) — one-line change at that merge.
fn validate_content_md5(md5: Option<&str>) -> S3Result<()> {
    let md5 = md5.ok_or_else(|| {
        s3_error!(
            InvalidRequest,
            "Missing required header for this request: Content-MD5"
        )
    })?;
    let raw = STANDARD
        .decode(md5)
        .map_err(|_| s3_error!(InvalidDigest, "The Content-MD5 you specified is not valid"))?;
    if raw.len() == 16 {
        Ok(())
    } else {
        Err(s3_error!(
            InvalidDigest,
            "The Content-MD5 you specified is not valid"
        ))
    }
}

/// Any C0 control byte (<0x20) or DEL (0x7f) — such bytes may not reach
/// response headers (op-review S1): HeaderValue construction rejects them,
/// and a stored poison would panic the per-connection task.
fn has_control_bytes(s: &str) -> bool {
    s.bytes().any(|b| b < 0x20 || b == 0x7f)
}

/// Decoded config size in bytes (op-review P1 — bounds the per-Origin
/// decode/scan amplification; the put body is otherwise limited only by
/// s3s's 20 MB XML cap).
fn config_bytes(config: &CorsConfig) -> usize {
    config
        .rules
        .iter()
        .map(|r| {
            r.id.as_deref().map_or(0, str::len)
                + r.allowed_methods.iter().map(String::len).sum::<usize>()
                + r.allowed_origins.iter().map(String::len).sum::<usize>()
                + r.allowed_headers
                    .as_ref()
                    .map_or(0, |v| v.iter().map(String::len).sum())
                + r.expose_headers
                    .as_ref()
                    .map_or(0, |v| v.iter().map(String::len).sum())
        })
        .sum()
}

/// dto → core conversion with request-level validation (400 InvalidRequest on
/// malformed configs; the storage codec's self-heal applies to rows, never
/// input).
fn cors_config_from_dto(xml: &dto::CORSConfiguration) -> S3Result<CorsConfig> {
    let rules = &xml.cors_rules;
    if rules.is_empty() {
        return Err(s3_error!(
            InvalidRequest,
            "The CORS configuration must have at least one rule"
        ));
    }
    if rules.len() > CORS_RULES_MAX {
        return Err(s3_error!(
            InvalidRequest,
            "The CORS configuration must have at most {CORS_RULES_MAX} rules"
        ));
    }
    let mut out = Vec::with_capacity(rules.len());
    for r in rules {
        if let Some(id) = &r.id
            && id.chars().count() > CORS_RULE_ID_MAX
        {
            return Err(s3_error!(
                InvalidRequest,
                "The rule ID must be at most {CORS_RULE_ID_MAX} characters"
            ));
        }
        // F5: the dto Vecs are non-Option but can be EMPTY from an XML body
        // with zero such elements — every rule must name ≥1 method and ≥1
        // origin.
        if r.allowed_methods.is_empty() {
            return Err(s3_error!(
                InvalidRequest,
                "Each CORS rule must have at least one AllowedMethod"
            ));
        }
        if r.allowed_origins.is_empty() {
            return Err(s3_error!(
                InvalidRequest,
                "Each CORS rule must have at least one AllowedOrigin"
            ));
        }
        for m in &r.allowed_methods {
            if !CORS_METHODS.iter().any(|v| v.eq_ignore_ascii_case(m)) {
                return Err(s3_error!(InvalidRequest, "Invalid AllowedMethod: {m}"));
            }
        }
        // grilling Q6 = (b): ≤1 `*` per pattern; op-review S1: no control
        // bytes; F1: no `,` in any list item (an unescaped `,` would split
        // the 6-field wire record).
        for (what, patterns) in [
            ("AllowedOrigin", Some(&r.allowed_origins)),
            ("AllowedHeader", r.allowed_headers.as_ref()),
        ] {
            if let Some(patterns) = patterns
                && patterns.iter().any(|p| {
                    p.bytes().filter(|b| *b == b'*').count() > 1
                        || has_control_bytes(p)
                        || p.contains(',')
                })
            {
                return Err(s3_error!(
                    InvalidRequest,
                    "Invalid {what} pattern in the CORS configuration"
                ));
            }
        }
        if let Some(expose) = &r.expose_headers
            && expose
                .iter()
                .any(|e| has_control_bytes(e) || e.contains(','))
        {
            return Err(s3_error!(
                InvalidRequest,
                "Invalid ExposeHeader in the CORS configuration"
            ));
        }
        if r.id
            .as_ref()
            .is_some_and(|id| has_control_bytes(id) || id.contains(','))
        {
            return Err(s3_error!(
                InvalidRequest,
                "Invalid rule ID in the CORS configuration"
            ));
        }
        if r.max_age_seconds.is_some_and(|m| m < 0) {
            return Err(s3_error!(
                InvalidRequest,
                "MaxAgeSeconds must be non-negative"
            ));
        }
        out.push(CorsRule {
            id: r.id.clone(),
            allowed_methods: r
                .allowed_methods
                .iter()
                .map(|m| m.to_ascii_uppercase())
                .collect(),
            allowed_origins: r.allowed_origins.clone(),
            allowed_headers: r.allowed_headers.clone(),
            expose_headers: r.expose_headers.clone(),
            max_age_seconds: r.max_age_seconds,
        });
    }
    let config = CorsConfig { rules: out };
    if config_bytes(&config) > CORS_CONFIG_BYTES_MAX {
        return Err(s3_error!(
            InvalidRequest,
            "The CORS configuration is too large"
        ));
    }
    Ok(config)
}

/// Core config into a dto `CorsRules` (GetBucketCors output).
fn cors_rules_to_dto(config: &CorsConfig) -> dto::CORSRules {
    config
        .rules
        .iter()
        .map(|r| dto::CORSRule {
            id: r.id.clone(),
            allowed_methods: r.allowed_methods.clone(),
            allowed_origins: r.allowed_origins.clone(),
            allowed_headers: r.allowed_headers.clone(),
            expose_headers: r.expose_headers.clone(),
            max_age_seconds: r.max_age_seconds,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use s3s::{S3, dto};

    use crate::backend::{
        Capabilities,
        testutil::{s3_request, setup, setup_with_caps},
    };

    /// A valid Content-MD5: base64 of 16 zero bytes. The ops never verify
    /// equality against the body (recorded deviation), so any 16-byte
    /// value passes.
    const VALID_MD5: &str = "AAAAAAAAAAAAAAAAAAAAAA==";

    /// A minimal valid rule: GET + one origin, no optional fields.
    fn rule(id: Option<&str>, origins: &[&str]) -> dto::CORSRule {
        dto::CORSRule {
            id: id.map(Into::into),
            allowed_methods: vec!["GET".into()],
            allowed_origins: origins.iter().map(|s| s.to_string()).collect(),
            allowed_headers: None,
            expose_headers: None,
            max_age_seconds: None,
        }
    }

    #[tokio::test]
    async fn bucket_cors_ops_round_trip_and_delete() {
        // put → get echoes (order preserved, optional fields);
        // delete → get 404 NoSuchCORSConfiguration.
        let (backend, b) = setup().await;
        let rules = vec![
            dto::CORSRule {
                id: Some("allow-example".into()),
                allowed_methods: vec!["GET".into(), "PUT".into()],
                allowed_origins: vec!["https://example.com".into(), "https://*.example.net".into()],
                allowed_headers: Some(vec!["x-amz-*".into(), "content-type".into()]),
                expose_headers: Some(vec!["ETag".into()]),
                max_age_seconds: Some(300),
            },
            dto::CORSRule {
                id: None,
                allowed_methods: vec!["DELETE".into()],
                allowed_origins: vec!["*".into()],
                allowed_headers: None,
                expose_headers: None,
                max_age_seconds: None,
            },
        ];
        backend
            .put_bucket_cors(s3_request(dto::PutBucketCorsInput {
                bucket: b.to_string(),
                cors_configuration: dto::CORSConfiguration {
                    cors_rules: rules.clone(),
                },
                checksum_algorithm: None,
                content_md5: Some(VALID_MD5.into()),
                expected_bucket_owner: None,
            }))
            .await
            .unwrap();
        let got = backend
            .get_bucket_cors(s3_request(dto::GetBucketCorsInput {
                bucket: b.to_string(),
                expected_bucket_owner: None,
            }))
            .await
            .unwrap();
        assert_eq!(got.output.cors_rules, Some(rules));
        backend
            .delete_bucket_cors(s3_request(dto::DeleteBucketCorsInput {
                bucket: b.to_string(),
                expected_bucket_owner: None,
            }))
            .await
            .unwrap();
        let err = backend
            .get_bucket_cors(s3_request(dto::GetBucketCorsInput {
                bucket: b.to_string(),
                expected_bucket_owner: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchCORSConfiguration", "{err:?}");
        assert!(
            err.message()
                .unwrap()
                .contains("The CORS configuration does not exist"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn put_bucket_cors_requires_content_md5() {
        let (backend, b) = setup().await;
        let put = |content_md5: Option<String>| {
            backend.put_bucket_cors(s3_request(dto::PutBucketCorsInput {
                bucket: b.to_string(),
                cors_configuration: dto::CORSConfiguration {
                    cors_rules: vec![rule(None, &["https://example.com"])],
                },
                checksum_algorithm: None,
                content_md5,
                expected_bucket_owner: None,
            }))
        };

        // Missing header → 400 InvalidRequest with the verified AWS
        // message (grilling Q8 — NOT InvalidDigest, differs from the ACL
        // plan's A7 one-state ruling; see validate_content_md5).
        let err = put(None).await.unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequest", "{err:?}");
        assert!(
            err.message()
                .unwrap()
                .contains("Missing required header for this request: Content-MD5"),
            "{err:?}"
        );

        // Malformed (not base64) → 400 InvalidDigest.
        let err = put(Some("not-base64!".into())).await.unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidDigest", "{err:?}");

        // Valid base64 that decodes to the wrong length (5 bytes) →
        // 400 InvalidDigest.
        let wrong_len = "aGVsbG8="; // base64("hello") = 5 bytes
        let err = put(Some(wrong_len.into())).await.unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidDigest", "{err:?}");
    }

    #[tokio::test]
    async fn put_bucket_cors_validation_rejects_bad_config() {
        let (backend, b) = setup().await;
        let put = |rules: Vec<dto::CORSRule>| {
            backend.put_bucket_cors(s3_request(dto::PutBucketCorsInput {
                bucket: b.to_string(),
                cors_configuration: dto::CORSConfiguration { cors_rules: rules },
                checksum_algorithm: None,
                content_md5: Some(VALID_MD5.into()),
                expected_bucket_owner: None,
            }))
        };
        let invalid_request = |err: s3s::S3Error, msg: &str| {
            assert_eq!(err.code().as_str(), "InvalidRequest", "{err:?}");
            assert!(err.message().unwrap().contains(msg), "{err:?}");
        };

        // Empty rules → InvalidRequest.
        let err = put(vec![]).await.unwrap_err();
        invalid_request(err, "at least one rule");
        // 101 rules → InvalidRequest; the 100 boundary is accepted.
        let over: Vec<dto::CORSRule> = (0..101)
            .map(|_| rule(None, &["https://example.com"]))
            .collect();
        let err = put(over).await.unwrap_err();
        invalid_request(err, "at most 100 rules");
        put((0..100)
            .map(|_| rule(None, &["https://example.com"]))
            .collect())
        .await
        .unwrap();

        // id > 255 → InvalidRequest; 255 itself is accepted.
        let err = put(vec![rule(Some(&"x".repeat(256)), &["https://example.com"])])
            .await
            .unwrap_err();
        invalid_request(err, "255");
        put(vec![rule(Some(&"x".repeat(255)), &["https://example.com"])])
            .await
            .unwrap();

        // AllowedMethod "PATCH" (not in the accept set) → InvalidRequest.
        let err = put(vec![dto::CORSRule {
            allowed_methods: vec!["PATCH".into()],
            ..rule(None, &["https://example.com"])
        }])
        .await
        .unwrap_err();
        invalid_request(err, "Invalid AllowedMethod");

        // A pattern with two '*' → InvalidRequest (grilling Q6).
        let err = put(vec![rule(None, &["a*b*c"])]).await.unwrap_err();
        invalid_request(err, "Invalid AllowedOrigin pattern");

        // A C0 control byte in an origin pattern → InvalidRequest
        // (op-review S1); DEL in an expose header → InvalidRequest.
        let err = put(vec![rule(None, &["https://a.example.com\u{1}"])])
            .await
            .unwrap_err();
        invalid_request(err, "Invalid AllowedOrigin pattern");
        let err = put(vec![dto::CORSRule {
            expose_headers: Some(vec!["ETag\u{7f}".into()]),
            ..rule(None, &["*"])
        }])
        .await
        .unwrap_err();
        invalid_request(err, "Invalid ExposeHeader");
        // A control byte in a rule ID → InvalidRequest.
        let err = put(vec![rule(Some("id\u{1}"), &["*"])]).await.unwrap_err();
        invalid_request(err, "Invalid rule ID");

        // A ',' in an origin/header/expose pattern or a rule ID →
        // InvalidRequest (F1).
        let err = put(vec![rule(None, &["https://a.example.com,evil"])])
            .await
            .unwrap_err();
        invalid_request(err, "Invalid AllowedOrigin pattern");
        let err = put(vec![dto::CORSRule {
            allowed_headers: Some(vec!["x-amz,evil".into()]),
            ..rule(None, &["*"])
        }])
        .await
        .unwrap_err();
        invalid_request(err, "Invalid AllowedHeader pattern");
        let err = put(vec![dto::CORSRule {
            expose_headers: Some(vec!["ETag,Evil".into()]),
            ..rule(None, &["*"])
        }])
        .await
        .unwrap_err();
        invalid_request(err, "Invalid ExposeHeader");
        let err = put(vec![rule(Some("a,b"), &["*"])]).await.unwrap_err();
        invalid_request(err, "Invalid rule ID");

        // An EMPTY AllowedMethod list, or an EMPTY AllowedOrigin list →
        // InvalidRequest (F5).
        let err = put(vec![dto::CORSRule {
            allowed_methods: vec![],
            ..rule(None, &["*"])
        }])
        .await
        .unwrap_err();
        invalid_request(err, "at least one AllowedMethod");
        let err = put(vec![dto::CORSRule {
            allowed_origins: vec![],
            ..rule(None, &["*"])
        }])
        .await
        .unwrap_err();
        invalid_request(err, "at least one AllowedOrigin");

        // max_age_seconds < 0 → InvalidRequest.
        let err = put(vec![dto::CORSRule {
            max_age_seconds: Some(-1),
            ..rule(None, &["*"])
        }])
        .await
        .unwrap_err();
        invalid_request(err, "MaxAgeSeconds must be non-negative");

        // Decoded config over 64 KB → InvalidRequest (op-review P1).
        let big = "a".repeat(70000);
        let err = put(vec![rule(None, &[big.as_str()])]).await.unwrap_err();
        invalid_request(err, "too large");
    }

    #[tokio::test]
    async fn cors_toggle_off_gates_the_bucket_cors_ops() {
        // The cors toggle off answers NotImplemented (FR-021). The gate
        // fires before the bucket check — no bucket is needed.
        let (backend, _b) = setup_with_caps(Capabilities {
            cors: false,
            ..Default::default()
        })
        .await;
        let disabled = |err: s3s::S3Error| {
            assert_eq!(err.code().as_str(), "NotImplemented", "{err:?}");
            // The gate's message distinguishes it from the trapless
            // feature-off default ("... is not implemented yet").
            assert!(err.message().unwrap().contains("is disabled"), "{err:?}");
        };
        let err = backend
            .get_bucket_cors(s3_request(dto::GetBucketCorsInput {
                bucket: "data".into(),
                expected_bucket_owner: None,
            }))
            .await
            .unwrap_err();
        disabled(err);
        let err = backend
            .put_bucket_cors(s3_request(dto::PutBucketCorsInput {
                bucket: "data".into(),
                cors_configuration: dto::CORSConfiguration {
                    cors_rules: vec![rule(None, &["https://example.com"])],
                },
                checksum_algorithm: None,
                content_md5: Some(VALID_MD5.into()),
                expected_bucket_owner: None,
            }))
            .await
            .unwrap_err();
        disabled(err);
        let err = backend
            .delete_bucket_cors(s3_request(dto::DeleteBucketCorsInput {
                bucket: "data".into(),
                expected_bucket_owner: None,
            }))
            .await
            .unwrap_err();
        disabled(err);
    }
}
