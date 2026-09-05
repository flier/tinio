//! Bucket CORS operations of the S3 mapping layer (spec 2026-09-05).
//!
//! GetBucketCors/PutBucketCors/DeleteBucketCors over the storage
//! contract, gated on `caps.cors` (feature-off builds omit the forwards
//! entirely — s3s's trait defaults answer "is not implemented yet").
//! The put validates the config on the request path (400 InvalidRequest
//! on any violation) — the storage codec's self-heal applies to rows,
//! never input — and enforces the Content-MD5 header three-state.
//! The browser preflight is a CUSTOM ROUTE on the data plane
//! ([`CorsPreflightRoute`]): OPTIONS with Origin + Access-Control-Request-
//! Method is answered before s3s's op dispatch, anonymously (browsers
//! cannot sign it), with the rule's allow-list and the echoed request.

use std::sync::Arc;

use base64::{Engine, engine::general_purpose::STANDARD};
use http::{Extensions, HeaderMap, HeaderValue, Method, Uri, header};
use s3s::{Body, S3Error, S3Request, S3Response, S3Result, dto, route::S3Route, s3_error};

use crate::{
    _core::{
        bucket,
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

/// The CORS config lookup for the preflight route and the Task-10
/// decoration — the erased boundary over the shared storage handle.
/// `#[async_trait]` REQUIRED: a native async-fn-in-trait is not
/// dyn-compatible on stable Rust (E0038); the same pattern the storage
/// traits use. The erased method's call sites: the route resolves it
/// statically (concrete `CorsConfigs`), the data-plane decoration
/// dispatches through `dyn` (Task 10).
#[async_trait::async_trait]
pub(crate) trait CorsLookup: Send + Sync {
    /// The bucket's CORS configuration — `None` on ANY resolution
    /// failure (missing bucket, invalid name, codec self-heal), which the
    /// route maps to the single "CORS is not enabled" message (N1 — the
    /// existence-oracle closure).
    async fn get(&self, bucket: &str) -> Option<CorsConfig>;
}

/// The `CorsLookup` adapter over the storage handle shared with the
/// backend: `get` resolves through the contract's three-state accessor and
/// keeps every non-resolution as `None` (callers never see the backend
/// error — the route's denial is the only answer).
pub(crate) struct CorsConfigs<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage> CorsConfigs<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }
}

#[async_trait::async_trait]
impl<S: Storage> CorsLookup for CorsConfigs<S> {
    async fn get(&self, bucket: &str) -> Option<CorsConfig> {
        match bucket::name(bucket) {
            Ok(name) => self.storage.get_bucket_cors(&name).await.ok().flatten(),
            Err(_) => None,
        }
    }
}

/// The browser preflight: OPTIONS `/key` with `Origin` +
/// `Access-Control-Request-Method` answered before the op dispatch — with
/// the rule's allow-list (Q9), the echoed request headers, and the
/// `Vary` trio (Q4). Bare OPTIONS falls through to s3s (501, as today) —
/// `is_match` decides what a preflight is. Holds the erased lookup — the
/// same `Arc<dyn CorsLookup>` the data-plane decorator uses.
#[derive(Clone)]
pub(crate) struct CorsPreflightRoute {
    configs: Arc<dyn CorsLookup>,
}

impl CorsPreflightRoute {
    pub fn new(configs: Arc<dyn CorsLookup>) -> Self {
        Self { configs }
    }
}

/// The bucket of a path-style request (`/bucket/key` → "bucket"), owned:
/// s3s parses to a fresh `S3Path` per call and its accessors borrow it (a
/// `&str` return would outlive the temporary — E0515). The tinio data
/// plane serves path-style only (no virtual-hosted `S3Host` configured),
/// so this parse is consistent with s3s's own op routing. NOTE:
/// `parse_path_style(...).as_bucket()` would be the wrong accessor — it
/// answers `None` for `/bucket/key` (the `Bucket`-variant-only view, s3s
/// 0.15); `get_bucket_name()` covers both the bucket-only and the object
/// path.
pub(crate) fn bucket_from_uri(uri: &Uri) -> Option<String> {
    s3s::path::parse_path_style(uri.path())
        .ok()
        .and_then(|path| path.get_bucket_name().map(str::to_string))
}

/// The verbatim AWS mismatch message (grilling Q10 — the "evalution" typo
/// is AWS's own, kept verbatim).
const CORS_DENIED_MISMATCH_MSG: &str = "CORSResponse: This CORS request is not allowed. This is usually because the evalution of Origin, request method / Access-Control-Request-Method or Access-Control-Request-Headers are not whitelisted by the resource's CORS spec.";
/// The single no-config message (N1): no configuration AND a well-formed
/// but missing bucket answer the same — a probe cannot distinguish the two.
const CORS_NO_CONFIG_MSG: &str = "CORS is not enabled for this bucket.";

/// One home for every 403 denial of the route: the `?`-sites and the
/// `return`-sites both route here. A denial is `AccessDenied` with the
/// message and NO `Access-Control-*` headers and no `Vary` — s3s's
/// `serialize_error` produces only the XML body.
fn cors_denied_err_at(message: &str) -> S3Error {
    s3_error!(AccessDenied, "{message}")
}

#[async_trait::async_trait]
impl S3Route for CorsPreflightRoute {
    fn is_match(
        &self,
        method: &Method,
        _uri: &Uri,
        headers: &HeaderMap,
        _extensions: &mut Extensions,
    ) -> bool {
        // A true preflight: browsers send Origin + Access-Control-Request-
        // Method. Bare OPTIONS (non-browser probes) fall through to s3s —
        // 501 as today. (s3s runs `prepare` BEFORE `is_match`: an invalid
        // bucket name is already a 400 `InvalidBucketName`, never a 403
        // from here — op-review C1.)
        method == Method::OPTIONS
            && headers.contains_key(header::ORIGIN)
            && headers.contains_key(header::ACCESS_CONTROL_REQUEST_METHOD)
    }

    // Preflight is anonymous by definition (browsers cannot sign OPTIONS);
    // the default `S3Route::check_access` would demand credentials (403
    // "Signature is required") — this is the ONLY auth override.
    async fn check_access(&self, _req: &mut S3Request<Body>) -> S3Result<()> {
        Ok(())
    }

    async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        let origin = req
            .headers
            .get(header::ORIGIN)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| cors_denied_err_at(CORS_DENIED_MISMATCH_MSG))?;
        let method = req
            .headers
            .get(header::ACCESS_CONTROL_REQUEST_METHOD)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| cors_denied_err_at(CORS_DENIED_MISMATCH_MSG))?;
        let requested_headers: Vec<String> = req
            .headers
            .get_all(header::ACCESS_CONTROL_REQUEST_HEADERS)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .map(str::trim)
            .filter(|h| !h.is_empty())
            .map(str::to_string)
            .collect();
        // op-review C1 (precision): `prepare` validated the DECODED path
        // already — a bad name is a 400 before the route matches — while
        // this parse reads the RAW `req.uri`; a percent-encoded name could
        // answer a 403 here instead. Impact nil (legal bucket names never
        // percent-encode); the guard stays as defense.
        let bucket = bucket_from_uri(&req.uri)
            .ok_or_else(|| cors_denied_err_at(CORS_DENIED_MISMATCH_MSG))?;
        let Some(config) = self.configs.get(&bucket).await else {
            return Err(cors_denied_err_at(CORS_NO_CONFIG_MSG));
        };
        let Some(matched) = config.preflight(origin, method, &requested_headers) else {
            return Err(cors_denied_err_at(CORS_DENIED_MISMATCH_MSG));
        };

        let mut resp = S3Response::new(Body::empty());
        apply_cors_headers(&mut resp.headers, &matched.rule, &matched.origin);
        // The request's own names (case/spelling verbatim — the codec
        // stores the request's values), echoed as one list.
        if !matched.requested_headers.is_empty() {
            let joined = matched.requested_headers.join(", ");
            if let Ok(v) = HeaderValue::from_str(&joined) {
                resp.headers.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, v);
            }
        }
        if let Some(max_age) = matched.rule.max_age_seconds {
            // op-review S1 applies here too (never unwrap; the insert of a
            // String would panic on failure).
            if let Ok(v) = HeaderValue::from_str(&max_age.to_string()) {
                resp.headers.insert(header::ACCESS_CONTROL_MAX_AGE, v);
            }
        }
        // op-review G4: s3s copies custom-route headers verbatim and sets
        // nothing — an explicit empty body length.
        resp.headers
            .insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
        Ok(resp)
    }
}

/// Whether the rule's origins contain a bare `*` (grilling Q11).
fn star_rule_origin(rule: &CorsRule) -> bool {
    rule.allowed_origins.iter().any(|o| o == "*")
}

/// The shared response-decoration of the CORS behavior — one home for the
/// pinned semantics, used by BOTH the preflight route and the data-plane
/// decorator of actual responses (the two must never answer differently):
/// ACAO = the echoed `origin` or a literal `*` for a bare-`*` origin rule
/// (`Access-Control-Allow-Credentials` omitted then — Q11; `true`
/// otherwise), the rule's method list (Q9), the rule's expose list (when
/// present and non-empty), and the `Vary` trio APPENDED (Q4/G3 — merge,
/// never replace). op-review S1: every value is constructed fallibly —
/// a value that cannot be a header is SKIPPED, never unwrap/panicked.
pub(crate) fn apply_cors_headers(headers: &mut HeaderMap, rule: &CorsRule, origin: &str) {
    // The literals are not request data — built once (`from_static` panics
    // only on an invalid literal; these are constants). Request/config data
    // passes `from_str`+skip.
    let acao = if star_rule_origin(rule) { "*" } else { origin };
    if let Ok(v) = HeaderValue::from_str(acao) {
        headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, v);
    }
    let methods = rule.allowed_methods.join(", ");
    if let Ok(v) = HeaderValue::from_str(&methods) {
        headers.insert(header::ACCESS_CONTROL_ALLOW_METHODS, v);
    }
    if !star_rule_origin(rule) {
        headers.insert(
            header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
            HeaderValue::from_static("true"),
        );
    }
    if let Some(expose) = &rule.expose_headers
        && !expose.is_empty()
    {
        let joined = expose.join(", ");
        if let Ok(v) = HeaderValue::from_str(&joined) {
            headers.insert(header::ACCESS_CONTROL_EXPOSE_HEADERS, v);
        }
    }
    for v in [
        "Origin",
        "Access-Control-Request-Headers",
        "Access-Control-Request-Method",
    ] {
        headers.append(header::VARY, HeaderValue::from_static(v));
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
    use std::sync::Arc;

    use http::{Extensions, HeaderMap, HeaderValue, Method, header};
    use s3s::{Body, S3, S3Request, dto, route::S3Route};

    use super::{
        CORS_DENIED_MISMATCH_MSG, CORS_NO_CONFIG_MSG, CorsConfigs, CorsLookup,
        CorsPreflightRoute, bucket_from_uri,
    };
    use crate::{
        _core::{
            bucket,
            cors::{CorsConfig, CorsRule},
            storage::BucketOps,
        },
        _mem::MemoryStorage,
        backend::{
            Capabilities,
            testutil::{s3_request, setup, setup_with_caps},
        },
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

    /// The preflight route over a fresh MemoryStorage seeded with `config`
    /// for the bucket "data" (created first — mem's CORS trio answers
    /// NoSuchBucket for a missing row).
    async fn test_route(config: CorsConfig) -> CorsPreflightRoute {
        let storage = Arc::new(MemoryStorage::new().unwrap());
        let name = bucket::name("data").unwrap();
        storage.create_bucket(&name).await.unwrap();
        storage.put_bucket_cors(&name, &config).await.unwrap();
        CorsPreflightRoute::new(Arc::new(CorsConfigs::new(storage)) as Arc<dyn CorsLookup>)
    }

    /// A browser preflight: anonymous OPTIONS `<path>` with Origin +
    /// Access-Control-Request-Method (+ optional requested headers).
    fn preflight_req(
        path: &str,
        origin: &str,
        method: &str,
        requested_headers: Option<&str>,
    ) -> S3Request<Body> {
        let mut headers = HeaderMap::new();
        headers.insert(header::ORIGIN, HeaderValue::from_str(origin).unwrap());
        headers.insert(
            header::ACCESS_CONTROL_REQUEST_METHOD,
            HeaderValue::from_str(method).unwrap(),
        );
        if let Some(requested) = requested_headers {
            headers.insert(
                header::ACCESS_CONTROL_REQUEST_HEADERS,
                HeaderValue::from_str(requested).unwrap(),
            );
        }
        S3Request {
            input: Body::empty(),
            method: Method::OPTIONS,
            uri: path.parse().unwrap(),
            headers,
            extensions: Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    #[tokio::test]
    async fn preflight_matches_allowed_origin_and_answers_headers() {
        let route = test_route(CorsConfig {
            rules: vec![CorsRule {
                id: Some("allow-example".into()),
                allowed_methods: vec!["GET".into(), "PUT".into()],
                allowed_origins: vec!["https://example.com".into()],
                allowed_headers: Some(vec!["x-amz-*".into()]),
                expose_headers: Some(vec!["ETag".into()]),
                max_age_seconds: Some(300),
            }],
        })
        .await;
        let resp = route
            .call(preflight_req(
                "/data/key",
                "https://example.com",
                "PUT",
                Some("x-amz-foo"),
            ))
            .await
            .unwrap();
        assert_eq!(
            resp.headers[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://example.com"
        );
        // grilling Q9: Allow-Methods is the RULE's list, not the
        // requested method.
        assert_eq!(
            resp.headers[header::ACCESS_CONTROL_ALLOW_METHODS],
            "GET, PUT"
        );
        // The request's own case/spelling, echoed (the codec stores the
        // request's values).
        assert_eq!(
            resp.headers[header::ACCESS_CONTROL_ALLOW_HEADERS],
            "x-amz-foo"
        );
        assert_eq!(resp.headers[header::ACCESS_CONTROL_EXPOSE_HEADERS], "ETag");
        assert_eq!(resp.headers[header::ACCESS_CONTROL_MAX_AGE], "300");
        assert_eq!(
            resp.headers[header::ACCESS_CONTROL_ALLOW_CREDENTIALS],
            "true"
        );
        // grilling Q4: the Vary trio is APPENDed — the wire carries it as
        // a multi-valued header (three Vary lines).
        let vary: Vec<&str> = resp
            .headers
            .get_all(header::VARY)
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert_eq!(
            vary,
            [
                "Origin",
                "Access-Control-Request-Headers",
                "Access-Control-Request-Method"
            ]
        );
        // op-review G4: set explicitly.
        assert_eq!(resp.headers[header::CONTENT_LENGTH], "0");
    }

    #[tokio::test]
    async fn preflight_bare_star_rule_answers_literal_star_without_credentials() {
        // grilling Q11: origin rule = "*" → ACAO literal "*", Allow-
        // Credentials OMITTED (the two are incompatible).
        let route = test_route(CorsConfig {
            rules: vec![CorsRule {
                id: None,
                allowed_methods: vec!["GET".into()],
                allowed_origins: vec!["*".into()],
                allowed_headers: None,
                expose_headers: None,
                max_age_seconds: None,
            }],
        })
        .await;
        let resp = route
            .call(preflight_req(
                "/data/key",
                "https://example.com",
                "GET",
                None,
            ))
            .await
            .unwrap();
        assert_eq!(resp.headers[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
        assert!(
            !resp
                .headers
                .contains_key(header::ACCESS_CONTROL_ALLOW_CREDENTIALS)
        );
    }

    #[tokio::test]
    async fn preflight_disallowed_and_no_config_answer_403_with_aws_messages() {
        // grilling Q10: a rule mismatch → 403 AccessDenied with the
        // verbatim AWS message (the "evalution" typo is AWS's own).
        let route = test_route(CorsConfig {
            rules: vec![CorsRule {
                id: None,
                allowed_methods: vec!["GET".into()],
                allowed_origins: vec!["https://example.com".into()],
                allowed_headers: None,
                expose_headers: None,
                max_age_seconds: None,
            }],
        })
        .await;
        let err = route
            .call(preflight_req(
                "/data/key",
                "https://example.com",
                "PUT",
                None,
            ))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "AccessDenied", "{err:?}");
        assert_eq!(err.message().unwrap(), CORS_DENIED_MISMATCH_MSG, "{err:?}");
        // Denials carry no Access-Control-* headers and no Vary.
        assert!(err.headers().is_none(), "{err:?}");

        // No CORS config (a configured-less bucket) → the same code with
        // the no-config message.
        let storage = Arc::new(MemoryStorage::new().unwrap());
        let name = bucket::name("data").unwrap();
        storage.create_bucket(&name).await.unwrap();
        let route = CorsPreflightRoute::new(Arc::new(CorsConfigs::new(storage)) as Arc<dyn CorsLookup>);
        let err = route
            .call(preflight_req(
                "/data/key",
                "https://example.com",
                "GET",
                None,
            ))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "AccessDenied", "{err:?}");
        assert_eq!(err.message().unwrap(), CORS_NO_CONFIG_MSG, "{err:?}");
        assert!(err.headers().is_none(), "{err:?}");
    }

    #[tokio::test]
    async fn preflight_missing_bucket_uses_the_same_no_config_message() {
        // N1 / existence-oracle closure: a well-formed but MISSING bucket
        // resolves to `CorsConfigs::get → None` — the route answers the
        // SAME "CORS is not enabled for this bucket." message as the
        // no-config case: a probe cannot distinguish "bucket exists, no
        // CORS" from "bucket missing".
        let storage = Arc::new(MemoryStorage::new().unwrap());
        let route = CorsPreflightRoute::new(Arc::new(CorsConfigs::new(storage)) as Arc<dyn CorsLookup>);
        let err = route
            .call(preflight_req(
                "/missing/key",
                "https://example.com",
                "GET",
                None,
            ))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "AccessDenied", "{err:?}");
        assert_eq!(err.message().unwrap(), CORS_NO_CONFIG_MSG, "{err:?}");
    }

    #[tokio::test]
    async fn preflight_anonymous_check_access_is_allowed() {
        // Ruling 1: preflight is anonymous by definition — browsers cannot
        // sign OPTIONS — the default `S3Route::check_access` ("Signature is
        // required") is overridden to Ok.
        let route = test_route(CorsConfig {
            rules: vec![CorsRule {
                id: None,
                allowed_methods: vec!["GET".into()],
                allowed_origins: vec!["*".into()],
                allowed_headers: None,
                expose_headers: None,
                max_age_seconds: None,
            }],
        })
        .await;
        let mut req = preflight_req("/data/key", "https://example.com", "GET", None);
        assert!(req.credentials.is_none());
        S3Route::check_access(&route, &mut req).await.unwrap();
    }

    #[test]
    fn bucket_from_uri_is_the_bucket_of_object_and_bucket_paths() {
        // s3s 0.15's `S3Path::as_bucket()` answers None for /bucket/key
        // (the Bucket-variant-only accessor) — the route needs
        // `get_bucket_name()`, which covers both path shapes.
        let bucket = |path: &str| -> Option<String> { bucket_from_uri(&path.parse().unwrap()) };
        assert_eq!(bucket("/data/key").as_deref(), Some("data"));
        assert_eq!(bucket("/data").as_deref(), Some("data"));
        assert_eq!(bucket("/"), None);
        assert_eq!(bucket("/MyBucket/key"), None);
    }
}
