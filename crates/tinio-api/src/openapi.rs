//! The OpenAPI document (`management-api.md:59`), behind the `openapi` feature.
//!
//! `research.md:96-97` fixes the crate and the feature: `utoipa` with `axum_extras`, no
//! Swagger UI in v1 — the spec JSON only.

use utoipa::{
    Modify, OpenApi,
    openapi::{
        OpenApi as OpenApiSchema,
        security::{ApiKey, ApiKeyValue, SecurityScheme},
    },
};

use crate::router::{StatusBody, StopBody};

/// The document, with the token security scheme attached.
///
/// # Examples
///
/// ```rust
/// let json = tinio_api::document().to_json().expect("json");
/// for path in ["/status", "/stop", "/metrics", "/openapi.json"] {
///     assert!(json.contains(path), "{path} is missing from the document");
/// }
/// assert!(
///     json.contains("X-Tinio-Token"),
///     "the credential a caller presents"
/// );
/// ```
pub fn document() -> OpenApiSchema {
    #[derive(OpenApi)]
    #[openapi(
        info(description = "\
            The management plane's token rule follows the bind address, not the handler. On the \
            local control channel — a unix socket or a named pipe — `/metrics` and \
            `/openapi.json` are open, and `/status` and `/stop` need the token; a TCP bind to a \
            loopback address opens `/metrics` alone, because a Prometheus scrape cannot send a \
            custom header, and that reason does not cover the schema or the control endpoints; \
            every other bind requires the token on every endpoint. The token is presented in \
            `X-Tinio-Token`, or in `Authorization: Bearer <token>` — the alias is accepted on \
            every transport, since a standard scraper can send only the latter header."),
        // The paths stay fully qualified on purpose: utoipa's macro synthesises a
        // `__path_<fn>` companion type from the path's last segment, so an imported name has no
        // module to resolve it against — `paths(status_doc, …)` does not compile. The
        // `absolute_paths` clippy lint cannot see inside a macro's token tree, which is the
        // other half of the same fact.
        paths(
            crate::router::status_doc,
            crate::router::stop_doc,
            crate::router::metrics_doc,
            crate::router::openapi_doc
        ),
        components(schemas(StatusBody, StopBody)),
        modifiers(&TokenScheme)
    )]
    struct Api;

    // The three lifecycle routes exist only in a build that carries them (LC14, LC15), so the
    // document lists them only there: the paths are merged rather than listed above, because a
    // `paths(...)` list cannot carry a `cfg` and a schema naming a route the plane does not serve
    // would be the lie the feature gate exists to prevent.
    #[cfg(feature = "lifecycle")]
    #[derive(OpenApi)]
    #[openapi(paths(
        crate::lifecycle::lifecycle_get_doc,
        crate::lifecycle::lifecycle_put_doc,
        crate::lifecycle::lifecycle_delete_doc
    ))]
    struct LifecycleApi;

    // The two Object Lock route pairs exist only in a build that carries them (OL23), so the
    // document lists them only there — the same merge, for the same reason.
    #[cfg(feature = "object-lock")]
    #[derive(OpenApi)]
    #[openapi(paths(
        crate::object_lock::object_lock_bucket_get_doc,
        crate::object_lock::object_lock_bucket_put_doc,
        crate::object_lock::object_lock_object_get_doc,
        crate::object_lock::object_lock_object_put_doc
    ))]
    struct ObjectLockApi;

    let document = Api::openapi();
    #[cfg(feature = "lifecycle")]
    let document = document.merge_from(LifecycleApi::openapi());
    #[cfg(feature = "object-lock")]
    let document = document.merge_from(ObjectLockApi::openapi());
    document
}

struct TokenScheme;

impl Modify for TokenScheme {
    fn modify(&self, openapi: &mut OpenApiSchema) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "token",
                SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::new("X-Tinio-Token"))),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use http_body_util::BodyExt;
    use tokio::sync::watch;
    use tower::ServiceExt;

    use crate::{
        router::{Auth, Context, router as management_router},
        state::State,
    };

    /// The document as the **local channel** serves it: `document()`'s own doctest pins the
    /// schema's content, and this the bytes a local caller receives — every registered path and
    /// the token scheme, through the router rather than the builder.
    #[tokio::test]
    async fn the_document_is_served_on_the_local_channel() {
        let state = State::new(
            "tinio-0123456789abcdef.sock",
            Some(9000),
            Path::new("/srv/data"),
        )
        .expect("state");
        let (tx, _rx) = watch::channel(false);
        let shared = Arc::new(Context::new(state, "127.0.0.1", Auth::Local, tx));

        let response = management_router(shared)
            .oneshot(
                Request::builder()
                    .uri("/openapi.json")
                    .method("GET")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "the local channel serves the schema openly"
        );
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let text = String::from_utf8(body.to_vec()).expect("utf8");
        for path in ["/status", "/stop", "/metrics", "/openapi.json"] {
            assert!(text.contains(path), "{path} is missing from the document");
        }
        assert!(
            text.contains("X-Tinio-Token"),
            "the token scheme is documented"
        );
    }

    /// LC14 mirrors the three routes in the document — and LC15 says only a build that registers
    /// them does, which is why this leg carries the same feature gate as the routes.
    #[cfg(feature = "lifecycle")]
    #[test]
    fn the_openapi_document_lists_the_three_paths() {
        let schema = serde_json::to_value(super::document()).expect("the document is JSON");
        let lifecycle = schema["paths"]
            .get("/buckets/{bucket}/lifecycle")
            .expect("the three routes are listed");
        for method in ["get", "put", "delete"] {
            assert!(lifecycle.get(method).is_some(), "{method} is listed");
        }
    }

    /// OL23 mirrors the four Object Lock operations in the document, and only a build carrying
    /// the routes lists them — the same gate as [`super::document`]'s merge.
    #[cfg(feature = "object-lock")]
    #[test]
    fn the_openapi_document_lists_the_four_object_lock_operations() {
        let schema = serde_json::to_value(super::document()).expect("the document is JSON");
        for (path, methods) in [
            ("/buckets/{bucket}/object-lock", ["get", "put"].as_slice()),
            (
                "/buckets/{bucket}/object-lock/objects",
                ["get", "put"].as_slice(),
            ),
        ] {
            let listed = schema["paths"].get(path).expect("the route is listed");
            for method in methods {
                assert!(listed.get(*method).is_some(), "{path} {method} is listed");
            }
        }
    }
}
