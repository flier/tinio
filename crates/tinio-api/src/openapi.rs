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

    Api::openapi()
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
