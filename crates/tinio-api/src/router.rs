//! The management router: `/status`, `/stop`, `/metrics`, `/openapi.json`, and the token rule.
//!
//! Which endpoints need the token follows the **bind address**, not the handler
//! (`management-api.md:9-15`, decision 8), and the rule is an **exception list on the open
//! side**: the open paths are named, every other *registered* path is guarded, and a request
//! that matches no route never reaches the middleware at all. That is why the guard below is a
//! **`route_layer`** and not a `layer` — a deliberate choice, not a typo: `route_layer` wraps
//! the routes registered before it and leaves the fallback routers unlayered
//! (`axum-0.8.9/src/routing/mod.rs:321-335`, dispatched at `:417-432`), while `Router::layer`
//! also wraps `fallback_router` (`:311-316`), whose built-in catch-all route answers every
//! unmatched path — so rewriting the call as `.layer(...)` would answer `401` without a token
//! for an unrouted request, instead of leaking nothing with the contract's plain `404`
//! (`management-api.md:50`). **Every route this crate serves is therefore registered before the
//! `.route_layer(...)` lines** — the invariant a new endpoint has to keep, and why every route
//! is registered inside `router()`, above them, rather than after them. On the local
//! channel the open paths are exactly the two read endpoints, `/metrics` and `/openapi.json`; a
//! TCP transport bound to a loopback address keeps **`/metrics`** open too — a scraper cannot
//! send a custom header, which is a specific reason and not a general trust in localhost —
//! while `/status`, `/stop`, `/openapi.json` and every other registered path still need the
//! token there; a non-loopback bind requires it on every endpoint, because a reachable host/port
//! otherwise leaks operational data. Because a standard scraper can send only
//! `Authorization: Bearer` and never a custom header, that header is an **alias** for
//! `X-Tinio-Token` on every transport — which is what makes
//! `management-api.md:15`'s rationale true rather than aspirational.

use std::{
    net::SocketAddr,
    sync::{Arc, LazyLock, OnceLock},
    time::Instant,
};

use axum::{
    Json, Router,
    extract::{Request, State as AxumState},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tracing::{Span, callsite::DefaultCallsite, dispatcher, field, instrument, metadata};
#[cfg(feature = "openapi")]
use utoipa::openapi::OpenApi as OpenApiSchema;

#[cfg(feature = "lifecycle")]
use crate::lifecycle::{self, LifecycleAdmin};
#[cfg(feature = "openapi")]
use crate::openapi::document;
use crate::{Error, metrics, state::State};

/// The token rule that applies to this plane.
///
/// # Examples
///
/// ```rust
/// use tinio_api::router::Auth;
///
/// assert!(Auth::Local.guards("/status"));
/// assert!(
///     !Auth::Local.guards("/metrics"),
///     "the local channel opens the read endpoints"
/// );
/// assert!(
///     !Auth::Loopback.guards("/metrics"),
///     "a scraper cannot send a custom header"
/// );
/// assert!(
///     Auth::Loopback.guards("/openapi.json"),
///     "the schema is not a scrape"
/// );
/// assert!(
///     Auth::Tcp.guards("/metrics"),
///     "a reachable port leaks operational data"
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Auth {
    /// The local channel: every registered path is guarded **except** `/metrics` and
    /// `/openapi.json`. The two are the rule's open side, so `/status`, `/stop` and any other
    /// route registered before the layer are guarded by default. What licenses that opening is
    /// the channel's privacy, and its one caveat is the unix socket's mode: `bind_unix` sets it
    /// to 0600 immediately after the bind, so with a permissive umask the socket file is briefly
    /// group- or world-connectable between `bind(2)` and that `chmod` — and a client that
    /// connected inside that window keeps the connection, because the mode is checked at connect
    /// and not per request. The state directory is expected to be private, which is what keeps
    /// the window unreachable in practice. Windows carries no such window: `pipe::create_pipe`
    /// installs no descriptor at all, so the pipe carries Windows' own default named-pipe
    /// descriptor from the instant it exists — the platform's own answer rather than one this
    /// crate narrows later — and what guards `/status` and `/stop` there is the access token.
    Local,
    /// A TCP transport bound to a loopback address: `/metrics` alone is open, because a
    /// scraper cannot send a custom header; everything else — `/status`, `/stop`,
    /// `/openapi.json` and any other registered path — is guarded. Deliberately narrower than
    /// [`Auth::Local`] — a loopback TCP port is reachable by every local user, which a
    /// mode-0600 unix socket is not.
    Loopback,
    /// A TCP transport bound to any other address: every endpoint is guarded.
    Tcp,
}

impl Auth {
    /// The rule for a TCP transport bound to `addr`. The rule follows the **bind** address:
    /// `0.0.0.0:9000` is not a loopback bind, whatever a client dials to reach it.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::net::SocketAddr;
    ///
    /// use tinio_api::router::Auth;
    ///
    /// let local = SocketAddr::from(([127, 0, 0, 1], 9000));
    /// assert_eq!(Auth::for_bind(local), Auth::Loopback);
    /// let reachable = SocketAddr::from(([0, 0, 0, 0], 9000));
    /// assert_eq!(Auth::for_bind(reachable), Auth::Tcp);
    /// ```
    pub fn for_bind(addr: SocketAddr) -> Self {
        if addr.ip().is_loopback() {
            Self::Loopback
        } else {
            Self::Tcp
        }
    }

    /// Whether `path` needs a token under this rule. Every arm is written as the **exception**
    /// list: a `false` is a deliberate opening, and every other registered path is guarded.
    /// An unmatched path never reaches this function — see the module doc.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_api::router::Auth;
    ///
    /// assert!(Auth::Loopback.guards("/stop"));
    /// assert!(!Auth::Loopback.guards("/metrics"));
    /// assert!(
    ///     Auth::Tcp.guards("/metrics"),
    ///     "every endpoint, on a reachable bind"
    /// );
    /// ```
    pub fn guards(self, path: &str) -> bool {
        self.guards_with(path, false)
    }

    /// [`Self::guards`], with the configured `/metrics` switch.
    pub fn guards_with(self, path: &str, metrics_requires_token: bool) -> bool {
        if metrics_requires_token && path == "/metrics" {
            return true;
        }
        match self {
            // The local channel: the two read endpoints are open, everything else that is
            // routed — `/status`, `/stop`, and any route registered before the layer — is
            // guarded (`management-api.md:11`).
            Self::Local => !matches!(path, "/metrics" | "/openapi.json"),
            // A loopback bind opens `/metrics` and nothing else: the scraper's inability to
            // send a custom header is the whole reason, and it does not extend to the API
            // schema, which a browser or a curl one-liner reads as easily as a scraper does.
            Self::Loopback => path != "/metrics",
            Self::Tcp => true,
        }
    }
}

/// What every handler shares.
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_api::{
///     router::{Auth, Context},
///     state::State,
/// };
/// use tokio::sync::watch;
///
/// let state = State::new(
///     "tinio-0123456789abcdef.sock",
///     Some(9000),
///     Path::new("/srv/data"),
/// )
/// .expect("state");
/// let (stop, _rx) = watch::channel(false);
/// let context = Context::new(state, "127.0.0.1", Auth::Local, stop);
/// assert_eq!(context.host, "127.0.0.1");
/// assert_eq!(context.auth, Auth::Local);
/// assert!(
///     context.port.get().is_none(),
///     "the port is published at readiness"
/// );
/// ```
#[derive(derive_more::Debug)]
pub struct Context {
    /// The state this plane published.
    pub state: State,
    /// The data plane's host, as `/status` reports it (`state` carries its port).
    pub host: String,
    /// The token rule for the transport it is served over.
    pub auth: Auth,
    /// The shutdown channel `/stop` flips. The CLI owns the channel and its receiving ends.
    pub stop: watch::Sender<bool>,
    /// The data plane's port, set once at the readiness point by `Plane::publish` (Task 5) —
    /// the same value `state` records — and reported as `None` by `/status` until then.
    pub port: Arc<OnceLock<u16>>,
    /// Require the control token for `/metrics` too (default `false`).
    pub metrics_requires_token: bool,
    /// The lifecycle admin the three `/buckets/{bucket}/lifecycle` routes are served from,
    /// attached at construction ([`Context::with_lifecycle`]). **`None` means those routes do
    /// not exist**: a build without the `lifecycle` feature has no admin to inject (LC15) and a
    /// run whose capability is off injects none (LC23), so the handlers answer `404` and nothing
    /// can replace a stored configuration.
    #[cfg(feature = "lifecycle")]
    #[debug("<injected>")]
    lifecycle: Option<Arc<dyn LifecycleAdmin>>,
}

impl Context {
    /// The context a plane serves: `host` is the data plane's host, and the port is published
    /// later (`Plane::publish` is the only writer of either it or `state.port`).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::path::Path;
    ///
    /// use tinio_api::{
    ///     router::{Auth, Context},
    ///     state::State,
    /// };
    /// use tokio::sync::watch;
    ///
    /// let state =
    ///     State::new("tinio-0123456789abcdef.sock", None, Path::new("/srv/data")).expect("state");
    /// let (stop, _rx) = watch::channel(false);
    /// let context = Context::new(state, "127.0.0.1", Auth::Loopback, stop);
    /// assert_eq!(context.state.port, None);
    /// assert_eq!(context.auth, Auth::Loopback);
    /// // Every context serves `/metrics`; the route gathers the process registry.
    /// ```
    pub fn new(
        state: State,
        host: impl Into<String>,
        auth: Auth,
        stop: watch::Sender<bool>,
    ) -> Self {
        Self {
            state,
            host: host.into(),
            auth,
            stop,
            port: Arc::new(OnceLock::new()),
            metrics_requires_token: false,
            #[cfg(feature = "lifecycle")]
            lifecycle: None,
        }
    }

    /// Require the control token for `/metrics` too.
    pub fn with_metrics_requires_token(mut self, required: bool) -> Self {
        self.metrics_requires_token = required;
        self
    }

    /// The same context with a lifecycle admin attached, or with none (LC14).
    ///
    /// **`None` is the whole feature-switch mechanism.** A build without the `lifecycle` feature
    /// does not register the three `/buckets/{bucket}/lifecycle` routes at all (LC15), and a run
    /// whose `[s3] lifecycle` is `false` injects no admin (LC23) — the handlers answer the
    /// contract's plain `404` either way, so a capability that is off cannot have its stored
    /// configurations replaced. The rules stay where they are, which is what makes turning the
    /// capability back on resume enforcement with the rules that were already there.
    ///
    /// The admin travels in at construction so a plane carries what it was built with.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{path::Path, sync::Arc};
    ///
    /// use async_trait::async_trait;
    /// use tinio_api::{
    ///     _core::{bucket, lifecycle, storage},
    ///     lifecycle::LifecycleAdmin,
    ///     router::{Auth, Context},
    ///     state::State,
    /// };
    /// use tokio::sync::watch;
    ///
    /// struct Admin;
    ///
    /// #[async_trait]
    /// impl LifecycleAdmin for Admin {
    ///     async fn get(
    ///         &self,
    ///         _bucket: &bucket::Name,
    ///     ) -> Result<Option<lifecycle::Config>, storage::Error> {
    ///         Ok(None)
    ///     }
    ///
    ///     async fn put(
    ///         &self,
    ///         _bucket: &bucket::Name,
    ///         _config: &lifecycle::Config,
    ///     ) -> Result<(), storage::Error> {
    ///         Ok(())
    ///     }
    ///
    ///     async fn delete(&self, _bucket: &bucket::Name) -> Result<(), storage::Error> {
    ///         Ok(())
    ///     }
    /// }
    ///
    /// let state =
    ///     State::new("tinio-0123456789abcdef.sock", None, Path::new("/srv/data")).expect("state");
    /// let (stop, _rx) = watch::channel(false);
    /// // The CLI's injection: `Some(admin)` serves the three routes. `with_lifecycle(None)`, or no
    /// // call at all, is the capability-off context — no routes (`404`), rules untouched.
    /// let context =
    ///     Context::new(state, "127.0.0.1", Auth::Local, stop).with_lifecycle(Some(Arc::new(Admin)));
    /// assert!(
    ///     format!("{context:?}").contains("<injected>"),
    ///     "the admin is opaque in `Debug`"
    /// );
    /// ```
    #[cfg(feature = "lifecycle")]
    pub fn with_lifecycle(mut self, admin: Option<Arc<dyn LifecycleAdmin>>) -> Self {
        self.lifecycle = admin;
        self
    }

    /// The attached admin, when there is one — the only door to a stored configuration, and
    /// `None` for both causes the doc on the field names. Crate-private: the admin is an input to
    /// construction, not something a caller patches in.
    #[cfg(feature = "lifecycle")]
    pub(crate) fn lifecycle(&self) -> Option<&Arc<dyn LifecycleAdmin>> {
        self.lifecycle.as_ref()
    }
}

/// The shared context, behind an `Arc` because every handler clones it.
///
/// # Examples
///
/// ```rust
/// use std::{path::Path, sync::Arc};
///
/// use tinio_api::{
///     router::{Auth, Context, Shared},
///     state::State,
/// };
/// use tokio::sync::watch;
///
/// let state =
///     State::new("tinio-0123456789abcdef.sock", None, Path::new("/srv/data")).expect("state");
/// let (stop, _rx) = watch::channel(false);
/// let shared: Shared = Arc::new(Context::new(state, "127.0.0.1", Auth::Local, stop));
/// assert_eq!(Arc::strong_count(&shared), 1);
/// ```
pub type Shared = Arc<Context>;

/// The `GET /status` body (`management-api.md:30-41`). The document publishes these fields as the
/// endpoint's schema under the `openapi` feature.
///
/// # Examples
///
/// ```rust
/// use tinio_api::router::StatusBody;
///
/// let body = StatusBody {
///     running: true,
///     pid: 1234,
///     host: "127.0.0.1".to_string(),
///     root: "/srv/data".to_string(),
///     port: None,
///     started_at: "2026-08-21T12:00:00Z".to_string(),
///     version: "0.1.0".to_string(),
/// };
/// let json = serde_json::to_string(&body).expect("json");
/// assert!(json.contains(r#""running":true"#), "{json}");
/// assert!(json.contains(r#""host":"127.0.0.1""#), "{json}");
/// ```
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusBody {
    /// Always true: a stopped server does not answer.
    pub running: bool,
    /// The serving pid.
    pub pid: u32,
    /// The data plane's host (`management-api.md:35`'s `"host": "127.0.0.1"`).
    pub host: String,
    /// The storage root, canonical.
    pub root: String,
    /// The data plane's port, when it is bound.
    pub port: Option<u16>,
    /// RFC3339, UTC.
    pub started_at: String,
    /// The serving build's version.
    pub version: String,
}

/// The `POST /stop` body. The document publishes these fields as the endpoint's schema under the
/// `openapi` feature.
///
/// # Examples
///
/// ```rust
/// use tinio_api::router::StopBody;
///
/// let body = StopBody {
///     status: "draining".to_string(),
/// };
/// let json = serde_json::to_string(&body).expect("json");
/// assert_eq!(json, r#"{"status":"draining"}"#);
/// ```
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopBody {
    /// Always `"draining"`.
    pub status: String,
}

/// The `PUT /buckets/{bucket}/lifecycle` refusal body: the model's own reason, and the offending
/// rule's ID when the rule carries one — **no S3 error code**, because this is the management
/// plane and not the S3 surface (LC8's two classes belong to the subresource, not here).
///
/// # Examples
///
/// ```rust
/// use tinio_api::router::InvalidBody;
///
/// let body = InvalidBody {
///     rule: Some("expire-logs".to_string()),
///     reason: "Filter carries exactly one predicate".to_string(),
/// };
/// let json = serde_json::to_string(&body).expect("json");
/// assert_eq!(
///     json,
///     r#"{"rule":"expire-logs","reason":"Filter carries exactly one predicate"}"#
/// );
/// ```
#[cfg(feature = "lifecycle")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvalidBody {
    /// The offending rule's ID, absent when the refusal is the configuration's own (an empty or
    /// over-long set) or its rule carries no ID.
    pub rule: Option<String>,
    /// Why it was refused, in the model's own words.
    pub reason: String,
}

/// Builds the router for `shared`.
///
/// **Every route goes before the `.route_layer(...)` lines** — why that ordering and that layer
/// kind are load-bearing is the module doc's rule, stated there once with its axum citations.
/// `/metrics` is registered here for it, and so is `/openapi.json` under the `openapi` feature:
/// it carries the API schema, which is exactly what the token exists to protect on a bind that
/// is not the local channel.
///
/// # Examples
///
/// ```rust
/// use std::{path::Path, sync::Arc};
///
/// use tinio_api::{
///     router::{Auth, Context, router},
///     state::State,
/// };
/// use tokio::sync::watch;
///
/// let state =
///     State::new("tinio-0123456789abcdef.sock", None, Path::new("/srv/data")).expect("state");
/// let (stop, _rx) = watch::channel(false);
/// let shared = Arc::new(Context::new(state, "127.0.0.1", Auth::Local, stop));
/// // `/status` and `/stop` are registered before the token layer, so both are guarded;
/// // `/metrics` is registered there too, and is open on the local channel.
/// let _app = router(shared);
/// ```
pub fn router(shared: Shared) -> Router {
    let routes = Router::new()
        .route("/status", get(status))
        .route("/stop", post(stop))
        // Registered above the token layer with the other two: the open-side rule can only be
        // discriminating because a route that reaches the middleware is a route that exists.
        .route("/metrics", get(metrics));
    // The three lifecycle routes are registered here **only** in a build that carries them, and
    // above the token layer like every other route, so the path rule covers them without a second
    // copy of it (LC14, LC15). A stripped build has no such path, and the fallback answers `404`.
    #[cfg(feature = "lifecycle")]
    let routes = lifecycle::mount(routes);
    // And the schema, likewise above the layer: the local channel serves it openly, every other
    // transport has to present the token — a hidden route would be served unchecked.
    #[cfg(feature = "openapi")]
    let routes = routes.route("/openapi.json", get(openapi_json));
    // The observation layer is outermost, so the 401s the token layer answers
    // and the `/metrics` scrape itself are counted; an unmatched path still
    // never reaches either layer (the module doc's `route_layer` rule).
    routes
        .route_layer(middleware::from_fn_with_state(shared.clone(), authorize))
        .route_layer(middleware::from_fn(observe_http))
        .with_state(shared)
}

/// One layer for the three HTTP families and the request's span: every routed management
/// request, including the refused one and the scrape itself. The span is the unit here — the
/// axum tower layers stay unspanned — and lives on [`api_request`]. `status` is recorded after
/// the response exists; the path is the request's, with no query.
async fn observe_http(request: Request, next: Next) -> Response {
    let start = Instant::now();
    let _in_flight = metrics::InFlightGuard::new();
    let method = request.method().clone();
    // The path exists only for the request span's field: a filtered-out span pays no allocation.
    let path = request_span_enabled().then(|| request.uri().path().to_owned());
    let response = api_request(method.as_str(), path, request, next).await;
    metrics::record_http_request(method.as_str(), response.status().as_u16(), start.elapsed());
    response
}

/// The static callsite of the request span's allocation gate: checked against the live
/// dispatcher, never `tracing::enabled!`'s frozen first appraisal.
static REQUEST_SPAN_CALLSITE: LazyLock<&'static DefaultCallsite> = LazyLock::new(|| {
    tracing::callsite! {
        name: "tinio.api.request",
        kind: metadata::Kind::SPAN,
        target: "tinio::api",
        level: tracing::Level::INFO,
        fields: (method, path, status),
    }
});

/// Whether the management request span is enabled — the gate on the per-request path allocation.
#[inline]
fn request_span_enabled() -> bool {
    use tracing::Callsite;
    dispatcher::get_default(|d| d.enabled(REQUEST_SPAN_CALLSITE.metadata()))
}

#[instrument(
    level = "info",
    target = "tinio::api",
    name = "tinio.api.request",
    skip_all,
    fields(method = %method, path = %path.as_deref().unwrap_or("-"), status = field::Empty, close_line = false)
)]
async fn api_request(method: &str, path: Option<String>, request: Request, next: Next) -> Response {
    let response = next.run(request).await;
    Span::current().record("status", u64::from(response.status().as_u16()));
    response
}

async fn authorize(
    AxumState(shared): AxumState<Shared>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if shared.auth.guards_with(path, shared.metrics_requires_token) {
        // `X-Tinio-Token`, or `Authorization: Bearer <token>` as an alias — the alias is what
        // lets a Prometheus scrape reach `/metrics` over TCP (`management-api.md:15`).
        let presented = headers
            .get("x-tinio-token")
            .and_then(|value| value.to_str().ok())
            .or_else(|| bearer(&headers))
            .unwrap_or_default();
        if !tokens_match(presented, shared.state.token.expose_secret().as_str()) {
            return Error::Unauthorized.into_response();
        }
    }
    next.run(request).await
}

/// Compares two tokens without leaking, through the comparison's own timing, *where* they
/// first differ — the length check is not a leak worth avoiding (the token is a fixed 64 hex
/// characters by construction, `state.rs`'s `TOKEN_BYTES`), and past it the loop touches every
/// byte of both sides whichever byte differs, so a token cannot be walked one byte at a time.
/// `==` on `&str` short-circuits at the first difference, which is the whole reason this helper
/// exists rather than a plain comparison: it is a few cycles on a path that runs once per
/// management request, and the alternative (`subtle`'s `ConstantTimeEq`, or comparing hashes
/// through it) buys the same property with a new dependency.
fn tokens_match(presented: &str, expected: &str) -> bool {
    let (presented, expected) = (presented.as_bytes(), expected.as_bytes());
    if presented.len() != expected.len() {
        return false;
    }
    presented
        .iter()
        .zip(expected)
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

/// The token from an `Authorization: Bearer <token>` header, when there is one.
fn bearer(headers: &HeaderMap) -> Option<&str> {
    let (scheme, token) = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())?
        .split_once(' ')?;
    scheme.eq_ignore_ascii_case("bearer").then_some(token)
}

async fn status(AxumState(shared): AxumState<Shared>) -> Json<StatusBody> {
    Json(StatusBody {
        running: true,
        pid: shared.state.pid,
        host: shared.host.clone(),
        root: shared.state.root.to_string_lossy().into_owned(),
        port: shared.port.get().copied(),
        started_at: shared.state.started_at.clone(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

// Never called: `#[utoipa::path]` needs an item of its own, and it sits beside the handler it
// mirrors so the two cannot drift apart — hence the deliberate `dead_code` `expect`. The `///` line
// below is the operation's `summary` in the document, which is why it describes the endpoint
// rather than this stub.

/// The running server's state: pid, host, storage root, port, start time and version.
#[cfg(feature = "openapi")]
#[expect(
    dead_code,
    reason = "the stub exists only so `#[utoipa::path]` has an item to attach to; it sits beside the handler it mirrors so the two cannot drift apart and is deliberately never called"
)]
#[utoipa::path(
    get,
    path = "/status",
    tag = "management",
    responses(
        (status = 200, description = "The server's state", body = StatusBody),
        (status = 401, description = "Missing or wrong token")
    ),
    security(("token" = []))
)]
fn status_doc() {}

async fn stop(AxumState(shared): AxumState<Shared>) -> (StatusCode, Json<StopBody>) {
    // A send failure means every receiver is gone, which is only possible while shutting
    // down — the answer the caller needs is the same either way.
    let _ = shared.stop.send(true);
    (
        StatusCode::ACCEPTED,
        Json(StopBody {
            status: "draining".to_string(),
        }),
    )
}

// Never called — `status_doc` carries why.

/// Cease accepting, drain in-flight requests for up to 10 s, then exit.
#[cfg(feature = "openapi")]
#[expect(
    dead_code,
    reason = "same shape as `status_doc`: a `#[utoipa::path]` stub, deliberately never called"
)]
#[utoipa::path(
    post,
    path = "/stop",
    tag = "management",
    responses(
        (status = 202, description = "Draining; the process then exits", body = StopBody),
        (status = 401, description = "Missing or wrong token")
    ),
    security(("token" = []))
)]
fn stop_doc() {}

/// `GET /metrics`: the Prometheus text exposition (`management-api.md:53-55`).
///
/// The whole handler is the registry gather and the encode ([crate::metrics::render]). No number
/// is refreshed here and no storage is read: the composition root's reader task keeps the
/// pipeline, write-lock and storage families fresh on its own `ttl`, so a scrape's cost is the
/// registry's size and a stalled scan cannot reach this request at all.
async fn metrics() -> Response {
    match metrics::render() {
        Ok(text) => (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
            text,
        )
            .into_response(),
        Err(err) => err.into_response(),
    }
}

// Never called — `status_doc` carries why. **No security requirement**: the endpoint's rule is the
// bind's, so a scraper's tokenless `GET` is what the document's prose describes rather than a
// requirement every caller has to satisfy.

/// The Prometheus text exposition, `text/plain; version=0.0.4; charset=utf-8`.
///
/// Every plane serves it: the route gathers the process registry, and no family is injected.
#[cfg(feature = "openapi")]
#[expect(
    dead_code,
    reason = "same shape as `status_doc`: a `#[utoipa::path]` stub, deliberately never called"
)]
#[utoipa::path(
    get,
    path = "/metrics",
    tag = "management",
    responses((status = 200, description = "The metric layers, in the Prometheus text format"))
)]
fn metrics_doc() {}

/// `GET /openapi.json`: the document (`management-api.md:59`), behind the `openapi` feature.
///
/// The route is registered above the token layer like the others, and the guard already carries
/// the path's rule — open on the local channel, token on a loopback bind and everywhere else.
///
/// The document has no runtime inputs, so it is **built once and held**: the cached schema is
/// what a request hands `Json`, which serializes it and sets `content-type: application/json` —
/// the same bytes a per-request rebuild would produce, without rebuilding.
#[cfg(feature = "openapi")]
async fn openapi_json() -> Json<&'static OpenApiSchema> {
    static DOCUMENT: OnceLock<OpenApiSchema> = OnceLock::new();
    Json(DOCUMENT.get_or_init(document))
}

// Never called — `status_doc` carries why. Registering it is what makes the document carry its own
// path rather than only the three it documents.

/// This OpenAPI document, as `application/json`.
#[cfg(feature = "openapi")]
#[expect(
    dead_code,
    reason = "same shape as `status_doc`: a `#[utoipa::path]` stub, deliberately never called"
)]
#[utoipa::path(
    get,
    path = "/openapi.json",
    tag = "management",
    responses((
        status = 200,
        description = "The management plane's specification"
    ))
)]
fn openapi_doc() {}

#[cfg(test)]
#[path = "router_tests.rs"]
mod tests;
