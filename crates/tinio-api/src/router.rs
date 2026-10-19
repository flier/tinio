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
//! `.route_layer(...)` line** — the invariant a new endpoint has to keep, and why every route is
//! registered inside `router()`, above that line, rather than after it. On the local
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
    sync::{Arc, OnceLock},
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
#[cfg(feature = "openapi")]
use utoipa::openapi::OpenApi as OpenApiSchema;

#[cfg(feature = "lifecycle")]
use crate::lifecycle::{self, LifecycleAdmin};
#[cfg(feature = "openapi")]
use crate::openapi::document;
use crate::{Error, ErrorBody, metrics::Gauges, state::State};

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
    /// The scrape-time gauge source `/metrics` gathers, attached at construction
    /// ([`Context::with_gauges`]) and read back through [`Context::gauges`]. Private for that
    /// reason: a plane carries what it was built with, so a source cannot be patched onto a
    /// context after the fact.
    gauges: Option<Arc<Gauges>>,
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
    /// // No source yet: a plane with no registry side has no `/metrics` at all
    /// // (`Context::with_gauges` is where one is attached).
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
            gauges: None,
            #[cfg(feature = "lifecycle")]
            lifecycle: None,
        }
    }

    /// The same context with a scrape-time gauge source attached, or with none.
    ///
    /// **`None` means the plane has no `/metrics`**: the handler answers the contract's plain
    /// `404` rather than an empty `200`, so the endpoint's absence is something a scraper can
    /// see (`management-api.md:23`). The source travels in at construction rather than being
    /// assigned to a built context — a plane carries what it was built with, and the reader
    /// task's input is fixed by the time [`Plane::bind`](crate::Plane::bind) returns.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{path::Path, sync::Arc};
    ///
    /// use tinio_api::{
    ///     metrics::{DEFAULT_TTL, Gauges, MetricsSource},
    ///     router::{Auth, Context},
    ///     state::State,
    /// };
    /// use tokio::sync::watch;
    ///
    /// struct Source;
    ///
    /// impl MetricsSource for Source {
    ///     fn refresh(&self) {}
    ///
    ///     fn counts(&self) {}
    /// }
    ///
    /// let state =
    ///     State::new("tinio-0123456789abcdef.sock", None, Path::new("/srv/data")).expect("state");
    /// let (stop, _rx) = watch::channel(false);
    /// let gauges = Arc::new(Gauges::new(Arc::new(Source), DEFAULT_TTL));
    /// // A context built with a source serves `/metrics`; `with_gauges(None)` is the one a
    /// // caller with no registry side builds.
    /// let context = Context::new(state, "127.0.0.1", Auth::Local, stop).with_gauges(Some(gauges));
    /// assert_eq!(context.auth, Auth::Local);
    /// ```
    pub fn with_gauges(mut self, gauges: Option<Arc<Gauges>>) -> Self {
        self.gauges = gauges;
        self
    }

    /// The attached gauge source, when there is one — what the `/metrics` handler serves from
    /// and what [`Plane::serve`](crate::Plane::serve) starts the reader task for. Crate-private:
    /// the source is an input to construction, not something a caller patches in or reads out.
    pub(crate) fn gauges(&self) -> Option<&Arc<Gauges>> {
        self.gauges.as_ref()
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
    /// The admin travels in at construction for [`Context::with_gauges`]'s reason: a plane
    /// carries what it was built with.
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
    ///     "the admin is opaque in `Debug`, as the gauge source is"
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
/// **Every route goes before the `.route_layer(...)` line** — why that ordering and that layer
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
    routes
        .route_layer(middleware::from_fn_with_state(shared.clone(), authorize))
        .with_state(shared)
}

async fn authorize(
    AxumState(shared): AxumState<Shared>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if shared.auth.guards(path) {
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
async fn metrics(AxumState(shared): AxumState<Shared>) -> Response {
    // Deliberate and contractual: a plane whose source was never attached has no `/metrics`,
    // so a scraper sees the endpoint's absence rather than an empty 200 (decision 8's
    // round-2 ruling — keep the 404, change the contract). The body is this crate's error shape
    // (`ErrorBody`, `{"error": "..."}`) like every other failure the plane answers, so a caller
    // parses one format; the message keeps the sentence that says what is missing.
    let Some(gauges) = shared.gauges() else {
        return (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: "metrics are not attached".to_string(),
            }),
        )
            .into_response();
    };
    // Cheap half only, and no `await`: the storage scan is the reader task's (`spawn_reader`),
    // so this handler never touches the backend and never scans — it refreshes the pipeline
    // families the caller's adapter republishes and gathers the registry the reader has
    // already filled. A scrape's cost is therefore independent of the root's size and of how
    // often the endpoint is hit.
    gauges.refresh();
    match gauges.render() {
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
/// `404` when the plane carries no gauge source: the absence is served, not an empty `200`.
#[cfg(feature = "openapi")]
#[expect(
    dead_code,
    reason = "same shape as `status_doc`: a `#[utoipa::path]` stub, deliberately never called"
)]
#[utoipa::path(
    get,
    path = "/metrics",
    tag = "management",
    responses(
        (status = 200, description = "The three metric layers, in the Prometheus text format"),
        (status = 404, description = "The plane carries no gauge source")
    )
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
