//! The hyper data plane (task T051).
//!
//! Hosts the s3s [`S3Service`] over hyper/hyper-util: a tower middleware
//! opens the `tinio.http.request` admission span (target `tinio::http`),
//! emits the access-log event inside it (`tinio::access` target, T052)
//! and counts the upload/download bytes on the streaming bodies (T054).
//! The HTTP families belong to the management plane (`tinio-api`), not
//! this listener.
//! The service itself is the s3s service over a stack of decorators: the
//! [`MetricS3`] metrics/access-log wrapper, the write policy
//! ([`S3ReadonlyBackend`], read-only planes only), the capability gate
//! ([`CapabilityS3`]), and [`S3Backend`] at the bottom.
//!
//! Graceful shutdown: `serve` stops accepting connections when the
//! shutdown channel turns true; in-flight connections are spawned tasks
//! that drain naturally (bounded by the OS connection handling).

use std::{
    error::Error as StdError,
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc, LazyLock, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Instant,
};

#[cfg(feature = "cors")]
use http::header::ORIGIN;
use http::{
    Request, Response,
    header::{CONTENT_LENGTH, REFERER, USER_AGENT},
};
use hyper::{
    body::{Bytes, Incoming},
    service::Service,
};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
};
use io::Error as IoError;
use s3s::{
    Body as S3Body,
    auth::SecretKey,
    service::{S3Service, S3ServiceBuilder},
};
#[cfg(not(any(feature = "acl", feature = "copy")))]
use s3s::{auth::S3Auth, s3_error};
use time::{OffsetDateTime, format_description, format_description::BorrowedFormatItem};
use tokio::{net::TcpListener, sync::watch};
use tower::Service as TowerService;
use tracing::{callsite::DefaultCallsite, dispatcher, instrument, metadata};

#[cfg(feature = "acl")]
use crate::_auth::AclAccess;
#[cfg(any(feature = "acl", feature = "copy"))]
use crate::_auth::{ConfigAuth, Identity, User, derive_canonical_id};
#[cfg(any(feature = "acl", feature = "copy"))]
use crate::_core::acl::{DEFAULT_OWNER_DISPLAY_NAME, default_owner_id};
#[cfg(feature = "cors")]
use crate::backend::cors::{self, apply_cors_headers, bucket_from_uri};
use crate::{
    _core::{AccessMode, storage::Storage},
    backend::{Capabilities, S3Backend},
    capability::CapabilityS3,
    error::Error,
    log::{ACCESS_TARGET, AccessField, AccessFields},
    metrics::{self, MetricS3},
    readonly::S3ReadonlyBackend,
};

/// The scrape-time metrics refresh (F49): the hook refreshes the
/// scrape-computed families (`tinio_server::metrics::refresh` — the CLI's
/// reader tick wires it to the pipelines' `Stats` and the storage's
/// write-lock snapshot) so that whoever gathers the registry finds them
/// current.
///
/// **The scrape lives on the management plane** (`tinio-api`'s
/// token-gated `/metrics`), which the CLI's reader tick drives: this hook is
/// what that tick's cheap half calls. Nothing in
/// this file serves the path — the data-plane listener answers `/metrics`
/// as the bucket named `metrics`, like any other first path segment (the
/// interim unauthenticated data-plane route was removed when the
/// management plane's endpoint landed, F10).
///
/// # Examples
///
/// ```rust
/// use tinio_server::MetricsRefresh;
///
/// // The shape the CLI's reader tick holds.
/// let refresh: MetricsRefresh = std::sync::Arc::new(|| {});
/// refresh();
/// ```
pub type MetricsRefresh = Arc<dyn Fn() + Send + Sync>;

/// The static pair's provider: [`ConfigAuth`] over a one-user identity map,
/// so the credential lookup — its `tinio::auth` span, its `result` field
/// and its metric — is the acl plane's and runs once. The owner element is
/// the built-in default: this path carries no identity in the backend.
#[cfg(any(feature = "acl", feature = "copy"))]
fn static_auth(access_key: &str, secret_key: &str) -> ConfigAuth {
    let user = User {
        access_key: access_key.to_owned(),
        canonical_id: derive_canonical_id(access_key),
        display_name: DEFAULT_OWNER_DISPLAY_NAME.to_owned(),
        secret: SecretKey::from(secret_key),
    };
    ConfigAuth::new(Arc::new(Identity::new(
        [(access_key.to_owned(), user)].into_iter().collect(),
        default_owner_id(),
        DEFAULT_OWNER_DISPLAY_NAME.to_owned(),
    )))
}

/// The pair's provider in a bare build — the only one with no `tinio-auth`
/// edge, so the only one where no shared lookup exists to reuse. Unlike
/// `SimpleAuth` (which answers `NotSignedUp`), an unknown access key
/// answers AWS's `InvalidAccessKeyId` — the code aws cli and the FR-008
/// interop scenario expect for a wrong key.
#[cfg(not(any(feature = "acl", feature = "copy")))]
struct StaticAuth {
    access_key: String,
    secret_key: SecretKey,
}

#[cfg(not(any(feature = "acl", feature = "copy")))]
#[async_trait::async_trait]
impl S3Auth for StaticAuth {
    #[tracing::instrument(
        level = "info",
        target = "tinio::auth",
        skip_all,
        fields(result = tracing::field::Empty)
    )]
    async fn get_secret_key(&self, access_key: &str) -> s3s::S3Result<SecretKey> {
        let found = access_key == self.access_key;
        tracing::Span::current().record("result", if found { "found" } else { "unknown" });
        if found {
            Ok(self.secret_key.clone())
        } else {
            Err(s3_error!(
                InvalidAccessKeyId,
                "The AWS Access Key Id you provided does not exist in our records."
            ))
        }
    }
}

/// The bare build's provider: the same pair, compared in place.
#[cfg(not(any(feature = "acl", feature = "copy")))]
fn static_auth(access_key: &str, secret_key: &str) -> StaticAuth {
    StaticAuth {
        access_key: access_key.to_owned(),
        secret_key: SecretKey::from(secret_key),
    }
}

/// A data plane's construction inputs: the `[s3]` capability toggles and the
/// write policy. `From<Capabilities>` yields read-write, so existing call
/// sites pass a bare `Capabilities` unchanged and a read-only caller opts in
/// with [`PlaneConfig::read_only`].
#[derive(Debug, Clone, Copy)]
pub struct PlaneConfig {
    /// The `[s3]` capability toggles (FR-021).
    pub caps: Capabilities,
    /// The write policy (FR-023).
    pub access: AccessMode,
}

impl PlaneConfig {
    /// A read-only plane over `caps`.
    pub fn read_only(caps: Capabilities) -> Self {
        Self {
            caps,
            access: AccessMode::ReadOnly,
        }
    }

    /// Whether this plane refuses mutating operations.
    pub fn is_read_only(&self) -> bool {
        self.access.is_read_only()
    }
}

impl From<Capabilities> for PlaneConfig {
    fn from(caps: Capabilities) -> Self {
        Self {
            caps,
            access: AccessMode::ReadWrite,
        }
    }
}

/// The data plane: the s3s service behind the metrics/access middleware.
pub struct DataPlane {
    service: Arc<DataPlaneService>,
}

impl DataPlane {
    /// Build the data plane over a storage backend.
    pub fn new<S: Storage>(storage: S, plane: impl Into<PlaneConfig>) -> Self {
        Self::new_shared(Arc::new(storage), plane)
    }

    /// Build the data plane over a SHARED storage handle: the backend and
    /// the CORS preflight route (when armed) share the one `Arc<S>`.
    ///
    /// UNGATED: it exists in every build (feature-off builds keep a
    /// constructor via [`DataPlane::new`]); only the cors wiring is
    /// `#[cfg(feature = "cors")]`.
    pub fn new_shared<S: Storage>(storage: Arc<S>, plane: impl Into<PlaneConfig>) -> Self {
        let plane = plane.into();
        let backend = S3Backend::new_shared(Arc::clone(&storage), plane.caps);
        #[cfg(feature = "cors")]
        {
            let mut builder = Self::service_builder(plane, backend);
            // Double gate: the compile-time feature AND the runtime
            // capability arm the preflight route; the erased lookup rides
            // on the service either way (None with the capability off — no
            // route, no decoration).
            if let Some(lookup) = Self::cors_lookup(&storage, &plane.caps) {
                builder.set_route(cors::PreflightRoute::new(Arc::clone(&lookup)));
                return Self::from_service(builder.build(), Some(lookup));
            }
            Self::from_service(builder.build(), None)
        }
        #[cfg(not(feature = "cors"))]
        Self::from_service(Self::service_builder(plane, backend).build())
    }

    /// Build the data plane with a single static credential pair (SigV4).
    ///
    /// Unsigned requests stay allowed only when no auth provider is set; with
    /// one set, s3s requires signed requests to verify against these
    /// credentials. The provider is the acl plane's `ConfigAuth` over a
    /// one-user identity, so this constructor reuses that lookup — span,
    /// metric and refusal included — instead of a second one.
    pub fn new_with_auth<S: Storage>(
        storage: S,
        plane: impl Into<PlaneConfig>,
        access_key: &str,
        secret_key: &str,
    ) -> Self {
        let plane = plane.into();
        let storage = Arc::new(storage);
        let backend = S3Backend::new_shared(Arc::clone(&storage), plane.caps);
        let mut builder = Self::service_builder(plane, backend);
        builder.set_auth(static_auth(access_key, secret_key));
        #[cfg(feature = "cors")]
        {
            if let Some(lookup) = Self::cors_lookup(&storage, &plane.caps) {
                builder.set_route(cors::PreflightRoute::new(Arc::clone(&lookup)));
                return Self::from_service(builder.build(), Some(lookup));
            }
            Self::from_service(builder.build(), None)
        }
        #[cfg(not(feature = "cors"))]
        Self::from_service(builder.build())
    }

    /// The double-gate CORS lookup (feature is compile-gated; the runtime
    /// capability arms the route and the decoration together).
    #[cfg(feature = "cors")]
    fn cors_lookup<S: Storage>(
        storage: &Arc<S>,
        caps: &Capabilities,
    ) -> Option<Arc<dyn cors::Lookup>> {
        let configs = Arc::new(cors::Configs::new(Arc::clone(storage)));
        caps.cors.then_some(configs as Arc<dyn cors::Lookup>)
    }

    /// Build the data plane with the identity-backed authn/authz stack
    /// (feature `acl`): [`ConfigAuth`] serves the configured users'
    /// secrets, [`AclAccess`] runs the pre-route authorization pipeline.
    /// `DataPlane::new` (legacy) and `new_with_auth` remain feature-off
    /// constructions — no identity, no enforced behavior.
    ///
    /// The enforced-mode condition: `new_with_acl` is selected exactly
    /// when the feature is compiled on AND `caps.acl` runs — the same
    /// condition that gates every handler-side enforced check (Tasks
    /// 10-12), so a wired identity never exists without `AclAccess`.
    #[cfg(feature = "acl")]
    pub fn new_with_acl<S: Storage>(
        storage: S,
        plane: impl Into<PlaneConfig>,
        identity: Arc<Identity>,
    ) -> Self {
        let plane = plane.into();
        let storage = Arc::new(storage);
        let backend = S3Backend::new_shared(Arc::clone(&storage), plane.caps)
            .with_identity(Arc::clone(&identity));
        let mut builder = Self::service_builder(plane, backend);
        builder.set_auth(ConfigAuth::new(Arc::clone(&identity)));
        builder.set_access(AclAccess::new(Arc::clone(&storage), identity));
        #[cfg(feature = "cors")]
        {
            if let Some(lookup) = Self::cors_lookup(&storage, &plane.caps) {
                builder.set_route(cors::PreflightRoute::new(Arc::clone(&lookup)));
                return Self::from_service(builder.build(), Some(lookup));
            }
            Self::from_service(builder.build(), None)
        }
        #[cfg(not(feature = "cors"))]
        Self::from_service(builder.build())
    }

    /// The s3s service builder over `backend`. The layers nest in order,
    /// from the inside out: the capability gate, then the write policy —
    /// a read-only plane refuses mutating operations *inside* the
    /// [`MetricS3`] wrapper, so a refusal is still counted and logged —
    /// and the metrics wrapper outermost.
    ///
    /// The two policies build different chain types, so the fork lives
    /// here, where s3s's type-erased builder is the one meeting point.
    fn service_builder<S: Storage>(plane: PlaneConfig, backend: S3Backend<S>) -> S3ServiceBuilder {
        let gated = CapabilityS3::new(backend, plane.caps);
        if plane.is_read_only() {
            S3ServiceBuilder::new(MetricS3::new(S3ReadonlyBackend::new(gated)))
        } else {
            S3ServiceBuilder::new(MetricS3::new(gated))
        }
    }

    #[cfg(feature = "cors")]
    fn from_service(service: S3Service, cors: Option<Arc<dyn cors::Lookup>>) -> Self {
        Self {
            service: Arc::new(DataPlaneService::new(service, cors)),
        }
    }

    #[cfg(not(feature = "cors"))]
    fn from_service(service: S3Service) -> Self {
        Self {
            service: Arc::new(DataPlaneService::new(service)),
        }
    }

    /// Serve `listener` until the shutdown channel turns true.
    pub async fn serve(
        self,
        listener: TcpListener,
        shutdown: watch::Receiver<bool>,
    ) -> Result<(), Error> {
        tracing::info!("S3 data plane listening on {}", listener.local_addr()?);
        let mut shutdown = shutdown;
        loop {
            let (stream, peer) = tokio::select! {
                _ = shutdown.changed() => break,
                accepted = listener.accept() => accepted?,
            };
            let service = (*self.service).clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let service = WithPeer { service, peer };
                if let Err(err) = Builder::new(TokioExecutor::new())
                    .serve_connection(io, service)
                    .await
                {
                    tracing::warn!(error = %err, "connection error");
                }
            });
        }
        tracing::info!("S3 data plane stopped accepting connections");
        Ok(())
    }
}

/// The boxed future every service call returns.
type ServiceFuture = Pin<
    Box<
        dyn Future<Output = Result<Response<CountingBody<S3Body>>, Box<dyn StdError + Send + Sync>>>
            + Send,
    >,
>;

/// Attach the peer address to the service (the middleware needs it for the
/// access log).
#[derive(Clone)]
struct WithPeer {
    service: DataPlaneService,
    peer: SocketAddr,
}

impl Service<Request<Incoming>> for WithPeer {
    type Error = Box<dyn StdError + Send + Sync>;
    type Future = ServiceFuture;
    type Response = Response<CountingBody<S3Body>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        // Every request reaches the S3 service, `/metrics` included: the
        // scrape endpoint lives on the management plane, so the path is
        // routed as the bucket named `metrics` like any other first path
        // segment (F10 — no request is intercepted here any more).
        self.service.call_with_peer(req, self.peer)
    }
}

/// Per-process monotonic id recorded on the admission span; one per
/// request, cheap enough for the hot path, and never formatted unless a
/// subscriber is listening.
static REQUEST_ID: AtomicU64 = AtomicU64::new(1);

#[inline]
fn next_request_id() -> u64 {
    REQUEST_ID.fetch_add(1, Ordering::Relaxed)
}

/// The tower middleware over the s3s service: the request admission span,
/// access-log events and byte counters.
#[derive(Clone)]
pub struct DataPlaneService {
    inner: S3Service,
    /// The CORS config lookup (feature + capability double-gated; `None`
    /// when the preflight route is not armed) — read by the Task-10
    /// decoration on every non-preflight response.
    #[cfg(feature = "cors")]
    cors: Option<Arc<dyn cors::Lookup>>,
}

impl DataPlaneService {
    #[cfg(feature = "cors")]
    fn new(inner: S3Service, cors: Option<Arc<dyn cors::Lookup>>) -> Self {
        Self { inner, cors }
    }

    #[cfg(not(feature = "cors"))]
    fn new(inner: S3Service) -> Self {
        Self { inner }
    }

    fn call_with_peer(&self, req: Request<Incoming>, peer: SocketAddr) -> ServiceFuture {
        let start = Instant::now();

        let method = req.method().as_str().to_string();
        // The path exists only for the admission span's field: a filtered-out span pays no
        // allocation (the access line derives its own path from the URI).
        let path = request_span_enabled().then(|| req.uri().path().to_string());
        let request_id = next_request_id();
        // The admission span: `tower::Service::call` is the unit, so an
        // `#[instrument]` helper wraps the service future (the method itself
        // would close before the first poll). The access event below is emitted
        // inside it, so the access line shares the trace; the access gate and
        // the counting bodies stay unspanned.
        // The access-log fields are built only when a subscriber listens
        // on the `tinio::access` target — tracing would filter the event,
        // but the strings (and the `nginx_time` clock read) would still be
        // allocated on every request (T052).
        let access_log = access_log_enabled().then(|| {
            let request = request_line(&method, req.uri());
            let user_agent = req
                .headers()
                .get(USER_AGENT)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("-")
                .to_string();
            let referer = strip_query(
                req.headers()
                    .get(REFERER)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("-"),
            )
            .to_string();
            (peer.ip().to_string(), request, referer, user_agent)
        });

        // The Task-10 CORS decoration inputs, captured BEFORE the request
        // is consumed: the Origin header and the URI (the decoration runs
        // after the inner service answers, against the stored values).
        #[cfg(feature = "cors")]
        let origin = req
            .headers()
            .get(ORIGIN)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        // The URI is only needed when the decoration can run (an Origin
        // header AND an armed lookup) — requests from boto3/aws-cli carry
        // no Origin and pay no clone.
        #[cfg(feature = "cors")]
        let uri = (origin.is_some() && self.cors.is_some()).then(|| req.uri().clone());

        // Count upload bytes on the request body.
        let (parts, body) = req.into_parts();
        let upload_counter = Arc::new(AtomicU64::new(0));
        let counted = CountingBody::new(body, Arc::clone(&upload_counter), CountingKind::Upload);
        let req = Request::from_parts(parts, counted);

        // The tower impl of `S3Service` accepts any body type (the
        // inherent `call` takes `s3s::Body` only, and hyper's impl is
        // exact-body `Incoming`); clone per request — `S3Service` is an
        // `Arc` internally.
        let mut service = self.inner.clone();
        // The decoration runs inside the 'static service future, which
        // cannot borrow `&self` — the erased lookup is cloned (Arc).
        #[cfg(feature = "cors")]
        let cors = self.cors.clone();
        let future = TowerService::call(&mut service, req);
        Box::pin(http_request(
            start,
            RequestTrace {
                method,
                path,
                request_id,
                access_log,
            },
            upload_counter,
            future,
            #[cfg(feature = "cors")]
            cors,
            #[cfg(feature = "cors")]
            origin,
            #[cfg(feature = "cors")]
            uri,
        ))
    }
}

type DataPlaneResponse = Result<Response<CountingBody<S3Body>>, Box<dyn StdError + Send + Sync>>;

/// The request identity the admission span and the access line share.
struct RequestTrace {
    method: String,
    /// The path for the admission span's field; `None` when the span is filtered out.
    path: Option<String>,
    request_id: u64,
    access_log: Option<(String, String, String, String)>,
}

#[instrument(
    level = "info",
    target = "tinio::http",
    name = "tinio.http.request",
    skip_all,
    fields(method = %trace.method, path = %trace.path.as_deref().unwrap_or("-"), request_id = trace.request_id, close_line = false)
)]
async fn http_request<F>(
    start: Instant,
    trace: RequestTrace,
    upload_counter: Arc<AtomicU64>,
    work: F,
    #[cfg(feature = "cors")] cors: Option<Arc<dyn cors::Lookup>>,
    #[cfg(feature = "cors")] origin: Option<String>,
    #[cfg(feature = "cors")] uri: Option<http::Uri>,
) -> DataPlaneResponse
where
    F: Future<
            Output = Result<
                Response<S3Body>,
                <S3Service as TowerService<Request<CountingBody<Incoming>>>>::Error,
            >,
        > + Send,
{
    // Its `Debug` form, boxed: s3s 0.17 made `HttpError` an `Error` (with
    // `into_std_error` to unwrap it without the extra box), so the cheaper
    // spelling exists — the text this records is deliberately unchanged.
    let result = work
        .await
        .map_err(|e| IoError::other(format!("{e:?}")).into());
    let elapsed = start.elapsed();
    let upload_bytes = upload_counter.load(Ordering::Relaxed);
    metrics::record_upload_bytes(upload_bytes);

    // One access-log record per request: the status and body bytes
    // come from the response, 500/0 for transport failures.
    let (status, body_bytes, result) = match result {
        Ok(response) => {
            let status = response.status().as_u16();
            // Task-10 CORS decoration of the ACTUAL response (the
            // preflight route answers OPTIONS itself; this covers
            // every other operation): the Access-Control-* headers
            // of the rule that matches the request's Origin +
            // method — `rule_for` is first-origin-match with the
            // method validated WITHIN that rule (no fall-through).
            // s3s encodes op errors as Ok(Response) bodies, so 4xx
            // XML answers are decorated too — matches AWS. The
            // lookup is `None` unless feature + capability are on,
            // which short-circuits everything below. (The mut
            // shadow is cfg'd: feature-off builds leave the
            // binding plain.)
            #[cfg(feature = "cors")]
            let mut response = response;
            #[cfg(feature = "cors")]
            // OPTIONS is skipped outright: the preflight route
            // answers it, and no rule may allow OPTIONS — so
            // `rule_for` cannot match and the per-request config
            // read would be wasted on every preflight.
            if trace.method != "OPTIONS"
                && let (Some(cors), Some(origin)) = (cors.as_deref(), origin.as_deref())
                && let Some(uri) = uri.as_ref()
                && let Some(bucket) = bucket_from_uri(uri)
                && let Some(config) = cors.get(&bucket).await
                && let Some(rule) = config.rule_for(origin, &trace.method)
            {
                // The shared decoration (the preflight route's
                // common header set — the two must never answer
                // differently; the Q11/S1 semantics live there).
                apply_cors_headers(response.headers_mut(), rule, origin);
            }
            // Count download bytes on the response body (recorded
            // when the stream ends); the access log uses the
            // response Content-Length (known upfront).
            let (parts, body) = response.into_parts();
            let download_counter = Arc::new(AtomicU64::new(0));
            let counted =
                CountingBody::new(body, Arc::clone(&download_counter), CountingKind::Download);
            let response = Response::from_parts(parts, counted);
            let body_bytes = response
                .headers()
                .get(CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            (status, body_bytes, Ok(response))
        }
        Err(err) => (500, 0, Err(err)),
    };
    if let Some((remote_addr, request, referer, user_agent)) = trace.access_log {
        let fields = AccessFields::new(
            remote_addr,
            "-".to_string(),
            nginx_time(),
            request,
            status,
            body_bytes,
            referer,
            user_agent,
            format!("{:.3}", elapsed.as_secs_f64()),
        );
        emit_access(&fields);
    }
    result
}

/// The static callsite of the admission span's allocation gate — see
/// [`request_span_enabled`]. Checked against the live dispatcher for the same
/// reason as [`ACCESS_LOG_CALLSITE`].
static REQUEST_SPAN_CALLSITE: LazyLock<&'static DefaultCallsite> = LazyLock::new(|| {
    tracing::callsite! {
        name: "tinio.http.request",
        kind: metadata::Kind::SPAN,
        target: "tinio::http",
        level: tracing::Level::INFO,
        fields: (method, path, request_id),
    }
});

/// Whether the admission span is enabled — the gate on the per-request path
/// allocation. The check follows the CURRENT dispatcher rather than
/// [`tracing::enabled!`]'s frozen first appraisal (see [`ACCESS_LOG_CALLSITE`]).
#[inline]
fn request_span_enabled() -> bool {
    use tracing::Callsite;
    dispatcher::get_default(|d| d.enabled(REQUEST_SPAN_CALLSITE.metadata()))
}

/// The static callsite of the access-log gate — see
/// [`access_log_enabled`]. A dedicated callsite (via the hidden
/// [`tracing::callsite!`] macro) because [`tracing::enabled!`]'s
/// static-callsite interest is cached process-globally on the FIRST
/// appraisal, which freezes it to "never" when a request is served
/// before any listener (exactly the pipeline tests' WARN-filtered
/// global): the gate must follow the LIVE dispatcher, not the process's
/// first appraisal.
static ACCESS_LOG_CALLSITE: LazyLock<&'static DefaultCallsite> = LazyLock::new(|| {
    tracing::callsite! {
        name: "tinio_access_log_gate",
        kind: metadata::Kind::EVENT,
        target: ACCESS_TARGET,
        level: tracing::Level::INFO,
        fields: (),
    }
});

/// Whether any subscriber listens on the access target — the T052
/// allocation gate, checked against the CURRENT dispatcher (thread-local
/// included), immune to the frozen callsite-interest of the
/// [`tracing::enabled!`] macro (see [`ACCESS_LOG_CALLSITE`]).
#[inline]
fn access_log_enabled() -> bool {
    use tracing::Callsite;
    dispatcher::get_default(|d| d.enabled(ACCESS_LOG_CALLSITE.metadata()))
}

/// Which direction a counting body streams (the metric recorded at the
/// end of the stream).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CountingKind {
    Upload,
    Download,
}

/// A body wrapper counting streamed bytes; the count lands in the metric
/// when the stream ends (downloads) or is read by the middleware after the
/// service returns (uploads).
pub struct CountingBody<B> {
    inner: Pin<Box<B>>,
    counter: Arc<AtomicU64>,
    kind: CountingKind,
    /// Whether the download bytes were already recorded into the metric
    /// (the stream ended normally, or the drop recorded them).
    recorded: bool,
}

impl<B> CountingBody<B> {
    fn new(inner: B, counter: Arc<AtomicU64>, kind: CountingKind) -> Self {
        Self {
            inner: Box::pin(inner),
            counter,
            kind,
            recorded: false,
        }
    }

    /// Record the download bytes into the metric, once (idempotent).
    fn record_download(&mut self) {
        if self.kind == CountingKind::Download && !self.recorded {
            self.recorded = true;
            let n = self.counter.load(Ordering::Relaxed);
            metrics::record_download_bytes(n);
        }
    }
}

impl<B> Drop for CountingBody<B> {
    fn drop(&mut self) {
        // A stream dropped mid-flight (client disconnect, transport
        // error) must still record the bytes already streamed — the
        // poll path alone would lose them.
        self.record_download();
    }
}

impl<B> http_body::Body for CountingBody<B>
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
{
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        match self.inner.as_mut().poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    self.counter.fetch_add(data.len() as u64, Ordering::Relaxed);
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => {
                self.record_download();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

/// Emit one access-log event on the `tinio::access` target (T052), with
/// the field names of the shared [`AccessField`] schema.
fn emit_access(fields: &AccessFields) {
    tracing::info!(
        target: ACCESS_TARGET,
        remote_addr = %fields.get(AccessField::RemoteAddr),
        remote_user = %fields.get(AccessField::RemoteUser),
        time_local = %fields.get(AccessField::TimeLocal),
        request = %fields.get(AccessField::Request),
        status = %fields.get(AccessField::Status),
        body_bytes_sent = %fields.get(AccessField::BodyBytesSent),
        http_referer = %fields.get(AccessField::HttpReferer),
        http_user_agent = %fields.get(AccessField::HttpUserAgent),
        request_time = %fields.get(AccessField::RequestTime),
        "s3 request completed"
    );
}

/// Nginx-style `time_local` (`23/Aug/2026:12:00:00 +0000`).
fn nginx_time() -> String {
    // The format description is static — parse it once, not per request.
    static FORMAT: OnceLock<Vec<BorrowedFormatItem<'static>>> = OnceLock::new();
    #[expect(
        clippy::expect_used,
        reason = "literal: the input is this call site's compile-time literal, so a refusal is a source-level mistake, not a runtime condition — and `nginx_time_is_formatted_like_nginx` formats a timestamp through this very `OnceLock`, so a malformed description fails that test (design 2026-10-09 D2a)"
    )]
    let format = FORMAT.get_or_init(|| {
        format_description::parse_borrowed::<2>(
            "[day]/[month repr:short]/[year]:[hour]:[minute]:[second] +0000",
        )
        .expect("static format")
    });
    let now = OffsetDateTime::now_utc();
    now.format(format).unwrap_or_else(|_| "-".into())
}

/// `$request` is method + path + protocol — never the query string
/// (FR-017: access logs must not contain presigned credentials).
fn request_line(method: &str, uri: &http::Uri) -> String {
    format!("{method} {} HTTP/1.1", uri.path())
}

/// Drop `?query` from a header value (Referer may carry a presigned URL).
fn strip_query(value: &str) -> &str {
    value.split_once('?').map(|(head, _)| head).unwrap_or(value)
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, io, thread, time::Duration};

    use futures::task;
    use http_body::{Body as _, Frame};
    #[cfg(any(feature = "acl", feature = "copy"))]
    use prometheus::default_registry;
    use tinio_util::testing::spans;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        runtime,
        task::JoinHandle,
        time::sleep,
    };
    use tracing::subscriber::with_default;
    use tracing_subscriber::prelude::*;

    use super::*;
    #[cfg(feature = "cors")]
    use crate::_core::cors;
    use crate::{
        _core::{
            acl::Acl,
            bucket, object,
            storage::{BucketOps, ObjectOps},
        },
        _mem::MemoryStorage,
        _util::testing::body,
    };

    /// One raw HTTP/1.1 request over a fresh connection (`Connection:
    /// close`); the response body is read to EOF.
    async fn raw_request(addr: SocketAddr, request: &str) -> (u16, Vec<u8>) {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let status = String::from_utf8_lossy(&buf)
            .split_whitespace()
            .nth(1)
            .unwrap_or("000")
            .parse()
            .unwrap_or(0);
        (status, buf)
    }

    async fn spawn_plane() -> (SocketAddr, watch::Sender<bool>, JoinHandle<()>) {
        let storage = MemoryStorage::new().unwrap();
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown, rx) = watch::channel(false);
        let plane = DataPlane::new(storage, Capabilities::default());
        let handle = tokio::spawn(async move {
            plane.serve(listener, rx).await.unwrap();
        });
        (addr, shutdown, handle)
    }

    /// A minimal [`http_body::Body`] serving in-memory chunks.
    struct VecBody {
        chunks: VecDeque<Bytes>,
    }

    impl VecBody {
        fn new(chunks: &[&'static [u8]]) -> Self {
            Self {
                chunks: chunks.iter().map(|c| Bytes::from_static(c)).collect(),
            }
        }
    }

    impl http_body::Body for VecBody {
        type Data = Bytes;
        type Error = io::Error;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            match self.chunks.pop_front() {
                Some(data) => Poll::Ready(Some(Ok(Frame::data(data)))),
                None => Poll::Ready(None),
            }
        }
    }

    fn cx() -> Context<'static> {
        Context::from_waker(task::noop_waker_ref())
    }

    fn drain(body: &mut CountingBody<VecBody>) -> u64 {
        let mut pin = Pin::new(body);
        while let Poll::Ready(Some(Ok(_))) = pin.as_mut().poll_frame(&mut cx()) {}
        pin.counter.load(Ordering::Relaxed)
    }

    #[test]
    fn request_line_drops_query_string() {
        let uri: http::Uri = "/bucket/key?X-Amz-Signature=secret&X-Amz-Credential=AKID"
            .parse()
            .unwrap();
        assert_eq!(request_line("GET", &uri), "GET /bucket/key HTTP/1.1");
    }

    #[test]
    fn strip_query_drops_presigned_referer() {
        assert_eq!(
            strip_query("https://example/x?X-Amz-Signature=s"),
            "https://example/x"
        );
        assert_eq!(strip_query("-"), "-");
    }

    #[test]
    fn nginx_time_is_formatted_like_nginx() {
        // `23/Aug/2026:12:00:00 +0000` — fixed 26-char shape.
        let t = nginx_time();
        assert_eq!(t.len(), 26, "{t}");
        assert_eq!(&t[2..3], "/");
        assert_eq!(&t[6..7], "/");
        assert_eq!(&t[11..12], ":");
        assert!(t.ends_with(" +0000"), "{t}");
    }

    #[test]
    fn capabilities_convert_to_read_write_plane() {
        let plane: PlaneConfig = Capabilities::default().into();
        assert!(!plane.is_read_only());
        assert!(PlaneConfig::read_only(Capabilities::default()).is_read_only());
    }

    #[test]
    fn counting_body_records_download_bytes_exactly_once() {
        // Scenarios are serialized inside one test — the gauge is global,
        // and parallel tests would clobber each other's deltas. The
        // process-wide metric window (metrics::test_lock) additionally
        // excludes the OTHER tests' data-plane writes for the window.
        let _window = metrics::test_lock::window();
        // (1) A fully-drained download records its total once.
        let before = metrics::STORAGE_DOWNLOAD_BYTES.get();
        let counter = Arc::new(AtomicU64::new(0));
        let mut body = CountingBody::new(
            VecBody::new(&[b"he", b"llo", b" world"]),
            Arc::clone(&counter),
            CountingKind::Download,
        );
        assert_eq!(drain(&mut body), 11);
        assert_eq!(metrics::STORAGE_DOWNLOAD_BYTES.get(), before + 11);
        // The Drop must not record the same stream twice.
        drop(body);
        assert_eq!(metrics::STORAGE_DOWNLOAD_BYTES.get(), before + 11);

        // (2) A stream dropped mid-flight records what already flowed.
        let before = metrics::STORAGE_DOWNLOAD_BYTES.get();
        let counter = Arc::new(AtomicU64::new(0));
        let body = CountingBody::new(
            VecBody::new(&[b"abc", b"def"]),
            counter.clone(),
            CountingKind::Download,
        );
        let mut pin = Box::pin(body);
        let _ = pin.as_mut().poll_frame(&mut cx());
        drop(pin); // client disconnect — the drop path records the 3 bytes
        assert_eq!(counter.load(Ordering::Relaxed), 3);
        assert_eq!(metrics::STORAGE_DOWNLOAD_BYTES.get(), before + 3);

        // (3) An upload body never touches the download metric.
        let before = metrics::STORAGE_DOWNLOAD_BYTES.get();
        let counter = Arc::new(AtomicU64::new(0));
        let mut body = CountingBody::new(
            VecBody::new(&[b"payload"]),
            counter.clone(),
            CountingKind::Upload,
        );
        assert_eq!(drain(&mut body), 7);
        assert_eq!(metrics::STORAGE_DOWNLOAD_BYTES.get(), before);
    }

    #[tokio::test]
    async fn serve_stops_on_shutdown_and_warns_on_a_bad_connection() {
        // The accept loop: a garbage connection is served and dropped
        // (the connection-error warn fires, the loop keeps going), and
        // the shutdown signal ends the loop cleanly with `Ok`.
        let (addr, shutdown, handle) = spawn_plane().await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"NOT-HTTP-AT-ALL\r\n\r\n").await.unwrap();
        drop(stream);
        // Let hyper fail the connection so the warn path runs.
        sleep(Duration::from_millis(100)).await;

        shutdown.send(true).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn auth_plane_rejects_unsigned_requests() {
        // `new_with_auth` configures the SigV4 static credential pair:
        // an unsigned request must be refused (s3s `NotSignedUp` → 403),
        // so interop clients cannot bypass the configured keys.
        let storage = MemoryStorage::new().unwrap();
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown, rx) = watch::channel(false);
        let plane = DataPlane::new_with_auth(storage, Capabilities::default(), "AKID", "secret");
        tokio::spawn(async move {
            plane.serve(listener, rx).await.unwrap();
        });

        let (status, body) = raw_request(
            addr,
            "PUT /bucket HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert_eq!(status, 403, "unsigned request must be refused");
        let body_text = String::from_utf8_lossy(&body);
        assert!(
            body_text.contains("AccessDenied") && body_text.contains("Signature is required"),
            "{body_text}"
        );
        shutdown.send(true).unwrap();
    }

    #[cfg(any(feature = "acl", feature = "copy"))]
    #[tokio::test]
    async fn the_static_pair_answers_through_the_shared_lookup() {
        // `new_with_auth` serves its pair through the acl plane's
        // `ConfigAuth`: an unknown key is the same refusal and books the
        // shared family, so no request performs a second, uninstrumented
        // lookup.
        use s3s::auth::S3Auth;

        let before = credential_lookups("unknown");
        let auth = static_auth("AKID", "secret");
        let err = auth.get_secret_key("WHO").await.err().unwrap();
        assert_eq!(err.code().as_str(), "InvalidAccessKeyId");
        assert!(
            credential_lookups("unknown") > before,
            "the shared credential-lookup family must move"
        );
        let secret = auth.get_secret_key("AKID").await.unwrap();
        assert_eq!(secret.expose(), "secret");
    }

    /// The shared credential-lookup counter at `result`, read out of the
    /// default registry.
    #[cfg(any(feature = "acl", feature = "copy"))]
    fn credential_lookups(result: &str) -> f64 {
        default_registry()
            .gather()
            .into_iter()
            .find(|family| family.name() == "tinio_auth_credential_lookups_total")
            .and_then(|family| {
                family
                    .get_metric()
                    .iter()
                    .find(|metric| {
                        metric
                            .get_label()
                            .iter()
                            .any(|label| label.name() == "result" && label.value() == result)
                    })
                    .map(|metric| metric.get_counter().value())
            })
            .unwrap_or(0.0)
    }

    #[cfg(feature = "cors")]
    #[tokio::test]
    async fn options_preflight_answered_on_the_plane() {
        use crate::_core::acl::Acl;
        // The preflight route on the real data plane: seed the mem storage
        // with a CORS config (via the storage handle), spawn the plane
        // (`Capabilities::default` → cors on), then a raw browser OPTIONS
        // (Origin + Access-Control-Request-Method) is answered 200 with the
        // allow headers — and a bare OPTIONS (no Origin) still falls
        // through to s3s: 501 unknown operation (the old behavior).
        let storage = MemoryStorage::new().unwrap();
        let name = bucket::name("data").unwrap();
        storage
            .create_bucket(&name, None, &Acl::default_private(None))
            .await
            .unwrap();
        storage
            .put_bucket_cors(
                &name,
                &cors::Config {
                    rules: vec![cors::Rule {
                        id: Some("allow".into()),
                        allowed_methods: vec!["PUT".into()],
                        allowed_origins: vec!["https://example.com".into()],
                        allowed_headers: None,
                        expose_headers: None,
                        max_age_seconds: Some(300),
                    }],
                },
            )
            .await
            .unwrap();

        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown, rx) = watch::channel(false);
        let plane = DataPlane::new(storage, Capabilities::default());
        let handle = tokio::spawn(async move {
            plane.serve(listener, rx).await.unwrap();
        });

        let (status, body) = raw_request(
            addr,
            "OPTIONS /data/key HTTP/1.1\r\nHost: localhost\r\nOrigin: https://example.com\r\nAccess-Control-Request-Method: PUT\r\nConnection: close\r\n\r\n",
        )
        .await;
        let text = String::from_utf8_lossy(&body).to_lowercase();
        assert_eq!(status, 200, "{text}");
        assert!(
            text.contains("access-control-allow-origin: https://example.com"),
            "{text}"
        );
        assert!(text.contains("access-control-allow-methods: put"), "{text}");

        // A bare OPTIONS (non-browser probe): the route's `is_match` falls
        // through to s3s — 501 unknown operation, the pre-route behavior.
        let (status, body) = raw_request(
            addr,
            "OPTIONS /data/key HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert_eq!(status, 501, "{}", String::from_utf8_lossy(&body));

        shutdown.send(true).unwrap();
        handle.await.unwrap();
    }

    #[cfg(feature = "cors")]
    #[tokio::test]
    async fn preflight_invalid_bucket_name_is_403_from_the_route() {
        // s3s 0.15 parsed the path in `prepare` (AwsNameValidation) BEFORE
        // route matching — an invalid bucket (uppercase) was a 400
        // InvalidBucketName and this route never ran. s3s matches the
        // custom route FIRST and lets it claim the request: a matched route
        // skips path classification, so `PreflightRoute` answers — 403
        // AccessDenied, the CORS mismatch message, never a 400. The route
        // re-checks the name itself (`bucket_from_uri` → `parse_path_style`,
        // the same AwsNameValidation): "MyBucket" fails that parse, the
        // denial site here.
        let (addr, shutdown, handle) = spawn_plane().await;
        let (status, body) = raw_request(
            addr,
            "OPTIONS /MyBucket/key HTTP/1.1\r\nHost: localhost\r\nOrigin: https://example.com\r\nAccess-Control-Request-Method: GET\r\nConnection: close\r\n\r\n",
        )
        .await;
        let text = String::from_utf8_lossy(&body).to_lowercase();
        assert_eq!(status, 403, "{text}");
        assert!(text.contains("<code>accessdenied</code>"), "{text}");
        assert!(
            text.contains("corsresponse: this cors request is not allowed"),
            "{text}"
        );
        // The pre-0.16 answer is gone: the 400 never reaches the wire.
        assert!(!text.contains("invalidbucketname"), "{text}");
        // A denial is bare — no CORS decoration (denials never echo ACAO).
        assert!(!text.contains("access-control-allow-origin"), "{text}");
        shutdown.send(true).unwrap();
        handle.await.unwrap();
    }

    /// A plane over a fresh MemoryStorage seeded with the bucket "data"
    /// (plus the object "key" carrying "payload") and the CORS config,
    /// served on an ephemeral port with the cors capability on.
    #[cfg(feature = "cors")]
    async fn seeded_cors_plane(
        config: cors::Config,
    ) -> (SocketAddr, watch::Sender<bool>, JoinHandle<()>) {
        use crate::_core::acl::Acl;
        let storage = MemoryStorage::new().unwrap();
        let name = bucket::name("data").unwrap();
        storage
            .create_bucket(&name, None, &Acl::default_private(None))
            .await
            .unwrap();
        storage
            .put_object(&name, &object::key("key").unwrap(), body(b"payload"))
            .await
            .unwrap();
        storage.put_bucket_cors(&name, &config).await.unwrap();
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown, rx) = watch::channel(false);
        let plane = DataPlane::new(storage, Capabilities::default());
        let handle = tokio::spawn(async move {
            plane.serve(listener, rx).await.unwrap();
        });
        (addr, shutdown, handle)
    }

    #[cfg(feature = "cors")]
    #[tokio::test]
    async fn get_with_matching_origin_is_decorated() {
        // The first plane's config is ONE concrete rule (GET +
        // https://example.com + expose ETag): a matching origin sees the
        // full header set, a foreign origin and a no-Origin request see
        // NONE, and a 404 (missing object) is still decorated (s3s encodes
        // op errors as Ok(Response) bodies — matches AWS).
        let (addr, shutdown, handle) = seeded_cors_plane(cors::Config {
            rules: vec![cors::Rule {
                id: Some("allow-example".into()),
                allowed_methods: vec!["GET".into()],
                allowed_origins: vec!["https://example.com".into()],
                allowed_headers: None,
                expose_headers: Some(vec!["ETag".into()]),
                max_age_seconds: None,
            }],
        })
        .await;

        // Matching origin → 200 + ACAO echo, the rule's methods, the
        // rule's expose list, allow-credentials true, and the Vary trio.
        let (status, body) = raw_request(
            addr,
            "GET /data/key HTTP/1.1\r\nHost: localhost\r\nOrigin: https://example.com\r\nConnection: close\r\n\r\n",
        )
        .await;
        let text = String::from_utf8_lossy(&body).to_lowercase();
        assert_eq!(status, 200, "{text}");
        assert!(
            text.contains("access-control-allow-origin: https://example.com"),
            "{text}"
        );
        assert!(text.contains("access-control-allow-methods: get"), "{text}");
        assert!(
            text.contains("access-control-expose-headers: etag"),
            "{text}"
        );
        assert!(
            text.contains("access-control-allow-credentials: true"),
            "{text}"
        );
        // grilling Q4: the Vary trio is APPENDed (the wire carries three
        // Vary lines).
        assert!(text.contains("vary: origin"), "{text}");
        assert!(
            text.contains("vary: access-control-request-headers"),
            "{text}"
        );
        assert!(
            text.contains("vary: access-control-request-method"),
            "{text}"
        );

        // A foreign origin, and no Origin at all → NO access-control-*.
        for origin in [Some("https://evil.com"), None] {
            let request = match origin {
                Some(origin) => format!(
                    "GET /data/key HTTP/1.1\r\nHost: localhost\r\nOrigin: {origin}\r\nConnection: close\r\n\r\n"
                ),
                None => "GET /data/key HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
                    .to_string(),
            };
            let (status, body) = raw_request(addr, &request).await;
            let text = String::from_utf8_lossy(&body).to_lowercase();
            assert_eq!(status, 200, "{text}");
            assert!(
                !text.contains("access-control-"),
                "no CORS headers for {origin:?}: {text}"
            );
        }

        // A MISSING object → 404 + still decorated with the rule's ACAO.
        let (status, body) = raw_request(
            addr,
            "GET /data/missing HTTP/1.1\r\nHost: localhost\r\nOrigin: https://example.com\r\nConnection: close\r\n\r\n",
        )
        .await;
        let text = String::from_utf8_lossy(&body).to_lowercase();
        assert_eq!(status, 404, "{text}");
        assert!(
            text.contains("access-control-allow-origin: https://example.com"),
            "{text}"
        );
        shutdown.send(true).unwrap();
        handle.await.unwrap();

        // Q11: a bare-"*"-origin rule decorates with ACAO "*" and OMITS
        // Allow-Credentials.
        let (addr, shutdown, handle) = seeded_cors_plane(cors::Config {
            rules: vec![cors::Rule {
                id: None,
                allowed_methods: vec!["GET".into()],
                allowed_origins: vec!["*".into()],
                allowed_headers: None,
                expose_headers: None,
                max_age_seconds: None,
            }],
        })
        .await;
        let (status, body) = raw_request(
            addr,
            "GET /data/key HTTP/1.1\r\nHost: localhost\r\nOrigin: https://any.example.com\r\nConnection: close\r\n\r\n",
        )
        .await;
        let text = String::from_utf8_lossy(&body).to_lowercase();
        assert_eq!(status, 200, "{text}");
        assert!(text.contains("access-control-allow-origin: *"), "{text}");
        assert!(!text.contains("access-control-allow-credentials"), "{text}");
        shutdown.send(true).unwrap();
        handle.await.unwrap();
    }

    #[cfg(feature = "cors")]
    #[tokio::test]
    async fn get_with_origin_match_but_method_mismatch_is_not_decorated() {
        // B5/S3 pin — first-origin-match applies to the DECORATION too:
        // rule1 claims https://example.com (GET only); a PUT from that
        // origin must NOT be decorated — rule1's origin matches, its
        // method check fails, and the decorator never falls through to
        // rule2 (which allows PUT for "*").
        let (addr, shutdown, handle) = seeded_cors_plane(cors::Config {
            rules: vec![
                cors::Rule {
                    id: Some("r1".into()),
                    allowed_methods: vec!["GET".into()],
                    allowed_origins: vec!["https://example.com".into()],
                    allowed_headers: None,
                    expose_headers: None,
                    max_age_seconds: None,
                },
                cors::Rule {
                    id: Some("r2".into()),
                    allowed_methods: vec!["PUT".into()],
                    allowed_origins: vec!["*".into()],
                    allowed_headers: None,
                    expose_headers: None,
                    max_age_seconds: None,
                },
            ],
        })
        .await;

        // Sanity: the same origin's GET IS decorated by rule1.
        let (status, body) = raw_request(
            addr,
            "GET /data/key HTTP/1.1\r\nHost: localhost\r\nOrigin: https://example.com\r\nConnection: close\r\n\r\n",
        )
        .await;
        let text = String::from_utf8_lossy(&body).to_lowercase();
        assert_eq!(status, 200, "{text}");
        assert!(
            text.contains("access-control-allow-origin: https://example.com"),
            "{text}"
        );

        // PUT from that origin → no access-control-* headers (rule1's
        // origin matched, method PUT disallowed, no rule2 fall-through).
        let (status, body) = raw_request(
            addr,
            "PUT /data/key HTTP/1.1\r\nHost: localhost\r\nOrigin: https://example.com\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc",
        )
        .await;
        let text = String::from_utf8_lossy(&body).to_lowercase();
        assert_eq!(status, 200, "{text}");
        assert!(!text.contains("access-control-"), "{text}");

        // ... whereas a PUT from a DIFFERENT origin (no rule claims it)
        // is decorated by rule2 — proof the pin is the first-origin rule,
        // not method blocking overall.
        let (status, body) = raw_request(
            addr,
            "PUT /data/key HTTP/1.1\r\nHost: localhost\r\nOrigin: https://any.example.com\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc",
        )
        .await;
        let text = String::from_utf8_lossy(&body).to_lowercase();
        assert_eq!(status, 200, "{text}");
        assert!(text.contains("access-control-allow-origin: *"), "{text}");
        assert!(text.contains("access-control-allow-methods: put"), "{text}");
        shutdown.send(true).unwrap();
        handle.await.unwrap();
    }

    #[cfg(feature = "acl")]
    mod acl {
        use std::sync::Arc;

        use bytes::Bytes;
        use futures::stream;
        use sha2::{Digest, Sha256};
        use time::OffsetDateTime;

        use super::*;
        use crate::{
            _auth::{
                derive_canonical_id,
                identity::{Identity, User},
            },
            _core::{
                acl::{Acl, GROUP_ALL_USERS, Grant, Grantee, GroupUri, OwnerId, Permission},
                bucket, object,
                storage::{BucketOps, ObjectOps},
            },
            _mem::MemoryStorage,
        };

        /// RFC 2104 HMAC-SHA256 (SigV4's derivation needs it; the test
        /// signer skips dedicating the `hmac` crate).
        fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
            let mut key_vec = key.to_vec();
            if key_vec.len() > 64 {
                key_vec = Sha256::digest(&key_vec).to_vec();
            }
            key_vec.resize(64, 0);
            let mut inner = Vec::with_capacity(64);
            let mut outer = Vec::with_capacity(64);
            for byte in &key_vec {
                inner.push(byte ^ 0x36);
                outer.push(byte ^ 0x5c);
            }
            let inner_hash = Sha256::digest([inner.as_slice(), data].concat());
            Sha256::digest([outer.as_slice(), inner_hash.as_slice()].concat()).into()
        }

        /// An `AuthorizationV4` header over an empty-body request
        /// (S3-specific: `x-amz-content-sha256` is required and computes
        /// over the payload). Mirrors the canonicalization s3s verifies
        /// in `ops/signature.rs`.
        #[expect(
            clippy::too_many_arguments,
            reason = "the SigV4 canonical request's inputs — access key, secret, method, path, host, the two dates and the payload hash — taken positionally because the test helper mirrors `ops/signature.rs`'s canonicalization"
        )]
        fn authorization(
            access_key: &str,
            secret: &str,
            method: &str,
            path: &str,
            host: &str,
            date8: &str,
            amz_date: &str,
            payload_hash: &str,
        ) -> String {
            let signed_headers = "host;x-amz-content-sha256;x-amz-date";
            let canonical_request = format!(
                "{method}\n{path}\n\nhost:{host}\n\
                 x-amz-content-sha256:{payload_hash}\n\
                 x-amz-date:{amz_date}\n\n\
                 {signed_headers}\n\
                 {payload_hash}"
            );
            let string_to_sign = format!(
                "AWS4-HMAC-SHA256\n{amz_date}\n{date8}/us-east-1/s3/aws4_request\n{}",
                hex::encode(Sha256::digest(canonical_request.as_bytes()))
            );
            let mut key = b"AWS4".to_vec();
            key.extend_from_slice(secret.as_bytes());
            for part in [date8.as_bytes(), b"us-east-1", b"s3", b"aws4_request"] {
                key = hmac_sha256(&key, part).to_vec();
            }
            let signature = hex::encode(hmac_sha256(&key, string_to_sign.as_bytes()));
            format!(
                "AWS4-HMAC-SHA256 Credential={access_key}/{date8}/us-east-1/s3/aws4_request, \
                 SignedHeaders={signed_headers}, Signature={signature}"
            )
        }

        /// A signed raw request (header-auth SigV4, empty body).
        async fn signed_raw_request(
            addr: SocketAddr,
            method: &str,
            path: &str,
            access_key: &str,
            secret: &str,
        ) -> (u16, Vec<u8>) {
            let now = OffsetDateTime::now_utc();
            let date8 = format!(
                "{:04}{:02}{:02}",
                now.year(),
                u8::from(now.month()),
                now.day()
            );
            let amz_date = format!(
                "{date8}T{:02}{:02}{:02}Z",
                now.hour(),
                now.minute(),
                now.second()
            );
            let payload_hash = hex::encode(Sha256::digest(b""));
            let host = "localhost";
            let auth = authorization(
                access_key,
                secret,
                method,
                path,
                host,
                &date8,
                &amz_date,
                &payload_hash,
            );
            let request = format!(
                "{method} {path} HTTP/1.1\r\nHost: {host}\r\n\
                 x-amz-date: {amz_date}\r\n\
                 x-amz-content-sha256: {payload_hash}\r\n\
                 Authorization: {auth}\r\n\
                 Connection: close\r\n\r\n"
            );
            raw_request(addr, &request).await
        }

        /// The two-user fixture: alice (the owner) and bob (the grantee).
        fn identity_pair() -> (Identity, OwnerId, OwnerId) {
            let alice_id = derive_canonical_id("AKID");
            let bob_id = derive_canonical_id("BKID");
            (
                Identity::test(vec![
                    User::test("AKID", "alice-secret", "alice"),
                    User::test("BKID", "bob-secret", "bob"),
                ]),
                alice_id,
                bob_id,
            )
        }

        /// One 4-byte body object in the fixture bucket.
        async fn put(storage: &MemoryStorage, key: &str, owner: &OwnerId, acl: &Acl) {
            let b = bucket::name("data").unwrap();
            let k = object::key(key).unwrap();
            let staged = storage
                .stage_body(
                    &b,
                    &k,
                    Box::pin(stream::iter([Ok::<Bytes, io::Error>(Bytes::from_static(
                        b"data",
                    ))])),
                    None,
                    None,
                )
                .await
                .unwrap();
            storage
                .commit_object(&b, &k, staged, object::Tags::empty(), Some(owner), acl)
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn acl_plane_two_user_access_control() {
            // The enforced-mode equivalence (controller carry; Task 11's
            // same condition): the access layer and the handler-side
            // checks are wired together under `caps.acl ∧ identity` —
            // unsigned GET passes a public-read object, 403s a private
            // one; a signed user's grants apply through the real plane.
            let storage = MemoryStorage::new().unwrap();
            let b = bucket::name("data").unwrap();
            let (identity, alice_id, bob_id) = identity_pair();
            storage
                .create_bucket(
                    &b,
                    Some(&alice_id),
                    &Acl::default_private(Some(alice_id.clone())),
                )
                .await
                .unwrap();
            let public = Acl {
                owner: Some(alice_id.clone()),
                grants: vec![Grant {
                    grantee: Grantee::Group(GroupUri(GROUP_ALL_USERS.to_string())),
                    permission: Permission::Read,
                }],
            };
            put(&storage, "public.txt", &alice_id, &public).await;
            put(
                &storage,
                "private.txt",
                &alice_id,
                &Acl::default_private(Some(alice_id.clone())),
            )
            .await;
            put(
                &storage,
                "bob-granted.txt",
                &alice_id,
                &Acl {
                    owner: Some(alice_id.clone()),
                    grants: vec![Grant {
                        grantee: Grantee::Canonical(bob_id.clone()),
                        permission: Permission::Read,
                    }],
                },
            )
            .await;

            let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                .await
                .unwrap();
            let addr = listener.local_addr().unwrap();
            let (shutdown, rx) = watch::channel(false);
            let plane =
                DataPlane::new_with_acl(storage, Capabilities::default(), Arc::new(identity));
            tokio::spawn(async move {
                plane.serve(listener, rx).await.unwrap();
            });

            // Unsigned: the public-read grant applies, the private one 403s.
            let (status, _) = raw_request(
                addr,
                "GET /data/public.txt HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            )
            .await;
            assert_eq!(
                status, 200,
                "unsigned GET on the public object must succeed"
            );
            let (status, body) = raw_request(
                addr,
                "GET /data/private.txt HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            )
            .await;
            assert_eq!(
                status, 403,
                "unsigned GET on the private object must be denied"
            );
            assert!(
                String::from_utf8_lossy(&body).contains("AccessDenied"),
                "{body:?}"
            );

            // Signed bob: his grants apply (and only his).
            let (status, _) =
                signed_raw_request(addr, "GET", "/data/bob-granted.txt", "BKID", "bob-secret")
                    .await;
            assert_eq!(status, 200, "signed bob's READ grant must apply");
            let (status, _) =
                signed_raw_request(addr, "GET", "/data/private.txt", "BKID", "bob-secret").await;
            assert_eq!(status, 403, "signed bob must not read a grant-less object");
            // The owner bypass holds for alice.
            let (status, _) =
                signed_raw_request(addr, "GET", "/data/private.txt", "AKID", "alice-secret").await;
            assert_eq!(status, 200, "the object owner bypasses the ACL");

            shutdown.send(true).unwrap();
        }
    }

    #[test]
    fn access_log_records_nginx_fields_under_a_subscriber() {
        // With a subscriber on the `tinio::access` target the middleware
        // builds the full field set (peer, request line, referer stripped
        // of its query, user agent) and emits the nginx-shaped record
        // (T052). Without a subscriber the strings are never allocated.
        //
        // The connection is served INLINE on the block_on thread (a
        // `tokio::spawn`ed task would not inherit the `with_default`
        // thread-local dispatcher); the raw client runs on a std thread.
        //
        // CURRENT-THREAD runtimes on both sides: a multi-thread runtime
        // can move the hyper connection task off the block_on thread
        // (the platform work-steals it to a worker), which would emit
        // the access-log event where the thread-local dispatch of
        // `with_default` does not reach — the capture then comes back
        // empty under parallel load. The current-thread executor pins
        // every task to the capture thread (same rationale as the INLINE
        // comment above).
        let capture = spans::Capture::new();
        let capture2 = capture.clone();
        with_default(tracing_subscriber::registry().with(capture), || {
            runtime::Builder::new_current_thread()
                .enable_io()
                .build()
                .unwrap()
                .block_on(async {
                let storage = MemoryStorage::new().unwrap();
                let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                    .await
                    .unwrap();
                let addr = listener.local_addr().unwrap();
                let plane = DataPlane::new(storage, Capabilities::default());

                let client = thread::spawn(move || {
                    runtime::Builder::new_current_thread()
                            .enable_io()
                            .build()
                            .unwrap()
                            .block_on(raw_request(
                        addr,
                        "GET /missing-bucket/key HTTP/1.1\r\nHost: localhost\r\nUser-Agent: test-agent/1.0\r\nReferer: https://example.com/x?token=secret\r\nConnection: close\r\n\r\n",
                    ))
                });

                let (stream, peer) = listener.accept().await.unwrap();
                let io = TokioIo::new(stream);
                let service = WithPeer {
                    service: (*plane.service).clone(),
                    peer,
                };
                Builder::new(TokioExecutor::new())
                    .serve_connection(io, service)
                    .await
                    .unwrap();
                let (status, _) = client.join().unwrap();
                assert_eq!(status, 404, "the request completes against the plane");
            });
        });

        let events = capture2.events();
        let access: Vec<_> = events
            .iter()
            .filter(|event| event.target == ACCESS_TARGET)
            .collect();
        assert_eq!(access.len(), 1, "{events:?}");
        let fields = access[0];
        assert_eq!(fields.field("remote_addr"), Some("127.0.0.1"));
        assert_eq!(
            fields.field("request"),
            Some("GET /missing-bucket/key HTTP/1.1")
        );
        assert_eq!(fields.field("status"), Some("404"));
        assert_eq!(fields.field("http_user_agent"), Some("test-agent/1.0"));
        // The presigned referer query is stripped (FR-017).
        assert_eq!(fields.field("http_referer"), Some("https://example.com/x"));
        assert!(fields.field("time_local").is_some());
        assert!(fields.field("request_time").is_some());
    }

    #[test]
    fn admission_span_wraps_each_request_and_its_access_event() {
        // One `tinio::http` span per request with the method, the path
        // (query stripped) and a request id; the access event is emitted
        // inside that span. A 404 and a 200 both produce exactly one span.
        let capture = spans::Capture::new();
        let capture2 = capture.clone();
        with_default(tracing_subscriber::registry().with(capture), || {
            runtime::Builder::new_current_thread()
                .enable_io()
                .build()
                .unwrap()
                .block_on(async {
                    let storage = MemoryStorage::new().unwrap();
                    let name = bucket::name("data").unwrap();
                    storage
                        .create_bucket(&name, None, &Acl::default_private(None))
                        .await
                        .unwrap();
                    storage
                        .put_object(
                            &name,
                            &object::key("key").unwrap(),
                            body(b"payload"),
                        )
                        .await
                        .unwrap();
                    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                        .await
                        .unwrap();
                    let addr = listener.local_addr().unwrap();
                    let plane = DataPlane::new(storage, Capabilities::default());

                    for (request, expected) in [
                        (
                            "GET /missing-bucket/key?x=1 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                            404,
                        ),
                        (
                            "GET /data/key HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                            200,
                        ),
                    ] {
                        let client = thread::spawn(move || {
                            runtime::Builder::new_current_thread()
                                .enable_io()
                                .build()
                                .unwrap()
                                .block_on(raw_request(addr, request))
                        });
                        let (stream, peer) = listener.accept().await.unwrap();
                        let io = TokioIo::new(stream);
                        let service = WithPeer {
                            service: (*plane.service).clone(),
                            peer,
                        };
                        Builder::new(TokioExecutor::new())
                            .serve_connection(io, service)
                            .await
                            .unwrap();
                        let (status, _) = client.join().unwrap();
                        assert_eq!(status, expected);
                    }
                });
        });

        let spans = capture2.spans();
        let http: Vec<_> = spans
            .iter()
            .filter(|span| span.target == "tinio::http")
            .collect();
        assert_eq!(http.len(), 2, "{spans:?}");
        for span in &http {
            assert_eq!(span.name, "tinio.http.request");
            assert_eq!(span.level, tracing::Level::INFO);
            assert_eq!(span.field("method"), Some("GET"));
            assert!(span.field("request_id").is_some(), "{span:?}");
        }
        // The query is stripped; both the 404 and the 200 path survive.
        assert_eq!(http[0].field("path"), Some("/missing-bucket/key"));
        assert_eq!(http[1].field("path"), Some("/data/key"));
        assert_ne!(http[0].field("request_id"), http[1].field("request_id"));

        // The access event rides the span: one event per request, each
        // emitted inside its own request span.
        let events = capture2.events();
        let access: Vec<_> = events
            .iter()
            .filter(|event| event.target == ACCESS_TARGET)
            .collect();
        assert_eq!(access.len(), 2, "{events:?}");
        for (event, span) in access.iter().zip(&http) {
            assert_eq!(event.span, Some(span.id));
        }
    }
}
