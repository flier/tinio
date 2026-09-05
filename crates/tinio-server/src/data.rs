//! The hyper data plane (task T051).
//!
//! Hosts the s3s [`S3Service`] over hyper/hyper-util: a tower middleware
//! wraps every request to record the access-log event (`tinio::access`
//! target, T052), the HTTP metrics (request count/duration/in-flight),
//! and the upload/download byte counters on the streaming bodies (T054).
//! The service itself is the [`MetricS3`] wrapper around [`S3Backend`].
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
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Instant,
};

#[cfg(feature = "cors")]
use http::{
    HeaderValue,
    header::{
        ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_METHODS,
        ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS, ORIGIN, VARY,
    },
};
use http::{
    Method, Request, Response,
    header::{CONTENT_LENGTH, CONTENT_TYPE, REFERER, USER_AGENT},
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
use prometheus::{Encoder, TextEncoder, default_registry};
use s3s::{
    Body as S3Body,
    auth::{S3Auth, SecretKey},
    s3_error,
    service::{S3Service, S3ServiceBuilder},
};
use time::{OffsetDateTime, format_description, format_description::BorrowedFormatItem};
use tokio::{net::TcpListener, sync::watch};
use tower::Service as TowerService;
use tracing::Level;

#[cfg(feature = "cors")]
use crate::backend::cors::{CorsConfigs, CorsLookup, CorsPreflightRoute, bucket_from_uri};
use crate::{
    _core::storage::Storage,
    backend::{Capabilities, S3Backend},
    error::Error,
    log::{ACCESS_TARGET, AccessField, AccessFields},
    metrics::{self, MetricS3},
};

/// The `/metrics` scrape path (F10/F49): the hook refreshes the
/// scrape-computed families (`tinio_server::metrics::refresh` — the
/// server wires it to the pipelines' [`Stats`] and the storage's
/// write-lock snapshot) before the endpoint gathers the registry.
pub type MetricsRefresh = Arc<dyn Fn() + Send + Sync>;

/// The reserved `/metrics` endpoint path on the data-plane listener.
pub const METRICS_PATH: &str = "/metrics";

/// The single static SigV4 credential pair as an s3s auth provider.
/// Unlike `SimpleAuth` (which answers `NotSignedUp`), an unknown access
/// key answers AWS's `InvalidAccessKeyId` — the code aws cli and the
/// FR-008 interop scenario expect for a wrong key.
struct StaticAuth {
    access_key: String,
    secret_key: SecretKey,
}

#[async_trait::async_trait]
impl S3Auth for StaticAuth {
    async fn get_secret_key(&self, access_key: &str) -> s3s::S3Result<SecretKey> {
        if access_key == self.access_key {
            Ok(self.secret_key.clone())
        } else {
            Err(s3_error!(
                InvalidAccessKeyId,
                "The AWS Access Key Id you provided does not exist in our records."
            ))
        }
    }
}

/// The data plane: the s3s service behind the metrics/access middleware.
pub struct DataPlane {
    service: Arc<DataPlaneService>,
}

impl DataPlane {
    /// Build the data plane over a storage backend.
    pub fn new<S: Storage>(storage: S, caps: Capabilities) -> Self {
        Self::new_shared(Arc::new(storage), caps)
    }

    /// Build the data plane over a SHARED storage handle: the backend and
    /// the CORS preflight route (when armed) share the one `Arc<S>`.
    ///
    /// UNGATED: it exists in every build (feature-off builds keep a
    /// constructor via [`DataPlane::new`]); only the cors wiring is
    /// `#[cfg(feature = "cors")]`.
    pub fn new_shared<S: Storage>(storage: Arc<S>, caps: Capabilities) -> Self {
        let backend = MetricS3::new(S3Backend::new_shared(Arc::clone(&storage), caps));
        #[cfg(feature = "cors")]
        {
            let mut builder = S3ServiceBuilder::new(backend);
            // Double gate: the compile-time feature AND the runtime
            // capability arm the preflight route; the erased lookup rides
            // on the service either way (None with the capability off — no
            // route, no decoration).
            let configs = Arc::new(CorsConfigs::new(Arc::clone(&storage)));
            if caps.cors {
                builder.set_route(CorsPreflightRoute::new(Arc::clone(&configs)));
            }
            Self::from_service(
                builder.build(),
                caps.cors.then_some(configs as Arc<dyn CorsLookup>),
            )
        }
        #[cfg(not(feature = "cors"))]
        Self::from_service(S3ServiceBuilder::new(backend).build())
    }

    /// Build the data plane with a single static credential pair (SigV4).
    ///
    /// Unsigned requests stay allowed only when no auth provider is set; with
    /// one set, s3s requires signed requests to verify against these
    /// credentials. Used by the `serve` example so the US1 interop harness
    /// works with real (always-signing) S3 clients; superseded by the
    /// config-based auth provider in US3 (T082/T083).
    pub fn new_with_auth<S: Storage>(
        storage: S,
        caps: Capabilities,
        access_key: &str,
        secret_key: &str,
    ) -> Self {
        let storage = Arc::new(storage);
        let backend = MetricS3::new(S3Backend::new_shared(Arc::clone(&storage), caps));
        let mut builder = S3ServiceBuilder::new(backend);
        builder.set_auth(StaticAuth {
            access_key: access_key.to_string(),
            secret_key: secret_key.into(),
        });
        #[cfg(feature = "cors")]
        {
            let configs = Arc::new(CorsConfigs::new(Arc::clone(&storage)));
            if caps.cors {
                builder.set_route(CorsPreflightRoute::new(Arc::clone(&configs)));
            }
            Self::from_service(
                builder.build(),
                caps.cors.then_some(configs as Arc<dyn CorsLookup>),
            )
        }
        #[cfg(not(feature = "cors"))]
        Self::from_service(builder.build())
    }

    /// Attach the scrape-time metrics refresh (F10): the `/metrics`
    /// endpoint on the data-plane listener calls the hook before
    /// gathering the registry. `serve` wires it to
    /// `tinio_server::metrics::refresh(io.stats(), db.stats(),
    /// storage.write_lock_stats())`.
    pub fn with_metrics(mut self, refresh: MetricsRefresh) -> Self {
        Arc::get_mut(&mut self.service)
            .expect("the plane is not shared at construction")
            .metrics = refresh;
        self
    }

    #[cfg(feature = "cors")]
    fn from_service(service: S3Service, cors: Option<Arc<dyn CorsLookup>>) -> Self {
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
        // The reserved `/metrics` scrape endpoint (F10): served here,
        // BEFORE the S3 service — a management path on the data-plane
        // listener, never routed to the storage plane.
        if is_metrics_request(req.method(), req.uri()) {
            return metrics_response(&self.service.metrics);
        }
        self.service.call_with_peer(req, self.peer)
    }
}

/// Whether the request targets the reserved `/metrics` scrape endpoint
/// (GET only; any other method falls through to the S3 service).
fn is_metrics_request(method: &Method, uri: &http::Uri) -> bool {
    method == Method::GET && uri.path() == METRICS_PATH
}

/// The `/metrics` response: refresh the scrape-computed families through
/// the plane's hook, then gather the default registry in the Prometheus
/// text format (F10/F49).
fn metrics_response(metrics: &MetricsRefresh) -> ServiceFuture {
    metrics();
    let mut buf = Vec::new();
    // TextEncoder::encode is infallible in practice (the error type
    // is a placeholder) — a failure serves an empty body.
    let _ = Encoder::encode(&TextEncoder::new(), &default_registry().gather(), &mut buf);
    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, "text/plain; version=0.0.4")
        .header(CONTENT_LENGTH, buf.len())
        .body(CountingBody::new(
            S3Body::from(buf),
            Arc::new(AtomicU64::new(0)),
            CountingKind::Download,
        ))
        .expect("a static /metrics response is always valid");
    Box::pin(async move { Ok(response) })
}

/// The tower middleware over the s3s service: access-log events, HTTP
/// metrics, and byte counters.
#[derive(Clone)]
pub struct DataPlaneService {
    inner: S3Service,
    /// The scrape-time metrics refresh (F10/F49) — a no-op until
    /// [`DataPlane::with_metrics`] attaches the server's hook.
    metrics: MetricsRefresh,
    /// The CORS config lookup (feature + capability double-gated; `None`
    /// when the preflight route is not armed) — read by the Task-10
    /// decoration on every non-preflight response.
    #[cfg(feature = "cors")]
    cors: Option<Arc<dyn CorsLookup>>,
}

/// Decrements `HTTP_IN_FLIGHT` when dropped — on request completion and
/// on cancellation (a dropped service future), so a client disconnect
/// cannot leak the gauge.
struct InFlightGauge;

impl Drop for InFlightGauge {
    fn drop(&mut self) {
        metrics::http_in_flight_dec();
    }
}

impl DataPlaneService {
    #[cfg(feature = "cors")]
    fn new(inner: S3Service, cors: Option<Arc<dyn CorsLookup>>) -> Self {
        Self {
            inner,
            metrics: Arc::new(|| {}),
            cors,
        }
    }

    #[cfg(not(feature = "cors"))]
    fn new(inner: S3Service) -> Self {
        Self {
            inner,
            metrics: Arc::new(|| {}),
        }
    }

    fn call_with_peer(&self, req: Request<Incoming>, peer: SocketAddr) -> ServiceFuture {
        let start = Instant::now();
        metrics::http_in_flight_inc();
        // Decrements when the future completes AND when hyper drops it
        // (client disconnect mid-request) — a cancelled future must not
        // leak the gauge. Moved into the future below.
        let inflight = InFlightGauge;

        let method = req.method().as_str().to_string();
        // The access-log fields are built only when a subscriber listens
        // on the `tinio::access` target — tracing would filter the event,
        // but the strings (and the `nginx_time` clock read) would still be
        // allocated on every request (T052).
        let access_log = tracing::enabled!(target: ACCESS_TARGET, Level::INFO).then(|| {
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
        #[cfg(feature = "cors")]
        let uri = req.uri().clone();

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
        Box::pin(async move {
            // s3s's `HttpError` is not a `std::error::Error` — box its
            // Display form.
            let result = future
                .await
                .map_err(|e| IoError::other(format!("{e:?}")).into());
            let elapsed = start.elapsed();
            let upload_bytes = upload_counter.load(Ordering::Relaxed);
            metrics::record_upload_bytes(upload_bytes);
            // The gauge is released here (normal completion) or with the
            // future itself (cancellation).
            drop(inflight);

            // One metric + access-log record per request: the status and
            // body bytes come from the response, 500/0 for transport
            // failures.
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
                    if let (Some(cors), Some(origin)) = (cors.as_deref(), origin.as_deref())
                        && let Some(bucket) = bucket_from_uri(&uri)
                        && let Some(config) = cors.get(&bucket).await
                        && let Some(rule) = config.rule_for(origin, &method)
                    {
                        let headers = response.headers_mut();
                        // grilling Q11: a rule whose allowed_origins
                        // contains bare "*" answers ACAO "*" and OMITS
                        // Allow-Credentials (the two are incompatible);
                        // otherwise echo the origin + allow the credentials.
                        // Fallible HeaderValue construction — every value
                        // here is request or config data — SKIPS, never
                        // unwraps (op-review S1).
                        let star_rule = rule.allowed_origins.iter().any(|o| o == "*");
                        if let Ok(v) = HeaderValue::from_str(if star_rule { "*" } else { origin }) {
                            headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, v);
                        }
                        let methods = rule.allowed_methods.join(", ");
                        if let Ok(v) = HeaderValue::from_str(&methods) {
                            headers.insert(ACCESS_CONTROL_ALLOW_METHODS, v);
                        }
                        if let Some(expose) = &rule.expose_headers
                            && !expose.is_empty()
                            && let Ok(v) = HeaderValue::from_str(&expose.join(", "))
                        {
                            headers.insert(ACCESS_CONTROL_EXPOSE_HEADERS, v);
                        }
                        if !star_rule {
                            // The literal is not request data — the safe
                            // constant form (the preflight route's pattern).
                            headers.insert(
                                ACCESS_CONTROL_ALLOW_CREDENTIALS,
                                HeaderValue::from_static("true"),
                            );
                        }
                        // grilling Q4 + op-review G3: APPEND (merge, never
                        // replace) the Vary trio; the wire carries the
                        // three as separate Vary lines.
                        for v in [
                            "Origin",
                            "Access-Control-Request-Headers",
                            "Access-Control-Request-Method",
                        ] {
                            headers.append(VARY, HeaderValue::from_static(v));
                        }
                    }
                    // Count download bytes on the response body (recorded
                    // when the stream ends); the access log uses the
                    // response Content-Length (known upfront).
                    let (parts, body) = response.into_parts();
                    let download_counter = Arc::new(AtomicU64::new(0));
                    let counted = CountingBody::new(
                        body,
                        Arc::clone(&download_counter),
                        CountingKind::Download,
                    );
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
            metrics::record_http_request(&method, status, elapsed);
            if let Some((remote_addr, request, referer, user_agent)) = access_log {
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
        })
    }
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
    use std::{
        collections::{HashMap, VecDeque},
        fmt::Debug,
        io,
        sync::{Mutex, atomic::AtomicUsize},
        thread,
        time::Duration,
    };

    use futures::task;
    use http_body::{Body as _, Frame};
    use metrics::WriteLockStats;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        runtime::Runtime,
        task::JoinHandle,
        time::sleep,
    };
    use tracing::{
        Event, Metadata, Subscriber, field,
        span::{self, Id},
        subscriber::with_default,
    };

    use super::*;
    use crate::{
        _core::{
            bucket,
            cors::{CorsConfig, CorsRule},
            object,
            pipeline::Stats,
            storage::{BucketOps, ObjectOps},
        },
        _mem::MemoryStorage,
        _util::testing::body,
    };

    /// One captured event: the target and its key/value fields.
    type CaptureEvent = (String, HashMap<String, String>);

    /// A bare subscriber capturing `(target, fields)` of every event —
    /// enough to assert the access-log records without pulling in a
    /// subscriber dependency.
    #[derive(Clone, Default)]
    struct CaptureSubscriber {
        events: Arc<Mutex<Vec<CaptureEvent>>>,
    }

    impl Subscriber for CaptureSubscriber {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
            Id::from_u64(1)
        }

        fn record(&self, _: &span::Id, _: &span::Record<'_>) {}

        fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}

        fn event(&self, event: &Event<'_>) {
            let mut fields = HashMap::new();
            event.record(&mut Fields(&mut fields));
            self.events
                .lock()
                .unwrap()
                .push((event.metadata().target().to_string(), fields));
        }

        fn enter(&self, _: &span::Id) {}

        fn exit(&self, _: &span::Id) {}
    }

    struct Fields<'a>(&'a mut HashMap<String, String>);

    impl field::Visit for Fields<'_> {
        fn record_str(&mut self, field: &field::Field, value: &str) {
            self.0.insert(field.name().to_string(), value.to_string());
        }

        fn record_debug(&mut self, field: &field::Field, value: &dyn Debug) {
            self.0
                .insert(field.name().to_string(), format!("{value:?}"));
        }
    }

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

    #[test]
    fn inflight_gauge_decrements_on_drop() {
        let _window = metrics::test_lock::window();
        metrics::HTTP_IN_FLIGHT.set(5);
        {
            let _gauge = InFlightGauge;
        }
        assert_eq!(metrics::HTTP_IN_FLIGHT.get(), 4);
    }

    #[tokio::test]
    async fn metrics_endpoint_refreshes_and_serves_the_registry() {
        // F10/F49: GET /metrics runs the plane's refresh hook (the
        // server wires it to the pipelines' Stats and the storage's
        // write-lock snapshot) and answers the Prometheus text format —
        // a management path on the data-plane listener, never routed to
        // the S3 service.
        let storage = MemoryStorage::new().unwrap();
        let hook_calls = Arc::new(AtomicUsize::new(0));
        let plane = DataPlane::new(storage, Capabilities::default()).with_metrics({
            let hook_calls = Arc::clone(&hook_calls);
            Arc::new(move || {
                hook_calls.fetch_add(1, Ordering::Relaxed);
                // The server's real hook (serve.rs): refresh the
                // scrape-computed families — which also registers them.
                // The write-lock data is the plain metric form (the
                // backend conversion lives at the wiring point).
                metrics::refresh(
                    Stats::default(),
                    Stats::default(),
                    WriteLockStats::default(),
                );
            })
        });
        // The route check (a `Request<Incoming>` cannot be constructed
        // outside hyper — its body constructors are pub(crate) — so the
        // method/uri predicate is tested directly).
        let metrics_uri: http::Uri = METRICS_PATH.parse().unwrap();
        assert!(is_metrics_request(&Method::GET, &metrics_uri));
        assert!(!is_metrics_request(&Method::POST, &metrics_uri));
        assert!(!is_metrics_request(
            &Method::GET,
            &"/bucket/key".parse().unwrap()
        ));
        // The response side through the plane's hook.
        let response = metrics_response(&plane.service.metrics).await.unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "text/plain; version=0.0.4"
        );
        // Drain the response body through its own poll_frame loop (the
        // test module's shared pattern).
        let mut body = response.into_body();
        let mut pin = Pin::new(&mut body);
        let mut text = String::new();
        while let Poll::Ready(Some(Ok(frame))) = pin.as_mut().poll_frame(&mut cx()) {
            if let Ok(chunk) = frame.into_data() {
                text.push_str(&String::from_utf8_lossy(&chunk));
            }
        }
        assert!(
            text.contains("tinio_pipeline_queue_depth"),
            "the refreshed pipeline gauges must be served: {text}"
        );
        assert!(
            text.contains("tinio_write_lock_wait_duration_seconds"),
            "the refreshed write-lock histograms must be served: {text}"
        );
        assert_eq!(hook_calls.load(Ordering::Relaxed), 1, "the hook ran once");
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

    #[tokio::test]
    async fn options_preflight_answered_on_the_plane() {
        // The preflight route on the real data plane: seed the mem storage
        // with a CORS config (via the storage handle), spawn the plane
        // (`Capabilities::default` → cors on), then a raw browser OPTIONS
        // (Origin + Access-Control-Request-Method) is answered 200 with the
        // allow headers — and a bare OPTIONS (no Origin) still falls
        // through to s3s: 501 unknown operation (the old behavior).
        let storage = MemoryStorage::new().unwrap();
        let name = bucket::name("data").unwrap();
        storage.create_bucket(&name).await.unwrap();
        storage
            .put_bucket_cors(
                &name,
                &CorsConfig {
                    rules: vec![CorsRule {
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

    #[tokio::test]
    async fn preflight_invalid_bucket_name_is_400_by_s3s_not_403() {
        // op-review C1: s3s validates the path in `prepare` (AwsNameValidation)
        // BEFORE the route matches — an invalid bucket (uppercase) is a 400
        // InvalidBucketName, never a 403 CORS denial from the route.
        let (addr, shutdown, handle) = spawn_plane().await;
        let (status, body) = raw_request(
            addr,
            "OPTIONS /MyBucket/key HTTP/1.1\r\nHost: localhost\r\nOrigin: https://example.com\r\nAccess-Control-Request-Method: GET\r\nConnection: close\r\n\r\n",
        )
        .await;
        let text = String::from_utf8_lossy(&body);
        assert_eq!(status, 400, "{text}");
        assert!(text.contains("InvalidBucketName"), "{text}");
        shutdown.send(true).unwrap();
        handle.await.unwrap();
    }

    /// A plane over a fresh MemoryStorage seeded with the bucket "data"
    /// (plus the object "key" carrying "payload") and the CORS config,
    /// served on an ephemeral port with the cors capability on.
    async fn seeded_cors_plane(
        config: CorsConfig,
    ) -> (SocketAddr, watch::Sender<bool>, JoinHandle<()>) {
        let storage = MemoryStorage::new().unwrap();
        let name = bucket::name("data").unwrap();
        storage.create_bucket(&name).await.unwrap();
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

    #[tokio::test]
    async fn get_with_matching_origin_is_decorated() {
        // The first plane's config is ONE concrete rule (GET +
        // https://example.com + expose ETag): a matching origin sees the
        // full header set, a foreign origin and a no-Origin request see
        // NONE, and a 404 (missing object) is still decorated (s3s encodes
        // op errors as Ok(Response) bodies — matches AWS).
        let (addr, shutdown, handle) = seeded_cors_plane(CorsConfig {
            rules: vec![CorsRule {
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
        let (addr, shutdown, handle) = seeded_cors_plane(CorsConfig {
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

    #[tokio::test]
    async fn get_with_origin_match_but_method_mismatch_is_not_decorated() {
        // B5/S3 pin — first-origin-match applies to the DECORATION too:
        // rule1 claims https://example.com (GET only); a PUT from that
        // origin must NOT be decorated — rule1's origin matches, its
        // method check fails, and the decorator never falls through to
        // rule2 (which allows PUT for "*").
        let (addr, shutdown, handle) = seeded_cors_plane(CorsConfig {
            rules: vec![
                CorsRule {
                    id: Some("r1".into()),
                    allowed_methods: vec!["GET".into()],
                    allowed_origins: vec!["https://example.com".into()],
                    allowed_headers: None,
                    expose_headers: None,
                    max_age_seconds: None,
                },
                CorsRule {
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
        let capture = CaptureSubscriber::default();
        let capture2 = capture.clone();
        with_default(capture, || {
            Runtime::new().unwrap().block_on(async {
                let storage = MemoryStorage::new().unwrap();
                let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                    .await
                    .unwrap();
                let addr = listener.local_addr().unwrap();
                let plane = DataPlane::new(storage, Capabilities::default());

                let client = thread::spawn(move || {
                    Runtime::new().unwrap().block_on(raw_request(
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

        let events = capture2.events.lock().unwrap();
        let access: Vec<_> = events
            .iter()
            .filter(|(target, _)| target == ACCESS_TARGET)
            .collect();
        assert_eq!(access.len(), 1, "{events:?}");
        let (_, fields) = access[0];
        assert_eq!(fields.get("remote_addr"), Some(&"127.0.0.1".to_string()));
        assert_eq!(
            fields.get("request"),
            Some(&"GET /missing-bucket/key HTTP/1.1".to_string())
        );
        assert_eq!(fields.get("status"), Some(&"404".to_string()));
        assert_eq!(
            fields.get("http_user_agent"),
            Some(&"test-agent/1.0".to_string())
        );
        // The presigned referer query is stripped (FR-017).
        assert_eq!(
            fields.get("http_referer"),
            Some(&"https://example.com/x".to_string())
        );
        assert!(fields.contains_key("time_local"));
        assert!(fields.contains_key("request_time"));
    }
}
