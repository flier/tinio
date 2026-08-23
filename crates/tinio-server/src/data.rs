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
    net::SocketAddr,
    pin::Pin,
    sync::OnceLock,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Instant,
};

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
    server::conn::auto,
};
use s3s::{
    Body as S3Body,
    auth::SimpleAuth,
    service::{S3Service, S3ServiceBuilder},
};
use time::{OffsetDateTime, format_description};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tower::Service as TowerService;

use tinio_core::storage::Storage;

use crate::{
    backend::{Capabilities, S3Backend},
    error::Error,
    log::{ACCESS_TARGET, AccessField, AccessFields},
    metrics::{self, MetricS3},
};

/// The data plane: the s3s service behind the metrics/access middleware.
pub struct DataPlane {
    service: Arc<DataPlaneService>,
}

impl DataPlane {
    /// Build the data plane over a storage backend.
    pub fn new<S: Storage>(storage: S, caps: Capabilities) -> Self {
        let backend = MetricS3::new(S3Backend::new(storage, caps));
        let service = S3ServiceBuilder::new(backend).build();
        Self::from_service(service)
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
        let backend = MetricS3::new(S3Backend::new(storage, caps));
        let mut builder = S3ServiceBuilder::new(backend);
        builder.set_auth(SimpleAuth::from_single(access_key, secret_key));
        Self::from_service(builder.build())
    }

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
                if let Err(err) = auto::Builder::new(TokioExecutor::new())
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
type ServiceFuture = Pin<Box<dyn Future<Output = Result<Response<CountingBody<S3Body>>, Box<dyn StdError + Send + Sync>>> + Send>>;

/// Attach the peer address to the service (the middleware needs it for the
/// access log).
#[derive(Clone)]
struct WithPeer {
    service: DataPlaneService,
    peer: SocketAddr,
}

impl Service<Request<Incoming>> for WithPeer {
    type Response = Response<CountingBody<S3Body>>;
    type Error = Box<dyn StdError + Send + Sync>;
    type Future = ServiceFuture;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        self.service.call_with_peer(req, self.peer)
    }
}

/// The tower middleware over the s3s service: access-log events, HTTP
/// metrics, and byte counters.
#[derive(Clone)]
pub struct DataPlaneService {
    inner: S3Service,
}

/// Decrements `HTTP_IN_FLIGHT` when dropped — on request completion and
/// on cancellation (a dropped service future), so a client disconnect
/// cannot leak the gauge.
struct InFlightGauge;

impl Drop for InFlightGauge {
    fn drop(&mut self) {
        metrics::HTTP_IN_FLIGHT.dec();
    }
}

impl DataPlaneService {
    fn new(inner: S3Service) -> Self {
        Self { inner }
    }

    fn call_with_peer(&self, req: Request<Incoming>, peer: SocketAddr) -> ServiceFuture {
        let start = Instant::now();
        metrics::HTTP_IN_FLIGHT.inc();
        // Decrements when the future completes AND when hyper drops it
        // (client disconnect mid-request) — a cancelled future must not
        // leak the gauge. Moved into the future below.
        let inflight = InFlightGauge;

        let method = req.method().as_str().to_string();
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
        let future = TowerService::call(&mut service, req);
        let remote_addr = peer.ip().to_string();
        Box::pin(async move {
            // s3s's `HttpError` is not a `std::error::Error` — box its
            // Display form.
            let result = future
                .await
                .map_err(|e| std::io::Error::other(format!("{e:?}")).into());
            let elapsed = start.elapsed();
            let upload_bytes = upload_counter.load(Ordering::Relaxed);
            metrics::STORAGE_UPLOAD_BYTES.inc_by(upload_bytes);
            // The gauge is released here (normal completion) or with the
            // future itself (cancellation).
            drop(inflight);

            // One metric + access-log record per request: the status and
            // body bytes come from the response, 500/0 for transport
            // failures.
            let (status, body_bytes, result) = match result {
                Ok(response) => {
                    let status = response.status().as_u16();
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
            metrics::STORAGE_DOWNLOAD_BYTES.inc_by(n);
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
    static FORMAT: OnceLock<Vec<time::format_description::BorrowedFormatItem<'static>>> =
        OnceLock::new();
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
    use super::*;

    #[test]
    fn request_line_drops_query_string() {
        let uri: http::Uri =
            "/bucket/key?X-Amz-Signature=secret&X-Amz-Credential=AKID"
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
}
