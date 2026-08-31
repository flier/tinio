//! Logging layers (task T052).
//!
//! The access-log tracing layer formats events on the `tinio::access`
//! target (emitted by the data-plane middleware) into the configured
//! nginx-style format (`combined`/`common`/custom over the fixed variable
//! set, FR-017) and writes them to the access-log file. Operational
//! layers: text or JSON `tracing_subscriber::fmt` layers at the configured
//! verbosity — errors are always visible on stderr (FR-017). The optional
//! `otel` feature adds an OTLP export layer (T053).

use std::{
    error::Error,
    fmt::Debug,
    fs::File,
    io::{self, Write},
    path::Path,
    sync::Mutex,
};

use tracing::{
    Event, Level, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{
    Layer,
    fmt::{
        self,
        writer::{BoxMakeWriter, MakeWriterExt},
    },
    layer::{Context, SubscriberExt},
    registry::LookupSpan,
};

use crate::_config::log::{self, Format, Verbosity};

/// The access-log event target (the data-plane middleware emits events
/// with this target).
pub const ACCESS_TARGET: &str = "tinio::access";

/// One access-log variable — the fixed, closed set of FR-017, and the
/// single schema shared by the data-plane emitter and the log formatter:
/// the `$name` lives here only, and a variant's position indexes its
/// value in [`AccessFields`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessField {
    RemoteAddr,
    RemoteUser,
    TimeLocal,
    Request,
    Status,
    BodyBytesSent,
    HttpReferer,
    HttpUserAgent,
    RequestTime,
}

impl AccessField {
    /// The fixed set, in `$name` order (variant order = value index).
    pub const ALL: [AccessField; 9] = [
        AccessField::RemoteAddr,
        AccessField::RemoteUser,
        AccessField::TimeLocal,
        AccessField::Request,
        AccessField::Status,
        AccessField::BodyBytesSent,
        AccessField::HttpReferer,
        AccessField::HttpUserAgent,
        AccessField::RequestTime,
    ];

    /// The `$name` of the variable.
    pub const fn name(self) -> &'static str {
        match self {
            AccessField::RemoteAddr => "remote_addr",
            AccessField::RemoteUser => "remote_user",
            AccessField::TimeLocal => "time_local",
            AccessField::Request => "request",
            AccessField::Status => "status",
            AccessField::BodyBytesSent => "body_bytes_sent",
            AccessField::HttpReferer => "http_referer",
            AccessField::HttpUserAgent => "http_user_agent",
            AccessField::RequestTime => "request_time",
        }
    }

    /// The variable with `$name`, if any.
    pub fn from_name(name: &str) -> Option<AccessField> {
        Some(match name {
            "remote_addr" => AccessField::RemoteAddr,
            "remote_user" => AccessField::RemoteUser,
            "time_local" => AccessField::TimeLocal,
            "request" => AccessField::Request,
            "status" => AccessField::Status,
            "body_bytes_sent" => AccessField::BodyBytesSent,
            "http_referer" => AccessField::HttpReferer,
            "http_user_agent" => AccessField::HttpUserAgent,
            "request_time" => AccessField::RequestTime,
            _ => return None,
        })
    }
}

/// The captured fields of one access-log event, one rendered value per
/// [`AccessField`].
#[derive(Debug, Clone, Default)]
pub struct AccessFields {
    /// One rendered value per [`AccessField`] (variant order = index).
    values: [String; 9],
}

impl AccessFields {
    /// Build the fields of one request (the data-plane emission shape).
    /// One argument per [`AccessField`] — the schema's fixed set.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        remote_addr: String,
        remote_user: String,
        time_local: String,
        request: String,
        status: u16,
        body_bytes_sent: u64,
        http_referer: String,
        http_user_agent: String,
        request_time: String,
    ) -> Self {
        Self {
            values: [
                remote_addr,
                remote_user,
                time_local,
                request,
                status.to_string(),
                body_bytes_sent.to_string(),
                http_referer,
                http_user_agent,
                request_time,
            ],
        }
    }

    /// The rendered value of a variable.
    pub fn get(&self, field: AccessField) -> &str {
        &self.values[field as usize]
    }

    fn set(&mut self, field: AccessField, value: String) {
        self.values[field as usize] = value;
    }

    /// Render one access-log line in the configured format. A single
    /// left-to-right scan expands `$name` (longest `[a-z_]` run — a
    /// `$request` prefix never eats `$request_time`); unknown variables
    /// stay as their literal `$name` (the config gate already restricted
    /// the set).
    pub fn format_line(&self, format: &log::AccessFormat) -> String {
        let fmt = format.as_str();
        let mut out = String::with_capacity(fmt.len() + 32);
        let mut rest = fmt;
        while let Some(start) = rest.find('$') {
            out.push_str(&rest[..start]);
            let after = &rest[start + 1..];
            let name: String = after
                .chars()
                .take_while(|c| c.is_ascii_lowercase() || *c == '_')
                .collect();
            match AccessField::from_name(&name) {
                Some(field) => out.push_str(self.get(field)),
                None => {
                    out.push('$');
                    out.push_str(&name);
                }
            }
            rest = &after[name.len()..];
        }
        out.push_str(rest);
        out
    }
}

/// Collects the structured fields of an access event into [`AccessFields`].
#[derive(Debug, Default)]
struct FieldCollector(AccessFields);

impl FieldCollector {
    fn record(&mut self, field: &Field, value: String) {
        if let Some(field) = AccessField::from_name(field.name()) {
            self.0.set(field, value);
        }
    }
}

impl Visit for FieldCollector {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record(field, value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record(field, value.to_string());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record(field, value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.record(field, format!("{value:?}"));
    }
}

/// The tracing layer that renders access events into the configured format.
///
/// # Examples
///
/// ```rust
/// use std::io::sink;
///
/// use tinio_config::log::AccessFormat;
/// use tinio_server::log::{ACCESS_TARGET, AccessFields, AccessLogLayer};
/// use tracing_subscriber::layer::SubscriberExt;
///
/// let layer = AccessLogLayer::new(AccessFormat::Common, sink());
/// let _subscriber = tracing_subscriber::registry().with(layer);
/// let fields = AccessFields::new(
///     "127.0.0.1".into(),
///     "-".into(),
///     "23/Aug/2026:12:00:00 +0000".into(),
///     "GET / HTTP/1.1".into(),
///     200,
///     0,
///     "-".into(),
///     "-".into(),
///     "0.001".into(),
/// );
/// assert!(
///     fields
///         .format_line(&AccessFormat::Combined)
///         .contains(" - - ")
/// );
/// ```
#[derive(Debug)]
pub struct AccessLogLayer<W: Write + Send + 'static> {
    format: log::AccessFormat,
    writer: Mutex<W>,
}

impl<W: Write + Send + 'static> AccessLogLayer<W> {
    /// Create the layer writing to `writer`.
    pub fn new(format: log::AccessFormat, writer: W) -> Self {
        Self {
            format,
            writer: Mutex::new(writer),
        }
    }
}

impl<S, W> Layer<S> for AccessLogLayer<W>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    W: Write + Send + 'static,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() != ACCESS_TARGET {
            return;
        }
        let mut collector = FieldCollector::default();
        event.record(&mut collector);
        let line = collector.0.format_line(&self.format);
        let mut writer = self
            .writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _ = writeln!(writer, "{line}");
        let _ = writer.flush();
    }
}

/// Build the operational subscriber: fmt layers (text or JSON) at the
/// configured verbosity plus the access-log layer writing to `access_path`.
/// Errors (level `error` and above) always go to stderr too (FR-017). When
/// the `otel` feature is compiled and `otel_endpoint` is `Some`, the OTLP
/// export layer is attached (T053).
///
/// # Errors
///
/// `Io` when the access-log file cannot be opened for appending; OTLP
/// exporter construction failures when the `otel` feature is enabled.
pub fn build_subscriber(
    verbosity: log::Verbosity,
    format: log::Format,
    access_format: &log::AccessFormat,
    access_path: &Path,
    otel_endpoint: Option<&str>,
) -> Result<Box<dyn Subscriber + Send + Sync>, Box<dyn Error + Send + Sync>> {
    let access_file = File::options()
        .create(true)
        .append(true)
        .open(access_path)?;
    let access_layer = AccessLogLayer::new(access_format.clone(), access_file);

    let level = match verbosity {
        Verbosity::Error => Level::ERROR,
        Verbosity::Warn => Level::WARN,
        Verbosity::Info => Level::INFO,
        Verbosity::Debug => Level::DEBUG,
    };
    let stderr = BoxMakeWriter::new(io::stderr).with_max_level(level);
    let op_layer = match format {
        Format::Text => fmt::layer().with_writer(stderr).with_target(false).boxed(),
        Format::Json => fmt::layer()
            .json()
            .with_writer(stderr)
            .with_target(false)
            .boxed(),
    };
    let base = tracing_subscriber::registry()
        .with(op_layer)
        .with(access_layer);
    #[cfg(feature = "otel")]
    if let Some(endpoint) = otel_endpoint {
        return Ok(Box::new(base.with(otel_layer(endpoint)?)));
    }
    let _ = otel_endpoint;
    Ok(Box::new(base))
}

/// The OpenTelemetry export layer (task T053, behind the `otel` feature):
/// an OTLP gRPC exporter at `endpoint` bridged into tracing. The endpoint
/// falls back to `OTEL_EXPORTER_OTLP_ENDPOINT` when unset (config.md).
#[cfg(feature = "otel")]
pub fn otel_layer<S>(
    endpoint: &str,
) -> Result<Box<dyn Layer<S> + Send + Sync>, Box<dyn Error + Send + Sync>>
where
    S: Subscriber + for<'a> LookupSpan<'a> + Send + Sync + 'static,
{
    use std::env;

    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_otlp::{SpanExporter, WithExportConfig};
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tracing_opentelemetry::OpenTelemetryLayer;

    let endpoint = if endpoint.is_empty() {
        env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:4317".to_string())
    } else {
        endpoint.to_string()
    };
    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .build();
    let tracer = provider.tracer("tinio");
    Ok(Box::new(OpenTelemetryLayer::new(tracer)))
}

#[cfg(test)]
mod tests {

    use std::{
        fs,
        panic::{AssertUnwindSafe, catch_unwind},
    };

    use tracing::subscriber::with_default;

    use super::*;
    use crate::{_config::log::AccessFormat, _util::testing::SharedBuf};

    fn fields(remote_addr: &str, request: &str, status: u16, body_bytes_sent: u64) -> AccessFields {
        AccessFields::new(
            remote_addr.into(),
            "-".into(),
            "t".into(),
            request.into(),
            status,
            body_bytes_sent,
            "-".into(),
            "-".into(),
            "0.001".into(),
        )
    }

    #[test]
    fn combined_format_line() {
        let fields = fields("127.0.0.1", "GET /data/a.txt HTTP/1.1", 200, 5);
        let line = fields.format_line(&AccessFormat::Combined);
        assert_eq!(
            line,
            "127.0.0.1 - - [t] \"GET /data/a.txt HTTP/1.1\" 200 5 \"-\" \"-\""
        );
    }

    #[test]
    fn common_format_line() {
        let fields = fields("127.0.0.1", "GET / HTTP/1.1", 404, 0);
        let line = fields.format_line(&AccessFormat::Common);
        assert_eq!(line, "127.0.0.1 - - [t] \"GET / HTTP/1.1\" 404 0");
    }

    #[test]
    fn custom_format_line() {
        let fields = fields("10.0.0.1", "GET / HTTP/1.1", 200, 1);
        let line = fields.format_line(&AccessFormat::Custom("$request $status".into()));
        assert_eq!(line, "GET / HTTP/1.1 200");
    }

    #[test]
    fn variable_prefixes_do_not_collide() {
        // `$request` is a prefix of `$request_time`: a naive
        // substitution order would corrupt the longer name.
        let fields = AccessFields::new(
            "-".into(),
            "-".into(),
            "-".into(),
            "GET / HTTP/1.1".into(),
            0,
            0,
            "-".into(),
            "-".into(),
            "0.5".into(),
        );
        let line = fields.format_line(&AccessFormat::Custom(
            "$request_time $request $unknown".into(),
        ));
        assert_eq!(line, "0.5 GET / HTTP/1.1 $unknown");
    }

    #[test]
    fn variable_names_round_trip() {
        for field in AccessField::ALL {
            assert_eq!(AccessField::from_name(field.name()), Some(field));
        }
        assert_eq!(AccessField::from_name("nope"), None);
    }

    #[test]
    fn access_event_renders_every_variable() {
        let sentinel = |var: &str| match var {
            "remote_addr" => "ra",
            "remote_user" => "ru",
            "time_local" => "tl",
            "request" => "rq",
            "status" => "201",
            "body_bytes_sent" => "7",
            "http_referer" => "hr",
            "http_user_agent" => "hua",
            "request_time" => "rt",
            other => panic!("{other} missing from the test fixture"),
        };

        // One real access event (the data-plane shape) through the
        // production layer: every variable must render its sentinel — a
        // name missed by the emission or the collector shows up empty.
        let format = AccessField::ALL
            .iter()
            .map(|f| format!("${}", f.name()))
            .collect::<Vec<_>>()
            .join(" ");
        let buf = SharedBuf::default();
        let layer = AccessLogLayer::new(AccessFormat::Custom(format), buf.clone());
        let subscriber = tracing_subscriber::registry().with(layer);
        with_default(subscriber, || {
            tracing::info!(
                target: ACCESS_TARGET,
                remote_addr = sentinel("remote_addr"),
                remote_user = sentinel("remote_user"),
                time_local = sentinel("time_local"),
                request = sentinel("request"),
                status = 201u16,
                body_bytes_sent = 7u64,
                http_referer = sentinel("http_referer"),
                http_user_agent = sentinel("http_user_agent"),
                request_time = sentinel("request_time"),
                "s3 request completed"
            );
        });
        let expected: Vec<&str> = AccessField::ALL
            .iter()
            .map(|f| sentinel(f.name()))
            .collect();
        let line = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert_eq!(line.trim(), expected.join(" "));
    }

    #[test]
    fn non_access_events_are_ignored() {
        let buf = SharedBuf::default();
        let layer = AccessLogLayer::new(AccessFormat::Combined, buf.clone());
        let subscriber = tracing_subscriber::registry().with(layer);
        with_default(subscriber, || {
            tracing::info!(target: "tinio::other", "not an access event");
        });
        assert!(buf.0.lock().unwrap().is_empty());
    }

    #[test]
    fn debug_and_signed_fields_are_collected() {
        let buf = SharedBuf::default();
        let layer =
            AccessLogLayer::new(AccessFormat::Custom("$status $request".into()), buf.clone());
        let subscriber = tracing_subscriber::registry().with(layer);
        with_default(subscriber, || {
            tracing::info!(
                target: ACCESS_TARGET,
                status = -3i64,
                request = ?"debug-rendered",
                "s3 request completed"
            );
        });
        let line = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert_eq!(line.trim(), "-3 \"debug-rendered\"");
    }

    #[test]
    fn poisoned_lock_recovers_into_inner_writer() {
        // A panicked writer (e.g. a full disk during a flush) poisons the
        // mutex; the layer must recover the inner writer, not lose events.
        let buf = SharedBuf::default();
        let layer = AccessLogLayer::new(AccessFormat::Combined, buf.clone());
        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _guard = layer.writer.lock().unwrap();
            panic!("poison the access-log mutex");
        }));
        let fields = AccessFields::new(
            "127.0.0.1".into(),
            "-".into(),
            "t".into(),
            "GET / HTTP/1.1".into(),
            200,
            0,
            "-".into(),
            "-".into(),
            "0.001".into(),
        );
        // Render through the layer's own writer path (poisoned → recover).
        {
            let mut writer = layer
                .writer
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let line = fields.format_line(&layer.format);
            let _ = writeln!(writer, "{line}");
            let _ = writer.flush();
        }
        let line = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert!(line.contains("GET / HTTP/1.1"), "{line}");
    }

    #[test]
    fn build_subscriber_writes_access_events_to_file() {
        for (format, suffix) in [(Format::Text, "text"), (Format::Json, "json")] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("access.log");
            let sub = build_subscriber(
                Verbosity::Info,
                format,
                &AccessFormat::Custom("$status $request".into()),
                &path,
                None,
            )
            .unwrap();
            with_default(sub, || {
                tracing::info!(
                    target: ACCESS_TARGET,
                    status = 201u16,
                    request = "GET /data/x HTTP/1.1",
                    "s3 request completed"
                );
            });
            let content = fs::read_to_string(&path).unwrap();
            assert!(
                content.contains("201 GET /data/x HTTP/1.1"),
                "{suffix} format: {content}"
            );
        }
    }

    #[test]
    fn build_subscriber_accepts_every_verbosity_and_format() {
        // The verbosity→level mapping and the text/JSON format split are
        // the two axis of the operational subscriber; every combination
        // must build and accept an access event.
        for verbosity in [
            Verbosity::Error,
            Verbosity::Warn,
            Verbosity::Info,
            Verbosity::Debug,
        ] {
            for format in [Format::Text, Format::Json] {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("access.log");
                let sub = build_subscriber(
                    verbosity,
                    format,
                    &AccessFormat::Custom("$status $request".into()),
                    &path,
                    None,
                )
                .expect("every verbosity/format combination builds");
                with_default(sub, || {
                    tracing::info!(
                        target: ACCESS_TARGET,
                        status = 200u16,
                        request = "GET /data/y HTTP/1.1",
                        "s3 request completed"
                    );
                });
                let content = fs::read_to_string(&path).unwrap();
                assert!(
                    content.contains("200 GET /data/y HTTP/1.1"),
                    "{verbosity:?} {format:?}: {content}"
                );
            }
        }
    }
}
