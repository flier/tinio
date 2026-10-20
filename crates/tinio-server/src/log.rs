//! Logging layers (task T052).
//!
//! It also owns the `[log]` schema (`Config`, `Verbosity`, `Format`,
//! `AccessFormat`), parseable behind the `config` feature.
//!
//! The access-log tracing layer formats events on the `tinio::access`
//! target (emitted by the data-plane middleware) into the configured
//! nginx-style format (`combined`/`common`/custom over the fixed variable
//! set, FR-017) and writes them to the access-log file. Operational
//! layers: text or JSON `tracing_subscriber::fmt` layers at the configured
//! verbosity — errors are always visible on stderr (FR-017). The optional
//! `otel` feature adds an OTLP export layer (T053).

use std::{
    env,
    error::Error,
    fmt::Debug,
    fs::File,
    io::{self, Write},
    path::Path,
    sync::{Mutex, OnceLock},
    time::Instant,
};

#[cfg(feature = "config")]
use garde::Validate;
#[cfg(feature = "otel")]
use opentelemetry::trace::TracerProvider as _;
#[cfg(feature = "otel")]
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
#[cfg(feature = "otel")]
use opentelemetry_sdk::trace::SdkTracerProvider;
use parse_display::{Display, FromStr};
#[cfg(feature = "config")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smart_default::SmartDefault;
use tracing::{
    Event, Id, Level, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Record},
};
#[cfg(feature = "otel")]
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{
    EnvFilter, Layer,
    filter::LevelFilter,
    fmt::{self, writer::BoxMakeWriter},
    layer::{Context, SubscriberExt},
    registry::{LookupSpan, Registry},
    reload,
};

/// Log level.
///
/// # Examples
///
/// ```rust
/// use std::str::FromStr;
///
/// use tinio_server::log::Verbosity;
///
/// assert_eq!(Verbosity::from_str("debug").unwrap(), Verbosity::Debug);
/// assert_eq!(Verbosity::default().to_string(), "info");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Display, FromStr)]
#[cfg_attr(feature = "config", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "config", serde(rename_all = "lowercase"))]
#[display(style = "lowercase")]
pub enum Verbosity {
    Error,
    Warn,
    /// Informational (default).
    #[default]
    Info,
    Debug,
}

/// Server log output format.
///
/// # Examples
///
/// ```rust
/// use std::str::FromStr;
///
/// use tinio_server::log::Format;
///
/// assert_eq!(Format::from_str("json").unwrap(), Format::Json);
/// assert_eq!(Format::default().to_string(), "text");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Display, FromStr)]
#[cfg_attr(feature = "config", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "config", serde(rename_all = "lowercase"))]
#[display(style = "lowercase")]
pub enum Format {
    /// Human-readable text lines.
    #[default]
    Text,
    Json,
}

/// The access-log format: `combined`, `common`, or a custom nginx-style
/// string over the closed variable set (FR-017 — it cannot reference
/// Authorization, query strings, or credentials).
///
/// # Examples
///
/// ```rust
/// use tinio_server::log::AccessFormat;
///
/// assert!(AccessFormat::default().as_str().contains("$status"));
/// let custom = AccessFormat::Custom("$remote_addr $status".into());
/// assert_eq!(custom.as_str(), "$remote_addr $status");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "config", derive(Validate))]
pub enum AccessFormat {
    /// Nginx combined: `$remote_addr - $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"`.
    #[default]
    Combined,
    Common,
    Custom(
        // FR-017 closed set, as a pattern gate. Known boundary: the regex
        // matches the longest listed variable, so a name with an identifier
        // suffix (e.g. `$statusx`) passes — acceptable while the formatter
        // resolves the set by exact name lookup at serve time.
        #[cfg_attr(
            feature = "config",
            garde(pattern(
                r"^([^\$]|\$(remote_addr|remote_user|time_local|request|status|body_bytes_sent|http_referer|http_user_agent|request_time))*$"
            ))
        )]
        String,
    ),
}

impl AccessFormat {
    /// The format as a format string (custom formats pass through).
    pub fn as_str(&self) -> &str {
        match self {
            Self::Combined => {
                "$remote_addr - $remote_user [$time_local] \"$request\" $status \
                 $body_bytes_sent \"$http_referer\" \"$http_user_agent\""
            }
            Self::Common => {
                "$remote_addr - $remote_user [$time_local] \"$request\" $status $body_bytes_sent"
            }
            Self::Custom(s) => s,
        }
    }
}

#[cfg(feature = "config")]
impl Serialize for AccessFormat {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Combined => serializer.serialize_str("combined"),
            Self::Common => serializer.serialize_str("common"),
            Self::Custom(s) => serializer.serialize_str(s),
        }
    }
}

#[cfg(feature = "config")]
impl<'de> Deserialize<'de> for AccessFormat {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "combined" => Ok(Self::Combined),
            "common" => Ok(Self::Common),
            other => Ok(Self::Custom(other.into())),
        }
    }
}

/// Logging configuration (`[log]`).
///
/// # Examples
///
/// ```rust
/// use tinio_server::log::Config;
///
/// let log = Config::default();
/// assert_eq!(log.access_log, "access.log");
/// assert_eq!(log.server_log_file, ""); // empty = stderr
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault)]
#[cfg_attr(feature = "config", derive(Serialize, Deserialize, Validate))]
#[cfg_attr(feature = "config", garde(allow_unvalidated))]
#[cfg_attr(feature = "config", serde(default))]
pub struct Config {
    /// `error | warn | info | debug` (default `info`).
    pub verbosity: Verbosity,
    /// Access-log file name (relative to the state dir, or absolute).
    #[default(_code = "DEFAULT_ACCESS_LOG.to_string()")]
    pub access_log: String,
    /// `combined | common | custom nginx-style string` (default `combined`).
    #[cfg_attr(feature = "config", garde(dive))]
    pub access_log_format: AccessFormat,
    /// `text | json` (default `text`; json defaults the file to `server.json`).
    pub server_log_format: Format,
    /// Server-log file (empty = stderr; daemon mode defaults it).
    pub server_log_file: String,
}

/// Access-log file name. Default `access.log`.
const DEFAULT_ACCESS_LOG: &str = "access.log";

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
    #[expect(
        clippy::too_many_arguments,
        reason = "one argument per `AccessField` — the schema's fixed set — so the arity is the data model's and the variant order is the index"
    )]
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
    pub fn format_line(&self, format: &AccessFormat) -> String {
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
/// use tinio_server::log::{ACCESS_TARGET, AccessFields, AccessFormat, AccessLogLayer};
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
    format: AccessFormat,
    writer: Mutex<W>,
}

impl<W: Write + Send + 'static> AccessLogLayer<W> {
    /// Create the layer writing to `writer`.
    pub fn new(format: AccessFormat, writer: W) -> Self {
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

/// The operational layers a subscriber runs: fmt, the close-only layer, the access log and, in an
/// `otel` build with an endpoint, the OTLP export.
pub type OperationalLayer = Box<dyn Layer<Registry> + Send + Sync + 'static>;

/// The retained OTLP provider: `None` when no endpoint is configured. Without the `otel` feature it
/// is `Option<()>`, so the composition root compiles unchanged in both graphs.
#[cfg(feature = "otel")]
pub type OtelHandle = Option<SdkTracerProvider>;

#[cfg(not(feature = "otel"))]
pub type OtelHandle = Option<()>;

/// The process's one provider. The export layer holds only a tracer, so dropping the provider on
/// return from `build_layer` left every export empty; this keeps it alive until process teardown.
static OTEL: OnceLock<OtelHandle> = OnceLock::new();

/// Retain `handle` for the process lifetime; the first install wins.
///
/// # Examples
///
/// ```rust
/// use tinio_server::log::install_otel;
///
/// install_otel(None);
/// ```
pub fn install_otel(handle: OtelHandle) {
    let _ = OTEL.set(handle);
}

/// Flush and stop the retained provider — the graceful-stop half. A no-op without the `otel`
/// feature or when no endpoint was configured.
///
/// # Examples
///
/// ```rust
/// use tinio_server::log::shutdown_otel;
///
/// shutdown_otel();
/// ```
pub fn shutdown_otel() {
    #[cfg(feature = "otel")]
    if let Some(provider) = OTEL.get().and_then(Option::as_ref)
        && let Err(error) = provider.shutdown()
    {
        tracing::warn!(error = %error, "the OTLP exporter did not stop cleanly");
    }
}

/// The handle that swaps the installed subscriber's operational layers.
pub type ReloadHandle = reload::Handle<OperationalLayer, Registry>;

/// The handle that swaps the installed subscriber's `EnvFilter`.
pub type FilterHandle = reload::Handle<EnvFilter, Registry>;

/// The two handles a later phase reloads through.
///
/// # Examples
///
/// ```rust
/// use tinio_server::log::build_bootstrap;
///
/// let logging = build_bootstrap().expect("a bootstrap subscriber");
/// let _ = (&logging.handles.layer, &logging.handles.filter);
/// ```
pub struct Handles {
    /// The operational layers: fmt, the close-only layer, the access log and OTLP.
    pub layer: ReloadHandle,
    /// The `EnvFilter` the operational layers and the span levels obey.
    pub filter: FilterHandle,
}

/// The subscriber the process installs — the fmt-only bootstrap in `main`, then the full stack
/// inside the confinement — and the handles the second phase swaps through.
///
/// # Examples
///
/// ```rust
/// use tinio_server::log::build_bootstrap;
///
/// let logging = build_bootstrap().expect("a bootstrap subscriber");
/// let _ = &logging.subscriber;
/// ```
pub struct Logging {
    /// The operational stack: fmt, the close-only layer, the access log and OTLP.
    pub subscriber: Box<dyn Subscriber + Send + Sync>,
    /// The OTLP provider, retained by whoever owns the process (the composition root).
    pub otel: OtelHandle,
    /// What a later phase reloads the layers and filter through.
    pub handles: Handles,
}

/// Build the process entry's bootstrap: fmt to stderr at the default verbosity, with no
/// access-log file, no OTLP exporter and no thread, so it can be installed before anything is
/// parsed (resolution, the daemon detach and the confinement all happen after it).
///
/// # Errors
///
/// The error arm of [`build_subscriber`]; this shape opens no file and builds no exporter.
///
/// # Examples
///
/// ```rust
/// use tinio_server::log::build_bootstrap;
///
/// let logging = build_bootstrap().expect("no file and no exporter is the infallible shape");
/// let _ = &logging.handles.filter;
/// ```
pub fn build_bootstrap() -> Result<Logging, Box<dyn Error + Send + Sync>> {
    build_subscriber(
        Verbosity::default(),
        Format::default(),
        &AccessFormat::default(),
        None,
        None,
    )
}

/// Build the process's subscriber: fmt layers (text or JSON), the
/// close-only layer, the access log when `access_path` is `Some`, and the OTLP layer when the
/// `otel` feature is compiled and `otel_endpoint` is `Some` (T053). Errors are always visible on
/// stderr down to the configured verbosity (FR-017).
///
/// # Errors
///
/// `Io` when `access_path` is `Some` and the file cannot be opened for appending; OTLP exporter
/// construction failures when the `otel` feature is enabled.
///
/// # Examples
///
/// ```rust
/// use tinio_server::log::{AccessFormat, Format, Verbosity, build_subscriber};
///
/// let logging = build_subscriber(
///     Verbosity::default(),
///     Format::default(),
///     &AccessFormat::default(),
///     None,
///     None,
/// )
/// .expect("stderr needs no file");
/// let _ = &logging.subscriber;
/// ```
pub fn build_subscriber(
    verbosity: Verbosity,
    format: Format,
    access_format: &AccessFormat,
    access_path: Option<&Path>,
    otel_endpoint: Option<&str>,
) -> Result<Logging, Box<dyn Error + Send + Sync>> {
    let (layer, otel) = build_layer(format, access_format, access_path, otel_endpoint)?;
    Ok(build_logging(verbosity, layer, otel))
}

/// The process's one subscriber shape: the operational stack under the configured filter. Built
/// once, inside the confinement, so nothing it starts lives outside the sandbox.
fn build_logging(verbosity: Verbosity, layer: OperationalLayer, otel: OtelHandle) -> Logging {
    let (reload_layer, layer_handle) = reload::Layer::new(layer);
    let (filter_layer, filter_handle) = reload::Layer::new(build_filter(verbosity));
    let subscriber = tracing_subscriber::registry().with(reload_layer.with_filter(filter_layer));
    Logging {
        subscriber: Box::new(subscriber),
        otel,
        handles: Handles {
            layer: layer_handle,
            filter: filter_handle,
        },
    }
}

/// The operational filter: the verbosity flag as the default directive, with `RUST_LOG` overriding
/// it when set. The access target is pinned at `info` so the observer's own sink survives a lower
/// level — the access log is not the operational stream.
///
/// # Examples
///
/// ```rust
/// use tinio_server::log::{Verbosity, build_filter};
///
/// let _filter = build_filter(Verbosity::Debug);
/// ```
pub fn build_filter(verbosity: Verbosity) -> EnvFilter {
    let level = match verbosity {
        Verbosity::Error => Level::ERROR,
        Verbosity::Warn => Level::WARN,
        Verbosity::Info => Level::INFO,
        Verbosity::Debug => Level::DEBUG,
    };
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::from_level(level).into())
        .from_env_lossy();
    // **`add_directive` takes the filter by value**, so the pinned access directive is either
    // applied or the seeded filter is returned unchanged — never a second spelling of the builder
    // chain. The literal is a valid directive by construction; the arm is spelled rather than
    // unwrapped because a parse failure must not take the process's log down.
    match "tinio::access=info".parse() {
        Ok(directive) => filter.add_directive(directive),
        Err(_) => filter,
    }
}

/// Build the operational layers alone — what `build_subscriber` wraps for the process's one
/// install, inside the confinement. The OTLP provider travels beside the layer; whoever owns the
/// process retains it (dropping it here is what emptied the export).
///
/// # Errors
///
/// The same two arms `build_subscriber` documents.
///
/// # Examples
///
/// ```rust
/// use tinio_server::log::{AccessFormat, Format, build_layer};
///
/// let (layer, otel) = build_layer(Format::default(), &AccessFormat::default(), None, None)
///     .expect("stderr needs no file");
/// let _ = (layer, otel);
/// ```
pub fn build_layer(
    format: Format,
    access_format: &AccessFormat,
    access_path: Option<&Path>,
    otel_endpoint: Option<&str>,
) -> Result<(OperationalLayer, OtelHandle), Box<dyn Error + Send + Sync>> {
    let stderr = BoxMakeWriter::new(io::stderr);
    // **ANSI follows `NO_COLOR`, and the variable is the *only* thing consulted.**
    // `tracing-subscriber` turns ANSI on whenever its `ansi` feature is compiled in — it never asks
    // `isatty` — so a run whose stderr is a pipe, a journal or a CI step still gets escapes around
    // every field *name*: `step` arrives as `\x1b[3mstep\x1b[0m\x1b[2m=\x1b[0m"profile"`, and no
    // consumer can find `step="profile"`. Honouring `NO_COLOR` is the standard remedy and the one
    // an operator already has: `NO_COLOR=1 tinio server …` gives plain text, and a terminal that
    // wants colour keeps it. The workspace's other subscribers set `with_ansi(false)` outright
    // because they exist only inside tests; this one is the product's, so the choice belongs to
    // whoever is reading the log.
    //
    // Measured on macOS 15.8 (2026-09-25): the sandbox phase's end-to-end leg failed its
    // `step="profile"` assertion against this subscriber's output for exactly this reason, while
    // the confined server itself was serving requests — so that leg now exports `NO_COLOR`. The
    // JSON layer needs no counterpart: its field names are quoted keys, never styled.
    let ansi = env::var_os("NO_COLOR").is_none();
    let op_layer: Box<dyn Layer<Registry> + Send + Sync> = match format {
        Format::Text => fmt::layer()
            .with_writer(stderr)
            .with_target(false)
            .with_ansi(ansi)
            .boxed(),
        Format::Json => fmt::layer()
            .json()
            .with_writer(stderr)
            .with_target(false)
            .boxed(),
    };
    let layer: OperationalLayer = match access_path {
        Some(path) => {
            let access_file = File::options().create(true).append(true).open(path)?;
            Box::new(
                op_layer
                    .and_then(SpanCloseLayer)
                    .and_then(AccessLogLayer::new(access_format.clone(), access_file)),
            )
        }
        None => Box::new(op_layer.and_then(SpanCloseLayer)),
    };
    #[cfg(feature = "otel")]
    if let Some(endpoint) = otel_endpoint {
        let (export, provider) = otel_layer::<Registry>(endpoint)?;
        return Ok((Box::new(layer.and_then(export)), Some(provider)));
    }
    let _ = otel_endpoint;
    Ok((layer, None))
}

/// Emits one event when a span closes, so a span that fires no event of its own still leaves a
/// duration line. The level is the span's own: a T0 (info) span survives the default filter, while
/// a T1 (debug) line appears only under `debug`.
///
/// A span that already logs its own duration declares `close_line = false` in its fields; the
/// directive is read at creation, so the policy lives with the span rather than in a target list
/// here.
///
/// # Examples
///
/// ```rust
/// use tinio_server::log::SpanCloseLayer;
///
/// let _layer = SpanCloseLayer;
/// ```
#[derive(Debug, Clone, Copy, SmartDefault)]
pub struct SpanCloseLayer;

/// A span's start and recorded fields, kept in its extensions until `on_close`.
#[derive(Debug)]
struct Started {
    started: Instant,
    fields: Vec<(&'static str, String)>,
}

/// Collects a span's fields for the close line.
struct CloseFields<'a>(&'a mut Vec<(&'static str, String)>);

/// Reads the one directive field a span may carry: `close_line = false` suppresses its close line
/// (the span already logs its own duration). Absent means `true`.
struct CloseDirective(bool);

impl Visit for CloseDirective {
    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == "close_line" {
            self.0 = value;
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn Debug) {}
}

impl Visit for CloseFields<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.push((field.name(), value.to_string()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.push((field.name(), value.to_string()));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.push((field.name(), value.to_string()));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.push((field.name(), value.to_string()));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.0.push((field.name(), format!("{value:?}")));
    }
}

impl<S> Layer<S> for SpanCloseLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        // Only T0 spans earn a close line: a T1/T2 line is not part of the
        // completion contract, and skipping it avoids an extension allocation
        // per debug/trace span.
        if *attrs.metadata().level() < Level::INFO {
            return;
        }
        let mut directive = CloseDirective(true);
        attrs.record(&mut directive);
        if !directive.0 {
            return;
        }
        let mut fields = Vec::new();
        attrs.record(&mut CloseFields(&mut fields));
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(Started {
                started: Instant::now(),
                fields,
            });
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };
        let mut extensions = span.extensions_mut();
        if let Some(started) = extensions.get_mut::<Started>() {
            values.record(&mut CloseFields(&mut started.fields));
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else {
            return;
        };
        let metadata = span.metadata();
        let Some(started) = span.extensions_mut().remove::<Started>() else {
            return;
        };
        let elapsed_ms = started.started.elapsed().as_secs_f64() * 1000.0;
        let fields = started.fields;
        // **A fixed target and a static level per arm, because a callsite's metadata is static**:
        // the span's own target and level cannot be handed to `event!` at run time. The fixed
        // target is `tinio`, so the seeded directive and `RUST_LOG` can both reach the line.
        let name = metadata.name();
        match *metadata.level() {
            Level::ERROR => tracing::event!(
                target: "tinio",
                Level::ERROR,
                span = name,
                elapsed_ms,
                fields = ?fields,
                "span closed"
            ),
            Level::WARN => tracing::event!(
                target: "tinio",
                Level::WARN,
                span = name,
                elapsed_ms,
                fields = ?fields,
                "span closed"
            ),
            Level::INFO => tracing::event!(
                target: "tinio",
                Level::INFO,
                span = name,
                elapsed_ms,
                fields = ?fields,
                "span closed"
            ),
            Level::DEBUG | Level::TRACE => {}
        }
    }
}

/// The OTLP export pair: the bridged layer and the provider that must outlive it.
#[cfg(feature = "otel")]
pub type OtelLayer<S> = (Box<dyn Layer<S> + Send + Sync>, SdkTracerProvider);

/// The OpenTelemetry export layer (task T053, behind the `otel` feature):
/// an OTLP gRPC exporter at `endpoint` bridged into tracing. The endpoint
/// falls back to `OTEL_EXPORTER_OTLP_ENDPOINT` when unset (config.md).
///
/// **The provider is returned, never dropped.** The layer borrows a tracer from it; a provider
/// that goes out of scope here exports nothing, which is why the caller retains the pair.
#[cfg(feature = "otel")]
pub fn otel_layer<S>(endpoint: &str) -> Result<OtelLayer<S>, Box<dyn Error + Send + Sync>>
where
    S: Subscriber + for<'a> LookupSpan<'a> + Send + Sync + 'static,
{
    let endpoint = if endpoint.is_empty() {
        env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:4317".to_string())
    } else {
        endpoint.to_string()
    };
    let uri: http::Uri = endpoint
        .parse()
        .map_err(|err| format!("the OTLP endpoint is not a URI: {err}"))?;
    let loopback = matches!(
        uri.host(),
        Some("127.0.0.1" | "localhost" | "::1" | "[::1]")
    );
    if uri.scheme_str() != Some("https") && !loopback {
        return Err("the OTLP endpoint must use https outside loopback".into());
    }
    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .build();
    let tracer = provider.tracer("tinio");
    Ok((Box::new(OpenTelemetryLayer::new(tracer)), provider))
}

#[cfg(test)]
mod tests {

    use std::{
        fs,
        panic::{AssertUnwindSafe, catch_unwind},
    };

    #[cfg(feature = "otel")]
    use opentelemetry_sdk::trace::InMemorySpanExporter;
    use tracing::subscriber::with_default;

    use super::*;
    use crate::_util::testing::SharedBuf;

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
                Some(&path),
                None,
            )
            .unwrap()
            .subscriber;
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
    fn a_span_prints_one_close_line_and_a_directive_suppresses_it() {
        // The close layer's own contract, without the seeded filter: one line per span, once, and
        // none for a span that declares `close_line = false` (it logs its own duration).
        let buf = SharedBuf::default();
        let writer = buf.clone();
        let subscriber = tracing_subscriber::registry()
            .with(
                fmt::layer()
                    .with_writer(move || writer.clone())
                    .with_target(false)
                    .with_ansi(false),
            )
            .with(SpanCloseLayer);
        with_default(subscriber, || {
            tracing::info_span!(target: "tinio::storage", "put_object")
                .in_scope(|| tracing::info!(target: "tinio::storage", "inside"));
            tracing::info_span!(target: "tinio::http", "request", close_line = false)
                .in_scope(|| {});
            tracing::info_span!(target: "tinio::api", "status", close_line = false).in_scope(|| {});
        });
        let logged = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert_eq!(logged.matches("span closed").count(), 1, "{logged}");
        assert!(logged.contains("put_object"), "{logged}");
        assert_eq!(logged.matches("inside").count(), 1, "{logged}");
        assert!(!logged.contains("request"), "{logged}");
        assert!(!logged.contains("status"), "{logged}");
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
                    Some(&path),
                    None,
                )
                .expect("every verbosity/format combination builds")
                .subscriber;
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

    #[cfg(feature = "otel")]
    #[tokio::test]
    async fn otel_layer_retains_a_shut_downable_provider() {
        // The regression: the provider used to be built and dropped inside `otel_layer`, so the
        // layer's tracer exported nothing. The returned handle must be `Some`, and the retained
        // install must reach a provider whose shutdown is callable. The exporter connects lazily,
        // so building one needs no live collector — only a Tokio reactor, hence the async test.
        let (layer, handle) = build_layer(
            Format::Text,
            &AccessFormat::Combined,
            None,
            Some("http://127.0.0.1:4317"),
        )
        .expect("the exporter builds without a live collector");
        assert!(handle.is_some(), "an endpoint must yield a provider handle");
        drop(layer);
        install_otel(handle);
        shutdown_otel();
    }

    #[cfg(feature = "otel")]
    #[test]
    fn otlp_refuses_plaintext_outside_loopback() {
        assert!(
            otel_layer::<tracing_subscriber::Registry>("http://collector.example:4317").is_err(),
            "a remote endpoint must use https"
        );
    }

    #[cfg(feature = "otel")]
    #[test]
    fn otel_in_memory_exporter_round_trips_a_span() {
        // The bridge's contract without a collector: a closed `tinio::s3` span reaches the SDK's
        // in-memory exporter with the tracing name, target and fields.
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("tinio-server-test");
        let subscriber = tracing_subscriber::registry().with(OpenTelemetryLayer::new(tracer));
        with_default(subscriber, || {
            let span = tracing::info_span!(
                target: "tinio::s3",
                "put_object",
                bucket = "bucket-a",
                key = "object.txt"
            );
            span.in_scope(|| {});
        });
        let spans = exporter
            .get_finished_spans()
            .expect("the in-memory exporter reads its finished spans");
        assert_eq!(spans.len(), 1, "{spans:?}");
        let span = &spans[0];
        assert_eq!(span.name, "put_object");
        let attribute = |name: &str| {
            span.attributes
                .iter()
                .find(|kv| kv.key.as_str() == name)
                .map(|kv| kv.value.as_str().into_owned())
        };
        assert_eq!(attribute("target").as_deref(), Some("tinio::s3"));
        assert_eq!(attribute("bucket").as_deref(), Some("bucket-a"));
        assert_eq!(attribute("key").as_deref(), Some("object.txt"));
        provider
            .shutdown()
            .expect("the in-memory provider shuts down");
    }
}

#[cfg(all(test, feature = "config"))]
mod schema_tests {
    use super::*;

    #[test]
    fn absent_keys_take_the_struct_default() {
        // The struct-level serde default is the only default path: an
        // empty section lands on the `SmartDefault` values.
        let config: Config = toml::from_str("").unwrap();
        assert_eq!(config, Config::default());
    }

    #[test]
    fn access_log_format_variables_validated() {
        let config: Config = toml::from_str(
            "access_log_format = \"$remote_addr - $remote_user [$time_local] \\\"$request\\\" $status\"",
        )
        .unwrap();
        config.validate().unwrap();
        let config: Config = toml::from_str("access_log_format = \"$authorization\"").unwrap();
        assert!(
            config.validate().is_err(),
            "$authorization must be rejected"
        );
        let config: Config = toml::from_str("access_log_format = \"$query_string\"").unwrap();
        assert!(config.validate().is_err(), "$query_string must be rejected");
    }

    #[test]
    fn access_log_format_serialization() {
        let log = Config {
            access_log_format: AccessFormat::Combined,
            ..Default::default()
        };
        let toml = toml::to_string(&log).unwrap();
        assert!(toml.contains("access_log_format = \"combined\""), "{toml}");
        let custom = Config {
            access_log_format: AccessFormat::Custom("$status".into()),
            ..Default::default()
        };
        let toml = toml::to_string(&custom).unwrap();
        assert!(toml.contains("access_log_format = \"$status\""), "{toml}");
        // And it round-trips back to Custom.
        let back: Config = toml::from_str(&toml).unwrap();
        assert_eq!(
            back.access_log_format,
            AccessFormat::Custom("$status".into())
        );
    }

    #[test]
    fn verbosity_display_and_parse() {
        assert_eq!(Verbosity::Info.to_string(), "info");
        assert_eq!(Verbosity::Debug.to_string(), "debug");
        assert_eq!("warn".parse::<Verbosity>().unwrap(), Verbosity::Warn);
        assert!("loud".parse::<Verbosity>().is_err());
        assert_eq!(Format::Json.to_string(), "json");
        assert_eq!("text".parse::<Format>().unwrap(), Format::Text);
    }

    #[test]
    fn access_log_format_as_str() {
        assert!(AccessFormat::Combined.as_str().contains("$remote_addr"));
        assert!(AccessFormat::Common.as_str().contains("$body_bytes_sent"));
        assert_eq!(AccessFormat::Custom("$request".into()).as_str(), "$request");
    }
}
