//! Configuration schema: the `Config` struct and its sections.
//!
//! Pure data types with serde attributes (TOML shape), `SmartDefault` field
//! defaults, and garde validation attributes. Unknown keys are not rejected
//! by serde — [`Config::parse_at`] collects them via `serde_ignored` and
//! reports [`Error::UnknownKey`] (FR-016, fail-fast).
//! Sections are presence-gated: absent optional sections
//! parse as `None` and are skipped when the config is re-serialized.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::Duration,
};

use garde::Validate;
use parse_display::{Display, FromStr};
use secrecy::{CloneableSecret, ExposeSecret, SecretBox, SerializableSecret, zeroize::Zeroize};
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

use crate::Error;

/// Config format version (currently only `1`).
///
/// # Examples
///
/// ```rust
/// use tinio_config::Config;
///
/// assert!(Config::parse("version = 1").is_ok());
/// assert!(Config::parse("version = 2").is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, SmartDefault, Serialize, Deserialize, Validate)]
#[serde(transparent)]
pub struct Version(
    #[garde(range(min = 1, max = 1))]
    #[default = 1]
    u32,
);

/// The resolved server configuration (contracts/config.md).
///
/// Parsed from the TOML config file with fail-fast validation: unknown keys
/// and sections are rejected (collected by `serde_ignored`), value rules are
/// enforced by garde (presence-gated sections, port rules, HTTPS cert/key,
/// credential pairs, the closed access-log variable set), and violations
/// fail startup.
///
/// # Examples
///
/// ```rust
/// use tinio_config::Config;
///
/// let config = Config::parse(
///     r#"
///     version = 1
///
///     [server]
///     host = "0.0.0.0"
///     port = 9000
///
///     [s3]
///     multipart = false
///     "#,
/// )
/// .unwrap();
/// assert_eq!(config.server.host, "0.0.0.0");
/// assert_eq!(config.server.port, 9000);
/// assert!(!config.s3.as_ref().unwrap().multipart);
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
pub struct Config {
    /// Config format version.
    #[serde(default)]
    #[garde(dive)]
    pub version: Version,
    /// The S3 data-plane listener.
    #[serde(default)]
    #[garde(dive)]
    pub server: ServerConfig,
    /// Background ETag scanner (presence = on, FR-024).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub scanner: Option<ScannerConfig>,
    /// S3 credentials (generated on first start).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub auth: Option<AuthConfig>,
    /// Logging configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub log: Option<LogConfig>,
    /// S3 capability toggles (runtime level; compile-time features strip the
    /// code, FR-021).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub s3: Option<S3Config>,
    /// Backend behavior keys (filesystem-only in v1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub storage: Option<StorageConfig>,
    /// Management-plane transports (presence-gated subsections).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub api: Option<ApiConfig>,
    /// OpenTelemetry export (opt-in; requires the `otel` feature).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub telemetry: Option<TelemetryConfig>,
}

impl Config {
    /// Parse a TOML string and run fail-fast validation.
    pub fn parse(input: &str) -> Result<Self, Error> {
        Self::parse_at(input, Path::new("(inline)"))
    }

    /// Read, parse, and validate a config file.
    pub fn load(path: &Path) -> Result<Self, Error> {
        let text = std::fs::read_to_string(path).map_err(|e| crate::error::io(path, e))?;
        Self::parse_at(&text, path)
    }

    /// Parse + validation with errors labeled by `path`.
    fn parse_at(input: &str, path: &Path) -> Result<Self, Error> {
        let deserializer = toml::Deserializer::parse(input)
            .map_err(|e| crate::error::parse(path, e.to_string()))?;
        let mut ignored = BTreeSet::new();
        let config: Config = match serde_ignored::deserialize(deserializer, |p| {
            ignored.insert(p.to_string());
        }) {
            Ok(config) => config,
            Err(e) => {
                if !ignored.is_empty() {
                    // Unknown keys were present even though deserialization
                    // also failed (e.g. a missing required field) — report
                    // them all, matching the old deny-fail-fast.
                    return Err(crate::error::unknown_key(sort_keys(&ignored)));
                }
                return Err(crate::error::parse(path, e.to_string()));
            }
        };
        if !ignored.is_empty() {
            return Err(crate::error::unknown_key(sort_keys(&ignored)));
        }
        config.validate().map_err(Error::from_report)?;
        Ok(config)
    }

    /// Serialize to TOML (used by the first-start auto-create).
    pub fn to_toml(&self) -> Result<String, Error> {
        toml::to_string(self)
            .map_err(|e| crate::error::parse(Path::new("(serialization)"), e.to_string()))
    }
}

/// The ignored key paths as one comma-separated message — the `BTreeSet`
/// keeps iteration (and therefore the error text) deterministic.
fn sort_keys(keys: &BTreeSet<String>) -> String {
    keys.iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(", ")
}

/// The S3 data-plane listener (`[server]`).
///
/// # Examples
///
/// ```rust
/// use tinio_config::ServerConfig;
///
/// let s = ServerConfig::default();
/// assert_eq!(s.port, 9000); // Minio-compatible default; 0 = ephemeral
/// assert!(!s.read_only);
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct ServerConfig {
    /// Bind host (default `127.0.0.1`).
    #[serde(default)]
    #[default = r#"127.0.0.1"#]
    pub host: String,
    /// Bind port (default 9000; `0` = OS-assigned ephemeral).
    #[serde(default)]
    #[default = 9000]
    pub port: u16,
    /// Read-only mode (FR-023): reject all S3 writes, state relocates to
    /// `~/.tinio/roots/<hash>/`.
    #[serde(default)]
    pub read_only: bool,
}

/// The background ETag scanner (`[scanner]`; presence = on, FR-024).
///
/// Keys are Minio-aligned (`mc admin config set myminio scanner ...`).
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use tinio_config::ScannerConfig;
///
/// let s = ScannerConfig::default();
/// assert_eq!(s.delay, 10.0);
/// assert_eq!(s.max_wait, Duration::from_secs(15));
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct ScannerConfig {
    /// Seconds between scan iterations (pacing/throttle), >= 0.
    #[serde(default)]
    #[garde(range(min = 0.0))]
    #[default = 10.0]
    pub delay: f64,
    /// Max time to wait for a scan slot when throttled.
    #[serde(default, with = "humantime_serde")]
    #[default(_code = "Duration::from_secs(15)")]
    pub max_wait: Duration,
    /// Full-tree scan cycle (re-scan for out-of-band changes).
    #[serde(default, with = "humantime_serde")]
    #[default(_code = "Duration::from_secs(24 * 60 * 60)")]
    pub cycle: Duration,
}

/// Secret key material: zeroized on drop; [`SerializableSecret`] opts into serde
/// serialization for config round-trips.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecretKey(String);

impl Zeroize for SecretKey {
    fn zeroize(&mut self) {
        self.0.zeroize();
    }
}

impl SerializableSecret for SecretKey {}

impl CloneableSecret for SecretKey {}

impl std::ops::Deref for SecretKey {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&str> for SecretKey {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl From<String> for SecretKey {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<SecretKey> for SecretBox<SecretKey> {
    fn from(key: SecretKey) -> Self {
        SecretBox::new(Box::new(key))
    }
}

fn validate_auth_secret_key(value: &SecretBox<SecretKey>, _context: &()) -> garde::Result {
    if value.expose_secret().is_empty() {
        Err(garde::Error::new("auth.secret_key must not be empty"))
    } else {
        Ok(())
    }
}

/// S3 credentials (`[auth]`; optional section — when present, both keys are
/// required; generated on first start with ≥ 16/32 bytes CSPRNG, per
/// data-model.md Credentials).
///
/// There is deliberately no `anonymous` key — anonymous mode is flag/env
/// only (the key is rejected as unknown).
///
/// # Examples
///
/// ```rust
/// use secrecy::ExposeSecret;
/// use tinio_config::Config;
///
/// let config = Config::parse(
///     r#"
///     version = 1
///     [auth]
///     access_key = "minioadmin"
///     secret_key = "minioadmin-secret"
///     "#,
/// )
/// .unwrap();
/// let auth = config.auth.as_ref().unwrap();
/// assert!(!auth.access_key.is_empty());
/// assert!(!auth.secret_key.expose_secret().is_empty());
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct AuthConfig {
    /// Access key (≥ 16 bytes when generated).
    #[garde(length(min = 1))]
    pub access_key: String,
    /// Secret key (≥ 32 bytes when generated).
    #[garde(custom(validate_auth_secret_key))]
    pub secret_key: SecretBox<SecretKey>,
}

impl PartialEq for AuthConfig {
    fn eq(&self, other: &Self) -> bool {
        self.access_key == other.access_key
            && self.secret_key.expose_secret() == other.secret_key.expose_secret()
    }
}

/// Log level.
///
/// # Examples
///
/// ```rust
/// use std::str::FromStr;
/// use tinio_config::Verbosity;
///
/// assert_eq!(Verbosity::from_str("debug").unwrap(), Verbosity::Debug);
/// assert_eq!(Verbosity::default().to_string(), "info");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, Display, FromStr)]
#[serde(rename_all = "lowercase")]
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
/// use tinio_config::LogFormat;
///
/// assert_eq!(LogFormat::from_str("json").unwrap(), LogFormat::Json);
/// assert_eq!(LogFormat::default().to_string(), "text");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, Display, FromStr)]
#[serde(rename_all = "lowercase")]
#[display(style = "lowercase")]
pub enum LogFormat {
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
/// use tinio_config::AccessLogFormat;
///
/// assert!(AccessLogFormat::default().as_str().contains("$status"));
/// let custom = AccessLogFormat::Custom("$remote_addr $status".into());
/// assert_eq!(custom.as_str(), "$remote_addr $status");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default, Validate)]
pub enum AccessLogFormat {
    /// Nginx combined: `$remote_addr - $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"`.
    #[default]
    Combined,
    Common,
    Custom(
        // FR-017 closed set, as a pattern gate. Known boundary: the regex
        // matches the longest listed variable, so a name with an identifier
        // suffix (e.g. `$statusx`) passes — acceptable while the formatter
        // resolves the set by exact name lookup at serve time.
        #[garde(pattern(
            r"^([^\$]|\$(remote_addr|remote_user|time_local|request|status|body_bytes_sent|http_referer|http_user_agent|request_time))*$"
        ))]
        String,
    ),
}

impl AccessLogFormat {
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

impl Serialize for AccessLogFormat {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Combined => serializer.serialize_str("combined"),
            Self::Common => serializer.serialize_str("common"),
            Self::Custom(s) => serializer.serialize_str(s),
        }
    }
}

impl<'de> Deserialize<'de> for AccessLogFormat {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
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
/// use tinio_config::LogConfig;
///
/// let log = LogConfig::default();
/// assert_eq!(log.access_log, "access.log");
/// assert_eq!(log.server_log_file, ""); // empty = stderr
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct LogConfig {
    /// `error | warn | info | debug` (default `info`).
    #[serde(default)]
    pub verbosity: Verbosity,
    /// Access-log file name (relative to the state dir, or absolute).
    #[serde(default)]
    #[default = r#"access.log"#]
    pub access_log: String,
    /// `combined | common | custom nginx-style string` (default `combined`).
    #[serde(default)]
    #[garde(dive)]
    pub access_log_format: AccessLogFormat,
    /// `text | json` (default `text`; json defaults the file to `server.json`).
    #[serde(default)]
    pub server_log_format: LogFormat,
    /// Server-log file (empty = stderr; daemon mode defaults it).
    #[serde(default)]
    pub server_log_file: String,
}

/// S3 capability toggles (`[s3]`; runtime level, FR-021). Disabled groups
/// return `NotImplemented`.
///
/// # Examples
///
/// ```rust
/// use tinio_config::S3Config;
///
/// let s3 = S3Config::default();
/// assert!(s3.multipart);
/// assert!(!s3.sig_v2); // deprecated, off by default
/// assert_eq!(s3.temp_ttl_hours, 24);
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct S3Config {
    /// Multipart operations + `upload_part_copy`.
    #[serde(default)]
    #[default = true]
    pub multipart: bool,
    /// Server-side `copy_object`.
    #[serde(default)]
    #[default = true]
    pub copy_object: bool,
    /// ListObjects (V1).
    #[serde(default)]
    #[default = true]
    pub list_objects_v1: bool,
    /// ListObjectsV2.
    #[serde(default)]
    #[default = true]
    pub list_objects_v2: bool,
    /// DeleteObjects (batch).
    #[serde(default)]
    #[default = true]
    pub delete_objects: bool,
    /// SigV2 verification (deprecated; enabling prints a startup warning).
    #[serde(default)]
    pub sig_v2: bool,
    /// Stale temp-write sweep timeout (hours).
    #[serde(default)]
    #[default = 24]
    pub temp_ttl_hours: u64,
    /// Abandoned-upload sweep timeout (days).
    #[serde(default)]
    #[default = 7]
    pub multipart_expire_days: u64,
}

/// Backend behavior keys (`[storage]`; filesystem-only in v1).
///
/// # Examples
///
/// ```rust
/// use tinio_config::StorageConfig;
///
/// let storage = StorageConfig::default();
/// assert!(storage.follow_symlinks);
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct StorageConfig {
    /// Follow symlinks in the storage root (false = reject access through
    /// links and exclude them from listings).
    #[serde(default)]
    #[default = true]
    pub follow_symlinks: bool,
}

/// Management-plane transports (`[api]`; each subsection is presence-gated:
/// present = on, absent = off). The three transports are mutually exclusive
/// — exactly one of `unix`/`http`/`https` may be enabled (three-choose-one,
/// contracts/config.md). The local channel is `[api.unix]` on Linux/macOS
/// and `[api.pipe]` on Windows — the wrong-platform section is rejected as
/// an unknown key (the other platform's field does not exist here). Not
/// validated further: the startup orchestration (US2) enforces the
/// three-choose-one after resolution.
///
/// # Examples
///
/// ```rust
/// use tinio_config::ApiConfig;
///
/// let api = ApiConfig::default();
/// #[cfg(unix)]
/// assert!(api.unix.is_none());
/// #[cfg(windows)]
/// assert!(api.pipe.is_none());
/// assert!(api.http.is_none() && api.https.is_none());
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, Validate)]
pub struct ApiConfig {
    /// Local channel, unix form (`[api.unix]`, Linux/macOS): socket path
    /// relative to the state dir, or absolute.
    #[cfg(unix)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub unix: Option<ApiUnix>,
    /// Local channel, Windows form (`[api.pipe]`): named-pipe name (empty =
    /// derived `tinio-<sha1>`).
    #[cfg(windows)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub pipe: Option<ApiPipe>,
    /// TCP HTTP exposure (token required on ALL endpoints).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub http: Option<ApiHttp>,
    /// TCP HTTPS exposure (cert + key required; token required on ALL
    /// endpoints).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub https: Option<ApiHttps>,
}

/// The local management channel, unix form (`[api.unix]`, Linux/macOS):
/// socket path relative to the state dir, or absolute (default `tinio.sock`).
///
/// # Examples
///
/// ```rust
/// use tinio_config::ApiUnix;
///
/// let unix = ApiUnix::default();
/// assert!(unix.path.as_os_str().is_empty()); // default: tinio.sock
/// ```
#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, Validate)]
#[garde(allow_unvalidated)]
pub struct ApiUnix {
    /// Socket path (relative to the state dir, or absolute); default
    /// `tinio.sock`.
    #[serde(default)]
    pub path: PathBuf,
}

/// The local management channel, Windows form (`[api.pipe]`): named-pipe
/// name (empty = derived `tinio-<sha1>`).
///
/// # Examples
///
/// ```rust
/// use tinio_config::ApiPipe;
///
/// let pipe = ApiPipe::default();
/// assert!(pipe.path.as_os_str().is_empty()); // default: derived tinio-<sha1>
/// ```
#[cfg(windows)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, Validate)]
#[garde(allow_unvalidated)]
pub struct ApiPipe {
    /// Named-pipe name (empty = derived `tinio-<sha1>`).
    #[serde(default)]
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct ApiHttp {
    /// Bind host (default `127.0.0.1`).
    #[serde(default)]
    #[default = r#"127.0.0.1"#]
    pub host: String,
    /// Bind port (default 9001).
    #[serde(default)]
    #[default = 9001]
    pub port: u16,
}

fn validate_non_empty_path(value: &Path, _context: &()) -> garde::Result {
    if value.as_os_str().is_empty() {
        Err(garde::Error::new("path must not be empty"))
    } else {
        Ok(())
    }
}

/// TCP HTTPS management listener (`[api.https]`; cert + key are required).
///
/// # Examples
///
/// ```rust
/// use std::path::PathBuf;
///
/// use tinio_config::{ApiHttp, ApiHttps};
///
/// let https = ApiHttps {
///     http: ApiHttp {
///         host: "127.0.0.1".into(),
///         port: 9001,
///         ..ApiHttp::default()
///     },
///     cert: PathBuf::from("/path/cert.pem"),
///     key: PathBuf::from("/path/key.pem"),
/// };
/// assert_eq!(https.http.port, 9001);
/// assert!(!https.cert.as_os_str().is_empty());
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct ApiHttps {
    /// Shared TCP bind (host + port) of the HTTPS listener.
    #[serde(flatten)]
    pub http: ApiHttp,
    /// PEM certificate path (required when the section is present).
    #[serde(default)]
    #[garde(custom(validate_non_empty_path))]
    pub cert: PathBuf,
    /// PEM private key path (required when the section is present).
    #[serde(default)]
    #[garde(custom(validate_non_empty_path))]
    pub key: PathBuf,
}

/// OpenTelemetry export (`[telemetry]`; requires the `otel` cargo feature).
///
/// Presence of the section requires a valid `otlp_endpoint` URL; omit the
/// section to disable export.
///
/// # Examples
///
/// ```rust
/// use tinio_config::TelemetryConfig;
///
/// let t = TelemetryConfig::default();
/// assert_eq!(t.otlp_endpoint, ""); // unset; a present section must carry a valid URL
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, Validate)]
#[garde(allow_unvalidated)]
pub struct TelemetryConfig {
    /// OTLP gRPC endpoint (`http://...`); required when the section is
    /// present — an empty value is a validation error (omit the section to
    /// disable).
    #[serde(default)]
    #[garde(url)]
    pub otlp_endpoint: String,
}

#[cfg(test)]
mod tests;
