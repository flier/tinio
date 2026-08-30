use garde::Validate;
use parse_display::{Display, FromStr};
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

/// Log level.
///
/// # Examples
///
/// ```rust
/// use std::str::FromStr;
///
/// use tinio_config::log::Verbosity;
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
///
/// use tinio_config::log::Format;
///
/// assert_eq!(Format::from_str("json").unwrap(), Format::Json);
/// assert_eq!(Format::default().to_string(), "text");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, Display, FromStr)]
#[serde(rename_all = "lowercase")]
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
/// use tinio_config::log::AccessFormat;
///
/// assert!(AccessFormat::default().as_str().contains("$status"));
/// let custom = AccessFormat::Custom("$remote_addr $status".into());
/// assert_eq!(custom.as_str(), "$remote_addr $status");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default, Validate)]
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
        #[garde(pattern(
            r"^([^\$]|\$(remote_addr|remote_user|time_local|request|status|body_bytes_sent|http_referer|http_user_agent|request_time))*$"
        ))]
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

impl Serialize for AccessFormat {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Combined => serializer.serialize_str("combined"),
            Self::Common => serializer.serialize_str("common"),
            Self::Custom(s) => serializer.serialize_str(s),
        }
    }
}

impl<'de> Deserialize<'de> for AccessFormat {
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
/// use tinio_config::log::Config;
///
/// let log = Config::default();
/// assert_eq!(log.access_log, "access.log");
/// assert_eq!(log.server_log_file, ""); // empty = stderr
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Config {
    /// `error | warn | info | debug` (default `info`).
    #[serde(default)]
    pub verbosity: Verbosity,
    /// Access-log file name (relative to the state dir, or absolute).
    #[serde(default = "access_log")]
    #[default = r#"access.log"#]
    pub access_log: String,
    /// `combined | common | custom nginx-style string` (default `combined`).
    #[serde(default)]
    #[garde(dive)]
    pub access_log_format: AccessFormat,
    /// `text | json` (default `text`; json defaults the file to `server.json`).
    #[serde(default)]
    pub server_log_format: Format,
    /// Server-log file (empty = stderr; daemon mode defaults it).
    #[serde(default)]
    pub server_log_file: String,
}

fn access_log() -> String {
    Config::default().access_log
}
