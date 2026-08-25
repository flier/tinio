use std::path::{Path, PathBuf};

use garde::Validate;
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

fn validate_non_empty_path(value: &Path, _context: &()) -> garde::Result {
    super::reject_empty("path must not be empty", value.as_os_str().is_empty())
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
/// use tinio_config::api::Config;
///
/// let api = Config::default();
/// #[cfg(unix)]
/// assert!(api.unix.is_none());
/// #[cfg(windows)]
/// assert!(api.pipe.is_none());
/// assert!(api.http.is_none() && api.https.is_none());
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, Validate)]
pub struct Config {
    /// Local channel, unix form (`[api.unix]`, Linux/macOS): socket path
    /// relative to the state dir, or absolute.
    #[cfg(unix)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub unix: Option<Unix>,
    /// Local channel, Windows form (`[api.pipe]`): named-pipe name (empty =
    /// derived `tinio-<sha1>`).
    #[cfg(windows)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub pipe: Option<Pipe>,
    /// TCP HTTP exposure (token required on ALL endpoints).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub http: Option<Http>,
    /// TCP HTTPS exposure (cert + key required; token required on ALL
    /// endpoints).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub https: Option<Https>,
}

/// The local management channel, unix form (`[api.unix]`, Linux/macOS):
/// socket path relative to the state dir, or absolute (default `tinio.sock`).
///
/// # Examples
///
/// ```rust
/// use tinio_config::api::Unix;
///
/// let unix = Unix::default();
/// assert!(unix.path.as_os_str().is_empty()); // default: tinio.sock
/// ```
#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, Validate)]
#[garde(allow_unvalidated)]
pub struct Unix {
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
/// use tinio_config::api::Pipe;
///
/// let pipe = Pipe::default();
/// assert!(pipe.path.as_os_str().is_empty()); // default: derived tinio-<sha1>
/// ```
#[cfg(windows)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, Validate)]
#[garde(allow_unvalidated)]
pub struct Pipe {
    /// Named-pipe name (empty = derived `tinio-<sha1>`).
    #[serde(default)]
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Http {
    /// Bind host (default `127.0.0.1`).
    #[serde(default = "host")]
    #[default = r#"127.0.0.1"#]
    pub host: String,
    /// Bind port (default 9001).
    #[serde(default = "port")]
    #[default = 9001]
    pub port: u16,
}

/// TCP HTTPS management listener (`[api.https]`; cert + key are required).
///
/// # Examples
///
/// ```rust
/// use std::path::PathBuf;
///
/// use tinio_config::api::{Http, Https};
///
/// let https = Https {
///     http: Http {
///         host: "127.0.0.1".into(),
///         port: 9001,
///         ..Http::default()
///     },
///     cert: PathBuf::from("/path/cert.pem"),
///     key: PathBuf::from("/path/key.pem"),
/// };
/// assert_eq!(https.http.port, 9001);
/// assert!(!https.cert.as_os_str().is_empty());
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Https {
    /// Shared TCP bind (host + port) of the HTTPS listener.
    #[serde(flatten)]
    pub http: Http,
    /// PEM certificate path (required when the section is present).
    #[serde(default)]
    #[garde(custom(validate_non_empty_path))]
    pub cert: PathBuf,
    /// PEM private key path (required when the section is present).
    #[serde(default)]
    #[garde(custom(validate_non_empty_path))]
    pub key: PathBuf,
}

fn host() -> String {
    Http::default().host
}

fn port() -> u16 {
    Http::default().port
}
