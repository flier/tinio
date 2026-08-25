use garde::Validate;
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

/// The S3 data-plane listener (`[server]`).
///
/// # Examples
///
/// ```rust
/// use tinio_config::server::Config;
///
/// let s = Config::default();
/// assert_eq!(s.port, 9000); // Minio-compatible default; 0 = ephemeral
/// assert!(!s.read_only);
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Config {
    /// Bind host (default `127.0.0.1`).
    #[serde(default = "host")]
    #[default = r#"127.0.0.1"#]
    pub host: String,
    /// Bind port (default 9000; `0` = OS-assigned ephemeral).
    #[serde(default = "port")]
    #[default = 9000]
    pub port: u16,
    /// Read-only mode (FR-023): reject all S3 writes, state relocates to
    /// `~/.tinio/roots/<hash>/`.
    #[serde(default)]
    pub read_only: bool,
}

fn host() -> String {
    Config::default().host
}

fn port() -> u16 {
    Config::default().port
}
