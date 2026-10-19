use garde::Validate;
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

/// The S3 data-plane listener (`[server]`).
///
/// # Examples
///
/// ```rust
/// use tinio_cli::config::server::Config;
///
/// let s = Config::default();
/// assert_eq!(s.port, 9000); // Minio-compatible default; 0 = ephemeral
/// assert!(!s.read_only);
/// assert!(!s.sandbox); // confinement is opt-in, on every channel
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
#[serde(default)]
pub struct Config {
    /// Bind host (default `127.0.0.1`).
    #[default(_code = "DEFAULT_HOST.to_owned()")]
    pub host: String,
    /// Bind port (default 9000; `0` = OS-assigned ephemeral).
    #[default(_code = "DEFAULT_PORT")]
    pub port: u16,
    /// Read-only mode (FR-023): reject all S3 writes, state relocates to
    /// `~/.tinio/roots/<hash>/`.
    pub read_only: bool,
    /// Process confinement (`--sandbox`, `TINIO_SANDBOX`; FR-036, design
    /// decision 10).
    pub sandbox: bool,
}

/// Default bind host: loopback, so an unconfigured server is not reachable off the host.
const DEFAULT_HOST: &str = "127.0.0.1";

/// Default bind port (Minio-compatible; `0` = OS-assigned ephemeral). Crate-visible because
/// `resolve` takes it as the port an `--address` without one gets.
pub(crate) const DEFAULT_PORT: u16 = 9000;
