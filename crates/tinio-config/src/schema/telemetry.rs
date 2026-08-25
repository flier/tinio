use garde::Validate;
use serde::{Deserialize, Serialize};

/// OpenTelemetry export (`[telemetry]`; requires the `otel` cargo feature).
///
/// Presence of the section requires a valid `otlp_endpoint` URL; omit the
/// section to disable export.
///
/// # Examples
///
/// ```rust
/// use tinio_config::telemetry::Config;
///
/// let t = Config::default();
/// assert_eq!(t.otlp_endpoint, ""); // unset; a present section must carry a valid URL
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, Validate)]
#[garde(allow_unvalidated)]
pub struct Config {
    /// OTLP gRPC endpoint (`http://...`); required when the section is
    /// present — an empty value is a validation error (omit the section to
    /// disable).
    #[serde(default)]
    #[garde(url)]
    pub otlp_endpoint: String,
}
