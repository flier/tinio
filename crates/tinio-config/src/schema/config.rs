use std::{collections::BTreeSet, path::Path};

use garde::Validate;
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

use super::{api, auth, log, pipeline, s3, scanner, server, storage, telemetry};
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
    pub server: server::Config,
    /// Background ETag scanner (presence = on, FR-024).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub scanner: Option<scanner::Config>,
    /// S3 credentials (generated on first start).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub auth: Option<auth::Config>,
    /// Logging configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub log: Option<log::Config>,
    /// S3 capability toggles (runtime level; compile-time features strip the
    /// code, FR-021).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub s3: Option<s3::Config>,
    /// Backend behavior keys (filesystem-only in v1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub storage: Option<storage::Config>,
    /// The task pipelines (`[pipeline.io]` / `[pipeline.db]`; absent =
    /// defaults).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub pipeline: Option<pipeline::Config>,
    /// Management-plane transports (presence-gated subsections).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub api: Option<api::Config>,
    /// OpenTelemetry export (opt-in; requires the `otel` feature).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub telemetry: Option<telemetry::Config>,
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
    ///
    /// Plain `toml` (the read side's serializer, Q8): the presence-gated
    /// serde attributes still apply, so absent sections — including
    /// `[pipeline.*]` — are never emitted. The `toml_edit` write path was
    /// an unmotivated second serializer for the same format (F47).
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

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, time::Duration};

    use secrecy::ExposeSecret;

    use super::{Config, Version, auth, log};
    use crate::Error;

    #[test]
    fn parse_minimal_config() {
        let config = Config::parse("version = 1").unwrap();
        assert_eq!(config.version, Version::default());
        assert_eq!(config.server.port, 9000);
        assert_eq!(config.server.host, "127.0.0.1");
        assert!(config.scanner.is_none());
    }

    #[test]
    fn full_config_round_trips() {
        let text = r#"
        version = 1

        [server]
        host = "0.0.0.0"
        port = 9001
        read_only = false

        [scanner]
        delay = 2.5
        max_wait = "10s"
        cycle = "12h"

        [auth]
        access_key = "ak"
        secret_key = "sk"

        [log]
        verbosity = "debug"
        access_log = "custom.log"
        access_log_format = "common"
        server_log_format = "json"
        server_log_file = "server.json"

        [s3]
        multipart = false
        copy_object = true
        list_objects_v1 = true
        list_objects_v2 = true
        delete_objects = true
        sig_v2 = false
        temp_ttl_hours = 12
        multipart_expire_days = 3

        [storage.fs]
        follow_symlinks = false
        compact_threshold_percent = 50
        meta_batch_size = 64
        meta_batch_bytes = 131072

        [pipeline.io]
        workers = 4
        priority = "low"
        capacity = 2048

        [pipeline.db]
        workers = 2
        priority = "high"
        capacity = 4096

        [api.http]
        host = "127.0.0.1"
        port = 9002

        [telemetry]
        otlp_endpoint = "http://127.0.0.1:4317"
    "#;
        let config = Config::parse(text).unwrap();
        assert_eq!(config.server.port, 9001);
        assert_eq!(config.scanner.as_ref().unwrap().delay, 2.5);
        assert_eq!(config.auth.as_ref().unwrap().access_key, "ak");
        assert_eq!(
            config.log.as_ref().unwrap().verbosity,
            log::Verbosity::Debug
        );
        assert_eq!(
            config.log.as_ref().unwrap().access_log_format,
            log::AccessFormat::Common
        );
        assert!(!config.s3.as_ref().unwrap().multipart);
        assert!(!config.storage.as_ref().unwrap().fs.follow_symlinks);
        assert_eq!(
            config
                .storage
                .as_ref()
                .unwrap()
                .fs
                .compact_threshold_percent,
            50
        );
        assert_eq!(config.storage.as_ref().unwrap().fs.meta_batch_size, 64);
        assert_eq!(config.storage.as_ref().unwrap().fs.meta_batch_bytes, 131072);
        let pipeline = config.pipeline.as_ref().unwrap();
        assert_eq!(pipeline.io.workers, 4);
        assert_eq!(pipeline.io.priority, super::pipeline::Priority::Low);
        assert_eq!(pipeline.io.capacity, 2048);
        assert_eq!(pipeline.db.workers, 2);
        assert_eq!(pipeline.db.priority, super::pipeline::Priority::High);
        assert_eq!(pipeline.db.capacity, 4096);
        assert_eq!(
            config.api.as_ref().unwrap().http.as_ref().unwrap().port,
            9002
        );
        assert_eq!(
            config.telemetry.as_ref().unwrap().otlp_endpoint,
            "http://127.0.0.1:4317"
        );

        // Round-trip through TOML preserves everything.
        let again = Config::parse(&config.to_toml().unwrap()).unwrap();
        assert_eq!(config, again);
    }

    #[test]
    fn unknown_keys_rejected() {
        let err = Config::parse("version = 1\n[server]\nunknown = 1").unwrap_err();
        assert!(matches!(err, Error::UnknownKey(_)), "{err}");
        let err = Config::parse("version = 1\n[bogus]\nx = 1").unwrap_err();
        assert!(matches!(err, Error::UnknownKey(_)), "{err}");
    }

    #[test]
    fn wrong_types_rejected() {
        // Boolean-typed keys must be booleans (serde rejects strings).
        let err = Config::parse("version = 1\n[server]\nread_only = \"yes\"").unwrap_err();
        assert!(matches!(err, Error::Parse { .. }), "{err}");
        // Port must be a number in range.
        let err = Config::parse("version = 1\n[server]\nport = 70000").unwrap_err();
        assert!(matches!(err, Error::Parse { .. }), "{err}");
    }

    #[test]
    fn version_must_be_one() {
        let err = Config::parse("version = 2").unwrap_err();
        assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
    }

    #[test]
    fn partial_sections_keep_field_defaults() {
        // A present section that omits a key must still deserialize the
        // SmartDefault value. (A bare `#[serde(default)]` would use the field
        // type's Default — `[storage.fs] follow_symlinks = false` used to
        // yield compact_threshold_percent = 0 and fail garde; `[s3]
        // multipart = false` used to disable every other toggle.)
        let config = Config::parse("version = 1\n[server]").unwrap();
        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 9000);

        let config = Config::parse("version = 1\n[s3]\nmultipart = false").unwrap();
        let s3 = config.s3.as_ref().unwrap();
        assert!(!s3.multipart);
        assert!(s3.copy_object && s3.list_objects_v1 && s3.list_objects_v2 && s3.delete_objects);
        assert_eq!(s3.temp_ttl_hours, 24);
        assert_eq!(s3.multipart_expire_days, 7);

        // An empty `[s3]` section falls back to every serde default
        // helper (the toggle defaults must match `Config::default`).
        let config = Config::parse("version = 1\n[s3]").unwrap();
        let empty = config.s3.as_ref().unwrap();
        assert_eq!(empty, &super::s3::Config::default());

        let config = Config::parse("version = 1\n[storage.fs]\nfollow_symlinks = false").unwrap();
        let fs = &config.storage.as_ref().unwrap().fs;
        assert_eq!(fs.compact_threshold_percent, 20);
        assert_eq!(fs.meta_batch_size, 128);
        assert_eq!(fs.meta_batch_bytes, 262144);

        let config = Config::parse("version = 1\n[scanner]").unwrap();
        let scanner = config.scanner.as_ref().unwrap();
        assert_eq!(scanner.delay, 10.0);
        assert_eq!(scanner.max_wait, Duration::from_secs(15));
        assert_eq!(scanner.cycle, Duration::from_secs(24 * 3600));

        let config = Config::parse("version = 1\n[api.http]").unwrap();
        let http = config.api.as_ref().unwrap().http.as_ref().unwrap();
        assert_eq!(http.host, "127.0.0.1");
        assert_eq!(http.port, 9001);
    }

    #[test]
    fn compact_threshold_percent_range_validated() {
        // 5..=90 (meta-redb-spec Q2); outside → startup error.
        for bad in [4u8, 91] {
            let text = format!("version = 1\n[storage.fs]\ncompact_threshold_percent = {bad}");
            let err = Config::parse(&text).unwrap_err();
            assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
        }
        let config =
            Config::parse("version = 1\n[storage.fs]\ncompact_threshold_percent = 90").unwrap();
        assert_eq!(
            config
                .storage
                .as_ref()
                .unwrap()
                .fs
                .compact_threshold_percent,
            90
        );
        // Defaults apply when the section (or the key) is absent.
        let config = Config::parse("version = 1\n[storage]").unwrap();
        let fs = &config.storage.as_ref().unwrap().fs;
        assert!(!fs.follow_symlinks); // secure default: reject symlinks
        assert_eq!(fs.compact_threshold_percent, 20);
    }

    #[test]
    fn anonymous_key_rejected() {
        let err = Config::parse("version = 1\n[auth]\nanonymous = true").unwrap_err();
        assert!(matches!(err, Error::UnknownKey(_)), "{err}");
    }

    #[test]
    fn credential_pair_required() {
        let err = Config::parse("version = 1\n[auth]\naccess_key = \"only\"").unwrap_err();
        assert!(matches!(err, Error::Parse { .. }), "{err}");
        let err = Config::parse("version = 1\n[auth]").unwrap_err();
        assert!(matches!(err, Error::Parse { .. }), "{err}");
        let err =
            Config::parse("version = 1\n[auth]\naccess_key = \"\"\nsecret_key = \"\"").unwrap_err();
        assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
    }

    #[test]
    fn https_requires_cert_and_key() {
        let err = Config::parse("version = 1\n[api.https]\nport = 9001").unwrap_err();
        assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
        let err =
            Config::parse("version = 1\n[api.https]\ncert = \"c.pem\"\nkey = \"\"").unwrap_err();
        assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
        Config::parse("version = 1\n[api.https]\ncert = \"c.pem\"\nkey = \"k.pem\"").unwrap();
    }

    #[test]
    fn scanner_delay_must_be_non_negative() {
        let err = Config::parse("version = 1\n[scanner]\ndelay = -1.0").unwrap_err();
        assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
        Config::parse("version = 1\n[scanner]\ndelay = 0.0").unwrap();
    }

    #[test]
    fn access_log_format_variables_validated() {
        Config::parse(
            "version = 1\n[log]\naccess_log_format = \"$remote_addr - $remote_user [$time_local] \\\"$request\\\" $status\"",
        )
        .unwrap();
        let err = Config::parse("version = 1\n[log]\naccess_log_format = \"$authorization\"")
            .unwrap_err();
        assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
        let err =
            Config::parse("version = 1\n[log]\naccess_log_format = \"$query_string\"").unwrap_err();
        assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
    }

    #[test]
    fn access_log_format_serialization() {
        let log = log::Config {
            access_log_format: log::AccessFormat::Combined,
            ..Default::default()
        };
        let toml = toml::to_string(&log).unwrap();
        assert!(toml.contains("access_log_format = \"combined\""), "{toml}");
        let custom = log::Config {
            access_log_format: log::AccessFormat::Custom("$status".into()),
            ..Default::default()
        };
        let toml = toml::to_string(&custom).unwrap();
        assert!(toml.contains("access_log_format = \"$status\""), "{toml}");
        // And it round-trips back to Custom.
        let back: log::Config = toml::from_str(&toml).unwrap();
        assert_eq!(
            back.access_log_format,
            log::AccessFormat::Custom("$status".into())
        );
    }

    #[test]
    fn verbosity_display_and_parse() {
        assert_eq!(log::Verbosity::Info.to_string(), "info");
        assert_eq!(log::Verbosity::Debug.to_string(), "debug");
        assert_eq!(
            "warn".parse::<log::Verbosity>().unwrap(),
            log::Verbosity::Warn
        );
        assert!("loud".parse::<log::Verbosity>().is_err());
        assert_eq!(log::Format::Json.to_string(), "json");
        assert_eq!("text".parse::<log::Format>().unwrap(), log::Format::Text);
    }

    #[test]
    fn presence_gated_sections_serialize_away() {
        let config = Config::default();
        let toml = config.to_toml().unwrap();
        assert!(!toml.contains("scanner"), "{toml}");
        assert!(!toml.contains("api"), "{toml}");
        // Q8: the auto-generated config never emits `[pipeline.*]` sections.
        assert!(!toml.contains("pipeline"), "{toml}");
    }

    #[test]
    fn ephemeral_port_zero_allowed() {
        let config = Config::parse("version = 1\n[server]\nport = 0").unwrap();
        assert_eq!(config.server.port, 0);
    }

    #[test]
    fn version_zero_rejected() {
        let err = Config::parse("version = 0").unwrap_err();
        assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
    }

    #[test]
    fn bogus_humantime_duration_rejected() {
        let err = Config::parse("version = 1\n[scanner]\nmax_wait = \"bogus\"").unwrap_err();
        assert!(matches!(err, Error::Parse { .. }), "{err}");
    }

    #[test]
    fn config_load_missing_file_is_io_error() {
        let dir = tempfile::tempdir().unwrap();
        let err = Config::load(&dir.path().join("missing.toml")).unwrap_err();
        assert!(matches!(err, Error::Io { .. }), "{err}");
    }

    #[test]
    fn telemetry_requires_valid_endpoint_when_present() {
        // Omit the section to disable; a present section must carry a valid URL.
        let err = Config::parse("version = 1\n[telemetry]").unwrap_err();
        assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
        let err =
            Config::parse("version = 1\n[telemetry]\notlp_endpoint = \"not-a-url\"").unwrap_err();
        assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
        Config::parse("version = 1\n[telemetry]\notlp_endpoint = \"http://127.0.0.1:4317\"")
            .unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn api_pipe_section_parses() {
        let config = Config::parse("version = 1\n[api.pipe]\npath = \"tinio-ctl\"").unwrap();
        let api = config.api.as_ref().unwrap();
        assert_eq!(api.pipe.as_ref().unwrap().path, PathBuf::from("tinio-ctl"));
    }

    #[cfg(unix)]
    #[test]
    fn api_unix_section_parses() {
        let config = Config::parse("version = 1\n[api.unix]\npath = \"tinio.sock\"").unwrap();
        let api = config.api.as_ref().unwrap();
        assert_eq!(api.unix.as_ref().unwrap().path, PathBuf::from("tinio.sock"));
    }

    #[cfg(windows)]
    #[test]
    fn api_unix_section_rejected_on_windows() {
        let err = Config::parse("version = 1\n[api.unix]").unwrap_err();
        assert!(matches!(err, Error::UnknownKey(_)), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn api_pipe_section_rejected_on_unix() {
        let err = Config::parse("version = 1\n[api.pipe]").unwrap_err();
        assert!(matches!(err, Error::UnknownKey(_)), "{err}");
    }

    #[test]
    fn load_reads_and_validates_a_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "version = 1").unwrap();
        let config = Config::load(&path).unwrap();
        assert_eq!(config.server.port, 9000);
    }

    #[test]
    fn secret_key_conversions() {
        let from_str = auth::SecretKey::from("sk");
        let from_string = auth::SecretKey::from("sk".to_string());
        assert_eq!(&*from_str, "sk");
        assert_eq!(&*from_string, "sk");
        let boxed: secrecy::SecretBox<auth::SecretKey> = from_str.into();
        assert_eq!(&**boxed.expose_secret(), "sk");
    }

    #[test]
    fn access_log_format_as_str() {
        assert!(
            log::AccessFormat::Combined
                .as_str()
                .contains("$remote_addr")
        );
        assert!(
            log::AccessFormat::Common
                .as_str()
                .contains("$body_bytes_sent")
        );
        assert_eq!(
            log::AccessFormat::Custom("$request".into()).as_str(),
            "$request"
        );
    }
}
