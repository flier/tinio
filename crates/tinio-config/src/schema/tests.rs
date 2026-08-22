use super::*;

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

        [storage]
        follow_symlinks = false

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
    assert_eq!(config.log.as_ref().unwrap().verbosity, Verbosity::Debug);
    assert_eq!(
        config.log.as_ref().unwrap().access_log_format,
        AccessLogFormat::Common
    );
    assert!(!config.s3.as_ref().unwrap().multipart);
    assert!(!config.storage.as_ref().unwrap().follow_symlinks);
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
    let err = Config::parse("version = 1\n[api.https]\ncert = \"c.pem\"\nkey = \"\"").unwrap_err();
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
    let err =
        Config::parse("version = 1\n[log]\naccess_log_format = \"$authorization\"").unwrap_err();
    assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
    let err =
        Config::parse("version = 1\n[log]\naccess_log_format = \"$query_string\"").unwrap_err();
    assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
}

#[test]
fn access_log_format_serialization() {
    let log = LogConfig {
        access_log_format: AccessLogFormat::Combined,
        ..Default::default()
    };
    let toml = toml::to_string(&log).unwrap();
    assert!(toml.contains("access_log_format = \"combined\""), "{toml}");
    let custom = LogConfig {
        access_log_format: AccessLogFormat::Custom("$status".into()),
        ..Default::default()
    };
    let toml = toml::to_string(&custom).unwrap();
    assert!(toml.contains("access_log_format = \"$status\""), "{toml}");
    // And it round-trips back to Custom.
    let back: LogConfig = toml::from_str(&toml).unwrap();
    assert_eq!(
        back.access_log_format,
        AccessLogFormat::Custom("$status".into())
    );
}

#[test]
fn verbosity_display_and_parse() {
    assert_eq!(Verbosity::Info.to_string(), "info");
    assert_eq!(Verbosity::Debug.to_string(), "debug");
    assert_eq!("warn".parse::<Verbosity>().unwrap(), Verbosity::Warn);
    assert!("loud".parse::<Verbosity>().is_err());
    assert_eq!(LogFormat::Json.to_string(), "json");
    assert_eq!("text".parse::<LogFormat>().unwrap(), LogFormat::Text);
}

#[test]
fn presence_gated_sections_serialize_away() {
    let config = Config::default();
    let toml = config.to_toml().unwrap();
    assert!(!toml.contains("scanner"), "{toml}");
    assert!(!toml.contains("api"), "{toml}");
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
    let err = Config::parse("version = 1\n[telemetry]\notlp_endpoint = \"not-a-url\"").unwrap_err();
    assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
    Config::parse("version = 1\n[telemetry]\notlp_endpoint = \"http://127.0.0.1:4317\"").unwrap();
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
    let from_str = SecretKey::from("sk");
    let from_string = SecretKey::from("sk".to_string());
    assert_eq!(&*from_str, "sk");
    assert_eq!(&*from_string, "sk");
    let boxed: secrecy::SecretBox<SecretKey> = from_str.into();
    assert_eq!(&**boxed.expose_secret(), "sk");
}

#[test]
fn access_log_format_as_str() {
    assert!(AccessLogFormat::Combined.as_str().contains("$remote_addr"));
    assert!(
        AccessLogFormat::Common
            .as_str()
            .contains("$body_bytes_sent")
    );
    assert_eq!(
        AccessLogFormat::Custom("$request".into()).as_str(),
        "$request"
    );
}
