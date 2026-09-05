//! External-consumer integration tests for the config crate: the file
//! path (`Config::load` -> `Config::to_toml`) as a real consumer drives
//! it — not the unit tests' in-memory `parse`. This is the crate's
//! integration surface with the filesystem and the TOML format.

use std::fs;

use tinio_config::{Config, Error};

fn write(dir: &tempfile::TempDir, name: &str, text: &str) -> std::path::PathBuf {
    let path = dir.path().join(name);
    fs::write(&path, text).unwrap();
    path
}

#[test]
fn loads_a_full_config_file_and_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let path = write(
        &dir,
        "config.toml",
        r#"
        version = 1

        [server]
        host = "0.0.0.0"
        port = 9001

        [auth]
        access_key = "ak"
        secret_key = "sk"

        [s3]
        multipart = false
        list_objects_v1 = true

        [storage.fs]
        follow_symlinks = false
        compact_threshold_percent = 50

        [scanner]
        delay = 2.5
        "#,
    );

    let config = Config::load(&path).unwrap();
    assert_eq!(config.server.host, "0.0.0.0");
    assert_eq!(config.server.port, 9001);
    assert_eq!(config.auth.as_ref().unwrap().access_key, "ak");
    assert!(!config.s3.as_ref().unwrap().capabilities.multipart);
    assert!(config.s3.as_ref().unwrap().capabilities.list_objects_v1);
    assert_eq!(
        config.storage.as_ref().unwrap().fs.compact_threshold_percent,
        50
    );
    assert_eq!(config.scanner.as_ref().unwrap().delay, 2.5);

    // Serialize and re-parse: the external round-trip is lossless.
    let again = Config::parse(&config.to_toml().unwrap()).unwrap();
    assert_eq!(config, again);
}

#[test]
fn load_rejects_unknown_keys_and_missing_files() {
    let dir = tempfile::tempdir().unwrap();
    let path = write(&dir, "bad.toml", "version = 1\n[bogus]\nx = 1");
    assert!(matches!(Config::load(&path), Err(Error::UnknownKey(_))));

    let path = write(&dir, "bad2.toml", "version = 1\n[server]\nport = 70000");
    assert!(matches!(Config::load(&path), Err(Error::Parse { .. })));

    assert!(matches!(
        Config::load(&dir.path().join("missing.toml")),
        Err(Error::Io { .. })
    ));
}

#[test]
fn all_optional_sections_serialize_away_when_absent() {
    // The auto-generated first-start config omits every presence-gated
    // section, so `to_toml` never emits `[pipeline.*]` or a section a
    // consumer could not round-trip.
    let toml = Config::default().to_toml().unwrap();
    for section in ["scanner", "api", "telemetry", "pipeline", "auth", "log", "s3", "storage"] {
        assert!(
            !toml.contains(section),
            "absent section {section:?} leaked into {toml}"
        );
    }
}
