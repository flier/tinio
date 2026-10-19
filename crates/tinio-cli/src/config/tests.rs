//! The composition root's tests: document-level wiring, unknown-key rejection,
//! round-trips and cross-section validation. Section rules live with their types.

use std::{fs, path::PathBuf, time::Duration};

use secrecy::ExposeSecret;

use super::{Config, Error, Version, credentials, s3, storage};
use crate::{
    _core::{
        pipeline::DEFAULT_CAPACITY,
        storage::{DEFAULT_META_BATCH_BYTES, DEFAULT_META_BATCH_SIZE},
    },
    _server::{
        identity::auth::SecretKey,
        log::{AccessFormat, Verbosity},
        pipeline::{Db, Io, Priority, Remove},
    },
};

/// A config file on disk, for the assertions that go through `Config::load`'s fs path
/// rather than the in-memory `Config::parse`.
fn write(dir: &tempfile::TempDir, name: &str, text: &str) -> PathBuf {
    let path = dir.path().join(name);
    fs::write(&path, text).unwrap();
    path
}

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

    [lifecycle]
    delay = "5s"
    cycle = "2h"

    [[lifecycle.rules]]
    bucket = "data"
    id = "expire-logs"
    prefix = "logs/"
    expire_days = 30
"#;
    let config = Config::parse(text).unwrap();
    assert_eq!(config.server.host, "0.0.0.0");
    assert_eq!(config.server.port, 9001);
    assert_eq!(config.scanner.as_ref().unwrap().delay, 2.5);
    assert_eq!(config.auth.as_ref().unwrap().access_key, "ak");
    assert_eq!(config.log.as_ref().unwrap().verbosity, Verbosity::Debug);
    assert_eq!(
        config.log.as_ref().unwrap().access_log_format,
        AccessFormat::Common
    );
    assert!(!config.s3.as_ref().unwrap().capabilities.multipart);
    assert!(config.s3.as_ref().unwrap().capabilities.list_objects_v1);
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
    assert_eq!(pipeline.io.priority, Priority::Low);
    assert_eq!(pipeline.io.capacity, 2048);
    assert_eq!(pipeline.db.workers, 2);
    assert_eq!(pipeline.db.priority, Priority::High);
    assert_eq!(pipeline.db.capacity, 4096);
    assert_eq!(
        config.api.as_ref().unwrap().http.as_ref().unwrap().port,
        9002
    );
    assert_eq!(
        config.telemetry.as_ref().unwrap().otlp_endpoint,
        "http://127.0.0.1:4317"
    );
    let lifecycle = config.lifecycle.as_ref().unwrap();
    assert_eq!(lifecycle.delay, Duration::from_secs(5));
    assert_eq!(lifecycle.cycle, Duration::from_secs(2 * 3600));
    assert_eq!(lifecycle.rules.len(), 1);
    assert_eq!(lifecycle.rules[0].id, "expire-logs");

    // Round-trip through TOML preserves everything.
    let again = Config::parse(&config.to_toml().unwrap()).unwrap();
    assert_eq!(config, again);

    // The same text through the fs path: `Config::load` reads the file and parses it into
    // the same value the in-memory parse did.
    let dir = tempfile::tempdir().unwrap();
    let path = write(&dir, "config.toml", text);
    assert_eq!(Config::load(&path).unwrap(), config);
}

/// **One document carrying every owner's keys at once**: the `[s3]` capability half
/// (flattened), its sweep half, `[scanner]` and `[telemetry]` — the shape a valid document
/// after the split must still parse, and the one an unknown-key false positive would redden.
#[test]
fn one_document_carries_capability_sweep_scanner_and_telemetry_keys() {
    let text = r#"
    version = 1

    [scanner]
    delay = 1.5
    max_wait = "3s"
    cycle = "6h"

    [s3]
    multipart = false
    copy_object = false
    list_objects_v1 = false
    list_objects_v2 = true
    delete_objects = true
    max_buckets = 7
    max_keys = 9
    allow_zero_page_size = true
    checksum = true
    tagging = false
    cors = false
    select = false
    acl = false
    versioning = false
    lifecycle = false
    select_concurrency = 2
    select_stack_bytes = 33554432
    sig_v2 = true
    temp_ttl_hours = 2
    multipart_expire_days = 4
    noncurrent_keep = 3
    noncurrent_expire_days = 30
    block_grace_minutes = 45
    block_bytes_warn = 1024
    max_concurrent_uploads = 5

    [telemetry]
    otlp_endpoint = "http://127.0.0.1:4317"

    [lifecycle]
    delay = "5s"
    cycle = "2h"

    [[lifecycle.rules]]
    bucket = "data"
    id = "expire-logs"
    prefix = "logs/"
    expire_days = 30
"#;
    let config = Config::parse(text).unwrap();

    let s3 = config.s3.as_ref().unwrap();
    assert!(!s3.capabilities.multipart);
    assert!(!s3.capabilities.copy_object);
    assert!(!s3.capabilities.list_objects_v1);
    assert!(s3.capabilities.list_objects_v2);
    assert!(s3.capabilities.delete_objects);
    assert_eq!(s3.capabilities.max_buckets, 7);
    assert_eq!(s3.capabilities.max_keys, 9);
    assert!(s3.capabilities.allow_zero_page_size);
    assert!(s3.capabilities.checksum);
    assert!(!s3.capabilities.tagging);
    assert!(!s3.capabilities.cors);
    assert!(!s3.capabilities.select);
    assert!(!s3.capabilities.acl);
    assert!(!s3.capabilities.versioning);
    assert!(
        !s3.capabilities.lifecycle,
        "the lifecycle capability's key is `[s3] lifecycle` — the one switch for the three \
         operations, the pass and the seed"
    );
    assert_eq!(s3.capabilities.select_concurrency, 2);
    assert_eq!(s3.capabilities.select_stack_bytes, 33_554_432);
    assert!(s3.sig_v2);
    assert_eq!(s3.temp_ttl_hours, 2);
    assert_eq!(s3.multipart_expire_days, Some(4));
    assert_eq!(s3.noncurrent_keep, 3);
    assert_eq!(s3.noncurrent_expire_days, 30);
    assert_eq!(s3.block_grace_minutes, 45);
    assert_eq!(s3.block_bytes_warn, 1024);
    assert_eq!(s3.max_concurrent_uploads, 5);

    let scanner = config.scanner.as_ref().unwrap();
    assert_eq!(scanner.delay, 1.5);
    assert_eq!(scanner.max_wait, Duration::from_secs(3));
    assert_eq!(scanner.cycle, Duration::from_secs(6 * 3600));
    assert_eq!(
        config.telemetry.as_ref().unwrap().otlp_endpoint,
        "http://127.0.0.1:4317"
    );

    // The whole document survives parse -> to_toml -> parse.
    let again = Config::parse(&config.to_toml().unwrap()).unwrap();
    assert_eq!(config, again);
}

/// **`[storage] type` is the TOML key, `backend` is the field**, and the presence-gated
/// `[storage.mem]`/`[storage.pg]` sub-sections serialize only when present.
#[test]
fn the_storage_type_rename_and_mem_pg_skip_behaviour() {
    let text = "version = 1\n[storage]\ntype = \"pg\"\n[storage.mem]\nmax_object_bytes = 1\n[storage.pg]\ndsn = \"postgres://db/tinio\"\npool = 4";
    let config = Config::parse(text).unwrap();
    let storage = config.storage.as_ref().unwrap();
    assert_eq!(storage.backend, storage::Backend::Pg);
    assert_eq!(storage.mem.as_ref().unwrap().max_object_bytes, Some(1));
    assert_eq!(storage.pg.as_ref().unwrap().pool, 4);
    let rendered = config.to_toml().unwrap();
    assert!(rendered.contains("type = \"pg\""), "{rendered}");
    assert!(rendered.contains("[storage.mem]"), "{rendered}");
    assert!(rendered.contains("[storage.pg]"), "{rendered}");

    // A section with neither sub-section writes neither: the skips are the shape's.
    let config = Config::parse("version = 1\n[storage]\ntype = \"fs\"").unwrap();
    let rendered = config.to_toml().unwrap();
    assert!(rendered.contains("type = \"fs\""), "{rendered}");
    assert!(!rendered.contains("[storage.mem]"), "{rendered}");
    assert!(!rendered.contains("[storage.pg]"), "{rendered}");
}

#[test]
fn unknown_keys_rejected() {
    let err = Config::parse("version = 1\n[server]\nunknown = 1").unwrap_err();
    assert!(matches!(err, Error::UnknownKey(_)), "{err}");
    let err = Config::parse("version = 1\n[bogus]\nx = 1").unwrap_err();
    assert!(matches!(err, Error::UnknownKey(_)), "{err}");
    // The same refusal on the fs path: `Config::load` reads the file and hands the text to
    // the parser above.
    let dir = tempfile::tempdir().unwrap();
    let path = write(&dir, "bad.toml", "version = 1\n[bogus]\nx = 1");
    let err = Config::load(&path).unwrap_err();
    assert!(matches!(err, Error::UnknownKey(_)), "{err}");
}

/// The `[s3]` table is one table across two owners (`#[serde(flatten)]`), which is exactly
/// the shape `serde_ignored` cannot see into: a typo beside valid capability and sweep keys
/// must still fail fast, not be silently ignored.
#[test]
fn unknown_key_in_the_s3_table_rejected() {
    let err = Config::parse(
        "version = 1\n[s3]\nmultipart = false\nsig_v2 = true\ntemp_ttl_hours = 2\nmultipartt = false",
    )
    .unwrap_err();
    assert!(matches!(err, Error::UnknownKey(_)), "{err}");
    assert!(err.to_string().contains("s3.multipartt"), "{err}");
}

/// `[api.https]` flattens its `Http` half; an unknown key beside the valid fields is the
/// same refusal, with the nested path spelling.
#[test]
fn unknown_key_in_the_api_https_table_rejected() {
    let err = Config::parse(
        "version = 1\n[api.https]\nhost = \"127.0.0.1\"\nport = 9001\ncert = \"c.pem\"\nkey = \"k.pem\"\nbogus = true",
    )
    .unwrap_err();
    assert!(matches!(err, Error::UnknownKey(_)), "{err}");
    assert!(err.to_string().contains("api.https.bogus"), "{err}");
}

/// Flattened tables do not change how a plain nested table reports: `[pipeline.io]` still
/// fails through `serde_ignored` itself, whose `?` marks the `Option` sections on the path.
#[test]
fn unknown_key_in_the_pipeline_table_rejected() {
    let err = Config::parse("version = 1\n[pipeline.io]\nworkers = 2\nbogus = true").unwrap_err();
    assert!(matches!(err, Error::UnknownKey(_)), "{err}");
    assert!(err.to_string().contains("pipeline.?.io.bogus"), "{err}");
}

#[test]
fn wrong_types_rejected() {
    // Boolean-typed keys must be booleans (serde rejects strings).
    let err = Config::parse("version = 1\n[server]\nread_only = \"yes\"").unwrap_err();
    assert!(matches!(err, Error::Parse { .. }), "{err}");
    // Port must be a number in range.
    let err = Config::parse("version = 1\n[server]\nport = 70000").unwrap_err();
    assert!(matches!(err, Error::Parse { .. }), "{err}");
    // The same refusal on the fs path.
    let dir = tempfile::tempdir().unwrap();
    let path = write(&dir, "bad.toml", "version = 1\n[server]\nport = 70000");
    let err = Config::load(&path).unwrap_err();
    assert!(matches!(err, Error::Parse { .. }), "{err}");
}

#[test]
fn version_must_be_one() {
    let err = Config::parse("version = 2").unwrap_err();
    assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
}

#[test]
fn partial_sections_keep_field_defaults() {
    // A present section that omits a key takes the section's `Default` (the
    // container-level `#[serde(default)]`), so `[s3] multipart = false` leaves
    // every other toggle on.
    let config = Config::parse("version = 1\n[server]").unwrap();
    assert_eq!(config.server.host, "127.0.0.1");
    assert_eq!(config.server.port, 9000);

    let config = Config::parse("version = 1\n[s3]\nmultipart = false").unwrap();
    let s3 = config.s3.as_ref().unwrap();
    assert!(!s3.capabilities.multipart);
    assert!(
        s3.capabilities.copy_object
            && s3.capabilities.list_objects_v1
            && s3.capabilities.list_objects_v2
            && s3.capabilities.delete_objects
    );
    assert_eq!(s3.temp_ttl_hours, 24);
    assert_eq!(s3.multipart_expire_days, None);
    assert_eq!(s3.multipart_expire_days(), 7);

    // An empty `[s3]` section falls back to every serde default
    // helper (the toggle defaults must match `Config::default`).
    let config = Config::parse("version = 1\n[s3]").unwrap();
    let empty = config.s3.as_ref().unwrap();
    assert_eq!(empty, &s3::Config::default());

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

    // The pipeline sections keep their per-lane defaults too.
    let config = Config::parse("version = 1\n[pipeline.io]\nworkers = 3").unwrap();
    let io = &config.pipeline.as_ref().unwrap().io;
    assert_eq!(io.workers, 3);
    assert_eq!(io.capacity, DEFAULT_CAPACITY);
    assert_eq!(io.priority, Priority::Normal);
    let config = Config::parse("version = 1\n[pipeline]").unwrap();
    let pipeline = config.pipeline.as_ref().unwrap();
    assert_eq!(pipeline.io, Io::default());
    assert_eq!(pipeline.remove, Remove::default());
    assert_eq!(pipeline.db, Db::default());
}

/// Probe 1 (non-flattened): a present section that omits a key takes the section's
/// `SmartDefault` value, not the field type's `Default`.
#[test]
fn an_empty_section_keeps_the_smart_default_values() {
    let config = Config::parse("version = 1\n[server]").unwrap();
    assert_eq!(config.server.host, "127.0.0.1");
    assert_eq!(config.server.port, 9000);
    assert!(!config.server.read_only && !config.server.sandbox);

    let config = Config::parse("version = 1\n[scanner]").unwrap();
    let scanner = config.scanner.as_ref().unwrap();
    assert_eq!(scanner.delay, 10.0);
    assert_eq!(scanner.max_wait, Duration::from_secs(15));

    let config = Config::parse("version = 1\n[api.http]").unwrap();
    let http = config.api.as_ref().unwrap().http.as_ref().unwrap();
    assert_eq!(http.host, "127.0.0.1");
    assert_eq!(http.port, 9001);

    let config = Config::parse("version = 1\n[storage.fs]").unwrap();
    let fs = &config.storage.as_ref().unwrap().fs;
    assert_eq!(fs.compact_threshold_percent, 20);
    assert_eq!(fs.meta_batch_size, DEFAULT_META_BATCH_SIZE);
    assert!(!fs.follow_symlinks);

    let config = Config::parse("version = 1\n[storage.pg]").unwrap();
    assert_eq!(config.pg_section().unwrap().pool, 8);

    // A key that is written still overrides the default.
    let config = Config::parse("version = 1\n[server]\nport = 9100").unwrap();
    assert_eq!(config.server.port, 9100);
    assert_eq!(config.server.host, "127.0.0.1");
}

/// Probe 2 (flattened): `[s3]` is one table across two owners, so the capability half is
/// reached through `#[serde(flatten)]` — an absent capability key must still take the
/// `SmartDefault` toggle (`true`), never the field type's `Default` (`false`).
#[test]
fn a_partial_s3_section_keeps_the_capability_defaults() {
    let config = Config::parse("version = 1\n[s3]\nsig_v2 = false").unwrap();
    let capabilities = &config.s3.as_ref().unwrap().capabilities;
    assert!(capabilities.multipart);
    assert_eq!(capabilities.max_buckets, 10_000);

    // A write toggle still overrides, and the absent ones keep their default.
    let config = Config::parse("version = 1\n[s3]\nmultipart = false\nmax_buckets = 5").unwrap();
    let capabilities = &config.s3.as_ref().unwrap().capabilities;
    assert!(!capabilities.multipart);
    assert_eq!(capabilities.max_buckets, 5);
    assert!(capabilities.copy_object && capabilities.versioning);
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
fn presence_gated_sections_serialize_away() {
    // The auto-generated first-start config omits every presence-gated section, so
    // `to_toml` never emits `[pipeline.*]` (Q8) or a section a consumer could not
    // round-trip.
    let config = Config::default();
    let toml = config.to_toml().unwrap();
    for section in [
        "scanner",
        "api",
        "telemetry",
        "pipeline",
        "auth",
        "log",
        "s3",
        "storage",
    ] {
        assert!(
            !toml.contains(section),
            "absent section {section:?} leaked into {toml}"
        );
    }
}

#[test]
fn generated_writes_the_first_start_shape() {
    let config = Config::generated(false, credentials::generate().unwrap()).unwrap();
    assert_eq!(config.server.port, 9000);
    assert!(!config.server.read_only);
    assert!(config.auth.is_some());
    assert!(config.scanner.is_some());
    assert!(config.storage.is_some());
    let api = config.api.as_ref().unwrap();
    #[cfg(unix)]
    assert!(api.unix.is_some());
    #[cfg(windows)]
    assert!(api.pipe.is_some());
    assert!(config.log.is_none());
    assert!(config.s3.is_none());
    assert!(config.pipeline.is_none());
    assert!(config.owner.is_none());
    assert!(config.users.is_none());
    assert!(config.telemetry.is_none());

    // A read-only run records its mode, and the document re-parses to the same value.
    let read_only = Config::generated(true, credentials::generate().unwrap()).unwrap();
    assert!(read_only.server.read_only);
    let again = Config::parse(&read_only.to_toml().unwrap()).unwrap();
    assert_eq!(read_only, again);
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
    fs::write(&path, "version = 1").unwrap();
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
fn owner_and_users_accessors_return_defaults_when_absent() {
    // Spec §6: the effective [owner] element is the core default when
    // the section is absent; users is empty without [[users]].
    let config = Config::parse("version = 1").unwrap();
    assert_eq!(
        config.owner().canonical_id,
        "d16b7e8c0bb9728d01e3bf9c30940a32622195f821325af577a87bd6284ac306"
    );
    assert_eq!(config.owner().display_name, "tinio");
    assert!(config.users().is_empty());
}

#[test]
fn identity_composes_the_three_identity_values() {
    let config = Config::parse(
        "version = 1\n[auth]\naccess_key = \"ak\"\nsecret_key = \"sk\"\n[[users]]\naccess_key = \"u\"\nsecret_key = \"s\"\n",
    )
    .unwrap();
    let identity = config.identity();
    assert_eq!(identity.auth.as_ref().unwrap().access_key, "ak");
    assert_eq!(identity.owner.display_name, "tinio");
    assert_eq!(identity.users.len(), 1);
    assert_eq!(identity.users[0].access_key, "u");
    // An absent `[owner]` is the core default, applied at composition.
    assert_eq!(
        Config::default().identity().owner.canonical_id,
        "d16b7e8c0bb9728d01e3bf9c30940a32622195f821325af577a87bd6284ac306"
    );
}

#[test]
fn owner_and_users_round_trip() {
    let config = Config::parse(
        "version = 1\n[owner]\ncanonical_id = \"ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100\"\ndisplay_name = \"ops\"\n[[users]]\naccess_key = \"AKID\"\nsecret_key = \"sk\"\n",
    )
    .unwrap();
    let again = Config::parse(&config.to_toml().unwrap()).unwrap();
    assert_eq!(config, again);
}

#[test]
fn owner_canonical_id_must_be_64_hex() {
    let err = Config::parse("version = 1\n[owner]\ncanonical_id = \"not-hex\"").unwrap_err();
    assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
}

#[test]
fn users_reject_duplicate_access_keys_and_ids() {
    // Spec §6: unique access keys and unique canonical IDs, the root
    // [auth]/[owner] elements included — a collision makes one
    // principal shadow (access key) or be (canonical ID) the other.
    let err = Config::parse(
        "version = 1\n[[users]]\naccess_key = \"ak\"\nsecret_key = \"sk\"\n[[users]]\naccess_key = \"ak\"\nsecret_key = \"sk2\"\n",
    )
    .unwrap_err();
    assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
    assert!(err.to_string().contains("duplicate access key"), "{err}");

    // A user access key colliding with the root [auth] credential.
    let err = Config::parse(
        "version = 1\n[auth]\naccess_key = \"root\"\nsecret_key = \"sk-root\"\n[[users]]\naccess_key = \"root\"\nsecret_key = \"sk\"\n",
    )
    .unwrap_err();
    assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
    assert!(err.to_string().contains("duplicate access key"), "{err}");

    // A user canonical ID colliding with another user's derived ID.
    let err = Config::parse(
        "version = 1\n[[users]]\naccess_key = \"AKID\"\nsecret_key = \"sk\"\n[[users]]\naccess_key = \"OTHER\"\nsecret_key = \"sk2\"\ncanonical_id = \"2c8a2a08ad81dddf7e7830cbc75310f731a1381431bb29afb55a76ee07e81721\"\n",
    )
    .unwrap_err();
    assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
    assert!(err.to_string().contains("duplicate canonical ID"), "{err}");

    // A user canonical ID colliding with the root [owner] element.
    let err = Config::parse(
        "version = 1\n[[users]]\naccess_key = \"u\"\nsecret_key = \"sk\"\ncanonical_id = \"d16b7e8c0bb9728d01e3bf9c30940a32622195f821325af577a87bd6284ac306\"\n",
    )
    .unwrap_err();
    assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
    assert!(err.to_string().contains("duplicate canonical ID"), "{err}");
}

#[test]
fn users_reject_the_default_minioadmin_fallback_when_auth_is_absent() {
    // The root credential falls back to the US1 pair when [auth] is
    // absent — a user key equal to the fallback access key would
    // shadow the root in the access-key map (same rule as the [auth]
    // collision, spec §6).
    let err =
        Config::parse("version = 1\n[[users]]\naccess_key = \"minioadmin\"\nsecret_key = \"sk\"\n")
            .unwrap_err();
    assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
    assert!(err.to_string().contains("duplicate access key"), "{err}");
}

#[test]
fn max_concurrent_uploads_defaults_to_1000() {
    assert_eq!(s3::Config::default().max_concurrent_uploads, 1000);
    let config = Config::parse("version = 1\n[s3]").unwrap();
    assert_eq!(config.s3.as_ref().unwrap().max_concurrent_uploads, 1000);
}

#[test]
fn max_concurrent_uploads_parses_when_set() {
    let config = Config::parse("version = 1\n[s3]\nmax_concurrent_uploads = 5").unwrap();
    assert_eq!(config.s3.as_ref().unwrap().max_concurrent_uploads, 5);
}

#[test]
fn max_concurrent_uploads_rejects_zero() {
    let err = Config::parse("version = 1\n[s3]\nmax_concurrent_uploads = 0").unwrap_err();
    assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
}

#[test]
fn capabilities_flatten_into_s3_section() {
    let config =
        Config::parse("version = 1\n[s3]\nmultipart = false\ncopy_object = false").unwrap();
    let caps = config.s3.as_ref().unwrap().capabilities;
    assert!(!caps.multipart);
    assert!(!caps.copy_object);
    assert!(caps.delete_objects);
    assert!(caps.list_objects_v1);
    assert!(caps.list_objects_v2);
}

#[test]
fn versioning_defaults_on_and_the_retention_knobs_default_to_keep_everything() {
    let config = Config::parse("version = 1\n[s3]").unwrap();
    let s3 = config.s3.clone().unwrap();
    assert!(
        s3.capabilities.versioning,
        "a core-surface capability defaults on, like tagging"
    );
    assert_eq!(s3.noncurrent_keep, 0, "0 = keep every noncurrent version");
    assert_eq!(s3.noncurrent_expire_days, 0, "0 = never expire by age");
    assert_eq!(
        s3.block_grace_minutes, 10,
        "the block sweep's grace defaults to ten minutes"
    );
    assert_eq!(
        s3.block_bytes_warn, 0,
        "0 = never warn about the block store's size"
    );

    let off = Config::parse("version = 1\n[s3]\nversioning = false\nnoncurrent_keep = 3\nnoncurrent_expire_days = 30\nblock_grace_minutes = 45\nblock_bytes_warn = 1073741824").unwrap();
    let s3 = off.s3.unwrap();
    assert!(!s3.capabilities.versioning);
    assert_eq!(s3.noncurrent_keep, 3);
    assert_eq!(s3.noncurrent_expire_days, 30);
    assert_eq!(s3.block_grace_minutes, 45);
    assert_eq!(s3.block_bytes_warn, 1073741824);
}

#[test]
fn a_zero_block_grace_is_refused() {
    let err = Config::parse("version = 1\n[s3]\nblock_grace_minutes = 0").unwrap_err();
    assert!(matches!(err, Error::InvalidValue { .. }), "got {err:?}");
}

#[test]
fn a_zero_sweep_ttl_is_refused() {
    // The retention neighbours use 0 for "unlimited"; the two TTLs do not: a zero arms
    // the sweeper maximally (a staged write temp removed mid-PUT, every upload aborted),
    // so both are floored at 1 exactly like `block_grace_minutes`.
    let err = Config::parse("version = 1\n[s3]\ntemp_ttl_hours = 0").unwrap_err();
    assert!(matches!(err, Error::InvalidValue { .. }), "got {err:?}");
    let err = Config::parse("version = 1\n[s3]\nmultipart_expire_days = 0").unwrap_err();
    assert!(matches!(err, Error::InvalidValue { .. }), "got {err:?}");
    // The floor itself parses.
    let config =
        Config::parse("version = 1\n[s3]\ntemp_ttl_hours = 1\nmultipart_expire_days = 1").unwrap();
    let s3 = config.s3.unwrap();
    assert_eq!(s3.temp_ttl_hours, 1);
    assert_eq!(s3.multipart_expire_days, Some(1));
}

#[test]
fn a_zero_scanner_cadence_is_refused() {
    // `cycle` and `max_wait` pace the scanner, so a zero would spin it instead of pacing it:
    // both carry the 1s floor (`config::nonzero_duration`), unlike `delay`'s own zero.
    for key in ["max_wait", "cycle"] {
        let text = format!("version = 1\n[scanner]\n{key} = \"0s\"");
        let err = Config::parse(&text).unwrap_err();
        assert!(matches!(err, Error::InvalidValue { .. }), "{text}: {err}");
        assert!(err.to_string().contains(key), "{text}: {err}");
    }
    // The floor itself parses.
    let config =
        Config::parse("version = 1\n[scanner]\nmax_wait = \"1s\"\ncycle = \"1s\"").unwrap();
    let scanner = config.scanner.as_ref().unwrap();
    assert_eq!(scanner.max_wait, Duration::from_secs(1));
    assert_eq!(scanner.cycle, Duration::from_secs(1));
}

#[test]
fn meta_batch_size_range_validated() {
    // 1..=4096; outside → startup error (pipeline-spec.md §3.3).
    for bad in [0u16, 4097] {
        let text = format!("version = 1\n[storage.fs]\nmeta_batch_size = {bad}");
        let err = Config::parse(&text).unwrap_err();
        assert!(matches!(err, Error::InvalidValue { .. }), "{text}: {err}");
    }
    for good in [1u16, 128, 4096] {
        let text = format!("version = 1\n[storage.fs]\nmeta_batch_size = {good}");
        let config = Config::parse(&text).unwrap();
        assert_eq!(config.storage.as_ref().unwrap().fs.meta_batch_size, good);
    }
}

#[test]
fn meta_batch_bytes_range_validated() {
    // 1024..=16 MiB; outside → startup error.
    for bad in [1023u32, 16 * 1024 * 1024 + 1] {
        let text = format!("version = 1\n[storage.fs]\nmeta_batch_bytes = {bad}");
        let err = Config::parse(&text).unwrap_err();
        assert!(matches!(err, Error::InvalidValue { .. }), "{text}: {err}");
    }
    for good in [1024u32, 262144, 16 * 1024 * 1024] {
        let text = format!("version = 1\n[storage.fs]\nmeta_batch_bytes = {good}");
        let config = Config::parse(&text).unwrap();
        assert_eq!(config.storage.as_ref().unwrap().fs.meta_batch_bytes, good);
    }
}

#[test]
fn absent_keys_keep_the_defaults() {
    // Presence-gated (Q8): a section that omits the keys deserializes
    // the defaults — never the field-type default (0 would fail garde).
    let config = Config::parse("version = 1\n[storage.fs]\nfollow_symlinks = false").unwrap();
    let fs = &config.storage.as_ref().unwrap().fs;
    assert_eq!(fs.meta_batch_size, DEFAULT_META_BATCH_SIZE);
    assert_eq!(fs.meta_batch_bytes, DEFAULT_META_BATCH_BYTES);
}

#[test]
fn mem_limits_default_to_unlimited() {
    // `[storage.mem]` is absent by default, and the limits are
    // optional — the documented no-limit posture (CHK028).
    let config = Config::parse("version = 1").unwrap();
    assert!(config.storage.is_none());

    let config = Config::parse("version = 1\n[storage.mem]").unwrap();
    let mem = config.storage.as_ref().unwrap().mem.as_ref().unwrap();
    assert_eq!(mem.max_object_bytes, None);
    assert_eq!(mem.max_total_bytes, None);
}

#[test]
fn mem_limits_parse_when_set() {
    let config = Config::parse(
        "version = 1\n[storage.mem]\nmax_object_bytes = 1048576\nmax_total_bytes = 1073741824",
    )
    .unwrap();
    let mem = config.storage.as_ref().unwrap().mem.as_ref().unwrap();
    assert_eq!(mem.max_object_bytes, Some(1048576));
    assert_eq!(mem.max_total_bytes, Some(1073741824));
}

#[test]
fn mem_limits_reject_zero() {
    // A byte limit must be positive when present (absent = unlimited).
    for bad in [
        "version = 1\n[storage.mem]\nmax_object_bytes = 0",
        "version = 1\n[storage.mem]\nmax_total_bytes = 0",
    ] {
        let err = Config::parse(bad).unwrap_err();
        assert!(matches!(err, Error::InvalidValue { .. }), "{bad}: {err}");
    }
}

#[test]
fn the_type_key_defaults_to_fs_and_selects_pg() {
    // No key (and no section at all): the filesystem backend, so a configuration written
    // before the selector existed keeps its meaning.
    let config = Config::parse("version = 1").unwrap();
    assert!(config.storage.is_none());
    assert_eq!(storage::Config::default().backend, storage::Backend::Fs);

    let config = Config::parse("version = 1\n[storage]\ntype = \"pg\"").unwrap();
    let storage = config.storage.as_ref().unwrap();
    assert_eq!(storage.backend, storage::Backend::Pg);
    // The selector and the section are independent keys: `type = "pg"` with no
    // `[storage.pg]` parses, because the dsn's other source is `TINIO_PG_DSN` (PG5) and the
    // refusal for having neither belongs where the environment is readable.
    assert!(storage.pg.is_none());
    assert_eq!(storage.fs, storage::Fs::default());
}

#[test]
fn an_unknown_type_names_the_alternatives() {
    // A startup error naming the remedy: the value the key accepts is what the message has
    // to carry, because the config file is the only place the key can be corrected.
    let err = Config::parse("version = 1\n[storage]\ntype = \"s3\"").unwrap_err();
    assert!(matches!(err, Error::Parse { .. }), "{err}");
    let rendered = err.to_string();
    assert!(rendered.contains("fs"), "{rendered}");
    assert!(rendered.contains("pg"), "{rendered}");
}

#[test]
fn a_pg_section_round_trips_beside_the_type_key() {
    // The whole `[storage]` section in one text: the selector, the filesystem keys and the
    // pg keys together, through parse -> to_toml -> parse.
    let text = "version = 1\n[storage]\ntype = \"pg\"\n[storage.fs]\nfollow_symlinks = true\n[storage.pg]\ndsn = \"postgres://user:password@db.example:5432/tinio\"\npool = 4";
    let config = Config::parse(text).unwrap();
    let again = Config::parse(&config.to_toml().unwrap()).unwrap();
    assert_eq!(config, again);
    let storage = config.storage.as_ref().unwrap();
    assert_eq!(storage.backend, storage::Backend::Pg);
    assert!(storage.fs.follow_symlinks);
    assert_eq!(storage.pg.as_ref().unwrap().pool, 4);
}

#[test]
fn the_section_parses_and_the_pool_defaults_to_eight() {
    let config = Config::parse(
        "version = 1\n[storage]\ntype = \"pg\"\n[storage.pg]\ndsn = \"postgres://user:hunter2@db.example:5432/tinio\"",
    )
    .unwrap();
    let pg = config.storage.as_ref().unwrap().pg.as_ref().unwrap();
    assert_eq!(pg.pool, 8);
    assert_eq!(
        pg.dsn.as_ref().expect("dsn").expose_secret().as_ref(),
        "postgres://user:hunter2@db.example:5432/tinio"
    );
}

#[test]
fn the_dsn_is_absent_by_default_and_the_pool_parses_when_written() {
    // The env-only shape (TINIO_PG_DSN, PG5): a section with no dsn parses.
    let config =
        Config::parse("version = 1\n[storage]\ntype = \"pg\"\n[storage.pg]\npool = 32").unwrap();
    let pg = config.storage.as_ref().unwrap().pg.as_ref().unwrap();
    assert!(pg.dsn.is_none());
    assert_eq!(pg.pool, 32);
}

#[test]
fn an_empty_dsn_parses_and_is_the_resolvers_to_read() {
    // `dsn = ""` names nothing, and the one reading that turns it into the unset key lives with
    // the resolver (`resolve::pg::resolve_dsn`), which reads the variable the same way.
    let config =
        Config::parse("version = 1\n[storage]\ntype = \"pg\"\n[storage.pg]\ndsn = \"\"").unwrap();
    let pg = config.storage.as_ref().unwrap().pg.as_ref().unwrap();
    assert_eq!(
        pg.dsn
            .as_ref()
            .expect("the key parses")
            .expose_secret()
            .as_ref(),
        ""
    );
}

#[test]
fn the_pool_must_be_positive() {
    let err = Config::parse("version = 1\n[storage.pg]\npool = 0").unwrap_err();
    assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
}

#[test]
fn a_rendering_of_the_section_masks_the_password() {
    // PG5: the dsn is a `Secret`; a section that reached a log through its `Debug` must not
    // carry the password.
    let config = Config::parse(
        "version = 1\n[storage]\ntype = \"pg\"\n[storage.pg]\ndsn = \"postgres://user:hunter2@db.example:5432/tinio\"",
    )
    .unwrap();
    let pg = config.storage.as_ref().unwrap().pg.as_ref().unwrap();
    let rendered = format!("{pg:?}");
    assert!(!rendered.contains("hunter2"), "{rendered}");
    assert!(rendered.contains("REDACTED"), "{rendered}");
}

#[test]
fn the_pg_section_round_trips_through_toml() {
    // The keys are the contract's shape in both directions: what the parser reads is what
    // the serializer writes (`dsn` included, and `pool` always).
    let text = "version = 1\n\n[storage]\ntype = \"pg\"\n\n[storage.pg]\ndsn = \"postgres://user:hunter2@db.example:5432/tinio\"\npool = 4";
    let config = Config::parse(text).expect("parses");
    let again = Config::parse(&config.to_toml().expect("serializes")).expect("re-parses");
    assert_eq!(config, again);
    let toml = config.to_toml().expect("serializes");
    assert!(toml.contains("type = \"pg\""), "{toml}");
    assert!(toml.contains("pool = 4"), "{toml}");
}

#[test]
fn the_pg_keys_parse_in_every_build() {
    // §6: the keys are not feature-gated -- no `[storage]` key is, so a configuration
    // written for a pg deployment parses and validates wherever the schema is linked.
    let config = Config::parse(
        "version = 1\n[storage]\ntype = \"pg\"\n[storage.pg]\ndsn = \"postgres://user:hunter2@db.example:5432/tinio\"\npool = 2",
    )
    .expect("the pg keys parse with no pg backend in the build");
    assert_eq!(
        config.storage.as_ref().unwrap().backend,
        storage::Backend::Pg
    );
}

#[test]
fn absent_pipeline_section_resolves_to_none() {
    // Q8: presence-gated — no `[pipeline]` section, no `pipeline` key.
    let config = Config::parse("version = 1").unwrap();
    assert!(config.pipeline.is_none());
}
