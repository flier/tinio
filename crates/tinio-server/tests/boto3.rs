//! boto3 basic-journey scenario (task T034) — best-effort client per
//! FR-025 (targeted/manual, NOT CI-gated): the SC-001 basic scenario set
//! via the boto3 SDK (create bucket, upload, byte-identical download,
//! prefix/delimiter listing, zero-byte round-trip, multipart via
//! `upload_file`, delete). boto3 is inherently a Python client — the
//! scenario drives the venv python against `tests/boto3_journey.py`.
//!
//! Run: `cargo test -p tinio-server --test boto3 -- --ignored`
//! (provision the venv first — see e2e/interop/TROUBLESHOOTING.md §2)

mod e2e;

use std::path::Path;

use e2e::Server;
use predicates::prelude::predicate::str::contains;

#[test]
#[ignore = "requires the tinio-e2e venv with boto3 (see TROUBLESHOOTING.md §2)"]
fn journey() {
    let python = e2e::boto3_python();
    assert!(
        python.exists(),
        "boto3 venv python not found at {} — create it and install boto3:\n\
         python3 -m venv <target>/tinio-e2e-venv && <venv>/pip install boto3\n\
         (or point TINIO_BOTO3_PYTHON at your own venv python; \
         see e2e/interop/TROUBLESHOOTING.md §2)",
        python.display()
    );
    let server = Server::start();
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/boto3_journey.py");
    e2e::boto3(server.endpoint(), &script)
        .assert()
        .success()
        .stdout(contains("BOTO3 JOURNEY OK"));
}

#[test]
#[ignore = "requires the tinio-e2e venv with boto3 (see TROUBLESHOOTING.md §2)"]
fn list_buckets_pagination() {
    let python = e2e::boto3_python();
    assert!(
        python.exists(),
        "boto3 venv python not found at {} — create it and install boto3:\n\
         python3 -m venv <target>/tinio-e2e-venv && <venv>/pip install boto3\n\
         (or point TINIO_BOTO3_PYTHON at your own venv python; \
         see e2e/interop/TROUBLESHOOTING.md §2)",
        python.display()
    );
    // The `[s3] max_buckets = 3` cap forces a page size below the
    // account's bucket count; the script asserts at least two pages
    // occur, so a dropped or ignored cap (default max_buckets = 10000 —
    // everything in one page) fails the test.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    let config = dir.path().join("config.toml");
    std::fs::write(&config, "version = 1\n\n[s3]\nmax_buckets = 3\n").unwrap();
    let server = e2e::Server::start_with_config(&root, &config);
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/boto3_buckets_pagination.py");
    e2e::boto3(server.endpoint(), &script)
        .assert()
        .success()
        .stdout(contains("BUCKET PAGINATION OK"));
}
