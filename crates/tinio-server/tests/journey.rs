//! Interop core journey (task T032) — aws cli v2 + rclone against
//! `127.0.0.1` with no client-side addressing overrides (SC-002): create
//! bucket, upload, byte-identical download, prefix/delimiter listing,
//! delete object, delete bucket, plus an ephemeral `--port 0` run.
//! CI-gated (FR-025).
//!
//! Run: `cargo test -p tinio-server --test journey -- --ignored`

mod e2e;

use std::fs;

use e2e::{Rclone, Server};
use predicates::prelude::*;

#[test]
#[ignore = "requires aws cli v2 and rclone on PATH"]
fn journey() {
    let server = Server::start();
    let ep = server.endpoint();
    let scratch = tempfile::tempdir().unwrap();

    // --- aws cli v2 journey ---------------------------------------------
    e2e::aws_s3(ep, "mb", &["s3://interop-bucket"])
        .assert()
        .success();
    let hello = scratch.path().join("hello.txt");
    fs::write(&hello, "hello from aws").unwrap();
    e2e::aws_s3(
        ep,
        "cp",
        &[hello.to_str().unwrap(), "s3://interop-bucket/hello.txt"],
    )
    .assert()
    .success();
    let down = scratch.path().join("downloaded.txt");
    e2e::aws_s3(
        ep,
        "cp",
        &["s3://interop-bucket/hello.txt", down.to_str().unwrap()],
    )
    .assert()
    .success();
    e2e::files_equal(&hello, &down);

    e2e::aws_s3(
        ep,
        "cp",
        &[
            hello.to_str().unwrap(),
            "s3://interop-bucket/dir/nested.txt",
        ],
    )
    .assert()
    .success();
    e2e::aws_s3(ep, "ls", &["s3://interop-bucket/"])
        .assert()
        .success()
        .stdout(predicate::str::contains("hello.txt"));
    e2e::aws_s3(ep, "ls", &["s3://interop-bucket/dir/"])
        .assert()
        .success()
        .stdout(predicate::str::contains("nested.txt"));
    e2e::aws_s3(ep, "rm", &["s3://interop-bucket/hello.txt"])
        .assert()
        .success();
    e2e::aws_s3(ep, "rb", &["s3://interop-bucket", "--force"])
        .assert()
        .success();

    // --- rclone journey --------------------------------------------------
    let rclone = Rclone::new(scratch.path().join("rclone.conf"));
    rclone.remote(ep).assert().success();
    rclone
        .cmd(&["mkdir", "tinio:rclone-bucket"])
        .assert()
        .success();
    let r = scratch.path().join("r.txt");
    fs::write(&r, "hello from rclone").unwrap();
    rclone
        .cmd(&["copy", r.to_str().unwrap(), "tinio:rclone-bucket/"])
        .assert()
        .success();
    let rdown = scratch.path().join("rclone-dl");
    fs::create_dir_all(&rdown).unwrap();
    rclone
        .cmd(&["copy", "tinio:rclone-bucket/r.txt", rdown.to_str().unwrap()])
        .assert()
        .success();
    e2e::files_equal(&r, &rdown.join("r.txt"));
    rclone
        .cmd(&["lsf", "tinio:rclone-bucket"])
        .assert()
        .success()
        .stdout(predicate::str::contains("r.txt"));
    rclone
        .cmd(&["delete", "tinio:rclone-bucket/r.txt"])
        .assert()
        .success();
    rclone
        .cmd(&["purge", "tinio:rclone-bucket"])
        .assert()
        .success();

    // --- ephemeral `--port 0` run ---------------------------------------
    // A second server starts while the first keeps serving (the bucket ops
    // below target the first server, as in the bash scenario).
    let second = Server::start();
    assert!(!second.endpoint().is_empty());
    e2e::aws_s3(ep, "mb", &["s3://ephemeral-bucket"])
        .assert()
        .success();
    e2e::aws_s3(ep, "rb", &["s3://ephemeral-bucket"])
        .assert()
        .success();
}

#[test]
#[ignore = "requires aws cli v2 on PATH"]
fn journey_checksum_multipart() {
    // The multipart checksum feature end-to-end with a real client (spec
    // 2026-08-31): `[s3] checksum = true` + aws-cli's default
    // CRC64NVME — create-algorithm upload, per-part trailer/header
    // values, FULL_OBJECT linearization at complete, all validated
    // server-side.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    fs::create_dir_all(&root).unwrap();
    let config = dir.path().join("config.toml");
    fs::write(&config, "version = 1\n\n[s3]\nchecksum = true\n").unwrap();
    let server = Server::start_with_config(&root, &config);
    let ep = server.endpoint();
    let scratch = tempfile::tempdir().unwrap();

    e2e::aws_s3(ep, "mb", &["s3://checksum-bucket"])
        .assert()
        .success();
    // > 8 MiB forces multipart; CRC64NVME is aws-cli's default checksum.
    let big = scratch.path().join("big.bin");
    let content: Vec<u8> = (0..(10 * 1024 * 1024)).map(|i| (i % 251) as u8).collect();
    fs::write(&big, &content).unwrap();
    e2e::aws_s3(
        ep,
        "cp",
        &[
            big.to_str().unwrap(),
            "s3://checksum-bucket/big.bin",
            "--checksum-algorithm",
            "CRC64NVME",
        ],
    )
    .assert()
    .success();
    let down = scratch.path().join("down.bin");
    e2e::aws_s3(
        ep,
        "cp",
        &["s3://checksum-bucket/big.bin", down.to_str().unwrap()],
    )
    .assert()
    .success();
    e2e::files_equal(&big, &down);
    e2e::aws_s3(ep, "rb", &["s3://checksum-bucket", "--force"])
        .assert()
        .success();
}
