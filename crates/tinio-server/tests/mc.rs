//! mc (MinIO Client) basic-journey scenario (task T035) — best-effort
//! client per FR-025 (targeted/manual, NOT CI-gated): mb/cp/ls/rm/rb,
//! large-file copy (multipart), `mc stat` ETag check, zero-byte object.
//!
//! Run: `cargo test -p tinio-server --test mc -- --ignored`

mod e2e;

use std::fs;

use e2e::{Mc, Server};
use predicates::prelude::*;

#[test]
#[ignore = "requires the mc binary on PATH"]
fn journey() {
    let server = Server::start();
    let ep = server.endpoint();
    let scratch = tempfile::tempdir().unwrap();

    let mc = Mc::new(scratch.path().join("mc"));
    mc.alias(ep).assert().success();
    mc.cmd(&["mb", "tinio/mc-bucket"]).assert().success();

    let hello = scratch.path().join("hello.txt");
    fs::write(&hello, "hello from mc").unwrap();
    mc.cmd(&["cp", hello.to_str().unwrap(), "tinio/mc-bucket/hello.txt"])
        .assert()
        .success();
    let down = scratch.path().join("downloaded.txt");
    mc.cmd(&["cp", "tinio/mc-bucket/hello.txt", down.to_str().unwrap()])
        .assert()
        .success();
    e2e::files_equal(&hello, &down);

    // Zero-byte object.
    let zero = scratch.path().join("zero");
    fs::write(&zero, "").unwrap();
    mc.cmd(&["cp", zero.to_str().unwrap(), "tinio/mc-bucket/zero"])
        .assert()
        .success();
    mc.cmd(&["stat", "tinio/mc-bucket/zero"])
        .assert()
        .success()
        .stdout(predicate::str::contains("0 B"));

    // ETag via `mc stat` (header is `ETag` in newer mc releases, `etag`
    // in older ones).
    mc.cmd(&["stat", "tinio/mc-bucket/hello.txt"])
        .assert()
        .success()
        .stdout(predicate::str::contains("etag").or(predicate::str::contains("ETag")));

    // Large file (multipart).
    let big = scratch.path().join("big.bin");
    e2e::write_bytes(&big, 10 * 1024 * 1024);
    mc.cmd(&["cp", big.to_str().unwrap(), "tinio/mc-bucket/big.bin"])
        .assert()
        .success();
    let big_dl = scratch.path().join("big-dl.bin");
    mc.cmd(&["cp", "tinio/mc-bucket/big.bin", big_dl.to_str().unwrap()])
        .assert()
        .success();
    e2e::files_equal(&big, &big_dl);

    // List + delete.
    mc.cmd(&["ls", "tinio/mc-bucket"])
        .assert()
        .success()
        .stdout(predicate::str::contains("hello.txt"));
    mc.cmd(&["rm", "tinio/mc-bucket/hello.txt"])
        .assert()
        .success();
    mc.cmd(&["rb", "tinio/mc-bucket", "--force"])
        .assert()
        .success();
}
