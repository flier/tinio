//! Interop advanced scenarios (task T033) — multipart upload (> 8 MiB →
//! composed ETag pattern), server-side copy, and cold listing with and
//! without the scanner — via aws cli v2 and rclone. CI-gated (FR-025).
//!
//! Run: `cargo test -p tinio-server --test advanced -- --ignored`

mod e2e;

use std::fs;

use e2e::{Rclone, Server};
use predicates::prelude::*;

#[test]
#[ignore = "requires aws cli v2 and rclone on PATH"]
fn advanced() {
    let dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let big = scratch.path().join("big.bin");
    e2e::write_bytes(&big, 10 * 1024 * 1024);

    // --- multipart: > 8 MiB file → composed ETag "md5-N" -----------------
    let server = Server::start_at(dir.path(), Some(true));
    let ep = server.endpoint();
    e2e::aws_s3(ep, "mb", &["s3://adv-bucket"])
        .assert()
        .success();
    e2e::aws_s3(
        ep,
        "cp",
        &[big.to_str().unwrap(), "s3://adv-bucket/big.bin"],
    )
    .assert()
    .success();
    e2e::aws(
        ep,
        &[
            "s3api",
            "head-object",
            "--bucket",
            "adv-bucket",
            "--key",
            "big.bin",
            "--query",
            "ETag",
            "--output",
            "text",
        ],
    )
    .assert()
    .success()
    .stdout(predicate::str::contains("-"));
    let big_down = scratch.path().join("big-downloaded.bin");
    e2e::aws_s3(
        ep,
        "cp",
        &["s3://adv-bucket/big.bin", big_down.to_str().unwrap()],
    )
    .assert()
    .success();
    e2e::files_equal(&big, &big_down);

    // --- server-side copy (no client passthrough) ------------------------
    e2e::aws_s3(
        ep,
        "cp",
        &["s3://adv-bucket/big.bin", "s3://adv-bucket/copy.bin"],
    )
    .assert()
    .success();
    let copy_down = scratch.path().join("copy-downloaded.bin");
    e2e::aws_s3(
        ep,
        "cp",
        &["s3://adv-bucket/copy.bin", copy_down.to_str().unwrap()],
    )
    .assert()
    .success();
    e2e::files_equal(&big, &copy_down);

    // --- cold listing (scanner ON) ---------------------------------------
    // Files dropped by hand on the filesystem, then listed via the API:
    // the first listing computes ETags synchronously; with the scanner
    // running, later listings are warm.
    let cold = dir.path().join("cold-bucket");
    fs::create_dir_all(&cold).unwrap();
    for i in 1..=50 {
        fs::write(cold.join(format!("file-{i}.txt")), format!("cold file {i}")).unwrap();
    }
    e2e::aws_s3(ep, "ls", &["s3://cold-bucket/"])
        .assert()
        .success()
        .stdout(predicate::str::contains("file-50.txt"));

    drop(server);

    // --- cold listing (scanner OFF, same root) ---------------------------
    let server = Server::start_at(dir.path(), Some(false));
    let ep = server.endpoint();
    e2e::aws_s3(ep, "ls", &["s3://cold-bucket/"])
        .assert()
        .success()
        .stdout(predicate::str::contains("file-50.txt"));

    // --- rclone multipart + copy -----------------------------------------
    let rclone = Rclone::new(scratch.path().join("rclone.conf"));
    rclone.remote(ep).assert().success();
    rclone
        .cmd(&["copy", big.to_str().unwrap(), "tinio:adv-bucket/"])
        .assert()
        .success();
    let rclone_dl = scratch.path().join("rclone-dl");
    fs::create_dir_all(&rclone_dl).unwrap();
    rclone
        .cmd(&[
            "copy",
            "tinio:adv-bucket/big.bin",
            rclone_dl.to_str().unwrap(),
        ])
        .assert()
        .success();
    e2e::files_equal(&big, &rclone_dl.join("big.bin"));
    rclone
        .cmd(&["copy", "tinio:adv-bucket/big.bin", "tinio:adv-bucket/"])
        .assert()
        .success();
    // Best-effort consistency check (non-fatal in the bash scenario).
    let _ = rclone
        .cmd(&[
            "check",
            "tinio:adv-bucket",
            scratch.path().to_str().unwrap(),
            "--include",
            "big.bin",
        ])
        .output();
}
