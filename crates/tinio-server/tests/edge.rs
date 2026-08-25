//! Edge-case e2e scenarios on top of the core journey (T032/T033):
//! special-character keys over real HTTP, multipart size boundary, Range
//! downloads, overwrite (last-write-wins), pagination truncation, and
//! error paths (missing objects/buckets, non-empty bucket delete) — via
//! aws cli v2; plus an mc special-key/overwrite supplement. Targeted/manual
//! runs; NOT CI-gated (the CI interop stage runs the bash scenarios).
//!
//! Run: `cargo test -p tinio-server --test edge -- --ignored`

mod e2e;

use std::fs;

use predicates::prelude::*;

/// ETag from `head-object` (text output, no quotes).
fn etag(ep: &str, key: &str) -> String {
    let out = e2e::aws(
        ep,
        &[
            "s3api",
            "head-object",
            "--bucket",
            "edge-bucket",
            "--key",
            key,
            "--query",
            "ETag",
            "--output",
            "text",
        ],
    )
    .assert()
    .success()
    .get_output()
    .stdout
    .clone();
    String::from_utf8_lossy(&out).into_owned()
}

#[test]
#[ignore = "requires aws cli v2 on PATH"]
fn edges() {
    let server = e2e::Server::start();
    let ep = server.endpoint();
    let scratch = tempfile::tempdir().unwrap();

    e2e::aws_s3(ep, "mb", &["s3://edge-bucket"])
        .assert()
        .success();

    // --- special-character keys round-trip through real HTTP ------------
    let data = scratch.path().join("data.bin");
    e2e::write_bytes(&data, 4096);
    for k in [
        "a b.txt",
        "中文.txt",
        "emoji-🎯.txt",
        "hash#pct%plus+at@.txt",
        ".hidden.txt",
    ] {
        let target = format!("s3://edge-bucket/{k}");
        e2e::aws_s3(ep, "cp", &[data.to_str().unwrap(), &target])
            .assert()
            .success();
        let dl = scratch.path().join("dl.bin");
        e2e::aws_s3(ep, "cp", &[&target, dl.to_str().unwrap()])
            .assert()
            .success();
        e2e::files_equal(&data, &dl);
    }

    // Deep nesting.
    e2e::aws_s3(
        ep,
        "cp",
        &[data.to_str().unwrap(), "s3://edge-bucket/a/b/c/d/e/f.txt"],
    )
    .assert()
    .success();
    let dl = scratch.path().join("dl.bin");
    e2e::aws_s3(
        ep,
        "cp",
        &["s3://edge-bucket/a/b/c/d/e/f.txt", dl.to_str().unwrap()],
    )
    .assert()
    .success();
    e2e::files_equal(&data, &dl);

    // Directory-ending keys (`key/`) are NOT stored as objects: the fs
    // backend maps them to directories under the bucket root (put answers
    // with an empty-body ETag, head 404s). Known v1 path-mapping limit —
    // see e2e/interop/README.md (known deviations).
    // --- multipart size boundary ----------------------------------------
    // 1 MiB → single PUT (ETag = content MD5); 16 MiB → multipart
    // (composed ETag with a `-N` suffix). No assertion on the exact
    // threshold — that is aws-cli-internal (default 8 MB).
    let one = scratch.path().join("one.bin");
    e2e::write_bytes(&one, 1024 * 1024);
    e2e::aws_s3(
        ep,
        "cp",
        &[one.to_str().unwrap(), "s3://edge-bucket/one.bin"],
    )
    .assert()
    .success();
    let big = scratch.path().join("big.bin");
    e2e::write_bytes(&big, 16 * 1024 * 1024);
    e2e::aws_s3(
        ep,
        "cp",
        &[big.to_str().unwrap(), "s3://edge-bucket/big.bin"],
    )
    .assert()
    .success();
    assert!(
        !etag(ep, "one.bin").contains('-'),
        "single upload must not have a composed ETag"
    );
    assert!(
        etag(ep, "big.bin").contains('-'),
        "multipart upload must have a composed ETag (md5-N)"
    );
    // Multipart content is byte-identical on download.
    let big_dl = scratch.path().join("big-dl.bin");
    e2e::aws_s3(
        ep,
        "cp",
        &["s3://edge-bucket/big.bin", big_dl.to_str().unwrap()],
    )
    .assert()
    .success();
    e2e::files_equal(&big, &big_dl);

    // --- Range download --------------------------------------------------
    let rng = scratch.path().join("range.bin");
    e2e::write_bytes(&rng, 1024 * 1024);
    e2e::aws_s3(
        ep,
        "cp",
        &[rng.to_str().unwrap(), "s3://edge-bucket/range.bin"],
    )
    .assert()
    .success();
    let part = scratch.path().join("part.bin");
    e2e::aws(
        ep,
        &[
            "s3api",
            "get-object",
            "--bucket",
            "edge-bucket",
            "--key",
            "range.bin",
            "--range",
            "bytes=0-99",
            part.to_str().unwrap(),
        ],
    )
    .assert()
    .success();
    let src = fs::read(&rng).unwrap();
    let got = fs::read(&part).unwrap();
    assert_eq!(got.len(), 100, "range reply must be exactly 100 bytes");
    assert_eq!(got, src[..100], "range reply must match the source prefix");

    // --- overwrite (last-write-wins) ------------------------------------
    let v1 = scratch.path().join("v1.txt");
    let v2 = scratch.path().join("v2.txt");
    fs::write(&v1, "first version").unwrap();
    fs::write(&v2, "second version — overwritten").unwrap();
    for f in [&v1, &v2] {
        e2e::aws_s3(
            ep,
            "cp",
            &[f.to_str().unwrap(), "s3://edge-bucket/overwrite.txt"],
        )
        .assert()
        .success();
    }
    let ov = scratch.path().join("ov-dl.txt");
    e2e::aws_s3(
        ep,
        "cp",
        &["s3://edge-bucket/overwrite.txt", ov.to_str().unwrap()],
    )
    .assert()
    .success();
    e2e::files_equal(&v2, &ov);

    // --- pagination (MaxKeys truncation) --------------------------------
    // 1100 objects dropped by hand on the filesystem; the API must page
    // them (default page is 1000, here forced to 100).
    let paged = server.root().join("paged-bucket");
    fs::create_dir_all(&paged).unwrap();
    for i in 0..1100 {
        fs::write(paged.join(format!("obj-{i:04}.txt")), "x").unwrap();
    }
    let kc = e2e::aws(
        ep,
        &[
            "s3api",
            "list-objects-v2",
            "--bucket",
            "paged-bucket",
            "--max-keys",
            "100",
            "--query",
            "KeyCount",
            "--output",
            "text",
        ],
    )
    .assert()
    .success()
    .get_output()
    .stdout
    .clone();
    assert_eq!(
        String::from_utf8_lossy(&kc).trim(),
        "100",
        "truncated page must return exactly 100 keys"
    );
    e2e::aws(
        ep,
        &[
            "s3api",
            "list-objects-v2",
            "--bucket",
            "paged-bucket",
            "--max-keys",
            "100",
            "--query",
            "IsTruncated",
            "--output",
            "text",
        ],
    )
    .assert()
    .success()
    .stdout(predicate::str::contains("True"));
    // The continuation token pages on to the next 100.
    let token = e2e::aws(
        ep,
        &[
            "s3api",
            "list-objects-v2",
            "--bucket",
            "paged-bucket",
            "--max-keys",
            "100",
            "--query",
            "NextContinuationToken",
            "--output",
            "text",
        ],
    )
    .assert()
    .success()
    .get_output()
    .stdout
    .clone();
    let token = String::from_utf8_lossy(&token).trim().to_string();
    assert!(
        !token.is_empty(),
        "truncated page must yield a continuation token"
    );
    e2e::aws(
        ep,
        &[
            "s3api",
            "list-objects-v2",
            "--bucket",
            "paged-bucket",
            "--max-keys",
            "100",
            "--continuation-token",
            &token,
            "--query",
            "KeyCount",
            "--output",
            "text",
        ],
    )
    .assert()
    .success()
    .stdout(predicate::str::contains("100"));

    // --- error paths -----------------------------------------------------
    // head-object on a missing key → 404. The server answers with the
    // raw `404` code (not AWS's `NoSuchKey`) — known v1 deviation, see
    // e2e/interop/README.md.
    e2e::aws(
        ep,
        &[
            "s3api",
            "head-object",
            "--bucket",
            "edge-bucket",
            "--key",
            "missing.txt",
        ],
    )
    .assert()
    .failure()
    .stderr(predicate::str::contains("404"));
    // List a missing bucket → NoSuchBucket.
    e2e::aws_s3(ep, "ls", &["s3://no-such-bucket/"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("NoSuchBucket"));
    // Delete a non-empty bucket without --force → BucketNotEmpty.
    e2e::aws_s3(ep, "rb", &["s3://edge-bucket"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("BucketNotEmpty"));
    // Delete a missing object is idempotent (aws cli lists first).
    e2e::aws_s3(ep, "rm", &["s3://edge-bucket/missing.txt"])
        .assert()
        .success();
    // Delete a missing bucket.
    e2e::aws_s3(ep, "rb", &["s3://no-such-bucket"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("NoSuchBucket"));

    // --- shortest legal bucket name -------------------------------------
    e2e::aws_s3(ep, "mb", &["s3://abc"]).assert().success();
    e2e::aws_s3(ep, "rb", &["s3://abc"]).assert().success();
}

#[test]
#[ignore = "requires the mc binary on PATH"]
fn mc_edges() {
    let server = e2e::Server::start();
    let ep = server.endpoint();
    let scratch = tempfile::tempdir().unwrap();
    let mc = e2e::Mc::new(scratch.path().join("mc"));

    mc.alias(ep).assert().success();
    mc.cmd(&["mb", "tinio/edge-bucket"]).assert().success();

    // Special-character keys (space, CJK, emoji) round-trip.
    let data = scratch.path().join("data.bin");
    e2e::write_bytes(&data, 4096);
    for k in ["a b.txt", "中文.txt", "emoji-🎯.txt"] {
        let target = format!("tinio/edge-bucket/{k}");
        mc.cmd(&["cp", data.to_str().unwrap(), &target])
            .assert()
            .success();
        let dl = scratch.path().join("dl.bin");
        mc.cmd(&["cp", &target, dl.to_str().unwrap()])
            .assert()
            .success();
        e2e::files_equal(&data, &dl);
    }

    // Deep nesting + overwrite (last-write-wins).
    mc.cmd(&["cp", data.to_str().unwrap(), "tinio/edge-bucket/x/y/z.txt"])
        .assert()
        .success();
    let v1 = scratch.path().join("v1.txt");
    let v2 = scratch.path().join("v2.txt");
    fs::write(&v1, "mc version one").unwrap();
    fs::write(&v2, "mc version two — overwritten").unwrap();
    for f in [&v1, &v2] {
        mc.cmd(&["cp", f.to_str().unwrap(), "tinio/edge-bucket/ov.txt"])
            .assert()
            .success();
    }
    let ov = scratch.path().join("ov-dl.txt");
    mc.cmd(&["cp", "tinio/edge-bucket/ov.txt", ov.to_str().unwrap()])
        .assert()
        .success();
    e2e::files_equal(&v2, &ov);
}
