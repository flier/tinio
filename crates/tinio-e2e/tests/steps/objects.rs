//! Object steps (T025, SC-006): upload/download/delete round trips, range
//! and conditional requests, concurrent writes, and fs staging semantics.
//! Ported from `tinio-server/tests/data_plane.rs`.

use cucumber::{given, then, when};

use super::common::{deterministic_bytes, md5_hex};

#[given(expr = "I upload {string} with body {string}")]
#[when(expr = "I upload {string} with body {string}")]
#[then(expr = "I upload {string} with body {string}")]
async fn upload_body(world: &mut super::World, key: String, body: String) {
    world.last_upload = body.clone().into_bytes();
    world.last = world
        .client
        .request("PUT", &format!("/{key}"), &[], world.last_upload.as_slice())
        .await;
}

/// A body of `n` deterministic bytes: the same size always produces the
/// same bytes within a run, so the upload and the later GET compare
/// against the same stored copy.
#[given(expr = "I upload {string} with {int} bytes")]
async fn upload_bytes(world: &mut super::World, key: String, n: u64) {
    let body = deterministic_bytes(n);
    world.last_upload = body.clone();
    world.last = world
        .client
        .request("PUT", &format!("/{key}"), &[], &body)
        .await;
}

#[given(expr = "I delete object {string}")]
#[when(expr = "I delete object {string}")]
async fn delete_object(world: &mut super::World, key: String) {
    world.last = world
        .client
        .request("DELETE", &format!("/{key}"), &[], &[])
        .await;
}

/// Server-side copy (FR-015): PUT with `x-amz-copy-source` — the content
/// never passes through the client. The destination key is the request
/// path; the response is the `CopyObjectResult` XML.
#[given(regex = r#"I copy object "([^"]+)" to "([^"]+)""#)]
#[when(regex = r#"I copy object "([^"]+)" to "([^"]+)""#)]
async fn copy_object(world: &mut super::World, src: String, dst: String) {
    let source = format!("/{src}");
    world.last = world
        .client
        .request(
            "PUT",
            &format!("/{dst}"),
            &[("x-amz-copy-source", source.as_str())],
            &[],
        )
        .await;
}

#[given(expr = "I get object {string}")]
#[when(expr = "I get object {string}")]
async fn get_object(world: &mut super::World, key: String) {
    world.last = world
        .client
        .request("GET", &format!("/{key}"), &[], &[])
        .await;
}

#[when(expr = "I head object {string}")]
async fn head_object(world: &mut super::World, key: String) {
    world.last = world
        .client
        .request("HEAD", &format!("/{key}"), &[], &[])
        .await;
}

#[then(expr = "the object body equals the uploaded bytes")]
async fn body_equals_upload(world: &mut super::World) {
    assert_eq!(world.last.body, world.last_upload, "body mismatch");
}

// The `the object body is {string}` phrase (objects/multipart/
// conditions/reserved_paths features) is registered on the shared
// exact-equality assertion in common.rs.

#[then(expr = "the object body length is {int}")]
async fn body_len(world: &mut super::World, n: u64) {
    assert_eq!(world.last.body.len() as u64, n, "body length mismatch");
}

/// The last response's ETag equals the MD5 of its served body — the old
/// test's content-MD5 invariant. (The brief's draft compared against
/// `last_upload`, but the round-trip scenario GETs objects uploaded
/// several steps earlier, so the tracked bytes would be stale; the body
/// itself is the object's uploaded bytes, pinned by the body assertions.)
#[then(expr = "the object ETag matches the MD5 of the uploaded bytes")]
async fn etag_md5_of_upload(world: &mut super::World) {
    assert_etag(world, &format!("\"{}\"", md5_hex(&world.last.body)));
}

/// The last response's ETag equals the MD5 of `text` — the out-of-band
/// scenario's variant, where the bytes never went through an upload step
/// (the old test's hard-coded digest assertion).
#[then(expr = "the object ETag is the MD5 of {string}")]
async fn etag_md5_of_text(world: &mut super::World, text: String) {
    assert_etag(world, &format!("\"{}\"", md5_hex(text.as_bytes())));
}

fn assert_etag(world: &super::World, expected: &str) {
    let etag = world
        .last
        .header("etag")
        .expect("ETag header must be present");
    assert_eq!(etag, expected, "ETag mismatch");
}

/// Two concurrent PUTs (distinct content, same length) on separate cloned
/// clients; the final object must be exactly one writer's payload — never
/// a torn mix (the old test's `any(|p| p == &on_disk)` assertion).
#[when(regex = r#"I concurrently upload "([^"]+)" and "([^"]+)" with (\d+) bytes each"#)]
async fn concurrent_upload(world: &mut super::World, k1: String, k2: String, n: u64) {
    let b1 = deterministic_bytes(n);
    let mut b2 = deterministic_bytes(n);
    b2.reverse(); // distinct content, same length
    let c1 = world.client.clone();
    let c2 = world.client.clone();
    let p1 = format!("/{k1}");
    let p2 = format!("/{k2}");
    let (r1, r2) = tokio::join!(
        c1.request("PUT", &p1, &[], &b1),
        c2.request("PUT", &p2, &[], &b2),
    );
    assert_eq!(r1.status, 200, "first concurrent PUT failed");
    assert_eq!(r2.status, 200, "second concurrent PUT failed");
    // Both writes are done; whatever won, the wire object is intact.
    let final_resp = world
        .client
        .request("GET", &format!("/{k1}"), &[], &[])
        .await;
    assert!(
        final_resp.body == b1 || final_resp.body == b2,
        "final object is a torn mix of the two payloads"
    );
    world.last = final_resp;
    world.last_upload = b1;
}

/// An upload whose body is cut off mid-stream: the headers declare
/// `declared` bytes, only `sent` are delivered, then the connection drops
/// (the old test's `abort_mid_upload`).
#[when(expr = "I interrupt the upload of {string} after {int} of {int} bytes")]
async fn interrupt_upload(world: &mut super::World, key: String, sent: usize, declared: usize) {
    let addr = world.server.as_ref().expect("server running").addr();
    let partial = vec![b'x'; sent];
    super::common::abort_mid_upload(addr, &format!("/{key}"), declared, &partial).await;
}

/// The interrupted upload left no temp file under the state dir (the
/// writer removes it best-effort once the aborted stream errors — poll
/// briefly for the server to notice the dropped connection).
#[then("no temp file remains under the state dir")]
async fn no_tmp_remains(world: &mut super::World) {
    let root = world
        .server
        .as_ref()
        .expect("server running")
        .root()
        .expect("fs-backed server root");
    let tmp = root.join(".tinio/tmp");
    let clean = super::common::eventually(|| {
        !tmp.exists() || std::fs::read_dir(&tmp).unwrap().next().is_none()
    })
    .await;
    assert!(clean, "temp file left behind under {tmp:?}");
}
