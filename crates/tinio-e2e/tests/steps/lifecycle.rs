//! The lifecycle steps (FR-042): a configuration document on the bucket
//! subresource, and the pass's effect on the wire.
//!
//! **The raw requests are the shared ones** (`errors.rs`): `?lifecycle` is a
//! bucket path like any other, so `GET`/`DELETE` need no step of their own.
//! What the corpus cannot spell is here — a document written as a docstring
//! with its own checksum, and the bounded poll an expiry needs.
//!
//! **Why the document carries a computed `Content-MD5`**:
//! `contracts/s3-surface.md` §Lifecycle requires a checksum on the PUT, and the
//! server form-checks `Content-MD5` (16-byte base64) — the digest's *value* is
//! never compared, because s3s's deserializer consumes the body before the
//! handler runs (the deviation `backend/cors.rs`, `backend/acls.rs` and
//! `backend/lifecycle.rs` all record). Computing the real digest keeps the step
//! honest against a future that does compare it.

use cucumber::{gherkin::Step, then, when};
use md5::{Digest as _, Md5};
use tokio::time::{Duration, Instant, sleep};

use super::cli::docstring;

/// The poll's tick. A pass runs on the `[lifecycle]` cadence the scenario
/// configures (tens of milliseconds), so a few ticks cover a full pass while
/// the whole wait stays far below the step's bound.
const POLL: Duration = Duration::from_millis(50);

/// The standard-alphabet, padded base64 of `bytes` — what a `Content-MD5`
/// header carries (RFC 4648 §4). Written out rather than adding `base64` to
/// this crate for one header on one request.
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let tail = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = u32::from(tail[0]) << 16 | u32::from(tail[1]) << 8 | u32::from(tail[2]);
        for i in 0..4u32 {
            out.push(if (i as usize) < chunk.len() + 1 {
                ALPHABET[((n >> (18 - 6 * i)) & 0x3f) as usize] as char
            } else {
                '='
            });
        }
    }
    out
}

/// `PUT /<bucket>?lifecycle` with the document the step carries as a
/// docstring, replacing the bucket's whole configuration.
///
/// The document is the framework's XML (`contracts/s3-surface.md` §Lifecycle),
/// which is why the corpus spells the wire shape and not the plane's JSON.
#[when(expr = "I put this lifecycle configuration on bucket {string}")]
async fn put_lifecycle_configuration(world: &mut super::World, bucket: String, step: &Step) {
    let body = docstring(step);
    let content_md5 = base64(&Md5::digest(body.as_bytes()));
    let headers = [
        ("Content-Type", "application/xml"),
        ("Content-MD5", content_md5.as_str()),
    ];
    world.last = world
        .client
        .request(
            "PUT",
            &format!("/{bucket}?lifecycle"),
            &headers,
            body.as_bytes(),
        )
        .await;
}

/// A bounded wait for the background pass: `{string}` is the request path of
/// the object (`/data/a.txt`), and "gone" is the wire's own answer — the
/// object's `404`, which is what expiry means to a client.
///
/// **Signed as the harness's root pair**, because the scenario driving this
/// step is `@cli`: a spawned server authenticates, and `the child holds the
/// harness's credentials` puts that same pair on it. A client answered with
/// anything but `404` until the bound is a failed expiry, reported with the
/// last status and body rather than as a timeout.
#[then(expr = "the key {string} is gone within {int} milliseconds")]
async fn the_key_is_gone_within(world: &mut super::World, path: String, milliseconds: u64) {
    let deadline = Instant::now() + Duration::from_millis(milliseconds);
    loop {
        let response = world
            .client
            .signed_request("GET", &path, &[], &[], super::common::ACL_ROOT)
            .await;
        if response.status == 404 {
            world.last = response;
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "`{path}` was still served after {milliseconds} ms (status {}): the lifecycle \
                 pass did not expire it\n{}",
                response.status,
                String::from_utf8_lossy(&response.body)
            );
        }
        sleep(POLL).await;
    }
}
