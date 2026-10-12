//! The `>100 MiB` streaming leg (PG22's mitigation for the chunked-content risk): a body over
//! 100 MiB streams **through the pg backend** and round-trips byte for byte, and the publish
//! stays bounded (the store splits it into 1 MiB rows and commits them in batches, PG18/PG22).
//!
//! Home: a Rust leg, not a `@pg` cucumber scenario. The assertion is a byte-exact round trip of
//! a stream the backend must not hold whole — machinery a cucumber step would have to grow — and
//! a Rust leg is gated off every database-free path by `TINIO_PG_DSN` (PG8), so the default
//! `cargo test --workspace` never pays for it. A `@pg` scenario would instead ride the
//! serialized pass (one shared database, `--concurrency 1`) and its per-scenario truncate, and a
//! 100 MiB transfer there would distort the pass for every other scenario queued behind it.
//!
//! The database is read from `TINIO_PG_DSN`; with the variable unset the leg prints the skip
//! notice and returns. It creates its **own scratch database** through `tinio-store-pg`'s
//! `testing` harness (`TestDb`) and drops it, so the leg is safe under `cargo test`'s
//! parallelism and leaves no state behind.
#![cfg(feature = "versioning")]
// The leg addresses the crate, the standard library and the driver's re-exports by absolute
// path on purpose (the store's own suite states the same rule); `expect` here is an assertion —
// the harness asserts by panicking, and a stream that did not round-trip is the failure this
// task's acceptance is about.
#![expect(
    clippy::absolute_paths,
    reason = "test support: a leg addresses the crate by its public path"
)]

use futures::{StreamExt, stream};
use tinio_core::{
    acl::Acl,
    bucket, object,
    storage::{BodyStream, BucketOps, ObjectOps},
};
use tinio_pg::{_store_pg::test_support::TestDb, Access, PgStorage};
use tinio_util::testing::body;

/// The body's chunk size: the store's content row is 1 MiB (PG2), so the stream arrives in the
/// row-sized pieces a real client would send.
const CHUNK: usize = 1024 * 1024;
/// Whole 1 MiB chunks — 100 MiB exactly at this point.
const CHUNKS: usize = 100;
/// A tail shorter than a chunk: with it the body is strictly **over** 100 MiB, and the
/// store must land `CHUNKS + 1` rows rather than a whole-chunk multiple.
const TAIL: usize = 7;
/// The streamed length, `100 MiB + 7` bytes.
const SIZE: usize = CHUNKS * CHUNK + TAIL;

/// A compile-time guard on the leg's own claim: the body is strictly over 100 MiB.
const _: () = assert!(SIZE > 100 * 1024 * 1024);

/// The `TINIO_PG_DSN` gate and the scratch configuration both `tinio-pg` test targets share.
mod support;

use support::{require_dsn, scratch_config};

/// One chunk of the deterministic pattern: byte `offset` of chunk `index` is
/// `index + offset`, so a byte's value is a function of its absolute stream position alone —
/// the read side recomputes it without holding the stream.
fn pattern_chunk(index: usize, len: usize) -> Vec<u8> {
    let seed = index as u8;
    (0..len)
        .map(|offset| seed.wrapping_add(offset as u8))
        .collect()
}

/// The `>100 MiB` body as a lazy multi-chunk stream: one 1 MiB chunk and a tail, built as the
/// backend pulls them, so the test never materializes the whole body.
fn large_body() -> BodyStream {
    let chunks = (0..CHUNKS)
        .map(|index| body(pattern_chunk(index, CHUNK)))
        .chain(std::iter::once(body(pattern_chunk(CHUNKS, TAIL))));
    Box::pin(stream::iter(chunks).flatten())
}

/// The expected byte at absolute stream position `position`.
fn expected_byte(position: usize) -> u8 {
    ((position / CHUNK) as u8).wrapping_add((position % CHUNK) as u8)
}

/// PG22's mitigation: a body over 100 MiB streams through the backend and round-trips.
#[tokio::test]
async fn a_body_over_a_hundred_mib_streams_through_the_backend_and_round_trips() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let storage = PgStorage::connect(&scratch_config(&db), Access::ReadWrite)
        .await
        .expect("a writable store over the scratch database");

    let bucket = bucket::name("conform-large-body").expect("a valid bucket name");
    let key = object::key("large/stream.bin").expect("a valid object key");
    storage
        .create_bucket(&bucket, None, &Acl::default_private(None))
        .await
        .expect("the bucket");

    // The stream lands through the served write path (`put_object` = stage + commit). The
    // store splits it into 1 MiB rows and commits them in bounded batches (PG18/PG22), so the
    // call's contract is the whole body's ETag, not a statement per chunk.
    let put = storage
        .put_object(&bucket, &key, large_body())
        .await
        .expect("the stream publishes");
    assert!(
        !put.etag.as_str().is_empty(),
        "the publish records the content's ETag"
    );

    let mut get = storage
        .get_object(&bucket, &key, None)
        .await
        .expect("the served body");
    assert_eq!(
        get.info.size, SIZE as u64,
        "the served length is the stream's"
    );
    assert!(
        get.served_range.is_none(),
        "a full read serves the whole object"
    );

    // Read the body back and verify each byte against its stream position, so the check holds
    // whatever chunk boundaries the backend chose to serve.
    let mut position = 0usize;
    while let Some(chunk) = get.body.next().await {
        let chunk = chunk.expect("the body reads");
        for byte in chunk {
            assert_eq!(
                byte,
                expected_byte(position),
                "byte {position} of the round trip"
            );
            position += 1;
        }
    }
    assert_eq!(position, SIZE, "the whole stream round-trips");

    db.drop_db().await;
}

/// The pattern's own leg: the byte a position expects matches what the generator wrote, tail
/// included. Guards the round trip above against a generator and a checker that drifted together.
#[test]
fn the_pattern_is_a_function_of_the_absolute_position() {
    for position in [0, 1, CHUNK - 1, CHUNK, CHUNK + 5, CHUNKS * CHUNK, SIZE - 1] {
        let (index, offset) = (position / CHUNK, position % CHUNK);
        let written = pattern_chunk(index, offset + 1)[offset];
        assert_eq!(
            written,
            expected_byte(position),
            "position {position} in the generator and the checker"
        );
    }
}
