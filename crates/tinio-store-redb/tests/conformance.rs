//! The aggregate `Metadata` conformance suite.
//!
//! The per-category legs run beside the implementations (`meta/bucket.rs`,
//! `meta/object.rs`, `meta/upload.rs`, each with its own `#[tokio::test]`).
//! At the time (review round 2, Task 6) nothing in the workspace called the
//! aggregate [`assert_metadata_conformance`], and the only runner anywhere was
//! the out-of-workspace pg spike (`spike/pg-meta/src/main.rs`, deleted by
//! `5304d33b`; its successor is `crates/tinio-store-pg/src/tests/`), so the
//! suite's D22 refusal and atomicity legs had never run against redb. This is
//! that call, in the crate's own `tests/` directory.

use tinio_store_redb::RedbMetadata;
use tinio_util::testing::metadata::{
    assert_guarded_write_conformance, assert_metadata_conformance,
};

#[tokio::test]
async fn metadata_conforms() {
    let store = RedbMetadata::in_memory().expect("the in-memory store opens");
    assert_metadata_conformance(&store).await;
}

/// The opt-in guarded writers redb overrides. They are not part of the
/// aggregate suite: a backend that takes their default cannot pass them, so the
/// aggregate stays callable by every `Metadata` implementation while this call
/// pins the guard redb actually ships.
#[tokio::test]
async fn guarded_writes_conform() {
    let store = RedbMetadata::in_memory().expect("the in-memory store opens");
    assert_guarded_write_conformance(&store).await;
}
