//! The aggregate `Metadata` conformance suite.
//!
//! The per-category legs run beside the implementations (`meta/bucket.rs`,
//! `meta/object.rs`, `meta/upload.rs`, each with its own `#[tokio::test]`).
//! Nothing else in the workspace called the aggregate
//! [`assert_metadata_conformance`] — only the out-of-workspace spike does
//! (`spike/pg-meta/src/main.rs`) — so its D22 refusal and atomicity legs had
//! never run against redb. This is that call, in the crate's own `tests/`
//! directory (review round 2, Task 6 addendum).

use tinio_store_redb::RedbMetadata;
use tinio_util::testing::metadata::assert_metadata_conformance;

#[tokio::test]
async fn metadata_conforms() {
    let store = RedbMetadata::in_memory().expect("the in-memory store opens");
    assert_metadata_conformance(&store).await;
}
