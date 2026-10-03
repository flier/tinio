//! The block-store contract, proven against the local implementation.
//!
//! This target is the template a future backend copies: point it at another
//! `BlockStore` and the same rules are asserted.

use tinio_blob::{Error, LocalBlocks, Store, testing};
use tinio_util::testing::assert_send_sync;

#[tokio::test]
async fn the_local_block_store_meets_the_contract() {
    let dir = tempfile::tempdir().unwrap();
    testing::assert_block_store(&LocalBlocks::new(dir.path())).await;
}

#[test]
fn the_public_types_cross_threads() {
    assert_send_sync::<Error>();
    assert_send_sync::<LocalBlocks>();
    assert_send_sync::<Store<LocalBlocks>>();
}
