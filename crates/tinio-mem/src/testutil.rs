//! Shared test helpers (`#[cfg(test)]` only).

use std::sync::Arc;

use bytes::Bytes;

use crate::{
    _core::{
        acl, bucket, checksum,
        content::{ContentKey, ObjectContent, PublishId, StagedObject},
    },
    _util::testing::{checksum_tee as util_checksum_tee, conform_other_owner, conform_owner},
    MemoryStorage,
};

/// A canonical owner id — the standard test owner (same shape as the fs
/// suite's helper), the shared `tinio_util::testing::conform_owner` fixture.
pub(crate) fn owner_id() -> acl::OwnerId {
    conform_owner()
}

/// A second canonical owner id, distinct from [`owner_id`] — the shared
/// `tinio_util::testing::conform_other_owner` fixture.
pub(crate) fn other_owner_id() -> acl::OwnerId {
    conform_other_owner()
}

/// A preset server tee slot (spec 2026-08-31): the digest cell already
/// holds `algorithm`/`base64_value`, so a staged body commits it as the
/// object's recorded checksum and an uploaded part retains it. The
/// server's tee would fill the cell while the body streamed; tests preset
/// it — the backends never validate the value against the bytes (mirrors
/// the fs suite's helper of the same shape). The shared
/// `tinio_util::testing::checksum_tee` fixture.
pub(crate) fn checksum_tee(
    algorithm: checksum::Algorithm,
    base64_value: &str,
) -> Arc<checksum::PartChecksum> {
    util_checksum_tee(algorithm, base64_value)
}

/// One tagged publish of `bytes` at `key` through the storage's content
/// plane — the fixture the compensation tests publish with.
pub(crate) async fn publish_tagged_bytes(
    storage: &MemoryStorage,
    bucket: &bucket::Name,
    key: &ContentKey,
    bytes: &'static [u8],
    id: PublishId,
) {
    let mut staged = storage.content().stage(bucket, key).await.unwrap();
    staged.write(Bytes::from_static(bytes)).await.unwrap();
    storage
        .content()
        .publish_tagged(bucket, staged, key, id)
        .await
        .unwrap();
}
