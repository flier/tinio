//! Shared test helpers (`#[cfg(test)]` only).

use std::sync::{Arc, OnceLock};

use crate::_core::{acl, checksum};

/// A canonical owner id — the standard test owner (same shape as the fs
/// suite's helper).
pub(crate) fn owner_id() -> acl::OwnerId {
    acl::OwnerId::new("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899").unwrap()
}

/// A second canonical owner id, distinct from [`owner_id`].
pub(crate) fn other_owner_id() -> acl::OwnerId {
    acl::OwnerId::new("ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100").unwrap()
}

/// A preset server tee slot (spec 2026-08-31): the digest cell already
/// holds `algorithm`/`base64_value`, so a staged body commits it as the
/// object's recorded checksum and an uploaded part retains it. The
/// server's tee would fill the cell while the body streamed; tests preset
/// it — the backends never validate the value against the bytes (mirrors
/// the fs suite's helper of the same shape).
pub(crate) fn checksum_tee(
    algorithm: checksum::Algorithm,
    base64_value: &str,
) -> Arc<checksum::PartChecksum> {
    let tee = Arc::new(checksum::PartChecksum {
        digest: OnceLock::new(),
        etag: None,
    });
    let _ = tee.digest.set(checksum::Part {
        algorithm,
        value: checksum::Value(base64_value.into()),
    });
    tee
}
