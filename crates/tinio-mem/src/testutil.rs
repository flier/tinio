//! Shared test helpers (`#[cfg(test)]` only).

use std::sync::{Arc, OnceLock};

use crate::_core::checksum;

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
