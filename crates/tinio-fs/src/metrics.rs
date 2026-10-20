//! The versioning metric families, declared in the crate that observes them.
//!
//! The numbers live here because the sweep does: the GC counters are fed from
//! [`Summary`] at the end of every sweep pass, and the noncurrent-version
//! gauge is the retention walk's own view, counted. A family declared in
//! `tinio-server` would invert the layering — the observing code is this
//! crate's — and the `prometheus` default registry is process-global, so a
//! family registered here is gathered by the management plane's `/metrics` on
//! the same process (the shape `tinio-sandbox-linux`'s metrics module pins).
//!
//! `specs/001-s3-local-server/data-model.md`'s `## Metrics (read-only views, not persisted)` owns the name
//! list. Additive only: every family here is a new name, and nothing changes
//! an existing one.

use std::sync::LazyLock;

use prometheus::{IntCounter, IntGauge, register_int_counter, register_int_gauge};
use tinio_util::metrics::RegisteredExt;

use crate::sweep::Summary;

/// Block files reclaimed by the block sweep, cumulative over the process's
/// passes — the sweep's own `reclaimed_blocks`, summed.
static RECLAIMED_BLOCKS: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "tinio_storage_blocks_reclaimed_total",
        "Block files reclaimed by the block sweep, cumulative"
    )
    .registered("tinio_storage_blocks_reclaimed_total")
});

/// Bytes reclaimed by the block sweep, cumulative over the process's passes.
static RECLAIMED_BYTES: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "tinio_storage_bytes_reclaimed_total",
        "Bytes reclaimed by the block sweep, cumulative"
    )
    .registered("tinio_storage_bytes_reclaimed_total")
});

/// Noncurrent versions retained across the versioned buckets, as the last
/// sweep pass counted them — the retention walk's view ([`Summary`]'s own
/// `expired_versions` is what a pass dropped, this is what remains).
static NONCURRENT_VERSIONS: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "tinio_storage_noncurrent_versions",
        "Noncurrent versions retained across versioned buckets, as of the last sweep pass"
    )
    .registered("tinio_storage_noncurrent_versions")
});

/// Total bytes held by the versioned buckets' block stores, as the last sweep
/// pass walked them — the walk that already stat'ed every block
/// ([`Summary`]'s own `reclaimed_bytes` is what a pass dropped, this is what
/// remains; the store has no hard cap, so this family is the capacity
/// signal).
static BLOCK_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "tinio_storage_block_bytes",
        "Bytes held by the versioned buckets' block stores, as of the last sweep pass"
    )
    .registered("tinio_storage_block_bytes")
});

/// Stored blocks that failed verification against their own name, cumulative
/// across verify passes (the doctor's and the post-repair startup pass's) —
/// each pass adds its findings, so the family answers "corrupt since start".
static BLOCKS_CORRUPT: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "tinio_storage_blocks_corrupt_total",
        "Stored blocks that failed verification against their own content hash, cumulative"
    )
    .registered("tinio_storage_blocks_corrupt_total")
});

/// Record one sweep pass's reclamation: the counters add the pass's blocks and
/// bytes, so the families answer "reclaimed since start", never "in the last
/// pass".
pub(crate) fn record_sweep(summary: &Summary) {
    RECLAIMED_BLOCKS.inc_by(summary.reclaimed_blocks);
    RECLAIMED_BYTES.inc_by(summary.reclaimed_bytes);
}

/// Record the noncurrent-version count the pass derived.
pub(crate) fn record_noncurrent_versions(count: u64) {
    NONCURRENT_VERSIONS.set(i64::try_from(count).unwrap_or(i64::MAX));
}

/// Record the block-store size the pass measured.
pub(crate) fn record_block_bytes(bytes: u64) {
    BLOCK_BYTES.set(i64::try_from(bytes).unwrap_or(i64::MAX));
}

/// Record a verify pass's corrupt-block findings.
pub(crate) fn record_blocks_corrupt(count: u64) {
    BLOCKS_CORRUPT.inc_by(count);
}

#[cfg(test)]
mod tests {

    use tinio_util::testing::metrics::{assert_registered, exposition};

    use super::*;

    #[test]
    fn the_version_families_render_their_recorded_values() {
        let summary = Summary {
            reclaimed_blocks: 2,
            reclaimed_bytes: 8,
            ..Summary::default()
        };
        record_sweep(&summary);
        record_noncurrent_versions(3);
        record_block_bytes(16);
        record_blocks_corrupt(1);

        assert_registered(&[
            "tinio_storage_blocks_reclaimed_total",
            "tinio_storage_bytes_reclaimed_total",
            "tinio_storage_noncurrent_versions",
            "tinio_storage_block_bytes",
            "tinio_storage_blocks_corrupt_total",
        ]);
        let text = exposition();
        assert!(
            text.contains("tinio_storage_blocks_reclaimed_total"),
            "the reclaimed-blocks counter is declared and carries a sample: {text}"
        );
        assert!(
            text.contains("tinio_storage_bytes_reclaimed_total"),
            "the reclaimed-bytes counter is declared: {text}"
        );
        assert!(
            text.contains("tinio_storage_noncurrent_versions"),
            "the noncurrent-versions gauge is declared: {text}"
        );
        assert!(
            text.contains("tinio_storage_block_bytes"),
            "the block-store size gauge is declared: {text}"
        );
        assert!(
            text.contains("tinio_storage_blocks_corrupt_total"),
            "the corrupt-blocks counter is declared: {text}"
        );
    }
}
