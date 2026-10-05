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
//! `specs/001-s3-local-server/data-model.md`'s `## Metrics` owns the name
//! list. Additive only: every family here is a new name, and nothing changes
//! an existing one.

use std::sync::LazyLock;

use prometheus::{IntCounter, IntGauge, register_int_counter, register_int_gauge};

use crate::sweep::Summary;

/// Block files reclaimed by the block sweep, cumulative over the process's
/// passes — the sweep's own `reclaimed_blocks`, summed.
static RECLAIMED_BLOCKS: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "tinio_storage_blocks_reclaimed_total",
        "Block files reclaimed by the block sweep, cumulative"
    )
    .expect("register tinio_storage_blocks_reclaimed_total")
});

/// Bytes reclaimed by the block sweep, cumulative over the process's passes.
static RECLAIMED_BYTES: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "tinio_storage_bytes_reclaimed_total",
        "Bytes reclaimed by the block sweep, cumulative"
    )
    .expect("register tinio_storage_bytes_reclaimed_total")
});

/// Noncurrent versions retained across the versioned buckets, as the last
/// sweep pass counted them — the retention walk's view ([`Summary`]'s own
/// `expired_versions` is what a pass dropped, this is what remains).
static NONCURRENT_VERSIONS: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "tinio_storage_noncurrent_versions_total",
        "Noncurrent versions retained across versioned buckets, as of the last sweep pass"
    )
    .expect("register tinio_storage_noncurrent_versions_total")
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

#[cfg(test)]
mod tests {
    use prometheus::{Encoder, TextEncoder, default_registry};

    use super::*;

    fn exposition() -> String {
        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&default_registry().gather(), &mut buffer)
            .unwrap();
        String::from_utf8(buffer).unwrap()
    }

    #[test]
    fn the_version_families_render_their_recorded_values() {
        let summary = Summary {
            reclaimed_blocks: 2,
            reclaimed_bytes: 8,
            ..Summary::default()
        };
        record_sweep(&summary);
        record_noncurrent_versions(3);

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
            text.contains("tinio_storage_noncurrent_versions_total"),
            "the noncurrent-versions gauge is declared: {text}"
        );
    }
}
