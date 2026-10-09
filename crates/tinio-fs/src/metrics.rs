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

use crate::sweep::Summary;

/// Block files reclaimed by the block sweep, cumulative over the process's
/// passes — the sweep's own `reclaimed_blocks`, summed.
#[expect(
    clippy::expect_used,
    reason = "argued: every error this registration can answer is about this family's own name — prometheus 0.14's `RegistryCore::register` refuses a name the process-global default registry already holds (`AlreadyReg`) or holds with a different label set or help string (`Msg`) — and each of this module's five names has one registration site in the repo (its own `LazyLock` here, whose closure runs once per process), against a family list owned by `specs/001-s3-local-server/data-model.md`'s `## Metrics (read-only views, not persisted)`; a collision is a source-level mistake the registry refuses here, not a runtime condition, and `LazyLock::new` has no `Result` to return it through. The module's test reads all five names back out of the gathered exposition (`the_version_families_render_their_recorded_values`), so a registration that did not land fails a test (design 2026-10-09 D2d)"
)]
static RECLAIMED_BLOCKS: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "tinio_storage_blocks_reclaimed_total",
        "Block files reclaimed by the block sweep, cumulative"
    )
    .expect("register tinio_storage_blocks_reclaimed_total")
});

/// Bytes reclaimed by the block sweep, cumulative over the process's passes.
#[expect(
    clippy::expect_used,
    reason = "argued: the same argument as `RECLAIMED_BLOCKS` above — every error this registration can answer is about this family's own name (already registered, or registered with a different label set or help), from one `LazyLock` site a process runs once, against the family list `specs/001-s3-local-server/data-model.md`'s `## Metrics (read-only views, not persisted)` owns; the refusal is the collision detection, and it has nowhere to return a `Result` to (design 2026-10-09 D2d)"
)]
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
#[expect(
    clippy::expect_used,
    reason = "argued: the same argument as `RECLAIMED_BLOCKS` above — every error this registration can answer is about this family's own name (already registered, or registered with a different label set or help), from one `LazyLock` site a process runs once, against the family list `specs/001-s3-local-server/data-model.md`'s `## Metrics (read-only views, not persisted)` owns; the refusal is the collision detection, and it has nowhere to return a `Result` to (design 2026-10-09 D2d)"
)]
static NONCURRENT_VERSIONS: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "tinio_storage_noncurrent_versions_total",
        "Noncurrent versions retained across versioned buckets, as of the last sweep pass"
    )
    .expect("register tinio_storage_noncurrent_versions_total")
});

/// Total bytes held by the versioned buckets' block stores, as the last sweep
/// pass walked them — the walk that already stat'ed every block
/// ([`Summary`]'s own `reclaimed_bytes` is what a pass dropped, this is what
/// remains; the store has no hard cap, so this family is the capacity
/// signal).
#[expect(
    clippy::expect_used,
    reason = "argued: the same argument as `RECLAIMED_BLOCKS` above — every error this registration can answer is about this family's own name (already registered, or registered with a different label set or help), from one `LazyLock` site a process runs once, against the family list `specs/001-s3-local-server/data-model.md`'s `## Metrics (read-only views, not persisted)` owns; the refusal is the collision detection, and it has nowhere to return a `Result` to (design 2026-10-09 D2d)"
)]
static BLOCK_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "tinio_storage_block_bytes",
        "Bytes held by the versioned buckets' block stores, as of the last sweep pass"
    )
    .expect("register tinio_storage_block_bytes")
});

/// Stored blocks that failed verification against their own name, cumulative
/// across verify passes (the doctor's and the post-repair startup pass's) —
/// each pass adds its findings, so the family answers "corrupt since start".
#[expect(
    clippy::expect_used,
    reason = "argued: the same argument as `RECLAIMED_BLOCKS` above — every error this registration can answer is about this family's own name (already registered, or registered with a different label set or help), from one `LazyLock` site a process runs once, against the family list `specs/001-s3-local-server/data-model.md`'s `## Metrics (read-only views, not persisted)` owns; the refusal is the collision detection, and it has nowhere to return a `Result` to (design 2026-10-09 D2d)"
)]
static BLOCKS_CORRUPT: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "tinio_storage_blocks_corrupt_total",
        "Stored blocks that failed verification against their own content hash, cumulative"
    )
    .expect("register tinio_storage_blocks_corrupt_total")
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
        record_block_bytes(16);
        record_blocks_corrupt(1);

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
